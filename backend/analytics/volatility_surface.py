"""
Volatility Surface Module
Builds and analyzes volatility surfaces for skew detection
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
from scipy import interpolate
from scipy.optimize import curve_fit

logger = logging.getLogger(__name__)


class VolatilitySurface:
    """Builds and analyzes volatility surfaces"""
    
    def __init__(self):
        self.surface_cache = {}
    
    def build_surface(self, options_data: Dict) -> Dict:
        """Build complete volatility surface from options data"""
        symbol = options_data['symbol']
        current_price = options_data['current_price']
        
        # Process calls and puts
        calls_df = options_data['calls'].copy()
        puts_df = options_data['puts'].copy()
        
        if calls_df.empty or puts_df.empty:
            return self._empty_surface(symbol)
        
        # Calculate moneyness and time to expiry
        calls_df = self._process_options_dataframe(calls_df, current_price)
        puts_df = self._process_options_dataframe(puts_df, current_price)
        
        # Build separate surfaces for calls and puts
        call_surface = self._build_option_surface(calls_df, 'call')
        put_surface = self._build_option_surface(puts_df, 'put')
        
        # Calculate combined surface metrics
        combined_surface = self._combine_surfaces(call_surface, put_surface)
        
        # Calculate forward prices and term structure
        term_structure = self._calculate_term_structure(calls_df, puts_df, current_price)
        
        return {
            'symbol': symbol,
            'current_price': current_price,
            'timestamp': options_data['timestamp'],
            'call_surface': call_surface,
            'put_surface': put_surface,
            'combined_surface': combined_surface,
            'term_structure': term_structure,
            'raw_data': {
                'calls': calls_df,
                'puts': puts_df
            }
        }
    
    def _process_options_dataframe(self, df: pd.DataFrame, current_price: float) -> pd.DataFrame:
        """Process options dataframe with calculated metrics"""
        if df.empty:
            return df
        
        # Calculate moneyness
        df['moneyness'] = df['strike'] / current_price
        df['log_moneyness'] = np.log(df['moneyness'])
        
        # Calculate time to expiry
        df['days_to_expiry'] = df.apply(self._calculate_dte, axis=1)
        df['years_to_expiry'] = df['days_to_expiry'] / 365.25
        
        # Filter out bad data
        df = df[
            (df['implied_vol'] > 0.04) &
            (df['implied_vol'] < 5.0) &
            (df['days_to_expiry'] >= 7) &
            (df['years_to_expiry'] <= 2.0)
        ].copy()
        
        # Calculate forward moneyness
        df['forward_moneyness'] = df['strike'] / (current_price * np.exp(0.05 * df['years_to_expiry']))
        
        return df
    
    def _calculate_dte(self, row) -> float:
        """Calculate days to expiry from expiry date"""
        try:
            expiry_date = pd.to_datetime(row['expiry'])
            return (expiry_date - datetime.now()).days
        except:
            return np.nan
    
    def _build_option_surface(self, df: pd.DataFrame, option_type: str) -> Dict:
        """Build volatility surface for calls or puts"""
        if df.empty:
            return self._empty_surface_data()
        
        # Group by expiry
        expiry_groups = df.groupby('expiry')
        
        surfaces_by_expiry = {}
        all_strikes = []
        all_vols = []
        all_ttes = []
        
        for expiry, group in expiry_groups:
            if len(group) < 3:  # Need at least 3 points
                continue
            
            # Sort by strike
            group = group.sort_values('strike')
            
            # Extract data
            strikes = group['strike'].values
            vols = group['implied_vol'].values
            tte = group['years_to_expiry'].iloc[0]
            
            # Filter for reasonable strikes
            current_price = df['current_price'].iloc[0] if 'current_price' in df.columns else 100
            min_strike = current_price * 0.5
            max_strike = current_price * 1.5
            
            mask = (strikes >= min_strike) & (strikes <= max_strike)
            if mask.sum() < 3:
                continue
            
            strikes = strikes[mask]
            vols = vols[mask]

            # Outlier removal: drop points where IV is >3x the median
            # (catches stale-lastPrice spikes that survive the moneyness filter)
            if len(vols) >= 4:
                median_vol = np.median(vols)
                outlier_mask = vols <= max(median_vol * 3.0, 0.05)
                if outlier_mask.sum() >= 3:
                    strikes = strikes[outlier_mask]
                    vols = vols[outlier_mask]

            if len(strikes) < 3:
                continue

            # Fit spline for smooth surface
            try:
                # Use cubic spline with smoothing
                spline = interpolate.UnivariateSpline(strikes, vols, s=0.1)

                # Generate smooth surface points
                smooth_strikes = np.linspace(strikes.min(), strikes.max(), 50)
                smooth_vols = spline(smooth_strikes)
                # Clamp spline output to valid range (splines can extrapolate negative)
                smooth_vols = np.clip(smooth_vols, 0.01, 5.0)
                
                surfaces_by_expiry[expiry] = {
                    'strikes': strikes,
                    'vols': vols,
                    'smooth_strikes': smooth_strikes,
                    'smooth_vols': smooth_vols,
                    'spline': spline,
                    'tte': tte,
                    'raw_data': group
                }
                
                all_strikes.extend(strikes)
                all_vols.extend(vols)
                all_ttes.extend([tte] * len(strikes))
                
            except Exception as e:
                logger.warning(f"Failed to fit spline for expiry {expiry}: {e}")
                continue
        
        # Build 2D surface if we have enough data
        surface_2d = None
        if len(surfaces_by_expiry) >= 2:
            surface_2d = self._build_2d_surface(surfaces_by_expiry)
        
        return {
            'type': option_type,
            'surfaces_by_expiry': surfaces_by_expiry,
            'surface_2d': surface_2d,
            'all_strikes': np.array(all_strikes),
            'all_vols': np.array(all_vols),
            'all_ttes': np.array(all_ttes)
        }
    
    def _build_2d_surface(self, surfaces_by_expiry: Dict) -> Dict:
        """Build 2D volatility surface (strike vs time)"""
        try:
            # Collect all data points
            all_strikes = []
            all_vols = []
            all_ttes = []
            
            for expiry_data in surfaces_by_expiry.values():
                all_strikes.extend(expiry_data['strikes'])
                all_vols.extend(expiry_data['vols'])
                all_ttes.extend([expiry_data['tte']] * len(expiry_data['strikes']))
            
            if len(all_strikes) < 10:
                return None
            
            # Convert to arrays
            strikes = np.array(all_strikes)
            vols = np.array(all_vols)
            ttes = np.array(all_ttes)
            
            # Create grid for interpolation
            strike_grid = np.linspace(strikes.min(), strikes.max(), 30)
            tte_grid = np.linspace(min(ttes), max(ttes), 20)
            
            # 2D interpolation
            try:
                surface_interp = interpolate.griddata(
                    (strikes, ttes), vols,
                    (strike_grid[None, :], tte_grid[:, None]),
                    method='cubic'
                )
                
                return {
                    'strike_grid': strike_grid,
                    'tte_grid': tte_grid,
                    'surface': surface_interp,
                    'raw_points': (strikes, ttes, vols)
                }
                
            except Exception as e:
                logger.warning(f"Failed to build 2D surface: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Error building 2D surface: {e}")
            return None
    
    def _combine_surfaces(self, call_surface: Dict, put_surface: Dict) -> Dict:
        """Combine call and put surfaces for analysis"""
        # Calculate put-call parity violations
        combined_metrics = {}
        
        # Extract common expirations
        call_expiries = set(call_surface['surfaces_by_expiry'].keys())
        put_expiries = set(put_surface['surfaces_by_expiry'].keys())
        common_expiries = call_expiries.intersection(put_expiries)
        
        parity_violations = []
        
        for expiry in common_expiries:
            call_data = call_surface['surfaces_by_expiry'][expiry]
            put_data = put_surface['surfaces_by_expiry'][expiry]
            
            # Find matching strikes
            common_strikes = set(call_data['strikes']).intersection(set(put_data['strikes']))
            
            for strike in common_strikes:
                call_idx = np.where(call_data['strikes'] == strike)[0]
                put_idx = np.where(put_data['strikes'] == strike)[0]
                
                if len(call_idx) > 0 and len(put_idx) > 0:
                    call_vol = call_data['vols'][call_idx[0]]
                    put_vol = put_data['vols'][put_idx[0]]
                    
                    # Simple parity check (should be similar for same strike)
                    vol_diff = abs(call_vol - put_vol)
                    parity_violations.append({
                        'expiry': expiry,
                        'strike': strike,
                        'call_vol': call_vol,
                        'put_vol': put_vol,
                        'vol_diff': vol_diff
                    })
        
        combined_metrics['parity_violations'] = parity_violations
        combined_metrics['avg_vol_diff'] = np.mean([v['vol_diff'] for v in parity_violations]) if parity_violations else 0
        
        return combined_metrics
    
    def _calculate_term_structure(self, calls_df: pd.DataFrame, puts_df: pd.DataFrame, current_price: float) -> Dict:
        """Calculate term structure of volatility"""
        try:
            # Calculate ATM vol for each expiry
            atm_vols = []
            
            for df in [calls_df, puts_df]:
                if df.empty:
                    continue
                
                expiry_groups = df.groupby('expiry')
                
                for expiry, group in expiry_groups:
                    if len(group) < 3:
                        continue
                    
                    # Find ATM option
                    atm_idx = (group['strike'] - current_price).abs().idxmin()
                    atm_option = group.loc[atm_idx]
                    
                    if pd.notna(atm_option['implied_vol']):
                        atm_vols.append({
                            'expiry': expiry,
                            'days_to_expiry': atm_option['days_to_expiry'],
                            'years_to_expiry': atm_option['years_to_expiry'],
                            'atm_vol': atm_option['implied_vol'],
                            'option_type': 'call' if 'call' in df.columns[0].lower() else 'put'
                        })
            
            if not atm_vols:
                return {}
            
            # Convert to DataFrame and sort
            term_df = pd.DataFrame(atm_vols).sort_values('days_to_expiry')
            
            # Calculate term structure metrics
            if len(term_df) >= 2:
                # Contango/backwardation
                short_vol = term_df.iloc[0]['atm_vol']
                long_vol = term_df.iloc[-1]['atm_vol']
                term_slope = (long_vol - short_vol) / (term_df.iloc[-1]['years_to_expiry'] - term_df.iloc[0]['years_to_expiry'])
                
                # Volatility risk premium
                avg_vol = term_df['atm_vol'].mean()
                
                return {
                    'atm_vols': term_df.to_dict('records'),
                    'term_slope': term_slope,
                    'short_vol': short_vol,
                    'long_vol': long_vol,
                    'avg_vol': avg_vol,
                    'contango': term_slope > 0
                }
            
            return {'atm_vols': term_df.to_dict('records')}
            
        except Exception as e:
            logger.error(f"Error calculating term structure: {e}")
            return {}
    
    def _empty_surface(self, symbol: str) -> Dict:
        """Return empty surface structure"""
        return {
            'symbol': symbol,
            'current_price': np.nan,
            'timestamp': datetime.now(),
            'call_surface': self._empty_surface_data(),
            'put_surface': self._empty_surface_data(),
            'combined_surface': {},
            'term_structure': {},
            'raw_data': {'calls': pd.DataFrame(), 'puts': pd.DataFrame()}
        }
    
    def _empty_surface_data(self) -> Dict:
        """Return empty surface data structure"""
        return {
            'type': 'unknown',
            'surfaces_by_expiry': {},
            'surface_2d': None,
            'all_strikes': np.array([]),
            'all_vols': np.array([]),
            'all_ttes': np.array([])
        }
    
    def calculate_local_volatility(self, surface: Dict, strike: float, time_to_expiry: float) -> Optional[float]:
        """Calculate local volatility using Dupire's formula"""
        try:
            # This would require implementing Dupire's formula
            # For now, return None as placeholder
            return None
        except Exception as e:
            logger.error(f"Error calculating local volatility: {e}")
            return None
    
    def interpolate_volatility(self, surface: Dict, strike: float, time_to_expiry: float) -> Optional[float]:
        """Interpolate volatility for arbitrary strike/time"""
        try:
            # Use the 2D surface if available
            if surface['surface_2d'] is not None:
                surface_2d = surface['surface_2d']
                
                # Find nearest grid points
                strike_idx = np.searchsorted(surface_2d['strike_grid'], strike)
                tte_idx = np.searchsorted(surface_2d['tte_grid'], time_to_expiry)
                
                # Boundary checks
                if (strike_idx <= 0 or strike_idx >= len(surface_2d['strike_grid']) - 1 or
                    tte_idx <= 0 or tte_idx >= len(surface_2d['tte_grid']) - 1):
                    return None
                
                # Bilinear interpolation
                vol_surface = surface_2d['surface']
                
                # Simple nearest neighbor for now
                return vol_surface[tte_idx, strike_idx]
            
            return None
            
        except Exception as e:
            logger.error(f"Error interpolating volatility: {e}")
            return None