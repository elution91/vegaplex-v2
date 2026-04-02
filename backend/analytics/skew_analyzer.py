"""
Skew Analyzer Module
Analyzes volatility skew patterns and identifies anomalies
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
from scipy import stats
from scipy.optimize import curve_fit

logger = logging.getLogger(__name__)


class SkewAnalyzer:
    """Analyzes volatility skew patterns and identifies trading opportunities"""
    
    def __init__(self, config):
        self.config = config
        self.skew_cache = {}
    
    def analyze_skew(self, vol_surface: Dict) -> Dict:
        """Comprehensive skew analysis"""
        symbol = vol_surface['symbol']
        current_price = vol_surface['current_price']
        
        # Extract call and put surfaces
        call_surface = vol_surface['call_surface']
        put_surface = vol_surface['put_surface']
        
        # Calculate skew metrics
        call_skew = self._calculate_option_skew(call_surface, current_price, 'call')
        put_skew = self._calculate_option_skew(put_surface, current_price, 'put')
        
        # Calculate term structure skew
        term_skew = self._calculate_term_skew(vol_surface, call_skew, put_skew)
        
        # Calculate skew curvature
        skew_curvature = self._calculate_skew_curvature(call_surface, put_surface)
        
        # Identify skew anomalies
        anomalies = self._identify_skew_anomalies(call_skew, put_skew, term_skew)
        
        # Calculate relative value metrics
        relative_value = self._calculate_relative_value_metrics(call_skew, put_skew, vol_surface)
        
        # Combine all metrics
        combined_metrics = {
            'symbol': symbol,
            'current_price': current_price,
            'timestamp': datetime.now(),
            'call_skew': call_skew,
            'put_skew': put_skew,
            'term_skew': term_skew,
            'skew_curvature': skew_curvature,
            'anomalies': anomalies,
            'relative_value': relative_value
        }
        
        # Calculate overall skew score
        combined_metrics['overall_skew_score'] = self._calculate_skew_score(combined_metrics)
        
        return combined_metrics
    
    def _calculate_option_skew(self, surface: Dict, current_price: float, option_type: str) -> Dict:
        """Calculate skew metrics for calls or puts"""
        if surface['type'] != option_type or not surface['surfaces_by_expiry']:
            return self._empty_skew_metrics(option_type)
        
        all_skew_metrics = []
        
        for expiry, expiry_data in surface['surfaces_by_expiry'].items():
            try:
                strikes = expiry_data['strikes']
                vols = expiry_data['vols']
                tte = expiry_data['tte']
                
                if len(strikes) < 3:
                    continue
                
                # Calculate moneyness
                moneyness = strikes / current_price
                log_moneyness = np.log(moneyness)
                
                # Calculate skew slope (linear regression)
                if len(vols) >= 3:
                    slope, intercept, r_value, p_value, std_err = stats.linregress(log_moneyness, vols)
                    
                    # Calculate skew at specific points
                    atm_vol = np.interp(0, log_moneyness, vols)  # ATM
                    otm_25_vol = np.interp(np.log(0.75), log_moneyness, vols)  # 25% OTM
                    otm_10_vol = np.interp(np.log(0.90), log_moneyness, vols)  # 10% OTM
                    
                    if option_type == 'call':
                        # For calls, look at ITM and OTM
                        itm_10_vol = np.interp(np.log(1.10), log_moneyness, vols)  # 10% ITM
                        skew_25 = otm_25_vol - atm_vol
                        skew_10 = otm_10_vol - atm_vol
                        skew_itm = atm_vol - itm_10_vol
                    else:
                        # For puts, focus on OTM puts
                        otm_25_put = np.interp(np.log(0.75), log_moneyness, vols)
                        otm_10_put = np.interp(np.log(0.90), log_moneyness, vols)
                        skew_25 = otm_25_put - atm_vol
                        skew_10 = otm_10_put - atm_vol
                        skew_itm = 0  # Not relevant for puts
                    
                    # Calculate skew curvature (second derivative)
                    if len(vols) >= 5:
                        # Fit quadratic for curvature
                        def quadratic(x, a, b, c):
                            return a * x**2 + b * x + c
                        
                        try:
                            popt, _ = curve_fit(quadratic, log_moneyness, vols)
                            curvature = 2 * popt[0]  # Second derivative
                        except:
                            curvature = 0
                    else:
                        curvature = 0
                    
                    # Calculate skew relative to historical (placeholder)
                    relative_skew = slope  # Would need historical data
                    
                    skew_metrics = {
                        'expiry': expiry,
                        'tte': tte,
                        'slope': slope,
                        'intercept': intercept,
                        'r_squared': r_value**2,
                        'p_value': p_value,
                        'atm_vol': atm_vol,
                        'skew_25': skew_25,
                        'skew_10': skew_10,
                        'skew_itm': skew_itm,
                        'curvature': curvature,
                        'relative_skew': relative_skew,
                        'data_points': len(strikes)
                    }
                    
                    all_skew_metrics.append(skew_metrics)
                    
            except Exception as e:
                logger.warning(f"Error calculating skew for expiry {expiry}: {e}")
                continue
        
        if not all_skew_metrics:
            return self._empty_skew_metrics(option_type)
        
        # Calculate aggregate metrics
        aggregate_metrics = self._calculate_aggregate_skew(all_skew_metrics)
        
        return {
            'type': option_type,
            'by_expiry': all_skew_metrics,
            'aggregate': aggregate_metrics
        }
    
    def _calculate_term_skew(self, vol_surface: Dict, call_skew: Dict, put_skew: Dict) -> Dict:
        """Calculate term structure skew (how skew changes with time)"""
        term_structure = vol_surface.get('term_structure', {})
        atm_vols = term_structure.get('atm_vols', [])

        if not atm_vols or len(atm_vols) < 2:
            return {}
        
        term_skew_data = []
        
        for call_expiry_data, put_expiry_data in zip(
            call_skew.get('by_expiry', []), 
            put_skew.get('by_expiry', [])
        ):
            try:
                # Match expirations
                if call_expiry_data['expiry'] == put_expiry_data['expiry']:
                    term_skew_data.append({
                        'expiry': call_expiry_data['expiry'],
                        'tte': call_expiry_data['tte'],
                        'call_skew_slope': call_expiry_data['slope'],
                        'put_skew_slope': put_expiry_data['slope'],
                        'skew_ratio': call_expiry_data['slope'] / put_expiry_data['slope'] if put_expiry_data['slope'] != 0 else np.nan,
                        'vol_diff': call_expiry_data['atm_vol'] - put_expiry_data['atm_vol']
                    })
            except:
                continue
        
        # Calculate term skew trends
        if len(term_skew_data) >= 2:
            ttes = [d['tte'] for d in term_skew_data]
            call_slopes = [d['call_skew_slope'] for d in term_skew_data]
            put_slopes = [d['put_skew_slope'] for d in term_skew_data]
            
            # How skew evolves with time
            call_skew_trend = np.polyfit(ttes, call_slopes, 1)[0] if len(ttes) >= 2 else 0
            put_skew_trend = np.polyfit(ttes, put_slopes, 1)[0] if len(ttes) >= 2 else 0
            
            return {
                'by_expiry': term_skew_data,
                'call_skew_trend': call_skew_trend,
                'put_skew_trend': put_skew_trend,
                'avg_skew_ratio': np.nanmean([d['skew_ratio'] for d in term_skew_data]),
                'term_skew_steepness': call_skew_trend - put_skew_trend
            }
        
        return {'by_expiry': term_skew_data}
    
    def _calculate_skew_curvature(self, call_surface: Dict, put_surface: Dict) -> Dict:
        """Calculate skew curvature across strikes and time"""
        curvature_metrics = {}
        
        # Call curvature
        call_curvatures = []
        for expiry_data in call_surface['surfaces_by_expiry'].values():
            if 'curvature' in expiry_data:
                call_curvatures.append(expiry_data['curvature'])
        
        # Put curvature
        put_curvatures = []
        for expiry_data in put_surface['surfaces_by_expiry'].values():
            if 'curvature' in expiry_data:
                put_curvatures.append(expiry_data['curvature'])
        
        curvature_metrics = {
            'call_avg_curvature': np.mean(call_curvatures) if call_curvatures else 0,
            'put_avg_curvature': np.mean(put_curvatures) if put_curvatures else 0,
            'call_curvature_range': (max(call_curvatures) - min(call_curvatures)) if len(call_curvatures) > 1 else 0,
            'put_curvature_range': (max(put_curvatures) - min(put_curvatures)) if len(put_curvatures) > 1 else 0,
            'curvature_ratio': np.mean(call_curvatures) / np.mean(put_curvatures) if put_curvatures and np.mean(put_curvatures) != 0 else np.nan
        }
        
        return curvature_metrics
    
    def _identify_skew_anomalies(self, call_skew: Dict, put_skew: Dict, term_skew: Dict) -> List[Dict]:
        """Identify skew anomalies and trading opportunities"""
        anomalies = []
        
        # Check for extreme skew slopes
        if call_skew.get('aggregate', {}).get('avg_slope'):
            call_slope = call_skew['aggregate']['avg_slope']
            if abs(call_slope) > self.config.max_skew_slope:
                anomalies.append({
                    'type': 'extreme_call_skew',
                    'severity': 'high' if abs(call_slope) > 1.5 else 'medium',
                    'value': call_slope,
                    'description': f"Extreme call skew slope: {call_slope:.3f}"
                })
        
        if put_skew.get('aggregate', {}).get('avg_slope'):
            put_slope = put_skew['aggregate']['avg_slope']
            if abs(put_slope) > self.config.max_skew_slope:
                anomalies.append({
                    'type': 'extreme_put_skew',
                    'severity': 'high' if abs(put_slope) > 1.5 else 'medium',
                    'value': put_slope,
                    'description': f"Extreme put skew slope: {put_slope:.3f}"
                })
        
        # Check for skew inversion
        call_slopes = [e['slope'] for e in call_skew.get('by_expiry', [])]
        put_slopes = [e['slope'] for e in put_skew.get('by_expiry', [])]
        
        if call_slopes and put_slopes:
            call_avg = np.mean(call_slopes)
            put_avg = np.mean(put_slopes)
            
            # Normally call skew should be negative, put skew positive
            skew_inversion = (call_avg > 0 and put_avg < 0) or (abs(call_avg - put_avg) > 1.0)
            if skew_inversion:
                anomalies.append({
                    'type': 'skew_inversion',
                    'severity': 'high',
                    'value': call_avg - put_avg,
                    'description': f"Skew inversion detected: call={call_avg:.3f}, put={put_avg:.3f}"
                })
        
        # Check for term structure anomalies
        if term_skew.get('term_skew_steepness'):
            term_steep = term_skew['term_skew_steepness']
            if abs(term_steep) > 0.5:
                anomalies.append({
                    'type': 'term_skew_anomaly',
                    'severity': 'medium',
                    'value': term_steep,
                    'description': f"Unusual term skew evolution: {term_steep:.3f}"
                })
        
        # Check for low curvature (flat skew)
        avg_curv = call_skew.get('aggregate', {}).get('avg_curvature')
        if avg_curv is not None and avg_curv < 0.01:
            anomalies.append({
                'type': 'flat_skew',
                'severity': 'low',
                'value': avg_curv,
                'description': "Unusually flat skew - potential arbitrage"
            })
        
        return anomalies
    
    def _calculate_relative_value_metrics(self, call_skew: Dict, put_skew: Dict, vol_surface: Dict) -> Dict:
        """Calculate relative value metrics for skew"""
        rel_value = {}
        
        # Compare current skew to historical averages (placeholder)
        # In practice, you'd need historical skew data
        
        # Calculate skew richness relative to ATM vol
        if call_skew.get('aggregate', {}).get('atm_vol'):
            atm_vol = call_skew['aggregate']['atm_vol']
            skew_25 = call_skew['aggregate'].get('skew_25', 0)
            skew_ratio = skew_25 / atm_vol if atm_vol > 0 else 0
            
            rel_value['skew_richness'] = skew_ratio
            rel_value['skew_expensive'] = skew_ratio > 0.3  # Threshold for "expensive" skew
        
        # Calculate forward skew expectations
        term_structure = vol_surface.get('term_structure', {})
        if term_structure.get('contango') is not None:
            rel_value['forward_skew_expectation'] = 'steepening' if term_structure['contango'] else 'flattening'
        
        # Calculate arbitrage potential
        call_surface = vol_surface['call_surface']
        put_surface = vol_surface['put_surface']
        
        # Look for calendar spread opportunities
        call_expiries = list(call_surface['surfaces_by_expiry'].keys())
        put_expiries = list(put_surface['surfaces_by_expiry'].keys())
        
        if len(call_expiries) >= 2 and len(put_expiries) >= 2:
            # Simple calendar spread potential
            rel_value['calendar_spread_potential'] = True
            rel_value['arb_potential'] = len(call_skew.get('anomalies', [])) > 0
        
        return rel_value
    
    def _calculate_aggregate_skew(self, skew_metrics: List[Dict]) -> Dict:
        """Calculate aggregate skew metrics across expirations"""
        if not skew_metrics:
            return {}
        
        # Average key metrics
        avg_slope = np.mean([m['slope'] for m in skew_metrics])
        avg_atm_vol = np.mean([m['atm_vol'] for m in skew_metrics])
        avg_skew_25 = np.mean([m['skew_25'] for m in skew_metrics])
        avg_skew_10 = np.mean([m['skew_10'] for m in skew_metrics])
        avg_curvature = np.mean([m['curvature'] for m in skew_metrics])
        
        # Calculate weighted averages (by time to expiry)
        weights = [m['tte'] for m in skew_metrics]
        total_weight = sum(weights)
        
        if total_weight > 0:
            weighted_slope = sum(m['slope'] * m['tte'] for m in skew_metrics) / total_weight
            weighted_atm_vol = sum(m['atm_vol'] * m['tte'] for m in skew_metrics) / total_weight
        else:
            weighted_slope = avg_slope
            weighted_atm_vol = avg_atm_vol
        
        return {
            'avg_slope': avg_slope,
            'weighted_slope': weighted_slope,
            'avg_atm_vol': avg_atm_vol,
            'weighted_atm_vol': weighted_atm_vol,
            'avg_skew_25': avg_skew_25,
            'avg_skew_10': avg_skew_10,
            'avg_curvature': avg_curvature,
            'slope_range': max(m['slope'] for m in skew_metrics) - min(m['slope'] for m in skew_metrics),
            'expiry_count': len(skew_metrics)
        }
    
    def _calculate_skew_score(self, combined_metrics: Dict) -> float:
        """Calculate overall skew score (0-1) for opportunity ranking"""
        score = 0.0
        
        # Anomaly score (40% weight)
        anomalies = combined_metrics.get('anomalies', [])
        anomaly_score = min(len(anomalies) * 0.2, 0.4)
        score += anomaly_score
        
        # Skew extremity score (30% weight)
        call_slope = abs(combined_metrics.get('call_skew', {}).get('aggregate', {}).get('avg_slope', 0))
        put_slope = abs(combined_metrics.get('put_skew', {}).get('aggregate', {}).get('avg_slope', 0))
        extremity_score = min((call_slope + put_slope) / 4, 0.3)
        score += extremity_score
        
        # Term structure score (20% weight)
        term_skew = combined_metrics.get('term_skew', {})
        if term_skew.get('term_skew_steepness'):
            term_score = min(abs(term_skew['term_skew_steepness']) * 0.4, 0.2)
            score += term_score
        
        # Relative value score (10% weight)
        rel_value = combined_metrics.get('relative_value', {})
        if rel_value.get('arb_potential'):
            score += 0.1
        
        return min(score, 1.0)
    
    def _empty_skew_metrics(self, option_type: str) -> Dict:
        """Return empty skew metrics structure"""
        return {
            'type': option_type,
            'by_expiry': [],
            'aggregate': {}
        }