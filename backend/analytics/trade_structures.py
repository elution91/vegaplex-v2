"""
Trade Structure Builder Module
Builds specific option trade structures for identified opportunities
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging

logger = logging.getLogger(__name__)


class TradeStructureBuilder:
    """Builds specific option trade structures"""
    
    def __init__(self, config):
        self.config = config
    
    def build_structure(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build the optimal trade structure for an opportunity"""

        trade_type = opportunity['type']
        symbol = opportunity['symbol']

        # New Bennett-framework types (from rewritten opportunity scanner)
        if trade_type == 'skew_trade':
            return self._build_skew_trade(opportunity, vol_surface, options_data)
        elif trade_type == 'term_structure':
            return self._build_term_structure_trade(opportunity, vol_surface, options_data)
        elif trade_type == 'vrp_harvest':
            return self._build_vrp_harvest(opportunity, vol_surface, options_data)
        elif trade_type == 'combo_trade':
            return self._build_combo_trade(opportunity, vol_surface, options_data)
        # Legacy types (kept for backward compatibility)
        elif trade_type == 'skew_arbitrage':
            return self._build_skew_arbitrage(opportunity, vol_surface, options_data)
        elif trade_type == 'calendar_spread':
            return self._build_calendar_spread(opportunity, vol_surface, options_data)
        elif trade_type == 'vertical_spread':
            return self._build_vertical_spread(opportunity, vol_surface, options_data)
        elif trade_type == 'ratio_spread':
            return self._build_ratio_spread(opportunity, vol_surface, options_data)
        elif trade_type == 'volatility_risk_premium':
            return self._build_vrp_structure(opportunity, vol_surface, options_data)
        else:
            return self._build_generic_structure(opportunity, vol_surface, options_data)
    
    def _build_skew_arbitrage(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build skew arbitrage trade structure"""
        subtype = opportunity['subtype']
        direction = opportunity['direction']
        
        if subtype == 'call_skew_steep':
            # Sell expensive OTM calls, buy cheaper ITM calls or puts
            structure = self._build_call_skew_arbitrage(opportunity, vol_surface, options_data)
        elif subtype == 'put_skew_steep':
            # Sell expensive OTM puts, buy cheaper ITM puts or calls
            structure = self._build_put_skew_arbitrage(opportunity, vol_surface, options_data)
        elif subtype == 'term_skew_flatten':
            # Calendar spread to flatten term skew
            structure = self._build_calendar_spread(opportunity, vol_surface, options_data)
        else:
            structure = self._build_generic_structure(opportunity, vol_surface, options_data)
        
        return structure
    
    def _build_call_skew_arbitrage(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build call skew arbitrage structure"""
        current_price = options_data['current_price']
        calls_df = options_data['calls'].copy()
        
        if calls_df.empty:
            return self._empty_structure()
        
        # Find the best expiry (closest to 30-45 DTE)
        calls_df['days_to_expiry'] = calls_df.apply(self._calculate_dte, axis=1)
        ideal_dte = 30
        calls_df['dte_distance'] = abs(calls_df['days_to_expiry'] - ideal_dte)
        best_expiry = calls_df.loc[calls_df['dte_distance'].idxmin(), 'expiry']
        
        # Filter for best expiry
        expiry_calls = calls_df[calls_df['expiry'] == best_expiry].copy()
        
        if len(expiry_calls) < 4:
            return self._empty_structure()
        
        # Select strikes based on skew
        # Sell OTM calls where vol is highest
        # Buy ITM calls or ATM puts where vol is lower
        
        # Sort by volatility
        expiry_calls = expiry_calls.sort_values('implied_vol', ascending=False)
        
        # Find OTM calls to sell (top 25% vol)
        otm_calls = expiry_calls[expiry_calls['strike'] > current_price * 1.05]
        if len(otm_calls) >= 2:
            sell_calls = otm_calls.head(2)
        else:
            sell_calls = otm_calls.head(1)
        
        # Find ITM calls to buy (bottom 25% vol)
        itm_calls = expiry_calls[expiry_calls['strike'] < current_price * 0.95]
        if len(itm_calls) >= 1:
            buy_calls = itm_calls.head(1)
        else:
            # Fallback to ATM calls
            atm_calls = expiry_calls[(expiry_calls['strike'] >= current_price * 0.98) & 
                                   (expiry_calls['strike'] <= current_price * 1.02)]
            buy_calls = atm_calls.head(1) if len(atm_calls) >= 1 else expiry_calls.head(1)
        
        # Build legs
        legs = []
        
        # Sell OTM calls
        for _, call in sell_calls.iterrows():
            legs.append({
                'type': 'call',
                'action': 'sell',
                'strike': call['strike'],
                'expiry': call['expiry'],
                'contracts': 1,
                'price': call['ask'] if pd.notna(call['ask']) else call['last'],
                'delta': call.get('delta', 0),
                'gamma': call.get('gamma', 0),
                'vega': call.get('vega', 0),
                'theta': call.get('theta', 0),
                'implied_vol': call['implied_vol']
            })
        
        # Buy ITM calls
        for _, call in buy_calls.iterrows():
            legs.append({
                'type': 'call',
                'action': 'buy',
                'strike': call['strike'],
                'expiry': call['expiry'],
                'contracts': 1,
                'price': call['bid'] if pd.notna(call['bid']) else call['last'],
                'delta': call.get('delta', 0),
                'gamma': call.get('gamma', 0),
                'vega': call.get('vega', 0),
                'theta': call.get('theta', 0),
                'implied_vol': call['implied_vol']
            })
        
        # Calculate structure metrics
        structure_metrics = self._calculate_structure_metrics(legs)
        
        return {
            'type': 'skew_arbitrage',
            'subtype': 'call_skew_arbitrage',
            'legs': legs,
            'metrics': structure_metrics,
            'entry_signal': opportunity['entry_signal'],
            'confidence': opportunity['confidence'],
            'expected_pnl': opportunity['expected_pnl'],
            'max_loss': structure_metrics.get('max_loss', 0),
            'max_gain': structure_metrics.get('max_gain', float('inf')),
            'breakeven_points': structure_metrics.get('breakeven_points', []),
            'days_to_expiry': best_expiry,
            'rationale': opportunity['rationale']
        }
    
    def _build_put_skew_arbitrage(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build put skew arbitrage structure"""
        current_price = options_data['current_price']
        puts_df = options_data['puts'].copy()
        
        if puts_df.empty:
            return self._empty_structure()
        
        # Similar logic to call skew arbitrage but for puts
        puts_df['days_to_expiry'] = puts_df.apply(self._calculate_dte, axis=1)
        ideal_dte = 30
        puts_df['dte_distance'] = abs(puts_df['days_to_expiry'] - ideal_dte)
        best_expiry = puts_df.loc[puts_df['dte_distance'].idxmin(), 'expiry']
        
        expiry_puts = puts_df[puts_df['expiry'] == best_expiry].copy()
        
        if len(expiry_puts) < 4:
            return self._empty_structure()
        
        # Sort by volatility
        expiry_puts = expiry_puts.sort_values('implied_vol', ascending=False)
        
        # Sell OTM puts (high vol)
        otm_puts = expiry_puts[expiry_puts['strike'] < current_price * 0.95]
        if len(otm_puts) >= 2:
            sell_puts = otm_puts.head(2)
        else:
            sell_puts = otm_puts.head(1)
        
        # Buy ITM puts (low vol)
        itm_puts = expiry_puts[expiry_puts['strike'] > current_price * 1.05]
        if len(itm_puts) >= 1:
            buy_puts = itm_puts.head(1)
        else:
            # Fallback to ATM puts
            atm_puts = expiry_puts[(expiry_puts['strike'] >= current_price * 0.98) & 
                                  (expiry_puts['strike'] <= current_price * 1.02)]
            buy_puts = atm_puts.head(1) if len(atm_puts) >= 1 else expiry_puts.head(1)
        
        # Build legs
        legs = []
        
        # Sell OTM puts
        for _, put in sell_puts.iterrows():
            legs.append({
                'type': 'put',
                'action': 'sell',
                'strike': put['strike'],
                'expiry': put['expiry'],
                'contracts': 1,
                'price': put['bid'] if pd.notna(put['bid']) else put['last'],
                'delta': put.get('delta', 0),
                'gamma': put.get('gamma', 0),
                'vega': put.get('vega', 0),
                'theta': put.get('theta', 0),
                'implied_vol': put['implied_vol']
            })
        
        # Buy ITM puts
        for _, put in buy_puts.iterrows():
            legs.append({
                'type': 'put',
                'action': 'buy',
                'strike': put['strike'],
                'expiry': put['expiry'],
                'contracts': 1,
                'price': put['ask'] if pd.notna(put['ask']) else put['last'],
                'delta': put.get('delta', 0),
                'gamma': put.get('gamma', 0),
                'vega': put.get('vega', 0),
                'theta': put.get('theta', 0),
                'implied_vol': put['implied_vol']
            })
        
        structure_metrics = self._calculate_structure_metrics(legs)
        
        return {
            'type': 'skew_arbitrage',
            'subtype': 'put_skew_arbitrage',
            'legs': legs,
            'metrics': structure_metrics,
            'entry_signal': opportunity['entry_signal'],
            'confidence': opportunity['confidence'],
            'expected_pnl': opportunity['expected_pnl'],
            'max_loss': structure_metrics.get('max_loss', 0),
            'max_gain': structure_metrics.get('max_gain', float('inf')),
            'breakeven_points': structure_metrics.get('breakeven_points', []),
            'days_to_expiry': best_expiry,
            'rationale': opportunity['rationale']
        }
    
    def _build_calendar_spread(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build calendar spread structure"""
        current_price = options_data['current_price']
        calls_df = options_data['calls'].copy()
        
        if calls_df.empty:
            return self._empty_structure()
        
        calls_df['days_to_expiry'] = calls_df.apply(self._calculate_dte, axis=1)
        
        # Find two suitable expirations
        near_dte_range = (15, 45)
        far_dte_range = (60, 120)
        
        near_calls = calls_df[
            (calls_df['days_to_expiry'] >= near_dte_range[0]) & 
            (calls_df['days_to_expiry'] <= near_dte_range[1])
        ]
        
        far_calls = calls_df[
            (calls_df['days_to_expiry'] >= far_dte_range[0]) & 
            (calls_df['days_to_expiry'] <= far_dte_range[1])
        ]
        
        if near_calls.empty or far_calls.empty:
            return self._empty_structure()
        
        # Select ATM strikes for both expirations
        near_expiry = near_calls['expiry'].iloc[0]
        far_expiry = far_calls['expiry'].iloc[0]
        
        near_expiry_calls = near_calls[near_calls['expiry'] == near_expiry]
        far_expiry_calls = far_calls[far_calls['expiry'] == far_expiry]
        
        # Find ATM strikes
        near_atm = near_expiry_calls.loc[(near_expiry_calls['strike'] - current_price).abs().idxmin()]
        far_atm = far_expiry_calls.loc[(far_expiry_calls['strike'] - current_price).abs().idxmin()]
        
        # Determine direction based on opportunity
        direction = opportunity.get('direction', 'sell_near_buy_far')
        
        if direction == 'sell_near_buy_far':
            # Sell near-term, buy far-term
            legs = [
                {
                    'type': 'call',
                    'action': 'sell',
                    'strike': near_atm['strike'],
                    'expiry': near_atm['expiry'],
                    'contracts': 1,
                    'price': near_atm['bid'] if pd.notna(near_atm['bid']) else near_atm['last'],
                    'delta': near_atm.get('delta', 0),
                    'gamma': near_atm.get('gamma', 0),
                    'vega': near_atm.get('vega', 0),
                    'theta': near_atm.get('theta', 0),
                },
                {
                    'type': 'call',
                    'action': 'buy',
                    'strike': far_atm['strike'],
                    'expiry': far_atm['expiry'],
                    'contracts': 1,
                    'price': far_atm['ask'] if pd.notna(far_atm['ask']) else far_atm['last'],
                    'delta': far_atm.get('delta', 0),
                    'gamma': far_atm.get('gamma', 0),
                    'vega': far_atm.get('vega', 0),
                    'theta': far_atm.get('theta', 0),
                }
            ]
        else:
            # Buy near-term, sell far-term
            legs = [
                {
                    'type': 'call',
                    'action': 'buy',
                    'strike': near_atm['strike'],
                    'expiry': near_atm['expiry'],
                    'contracts': 1,
                    'price': near_atm['ask'] if pd.notna(near_atm['ask']) else near_atm['last'],
                    'delta': near_atm.get('delta', 0),
                    'gamma': near_atm.get('gamma', 0),
                    'vega': near_atm.get('vega', 0),
                    'theta': near_atm.get('theta', 0),
                },
                {
                    'type': 'call',
                    'action': 'sell',
                    'strike': far_atm['strike'],
                    'expiry': far_atm['expiry'],
                    'contracts': 1,
                    'price': far_atm['bid'] if pd.notna(far_atm['bid']) else far_atm['last'],
                    'delta': far_atm.get('delta', 0),
                    'gamma': far_atm.get('gamma', 0),
                    'vega': far_atm.get('vega', 0),
                    'theta': far_atm.get('theta', 0),
                }
            ]
        
        structure_metrics = self._calculate_structure_metrics(legs)
        
        return {
            'type': 'calendar_spread',
            'subtype': opportunity.get('subtype', 'standard'),
            'legs': legs,
            'metrics': structure_metrics,
            'entry_signal': opportunity['entry_signal'],
            'confidence': opportunity['confidence'],
            'expected_pnl': opportunity['expected_pnl'],
            'max_loss': structure_metrics.get('max_loss', 0),
            'max_gain': structure_metrics.get('max_gain', float('inf')),
            'breakeven_points': structure_metrics.get('breakeven_points', []),
            'near_expiry': near_expiry,
            'far_expiry': far_expiry,
            'rationale': opportunity['rationale']
        }
    
    def _build_vertical_spread(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build vertical spread structure"""
        subtype = opportunity.get('subtype', 'call_credit_spread')
        strikes = opportunity.get('strikes', {})
        
        if 'call' in subtype:
            return self._build_call_vertical_spread(opportunity, vol_surface, options_data)
        else:
            return self._build_put_vertical_spread(opportunity, vol_surface, options_data)
    
    def _build_call_vertical_spread(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build call vertical spread"""
        calls_df = options_data['calls'].copy()
        expiry = opportunity.get('expiry')
        
        if calls_df.empty or not expiry:
            return self._empty_structure()
        
        expiry_calls = calls_df[calls_df['expiry'] == expiry].copy()
        
        if len(expiry_calls) < 2:
            return self._empty_structure()
        
        # Use suggested strikes or find optimal ones
        suggested_strikes = opportunity.get('strikes', {})
        if suggested_strikes:
            lower_strike = suggested_strikes.get('lower')
            higher_strike = suggested_strikes.get('higher')
        else:
            # Find ATM and OTM strikes
            current_price = options_data['current_price']
            atm_strike = expiry_calls.loc[(expiry_calls['strike'] - current_price).abs().idxmin(), 'strike']
            otm_strikes = expiry_calls[expiry_calls['strike'] > atm_strike]

            if not otm_strikes.empty:
                lower_strike = atm_strike
                higher_strike = otm_strikes.iloc[0]['strike']
            else:
                return self._empty_structure()
        
        # Get the options data
        lower_option = expiry_calls[expiry_calls['strike'] == lower_strike].iloc[0]
        higher_option = expiry_calls[expiry_calls['strike'] == higher_strike].iloc[0]
        
        # Determine direction
        subtype = opportunity.get('subtype', 'call_credit_spread')
        
        if 'credit' in subtype:
            # Credit spread: sell higher strike, buy lower strike
            legs = [
                {
                    'type': 'call',
                    'action': 'sell',
                    'strike': higher_strike,
                    'expiry': expiry,
                    'contracts': 1,
                    'price': higher_option['bid'] if pd.notna(higher_option['bid']) else higher_option['last'],
                    'delta': higher_option.get('delta', 0),
                    'gamma': higher_option.get('gamma', 0),
                    'vega': higher_option.get('vega', 0),
                    'theta': higher_option.get('theta', 0),
                },
                {
                    'type': 'call',
                    'action': 'buy',
                    'strike': lower_strike,
                    'expiry': expiry,
                    'contracts': 1,
                    'price': lower_option['ask'] if pd.notna(lower_option['ask']) else lower_option['last'],
                    'delta': lower_option.get('delta', 0),
                    'gamma': lower_option.get('gamma', 0),
                    'vega': lower_option.get('vega', 0),
                    'theta': lower_option.get('theta', 0),
                }
            ]
        else:
            # Debit spread: buy lower strike, sell higher strike
            legs = [
                {
                    'type': 'call',
                    'action': 'buy',
                    'strike': lower_strike,
                    'expiry': expiry,
                    'contracts': 1,
                    'price': lower_option['ask'] if pd.notna(lower_option['ask']) else lower_option['last'],
                    'delta': lower_option.get('delta', 0),
                    'gamma': lower_option.get('gamma', 0),
                    'vega': lower_option.get('vega', 0),
                    'theta': lower_option.get('theta', 0),
                },
                {
                    'type': 'call',
                    'action': 'sell',
                    'strike': higher_strike,
                    'expiry': expiry,
                    'contracts': 1,
                    'price': higher_option['bid'] if pd.notna(higher_option['bid']) else higher_option['last'],
                    'delta': higher_option.get('delta', 0),
                    'gamma': higher_option.get('gamma', 0),
                    'vega': higher_option.get('vega', 0),
                    'theta': higher_option.get('theta', 0),
                }
            ]
        
        structure_metrics = self._calculate_structure_metrics(legs)
        
        return {
            'type': 'vertical_spread',
            'subtype': subtype,
            'legs': legs,
            'metrics': structure_metrics,
            'entry_signal': opportunity['entry_signal'],
            'confidence': opportunity['confidence'],
            'expected_pnl': opportunity['expected_pnl'],
            'max_loss': structure_metrics.get('max_loss', 0),
            'max_gain': structure_metrics.get('max_gain', 0),
            'breakeven_points': structure_metrics.get('breakeven_points', []),
            'lower_strike': lower_strike,
            'higher_strike': higher_strike,
            'expiry': expiry,
            'rationale': opportunity['rationale']
        }
    
    def _build_put_vertical_spread(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build put vertical spread"""
        # Similar to call vertical spread but with puts
        puts_df = options_data['puts'].copy()
        expiry = opportunity.get('expiry')
        
        if puts_df.empty or not expiry:
            return self._empty_structure()
        
        expiry_puts = puts_df[puts_df['expiry'] == expiry].copy()
        
        if len(expiry_puts) < 2:
            return self._empty_structure()
        
        # Use suggested strikes or find optimal ones
        suggested_strikes = opportunity.get('strikes', {})
        if suggested_strikes:
            higher_strike = suggested_strikes.get('higher')
            lower_strike = suggested_strikes.get('lower')
        else:
            # Find ATM and OTM puts
            current_price = options_data['current_price']
            atm_strike = expiry_puts.loc[(expiry_puts['strike'] - current_price).abs().idxmin(), 'strike']
            otm_strikes = expiry_puts[expiry_puts['strike'] < atm_strike]

            if not otm_strikes.empty:
                higher_strike = atm_strike
                lower_strike = otm_strikes.iloc[0]['strike']
            else:
                return self._empty_structure()
        
        # Get the options data
        higher_option = expiry_puts[expiry_puts['strike'] == higher_strike].iloc[0]
        lower_option = expiry_puts[expiry_puts['strike'] == lower_strike].iloc[0]
        
        # Determine direction
        subtype = opportunity.get('subtype', 'put_credit_spread')
        
        if 'credit' in subtype:
            # Credit spread: sell lower strike, buy higher strike
            legs = [
                {
                    'type': 'put',
                    'action': 'sell',
                    'strike': lower_strike,
                    'expiry': expiry,
                    'contracts': 1,
                    'price': lower_option['bid'] if pd.notna(lower_option['bid']) else lower_option['last'],
                    'delta': lower_option.get('delta', 0),
                    'gamma': lower_option.get('gamma', 0),
                    'vega': lower_option.get('vega', 0),
                    'theta': lower_option.get('theta', 0),
                },
                {
                    'type': 'put',
                    'action': 'buy',
                    'strike': higher_strike,
                    'expiry': expiry,
                    'contracts': 1,
                    'price': higher_option['ask'] if pd.notna(higher_option['ask']) else higher_option['last'],
                    'delta': higher_option.get('delta', 0),
                    'gamma': higher_option.get('gamma', 0),
                    'vega': higher_option.get('vega', 0),
                    'theta': higher_option.get('theta', 0),
                }
            ]
        else:
            # Debit spread: buy higher strike, sell lower strike
            legs = [
                {
                    'type': 'put',
                    'action': 'buy',
                    'strike': higher_strike,
                    'expiry': expiry,
                    'contracts': 1,
                    'price': higher_option['ask'] if pd.notna(higher_option['ask']) else higher_option['last'],
                    'delta': higher_option.get('delta', 0),
                    'gamma': higher_option.get('gamma', 0),
                    'vega': higher_option.get('vega', 0),
                    'theta': higher_option.get('theta', 0),
                },
                {
                    'type': 'put',
                    'action': 'sell',
                    'strike': lower_strike,
                    'expiry': expiry,
                    'contracts': 1,
                    'price': lower_option['bid'] if pd.notna(lower_option['bid']) else lower_option['last'],
                    'delta': lower_option.get('delta', 0),
                    'gamma': lower_option.get('gamma', 0),
                    'vega': lower_option.get('vega', 0),
                    'theta': lower_option.get('theta', 0),
                }
            ]
        
        structure_metrics = self._calculate_structure_metrics(legs)
        
        return {
            'type': 'vertical_spread',
            'subtype': subtype,
            'legs': legs,
            'metrics': structure_metrics,
            'entry_signal': opportunity['entry_signal'],
            'confidence': opportunity['confidence'],
            'expected_pnl': opportunity['expected_pnl'],
            'max_loss': structure_metrics.get('max_loss', 0),
            'max_gain': structure_metrics.get('max_gain', 0),
            'breakeven_points': structure_metrics.get('breakeven_points', []),
            'higher_strike': higher_strike,
            'lower_strike': lower_strike,
            'expiry': expiry,
            'rationale': opportunity['rationale']
        }
    
    def _build_ratio_spread(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build ratio spread structure"""
        # Simplified ratio spread implementation
        return self._build_generic_structure(opportunity, vol_surface, options_data)
    
    def _build_vrp_structure(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build volatility risk premium structure"""
        subtype = opportunity.get('subtype', 'vrp_harvest')
        
        if subtype == 'vrp_harvest':
            # Simple strangle or straddle to harvest VRP
            return self._build_strangle(opportunity, vol_surface, options_data)
        elif subtype == 'skew_risk_premium':
            # Skew spread
            return self._build_skew_spread(opportunity, vol_surface, options_data)
        else:
            return self._build_generic_structure(opportunity, vol_surface, options_data)
    
    def _build_strangle(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build strangle for VRP harvesting"""
        current_price = options_data['current_price']
        calls_df = options_data['calls'].copy()
        puts_df = options_data['puts'].copy()
        
        if calls_df.empty or puts_df.empty:
            return self._empty_structure()
        
        # Find ideal expiry (30-45 DTE)
        calls_df['days_to_expiry'] = calls_df.apply(self._calculate_dte, axis=1)
        puts_df['days_to_expiry'] = puts_df.apply(self._calculate_dte, axis=1)
        
        ideal_dte = 30
        calls_df['dte_distance'] = abs(calls_df['days_to_expiry'] - ideal_dte)
        puts_df['dte_distance'] = abs(puts_df['days_to_expiry'] - ideal_dte)
        
        best_call_expiry = calls_df.loc[calls_df['dte_distance'].idxmin(), 'expiry']
        best_put_expiry = puts_df.loc[puts_df['dte_distance'].idxmin(), 'expiry']
        
        # Use same expiry if possible
        expiry = best_call_expiry if best_call_expiry == best_put_expiry else best_call_expiry
        
        # Select OTM strikes
        call_strike = current_price * 1.15  # 15% OTM
        put_strike = current_price * 0.85   # 15% OTM
        
        # Find closest available strikes
        expiry_calls = calls_df[calls_df['expiry'] == expiry]
        expiry_puts = puts_df[puts_df['expiry'] == expiry]
        
        if not expiry_calls.empty:
            call_option = expiry_calls.loc[(expiry_calls['strike'] - call_strike).abs().idxmin()]
        else:
            return self._empty_structure()
        
        if not expiry_puts.empty:
            put_option = expiry_puts.loc[(expiry_puts['strike'] - put_strike).abs().idxmin()]
        else:
            return self._empty_structure()
        
        # Build legs (sell both)
        legs = [
            {
                'type': 'call',
                'action': 'sell',
                'strike': call_option['strike'],
                'expiry': expiry,
                'contracts': 1,
                'price': call_option['bid'] if pd.notna(call_option['bid']) else call_option['last'],
                'delta': call_option.get('delta', 0),
                'gamma': call_option.get('gamma', 0),
                'vega': call_option.get('vega', 0),
                'theta': call_option.get('theta', 0),
            },
            {
                'type': 'put',
                'action': 'sell',
                'strike': put_option['strike'],
                'expiry': expiry,
                'contracts': 1,
                'price': put_option['bid'] if pd.notna(put_option['bid']) else put_option['last'],
                'delta': put_option.get('delta', 0),
                'gamma': put_option.get('gamma', 0),
                'vega': put_option.get('vega', 0),
                'theta': put_option.get('theta', 0),
            }
        ]
        
        structure_metrics = self._calculate_structure_metrics(legs)
        
        return {
            'type': 'strangle',
            'subtype': 'short_strangle',
            'legs': legs,
            'metrics': structure_metrics,
            'entry_signal': opportunity['entry_signal'],
            'confidence': opportunity['confidence'],
            'expected_pnl': opportunity['expected_pnl'],
            'max_loss': structure_metrics.get('max_loss', float('inf')),
            'max_gain': structure_metrics.get('max_gain', 0),
            'breakeven_points': structure_metrics.get('breakeven_points', []),
            'call_strike': call_option['strike'],
            'put_strike': put_option['strike'],
            'expiry': expiry,
            'rationale': opportunity['rationale']
        }
    
    def _build_skew_spread(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build skew spread for skew risk premium"""
        # Simplified implementation - would be more sophisticated in practice
        return self._build_generic_structure(opportunity, vol_surface, options_data)
    
    # ------------------------------------------------------------------
    # New Bennett-framework builders
    # ------------------------------------------------------------------

    def _build_skew_trade(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build legs from a skew_trade opportunity (already has strikes/prices)."""
        subtype = opportunity.get('subtype', '')
        strikes = opportunity.get('strikes', {})
        prices = opportunity.get('prices', {})
        expiry = opportunity.get('expiry', '')
        skew_vals = opportunity.get('skew_values', {})

        legs = []
        opt_type = 'put' if 'put' in subtype else 'call'

        if 'credit' in subtype:
            # Credit spread: sell near-ATM, buy further OTM
            sell_strike = strikes.get('sell') or strikes.get('near', 0)
            buy_strike = strikes.get('buy') or strikes.get('far', 0)
            sell_mid = prices.get('sell_mid') or prices.get('near_mid', 0)
            buy_mid = prices.get('buy_mid') or prices.get('far_mid', 0)
            legs = [
                {'type': opt_type, 'action': 'sell', 'strike': sell_strike,
                 'expiry': expiry, 'contracts': 1, 'price': sell_mid,
                 'implied_vol': skew_vals.get('atm_vol', 0),
                 'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0},
                {'type': opt_type, 'action': 'buy', 'strike': buy_strike,
                 'expiry': expiry, 'contracts': 1, 'price': buy_mid,
                 'implied_vol': skew_vals.get('atm_vol', 0),
                 'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0},
            ]
        elif 'debit' in subtype:
            # Debit spread: buy near-ATM, sell further OTM
            buy_strike = strikes.get('buy') or strikes.get('near', 0)
            sell_strike = strikes.get('sell') or strikes.get('far', 0)
            buy_mid = prices.get('buy_mid') or prices.get('near_mid', 0)
            sell_mid = prices.get('sell_mid') or prices.get('far_mid', 0)
            legs = [
                {'type': opt_type, 'action': 'buy', 'strike': buy_strike,
                 'expiry': expiry, 'contracts': 1, 'price': buy_mid,
                 'implied_vol': skew_vals.get('atm_vol', 0),
                 'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0},
                {'type': opt_type, 'action': 'sell', 'strike': sell_strike,
                 'expiry': expiry, 'contracts': 1, 'price': sell_mid,
                 'implied_vol': skew_vals.get('atm_vol', 0),
                 'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0},
            ]

        metrics = self._calculate_structure_metrics(legs) if legs else {}
        return {
            'type': 'skew_trade',
            'subtype': subtype,
            'legs': legs,
            'metrics': metrics,
            'entry_signal': opportunity.get('entry_signal', ''),
            'confidence': opportunity.get('confidence', 0),
            'expected_pnl': opportunity.get('expected_pnl', 0),
            'max_loss': opportunity.get('max_loss', 0),
            'max_gain': opportunity.get('max_gain', 0),
            'risk_reward': opportunity.get('risk_reward', 0),
            'breakeven_points': [],
            'rationale': opportunity.get('rationale', ''),
        }

    def _build_term_structure_trade(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build legs from a term_structure opportunity (calendar spread)."""
        subtype = opportunity.get('subtype', '')
        prices = opportunity.get('prices', {})
        vol_struct = opportunity.get('vol_structure', {})

        # Get expirations from vol surface
        term = vol_surface.get('term_structure', {})
        atm_vols = term.get('atm_vols', [])
        near_expiry = atm_vols[0].get('expiry', '') if atm_vols else ''
        far_expiry = atm_vols[-1].get('expiry', '') if len(atm_vols) > 1 else ''
        current_price = vol_surface.get('current_price', 0)

        # ATM strike (round to nearest standard)
        atm_strike = round(current_price)

        near_price = prices.get('near_atm', 0)
        far_price = prices.get('far_atm', 0)

        legs = []
        if subtype == 'vol_contango':
            # Sell near-term, buy far-term
            legs = [
                {'type': 'call', 'action': 'sell', 'strike': atm_strike,
                 'expiry': near_expiry, 'contracts': 1, 'price': near_price,
                 'implied_vol': vol_struct.get('short_vol', 0),
                 'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0},
                {'type': 'call', 'action': 'buy', 'strike': atm_strike,
                 'expiry': far_expiry, 'contracts': 1, 'price': far_price,
                 'implied_vol': vol_struct.get('long_vol', 0),
                 'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0},
            ]
        elif subtype == 'vol_backwardation':
            # Buy near-term, sell far-term
            legs = [
                {'type': 'call', 'action': 'buy', 'strike': atm_strike,
                 'expiry': near_expiry, 'contracts': 1, 'price': near_price,
                 'implied_vol': vol_struct.get('short_vol', 0),
                 'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0},
                {'type': 'call', 'action': 'sell', 'strike': atm_strike,
                 'expiry': far_expiry, 'contracts': 1, 'price': far_price,
                 'implied_vol': vol_struct.get('long_vol', 0),
                 'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0},
            ]

        metrics = self._calculate_structure_metrics(legs) if legs else {}
        return {
            'type': 'term_structure',
            'subtype': subtype,
            'legs': legs,
            'metrics': metrics,
            'entry_signal': opportunity.get('entry_signal', ''),
            'confidence': opportunity.get('confidence', 0),
            'expected_pnl': opportunity.get('expected_pnl', 0),
            'max_loss': opportunity.get('max_loss', 0),
            'max_gain': opportunity.get('max_gain', 0),
            'risk_reward': opportunity.get('risk_reward', 0),
            'breakeven_points': [],
            'rationale': opportunity.get('rationale', ''),
        }

    def _build_vrp_harvest(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build legs from a vrp_harvest opportunity (iron condor)."""
        strikes = opportunity.get('strikes', {})
        prices = opportunity.get('prices', {})
        expiry = opportunity.get('expiry', '')
        vol_met = opportunity.get('vol_metrics', {})
        atm_vol = vol_met.get('atm_vol', 0)

        legs = [
            {'type': 'put', 'action': 'buy', 'strike': strikes.get('put_long', 0),
             'expiry': expiry, 'contracts': 1,
             'price': prices.get('put_long_mid', 0), 'implied_vol': atm_vol,
             'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0},
            {'type': 'put', 'action': 'sell', 'strike': strikes.get('put_short', 0),
             'expiry': expiry, 'contracts': 1,
             'price': prices.get('put_short_mid', 0), 'implied_vol': atm_vol,
             'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0},
            {'type': 'call', 'action': 'sell', 'strike': strikes.get('call_short', 0),
             'expiry': expiry, 'contracts': 1,
             'price': prices.get('call_short_mid', 0), 'implied_vol': atm_vol,
             'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0},
            {'type': 'call', 'action': 'buy', 'strike': strikes.get('call_long', 0),
             'expiry': expiry, 'contracts': 1,
             'price': prices.get('call_long_mid', 0), 'implied_vol': atm_vol,
             'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0},
        ]

        metrics = self._calculate_structure_metrics(legs)
        return {
            'type': 'vrp_harvest',
            'subtype': 'iron_condor',
            'legs': legs,
            'metrics': metrics,
            'entry_signal': opportunity.get('entry_signal', ''),
            'confidence': opportunity.get('confidence', 0),
            'expected_pnl': opportunity.get('expected_pnl', 0),
            'max_loss': opportunity.get('max_loss', 0),
            'max_gain': opportunity.get('max_gain', 0),
            'risk_reward': opportunity.get('risk_reward', 0),
            'breakeven_points': [],
            'rationale': opportunity.get('rationale', ''),
        }

    def _build_combo_trade(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build combo trade structure from multi-leg optimizer output.

        The ComboOptimizer already provides fully-formed legs with BS Greeks,
        so this mostly passes through and calculates structure metrics.
        """
        legs = opportunity.get('scored_legs', [])
        if not legs:
            legs = []

        # Ensure all legs have the required fields
        for leg in legs:
            for key in ('delta', 'gamma', 'vega', 'theta'):
                if key not in leg:
                    leg[key] = 0
            if 'contracts' not in leg:
                leg['contracts'] = 1
            if 'implied_vol' not in leg:
                leg['implied_vol'] = 0

        metrics = self._calculate_structure_metrics(legs) if legs else {}

        return {
            'type': 'combo_trade',
            'subtype': opportunity.get('subtype', 'custom'),
            'legs': legs,
            'metrics': metrics,
            'iv_edge': opportunity.get('iv_edge', 0),
            'entry_signal': opportunity.get('entry_signal', 'iv_surface_mispricing'),
            'confidence': opportunity.get('confidence', 0),
            'expected_pnl': opportunity.get('expected_pnl', 0),
            'max_loss': opportunity.get('max_loss', 0),
            'max_gain': opportunity.get('max_gain', 0),
            'risk_reward': opportunity.get('risk_reward', 0),
            'breakeven_points': [],
            'rationale': opportunity.get('rationale', ''),
        }

    def _build_generic_structure(self, opportunity: Dict, vol_surface: Dict, options_data: Dict) -> Dict:
        """Build generic structure when specific type not implemented"""
        return {
            'type': 'generic',
            'subtype': opportunity.get('subtype', 'unknown'),
            'legs': [],
            'metrics': {},
            'entry_signal': opportunity['entry_signal'],
            'confidence': opportunity['confidence'],
            'expected_pnl': opportunity['expected_pnl'],
            'max_loss': 0,
            'max_gain': 0,
            'breakeven_points': [],
            'rationale': opportunity['rationale']
        }
    
    def _calculate_structure_metrics(self, legs: List[Dict]) -> Dict:
        """Calculate metrics for the trade structure"""
        if not legs:
            return {}
        
        # Sum Greeks
        total_delta = sum(leg.get('delta', 0) * leg.get('contracts', 1) for leg in legs)
        total_gamma = sum(leg.get('gamma', 0) * leg.get('contracts', 1) for leg in legs)
        total_vega = sum(leg.get('vega', 0) * leg.get('contracts', 1) for leg in legs)
        total_theta = sum(leg.get('theta', 0) * leg.get('contracts', 1) for leg in legs)
        
        # Calculate net premium
        net_premium = 0
        for leg in legs:
            price = leg.get('price', 0)
            contracts = leg.get('contracts', 1)
            if leg['action'] == 'buy':
                net_premium -= price * contracts * 100  # Debit
            else:
                net_premium += price * contracts * 100  # Credit
        
        # Payoff analysis at strike boundaries + extremes
        strikes = sorted(set(leg['strike'] for leg in legs if 'strike' in leg))
        if strikes:
            test_points = [0.0] + strikes + [strikes[-1] * 2]
            payoffs = []
            for spot in test_points:
                pnl = net_premium
                for leg in legs:
                    strike = leg.get('strike', 0)
                    opt_type = leg.get('type', leg.get('option_type', 'call'))
                    if opt_type == 'call':
                        intrinsic = max(0.0, spot - strike)
                    else:
                        intrinsic = max(0.0, strike - spot)
                    mult = leg.get('contracts', 1) * 100
                    if leg['action'] == 'buy':
                        pnl += intrinsic * mult
                    else:
                        pnl -= intrinsic * mult
                payoffs.append(pnl)
            max_gain = max(payoffs)
            max_loss = abs(min(payoffs))
        else:
            max_loss = abs(net_premium) if net_premium < 0 else 0.0
            max_gain = net_premium if net_premium > 0 else 0.0

        risk_reward = max_gain / max_loss if max_loss > 0 else float('inf')

        return {
            'total_delta': total_delta,
            'total_gamma': total_gamma,
            'total_vega': total_vega,
            'total_theta': total_theta,
            'net_premium': net_premium,
            'max_loss': max_loss,
            'max_gain': max_gain,
            'risk_reward': risk_reward,
            'theta_vega_ratio': total_theta / total_vega if total_vega != 0 else 0,
            'delta_exposure': abs(total_delta),
            'vega_exposure': abs(total_vega)
        }
    
    def _calculate_dte(self, row) -> float:
        """Calculate days to expiry from expiry date"""
        try:
            expiry_date = pd.to_datetime(row['expiry'])
            return (expiry_date - datetime.now()).days
        except:
            return np.nan
    
    def _empty_structure(self) -> Dict:
        """Return empty trade structure"""
        return {
            'type': 'unknown',
            'subtype': 'unknown',
            'legs': [],
            'metrics': {},
            'entry_signal': 'unknown',
            'confidence': 0,
            'expected_pnl': 0,
            'max_loss': 0,
            'max_gain': 0,
            'breakeven_points': [],
            'rationale': 'Unable to build trade structure'
        }