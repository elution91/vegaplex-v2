"""
Opportunity Scanner Module — Bennett Framework (pp.137-225)

Regime-aware opportunity detection using:
- 25-delta strike selection (not arbitrary adjacent strikes)
- Actual option prices for P&L estimation (not vol_diff * constant)
- Regime suitability gates (skew arb only profitable in Jumpy Vol)
- Hard risk/reward enforcement via config.min_risk_reward
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging

from combo_optimizer import ComboOptimizer

logger = logging.getLogger(__name__)

# Bennett regime suitability matrix (pp.154-169)
# True = emit, False = block, None = marginal (reduce confidence)
_REGIME_SUITABILITY = {
    'long_skew': {
        'sticky_delta': False,
        'sticky_strike': False,
        'sticky_local_vol': None,  # marginal — breakeven
        'jumpy_vol': True,
    },
    'short_skew': {
        'sticky_delta': True,
        'sticky_strike': True,
        'sticky_local_vol': None,
        'jumpy_vol': False,
    },
    'calendar': {
        'sticky_delta': True,
        'sticky_strike': True,
        'sticky_local_vol': True,
        'jumpy_vol': False,  # gamma risk
    },
    'vrp_harvest': {
        'sticky_delta': True,
        'sticky_strike': True,
        'sticky_local_vol': None,
        'jumpy_vol': False,  # gamma risk
    },
    'combo_trade': {
        'sticky_delta': True,
        'sticky_strike': True,
        'sticky_local_vol': True,
        'jumpy_vol': None,  # some combos work, some don't
    },
}


class OpportunityScanner:
    """Regime-aware opportunity scanner based on Bennett's vol trading framework."""

    def __init__(self, config):
        self.config = config
        self.combo_optimizer = ComboOptimizer(config)
        self.opportunity_types = [
            'skew_trade',
            'term_structure',
            'vrp_harvest',
            'combo_trade',
        ]

    def find_opportunities(self, symbol: str, vol_surface: Dict, skew_metrics: Dict,
                           regime_data: Optional[Dict] = None,
                           skew_context: Optional[Dict] = None) -> List[Dict]:
        """Find trading opportunities using Bennett's regime-aware framework.

        Parameters
        ----------
        symbol : str
            Ticker symbol.
        vol_surface : dict
            Output of VolatilitySurface.build_surface().
        skew_metrics : dict
            Output of SkewAnalyzer.analyze_skew().
        regime_data : dict, optional
            Output of RegimeClassifier.classify(). Contains regime label and
            confidence_multiplier.
        skew_context : dict, optional
            Output of SkewHistory.get_context(). Contains percentile data for
            all metrics. When not provided, percentile-based entry signals
            are relaxed (always pass).

        Returns
        -------
        List[Dict]
            All opportunities sorted by confidence (descending), filtered by
            risk/reward and regime suitability. Empty list if none found.
        """
        opportunities = []

        regime_label = _get_regime_label(regime_data)

        # Scan with all three scanners
        opportunities.extend(
            self._scan_skew_trades(symbol, vol_surface, skew_metrics,
                                   regime_label, skew_context)
        )
        opportunities.extend(
            self._scan_term_structure_trades(symbol, vol_surface, skew_metrics,
                                             regime_label, skew_context)
        )
        opportunities.extend(
            self._scan_vrp_trades(symbol, vol_surface, skew_metrics,
                                  regime_label, skew_context)
        )
        opportunities.extend(
            self._scan_combo_trades(symbol, vol_surface, skew_metrics,
                                    regime_label, skew_context)
        )

        if not opportunities:
            return []

        # Apply regime confidence multiplier
        if regime_data:
            multiplier = regime_data.get('confidence_multiplier', 1.0)
            for opp in opportunities:
                opp['raw_confidence'] = opp['confidence']
                opp['confidence'] = min(opp['confidence'] * multiplier, 1.0)
                opp['regime'] = regime_label
                opp['regime_multiplier'] = multiplier

        # Filter by hard economic gates
        opportunities = self.filter_opportunities(opportunities)

        # Sort by confidence
        opportunities.sort(key=lambda x: x['confidence'], reverse=True)

        return opportunities

    # ------------------------------------------------------------------
    # Scanner 1: Skew trades (replaces _scan_skew_arbitrage + _scan_vertical_spreads)
    # ------------------------------------------------------------------

    def _scan_skew_trades(self, symbol: str, vol_surface: Dict,
                          skew_metrics: Dict, regime: str,
                          skew_context: Optional[Dict]) -> List[Dict]:
        """Scan for skew-based opportunities using 25-delta reference strikes.

        Produces:
        - Risk reversals (sell overpriced wing, buy underpriced wing)
        - Vertical spreads at 25-delta with actual P&L
        """
        opportunities = []
        current_price = vol_surface.get('current_price', 0)
        if current_price <= 0:
            return opportunities

        # Extract skew slopes
        call_skew = skew_metrics.get('call_skew', {})
        put_skew = skew_metrics.get('put_skew', {})
        call_slope = call_skew.get('aggregate', {}).get('avg_slope', 0)
        put_slope = put_skew.get('aggregate', {}).get('avg_slope', 0)

        # Entry gate 1: skew must be at least min_skew_slope
        if abs(call_slope) < self.config.min_skew_slope and abs(put_slope) < self.config.min_skew_slope:
            return opportunities

        # Entry gate 2: skew percentile (if context available)
        skew_extreme = True  # default to pass if no context
        if skew_context:
            put_pct = skew_context.get('put_slope', {}).get('percentile')
            call_pct = skew_context.get('call_slope', {}).get('percentile')
            if put_pct is not None and call_pct is not None:
                skew_extreme = put_pct > 80 or put_pct < 20 or call_pct > 80 or call_pct < 20

        if not skew_extreme:
            return opportunities

        # Determine trade direction based on skew + regime
        # linregress(log_moneyness, vols): negative slope = IV rises as K falls
        # = OTM puts are expensive (normal steep put skew) -> short skew
        # Positive/flat slope = OTM puts cheap (unusual) -> long skew
        is_steep = put_slope < -self.config.min_skew_slope  # negative = steep (OTM puts overpriced)
        is_flat = put_slope > self.config.min_skew_slope  # positive = flat/inverted (rare)

        # Scan each expiry for actual trade construction
        for option_type in ['call', 'put']:
            surface_key = f'{option_type}_surface'
            surface = vol_surface.get(surface_key, {})
            if not surface.get('surfaces_by_expiry'):
                continue

            for expiry, expiry_data in surface['surfaces_by_expiry'].items():
                tte = expiry_data.get('tte', 0)
                if tte <= 0 or tte > 0.25:  # 0-90 days
                    continue

                raw_df = expiry_data.get('raw_data')
                if raw_df is None or raw_df.empty:
                    continue

                strikes = expiry_data['strikes']
                vols = expiry_data['vols']
                if len(strikes) < 3:
                    continue

                # Find ATM vol for 25-delta strike calculation
                atm_idx = np.argmin(np.abs(strikes - current_price))
                atm_vol = vols[atm_idx]
                if atm_vol <= 0 or np.isnan(atm_vol):
                    continue

                # 25-delta strike targets (Bennett standard)
                k_25d_put = current_price * np.exp(-0.675 * atm_vol * np.sqrt(tte))
                k_25d_call = current_price * np.exp(0.675 * atm_vol * np.sqrt(tte))

                # Find closest available strikes
                put_25d_idx = np.argmin(np.abs(strikes - k_25d_put))
                call_25d_idx = np.argmin(np.abs(strikes - k_25d_call))

                if put_25d_idx == call_25d_idx:
                    continue  # too narrow

                # Get actual prices from raw data
                put_25d_strike = strikes[put_25d_idx]
                call_25d_strike = strikes[call_25d_idx]

                put_25d_row = raw_df[raw_df['strike'] == put_25d_strike]
                call_25d_row = raw_df[raw_df['strike'] == call_25d_strike]
                atm_row = raw_df.iloc[(raw_df['strike'] - current_price).abs().argsort()[:1]]

                if put_25d_row.empty or call_25d_row.empty or atm_row.empty:
                    continue

                put_25d_mid = _mid_price(put_25d_row.iloc[0])
                call_25d_mid = _mid_price(call_25d_row.iloc[0])
                atm_mid = _mid_price(atm_row.iloc[0])

                if put_25d_mid <= 0 or call_25d_mid <= 0 or atm_mid <= 0:
                    continue

                # Skew measure: 25d put vol - ATM vol
                put_25d_vol = vols[put_25d_idx]
                call_25d_vol = vols[call_25d_idx]
                skew_25d = put_25d_vol - atm_vol

                # --- Short skew: sell overpriced OTM puts, buy ATM or OTM calls ---
                if is_steep and option_type == 'put':
                    trade_cat = 'short_skew'
                    suitable = _check_regime_suitability(trade_cat, regime)
                    if suitable is False:
                        continue

                    # Vertical put credit spread: sell 25d put, buy next lower strike
                    next_lower_idx = put_25d_idx - 1 if put_25d_idx > 0 else None
                    if next_lower_idx is not None and next_lower_idx >= 0:
                        hedge_strike = strikes[next_lower_idx]
                        hedge_row = raw_df[raw_df['strike'] == hedge_strike]
                        if not hedge_row.empty:
                            hedge_mid = _mid_price(hedge_row.iloc[0])
                            if hedge_mid > 0:
                                credit = put_25d_mid - hedge_mid
                                strike_width = put_25d_strike - hedge_strike
                                max_loss = (strike_width * 100) - (credit * 100)
                                max_gain = credit * 100

                                if max_loss > 0 and max_gain > 0:
                                    rr = max_gain / max_loss
                                    conf = _skew_confidence(
                                        put_slope, skew_25d, skew_context,
                                        'put_slope', suitable)

                                    opportunities.append({
                                        'type': 'skew_trade',
                                        'subtype': 'put_credit_spread',
                                        'symbol': symbol,
                                        'expiry': expiry,
                                        'confidence': conf,
                                        'direction': 'short_skew',
                                        'entry_signal': 'steep_put_skew',
                                        'regime_suitable': suitable,
                                        'strikes': {
                                            'sell': put_25d_strike,
                                            'buy': hedge_strike,
                                        },
                                        'prices': {
                                            'sell_mid': round(put_25d_mid, 2),
                                            'buy_mid': round(hedge_mid, 2),
                                        },
                                        'skew_values': {
                                            'put_slope': put_slope,
                                            'skew_25d': skew_25d,
                                            'atm_vol': atm_vol,
                                        },
                                        'expected_pnl': round(max_gain, 2),
                                        'max_loss': round(max_loss, 2),
                                        'max_gain': round(max_gain, 2),
                                        'risk_reward': round(rr, 2),
                                        'risk_level': 'low' if rr >= 2 else 'medium',
                                        'holding_period': '2-6 weeks',
                                        'rationale': (
                                            f"Put skew at P{_pct_str(skew_context, 'put_slope')}: "
                                            f"sell {put_25d_strike} put @ ${put_25d_mid:.2f}, "
                                            f"buy {hedge_strike} put @ ${hedge_mid:.2f} "
                                            f"= ${credit:.2f} credit ({rr:.1f}:1)"
                                        ),
                                    })

                # --- Long skew: buy OTM puts (cheap), sell ATM or OTM calls ---
                if is_flat and option_type == 'put':
                    trade_cat = 'long_skew'
                    suitable = _check_regime_suitability(trade_cat, regime)
                    if suitable is False:
                        continue

                    # Vertical put debit spread: buy 25d put, sell next lower strike
                    next_lower_idx = put_25d_idx - 1 if put_25d_idx > 0 else None
                    if next_lower_idx is not None and next_lower_idx >= 0:
                        hedge_strike = strikes[next_lower_idx]
                        hedge_row = raw_df[raw_df['strike'] == hedge_strike]
                        if not hedge_row.empty:
                            hedge_mid = _mid_price(hedge_row.iloc[0])
                            if hedge_mid > 0:
                                debit = put_25d_mid - hedge_mid
                                strike_width = put_25d_strike - hedge_strike
                                max_loss = debit * 100
                                max_gain = (strike_width * 100) - (debit * 100)

                                if max_loss > 0 and max_gain > 0:
                                    rr = max_gain / max_loss
                                    conf = _skew_confidence(
                                        put_slope, skew_25d, skew_context,
                                        'put_slope', suitable)

                                    opportunities.append({
                                        'type': 'skew_trade',
                                        'subtype': 'put_debit_spread',
                                        'symbol': symbol,
                                        'expiry': expiry,
                                        'confidence': conf,
                                        'direction': 'long_skew',
                                        'entry_signal': 'flat_put_skew',
                                        'regime_suitable': suitable,
                                        'strikes': {
                                            'buy': put_25d_strike,
                                            'sell': hedge_strike,
                                        },
                                        'prices': {
                                            'buy_mid': round(put_25d_mid, 2),
                                            'sell_mid': round(hedge_mid, 2),
                                        },
                                        'skew_values': {
                                            'put_slope': put_slope,
                                            'skew_25d': skew_25d,
                                            'atm_vol': atm_vol,
                                        },
                                        'expected_pnl': round(max_gain, 2),
                                        'max_loss': round(max_loss, 2),
                                        'max_gain': round(max_gain, 2),
                                        'risk_reward': round(rr, 2),
                                        'risk_level': 'low' if rr >= 2 else 'medium',
                                        'holding_period': '2-4 weeks',
                                        'rationale': (
                                            f"Flat skew at P{_pct_str(skew_context, 'put_slope')}: "
                                            f"buy {put_25d_strike} put @ ${put_25d_mid:.2f}, "
                                            f"sell {hedge_strike} put @ ${hedge_mid:.2f} "
                                            f"= ${debit:.2f} debit ({rr:.1f}:1)"
                                        ),
                                    })

                # --- Call-side skew trades ---
                # For calls: negative slope = OTM calls cheap (normal), positive = OTM calls expensive
                if option_type == 'call' and abs(call_slope) > self.config.min_skew_slope:
                    is_call_steep = call_slope < -self.config.min_skew_slope  # negative = steep (normal)
                    trade_cat = 'long_skew' if is_call_steep else 'short_skew'
                    suitable = _check_regime_suitability(trade_cat, regime)
                    if suitable is False:
                        continue

                    next_higher_idx = call_25d_idx + 1 if call_25d_idx < len(strikes) - 1 else None
                    if next_higher_idx is not None:
                        hedge_strike = strikes[next_higher_idx]
                        hedge_row = raw_df[raw_df['strike'] == hedge_strike]
                        if not hedge_row.empty:
                            hedge_mid = _mid_price(hedge_row.iloc[0])
                            if hedge_mid > 0:
                                strike_width = hedge_strike - call_25d_strike

                                if is_call_steep:
                                    # Call credit spread: sell 25d call, buy higher
                                    credit = call_25d_mid - hedge_mid
                                    max_gain = credit * 100
                                    max_loss = (strike_width * 100) - (credit * 100)
                                    subtype = 'call_credit_spread'
                                    signal = 'steep_call_skew'
                                else:
                                    # Call debit spread: buy 25d call, sell higher
                                    debit = call_25d_mid - hedge_mid
                                    max_loss = debit * 100
                                    max_gain = (strike_width * 100) - (debit * 100)
                                    subtype = 'call_debit_spread'
                                    signal = 'flat_call_skew'

                                if max_loss > 0 and max_gain > 0:
                                    rr = max_gain / max_loss
                                    conf = _skew_confidence(
                                        call_slope, call_25d_vol - atm_vol,
                                        skew_context, 'call_slope', suitable)

                                    opportunities.append({
                                        'type': 'skew_trade',
                                        'subtype': subtype,
                                        'symbol': symbol,
                                        'expiry': expiry,
                                        'confidence': conf,
                                        'direction': trade_cat,
                                        'entry_signal': signal,
                                        'regime_suitable': suitable,
                                        'strikes': {
                                            'near': call_25d_strike,
                                            'far': hedge_strike,
                                        },
                                        'prices': {
                                            'near_mid': round(call_25d_mid, 2),
                                            'far_mid': round(hedge_mid, 2),
                                        },
                                        'skew_values': {
                                            'call_slope': call_slope,
                                            'call_25d_vol': call_25d_vol,
                                            'atm_vol': atm_vol,
                                        },
                                        'expected_pnl': round(max_gain, 2),
                                        'max_loss': round(max_loss, 2),
                                        'max_gain': round(max_gain, 2),
                                        'risk_reward': round(rr, 2),
                                        'risk_level': 'low' if rr >= 2 else 'medium',
                                        'holding_period': '2-6 weeks',
                                        'rationale': (
                                            f"Call skew at P{_pct_str(skew_context, 'call_slope')}: "
                                            f"{subtype.replace('_', ' ')} "
                                            f"{call_25d_strike}/{hedge_strike} "
                                            f"({rr:.1f}:1 R/R)"
                                        ),
                                    })

        return opportunities

    # ------------------------------------------------------------------
    # Scanner 2: Term structure trades (replaces _scan_calendar_spreads)
    # ------------------------------------------------------------------

    def _scan_term_structure_trades(self, symbol: str, vol_surface: Dict,
                                    skew_metrics: Dict, regime: str,
                                    skew_context: Optional[Dict]) -> List[Dict]:
        """Scan for calendar spread opportunities using ATM term structure."""
        opportunities = []
        term_structure = vol_surface.get('term_structure', {})
        atm_vols = term_structure.get('atm_vols', [])

        if len(atm_vols) < 2:
            return opportunities

        suitable = _check_regime_suitability('calendar', regime)
        if suitable is False:
            return opportunities

        short_entry = atm_vols[0]
        long_entry = atm_vols[-1]
        short_vol = short_entry.get('atm_vol', 0)
        long_vol = long_entry.get('atm_vol', 0)

        if short_vol <= 0 or long_vol <= 0:
            return opportunities

        vol_diff = long_vol - short_vol

        # Need meaningful term structure difference
        if abs(vol_diff) < getattr(self.config, 'min_term_skew_diff', 0.02):
            return opportunities

        # Percentile check on term steepness
        term_extreme = True
        if skew_context:
            ts_pct = skew_context.get('term_steepness', {}).get('percentile')
            if ts_pct is not None:
                term_extreme = ts_pct > 75 or ts_pct < 25

        if not term_extreme:
            return opportunities

        # Get ATM option prices for the two expirations
        current_price = vol_surface.get('current_price', 0)
        near_price = _get_atm_mid_price(vol_surface, 'call', 0, current_price)
        far_price = _get_atm_mid_price(vol_surface, 'call', -1, current_price)

        if near_price <= 0 or far_price <= 0:
            return opportunities

        if vol_diff > 0:
            # Contango: sell near-term, buy far-term calendar
            net_debit = far_price - near_price
            # Calendar spread max loss ≈ net debit, max gain ≈ near premium at expiry
            max_loss = net_debit * 100
            max_gain = near_price * 100  # approximate: near option decays to 0
            subtype = 'vol_contango'
            direction = 'sell_near_buy_far'
        else:
            # Backwardation: buy near-term, sell far-term (reverse calendar)
            net_credit = near_price - far_price
            max_gain = net_credit * 100
            max_loss = far_price * 100  # approximate
            subtype = 'vol_backwardation'
            direction = 'buy_near_sell_far'

        if max_loss > 0 and max_gain > 0:
            rr = max_gain / max_loss
            pct_score = 0.5
            if skew_context:
                ts_data = skew_context.get('term_steepness', {})
                if ts_data:
                    p = ts_data.get('percentile', 50)
                    pct_score = abs(p - 50) / 50  # 0-1, higher = more extreme

            conf = min(0.3 + pct_score * 0.4 + min(abs(vol_diff) * 2, 0.3), 0.9)
            if suitable is None:
                conf *= 0.6  # marginal regime

            opportunities.append({
                'type': 'term_structure',
                'subtype': subtype,
                'symbol': symbol,
                'confidence': round(conf, 3),
                'direction': direction,
                'entry_signal': f'term_{subtype}',
                'regime_suitable': suitable,
                'vol_structure': {
                    'short_vol': round(short_vol, 4),
                    'long_vol': round(long_vol, 4),
                    'diff': round(vol_diff, 4),
                },
                'prices': {
                    'near_atm': round(near_price, 2),
                    'far_atm': round(far_price, 2),
                },
                'expected_pnl': round(max_gain, 2),
                'max_loss': round(max_loss, 2),
                'max_gain': round(max_gain, 2),
                'risk_reward': round(rr, 2),
                'risk_level': 'low' if rr >= 2 else 'medium',
                'holding_period': '1-2 months',
                'rationale': (
                    f"Term structure {subtype.replace('_', ' ')} "
                    f"({vol_diff:+.1%}): "
                    f"P{_pct_str(skew_context, 'term_steepness')} percentile, "
                    f"{rr:.1f}:1 R/R"
                ),
            })

        return opportunities

    # ------------------------------------------------------------------
    # Scanner 3: VRP trades (replaces _scan_ratio_spreads + _scan_volatility_risk_premium)
    # ------------------------------------------------------------------

    def _scan_vrp_trades(self, symbol: str, vol_surface: Dict,
                         skew_metrics: Dict, regime: str,
                         skew_context: Optional[Dict]) -> List[Dict]:
        """Scan for volatility risk premium harvesting (iron condor at 25-delta)."""
        opportunities = []
        current_price = vol_surface.get('current_price', 0)
        if current_price <= 0:
            return opportunities

        suitable = _check_regime_suitability('vrp_harvest', regime)
        if suitable is False:
            return opportunities

        # Entry gate: IV must be significantly above RV
        iv_rv_pass = False
        iv_rv_ratio = 0.0
        if skew_context:
            atm_data = skew_context.get('call_atm_vol', {})
            rv_data = skew_context.get('rv_21d', {})
            if atm_data and rv_data:
                atm_val = atm_data.get('current', 0)
                rv_val = rv_data.get('current', 0)
                if rv_val > 0:
                    iv_rv_ratio = atm_val / rv_val
                    iv_rv_pass = iv_rv_ratio > 1.15  # IV at least 15% above RV
                atm_pct = atm_data.get('percentile', 50)
                iv_rv_pass = iv_rv_pass or atm_pct > 70  # or IV elevated historically

        # Fallback: use term structure avg_vol
        term_structure = vol_surface.get('term_structure', {})
        avg_vol = term_structure.get('avg_vol', 0)
        if not iv_rv_pass and avg_vol > 0.30 and term_structure.get('contango'):
            iv_rv_pass = True

        if not iv_rv_pass:
            return opportunities

        # Build iron condor at 25-delta strikes
        # Need both call and put surfaces
        call_surface = vol_surface.get('call_surface', {})
        put_surface = vol_surface.get('put_surface', {})

        # Use first available expiry in our DTE range
        best_expiry = None
        best_tte = None
        for skey, surface in [('call', call_surface), ('put', put_surface)]:
            for expiry, edata in surface.get('surfaces_by_expiry', {}).items():
                tte = edata.get('tte', 0)
                if 0.05 <= tte <= 0.17:  # 18-60 days
                    if best_expiry is None or tte < (best_tte or 999):
                        best_expiry = expiry
                        best_tte = tte

        if best_expiry is None:
            return opportunities

        # Get call and put chains for this expiry
        call_edata = call_surface.get('surfaces_by_expiry', {}).get(best_expiry)
        put_edata = put_surface.get('surfaces_by_expiry', {}).get(best_expiry)

        if not call_edata or not put_edata:
            return opportunities

        call_strikes = call_edata['strikes']
        call_vols = call_edata['vols']
        put_strikes = put_edata['strikes']
        put_vols = put_edata['vols']
        call_raw = call_edata.get('raw_data')
        put_raw = put_edata.get('raw_data')

        if call_raw is None or put_raw is None:
            return opportunities

        if len(call_strikes) < 3 or len(put_strikes) < 3:
            return opportunities

        # ATM vol for 25-delta calculation
        atm_call_idx = np.argmin(np.abs(call_strikes - current_price))
        atm_vol = call_vols[atm_call_idx]
        if atm_vol <= 0 or np.isnan(atm_vol):
            return opportunities

        # 25-delta strikes
        k_put_25d = current_price * np.exp(-0.675 * atm_vol * np.sqrt(best_tte))
        k_call_25d = current_price * np.exp(0.675 * atm_vol * np.sqrt(best_tte))

        # Find actual strikes + wing strikes (for defined risk)
        put_short_idx = np.argmin(np.abs(put_strikes - k_put_25d))
        call_short_idx = np.argmin(np.abs(call_strikes - k_call_25d))

        put_long_idx = put_short_idx - 1 if put_short_idx > 0 else None
        call_long_idx = call_short_idx + 1 if call_short_idx < len(call_strikes) - 1 else None

        if put_long_idx is None or call_long_idx is None:
            return opportunities

        # Get actual prices
        put_short_strike = put_strikes[put_short_idx]
        put_long_strike = put_strikes[put_long_idx]
        call_short_strike = call_strikes[call_short_idx]
        call_long_strike = call_strikes[call_long_idx]

        put_short_mid = _get_strike_mid(put_raw, put_short_strike)
        put_long_mid = _get_strike_mid(put_raw, put_long_strike)
        call_short_mid = _get_strike_mid(call_raw, call_short_strike)
        call_long_mid = _get_strike_mid(call_raw, call_long_strike)

        if any(p <= 0 for p in [put_short_mid, put_long_mid, call_short_mid, call_long_mid]):
            return opportunities

        # Iron condor P&L
        credit = (put_short_mid - put_long_mid) + (call_short_mid - call_long_mid)
        put_width = put_short_strike - put_long_strike
        call_width = call_long_strike - call_short_strike
        max_width = max(put_width, call_width)

        max_gain = credit * 100
        max_loss = (max_width * 100) - max_gain

        if max_loss <= 0 or max_gain <= 0:
            return opportunities

        rr = max_gain / max_loss

        # Confidence based on IV-RV ratio + percentile
        conf = min(0.3 + min((iv_rv_ratio - 1.0) * 2, 0.3), 0.8)
        if skew_context:
            atm_pct = skew_context.get('call_atm_vol', {}).get('percentile', 50)
            if atm_pct > 80:
                conf += 0.1
        if suitable is None:
            conf *= 0.6

        opportunities.append({
            'type': 'vrp_harvest',
            'subtype': 'iron_condor',
            'symbol': symbol,
            'expiry': best_expiry,
            'confidence': round(min(conf, 0.9), 3),
            'direction': 'sell_volatility',
            'entry_signal': 'iv_above_rv',
            'regime_suitable': suitable,
            'strikes': {
                'put_long': put_long_strike,
                'put_short': put_short_strike,
                'call_short': call_short_strike,
                'call_long': call_long_strike,
            },
            'prices': {
                'put_short_mid': round(put_short_mid, 2),
                'put_long_mid': round(put_long_mid, 2),
                'call_short_mid': round(call_short_mid, 2),
                'call_long_mid': round(call_long_mid, 2),
            },
            'vol_metrics': {
                'atm_vol': round(atm_vol, 4),
                'iv_rv_ratio': round(iv_rv_ratio, 2),
            },
            'expected_pnl': round(max_gain, 2),
            'max_loss': round(max_loss, 2),
            'max_gain': round(max_gain, 2),
            'risk_reward': round(rr, 2),
            'risk_level': 'low' if rr >= 1.5 else 'medium',
            'holding_period': '3-6 weeks',
            'rationale': (
                f"IV/RV={iv_rv_ratio:.2f}: iron condor "
                f"{put_long_strike}/{put_short_strike}p "
                f"{call_short_strike}/{call_long_strike}c "
                f"= ${credit:.2f} credit ({rr:.1f}:1)"
            ),
        })

        return opportunities

    # ------------------------------------------------------------------
    # Filter + scoring
    # ------------------------------------------------------------------

    def filter_opportunities(self, opportunities: List[Dict]) -> List[Dict]:
        """Filter by hard economic gates — enforces config.min_risk_reward."""
        filtered = []
        min_rr = getattr(self.config, 'min_risk_reward', 2.0)

        for opp in opportunities:
            # Hard gate: risk/reward
            rr = opp.get('risk_reward', 0)
            if rr < min_rr:
                continue

            # Hard gate: regime must not block
            if opp.get('regime_suitable') is False:
                continue

            # Hard gate: positive expected P&L (combo trades use IV edge instead)
            if opp.get('type') != 'combo_trade' and opp.get('expected_pnl', 0) <= 0:
                continue

            # Hard gate: max_loss must be finite and reasonable
            # (combo ratio spreads may have high/undefined risk, handled by their own filter)
            if opp.get('type') != 'combo_trade' and opp.get('max_loss', 0) <= 0:
                continue

            filtered.append(opp)

        return filtered

    def _scan_combo_trades(self, symbol: str, vol_surface: Dict,
                           skew_metrics: Dict, regime: str,
                           skew_context: Optional[Dict]) -> List[Dict]:
        """Scan for multi-leg combo opportunities via the optimizer."""
        try:
            regime_data = {'regime': regime}
            combos = self.combo_optimizer.find_combos(
                symbol, vol_surface, skew_metrics,
                regime_data=regime_data, skew_context=skew_context
            )
            return combos
        except Exception as e:
            logger.warning(f"Combo optimizer failed for {symbol}: {e}")
            return []

    def calculate_opportunity_score(self, opportunity: Dict) -> float:
        """Calculate overall opportunity score."""
        conf = opportunity.get('confidence', 0)
        rr = opportunity.get('risk_reward', 1)
        rr_bonus = min((rr - 1) * 0.1, 0.2)
        return min(conf + rr_bonus, 1.0)


# ======================================================================
# Helper functions
# ======================================================================

def _get_regime_label(regime_data: Optional[Dict]) -> str:
    """Extract regime label string from regime_data."""
    if not regime_data:
        return 'unknown'
    label = regime_data.get('regime', 'unknown')
    if hasattr(label, 'value'):
        label = label.value
    return label


def _check_regime_suitability(trade_category: str, regime: str):
    """Check Bennett regime suitability. Returns True/False/None."""
    table = _REGIME_SUITABILITY.get(trade_category, {})
    return table.get(regime, None)  # default to marginal if regime unknown


def _mid_price(row) -> float:
    """Calculate mid price from a DataFrame row with bid/ask."""
    bid = row.get('bid', 0) if hasattr(row, 'get') else getattr(row, 'bid', 0)
    ask = row.get('ask', 0) if hasattr(row, 'get') else getattr(row, 'ask', 0)
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    last = row.get('last_price', 0) if hasattr(row, 'get') else getattr(row, 'last_price', 0)
    if last > 0:
        return last
    return 0.0


def _get_strike_mid(raw_df: pd.DataFrame, strike: float) -> float:
    """Get mid price for a specific strike from raw data."""
    row = raw_df[raw_df['strike'] == strike]
    if row.empty:
        return 0.0
    return _mid_price(row.iloc[0])


def _get_atm_mid_price(vol_surface: Dict, option_type: str,
                       expiry_idx: int, current_price: float) -> float:
    """Get ATM mid price for a given expiry index (0=first, -1=last)."""
    surface = vol_surface.get(f'{option_type}_surface', {})
    expiries = list(surface.get('surfaces_by_expiry', {}).keys())
    if not expiries:
        return 0.0
    try:
        expiry = expiries[expiry_idx]
    except IndexError:
        return 0.0
    edata = surface['surfaces_by_expiry'][expiry]
    raw_df = edata.get('raw_data')
    if raw_df is None or raw_df.empty:
        return 0.0
    atm_row = raw_df.iloc[(raw_df['strike'] - current_price).abs().argsort()[:1]]
    if atm_row.empty:
        return 0.0
    return _mid_price(atm_row.iloc[0])


def _pct_str(skew_context: Optional[Dict], metric: str) -> str:
    """Format percentile as string for rationale text."""
    if not skew_context:
        return '?'
    data = skew_context.get(metric, {})
    pct = data.get('percentile')
    if pct is not None:
        return f'{pct:.0f}'
    return '?'


def _skew_confidence(slope: float, skew_25d: float,
                     skew_context: Optional[Dict], metric: str,
                     regime_suitable) -> float:
    """Calculate confidence score for skew trades.

    Combines:
    - Slope magnitude (0-0.3)
    - Percentile extremity (0-0.3)
    - Skew 25d magnitude (0-0.2)
    - Base (0.1)
    """
    conf = 0.1

    # Slope magnitude
    conf += min(abs(slope) * 0.5, 0.3)

    # Percentile extremity
    if skew_context:
        data = skew_context.get(metric, {})
        pct = data.get('percentile', 50)
        extremity = abs(pct - 50) / 50  # 0-1
        conf += extremity * 0.3

    # 25-delta skew magnitude
    conf += min(abs(skew_25d) * 0.5, 0.2)

    # Regime penalty for marginal
    if regime_suitable is None:
        conf *= 0.6

    return round(min(conf, 0.9), 3)
