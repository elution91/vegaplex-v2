"""
Multi-Leg Combo Optimizer — Vol Surface Mispricing Scanner

Scores every option on the vol surface for IV richness/cheapness
relative to the fitted spline, then constructs professional-grade
multi-leg combo structures (risk reversals, ratios, broken wings,
diagonals, calendar skews, asymmetric condors, custom N-leg).

Three phases:
  1. score_surface()  — IV residual z-scoring per option
  2. generate_candidates() — template-based structure generation
  3. optimize() — composite scoring + hard risk filters

Output format matches OpportunityScanner for seamless dashboard integration.
"""

import math
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging

from bs_greeks import BSGreeks, GreeksResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

class ComboType(str, Enum):
    RISK_REVERSAL = 'risk_reversal'
    RATIO_SPREAD = 'ratio_spread'
    BROKEN_WING_BUTTERFLY = 'broken_wing_butterfly'
    CALENDAR_SKEW = 'calendar_skew'
    DIAGONAL = 'diagonal'
    ASYMMETRIC_CONDOR = 'asymmetric_condor'
    CUSTOM_N_LEG = 'custom_n_leg'


@dataclass
class ScoredOption:
    """An option with its IV richness score and Greeks."""
    option_type: str          # 'call' or 'put'
    strike: float
    expiry: str
    tte: float                # years to expiry
    market_iv: float          # market-observed IV
    model_iv: float           # smooth surface IV (from spline)
    iv_residual: float        # market_iv - model_iv (positive = overpriced)
    iv_zscore: float          # standardised residual
    bid: float
    ask: float
    mid: float
    greeks: GreeksResult
    richness_score: float     # tanh(zscore), bounded [-1, +1]
    open_interest: int = 0
    volume: int = 0


@dataclass
class ComboCandidate:
    """A fully specified multi-leg combo before final filtering."""
    combo_type: ComboType
    legs: List[Dict]          # same format as existing trade_structures legs
    net_premium: float        # positive = credit
    iv_edge: float            # weighted average IV residual (sell-buy)
    total_delta: float
    total_gamma: float
    total_vega: float
    total_theta: float
    max_loss: float
    max_gain: float
    risk_reward: float
    score: float              # composite score
    rationale: str
    expiry: str = ''          # primary expiry
    risk_level: str = 'medium'


# ---------------------------------------------------------------------------
# Regime suitability for combo types
# ---------------------------------------------------------------------------

_COMBO_REGIME_SUITABILITY = {
    ComboType.RISK_REVERSAL: {
        'sticky_delta': True,   # sell skew
        'sticky_strike': True,
        'sticky_local_vol': None,
        'jumpy_vol': True,      # buy skew
    },
    ComboType.RATIO_SPREAD: {
        'sticky_delta': True,
        'sticky_strike': True,
        'sticky_local_vol': None,
        'jumpy_vol': False,     # undefined risk + gamma
    },
    ComboType.BROKEN_WING_BUTTERFLY: {
        'sticky_delta': True,
        'sticky_strike': True,
        'sticky_local_vol': True,
        'jumpy_vol': None,
    },
    ComboType.CALENDAR_SKEW: {
        'sticky_delta': True,
        'sticky_strike': True,
        'sticky_local_vol': True,
        'jumpy_vol': False,
    },
    ComboType.DIAGONAL: {
        'sticky_delta': True,
        'sticky_strike': True,
        'sticky_local_vol': True,
        'jumpy_vol': None,
    },
    ComboType.ASYMMETRIC_CONDOR: {
        'sticky_delta': True,
        'sticky_strike': True,
        'sticky_local_vol': None,
        'jumpy_vol': False,
    },
    ComboType.CUSTOM_N_LEG: {
        'sticky_delta': True,
        'sticky_strike': True,
        'sticky_local_vol': True,
        'jumpy_vol': None,
    },
}


def _check_combo_regime(combo_type: ComboType, regime: str):
    """Returns True/False/None for regime suitability."""
    table = _COMBO_REGIME_SUITABILITY.get(combo_type, {})
    return table.get(regime, None)


# ---------------------------------------------------------------------------
# Main optimizer class
# ---------------------------------------------------------------------------

class ComboOptimizer:
    """Multi-leg combo optimizer driven by vol surface mispricing."""

    def __init__(self, config, bs_calc: BSGreeks = None):
        self.config = config
        self.bs = bs_calc or BSGreeks(
            risk_free_rate=getattr(config, 'risk_free_rate', 0.05)
        )
        self.max_legs = getattr(config, 'combo_max_legs', 6)
        self.max_delta = getattr(config, 'max_delta_exposure', 0.30)
        self.max_vega = getattr(config, 'max_vega_exposure', 0.50)
        self.min_risk_reward = getattr(config, 'min_risk_reward', 2.0)
        self.min_richness = getattr(config, 'combo_min_richness', 0.3)
        self.min_iv_edge = getattr(config, 'combo_min_iv_edge', 0.005)
        self.liquidity_filter = getattr(config, 'combo_liquidity_filter', True)
        self.min_oi = getattr(config, 'combo_min_open_interest', 10)
        self.max_spread_pct = getattr(config, 'combo_max_bid_ask_spread_pct', 0.40)

    # ------------------------------------------------------------------
    # Phase 1: Score the vol surface
    # ------------------------------------------------------------------

    def score_surface(self, vol_surface: Dict) -> Dict[str, List[ScoredOption]]:
        """
        Score every option on the vol surface for IV richness/cheapness.

        Uses spline residual z-score method:
        1. For each expiry, get the spline-fitted smooth IV curve
        2. Compute iv_residual = market_iv - spline_iv(strike)
        3. Standardize across the expiry
        4. richness_score = tanh(z-score), bounded [-1, +1]

        Returns: {'call': [ScoredOption, ...], 'put': [ScoredOption, ...]}
        """
        current_price = vol_surface.get('current_price', 0)
        if current_price <= 0:
            return {'call': [], 'put': []}

        scored = {'call': [], 'put': []}

        for opt_type in ['call', 'put']:
            surface_key = f'{opt_type}_surface'
            surface = vol_surface.get(surface_key, {})
            if not surface.get('surfaces_by_expiry'):
                continue

            for expiry, expiry_data in surface['surfaces_by_expiry'].items():
                spline = expiry_data.get('spline')
                raw_df = expiry_data.get('raw_data')
                tte = expiry_data.get('tte', 0)

                if spline is None or raw_df is None or raw_df.empty or tte <= 0:
                    continue

                # Compute residuals for all options at this expiry
                expiry_scored = []
                for _, row in raw_df.iterrows():
                    strike = row.get('strike', 0)
                    market_iv = row.get('implied_vol', 0)
                    bid = row.get('bid', 0)
                    ask = row.get('ask', 0)
                    oi = row.get('open_interest', 0)
                    vol = row.get('volume', 0)

                    # Handle NaN values from yfinance
                    if pd.isna(strike) or pd.isna(market_iv):
                        continue
                    if pd.isna(bid):
                        bid = 0
                    if pd.isna(ask):
                        ask = 0
                    if pd.isna(oi):
                        oi = 0
                    if pd.isna(vol):
                        vol = 0

                    if market_iv <= 0.01 or strike <= 0:
                        continue

                    # Moneyness filter: skip deep ITM/OTM options
                    # Only consider strikes within 0.70-1.30 of current price
                    moneyness = strike / current_price
                    if moneyness < 0.70 or moneyness > 1.30:
                        continue

                    # Model IV from spline
                    try:
                        model_iv = float(spline(strike))
                    except Exception:
                        continue

                    if model_iv <= 0.01:
                        continue

                    iv_residual = market_iv - model_iv
                    mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else row.get('last_price', 0)

                    # Liquidity filter
                    if self.liquidity_filter:
                        if bid <= 0 or ask <= 0:
                            continue
                        if isinstance(oi, (int, float)) and oi < self.min_oi:
                            continue
                        if mid > 0 and (ask - bid) / mid > self.max_spread_pct:
                            continue

                    # Compute Greeks via BS
                    greeks = self.bs.calculate(
                        S=current_price, K=strike, T=tte,
                        sigma=market_iv, option_type=opt_type
                    )

                    # Delta filter: skip options with |delta| > 0.85
                    # Deep ITM options are stock proxies with no vol sensitivity
                    if abs(greeks.delta) > 0.85:
                        continue

                    # Extrinsic value filter: skip options where extrinsic < 15%
                    # of mid price (deep ITM with mostly intrinsic value)
                    if mid > 0:
                        if opt_type == 'call':
                            intrinsic = max(current_price - strike, 0)
                        else:
                            intrinsic = max(strike - current_price, 0)
                        extrinsic = mid - intrinsic
                        if extrinsic < 0.15 * mid and intrinsic > 0:
                            continue

                    expiry_scored.append(ScoredOption(
                        option_type=opt_type,
                        strike=strike,
                        expiry=str(expiry),
                        tte=tte,
                        market_iv=market_iv,
                        model_iv=model_iv,
                        iv_residual=iv_residual,
                        iv_zscore=0.0,  # computed below
                        bid=bid,
                        ask=ask,
                        mid=mid,
                        greeks=greeks,
                        richness_score=0.0,  # computed below
                        open_interest=int(oi) if isinstance(oi, (int, float)) else 0,
                        volume=int(vol) if isinstance(vol, (int, float)) else 0,
                    ))

                if not expiry_scored:
                    continue

                # Standardize residuals within this expiry
                residuals = np.array([s.iv_residual for s in expiry_scored])
                mean_r = np.mean(residuals)
                std_r = np.std(residuals)

                for s in expiry_scored:
                    if std_r > 0.001:
                        s.iv_zscore = (s.iv_residual - mean_r) / std_r
                    else:
                        s.iv_zscore = 0.0
                    s.richness_score = float(np.tanh(s.iv_zscore))

                scored[opt_type].extend(expiry_scored)

        return scored

    # ------------------------------------------------------------------
    # Phase 2: Generate candidate structures
    # ------------------------------------------------------------------

    def generate_candidates(self, scored: Dict[str, List[ScoredOption]],
                            current_price: float,
                            regime: str = 'unknown') -> List[ComboCandidate]:
        """Generate candidate combo structures from scored options."""
        candidates = []

        generators = [
            (ComboType.RISK_REVERSAL, self._gen_risk_reversals),
            (ComboType.RATIO_SPREAD, self._gen_ratio_spreads),
            (ComboType.BROKEN_WING_BUTTERFLY, self._gen_broken_wing_butterflies),
            (ComboType.CALENDAR_SKEW, self._gen_calendar_skews),
            (ComboType.DIAGONAL, self._gen_diagonals),
            (ComboType.ASYMMETRIC_CONDOR, self._gen_asymmetric_condors),
            (ComboType.CUSTOM_N_LEG, self._gen_custom_n_leg),
        ]

        for combo_type, generator in generators:
            # Check regime suitability
            suitable = _check_combo_regime(combo_type, regime)
            if suitable is False:
                continue

            try:
                results = generator(scored, current_price, regime)
                # Penalize marginal regime
                if suitable is None:
                    for c in results:
                        c.score *= 0.7
                candidates.extend(results)
            except Exception as e:
                logger.warning(f"Combo generator {combo_type.value} failed: {e}")

        return candidates

    # ------------------------------------------------------------------
    # Template generators
    # ------------------------------------------------------------------

    def _gen_risk_reversals(self, scored: Dict[str, List[ScoredOption]],
                            current_price: float, regime: str) -> List[ComboCandidate]:
        """
        Risk Reversal: Sell overpriced OTM put + buy underpriced OTM call (or vice versa).
        Pure skew play — profit from skew mean-reversion.
        """
        candidates = []
        calls = scored.get('call', [])
        puts = scored.get('put', [])

        if not calls or not puts:
            return candidates

        # Group by expiry
        call_by_exp = _group_by_expiry(calls)
        put_by_exp = _group_by_expiry(puts)

        common_expiries = set(call_by_exp.keys()) & set(put_by_exp.keys())

        for expiry in common_expiries:
            exp_calls = call_by_exp[expiry]
            exp_puts = put_by_exp[expiry]

            # OTM options only
            otm_calls = [c for c in exp_calls if c.strike > current_price]
            otm_puts = [p for p in exp_puts if p.strike < current_price]

            if not otm_calls or not otm_puts:
                continue

            # Strategy A: Sell rich put + buy cheap call (bullish risk reversal)
            rich_puts = sorted(otm_puts, key=lambda x: x.richness_score, reverse=True)
            cheap_calls = sorted(otm_calls, key=lambda x: x.richness_score)

            for sell_put in rich_puts[:2]:
                if sell_put.richness_score < self.min_richness:
                    continue
                for buy_call in cheap_calls[:2]:
                    if buy_call.richness_score > -self.min_richness * 0.5:
                        continue  # not cheap enough

                    combo = self._build_combo(
                        combo_type=ComboType.RISK_REVERSAL,
                        legs_spec=[
                            ('sell', sell_put),
                            ('buy', buy_call),
                        ],
                        current_price=current_price,
                        rationale=(
                            f"Sell overpriced {sell_put.strike} put "
                            f"(richness {sell_put.richness_score:+.2f}) + "
                            f"buy cheap {buy_call.strike} call "
                            f"(richness {buy_call.richness_score:+.2f})"
                        ),
                        risk_level='medium',
                    )
                    if combo:
                        candidates.append(combo)

            # Strategy B: Sell rich call + buy cheap put (bearish risk reversal)
            rich_calls = sorted(otm_calls, key=lambda x: x.richness_score, reverse=True)
            cheap_puts = sorted(otm_puts, key=lambda x: x.richness_score)

            for sell_call in rich_calls[:2]:
                if sell_call.richness_score < self.min_richness:
                    continue
                for buy_put in cheap_puts[:2]:
                    if buy_put.richness_score > -self.min_richness * 0.5:
                        continue

                    combo = self._build_combo(
                        combo_type=ComboType.RISK_REVERSAL,
                        legs_spec=[
                            ('sell', sell_call),
                            ('buy', buy_put),
                        ],
                        current_price=current_price,
                        rationale=(
                            f"Sell overpriced {sell_call.strike} call "
                            f"(richness {sell_call.richness_score:+.2f}) + "
                            f"buy cheap {buy_put.strike} put "
                            f"(richness {buy_put.richness_score:+.2f})"
                        ),
                        risk_level='medium',
                    )
                    if combo:
                        candidates.append(combo)

        return candidates

    def _gen_ratio_spreads(self, scored: Dict[str, List[ScoredOption]],
                           current_price: float, regime: str) -> List[ComboCandidate]:
        """
        1x2 Ratio Spread: Buy 1 ATM + sell 2 OTM.
        Net credit, profit from vol convergence. Undefined risk on sell side.
        """
        candidates = []

        for opt_type in ['call', 'put']:
            options = scored.get(opt_type, [])
            if not options:
                continue

            by_exp = _group_by_expiry(options)

            for expiry, exp_opts in by_exp.items():
                # Find near-ATM option (fairest priced)
                atm_opts = sorted(exp_opts, key=lambda x: abs(x.strike - current_price))
                if not atm_opts:
                    continue

                buy_opt = atm_opts[0]  # closest to ATM

                # Find overpriced OTM options to sell 2x
                if opt_type == 'call':
                    otm = [o for o in exp_opts if o.strike > buy_opt.strike
                           and o.richness_score > self.min_richness]
                else:
                    otm = [o for o in exp_opts if o.strike < buy_opt.strike
                           and o.richness_score > self.min_richness]

                otm_sorted = sorted(otm, key=lambda x: x.richness_score, reverse=True)

                for sell_opt in otm_sorted[:3]:
                    # Check net credit: 2 * sell_mid - buy_mid > 0
                    net = 2 * sell_opt.mid - buy_opt.mid
                    if net <= 0:
                        continue

                    combo = self._build_combo(
                        combo_type=ComboType.RATIO_SPREAD,
                        legs_spec=[
                            ('buy', buy_opt, 1),
                            ('sell', sell_opt, 2),
                        ],
                        current_price=current_price,
                        rationale=(
                            f"1x2 {opt_type} ratio: buy 1x {buy_opt.strike} "
                            f"+ sell 2x {sell_opt.strike} "
                            f"(richness {sell_opt.richness_score:+.2f}, "
                            f"net credit ${net:.2f})"
                        ),
                        risk_level='high',
                    )
                    if combo:
                        candidates.append(combo)

        return candidates

    def _gen_broken_wing_butterflies(self, scored: Dict[str, List[ScoredOption]],
                                     current_price: float, regime: str) -> List[ComboCandidate]:
        """
        Broken Wing Butterfly: 3 strikes, offset center.
        Credit structure with asymmetric risk based on skew richness.
        """
        candidates = []

        for opt_type in ['put']:  # put-side BWB is most common for skew plays
            options = scored.get(opt_type, [])
            if not options:
                continue

            by_exp = _group_by_expiry(options)

            for expiry, exp_opts in by_exp.items():
                if len(exp_opts) < 5:
                    continue

                sorted_by_strike = sorted(exp_opts, key=lambda x: x.strike)
                strikes = [o.strike for o in sorted_by_strike]

                # Find body near ATM
                atm_idx = min(range(len(strikes)),
                              key=lambda i: abs(strikes[i] - current_price))

                # Try different wing spans
                for lower_offset in range(2, min(5, atm_idx + 1)):
                    for upper_offset in range(1, min(4, len(strikes) - atm_idx)):
                        if lower_offset == upper_offset:
                            continue  # need asymmetry

                        lower_idx = atm_idx - lower_offset
                        upper_idx = atm_idx + upper_offset

                        if lower_idx < 0 or upper_idx >= len(strikes):
                            continue

                        lower = sorted_by_strike[lower_idx]
                        body = sorted_by_strike[atm_idx]
                        upper = sorted_by_strike[upper_idx]

                        # BWB: buy 1 lower, sell 2 body, buy 1 upper
                        combo = self._build_combo(
                            combo_type=ComboType.BROKEN_WING_BUTTERFLY,
                            legs_spec=[
                                ('buy', lower, 1),
                                ('sell', body, 2),
                                ('buy', upper, 1),
                            ],
                            current_price=current_price,
                            rationale=(
                                f"BWB {opt_type}: buy {lower.strike}, "
                                f"sell 2x {body.strike}, buy {upper.strike} "
                                f"(body richness {body.richness_score:+.2f})"
                            ),
                            risk_level='medium',
                        )
                        if combo and combo.net_premium > 0:  # credit only
                            candidates.append(combo)

                        if len(candidates) > 20:  # limit per expiry
                            break
                    if len(candidates) > 20:
                        break

        return candidates[:10]  # keep best

    def _gen_calendar_skews(self, scored: Dict[str, List[ScoredOption]],
                            current_price: float, regime: str) -> List[ComboCandidate]:
        """
        Calendar Skew: Sell near-term overpriced + buy far-term underpriced
        at same or nearby strike. Exploits term structure + IV differential.
        """
        candidates = []

        for opt_type in ['call', 'put']:
            options = scored.get(opt_type, [])
            if not options:
                continue

            by_exp = _group_by_expiry(options)
            expiries = sorted(by_exp.keys())

            if len(expiries) < 2:
                continue

            # Compare each near-term with each far-term
            for i, near_exp in enumerate(expiries[:-1]):
                for far_exp in expiries[i + 1:]:
                    near_opts = by_exp[near_exp]
                    far_opts = by_exp[far_exp]

                    # Find near-term options that are overpriced
                    rich_near = [o for o in near_opts if o.richness_score > self.min_richness]
                    # Find far-term options that are underpriced
                    cheap_far = [o for o in far_opts if o.richness_score < -self.min_richness * 0.3]

                    for sell_near in rich_near[:3]:
                        # Find closest strike in far-term
                        best_far = min(cheap_far, key=lambda x: abs(x.strike - sell_near.strike),
                                       default=None)
                        if best_far is None:
                            continue

                        # Strike must be close enough
                        if abs(best_far.strike - sell_near.strike) / current_price > 0.05:
                            continue

                        combo = self._build_combo(
                            combo_type=ComboType.CALENDAR_SKEW,
                            legs_spec=[
                                ('sell', sell_near),
                                ('buy', best_far),
                            ],
                            current_price=current_price,
                            rationale=(
                                f"Calendar {opt_type}: sell {sell_near.expiry} "
                                f"{sell_near.strike} (richness {sell_near.richness_score:+.2f}) "
                                f"+ buy {best_far.expiry} {best_far.strike} "
                                f"(richness {best_far.richness_score:+.2f})"
                            ),
                            risk_level='medium',
                        )
                        if combo:
                            candidates.append(combo)

        return candidates

    def _gen_diagonals(self, scored: Dict[str, List[ScoredOption]],
                       current_price: float, regime: str) -> List[ComboCandidate]:
        """
        Diagonal: Buy DITM near-term (low extrinsic) + sell OTM far-term (high richness).
        Edge is in IV differential — like the trader's SMCX example.
        """
        candidates = []

        for opt_type in ['call', 'put']:
            options = scored.get(opt_type, [])
            if not options:
                continue

            by_exp = _group_by_expiry(options)
            expiries = sorted(by_exp.keys())

            if len(expiries) < 2:
                continue

            for i, near_exp in enumerate(expiries[:-1]):
                for far_exp in expiries[i + 1:]:
                    near_opts = by_exp[near_exp]
                    far_opts = by_exp[far_exp]

                    # Buy DITM near-term (high delta, low extrinsic)
                    if opt_type == 'call':
                        ditm_near = [o for o in near_opts
                                     if o.strike < current_price * 0.92
                                     and o.greeks.delta > 0.80]
                    else:
                        ditm_near = [o for o in near_opts
                                     if o.strike > current_price * 1.08
                                     and o.greeks.delta < -0.80]

                    # Sell overpriced OTM far-term
                    if opt_type == 'call':
                        otm_far = [o for o in far_opts
                                   if o.strike > current_price
                                   and o.richness_score > self.min_richness]
                    else:
                        otm_far = [o for o in far_opts
                                   if o.strike < current_price
                                   and o.richness_score > self.min_richness]

                    otm_far_sorted = sorted(otm_far, key=lambda x: x.richness_score,
                                            reverse=True)

                    for buy_opt in ditm_near[:2]:
                        for sell_opt in otm_far_sorted[:3]:
                            # IV differential must be positive (selling higher IV)
                            if sell_opt.market_iv <= buy_opt.market_iv:
                                continue

                            combo = self._build_combo(
                                combo_type=ComboType.DIAGONAL,
                                legs_spec=[
                                    ('buy', buy_opt),
                                    ('sell', sell_opt),
                                ],
                                current_price=current_price,
                                rationale=(
                                    f"Diagonal {opt_type}: buy DITM {buy_opt.strike} "
                                    f"({buy_opt.expiry}, IV={buy_opt.market_iv:.1%}) "
                                    f"+ sell OTM {sell_opt.strike} "
                                    f"({sell_opt.expiry}, IV={sell_opt.market_iv:.1%}, "
                                    f"richness {sell_opt.richness_score:+.2f})"
                                ),
                                risk_level='medium',
                            )
                            if combo:
                                candidates.append(combo)

        return candidates

    def _gen_asymmetric_condors(self, scored: Dict[str, List[ScoredOption]],
                                current_price: float, regime: str) -> List[ComboCandidate]:
        """
        Asymmetric Iron Condor: 4-leg condor with wing widths proportional
        to skew richness per side. Wider spread where IV is more overpriced.
        """
        candidates = []
        calls = scored.get('call', [])
        puts = scored.get('put', [])

        if not calls or not puts:
            return candidates

        call_by_exp = _group_by_expiry(calls)
        put_by_exp = _group_by_expiry(puts)
        common_expiries = set(call_by_exp.keys()) & set(put_by_exp.keys())

        for expiry in common_expiries:
            exp_calls = sorted(call_by_exp[expiry], key=lambda x: x.strike)
            exp_puts = sorted(put_by_exp[expiry], key=lambda x: x.strike)

            # OTM options
            otm_calls = [c for c in exp_calls if c.strike > current_price]
            otm_puts = [p for p in exp_puts if p.strike < current_price]

            if len(otm_calls) < 2 or len(otm_puts) < 2:
                continue

            # Sell strikes: highest richness OTM on each side
            sell_put = max(otm_puts, key=lambda x: x.richness_score)
            sell_call = max(otm_calls, key=lambda x: x.richness_score)

            if sell_put.richness_score < 0.1 and sell_call.richness_score < 0.1:
                continue

            # Buy wings: cheapest (most underpriced) further OTM for protection
            buy_puts = [p for p in otm_puts if p.strike < sell_put.strike]
            buy_calls = [c for c in otm_calls if c.strike > sell_call.strike]

            if not buy_puts or not buy_calls:
                continue

            buy_put = min(buy_puts, key=lambda x: x.richness_score)
            buy_call = min(buy_calls, key=lambda x: x.richness_score)

            combo = self._build_combo(
                combo_type=ComboType.ASYMMETRIC_CONDOR,
                legs_spec=[
                    ('buy', buy_put),
                    ('sell', sell_put),
                    ('sell', sell_call),
                    ('buy', buy_call),
                ],
                current_price=current_price,
                rationale=(
                    f"Asymmetric condor: "
                    f"put wing {buy_put.strike}/{sell_put.strike} "
                    f"(sell richness {sell_put.richness_score:+.2f}), "
                    f"call wing {sell_call.strike}/{buy_call.strike} "
                    f"(sell richness {sell_call.richness_score:+.2f})"
                ),
                risk_level='medium',
            )
            if combo and combo.net_premium > 0:
                candidates.append(combo)

        return candidates

    def _gen_custom_n_leg(self, scored: Dict[str, List[ScoredOption]],
                          current_price: float, regime: str) -> List[ComboCandidate]:
        """
        Greedy N-leg: pick top sell candidates + top buy candidates,
        build delta-neutral combos iteratively.
        """
        candidates = []
        all_options = scored.get('call', []) + scored.get('put', [])

        if len(all_options) < 4:
            return candidates

        # Top sell candidates (overpriced)
        sell_pool = sorted([o for o in all_options if o.richness_score > self.min_richness],
                           key=lambda x: x.richness_score, reverse=True)
        # Top buy candidates (underpriced)
        buy_pool = sorted([o for o in all_options if o.richness_score < -self.min_richness * 0.5],
                          key=lambda x: x.richness_score)

        if not sell_pool or not buy_pool:
            return candidates

        # Build combos greedily: start with top sell, add buys to neutralize delta
        for start_sell in sell_pool[:5]:
            legs_spec = [('sell', start_sell)]
            total_delta = -start_sell.greeks.delta  # sell = negate delta

            # Add buy legs to bring delta toward zero
            remaining_buys = list(buy_pool)
            for _ in range(self.max_legs - 1):
                if not remaining_buys:
                    break

                # Pick buy that best reduces |delta|
                best_buy = min(remaining_buys,
                               key=lambda x: abs(total_delta + x.greeks.delta))
                remaining_buys.remove(best_buy)

                legs_spec.append(('buy', best_buy))
                total_delta += best_buy.greeks.delta

                # Check if delta-neutral enough
                if abs(total_delta) < 0.15:
                    break

            if len(legs_spec) < 2:
                continue

            # Check IV edge
            sell_iv = np.mean([s.iv_residual for a, s, *_ in legs_spec if a == 'sell'])
            buy_iv = np.mean([s.iv_residual for a, s, *_ in legs_spec if a == 'buy'])
            iv_edge = sell_iv - buy_iv

            if iv_edge < self.min_iv_edge:
                continue

            combo = self._build_combo(
                combo_type=ComboType.CUSTOM_N_LEG,
                legs_spec=legs_spec,
                current_price=current_price,
                rationale=(
                    f"Custom {len(legs_spec)}-leg combo: "
                    f"IV edge {iv_edge:.4f}, "
                    f"net delta {total_delta:.3f}"
                ),
                risk_level='medium',
            )
            if combo:
                candidates.append(combo)

        return candidates

    # ------------------------------------------------------------------
    # Combo builder helper
    # ------------------------------------------------------------------

    def _build_combo(self, combo_type: ComboType,
                     legs_spec: List[tuple],
                     current_price: float,
                     rationale: str,
                     risk_level: str = 'medium') -> Optional[ComboCandidate]:
        """
        Build a ComboCandidate from leg specifications.

        legs_spec: List of (action, ScoredOption) or (action, ScoredOption, contracts)
        """
        legs = []
        total_delta = 0.0
        total_gamma = 0.0
        total_vega = 0.0
        total_theta = 0.0
        net_premium = 0.0
        iv_edge_sum = 0.0
        iv_edge_weight = 0.0

        for spec in legs_spec:
            action = spec[0]
            opt = spec[1]
            contracts = spec[2] if len(spec) > 2 else 1

            price = opt.mid
            if price <= 0:
                return None

            # Premium
            if action == 'buy':
                cost = price  # use ask for more conservative
                net_premium -= cost * contracts * 100
            else:
                cost = price  # use bid for more conservative
                net_premium += cost * contracts * 100

            sign = 1 if action == 'buy' else -1

            total_delta += sign * opt.greeks.delta * contracts
            total_gamma += sign * opt.greeks.gamma * contracts
            total_vega += sign * opt.greeks.vega * contracts
            total_theta += sign * opt.greeks.theta * contracts

            # IV edge tracking
            iv_edge_sum += (-sign) * opt.iv_residual * contracts  # sell overpriced = positive edge
            iv_edge_weight += contracts

            legs.append({
                'type': opt.option_type,
                'action': action,
                'strike': opt.strike,
                'expiry': opt.expiry,
                'contracts': contracts,
                'price': price,
                'implied_vol': opt.market_iv,
                'delta': round(sign * opt.greeks.delta, 4),
                'gamma': round(sign * opt.greeks.gamma, 6),
                'vega': round(sign * opt.greeks.vega, 4),
                'theta': round(sign * opt.greeks.theta, 4),
                'richness_score': round(opt.richness_score, 3),
                'iv_residual': round(opt.iv_residual, 4),
            })

        if not legs:
            return None

        iv_edge = iv_edge_sum / iv_edge_weight if iv_edge_weight > 0 else 0.0

        # Payoff evaluation
        payoff = self._evaluate_payoff(legs, current_price)
        max_loss = payoff['max_loss']
        max_gain = payoff['max_gain']
        risk_reward = max_gain / max_loss if max_loss > 0 else float('inf')

        # Primary expiry (first leg)
        primary_expiry = legs[0]['expiry']

        # Composite score
        score = self._composite_score(
            iv_edge=iv_edge,
            risk_reward=risk_reward,
            theta=total_theta,
            credit=net_premium,
            delta=total_delta,
            max_loss=max_loss,
            bid_ask_cost=sum(
                (spec[1].ask - spec[1].bid) * (spec[2] if len(spec) > 2 else 1)
                for spec in legs_spec
            ),
        )

        return ComboCandidate(
            combo_type=combo_type,
            legs=legs,
            net_premium=net_premium,
            iv_edge=iv_edge,
            total_delta=total_delta,
            total_gamma=total_gamma,
            total_vega=total_vega,
            total_theta=total_theta,
            max_loss=max_loss,
            max_gain=max_gain,
            risk_reward=risk_reward,
            score=score,
            rationale=rationale,
            expiry=primary_expiry,
            risk_level=risk_level,
        )

    def _evaluate_payoff(self, legs: List[Dict], current_price: float) -> Dict:
        """
        Evaluate payoff at expiry across a range of spot prices.
        For multi-expiry structures, evaluates at each expiry date.
        """
        # Group legs by expiry
        expiry_set = set(leg['expiry'] for leg in legs)
        is_multi_expiry = len(expiry_set) > 1

        if is_multi_expiry:
            return self._evaluate_multi_expiry_payoff(legs, current_price)

        # Single-expiry: standard intrinsic payoff
        net_premium = 0.0
        for leg in legs:
            contracts = leg.get('contracts', 1)
            if leg['action'] == 'buy':
                net_premium -= leg['price'] * contracts * 100
            else:
                net_premium += leg['price'] * contracts * 100

        strikes = sorted(set(leg['strike'] for leg in legs))
        if not strikes:
            return {'max_loss': abs(net_premium), 'max_gain': net_premium if net_premium > 0 else 0}

        test_points = np.linspace(strikes[0] * 0.5, strikes[-1] * 1.5, 200)
        test_points = np.concatenate([[0.0], test_points, strikes])
        test_points = np.unique(np.sort(test_points))

        payoffs = []
        for spot in test_points:
            pnl = net_premium
            for leg in legs:
                strike = leg['strike']
                opt_type = leg.get('type', 'call')
                contracts = leg.get('contracts', 1)

                if opt_type == 'call':
                    intrinsic = max(0.0, spot - strike)
                else:
                    intrinsic = max(0.0, strike - spot)

                mult = contracts * 100
                if leg['action'] == 'buy':
                    pnl += intrinsic * mult
                else:
                    pnl -= intrinsic * mult
            payoffs.append(pnl)

        payoffs = np.array(payoffs)
        max_gain = float(np.max(payoffs))
        max_loss = float(abs(np.min(payoffs)))

        return {'max_loss': max_loss, 'max_gain': max_gain}

    def _evaluate_multi_expiry_payoff(self, legs: List[Dict],
                                      current_price: float) -> Dict:
        """
        For structures spanning multiple expirations.
        Evaluates at each expiry: expired legs use intrinsic,
        remaining legs use BS model value.
        """
        # Parse expiry dates and sort
        expiry_dates = {}
        for leg in legs:
            exp_str = leg['expiry']
            try:
                exp_dt = pd.to_datetime(exp_str)
            except Exception:
                exp_dt = datetime.now()
            expiry_dates[exp_str] = exp_dt

        sorted_expiries = sorted(set(expiry_dates.values()))
        if not sorted_expiries:
            return {'max_loss': 0, 'max_gain': 0}

        strikes_all = [leg['strike'] for leg in legs]
        min_strike = min(strikes_all)
        max_strike = max(strikes_all)
        spot_range = np.linspace(min_strike * 0.5, max_strike * 1.5, 150)

        worst_loss = 0.0
        best_gain = 0.0

        # Net premium paid/received upfront
        net_premium = 0.0
        for leg in legs:
            contracts = leg.get('contracts', 1)
            if leg['action'] == 'buy':
                net_premium -= leg['price'] * contracts * 100
            else:
                net_premium += leg['price'] * contracts * 100

        # Evaluate at each expiry date
        for eval_dt in sorted_expiries:
            payoffs = np.full_like(spot_range, net_premium)

            for leg in legs:
                leg_dt = expiry_dates[leg['expiry']]
                strike = leg['strike']
                opt_type = leg.get('type', 'call')
                contracts = leg.get('contracts', 1)
                sign = 1 if leg['action'] == 'buy' else -1

                if leg_dt <= eval_dt:
                    # Expired: intrinsic value
                    for j, spot in enumerate(spot_range):
                        if opt_type == 'call':
                            intrinsic = max(0.0, spot - strike)
                        else:
                            intrinsic = max(0.0, strike - spot)
                        payoffs[j] += sign * intrinsic * contracts * 100
                else:
                    # Still alive: BS model value
                    remaining_days = (leg_dt - eval_dt).days
                    remaining_tte = max(remaining_days / 365.25, 0.001)
                    iv = leg.get('implied_vol', 0.3)

                    for j, spot in enumerate(spot_range):
                        bs_val = self.bs.price_option(
                            S=spot, K=strike, T=remaining_tte,
                            sigma=iv, option_type=opt_type
                        )
                        payoffs[j] += sign * bs_val * contracts * 100

            worst_loss = min(worst_loss, float(np.min(payoffs)))
            best_gain = max(best_gain, float(np.max(payoffs)))

        return {
            'max_loss': abs(worst_loss) if worst_loss < 0 else 0.0,
            'max_gain': best_gain if best_gain > 0 else 0.0,
        }

    # ------------------------------------------------------------------
    # Phase 3: Scoring and filtering
    # ------------------------------------------------------------------

    def _composite_score(self, iv_edge: float, risk_reward: float,
                         theta: float, credit: float, delta: float,
                         max_loss: float, bid_ask_cost: float) -> float:
        """Compute composite score for ranking candidates."""
        score = 0.0

        # IV edge (35% weight)
        score += 0.35 * _normalize(iv_edge, 0, 0.05)

        # Risk/reward (25% weight)
        score += 0.25 * _normalize(risk_reward, 1.0, 10.0)

        # Theta benefit (20% weight) — positive theta is desirable
        score += 0.20 * _normalize(theta, 0, 5.0)

        # Net credit (10% weight)
        score += 0.10 * _normalize(credit, 0, 500.0)

        # Delta neutrality bonus (10% weight)
        score += 0.10 * (1.0 - _normalize(abs(delta), 0, 1.0))

        # Penalties
        if max_loss == 0 or max_loss == float('inf'):
            score -= 0.15  # undefined risk
        score -= 0.10 * _normalize(bid_ask_cost, 0, 2.0)

        return max(score, 0.0)

    def optimize(self, candidates: List[ComboCandidate],
                 current_price: float) -> List[ComboCandidate]:
        """Rank and filter candidates by composite score and hard constraints."""
        filtered = []

        for c in candidates:
            # Hard filters
            if c.max_loss <= 0 and c.risk_level != 'high':
                continue  # undefined risk only for ratio spreads
            if c.max_loss > 0 and c.risk_reward < self.min_risk_reward:
                continue
            if abs(c.total_delta) > self.max_delta:
                continue
            if abs(c.total_vega) > self.max_vega:
                continue
            if c.iv_edge < self.min_iv_edge:
                continue
            # Must have at least 1 sell + 1 buy
            actions = set(leg['action'] for leg in c.legs)
            if 'buy' not in actions or 'sell' not in actions:
                continue

            filtered.append(c)

        # Sort by score descending
        filtered.sort(key=lambda x: x.score, reverse=True)

        # Deduplicate: remove candidates with very similar legs
        deduped = []
        seen_sigs = set()
        for c in filtered:
            sig = _combo_signature(c)
            if sig not in seen_sigs:
                seen_sigs.add(sig)
                deduped.append(c)

        return deduped[:10]  # top 10

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def find_combos(self, symbol: str, vol_surface: Dict,
                    skew_metrics: Dict,
                    regime_data: Optional[Dict] = None,
                    skew_context: Optional[Dict] = None) -> List[Dict]:
        """
        Top-level entry point. Returns opportunities in the same format
        as OpportunityScanner.find_opportunities().

        Steps:
        1. score_surface(vol_surface)
        2. generate_candidates(scored, current_price, regime)
        3. optimize(candidates, current_price)
        4. Convert top candidates to opportunity dicts
        """
        current_price = vol_surface.get('current_price', 0)
        if current_price <= 0:
            return []

        # Extract regime
        regime = 'unknown'
        if regime_data:
            label = regime_data.get('regime', regime_data.get('label', 'unknown'))
            if hasattr(label, 'value'):
                label = label.value
            regime = label

        # Phase 1: Score
        scored = self.score_surface(vol_surface)

        total_scored = len(scored.get('call', [])) + len(scored.get('put', []))
        if total_scored < 4:
            logger.info(f"{symbol}: Only {total_scored} scored options, skipping combo scan")
            return []

        logger.info(f"{symbol}: Scored {total_scored} options for combo analysis")

        # Phase 2: Generate
        candidates = self.generate_candidates(scored, current_price, regime)

        if not candidates:
            return []

        logger.info(f"{symbol}: Generated {len(candidates)} combo candidates")

        # Phase 3: Optimize
        best = self.optimize(candidates, current_price)

        if not best:
            return []

        logger.info(f"{symbol}: {len(best)} combos passed filters")

        # Convert to opportunity dict format
        opportunities = []
        for combo in best:
            opp = {
                'type': 'combo_trade',
                'subtype': combo.combo_type.value,
                'symbol': symbol,
                'expiry': combo.expiry,
                'confidence': min(combo.score, 1.0),
                'direction': 'multi_leg',
                'entry_signal': 'iv_surface_mispricing',
                'regime_suitable': True,
                'iv_edge': round(combo.iv_edge, 4),
                'scored_legs': combo.legs,
                'strikes': {f'leg_{i}': leg['strike'] for i, leg in enumerate(combo.legs)},
                'prices': {f'leg_{i}': leg['price'] for i, leg in enumerate(combo.legs)},
                'expected_pnl': round(combo.net_premium, 2),
                'max_loss': round(combo.max_loss, 2),
                'max_gain': round(combo.max_gain, 2),
                'risk_reward': round(combo.risk_reward, 2),
                'risk_level': combo.risk_level,
                'holding_period': '7-45 DTE',
                'rationale': combo.rationale,
                'metrics': {
                    'total_delta': round(combo.total_delta, 4),
                    'total_gamma': round(combo.total_gamma, 6),
                    'total_vega':  round(combo.total_vega,  4),
                    'total_theta': round(combo.total_theta, 4),
                },
            }
            opportunities.append(opp)

        return opportunities


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _group_by_expiry(options: List[ScoredOption]) -> Dict[str, List[ScoredOption]]:
    """Group scored options by expiry date."""
    groups = {}
    for opt in options:
        groups.setdefault(opt.expiry, []).append(opt)
    return groups


def _normalize(value: float, lo: float, hi: float) -> float:
    """Normalize value to [0, 1] range."""
    if hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (value - lo) / (hi - lo)))


def _combo_signature(combo: ComboCandidate) -> str:
    """Create a hashable signature for deduplication."""
    parts = []
    for leg in sorted(combo.legs, key=lambda x: (x['expiry'], x['strike'])):
        parts.append(f"{leg['action']}{leg['type']}{leg['strike']}{leg['expiry']}{leg.get('contracts', 1)}")
    return '|'.join(parts)
