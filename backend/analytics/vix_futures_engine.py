"""
VIX Futures Engine
Computes VIX term structure metrics, carry quality, roll costs, and
synthesises a plain-English environment description.

Data sources (all free):
  - yfinance: ^VIX, ^VIX3M, ^VVIX, SPY, UVXY, SVXY, VXX
  - CBOE delayed JSON endpoint for the live VIX futures strip
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    import yfinance as yf
except ImportError:
    yf = None

try:
    import requests
except ImportError:
    requests = None

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CBOE_FUTURES_URL = "https://cdn.cboe.com/api/global/delayed_quotes/futures/_VX.json"  # blocked
VIXCENTRAL_URL   = "http://vixcentral.com"

# Strategy thresholds (from newVIXfut_balanced.py / VIXSeppBalancedV2)
CARRY_ON_RATIO   = 92.0   # smoothed VIX/VIX3M below this → carry available
CARRY_OFF_RATIO  = 98.0   # smoothed VIX/VIX3M above this → exit
CARRY_MIN_RATIO  = 0.40   # minimum carry ratio for entry
VIX_CIRCUIT_BRK  = 40.0   # hard stop regardless of ratio
TARGET_VOL       = 0.25   # annualised vol target for position sizing
MIN_POSITION     = 0.20   # floor on position size
MAX_POSITION     = 0.90   # ceiling on position size
LOW_VOL_REGIME_ENTER = 12.0  # VIX below this → reduce size
LOW_VOL_REDUCTION    = 0.75  # size multiplier in low-vol regime
VOL_LOOKBACK     = 20     # days for realized-vol calculation

# Carry ratio buckets for outcome distribution labelling
CARRY_BUCKETS = [
    (None,  0.40, "< 0.40  (off)"),
    (0.40,  0.60, "0.40–0.60"),
    (0.60,  0.85, "0.60–0.85"),
    (0.85,  1.10, "0.85–1.10"),
    (1.10,  None, "> 1.10  (rich)"),
]

LOOKBACK_DAYS = 4500  # ~18 years — VIX3M available from 2007; use full history for percentiles


# ---------------------------------------------------------------------------
# Main engine class
# ---------------------------------------------------------------------------

class VIXFuturesEngine:
    """Fetch and compute all VIX-futures metrics needed by the dashboard tab."""

    def __init__(self):
        self._cache: Optional[dict] = None
        self._cache_ts: Optional[datetime] = None
        self._cache_ttl_minutes = 15
        self._ibkr_fetcher = None   # lazy-init on first strip fetch

        # PCA — lazy import so app starts even if sklearn is missing
        try:
            from vix_pca import VIXTermStructurePCA
            self._pca = VIXTermStructurePCA()
        except Exception:
            self._pca = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_all(self, regime_result: Optional[dict] = None) -> dict:
        """
        Main entry point. Returns a dict with:
          metrics       — scalar KPIs
          history       — pd.DataFrame (date-indexed) with all time series
          percentiles   — percentile positions for key metrics
          outcomes      — forward-return distribution by carry bucket
          futures_strip — list of {label, level, dte} or []
          synthesis     — plain-English paragraph (str)
          error         — None or error message string
        """
        now = datetime.now()
        if (self._cache and self._cache_ts and
                (now - self._cache_ts).total_seconds() < self._cache_ttl_minutes * 60):
            result = dict(self._cache)
            # Always regenerate synthesis in case regime_result changed
            if regime_result is not None:
                result['synthesis'] = self._build_synthesis(
                    result['metrics'], result['percentiles'],
                    result['outcomes'], regime_result,
                    result.get('pca', {}),
                )
            return result

        try:
            df = self._fetch_history(LOOKBACK_DAYS)
            if df is None or df.empty:
                return self._error_result("Failed to fetch VIX history from yfinance")

            df = self._compute_derived(df)
            metrics = self._scalar_metrics(df)
            percentiles = self._compute_percentiles(df)
            outcomes = self._compute_outcomes(df)
            futures_strip = self._fetch_futures_strip()

            # Store today's strip and run PCA (no-op if sklearn missing or strip empty)
            pca_signal = {}
            if self._pca is not None and futures_strip:
                self._pca.store_strip(futures_strip)
                pca_signal = self._pca.get_signal(futures_strip)

            synthesis = self._build_synthesis(
                metrics, percentiles, outcomes, regime_result, pca_signal)

            result = {
                'metrics': metrics,
                'history': df,
                'percentiles': percentiles,
                'outcomes': outcomes,
                'futures_strip': futures_strip,
                'pca': pca_signal,
                'synthesis': synthesis,
                'error': None,
            }
            self._cache = result
            self._cache_ts = now
            return result

        except Exception as e:
            logger.exception("VIXFuturesEngine.get_all failed")
            return self._error_result(str(e))

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    def _fetch_history(self, lookback_days: int) -> Optional[pd.DataFrame]:
        if yf is None:
            return None
        end = datetime.today()
        start = end - timedelta(days=lookback_days + 30)

        tickers = ['^VIX', '^VIX3M', '^VVIX', 'SPY', 'UVXY', 'SVXY']
        try:
            raw = yf.download(tickers, start=start, end=end,
                              auto_adjust=True, progress=False)
            closes = raw['Close'] if isinstance(raw.columns, pd.MultiIndex) else raw
            closes.columns = [c.replace('^', '') for c in closes.columns]
            # Keep full VIX/VIX3M history for percentile calibration.
            # VIX3M available from ~2007; SVXY only from 2011 (pre-2018 data is
            # post-split adjusted and unreliable for return calcs — NaN is fine here).
            closes = closes.dropna(subset=['VIX', 'VIX3M'])
            closes = closes.tail(lookback_days)
            return closes
        except Exception as e:
            logger.warning(f"VIX history fetch failed: {e}")
            return None

    def _fetch_futures_strip(self) -> list:
        """Fetch VIX futures strip. Priority: IBKR (live) → vixcentral (delayed) → [].

        IBKR is tried first when TWS/Gateway is running locally.
        Falls back to vixcentral scrape (~15-min delayed, same source as vixcentral.com).
        """
        # 1. Try IBKR live feed
        strip = self._fetch_strip_ibkr()
        if strip:
            logger.info("VX strip source: IBKR (live)")
            return strip

        # 2. Fall back to vixcentral scrape
        logger.info("VX strip source: vixcentral (delayed)")
        return self._fetch_strip_vixcentral()

    def _fetch_strip_ibkr(self) -> list:
        """Attempt to get VX strip from IBKR. Returns [] silently if unavailable."""
        try:
            from ibkr_fetcher import IBKRFetcher
            if self._ibkr_fetcher is None:
                self._ibkr_fetcher = IBKRFetcher()
            return self._ibkr_fetcher.get_vx_strip()
        except Exception:
            return []

    def _fetch_strip_vixcentral(self) -> list:
        """Fetch VIX futures strip from vixcentral.com (same source as the site).

        vixcentral embeds settlement data as JS variables in the page HTML:
          last_data_var      — live intraday prices (empty outside market hours)
          previous_close_var — prior settlement prices (always populated)
        We prefer live data when available, fall back to previous close.
        """
        import re
        if requests is None:
            return []
        try:
            headers = {
                'User-Agent': (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
                ),
                'Accept': 'text/html',
            }
            resp = requests.get(VIXCENTRAL_URL, headers=headers, timeout=10)
            resp.raise_for_status()
            html = resp.text

            # Extract live intraday prices
            live_match = re.search(r'last_data_var=clean_array\(\[([^\]]*)\]\)', html)
            live_vals = []
            if live_match:
                live_vals = [
                    float(v.strip()) for v in live_match.group(1).split(',')
                    if v.strip() and v.strip() not in ('', 'null')
                ]

            # Extract previous settlement prices (always present)
            prev_match = re.search(r'previous_close_var=\[([^\]]+)\]', html)
            prev_vals = []
            if prev_match:
                prev_vals = [
                    float(v.strip()) for v in prev_match.group(1).split(',')
                    if v.strip()
                ]

            # Prefer live; fall back to previous close per contract
            n = max(len(live_vals), len(prev_vals))
            if n == 0:
                logger.warning("vixcentral: no futures data found in page")
                return []

            levels = []
            for i in range(n):
                live = live_vals[i] if i < len(live_vals) and live_vals[i] > 0 else None
                prev = prev_vals[i] if i < len(prev_vals) else None
                levels.append(live if live else prev)

            # Generate month labels starting from next upcoming VX expiry
            today = datetime.today().date()
            strip = []
            month = today.month
            year  = today.year
            # Advance to next month if past mid-month (VX expires ~3rd Wednesday)
            if today.day > 18:
                month += 1
                if month > 12:
                    month = 1
                    year += 1

            for i, level in enumerate(levels[:8]):
                if level is None or level <= 0:
                    continue
                label = datetime(year, month, 1).strftime('%b %Y')
                dte   = (datetime(year, month, 15).date() - today).days
                strip.append({'label': label, 'level': round(level, 3), 'dte': dte})
                month += 1
                if month > 12:
                    month = 1
                    year += 1

            logger.info(f"vixcentral: fetched {len(strip)} VX contracts")
            return strip

        except Exception as e:
            logger.warning(f"vixcentral futures strip fetch failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Derived computations
    # ------------------------------------------------------------------

    def _compute_derived(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Core ratio
        df['vix_ratio'] = df['VIX'] / df['VIX3M'] * 100
        df['vix_ratio_10d'] = df['vix_ratio'].rolling(10).mean()

        # Carry ratio — realized vol from actual SVXY returns (mirrors QC strategy)
        df['daily_roll'] = (df['VIX3M'] - df['VIX']) / df['VIX3M']
        df['annualised_roll'] = df['daily_roll'] * 5.8
        if 'SVXY' in df.columns:
            svxy_ret = df['SVXY'].pct_change().clip(-0.5, 0.5)
            df['realized_vol'] = svxy_ret.rolling(VOL_LOOKBACK).std() * np.sqrt(252)
            df['realized_vol'] = df['realized_vol'].clip(0.15, 1.0).fillna(0.40)
        else:
            df['realized_vol'] = 0.40
        df['carry_ratio'] = df['annualised_roll'] / df['realized_vol'].replace(0, np.nan)

        # Volatility Risk Premium: implied − realized (vol-selling edge signal)
        # VRP > 0 → implied > realized → vol-selling has positive expected value
        # VRP < 0 → unusual; typically post-spike when realized > implied
        df['vrp'] = df['VIX'] - df['realized_vol'] * 100

        # VVIX/VIX ratio: vol-of-vol relative to vol level
        # Elevated ratio (>5) signals tail-risk premium, reduces carry quality
        if 'VVIX' in df.columns:
            df['vvix_vix_ratio'] = df['VVIX'] / df['VIX']
        else:
            df['vvix_vix_ratio'] = np.nan

        # Monthly roll cost / yield estimates
        # annualised_roll (Sepp 5.8 factor) is the correctly scaled annual carry.
        # ÷12 converts to monthly; ×100 to percent.  Do NOT multiply daily_roll by
        # a day-count — daily_roll is the total basis ratio, not a per-day rate.
        df['monthly_roll_pct'] = df['annualised_roll'] / 12 * 100
        df['uvxy_monthly_cost'] = df['monthly_roll_pct'] * 1.5   # 1.5× leverage
        df['svxy_monthly_yield'] = df['monthly_roll_pct'] * 0.5  # 0.5× inverse

        # SPY signals
        if 'SPY' in df.columns:
            df['spy_roc_60d'] = df['SPY'].pct_change(60) * 100
            df['spy_ma200'] = df['SPY'].rolling(200).mean()
            df['spy_above_ma200'] = df['SPY'] > df['spy_ma200']

        # Position sizing: TARGET_VOL / realized_vol, clamped, with low-vol regime reduction
        raw_size = (TARGET_VOL / df['realized_vol']).clip(MIN_POSITION, MAX_POSITION)
        low_vol_regime = df['VIX'] < LOW_VOL_REGIME_ENTER
        df['target_size'] = np.where(low_vol_regime, raw_size * LOW_VOL_REDUCTION, raw_size)

        # Strategy carry-on state
        df['carry_on'] = (
            (df['vix_ratio_10d'] < CARRY_ON_RATIO) &
            (df['carry_ratio'] >= CARRY_MIN_RATIO) &
            (df['VIX'] < VIX_CIRCUIT_BRK)
        )

        return df

    def _scalar_metrics(self, df: pd.DataFrame) -> dict:
        last = df.iloc[-1]
        spy_roc = float(last.get('spy_roc_60d', 0) or 0)
        spy_above = bool(last.get('spy_above_ma200', False))
        carry_on = bool(last.get('carry_on', False))
        realized_vol = float(last.get('realized_vol', 0.40) or 0.40)
        low_vol_regime = bool(last['VIX'] < LOW_VOL_REGIME_ENTER)

        # Position size from vol-targeting formula (mirrors QC strategy exactly)
        if carry_on:
            raw_size = float(np.clip(TARGET_VOL / realized_vol, MIN_POSITION, MAX_POSITION))
            target_size = raw_size * LOW_VOL_REDUCTION if low_vol_regime else raw_size
            allocation = round(target_size * 100, 1)
            regime_tag = ' [low-vol regime]' if low_vol_regime else ''
            allocation_label = f'{allocation:.0f}%{regime_tag}'
        else:
            target_size = 0.0
            allocation = 0
            allocation_label = 'OFF'

        vrp = float(last.get('vrp', 0) or 0)
        vvix_vix = float(last.get('vvix_vix_ratio', np.nan) or np.nan)

        return {
            'vix':                   round(float(last['VIX']), 2),
            'vix3m':                 round(float(last['VIX3M']), 2),
            'vvix':                  round(float(last.get('VVIX', 0) or 0), 1),
            'vix_ratio':             round(float(last['vix_ratio']), 2),
            'vix_ratio_10d':         round(float(last['vix_ratio_10d']), 2),
            'carry_ratio':           round(float(last['carry_ratio']), 3),
            'realized_vol':          round(realized_vol, 4),
            'vrp':                   round(vrp, 2),
            'vvix_vix_ratio':        round(vvix_vix, 2) if not np.isnan(vvix_vix) else None,
            'monthly_roll_pct':      round(float(last['monthly_roll_pct']), 2),
            'uvxy_monthly_cost':     round(float(last['uvxy_monthly_cost']), 2),
            'svxy_monthly_yield':    round(float(last['svxy_monthly_yield']), 2),
            'spy_roc_60d':           round(spy_roc, 2),
            'spy_above_ma200':       spy_above,
            'carry_on':              carry_on,
            'low_vol_regime':        low_vol_regime,
            'target_size':           round(target_size, 4),
            'allocation':            allocation,
            'allocation_label':      allocation_label,
            'as_of':                 df.index[-1].strftime('%Y-%m-%d'),
        }

    def _compute_percentiles(self, df: pd.DataFrame) -> dict:
        def pct_rank(series: pd.Series) -> float:
            s = series.dropna()
            if len(s) < 2:
                return 50.0
            current = s.iloc[-1]
            return float(round((s < current).mean() * 100, 1))

        return {
            'vix_ratio_pct':    pct_rank(df['vix_ratio']),
            'carry_ratio_pct':  pct_rank(df['carry_ratio']),
            'vix_pct':          pct_rank(df['VIX']),
            'vvix_pct':         pct_rank(df.get('VVIX', pd.Series(dtype=float))),
            'vrp_pct':          pct_rank(df['vrp']) if 'vrp' in df.columns else 50.0,
            'vvix_vix_pct':     pct_rank(df['vvix_vix_ratio'].dropna()) if 'vvix_vix_ratio' in df.columns else 50.0,
            'n_obs':            int(df['vix_ratio'].dropna().count()),
        }

    def _compute_outcomes(self, df: pd.DataFrame) -> dict:
        """
        For each carry_ratio bucket, compute the distribution of
        forward 21-day returns for SVXY and the VIX spike frequency.
        """
        outcomes = {}
        if 'SVXY' not in df.columns:
            return outcomes

        df = df.copy()
        df['svxy_fwd_21d'] = df['SVXY'].pct_change(21).shift(-21) * 100
        df['vix_spike_30d'] = (
            df['VIX'].rolling(30).max().shift(-30) / df['VIX'] - 1
        ) > 0.30

        for lo, hi, label in CARRY_BUCKETS:
            mask = pd.Series([True] * len(df), index=df.index)
            if lo is not None:
                mask &= df['carry_ratio'] >= lo
            if hi is not None:
                mask &= df['carry_ratio'] < hi

            subset = df.loc[mask, ['svxy_fwd_21d', 'vix_spike_30d']].dropna()
            if len(subset) < 5:
                outcomes[label] = {'n': 0, 'median': None, 'p25': None, 'p75': None,
                                   'spike_pct': None}
                continue

            fwd = subset['svxy_fwd_21d']
            outcomes[label] = {
                'n':         len(subset),
                'median':    round(float(fwd.median()), 2),
                'p25':       round(float(fwd.quantile(0.25)), 2),
                'p75':       round(float(fwd.quantile(0.75)), 2),
                'spike_pct': round(float(subset['vix_spike_30d'].mean() * 100), 1),
            }
        return outcomes

    # ------------------------------------------------------------------
    # Synthesis text
    # ------------------------------------------------------------------

    def _build_synthesis(self, metrics: dict, percentiles: dict,
                         outcomes: dict, regime_result: Optional[dict],
                         pca_signal: dict = None) -> str:
        if not metrics:
            return "Insufficient data to generate synthesis."

        vix         = metrics['vix']
        vix3m       = metrics['vix3m']
        ratio       = metrics['vix_ratio']
        ratio_p     = percentiles.get('vix_ratio_pct', 50)
        carry       = metrics['carry_ratio']
        carry_p     = percentiles.get('carry_ratio_pct', 50)
        cost        = abs(metrics['uvxy_monthly_cost'])
        yield_      = metrics['svxy_monthly_yield']
        n_obs       = percentiles.get('n_obs', 0)
        as_of       = metrics['as_of']
        carry_on    = metrics['carry_on']
        alloc       = metrics['allocation_label']
        real_vol    = metrics.get('realized_vol', 0.40)
        low_vol_reg = metrics.get('low_vol_regime', False)

        # Contango/backwardation description
        if ratio < 85:
            basis_desc = "deep contango"
        elif ratio < 92:
            basis_desc = "moderate contango"
        elif ratio < 98:
            basis_desc = "shallow contango (caution zone)"
        elif ratio < 100:
            basis_desc = "near-flat term structure"
        else:
            basis_desc = "backwardation"

        # Carry quality description
        if carry >= 1.0:
            carry_desc = "exceptionally well-compensated"
        elif carry >= 0.75:
            carry_desc = "well-compensated"
        elif carry >= 0.55:
            carry_desc = "adequately compensated"
        elif carry >= 0.40:
            carry_desc = "marginally compensated (near threshold)"
        else:
            carry_desc = "insufficient — carry filter off"

        # Outcome context for current carry bucket
        outcome_text = ""
        current_bucket_label = None
        for lo, hi, label in CARRY_BUCKETS:
            in_bucket = (lo is None or carry >= lo) and (hi is None or carry < hi)
            if in_bucket:
                current_bucket_label = label
                break
        if current_bucket_label and current_bucket_label in outcomes:
            o = outcomes[current_bucket_label]
            if o['n'] >= 5:
                outcome_text = (
                    f"\n\nHistorical context: At carry ratios in the {current_bucket_label} "
                    f"range, SVXY produced a median {o['median']:+.1f}% over the following "
                    f"21 days (25th pct: {o['p25']:+.1f}%, 75th pct: {o['p75']:+.1f}%, "
                    f"N={o['n']} periods). VIX spiked >30% in the following 30 days "
                    f"{o['spike_pct']:.0f}% of the time."
                )

        # Regime linkage
        regime_text = ""
        if regime_result:
            regime_name = regime_result.get('regime', '')
            regime_contango = {
                'Sticky Delta':     74,
                'Sticky Strike':    68,
                'Sticky Local Vol': 55,
                'Jumpy Volatility': 28,
            }.get(regime_name)
            if regime_name and regime_contango:
                regime_text = (
                    f"\n\nRegime linkage: Current Bennett regime is {regime_name}. "
                    f"Historically, contango persists {regime_contango}% of the time "
                    f"in {regime_name} periods."
                )

        vrp         = metrics.get('vrp', 0) or 0
        vrp_p       = percentiles.get('vrp_pct', 50)
        vvix_ratio  = metrics.get('vvix_vix_ratio')
        vvix_p      = percentiles.get('vvix_vix_pct', 50)

        # VRP description
        if vrp >= 6:
            vrp_desc = "elevated — unusually rich vol-selling environment"
        elif vrp >= 3:
            vrp_desc = "healthy — structural vol-selling edge present"
        elif vrp >= 0:
            vrp_desc = "thin — limited vol premium; carry quality marginal"
        else:
            vrp_desc = "negative — realized vol exceeds implied (post-spike regime)"

        # VVIX/VIX tail-risk note
        vvix_note = ""
        if vvix_ratio is not None:
            if vvix_ratio > 6:
                vvix_note = (f" VVIX/VIX at {vvix_ratio:.2f} (P{vvix_p:.0f}) — "
                             f"elevated tail-risk premium; vol-of-vol pricing is demanding.")
            elif vvix_ratio > 4.5:
                vvix_note = (f" VVIX/VIX at {vvix_ratio:.2f} (P{vvix_p:.0f}) — "
                             f"moderate vol-of-vol pressure; monitor for regime shift.")

        carry_state = "ON" if carry_on else "OFF"
        regime_note = " (low-vol regime active — size reduced 25%)" if low_vol_reg else ""
        lines = [
            f"VIX FUTURES ENVIRONMENT SYNTHESIS",
            f"As of {as_of}",
            "",
            f"Basis: VIX spot at {vix} vs VIX3M at {vix3m} — ratio {ratio:.1f}, "
            f"in the {ratio_p:.0f}th percentile of the past {n_obs}-observation window "
            f"({basis_desc}).",
            "",
            f"Roll dynamics: At current basis, UVXY holders are paying an estimated "
            f"{cost:.1f}%/month in roll cost. SVXY is accruing approximately "
            f"{yield_:.1f}%/month in roll yield.",
            "",
            f"Carry quality: Carry ratio at {carry:.2f} — {carry_desc}. "
            f"Carry ratio is at the {carry_p:.0f}th percentile of its historical range. "
            f"Realized vol (SVXY 20d): {real_vol*100:.0f}%.",
            "",
            f"Vol Risk Premium: VRP at {vrp:+.1f} vol pts (P{vrp_p:.0f}) — {vrp_desc}.{vvix_note}",
            "",
            f"Strategy state: Carry filter is {carry_state}. "
            f"Vol-targeted allocation: {alloc}{regime_note}.",
        ]

        # Johnson (2017) SLOPE factor commentary
        pca_text = ""
        if pca_signal and pca_signal.get('is_ready'):
            slope     = pca_signal['slope']
            slope_pct = pca_signal['slope_pct']
            n_obs     = pca_signal['n_obs']
            ev        = pca_signal.get('explained_variance', [])
            ev_str    = (f"PC1 {ev[0]*100:.0f}%, PC2 {ev[1]*100:.0f}%"
                         if len(ev) >= 2 else "")
            if slope_pct >= 75:
                slope_desc = "elevated — term structure pricing high vol risk premium"
            elif slope_pct >= 50:
                slope_desc = "above median — carry environment is supportive"
            elif slope_pct >= 25:
                slope_desc = "below median — carry quality is degrading"
            else:
                slope_desc = "compressed — term structure flattening; reduce exposure"
            pca_text = (
                f"\n\nTerm structure PCA (Johnson 2017): SLOPE score {slope:+.2f} "
                f"(P{slope_pct:.0f} of {n_obs}-day window). {slope_desc}. "
                f"{ev_str}."
            )
        elif pca_signal and not pca_signal.get('is_ready'):
            min_obs   = pca_signal.get('min_obs', 60)
            n_obs_pca = pca_signal.get('n_obs', 0)
            remaining = max(0, min_obs - n_obs_pca)
            if remaining > 0:
                pca_text = (
                    f"\n\nTerm structure PCA: accumulating strip history "
                    f"({n_obs_pca}/{min_obs} days — {remaining} more needed). "
                    f"Backfill with Polygon flat files to enable immediately."
                )

        return "\n".join(lines) + outcome_text + regime_text + pca_text

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _error_result(msg: str) -> dict:
        return {
            'metrics': {},
            'history': pd.DataFrame(),
            'percentiles': {},
            'outcomes': {},
            'futures_strip': [],
            'synthesis': f"Data unavailable: {msg}",
            'error': msg,
        }
