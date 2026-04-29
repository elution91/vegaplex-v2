"""
Regime Classifier Module
Classifies volatility surface dynamics into Bennett's four regimes:
  1. Sticky Delta  - calm/trending, long-term, positive spot-vol correlation
  2. Sticky Strike  - normal, medium-term, zero spot-vol correlation
  3. Sticky Local Vol - normal, medium-term, negative spot-vol correlation (fairly prices skew)
  4. Jumpy Volatility - panicked, short-term, very negative spot-vol correlation

Reference: Bennett, "Trading Volatility" - Skew and Term Structure Trading, pp.154-169
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any
from enum import Enum
import logging

logger = logging.getLogger(__name__)

try:
    import yfinance as yf
except ImportError:
    yf = None


class VolRegime(str, Enum):
    STICKY_DELTA = "sticky_delta"
    STICKY_STRIKE = "sticky_strike"
    STICKY_LOCAL_VOL = "sticky_local_vol"
    JUMPY_VOL = "jumpy_vol"
    UNKNOWN = "unknown"


# Regime metadata following Bennett's framework (p.156 Figure 99)
REGIME_INFO = {
    VolRegime.STICKY_DELTA: {
        'label': 'Sticky Delta',
        'sentiment': 'Calm / Trending',
        'time_horizon': 'Long-term',
        'spot_vol_corr': 'Positive',
        'skew_trade_pnl': 'Very Negative',
        'description': (
            'ATM vol stays constant as spot moves. Fixed-strike implieds move '
            'to keep moneyness-based vol unchanged. Long skew very unprofitable.'
        ),
        'color': '#2ecc71',  # green - calm
        'recommendation': 'SELL skew (risk reversals). Short vol strategies favoured.',
    },
    VolRegime.STICKY_STRIKE: {
        'label': 'Sticky Strike',
        'sentiment': 'Normal',
        'time_horizon': 'Medium-term',
        'spot_vol_corr': 'Zero',
        'skew_trade_pnl': 'Negative (skew theta)',
        'description': (
            'Fixed-strike implied volatilities remain constant. '
            'No surface re-mark profit, but skew theta still costs. '
            'Long skew mildly unprofitable.'
        ),
        'color': '#3498db',  # blue - neutral
        'recommendation': 'Mild short skew bias. Calendar spreads attractive.',
    },
    VolRegime.STICKY_LOCAL_VOL: {
        'label': 'Sticky Local Vol',
        'sentiment': 'Normal',
        'time_horizon': 'Medium-term',
        'spot_vol_corr': 'Negative',
        'skew_trade_pnl': 'Break-even',
        'description': (
            'Local volatility surface stays constant. ATM implied moves by 2x the skew '
            'for each spot move. Re-mark profit exactly offsets skew theta. '
            'Skew is fairly priced.'
        ),
        'color': '#f39c12',  # orange
        'recommendation': 'Skew fairly priced. Focus on term structure trades and vol level.',
    },
    VolRegime.JUMPY_VOL: {
        'label': 'Jumpy Volatility',
        'sentiment': 'Panicked',
        'time_horizon': 'Short-term',
        'spot_vol_corr': 'Very Negative',
        'skew_trade_pnl': 'Profitable',
        'description': (
            'Excessive implied vol jumps for a given spot move. '
            'Re-mark profit exceeds skew theta cost. '
            'This is the ONLY regime where long skew is profitable.'
        ),
        'color': '#e74c3c',  # red - panicked
        'recommendation': 'BUY skew (long puts, short calls). Risk reversals profitable.',
    },
    VolRegime.UNKNOWN: {
        'label': 'Unknown',
        'sentiment': 'Insufficient data',
        'time_horizon': '-',
        'spot_vol_corr': '-',
        'skew_trade_pnl': '-',
        'description': 'Not enough data to classify the regime.',
        'color': '#95a5a6',  # grey
        'recommendation': 'Gather more data before trading.',
    },
}

# Confidence multipliers for opportunity scoring by regime
REGIME_CONFIDENCE_MULTIPLIER = {
    VolRegime.STICKY_DELTA: 0.3,
    VolRegime.STICKY_STRIKE: 0.6,
    VolRegime.STICKY_LOCAL_VOL: 1.0,
    VolRegime.JUMPY_VOL: 1.4,
    VolRegime.UNKNOWN: 0.5,
}


class RegimeClassifier:
    """Classifies the current volatility regime using Bennett's framework.

    The key metric is the spot-vol correlation (and its magnitude), which maps
    directly to the four regimes.  We also compute "realised skew"
    (Bennett p.164) as a secondary signal.
    """

    def __init__(self, lookback_days: int = 60, iv_store=None):
        self.lookback_days = lookback_days
        self._cache: Dict[str, Dict] = {}  # symbol -> last result
        self._iv_store = iv_store           # optional SkewHistory instance

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def classify(self, symbol: str, hist_data: Optional[pd.DataFrame] = None) -> Dict:
        """Classify the current vol regime for *symbol*.

        Parameters
        ----------
        symbol : str
            Ticker symbol.
        hist_data : pd.DataFrame, optional
            Historical OHLCV data (must include 'Close' column).
            If None, we fetch it via yfinance.

        Returns
        -------
        dict with keys:
            regime, regime_info, spot_vol_corr, realised_vol,
            vol_of_vol, spot_returns, vol_series, details
        """
        if hist_data is None:
            hist_data = self._fetch_history(symbol)

        if hist_data is None or len(hist_data) < 20:
            return self._empty_result(symbol)

        # 1. Compute daily log returns
        close = hist_data['Close'].dropna()
        log_returns = np.log(close / close.shift(1)).dropna()

        # 2. Compute rolling realised vol (20-day window, annualised)
        window = min(20, len(log_returns) - 1)
        if window < 5:
            return self._empty_result(symbol)

        rolling_vol = log_returns.rolling(window).std() * np.sqrt(252)
        rolling_vol = rolling_vol.dropna()

        if len(rolling_vol) < 10:
            return self._empty_result(symbol)

        aligned_returns = log_returns.loc[rolling_vol.index]

        # 3. Regime classification — three-tier fallback:
        #
        #   Tier 1 (best): stored IBKR ATM IV history → proper Bennett β = dIV/dSpot
        #   Tier 2       : VIX year percentile + trend (free, good for index ETFs)
        #   Tier 3       : realized vol percentile (always available, weakest signal)
        #
        anchor_date = hist_data.index[-1]

        as_of = anchor_date.strftime('%Y-%m-%d') if hasattr(anchor_date, 'strftime') else str(anchor_date)[:10]
        stored_iv = self._iv_store.get_iv_series(symbol, as_of_date=as_of) if self._iv_store else None

        # --- Tier 1: Ticker-level spot-vol correlation (Bennett proper) ---
        # Requires 30+ daily ATM IV observations in iv_history.
        # Aligns ticker price returns with daily ATM IV changes and computes
        # the true spot-vol β → maps directly to Bennett's four regimes.
        stored_iv_raw = stored_iv  # keep reference for fall-through check
        if stored_iv is not None and len(stored_iv) >= 30:
            try:
                # Align price history with IV history
                iv_chg = stored_iv.diff().dropna()
                price_ret = np.log(close / close.shift(1)).dropna()

                # Inner join on dates
                common_idx = iv_chg.index.intersection(price_ret.index)
                if len(common_idx) >= 20:
                    iv_aligned  = iv_chg.loc[common_idx]
                    ret_aligned = price_ret.loc[common_idx]

                    # Spot-vol correlation: negative = IV rises when spot falls
                    corr_val = float(ret_aligned.corr(iv_aligned))
                    if np.isnan(corr_val):
                        raise ValueError("NaN correlation")

                    # Map correlation to regime (Bennett p.154-169)
                    # corr > 0.1    → Sticky Delta  (IV moves WITH spot, calm trending)
                    # -0.15 to 0.1  → Sticky Strike (near zero, neutral)
                    # -0.40 to -0.15→ Sticky Local Vol (moderate negative)
                    # < -0.40       → Jumpy Vol (very negative, panicked)
                    if corr_val > 0.1:
                        regime = VolRegime.STICKY_DELTA
                    elif corr_val > -0.15:
                        regime = VolRegime.STICKY_STRIKE
                    elif corr_val > -0.40:
                        regime = VolRegime.STICKY_LOCAL_VOL
                    else:
                        regime = VolRegime.JUMPY_VOL

                    # IV percentile vs own history — used for display and trend
                    vix_pct     = float(pd.Series(stored_iv.values).rank(pct=True).iloc[-1] * 100)
                    vix_trend   = float(stored_iv.iloc[-5:].mean() - stored_iv.iloc[-20:].mean()) * 100
                    vix_current = float(stored_iv.iloc[-1]) * 100
                    primary_corr = corr_val
                    iv_source   = 'ticker_spot_vol_corr'
                else:
                    stored_iv_raw = None   # not enough overlap — fall through
            except Exception as e:
                logger.debug(f"{symbol} Tier 1 corr failed: {e}")
                stored_iv_raw = None

        if stored_iv_raw is None or len(stored_iv_raw) < 30:
            # --- Tier 2: per-ticker realized spot-vol correlation ---
            # Correlate daily price returns with changes in 20-day realized vol.
            # This is the same logic as Tier 1 but using realized vol as the IV proxy.
            # Ticker-specific: NVDA, BOIL, UVXY all get different readings.
            rv_diff  = rolling_vol.diff().dropna()
            common   = aligned_returns.index.intersection(rv_diff.index)
            corr_val = 0.0
            if len(common) >= 20:
                r = aligned_returns.loc[common]
                v = rv_diff.loc[common]
                if r.std() > 0 and v.std() > 0:
                    corr_val = float(r.corr(v))
                    corr_val = 0.0 if np.isnan(corr_val) else corr_val

            # Map correlation to regime using same thresholds as Tier 1
            if corr_val > 0.1:
                regime = VolRegime.STICKY_DELTA
            elif corr_val > -0.15:
                regime = VolRegime.STICKY_STRIKE
            elif corr_val > -0.40:
                regime = VolRegime.STICKY_LOCAL_VOL
            else:
                regime = VolRegime.JUMPY_VOL

            # VIX context still used for display metrics (not regime decision)
            vix_year = self._fetch_vix_year(anchor_date)
            if vix_year is not None and len(vix_year) >= 60:
                vix_pct     = float(pd.Series(vix_year.values).rank(pct=True).iloc[-1] * 100)
                vix_trend   = float(vix_year.iloc[-5:].mean() - vix_year.iloc[-20:].mean())
                vix_current = float(vix_year.iloc[-1])
            else:
                rv_vals     = rolling_vol.values
                vix_pct     = float(np.mean(rv_vals[-1] >= rv_vals) * 100)
                vix_trend   = 0.0
                vix_current = float(rolling_vol.iloc[-1])

            primary_corr = corr_val
            iv_source    = 'realized_spot_vol_corr'

        # 4. Vol-of-vol (how jumpy is vol itself) — kept for dashboard display
        vol_changes = rolling_vol.diff().dropna()
        vol_of_vol = vol_changes.std() * np.sqrt(252) if len(vol_changes) > 5 else 0.0

        avg_abs_return = aligned_returns.abs().mean()
        avg_vol_change = vol_changes.abs().mean() if len(vol_changes) > 0 else 0
        realised_skew_ratio = (avg_vol_change / avg_abs_return) if avg_abs_return > 0 else 0

        # 5. Compute √T-normalised skew comparison if enough data
        sqrt_t_skew = self._compute_sqrt_time_skew(rolling_vol, log_returns)

        result = {
            'symbol': symbol,
            'regime': regime,
            'regime_info': REGIME_INFO[regime],
            'confidence_multiplier': REGIME_CONFIDENCE_MULTIPLIER[regime],
            'vix_percentile': round(vix_pct, 1),
            'vix_trend': round(vix_trend, 2),
            'vix_current': round(vix_current, 2),
            'iv_source': iv_source,
            # kept for dashboard backward-compat
            'spot_vol_corr_5d': primary_corr,
            'spot_vol_corr_20d': primary_corr,
            'spot_vol_corr_60d': primary_corr,
            'primary_corr': primary_corr,
            'realised_vol': float(rolling_vol.iloc[-1]) if len(rolling_vol) > 0 else 0,
            'vol_of_vol': float(vol_of_vol),
            'realised_skew_ratio': float(realised_skew_ratio),
            'sqrt_t_skew': sqrt_t_skew,
            'timestamp': datetime.now().isoformat(),
            'ts_dates': [d.isoformat() for d in rolling_vol.index[-60:]],
            'ts_realised_vol': rolling_vol.values[-60:].tolist(),
            'ts_spot_returns': aligned_returns.values[-60:].tolist(),
            'ts_close': close.loc[rolling_vol.index[-60:]].values.tolist(),
        }

        self._cache[symbol] = result
        return result

    def classify_universe(self, symbols: List[str]) -> Dict[str, Dict]:
        """Classify regimes for a list of symbols in parallel."""
        from concurrent.futures import ThreadPoolExecutor, as_completed  # noqa: PLC0415
        results: Dict[str, Dict] = {}

        def _classify_one(sym: str) -> tuple:
            try:
                return sym, self.classify(sym)
            except Exception as e:
                logger.warning(f"Regime classification failed for {sym}: {e}")
                return sym, self._empty_result(sym)

        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(_classify_one, s): s for s in symbols}
            for fut in as_completed(futs):
                sym, res = fut.result()
                results[sym] = res
        return results

    def get_cached(self, symbol: str) -> Optional[Dict]:
        """Return cached result if available."""
        return self._cache.get(symbol)

    # ------------------------------------------------------------------
    # Classification logic
    # ------------------------------------------------------------------

    @staticmethod
    def _classify_from_vix(vix_pct: float, vix_trend: float) -> VolRegime:
        """Map VIX percentile + trend to Bennett's four regimes.

        Primary signal: VIX percentile vs trailing 252-day history.
          <30th pct  → low vol environment   → Sticky Delta
          30-60th    → moderate vol          → Sticky Strike
          60-80th    → elevated vol          → Sticky Local Vol
          >80th      → high/panic vol        → Jumpy Vol

        Secondary adjustment: rising vol (vix_trend > +2 VIX points) upgrades
        one step toward Jumpy; falling vol (< -2) downgrades one step toward Delta.
        """
        # Base regime from percentile
        if vix_pct < 45:
            base = 0   # sticky_delta
        elif vix_pct < 65:
            base = 1   # sticky_strike
        elif vix_pct < 85:
            base = 2   # sticky_local_vol
        else:
            base = 3   # jumpy_vol

        # Trend adjustment: ±1 step — only trigger on meaningful VIX drift
        # (threshold=4 avoids noise from slow 2022-style grinding moves)
        if vix_trend > 4:
            base = min(base + 1, 3)
        elif vix_trend < -4:
            base = max(base - 1, 0)

        return [
            VolRegime.STICKY_DELTA,
            VolRegime.STICKY_STRIKE,
            VolRegime.STICKY_LOCAL_VOL,
            VolRegime.JUMPY_VOL,
        ][base]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _rolling_corr(returns: pd.Series, vol: pd.Series, window: int) -> float:
        """Compute trailing correlation between returns and vol changes."""
        vol_chg = vol.diff().dropna()
        common = returns.index.intersection(vol_chg.index)
        if len(common) < max(window, 5):
            return 0.0
        r = returns.loc[common].iloc[-window:]
        v = vol_chg.loc[common].iloc[-window:]
        if r.std() == 0 or v.std() == 0:
            return 0.0
        return float(r.corr(v))

    @staticmethod
    def _compute_sqrt_time_skew(rolling_vol: pd.Series, returns: pd.Series) -> Dict:
        """Bennett pp.144-146: skew × √T should be roughly constant.

        We approximate by computing the vol-move-per-return-unit at different
        rolling windows and multiplying by √(window/252).
        """
        result = {}
        for w in [5, 10, 21, 42]:
            if len(returns) < w + 5:
                continue
            r = returns.iloc[-w:]
            v = rolling_vol.diff().dropna()
            common = r.index.intersection(v.index)
            if len(common) < w:
                continue
            r_slice = r.loc[common]
            v_slice = v.loc[common]
            if r_slice.std() == 0:
                continue
            # "skew" ≈ regression slope of vol_change on return
            slope = np.polyfit(r_slice.values, v_slice.values, 1)[0] if len(r_slice) > 2 else 0
            T_years = w / 252
            normalised = slope * np.sqrt(T_years)
            result[f'{w}d'] = {
                'raw_slope': float(slope),
                'sqrt_t_normalised': float(normalised),
                'window_days': w,
            }
        return result

    def _fetch_vix_year(self, anchor_date) -> Optional[pd.Series]:
        """Fetch ~252 trading days of VIX ending at anchor_date.

        Returns a tz-naive pd.Series of VIX closing prices, or None on failure.
        Used for percentile and trend calculations.
        """
        if yf is None:
            return None
        try:
            end   = pd.Timestamp(anchor_date).tz_localize(None) + pd.Timedelta(days=3)
            start = end - pd.Timedelta(days=400)  # ~252 trading days + buffer
            vix = yf.Ticker('^VIX').history(start=start.date(), end=end.date())
            if vix is None or vix.empty:
                return None
            vix_close = vix['Close']
            if vix_close.index.tz is not None:
                vix_close.index = vix_close.index.tz_localize(None)
            return vix_close.sort_index()
        except Exception as e:
            logger.debug(f"VIX year fetch failed: {e}")
            return None

    def _fetch_vix(self, target_index: pd.Index) -> Optional[pd.Series]:
        """Fetch VIX close prices aligned to target_index.

        Returns a pd.Series of VIX levels indexed to match the ticker dates,
        or None if fetching fails.  The caller will call .diff() via _rolling_corr.
        """
        if yf is None:
            return None
        try:
            start = target_index.min() - pd.Timedelta(days=10)
            end   = target_index.max() + pd.Timedelta(days=2)
            vix = yf.Ticker('^VIX').history(start=start.date(), end=end.date())
            if vix is None or vix.empty:
                return None
            vix_close = vix['Close'].rename('VIX')
            # Normalize to tz-naive so reindex matches ticker data (which may be tz-naive)
            if vix_close.index.tz is not None:
                vix_close.index = vix_close.index.tz_localize(None)
            target_naive = target_index.tz_localize(None) if target_index.tz is not None else target_index
            # Align to ticker dates (forward-fill at most 3 days for holidays)
            vix_aligned = vix_close.reindex(target_naive, method='ffill', limit=3)
            if vix_aligned.isna().sum() > len(target_index) * 0.10:
                return None  # too many missing values
            return vix_aligned.dropna()
        except Exception as e:
            logger.debug(f"VIX fetch failed, falling back to realized vol: {e}")
            return None

    def _fetch_history(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch historical price data via yfinance, patching today's price via fast_info."""
        if yf is None:
            logger.warning("yfinance not available for regime classification")
            return None
        try:
            period = f"{max(self.lookback_days + 30, 90)}d"
            ticker = yf.Ticker(symbol)
            data = ticker.history(period=period)
            if data is None or data.empty:
                return None

            # Patch today's close with intraday price to avoid 1-2 day yfinance lag
            try:
                fi = ticker.fast_info
                live_price = fi.get('lastPrice') or fi.get('regularMarketPrice')
                if live_price and float(live_price) > 0:
                    today = pd.Timestamp(datetime.today().date())
                    last_date = data.index[-1].normalize() if not data.empty else None
                    new_row = data.iloc[-1].copy()
                    new_row['Close'] = float(live_price)
                    new_row['Open'] = float(live_price)
                    new_row['High'] = float(live_price)
                    new_row['Low'] = float(live_price)
                    if last_date is None or today > last_date:
                        new_row.name = today
                        data = pd.concat([data, new_row.to_frame().T])
                    else:
                        data.iloc[-1, data.columns.get_loc('Close')] = float(live_price)
            except Exception:
                pass  # fast_info failure is non-fatal

            return data
        except Exception as e:
            logger.warning(f"Failed to fetch history for {symbol}: {e}")
            return None

    @staticmethod
    def _empty_result(symbol: str) -> Dict:
        return {
            'symbol': symbol,
            'regime': VolRegime.UNKNOWN,
            'regime_info': REGIME_INFO[VolRegime.UNKNOWN],
            'confidence_multiplier': REGIME_CONFIDENCE_MULTIPLIER[VolRegime.UNKNOWN],
            'spot_vol_corr_5d': 0.0,
            'spot_vol_corr_20d': 0.0,
            'spot_vol_corr_60d': 0.0,
            'primary_corr': 0.0,
            'realised_vol': 0.0,
            'vol_of_vol': 0.0,
            'realised_skew_ratio': 0.0,
            'sqrt_t_skew': {},
            'timestamp': datetime.now().isoformat(),
            'ts_dates': [],
            'ts_realised_vol': [],
            'ts_spot_returns': [],
            'ts_close': [],
        }
