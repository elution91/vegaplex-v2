"""
Skew History Module
Persistent storage for skew snapshots with percentile ranking,
forward skew extraction, stickiness measurement, multi-horizon
realized vol, and universe state radar.

Bennett pp.180-225: Skew dynamics, forward skew, mean reversion signals.
"""

import sqlite3
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path

import numpy as np
from scipy import stats

try:
    import yfinance as yf
except ImportError:
    yf = None

logger = logging.getLogger(__name__)

# Database location:
#   default — beside the scanner code (local dev)
#   override — set VEGAPLEX_DATA_DIR (e.g. /app/data on Render persistent disk)
import os  # noqa: E402
_DATA_DIR = os.environ.get('VEGAPLEX_DATA_DIR')
if _DATA_DIR:
    DB_PATH = Path(_DATA_DIR) / 'skew_history.db'
else:
    DB_PATH = Path(__file__).parent / 'skew_history.db'


class SkewHistory:
    """Persistent skew history with analytics for forward-looking skew dynamics."""

    def __init__(self, db_path: str = None):
        self.db_path = str(db_path or DB_PATH)
        self._init_db()

    # ------------------------------------------------------------------
    # Database setup
    # ------------------------------------------------------------------

    def _init_db(self):
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS skew_snapshots (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol      TEXT NOT NULL,
                    ts          TEXT NOT NULL,             -- ISO timestamp
                    -- per-expiry key metrics (JSON array)
                    by_expiry   TEXT,                      -- [{expiry, tte, slope, atm_vol, skew_25, skew_10, curvature}, ...]
                    -- aggregate metrics (flattened for fast queries)
                    call_slope      REAL,
                    put_slope       REAL,
                    call_atm_vol    REAL,
                    put_atm_vol     REAL,
                    call_skew_25    REAL,
                    put_skew_25     REAL,
                    call_curvature  REAL,
                    put_curvature   REAL,
                    -- term structure of skew
                    call_skew_trend REAL,   -- d(skew_slope)/d(tte)
                    put_skew_trend  REAL,
                    term_steepness  REAL,
                    -- forward skew (extracted)
                    fwd_skew_near   REAL,   -- forward skew for near-to-mid period
                    fwd_skew_far    REAL,   -- forward skew for mid-to-far period
                    fwd_vol_near    REAL,   -- forward ATM vol near-to-mid
                    fwd_vol_far     REAL,   -- forward ATM vol mid-to-far
                    -- multi-horizon realized vol
                    rv_5d           REAL,   -- 5-day annualized realized vol
                    rv_10d          REAL,   -- 10-day
                    rv_21d          REAL,   -- 21-day (1 month)
                    -- regime context
                    regime          TEXT,
                    spot_price      REAL,
                    overall_score   REAL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_skew_symbol_ts
                ON skew_snapshots (symbol, ts)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_skew_symbol
                ON skew_snapshots (symbol)
            """)
            # ATM IV history — one row per symbol per day
            conn.execute("""
                CREATE TABLE IF NOT EXISTS iv_history (
                    symbol  TEXT NOT NULL,
                    date    TEXT NOT NULL,   -- YYYY-MM-DD
                    atm_iv  REAL NOT NULL,   -- annualised (0.20 = 20%)
                    source  TEXT,            -- 'ibkr' | 'yfinance'
                    PRIMARY KEY (symbol, date)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_iv_history_symbol_date
                ON iv_history (symbol, date)
            """)

            # Backward-compatible migration: add columns if they don't exist
            existing = {r[1] for r in conn.execute(
                "PRAGMA table_info(skew_snapshots)"
            ).fetchall()}
            for col in ('rv_5d', 'rv_10d', 'rv_21d'):
                if col not in existing:
                    conn.execute(f"ALTER TABLE skew_snapshots ADD COLUMN {col} REAL")
                    logger.info(f"Added column {col} to skew_snapshots")

            # Scan log table — persists opportunity history across restarts
            conn.execute("""
                CREATE TABLE IF NOT EXISTS scan_log (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp       TEXT NOT NULL,
                    symbol          TEXT NOT NULL,
                    opportunity_type TEXT,
                    subtype         TEXT,
                    confidence      REAL,
                    expected_pnl    REAL,
                    max_loss        REAL,
                    max_gain        REAL,
                    risk_reward     REAL,
                    regime          TEXT,
                    rationale       TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_scan_log_ts
                ON scan_log (timestamp)
            """)

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    # ------------------------------------------------------------------
    # ATM IV history
    # ------------------------------------------------------------------

    def store_iv_series(self, symbol: str, dates: list, ivs: list, source: str = 'ibkr'):
        """Upsert a batch of (date, atm_iv) rows for *symbol*.

        Existing rows for the same (symbol, date) are replaced so re-runs
        are safe.  dates should be 'YYYY-MM-DD' strings; ivs are floats.
        """
        rows = [(symbol, d, v, source) for d, v in zip(dates, ivs) if v and v == v]
        if not rows:
            return
        with self._conn() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO iv_history (symbol, date, atm_iv, source) "
                "VALUES (?, ?, ?, ?)",
                rows,
            )
        logger.info(f"Stored {len(rows)} IV rows for {symbol} (source={source})")

    def get_iv_series(self, symbol: str, days: int = 365, as_of_date: str = None):
        """Return a pd.Series of daily ATM IV indexed by date for *symbol*.

        Parameters
        ----------
        as_of_date : str, optional
            'YYYY-MM-DD' upper bound — returns data UP TO this date.
            Defaults to the most recent stored row.  Pass this when
            doing historical back-tests so the classifier only sees data
            that would have been available at that point in time.

        Returns None if fewer than 30 rows are available.
        """
        try:
            import pandas as pd
        except ImportError:
            return None
        with self._conn() as conn:
            if as_of_date:
                rows = conn.execute(
                    "SELECT date, atm_iv FROM iv_history "
                    "WHERE symbol = ? AND date <= ? ORDER BY date DESC LIMIT ?",
                    (symbol, as_of_date, days),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT date, atm_iv FROM iv_history "
                    "WHERE symbol = ? ORDER BY date DESC LIMIT ?",
                    (symbol, days),
                ).fetchall()
        if len(rows) < 30:
            return None
        dates = [r[0] for r in reversed(rows)]
        ivs   = [r[1] for r in reversed(rows)]
        return pd.Series(ivs, index=pd.to_datetime(dates), name='atm_iv')

    def latest_iv_date(self, symbol: str) -> Optional[str]:
        """Return the most recent stored date for *symbol*, or None."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM iv_history WHERE symbol = ?", (symbol,)
            ).fetchone()
        return row[0] if row and row[0] else None

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record(self, symbol: str, skew_metrics: Dict, vol_surface: Dict = None,
               regime_data: Dict = None):
        """Record a skew snapshot from a scan.

        Args:
            symbol: Ticker symbol.
            skew_metrics: Output of SkewAnalyzer.analyze_skew().
            vol_surface: Optional vol surface dict for forward skew calc.
            regime_data: Optional regime classification dict.
        """
        if not skew_metrics or not skew_metrics.get('call_skew'):
            return

        call_agg = skew_metrics.get('call_skew', {}).get('aggregate', {})
        put_agg = skew_metrics.get('put_skew', {}).get('aggregate', {})
        term = skew_metrics.get('term_skew', {})

        # Serialize per-expiry data (compact)
        by_expiry_list = []
        for side in ('call_skew', 'put_skew'):
            for e in skew_metrics.get(side, {}).get('by_expiry', []):
                by_expiry_list.append({
                    'side': side.split('_')[0],
                    'expiry': str(e.get('expiry', '')),
                    'tte': _safe_float(e.get('tte')),
                    'slope': _safe_float(e.get('slope')),
                    'atm_vol': _safe_float(e.get('atm_vol')),
                    'skew_25': _safe_float(e.get('skew_25')),
                    'skew_10': _safe_float(e.get('skew_10')),
                    'curvature': _safe_float(e.get('curvature')),
                })

        # Extract forward skew from term structure
        fwd = self._extract_forward_skew(skew_metrics)

        # Forward vol from vol_surface term structure
        fwd_vol = self._extract_forward_vol(vol_surface) if vol_surface else {}

        # Multi-horizon realized vol
        rv = self._compute_multi_horizon_rv(symbol)

        row = {
            'symbol': symbol,
            'ts': datetime.now().isoformat(),
            'by_expiry': json.dumps(by_expiry_list),
            'call_slope': _safe_float(call_agg.get('avg_slope')),
            'put_slope': _safe_float(put_agg.get('avg_slope')),
            'call_atm_vol': _safe_float(call_agg.get('avg_atm_vol')),
            'put_atm_vol': _safe_float(put_agg.get('avg_atm_vol')),
            'call_skew_25': _safe_float(call_agg.get('avg_skew_25')),
            'put_skew_25': _safe_float(put_agg.get('avg_skew_25')),
            'call_curvature': _safe_float(call_agg.get('avg_curvature')),
            'put_curvature': _safe_float(put_agg.get('avg_curvature')),
            'call_skew_trend': _safe_float(term.get('call_skew_trend')),
            'put_skew_trend': _safe_float(term.get('put_skew_trend')),
            'term_steepness': _safe_float(term.get('term_skew_steepness')),
            'fwd_skew_near': fwd.get('fwd_skew_near'),
            'fwd_skew_far': fwd.get('fwd_skew_far'),
            'fwd_vol_near': fwd_vol.get('fwd_vol_near'),
            'fwd_vol_far': fwd_vol.get('fwd_vol_far'),
            'rv_5d': rv.get('rv_5d'),
            'rv_10d': rv.get('rv_10d'),
            'rv_21d': rv.get('rv_21d'),
            'regime': (regime_data or {}).get('regime', ''),
            'spot_price': _safe_float(skew_metrics.get('current_price')),
            'overall_score': _safe_float(skew_metrics.get('overall_skew_score')),
        }

        cols = ', '.join(row.keys())
        placeholders = ', '.join(['?'] * len(row))
        with self._conn() as conn:
            conn.execute(
                f"INSERT INTO skew_snapshots ({cols}) VALUES ({placeholders})",
                list(row.values())
            )
        logger.debug(f"Recorded skew snapshot for {symbol}")

    # ------------------------------------------------------------------
    # Multi-horizon realized vol
    # ------------------------------------------------------------------

    def _compute_multi_horizon_rv(self, symbol: str) -> Dict:
        """Compute annualized realized vol over 5d, 10d, 21d windows.

        Uses yfinance to fetch recent price history.  Returns empty dict
        on failure so callers can proceed without RV.
        """
        if yf is None:
            return {}
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period='60d')
            if hist is None or len(hist) < 22:
                return {}
            close = hist['Close'].dropna()
            log_ret = np.log(close / close.shift(1)).dropna()
            if len(log_ret) < 21:
                return {}
            rv = {}
            for window in (5, 10, 21):
                if len(log_ret) >= window:
                    rv[f'rv_{window}d'] = float(
                        log_ret.iloc[-window:].std() * np.sqrt(252)
                    )
            return rv
        except Exception as e:
            logger.debug(f"RV computation failed for {symbol}: {e}")
            return {}

    # ------------------------------------------------------------------
    # Forward skew extraction (Bennett pp.210-225)
    # ------------------------------------------------------------------

    def _extract_forward_skew(self, skew_metrics: Dict) -> Dict:
        """Extract forward-starting skew from the term structure of skew slopes.

        If we have skew slopes at different TTEs, the forward skew between
        T1 and T2 is:
            fwd_skew(T1,T2) = [skew(T2)*T2 - skew(T1)*T1] / (T2 - T1)

        This tells us what the market implies about skew for the *future*
        period between T1 and T2.
        """
        result = {}
        call_by_expiry = skew_metrics.get('call_skew', {}).get('by_expiry', [])
        if len(call_by_expiry) < 2:
            return result

        # Sort by TTE
        sorted_exp = sorted(call_by_expiry, key=lambda x: x.get('tte', 0))
        sorted_exp = [e for e in sorted_exp if e.get('tte') and e.get('tte') > 0]
        if len(sorted_exp) < 2:
            return result

        # Near-to-mid forward skew
        e0, e1 = sorted_exp[0], sorted_exp[1]
        t0, t1 = e0['tte'], e1['tte']
        s0, s1 = e0['slope'], e1['slope']
        if t1 > t0:
            result['fwd_skew_near'] = (s1 * t1 - s0 * t0) / (t1 - t0)

        # Mid-to-far forward skew (if we have 3+ expirations)
        if len(sorted_exp) >= 3:
            e2 = sorted_exp[2]
            t2, s2 = e2['tte'], e2['slope']
            if t2 > t1:
                result['fwd_skew_far'] = (s2 * t2 - s1 * t1) / (t2 - t1)

        return result

    def _extract_forward_vol(self, vol_surface: Dict) -> Dict:
        """Extract forward-starting ATM vol from term structure.

        Forward variance:  var_fwd(T1,T2) = [IV(T2)^2*T2 - IV(T1)^2*T1] / (T2-T1)
        Forward vol = sqrt(var_fwd)
        """
        result = {}
        term = vol_surface.get('term_structure', {})
        atm_vols = term.get('atm_vols', [])
        ttes = term.get('ttes', [])

        if not atm_vols or not ttes or len(atm_vols) < 2:
            return result

        # Pair and sort
        pairs = sorted(zip(ttes, atm_vols), key=lambda x: x[0])
        pairs = [(t, v) for t, v in pairs if t > 0 and v > 0]
        if len(pairs) < 2:
            return result

        t0, v0 = pairs[0]
        t1, v1 = pairs[1]
        var_fwd = (v1**2 * t1 - v0**2 * t0) / (t1 - t0)
        if var_fwd > 0:
            result['fwd_vol_near'] = np.sqrt(var_fwd)

        if len(pairs) >= 3:
            t2, v2 = pairs[2]
            var_fwd2 = (v2**2 * t2 - v1**2 * t1) / (t2 - t1)
            if var_fwd2 > 0:
                result['fwd_vol_far'] = np.sqrt(var_fwd2)

        return result

    # ------------------------------------------------------------------
    # Percentile ranking & context (Bennett pp.180-195)
    # ------------------------------------------------------------------

    def get_percentile(self, symbol: str, metric: str = 'call_slope',
                       lookback_days: int = 60) -> Optional[Dict]:
        """Get current metric value in context of its historical distribution.

        Returns:
            Dict with: current, percentile, mean, std, min, max, z_score, n_obs
        """
        cutoff = (datetime.now() - timedelta(days=lookback_days)).isoformat()
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT {metric} FROM skew_snapshots "
                f"WHERE symbol = ? AND ts >= ? AND {metric} IS NOT NULL "
                f"ORDER BY ts",
                (symbol, cutoff)
            ).fetchall()

        if len(rows) < 5:
            return None

        values = np.array([r[0] for r in rows])
        current = values[-1]
        pct = stats.percentileofscore(values, current, kind='rank')
        mean = np.mean(values)
        std = np.std(values)
        z = (current - mean) / std if std > 0 else 0.0

        return {
            'current': float(current),
            'percentile': float(pct),
            'mean': float(mean),
            'std': float(std),
            'min': float(np.min(values)),
            'max': float(np.max(values)),
            'z_score': float(z),
            'n_obs': len(values),
            'lookback_days': lookback_days,
        }

    def get_context(self, symbol: str, lookback_days: int = 60) -> Dict:
        """Full skew context: percentiles for all key metrics + forward skew + RV."""
        metrics = [
            'call_slope', 'put_slope',
            'call_atm_vol', 'put_atm_vol',
            'call_skew_25', 'put_skew_25',
            'call_curvature', 'put_curvature',
            'term_steepness',
            'fwd_skew_near', 'fwd_skew_far',
            'fwd_vol_near', 'fwd_vol_far',
            'rv_5d', 'rv_10d', 'rv_21d',
        ]
        context = {}
        for m in metrics:
            pct = self.get_percentile(symbol, m, lookback_days)
            if pct:
                context[m] = pct
        return context

    # ------------------------------------------------------------------
    # Scan log — persistent opportunity history
    # ------------------------------------------------------------------

    def log_scan_result(self, symbol: str, opp_type: str, subtype: str,
                        confidence: float, expected_pnl: float,
                        max_loss: float, max_gain: float,
                        risk_reward: float, regime: str,
                        rationale: str = '') -> None:
        """Log a detected opportunity to the persistent scan_log table."""
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO scan_log
                   (timestamp, symbol, opportunity_type, subtype, confidence,
                    expected_pnl, max_loss, max_gain, risk_reward, regime, rationale)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (datetime.now().isoformat(), symbol, opp_type, subtype,
                 confidence, expected_pnl, max_loss, max_gain, risk_reward,
                 regime, rationale)
            )

    def get_scan_history(self, limit: int = 200) -> List[Dict]:
        """Load scan history from DB, ordered by timestamp descending."""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT id, timestamp, symbol, opportunity_type, subtype,
                          confidence, expected_pnl, max_loss, max_gain,
                          risk_reward, regime, rationale
                   FROM scan_log ORDER BY timestamp DESC LIMIT ?""",
                (limit,)
            ).fetchall()
        cols = ['id', 'timestamp', 'symbol', 'opportunity_type', 'subtype',
                'confidence', 'expected_pnl', 'max_loss', 'max_gain',
                'risk_reward', 'regime', 'rationale']
        return [dict(zip(cols, r)) for r in rows]

    def get_mean_reversion_signal(self, symbol: str, lookback_days: int = 60,
                                  threshold_z: float = 1.5) -> Optional[Dict]:
        """Detect mean reversion opportunities in skew.

        Returns signal when skew is significantly above/below its mean,
        indicating likely reversion.
        """
        ctx = self.get_context(symbol, lookback_days)
        if not ctx:
            return None

        signals = []
        for metric, data in ctx.items():
            z = data['z_score']
            pct = data['percentile']
            if abs(z) >= threshold_z:
                direction = 'skew_too_steep' if z > 0 else 'skew_too_flat'
                signals.append({
                    'metric': metric,
                    'z_score': z,
                    'percentile': pct,
                    'direction': direction,
                    'current': data['current'],
                    'mean': data['mean'],
                    'expected_reversion': data['mean'] - data['current'],
                })

        if not signals:
            return None

        # Pick the strongest signal
        strongest = max(signals, key=lambda s: abs(s['z_score']))
        return {
            'symbol': symbol,
            'primary_signal': strongest,
            'all_signals': signals,
            'signal_count': len(signals),
        }

    # ------------------------------------------------------------------
    # Skew stickiness measurement (Bennett pp.154-169 extension)
    # ------------------------------------------------------------------

    def get_stickiness_ratio(self, symbol: str, lookback_days: int = 30,
                             min_obs: int = 10) -> Optional[Dict]:
        """Measure how much skew moves per unit spot move.

        stickiness = beta from regression: Δ(call_slope) ~ β * Δ(spot_price)

        High stickiness → skew moves a lot with spot (jumpy vol behavior)
        Low stickiness → skew is independent of spot (sticky strike/delta)
        """
        cutoff = (datetime.now() - timedelta(days=lookback_days)).isoformat()
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT call_slope, put_slope, spot_price, call_atm_vol FROM skew_snapshots "
                "WHERE symbol = ? AND ts >= ? AND spot_price IS NOT NULL "
                "AND call_slope IS NOT NULL "
                "ORDER BY ts",
                (symbol, cutoff)
            ).fetchall()

        if len(rows) < min_obs:
            return None

        call_slopes = np.array([r[0] for r in rows])
        put_slopes = np.array([r[1] for r in rows if r[1] is not None])
        spots = np.array([r[2] for r in rows])
        atm_vols = np.array([r[3] for r in rows if r[3] is not None])

        # Changes
        d_call_slope = np.diff(call_slopes)
        d_spot = np.diff(spots)
        d_spot_pct = d_spot / spots[:-1]

        # Filter out zero moves
        mask = np.abs(d_spot_pct) > 1e-6
        if mask.sum() < 5:
            return None

        # Regression: Δ(skew_slope) = α + β * Δ(spot%)
        slope_beta, intercept, r, p, se = stats.linregress(
            d_spot_pct[mask], d_call_slope[mask]
        )

        # Also measure vol-spot sensitivity (Δvol / Δspot%)
        vol_spot_beta = None
        if len(atm_vols) == len(spots):
            d_vol = np.diff(atm_vols)
            vol_beta, _, r_vol, p_vol, _ = stats.linregress(
                d_spot_pct[mask], d_vol[mask]
            )
            vol_spot_beta = float(vol_beta)

        return {
            'symbol': symbol,
            'skew_spot_beta': float(slope_beta),      # Δ(skew)/Δ(spot%)
            'skew_spot_r2': float(r**2),
            'skew_spot_pvalue': float(p),
            'vol_spot_beta': vol_spot_beta,            # Δ(vol)/Δ(spot%)
            'n_obs': int(mask.sum()),
            'lookback_days': lookback_days,
            'interpretation': _interpret_stickiness(slope_beta, r**2),
        }

    # ------------------------------------------------------------------
    # Persistence tracking
    # ------------------------------------------------------------------

    def get_persistence(self, symbol: str, metric: str = 'call_slope',
                        threshold_pct: float = 80.0,
                        lookback_days: int = 60) -> Optional[Dict]:
        """Count how many consecutive recent scans the metric was above/below
        a percentile threshold.

        Returns:
            Dict with consecutive_above, consecutive_below, current_pct, n_obs
        """
        ctx = self.get_percentile(symbol, metric, lookback_days)
        if not ctx:
            return None

        cutoff = (datetime.now() - timedelta(days=lookback_days)).isoformat()
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT {metric} FROM skew_snapshots "
                f"WHERE symbol = ? AND ts >= ? AND {metric} IS NOT NULL "
                f"ORDER BY ts DESC",
                (symbol, cutoff)
            ).fetchall()

        if len(rows) < 3:
            return None

        values = np.array([r[0] for r in rows])
        all_values = values[::-1]  # chronological
        mean = np.mean(all_values)
        std = np.std(all_values)
        if std == 0:
            return None

        # Compute percentile for each observation
        pcts = np.array([
            stats.percentileofscore(all_values, v, kind='rank')
            for v in all_values
        ])

        # Count consecutive from most recent
        consecutive_above = 0
        consecutive_below = 0
        for p in pcts[::-1]:  # newest first
            if p >= threshold_pct:
                consecutive_above += 1
            else:
                break
        for p in pcts[::-1]:
            if p <= (100 - threshold_pct):
                consecutive_below += 1
            else:
                break

        return {
            'current_pct': float(pcts[-1]),
            'consecutive_above': consecutive_above,
            'consecutive_below': consecutive_below,
            'threshold_pct': threshold_pct,
            'n_obs': len(values),
        }

    # ------------------------------------------------------------------
    # Universe state radar (cross-sectional view)
    # ------------------------------------------------------------------

    def get_universe_state(self, symbols: List[str],
                           lookback_days: int = 60) -> List[Dict]:
        """Build a state vector for every symbol in the universe.

        For each symbol returns percentiles for key metrics, IV-RV spread,
        persistence flags, and regime — all in one row suitable for a
        heatmap / radar display.

        Returns:
            List of dicts, one per symbol.
        """
        # Radar metrics: these are the columns of the heatmap
        radar_metrics = [
            'call_slope', 'call_atm_vol', 'call_skew_25',
            'call_curvature', 'term_steepness',
            'fwd_skew_near',
        ]
        rv_metrics = ['rv_5d', 'rv_10d', 'rv_21d']

        rows = []
        for sym in symbols:
            n_obs = self.get_snapshot_count(sym)
            if n_obs < 3:
                # Not enough data yet — placeholder row
                rows.append({
                    'symbol': sym,
                    'n_obs': n_obs,
                    'has_data': False,
                })
                continue

            row = {'symbol': sym, 'n_obs': n_obs, 'has_data': True}

            # Percentiles for surface shape metrics
            for m in radar_metrics:
                pct = self.get_percentile(sym, m, lookback_days)
                if pct:
                    row[f'{m}_pct'] = pct['percentile']
                    row[f'{m}_z'] = pct['z_score']
                    row[f'{m}_val'] = pct['current']

            # RV metrics + IV-RV spread
            for m in rv_metrics:
                pct = self.get_percentile(sym, m, lookback_days)
                if pct:
                    row[f'{m}_pct'] = pct['percentile']
                    row[f'{m}_val'] = pct['current']

            # IV - RV spread (ATM vol vs realized vol at each horizon)
            atm = row.get('call_atm_vol_val')
            for h in (5, 10, 21):
                rv_val = row.get(f'rv_{h}d_val')
                if atm is not None and rv_val is not None and rv_val > 0:
                    row[f'iv_rv_{h}d'] = atm - rv_val

            # Persistence: is skew slope persistently extreme?
            persist = self.get_persistence(sym, 'call_slope', 80.0, lookback_days)
            if persist:
                row['persist_above'] = persist['consecutive_above']
                row['persist_below'] = persist['consecutive_below']

            # Regime from latest snapshot
            latest = self.get_latest(sym)
            if latest:
                row['regime'] = latest.get('regime', '')

            rows.append(row)

        return rows

    # ------------------------------------------------------------------
    # Time series retrieval for charts
    # ------------------------------------------------------------------

    def get_history(self, symbol: str, metric: str = 'call_slope',
                    lookback_days: int = 90) -> List[Dict]:
        """Get time series of a metric for charting."""
        cutoff = (datetime.now() - timedelta(days=lookback_days)).isoformat()
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT ts, {metric} FROM skew_snapshots "
                f"WHERE symbol = ? AND ts >= ? AND {metric} IS NOT NULL "
                f"ORDER BY ts",
                (symbol, cutoff)
            ).fetchall()
        return [{'ts': r[0], 'value': r[1]} for r in rows]

    def get_forward_skew_history(self, symbol: str,
                                 lookback_days: int = 90) -> List[Dict]:
        """Get time series of forward skew for charting."""
        cutoff = (datetime.now() - timedelta(days=lookback_days)).isoformat()
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT ts, fwd_skew_near, fwd_skew_far, fwd_vol_near, fwd_vol_far, "
                "call_slope, spot_price "
                "FROM skew_snapshots "
                "WHERE symbol = ? AND ts >= ? "
                "ORDER BY ts",
                (symbol, cutoff)
            ).fetchall()
        return [{
            'ts': r[0],
            'fwd_skew_near': r[1],
            'fwd_skew_far': r[2],
            'fwd_vol_near': r[3],
            'fwd_vol_far': r[4],
            'call_slope': r[5],
            'spot_price': r[6],
        } for r in rows]

    def get_snapshot_count(self, symbol: str = None) -> int:
        """Get total snapshot count, optionally for a specific symbol."""
        with self._conn() as conn:
            if symbol:
                row = conn.execute(
                    "SELECT COUNT(*) FROM skew_snapshots WHERE symbol = ?",
                    (symbol,)
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT COUNT(*) FROM skew_snapshots"
                ).fetchone()
        return row[0] if row else 0

    def get_symbols(self) -> List[str]:
        """Get all symbols with history."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM skew_snapshots ORDER BY symbol"
            ).fetchall()
        return [r[0] for r in rows]

    def get_latest(self, symbol: str) -> Optional[Dict]:
        """Get the most recent snapshot for a symbol."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM skew_snapshots WHERE symbol = ? ORDER BY ts DESC LIMIT 1",
                (symbol,)
            ).fetchone()
        if not row:
            return None
        cols = [d[0] for d in conn.execute(
            "SELECT * FROM skew_snapshots LIMIT 0"
        ).description]
        # Re-fetch with column names
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM skew_snapshots WHERE symbol = ? ORDER BY ts DESC LIMIT 1",
                (symbol,)
            ).fetchone()
        return dict(row) if row else None


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _safe_float(val) -> Optional[float]:
    """Convert to float, returning None for NaN/inf/None."""
    if val is None:
        return None
    try:
        f = float(val)
        if np.isnan(f) or np.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _interpret_stickiness(beta: float, r2: float) -> str:
    """Interpret skew-spot stickiness beta."""
    if r2 < 0.05:
        return "No significant skew-spot relationship (regime-independent skew)"
    if beta > 0.5:
        return "High stickiness: skew steepens on sell-offs (jumpy vol behavior)"
    elif beta > 0.1:
        return "Moderate stickiness: skew responds to spot moves (sticky local vol)"
    elif beta > -0.1:
        return "Low stickiness: skew relatively stable (sticky strike/delta)"
    else:
        return "Negative stickiness: skew flattens on sell-offs (unusual, check data)"
