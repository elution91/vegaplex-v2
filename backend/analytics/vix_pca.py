"""
VIX Term Structure PCA
======================
Applies principal component analysis to the daily VX futures strip
(M1–M8) following Johnson (2017) "Risk Premia and the VIX Term Structure"
(JFQA 52(6):2461-2490).

Key findings from Johnson (2017):
  PC1 (LEVEL)   — parallel shift of the curve; explains ~90% of variance
  PC2 (SLOPE)   — tilt; explains most of the *predictive* power for
                  variance swap / VIX futures / straddle excess returns
  PC3 (CURVE)   — curvature; minor incremental predictability

The SLOPE score is the primary trading signal: high SLOPE (steep contango)
→ vol risk premium is well-compensated; low/negative SLOPE (flat / inversion)
→ reduce or exit.

Data flow
---------
1. vix_futures_engine stores each day's strip → SQLite vix_strip_history
2. VIXTermStructurePCA.fit() runs PCA on the accumulated matrix
3. .transform_current() projects today's strip into PC space
4. Dashboard renders SLOPE history, loadings chart, and percentile KPI

Minimum history to fit: MIN_FIT_DAYS (60 rows).  Below that, get_signal()
returns a stub with is_ready=False.

Polygon backfill
----------------
When the Polygon flat files arrive, call backfill_from_polygon(df) to
populate historical rows before the live accumulation started.
"""

import logging
import sqlite3
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Require sklearn; graceful degradation if missing
try:
    from sklearn.preprocessing import StandardScaler
    from sklearn.decomposition import PCA
    _SKLEARN = True
except ImportError:
    _SKLEARN = False
    logger.warning("scikit-learn not installed — VIX PCA disabled")

import os  # noqa: E402
_DATA_DIR = os.environ.get('VEGAPLEX_DATA_DIR')
DB_PATH = (Path(_DATA_DIR) / 'skew_history.db'
           if _DATA_DIR
           else Path(__file__).parent / 'skew_history.db')
MIN_FIT_DAYS = 60   # minimum strip observations needed before PCA is meaningful
N_CONTRACTS  = 8    # M1 … M8


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

def _init_strip_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vix_strip_history (
            trade_date  TEXT PRIMARY KEY,   -- YYYY-MM-DD
            m1  REAL, m2  REAL, m3  REAL, m4  REAL,
            m5  REAL, m6  REAL, m7  REAL, m8  REAL,
            source      TEXT                -- 'live' | 'ibkr' | 'polygon' | 'vixcentral'
        )
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class VIXTermStructurePCA:
    """Fit and query a PCA model of the VX futures term structure."""

    def __init__(self, db_path: str = None):
        self.db_path = str(db_path or DB_PATH)
        self._pca:    Optional[PCA]           = None
        self._scaler: Optional[StandardScaler] = None
        self._history: Optional[pd.DataFrame] = None   # rows=dates, cols=m1..m8
        self._slope_history: Optional[pd.Series] = None
        self._fitted = False

        with self._conn() as conn:
            _init_strip_table(conn)

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    # ------------------------------------------------------------------
    # Storage
    # ------------------------------------------------------------------

    def store_strip(self, strip: list, trade_date: str = None,
                    source: str = 'vixcentral') -> bool:
        """Persist today's futures strip.  strip = [{label, level, dte}, ...]

        Returns True if stored, False if already present or strip too short.
        """
        if not strip or len(strip) < 2:
            return False
        dt = trade_date or date.today().isoformat()
        levels = [c['level'] for c in strip[:N_CONTRACTS]]
        # Pad to 8 with None if fewer contracts available
        while len(levels) < N_CONTRACTS:
            levels.append(None)

        try:
            with self._conn() as conn:
                _init_strip_table(conn)
                conn.execute(
                    "INSERT OR IGNORE INTO vix_strip_history "
                    "(trade_date, m1, m2, m3, m4, m5, m6, m7, m8, source) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    [dt] + levels + [source],
                )
                conn.commit()
            return True
        except Exception as e:
            logger.warning(f"vix_strip store failed: {e}")
            return False

    def backfill_from_polygon(self, df: pd.DataFrame) -> int:
        """Bulk-insert historical strip rows from a Polygon flat-file DataFrame.

        Expected columns: trade_date (str YYYY-MM-DD), m1..m8 (float).
        Returns number of rows inserted.
        """
        inserted = 0
        try:
            with self._conn() as conn:
                _init_strip_table(conn)
                for _, row in df.iterrows():
                    levels = [row.get(f'm{i}') for i in range(1, N_CONTRACTS + 1)]
                    conn.execute(
                        "INSERT OR IGNORE INTO vix_strip_history "
                        "(trade_date, m1, m2, m3, m4, m5, m6, m7, m8, source) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?)",
                        [str(row['trade_date'])] + levels + ['polygon'],
                    )
                    inserted += 1
                conn.commit()
        except Exception as e:
            logger.warning(f"Polygon backfill failed: {e}")
        return inserted

    # ------------------------------------------------------------------
    # Load history
    # ------------------------------------------------------------------

    def load_history(self) -> pd.DataFrame:
        """Return all stored strip rows as a DataFrame indexed by trade_date."""
        try:
            with self._conn() as conn:
                _init_strip_table(conn)
                df = pd.read_sql(
                    "SELECT trade_date, m1, m2, m3, m4, m5, m6, m7, m8 "
                    "FROM vix_strip_history ORDER BY trade_date",
                    conn, parse_dates=['trade_date'],
                )
            df = df.set_index('trade_date')
            # Drop rows missing more than half the contracts
            df = df.dropna(thresh=4)
            return df
        except Exception as e:
            logger.warning(f"vix_strip load failed: {e}")
            return pd.DataFrame()

    def n_observations(self) -> int:
        try:
            with self._conn() as conn:
                _init_strip_table(conn)
                return conn.execute(
                    "SELECT COUNT(*) FROM vix_strip_history"
                ).fetchone()[0]
        except Exception:
            return 0

    # ------------------------------------------------------------------
    # PCA fitting
    # ------------------------------------------------------------------

    def fit(self) -> bool:
        """Fit PCA on all available strip history.

        Returns True if successful, False if insufficient data or sklearn missing.
        """
        if not _SKLEARN:
            return False

        df = self.load_history()
        if len(df) < MIN_FIT_DAYS:
            logger.info(f"VIX PCA: only {len(df)} rows, need {MIN_FIT_DAYS}")
            return False

        # Forward-fill isolated NaNs (e.g., missing M7/M8 on some days)
        df = df.fillna(method='ffill').dropna()
        if len(df) < MIN_FIT_DAYS:
            return False

        self._history = df
        X = df.values  # shape (n_days, n_contracts)

        # Standardize by column (each maturity point centred + unit variance)
        # — Johnson (2017) uses levels; standardising here makes loadings
        #   comparable across maturity points regardless of absolute level.
        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)

        self._pca = PCA(n_components=min(3, X_scaled.shape[1]))
        self._pca.fit(X_scaled)

        # Store SLOPE history (PC2 scores) as a Series indexed by date
        scores = self._pca.transform(X_scaled)
        self._slope_history = pd.Series(
            scores[:, 1] if scores.shape[1] > 1 else scores[:, 0],
            index=df.index,
        )
        self._fitted = True
        logger.info(
            f"VIX PCA fitted on {len(df)} observations. "
            f"Explained variance: {self._pca.explained_variance_ratio_.round(3)}"
        )
        return True

    # ------------------------------------------------------------------
    # Signal extraction
    # ------------------------------------------------------------------

    def get_signal(self, current_strip: list) -> dict:
        """Project today's strip into PC space and return signal dict.

        Always returns a dict; check 'is_ready' before using scores.
        """
        n_obs = self.n_observations()
        stub = {
            'is_ready':     False,
            'n_obs':        n_obs,
            'min_obs':      MIN_FIT_DAYS,
            'level':        None,
            'slope':        None,
            'curvature':    None,
            'slope_pct':    None,
            'level_pct':    None,
            'explained_variance': None,
            'loadings':     None,
        }
        if not _SKLEARN or not current_strip:
            return stub

        if not self._fitted:
            if not self.fit():
                stub['n_obs'] = n_obs
                return stub

        # Build current vector, pad to match training width
        levels = [c['level'] for c in current_strip[:N_CONTRACTS]]
        while len(levels) < self._history.shape[1]:
            # Extrapolate last value for missing far contracts
            levels.append(levels[-1])
        levels = levels[:self._history.shape[1]]

        if any(v is None for v in levels):
            return stub

        x = np.array(levels, dtype=float).reshape(1, -1)
        x_scaled = self._scaler.transform(x)
        scores = self._pca.transform(x_scaled)[0]

        level    = float(scores[0])
        slope    = float(scores[1]) if len(scores) > 1 else 0.0
        curve    = float(scores[2]) if len(scores) > 2 else 0.0

        # Percentile rank within history
        def _pct(series: pd.Series, val: float) -> float:
            s = series.dropna()
            return float(round((s < val).mean() * 100, 1)) if len(s) > 1 else 50.0

        slope_pct = _pct(self._slope_history, slope)
        level_pct = _pct(self._slope_history.rename('level').reset_index(drop=True).rename(
            pd.Series(self._pca.transform(
                self._scaler.transform(self._history.values)
            )[:, 0], index=self._history.index)
        ), level) if self._history is not None else 50.0

        # Loadings table: which maturity each PC loads on
        cols = [f'M{i+1}' for i in range(self._pca.components_.shape[1])]
        loadings = pd.DataFrame(
            self._pca.components_[:min(3, len(scores))],
            index=[f'PC{i+1}' for i in range(min(3, len(scores)))],
            columns=cols,
        )

        return {
            'is_ready':           True,
            'n_obs':              n_obs,
            'min_obs':            MIN_FIT_DAYS,
            'level':              round(level, 3),
            'slope':              round(slope, 3),
            'curvature':          round(curve, 3),
            'slope_pct':          slope_pct,
            'level_pct':          _pct(
                pd.Series(self._pca.transform(
                    self._scaler.transform(self._history.values)
                )[:, 0], index=self._history.index),
                level,
            ),
            'explained_variance': self._pca.explained_variance_ratio_.tolist(),
            'loadings':           loadings,
            'slope_history':      self._slope_history,
        }

    # ------------------------------------------------------------------
    # Convenience — invalidate cache after new data stored
    # ------------------------------------------------------------------

    def invalidate(self):
        """Force re-fit on next get_signal() call."""
        self._fitted = False
        self._pca    = None
        self._scaler = None
        self._history = None
        self._slope_history = None
