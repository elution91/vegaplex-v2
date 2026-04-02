"""
Polygon.io Skew History Seeder
==============================
Bulk-imports 2 years of historical skew metrics for the full US options
universe into skew_history.db using Polygon.io's Options Starter plan
($29/mo, 2-year history, unlimited API calls + flat file downloads).

What gets seeded
----------------
  iv_history table (fast path, always):
    • Daily ATM IV per ticker — unlocks per-ticker vol percentile and
      regime classification from day one instead of after 6 weeks

  skew_snapshots table (--full-skew flag):
    • call/put slope (IV vs moneyness) per expiry
    • 25-delta skew  (key arb signal — how rich are puts vs calls)
    • smile curvature (convexity premium)
    • term steepness  (short vs long dated skew — calendar signals)
    • forward skew    (near/far — term structure arb signal)
    These power the Skew Dynamics tab, √T synthesis, and all skew
    history charts.

Why this matters
----------------
  Without history: "current put skew is 0.45" — no context, no signal.
  With 2 years: "put skew is at the 91st percentile vs its own history,
    regime is Jumpy Vol, √T normalised slope is rising across tenors" —
    that's a sell-near-puts / buy-far-puts calendar signal.

Flat file mode (recommended)
-----------------------------
  1. polygon.io → Data → Flat Files → Options → Daily Snapshots
  2. Download 2024-01-01 → today
  3. Place all files in  data/polygon_flat/
  4. python seed_polygon.py --flat-files data/polygon_flat/ --universe --full-skew

API mode (today's IV only — for ongoing use after cancelling subscription)
---------------------------------------------------------------------------
  python seed_polygon.py --api-key YOUR_KEY --universe
"""

import argparse
import json
import logging
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from skew_history import SkewHistory

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

UNIVERSE_FILE = HERE / 'universe_config.json'


# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------

def load_universe() -> List[str]:
    with open(UNIVERSE_FILE) as f:
        cfg = json.load(f)
    active = cfg.get('_active_groups', [])
    seen, out = set(), []
    for g in active:
        for t in cfg.get(g, []):
            if t not in seen:
                seen.add(t); out.append(t)
    return out


# ---------------------------------------------------------------------------
# Column name normaliser  (Polygon column names vary by snapshot version)
# ---------------------------------------------------------------------------

_COL = {
    'ticker':  ['underlying_ticker', 'ticker', 'symbol', 'root', 'underlying'],
    'strike':  ['strike_price', 'strike'],
    'right':   ['contract_type', 'type', 'right', 'put_call'],
    'expiry':  ['expiration_date', 'expiry', 'expiration'],
    'iv':      ['implied_volatility', 'iv', 'impliedvolatility', 'mid_iv'],
    'delta':   ['delta', 'greeks_delta', 'day_delta'],
    'spot':    ['underlying_price', 'day_underlying_price', 'underlying_close',
                'spot', 'undprice'],
}

def _nc(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise column names."""
    low = {c.lower().replace('.', '_').replace(' ', '_'): c for c in df.columns}
    rename = {}
    for canonical, aliases in _COL.items():
        for a in aliases:
            if a in low and canonical not in rename.values():
                rename[low[a]] = canonical
                break
    return df.rename(columns=rename)


# ---------------------------------------------------------------------------
# Core skew metrics  (all signals that power the dashboard)
# ---------------------------------------------------------------------------

def compute_skew_metrics(chain: pd.DataFrame, spot: float,
                         file_date: date) -> Optional[Dict]:
    """
    Compute full skew metrics from one ticker's options chain for one date.

    Input DataFrame columns (after normalisation):
      strike, right (C/P), expiry, iv, delta (optional), spot

    Returns a dict ready to INSERT into skew_snapshots, or None on failure.

    Metrics computed
    ----------------
    call_slope / put_slope
        Linear regression of IV vs moneyness (K/S) per side, averaged across
        front expiries.  The core skew steepness signal.

    call_skew_25 / put_skew_25
        IV(25Δ) − IV(ATM).  The canonical skew richness measure.
        If delta column present: uses actual 25Δ contract.
        Fallback: 90% moneyness strike as proxy for 25Δ put.

    call_curvature / put_curvature
        Second-order coefficient from quadratic fit of IV smile.
        Measures smile convexity — high curvature → wings rich vs belly.

    term_steepness
        Slope of skew_slope vs √T across expiries.
        Rising → near-dated skew steeper than far (typical risk-off).
        Falling → far-dated skew steeper (term arb opportunity).

    fwd_skew_near / fwd_skew_far
        Implied forward skew extracted from the term structure of ATM vols
        and slopes.  Near = between expiry-1 and expiry-2.
        Far = between expiry-2 and expiry-3.
        The key calendar / diagonal trade signal.

    by_expiry
        JSON array with per-expiry: {expiry, tte, slope, atm_vol,
        skew_25, curvature} — drives the skew dynamics charts.
    """
    if chain.empty or spot <= 0:
        return None

    # ── Prep ────────────────────────────────────────────────────────────────
    c = chain.copy()
    c['strike'] = pd.to_numeric(c.get('strike', pd.Series()), errors='coerce')
    c['iv']     = pd.to_numeric(c.get('iv',     pd.Series()), errors='coerce')
    c = c.dropna(subset=['strike', 'iv'])
    c = c[c['iv'].between(0.001, 5.0) & (c['strike'] > 0)]

    has_delta = 'delta' in c.columns
    if has_delta:
        c['delta'] = pd.to_numeric(c['delta'], errors='coerce')

    has_expiry = 'expiry' in c.columns
    if has_expiry:
        c['expiry_dt'] = pd.to_datetime(c['expiry'], errors='coerce')
        c = c.dropna(subset=['expiry_dt'])
        c['tte'] = (c['expiry_dt'] - pd.Timestamp(file_date)).dt.days / 365.0
        c = c[c['tte'] >= 5/365]   # drop expiry-day contracts

    c['moneyness'] = c['strike'] / spot

    calls = c[c['right'].str.upper().str[0] == 'C']
    puts  = c[c['right'].str.upper().str[0] == 'P']

    # ── Per-expiry metrics ──────────────────────────────────────────────────
    def _expiry_metrics(side: pd.DataFrame) -> List[Dict]:
        if side.empty or not has_expiry:
            return []
        rows = []
        for exp, grp in side.groupby('expiry_dt'):
            if len(grp) < 3:
                continue
            tte = float(grp['tte'].iloc[0])
            x = grp['moneyness'].values
            y = grp['iv'].values

            # ATM IV
            atm_idx = (grp['strike'] - spot).abs().idxmin()
            atm_vol = float(grp.loc[atm_idx, 'iv'])

            # Slope  (linear)
            try:
                slope = float(np.polyfit(x, y, 1)[0])
            except Exception:
                slope = None

            # Curvature  (quadratic second derivative)
            try:
                coeffs = np.polyfit(x, y, 2)
                curvature = float(2 * coeffs[0])
            except Exception:
                curvature = None

            # 25-delta skew
            skew_25 = None
            if has_delta and 'delta' in grp.columns:
                d = grp['delta'].abs()
                near25 = grp[(d - 0.25).abs() < 0.10]
                if not near25.empty:
                    idx25 = (near25['delta'].abs() - 0.25).abs().idxmin()
                    iv25  = float(near25.loc[idx25, 'iv'])
                    skew_25 = round(iv25 - atm_vol, 4)
            if skew_25 is None:
                # Proxy: 90% moneyness for puts, 110% for calls
                proxy_m = 0.90 if side['right'].iloc[0].upper()[0] == 'P' else 1.10
                wing = grp[(grp['moneyness'] - proxy_m).abs() < 0.05]
                if not wing.empty:
                    skew_25 = round(float(wing['iv'].mean()) - atm_vol, 4)

            rows.append({
                'expiry': str(exp.date()),
                'tte':    round(tte, 4),
                'slope':  round(slope, 6) if slope is not None else None,
                'atm_vol': round(atm_vol, 4),
                'skew_25': skew_25,
                'curvature': round(curvature, 6) if curvature is not None else None,
            })
        return sorted(rows, key=lambda r: r['tte'])

    call_by_exp = _expiry_metrics(calls)
    put_by_exp  = _expiry_metrics(puts)
    by_expiry   = [{'side': 'call', **r} for r in call_by_exp] + \
                  [{'side': 'put',  **r} for r in put_by_exp]

    if not call_by_exp and not put_by_exp:
        return None

    # ── Aggregate across front 3 expiries ──────────────────────────────────
    def _agg(exp_list):
        front = exp_list[:3]
        if not front:
            return {}
        slopes = [e['slope'] for e in front if e['slope'] is not None]
        atms   = [e['atm_vol'] for e in front]
        sk25s  = [e['skew_25'] for e in front if e['skew_25'] is not None]
        curvs  = [e['curvature'] for e in front if e['curvature'] is not None]
        return {
            'avg_slope':     float(np.mean(slopes))  if slopes else None,
            'avg_atm_vol':   float(np.mean(atms)),
            'avg_skew_25':   float(np.mean(sk25s))   if sk25s  else None,
            'avg_curvature': float(np.mean(curvs))   if curvs  else None,
        }

    c_agg = _agg(call_by_exp)
    p_agg = _agg(put_by_exp)

    # ── Term steepness  (how slope changes with √T) ─────────────────────────
    term_steepness = None
    all_exp = call_by_exp or put_by_exp
    if len(all_exp) >= 2:
        sqrt_t = np.array([np.sqrt(e['tte']) for e in all_exp if e['slope'] is not None])
        slopes = np.array([e['slope']         for e in all_exp if e['slope'] is not None])
        if len(sqrt_t) >= 2:
            try:
                term_steepness = float(np.polyfit(sqrt_t, slopes, 1)[0])
            except Exception:
                pass

    # ── Forward skew  (Bennett pp.210-225) ─────────────────────────────────
    # fwd_skew_near = implied skew for the period BETWEEN exp1 and exp2
    # Approximation: (slope_2 * √T_2 - slope_1 * √T_1) / (√T_2 - √T_1)
    fwd_skew_near = fwd_skew_far = None
    ref = [e for e in all_exp if e['slope'] is not None and e['tte'] > 0]
    if len(ref) >= 2:
        try:
            t1, s1 = ref[0]['tte'], ref[0]['slope']
            t2, s2 = ref[1]['tte'], ref[1]['slope']
            denom = np.sqrt(t2) - np.sqrt(t1)
            if abs(denom) > 1e-6:
                fwd_skew_near = (s2 * np.sqrt(t2) - s1 * np.sqrt(t1)) / denom
        except Exception:
            pass
    if len(ref) >= 3:
        try:
            t2, s2 = ref[1]['tte'], ref[1]['slope']
            t3, s3 = ref[2]['tte'], ref[2]['slope']
            denom = np.sqrt(t3) - np.sqrt(t2)
            if abs(denom) > 1e-6:
                fwd_skew_far = (s3 * np.sqrt(t3) - s2 * np.sqrt(t2)) / denom
        except Exception:
            pass

    # ── ATM IV for iv_history ───────────────────────────────────────────────
    atm_iv = None
    front_ivs = []
    for exp_list in [call_by_exp, put_by_exp]:
        if exp_list:
            front_ivs.append(exp_list[0]['atm_vol'])
    if front_ivs:
        atm_iv = float(np.mean(front_ivs))

    return {
        # iv_history
        'atm_iv': round(atm_iv, 4) if atm_iv else None,
        # skew_snapshots
        'call_slope':      _safe(c_agg.get('avg_slope')),
        'put_slope':       _safe(p_agg.get('avg_slope')),
        'call_atm_vol':    _safe(c_agg.get('avg_atm_vol')),
        'put_atm_vol':     _safe(p_agg.get('avg_atm_vol')),
        'call_skew_25':    _safe(c_agg.get('avg_skew_25')),
        'put_skew_25':     _safe(p_agg.get('avg_skew_25')),
        'call_curvature':  _safe(c_agg.get('avg_curvature')),
        'put_curvature':   _safe(p_agg.get('avg_curvature')),
        'term_steepness':  _safe(term_steepness),
        'fwd_skew_near':   _safe(fwd_skew_near),
        'fwd_skew_far':    _safe(fwd_skew_far),
        'by_expiry':       json.dumps(by_expiry),
        'spot_price':      round(spot, 4),
    }

def _safe(v):
    if v is None or (isinstance(v, float) and not np.isfinite(v)):
        return None
    return round(float(v), 6)


# ---------------------------------------------------------------------------
# Database writers
# ---------------------------------------------------------------------------

def _write_iv(store: SkewHistory, symbol: str, dt: str, atm_iv: float, source: str):
    store.store_iv_series(symbol, [dt], [atm_iv], source=source)


def _write_skew_snapshot(store: SkewHistory, symbol: str, dt: str, metrics: Dict):
    """Direct INSERT into skew_snapshots (bypasses record() format requirements)."""
    row = {
        'symbol':         symbol,
        'ts':             dt,
        'by_expiry':      metrics.get('by_expiry', '[]'),
        'call_slope':     metrics.get('call_slope'),
        'put_slope':      metrics.get('put_slope'),
        'call_atm_vol':   metrics.get('call_atm_vol'),
        'put_atm_vol':    metrics.get('put_atm_vol'),
        'call_skew_25':   metrics.get('call_skew_25'),
        'put_skew_25':    metrics.get('put_skew_25'),
        'call_curvature': metrics.get('call_curvature'),
        'put_curvature':  metrics.get('put_curvature'),
        'term_steepness': metrics.get('term_steepness'),
        'fwd_skew_near':  metrics.get('fwd_skew_near'),
        'fwd_skew_far':   metrics.get('fwd_skew_far'),
        'spot_price':     metrics.get('spot_price'),
    }
    cols = ', '.join(row.keys())
    ph   = ', '.join(['?'] * len(row))
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(f"INSERT INTO skew_snapshots ({cols}) VALUES ({ph})",
                     list(row.values()))


# ---------------------------------------------------------------------------
# Flat file loader
# ---------------------------------------------------------------------------

def _load_flat_file(path: Path) -> Optional[pd.DataFrame]:
    try:
        if path.suffix == '.parquet':
            return pd.read_parquet(path)
        elif path.name.endswith('.csv.gz'):
            return pd.read_csv(path, compression='gzip', low_memory=False)
        else:
            return pd.read_csv(path, low_memory=False)
    except Exception as e:
        logger.warning(f"Could not load {path.name}: {e}")
        return None


def _date_from_filename(path: Path) -> Optional[date]:
    stem = path.stem.replace('.csv', '')
    for part in stem.replace('options_', '').split('_'):
        try:
            return date.fromisoformat(part[:10])
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Mode 1: Flat file processing
# ---------------------------------------------------------------------------

def process_flat_files(
    flat_dir: Path,
    tickers: Optional[List[str]],
    start: Optional[date],
    end: Optional[date],
    full_skew: bool,
    dry_run: bool,
    store: SkewHistory,
) -> Dict:
    stats = {'iv_stored': 0, 'skew_stored': 0, 'skipped': 0, 'errors': 0}

    files = sorted(
        list(flat_dir.glob('options_*.parquet')) +
        list(flat_dir.glob('*.parquet')) +
        list(flat_dir.glob('options_*.csv.gz')) +
        list(flat_dir.glob('*.csv.gz')) +
        list(flat_dir.glob('options_*.csv'))
    )
    if not files:
        logger.error(f"No flat files found in {flat_dir}")
        return stats

    logger.info(f"Found {len(files)} flat files | full_skew={full_skew}")
    ticker_set = set(tickers) if tickers else None

    for i, path in enumerate(files, 1):
        file_date = _date_from_filename(path)
        if file_date is None:
            logger.warning(f"Cannot parse date from {path.name}")
            continue
        if start and file_date < start: continue
        if end   and file_date > end:   continue

        logger.info(f"[{i}/{len(files)}] {path.name}")

        df = _load_flat_file(path)
        if df is None:
            stats['errors'] += 1
            continue

        df = _nc(df)

        sym_col = 'ticker' if 'ticker' in df.columns else None
        if sym_col is None:
            logger.warning(f"  No ticker column in {path.name}")
            stats['errors'] += 1
            continue

        # Filter to requested tickers
        if ticker_set:
            df = df[df[sym_col].isin(ticker_set)]
        if df.empty:
            continue

        # Ensure numeric
        for col in ['strike', 'iv', 'spot', 'delta']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        dt_str = str(file_date)
        n_syms = df[sym_col].nunique()

        for sym, grp in df.groupby(sym_col):
            sym = str(sym)
            spot_vals = grp['spot'].dropna() if 'spot' in grp.columns else pd.Series()
            spot = float(spot_vals.iloc[0]) if not spot_vals.empty else 0.0

            if spot <= 0:
                stats['skipped'] += 1
                continue

            if full_skew:
                metrics = compute_skew_metrics(grp, spot, file_date)
                if metrics is None:
                    stats['skipped'] += 1
                    continue
                if not dry_run:
                    if metrics.get('atm_iv'):
                        _write_iv(store, sym, dt_str, metrics['atm_iv'], 'polygon_flat')
                    _write_skew_snapshot(store, sym, dt_str, metrics)
                stats['iv_stored']   += 1
                stats['skew_stored'] += 1
            else:
                # ATM IV only (fast path)
                atm_iv = _atm_iv_fast(grp, spot)
                if atm_iv is None:
                    stats['skipped'] += 1
                    continue
                if not dry_run:
                    _write_iv(store, sym, dt_str, atm_iv, 'polygon_flat')
                stats['iv_stored'] += 1

        if i % 10 == 0:
            logger.info(f"  stored iv={stats['iv_stored']:,} skew={stats['skew_stored']:,} "
                        f"skipped={stats['skipped']:,}")

    return stats


def _atm_iv_fast(grp: pd.DataFrame, spot: float) -> Optional[float]:
    """Quick ATM IV without full skew computation."""
    if 'iv' not in grp.columns or 'strike' not in grp.columns:
        return None
    ivs = []
    right_col = 'right' if 'right' in grp.columns else None
    for ct_val in (['C', 'P'] if right_col else [None]):
        side = grp[grp[right_col].str.upper().str[0] == ct_val] if ct_val else grp
        if side.empty:
            continue
        idx = (side['strike'] - spot).abs().idxmin()
        iv = float(side.loc[idx, 'iv'])
        if 0.001 < iv < 5.0:
            ivs.append(iv)
    return round(float(np.mean(ivs)), 4) if ivs else None


# ---------------------------------------------------------------------------
# Mode 2: Polygon REST API  (current day, ongoing use)
# ---------------------------------------------------------------------------

class PolygonClient:
    BASE = "https://api.polygon.io"

    def __init__(self, api_key: str):
        try:
            import requests as req
            self._s = req.Session()
            self._s.params = {'apiKey': api_key}  # type: ignore
        except ImportError:
            raise RuntimeError("pip install requests")

    def get(self, path: str, **params) -> dict:
        r = self._s.get(self.BASE + path, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def options_snapshot(self, ticker: str) -> List[dict]:
        results, url = [], f"/v3/snapshot/options/{ticker}"
        params = {'limit': 250}
        while url:
            data = self.get(url if url.startswith('/') else url.replace(self.BASE, ''),
                            **params)
            results.extend(data.get('results', []))
            url = (data.get('next_url', '') or '').replace(self.BASE, '') or None
            params = {}
            if len(results) >= 2000:
                break
        return results

    def daily_close(self, ticker: str, dt: str) -> Optional[float]:
        try:
            d = self.get(f"/v2/aggs/ticker/{ticker}/range/1/day/{dt}/{dt}",
                         adjusted='true', limit=1)
            r = d.get('results', [])
            return float(r[0]['c']) if r else None
        except Exception:
            return None


def _snapshot_to_chain(contracts: List[dict], spot: float,
                       as_of_date: str) -> pd.DataFrame:
    """Convert Polygon snapshot API response into normalised chain DataFrame."""
    rows = []
    for c in contracts:
        d = c.get('details', {})
        g = c.get('greeks', {})
        rows.append({
            'strike':  d.get('strike_price'),
            'right':   d.get('contract_type', '')[0].upper() if d.get('contract_type') else None,
            'expiry':  d.get('expiration_date'),
            'iv':      c.get('implied_volatility') or c.get('iv'),
            'delta':   g.get('delta'),
            'spot':    spot,
        })
    return pd.DataFrame(rows)


def process_api(
    client: PolygonClient,
    tickers: List[str],
    as_of_date: str,
    full_skew: bool,
    dry_run: bool,
    store: SkewHistory,
    workers: int = 8,
) -> Dict:
    stats = {'iv_stored': 0, 'skew_stored': 0, 'skipped': 0, 'errors': 0}
    file_date = date.fromisoformat(as_of_date)

    def _one(ticker):
        try:
            if not dry_run and store.latest_iv_date(ticker) == as_of_date:
                return ticker, None, 'skipped'
            spot = client.daily_close(ticker, as_of_date)
            if not spot or spot <= 0:
                return ticker, None, 'no_spot'
            contracts = client.options_snapshot(ticker)
            if not contracts:
                return ticker, None, 'no_data'
            chain = _snapshot_to_chain(contracts, spot, as_of_date)
            if full_skew:
                metrics = compute_skew_metrics(chain, spot, file_date)
                return ticker, metrics, 'ok'
            else:
                atm_iv = _atm_iv_fast(chain, spot)
                return ticker, {'atm_iv': atm_iv} if atm_iv else None, 'ok' if atm_iv else 'no_iv'
        except Exception as e:
            logger.debug(f"{ticker}: {e}")
            return ticker, None, 'error'

    logger.info(f"API mode | {len(tickers)} tickers | {as_of_date} | full_skew={full_skew}")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_one, t): t for t in tickers}
        for i, future in enumerate(as_completed(futures), 1):
            ticker, result, status = future.result()
            if status == 'skipped':
                stats['skipped'] += 1
            elif result:
                if not dry_run:
                    if result.get('atm_iv'):
                        _write_iv(store, ticker, as_of_date, result['atm_iv'], 'polygon_api')
                        stats['iv_stored'] += 1
                    if full_skew and result.get('call_slope') is not None:
                        _write_skew_snapshot(store, ticker, as_of_date, result)
                        stats['skew_stored'] += 1
            else:
                stats['skipped'] += 1
            if i % 50 == 0:
                logger.info(f"  {i}/{len(tickers)} iv={stats['iv_stored']} "
                            f"skew={stats['skew_stored']}")
            time.sleep(0.05)

    return stats


# ---------------------------------------------------------------------------
# Coverage report
# ---------------------------------------------------------------------------

def print_coverage(store: SkewHistory, tickers: Optional[List[str]] = None):
    conn = sqlite3.connect(store.db_path)
    iv_rows = conn.execute(
        "SELECT symbol, COUNT(*) n, MIN(date) first, MAX(date) last "
        "FROM iv_history GROUP BY symbol ORDER BY n DESC"
    ).fetchall()
    skew_counts = dict(conn.execute(
        "SELECT symbol, COUNT(*) FROM skew_snapshots GROUP BY symbol"
    ).fetchall())
    conn.close()

    ticker_set = set(tickers) if tickers else None
    if ticker_set:
        iv_rows = [r for r in iv_rows if r[0] in ticker_set]

    print(f"\n{'Symbol':<12} {'IV obs':>6}  {'First':>12}  {'Last':>12}  "
          f"{'Skew snaps':>10}  {'Status':>8}")
    print('-' * 70)
    for sym, n, first, last in iv_rows[:60]:
        sk = skew_counts.get(sym, 0)
        status = '✓ ready' if n >= 30 else f'{n}/30'
        print(f"{sym:<12} {n:>6}  {first:>12}  {last:>12}  {sk:>10}  {status:>8}")
    if len(iv_rows) > 60:
        print(f"  ... and {len(iv_rows)-60} more")

    ready  = sum(1 for _, n, _, _ in iv_rows if n >= 30)
    has_sk = sum(1 for r in iv_rows if skew_counts.get(r[0], 0) > 0)
    print(f"\n{ready}/{len(iv_rows)} symbols have 30+ IV obs (regime active)")
    print(f"{has_sk}/{len(iv_rows)} symbols have full skew history (dynamics active)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description='Polygon.io bulk seeder — iv_history + skew_snapshots',
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)

    src = p.add_mutually_exclusive_group()
    src.add_argument('--flat-files', metavar='DIR',
                     help='Directory of Polygon daily flat files (.parquet / .csv.gz)')
    src.add_argument('--api-key', metavar='KEY',
                     help='Polygon API key (today\'s data only)')

    p.add_argument('--full-skew', action='store_true',
                   help='Compute and store slope, skew25, curvature, fwd skew (recommended)')
    p.add_argument('--universe', action='store_true',
                   help='Use all tickers from universe_config.json')
    p.add_argument('--tickers', nargs='+', default=None)
    p.add_argument('--tickers-file', metavar='FILE')
    p.add_argument('--start', default=None, help='YYYY-MM-DD (flat file mode)')
    p.add_argument('--end',   default=None, help='YYYY-MM-DD (flat file mode)')
    p.add_argument('--workers', type=int, default=8)
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--status', action='store_true', help='Show coverage and exit')
    args = p.parse_args()

    store = SkewHistory()

    tickers = list(args.tickers or [])
    if args.universe:
        tickers = load_universe()
        logger.info(f"Universe: {len(tickers)} tickers")
    if args.tickers_file:
        with open(args.tickers_file) as f:
            tickers += [l.strip().upper() for l in f if l.strip()]
    tickers = sorted(set(tickers)) or None

    if args.status:
        print_coverage(store, tickers)
        return

    start = date.fromisoformat(args.start) if args.start else date.today() - timedelta(days=730)
    end_d = date.fromisoformat(args.end)   if args.end   else date.today() - timedelta(days=1)

    if args.flat_files:
        flat_dir = Path(args.flat_files)
        if not flat_dir.exists():
            logger.error(f"Directory not found: {flat_dir}"); sys.exit(1)
        stats = process_flat_files(flat_dir, tickers, start, end_d,
                                   args.full_skew, args.dry_run, store)

    elif args.api_key:
        if not tickers:
            logger.error("API mode requires --tickers or --universe"); sys.exit(1)
        client = PolygonClient(args.api_key)
        stats = process_api(client, tickers, str(end_d),
                            args.full_skew, args.dry_run, store, args.workers)
    else:
        p.print_help()
        print("\nExample (recommended weekend workflow):")
        print("  python seed_polygon.py --flat-files data/polygon_flat/ --universe --full-skew")
        return

    logger.info(f"\nDone. iv={stats.get('iv_stored',0):,}  "
                f"skew={stats.get('skew_stored',0):,}  "
                f"skipped={stats.get('skipped',0):,}  "
                f"errors={stats.get('errors',0)}")
    if not args.dry_run and stats.get('iv_stored', 0) > 0:
        print_coverage(store, tickers)


if __name__ == '__main__':
    main()
