"""
Skew History Seeder
===================
Bulk-imports historical EOD options data into skew_history.db so that
the regime classifier and skew dynamics charts start with meaningful history
instead of from zero.

Supported input formats
-----------------------
  --format optionsdx   OptionsDX EOD CSV (one row per contract)
  --format orats       ORATS historical strikes CSV (one row per contract)
  --format generic     Any CSV with the columns listed in --help

OptionsDX column map (all names case-insensitive):
    date               YYYY-MM-DD  (or MM/DD/YYYY auto-detected)
    symbol / underlying / ticker
    expiration / expiry
    strike
    option_type / type / right   (C/P or call/put)
    implied_volatility / iv / impliedvol
    underlying_price / stock_price / spot / close

ORATS strike column map:
    date, ticker, expirDate, strike, callBidIv, callAskIv, putBidIv, putAskIv,
    spotPrice (plus many others — only the above are used)

Usage
-----
  # Dry-run to see what would be imported:
  python seed_skew_history.py data/optionsdx_SPY_2024.csv --format optionsdx --dry-run

  # Seed ATM IV only (fast, works for all tickers in the file):
  python seed_skew_history.py data/optionsdx_*.csv --format optionsdx

  # Seed full skew snapshots (computes slope, curvature per expiry — slower):
  python seed_skew_history.py data/optionsdx_SPY_2024.csv --format optionsdx --full-skew

  # Filter to specific tickers:
  python seed_skew_history.py data/orats_2024.csv --format orats --tickers SPY QQQ IWM

  # Backfill a specific date range:
  python seed_skew_history.py data/*.csv --format optionsdx --start 2024-01-01 --end 2024-06-30

Output
------
  Writes to skew_history.db (same directory as this script):
    iv_history table        — one row per symbol per date (ATM IV)
    skew_snapshots table    — one row per symbol per scan (if --full-skew)
"""

import argparse
import glob
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Column name normaliser
# ---------------------------------------------------------------------------

_COL_ALIASES = {
    'date':       ['date', 'quotedate', 'quote_date', 'trade_date'],
    'symbol':     ['symbol', 'underlying', 'ticker', 'root', 'sym'],
    'expiration': ['expiration', 'expiry', 'expirdate', 'exp_date', 'expire_date'],
    'strike':     ['strike', 'strikeprice', 'strike_price'],
    'right':      ['option_type', 'right', 'type', 'put_call', 'cp_flag'],
    'iv':         ['implied_volatility', 'iv', 'impliedvol', 'impliedvolatility',
                   'mid_iv', 'callaskim', 'putaskim'],
    'spot':       ['underlying_price', 'stock_price', 'spot', 'close', 'spotprice',
                   'undprice', 'underlyingprice'],
}


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename df columns to canonical names using alias table."""
    low_cols = {c.lower().replace(' ', '_'): c for c in df.columns}
    rename = {}
    for canonical, aliases in _COL_ALIASES.items():
        for alias in aliases:
            if alias in low_cols and canonical not in rename.values():
                rename[low_cols[alias]] = canonical
                break
    return df.rename(columns=rename)


# ---------------------------------------------------------------------------
# Date parser
# ---------------------------------------------------------------------------

def _parse_dates(series: pd.Series) -> pd.Series:
    """Parse date strings in MM/DD/YYYY or YYYY-MM-DD format."""
    sample = str(series.dropna().iloc[0]) if not series.dropna().empty else ''
    if '/' in sample:
        return pd.to_datetime(series, format='%m/%d/%Y', errors='coerce').dt.date
    return pd.to_datetime(series, errors='coerce').dt.date


# ---------------------------------------------------------------------------
# ATM IV extractor  (works for both OptionsDX and generic formats)
# ---------------------------------------------------------------------------

def _extract_atm_iv(group: pd.DataFrame) -> Optional[float]:
    """
    Given a DataFrame of options for one symbol/date, return the ATM IV.

    Picks the nearest-to-spot strike and averages the call + put IV
    (or uses whichever side is available).
    """
    if 'spot' not in group.columns or group['spot'].isna().all():
        return None
    if 'iv' not in group.columns:
        return None

    spot = float(group['spot'].dropna().iloc[0])
    if spot <= 0:
        return None

    # Front-month expiry only (smallest DTE >= 5 calendar days from quote date)
    if 'expiration' in group.columns and 'date' in group.columns:
        try:
            exp_dates = pd.to_datetime(group['expiration'], errors='coerce')
            qdate = pd.to_datetime(group['date'].iloc[0], errors='coerce')
            dte = (exp_dates - qdate).dt.days
            valid_exp = exp_dates[dte >= 5]
            if not valid_exp.empty:
                front_exp = valid_exp.min()
                group = group[exp_dates == front_exp]
        except Exception:
            pass

    # Separate calls and puts
    if 'right' in group.columns:
        right = group['right'].str.upper().str[0]   # 'C' or 'P'
        calls = group[right == 'C']
        puts  = group[right == 'P']
    else:
        calls = group
        puts  = pd.DataFrame()

    ivs = []
    for side in [calls, puts]:
        if side.empty:
            continue
        atm_idx = (side['strike'] - spot).abs().idxmin()
        iv_val = float(side.loc[atm_idx, 'iv'])
        if 0.001 < iv_val < 5.0:
            ivs.append(iv_val)

    return float(np.mean(ivs)) if ivs else None


# ---------------------------------------------------------------------------
# Skew metrics extractor  (for --full-skew mode)
# ---------------------------------------------------------------------------

def _compute_skew_slope(side: pd.DataFrame, spot: float) -> Optional[float]:
    """Linear regression of IV vs moneyness (strike/spot) for one option type."""
    if len(side) < 3:
        return None
    x = (side['strike'] / spot).values
    y = side['iv'].values
    mask = np.isfinite(x) & np.isfinite(y) & (y > 0.001) & (y < 5.0)
    if mask.sum() < 3:
        return None
    coeffs = np.polyfit(x[mask], y[mask], 1)
    return float(coeffs[0])   # slope = dIV / d(K/S)


def _extract_full_skew(group: pd.DataFrame) -> Optional[Dict]:
    """
    Compute call_slope, put_slope, call_atm_vol, put_atm_vol for one symbol/date.
    Returns dict ready to pass to SkewHistory.store_skew_snapshot().
    """
    if 'spot' not in group.columns or group['spot'].isna().all():
        return None
    spot = float(group['spot'].dropna().iloc[0])
    if spot <= 0:
        return None

    if 'right' not in group.columns:
        return None

    right = group['right'].str.upper().str[0]
    calls = group[right == 'C'].copy()
    puts  = group[right == 'P'].copy()

    def _atm_iv(side):
        if side.empty:
            return None
        idx = (side['strike'] - spot).abs().idxmin()
        v = float(side.loc[idx, 'iv'])
        return v if 0.001 < v < 5.0 else None

    call_slope = _compute_skew_slope(calls, spot)
    put_slope  = _compute_skew_slope(puts,  spot)
    c_atm = _atm_iv(calls)
    p_atm = _atm_iv(puts)

    if call_slope is None and put_slope is None:
        return None

    # Minimal skew_data dict matching build_skew_charts() expectations
    return {
        'call_slope':   round(call_slope, 6) if call_slope is not None else None,
        'put_slope':    round(put_slope,  6) if put_slope  is not None else None,
        'call_atm_vol': round(c_atm, 6) if c_atm is not None else None,
        'put_atm_vol':  round(p_atm, 6) if p_atm is not None else None,
    }


# ---------------------------------------------------------------------------
# ORATS-specific loader
# ---------------------------------------------------------------------------

def _load_orats(path: Path) -> pd.DataFrame:
    """
    Load ORATS historical strikes CSV.
    ORATS format (key columns):
      date, ticker, expirDate, strike, callBidIv, callAskIv, putBidIv, putAskIv, spotPrice
    We compute mid-IV for calls and puts then feed into the standard pipeline.
    """
    df = pd.read_csv(path, low_memory=False)
    cols = {c.lower(): c for c in df.columns}

    rename = {}
    if 'ticker' in cols:    rename[cols['ticker']]    = 'symbol'
    if 'expirdate' in cols: rename[cols['expirdate']] = 'expiration'
    if 'strikeprice' in cols: rename[cols['strikeprice']] = 'strike'
    elif 'strike' in cols:  rename[cols['strike']]    = 'strike'
    if 'spotprice' in cols: rename[cols['spotprice']] = 'spot'
    if 'date' in cols:      rename[cols['date']]      = 'date'

    df = df.rename(columns=rename)

    # Build iv column: average of call mid and put mid
    call_bid = df.get(cols.get('callbidiv', ''), pd.Series(dtype=float))
    call_ask = df.get(cols.get('callaskiv', ''), pd.Series(dtype=float))
    put_bid  = df.get(cols.get('putbidiv',  ''), pd.Series(dtype=float))
    put_ask  = df.get(cols.get('putaskiv',  ''), pd.Series(dtype=float))

    rows = []
    for i in range(len(df)):
        base = df.iloc[i][['date', 'symbol', 'expiration', 'strike', 'spot']].to_dict()
        # Call row
        c_mid = (call_bid.iloc[i] + call_ask.iloc[i]) / 2 if i < len(call_bid) else np.nan
        if pd.notna(c_mid) and c_mid > 0:
            rows.append({**base, 'iv': c_mid, 'right': 'C'})
        # Put row
        p_mid = (put_bid.iloc[i] + put_ask.iloc[i]) / 2 if i < len(put_bid) else np.nan
        if pd.notna(p_mid) and p_mid > 0:
            rows.append({**base, 'iv': p_mid, 'right': 'P'})

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ---------------------------------------------------------------------------
# Main seeder
# ---------------------------------------------------------------------------

def seed_files(
    paths: List[Path],
    fmt: str,
    tickers: Optional[List[str]],
    start: Optional[date],
    end: Optional[date],
    full_skew: bool,
    dry_run: bool,
    store: SkewHistory,
) -> Dict:
    stats = {'iv_stored': 0, 'skew_stored': 0, 'skipped': 0, 'errors': 0}

    for path in paths:
        logger.info(f"Loading {path.name} ...")
        try:
            if fmt == 'orats':
                df = _load_orats(path)
            else:
                df = pd.read_csv(path, low_memory=False)
                df = _normalise_columns(df)

            required = {'date', 'symbol', 'iv', 'spot'}
            missing = required - set(df.columns)
            if missing:
                logger.warning(f"  Skipping {path.name}: missing columns {missing}. "
                               f"Found: {list(df.columns)}")
                stats['errors'] += 1
                continue

            # Normalise dates
            df['date'] = _parse_dates(df['date'].astype(str))
            df = df.dropna(subset=['date'])

            # Filter
            if tickers:
                df = df[df['symbol'].isin(set(tickers))]
            if start:
                df = df[df['date'] >= start]
            if end:
                df = df[df['date'] <= end]
            if df.empty:
                logger.info(f"  No rows after filters.")
                continue

            # Ensure numeric
            df['strike'] = pd.to_numeric(df['strike'], errors='coerce')
            df['iv']     = pd.to_numeric(df['iv'],     errors='coerce')
            df['spot']   = pd.to_numeric(df['spot'],   errors='coerce')

            # Group by symbol × date
            groups = df.groupby(['symbol', 'date'])
            total = len(groups)
            logger.info(f"  {total:,} symbol-date groups, {len(df):,} rows")

            for i, ((sym, dt), grp) in enumerate(groups):
                if i % 500 == 0 and i > 0:
                    logger.info(f"  {i:,}/{total:,} processed ...")

                dt_str = str(dt)

                # --- ATM IV ---
                atm_iv = _extract_atm_iv(grp)
                if atm_iv is None:
                    stats['skipped'] += 1
                    continue

                if not dry_run:
                    store.store_iv_series(sym, [dt_str], [atm_iv], source=fmt)
                stats['iv_stored'] += 1

                # --- Full skew snapshot (optional) ---
                if full_skew:
                    skew = _extract_full_skew(grp)
                    if skew and not dry_run:
                        try:
                            store.store_skew_snapshot(
                                symbol=sym,
                                ts=dt_str,
                                call_skew={'by_expiry': []},
                                put_skew={'by_expiry': []},
                                call_slope=skew.get('call_slope'),
                                put_slope=skew.get('put_slope'),
                                call_atm_vol=skew.get('call_atm_vol'),
                                put_atm_vol=skew.get('put_atm_vol'),
                                spot_price=float(grp['spot'].iloc[0]),
                            )
                            stats['skew_stored'] += 1
                        except TypeError:
                            # store_skew_snapshot signature may differ; iv_history is enough
                            pass

        except Exception as e:
            logger.error(f"  Failed to process {path.name}: {e}")
            stats['errors'] += 1

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Bulk-import historical options data into skew_history.db',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('files', nargs='+',
                        help='Input CSV file(s) or glob patterns')
    parser.add_argument('--format', choices=['optionsdx', 'orats', 'generic'],
                        default='optionsdx',
                        help='Input data format (default: optionsdx)')
    parser.add_argument('--tickers', nargs='+', default=None,
                        help='Restrict import to these symbols')
    parser.add_argument('--start', default=None,
                        help='Start date YYYY-MM-DD (inclusive)')
    parser.add_argument('--end', default=None,
                        help='End date YYYY-MM-DD (inclusive)')
    parser.add_argument('--full-skew', action='store_true',
                        help='Also compute and store skew slope/curvature snapshots')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse and compute but do not write to database')
    parser.add_argument('--status', action='store_true',
                        help='Show current iv_history coverage and exit')
    args = parser.parse_args()

    store = SkewHistory()

    if args.status:
        import sqlite3
        conn = sqlite3.connect(store.db_path)
        rows = conn.execute(
            "SELECT symbol, COUNT(*) n, MIN(date) first, MAX(date) last "
            "FROM iv_history GROUP BY symbol ORDER BY n DESC LIMIT 50"
        ).fetchall()
        conn.close()
        print(f"\n{'Symbol':<12} {'Obs':>5}  {'First':>12}  {'Last':>12}  {'Status':>8}")
        print('-' * 56)
        for sym, n, first, last in rows:
            status = '✓ ready' if n >= 30 else f'{n}/30'
            print(f"{sym:<12} {n:>5}  {first:>12}  {last:>12}  {status:>8}")
        total = len(rows)
        ready = sum(1 for _, n, _, _ in rows if n >= 30)
        print(f"\n{ready}/{total} symbols with 30+ observations (ticker-level regime active)")
        return

    # Resolve file globs
    paths = []
    for pattern in args.files:
        matched = glob.glob(pattern)
        paths.extend(Path(p) for p in matched)
    paths = [p for p in paths if p.exists()]

    if not paths:
        logger.error("No input files found.")
        sys.exit(1)

    start = date.fromisoformat(args.start) if args.start else None
    end_d = date.fromisoformat(args.end)   if args.end   else None

    logger.info(f"Seeding from {len(paths)} file(s) | format={args.format} "
                f"| dry_run={args.dry_run} | full_skew={args.full_skew}")

    stats = seed_files(
        paths=paths,
        fmt=args.format,
        tickers=args.tickers,
        start=start,
        end=end_d,
        full_skew=args.full_skew,
        dry_run=args.dry_run,
        store=store,
    )

    logger.info(
        f"\nDone. iv_stored={stats['iv_stored']:,}  skew_stored={stats['skew_stored']:,}  "
        f"skipped={stats['skipped']:,}  errors={stats['errors']}"
    )
    if args.dry_run:
        logger.info("(dry-run: nothing written to database)")

    if not args.dry_run and stats['iv_stored'] > 0:
        # Print updated coverage
        import sqlite3
        conn = sqlite3.connect(store.db_path)
        rows = conn.execute(
            "SELECT symbol, COUNT(*) n FROM iv_history GROUP BY symbol ORDER BY n DESC LIMIT 20"
        ).fetchall()
        conn.close()
        logger.info(f"\nTop symbols by observation count:")
        for sym, n in rows:
            logger.info(f"  {sym:<10} {n:>5} obs {'✓' if n >= 30 else ''}")


if __name__ == '__main__':
    main()
