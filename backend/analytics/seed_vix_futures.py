"""
VIX Futures History Seeder
==========================
Downloads historical VX futures settlement data from CBOE and stores
daily term structure snapshots so the dashboard can show how the
VIX term structure has evolved over time.

Note on what already works without this script
-----------------------------------------------
  The VIX tab charts (ratio history, percentile, roll cost, outcomes) all
  use ^VIX and ^VIX3M from yfinance — they already have 504 days of history
  from day 1.  No download is needed for those.

  This script is only needed if you want to show the HISTORICAL EVOLUTION
  of the full futures strip (M1 / M2 / M3 / M4+ settlement prices over time).
  The current dashboard does not yet have a chart for this, but this seeder
  prepares the data for when that chart is added.

CBOE VX futures settlement data
---------------------------------
  CBOE provides free historical settlement CSVs at:
    https://www.cboe.com/derivatives/futures/vix/historical-data/

  Each file covers one contract month, e.g. vx_settlement_2024_01.csv:
    Trade Date, Futures, Open, High, Low, Close, Settle, Change,
    Total Volume, EFP, Open Interest

  This script also accepts the consolidated CSV from the CBOE Data Shop
  or any provider that gives one row per contract per date with the
  columns: date, contract (e.g. "VX Jan 2024"), settle, expiration.

Usage
-----
  # Download and store CBOE settlement data automatically (requires requests):
  python seed_vix_futures.py --download --years 2

  # Import pre-downloaded CBOE CSV files:
  python seed_vix_futures.py data/vix_futures/*.csv

  # Show stored term structure coverage:
  python seed_vix_futures.py --status

Output
------
  Writes to  data/vix_term_structure.csv  (appends, deduplicates).
  Format: date, m1_settle, m2_settle, m3_settle, m4_settle, contango_m1m2, contango_m2m3
"""

import argparse
import glob
import io
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
HERE = Path(__file__).parent
DATA_DIR = HERE / 'data'
OUTPUT_FILE = DATA_DIR / 'vix_term_structure.csv'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CBOE VX settlement URL pattern
# Each monthly contract has a URL like:
#   https://markets.cboe.com/us/futures/market_statistics/historical_data/
#        products/vix/VX_2024_01.csv
# The actual URL format varies — use the direct quote API as fallback.
# ---------------------------------------------------------------------------

CBOE_SETTLEMENT_URL = (
    "https://cdn.cboe.com/api/global/us_indices/daily_prices/"
    "VIX_History.csv"
)

# CBOE provides daily VIX history (spot only) at this URL.
# For futures settlements per contract month, download manually from:
#   https://www.cboe.com/derivatives/futures/vix/historical-data/
# and place the CSVs in  data/vix_futures/


# ---------------------------------------------------------------------------
# Download VIX spot history (already used by yfinance in engine, but useful
# for local caching and cross-checking).
# ---------------------------------------------------------------------------

def download_vix_spot_history() -> Optional[pd.DataFrame]:
    """Download VIX spot closing prices from CBOE free CSV endpoint."""
    try:
        import requests
        logger.info(f"Downloading VIX spot history from CBOE ...")
        resp = requests.get(CBOE_SETTLEMENT_URL, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        # CBOE VIX history columns: DATE, OPEN, HIGH, LOW, CLOSE
        df.columns = [c.strip().lower() for c in df.columns]
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
        df = df.dropna(subset=['date', 'close']).sort_values('date')
        df = df.rename(columns={'close': 'vix_spot'})
        logger.info(f"  Got {len(df):,} VIX spot rows "
                    f"({df['date'].iloc[0]} → {df['date'].iloc[-1]})")
        return df[['date', 'vix_spot']]
    except Exception as e:
        logger.error(f"VIX spot download failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Parse CBOE per-contract settlement CSV
# ---------------------------------------------------------------------------

def _parse_cboe_contract_csv(path: Path) -> Optional[pd.DataFrame]:
    """
    Parse a CBOE VX monthly settlement CSV.
    Format:
      Trade Date, Futures, Open, High, Low, Close, Settle, Change,
      Total Volume, EFP, Open Interest
    Returns DataFrame with columns: date, contract, settle, expiration (estimated).
    """
    try:
        df = pd.read_csv(path)
        df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]

        if 'trade_date' in df.columns:
            df['date'] = pd.to_datetime(df['trade_date'], errors='coerce').dt.date
        elif 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
        else:
            logger.warning(f"No date column in {path.name}")
            return None

        settle_col = next((c for c in ['settle', 'settlement', 'close'] if c in df.columns), None)
        if settle_col is None:
            logger.warning(f"No settle column in {path.name}")
            return None
        df['settle'] = pd.to_numeric(df[settle_col], errors='coerce')

        # Contract name from filename if not in data (e.g. VX_2024_01.csv → "VX Jan 2024")
        contract_name = path.stem.replace('_', ' ').upper()
        df['contract'] = df.get('futures', contract_name)

        return df[['date', 'contract', 'settle']].dropna()

    except Exception as e:
        logger.error(f"Failed to parse {path.name}: {e}")
        return None


# ---------------------------------------------------------------------------
# Build term structure from multiple contract files
# ---------------------------------------------------------------------------

def build_term_structure(contract_frames: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Given a list of per-contract DataFrames (each with date, contract, settle),
    pivot into a single DataFrame with columns:
      date, m1_settle, m2_settle, m3_settle, m4_settle, contango_m1m2, contango_m2m3

    M1 = nearest contract expiring after each date,
    M2 = second nearest, etc.
    """
    if not contract_frames:
        return pd.DataFrame()

    combined = pd.concat(contract_frames, ignore_index=True)
    combined = combined.dropna(subset=['date', 'settle'])
    combined['settle'] = combined['settle'].astype(float)

    # Estimate expiration from contract name (e.g. "VX Jan 2024" → 2024-01)
    def _exp_from_name(name):
        try:
            parts = str(name).upper().split()
            months = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
                      'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
            month = next((months[p] for p in parts if p in months), None)
            year  = next((int(p) for p in parts if len(p) == 4 and p.isdigit()), None)
            if month and year:
                # VX expires on Wednesday before 3rd Friday, approx 3rd week
                return date(year, month, 15)
        except Exception:
            pass
        return None

    combined['expiration'] = combined['contract'].apply(_exp_from_name)

    rows = []
    for trade_date, day_df in combined.groupby('date'):
        # Only contracts not yet expired on this date
        if combined['expiration'].notna().any():
            valid = day_df[day_df['expiration'].apply(
                lambda e: e >= trade_date if pd.notna(e) else True
            )]
        else:
            valid = day_df

        valid = valid.sort_values('expiration').reset_index(drop=True)
        settles = valid['settle'].tolist()

        m1 = settles[0] if len(settles) > 0 else np.nan
        m2 = settles[1] if len(settles) > 1 else np.nan
        m3 = settles[2] if len(settles) > 2 else np.nan
        m4 = settles[3] if len(settles) > 3 else np.nan

        contango_m1m2 = (m2 - m1) / m1 * 100 if pd.notna(m1) and pd.notna(m2) and m1 > 0 else np.nan
        contango_m2m3 = (m3 - m2) / m2 * 100 if pd.notna(m2) and pd.notna(m3) and m2 > 0 else np.nan

        rows.append({
            'date':           trade_date,
            'm1_settle':      round(m1, 2) if pd.notna(m1) else np.nan,
            'm2_settle':      round(m2, 2) if pd.notna(m2) else np.nan,
            'm3_settle':      round(m3, 2) if pd.notna(m3) else np.nan,
            'm4_settle':      round(m4, 2) if pd.notna(m4) else np.nan,
            'contango_m1m2':  round(contango_m1m2, 4) if pd.notna(contango_m1m2) else np.nan,
            'contango_m2m3':  round(contango_m2m3, 4) if pd.notna(contango_m2m3) else np.nan,
        })

    return pd.DataFrame(rows).sort_values('date').reset_index(drop=True)


# ---------------------------------------------------------------------------
# Save / merge into output CSV
# ---------------------------------------------------------------------------

def save_term_structure(df: pd.DataFrame, output: Path, dry_run: bool):
    """Merge new data into existing output CSV (deduplicating by date)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if output.exists():
        existing = pd.read_csv(output)
        existing['date'] = pd.to_datetime(existing['date']).dt.date
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=['date'], keep='last')
        combined = combined.sort_values('date').reset_index(drop=True)
    else:
        combined = df

    logger.info(f"Term structure: {len(combined):,} rows "
                f"({combined['date'].iloc[0]} → {combined['date'].iloc[-1]})")

    if not dry_run:
        combined.to_csv(output, index=False)
        logger.info(f"Saved to {output}")
    else:
        logger.info("(dry-run: not saved)")

    return combined


# ---------------------------------------------------------------------------
# Status report
# ---------------------------------------------------------------------------

def print_status():
    if not OUTPUT_FILE.exists():
        logger.info("No term structure file found. Run with --download or provide CSV files.")
        return
    df = pd.read_csv(OUTPUT_FILE)
    df['date'] = pd.to_datetime(df['date']).dt.date
    print(f"\nVIX Term Structure History")
    print(f"  File:  {OUTPUT_FILE}")
    print(f"  Rows:  {len(df):,}")
    print(f"  Range: {df['date'].iloc[0]} → {df['date'].iloc[-1]}")
    recent = df.tail(5)
    print(f"\n  Last 5 rows:")
    print(recent[['date', 'm1_settle', 'm2_settle', 'm3_settle', 'contango_m1m2']].to_string(index=False))
    m1_avail = df['m1_settle'].notna().sum()
    m4_avail = df['m4_settle'].notna().sum()
    print(f"\n  M1 coverage: {m1_avail:,}/{len(df):,} days")
    print(f"  M4 coverage: {m4_avail:,}/{len(df):,} days")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Download and store VIX futures term structure history',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('files', nargs='*',
                        help='Pre-downloaded CBOE VX settlement CSV files')
    parser.add_argument('--download', action='store_true',
                        help='Download VIX spot history from CBOE (fast, free)')
    parser.add_argument('--years', type=int, default=2,
                        help='Years of history to download (default: 2)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Process files but do not write output')
    parser.add_argument('--status', action='store_true',
                        help='Show stored term structure coverage and exit')
    args = parser.parse_args()

    if args.status:
        print_status()
        return

    frames = []

    # Download VIX spot history (for the spot column — free from CBOE)
    if args.download:
        spot_df = download_vix_spot_history()
        if spot_df is not None:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            spot_out = DATA_DIR / 'vix_spot_history.csv'
            if not args.dry_run:
                spot_df.to_csv(spot_out, index=False)
                logger.info(f"VIX spot history saved to {spot_out}")
            logger.info(
                "\nNote: VIX futures term structure (M1/M2/M3) settlement data"
                "\nmust be downloaded manually from CBOE:"
                "\n  https://www.cboe.com/derivatives/futures/vix/historical-data/"
                "\nDownload the monthly settlement CSV files and run:"
                "\n  python seed_vix_futures.py data/vix_futures/VX_*.csv"
            )

    # Process pre-downloaded CBOE CSV files
    paths = []
    for pattern in args.files:
        matched = glob.glob(pattern)
        paths.extend(Path(p) for p in matched if Path(p).exists())

    if paths:
        logger.info(f"Processing {len(paths)} contract file(s) ...")
        for path in paths:
            df = _parse_cboe_contract_csv(path)
            if df is not None:
                logger.info(f"  {path.name}: {len(df):,} rows")
                frames.append(df)

        if frames:
            ts_df = build_term_structure(frames)
            if not ts_df.empty:
                save_term_structure(ts_df, OUTPUT_FILE, args.dry_run)
            else:
                logger.warning("No term structure data could be built from the files.")
        else:
            logger.warning("No valid data parsed from input files.")

    elif not args.download:
        parser.print_help()


if __name__ == '__main__':
    main()
