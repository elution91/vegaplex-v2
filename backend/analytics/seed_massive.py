"""
Massive Flat Files Skew History Seeder
=======================================
Bulk-imports historical IV / skew metrics from Massive Flat Files
(us_options_opra/day_aggs_v1) into skew_history.db.

Massive day_aggs files contain OHLCV per option contract — but no IV,
no greeks. We compute IV ourselves via Black-Scholes inversion using:
  - Closing option price       (from Massive)
  - Underlying spot at close   (from yfinance, cached)
  - Strike + expiry + type     (parsed from the OPRA ticker)
  - Risk-free rate             (constant 4.5% — close enough for IV)

Workflow
--------
  python seed_massive.py --start 2024-04-01 --end 2026-04-25
    [--universe sp500_core,sector_etfs,vol_products,leveraged,macro]
    [--workers 4]
    [--dry-run]

Output
------
  iv_history       — daily ATM IV per symbol
  skew_snapshots   — daily 25Δ skew, slope, term structure per symbol

Requirements
------------
  pip install boto3 python-dotenv yfinance scipy pandas numpy
  backend/.env with MASSIVE_ACCESS_KEY / MASSIVE_SECRET_KEY
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import logging
import math
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import boto3
import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from scipy.stats import norm

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from skew_history import SkewHistory  # noqa: E402

load_dotenv(HERE.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("seed_massive")

# ── Config ────────────────────────────────────────────────────────────────────

ENDPOINT = "https://files.massive.com"
BUCKET   = "flatfiles"
PREFIX   = "us_options_opra/day_aggs_v1"

UNIVERSE_FILE = HERE / "universe_config.json"
RISK_FREE_RATE = 0.045
MIN_DTE = 7
MAX_DTE = 90
MAX_MONEYNESS = 0.25  # ±25% from spot

# ── Data classes ──────────────────────────────────────────────────────────────

class TickerParseError(ValueError): pass


def parse_opra(ticker: str) -> tuple[str, date, str, float]:
    """
    Parse OPRA ticker like 'O:SPY260415C00500000' into:
      (underlying, expiry, right, strike)

    Format: O:<ROOT><YYMMDD><C|P><STRIKE_8DIGITS>
    Strike is in 1/1000ths of a dollar.
    """
    if not ticker.startswith("O:"):
        raise TickerParseError(ticker)
    body = ticker[2:]
    # Strike is last 8 chars, right is char before that, expiry is 6 before that
    if len(body) < 15:
        raise TickerParseError(ticker)
    strike_raw = body[-8:]
    right = body[-9]
    expiry_raw = body[-15:-9]
    underlying = body[:-15]
    if right not in ("C", "P"):
        raise TickerParseError(ticker)
    try:
        expiry = datetime.strptime(expiry_raw, "%y%m%d").date()
        strike = int(strike_raw) / 1000.0
    except ValueError as e:
        raise TickerParseError(f"{ticker}: {e}") from e
    return underlying, expiry, right, strike


# ── Black-Scholes IV solver ───────────────────────────────────────────────────

def _bs_price(S: float, K: float, T: float, r: float, sigma: float, right: str) -> float:
    if T <= 0 or sigma <= 0:
        # intrinsic
        return max(S - K, 0.0) if right == "C" else max(K - S, 0.0)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if right == "C":
        return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def implied_vol(price: float, S: float, K: float, T: float, r: float, right: str) -> Optional[float]:
    """
    Scalar bisection IV solver. Kept for back-compat; per-day code uses
    `implied_vol_vec` for batch performance.
    """
    if price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return None
    intrinsic = max(S - K, 0.0) if right == "C" else max(K - S, 0.0)
    if price < intrinsic - 0.01:
        return None
    lo, hi = 0.01, 5.0
    for _ in range(60):
        mid = 0.5 * (lo + hi)
        try:
            p = _bs_price(S, K, T, r, mid, right)
        except (ValueError, OverflowError):
            return None
        if abs(p - price) < 0.005:
            return mid
        if p < price:
            lo = mid
        else:
            hi = mid
    return mid if 0.02 < mid < 4.9 else None


def implied_vol_vec(
    prices: np.ndarray, S: float, K: np.ndarray, T: np.ndarray,
    r: float, is_call: np.ndarray,
) -> np.ndarray:
    """
    Vectorised bisection IV solver. Operates on arrays of contracts that share
    the same underlying spot S. Returns array of IVs (NaN where no solution).
    Equivalent semantics to the scalar `implied_vol` but ~100x faster.
    """
    n = prices.shape[0]
    iv = np.full(n, np.nan)

    # Pre-validate
    valid = (prices > 0) & (K > 0) & (T > 0) & np.isfinite(prices) & np.isfinite(K) & np.isfinite(T)
    intrinsic = np.where(is_call, np.maximum(S - K, 0.0), np.maximum(K - S, 0.0))
    valid &= prices >= intrinsic - 0.01
    if not valid.any():
        return iv

    lo = np.full(n, 0.01)
    hi = np.full(n, 5.0)
    sqrtT = np.sqrt(np.where(T > 0, T, 1.0))
    discount = np.exp(-r * T)
    log_SK = np.log(S / np.where(K > 0, K, 1.0))

    for _ in range(60):
        mid = 0.5 * (lo + hi)
        d1 = (log_SK + (r + 0.5 * mid * mid) * T) / (mid * sqrtT)
        d2 = d1 - mid * sqrtT
        Nd1 = norm.cdf(d1); Nd2 = norm.cdf(d2)
        call_p = S * Nd1 - K * discount * Nd2
        put_p  = K * discount * (1 - Nd2) - S * (1 - Nd1)
        p = np.where(is_call, call_p, put_p)
        # Update bracket
        below = p < prices
        lo = np.where(below, mid, lo)
        hi = np.where(below, hi, mid)

    # Final mid is the answer
    final = 0.5 * (lo + hi)
    ok = valid & (final > 0.02) & (final < 4.9)
    iv[ok] = final[ok]
    return iv


# ── Universe loader ───────────────────────────────────────────────────────────

def load_universe(groups: Optional[list[str]] = None) -> list[str]:
    with open(UNIVERSE_FILE) as f:
        cfg = json.load(f)
    if not groups:
        groups = [k for k in cfg.keys() if not k.startswith("_") and isinstance(cfg[k], list)]
    seen, out = set(), []
    for g in groups:
        for t in cfg.get(g, []):
            if t not in seen:
                seen.add(t)
                out.append(t)
    return out


# ── Spot price cache ──────────────────────────────────────────────────────────

class SpotCache:
    """yfinance close-price cache, one fetch per symbol covering [start, end]."""

    def __init__(self):
        self._cache: dict[str, pd.Series] = {}

    def prefetch(self, symbols: list[str], start: date, end: date) -> None:
        log.info(f"Prefetching spot history for {len(symbols)} symbols…")
        # Fetch in chunks of 25
        for i in range(0, len(symbols), 25):
            batch = symbols[i:i+25]
            try:
                df = yf.download(batch, start=start, end=end + timedelta(days=2),
                                 auto_adjust=True, progress=False, threads=True)
                if df.empty:
                    continue
                closes = df["Close"] if isinstance(df.columns, pd.MultiIndex) else df
                if isinstance(closes, pd.DataFrame):
                    for sym in closes.columns:
                        self._cache[sym] = closes[sym].dropna()
                else:
                    self._cache[batch[0]] = closes.dropna()
            except Exception as e:
                log.warning(f"yfinance batch fetch failed: {e}")

    def get(self, symbol: str, on_date: date) -> Optional[float]:
        s = self._cache.get(symbol)
        if s is None or s.empty:
            return None
        idx = pd.Timestamp(on_date)
        # Use the row at or just before this date
        try:
            sub = s.loc[:idx]
            if sub.empty:
                return None
            return float(sub.iloc[-1])
        except KeyError:
            return None


# ── Massive S3 client ─────────────────────────────────────────────────────────

def s3_client():
    ak = os.getenv("MASSIVE_ACCESS_KEY")
    sk = os.getenv("MASSIVE_SECRET_KEY")
    if not ak or not sk:
        raise SystemExit("MASSIVE_ACCESS_KEY / MASSIVE_SECRET_KEY missing in .env")
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        region_name="us-east-1",
    )


def list_days(s3, start: date, end: date) -> list[tuple[date, str]]:
    """Return [(date, key), ...] for all day_aggs files in [start, end]."""
    out: list[tuple[date, str]] = []
    cur = date(start.year, start.month, 1)
    end_month = date(end.year, end.month, 1)
    while cur <= end_month:
        prefix = f"{PREFIX}/{cur.year}/{cur.month:02d}/"
        token = None
        while True:
            kw = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for obj in resp.get("Contents", []):
                key = obj["Key"]
                # key like us_options_opra/day_aggs_v1/2026/04/2026-04-15.csv.gz
                date_str = Path(key).stem.replace(".csv", "")
                try:
                    d = datetime.strptime(date_str, "%Y-%m-%d").date()
                    if start <= d <= end:
                        out.append((d, key))
                except ValueError:
                    continue
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        # advance month
        cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
    return sorted(out)


# ── Per-day processing ────────────────────────────────────────────────────────

def process_day(
    s3, key: str, day: date, universe: set[str], spot_cache: SpotCache,
) -> tuple[list[dict], list[dict]]:
    """
    Download + parse one day's options aggregates.
    Returns (iv_rows, skew_rows) ready for SkewHistory inserts.
    """
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    text = gzip.decompress(body).decode("utf-8", errors="replace")
    df = pd.read_csv(io.StringIO(text))

    # Parse OPRA tickers
    parsed = []
    for tk in df["ticker"].values:
        try:
            u, exp, r, k = parse_opra(tk)
            parsed.append((u, exp, r, k))
        except TickerParseError:
            parsed.append((None, None, None, None))
    df["underlying"] = [p[0] for p in parsed]
    df["expiry"]     = [p[1] for p in parsed]
    df["right"]      = [p[2] for p in parsed]
    df["strike"]     = [p[3] for p in parsed]

    # Filter to universe symbols
    df = df[df["underlying"].isin(universe)].copy()
    if df.empty:
        return [], []

    # Filter by DTE + moneyness + volume
    df = df.dropna(subset=["expiry", "strike", "right", "close"])
    df = df[df["close"] > 0]
    df = df[df["volume"] > 0]
    df["dte"] = df["expiry"].apply(lambda e: (e - day).days)
    df = df[(df["dte"] >= MIN_DTE) & (df["dte"] <= MAX_DTE)]

    iv_rows: list[dict] = []
    skew_rows: list[dict] = []

    for sym, group in df.groupby("underlying"):
        spot = spot_cache.get(sym, day)
        if spot is None or spot <= 0:
            continue

        # Moneyness filter
        group = group.assign(moneyness=group["strike"] / spot - 1.0)
        group = group[group["moneyness"].abs() <= MAX_MONEYNESS]
        if len(group) < 5:
            continue

        # Compute IV vectorised — entire symbol's contracts in one pass
        T_arr  = np.maximum(group["dte"].values.astype(float), 1.0) / 365.25
        K_arr  = group["strike"].values.astype(float)
        P_arr  = group["close"].values.astype(float)
        is_call = (group["right"].values == "C")
        ivs = implied_vol_vec(P_arr, spot, K_arr, T_arr, RISK_FREE_RATE, is_call)
        group = group.assign(iv=ivs)
        group = group.dropna(subset=["iv"])
        if len(group) < 5:
            continue

        # ── ATM IV: closest strike to spot, average call+put ──
        nearest = group.iloc[(group["strike"] - spot).abs().argsort()].head(4)
        atm_iv = float(nearest["iv"].mean())

        iv_rows.append({"symbol": sym, "date": day.isoformat(), "atm_iv": atm_iv})

        # ── Skew snapshot: per nearest expiry ──
        # Pick the expiry with most contracts, near 30 DTE if possible
        target_dte = 30
        exp_groups = group.groupby("expiry")
        best_exp = min(exp_groups.groups.keys(), key=lambda e: abs((e - day).days - target_dte))
        front = exp_groups.get_group(best_exp)
        if len(front) < 4:
            continue

        # Sort by strike, fit linear slope of IV vs log-moneyness
        front = front.sort_values("strike")
        log_m = np.log(front["strike"].values / spot)
        ivs_arr = front["iv"].values
        # Simple linear fit
        if len(set(log_m)) < 2:
            continue
        slope, intercept = np.polyfit(log_m, ivs_arr, 1)

        # Curvature: 2nd-order fit
        try:
            poly = np.polyfit(log_m, ivs_arr, 2)
            curvature = float(poly[0])
        except Exception:
            curvature = 0.0

        # 25Δ skew: difference between IV at -10% strike and IV at +10% strike
        def iv_at(target_logm: float) -> Optional[float]:
            return float(slope * target_logm + intercept)

        iv_otm_put  = iv_at(-0.10)
        iv_otm_call = iv_at(+0.10)
        skew_25d = iv_otm_put - iv_otm_call  # positive = put skew

        skew_rows.append({
            "symbol": sym,
            "date": day.isoformat(),
            "expiry": best_exp.isoformat(),
            "dte": int((best_exp - day).days),
            "atm_iv": atm_iv,
            "slope": float(slope),
            "curvature": curvature,
            "skew_25d": float(skew_25d),
            "n_strikes": len(front),
            "spot": float(spot),
        })

    return iv_rows, skew_rows


# ── Storage ───────────────────────────────────────────────────────────────────

def write_iv_rows(history: SkewHistory, rows: list[dict]) -> None:
    if not rows:
        return
    # Group by symbol and use store_iv_series
    by_sym: dict[str, list[tuple[str, float]]] = {}
    for r in rows:
        by_sym.setdefault(r["symbol"], []).append((r["date"], r["atm_iv"]))
    for sym, items in by_sym.items():
        dates  = [d for d, _ in items]
        ivs    = [v for _, v in items]
        history.store_iv_series(sym, dates, ivs, source="massive")


def write_skew_rows(history: SkewHistory, rows: list[dict]) -> None:
    if not rows:
        return
    # skew_history.SkewHistory stores via internal API; for now write directly
    import sqlite3
    # We use a separate light table: skew_seeded (custom) — or extend skew_snapshots
    with sqlite3.connect(history.db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS skew_history_daily (
                symbol     TEXT NOT NULL,
                date       TEXT NOT NULL,
                expiry     TEXT NOT NULL,
                dte        INTEGER,
                atm_iv     REAL,
                slope      REAL,
                curvature  REAL,
                skew_25d   REAL,
                n_strikes  INTEGER,
                spot       REAL,
                PRIMARY KEY (symbol, date, expiry)
            )
        """)
        conn.executemany("""
            INSERT OR REPLACE INTO skew_history_daily
            (symbol, date, expiry, dte, atm_iv, slope, curvature, skew_25d, n_strikes, spot)
            VALUES (:symbol, :date, :expiry, :dte, :atm_iv, :slope, :curvature, :skew_25d, :n_strikes, :spot)
        """, rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end",   required=True, help="YYYY-MM-DD")
    p.add_argument("--universe", default="",
                   help="Comma-separated group names (default: all groups in universe_config.json)")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--dry-run", action="store_true",
                   help="Process first 3 days only, print stats, no DB writes.")
    args = p.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end   = datetime.strptime(args.end,   "%Y-%m-%d").date()
    groups = [g.strip() for g in args.universe.split(",") if g.strip()] or None

    universe = load_universe(groups)
    log.info(f"Universe: {len(universe)} symbols ({', '.join(universe[:8])}…)")

    spot_cache = SpotCache()
    spot_cache.prefetch(universe, start, end)

    s3 = s3_client()
    days = list_days(s3, start, end)
    log.info(f"Found {len(days)} day_aggs files in [{start}, {end}]")

    if args.dry_run:
        days = days[:3]
        log.info(f"DRY-RUN: processing first {len(days)} days only")

    history = SkewHistory()
    universe_set = set(universe)

    n_iv = 0
    n_skew = 0
    failed = 0

    def _work(item):
        d, key = item
        try:
            return d, process_day(s3, key, d, universe_set, spot_cache)
        except Exception as e:
            log.warning(f"{d}: failed — {e}")
            return d, ([], [])

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(_work, item) for item in days]
        for i, fut in enumerate(as_completed(futures), 1):
            d, (iv_rows, skew_rows) = fut.result()
            if not iv_rows and not skew_rows:
                failed += 1
                log.warning(f"  [{i}/{len(days)}] {d}  no rows produced")
                continue
            if not args.dry_run:
                write_iv_rows(history, iv_rows)
                write_skew_rows(history, skew_rows)
            n_iv += len(iv_rows)
            n_skew += len(skew_rows)
            log.info(f"  [{i}/{len(days)}] {d}  +iv={len(iv_rows)} +skew={len(skew_rows)}  total iv={n_iv:,} skew={n_skew:,}")

    log.info(f"\nDone. iv_rows={n_iv:,}  skew_rows={n_skew:,}  failed_days={failed}")


if __name__ == "__main__":
    main()
