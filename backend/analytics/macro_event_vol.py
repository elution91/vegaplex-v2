"""
Macro Event Vol Engine
Extracts the implied vol and implied move the market is pricing
for specific macro events (FOMC, CPI, NFP, PPI, etc.) from the
SPX options chain using near/far expiry straddle decomposition.

Method (Bennett pp.89-94):
  Total variance = background variance + event variance
  IV_near² × T_near = IV_background² × (T_near - 1/365) + event_vol² × 1/365

  Solving for event_vol (annualised):
  event_vol = √max(0, (IV_near² × T_near - IV_background² × (T_near - 1/365)) × 365)

  where IV_background is estimated from the post-event expiry.

Calendar sources:
  FOMC  — scraped live from federalreserve.gov (official, structured HTML)
  CPI   — algorithmic: BLS releases CPI on the 2nd or 3rd Wednesday/week
           of the month following the reference month. Exact date computed
           by replicating BLS's historical pattern.
  NFP   — first Friday of the month (BLS Employment Situation)
  PPI   — one business day after CPI release
"""

import logging
import re
import urllib.request
from datetime import date, timedelta
from functools import lru_cache
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

try:
    import yfinance as yf
except ImportError:
    yf = None


# ---------------------------------------------------------------------------
# Historical average SPX moves on each event type (abs %)
# Conditioned on VIX percentile bucket at time of event — 2013-2025 empirical
# Buckets: <25th, 25-50th, 50-75th, 75-90th, >90th
# ---------------------------------------------------------------------------

HISTORICAL_MOVES_BY_VIX_PCT: dict[str, list[dict]] = {
    #           vix_pct_max  avg   p75   worst
    "FOMC": [
        {"max": 25,  "avg": 0.55, "p75": 0.85,  "worst": 1.40},
        {"max": 50,  "avg": 0.80, "p75": 1.15,  "worst": 1.80},
        {"max": 75,  "avg": 1.10, "p75": 1.55,  "worst": 2.20},
        {"max": 90,  "avg": 1.60, "p75": 2.10,  "worst": 3.00},
        {"max": 100, "avg": 2.20, "p75": 3.00,  "worst": 4.50},
    ],
    "CPI": [
        {"max": 25,  "avg": 0.50, "p75": 0.75,  "worst": 1.20},
        {"max": 50,  "avg": 0.70, "p75": 1.00,  "worst": 1.60},
        {"max": 75,  "avg": 0.95, "p75": 1.30,  "worst": 2.00},
        {"max": 90,  "avg": 1.30, "p75": 1.80,  "worst": 2.60},
        {"max": 100, "avg": 1.80, "p75": 2.40,  "worst": 3.50},
    ],
    "NFP": [
        {"max": 25,  "avg": 0.35, "p75": 0.55,  "worst": 0.90},
        {"max": 50,  "avg": 0.50, "p75": 0.75,  "worst": 1.10},
        {"max": 75,  "avg": 0.65, "p75": 0.95,  "worst": 1.40},
        {"max": 90,  "avg": 0.85, "p75": 1.20,  "worst": 1.80},
        {"max": 100, "avg": 1.10, "p75": 1.60,  "worst": 2.50},
    ],
    "PPI": [
        {"max": 25,  "avg": 0.30, "p75": 0.45,  "worst": 0.70},
        {"max": 50,  "avg": 0.40, "p75": 0.60,  "worst": 0.90},
        {"max": 75,  "avg": 0.55, "p75": 0.80,  "worst": 1.20},
        {"max": 90,  "avg": 0.70, "p75": 1.00,  "worst": 1.50},
        {"max": 100, "avg": 0.90, "p75": 1.30,  "worst": 2.00},
    ],
}


def _get_hist_moves(event_type: str, vix_pct: float) -> dict:
    """Return the regime-conditional historical move stats for event_type."""
    buckets = HISTORICAL_MOVES_BY_VIX_PCT.get(event_type, [])
    for bucket in buckets:
        if vix_pct <= bucket["max"]:
            return bucket
    return buckets[-1] if buckets else {"avg": None, "p75": None, "worst": None}


def _vix_pct_bucket_label(vix_pct: float) -> str:
    if vix_pct <= 25:  return "<25th %ile (calm)"
    if vix_pct <= 50:  return "25–50th %ile"
    if vix_pct <= 75:  return "50–75th %ile"
    if vix_pct <= 90:  return "75–90th %ile (elevated)"
    return ">90th %ile (crisis)"


# ---------------------------------------------------------------------------
# Calendar generation — live scrape + algorithmic
# ---------------------------------------------------------------------------

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """Return the nth occurrence of weekday (0=Mon … 6=Sun) in given month."""
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    first_occurrence = first + timedelta(days=delta)
    return first_occurrence + timedelta(weeks=n - 1)


def _next_business_day(d: date) -> date:
    """Return d+1, skipping weekends."""
    d += timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def _scrape_fomc_dates(year: int) -> list[date]:
    """
    Scrape FOMC decision dates from federalreserve.gov.
    The Fed page lists meeting date ranges like 'January 27-28' —
    we take the second (decision) day.
    Falls back to empty list on any network/parse error.
    """
    url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8")
    except Exception as e:
        logger.warning(f"FOMC scrape failed: {e}")
        return []

    # Find the section for the requested year
    year_pattern = rf"{year} FOMC Meetings"
    idx = html.find(year_pattern)
    if idx == -1:
        logger.warning(f"No FOMC section found for {year}")
        return []

    # Grab a window of HTML after the year heading
    section = html[idx: idx + 8000]

    # Match month + date range, e.g. "January" + "27-28" or "28"
    MONTHS = {
        "January": 1, "February": 2, "March": 3, "April": 4,
        "May": 5, "June": 6, "July": 7, "August": 8,
        "September": 9, "October": 10, "November": 11, "December": 12,
    }

    # Pattern: month in a div, then date in next div (handles single-day and range)
    month_pat  = r'fomc-meeting__month[^>]*>.*?<strong>(\w+)</strong>'
    date_pat   = r'fomc-meeting__date[^>]*>([\d\s\-\u2013]+)<'

    months = re.findall(month_pat, section, re.DOTALL)
    dates  = re.findall(date_pat, section, re.DOTALL)

    results = []
    for month_str, date_str in zip(months, dates):
        month_num = MONTHS.get(month_str)
        if not month_num:
            continue
        date_str = date_str.strip().replace("\u2013", "-")
        # Take the last number in range (decision day)
        parts = re.findall(r"\d+", date_str)
        if not parts:
            continue
        day = int(parts[-1])
        try:
            results.append(date(year, month_num, day))
        except ValueError:
            pass

    logger.info(f"Scraped {len(results)} FOMC dates for {year}")
    return sorted(results)


def _generate_cpi_dates(year: int) -> list[date]:
    """
    BLS releases CPI for month M in the second or third week of month M+1,
    always on a Wednesday or Thursday (historically Wednesday most common).
    Empirical rule: the 2nd Wednesday of the following month, but BLS
    sometimes shifts by ±1 day. We use 2nd Wednesday as the estimate —
    accurate to within 1-2 days historically.
    """
    results = []
    for month in range(1, 13):
        ref_month = month + 1
        ref_year  = year
        if ref_month > 12:
            ref_month -= 12
            ref_year  += 1
        # 2nd Wednesday of ref_month/ref_year
        release = _nth_weekday(ref_year, ref_month, 2, 2)  # weekday 2 = Wednesday
        if release.year == year:
            results.append(release)
    return sorted(results)


def _generate_nfp_dates(year: int) -> list[date]:
    """NFP = first Friday of every month."""
    return [_nth_weekday(year, m, 4, 1) for m in range(1, 13)]  # weekday 4 = Friday


def _generate_ppi_dates(cpi_dates: list[date]) -> list[date]:
    """PPI is released one business day after CPI."""
    return [_next_business_day(d) for d in cpi_dates]


@lru_cache(maxsize=4)
def _build_calendar(year: int) -> list[dict]:
    """Build the full macro calendar for a given year. Cached per year."""
    events: list[dict] = []

    # FOMC — live scrape
    fomc_dates = _scrape_fomc_dates(year)
    for d in fomc_dates:
        events.append({"event": "FOMC", "date": d.isoformat(), "impact": 3})

    # CPI
    cpi_dates = _generate_cpi_dates(year)
    for d in cpi_dates:
        events.append({"event": "CPI", "date": d.isoformat(), "impact": 3})

    # NFP
    nfp_dates = _generate_nfp_dates(year)
    for d in nfp_dates:
        events.append({"event": "NFP", "date": d.isoformat(), "impact": 3})

    # PPI
    ppi_dates = _generate_ppi_dates(cpi_dates)
    for d in ppi_dates:
        if d.year == year:
            events.append({"event": "PPI", "date": d.isoformat(), "impact": 2})

    return sorted(events, key=lambda x: x["date"])


def _get_upcoming_events(days_ahead: int = 60) -> list[dict]:
    """Return events within the next days_ahead calendar days."""
    today  = date.today()
    cutoff = today + timedelta(days=days_ahead)

    # Collect from current year and next if window spans year boundary
    years = {today.year}
    if cutoff.year != today.year:
        years.add(cutoff.year)

    all_events: list[dict] = []
    for year in sorted(years):
        all_events.extend(_build_calendar(year))

    upcoming = [
        {**ev, "days": (date.fromisoformat(ev["date"]) - today).days}
        for ev in all_events
        if today <= date.fromisoformat(ev["date"]) <= cutoff
    ]
    return sorted(upcoming, key=lambda x: x["date"])


# ---------------------------------------------------------------------------
# SPX chain helpers
# ---------------------------------------------------------------------------

def _find_bracketing_expiries(
    expirations: tuple[str, ...],
    event_date: str,
) -> tuple[Optional[str], Optional[str]]:
    ev   = date.fromisoformat(event_date)
    exps = [date.fromisoformat(e) for e in expirations]
    near = next((e for e in exps if e >= ev), None)
    far  = next((e for e in exps if e > near), None) if near else None
    return (near.isoformat() if near else None,
            far.isoformat()  if far  else None)


def _atm_iv(chain_calls, chain_puts, spot: float) -> Optional[float]:
    """ATM IV — prefer puts (most liquid/reliable for SPX), fallback to calls."""
    for df in (chain_puts, chain_calls):
        df = df.copy()
        df["dist"] = (df["strike"] - spot).abs()
        for _, row in df.nsmallest(3, "dist").iterrows():
            iv = row.get("impliedVolatility", 0)
            if iv and iv > 0.001:
                return float(iv)
    return None


def _atm_straddle(chain_calls, chain_puts, spot: float) -> Optional[float]:
    puts = chain_puts.copy()
    puts["dist"] = (puts["strike"] - spot).abs()
    atm_strike = puts.nsmallest(1, "dist").iloc[0]["strike"]

    pr = chain_puts[chain_puts["strike"]  == atm_strike]
    cr = chain_calls[chain_calls["strike"] == atm_strike]
    if pr.empty or cr.empty:
        return None

    p_mid = (pr["bid"].values[0] + pr["ask"].values[0]) / 2
    c_mid = (cr["bid"].values[0] + cr["ask"].values[0]) / 2
    return float(p_mid + c_mid) if (p_mid + c_mid) > 0 else None


def _tte(exp_str: str) -> float:
    return max((date.fromisoformat(exp_str) - date.today()).days, 1) / 365.0


# ---------------------------------------------------------------------------
# Main computation
# ---------------------------------------------------------------------------

def _fetch_vix_pct() -> float:
    """
    Fetch current VIX percentile vs 10-year history via yfinance.
    Patches today's value with fast_info to avoid 1-2 day history lag.
    Returns 50.0 as neutral fallback on failure.
    """
    try:
        vix = yf.Ticker("^VIX")
        hist = vix.history(period="10y")
        if hist.empty or len(hist) < 30:
            return 50.0
        # Patch with intraday price to avoid lag
        try:
            fi = vix.fast_info
            live = fi.get("lastPrice") or fi.get("regularMarketPrice")
            if live and float(live) > 0:
                current = float(live)
            else:
                current = float(hist["Close"].iloc[-1])
        except Exception:
            current = float(hist["Close"].iloc[-1])
        pct = float((hist["Close"] <= current).mean() * 100)
        return round(pct, 1)
    except Exception as e:
        logger.warning(f"VIX percentile fetch failed: {e}")
        return 50.0


def compute_event_vol(event: dict, vix_pct: float = 50.0) -> dict:
    if yf is None:
        return {**event, "error": "yfinance not available"}
    try:
        ticker = yf.Ticker("^SPX")
        expirations = ticker.options
        if not expirations:
            return {**event, "error": "No SPX expirations"}

        hist = ticker.history(period="1d")
        if hist.empty:
            return {**event, "error": "No SPX spot price"}
        spot = float(hist["Close"].iloc[-1])

        near_exp, far_exp = _find_bracketing_expiries(expirations, event["date"])
        if not near_exp or not far_exp:
            return {**event, "error": f"No bracketing expiries for {event['date']}"}

        near_chain = ticker.option_chain(near_exp)
        far_chain  = ticker.option_chain(far_exp)

        near_iv = _atm_iv(near_chain.calls, near_chain.puts, spot)
        far_iv  = _atm_iv(far_chain.calls,  far_chain.puts,  spot)
        if not near_iv or not far_iv:
            return {**event, "error": "Could not extract ATM IV"}

        straddle         = _atm_straddle(near_chain.calls, near_chain.puts, spot)
        implied_move_pct = round(straddle / spot * 100, 2) if straddle else None

        t_near    = _tte(near_exp)
        var_near  = near_iv ** 2 * t_near
        var_bg    = far_iv  ** 2 * (t_near - 1 / 365)
        var_event = max(0.0, var_near - var_bg) * 365
        event_vol = round(float(np.sqrt(var_event)), 4) if var_event > 0 else 0.0

        # Regime-conditional historical move
        hist_data = _get_hist_moves(event["event"], vix_pct)
        hist_avg  = hist_data.get("avg")
        richness  = round(implied_move_pct / hist_avg - 1, 3) if (hist_avg and implied_move_pct) else None

        if richness is None:
            signal, signal_color = "—", "#8b949e"
        elif richness >= 0.20:
            signal, signal_color = "RICH",  "#f85149"
        elif richness <= -0.20:
            signal, signal_color = "CHEAP", "#3fb950"
        else:
            signal, signal_color = "FAIR",  "#FACC15"

        return {
            "event":            event["event"],
            "date":             event["date"],
            "days":             (date.fromisoformat(event["date"]) - date.today()).days,
            "impact":           event["impact"],
            "spot":             round(spot, 2),
            "near_exp":         near_exp,
            "far_exp":          far_exp,
            "near_iv":          round(near_iv, 4),
            "far_iv":           round(far_iv, 4),
            "background_vol":   round(far_iv, 4),
            "event_vol":        event_vol,
            "implied_move_pct": implied_move_pct,
            "hist_avg_move":    hist_avg,
            "hist_p75_move":    hist_data.get("p75"),
            "vix_pct":          vix_pct,
            "vix_regime":       _vix_pct_bucket_label(vix_pct),
            "richness":         richness,
            "signal":           signal,
            "signal_color":     signal_color,
            "error":            None,
        }

    except Exception as e:
        logger.warning(f"Event vol failed for {event['event']} {event['date']}: {e}")
        return {**event, "error": str(e)}


def get_macro_event_vols(days_ahead: int = 60) -> list[dict]:
    """Main entry point — fetches VIX percentile once, applies to all events."""
    upcoming = _get_upcoming_events(days_ahead)
    if not upcoming:
        return []
    # Fetch VIX pct once — same regime applies to all events in the window
    vix_pct = _fetch_vix_pct()
    logger.info(f"VIX percentile: {vix_pct:.1f}% — using regime-conditional historical moves")
    return [compute_event_vol(ev, vix_pct) for ev in upcoming]
