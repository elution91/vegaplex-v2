from __future__ import annotations

import asyncio
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout

from fastapi import APIRouter

from app.dependencies import get_scanner
from app.models.requests import RadarRequest

router = APIRouter()

_ATM_IV_TIMEOUT = 8   # seconds per symbol before giving up
_MAX_WORKERS    = 8   # parallel threads for IV fetches


def _fetch_atm_iv_single(scanner, sym: str) -> tuple[str, float | None]:
    """Fetch near-30d ATM IV for one symbol. Returns (sym, iv) or (sym, None)."""
    import logging  # noqa: PLC0415
    log = logging.getLogger("vegaplex.radar")
    try:
        opts = scanner.data_fetcher.get_options_chain(sym)
        if not opts:
            log.warning(f"_fetch_atm_iv_single({sym}): empty options chain")
            return sym, None
        spot = opts.get('current_price', 0)
        if spot <= 0:
            return sym, None

        exps  = opts.get('expirations', [])
        today = datetime.date.today()
        best_exp, best_dte = None, 9999
        for e in exps:
            try:
                d   = datetime.date.fromisoformat(str(e))
                dte = (d - today).days
                if dte > 0 and abs(dte - 30) < abs(best_dte - 30):
                    best_exp, best_dte = e, dte
            except Exception:
                pass
        if best_exp is None:
            return sym, None

        calls = opts.get('calls', {}).get(str(best_exp), [])
        puts  = opts.get('puts',  {}).get(str(best_exp), [])

        def _nearest(chain: list) -> float | None:
            if not chain:
                return None
            best = min(chain, key=lambda r: abs(r.get('strike', 0) - spot))
            iv   = best.get('impliedVolatility') or best.get('iv')
            return float(iv) if iv and float(iv) > 0.01 else None

        c_iv = _nearest(calls)
        p_iv = _nearest(puts)
        if c_iv and p_iv:
            return sym, (c_iv + p_iv) / 2
        if not c_iv and not p_iv:
            log.warning(f"_fetch_atm_iv_single({sym}): no IV in chain (exp={best_exp}, "
                        f"calls={len(calls)}, puts={len(puts)})")
        return sym, c_iv or p_iv
    except Exception as e:
        log.warning(f"_fetch_atm_iv_single({sym}): {type(e).__name__}: {e}")
        return sym, None


def _fetch_atm_ivs(scanner, symbols: list[str]) -> dict[str, float]:
    """
    Fetch the most recent ATM IV per symbol.

    Strategy:
      1. Read latest row from iv_history (Massive seed). If row date is today
         (or last trading day), use it.
      2. For symbols with no DB row OR a stale row, fetch live via yfinance
         and write the result back to iv_history (write-through cache).

    This means the DB keeps growing whether or not Massive is active.
    """
    from app.dependencies import get_skew_history  # noqa: PLC0415
    sh = get_skew_history()

    today      = datetime.date.today().isoformat()
    fresh: dict[str, float]   = {}
    stale: dict[str, str]     = {}    # symbol → last_seen_date

    # 1. Bulk-read latest IV + date per symbol from iv_history
    try:
        with sh._conn() as conn:
            placeholders = ",".join("?" * len(symbols))
            rows = conn.execute(
                f"""
                SELECT symbol, date, atm_iv
                FROM   iv_history
                WHERE  (symbol, date) IN (
                    SELECT symbol, MAX(date)
                    FROM   iv_history
                    WHERE  symbol IN ({placeholders})
                    GROUP  BY symbol
                )
                """,
                symbols,
            ).fetchall()
        for sym, dt, iv in rows:
            if iv and iv > 0:
                if dt == today:
                    fresh[sym] = float(iv)
                else:
                    stale[sym] = dt
    except Exception:
        pass

    # 2. Live fetch for symbols missing entirely OR with stale rows
    missing = [s for s in symbols if s not in fresh]
    if missing:
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
            futures = {pool.submit(_fetch_atm_iv_single, scanner, s): s for s in missing}
            for fut in as_completed(futures, timeout=_ATM_IV_TIMEOUT * 2):
                try:
                    sym, iv = fut.result(timeout=_ATM_IV_TIMEOUT)
                    if iv is not None:
                        fresh[sym] = iv
                except Exception:
                    pass

        # Write-through: persist live IVs to iv_history for tomorrow's percentiles
        new_rows = [(sym, today, fresh[sym], 'yfinance')
                    for sym in missing if sym in fresh]
        if new_rows:
            try:
                with sh._conn() as conn:
                    conn.executemany(
                        "INSERT OR REPLACE INTO iv_history "
                        "(symbol, date, atm_iv, source) VALUES (?, ?, ?, ?)",
                        new_rows,
                    )
            except Exception:
                pass  # cache miss isn't fatal; data still served from `fresh`

        # If live failed, keep yesterday's IV as a degraded fallback so
        # the row at least renders something plausible.
        for sym, dt in stale.items():
            if sym not in fresh:
                try:
                    with sh._conn() as conn:
                        row = conn.execute(
                            "SELECT atm_iv FROM iv_history WHERE symbol = ? AND date = ?",
                            (sym, dt),
                        ).fetchone()
                    if row and row[0]:
                        fresh[sym] = float(row[0])
                except Exception:
                    pass

    return fresh


def _build_radar_charts(regime_results: dict, atm_ivs: dict[str, float]) -> dict:
    """Build ECharts option dicts for the radar page."""
    symbols   = list(regime_results.keys())
    rv_vals   = [regime_results[s].get('realised_vol', 0) * 100 for s in symbols]
    vov_vals  = [regime_results[s].get('vol_of_vol', 0)          for s in symbols]
    corr_vals = [regime_results[s].get('primary_corr', 0)        for s in symbols]

    # IV/RV ratio per symbol (ATM IV annualised % ÷ RV %)
    iv_rv_ratios: list[float | None] = []
    for i, s in enumerate(symbols):
        atm = atm_ivs.get(s)
        rv  = rv_vals[i]
        if atm and rv > 0:
            iv_rv_ratios.append(round(atm * 100 / rv, 3))
        else:
            iv_rv_ratios.append(None)

    REGIME_COLORS = {
        'sticky_delta':     '#2ecc71',
        'sticky_strike':    '#2DD4BF',
        'sticky_local_vol': '#FACC15',
        'jumpy_vol':        '#f85149',
        'unknown':          '#8b949e',
    }

    def _regime_color(s: str) -> str:
        r = regime_results[s].get('regime', 'unknown')
        if hasattr(r, 'value'):
            r = r.value
        return REGIME_COLORS.get(str(r), '#8b949e')

    dot_colors = [_regime_color(s) for s in symbols]

    # ── Chart 1: Scatter — Realised Vol (x) vs Vol-of-Vol (y), colored by regime ──
    scatter_data = [
        {
            'value': [round(rv_vals[i], 2), round(vov_vals[i], 3)],
            'name':  symbols[i],
            'itemStyle': {'color': dot_colors[i]},
        }
        for i in range(len(symbols))
    ]
    scatter = {
        'backgroundColor': '#0d1117',
        'animation': False,
        'grid': {'left': 52, 'right': 16, 'top': 12, 'bottom': 40},
        'tooltip': {
            'trigger': 'item',
            'formatter': 'function(p){'
                         'var ratio = ' + str({s: iv_rv_ratios[i] for i, s in enumerate(symbols)}).replace("None", "null") + ';'
                         'var r = ratio[p.name];'
                         'var line = p.name + "<br/>RV: " + p.value[0].toFixed(1) + "% | VoV: " + p.value[1].toFixed(3);'
                         'if(r != null) line += "<br/>IV/RV: " + r.toFixed(2);'
                         'return line;'
                         '}',
            'backgroundColor': '#1f2937', 'borderColor': '#2d3748',
            'textStyle': {'color': '#e2e8f0', 'fontSize': 11},
        },
        'xAxis': {
            'type': 'value', 'name': 'Realised Vol (%)', 'nameLocation': 'middle', 'nameGap': 24,
            'nameTextStyle': {'color': '#8b949e', 'fontSize': 10},
            'axisLabel': {'color': '#8b949e', 'fontSize': 9},
            'axisLine': {'lineStyle': {'color': '#2d3748'}},
            'splitLine': {'lineStyle': {'color': '#1f2937', 'type': 'dashed'}},
        },
        'yAxis': {
            'type': 'value', 'name': 'Vol-of-Vol',
            'nameTextStyle': {'color': '#8b949e', 'fontSize': 10},
            'axisLabel': {'color': '#8b949e', 'fontSize': 9},
            'axisLine': {'lineStyle': {'color': '#2d3748'}},
            'splitLine': {'lineStyle': {'color': '#1f2937', 'type': 'dashed'}},
        },
        'series': [{
            'type': 'scatter', 'data': scatter_data, 'symbolSize': 8,
            'label': {'show': True, 'formatter': '{b}', 'position': 'top',
                      'color': '#8b949e', 'fontSize': 9},
        }],
    }

    # ── Chart 2: Bar — Realised Vol per symbol + IV/RV ratio as line overlay ──
    bar_data = [
        {'value': round(v, 1),
         'itemStyle': {'color': '#f85149' if v > 40 else '#FACC15' if v > 20 else '#3fb950'}}
        for v in rv_vals
    ]
    # IV/RV ratio series — scatter dots on secondary y-axis
    def _ratio_color(r: float | None) -> str:
        if r is None: return '#8b949e'
        if r >= 1.25: return '#3fb950'
        if r >= 1.0:  return '#e3b341'
        return '#f85149'

    ratio_data = [
        {'value': r, 'itemStyle': {'color': _ratio_color(r)}}
        if r is not None else {'value': None}
        for r in iv_rv_ratios
    ]

    iv_rv_chart = {
        'backgroundColor': '#0d1117',
        'animation': False,
        'grid': {'left': 52, 'right': 52, 'top': 28, 'bottom': 60},
        'legend': {
            'data': ['Realised Vol (%)', 'IV/RV Ratio'],
            'top': 4, 'left': 'center',
            'textStyle': {'color': '#8b949e', 'fontSize': 10},
            'itemHeight': 8,
        },
        'tooltip': {
            'trigger': 'axis',
            'formatter': 'function(params){'
                         'var out = params[0].axisValueLabel + "<br/>";'
                         'params.forEach(function(p){'
                         '  if(p.value == null) return;'
                         '  var dot = \'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:\' + p.color + \';margin-right:5px"></span>\';'
                         '  if(p.seriesIndex===0) out += dot + "RV: " + p.value.toFixed(1) + "%<br/>";'
                         '  else out += dot + "IV/RV: " + p.value.toFixed(2) + "<br/>";'
                         '});'
                         'return out;'
                         '}',
            'backgroundColor': '#1f2937', 'borderColor': '#2d3748',
            'textStyle': {'color': '#e2e8f0', 'fontSize': 11},
        },
        'xAxis': {
            'type': 'category', 'data': symbols,
            'axisLabel': {'color': '#8b949e', 'fontSize': 9, 'rotate': 35},
            'axisLine': {'lineStyle': {'color': '#2d3748'}},
        },
        'yAxis': [
            {
                'type': 'value', 'name': 'RV (%)',
                'nameTextStyle': {'color': '#8b949e', 'fontSize': 10},
                'axisLabel': {'color': '#8b949e', 'fontSize': 9},
                'axisLine': {'lineStyle': {'color': '#2d3748'}},
                'splitLine': {'lineStyle': {'color': '#1f2937', 'type': 'dashed'}},
            },
            {
                'type': 'value', 'name': 'IV/RV',
                'nameTextStyle': {'color': '#58a6ff', 'fontSize': 10},
                'axisLabel': {'color': '#58a6ff', 'fontSize': 9},
                'axisLine': {'lineStyle': {'color': '#2d3748'}},
                'splitLine': {'show': False},
                'min': 0,
            },
        ],
        'series': [
            {
                'type': 'bar',
                'name': 'Realised Vol (%)',
                'data': bar_data,
                'barMaxWidth': 32,
                'yAxisIndex': 0,
            },
            {
                'type': 'scatter',
                'name': 'IV/RV Ratio',
                'data': ratio_data,
                'symbolSize': 10,
                'symbol': 'diamond',
                'yAxisIndex': 1,
                'itemStyle': {'color': '#3fb950', 'borderWidth': 1, 'borderColor': '#fff'},
            },
            # Invisible line series on y1 to host the 1.0 markLine
            {
                'type': 'line',
                'name': '_ref',
                'data': [None] * len(symbols),
                'yAxisIndex': 1,
                'silent': True,
                'symbol': 'none',
                'lineStyle': {'opacity': 0},
                'legendHoverLink': False,
                'showInLegend': False,
                'markLine': {
                    'silent': True,
                    'symbol': ['none', 'none'],
                    'data': [{'yAxis': 1.0}],
                    'lineStyle': {'color': '#58a6ff', 'type': 'dashed', 'width': 1, 'opacity': 0.6},
                    'label': {'show': True, 'formatter': '1.0', 'color': '#58a6ff', 'fontSize': 9,
                              'position': 'end'},
                },
            },
        ],
    }

    # ── Chart 3: Bar — Spot-Vol Correlation per symbol ──
    persistence = {
        'backgroundColor': '#0d1117',
        'animation': False,
        'grid': {'left': 48, 'right': 16, 'top': 12, 'bottom': 60},
        'tooltip': {
            'trigger': 'axis',
            'formatter': 'function(p){'
                         'if(!p[0]) return "";'
                         'var v=p[0].value;'
                         'return p[0].axisValueLabel + "<br/>Corr: " + (v>0?"+":"") + v.toFixed(3);'
                         '}',
            'backgroundColor': '#1f2937', 'borderColor': '#2d3748',
            'textStyle': {'color': '#e2e8f0', 'fontSize': 11},
        },
        'xAxis': {
            'type': 'category', 'data': symbols,
            'axisLabel': {'color': '#8b949e', 'fontSize': 9, 'rotate': 35},
            'axisLine': {'lineStyle': {'color': '#2d3748'}},
        },
        'yAxis': {
            'type': 'value', 'name': 'Spot-Vol Corr', 'min': -1, 'max': 1,
            'nameTextStyle': {'color': '#8b949e', 'fontSize': 10},
            'axisLabel': {'color': '#8b949e', 'fontSize': 9},
            'axisLine': {'lineStyle': {'color': '#2d3748'}},
            'splitLine': {'lineStyle': {'color': '#1f2937', 'type': 'dashed'}},
        },
        'series': [{
            'type': 'bar',
            'data': [
                {'value': round(v, 3),
                 'itemStyle': {'color': '#3fb950' if v > 0 else '#f85149'}}
                for v in corr_vals
            ],
            'barMaxWidth': 32,
            'markLine': {
                'silent': True, 'symbol': ['none', 'none'],
                'data': [{'yAxis': 0}],
                'lineStyle': {'color': '#4a5568', 'type': 'dashed', 'width': 1},
                'label': {'show': False},
            },
        }],
    }

    return {'scatter': scatter, 'iv_rv': iv_rv_chart, 'persistence': persistence}


def _ticker_sentiment(iv_pct: float | None, rv_pct: float | None,
                      iv_rv_ratio: float | None) -> str:
    """Derive per-ticker sentiment from this ticker's own IV/RV percentiles.

    Calm     — IV%ile <25 AND RV%ile <25
    Trending — IV ≈ RV, both low-mid percentile
    Elevated — IV%ile >70 OR IV/RV >1.3
    Stressed — IV%ile >85 AND RV%ile >70
    Panic    — IV%ile >95 AND IV/RV >1.5
    """
    if iv_pct is None and rv_pct is None:
        return 'unknown'
    iv_p = iv_pct if iv_pct is not None else 50
    rv_p = rv_pct if rv_pct is not None else 50
    r    = iv_rv_ratio or 1.0

    if iv_p > 95 and r > 1.5:                return 'panic'
    if iv_p > 85 and rv_p > 70:              return 'stressed'
    if iv_p > 70 or r > 1.3:                 return 'elevated'
    if iv_p < 25 and rv_p < 25:              return 'calm'
    return 'trending'


def _percentile_from_history(values: list, current: float) -> float | None:
    """Cheap percentile rank for a list of historical values + a current reading."""
    if not values or current is None:
        return None
    clean = [v for v in values if v is not None and v == v]
    if len(clean) < 30:
        return None
    below = sum(1 for v in clean if v < current)
    return round(below / len(clean) * 100, 1)


def _fetch_iv_pct_batch(symbols: list[str], atm_ivs: dict[str, float]) -> dict[str, float]:
    """For each symbol, compute IV percentile vs trailing iv_history."""
    from app.dependencies import get_skew_history  # noqa: PLC0415
    sh = get_skew_history()
    out: dict[str, float] = {}
    for sym in symbols:
        cur = atm_ivs.get(sym)
        if not cur:
            continue
        try:
            with sh._conn() as conn:
                rows = conn.execute(
                    "SELECT atm_iv FROM iv_history WHERE symbol = ? "
                    "ORDER BY date DESC LIMIT 252",
                    (sym,),
                ).fetchall()
        except Exception:
            continue
        history = [r[0] for r in rows]
        pct = _percentile_from_history(history, cur)
        if pct is not None:
            out[sym] = pct
    return out


def _regime_to_table_rows(
    regime_results: dict,
    atm_ivs: dict[str, float],
    iv_pcts: dict[str, float],
) -> list:
    """Map classify_universe results to the frontend's RegimeMapRow shape."""
    rows = []
    for sym, data in regime_results.items():
        info = data.get('regime_info', {})
        regime_val = data.get('regime', 'unknown')
        if hasattr(regime_val, 'value'):
            regime_val = regime_val.value
        rv = data.get('realised_vol', 0)
        atm = atm_ivs.get(sym)
        iv_rv_ratio = round(atm / rv, 2) if atm and rv > 0 else None

        iv_pct  = iv_pcts.get(sym)
        # RV percentile — use the regime classifier's own RV history if exposed,
        # else fall back to None. data.get('rv_history') is a list of recent RVs.
        rv_history = data.get('rv_history') or []
        rv_pct = _percentile_from_history(rv_history, rv) if rv_history else None

        sentiment = _ticker_sentiment(iv_pct, rv_pct, iv_rv_ratio)

        rows.append({
            'symbol':         sym,
            'regime':         info.get('label', regime_val),
            'sentiment':      sentiment,
            'iv_pct':         iv_pct,
            'rv_pct':         rv_pct,
            'rv':             round(rv, 4),
            'vov':            round(data.get('vol_of_vol', 0), 4),
            'iv_rv':          iv_rv_ratio,
            'mult':           round(data.get('confidence_multiplier', 1.0), 2),
            'recommendation': info.get('recommendation', ''),
            'color':          info.get('color', '#8b949e'),
        })
    return rows


@router.post("")
async def get_radar(body: RadarRequest) -> dict:
    symbols = [s.upper() for s in body.symbols]
    if not symbols:
        return {"regime_table": [], "summary_table": [], "charts": {}}

    scanner = get_scanner()

    # Run classify_universe and ATM IV fetch concurrently
    regime_results, atm_ivs = await asyncio.gather(
        asyncio.to_thread(scanner.regime_classifier.classify_universe, symbols),
        asyncio.to_thread(_fetch_atm_ivs, scanner, symbols),
    )

    # IV percentiles vs each ticker's own 252d history (from skew_history.db)
    iv_pcts = await asyncio.to_thread(_fetch_iv_pct_batch, symbols, atm_ivs)

    regime_rows = _regime_to_table_rows(regime_results, atm_ivs, iv_pcts)
    charts      = _build_radar_charts(regime_results, atm_ivs) if regime_results else {}

    return {
        "regime_table":  regime_rows,
        "summary_table": [],
        "charts":        charts,
    }
