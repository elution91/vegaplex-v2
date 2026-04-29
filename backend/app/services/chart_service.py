from __future__ import annotations

"""
Thin wrapper that calls dashboard_helpers builders and returns plain dicts.

dashboard_helpers was written for Dash — every ECharts builder wraps the
option dict in html.Iframe via _ec_iframe().  We monkey-patch _ec_iframe
to return the raw option dict so FastAPI can JSON-serialise it directly.
"""

from typing import Any


def _sanitise_surface(vol_surface: dict[str, Any]) -> dict[str, Any]:
    """Convert numpy arrays inside surfaces_by_expiry to plain lists so
    dashboard_helpers can safely use  `arr or []`."""
    import numpy as np
    import copy
    vs = copy.deepcopy(vol_surface)
    for side in ("call_surface", "put_surface", "combined_surface"):
        surf = vs.get(side, {})
        for exp_data in surf.get("surfaces_by_expiry", {}).values():
            for key in ("smooth_strikes", "smooth_vols", "strikes", "ivs"):
                v = exp_data.get(key)
                if isinstance(v, np.ndarray):
                    exp_data[key] = v.tolist()
    return vs


def get_surface_charts(scan_result: dict[str, Any], option_type: str = "call") -> dict[str, Any]:
    from dashboard_helpers import (  # noqa: PLC0415
        build_3d_surface_figure,
        build_smile_curves_figure,
        build_term_structure_figure,
    )
    vol_surface = _sanitise_surface(scan_result.get("vol_surface") or scan_result)
    charts: dict[str, Any] = {}
    try:
        import json as _json  # noqa: PLC0415
        fig = build_3d_surface_figure(vol_surface, option_type)
        charts["surface_3d"] = _json.loads(fig.to_json())
    except Exception:
        pass
    try:
        charts["smile"] = build_smile_curves_figure(vol_surface)
    except Exception:
        pass
    try:
        charts["term_structure"] = build_term_structure_figure(vol_surface)
    except Exception:
        pass
    return charts


def get_smile_chart(scan_result: dict[str, Any], expiry: str) -> dict[str, Any]:
    from dashboard_helpers import build_iv_smile_figure  # noqa: PLC0415
    vol_surface = _sanitise_surface(scan_result.get("vol_surface") or scan_result)
    try:
        return build_iv_smile_figure(vol_surface, expiry)
    except Exception:
        return {}


def get_skew_charts(scan_result: dict[str, Any], skew_data: dict[str, Any], expiry: str = "") -> dict[str, Any]:
    from dashboard_helpers import build_skew_charts  # noqa: PLC0415
    # build_skew_charts(skew_data) → tuple (slope_chart, {}, curvature_chart)
    result = build_skew_charts(skew_data)
    if isinstance(result, tuple) and len(result) == 3:
        return {"slope": result[0], "curvature": result[2]}
    if isinstance(result, dict):
        return result
    return {}


def get_regime_charts(regime_result: dict[str, Any]) -> dict[str, Any]:
    from dashboard_helpers import (  # noqa: PLC0415
        build_regime_correlation_figure,
        build_regime_spot_vol_figure,
        build_regime_sqrt_t_figure,
    )
    charts: dict[str, Any] = {}
    try:
        charts["spot_vol"] = build_regime_spot_vol_figure(regime_result)
    except Exception:
        pass
    try:
        charts["correlation"] = build_regime_correlation_figure(regime_result)
    except Exception:
        pass
    try:
        charts["sqrt_t"] = build_regime_sqrt_t_figure(regime_result)
    except Exception:
        pass
    return charts


def get_skew_dynamics_charts(skew_ctx: dict[str, Any]) -> dict[str, Any]:
    from dashboard_helpers import build_skew_dynamics_charts  # noqa: PLC0415
    return build_skew_dynamics_charts(skew_ctx)


def _series_stats(series, decimals: int = 2) -> dict[str, Any]:
    """Compute current/mean/std/min/max for a pandas Series. Returns formatted strings."""
    import pandas as pd  # noqa: PLC0415
    if series is None:
        return {}
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return {}
    cur = float(s.iloc[-1])
    mean = float(s.mean())
    std = float(s.std())
    return {
        "current": f"{cur:.{decimals}f}",
        "mean":    f"{mean:.{decimals}f}",
        "std":     f"{std:.{decimals}f}",
    }


def get_vix_charts(vix_data: dict[str, Any]) -> dict[str, Any]:
    from dashboard_helpers import (  # noqa: PLC0415
        build_vix_ratio_history_figure,
        build_vix_outcomes_figure,
        build_vix_pca_loadings_figure,
        build_vix_roll_cost_figure,
        build_vix_slope_history_figure,
        build_vix_term_structure_figure,
        build_vix_vrp_figure,
        build_vix_percentile_figure,
    )
    strip        = vix_data.get("futures_strip", [])
    strip_source = vix_data.get("futures_strip_source", "prev_settlement")
    df          = vix_data.get("history")       # pandas DataFrame or None
    metrics     = vix_data.get("metrics", {})
    percentiles = vix_data.get("percentiles", {})
    outcomes    = vix_data.get("outcomes", {})
    pca         = vix_data.get("pca", {})
    vix_spot    = metrics.get("vix", 0)
    vix3m       = metrics.get("vix3m", 0)

    import logging  # noqa: PLC0415
    log = logging.getLogger("vegaplex.charts")

    charts: dict[str, Any] = {}
    try:
        charts["term_structure"] = build_vix_term_structure_figure(strip, vix_spot, vix3m, strip_source)
    except Exception as e:
        log.exception(f"build_vix_term_structure_figure failed: {e}")
    if df is not None and not df.empty:
        try:
            charts["ratio_history"] = build_vix_ratio_history_figure(df, metrics)
        except Exception as e:
            log.exception(f"build_vix_ratio_history_figure failed: {e}")
        try:
            charts["vrp"] = build_vix_vrp_figure(df, percentiles)
        except Exception as e:
            log.exception(f"build_vix_vrp_figure failed: {e}")
        try:
            charts["roll_cost"] = build_vix_roll_cost_figure(df)
        except Exception as e:
            log.exception(f"build_vix_roll_cost_figure failed: {e}")
        try:
            charts["percentile"] = build_vix_percentile_figure(df, percentiles)
        except Exception as e:
            log.exception(f"build_vix_percentile_figure failed: {e}")
    try:
        charts["outcomes"] = build_vix_outcomes_figure(outcomes)
    except Exception as e:
        log.exception(f"build_vix_outcomes_figure failed: {e}")
    if pca:
        try:
            charts["pca"] = build_vix_pca_loadings_figure(pca)
        except Exception as e:
            log.exception(f"build_vix_pca_loadings_figure failed: {e}")
        try:
            charts["slope_history"] = build_vix_slope_history_figure(pca)
        except Exception as e:
            log.exception(f"build_vix_slope_history_figure failed: {e}")

    return charts


def get_vix_chart_stats(vix_data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Compute current/mean/std stats for each VIX time series, keyed by chart id."""
    df = vix_data.get("history")
    if df is None or df.empty:
        return {}
    stats: dict[str, dict[str, Any]] = {}
    if "vix_ratio_10d" in df.columns:
        stats["ratio_history"] = _series_stats(df["vix_ratio_10d"], decimals=2)
    if "vrp" in df.columns:
        stats["vrp"] = _series_stats(df["vrp"], decimals=2)
    if "monthly_roll_pct" in df.columns:
        stats["roll_cost"] = _series_stats(df["monthly_roll_pct"], decimals=2)
    if "vix_ratio" in df.columns:
        stats["percentile"] = _series_stats(df["vix_ratio"], decimals=2)
    return stats


def get_radar_charts(radar_data: dict[str, Any]) -> dict[str, Any]:
    """
    Radar chart builders — extracted from dashboard.py private functions.
    TODO: move build_radar_scatter_figure etc. into dashboard_helpers.py
    """
    charts: dict[str, Any] = {}
    try:
        from dashboard_helpers import build_radar_scatter_figure  # noqa: PLC0415
        charts["scatter"] = build_radar_scatter_figure(radar_data)
    except (ImportError, Exception):
        pass
    try:
        from dashboard_helpers import build_iv_rv_figure  # noqa: PLC0415
        charts["iv_rv"] = build_iv_rv_figure(radar_data)
    except (ImportError, Exception):
        pass
    try:
        from dashboard_helpers import build_persistence_figure  # noqa: PLC0415
        charts["persistence"] = build_persistence_figure(radar_data)
    except (ImportError, Exception):
        pass
    return charts


def get_payoff_chart(legs: list[dict[str, Any]], spot: float) -> dict[str, Any]:
    from dashboard_helpers import build_payoff_figure  # noqa: PLC0415
    return build_payoff_figure({'legs': legs}, spot)


def get_iron_fly_payoff_chart(iron_fly: dict[str, Any], spot: float) -> dict[str, Any]:
    """Build an ECharts option dict for an iron fly P&L diagram (no Dash dependency)."""
    import numpy as np  # noqa: PLC0415

    if not iron_fly or 'error' in iron_fly:
        return {}

    k_sp = iron_fly.get('short_put_strike', spot)
    k_sc = iron_fly.get('short_call_strike', spot)
    k_lp = iron_fly.get('long_put_strike', spot * 0.9)
    k_lc = iron_fly.get('long_call_strike', spot * 1.1)
    net_credit = iron_fly.get('net_credit', 0)
    lower_be   = iron_fly.get('lower_breakeven', k_sp - net_credit)
    upper_be   = iron_fly.get('upper_breakeven', k_sc + net_credit)

    atm = (k_sp + k_sc) / 2
    wing_spread = max(k_lc - k_lp, atm * 0.01)
    lo = k_lp - wing_spread * 0.30
    hi = k_lc + wing_spread * 0.30
    prices = np.linspace(lo, hi, 300)

    y = (net_credit
         - np.maximum(0, k_sp - prices)
         - np.maximum(0, prices - k_sc)
         + np.maximum(0, k_lp - prices)
         + np.maximum(0, prices - k_lc))

    y_min = float(np.min(y))
    y_max = float(np.max(y))
    y_range = y_max - y_min

    all_data = [[round(float(x), 4), round(float(v), 4)] for x, v in zip(prices, y)]
    pos_data = [[round(float(x), 4), round(max(0.0, float(v)), 4)] for x, v in zip(prices, y)]
    neg_data = [[round(float(x), 4), round(min(0.0, float(v)), 4)] for x, v in zip(prices, y)]

    def _ml(x_val: float, color: str, dash: str, label: str, pos: str = 'end') -> dict:
        return {
            'name': label,
            'xAxis': round(float(x_val), 4),
            'lineStyle': {'color': color, 'type': dash, 'width': 1},
            'label': {
                'show': True, 'formatter': label, 'color': color,
                'position': pos, 'fontSize': 10,
                'padding': [2, 4],
                'backgroundColor': 'rgba(13,17,23,0.80)',
                'borderRadius': 2,
            },
        }

    mark_lines = [
        _ml(spot,     '#e2e8f0', 'solid',  f'Spot ${spot:.2f}',   'end'),
        _ml(lower_be, '#f85149', 'dashed', f'BE ${lower_be:.2f}', 'middle'),
        _ml(upper_be, '#f85149', 'dashed', f'BE ${upper_be:.2f}', 'middle'),
    ]
    if k_lp != k_sp:
        mark_lines += [
            _ml(k_lp, '#d29922', 'dotted', f'Wing ${k_lp:.2f}', 'start'),
            _ml(k_lc, '#d29922', 'dotted', f'Wing ${k_lc:.2f}', 'start'),
        ]

    tooltip_fn = (
        'function(params){'
        'var p=null;'
        'for(var i=params.length-1;i>=0;i--){if(params[i]&&params[i].value){p=params[i];break;}}'
        'if(!p)return "";'
        'var v=p.value[1];'
        'return "$"+p.value[0].toFixed(2)+" \u2192 "+(v>=0?"+":"")+v.toFixed(3);'
        '}'
    )

    return {
        'backgroundColor': '#0d1117',
        'animation': False,
        'grid': {'left': 52, 'right': 16, 'top': 32, 'bottom': 38, 'containLabel': False},
        'tooltip': {
            'trigger': 'axis',
            'axisPointer': {'type': 'cross', 'lineStyle': {'color': '#4a5568'}},
            'backgroundColor': '#1f2937',
            'borderColor': '#2d3748',
            'textStyle': {'color': '#e2e8f0', 'fontSize': 11},
            'formatter': tooltip_fn,
        },
        'xAxis': {
            'type': 'value',
            'name': 'Price at Expiry',
            'nameLocation': 'middle',
            'nameGap': 24,
            'nameTextStyle': {'color': '#8b949e', 'fontSize': 11},
            'min': round(float(lo), 4),
            'max': round(float(hi), 4),
            'axisLabel': {'color': '#8b949e', 'fontSize': 10, 'formatter': '${value}'},
            'axisLine': {'lineStyle': {'color': '#2d3748'}},
            'axisTick': {'lineStyle': {'color': '#2d3748'}},
            'splitLine': {'show': False},
        },
        'yAxis': {
            'type': 'value',
            'name': 'P&L ($)',
            'nameTextStyle': {'color': '#8b949e', 'fontSize': 11},
            'min': round(y_min - y_range * 0.30, 4),
            'max': round(y_max + y_range * 0.35, 4),
            'axisLabel': {'color': '#8b949e', 'fontSize': 10, 'formatter': '${value}'},
            'axisLine': {'lineStyle': {'color': '#2d3748'}},
            'axisTick': {'lineStyle': {'color': '#2d3748'}},
            'splitLine': {'lineStyle': {'color': '#1f2937', 'type': 'dashed'}},
        },
        'series': [
            {'type': 'line', 'data': pos_data, 'symbol': 'none',
             'lineStyle': {'width': 0, 'color': 'transparent'},
             'areaStyle': {'color': 'rgba(63,185,80,0.18)'}, 'silent': True, 'z': 1},
            {'type': 'line', 'data': neg_data, 'symbol': 'none',
             'lineStyle': {'width': 0, 'color': 'transparent'},
             'areaStyle': {'color': 'rgba(248,81,73,0.12)'}, 'silent': True, 'z': 1},
            {
                'type': 'line', 'data': all_data, 'symbol': 'none',
                'lineStyle': {'color': '#58a6ff', 'width': 2}, 'z': 2,
                'markLine': {'symbol': ['none', 'none'], 'silent': True, 'data': mark_lines},
                'markPoint': {
                    'symbol': 'circle', 'symbolSize': 6, 'silent': True,
                    'data': [{
                        'coord': [round(float(atm), 4), round(float(net_credit), 4)],
                        'itemStyle': {'color': '#3fb950'},
                        'label': {
                            'show': True,
                            'formatter': f'+{net_credit:.2f}',
                            'color': '#3fb950',
                            'fontSize': 11, 'fontWeight': 'bold', 'position': 'top',
                        },
                    }],
                },
            },
        ],
    }
