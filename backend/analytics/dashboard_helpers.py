"""
Dashboard Helpers Module
Data transformation functions bridging scanner outputs to Dash-compatible formats.
"""

import base64
import os
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import logging
import json

import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash import html

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Unified Plotly dark theme
# ---------------------------------------------------------------------------

# Colour palette — matches custom.css variables
_C = {
    'bg':       '#0d1117',
    'surface':  '#161b22',
    'elevated': '#1f2937',
    'border':   '#2d3748',
    'text':     '#e2e8f0',
    'muted':    '#8b949e',
    'accent':   '#00d4aa',
    'blue':     '#58a6ff',
    'positive': '#3fb950',
    'negative': '#f85149',
    'warning':  '#d29922',
    'purple':   '#a78bfa',
}

# Ordered colour sequence for multi-series charts
CHART_COLORS = [
    _C['accent'], _C['blue'], _C['warning'], _C['purple'],
    _C['positive'], _C['negative'], '#f0a500', '#c084fc',
]


# ---------------------------------------------------------------------------
# Logo watermark — loaded once at import time
# ---------------------------------------------------------------------------

def _load_logo_data_uri() -> Optional[str]:
    try:
        path = os.path.join(os.path.dirname(__file__), 'assets', 'logo.svg')
        with open(path, 'rb') as f:
            b64 = base64.b64encode(f.read()).decode()
        return f'data:image/svg+xml;base64,{b64}'
    except Exception:
        return None

_LOGO_DATA_URI: Optional[str] = _load_logo_data_uri()

# Watermark graphic element — injected into every ECharts option
# Aspect ratio of logo viewBox: 168 × 50  →  width 300, height ~89
_WATERMARK_GRAPHIC = [{
    'type': 'image',
    'left': 'center',
    'top': 'middle',
    'silent': True,
    'z': 0,
    'style': {
        'image': _LOGO_DATA_URI,
        'width': 300,
        'height': 89,
        'opacity': 0.07,
    },
}] if _LOGO_DATA_URI else [{
    'type': 'text',
    'left': 'center',
    'top': 'middle',
    'silent': True,
    'z': 0,
    'style': {
        'text': '\u03bdegaPlex',
        'fontSize': 24,
        'fontWeight': '700',
        'fontFamily': 'sans-serif',
        'fill': 'rgba(255,255,255,0.07)',
    },
}]


# ---------------------------------------------------------------------------
# ECharts shared utilities
# ---------------------------------------------------------------------------

_EC_TOOLTIP = {
    'trigger': 'axis',
    'backgroundColor': '#1f2937',
    'borderColor': '#2d3748',
    'textStyle': {'color': '#e2e8f0', 'fontSize': 11},
}
_EC_TOOLTIP_ITEM = {**_EC_TOOLTIP, 'trigger': 'item'}

_EC_LEGEND = {
    'textStyle': {'color': '#8b949e', 'fontSize': 11},
    'inactiveColor': '#4a5568',
    'top': 4,
    'right': 8,
    'itemWidth': 12,
    'itemHeight': 4,
}

# Grid with enough top-margin to clear title (28px) + legend (18px) = 50px
_EC_GRID = {'left': 52, 'right': 20, 'top': 50, 'bottom': 40, 'containLabel': True}
# Grid for charts with no title — less top margin
_EC_GRID_NO_TITLE = {'left': 52, 'right': 20, 'top': 32, 'bottom': 40, 'containLabel': True}

_EC_AXIS_BASE = {
    'axisLabel': {'color': '#8b949e', 'fontSize': 11},
    'axisLine': {'lineStyle': {'color': '#2d3748'}},
    'axisTick': {'lineStyle': {'color': '#2d3748'}},
    'splitLine': {'lineStyle': {'color': '#1f2937', 'type': 'dashed'}},
    'nameTextStyle': {'color': '#8b949e', 'fontSize': 11},
}

# Shared title style
_EC_TITLE = {'textStyle': {'color': '#8b949e', 'fontSize': 12}, 'left': 4, 'top': 4}
_EC_AXIS_X = {**_EC_AXIS_BASE, 'type': 'value'}
_EC_AXIS_CAT = {**_EC_AXIS_BASE, 'type': 'category'}
_EC_AXIS_TIME = {**_EC_AXIS_BASE, 'type': 'time'}
_EC_AXIS_Y = {**_EC_AXIS_BASE, 'type': 'value'}


def _ec_iframe(option: dict, height: int = 300) -> html.Iframe:
    """Wrap an ECharts option dict in a self-contained srcdoc iframe."""
    # Inject watermark — scale down for short charts so it doesn't dominate
    if 'graphic' not in option:
        wm_w = 240 if height <= 260 else 300
        wm_h = round(wm_w * 50 / 168)   # preserve 168:50 aspect ratio
        wm = [{**_WATERMARK_GRAPHIC[0],
               'style': {**_WATERMARK_GRAPHIC[0]['style'],
                         'width': wm_w, 'height': wm_h}}]
        option = {**option, 'graphic': wm}
    option_json = json.dumps(option)
    srcdoc = (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<style>*{margin:0;padding:0;box-sizing:border-box;}'
        'body{background:#0d1117;overflow:hidden;}'
        '#c{width:100vw;height:100vh;}</style></head>'
        '<body><div id="c"></div>'
        '<script src="/assets/echarts.min.js"></script>'
        '<script>var chart=echarts.init(document.getElementById("c"));'
        f'chart.setOption({option_json});'
        'window.addEventListener("resize",function(){chart.resize();});'
        '</script></body></html>'
    )
    return html.Iframe(
        srcDoc=srcdoc,
        style={'width': '100%', 'height': f'{height}px', 'border': 'none', 'display': 'block'},
    )


def _ec_empty(message: str = 'Run a scan to see data', height: int = 200):
    """Return a plain dark placeholder (no iframe overhead)."""
    return html.Div(
        message,
        style={
            'height': f'{height}px', 'display': 'flex', 'alignItems': 'center',
            'justifyContent': 'center', 'color': '#8b949e', 'fontSize': '13px',
            'background': '#0d1117',
        },
    )


def apply_dark_theme(fig: go.Figure, title: str = '',
                     height: int = None, legend: bool = True) -> go.Figure:
    """Apply the unified dark theme to any Plotly figure.

    Call this as the last step before returning a figure from any builder.
    """
    layout = dict(
        template='plotly_dark',
        paper_bgcolor=_C['bg'],
        plot_bgcolor=_C['surface'],
        font=dict(family="-apple-system, 'Inter', 'Segoe UI', sans-serif",
                  size=12, color=_C['text']),
        title=dict(text=title, font=dict(size=13, color=_C['muted'],
                   family="inherit"), x=0.01, xanchor='left') if title else None,
        margin=dict(l=48, r=16, t=36 if title else 20, b=40),
        xaxis=dict(
            gridcolor=_C['border'], gridwidth=1,
            linecolor=_C['border'], zerolinecolor=_C['border'],
            tickfont=dict(size=11, color=_C['muted']),
        ),
        yaxis=dict(
            gridcolor=_C['border'], gridwidth=1,
            linecolor=_C['border'], zerolinecolor=_C['border'],
            tickfont=dict(size=11, color=_C['muted']),
        ),
        legend=dict(
            bgcolor='rgba(0,0,0,0)', bordercolor=_C['border'], borderwidth=1,
            font=dict(size=11, color=_C['muted']),
        ) if legend else dict(visible=False),
        hoverlabel=dict(
            bgcolor=_C['elevated'], bordercolor=_C['border'],
            font=dict(size=12, color=_C['text']),
        ),
        colorway=CHART_COLORS,
    )
    if height:
        layout['height'] = height
    fig.update_layout(**{k: v for k, v in layout.items() if v is not None})
    return fig


def empty_figure(message: str = 'Run a scan to see data') -> go.Figure:
    """Return a themed empty figure with a centred message."""
    fig = go.Figure()
    fig.add_annotation(
        text=message, showarrow=False,
        font=dict(size=13, color=_C['muted']),
        xref='paper', yref='paper', x=0.5, y=0.5,
    )
    return apply_dark_theme(fig)


# ---------------------------------------------------------------------------
# Serialization helpers (numpy / pandas -> JSON for dcc.Store)
# ---------------------------------------------------------------------------

def numpy_to_json(obj):
    """Recursively convert numpy/pandas types to JSON-safe Python types."""
    from enum import Enum
    if isinstance(obj, dict):
        return {k: numpy_to_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [numpy_to_json(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        v = float(obj)
        if np.isnan(v) or np.isinf(v):
            return None
        return v
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, pd.DataFrame):
        return obj.to_dict('records')
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, pd.Period):
        return str(obj)
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, float):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj
    return obj


def serialize_vol_surface(vol_surface: Dict) -> Dict:
    """Serialize a vol_surface dict for storage in dcc.Store."""
    if not vol_surface:
        return {}

    safe = {}
    for key in ('symbol', 'current_price', 'timestamp'):
        safe[key] = numpy_to_json(vol_surface.get(key))

    # Serialize call/put surfaces
    for surface_key in ('call_surface', 'put_surface'):
        src = vol_surface.get(surface_key, {})
        surface_out = {'type': src.get('type', 'unknown')}

        # Per-expiry data (drop spline & raw_data which aren't serializable)
        expiry_dict = {}
        for expiry, data in src.get('surfaces_by_expiry', {}).items():
            expiry_dict[str(expiry)] = {
                'strikes': numpy_to_json(data.get('strikes')),
                'vols': numpy_to_json(data.get('vols')),
                'smooth_strikes': numpy_to_json(data.get('smooth_strikes')),
                'smooth_vols': numpy_to_json(data.get('smooth_vols')),
                'tte': numpy_to_json(data.get('tte')),
            }
        surface_out['surfaces_by_expiry'] = expiry_dict

        # 2D surface
        s2d = src.get('surface_2d')
        if s2d is not None:
            surface_out['surface_2d'] = {
                'strike_grid': numpy_to_json(s2d['strike_grid']),
                'tte_grid': numpy_to_json(s2d['tte_grid']),
                'surface': numpy_to_json(s2d['surface']),
            }
        else:
            surface_out['surface_2d'] = None

        safe[surface_key] = surface_out

    # Term structure
    safe['term_structure'] = numpy_to_json(vol_surface.get('term_structure', {}))
    # Combined surface
    safe['combined_surface'] = numpy_to_json(vol_surface.get('combined_surface', {}))

    return safe


def serialize_skew_metrics(skew_metrics: Dict) -> Dict:
    """Serialize skew_metrics dict for dcc.Store."""
    return numpy_to_json(skew_metrics) if skew_metrics else {}


def serialize_scan_result(result) -> Dict:
    """Convert a ScanResult dataclass to a JSON-safe dict."""
    from dataclasses import asdict
    try:
        d = asdict(result)
    except Exception:
        d = {
            'timestamp': getattr(result, 'timestamp', ''),
            'symbol': getattr(result, 'symbol', ''),
            'opportunity_type': getattr(result, 'opportunity_type', ''),
            'confidence_score': getattr(result, 'confidence_score', 0),
            'skew_metrics': getattr(result, 'skew_metrics', {}),
            'trade_structure': getattr(result, 'trade_structure', {}),
            'risk_metrics': getattr(result, 'risk_metrics', {}),
            'expected_pnl': getattr(result, 'expected_pnl', None),
            'max_loss': getattr(result, 'max_loss', None),
            'rationale': getattr(result, 'rationale', ''),
        }
    return numpy_to_json(d)


# ---------------------------------------------------------------------------
# Figure builders
# ---------------------------------------------------------------------------

def build_3d_surface_figure(surface_data: Dict, surface_type: str = 'call') -> go.Figure:
    """Build 3D volatility surface figure from serialized surface data."""
    key = f'{surface_type}_surface'
    surface = surface_data.get(key, {})
    s2d = surface.get('surface_2d')

    fig = go.Figure()

    if s2d is None:
        fig.add_annotation(text="No 2D surface data available", showarrow=False,
                           font=dict(size=16, color='white'), xref='paper', yref='paper', x=0.5, y=0.5)
        fig.update_layout(template='plotly_dark', title='Volatility Surface (no data)')
        return fig

    strike_grid = np.array(s2d['strike_grid'])
    tte_grid = np.array(s2d['tte_grid'])
    surface_arr = np.array(s2d['surface'])

    # Replace None / NaN for display
    surface_arr = np.where(surface_arr is None, np.nan, surface_arr).astype(float)

    fig.add_trace(go.Surface(
        x=strike_grid,
        y=tte_grid,
        z=surface_arr,
        colorscale='Viridis',
        hovertemplate='Strike: %{x:.1f}<br>TTE: %{y:.3f}y<br>IV: %{z:.4f}<extra></extra>',
    ))

    symbol = surface_data.get('symbol', '')
    fig.update_layout(
        title=f'{surface_type.capitalize()} Volatility Surface — {symbol}',
        scene=dict(
            xaxis_title='Strike',
            yaxis_title='Time to Expiry (years)',
            zaxis_title='Implied Volatility',
        ),
        template='plotly_dark',
        margin=dict(l=0, r=0, t=40, b=0),
    )
    return fig


def build_smile_curves_figure(surface_data: Dict, surface_type: str = 'call',
                               selected_expiries: Optional[List[str]] = None):
    """Build per-expiry smile curves (strike vs IV)."""
    key = f'{surface_type}_surface'
    by_expiry = surface_data.get(key, {}).get('surfaces_by_expiry', {})
    if not by_expiry:
        return _ec_empty('No per-expiry data available')

    series = []
    all_strikes = []
    for i, (expiry, data) in enumerate(by_expiry.items()):
        if selected_expiries and expiry not in selected_expiries:
            continue
        sx = data.get('smooth_strikes') or []
        sy = data.get('smooth_vols') or []
        if not sx or not sy:
            continue
        # Clip to ±40% from ATM to remove near-zero/far-OTM noise
        spot = data.get('spot') or (sum(sx) / len(sx))
        lo, hi = spot * 0.60, spot * 1.40
        filtered = [(x, v) for x, v in zip(sx, sy) if lo <= x <= hi and v > 0.01]
        if not filtered:
            continue
        fsx, fsy = zip(*filtered)
        all_strikes.extend(fsx)
        tte_label = f"{data.get('tte', 0):.2f}y" if data.get('tte') else ''
        series.append({
            'type': 'line', 'symbol': 'none',
            'name': f'{expiry} ({tte_label})',
            'data': [[round(float(x), 4), round(float(v), 6)] for x, v in zip(fsx, fsy)],
            'lineStyle': {'color': CHART_COLORS[i % len(CHART_COLORS)], 'width': 2},
        })
    if not series:
        return _ec_empty('No data for selected expiries')
    x_min = round(min(all_strikes) * 0.99, 2) if all_strikes else None
    x_max = round(max(all_strikes) * 1.01, 2) if all_strikes else None
    x_axis = {**_EC_AXIS_X, 'name': 'Strike', 'nameLocation': 'middle', 'nameGap': 28}
    if x_min and x_max:
        x_axis['min'] = x_min
        x_axis['max'] = x_max
    sym = surface_data.get('symbol', '')
    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'title': {**_EC_TITLE, 'text': f'IV Smile — {sym}'},
        'grid': _EC_GRID, 'tooltip': _EC_TOOLTIP,
        'legend': _EC_LEGEND,
        'xAxis': x_axis,
        'yAxis': {**_EC_AXIS_Y, 'name': 'IV (%)', 'nameLocation': 'middle', 'nameGap': 36},
        'series': series,
    }, 320)


def build_term_structure_figure(surface_data: Dict):
    """Build term structure chart (ATM vol vs DTE)."""
    ts = surface_data.get('term_structure', {})
    atm_vols = ts.get('atm_vols', [])
    if not atm_vols:
        return _ec_empty('No term structure data')

    dte_vals = [v.get('days_to_expiry', 0) for v in atm_vols]
    vol_vals = [round(float(v.get('atm_vol', 0)), 6) for v in atm_vols]
    contango = ts.get('contango')
    slope = ts.get('slope')

    if contango is True:
        shape_label = 'Contango'
        shape_color = '#f85149'
        shape_note  = 'Near-term IV below far-term — normal carry, vol sellers paid over time'
    elif contango is False:
        shape_label = 'Backwardation'
        shape_color = '#3fb950'
        shape_note  = 'Near-term IV elevated vs far-term — event risk priced in front, favours iron fly / calendar'
    else:
        shape_label = ''
        shape_color = '#8b949e'
        shape_note  = 'Flat term structure — no meaningful slope, premium roughly equal across expirations'

    slope_str = f'  ·  slope {slope:+.4f}/d' if slope is not None else ''
    sym = surface_data.get('symbol', '')

    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'grid': _EC_GRID, 'tooltip': _EC_TOOLTIP,
        'title': [
            {'text': f'Term Structure — {sym}  {shape_label}{slope_str}',
             'textStyle': {'color': shape_color, 'fontSize': 12}, 'left': 4, 'top': 4},
            {'text': shape_note,
             'textStyle': {'color': '#555e6b', 'fontSize': 10}, 'left': 4, 'top': 24},
        ],
        'xAxis': {**_EC_AXIS_X, 'name': 'Days to Expiry', 'nameLocation': 'middle', 'nameGap': 28},
        'yAxis': {**_EC_AXIS_Y, 'name': 'ATM IV (%)', 'nameLocation': 'middle', 'nameGap': 42,
                  'min': 'dataMin', 'max': 'dataMax'},
        'series': [{
            'type': 'line', 'data': list(zip(dte_vals, vol_vals)),
            'symbol': 'circle', 'symbolSize': 7,
            'lineStyle': {'color': '#00d4aa', 'width': 2},
            'itemStyle': {'color': '#00d4aa'},
        }],
    }, 320)


def build_skew_charts(skew_data: Dict):
    """Return (slope_chart, placeholder, curvature_chart) for skew analysis tab."""
    call_by_expiry = skew_data.get('call_skew', {}).get('by_expiry', [])
    put_by_expiry  = skew_data.get('put_skew',  {}).get('by_expiry', [])

    def _two_series_chart(call_data, put_data, y_name):
        series = []
        if call_data:
            series.append({'type': 'line', 'name': 'Call', 'data': call_data,
                           'symbol': 'circle', 'symbolSize': 6,
                           'lineStyle': {'color': '#3fb950', 'width': 2},
                           'itemStyle': {'color': '#3fb950'}})
        if put_data:
            series.append({'type': 'line', 'name': 'Put', 'data': put_data,
                           'symbol': 'circle', 'symbolSize': 6,
                           'lineStyle': {'color': '#f85149', 'width': 2},
                           'itemStyle': {'color': '#f85149'}})
        if not series:
            return _ec_empty(f'No {y_name} data')
        sym = skew_data.get('symbol', '')
        return _ec_iframe({
            'backgroundColor': '#0d1117', 'animation': False,
            'title': {**_EC_TITLE, 'text': f'Skew {y_name} — {sym}'},
            'grid': _EC_GRID, 'tooltip': _EC_TOOLTIP,
            'legend': _EC_LEGEND,
            'xAxis': {**_EC_AXIS_X, 'name': 'TTE (years)', 'nameLocation': 'middle', 'nameGap': 28,
                      'min': 0},
            'yAxis': {**_EC_AXIS_Y, 'name': y_name, 'nameLocation': 'middle', 'nameGap': 42},
            'series': series,
        }, 320)

    call_s = [[round(e.get('tte', 0), 4), round(e.get('slope', 0), 6)] for e in call_by_expiry]
    put_s  = [[round(e.get('tte', 0), 4), round(e.get('slope', 0), 6)] for e in put_by_expiry]
    call_c = [[round(e.get('tte', 0), 4), round(e.get('curvature', 0), 6)] for e in call_by_expiry]
    put_c  = [[round(e.get('tte', 0), 4), round(e.get('curvature', 0), 6)] for e in put_by_expiry]

    return _two_series_chart(call_s, put_s, 'Slope'), html.Div(), _two_series_chart(call_c, put_c, 'Curvature')


def build_iron_fly_payoff_echarts(iron_fly: dict, spot: float) -> html.Iframe:
    """Return an html.Iframe containing a self-contained ECharts P&L diagram.

    Uses srcdoc so the chart is fully embedded — no clientside callback, no
    CDN timing issues. ECharts is loaded from /assets/echarts.min.js.
    """
    if not iron_fly or 'error' in iron_fly:
        return html.Div("No iron fly data", style={'color': '#8b949e', 'padding': '12px'})

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

    def _ml(x_val, color, dash, label, pos='end'):
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
        _ml(spot,     '#e2e8f0', 'solid',  f'Spot ${spot:.2f}',       'end'),
        _ml(lower_be, '#f85149', 'dashed', f'BE ${lower_be:.2f}',     'middle'),
        _ml(upper_be, '#f85149', 'dashed', f'BE ${upper_be:.2f}',     'middle'),
    ]
    if k_lp != k_sp:
        mark_lines += [
            _ml(k_lp, '#d29922', 'dotted', f'Wing ${k_lp:.2f}', 'start'),
            _ml(k_lc, '#d29922', 'dotted', f'Wing ${k_lc:.2f}', 'start'),
        ]

    option = {
        'backgroundColor': '#0d1117',
        'animation': False,
        'grid': {'left': 52, 'right': 16, 'top': 32, 'bottom': 38, 'containLabel': False},
        'tooltip': {
            'trigger': 'axis',
            'axisPointer': {'type': 'cross', 'lineStyle': {'color': '#4a5568'}},
            'backgroundColor': '#1f2937',
            'borderColor': '#2d3748',
            'textStyle': {'color': '#e2e8f0', 'fontSize': 11},
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

    option_json = json.dumps(option)
    srcdoc = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ background: #0d1117; overflow: hidden; }}
  #c {{ width: 100vw; height: 100vh; }}
</style>
</head>
<body>
<div id="c"></div>
<script src="/assets/echarts.min.js"></script>
<script>
  var chart = echarts.init(document.getElementById('c'));
  var opt = {option_json};
  opt.tooltip.formatter = function(params) {{
    var p = null;
    for (var i = params.length - 1; i >= 0; i--) {{
      if (params[i] && params[i].value) {{ p = params[i]; break; }}
    }}
    if (!p) return '';
    var v = p.value[1];
    return '$' + p.value[0].toFixed(2) + ' \u2192 ' + (v >= 0 ? '+' : '') + v.toFixed(3);
  }};
  chart.setOption(opt);
  window.addEventListener('resize', function() {{ chart.resize(); }});
</script>
</body>
</html>"""

    return html.Iframe(
        srcDoc=srcdoc,
        style={
            'width': '100%',
            'height': '290px',
            'border': 'none',
            'display': 'block',
        },
    )


def build_iron_fly_payoff(iron_fly: dict, spot: float) -> go.Figure:
    """P&L at expiry diagram for an iron fly position."""
    import numpy as np

    if not iron_fly or 'error' in iron_fly:
        return empty_figure("No iron fly data")

    k_sp = iron_fly.get('short_put_strike', spot)
    k_sc = iron_fly.get('short_call_strike', spot)
    k_lp = iron_fly.get('long_put_strike', spot * 0.9)
    k_lc = iron_fly.get('long_call_strike', spot * 1.1)
    net_credit = iron_fly.get('net_credit', 0)
    max_risk = iron_fly.get('max_risk', 0)
    lower_be = iron_fly.get('lower_breakeven', k_sp - net_credit)
    upper_be = iron_fly.get('upper_breakeven', k_sc + net_credit)

    atm = (k_sp + k_sc) / 2
    wing_spread = max(k_lc - k_lp, atm * 0.01)  # guard against zero spread

    # Tight range: 30% of wing spread outside each wing — keeps action front and centre
    lo = k_lp - wing_spread * 0.30
    hi = k_lc + wing_spread * 0.30
    prices = np.linspace(lo, hi, 400)

    def pnl(s):
        short_put  = -np.maximum(0, k_sp - s)
        short_call = -np.maximum(0, s - k_sc)
        long_put   =  np.maximum(0, k_lp - s)
        long_call  =  np.maximum(0, s - k_lc)
        return net_credit + short_put + short_call + long_put + long_call

    y = pnl(prices)
    y_max = float(np.max(y))
    y_min = float(np.min(y))
    y_range = y_max - y_min

    fig = go.Figure()

    # Profit fill
    fig.add_trace(go.Scatter(
        x=prices, y=np.where(y >= 0, y, 0),
        fill='tozeroy', fillcolor='rgba(63,185,80,0.18)',
        line=dict(width=0), showlegend=False, hoverinfo='skip',
    ))
    # Loss fill
    fig.add_trace(go.Scatter(
        x=prices, y=np.where(y < 0, y, 0),
        fill='tozeroy', fillcolor='rgba(248,81,73,0.12)',
        line=dict(width=0), showlegend=False, hoverinfo='skip',
    ))
    # P&L line
    fig.add_trace(go.Scatter(
        x=prices, y=y,
        mode='lines',
        line=dict(color='#58a6ff', width=2),
        name='P&L',
        hovertemplate='$%{x:.2f} → %{y:+.3f}<extra></extra>',
    ))

    fig.add_hline(y=0, line=dict(color='#4a5568', width=1))

    # Staggered annotations — alternate top/bottom to avoid collision
    # Each label positioned via add_annotation so we can control y precisely
    label_y_top    = y_max + y_range * 0.18
    label_y_mid    = y_max + y_range * 0.06
    label_y_bottom = y_min - y_range * 0.18

    def _vline_label(x, color, dash, text, y_label, anchor='center'):
        fig.add_vline(x=x, line=dict(color=color, width=1, dash=dash))
        fig.add_annotation(
            x=x, y=y_label, text=text,
            showarrow=False,
            font=dict(color=color, size=10),
            xanchor=anchor, yanchor='bottom',
            bgcolor='rgba(13,17,23,0.7)', borderpad=2,
        )

    if k_lp != k_sp:
        _vline_label(k_lp, '#d29922', 'dot', f'Wing ${k_lp:.2f}', label_y_bottom, 'right')
        _vline_label(k_lc, '#d29922', 'dot', f'Wing ${k_lc:.2f}', label_y_bottom, 'left')

    _vline_label(lower_be, '#f85149', 'dash', f'BE ${lower_be:.2f}', label_y_mid, 'right')
    _vline_label(upper_be, '#f85149', 'dash', f'BE ${upper_be:.2f}', label_y_mid, 'left')
    _vline_label(spot,     '#e2e8f0', 'solid', f'Spot ${spot:.2f}', label_y_top,  'left')

    # Max profit label at peak
    fig.add_annotation(
        x=atm, y=net_credit,
        text=f'<b>+{net_credit:.2f}</b>',
        showarrow=False, font=dict(color='#3fb950', size=11),
        yshift=10,
    )

    fig = apply_dark_theme(fig)
    fig.update_layout(
        title=None,
        xaxis_title='Price at Expiry',
        yaxis_title='P&L ($)',
        xaxis=dict(tickprefix='$', range=[lo, hi]),
        yaxis=dict(tickprefix='$', zeroline=False,
                   range=[y_min - y_range * 0.30, y_max + y_range * 0.35]),
        margin=dict(l=50, r=20, t=10, b=40),
        height=260,
        showlegend=False,
        hovermode='x unified',
    )
    return fig


def get_regime_skew_interpretation(regime: str, skew_data: dict) -> dict:
    """Return a signal/action/color/detail dict based on regime and skew metrics."""
    put_slope = skew_data.get('put_skew', {}).get('overall_slope', 0) or 0
    call_slope = skew_data.get('call_skew', {}).get('overall_slope', 0) or 0
    overall_skew_score = skew_data.get('overall_skew_score', 0) or 0

    regime_upper = regime.upper()

    if 'STICKY DELTA' in regime_upper or regime.strip() == 'Sticky Delta':
        if put_slope < -0.3:
            return {
                'signal': 'Put skew rich — sell bias',
                'action': 'Consider short put spread or risk reversal (sell puts, buy calls)',
                'color': '#3fb950',
                'detail': 'In Sticky Delta, long skew is very unprofitable. Steep put skew = premium selling opportunity.',
            }
        elif put_slope > -0.05:
            return {
                'signal': 'Skew flat — no edge',
                'action': 'Wait for skew to reprice',
                'color': '#8b949e',
                'detail': 'In Sticky Delta, flat skew has no premium to harvest.',
            }
        else:
            return {
                'signal': 'Mild put skew — monitor',
                'action': 'Short skew bias but no urgent entry',
                'color': '#d29922',
                'detail': 'In Sticky Delta, skew decay favours sellers over time.',
            }

    elif 'STICKY STRIKE' in regime_upper or regime.strip() == 'Sticky Strike':
        if put_slope < -0.3:
            return {
                'signal': 'Put skew elevated vs regime',
                'action': 'Calendar spread attractive — sell front, buy back expiry',
                'color': '#58a6ff',
                'detail': 'In Sticky Strike, skew theta costs but surface doesn\'t re-mark. Calendars exploit term structure premium.',
            }
        else:
            return {
                'signal': 'Regime neutral',
                'action': 'No strong skew edge',
                'color': '#8b949e',
                'detail': 'In Sticky Strike, skew is mildly over-priced but not actionable alone.',
            }

    elif 'STICKY LOCAL VOL' in regime_upper or regime.strip() == 'Sticky Local Vol':
        if overall_skew_score > 1.0:
            return {
                'signal': 'Skew anomaly detected',
                'action': 'Investigate term structure — vol level trade preferred over skew trade',
                'color': '#d29922',
                'detail': 'In Sticky Local Vol, skew re-marks offset theta. Any anomaly suggests vol level mispricing, not skew.',
            }
        else:
            return {
                'signal': 'Skew fairly priced',
                'action': 'No skew edge — consider VRP harvest if IV > RV',
                'color': '#8b949e',
                'detail': 'In Sticky Local Vol, skew is fairly priced by construction.',
            }

    elif 'JUMPY' in regime_upper:
        if put_slope < -0.3:
            return {
                'signal': 'Put skew steep + Jumpy Vol = strong buy signal',
                'action': 'BUY skew — long puts, short calls. Risk reversal favoured.',
                'color': '#f85149',
                'detail': 'In Jumpy Vol, re-mark profit exceeds skew theta. This is the ONLY regime where long skew is consistently profitable.',
            }
        elif put_slope > -0.1:
            return {
                'signal': 'Jumpy Vol but skew not yet pricing tail risk',
                'action': 'Buy OTM puts — skew likely to steepen',
                'color': '#d29922',
                'detail': 'In Jumpy Vol with flat skew, puts are cheap relative to regime. Skew tends to expand further.',
            }
        else:
            return {
                'signal': 'Jumpy Vol — long skew bias',
                'action': 'Long skew positions. Monitor for further expansion.',
                'color': '#f85149',
                'detail': 'In Jumpy Vol, long skew is the structural trade.',
            }

    else:
        return {
            'signal': 'Insufficient data',
            'action': 'Classify regime first',
            'color': '#4a5568',
            'detail': '',
        }


def build_iv_smile_figure(surface_data: Dict, selected_expiry: str = ''):
    """Combined IV vs Strike smile — calls and puts on same axes."""
    if not surface_data:
        return _ec_empty('Run a scan to view the IV smile', 380)

    spot = surface_data.get('current_price', 0)
    call_expiries = surface_data.get('call_surface', {}).get('surfaces_by_expiry', {})
    put_expiries  = surface_data.get('put_surface',  {}).get('surfaces_by_expiry', {})
    all_expiries = sorted(set(list(call_expiries) + list(put_expiries)))
    if not all_expiries:
        return _ec_empty('No expiry data available', 380)

    _CALL_COLORS = ['#3fb950', '#00d4aa', '#58a6ff', '#a78bfa']
    _PUT_COLORS  = ['#f85149', '#d29922', '#ff7b72', '#ffa657']

    show_expiries = all_expiries if selected_expiry == 'all' else (
        [selected_expiry] if selected_expiry in all_expiries else [all_expiries[0]]
    )

    series = []
    for i, exp in enumerate(show_expiries):
        suffix = f' ({exp})' if selected_expiry == 'all' else ''
        cd = call_expiries.get(exp, {})
        pd_ = put_expiries.get(exp, {})
        cx = cd.get('smooth_strikes') or cd.get('strikes') or []
        cy = cd.get('smooth_vols') or cd.get('vols') or []
        px = pd_.get('smooth_strikes') or pd_.get('strikes') or []
        py = pd_.get('smooth_vols') or pd_.get('vols') or []
        if cy and max(cy) < 5:
            cy = [v * 100 for v in cy]
        if py and max(py) < 5:
            py = [v * 100 for v in py]
        if cx and cy:
            series.append({'type': 'line', 'name': f'Call{suffix}',
                           'data': [[round(float(x), 4), round(float(v), 4)] for x, v in zip(cx, cy)],
                           'symbol': 'circle', 'symbolSize': 4,
                           'lineStyle': {'color': _CALL_COLORS[i % 4], 'width': 2},
                           'itemStyle': {'color': _CALL_COLORS[i % 4]}})
        if px and py:
            series.append({'type': 'line', 'name': f'Put{suffix}',
                           'data': [[round(float(x), 4), round(float(v), 4)] for x, v in zip(px, py)],
                           'symbol': 'circle', 'symbolSize': 4,
                           'lineStyle': {'color': _PUT_COLORS[i % 4], 'width': 2, 'type': 'dashed'},
                           'itemStyle': {'color': _PUT_COLORS[i % 4]}})

    if not series:
        return _ec_empty('No data for selected expiry', 380)

    mark_lines = []
    if spot:
        mark_lines = [{'xAxis': round(float(spot), 4),
                       'lineStyle': {'color': '#8b949e', 'type': 'dashed', 'width': 1},
                       'label': {'show': True, 'formatter': f'{spot:.2f}', 'color': '#8b949e',
                                 'position': 'end', 'fontSize': 10}}]
    if mark_lines:
        series[-1]['markLine'] = {'symbol': ['none', 'none'], 'silent': True, 'data': mark_lines}

    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'grid': _EC_GRID, 'tooltip': _EC_TOOLTIP,
        'legend': {**_EC_LEGEND, 'top': 'top'},
        'xAxis': {**_EC_AXIS_X, 'name': 'Strike', 'nameLocation': 'middle', 'nameGap': 28,
                  'min': 'dataMin', 'max': 'dataMax'},
        'yAxis': {**_EC_AXIS_Y, 'name': 'IV (%)', 'nameLocation': 'middle', 'nameGap': 42,
                  'min': 0},
        'series': series,
    }, 380)


def build_payoff_figure(trade_structure: Dict, current_price: float = 100):
    """Build trade payoff diagram from trade_structure dict."""
    legs = trade_structure.get('legs', [])
    if not legs:
        return _ec_empty('No trade legs', 300)

    # ── Price grid ────────────────────────────────────────────────────────────
    n_pts = 300
    price_range = np.linspace(current_price * 0.65, current_price * 1.35, n_pts)

    # ── Net premium (initial cash flow, correct sign convention) ─────────────
    net_premium = 0.0
    for leg in legs:
        p = (leg.get('price') or 0)
        c = (leg.get('contracts') or 1)
        if leg.get('action') == 'buy':
            net_premium -= p * c * 100   # debit
        else:
            net_premium += p * c * 100   # credit

    # ── Per-leg series (individual legs, muted) ───────────────────────────────
    total_payoff = np.zeros(n_pts)
    series = []
    for i, leg in enumerate(legs):
        strike    = leg.get('strike') or current_price
        contracts = leg.get('contracts') or 1
        price     = leg.get('price') or 0
        opt_type  = leg.get('type', 'call')
        action    = leg.get('action', 'buy')
        mult      = contracts * 100

        intrinsic = (np.maximum(price_range - strike, 0) if opt_type == 'call'
                     else np.maximum(strike - price_range, 0))

        # P&L = ±(intrinsic − cost) × multiplier, fully per-contract
        if action == 'buy':
            leg_pnl = (intrinsic - price) * mult
        else:
            leg_pnl = (price - intrinsic) * mult

        total_payoff += leg_pnl

        series.append({
            'type': 'line', 'symbol': 'none',
            'name': f"{action} {opt_type} {strike:.0f}",
            'data': [[round(float(x), 2), round(float(v), 2)]
                     for x, v in zip(price_range, leg_pnl)],
            'lineStyle': {'color': CHART_COLORS[i % len(CHART_COLORS)],
                          'width': 1.5, 'opacity': 0.55},
            'z': 2,
        })

    # ── Total P&L with profit/loss area shading ───────────────────────────────
    total_data = [[round(float(x), 2), round(float(v), 2)]
                  for x, v in zip(price_range, total_payoff)]

    # Identify breakeven crossings for markPoint
    breakevens = []
    for j in range(len(total_payoff) - 1):
        if total_payoff[j] * total_payoff[j + 1] <= 0 and total_payoff[j] != total_payoff[j + 1]:
            be = price_range[j] - total_payoff[j] * (
                (price_range[j + 1] - price_range[j]) /
                (total_payoff[j + 1] - total_payoff[j])
            )
            breakevens.append(round(float(be), 2))

    mark_lines_data = [
        {'xAxis': round(float(current_price), 2),
         'lineStyle': {'color': '#00d4aa', 'type': 'dashed', 'width': 1},
         'label': {'show': True, 'formatter': f'Spot\n{current_price:.1f}',
                   'color': '#00d4aa', 'fontSize': 9, 'position': 'insideEndTop'}},
        {'yAxis': 0,
         'lineStyle': {'color': '#4a5568', 'type': 'solid', 'width': 1},
         'label': {'show': False}},
    ]
    for be in breakevens[:3]:
        mark_lines_data.append({
            'xAxis': be,
            'lineStyle': {'color': '#d29922', 'type': 'dotted', 'width': 1},
            'label': {'show': True, 'formatter': f'BE\n{be:.1f}',
                      'color': '#d29922', 'fontSize': 9, 'position': 'insideEndTop'},
        })

    series.append({
        'type': 'line', 'symbol': 'none', 'name': 'Total',
        'data': total_data,
        'lineStyle': {'color': '#e2e8f0', 'width': 2.5},
        'areaStyle': {
            'origin': 'auto',
            'color': {
                'type': 'linear', 'x': 0, 'y': 0, 'x2': 0, 'y2': 1,
                'colorStops': [
                    {'offset': 0,   'color': 'rgba(63,185,80,0.18)'},
                    {'offset': 0.5, 'color': 'rgba(63,185,80,0.04)'},
                    {'offset': 0.5, 'color': 'rgba(248,81,73,0.04)'},
                    {'offset': 1,   'color': 'rgba(248,81,73,0.18)'},
                ],
            },
        },
        'markLine': {'symbol': ['none', 'none'], 'silent': True,
                     'data': mark_lines_data},
        'z': 3,
    })

    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'title': {**_EC_TITLE, 'text': 'Payoff at Expiry'},
        'grid': _EC_GRID,
        'tooltip': {**_EC_TOOLTIP, 'trigger': 'axis',
                    'formatter': ("function(p){var x=p[0].axisValue.toFixed(2);"
                                  "var s='<b>$'+x+'</b><br/>';"
                                  "p.forEach(function(i){"
                                  "s+=i.marker+i.seriesName+': <b>$'+"
                                  "i.value[1].toFixed(0)+'</b><br/>';});"
                                  "return s;}")},
        'legend': _EC_LEGEND,
        'xAxis': {**_EC_AXIS_X, 'name': 'Underlying Price',
                  'nameLocation': 'middle', 'nameGap': 28},
        'yAxis': {**_EC_AXIS_Y, 'name': 'P&L ($)',
                  'nameLocation': 'middle', 'nameGap': 48},
        'series': series,
    }, 300)


def build_greeks_figure(trade_structure: Dict):
    """Greeks bar chart."""
    metrics = trade_structure.get('metrics', {})
    names  = ['Delta', 'Gamma', 'Vega', 'Theta']
    values = [metrics.get(f'total_{n.lower()}', 0) or 0 for n in names]
    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'grid': _EC_GRID, 'tooltip': {**_EC_TOOLTIP, 'trigger': 'item'},
        'title': {**_EC_TITLE, 'text': 'Greeks Profile'},
        'grid': _EC_GRID,
        'tooltip': {**_EC_TOOLTIP, 'trigger': 'item',
                    'formatter': "function(p){return p.name+': <b>'+p.value.toFixed(4)+'</b>';}"},
        'xAxis': {**_EC_AXIS_CAT, 'data': names},
        'yAxis': {**_EC_AXIS_Y, 'name': 'Net value', 'nameLocation': 'middle', 'nameGap': 42},
        'series': [{'type': 'bar', 'barMaxWidth': 40,
                    'data': [{'value': round(v, 6),
                              'itemStyle': {'color': '#3fb950' if v >= 0 else '#f85149'}}
                             for v in values]}],
    }, 240)


# ---------------------------------------------------------------------------
# Table data helpers
# ---------------------------------------------------------------------------

def _greek_profile(metrics: dict) -> dict:
    """Summarise net Greeks into a compact display string + bias alignment label.

    Returns:
        display   — formatted string shown in the table cell
        vega_bias — 'short' | 'long' | 'neutral'  (for cell colouring)
        aligned   — True if theta sign is consistent with vega bias
    """
    if not metrics:
        return {'display': '—', 'vega_bias': 'neutral', 'aligned': True}

    delta = metrics.get('total_delta', 0) or 0
    vega  = metrics.get('total_vega',  0) or 0
    theta = metrics.get('total_theta', 0) or 0

    def _fmt(v, decimals=0):
        s = f"{abs(v):.{decimals}f}"
        return ('+' if v >= 0 else '−') + s

    parts = []
    if abs(delta) >= 0.01:
        parts.append(f"δ {_fmt(delta, 2)}")
    if abs(vega) >= 0.5:
        parts.append(f"ν {_fmt(vega, 0)}")
    if abs(theta) >= 0.5:
        parts.append(f"θ {_fmt(theta, 0)}")

    display = '  '.join(parts) if parts else '—'

    vega_bias = 'short' if vega < -0.5 else ('long' if vega > 0.5 else 'neutral')
    # Aligned: short-vol should collect theta (theta > 0); long-vol should pay theta (theta < 0)
    aligned = (vega_bias == 'short' and theta >= 0) or \
              (vega_bias == 'long'  and theta <= 0) or \
              (vega_bias == 'neutral')

    # Store as string so Dash filter_query {_greek_aligned} = "False" matches correctly
    return {'display': display, 'vega_bias': vega_bias, 'aligned': 'True' if aligned else 'False'}


def scan_results_to_table(results: List[Dict]) -> List[Dict]:
    """Convert serialized ScanResult list to flat rows for DataTable.

    When a ScanResult carries ``all_opportunities`` (list of dicts), each
    opportunity is expanded into its own row so the table shows every detected
    setup, not just the best one per symbol.
    """
    rows = []
    for r in results:
        regime = r.get('regime_data') or {}
        regime_info = regime.get('regime_info', {})
        regime_label = regime_info.get('label', '') if regime_info else ''

        all_opps = r.get('all_opportunities') or []
        if all_opps:
            for opp in all_opps:
                gp = _greek_profile(opp.get('metrics') or {})
                rows.append({
                    'symbol': r.get('symbol', ''),
                    'type': opp.get('type', ''),
                    'subtype': opp.get('subtype', ''),
                    'confidence': round(opp.get('confidence', 0), 3),
                    'expected_pnl': round(opp.get('expected_pnl', 0) or 0, 2),
                    'max_loss': round(opp.get('max_loss', 0) or 0, 2),
                    'max_gain': round(opp.get('max_gain', 0) or 0, 2),
                    'risk_reward': round(opp.get('risk_reward', 0) or 0, 2),
                    'regime': opp.get('regime', regime_label),
                    'rationale': opp.get('rationale', ''),
                    'greeks': gp['display'],
                    '_vega_bias': gp['vega_bias'],
                    '_greek_aligned': gp['aligned'],
                })
        else:
            # Fallback for old-format results without all_opportunities
            rows.append({
                'symbol': r.get('symbol', ''),
                'type': r.get('opportunity_type', ''),
                'subtype': '',
                'confidence': round(r.get('confidence_score', 0), 3),
                'expected_pnl': round(r.get('expected_pnl', 0) or 0, 2),
                'max_loss': round(r.get('max_loss', 0) or 0, 2),
                'max_gain': round(r.get('max_gain', 0) or 0, 2),
                'risk_reward': round(r.get('risk_reward', 0) or 0, 2),
                'regime': regime_label,
                'rationale': r.get('rationale', ''),
                'greeks': '—',
                '_vega_bias': 'neutral',
                '_greek_aligned': True,
            })
    return rows


def opportunities_to_table(opportunity: Dict) -> List[Dict]:
    """Expand all_opportunities from a single opportunity dict to flat rows."""
    all_opp = opportunity.get('all_opportunities', [opportunity])
    rows = []
    for opp in all_opp:
        rows.append({
            'symbol': opp.get('symbol', ''),
            'type': opp.get('type', ''),
            'subtype': opp.get('subtype', ''),
            'confidence': round(opp.get('confidence', 0), 3),
            'direction': opp.get('direction', ''),
            'expected_pnl': round(opp.get('expected_pnl', 0) or 0, 4),
            'risk_level': opp.get('risk_level', ''),
            'holding_period': opp.get('holding_period', ''),
            'rationale': opp.get('rationale', ''),
        })
    return rows


def legs_to_table(trade_structure: Dict) -> List[Dict]:
    """Convert trade legs to DataTable rows."""
    rows = []
    for leg in trade_structure.get('legs', []):
        rows.append({
            'action': leg.get('action', ''),
            'type': leg.get('type', ''),
            'strike': round(leg.get('strike', 0), 2),
            'expiry': str(leg.get('expiry', '')),
            'contracts': leg.get('contracts', 1),
            'price': round(leg.get('price', 0), 4),
            'delta': round(leg.get('delta', 0), 4),
            'gamma': round(leg.get('gamma', 0), 6),
            'vega': round(leg.get('vega', 0), 4),
            'theta': round(leg.get('theta', 0), 4),
            'iv': round(leg.get('implied_vol', 0), 4),
            'richness': round(leg.get('richness_score', 0), 3),
        })
    return rows


# ---------------------------------------------------------------------------
# Regime tab figure builders
# ---------------------------------------------------------------------------

def build_regime_spot_vol_figure(regime_data: Dict):
    """Spot-returns vs vol-changes scatter (left) + realised vol time series (right)."""
    dates   = regime_data.get('ts_dates', [])
    returns = regime_data.get('ts_spot_returns', [])
    vol     = regime_data.get('ts_realised_vol', [])
    if not returns or not vol:
        return _ec_empty('Insufficient data', 300)

    vol_changes = [vol[i] - vol[i - 1] for i in range(1, len(vol))]
    ret_aligned = returns[1:]
    scatter_data = [[round(float(r), 6), round(float(v), 6)] for r, v in zip(ret_aligned, vol_changes)]
    ts_data = [[str(d)[:10], round(float(v), 6)] for d, v in zip(dates, vol)]

    sym = regime_data.get('symbol', '')
    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'title': {**_EC_TITLE, 'text': f'Spot–Vol Dynamics — {sym}'},
        'tooltip': {**_EC_TOOLTIP, 'trigger': 'item',
                    'formatter': "function(p){return 'Return: <b>'+p.value[0].toFixed(4)+"
                                 "'</b><br>ΔVol: <b>'+p.value[1].toFixed(4)+'</b>';}"},
        'legend': {**_EC_LEGEND, 'data': ['Scatter', 'Realised Vol']},
        'grid': [
            {'left': '6%', 'right': '54%', 'top': '18%', 'bottom': '14%'},
            {'left': '52%', 'right': '4%',  'top': '18%', 'bottom': '14%'},
        ],
        'xAxis': [
            {**_EC_AXIS_X, 'gridIndex': 0, 'name': 'Spot Return', 'nameLocation': 'middle', 'nameGap': 28},
            {**_EC_AXIS_TIME, 'gridIndex': 1},
        ],
        'yAxis': [
            {**_EC_AXIS_Y, 'gridIndex': 0, 'name': 'ΔVol', 'nameLocation': 'middle', 'nameGap': 42},
            {**_EC_AXIS_Y, 'gridIndex': 1, 'name': 'Realised Vol', 'nameLocation': 'middle', 'nameGap': 42},
        ],
        'series': [
            {'type': 'scatter', 'name': 'Scatter', 'xAxisIndex': 0, 'yAxisIndex': 0,
             'data': scatter_data, 'symbolSize': 5,
             'itemStyle': {'color': '#00d4aa', 'opacity': 0.6}},
            {'type': 'line', 'name': 'Realised Vol', 'xAxisIndex': 1, 'yAxisIndex': 1,
             'symbol': 'none', 'data': ts_data,
             'lineStyle': {'color': '#ffa15a', 'width': 2}},
        ],
    }, 300)


def build_regime_correlation_figure(regime_data: Dict):
    """Bar chart of spot-vol correlations at different windows."""
    windows = ['5d', '20d', '60d']
    values  = [regime_data.get(f'spot_vol_corr_{w}', 0) for w in windows]
    thresholds = [
        {'yAxis': 0.05,  'lineStyle': {'color': '#3fb950', 'type': 'dotted'}, 'label': {'formatter': 'Sticky Δ',     'color': '#3fb950', 'fontSize': 9}},
        {'yAxis': -0.15, 'lineStyle': {'color': '#58a6ff', 'type': 'dotted'}, 'label': {'formatter': 'Sticky Strike', 'color': '#58a6ff', 'fontSize': 9}},
        {'yAxis': -0.45, 'lineStyle': {'color': '#f85149', 'type': 'dotted'}, 'label': {'formatter': 'Jumpy',         'color': '#f85149', 'fontSize': 9}},
    ]
    sym = regime_data.get('symbol', '')
    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'title': {**_EC_TITLE, 'text': f'Spot–Vol Correlation Windows — {sym}'},
        'grid': _EC_GRID,
        'tooltip': {**_EC_TOOLTIP, 'trigger': 'item',
                    'formatter': "function(p){return p.name+' window: <b>'+p.value.toFixed(3)+'</b>';}"},
        'xAxis': {**_EC_AXIS_CAT, 'data': windows},
        'yAxis': {**_EC_AXIS_Y, 'name': 'Correlation',
                  'min': -1, 'max': 1,
                  'markLine': {'symbol': ['none', 'none'], 'silent': True, 'data': thresholds}},
        'series': [{'type': 'bar', 'barMaxWidth': 44, 'barCategoryGap': '50%',
                    'data': [{'value': round(v, 4),
                              'itemStyle': {'color': '#f85149' if v < -0.15 else '#d29922' if v < 0.05 else '#3fb950'}}
                             for v in values]}],
    }, 240)


def build_regime_sqrt_t_figure(regime_data: Dict):
    """√T-normalised skew comparison grouped bar chart."""
    sqrt_t = regime_data.get('sqrt_t_skew', {})
    if not sqrt_t:
        return _ec_empty('No √T data', 240)
    labels, raw_vals, norm_vals = [], [], []
    for key in sorted(sqrt_t.keys()):
        e = sqrt_t[key]
        labels.append(key)
        raw_vals.append(round(e.get('raw_slope', 0), 6))
        norm_vals.append(round(e.get('sqrt_t_normalised', 0), 6))
    sym = regime_data.get('symbol', '')
    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'title': {**_EC_TITLE, 'text': f'√T Skew Normalisation — {sym}'},
        'grid': _EC_GRID,
        'tooltip': {**_EC_TOOLTIP,
                    'formatter': "function(params){if(!Array.isArray(params))params=[params];"
                                 "var s=params[0].axisValue+'<br/>';"
                                 "params.forEach(function(p){"
                                 "var diff=p.seriesName==='Raw Slope'?'':'  '+(p.value>=0?'rich vs theory':'cheap vs theory');"
                                 "s+=p.marker+p.seriesName+': <b>'+p.value.toFixed(4)+'</b>'+diff+'<br/>';});"
                                 "return s;}"},
        'legend': _EC_LEGEND,
        'xAxis': {**_EC_AXIS_CAT, 'data': labels},
        'yAxis': {**_EC_AXIS_Y, 'name': 'Slope', 'nameLocation': 'middle', 'nameGap': 42},
        'series': [
            {'type': 'bar', 'name': 'Raw Slope',     'barMaxWidth': 28, 'data': raw_vals,  'itemStyle': {'color': '#636efa'}},
            {'type': 'bar', 'name': '√T Normalised', 'barMaxWidth': 28, 'data': norm_vals, 'itemStyle': {'color': '#00d4aa'}},
        ],
    }, 240)


def regime_universe_to_table(regime_results: Dict) -> List[Dict]:
    """Convert a dict of {symbol: regime_result} to flat table rows."""
    rows = []
    for sym, data in regime_results.items():
        info = data.get('regime_info', {})
        regime_val = data.get('regime', 'unknown')
        if hasattr(regime_val, 'value'):
            regime_val = regime_val.value
        rows.append({
            'symbol': sym,
            'regime': info.get('label', regime_val),
            'sentiment': info.get('sentiment', ''),
            'corr_20d': round(data.get('spot_vol_corr_20d', 0), 3),
            'realised_vol': f"{data.get('realised_vol', 0):.1%}",
            'vol_of_vol': round(data.get('vol_of_vol', 0), 3),
            'multiplier': round(data.get('confidence_multiplier', 1.0), 1),
            'recommendation': info.get('recommendation', ''),
        })
    return rows


# ---------------------------------------------------------------------------
# Earnings tab helpers
# ---------------------------------------------------------------------------

def earnings_results_to_table(data):
    """Convert earnings store data into flat table rows for the DataTable."""
    rows = []
    if not data:
        return rows
    metrics = data.get('stock_metrics', {})
    recommended = data.get('recommended', [])
    near_misses = data.get('near_misses', [])
    timing_map = data.get('timing', {})

    from datetime import date as _today_cls, datetime as _dt_cls
    _today = _today_cls.today()

    def _build_row(ticker, tier_label, reason=''):
        m = metrics.get(ticker, {})
        t = timing_map.get(ticker, {})

        # Merge date + timing → "Feb 24 · PM"
        raw_date = t.get('earnings_date', '')
        raw_timing = t.get('timing', '')
        try:
            _d = _dt_cls.strptime(raw_date, '%Y-%m-%d')
            short_date = f"{_d.strftime('%b')} {_d.day}"
            days_ahead = (_d.date() - _today).days
        except Exception:
            short_date = raw_date
            days_ahead = None
        timing_short = 'PM' if 'Post' in raw_timing else ('AM' if 'Pre' in raw_timing else raw_timing)
        when = f"{short_date} · {timing_short}" if short_date else '—'

        # Merge win_rate + quarters → "83% (12q)"
        wr = m.get('win_rate', 0)
        wq = m.get('win_quarters', 0)
        win_rate_str = f"{wr:.0f}% ({wq}q)" if wq else f"{wr:.0f}%"

        # Merge straddle_vs_bennett + premium_signal → "+2.1pp · Rich"
        svb = m.get('straddle_vs_bennett')
        sig = m.get('premium_signal', '')
        if svb is not None and sig:
            prefix = '+' if svb >= 0 else ''
            premium = f"{prefix}{svb:.1f}pp · {sig}"
        elif sig:
            premium = sig
        else:
            premium = '—'

        return {
            'ticker': ticker,
            'days_ahead': days_ahead,
            'tier': tier_label,
            'when': when,
            'price': round(m.get('price', 0), 2),
            'iv_rv_ratio': round(m.get('iv_rv_ratio', 0), 2),
            'term_structure': round(m.get('term_structure', 0), 4),
            'win_rate_disp': win_rate_str,
            'expected_move': f"${m.get('expected_move_dollars', 0):.2f}",
            'bennett_move': f"±{m['bennett_move_pct']:.1f}%" if m.get('bennett_move_pct') else '—',
            'premium': premium,
            'vol_edge_pp': f"{m['vol_edge_pp']:.0f}pp" if m.get('vol_edge_pp') is not None else '—',
            'richness_ratio': f"{m['richness_ratio']:.2f}x" if m.get('richness_ratio') is not None else '—',
            'structure_rec': m.get('structure_rec', '—'),
            'spread_signal': m.get('spread_signal', '—'),
            'cal_slippage_pct': (
                f"{m['cal_slippage_pct']:.0f}%" if m.get('cal_slippage_pct') is not None else '—'
            ),
            # Keep raw fields for filter_query styling
            'premium_signal': m.get('premium_signal', '—'),
            'straddle_vs_bennett': svb,
        }

    for ticker in recommended:
        m = metrics.get(ticker, {})
        rows.append(_build_row(ticker, f"Tier {m.get('tier', '?')}"))

    for item in near_misses:
        ticker = item[0] if isinstance(item, (list, tuple)) else item
        reason = item[1] if isinstance(item, (list, tuple)) and len(item) > 1 else ''
        rows.append(_build_row(ticker, 'Near Miss', reason))

    # Sort: soonest first, then Tier 1 before Tier 2 before Near Miss
    _tier_order = {'Tier 1': 0, 'Tier 2': 1, 'Near Miss': 2}
    rows.sort(key=lambda r: (
        r['days_ahead'] if r['days_ahead'] is not None else 999,
        _tier_order.get(r['tier'], 9),
    ))

    return rows


def earnings_iron_fly_to_table(data):
    """Convert iron fly dict into key-value rows for a detail table."""
    if not data or 'error' in data:
        return []
    labels = [
        ('Expiration', 'expiration'),
        ('Short Put Strike', 'short_put_strike'),
        ('Short Call Strike', 'short_call_strike'),
        ('Long Put Strike', 'long_put_strike'),
        ('Long Call Strike', 'long_call_strike'),
        ('Short Put Premium', 'short_put_premium'),
        ('Short Call Premium', 'short_call_premium'),
        ('Total Credit', 'total_credit'),
        ('Total Debit', 'total_debit'),
        ('Net Credit', 'net_credit'),
        ('Max Profit', 'max_profit'),
        ('Max Risk', 'max_risk'),
        ('Lower Break-even', 'lower_breakeven'),
        ('Upper Break-even', 'upper_breakeven'),
        ('Risk / Reward', 'risk_reward_ratio'),
    ]
    rows = []
    for label, key in labels:
        val = data.get(key, 'N/A')
        if isinstance(val, float):
            val = f"${val:.2f}" if key not in ('risk_reward_ratio',) else f"1:{val}"
        rows.append({'metric': label, 'value': str(val)})
    return rows


# ---------------------------------------------------------------------------
# VIX Futures tab figure builders
# ---------------------------------------------------------------------------

def build_vix_ratio_history_figure(df: pd.DataFrame, metrics: dict):
    """VIX/VIX3M ratio history with carry zones and carry ratio overlay (dual axis)."""
    if df is None or df.empty:
        return _ec_empty('No VIX data', 320)

    dates = [str(d)[:10] for d in df.index]
    raw   = [round(float(v), 4) if pd.notna(v) else None for v in df['vix_ratio']]
    smo   = [round(float(v), 4) if pd.notna(v) else None for v in df['vix_ratio_10d']]
    carry = [round(float(v), 4) if pd.notna(v) else None for v in df['carry_ratio']]
    raw_data   = [[d, v] for d, v in zip(dates, raw)   if v is not None]
    smo_data   = [[d, v] for d, v in zip(dates, smo)   if v is not None]
    carry_data = [[d, v] for d, v in zip(dates, carry) if v is not None]

    mark_lines = [
        {'yAxis': 92,  'lineStyle': {'color': '#3fb950', 'type': 'dashed', 'width': 1},
         'label': {'formatter': 'Entry 92', 'color': '#3fb950', 'fontSize': 9, 'position': 'insideEndTop'}},
        {'yAxis': 98,  'lineStyle': {'color': '#f85149', 'type': 'dashed', 'width': 1},
         'label': {'formatter': 'Exit 98',  'color': '#f85149', 'fontSize': 9, 'position': 'insideEndTop'}},
        {'yAxis': 100, 'lineStyle': {'color': '#8b949e', 'type': 'dashed', 'width': 1},
         'label': {'formatter': 'Parity',   'color': '#8b949e', 'fontSize': 9, 'position': 'insideEndTop'}},
    ]
    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'title': {**_EC_TITLE, 'text': 'VIX/VIX3M Ratio — Carry History'},
        'grid': {'left': 52, 'right': 64, 'top': 50, 'bottom': 40, 'containLabel': True},
        'tooltip': {**_EC_TOOLTIP, 'trigger': 'axis'},
        'legend': _EC_LEGEND,
        'xAxis': {**_EC_AXIS_TIME},
        'yAxis': [
            {**_EC_AXIS_Y, 'name': 'VIX/VIX3M (%)',
             'markLine': {'symbol': ['none', 'none'], 'silent': True, 'data': mark_lines}},
            {**_EC_AXIS_Y, 'name': 'Carry ratio', 'splitLine': {'show': False}},
        ],
        'series': [
            {'type': 'line', 'name': 'VIX/VIX3M', 'data': raw_data, 'symbol': 'none',
             'yAxisIndex': 0, 'lineStyle': {'color': '#58a6ff', 'width': 1.2, 'opacity': 0.5}},
            {'type': 'line', 'name': '10d smoothed', 'data': smo_data, 'symbol': 'none',
             'yAxisIndex': 0, 'lineStyle': {'color': '#00d4aa', 'width': 2}},
            {'type': 'line', 'name': 'Carry ratio', 'data': carry_data, 'symbol': 'none',
             'yAxisIndex': 1, 'lineStyle': {'color': '#d29922', 'width': 1.5, 'type': 'dotted'}},
        ],
    }, 320)


def build_vix_term_structure_figure(futures_strip: list, vix_spot: float, vix3m: float):
    """VIX term structure: spot + futures strip with M1/M2/M3 labels."""
    if futures_strip:
        def _lbl(i, c):
            return f'M{i+1} {c["label"]}' if i < 3 else c['label']
        labels = ['Spot'] + [_lbl(i, c) for i, c in enumerate(futures_strip)]
        levels = [vix_spot] + [c['level'] for c in futures_strip]
        dtes   = [0] + [c['dte'] for c in futures_strip]
        colors = [_C['text']] + [
            _C['positive'] if levels[i] >= levels[i-1] else _C['negative']
            for i in range(1, len(levels))
        ]
    else:
        vix_2m = vix_spot + (vix3m - vix_spot) * (60 / 93)
        dtes   = [0, 30, 60, 93]
        levels = [vix_spot, vix_spot, vix_2m, vix3m]
        labels = ['Spot', 'VIX 1M', 'VIX 2M', 'VIX3M']
        colors = [_C['text'], _C['blue'], _C['accent'], _C['accent']]

    scatter_data = [{'value': [dtes[i], round(float(levels[i]), 4)],
                     'symbol': 'circle', 'symbolSize': 9,
                     'itemStyle': {'color': colors[i]},
                     'label': {'show': True, 'formatter': labels[i],
                               'position': 'top', 'color': '#8b949e', 'fontSize': 9}}
                    for i in range(len(dtes))]
    # Auto-range y-axis so a shallow contango is visually apparent (not squashed to 0-baseline)
    _lmin = min(float(v) for v in levels)
    _lmax = max(float(v) for v in levels)
    _pad  = max(0.5, (_lmax - _lmin) * 0.5)
    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'title': {**_EC_TITLE, 'text': 'VIX Futures Term Structure'},
        'grid': _EC_GRID,
        'tooltip': {**_EC_TOOLTIP, 'trigger': 'item'},
        'xAxis': {**_EC_AXIS_X, 'name': 'Days to expiry', 'nameLocation': 'middle', 'nameGap': 28},
        'yAxis': {**_EC_AXIS_Y, 'name': 'VIX level', 'nameLocation': 'middle', 'nameGap': 36,
                  'min': round(_lmin - _pad, 1), 'max': round(_lmax + _pad, 1)},
        'series': [
            {'type': 'line', 'data': [[dtes[i], round(float(levels[i]), 4)] for i in range(len(dtes))],
             'symbol': 'none', 'lineStyle': {'color': '#00d4aa', 'width': 2},
             'z': 1},
            {'type': 'scatter', 'data': scatter_data, 'z': 2},
        ],
    }, 290)


def build_vix_percentile_figure(df: pd.DataFrame, percentiles: dict):
    """Contango percentile history with current level line."""
    if df is None or df.empty:
        return _ec_empty('No VIX data', 240)

    pct_series = (df['vix_ratio'].rank(pct=True) * 100).fillna(50)
    dates = [str(d)[:10] for d in df.index]
    pct_data = [[d, round(float(v), 2)] for d, v in zip(dates, pct_series)]
    current_pct = round(float(percentiles.get('vix_ratio_pct', 50)), 1)

    mark_lines = [{'yAxis': current_pct,
                   'lineStyle': {'color': '#00d4aa', 'type': 'dashed', 'width': 1.5},
                   'label': {'formatter': f'Now: P{current_pct:.0f}',
                             'color': '#00d4aa', 'fontSize': 10, 'position': 'insideEndTop'}}]
    for y in [25, 50, 75]:
        mark_lines.append({'yAxis': y, 'lineStyle': {'color': '#2d3748', 'type': 'dotted', 'width': 1},
                           'label': {'show': False}})
    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'title': {**_EC_TITLE, 'text': 'VIX/VIX3M — Historical Percentile'},
        'grid': _EC_GRID, 'tooltip': _EC_TOOLTIP,
        'xAxis': {**_EC_AXIS_TIME},
        'yAxis': {**_EC_AXIS_Y, 'name': 'Percentile', 'min': 0, 'max': 100,
                  'markLine': {'symbol': ['none', 'none'], 'silent': True, 'data': mark_lines}},
        'series': [{'type': 'line', 'data': pct_data, 'symbol': 'none',
                    'lineStyle': {'color': '#58a6ff', 'width': 1.5},
                    'areaStyle': {'color': 'rgba(88,166,255,0.08)'}}],
    }, 240)


def build_vix_roll_cost_figure(df: pd.DataFrame):
    """Monthly roll cost (UVXY) and yield (SVXY) over time."""
    if df is None or df.empty:
        return _ec_empty('No VIX data', 240)

    dates = [str(d)[:10] for d in df.index]
    cost  = [[d, round(float(v), 4) if pd.notna(v) else None] for d, v in zip(dates, df['uvxy_monthly_cost'])]
    yield_ = [[d, round(float(v), 4) if pd.notna(v) else None] for d, v in zip(dates, df['svxy_monthly_yield'])]
    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'title': {**_EC_TITLE, 'text': 'Estimated Monthly Roll Cost / Yield'},
        'grid': _EC_GRID, 'tooltip': _EC_TOOLTIP,
        'legend': _EC_LEGEND,
        'xAxis': {**_EC_AXIS_TIME},
        'yAxis': {**_EC_AXIS_Y, 'name': '%/month', 'nameLocation': 'middle', 'nameGap': 42},
        'series': [
            {'type': 'line', 'name': 'UVXY cost (%)', 'data': cost, 'symbol': 'none',
             'lineStyle': {'color': '#f85149', 'width': 1.5},
             'areaStyle': {'color': 'rgba(248,81,73,0.07)'}},
            {'type': 'line', 'name': 'SVXY yield (%)', 'data': yield_, 'symbol': 'none',
             'lineStyle': {'color': '#3fb950', 'width': 1.5}},
        ],
    }, 240)


def build_vix_outcomes_figure(outcomes: dict):
    """Forward 21-day SVXY return distribution by carry ratio bucket (2-row chart)."""
    if not outcomes:
        return _ec_empty('No outcome data', 380)

    labels, medians, p25s, p75s, spike_pcts, ns = [], [], [], [], [], []
    for label in ['< 0.35', '0.35–0.50', '0.50–0.75', '0.75–1.00', '> 1.00']:
        o = {}
        for key in [label, f'{label}  (off)', f'{label}  (rich)', label]:
            o = outcomes.get(key, o)
            if o.get('n', 0) > 0:
                break
        if not o or not o.get('n'):
            continue
        labels.append(label)
        medians.append(round(float(o['median']), 4))
        p25s.append(round(float(o['p25']), 4))
        p75s.append(round(float(o['p75']), 4))
        spike_pcts.append(round(float(o['spike_pct']), 2))
        ns.append(int(o['n']))

    if not labels:
        return _ec_empty('Insufficient history for outcome analysis', 380)

    bar1_data = [{'value': m,
                  'label': {'show': True, 'formatter': f'N={n}', 'position': 'insideTop' if m < 0 else 'top',
                             'fontSize': 9, 'color': '#8b949e'},
                  'itemStyle': {'color': '#3fb950' if m >= 0 else '#f85149'}}
                 for m, n in zip(medians, ns)]

    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'title': {**_EC_TITLE, 'text': 'Historical Outcomes by Carry Ratio Bucket'},
        'tooltip': _EC_TOOLTIP,
        'legend': {**_EC_LEGEND, 'left': 'center'},
        'grid': [
            {'left': 52, 'right': 20, 'top': '18%', 'height': '48%', 'containLabel': True},
            {'left': 52, 'right': 20, 'top': '72%', 'height': '22%', 'containLabel': True},
        ],
        'xAxis': [
            {**_EC_AXIS_CAT, 'gridIndex': 0, 'data': labels, 'axisLabel': {'show': False}},
            {**_EC_AXIS_CAT, 'gridIndex': 1, 'data': labels,
             'axisLabel': {'color': '#8b949e', 'fontSize': 9, 'rotate': 20}},
        ],
        'yAxis': [
            {**_EC_AXIS_Y, 'gridIndex': 0, 'name': 'SVXY 21d ret (%)', 'nameLocation': 'middle', 'nameGap': 42},
            {**_EC_AXIS_Y, 'gridIndex': 1, 'name': 'Spike %', 'nameLocation': 'middle', 'nameGap': 36},
        ],
        'series': [
            {'type': 'bar', 'name': 'Median return', 'xAxisIndex': 0, 'yAxisIndex': 0,
             'data': bar1_data},
            {'type': 'bar', 'name': 'VIX spike rate', 'xAxisIndex': 1, 'yAxisIndex': 1,
             'data': spike_pcts, 'itemStyle': {'color': '#d29922'}},
        ],
    }, 380)


def build_vix_pca_loadings_figure(pca_signal: dict):
    """Bar chart of PC1 (Level) and PC2 (Slope) loadings across M1–M8.

    Shows which maturity points each principal component loads on most heavily.
    PC1 loads uniformly (parallel shift); PC2 loads short-end negative / long-end
    positive (tilt), confirming Johnson (2017).
    """
    if not pca_signal or not pca_signal.get('is_ready'):
        n   = pca_signal.get('n_obs', 0) if pca_signal else 0
        req = pca_signal.get('min_obs', 60) if pca_signal else 60
        msg = (f'Accumulating strip data ({n}/{req} days)'
               if n < req else 'PCA not available')
        return _ec_empty(msg, 220)

    loadings = pca_signal.get('loadings')
    if loadings is None:
        return _ec_empty('No loadings', 220)

    contracts = list(loadings.columns)
    pc1 = [round(float(v), 4) for v in loadings.iloc[0]]
    pc2 = [round(float(v), 4) for v in loadings.iloc[1]] if len(loadings) > 1 else []
    ev  = pca_signal.get('explained_variance', [])
    ev1 = f"{ev[0]*100:.0f}%" if ev else ''
    ev2 = f"{ev[1]*100:.0f}%" if len(ev) > 1 else ''

    series = [
        {'type': 'bar', 'name': f'PC1 Level ({ev1})',
         'data': pc1, 'itemStyle': {'color': '#2DD4BF'}, 'barGap': '10%'},
    ]
    if pc2:
        series.append({
            'type': 'bar', 'name': f'PC2 Slope ({ev2})',
            'data': pc2, 'itemStyle': {'color': '#BD5DFF'}, 'barGap': '10%',
        })

    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'title': {**_EC_TITLE, 'text': 'PCA Loadings — VX Term Structure (Johnson 2017)'},
        'grid': _EC_GRID,
        'tooltip': _EC_TOOLTIP,
        'legend': _EC_LEGEND,
        'xAxis': {**_EC_AXIS_CAT, 'data': contracts},
        'yAxis': {**_EC_AXIS_Y, 'name': 'Loading'},
        'series': series,
    }, 220)


def build_vix_slope_history_figure(pca_signal: dict):
    """SLOPE factor (PC2) history with percentile bands.

    High SLOPE → steep contango → vol risk premium well-compensated.
    Low SLOPE  → flat / inverted → reduce exposure.
    Source: Johnson (2017) JFQA 52(6).
    """
    if not pca_signal or not pca_signal.get('is_ready'):
        n   = pca_signal.get('n_obs', 0) if pca_signal else 0
        req = pca_signal.get('min_obs', 60) if pca_signal else 60
        msg = (f'Accumulating strip data ({n}/{req} days)'
               if n < req else 'PCA not available')
        return _ec_empty(msg, 240)

    slope_hist = pca_signal.get('slope_history')
    if slope_hist is None or len(slope_hist) == 0:
        return _ec_empty('No SLOPE history', 240)

    dates = [str(d)[:10] for d in slope_hist.index]
    vals  = [round(float(v), 4) if pd.notna(v) else None for v in slope_hist]
    data  = [[d, v] for d, v in zip(dates, vals) if v is not None]

    slope_pct = pca_signal.get('slope_pct', 50)
    current   = pca_signal.get('slope', 0)

    p25 = float(slope_hist.quantile(0.25))
    p75 = float(slope_hist.quantile(0.75))

    mark_lines = [
        {'yAxis': round(current, 4),
         'lineStyle': {'color': '#BD5DFF', 'type': 'dashed', 'width': 1.5},
         'label': {'formatter': f'Now P{slope_pct:.0f}',
                   'color': '#BD5DFF', 'fontSize': 10, 'position': 'insideEndTop'}},
        {'yAxis': round(p75, 4),
         'lineStyle': {'color': '#3fb950', 'type': 'dotted', 'width': 1},
         'label': {'formatter': 'P75', 'color': '#3fb950', 'fontSize': 9,
                   'position': 'insideEndTop'}},
        {'yAxis': round(p25, 4),
         'lineStyle': {'color': '#f85149', 'type': 'dotted', 'width': 1},
         'label': {'formatter': 'P25', 'color': '#f85149', 'fontSize': 9,
                   'position': 'insideEndTop'}},
    ]

    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'title': {**_EC_TITLE, 'text': 'SLOPE Factor (PC2) History — VIX Term Structure'},
        'grid': _EC_GRID, 'tooltip': _EC_TOOLTIP,
        'xAxis': {**_EC_AXIS_TIME},
        'yAxis': {**_EC_AXIS_Y, 'name': 'SLOPE score',
                  'markLine': {'symbol': ['none', 'none'], 'silent': True,
                               'data': mark_lines}},
        'series': [{'type': 'line', 'name': 'SLOPE (PC2)', 'data': data,
                    'symbol': 'none',
                    'lineStyle': {'color': '#BD5DFF', 'width': 1.8},
                    'areaStyle': {'color': 'rgba(189,93,255,0.08)'}}],
    }, 240)


def build_vix_vrp_figure(df: pd.DataFrame, percentiles: dict):
    """Volatility Risk Premium (VIX − realised vol) history.

    VRP > 0  → implied vol exceeds realised → structural vol-selling edge
    VRP < 0  → realised > implied (rare; typically post-spike unwind)

    Source: Carr & Wu (2016), Sepp (2017), Simon & Campasano (2014).
    """
    if df is None or df.empty or 'vrp' not in df.columns:
        return _ec_empty('No VRP data', 260)

    dates   = [str(d)[:10] for d in df.index]
    vrp_raw = [round(float(v), 4) if pd.notna(v) else None for v in df['vrp']]
    vrp_data = [[d, v] for d, v in zip(dates, vrp_raw) if v is not None]

    # VVIX/VIX ratio on secondary axis — tail-risk premium signal
    has_vvix_ratio = 'vvix_vix_ratio' in df.columns
    vvix_data = []
    if has_vvix_ratio:
        vvix_vals = [round(float(v), 3) if pd.notna(v) else None
                     for v in df['vvix_vix_ratio']]
        vvix_data = [[d, v] for d, v in zip(dates, vvix_vals) if v is not None]

    vrp_pct = percentiles.get('vrp_pct', 50)
    vvix_pct = percentiles.get('vvix_vix_pct', 50)

    mark_lines = [
        {'yAxis': 0, 'lineStyle': {'color': '#8b949e', 'type': 'dashed', 'width': 1},
         'label': {'formatter': 'Zero', 'color': '#8b949e', 'fontSize': 9,
                   'position': 'insideEndTop'}},
    ]

    series = [
        {'type': 'line', 'name': 'VRP (VIX−RV)', 'data': vrp_data, 'symbol': 'none',
         'yAxisIndex': 0,
         'lineStyle': {'color': '#BD5DFF', 'width': 1.8},
         'areaStyle': {
             'color': {
                 'type': 'linear', 'x': 0, 'y': 0, 'x2': 0, 'y2': 1,
                 'colorStops': [
                     {'offset': 0, 'color': 'rgba(189,93,255,0.18)'},
                     {'offset': 1, 'color': 'rgba(189,93,255,0.01)'},
                 ],
             }
         }},
    ]
    y_axes = [
        {**_EC_AXIS_Y, 'name': 'VRP (vol pts)',
         'nameLocation': 'middle', 'nameGap': 42,
         'markLine': {'symbol': ['none', 'none'], 'silent': True, 'data': mark_lines}},
    ]
    legend_items = [{'name': 'VRP (VIX−RV)'}]

    if vvix_data:
        series.append({
            'type': 'line', 'name': 'VVIX/VIX ratio', 'data': vvix_data, 'symbol': 'none',
            'yAxisIndex': 1,
            'lineStyle': {'color': '#FACC15', 'width': 1.4, 'type': 'dotted'},
        })
        y_axes.append({
            **_EC_AXIS_Y, 'name': 'VVIX/VIX', 'splitLine': {'show': False},
        })
        legend_items.append({'name': 'VVIX/VIX ratio'})

    return _ec_iframe({
        'backgroundColor': '#0d1117', 'animation': False,
        'title': {**_EC_TITLE, 'text': f'Volatility Risk Premium  ·  VRP P{vrp_pct:.0f}  |  VVIX/VIX P{vvix_pct:.0f}'},
        'grid': {'left': 52, 'right': 64, 'top': 50, 'bottom': 40, 'containLabel': True},
        'tooltip': {**_EC_TOOLTIP, 'trigger': 'axis'},
        'legend': {**_EC_LEGEND, 'data': legend_items},
        'xAxis': {**_EC_AXIS_TIME},
        'yAxis': y_axes,
        'series': series,
    }, 260)
