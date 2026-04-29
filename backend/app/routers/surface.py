from __future__ import annotations

import asyncio

from fastapi import APIRouter, Query

from app.dependencies import get_scanner, get_skew_history
from app.services.chart_service import get_skew_charts, get_skew_dynamics_charts, get_smile_chart, get_surface_charts

router = APIRouter()


@router.get("/{symbol}")
async def get_surface(symbol: str) -> dict:
    result = await asyncio.to_thread(get_scanner().scan_symbol_full, symbol.upper())
    return result or {}


@router.get("/{symbol}/charts")
async def get_surface_charts_endpoint(
    symbol: str,
    option_type: str = Query(default="call", pattern="^(call|put)$"),
) -> dict:
    surface_data = await asyncio.to_thread(get_scanner().scan_symbol_full, symbol.upper())
    if not surface_data:
        return {"error": "No surface data"}
    charts = get_surface_charts(surface_data, option_type=option_type)
    vol_surface = surface_data.get("vol_surface", {})
    key = f"{option_type}_surface"
    expiries = sorted(vol_surface.get(key, {}).get("surfaces_by_expiry", {}).keys())

    # Raw grid arrays for WebGL surface renderer — cleaned and smoothed
    import numpy as np                          # noqa: PLC0415
    from scipy.ndimage import gaussian_filter   # noqa: PLC0415

    s2d = vol_surface.get(key, {}).get("surface_2d")
    raw_surface: dict = {"symbol": symbol.upper(), "option_type": option_type,
                         "strike_grid": None, "tte_grid": None, "surface": None}
    if s2d:
        strikes = s2d.get("strike_grid")
        ttes    = s2d.get("tte_grid")
        surf    = s2d.get("surface")
        if strikes is not None and ttes is not None and surf is not None:
            strikes = np.asarray(strikes, dtype=float)
            ttes    = np.asarray(ttes,    dtype=float)
            surf    = np.asarray(surf,    dtype=float)

            # 1. Clamp IV to sane range (0.01 – 5.0) — removes bad yfinance points
            surf = np.clip(surf, 0.01, 5.0)

            # 2. Winsorise outliers: replace cells > 3 IQR from median with NaN
            med = np.nanmedian(surf)
            q75, q25 = np.nanpercentile(surf[surf > 0], [75, 25])
            iqr = q75 - q25
            surf[(surf > med + 3 * iqr) | (surf < max(0.01, med - 3 * iqr))] = np.nan

            # 3. Fill NaN by linear interpolation along strike axis per expiry row
            for i in range(surf.shape[0]):
                row = surf[i]
                nans = np.isnan(row)
                if nans.all(): continue
                x = np.arange(len(row))
                row[nans] = np.interp(x[nans], x[~nans], row[~nans])

            # 4. Gaussian smooth — sigma=1 gives gentle smoothing without over-blurring
            surf_smooth = gaussian_filter(surf, sigma=1.0)

            surf_list = [[round(float(v), 5) for v in row] for row in surf_smooth]
            raw_surface["strike_grid"] = [round(float(s), 4) for s in strikes]
            raw_surface["tte_grid"]    = [round(float(t), 6) for t in ttes]
            raw_surface["surface"]     = surf_list

    return {"symbol": symbol.upper(), "expiries": expiries, "raw_surface": raw_surface, **charts}


@router.get("/{symbol}/smile")
async def get_smile(symbol: str, expiry: str = Query(...)) -> dict:
    surface_data = await asyncio.to_thread(get_scanner().scan_symbol_full, symbol.upper())
    if not surface_data:
        return {"error": "No surface data"}
    return get_smile_chart(surface_data, expiry)


@router.get("/{symbol}/skew")
async def get_skew(symbol: str, expiry: str = Query(default="")) -> dict:
    surface_data = await asyncio.to_thread(get_scanner().scan_symbol_full, symbol.upper())
    if not surface_data:
        return {"error": "No surface data"}
    skew_data = surface_data.get("skew_metrics", {})
    return get_skew_charts(surface_data, skew_data, expiry)


@router.get("/{symbol}/skew-dynamics")
async def get_skew_dynamics(symbol: str) -> dict:
    skew_hist = get_skew_history()
    sym = symbol.upper()
    skew_ctx, mr_signal = await asyncio.gather(
        asyncio.to_thread(skew_hist.get_context, sym),
        asyncio.to_thread(skew_hist.get_mean_reversion_signal, sym),
    )
    if not skew_ctx:
        return {}
    charts = get_skew_dynamics_charts(skew_ctx)
    if mr_signal:
        charts['mean_reversion'] = mr_signal
    return charts
