from __future__ import annotations

"""
Thin wrapper that calls dashboard_helpers builders and returns plain dicts.
All builders already return ECharts option dicts — we just JSON-encode them
through FastAPI's jsonable_encoder.  The 3D surface stays as Plotly JSON
(bridge mode) until echarts-gl port in a later phase.
"""

from typing import Any


def get_surface_charts(surface_data: dict[str, Any], option_type: str = "call") -> dict[str, Any]:
    from dashboard_helpers import (  # noqa: PLC0415
        build_3d_surface_figure,
        build_smile_curves_figure,
        build_term_structure_figure,
    )
    return {
        "surface_3d": build_3d_surface_figure(surface_data, option_type=option_type),
        "smile": build_smile_curves_figure(surface_data),
        "term_structure": build_term_structure_figure(surface_data),
    }


def get_smile_chart(surface_data: dict[str, Any], expiry: str) -> dict[str, Any]:
    from dashboard_helpers import build_iv_smile_figure  # noqa: PLC0415
    return build_iv_smile_figure(surface_data, expiry)


def get_skew_charts(surface_data: dict[str, Any], skew_data: dict[str, Any], expiry: str) -> dict[str, Any]:
    from dashboard_helpers import build_skew_charts  # noqa: PLC0415
    return build_skew_charts(surface_data, skew_data, expiry)


def get_regime_charts(regime_result: dict[str, Any], price_history: Any) -> dict[str, Any]:
    from dashboard_helpers import (  # noqa: PLC0415
        build_regime_correlation_figure,
        build_regime_spot_vol_figure,
        build_regime_sqrt_t_figure,
    )
    return {
        "spot_vol": build_regime_spot_vol_figure(regime_result, price_history),
        "correlation": build_regime_correlation_figure(regime_result),
        "sqrt_t": build_regime_sqrt_t_figure(regime_result),
    }


def get_skew_dynamics_charts(skew_ctx: dict[str, Any]) -> dict[str, Any]:
    from dashboard_helpers import build_skew_dynamics_charts  # noqa: PLC0415
    return build_skew_dynamics_charts(skew_ctx)


def get_vix_charts(vix_data: dict[str, Any]) -> dict[str, Any]:
    from dashboard_helpers import (  # noqa: PLC0415
        build_vix_carry_ratio_figure,
        build_vix_outcomes_figure,
        build_vix_pca_figure,
        build_vix_roll_cost_figure,
        build_vix_slope_history_figure,
        build_vix_term_structure_figure,
        build_vix_vrp_figure,
        build_vix_percentile_figure,
    )
    strip = vix_data.get("strip", [])
    history = vix_data.get("history", {})
    return {
        "term_structure":  build_vix_term_structure_figure(strip),
        "ratio_history":   build_vix_carry_ratio_figure(history),
        "vrp":             build_vix_vrp_figure(history),
        "pca":             build_vix_pca_figure(vix_data),
        "slope_history":   build_vix_slope_history_figure(history),
        "roll_cost":       build_vix_roll_cost_figure(strip),
        "outcomes":        build_vix_outcomes_figure(history),
        "percentile":      build_vix_percentile_figure(vix_data),
    }


def get_radar_charts(radar_data: dict[str, Any]) -> dict[str, Any]:
    """
    Radar chart builders were private functions in dashboard.py.
    They are extracted here in v2.
    """
    from dashboard_helpers import (  # noqa: PLC0415
        build_radar_scatter_figure,
        build_iv_rv_figure,
        build_persistence_figure,
    )
    return {
        "scatter":     build_radar_scatter_figure(radar_data),
        "iv_rv":       build_iv_rv_figure(radar_data),
        "persistence": build_persistence_figure(radar_data),
    }


def get_payoff_chart(legs: list[dict[str, Any]], spot: float) -> dict[str, Any]:
    from dashboard_helpers import build_payoff_figure  # noqa: PLC0415
    return build_payoff_figure(legs, spot)
