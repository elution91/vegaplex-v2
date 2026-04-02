from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class JobCreated(BaseModel):
    job_id: str
    message: str = "Job started"


class ScanResult(BaseModel):
    symbol: str
    opportunities: list[dict[str, Any]] = []
    surface_data: dict[str, Any] | None = None
    error: str | None = None


class ScanUniverseResult(BaseModel):
    results: list[ScanResult]
    status: str
    opportunities_count: int


class SurfaceCharts(BaseModel):
    surface_3d: dict[str, Any] | None = None   # Plotly JSON (bridge mode)
    smile: dict[str, Any] | None = None         # ECharts option
    term_structure: dict[str, Any] | None = None
    expiries: list[str] = []
    symbol: str = ""


class RegimeResult(BaseModel):
    symbol: str
    regime: str
    sentiment: str
    description: str
    recommendation: str
    metrics: dict[str, Any] = {}
    charts: dict[str, Any] = {}   # spot_vol, correlation, sqrt_t — ECharts options


class SkewDynamicsResult(BaseModel):
    symbol: str
    context: dict[str, Any] = {}
    signal: dict[str, Any] = {}
    synthesis: str = ""
    charts: dict[str, Any] = {}   # slope, fwd, vol, sticky — ECharts options


class RadarResult(BaseModel):
    universe_table: list[dict[str, Any]] = []
    summary_table: list[dict[str, Any]] = []
    charts: dict[str, Any] = {}   # scatter, iv_rv, persistence — ECharts options


class VIXResult(BaseModel):
    metrics: dict[str, Any] = {}
    percentiles: dict[str, Any] = {}
    kpis: dict[str, Any] = {}
    carry_on: bool = True
    strategy_banner: dict[str, Any] = {}
    synthesis: str = ""
    charts: dict[str, Any] = {}   # term_structure, ratio_history, … — ECharts options


class EarningsRow(BaseModel):
    ticker: str
    date: str
    direction: str
    bennett_move: float | None = None
    iv_rv_ratio: float | None = None
    structure: str = ""
    spread_signal: str = ""
    confidence: float | None = None
    extra: dict[str, Any] = {}


class EarningsScanResult(BaseModel):
    rows: list[EarningsRow] = []
    status: str = ""


class SchedulerJob(BaseModel):
    id: str
    name: str
    next_run: str | None
    trigger: str
    paused: bool


class BrokerTestResult(BaseModel):
    connected: bool
    message: str
