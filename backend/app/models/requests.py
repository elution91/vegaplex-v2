from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class ScanSymbolRequest(BaseModel):
    symbol: str
    data_source: Literal["yfinance", "ibkr"] | None = None


class ScanUniverseRequest(BaseModel):
    symbols: list[str] = Field(min_length=1)
    data_source: Literal["yfinance", "ibkr"] | None = None


class RegimeClassifyRequest(BaseModel):
    symbol: str
    lookback: int = 252


class RegimeUniverseRequest(BaseModel):
    symbols: list[str]
    lookback: int = 252


class RadarRequest(BaseModel):
    symbols: list[str]
    lookback: int = 252


class EarningsScanRequest(BaseModel):
    days_ahead: int = Field(default=7, ge=1, le=30)
    min_iv_rv_ratio: float = 0.8
    data_source: Literal["yfinance", "ibkr"] = "yfinance"
    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 7497


class BrokerTestRequest(BaseModel):
    broker: Literal["ibkr", "yfinance"] = "ibkr"
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 2
