from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from fastapi import Depends

from app.config import Settings, get_settings

# ── Lazy singleton imports (analytics modules live in backend/analytics/) ────


@lru_cache(maxsize=1)
def _get_scanner(settings: Settings):
    from vol_skew_scanner import VolatilityScanner  # noqa: PLC0415
    return VolatilityScanner(data_source=settings.data_source)


@lru_cache(maxsize=1)
def _get_vix_engine(settings: Settings):
    from vix_futures_engine import VIXFuturesEngine  # noqa: PLC0415
    return VIXFuturesEngine()


@lru_cache(maxsize=1)
def _get_skew_history(settings: Settings):
    from skew_history import SkewHistory  # noqa: PLC0415
    return SkewHistory()


def get_scanner(settings: Annotated[Settings, Depends(get_settings)]):
    return _get_scanner(settings)


def get_vix_engine(settings: Annotated[Settings, Depends(get_settings)]):
    return _get_vix_engine(settings)


def get_skew_history(settings: Annotated[Settings, Depends(get_settings)]):
    return _get_skew_history(settings)
