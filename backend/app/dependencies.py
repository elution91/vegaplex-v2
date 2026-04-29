from __future__ import annotations

from app.config import get_settings

# ── Lazy singletons — created once, reused for the lifetime of the process ───

_scanner = None
_vix_engine = None
_skew_history = None


def get_scanner():
    global _scanner
    if _scanner is None:
        from vol_skew_scanner import VolatilityScanner, ScannerConfig, DataSource  # noqa: PLC0415
        s = get_settings()
        cfg = ScannerConfig(
            data_source=DataSource(s.data_source),
            ibkr_host=s.ibkr_host,
            ibkr_port=s.ibkr_port,
            ibkr_client_id=s.ibkr_client_id,
        )
        _scanner = VolatilityScanner(cfg)
        _scanner.data_fetcher.connect()   # must connect before any fetch
    return _scanner


def get_vix_engine():
    global _vix_engine
    if _vix_engine is None:
        from vix_futures_engine import VIXFuturesEngine  # noqa: PLC0415
        _vix_engine = VIXFuturesEngine()
    return _vix_engine


def get_skew_history():
    global _skew_history
    if _skew_history is None:
        from skew_history import SkewHistory  # noqa: PLC0415
        _skew_history = SkewHistory()
    return _skew_history
