from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from app.dependencies import get_scanner
from app.models.requests import ScanSymbolRequest, ScanUniverseRequest
from app.models.responses import JobCreated, ScanResult
from app.services import scan_service

router = APIRouter()


@router.post("/symbol")
async def scan_symbol(body: ScanSymbolRequest) -> dict:
    return await scan_service.run_symbol_scan(get_scanner(), body.symbol.upper(), body.thresholds)


@router.post("/universe", response_model=JobCreated)
async def scan_universe(body: ScanUniverseRequest) -> JobCreated:
    symbols = [s.upper() for s in body.symbols]
    job_id = await scan_service.start_universe_scan(get_scanner(), symbols, body.thresholds)
    return JobCreated(job_id=job_id, message=f"Universe scan started for {len(symbols)} tickers")


@router.get("/stream/{job_id}")
async def stream_scan(job_id: str) -> StreamingResponse:
    return StreamingResponse(
        scan_service.stream_job(job_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/symbol/{symbol}/detail")
async def symbol_detail(symbol: str) -> dict:
    return await scan_service.run_symbol_scan(get_scanner(), symbol.upper())
