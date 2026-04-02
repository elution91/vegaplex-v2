from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from app.dependencies import get_scanner
from app.models.requests import ScanSymbolRequest, ScanUniverseRequest
from app.models.responses import JobCreated, ScanResult
from app.services import scan_service

router = APIRouter()


@router.post("/symbol", response_model=ScanResult)
async def scan_symbol(
    body: ScanSymbolRequest,
    scanner: Annotated[Any, Depends(get_scanner)],
) -> ScanResult:
    result = await scan_service.run_symbol_scan(scanner, body.symbol.upper())
    return ScanResult(**result)


@router.post("/universe", response_model=JobCreated)
async def scan_universe(
    body: ScanUniverseRequest,
    scanner: Annotated[Any, Depends(get_scanner)],
) -> JobCreated:
    symbols = [s.upper() for s in body.symbols]
    job_id = await scan_service.start_universe_scan(scanner, symbols)
    return JobCreated(job_id=job_id, message=f"Universe scan started for {len(symbols)} tickers")


@router.get("/stream/{job_id}")
async def stream_scan(job_id: str) -> StreamingResponse:
    return StreamingResponse(
        scan_service.stream_job(job_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # disable nginx buffering
        },
    )


@router.get("/symbol/{symbol}/detail")
async def symbol_detail(
    symbol: str,
    scanner: Annotated[Any, Depends(get_scanner)],
) -> dict:
    result = await scan_service.run_symbol_scan(scanner, symbol.upper())
    return result
