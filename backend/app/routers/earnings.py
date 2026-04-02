from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from app.models.requests import EarningsScanRequest
from app.models.responses import JobCreated
from app.services.earnings_service import start_earnings_scan, stream_earnings

router = APIRouter()


@router.post("/scan", response_model=JobCreated)
async def scan_earnings(body: EarningsScanRequest) -> JobCreated:
    job_id = await start_earnings_scan(
        settings=None,
        days_ahead=body.days_ahead,
        min_iv_rv_ratio=body.min_iv_rv_ratio,
        data_source=body.data_source,
        ibkr_host=body.ibkr_host,
        ibkr_port=body.ibkr_port,
    )
    return JobCreated(job_id=job_id, message="Earnings scan started")


@router.get("/stream/{job_id}")
async def stream_earnings_endpoint(job_id: str) -> StreamingResponse:
    return StreamingResponse(
        stream_earnings(job_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
