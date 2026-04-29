from __future__ import annotations

import asyncio

from fastapi import APIRouter, Query

from app.dependencies import get_scanner
from app.models.requests import BrokerTestRequest
from app.models.responses import BrokerTestResult

router = APIRouter()


@router.post("/test", response_model=BrokerTestResult)
async def test_broker(body: BrokerTestRequest) -> BrokerTestResult:
    if body.broker == "yfinance":
        return BrokerTestResult(connected=True, message="yfinance requires no connection")
    try:
        from ibkr_fetcher import IBKRFetcher  # noqa: PLC0415
        fetcher = IBKRFetcher(host=body.host, port=body.port, client_id=body.client_id)
        ok = await asyncio.to_thread(fetcher.test_connection)
        return BrokerTestResult(
            connected=bool(ok),
            message="Connected" if ok else "Connection failed — check TWS/Gateway is running",
        )
    except Exception as exc:
        return BrokerTestResult(connected=False, message=str(exc))


@router.get("/options-chain/{symbol}")
async def get_options_chain(
    symbol: str,
    expiry: str = Query(default=""),
) -> dict:
    try:
        from ibkr_fetcher import IBKRFetcher  # noqa: PLC0415
        fetcher: IBKRFetcher = get_scanner().ibkr_fetcher
        chain = await asyncio.to_thread(
            fetcher.get_options_for_bennett, symbol.upper(), expiry=expiry or None
        )
        return {"symbol": symbol.upper(), "chain": chain or [], "count": len(chain or [])}
    except Exception as exc:
        return {"symbol": symbol.upper(), "error": str(exc), "chain": []}
