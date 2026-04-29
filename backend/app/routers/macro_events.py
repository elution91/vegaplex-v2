from __future__ import annotations

import asyncio
from fastapi import APIRouter, Query

router = APIRouter()


@router.get("")
async def get_macro_events(days_ahead: int = Query(default=60, ge=7, le=120)) -> list[dict]:
    from macro_event_vol import get_macro_event_vols  # noqa: PLC0415
    return await asyncio.to_thread(get_macro_event_vols, days_ahead)
