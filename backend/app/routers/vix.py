from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends

from app.dependencies import get_vix_engine
from app.services.vix_service import get_vix_data

router = APIRouter()


@router.get("")
async def get_vix(
    vix_engine: Annotated[Any, Depends(get_vix_engine)],
) -> dict:
    return await get_vix_data(vix_engine)
