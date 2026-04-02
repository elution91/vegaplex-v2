from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.models.responses import JobCreated, SchedulerJob
from app.services import scheduler_service

router = APIRouter()


@router.get("/jobs", response_model=list[SchedulerJob])
def list_jobs() -> list[SchedulerJob]:
    return [SchedulerJob(**j) for j in scheduler_service.list_jobs()]


@router.post("/seed-polygon", response_model=JobCreated)
async def trigger_polygon_seed() -> JobCreated:
    ok = scheduler_service.trigger_job_now("polygon_seeder")
    if not ok:
        raise HTTPException(status_code=404, detail="polygon_seeder job not found — is POLYGON_API_KEY set?")
    return JobCreated(job_id="polygon_seeder", message="Polygon seeder triggered")


@router.put("/jobs/{job_id}/pause")
def pause_job(job_id: str) -> dict:
    ok = scheduler_service.pause_job(job_id)
    if not ok:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")
    return {"status": "paused", "job_id": job_id}


@router.put("/jobs/{job_id}/resume")
def resume_job(job_id: str) -> dict:
    ok = scheduler_service.resume_job(job_id)
    if not ok:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")
    return {"status": "resumed", "job_id": job_id}
