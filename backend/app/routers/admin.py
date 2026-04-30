"""
Admin endpoints — long-running maintenance tasks (DB seed, etc.).

Protected by the same shared-password gate as everything under /api/.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger("vegaplex.admin")
router = APIRouter()

_seed_proc: subprocess.Popen | None = None
_seed_log_path = Path("/tmp/vegaplex_seed.log")


def _db_path() -> Path:
    data_dir = os.environ.get("VEGAPLEX_DATA_DIR")
    if data_dir:
        return Path(data_dir) / "skew_history.db"
    return Path(__file__).parent.parent.parent / "analytics" / "skew_history.db"


@router.get("/db/status")
async def db_status():
    """Quick health check of the seeded DB — counts + date range."""
    db = _db_path()
    if not db.exists():
        return {"exists": False, "path": str(db)}
    out: dict = {"exists": True, "path": str(db), "size_mb": round(db.stat().st_size / 1e6, 1)}
    try:
        with sqlite3.connect(str(db)) as c:
            for table in ("iv_history", "skew_history_daily", "skew_snapshots", "vix_strip_history"):
                try:
                    n = c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    out[table] = n
                except sqlite3.OperationalError:
                    out[table] = "(missing table)"
            try:
                row = c.execute(
                    "SELECT MIN(date), MAX(date), COUNT(DISTINCT symbol) FROM iv_history"
                ).fetchone()
                out["iv_history_min"]    = row[0]
                out["iv_history_max"]    = row[1]
                out["iv_history_n_syms"] = row[2]
            except sqlite3.OperationalError:
                pass
    except Exception as e:
        out["query_error"] = str(e)
    return out


@router.post("/seed/start")
async def seed_start(
    start: str = Query(..., description="YYYY-MM-DD"),
    end:   str | None = Query(None, description="YYYY-MM-DD; defaults to today"),
    workers: int = 4,
):
    """
    Kick off the Massive seeder in a background subprocess. Returns immediately.
    Poll /api/admin/seed/status to track progress.

    Requires MASSIVE_ACCESS_KEY + MASSIVE_SECRET_KEY env vars.
    """
    global _seed_proc

    if _seed_proc is not None and _seed_proc.poll() is None:
        raise HTTPException(409, "Seeder already running")

    if not (os.environ.get("MASSIVE_ACCESS_KEY") and os.environ.get("MASSIVE_SECRET_KEY")):
        raise HTTPException(500, "MASSIVE_ACCESS_KEY / MASSIVE_SECRET_KEY env vars not set")

    try:
        datetime.strptime(start, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, f"Invalid start date: {start}")

    end = end or date.today().isoformat()
    try:
        datetime.strptime(end, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, f"Invalid end date: {end}")

    cmd = [
        sys.executable, "analytics/seed_massive.py",
        "--start", start,
        "--end",   end,
        "--workers", str(workers),
    ]

    log_file = open(_seed_log_path, "w")
    _seed_proc = subprocess.Popen(
        cmd,
        cwd="/app",
        stdout=log_file,
        stderr=subprocess.STDOUT,
        env={**os.environ},
    )
    logger.info(f"Started seeder PID {_seed_proc.pid}: {' '.join(cmd)}")

    return {
        "started": True,
        "pid": _seed_proc.pid,
        "start": start,
        "end": end,
        "workers": workers,
        "log": str(_seed_log_path),
    }


@router.get("/seed/status")
async def seed_status(tail: int = Query(50, ge=1, le=500)):
    """Return current seeder process state + last N log lines."""
    global _seed_proc

    state: dict = {"running": False, "pid": None, "exit_code": None}
    if _seed_proc is not None:
        rc = _seed_proc.poll()
        state["pid"] = _seed_proc.pid
        if rc is None:
            state["running"] = True
        else:
            state["exit_code"] = rc

    if _seed_log_path.exists():
        try:
            with open(_seed_log_path, "r") as f:
                lines = f.readlines()
            state["log_tail"] = "".join(lines[-tail:])
            state["log_lines_total"] = len(lines)
        except Exception as e:
            state["log_error"] = str(e)
    else:
        state["log_tail"] = "(no log yet)"

    return state


@router.post("/seed/stop")
async def seed_stop():
    """Kill the running seeder if any."""
    global _seed_proc
    if _seed_proc is None or _seed_proc.poll() is not None:
        return {"running": False, "killed": False}
    _seed_proc.terminate()
    try:
        _seed_proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        _seed_proc.kill()
    return {"running": False, "killed": True}
