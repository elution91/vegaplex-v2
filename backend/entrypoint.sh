#!/bin/sh
# Ensure the persistent disk directory exists. The skew_history.db itself is
# NOT bundled in the image (too large for git). Seed it manually one time:
#   render shell vegaplex-api
#   python analytics/seed_massive.py --start 2024-04-29 --end <today>
# After that, the DB lives on the persistent disk and survives redeploys.

set -e

if [ -n "$VEGAPLEX_DATA_DIR" ]; then
    mkdir -p "$VEGAPLEX_DATA_DIR"
    if [ ! -f "$VEGAPLEX_DATA_DIR/skew_history.db" ]; then
        echo "[entrypoint] Note: skew_history.db not present yet — run seed_massive.py via Render shell."
    fi
fi

exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
