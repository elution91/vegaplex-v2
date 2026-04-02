# νegaPlex v2 — Volatility Surface Intelligence

FastAPI + React rewrite of the v1 Dash dashboard.

## Stack

| Layer | Tech |
|-------|------|
| Backend | FastAPI + uvicorn + APScheduler |
| Frontend | React 18 + Vite + TypeScript + Tailwind CSS |
| Charting | Apache ECharts (`echarts-for-react`), Plotly bridge for 3D surface |
| State | Zustand + TanStack Query |
| Streaming | Server-Sent Events (SSE) for scan progress |

## Structure

```
backend/
  app/            ← FastAPI app (routers, services, models, core)
  analytics/      ← All v1 analytics modules (unchanged)
frontend/
  src/
    api/          ← Axios client + SSE wrapper
    store/        ← Zustand slices
    hooks/        ← useScan, useVix, useEarnings
    components/   ← EChart, Surface3D, ChartCard, tables, shared
    views/        ← RadarView, VolDeskView, ResultsView, VIXView, EarningsView
```

## Running (development)

### Backend

```bash
cd backend
pip install -e ".[dev]"
cp analytics/config.example.json analytics/config.json
uvicorn app.main:app --reload --port 8000
```

### Frontend

```bash
cd frontend
npm install
npm run dev
# → http://localhost:5173  (proxied to backend at :8000)
```

## Key endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/scan/symbol` | Single-symbol scan |
| `POST` | `/api/scan/universe` | Start universe scan → returns job_id |
| `GET`  | `/api/scan/stream/{job_id}` | SSE stream of scan progress |
| `GET`  | `/api/surface/{sym}/charts` | IV surface + smile + term structure charts |
| `GET`  | `/api/regime/{sym}/charts` | Regime classification + charts |
| `GET`  | `/api/vix` | Full VIX futures data + all charts |
| `POST` | `/api/earnings/scan` | Start earnings scan → returns job_id |
| `GET`  | `/api/earnings/stream/{job_id}` | SSE stream of earnings scan |
| `GET`  | `/api/scheduler/jobs` | APScheduler job list |

Interactive docs: `http://127.0.0.1:8000/docs`

## Migration status

- [x] Backend skeleton (FastAPI, routers, services, models)
- [x] All analytics modules ported (unchanged from v1)
- [x] SSE scan streaming
- [x] Frontend shell (Vite + Tailwind + Zustand + TanStack Query)
- [x] VIX tab (fully wired)
- [x] Results tab (scan progress + opportunities table + drill-down)
- [x] Vol Desk (Surface / Skew / Regime sub-tabs)
- [x] Earnings tab (SSE progress + table)
- [x] Radar tab
- [ ] ECharts GL port for 3D surface (currently Plotly bridge)
- [ ] Skew Dynamics sub-tab (full panel)
- [ ] Settings / Universe panel UI
- [ ] IBKR BNTT Δ/RICH/REC debug endpoint wired to frontend

## Disclaimer

For informational and educational purposes only. Not financial advice. Options trading involves substantial risk of loss.

---
*νegaPlex v2 — volatility surface intelligence*
