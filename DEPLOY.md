# Deployment — Vercel + Render

Backend (FastAPI) deploys to **Render**, frontend (Vite) deploys to **Vercel**.
Total runtime cost: ~$7/mo (Render Starter; Vercel Hobby free).

## One-time setup

### 1. Push to GitHub
Both Vercel and Render deploy from a Git repo.
Make sure `backend/.env` and any secret files are gitignored (already done).

### 2. Backend → Render

1. Sign in at https://render.com → **New + → Blueprint**
2. Connect the repo. Render reads `render.yaml` automatically.
3. When prompted, set the secret env vars in the dashboard:
   - `VEGAPLEX_AUTH_PASSWORD` — choose a strong shared password (12+ chars)
   - `VEGAPLEX_CORS_ORIGINS` — set after Vercel deploys, e.g.
     `https://vegaplex.vercel.app,https://your-vercel-url.vercel.app`
4. Click **Apply**. First build takes ~5 min.
5. Once green, note the public URL: `https://vegaplex-api.onrender.com`
6. Verify: `curl https://vegaplex-api.onrender.com/api/auth/status`
   should return `{"auth_required": true}`.
7. **Seed the historical DB** (one-time): the `skew_history.db` is too big for
   git, so seed it directly on Render's persistent disk:
   - Render dashboard → vegaplex-api → Shell tab → Connect
   - Add `MASSIVE_ACCESS_KEY` + `MASSIVE_SECRET_KEY` to env vars temporarily
   - Run: `python analytics/seed_massive.py --start 2024-04-29 --end $(date -u +%Y-%m-%d) --workers 4`
   - Takes ~50 min. After it finishes, you can remove the Massive keys.

### 3. Frontend → Vercel

1. Sign in at https://vercel.com → **Add New → Project**
2. Import the same repo. Set:
   - **Root Directory**: `frontend`
   - **Framework Preset**: Vite (auto-detected)
3. Under **Environment Variables**, add:
   - `VITE_API_URL` = `https://vegaplex-api.onrender.com` (your Render URL)
4. Deploy. Takes ~1-2 min.
5. Note the URL: `https://vegaplex.vercel.app`
6. Go back to Render dashboard → vegaplex-api → Environment → update
   `VEGAPLEX_CORS_ORIGINS` to include the Vercel URL.

### 4. Hand out the password

Send beta users:
- URL: `https://vegaplex.vercel.app`
- Password: (whatever you set for `VEGAPLEX_AUTH_PASSWORD`)

Sessions persist for 30 days via cookie. Rotate the password by updating
the env var in Render — all sessions invalidate immediately.

## Updating

- **Frontend changes**: `git push` → Vercel auto-deploys.
- **Backend changes**: `git push` → Render auto-deploys.
- **Database backfill**: run `seed_massive.py` from your laptop. The persistent
  disk on Render keeps growing via the live yfinance write-through that
  fires on every Radar/Vol Desk scan.

## What's NOT deployed

- The Massive seeder cron (manual runs from your laptop for now)
- Per-user accounts / login (shared password only)
- Custom domain (use Vercel/Render subdomains)

These are tractable v2 additions when needed.

## Local development

Unchanged. Backend: `uvicorn app.main:app --reload --port 8000`.
Frontend: `npm run dev`. With no `VEGAPLEX_AUTH_PASSWORD` set, auth is
disabled and the login page is bypassed.
