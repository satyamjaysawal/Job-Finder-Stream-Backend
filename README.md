# Job Finder Stream — Backend

FastAPI job tracker API: **scraper config**, **MongoDB scrape snapshots**, and **live WebSocket stream**.

| Layer | Stack |
|-------|--------|
| **Backend** | FastAPI + Uvicorn + Pydantic + MongoDB (PyMongo) |
| **Frontend** | Vite + React ([separate repo](https://github.com/satyamjaysawal/Job-Finder-Stream)) |

---

## Live production (Vercel)

| Service | URL |
|---------|-----|
| **Backend API** | https://job-finder-stream-backend.vercel.app |
| **API base** | https://job-finder-stream-backend.vercel.app/api |
| **Health** | https://job-finder-stream-backend.vercel.app/api/health |
| **Swagger docs** | https://job-finder-stream-backend.vercel.app/docs |
| **Frontend** | https://job-finder-stream.vercel.app |

**GitHub:** https://github.com/satyamjaysawal/Job-Finder-Stream-Backend  
**Vercel project:** `job-finder-stream-backend` under [satyam-jaysawals-projects](https://vercel.com/satyam-jaysawals-projects)

---

## Features

- Config in MongoDB (`config` collection: queries, cities, countries, target, etc.)
- Each search creates a new `scrape_jsons` snapshot + jobs collection
- List / get / delete scrape snapshots
- Live WebSocket job stream at `/api/ws/jobs` (best with local Uvicorn)
- CORS driven by `FRONTEND_URL` / `CORS_ORIGINS`
- LinkedIn-style scraping via `python-jobspy` (when available)

---

## File structure

```
backend/
├── app.py                 # FastAPI app (routes, WS, MongoDB)
├── requirements.txt
├── runtime.txt            # Python 3.12 for Vercel
├── vercel.json            # @vercel/python entry → app.py
├── .env.example
├── .vercelignore
└── linkedin_realtime_hyderabad.py   # scraper helper (optional)
```

---

## Environment variables

Copy `.env.example` → `.env` for local development. **Never commit real secrets.**

### Required

| Variable | Example | Purpose |
|----------|---------|---------|
| `MONGODB_URI` | `mongodb+srv://…` | MongoDB Atlas / local URI |
| `DATABASE_NAME` | `job_portal` | Database name |

### Recommended (local + production)

| Variable | Local example | Production example |
|----------|---------------|--------------------|
| `FRONTEND_URL` | `http://127.0.0.1:5173` | `https://job-finder-stream.vercel.app` |
| `CORS_ORIGINS` | _(optional; defaults include localhost Vite ports)_ | `https://job-finder-stream.vercel.app,https://job-finder-stream-satyam-jaysawals-projects.vercel.app` |
| `BASE_URL` | `http://127.0.0.1:5000` | `https://job-finder-stream-backend.vercel.app` |
| `RELOAD` | `true` | `false` |

`FRONTEND_URL` and `CORS_ORIGINS` may be comma-separated. Origins are merged with local Vite defaults for CORS.

### Vercel Production env (set in dashboard or CLI)

```
MONGODB_URI=mongodb+srv://...
DATABASE_NAME=job_portal
FRONTEND_URL=https://job-finder-stream.vercel.app
CORS_ORIGINS=https://job-finder-stream.vercel.app,https://job-finder-stream-satyam-jaysawals-projects.vercel.app
BASE_URL=https://job-finder-stream-backend.vercel.app
RELOAD=false
```

---

## Local setup

**Prerequisites:** Python 3.10+, MongoDB URI.

```powershell
cd backend
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
# create .env with MONGODB_URI + DATABASE_NAME + FRONTEND_URL
uvicorn app:app --reload --host 127.0.0.1 --port 5000
```

| Service | URL |
|---------|-----|
| API | http://127.0.0.1:5000 |
| Swagger | http://127.0.0.1:5000/docs |
| Health | http://127.0.0.1:5000/api/health |

Run the frontend separately on http://127.0.0.1:5173 (proxies `/api` here).

---

## API overview

Base: `/api`  
Local: `http://127.0.0.1:5000/api`  
Production: `https://job-finder-stream-backend.vercel.app/api`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health + non-secret settings |
| GET/PUT | `/config` | Scraper config |
| POST/PUT/DELETE | `/config/queries` | Search queries CRUD |
| POST/PUT/DELETE | `/config/cities` | Cities CRUD |
| POST/PUT/DELETE | `/config/countries` | Countries CRUD |
| POST | `/scrape-jsons/search` | Create new snapshot collection |
| GET | `/scrape-jsons` | List snapshots |
| GET/DELETE | `/scrape-jsons/{id}` | Get / delete snapshot |
| GET | `/db-info` | Mongo connection metadata |
| WS | `/ws/jobs` | Live job stream |

---

## Deploy on Vercel

1. Create/link project **`job-finder-stream-backend`**.
2. Entry is `app.py` via `vercel.json` (`@vercel/python`).
3. Set Production env vars (table above).
4. Deploy:

```powershell
npm i -g vercel
vercel link --yes --project job-finder-stream-backend
vercel env add MONGODB_URI production --value "YOUR_URI" --yes --sensitive
vercel env add DATABASE_NAME production --value "job_portal" --yes
vercel env add FRONTEND_URL production --value "https://job-finder-stream.vercel.app" --yes
vercel env add CORS_ORIGINS production --value "https://job-finder-stream.vercel.app" --yes
vercel env add BASE_URL production --value "https://job-finder-stream-backend.vercel.app" --yes
vercel env add RELOAD production --value "false" --yes
vercel deploy --prod
```

Ensure Git author email is valid for Vercel Git deploys:

```powershell
git config --global user.email "your-email@example.com"
git config --global user.name "Your Name"
```

---

## Vercel notes

- **REST APIs** (health, config, scrape-jsons) work in production with MongoDB Atlas.
- **WebSockets** are not reliable on Vercel serverless — use local Uvicorn for full live stream.
- Background poller is **skipped** when `VERCEL` is set (serverless-friendly).
- Cold starts and **max duration** limits apply to long scrapes.
- Bundle includes heavy deps (`pandas`, `numpy`, `python-jobspy`); keep `venv/` out of deploys (`.vercelignore`).

---

## Dependencies

fastapi, uvicorn[standard], pydantic, python-dotenv, pymongo, dnspython, python-jobspy, pandas, requests, beautifulsoup4, tls-client, markdownify, regex, numpy

See `requirements.txt`.

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `MONGODB_URI is not set` | Set `.env` or Vercel env `MONGODB_URI` |
| CORS blocked from frontend | Add frontend origin to `FRONTEND_URL` / `CORS_ORIGINS`, redeploy |
| API Offline (local) | Start Uvicorn on `127.0.0.1:5000` |
| Live WS fails on Vercel | Expected for serverless; run backend locally for WS |
| Port in use | Change uvicorn `--port` |
| Deploy author rejected | Use a real `git config user.email` matching your GitHub/Vercel account |
