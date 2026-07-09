# Job Portal Tracker

Full-stack job dashboard: **scraper settings** + **scrape JSON snapshots** in **MongoDB**.  
Each search creates a **new JSON document**. Select any snapshot, view jobs, or delete it.

| Layer | Stack | Port |
|-------|--------|------|
| **Backend** | FastAPI + Uvicorn + Pydantic + MongoDB | **5000** |
| **Frontend** | Vite 7 + React 19 + Redux Toolkit + Tailwind CSS 4 | **5173** |

---

## Features

- Config stored in MongoDB (queries, cities, countries, target, etc.)
- Scrape JSON runs — every search inserts a new `scrape_jsons` document
- Select / view / delete any saved snapshot
- Live WebSocket job stream
- Redux Toolkit global state
- Dark & light themes

---

## File structure

```
job_portal/
├── README.md
├── backend/
│   ├── .env                  # MONGODB_URI + DATABASE_NAME
│   ├── app.py                # FastAPI app
│   ├── requirements.txt
│   └── venv/                 # local Python env (not committed)
└── frontend/
    ├── package.json
    ├── vite.config.js        # port 5173, /api proxy → :5000
    ├── .env / .env.example
    ├── index.html
    └── src/
        ├── main.jsx
        ├── App.jsx
        ├── index.css
        ├── components/
        ├── pages/
        ├── store/
        └── utils/
```

### Ports

| Service | URL | Notes |
|---------|-----|--------|
| Frontend (Vite) | http://127.0.0.1:5173 | `npm run dev` |
| Backend API | http://127.0.0.1:5000 | `uvicorn …` |
| API via proxy | http://127.0.0.1:5173/api/… | same-origin in browser |
| Swagger | http://127.0.0.1:5000/docs | FastAPI docs |

---

## Prerequisites

- Python 3.10+
- Node.js 20+ and npm
- MongoDB URI (Atlas or local)

---

## Setup & run

### Backend (port 5000)

```powershell
cd backend
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
# ensure .env has MONGODB_URI + DATABASE_NAME
uvicorn app:app --reload --host 127.0.0.1 --port 5000
```

### Frontend (port 5173)

```powershell
cd frontend
npm install
npm run dev
```

Open: http://127.0.0.1:5173

---

## API overview

Base: `http://127.0.0.1:5000/api` (or via Vite proxy `/api`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health |
| GET/PUT | `/config` | Scraper config (single `config` collection) |
| POST/PUT/DELETE | `/config/queries` | Add / edit / remove search queries |
| POST/PUT/DELETE | `/config/cities` | Add / edit / remove cities |
| POST/PUT/DELETE | `/config/countries` | Add / edit / remove countries |
| POST | `/scrape-jsons/search` | Create new snapshot collection |
| GET | `/scrape-jsons` | List snapshots |
| GET/DELETE | `/scrape-jsons/{id}` | Get / delete snapshot |
| WS | `/ws/jobs` | Live job stream |

---

## Dependencies

**Backend:** fastapi, uvicorn[standard], pymongo, dnspython, pydantic, python-dotenv  

**Frontend:** react, react-dom, @reduxjs/toolkit, react-redux, vite, tailwindcss, @tailwindcss/vite, @vitejs/plugin-react

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `MONGODB_URI must be set` | Set `backend/.env` |
| API Offline | Start Uvicorn on `:5000` |
| Proxy fail | Start backend before `npm run dev` |
| Port in use | Change `VITE_PORT` or uvicorn `--port` |
