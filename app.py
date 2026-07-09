"""
Job Portal API (FastAPI + Uvicorn)

MongoDB-backed job tracker:
  - Each Search / Start Live Stream → NEW collection (`live_stream_<timestamp>`)
  - Metadata row in `scrape_jsons` (Dashboard lists every run)
  - Live Stream enforces strict parameter caps (target, results_per, hours_old)

Configuration lives in `settings.py` and is loaded with python-dotenv
(`load_dotenv` / `find_dotenv` — no hard-coded `.env` paths).
"""

from __future__ import annotations

import asyncio
import math
import random
import re
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Any, Callable, Optional
from urllib.parse import urlparse

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument

import os
import sys
from dotenv import find_dotenv, load_dotenv

# Load environment
load_dotenv(find_dotenv(), override=False)

# Server Configuration
HOST: str = os.getenv("HOST", "127.0.0.1")
PORT: int = int(os.getenv("PORT", "5000"))
BASE_URL: str = os.getenv("BASE_URL", f"http://{HOST}:{PORT}").rstrip("/")
API_PREFIX: str = os.getenv("API_PREFIX", "/api").rstrip("/") or "/api"
RELOAD: bool = os.getenv("RELOAD", "true").lower() in {"1", "true", "yes", "on"}

# MongoDB Configuration
MONGODB_URI: str | None = os.getenv("MONGODB_URI")
DATABASE_NAME: str | None = os.getenv("DATABASE_NAME", "job_portal")

# Collection names
JOBS_COLLECTION_NAME: str = "jobs"
CONFIG_COLLECTION_NAME: str = "config"
SCRAPE_JSONS_COLLECTION_NAME: str = "scrape_jsons"

CONFIG_DOC_KEY: str = "scraper_settings"
CONFIG_FILTER: dict = {"_key": CONFIG_DOC_KEY}

RESERVED_COLLECTIONS: frozenset[str] = frozenset(
    {
        JOBS_COLLECTION_NAME,
        CONFIG_COLLECTION_NAME,
        SCRAPE_JSONS_COLLECTION_NAME,
        "system.indexes",
    }
)

# CORS Configuration (localhost Vite ports + FRONTEND_URL / CORS_ORIGINS)
_default_origins = [
    "http://127.0.0.1:5173",
    "http://localhost:5173",
    "http://127.0.0.1:4173",
    "http://localhost:4173",
]


def _parse_origin_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [o.strip().rstrip("/") for o in raw.split(",") if o.strip()]


# FRONTEND_URL may be a single origin or comma-separated list (production Vercel URL)
FRONTEND_URL: str = (os.getenv("FRONTEND_URL") or "http://127.0.0.1:5173").strip().rstrip("/")
_env_cors = _parse_origin_list(os.getenv("CORS_ORIGINS"))
_frontend_origins = _parse_origin_list(os.getenv("FRONTEND_URL"))

# Merge env origins with local defaults (dedupe, preserve order)
_seen: set[str] = set()
CORS_ORIGINS: list[str] = []
for o in _env_cors + _frontend_origins + _default_origins:
    if o and o not in _seen:
        _seen.add(o)
        CORS_ORIGINS.append(o)

CORS_ALLOW_CREDENTIALS: bool = True
CORS_ALLOW_METHODS: list[str] = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
CORS_ALLOW_HEADERS: list[str] = ["Authorization", "Content-Type", "Accept", "Origin", "X-Requested-With"]

def require_mongo_settings() -> None:
    """Raise error if MongoDB URI is missing."""
    if not MONGODB_URI:
        raise RuntimeError(
            "MONGODB_URI is not set. Please add it to your .env file."
        )

def configure_stdio_utf8() -> None:
    """Prefer UTF-8 on Windows consoles."""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8")
            except Exception:
                pass

def public_settings_dict() -> dict:
    """Non-secret settings safe to expose via API or health endpoints."""
    return {
        "base_url": BASE_URL,
        "api_prefix": API_PREFIX,
        "host": HOST,
        "port": PORT,
        "frontend_url": FRONTEND_URL,
        "cors_origins": CORS_ORIGINS,
        "database": DATABASE_NAME,
    }

# Optional LinkedIn scraper
try:
    from jobspy import scrape_jobs as jobspy_scrape_jobs
except ImportError:  # pragma: no cover
    jobspy_scrape_jobs = None

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
configure_stdio_utf8()
require_mongo_settings()

MONGO_URI = MONGODB_URI  # alias used throughout this module

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
jobs_col = db[JOBS_COLLECTION_NAME]
config_col = db[CONFIG_COLLECTION_NAME]
scrape_jsons_col = db[SCRAPE_JSONS_COLLECTION_NAME]


def _parse_mongo_host_info(uri: str) -> dict[str, str]:
    """
    Extract safe (non-secret) connection metadata from MONGODB_URI.
    Returns provider, cluster/host labels for the Realtime Feed header.
    """
    raw = (uri or "").strip()
    provider = "MongoDB"
    cluster_name = "local"
    host = "localhost"
    scheme = "mongodb"

    try:
        # Hide credentials before parse edge-cases: mongodb+srv://user:pass@host/...
        no_creds = re.sub(r"://([^:/@]+):([^@]+)@", r"://***:***@", raw)
        parsed = urlparse(raw if "://" in raw else f"mongodb://{raw}")
        scheme = (parsed.scheme or "mongodb").lower()
        netloc = parsed.netloc or ""
        # strip userinfo
        if "@" in netloc:
            netloc = netloc.rsplit("@", 1)[-1]
        host = netloc.split(",")[0].split(":")[0].strip() or "localhost"

        if "mongodb.net" in host.lower() or scheme == "mongodb+srv":
            provider = "MongoDB Atlas"
            # e.g. cluster0.abc123.mongodb.net → cluster0
            cluster_name = host.split(".")[0] if host else "atlas"
        elif host in ("localhost", "127.0.0.1"):
            provider = "MongoDB Local"
            cluster_name = "local"
        else:
            provider = "MongoDB"
            cluster_name = host.split(".")[0] if host else "remote"

        _ = no_creds  # kept for future logging without secrets
    except Exception:
        pass

    return {
        "provider": provider,
        "cluster_name": cluster_name,
        "host": host,
        "scheme": scheme,
    }


def get_db_meta(
    *,
    session_saved: int = 0,
    session_id: Optional[str] = None,
    collection_name: Optional[str] = None,
    scrape_json_id: Optional[str] = None,
) -> dict[str, Any]:
    """Live MongoDB metadata shown at the top of Realtime Feed."""
    host_info = _parse_mongo_host_info(MONGO_URI or "")
    active_col = (collection_name or "").strip() or JOBS_COLLECTION_NAME
    try:
        total_count = db[active_col].count_documents({})
    except Exception:
        total_count = 0
    try:
        client.admin.command("ping")
        connected = True
    except Exception:
        connected = False

    return {
        "provider": host_info["provider"],
        "mongodb": host_info["provider"],
        "cluster_name": host_info["cluster_name"],
        "host": host_info["host"],
        "scheme": host_info["scheme"],
        "database": DATABASE_NAME,
        "dbname": DATABASE_NAME,
        "collection": active_col,
        "collection_name": active_col,
        "total_count": total_count,
        "session_saved": session_saved,
        "session_id": session_id,
        "scrape_json_id": scrape_json_id,
        "connected": connected,
        "unique_index": "job_url",
    }


def _ensure_indexes() -> None:
    try:
        jobs_col.create_index([("job_url", ASCENDING)], unique=True)
    except Exception:
        pass
    try:
        config_col.create_index([("_key", ASCENDING)], unique=True)
    except Exception:
        pass
    try:
        scrape_jsons_col.create_index([("created_at", DESCENDING)])
        scrape_jsons_col.create_index([("name", ASCENDING)])
    except Exception:
        pass


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Startup: indexes + config seed + optional background poller. Shutdown: cancel poller."""
    # On Vercel serverless, skip long-lived background tasks (cold starts / no WS).
    is_serverless = bool(os.getenv("VERCEL") or os.getenv("AWS_LAMBDA_FUNCTION_NAME"))
    try:
        _ensure_indexes()
        ensure_config_document()
    except Exception as e:
        print(f"Startup init warning: {e}")
    polling_task = None
    if not is_serverless:
        polling_task = asyncio.create_task(poll_new_jobs())
    yield
    if polling_task is not None:
        polling_task.cancel()


app = FastAPI(
    title="Job Portal API",
    description=(
        "MongoDB-backed job tracker with live scrape snapshots. "
        f"Base URL: {BASE_URL}"
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# Explicit CORS origins from settings / .env (not wildcard + credentials).
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=CORS_ALLOW_CREDENTIALS,
    allow_methods=CORS_ALLOW_METHODS or ["*"],
    allow_headers=CORS_ALLOW_HEADERS or ["*"],
)


# ── Pydantic models ──────────────────────────────────────────────────────────

def _strip_nonempty(v: str) -> str:
    s = (v or "").strip()
    if not s:
        raise ValueError("value cannot be empty")
    return s


def _clean_str_list(values: Optional[list[Any]]) -> list[str]:
    if values is None:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        s = str(raw).strip()
        if not s:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
    return out


class ConfigUpdate(BaseModel):
    """Partial update for any config field in the single config document."""

    model_config = ConfigDict(extra="forbid")

    target: Optional[int] = Field(default=None, ge=1)
    results_per: Optional[int] = Field(default=None, ge=1)
    hours_old: Optional[int] = Field(default=None, ge=1)
    country: Optional[str] = Field(default=None, min_length=1)
    min_exp: Optional[int] = Field(default=None, ge=0)
    max_exp: Optional[int] = Field(default=None, ge=0)
    search_queries: Optional[list[str]] = None
    cities: Optional[list[str]] = None
    countries: Optional[list[str]] = None

    @field_validator("country", mode="before")
    @classmethod
    def strip_country(cls, v: Any) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            raise ValueError("country cannot be empty")
        return s

    @field_validator("search_queries", "cities", "countries", mode="before")
    @classmethod
    def clean_lists(cls, v: Any) -> Optional[list[str]]:
        if v is None:
            return None
        if not isinstance(v, list):
            raise ValueError("must be a list of strings")
        return _clean_str_list(v)


class QueryItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: str = Field(..., min_length=1)

    @field_validator("query")
    @classmethod
    def strip_query(cls, v: str) -> str:
        return _strip_nonempty(v)


class CityItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    city: str = Field(..., min_length=1)

    @field_validator("city")
    @classmethod
    def strip_city(cls, v: str) -> str:
        return _strip_nonempty(v)


class CountryItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    country: str = Field(..., min_length=1)

    @field_validator("country")
    @classmethod
    def strip_country_item(cls, v: str) -> str:
        return _strip_nonempty(v)


class ListItemEdit(BaseModel):
    """Rename / edit one list item inside the single config document."""

    model_config = ConfigDict(extra="forbid")
    old_value: str = Field(..., min_length=1)
    new_value: str = Field(..., min_length=1)

    @field_validator("old_value", "new_value")
    @classmethod
    def strip_values(cls, v: str) -> str:
        return _strip_nonempty(v)


class SearchCreateJson(BaseModel):
    """Body for Search → always creates a brand-new snapshot collection."""

    model_config = ConfigDict(extra="forbid")

    search: Optional[str] = Field(default=None)
    city: Optional[str] = Field(default=None)
    category: Optional[str] = Field(default=None)
    name: Optional[str] = Field(default=None, max_length=200)
    limit: Optional[int] = Field(default=None, ge=1)

    @field_validator("search", "city", "category", "name", mode="before")
    @classmethod
    def strip_optional(cls, v: Any) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None


# ── Config helpers (single collection: `config`, single document) ────────────

CONFIG_LIST_FIELDS = ("search_queries", "cities", "countries")
CONFIG_SCALAR_FIELDS = (
    "target",
    "results_per",
    "hours_old",
    "country",
    "min_exp",
    "max_exp",
)


def _empty_config_doc() -> dict:
    """Default payload for the single scraper config document."""
    return {
        "_key": CONFIG_DOC_KEY,
        "search_queries": [
            "Generative AI Engineer",
            "GenAI Engineer",
            "AI Engineer",
            "Agentic AI Engineer",
            "LLM Engineer",
            "Machine Learning Engineer",
            "MLOps Engineer",
            "Deep Learning Engineer",
            "NLP Engineer",
            "Computer Vision Engineer",
            "AI ML Engineer",
            "RAG Engineer",
            "Software Engineer AI",
            "Software Engineer",
            "Senior Software Engineer",
            "Python Developer",
            "Full Stack Developer",
            "Data Scientist",
            "Backend Engineer",
            "ML Platform Engineer",
        ],
        "cities": [
            "Bengaluru",
            "Hyderabad",
            "Pune",
            "Mumbai",
            "Noida",
            "Gurgaon",
            "Chennai",
            "Kolkata",
        ],
        "countries": [
            "India",
            "United States",
            "United Kingdom",
            "Germany",
            "Canada",
            "Singapore",
            "Australia",
            "United Arab Emirates",
            "Netherlands",
            "Switzerland",
            "Ireland",
            "Japan",
            "France",
            "Sweden",
            "Israel",
            "Poland",
            "Brazil",
            "New Zealand",
            "Spain",
            "South Korea",
            "South Africa",
            "Denmark",
            "Norway",
            "Finland",
            "Belgium",
            "Malaysia",
            "Philippines",
            "Vietnam",
            "Hong Kong",
            "Italy",
        ],
        "target": 20,
        "results_per": 20,
        "hours_old": 6,
        "country": "India",
        "min_exp": None,
        "max_exp": None,
        "updated_at": None,
    }


def touch_updated_at() -> str:
    return datetime.now(timezone.utc).isoformat()


def _migrate_legacy_config_doc() -> Optional[dict]:
    """
    If an older config document exists without `_key`, adopt it as the
    single scraper_settings document so we never have multiple configs.
    """
    keyed = config_col.find_one(CONFIG_FILTER)
    if keyed:
        return keyed
    legacy = config_col.find_one({"_key": {"$exists": False}})
    if legacy:
        config_col.update_one(
            {"_id": legacy["_id"]},
            {"$set": {"_key": CONFIG_DOC_KEY}},
        )
        # Drop any extra unkeyed leftovers so only one config document remains
        config_col.delete_many({"_key": {"$exists": False}})
        return config_col.find_one(CONFIG_FILTER)
    return None


def ensure_config_document() -> dict:
    """
    Guarantee exactly one config document in the `config` collection.
    All variables live in this document: search_queries, cities, countries,
    target, results_per, hours_old, country.
    """
    doc = config_col.find_one(CONFIG_FILTER) or _migrate_legacy_config_doc()
    if doc is None:
        payload = _empty_config_doc()
        payload["updated_at"] = touch_updated_at()
        config_col.update_one(
            CONFIG_FILTER,
            {"$setOnInsert": payload},
            upsert=True,
        )
        doc = config_col.find_one(CONFIG_FILTER)
        return doc

    # Fill only missing keys — never overwrite user-cleared lists with defaults
    defaults = _empty_config_doc()
    patch: dict[str, Any] = {}
    for field in (*CONFIG_LIST_FIELDS, *CONFIG_SCALAR_FIELDS, "updated_at"):
        if field not in doc:
            patch[field] = defaults[field]
    if "_key" not in doc:
        patch["_key"] = CONFIG_DOC_KEY
    if patch:
        config_col.update_one({"_id": doc["_id"]}, {"$set": patch})
        doc = config_col.find_one(CONFIG_FILTER)

    # Merge default worldwide IT countries that are missing from the existing database config doc
    existing_countries = set(doc.get("countries") or [])
    default_countries = defaults["countries"]
    missing_countries = [c for c in default_countries if c not in existing_countries]
    if missing_countries:
        new_countries = list(doc.get("countries") or []) + missing_countries
        config_col.update_one(
            {"_id": doc["_id"]},
            {"$set": {"countries": new_countries, "updated_at": touch_updated_at()}},
        )
        doc = config_col.find_one(CONFIG_FILTER)

    # Enforce single document: remove any other docs in config collection
    config_col.delete_many({"_id": {"$ne": doc["_id"]}})
    return doc


def get_config_doc() -> dict:
    return ensure_config_document()


def update_config_fields(fields: dict[str, Any]) -> dict:
    """$set any fields on the single config document and return fresh doc."""
    if not fields:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    ensure_config_document()
    fields = {**fields, "updated_at": touch_updated_at(), "_key": CONFIG_DOC_KEY}
    config_col.update_one(CONFIG_FILTER, {"$set": fields}, upsert=True)
    return get_config_doc()


def add_list_item(field: str, value: str) -> dict:
    if field not in CONFIG_LIST_FIELDS:
        raise HTTPException(status_code=400, detail=f"Invalid list field: {field}")
    ensure_config_document()
    config_col.update_one(
        CONFIG_FILTER,
        {
            "$addToSet": {field: value},
            "$set": {"updated_at": touch_updated_at(), "_key": CONFIG_DOC_KEY},
        },
        upsert=True,
    )
    return get_config_doc()


def remove_list_item(field: str, value: str) -> dict:
    if field not in CONFIG_LIST_FIELDS:
        raise HTTPException(status_code=400, detail=f"Invalid list field: {field}")
    ensure_config_document()
    config_col.update_one(
        CONFIG_FILTER,
        {
            "$pull": {field: value},
            "$set": {"updated_at": touch_updated_at()},
        },
    )
    return get_config_doc()


def edit_list_item(field: str, old_value: str, new_value: str) -> dict:
    """Replace one list entry (edit) inside the single config document."""
    if field not in CONFIG_LIST_FIELDS:
        raise HTTPException(status_code=400, detail=f"Invalid list field: {field}")
    doc = ensure_config_document()
    items = list(doc.get(field) or [])
    try:
        idx = items.index(old_value)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"'{old_value}' not found in {field}")
    if new_value != old_value and new_value in items:
        raise HTTPException(status_code=400, detail=f"'{new_value}' already exists in {field}")
    items[idx] = new_value
    return update_config_fields({field: items})


def serialize_config(doc: Optional[dict]) -> dict:
    if not doc:
        base = _empty_config_doc()
        base.pop("_key", None)
        base["is_ready"] = False
        base["collection"] = "config"
        return base
    return {
        "collection": "config",
        "search_queries": list(doc.get("search_queries") or []),
        "cities": list(doc.get("cities") or []),
        "countries": list(doc.get("countries") or []),
        "target": doc.get("target"),
        "results_per": doc.get("results_per"),
        "hours_old": doc.get("hours_old"),
        "country": doc.get("country"),
        "min_exp": doc.get("min_exp"),
        "max_exp": doc.get("max_exp"),
        "updated_at": doc.get("updated_at"),
        "is_ready": config_is_ready(doc),
    }


def config_is_ready(doc: Optional[dict]) -> bool:
    if not doc:
        return False
    queries = doc.get("search_queries") or []
    cities = doc.get("cities") or []
    target = doc.get("target")
    results_per = doc.get("results_per")
    hours_old = doc.get("hours_old")
    country = doc.get("country")
    if not queries or not cities:
        return False
    if not isinstance(target, int) or target < 1:
        return False
    if not isinstance(results_per, int) or results_per < 1:
        return False
    if not isinstance(hours_old, int) or hours_old < 1:
        return False
    if not country or not str(country).strip():
        return False
    return True


def now_stamp() -> datetime:
    return datetime.now(timezone.utc)


def make_snapshot_collection_name(custom_name: Optional[str] = None) -> str:
    """
    Unique MongoDB collection name for a scrape snapshot.
    Jobs are stored as documents in this collection — not as a JSON file/blob.
    Example: live_stream_2026_07_09_15_52_31_092000
    """
    stamp = now_stamp().strftime("%Y_%m_%d_%H_%M_%S")
    micros = now_stamp().strftime("%f")
    if custom_name:
        safe = re.sub(r"[^\w]+", "_", str(custom_name)).strip("_")
        # Drop accidental .json / _json suffixes from older naming
        if safe.lower().endswith(".json"):
            safe = safe[:-5]
        if safe.lower().endswith("_json"):
            safe = safe[:-5]
        if safe.lower().endswith("json"):
            safe = safe[:-4].rstrip("_")
        safe = re.sub(r"[^\w]+", "_", safe).strip("_") or "jobs"
        if safe.lower() in RESERVED_COLLECTIONS:
            safe = f"snap_{safe}"
        return f"{safe}_{stamp}_{micros}"
    return f"jobs_{stamp}_{micros}"


def serialize_job(job: dict) -> dict:
    return {
        "title": job.get("title"),
        "company": job.get("company"),
        "location": job.get("location"),
        "city": job.get("city"),
        "country": job.get("country"),
        "job_url": job.get("job_url"),
        "category": job.get("category"),
        "time_ago": job.get("time_ago"),
        "scraped_at": job.get("scraped_at"),
        "description": job.get("description"),
        "date_posted": job.get("date_posted"),
        "source": job.get("source"),
    }


# Country name → Indeed country_indeed codes / location aliases used by job boards
_COUNTRY_ALIASES: dict[str, set[str]] = {
    "india": {"india", "in", "ind", "bharat"},
    "united states": {"united states", "usa", "us", "u.s.", "u.s.a.", "america"},
    "united kingdom": {"united kingdom", "uk", "u.k.", "great britain", "england", "gb"},
    "germany": {"germany", "de", "deutschland"},
    "canada": {"canada", "ca"},
}

_INDEED_COUNTRY_MAP: dict[str, str] = {
    "india": "India",
    "united states": "USA",
    "usa": "USA",
    "us": "USA",
    "united kingdom": "UK",
    "uk": "UK",
    "germany": "Germany",
    "canada": "Canada",
}


def _split_csv(raw: Optional[str]) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in str(raw).split(",") if part and str(part).strip()]


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return None
    return text


def _normalize_date_posted(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return _safe_str(value)


def country_aliases(name: str) -> set[str]:
    key = (name or "").strip().lower()
    if not key:
        return set()
    aliases = set(_COUNTRY_ALIASES.get(key, set()))
    aliases.add(key)
    # Also index short codes back to themselves
    for full, codes in _COUNTRY_ALIASES.items():
        if key in codes:
            aliases |= codes
            aliases.add(full)
    return aliases


def resolve_indeed_country(countries: list[str], fallback: Optional[str] = None) -> str:
    for raw in [*countries, fallback or ""]:
        key = (raw or "").strip().lower()
        if not key:
            continue
        if key in _INDEED_COUNTRY_MAP:
            return _INDEED_COUNTRY_MAP[key]
        for alias_key, mapped in _INDEED_COUNTRY_MAP.items():
            if key == alias_key or key in country_aliases(alias_key):
                return mapped
        # JobSpy accepts free-form names for many countries
        return raw.strip()
    return "India"


def job_matches_exp_range(description: str, min_exp: Optional[int], max_exp: Optional[int]) -> bool:
    if min_exp is None and max_exp is None:
        return True
    if not description:
        return True

    # Matches patterns like "3 years", "3+ years", "3-5 years", "3 to 5 years"
    matches = re.findall(r"(\d+)\s*(?:to|-|\+)?\s*(\d+)?\s*years?", description.lower())
    if not matches:
        return True  # Fallback: if no exp mentioned, pass the candidate

    for match in matches:
        try:
            low = int(match[0])
            high = int(match[1]) if match[1] else (low + 5 if "+" in description else low)

            if min_exp is not None and high < min_exp:
                continue
            if max_exp is not None and low > max_exp:
                continue
            return True
        except ValueError:
            continue
    return False


def _search_term_matches(term: str, title: str, company: str, description: str = "") -> bool:
    """Match full phrase, or a majority of tokens (handles Gen AI ↔ Generative AI)."""
    term = (term or "").strip().lower()
    if not term:
        return True
    haystack = f"{title} {company} {description}".lower()
    if term in haystack:
        return True
    # Normalize common abbreviations so DB filters stay useful after scrape.
    normalized = (
        haystack.replace("genai", "generative ai")
        .replace("gen ai", "generative ai")
        .replace("gen-ai", "generative ai")
        .replace("llms", "llm")
        .replace("machine learning", "ml")
    )
    term_norm = (
        term.replace("genai", "generative ai")
        .replace("gen ai", "generative ai")
        .replace("gen-ai", "generative ai")
    )
    if term_norm in normalized or term_norm in haystack:
        return True
    tokens = [t for t in re.split(r"\s+", term_norm) if len(t) >= 2]
    if not tokens:
        return term in haystack
    blob = f"{haystack} {normalized}"
    hits = sum(1 for t in tokens if t in blob)
    need = max(1, math.ceil(len(tokens) * 0.5))
    return hits >= need


def _job_matches_country(job: dict, selected_countries: list[str]) -> bool:
    if not selected_countries:
        return True
    job_country = (job.get("country") or "").strip().lower()
    job_loc = (job.get("location") or "").strip().lower()
    blob = f"{job_country} {job_loc}"
    for raw in selected_countries:
        aliases = country_aliases(raw)
        if any(alias and alias in blob for alias in aliases):
            return True
        # Location fragments like "TS, IN" / "Hyderabad, India"
        for alias in aliases:
            if len(alias) <= 3:
                # short codes: match as , IN / IN / (IN)
                if re.search(rf"(?:^|,\s*|\s){re.escape(alias)}(?:$|\s|,)", blob):
                    return True
            elif alias in blob:
                return True
    return False


def filter_jobs_list(
    jobs: list[dict],
    search: str = "",
    city_param: str = "",
    category: str = "",
    min_exp: Optional[int] = None,
    max_exp: Optional[int] = None,
    country_param: str = "",
    *,
    strict_search: bool = True,
) -> list[dict]:
    category = (category or "").strip().lower()
    search_terms = _split_csv(search)
    selected_cities = [c.lower() for c in _split_csv(city_param)]
    selected_countries = [co.lower() for co in _split_csv(country_param)]

    out = []
    for job in jobs:
        if category and (job.get("category") or "").lower() != category:
            continue
        title = (job.get("title") or "").lower()
        company = (job.get("company") or "").lower()
        description = (job.get("description") or "").lower()
        if strict_search and search_terms:
            if not any(
                _search_term_matches(st, title, company, description) for st in search_terms
            ):
                continue
        job_city = (job.get("city") or job.get("location") or "").lower()
        if selected_cities and not any(sc in job_city for sc in selected_cities):
            continue
        if not _job_matches_country(job, selected_countries):
            continue
        if min_exp is not None or max_exp is not None:
            if not job_matches_exp_range(description, min_exp, max_exp):
                continue
        out.append(job)
    return out


def collect_jobs_from_live_collection(
    search: str = "",
    city: str = "",
    category: str = "",
    limit: Optional[int] = None,
    min_exp: Optional[int] = None,
    max_exp: Optional[int] = None,
    country_param: str = "",
) -> list[dict]:
    query: dict = {}
    cat = (category or "").strip().lower()
    if cat:
        query["category"] = cat

    db_jobs = list(jobs_col.find(query).sort("scraped_at", -1))
    serialized = [serialize_job(j) for j in db_jobs]
    filtered = filter_jobs_list(
        serialized,
        search=search,
        city_param=city,
        category="",
        min_exp=min_exp,
        max_exp=max_exp,
        country_param=country_param,
    )
    if limit is not None and limit > 0:
        filtered = filtered[:limit]
    return filtered


def _today_iso() -> str:
    return date.today().isoformat()


def _estimate_time_ago(date_posted: str, hours_old: int) -> str:
    """Same fallback logic as linkedin_realtime_hyderabad.py."""
    try:
        posted_dt = date.fromisoformat(date_posted)
        days_diff = (date.today() - posted_dt).days
    except Exception:
        days_diff = 0

    if days_diff <= 0:
        if random.random() < 0.15:
            minutes = random.randint(5, 59)
            return f"{minutes} minutes ago"
        max_h = min(23, max(1, hours_old))
        hours = random.randint(1, max_h)
        return f"{hours} hour{'s' if hours > 1 else ''} ago"
    if days_diff == 1:
        return "1 day ago"
    return f"{days_diff} days ago"


def _city_from_location(location: str, fallback_city: str) -> str:
    """Extract city from 'Hyderabad, Telangana, India' style strings."""
    loc = (location or "").strip()
    if not loc or loc.lower() == "nan":
        return fallback_city or "Hyderabad"
    return loc.split(",")[0].strip() or fallback_city or "Hyderabad"


def scrape_linkedin(
    query: str,
    location: str,
    results: int,
    hours_old: int,
    *,
    country_indeed: str = "India",
    default_city: str = "",
) -> list[dict]:
    """
    Call python-jobspy for LinkedIn only (mirrors linkedin_realtime_hyderabad.py).
    Returns normalized job dicts ready for MongoDB / WebSocket.
    """
    if jobspy_scrape_jobs is None:
        raise RuntimeError(
            "python-jobspy is not installed. "
            "Run: pip install python-jobspy --no-deps && "
            "pip install pandas requests beautifulsoup4 tls-client markdownify regex numpy"
        )

    loc = (location or "").strip() or "Hyderabad"
    results = max(1, int(results or 10))
    hours_old = max(1, int(hours_old or 6))
    fallback_city = (default_city or loc.split(",")[0].strip() or "Hyderabad")

    try:
        df = jobspy_scrape_jobs(
            site_name=["linkedin"],
            search_term=query,
            location=loc,
            results_wanted=results,
            hours_old=hours_old,
            country_indeed=country_indeed or "India",
            verbose=0,
        )
    except Exception as e:
        print(f"    scrape_linkedin ERROR ({query!r} @ {loc!r}): {e}")
        return []

    if df is None or getattr(df, "empty", True):
        return []

    jobs: list[dict] = []
    today = _today_iso()
    scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for _, row in df.iterrows():
        url = str(row.get("job_url", "") or "").strip()
        if not url or url.lower() == "nan":
            continue

        raw_date = str(row.get("date_posted", "") or "").strip()
        if not raw_date or raw_date.lower() == "nan":
            date_posted = today
        else:
            date_posted = raw_date.split(" ")[0].strip()

        time_ago_scraped = str(row.get("time_ago", "") or "").strip()
        if time_ago_scraped and time_ago_scraped.lower() != "nan":
            time_ago = time_ago_scraped
        else:
            time_ago = _estimate_time_ago(date_posted, hours_old)

        location_val = str(row.get("location", "") or "").strip()
        if not location_val or location_val.lower() == "nan":
            location_val = loc

        city = _city_from_location(location_val, fallback_city)
        title = str(row.get("title", "") or "").strip()
        company = str(row.get("company", "") or "").strip()
        description = _safe_str(row.get("description")) or ""

        if not title:
            continue

        jobs.append(
            {
                "title": title,
                "company": company or "Unknown",
                "location": location_val,
                "city": city,
                "country": country_indeed or "India",
                "job_url": url,
                "date_posted": date_posted,
                "time_ago": time_ago,
                "category": f"{hours_old}h",
                "source": "linkedin",
                "scraped_at": scraped_at,
                "description": description,
                "search_term": query,
            }
        )
    return jobs


def upsert_job(
    job: dict,
    *,
    collection_name: Optional[str] = None,
    also_global: bool = True,
) -> dict:
    """
    Real-time upsert one job into a MongoDB collection by job_url.
    - collection_name: session/snapshot collection (defaults to shared `jobs`)
    - also_global: when writing to a session collection, also mirror into `jobs`
    Returns serialized doc including Mongo _id and save confirmation flags.
    """
    url = (job.get("job_url") or "").strip()
    payload = {k: v for k, v in (job or {}).items() if k != "_id"}
    target_name = (collection_name or "").strip() or JOBS_COLLECTION_NAME
    target_col = db[target_name]

    if not url:
        out = serialize_job(payload)
        out["saved"] = False
        out["save_error"] = "missing job_url"
        out["collection"] = target_name
        out["database"] = DATABASE_NAME
        return out

    try:
        # Ensure unique index on session collections (best-effort)
        if target_name != JOBS_COLLECTION_NAME:
            try:
                target_col.create_index([("job_url", ASCENDING)], unique=True)
            except Exception:
                pass

        doc = target_col.find_one_and_update(
            {"job_url": url},
            {"$set": payload},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if not doc:
            doc = target_col.find_one({"job_url": url}) or {**payload, "job_url": url}

        # Mirror into global live cache so poller / shared views stay current
        if also_global and target_name != JOBS_COLLECTION_NAME:
            try:
                jobs_col.update_one({"job_url": url}, {"$set": payload}, upsert=True)
            except Exception as mirror_err:
                print(f"upsert_job mirror to jobs error: {mirror_err}")

        out = serialize_job(doc)
        out["_id"] = str(doc.get("_id", ""))
        out["saved"] = True
        out["saved_at"] = payload.get("scraped_at") or touch_updated_at()
        out["collection"] = target_name
        out["database"] = DATABASE_NAME
        return out
    except Exception as e:
        print(f"upsert_job error ({target_name}): {e}")
        out = serialize_job(payload)
        out["saved"] = False
        out["save_error"] = str(e)
        out["collection"] = target_name
        out["database"] = DATABASE_NAME
        return out


def upsert_jobs(
    jobs: list[dict],
    *,
    collection_name: Optional[str] = None,
    also_global: bool = True,
) -> list[dict]:
    """Upsert many jobs one-by-one so each document is confirmed in MongoDB."""
    if not jobs:
        return []
    return [
        upsert_job(j, collection_name=collection_name, also_global=also_global)
        for j in jobs
    ]


def _clamp_positive_int(value: Any, default: int, *, minimum: int = 1) -> int:
    """Coerce to int and enforce a hard minimum (parameter caps)."""
    try:
        n = int(value)
    except (TypeError, ValueError):
        n = int(default)
    if n < minimum:
        return minimum
    return n


def begin_live_stream_snapshot(
    *,
    search_term: Optional[str] = None,
    filters: Optional[dict] = None,
    name: Optional[str] = None,
) -> dict:
    """
    Each Start Live Stream run creates:
      1) a brand-new MongoDB collection: {name}_{timestamp}
      2) a metadata row in scrape_jsons (so Dashboard lists it immediately)
    """
    cfg = ensure_config_document()
    created = touch_updated_at()
    base = (name or "live_stream").strip() or "live_stream"
    collection_name = make_snapshot_collection_name(base)

    existing = set(db.list_collection_names())
    if collection_name in existing or collection_name in RESERVED_COLLECTIONS:
        collection_name = f"{collection_name}_{ObjectId()}"

    # Create empty collection up-front so it exists even before first job
    try:
        db.create_collection(collection_name)
    except Exception:
        # Already exists (race) — continue using it
        pass
    try:
        db[collection_name].create_index([("job_url", ASCENDING)], unique=True)
    except Exception:
        pass

    payload = {
        "name": collection_name,
        "collection_name": collection_name,
        "created_at": created,
        "search_term": search_term,
        "filters": filters or {},
        "source": "live_stream",
        "config_snapshot": {
            "search_queries": list(cfg.get("search_queries") or []),
            "cities": list(cfg.get("cities") or []),
            "countries": list(cfg.get("countries") or []),
            "target": cfg.get("target"),
            "results_per": cfg.get("results_per"),
            "hours_old": cfg.get("hours_old"),
            "country": cfg.get("country"),
            "min_exp": cfg.get("min_exp"),
            "max_exp": cfg.get("max_exp"),
        },
        "job_count": 0,
        "status": "running",
    }
    result = scrape_jsons_col.insert_one(payload)
    doc = scrape_jsons_col.find_one({"_id": result.inserted_id})
    return doc


def finalize_live_stream_snapshot(
    scrape_json_id: Any,
    *,
    job_count: int,
    status: str = "completed",
    extra_filters: Optional[dict] = None,
) -> Optional[dict]:
    """Update scrape_jsons metadata after a live stream finishes."""
    oid = scrape_json_id if isinstance(scrape_json_id, ObjectId) else parse_object_id(str(scrape_json_id or ""))
    if not oid:
        return None
    patch: dict[str, Any] = {
        "job_count": int(job_count or 0),
        "status": status,
        "completed_at": touch_updated_at(),
    }
    if extra_filters:
        patch["filters"] = extra_filters
    scrape_jsons_col.update_one({"_id": oid}, {"$set": patch})
    return scrape_jsons_col.find_one({"_id": oid})


def scrape_external_jobs(
    search: str = "",
    city: str = "",
    countries: str = "",
    results_per: int = 10,
    hours_old: int = 6,
    target: int = 20,
    min_exp: Optional[int] = None,
    max_exp: Optional[int] = None,
    on_status: Optional[Callable[[str], None]] = None,
    on_job: Optional[Callable[[dict, int, int, dict], None]] = None,
    *,
    collection_name: Optional[str] = None,
    session_id: Optional[str] = None,
    scrape_json_id: Optional[str] = None,
    strict_caps: bool = False,
) -> tuple[list[dict], dict]:
    """
    LinkedIn real-time scrape — same loop as linkedin_realtime_hyderabad.py:
      for each query × city → scrape_linkedin → dedupe by job_url → stop at target.

    Parameter caps (strict_caps=True for Start Live Stream):
      - target: never store more than this many unique jobs
      - results_per: never request more hits per query from LinkedIn
      - hours_old: never expand the window beyond the requested value
    When collection_name is set, every job is upserted into that session collection
    (and mirrored into global `jobs`).
    """
    cfg = ensure_config_document()
    queries = _split_csv(search) or list(cfg.get("search_queries") or [])[:1] or [
        "Software Engineer"
    ]
    cities = _split_csv(city) or list(cfg.get("cities") or [])[:1] or ["Hyderabad"]
    country_list = _split_csv(countries)
    if not country_list:
        fallback = (cfg.get("country") or "India").strip()
        country_list = [fallback] if fallback else ["India"]

    # Hard-clamp parameter caps — never go below 1; never inflate beyond request
    results_per = _clamp_positive_int(
        results_per if results_per is not None else cfg.get("results_per"),
        int(cfg.get("results_per") or 10),
    )
    hours_old = _clamp_positive_int(
        hours_old if hours_old is not None else cfg.get("hours_old"),
        int(cfg.get("hours_old") or 6),
    )
    target = _clamp_positive_int(
        target if target is not None else cfg.get("target"),
        int(cfg.get("target") or 20),
    )
    country_indeed = resolve_indeed_country(country_list, "India")
    session_col = (collection_name or "").strip() or None

    meta: dict[str, Any] = {
        "queries": queries,
        "cities": cities,
        "countries": country_list,
        "results_per": results_per,
        "hours_old": hours_old,
        "target": target,
        "strict_caps": bool(strict_caps),
        "collection_name": session_col or JOBS_COLLECTION_NAME,
        "site": "linkedin",
        "attempts": [],
        "expanded_hours": None,
    }

    collected: list[dict] = []
    seen_urls: set[str] = set()

    def _notify_status(msg: str) -> None:
        if on_status:
            try:
                on_status(msg)
            except Exception:
                pass
        print(msg)

    def _session_db_meta(saved: int) -> dict:
        return get_db_meta(
            session_saved=saved,
            session_id=session_id,
            collection_name=session_col or JOBS_COLLECTION_NAME,
            scrape_json_id=scrape_json_id,
        )

    def run_pass(hours: int) -> None:
        nonlocal collected
        # Strict cap: never scrape with a wider hours window than requested
        hours = min(int(hours), hours_old) if strict_caps else int(hours)
        for qi, q in enumerate(queries, 1):
            if len(collected) >= target:
                return
            for city_name in cities:
                if len(collected) >= target:
                    return

                _notify_status(
                    f"[{qi:02d}/{len(queries)}] LinkedIn scrape: {q!r} @ {city_name} "
                    f"(hours_old={hours}, results={results_per}, target_cap={target}"
                    f"{f', col={session_col}' if session_col else ''})"
                )
                try:
                    batch = scrape_linkedin(
                        query=q,
                        location=city_name,
                        results=results_per,  # strict results_per cap
                        hours_old=hours,  # strict hours_old cap when strict_caps
                        country_indeed=country_indeed,
                        default_city=city_name,
                    )
                    # Never keep more raw rows than results_per asked for
                    if len(batch) > results_per:
                        batch = batch[:results_per]
                    meta["attempts"].append(
                        {
                            "query": q,
                            "city": city_name,
                            "country": country_indeed,
                            "hours_old": hours,
                            "results_per": results_per,
                            "raw_count": len(batch),
                            "site": "linkedin",
                        }
                    )
                except Exception as e:
                    meta["attempts"].append(
                        {
                            "query": q,
                            "city": city_name,
                            "country": country_indeed,
                            "hours_old": hours,
                            "results_per": results_per,
                            "error": str(e),
                            "site": "linkedin",
                        }
                    )
                    _notify_status(f"Scrape error for '{q}' / '{city_name}': {e}")
                    continue

                # Optional experience filter only — LinkedIn already applied query/location.
                filtered = filter_jobs_list(
                    batch,
                    search="",
                    city_param="",
                    country_param="",
                    min_exp=min_exp,
                    max_exp=max_exp,
                    strict_search=False,
                )

                added = 0
                for job in filtered:
                    if len(collected) >= target:
                        break
                    url = job.get("job_url")
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)
                    # Real-time write into session collection (and global jobs)
                    serialized = upsert_job(job, collection_name=session_col)
                    collected.append(serialized)
                    added += 1
                    db_meta = _session_db_meta(len(collected))
                    if serialized.get("saved"):
                        _notify_status(
                            f"  SAVED → {db_meta['dbname']}.{db_meta['collection']} "
                            f"| {serialized.get('title', '')[:40]} "
                            f"| _id={str(serialized.get('_id', ''))[:8]}… "
                            f"| session={len(collected)}/{target} "
                            f"| total_count={db_meta['total_count']}"
                        )
                    else:
                        _notify_status(
                            f"  SAVE FAILED: {serialized.get('save_error', 'unknown')} "
                            f"for {serialized.get('title', '')[:40]}"
                        )
                    if on_job:
                        try:
                            on_job(serialized, len(collected), target, db_meta)
                        except Exception:
                            pass
                    if len(collected) >= target:
                        _notify_status(
                            f"Reached target cap of {target} unique jobs "
                            f"(collection total={db_meta['total_count']})."
                        )
                        return

                _notify_status(
                    f"  +{added} new from {city_name} for {q!r} "
                    f"(session={len(collected)}/{target}, "
                    f"db_total={_session_db_meta(len(collected))['total_count']})"
                )
                # Same anti-rate-limit delay as linkedin_realtime_hyderabad.py
                time.sleep(random.uniform(1.5, 2.5))

    run_pass(hours_old)

    # Auto-expand hours only when NOT in strict_caps mode (Live Stream is strict).
    if not strict_caps:
        if not collected and hours_old < 24:
            expanded = 24
            meta["expanded_hours"] = expanded
            _notify_status(
                f"No jobs in last {hours_old}h — expanding window to {expanded}h…"
            )
            run_pass(expanded)

        if (
            not collected
            and hours_old < 72
            and (meta.get("expanded_hours") or hours_old) < 72
        ):
            expanded = 72
            meta["expanded_hours"] = expanded
            _notify_status(f"Still empty — expanding window to {expanded}h…")
            run_pass(expanded)

    # Hard target cap — never return more than requested
    return collected[:target], meta


def snapshot_collection_name(doc: dict) -> Optional[str]:
    """Resolve the MongoDB collection that holds this snapshot's job documents."""
    return doc.get("collection_name") or doc.get("name")


def serialize_scrape_json_meta(doc: dict) -> dict:
    col_name = snapshot_collection_name(doc)
    return {
        "id": str(doc["_id"]),
        "name": col_name,
        "collection_name": col_name,
        "created_at": doc.get("created_at"),
        "completed_at": doc.get("completed_at"),
        "job_count": doc.get("job_count", 0),
        "search_term": doc.get("search_term"),
        "filters": doc.get("filters") or {},
        "config_snapshot": doc.get("config_snapshot") or {},
        "source": doc.get("source") or "search",
        "status": doc.get("status") or "completed",
    }


def serialize_scrape_json_full(doc: dict) -> dict:
    meta = serialize_scrape_json_meta(doc)
    col_name = snapshot_collection_name(doc)
    jobs = []
    if col_name and col_name in db.list_collection_names():
        col_jobs = list(db[col_name].find({}))
        for j in col_jobs:
            if "_id" in j:
                j["_id"] = str(j["_id"])
            jobs.append(j)
    for i, j in enumerate(jobs):
        if not j.get("_id"):
            j["_id"] = f"{meta['id']}-{i}"
    meta["jobs"] = jobs
    meta["job_count"] = len(jobs)
    return meta


def parse_object_id(raw: str) -> Optional[ObjectId]:
    try:
        return ObjectId(raw)
    except (InvalidId, TypeError):
        return None


def create_scrape_json_document(
    jobs: list[dict],
    *,
    search_term: Optional[str] = None,
    filters: Optional[dict] = None,
    name: Optional[str] = None,
) -> dict:
    """
    Create a NEW MongoDB collection for this snapshot and insert jobs into it.
    Metadata (name, filters, counts) is stored in scrape_jsons — jobs are NOT
    embedded as a JSON array/blob.
    """
    cfg = ensure_config_document()
    created = touch_updated_at()
    collection_name = make_snapshot_collection_name(name)

    # Avoid colliding with an existing collection name
    existing = set(db.list_collection_names())
    if collection_name in existing or collection_name in RESERVED_COLLECTIONS:
        collection_name = f"{collection_name}_{ObjectId()}"

    new_col = db[collection_name]
    serialized_jobs = [
        serialize_job(j)
        if "job_url" in (j or {}) or "title" in (j or {})
        else j
        for j in (jobs or [])
    ]
    if serialized_jobs:
        new_col.insert_many(serialized_jobs)
    else:
        # Ensure the empty collection exists in job_portal DB
        db.create_collection(collection_name)

    payload = {
        "name": collection_name,
        "collection_name": collection_name,
        "created_at": created,
        "search_term": search_term,
        "filters": filters or {},
        "config_snapshot": {
            "search_queries": list(cfg.get("search_queries") or []),
            "cities": list(cfg.get("cities") or []),
            "target": cfg.get("target"),
            "results_per": cfg.get("results_per"),
            "hours_old": cfg.get("hours_old"),
            "country": cfg.get("country"),
        },
        "job_count": len(serialized_jobs),
    }
    result = scrape_jsons_col.insert_one(payload)
    doc = scrape_jsons_col.find_one({"_id": result.inserted_id})
    return doc


# ── Health & config routes ───────────────────────────────────────────────────

@app.get("/api/health")
def health():
    """Liveness + non-secret runtime settings (base_url, cors, db name)."""
    try:
        client.admin.command("ping")
        cfg = ensure_config_document()
        return {
            "status": "ok",
            "database": DATABASE_NAME,
            "config_ready": config_is_ready(cfg),
            "scrape_json_count": scrape_jsons_col.count_documents({}),
            "server": "fastapi+uvicorn",
            **public_settings_dict(),
        }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={"status": "error", "message": str(e)},
        )


@app.get("/api/config")
def get_config():
    """Read all config variables from the single `config` collection document."""
    cfg = get_config_doc()
    return serialize_config(cfg)


@app.put("/api/config")
def update_config(data: ConfigUpdate):
    """
    Update any subset of config fields in the single `config` document.
    Supports scalars (target, results_per, hours_old, country, min_exp, max_exp)
    and full list replace (search_queries, cities, countries).
    """
    update_fields = data.model_dump(exclude_unset=True)
    # Allow clearing optional exp defaults with null from the UI
    raw = data.model_dump(exclude_unset=True, exclude_none=False)
    for exp_key in ("min_exp", "max_exp"):
        if exp_key in raw and raw[exp_key] is None:
            update_fields[exp_key] = None
    updated = update_config_fields(update_fields)
    return {
        "status": "success",
        "message": "Config updated in collection `config`",
        "config": serialize_config(updated),
    }


@app.post("/api/config/queries")
def add_query(data: QueryItem):
    """Add a search query into config.search_queries."""
    updated = add_list_item("search_queries", data.query)
    return {"status": "success", "config": serialize_config(updated)}


@app.put("/api/config/queries")
def edit_query(data: ListItemEdit):
    """Edit (rename) a search query in the single config document."""
    updated = edit_list_item("search_queries", data.old_value, data.new_value)
    return {"status": "success", "config": serialize_config(updated)}


@app.delete("/api/config/queries")
def remove_query(data: QueryItem):
    """Delete a search query from config.search_queries."""
    updated = remove_list_item("search_queries", data.query)
    return {"status": "success", "config": serialize_config(updated)}


@app.post("/api/config/cities")
def add_city(data: CityItem):
    """Add a city into config.cities."""
    updated = add_list_item("cities", data.city)
    return {"status": "success", "config": serialize_config(updated)}


@app.put("/api/config/cities")
def edit_city(data: ListItemEdit):
    """Edit (rename) a city in the single config document."""
    updated = edit_list_item("cities", data.old_value, data.new_value)
    return {"status": "success", "config": serialize_config(updated)}


@app.delete("/api/config/cities")
def remove_city(data: CityItem):
    """Delete a city from config.cities."""
    updated = remove_list_item("cities", data.city)
    return {"status": "success", "config": serialize_config(updated)}


@app.post("/api/config/countries")
def add_country(data: CountryItem):
    """Add a country into config.countries."""
    updated = add_list_item("countries", data.country)
    return {"status": "success", "config": serialize_config(updated)}


@app.put("/api/config/countries")
def edit_country(data: ListItemEdit):
    """Edit (rename) a country in the single config document."""
    updated = edit_list_item("countries", data.old_value, data.new_value)
    return {"status": "success", "config": serialize_config(updated)}


@app.delete("/api/config/countries")
def remove_country(data: CountryItem):
    """Delete a country from config.countries."""
    updated = remove_list_item("countries", data.country)
    return {"status": "success", "config": serialize_config(updated)}


# ── Scrape JSON runs (each Search = new document) ────────────────────────────
# NOTE: /search must be declared BEFORE /{json_id} so "search" is not captured as id.

@app.get("/api/scrape-jsons")
def list_scrape_jsons():
    """
    List all saved scrape / live-stream snapshot documents (metadata only).
    Refreshes job_count from the actual MongoDB collection when present so
    Dashboard always reflects real data after live streams complete.
    """
    docs = list(scrape_jsons_col.find({}).sort("created_at", -1))
    known_cols = set(db.list_collection_names())
    out = []
    for d in docs:
        col_name = snapshot_collection_name(d)
        if col_name and col_name in known_cols:
            try:
                real_count = db[col_name].count_documents({})
                if real_count != d.get("job_count"):
                    scrape_jsons_col.update_one(
                        {"_id": d["_id"]},
                        {"$set": {"job_count": real_count}},
                    )
                    d["job_count"] = real_count
            except Exception:
                pass
        out.append(serialize_scrape_json_meta(d))
    return out


@app.post("/api/scrape-jsons/search", status_code=201)
def search_and_create_json(data: SearchCreateJson = SearchCreateJson()):
    """
    Search handler: creates a NEW MongoDB collection with matching job documents
    and a metadata row in scrape_jsons. Never overwrites an existing collection.
    Prefers a live external scrape; falls back to the MongoDB jobs cache.
    """
    cfg = ensure_config_document()
    jobs: list[dict] = []
    if jobspy_scrape_jobs is not None:
        try:
            jobs, _meta = scrape_external_jobs(
                search=data.search or "",
                city=data.city or "",
                countries=(cfg.get("country") or "India"),
                results_per=int(cfg.get("results_per") or 10),
                hours_old=int(cfg.get("hours_old") or 6),
                target=int(data.limit or cfg.get("target") or 20),
            )
        except Exception as e:
            print(f"search_and_create_json scrape failed: {e}")
            jobs = []

    if not jobs:
        jobs = collect_jobs_from_live_collection(
            search=data.search or "",
            city=data.city or "",
            category=data.category or "",
            limit=data.limit,
        )
    filters = {
        "search": data.search,
        "city": data.city,
        "category": data.category,
        "limit": data.limit,
    }
    doc = create_scrape_json_document(
        jobs,
        search_term=data.search,
        filters=filters,
        name=data.name,
    )
    full = serialize_scrape_json_full(doc)
    col_name = full.get("collection_name") or full.get("name")
    return {
        "status": "success",
        "message": f"New snapshot collection created: {col_name}",
        "scrape_json": full,
        "collection_name": col_name,
        "list": [
            serialize_scrape_json_meta(d)
            for d in scrape_jsons_col.find({}).sort("created_at", -1)
        ],
    }


@app.get("/api/scrape-jsons/{json_id}")
def get_scrape_json(json_id: str):
    """Load one snapshot metadata and all jobs from its MongoDB collection."""
    oid = parse_object_id(json_id)
    if not oid:
        raise HTTPException(status_code=400, detail="Invalid snapshot id")
    doc = scrape_jsons_col.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return serialize_scrape_json_full(doc)


@app.delete("/api/scrape-jsons/{json_id}")
def delete_scrape_json(json_id: str):
    """Drop the snapshot's job collection and remove its metadata from scrape_jsons."""
    oid = parse_object_id(json_id)
    if not oid:
        raise HTTPException(status_code=400, detail="Invalid snapshot id")
    doc = scrape_jsons_col.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Snapshot not found")

    col_name = snapshot_collection_name(doc)
    if col_name and col_name not in RESERVED_COLLECTIONS:
        db.drop_collection(col_name)

    result = scrape_jsons_col.delete_one({"_id": oid})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    remaining = [
        serialize_scrape_json_meta(d)
        for d in scrape_jsons_col.find({}).sort("created_at", -1)
    ]
    return {
        "status": "success",
        "deleted_id": json_id,
        "deleted_collection": col_name,
        "list": remaining,
    }


# ── WebSocket Realtime Job Loader ──────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

async def poll_new_jobs():
    """Background task to poll MongoDB jobs collection and broadcast new jobs."""
    # Find the latest job currently in DB to set as baseline
    try:
        latest = jobs_col.find_one(sort=[("_id", DESCENDING)])
        last_seen_id = latest["_id"] if latest else None
    except Exception:
        last_seen_id = None

    while True:
        await asyncio.sleep(3.0)
        if not manager.active_connections:
            continue
        try:
            query = {}
            if last_seen_id:
                query["_id"] = {"$gt": last_seen_id}
            
            new_docs = list(jobs_col.find(query).sort("_id", ASCENDING))
            for doc in new_docs:
                last_seen_id = doc["_id"]
                job_data = serialize_job(doc)
                job_data["_id"] = str(doc["_id"])
                # Broadcast new job to all connected sockets
                await manager.broadcast({
                    "type": "new_job_broadcast",
                    "data": job_data,
                    "message": f"Real-time update: New job found - {job_data.get('title')} at {job_data.get('company')}"
                })
        except Exception as e:
            print(f"Error in poll_new_jobs: {e}")


@app.get("/api/db-info")
def db_info():
    """MongoDB connection metadata for the Realtime Feed header."""
    return {"status": "success", "db": get_db_meta()}


@app.websocket("/api/ws/jobs")
async def websocket_jobs(websocket: WebSocket):
    await manager.connect(websocket)
    # Welcome + live DB metadata for Realtime Feed header
    await websocket.send_json({
        "type": "status",
        "message": "Connected to Real-time Job Tracker WebSocket",
    })
    await websocket.send_json({
        "type": "db_info",
        "data": get_db_meta(session_saved=0),
        "message": "MongoDB connection metadata loaded",
    })

    # Send latest 10 jobs as baseline
    try:
        latest_jobs = list(jobs_col.find({}).sort("scraped_at", -1).limit(10))
        serialized_latest = []
        for j in latest_jobs:
            item = serialize_job(j)
            item["_id"] = str(j["_id"])
            item["saved"] = True
            item["collection"] = JOBS_COLLECTION_NAME
            item["database"] = DATABASE_NAME
            serialized_latest.append(item)
        if serialized_latest:
            await websocket.send_json({
                "type": "initial_jobs",
                "data": serialized_latest,
                "db": get_db_meta(session_saved=0),
                "message": "Loaded last 10 jobs from database",
            })
    except Exception as e:
        print(f"Error sending initial jobs: {e}")

    try:
        while True:
            # Wait for client commands (e.g. live scrape session)
            data = await websocket.receive_json()
            action = data.get("action")

            if action == "start_scrape":
                search = data.get("search", "") or ""
                city = data.get("city", "") or ""
                category = data.get("category", "") or ""
                # Strict parameter caps — never below 1; values are hard ceilings
                target = _clamp_positive_int(data.get("target"), 20)
                results_per = _clamp_positive_int(data.get("results_per"), 10)
                hours_old = _clamp_positive_int(data.get("hours_old"), 6)
                min_exp = data.get("min_exp")
                max_exp = data.get("max_exp")
                if min_exp is not None:
                    try:
                        min_exp = max(0, int(min_exp))
                    except (TypeError, ValueError):
                        min_exp = None
                if max_exp is not None:
                    try:
                        max_exp = max(0, int(max_exp))
                    except (TypeError, ValueError):
                        max_exp = None
                countries = data.get("countries", "") or ""
                custom_col_name = (data.get("collection_name") or data.get("name") or "").strip()
                session_id = str(ObjectId())

                # Every Start Live Stream → NEW collection: live_stream_{timestamp}
                # + scrape_jsons metadata so Dashboard lists it immediately.
                stream_filters = {
                    "search": search or None,
                    "city": city or None,
                    "category": category or None,
                    "countries": countries or None,
                    "target": target,
                    "results_per": results_per,
                    "hours_old": hours_old,
                    "min_exp": min_exp,
                    "max_exp": max_exp,
                    "strict_caps": True,
                    "source": "live_stream",
                }
                snapshot_doc = begin_live_stream_snapshot(
                    search_term=search or None,
                    filters=stream_filters,
                    name=custom_col_name or "live_stream",
                )
                session_collection = snapshot_collection_name(snapshot_doc) or make_snapshot_collection_name(
                    "live_stream"
                )
                scrape_json_id = str(snapshot_doc["_id"]) if snapshot_doc else None

                def _live_db_meta(saved: int = 0) -> dict:
                    return get_db_meta(
                        session_saved=saved,
                        session_id=session_id,
                        collection_name=session_collection,
                        scrape_json_id=scrape_json_id,
                    )

                await websocket.send_json({
                    "type": "db_info",
                    "data": _live_db_meta(0),
                    "message": (
                        f"Live scrape session starting — new collection "
                        f"{DATABASE_NAME}.{session_collection}"
                    ),
                })

                await websocket.send_json({
                    "type": "status",
                    "message": (
                        f"Starting LinkedIn live scrape (STRICT caps) "
                        f"(Target≤{target}, Results/query≤{results_per}, "
                        f"Hours old={hours_old}, "
                        f"Countries: {countries or 'India'}, "
                        f"Exp: {min_exp if min_exp is not None else 0}-"
                        f"{max_exp if max_exp is not None else 'any'} yrs) "
                        f"for queries: '{search or 'config defaults'}' "
                        f"in cities: '{city or 'config defaults'}' → "
                        f"NEW collection {DATABASE_NAME}.{session_collection} "
                        f"(also listed on Dashboard)"
                    ),
                })

                streamed_count = 0
                meta: dict[str, Any] = {
                    "expanded_hours": None,
                    "attempts": [],
                    "collection_name": session_collection,
                }
                stream_status = "completed"

                if jobspy_scrape_jobs is None:
                    await websocket.send_json({
                        "type": "status",
                        "message": (
                            "python-jobspy is not installed on the server. "
                            "Falling back to MongoDB jobs collection only "
                            f"(still writing into {session_collection})."
                        ),
                    })
                    jobs = collect_jobs_from_live_collection(
                        search=search,
                        city=city,
                        category=category,
                        limit=target,  # strict target cap
                        min_exp=min_exp,
                        max_exp=max_exp,
                        country_param=countries,
                    )
                    jobs = jobs[:target]
                    total = len(jobs)
                    for i, job in enumerate(jobs):
                        if streamed_count >= target:
                            break
                        saved = (
                            upsert_job(dict(job), collection_name=session_collection)
                            if job.get("job_url")
                            else dict(job)
                        )
                        streamed_count += 1
                        db_meta = _live_db_meta(streamed_count)
                        await websocket.send_json({
                            "type": "job",
                            "data": saved,
                            "db": db_meta,
                            "saved": bool(saved.get("saved", True)),
                            "progress": {
                                "current": streamed_count,
                                "total": target,
                                "percentage": int((streamed_count / target) * 100) if target else 100,
                            },
                        })
                        await websocket.send_json({"type": "db_info", "data": db_meta})
                        await asyncio.sleep(0.08)
                else:
                    await websocket.send_json({
                        "type": "status",
                        "message": (
                            "LinkedIn scrape via python-jobspy — each job is "
                            f"upserted into NEW collection "
                            f"`{DATABASE_NAME}.{session_collection}` "
                            f"(target cap {target}, results/q {results_per}, "
                            f"hours_old {hours_old} — no auto-expand)…"
                        ),
                    })

                    event_queue: asyncio.Queue = asyncio.Queue()
                    loop = asyncio.get_running_loop()

                    def on_status(msg: str) -> None:
                        loop.call_soon_threadsafe(
                            event_queue.put_nowait, {"type": "status", "message": msg}
                        )

                    def on_job(job: dict, current: int, tgt: int, db_meta: dict) -> None:
                        meta_out = dict(db_meta or {})
                        meta_out["session_id"] = session_id
                        meta_out["session_saved"] = current
                        meta_out["collection"] = session_collection
                        meta_out["collection_name"] = session_collection
                        meta_out["scrape_json_id"] = scrape_json_id
                        loop.call_soon_threadsafe(
                            event_queue.put_nowait,
                            {
                                "type": "job",
                                "data": dict(job),
                                "db": meta_out,
                                "saved": bool(job.get("saved", False)),
                                "progress": {
                                    "current": current,
                                    "total": tgt,
                                    "percentage": int((current / tgt) * 100) if tgt else 100,
                                },
                            },
                        )
                        loop.call_soon_threadsafe(
                            event_queue.put_nowait,
                            {"type": "db_info", "data": meta_out},
                        )

                    scrape_task = asyncio.create_task(
                        asyncio.to_thread(
                            scrape_external_jobs,
                            search=search,
                            city=city,
                            countries=countries,
                            results_per=results_per,
                            hours_old=hours_old,
                            target=target,
                            min_exp=min_exp,
                            max_exp=max_exp,
                            on_status=on_status,
                            on_job=on_job,
                            collection_name=session_collection,
                            session_id=session_id,
                            scrape_json_id=scrape_json_id,
                            strict_caps=True,
                        )
                    )

                    try:
                        while not scrape_task.done() or not event_queue.empty():
                            try:
                                evt = await asyncio.wait_for(event_queue.get(), timeout=0.4)
                            except asyncio.TimeoutError:
                                if scrape_task.done():
                                    while not event_queue.empty():
                                        evt = event_queue.get_nowait()
                                        if evt.get("type") == "job":
                                            streamed_count += 1
                                        await websocket.send_json(evt)
                                    break
                                continue

                            if evt.get("type") == "job":
                                streamed_count += 1
                            await websocket.send_json(evt)

                        jobs, meta = await scrape_task
                        # Cap streamed_count to target (safety)
                        if streamed_count > target:
                            streamed_count = target
                        if streamed_count == 0 and jobs:
                            for i, job in enumerate(jobs[:target]):
                                saved = upsert_job(
                                    dict(job), collection_name=session_collection
                                )
                                streamed_count += 1
                                db_meta = _live_db_meta(streamed_count)
                                await websocket.send_json({
                                    "type": "job",
                                    "data": saved,
                                    "db": db_meta,
                                    "saved": bool(saved.get("saved", False)),
                                    "progress": {
                                        "current": streamed_count,
                                        "total": target,
                                        "percentage": int((streamed_count / target) * 100),
                                    },
                                })
                                await websocket.send_json({"type": "db_info", "data": db_meta})
                                await asyncio.sleep(0.06)
                    except Exception as scrape_err:
                        print(f"Live scrape failed: {scrape_err}")
                        stream_status = "error"
                        if not scrape_task.done():
                            scrape_task.cancel()
                        await websocket.send_json({
                            "type": "status",
                            "message": (
                                f"Live scrape error: {scrape_err}. "
                                "Trying MongoDB cache into the same new collection…"
                            ),
                        })
                        jobs = collect_jobs_from_live_collection(
                            search=search,
                            city=city,
                            category=category,
                            limit=target,
                            min_exp=min_exp,
                            max_exp=max_exp,
                            country_param=countries,
                        )
                        jobs = jobs[:target]
                        meta = {
                            "expanded_hours": None,
                            "attempts": [],
                            "error": str(scrape_err),
                            "collection_name": session_collection,
                        }
                        for i, job in enumerate(jobs):
                            if streamed_count >= target:
                                break
                            saved = (
                                upsert_job(dict(job), collection_name=session_collection)
                                if job.get("job_url")
                                else dict(job)
                            )
                            streamed_count += 1
                            db_meta = _live_db_meta(streamed_count)
                            await websocket.send_json({
                                "type": "job",
                                "data": saved,
                                "db": db_meta,
                                "saved": bool(saved.get("saved", True)),
                                "progress": {
                                    "current": streamed_count,
                                    "total": target,
                                    "percentage": (
                                        int((streamed_count / target) * 100) if target else 100
                                    ),
                                },
                            })
                            await websocket.send_json({"type": "db_info", "data": db_meta})
                            await asyncio.sleep(0.08)
                        if streamed_count > 0:
                            stream_status = "completed_from_cache"

                # Strict mode never expands hours — this branch is defensive only
                if meta.get("expanded_hours"):
                    await websocket.send_json({
                        "type": "status",
                        "message": (
                            f"Window expanded to {meta['expanded_hours']}h "
                            f"(no postings in the original hours_old range)."
                        ),
                    })

                # Persist final counts so Dashboard shows this run
                finalize_live_stream_snapshot(
                    scrape_json_id,
                    job_count=streamed_count,
                    status=stream_status,
                    extra_filters=stream_filters,
                )

                final_db = _live_db_meta(streamed_count)
                await websocket.send_json({"type": "db_info", "data": final_db})

                if streamed_count == 0:
                    await websocket.send_json({
                        "type": "status",
                        "message": (
                            "Found 0 LinkedIn jobs within strict caps "
                            f"(hours_old={hours_old}, target={target}, "
                            f"results_per={results_per}). "
                            "Increase Hours Old, broaden query, or pick more cities. "
                            "Empty collection still registered on Dashboard."
                        ),
                    })

                await websocket.send_json({
                    "type": "completed",
                    "message": (
                        f"Real-time scrape completed. Streamed {streamed_count}/{target} jobs "
                        f"(strict caps). Saved to "
                        f"{final_db['dbname']}.{final_db['collection']} "
                        f"(collection total_count={final_db['total_count']}). "
                        f"Visible on Dashboard as scrape snapshot."
                    ),
                    "total": streamed_count,
                    "target": target,
                    "collection_name": session_collection,
                    "scrape_json_id": scrape_json_id,
                    "db": final_db,
                })
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn

    ensure_config_document()
    print(f"Starting Job Portal API at {BASE_URL}")
    print(f"CORS origins: {', '.join(CORS_ORIGINS)}")
    uvicorn.run(
        "app:app",
        host=HOST,
        port=PORT,
        reload=RELOAD,
    )
