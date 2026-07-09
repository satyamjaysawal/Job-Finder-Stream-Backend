"""
Application settings loaded via python-dotenv.

Usage:
  - Put variables in a `.env` file (backend working directory or any parent).
  - `load_dotenv()` discovers `.env` automatically — no hard-coded paths.
  - Process environment variables always win over `.env` values.
"""

from __future__ import annotations

import os
import sys
from typing import List

from dotenv import find_dotenv, load_dotenv

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
# find_dotenv() walks upward from the current working directory.
# A second call with usecwd=False also checks near this package if needed.
load_dotenv(find_dotenv(usecwd=True), override=False)
load_dotenv(find_dotenv(), override=False)


def _env(key: str, default: str | None = None) -> str | None:
    """Read a string env var; strip whitespace; treat empty as missing."""
    raw = os.getenv(key)
    if raw is None:
        return default
    value = str(raw).strip()
    return value if value else default


def _env_int(key: str, default: int) -> int:
    raw = _env(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(key: str, default: bool = False) -> bool:
    raw = _env(key)
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def _env_csv(key: str, default: str = "") -> List[str]:
    """Comma-separated list → list of non-empty stripped strings."""
    raw = _env(key, default) or ""
    return [part.strip() for part in raw.split(",") if part.strip()]


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------
HOST: str = _env("HOST", "127.0.0.1") or "127.0.0.1"
PORT: int = _env_int("PORT", 5000)
BASE_URL: str = (_env("BASE_URL") or f"http://{HOST}:{PORT}").rstrip("/")
API_PREFIX: str = (_env("API_PREFIX", "/api") or "/api").rstrip("/") or "/api"
RELOAD: bool = _env_bool("RELOAD", True)

# ---------------------------------------------------------------------------
# CORS — explicit origins (never use "*" with credentials)
# ---------------------------------------------------------------------------
FRONTEND_URL: str = (
    _env("FRONTEND_URL", "http://127.0.0.1:5173") or "http://127.0.0.1:5173"
).rstrip("/")

_DEFAULT_CORS = (
    f"{FRONTEND_URL},"
    "http://127.0.0.1:5173,"
    "http://localhost:5173,"
    "http://127.0.0.1:4173,"
    "http://localhost:4173"
)

_raw_cors = _env_csv("CORS_ORIGINS", _DEFAULT_CORS)
# De-dupe while preserving order
CORS_ORIGINS: List[str] = []
_seen_origins: set[str] = set()
for _origin in _raw_cors:
    if _origin not in _seen_origins:
        _seen_origins.add(_origin)
        CORS_ORIGINS.append(_origin)

CORS_ALLOW_CREDENTIALS: bool = _env_bool("CORS_ALLOW_CREDENTIALS", True)
CORS_ALLOW_METHODS: List[str] = _env_csv(
    "CORS_ALLOW_METHODS", "GET,POST,PUT,PATCH,DELETE,OPTIONS"
)
CORS_ALLOW_HEADERS: List[str] = _env_csv(
    "CORS_ALLOW_HEADERS", "Authorization,Content-Type,Accept,Origin,X-Requested-With"
)

# ---------------------------------------------------------------------------
# MongoDB (required)
# ---------------------------------------------------------------------------
MONGODB_URI: str | None = _env("MONGODB_URI")
DATABASE_NAME: str | None = _env("DATABASE_NAME")

# Collection names
JOBS_COLLECTION_NAME: str = _env("JOBS_COLLECTION_NAME", "jobs") or "jobs"
CONFIG_COLLECTION_NAME: str = _env("CONFIG_COLLECTION_NAME", "config") or "config"
SCRAPE_JSONS_COLLECTION_NAME: str = (
    _env("SCRAPE_JSONS_COLLECTION_NAME", "scrape_jsons") or "scrape_jsons"
)

# Fixed key so config always lives in one document inside `config`.
CONFIG_DOC_KEY: str = "scraper_settings"
CONFIG_FILTER: dict = {"_key": CONFIG_DOC_KEY}

# Reserved collection names — never use as snapshot names.
RESERVED_COLLECTIONS: frozenset[str] = frozenset(
    {
        JOBS_COLLECTION_NAME,
        CONFIG_COLLECTION_NAME,
        SCRAPE_JSONS_COLLECTION_NAME,
        "system.indexes",
    }
)


def require_mongo_settings() -> None:
    """Raise a clear error if Mongo env vars are missing."""
    if not MONGODB_URI:
        raise RuntimeError(
            "MONGODB_URI is not set. Add it to your .env (python-dotenv) or environment."
        )
    if not DATABASE_NAME:
        raise RuntimeError(
            "DATABASE_NAME is not set. Add it to your .env (python-dotenv) or environment."
        )


def configure_stdio_utf8() -> None:
    """Prefer UTF-8 on Windows consoles (emoji / non-ASCII logs)."""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8")
            except Exception:
                pass


def public_settings_dict() -> dict:
    """Non-secret settings safe to expose via /api/health or docs."""
    return {
        "base_url": BASE_URL,
        "api_prefix": API_PREFIX,
        "host": HOST,
        "port": PORT,
        "frontend_url": FRONTEND_URL,
        "cors_origins": list(CORS_ORIGINS),
        "database": DATABASE_NAME,
    }
