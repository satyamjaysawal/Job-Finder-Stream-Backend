"""JWT auth: user + admin roles. No guest role.

Continue-as-user creates an anonymous Mongo account with role=user (full product access).
Admin can delete snapshots and mutate scraper config.
"""

from __future__ import annotations

import binascii
import hashlib
import hmac
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import jwt
from fastapi import Depends, Header, HTTPException
from pydantic import BaseModel, Field
from pymongo import MongoClient

ROLE_USER = "user"
ROLE_ADMIN = "admin"
ALL_ROLES = (ROLE_USER, ROLE_ADMIN)

JWT_SECRET = os.getenv("JWT_SECRET") or os.getenv("MONGODB_URI") or "job-finder-change-me"
JWT_HOURS = int(os.getenv("JWT_HOURS", "168"))
USERS_COLLECTION = "users"

_client: MongoClient | None = None


class AuthLogin(BaseModel):
    email: str
    password: str


class AuthRegister(BaseModel):
    email: str
    password: str
    name: str = ""
    role: str = Field(default=ROLE_USER)


class ProfileUpdate(BaseModel):
    name: str = ""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def users_col():
    global _client
    uri = os.getenv("MONGODB_URI")
    if not uri:
        raise HTTPException(status_code=503, detail="MongoDB is not available")
    if _client is None:
        _client = MongoClient(uri)
    db_name = os.getenv("DATABASE_NAME", "job_portal")
    col = _client[db_name][USERS_COLLECTION]
    try:
        col.create_index("email", unique=True)
        col.create_index("user_id", unique=True)
    except Exception:
        pass
    return col


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120_000)
    return f"pbkdf2${binascii.hexlify(salt).decode()}${binascii.hexlify(dk).decode()}"


def check_password(password: str, hashed: str) -> bool:
    try:
        kind, salt_hex, dk_hex = (hashed or "").split("$", 2)
        if kind != "pbkdf2":
            return False
        salt = binascii.unhexlify(salt_hex)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120_000)
        return hmac.compare_digest(binascii.hexlify(dk).decode(), dk_hex)
    except Exception:
        return False


def public_user(doc: dict[str, Any]) -> dict[str, Any]:
    return {
        "user_id": doc.get("user_id"),
        "email": doc.get("email"),
        "name": doc.get("name") or "",
        "role": doc.get("role") or ROLE_USER,
        "anonymous": bool(doc.get("anonymous")),
        "created_at": doc.get("created_at"),
        "updated_at": doc.get("updated_at"),
    }


def make_token(user_id: str, email: str, role: str = ROLE_USER) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def auth_payload(doc: dict[str, Any]) -> dict[str, Any]:
    user = public_user(doc)
    return {
        "token": make_token(user["user_id"], user["email"], user["role"]),
        "user": user,
    }


def register_user(email: str, password: str, name: str, role: str = ROLE_USER) -> dict[str, Any]:
    email = (email or "").strip().lower()
    name = (name or "").strip() or email.split("@")[0]
    role = role if role in ALL_ROLES else ROLE_USER
    if "@" not in email or "." not in email.split("@")[-1]:
        raise HTTPException(status_code=400, detail="Enter a valid email")
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    col = users_col()
    if col.find_one({"email": email}):
        raise HTTPException(status_code=409, detail="Email already registered")
    doc = {
        "user_id": str(uuid4()),
        "email": email,
        "name": name,
        "role": role,
        "password_hash": hash_password(password),
        "anonymous": False,
        "created_at": _now(),
        "updated_at": _now(),
    }
    col.insert_one(doc)
    return doc


def login_user(email: str, password: str) -> dict[str, Any]:
    email = (email or "").strip().lower()
    doc = users_col().find_one({"email": email})
    if not doc or not check_password(password, doc.get("password_hash") or ""):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return doc


def create_anonymous_user() -> dict[str, Any]:
    """No-login full-access account (role=user). Not a limited guest."""
    user_id = str(uuid4())
    doc = {
        "user_id": user_id,
        "email": f"anon-{user_id[:8]}@anonymous.local",
        "name": "User",
        "role": ROLE_USER,
        "password_hash": hash_password(uuid4().hex),
        "anonymous": True,
        "created_at": _now(),
        "updated_at": _now(),
    }
    users_col().insert_one(doc)
    return doc


def find_user(user_id: str) -> dict[str, Any] | None:
    if not user_id:
        return None
    doc = users_col().find_one({"user_id": user_id})
    return public_user(doc) if doc else None


def current_user(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Login required")
    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status_code=401, detail="Session expired") from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc
    user = find_user(str(payload.get("sub") or ""))
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    if not user.get("role"):
        user["role"] = payload.get("role") or ROLE_USER
    return user


def require_role(*allowed_roles: str):
    allowed = set(allowed_roles) or {ROLE_ADMIN}

    def _check(user: dict[str, Any] = Depends(current_user)) -> dict[str, Any]:
        if (user.get("role") or ROLE_USER) not in allowed:
            raise HTTPException(
                status_code=403,
                detail=f"Requires role: {', '.join(sorted(allowed))}. Your role: {user.get('role')}",
            )
        return user

    return _check
