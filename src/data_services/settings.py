"""Connection settings from the process environment, filled from .env when unset.

Database, Redis, and NATS connection keys are copied. STORAGE_BACKEND and
CNC_BUS stay process settings so a local .env cannot move the test run
onto Postgres or JetStream.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = ROOT / ".env"
_KEYS = (
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_APP_USER",
    "POSTGRES_APP_PASSWORD",
    "REDIS_PASSWORD",
    "CNC_REDIS_URL",
    "NATS_TOKEN",
    "CNC_NATS_URL",
)
_loaded = False


def load_connection_env() -> None:
    global _loaded
    if _loaded:
        return
    _loaded = True
    if not _ENV_FILE.exists():
        return
    values = dotenv_values(_ENV_FILE)
    for key in _KEYS:
        if os.environ.get(key):
            continue
        loaded = values.get(key)
        if loaded:
            os.environ[key] = loaded


def setting(name: str, default: str = "") -> str:
    load_connection_env()
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()
