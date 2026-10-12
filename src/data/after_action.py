"""After-action Parquet for one mission: telemetry, commands, events, alerts.

Files land in a local folder. When MINIO_ENDPOINT is set and the minio
package imports, the same files are uploaded. A missing package or endpoint
leaves the local folder as the export.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from src.data_services.repositories import get_repositories

TABLES = ("telemetry", "commands", "events", "alerts")

_SCHEMAS: dict[str, dict[str, pl.DataType]] = {
    "telemetry": {
        "mission_id": pl.Utf8,
        "asset_id": pl.Utf8,
        "time": pl.Utf8,
        "lat": pl.Float64,
        "lon": pl.Float64,
        "alt": pl.Float64,
        "heading": pl.Float64,
        "speed": pl.Float64,
        "battery_percent": pl.Float64,
        "status": pl.Utf8,
        "mode": pl.Utf8,
    },
    "commands": {
        "mission_id": pl.Utf8,
        "command_id": pl.Utf8,
        "asset_id": pl.Utf8,
        "command_type": pl.Utf8,
        "parameters": pl.Utf8,
        "issued_by": pl.Utf8,
        "issued_at": pl.Utf8,
    },
    "events": {
        "mission_id": pl.Utf8,
        "event_id": pl.Utf8,
        "time": pl.Utf8,
        "event_type": pl.Utf8,
        "asset_id": pl.Utf8,
        "message": pl.Utf8,
        "severity": pl.Utf8,
        "data": pl.Utf8,
    },
    "alerts": {
        "mission_id": pl.Utf8,
        "alert_id": pl.Utf8,
        "time": pl.Utf8,
        "asset_id": pl.Utf8,
        "level": pl.Utf8,
        "message": pl.Utf8,
        "payload": pl.Utf8,
    },
}

_DUCK = {
    "telemetry": """
        SELECT asset_id, timestamp AS time, lat, lon, alt, heading, speed,
               battery_percent, status, mode
        FROM telemetry WHERE asset_id IN ({marks})
    """,
    "commands": """
        SELECT command_id, asset_id, command_type, parameters, issued_by, issued_at
        FROM commands WHERE asset_id IN ({marks})
    """,
    "events": """
        SELECT event_id, timestamp AS time, event_type, asset_id, message, severity, data
        FROM events WHERE asset_id IN ({marks})
    """,
    "alerts": """
        SELECT alert_id, timestamp AS time, asset_id, level, message, payload
        FROM alerts WHERE asset_id IN ({marks})
    """,
}

_POSTGRES = {
    "telemetry": """
        SELECT asset_id, "time" AS time, lat, lon, alt, heading, speed,
               battery_percent, status, mode
        FROM telemetry WHERE asset_id IN ({marks})
    """,
    "commands": """
        SELECT command_id, asset_id, command_type, parameters, issued_by, issued_at
        FROM commands WHERE asset_id IN ({marks})
    """,
    "events": """
        SELECT event_id, "time" AS time, event_type, asset_id, message, severity, data
        FROM events WHERE asset_id IN ({marks})
    """,
    "alerts": """
        SELECT alert_id, "time" AS time, asset_id, level, message, payload
        FROM alerts WHERE asset_id IN ({marks})
    """,
}


def _safe_mission_id(mission_id: str) -> str:
    cleaned = "".join(char for char in mission_id if char.isalnum() or char in "-_")
    return cleaned or "mission"


def _norm(value):
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    if isinstance(value, bytes):
        return value.decode()
    return value


def _fetch_duck(con, sql: str, asset_ids: list[str]) -> list[dict]:
    cursor = con.execute(sql, asset_ids)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _fetch_postgres(repos, sql: str, asset_ids: list[str]) -> list[dict]:
    binds = {f"a{index}": asset_id for index, asset_id in enumerate(asset_ids)}
    from sqlalchemy import text

    with repos.engine.connect() as conn:
        rows = conn.execute(text(sql), binds).mappings()
        return [dict(row) for row in rows]


def _collect(asset_ids: list[str]) -> dict[str, list[dict]]:
    empty = {name: [] for name in TABLES}
    if not asset_ids:
        return empty
    repos = get_repositories()
    if getattr(repos, "backend", "") == "postgres":
        marks = ", ".join(f":a{index}" for index in range(len(asset_ids)))
        return {
            name: _fetch_postgres(repos, sql.format(marks=marks), asset_ids)
            for name, sql in _POSTGRES.items()
        }
    marks = ", ".join("?" for _ in asset_ids)
    return {
        name: _fetch_duck(repos.database.con, sql.format(marks=marks), asset_ids)
        for name, sql in _DUCK.items()
    }


def _frame(mission_id: str, name: str, rows: list[dict]) -> pl.DataFrame:
    schema = _SCHEMAS[name]
    records = []
    for row in rows:
        item = {"mission_id": mission_id}
        for key in schema:
            if key == "mission_id":
                continue
            item[key] = _norm(row.get(key))
        records.append(item)
    if not records:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(records, schema=schema)


def _upload(folder: Path, mission_id: str) -> bool:
    endpoint = os.environ.get("MINIO_ENDPOINT", "").strip()
    if not endpoint:
        return False
    try:
        from minio import Minio
    except ImportError:
        return False
    secure = os.environ.get("MINIO_SECURE", "").lower() in {"1", "true", "yes"}
    client = Minio(
        endpoint,
        access_key=os.environ.get("MINIO_ACCESS_KEY", ""),
        secret_key=os.environ.get("MINIO_SECRET_KEY", ""),
        secure=secure,
    )
    bucket = os.environ.get("MINIO_BUCKET", "cnc-after-action")
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    for path in sorted(folder.glob("*.parquet")):
        client.fput_object(bucket, f"{mission_id}/{path.name}", str(path))
    return True


def write_after_action(
    mission_id: str,
    asset_ids: list[str] | None = None,
    folder: str | Path | None = None,
) -> dict:
    """Write four Parquet files for the assets assigned to this mission."""
    safe = _safe_mission_id(mission_id)
    dest = Path(folder) if folder is not None else Path("data/after_action") / safe
    dest.mkdir(parents=True, exist_ok=True)
    collected = _collect(list(asset_ids or []))
    files: dict[str, str] = {}
    for name in TABLES:
        path = dest / f"{name}.parquet"
        _frame(mission_id, name, collected[name]).write_parquet(path)
        files[name] = str(path)
    uploaded = False
    try:
        uploaded = _upload(dest, safe)
    except Exception:
        uploaded = False
    return {
        "mission_id": mission_id,
        "folder": str(dest),
        "files": files,
        "uploaded": uploaded,
    }
