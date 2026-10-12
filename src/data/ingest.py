"""Dual-path ingest: Redis hot state, then the durable store.

Store failures are logged and swallowed. A missing id is a caller error
and still raises, so a GOTO is not dropped when Redis is down.
"""

from __future__ import annotations

from datetime import datetime, timezone

from loguru import logger

_live = None
_durable = None


def configure(live=None, durable=None) -> None:
    """Install test fakes, or pass None to return to the process defaults."""
    global _live, _durable
    _live = live
    _durable = durable


def get_live():
    global _live
    if _live is None:
        from src.data.redis_live import RedisLiveStore

        _live = RedisLiveStore()
    return _live


def get_durable():
    global _durable
    if _durable is None:
        from src.data_services.durable import DuckDBDurableStore

        _durable = DuckDBDurableStore()
    return _durable


def _stamp(payload: dict, actor: str | None) -> dict:
    record = dict(payload)
    record.setdefault("ingested_at", datetime.now(timezone.utc).isoformat())
    if actor and "actor" not in record:
        record["actor"] = actor
    return record


def _require_id(record: dict, *keys: str) -> str:
    for key in keys:
        value = record.get(key)
        if value:
            return str(value)
    raise ValueError(f"missing id, looked for {keys}")


def _write_live(kind: str, object_id: str, record: dict) -> None:
    try:
        get_live().put(kind, object_id, record)
    except Exception as exc:
        logger.warning(f"[ingest] live {kind} {object_id} failed: {exc}")


def _write_durable(method: str, record: dict) -> None:
    try:
        getattr(get_durable(), method)(record)
    except Exception as exc:
        logger.warning(f"[ingest] durable {method} failed: {exc}")


def ingest_telemetry(platform_id: str, state: dict, role: str = "system", pedigree: str = "cnc"):
    """Live Redis + historical Parquet. role / pedigree are the minimal ICD fields."""
    record = _stamp(
        {
            **state,
            "platform_id": platform_id,
            "role": role,
            "pedigree": pedigree,
            "ts": datetime.now(timezone.utc).isoformat(),
        },
        actor=role,
    )
    record["updated_at"] = record["ts"]
    _write_live("platform", platform_id, record)
    _write_durable("save_telemetry", record)
    return record


def ingest_track(track: dict, actor: str = "fusion") -> dict:
    record = _stamp(track, actor)
    track_id = _require_id(record, "track_id", "id")
    record["track_id"] = track_id
    _write_live("track", track_id, record)
    _write_durable("save_track", record)
    return record


def ingest_detection(detection: dict, actor: str = "fusion") -> dict:
    record = _stamp(detection, actor)
    detection_id = _require_id(record, "detection_id", "id")
    record["detection_id"] = detection_id
    _write_live("detection", detection_id, record)
    _write_durable("save_detection", record)
    return record


def ingest_alert(alert: dict, actor: str = "operator") -> dict:
    record = _stamp(alert, actor)
    alert_id = _require_id(record, "alert_id", "id")
    record["alert_id"] = alert_id
    _write_live("alert", alert_id, record)
    _write_durable("save_alert", record)
    return record


def ingest_command(command: dict, actor: str = "operator") -> dict:
    """Hot command plus the durable command row. The command Event stays with the caller."""
    record = _stamp(command, actor)
    command_id = _require_id(record, "command_id", "id")
    record["command_id"] = command_id
    _write_live("command", command_id, record)
    _write_durable("save_command", record)
    return record
