"""DuckDB implementation of the durable store. Telemetry stays on the Parquet lake."""

from __future__ import annotations

import json
from datetime import datetime, timezone


def _as_dt(value) -> datetime:
    """UTC timestamp for DuckDB. Naive inputs are read as UTC, not local time."""
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, str) and value:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        moment = datetime.now(timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def _float(value, default=0.0):
    if value is None or value == "":
        return default
    return float(value)


def _text(value, default: str = "") -> str:
    if value is None:
        return default
    return getattr(value, "value", value) if not isinstance(value, str) else value


class DuckDBDurableStore:
    def __init__(self, database=None):
        self._db = database

    def _repos(self):
        """SQL goes through the repository seam. An injected database stays on that connection."""
        from src.data_services.repositories import duckdb_repositories, get_repositories

        if self._db is None:
            return get_repositories()
        bundle = getattr(self, "_bundle", None)
        if bundle is None or getattr(bundle, "database", None) is not self._db:
            self._bundle = duckdb_repositories(self._db)
        return self._bundle

    def save_telemetry(self, record: dict) -> None:
        from src.data.historical import append_telemetry

        append_telemetry([record])

    def save_track(self, track: dict) -> None:
        track_id = str(track.get("track_id") or track.get("id") or "")
        if not track_id:
            raise ValueError("track id required")
        source_ids = track.get("source_asset_ids") or []
        asset_id = track.get("asset_id") or track.get("source_asset")
        if not asset_id and source_ids:
            asset_id = source_ids[0]
        self._repos().tracks.upsert(
            track_id=track_id,
            updated_at=_as_dt(
                track.get("updated_at")
                or track.get("last_update")
                or track.get("last_seen")
                or track.get("ingested_at")
            ),
            asset_id=None if asset_id is None else str(asset_id),
            lat=_float(track.get("lat"), default=None),
            lon=_float(track.get("lon"), default=None),
            label=str(_text(track.get("label"), "unknown")),
            status=str(_text(track.get("status") or track.get("state"), "")),
            confidence=_float(track.get("confidence"), default=None),
            payload=track,
        )

    def save_detection(self, detection: dict) -> None:
        detection_id = str(detection.get("detection_id") or detection.get("id") or "")
        if not detection_id:
            raise ValueError("detection id required")
        self._repos().detections.insert(
            detection_id=detection_id,
            timestamp=_as_dt(detection.get("timestamp") or detection.get("ingested_at")),
            asset_id=str(detection.get("asset_id") or ""),
            lat=_float(detection.get("lat")),
            lon=_float(detection.get("lon")),
            label=str(_text(detection.get("label"), "unknown")),
            confidence=_float(detection.get("confidence")),
            mission_id=detection.get("mission_id"),
        )

    def save_alert(self, alert: dict) -> None:
        alert_id = str(alert.get("alert_id") or alert.get("id") or "")
        if not alert_id:
            raise ValueError("alert id required")
        asset_id = alert.get("asset_id")
        self._repos().alerts.insert(
            alert_id=alert_id,
            timestamp=_as_dt(alert.get("timestamp") or alert.get("ingested_at")),
            asset_id=None if asset_id is None else str(asset_id),
            level=str(_text(alert.get("level") or alert.get("severity"), "info")),
            message=str(alert.get("message") or alert.get("title") or ""),
            payload=alert,
        )

    def save_command(self, command: dict) -> None:
        command_id = str(command.get("command_id") or command.get("id") or "")
        if not command_id:
            raise ValueError("command id required")
        parameters = command.get("parameters") or {}
        if not isinstance(parameters, dict):
            parameters = {"value": parameters}
        self._repos().commands.insert(
            command_id=command_id,
            asset_id=str(command.get("asset_id") or ""),
            command_type=str(_text(command.get("command_type") or command.get("type"), "custom")),
            parameters=parameters,
            issued_by=str(command.get("issued_by") or command.get("actor") or "operator"),
            issued_at=_as_dt(
                command.get("issued_at") or command.get("timestamp") or command.get("ingested_at")
            ),
        )
