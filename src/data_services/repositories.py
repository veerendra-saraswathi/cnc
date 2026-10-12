"""Repository seam for the ten kernel stores.

DuckDB is the default. STORAGE_BACKEND=postgres returns the SQLAlchemy
backend and does not open a connection until a method runs.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Protocol

from src.data_services.db import db as duckdb
from src.data_services.ids import uuid7
from src.data_services.outbox_record import OutboxRecord, as_payload, issued_parts, utcnow
from src.kernel.models import OperatorAction, Zone

BACKENDS = ("duckdb", "postgres")


def storage_backend() -> str:
    raw = os.environ.get("STORAGE_BACKEND", "duckdb")
    value = (raw or "duckdb").strip().lower()
    if value not in BACKENDS:
        raise RuntimeError(f"STORAGE_BACKEND must be duckdb or postgres, not {raw!r}")
    return value


class AssetRepository(Protocol):
    def upsert(self, asset_id: str, name: str, asset_type: str,
               capabilities: list = None, max_speed: float = None) -> None: ...


class TelemetryRepository(Protocol):
    def insert(self, asset_id: str, timestamp: datetime,
               lat: float, lon: float, alt: float, heading: float,
               speed: float, battery: float, status: str, mode: str) -> None: ...


class CommandRepository(Protocol):
    def insert(self, command_id: str, asset_id: str, command_type: str,
               parameters: dict, issued_by: str = "operator",
               issued_at: datetime = None) -> None: ...


class EventRepository(Protocol):
    def insert(self, event_id: str, timestamp: datetime, event_type: str,
               message: str, asset_id: str = None,
               severity: str = "info", data: dict = None) -> None: ...


class OperatorActionRepository(Protocol):
    def add(self, action: OperatorAction) -> None: ...

    def list(self) -> list[OperatorAction]: ...


class MissionRepository(Protocol):
    def upsert(self, mission_id: str, name: str, status: str,
               detection_lat: float = None, detection_lon: float = None,
               assigned_uavs: list = None, assigned_ugv: str = None,
               completed_at: datetime = None) -> None: ...


class TrackRepository(Protocol):
    def upsert(self, track_id: str, updated_at: datetime, asset_id: str = None,
               lat: float = None, lon: float = None, label: str = "unknown",
               status: str = "", confidence: float = None, payload: dict = None) -> None: ...


class DetectionRepository(Protocol):
    def insert(self, detection_id: str, timestamp: datetime,
               asset_id: str, lat: float, lon: float,
               label: str = "unknown", confidence: float = 0.0,
               mission_id: str = None) -> None: ...


class AlertRepository(Protocol):
    def insert(self, alert_id: str, timestamp: datetime, asset_id: str = None,
               level: str = "info", message: str = "", payload: dict = None) -> None: ...


class ZoneRepository(Protocol):
    def add(self, zone: Zone) -> None: ...

    def list(self) -> list[Zone]: ...


class Repositories(Protocol):
    backend: str
    assets: AssetRepository
    telemetry: TelemetryRepository
    commands: CommandRepository
    events: EventRepository
    operator_actions: OperatorActionRepository
    missions: MissionRepository
    tracks: TrackRepository
    detections: DetectionRepository
    alerts: AlertRepository
    zones: ZoneRepository


class OperatorSnapshotStore(Protocol):
    """Async SQLite snapshots of operator assets, missions, tracks, and alerts."""

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def save_asset(self, asset) -> None: ...

    async def load_assets(self) -> list: ...

    async def save_mission(self, mission) -> None: ...

    async def load_missions(self) -> list: ...

    async def save_track(self, track) -> None: ...

    async def load_tracks(self) -> list: ...

    async def save_alert(self, alert) -> None: ...

    async def load_alerts(self, limit: int = 50) -> list: ...

    async def save_observation(self, obs) -> None: ...


class _DuckAsset:
    def __init__(self, database):
        self._db = database

    def upsert(self, asset_id: str, name: str, asset_type: str,
               capabilities: list = None, max_speed: float = None) -> None:
        self._db.upsert_asset(asset_id, name, asset_type, capabilities, max_speed)


class _DuckTelemetry:
    def __init__(self, database):
        self._db = database

    def insert(self, asset_id: str, timestamp: datetime,
               lat: float, lon: float, alt: float, heading: float,
               speed: float, battery: float, status: str, mode: str) -> None:
        self._db.insert_telemetry(
            asset_id, timestamp, lat, lon, alt, heading, speed, battery, status, mode,
        )


class _DuckCommand:
    def __init__(self, database):
        self._db = database

    def insert(self, command_id: str, asset_id: str, command_type: str,
               parameters: dict, issued_by: str = "operator",
               issued_at: datetime = None) -> None:
        self._db.insert_command(
            command_id, asset_id, command_type, parameters, issued_by, issued_at,
        )


class _DuckEvent:
    def __init__(self, database):
        self._db = database

    def insert(self, event_id: str, timestamp: datetime, event_type: str,
               message: str, asset_id: str = None,
               severity: str = "info", data: dict = None) -> None:
        self._db.insert_event(
            event_id, timestamp, event_type, message, asset_id, severity, data,
        )


class _DuckMission:
    def __init__(self, database):
        self._db = database

    def upsert(self, mission_id: str, name: str, status: str,
               detection_lat: float = None, detection_lon: float = None,
               assigned_uavs: list = None, assigned_ugv: str = None,
               completed_at: datetime = None) -> None:
        self._db.upsert_mission(
            mission_id, name, status, detection_lat, detection_lon,
            assigned_uavs, assigned_ugv, completed_at,
        )


class _DuckTrack:
    def __init__(self, database):
        self._db = database

    def upsert(self, track_id: str, updated_at: datetime, asset_id: str = None,
               lat: float = None, lon: float = None, label: str = "unknown",
               status: str = "", confidence: float = None, payload: dict = None) -> None:
        self._db.upsert_track(
            track_id, updated_at, asset_id, lat, lon, label, status, confidence, payload,
        )


class _DuckDetection:
    def __init__(self, database):
        self._db = database

    def insert(self, detection_id: str, timestamp: datetime,
               asset_id: str, lat: float, lon: float,
               label: str = "unknown", confidence: float = 0.0,
               mission_id: str = None) -> None:
        self._db.insert_detection(
            detection_id, timestamp, asset_id, lat, lon, label, confidence, mission_id,
        )


class _DuckAlert:
    def __init__(self, database):
        self._db = database

    def insert(self, alert_id: str, timestamp: datetime, asset_id: str = None,
               level: str = "info", message: str = "", payload: dict = None) -> None:
        self._db.insert_alert(alert_id, timestamp, asset_id, level, message, payload)


class InMemoryOperatorActionRepository:
    """No operator_actions table yet. The list is the current store."""

    def __init__(self) -> None:
        self.records: list[OperatorAction] = []

    def add(self, action: OperatorAction) -> None:
        self.records.append(action)

    def list(self) -> list[OperatorAction]:
        return list(self.records)


class InMemoryZoneRepository:
    """Zone polygons for the duckdb backend. Postgres stores them in PostGIS."""

    def __init__(self) -> None:
        self.records: list[Zone] = []

    def add(self, zone: Zone) -> None:
        self.records = [item for item in self.records if item.id != zone.id]
        self.records.append(zone)

    def remove(self, zone_id: str) -> None:
        self.records = [item for item in self.records if item.id != zone_id]

    def list(self) -> list[Zone]:
        return list(self.records)


class DuckDbRepositories:
    backend = "duckdb"

    def __init__(self, database=None):
        database = database if database is not None else duckdb
        self.database = database
        self.assets = _DuckAsset(database)
        self.telemetry = _DuckTelemetry(database)
        self.commands = _DuckCommand(database)
        self.events = _DuckEvent(database)
        self.operator_actions = InMemoryOperatorActionRepository()
        self.missions = _DuckMission(database)
        self.tracks = _DuckTrack(database)
        self.detections = _DuckDetection(database)
        self.alerts = _DuckAlert(database)
        self.zones = InMemoryZoneRepository()

    def commit_issued(self, command, event, subject: str, outbox_id: str | None = None) -> str:
        oid = outbox_id or uuid7()
        self.database.commit_issued(issued_parts(command, event, subject, oid, utcnow()))
        return oid

    def list_unpublished(self) -> list[OutboxRecord]:
        return [_record(row) for row in self.database.list_unpublished()]

    def mark_published(self, outbox_id: str, when) -> None:
        self.database.mark_published(outbox_id, when)

    def note_attempt(self, outbox_id: str) -> None:
        self.database.note_attempt(outbox_id)

    def mark_published_command(self, command_id: str, when) -> None:
        self.database.mark_published_command(command_id, when)

    def command_published(self, command_id: str) -> bool:
        return self.database.command_published(command_id)

    def update_command_result(self, command_id: str, status: str, result) -> None:
        self.database.update_command_result(command_id, status, result)

    def command_event_recorded(self, command_id: str) -> bool:
        return self.database.command_event_recorded(command_id)

    def list_recent_commands(self, limit: int = 15) -> list[dict]:
        return self.database.list_recent_commands(limit)

    def list_recent_events(self, limit: int = 500) -> list[dict]:
        return self.database.list_recent_events(limit)


class PostgresSnapshot:
    """JSON operator snapshot in Postgres. Construction does not connect."""

    def __init__(self) -> None:
        self._engine = None

    async def connect(self) -> None:
        import asyncio
        await asyncio.to_thread(self._connect)

    def _connect(self) -> None:
        from src.data_services.postgres_snapshot import connect_snapshot
        self._engine = connect_snapshot()

    async def close(self) -> None:
        self._engine = None

    async def save_asset(self, asset) -> None:
        await self._save("asset", asset.id, asset.model_dump(mode="json"))

    async def load_assets(self) -> list:
        from src.kernel.models import Asset
        return await self._load("asset", Asset)

    async def save_mission(self, mission) -> None:
        await self._save("mission", mission.id, mission.model_dump(mode="json"))

    async def load_missions(self) -> list:
        from src.kernel.models import Mission
        return await self._load("mission", Mission)

    async def save_track(self, track) -> None:
        from src.kernel.models import Track
        kept = track.model_copy()
        kept.history = kept.history[-10:]
        await self._save("track", kept.id, kept.model_dump(mode="json"))

    async def load_tracks(self) -> list:
        from src.kernel.models import Track
        return await self._load("track", Track)

    async def save_alert(self, alert) -> None:
        await self._save("alert", alert.id, alert.model_dump(mode="json"))

    async def load_alerts(self, limit: int = 50) -> list:
        from src.kernel.models import Alert
        return await self._load("alert", Alert, limit=limit)

    async def save_observation(self, obs) -> None:
        from uuid import uuid4
        await self._save("observation", str(uuid4()), obs.model_dump(mode="json"))

    async def _save(self, kind: str, object_id: str, document: dict) -> None:
        import asyncio
        await asyncio.to_thread(self._save_sync, kind, object_id, document)

    def _save_sync(self, kind: str, object_id: str, document: dict) -> None:
        from src.data_services.postgres_snapshot import save_document
        save_document(self._require(), kind, object_id, document)

    async def _load(self, kind: str, model, limit: int | None = None) -> list:
        import asyncio
        rows = await asyncio.to_thread(self._load_sync, kind, limit)
        return [model.model_validate(row) for row in rows]

    def _load_sync(self, kind: str, limit: int | None) -> list:
        from src.data_services.postgres_snapshot import load_documents
        return load_documents(self._require(), kind, limit)

    def _require(self):
        if self._engine is None:
            raise RuntimeError("Postgres snapshot is not connected")
        return self._engine


_duckdb_repos: Optional[DuckDbRepositories] = None


def get_repositories() -> Repositories:
    backend = storage_backend()
    if backend == "postgres":
        from src.data_services.postgres_backend import PostgresRepositories
        return PostgresRepositories()
    global _duckdb_repos
    if _duckdb_repos is None:
        _duckdb_repos = DuckDbRepositories(duckdb)
    return _duckdb_repos


def _record(row) -> OutboxRecord:
    return OutboxRecord(
        id=str(row[0]),
        subject=str(row[1]),
        payload=as_payload(row[2]),
        created_at=row[3],
        published_at=row[4],
        attempts=int(row[5] or 0),
    )


def duckdb_repositories(database) -> DuckDbRepositories:
    """Bundle bound to one CNCDatabase, including a test double."""
    return DuckDbRepositories(database)


def open_snapshot(path: Path) -> OperatorSnapshotStore:
    if storage_backend() == "postgres":
        return PostgresSnapshot()
    from src.data_services.sqlite_store import SqliteSnapshotStore
    return SqliteSnapshotStore(path)
