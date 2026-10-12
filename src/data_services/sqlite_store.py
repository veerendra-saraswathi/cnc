"""Operator snapshot store. SQLite JSON rows, opened only when connect() runs."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import aiosqlite

from src.kernel.models import Alert, Asset, Mission, Track


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _dt_to_str(dt: datetime) -> str:
    return dt.isoformat()


class SqliteSnapshotStore:
    """Same tables and statements the operator app used inline."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    async def connect(self):
        self.db = await aiosqlite.connect(self.path)
        self.db.row_factory = aiosqlite.Row
        await self._create_tables()

    async def close(self):
        await self.db.close()

    async def _create_tables(self):
        await self.db.executescript("""
        CREATE TABLE IF NOT EXISTS assets (
            id TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS missions (
            id TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tracks (
            id TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS alerts (
            id TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS observations (
            id TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """)
        await self.db.commit()

    async def save_asset(self, asset: Asset):
        await self.db.execute(
            "INSERT OR REPLACE INTO assets (id, data, updated_at) VALUES (?, ?, ?)",
            (asset.id, asset.model_dump_json(), _dt_to_str(_utcnow()))
        )
        await self.db.commit()

    async def load_assets(self) -> list[Asset]:
        async with self.db.execute("SELECT data FROM assets") as cursor:
            rows = await cursor.fetchall()
        return [Asset.model_validate_json(r["data"]) for r in rows]

    async def save_mission(self, mission: Mission):
        await self.db.execute(
            "INSERT OR REPLACE INTO missions (id, data, updated_at) VALUES (?, ?, ?)",
            (mission.id, mission.model_dump_json(), _dt_to_str(_utcnow()))
        )
        await self.db.commit()

    async def load_missions(self) -> list[Mission]:
        async with self.db.execute("SELECT data FROM missions") as cursor:
            rows = await cursor.fetchall()
        return [Mission.model_validate_json(r["data"]) for r in rows]

    async def save_track(self, track: Track):
        t = track.model_copy()
        t.history = t.history[-10:]
        await self.db.execute(
            "INSERT OR REPLACE INTO tracks (id, data, updated_at) VALUES (?, ?, ?)",
            (track.id, t.model_dump_json(), _dt_to_str(_utcnow()))
        )
        await self.db.commit()

    async def load_tracks(self) -> list[Track]:
        async with self.db.execute("SELECT data FROM tracks") as cursor:
            rows = await cursor.fetchall()
        return [Track.model_validate_json(r["data"]) for r in rows]

    async def save_alert(self, alert: Alert):
        await self.db.execute(
            "INSERT OR REPLACE INTO alerts (id, data, created_at) VALUES (?, ?, ?)",
            (alert.id, alert.model_dump_json(), _dt_to_str(alert.timestamp))
        )
        await self.db.commit()

    async def load_alerts(self, limit: int = 50) -> list[Alert]:
        async with self.db.execute(
            "SELECT data FROM alerts ORDER BY created_at DESC LIMIT ?", (limit,)
        ) as cursor:
            rows = await cursor.fetchall()
        return [Alert.model_validate_json(r["data"]) for r in rows]

    async def save_observation(self, obs) -> None:
        oid = str(uuid4())
        await self.db.execute(
            "INSERT INTO observations (id, data, created_at) VALUES (?, ?, ?)",
            (oid, obs.model_dump_json(), _dt_to_str(_utcnow()))
        )
        await self.db.commit()
