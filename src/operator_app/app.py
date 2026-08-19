"""
CNC Operator Station
- 2 UAV + 2 UGV + Mission + Tracks + Sensor fusion
- Track lifecycle (ACTIVE → STALE → LOST) + history + confidence decay
- SQLite persistence + startup recovery
"""

from __future__ import annotations

import asyncio
import json
import math
import random
import aiosqlite
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

# ── Paths ──────────────────────────────────────────────────────────────
BASE = Path(__file__).resolve().parent
STATIC = BASE / "static"
TEMPLATES = BASE / "templates"
DB_PATH = BASE.parent.parent / "data" / "cnc.db"   # ~/cnc/data/cnc.db

# ── Domain models ──────────────────────────────────────────────────────
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def dt_to_str(dt: datetime) -> str:
    return dt.isoformat()

def str_to_dt(s: str) -> datetime:
    return datetime.fromisoformat(s)

class AssetType(str, Enum):
    UAV = "uav"
    UGV = "ugv"

class AssetStatus(str, Enum):
    OFFLINE = "offline"
    IDLE = "idle"
    MISSION = "mission"
    RETURNING = "returning"
    EMERGENCY = "emergency"
    LOST = "lost"

class CommandType(str, Enum):
    TAKEOFF = "takeoff"
    LAND = "land"
    RTL = "rtl"
    HOLD = "hold"
    GOTO = "goto"
    SET_SPEED = "set_speed"
    CUSTOM = "custom"

class AlertLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

class MissionStatus(str, Enum):
    PLANNED = "planned"
    ACTIVE = "active"
    COMPLETED = "completed"
    ABORTED = "aborted"

class TrackType(str, Enum):
    FRIENDLY = "friendly"
    HOSTILE = "hostile"
    NEUTRAL = "neutral"
    UNKNOWN = "unknown"

class TrackState(str, Enum):
    ACTIVE = "active"
    STALE = "stale"
    LOST = "lost"

class Asset(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    asset_type: AssetType
    status: AssetStatus = AssetStatus.OFFLINE
    lat: float = 0.0
    lon: float = 0.0
    alt: float = 0.0
    heading: float = 0.0
    speed: float = 0.0
    battery: float = 100.0
    capabilities: list[str] = Field(default_factory=list)
    home_lat: Optional[float] = None
    home_lon: Optional[float] = None
    last_seen: datetime = Field(default_factory=utcnow)

class Telemetry(BaseModel):
    asset_id: str
    timestamp: datetime = Field(default_factory=utcnow)
    lat: float
    lon: float
    alt: float = 0.0
    heading: float = 0.0
    speed: float = 0.0
    battery: float = 100.0
    status: AssetStatus = AssetStatus.IDLE

class Command(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    asset_id: str
    command_type: CommandType
    parameters: dict[str, Any] = Field(default_factory=dict)
    issued_at: datetime = Field(default_factory=utcnow)
    issued_by: str = "operator"
    status: str = "pending"
    result: Optional[dict[str, Any]] = None

class Alert(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    level: AlertLevel
    title: str
    message: str
    asset_id: Optional[str] = None
    timestamp: datetime = Field(default_factory=utcnow)
    acknowledged: bool = False
    source: str = "system"

class Mission(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    description: str = ""
    status: MissionStatus = MissionStatus.PLANNED
    assigned_assets: list[str] = Field(default_factory=list)
    waypoints: list[dict[str, float]] = Field(default_factory=list)
    area: Optional[dict[str, Any]] = None
    objectives: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

class TrackHistoryPoint(BaseModel):
    timestamp: datetime
    lat: float
    lon: float
    alt: float = 0.0
    speed: float = 0.0
    heading: float = 0.0

class Track(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    label: str = "unknown"
    track_type: TrackType = TrackType.FRIENDLY
    state: TrackState = TrackState.ACTIVE
    lat: float
    lon: float
    alt: float = 0.0
    heading: float = 0.0
    speed: float = 0.0
    confidence: float = 0.9
    source_asset_ids: list[str] = Field(default_factory=list)
    first_seen: datetime = Field(default_factory=utcnow)
    last_update: datetime = Field(default_factory=utcnow)
    attributes: dict[str, Any] = Field(default_factory=dict)
    history: list[TrackHistoryPoint] = Field(default_factory=list)

class SensorObservation(BaseModel):
    sensor_id: str = "sim-sensor"
    lat: float
    lon: float
    alt: float = 0.0
    classification: str = "unknown"
    confidence: float = 0.7
    label: Optional[str] = None
    source_asset_id: Optional[str] = None

# ── Lifecycle config ───────────────────────────────────────────────────
STALE_AFTER_SEC = 8.0
LOST_AFTER_SEC = 25.0
CONFIDENCE_DECAY = 0.04
MAX_HISTORY = 30

# ── SQLite Persistence ─────────────────────────────────────────────────
class Database:
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
            (asset.id, asset.model_dump_json(), dt_to_str(utcnow()))
        )
        await self.db.commit()

    async def load_assets(self) -> list[Asset]:
        async with self.db.execute("SELECT data FROM assets") as cursor:
            rows = await cursor.fetchall()
        return [Asset.model_validate_json(r["data"]) for r in rows]

    async def save_mission(self, mission: Mission):
        await self.db.execute(
            "INSERT OR REPLACE INTO missions (id, data, updated_at) VALUES (?, ?, ?)",
            (mission.id, mission.model_dump_json(), dt_to_str(utcnow()))
        )
        await self.db.commit()

    async def load_missions(self) -> list[Mission]:
        async with self.db.execute("SELECT data FROM missions") as cursor:
            rows = await cursor.fetchall()
        return [Mission.model_validate_json(r["data"]) for r in rows]

    async def save_track(self, track: Track):
        # Keep history short in DB to avoid huge rows
        t = track.model_copy()
        t.history = t.history[-10:]
        await self.db.execute(
            "INSERT OR REPLACE INTO tracks (id, data, updated_at) VALUES (?, ?, ?)",
            (track.id, t.model_dump_json(), dt_to_str(utcnow()))
        )
        await self.db.commit()

    async def load_tracks(self) -> list[Track]:
        async with self.db.execute("SELECT data FROM tracks") as cursor:
            rows = await cursor.fetchall()
        return [Track.model_validate_json(r["data"]) for r in rows]

    async def save_alert(self, alert: Alert):
        await self.db.execute(
            "INSERT OR REPLACE INTO alerts (id, data, created_at) VALUES (?, ?, ?)",
            (alert.id, alert.model_dump_json(), dt_to_str(alert.timestamp))
        )
        await self.db.commit()

    async def load_alerts(self, limit: int = 50) -> list[Alert]:
        async with self.db.execute(
            "SELECT data FROM alerts ORDER BY created_at DESC LIMIT ?", (limit,)
        ) as cursor:
            rows = await cursor.fetchall()
        return [Alert.model_validate_json(r["data"]) for r in rows]

    async def save_observation(self, obs: SensorObservation):
        oid = str(uuid4())
        await self.db.execute(
            "INSERT INTO observations (id, data, created_at) VALUES (?, ?, ?)",
            (oid, obs.model_dump_json(), dt_to_str(utcnow()))
        )
        await self.db.commit()

# ── In-memory store (now backed by SQLite) ─────────────────────────────
class EntityStore:
    def __init__(self, db: Database):
        self.db = db
        self.assets: dict[str, Asset] = {}
        self.alerts: dict[str, Alert] = {}
        self.commands: dict[str, Command] = {}
        self.missions: dict[str, Mission] = {}
        self.tracks: dict[str, Track] = {}
        self._lock = asyncio.Lock()

    async def recover(self):
        """Load last known state from SQLite on startup."""
        for a in await self.db.load_assets():
            self.assets[a.id] = a
        for m in await self.db.load_missions():
            self.missions[m.id] = m
        for t in await self.db.load_tracks():
            self.tracks[t.id] = t
        for al in await self.db.load_alerts(30):
            self.alerts[al.id] = al
        print(f"[DB] Recovered {len(self.assets)} assets, {len(self.missions)} missions, {len(self.tracks)} tracks")

    async def upsert_asset(self, asset: Asset) -> Asset:
        async with self._lock:
            self.assets[asset.id] = asset
        await self.db.save_asset(asset)
        return asset

    async def update_from_telem(self, t: Telemetry) -> Optional[Asset]:
        async with self._lock:
            a = self.assets.get(t.asset_id)
            if not a:
                return None
            a.lat, a.lon, a.alt = t.lat, t.lon, t.alt
            a.heading, a.speed, a.battery = t.heading, t.speed, t.battery
            a.status = t.status
            a.last_seen = t.timestamp
        await self.db.save_asset(a)
        return a

    async def add_alert(self, alert: Alert) -> Alert:
        async with self._lock:
            self.alerts[alert.id] = alert
        await self.db.save_alert(alert)
        return alert

    async def list_alerts(self, unacked_only: bool = True) -> list[Alert]:
        items = list(self.alerts.values())
        if unacked_only:
            items = [a for a in items if not a.acknowledged]
        return sorted(items, key=lambda x: x.timestamp, reverse=True)

    async def upsert_mission(self, mission: Mission) -> Mission:
        async with self._lock:
            self.missions[mission.id] = mission
        await self.db.save_mission(mission)
        return mission

    async def get_mission(self, mission_id: str) -> Optional[Mission]:
        return self.missions.get(mission_id)

    async def list_missions(self) -> list[Mission]:
        return list(self.missions.values())

    async def upsert_track(self, track: Track) -> Track:
        async with self._lock:
            self.tracks[track.id] = track
        await self.db.save_track(track)
        return track

    async def get_track(self, track_id: str) -> Optional[Track]:
        return self.tracks.get(track_id)

    async def list_tracks(self) -> list[Track]:
        return list(self.tracks.values())

    async def snapshot(self) -> dict:
        return {
            "timestamp": utcnow().isoformat(),
            "assets": [a.model_dump(mode="json") for a in self.assets.values()],
            "tracks": [t.model_dump(mode="json") for t in self.tracks.values()],
            "missions": [m.model_dump(mode="json") for m in self.missions.values()],
            "alerts": [a.model_dump(mode="json") for a in await self.list_alerts()],
            "recent_commands": [
                c.model_dump(mode="json")
                for c in sorted(self.commands.values(), key=lambda x: x.issued_at, reverse=True)[:15]
            ],
        }

# ── Track Manager ──────────────────────────────────────────────────────
class TrackManager:
    def __init__(self, store: EntityStore):
        self.store = store
        self._asset_to_track: dict[str, str] = {}

    def _append_history(self, track: Track):
        point = TrackHistoryPoint(
            timestamp=utcnow(), lat=track.lat, lon=track.lon, alt=track.alt,
            speed=track.speed, heading=track.heading,
        )
        track.history.append(point)
        if len(track.history) > MAX_HISTORY:
            track.history = track.history[-MAX_HISTORY:]

    async def update_from_asset(self, asset: Asset) -> Track:
        track_id = self._asset_to_track.get(asset.id)
        if track_id is None:
            # try to find existing track by label after recovery
            for t in await self.store.list_tracks():
                if t.label == asset.name and t.track_type == TrackType.FRIENDLY:
                    track_id = t.id
                    self._asset_to_track[asset.id] = track_id
                    break

        if track_id is None:
            track = Track(
                label=asset.name, track_type=TrackType.FRIENDLY, state=TrackState.ACTIVE,
                lat=asset.lat, lon=asset.lon, alt=asset.alt,
                heading=asset.heading, speed=asset.speed, confidence=0.95,
                source_asset_ids=[asset.id],
                attributes={"asset_type": asset.asset_type.value, "battery": asset.battery, "status": asset.status.value},
            )
            self._asset_to_track[asset.id] = track.id
            self._append_history(track)
        else:
            track = await self.store.get_track(track_id)
            if track is None:
                track = Track(
                    id=track_id, label=asset.name, track_type=TrackType.FRIENDLY, state=TrackState.ACTIVE,
                    lat=asset.lat, lon=asset.lon, alt=asset.alt,
                    heading=asset.heading, speed=asset.speed, source_asset_ids=[asset.id],
                )
            else:
                track.lat = asset.lat
                track.lon = asset.lon
                track.alt = asset.alt
                track.heading = asset.heading
                track.speed = asset.speed
                track.last_update = utcnow()
                track.state = TrackState.ACTIVE
                track.confidence = min(0.99, track.confidence + 0.05)
                track.attributes["battery"] = asset.battery
                track.attributes["status"] = asset.status.value
                self._append_history(track)
        await self.store.upsert_track(track)
        return track

    async def ingest_observation(self, obs: SensorObservation) -> Track:
        best_track = None
        best_dist = 999999.0
        for t in await self.store.list_tracks():
            if t.state == TrackState.LOST:
                continue
            dlat = (t.lat - obs.lat) * 111000
            dlon = (t.lon - obs.lon) * 111000 * math.cos(math.radians(t.lat))
            dist = math.sqrt(dlat*dlat + dlon*dlon)
            if dist < best_dist:
                best_dist = dist
                best_track = t

        if best_track and best_dist < 150.0:
            best_track.lat = obs.lat
            best_track.lon = obs.lon
            best_track.alt = obs.alt
            best_track.confidence = max(best_track.confidence, obs.confidence)
            best_track.last_update = utcnow()
            best_track.state = TrackState.ACTIVE
            if obs.source_asset_id and obs.source_asset_id not in best_track.source_asset_ids:
                best_track.source_asset_ids.append(obs.source_asset_id)
            if obs.classification != "unknown":
                try: best_track.track_type = TrackType(obs.classification)
                except ValueError: pass
            self._append_history(best_track)
            await self.store.upsert_track(best_track)
            return best_track

        label = obs.label or f"Sensor-{obs.classification[:3].upper()}"
        try: ttype = TrackType(obs.classification)
        except ValueError: ttype = TrackType.UNKNOWN

        track = Track(
            label=label, track_type=ttype, state=TrackState.ACTIVE,
            lat=obs.lat, lon=obs.lon, alt=obs.alt, confidence=obs.confidence,
            source_asset_ids=[obs.source_asset_id] if obs.source_asset_id else [],
            attributes={"origin": "sensor", "sensor_id": obs.sensor_id},
        )
        self._append_history(track)
        await self.store.upsert_track(track)

        if ttype in (TrackType.HOSTILE, TrackType.UNKNOWN):
            await self.store.add_alert(Alert(
                level=AlertLevel.WARNING if ttype == TrackType.UNKNOWN else AlertLevel.CRITICAL,
                title=f"New {ttype.value} track",
                message=f"{label} detected at {obs.lat:.5f}, {obs.lon:.5f}",
                source="fusion",
            ))
        return track

    async def lifecycle_tick(self):
        now = utcnow()
        for track in await self.store.list_tracks():
            age = (now - track.last_update).total_seconds()
            if age > LOST_AFTER_SEC:
                if track.state != TrackState.LOST:
                    track.state = TrackState.LOST
                    track.confidence = max(0.05, track.confidence - CONFIDENCE_DECAY * 2)
                    await self.store.upsert_track(track)
            elif age > STALE_AFTER_SEC:
                track.state = TrackState.STALE
                track.confidence = max(0.1, track.confidence - CONFIDENCE_DECAY)
                await self.store.upsert_track(track)
            else:
                if track.state != TrackState.ACTIVE:
                    track.state = TrackState.ACTIVE
                    await self.store.upsert_track(track)

# ── Mock assets (unchanged logic) ──────────────────────────────────────
class MockAsset:
    def __init__(self, name: str, asset_type: AssetType, lat: float, lon: float):
        self.asset = Asset(
            name=name, asset_type=asset_type, status=AssetStatus.OFFLINE,
            lat=lat, lon=lon, alt=0.0, home_lat=lat, home_lon=lon,
            capabilities=["takeoff", "land", "goto", "hold", "rtl"] if asset_type == AssetType.UAV else ["goto", "hold", "rtl"],
        )
        self._target_lat = self._target_lon = None
        self._target_alt = 50.0
        self._cruise = 12.0 if asset_type == AssetType.UAV else 4.0
        self._climb = 3.0
        self._dt = 0.5
        self._task = None
        self._running = False

    @property
    def id(self) -> str:
        return self.asset.id

    async def start(self):
        self._running = True
        self.asset.status = AssetStatus.IDLE
        self.asset.last_seen = utcnow()
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try: await self._task
            except asyncio.CancelledError: pass
        self.asset.status = AssetStatus.OFFLINE

    async def send_command(self, cmd: Command) -> bool:
        ct, p = cmd.command_type, cmd.parameters or {}
        if ct == CommandType.TAKEOFF and self.asset.asset_type == AssetType.UAV:
            self._target_alt = float(p.get("altitude", 50))
            self._target_lat, self._target_lon = self.asset.lat, self.asset.lon
            self.asset.status = AssetStatus.MISSION
            return True
        if ct == CommandType.LAND and self.asset.asset_type == AssetType.UAV:
            self._target_alt = 0.0
            self._target_lat, self._target_lon = self.asset.lat, self.asset.lon
            self.asset.status = AssetStatus.RETURNING
            return True
        if ct == CommandType.RTL:
            self._target_lat, self._target_lon = self.asset.home_lat, self.asset.home_lon
            self._target_alt = 40.0 if self.asset.asset_type == AssetType.UAV else 0.0
            self.asset.status = AssetStatus.RETURNING
            return True
        if ct == CommandType.HOLD:
            self._target_lat, self._target_lon = self.asset.lat, self.asset.lon
            self.asset.status = AssetStatus.IDLE
            return True
        if ct == CommandType.GOTO:
            self._target_lat = float(p["lat"])
            self._target_lon = float(p["lon"])
            self._target_alt = float(p.get("alt", self._target_alt))
            self.asset.status = AssetStatus.MISSION
            return True
        return False

    async def get_telemetry(self) -> Optional[Telemetry]:
        if self.asset.status == AssetStatus.OFFLINE:
            return None
        return Telemetry(
            asset_id=self.asset.id, lat=self.asset.lat, lon=self.asset.lon, alt=self.asset.alt,
            heading=self.asset.heading, speed=self.asset.speed,
            battery=self.asset.battery, status=self.asset.status,
        )

    async def _loop(self):
        while self._running:
            try:
                await self._step()
                await asyncio.sleep(self._dt)
            except asyncio.CancelledError: break
            except Exception: await asyncio.sleep(self._dt)

    async def _step(self):
        a = self.asset
        if a.status in (AssetStatus.MISSION, AssetStatus.RETURNING):
            a.battery = max(0.0, a.battery - (0.02 if a.asset_type == AssetType.UAV else 0.015))
            if a.battery < 15 and a.status != AssetStatus.RETURNING:
                self._target_lat, self._target_lon = a.home_lat, a.home_lon
                a.status = AssetStatus.RETURNING
        if self._target_lat is None:
            a.speed = 0.0
            return
        dlat = self._target_lat - a.lat
        dlon = self._target_lon - a.lon
        dist = math.sqrt((dlat * 111000)**2 + (dlon * 111000 * math.cos(math.radians(a.lat)))**2)
        if a.asset_type == AssetType.UAV:
            dalt = self._target_alt - a.alt
            if abs(dalt) > 0.5:
                step = math.copysign(min(abs(dalt), self._climb * self._dt), dalt)
                a.alt += step
                a.speed = abs(step) / self._dt
                a.last_seen = utcnow()
                return
        if dist > 2.0:
            step = min(self._cruise * self._dt, dist)
            frac = step / dist
            a.lat += dlat * frac
            a.lon += dlon * frac
            a.heading = math.degrees(math.atan2(dlon, dlat)) % 360
            a.speed = self._cruise
        else:
            a.lat, a.lon = self._target_lat, self._target_lon
            a.speed = 0.0
            if a.status == AssetStatus.RETURNING and (a.asset_type == AssetType.UGV or a.alt < 1.0):
                a.status = AssetStatus.IDLE
                self._target_lat = None
            elif a.status == AssetStatus.MISSION:
                a.status = AssetStatus.IDLE
        a.lat += random.uniform(-0.000004, 0.000004)
        a.lon += random.uniform(-0.000004, 0.000004)
        a.last_seen = utcnow()

# ── Platform ───────────────────────────────────────────────────────────
class Platform:
    def __init__(self):
        self.db = Database(DB_PATH)
        self.store = EntityStore(self.db)
        self.track_manager = TrackManager(self.store)
        self.adapters: dict[str, MockAsset] = {}
        self._task = self._lifecycle_task = None
        self._running = False

    async def register(self, mock: MockAsset):
        # If we recovered an asset with the same name, reuse its id for continuity
        for existing in self.store.assets.values():
            if existing.name == mock.asset.name:
                mock.asset.id = existing.id
                mock.asset.lat = existing.lat
                mock.asset.lon = existing.lon
                mock.asset.alt = existing.alt
                break
        await self.store.upsert_asset(mock.asset)
        self.adapters[mock.id] = mock
        await mock.start()
        await self.track_manager.update_from_asset(mock.asset)
        await self.store.add_alert(Alert(
            level=AlertLevel.INFO, title="Asset online",
            message=f"{mock.asset.name} connected", asset_id=mock.id,
        ))

    async def issue(self, cmd: Command) -> Command:
        self.store.commands[cmd.id] = cmd
        ad = self.adapters.get(cmd.asset_id)
        if not ad:
            cmd.status = "failed"
            cmd.result = {"error": "asset not found"}
            return cmd
        ok = await ad.send_command(cmd)
        cmd.status = "sent" if ok else "failed"
        return cmd

    async def start(self):
        await self.db.connect()
        await self.store.recover()
        self._running = True
        self._task = asyncio.create_task(self._telem_loop())
        self._lifecycle_task = asyncio.create_task(self._lifecycle_loop())

    async def stop(self):
        self._running = False
        for t in (self._task, self._lifecycle_task):
            if t:
                t.cancel()
                try: await t
                except asyncio.CancelledError: pass
        for ad in list(self.adapters.values()):
            await ad.stop()
        await self.db.close()

    async def _telem_loop(self):
        while self._running:
            try:
                for ad in list(self.adapters.values()):
                    t = await ad.get_telemetry()
                    if t:
                        asset = await self.store.update_from_telem(t)
                        if asset:
                            await self.track_manager.update_from_asset(asset)
                        if t.battery < 20:
                            await self.store.add_alert(Alert(
                                level=AlertLevel.WARNING, title="Low battery",
                                message=f"{ad.asset.name} at {t.battery:.0f}%",
                                asset_id=ad.id, source="health",
                            ))
                await asyncio.sleep(0.8)
            except asyncio.CancelledError: break
            except Exception: await asyncio.sleep(1.0)

    async def _lifecycle_loop(self):
        while self._running:
            try:
                await self.track_manager.lifecycle_tick()
                await asyncio.sleep(1.0)
            except asyncio.CancelledError: break
            except Exception: await asyncio.sleep(1.0)

# ── FastAPI ────────────────────────────────────────────────────────────
platform = Platform()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await platform.start()
    # Always ensure the four demo assets exist
    await platform.register(MockAsset("Eagle-1", AssetType.UAV, 17.4500, 78.4800))
    await platform.register(MockAsset("Eagle-2", AssetType.UAV, 17.4515, 78.4815))
    await platform.register(MockAsset("Wolf-1", AssetType.UGV, 17.4490, 78.4790))
    await platform.register(MockAsset("Wolf-2", AssetType.UGV, 17.4485, 78.4820))
    yield
    await platform.stop()

app = FastAPI(title="CNC Operator Station", version="0.7.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES))

class CommandRequest(BaseModel):
    command_type: CommandType
    parameters: dict[str, Any] = Field(default_factory=dict)

class CreateMissionRequest(BaseModel):
    name: str
    description: str = ""
    assigned_assets: list[str] = Field(default_factory=list)
    waypoints: list[dict[str, float]] = Field(default_factory=list)
    objectives: list[str] = Field(default_factory=list)

class AssignAssetsRequest(BaseModel):
    asset_ids: list[str]

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(request, "dashboard.html")

@app.get("/health")
async def health():
    return {"status": "ok", "db": str(DB_PATH)}

@app.get("/snapshot")
async def snapshot():
    return await platform.store.snapshot()

@app.get("/assets")
async def list_assets():
    return [a.model_dump(mode="json") for a in platform.store.assets.values()]

@app.get("/tracks")
async def list_tracks():
    return [t.model_dump(mode="json") for t in await platform.store.list_tracks()]

@app.post("/assets/{asset_id}/command")
async def send_command(asset_id: str, body: CommandRequest):
    cmd = Command(asset_id=asset_id, command_type=body.command_type, parameters=body.parameters)
    return (await platform.issue(cmd)).model_dump(mode="json")

@app.post("/sensors/observe")
async def sensor_observe(obs: SensorObservation):
    await platform.db.save_observation(obs)
    track = await platform.track_manager.ingest_observation(obs)
    return track.model_dump(mode="json")

@app.get("/missions")
async def list_missions():
    return [m.model_dump(mode="json") for m in await platform.store.list_missions()]

@app.post("/missions")
async def create_mission(body: CreateMissionRequest):
    mission = Mission(name=body.name, description=body.description,
                      assigned_assets=body.assigned_assets,
                      waypoints=body.waypoints, objectives=body.objectives)
    await platform.store.upsert_mission(mission)
    await platform.store.add_alert(Alert(level=AlertLevel.INFO, title="Mission created",
                                         message=f"Mission '{mission.name}' created", source="mission"))
    return mission.model_dump(mode="json")

@app.get("/missions/{mission_id}")
async def get_mission(mission_id: str):
    mission = await platform.store.get_mission(mission_id)
    if not mission: raise HTTPException(404, "Mission not found")
    return mission.model_dump(mode="json")

@app.post("/missions/{mission_id}/assign")
async def assign_assets(mission_id: str, body: AssignAssetsRequest):
    mission = await platform.store.get_mission(mission_id)
    if not mission: raise HTTPException(404, "Mission not found")
    existing = set(mission.assigned_assets)
    for aid in body.asset_ids: existing.add(aid)
    mission.assigned_assets = list(existing)
    await platform.store.upsert_mission(mission)
    return mission.model_dump(mode="json")

@app.post("/missions/{mission_id}/start")
async def start_mission(mission_id: str):
    mission = await platform.store.get_mission(mission_id)
    if not mission: raise HTTPException(404, "Mission not found")
    if not mission.assigned_assets: raise HTTPException(400, "No assets assigned")
    mission.status = MissionStatus.ACTIVE
    mission.started_at = utcnow()
    await platform.store.upsert_mission(mission)
    for asset_id in mission.assigned_assets:
        if mission.waypoints:
            wp = mission.waypoints[0]
            cmd = Command(asset_id=asset_id, command_type=CommandType.GOTO,
                          parameters={"lat": wp["lat"], "lon": wp["lon"], "alt": wp.get("alt", 40)}, issued_by="mission")
        else:
            asset = platform.store.assets.get(asset_id)
            if not asset: continue
            cmd = Command(asset_id=asset_id, command_type=CommandType.GOTO,
                          parameters={"lat": asset.lat+0.0015, "lon": asset.lon+0.0010,
                                      "alt": 50 if asset.asset_type == AssetType.UAV else 0}, issued_by="mission")
        await platform.issue(cmd)
    await platform.store.add_alert(Alert(level=AlertLevel.INFO, title="Mission started",
                                         message=f"Mission '{mission.name}' is now ACTIVE", source="mission"))
    return mission.model_dump(mode="json")

@app.post("/missions/{mission_id}/abort")
async def abort_mission(mission_id: str):
    mission = await platform.store.get_mission(mission_id)
    if not mission: raise HTTPException(404, "Mission not found")
    mission.status = MissionStatus.ABORTED
    mission.completed_at = utcnow()
    await platform.store.upsert_mission(mission)
    for asset_id in mission.assigned_assets:
        await platform.issue(Command(asset_id=asset_id, command_type=CommandType.HOLD, issued_by="mission"))
    return mission.model_dump(mode="json")

@app.websocket("/ws/cop")
async def ws_cop(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            await ws.send_json(await platform.store.snapshot())
            await asyncio.sleep(1.0)
    except WebSocketDisconnect: pass
    except Exception:
        try: await ws.close()
        except Exception: pass
