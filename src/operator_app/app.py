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
import os
import random
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from src.kernel.models import (
    Alert,
    AlertLevel,
    Asset,
    AssetStatus,
    AssetType,
    Command,
    CommandType,
    Event,
    Mission,
    MissionStatus,
    OperatorAction,
    Telemetry,
    Track,
    TrackHistoryPoint,
    TrackState,
    TrackType,
)
from src.kernel.provenance import ProvenanceError, require_command_event
from src.data_services.repositories import (
    InMemoryOperatorActionRepository,
    OperatorSnapshotStore,
    open_snapshot,
)
from src.operator_app.command_gate import stamp
from src.operator_app.live_feed import (
    bind_snapshot,
    cop_frame,
    hub,
    notify_live,
    serialize_frame,
    start_redis_listener,
)
from src.operator_app.zones import apply_zone_check, keep_out_zone
# ── Paths ──────────────────────────────────────────────────────────────
BASE = Path(__file__).resolve().parent
STATIC = BASE / "static"
TEMPLATES = BASE / "templates"
# CNC_SNAPSHOT_PATH isolates a second process. Unset, this stays data/cnc.db.
DB_PATH = Path(os.environ["CNC_SNAPSHOT_PATH"]) if os.environ.get("CNC_SNAPSHOT_PATH") else BASE.parent.parent / "data" / "cnc.db"

# ── Domain models ──────────────────────────────────────────────────────
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def dt_to_str(dt: datetime) -> str:
    return dt.isoformat()

def str_to_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

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

# ── In-memory store (snapshot file opened via open_snapshot) ───────────
class EntityStore:
    def __init__(self, db: OperatorSnapshotStore):
        self.db = db
        self.assets: dict[str, Asset] = {}
        self.alerts: dict[str, Alert] = {}
        self.commands: dict[str, Command] = {}
        self.missions: dict[str, Mission] = {}
        self.tracks: dict[str, Track] = {}
        self._lock = asyncio.Lock()

    async def recover(self):
        """Load last known state from the snapshot and the durable command tape."""
        for a in await self.db.load_assets():
            self.assets[a.id] = a
        for m in await self.db.load_missions():
            self.missions[m.id] = m
        for t in await self.db.load_tracks():
            self.tracks[t.id] = t
        for al in await self.db.load_alerts(30):
            self.alerts[al.id] = al
        restored = _rehydrate_operator_history(self)
        print(
            f"[DB] Recovered {len(self.assets)} assets, {len(self.missions)} missions, "
            f"{len(self.tracks)} tracks, {len(self.commands)} commands, {restored} events"
        )

    async def upsert_asset(self, asset: Asset) -> Asset:
        async with self._lock:
            self.assets[asset.id] = asset
        await self.db.save_asset(asset)
        return asset

    async def update_from_telem(self, t: Telemetry, *, persist: bool = True) -> Optional[Asset]:
        async with self._lock:
            a = self.assets.get(t.asset_id)
            if not a:
                return None
            a.lat, a.lon, a.alt = t.lat, t.lon, t.alt
            a.heading, a.speed, a.battery = t.heading, t.speed, t.battery
            a.status = t.status
            a.last_seen = t.timestamp
        if persist:
            await self.db.save_asset(a)
        return a

    async def add_alert(self, alert: Alert) -> Alert:
        async with self._lock:
            self.alerts[alert.id] = alert
        await self.db.save_alert(alert)
        _ingest_quietly("ingest_alert", alert.model_dump(mode="json"), actor="operator")
        notify_live()
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
        notify_live()
        return mission

    async def get_mission(self, mission_id: str) -> Optional[Mission]:
        return self.missions.get(mission_id)

    async def list_missions(self) -> list[Mission]:
        return list(self.missions.values())

    async def upsert_track(self, track: Track) -> Track:
        async with self._lock:
            self.tracks[track.id] = track
        await self.db.save_track(track)
        _ingest_quietly("ingest_track", track.model_dump(mode="json"), actor="operator")
        notify_live(track_id=track.id)
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

    async def update_from_asset(self, asset: Asset) -> Track | None:
        track_id = self._asset_to_track.get(asset.id)
        # A sample still in the durable queue must not mark a stopped vehicle active.
        if track_id is not None and asset.status in (
            AssetStatus.OFFLINE,
            AssetStatus.LOST,
            AssetStatus.ERROR,
        ):
            return await self.store.get_track(track_id)
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

    def _asset_for_track(self, track: Track) -> Optional[Asset]:
        for asset_id, track_id in self._asset_to_track.items():
            if track_id == track.id:
                asset = self.store.assets.get(asset_id)
                if asset is not None:
                    return asset
        for asset_id in track.source_asset_ids or []:
            asset = self.store.assets.get(asset_id)
            if asset is not None and (
                track.track_type == TrackType.FRIENDLY or self._asset_to_track.get(asset_id) == track.id
            ):
                return asset
        for asset in self.store.assets.values():
            if asset.name and asset.name == track.label and track.track_type == TrackType.FRIENDLY:
                return asset
        return None

    async def _presence_alert(self, asset: Asset, *, lost: bool) -> None:
        """One warning per asset. The lost text replaces the offline text."""
        alert_id = f"presence-{asset.id}"
        if lost:
            title = "Asset lost"
            message = f"{asset.name} is offline and its track is lost"
        else:
            title = "Asset offline"
            message = f"{asset.name} went offline"
        existing = self.store.alerts.get(alert_id)
        if existing is not None and existing.message == message:
            return
        if existing is not None and not lost:
            return
        alert = Alert(
            id=alert_id,
            level=AlertLevel.WARNING,
            title=title,
            message=message,
            asset_id=asset.id,
            source="lifecycle",
            timestamp=existing.timestamp if existing is not None else utcnow(),
        )
        await self.store.add_alert(alert)

    async def _track_lost_alert(self, track: Track) -> None:
        asset = self._asset_for_track(track)
        if asset is None:
            alert_id = f"presence-track-{track.id}"
            if alert_id in self.store.alerts:
                return
            label = track.label or track.id
            await self.store.add_alert(Alert(
                id=alert_id,
                level=AlertLevel.WARNING,
                title="Track lost",
                message=f"Track {label} is lost",
                source="lifecycle",
            ))
            return
        if asset.status in (AssetStatus.OFFLINE, AssetStatus.ERROR, AssetStatus.LOST):
            if asset.status != AssetStatus.LOST:
                asset.status = AssetStatus.LOST
                await self.store.upsert_asset(asset)
        await self._presence_alert(asset, lost=True)

    async def lifecycle_tick(self):
        now = utcnow()
        for track in await self.store.list_tracks():
            age = (now - as_utc(track.last_update)).total_seconds()
            if age > LOST_AFTER_SEC:
                if track.state != TrackState.LOST:
                    track.state = TrackState.LOST
                    track.confidence = max(0.05, track.confidence - CONFIDENCE_DECAY * 2)
                    await self.store.upsert_track(track)
                    await self._track_lost_alert(track)
            elif age > STALE_AFTER_SEC:
                track.state = TrackState.STALE
                track.confidence = max(0.1, track.confidence - CONFIDENCE_DECAY)
                await self.store.upsert_track(track)
            else:
                if track.state != TrackState.ACTIVE:
                    track.state = TrackState.ACTIVE
                    await self.store.upsert_track(track)
        for asset in list(self.store.assets.values()):
            if asset.status == AssetStatus.OFFLINE:
                await self._presence_alert(asset, lost=False)
            elif asset.status == AssetStatus.LOST:
                await self._presence_alert(asset, lost=True)

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
        self.db = open_snapshot(DB_PATH)
        self.store = EntityStore(self.db)
        self.track_manager = TrackManager(self.store)
        self.adapters: dict[str, MockAsset] = {}
        self.operator_action_repo = InMemoryOperatorActionRepository()
        self._task = self._lifecycle_task = None
        self._running = False
        self._commands_open = False

    @property
    def operator_actions(self) -> list[OperatorAction]:
        return self.operator_action_repo.records

    async def register(self, mock: MockAsset, *, start: bool = True):
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
        if not start:
            return
        await mock.start()
        await self.track_manager.update_from_asset(mock.asset)
        await self.store.add_alert(Alert(
            level=AlertLevel.INFO, title="Asset online",
            message=f"{mock.asset.name} connected", asset_id=mock.id,
        ))

    def _reject_keep_out(self, cmd: Command) -> Command | None:
        """Fail a GOTO inside a keep-out before the command transaction."""
        if cmd.command_type != CommandType.GOTO:
            return None
        params = cmd.parameters or {}
        try:
            lat = float(params["lat"])
            lon = float(params["lon"])
        except (KeyError, TypeError, ValueError):
            return None
        zone = keep_out_zone(lat, lon)
        if zone is None:
            return None
        from src.data_services.event_store import EventType, event_store
        cmd.status = "failed"
        cmd.result = {"error": "keep-out", "zone_id": zone.id}
        event_store.add(
            EventType.COMMAND,
            f"goto rejected keep-out {zone.id}",
            asset_id=cmd.asset_id,
            actor=cmd.issued_by,
            data={
                "operator_id": cmd.issued_by,
                "command_id": cmd.id,
                "command_type": cmd.command_type.value,
                "status": "failed",
                "payload": dict(cmd.parameters),
                "zone_id": zone.id,
                "reason": "keep-out",
            },
        )
        self.store.commands[cmd.id] = cmd
        notify_live()
        return cmd

    async def issue(self, cmd: Command) -> Command:
        """Record the command Event, prove it matches, then dispatch.

        A keep-out GOTO is rejected with an Event and is not dispatched.
        A failed Event write raises ProvenanceError and does not call the adapter.
        """
        rejected = self._reject_keep_out(cmd)
        if rejected is not None:
            return rejected
        from src.data_services.event_store import event_store
        try:
            _record_command_event(cmd)
            require_command_event(cmd, event_store.get_by_asset(cmd.asset_id))
        except ProvenanceError:
            cmd.status = "failed"
            cmd.result = {"error": "command event was not recorded"}
            raise
        except Exception as exc:
            cmd.status = "failed"
            cmd.result = {"error": "command event was not recorded"}
            raise ProvenanceError("command event was not recorded") from exc
        self.store.commands[cmd.id] = cmd
        if cmd.asset_id not in self.adapters:
            cmd.status = "failed"
            cmd.result = {"error": "asset not found"}
            from src.data.outbox import mark_command_published
            from src.operator_app.command_bus import remember_command
            mark_command_published(cmd.id)
            remember_command(cmd)
            notify_live()
            return cmd
        from src.operator_app.command_bus import dispatch_command
        await dispatch_command(self, cmd)
        notify_live()
        return cmd

    async def start(self):
        await self.db.connect()
        await self.store.recover()
        self._running = True
        self._task = asyncio.create_task(self._telem_loop())
        self._lifecycle_task = asyncio.create_task(self._lifecycle_loop())
        from src.data.bus import current_mode
        from src.data.outbox import start_relay
        from src.data.pipeline import start_pipeline
        from src.operator_app.command_bus import start_command_bridge

        async def _consumers_up() -> None:
            if await start_pipeline() and self._commands_open:
                await start_command_bridge(self.adapters.get)

        async def _consumers_again() -> None:
            from src.data.pipeline import restart_pipeline
            from src.operator_app.command_bus import restart_command_bridge
            if await restart_pipeline() and self._commands_open:
                await restart_command_bridge(self.adapters.get)

        if current_mode() == "nats":
            await start_relay(_consumers_up, on_reconnect=_consumers_again)
            await _consumers_up()
        else:
            await start_pipeline()

    async def open_command_consumer(self) -> None:
        """Subscribe to commands only after adapters are in self.adapters."""
        from src.operator_app.command_bus import start_command_bridge

        self._commands_open = True
        await start_command_bridge(self.adapters.get)

    async def stop(self):
        self._running = False
        from src.data.bus import get_bus
        from src.data.outbox import stop_relay
        from src.data.pipeline import stop_pipeline
        from src.operator_app.command_bus import stop_command_bridge
        await stop_relay()
        await stop_command_bridge()
        await stop_pipeline()
        from src.operator_app.adapters.telemetry_publish import drain_telemetry_publishes
        await drain_telemetry_publishes()
        await get_bus().close()
        for t in (self._task, self._lifecycle_task):
            if t:
                t.cancel()
                try: await t
                except asyncio.CancelledError: pass
        for ad in list(self.adapters.values()):
            await ad.stop()
        await self.db.close()

    def _record_poll(self, adapter, sample) -> None:
        from src.operator_app.telem_timing import note_poll

        extras = getattr(sample, "extras", None) or {}
        timing = extras.get("timing") if isinstance(extras, dict) else None
        timing = timing if isinstance(timing, dict) else {}
        poll_ms = timing.get("poll_ms")
        if poll_ms is None:
            poll_ms = sample.timestamp.timestamp() * 1000.0
        kind = getattr(getattr(adapter, "asset", None), "asset_type", None)
        note_poll(
            sample.asset_id,
            kind,
            timing.get("source_ms"),
            timing.get("adapter_ms"),
            poll_ms,
            timing.get("message_gap_ms"),
        )

    async def _durable_telem(self, queue: asyncio.Queue) -> None:
        """Track update and sqlite. JetStream acks run on their own tasks."""
        while True:
            adapter, sample, publish = await queue.get()
            try:
                asset = self.store.assets.get(sample.asset_id)
                if asset is not None:
                    await self.track_manager.update_from_asset(asset)
                if publish:
                    await adapter.publish_vendor_telemetry(sample)
                if asset is not None:
                    await self.store.db.save_asset(asset)
                if sample.battery < 20:
                    alert_id = f"battery-{sample.asset_id}"
                    if alert_id not in self.store.alerts:
                        await self.store.add_alert(Alert(
                            id=alert_id,
                            level=AlertLevel.WARNING, title="Low battery",
                            message=f"{adapter.asset.name} at {sample.battery:.0f}%",
                            asset_id=adapter.id, source="health",
                        ))
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            finally:
                queue.task_done()

    async def _telem_loop(self):
        from src.operator_app.adapters.telemetry_publish import accept_vendor_telemetry

        # Track and sqlite stay off this loop. JetStream acks are their own tasks,
        # so a slow PubAck cannot fill a queue and drop the sample before publish.
        queue: asyncio.Queue = asyncio.Queue()
        worker = asyncio.create_task(self._durable_telem(queue))
        try:
            while self._running:
                try:
                    for ad in list(self.adapters.values()):
                        poll = getattr(ad, "poll_telemetry", None)
                        if poll is not None:
                            sample = poll()
                        else:
                            sample = await ad.get_telemetry()
                        if not sample:
                            continue
                        asset = await self.store.update_from_telem(sample, persist=False)
                        self._record_poll(ad, sample)
                        if poll is not None:
                            await accept_vendor_telemetry(sample)
                        if asset is not None:
                            # DuckDB zone checks are in memory. Alerts must not wait on JetStream.
                            await apply_zone_check(self.store, asset)
                            notify_live(asset.id)
                        # The frame task reads last_seen before the next asset is polled.
                        await asyncio.sleep(0)
                        queue.put_nowait((ad, sample, False))
                    await asyncio.sleep(0.1)
                except asyncio.CancelledError:
                    break
                except Exception:
                    await asyncio.sleep(1.0)
        finally:
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass

    async def _lifecycle_loop(self):
        while self._running:
            try:
                await self.track_manager.lifecycle_tick()
                await asyncio.sleep(1.0)
            except asyncio.CancelledError: break
            except Exception: await asyncio.sleep(1.0)

# ── FastAPI ────────────────────────────────────────────────────────────
platform = Platform()


async def _cop_snapshot() -> dict:
    return await platform.store.snapshot()


bind_snapshot(_cop_snapshot)


async def _register_configured_fleet() -> None:
    """Register every fleet.yaml asset, then connect them together.

    The command consumer is still closed here, so a late vehicle cannot nak
    a command that arrived while connect() was waiting on a heartbeat.
    """
    from src.operator_app.fleet_config import load_fleet_adapters

    adapters = load_fleet_adapters()
    for adapter in adapters:
        wanted = adapter.asset.id
        await platform.register(adapter, start=False)
        if adapter.asset.id != wanted:
            previous = adapter.asset.id
            platform.adapters.pop(previous, None)
            platform.store.assets.pop(previous, None)
            adapter.asset.id = wanted
            platform.adapters[wanted] = adapter
            await platform.store.upsert_asset(adapter.asset)
    if adapters:
        await asyncio.gather(*(adapter.start() for adapter in adapters))
    for adapter in adapters:
        await platform.track_manager.update_from_asset(adapter.asset)
        await platform.store.add_alert(Alert(
            level=AlertLevel.INFO, title="Asset online",
            message=f"{adapter.asset.name} connected", asset_id=adapter.asset.id,
        ))


async def boot_operator() -> None:
    """Start the app, register the fleet, then open the command consumer."""
    await platform.start()
    start_redis_listener()
    await _register_configured_fleet()
    await platform.open_command_consumer()

def _rehydrate_operator_history(store: EntityStore) -> int:
    """Copy recent commands and events from the durable store into memory."""
    from src.data_services.event_store import event_store
    from src.data_services.repositories import get_repositories

    repos = get_repositories()
    for row in repos.list_recent_commands(15):
        try:
            command = Command(
                id=row["id"],
                asset_id=row["asset_id"],
                command_type=row["command_type"],
                parameters=row.get("parameters") or {},
                issued_by=row.get("issued_by") or "operator",
                issued_at=row["issued_at"],
                status=row.get("status") or "pending",
                result=row.get("result"),
            )
        except Exception as exc:
            print(f"[DB] skip command {row.get('id')}: {exc}")
            continue
        store.commands[command.id] = command
    return event_store.restore(repos.list_recent_events(500))


def _ingest_quietly(function_name: str, payload: dict, actor: str) -> None:
    try:
        from src.data import ingest as ingest_mod
        getattr(ingest_mod, function_name)(payload, actor=actor)
    except Exception as exc:
        print(f"[ingest] {function_name} failed: {exc}")


def _record_command_event(cmd: Command) -> Event:
    """Write the command, its Event, and the outbox row. Raises when that commit fails."""
    from src.data.outbox import enqueue_issued
    from src.data.sequence import next_command_seq
    from src.data_services.event_store import EventType, event_store
    if cmd.seq <= 0:
        cmd.seq = next_command_seq(cmd.asset_id)
    event = event_store.add(
        EventType.COMMAND,
        f"{cmd.command_type.value} ({cmd.status})",
        asset_id=cmd.asset_id,
        actor=cmd.issued_by,
        persist=False,
        data={
            "operator_id": cmd.issued_by,
            "command_id": cmd.id,
            "command_type": cmd.command_type.value,
            "status": cmd.status,
            "payload": cmd.parameters,
            "seq": cmd.seq,
        },
    )
    enqueue_issued(cmd, event)
    _ingest_quietly(
        "ingest_command",
        {
            "command_id": cmd.id,
            "asset_id": cmd.asset_id,
            "command_type": cmd.command_type.value,
            "parameters": cmd.parameters,
            "issued_by": cmd.issued_by,
            "issued_at": dt_to_str(cmd.issued_at),
            "status": cmd.status,
        },
        actor=cmd.issued_by,
    )
    return event


@asynccontextmanager
async def lifespan(app: FastAPI):
    await boot_operator()
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

@app.post("/assets/{asset_id}/detect")
async def mark_detection(asset_id: str, lat: float | None = None, lon: float | None = None):
    """A UAV detection becomes a track the operator can hand to the nearest UGV."""
    from src.kernel.models import Detection
    from src.operator_app.coordinate import track_from_detection
    asset = platform.store.assets.get(asset_id)
    if asset is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    if asset.asset_type != AssetType.UAV:
        raise HTTPException(status_code=400, detail="Detection comes from a UAV")
    detection = Detection(
        asset_id=asset_id,
        lat=asset.lat if lat is None else lat,
        lon=asset.lon if lon is None else lon,
        confidence=0.86,
    )
    track = track_from_detection(detection)
    await platform.store.upsert_track(track)
    return track.model_dump(mode="json")

@app.post("/tracks/{track_id}/task-nearest-ugv")
async def task_nearest_ugv(track_id: str):
    from src.operator_app.coordinate import task_nearest_ugv as task_ugv
    try:
        issued = await task_ugv(platform, track_id)
    except ProvenanceError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return issued.model_dump(mode="json")

@app.post("/assets/{asset_id}/command")
async def send_command(asset_id: str, body: CommandRequest):
    issued_by = stamp(asset_id, body.command_type.value)
    cmd = Command(
        asset_id=asset_id,
        command_type=body.command_type,
        parameters=body.parameters,
        issued_by=issued_by,
    )
    platform.operator_action_repo.add(OperatorAction(
        time=cmd.issued_at,
        actor=issued_by,
        asset_id=asset_id,
        action=body.command_type.value,
        command_id=cmd.id,
        payload=dict(body.parameters),
    ))
    try:
        issued = await platform.issue(cmd)
    except ProvenanceError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return issued.model_dump(mode="json")

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

@app.post("/missions/{mission_id}/export")
async def export_mission(mission_id: str):
    mission = await platform.store.get_mission(mission_id)
    if not mission:
        raise HTTPException(404, "Mission not found")
    from src.data.after_action import write_after_action
    return write_after_action(mission.id, list(mission.assigned_assets))

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
    hub.loop = asyncio.get_running_loop()
    queue = hub.subscribe()
    try:
        initial = await cop_frame(await platform.store.snapshot())
        initial["type"] = "snapshot"
        # One serialization. Later frames are already text.
        await ws.send_text(serialize_frame(initial))
        while True:
            frame = await queue.get()
            if isinstance(frame, str):
                await ws.send_text(frame)
            else:
                await ws.send_text(serialize_frame(frame))
    except WebSocketDisconnect:
        pass
    except Exception:
        try:
            await ws.close()
        except Exception:
            pass
    finally:
        hub.unsubscribe(queue)
