"""Push the common operating picture. The dashboard waits on a queue.

Redis supplies live positions when it answers. A failed ping waits on a
backoff and is tried again, so the overlay returns without a process restart.
A new socket gets one full snapshot. Later pushes are per-asset deltas,
coalesced for about 50 ms, and serialized once for every subscriber.
Missions, command phase, and zones ride along only when that state changes.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from datetime import datetime, timezone
from typing import Awaitable, Callable

from src.data.redis_live import RedisLiveStore
from src.data_services.repositories import get_repositories

SnapshotProvider = Callable[[], Awaitable[dict]]

_provider: SnapshotProvider | None = None
_redis: RedisLiveStore | None = None
_redis_down = False
_redis_retry_at = 0.0
_redis_backoff = 0.5
_listener_started = False
_overlay_loaded = False
_overlay: dict[str, dict[str, dict]] = {"platform": {}, "track": {}, "alert": {}}
_coalesce_task: asyncio.Task | None = None
_dirty_assets: set[str] = set()
_dirty_tracks: set[str] = set()
_dirty_picture = False
_COALESCE_S = 0.05
_lock = threading.Lock()
_zone_fail_until = 0.0

_CHANNEL_KIND = {
    "cnc:telemetry": "platform",
    "cnc:track": "track",
    "cnc:alert": "alert",
}


class CopHub:
    """Fan-out for /ws/cop. publish() wakes every subscriber."""

    def __init__(self) -> None:
        self._queues: list[asyncio.Queue] = []
        self.loop: asyncio.AbstractEventLoop | None = None

    @property
    def subscribers(self) -> int:
        return len(self._queues)

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=32)
        self._queues.append(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        try:
            self._queues.remove(queue)
        except ValueError:
            pass

    def publish(self, frame: dict) -> None:
        for queue in list(self._queues):
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                queue.put_nowait(frame)
            except asyncio.QueueFull:
                pass


hub = CopHub()


def bind_snapshot(provider: SnapshotProvider) -> None:
    global _provider
    _provider = provider


def command_phase(status: str, result: dict | None) -> str:
    """pending / sent / acked / timeout, derived from status and result."""
    body = result or {}
    error = body.get("error")
    if isinstance(error, str) and "ack timeout" in error:
        return "timeout"
    if status == "pending":
        return "pending"
    if status == "sent":
        return "acked" if body.get("ack") else "sent"
    if status == "failed":
        return "failed"
    return status or "pending"


def mission_progress(mission: dict, commands: list[dict]) -> float:
    status = str(mission.get("status") or "planned")
    if status in {"completed", "aborted", "failed"}:
        return 1.0
    if status in {"planned", "planning", "idle"}:
        return 0.0
    assigned = list(mission.get("assigned_assets") or [])
    if not assigned:
        return 0.0
    done = 0
    for asset_id in assigned:
        phases = [
            command.get("phase")
            for command in commands
            if command.get("asset_id") == asset_id
        ]
        if any(phase in {"sent", "acked"} for phase in phases):
            done += 1
    return done / len(assigned)


def _epoch(value) -> float | None:
    """Seconds since the epoch. Overlay and card stamps are ISO strings."""
    if isinstance(value, (int, float)):
        number = float(value)
        if number > 10_000_000_000:
            return number / 1000.0
        return number
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _pose(payload: dict) -> dict | None:
    position = payload.get("position") if isinstance(payload.get("position"), dict) else {}
    lat = position.get("lat", payload.get("lat"))
    lon = position.get("lon", payload.get("lon"))
    if lat is None or lon is None:
        return None
    status = payload.get("status")
    if hasattr(status, "value"):
        status = status.value
    battery = payload.get("battery_percent", payload.get("battery"))
    return {
        "lat": lat,
        "lon": lon,
        "alt": position.get("alt", payload.get("alt", 0)),
        "heading": position.get("heading", payload.get("heading", 0)),
        "speed": payload.get("speed", 0),
        "battery": battery,
        "status": status,
    }


def _mark_redis_down() -> None:
    global _redis, _redis_down, _redis_retry_at, _redis_backoff, _overlay_loaded
    with _lock:
        _redis = None
        _redis_down = True
        wait = _redis_backoff
        _redis_backoff = min(5.0, max(0.5, wait * 2))
        _redis_retry_at = time.monotonic() + wait
        _overlay_loaded = False
        for kind in _overlay:
            _overlay[kind].clear()


def _mark_redis_up(store: RedisLiveStore) -> None:
    global _redis, _redis_down, _redis_backoff
    _redis = store
    _redis_down = False
    _redis_backoff = 0.5


def live_redis() -> RedisLiveStore | None:
    """Ping Redis. A failure backs off, then the next call tries again."""
    global _redis
    if _redis is not None:
        return _redis
    if time.monotonic() < _redis_retry_at:
        return None
    with _lock:
        if _redis is not None:
            return _redis
        if time.monotonic() < _redis_retry_at:
            return None
        try:
            store = RedisLiveStore()
            store.client.ping()
        except Exception:
            _mark_redis_down_locked()
            return None
        _mark_redis_up(store)
        return store


def _mark_redis_down_locked() -> None:
    """Caller holds _lock. Same backoff as _mark_redis_down."""
    global _redis, _redis_down, _redis_retry_at, _redis_backoff, _overlay_loaded
    _redis = None
    _redis_down = True
    wait = _redis_backoff
    _redis_backoff = min(5.0, max(0.5, wait * 2))
    _redis_retry_at = time.monotonic() + wait
    _overlay_loaded = False
    for kind in _overlay:
        _overlay[kind].clear()


def _object_id(kind: str, payload: dict) -> str:
    if kind == "platform":
        return str(payload.get("asset_id") or payload.get("platform_id") or payload.get("id") or "")
    return str(payload.get("id") or payload.get("track_id") or payload.get("alert_id") or "")


def _remember_overlay(kind: str, payload: dict) -> None:
    if kind not in _overlay or not isinstance(payload, dict):
        return
    object_id = _object_id(kind, payload)
    if not object_id:
        return
    copied = dict(payload)
    copied.setdefault("id", object_id)
    _overlay[kind][object_id] = copied


def note_overlay(channel: str, payload: dict) -> None:
    global _overlay_loaded
    kind = _CHANNEL_KIND.get(str(channel))
    if kind is None:
        return
    with _lock:
        _remember_overlay(kind, payload)
        _overlay_loaded = True


def _bodies(store: RedisLiveStore, kind: str) -> list[dict]:
    rows: list[dict] = []
    for object_id in store.list_ids(kind):
        body = store.get(kind, object_id)
        if not isinstance(body, dict) or not body:
            continue
        copied = dict(body)
        copied.setdefault("id", object_id)
        rows.append(copied)
    return rows


def _warm_overlay(store: RedisLiveStore) -> None:
    global _overlay_loaded
    for kind in ("platform", "track", "alert"):
        for body in _bodies(store, kind):
            _remember_overlay(kind, body)
    _overlay_loaded = True


def _overlay_copy() -> dict:
    return {
        "platform": list(_overlay["platform"].values()),
        "track": list(_overlay["track"].values()),
        "alert": list(_overlay["alert"].values()),
    }


def read_redis_overlay() -> dict:
    """Cached poses. One key scan when the cache is empty and Redis answers."""
    global _overlay_loaded
    if _overlay_loaded:
        with _lock:
            return _overlay_copy()
    if _redis_down and time.monotonic() < _redis_retry_at:
        return {}
    with _lock:
        if _overlay_loaded:
            return _overlay_copy()
        store = _redis
        if store is None:
            store = None
        else:
            try:
                _warm_overlay(store)
                return _overlay_copy()
            except Exception:
                _mark_redis_down_locked()
                return {}
    store = live_redis()
    if store is None:
        return {}
    with _lock:
        if _overlay_loaded:
            return _overlay_copy()
        try:
            _warm_overlay(store)
            return _overlay_copy()
        except Exception:
            _mark_redis_down_locked()
            return {}


def zone_rings() -> list[dict]:
    """Zone polygons for the map. A failed repository waits 5 s before the next try."""
    global _zone_fail_until
    now = time.monotonic()
    if now < _zone_fail_until:
        return []
    rings: list[dict] = []
    try:
        zones = get_repositories().zones.list()
    except Exception:
        _zone_fail_until = now + 5.0
        return rings
    for zone in zones:
        ring: list[list[float]] = []
        for point in zone.polygon or []:
            if isinstance(point, dict) and "lat" in point and "lon" in point:
                ring.append([float(point["lat"]), float(point["lon"])])
        rings.append({
            "id": zone.id,
            "name": zone.name,
            "kind": zone.kind,
            "ring": ring,
        })
    return rings


def present_snapshot(snapshot: dict, overlay: dict | None = None, zones: list | None = None) -> dict:
    """Operator cards plus Redis positions, command phase, and mission progress."""
    overlay = overlay or {}
    assets: list[dict] = [dict(asset) for asset in snapshot.get("assets") or []]
    by_id = {asset.get("id"): asset for asset in assets}
    for payload in overlay.get("platform") or []:
        pose = _pose(payload)
        if pose is None:
            continue
        asset_id = str(payload.get("asset_id") or payload.get("platform_id") or payload.get("id") or "")
        if not asset_id:
            continue
        card = by_id.get(asset_id)
        # A poll newer than this Redis sample owns the picture. Ingest lags the loop.
        overlay_at = _epoch(payload.get("timestamp"))
        card_at = _epoch((card or {}).get("last_seen"))
        if card is not None and overlay_at is not None and card_at is not None and overlay_at + 1e-3 < card_at:
            continue
        if card is None:
            card = {
                "id": asset_id,
                "name": payload.get("name") or asset_id,
                "asset_type": payload.get("asset_type") or "other",
                "battery": 0,
                "heading": 0,
                "speed": 0,
                "alt": 0,
                "status": "idle",
            }
            assets.append(card)
            by_id[asset_id] = card
        card["lat"] = pose["lat"]
        card["lon"] = pose["lon"]
        card["alt"] = pose["alt"]
        card["heading"] = pose["heading"]
        card["speed"] = pose["speed"]
        if pose["battery"] is not None:
            card["battery"] = pose["battery"]
        if pose["status"]:
            card["status"] = pose["status"]

    tracks: list[dict] = [dict(track) for track in snapshot.get("tracks") or []]
    seen_tracks = {track.get("id") for track in tracks}
    for payload in overlay.get("track") or []:
        track_id = str(payload.get("id") or payload.get("track_id") or "")
        if not track_id:
            continue
        if track_id in seen_tracks:
            for track in tracks:
                if track.get("id") == track_id and payload.get("lat") is not None:
                    track["lat"] = payload["lat"]
                    track["lon"] = payload.get("lon", track.get("lon"))
            continue
        if payload.get("lat") is None or payload.get("lon") is None:
            continue
        tracks.append({
            "id": track_id,
            "label": payload.get("label") or track_id,
            "track_type": payload.get("track_type") or "unknown",
            "lat": payload["lat"],
            "lon": payload["lon"],
        })

    alerts: list[dict] = [dict(alert) for alert in snapshot.get("alerts") or []]
    seen_alerts = {alert.get("id") for alert in alerts}
    for payload in overlay.get("alert") or []:
        alert_id = str(payload.get("id") or payload.get("alert_id") or "")
        if alert_id and alert_id not in seen_alerts:
            alerts.append(payload)
            seen_alerts.add(alert_id)

    commands: list[dict] = []
    for raw in snapshot.get("recent_commands") or []:
        command = dict(raw)
        command["phase"] = command_phase(str(command.get("status") or "pending"), command.get("result"))
        commands.append(command)

    missions = []
    for raw in snapshot.get("missions") or []:
        mission = dict(raw)
        mission["progress"] = mission_progress(mission, commands)
        missions.append(mission)

    from src.operator_app.telem_timing import now_ms, stages_for, stamp_frame

    frame_ms = now_ms()
    for card in assets:
        asset_id = str(card.get("id") or "")
        if not asset_id:
            continue
        stages = stages_for(asset_id)
        if not stages:
            continue
        if stages.get("frame_ms") is None:
            stages["frame_ms"] = frame_ms
            stamp_frame(asset_id, frame_ms)
        card["stages"] = stages

    return {
        "timestamp": snapshot.get("timestamp"),
        "assets": assets,
        "tracks": tracks,
        "alerts": alerts,
        "missions": missions,
        "recent_commands": commands,
        "zones": list(zones or []),
    }


async def cop_frame(snapshot: dict) -> dict:
    if _overlay_loaded or (_redis_down and time.monotonic() < _redis_retry_at):
        overlay = read_redis_overlay()
    else:
        overlay = await asyncio.to_thread(read_redis_overlay)
    zones = await asyncio.to_thread(zone_rings)
    return present_snapshot(snapshot, overlay, zones)


def _asset_cards(snapshot: dict, overlay: dict, asset_ids: set[str]) -> list[dict]:
    reduced = {
        "timestamp": snapshot.get("timestamp"),
        "assets": [
            asset for asset in (snapshot.get("assets") or [])
            if str(asset.get("id") or "") in asset_ids
        ],
        "tracks": [],
        "alerts": [],
        "missions": [],
        "recent_commands": [],
    }
    return present_snapshot(reduced, overlay, None)["assets"]


def _track_cards(snapshot: dict, track_ids: set[str]) -> list[dict]:
    return [
        dict(track)
        for track in (snapshot.get("tracks") or [])
        if str(track.get("id") or "") in track_ids
    ]


def serialize_frame(frame: dict) -> str:
    """One JSON text for every subscriber. Sockets send this string as-is."""
    return json.dumps(frame, default=str, separators=(",", ":"))


async def emit_delta(asset_ids: set[str], track_ids: set[str], picture: bool) -> None:
    """One coalesced push. Asset polls do not rebuild zones or the whole fleet."""
    provider = _provider
    if provider is None or hub.subscribers == 0:
        return
    try:
        snapshot = await provider()
    except Exception:
        return
    overlay = read_redis_overlay() if _overlay_loaded else {}
    frame: dict = {"type": "delta", "timestamp": snapshot.get("timestamp")}
    try:
        if picture:
            zones = await asyncio.to_thread(zone_rings)
            body = present_snapshot(snapshot, overlay, zones)
            frame["assets"] = body["assets"]
            frame["tracks"] = body["tracks"]
            frame["alerts"] = body["alerts"]
            frame["missions"] = body["missions"]
            frame["recent_commands"] = body["recent_commands"]
            frame["zones"] = body["zones"]
        else:
            if asset_ids:
                frame["assets"] = _asset_cards(snapshot, overlay, asset_ids)
            if track_ids:
                frame["tracks"] = _track_cards(snapshot, track_ids)
    except Exception:
        return
    if set(frame) <= {"type", "timestamp"}:
        return
    hub.publish(serialize_frame(frame))


def _mark_dirty(asset_id: str | None, track_id: str | None) -> None:
    global _dirty_picture
    if hub.subscribers == 0:
        return
    if asset_id:
        _dirty_assets.add(str(asset_id))
    elif track_id:
        _dirty_tracks.add(str(track_id))
    else:
        _dirty_picture = True
    _ensure_coalesce()


def _ensure_coalesce() -> None:
    global _coalesce_task
    if _coalesce_task is not None and not _coalesce_task.done():
        return
    _coalesce_task = asyncio.create_task(_coalesced_emit())


async def _coalesced_emit() -> None:
    global _dirty_picture
    while True:
        await asyncio.sleep(_COALESCE_S)
        asset_ids = set(_dirty_assets)
        track_ids = set(_dirty_tracks)
        picture = _dirty_picture
        _dirty_assets.clear()
        _dirty_tracks.clear()
        _dirty_picture = False
        if not asset_ids and not track_ids and not picture:
            return
        await emit_delta(asset_ids, track_ids, picture)
        if not _dirty_assets and not _dirty_tracks and not _dirty_picture:
            return


def notify_live(asset_id: str | None = None, *, track_id: str | None = None) -> None:
    """Schedule a delta. Changes inside one 50 ms window share a frame."""
    if hub.subscribers == 0:
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = hub.loop
        if loop is None or not loop.is_running():
            return
        loop.call_soon_threadsafe(_mark_dirty, asset_id, track_id)
        return
    _mark_dirty(asset_id, track_id)


def _notify_overlay(channel: str, payload: dict) -> None:
    kind = _CHANNEL_KIND.get(str(channel))
    if kind == "platform":
        asset_id = str(payload.get("asset_id") or payload.get("platform_id") or payload.get("id") or "")
        if asset_id:
            notify_live(asset_id)
            return
    if kind == "track":
        track_id = str(payload.get("id") or payload.get("track_id") or "")
        if track_id:
            notify_live(track_id=track_id)
            return
    notify_live()


def start_redis_listener() -> None:
    """Wake the hub when Redis publishes telemetry, tracks, alerts, or commands."""
    global _listener_started
    with _lock:
        if _listener_started:
            return
        _listener_started = True
    threading.Thread(target=_listen, name="cnc-cop-redis", daemon=True).start()


def _listen() -> None:
    import redis

    while True:
        store = live_redis()
        if store is None:
            delay = max(0.2, _redis_retry_at - time.monotonic())
            time.sleep(min(delay, 5.0))
            continue
        client = redis.Redis.from_url(
            store._url,
            decode_responses=True,
            socket_connect_timeout=0.5,
            socket_timeout=None,
        )
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        try:
            try:
                with _lock:
                    _warm_overlay(store)
            except Exception:
                _mark_redis_down()
                continue
            pubsub.subscribe(
                "cnc:telemetry",
                "cnc:track",
                "cnc:detection",
                "cnc:alert",
                "cnc:command",
            )
            for message in pubsub.listen():
                if not isinstance(message, dict) or message.get("type") != "message":
                    continue
                raw = message.get("data")
                channel = message.get("channel") or ""
                if isinstance(raw, str):
                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError:
                        payload = None
                    if isinstance(payload, dict):
                        note_overlay(str(channel), payload)
                        _notify_overlay(str(channel), payload)
                        continue
                notify_live()
        except Exception:
            _mark_redis_down()
        finally:
            try:
                pubsub.close()
            except Exception:
                pass
            try:
                client.close()
            except Exception:
                pass
