"""Push the common operating picture. The dashboard waits on a queue.

Redis supplies live positions when it answers. A failed ping waits on a
backoff and is tried again, so the overlay returns without a process restart.
Frames are pushed when state changes. One frame is in flight, then one more
with the latest snapshot. Missions, command phase, and zones come from the
operator snapshot and the zone repository.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
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
_frame_task: asyncio.Task | None = None
_frame_again = False
_lock = threading.Lock()

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
        queue: asyncio.Queue = asyncio.Queue(maxsize=8)
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
    rings: list[dict] = []
    try:
        zones = get_repositories().zones.list()
    except Exception:
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


async def emit_frame() -> None:
    provider = _provider
    if provider is None:
        return
    try:
        frame = await cop_frame(await provider())
    except Exception:
        return
    hub.publish(frame)


def _schedule_frame() -> None:
    global _frame_task, _frame_again
    if _frame_task is not None and not _frame_task.done():
        _frame_again = True
        return
    _frame_again = False
    _frame_task = asyncio.create_task(_pump_frames())


async def _pump_frames() -> None:
    global _frame_again
    while True:
        _frame_again = False
        await emit_frame()
        if not _frame_again:
            return


def notify_live() -> None:
    """Schedule one frame. A second change coalesces into the next push."""
    if hub.subscribers == 0:
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = hub.loop
        if loop is None or not loop.is_running():
            return
        loop.call_soon_threadsafe(_schedule_frame)
        return
    _schedule_frame()


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
