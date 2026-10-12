"""Ingest writes hot state and the durable store. No Redis and no DuckDB file."""

import json

import pytest

from src.data.ingest import (
    configure,
    ingest_alert,
    ingest_command,
    ingest_detection,
    ingest_track,
)
from src.data.redis_live import RedisLiveStore


class MemoryLive:
    def __init__(self):
        self.objects = {}
        self.streams = []
        self.fail = False

    def put(self, kind, object_id, payload):
        if self.fail:
            raise RuntimeError("redis down")
        self.objects[(kind, object_id)] = dict(payload)
        self.publish(kind, payload)

    def publish(self, kind, payload):
        if self.fail:
            raise RuntimeError("redis down")
        self.streams.append((kind, dict(payload)))

    def get(self, kind, object_id):
        return dict(self.objects.get((kind, object_id), {}))

    def read_recent(self, kind, limit=50):
        rows = [payload for key, payload in self.streams if key == kind]
        return rows[-limit:]

    def list_ids(self, kind):
        return [object_id for (key, object_id) in self.objects if key == kind]


class MemoryDurable:
    def __init__(self):
        self.tracks = {}
        self.detections = []
        self.alerts = []
        self.commands = []
        self.telemetry = []

    def save_telemetry(self, record):
        self.telemetry.append(dict(record))

    def save_track(self, track):
        track_id = track.get("track_id") or track.get("id")
        self.tracks[track_id] = dict(track)

    def save_detection(self, detection):
        self.detections.append(dict(detection))

    def save_alert(self, alert):
        self.alerts.append(dict(alert))

    def save_command(self, command):
        self.commands.append(dict(command))


class FakeRedis:
    def __init__(self):
        self.calls = []
        self.hashes = {}
        self.streams = {}

    def hset(self, key, mapping):
        self.calls.append(("hset", key, mapping))
        self.hashes[key] = dict(mapping)

    def expire(self, key, ttl):
        self.calls.append(("expire", key, ttl))

    def xadd(self, name, fields, maxlen=None, approximate=None):
        self.calls.append(("xadd", name, fields, maxlen, approximate))
        self.streams.setdefault(name, []).append(("1-0", dict(fields)))
        return "1-0"

    def publish(self, channel, message):
        self.calls.append(("publish", channel, message))
        return 1

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def xrevrange(self, name, count=None):
        rows = list(reversed(self.streams.get(name, [])))
        if count is not None:
            return rows[:count]
        return rows

    def keys(self, pattern):
        return []


@pytest.fixture
def pipeline():
    live = MemoryLive()
    durable = MemoryDurable()
    configure(live=live, durable=durable)
    try:
        yield live, durable
    finally:
        configure(live=None, durable=None)


def test_ingest_track_updates_in_place(pipeline):
    live, durable = pipeline
    ingest_track({"track_id": "T-1", "lat": 17.1, "lon": 78.1, "label": "vehicle"})
    ingest_track({"track_id": "T-1", "lat": 17.2, "lon": 78.1, "label": "vehicle"})

    assert live.objects[("track", "T-1")]["lat"] == 17.2
    assert "ingested_at" in live.objects[("track", "T-1")]
    assert [kind for kind, _ in live.streams].count("track") == 2
    assert list(durable.tracks) == ["T-1"]
    assert durable.tracks["T-1"]["lat"] == 17.2


def test_ingest_detection_and_alert(pipeline):
    live, durable = pipeline
    ingest_detection({
        "detection_id": "d1",
        "asset_id": "uav-01",
        "lat": 17.4,
        "lon": 78.5,
        "label": "vehicle",
        "confidence": 0.8,
    })
    ingest_alert({
        "id": "a1",
        "level": "warning",
        "message": "new track",
        "asset_id": "uav-01",
    })

    assert live.objects[("detection", "d1")]["asset_id"] == "uav-01"
    assert durable.detections[0]["detection_id"] == "d1"
    assert [kind for kind, _ in live.streams].count("detection") == 1
    assert live.objects[("alert", "a1")]["message"] == "new track"
    assert durable.alerts[0]["alert_id"] == "a1"
    assert [kind for kind, _ in live.streams].count("alert") == 1


def test_ingest_command_keeps_operator_fields(pipeline):
    live, durable = pipeline
    record = ingest_command({
        "command_id": "c1",
        "asset_id": "ugv-01",
        "command_type": "hold",
        "issued_by": "operator-1",
        "issued_at": "2026-10-12T00:00:00+00:00",
        "parameters": {},
    })

    assert record["issued_by"] == "operator-1"
    assert record["asset_id"] == "ugv-01"
    assert record["issued_at"] == "2026-10-12T00:00:00+00:00"
    assert live.objects[("command", "c1")]["issued_by"] == "operator-1"
    assert durable.commands[0]["asset_id"] == "ugv-01"
    assert durable.commands[0]["issued_at"] == "2026-10-12T00:00:00+00:00"


def test_live_failure_still_writes_durable(pipeline):
    live, durable = pipeline
    live.fail = True
    record = ingest_track({"track_id": "T-9", "lat": 1.0, "lon": 2.0})
    assert record["track_id"] == "T-9"
    assert "ingested_at" in record
    assert ("track", "T-9") not in live.objects
    assert durable.tracks["T-9"]["lat"] == 1.0


def test_redis_live_store_uses_phase1_keys():
    fake = FakeRedis()
    store = RedisLiveStore(client=fake)
    store.put("platform", "uav-01", {"lat": 17.1, "mode": "GOTO"})

    assert [call[0] for call in fake.calls] == ["hset", "expire", "xadd", "publish"]
    assert fake.calls[0][1] == "cnc:platform:uav-01"
    assert json.loads(fake.calls[0][2]["body"])["lat"] == 17.1
    assert fake.calls[1] == ("expire", "cnc:platform:uav-01", 3600)
    assert fake.calls[2][1] == "cnc:stream:platform"
    assert fake.calls[2][3] == 200
    assert fake.calls[3][1] == "cnc:telemetry"

    store.put("track", "T-1", {"track_id": "T-1"})
    store.put("detection", "d1", {"detection_id": "d1"})
    store.put("alert", "a1", {"alert_id": "a1"})
    store.put("command", "c1", {"command_id": "c1"})

    keys = [call[1] for call in fake.calls if call[0] == "hset"]
    assert keys == [
        "cnc:platform:uav-01",
        "cnc:track:T-1",
        "cnc:detection:d1",
        "cnc:alert:a1",
        "cnc:command:c1",
    ]
    channels = [call[1] for call in fake.calls if call[0] == "publish"]
    assert channels == [
        "cnc:telemetry",
        "cnc:track",
        "cnc:detection",
        "cnc:alert",
        "cnc:command",
    ]
    streams = [call[1] for call in fake.calls if call[0] == "xadd"]
    assert streams == [
        "cnc:stream:platform",
        "cnc:stream:track",
        "cnc:stream:detection",
        "cnc:stream:alert",
        "cnc:stream:command",
    ]


def test_redis_live_reads_body_and_legacy_hash():
    fake = FakeRedis()
    store = RedisLiveStore(client=fake)
    store.put("alert", "a1", {"message": "hi"})
    assert store.get("alert", "a1")["message"] == "hi"

    fake.hashes["cnc:platform:old"] = {"lat": "17.1", "mode": "GOTO"}
    legacy = store.get("platform", "old")
    assert legacy["lat"] == 17.1
    assert legacy["mode"] == "GOTO"
