"""Stage stamps and the durable telemetry queue."""

import asyncio

from src.operator_app.live_feed import present_snapshot, zone_rings
from src.operator_app.telem_timing import (
    dropped,
    note_ingest,
    note_nats,
    note_poll,
    offer,
    reset,
    stamp_frame,
    summary,
)


def setup_function():
    reset()


def test_stage_deltas_split_by_asset_type():
    note_poll("uav-01", "uav", None, 1000.0, 1200.0, 200.0)
    note_poll("ugv-01", "ugv", 500.0, 550.0, 600.0, 50.0)
    stamp_frame("uav-01", 1250.0)
    stamp_frame("ugv-01", 640.0)
    note_nats("uav-01", 1200.0, 1400.0)
    note_ingest("uav-01", 1200.0, 1550.0)
    body = summary()
    uav = body["by_type"]["uav"]
    ugv = body["by_type"]["ugv"]
    assert uav["source_to_adapter"]["n"] == 0
    assert uav["adapter_to_poll"]["p50_ms"] == 200.0
    assert uav["poll_to_frame"]["p50_ms"] == 50.0
    assert uav["message_gap"]["p50_ms"] == 200.0
    assert uav["poll_to_nats"]["p50_ms"] == 200.0
    assert uav["nats_to_ingest"]["p50_ms"] == 150.0
    assert ugv["source_to_adapter"]["p50_ms"] == 50.0
    assert ugv["adapter_to_poll"]["p50_ms"] == 50.0
    assert body["by_type"]["all"]["message_gap"]["n"] == 2
    assert body["durable_dropped"] == 0


def test_frame_keeps_the_first_stamp_and_present_snapshot_copies_it():
    note_poll("uav-01", "uav", None, 1000.0, 1100.0, 100.0)
    frame = present_snapshot({"assets": [{"id": "uav-01", "asset_type": "uav", "name": "Eagle-1"}]})
    stages = frame["assets"][0]["stages"]
    assert stages["poll_ms"] == 1100.0
    assert stages["message_gap_ms"] == 100.0
    assert stages["source_ms"] is None
    assert isinstance(stages["frame_ms"], float)
    stamp_frame("uav-01", stages["frame_ms"] + 500.0)
    again = present_snapshot({"assets": [{"id": "uav-01", "asset_type": "uav"}]})
    assert again["assets"][0]["stages"]["frame_ms"] == stages["frame_ms"]


def test_full_durable_queue_drops_the_oldest():
    queue = asyncio.Queue(maxsize=2)
    assert offer(queue, "a")
    assert offer(queue, "b")
    assert offer(queue, "c")
    assert dropped() == 1
    assert queue.get_nowait() == "b"
    assert queue.get_nowait() == "c"


def test_stale_redis_pose_does_not_replace_a_newer_poll():
    frame = present_snapshot(
        {
            "assets": [{
                "id": "ugv-01",
                "asset_type": "ugv",
                "lat": 17.382,
                "lon": 78.484,
                "last_seen": "2026-10-13T12:30:20+00:00",
            }],
        },
        {
            "platform": [{
                "asset_id": "ugv-01",
                "timestamp": "2026-10-13T11:40:00+00:00",
                "position": {"lat": 17.40, "lon": 78.50, "alt": 0, "heading": 1},
                "speed": 3,
                "status": "mission",
            }],
        },
    )
    assert frame["assets"][0]["lat"] == 17.382
    fresh = present_snapshot(
        {
            "assets": [{
                "id": "ugv-01",
                "asset_type": "ugv",
                "lat": 17.382,
                "lon": 78.484,
                "last_seen": "2026-10-13T12:30:20+00:00",
            }],
        },
        {
            "platform": [{
                "asset_id": "ugv-01",
                "timestamp": "2026-10-13T12:30:21+00:00",
                "position": {"lat": 17.383, "lon": 78.485, "alt": 0, "heading": 2},
                "speed": 1,
            }],
        },
    )
    assert fresh["assets"][0]["lat"] == 17.383


def test_zone_rings_backs_off_after_a_repository_failure(monkeypatch):
    calls = {"n": 0}

    class Down:
        class zones:
            @staticmethod
            def list():
                calls["n"] += 1
                raise RuntimeError("postgres down")

    monkeypatch.setattr("src.operator_app.live_feed.get_repositories", lambda: Down())
    monkeypatch.setattr("src.operator_app.live_feed._zone_fail_until", 0.0)
    assert zone_rings() == []
    assert zone_rings() == []
    assert calls["n"] == 1
