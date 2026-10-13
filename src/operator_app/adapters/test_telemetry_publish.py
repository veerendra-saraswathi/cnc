"""Bounded JetStream acks for vendor telemetry."""

import asyncio

from src.data.bus import get_bus, reset_bus
from src.operator_app.adapters import telemetry_publish as publish


def setup_function():
    reset_bus()
    publish._reset_publish_state()
    publish._pending.clear()


def teardown_function():
    reset_bus()
    publish._pending.clear()


def _sample(asset_id: str = "uav-01") -> dict:
    return {
        "asset_id": asset_id,
        "lat": 17.1,
        "lon": 78.2,
        "alt": 30,
        "extras": {"timing": {"poll_ms": 1_000.0}},
    }


def test_nats_publish_bounds_in_flight_acks(monkeypatch):
    monkeypatch.setenv("CNC_BUS", "nats")
    reset_bus()
    bus = get_bus()
    publish._inflight = asyncio.Semaphore(2)
    current = {"n": 0, "peak": 0}

    async def slow(subject, payload, msg_id=None, timeout=None):
        current["n"] += 1
        current["peak"] = max(current["peak"], current["n"])
        await asyncio.sleep(0.05)
        current["n"] -= 1
        return True

    async def body():
        monkeypatch.setattr(bus, "publish_jetstream", slow)
        for _ in range(6):
            assert await publish.accept_vendor_telemetry(_sample()) is not None
        await asyncio.wait(set(publish._pending), timeout=2)
        assert current["peak"] <= 2
        assert current["peak"] >= 1

    asyncio.run(body())


def test_publish_failures_raise_one_alert(monkeypatch):
    monkeypatch.setenv("CNC_BUS", "nats")
    reset_bus()
    bus = get_bus()
    alerts: list[str] = []

    async def refuse(subject, payload, msg_id=None, timeout=None):
        return False

    async def grab(asset_id, message):
        alerts.append(message)

    async def body():
        monkeypatch.setattr(bus, "publish_jetstream", refuse)
        monkeypatch.setattr(publish, "_raise_publish_alert", grab)
        for _ in range(5):
            assert await publish.accept_vendor_telemetry(_sample("ugv-01")) is not None
        if publish._pending:
            await asyncio.wait(set(publish._pending), timeout=2)
        assert len(alerts) == 1
        assert "ugv-01" in alerts[0]
        assert "failures" in alerts[0]

    asyncio.run(body())
