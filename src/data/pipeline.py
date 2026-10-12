"""Start the ingest and fusion JetStream consumers in this process."""

from __future__ import annotations

import asyncio

from src.data.bus import PullConsumer, current_mode, get_bus

_consumers: list[PullConsumer] = []
_gate = asyncio.Lock()


def _live(consumers: list[PullConsumer]) -> bool:
    return bool(consumers) and all(consumer.running for consumer in consumers)


async def start_pipeline() -> bool:
    """Return True when the broker consumers are running."""
    async with _gate:
        return await _start_pipeline()


async def restart_pipeline() -> bool:
    """Stop pull consumers and subscribe again. Used after a broker reconnect."""
    async with _gate:
        await _stop_pipeline()
        return await _start_pipeline()


async def _start_pipeline() -> bool:
    global _consumers
    if current_mode() != "nats":
        return False
    if _live(_consumers):
        return True
    _consumers = []
    bus = get_bus()
    if not await bus.ensure_jetstream():
        return False
    from src.ai_fusion.stream_fusion import handle_telemetry as fuse_telemetry
    from src.data.ingest_consumer import handle_alert, handle_event, handle_telemetry

    specs = (
        ("CNC_TELEMETRY", "cnc-ingest", "cnc.telemetry.>", handle_telemetry),
        ("CNC_EVENT", "cnc-ingest-event", "cnc.event", handle_event),
        ("CNC_ALERT", "cnc-ingest-alert", "cnc.alert", handle_alert),
        ("CNC_TELEMETRY", "cnc-fusion", "cnc.telemetry.>", fuse_telemetry),
    )
    started: list[PullConsumer] = []
    try:
        for stream, durable, subject, handler in specs:
            consumer = PullConsumer(bus, stream, durable, subject, handler)
            if not await consumer.start():
                raise RuntimeError(f"{durable} did not start")
            started.append(consumer)
    except Exception:
        for consumer in started:
            await consumer.stop()
        raise
    _consumers = started
    return True


async def stop_pipeline() -> None:
    async with _gate:
        await _stop_pipeline()


async def _stop_pipeline() -> None:
    global _consumers
    current = list(_consumers)
    _consumers = []
    for consumer in current:
        await consumer.stop()
