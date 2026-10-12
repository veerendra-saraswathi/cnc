"""Publish committed command rows. A down broker leaves them unpublished.

Platform.issue inserts the command, its event, and this row in one
transaction. This relay is the only publisher. A failed publish increments
attempts and tries again after backoff. It does not call the adapter.
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable

from loguru import logger

from src.data.bus import current_mode, get_bus
from src.data.subjects import command_subject
from src.data_services.ids import uuid7
from src.data_services.outbox_record import utcnow
from src.data_services.repositories import get_repositories

Publisher = Callable[[str, dict, str], Awaitable[bool]]
Connected = Callable[[], Awaitable[None]]

_relay: OutboxRelay | None = None


def backoff_seconds(attempts: int) -> float:
    """Delay after a failed publish. The first retry waits a short step."""
    if attempts <= 0:
        return 0.0
    return min(5.0, 0.05 * (2 ** (attempts - 1)))


class OutboxRelay:
    def __init__(self) -> None:
        self._publisher: Publisher | None = None
        self._on_connected: Connected | None = None
        self._next_due: dict[str, float] = {}
        self._task: asyncio.Task | None = None
        self._stop = False
        self._lock = asyncio.Lock()

    def set_publisher(self, publisher: Publisher | None) -> None:
        """Tests inject a publisher. None uses JetStream."""
        self._publisher = publisher

    def reset_schedule(self) -> None:
        self._next_due.clear()

    async def start(self, on_connected: Connected | None = None) -> bool:
        if on_connected is not None:
            self._on_connected = on_connected
        if self._task is not None and not self._task.done():
            return True
        if self._publisher is None and current_mode() != "nats":
            return False
        self._stop = False
        self._task = asyncio.create_task(self._loop(), name="cnc-outbox")
        return True

    async def stop(self) -> None:
        self._stop = True
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    async def publish_pending(self, *, force: bool = False) -> int:
        """Publish due rows. Returns how many JetStream accepted."""
        async with self._lock:
            return await self._publish_pending(force=force)

    async def _publish_pending(self, *, force: bool) -> int:
        rows = get_repositories().list_unpublished()
        if not rows:
            return 0
        now = time.monotonic()
        if self._publisher is None:
            bus = get_bus()
            bus.prepare_retry()
            if not await bus.ensure_jetstream():
                self._defer(rows, force=force, now=now)
                return 0
        published = 0
        for row in rows:
            due = self._next_due.get(row.id, 0.0)
            if not force and now < due:
                continue
            msg_id = str(row.payload.get("id") or row.payload.get("command_id") or row.id)
            try:
                ok = await self._emit(row.subject, row.payload, msg_id)
            except Exception as exc:
                logger.warning(f"[outbox] publish {row.subject} failed: {exc}")
                ok = False
            if ok:
                get_repositories().mark_published(row.id, utcnow())
                self._next_due.pop(row.id, None)
                published += 1
                continue
            self._defer([row], force=True, now=time.monotonic())
        return published

    def _defer(self, rows, *, force: bool, now: float) -> None:
        for row in rows:
            due = self._next_due.get(row.id, 0.0)
            if not force and now < due:
                continue
            get_repositories().note_attempt(row.id)
            self._next_due[row.id] = time.monotonic() + backoff_seconds(row.attempts + 1)

    async def _emit(self, subject: str, payload: dict, msg_id: str) -> bool:
        if self._publisher is not None:
            return bool(await self._publisher(subject, payload, msg_id))
        return await get_bus().publish_jetstream(subject, payload, msg_id=msg_id)

    async def _loop(self) -> None:
        delay = 0.2
        while not self._stop:
            try:
                up = await self._broker_ready()
                count = 0
                if up:
                    if self._on_connected is not None:
                        await self._on_connected()
                    count = await self.publish_pending()
                    delay = 0.2 if count else 0.5
                else:
                    delay = min(5.0, max(0.4, delay * 2))
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning(f"[outbox] relay: {exc}")
                delay = min(5.0, max(0.4, delay * 2))
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                break

    async def _broker_ready(self) -> bool:
        if self._publisher is not None:
            return True
        if current_mode() != "nats":
            return False
        bus = get_bus()
        bus.prepare_retry()
        return await bus.ensure_jetstream()


def get_relay() -> OutboxRelay:
    global _relay
    if _relay is None:
        _relay = OutboxRelay()
    return _relay


def reset_relay() -> None:
    """Drop the in-process relay. Used by tests, not by Platform.stop."""
    global _relay
    _relay = OutboxRelay()


async def start_relay(
    on_connected: Connected | None = None,
    on_reconnect: Connected | None = None,
) -> bool:
    """on_reconnect runs after nats-py reconnects, then pending rows are published."""
    relay = get_relay()
    if on_reconnect is not None:
        async def _resume() -> None:
            try:
                await on_reconnect()
            except Exception as exc:
                logger.warning(f"[outbox] reconnect consumers: {exc}")
            await relay.publish_pending(force=True)

        get_bus().add_resume(_resume)
    return await relay.start(on_connected)


async def stop_relay() -> None:
    if _relay is None:
        return
    await _relay.stop()


async def publish_pending(*, force: bool = False) -> int:
    return await get_relay().publish_pending(force=force)


def enqueue_issued(command, event) -> str:
    """Insert command, event, and outbox. Raises when the transaction rolls back."""
    subject = command_subject(command.asset_id)
    return get_repositories().commit_issued(command, event, subject, outbox_id=uuid7())


def command_published(command_id: str) -> bool:
    return get_repositories().command_published(command_id)


def mark_command_published(command_id: str) -> None:
    try:
        get_repositories().mark_published_command(command_id, utcnow())
    except Exception as exc:
        logger.warning(f"[outbox] mark {command_id} published failed: {exc}")
