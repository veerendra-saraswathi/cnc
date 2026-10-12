"""JetStream publish and pull consumers, with an in-process fallback.

This module does not import fusion, tracking, or the operator app.
CNC_BUS=nats dials the broker. Any other value, including unset, stays
in process and does not open a connection. The first dial is one attempt.
After it connects, nats-py reconnects on its own. A refused first dial is
remembered until prepare_retry, so a later command does not wait on the
connect timeout again.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Awaitable, Callable

from loguru import logger

from src.data.subjects import (
    DLQ_SUBJECT,
    STREAMS,
    StreamSpec,
    subject_matches,
)

Handler = Callable[[str, bytes], Awaitable[None]]
LocalHandler = Callable[[str, bytes], Awaitable[None]]

MAX_DELIVER = 5
ACK_WAIT_SECONDS = 30
FETCH_TIMEOUT = 0.2


class PoisonMessage(Exception):
    """The payload will not become valid on retry. Term it and use cnc.dlq."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def current_mode() -> str:
    value = os.environ.get("CNC_BUS", "off").strip().lower()
    if value == "nats":
        return "nats"
    return "off"


def ack_timeout() -> float:
    raw = os.environ.get("CNC_ACK_TIMEOUT_S", "5")
    try:
        return max(0.1, float(raw))
    except ValueError:
        return 5.0


def _dumps(payload: dict) -> bytes:
    return json.dumps(payload, default=str, separators=(",", ":")).encode()


def _show(payload) -> object:
    if isinstance(payload, bytes):
        text = payload.decode(errors="replace")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    try:
        json.dumps(payload)
    except TypeError:
        return str(payload)
    return payload


class MessageBus:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self._nc = None
        self._js = None
        self._gave_up = False
        self._closing = False
        self._ready = False
        self._resume: list[Callable[[], Awaitable[None]]] = []
        self._dial_lock = asyncio.Lock()
        self._local: list[tuple[str, LocalHandler]] = []
        self._acks: dict[str, asyncio.Future] = {}

    @property
    def js(self):
        if self._js is None:
            raise RuntimeError("JetStream is not connected")
        return self._js

    def add_resume(self, callback: Callable[[], Awaitable[None]]) -> None:
        """Run after the broker reconnects. The callback is not awaited inline."""
        if callback not in self._resume:
            self._resume.append(callback)

    def subscribe_local(self, pattern: str, callback: LocalHandler) -> None:
        self._local.append((pattern, callback))

    def has_local(self, subject: str) -> bool:
        return any(subject_matches(pattern, subject) for pattern, _ in self._local)

    def _connected(self) -> bool:
        nc = self._nc
        if nc is None:
            return False
        # A test double with a JetStream stand-in has no connection flag.
        if not hasattr(nc, "is_connected"):
            return self._js is not None
        return bool(nc.is_connected)

    def _reconnecting(self) -> bool:
        nc = self._nc
        return bool(nc is not None and getattr(nc, "is_reconnecting", False))

    async def ensure_jetstream(self) -> bool:
        if self.mode != "nats":
            return False
        if self._connected():
            if self._js is None and self._nc is not None:
                self._js = self._nc.jetstream()
            self._ready = True
            return True
        if self._reconnecting():
            return False
        if self._nc is not None and not self._connected():
            self._nc = None
            self._js = None
        if self._gave_up:
            return False
        async with self._dial_lock:
            if self._connected():
                return True
            if self._reconnecting():
                return False
            if self._gave_up:
                return False
            from src.data_services.settings import setting

            url = setting("CNC_NATS_URL", "nats://127.0.0.1:4222")
            token = setting("NATS_TOKEN", "")
            try:
                self._nc = await open_connection(url, token, self._callbacks())
                self._js = self._nc.jetstream()
                self._ready = True
                await self.ensure_streams()
                return True
            except Exception as exc:
                detail = str(exc).splitlines()
                # TimeoutError from the client often has an empty message.
                logger.warning(
                    f"[bus] JetStream unavailable ({exc.__class__.__name__}): "
                    f"{detail[0] if detail else exc.__class__.__name__}"
                )
                nc = self._nc
                self._nc = None
                self._js = None
                self._ready = False
                self._gave_up = True
                if nc is not None:
                    try:
                        await nc.close()
                    except Exception:
                        pass
                return False

    def _callbacks(self) -> dict:
        return {
            "disconnected": self._on_disconnected,
            "reconnected": self._on_reconnected,
            "closed": self._on_closed,
            "error": self._on_error,
        }

    async def _on_disconnected(self) -> None:
        self._ready = False

    async def _on_reconnected(self) -> None:
        self._ready = True
        self._gave_up = False
        nc = self._nc
        if nc is not None:
            try:
                self._js = nc.jetstream()
            except Exception:
                self._js = None
        for callback in list(self._resume):
            asyncio.create_task(self._run_resume(callback))

    async def _run_resume(self, callback) -> None:
        try:
            await callback()
        except Exception as exc:
            logger.warning(f"[bus] resume failed: {exc}")

    async def _on_closed(self) -> None:
        if self._closing:
            return
        self._ready = False
        self._nc = None
        self._js = None

    async def _on_error(self, exc=None) -> None:
        if exc is None:
            return
        logger.warning(f"[bus] nats error: {exc}")

    async def ensure_streams(self) -> None:
        from nats.js.api import DiscardPolicy, RetentionPolicy, StorageType, StreamConfig
        from nats.js.errors import NotFoundError

        for spec in STREAMS:
            config = _stream_config(spec, DiscardPolicy, RetentionPolicy, StorageType, StreamConfig)
            try:
                await self.js.stream_info(spec.name)
            except NotFoundError:
                await self.js.add_stream(config=config)
            else:
                await self.js.update_stream(config=config)

    def prepare_retry(self) -> None:
        """Allow another dial after a refused broker. A live connection stays up."""
        if self._js is None:
            self._gave_up = False

    async def publish_jetstream(self, subject: str, payload: dict, *, msg_id: str | None = None) -> bool:
        """Publish via JetStream. False means the broker did not accept it.

        A successful publish is not also delivered to local subscribers.
        msg_id is the JetStream Nats-Msg-Id deduplication header.
        """
        if self.mode != "nats":
            return False
        if self._nc is not None and not self._connected():
            return False
        if not await self.ensure_jetstream():
            return False
        headers = {"Nats-Msg-Id": msg_id} if msg_id else None
        try:
            await self.js.publish(subject, _dumps(payload), headers=headers)
            return True
        except Exception as exc:
            logger.warning(f"[bus] publish {subject} failed: {exc}")
            return False

    async def deliver_local(self, subject: str, payload: dict) -> bool:
        handlers = [cb for pattern, cb in self._local if subject_matches(pattern, subject)]
        if not handlers:
            return False
        raw = _dumps(payload)
        for callback in handlers:
            await callback(subject, raw)
        return True

    async def publish_dlq(self, *, reason: str, payload, subject: str) -> bool:
        body = {"reason": reason, "subject": subject, "payload": _show(payload)}
        if await self.publish_jetstream(DLQ_SUBJECT, body):
            return True
        return await self.deliver_local(DLQ_SUBJECT, body)

    def expect_ack(self, command_id: str) -> None:
        self._acks[command_id] = asyncio.get_running_loop().create_future()

    async def wait_ack(self, command_id: str, timeout: float) -> dict | None:
        future = self._acks.get(command_id)
        if future is None:
            return None
        try:
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            return None

    def forget_ack(self, command_id: str) -> None:
        future = self._acks.pop(command_id, None)
        if future is not None and not future.done():
            future.cancel()

    def notify_ack(self, command_id: str, payload: dict) -> None:
        future = self._acks.get(command_id)
        if future is not None and not future.done():
            future.set_result(payload)

    async def close(self) -> None:
        self._closing = True
        self._gave_up = False
        self._ready = False
        nc = self._nc
        self._nc = None
        self._js = None
        if nc is not None:
            try:
                await nc.close()
            except Exception:
                pass
        self._closing = False

    def detach(self) -> None:
        nc = self._nc
        self._nc = None
        self._js = None
        if nc is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(_close_connection(nc))


async def open_connection(url: str, token: str = "", callbacks: dict | None = None):
    """One attempt for the first dial. A live client then reconnects on its own.

    nats-py treats a non-positive max_reconnect_attempts as forever, and
    allow_reconnect also retries the initial connect. The first dial stays
    a single attempt. After it returns, the same client is allowed to
    reconnect without a new process.
    """
    try:
        import nats
    except ImportError as exc:
        raise RuntimeError("nats client is not installed") from exc
    hooks = callbacks or {}
    nc = await nats.connect(
        url,
        token=token or None,
        name="cnc",
        allow_reconnect=False,
        max_reconnect_attempts=1,
        reconnect_time_wait=0,
        connect_timeout=2,
        disconnected_cb=hooks.get("disconnected"),
        reconnected_cb=hooks.get("reconnected"),
        closed_cb=hooks.get("closed"),
        error_cb=hooks.get("error"),
    )
    nc.options["allow_reconnect"] = True
    nc.options["max_reconnect_attempts"] = -1
    nc.options["reconnect_time_wait"] = 0.5
    return nc


async def _close_connection(nc) -> None:
    try:
        await nc.close()
    except Exception:
        pass


def _stream_config(spec: StreamSpec, discard_policy, retention_policy, storage_type, stream_config):
    # Limits retention keeps the message for every durable consumer.
    config = stream_config(
        name=spec.name,
        subjects=list(spec.subjects),
        retention=retention_policy.LIMITS,
        storage=storage_type.FILE,
        discard=discard_policy.OLD,
        max_age=spec.max_age_seconds,
        max_bytes=spec.max_bytes,
    )
    if spec.duplicate_window > 0:
        config.duplicate_window = spec.duplicate_window
    return config


class PullConsumer:
    """Durable pull consumer. Explicit ack, five deliveries, then term."""

    def __init__(self, bus: MessageBus, stream: str, durable: str, subject: str, handler: Handler) -> None:
        self.bus = bus
        self.stream = stream
        self.durable = durable
        self.subject = subject
        self.handler = handler
        self.ready = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._stop = False

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> bool:
        if self.running:
            return True
        if not await self.bus.ensure_jetstream():
            return False
        self._stop = False
        self.ready = asyncio.Event()
        self._task = asyncio.create_task(self._run(), name=self.durable)
        waiter = asyncio.create_task(self.ready.wait())
        try:
            done, _pending = await asyncio.wait(
                {waiter, self._task},
                timeout=5,
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            waiter.cancel()
            self._task.cancel()
            raise
        if self._task in done and not self.ready.is_set():
            waiter.cancel()
            self._task.result()
            return False
        if waiter not in done:
            self._task.cancel()
            raise TimeoutError(f"{self.durable} did not subscribe")
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

    async def _subscribe(self):
        from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

        config = ConsumerConfig(
            durable_name=self.durable,
            ack_policy=AckPolicy.EXPLICIT,
            deliver_policy=DeliverPolicy.NEW,
            max_deliver=MAX_DELIVER,
            ack_wait=ACK_WAIT_SECONDS,
            filter_subject=self.subject,
        )
        return await self.bus.js.pull_subscribe(
            self.subject,
            durable=self.durable,
            stream=self.stream,
            config=config,
        )

    async def _run(self) -> None:
        subscription = await self._subscribe()
        self.ready.set()
        while not self._stop:
            try:
                messages = await subscription.fetch(1, timeout=FETCH_TIMEOUT)
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                continue
            for message in messages or []:
                if self._stop:
                    break
                await self._take(message)

    async def _take(self, message) -> None:
        try:
            await self.handler(message.subject, message.data)
        except asyncio.CancelledError:
            raise
        except PoisonMessage as exc:
            await self.bus.publish_dlq(reason=exc.reason, payload=message.data, subject=message.subject)
            await _settle(message.term)
            return
        except Exception as exc:
            logger.warning(f"[bus] {self.durable} {message.subject} failed: {exc}")
            delivered = _deliveries(message)
            if delivered >= MAX_DELIVER:
                await self.bus.publish_dlq(reason=str(exc), payload=message.data, subject=message.subject)
                await _settle(message.term)
            else:
                await _settle(lambda: message.nak(delay=1))
            return
        await _settle(message.ack)


def _deliveries(message) -> int:
    try:
        return int(message.metadata.num_delivered)
    except Exception:
        return 1


async def _settle(call) -> None:
    try:
        await call()
    except Exception as exc:
        logger.warning(f"[bus] ack settle failed: {exc}")


_bus: MessageBus | None = None


def get_bus() -> MessageBus:
    global _bus
    mode = current_mode()
    if _bus is None or _bus.mode != mode:
        previous = _bus
        _bus = MessageBus(mode)
        if previous is not None:
            previous.detach()
    return _bus


def reset_bus() -> None:
    global _bus
    if _bus is not None:
        _bus.detach()
    _bus = None
