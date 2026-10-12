"""A failed Redis ping backs off, then a later ping can succeed."""

import time

from src.operator_app import live_feed


def _reset() -> None:
    live_feed._redis = None
    live_feed._redis_down = False
    live_feed._redis_retry_at = 0.0
    live_feed._redis_backoff = 0.5
    live_feed._overlay_loaded = False
    for kind in live_feed._overlay:
        live_feed._overlay[kind].clear()


def test_redis_ping_retries_after_backoff(monkeypatch):
    _reset()

    class _Down:
        def __init__(self):
            self._url = "redis://127.0.0.1:6379/0"

        @property
        def client(self):
            return self

        def ping(self):
            raise RuntimeError("redis down")

    class _Up:
        def __init__(self):
            self._url = "redis://127.0.0.1:6379/0"
            self.client = self

        def ping(self):
            return True

    monkeypatch.setattr(live_feed, "RedisLiveStore", _Down)
    try:
        assert live_feed.live_redis() is None
        assert live_feed._redis_down is True
        retry_at = live_feed._redis_retry_at
        assert retry_at > time.monotonic()
        assert live_feed.live_redis() is None
        live_feed._redis_retry_at = 0.0
        monkeypatch.setattr(live_feed, "RedisLiveStore", _Up)
        store = live_feed.live_redis()
        assert store is not None
        assert live_feed._redis_down is False
    finally:
        _reset()
