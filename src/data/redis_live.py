"""Redis hot store for platform state, tracks, detections, alerts, and commands."""

from __future__ import annotations

import json
import os
from urllib.parse import quote

import redis

from src.data_services.settings import setting


def resolve_redis_url() -> str:
    """CNC_REDIS_URL wins. Otherwise REDIS_PASSWORD is the cnc-redis requirepass."""
    configured = setting("CNC_REDIS_URL")
    if configured:
        return configured
    password = setting("REDIS_PASSWORD")
    if password:
        return f"redis://:{quote(password, safe='')}@localhost:6379/0"
    return "redis://localhost:6379/0"


REDIS_URL = os.environ.get("CNC_REDIS_URL", "redis://localhost:6379/0")
HOT_TTL_S = 3600
STREAM_MAXLEN = 200

# Platform keeps the existing channel name. Other kinds use cnc:{kind}.
CHANNELS = {
    "platform": "cnc:telemetry",
    "track": "cnc:track",
    "detection": "cnc:detection",
    "alert": "cnc:alert",
    "command": "cnc:command",
}


def channel_for(kind: str) -> str:
    return CHANNELS.get(kind, f"cnc:{kind}")


def _decode_field(value: str):
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


class RedisLiveStore:
    def __init__(self, client=None, url: str | None = None):
        self._client = client
        self._url = resolve_redis_url() if url is None else url

    @property
    def client(self):
        if self._client is None:
            self._client = redis.Redis.from_url(
                self._url,
                decode_responses=True,
                socket_connect_timeout=0.5,
                socket_timeout=0.5,
            )
        return self._client

    def put(self, kind: str, object_id: str, payload: dict) -> None:
        key = f"cnc:{kind}:{object_id}"
        self.client.hset(key, mapping={"body": json.dumps(payload, default=str)})
        self.client.expire(key, HOT_TTL_S)
        self.publish(kind, payload)

    def publish(self, kind: str, payload: dict) -> None:
        raw = json.dumps(payload, default=str)
        self.client.xadd(
            f"cnc:stream:{kind}",
            {"body": raw},
            maxlen=STREAM_MAXLEN,
            approximate=True,
        )
        self.client.publish(channel_for(kind), raw)

    def get(self, kind: str, object_id: str) -> dict:
        raw = self.client.hgetall(f"cnc:{kind}:{object_id}")
        if not raw:
            return {}
        if "body" in raw:
            body = raw["body"]
            if isinstance(body, str):
                try:
                    parsed = json.loads(body)
                except json.JSONDecodeError:
                    return {"body": body}
                if isinstance(parsed, dict):
                    return parsed
            return {"body": body}
        return {key: _decode_field(value) for key, value in raw.items()}

    def read_recent(self, kind: str, limit: int = 50) -> list[dict]:
        rows = self.client.xrevrange(f"cnc:stream:{kind}", count=limit)
        recent = []
        for entry_id, fields in rows:
            body = fields.get("body")
            if body:
                try:
                    item = json.loads(body)
                except json.JSONDecodeError:
                    item = {"body": body}
            else:
                item = dict(fields)
            if isinstance(item, dict):
                item["_stream_id"] = entry_id
                recent.append(item)
        return recent

    def list_ids(self, kind: str) -> list[str]:
        prefix = f"cnc:{kind}:"
        keys = self.client.keys(f"{prefix}*")
        return [key[len(prefix):] if str(key).startswith(prefix) else str(key).split(":")[-1] for key in keys]
