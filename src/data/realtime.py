"""Compatibility wrappers around the Redis live store."""

from datetime import datetime, timezone

from src.data.ingest import get_live


def set_platform_state(platform_id: str, state: dict):
    """Live COP / platform state snapshot."""
    record = dict(state)
    record["updated_at"] = datetime.now(timezone.utc).isoformat()
    get_live().put("platform", platform_id, record)


def get_platform_state(platform_id: str) -> dict:
    return get_live().get("platform", platform_id)


def list_platforms() -> list[str]:
    return get_live().list_ids("platform")
