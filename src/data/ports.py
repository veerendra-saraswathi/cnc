"""Storage ports for the phase-1 data layer.

Callers pass plain dicts. Redis and DuckDB stay behind these protocols.
"""

from typing import Protocol


class LiveStore(Protocol):
    """Hot state: latest object, a short replay stream, and a wake-up publish."""

    def put(self, kind: str, object_id: str, payload: dict) -> None: ...

    def get(self, kind: str, object_id: str) -> dict: ...

    def publish(self, kind: str, payload: dict) -> None: ...

    def read_recent(self, kind: str, limit: int = 50) -> list[dict]: ...

    def list_ids(self, kind: str) -> list[str]: ...


class DurableStore(Protocol):
    """Durable rows. Telemetry stays on the Parquet lake; the rest go to DuckDB."""

    def save_telemetry(self, record: dict) -> None: ...

    def save_track(self, track: dict) -> None: ...

    def save_detection(self, detection: dict) -> None: ...

    def save_alert(self, alert: dict) -> None: ...

    def save_command(self, command: dict) -> None: ...
