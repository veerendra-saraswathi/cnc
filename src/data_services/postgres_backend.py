"""Postgres repository backend. SQLAlchemy 2 engine, psycopg 3 driver.

Constructing PostgresRepositories does not open a connection. The engine
is created on the first call. The app role is cnc_app; events and
operator_actions are insert-only for that role.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine

from src.data_services.ids import uuid7
from src.data_services.outbox_record import OutboxRecord, as_payload, issued_parts, utcnow
from src.data_services.settings import setting
from src.kernel.models import OperatorAction, Zone

_TABLES = (
    "assets",
    "telemetry",
    "commands",
    "events",
    "operator_actions",
    "missions",
    "tracks",
    "detections",
    "alerts",
    "zones",
    "outbox",
    "operator_snapshot",
)

_app_engine: Engine | None = None
_admin_engine: Engine | None = None


def _url(user: str, password: str) -> URL:
    return URL.create(
        "postgresql+psycopg",
        username=user,
        password=password,
        host=setting("POSTGRES_HOST", "127.0.0.1"),
        port=int(setting("POSTGRES_PORT", "5432")),
        database=setting("POSTGRES_DB", "cnc"),
    )


def admin_url() -> URL:
    """Migration user from POSTGRES_USER. Not the application role."""
    return _url(setting("POSTGRES_USER", "cnc"), setting("POSTGRES_PASSWORD"))


def app_url() -> URL:
    return _url(setting("POSTGRES_APP_USER", "cnc_app"), setting("POSTGRES_APP_PASSWORD"))


def _engine(url: URL) -> Engine:
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 3})


def app_engine() -> Engine:
    global _app_engine
    if _app_engine is None:
        _app_engine = _engine(app_url())
    return _app_engine


def admin_engine() -> Engine:
    global _admin_engine
    if _admin_engine is None:
        _admin_engine = _engine(admin_url())
    return _admin_engine


def truncate_kernel_tables() -> None:
    names = ", ".join(_TABLES)
    with admin_engine().begin() as conn:
        conn.execute(text(f"TRUNCATE {names} RESTART IDENTITY"))


def _aware(value: datetime | None, *, now: bool = False) -> datetime | None:
    if value is None:
        if not now:
            return None
        value = datetime.now(timezone.utc)
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("time must be timezone-aware UTC")
    return value.astimezone(timezone.utc)


def _json(value) -> str:
    if value is None:
        value = {}
    return json.dumps(value, default=str)


def _loads(value):
    if isinstance(value, str):
        return json.loads(value)
    return value


class _PgConn:
    def __init__(self, owner: PostgresRepositories):
        self._owner = owner

    def _run(self, sql: str, params: dict) -> None:
        with self._owner.engine.begin() as conn:
            conn.execute(text(sql), params)


class _PgAssets(_PgConn):
    def upsert(self, asset_id: str, name: str, asset_type: str,
               capabilities: list = None, max_speed: float = None) -> None:
        self._run(
            """
            INSERT INTO assets
                (asset_id, name, asset_type, capabilities, max_speed, created_at)
            VALUES
                (:asset_id, :name, :asset_type, CAST(:capabilities AS jsonb),
                 :max_speed, :created_at)
            ON CONFLICT (asset_id) DO UPDATE SET
                name = EXCLUDED.name,
                asset_type = EXCLUDED.asset_type,
                capabilities = EXCLUDED.capabilities,
                max_speed = EXCLUDED.max_speed,
                created_at = EXCLUDED.created_at
            """,
            {
                "asset_id": asset_id,
                "name": name,
                "asset_type": asset_type,
                "capabilities": json.dumps(capabilities or []),
                "max_speed": max_speed,
                "created_at": datetime.now(timezone.utc),
            },
        )


class _PgTelemetry(_PgConn):
    def insert(self, asset_id: str, timestamp: datetime,
               lat: float, lon: float, alt: float, heading: float,
               speed: float, battery: float, status: str, mode: str) -> None:
        self._run(
            """
            INSERT INTO telemetry (
                asset_id, "time", lat, lon, alt, heading, speed,
                battery_percent, status, mode, position
            ) VALUES (
                :asset_id, :recorded, :lat, :lon, :alt, :heading, :speed,
                :battery, :status, :mode,
                ST_SetSRID(ST_MakePoint(:lon, :lat, :alt), 4326)
            )
            """,
            {
                "asset_id": asset_id,
                "recorded": _aware(timestamp),
                "lat": lat,
                "lon": lon,
                "alt": alt,
                "heading": heading,
                "speed": speed,
                "battery": battery,
                "status": status,
                "mode": mode,
            },
        )


class _PgCommands(_PgConn):
    def insert(self, command_id: str, asset_id: str, command_type: str,
               parameters: dict, issued_by: str = "operator",
               issued_at: datetime = None) -> None:
        if not isinstance(parameters, dict):
            parameters = {"value": parameters}
        self._run(
            """
            INSERT INTO commands (
                command_id, asset_id, command_type, parameters, issued_by, issued_at
            ) VALUES (
                :command_id, :asset_id, :command_type, CAST(:parameters AS jsonb),
                :issued_by, :issued_at
            )
            ON CONFLICT (command_id) DO NOTHING
            """,
            {
                "command_id": command_id,
                "asset_id": asset_id,
                "command_type": command_type,
                "parameters": _json(parameters),
                "issued_by": issued_by,
                "issued_at": _aware(issued_at, now=True),
            },
        )


class _PgEvents(_PgConn):
    def insert(self, event_id: str, timestamp: datetime, event_type: str,
               message: str, asset_id: str = None,
               severity: str = "info", data: dict = None) -> None:
        self._run(
            """
            INSERT INTO events (
                event_id, "time", event_type, asset_id, message, severity, data
            ) VALUES (
                :event_id, :recorded, :event_type, :asset_id, :message, :severity,
                CAST(:data AS jsonb)
            )
            ON CONFLICT (event_id) DO NOTHING
            """,
            {
                "event_id": event_id,
                "recorded": _aware(timestamp),
                "event_type": event_type,
                "asset_id": asset_id,
                "message": message,
                "severity": severity,
                "data": _json(data or {}),
            },
        )


class _PgOperatorActions(_PgConn):
    def add(self, action: OperatorAction) -> None:
        self._run(
            """
            INSERT INTO operator_actions (
                id, "time", actor, asset_id, action, command_id, payload
            ) VALUES (
                :id, :recorded, :actor, :asset_id, :action, :command_id,
                CAST(:payload AS jsonb)
            )
            """,
            {
                "id": action.id,
                "recorded": _aware(action.time),
                "actor": action.actor,
                "asset_id": action.asset_id,
                "action": action.action,
                "command_id": action.command_id,
                "payload": _json(action.payload),
            },
        )

    def list(self) -> list[OperatorAction]:
        with self._owner.engine.connect() as conn:
            rows = conn.execute(text(
                """
                SELECT id, "time", actor, asset_id, action, command_id, payload
                FROM operator_actions
                ORDER BY "time", id
                """
            )).mappings()
            return [
                OperatorAction(
                    id=row["id"],
                    time=row["time"],
                    actor=row["actor"],
                    asset_id=row["asset_id"] or "",
                    action=row["action"],
                    command_id=row["command_id"] or "",
                    payload=_loads(row["payload"]) or {},
                )
                for row in rows
            ]


class _PgMissions(_PgConn):
    def upsert(self, mission_id: str, name: str, status: str,
               detection_lat: float = None, detection_lon: float = None,
               assigned_uavs: list = None, assigned_ugv: str = None,
               completed_at: datetime = None) -> None:
        self._run(
            """
            INSERT INTO missions (
                mission_id, name, status, detection_lat, detection_lon,
                assigned_uavs, assigned_ugv, created_at, completed_at, detection
            ) VALUES (
                :mission_id, :name, :status, :detection_lat, :detection_lon,
                CAST(:assigned_uavs AS jsonb), :assigned_ugv, :created_at, :completed_at,
                CASE
                    WHEN CAST(:detection_lat AS double precision) IS NULL
                      OR CAST(:detection_lon AS double precision) IS NULL THEN NULL
                    ELSE ST_SetSRID(ST_MakePoint(:detection_lon, :detection_lat), 4326)
                END
            )
            ON CONFLICT (mission_id) DO UPDATE SET
                name = EXCLUDED.name,
                status = EXCLUDED.status,
                detection_lat = EXCLUDED.detection_lat,
                detection_lon = EXCLUDED.detection_lon,
                assigned_uavs = EXCLUDED.assigned_uavs,
                assigned_ugv = EXCLUDED.assigned_ugv,
                created_at = EXCLUDED.created_at,
                completed_at = EXCLUDED.completed_at,
                detection = EXCLUDED.detection
            """,
            {
                "mission_id": mission_id,
                "name": name,
                "status": status,
                "detection_lat": detection_lat,
                "detection_lon": detection_lon,
                "assigned_uavs": json.dumps(assigned_uavs or []),
                "assigned_ugv": assigned_ugv,
                "created_at": datetime.now(timezone.utc),
                "completed_at": _aware(completed_at),
            },
        )


class _PgTracks(_PgConn):
    def upsert(self, track_id: str, updated_at: datetime, asset_id: str = None,
               lat: float = None, lon: float = None, label: str = "unknown",
               status: str = "", confidence: float = None, payload: dict = None) -> None:
        self._run(
            """
            INSERT INTO tracks (
                track_id, updated_at, asset_id, lat, lon, label, status,
                confidence, payload, position
            ) VALUES (
                :track_id, :updated_at, :asset_id, :lat, :lon, :label, :status,
                :confidence, CAST(:payload AS jsonb),
                CASE
                    WHEN CAST(:lat AS double precision) IS NULL
                      OR CAST(:lon AS double precision) IS NULL THEN NULL
                    ELSE ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
                END
            )
            ON CONFLICT (track_id) DO UPDATE SET
                updated_at = EXCLUDED.updated_at,
                asset_id = EXCLUDED.asset_id,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                label = EXCLUDED.label,
                status = EXCLUDED.status,
                confidence = EXCLUDED.confidence,
                payload = EXCLUDED.payload,
                position = EXCLUDED.position
            """,
            {
                "track_id": track_id,
                "updated_at": _aware(updated_at),
                "asset_id": asset_id,
                "lat": lat,
                "lon": lon,
                "label": label,
                "status": status,
                "confidence": confidence,
                "payload": _json(payload or {}),
            },
        )


class _PgDetections(_PgConn):
    def insert(self, detection_id: str, timestamp: datetime,
               asset_id: str, lat: float, lon: float,
               label: str = "unknown", confidence: float = 0.0,
               mission_id: str = None) -> None:
        self._run(
            """
            INSERT INTO detections (
                detection_id, "time", asset_id, lat, lon, label, confidence,
                mission_id, position
            ) VALUES (
                :detection_id, :recorded, :asset_id, :lat, :lon, :label, :confidence,
                :mission_id, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
            )
            ON CONFLICT (detection_id) DO NOTHING
            """,
            {
                "detection_id": detection_id,
                "recorded": _aware(timestamp),
                "asset_id": asset_id,
                "lat": lat,
                "lon": lon,
                "label": label,
                "confidence": confidence,
                "mission_id": mission_id,
            },
        )


class _PgAlerts(_PgConn):
    def insert(self, alert_id: str, timestamp: datetime, asset_id: str = None,
               level: str = "info", message: str = "", payload: dict = None) -> None:
        self._run(
            """
            INSERT INTO alerts (
                alert_id, "time", asset_id, level, message, payload
            ) VALUES (
                :alert_id, :recorded, :asset_id, :level, :message, CAST(:payload AS jsonb)
            )
            ON CONFLICT (alert_id) DO UPDATE SET
                "time" = EXCLUDED."time",
                asset_id = EXCLUDED.asset_id,
                level = EXCLUDED.level,
                message = EXCLUDED.message,
                payload = EXCLUDED.payload
            """,
            {
                "alert_id": alert_id,
                "recorded": _aware(timestamp),
                "asset_id": asset_id,
                "level": level,
                "message": message,
                "payload": _json(payload or {}),
            },
        )


def _polygon_wkt(points: list) -> str | None:
    coords: list[tuple[float, float]] = []
    for point in points or []:
        if not isinstance(point, dict) or "lat" not in point or "lon" not in point:
            return None
        coords.append((float(point["lon"]), float(point["lat"])))
    if len(coords) < 3:
        return None
    if coords[0] != coords[-1]:
        coords.append(coords[0])
    if len(coords) < 4:
        return None
    body = ", ".join(f"{lon} {lat}" for lon, lat in coords)
    return f"POLYGON(({body}))"


def _zone_row(row) -> Zone:
    return Zone(
        id=row["id"],
        name=row["name"] or "",
        kind=row["kind"] or "aoi",
        polygon=_loads(row["polygon"]) or [],
    )


class _PgZones(_PgConn):
    def add(self, zone: Zone) -> None:
        self._run(
            """
            INSERT INTO zones (id, name, kind, polygon, geom)
            VALUES (
                :id, :name, :kind, CAST(:polygon AS jsonb),
                CASE
                    WHEN CAST(:wkt AS text) IS NULL THEN NULL
                    ELSE ST_SetSRID(ST_GeomFromText(CAST(:wkt AS text)), 4326)
                END
            )
            """,
            {
                "id": zone.id,
                "name": zone.name,
                "kind": zone.kind,
                "polygon": json.dumps(zone.polygon),
                "wkt": _polygon_wkt(zone.polygon),
            },
        )

    def remove(self, zone_id: str) -> None:
        self._run("DELETE FROM zones WHERE id = :id", {"id": zone_id})

    def containing(self, lat: float, lon: float) -> list[Zone]:
        """Zones whose PostGIS polygon contains this lon/lat point."""
        with self._owner.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, name, kind, polygon
                FROM zones
                WHERE geom IS NOT NULL
                  AND ST_Contains(
                        geom,
                        ST_SetSRID(
                            ST_MakePoint(
                                CAST(:lon AS double precision),
                                CAST(:lat AS double precision)
                            ),
                            4326
                        )
                      )
                ORDER BY id
            """), {"lat": lat, "lon": lon}).mappings()
            return [_zone_row(row) for row in rows]

    def list(self) -> list[Zone]:
        with self._owner.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, name, kind, polygon FROM zones ORDER BY id"
            )).mappings()
            return [_zone_row(row) for row in rows]


class PostgresRepositories:
    """Kernel repositories on Postgres. The engine connects on first use."""

    backend = "postgres"

    def __init__(self, engine: Engine | None = None) -> None:
        self._engine = engine
        self.assets = _PgAssets(self)
        self.telemetry = _PgTelemetry(self)
        self.commands = _PgCommands(self)
        self.events = _PgEvents(self)
        self.operator_actions = _PgOperatorActions(self)
        self.missions = _PgMissions(self)
        self.tracks = _PgTracks(self)
        self.detections = _PgDetections(self)
        self.alerts = _PgAlerts(self)
        self.zones = _PgZones(self)

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = app_engine()
        return self._engine

    def commit_issued(self, command, event, subject: str, outbox_id: str | None = None) -> str:
        oid = outbox_id or uuid7()
        row = issued_parts(command, event, subject, oid, utcnow())
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO commands (
                        command_id, asset_id, command_type, parameters, issued_by, issued_at,
                        status, result
                    ) VALUES (
                        :command_id, :asset_id, :command_type, CAST(:parameters AS jsonb),
                        :issued_by, :issued_at, :status, CAST(:result AS jsonb)
                    )
                    """
                ),
                {
                    "command_id": row["command_id"],
                    "asset_id": row["asset_id"],
                    "command_type": row["command_type"],
                    "parameters": _json(row["parameters"]),
                    "issued_by": row["issued_by"],
                    "issued_at": _aware(row["issued_at"], now=True),
                    "status": row.get("status") or "pending",
                    "result": _json(row["result"]) if row.get("result") is not None else None,
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO events (
                        event_id, "time", event_type, asset_id, message, severity, data
                    ) VALUES (
                        :event_id, :recorded, :event_type, :asset_id, :message, :severity,
                        CAST(:data AS jsonb)
                    )
                    """
                ),
                {
                    "event_id": row["event_id"],
                    "recorded": _aware(row["event_time"]),
                    "event_type": row["event_type"],
                    "asset_id": row["event_asset_id"],
                    "message": row["message"],
                    "severity": row["severity"],
                    "data": _json(row["data"]),
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO outbox (
                        id, subject, payload, created_at, published_at, attempts
                    ) VALUES (
                        CAST(:id AS uuid), :subject, CAST(:payload AS jsonb), :created_at, NULL, 0
                    )
                    """
                ),
                {
                    "id": row["outbox_id"],
                    "subject": row["subject"],
                    "payload": _json(row["payload"]),
                    "created_at": _aware(row["created_at"], now=True),
                },
            )
        return oid

    def list_unpublished(self) -> list[OutboxRecord]:
        with self.engine.connect() as conn:
            rows = conn.execute(text(
                """
                SELECT id::text, subject, payload, created_at, published_at, attempts
                FROM outbox
                WHERE published_at IS NULL
                ORDER BY created_at, id
                """
            )).all()
        return [
            OutboxRecord(
                id=str(row[0]),
                subject=str(row[1]),
                payload=as_payload(row[2]),
                created_at=row[3],
                published_at=row[4],
                attempts=int(row[5] or 0),
            )
            for row in rows
        ]

    def mark_published(self, outbox_id: str, when) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE outbox SET published_at = :when
                    WHERE id = CAST(:id AS uuid) AND published_at IS NULL
                    """
                ),
                {"id": outbox_id, "when": _aware(when, now=True)},
            )

    def note_attempt(self, outbox_id: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE outbox SET attempts = attempts + 1
                    WHERE id = CAST(:id AS uuid) AND published_at IS NULL
                    """
                ),
                {"id": outbox_id},
            )

    def mark_published_command(self, command_id: str, when) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE outbox SET published_at = :when
                    WHERE published_at IS NULL AND payload->>'id' = :command_id
                    """
                ),
                {"command_id": command_id, "when": _aware(when, now=True)},
            )

    def command_published(self, command_id: str) -> bool:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT published_at FROM outbox
                    WHERE payload->>'id' = :command_id
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"command_id": command_id},
            ).first()
        return row is not None and row[0] is not None

    def update_command_result(self, command_id: str, status: str, result) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE commands
                    SET status = :status, result = CAST(:result AS jsonb)
                    WHERE command_id = :command_id
                    """
                ),
                {
                    "command_id": command_id,
                    "status": status,
                    "result": _json(result) if result is not None else None,
                },
            )

    def command_event_recorded(self, command_id: str) -> bool:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT 1 FROM events
                    WHERE data->>'command_id' = :command_id
                    LIMIT 1
                    """
                ),
                {"command_id": command_id},
            ).first()
        return row is not None

    def list_recent_commands(self, limit: int = 15) -> list[dict]:
        from src.data_services.outbox_record import aware_utc

        limit = max(1, min(int(limit), 50))
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT command_id, asset_id, command_type, parameters, issued_by,
                           issued_at, status, result
                    FROM commands
                    ORDER BY issued_at DESC
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            ).all()
        found = []
        for row in rows:
            parameters = _loads(row[3]) if row[3] is not None else {}
            result = _loads(row[7]) if row[7] is not None else None
            found.append({
                "id": row[0],
                "asset_id": row[1],
                "command_type": row[2],
                "parameters": parameters if isinstance(parameters, dict) else {},
                "issued_by": row[4] or "operator",
                "issued_at": aware_utc(row[5]),
                "status": row[6] or "pending",
                "result": result if isinstance(result, dict) else None,
            })
        return found

    def list_recent_events(self, limit: int = 500) -> list[dict]:
        from src.data_services.outbox_record import aware_utc

        limit = max(1, min(int(limit), 500))
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT event_id, "time", event_type, asset_id, message, severity, data
                    FROM events
                    ORDER BY "time" DESC
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            ).all()
        found = []
        for row in rows:
            data = _loads(row[6]) if row[6] is not None else {}
            if not isinstance(data, dict):
                data = {}
            actor = str(data.get("operator_id") or data.get("actor") or "system")
            found.append({
                "event_id": row[0],
                "timestamp": aware_utc(row[1]),
                "event_type": row[2],
                "asset_id": row[3] or "",
                "message": row[4],
                "severity": row[5] or "info",
                "data": data,
                "actor": actor,
            })
        return found

    def dispose(self) -> None:
        global _app_engine
        engine = self._engine
        self._engine = None
        if engine is None:
            return
        engine.dispose()
        if _app_engine is engine:
            _app_engine = None
