"""Kernel tables for Postgres, PostGIS, and TimescaleDB.

Revision ID: 0001_kernel_tables
Revises:
Create Date: 2026-10-12

One table per kernel model. Telemetry is the only hypertable. The app role
may insert and read events and operator_actions, and may not update or delete
them. Passwords stay in .env; this file quotes them at upgrade time.
"""

from __future__ import annotations

import re

from alembic import op
from sqlalchemy import text

revision = "0001_kernel_tables"
down_revision = None
branch_labels = None
depends_on = None

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_TABLES = (
    """
    CREATE TABLE IF NOT EXISTS assets (
        asset_id      text PRIMARY KEY,
        name          text NOT NULL,
        asset_type    text NOT NULL,
        capabilities  jsonb NOT NULL DEFAULT '[]',
        max_speed     double precision,
        created_at    timestamptz NOT NULL DEFAULT now(),
        position      geometry(PointZ, 4326)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS telemetry (
        asset_id         text NOT NULL,
        "time"           timestamptz NOT NULL,
        lat              double precision NOT NULL,
        lon              double precision NOT NULL,
        alt              double precision NOT NULL,
        heading          double precision NOT NULL,
        speed            double precision NOT NULL,
        battery_percent  double precision NOT NULL,
        status           text NOT NULL,
        mode             text NOT NULL,
        position         geometry(PointZ, 4326) NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS commands (
        command_id    text PRIMARY KEY,
        asset_id      text NOT NULL,
        command_type  text NOT NULL,
        parameters    jsonb NOT NULL DEFAULT '{}',
        issued_by     text NOT NULL,
        issued_at     timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        event_id    text PRIMARY KEY,
        "time"      timestamptz NOT NULL,
        event_type  text NOT NULL,
        asset_id    text,
        message     text NOT NULL,
        severity    text NOT NULL DEFAULT 'info',
        data        jsonb NOT NULL DEFAULT '{}'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS operator_actions (
        id          text PRIMARY KEY,
        "time"      timestamptz NOT NULL,
        actor       text NOT NULL,
        asset_id    text NOT NULL,
        action      text NOT NULL,
        command_id  text NOT NULL,
        payload     jsonb NOT NULL DEFAULT '{}'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS missions (
        mission_id      text PRIMARY KEY,
        name            text NOT NULL,
        status          text NOT NULL,
        detection_lat   double precision,
        detection_lon   double precision,
        assigned_uavs   jsonb NOT NULL DEFAULT '[]',
        assigned_ugv    text,
        created_at      timestamptz NOT NULL DEFAULT now(),
        completed_at    timestamptz,
        detection       geometry(Point, 4326)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tracks (
        track_id    text PRIMARY KEY,
        updated_at  timestamptz NOT NULL,
        asset_id    text,
        lat         double precision,
        lon         double precision,
        label       text NOT NULL DEFAULT 'unknown',
        status      text NOT NULL DEFAULT '',
        confidence  double precision,
        payload     jsonb NOT NULL DEFAULT '{}',
        position    geometry(Point, 4326)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS detections (
        detection_id  text PRIMARY KEY,
        "time"        timestamptz NOT NULL,
        asset_id      text NOT NULL,
        lat           double precision NOT NULL,
        lon           double precision NOT NULL,
        label         text NOT NULL DEFAULT 'unknown',
        confidence    double precision NOT NULL DEFAULT 0,
        mission_id    text,
        position      geometry(Point, 4326) NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS alerts (
        alert_id   text PRIMARY KEY,
        "time"     timestamptz NOT NULL,
        asset_id   text,
        level      text NOT NULL DEFAULT 'info',
        message    text NOT NULL DEFAULT '',
        payload    jsonb NOT NULL DEFAULT '{}'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS zones (
        id       text PRIMARY KEY,
        name     text NOT NULL DEFAULT '',
        kind     text NOT NULL DEFAULT 'aoi',
        polygon  jsonb NOT NULL DEFAULT '[]',
        geom     geometry(Polygon, 4326)
    )
    """,
)

_INDEXES = (
    "CREATE INDEX IF NOT EXISTS assets_created_at_idx ON assets (created_at)",
    "CREATE INDEX IF NOT EXISTS assets_position_gix ON assets USING GIST (position)",
    "CREATE INDEX IF NOT EXISTS telemetry_asset_id_idx ON telemetry (asset_id)",
    'CREATE INDEX IF NOT EXISTS telemetry_time_idx ON telemetry ("time")',
    "CREATE INDEX IF NOT EXISTS telemetry_position_gix ON telemetry USING GIST (position)",
    "CREATE INDEX IF NOT EXISTS commands_asset_id_idx ON commands (asset_id)",
    "CREATE INDEX IF NOT EXISTS commands_issued_at_idx ON commands (issued_at)",
    "CREATE INDEX IF NOT EXISTS events_asset_id_idx ON events (asset_id)",
    'CREATE INDEX IF NOT EXISTS events_time_idx ON events ("time")',
    "CREATE INDEX IF NOT EXISTS operator_actions_asset_id_idx ON operator_actions (asset_id)",
    'CREATE INDEX IF NOT EXISTS operator_actions_time_idx ON operator_actions ("time")',
    "CREATE INDEX IF NOT EXISTS missions_created_at_idx ON missions (created_at)",
    "CREATE INDEX IF NOT EXISTS missions_completed_at_idx ON missions (completed_at)",
    "CREATE INDEX IF NOT EXISTS missions_detection_gix ON missions USING GIST (detection)",
    "CREATE INDEX IF NOT EXISTS tracks_asset_id_idx ON tracks (asset_id)",
    "CREATE INDEX IF NOT EXISTS tracks_updated_at_idx ON tracks (updated_at)",
    "CREATE INDEX IF NOT EXISTS tracks_position_gix ON tracks USING GIST (position)",
    "CREATE INDEX IF NOT EXISTS detections_asset_id_idx ON detections (asset_id)",
    'CREATE INDEX IF NOT EXISTS detections_time_idx ON detections ("time")',
    "CREATE INDEX IF NOT EXISTS detections_position_gix ON detections USING GIST (position)",
    "CREATE INDEX IF NOT EXISTS alerts_asset_id_idx ON alerts (asset_id)",
    'CREATE INDEX IF NOT EXISTS alerts_time_idx ON alerts ("time")',
    "CREATE INDEX IF NOT EXISTS zones_geom_gix ON zones USING GIST (geom)",
)

_MUTABLE = (
    "assets, telemetry, commands, missions, tracks, detections, alerts, zones"
)
_APPEND_ONLY = "events, operator_actions"


def _ident(name: str) -> str:
    if not _IDENT.match(name):
        raise RuntimeError(f"unsafe SQL identifier: {name!r}")
    return name


def _app_role() -> str:
    from src.data_services.settings import setting

    role = _ident(setting("POSTGRES_APP_USER", "cnc_app"))
    admin = _ident(setting("POSTGRES_USER", "cnc"))
    if role == admin:
        raise RuntimeError("POSTGRES_APP_USER must be a different role from POSTGRES_USER")
    return role


def _ensure_app_role() -> None:
    from src.data_services.settings import setting

    password = setting("POSTGRES_APP_PASSWORD")
    if not password:
        raise RuntimeError("Set POSTGRES_APP_PASSWORD in .env before alembic upgrade")
    role = _app_role()
    bind = op.get_bind()
    quoted = bind.execute(
        text("SELECT quote_literal(:password)"),
        {"password": password},
    ).scalar_one()
    bind.execute(text(f"""
        DO $cnc_role$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOINHERIT PASSWORD {quoted};
            ELSE
                ALTER ROLE {role} WITH LOGIN NOSUPERUSER NOCREATEDB NOINHERIT PASSWORD {quoted};
            END IF;
        END
        $cnc_role$;
    """))
    grant = bind.execute(
        text("""
            SELECT format(
                'GRANT CONNECT ON DATABASE %I TO %I',
                current_database(),
                CAST(:role AS text)
            )
        """),
        {"role": role},
    ).scalar_one()
    bind.execute(text(grant))
    bind.execute(text(f"GRANT USAGE ON SCHEMA public TO {role}"))


def _grant() -> None:
    role = _app_role()
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {_MUTABLE} TO {role}")
    op.execute(f"GRANT SELECT, INSERT ON {_APPEND_ONLY} TO {role}")
    op.execute(f"REVOKE UPDATE, DELETE ON {_APPEND_ONLY} FROM {role}")
    op.execute(f"REVOKE UPDATE, DELETE ON {_APPEND_ONLY} FROM PUBLIC")


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
    _ensure_app_role()
    for statement in _TABLES:
        op.execute(statement)
    for statement in _INDEXES:
        op.execute(statement)
    with op.get_context().autocommit_block():
        op.execute(
            "SELECT create_hypertable("
            "'telemetry', 'time', "
            "chunk_time_interval => INTERVAL '1 day', "
            "if_not_exists => TRUE)"
        )
    _grant()


def downgrade() -> None:
    role = _app_role()
    for name in (
        "zones",
        "alerts",
        "detections",
        "tracks",
        "missions",
        "operator_actions",
        "events",
        "commands",
        "telemetry",
        "assets",
    ):
        op.execute(f"DROP TABLE IF EXISTS {name} CASCADE")
    op.execute(text(f"""
        DO $body$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                DROP ROLE {role};
            END IF;
        END
        $body$;
    """))
