"""The same repository cases on DuckDB and Postgres.

Postgres cases are marked and skipped when nothing answers on the configured
host, so a laptop without Docker still passes the default suite.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from src.data_services.db import CNCDatabase
from src.data_services.repositories import duckdb_repositories
from src.kernel.models import OperatorAction, Zone

WHEN = datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc)
_PROBE = {"done": False, "error": None}


def _postgres_error() -> str | None:
    if _PROBE["done"]:
        return _PROBE["error"]
    _PROBE["done"] = True
    try:
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool

        from src.data_services.postgres_backend import admin_url

        engine = create_engine(
            admin_url(),
            poolclass=NullPool,
            connect_args={"connect_timeout": 2},
        )
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        finally:
            engine.dispose()
    except Exception as exc:
        _PROBE["error"] = f"{exc.__class__.__name__}: {str(exc).splitlines()[0]}"
    return _PROBE["error"]


def _require_postgres(request) -> None:
    error = _postgres_error()
    if error is None:
        return
    pytest.skip(f"Postgres is not reachable: {error}")


@pytest.fixture(params=[
    "duckdb",
    pytest.param("postgres", marks=pytest.mark.postgres),
])
def repos(request, tmp_path):
    if request.param == "duckdb":
        database = CNCDatabase(tmp_path / "repo.duckdb")
        try:
            yield duckdb_repositories(database)
        finally:
            database.close()
        return
    _require_postgres(request)
    from src.data_services.postgres_backend import PostgresRepositories, truncate_kernel_tables

    truncate_kernel_tables()
    bundle = PostgresRepositories()
    try:
        yield bundle
    finally:
        bundle.dispose()


@pytest.fixture
def postgres_repos(request):
    _require_postgres(request)
    from src.data_services.postgres_backend import PostgresRepositories, truncate_kernel_tables

    truncate_kernel_tables()
    bundle = PostgresRepositories()
    try:
        yield bundle
    finally:
        bundle.dispose()


@pytest.fixture
def admin(request):
    _require_postgres(request)
    from src.data_services.postgres_backend import admin_engine

    yield admin_engine()


def _count(repos, table: str, column: str, value) -> int:
    if repos.backend == "duckdb":
        row = repos.database.con.execute(
            f"SELECT count(*) FROM {table} WHERE {column} = ?",
            [value],
        ).fetchone()
        return int(row[0])
    with repos.engine.connect() as conn:
        return int(conn.execute(
            text(f"SELECT count(*) FROM {table} WHERE {column} = :value"),
            {"value": value},
        ).scalar_one())


def _cell(repos, table: str, key_col: str, key, column: str):
    if repos.backend == "duckdb":
        row = repos.database.con.execute(
            f"SELECT {column} FROM {table} WHERE {key_col} = ?",
            [key],
        ).fetchone()
    else:
        with repos.engine.connect() as conn:
            row = conn.execute(
                text(f"SELECT {column} FROM {table} WHERE {key_col} = :key"),
                {"key": key},
            ).first()
    assert row is not None
    value = row[0]
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _lats(repos, asset_id: str) -> list[float]:
    if repos.backend == "duckdb":
        rows = repos.database.con.execute(
            "SELECT lat FROM telemetry WHERE asset_id = ? ORDER BY timestamp",
            [asset_id],
        ).fetchall()
    else:
        with repos.engine.connect() as conn:
            rows = conn.execute(
                text('SELECT lat FROM telemetry WHERE asset_id = :id ORDER BY "time"'),
                {"id": asset_id},
            ).fetchall()
    return [float(row[0]) for row in rows]


def test_asset_upsert_replaces_the_row(repos):
    repos.assets.upsert("uav-01", "Eagle-1", "uav", ["eo"], 18.0)
    repos.assets.upsert("uav-01", "Eagle-1b", "uav", ["eo", "ir"], 20.0)
    assert _count(repos, "assets", "asset_id", "uav-01") == 1
    assert _cell(repos, "assets", "asset_id", "uav-01", "name") == "Eagle-1b"
    assert float(_cell(repos, "assets", "asset_id", "uav-01", "max_speed")) == 20.0
    assert _cell(repos, "assets", "asset_id", "uav-01", "capabilities") == ["eo", "ir"]


def test_telemetry_inserts_each_sample(repos):
    repos.telemetry.insert(
        "uav-01", WHEN, 17.4, 78.5, 100.0, 90.0, 12.0, 80.0, "mission", "auto",
    )
    repos.telemetry.insert(
        "uav-01", WHEN + timedelta(minutes=1), 17.41, 78.51, 110.0, 91.0, 13.0, 79.0,
        "mission", "auto",
    )
    assert _lats(repos, "uav-01") == pytest.approx([17.4, 17.41])


def test_commit_issued_rolls_back_command_and_event_together(repos):
    from uuid import UUID, uuid4

    from src.data_services.ids import uuid7
    from src.kernel.models import Command, CommandType, Event, EventType

    asset_id = f"outbox-{uuid4().hex[:8]}"
    outbox_id = uuid7()
    assert UUID(outbox_id).version == 7
    first = Command(
        asset_id=asset_id,
        command_type=CommandType.HOLD,
        issued_by="operator-1",
        seq=1,
    )
    first_event = Event(
        event_type=EventType.COMMAND,
        message="hold (pending)",
        asset_id=asset_id,
        actor="operator-1",
        data={"command_id": first.id},
    )
    stored = repos.commit_issued(first, first_event, f"cnc.command.{asset_id}", outbox_id=outbox_id)
    assert stored == outbox_id
    assert _count(repos, "commands", "command_id", first.id) == 1
    assert _count(repos, "events", "event_id", first_event.event_id) == 1
    assert _outbox_count(repos, outbox_id) == 1

    second = Command(
        asset_id=asset_id,
        command_type=CommandType.GOTO,
        issued_by="operator-1",
        seq=2,
        parameters={"lat": 1.0, "lon": 2.0},
    )
    second_event = Event(
        event_type=EventType.COMMAND,
        message="goto (pending)",
        asset_id=asset_id,
        actor="operator-1",
        data={"command_id": second.id},
    )
    with pytest.raises(Exception):
        repos.commit_issued(second, second_event, f"cnc.command.{asset_id}", outbox_id=outbox_id)
    assert _count(repos, "commands", "command_id", second.id) == 0
    assert _count(repos, "events", "event_id", second_event.event_id) == 0
    assert _count(repos, "commands", "command_id", first.id) == 1


def _outbox_count(repos, outbox_id: str) -> int:
    if repos.backend == "duckdb":
        return _count(repos, "outbox", "id", outbox_id)
    with repos.engine.connect() as conn:
        return int(conn.execute(
            text("SELECT count(*) FROM outbox WHERE id = CAST(:id AS uuid)"),
            {"id": outbox_id},
        ).scalar_one())


def test_recent_command_keeps_status_and_finds_its_event(repos):
    from src.kernel.models import Command, CommandType, Event, EventType

    command = Command(
        asset_id="uav-01",
        command_type=CommandType.HOLD,
        issued_by="operator-1",
        seq=4,
        status="pending",
    )
    event = Event(
        event_type=EventType.COMMAND,
        message="hold (pending)",
        asset_id="uav-01",
        actor="operator-1",
        data={"command_id": command.id, "operator_id": "operator-1"},
    )
    repos.commit_issued(command, event, "cnc.command.uav-01")
    assert repos.command_event_recorded(command.id) is True
    assert repos.command_event_recorded("missing-command") is False
    repos.update_command_result(command.id, "sent", {"ack": True})
    recent = repos.list_recent_commands(5)
    match = next(row for row in recent if row["id"] == command.id)
    assert match["status"] == "sent"
    assert match["result"] == {"ack": True}
    assert match["issued_by"] == "operator-1"
    assert match["issued_at"].tzinfo is not None
    events = repos.list_recent_events(20)
    recorded = next(row for row in events if row["event_id"] == event.event_id)
    assert recorded["actor"] == "operator-1"
    assert recorded["data"]["command_id"] == command.id


def test_command_insert_ignores_a_duplicate_id(repos):
    repos.commands.insert("c1", "uav-01", "goto", {"alt": 1}, "operator-1", WHEN)
    repos.commands.insert("c1", "uav-01", "hold", {"alt": 2}, "operator-1", WHEN)
    assert _count(repos, "commands", "command_id", "c1") == 1
    assert _cell(repos, "commands", "command_id", "c1", "parameters") == {"alt": 1}
    assert _cell(repos, "commands", "command_id", "c1", "command_type") == "goto"


def test_event_insert_ignores_a_duplicate_and_keeps_a_null_asset(repos):
    repos.events.insert("e1", WHEN, "command", "go", None, "info", {"command_id": "c1"})
    repos.events.insert("e1", WHEN, "command", "other", "uav-01", "warning", {})
    assert _count(repos, "events", "event_id", "e1") == 1
    assert _cell(repos, "events", "event_id", "e1", "message") == "go"
    assert _cell(repos, "events", "event_id", "e1", "asset_id") is None


def test_operator_action_round_trip(repos):
    repos.operator_actions.add(OperatorAction(
        id="oa1",
        time=WHEN,
        actor="operator-1",
        asset_id="uav-01",
        action="goto",
        command_id="c1",
        payload={"alt": 40},
    ))
    found = repos.operator_actions.list()
    assert len(found) == 1
    action = found[0]
    assert action.id == "oa1"
    assert action.actor == "operator-1"
    assert action.asset_id == "uav-01"
    assert action.action == "goto"
    assert action.command_id == "c1"
    assert action.payload == {"alt": 40}
    assert action.time == WHEN


def test_mission_upsert_replaces_status(repos):
    repos.missions.upsert("m1", "recon", "executing", 17.4, 78.5, ["uav-01"], "ugv-01", None)
    repos.missions.upsert(
        "m1", "recon", "completed", 17.4, 78.5, ["uav-01"], "ugv-01", WHEN,
    )
    assert _count(repos, "missions", "mission_id", "m1") == 1
    assert _cell(repos, "missions", "mission_id", "m1", "status") == "completed"
    assert float(_cell(repos, "missions", "mission_id", "m1", "detection_lat")) == pytest.approx(17.4)


def test_track_upsert_replaces_label_state(repos):
    repos.tracks.upsert("t1", WHEN, "uav-01", 17.4, 78.5, "vehicle", "tentative", 0.4, {"hits": 1})
    repos.tracks.upsert(
        "t1", WHEN + timedelta(seconds=5), "uav-01", 17.42, 78.52,
        "vehicle", "confirmed", 0.9, {"hits": 2},
    )
    assert _count(repos, "tracks", "track_id", "t1") == 1
    assert _cell(repos, "tracks", "track_id", "t1", "status") == "confirmed"
    assert _cell(repos, "tracks", "track_id", "t1", "payload") == {"hits": 2}


def test_detection_insert_ignores_a_duplicate_id(repos):
    repos.detections.insert("d1", WHEN, "uav-01", 17.4, 78.5, "vehicle", 0.8, "m1")
    repos.detections.insert("d1", WHEN, "uav-01", 18.0, 79.0, "person", 0.1, "m1")
    assert _count(repos, "detections", "detection_id", "d1") == 1
    assert _cell(repos, "detections", "detection_id", "d1", "label") == "vehicle"


def test_alert_insert_replaces_the_same_id(repos):
    repos.alerts.insert("a1", WHEN, "uav-01", "info", "one", {"n": 1})
    repos.alerts.insert("a1", WHEN, "uav-01", "warning", "two", {"n": 2})
    assert _count(repos, "alerts", "alert_id", "a1") == 1
    assert _cell(repos, "alerts", "alert_id", "a1", "message") == "two"
    assert _cell(repos, "alerts", "alert_id", "a1", "level") == "warning"


def test_zone_round_trip(repos):
    ring = [
        {"lat": 17.0, "lon": 78.0},
        {"lat": 17.1, "lon": 78.0},
        {"lat": 17.1, "lon": 78.1},
        {"lat": 17.0, "lon": 78.0},
    ]
    repos.zones.add(Zone(id="z1", name="north", kind="aoi", polygon=ring))
    found = repos.zones.list()
    assert len(found) == 1
    assert found[0].name == "north"
    assert found[0].kind == "aoi"
    assert [(float(p["lat"]), float(p["lon"])) for p in found[0].polygon] == [
        (17.0, 78.0), (17.1, 78.0), (17.1, 78.1), (17.0, 78.0),
    ]


@pytest.mark.postgres
def test_naive_timestamp_is_rejected(postgres_repos):
    with pytest.raises(ValueError):
        postgres_repos.telemetry.insert(
            "uav-01", datetime(2026, 10, 12, 1, 0), 1.0, 2.0, 3.0, 0.0, 0.0, 1.0,
            "idle", "manual",
        )


@pytest.mark.postgres
def test_events_and_operator_actions_are_append_only(postgres_repos):
    postgres_repos.events.insert("e1", WHEN, "command", "go", "uav-01", "info", {})
    postgres_repos.operator_actions.add(OperatorAction(
        id="oa1", time=WHEN, actor="operator-1", asset_id="uav-01",
        action="goto", command_id="c1", payload={},
    ))
    for statement in (
        "UPDATE events SET message = 'changed'",
        "DELETE FROM events",
        "UPDATE operator_actions SET action = 'changed'",
        "DELETE FROM operator_actions",
    ):
        with pytest.raises(DBAPIError) as caught:
            with postgres_repos.engine.begin() as conn:
                conn.execute(text(statement))
        assert "permission denied" in str(caught.value).lower()
    assert postgres_repos.operator_actions.list()[0].action == "goto"
    assert _cell(postgres_repos, "events", "event_id", "e1", "message") == "go"


@pytest.mark.postgres
def test_telemetry_is_the_only_kernel_hypertable(admin):
    with admin.connect() as conn:
        rows = conn.execute(text(
            "SELECT hypertable_name FROM timescaledb_information.hypertables"
        )).fetchall()
    names = {row[0] for row in rows}
    assert "telemetry" in names
    assert "events" not in names
    assert "operator_actions" not in names


@pytest.mark.postgres
def test_time_columns_are_timestamptz(admin):
    expected = [
        ("assets", "created_at"),
        ("telemetry", "time"),
        ("commands", "issued_at"),
        ("events", "time"),
        ("operator_actions", "time"),
        ("missions", "created_at"),
        ("missions", "completed_at"),
        ("tracks", "updated_at"),
        ("detections", "time"),
        ("alerts", "time"),
    ]
    with admin.connect() as conn:
        for table, column in expected:
            data_type = conn.execute(text("""
                SELECT data_type FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = :table
                  AND column_name = :column
            """), {"table": table, "column": column}).scalar_one()
            assert data_type == "timestamp with time zone"


@pytest.mark.postgres
def test_indexes_cover_asset_id_and_time(admin):
    needles = {
        "assets": ("asset_id", "created_at"),
        "telemetry": ("asset_id", "time"),
        "commands": ("asset_id", "issued_at"),
        "events": ("asset_id", "time"),
        "operator_actions": ("asset_id", "time"),
        "missions": ("created_at",),
        "tracks": ("asset_id", "updated_at"),
        "detections": ("asset_id", "time"),
        "alerts": ("asset_id", "time"),
    }
    with admin.connect() as conn:
        rows = conn.execute(text(
            "SELECT tablename, indexdef FROM pg_indexes WHERE schemaname = 'public'"
        )).fetchall()
    by_table: dict[str, str] = {}
    for table, indexdef in rows:
        by_table[table] = by_table.get(table, "") + " " + indexdef.lower()
    for table, columns in needles.items():
        blob = by_table[table]
        for column in columns:
            assert column in blob, f"{table} is missing an index on {column}"


@pytest.mark.postgres
def test_positions_and_zone_polygons_are_postgis(admin, postgres_repos):
    expected = {
        ("assets", "position"): ("POINT", 3),
        ("telemetry", "position"): ("POINT", 3),
        ("missions", "detection"): ("POINT", 2),
        ("tracks", "position"): ("POINT", 2),
        ("detections", "position"): ("POINT", 2),
        ("zones", "geom"): ("POLYGON", 2),
    }
    with admin.connect() as conn:
        rows = conn.execute(text("""
            SELECT f_table_name, f_geometry_column, type, coord_dimension, srid
            FROM geometry_columns
            WHERE f_table_schema = 'public'
        """)).fetchall()
    found = {(row[0], row[1]): row[2:] for row in rows}
    for key, (kind, dim) in expected.items():
        got_type, got_dim, srid = found[key]
        assert srid == 4326
        assert got_dim == dim
        assert str(got_type).upper().startswith(kind)

    postgres_repos.telemetry.insert(
        "uav-01", WHEN, 17.4, 78.5, 100.0, 90.0, 12.0, 80.0, "mission", "auto",
    )
    postgres_repos.zones.add(Zone(
        id="z1",
        name="north",
        kind="aoi",
        polygon=[
            {"lat": 17.0, "lon": 78.0},
            {"lat": 17.1, "lon": 78.0},
            {"lat": 17.1, "lon": 78.1},
            {"lat": 17.0, "lon": 78.0},
        ],
    ))
    with postgres_repos.engine.connect() as conn:
        point = conn.execute(text(
            'SELECT ST_X(position), ST_Y(position), ST_Z(position) '
            "FROM telemetry WHERE asset_id = 'uav-01'"
        )).one()
        zone = conn.execute(text(
            "SELECT ST_GeometryType(geom), ST_NPoints(geom) FROM zones WHERE id = 'z1'"
        )).one()
    assert float(point[0]) == pytest.approx(78.5)
    assert float(point[1]) == pytest.approx(17.4)
    assert float(point[2]) == pytest.approx(100.0)
    assert zone[0] == "ST_Polygon"
    assert zone[1] >= 4


@pytest.mark.postgres
def test_postgres_snapshot_round_trip(request):
    _require_postgres(request)
    from src.data_services.postgres_backend import truncate_kernel_tables
    from src.data_services.repositories import PostgresSnapshot
    from src.kernel.models import Asset, AssetStatus, AssetType

    truncate_kernel_tables()
    store = PostgresSnapshot()
    assert store._engine is None
    asset = Asset(
        id="snap-roundtrip",
        name="Snap",
        asset_type=AssetType.UAV,
        status=AssetStatus.IDLE,
        lat=17.4,
        lon=78.5,
    )

    async def body():
        await store.connect()
        await store.save_asset(asset)
        loaded = await store.load_assets()
        await store.close()
        return loaded

    try:
        loaded = asyncio.run(body())
    finally:
        truncate_kernel_tables()
    assert store._engine is None
    match = next(item for item in loaded if item.id == "snap-roundtrip")
    assert match.name == "Snap"
    assert match.lat == pytest.approx(17.4)
