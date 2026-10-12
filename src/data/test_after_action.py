"""After-action Parquet contains the mission's telemetry, commands, events, and alerts."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import polars as pl

from src.data.after_action import write_after_action
from src.data_services.repositories import get_repositories

WHEN = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)


def test_export_writes_one_parquet_per_table(tmp_path):
    asset_id = f"export-{uuid4()}"
    other_id = f"export-other-{uuid4()}"
    mission_id = f"mission-{uuid4()}"
    repos = get_repositories()
    repos.telemetry.insert(asset_id, WHEN, 17.4, 78.5, 40.0, 10.0, 5.0, 90.0, "mission", "auto")
    repos.telemetry.insert(other_id, WHEN, 1.0, 2.0, 3.0, 0.0, 0.0, 50.0, "idle", "manual")
    repos.commands.insert(f"cmd-{uuid4()}", asset_id, "goto", {"lat": 17.4}, "operator-1", WHEN)
    repos.events.insert(f"evt-{uuid4()}", WHEN, "command", "goto", asset_id, "info", {"reason": "demo"})
    repos.alerts.insert(f"alt-{uuid4()}", WHEN, asset_id, "warning", "zone", {"zone_id": "z"})

    result = write_after_action(mission_id, [asset_id], tmp_path / mission_id)
    assert result["uploaded"] is False
    assert set(result["files"]) == {"telemetry", "commands", "events", "alerts"}

    telemetry = pl.read_parquet(result["files"]["telemetry"])
    commands = pl.read_parquet(result["files"]["commands"])
    events = pl.read_parquet(result["files"]["events"])
    alerts = pl.read_parquet(result["files"]["alerts"])
    assert telemetry.height == 1
    assert telemetry["asset_id"][0] == asset_id
    assert telemetry["mission_id"][0] == mission_id
    assert telemetry["lat"][0] == 17.4
    assert commands.height == 1
    assert commands["command_type"][0] == "goto"
    assert events.height == 1
    assert "demo" in events["data"][0]
    assert alerts.height == 1
    assert alerts["level"][0] == "warning"


def test_export_writes_an_empty_schema(tmp_path):
    mission_id = f"empty-{uuid4()}"
    result = write_after_action(mission_id, [f"nobody-{uuid4()}"], tmp_path / mission_id)
    for name in ("telemetry", "commands", "events", "alerts"):
        frame = pl.read_parquet(result["files"][name])
        assert frame.height == 0
        assert "mission_id" in frame.columns
        assert "asset_id" in frame.columns
