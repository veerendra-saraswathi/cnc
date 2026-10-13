"""Stop deadline and dashboard delta merge. No Docker daemon required."""

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import acceptance_run  # noqa: E402


def test_run_cmd_kills_a_hung_child():
    started = time.monotonic()
    proc = acceptance_run.run_cmd(["sleep", "30"], timeout=0.4)
    elapsed = time.monotonic() - started
    assert elapsed < 2.0
    assert proc.returncode != 0


def test_delta_merges_into_the_last_snapshot():
    latest = {
        "type": "snapshot",
        "assets": [
            {"id": "uav-01", "alt": 10, "name": "Eagle-1"},
            {"id": "ugv-01", "alt": 0, "name": "Rover-1"},
        ],
        "alerts": [{"id": "a1", "title": "old"}],
    }
    merged = acceptance_run._merge_frame(latest, {
        "type": "delta",
        "timestamp": "t",
        "assets": [{"id": "uav-01", "alt": 80}],
    })
    by_id = {row["id"]: row for row in merged["assets"]}
    assert by_id["uav-01"]["alt"] == 80
    assert by_id["uav-01"]["name"] == "Eagle-1"
    assert by_id["ugv-01"]["alt"] == 0
    assert merged["alerts"][0]["id"] == "a1"
    replaced = acceptance_run._merge_frame(latest, {"type": "snapshot", "assets": []})
    assert replaced["assets"] == []
