"""Fusion products from a telemetry sample. No broker and no detector model."""

from src.ai_fusion.stream_fusion import fuse
from src.kernel.models import AlertLevel, Telemetry


def test_fuse_builds_track_detection_and_alert():
    sample = Telemetry(asset_id="uav-01", lat=17.0, lon=78.0, alt=40, battery=90)
    track, detection, alert = fuse(sample)
    assert track.id == "own-uav-01"
    assert track.lat == 17.0
    assert track.source_asset_ids == ["uav-01"]
    assert detection.asset_id == "uav-01"
    assert detection.lat == 17.0
    assert alert.level == AlertLevel.INFO
    assert alert.id == "fuse-uav-01"

    low = Telemetry(asset_id="uav-01", lat=17.0, lon=78.0, battery=10)
    warning = fuse(low)[2]
    assert warning.level == AlertLevel.WARNING
    assert warning.title == "Low battery"
