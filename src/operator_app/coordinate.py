"""One operator action: a UAV detection track tasks the nearest UGV."""

from __future__ import annotations

import math
from enum import Enum

from fastapi import HTTPException

from src.data_services.repositories import get_repositories
from src.kernel.models import (
    AssetType,
    Command,
    CommandType,
    Detection,
    OperatorAction,
    Track,
    TrackState,
    TrackType,
)
from src.operator_app.command_gate import stamp


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6_371_000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lon / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


def _kind(asset) -> str:
    value = getattr(asset, "asset_type", "")
    if isinstance(value, AssetType):
        return value.value
    if hasattr(value, "value"):
        return str(value.value)
    return str(value)


def nearest_ugv(assets, lat: float, lon: float):
    best = None
    best_distance = None
    for asset in assets:
        if _kind(asset) != AssetType.UGV.value:
            continue
        distance = haversine_m(lat, lon, float(asset.lat), float(asset.lon))
        if best_distance is None or distance < best_distance:
            best = asset
            best_distance = distance
    return best


def track_from_detection(detection: Detection) -> Track:
    """UAV detection becomes a track the operator can task. Id is det-<detection_id>."""
    label = detection.label.value if isinstance(detection.label, Enum) else str(detection.label)
    track = Track(
        id=f"det-{detection.detection_id}",
        label=label,
        track_type=TrackType.UNKNOWN,
        state=TrackState.ACTIVE,
        lat=detection.lat,
        lon=detection.lon,
        confidence=detection.confidence,
        source_asset_ids=[detection.asset_id] if detection.asset_id else [],
        attributes={"detection_id": detection.detection_id, "origin": "uav"},
    )
    get_repositories().detections.insert(
        detection_id=detection.detection_id,
        timestamp=detection.timestamp,
        asset_id=detection.asset_id,
        lat=detection.lat,
        lon=detection.lon,
        label=label,
        confidence=detection.confidence,
        mission_id=detection.mission_id,
    )
    return track


async def task_nearest_ugv(platform, track_id: str) -> Command:
    """Issue one GOTO to the nearest UGV and record the operator action."""
    track = await platform.store.get_track(track_id)
    if track is None:
        raise HTTPException(status_code=404, detail="Track not found")
    ugv = nearest_ugv(platform.store.assets.values(), track.lat, track.lon)
    if ugv is None:
        raise HTTPException(status_code=404, detail="No UGV in the fleet")
    issued_by = stamp(ugv.id, "goto")
    parameters = {"lat": track.lat, "lon": track.lon, "alt": 0, "track_id": track.id}
    cmd = Command(
        asset_id=ugv.id,
        command_type=CommandType.GOTO,
        parameters=parameters,
        issued_by=issued_by,
    )
    platform.operator_action_repo.add(OperatorAction(
        time=cmd.issued_at,
        actor=issued_by,
        asset_id=ugv.id,
        action="task-nearest-ugv",
        command_id=cmd.id,
        payload={"track_id": track.id, "lat": track.lat, "lon": track.lon},
    ))
    return await platform.issue(cmd)
