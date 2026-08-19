"""
Improved Tracking Layer
-----------------------
More stable Track IDs across detection cycles.
Association uses both distance and label matching.
"""

from datetime import datetime
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field
from enum import Enum
import uuid
import math

from src.ai_fusion.perception import DetectionResult, DetectionLabel


class TrackStatus(str, Enum):
    TENTATIVE = "tentative"
    CONFIRMED = "confirmed"
    COASTING = "coasting"
    LOST = "lost"


class Track(BaseModel):
    track_id: str = Field(default_factory=lambda: f"T-{str(uuid.uuid4())[:4].upper()}")
    label: DetectionLabel = DetectionLabel.OBJECT_OF_INTEREST
    status: TrackStatus = TrackStatus.TENTATIVE

    lat: float
    lon: float
    confidence: float = 0.0

    positions: List[Dict[str, Any]] = Field(default_factory=list)
    hits: int = 1
    misses: int = 0

    first_seen: datetime = Field(default_factory=datetime.utcnow)
    last_seen: datetime = Field(default_factory=datetime.utcnow)

    source_asset: Optional[str] = None
    mission_id: Optional[str] = None
    raw: Dict[str, Any] = Field(default_factory=dict)

    def update(self, detection: DetectionResult):
        self.lat = detection.lat
        self.lon = detection.lon
        self.confidence = max(self.confidence * 0.6 + detection.confidence * 0.4, detection.confidence)
        self.label = detection.label
        self.last_seen = detection.timestamp
        self.hits += 1
        self.misses = 0

        if self.hits >= 2:
            self.status = TrackStatus.CONFIRMED
        else:
            self.status = TrackStatus.TENTATIVE

        self.positions.append({
            "lat": detection.lat,
            "lon": detection.lon,
            "timestamp": detection.timestamp.isoformat(),
            "confidence": detection.confidence
        })
        if len(self.positions) > 60:
            self.positions = self.positions[-60:]


class SimpleTracker:
    """
    Improved simple tracker.
    Association cost = distance + label mismatch penalty.
    """

    def __init__(self, dist_threshold_m: float = 120.0, max_misses: int = 6):
        self.tracks: Dict[str, Track] = {}
        self.dist_threshold_m = dist_threshold_m
        self.max_misses = max_misses

    def _haversine_m(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R = 6371000.0
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
        return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    def _association_cost(self, track: Track, det: DetectionResult) -> float:
        """
        Lower cost = better match.
        Distance in meters + penalty if labels differ.
        """
        dist = self._haversine_m(track.lat, track.lon, det.lat, det.lon)

        label_penalty = 0.0
        if track.label != det.label:
            # Strong penalty for different semantic class
            label_penalty = 80.0

        return dist + label_penalty

    def update(self, detections: List[DetectionResult]) -> List[Track]:
        updated_ids = set()

        for det in detections:
            best_track = None
            best_cost = float("inf")

            for track in self.tracks.values():
                if track.status == TrackStatus.LOST:
                    continue

                cost = self._association_cost(track, det)

                # Only consider if within threshold (after label penalty)
                if cost < self.dist_threshold_m and cost < best_cost:
                    best_cost = cost
                    best_track = track

            if best_track is not None:
                best_track.update(det)
                updated_ids.add(best_track.track_id)
            else:
                # Create new track
                new_track = Track(
                    label=det.label,
                    lat=det.lat,
                    lon=det.lon,
                    confidence=det.confidence,
                    source_asset=det.asset_id,
                    mission_id=det.mission_id,
                    positions=[{
                        "lat": det.lat,
                        "lon": det.lon,
                        "timestamp": det.timestamp.isoformat(),
                        "confidence": det.confidence
                    }]
                )
                self.tracks[new_track.track_id] = new_track
                updated_ids.add(new_track.track_id)

        # Age tracks that were not matched
        for tid, track in list(self.tracks.items()):
            if tid not in updated_ids:
                track.misses += 1
                if track.misses >= self.max_misses:
                    track.status = TrackStatus.LOST
                else:
                    track.status = TrackStatus.COASTING

        return [t for t in self.tracks.values() if t.status != TrackStatus.LOST]

    def get_active_tracks(self) -> List[Track]:
        return [t for t in self.tracks.values() if t.status != TrackStatus.LOST]

    def get_confirmed_tracks(self) -> List[Track]:
        return [t for t in self.tracks.values() if t.status == TrackStatus.CONFIRMED]

    def get_track(self, track_id: str) -> Optional[Track]:
        return self.tracks.get(track_id)


# Global instance – slightly more forgiving distance
tracker = SimpleTracker(dist_threshold_m=120.0, max_misses=6)
