"""
Mission Manager – uses real AI Perception (YOLO)
"""

from datetime import datetime
from enum import Enum
from typing import Dict, Optional, List
from pydantic import BaseModel, Field
import uuid

from src.data_services.event_store import event_store, EventType
from src.data_services.db import db
from src.ai_fusion.perception import perception_service


class MissionStatus(str, Enum):
    IDLE = "idle"
    PLANNING = "planning"
    EXECUTING = "executing"
    DETECTION = "detection"
    INVESTIGATING = "investigating"
    COMPLETED = "completed"
    FAILED = "failed"


class Mission(BaseModel):
    mission_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str = "Reconnaissance"
    status: MissionStatus = MissionStatus.IDLE
    created_at: datetime = Field(default_factory=datetime.utcnow)
    detection_point: Optional[Dict] = None
    assigned_uavs: List[str] = []
    assigned_ugv: Optional[str] = None
    notes: List[str] = []


class MissionManager:
    def __init__(self):
        self.current: Optional[Mission] = None

    def start_reconnaissance(self, fleet: dict) -> Mission:
        mission = Mission(
            name="Area Reconnaissance",
            status=MissionStatus.EXECUTING,
            assigned_uavs=["uav-01", "uav-02"],
            assigned_ugv="ugv-01"
        )
        self.current = mission

        if "uav-01" in fleet:
            fleet["uav-01"].send_command("goto", {"lat": 17.3980, "lon": 78.5050, "alt": 110})
        if "uav-02" in fleet:
            fleet["uav-02"].send_command("goto", {"lat": 17.4050, "lon": 78.4750, "alt": 120})

        db.upsert_mission(
            mission_id=mission.mission_id,
            name=mission.name,
            status=mission.status.value,
            assigned_uavs=mission.assigned_uavs,
            assigned_ugv=mission.assigned_ugv
        )

        event_store.add(
            EventType.MISSION,
            f"Mission {mission.mission_id} STARTED – UAVs tasked for reconnaissance",
            severity="info"
        )
        mission.notes.append("UAVs tasked for search areas")
        return mission

    def check_progress(self, fleet: dict) -> Optional[Mission]:
        if not self.current or self.current.status in (MissionStatus.COMPLETED, MissionStatus.FAILED):
            return self.current

        mission = self.current

        # -------------------------------------------------
        # Phase 1 – Real AI Perception / Detection
        # -------------------------------------------------
        if mission.status == MissionStatus.EXECUTING:
            uav1 = fleet.get("uav-01")
            if uav1 and len(uav1.trail) > 6:

                # Use one of your real test images
                test_image = "assets/car1.jpg"          # change to person1.jpg if you want

                detections = perception_service.process_asset(
                    asset_id="uav-01",
                    lat=uav1.position.lat,
                    lon=uav1.position.lon,
                    mission_id=mission.mission_id,
                    image_path=test_image
                )

                if detections:
                    det = detections[0]
                    mission.detection_point = {"lat": det.lat, "lon": det.lon}
                    mission.status = MissionStatus.DETECTION

                    db.upsert_mission(
                        mission_id=mission.mission_id,
                        name=mission.name,
                        status=mission.status.value,
                        detection_lat=det.lat,
                        detection_lon=det.lon,
                        assigned_uavs=mission.assigned_uavs,
                        assigned_ugv=mission.assigned_ugv
                    )
                    mission.notes.append(
                        f"Detection from YOLO ({det.label.value}, conf={det.confidence:.2f})"
                    )

        # -------------------------------------------------
        # Phase 2 – Task UGV
        # -------------------------------------------------
        if mission.status == MissionStatus.DETECTION and mission.detection_point:
            ugv = fleet.get(mission.assigned_ugv)
            if ugv:
                ugv.send_command("goto", {
                    "lat": mission.detection_point["lat"],
                    "lon": mission.detection_point["lon"],
                    "alt": 0
                })
                mission.status = MissionStatus.INVESTIGATING

                db.upsert_mission(
                    mission_id=mission.mission_id,
                    name=mission.name,
                    status=mission.status.value,
                    detection_lat=mission.detection_point["lat"],
                    detection_lon=mission.detection_point["lon"],
                    assigned_uavs=mission.assigned_uavs,
                    assigned_ugv=mission.assigned_ugv
                )

                event_store.add(
                    EventType.MISSION,
                    "UGV-01 tasked to investigate detection point",
                    asset_id="ugv-01"
                )
                mission.notes.append("UGV-01 investigating")

        # -------------------------------------------------
        # Phase 3 – Complete
        # -------------------------------------------------
        if mission.status == MissionStatus.INVESTIGATING and mission.detection_point:
            ugv = fleet.get(mission.assigned_ugv)
            if ugv:
                dlat = abs(ugv.position.lat - mission.detection_point["lat"])
                dlon = abs(ugv.position.lon - mission.detection_point["lon"])
                if dlat < 0.002 and dlon < 0.002:
                    mission.status = MissionStatus.COMPLETED
                    completed_at = datetime.utcnow()

                    db.upsert_mission(
                        mission_id=mission.mission_id,
                        name=mission.name,
                        status=mission.status.value,
                        detection_lat=mission.detection_point["lat"],
                        detection_lon=mission.detection_point["lon"],
                        assigned_uavs=mission.assigned_uavs,
                        assigned_ugv=mission.assigned_ugv,
                        completed_at=completed_at
                    )

                    event_store.add(
                        EventType.MISSION,
                        f"Mission {mission.mission_id} COMPLETED – UGV reached detection point",
                        severity="info"
                    )
                    mission.notes.append("Mission completed successfully")

        return mission

    def get_status_text(self) -> str:
        if not self.current:
            return "No active mission"
        return f"{self.current.name} [{self.current.status.value.upper()}]"


mission_manager = MissionManager()
