"""One Pydantic model per ontology name.

Other packages import these classes. Do not declare a second Asset,
Position, Telemetry, Command, Mission, Track, Detection, Alert, Event,
Zone, or OperatorAction.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from uuid import uuid4

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _require_aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("time must be timezone-aware UTC")
    return value.astimezone(timezone.utc)


def _new_id() -> str:
    return str(uuid4())


def _short_id() -> str:
    return str(uuid4())[:8]


class AssetType(str, Enum):
    UAV = "uav"
    UGV = "ugv"
    USV = "usv"
    UUV = "uuv"
    SENSOR = "sensor"
    OTHER = "other"


class AssetStatus(str, Enum):
    OFFLINE = "offline"
    IDLE = "idle"
    ACTIVE = "active"
    MISSION = "mission"
    RETURNING = "returning"
    EMERGENCY = "emergency"
    ERROR = "error"
    LOST = "lost"


class CommandType(str, Enum):
    GOTO = "goto"
    LOITER = "loiter"
    RTL = "rtl"
    STOP = "stop"
    TAKEOFF = "takeoff"
    LAND = "land"
    ARM = "arm"
    DISARM = "disarm"
    SET_MODE = "set_mode"
    CUSTOM = "custom"
    HOLD = "hold"
    SET_SPEED = "set_speed"


class AlertLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class MissionStatus(str, Enum):
    PLANNED = "planned"
    ACTIVE = "active"
    COMPLETED = "completed"
    ABORTED = "aborted"
    IDLE = "idle"
    PLANNING = "planning"
    EXECUTING = "executing"
    DETECTION = "detection"
    INVESTIGATING = "investigating"
    FAILED = "failed"


class TrackType(str, Enum):
    FRIENDLY = "friendly"
    HOSTILE = "hostile"
    NEUTRAL = "neutral"
    UNKNOWN = "unknown"


class TrackState(str, Enum):
    ACTIVE = "active"
    STALE = "stale"
    LOST = "lost"


class TrackStatus(str, Enum):
    TENTATIVE = "tentative"
    CONFIRMED = "confirmed"
    COASTING = "coasting"
    LOST = "lost"


class DetectionLabel(str, Enum):
    VEHICLE = "vehicle"
    PERSON = "person"
    UNKNOWN = "unknown"
    OBJECT_OF_INTEREST = "object_of_interest"


class EventType(str, Enum):
    COMMAND = "command"
    STATUS_CHANGE = "status_change"
    ALERT = "alert"
    HEALTH = "health"
    MISSION = "mission"
    DETECTION = "detection"
    INFO = "info"


class Position(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    lat: float
    lon: float
    alt: float = 0.0
    heading: float = 0.0


class Asset(BaseModel):
    """Live operator asset plus the static fields integration used to call AssetInfo."""

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(default_factory=_new_id, validation_alias=AliasChoices("id", "asset_id"))
    name: str
    asset_type: AssetType
    status: AssetStatus = AssetStatus.OFFLINE
    lat: float = 0.0
    lon: float = 0.0
    alt: float = 0.0
    heading: float = 0.0
    speed: float = 0.0
    battery: float = 100.0
    capabilities: list[str] = Field(default_factory=list)
    home_lat: Optional[float] = None
    home_lon: Optional[float] = None
    last_seen: datetime = Field(default_factory=_utcnow)
    manufacturer: Optional[str] = Field(default=None, exclude=True)
    vehicle_model: Optional[str] = Field(
        default=None,
        exclude=True,
        validation_alias=AliasChoices("vehicle_model", "model"),
    )
    max_speed: Optional[float] = Field(default=None, exclude=True)
    max_endurance_min: Optional[float] = Field(default=None, exclude=True)
    home_position: Optional[Position] = Field(default=None, exclude=True)
    metadata: dict[str, Any] = Field(default_factory=dict, exclude=True)
    connected: bool = Field(default=False, exclude=True)

    @property
    def asset_id(self) -> str:
        return self.id

    @property
    def model(self) -> Optional[str]:
        return self.vehicle_model


class Telemetry(BaseModel):
    """Integration shape in storage. Operator code may pass flat lat/lon/battery."""

    model_config = ConfigDict(populate_by_name=True)

    asset_id: str
    seq: int = Field(default=0, ge=0)
    timestamp: datetime = Field(default_factory=_utcnow)
    position: Position
    speed: float = 0.0
    battery_percent: float = 100.0
    status: AssetStatus = AssetStatus.IDLE
    mode: str = "manual"
    sensors: dict[str, Any] = Field(default_factory=dict)
    extras: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _accept_flat_pose(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        raw = dict(data)
        if "position" not in raw and "lat" in raw and "lon" in raw:
            raw["position"] = {
                "lat": raw.pop("lat"),
                "lon": raw.pop("lon"),
                "alt": raw.pop("alt", 0.0),
                "heading": raw.pop("heading", 0.0),
            }
        if "battery_percent" not in raw and "battery" in raw:
            raw["battery_percent"] = raw.pop("battery")
        else:
            raw.pop("battery", None)
        return raw

    @property
    def lat(self) -> float:
        return self.position.lat

    @property
    def lon(self) -> float:
        return self.position.lon

    @property
    def alt(self) -> float:
        return self.position.alt

    @property
    def heading(self) -> float:
        return self.position.heading

    @property
    def battery(self) -> float:
        return self.battery_percent


class Command(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(default_factory=_new_id, validation_alias=AliasChoices("id", "command_id"))
    asset_id: str
    seq: int = Field(default=0, ge=0)
    command_type: CommandType = Field(validation_alias=AliasChoices("command_type", "type"))
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        validation_alias=AliasChoices("parameters", "payload"),
    )
    issued_at: datetime = Field(
        default_factory=_utcnow,
        validation_alias=AliasChoices("issued_at", "time"),
    )
    issued_by: str = Field(
        default="operator",
        min_length=1,
        validation_alias=AliasChoices("issued_by", "actor"),
    )
    status: str = "pending"
    result: Optional[dict[str, Any]] = None

    @field_validator("issued_at")
    @classmethod
    def _aware_issued_at(cls, value: datetime) -> datetime:
        return _require_aware(value)

    @property
    def command_id(self) -> str:
        return self.id

    @property
    def type(self) -> CommandType:
        return self.command_type

    @property
    def payload(self) -> dict[str, Any]:
        return self.parameters

    @property
    def time(self) -> datetime:
        return self.issued_at

    @property
    def actor(self) -> str:
        return self.issued_by


class Alert(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(default_factory=_new_id)
    level: AlertLevel
    title: str
    message: str
    asset_id: Optional[str] = None
    timestamp: datetime = Field(default_factory=_utcnow)
    acknowledged: bool = False
    source: str = "system"


class Mission(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(default_factory=_new_id, validation_alias=AliasChoices("id", "mission_id"))
    name: str = "Reconnaissance"
    description: str = ""
    status: MissionStatus = MissionStatus.PLANNED
    assigned_assets: list[str] = Field(default_factory=list)
    waypoints: list[dict[str, float]] = Field(default_factory=list)
    area: Optional[dict[str, Any]] = None
    objectives: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=_utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    detection_point: Optional[dict[str, Any]] = Field(default=None, exclude=True)
    assigned_uavs: list[str] = Field(default_factory=list, exclude=True)
    assigned_ugv: Optional[str] = Field(default=None, exclude=True)
    notes: list[str] = Field(default_factory=list, exclude=True)

    @property
    def mission_id(self) -> str:
        return self.id


class TrackHistoryPoint(BaseModel):
    """Operator sparkline sample. Not an ontology name."""

    model_config = ConfigDict(populate_by_name=True)

    timestamp: datetime
    lat: float
    lon: float
    alt: float = 0.0
    speed: float = 0.0
    heading: float = 0.0


class Track(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(default_factory=_new_id, validation_alias=AliasChoices("id", "track_id"))
    label: str = "unknown"
    track_type: TrackType = TrackType.FRIENDLY
    state: TrackState = TrackState.ACTIVE
    lat: float
    lon: float
    alt: float = 0.0
    heading: float = 0.0
    speed: float = 0.0
    confidence: float = 0.9
    source_asset_ids: list[str] = Field(default_factory=list)
    first_seen: datetime = Field(default_factory=_utcnow)
    last_update: datetime = Field(default_factory=_utcnow)
    attributes: dict[str, Any] = Field(default_factory=dict)
    history: list[TrackHistoryPoint] = Field(default_factory=list)
    # Fusion association state. Kept out of model_dump so a default
    # "tentative" is not written as status ahead of the operator state.
    status: TrackStatus = Field(default=TrackStatus.TENTATIVE, exclude=True)
    positions: list[dict[str, Any]] = Field(default_factory=list, exclude=True)
    hits: int = Field(default=1, exclude=True)
    misses: int = Field(default=0, exclude=True)
    last_seen: datetime = Field(default_factory=_utcnow, exclude=True)
    source_asset: Optional[str] = Field(default=None, exclude=True)
    mission_id: Optional[str] = Field(default=None, exclude=True)
    raw: dict[str, Any] = Field(default_factory=dict, exclude=True)

    @property
    def track_id(self) -> str:
        return self.id

    def update(self, detection: Detection) -> None:
        self.lat = detection.lat
        self.lon = detection.lon
        self.confidence = max(self.confidence * 0.6 + detection.confidence * 0.4, detection.confidence)
        label = detection.label
        self.label = label.value if isinstance(label, Enum) else str(label)
        self.last_seen = detection.timestamp
        self.last_update = detection.timestamp
        if detection.asset_id and detection.asset_id not in self.source_asset_ids:
            self.source_asset_ids.append(detection.asset_id)
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
            "confidence": detection.confidence,
        })
        if len(self.positions) > 60:
            self.positions = self.positions[-60:]


class Detection(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    detection_id: str = Field(default_factory=_short_id)
    timestamp: datetime = Field(default_factory=_utcnow)
    asset_id: str
    lat: float
    lon: float
    label: DetectionLabel = DetectionLabel.OBJECT_OF_INTEREST
    confidence: float = 0.0
    bbox: Optional[list[float]] = None
    image_path: Optional[str] = None
    mission_id: Optional[str] = None
    raw: dict[str, Any] = Field(default_factory=dict)

    @property
    def id(self) -> str:
        return self.detection_id


class Event(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_id: str = Field(default_factory=_short_id)
    timestamp: datetime = Field(
        default_factory=_utcnow,
        validation_alias=AliasChoices("timestamp", "time"),
    )
    event_type: EventType
    asset_id: str = ""
    message: str
    data: dict[str, Any] = Field(
        default_factory=dict,
        validation_alias=AliasChoices("data", "payload"),
    )
    actor: str = "system"
    severity: str = "info"

    @field_validator("timestamp")
    @classmethod
    def _aware_timestamp(cls, value: datetime) -> datetime:
        return _require_aware(value)

    @field_validator("asset_id", mode="before")
    @classmethod
    def _blank_asset_id(cls, value: object) -> str:
        if value is None:
            return ""
        return str(value)

    @field_validator("actor")
    @classmethod
    def _actor_present(cls, value: str) -> str:
        if not value:
            raise ValueError("actor is required")
        return value

    @property
    def time(self) -> datetime:
        return self.timestamp

    @property
    def payload(self) -> dict[str, Any]:
        return self.data


class Zone(BaseModel):
    """Keep-in, keep-out, or AOI polygon. A keep-out hit rejects GOTO. A breach raises an Alert."""

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(default_factory=_new_id)
    name: str = ""
    kind: str = "aoi"
    polygon: list[dict[str, Any]] = Field(default_factory=list)


class OperatorAction(BaseModel):
    """Who requested a command. command_id links the action to that Command."""

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(default_factory=_new_id)
    time: datetime = Field(default_factory=_utcnow)
    actor: str = Field(min_length=1)
    asset_id: str = ""
    action: str = Field(min_length=1)
    command_id: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("time")
    @classmethod
    def _aware_time(cls, value: datetime) -> datetime:
        return _require_aware(value)


# Track.update annotates Detection, which is defined above.
Track.model_rebuild()
