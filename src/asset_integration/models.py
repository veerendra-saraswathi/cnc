"""
Asset Abstraction Models
------------------------
Core data models for the hardware-agnostic Asset Integration Layer.
All UAVs, UGVs and future assets must speak this language.
"""

from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


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


class Position(BaseModel):
    lat: float = Field(..., description="Latitude in decimal degrees")
    lon: float = Field(..., description="Longitude in decimal degrees")
    alt: float = Field(0.0, description="Altitude in meters")
    heading: float = Field(0.0, description="Heading in degrees (0-360)")


class Telemetry(BaseModel):
    """Live telemetry published by every asset."""
    asset_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    position: Position
    speed: float = 0.0
    battery_percent: float = 100.0
    status: AssetStatus = AssetStatus.IDLE
    mode: str = "manual"
    sensors: Dict[str, Any] = Field(default_factory=dict)
    extras: Dict[str, Any] = Field(default_factory=dict)


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


class Command(BaseModel):
    command_id: str
    asset_id: str
    type: CommandType
    parameters: Dict[str, Any] = Field(default_factory=dict)
    issued_at: datetime = Field(default_factory=datetime.utcnow)
    issued_by: str = "operator"


class AssetInfo(BaseModel):
    asset_id: str
    name: str
    asset_type: AssetType
    manufacturer: Optional[str] = None
    model: Optional[str] = None
    capabilities: List[str] = Field(default_factory=list)
    max_speed: Optional[float] = None
    max_endurance_min: Optional[float] = None
    home_position: Optional[Position] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AssetState(BaseModel):
    info: AssetInfo
    telemetry: Optional[Telemetry] = None
    last_seen: Optional[datetime] = None
    connected: bool = False
    