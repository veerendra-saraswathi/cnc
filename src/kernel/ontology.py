"""
Canonical object names for the year-1 kernel.

Each name has one Pydantic model in src.kernel.models. Other packages
import those classes or re-export them. Do not define a second copy.
"""

from enum import Enum


class ObjectName(str, Enum):
    ASSET = "Asset"
    POSITION = "Position"
    TELEMETRY = "Telemetry"
    COMMAND = "Command"
    MISSION = "Mission"
    TRACK = "Track"
    DETECTION = "Detection"
    ALERT = "Alert"
    EVENT = "Event"
    ZONE = "Zone"
    OPERATOR_ACTION = "OperatorAction"


KERNEL_OBJECTS = tuple(item.value for item in ObjectName)

# Ontology name -> the one model. Zone polygons are keep-in, keep-out, or
# AOI. Keep-out rejects a GOTO inside the polygon. A breach raises an Alert.
# OperatorAction is not wired into login.
IMPLEMENTATION = {
    ObjectName.ASSET: "src.kernel.models.Asset",
    ObjectName.POSITION: "src.kernel.models.Position",
    ObjectName.TELEMETRY: "src.kernel.models.Telemetry",
    ObjectName.COMMAND: "src.kernel.models.Command",
    ObjectName.MISSION: "src.kernel.models.Mission",
    ObjectName.TRACK: "src.kernel.models.Track",
    ObjectName.DETECTION: "src.kernel.models.Detection",
    ObjectName.ALERT: "src.kernel.models.Alert",
    ObjectName.EVENT: "src.kernel.models.Event",
    ObjectName.ZONE: "src.kernel.models.Zone",
    ObjectName.OPERATOR_ACTION: "src.kernel.models.OperatorAction",
}
