"""
Canonical object names for the year-1 kernel.

Existing Pydantic models live in src.asset_integration.models and
src.operator_app.app. This module does not replace them. It is the
shared vocabulary so Gotham-style growth does not fork a second world model.
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

# Map ontology name -> current implementation module.
# Update this table when a model is promoted or merged.
IMPLEMENTATION = {
    ObjectName.ASSET: "src.asset_integration.models.AssetInfo / AssetState",
    ObjectName.POSITION: "src.asset_integration.models.Position",
    ObjectName.TELEMETRY: "src.asset_integration.models.Telemetry",
    ObjectName.COMMAND: "src.asset_integration.models.Command",
    ObjectName.MISSION: "src.command_control.mission_manager / operator_app.app",
    ObjectName.TRACK: "src.ai_fusion.tracking / operator_app.app",
    ObjectName.DETECTION: "src.ai_fusion.perception + EventType.DETECTION",
    ObjectName.ALERT: "EventType.ALERT / operator_app Alert",
    ObjectName.EVENT: "src.data_services.event_store.Event",
    ObjectName.ZONE: "not implemented — add when missions need keep-in/out",
    ObjectName.OPERATOR_ACTION: "Command.issued_by + Event",
}
