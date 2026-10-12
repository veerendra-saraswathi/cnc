from src.data.realtime import set_platform_state, get_platform_state
from src.data.historical import append_telemetry, query_recent
from datetime import datetime, timezone

state = {
    "platform_id": "uav-01",
    "lat": 17.385,
    "lon": 78.486,
    "alt": 120.5,
    "mode": "GOTO",
    "battery": 87,
}
set_platform_state("uav-01", state)
print("Redis (fake):", get_platform_state("uav-01"))

append_telemetry([{**state, "ts": datetime.now(timezone.utc).isoformat()}])
print(query_recent("uav-01"))
