from src.data.ingest import ingest_telemetry
from src.data.realtime import get_platform_state
from src.data.historical import query_recent

ingest_telemetry(
    "uav-01",
    {"lat": 17.39, "lon": 78.49, "alt": 150, "mode": "GOTO", "battery": 85},
    role="operator",
    pedigree="cnc-phase1",
)
print("Live:", get_platform_state("uav-01"))
print(query_recent("uav-01", limit=2))
