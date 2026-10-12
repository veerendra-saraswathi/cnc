"""Keep-in and keep-out checks for Zone polygons.

PostGIS ST_Contains is the geometry check when the repository backend is
postgres. The duckdb backend keeps zones in memory and uses a ray cast.
An AOI does not breach. A keep-out hit rejects GOTO before the command
transaction. A breach raises one Alert per zone and asset.
"""

from __future__ import annotations

from src.data_services.repositories import get_repositories
from src.kernel.models import Alert, AlertLevel, Zone


def point_in_polygon(lat: float, lon: float, polygon: list) -> bool:
    """Even-odd ray cast. Ring points are {lat, lon}."""
    ring: list[tuple[float, float]] = []
    for point in polygon or []:
        if isinstance(point, dict) and "lat" in point and "lon" in point:
            ring.append((float(point["lon"]), float(point["lat"])))
    if len(ring) >= 2 and ring[0] == ring[-1]:
        ring = ring[:-1]
    count = len(ring)
    if count < 3:
        return False
    inside = False
    previous = count - 1
    for index in range(count):
        xi, yi = ring[index]
        xj, yj = ring[previous]
        if (yi > lat) != (yj > lat) and lon < (xj - xi) * (lat - yi) / (yj - yi) + xi:
            inside = not inside
        previous = index
    return inside


def _contains(zone: Zone, lat: float, lon: float, inside_ids: set[str] | None) -> bool:
    if inside_ids is not None:
        return zone.id in inside_ids
    return point_in_polygon(lat, lon, zone.polygon)


def _zones_and_hits(lat: float, lon: float) -> tuple[list[Zone], set[str] | None]:
    repos = get_repositories()
    zones = repos.zones.list()
    if getattr(repos, "backend", "") == "postgres":
        inside = {zone.id for zone in repos.zones.containing(lat, lon)}
        return zones, inside
    return zones, None


def keep_out_zone(lat: float, lon: float) -> Zone | None:
    """The keep-out polygon that contains this point, if any."""
    zones, inside_ids = _zones_and_hits(lat, lon)
    for zone in zones:
        if zone.kind == "keep-out" and _contains(zone, lat, lon, inside_ids):
            return zone
    return None


def breached_zone(lat: float, lon: float) -> Zone | None:
    """Keep-out when inside. Keep-in when outside every keep-in. AOI never."""
    zones, inside_ids = _zones_and_hits(lat, lon)
    for zone in zones:
        if zone.kind == "keep-out" and _contains(zone, lat, lon, inside_ids):
            return zone
    keep_ins = [zone for zone in zones if zone.kind == "keep-in"]
    if keep_ins and not any(_contains(zone, lat, lon, inside_ids) for zone in keep_ins):
        return keep_ins[0]
    return None


def zone_alert_id(zone_id: str, asset_id: str) -> str:
    return f"zone-{zone_id}-{asset_id}"


async def apply_zone_check(store, asset) -> Alert | None:
    """Raise one Alert when asset is outside a keep-in or inside a keep-out."""
    zone = breached_zone(asset.lat, asset.lon)
    if zone is None:
        return None
    alert_id = zone_alert_id(zone.id, asset.id)
    existing = store.alerts.get(alert_id)
    if existing is not None:
        return existing
    name = zone.name or zone.id
    if zone.kind == "keep-in":
        message = f"{asset.name} left keep-in {name}"
        level = AlertLevel.WARNING
    else:
        message = f"{asset.name} entered keep-out {name}"
        level = AlertLevel.CRITICAL
    alert = Alert(
        id=alert_id,
        level=level,
        title="Zone breach",
        message=message,
        asset_id=asset.id,
        source="zone",
    )
    await store.add_alert(alert)
    return alert
