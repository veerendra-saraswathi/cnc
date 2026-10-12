# Ontology (Gotham slice)

This is the world model. Foundry later is “apps that write back to these objects.”
Lattice now is “assets and tracks live on the map.”

Keep this list under 12 types until month 6.

| Object | Meaning | Already in repo |
|---|---|---|
| **Asset** | UAV, UGV, USV, sensor. Hardware-agnostic. | `src.kernel.models.Asset` (`AssetInfo` is an alias) |
| **Position** | lat, lon, alt, heading | `src.kernel.models.Position` |
| **Telemetry** | last known live state of an Asset | `src.kernel.models.Telemetry` |
| **Command** | operator or C2 intent to one Asset | `src.kernel.models.Command` |
| **Mission** | ordered tasking across one or more Assets | `src.kernel.models.Mission` |
| **Track** | fused estimate of a thing in the world (friendly asset or detection) | `src.kernel.models.Track` |
| **Detection** | raw observation from one sensor / asset | `src.kernel.models.Detection` (`DetectionResult` is an alias) |
| **Alert** | something an operator must see | `src.kernel.models.Alert` |
| **Event** | immutable fact: command, status, detection | `src.kernel.models.Event` |
| **Zone** | keep-in / keep-out / AOI polygon | `src.kernel.models.Zone` (keep-out blocks GOTO; a breach raises an Alert) |
| **OperatorAction** | who clicked what | `src.kernel.models.OperatorAction` |

## Links that matter

- Asset *emits* Telemetry
- Asset *receives* Command
- Mission *assigns* Asset
- Detection *updates* Track
- Track *raises* Alert
- Command *writes* Event
- Event *points at* Asset and Operator

## Provenance (do this early, cheap)

Every Event and Command stores: `timestamp`, `actor`, `asset_id`, `payload`.
That is Gotham. You do not need a graph database in year 1. Postgres/SQLite/DuckDB + these fields is enough.

## What Foundry would add later (not year 1)

Pipelines that land logistics, test reports, vendor lots, export licences onto the same Asset and Mission objects. Same kernel. Different adapters.
