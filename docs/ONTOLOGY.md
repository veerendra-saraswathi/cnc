# Ontology (Gotham slice)

This is the world model. Foundry later is “apps that write back to these objects.”
Lattice now is “assets and tracks live on the map.”

Keep this list under 12 types until month 6.

| Object | Meaning | Already in repo |
|---|---|---|
| **Asset** | UAV, UGV, USV, sensor. Hardware-agnostic. | `AssetInfo`, `AssetState` |
| **Position** | lat, lon, alt, heading | `Position` |
| **Telemetry** | last known live state of an Asset | `Telemetry` |
| **Command** | operator or C2 intent to one Asset | `Command` |
| **Mission** | ordered tasking across one or more Assets | `mission_manager.py`, `app.py` |
| **Track** | fused estimate of a thing in the world (friendly asset or detection) | `ai_fusion/tracking.py`, `app.py` |
| **Detection** | raw observation from one sensor / asset | perception + events |
| **Alert** | something an operator must see | EventType.ALERT |
| **Event** | immutable fact: command, status, detection | `event_store.py` |
| **Zone** | keep-in / keep-out / AOI polygon | not yet — add when missions need it |
| **OperatorAction** | who clicked what | Command.issued_by + Event |

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
