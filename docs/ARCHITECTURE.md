# Architecture — year 1 kernel

```
[UAV / UGV / sim]
        |  MAVLink | ROS2 | MQTT | mock
        v
 Asset Integration (adapters → ontology)
        |
        +--> Data & Services (state + event store)
        |
        +--> AI & Fusion (tracks, alerts)
        |
        +--> Command & Control (missions, tasking)
        |
        v
 Operator Application (web map + snapshot + commands)
        |
 Security & Audit (wraps commands and sessions)
```

## Mapping to the three reference systems

| Reference | In this repo, year 1 | Later |
|---|---|---|
| Lattice | Asset adapters + live telemetry + C2 | Mesh / denied comms |
| Gotham | Ontology objects + map + timeline of Events | Link analysis UI |
| Foundry | Same objects, write-back via Command | Pipelines, more domains |

## Design rules

1. One snapshot JSON feeds the dashboard. Do not let the UI query five stores.
2. Adapters are replaceable. Ontology is not.
3. Simulation implements the same Command/Telemetry contracts as hardware.
4. Offline / single-box deploy for the demo (one laptop is legal).

## Suggested first Grok Build tasks (in order)

1. Add `src/kernel/ontology.py` and import it from models (no behaviour change).
2. Fill `src/security/` with operator login stub + command audit wrapper.
3. One script: boot 2 UAV + 2 UGV sims, open dashboard, issue GOTO, dump events.
4. Test report template under `docs/`.
