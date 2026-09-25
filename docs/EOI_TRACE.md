# RCI EOI → code

Source: Unified Mission Autonomy Platform with Heterogeneous Defence Assets (12 months).

| Clause | Requirement | Where it lives now | Year-1 gap |
|---|---|---|---|
| 1.1–1.3 | Unified platform, one picture, 12 months | Whole repo | Need a dated demo script |
| 2.1 Asset Integration | Hardware-agnostic onboard | `src/asset_integration/` | More adapters, not more frameworks |
| 2.1 Communication | Secure exchange | `src/communication/mqtt_client.py` | TLS + auth still thin |
| 2.1 Data & Services | Persist and query | `src/data_services/` | Dual memory + DuckDB; add snapshot API tests |
| 2.1 AI & Fusion | Collect, fuse, COP | `src/ai_fusion/` | Sim detections only until real video/radar |
| 2.1 C2 | Monitor + task | `src/command_control/` | Multi-asset mission language |
| 2.1 Operator app | Web dashboard | `src/operator_app/` | Single screen exists; harden |
| 2.1 Security & Audit | Security layer | `src/security/` **empty** | Auth + signed commands + audit log |
| 2.2 Scalable later | No major redesign | Ontology + adapters | Keep adapters at the edge |
| 3.2 Two UAV + two UGV | Real, sim, or hybrid | `simulations/` | Script a 4-asset demo and record it |
| 3.3 Telemetry, status, commands | Bidirectional | models + mqtt + adapters | Prove on 4 assets in one run |
| 4.x Interoperability | UAV ↔ UGV share | `run_coordinated_mission.py` | Make this the acceptance path |
| 5.x COP, alerts, decision support | Fusion + dashboard | tracking + dashboard | Alerts must be visible on the map |
| 6.x C2 from one UI | Locations, status, tasking | `/` dashboard + command APIs | Operator click → Event |
| 7.x Web dashboard | Map, monitor, alerts, control | templates + static js | One screen, no second Streamlit for the demo |
| 8.x Vendor hardware | Vendor supplies | Out of software kernel | List kit in design doc |
| 9.x Deliverables | Design, source, adapters, manuals, tests, demo | docs/ + repo | Fill design + test report last quarter |
| 10.x Acceptance | 4 assets, comms, C2, one operator, 12 months | demo day | Freeze scope now |

If a pull request cannot be pointed at a row above, it is out of scope for the EOI year.
