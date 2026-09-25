# AGENTS.md — CNC / RCI mission autonomy

You are working on a 12-month DRDO/RCI-style unified mission autonomy platform.
Repo: veerendra-saraswathi/cnc
Team year 1: one engineer. Two more join ~month 3.

## Product in one sentence

One operator, one map, heterogeneous UAV + UGV assets, commands out, audit trail on.

## Objects you may create or change

Asset, Position, Telemetry, Command, Mission, Track, Detection,
Alert, Event, Zone, OperatorAction.

Canonical definitions: `docs/ONTOLOGY.md` and `src/kernel/ontology.py`.
Do not invent a second Asset or Mission model. Extend the existing ones.

## Layer rules

- Adapters (MAVLink, ROS2, MQTT, mock) convert vendor bytes → ontology objects.
- Fusion writes Tracks and Alerts. It does not talk to hardware.
- C2 issues Commands. It does not own the map UI.
- Operator app reads the snapshot and posts commands. It does not embed fusion math.
- Security wraps every command and login. Empty `src/security/` is debt, not optional.

## Engineering rules

- Plan mode before multi-file edits.
- Do not add langchain / ollama / streamlit features unless the task names them.
- Prefer the existing FastAPI dashboard over a second UI.
- Tests live next to the behaviour they protect.
- No secrets in git. No `.env` commits.
- Every command issued must produce an Event with operator id, time, asset id.

## 12-month demo bar (do not gold-plate past this)

- ≥2 UAV + ≥2 UGV (real, sim, or hybrid)
- Telemetry in, commands out
- One web dashboard: map, status, alerts, tasking
- Coordinated information sharing UAV ↔ UGV
- Design docs + source + test report + live demo

## When unsure

Read `docs/EOI_TRACE.md`. If a feature is not in the EOI and not in the ontology, do not build it.
