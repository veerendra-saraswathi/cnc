# CNC kernel pack

Drop these files onto https://github.com/veerendra-saraswathi/cnc  
Do not replace `src/operator_app`, `src/command_control`, or simulations. This pack **names** the platform and maps the RCI EOI onto code you already have.

## What you already have

Layered Python prototype matching the EOI:

| EOI layer | Existing path |
|---|---|
| Asset integration | `src/asset_integration/models.py` |
| Communication | `src/communication/mqtt_client.py` |
| Data & services | `src/data_services/` |
| AI & fusion | `src/ai_fusion/` |
| Command & control | `src/command_control/` |
| Operator app | `src/operator_app/` |
| Security | `src/security/` (stub) |
| Demo assets | `simulations/` |

That is the **Lattice-shaped year-1 product**: 2 UAV + 2 UGV, one map, one operator, commands out.

Gotham / Foundry are the **growth path**, not the 12-month demo:

- Gotham = objects + links + map + timeline + provenance (ontology).
- Foundry = same objects become the company operating system later.
- Lattice = live mesh of assets. You are building this first because the EOI requires it.

## Install this pack

```bash
cd ~/src   # or wherever you keep repos
git clone https://github.com/veerendra-saraswathi/cnc.git
cd cnc

# copy the unzipped pack over the repo root
# (README.md, AGENTS.md, docs/, src/kernel/)

git checkout -b kernel
git add README.md AGENTS.md docs src/kernel
git commit -m "Add platform kernel: ontology, EOI trace, agent rules"
git push -u origin kernel
```

Then open a PR into `main`. Or merge locally if you are still solo.

## Run what exists today

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# start the operator app the way you already do; typical:
uvicorn src.operator_app.app:app --reload --host 127.0.0.1 --port 8000
```

Use Grok Build *inside this folder* (`grok`). Read `AGENTS.md` first. Plan mode on.

## Non-goals for the 12-month EOI

- Rebuilding Palantir Foundry
- Classified multi-tenant ontology studio
- Training foundation models
- Real weapons release
- Every connector on earth

Year 1 acceptance is in `docs/EOI_TRACE.md` section 10.
