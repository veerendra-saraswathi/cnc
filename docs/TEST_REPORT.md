# RCI acceptance test report

**Date:** 12 October 2026  
**Commit:** `473de7eba09a61e18146b85136ebfca9de6b8536`  
**Worktree:** dirty. The machine record counted 59 paths at the start of the run. This report and the Word file bring the count to 61.  
**Branch:** `main`, ahead of `origin/main` by 2  
**Machine record:** `docs/acceptance_m5.json`

## Result

Criteria 10.1, 10.2, 10.3, and 10.4 passed on one scripted run of `scripts/acceptance_run.py`. Wall clock was 13 minutes 38 seconds (817.8 s), including an 11-minute cruise and a measured 30.0-second NATS outage.

The fault story diverged from the expected column in three places. A command issued while NATS was down stayed `pending` after the broker accepted connections again. Stopping `ugv-02` set the asset `offline` and, after 32 seconds, the friendly track `lost`, with no Alert. With Redis stopped, the dashboard kept drawing the fleet from the operator entity store. Postgres was not on that path.

Telemetry p95 on this laptop was **1,395 ms**. The acceptance target for this run is telemetry p95 under 500 ms. That target was missed. GOTO and alert times are reported below. The repository documents do not state a numeric target for those two.

## Environment

| Item | Value |
| --- | --- |
| Where measured | This laptop, in-process sim. Field radios and SITL were not in the path. |
| OS | macOS 26.7.1, x86_64 |
| Python | 3.13.0 |
| FastAPI / Uvicorn | 0.141.1 / 0.54.0 |
| Pydantic | 2.13.5 |
| nats-py | 2.16.0 |
| Redis client / server | client 8.1.0, container `redis:7` (`cnc-redis`) |
| DuckDB | 1.5.5 |
| httpx / websockets | 0.28.1 / 16.1.1 |
| SQLAlchemy / psycopg | 2.1.1 / 3.3.6 |
| NATS | `nats:2.10` container `cnc-nats-1`, JetStream, port 4222 |
| Postgres | `timescale/timescaledb-ha:pg16` container `cnc-timescale-1`, left running. The operator process did not use it. |
| Process settings | `CNC_BUS=nats`, `STORAGE_BACKEND` unset (duckdb) |
| Acceptance files | `/tmp/cnc-m5/cnc.duckdb`, `/tmp/cnc-m5/cnc.db` |
| Dashboard | `http://127.0.0.1:8765/` HTTP 200, one `/ws/cop` client |

`CNC_DUCKDB_PATH` and `CNC_SNAPSHOT_PATH` select those files. Unset, the process still uses `data/cnc.duckdb` and `data/cnc.db`. The acceptance process used the temporary paths so it would not take the file lock of another operator process.

A `uvicorn --reload` server was already listening on port 8000 with `CNC_BUS=nats`. It shared the JetStream durable consumers and the Redis keys with this run, so it was stopped for the measurement and started again afterward.

## Scenario

`scripts/acceptance_run.py` started the operator app on port 8765 with the demo sim fleet:

| Id | Name | Type | Home |
| --- | --- | --- | --- |
| uav-01 | Eagle-1 | UAV | 17.3850, 78.4867 |
| uav-02 | Eagle-2 | UAV | 17.3900, 78.4900 |
| ugv-01 | Wolf-1 | UGV | 17.3820, 78.4840 |
| ugv-02 | Wolf-2 | UGV | 17.3880, 78.4920 |

One HTTP client and one WebSocket client drove the dashboard. The script issued GOTO and HOLD through `POST /assets/{id}/command`, created mission `c7989d00-2039-48e5-856d-e80e27f2fdb1` ("M5 acceptance"), and started it through the HTTP mission routes. A UAV detection created track `det-b45262d6`, and `POST /tracks/{id}/task-nearest-ugv` tasked the nearest UGV. The cruise issued a GOTO about every 20 seconds and armed a temporary keep-out around a UAV about every 45 seconds, then removed that zone. Faults followed: stop `cnc-nats-1` for 30 seconds, stop the `ugv-02` adapter and wait 32 seconds, stop `cnc-redis`, then restart the operator process. Both containers were running at the end (`action: left` after they had been started again).

## Criteria

| Id | Result | Evidence |
| --- | --- | --- |
| 10.1 Integration of 2 UAV + 2 UGV | Pass | Four distinct ids and homes on one socket. Movement after the opening GOTO: uav-01 180.5 m, uav-02 182.1 m, ugv-01 60.0 m, ugv-02 61.7 m. |
| 10.2 Information exchange among assets | Pass | `POST /assets/uav-01/detect` returned track `det-b45262d6` (source `uav-01`, 17.3865, 78.4875). Task command `ed11e8b1-98ec-476a-8525-8ae83bab5738` went to `ugv-02`, status `sent`, actor `operator:op-1`, issued `2026-10-12T18:51:22.183246Z`. DuckDB events `538b0724` (`goto (pending)`, stored `2026-10-13 00:21:22.183394`) and `5545dad9` (`ack goto (ok)`, stored `2026-10-13 00:21:22.4614`). OperatorAction `task-nearest-ugv` links the same command id. Idempotency recall is one success. |
| 10.3 C2 through the platform | Pass | HOLD `f9217303-c37d-4b3f-8c3d-5dc699334a35` on `uav-02`, status `sent`, ack true, actor `operator:op-1`, issued `2026-10-12T18:51:22.711561Z`. Events `cf95272a` and `a39e6957` (`ack hold (ok)`). Mission `c7989d00-2039-48e5-856d-e80e27f2fdb1` created and started over HTTP, status `active` at `2026-10-12T18:51:23.121858Z`. Mission-start GOTOs are issued with actor `mission`. Operator HTTP commands use `operator:op-1`. |
| 10.4 One operator, one dashboard | Pass | `GET /` returned 200 and referenced `dashboard.js`. One socket subscriber. The frame carried assets, tracks, alerts, missions, recent commands, and zones for all four fleet ids. Operator-issued commands in the run shared actor `operator:op-1`. |

DuckDB event timestamps above are the naive values stored in `/tmp/cnc-m5/cnc.duckdb`. They match the UTC `issued_at` values shifted to the laptop's local zone (UTC+5:30).

## Faults

Expected text is the acceptance brief. The actual column is what this run recorded.

| Fault | Expected | Actual |
| --- | --- | --- |
| NATS stopped for 30 s | The command stays pending, then is delivered once when NATS returns. | `docker stop cnc-nats-1` held the broker down for 30.0 s. GOTO `7520bbb3-9290-4530-b08e-ae045866c822` on `ugv-01` (17.42, 78.52) returned HTTP 200 in 94 ms with status `pending` and result `{"queued": true}`, actor `operator:op-1`, issued `2026-10-12T19:02:25.220205Z`. Outbox `01a1317e-a2c4-7fd9-98fb-c099865049e1` on `cnc.command.ugv-01` stayed unpublished (`attempts` 1). Port 4222 accepted connections after `docker start`. For the following 45 s the same process left the command `pending`, `published_at` null, and recall empty. The live JetStream client was not replaced. |
| One vehicle adapter stopped | The vehicle is marked lost and an Alert is raised. | `ugv-02` (Wolf-2) was `offline` immediately. Friendly track `3ac2be98-e79d-4cb9-a33f-54b523691060` was `active` at that instant and `lost` after 32 s. Alerts whose text contains "lost": 0. New alerts after the stop: 0. |
| Redis unreachable | The common operating picture falls back to Postgres. | `docker stop cnc-redis`. A following HOLD returned HTTP 200. The socket frame still contained all four fleet assets, and `uav-01` had moved about 169 m from the pre-outage fix. `redis_down` was true. After `docker start cnc-redis` and another HOLD, `redis_down` was still true and the frame still contained the four assets. The operator picture is the entity store (memory plus the SQLite snapshot). `cop_frame` does not query Postgres. `STORAGE_BACKEND` for the process was duckdb. The HOLD itself stayed `pending`, because the NATS client from the earlier outage was still closed. |
| Process restart | Operator state comes back. | The new process loaded `uav-01`, `uav-02`, `ugv-01`, `ugv-02` and mission `c7989d00-2039-48e5-856d-e80e27f2fdb1`. Five prior tracks were present. 22 of the prior alert ids were present (the snapshot load keeps 30). Recent commands after restart: none. The in-memory event ring held 3 new rows. DuckDB still had 92 event rows, including hold events `cf95272a` / `a39e6957` and task events `538b0724` / `5545dad9`. The queued GOTO's outbox row was published at stored time `2026-10-13 00:34:37.514065` (`attempts` 3). One ack event followed: `ack goto (ok)` at `2026-10-13 00:34:38.618726`. Recall for `ugv-01` + `7520bbb3-9290-4530-b08e-ae045866c822` is a single success. The sim target was 17.42, 78.52, alt 0. |

## Latency

Samples were taken on this laptop while NATS and Redis were up, before the fault windows. Percentiles use linear interpolation on the sorted samples (the same convention as NumPy's default percentile). Times are milliseconds, rounded to the nearest millisecond.

| Path | n | p50 | p95 | p99 | Target |
| --- | --- | --- | --- | --- | --- |
| Telemetry sample (`asset.last_seen`) to WebSocket frame | 3,493 | 860 | 1,395 | 2,969 | p95 under 500 ms. **Missed** on this laptop. |
| GOTO HTTP issue to ack response | 37 | 165 | 437 | 531 | No numeric target in the repo docs. |
| Alert timestamp to WebSocket frame | 14 | 953 | 1,734 | 1,942 | No numeric target in the repo docs. |
| Keep-out zone insert to the frame that shows the alert | 14 | 1,830 | 3,370 | 3,431 | Includes the telemetry scan (about 0.8 s) plus the push above. |

No sample had a negative clock difference. The minimum telemetry sample was 516 ms, so every measured frame in this set was already over the 500 ms line.

## Fix applied before this run

The restart shake-out published the queued GOTO and the command consumer answered `asset not found` because the demo fleet was not registered yet. That ack completed the JetStream delivery, so the vehicle never executed the command.

`src/operator_app/command_bus.py` now raises when the adapter is missing. The pull consumer naks the message and JetStream redelivers it. On this run the redelivery executed once: one `ack goto (ok)` event, one recall success, and the sim target at 17.42, 78.52.

No other product behavior was changed to match the expected fault column. The same-process NATS recovery, the lost-track Alert, and a Postgres picture were left as measured.

Bringing the operator process back on port 8000 exposed a second defect. `ensure_jetstream` logged a failed dial with `str(exc).splitlines()[0]`. A `TimeoutError` from the NATS client has an empty message, so that lookup raised `IndexError` and the process exited during startup. The handler now uses the exception class name when the message is empty, and the dial still returns false. `src/data/test_bus.py` covers an empty `TimeoutError`.

## Regression suite

After the acceptance run, from the repository root on the default store paths:

`.venv/bin/python -m pytest -q --tb=line`

83 passed in 27.36 s. Exit code 0. The count includes the empty-`TimeoutError` dial test added after the acceptance run.

## Known gaps

- The operator picture is the entity store and the SQLite snapshot. `PostgresSnapshot.connect` is unimplemented, so this process cannot start with `STORAGE_BACKEND=postgres`.
- A failed Redis ping is remembered for the life of the process. Frames continue from the entity store, and the process does not resume the Redis overlay when Redis returns.
- `lifecycle_tick` moves a track to `lost` after 25 seconds without telemetry. It does not raise an Alert. `adapter.stop()` sets the asset `offline` immediately.
- After the broker returns, a process that still holds the old JetStream client leaves unpublished outbox rows pending. Delivery of the queued command happened in the restarted process.
- Restart restores SQLite assets, missions, tracks, and the newest alerts. The in-memory command list starts empty. The in-memory event ring starts empty. DuckDB event rows remain.
- The security stamp is the stub session `operator:op-1`. Audit failure stays fail-open.
- The fleet is the in-process sim (`SimAssetAdapter`), not ArduPilot SITL or ROS 2.
- Latency figures are this laptop's loop, NATS, Redis, and one WebSocket client. They are not a field-radio measurement.
- `docs/ARCHITECTURE_DATA.md` was left as it stood.
