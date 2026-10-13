# RCI acceptance test report

**Date:** 13 October 2026  
**Code under test:** `7868a5188aa62cc29352c251a40218a950e81e91`  
**Worktree at measurement:** clean. `git status --porcelain` counted 0 paths.  
**Branch:** `main`, ahead of `origin/main` by 3  
**Machine record:** `docs/acceptance_m5.json` (this file is the live result; the 12 October record is in git history at `473de7e`)  
**Previous measurement:** 12 October 2026, commit `473de7eba09a61e18146b85136ebfca9de6b8536`, dirty worktree (59 paths at the start of that run)

## Result

Criteria 10.1, 10.2, 10.3, and 10.4 passed on one full run of `scripts/acceptance_run.py` (`--quick` was not used). Wall clock was 13 minutes 55 seconds (834.8 s), from `2026-10-12T20:23:58Z` to `2026-10-12T20:37:52Z`, including an 11-minute cruise and a measured 30.0-second NATS outage.

All four faults matched the expected column. On 12 October, three of the four did not.

Telemetry p95 on this laptop was **26 ms** (7,659 samples). The acceptance target for this run is telemetry p95 under 500 ms. That target was met. Every sample was under 500 ms. The 12 October p95 was 1,395 ms, and every sample that day was at least 516 ms.

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
| Acceptance files | `/tmp/cnc-m6/cnc.duckdb`, `/tmp/cnc-m6/cnc.db` |
| Dashboard | `http://127.0.0.1:8765/` HTTP 200, one `/ws/cop` client |

`CNC_DUCKDB_PATH` and `CNC_SNAPSHOT_PATH` select those files. Unset, the process still uses `data/cnc.duckdb` and `data/cnc.db`. Port 8000 was free for the whole run, so this process did not share JetStream durables or Redis keys with another operator.

`cnc-nats-1` and `cnc-redis` were the only containers stopped. Both were running again at the end (`action: left` after they had been started). `cnc-timescale-1` was not stopped.

## Scenario

`scripts/acceptance_run.py` started the operator app on port 8765 with the demo sim fleet:

| Id | Name | Type | Home |
| --- | --- | --- | --- |
| uav-01 | Eagle-1 | UAV | 17.3850, 78.4867 |
| uav-02 | Eagle-2 | UAV | 17.3900, 78.4900 |
| ugv-01 | Wolf-1 | UGV | 17.3820, 78.4840 |
| ugv-02 | Wolf-2 | UGV | 17.3880, 78.4920 |

One HTTP client and one WebSocket client drove the dashboard. The script issued GOTO and HOLD through `POST /assets/{id}/command`, created mission `86bddd2d-1886-48a4-be21-d63542ba6006`, and started it through the HTTP mission routes. A UAV detection created track `det-d5948df1`, and `POST /tracks/{id}/task-nearest-ugv` tasked the nearest UGV. The cruise issued a GOTO about every 20 seconds and armed a temporary keep-out around a UAV about every 45 seconds, then removed that zone. Faults followed: stop `cnc-nats-1` for 30 seconds, stop the `ugv-02` adapter and wait 32 seconds, stop `cnc-redis` for 15 seconds, then restart the operator process.

## Criteria

| Id | Result | Evidence |
| --- | --- | --- |
| 10.1 Integration of 2 UAV + 2 UGV | Pass | Four distinct ids and homes on one socket. Movement after the opening GOTO: uav-01 181.3 m, uav-02 176.8 m, ugv-01 58.8 m, ugv-02 58.7 m. |
| 10.2 Information exchange among assets | Pass | `POST /assets/uav-01/detect` returned track `det-d5948df1` (source `uav-01`, 17.3865, 78.4875). Task command `9f01d4be-cfe5-4e13-b754-f51fc094381f` went to `ugv-02`, status `sent`, actor `operator:op-1`, issued `2026-10-12T20:24:15.212627Z`. DuckDB events `1edf4a40` (`goto (pending)`, stored `2026-10-13 01:54:15.212732`) and `817cfc65` (`ack goto (ok)`, stored `2026-10-13 01:54:15.259172`). OperatorAction `task-nearest-ugv` links the same command id. Idempotency recall is one success. |
| 10.3 C2 through the platform | Pass | HOLD `08c475d4-4622-4fbe-8cb3-901f8b7cd41f` on `uav-02`, status `sent`, ack true, actor `operator:op-1`, issued `2026-10-12T20:24:15.304666Z`. Events `0d17f1fd` and `a4ed84d7` (`ack hold (ok)`). Mission `86bddd2d-1886-48a4-be21-d63542ba6006` created and started over HTTP, status `active` at `2026-10-12T20:24:15.421797Z`. Mission-start GOTOs are issued with actor `mission`. Operator HTTP commands use `operator:op-1`. |
| 10.4 One operator, one dashboard | Pass | `GET /` returned 200 and referenced `dashboard.js`. One socket subscriber. The frame carried assets, tracks, alerts, missions, recent commands, and zones for all four fleet ids. Operator-issued commands in the run shared actor `operator:op-1`. |

DuckDB event timestamps above are the naive values stored in `/tmp/cnc-m6/cnc.duckdb`. They match the UTC `issued_at` values shifted to the laptop's local zone (UTC+5:30).

## Faults

The expected column is the acceptance brief for this run. The 12 October column is what commit `473de7e` recorded. The 13 October column is what commit `7868a51` recorded in the same process, with the containers restored afterwards.

| Fault | 12 October (`473de7e`) | 13 October (`7868a51`) |
| --- | --- | --- |
| NATS stopped for 30 s | Matched: no. GOTO `7520bbb3-9290-4530-b08e-ae045866c822` on `ugv-01` returned HTTP 200 in 94 ms, status `pending`, result `{"queued": true}`. Outbox `01a1317e-a2c4-7fd9-98fb-c099865049e1` stayed unpublished (`attempts` 1). After port 4222 accepted connections, the same process left the command `pending` for 45 s. `published_at` stayed null and recall stayed empty. Delivery happened only after a later process restart. | Matched: yes. GOTO `0e99e896-637b-4345-823f-5345dace542d` on `ugv-01` (17.42, 78.52) returned HTTP 200 in 233 ms at `2026-10-12T20:35:18.419362Z`, status `pending`, result `{"queued": true}`, actor `operator:op-1`. The broker was down for 30.0 s and port 4222 accepted connections after `docker start` at `2026-10-12T20:35:48Z`. The same process published outbox `01a131d3-ad13-7012-b4f2-fa315e8610df` (`cnc.command.ugv-01`) at stored time `2026-10-13 02:05:54.655964` (`attempts` 1). The command then read status `sent`, result `{"ack": true}`. Ack event `6c057911` is stored at `2026-10-13 02:06:24.681499`. Recall is one success. The sim target was 17.42, 78.52, alt 0. The following process restart did not send it again. |
| One vehicle adapter stopped | Matched: no. `ugv-02` (Wolf-2) was `offline` immediately. Friendly track `3ac2be98-e79d-4cb9-a33f-54b523691060` was `active`, then `lost` after 32 s. Alerts whose text contains "lost": 0. | Matched: yes. `ugv-02` was `offline` immediately and friendly track `a9760f5d-e186-4d99-8328-b19adb19b7fa` (Wolf-2) was `active`. After 32 s the asset and the track were both `lost`. One new alert, id `presence-ugv-02`, level `warning`, title "Asset lost", message "Wolf-2 is offline and its track is lost". A second warning, `presence-track-det-d5948df1`, covers the detection track that has no asset ("Track object_of_interest is lost"). A further lifecycle tick did not add another alert for `ugv-02`. |
| Redis unreachable | Matched: no. The socket kept the four fleet assets and `uav-01` had moved about 169 m, with `redis_down` true. After `docker start`, `redis_down` stayed true for the rest of the process. `fell_back_to_postgres` was false. The HOLD issued in that window stayed `pending` because the NATS client from the earlier outage was still closed. | Matched: yes. With Redis stopped, a HOLD returned HTTP 200 with body status `sent` in 128 ms. The socket frame still contained all four fleet assets, `redis_down` was true, and `overlay_loaded` was false. `uav-01` was at the same coordinates as the pre-outage fix (0.0 m); `last_seen` advanced from `2026-10-12T20:36:56.986615Z` to `2026-10-12T20:37:14.170630Z`. `ugv-02` was already `lost` from the adapter fault. `fell_back_to_postgres` stayed false: this process was on duckdb, and `cop_frame` does not query Postgres. After `docker start` at `2026-10-12T20:37:14Z`, within the 8 s poll, `redis_down` was false and `overlay_loaded` was true. One socket subscriber remained. |
| Process restart | Matched: yes, for assets and the mission. The new process loaded `uav-01`, `uav-02`, `ugv-01`, `ugv-02` and mission `c7989d00-2039-48e5-856d-e80e27f2fdb1`. Five tracks and 22 of the prior alert ids came back. Recent command ids: none. The event ring held 3 new rows. DuckDB still had 92 event rows. The queued GOTO was published by this new process (`attempts` 3) and acked once. | Matched: yes, including commands and the event ring. The child logged `Recovered 4 assets, 1 missions, 5 tracks, 15 commands, 92 events`. Assets `uav-01`, `uav-02`, `ugv-01`, `ugv-02` and mission `86bddd2d-1886-48a4-be21-d63542ba6006` were back. Five prior tracks and 24 of the prior alert ids were present (the snapshot load keeps 30). Fifteen recent command ids were reloaded, including the queued GOTO `0e99e896-637b-4345-823f-5345dace542d` with status `sent` and result `{"ack": true}`. The event ring held 50 events. DuckDB still had 92 event rows, including hold events `0d17f1fd` / `a4ed84d7` and task events `1edf4a40` / `817cfc65`. The outbox row was already published by the previous process, so the child recall is empty and the child did not move the sim target again. |

## Latency

Samples were taken on this laptop while NATS and Redis were up, before the fault windows. Percentiles use linear interpolation on the sorted samples (the same convention as NumPy's default percentile). Times below are milliseconds, rounded to the nearest millisecond. Raw M6 telemetry p95 is 25.644 ms.

| Path | Run | n | p50 | p95 | p99 | min | max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Telemetry sample (`asset.last_seen`) to WebSocket frame | 12 Oct | 3,493 | 860 | 1,395 | 2,969 | 516 | 6,382 |
| Telemetry sample (`asset.last_seen`) to WebSocket frame | 13 Oct | 7,659 | 14 | 26 | 38 | 2 | 260 |
| GOTO HTTP issue to ack response | 12 Oct | 37 | 165 | 437 | 531 | 34 | 568 |
| GOTO HTTP issue to ack response | 13 Oct | 37 | 85 | 175 | 329 | 39 | 406 |
| Alert timestamp to WebSocket frame | 12 Oct | 14 | 953 | 1,734 | 1,942 | — | — |
| Alert timestamp to WebSocket frame | 13 Oct | 15 | 6 | 25 | 26 | 3 | 27 |
| Keep-out zone insert to the frame that shows the alert | 12 Oct | 14 | 1,830 | 3,370 | 3,431 | — | — |
| Keep-out zone insert to the frame that shows the alert | 13 Oct | 15 | 197 | 383 | 464 | 47 | 484 |

The telemetry target is p95 under 500 ms. 12 October missed it. 13 October met it: 0 of 7,659 samples were at or above 500 ms. The mass of the 13 October samples sits under 20 ms (3,271 in 0–10 ms, 3,332 in 10–20 ms, 853 in 20–30 ms, 179 in 30–50 ms, 21 in 50–100 ms, 2 in 100–200 ms, 1 at 260 ms). No sample had a negative clock difference on either run.

The repository documents do not state a numeric target for GOTO, alert raise, or zone insert. Those three paths are shorter on 13 October as well. The zone-insert figure on 12 October included the old telemetry scan of about 0.8 s.

The 13 October picture pushes a frame when the snapshot changes. The sim stamps a sample on poll, the operator loop notifies the socket before the durable write, and a frame already in flight is followed by one frame that carries the latest snapshot. The poll interval on that loop is 0.1 s. The Redis pub/sub listen socket is not given a 0.5 s read timeout.

## Audit failure and ProvenanceError

Two different gates were easy to collapse into one sentence on 12 October. They are separate.

`docs/ARCHITECTURE_DATA.md` still says the acceptance demo stays fail-open and that `CNC_AUDIT_STRICT` stays `0`. That sentence means a failed durable write is logged and delivery can continue, so a database blip does not strand a vehicle. This run did not set `CNC_AUDIT_STRICT`. The architecture note was left as it stood.

`Platform.issue` is the in-memory gate. It writes the command Event and calls `require_command_event` before `dispatch_command`. If that Event cannot be recorded, `issue` sets status `failed`, result `{"error": "command event was not recorded"}`, and raises `ProvenanceError`. The HTTP route turns that into 503. The adapter has not been called.

The 13 October code adds the durable gate on every path that hands a command to an adapter or to JetStream. `command_audit_recorded` must read the events row for that command id. A missing row or a query error fails the command with the same result object, and `_send_local` returns without `send_command`. The JetStream pull consumer raises `RuntimeError("command event was not recorded")`, so the message is nak'd and the adapter is not called. That is the fail-closed behaviour for command dispatch. It does not depend on `CNC_AUDIT_STRICT`.

Telemetry and fusion writes still log a failure and continue. A database blip on those paths does not stop the picture loop.

The operator stamp on this run is still the stub session `operator:op-1`.

## What this commit changed relative to 12 October

Commit `7868a51` is the tree that was measured. It also contains the durable store, JetStream outbox, and live picture that were uncommitted on 12 October. The gaps named in the 12 October report, and what this run showed:

- Telemetry reaches the socket when the snapshot changes. p95 moved from 1,395 ms to 26 ms.
- After the broker returns, the JetStream client reconnects and the outbox relay publishes pending rows in the same process. The queued GOTO was published once (`attempts` 1) and acked once.
- An asset that goes offline, and a track that goes lost, raise one warning alert per asset (`presence-{asset_id}`). A track with no asset uses `presence-track-{track_id}`.
- A failed Redis ping retries with backoff. After Redis returned, `overlay_loaded` became true in the same process. `PostgresSnapshot.connect` creates the operator snapshot table when `STORAGE_BACKEND=postgres`. This acceptance process stayed on duckdb, so that connect path was not exercised here. The unit suite covers construction without a dial, and a Postgres round trip.
- Command dispatch fails closed when the durable command event is missing, as described above.
- Restart reloads the newest commands and the recent event ring from the store, along with assets, missions, tracks, and alerts.

Two earlier fixes are inside the same commit and were already described on 12 October: a missing adapter naks the JetStream message instead of completing the delivery, and an empty `TimeoutError` from the NATS dial no longer aborts process startup.

## Regression suite

After this acceptance run, from the repository root on the default store paths:

`.venv/bin/python -m pytest -q --tb=line`

92 passed in 8.85 s. Exit code 0. The acceptance script is not collected by pytest. The same 92 passed in 14.14 s on this commit before the acceptance run.

## Known gaps

- The security stamp is the stub session `operator:op-1`.
- The fleet is the in-process sim (`SimAssetAdapter`). ArduPilot SITL and ROS 2 were not in this run.
- Latency figures are this laptop's loop, NATS, Redis, and one WebSocket client. They are not a field-radio measurement.
- The operator picture on this run is the entity store plus the SQLite snapshot, with Redis as an overlay. `STORAGE_BACKEND` was duckdb. `cop_frame` does not query Postgres. `PostgresSnapshot.connect` is implemented for a process that sets `STORAGE_BACKEND=postgres`; this run did not.
- `docs/ARCHITECTURE_DATA.md` still describes `CNC_AUDIT_STRICT=0` as fail-open for a failed durable write. Command dispatch on this commit refuses to send when the durable command event is missing, and the flag was not set to `1`.
- The dashboard was checked through the acceptance HTTP and WebSocket client on port 8765. It was not opened in a browser, and the 860 px layout was not visually checked.

## M7 — ArduPilot SITL and Gazebo, 13 October 2026

This section is a separate measurement. The sections above record the in-process sim run and are unchanged.

`docs/acceptance_m7.json` is the M7.1 run below. The 13:20Z numbers are the previous record, kept here and in commit `63a89a6`.

### M7.1 result

**When:** `2026-10-13T15:24:48Z` to `2026-10-13T16:27:54Z` (3785.4 s). `--quick` was not used. Cruise was 660 s.  
**Tree the operator imported:** commit `63a89a6`, dirty, 15 paths.  
**Fleet file:** `/work/fleet.sitl.yaml` inside `cnc-fleet`. The default `fleet.yaml` is still the in-process sim.

The run completed and wrote the JSON. Criteria 10.2 passed. Criteria 10.1, 10.3, and 10.4 failed. All four faults matched. `cnc-nats-1` and `cnc-redis` were running again at the end.

Opening GOTOs all acked (2.0 s to 3.3 s). Movement over the 12 s observe window:

| Vehicle | Metres |
| --- | --- |
| uav-01 | 237.0 |
| uav-02 | 0.0 |
| ugv-01 | 0.0 |
| ugv-02 | 29.3 |

The 15 m bar is unchanged. uav-02 and ugv-01 stayed on the same lat/lon through that window, so 10.1 failed.

10.3 created the mission (HTTP 200) and the HOLD acked. Mission start raised `ReadTimeout` during the host freeze below, so the start status was empty and 10.3 failed. 10.4 saw two WebSocket subscribers. The frame still had assets, tracks, alerts, missions, and recent commands. The acceptance client is one of those sockets. A second socket was already connected when the criterion ran.

The host froze twice during the run. Docker stats jumped from `15:25:17Z` to `16:05:50Z` (2433 s), between 10.2 and 10.3, and from `16:17:07Z` to `16:25:42Z` (515 s), during the NATS hold.

### M7.1 latency

Poll stamp to WebSocket receive, 5081 samples. The script's stated target is 500 ms. p50 is 338.4 ms, p95 is 859.5 ms, p99 is 1722.5 ms, and the max is 2,425,001 ms. Thirty-one samples are over 2 s. Eight are over 5 s. The max is one sample that crosses a host freeze. Dropping samples over 5 s leaves p95 at 845 ms. The 500 ms target is missed. GOTO issue-to-ack p95 is 3554 ms (19 samples, p50 2303 ms). Alert raise-to-dashboard p95 is 1539 ms (13 samples, p50 618 ms). Zone insert to dashboard p95 is 2162 ms (13 samples). No telemetry sample had a negative clock.

Picture-path stages, client samples, milliseconds. UAV n is 2542 and UGV n is 2539.

| Stage | All p50 | All p95 | UAV p95 | UGV p95 |
| --- | --- | --- | --- | --- |
| message gap (simulator receive interval) | 15.3 | 55.5 | 66.3 | 30.7 |
| adapter receive to poll | 10.0 | 46.2 | 54.4 | 25.0 |
| poll to frame assembly | 162.0 | 578.8 | 605.7 | 557.0 |
| frame assembly to WebSocket receive | 71.8 | 487.2 | 491.0 | 476.7 |
| poll to WebSocket receive | 338.4 | 859.5 | 892.7 | 826.7 |

`source_to_adapter` is now present for all 5081 client samples. UGV p50 is 21.6 s and p95 is 22.6 s, a tight band: the Gazebo sim-to-wall offset sat about 21 s off the wall clock, inside the 30 s re-anchor, so the stage is the sim clock lag. UAV p50 is 641 s and p95 is 1457 s. `SYSTEM_TIME` supplied a boot offset, and the copter clock (SITL speedup 4) diverged from the wall clock across the freezes. The stage is measured. It is the vehicle clock, measured against wall time, and the message gap above is the receive interval.

A connect gets one snapshot. Later `/ws/cop` messages are deltas, coalesced for about 50 ms and serialized once. That coalesce is why poll-to-frame p50 moved from 15.6 ms on the previous run to 162 ms here. The WebSocket hop p50 moved from 175 ms to 72 ms. From the Mac during this run, the page and `dashboard.js` returned HTTP 200, and a WebSocket client received one snapshot of all four vehicles and then deltas. Folding the deltas still left all four. The map was not clicked. There is no browser tool in this session.

### JetStream

The previous run's 5710 missing samples were dropped by an in-process queue of 256 in front of a worker that awaited each PubAck. The stream's 1 GiB limit was not full. This run's `durable_dropped` is 0. Telemetry publishes run as tasks with at most 32 in-flight acks. A failed publish is delivered locally and raises one coalesced alert (`telemetry-publish`). `CNC_TELEMETRY` `max_bytes` is 7,077,888,000 (4 assets, 10 Hz, 24 h, 2 KiB). There is no `max_msgs` cap. Other streams stay at 1 GiB. Discard policy is still old.

`poll_to_nats` n is 5205, p50 410.5 ms, p95 969.5 ms. The 100 ms p95 target is missed. A probe publish from a second client during the cruise acked in 5 ms. That stream's configured `max_bytes` was 7,077,888,000 and `max_msgs` was unset. The in-process ack waited on the operator event loop. Six telemetry publishes timed out at 2 s inside the measurement window: one at `15:25:29Z`, and five between `16:06:07Z` and `16:06:53Z`, on the wake from the first freeze. Those six have no PubAck. `nats_to_ingest` n is 1542, p50 288 s, p95 2538 s. Ingest lagged across the freezes. It does not set `last_seen` and does not feed the WebSocket number.

### NATS stop

On the 13:20Z run, `docker stop cnc-nats-1` was logged at `13:32:25Z` and the broker answered with EOF about a second later. `down_s` was 1809.4 s. That figure is the time from stop until the planned hold finished, and the host resource sampler was silent for the same gap. The stop command itself returned. It was not a 30-minute lame-duck drain.

`nats:2.10` has no lame-duck duration flag. Compose sets `stop_grace_period: 5s`. The harness runs `docker stop -t 2` under its own deadline and records `elapsed_s`. On this run that elapsed time is 1.328 s and the container was exited. The target is under 15 s, and the stop met it. `down_s` is 496.7 s because the 29 s hold spanned the second freeze (`16:17:07Z` to `16:25:42Z`). The log line `nats stays down 29.0s` is the monotonic hold. `docker start` is at `16:25:35Z`. The fault still matched: the queued GOTO was published once and acked after the broker returned. Redis stop elapsed 1.211 s. The ugv-02 adapter stop reached `lost` on the asset and the friendly track after 32 s. Process restart reloaded all four assets and the mission (child exit 0).

### What ran

| Vehicle | Software | Connection |
| --- | --- | --- |
| uav-01, uav-02 | ArduCopter SITL, image `radarku/ardupilot-sitl:latest` | MAVLink UDP `udpin:0.0.0.0:14550` and `:14560` inside `cnc-fleet`. Each SITL container shares that network namespace and sends `udpclient` to `127.0.0.1`. |
| ugv-01, ugv-02 | ROS 2 Humble and Gazebo Classic, image `cnc-fleet:humble` | Namespaces `/ugv_01` and `/ugv_02`. Odometry becomes Telemetry. GOTO is a `NavigateToPose` goal on the waypoint follower. The ack is goal acceptance. |

Gazebo ran headless: `xvfb-run` plus `gzserver` only, `LIBGL_ALWAYS_SOFTWARE=1`, `GALLIUM_DRIVER=llvmpipe`, no `gzclient`. The rovers are a diff-drive model. Headless was the setup for this 16 GiB laptop. The run finished, and Gazebo was not killed for memory.

The operator process lived inside `cnc-fleet` and talked to NATS and Redis through `host.docker.internal`. Uvicorn binds `0.0.0.0:8765` inside the container. Compose publishes `127.0.0.1:8765:8765`. The in-container check was HTTP 200, 3420 bytes, and the page references `dashboard.js`.

### CPU and memory

Host: MacBookPro16,1, Intel Core i7-9750H at 2.60 GHz, 12 logical CPUs, 16 GiB (`hw.memsize` 17179869184). Docker server 29.8.2, VM memory 8839323648 bytes (8.232 GiB), 5 CPUs.

Twenty-nine docker-stats samples from `15:24:45Z` through `16:27:53Z`. The two freezes above are gaps inside that window, so the samples are not one per minute of wall clock. Docker VM RSS was not sampled on this run.

| Process | CPU | Memory |
| --- | --- | --- |
| `cnc-fleet` (Gazebo, follower, operator) | 46% to 198% | 164 MiB to 482 MiB |
| `cnc-sitl-uav-01` | 16% to 30% | docker stats RSS 1.0 MiB to 2.9 MiB |
| `cnc-sitl-uav-02` | 5% to 44% | docker stats RSS 1.2 MiB to 3.4 MiB |
| `cnc-nats-1` | 0.05% to 21% | 3.4 MiB to 19.5 MiB |
| `cnc-redis` | 0% to 3.2% | 4.5 MiB to 23.7 MiB |

The SITL RSS figures are the docker-stats values for containers that use `network_mode: service:cnc-fleet`. Those numbers stay near 1–3 MiB.

### Host suite

Commit `ca0ad62` reports 92 passed. That collection is 80 test functions, 12 of which also run a second time under the postgres parameter. No test function was deleted. At `63a89a6` the same tree, with `cnc-timescale-1` exited, was 81 passed and 27 skipped.

This tree adds seven tests: bounded in-flight telemetry publish, one coalesced publish-failure alert, a hung `docker` CLI that is killed at its deadline, delta merge into the last snapshot, the Gazebo sim-to-wall offset, the MAVLink `SYSTEM_TIME` offset, and a one-asset WebSocket delta. From the repository root, `CNC_DUCKDB_PATH=/tmp/cnc-it.duckdb CNC_SNAPSHOT_PATH=/tmp/cnc-it.db .venv/bin/python -m pytest -q -rs --tb=line`: 88 passed, 27 skipped, in 15.47 s. Exit code 0. That command does not start SITL or Gazebo.

The same 27 skip. Twenty-five skip because Postgres on `127.0.0.1:5432` is down. The duckdb parameter of a shared test still passes.

- `src/data/test_nats_pipeline.py`: `test_streams_use_the_architecture_retention`, `test_sim_telemetry_reaches_postgres_redis_and_fusion`, `test_goto_is_dispatched_acked_and_audited`, `test_missing_ack_raises_an_alert`, `test_invalid_mavlink_telemetry_lands_on_the_dlq`. The file is marked `nats`. The skip text is the Postgres `OperationalError`, because the JetStream check opens that database.
- `src/data_services/test_repositories.py`, postgres parameter: `test_asset_upsert_replaces_the_row`, `test_telemetry_inserts_each_sample`, `test_commit_issued_rolls_back_command_and_event_together`, `test_recent_command_keeps_status_and_finds_its_event`, `test_command_insert_ignores_a_duplicate_id`, `test_event_insert_ignores_a_duplicate_and_keeps_a_null_asset`, `test_operator_action_round_trip`, `test_mission_upsert_replaces_status`, `test_track_upsert_replaces_label_state`, `test_detection_insert_ignores_a_duplicate_id`, `test_alert_insert_replaces_the_same_id`, `test_zone_round_trip`.
- `src/data_services/test_repositories.py`, postgres only: `test_naive_timestamp_is_rejected`, `test_events_and_operator_actions_are_append_only`, `test_telemetry_is_the_only_kernel_hypertable`, `test_time_columns_are_timestamptz`, `test_indexes_cover_asset_id_and_time`, `test_positions_and_zone_polygons_are_postgis`, `test_postgres_snapshot_round_trip`.
- `src/operator_app/test_live_cop.py`: `test_postgis_keep_out_contains_the_interior_point`.

Skipped until the integration flag is set:

- `src/operator_app/adapters/test_sitl.py`: `test_sitl_goto_hold_and_rtl_ack` — set `CNC_SITL=1` when ArduCopter SITL is sending MAVLink UDP.
- `src/operator_app/adapters/test_ros2_integration.py`: `test_ros2_odom_and_goto_ack` — set `CNC_ROS2=1` when the Gazebo waypoint action is up.

### Previous record, 13:20Z

Commit `63a89a6`. `2026-10-13T13:20:39Z` to `2026-10-13T14:13:43Z` (3183.7 s), tree `ca0ad62` dirty. Criteria 10.1 through 10.4 passed. Opening movement was uav-01 237.0 m, uav-02 236.8 m, ugv-01 51.7 m, ugv-02 40.1 m. All four faults matched. Poll to WebSocket receive, 6610 samples: p50 233.7 ms, p95 537.3 ms, p99 757.9 ms, max 2893 ms. The 500 ms target was missed by 37 ms at p95. `durable_dropped` was 5710. `poll_to_nats` p95 was 31.3 s on 714 stamps. `source_to_adapter` n was 0. `down_s` for the NATS fault was 1809.4 s, explained above. An earlier same-day sample of the same interval was 1482 ms p95 on 364 samples.
