#!/usr/bin/env python3
"""RCI acceptance run (EOI 10.1–10.4) against the live operator app.

One process serves the dashboard with CNC_BUS=nats and the default DuckDB
store. The bind is 127.0.0.1 unless CNC_FLEET or CNC_HTTP_HOST is set. The
fleet container binds 0.0.0.0 so compose can publish 127.0.0.1:8765 on the
host. This client still calls 127.0.0.1. A single WebSocket client plus HTTP
commands exercise the fleet. Faults stop only the cnc-nats-1 and cnc-redis
containers, stop one in-process adapter, then restart the app. The script
records measured behavior. It does not grade a fault as passed when the
platform did something else.

The reportable run is the default: about 15 minutes, NATS down for 30 seconds.
``--quick`` shortens every wait so the harness can be shaken out. Do not
quote --quick numbers as the acceptance result.

Writes docs/acceptance_m5.json. Prints ACCEPTANCE_DONE or ACCEPTANCE_FAILED
as the last line.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import platform
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FLEET = ("uav-01", "uav-02", "ugv-01", "ugv-02")
UAVS = ("uav-01", "uav-02")
UGVS = ("ugv-01", "ugv-02")
KILLED_ASSET = "ugv-02"
QUEUED_ASSET = "ugv-01"
ALLOWED_CONTAINERS = {
    "cnc-nats-1": "nats",
    "cnc-redis": "redis",
}
RESULT_PATH = ROOT / "docs" / "acceptance_m5.json"
ACTOR = "operator:op-1"

_stopped: set[str] = set()


def isolate_stores() -> None:
    """Keep this run off the demo server's DuckDB and SQLite files."""
    root = Path("/tmp/cnc-m7" if os.environ.get("CNC_FLEET") else "/tmp/cnc-m6")
    root.mkdir(parents=True, exist_ok=True)
    os.environ["CNC_DUCKDB_PATH"] = str(root / "cnc.duckdb")
    os.environ["CNC_SNAPSHOT_PATH"] = str(root / "cnc.db")


def log(message: str) -> None:
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"{stamp} {message}", flush=True)


def scrub(text: str) -> str:
    return re.sub(
        r"(token|password|passwd|secret)=([^\s'\"]+)",
        r"\1=<redacted>",
        text or "",
        flags=re.IGNORECASE,
    )


def iso(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def parse_dt(value) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def percentile(samples: list[float], pct: float) -> float | None:
    """Linear interpolation between nearest ranks (numpy's percentile default)."""
    if not samples:
        return None
    ordered = sorted(samples)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (pct / 100.0)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[int(rank)]
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def _stage_num(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _stage_span(start: float | None, end: float | None) -> float | None:
    if start is None or end is None:
        return None
    return end - start


def stats(samples: list[float]) -> dict:
    return {
        "n": len(samples),
        "p50_ms": percentile(samples, 50),
        "p95_ms": percentile(samples, 95),
        "p99_ms": percentile(samples, 99),
        "min_ms": min(samples) if samples else None,
        "max_ms": max(samples) if samples else None,
    }


def _kill_group(proc: subprocess.Popen) -> None:
    """SIGKILL the child and the session it leads. Already-exited is fine."""
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.kill()
        except OSError:
            pass


def _read_pipe(pipe, sink: list[str]) -> None:
    try:
        sink.append(pipe.read())
    except Exception:
        sink.append("")


def run_cmd(args: list[str], timeout: float = 40) -> subprocess.CompletedProcess:
    """Run a CLI and return on a monotonic deadline.

    subprocess.run on POSIX kills a timed-out child and then process.wait()
    with no second deadline. A Docker CLI stuck in the Desktop API proxy
    holds that wait. The M7 fault clock advanced 1,809 s with no
    "did not return" line. This helper does not call that wait.
    """
    proc = subprocess.Popen(
        args,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    out_sink: list[str] = []
    err_sink: list[str] = []
    threads = [
        threading.Thread(target=_read_pipe, args=(proc.stdout, out_sink), daemon=True),
        threading.Thread(target=_read_pipe, args=(proc.stderr, err_sink), daemon=True),
    ]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + max(0.1, timeout)
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        time.sleep(0.05)
    else:
        _kill_group(proc)
        reap_until = time.monotonic() + 0.5
        while time.monotonic() < reap_until and proc.poll() is None:
            time.sleep(0.02)
    for thread in threads:
        thread.join(0.2)
    code = proc.returncode if proc.returncode is not None else 124
    stdout = out_sink[0] if out_sink else ""
    stderr = err_sink[0] if err_sink else ""
    if proc.returncode is None and not stderr:
        stderr = "command exceeded deadline"
    return subprocess.CompletedProcess(args, code, stdout, stderr)


def container_info(name: str, timeout: float = 20) -> dict:
    if name not in ALLOWED_CONTAINERS:
        raise RuntimeError(f"refusing to inspect {name}")
    proc = run_cmd(
        ["docker", "inspect", "-f", "{{.Name}}\t{{.State.Status}}\t{{.Config.Image}}", name],
        timeout=timeout,
    )
    if proc.returncode == 124 or "exceeded deadline" in (proc.stderr or ""):
        return {"name": name, "present": True, "status": "unknown", "error": "inspect timed out"}
    if proc.returncode != 0:
        return {"name": name, "present": False, "error": scrub(proc.stderr.strip())}
    parts = proc.stdout.strip().split("\t")
    if len(parts) != 3:
        return {"name": name, "present": False, "error": scrub(proc.stdout.strip())}
    raw_name, status, image = parts
    short = raw_name.lstrip("/")
    if short != name:
        raise RuntimeError(f"refusing {name}: inspect returned {short}")
    if ALLOWED_CONTAINERS[name] not in image:
        raise RuntimeError(f"refusing {name}: image {image}")
    return {"name": name, "present": True, "status": status, "image": image}


def _wait_until_stopped(name: str, seconds: float = 20.0) -> dict:
    """Poll until Docker reports the container is not running."""
    deadline = time.monotonic() + seconds
    last = container_info(name, timeout=2)
    while time.monotonic() < deadline:
        if last.get("status") not in {"running", "unknown"}:
            return last
        time.sleep(0.3)
        last = container_info(name, timeout=2)
    return last


def docker_stop(name: str) -> dict:
    info = container_info(name, timeout=2)
    if not info.get("present") and info.get("status") != "unknown":
        return {"ok": False, "action": "stop", **info}
    if info.get("status") not in {"running", "unknown"}:
        return {"ok": False, "action": "stop", "detail": "not running", **info}
    log(f"docker stop {name}")
    started = time.monotonic()
    # NATS 2.10 lame-ducks for 2 minutes on SIGTERM, then waits for clients.
    # -t 2 cuts that short. The CLI is capped at 6 s. Inspect decides.
    # The whole stop, including a SIGKILL, stays inside 12 s.
    proc = run_cmd(["docker", "stop", "-t", "2", name], timeout=6)
    stderr = scrub(proc.stderr.strip())
    info = container_info(name, timeout=2)
    if info.get("status") in {"running", "unknown"}:
        log(f"docker stop {name} did not return; killing")
        remain = max(1.0, 12.0 - (time.monotonic() - started) - 1.0)
        kill = run_cmd(["docker", "kill", name], timeout=min(4.0, remain))
        stderr = scrub(kill.stderr.strip()) or stderr
        info = _wait_until_stopped(name, seconds=max(0.5, 12.0 - (time.monotonic() - started)))
    elapsed = time.monotonic() - started
    ok = info.get("status") not in {"running", "unknown", None}
    if ok:
        _stopped.add(name)
    return {
        "ok": ok,
        "action": "stop",
        "stderr": stderr,
        "elapsed_s": round(elapsed, 3),
        **info,
    }


def docker_start(name: str) -> dict:
    info = container_info(name)
    if not info.get("present"):
        return {"ok": False, "action": "start", **info}
    if ALLOWED_CONTAINERS[name] not in info.get("image", ""):
        raise RuntimeError(f"refusing to start {name}")
    log(f"docker start {name}")
    # A stop that is still tearing the task down returns "already exists".
    deadline = time.monotonic() + 30
    proc = None
    while time.monotonic() < deadline:
        try:
            proc = run_cmd(["docker", "start", name], timeout=20)
        except subprocess.TimeoutExpired:
            proc = None
            time.sleep(1.0)
            continue
        if proc.returncode == 0:
            break
        if proc.returncode == 124:
            time.sleep(1.0)
            continue
        text = proc.stderr or ""
        if "already exists" not in text and "is already running" not in text:
            break
        time.sleep(1.0)
    if proc is not None and proc.returncode == 0:
        _stopped.discard(name)
    stderr = "" if proc is None else scrub(proc.stderr.strip())
    return {
        "ok": proc is not None and proc.returncode == 0,
        "action": "start",
        "stderr": stderr,
        **container_info(name),
    }


def restore_containers() -> list[dict]:
    restored = []
    for name in ("cnc-nats-1", "cnc-redis"):
        try:
            info = container_info(name)
        except Exception as exc:
            restored.append({"name": name, "ok": False, "error": str(exc)})
            continue
        if info.get("present") and info.get("status") != "running":
            restored.append(docker_start(name))
        else:
            restored.append({"name": name, "ok": True, "status": info.get("status"), "action": "left"})
    return restored


def probe_host() -> str:
    """NATS and Redis are on this host. Inside cnc-fleet that is the gateway, not loopback."""
    return os.environ.get("CNC_PROBE_HOST", "127.0.0.1")


def dashboard_host() -> str:
    """0.0.0.0 inside the fleet container. The local sim server stays on 127.0.0.1."""
    chosen = os.environ.get("CNC_HTTP_HOST", "").strip()
    if chosen:
        return chosen
    if os.environ.get("CNC_FLEET"):
        return "0.0.0.0"
    return "127.0.0.1"


def port_open(port: int, host: str = "127.0.0.1") -> bool:
    try:
        with socket.create_connection((host, port), 0.4):
            return True
    except OSError:
        return False


def wait_port(port: int, timeout: float, host: str = "127.0.0.1") -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if port_open(port, host):
            return True
        time.sleep(0.3)
    return False


def wait_port_free(port: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not port_open(port):
            return True
        time.sleep(0.2)
    return False


def git_facts() -> dict:
    head = run_cmd(["git", "rev-parse", "HEAD"], timeout=10)
    status = run_cmd(["git", "status", "--porcelain"], timeout=10)
    branch = run_cmd(["git", "status", "-sb"], timeout=10)
    dirty = [line for line in status.stdout.splitlines() if line.strip()]
    return {
        "commit": head.stdout.strip() if head.returncode == 0 else None,
        "branch_status": branch.stdout.splitlines()[0] if branch.stdout else "",
        "dirty": bool(dirty),
        "dirty_count": len(dirty),
    }


def package_versions() -> dict:
    import importlib.metadata as metadata

    names = (
        "fastapi",
        "uvicorn",
        "pydantic",
        "redis",
        "duckdb",
        "nats-py",
        "httpx",
        "websockets",
        "polars",
        "sqlalchemy",
        "psycopg",
    )
    found = {}
    for name in names:
        try:
            found[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            found[name] = None
    return found


def environment(args) -> dict:
    containers = {}
    for name in ("cnc-nats-1", "cnc-redis", "cnc-timescale-1"):
        if name in ALLOWED_CONTAINERS or name == "cnc-timescale-1":
            try:
                if name == "cnc-timescale-1":
                    proc = run_cmd(
                        ["docker", "inspect", "-f", "{{.State.Status}}\t{{.Config.Image}}", name],
                        timeout=15,
                    )
                    if proc.returncode == 0:
                        status, image = proc.stdout.strip().split("\t", 1)
                        containers[name] = {"status": status, "image": image}
                    else:
                        containers[name] = {"present": False}
                else:
                    containers[name] = container_info(name)
            except Exception as exc:
                containers[name] = {"error": str(exc)}
    return {
        "measured_on": "this laptop",
        "radio_path": (
            f"fleet file {os.environ['CNC_FLEET']}"
            if os.environ.get("CNC_FLEET")
            else "in-process sim, not field radios or SITL"
        ),
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "machine": platform.machine(),
        "packages": package_versions(),
        "git": git_facts(),
        "containers": containers,
        "CNC_BUS": "nats",
        "STORAGE_BACKEND": os.environ.get("STORAGE_BACKEND", "duckdb"),
        "duckdb_path": os.environ.get("CNC_DUCKDB_PATH", "data/cnc.duckdb"),
        "snapshot_path": os.environ.get("CNC_SNAPSHOT_PATH", "data/cnc.db"),
        "port": args.port,
        "http_bind": dashboard_host(),
        "scale_cruise_s": args.plan.cruise_s,
        "nats_down_s": args.plan.nats_down_s,
        "reportable": args.plan.reportable,
        "quick": args.quick,
    }


class Plan:
    def __init__(
        self,
        cruise_s: float,
        nats_down_s: float,
        nats_recover_s: float,
        lost_wait_s: float,
        redis_down_s: float,
        goto_every_s: float,
        alert_every_s: float,
        observe_s: float,
        reportable: bool,
    ) -> None:
        self.cruise_s = cruise_s
        self.nats_down_s = nats_down_s
        self.nats_recover_s = nats_recover_s
        self.lost_wait_s = lost_wait_s
        self.redis_down_s = redis_down_s
        self.goto_every_s = goto_every_s
        self.alert_every_s = alert_every_s
        self.observe_s = observe_s
        self.reportable = reportable


def build_plan(args) -> Plan:
    if args.quick:
        return Plan(25, 5, 12, 3, 8, 5, 8, 4, False)
    return Plan(660, 30, 45, 32, 15, 20, 45, 12, True)


def _merge_rows(current, incoming) -> list[dict]:
    by_id = {row.get("id"): dict(row) for row in (current or []) if isinstance(row, dict) and row.get("id")}
    for row in incoming or []:
        if not isinstance(row, dict) or not row.get("id"):
            continue
        by_id[row["id"]] = {**by_id.get(row["id"], {}), **row}
    return list(by_id.values())


def _merge_frame(latest: dict | None, frame: dict) -> dict:
    """Fold a per-asset delta into the last full picture. A snapshot replaces it."""
    if frame.get("type") != "delta" or not latest:
        return frame
    merged = dict(latest)
    if frame.get("timestamp"):
        merged["timestamp"] = frame["timestamp"]
    if "assets" in frame:
        merged["assets"] = _merge_rows(merged.get("assets"), frame.get("assets"))
    if "tracks" in frame:
        merged["tracks"] = _merge_rows(merged.get("tracks"), frame.get("tracks"))
    for key in ("alerts", "missions", "recent_commands", "zones"):
        if key in frame:
            merged[key] = frame[key]
    return merged


class CopClient:
    """One dashboard socket. Latency samples are taken only while measuring."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.latest: dict | None = None
        self.frames = 0
        self.telem_ms: list[float] = []
        self.negative = 0
        self.measure_telem = False
        self._stages: dict[str, list[float]] = {}
        self._seen: dict[str, datetime] = {}
        self.watch_id: str | None = None
        self.alert_event = asyncio.Event()
        self.alert_hit: dict | None = None
        self.task: asyncio.Task | None = None
        self.error: str | None = None

    async def start(self) -> None:
        self.task = asyncio.create_task(self._run(), name="cop-client")
        deadline = time.monotonic() + 20
        while self.latest is None and time.monotonic() < deadline:
            if self.error:
                raise RuntimeError(self.error)
            await asyncio.sleep(0.05)
        if self.latest is None:
            raise RuntimeError("dashboard socket produced no frame")

    async def stop(self) -> None:
        task = self.task
        self.task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _run(self) -> None:
        import websockets

        try:
            async with websockets.connect(self.url, open_timeout=10, max_size=8_000_000) as ws:
                async for raw in ws:
                    recv = datetime.now(timezone.utc)
                    try:
                        frame = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    self.latest = _merge_frame(self.latest, frame)
                    self.frames += 1
                    self._telem(frame, recv)
                    self._alert(frame, recv)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.error = f"{exc.__class__.__name__}: {exc}"

    def _telem(self, frame: dict, recv: datetime) -> None:
        if not self.measure_telem:
            return
        for asset in frame.get("assets") or []:
            asset_id = asset.get("id")
            seen = parse_dt(asset.get("last_seen"))
            if not asset_id or seen is None:
                continue
            previous = self._seen.get(asset_id)
            self._seen[asset_id] = seen
            if previous is None or seen <= previous:
                continue
            delta_ms = (recv - seen).total_seconds() * 1000.0
            if delta_ms < 0:
                self.negative += 1
                continue
            self.telem_ms.append(delta_ms)
            kind = str(asset.get("asset_type") or "other").lower()
            self._add_stage("all", "poll_to_recv", delta_ms)
            if kind in {"uav", "ugv"}:
                self._add_stage(kind, "poll_to_recv", delta_ms)
            stages = asset.get("stages") if isinstance(asset.get("stages"), dict) else {}
            recv_ms = recv.timestamp() * 1000.0
            source = _stage_num(stages.get("source_ms"))
            adapter = _stage_num(stages.get("adapter_ms"))
            poll = _stage_num(stages.get("poll_ms"))
            frame = _stage_num(stages.get("frame_ms"))
            gap = _stage_num(stages.get("message_gap_ms"))
            self._add_stage_both(kind, "source_to_adapter", _stage_span(source, adapter))
            self._add_stage_both(kind, "adapter_to_poll", _stage_span(adapter, poll))
            self._add_stage_both(kind, "poll_to_frame", _stage_span(poll, frame))
            self._add_stage_both(kind, "frame_to_recv", _stage_span(frame, recv_ms))
            self._add_stage_both(kind, "message_gap", gap)

    def _add_stage(self, kind: str, name: str, value: float | None) -> None:
        if value is None:
            return
        self._stages.setdefault(f"{kind}:{name}", []).append(value)

    def _add_stage_both(self, kind: str, name: str, value: float | None) -> None:
        self._add_stage("all", name, value)
        if kind in {"uav", "ugv"}:
            self._add_stage(kind, name, value)

    def stage_report(self) -> dict:
        names = (
            "source_to_adapter",
            "adapter_to_poll",
            "poll_to_frame",
            "frame_to_recv",
            "poll_to_recv",
            "message_gap",
        )
        report = {}
        for kind in ("all", "uav", "ugv"):
            report[kind] = {name: stats(self._stages.get(f"{kind}:{name}") or []) for name in names}
        return report

    def _alert(self, frame: dict, recv: datetime) -> None:
        wanted = self.watch_id
        if not wanted:
            return
        for alert in frame.get("alerts") or []:
            if alert.get("id") != wanted:
                continue
            raised = parse_dt(alert.get("timestamp"))
            self.alert_hit = {
                "id": wanted,
                "alert_timestamp": alert.get("timestamp"),
                "received_at": recv.isoformat(),
                "raise_to_frame_ms": (
                    (recv - raised).total_seconds() * 1000.0 if raised else None
                ),
                "title": alert.get("title"),
                "message": alert.get("message"),
                "level": alert.get("level"),
                "source": alert.get("source"),
            }
            self.alert_event.set()
            return

    def asset(self, asset_id: str) -> dict | None:
        if not self.latest:
            return None
        for asset in self.latest.get("assets") or []:
            if asset.get("id") == asset_id:
                return asset
        return None

    def fleet(self) -> list[dict]:
        return [card for asset_id in FLEET if (card := self.asset(asset_id))]


class AppServer:
    def __init__(self, port: int) -> None:
        self.port = port
        self.host = dashboard_host()
        self.server = None
        self.thread: threading.Thread | None = None

    def start(self) -> None:
        import uvicorn

        self.host = dashboard_host()
        config = uvicorn.Config(
            "src.operator_app.app:app",
            host=self.host,
            port=self.port,
            log_level="warning",
            access_log=False,
        )
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, name="cnc-acceptance")
        self.thread.start()

    def stop(self) -> None:
        if self.server is not None:
            self.server.should_exit = True
        if self.thread is not None:
            self.thread.join(25)
            if self.thread.is_alive() and self.server is not None:
                self.server.force_exit = True
                self.thread.join(10)
        self.server = None
        self.thread = None


def app_loop():
    from src.operator_app.live_feed import hub

    loop = hub.loop
    if loop is None or not loop.is_running():
        raise RuntimeError("operator loop is not running")
    return loop


async def on_app(fn, timeout: float = 30):
    loop = app_loop()

    async def _wrap():
        return fn()

    future = asyncio.run_coroutine_threadsafe(_wrap(), loop)
    return await asyncio.to_thread(future.result, timeout)


def close_duckdb() -> None:
    try:
        from src.data_services.db import db
    except Exception:
        return
    try:
        db.close()
    except Exception as exc:
        log(f"duckdb close: {exc}")


def command_evidence(command_id: str, asset_id: str | None = None) -> dict:
    from src.data_services.db import db
    from src.operator_app.adapters.idempotency import recall
    from src.operator_app.app import platform

    events = []
    try:
        rows = db.con.execute(
            """
            SELECT event_id, CAST(timestamp AS VARCHAR), event_type, asset_id, message, data
            FROM events
            WHERE json_extract_string(data, '$.command_id') = ?
            ORDER BY timestamp
            """,
            [command_id],
        ).fetchall()
        for row in rows:
            events.append(
                {
                    "event_id": row[0],
                    "timestamp": row[1],
                    "event_type": row[2],
                    "asset_id": row[3],
                    "message": row[4],
                    "data": row[5],
                }
            )
    except Exception as exc:
        events.append({"error": str(exc)})
    actions = [
        action.model_dump(mode="json")
        for action in platform.operator_actions
        if action.command_id == command_id
    ]
    cmd = platform.store.commands.get(command_id)
    body = cmd.model_dump(mode="json") if cmd is not None else None
    if cmd is not None:
        asset_id = cmd.asset_id
    remembered = None
    target = None
    if asset_id:
        hit = recall(asset_id, command_id)
        if hit is not None:
            remembered = {"ok": bool(hit[0]), "reason": hit[1]}
        adapter = platform.adapters.get(asset_id)
        sim = getattr(adapter, "sim", None) if adapter is not None else None
        sim_target = getattr(sim, "target", None) if sim is not None else None
        if sim_target is not None:
            target = {"lat": sim_target.lat, "lon": sim_target.lon, "alt": sim_target.alt}
    outbox = None
    try:
        row = db.con.execute(
            """
            SELECT id, subject, CAST(created_at AS VARCHAR), CAST(published_at AS VARCHAR), attempts
            FROM outbox
            WHERE json_extract_string(payload, '$.id') = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [command_id],
        ).fetchone()
        if row is not None:
            outbox = {
                "id": row[0],
                "subject": row[1],
                "created_at": row[2],
                "published_at": row[3],
                "attempts": row[4],
            }
    except Exception as exc:
        outbox = {"error": str(exc)}
    return {
        "command": body,
        "events": events,
        "operator_actions": actions,
        "recall": remembered,
        "sim_target": target,
        "outbox": outbox,
    }


def compact_asset(asset: dict) -> dict:
    return {
        "id": asset.get("id"),
        "name": asset.get("name"),
        "asset_type": asset.get("asset_type"),
        "status": asset.get("status"),
        "lat": asset.get("lat"),
        "lon": asset.get("lon"),
        "alt": asset.get("alt"),
        "home_lat": asset.get("home_lat"),
        "home_lon": asset.get("home_lon"),
        "last_seen": asset.get("last_seen"),
    }


async def wait_health(client, timeout: float | None = None) -> dict:
    if timeout is None:
        timeout = 360.0 if os.environ.get("CNC_FLEET") else 40.0
    deadline = time.monotonic() + timeout
    last = "no response"
    while time.monotonic() < deadline:
        try:
            response = await client.get("/health")
            if response.status_code == 200:
                return response.json()
            last = f"HTTP {response.status_code}"
        except Exception as exc:
            last = str(exc)
        await asyncio.sleep(0.4)
    raise RuntimeError(f"health did not come up: {last}")


async def post_json(client, path: str, body: dict | None = None, timeout: float | None = None):
    if timeout is None:
        timeout = 45.0 if os.environ.get("CNC_FLEET") else 20.0
    started = time.perf_counter()
    try:
        response = await client.post(path, json=body, timeout=timeout)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        try:
            payload = response.json()
        except Exception:
            payload = {"raw": response.text[:500]}
        return response.status_code, payload, elapsed_ms, None
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return 0, None, elapsed_ms, f"{exc.__class__.__name__}: {exc}"


def acked(payload: dict | None) -> bool:
    if not isinstance(payload, dict):
        return False
    result = payload.get("result") or {}
    return payload.get("status") == "sent" and bool(result.get("ack"))


async def issue_command(client, asset_id: str, kind: str, parameters: dict, samples: list) -> dict:
    code, payload, elapsed_ms, error = await post_json(
        client,
        f"/assets/{asset_id}/command",
        {"command_type": kind, "parameters": parameters},
    )
    row = {
        "asset_id": asset_id,
        "command_type": kind,
        "parameters": parameters,
        "http_status": code,
        "elapsed_ms": elapsed_ms,
        "error": error,
        "command_id": payload.get("id") if isinstance(payload, dict) else None,
        "status": payload.get("status") if isinstance(payload, dict) else None,
        "result": payload.get("result") if isinstance(payload, dict) else None,
        "issued_by": payload.get("issued_by") if isinstance(payload, dict) else None,
        "issued_at": payload.get("issued_at") if isinstance(payload, dict) else None,
        "acked": acked(payload),
    }
    if kind == "goto" and row["acked"]:
        samples.append(elapsed_ms)
    if row["command_id"]:
        try:
            row["evidence"] = await on_app(lambda cid=row["command_id"]: command_evidence(cid))
        except Exception as exc:
            row["evidence_error"] = str(exc)
    return row


def pose(asset: dict | None) -> tuple[float, float] | None:
    if not asset or asset.get("lat") is None or asset.get("lon") is None:
        return None
    return float(asset["lat"]), float(asset["lon"])


def moved_m(before: tuple[float, float], after: tuple[float, float]) -> float:
    # Equirectangular meters, fine for a few kilometres around Hyderabad.
    mean_lat = math.radians((before[0] + after[0]) / 2.0)
    dy = math.radians(after[0] - before[0]) * 6_371_000.0
    dx = math.radians(after[1] - before[1]) * 6_371_000.0 * math.cos(mean_lat)
    return math.hypot(dx, dy)


async def measure_alert(cop: CopClient, asset_id: str) -> dict:
    asset = cop.asset(asset_id)
    point = pose(asset)
    if point is None:
        return {"ok": False, "reason": "asset has no position", "asset_id": asset_id}
    zone_id = f"m5{time.time_ns() % 10**10}"
    alert_id = f"zone-{zone_id}-{asset_id}"
    cop.watch_id = alert_id
    cop.alert_hit = None
    cop.alert_event.clear()

    def _add():
        from src.data_services.repositories import get_repositories
        from src.kernel.models import Zone
        from src.operator_app.app import platform

        asset = platform.store.assets.get(asset_id)
        if asset is None:
            return None
        lat = float(asset.lat)
        lon = float(asset.lon)
        delta = 0.002
        get_repositories().zones.add(
            Zone(
                id=zone_id,
                name="m5 keep-out",
                kind="keep-out",
                polygon=[
                    {"lat": lat - delta, "lon": lon - delta},
                    {"lat": lat - delta, "lon": lon + delta},
                    {"lat": lat + delta, "lon": lon + delta},
                    {"lat": lat + delta, "lon": lon - delta},
                ],
            )
        )
        return {"lat": lat, "lon": lon}

    def _remove():
        from src.data_services.repositories import get_repositories

        get_repositories().zones.remove(zone_id)

    started = time.perf_counter()
    armed = None
    try:
        armed = await on_app(_add)
        if not armed:
            return {"ok": False, "reason": "asset missing from the operator store", "asset_id": asset_id}
        await asyncio.wait_for(cop.alert_event.wait(), timeout=8)
        condition_ms = (time.perf_counter() - started) * 1000.0
        hit = dict(cop.alert_hit or {})
        hit["zone_insert_to_frame_ms"] = condition_ms
        hit["ok"] = True
        hit["asset_id"] = asset_id
        return hit
    except asyncio.TimeoutError:
        def _seen():
            from src.operator_app.app import platform

            alert = platform.store.alerts.get(alert_id)
            return None if alert is None else {"id": alert.id, "message": alert.message}

        try:
            stored = await on_app(_seen)
        except Exception as exc:
            stored = {"error": str(exc)}
        return {
            "ok": False,
            "reason": "alert id did not appear on the dashboard socket within 8s",
            "alert_id": alert_id,
            "asset_id": asset_id,
            "armed_at": armed,
            "stored_alert": stored,
        }
    finally:
        cop.watch_id = None
        try:
            await on_app(_remove)
        except Exception as exc:
            log(f"zone remove {zone_id}: {exc}")


async def cruise(client, cop: CopClient, plan: Plan, goto_samples: list, alert_samples: list, insert_samples: list) -> None:
    log(f"cruise {plan.cruise_s:.0f}s")
    end = time.monotonic() + plan.cruise_s
    next_goto = time.monotonic()
    next_alert = time.monotonic() + min(3.0, plan.alert_every_s / 2.0)
    next_log = time.monotonic() + 20
    index = 0
    while time.monotonic() < end:
        now = time.monotonic()
        if now >= next_goto:
            asset_id = FLEET[index % len(FLEET)]
            index += 1
            card = cop.asset(asset_id) or {}
            lat = float(card.get("lat") or 17.39) + (0.002 if index % 2 else -0.002)
            lon = float(card.get("lon") or 78.49) + (0.0015 if index % 2 else -0.0015)
            alt = 70 if asset_id in UAVS else 0
            row = await issue_command(client, asset_id, "goto", {"lat": lat, "lon": lon, "alt": alt}, goto_samples)
            log(f"goto {asset_id} acked={row['acked']} {row['elapsed_ms']:.0f}ms status={row['status']}")
            next_goto = time.monotonic() + plan.goto_every_s
        if time.monotonic() >= next_alert and time.monotonic() < end:
            asset_id = UAVS[index % len(UAVS)]
            sample = await measure_alert(cop, asset_id)
            if sample.get("ok") and sample.get("raise_to_frame_ms") is not None:
                alert_samples.append(sample["raise_to_frame_ms"])
            if sample.get("ok") and sample.get("zone_insert_to_frame_ms") is not None:
                insert_samples.append(sample["zone_insert_to_frame_ms"])
            log(
                "alert "
                f"{asset_id} ok={sample.get('ok')} "
                f"raise_ms={sample.get('raise_to_frame_ms')} "
                f"insert_ms={sample.get('zone_insert_to_frame_ms')}"
            )
            next_alert = time.monotonic() + plan.alert_every_s
        if time.monotonic() >= next_log:
            left = max(0.0, end - time.monotonic())
            log(f"cruise remaining {left:.0f}s telem_n={len(cop.telem_ms)} goto_n={len(goto_samples)} alert_n={len(alert_samples)} frames={cop.frames}")
            next_log = time.monotonic() + 30
        await asyncio.sleep(0.2)


def track_for(tracks: list[dict], asset_id: str, name: str) -> dict | None:
    for track in tracks:
        sources = track.get("source_asset_ids") or []
        if asset_id in sources or track.get("label") == name:
            if str(track.get("id", "")).startswith("det-"):
                continue
            return track
    return None


def lost_alerts(alerts: list[dict]) -> list[dict]:
    found = []
    for alert in alerts:
        text = " ".join(
            str(alert.get(key) or "")
            for key in ("title", "message", "source", "level")
        ).lower()
        if "lost" in text:
            found.append(
                {
                    "id": alert.get("id"),
                    "title": alert.get("title"),
                    "message": alert.get("message"),
                    "source": alert.get("source"),
                    "level": alert.get("level"),
                }
            )
    return found


async def fault_nats(client, plan: Plan, goto_samples: list) -> dict:
    """Stop NATS, issue one GOTO, hold the outage, start NATS, watch delivery."""
    expected = (
        "The GOTO stays pending while NATS is down, then the outbox publishes "
        "it once and the adapter executes it once after NATS returns."
    )
    stop = docker_stop("cnc-nats-1")
    if not stop.get("ok"):
        return {"name": "nats_down", "expected": expected, "actual": "NATS was not stopped", "stop": stop}
    down_at = time.monotonic()
    queued = await issue_command(
        client,
        QUEUED_ASSET,
        "goto",
        {"lat": 17.4200, "lon": 78.5200, "alt": 0},
        goto_samples,
    )
    # This sample was taken while the broker was down. Drop it if the helper
    # recorded an ack that cannot belong to the outage window.
    if queued.get("acked") and goto_samples:
        goto_samples.pop()
    remaining = plan.nats_down_s - (time.monotonic() - down_at)
    if remaining > 0:
        log(f"nats stays down {remaining:.1f}s")
        await asyncio.sleep(remaining)
    down_s = time.monotonic() - down_at
    start = docker_start("cnc-nats-1")
    port_up = wait_port(4222, 20, probe_host()) if start.get("ok") else False
    log("waiting for queued command after nats start")
    deadline = time.monotonic() + plan.nats_recover_s
    last = queued.get("evidence")
    while time.monotonic() < deadline:
        await asyncio.sleep(1.0)
        if not queued.get("command_id"):
            break
        try:
            last = await on_app(lambda cid=queued["command_id"]: command_evidence(cid))
        except Exception as exc:
            last = {"error": str(exc)}
            continue
        status = (last.get("command") or {}).get("status")
        published = (last.get("outbox") or {}).get("published_at")
        if status and status != "pending" and published:
            break
    command = (last or {}).get("command") or {}
    outbox = (last or {}).get("outbox") or {}
    actual = (
        f"HTTP status {queued.get('status')} result {queued.get('result')}. "
        f"After NATS listened again, command status was {command.get('status')} "
        f"result {command.get('result')}, outbox published_at {outbox.get('published_at')}, "
        f"attempts {outbox.get('attempts')}, recall {((last or {}).get('recall'))}, "
        f"sim target {((last or {}).get('sim_target'))}."
    )
    delivered = command.get("status") == "sent" and (command.get("result") or {}).get("ack") is True
    once = (last or {}).get("recall") is not None and outbox.get("published_at") not in (None, "None")
    return {
        "name": "nats_down",
        "expected": expected,
        "actual": actual,
        "matched_expected": bool(queued.get("status") == "pending" and delivered and once),
        "down_s": down_s,
        "port_4222_after_start": port_up,
        "stop": stop,
        "start": start,
        "issued": {key: queued.get(key) for key in ("command_id", "status", "result", "issued_by", "issued_at", "http_status", "error", "elapsed_ms")},
        "after_restart_broker": last,
    }


async def fault_adapter(client, plan: Plan) -> dict:
    expected = "The stopped vehicle is marked lost and an Alert is raised."
    snap = (await client.get("/snapshot")).json()
    before_ids = {alert.get("id") for alert in snap.get("alerts") or []}
    name = None
    for asset in snap.get("assets") or []:
        if asset.get("id") == KILLED_ASSET:
            name = asset.get("name")
    log(f"stop adapter {KILLED_ASSET}")
    loop = app_loop()

    async def _do_stop():
        from src.operator_app.app import platform

        adapter = platform.adapters.get(KILLED_ASSET)
        if adapter is None:
            return {"stopped": False, "reason": "adapter missing"}
        await adapter.stop()
        asset = platform.store.assets.get(KILLED_ASSET)
        return {
            "stopped": True,
            "asset_status": asset.status.value if asset is not None else None,
        }

    future = asyncio.run_coroutine_threadsafe(_do_stop(), loop)
    stopped = await asyncio.to_thread(future.result, 20)
    immediate = (await client.get("/assets")).json()
    immediate_asset = next((item for item in immediate if item.get("id") == KILLED_ASSET), None)
    tracks_now = (await client.get("/tracks")).json()
    immediate_track = track_for(tracks_now, KILLED_ASSET, name or "Wolf-2")
    log(f"waiting {plan.lost_wait_s:.0f}s for track lifecycle")
    await asyncio.sleep(plan.lost_wait_s)
    later_assets = (await client.get("/assets")).json()
    later_asset = next((item for item in later_assets if item.get("id") == KILLED_ASSET), None)
    later_tracks = (await client.get("/tracks")).json()
    later_track = track_for(later_tracks, KILLED_ASSET, name or "Wolf-2")
    snap = (await client.get("/snapshot")).json()
    new_alerts = [alert for alert in snap.get("alerts") or [] if alert.get("id") not in before_ids]
    lost = lost_alerts(list(snap.get("alerts") or []))
    track_state = (later_track or {}).get("state")
    asset_status = (later_asset or {}).get("status")
    actual = (
        f"Immediately after stop, asset status was {(immediate_asset or {}).get('status')} "
        f"and the friendly track state was {(immediate_track or {}).get('state')}. "
        f"After {plan.lost_wait_s:.0f}s, asset status was {asset_status} and track state was {track_state}. "
        f"Alerts whose text contains lost: {len(lost)}. New alerts since the stop: {len(new_alerts)}."
    )
    return {
        "name": "adapter_stop",
        "expected": expected,
        "actual": actual,
        "matched_expected": asset_status == "lost" and bool(lost),
        "stop_call": stopped,
        "immediate": {
            "asset_status": (immediate_asset or {}).get("status"),
            "track_id": (immediate_track or {}).get("id"),
            "track_state": (immediate_track or {}).get("state"),
        },
        "after_wait": {
            "asset_status": asset_status,
            "track_id": (later_track or {}).get("id"),
            "track_state": track_state,
            "track_label": (later_track or {}).get("label"),
        },
        "lost_alerts": lost,
        "new_alert_ids": [alert.get("id") for alert in new_alerts][:20],
        "before_alert_count": len(before_ids),
    }


async def fault_redis(client, cop: CopClient, plan: Plan) -> dict:
    expected = (
        "With Redis unreachable, the picture keeps serving from the operator store. "
        "After Redis returns, the overlay ping succeeds again."
    )
    before = cop.asset("uav-01") or {}
    before_pose = {"lat": before.get("lat"), "lon": before.get("lon"), "last_seen": before.get("last_seen")}
    stop = docker_stop("cnc-redis")
    if not stop.get("ok"):
        return {"name": "redis_down", "expected": expected, "actual": "Redis was not stopped", "stop": stop}
    log(f"redis stays down {plan.redis_down_s:.0f}s")
    await asyncio.sleep(plan.redis_down_s)
    code, payload, elapsed_ms, error = await post_json(
        client, "/assets/uav-02/command", {"command_type": "hold", "parameters": {}}
    )
    await asyncio.sleep(1.0)
    frame = cop.latest or {}
    flags = await on_app(_redis_flags)
    cards = [compact_asset(asset) for asset in (frame.get("assets") or []) if asset.get("id") in FLEET]
    uav = next((card for card in cards if card["id"] == "uav-01"), None)
    moved = None
    if uav and before_pose["lat"] is not None:
        moved = moved_m(
            (float(before_pose["lat"]), float(before_pose["lon"])),
            (float(uav["lat"]), float(uav["lon"])),
        )
    start = docker_start("cnc-redis")
    redis_up = wait_port(6379, 15, probe_host()) if start.get("ok") else False
    flags_after: dict = {}
    resume_deadline = time.monotonic() + 8
    while time.monotonic() < resume_deadline:
        await asyncio.sleep(0.5)
        try:
            flags_after = await on_app(_redis_flags)
        except Exception as exc:
            flags_after = {"error": str(exc)}
        if flags_after.get("redis_down") is False and flags_after.get("overlay_loaded"):
            break
    await post_json(client, "/assets/uav-02/command", {"command_type": "hold", "parameters": {}})
    await asyncio.sleep(0.5)
    try:
        flags_after = await on_app(_redis_flags)
    except Exception as exc:
        flags_after = {"error": str(exc)}
    frame_after = cop.latest or {}
    still_serving = len([a for a in (frame.get("assets") or []) if a.get("id") in FLEET]) >= 4
    actual = (
        f"After Redis was stopped, a HOLD still produced an HTTP {code} and the socket frame "
        f"contained {len(cards)} fleet assets (redis_down flag {flags.get('redis_down')}). "
        f"uav-01 moved about {None if moved is None else round(moved, 1)} m on that frame relative "
        f"to the pre-outage fix. Storage backend for this process is "
        f"{os.environ.get('STORAGE_BACKEND', 'duckdb')}. cop_frame does not query Postgres. "
        f"After Redis was started again, redis_down flag was {flags_after.get('redis_down')} "
        f"and the socket still had {sum(1 for a in (frame_after.get('assets') or []) if a.get('id') in FLEET)} fleet assets."
    )
    return {
        "name": "redis_down",
        "expected": expected,
        "actual": actual,
        "matched_expected": bool(
            still_serving
            and redis_up
            and flags_after.get("redis_down") is False
            and flags_after.get("overlay_loaded")
        ),
        "fell_back_to_postgres": False,
        "frames_continued_from_entity_store": still_serving,
        "stop": stop,
        "start": start,
        "port_6379_after_start": redis_up,
        "hold": {"http_status": code, "elapsed_ms": elapsed_ms, "error": error, "body_status": (payload or {}).get("status") if isinstance(payload, dict) else None},
        "before_pose": before_pose,
        "during_cards": cards,
        "uav01_moved_m": moved,
        "redis_flags_during": flags,
        "redis_flags_after_start": flags_after,
        "frame_keys": sorted(frame.keys()),
    }


def _redis_flags() -> dict:
    from src.operator_app import live_feed

    return {
        "redis_down": bool(live_feed._redis_down),
        "listener_started": bool(live_feed._listener_started),
        "overlay_loaded": bool(live_feed._overlay_loaded),
        "subscribers": live_feed.hub.subscribers,
    }


async def restart_app(server: AppServer, cop: CopClient, client, expect: dict, args) -> dict:
    expected = "After the process restarts, operator state is restored."
    log("restart: snapshot then stop the app")
    snap = (await client.get("/snapshot")).json()
    expect = {
        **expect,
        "asset_ids": [asset.get("id") for asset in snap.get("assets") or [] if asset.get("id") in FLEET],
        "mission_ids": [mission.get("id") for mission in snap.get("missions") or []],
        "track_ids": [track.get("id") for track in snap.get("tracks") or []],
        "alert_ids": [alert.get("id") for alert in snap.get("alerts") or []],
        "command_ids": [cmd.get("id") for cmd in snap.get("recent_commands") or []],
    }
    expect_path = Path("/tmp/m5_expect.json")
    result_path = Path("/tmp/m5_recover.json")
    expect_path.write_text(json.dumps(expect, default=str), encoding="utf-8")
    if result_path.exists():
        result_path.unlink()
    await cop.stop()
    await client.aclose()
    server.stop()
    if not wait_port_free(args.port, 15):
        log("port still busy after stop")
    close_duckdb()
    log("restart: child process")
    proc = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--phase",
            "recover",
            "--port",
            str(args.port),
            "--expect",
            str(expect_path),
            "--result",
            str(result_path),
        ],
        cwd=ROOT,
        env={**os.environ, "CNC_BUS": "nats", "PYTHONUNBUFFERED": "1"},
        capture_output=True,
        text=True,
        timeout=150,
        check=False,
    )
    child = {}
    if result_path.exists():
        child = json.loads(result_path.read_text(encoding="utf-8"))
    actual_bits = child.get("actual") or scrub(proc.stderr[-1500:])
    return {
        "name": "process_restart",
        "expected": expected,
        "actual": child.get("summary") or actual_bits,
        "matched_expected": bool(
            child.get("assets_back")
            and child.get("mission_back")
            and child.get("commands_reloaded")
            and isinstance(child.get("event_ring_count"), int)
            and child.get("event_ring_count") > 0
        ),
        "child_returncode": proc.returncode,
        "child_stdout_tail": scrub(proc.stdout[-2500:]),
        "child_stderr_tail": scrub(proc.stderr[-2500:]),
        "child": child,
        "parent_expected": {
            "mission_ids": expect.get("mission_ids"),
            "asset_ids": expect.get("asset_ids"),
            "queued_command_id": expect.get("queued_command_id"),
        },
    }


async def recover_phase(args) -> int:
    """Second process. Load the snapshot and see whether the queued command runs once."""
    os.chdir(ROOT)
    sys.path.insert(0, str(ROOT))
    os.environ["CNC_BUS"] = "nats"
    expect = json.loads(Path(args.expect).read_text(encoding="utf-8"))
    server = AppServer(args.port)
    summary: dict = {"expect_mission_ids": expect.get("mission_ids"), "expect_assets": expect.get("asset_ids")}
    client = None
    try:
        server.start()
        import httpx

        client = httpx.AsyncClient(base_url=f"http://127.0.0.1:{args.port}", timeout=20)
        health = await wait_health(client)
        assets = (await client.get("/assets")).json()
        missions = (await client.get("/missions")).json()
        tracks = (await client.get("/tracks")).json()
        snap = (await client.get("/snapshot")).json()
        asset_ids = {asset.get("id") for asset in assets}
        mission_ids = {mission.get("id") for mission in missions}
        track_ids = {track.get("id") for track in tracks}
        wanted_assets = set(expect.get("asset_ids") or [])
        wanted_missions = set(expect.get("mission_ids") or [])
        summary["health_db"] = health.get("db")
        summary["assets_back"] = wanted_assets <= asset_ids
        summary["mission_back"] = bool(wanted_missions) and wanted_missions <= mission_ids
        summary["tracks_back"] = len(set(expect.get("track_ids") or []) & track_ids)
        summary["alerts_back"] = len(set(expect.get("alert_ids") or []) & {a.get("id") for a in snap.get("alerts") or []})
        summary["alert_count"] = len(snap.get("alerts") or [])
        summary["recent_command_ids"] = [cmd.get("id") for cmd in snap.get("recent_commands") or []]
        summary["commands_reloaded"] = bool(
            set(expect.get("command_ids") or []) & set(summary["recent_command_ids"])
        )
        summary["asset_cards"] = [compact_asset(asset) for asset in assets if asset.get("id") in FLEET]

        # Give the fresh relay time to publish a command the parent left unpublished.
        queued_id = expect.get("queued_command_id")
        queued_asset = expect.get("queued_asset_id")
        evidence = None
        if queued_id:
            deadline = time.monotonic() + 25
            while time.monotonic() < deadline:
                # hub.loop is set once a socket connects. Open one briefly.
                if "socket_ready" not in summary:
                    import websockets

                    try:
                        async with websockets.connect(
                            f"ws://127.0.0.1:{args.port}/ws/cop", open_timeout=8, max_size=2_000_000
                        ) as ws:
                            await asyncio.wait_for(ws.recv(), timeout=8)
                        summary["socket_ready"] = True
                    except Exception as exc:
                        summary["socket_error"] = str(exc)
                try:
                    evidence = await on_app(
                        lambda cid=queued_id, aid=queued_asset: command_evidence(cid, aid)
                    )
                except Exception as exc:
                    evidence = {"error": str(exc)}
                    await asyncio.sleep(1)
                    continue
                if evidence.get("recall") is not None:
                    break
                await asyncio.sleep(1)
        summary["queued_command_id"] = queued_id
        summary["queued_asset_id"] = queued_asset
        summary["queued_after_restart"] = evidence

        def _event_ring():
            from src.data_services.event_store import event_store

            return len(event_store.get_recent(50))

        try:
            summary["event_ring_count"] = await on_app(_event_ring)
        except Exception as exc:
            summary["event_ring_count"] = f"error: {exc}"

        def _durable_event():
            from src.data_services.db import db

            sample_ids = expect.get("event_ids") or []
            found = []
            for event_id in sample_ids:
                row = db.con.execute(
                    "SELECT event_id, CAST(timestamp AS VARCHAR), asset_id, message FROM events WHERE event_id = ?",
                    [event_id],
                ).fetchone()
                if row:
                    found.append({"event_id": row[0], "timestamp": row[1], "asset_id": row[2], "message": row[3]})
            total = db.con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            return {"matched": found, "events_table_count": int(total)}

        try:
            summary["durable_events"] = await on_app(_durable_event)
        except Exception as exc:
            summary["durable_events"] = {"error": str(exc)}

        published = ((evidence or {}).get("outbox") or {}).get("published_at")
        recall = (evidence or {}).get("recall") if isinstance(evidence, dict) else None
        summary["summary"] = (
            f"Restart loaded assets {sorted(wanted_assets & asset_ids)} "
            f"(all expected: {summary['assets_back']}) and missions {sorted(wanted_missions & mission_ids)} "
            f"(all expected: {summary['mission_back']}). "
            f"Tracks still present: {summary['tracks_back']}. Alerts still present: {summary['alerts_back']} "
            f"of the previous id set (snapshot load keeps 30). "
            f"Recent commands after restart: {summary['recent_command_ids'] or 'none'}. "
            f"Parent command ids reappeared: {summary['commands_reloaded']}. "
            f"Event ring count: {summary['event_ring_count']}. "
            f"Queued command outbox published_at: {published}. recall: {recall}."
        )
        code = 0
    except Exception:
        summary["summary"] = "recover process failed"
        summary["error"] = traceback.format_exc()
        code = 1
    finally:
        if client is not None:
            await client.aclose()
        server.stop()
        close_duckdb()
        Path(args.result).write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    log(summary.get("summary") or "recover finished")
    return code


def criterion(name: str, passed: bool, evidence: dict) -> dict:
    return {"id": name, "pass": bool(passed), "evidence": evidence}


async def scenario(args) -> int:
    os.chdir(ROOT)
    sys.path.insert(0, str(ROOT))
    os.environ["CNC_BUS"] = "nats"
    # Leave STORAGE_BACKEND unset so the operator snapshot stays on SQLite/DuckDB.
    os.environ.pop("STORAGE_BACKEND", None)
    result: dict = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "scenario": (
            "Scripted fleet of uav-01 Eagle-1, uav-02 Eagle-2, ugv-01 Wolf-1, ugv-02 Wolf-2. "
            "One HTTP operator and one /ws/cop client. Cruise, then NATS outage, one adapter stop, "
            "Redis outage, and a process restart."
        ),
        "criteria": [],
        "faults": [],
        "goto_samples_ms": [],
        "alert_raise_samples_ms": [],
        "alert_insert_samples_ms": [],
        "fixes": [],
    }
    server: AppServer | None = None
    cop: CopClient | None = None
    client = None
    restart_done = False
    frozen_client = None
    frozen_server = None
    try:
        if port_open(args.port):
            raise RuntimeError(f"127.0.0.1:{args.port} is already in use")
        for name in ALLOWED_CONTAINERS:
            info = container_info(name)
            if not info.get("present") or info.get("status") != "running":
                raise RuntimeError(f"{name} is not running: {info}")
        result["environment"] = environment(args)
        log("starting operator app")
        server = AppServer(args.port)
        server.start()
        import httpx

        client = httpx.AsyncClient(base_url=f"http://127.0.0.1:{args.port}", timeout=20)
        health = await wait_health(client)
        result["health"] = health
        page = await client.get("/")
        result["dashboard_http"] = {
            "status": page.status_code,
            "references_dashboard_js": "dashboard.js" in page.text,
            "bytes": len(page.content),
            "bind_host": server.host,
            "client_url": f"http://127.0.0.1:{args.port}/",
        }
        cop = CopClient(f"ws://127.0.0.1:{args.port}/ws/cop")
        await cop.start()
        cop.measure_telem = True
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and len(cop.fleet()) < 4:
            await asyncio.sleep(0.3)
        fleet = cop.fleet()
        if len(fleet) < 4:
            raise RuntimeError(f"fleet on the socket: {[card.get('id') for card in fleet]}")
        homes = {(card.get("id"), card.get("home_lat"), card.get("home_lon")) for card in fleet}
        types = {}
        for card in fleet:
            types.setdefault(card.get("asset_type"), []).append(card.get("id"))
        start_pose = {card["id"]: pose(card) for card in fleet}
        log("initial GOTO so each vehicle moves")
        for index, asset_id in enumerate(FLEET):
            card = cop.asset(asset_id) or {}
            lat = float(card.get("lat") or 17.39) + 0.0018
            lon = float(card.get("lon") or 78.49) + (0.0012 if index % 2 == 0 else -0.0012)
            alt = 80 if asset_id in UAVS else 0
            row = await issue_command(client, asset_id, "goto", {"lat": lat, "lon": lon, "alt": alt}, result["goto_samples_ms"])
            result.setdefault("opening_commands", []).append(
                {key: row[key] for key in ("asset_id", "command_id", "status", "issued_by", "acked", "elapsed_ms")}
            )
        await asyncio.sleep(args.plan.observe_s)
        end_pose = {asset_id: pose(cop.asset(asset_id)) for asset_id in FLEET}
        movement = {}
        for asset_id in FLEET:
            if start_pose.get(asset_id) and end_pose.get(asset_id):
                movement[asset_id] = round(moved_m(start_pose[asset_id], end_pose[asset_id]), 1)
        distinct_homes = len({(card.get("home_lat"), card.get("home_lon")) for card in fleet}) == 4
        moved_ok = sum(1 for metres in movement.values() if metres >= 15) >= 4
        subscribers = await on_app(_redis_flags)
        result["criteria"].append(
            criterion(
                "10.1",
                len(types.get("uav") or []) >= 2
                and len(types.get("ugv") or []) >= 2
                and distinct_homes
                and moved_ok,
                {
                    "assets": [compact_asset(card) for card in fleet],
                    "homes": sorted(list(homes), key=lambda item: str(item[0])),
                    "movement_m": movement,
                    "frames": cop.frames,
                    "subscribers": subscribers.get("subscribers"),
                },
            )
        )
        log(f"10.1 movement_m={movement}")

        # 10.2 detection hands a track to the nearest UGV through the platform.
        detect_code, detect_body, detect_ms, detect_err = await post_json(
            client, "/assets/uav-01/detect?lat=17.3865&lon=78.4875"
        )
        track_id = detect_body.get("id") if isinstance(detect_body, dict) else None
        task_code, task_body, task_ms, task_err = (0, None, 0, "no track")
        if track_id:
            task_code, task_body, task_ms, task_err = await post_json(
                client, f"/tracks/{track_id}/task-nearest-ugv"
            )
        task_id = task_body.get("id") if isinstance(task_body, dict) else None
        task_evidence = await on_app(lambda: command_evidence(task_id)) if task_id else None
        task_actions = (task_evidence or {}).get("operator_actions") or []
        task_events = (task_evidence or {}).get("events") or []
        info_ok = (
            isinstance(track_id, str)
            and track_id.startswith("det-")
            and isinstance(task_body, dict)
            and task_body.get("asset_id") in UGVS
            and any(action.get("action") == "task-nearest-ugv" for action in task_actions)
            and any(event.get("event_id") for event in task_events if "error" not in event)
            and (task_evidence or {}).get("recall") is not None
        )
        result["criteria"].append(
            criterion(
                "10.2",
                info_ok,
                {
                    "detect_http": detect_code,
                    "detect_ms": detect_ms,
                    "detect_error": detect_err,
                    "track": detect_body if isinstance(detect_body, dict) else detect_body,
                    "task_http": task_code,
                    "task_ms": task_ms,
                    "task_error": task_err,
                    "task_command": {
                        "id": task_id,
                        "asset_id": (task_body or {}).get("asset_id") if isinstance(task_body, dict) else None,
                        "status": (task_body or {}).get("status") if isinstance(task_body, dict) else None,
                        "issued_by": (task_body or {}).get("issued_by") if isinstance(task_body, dict) else None,
                        "issued_at": (task_body or {}).get("issued_at") if isinstance(task_body, dict) else None,
                        "parameters": (task_body or {}).get("parameters") if isinstance(task_body, dict) else None,
                    },
                    "events": [
                        {key: event.get(key) for key in ("event_id", "timestamp", "asset_id", "message")}
                        for event in task_events
                    ],
                    "operator_actions": task_actions,
                    "recall": (task_evidence or {}).get("recall"),
                },
            )
        )
        log(f"10.2 track={track_id} task={task_id} ok={info_ok}")

        # 10.3 C2 through HTTP, plus one mission the restart must find.
        hold = await issue_command(client, "uav-02", "hold", {}, result["goto_samples_ms"])
        mission_code, mission_body, mission_ms, mission_err = await post_json(
            client,
            "/missions",
            {
                "name": "M5 acceptance",
                "description": "Scripted RCI run",
                "assigned_assets": list(FLEET),
                "waypoints": [{"lat": 17.4000, "lon": 78.5000, "alt": 70}],
                "objectives": ["acceptance"],
            },
        )
        mission_id = mission_body.get("id") if isinstance(mission_body, dict) else None
        start_code, start_body, start_ms, start_err = (0, None, 0, "no mission")
        if mission_id:
            start_code, start_body, start_ms, start_err = await post_json(
                client, f"/missions/{mission_id}/start", timeout=40
            )
        c2_commands = [row for row in result.get("opening_commands", [])] + [
            {key: hold.get(key) for key in ("asset_id", "command_id", "status", "issued_by", "acked", "elapsed_ms")}
        ]
        hold_evidence = hold.get("evidence")
        actors = {row.get("issued_by") for row in c2_commands if row.get("issued_by")}
        c2_ok = (
            hold.get("acked")
            and hold.get("issued_by") == ACTOR
            and bool((hold_evidence or {}).get("events"))
            and mission_code == 200
            and start_code == 200
            and (start_body or {}).get("status") == "active"
        )
        result["criteria"].append(
            criterion(
                "10.3",
                c2_ok,
                {
                    "hold": {
                        "command_id": hold.get("command_id"),
                        "status": hold.get("status"),
                        "issued_by": hold.get("issued_by"),
                        "issued_at": hold.get("issued_at"),
                        "acked": hold.get("acked"),
                        "events": [
                            {key: event.get(key) for key in ("event_id", "timestamp", "asset_id", "message")}
                            for event in ((hold_evidence or {}).get("events") or [])
                        ],
                    },
                    "opening_actors": sorted(actors),
                    "mission_http": mission_code,
                    "mission_ms": mission_ms,
                    "mission_error": mission_err,
                    "mission_id": mission_id,
                    "mission_started_http": start_code,
                    "mission_start_ms": start_ms,
                    "mission_start_error": start_err,
                    "mission_status": (start_body or {}).get("status") if isinstance(start_body, dict) else None,
                    "mission_started_at": (start_body or {}).get("started_at") if isinstance(start_body, dict) else None,
                    "mission_actor_note": "Mission start issues GOTO with actor mission. Operator HTTP commands use operator:op-1.",
                },
            )
        )
        log(f"10.3 hold={hold.get('command_id')} mission={mission_id} start={start_code}")

        frame = cop.latest or {}
        subscriber_count = (await on_app(_redis_flags)).get("subscribers")
        operator_actors = sorted(
            {
                row.get("issued_by")
                for row in (result.get("opening_commands") or [])
                if row.get("issued_by")
            }
            | ({hold.get("issued_by")} if hold.get("issued_by") else set())
        )
        sections = all(key in frame for key in ("assets", "tracks", "alerts", "missions", "recent_commands"))
        single = (
            subscriber_count == 1
            and result["dashboard_http"]["status"] == 200
            and result["dashboard_http"]["references_dashboard_js"]
            and sections
            and operator_actors == [ACTOR]
            and len(cop.fleet()) == 4
        )
        result["criteria"].append(
            criterion(
                "10.4",
                single,
                {
                    "dashboard_http": result["dashboard_http"],
                    "socket_subscribers": subscriber_count,
                    "frame_keys": sorted(frame.keys()),
                    "fleet_ids": [card.get("id") for card in cop.fleet()],
                    "operator_actors": operator_actors,
                    "mission_actor": "mission",
                    "frames": cop.frames,
                },
            )
        )
        log(f"10.4 subscribers={subscriber_count} actors={operator_actors}")

        await cruise(
            client,
            cop,
            args.plan,
            result["goto_samples_ms"],
            result["alert_raise_samples_ms"],
            result["alert_insert_samples_ms"],
        )
        # Keep a few alert observations for the report without storing every zone id twice.
        result["alert_sample_count"] = len(result["alert_raise_samples_ms"])

        cop.measure_telem = False
        result["telemetry_samples_ms"] = cop.telem_ms
        result["telemetry_negative_clock"] = cop.negative
        result["frames"] = cop.frames
        frozen_client = cop.stage_report()
        try:
            from src.operator_app.telem_timing import summary as timing_summary

            frozen_server = timing_summary()
        except Exception as exc:
            frozen_server = {"error": str(exc)}
        result["faults"].append(await fault_nats(client, args.plan, result["goto_samples_ms"]))
        result["faults"].append(await fault_adapter(client, args.plan))
        result["faults"].append(await fault_redis(client, cop, args.plan))

        queued = (result["faults"][0].get("after_restart_broker") or {}).get("command") or {}
        event_ids = []
        for row in result.get("opening_commands") or []:
            evidence = None
        # Pull a few durable event ids from the hold and the task.
        for evidence in (
            (hold.get("evidence") or {}).get("events") or [],
            task_events,
        ):
            for event in evidence:
                if event.get("event_id"):
                    event_ids.append(event["event_id"])
        expect = {
            "queued_command_id": (result["faults"][0].get("issued") or {}).get("command_id"),
            "queued_asset_id": QUEUED_ASSET,
            "event_ids": event_ids[:6],
            "parent_queued_status": queued.get("status"),
        }
        result["faults"].append(await restart_app(server, cop, client, expect, args))
        restart_done = True
        server = None
        cop = None
        client = None
        result["completed"] = True
    except Exception:
        result["harness_error"] = traceback.format_exc()
        log("harness error\n" + result["harness_error"])
    finally:
        result["container_restore"] = restore_containers()
        if not restart_done:
            if cop is not None:
                try:
                    await cop.stop()
                except Exception:
                    pass
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass
            if server is not None:
                server.stop()
            close_duckdb()
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        started = parse_dt(result["started_at"])
        finished = parse_dt(result["finished_at"])
        if started and finished:
            result["wall_clock_s"] = round((finished - started).total_seconds(), 1)
        result["latency"] = {
            "measured_on": "this laptop",
            "method": "linear interpolation percentile on samples taken while NATS and Redis were up",
            "telemetry_to_websocket_frame": stats(result.get("telemetry_samples_ms") or []),
            "telemetry_target_p95_ms": 500,
            "telemetry_target_source": "stated for this acceptance run; the repo docs do not contain a numeric latency target",
            "goto_issue_to_ack": stats(result.get("goto_samples_ms") or []),
            "alert_raise_to_dashboard": stats(result.get("alert_raise_samples_ms") or []),
            "zone_insert_to_dashboard_frame": stats(result.get("alert_insert_samples_ms") or []),
            "telemetry_negative_clock": result.get("telemetry_negative_clock", 0),
        }
        if frozen_client is None and cop is not None:
            try:
                frozen_client = cop.stage_report()
            except Exception as exc:
                frozen_client = {"error": str(exc)}
        if frozen_server is None:
            try:
                from src.operator_app.telem_timing import summary as timing_summary

                frozen_server = timing_summary()
            except Exception as exc:
                frozen_server = {"error": str(exc)}
        result["latency_stages"] = {
            "sample_target": 3000,
            "telemetry_n": len(result.get("telemetry_samples_ms") or []),
            "client": frozen_client,
            "server": frozen_server,
            "mavlink_stream_request_hz": 10,
            "uav_source_time": "SYSTEM_TIME offset: unix usec minus boot ms, plus GLOBAL_POSITION_INT time_boot_ms",
            "ugv_source_time": "Gazebo header converted with a sim-to-wall offset taken when the stamp is not unix time",
        }
        # Samples can be long. Keep them in the JSON; the markdown report quotes the stats.
        RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
        RESULT_PATH.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
        log(f"wrote {RESULT_PATH}")
    if result.get("harness_error") or not result.get("completed"):
        print("ACCEPTANCE_FAILED", flush=True)
        return 1
    print("ACCEPTANCE_DONE", flush=True)
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RCI acceptance run")
    parser.add_argument("--phase", choices=("run", "recover"), default="run")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--quick", action="store_true", help="short shake-out, not the acceptance numbers")
    parser.add_argument("--expect", default="")
    parser.add_argument("--result", default="")
    parser.add_argument(
        "--report",
        default="",
        help="JSON path for the run phase. Default is docs/acceptance_m5.json, or docs/acceptance_m7.json when CNC_FLEET is set.",
    )
    args = parser.parse_args(argv)
    args.plan = build_plan(args)
    return args


def main(argv: list[str] | None = None) -> int:
    global RESULT_PATH
    os.chdir(ROOT)
    isolate_stores()
    args = parse_args(argv if argv is not None else sys.argv[1:])
    if args.report:
        RESULT_PATH = Path(args.report)
    elif os.environ.get("CNC_FLEET"):
        RESULT_PATH = ROOT / "docs" / "acceptance_m7.json"
    if args.phase == "recover":
        if not args.expect or not args.result:
            print("recover requires --expect and --result", flush=True)
            return 2
        return asyncio.run(recover_phase(args))
    return asyncio.run(scenario(args))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        # A killed parent must not leave cnc-nats-1 or cnc-redis stopped.
        if _stopped:
            restore_containers()
