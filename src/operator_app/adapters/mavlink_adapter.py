"""MAVLink vehicle adapter. pymavlink is imported when the link opens.

ArduCopter SITL sends UDP to this process. HOLD, RTL, ARM, and TAKEOFF wait
for COMMAND_ACK. GOTO uploads one waypoint and waits for COMMAND_ACK of
MISSION_START. Copter 4.5 does not implement DO_REPOSITION. A missing ack
is a rejected command.
"""

from __future__ import annotations

import asyncio
import math
import os
import select
import threading
import time
from typing import Optional

from src.operator_app.adapters.base import AssetAdapter
from src.operator_app.app import (
    Asset,
    AssetStatus,
    AssetType,
    Command,
    CommandType,
    Telemetry,
    utcnow,
)

# MAV_RESULT values used here. Kept numeric so tests do not import pymavlink.
ACK_ACCEPTED = 0
ACK_IN_PROGRESS = 5
# REQUEST_DATA_STREAM rate. GLOBAL_POSITION_INT has time_boot_ms, not unix time.
STREAM_HZ = 10

_COPTER_MODES = {
    "STABILIZE": 0,
    "GUIDED": 4,
    "LOITER": 5,
    "RTL": 6,
    "AUTO": 3,
    "LAND": 9,
}


def ack_is_final(result: int) -> bool:
    """IN_PROGRESS is not the vehicle's answer yet."""
    return int(result) != ACK_IN_PROGRESS


def ack_ok(result: int) -> bool:
    return int(result) == ACK_ACCEPTED


def command_ack_budget() -> float:
    """Stay inside the bus ack wait so the adapter publishes before it fires."""
    raw = os.environ.get("CNC_ACK_TIMEOUT_S", "5")
    try:
        budget = float(raw) - 1.0
    except ValueError:
        budget = 4.0
    return max(1.0, budget)


class MAVLinkAdapter(AssetAdapter):
    """One MAVLink system. `connection_string` is a pymavlink URL such as udpin:0.0.0.0:14550."""

    def __init__(
        self,
        asset: Asset,
        connection_string: str = "udpin:0.0.0.0:14550",
        home_alt: float | None = None,
    ):
        super().__init__(asset)
        self.connection_string = connection_string
        self._home_alt = float(asset.alt if home_alt is None else home_alt)
        self._master = None
        self._thread: threading.Thread | None = None
        self._io = threading.Lock()
        self._ack_cv = threading.Condition()
        self._acks: dict[int, int] = {}
        self._mission_result: int | None = None
        self._mission_request: tuple[str, int] | None = None
        self._last_text = ""
        self._pose = threading.Lock()
        self._lat = float(asset.lat)
        self._lon = float(asset.lon)
        self._alt = float(asset.alt)
        self._heading = float(asset.heading)
        self._speed = 0.0
        self._battery = float(asset.battery or 100.0)
        self._mode = "UNKNOWN"
        self._armed = False
        self._have_fix = False
        self._mission = "offline"
        self._source_ms: float | None = None
        self._adapter_ms: float | None = None
        self._gap_ms: float | None = None

    @classmethod
    def from_spec(cls, spec) -> "MAVLinkAdapter":
        asset = Asset(
            id=spec.id,
            name=spec.name,
            asset_type=AssetType(spec.type),
            status=AssetStatus.OFFLINE,
            lat=spec.home_lat,
            lon=spec.home_lon,
            alt=spec.home_alt,
            home_lat=spec.home_lat,
            home_lon=spec.home_lon,
            battery=100.0,
            capabilities=["goto", "hold", "rtl", "arm", "takeoff", "land"],
        )
        return cls(asset, connection_string=spec.endpoint or "udpin:0.0.0.0:14550", home_alt=spec.home_alt)

    @property
    def id(self) -> str:
        return self.asset.id

    async def connect(self) -> None:
        from pymavlink import mavutil

        self._mavutil = mavutil
        try:
            self._master = await asyncio.to_thread(
                mavutil.mavlink_connection,
                self.connection_string,
                source_system=255,
                source_component=190,
            )
            await asyncio.to_thread(self._master.wait_heartbeat, timeout=90)
        except Exception as exc:
            self.asset.status = AssetStatus.OFFLINE
            self._mission = f"link down: {exc}"
            print(f"[MAVLinkAdapter] {self.asset.name} no heartbeat on {self.connection_string}: {exc}")
            return
        self._running = True
        self._thread = threading.Thread(target=self._read_loop, name=f"mav-{self.asset.id}", daemon=True)
        self._thread.start()
        self._request_streams()
        ready = await asyncio.to_thread(self._wait_fix, 40.0)
        if self.asset.asset_type == AssetType.UAV and ready:
            await asyncio.to_thread(self._prepare_airborne, 90.0)
        self.asset.status = AssetStatus.IDLE if self._master is not None else AssetStatus.OFFLINE
        self.asset.last_seen = utcnow()
        with self._pose:
            alt_now = self._alt
            mission = self._mission
        print(
            f"[MAVLinkAdapter] {self.asset.name} {self.connection_string} "
            f"mode={self._mode} armed={self._armed} fix={self._have_fix} "
            f"alt={alt_now:.1f} mission={mission}"
        )

    async def disconnect(self) -> None:
        self._running = False
        master = self._master
        self._master = None
        if master is not None:
            try:
                master.close()
            except Exception:
                pass
        self.asset.status = AssetStatus.OFFLINE
        self._mission = "offline"

    async def send_command(self, command: Command) -> bool:
        from src.operator_app.adapters.idempotency import deliver_once

        async def apply(cmd: Command) -> tuple[bool, str]:
            if self._master is None:
                return False, "link down"
            try:
                ok = await asyncio.to_thread(self._apply_command, cmd)
            except Exception as exc:
                return False, str(exc)
            reason = self._mission if ok else (self._mission or "command rejected")
            return ok, reason

        return await deliver_once(apply, command)

    def poll_telemetry(self) -> Telemetry | None:
        if not self._running or self._master is None:
            return None
        with self._pose:
            lat, lon, alt = self._lat, self._lon, self._alt
            heading, speed, battery = self._heading, self._speed, self._battery
            mode, armed, mission = self._mode, self._armed, self._mission
            have_fix = self._have_fix
            source_ms, adapter_ms, gap_ms = self._source_ms, self._adapter_ms, self._gap_ms
        if not have_fix:
            return None
        if armed and mode in {"GUIDED", "AUTO", "RTL"}:
            status = AssetStatus.RETURNING if mode == "RTL" else AssetStatus.MISSION
        else:
            status = AssetStatus.IDLE
        seen = utcnow()
        self.asset.lat = lat
        self.asset.lon = lon
        self.asset.alt = alt
        self.asset.heading = heading
        self.asset.speed = speed
        self.asset.battery = battery
        self.asset.status = status
        self.asset.last_seen = seen
        return Telemetry(
            asset_id=self.asset.id,
            lat=lat,
            lon=lon,
            alt=alt,
            heading=heading,
            speed=speed,
            battery=battery,
            status=status,
            mode=mode,
            timestamp=seen,
            extras={
                "armed": armed,
                "mission_status": mission,
                "timing": {
                    "source_ms": source_ms,
                    "adapter_ms": adapter_ms,
                    "poll_ms": seen.timestamp() * 1000.0,
                    "message_gap_ms": gap_ms,
                },
            },
        )

    async def get_telemetry(self) -> Optional[Telemetry]:
        sample = self.poll_telemetry()
        if sample is None:
            return None
        return await self.publish_vendor_telemetry(sample)

    def _request_streams(self) -> None:
        master = self._master
        if master is None:
            return
        mav = self._mavutil.mavlink
        sysid, comp = self._dest()
        with self._io:
            master.mav.request_data_stream_send(
                sysid,
                comp,
                mav.MAV_DATA_STREAM_ALL,
                STREAM_HZ,
                1,
            )

    def _read_loop(self) -> None:
        master = self._master
        while self._running and master is not None and self._master is master:
            # Wait for bytes without the send lock. pymavlink is not safe to send and receive at once,
            # so the lock covers only the read, and a command is not stuck behind a 0.5 s idle recv.
            fd = getattr(master, "fd", None)
            if isinstance(fd, int) and fd < 0:
                fd = None
            if fd is not None:
                try:
                    ready, _, _ = select.select([fd], [], [], 0.05)
                except Exception:
                    break
                if not ready:
                    continue
            try:
                with self._io:
                    msg = master.recv_match(blocking=fd is None, timeout=0.05)
            except Exception:
                break
            if msg is None:
                continue
            self._on_message(msg)

    def _on_message(self, msg) -> None:
        kind = msg.get_type()
        if kind == "COMMAND_ACK":
            with self._ack_cv:
                self._acks[int(msg.command)] = int(msg.result)
                self._ack_cv.notify_all()
            return
        if kind == "MISSION_ACK":
            with self._ack_cv:
                self._mission_result = int(msg.type)
                self._ack_cv.notify_all()
            return
        if kind in {"MISSION_REQUEST", "MISSION_REQUEST_INT"}:
            with self._ack_cv:
                self._mission_request = (kind, int(msg.seq))
                self._ack_cv.notify_all()
            return
        if kind == "STATUSTEXT":
            raw = getattr(msg, "text", "")
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", "ignore")
            text = str(raw).strip("\x00").strip()
            if text:
                with self._pose:
                    self._last_text = text[:160]
            return
        if kind == "GLOBAL_POSITION_INT":
            # No unix time on this message. The gap between receives is the SITL stream period.
            received = time.time() * 1000.0
            with self._pose:
                self._lat = msg.lat / 1.0e7
                self._lon = msg.lon / 1.0e7
                self._alt = msg.relative_alt / 1000.0
                if msg.hdg != 65535:
                    self._heading = msg.hdg / 100.0
                self._speed = math.hypot(msg.vx, msg.vy) / 100.0
                self._have_fix = True
                self._source_ms = None
                if self._adapter_ms is not None:
                    self._gap_ms = received - self._adapter_ms
                self._adapter_ms = received
            return
        if kind == "HEARTBEAT":
            # Skip GCS heartbeats (type 6) and MAV_AUTOPILOT_INVALID (8).
            if int(getattr(msg, "type", 0)) == 6 or int(getattr(msg, "autopilot", 8)) == 8:
                return
            armed = bool(msg.base_mode & 0b10000000)  # MAV_MODE_FLAG_SAFETY_ARMED
            mode = self._mode_name(int(msg.custom_mode))
            with self._pose:
                self._armed = armed
                self._mode = mode
            return
        if kind == "SYS_STATUS":
            remaining = int(getattr(msg, "battery_remaining", -1))
            if remaining >= 0:
                with self._pose:
                    self._battery = float(remaining)

    def _mode_name(self, custom_mode: int) -> str:
        master = self._master
        mapping = {}
        if master is not None:
            try:
                mapping = master.mode_mapping() or {}
            except Exception:
                mapping = {}
        for name, number in mapping.items():
            if int(number) == custom_mode:
                return str(name)
        for name, number in _COPTER_MODES.items():
            if number == custom_mode:
                return name
        return f"MODE_{custom_mode}"

    def _mode_number(self, name: str) -> int | None:
        master = self._master
        if master is not None:
            try:
                mapping = master.mode_mapping() or {}
            except Exception:
                mapping = {}
            if name in mapping:
                return int(mapping[name])
        return _COPTER_MODES.get(name)

    def _wait_fix(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._pose:
                if self._have_fix:
                    return True
            time.sleep(0.2)
        return False

    def _prepare_airborne(self, timeout: float) -> None:
        """Arm and climb before the command consumer opens, so a GOTO can move in the 12 s window."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and not self._armed:
            # A missed mode ack is not fatal. The heartbeat is the armed bit.
            remaining = deadline - time.monotonic()
            self._set_mode("GUIDED", timeout=min(4.0, max(0.4, remaining)))
            remaining = deadline - time.monotonic()
            self._command_long(400, 1, timeout=min(4.0, max(0.4, remaining)))
            pulse = time.monotonic() + 1.5
            while time.monotonic() < pulse and time.monotonic() < deadline and not self._armed:
                time.sleep(0.1)
        if not self._armed:
            self._mission = self._last_text or "arm rejected"
            return
        alt = self._home_alt if self._home_alt > 5.0 else 20.0
        goal_alt = max(15.0, alt * 0.7)
        with self._pose:
            alt_now = self._alt
        if alt_now < goal_alt:
            # MAV_CMD_NAV_TAKEOFF. Altitude is param 7.
            self._command_long(22, 0, 0, 0, 0, 0, 0, alt, timeout=8.0)
        # One high sample is not a climb. Hold the altitude across a short stretch of heartbeats.
        climb_deadline = min(deadline, time.monotonic() + 45.0)
        stable = 0.0
        mark = time.monotonic()
        while time.monotonic() < climb_deadline:
            now = time.monotonic()
            with self._pose:
                alt_now = self._alt
                armed = self._armed
            if armed and alt_now >= goal_alt:
                stable += now - mark
                if stable >= 1.5:
                    self._mission = "airborne"
                    return
            else:
                stable = 0.0
            mark = now
            time.sleep(0.2)
        self._mission = "takeoff"

    def _apply_command(self, cmd: Command) -> bool:
        kind = cmd.command_type
        params = cmd.parameters or {}
        # One shared budget so a mode change plus the vehicle command still ack inside the bus wait.
        deadline = time.monotonic() + command_ack_budget()

        def left() -> float:
            return max(0.4, deadline - time.monotonic())

        if kind == CommandType.ARM:
            ok = self._set_mode("GUIDED", timeout=left()) and self._command_long(400, 1, timeout=left())
            self._mission = "armed" if ok else "arm rejected"
            return ok
        if kind == CommandType.DISARM:
            ok = self._command_long(400, 0, timeout=left())
            self._mission = "disarmed" if ok else "disarm rejected"
            return ok
        if kind == CommandType.TAKEOFF:
            try:
                alt = float(params.get("altitude", params.get("alt", self._home_alt or 20.0)))
            except (TypeError, ValueError):
                return False
            ok = self._set_mode("GUIDED", timeout=left()) and self._command_long(22, 0, 0, 0, 0, 0, 0, alt, timeout=left())
            self._mission = "takeoff" if ok else "takeoff rejected"
            return ok
        if kind == CommandType.HOLD or kind == CommandType.LOITER or kind == CommandType.STOP:
            ok = self._set_mode("LOITER", timeout=left())
            self._mission = "LOITER" if ok else "hold rejected"
            return ok
        if kind == CommandType.RTL:
            ok = self._command_long(20, timeout=left())  # MAV_CMD_NAV_RETURN_TO_LAUNCH
            self._mission = "RTL" if ok else "rtl rejected"
            return ok
        if kind == CommandType.LAND:
            ok = self._command_long(21, timeout=left())  # MAV_CMD_NAV_LAND
            self._mission = "LAND" if ok else "land rejected"
            return ok
        if kind == CommandType.GOTO:
            try:
                lat = float(params["lat"])
                lon = float(params["lon"])
                alt = float(params.get("alt", self._home_alt or self._alt or 20.0))
            except (KeyError, TypeError, ValueError):
                self._mission = "goto rejected"
                return False
            deadline = time.monotonic() + left()
            if not self._upload_waypoint(lat, lon, alt, deadline):
                self._mission = self._last_text or "goto rejected"
                return False
            # MAV_CMD_MISSION_START at the waypoint. Home was uploaded as seq 0.
            ok = self._command_long(300, 1, 1, timeout=max(0.4, deadline - time.monotonic()))
            self._mission = "AUTO" if ok else (self._last_text or "goto rejected")
            return ok
        self._mission = "unsupported"
        return False

    def _set_mode(self, name: str, timeout: float) -> bool:
        number = self._mode_number(name)
        if number is None or self._master is None:
            return False
        # MAV_CMD_DO_SET_MODE, custom mode enabled.
        return self._command_long(176, 1, number, timeout=timeout)

    def _command_long(self, command: int, *params: float, timeout: float) -> bool:
        master = self._master
        if master is None:
            return False
        values = list(params) + [0.0] * (7 - len(params))
        sysid, comp = self._dest()
        with self._ack_cv:
            self._acks.pop(command, None)
        with self._io:
            master.mav.command_long_send(
                sysid,
                comp,
                command,
                0,
                float(values[0]),
                float(values[1]),
                float(values[2]),
                float(values[3]),
                float(values[4]),
                float(values[5]),
                float(values[6]),
            )
        return self._wait_ack(command, timeout)

    def _dest(self) -> tuple[int, int]:
        """System and component to address. pymavlink leaves component at 0."""
        master = self._master
        if master is None:
            return 1, 1
        system = int(getattr(master, "target_system", 0) or 1)
        component = int(getattr(master, "target_component", 0) or 1)
        return system, component

    def _upload_waypoint(self, lat: float, lon: float, alt: float, deadline: float) -> bool:
        """Upload home plus one NAV_WAYPOINT. Copter 4.5 acks this, not DO_REPOSITION."""
        master = self._master
        if master is None:
            return False
        sysid, comp = self._dest()
        home_lat = float(self.asset.home_lat if self.asset.home_lat is not None else self._lat)
        home_lon = float(self.asset.home_lon if self.asset.home_lon is not None else self._lon)
        items = (
            (home_lat, home_lon, 0.0, 0),
            (lat, lon, alt, 1),
        )

        def left() -> float:
            return deadline - time.monotonic()

        if not self._mission_call(lambda: master.mav.mission_clear_all_send(sysid, comp, 0), left()):
            return False
        if left() <= 0.2:
            return False
        with self._ack_cv:
            self._mission_result = None
            self._mission_request = None
        with self._io:
            master.mav.mission_count_send(sysid, comp, len(items), 0)
        pending = {0, 1}
        while pending and left() > 0:
            request = self._wait_mission_request(left())
            if request is None:
                return False
            kind, seq = request
            if seq not in pending:
                continue
            self._send_mission_item(sysid, comp, seq, items[seq], kind)
            pending.discard(seq)
        return not pending and self._wait_mission_result(left())

    def _mission_call(self, send, timeout: float) -> bool:
        master = self._master
        if master is None or timeout <= 0:
            return False
        with self._ack_cv:
            self._mission_result = None
        with self._io:
            send()
        return self._wait_mission_result(timeout)

    def _wait_mission_result(self, timeout: float) -> bool:
        if timeout <= 0:
            return False
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._ack_cv:
                result = self._mission_result
                if result is None:
                    remaining = deadline - time.monotonic()
                    self._ack_cv.wait(timeout=min(0.2, max(0.01, remaining)))
                    continue
                self._mission_result = None
                return result == 0
        return False

    def _wait_mission_request(self, timeout: float) -> tuple[str, int] | None:
        if timeout <= 0:
            return None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._ack_cv:
                request = self._mission_request
                if request is None:
                    remaining = deadline - time.monotonic()
                    self._ack_cv.wait(timeout=min(0.2, max(0.01, remaining)))
                    continue
                self._mission_request = None
                return request
        return None

    def _send_mission_item(
        self,
        sysid: int,
        comp: int,
        seq: int,
        item: tuple[float, float, float, int],
        kind: str,
    ) -> None:
        master = self._master
        if master is None:
            return
        lat, lon, alt, current = item
        # MAV_FRAME_GLOBAL_RELATIVE_ALT_INT is 6. MAV_CMD_NAV_WAYPOINT is 16.
        frame = 6
        command = 16
        with self._io:
            if kind == "MISSION_REQUEST_INT":
                master.mav.mission_item_int_send(
                    sysid,
                    comp,
                    seq,
                    frame,
                    command,
                    current,
                    1,
                    0,
                    0,
                    0,
                    0,
                    int(round(lat * 1.0e7)),
                    int(round(lon * 1.0e7)),
                    float(alt),
                    0,
                )
            else:
                master.mav.mission_item_send(
                    sysid,
                    comp,
                    seq,
                    3,
                    command,
                    current,
                    1,
                    0,
                    0,
                    0,
                    0,
                    float(lat),
                    float(lon),
                    float(alt),
                    0,
                )

    def _wait_ack(self, command: int, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._ack_cv:
                result = self._acks.get(command)
                if result is None or not ack_is_final(result):
                    remaining = deadline - time.monotonic()
                    self._ack_cv.wait(timeout=min(0.2, max(0.01, remaining)))
                    continue
                return ack_ok(result)
        return False
