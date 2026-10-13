"""ROS 2 vehicle adapter. rclpy is imported when the node starts.

Odometry on /<namespace>/odom becomes Telemetry. GOTO sends NavigateToPose and
acks when the action server accepts the goal. Feedback updates mission status.
"""

from __future__ import annotations

import asyncio
import math
import os
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

_LOCK = threading.Lock()
_EXECUTOR = None
_THREAD: threading.Thread | None = None
_STARTED = False


def local_offset_m(lat: float, lon: float, home_lat: float, home_lon: float) -> tuple[float, float]:
    """East, north metres from home. Gazebo odom uses x east and y north."""
    north = (lat - home_lat) * 111_320.0
    east = (lon - home_lon) * 111_320.0 * math.cos(math.radians(home_lat))
    return east, north


def latlon_from_local(east: float, north: float, home_lat: float, home_lon: float) -> tuple[float, float]:
    lat = home_lat + (north / 111_320.0)
    lon = home_lon + (east / (111_320.0 * math.cos(math.radians(home_lat))))
    return lat, lon


def header_source_ms(stamp_sec: float | None, now_sec: float, *, skew_s: float = 30.0) -> float | None:
    """Wall-clock milliseconds from a ROS stamp. A sim-time stamp is not a source time."""
    if stamp_sec is None or stamp_sec <= 0:
        return None
    if abs(now_sec - float(stamp_sec)) > skew_s:
        return None
    return float(stamp_sec) * 1000.0


def sim_stamp_to_wall_ms(
    stamp_sec: float | None,
    now_sec: float,
    offset_s: float | None,
    *,
    skew_s: float = 30.0,
) -> tuple[float | None, float | None]:
    """Convert a Gazebo sim stamp to unix milliseconds.

    A stamp already near the wall clock is used as-is. A sim-time stamp keeps
    one offset (wall minus sim) so later messages show the real delay. The
    offset is taken again when the converted time is more than skew_s off.
    """
    if stamp_sec is None or stamp_sec <= 0:
        return None, offset_s
    stamp = float(stamp_sec)
    now = float(now_sec)
    if abs(now - stamp) <= skew_s:
        return stamp * 1000.0, 0.0
    if offset_s is None or abs(now - (stamp + float(offset_s))) > skew_s:
        offset_s = now - stamp
    return (stamp + float(offset_s)) * 1000.0, float(offset_s)


def _yaw_from_quat(x: float, y: float, z: float, w: float) -> float:
    siny = 2.0 * (w * z + x * y)
    cosy = 1.0 - 2.0 * (y * y + z * z)
    return math.degrees(math.atan2(siny, cosy)) % 360.0


def _executor():
    global _EXECUTOR, _THREAD, _STARTED
    import rclpy
    from rclpy.executors import MultiThreadedExecutor

    with _LOCK:
        if not _STARTED:
            if not rclpy.ok():
                rclpy.init()
            _STARTED = True
        if _EXECUTOR is None:
            _EXECUTOR = MultiThreadedExecutor()
            _THREAD = threading.Thread(target=_EXECUTOR.spin, name="cnc-rclpy", daemon=True)
            _THREAD.start()
        return _EXECUTOR


class ROS2Adapter(AssetAdapter):
    """One namespaced diff-drive robot. `namespace` is the ROS graph name, without a leading slash."""

    def __init__(self, asset: Asset, namespace: str = "", home_alt: float = 0.0):
        super().__init__(asset)
        self.namespace = namespace.strip("/")
        self._home_lat = float(asset.home_lat if asset.home_lat is not None else asset.lat)
        self._home_lon = float(asset.home_lon if asset.home_lon is not None else asset.lon)
        self._home_alt = float(home_alt)
        self._node = None
        self._odom_sub = None
        self._cmd_pub = None
        self._action = None
        self._goal_handle = None
        self._pose = threading.Lock()
        self._east = 0.0
        self._north = 0.0
        self._heading = 0.0
        self._speed = 0.0
        self._have_odom = False
        self._mission = "offline"
        self._distance: float | None = None
        self._source_ms: float | None = None
        self._adapter_ms: float | None = None
        self._gap_ms: float | None = None
        self._sim_offset_s: float | None = None

    @classmethod
    def from_spec(cls, spec) -> "ROS2Adapter":
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
            capabilities=["goto", "hold", "rtl"],
        )
        return cls(asset, namespace=spec.endpoint or spec.id.replace("-", "_"), home_alt=spec.home_alt)

    @property
    def id(self) -> str:
        return self.asset.id

    async def connect(self) -> None:
        try:
            await asyncio.to_thread(self._connect_ros)
        except Exception as exc:
            self.asset.status = AssetStatus.OFFLINE
            self._mission = f"ros down: {exc}"
            print(f"[ROS2Adapter] {self.asset.name} did not start: {exc}")
            return
        self.asset.status = AssetStatus.IDLE
        self.asset.last_seen = utcnow()
        print(f"[ROS2Adapter] {self.asset.name} namespace=/{self.namespace} odom={self._have_odom}")

    def _connect_ros(self) -> None:
        from nav_msgs.msg import Odometry
        from nav2_msgs.action import NavigateToPose
        from rclpy.action import ActionClient
        from geometry_msgs.msg import Twist

        executor = _executor()
        import rclpy

        self._node = rclpy.create_node(f"cnc_{self.namespace or self.asset.id}")
        topic_ns = f"/{self.namespace}" if self.namespace else ""
        self._odom_sub = self._node.create_subscription(
            Odometry, f"{topic_ns}/odom", self._on_odom, 20
        )
        self._cmd_pub = self._node.create_publisher(Twist, f"{topic_ns}/cmd_vel", 10)
        self._action = ActionClient(self._node, NavigateToPose, f"{topic_ns}/navigate_to_pose")
        executor.add_node(self._node)
        self._running = True
        self._action.wait_for_server(timeout_sec=60.0)
        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline and not self._have_odom:
            threading.Event().wait(0.2)

    async def disconnect(self) -> None:
        self._running = False
        node = self._node
        self._node = None
        if node is not None:
            try:
                executor = _executor()
                executor.remove_node(node)
                node.destroy_node()
            except Exception:
                pass
        self.asset.status = AssetStatus.OFFLINE
        self._mission = "offline"

    async def send_command(self, command: Command) -> bool:
        from src.operator_app.adapters.idempotency import deliver_once

        async def apply(cmd: Command) -> tuple[bool, str]:
            if self._node is None:
                return False, "ros down"
            try:
                ok = await asyncio.to_thread(self._apply_command, cmd)
            except Exception as exc:
                return False, str(exc)
            return ok, self._mission if ok else (self._mission or "rejected")

        return await deliver_once(apply, command)

    def poll_telemetry(self) -> Telemetry | None:
        if not self._running or not self._have_odom:
            return None
        with self._pose:
            east, north = self._east, self._north
            heading, speed = self._heading, self._speed
            mission = self._mission
            distance = self._distance
            source_ms, adapter_ms, gap_ms = self._source_ms, self._adapter_ms, self._gap_ms
        lat, lon = latlon_from_local(east, north, self._home_lat, self._home_lon)
        status = AssetStatus.MISSION if mission == "navigating" else AssetStatus.IDLE
        if mission == "rtl":
            status = AssetStatus.RETURNING
        seen = utcnow()
        self.asset.lat = lat
        self.asset.lon = lon
        self.asset.alt = self._home_alt
        self.asset.heading = heading
        self.asset.speed = speed
        self.asset.status = status
        self.asset.last_seen = seen
        return Telemetry(
            asset_id=self.asset.id,
            lat=lat,
            lon=lon,
            alt=self._home_alt,
            heading=heading,
            speed=speed,
            battery=100.0,
            status=status,
            mode=mission,
            timestamp=seen,
            extras={
                "mission_status": mission,
                "distance_remaining": distance,
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

    def _on_odom(self, msg) -> None:
        pose = msg.pose.pose
        twist = msg.twist.twist
        quat = pose.orientation
        now_s = time.time()
        stamp = getattr(getattr(msg, "header", None), "stamp", None)
        stamp_sec = None
        if stamp is not None:
            stamp_sec = float(getattr(stamp, "sec", 0)) + float(getattr(stamp, "nanosec", 0)) / 1e9
        received = now_s * 1000.0
        with self._pose:
            source_ms, self._sim_offset_s = sim_stamp_to_wall_ms(stamp_sec, now_s, self._sim_offset_s)
            self._east = float(pose.position.x)
            self._north = float(pose.position.y)
            self._heading = _yaw_from_quat(quat.x, quat.y, quat.z, quat.w)
            self._speed = math.hypot(twist.linear.x, twist.linear.y)
            self._have_odom = True
            self._source_ms = source_ms
            if self._adapter_ms is not None:
                self._gap_ms = received - self._adapter_ms
            self._adapter_ms = received

    def _apply_command(self, cmd: Command) -> bool:
        kind = cmd.command_type
        params = cmd.parameters or {}
        if kind in {CommandType.HOLD, CommandType.STOP, CommandType.LOITER}:
            self._cancel_goal()
            self._publish_stop()
            self._mission = "hold"
            return True
        if kind == CommandType.RTL:
            return self._send_goal(0.0, 0.0, mission="rtl")
        if kind == CommandType.GOTO:
            try:
                lat = float(params["lat"])
                lon = float(params["lon"])
            except (KeyError, TypeError, ValueError):
                self._mission = "goto rejected"
                return False
            east, north = local_offset_m(lat, lon, self._home_lat, self._home_lon)
            return self._send_goal(east, north, mission="navigating")
        self._mission = "unsupported"
        return False

    def _publish_stop(self) -> None:
        if self._cmd_pub is None:
            return
        from geometry_msgs.msg import Twist

        self._cmd_pub.publish(Twist())

    def _cancel_goal(self) -> None:
        handle = self._goal_handle
        self._goal_handle = None
        if handle is None:
            return
        try:
            handle.cancel_goal_async()
        except Exception:
            pass

    def _send_goal(self, east: float, north: float, mission: str) -> bool:
        from nav2_msgs.action import NavigateToPose

        ready = self._action is not None and getattr(self._action, "server_is_ready", lambda: False)()
        if not ready:
            if self._action is None or not self._action.wait_for_server(timeout_sec=2.0):
                self._mission = "action server down"
                return False
        self._cancel_goal()
        goal = NavigateToPose.Goal()
        goal.pose.header.frame_id = "odom"
        goal.pose.pose.position.x = float(east)
        goal.pose.pose.position.y = float(north)
        goal.pose.pose.orientation.w = 1.0
        done = threading.Event()
        state = {"ok": False, "handle": None}

        def feedback_cb(msg) -> None:
            remaining = getattr(msg.feedback, "distance_remaining", None)
            with self._pose:
                if remaining is not None:
                    self._distance = float(remaining)
                self._mission = mission

        def response_cb(future) -> None:
            try:
                handle = future.result()
            except Exception:
                handle = None
            accepted = bool(handle is not None and handle.accepted)
            state["ok"] = accepted
            state["handle"] = handle if accepted else None
            done.set()

        self._action.send_goal_async(goal, feedback_callback=feedback_cb).add_done_callback(response_cb)
        raw = os.environ.get("CNC_ACK_TIMEOUT_S", "5")
        try:
            budget = max(1.0, float(raw) - 1.0)
        except ValueError:
            budget = 4.0
        if not done.wait(budget):
            self._mission = "goto timeout"
            return False
        self._goal_handle = state["handle"]
        self._mission = mission if state["ok"] else "goto rejected"
        return state["ok"]
