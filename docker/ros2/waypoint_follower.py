#!/usr/bin/env python3
"""NavigateToPose servers for ugv_01 and ugv_02.

Feedback is distance remaining. The goal succeeds inside 0.6 m. The control
loop runs on its own executor thread so odometry keeps updating. This is a
waypoint follower on /cmd_vel, not the Nav2 stack.
"""

from __future__ import annotations

import math
import threading
import time

import rclpy
from geometry_msgs.msg import Twist
from nav2_msgs.action import NavigateToPose
from nav_msgs.msg import Odometry
from rclpy.action import ActionServer, CancelResponse, GoalResponse
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup, ReentrantCallbackGroup
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node


def _yaw(q) -> float:
    siny = 2.0 * (q.w * q.z + q.x * q.y)
    cosy = 1.0 - 2.0 * (q.y * q.y + q.z * q.z)
    return math.atan2(siny, cosy)


def _wrap(angle: float) -> float:
    while angle > math.pi:
        angle -= 2.0 * math.pi
    while angle < -math.pi:
        angle += 2.0 * math.pi
    return angle


class WaypointFollower(Node):
    def __init__(self) -> None:
        super().__init__("cnc_waypoint_follower")
        self._pose: dict[str, tuple[float, float, float]] = {}
        self._pubs: dict[str, object] = {}
        self._lock = threading.Lock()
        # One group per odom subscription. A Reentrant group on these
        # subscriptions delivered no samples. Separate exclusive groups do.
        for ns in ("ugv_01", "ugv_02"):
            self.create_subscription(
                Odometry,
                f"/{ns}/odom",
                lambda msg, ns=ns: self._on_odom(ns, msg),
                20,
                callback_group=MutuallyExclusiveCallbackGroup(),
            )
            self._pubs[ns] = self.create_publisher(Twist, f"/{ns}/cmd_vel", 10)
            ActionServer(
                self,
                NavigateToPose,
                f"/{ns}/navigate_to_pose",
                execute_callback=self._executor(ns),
                goal_callback=lambda _goal: GoalResponse.ACCEPT,
                cancel_callback=lambda _goal: CancelResponse.ACCEPT,
                callback_group=ReentrantCallbackGroup(),
            )
        self.get_logger().info("waypoint follower ready for ugv_01 and ugv_02")

    def _executor(self, ns: str):
        def execute(goal_handle):
            return self._execute(ns, goal_handle)

        return execute

    def _on_odom(self, ns: str, msg: Odometry) -> None:
        pose = msg.pose.pose
        with self._lock:
            first = ns not in self._pose
            self._pose[ns] = (float(pose.position.x), float(pose.position.y), _yaw(pose.orientation))
        if first:
            self.get_logger().info(f"{ns} odom {pose.position.x:.1f},{pose.position.y:.1f}")

    def _stop(self, ns: str) -> None:
        self._pubs[ns].publish(Twist())

    def _execute(self, ns: str, goal_handle):
        target_x = float(goal_handle.request.pose.pose.position.x)
        target_y = float(goal_handle.request.pose.pose.position.y)
        self.get_logger().info(f"{ns} goal {target_x:.1f},{target_y:.1f}")
        feedback = NavigateToPose.Feedback()
        result = NavigateToPose.Result()
        if hasattr(result, "error_code"):
            result.error_code = 0
        while rclpy.ok():
            if goal_handle.is_cancel_requested:
                self._stop(ns)
                goal_handle.canceled()
                return result
            with self._lock:
                pose = self._pose.get(ns)
            if pose is None:
                time.sleep(0.1)
                continue
            dx = target_x - pose[0]
            dy = target_y - pose[1]
            dist = math.hypot(dx, dy)
            if hasattr(feedback, "distance_remaining"):
                feedback.distance_remaining = float(dist)
            goal_handle.publish_feedback(feedback)
            if dist < 0.6:
                self._stop(ns)
                goal_handle.succeed()
                return result
            err = _wrap(math.atan2(dy, dx) - pose[2])
            twist = Twist()
            # A pure spin does not change lat/lon, so the 12 s opening window
            # records 0 m when the goal sits behind the robot (ugv-02's offset).
            # Keep moving. A large yaw rate at this speed loops the chassis.
            if abs(err) > 1.2:
                twist.linear.x = 3.5
                twist.angular.z = 0.25 if err > 0.0 else -0.25
            else:
                twist.linear.x = 3.5
                twist.angular.z = max(-0.3, min(0.3, 0.6 * err))
            self._pubs[ns].publish(twist)
            time.sleep(0.1)
        goal_handle.abort()
        return result


def main() -> None:
    rclpy.init()
    node = WaypointFollower()
    executor = MultiThreadedExecutor(num_threads=4)
    executor.add_node(node)
    try:
        executor.spin()
    finally:
        executor.shutdown()
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
