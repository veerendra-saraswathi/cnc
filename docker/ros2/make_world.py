#!/usr/bin/env python3
"""Write a headless world with two diff-drive rovers.

The chassis is the gazebo_ros diff-drive demo. Each robot publishes
/<name>/odom and subscribes to /<name>/cmd_vel.
"""

from __future__ import annotations

import sys

MODEL = """
    <model name="{name}">
      <pose>{x} 0 0.325 0 0 0</pose>
      <link name="chassis">
        <pose>-0.15 0 0.175 0 0 0</pose>
        <inertial>
          <mass>1.14395</mass>
          <inertia>
            <ixx>0.126164</ixx><ixy>0</ixy><ixz>0</ixz>
            <iyy>0.416519</iyy><iyz>0</iyz><izz>0.481014</izz>
          </inertia>
        </inertial>
        <collision name="collision"><geometry><box><size>2.0 1.0 0.57</size></box></geometry></collision>
        <visual name="visual"><geometry><box><size>2.0 1.0 0.57</size></box></geometry></visual>
      </link>
      <link name="left_wheel">
        <pose>0.55 0.625 -0.025 -1.5707 0 0</pose>
        <inertial>
          <mass>2</mass>
          <inertia>
            <ixx>0.145833</ixx><ixy>0</ixy><ixz>0</ixz>
            <iyy>0.145833</iyy><iyz>0</iyz><izz>0.125</izz>
          </inertia>
        </inertial>
        <collision name="collision">
          <geometry><sphere><radius>0.3</radius></sphere></geometry>
          <surface><friction><ode><mu>1</mu><mu2>1</mu2></ode></friction></surface>
        </collision>
        <visual name="visual"><geometry><sphere><radius>0.3</radius></sphere></geometry></visual>
      </link>
      <link name="right_wheel">
        <pose>0.55 -0.625 -0.025 -1.5707 0 0</pose>
        <inertial>
          <mass>2</mass>
          <inertia>
            <ixx>0.145833</ixx><ixy>0</ixy><ixz>0</ixz>
            <iyy>0.145833</iyy><iyz>0</iyz><izz>0.125</izz>
          </inertia>
        </inertial>
        <collision name="collision">
          <geometry><sphere><radius>0.3</radius></sphere></geometry>
          <surface><friction><ode><mu>1</mu><mu2>1</mu2></ode></friction></surface>
        </collision>
        <visual name="visual"><geometry><sphere><radius>0.3</radius></sphere></geometry></visual>
      </link>
      <link name="caster">
        <pose>-0.96 0 -0.125 0 0 0</pose>
        <inertial>
          <mass>1</mass>
          <inertia>
            <ixx>0.1</ixx><ixy>0</ixy><ixz>0</ixz>
            <iyy>0.1</iyy><iyz>0</iyz><izz>0.1</izz>
          </inertia>
        </inertial>
        <collision name="collision"><geometry><sphere><radius>0.2</radius></sphere></geometry></collision>
        <visual name="visual"><geometry><sphere><radius>0.2</radius></sphere></geometry></visual>
      </link>
      <joint name="left_wheel_joint" type="revolute">
        <parent>chassis</parent><child>left_wheel</child>
        <axis><xyz>0 0 1</xyz><limit><lower>-1e30</lower><upper>1e30</upper></limit></axis>
      </joint>
      <joint name="right_wheel_joint" type="revolute">
        <parent>chassis</parent><child>right_wheel</child>
        <axis><xyz>0 0 1</xyz><limit><lower>-1e30</lower><upper>1e30</upper></limit></axis>
      </joint>
      <joint name="caster_wheel" type="ball">
        <parent>chassis</parent><child>caster</child>
      </joint>
      <plugin name="diff_drive_{name}" filename="libgazebo_ros_diff_drive.so">
        <ros>
          <namespace>/{name}</namespace>
        </ros>
        <left_joint>left_wheel_joint</left_joint>
        <right_joint>right_wheel_joint</right_joint>
        <wheel_separation>1.25</wheel_separation>
        <wheel_diameter>0.6</wheel_diameter>
        <max_wheel_torque>20</max_wheel_torque>
        <max_wheel_acceleration>40</max_wheel_acceleration>
        <publish_odom>true</publish_odom>
        <publish_odom_tf>false</publish_odom_tf>
        <publish_wheel_tf>false</publish_wheel_tf>
        <odometry_frame>{name}_odom</odometry_frame>
        <robot_base_frame>{name}_base</robot_base_frame>
      </plugin>
    </model>
"""

WORLD = """<?xml version="1.0"?>
<sdf version="1.6">
  <world name="cnc">
    <physics type="ode">
      <!-- 100 Hz. 250 Hz falls behind wall clock on this Docker VM, so a 12 s observe misses 15 m. -->
      <max_step_size>0.01</max_step_size>
      <real_time_update_rate>100</real_time_update_rate>
    </physics>
    <include><uri>model://ground_plane</uri></include>
    <include><uri>model://sun</uri></include>
    {models}
  </world>
</sdf>
"""


def main() -> None:
    dest = sys.argv[1] if len(sys.argv) > 1 else "/tmp/cnc_ugv.world"
    models = MODEL.format(name="ugv_01", x="0") + MODEL.format(name="ugv_02", x="8")
    with open(dest, "w", encoding="utf-8") as handle:
        handle.write(WORLD.format(models=models))


if __name__ == "__main__":
    main()
