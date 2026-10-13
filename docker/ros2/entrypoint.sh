#!/bin/bash
# Headless Gazebo (gzserver via xvfb, no gzclient) plus the waypoint follower.
# ROS setup references unset variables, so nounset stays off.
set -eo pipefail
source /opt/ros/humble/setup.bash
# The image pip line lists these. A container created before that rebuild still needs them.
if ! python3 -c 'import jinja2, multipart' >/dev/null 2>&1; then
  pip3 install --disable-pip-version-check -q 'jinja2>=3.1' 'python-multipart>=0.0.9'
fi
export ROS_DOMAIN_ID="${ROS_DOMAIN_ID:-42}"
export LIBGL_ALWAYS_SOFTWARE=1
export GALLIUM_DRIVER=llvmpipe
export QT_QPA_PLATFORM=offscreen
export PYTHONDONTWRITEBYTECODE=1
export GAZEBO_MODEL_PATH="${GAZEBO_MODEL_PATH:-}:/usr/share/gazebo-11/models"
export GAZEBO_RESOURCE_PATH="${GAZEBO_RESOURCE_PATH:-}:/usr/share/gazebo-11"
ROS_DIR=/opt/cnc
if [[ -f /work/docker/ros2/make_world.py ]]; then
  ROS_DIR=/work/docker/ros2
fi
python3 "$ROS_DIR/make_world.py" /tmp/cnc_ugv.world
xvfb-run -a ros2 launch gazebo_ros gzserver.launch.py \
  world:=/tmp/cnc_ugv.world verbose:=true &
GZ_PID=$!
ready=0
for _ in $(seq 1 90); do
  if ros2 topic list 2>/dev/null | grep -q "/ugv_01/odom"; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "$ready" != 1 ]]; then
  echo "ugv_01/odom did not appear; waypoint follower still starting" >&2
fi
python3 "$ROS_DIR/waypoint_follower.py" &
wait "$GZ_PID"
