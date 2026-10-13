#!/bin/bash
# Run scripts/acceptance_run.py inside cnc-fleet against SITL and Gazebo.
# NATS and Redis stay the lab containers. Their passwords come from .env and are not printed.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
export DOCKER_SOCKET="${DOCKER_SOCKET:-${HOME}/.docker/run/docker.sock}"
if [[ -z "${CNC_REDIS_URL:-}" && -n "${REDIS_PASSWORD:-}" ]]; then
  CNC_REDIS_URL="redis://:${REDIS_PASSWORD}@host.docker.internal:6379/0"
fi
CNC_REDIS_URL="${CNC_REDIS_URL//127.0.0.1/host.docker.internal}"
CNC_REDIS_URL="${CNC_REDIS_URL//localhost/host.docker.internal}"
export CNC_REDIS_URL
export CNC_NATS_URL="${CNC_NATS_URL:-nats://host.docker.internal:4222}"
CNC_NATS_URL="${CNC_NATS_URL//127.0.0.1/host.docker.internal}"
export CNC_NATS_URL
export CNC_FLEET="${CNC_FLEET:-/work/fleet.sitl.yaml}"
export CNC_BUS=nats
export CNC_ACK_TIMEOUT_S="${CNC_ACK_TIMEOUT_S:-30}"
export CNC_PROBE_HOST="${CNC_PROBE_HOST:-host.docker.internal}"
export CNC_HTTP_HOST="${CNC_HTTP_HOST:-0.0.0.0}"
export NATS_TOKEN="${NATS_TOKEN:-}"
export REDIS_PASSWORD="${REDIS_PASSWORD:-}"
export PYTHONUNBUFFERED=1
export PYTHONDONTWRITEBYTECODE=1
echo "waiting for /ugv_01/odom inside cnc-fleet"
docker exec cnc-fleet bash -lc 'source /opt/ros/humble/setup.bash; for _ in $(seq 1 90); do ros2 topic list 2>/dev/null | grep -q /ugv_01/odom && exit 0; sleep 2; done; echo "ugv_01/odom missing" >&2; exit 1'
extra=""
if [[ $# -gt 0 ]]; then
  extra=$(printf '%q ' "$@")
fi
exec docker exec \
  -e CNC_FLEET \
  -e CNC_BUS \
  -e CNC_NATS_URL \
  -e CNC_REDIS_URL \
  -e CNC_ACK_TIMEOUT_S \
  -e CNC_PROBE_HOST \
  -e CNC_HTTP_HOST \
  -e NATS_TOKEN \
  -e REDIS_PASSWORD \
  -e PYTHONUNBUFFERED \
  -e PYTHONDONTWRITEBYTECODE \
  -e ROS_DOMAIN_ID=42 \
  -w /work \
  cnc-fleet \
  bash -lc "source /opt/ros/humble/setup.bash && exec python3 /work/scripts/acceptance_run.py --report /work/docs/acceptance_m7.json ${extra}"
