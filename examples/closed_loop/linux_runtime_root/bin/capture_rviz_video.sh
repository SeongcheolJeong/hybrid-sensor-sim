#!/usr/bin/env bash
set -euo pipefail

# Optional helper.
# TODO: replace this placeholder with the real RViz video capture command.
# Expected env exported by the workflow:
#   LINUX_RUNTIME_ROOT
#   AUTOWARE_WORKSPACE_ROOT
#   AWSIM_RUNTIME_ROOT
#   SCENARIO_PATH
#   MAP_PATH
#   ROUTE_PATH
#   RUN_OUT_ROOT
#   CAPTURE_ROOT
#   ROSBAG_ROOT
#   AWSIM_CAMERA_CAPTURE_PATH
#   RVIZ_CAPTURE_PATH
#   AUTOWARE_TOPIC_CATALOG_PATH
#   AUTOWARE_CONSUMER_INPUT_MANIFEST_PATH

mkdir -p "$(dirname "$RVIZ_CAPTURE_PATH")"
: > "$RVIZ_CAPTURE_PATH"
echo "TODO: replace placeholder RViz capture in $0" >&2
while true; do sleep 60; done
