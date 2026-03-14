#!/usr/bin/env bash
set -euo pipefail

# TODO: replace this placeholder with the real AWSIM video capture command.
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
#
# This template materializes the expected artifact path so downstream tooling has a stable contract.
# Replace it with a real ffmpeg or window-capture command before recording a real demo.

mkdir -p "$(dirname "$AWSIM_CAMERA_CAPTURE_PATH")"
: > "$AWSIM_CAMERA_CAPTURE_PATH"
echo "TODO: replace placeholder video capture in $0" >&2
while true; do sleep 60; done
