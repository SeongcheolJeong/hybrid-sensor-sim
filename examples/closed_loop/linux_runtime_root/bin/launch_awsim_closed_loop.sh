#!/usr/bin/env bash
set -euo pipefail

# TODO: replace this template with the real AWSIM launch command for the Linux host.
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
# Example shape:
#   exec "$AWSIM_RUNTIME_ROOT/AWSIM-Demo-Lightweight.x86_64" -batchmode -nographics

echo "TODO: implement AWSIM launch in $0" >&2
exit 2
