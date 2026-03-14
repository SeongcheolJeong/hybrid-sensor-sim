#!/usr/bin/env bash
set -euo pipefail

# TODO: publish the real route goal into Autoware.
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
# This helper should return 0 only when the route goal was accepted.

echo "TODO: implement route goal publish in $0" >&2
exit 2
