#!/usr/bin/env bash
set -euo pipefail

# TODO: return 0 only when localization is actually ready.
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
#   source "$AUTOWARE_WORKSPACE_ROOT/install/setup.bash"
#   ros2 topic echo --once /localization/kinematic_state >/dev/null

echo "TODO: localization readiness check not implemented in $0" >&2
exit 1
