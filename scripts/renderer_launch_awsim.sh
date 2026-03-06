#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: renderer_launch_awsim.sh <renderer args...>" >&2
  exit 2
fi

backend_bin="${AWSIM_BIN:-}"
if [[ -z "${backend_bin}" ]]; then
  echo "AWSIM_BIN is not set. Cannot execute AWSIM runtime." >&2
  exit 127
fi

exec "${backend_bin}" "$@"
