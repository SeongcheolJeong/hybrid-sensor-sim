#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: renderer_launch_carla.sh <renderer args...>" >&2
  exit 2
fi

backend_bin="${CARLA_BIN:-}"
if [[ -z "${backend_bin}" ]]; then
  echo "CARLA_BIN is not set. Cannot execute CARLA runtime." >&2
  exit 127
fi

input_args=("$@")
output_args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --sensor-mount)
      if [[ $# -lt 2 ]]; then
        echo "missing payload for --sensor-mount" >&2
        exit 2
      fi
      payload="$2"
      shift 2
      parsed="$(python3 - "${payload}" <<'PY'
import json
import sys
raw = sys.argv[1]
if raw.startswith("{"):
    obj = json.loads(raw)
    sid = str(obj.get("sensor_id", ""))
    stype = str(obj.get("sensor_type", ""))
    attach = str(obj.get("attach_to_actor_id", ""))
else:
    parts = raw.split("|")
    parts += ["", "", ""]
    sid, stype, attach = parts[:3]
print("|".join([sid, stype, attach]))
PY
)"
      IFS='|' read -r sensor_id sensor_type attach_to <<<"${parsed}"
      if [[ -n "${sensor_id}" && -n "${sensor_type}" && -n "${attach_to}" ]]; then
        output_args+=(--attach-sensor "${sensor_type}:${sensor_id}:${attach_to}")
      fi
      ;;
    *)
      output_args+=("$1")
      shift
      ;;
  esac
done

if [[ -n "${RENDERER_WRAPPER_DUMP:-}" ]]; then
  python3 - "${RENDERER_WRAPPER_DUMP}" "${backend_bin}" "${input_args[@]}" "__WRAPSEP__" "${output_args[@]}" <<'PY'
import json
import sys
dump = sys.argv[1]
backend = sys.argv[2]
args = sys.argv[3:]
sep = "__WRAPSEP__"
if sep in args:
    idx = args.index(sep)
    in_args = args[:idx]
    out_args = args[idx + 1 :]
else:
    in_args = args
    out_args = []
with open(dump, "w", encoding="utf-8") as fp:
    json.dump(
        {
            "wrapper": "carla",
            "backend_bin": backend,
            "input_args": in_args,
            "output_args": out_args,
        },
        fp,
        indent=2,
    )
PY
fi

exec "${backend_bin}" "${output_args[@]}"
