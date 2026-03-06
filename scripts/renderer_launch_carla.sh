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
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid --sensor-mount payload: {exc}")
    sid = str(obj.get("sensor_id", ""))
    stype = str(obj.get("sensor_type", ""))
    attach = str(obj.get("attach_to_actor_id", ""))
    extrinsics = obj.get("extrinsics")
    if isinstance(extrinsics, dict):
        tx = str(extrinsics.get("tx", ""))
        ty = str(extrinsics.get("ty", ""))
        tz = str(extrinsics.get("tz", ""))
        roll = str(extrinsics.get("roll_deg", ""))
        pitch = str(extrinsics.get("pitch_deg", ""))
        yaw = str(extrinsics.get("yaw_deg", ""))
    else:
        tx = ""
        ty = ""
        tz = ""
        roll = ""
        pitch = ""
        yaw = ""
else:
    parts = raw.split("|")
    parts += [""] * 9
    sid, stype, attach, tx, ty, tz, roll, pitch, yaw = parts[:9]
print("|".join([sid, stype, attach, tx, ty, tz, roll, pitch, yaw]))
PY
)"
      IFS='|' read -r sensor_id sensor_type attach_to tx ty tz roll pitch yaw <<<"${parsed}"
      if [[ -n "${sensor_id}" && -n "${sensor_type}" && -n "${attach_to}" ]]; then
        output_args+=(--attach-sensor "${sensor_type}:${sensor_id}:${attach_to}")
      fi
      if [[ -n "${sensor_id}" && -n "${tx}" && -n "${ty}" && -n "${tz}" && -n "${roll}" && -n "${pitch}" && -n "${yaw}" ]]; then
        output_args+=(--sensor-pose "${sensor_id}:${tx}:${ty}:${tz}:${roll}:${pitch}:${yaw}")
      fi
      ;;
    --frame-manifest)
      if [[ $# -lt 2 ]]; then
        echo "missing payload for --frame-manifest" >&2
        exit 2
      fi
      manifest_path="$2"
      shift 2
      parsed_frames="$(python3 - "${manifest_path}" <<'PY'
import json
import os
import sys

path = sys.argv[1]
try:
    with open(path, "r", encoding="utf-8") as fp:
        payload = json.load(fp)
except OSError as exc:
    raise SystemExit(f"cannot read --frame-manifest file: {exc}")
except json.JSONDecodeError as exc:
    raise SystemExit(f"invalid --frame-manifest payload: {exc}")

frames = payload.get("frames")
if not isinstance(frames, list):
    raise SystemExit("--frame-manifest payload missing frames list")

allowed_formats = {
    "camera": {"camera_projection_json"},
    "lidar": {"lidar_points_xyz", "lidar_points_json", "lidar_points"},
    "radar": {"radar_targets_json"},
}
seen_meta = set()
for frame in frames:
    if not isinstance(frame, dict):
        continue
    rid = frame.get("renderer_frame_id", frame.get("frame_id", 0))
    try:
        rid = int(rid)
    except (TypeError, ValueError):
        rid = 0
    for sensor in ("camera", "lidar", "radar"):
        source = frame.get(sensor)
        if not isinstance(source, dict):
            continue
        if not bool(source.get("available", False)):
            continue
        sensor_name = str(source.get("sensor_name", sensor)).strip().lower() or sensor
        if sensor_name not in allowed_formats:
            raise SystemExit(f"unsupported sensor in frame-manifest: {sensor_name}")
        sensor_id = str(source.get("sensor_id", "")).strip() or sensor_name
        data_format = str(source.get("data_format", "")).strip()
        if not data_format:
            suffix = os.path.splitext(str(source.get("payload_artifact", "")))[1].lower()
            if sensor_name == "camera":
                data_format = "camera_projection_json"
            elif sensor_name == "lidar":
                data_format = "lidar_points_xyz" if suffix == ".xyz" else "lidar_points_json"
            elif sensor_name == "radar":
                data_format = "radar_targets_json"
        if data_format not in allowed_formats[sensor_name]:
            raise SystemExit(
                f"unsupported data_format for {sensor_name}: {data_format or '<empty>'}"
            )
        attach_to = str(source.get("attach_to_actor_id", "")).strip() or "ego"
        payload_artifact = str(source.get("payload_artifact", "")).strip()
        if not payload_artifact:
            continue
        if not os.path.exists(payload_artifact):
            raise SystemExit(f"payload artifact does not exist: {payload_artifact}")
        print(f"FRAME|{rid}|{sensor_name}|{payload_artifact}")
        meta_key = (sensor_name, sensor_id, data_format, attach_to)
        if meta_key in seen_meta:
            continue
        seen_meta.add(meta_key)
        print(f"META|{sensor_name}|{sensor_id}|{data_format}|{attach_to}")
PY
)"
      while IFS='|' read -r record field1 field2 field3 field4; do
        case "${record}" in
          FRAME)
            renderer_frame_id="${field1}"
            sensor_name="${field2}"
            payload_artifact="${field3}"
            if [[ -z "${sensor_name}" || -z "${payload_artifact}" ]]; then
              continue
            fi
            output_args+=(--ingest-frame "${renderer_frame_id}:${sensor_name}:${payload_artifact}")
            ;;
          META)
            sensor_name="${field1}"
            sensor_id="${field2}"
            data_format="${field3}"
            attach_to="${field4}"
            if [[ -z "${sensor_name}" || -z "${sensor_id}" || -z "${data_format}" ]]; then
              continue
            fi
            output_args+=(--ingest-meta "${sensor_name}:${sensor_id}:${data_format}:${attach_to}")
            ;;
        esac
      done <<<"${parsed_frames}"
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
