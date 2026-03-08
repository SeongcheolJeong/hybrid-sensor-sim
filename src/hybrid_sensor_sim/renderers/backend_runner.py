from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

_BACKEND_DEFAULT_BINS: dict[str, str] = {
    "awsim": "awsim",
    "carla": "carla",
}

_BACKEND_ENV_BIN_KEYS: dict[str, str] = {
    "awsim": "AWSIM_BIN",
    "carla": "CARLA_BIN",
}

_BACKEND_EXPECTED_OUTPUT_PRESETS: dict[str, list[dict[str, Any]]] = {
    "awsim": [
        {
            "artifact_key": "backend_output_root_dir",
            "relative_path": ".",
            "kind": "directory",
            "required": False,
            "description": "Backend-specific output root directory.",
        },
        {
            "artifact_key": "awsim_sensor_exports_dir",
            "relative_path": "sensor_exports",
            "kind": "directory",
            "required": False,
            "description": "AWSIM sensor export directory.",
        },
        {
            "artifact_key": "awsim_runtime_state_json",
            "relative_path": "awsim_runtime_state.json",
            "kind": "file",
            "required": False,
            "description": "AWSIM runtime state summary.",
        },
    ],
    "carla": [
        {
            "artifact_key": "backend_output_root_dir",
            "relative_path": ".",
            "kind": "directory",
            "required": False,
            "description": "Backend-specific output root directory.",
        },
        {
            "artifact_key": "carla_sensor_exports_dir",
            "relative_path": "sensor_exports",
            "kind": "directory",
            "required": False,
            "description": "CARLA sensor export directory.",
        },
        {
            "artifact_key": "carla_recorder_log",
            "relative_path": "carla_recorder.log",
            "kind": "file",
            "required": False,
            "description": "CARLA recorder log.",
        },
        {
            "artifact_key": "carla_runtime_state_json",
            "relative_path": "carla_runtime_state.json",
            "kind": "file",
            "required": False,
            "description": "CARLA runtime state summary.",
        },
    ],
}

_BACKEND_SENSOR_EXPORT_LAYOUTS: dict[str, dict[str, dict[str, str]]] = {
    "awsim": {
        "camera_projection_json": {
            "modality": "camera",
            "filename": "rgb_frame.json",
            "output_role": "camera_visible",
            "artifact_type": "awsim_camera_rgb_json",
        },
        "camera_depth_json": {
            "modality": "camera",
            "filename": "depth_frame.json",
            "output_role": "camera_depth",
            "artifact_type": "awsim_camera_depth_json",
        },
        "camera_semantic_json": {
            "modality": "camera",
            "filename": "semantic_frame.json",
            "output_role": "camera_semantic",
            "artifact_type": "awsim_camera_semantic_json",
        },
        "lidar_points_xyz": {
            "modality": "lidar",
            "filename": "point_cloud.xyz",
            "output_role": "lidar_point_cloud",
            "artifact_type": "awsim_lidar_xyz_point_cloud",
        },
        "lidar_points_json": {
            "modality": "lidar",
            "filename": "point_cloud.json",
            "output_role": "lidar_point_cloud",
            "artifact_type": "awsim_lidar_json_point_cloud",
        },
        "lidar_points": {
            "modality": "lidar",
            "filename": "point_cloud.bin",
            "output_role": "lidar_point_cloud",
            "artifact_type": "awsim_lidar_binary_point_cloud",
        },
        "radar_targets_json": {
            "modality": "radar",
            "filename": "targets.json",
            "output_role": "radar_detections",
            "artifact_type": "awsim_radar_detections_json",
        },
        "radar_tracks_json": {
            "modality": "radar",
            "filename": "tracks.json",
            "output_role": "radar_tracks",
            "artifact_type": "awsim_radar_tracks_json",
        },
    },
    "carla": {
        "camera_projection_json": {
            "modality": "camera",
            "filename": "image.json",
            "output_role": "camera_visible",
            "artifact_type": "carla_camera_rgb_json",
        },
        "camera_depth_json": {
            "modality": "camera",
            "filename": "image_depth.json",
            "output_role": "camera_depth",
            "artifact_type": "carla_camera_depth_json",
        },
        "camera_semantic_json": {
            "modality": "camera",
            "filename": "image_semantic.json",
            "output_role": "camera_semantic",
            "artifact_type": "carla_camera_semantic_json",
        },
        "lidar_points_xyz": {
            "modality": "lidar",
            "filename": "point_cloud.xyz",
            "output_role": "lidar_point_cloud",
            "artifact_type": "carla_lidar_xyz_point_cloud",
        },
        "lidar_points_json": {
            "modality": "lidar",
            "filename": "point_cloud.json",
            "output_role": "lidar_point_cloud",
            "artifact_type": "carla_lidar_json_point_cloud",
        },
        "lidar_points": {
            "modality": "lidar",
            "filename": "point_cloud.bin",
            "output_role": "lidar_point_cloud",
            "artifact_type": "carla_lidar_binary_point_cloud",
        },
        "radar_targets_json": {
            "modality": "radar",
            "filename": "detections.json",
            "output_role": "radar_detections",
            "artifact_type": "carla_radar_detections_json",
        },
        "radar_tracks_json": {
            "modality": "radar",
            "filename": "tracks.json",
            "output_role": "radar_tracks",
            "artifact_type": "carla_radar_tracks_json",
        },
    },
}


@dataclass
class BackendRunnerExecutionResult:
    success: bool
    message: str
    return_code: int | None = None
    artifacts: dict[str, Path] = field(default_factory=dict)


def _read_json_payload(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _load_backend_runner_request_payload(
    *,
    request_path: Path,
) -> tuple[dict[str, Any] | None, str | None]:
    try:
        payload = json.loads(request_path.read_text(encoding="utf-8"))
    except OSError as exc:
        return None, f"Backend runner request read error: {exc}"
    except json.JSONDecodeError as exc:
        return None, f"Backend runner request decode error: {exc}"
    if not isinstance(payload, dict):
        return None, "Backend runner request payload must be a JSON object."
    return payload, None


def _coerce_arg_list(raw: Any) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw]


def _resolve_direct_backend_bin(
    *,
    options: dict[str, Any],
    backend: str,
) -> tuple[str, str, str | None]:
    explicit_bin = str(
        options.get(
            f"{backend}_bin",
            options.get(
                f"renderer_{backend}_bin",
                options.get("renderer_backend_bin", ""),
            ),
        )
    ).strip()
    if explicit_bin:
        return explicit_bin, "option", None

    env_key = _BACKEND_ENV_BIN_KEYS.get(backend)
    if env_key is not None:
        env_value = str(os.environ.get(env_key, "")).strip()
        if env_value:
            return env_value, "env", env_key

    default_bin = _BACKEND_DEFAULT_BINS.get(backend, backend or "backend")
    return default_bin, "preset_default", env_key


def _collect_extra_args(*, options: dict[str, Any], backend: str) -> list[str]:
    backend_extra_args = _coerce_arg_list(
        options.get(
            f"{backend}_extra_args",
            options.get(f"renderer_{backend}_extra_args"),
        )
    )
    shared_extra_args = _coerce_arg_list(options.get("renderer_extra_args", []))
    return [*backend_extra_args, *shared_extra_args]


def _collect_scene_args(backend_args_preview: dict[str, Any] | None) -> list[str]:
    if not isinstance(backend_args_preview, dict):
        return []
    return _coerce_arg_list(backend_args_preview.get("scene_cli_args", []))


def _collect_mount_args(
    *,
    backend: str,
    backend_args_preview: dict[str, Any] | None,
) -> list[str]:
    if not isinstance(backend_args_preview, dict):
        return []
    mounts = backend_args_preview.get("sensor_mounts")
    if not isinstance(mounts, list):
        return []

    args: list[str] = []
    for mount in mounts:
        if not isinstance(mount, dict):
            continue
        sensor_id = str(mount.get("sensor_id", "")).strip()
        sensor_type = str(mount.get("sensor_type", "")).strip()
        attach_to = str(mount.get("attach_to_actor_id", "")).strip()
        if sensor_id and sensor_type and attach_to:
            if backend == "awsim":
                args.extend(["--mount-sensor", f"{sensor_id}:{sensor_type}:{attach_to}"])
            elif backend == "carla":
                args.extend(["--attach-sensor", f"{sensor_type}:{sensor_id}:{attach_to}"])
        extrinsics = mount.get("extrinsics")
        if not isinstance(extrinsics, dict):
            continue
        tx = str(extrinsics.get("tx", "")).strip()
        ty = str(extrinsics.get("ty", "")).strip()
        tz = str(extrinsics.get("tz", "")).strip()
        roll = str(extrinsics.get("roll_deg", "")).strip()
        pitch = str(extrinsics.get("pitch_deg", "")).strip()
        yaw = str(extrinsics.get("yaw_deg", "")).strip()
        if not sensor_id or not all([tx, ty, tz, roll, pitch, yaw]):
            continue
        if backend == "awsim":
            args.extend(["--mount-pose", f"{sensor_id}:{tx}:{ty}:{tz}:{roll}:{pitch}:{yaw}"])
        elif backend == "carla":
            args.extend(["--sensor-pose", f"{sensor_id}:{tx}:{ty}:{tz}:{roll}:{pitch}:{yaw}"])
    return args


def _load_launcher_args(launcher_template_path: Path | None) -> list[str]:
    if launcher_template_path is None or not launcher_template_path.exists():
        return []
    try:
        payload = json.loads(launcher_template_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, dict):
        return []
    return _coerce_arg_list(payload.get("args", []))


def _sanitize_artifact_key(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_") or "artifact"


def _group_expected_outputs(
    *,
    expected_outputs: list[dict[str, Any]],
    field: str,
    group_key_name: str,
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for entry in expected_outputs:
        group_value = str(entry.get(field, "")).strip()
        if not group_value:
            continue
        summary = groups.setdefault(
            group_value,
            {
                group_key_name: group_value,
                "expected_count": 0,
                "found_count": 0,
                "missing_count": 0,
                "backend_runtime_found_count": 0,
                "sidecar_found_count": 0,
                "artifact_keys": [],
                "sensor_ids": [],
                "found_sensor_ids": [],
                "missing_sensor_ids": [],
                "data_formats": [],
                "carrier_data_formats": [],
                "backend_filenames": [],
                "embedded_output_count": 0,
            },
        )
        summary["expected_count"] += 1
        artifact_key = str(entry.get("artifact_key", "")).strip()
        sensor_id = str(entry.get("sensor_id", "")).strip()
        data_format = str(entry.get("data_format", "")).strip()
        carrier_data_format = str(entry.get("carrier_data_format", "")).strip()
        backend_filename = str(entry.get("backend_filename", "")).strip()
        if artifact_key and artifact_key not in summary["artifact_keys"]:
            summary["artifact_keys"].append(artifact_key)
        if sensor_id and sensor_id not in summary["sensor_ids"]:
            summary["sensor_ids"].append(sensor_id)
        if data_format and data_format not in summary["data_formats"]:
            summary["data_formats"].append(data_format)
        if carrier_data_format and carrier_data_format not in summary["carrier_data_formats"]:
            summary["carrier_data_formats"].append(carrier_data_format)
        if backend_filename and backend_filename not in summary["backend_filenames"]:
            summary["backend_filenames"].append(backend_filename)
        if bool(entry.get("embedded_output", False)):
            summary["embedded_output_count"] += 1
        if "exists" in entry:
            if bool(entry.get("exists", False)):
                summary["found_count"] += 1
                if str(entry.get("output_origin", "")).strip() == "sidecar_materialized":
                    summary["sidecar_found_count"] += 1
                else:
                    summary["backend_runtime_found_count"] += 1
                if sensor_id and sensor_id not in summary["found_sensor_ids"]:
                    summary["found_sensor_ids"].append(sensor_id)
            else:
                summary["missing_count"] += 1
                if sensor_id and sensor_id not in summary["missing_sensor_ids"]:
                    summary["missing_sensor_ids"].append(sensor_id)
    grouped_rows = [groups[key] for key in sorted(groups)]
    for summary in grouped_rows:
        backend_runtime_found_count = int(summary.get("backend_runtime_found_count", 0))
        sidecar_found_count = int(summary.get("sidecar_found_count", 0))
        missing_count = int(summary.get("missing_count", 0))
        expected_count = int(summary.get("expected_count", 0))
        summary["output_origin_status"] = _output_origin_status(
            expected_count=expected_count,
            backend_runtime_count=backend_runtime_found_count,
            sidecar_count=sidecar_found_count,
        )
        summary["output_origin_counts"] = {
            "backend_runtime": backend_runtime_found_count,
            "sidecar_materialized": sidecar_found_count,
            "missing": missing_count,
        }
        summary["output_origin_reasons"] = _output_origin_reasons(
            backend_runtime_count=backend_runtime_found_count,
            sidecar_count=sidecar_found_count,
        )
    return grouped_rows


def _output_smoke_status(
    *,
    expected_count: int,
    found_count: int,
    missing_count: int,
) -> str:
    if expected_count <= 0:
        return "UNOBSERVED"
    if found_count <= 0:
        return "MISSING"
    if missing_count <= 0 and found_count >= expected_count:
        return "COMPLETE"
    return "PARTIAL"


def _output_coverage_ratio(*, expected_count: int, found_count: int) -> float | None:
    if expected_count <= 0:
        return None
    return round(found_count / expected_count, 4)


def _output_origin_status(
    *,
    expected_count: int,
    backend_runtime_count: int,
    sidecar_count: int,
) -> str:
    if expected_count <= 0:
        return "UNOBSERVED"
    found_count = backend_runtime_count + sidecar_count
    if found_count <= 0:
        return "MISSING"
    if backend_runtime_count > 0 and sidecar_count <= 0:
        return "BACKEND_RUNTIME_ONLY"
    if backend_runtime_count <= 0 and sidecar_count > 0:
        return "SIDECAR_ONLY"
    return "MIXED"


def _output_origin_reasons(
    *,
    backend_runtime_count: int,
    sidecar_count: int,
) -> list[str]:
    reasons: list[str] = []
    if sidecar_count > 0:
        reasons.append("SIDECAR_OUTPUTS_PRESENT")
    if sidecar_count > 0 and backend_runtime_count <= 0:
        reasons.append("NO_NATIVE_BACKEND_OUTPUTS")
    return reasons


def _output_comparison_status(
    *,
    expected_count: int,
    missing_count: int,
    unexpected_count: int,
    discovered_count: int,
) -> str:
    if expected_count <= 0 and discovered_count <= 0:
        return "UNOBSERVED"
    if missing_count <= 0 and unexpected_count <= 0:
        return "MATCHED"
    if missing_count > 0 and unexpected_count <= 0:
        return "MISSING_EXPECTED"
    if missing_count <= 0 and unexpected_count > 0:
        return "UNEXPECTED_OUTPUTS"
    return "MIXED"


def _output_comparison_mismatch_reasons(
    *,
    expected_count: int,
    missing_count: int,
    unexpected_count: int,
    discovered_count: int,
    output_root_exists: bool = True,
) -> list[str]:
    reasons: list[str] = []
    if not output_root_exists:
        reasons.append("OUTPUT_ROOT_MISSING")
    if expected_count > 0 and discovered_count <= 0:
        reasons.append("NO_DISCOVERED_FILES")
    if expected_count > 0 and missing_count > 0:
        reasons.append("MISSING_EXPECTED_OUTPUTS")
    if unexpected_count > 0:
        reasons.append("UNEXPECTED_OUTPUTS_PRESENT")
    if expected_count <= 0 and discovered_count > 0:
        reasons.append("UNEXPECTED_DISCOVERED_WITHOUT_CONTRACT")
    return reasons


def _sensor_id_from_relative_output_path(*, relative_path: str, backend: str) -> str:
    parts = PurePosixPath(relative_path).parts
    if len(parts) < 2 or parts[0] != "sensor_exports":
        return ""
    if len(parts) >= 3 and parts[1] == backend:
        return str(parts[2]).strip()
    return str(parts[1]).strip()


def _sensor_export_filename(data_format: str) -> str:
    mapping = {
        "camera_projection_json": "camera_projection.json",
        "camera_depth_json": "camera_depth.json",
        "camera_semantic_json": "camera_semantic.json",
        "lidar_points_xyz": "lidar_points.xyz",
        "lidar_points_json": "lidar_points.json",
        "lidar_points": "lidar_points.dat",
        "radar_targets_json": "radar_targets.json",
        "radar_tracks_json": "radar_tracks.json",
    }
    return mapping.get(data_format, f"{_sanitize_artifact_key(data_format)}.dat")


def _sensor_export_layout(
    *,
    backend: str,
    data_format: str,
) -> tuple[str, str, str, str, str]:
    backend_layout = _BACKEND_SENSOR_EXPORT_LAYOUTS.get(backend, {})
    layout = backend_layout.get(data_format, {})
    modality = str(layout.get("modality", "")).strip() or data_format.split("_", 1)[0]
    backend_filename = str(layout.get("filename", "")).strip() or _sensor_export_filename(data_format)
    output_role = str(layout.get("output_role", "")).strip() or modality
    artifact_type = (
        str(layout.get("artifact_type", "")).strip()
        or f"{backend}_{_sanitize_artifact_key(data_format)}"
    )
    fallback_filename = _sensor_export_filename(data_format)
    return modality, backend_filename, output_role, artifact_type, fallback_filename


def _sensor_export_relative_paths(
    *,
    backend: str,
    sensor_id: str,
    sensor_name: str,
    data_format: str,
) -> tuple[str, list[str]]:
    modality, export_filename, _output_role, _artifact_type, fallback_filename = _sensor_export_layout(
        backend=backend,
        data_format=data_format,
    )
    canonical = f"sensor_exports/{sensor_id}/{export_filename}"
    candidates = [canonical]
    backend_namespaced_with_modality = f"sensor_exports/{backend}/{sensor_id}/{modality}/{export_filename}"
    if backend_namespaced_with_modality not in candidates:
        candidates.append(backend_namespaced_with_modality)
    backend_namespaced = f"sensor_exports/{backend}/{sensor_id}/{export_filename}"
    if backend_namespaced not in candidates:
        candidates.append(backend_namespaced)
    modality_path = f"sensor_exports/{sensor_id}/{modality}/{export_filename}"
    if modality_path not in candidates:
        candidates.append(modality_path)
    sensor_prefixed = f"sensor_exports/{sensor_id}/{sensor_name}_{export_filename}"
    if sensor_prefixed not in candidates:
        candidates.append(sensor_prefixed)
    if fallback_filename != export_filename:
        fallback_canonical = f"sensor_exports/{sensor_id}/{fallback_filename}"
        if fallback_canonical not in candidates:
            candidates.append(fallback_canonical)
        fallback_backend_namespaced = f"sensor_exports/{backend}/{sensor_id}/{fallback_filename}"
        if fallback_backend_namespaced not in candidates:
            candidates.append(fallback_backend_namespaced)
    return canonical, candidates


def _build_sensor_expected_output_entries(
    *,
    backend: str,
    output_root: Path,
    ingestion_profile_path: Path | None,
) -> list[dict[str, Any]]:
    if ingestion_profile_path is None or not ingestion_profile_path.exists():
        return []
    try:
        payload = json.loads(ingestion_profile_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, dict):
        return []
    raw_entries = payload.get("entries")
    if not isinstance(raw_entries, list):
        return []

    expected_outputs: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for entry in raw_entries:
        if not isinstance(entry, dict):
            continue
        sensor_id = str(entry.get("sensor_id", "")).strip()
        data_format = str(entry.get("data_format", "")).strip()
        if not sensor_id or not data_format:
            continue
        artifact_key = f"sensor_output_{_sanitize_artifact_key(sensor_id)}"
        if artifact_key in seen_keys:
            continue
        seen_keys.add(artifact_key)
        sensor_name = str(entry.get("sensor_name", "")).strip() or sensor_id
        modality, export_filename, output_role, artifact_type, _fallback_filename = _sensor_export_layout(
            backend=backend,
            data_format=data_format,
        )
        relative_path, candidate_relative_paths = _sensor_export_relative_paths(
            backend=backend,
            sensor_id=sensor_id,
            sensor_name=sensor_name,
            data_format=data_format,
        )
        base_entry = {
            "artifact_key": artifact_key,
            "backend": backend,
            "modality": modality,
            "backend_filename": export_filename,
            "output_role": output_role,
            "artifact_type": artifact_type,
            "sensor_name": sensor_name,
            "sensor_id": sensor_id,
            "data_format": data_format,
            "kind": "file",
            "required": False,
            "description": f"Expected exported payload for sensor {sensor_id} ({data_format}).",
            "relative_path": relative_path,
            "path": str((output_root / relative_path).resolve()),
            "path_candidates": [
                str((output_root / candidate).resolve()) for candidate in candidate_relative_paths
            ],
            "embedded_output": False,
        }
        expected_outputs.append(base_entry)

        # Radar track exports in the current native/runtime contract carry detections
        # and tracks in the same JSON artifact. Expose the embedded detections as a
        # separate logical output role so backend summaries can reason about both.
        if data_format == "radar_tracks_json":
            (
                embedded_modality,
                _embedded_filename,
                embedded_output_role,
                embedded_artifact_type,
                _embedded_fallback,
            ) = _sensor_export_layout(
                backend=backend,
                data_format="radar_targets_json",
            )
            embedded_artifact_key = f"{artifact_key}_radar_detections"
            if embedded_artifact_key not in seen_keys:
                seen_keys.add(embedded_artifact_key)
                expected_outputs.append(
                    {
                        **base_entry,
                        "artifact_key": embedded_artifact_key,
                        "modality": embedded_modality,
                        "output_role": embedded_output_role,
                        "artifact_type": embedded_artifact_type,
                        "data_format": "radar_targets_json",
                        "description": (
                            f"Embedded radar detections available inside {artifact_key} for sensor {sensor_id}."
                        ),
                        "carrier_data_format": data_format,
                        "embedded_output": True,
                        "embedded_field": "targets",
                        "shared_output_artifact_key": artifact_key,
                    }
                )
    return expected_outputs


def _build_backend_output_spec(
    *,
    backend: str,
    runtime_dir: Path,
    ingestion_profile_path: Path | None,
) -> tuple[Path | None, Path | None, list[dict[str, Any]], dict[str, float]]:
    if backend in {"", "none"}:
        return None, None, [], {
            "renderer_backend_output_spec_written": 0.0,
            "renderer_backend_expected_output_count": 0.0,
        }

    output_root = runtime_dir / "backend_outputs" / backend
    preset_entries = _BACKEND_EXPECTED_OUTPUT_PRESETS.get(backend, [])
    expected_outputs: list[dict[str, Any]] = []
    for entry in preset_entries:
        relative_path = str(entry.get("relative_path", "")).strip()
        artifact_path = (output_root / relative_path).resolve() if relative_path else output_root.resolve()
        expected_outputs.append(
            {
                "artifact_key": str(entry.get("artifact_key", "")).strip(),
                "kind": str(entry.get("kind", "file")).strip() or "file",
                "required": bool(entry.get("required", False)),
                "description": str(entry.get("description", "")).strip(),
                "path": str(artifact_path),
            }
        )
    expected_outputs.extend(
        _build_sensor_expected_output_entries(
            backend=backend,
            output_root=output_root,
            ingestion_profile_path=ingestion_profile_path,
        )
    )
    expected_outputs_by_sensor: dict[str, list[dict[str, Any]]] = {}
    for entry in expected_outputs:
        sensor_id = str(entry.get("sensor_id", "")).strip()
        if not sensor_id:
            continue
        expected_outputs_by_sensor.setdefault(sensor_id, []).append(
            {
                "artifact_key": str(entry.get("artifact_key", "")).strip(),
                "backend": str(entry.get("backend", "")).strip(),
                "modality": str(entry.get("modality", "")).strip(),
                "backend_filename": str(entry.get("backend_filename", "")).strip(),
                "output_role": str(entry.get("output_role", "")).strip(),
                "artifact_type": str(entry.get("artifact_type", "")).strip(),
                "sensor_name": str(entry.get("sensor_name", "")).strip() or sensor_id,
                "data_format": str(entry.get("data_format", "")).strip(),
                "carrier_data_format": str(entry.get("carrier_data_format", "")).strip(),
                "relative_path": str(entry.get("relative_path", "")).strip(),
                "embedded_output": bool(entry.get("embedded_output", False)),
                "embedded_field": str(entry.get("embedded_field", "")).strip(),
                "shared_output_artifact_key": str(
                    entry.get("shared_output_artifact_key", "")
                ).strip(),
                "path_candidates": list(entry.get("path_candidates", []))
                if isinstance(entry.get("path_candidates"), list)
                else [],
            }
        )
    expected_outputs_by_role = _group_expected_outputs(
        expected_outputs=expected_outputs,
        field="output_role",
        group_key_name="output_role",
    )
    expected_outputs_by_artifact_type = _group_expected_outputs(
        expected_outputs=expected_outputs,
        field="artifact_type",
        group_key_name="artifact_type",
    )

    spec_payload = {
        "backend": backend,
        "output_root": str(output_root),
        "expected_output_count": len(expected_outputs),
        "expected_outputs": expected_outputs,
        "expected_sensor_output_count": len(expected_outputs_by_sensor),
        "expected_output_role_count": len(expected_outputs_by_role),
        "expected_artifact_type_count": len(expected_outputs_by_artifact_type),
        "expected_outputs_by_sensor": [
            {
                "sensor_id": sensor_id,
                "output_count": len(outputs),
                "outputs": outputs,
            }
            for sensor_id, outputs in sorted(expected_outputs_by_sensor.items())
        ],
        "expected_outputs_by_role": expected_outputs_by_role,
        "expected_outputs_by_artifact_type": expected_outputs_by_artifact_type,
    }
    spec_path = runtime_dir / "backend_output_spec.json"
    spec_path.write_text(json.dumps(spec_payload, indent=2), encoding="utf-8")
    return spec_path, output_root, expected_outputs, {
        "renderer_backend_output_spec_written": 1.0,
        "renderer_backend_expected_output_count": float(len(expected_outputs)),
    }


def build_backend_runner_artifacts(
    *,
    options: dict[str, Any],
    backend: str,
    cwd: Path,
    runtime_dir: Path,
    command_source: str,
    backend_wrapper_used: bool,
    backend_args_preview: dict[str, Any] | None,
    frame_manifest_path: Path | None,
    ingestion_profile_path: Path | None,
    bundle_summary_path: Path | None,
    launcher_template_path: Path | None,
) -> tuple[dict[str, Path], dict[str, float]]:
    if backend in {"", "none"}:
        return {}, {
            "renderer_backend_runner_request_written": 0.0,
            "renderer_backend_runner_command_written": 0.0,
            "renderer_backend_runner_arg_count": 0.0,
            "renderer_backend_runner_scene_arg_count": 0.0,
            "renderer_backend_runner_mount_arg_count": 0.0,
            "renderer_backend_runner_ingestion_arg_count": 0.0,
            "renderer_backend_output_spec_written": 0.0,
            "renderer_backend_expected_output_count": 0.0,
        }

    resolved_backend_bin, backend_bin_source, backend_bin_env_key = _resolve_direct_backend_bin(
        options=options,
        backend=backend,
    )
    output_spec_path, output_root, expected_outputs, output_spec_metrics = _build_backend_output_spec(
        backend=backend,
        runtime_dir=runtime_dir,
        ingestion_profile_path=ingestion_profile_path,
    )
    extra_args = _collect_extra_args(options=options, backend=backend)
    scene_args = _collect_scene_args(backend_args_preview)
    mount_args = _collect_mount_args(
        backend=backend,
        backend_args_preview=backend_args_preview,
    )
    ingestion_args = _load_launcher_args(launcher_template_path)
    command = [resolved_backend_bin, *extra_args, *scene_args, *mount_args, *ingestion_args]

    request_payload = {
        "backend": backend,
        "cwd": str(cwd),
        "backend_wrapper_used": backend_wrapper_used,
        "source_command_source": command_source,
        "runner_mode": "direct_backend",
        "resolved_backend_bin": resolved_backend_bin,
        "backend_bin_source": backend_bin_source,
        "backend_bin_env_key": backend_bin_env_key,
        "command_arg_count": len(command),
        "command": command,
        "extra_args": extra_args,
        "scene_args": scene_args,
        "mount_args": mount_args,
        "ingestion_args": ingestion_args,
        "output_root": str(output_root) if output_root is not None else None,
        "output_spec_path": str(output_spec_path) if output_spec_path is not None else None,
        "expected_outputs": expected_outputs,
        "env": {
            "BACKEND_OUTPUT_ROOT": str(output_root) if output_root is not None else "",
            "BACKEND_OUTPUT_SPEC_PATH": str(output_spec_path) if output_spec_path is not None else "",
        },
        "artifacts": {
            "backend_frame_inputs_manifest": str(frame_manifest_path) if frame_manifest_path else None,
            "backend_ingestion_profile": str(ingestion_profile_path) if ingestion_profile_path else None,
            "backend_sensor_bundle_summary": str(bundle_summary_path) if bundle_summary_path else None,
            "backend_launcher_template": str(launcher_template_path) if launcher_template_path else None,
            "backend_output_spec": str(output_spec_path) if output_spec_path else None,
        },
    }
    request_path = runtime_dir / "backend_runner_request.json"
    request_path.write_text(json.dumps(request_payload, indent=2), encoding="utf-8")

    shell_path = runtime_dir / "backend_direct_run_command.sh"
    command_line = " ".join(shlex.quote(token) for token in command)
    shell_path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "",
                "# Generated by backend runner.",
                command_line,
                "",
            ]
        ),
        encoding="utf-8",
    )
    shell_path.chmod(0o755)

    artifacts = {
        "backend_runner_request": request_path,
        "backend_direct_run_command": shell_path,
    }
    if output_spec_path is not None:
        artifacts["backend_output_spec"] = output_spec_path

    return artifacts, {
        "renderer_backend_runner_request_written": 1.0,
        "renderer_backend_runner_command_written": 1.0,
        "renderer_backend_runner_arg_count": float(len(command)),
        "renderer_backend_runner_scene_arg_count": float(len(scene_args)),
        "renderer_backend_runner_mount_arg_count": float(len(mount_args)),
        "renderer_backend_runner_ingestion_arg_count": float(len(ingestion_args)),
        **output_spec_metrics,
    }


def _write_execution_manifest(
    *,
    path: Path,
    request_path: Path,
    status: str,
    message: str,
    return_code: int | None,
    payload: dict[str, Any] | None,
    expected_outputs: list[dict[str, Any]] | None,
    found_expected_outputs: int = 0,
    missing_expected_outputs: int = 0,
    sensor_output_summary_path: Path | None,
    output_smoke_report_path: Path | None,
    output_smoke_report: dict[str, Any] | None,
    output_comparison_report_path: Path | None,
    output_comparison_report: dict[str, Any] | None,
    sidecar_materialization_report_path: Path | None,
    sidecar_materialization_report: dict[str, Any] | None,
    stdout_path: Path | None,
    stderr_path: Path | None,
) -> None:
    outputs = expected_outputs if isinstance(expected_outputs, list) else []
    manifest = {
        "request_path": str(request_path),
        "backend": str(payload.get("backend", "")) if isinstance(payload, dict) else "",
        "status": status,
        "message": message,
        "return_code": return_code,
        "cwd": str(payload.get("cwd", "")) if isinstance(payload, dict) else "",
        "runner_mode": str(payload.get("runner_mode", "")) if isinstance(payload, dict) else "",
        "command": payload.get("command", []) if isinstance(payload, dict) else [],
        "expected_output_summary": {
            "found_count": found_expected_outputs,
            "missing_count": missing_expected_outputs,
            "by_output_role": _group_expected_outputs(
                expected_outputs=outputs,
                field="output_role",
                group_key_name="output_role",
            ),
            "by_artifact_type": _group_expected_outputs(
                expected_outputs=outputs,
                field="artifact_type",
                group_key_name="artifact_type",
            ),
        },
        "expected_outputs": outputs,
        "output_smoke_report": output_smoke_report if isinstance(output_smoke_report, dict) else None,
        "output_comparison_report": (
            output_comparison_report if isinstance(output_comparison_report, dict) else None
        ),
        "sidecar_materialization_report": (
            sidecar_materialization_report
            if isinstance(sidecar_materialization_report, dict)
            else None
        ),
        "artifacts": {
            "backend_sensor_output_summary": (
                str(sensor_output_summary_path) if sensor_output_summary_path else None
            ),
            "backend_output_smoke_report": (
                str(output_smoke_report_path) if output_smoke_report_path else None
            ),
            "backend_output_comparison_report": (
                str(output_comparison_report_path) if output_comparison_report_path else None
            ),
            "backend_sidecar_materialization_report": (
                str(sidecar_materialization_report_path)
                if sidecar_materialization_report_path
                else None
            ),
            "backend_runner_stdout": str(stdout_path) if stdout_path else None,
            "backend_runner_stderr": str(stderr_path) if stderr_path else None,
        },
    }
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _write_output_inspection_manifest(
    *,
    path: Path,
    request_path: Path,
    payload: dict[str, Any] | None,
    status: str,
    success: bool,
    message: str,
    return_code: int | None,
    sensor_output_summary_path: Path | None,
    output_smoke_report_path: Path | None,
    output_smoke_report: dict[str, Any] | None,
    output_comparison_report_path: Path | None,
    output_comparison_report: dict[str, Any] | None,
) -> None:
    manifest = {
        "request_path": str(request_path),
        "backend": str(payload.get("backend", "")) if isinstance(payload, dict) else "",
        "status": status,
        "success": success,
        "message": message,
        "return_code": return_code,
        "output_root": str(payload.get("output_root", "")) if isinstance(payload, dict) else "",
        "output_smoke_report": output_smoke_report if isinstance(output_smoke_report, dict) else None,
        "output_comparison_report": (
            output_comparison_report if isinstance(output_comparison_report, dict) else None
        ),
        "artifacts": {
            "backend_sensor_output_summary": (
                str(sensor_output_summary_path) if sensor_output_summary_path else None
            ),
            "backend_output_smoke_report": (
                str(output_smoke_report_path) if output_smoke_report_path else None
            ),
            "backend_output_comparison_report": (
                str(output_comparison_report_path) if output_comparison_report_path else None
            ),
        },
    }
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _write_backend_runner_smoke_manifest(
    *,
    path: Path,
    request_path: Path,
    execution_result: BackendRunnerExecutionResult,
    inspection_result: BackendRunnerExecutionResult,
    execution_manifest_path: Path | None,
    inspection_manifest_path: Path | None,
) -> None:
    execution_manifest = _read_json_payload(execution_manifest_path)
    inspection_manifest = _read_json_payload(inspection_manifest_path)
    execution_status = (
        str(execution_manifest.get("status", "")).strip()
        if isinstance(execution_manifest, dict)
        else ""
    )
    inspection_status = (
        str(inspection_manifest.get("status", "")).strip()
        if isinstance(inspection_manifest, dict)
        else ""
    )
    if execution_status == "REQUEST_ERROR" or inspection_status == "REQUEST_ERROR":
        status = "REQUEST_ERROR"
    elif execution_status == "PROCESS_ERROR":
        status = "PROCESS_ERROR"
    elif execution_result.success and inspection_result.success:
        status = "SMOKE_SUCCEEDED"
    elif not execution_result.success and not inspection_result.success:
        status = "SMOKE_FAILED"
    elif not execution_result.success:
        status = "EXECUTION_FAILED"
    else:
        status = "INSPECTION_FAILED"

    success = execution_result.success and inspection_result.success
    if success:
        message = "Backend runner execution and inspection completed."
    elif not execution_result.success and not inspection_result.success:
        message = (
            f"{execution_result.message} Inspection follow-up: {inspection_result.message}"
        )
    elif not execution_result.success:
        message = execution_result.message
    else:
        message = inspection_result.message

    return_code = 0
    if not execution_result.success and execution_result.return_code not in (None, 0):
        return_code = execution_result.return_code
    elif not inspection_result.success and inspection_result.return_code not in (None, 0):
        return_code = inspection_result.return_code
    elif not execution_result.success:
        return_code = execution_result.return_code if execution_result.return_code is not None else 1
    elif not inspection_result.success:
        return_code = (
            inspection_result.return_code if inspection_result.return_code is not None else 1
        )

    backend = ""
    output_root = ""
    if isinstance(execution_manifest, dict):
        backend = str(execution_manifest.get("backend", "")).strip()
    if not backend and isinstance(inspection_manifest, dict):
        backend = str(inspection_manifest.get("backend", "")).strip()
    if isinstance(inspection_manifest, dict):
        output_root = str(inspection_manifest.get("output_root", "")).strip()

    payload = {
        "request_path": str(request_path),
        "backend": backend,
        "status": status,
        "success": success,
        "message": message,
        "return_code": return_code,
        "output_root": output_root,
        "execution": {
            "status": execution_status,
            "success": execution_result.success,
            "message": execution_result.message,
            "return_code": execution_result.return_code,
        },
        "inspection": {
            "status": inspection_status,
            "success": inspection_result.success,
            "message": inspection_result.message,
            "return_code": inspection_result.return_code,
        },
        "artifacts": {
            "backend_runner_execution_manifest": (
                str(execution_manifest_path) if execution_manifest_path else None
            ),
            "backend_output_inspection_manifest": (
                str(inspection_manifest_path) if inspection_manifest_path else None
            ),
            "backend_runner_stdout": (
                str(execution_result.artifacts["backend_runner_stdout"])
                if "backend_runner_stdout" in execution_result.artifacts
                else None
            ),
            "backend_runner_stderr": (
                str(execution_result.artifacts["backend_runner_stderr"])
                if "backend_runner_stderr" in execution_result.artifacts
                else None
            ),
            "backend_sensor_output_summary": (
                str(inspection_result.artifacts["backend_sensor_output_summary"])
                if "backend_sensor_output_summary" in inspection_result.artifacts
                else None
            ),
            "backend_output_smoke_report": (
                str(inspection_result.artifacts["backend_output_smoke_report"])
                if "backend_output_smoke_report" in inspection_result.artifacts
                else None
            ),
            "backend_output_comparison_report": (
                str(inspection_result.artifacts["backend_output_comparison_report"])
                if "backend_output_comparison_report" in inspection_result.artifacts
                else None
            ),
        },
        "output_smoke_report": (
            inspection_manifest.get("output_smoke_report")
            if isinstance(inspection_manifest, dict)
            else None
        ),
        "output_comparison_report": (
            inspection_manifest.get("output_comparison_report")
            if isinstance(inspection_manifest, dict)
            else None
        ),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _normalize_expected_outputs(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    outputs: list[dict[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        artifact_key = str(entry.get("artifact_key", "")).strip()
        path = str(entry.get("path", "")).strip()
        if not artifact_key or not path:
            continue
        outputs.append(
            {
                "artifact_key": artifact_key,
                "path": path,
                "kind": str(entry.get("kind", "file")).strip() or "file",
                "required": bool(entry.get("required", False)),
                "description": str(entry.get("description", "")).strip(),
                "backend": str(entry.get("backend", "")).strip(),
                "modality": str(entry.get("modality", "")).strip(),
                "backend_filename": str(entry.get("backend_filename", "")).strip(),
                "output_role": str(entry.get("output_role", "")).strip(),
                "artifact_type": str(entry.get("artifact_type", "")).strip(),
                "sensor_name": str(entry.get("sensor_name", "")).strip(),
                "sensor_id": str(entry.get("sensor_id", "")).strip(),
                "data_format": str(entry.get("data_format", "")).strip(),
                "carrier_data_format": str(entry.get("carrier_data_format", "")).strip(),
                "relative_path": str(entry.get("relative_path", "")).strip(),
                "embedded_output": bool(entry.get("embedded_output", False)),
                "embedded_field": str(entry.get("embedded_field", "")).strip(),
                "shared_output_artifact_key": str(
                    entry.get("shared_output_artifact_key", "")
                ).strip(),
                "path_candidates": [
                    str(item).strip()
                    for item in entry.get("path_candidates", [])
                    if str(item).strip()
                ]
                if isinstance(entry.get("path_candidates"), list)
                else [],
            }
        )
    return outputs


def _load_request_artifact_payload(
    payload: dict[str, Any],
    *,
    artifact_key: str,
) -> dict[str, Any] | None:
    raw_artifacts = payload.get("artifacts")
    if not isinstance(raw_artifacts, dict):
        return None
    raw_path = str(raw_artifacts.get(artifact_key, "")).strip()
    if not raw_path:
        return None
    return _read_json_payload(Path(raw_path).expanduser())


def _materialize_sidecar_outputs(
    *,
    payload: dict[str, Any],
    expected_outputs: list[dict[str, Any]],
    runner_output_dir: Path,
    backend_return_code: int | None,
) -> tuple[Path | None, dict[str, Path], dict[str, Any] | None]:
    ingestion_profile = _load_request_artifact_payload(
        payload,
        artifact_key="backend_ingestion_profile",
    )
    entries = ingestion_profile.get("entries") if isinstance(ingestion_profile, dict) else None
    normalized_entries = (
        [entry for entry in entries if isinstance(entry, dict)] if isinstance(entries, list) else []
    )

    expected_by_sensor_format: dict[tuple[str, str], list[dict[str, Any]]] = {}
    runtime_state_entry: dict[str, Any] | None = None
    for entry in expected_outputs:
        artifact_key = str(entry.get("artifact_key", "")).strip()
        if artifact_key in {"awsim_runtime_state_json", "carla_runtime_state_json"}:
            runtime_state_entry = entry
        if bool(entry.get("embedded_output", False)):
            continue
        if str(entry.get("kind", "file")).strip() != "file":
            continue
        sensor_id = str(entry.get("sensor_id", "")).strip()
        data_format = str(entry.get("data_format", "")).strip()
        if not sensor_id or not data_format:
            continue
        expected_by_sensor_format.setdefault((sensor_id, data_format), []).append(entry)

    materialized_outputs: list[dict[str, Any]] = []
    skipped_entries: list[dict[str, Any]] = []
    for entry in normalized_entries:
        sensor_id = str(entry.get("sensor_id", "")).strip()
        data_format = str(entry.get("data_format", "")).strip()
        payload_artifact_text = str(entry.get("payload_artifact", "")).strip()
        if not sensor_id or not data_format or not payload_artifact_text:
            skipped_entries.append(
                {
                    "sensor_id": sensor_id or None,
                    "data_format": data_format or None,
                    "reason": "incomplete_entry",
                }
            )
            continue
        payload_artifact_path = Path(payload_artifact_text).expanduser()
        if not payload_artifact_path.exists() or not payload_artifact_path.is_file():
            skipped_entries.append(
                {
                    "sensor_id": sensor_id,
                    "data_format": data_format,
                    "payload_artifact": str(payload_artifact_path),
                    "reason": "payload_artifact_missing",
                }
            )
            continue
        for expected_entry in expected_by_sensor_format.get((sensor_id, data_format), []):
            target_path = Path(str(expected_entry.get("path", ""))).expanduser()
            if not str(target_path):
                continue
            if target_path.exists():
                skipped_entries.append(
                    {
                        "sensor_id": sensor_id,
                        "data_format": data_format,
                        "artifact_key": str(expected_entry.get("artifact_key", "")).strip() or None,
                        "target_path": str(target_path),
                        "reason": "target_exists",
                    }
                )
                continue
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(payload_artifact_path.read_bytes())
            materialized_outputs.append(
                {
                    "sensor_id": sensor_id,
                    "sensor_name": str(entry.get("sensor_name", "")).strip() or None,
                    "data_format": data_format,
                    "payload_artifact": str(payload_artifact_path),
                    "artifact_key": str(expected_entry.get("artifact_key", "")).strip() or None,
                    "output_role": str(expected_entry.get("output_role", "")).strip() or None,
                    "artifact_type": str(expected_entry.get("artifact_type", "")).strip() or None,
                    "target_path": str(target_path),
                }
            )

    runtime_state_materialized = False
    runtime_state_target_path: str | None = None
    runtime_state_artifact_key: str | None = None
    if runtime_state_entry is not None:
        runtime_state_path = Path(str(runtime_state_entry.get("path", ""))).expanduser()
        runtime_state_target_path = str(runtime_state_path) if str(runtime_state_path) else None
        runtime_state_artifact_key = (
            str(runtime_state_entry.get("artifact_key", "")).strip() or None
        )
        if str(runtime_state_path) and not runtime_state_path.exists():
            runtime_state_path.parent.mkdir(parents=True, exist_ok=True)
            runtime_state_payload = {
                "backend": str(payload.get("backend", "")).strip() or None,
                "status": "sidecar_materialized",
                "export_mode": "sidecar_materialized",
                "backend_return_code": backend_return_code,
                "materialized_output_count": len(materialized_outputs),
                "materialized_outputs": materialized_outputs,
                "skipped_entry_count": len(skipped_entries),
            }
            runtime_state_path.write_text(
                json.dumps(runtime_state_payload, indent=2),
                encoding="utf-8",
            )
            runtime_state_materialized = True

    report_payload = {
        "backend": str(payload.get("backend", "")).strip() or None,
        "status": "MATERIALIZED"
        if materialized_outputs or runtime_state_materialized
        else "NO_ACTION",
        "backend_return_code": backend_return_code,
        "materialized_output_count": len(materialized_outputs),
        "runtime_state_materialized": runtime_state_materialized,
        "runtime_state_target_path": runtime_state_target_path,
        "runtime_state_artifact_key": runtime_state_artifact_key,
        "materialized_outputs": materialized_outputs,
        "skipped_entry_count": len(skipped_entries),
        "skipped_entries": skipped_entries,
    }
    report_path = runner_output_dir / "backend_sidecar_materialization_report.json"
    report_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")
    return (
        report_path,
        {"backend_sidecar_materialization_report": report_path},
        report_payload,
    )


def _inspect_expected_outputs(
    expected_outputs: list[dict[str, Any]],
    *,
    sidecar_materialization_report: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Path], int, int]:
    sidecar_target_paths: set[str] = set()
    if isinstance(sidecar_materialization_report, dict):
        for output in sidecar_materialization_report.get("materialized_outputs", []):
            if not isinstance(output, dict):
                continue
            target_path = str(output.get("target_path", "")).strip()
            if target_path:
                sidecar_target_paths.add(str(Path(target_path).expanduser().resolve()))
        if bool(sidecar_materialization_report.get("runtime_state_materialized")):
            runtime_state_target_path = str(
                sidecar_materialization_report.get("runtime_state_target_path", "")
            ).strip()
            if runtime_state_target_path:
                sidecar_target_paths.add(
                    str(Path(runtime_state_target_path).expanduser().resolve())
                )

    inspected: list[dict[str, Any]] = []
    artifacts: dict[str, Path] = {}
    found_count = 0
    missing_count = 0
    for entry in expected_outputs:
        candidate_values = [str(entry.get("path", "")).strip()]
        raw_candidates = entry.get("path_candidates")
        if isinstance(raw_candidates, list):
            candidate_values.extend(str(item).strip() for item in raw_candidates)
        candidate_paths: list[Path] = []
        seen_candidates: set[str] = set()
        for value in candidate_values:
            if not value or value in seen_candidates:
                continue
            seen_candidates.add(value)
            candidate_paths.append(Path(value).expanduser())
        resolved_path = next((candidate for candidate in candidate_paths if candidate.exists()), None)
        path = resolved_path or (candidate_paths[0] if candidate_paths else Path(""))
        exists = resolved_path is not None
        output_origin = "missing"
        if exists:
            resolved_key = str(path.resolve())
            output_origin = (
                "sidecar_materialized"
                if resolved_key in sidecar_target_paths
                else "backend_runtime"
            )
        if exists:
            found_count += 1
        else:
            missing_count += 1
        record = {
            **entry,
            "exists": exists,
            "is_dir": path.is_dir() if exists else False,
            "resolved_path": str(path) if exists else None,
            "inspected_paths": [str(candidate) for candidate in candidate_paths],
            "output_origin": output_origin,
            "sidecar_materialized": output_origin == "sidecar_materialized",
        }
        if exists and path.is_file():
            record["size_bytes"] = path.stat().st_size
        inspected.append(record)
        if exists:
            artifacts[str(entry.get("artifact_key"))] = path
    return inspected, artifacts, found_count, missing_count


def _build_sensor_output_summary(
    *,
    expected_outputs: list[dict[str, Any]],
    output_dir: Path,
) -> tuple[Path | None, dict[str, Path]]:
    sensor_entries = [
        entry
        for entry in expected_outputs
        if str(entry.get("sensor_id", "")).strip()
    ]
    if not sensor_entries:
        return None, {}

    sensors: dict[str, dict[str, Any]] = {}
    found_sensor_count = 0
    missing_sensor_count = 0
    for entry in sensor_entries:
        sensor_id = str(entry.get("sensor_id", "")).strip()
        if not sensor_id:
            continue
        sensor_summary = sensors.setdefault(
            sensor_id,
            {
                "sensor_id": sensor_id,
                "sensor_name": str(entry.get("sensor_name", "")).strip() or sensor_id,
                "backend": str(entry.get("backend", "")).strip(),
                "modality": str(entry.get("modality", "")).strip(),
                "available": False,
                "output_count": 0,
                "found_output_count": 0,
                "missing_output_count": 0,
                "backend_runtime_output_count": 0,
                "sidecar_output_count": 0,
                "output_role_counts": {},
                "artifact_type_counts": {},
                "found_output_roles": [],
                "missing_output_roles": [],
                "outputs": [],
            },
        )
        exists = bool(entry.get("exists", False))
        output_role = str(entry.get("output_role", "")).strip()
        artifact_type = str(entry.get("artifact_type", "")).strip()
        sensor_summary["output_count"] += 1
        if exists:
            sensor_summary["found_output_count"] += 1
            sensor_summary["available"] = True
            if str(entry.get("output_origin", "")).strip() == "sidecar_materialized":
                sensor_summary["sidecar_output_count"] += 1
            else:
                sensor_summary["backend_runtime_output_count"] += 1
        else:
            sensor_summary["missing_output_count"] += 1
        if output_role:
            role_counts = sensor_summary["output_role_counts"]
            role_counts[output_role] = int(role_counts.get(output_role, 0)) + 1
            target_list = (
                sensor_summary["found_output_roles"]
                if exists
                else sensor_summary["missing_output_roles"]
            )
            if output_role not in target_list:
                target_list.append(output_role)
        if artifact_type:
            artifact_counts = sensor_summary["artifact_type_counts"]
            artifact_counts[artifact_type] = int(artifact_counts.get(artifact_type, 0)) + 1
        sensor_summary["outputs"].append(
            {
                "artifact_key": str(entry.get("artifact_key", "")).strip(),
                "backend_filename": str(entry.get("backend_filename", "")).strip(),
                "modality": str(entry.get("modality", "")).strip(),
                "output_role": output_role,
                "artifact_type": artifact_type,
                "data_format": str(entry.get("data_format", "")).strip(),
                "carrier_data_format": str(entry.get("carrier_data_format", "")).strip(),
                "relative_path": str(entry.get("relative_path", "")).strip(),
                "resolved_path": str(entry.get("resolved_path", "")).strip() or None,
                "exists": exists,
                "output_origin": str(entry.get("output_origin", "")).strip() or "missing",
                "sidecar_materialized": bool(entry.get("sidecar_materialized", False)),
                "embedded_output": bool(entry.get("embedded_output", False)),
                "embedded_field": str(entry.get("embedded_field", "")).strip(),
                "shared_output_artifact_key": str(
                    entry.get("shared_output_artifact_key", "")
                ).strip(),
            }
        )

    for sensor_summary in sensors.values():
        expected_count = int(sensor_summary.get("output_count", 0))
        found_count = int(sensor_summary.get("found_output_count", 0))
        missing_count = int(sensor_summary.get("missing_output_count", 0))
        backend_runtime_output_count = int(
            sensor_summary.get("backend_runtime_output_count", 0)
        )
        sidecar_output_count = int(sensor_summary.get("sidecar_output_count", 0))
        sensor_summary["status"] = _output_smoke_status(
            expected_count=expected_count,
            found_count=found_count,
            missing_count=missing_count,
        )
        sensor_summary["coverage_ratio"] = _output_coverage_ratio(
            expected_count=expected_count,
            found_count=found_count,
        )
        sensor_summary["output_origin_status"] = _output_origin_status(
            expected_count=expected_count,
            backend_runtime_count=backend_runtime_output_count,
            sidecar_count=sidecar_output_count,
        )
        sensor_summary["output_origin_counts"] = {
            "backend_runtime": backend_runtime_output_count,
            "sidecar_materialized": sidecar_output_count,
            "missing": missing_count,
        }
        sensor_summary["output_origin_reasons"] = _output_origin_reasons(
            backend_runtime_count=backend_runtime_output_count,
            sidecar_count=sidecar_output_count,
        )
        if bool(sensor_summary.get("available")):
            found_sensor_count += 1
        else:
            missing_sensor_count += 1
    output_role_counts: dict[str, int] = {}
    artifact_type_counts: dict[str, int] = {}
    for entry in sensor_entries:
        output_role = str(entry.get("output_role", "")).strip()
        artifact_type = str(entry.get("artifact_type", "")).strip()
        if output_role:
            output_role_counts[output_role] = int(output_role_counts.get(output_role, 0)) + 1
        if artifact_type:
            artifact_type_counts[artifact_type] = int(artifact_type_counts.get(artifact_type, 0)) + 1
    backend_runtime_output_count = sum(
        1
        for entry in sensor_entries
        if bool(entry.get("exists", False))
        and str(entry.get("output_origin", "")).strip() != "sidecar_materialized"
    )
    sidecar_output_count = sum(
        1
        for entry in sensor_entries
        if bool(entry.get("exists", False))
        and str(entry.get("output_origin", "")).strip() == "sidecar_materialized"
    )

    status_counts: dict[str, int] = {}
    for sensor_summary in sensors.values():
        status = str(sensor_summary.get("status", "")).strip()
        if not status:
            continue
        status_counts[status] = int(status_counts.get(status, 0)) + 1

    summary_payload = {
        "sensor_count": len(sensors),
        "found_sensor_count": found_sensor_count,
        "missing_sensor_count": missing_sensor_count,
        "status": _output_smoke_status(
            expected_count=len(sensors),
            found_count=found_sensor_count,
            missing_count=missing_sensor_count,
        ),
        "coverage_ratio": _output_coverage_ratio(
            expected_count=len(sensors),
            found_count=found_sensor_count,
        ),
        "output_origin_status": _output_origin_status(
            expected_count=len(sensor_entries),
            backend_runtime_count=backend_runtime_output_count,
            sidecar_count=sidecar_output_count,
        ),
        "output_origin_counts": {
            "backend_runtime": backend_runtime_output_count,
            "sidecar_materialized": sidecar_output_count,
            "missing": sum(
                1 for entry in sensor_entries if not bool(entry.get("exists", False))
            ),
        },
        "output_origin_reasons": _output_origin_reasons(
            backend_runtime_count=backend_runtime_output_count,
            sidecar_count=sidecar_output_count,
        ),
        "status_counts": status_counts,
        "output_role_counts": output_role_counts,
        "artifact_type_counts": artifact_type_counts,
        "output_roles": _group_expected_outputs(
            expected_outputs=sensor_entries,
            field="output_role",
            group_key_name="output_role",
        ),
        "artifact_types": _group_expected_outputs(
            expected_outputs=sensor_entries,
            field="artifact_type",
            group_key_name="artifact_type",
        ),
        "sensors": [sensors[key] for key in sorted(sensors)],
    }
    summary_path = output_dir / "backend_sensor_output_summary.json"
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    return summary_path, {"backend_sensor_output_summary": summary_path}


def _build_backend_output_smoke_report(
    *,
    expected_outputs: list[dict[str, Any]],
    output_dir: Path,
) -> tuple[Path, dict[str, Path], dict[str, Any]]:
    found_count = sum(1 for entry in expected_outputs if bool(entry.get("exists", False)))
    missing_count = max(0, len(expected_outputs) - found_count)
    backend_runtime_found_count = sum(
        1
        for entry in expected_outputs
        if bool(entry.get("exists", False))
        and str(entry.get("output_origin", "")).strip() != "sidecar_materialized"
    )
    sidecar_found_count = sum(
        1
        for entry in expected_outputs
        if bool(entry.get("exists", False))
        and str(entry.get("output_origin", "")).strip() == "sidecar_materialized"
    )
    found_artifact_keys = sorted(
        {
            str(entry.get("artifact_key", "")).strip()
            for entry in expected_outputs
            if bool(entry.get("exists", False)) and str(entry.get("artifact_key", "")).strip()
        }
    )
    missing_artifact_keys = sorted(
        {
            str(entry.get("artifact_key", "")).strip()
            for entry in expected_outputs
            if not bool(entry.get("exists", False)) and str(entry.get("artifact_key", "")).strip()
        }
    )

    def _annotate_group(
        group: dict[str, Any],
        *,
        expected_field: str = "expected_count",
        found_field: str = "found_count",
        missing_field: str = "missing_count",
    ) -> dict[str, Any]:
        expected_count = int(group.get(expected_field, 0))
        found_group_count = int(group.get(found_field, 0))
        missing_group_count = int(group.get(missing_field, 0))
        return {
            **group,
            "status": _output_smoke_status(
                expected_count=expected_count,
                found_count=found_group_count,
                missing_count=missing_group_count,
            ),
            "coverage_ratio": _output_coverage_ratio(
                expected_count=expected_count,
                found_count=found_group_count,
            ),
        }

    output_role_groups = [
        _annotate_group(group)
        for group in _group_expected_outputs(
            expected_outputs=expected_outputs,
            field="output_role",
            group_key_name="output_role",
        )
    ]
    artifact_type_groups = [
        _annotate_group(group)
        for group in _group_expected_outputs(
            expected_outputs=expected_outputs,
            field="artifact_type",
            group_key_name="artifact_type",
        )
    ]

    sensors: dict[str, dict[str, Any]] = {}
    for entry in expected_outputs:
        sensor_id = str(entry.get("sensor_id", "")).strip()
        if not sensor_id:
            continue
        sensor_summary = sensors.setdefault(
            sensor_id,
            {
                "sensor_id": sensor_id,
                "sensor_name": str(entry.get("sensor_name", "")).strip() or sensor_id,
                "backend": str(entry.get("backend", "")).strip(),
                "modality": str(entry.get("modality", "")).strip(),
                "expected_output_count": 0,
                "found_output_count": 0,
                "missing_output_count": 0,
                "backend_runtime_found_count": 0,
                "sidecar_found_count": 0,
                "artifact_keys": [],
                "found_artifact_keys": [],
                "missing_artifact_keys": [],
                "output_roles": [],
                "artifact_types": [],
            },
        )
        artifact_key = str(entry.get("artifact_key", "")).strip()
        output_role = str(entry.get("output_role", "")).strip()
        artifact_type = str(entry.get("artifact_type", "")).strip()
        exists = bool(entry.get("exists", False))
        sensor_summary["expected_output_count"] += 1
        if exists:
            sensor_summary["found_output_count"] += 1
            if str(entry.get("output_origin", "")).strip() == "sidecar_materialized":
                sensor_summary["sidecar_found_count"] += 1
            else:
                sensor_summary["backend_runtime_found_count"] += 1
        else:
            sensor_summary["missing_output_count"] += 1
        if artifact_key and artifact_key not in sensor_summary["artifact_keys"]:
            sensor_summary["artifact_keys"].append(artifact_key)
        target_keys = (
            sensor_summary["found_artifact_keys"]
            if exists
            else sensor_summary["missing_artifact_keys"]
        )
        if artifact_key and artifact_key not in target_keys:
            target_keys.append(artifact_key)
        if output_role and output_role not in sensor_summary["output_roles"]:
            sensor_summary["output_roles"].append(output_role)
        if artifact_type and artifact_type not in sensor_summary["artifact_types"]:
            sensor_summary["artifact_types"].append(artifact_type)

    sensor_summaries = [
        _annotate_group(
            summary,
            expected_field="expected_output_count",
            found_field="found_output_count",
            missing_field="missing_output_count",
        )
        for summary in (sensors[key] for key in sorted(sensors))
    ]
    sensor_status_counts: dict[str, int] = {}
    for summary in sensor_summaries:
        status = str(summary.get("status", "")).strip()
        if not status:
            continue
        sensor_status_counts[status] = int(sensor_status_counts.get(status, 0)) + 1
        backend_runtime_sensor_count = int(summary.get("backend_runtime_found_count", 0))
        sidecar_sensor_count = int(summary.get("sidecar_found_count", 0))
        expected_sensor_count = int(summary.get("expected_output_count", 0))
        missing_sensor_count = int(summary.get("missing_output_count", 0))
        summary["output_origin_status"] = _output_origin_status(
            expected_count=expected_sensor_count,
            backend_runtime_count=backend_runtime_sensor_count,
            sidecar_count=sidecar_sensor_count,
        )
        summary["output_origin_counts"] = {
            "backend_runtime": backend_runtime_sensor_count,
            "sidecar_materialized": sidecar_sensor_count,
            "missing": missing_sensor_count,
        }
        summary["output_origin_reasons"] = _output_origin_reasons(
            backend_runtime_count=backend_runtime_sensor_count,
            sidecar_count=sidecar_sensor_count,
        )

    report_payload = {
        "status": _output_smoke_status(
            expected_count=len(expected_outputs),
            found_count=found_count,
            missing_count=missing_count,
        ),
        "coverage_ratio": _output_coverage_ratio(
            expected_count=len(expected_outputs),
            found_count=found_count,
        ),
        "output_origin_status": _output_origin_status(
            expected_count=len(expected_outputs),
            backend_runtime_count=backend_runtime_found_count,
            sidecar_count=sidecar_found_count,
        ),
        "output_origin_counts": {
            "backend_runtime": backend_runtime_found_count,
            "sidecar_materialized": sidecar_found_count,
            "missing": missing_count,
        },
        "output_origin_reasons": _output_origin_reasons(
            backend_runtime_count=backend_runtime_found_count,
            sidecar_count=sidecar_found_count,
        ),
        "expected_output_count": len(expected_outputs),
        "found_output_count": found_count,
        "missing_output_count": missing_count,
        "found_artifact_keys": found_artifact_keys,
        "missing_artifact_keys": missing_artifact_keys,
        "by_output_role": output_role_groups,
        "by_artifact_type": artifact_type_groups,
        "by_sensor": sensor_summaries,
        "sensor_status_counts": sensor_status_counts,
    }
    report_path = output_dir / "backend_output_smoke_report.json"
    report_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")
    return report_path, {"backend_output_smoke_report": report_path}, report_payload


def _build_backend_output_comparison_report(
    *,
    payload: dict[str, Any],
    expected_outputs: list[dict[str, Any]],
    output_dir: Path,
) -> tuple[Path | None, dict[str, Path], dict[str, Any] | None]:
    raw_output_root = str(payload.get("output_root", "")).strip()
    if not raw_output_root:
        raw_env = payload.get("env")
        if isinstance(raw_env, dict):
            raw_output_root = str(raw_env.get("BACKEND_OUTPUT_ROOT", "")).strip()
    if not raw_output_root:
        return None, {}, None

    output_root = Path(raw_output_root).expanduser().resolve()
    if not output_root.exists():
        backend_runtime_found_count = sum(
            1
            for entry in expected_outputs
            if bool(entry.get("exists", False))
            and str(entry.get("output_origin", "")).strip() != "sidecar_materialized"
        )
        sidecar_found_count = sum(
            1
            for entry in expected_outputs
            if bool(entry.get("exists", False))
            and str(entry.get("output_origin", "")).strip() == "sidecar_materialized"
        )
        report_payload = {
            "status": "UNOBSERVED",
            "mismatch_reasons": _output_comparison_mismatch_reasons(
                expected_count=len(expected_outputs),
                missing_count=len(expected_outputs),
                unexpected_count=0,
                discovered_count=0,
                output_root_exists=False,
            ),
            "output_root": str(output_root),
            "output_root_exists": False,
            "expected_output_count": len(expected_outputs),
            "output_origin_status": _output_origin_status(
                expected_count=len(expected_outputs),
                backend_runtime_count=backend_runtime_found_count,
                sidecar_count=sidecar_found_count,
            ),
            "output_origin_counts": {
                "backend_runtime": backend_runtime_found_count,
                "sidecar_materialized": sidecar_found_count,
                "missing": len(expected_outputs),
            },
            "output_origin_reasons": _output_origin_reasons(
                backend_runtime_count=backend_runtime_found_count,
                sidecar_count=sidecar_found_count,
            ),
            "discovered_file_count": 0,
            "matched_file_count": 0,
            "unexpected_output_count": 0,
            "canonical_match_count": 0,
            "candidate_match_count": 0,
            "embedded_match_count": 0,
            "unexpected_outputs": [],
            "matched_outputs": [],
            "by_sensor": [],
        }
        report_path = output_dir / "backend_output_comparison_report.json"
        report_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")
        return report_path, {"backend_output_comparison_report": report_path}, report_payload

    path_index: dict[str, list[dict[str, Any]]] = {}
    for entry in expected_outputs:
        candidate_values: list[str] = []
        raw_inspected_paths = entry.get("inspected_paths")
        if isinstance(raw_inspected_paths, list):
            candidate_values.extend(str(item).strip() for item in raw_inspected_paths)
        resolved_path = str(entry.get("resolved_path", "")).strip()
        if resolved_path:
            candidate_values.append(resolved_path)
        canonical_path = str(entry.get("path", "")).strip()
        if canonical_path:
            candidate_values.append(canonical_path)
        for candidate_value in candidate_values:
            if not candidate_value:
                continue
            candidate_key = str(Path(candidate_value).expanduser().resolve())
            indexed_entries = path_index.setdefault(candidate_key, [])
            artifact_key = str(entry.get("artifact_key", "")).strip()
            if artifact_key and any(
                str(item.get("artifact_key", "")).strip() == artifact_key
                for item in indexed_entries
            ):
                continue
            indexed_entries.append(entry)

    discovered_files = sorted(path for path in output_root.rglob("*") if path.is_file())
    matched_outputs: list[dict[str, Any]] = []
    unexpected_outputs: list[dict[str, Any]] = []
    matched_file_count = 0
    unexpected_output_count = 0
    canonical_match_count = 0
    candidate_match_count = 0
    embedded_match_count = 0
    sensor_discovery_counts: dict[str, int] = {}
    sensor_unexpected_counts: dict[str, int] = {}
    sensor_unexpected_relative_paths: dict[str, list[str]] = {}
    sensor_role_discovery_paths: dict[str, dict[str, list[str]]] = {}
    sensor_role_match_types: dict[str, dict[str, list[str]]] = {}
    sensor_role_backend_filenames: dict[str, dict[str, list[str]]] = {}
    for discovered_file in discovered_files:
        key = str(discovered_file.resolve())
        matching_entries = path_index.get(key, [])
        relative_path = (
            str(discovered_file.relative_to(output_root))
            if discovered_file.is_relative_to(output_root)
            else discovered_file.name
        )
        if not matching_entries:
            unexpected_output_count += 1
            unexpected_outputs.append(
                {
                    "path": key,
                    "relative_path": relative_path,
                    "backend_filename": discovered_file.name,
                    "size_bytes": discovered_file.stat().st_size,
                }
            )
            sensor_guess = _sensor_id_from_relative_output_path(
                relative_path=relative_path,
                backend=str(payload.get("backend", "")).strip(),
            )
            if sensor_guess:
                sensor_unexpected_counts[sensor_guess] = (
                    int(sensor_unexpected_counts.get(sensor_guess, 0)) + 1
                )
                sensor_unexpected_relative_paths.setdefault(sensor_guess, []).append(relative_path)
            continue

        matched_file_count += 1
        artifact_keys: list[str] = []
        sensor_ids: list[str] = []
        output_roles: list[str] = []
        artifact_types: list[str] = []
        match_types: list[str] = []
        for entry in matching_entries:
            artifact_key = str(entry.get("artifact_key", "")).strip()
            sensor_id = str(entry.get("sensor_id", "")).strip()
            output_role = str(entry.get("output_role", "")).strip()
            artifact_type = str(entry.get("artifact_type", "")).strip()
            canonical_path_raw = str(entry.get("path", "")).strip()
            canonical_path = (
                str(Path(canonical_path_raw).expanduser().resolve())
                if canonical_path_raw
                else ""
            )
            embedded_output = bool(entry.get("embedded_output", False))
            if embedded_output:
                match_type = "EMBEDDED_SHARED"
                embedded_match_count += 1
            elif canonical_path and key == canonical_path:
                match_type = "CANONICAL"
                canonical_match_count += 1
            else:
                match_type = "CANDIDATE"
                candidate_match_count += 1
            match_types.append(match_type)
            if artifact_key and artifact_key not in artifact_keys:
                artifact_keys.append(artifact_key)
            if sensor_id and sensor_id not in sensor_ids:
                sensor_ids.append(sensor_id)
                sensor_discovery_counts[sensor_id] = int(sensor_discovery_counts.get(sensor_id, 0)) + 1
            if output_role and output_role not in output_roles:
                output_roles.append(output_role)
            if sensor_id and output_role:
                role_paths = sensor_role_discovery_paths.setdefault(sensor_id, {}).setdefault(
                    output_role,
                    [],
                )
                if relative_path not in role_paths:
                    role_paths.append(relative_path)
                role_match_types = sensor_role_match_types.setdefault(sensor_id, {}).setdefault(
                    output_role,
                    [],
                )
                if match_type not in role_match_types:
                    role_match_types.append(match_type)
                role_backend_filenames = sensor_role_backend_filenames.setdefault(
                    sensor_id,
                    {},
                ).setdefault(output_role, [])
                if discovered_file.name not in role_backend_filenames:
                    role_backend_filenames.append(discovered_file.name)
            if artifact_type and artifact_type not in artifact_types:
                artifact_types.append(artifact_type)
        matched_outputs.append(
            {
                "path": key,
                "relative_path": relative_path,
                "backend_filename": discovered_file.name,
                "size_bytes": discovered_file.stat().st_size,
                "artifact_keys": artifact_keys,
                "sensor_ids": sensor_ids,
                "output_roles": output_roles,
                "artifact_types": artifact_types,
                "match_types": sorted(set(match_types)),
            }
        )

    by_sensor: list[dict[str, Any]] = []
    expected_sensor_ids = sorted(
        {
            str(entry.get("sensor_id", "")).strip()
            for entry in expected_outputs
            if str(entry.get("sensor_id", "")).strip()
        }
    )
    for sensor_id in expected_sensor_ids:
        sensor_entries = [
            entry for entry in expected_outputs if str(entry.get("sensor_id", "")).strip() == sensor_id
        ]
        expected_count = len(sensor_entries)
        missing_count = sum(1 for entry in sensor_entries if not bool(entry.get("exists", False)))
        found_count = sum(1 for entry in sensor_entries if bool(entry.get("exists", False)))
        backend_runtime_found_count = sum(
            1
            for entry in sensor_entries
            if bool(entry.get("exists", False))
            and str(entry.get("output_origin", "")).strip() != "sidecar_materialized"
        )
        sidecar_found_count = sum(
            1
            for entry in sensor_entries
            if bool(entry.get("exists", False))
            and str(entry.get("output_origin", "")).strip() == "sidecar_materialized"
        )
        discovered_count = int(sensor_discovery_counts.get(sensor_id, 0))
        unexpected_count = int(sensor_unexpected_counts.get(sensor_id, 0))
        found_output_roles = sorted(
            {
                str(entry.get("output_role", "")).strip()
                for entry in sensor_entries
                if bool(entry.get("exists", False)) and str(entry.get("output_role", "")).strip()
            }
        )
        missing_output_roles = sorted(
            {
                str(entry.get("output_role", "")).strip()
                for entry in sensor_entries
                if not bool(entry.get("exists", False)) and str(entry.get("output_role", "")).strip()
            }
        )
        matched_relative_paths = sorted(
            {
                str(entry.get("relative_path", "")).strip()
                for entry in sensor_entries
                if bool(entry.get("exists", False)) and str(entry.get("relative_path", "")).strip()
            }
        )
        unexpected_relative_paths = sorted(
            sensor_unexpected_relative_paths.get(sensor_id, [])
        )
        expected_roles = sorted(
            {
                str(entry.get("output_role", "")).strip()
                for entry in sensor_entries
                if str(entry.get("output_role", "")).strip()
            }
        )
        role_diffs: list[dict[str, Any]] = []
        sensor_mismatch_reasons = _output_comparison_mismatch_reasons(
            expected_count=expected_count,
            missing_count=missing_count,
            unexpected_count=unexpected_count,
            discovered_count=discovered_count,
        )
        for output_role in expected_roles:
            role_entries = [
                entry
                for entry in sensor_entries
                if str(entry.get("output_role", "")).strip() == output_role
            ]
            role_expected_count = len(role_entries)
            role_found_count = sum(1 for entry in role_entries if bool(entry.get("exists", False)))
            role_missing_count = max(0, role_expected_count - role_found_count)
            role_discovered_paths = sorted(
                sensor_role_discovery_paths.get(sensor_id, {}).get(output_role, [])
            )
            role_match_types = sorted(
                sensor_role_match_types.get(sensor_id, {}).get(output_role, [])
            )
            role_expected_backend_filenames = sorted(
                {
                    str(entry.get("backend_filename", "")).strip()
                    for entry in role_entries
                    if str(entry.get("backend_filename", "")).strip()
                }
            )
            role_discovered_backend_filenames = sorted(
                sensor_role_backend_filenames.get(sensor_id, {}).get(output_role, [])
            )
            role_mismatch_reasons = _output_comparison_mismatch_reasons(
                expected_count=role_expected_count,
                missing_count=role_missing_count,
                unexpected_count=0,
                discovered_count=len(role_discovered_paths),
            )
            if (
                role_discovered_backend_filenames
                and set(role_discovered_backend_filenames) != set(role_expected_backend_filenames)
            ):
                role_mismatch_reasons.append("BACKEND_FILENAME_MISMATCH")
                if "BACKEND_FILENAME_MISMATCH" not in sensor_mismatch_reasons:
                    sensor_mismatch_reasons.append("BACKEND_FILENAME_MISMATCH")
            role_diffs.append(
                {
                    "output_role": output_role,
                    "status": _output_comparison_status(
                        expected_count=role_expected_count,
                        missing_count=role_missing_count,
                        unexpected_count=0,
                        discovered_count=len(role_discovered_paths),
                    ),
                    "mismatch_reasons": role_mismatch_reasons,
                    "expected_output_count": role_expected_count,
                    "found_output_count": role_found_count,
                    "missing_output_count": role_missing_count,
                    "discovered_file_count": len(role_discovered_paths),
                    "data_formats": sorted(
                        {
                            str(entry.get("data_format", "")).strip()
                            for entry in role_entries
                            if str(entry.get("data_format", "")).strip()
                        }
                    ),
                    "artifact_types": sorted(
                        {
                            str(entry.get("artifact_type", "")).strip()
                            for entry in role_entries
                            if str(entry.get("artifact_type", "")).strip()
                        }
                    ),
                    "backend_filenames": sorted(
                        {
                            str(entry.get("backend_filename", "")).strip()
                            for entry in role_entries
                            if str(entry.get("backend_filename", "")).strip()
                        }
                    ),
                    "expected_backend_filenames": role_expected_backend_filenames,
                    "discovered_backend_filenames": role_discovered_backend_filenames,
                    "expected_relative_paths": sorted(
                        {
                            str(entry.get("relative_path", "")).strip()
                            for entry in role_entries
                            if str(entry.get("relative_path", "")).strip()
                        }
                    ),
                    "found_relative_paths": role_discovered_paths,
                    "missing_relative_paths": sorted(
                        {
                            str(entry.get("relative_path", "")).strip()
                            for entry in role_entries
                            if not bool(entry.get("exists", False))
                            and str(entry.get("relative_path", "")).strip()
                        }
                    ),
                    "match_types": role_match_types,
                }
            )
        by_sensor.append(
            {
                "sensor_id": sensor_id,
                "sensor_name": str(sensor_entries[0].get("sensor_name", "")).strip() or sensor_id,
                "expected_output_count": expected_count,
                "found_output_count": found_count,
                "missing_output_count": missing_count,
                "discovered_file_count": discovered_count,
                "unexpected_output_count": unexpected_count,
                "backend_runtime_found_count": backend_runtime_found_count,
                "sidecar_found_count": sidecar_found_count,
                "output_origin_status": _output_origin_status(
                    expected_count=expected_count,
                    backend_runtime_count=backend_runtime_found_count,
                    sidecar_count=sidecar_found_count,
                ),
                "output_origin_counts": {
                    "backend_runtime": backend_runtime_found_count,
                    "sidecar_materialized": sidecar_found_count,
                    "missing": missing_count,
                },
                "output_origin_reasons": _output_origin_reasons(
                    backend_runtime_count=backend_runtime_found_count,
                    sidecar_count=sidecar_found_count,
                ),
                "status": _output_comparison_status(
                    expected_count=expected_count,
                    missing_count=missing_count,
                    unexpected_count=unexpected_count,
                    discovered_count=discovered_count,
                ),
                "mismatch_reasons": sensor_mismatch_reasons,
                "output_roles": sorted(
                    {
                        str(entry.get("output_role", "")).strip()
                        for entry in sensor_entries
                        if str(entry.get("output_role", "")).strip()
                    }
                ),
                "found_output_roles": found_output_roles,
                "missing_output_roles": missing_output_roles,
                "artifact_types": sorted(
                    {
                        str(entry.get("artifact_type", "")).strip()
                        for entry in sensor_entries
                        if str(entry.get("artifact_type", "")).strip()
                    }
                ),
                "matched_relative_paths": matched_relative_paths,
                "unexpected_relative_paths": unexpected_relative_paths,
                "role_diffs": role_diffs,
            }
        )

    overall_missing_count = sum(1 for entry in expected_outputs if not bool(entry.get("exists", False)))
    overall_backend_runtime_found_count = sum(
        1
        for entry in expected_outputs
        if bool(entry.get("exists", False))
        and str(entry.get("output_origin", "")).strip() != "sidecar_materialized"
    )
    overall_sidecar_found_count = sum(
        1
        for entry in expected_outputs
        if bool(entry.get("exists", False))
        and str(entry.get("output_origin", "")).strip() == "sidecar_materialized"
    )
    overall_mismatch_reasons = _output_comparison_mismatch_reasons(
        expected_count=len(expected_outputs),
        missing_count=overall_missing_count,
        unexpected_count=unexpected_output_count,
        discovered_count=len(discovered_files),
    )
    if any(
        "BACKEND_FILENAME_MISMATCH" in entry.get("mismatch_reasons", [])
        for entry in by_sensor
        if isinstance(entry, dict)
    ) and "BACKEND_FILENAME_MISMATCH" not in overall_mismatch_reasons:
        overall_mismatch_reasons.append("BACKEND_FILENAME_MISMATCH")
    report_payload = {
        "status": _output_comparison_status(
            expected_count=len(expected_outputs),
            missing_count=overall_missing_count,
            unexpected_count=unexpected_output_count,
            discovered_count=len(discovered_files),
        ),
        "mismatch_reasons": overall_mismatch_reasons,
        "output_root": str(output_root),
        "output_root_exists": True,
        "expected_output_count": len(expected_outputs),
        "output_origin_status": _output_origin_status(
            expected_count=len(expected_outputs),
            backend_runtime_count=overall_backend_runtime_found_count,
            sidecar_count=overall_sidecar_found_count,
        ),
        "output_origin_counts": {
            "backend_runtime": overall_backend_runtime_found_count,
            "sidecar_materialized": overall_sidecar_found_count,
            "missing": overall_missing_count,
        },
        "output_origin_reasons": _output_origin_reasons(
            backend_runtime_count=overall_backend_runtime_found_count,
            sidecar_count=overall_sidecar_found_count,
        ),
        "discovered_file_count": len(discovered_files),
        "matched_file_count": matched_file_count,
        "unexpected_output_count": unexpected_output_count,
        "canonical_match_count": canonical_match_count,
        "candidate_match_count": candidate_match_count,
        "embedded_match_count": embedded_match_count,
        "matched_outputs": matched_outputs,
        "unexpected_outputs": unexpected_outputs,
        "by_sensor": by_sensor,
    }
    report_path = output_dir / "backend_output_comparison_report.json"
    report_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")
    return report_path, {"backend_output_comparison_report": report_path}, report_payload


def execute_backend_runner_request(
    *,
    request_path: Path,
    output_dir: Path | None = None,
) -> BackendRunnerExecutionResult:
    resolved_request_path = request_path.expanduser().resolve()
    runner_output_dir = (
        output_dir.expanduser().resolve()
        if output_dir is not None
        else resolved_request_path.parent
    )
    runner_output_dir.mkdir(parents=True, exist_ok=True)
    execution_manifest_path = runner_output_dir / "backend_runner_execution_manifest.json"
    stdout_path = runner_output_dir / "backend_runner_stdout.log"
    stderr_path = runner_output_dir / "backend_runner_stderr.log"
    artifacts = {
        "backend_runner_execution_manifest": execution_manifest_path,
        "backend_runner_stdout": stdout_path,
        "backend_runner_stderr": stderr_path,
    }

    payload, request_error = _load_backend_runner_request_payload(request_path=resolved_request_path)
    if request_error is not None:
        stderr_path.write_text(request_error, encoding="utf-8")
        _write_execution_manifest(
            path=execution_manifest_path,
            request_path=resolved_request_path,
            status="REQUEST_ERROR",
            message=request_error,
            return_code=None,
            payload=None,
            expected_outputs=None,
            found_expected_outputs=0,
            missing_expected_outputs=0,
            sensor_output_summary_path=None,
            output_smoke_report_path=None,
            output_smoke_report=None,
            output_comparison_report_path=None,
            output_comparison_report=None,
            sidecar_materialization_report_path=None,
            sidecar_materialization_report=None,
            stdout_path=None,
            stderr_path=stderr_path,
        )
        return BackendRunnerExecutionResult(
            success=False,
            message=request_error,
            return_code=None,
            artifacts=artifacts,
        )

    command = payload.get("command")
    if not isinstance(command, list) or not command:
        stderr_path.write_text("backend runner request command must be a non-empty list", encoding="utf-8")
        _write_execution_manifest(
            path=execution_manifest_path,
            request_path=resolved_request_path,
            status="REQUEST_ERROR",
            message="Backend runner request command must be a non-empty list.",
            return_code=None,
            payload=payload,
            expected_outputs=None,
            found_expected_outputs=0,
            missing_expected_outputs=0,
            sensor_output_summary_path=None,
            output_smoke_report_path=None,
            output_smoke_report=None,
            output_comparison_report_path=None,
            output_comparison_report=None,
            sidecar_materialization_report_path=None,
            sidecar_materialization_report=None,
            stdout_path=None,
            stderr_path=stderr_path,
        )
        return BackendRunnerExecutionResult(
            success=False,
            message="Backend runner request command must be a non-empty list.",
            return_code=None,
            artifacts=artifacts,
        )

    cwd = Path(str(payload.get("cwd", "."))).expanduser()
    if not cwd.is_absolute():
        cwd = (resolved_request_path.parent / cwd).resolve()
    command_tokens = [str(item) for item in command]
    env = dict(os.environ)
    raw_env = payload.get("env")
    if isinstance(raw_env, dict):
        for key, value in raw_env.items():
            key_text = str(key).strip()
            if not key_text:
                continue
            env[key_text] = str(value)
    env["BACKEND_RUNNER_REQUEST_PATH"] = str(resolved_request_path)
    expected_outputs = _normalize_expected_outputs(payload.get("expected_outputs"))
    for entry in expected_outputs:
        path = Path(str(entry["path"])).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)

    try:
        proc = subprocess.run(  # noqa: S603
            command_tokens,
            cwd=str(cwd),
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        stderr_path.write_text(str(exc), encoding="utf-8")
        _write_execution_manifest(
            path=execution_manifest_path,
            request_path=resolved_request_path,
            status="PROCESS_ERROR",
            message=f"Backend runner process error: {exc}",
            return_code=-1,
            payload=payload,
            expected_outputs=expected_outputs,
            found_expected_outputs=0,
            missing_expected_outputs=len(expected_outputs),
            sensor_output_summary_path=None,
            output_smoke_report_path=None,
            output_smoke_report=None,
            output_comparison_report_path=None,
            output_comparison_report=None,
            sidecar_materialization_report_path=None,
            sidecar_materialization_report=None,
            stdout_path=None,
            stderr_path=stderr_path,
        )
        return BackendRunnerExecutionResult(
            success=False,
            message=f"Backend runner process error: {exc}",
            return_code=-1,
            artifacts=artifacts,
        )

    stdout_path.write_text(proc.stdout, encoding="utf-8")
    stderr_path.write_text(proc.stderr, encoding="utf-8")
    (
        sidecar_materialization_report_path,
        sidecar_materialization_artifacts,
        sidecar_materialization_report,
    ) = _materialize_sidecar_outputs(
        payload=payload,
        expected_outputs=expected_outputs,
        runner_output_dir=runner_output_dir,
        backend_return_code=proc.returncode,
    )
    artifacts.update(sidecar_materialization_artifacts)
    inspected_outputs, discovered_artifacts, found_count, missing_count = _inspect_expected_outputs(
        expected_outputs,
        sidecar_materialization_report=sidecar_materialization_report,
    )
    artifacts.update(discovered_artifacts)
    sensor_output_summary_path, sensor_output_summary_artifacts = _build_sensor_output_summary(
        expected_outputs=inspected_outputs,
        output_dir=runner_output_dir,
    )
    artifacts.update(sensor_output_summary_artifacts)
    output_smoke_report_path, output_smoke_report_artifacts, output_smoke_report = (
        _build_backend_output_smoke_report(
            expected_outputs=inspected_outputs,
            output_dir=runner_output_dir,
        )
    )
    artifacts.update(output_smoke_report_artifacts)
    output_comparison_report_path, output_comparison_report_artifacts, output_comparison_report = (
        _build_backend_output_comparison_report(
            payload=payload,
            expected_outputs=inspected_outputs,
            output_dir=runner_output_dir,
        )
    )
    artifacts.update(output_comparison_report_artifacts)
    success = proc.returncode == 0
    status = "EXECUTION_SUCCEEDED" if success else "EXECUTION_FAILED"
    message = (
        "Backend runner execution completed."
        if success
        else f"Backend runner command failed with exit code {proc.returncode}."
    )
    _write_execution_manifest(
        path=execution_manifest_path,
        request_path=resolved_request_path,
        status=status,
        message=message,
        return_code=proc.returncode,
        payload=payload,
        expected_outputs=inspected_outputs,
        found_expected_outputs=found_count,
        missing_expected_outputs=missing_count,
        sensor_output_summary_path=sensor_output_summary_path,
        output_smoke_report_path=output_smoke_report_path,
        output_smoke_report=output_smoke_report,
        output_comparison_report_path=output_comparison_report_path,
        output_comparison_report=output_comparison_report,
        sidecar_materialization_report_path=sidecar_materialization_report_path,
        sidecar_materialization_report=sidecar_materialization_report,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    artifacts["backend_runner_execution_manifest"] = execution_manifest_path
    artifacts["backend_runner_stdout"] = stdout_path
    artifacts["backend_runner_stderr"] = stderr_path
    return BackendRunnerExecutionResult(
        success=success,
        message=message,
        return_code=proc.returncode,
        artifacts=artifacts,
    )


def inspect_backend_runner_request_outputs(
    *,
    request_path: Path,
    output_dir: Path | None = None,
) -> BackendRunnerExecutionResult:
    resolved_request_path = request_path.expanduser().resolve()
    inspection_output_dir = (
        output_dir.expanduser().resolve()
        if output_dir is not None
        else resolved_request_path.parent
    )
    inspection_output_dir.mkdir(parents=True, exist_ok=True)
    inspection_manifest_path = inspection_output_dir / "backend_output_inspection_manifest.json"
    artifacts = {
        "backend_output_inspection_manifest": inspection_manifest_path,
    }

    payload, request_error = _load_backend_runner_request_payload(request_path=resolved_request_path)
    if request_error is not None:
        _write_output_inspection_manifest(
            path=inspection_manifest_path,
            request_path=resolved_request_path,
            payload=None,
            status="REQUEST_ERROR",
            success=False,
            message=request_error,
            return_code=1,
            sensor_output_summary_path=None,
            output_smoke_report_path=None,
            output_smoke_report=None,
            output_comparison_report_path=None,
            output_comparison_report=None,
        )
        return BackendRunnerExecutionResult(
            success=False,
            message=request_error,
            return_code=1,
            artifacts=artifacts,
        )

    expected_outputs = _normalize_expected_outputs(payload.get("expected_outputs"))
    inspected_outputs, discovered_artifacts, _found_count, _missing_count = _inspect_expected_outputs(
        expected_outputs,
        sidecar_materialization_report=None,
    )
    artifacts.update(discovered_artifacts)
    sensor_output_summary_path, sensor_output_summary_artifacts = _build_sensor_output_summary(
        expected_outputs=inspected_outputs,
        output_dir=inspection_output_dir,
    )
    artifacts.update(sensor_output_summary_artifacts)
    output_smoke_report_path, output_smoke_report_artifacts, output_smoke_report = (
        _build_backend_output_smoke_report(
            expected_outputs=inspected_outputs,
            output_dir=inspection_output_dir,
        )
    )
    artifacts.update(output_smoke_report_artifacts)
    output_comparison_report_path, output_comparison_report_artifacts, output_comparison_report = (
        _build_backend_output_comparison_report(
            payload=payload,
            expected_outputs=inspected_outputs,
            output_dir=inspection_output_dir,
        )
    )
    artifacts.update(output_comparison_report_artifacts)
    comparison_status = (
        str(output_comparison_report.get("status", "")).strip()
        if isinstance(output_comparison_report, dict)
        else "UNOBSERVED"
    )
    success = comparison_status == "MATCHED"
    return_code = 0 if success else 2
    message = (
        "Backend output inspection matched the contract."
        if success
        else f"Backend output inspection detected {comparison_status.lower()} state."
    )
    _write_output_inspection_manifest(
        path=inspection_manifest_path,
        request_path=resolved_request_path,
        payload=payload,
        status=comparison_status,
        success=success,
        message=message,
        return_code=return_code,
        sensor_output_summary_path=sensor_output_summary_path,
        output_smoke_report_path=output_smoke_report_path,
        output_smoke_report=output_smoke_report,
        output_comparison_report_path=output_comparison_report_path,
        output_comparison_report=output_comparison_report,
    )
    return BackendRunnerExecutionResult(
        success=success,
        message=message,
        return_code=return_code,
        artifacts=artifacts,
    )


def execute_and_inspect_backend_runner_request(
    *,
    request_path: Path,
    output_dir: Path | None = None,
) -> BackendRunnerExecutionResult:
    resolved_request_path = request_path.expanduser().resolve()
    smoke_output_dir = (
        output_dir.expanduser().resolve()
        if output_dir is not None
        else resolved_request_path.parent
    )
    smoke_output_dir.mkdir(parents=True, exist_ok=True)
    smoke_manifest_path = smoke_output_dir / "backend_runner_smoke_manifest.json"
    artifacts: dict[str, Path] = {
        "backend_runner_smoke_manifest": smoke_manifest_path,
    }

    execution_result = execute_backend_runner_request(
        request_path=resolved_request_path,
        output_dir=smoke_output_dir,
    )
    artifacts.update(execution_result.artifacts)
    inspection_result = inspect_backend_runner_request_outputs(
        request_path=resolved_request_path,
        output_dir=smoke_output_dir,
    )
    artifacts.update(inspection_result.artifacts)

    execution_manifest_path = execution_result.artifacts.get("backend_runner_execution_manifest")
    inspection_manifest_path = inspection_result.artifacts.get("backend_output_inspection_manifest")
    _write_backend_runner_smoke_manifest(
        path=smoke_manifest_path,
        request_path=resolved_request_path,
        execution_result=execution_result,
        inspection_result=inspection_result,
        execution_manifest_path=execution_manifest_path,
        inspection_manifest_path=inspection_manifest_path,
    )

    success = execution_result.success and inspection_result.success
    if success:
        message = "Backend runner execution and inspection completed."
    elif not execution_result.success and not inspection_result.success:
        message = (
            f"{execution_result.message} Inspection follow-up: {inspection_result.message}"
        )
    elif not execution_result.success:
        message = execution_result.message
    else:
        message = inspection_result.message

    if not execution_result.success and execution_result.return_code not in (None, 0):
        return_code = execution_result.return_code
    elif not inspection_result.success and inspection_result.return_code not in (None, 0):
        return_code = inspection_result.return_code
    elif not execution_result.success:
        return_code = execution_result.return_code if execution_result.return_code is not None else 1
    elif not inspection_result.success:
        return_code = (
            inspection_result.return_code if inspection_result.return_code is not None else 1
        )
    else:
        return_code = 0

    return BackendRunnerExecutionResult(
        success=success,
        message=message,
        return_code=return_code,
        artifacts=artifacts,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Execute a backend runner request JSON.")
    parser.add_argument("request_path", help="Path to backend_runner_request.json")
    parser.add_argument(
        "--output-dir",
        help="Directory for backend_runner execution artifacts. Defaults to request directory.",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--compare-only",
        action="store_true",
        help="Inspect existing backend outputs without executing the backend command.",
    )
    mode_group.add_argument(
        "--execute-and-inspect",
        action="store_true",
        help="Execute the backend command and then run compare-only inspection in the same output directory.",
    )
    args = parser.parse_args(argv)
    output_dir = Path(args.output_dir).expanduser() if args.output_dir else None
    if args.compare_only:
        result = inspect_backend_runner_request_outputs(
            request_path=Path(args.request_path),
            output_dir=output_dir,
        )
    elif args.execute_and_inspect:
        result = execute_and_inspect_backend_runner_request(
            request_path=Path(args.request_path),
            output_dir=output_dir,
        )
    else:
        result = execute_backend_runner_request(
            request_path=Path(args.request_path),
            output_dir=output_dir,
        )
    stream = sys.stdout if result.success else sys.stderr
    print(result.message, file=stream)
    if result.success:
        return 0
    if result.return_code is not None and result.return_code >= 0:
        return result.return_code
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
