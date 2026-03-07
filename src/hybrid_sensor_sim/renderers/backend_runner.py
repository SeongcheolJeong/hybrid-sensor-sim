from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
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
                "artifact_keys": [],
                "sensor_ids": [],
            },
        )
        summary["expected_count"] += 1
        artifact_key = str(entry.get("artifact_key", "")).strip()
        sensor_id = str(entry.get("sensor_id", "")).strip()
        if artifact_key and artifact_key not in summary["artifact_keys"]:
            summary["artifact_keys"].append(artifact_key)
        if sensor_id and sensor_id not in summary["sensor_ids"]:
            summary["sensor_ids"].append(sensor_id)
        if "exists" in entry:
            if bool(entry.get("exists", False)):
                summary["found_count"] += 1
            else:
                summary["missing_count"] += 1
    return [groups[key] for key in sorted(groups)]


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
        "artifacts": {
            "backend_sensor_output_summary": (
                str(sensor_output_summary_path) if sensor_output_summary_path else None
            ),
            "backend_runner_stdout": str(stdout_path) if stdout_path else None,
            "backend_runner_stderr": str(stderr_path) if stderr_path else None,
        },
    }
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


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


def _inspect_expected_outputs(
    expected_outputs: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, Path], int, int]:
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
                "embedded_output": bool(entry.get("embedded_output", False)),
                "embedded_field": str(entry.get("embedded_field", "")).strip(),
                "shared_output_artifact_key": str(
                    entry.get("shared_output_artifact_key", "")
                ).strip(),
            }
        )

    for sensor_summary in sensors.values():
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

    summary_payload = {
        "sensor_count": len(sensors),
        "found_sensor_count": found_sensor_count,
        "missing_sensor_count": missing_sensor_count,
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

    try:
        payload = json.loads(resolved_request_path.read_text(encoding="utf-8"))
    except OSError as exc:
        stderr_path.write_text(str(exc), encoding="utf-8")
        _write_execution_manifest(
            path=execution_manifest_path,
            request_path=resolved_request_path,
            status="REQUEST_ERROR",
            message=f"Backend runner request read error: {exc}",
            return_code=None,
            payload=None,
            expected_outputs=None,
            found_expected_outputs=0,
            missing_expected_outputs=0,
            sensor_output_summary_path=None,
            stdout_path=None,
            stderr_path=stderr_path,
        )
        return BackendRunnerExecutionResult(
            success=False,
            message=f"Backend runner request read error: {exc}",
            return_code=None,
            artifacts=artifacts,
        )
    except json.JSONDecodeError as exc:
        stderr_path.write_text(str(exc), encoding="utf-8")
        _write_execution_manifest(
            path=execution_manifest_path,
            request_path=resolved_request_path,
            status="REQUEST_ERROR",
            message=f"Backend runner request decode error: {exc}",
            return_code=None,
            payload=None,
            expected_outputs=None,
            found_expected_outputs=0,
            missing_expected_outputs=0,
            sensor_output_summary_path=None,
            stdout_path=None,
            stderr_path=stderr_path,
        )
        return BackendRunnerExecutionResult(
            success=False,
            message=f"Backend runner request decode error: {exc}",
            return_code=None,
            artifacts=artifacts,
        )

    if not isinstance(payload, dict):
        stderr_path.write_text("backend runner request payload must be a JSON object", encoding="utf-8")
        _write_execution_manifest(
            path=execution_manifest_path,
            request_path=resolved_request_path,
            status="REQUEST_ERROR",
            message="Backend runner request payload must be a JSON object.",
            return_code=None,
            payload=None,
            expected_outputs=None,
            found_expected_outputs=0,
            missing_expected_outputs=0,
            sensor_output_summary_path=None,
            stdout_path=None,
            stderr_path=stderr_path,
        )
        return BackendRunnerExecutionResult(
            success=False,
            message="Backend runner request payload must be a JSON object.",
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
    inspected_outputs, discovered_artifacts, found_count, missing_count = _inspect_expected_outputs(
        expected_outputs
    )
    artifacts.update(discovered_artifacts)
    sensor_output_summary_path, sensor_output_summary_artifacts = _build_sensor_output_summary(
        expected_outputs=inspected_outputs,
        output_dir=runner_output_dir,
    )
    artifacts.update(sensor_output_summary_artifacts)
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Execute a backend runner request JSON.")
    parser.add_argument("request_path", help="Path to backend_runner_request.json")
    parser.add_argument(
        "--output-dir",
        help="Directory for backend_runner execution artifacts. Defaults to request directory.",
    )
    args = parser.parse_args(argv)
    output_dir = Path(args.output_dir).expanduser() if args.output_dir else None
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
