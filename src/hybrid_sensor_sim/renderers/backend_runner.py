from __future__ import annotations

import json
import os
import shlex
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
        }

    resolved_backend_bin, backend_bin_source, backend_bin_env_key = _resolve_direct_backend_bin(
        options=options,
        backend=backend,
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
        "artifacts": {
            "backend_frame_inputs_manifest": str(frame_manifest_path) if frame_manifest_path else None,
            "backend_ingestion_profile": str(ingestion_profile_path) if ingestion_profile_path else None,
            "backend_sensor_bundle_summary": str(bundle_summary_path) if bundle_summary_path else None,
            "backend_launcher_template": str(launcher_template_path) if launcher_template_path else None,
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

    return {
        "backend_runner_request": request_path,
        "backend_direct_run_command": shell_path,
    }, {
        "renderer_backend_runner_request_written": 1.0,
        "renderer_backend_runner_command_written": 1.0,
        "renderer_backend_runner_arg_count": float(len(command)),
        "renderer_backend_runner_scene_arg_count": float(len(scene_args)),
        "renderer_backend_runner_mount_arg_count": float(len(mount_args)),
        "renderer_backend_runner_ingestion_arg_count": float(len(ingestion_args)),
    }
