from __future__ import annotations

import json
import os
import shlex
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_BACKEND_PRESETS: dict[str, dict[str, str]] = {
    "awsim": {
        "default_bin": "awsim",
        "wrapper": "scripts/renderer_launch_awsim.sh",
        "scene_map_flag": "--map",
    },
    "carla": {
        "default_bin": "carla",
        "wrapper": "scripts/renderer_launch_carla.sh",
        "scene_map_flag": "--town",
    },
}


@dataclass
class RendererRuntimeResult:
    success: bool
    message: str
    artifacts: dict[str, Path] = field(default_factory=dict)
    metrics: dict[str, float] = field(default_factory=dict)


def _resolve_renderer_cwd(options: dict[str, Any]) -> Path:
    raw = options.get("renderer_cwd")
    if raw is None or str(raw).strip() == "":
        return Path.cwd().resolve()
    path = Path(str(raw)).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (Path.cwd() / path).resolve()


def _read_contract_payload(contract_path: Path) -> dict[str, Any] | None:
    if not contract_path.exists():
        return None
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _coerce_arg_list(raw: Any, name: str) -> tuple[list[str], str | None]:
    if raw is None:
        return [], None
    if not isinstance(raw, list):
        return [], f"{name} must be a list when provided."
    return [str(item) for item in raw], None


def _backend_preset(backend: str) -> dict[str, str] | None:
    return _BACKEND_PRESETS.get(backend)


def _resolve_backend_executable(
    *,
    options: dict[str, Any],
    backend: str,
) -> tuple[str, str, str | None]:
    preset = _backend_preset(backend)
    if preset is None:
        return "", "none", "renderer_bin is required when renderer_command is not set."

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
        return explicit_bin, "backend_default", None

    wrapper_enabled = bool(options.get("renderer_backend_wrapper_enabled", True))
    if wrapper_enabled:
        wrapper_raw = str(
            options.get(
                f"{backend}_wrapper",
                options.get(
                    f"renderer_{backend}_wrapper",
                    options.get(
                        "renderer_backend_wrapper",
                        preset["wrapper"],
                    ),
                ),
            )
        ).strip()
        wrapper_path = Path(wrapper_raw).expanduser()
        if not wrapper_path.is_absolute():
            wrapper_path = (Path.cwd() / wrapper_path).resolve()
        if wrapper_path.exists() and os.access(wrapper_path, os.X_OK):
            return str(wrapper_path), "backend_wrapper", None

    return preset["default_bin"], "backend_default", None


def _resolve_backend_default_command(
    *,
    options: dict[str, Any],
    backend: str,
) -> tuple[list[str], str, str | None]:
    executable, source, resolve_error = _resolve_backend_executable(
        options=options,
        backend=backend,
    )
    if resolve_error is not None:
        return [], "none", resolve_error

    backend_extra_args, backend_error = _coerce_arg_list(
        options.get(
            f"{backend}_extra_args",
            options.get(f"renderer_{backend}_extra_args"),
        ),
        f"{backend}_extra_args",
    )
    if backend_error is not None:
        return [], "none", backend_error

    shared_extra_args, shared_error = _coerce_arg_list(
        options.get("renderer_extra_args", []),
        "renderer_extra_args",
    )
    if shared_error is not None:
        return [], "none", shared_error
    return [executable, *backend_extra_args, *shared_extra_args], source, None


def _resolve_scene_flag(
    *,
    options: dict[str, Any],
    key: str,
    default: str,
) -> str:
    raw = options.get(key)
    if raw is None:
        return default
    text = str(raw).strip()
    return text if text else default


def _collect_contract_scene_args(
    *,
    options: dict[str, Any],
    backend: str,
    contract_payload: dict[str, Any] | None,
) -> list[str]:
    if not bool(options.get("renderer_inject_scene_args", True)):
        return []
    if not isinstance(contract_payload, dict):
        return []
    scene = contract_payload.get("renderer_scene")
    if not isinstance(scene, dict):
        return []

    preset = _backend_preset(backend)
    default_map_flag = preset["scene_map_flag"] if preset is not None else "--map"
    map_flag = _resolve_scene_flag(
        options=options,
        key="renderer_scene_map_flag",
        default=default_map_flag,
    )
    weather_flag = _resolve_scene_flag(
        options=options,
        key="renderer_scene_weather_flag",
        default="--weather",
    )
    seed_flag = _resolve_scene_flag(
        options=options,
        key="renderer_scene_seed_flag",
        default="--seed",
    )
    ego_flag = _resolve_scene_flag(
        options=options,
        key="renderer_scene_ego_actor_flag",
        default="--ego-actor-id",
    )

    args: list[str] = []
    map_value = scene.get("map")
    if map_value is not None and str(map_value).strip():
        if map_flag:
            args.extend([map_flag, str(map_value)])
        else:
            args.append(str(map_value))

    weather_value = scene.get("weather")
    if weather_value is not None and str(weather_value).strip():
        if weather_flag:
            args.extend([weather_flag, str(weather_value)])
        else:
            args.append(str(weather_value))

    seed_value = scene.get("scene_seed")
    if seed_value is not None:
        if seed_flag:
            args.extend([seed_flag, str(seed_value)])
        else:
            args.append(str(seed_value))

    ego_value = scene.get("ego_actor_id")
    if ego_value is not None and str(ego_value).strip():
        if ego_flag:
            args.extend([ego_flag, str(ego_value)])
        else:
            args.append(str(ego_value))
    return args


def _extract_contract_sensor_mounts(
    *,
    options: dict[str, Any],
    contract_payload: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not isinstance(contract_payload, dict):
        return []
    mounts_raw = contract_payload.get("renderer_sensor_mounts")
    if not isinstance(mounts_raw, list):
        return []

    only_enabled = bool(options.get("renderer_sensor_mounts_only_enabled", True))
    mounts: list[dict[str, Any]] = []
    for mount in mounts_raw:
        if not isinstance(mount, dict):
            continue
        if only_enabled and not bool(mount.get("enabled", False)):
            continue
        mounts.append(mount)
    return mounts


def _collect_contract_sensor_mount_args(
    *,
    options: dict[str, Any],
    contract_payload: dict[str, Any] | None,
) -> list[str]:
    if not bool(options.get("renderer_inject_sensor_mount_args", True)):
        return []
    mounts = _extract_contract_sensor_mounts(options=options, contract_payload=contract_payload)
    if not mounts:
        return []

    mount_flag = str(options.get("renderer_sensor_mount_flag", "--sensor-mount")).strip()
    mount_format = str(options.get("renderer_sensor_mount_format", "json")).lower().strip()
    args: list[str] = []
    for mount in mounts:
        if mount_format == "compact":
            payload = "|".join(
                [
                    str(mount.get("sensor_id", "")),
                    str(mount.get("sensor_type", "")),
                    str(mount.get("attach_to_actor_id", "")),
                ]
            )
        else:
            payload = json.dumps(mount, separators=(",", ":"), sort_keys=True)
        if mount_flag:
            args.extend([mount_flag, payload])
        else:
            args.append(payload)
    return args


def _build_backend_args_preview(
    *,
    options: dict[str, Any],
    backend: str,
    contract_payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if backend in {"", "none"}:
        return None
    scene_payload = (
        contract_payload.get("renderer_scene")
        if isinstance(contract_payload, dict) and isinstance(contract_payload.get("renderer_scene"), dict)
        else None
    )
    mounts = _extract_contract_sensor_mounts(
        options=options,
        contract_payload=contract_payload,
    )
    return {
        "backend": backend,
        "scene": scene_payload,
        "scene_cli_args": _collect_contract_scene_args(
            options=options,
            backend=backend,
            contract_payload=contract_payload,
        ),
        "sensor_mounts": mounts,
        "sensor_mount_cli_args": _collect_contract_sensor_mount_args(
            options=options,
            contract_payload=contract_payload,
        ),
        "scene_injection_enabled": bool(options.get("renderer_inject_scene_args", True)),
        "sensor_mount_injection_enabled": bool(
            options.get("renderer_inject_sensor_mount_args", True)
        ),
    }


def _build_backend_invocation_payload(
    *,
    backend: str,
    execute: bool,
    cwd: Path,
    command: list[str],
    command_source: str,
    backend_wrapper_used: bool,
    backend_args_preview: dict[str, Any] | None,
    backend_frame_manifest: str | None,
    backend_ingestion_profile: str | None,
    backend_launcher_template: str | None,
    backend_frame_count: int,
    backend_sensor_bindings: int,
    backend_ingestion_entry_count: int,
    backend_launcher_arg_count: int,
    build_error: str | None,
) -> dict[str, Any]:
    return {
        "backend": backend,
        "execute": execute,
        "cwd": str(cwd),
        "command": command,
        "command_source": command_source,
        "backend_wrapper_used": backend_wrapper_used,
        "backend_args_preview": backend_args_preview,
        "backend_frame_inputs_manifest": backend_frame_manifest,
        "backend_ingestion_profile": backend_ingestion_profile,
        "backend_launcher_template": backend_launcher_template,
        "backend_frame_count": backend_frame_count,
        "backend_sensor_bindings": backend_sensor_bindings,
        "backend_ingestion_entry_count": backend_ingestion_entry_count,
        "backend_launcher_arg_count": backend_launcher_arg_count,
        "error": build_error,
    }


def _coerce_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _resolve_artifact_path(*, raw: Any, cwd: Path) -> Path | None:
    text = str(raw).strip() if raw is not None else ""
    if not text:
        return None
    artifact = Path(text).expanduser()
    if artifact.is_absolute():
        return artifact
    return (cwd / artifact).resolve()


def _resolve_backend_frame_source(
    *,
    source: dict[str, Any],
    sensor_name: str,
    frame_index: int,
    runtime_dir: Path,
    cwd: Path,
) -> tuple[dict[str, Any], int]:
    source_type = str(source.get("source_type", "unknown")).strip()
    requested_frame_index = _coerce_int(source.get("frame_index"), default=0)
    frame_count = max(0, _coerce_int(source.get("frame_count"), default=0))
    resolved: dict[str, Any] = {
        "source_type": source_type,
        "requested_frame_index": requested_frame_index,
        "frame_count": frame_count,
        "available": False,
    }

    artifact_path = _resolve_artifact_path(raw=source.get("artifact"), cwd=cwd)
    if artifact_path is None:
        resolved["error"] = "source artifact is not set."
        return resolved, 0
    resolved["artifact"] = str(artifact_path)
    if not artifact_path.exists():
        resolved["error"] = "source artifact does not exist."
        return resolved, 0

    payload_path = artifact_path
    materialized_count = 0
    if source_type == "sweep" and artifact_path.suffix.lower() == ".json":
        sweep_payload = _read_contract_payload(artifact_path)
        frames = sweep_payload.get("frames") if isinstance(sweep_payload, dict) else None
        if not isinstance(frames, list) or not frames:
            resolved["error"] = "sweep artifact has no frames."
            return resolved, 0
        resolved_index = min(max(requested_frame_index, 0), len(frames) - 1)
        frame_payload = frames[resolved_index]
        if not isinstance(frame_payload, dict):
            resolved["error"] = "sweep frame payload is invalid."
            return resolved, 0
        payload_dir = runtime_dir / "frame_payloads"
        payload_dir.mkdir(parents=True, exist_ok=True)
        payload_path = payload_dir / f"frame_{frame_index:04d}_{sensor_name}.json"
        payload_path.write_text(json.dumps(frame_payload, indent=2), encoding="utf-8")
        resolved["resolved_frame_index"] = resolved_index
        resolved["materialized_payload_artifact"] = str(payload_path)
        materialized_count = 1

    resolved["payload_artifact"] = str(payload_path)
    resolved["available"] = True
    return resolved, materialized_count


def _sensor_mount_index(contract_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    mounts = contract_payload.get("renderer_sensor_mounts")
    if not isinstance(mounts, list):
        return {}
    index: dict[str, dict[str, Any]] = {}
    for mount in mounts:
        if not isinstance(mount, dict):
            continue
        sensor_type = str(mount.get("sensor_type", "")).strip().lower()
        if sensor_type not in {"camera", "lidar", "radar"}:
            continue
        if sensor_type in index:
            continue
        index[sensor_type] = mount
    return index


def _infer_sensor_payload_format(
    *,
    sensor_name: str,
    payload_artifact: str,
) -> str:
    suffix = Path(payload_artifact).suffix.lower()
    if sensor_name == "camera":
        return "camera_projection_json"
    if sensor_name == "lidar":
        if suffix == ".xyz":
            return "lidar_points_xyz"
        if suffix == ".json":
            return "lidar_points_json"
        return "lidar_points"
    if sensor_name == "radar":
        return "radar_targets_json"
    return "unknown"


def _enrich_resolved_sensor_source(
    *,
    sensor_name: str,
    resolved_source: dict[str, Any],
    mount_index: dict[str, dict[str, Any]],
) -> None:
    mount = mount_index.get(sensor_name, {})
    sensor_id = str(mount.get("sensor_id", "")).strip() if isinstance(mount, dict) else ""
    attach_to = (
        str(mount.get("attach_to_actor_id", "")).strip() if isinstance(mount, dict) else ""
    )
    if not sensor_id:
        sensor_id = sensor_name
    if not attach_to:
        attach_to = "ego"
    payload_artifact = str(resolved_source.get("payload_artifact", "")).strip()
    data_format = _infer_sensor_payload_format(
        sensor_name=sensor_name,
        payload_artifact=payload_artifact,
    )
    resolved_source["sensor_name"] = sensor_name
    resolved_source["sensor_type"] = sensor_name
    resolved_source["sensor_id"] = sensor_id
    resolved_source["attach_to_actor_id"] = attach_to
    resolved_source["data_format"] = data_format


def _build_backend_frame_inputs_manifest(
    *,
    options: dict[str, Any],
    contract_payload: dict[str, Any] | None,
    contract_path: Path,
    runtime_dir: Path,
    cwd: Path,
) -> tuple[Path | None, dict[str, float]]:
    if not isinstance(contract_payload, dict):
        return None, {
            "renderer_backend_frame_manifest_written": 0.0,
            "renderer_backend_frame_count": 0.0,
            "renderer_backend_sensor_bindings": 0.0,
            "renderer_backend_materialized_frame_payload_count": 0.0,
        }

    frames_raw = contract_payload.get("frames")
    if not isinstance(frames_raw, list):
        frames_raw = []
    mount_index = _sensor_mount_index(contract_payload)

    frame_start = max(0, _coerce_int(options.get("renderer_backend_frame_start"), default=0))
    frame_stride = max(1, _coerce_int(options.get("renderer_backend_frame_stride"), default=1))
    max_frames = _coerce_int(options.get("renderer_backend_max_frames"), default=0)
    selected_indices = list(range(frame_start, len(frames_raw), frame_stride))
    if max_frames > 0:
        selected_indices = selected_indices[:max_frames]

    manifest_frames: list[dict[str, Any]] = []
    sensor_binding_count = 0
    materialized_payload_count = 0
    for fallback_index in selected_indices:
        frame = frames_raw[fallback_index]
        if not isinstance(frame, dict):
            continue
        frame_id = _coerce_int(frame.get("frame_id"), default=fallback_index)
        frame_manifest: dict[str, Any] = {
            "frame_id": frame_id,
            "renderer_frame_id": _coerce_int(
                frame.get("renderer_frame_id"),
                default=frame_id,
            ),
            "time_s": _coerce_float(frame.get("time_s"), default=0.0),
        }
        for sensor_name in ("camera", "lidar", "radar"):
            source = frame.get(sensor_name)
            if not isinstance(source, dict):
                continue
            resolved_source, materialized_count = _resolve_backend_frame_source(
                source=source,
                sensor_name=sensor_name,
                frame_index=frame_id,
                runtime_dir=runtime_dir,
                cwd=cwd,
            )
            _enrich_resolved_sensor_source(
                sensor_name=sensor_name,
                resolved_source=resolved_source,
                mount_index=mount_index,
            )
            frame_manifest[sensor_name] = resolved_source
            if resolved_source.get("available") is True:
                sensor_binding_count += 1
            materialized_payload_count += materialized_count
        manifest_frames.append(frame_manifest)

    manifest_payload = {
        "contract_path": str(contract_path),
        "selection": {
            "start": frame_start,
            "stride": frame_stride,
            "max_frames": max_frames if max_frames > 0 else None,
            "selected_indices": selected_indices,
            "source_frame_count": len(frames_raw),
        },
        "frame_count": len(manifest_frames),
        "frames": manifest_frames,
    }
    manifest_path = runtime_dir / "backend_frame_inputs_manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
    return manifest_path, {
        "renderer_backend_frame_manifest_written": 1.0,
        "renderer_backend_frame_count": float(len(manifest_frames)),
        "renderer_backend_sensor_bindings": float(sensor_binding_count),
        "renderer_backend_materialized_frame_payload_count": float(materialized_payload_count),
    }


def _build_backend_ingestion_profile(
    *,
    backend: str,
    frame_manifest_path: Path | None,
    runtime_dir: Path,
) -> tuple[Path | None, dict[str, float]]:
    if frame_manifest_path is None or not frame_manifest_path.exists():
        return None, {
            "renderer_backend_ingestion_profile_written": 0.0,
            "renderer_backend_ingestion_entry_count": 0.0,
        }
    manifest = _read_contract_payload(frame_manifest_path)
    if not isinstance(manifest, dict):
        return None, {
            "renderer_backend_ingestion_profile_written": 0.0,
            "renderer_backend_ingestion_entry_count": 0.0,
        }

    frames = manifest.get("frames")
    if not isinstance(frames, list):
        frames = []

    frame_flag = "--ingest-sensor-frame" if backend == "awsim" else "--ingest-frame"
    meta_flag = "--ingest-sensor-meta" if backend == "awsim" else "--ingest-meta"
    entries: list[dict[str, Any]] = []
    for frame in frames:
        if not isinstance(frame, dict):
            continue
        renderer_frame_id = _coerce_int(frame.get("renderer_frame_id"), default=0)
        for sensor_name in ("camera", "lidar", "radar"):
            source = frame.get(sensor_name)
            if not isinstance(source, dict) or not bool(source.get("available", False)):
                continue
            payload_artifact = str(source.get("payload_artifact", "")).strip()
            if not payload_artifact:
                continue
            sensor = str(source.get("sensor_name", sensor_name)).strip() or sensor_name
            sensor_id = str(source.get("sensor_id", "")).strip() or sensor
            data_format = str(source.get("data_format", "")).strip()
            attach_to = str(source.get("attach_to_actor_id", "")).strip() or "ego"
            if backend == "awsim":
                frame_value = f"{sensor}:{renderer_frame_id}:{payload_artifact}"
            else:
                frame_value = f"{renderer_frame_id}:{sensor}:{payload_artifact}"
            meta_value = f"{sensor}:{sensor_id}:{data_format}:{attach_to}"
            entries.append(
                {
                    "renderer_frame_id": renderer_frame_id,
                    "sensor_name": sensor,
                    "sensor_id": sensor_id,
                    "data_format": data_format,
                    "payload_artifact": payload_artifact,
                    "frame_flag": frame_flag,
                    "frame_value": frame_value,
                    "meta_flag": meta_flag,
                    "meta_value": meta_value,
                }
            )

    profile_payload = {
        "backend": backend,
        "frame_manifest": str(frame_manifest_path),
        "frame_flag": frame_flag,
        "meta_flag": meta_flag,
        "entry_count": len(entries),
        "entries": entries,
    }
    profile_path = runtime_dir / "backend_ingestion_profile.json"
    profile_path.write_text(json.dumps(profile_payload, indent=2), encoding="utf-8")
    return profile_path, {
        "renderer_backend_ingestion_profile_written": 1.0,
        "renderer_backend_ingestion_entry_count": float(len(entries)),
    }


def _build_backend_launcher_templates(
    *,
    backend: str,
    ingestion_profile_path: Path | None,
    runtime_dir: Path,
) -> tuple[dict[str, Path], dict[str, float]]:
    if ingestion_profile_path is None or not ingestion_profile_path.exists():
        return {}, {
            "renderer_backend_launcher_template_written": 0.0,
            "renderer_backend_ingestion_shell_written": 0.0,
            "renderer_backend_launcher_arg_count": 0.0,
            "renderer_backend_launcher_frame_arg_count": 0.0,
            "renderer_backend_launcher_meta_arg_count": 0.0,
        }
    payload = _read_contract_payload(ingestion_profile_path)
    if not isinstance(payload, dict):
        return {}, {
            "renderer_backend_launcher_template_written": 0.0,
            "renderer_backend_ingestion_shell_written": 0.0,
            "renderer_backend_launcher_arg_count": 0.0,
            "renderer_backend_launcher_frame_arg_count": 0.0,
            "renderer_backend_launcher_meta_arg_count": 0.0,
        }

    entries = payload.get("entries")
    if not isinstance(entries, list):
        entries = []

    frame_args: list[str] = []
    meta_args: list[str] = []
    seen_meta: set[tuple[str, str]] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        frame_flag = str(entry.get("frame_flag", "")).strip()
        frame_value = str(entry.get("frame_value", "")).strip()
        if frame_flag and frame_value:
            frame_args.extend([frame_flag, frame_value])
        meta_flag = str(entry.get("meta_flag", "")).strip()
        meta_value = str(entry.get("meta_value", "")).strip()
        if not meta_flag or not meta_value:
            continue
        meta_key = (meta_flag, meta_value)
        if meta_key in seen_meta:
            continue
        seen_meta.add(meta_key)
        meta_args.extend([meta_flag, meta_value])

    all_args = [*meta_args, *frame_args]
    template_payload = {
        "backend": backend,
        "source_ingestion_profile": str(ingestion_profile_path),
        "arg_count": len(all_args),
        "meta_arg_count": len(meta_args),
        "frame_arg_count": len(frame_args),
        "args": all_args,
        "meta_args": meta_args,
        "frame_args": frame_args,
    }
    template_path = runtime_dir / "backend_launcher_template.json"
    template_path.write_text(json.dumps(template_payload, indent=2), encoding="utf-8")

    shell_path = runtime_dir / "backend_ingestion_args.sh"
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "",
        "# Generated by renderer runtime executor.",
        "BACKEND_INGEST_ARGS=(",
    ]
    for token in all_args:
        lines.append(f"  {shlex.quote(token)}")
    lines.extend(
        [
            ")",
            "",
            'printf "%s\\n" "${BACKEND_INGEST_ARGS[@]}"',
            "",
        ]
    )
    shell_path.write_text("\n".join(lines), encoding="utf-8")
    shell_path.chmod(0o755)

    return {
        "backend_launcher_template": template_path,
        "backend_ingestion_args_sh": shell_path,
    }, {
        "renderer_backend_launcher_template_written": 1.0,
        "renderer_backend_ingestion_shell_written": 1.0,
        "renderer_backend_launcher_arg_count": float(len(all_args)),
        "renderer_backend_launcher_frame_arg_count": float(len(frame_args)),
        "renderer_backend_launcher_meta_arg_count": float(len(meta_args)),
    }


def _inject_contract_scene_args(
    *,
    command: list[str],
    options: dict[str, Any],
    backend: str,
    contract_payload: dict[str, Any] | None,
) -> int:
    args = _collect_contract_scene_args(
        options=options,
        backend=backend,
        contract_payload=contract_payload,
    )
    command.extend(args)
    return len(args)


def _inject_contract_sensor_mount_args(
    *,
    command: list[str],
    options: dict[str, Any],
    contract_payload: dict[str, Any] | None,
) -> int:
    args = _collect_contract_sensor_mount_args(
        options=options,
        contract_payload=contract_payload,
    )
    command.extend(args)
    return len(args)


def _inject_frame_manifest_arg(
    *,
    command: list[str],
    options: dict[str, Any],
    frame_manifest_path: Path | None,
) -> int:
    if frame_manifest_path is None:
        return 0
    if not bool(options.get("renderer_inject_frame_manifest_arg", True)):
        return 0
    manifest_flag = str(options.get("renderer_frame_manifest_flag", "--frame-manifest")).strip()
    if bool(options.get("renderer_frame_manifest_positional", False)):
        command.append(str(frame_manifest_path))
        return 1
    if manifest_flag:
        command.extend([manifest_flag, str(frame_manifest_path)])
        return 2
    command.append(str(frame_manifest_path))
    return 1


def _inject_ingestion_profile_arg(
    *,
    command: list[str],
    options: dict[str, Any],
    ingestion_profile_path: Path | None,
    backend_wrapper_used: bool,
) -> int:
    if ingestion_profile_path is None:
        return 0
    inject_option = options.get("renderer_inject_ingestion_profile_arg")
    if inject_option is None:
        inject_enabled = backend_wrapper_used
    else:
        inject_enabled = bool(inject_option)
    if not inject_enabled:
        return 0
    profile_flag = str(options.get("renderer_ingestion_profile_flag", "--ingestion-profile")).strip()
    if bool(options.get("renderer_ingestion_profile_positional", False)):
        command.append(str(ingestion_profile_path))
        return 1
    if profile_flag:
        command.extend([profile_flag, str(ingestion_profile_path)])
        return 2
    command.append(str(ingestion_profile_path))
    return 1


def _build_renderer_command(
    options: dict[str, Any],
    backend: str,
    contract_path: Path,
    contract_payload: dict[str, Any] | None,
) -> tuple[list[str], bool, str, int, int, str | None]:
    command_override = options.get("renderer_command", [])
    used_override = isinstance(command_override, list) and len(command_override) > 0
    command_source = "renderer_command"
    if used_override:
        command = [str(item) for item in command_override]
    else:
        renderer_bin = str(options.get("renderer_bin", "")).strip()
        if not renderer_bin:
            command, backend_source, default_error = _resolve_backend_default_command(
                options=options,
                backend=backend,
            )
            if default_error is not None:
                return [], False, "none", 0, 0, default_error
            command_source = backend_source
        else:
            extra_args, shared_error = _coerce_arg_list(
                options.get("renderer_extra_args", []),
                "renderer_extra_args",
            )
            if shared_error is not None:
                return [], False, "none", 0, 0, shared_error
            command = [renderer_bin, *extra_args]
            command_source = "renderer_bin"

    contract_token_used = False
    expanded: list[str] = []
    for token in command:
        if "{contract}" in token:
            expanded.append(token.replace("{contract}", str(contract_path)))
            contract_token_used = True
        else:
            expanded.append(token)
    command = expanded

    inject_contract = bool(options.get("renderer_inject_contract_arg", True))
    if inject_contract and not contract_token_used:
        if bool(options.get("renderer_contract_positional", False)):
            command.append(str(contract_path))
        else:
            contract_flag = str(options.get("renderer_contract_flag", "--contract")).strip()
            if contract_flag:
                command.extend([contract_flag, str(contract_path)])
            else:
                command.append(str(contract_path))
    scene_args_count = _inject_contract_scene_args(
        command=command,
        options=options,
        backend=backend,
        contract_payload=contract_payload,
    )
    sensor_mount_args_count = _inject_contract_sensor_mount_args(
        command=command,
        options=options,
        contract_payload=contract_payload,
    )
    return command, used_override, command_source, scene_args_count, sensor_mount_args_count, None


def execute_renderer_runtime(
    *,
    options: dict[str, Any],
    contract_path: Path,
    output_dir: Path,
) -> RendererRuntimeResult:
    runtime_dir = output_dir / "renderer_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    plan_path = runtime_dir / "renderer_execution_plan.json"
    backend = str(options.get("renderer_backend", "none")).lower().strip()
    execute = bool(options.get("renderer_execute", False))
    cwd = _resolve_renderer_cwd(options)
    contract_payload = _read_contract_payload(contract_path)

    command, used_override, command_source, scene_args_count, sensor_mount_args_count, build_error = (
        _build_renderer_command(
            options=options,
            backend=backend,
            contract_path=contract_path,
            contract_payload=contract_payload,
        )
    )
    frame_manifest_path, frame_manifest_metrics = _build_backend_frame_inputs_manifest(
        options=options,
        contract_payload=contract_payload,
        contract_path=contract_path,
        runtime_dir=runtime_dir,
        cwd=cwd,
    )
    ingestion_profile_path, ingestion_profile_metrics = _build_backend_ingestion_profile(
        backend=backend,
        frame_manifest_path=frame_manifest_path,
        runtime_dir=runtime_dir,
    )
    launcher_template_artifacts, launcher_template_metrics = _build_backend_launcher_templates(
        backend=backend,
        ingestion_profile_path=ingestion_profile_path,
        runtime_dir=runtime_dir,
    )
    backend_wrapper_used = command_source == "backend_wrapper"
    frame_manifest_args_count = _inject_frame_manifest_arg(
        command=command,
        options=options,
        frame_manifest_path=frame_manifest_path,
    )
    ingestion_profile_args_count = _inject_ingestion_profile_arg(
        command=command,
        options=options,
        ingestion_profile_path=ingestion_profile_path,
        backend_wrapper_used=backend_wrapper_used,
    )
    backend_args_preview = _build_backend_args_preview(
        options=options,
        backend=backend,
        contract_payload=contract_payload,
    )
    wrapper_dump_path = runtime_dir / "backend_wrapper_invocation.json"
    plan_payload = {
        "backend": backend,
        "execute": execute,
        "contract_path": str(contract_path),
        "cwd": str(cwd),
        "command": command,
        "used_command_override": used_override,
        "command_source": command_source,
        "backend_wrapper_used": backend_wrapper_used,
        "contract_scene_args_count": scene_args_count,
        "contract_sensor_mount_args_count": sensor_mount_args_count,
        "contract_frame_manifest_args_count": frame_manifest_args_count,
        "contract_ingestion_profile_args_count": ingestion_profile_args_count,
        "backend_args_preview": backend_args_preview,
        "backend_frame_inputs_manifest": str(frame_manifest_path) if frame_manifest_path else None,
        "backend_ingestion_profile": str(ingestion_profile_path) if ingestion_profile_path else None,
        "backend_launcher_template": str(launcher_template_artifacts["backend_launcher_template"])
        if "backend_launcher_template" in launcher_template_artifacts
        else None,
        "backend_frame_count": int(frame_manifest_metrics.get("renderer_backend_frame_count", 0.0)),
        "backend_sensor_bindings": int(
            frame_manifest_metrics.get("renderer_backend_sensor_bindings", 0.0)
        ),
        "backend_ingestion_entry_count": int(
            ingestion_profile_metrics.get("renderer_backend_ingestion_entry_count", 0.0)
        ),
        "backend_launcher_arg_count": int(
            launcher_template_metrics.get("renderer_backend_launcher_arg_count", 0.0)
        ),
        "backend_wrapper_dump_path": str(wrapper_dump_path) if backend_wrapper_used else None,
        "error": build_error,
    }
    plan_path.write_text(json.dumps(plan_payload, indent=2), encoding="utf-8")
    backend_invocation_path = runtime_dir / "backend_invocation.json"
    backend_invocation_payload = _build_backend_invocation_payload(
        backend=backend,
        execute=execute,
        cwd=cwd,
        command=command,
        command_source=command_source,
        backend_wrapper_used=backend_wrapper_used,
        backend_args_preview=backend_args_preview,
        backend_frame_manifest=str(frame_manifest_path) if frame_manifest_path else None,
        backend_ingestion_profile=str(ingestion_profile_path) if ingestion_profile_path else None,
        backend_launcher_template=(
            str(launcher_template_artifacts["backend_launcher_template"])
            if "backend_launcher_template" in launcher_template_artifacts
            else None
        ),
        backend_frame_count=int(frame_manifest_metrics.get("renderer_backend_frame_count", 0.0)),
        backend_sensor_bindings=int(
            frame_manifest_metrics.get("renderer_backend_sensor_bindings", 0.0)
        ),
        backend_ingestion_entry_count=int(
            ingestion_profile_metrics.get("renderer_backend_ingestion_entry_count", 0.0)
        ),
        backend_launcher_arg_count=int(
            launcher_template_metrics.get("renderer_backend_launcher_arg_count", 0.0)
        ),
        build_error=build_error,
    )
    backend_invocation_path.write_text(
        json.dumps(backend_invocation_payload, indent=2),
        encoding="utf-8",
    )
    artifacts: dict[str, Path] = {
        "renderer_execution_plan": plan_path,
        "backend_invocation": backend_invocation_path,
    }
    if frame_manifest_path is not None:
        artifacts["backend_frame_inputs_manifest"] = frame_manifest_path
    if ingestion_profile_path is not None:
        artifacts["backend_ingestion_profile"] = ingestion_profile_path
    artifacts.update(launcher_template_artifacts)
    metrics: dict[str, float] = {
        "renderer_runtime_planned": 1.0,
        "renderer_execute_requested": 1.0 if execute else 0.0,
        "renderer_backend_wrapper_used": 1.0 if backend_wrapper_used else 0.0,
        "renderer_backend_invocation_written": 1.0,
        "renderer_contract_scene_args_count": float(scene_args_count),
        "renderer_contract_sensor_mount_args_count": float(sensor_mount_args_count),
        "renderer_contract_frame_manifest_args_count": float(frame_manifest_args_count),
        "renderer_contract_ingestion_profile_args_count": float(ingestion_profile_args_count),
    }
    metrics.update(frame_manifest_metrics)
    metrics.update(ingestion_profile_metrics)
    metrics.update(launcher_template_metrics)

    if backend in {"", "none"}:
        return RendererRuntimeResult(
            success=True,
            message="Renderer runtime skipped: renderer_backend is none.",
            artifacts=artifacts,
            metrics=metrics,
        )
    if build_error is not None:
        return RendererRuntimeResult(
            success=False,
            message=f"Renderer runtime plan error: {build_error}",
            artifacts=artifacts,
            metrics=metrics,
        )
    if not execute:
        return RendererRuntimeResult(
            success=True,
            message="Renderer runtime plan generated only (renderer_execute=false).",
            artifacts=artifacts,
            metrics=metrics,
        )

    stdout_path = runtime_dir / "renderer_stdout.log"
    stderr_path = runtime_dir / "renderer_stderr.log"
    run_env = None
    if backend_wrapper_used:
        run_env = dict(os.environ)
        run_env["RENDERER_WRAPPER_DUMP"] = str(wrapper_dump_path)
    try:
        proc = subprocess.run(  # noqa: S603
            command,
            cwd=str(cwd),
            env=run_env,
            text=True,
            capture_output=True,
            check=False,
        )
        stdout_path.write_text(proc.stdout, encoding="utf-8")
        stderr_path.write_text(proc.stderr, encoding="utf-8")
        artifacts["renderer_stdout"] = stdout_path
        artifacts["renderer_stderr"] = stderr_path
        if backend_wrapper_used and wrapper_dump_path.exists():
            artifacts["backend_wrapper_invocation"] = wrapper_dump_path
        metrics["renderer_return_code"] = float(proc.returncode)
        if proc.returncode != 0:
            return RendererRuntimeResult(
                success=False,
                message=f"Renderer runtime command failed with exit code {proc.returncode}.",
                artifacts=artifacts,
                metrics=metrics,
            )
    except OSError as exc:
        stderr_path.write_text(str(exc), encoding="utf-8")
        artifacts["renderer_stderr"] = stderr_path
        if backend_wrapper_used and wrapper_dump_path.exists():
            artifacts["backend_wrapper_invocation"] = wrapper_dump_path
        metrics["renderer_return_code"] = -1.0
        return RendererRuntimeResult(
            success=False,
            message=f"Renderer runtime process error: {exc}",
            artifacts=artifacts,
            metrics=metrics,
        )

    return RendererRuntimeResult(
        success=True,
        message="Renderer runtime execution completed.",
        artifacts=artifacts,
        metrics=metrics,
    )
