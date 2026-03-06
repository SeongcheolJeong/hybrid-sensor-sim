from __future__ import annotations

import json
import os
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
        "error": build_error,
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
    backend_args_preview = _build_backend_args_preview(
        options=options,
        backend=backend,
        contract_payload=contract_payload,
    )
    backend_wrapper_used = command_source == "backend_wrapper"
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
        "backend_args_preview": backend_args_preview,
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
    metrics: dict[str, float] = {
        "renderer_runtime_planned": 1.0,
        "renderer_execute_requested": 1.0 if execute else 0.0,
        "renderer_backend_wrapper_used": 1.0 if backend_wrapper_used else 0.0,
        "renderer_backend_invocation_written": 1.0,
        "renderer_contract_scene_args_count": float(scene_args_count),
        "renderer_contract_sensor_mount_args_count": float(sensor_mount_args_count),
    }

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
