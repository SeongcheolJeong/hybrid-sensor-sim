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


def _write_execution_manifest(
    *,
    path: Path,
    request_path: Path,
    status: str,
    message: str,
    return_code: int | None,
    payload: dict[str, Any] | None,
    stdout_path: Path | None,
    stderr_path: Path | None,
) -> None:
    manifest = {
        "request_path": str(request_path),
        "backend": str(payload.get("backend", "")) if isinstance(payload, dict) else "",
        "status": status,
        "message": message,
        "return_code": return_code,
        "cwd": str(payload.get("cwd", "")) if isinstance(payload, dict) else "",
        "runner_mode": str(payload.get("runner_mode", "")) if isinstance(payload, dict) else "",
        "command": payload.get("command", []) if isinstance(payload, dict) else [],
        "artifacts": {
            "backend_runner_stdout": str(stdout_path) if stdout_path else None,
            "backend_runner_stderr": str(stderr_path) if stderr_path else None,
        },
    }
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


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

    try:
        proc = subprocess.run(  # noqa: S603
            command_tokens,
            cwd=str(cwd),
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
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
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
