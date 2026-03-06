from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


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


def _build_renderer_command(
    options: dict[str, Any],
    contract_path: Path,
) -> tuple[list[str], bool, str | None]:
    command_override = options.get("renderer_command", [])
    used_override = isinstance(command_override, list) and len(command_override) > 0
    if used_override:
        command = [str(item) for item in command_override]
    else:
        renderer_bin = str(options.get("renderer_bin", "")).strip()
        if not renderer_bin:
            return [], False, "renderer_bin is required when renderer_command is not set."
        extra_args = options.get("renderer_extra_args", [])
        if not isinstance(extra_args, list):
            return [], False, "renderer_extra_args must be a list when provided."
        command = [renderer_bin, *[str(item) for item in extra_args]]

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
    return command, used_override, None


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

    command, used_override, build_error = _build_renderer_command(
        options=options,
        contract_path=contract_path,
    )
    plan_payload = {
        "backend": backend,
        "execute": execute,
        "contract_path": str(contract_path),
        "cwd": str(cwd),
        "command": command,
        "used_command_override": used_override,
        "error": build_error,
    }
    plan_path.write_text(json.dumps(plan_payload, indent=2), encoding="utf-8")
    artifacts: dict[str, Path] = {"renderer_execution_plan": plan_path}
    metrics: dict[str, float] = {
        "renderer_runtime_planned": 1.0,
        "renderer_execute_requested": 1.0 if execute else 0.0,
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
    try:
        proc = subprocess.run(  # noqa: S603
            command,
            cwd=str(cwd),
            text=True,
            capture_output=True,
            check=False,
        )
        stdout_path.write_text(proc.stdout, encoding="utf-8")
        stderr_path.write_text(proc.stderr, encoding="utf-8")
        artifacts["renderer_stdout"] = stdout_path
        artifacts["renderer_stderr"] = stderr_path
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
