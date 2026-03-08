from __future__ import annotations

import argparse
import copy
import contextlib
import hashlib
import io
import json
import os
import shlex
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.tools.renderer_backend_local_setup import (
    _render_env_file as _render_local_setup_env_file,
)
from hybrid_sensor_sim.tools.renderer_backend_local_setup import (
    _inspect_executable_host_compatibility,
)
from hybrid_sensor_sim.tools.renderer_backend_local_setup import (
    build_renderer_backend_local_setup,
)
from hybrid_sensor_sim.tools.renderer_backend_package_acquire import (
    build_renderer_backend_package_acquire,
)
from hybrid_sensor_sim.tools.renderer_backend_linux_handoff_docker import (
    run_renderer_backend_linux_handoff_in_docker,
)
from hybrid_sensor_sim.tools.renderer_backend_smoke import (
    _build_effective_config as _build_smoke_effective_config,
)
from hybrid_sensor_sim.tools.renderer_backend_smoke import main as smoke_main


_DEFAULT_SMOKE_PRESETS = {
    "awsim": {
        "binary": "configs/renderer_backend_smoke.awsim.local.example.json",
        "docker": "configs/renderer_backend_smoke.awsim.local.docker.example.json",
    },
    "carla": {
        "binary": "configs/renderer_backend_smoke.carla.local.example.json",
        "docker": "configs/renderer_backend_smoke.carla.local.docker.example.json",
    },
}
_BACKEND_ENV_VARS = {
    "awsim": ("AWSIM_BIN", "AWSIM_RENDERER_MAP"),
    "carla": ("CARLA_BIN", "CARLA_RENDERER_MAP"),
}
_DEFAULT_LINUX_HANDOFF_DOCKER_IMAGE = "python:3.11-slim"
_DEFAULT_LINUX_HANDOFF_CONTAINER_WORKSPACE = "/workspace"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run local backend discovery/acquire/smoke as a single workflow."
    )
    parser.add_argument(
        "--backend",
        choices=("awsim", "carla"),
        required=True,
        help="Backend workflow to execute.",
    )
    parser.add_argument(
        "--setup-summary",
        type=Path,
        help="Existing renderer_backend_local_setup.json. If omitted, discovery is run automatically.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="Optional smoke base config. If omitted, the backend local preset is selected automatically.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        help="Directory for workflow artifacts. Defaults to artifacts/renderer_backend_workflow/<backend>.",
    )
    parser.add_argument(
        "--backend-bin",
        help="Explicit backend runtime path override.",
    )
    parser.add_argument(
        "--renderer-map",
        help="Explicit renderer map/town override.",
    )
    parser.add_argument(
        "--auto-acquire",
        action="store_true",
        help="If the backend runtime is missing, try package acquire+stage automatically before smoke.",
    )
    parser.add_argument(
        "--download-url",
        help="Explicit package URL passed through to acquire when --auto-acquire is enabled.",
    )
    parser.add_argument(
        "--download-name",
        help="Explicit local archive filename passed through to acquire.",
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        help="Download target directory for acquire. Defaults to ~/Downloads.",
    )
    parser.add_argument(
        "--overwrite-download",
        action="store_true",
        help="Force re-download during acquire.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve setup/acquire/smoke decisions without executing smoke.",
    )
    parser.add_argument(
        "--set-option",
        action="append",
        default=[],
        help="Forwarded to renderer_backend_smoke.py when smoke runs.",
    )
    parser.add_argument(
        "--pack-linux-handoff",
        action="store_true",
        help="When Linux handoff artifacts are ready, also build a tar.gz transfer bundle locally.",
    )
    parser.add_argument(
        "--verify-linux-handoff-bundle",
        action="store_true",
        help="After generating or locating a Linux handoff bundle, unpack and verify it locally.",
    )
    parser.add_argument(
        "--run-linux-handoff-docker",
        action="store_true",
        help="After Linux handoff artifacts are ready, run the handoff helper inside a local Linux Docker container.",
    )
    parser.add_argument(
        "--docker-handoff-execute",
        action="store_true",
        help="When running the Docker handoff helper, execute the extracted handoff script instead of verify-only mode.",
    )
    parser.add_argument(
        "--docker-binary",
        default="docker",
        help="Docker CLI binary used for --run-linux-handoff-docker.",
    )
    parser.add_argument(
        "--docker-image",
        default=_DEFAULT_LINUX_HANDOFF_DOCKER_IMAGE,
        help="Linux Docker image used for --run-linux-handoff-docker.",
    )
    parser.add_argument(
        "--docker-container-workspace",
        default=_DEFAULT_LINUX_HANDOFF_CONTAINER_WORKSPACE,
        help="Workspace mount path inside the Docker container for --run-linux-handoff-docker.",
    )
    return parser.parse_args(argv)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_executable_text(path: Path, payload: str) -> None:
    _write_text(path, payload)
    path.chmod(path.stat().st_mode | 0o111)


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"JSON payload at {path} must be an object.")
    return payload


def _resolve_path(raw: str | Path) -> Path:
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (Path.cwd() / path).resolve()


def _selection_value(selection: dict[str, Any], key: str) -> str | None:
    value = selection.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _render_workflow_env_file(summary: dict[str, Any]) -> str:
    selection = summary.get("final_selection", {})
    lines = [
        "#!/usr/bin/env bash",
        "# Generated by renderer_backend_workflow.py",
        "# Source this file to reuse the latest workflow selections.",
        "",
    ]
    for env_key in (
        "HELIOS_BIN",
        "HELIOS_DOCKER_IMAGE",
        "HELIOS_DOCKER_BINARY",
        "AWSIM_BIN",
        "AWSIM_RENDERER_MAP",
        "CARLA_BIN",
        "CARLA_RENDERER_MAP",
    ):
        value = selection.get(env_key) if isinstance(selection, dict) else None
        if value:
            lines.append(f"export {env_key}={shlex.quote(str(value))}")
        else:
            lines.append(f"# export {env_key}=<set-me>")
    lines.extend(
        [
            "",
            f"# backend={summary.get('backend')}",
            f"# status={summary.get('status')}",
            f"# success={summary.get('success')}",
        ]
    )
    recommended = summary.get("recommended_next_command")
    if recommended:
        lines.append(f"# next: {recommended}")
    commands = summary.get("commands", {})
    if isinstance(commands, dict):
        smoke_command = commands.get("smoke")
        acquire_command = commands.get("acquire")
        if smoke_command:
            lines.append(f"# smoke: {smoke_command}")
        if acquire_command:
            lines.append(f"# acquire: {acquire_command}")
    lines.append("")
    return "\n".join(lines)


def _render_workflow_markdown_report(summary: dict[str, Any], summary_path: Path) -> str:
    def _inline(value: Any) -> str:
        if value is None:
            return "-"
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value).replace("\n", " ").strip() or "-"

    lines = [
        "# Renderer Backend Workflow Report",
        "",
        "## Overview",
        f"- backend: `{_inline(summary.get('backend'))}`",
        f"- status: `{_inline(summary.get('status'))}`",
        f"- success: `{_inline(summary.get('success'))}`",
        f"- dry run: `{_inline(summary.get('dry_run'))}`",
        f"- summary json: `{summary_path}`",
        "",
    ]
    recommended = summary.get("recommended_next_command")
    if recommended:
        lines.extend(
            [
                "## Next Command",
                f"- `{recommended}`",
                "",
            ]
        )
    issues = summary.get("issues", [])
    if isinstance(issues, list) and issues:
        lines.append("## Issues")
        for issue in issues:
            lines.append(f"- `{_inline(issue)}`")
        lines.append("")
    blockers = summary.get("blockers", [])
    if isinstance(blockers, list) and blockers:
        lines.extend(
            [
                "## Blockers",
                "| Code | Severity | Message | Recommended Command |",
                "| --- | --- | --- | --- |",
            ]
        )
        for blocker in blockers:
            if not isinstance(blocker, dict):
                continue
            lines.append(
                "| {code} | {severity} | `{message}` | `{recommended}` |".format(
                    code=_inline(blocker.get("code")),
                    severity=_inline(blocker.get("severity")),
                    message=_inline(blocker.get("message")),
                    recommended=_inline(blocker.get("recommended_command")),
                )
            )
        lines.append("")
    final_selection = summary.get("final_selection", {})
    if isinstance(final_selection, dict) and final_selection:
        lines.extend(
            [
                "## Final Selection",
                "| Key | Value |",
                "| --- | --- |",
            ]
        )
        for key in sorted(final_selection):
            lines.append(f"| {key} | `{_inline(final_selection[key])}` |")
        lines.append("")
    smoke = summary.get("smoke", {})
    if isinstance(smoke, dict):
        lines.extend(
            [
                "## Smoke",
                f"- ready: `{_inline(smoke.get('ready'))}`",
                f"- executed: `{_inline(smoke.get('executed'))}`",
                f"- exit_code: `{_inline(smoke.get('exit_code'))}`",
                f"- config: `{_inline(smoke.get('config_path'))}`",
                "",
            ]
        )
    docker_handoff = summary.get("docker_handoff", {})
    if isinstance(docker_handoff, dict) and docker_handoff:
        lines.extend(
            [
                "## Docker Handoff",
                f"- requested: `{_inline(docker_handoff.get('requested'))}`",
                f"- ready: `{_inline(docker_handoff.get('ready'))}`",
                f"- executed: `{_inline(docker_handoff.get('executed'))}`",
                f"- skip_run: `{_inline(docker_handoff.get('skip_run'))}`",
                f"- return_code: `{_inline(docker_handoff.get('return_code'))}`",
                f"- docker_image: `{_inline(docker_handoff.get('docker_image'))}`",
                f"- summary_path: `{_inline(docker_handoff.get('summary_path'))}`",
                "",
            ]
        )
        preflight = docker_handoff.get("preflight", {})
        if isinstance(preflight, dict) and preflight:
            lines.extend(
                [
                    "### Docker Preflight",
                    f"- available: `{_inline(preflight.get('available'))}`",
                    f"- success: `{_inline(preflight.get('success'))}`",
                    f"- execute: `{_inline(preflight.get('execute'))}`",
                    f"- marker_exists: `{_inline(preflight.get('marker_exists'))}`",
                    f"- docker_return_code: `{_inline(preflight.get('docker_return_code'))}`",
                    f"- command: `{_inline(preflight.get('command'))}`",
                    f"- summary_path: `{_inline(preflight.get('summary_path'))}`",
                    "",
                ]
            )
        if docker_handoff.get("error"):
            lines.append(f"- error: `{_inline(docker_handoff.get('error'))}`")
            lines.append("")
    linux_handoff = summary.get("linux_handoff", {})
    if isinstance(linux_handoff, dict) and linux_handoff:
        lines.extend(
            [
                "## Linux Runner Handoff",
                f"- ready: `{_inline(linux_handoff.get('ready'))}`",
                f"- target platform: `{_inline(linux_handoff.get('target_platform'))}`",
                f"- command: `{_inline(linux_handoff.get('runner_command'))}`",
                f"- script command: `{_inline(linux_handoff.get('script_command'))}`",
                f"- helper command: `{_inline(linux_handoff.get('helper_command'))}`",
                f"- docker command: `{_inline(linux_handoff.get('docker_script_command'))}`",
                f"- docker helper command: `{_inline(linux_handoff.get('docker_helper_command'))}`",
                f"- pack command: `{_inline(linux_handoff.get('pack_command'))}`",
                f"- unpack command: `{_inline(linux_handoff.get('unpack_command'))}`",
                "",
            ]
        )
        required_env_vars = linux_handoff.get("required_env_vars", [])
        if isinstance(required_env_vars, list) and required_env_vars:
            lines.append("### Required Env")
            for item in required_env_vars:
                lines.append(f"- `{_inline(item)}`")
            lines.append("")
        transfer_candidates = linux_handoff.get("transfer_candidates", [])
        if isinstance(transfer_candidates, list) and transfer_candidates:
            lines.extend(
                [
                    "### Transfer Candidates",
                    "| Type | Env Var | Local Path |",
                    "| --- | --- | --- |",
                ]
            )
            for item in transfer_candidates:
                if not isinstance(item, dict):
                    continue
                lines.append(
                    "| {kind} | {env_var} | `{path}` |".format(
                        kind=_inline(item.get("kind")),
                        env_var=_inline(item.get("env_var")),
                        path=_inline(item.get("local_path")),
                    )
                )
            lines.append("")
        transfer_manifest = linux_handoff.get("transfer_manifest", {})
        if isinstance(transfer_manifest, dict) and transfer_manifest:
            lines.extend(
                [
                    "### Transfer Manifest",
                    f"- entry_count: `{_inline(transfer_manifest.get('entry_count'))}`",
                    f"- packable_entry_count: `{_inline(transfer_manifest.get('packable_entry_count'))}`",
                    f"- missing_entry_count: `{_inline(transfer_manifest.get('missing_entry_count'))}`",
                    f"- verifiable_entry_count: `{_inline(transfer_manifest.get('verifiable_entry_count'))}`",
                    f"- bundle_path: `{_inline(transfer_manifest.get('bundle_path'))}`",
                    f"- bundle_manifest_path: `{_inline(transfer_manifest.get('bundle_manifest_path'))}`",
                    "",
                ]
            )
        bundle = linux_handoff.get("bundle", {})
        if isinstance(bundle, dict) and bundle:
            lines.extend(
                [
                    "### Bundle",
                    f"- pack_requested: `{_inline(bundle.get('pack_requested'))}`",
                    f"- bundle_generated: `{_inline(bundle.get('bundle_generated'))}`",
                    f"- verify_requested: `{_inline(bundle.get('verify_requested'))}`",
                    f"- bundle_verified: `{_inline(bundle.get('bundle_verified'))}`",
                    f"- bundle_path: `{_inline(bundle.get('bundle_path'))}`",
                    f"- verification_manifest_path: `{_inline(bundle.get('verification_manifest_path'))}`",
                    "",
                ]
            )
    commands = summary.get("commands", {})
    if isinstance(commands, dict) and commands:
        lines.extend(
            [
                "## Commands",
                "| Command | Value |",
                "| --- | --- |",
            ]
        )
        for key in sorted(commands):
            lines.append(f"| {key} | `{_inline(commands[key])}` |")
        lines.append("")
    return "\n".join(lines)


def _build_or_load_setup_summary(
    *,
    repo_root: Path,
    workflow_root: Path,
    setup_summary_path: Path | None,
) -> tuple[dict[str, Any], Path, bool]:
    if setup_summary_path is not None:
        resolved = _resolve_path(setup_summary_path)
        return _load_json(resolved), resolved, False

    local_setup_root = workflow_root / "local_setup"
    summary = build_renderer_backend_local_setup(
        repo_root=repo_root,
        output_dir=local_setup_root,
    )
    summary_path = Path(summary["artifacts"]["summary_path"])
    env_path = Path(summary["artifacts"]["env_path"])
    _write_json(summary_path, summary)
    _write_text(env_path, _render_local_setup_env_file(summary))
    return summary, summary_path, True


def _reuse_search_roots_from_setup(summary: dict[str, Any]) -> list[Path]:
    raw_search_roots = summary.get("search_roots", [])
    if not isinstance(raw_search_roots, list):
        return []
    roots: list[Path] = []
    seen: set[str] = set()
    for item in raw_search_roots:
        if not isinstance(item, str) or not item.strip():
            continue
        path = _resolve_path(item)
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        roots.append(path)
    return roots


def _refresh_setup_summary(
    *,
    repo_root: Path,
    workflow_root: Path,
    prior_summary: dict[str, Any],
) -> tuple[dict[str, Any], Path, Path]:
    refresh_root = workflow_root / "local_setup_refreshed"
    refreshed = build_renderer_backend_local_setup(
        repo_root=repo_root,
        search_roots=_reuse_search_roots_from_setup(prior_summary),
        output_dir=refresh_root,
        include_default_search_roots=False,
    )
    summary_path = Path(refreshed["artifacts"]["summary_path"])
    env_path = Path(refreshed["artifacts"]["env_path"])
    _write_json(summary_path, refreshed)
    _write_text(env_path, _render_local_setup_env_file(refreshed))
    return refreshed, summary_path, env_path


def _extract_linux_handoff_docker_preflight(setup_summary: dict[str, Any]) -> dict[str, Any]:
    probes = setup_summary.get("probes", {})
    probe = probes.get("linux_handoff_docker_selftest", {}) if isinstance(probes, dict) else {}
    commands = setup_summary.get("commands", {})
    artifacts = setup_summary.get("artifacts", {})
    if not isinstance(probe, dict):
        probe = {}
    return {
        "available": bool(probe),
        "success": probe.get("success"),
        "execute": probe.get("execute"),
        "marker_exists": probe.get("marker_exists"),
        "marker_content": probe.get("marker_content"),
        "docker_return_code": (
            probe.get("docker", {}).get("return_code")
            if isinstance(probe.get("docker"), dict)
            else None
        ),
        "summary_path": probe.get("summary_path")
        or (
            artifacts.get("linux_handoff_docker_selftest_probe_path")
            if isinstance(artifacts, dict)
            else None
        ),
        "command": (
            commands.get("linux_handoff_docker_selftest")
            if isinstance(commands, dict)
            else None
        ),
        "probe": probe,
    }


def _build_blockers(
    *,
    backend: str,
    setup_summary: dict[str, Any],
    helios_ready: bool,
    backend_bin: str | None,
    backend_host_compatible: bool,
    backend_host_compatibility_reason: str | None,
    auto_acquire: bool,
    acquire_summary: dict[str, Any] | None,
    smoke_executed: bool,
    smoke_exit_code: int | None,
    planned_smoke_config_ready: bool,
    planned_smoke_config_error: str | None,
    recommended_next_command: str | None,
    run_linux_handoff_docker: bool = False,
    docker_handoff_preflight: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    acquisition_hints = setup_summary.get("acquisition_hints", {})
    backend_hints = acquisition_hints.get(backend, {}) if isinstance(acquisition_hints, dict) else {}
    if isinstance(backend_hints, dict) and backend_hints.get("platform_supported") is False:
        blockers.append(
            {
                "code": "BACKEND_PLATFORM_UNSUPPORTED",
                "severity": "warning",
                "message": str(backend_hints.get("platform_note") or f"{backend} platform unsupported"),
                "recommended_command": recommended_next_command,
            }
        )
    if not helios_ready:
        blockers.append(
            {
                "code": "HELIOS_RUNTIME_MISSING",
                "severity": "blocking",
                "message": "HELIOS runtime is not ready for backend smoke.",
                "recommended_command": None,
            }
        )
    if backend_bin is None:
        blockers.append(
            {
                "code": "BACKEND_BIN_MISSING",
                "severity": "blocking",
                "message": f"{backend.upper()} runtime binary is not resolved.",
                "recommended_command": recommended_next_command,
            }
        )
        if not auto_acquire:
            blockers.append(
                {
                    "code": "AUTO_ACQUIRE_DISABLED",
                    "severity": "blocking",
                    "message": "Workflow did not attempt package acquire because --auto-acquire is disabled.",
                    "recommended_command": recommended_next_command,
                }
            )
    elif not backend_host_compatible:
        blockers.append(
            {
                "code": "BACKEND_HOST_INCOMPATIBLE",
                "severity": "blocking",
                "message": (
                    backend_host_compatibility_reason
                    or f"{backend.upper()} runtime binary is not executable on the current host."
                ),
                "recommended_command": None,
            }
        )
    if auto_acquire and isinstance(acquire_summary, dict):
        acquire_readiness = acquire_summary.get("readiness", {})
        if acquire_readiness.get("download_url_resolved") is False:
            blockers.append(
                {
                    "code": "ACQUIRE_SOURCE_UNRESOLVED",
                    "severity": "blocking",
                    "message": "No package URL or local archive candidate was resolved for auto-acquire.",
                    "recommended_command": recommended_next_command,
                }
            )
        elif acquire_readiness.get("stage_ready") is False and backend_bin is None:
            blockers.append(
                {
                    "code": "ACQUIRE_OR_STAGE_FAILED",
                    "severity": "blocking",
                    "message": "Package acquire completed without producing a runnable backend binary.",
                    "recommended_command": recommended_next_command,
                }
            )
    if not planned_smoke_config_ready and planned_smoke_config_error:
        blockers.append(
            {
                "code": "SMOKE_CONFIG_UNRESOLVED",
                "severity": "blocking",
                "message": planned_smoke_config_error,
                "recommended_command": recommended_next_command,
            }
        )
    if smoke_executed and smoke_exit_code not in (None, 0):
        blockers.append(
            {
                "code": "SMOKE_EXECUTION_FAILED",
                "severity": "blocking",
                "message": f"Backend smoke exited with code {smoke_exit_code}.",
                "recommended_command": recommended_next_command,
            }
        )
    if (
        run_linux_handoff_docker
        and isinstance(docker_handoff_preflight, dict)
        and docker_handoff_preflight.get("available")
        and docker_handoff_preflight.get("success") is False
    ):
        blockers.append(
            {
                "code": "HANDOFF_DOCKER_PREFLIGHT_FAILED",
                "severity": "blocking",
                "message": "Docker handoff preflight probe failed.",
                "recommended_command": docker_handoff_preflight.get("command"),
            }
        )
    return blockers


def _relative_to_root(path: Path, root: Path) -> str | None:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return None


def _prepare_linux_runner_path_binding(
    *,
    raw_path: str | None,
    repo_root: Path,
    env_var: str,
    kind: str,
) -> tuple[str | None, dict[str, Any] | None]:
    if raw_path is None:
        return None, None
    resolved = _resolve_path(raw_path)
    relative = _relative_to_root(resolved, repo_root)
    binding = {
        "kind": kind,
        "env_var": env_var,
        "local_path": str(resolved),
        "repo_relative_path": relative,
        "uses_env": relative is None,
    }
    if relative is not None:
        return relative, binding
    return f"${{{env_var}}}", binding


def _linux_handoff_target_relative_path(
    *,
    binding: dict[str, Any],
    backend: str,
    linux_workflow_dir: str,
) -> str | None:
    repo_relative = binding.get("repo_relative_path")
    if isinstance(repo_relative, str) and repo_relative.strip():
        return repo_relative
    local_path = binding.get("local_path")
    if not isinstance(local_path, str) or not local_path.strip():
        return None
    base_name = Path(local_path).name
    subdir = {
        "scenario": "scenario",
        "helios_binary": "helios",
        "backend_runtime": backend,
        "backend_archive": "archive",
    }.get(str(binding.get("kind")), "misc")
    return f"{linux_workflow_dir}/linux_handoff_inputs/{subdir}/{base_name}"


def _render_linux_handoff_env_file(handoff: dict[str, Any]) -> str:
    backend = handoff.get("backend")
    bindings = handoff.get("path_bindings", {})
    selection = handoff.get("selection", {})
    linux_paths = handoff.get("linux_paths", {})
    required_env_vars = set(handoff.get("required_env_vars", []))
    lines = [
        "#!/usr/bin/env bash",
        "# Generated by renderer_backend_workflow.py",
        "# Source this file on a Linux runner before executing the handoff script.",
        "",
    ]
    config_path = linux_paths.get("handoff_config_path")
    if config_path:
        lines.append(
            f'export HANDOFF_SMOKE_CONFIG_PATH="${{HANDOFF_SMOKE_CONFIG_PATH:-{config_path}}}"'
        )
    else:
        lines.append("# export HANDOFF_SMOKE_CONFIG_PATH=<linux-path-to-renderer_backend_workflow_linux_handoff_config.json>")
    lines.append('export WORKFLOW_REPO_ROOT="${WORKFLOW_REPO_ROOT:-$(pwd)}"')
    backend_env_var, map_env_var = _BACKEND_ENV_VARS[str(backend)]
    backend_binding = bindings.get("backend_bin", {}) if isinstance(bindings, dict) else {}
    backend_relative = backend_binding.get("repo_relative_path") if isinstance(backend_binding, dict) else None
    if backend_relative:
        lines.append(f'export {backend_env_var}="${{{backend_env_var}:-{backend_relative}}}"')
    else:
        lines.append(f"# export {backend_env_var}=<linux-path-to-{backend}-runtime>")
    map_value = selection.get(map_env_var) if isinstance(selection, dict) else None
    if map_value:
        lines.append(f"export {map_env_var}={shlex.quote(str(map_value))}")
    else:
        lines.append(f"# export {map_env_var}=<set-me>")
    scenario_binding = bindings.get("scenario_path", {}) if isinstance(bindings, dict) else {}
    scenario_relative = scenario_binding.get("repo_relative_path") if isinstance(scenario_binding, dict) else None
    if scenario_relative:
        lines.append(f'export HANDOFF_SCENARIO_PATH="${{HANDOFF_SCENARIO_PATH:-{scenario_relative}}}"')
    elif isinstance(scenario_binding, dict) and scenario_binding.get("linux_relative_path"):
        lines.append(
            'export HANDOFF_SCENARIO_PATH="${HANDOFF_SCENARIO_PATH:-%s}"'
            % scenario_binding["linux_relative_path"]
        )
    elif "HANDOFF_SCENARIO_PATH" in required_env_vars:
        lines.append("# export HANDOFF_SCENARIO_PATH=<linux-path-to-scenario>")
    helios_binding = bindings.get("helios_bin", {}) if isinstance(bindings, dict) else {}
    helios_relative = helios_binding.get("repo_relative_path") if isinstance(helios_binding, dict) else None
    if helios_relative:
        lines.append(f'export HELIOS_BIN="${{HELIOS_BIN:-{helios_relative}}}"')
    elif isinstance(helios_binding, dict) and helios_binding.get("linux_relative_path"):
        lines.append(f'export HELIOS_BIN="${{HELIOS_BIN:-{helios_binding["linux_relative_path"]}}}"')
    elif "HELIOS_BIN" in required_env_vars:
        lines.append("# export HELIOS_BIN=<linux-path-to-helios-binary>")
    helios_docker_image = selection.get("HELIOS_DOCKER_IMAGE") if isinstance(selection, dict) else None
    helios_docker_binary = selection.get("HELIOS_DOCKER_BINARY") if isinstance(selection, dict) else None
    if helios_docker_image:
        lines.append(f"export HELIOS_DOCKER_IMAGE={shlex.quote(str(helios_docker_image))}")
    if helios_docker_binary:
        lines.append(f"export HELIOS_DOCKER_BINARY={shlex.quote(str(helios_docker_binary))}")
    lines.append("")
    return "\n".join(lines)


def _render_linux_handoff_script(handoff: dict[str, Any]) -> str:
    env_path = handoff.get("linux_paths", {}).get("handoff_env_path")
    backend = handoff.get("backend")
    runner_command = handoff.get("runner_command")
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "# Generated by renderer_backend_workflow.py",
        "",
    ]
    if env_path:
        lines.append(f"source {shlex.quote(str(env_path))}")
    else:
        lines.append("# export HANDOFF_SMOKE_CONFIG_PATH before running this script")
    lines.extend(
        [
            'REPO_ROOT="${WORKFLOW_REPO_ROOT:-$(pwd)}"',
            'CONFIG_PATH="${HANDOFF_SMOKE_CONFIG_PATH:?set HANDOFF_SMOKE_CONFIG_PATH}"',
            f'python3 "$REPO_ROOT/scripts/run_renderer_backend_smoke.py" --config "$CONFIG_PATH" --backend {backend}',
            "",
        ]
    )
    if runner_command:
        lines.append(f"# equivalent: {runner_command}")
        lines.append("")
    return "\n".join(lines)


def _render_linux_handoff_pack_script(handoff: dict[str, Any]) -> str:
    manifest_path = handoff.get("artifacts", {}).get("handoff_transfer_manifest_path")
    bundle_path = handoff.get("transfer_manifest", {}).get("bundle_path")
    bundle_manifest_path = handoff.get("transfer_manifest", {}).get("bundle_manifest_path")
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "# Generated by renderer_backend_workflow.py",
        "",
        f'MANIFEST_PATH="${{HANDOFF_TRANSFER_MANIFEST_PATH:-{manifest_path}}}"',
        f'BUNDLE_PATH="${{HANDOFF_BUNDLE_PATH:-{bundle_path}}}"',
        f'BUNDLE_MANIFEST_PATH="${{HANDOFF_BUNDLE_MANIFEST_PATH:-{bundle_manifest_path}}}"',
        'python3 - "$MANIFEST_PATH" "$BUNDLE_PATH" "$BUNDLE_MANIFEST_PATH" <<\'PY\'',
        "import hashlib",
        "import json",
        "import shutil",
        "import tarfile",
        "import tempfile",
        "import sys",
        "from pathlib import Path",
        "",
        "manifest = json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))",
        "bundle_path = Path(sys.argv[2]).expanduser().resolve()",
        "bundle_manifest_path = Path(sys.argv[3]).expanduser().resolve()",
        "entries = manifest.get('packable_entries', [])",
        "bundle_path.parent.mkdir(parents=True, exist_ok=True)",
        "with tempfile.TemporaryDirectory(prefix='linux_handoff_bundle_') as tmp:",
        "    staging_root = Path(tmp) / 'bundle'",
        "    staging_root.mkdir(parents=True, exist_ok=True)",
        "    for entry in entries:",
        "        src = Path(entry['local_path']).expanduser().resolve()",
        "        dst = staging_root / entry['target_relative_path']",
        "        if src.is_dir():",
        "            shutil.copytree(src, dst, dirs_exist_ok=True)",
        "        else:",
        "            dst.parent.mkdir(parents=True, exist_ok=True)",
        "            shutil.copy2(src, dst)",
        "    with tarfile.open(bundle_path, 'w:gz') as handle:",
        "        for path in sorted(staging_root.rglob('*')):",
        "            handle.add(path, arcname=str(path.relative_to(staging_root)))",
        "digest = hashlib.sha256()",
        "with bundle_path.open('rb') as handle:",
        "    while True:",
        "        chunk = handle.read(1024 * 1024)",
        "        if not chunk:",
        "            break",
        "        digest.update(chunk)",
        "bundle_manifest_path.parent.mkdir(parents=True, exist_ok=True)",
        "bundle_manifest_path.write_text(",
        "    json.dumps(",
        "        {",
        "            'bundle_path': str(bundle_path),",
        "            'bundle_sha256': digest.hexdigest(),",
        "            'transfer_manifest_path': str(Path(sys.argv[1]).expanduser().resolve()),",
        "            'entry_count': len(entries),",
        "        },",
        "        indent=2,",
        "    ),",
        "    encoding='utf-8',",
        ")",
        "print(str(bundle_path))",
        "PY",
        "",
    ]
    return "\n".join(lines)


def _render_linux_handoff_unpack_script(handoff: dict[str, Any]) -> str:
    linux_paths = handoff.get("linux_paths", {})
    transfer_manifest_path = handoff.get("artifacts", {}).get("handoff_transfer_manifest_path")
    bundle_path = handoff.get("transfer_manifest", {}).get("bundle_path")
    bundle_manifest_path = handoff.get("transfer_manifest", {}).get("bundle_manifest_path")
    handoff_script_path = linux_paths.get("handoff_script_path")
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "# Generated by renderer_backend_workflow.py",
        "",
        'REPO_ROOT="${WORKFLOW_REPO_ROOT:-$(pwd)}"',
        f'BUNDLE_PATH="${{HANDOFF_BUNDLE_PATH:-{bundle_path}}}"',
        f'TRANSFER_MANIFEST_PATH="${{HANDOFF_TRANSFER_MANIFEST_PATH:-{transfer_manifest_path}}}"',
        f'BUNDLE_MANIFEST_PATH="${{HANDOFF_BUNDLE_MANIFEST_PATH:-{bundle_manifest_path}}}"',
        'EXTRACT_ROOT="${HANDOFF_UNPACK_ROOT:-$REPO_ROOT}"',
        'mkdir -p "$EXTRACT_ROOT"',
        'tar -xzf "$BUNDLE_PATH" -C "$EXTRACT_ROOT"',
        'python3 - "$EXTRACT_ROOT" "$TRANSFER_MANIFEST_PATH" "$BUNDLE_MANIFEST_PATH" <<\'PY\'',
        "import hashlib",
        "import json",
        "import sys",
        "from pathlib import Path",
        "",
        "extract_root = Path(sys.argv[1]).expanduser().resolve()",
        "manifest = json.loads(Path(sys.argv[2]).read_text(encoding='utf-8'))",
        "bundle_manifest_path = Path(sys.argv[3]).expanduser().resolve()",
        "if bundle_manifest_path.exists():",
        "    bundle_manifest = json.loads(bundle_manifest_path.read_text(encoding='utf-8'))",
        "else:",
        "    bundle_manifest = {}",
        "for entry in manifest.get('verifiable_entries', []):",
        "    target_path = extract_root / entry['target_relative_path']",
        "    if not target_path.exists():",
        "        raise SystemExit(f\"Missing extracted handoff file: {target_path}\")",
        "    digest = hashlib.sha256()",
        "    with target_path.open('rb') as handle:",
        "        while True:",
        "            chunk = handle.read(1024 * 1024)",
        "            if not chunk:",
        "                break",
        "            digest.update(chunk)",
        "    if digest.hexdigest() != entry['sha256']:",
        "        raise SystemExit(f\"Checksum mismatch for {target_path}\")",
        "print(json.dumps({'verified_entries': len(manifest.get('verifiable_entries', [])), 'bundle_manifest_present': bool(bundle_manifest)}))",
        "PY",
        'if [[ "${HANDOFF_SKIP_RUN:-0}" == "1" ]]; then',
        '  echo "Verified handoff bundle. Skipping execution because HANDOFF_SKIP_RUN=1."',
        "  exit 0",
        "fi",
        f'bash "$EXTRACT_ROOT/{handoff_script_path}" "$@"',
        "",
    ]
    return "\n".join(lines)


def _render_linux_handoff_docker_script(handoff: dict[str, Any]) -> str:
    transfer_manifest_path = handoff.get("artifacts", {}).get("handoff_transfer_manifest_path")
    bundle_path = handoff.get("transfer_manifest", {}).get("bundle_path")
    bundle_manifest_path = handoff.get("transfer_manifest", {}).get("bundle_manifest_path")
    workflow_root = Path(
        handoff.get("artifacts", {}).get("handoff_config_path", ".")
    ).resolve().parent
    docker_output_root = workflow_root / "renderer_backend_linux_handoff_docker_run"
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "# Generated by renderer_backend_workflow.py",
        "# Default mode is verify-only inside Docker. Set HANDOFF_SKIP_RUN=0 to attempt execution too.",
        "",
        'REPO_ROOT="${WORKFLOW_REPO_ROOT:-$(pwd)}"',
        f'BUNDLE_PATH="${{HANDOFF_BUNDLE_PATH:-{bundle_path}}}"',
        f'MANIFEST_PATH="${{HANDOFF_TRANSFER_MANIFEST_PATH:-{transfer_manifest_path}}}"',
        (
            f'BUNDLE_MANIFEST_PATH="${{HANDOFF_BUNDLE_MANIFEST_PATH:-{bundle_manifest_path}}}"'
            if bundle_manifest_path
            else 'BUNDLE_MANIFEST_PATH="${HANDOFF_BUNDLE_MANIFEST_PATH:-}"'
        ),
        'DOCKER_BINARY="${HANDOFF_DOCKER_BINARY:-docker}"',
        (
            f'DOCKER_IMAGE="${{HANDOFF_DOCKER_IMAGE:-{_DEFAULT_LINUX_HANDOFF_DOCKER_IMAGE}}}"'
        ),
        (
            f'CONTAINER_WORKSPACE="${{HANDOFF_CONTAINER_WORKSPACE:-{_DEFAULT_LINUX_HANDOFF_CONTAINER_WORKSPACE}}}"'
        ),
        f'DOCKER_OUTPUT_ROOT="${{HANDOFF_DOCKER_OUTPUT_ROOT:-{docker_output_root}}}"',
        'SKIP_RUN="${HANDOFF_SKIP_RUN:-1}"',
        "",
        "cmd=(",
        '  python3 "$REPO_ROOT/scripts/run_renderer_backend_linux_handoff_docker.py"',
        '  --bundle "$BUNDLE_PATH"',
        '  --transfer-manifest "$MANIFEST_PATH"',
        '  --repo-root "$REPO_ROOT"',
        '  --output-root "$DOCKER_OUTPUT_ROOT"',
        '  --docker-binary "$DOCKER_BINARY"',
        '  --docker-image "$DOCKER_IMAGE"',
        '  --container-workspace "$CONTAINER_WORKSPACE"',
        ")",
        'if [[ -n "$BUNDLE_MANIFEST_PATH" ]]; then',
        '  cmd+=(--bundle-manifest "$BUNDLE_MANIFEST_PATH")',
        "fi",
        'if [[ "$SKIP_RUN" == "1" ]]; then',
        '  cmd+=(--skip-run)',
        "fi",
        'exec "${cmd[@]}"',
        "",
    ]
    return "\n".join(lines)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _pack_linux_handoff_bundle(
    *,
    transfer_manifest: dict[str, Any],
    bundle_path: Path,
    bundle_manifest_path: Path,
) -> dict[str, Any]:
    entries = transfer_manifest.get("packable_entries", [])
    if not isinstance(entries, list):
        entries = []
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="linux_handoff_bundle_") as tmp_dir:
        staging_root = Path(tmp_dir) / "bundle"
        staging_root.mkdir(parents=True, exist_ok=True)
        copied_entries = 0
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            local_path = entry.get("local_path")
            target_relative_path = entry.get("target_relative_path")
            if not isinstance(local_path, str) or not isinstance(target_relative_path, str):
                continue
            source = _resolve_path(local_path)
            destination = staging_root / target_relative_path
            if not source.exists():
                continue
            if source.is_dir():
                shutil.copytree(source, destination, dirs_exist_ok=True)
            else:
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, destination)
            copied_entries += 1
        with tarfile.open(bundle_path, "w:gz") as handle:
            for path in sorted(staging_root.rglob("*")):
                handle.add(path, arcname=str(path.relative_to(staging_root)))
    bundle_sha256 = _sha256_file(bundle_path)
    bundle_manifest = {
        "bundle_path": str(bundle_path),
        "bundle_sha256": bundle_sha256,
        "transfer_manifest_path": transfer_manifest.get("artifacts", {}).get("transfer_manifest_path"),
        "entry_count": len(entries),
        "copied_entry_count": copied_entries,
    }
    _write_json(bundle_manifest_path, bundle_manifest)
    return {
        "bundle_path": str(bundle_path),
        "bundle_manifest_path": str(bundle_manifest_path),
        "bundle_sha256": bundle_sha256,
        "copied_entry_count": copied_entries,
    }


def _verify_linux_handoff_bundle(
    *,
    transfer_manifest: dict[str, Any],
    bundle_path: Path,
    bundle_manifest_path: Path,
    extract_root: Path,
    verification_manifest_path: Path,
) -> dict[str, Any]:
    transfer_manifest_payload = dict(transfer_manifest)
    verifiable_entries = transfer_manifest_payload.get("verifiable_entries", [])
    if not isinstance(verifiable_entries, list):
        verifiable_entries = []
    bundle_manifest = _load_json(bundle_manifest_path) if bundle_manifest_path.exists() else {}
    extract_root.parent.mkdir(parents=True, exist_ok=True)
    if extract_root.exists():
        shutil.rmtree(extract_root)
    extract_root.mkdir(parents=True, exist_ok=True)
    with tarfile.open(bundle_path, "r:gz") as handle:
        handle.extractall(extract_root)
    verified_entries = 0
    missing_targets: list[str] = []
    checksum_mismatches: list[str] = []
    for entry in verifiable_entries:
        if not isinstance(entry, dict):
            continue
        target_relative_path = entry.get("target_relative_path")
        expected_sha = entry.get("sha256")
        if not isinstance(target_relative_path, str) or not isinstance(expected_sha, str):
            continue
        extracted = extract_root / target_relative_path
        if not extracted.exists():
            missing_targets.append(target_relative_path)
            continue
        actual_sha = _sha256_file(extracted)
        if actual_sha != expected_sha:
            checksum_mismatches.append(target_relative_path)
            continue
        verified_entries += 1
    bundle_sha_matches = None
    if bundle_manifest.get("bundle_sha256"):
        bundle_sha_matches = _sha256_file(bundle_path) == bundle_manifest["bundle_sha256"]
    verified = not missing_targets and not checksum_mismatches and bundle_sha_matches is not False
    verification_payload = {
        "bundle_path": str(bundle_path),
        "bundle_manifest_path": str(bundle_manifest_path),
        "extract_root": str(extract_root),
        "verified": verified,
        "verified_entry_count": verified_entries,
        "missing_targets": missing_targets,
        "checksum_mismatches": checksum_mismatches,
        "bundle_sha_matches": bundle_sha_matches,
    }
    _write_json(verification_manifest_path, verification_payload)
    return verification_payload


def _finalize_linux_handoff_transfer_manifest(transfer_manifest: dict[str, Any]) -> dict[str, Any]:
    finalized = copy.deepcopy(transfer_manifest)
    verifiable_entries: list[dict[str, Any]] = []
    finalized_entries: list[dict[str, Any]] = []
    for entry in finalized.get("entries", []):
        if not isinstance(entry, dict):
            continue
        enriched = dict(entry)
        local_path = enriched.get("local_path")
        if isinstance(local_path, str) and local_path.strip():
            resolved = _resolve_path(local_path)
            enriched["local_exists"] = resolved.exists()
            enriched["local_is_dir"] = resolved.is_dir()
            if resolved.exists() and resolved.is_file():
                enriched["size_bytes"] = resolved.stat().st_size
                enriched["sha256"] = _sha256_file(resolved)
                verifiable_entries.append(
                    {
                        "target_relative_path": enriched["target_relative_path"],
                        "sha256": enriched["sha256"],
                        "size_bytes": enriched["size_bytes"],
                        "kind": enriched.get("kind"),
                    }
                )
            elif resolved.exists() and resolved.is_dir():
                enriched["size_bytes"] = None
                enriched["sha256"] = None
            else:
                enriched["size_bytes"] = None
                enriched["sha256"] = None
        finalized_entries.append(enriched)
    finalized["entries"] = finalized_entries
    finalized["packable_entries"] = [
        entry for entry in finalized_entries if entry.get("local_exists") or entry.get("source_scope") == "generated"
    ]
    finalized["missing_entries"] = [
        entry for entry in finalized_entries if not entry.get("local_exists") and entry.get("source_scope") != "generated"
    ]
    finalized["packable_entry_count"] = len(finalized["packable_entries"])
    finalized["missing_entry_count"] = len(finalized["missing_entries"])
    finalized["verifiable_entries"] = verifiable_entries
    finalized["verifiable_entry_count"] = len(verifiable_entries)
    return finalized


def _build_linux_handoff_transfer_manifest(
    *,
    backend: str,
    repo_root: Path,
    workflow_root: Path,
    linux_handoff: dict[str, Any],
) -> dict[str, Any]:
    linux_paths = linux_handoff.get("linux_paths", {})
    artifacts = linux_handoff.get("artifacts", {})
    entries: list[dict[str, Any]] = []
    local_bundle_path = workflow_root / "renderer_backend_workflow_linux_handoff_bundle.tar.gz"

    for artifact_key, linux_key, kind in (
        ("handoff_config_path", "handoff_config_path", "handoff_generated_config"),
        ("handoff_env_path", "handoff_env_path", "handoff_generated_env"),
        ("handoff_script_path", "handoff_script_path", "handoff_generated_script"),
    ):
        local_path = artifacts.get(artifact_key)
        target_relative_path = linux_paths.get(linux_key)
        if not isinstance(local_path, str) or not isinstance(target_relative_path, str):
            continue
        entries.append(
            {
                "kind": kind,
                "local_path": str(_resolve_path(local_path)),
                "target_relative_path": target_relative_path,
                "source_scope": "generated",
                "env_var": None,
            }
        )

    path_bindings = linux_handoff.get("path_bindings", {})
    if isinstance(path_bindings, dict):
        for binding in path_bindings.values():
            if not isinstance(binding, dict):
                continue
            local_path = binding.get("local_path")
            if not isinstance(local_path, str) or not local_path.strip():
                continue
            target_relative_path = binding.get("linux_relative_path")
            if not isinstance(target_relative_path, str) or not target_relative_path.strip():
                target_relative_path = _linux_handoff_target_relative_path(
                    binding=binding,
                    backend=backend,
                    linux_workflow_dir=str(linux_paths.get("workflow_dir", "")),
                )
            if not target_relative_path:
                continue
            entries.append(
                {
                    "kind": binding.get("kind"),
                    "local_path": str(_resolve_path(local_path)),
                    "target_relative_path": target_relative_path,
                    "source_scope": (
                        "repo"
                        if _relative_to_root(_resolve_path(local_path), repo_root) is not None
                        else "external"
                    ),
                    "env_var": binding.get("env_var"),
                }
            )

    transfer_candidates = linux_handoff.get("transfer_candidates", [])
    if isinstance(transfer_candidates, list):
        for binding in transfer_candidates:
            if not isinstance(binding, dict):
                continue
            local_path = binding.get("local_path")
            if not isinstance(local_path, str) or not local_path.strip():
                continue
            target_relative_path = binding.get("linux_relative_path")
            if not isinstance(target_relative_path, str) or not target_relative_path.strip():
                target_relative_path = _linux_handoff_target_relative_path(
                    binding=binding,
                    backend=backend,
                    linux_workflow_dir=str(linux_paths.get("workflow_dir", "")),
                )
            if not target_relative_path:
                continue
            entries.append(
                {
                    "kind": binding.get("kind"),
                    "local_path": str(_resolve_path(local_path)),
                    "target_relative_path": target_relative_path,
                    "source_scope": (
                        "repo"
                        if _relative_to_root(_resolve_path(local_path), repo_root) is not None
                        else "external"
                    ),
                    "env_var": binding.get("env_var"),
                }
            )

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for entry in entries:
        key = (str(entry["local_path"]), str(entry["target_relative_path"]))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)

    packable_entries: list[dict[str, Any]] = []
    missing_entries: list[dict[str, Any]] = []
    for entry in deduped:
        resolved = _resolve_path(entry["local_path"])
        if entry.get("source_scope") == "generated" or resolved.exists():
            packable_entries.append(entry)
        else:
            missing_entries.append(entry)

    return {
        "entry_count": len(deduped),
        "packable_entry_count": len(packable_entries),
        "missing_entry_count": len(missing_entries),
        "verifiable_entry_count": 0,
        "entries": deduped,
        "packable_entries": packable_entries,
        "missing_entries": missing_entries,
        "verifiable_entries": [],
        "bundle_path": str(local_bundle_path),
        "bundle_manifest_path": str(
            workflow_root / "renderer_backend_workflow_linux_handoff_bundle_manifest.json"
        ),
        "pack_command": f"bash {workflow_root / 'renderer_backend_workflow_linux_handoff_pack.sh'}",
    }


def _build_linux_handoff(
    *,
    backend: str,
    repo_root: Path,
    workflow_root: Path,
    final_selection: dict[str, Any],
    planned_effective_config: dict[str, Any],
    planned_smoke_config_ready: bool,
    planned_smoke_config_error: str | None,
    selected_backend_bin: str | None,
    selected_renderer_map: str | None,
    backend_host_compatible: bool,
    backend_host_compatibility_reason: str | None,
    acquire_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    backend_env_var, map_env_var = _BACKEND_ENV_VARS[backend]
    workflow_relative = _relative_to_root(workflow_root, repo_root)
    linux_workflow_dir = workflow_relative or f"artifacts/renderer_backend_workflow/{backend}"
    handoff_config_path = workflow_root / "renderer_backend_workflow_linux_handoff_config.json"
    handoff_env_path = workflow_root / "renderer_backend_workflow_linux_handoff.env.sh"
    handoff_script_path = workflow_root / "renderer_backend_workflow_linux_handoff.sh"
    handoff_script_relative = _relative_to_root(handoff_script_path, repo_root)
    ready = bool(planned_smoke_config_ready)
    path_bindings: dict[str, Any] = {}
    required_env_vars: list[str] = []
    transfer_candidates: list[dict[str, Any]] = []
    handoff_config = (
        copy.deepcopy(planned_effective_config)
        if planned_smoke_config_ready
        else {
            "error": planned_smoke_config_error or "Planned smoke config is not ready.",
        }
    )

    if planned_smoke_config_ready:
        scenario_value, scenario_binding = _prepare_linux_runner_path_binding(
            raw_path=str(handoff_config.get("scenario_path")) if handoff_config.get("scenario_path") else None,
            repo_root=repo_root,
            env_var="HANDOFF_SCENARIO_PATH",
            kind="scenario",
        )
        if scenario_value is not None:
            handoff_config["scenario_path"] = scenario_value
        if scenario_binding is not None:
            scenario_binding["linux_relative_path"] = _linux_handoff_target_relative_path(
                binding=scenario_binding,
                backend=backend,
                linux_workflow_dir=linux_workflow_dir,
            )
            path_bindings["scenario_path"] = scenario_binding
            if scenario_binding["uses_env"]:
                required_env_vars.append("HANDOFF_SCENARIO_PATH")
                transfer_candidates.append(scenario_binding)

        if handoff_config.get("helios_bin"):
            helios_value, helios_binding = _prepare_linux_runner_path_binding(
                raw_path=str(handoff_config.get("helios_bin")),
                repo_root=repo_root,
                env_var="HELIOS_BIN",
                kind="helios_binary",
            )
            if helios_value is not None:
                handoff_config["helios_bin"] = helios_value
            if helios_binding is not None:
                helios_binding["linux_relative_path"] = _linux_handoff_target_relative_path(
                    binding=helios_binding,
                    backend=backend,
                    linux_workflow_dir=linux_workflow_dir,
                )
                path_bindings["helios_bin"] = helios_binding
                if helios_binding["uses_env"]:
                    required_env_vars.append("HELIOS_BIN")
                    transfer_candidates.append(helios_binding)

        handoff_config["output_dir"] = f"{linux_workflow_dir}/linux_handoff/smoke_run"
        options = dict(handoff_config.get("options", {}))
        for key in ("renderer_cwd", "renderer_bin", "awsim_wrapper", "carla_wrapper"):
            options.pop(key, None)
        backend_value, backend_binding = _prepare_linux_runner_path_binding(
            raw_path=selected_backend_bin,
            repo_root=repo_root,
            env_var=backend_env_var,
            kind="backend_runtime",
        )
        if backend_value is not None:
            options[f"{backend}_bin"] = backend_value
        if backend_binding is not None:
            backend_binding["linux_relative_path"] = _linux_handoff_target_relative_path(
                binding=backend_binding,
                backend=backend,
                linux_workflow_dir=linux_workflow_dir,
            )
            path_bindings["backend_bin"] = backend_binding
            if backend_binding["uses_env"]:
                required_env_vars.append(backend_env_var)
                transfer_candidates.append(backend_binding)
        if selected_renderer_map is not None:
            options["renderer_map"] = selected_renderer_map
        handoff_config["options"] = options

    acquire_download = acquire_summary.get("download", {}) if isinstance(acquire_summary, dict) else {}
    if isinstance(acquire_download, dict):
        archive_path = acquire_download.get("target_path")
        if isinstance(archive_path, str) and archive_path.strip():
            transfer_candidates.append(
                {
                    "kind": "backend_archive",
                    "env_var": None,
                    "local_path": str(_resolve_path(archive_path)),
                    "repo_relative_path": _relative_to_root(_resolve_path(archive_path), repo_root),
                    "linux_relative_path": f"{linux_workflow_dir}/linux_handoff_inputs/archive/{Path(archive_path).name}",
                    "uses_env": False,
                }
            )

    reason_codes: list[str] = []
    if selected_backend_bin is not None and not backend_host_compatible:
        reason_codes.append("BACKEND_HOST_INCOMPATIBLE")
    if backend_host_compatibility_reason:
        reason_codes.append("HOST_COMPATIBILITY_REASON_AVAILABLE")
    runner_command = (
        f'python3 scripts/run_renderer_backend_smoke.py --config "${{HANDOFF_SMOKE_CONFIG_PATH}}" --backend {backend}'
    )
    script_command = (
        f"bash {handoff_script_relative}"
        if handoff_script_relative is not None
        else f"bash {handoff_script_path.name}"
    )
    handoff = {
        "ready": ready,
        "backend": backend,
        "target_platform": "Linux x86_64",
        "reason_codes": reason_codes,
        "reason": backend_host_compatibility_reason,
        "required_env_vars": sorted(set(required_env_vars)),
        "path_bindings": path_bindings,
        "transfer_candidates": transfer_candidates,
        "selection": final_selection,
        "config": handoff_config,
        "runner_command": runner_command,
        "script_command": script_command,
        "linux_paths": {
            "workflow_dir": linux_workflow_dir,
            "handoff_config_path": (
                f"{linux_workflow_dir}/renderer_backend_workflow_linux_handoff_config.json"
            ),
            "handoff_env_path": (
                f"{linux_workflow_dir}/renderer_backend_workflow_linux_handoff.env.sh"
            ),
            "handoff_script_path": (
                f"{linux_workflow_dir}/renderer_backend_workflow_linux_handoff.sh"
            ),
        },
        "artifacts": {
            "handoff_config_path": str(handoff_config_path),
            "handoff_env_path": str(handoff_env_path),
            "handoff_script_path": str(handoff_script_path),
            "handoff_transfer_manifest_path": str(
                workflow_root / "renderer_backend_workflow_linux_handoff_transfer_manifest.json"
            ),
            "handoff_pack_script_path": str(
                workflow_root / "renderer_backend_workflow_linux_handoff_pack.sh"
            ),
        },
    }
    handoff["transfer_manifest"] = _build_linux_handoff_transfer_manifest(
        backend=backend,
        repo_root=repo_root,
        workflow_root=workflow_root,
        linux_handoff=handoff,
    )
    handoff["pack_command"] = handoff["transfer_manifest"]["pack_command"]
    handoff["unpack_command"] = (
        f"bash {workflow_root / 'renderer_backend_workflow_linux_handoff_unpack.sh'}"
    )
    handoff["helper_command"] = (
        'python3 scripts/run_renderer_backend_linux_handoff.py '
        '--bundle "${HANDOFF_BUNDLE_PATH}" '
        '--transfer-manifest "${HANDOFF_TRANSFER_MANIFEST_PATH}" '
        '--bundle-manifest "${HANDOFF_BUNDLE_MANIFEST_PATH}" '
        '--repo-root "${WORKFLOW_REPO_ROOT:-$(pwd)}"'
    )
    handoff["docker_helper_command"] = (
        'python3 scripts/run_renderer_backend_linux_handoff_docker.py '
        '--bundle "${HANDOFF_BUNDLE_PATH}" '
        '--transfer-manifest "${HANDOFF_TRANSFER_MANIFEST_PATH}" '
        '--bundle-manifest "${HANDOFF_BUNDLE_MANIFEST_PATH}" '
        '--repo-root "${WORKFLOW_REPO_ROOT:-$(pwd)}" '
        '--docker-binary "${HANDOFF_DOCKER_BINARY:-docker}" '
        f'--docker-image "${{HANDOFF_DOCKER_IMAGE:-{_DEFAULT_LINUX_HANDOFF_DOCKER_IMAGE}}}" '
        f'--container-workspace "${{HANDOFF_CONTAINER_WORKSPACE:-{_DEFAULT_LINUX_HANDOFF_CONTAINER_WORKSPACE}}}"'
    )
    handoff["docker_script_command"] = (
        f"bash {workflow_root / 'renderer_backend_workflow_linux_handoff_docker.sh'}"
    )
    return handoff


def _render_next_step_script(summary: dict[str, Any]) -> str:
    env_path = summary.get("artifacts", {}).get("env_path")
    recommended = summary.get("recommended_next_command")
    rerun_smoke_command = summary.get("commands", {}).get("rerun_smoke")
    smoke_command = summary.get("commands", {}).get("smoke")
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "# Generated by renderer_backend_workflow.py",
        "",
    ]
    if env_path:
        lines.append(f"source {shlex.quote(str(env_path))}")
    lines.append("")
    if recommended:
        lines.append(recommended)
    elif rerun_smoke_command:
        lines.append(rerun_smoke_command)
    elif smoke_command:
        lines.append(smoke_command)
    else:
        lines.append("echo 'No recommended next command available.'")
        lines.append("exit 1")
    lines.append("")
    return "\n".join(lines)


def _determine_smoke_config(
    *,
    backend: str,
    repo_root: Path,
    explicit_config: Path | None,
    selection: dict[str, Any],
) -> tuple[Path, str]:
    if explicit_config is not None:
        return _resolve_path(explicit_config), "explicit"
    helios_bin = _selection_value(selection, "HELIOS_BIN")
    use_docker = not helios_bin and bool(
        _selection_value(selection, "HELIOS_DOCKER_IMAGE")
        and _selection_value(selection, "HELIOS_DOCKER_BINARY")
    )
    mode = "docker" if use_docker else "binary"
    return (repo_root / _DEFAULT_SMOKE_PRESETS[backend][mode]).resolve(), f"default_{mode}_preset"


def build_renderer_backend_workflow(
    *,
    backend: str,
    repo_root: Path,
    workflow_root: Path,
    setup_summary_path: Path | None = None,
    config_path: Path | None = None,
    backend_bin_override: str | None = None,
    renderer_map_override: str | None = None,
    auto_acquire: bool = False,
    download_url: str | None = None,
    download_name: str | None = None,
    download_dir: Path | None = None,
    overwrite_download: bool = False,
    dry_run: bool = False,
    option_overrides: list[str] | None = None,
    pack_linux_handoff: bool = False,
    verify_linux_handoff_bundle: bool = False,
    run_linux_handoff_docker: bool = False,
    docker_handoff_execute: bool = False,
    docker_binary: str = "docker",
    docker_image: str = _DEFAULT_LINUX_HANDOFF_DOCKER_IMAGE,
    docker_container_workspace: str = _DEFAULT_LINUX_HANDOFF_CONTAINER_WORKSPACE,
) -> dict[str, Any]:
    backend = backend.strip().lower()
    if backend not in _BACKEND_ENV_VARS:
        raise ValueError(f"Unsupported backend: {backend}")
    repo_root = repo_root.resolve()
    workflow_root = workflow_root.resolve()
    workflow_root.mkdir(parents=True, exist_ok=True)
    issues: list[str] = []
    option_overrides = list(option_overrides or [])

    setup_summary, resolved_setup_summary_path, generated_setup = _build_or_load_setup_summary(
        repo_root=repo_root,
        workflow_root=workflow_root,
        setup_summary_path=setup_summary_path,
    )
    setup_readiness = setup_summary.get("readiness", {})
    setup_selection = dict(setup_summary.get("selection", {}))
    backend_env_var, map_env_var = _BACKEND_ENV_VARS[backend]

    acquire_summary: dict[str, Any] | None = None
    refreshed_setup_summary: dict[str, Any] | None = None
    refreshed_setup_summary_path: Path | None = None
    refreshed_setup_env_path: Path | None = None
    selected_backend_bin = (
        str(_resolve_path(backend_bin_override))
        if backend_bin_override
        else _selection_value(setup_selection, backend_env_var)
    )
    selected_renderer_map = renderer_map_override or _selection_value(setup_selection, map_env_var)

    if selected_backend_bin is None and auto_acquire:
        acquire_output_root = repo_root / "third_party" / "runtime_backends" / backend
        acquire_summary = build_renderer_backend_package_acquire(
            backend=backend,
            repo_root=repo_root,
            setup_summary_path=resolved_setup_summary_path,
            download_url=download_url,
            download_name=download_name,
            download_dir=download_dir,
            output_root=acquire_output_root,
            dry_run=dry_run,
            overwrite_download=overwrite_download,
        )
        issues.extend(str(item) for item in acquire_summary.get("issues", []) if str(item).strip())
        stage_summary = acquire_summary.get("stage")
        if isinstance(stage_summary, dict):
            stage_selection = stage_summary.get("selection", {})
            if isinstance(stage_selection, dict):
                if selected_backend_bin is None:
                    selected_backend_bin = _selection_value(stage_selection, backend_env_var)
                if selected_renderer_map is None:
                    selected_renderer_map = _selection_value(stage_selection, map_env_var)
        if acquire_summary.get("readiness", {}).get("stage_ready") and not dry_run:
            (
                refreshed_setup_summary,
                refreshed_setup_summary_path,
                refreshed_setup_env_path,
            ) = _refresh_setup_summary(
                repo_root=repo_root,
                workflow_root=workflow_root,
                prior_summary=setup_summary,
            )
            refreshed_selection = refreshed_setup_summary.get("selection", {})
            if isinstance(refreshed_selection, dict):
                selected_backend_bin = _selection_value(refreshed_selection, backend_env_var) or selected_backend_bin
                if selected_renderer_map is None:
                    selected_renderer_map = _selection_value(refreshed_selection, map_env_var)

    active_setup_summary = refreshed_setup_summary or setup_summary
    active_setup_summary_path = refreshed_setup_summary_path or resolved_setup_summary_path
    active_setup_selection = dict(active_setup_summary.get("selection", {}))
    active_setup_readiness = active_setup_summary.get("readiness", {})
    docker_handoff_preflight = _extract_linux_handoff_docker_preflight(active_setup_summary)

    smoke_config_path, smoke_config_source = _determine_smoke_config(
        backend=backend,
        repo_root=repo_root,
        explicit_config=config_path,
        selection=active_setup_selection,
    )
    backend_compatibility = (
        _inspect_executable_host_compatibility(_resolve_path(selected_backend_bin))
        if selected_backend_bin is not None
        else {
            "host_compatible": False,
            "host_compatibility_reason": "backend runtime binary is not resolved",
            "binary_format": "missing",
            "file_description": "",
        }
    )
    backend_host_compatible = bool(backend_compatibility.get("host_compatible"))
    backend_host_compatibility_reason = str(
        backend_compatibility.get("host_compatibility_reason") or ""
    ).strip() or None
    helios_ready = bool(
        active_setup_readiness.get("helios_ready")
        or _selection_value(active_setup_selection, "HELIOS_BIN")
        or (
            _selection_value(active_setup_selection, "HELIOS_DOCKER_IMAGE")
            and _selection_value(active_setup_selection, "HELIOS_DOCKER_BINARY")
        )
    )
    smoke_output_dir = workflow_root / "smoke_run"
    smoke_summary_path = smoke_output_dir / "renderer_backend_smoke_summary.json"
    smoke_markdown_path = smoke_output_dir / "renderer_backend_smoke_report.md"
    smoke_html_path = smoke_output_dir / "renderer_backend_smoke_report.html"
    workflow_smoke_config_path = workflow_root / "renderer_backend_workflow_smoke_config.json"
    workflow_rerun_smoke_script_path = workflow_root / "renderer_backend_workflow_rerun_smoke.sh"

    smoke_args: list[str] = [
        "--config",
        str(smoke_config_path),
        "--backend",
        backend,
        "--output-dir",
        str(smoke_output_dir),
        "--summary-path",
        str(smoke_summary_path),
        "--markdown-report-path",
        str(smoke_markdown_path),
        "--html-report-path",
        str(smoke_html_path),
    ]
    if selected_backend_bin is not None:
        smoke_args.extend(["--backend-bin", selected_backend_bin])
    if selected_renderer_map is not None:
        smoke_args.extend(["--renderer-map", selected_renderer_map])
    for raw in option_overrides:
        smoke_args.extend(["--set-option", raw])
    planned_smoke_config_ready = False
    planned_smoke_config_error: str | None = None
    planned_forced_options: dict[str, Any] = {}
    try:
        smoke_base_config = _load_json(smoke_config_path)
        planned_effective_config, planned_forced_options = _build_smoke_effective_config(
            base_config=smoke_base_config,
            backend=backend,
            output_dir_override=smoke_output_dir,
            backend_bin_override=selected_backend_bin,
            renderer_map_override=selected_renderer_map,
            option_overrides=option_overrides,
            repo_root=repo_root,
            env_overrides=active_setup_selection,
        )
        planned_smoke_config_ready = True
    except Exception as exc:
        planned_smoke_config_error = str(exc)
        issues.append(f"Planned smoke config could not be materialized: {exc}")
        planned_effective_config = {
            "error": str(exc),
            "config_path": str(smoke_config_path),
            "config_source": smoke_config_source,
        }

    smoke_executed = False
    smoke_exit_code: int | None = None
    smoke_summary: dict[str, Any] | None = None
    smoke_stdout = ""
    smoke_ready = helios_ready and selected_backend_bin is not None and backend_host_compatible
    if not helios_ready:
        issues.append("HELIOS runtime is not ready for backend smoke.")
    if selected_backend_bin is None:
        issues.append(f"{backend_env_var} is not resolved for backend smoke.")
    elif not backend_host_compatible:
        issues.append(
            backend_host_compatibility_reason
            or f"{backend_env_var} is resolved but not executable on the current host."
        )

    if smoke_ready and not dry_run:
        smoke_executed = True
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            smoke_exit_code = smoke_main(smoke_args)
            smoke_stdout = stdout.getvalue()
        if smoke_summary_path.exists():
            smoke_summary = _load_json(smoke_summary_path)
    elif dry_run and not smoke_ready:
        smoke_exit_code = 1

    if dry_run:
        status = "DRY_RUN_READY" if smoke_ready else "DRY_RUN_BLOCKED"
    elif smoke_executed and smoke_exit_code == 0:
        status = "SMOKE_SUCCEEDED"
    elif smoke_executed:
        status = "SMOKE_FAILED"
    elif selected_backend_bin is None and acquire_summary is not None and acquire_summary.get("readiness", {}).get("download_url_resolved"):
        status = "ACQUIRE_BLOCKED"
    else:
        status = "BLOCKED"

    summary_path = workflow_root / "renderer_backend_workflow_summary.json"
    env_path = workflow_root / "renderer_backend_workflow.env.sh"
    report_path = workflow_root / "renderer_backend_workflow_report.md"
    final_selection = dict(active_setup_selection)
    final_selection[backend_env_var] = selected_backend_bin
    if selected_renderer_map is not None:
        final_selection[map_env_var] = selected_renderer_map
    recommended_next_command = None
    if status in {"BLOCKED", "DRY_RUN_BLOCKED", "ACQUIRE_BLOCKED"}:
        recommended_next_command = (
            "python3 scripts/acquire_renderer_backend_package.py "
            f"--backend {backend} --setup-summary {active_setup_summary_path}"
        )
    elif status == "DRY_RUN_READY":
        recommended_next_command = "python3 scripts/run_renderer_backend_smoke.py " + " ".join(
            f"\"{arg}\"" if " " in arg else arg for arg in smoke_args
        )
    blockers = _build_blockers(
        backend=backend,
        setup_summary=setup_summary,
        helios_ready=helios_ready,
        backend_bin=selected_backend_bin,
        backend_host_compatible=backend_host_compatible,
        backend_host_compatibility_reason=backend_host_compatibility_reason,
        auto_acquire=auto_acquire,
        acquire_summary=acquire_summary,
        smoke_executed=smoke_executed,
        smoke_exit_code=smoke_exit_code,
        planned_smoke_config_ready=planned_smoke_config_ready,
        planned_smoke_config_error=planned_smoke_config_error,
        recommended_next_command=recommended_next_command,
        run_linux_handoff_docker=run_linux_handoff_docker,
        docker_handoff_preflight=docker_handoff_preflight,
    )
    linux_handoff = _build_linux_handoff(
        backend=backend,
        repo_root=repo_root,
        workflow_root=workflow_root,
        final_selection=final_selection,
        planned_effective_config=planned_effective_config,
        planned_smoke_config_ready=planned_smoke_config_ready,
        planned_smoke_config_error=planned_smoke_config_error,
        selected_backend_bin=selected_backend_bin,
        selected_renderer_map=selected_renderer_map,
        backend_host_compatible=backend_host_compatible,
        backend_host_compatibility_reason=backend_host_compatibility_reason,
        acquire_summary=acquire_summary,
    )
    if (
        status in {"BLOCKED", "DRY_RUN_BLOCKED", "ACQUIRE_BLOCKED"}
        and linux_handoff.get("ready")
        and selected_backend_bin is not None
        and not backend_host_compatible
    ):
        recommended_next_command = linux_handoff.get("script_command") or linux_handoff.get("runner_command")
        if (
            run_linux_handoff_docker
            and docker_handoff_preflight.get("available")
            and docker_handoff_preflight.get("success") is False
        ):
            recommended_next_command = docker_handoff_preflight.get("command") or recommended_next_command
        blockers = _build_blockers(
            backend=backend,
            setup_summary=setup_summary,
            helios_ready=helios_ready,
            backend_bin=selected_backend_bin,
            backend_host_compatible=backend_host_compatible,
            backend_host_compatibility_reason=backend_host_compatibility_reason,
            auto_acquire=auto_acquire,
            acquire_summary=acquire_summary,
            smoke_executed=smoke_executed,
            smoke_exit_code=smoke_exit_code,
            planned_smoke_config_ready=planned_smoke_config_ready,
            planned_smoke_config_error=planned_smoke_config_error,
            recommended_next_command=recommended_next_command,
            run_linux_handoff_docker=run_linux_handoff_docker,
            docker_handoff_preflight=docker_handoff_preflight,
        )
    return {
        "backend": backend,
        "status": status,
        "success": status in {"SMOKE_SUCCEEDED", "DRY_RUN_READY"},
        "dry_run": dry_run,
        "generated_setup": generated_setup,
        "issues": issues,
        "blockers": blockers,
        "final_selection": final_selection,
        "recommended_next_command": recommended_next_command,
        "setup": {
            "summary_path": str(resolved_setup_summary_path),
            "readiness": setup_readiness,
            "selection": setup_selection,
            "probes": setup_summary.get("probes"),
            "commands": setup_summary.get("commands"),
        },
        "refreshed_setup": (
            {
                "summary_path": str(refreshed_setup_summary_path),
                "env_path": str(refreshed_setup_env_path),
                "readiness": refreshed_setup_summary.get("readiness"),
                "selection": refreshed_setup_summary.get("selection"),
                "probes": refreshed_setup_summary.get("probes"),
                "commands": refreshed_setup_summary.get("commands"),
            }
            if refreshed_setup_summary is not None
            else None
        ),
        "acquire": acquire_summary,
        "smoke": {
            "ready": smoke_ready,
            "executed": smoke_executed,
            "exit_code": smoke_exit_code,
            "config_path": str(smoke_config_path),
            "config_source": smoke_config_source,
            "output_dir": str(smoke_output_dir),
            "summary_path": str(smoke_summary_path),
            "planned_effective_config_path": str(workflow_smoke_config_path),
            "planned_effective_config_ready": planned_smoke_config_ready,
            "planned_effective_config_error": planned_smoke_config_error,
            "planned_effective_config": planned_effective_config,
            "planned_forced_options": planned_forced_options,
            "stdout": smoke_stdout,
            "backend_bin": selected_backend_bin,
            "backend_host_compatible": backend_host_compatible,
            "backend_host_compatibility_reason": backend_host_compatibility_reason,
            "backend_binary_format": backend_compatibility.get("binary_format"),
            "backend_binary_architectures": backend_compatibility.get("binary_architectures"),
            "backend_translation_required": backend_compatibility.get("translation_required"),
            "backend_file_description": backend_compatibility.get("file_description"),
            "renderer_map": selected_renderer_map,
            "args": smoke_args,
            "summary": smoke_summary,
        },
        "linux_handoff": {
            **linux_handoff,
            "bundle": {
                "pack_requested": pack_linux_handoff,
                "verify_requested": verify_linux_handoff_bundle,
                "bundle_generated": False,
                "bundle_verified": False,
                "bundle_path": linux_handoff["transfer_manifest"].get("bundle_path"),
                "bundle_manifest_path": linux_handoff["transfer_manifest"].get("bundle_manifest_path"),
                "verification_manifest_path": str(
                    workflow_root / "renderer_backend_workflow_linux_handoff_verification.json"
                ),
                "verification_extract_root": str(
                    workflow_root / "renderer_backend_workflow_linux_handoff_verify_extract"
                ),
            },
            "env_text": _render_linux_handoff_env_file(linux_handoff),
            "script_text": _render_linux_handoff_script(linux_handoff),
            "pack_script_text": _render_linux_handoff_pack_script(linux_handoff),
            "unpack_script_text": _render_linux_handoff_unpack_script(linux_handoff),
            "docker_script_text": _render_linux_handoff_docker_script(linux_handoff),
        },
        "docker_handoff": {
            "requested": run_linux_handoff_docker,
            "ready": bool(linux_handoff.get("ready")),
            "preflight": docker_handoff_preflight,
            "execute_in_container": docker_handoff_execute,
            "skip_run": not docker_handoff_execute,
            "docker_binary": docker_binary,
            "docker_image": docker_image,
            "container_workspace": docker_container_workspace,
            "output_root": str(workflow_root / "renderer_backend_linux_handoff_docker_run"),
            "summary_path": str(
                workflow_root
                / "renderer_backend_linux_handoff_docker_run"
                / "renderer_backend_linux_handoff_docker_run.json"
            ),
            "executed": False,
            "return_code": None,
            "summary": None,
            "error": None,
        },
        "commands": {
            "smoke": "python3 scripts/run_renderer_backend_smoke.py " + " ".join(
                f"\"{arg}\"" if " " in arg else arg for arg in smoke_args
            ),
            "rerun_smoke": (
                "python3 scripts/run_renderer_backend_smoke.py "
                f"--config {shlex.quote(str(workflow_smoke_config_path))} "
                f"--backend {backend} "
                f"--output-dir {shlex.quote(str(smoke_output_dir))}"
            )
            if planned_smoke_config_ready
            else None,
            "acquire": (
                "python3 scripts/acquire_renderer_backend_package.py "
                f"--backend {backend} --setup-summary {active_setup_summary_path}"
            ),
            "linux_handoff": linux_handoff.get("script_command"),
            "linux_handoff_helper": linux_handoff.get("helper_command"),
            "linux_handoff_docker": linux_handoff.get("docker_script_command"),
            "linux_handoff_docker_helper": linux_handoff.get("docker_helper_command"),
            "linux_handoff_pack": linux_handoff.get("pack_command"),
            "linux_handoff_unpack": linux_handoff.get("unpack_command"),
        },
        "artifacts": {
            "summary_path": str(summary_path),
            "env_path": str(env_path),
            "report_path": str(report_path),
            "next_step_script_path": str(workflow_root / "renderer_backend_workflow_next_step.sh"),
            "smoke_config_path": str(workflow_smoke_config_path),
            "rerun_smoke_script_path": str(workflow_rerun_smoke_script_path),
            "linux_handoff_config_path": linux_handoff["artifacts"]["handoff_config_path"],
            "linux_handoff_env_path": linux_handoff["artifacts"]["handoff_env_path"],
            "linux_handoff_script_path": linux_handoff["artifacts"]["handoff_script_path"],
            "linux_handoff_docker_script_path": str(
                workflow_root / "renderer_backend_workflow_linux_handoff_docker.sh"
            ),
            "linux_handoff_transfer_manifest_path": linux_handoff["artifacts"]["handoff_transfer_manifest_path"],
            "linux_handoff_pack_script_path": linux_handoff["artifacts"]["handoff_pack_script_path"],
            "linux_handoff_unpack_script_path": str(
                workflow_root / "renderer_backend_workflow_linux_handoff_unpack.sh"
            ),
            "linux_handoff_bundle_manifest_path": str(
                workflow_root / "renderer_backend_workflow_linux_handoff_bundle_manifest.json"
            ),
            "linux_handoff_verification_manifest_path": str(
                workflow_root / "renderer_backend_workflow_linux_handoff_verification.json"
            ),
            "refreshed_setup_summary_path": (
                str(refreshed_setup_summary_path) if refreshed_setup_summary_path is not None else None
            ),
            "refreshed_setup_env_path": (
                str(refreshed_setup_env_path) if refreshed_setup_env_path is not None else None
            ),
        },
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    workflow_root = (
        _resolve_path(args.output_root)
        if args.output_root is not None
        else (repo_root / "artifacts" / "renderer_backend_workflow" / args.backend).resolve()
    )
    summary = build_renderer_backend_workflow(
        backend=args.backend,
        repo_root=repo_root,
        workflow_root=workflow_root,
        setup_summary_path=_resolve_path(args.setup_summary) if args.setup_summary is not None else None,
        config_path=_resolve_path(args.config) if args.config is not None else None,
        backend_bin_override=args.backend_bin,
        renderer_map_override=args.renderer_map,
        auto_acquire=args.auto_acquire,
        download_url=args.download_url,
        download_name=args.download_name,
        download_dir=_resolve_path(args.download_dir) if args.download_dir is not None else None,
        overwrite_download=args.overwrite_download,
        dry_run=args.dry_run,
        option_overrides=list(args.set_option),
        pack_linux_handoff=args.pack_linux_handoff,
        verify_linux_handoff_bundle=args.verify_linux_handoff_bundle,
        run_linux_handoff_docker=args.run_linux_handoff_docker,
        docker_handoff_execute=args.docker_handoff_execute,
        docker_binary=args.docker_binary,
        docker_image=args.docker_image,
        docker_container_workspace=args.docker_container_workspace,
    )
    summary_path = Path(summary["artifacts"]["summary_path"])
    env_path = Path(summary["artifacts"]["env_path"])
    report_path = Path(summary["artifacts"]["report_path"])
    next_step_script_path = Path(summary["artifacts"]["next_step_script_path"])
    workflow_smoke_config_path = Path(summary["artifacts"]["smoke_config_path"])
    workflow_rerun_smoke_script_path = Path(summary["artifacts"]["rerun_smoke_script_path"])
    linux_handoff_config_path = Path(summary["artifacts"]["linux_handoff_config_path"])
    linux_handoff_env_path = Path(summary["artifacts"]["linux_handoff_env_path"])
    linux_handoff_script_path = Path(summary["artifacts"]["linux_handoff_script_path"])
    linux_handoff_docker_script_path = Path(summary["artifacts"]["linux_handoff_docker_script_path"])
    linux_handoff_transfer_manifest_path = Path(summary["artifacts"]["linux_handoff_transfer_manifest_path"])
    linux_handoff_pack_script_path = Path(summary["artifacts"]["linux_handoff_pack_script_path"])
    linux_handoff_unpack_script_path = Path(summary["artifacts"]["linux_handoff_unpack_script_path"])
    linux_handoff_bundle_manifest_path = Path(summary["artifacts"]["linux_handoff_bundle_manifest_path"])
    linux_handoff_verification_manifest_path = Path(summary["artifacts"]["linux_handoff_verification_manifest_path"])
    _write_json(workflow_smoke_config_path, summary["smoke"]["planned_effective_config"])
    _write_json(linux_handoff_config_path, summary["linux_handoff"]["config"])
    _write_text(env_path, _render_workflow_env_file(summary))
    _write_text(linux_handoff_env_path, summary["linux_handoff"]["env_text"])
    _write_executable_text(
        linux_handoff_script_path,
        summary["linux_handoff"]["script_text"],
    )
    _write_executable_text(
        linux_handoff_docker_script_path,
        summary["linux_handoff"]["docker_script_text"],
    )
    _write_executable_text(
        linux_handoff_pack_script_path,
        summary["linux_handoff"]["pack_script_text"],
    )
    _write_executable_text(
        linux_handoff_unpack_script_path,
        summary["linux_handoff"]["unpack_script_text"],
    )
    summary["linux_handoff"]["transfer_manifest"] = _finalize_linux_handoff_transfer_manifest(
        summary["linux_handoff"]["transfer_manifest"]
    )
    summary["linux_handoff"]["transfer_manifest"]["artifacts"] = {
        "transfer_manifest_path": str(linux_handoff_transfer_manifest_path),
        "bundle_manifest_path": str(linux_handoff_bundle_manifest_path),
    }
    _write_json(linux_handoff_transfer_manifest_path, summary["linux_handoff"]["transfer_manifest"])
    bundle_summary = summary["linux_handoff"].get("bundle", {})
    if isinstance(bundle_summary, dict):
        bundle_path_value = bundle_summary.get("bundle_path")
        if isinstance(bundle_path_value, str) and bundle_summary.get("pack_requested") and summary["linux_handoff"].get("ready"):
            bundle_result = _pack_linux_handoff_bundle(
                transfer_manifest=summary["linux_handoff"]["transfer_manifest"],
                bundle_path=_resolve_path(bundle_path_value),
                bundle_manifest_path=linux_handoff_bundle_manifest_path,
            )
            bundle_summary.update(bundle_result)
            bundle_summary["bundle_generated"] = True
        if isinstance(bundle_path_value, str) and bundle_summary.get("verify_requested"):
            bundle_path = _resolve_path(bundle_path_value)
            if bundle_path.exists() and linux_handoff_bundle_manifest_path.exists():
                verification_payload = _verify_linux_handoff_bundle(
                    transfer_manifest=summary["linux_handoff"]["transfer_manifest"],
                    bundle_path=bundle_path,
                    bundle_manifest_path=linux_handoff_bundle_manifest_path,
                    extract_root=_resolve_path(bundle_summary["verification_extract_root"]),
                    verification_manifest_path=linux_handoff_verification_manifest_path,
                )
                bundle_summary["bundle_verified"] = bool(verification_payload.get("verified"))
                bundle_summary["verification"] = verification_payload
            else:
                verification_payload = {
                    "verified": False,
                    "message": "Bundle archive or bundle manifest is missing.",
                }
                bundle_summary["verification"] = verification_payload
                _write_json(linux_handoff_verification_manifest_path, verification_payload)
    docker_handoff = summary.get("docker_handoff", {})
    if isinstance(docker_handoff, dict) and docker_handoff.get("requested"):
        preflight = docker_handoff.get("preflight", {})
        if isinstance(preflight, dict) and preflight.get("available") and preflight.get("success") is False:
            docker_handoff["error"] = "Docker handoff preflight probe failed."
            summary["status"] = "HANDOFF_DOCKER_PREFLIGHT_FAILED"
            summary["success"] = False
        else:
            bundle_path_value = None
            if isinstance(bundle_summary, dict):
                bundle_path_value = bundle_summary.get("bundle_path")
            if (
                isinstance(bundle_path_value, str)
                and summary["linux_handoff"].get("ready")
                and not _resolve_path(bundle_path_value).exists()
            ):
                bundle_result = _pack_linux_handoff_bundle(
                    transfer_manifest=summary["linux_handoff"]["transfer_manifest"],
                    bundle_path=_resolve_path(bundle_path_value),
                    bundle_manifest_path=linux_handoff_bundle_manifest_path,
                )
                if isinstance(bundle_summary, dict):
                    bundle_summary.update(bundle_result)
                    bundle_summary["bundle_generated"] = True
            if not summary["linux_handoff"].get("ready"):
                docker_handoff["error"] = "Linux handoff is not ready."
            elif not isinstance(bundle_path_value, str):
                docker_handoff["error"] = "Linux handoff bundle path is not available."
            else:
                bundle_path = _resolve_path(bundle_path_value)
                if not bundle_path.exists():
                    docker_handoff["error"] = f"Linux handoff bundle is missing: {bundle_path}"
                else:
                    docker_result = run_renderer_backend_linux_handoff_in_docker(
                        bundle_path=bundle_path,
                        transfer_manifest_path=linux_handoff_transfer_manifest_path,
                        bundle_manifest_path=(
                            linux_handoff_bundle_manifest_path
                            if linux_handoff_bundle_manifest_path.exists()
                            else None
                        ),
                        repo_root=repo_root,
                        output_root=_resolve_path(docker_handoff["output_root"]),
                        summary_path=_resolve_path(docker_handoff["summary_path"]),
                        docker_binary=str(docker_handoff["docker_binary"]),
                        docker_image=str(docker_handoff["docker_image"]),
                        container_workspace=str(docker_handoff["container_workspace"]),
                        skip_run=bool(docker_handoff["skip_run"]),
                    )
                    docker_handoff["executed"] = True
                    docker_handoff["return_code"] = docker_result.get("return_code")
                    docker_handoff["summary"] = docker_result
                    docker_handoff["error"] = docker_result.get("launch_error")
                    if docker_result.get("return_code") == 0:
                        summary["status"] = (
                            "HANDOFF_DOCKER_VERIFIED"
                            if docker_handoff.get("skip_run")
                            else "HANDOFF_DOCKER_EXECUTED"
                        )
                        summary["success"] = True
                    else:
                        summary["status"] = "HANDOFF_DOCKER_FAILED"
                        summary["success"] = False
    _write_json(summary_path, summary)
    _write_text(report_path, _render_workflow_markdown_report(summary, summary_path))
    _write_executable_text(next_step_script_path, _render_next_step_script(summary))
    _write_executable_text(
        workflow_rerun_smoke_script_path,
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "# Generated by renderer_backend_workflow.py",
                "",
                f"source {shlex.quote(str(env_path))}",
                summary["commands"]["rerun_smoke"]
                if summary["commands"]["rerun_smoke"]
                else "echo 'Workflow smoke config is not ready yet. Check renderer_backend_workflow_summary.json for blockers.'\nexit 1",
                "",
            ]
        ),
    )
    print(json.dumps(summary, indent=2))
    docker_handoff = summary.get("docker_handoff", {})
    if isinstance(docker_handoff, dict) and docker_handoff.get("requested"):
        if docker_handoff.get("executed"):
            return_code = docker_handoff.get("return_code")
            if isinstance(return_code, int):
                return return_code
        if docker_handoff.get("error"):
            return 1
    return 0 if summary["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
