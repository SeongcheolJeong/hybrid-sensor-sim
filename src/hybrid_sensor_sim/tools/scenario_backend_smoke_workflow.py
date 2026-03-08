from __future__ import annotations

import argparse
import contextlib
import io
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.scenarios import LOG_SCENE_SCHEMA_VERSION_V0, SCENARIO_SCHEMA_VERSION_V0
from hybrid_sensor_sim.tools.renderer_backend_smoke import main as renderer_backend_smoke_main
from hybrid_sensor_sim.tools.renderer_backend_workflow import (
    run_renderer_backend_workflow,
)
from hybrid_sensor_sim.tools.renderer_backend_local_setup import (
    _inspect_executable_host_compatibility,
)
from hybrid_sensor_sim.tools.autonomy_e2e_history_guard import (
    build_autonomy_e2e_history_guard_report,
)
from hybrid_sensor_sim.tools.autoware_pipeline_bridge import (
    run_autoware_pipeline_bridge,
)
from hybrid_sensor_sim.tools.scenario_runtime_bridge import (
    DEFAULT_LANE_SPACING_M,
    write_smoke_ready_scenario,
)


SCENARIO_BACKEND_SMOKE_WORKFLOW_REPORT_SCHEMA_VERSION_V0 = "scenario_backend_smoke_workflow_report_v0"
SCENARIO_VARIANT_WORKFLOW_REPORT_SCHEMA_VERSION_V0 = "scenario_variant_workflow_report_v0"
SCENARIO_BATCH_WORKFLOW_REPORT_SCHEMA_VERSION_V0 = "scenario_batch_workflow_report_v0"
SCENARIO_VARIANT_RUN_REPORT_SCHEMA_VERSION_V0 = "scenario_variant_run_report_v0"
_BACKEND_ENV_VARS = {
    "awsim": ("AWSIM_BIN", "AWSIM_RENDERER_MAP"),
    "carla": ("CARLA_BIN", "CARLA_RENDERER_MAP"),
}
_WORKSPACE_ROOT = "/workspace"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Select a scenario variant, translate it to a smoke-ready scenario, and optionally run renderer backend smoke."
    )
    parser.add_argument("--variant-workflow-report", default="", help="Path to scenario_variant_workflow_report_v0.json")
    parser.add_argument("--batch-workflow-report", default="", help="Path to scenario_batch_workflow_report_v0.json")
    parser.add_argument("--smoke-config", required=True, help="Base renderer backend smoke JSON config")
    parser.add_argument("--backend", choices=("awsim", "carla"), required=True, help="Renderer backend")
    parser.add_argument("--out-root", required=True, help="Workflow output root")
    parser.add_argument(
        "--selection-strategy",
        choices=("first_successful_variant", "worst_logical_scenario", "variant_id"),
        default="",
        help="How to choose the variant to bridge",
    )
    parser.add_argument("--selected-variant-id", default="", help="Variant ID for selection_strategy=variant_id")
    parser.add_argument("--lane-spacing-m", type=float, default=DEFAULT_LANE_SPACING_M, help="Lane center spacing used in smoke scenario translation")
    parser.add_argument("--smoke-output-dir", default="", help="Override smoke output directory")
    parser.add_argument("--setup-summary", default="", help="Optional renderer_backend_local_setup.json used to resolve backend runtime selection")
    parser.add_argument("--backend-workflow-summary", default="", help="Optional renderer_backend_workflow_summary.json used to resolve backend runtime selection")
    parser.add_argument("--backend-bin", default="", help="Forwarded to renderer backend smoke")
    parser.add_argument("--renderer-map", default="", help="Forwarded to renderer backend smoke")
    parser.add_argument("--set-option", action="append", default=[], help="Forwarded to renderer backend smoke")
    parser.add_argument(
        "--renderer-backend-workflow-output-root",
        default="",
        help="Optional output root for staged packaged backend handoff workflow when the selected runtime is host-incompatible",
    )
    parser.add_argument(
        "--pack-linux-handoff",
        action="store_true",
        help="When preparing renderer backend handoff, also build a transfer bundle",
    )
    parser.add_argument(
        "--verify-linux-handoff-bundle",
        action="store_true",
        help="When preparing renderer backend handoff, verify the generated transfer bundle locally",
    )
    parser.add_argument(
        "--run-linux-handoff-docker",
        action="store_true",
        help="When preparing renderer backend handoff, run the Docker handoff helper",
    )
    parser.add_argument(
        "--docker-handoff-execute",
        action="store_true",
        help="When running the Docker handoff helper, execute the extracted handoff script instead of verify-only mode",
    )
    parser.add_argument(
        "--docker-binary",
        default="docker",
        help="Docker CLI binary used for renderer backend handoff",
    )
    parser.add_argument(
        "--docker-image",
        default="python:3.11-slim",
        help="Linux Docker image used for renderer backend handoff",
    )
    parser.add_argument(
        "--docker-platform",
        default="",
        help="Optional Docker platform (for example linux/amd64) used for renderer backend handoff",
    )
    parser.add_argument(
        "--docker-container-workspace",
        default="/workspace",
        help="Workspace mount path inside the Docker container for renderer backend handoff",
    )
    parser.add_argument(
        "--refresh-docker-handoff-preflight",
        action="store_true",
        help="Refresh the Docker handoff preflight probe before renderer backend handoff execution",
    )
    parser.add_argument("--skip-smoke", action="store_true", help="Only select and translate; do not execute renderer backend smoke")
    parser.add_argument(
        "--skip-autoware-bridge",
        action="store_true",
        help="Do not build Autoware-facing sensor/data manifests from backend smoke artifacts",
    )
    parser.add_argument(
        "--autoware-base-frame",
        default="base_link",
        help="Base frame ID for generated Autoware frame tree",
    )
    parser.add_argument(
        "--autoware-strict",
        action="store_true",
        help="Fail Autoware bridge if required sensor outputs are missing",
    )
    parser.add_argument(
        "--run-history-guard",
        action="store_true",
        help="Run Autonomy-E2E provenance guard against the canonical baseline after workflow execution",
    )
    parser.add_argument(
        "--history-guard-metadata-root",
        default="",
        help="Override metadata root for Autonomy-E2E provenance guard",
    )
    parser.add_argument(
        "--history-guard-current-repo-root",
        default="",
        help="Override repo root for Autonomy-E2E provenance guard",
    )
    parser.add_argument(
        "--history-guard-compare-ref",
        default="origin/main",
        help="Git compare ref used by Autonomy-E2E provenance guard",
    )
    parser.add_argument(
        "--history-guard-include-untracked",
        action="store_true",
        help="Include untracked files in Autonomy-E2E provenance guard evaluation",
    )
    return parser.parse_args(argv)


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _optional_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _selection_value(selection: dict[str, Any], key: str) -> str | None:
    value = selection.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _resolve_nested_handoff_smoke_artifacts(
    renderer_backend_workflow_summary: dict[str, Any] | None,
) -> dict[str, Path]:
    if not isinstance(renderer_backend_workflow_summary, dict):
        return {}
    summary_path = str(
        renderer_backend_workflow_summary.get("artifacts", {}).get("summary_path", "")
    ).strip()
    if not summary_path:
        return {}
    workflow_root = Path(summary_path).expanduser().resolve().parent
    smoke_root = workflow_root / "linux_handoff" / "smoke_run"
    artifacts = {
        "summary_path": smoke_root / "renderer_backend_smoke_summary.json",
        "markdown_report_path": smoke_root / "renderer_backend_smoke_report.md",
        "html_report_path": smoke_root / "renderer_backend_smoke_report.html",
    }
    return {key: path for key, path in artifacts.items() if path.exists()}


def _resolve_runtime_artifact_path(raw_path: Any, *, repo_root: Path) -> Path | None:
    path_text = str(raw_path or "").strip()
    if not path_text:
        return None
    candidate = Path(path_text).expanduser()
    if candidate.exists():
        return candidate.resolve()
    if path_text.startswith(f"{_WORKSPACE_ROOT}/"):
        translated = repo_root / path_text[len(_WORKSPACE_ROOT) + 1 :]
        if translated.exists():
            return translated.resolve()
        return translated.resolve()
    if candidate.is_absolute():
        return candidate.resolve()
    return (repo_root / candidate).resolve()


def _extract_backend_runtime_diagnostics(
    smoke_summary: dict[str, Any],
    *,
    repo_root: Path,
) -> dict[str, Any]:
    artifacts = dict(smoke_summary.get("artifacts", {}))
    stdout_path = _resolve_runtime_artifact_path(
        artifacts.get("backend_runner_stdout") or artifacts.get("renderer_stdout"),
        repo_root=repo_root,
    )
    stderr_path = _resolve_runtime_artifact_path(
        artifacts.get("backend_runner_stderr") or artifacts.get("renderer_stderr"),
        repo_root=repo_root,
    )
    stdout_text = ""
    stderr_text = ""
    if stdout_path is not None and stdout_path.exists():
        stdout_text = stdout_path.read_text(encoding="utf-8", errors="replace")
    if stderr_path is not None and stderr_path.exists():
        stderr_text = stderr_path.read_text(encoding="utf-8", errors="replace")
    combined_text = "\n".join(part for part in (stdout_text, stderr_text) if part)

    failed_plugin_paths: list[str] = []
    for line in combined_text.splitlines():
        if line.startswith("Failed to open plugin: "):
            failed_plugin_paths.append(line.split(": ", 1)[1].strip())
    failed_plugin_paths = list(dict.fromkeys(failed_plugin_paths))
    failed_plugins = [Path(path).name for path in failed_plugin_paths]

    missing_shared_libraries: list[str] = []
    for match in re.finditer(r"([A-Za-z0-9_./+-]+\.so(?:\.[0-9]+)*) => not found", combined_text):
        missing_shared_libraries.append(match.group(1))
    for match in re.finditer(
        r"error while loading shared libraries: ([A-Za-z0-9_./+-]+\.so(?:\.[0-9]+)*):",
        combined_text,
    ):
        missing_shared_libraries.append(match.group(1))
    missing_shared_libraries = list(dict.fromkeys(missing_shared_libraries))

    crash_signatures: list[str] = []
    if "Forcing GfxDevice: Null" in combined_text:
        crash_signatures.append("NULL_GFX_DEVICE")
    if "* Assertion: should not be reached at tramp-amd64.c:641" in combined_text:
        crash_signatures.append("MONO_TRAMP_AMD64_ASSERT")
    if "monoeg_assert_abort" in combined_text or "mono_assertion_message_unreachable" in combined_text:
        crash_signatures.append("MONO_ASSERT_ABORT")
    if "Caught fatal signal - signo:6" in combined_text or "signal 6" in combined_text:
        crash_signatures.append("SIGABRT")
    if failed_plugins:
        crash_signatures.append("PLUGIN_LOAD_FAILURES")
    crash_signatures = list(dict.fromkeys(crash_signatures))

    runtime_exit_code = None
    if isinstance(smoke_summary.get("runner_smoke"), dict):
        runtime_exit_code = smoke_summary["runner_smoke"].get("return_code")
    if runtime_exit_code is None and isinstance(smoke_summary.get("run"), dict):
        runtime_exit_code = smoke_summary["run"].get("return_code")

    return {
        "backend_runtime_exit_code": runtime_exit_code,
        "backend_runtime_stdout_path": str(stdout_path) if stdout_path is not None else None,
        "backend_runtime_stderr_path": str(stderr_path) if stderr_path is not None else None,
        "backend_runtime_failed_plugin_count": len(failed_plugins),
        "backend_runtime_failed_plugins": failed_plugins,
        "backend_runtime_failed_plugin_paths": failed_plugin_paths,
        "backend_runtime_missing_shared_libraries": missing_shared_libraries,
        "backend_runtime_crash_signatures": crash_signatures,
    }


def _load_variant_workflow_report(path: Path) -> dict[str, Any]:
    payload = _load_json_object(path)
    schema_version = str(payload.get("scenario_variant_workflow_report_schema_version", "")).strip()
    if schema_version != SCENARIO_VARIANT_WORKFLOW_REPORT_SCHEMA_VERSION_V0:
        raise ValueError(
            "scenario_variant_workflow_report_schema_version must be "
            f"{SCENARIO_VARIANT_WORKFLOW_REPORT_SCHEMA_VERSION_V0}"
        )
    return payload


def _load_batch_workflow_report(path: Path) -> dict[str, Any]:
    payload = _load_json_object(path)
    schema_version = str(payload.get("scenario_batch_workflow_report_schema_version", "")).strip()
    if schema_version != SCENARIO_BATCH_WORKFLOW_REPORT_SCHEMA_VERSION_V0:
        raise ValueError(
            "scenario_batch_workflow_report_schema_version must be "
            f"{SCENARIO_BATCH_WORKFLOW_REPORT_SCHEMA_VERSION_V0}"
        )
    return payload


def _load_variant_run_report(path: Path) -> dict[str, Any]:
    payload = _load_json_object(path)
    schema_version = str(payload.get("scenario_variant_run_report_schema_version", "")).strip()
    if schema_version != SCENARIO_VARIANT_RUN_REPORT_SCHEMA_VERSION_V0:
        raise ValueError(
            "scenario_variant_run_report_schema_version must be "
            f"{SCENARIO_VARIANT_RUN_REPORT_SCHEMA_VERSION_V0}"
        )
    variant_runs = payload.get("variant_runs")
    if not isinstance(variant_runs, list):
        raise ValueError("scenario variant run report missing variant_runs")
    return payload


def _resolve_reports(
    *,
    variant_workflow_report_path: str,
    batch_workflow_report_path: str,
) -> tuple[str, Path, dict[str, Any], Path, dict[str, Any]]:
    variant_path_text = str(variant_workflow_report_path).strip()
    batch_path_text = str(batch_workflow_report_path).strip()
    if bool(variant_path_text) == bool(batch_path_text):
        raise ValueError("provide exactly one of --variant-workflow-report or --batch-workflow-report")

    if variant_path_text:
        workflow_report_path = Path(variant_path_text).resolve()
        workflow_report = _load_variant_workflow_report(workflow_report_path)
        artifacts = workflow_report.get("artifacts", {})
        variant_run_report_path = Path(str(artifacts.get("variant_run_report_path", "")).strip()).resolve()
        variant_run_report = _load_variant_run_report(variant_run_report_path)
        return (
            "variant_workflow_report",
            workflow_report_path,
            workflow_report,
            variant_run_report_path,
            variant_run_report,
        )

    workflow_report_path = Path(batch_path_text).resolve()
    workflow_report = _load_batch_workflow_report(workflow_report_path)
    artifacts = workflow_report.get("artifacts", {})
    variant_run_report_path = Path(str(artifacts.get("variant_run_report_path", "")).strip()).resolve()
    if not str(variant_run_report_path):
        raise ValueError("batch workflow report missing artifacts.variant_run_report_path")
    variant_run_report = _load_variant_run_report(variant_run_report_path)
    return (
        "batch_workflow_report",
        workflow_report_path,
        workflow_report,
        variant_run_report_path,
        variant_run_report,
    )


def _supported_variant_run_entry(entry: dict[str, Any]) -> bool:
    payload_kind = str(entry.get("rendered_payload_kind", "")).strip()
    if payload_kind not in {LOG_SCENE_SCHEMA_VERSION_V0, SCENARIO_SCHEMA_VERSION_V0}:
        return False
    rendered_payload_path = str(entry.get("rendered_payload_path", "")).strip()
    replay_scenario_path = str(entry.get("replay_scenario_path", "")).strip()
    return bool(rendered_payload_path or replay_scenario_path)


def _select_variant_run_entry(
    *,
    report_kind: str,
    workflow_report: dict[str, Any],
    variant_run_report: dict[str, Any],
    selection_strategy: str,
    selected_variant_id: str,
) -> dict[str, Any]:
    variant_runs = [
        dict(item)
        for item in variant_run_report.get("variant_runs", [])
        if isinstance(item, dict) and _supported_variant_run_entry(item)
    ]
    if not variant_runs:
        raise ValueError("no bridgeable variant runs found")

    effective_strategy = selection_strategy.strip()
    if not effective_strategy:
        effective_strategy = "worst_logical_scenario" if report_kind == "batch_workflow_report" else "first_successful_variant"

    if effective_strategy == "first_successful_variant":
        for entry in variant_runs:
            if str(entry.get("execution_status", "")).strip() == "SUCCEEDED":
                return entry
        return variant_runs[0]

    if effective_strategy == "variant_id":
        variant_id = str(selected_variant_id).strip()
        if not variant_id:
            raise ValueError("selection_strategy=variant_id requires --selected-variant-id")
        for entry in variant_runs:
            if str(entry.get("variant_id", "")).strip() == variant_id:
                return entry
        raise ValueError(f"variant_id not found in variant run report: {variant_id}")

    if effective_strategy == "worst_logical_scenario":
        if report_kind != "batch_workflow_report":
            raise ValueError("selection_strategy=worst_logical_scenario requires --batch-workflow-report")
        status_summary = workflow_report.get("status_summary", {})
        worst_row = status_summary.get("worst_logical_scenario_row", {})
        logical_scenario_id = str(worst_row.get("logical_scenario_id", "")).strip()
        if not logical_scenario_id:
            raise ValueError("batch workflow report missing status_summary.worst_logical_scenario_row.logical_scenario_id")
        matching = [
            entry
            for entry in variant_runs
            if str(entry.get("logical_scenario_id", "")).strip() == logical_scenario_id
        ]
        if not matching:
            raise ValueError(
                f"no bridgeable variant found for worst logical scenario: {logical_scenario_id}"
            )
        for entry in matching:
            if str(entry.get("execution_status", "")).strip() == "SUCCEEDED":
                return entry
        return matching[0]

    raise ValueError(f"unsupported selection strategy: {effective_strategy}")


def _select_bridge_source(entry: dict[str, Any]) -> tuple[str, Path, str]:
    replay_scenario_path = _optional_text(entry.get("replay_scenario_path"))
    if replay_scenario_path:
        return (
            SCENARIO_SCHEMA_VERSION_V0,
            Path(replay_scenario_path).resolve(),
            "replay_scenario_path",
        )
    rendered_payload_kind = _optional_text(entry.get("rendered_payload_kind"))
    rendered_payload_path = _optional_text(entry.get("rendered_payload_path"))
    if rendered_payload_kind in {LOG_SCENE_SCHEMA_VERSION_V0, SCENARIO_SCHEMA_VERSION_V0} and rendered_payload_path:
        return (
            rendered_payload_kind,
            Path(rendered_payload_path).resolve(),
            "rendered_payload_path",
        )
    raise ValueError("selected variant run entry does not expose a bridgeable source payload")


def _build_selection_payload(
    *,
    report_kind: str,
    source_report_path: Path,
    variant_run_report_path: Path,
    selection_strategy: str,
    selected_entry: dict[str, Any],
    bridge_source_kind: str,
    bridge_source_path: Path,
    bridge_source_origin: str,
) -> dict[str, Any]:
    return {
        "report_kind": report_kind,
        "source_report_path": str(source_report_path),
        "variant_run_report_path": str(variant_run_report_path),
        "selection_strategy": selection_strategy,
        "variant_id": _optional_text(selected_entry.get("variant_id")) or None,
        "logical_scenario_id": _optional_text(selected_entry.get("logical_scenario_id")) or None,
        "execution_status": _optional_text(selected_entry.get("execution_status")) or None,
        "object_sim_status": _optional_text(selected_entry.get("object_sim_status")) or None,
        "termination_reason": _optional_text(selected_entry.get("termination_reason")) or None,
        "rendered_payload_kind": _optional_text(selected_entry.get("rendered_payload_kind")) or None,
        "execution_path": _optional_text(selected_entry.get("execution_path")) or None,
        "variant_run_dir": _optional_text(selected_entry.get("variant_run_dir")) or None,
        "rendered_payload_path": _optional_text(selected_entry.get("rendered_payload_path")) or None,
        "replay_scenario_path": _optional_text(selected_entry.get("replay_scenario_path")) or None,
        "bridge_source_kind": bridge_source_kind,
        "bridge_source_path": str(bridge_source_path),
        "bridge_source_origin": bridge_source_origin,
    }


def _build_smoke_input_config(
    *,
    base_config_path: Path,
    smoke_scenario_path: Path,
    smoke_output_dir: Path,
) -> dict[str, Any]:
    payload = _load_json_object(base_config_path)
    payload["scenario_path"] = str(smoke_scenario_path.resolve())
    payload["output_dir"] = str(smoke_output_dir.resolve())
    return payload


def _discover_runtime_selection_paths(
    *,
    backend: str,
    repo_root: Path,
) -> dict[str, str | None]:
    canonical_setup_summary = (
        repo_root / "artifacts" / "renderer_backend_local_setup" / "renderer_backend_local_setup.json"
    ).resolve()
    canonical_backend_workflow_summary = (
        repo_root
        / "artifacts"
        / "renderer_backend_workflow"
        / backend
        / "renderer_backend_workflow_summary.json"
    ).resolve()
    canonical_package_stage_summary = (
        repo_root
        / "third_party"
        / "runtime_backends"
        / backend
        / "renderer_backend_package_stage.json"
    ).resolve()
    canonical_package_acquire_summary = (
        repo_root
        / "third_party"
        / "runtime_backends"
        / backend
        / "renderer_backend_package_acquire.json"
    ).resolve()
    return {
        "setup_summary_path": (
            str(canonical_setup_summary) if canonical_setup_summary.exists() else None
        ),
        "backend_workflow_summary_path": (
            str(canonical_backend_workflow_summary)
            if canonical_backend_workflow_summary.exists()
            else None
        ),
        "package_stage_summary_path": (
            str(canonical_package_stage_summary)
            if canonical_package_stage_summary.exists()
            else None
        ),
        "package_acquire_summary_path": (
            str(canonical_package_acquire_summary)
            if canonical_package_acquire_summary.exists()
            else None
        ),
    }


def _resolve_runtime_selection(
    *,
    backend: str,
    explicit_backend_bin: str,
    explicit_renderer_map: str,
    setup_summary_path: str,
    backend_workflow_summary_path: str,
) -> dict[str, Any]:
    if backend not in _BACKEND_ENV_VARS:
        raise ValueError(f"unsupported backend: {backend}")
    backend_env_var, map_env_var = _BACKEND_ENV_VARS[backend]
    resolved_backend_bin = _optional_text(explicit_backend_bin)
    resolved_renderer_map = _optional_text(explicit_renderer_map)
    backend_bin_source = "explicit" if resolved_backend_bin else "unresolved"
    renderer_map_source = "explicit" if resolved_renderer_map else "unresolved"
    setup_summary_text = _optional_text(setup_summary_path)
    backend_workflow_summary_text = _optional_text(backend_workflow_summary_path)
    package_stage_summary_text = ""
    package_acquire_summary_text = ""
    setup_summary_path_source = "explicit" if setup_summary_text else "unresolved"
    backend_workflow_summary_path_source = (
        "explicit" if backend_workflow_summary_text else "unresolved"
    )
    package_stage_summary_path_source = "unresolved"
    package_acquire_summary_path_source = "unresolved"

    if (
        not setup_summary_text
        or not backend_workflow_summary_text
        or not package_stage_summary_text
        or not package_acquire_summary_text
    ):
        discovered_paths = _discover_runtime_selection_paths(
            backend=backend,
            repo_root=Path(__file__).resolve().parents[3],
        )
        if not setup_summary_text:
            discovered_setup_summary = _optional_text(discovered_paths.get("setup_summary_path"))
            if discovered_setup_summary:
                setup_summary_text = discovered_setup_summary
                setup_summary_path_source = "auto"
        if not backend_workflow_summary_text:
            discovered_backend_workflow_summary = _optional_text(
                discovered_paths.get("backend_workflow_summary_path")
            )
            if discovered_backend_workflow_summary:
                backend_workflow_summary_text = discovered_backend_workflow_summary
                backend_workflow_summary_path_source = "auto"
        if not package_stage_summary_text:
            discovered_package_stage_summary = _optional_text(
                discovered_paths.get("package_stage_summary_path")
            )
            if discovered_package_stage_summary:
                package_stage_summary_text = discovered_package_stage_summary
                package_stage_summary_path_source = "auto"
        if not package_acquire_summary_text:
            discovered_package_acquire_summary = _optional_text(
                discovered_paths.get("package_acquire_summary_path")
            )
            if discovered_package_acquire_summary:
                package_acquire_summary_text = discovered_package_acquire_summary
                package_acquire_summary_path_source = "auto"

    if setup_summary_text and not resolved_backend_bin:
        setup_payload = _load_json_object(Path(setup_summary_text).resolve())
        candidate = _selection_value(dict(setup_payload.get("selection", {})), backend_env_var)
        if candidate:
            resolved_backend_bin = candidate
            backend_bin_source = "setup_summary"
    if setup_summary_text and not resolved_renderer_map:
        setup_payload = _load_json_object(Path(setup_summary_text).resolve())
        candidate = _selection_value(dict(setup_payload.get("selection", {})), map_env_var)
        if candidate:
            resolved_renderer_map = candidate
            renderer_map_source = "setup_summary"

    if backend_workflow_summary_text and not resolved_backend_bin:
        workflow_payload = _load_json_object(Path(backend_workflow_summary_text).resolve())
        candidate = _selection_value(dict(workflow_payload.get("final_selection", {})), backend_env_var)
        if candidate:
            resolved_backend_bin = candidate
            backend_bin_source = "backend_workflow_summary"
    if backend_workflow_summary_text and not resolved_renderer_map:
        workflow_payload = _load_json_object(Path(backend_workflow_summary_text).resolve())
        candidate = _selection_value(dict(workflow_payload.get("final_selection", {})), map_env_var)
        if candidate:
            resolved_renderer_map = candidate
            renderer_map_source = "backend_workflow_summary"

    if package_stage_summary_text and not resolved_backend_bin:
        package_stage_payload = _load_json_object(Path(package_stage_summary_text).resolve())
        candidate = _selection_value(
            dict(package_stage_payload.get("selection", {})), backend_env_var
        )
        if candidate:
            resolved_backend_bin = candidate
            backend_bin_source = "package_stage_summary"
    if package_stage_summary_text and not resolved_renderer_map:
        package_stage_payload = _load_json_object(Path(package_stage_summary_text).resolve())
        candidate = _selection_value(
            dict(package_stage_payload.get("selection", {})), map_env_var
        )
        if candidate:
            resolved_renderer_map = candidate
            renderer_map_source = "package_stage_summary"

    if package_acquire_summary_text and not resolved_backend_bin:
        package_acquire_payload = _load_json_object(Path(package_acquire_summary_text).resolve())
        candidate = _selection_value(
            dict(dict(package_acquire_payload.get("stage", {})).get("selection", {})),
            backend_env_var,
        )
        if candidate:
            resolved_backend_bin = candidate
            backend_bin_source = "package_acquire_summary"
    if package_acquire_summary_text and not resolved_renderer_map:
        package_acquire_payload = _load_json_object(Path(package_acquire_summary_text).resolve())
        candidate = _selection_value(
            dict(dict(package_acquire_payload.get("stage", {})).get("selection", {})),
            map_env_var,
        )
        if candidate:
            resolved_renderer_map = candidate
            renderer_map_source = "package_acquire_summary"

    return {
        "backend_env_var": backend_env_var,
        "map_env_var": map_env_var,
        "setup_summary_path": str(Path(setup_summary_text).resolve()) if setup_summary_text else None,
        "setup_summary_path_source": setup_summary_path_source,
        "backend_workflow_summary_path": (
            str(Path(backend_workflow_summary_text).resolve()) if backend_workflow_summary_text else None
        ),
        "backend_workflow_summary_path_source": backend_workflow_summary_path_source,
        "package_stage_summary_path": (
            str(Path(package_stage_summary_text).resolve()) if package_stage_summary_text else None
        ),
        "package_stage_summary_path_source": package_stage_summary_path_source,
        "package_acquire_summary_path": (
            str(Path(package_acquire_summary_text).resolve()) if package_acquire_summary_text else None
        ),
        "package_acquire_summary_path_source": package_acquire_summary_path_source,
        "backend_bin": resolved_backend_bin or None,
        "renderer_map": resolved_renderer_map or None,
        "backend_bin_source": backend_bin_source,
        "renderer_map_source": renderer_map_source,
    }


def run_scenario_backend_smoke_workflow(
    *,
    variant_workflow_report_path: str,
    batch_workflow_report_path: str,
    smoke_config_path: Path,
    backend: str,
    out_root: Path,
    selection_strategy: str,
    selected_variant_id: str,
    lane_spacing_m: float,
    smoke_output_dir: str,
    setup_summary_path: str,
    backend_workflow_summary_path: str,
    backend_bin: str,
    renderer_map: str,
    option_overrides: list[str],
    renderer_backend_workflow_output_root: str = "",
    pack_linux_handoff: bool = False,
    verify_linux_handoff_bundle: bool = False,
    run_linux_handoff_docker: bool = False,
    docker_handoff_execute: bool = False,
    docker_binary: str = "docker",
    docker_image: str = "python:3.11-slim",
    docker_platform: str | None = None,
    docker_container_workspace: str = "/workspace",
    refresh_docker_handoff_preflight: bool = False,
    skip_smoke: bool = False,
    skip_autoware_bridge: bool = False,
    autoware_base_frame: str = "base_link",
    autoware_strict: bool = False,
    run_history_guard: bool = False,
    history_guard_metadata_root: str | Path | None = None,
    history_guard_current_repo_root: str | Path | None = None,
    history_guard_compare_ref: str = "origin/main",
    history_guard_include_untracked: bool = False,
) -> dict[str, Any]:
    out_root.mkdir(parents=True, exist_ok=True)
    (
        report_kind,
        source_report_path,
        workflow_report,
        variant_run_report_path,
        variant_run_report,
    ) = _resolve_reports(
        variant_workflow_report_path=variant_workflow_report_path,
        batch_workflow_report_path=batch_workflow_report_path,
    )
    selected_entry = _select_variant_run_entry(
        report_kind=report_kind,
        workflow_report=workflow_report,
        variant_run_report=variant_run_report,
        selection_strategy=selection_strategy,
        selected_variant_id=selected_variant_id,
    )
    effective_selection_strategy = selection_strategy.strip() or (
        "worst_logical_scenario" if report_kind == "batch_workflow_report" else "first_successful_variant"
    )
    bridge_source_kind, bridge_source_path, bridge_source_origin = _select_bridge_source(selected_entry)

    bridge_root = out_root / "bridge"
    bridge_result = write_smoke_ready_scenario(
        source_payload_path=bridge_source_path,
        source_payload_kind=bridge_source_kind,
        out_root=bridge_root,
        lane_spacing_m=lane_spacing_m,
    )
    selection_payload = _build_selection_payload(
        report_kind=report_kind,
        source_report_path=source_report_path,
        variant_run_report_path=variant_run_report_path,
        selection_strategy=effective_selection_strategy,
        selected_entry=selected_entry,
        bridge_source_kind=bridge_source_kind,
        bridge_source_path=bridge_source_path,
        bridge_source_origin=bridge_source_origin,
    )
    selection_path = out_root / "scenario_backend_smoke_selection.json"
    _write_json(selection_path, selection_payload)

    smoke_output_root = (
        Path(str(smoke_output_dir).strip()).expanduser().resolve()
        if str(smoke_output_dir).strip()
        else (out_root / "smoke_run").resolve()
    )
    smoke_input_config = _build_smoke_input_config(
        base_config_path=smoke_config_path.resolve(),
        smoke_scenario_path=Path(bridge_result["smoke_scenario_path"]),
        smoke_output_dir=smoke_output_root,
    )
    smoke_input_config_path = out_root / "scenario_backend_smoke_input_config.json"
    _write_json(smoke_input_config_path, smoke_input_config)
    runtime_selection = _resolve_runtime_selection(
        backend=backend,
        explicit_backend_bin=backend_bin,
        explicit_renderer_map=renderer_map,
        setup_summary_path=setup_summary_path,
        backend_workflow_summary_path=backend_workflow_summary_path,
    )
    resolved_backend_bin = _optional_text(runtime_selection.get("backend_bin"))
    resolved_renderer_map = _optional_text(runtime_selection.get("renderer_map"))
    backend_compatibility = (
        _inspect_executable_host_compatibility(Path(resolved_backend_bin))
        if resolved_backend_bin
        else {
            "host_compatible": None,
            "host_compatibility_reason": "",
            "binary_format": None,
            "file_description": "",
            "binary_architectures": [],
            "translation_required": None,
        }
    )
    runtime_selection.update(
        {
            "backend_host_compatible": backend_compatibility.get("host_compatible"),
            "backend_host_compatibility_reason": backend_compatibility.get(
                "host_compatibility_reason"
            )
            or None,
            "backend_binary_format": backend_compatibility.get("binary_format"),
            "backend_binary_architectures": list(
                backend_compatibility.get("binary_architectures", [])
            ),
            "backend_translation_required": backend_compatibility.get(
                "translation_required"
            ),
            "backend_file_description": backend_compatibility.get("file_description") or None,
        }
    )

    smoke_stdout_path = out_root / "scenario_backend_smoke_stdout.log"
    smoke_stderr_path = out_root / "scenario_backend_smoke_stderr.log"
    smoke_summary_path = smoke_output_root / "renderer_backend_smoke_summary.json"
    smoke_markdown_path = smoke_output_root / "renderer_backend_smoke_report.md"
    smoke_html_path = smoke_output_root / "renderer_backend_smoke_report.html"
    smoke_exit_code = None
    smoke_summary = None
    autoware_result = None
    renderer_backend_workflow_summary = None
    status = "BRIDGED_ONLY" if skip_smoke else "SMOKE_FAILED"

    if (
        not skip_smoke
        and resolved_backend_bin
        and backend_compatibility.get("host_compatible") is False
    ):
        repo_root = Path(__file__).resolve().parents[3]
        renderer_backend_workflow_root = (
            Path(str(renderer_backend_workflow_output_root).strip()).expanduser().resolve()
            if str(renderer_backend_workflow_output_root).strip()
            else (out_root / "renderer_backend_workflow").resolve()
        )
        renderer_backend_workflow_summary = run_renderer_backend_workflow(
            backend=backend,
            repo_root=repo_root,
            workflow_root=renderer_backend_workflow_root,
            setup_summary_path=(
                Path(str(runtime_selection.get("setup_summary_path"))).resolve()
                if runtime_selection.get("setup_summary_path")
                else None
            ),
            config_path=smoke_input_config_path.resolve(),
            backend_bin_override=resolved_backend_bin,
            renderer_map_override=resolved_renderer_map or None,
            auto_acquire=False,
            dry_run=True,
            option_overrides=list(option_overrides),
            pack_linux_handoff=bool(pack_linux_handoff),
            verify_linux_handoff_bundle=bool(verify_linux_handoff_bundle),
            run_linux_handoff_docker=bool(run_linux_handoff_docker),
            docker_handoff_execute=bool(docker_handoff_execute),
            docker_binary=docker_binary,
            docker_image=docker_image,
            docker_platform=docker_platform,
            docker_container_workspace=docker_container_workspace,
            refresh_docker_handoff_preflight=bool(refresh_docker_handoff_preflight),
        )
        renderer_workflow_status = str(renderer_backend_workflow_summary.get("status", "")).strip()
        if renderer_workflow_status in {
            "HANDOFF_DOCKER_VERIFIED",
            "HANDOFF_DOCKER_EXECUTED",
            "HANDOFF_DOCKER_OUTPUT_READY",
            "HANDOFF_DOCKER_FAILED",
            "HANDOFF_DOCKER_PREFLIGHT_FAILED",
        }:
            status = renderer_workflow_status
        elif renderer_backend_workflow_summary.get("linux_handoff", {}).get("ready"):
            status = "HANDOFF_READY"
        else:
            status = "SMOKE_FAILED"
        nested_smoke_artifacts = _resolve_nested_handoff_smoke_artifacts(
            renderer_backend_workflow_summary
        )
        nested_summary_path = nested_smoke_artifacts.get("summary_path")
        if nested_summary_path is not None:
            smoke_summary_path = nested_summary_path
            smoke_markdown_path = nested_smoke_artifacts.get("markdown_report_path", smoke_markdown_path)
            smoke_html_path = nested_smoke_artifacts.get("html_report_path", smoke_html_path)
            smoke_summary = _load_json_object(smoke_summary_path)
    elif not skip_smoke:
        smoke_argv = [
            "--config",
            str(smoke_input_config_path),
            "--backend",
            backend,
            "--output-dir",
            str(smoke_output_root),
        ]
        if resolved_backend_bin:
            smoke_argv.extend(["--backend-bin", resolved_backend_bin])
        if resolved_renderer_map:
            smoke_argv.extend(["--renderer-map", resolved_renderer_map])
        for override in option_overrides:
            smoke_argv.extend(["--set-option", override])

        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()
        with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
            smoke_exit_code = renderer_backend_smoke_main(smoke_argv)
        smoke_stdout_path.write_text(stdout_buffer.getvalue(), encoding="utf-8")
        smoke_stderr_path.write_text(stderr_buffer.getvalue(), encoding="utf-8")
        if smoke_summary_path.exists():
            smoke_summary = _load_json_object(smoke_summary_path)
        status = "SMOKE_SUCCEEDED" if smoke_exit_code == 0 else "SMOKE_FAILED"

    history_guard_report = None
    history_guard_report_path = None
    if run_history_guard:
        guard_root = out_root / "history_guard"
        guard_root.mkdir(parents=True, exist_ok=True)
        history_guard_report_path = (
            guard_root / "autonomy_e2e_history_guard_report_v0.json"
        )
        metadata_root = (
            Path(history_guard_metadata_root).resolve()
            if history_guard_metadata_root
            else Path(__file__).resolve().parents[3] / "metadata" / "autonomy_e2e"
        )
        current_repo_root = (
            Path(history_guard_current_repo_root).resolve()
            if history_guard_current_repo_root
            else Path(__file__).resolve().parents[3]
        )
        history_guard_report = build_autonomy_e2e_history_guard_report(
            current_repo_root=current_repo_root,
            metadata_root=metadata_root,
            compare_ref=history_guard_compare_ref,
            include_untracked=history_guard_include_untracked,
            json_out=history_guard_report_path,
        )
        if str(history_guard_report.get("status", "")).strip() == "FAIL":
            status = "FAILED"

    workflow_report = {
        "scenario_backend_smoke_workflow_report_schema_version": SCENARIO_BACKEND_SMOKE_WORKFLOW_REPORT_SCHEMA_VERSION_V0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "backend": backend,
        "skip_smoke": bool(skip_smoke),
        "lane_spacing_m": float(lane_spacing_m),
        "selection": selection_payload,
        "runtime_selection": runtime_selection,
        "bridge": dict(bridge_result["bridge_manifest"]),
        "history_guard": {
            "requested": bool(run_history_guard),
            "status": (
                history_guard_report.get("status")
                if isinstance(history_guard_report, dict)
                else None
            ),
            "failure_codes": (
                list(history_guard_report.get("failure_codes", []))
                if isinstance(history_guard_report, dict)
                else []
            ),
            "warnings": (
                list(history_guard_report.get("warnings", []))
                if isinstance(history_guard_report, dict)
                else []
            ),
            "compare_ref": history_guard_compare_ref if run_history_guard else None,
            "report_path": (
                str(history_guard_report_path.resolve())
                if history_guard_report_path is not None
                else None
            ),
        },
        "smoke": {
            "requested": not bool(skip_smoke),
            "exit_code": smoke_exit_code,
            "summary_path": str(smoke_summary_path.resolve()) if smoke_summary_path.exists() else None,
            "markdown_report_path": str(smoke_markdown_path.resolve()) if smoke_markdown_path.exists() else None,
            "html_report_path": str(smoke_html_path.resolve()) if smoke_html_path.exists() else None,
            "stdout_path": str(smoke_stdout_path.resolve()) if smoke_stdout_path.exists() else None,
            "stderr_path": str(smoke_stderr_path.resolve()) if smoke_stderr_path.exists() else None,
            "success": (
                bool(smoke_summary.get("success"))
                if isinstance(smoke_summary, dict) and "success" in smoke_summary
                else None
            ),
        },
        "renderer_backend_workflow": {
            "requested": bool(
                not skip_smoke
                and resolved_backend_bin
                and backend_compatibility.get("host_compatible") is False
            ),
            "status": (
                renderer_backend_workflow_summary.get("status")
                if isinstance(renderer_backend_workflow_summary, dict)
                else None
            ),
            "success": (
                renderer_backend_workflow_summary.get("success")
                if isinstance(renderer_backend_workflow_summary, dict)
                else None
            ),
            "summary_path": (
                renderer_backend_workflow_summary.get("artifacts", {}).get("summary_path")
                if isinstance(renderer_backend_workflow_summary, dict)
                else None
            ),
            "report_path": (
                renderer_backend_workflow_summary.get("artifacts", {}).get("report_path")
                if isinstance(renderer_backend_workflow_summary, dict)
                else None
            ),
            "recommended_next_command": (
                renderer_backend_workflow_summary.get("recommended_next_command")
                if isinstance(renderer_backend_workflow_summary, dict)
                else None
            ),
            "blocker_codes": (
                [
                    str(item.get("code"))
                    for item in renderer_backend_workflow_summary.get("blockers", [])
                    if isinstance(item, dict) and str(item.get("code", "")).strip()
                ]
                if isinstance(renderer_backend_workflow_summary, dict)
                else []
            ),
            "linux_handoff_ready": (
                renderer_backend_workflow_summary.get("linux_handoff", {}).get("ready")
                if isinstance(renderer_backend_workflow_summary, dict)
                else None
            ),
            "linux_handoff_bundle_path": (
                renderer_backend_workflow_summary.get("linux_handoff", {})
                .get("bundle", {})
                .get("bundle_path")
                if isinstance(renderer_backend_workflow_summary, dict)
                else None
            ),
            "linux_handoff_script_path": (
                renderer_backend_workflow_summary.get("artifacts", {}).get(
                    "linux_handoff_script_path"
                )
                if isinstance(renderer_backend_workflow_summary, dict)
                else None
            ),
            "linux_handoff_docker_script_path": (
                renderer_backend_workflow_summary.get("artifacts", {}).get(
                    "linux_handoff_docker_script_path"
                )
                if isinstance(renderer_backend_workflow_summary, dict)
                else None
            ),
            "docker_handoff_status": (
                renderer_backend_workflow_summary.get("status")
                if isinstance(renderer_backend_workflow_summary, dict)
                and str(renderer_backend_workflow_summary.get("status", "")).startswith("HANDOFF_DOCKER_")
                else None
            ),
        },
        "autoware": {
            "requested": not bool(skip_autoware_bridge),
            "status": (
                dict(autoware_result.get("report", {})).get("status")
                if isinstance(autoware_result, dict)
                else None
            ),
            "strict": bool(autoware_strict),
            "base_frame": str(autoware_base_frame).strip() or "base_link",
            "available_sensor_count": (
                dict(autoware_result.get("report", {})).get("available_sensor_count")
                if isinstance(autoware_result, dict)
                else None
            ),
            "missing_required_sensor_count": (
                dict(autoware_result.get("report", {})).get("missing_required_sensor_count")
                if isinstance(autoware_result, dict)
                else None
            ),
            "available_topics": (
                list(dict(autoware_result.get("report", {})).get("available_topics", []))
                if isinstance(autoware_result, dict)
                else []
            ),
            "required_topics_complete": (
                dict(autoware_result.get("report", {})).get("required_topics_complete")
                if isinstance(autoware_result, dict)
                else None
            ),
            "frame_tree_complete": (
                dict(autoware_result.get("report", {})).get("frame_tree_complete")
                if isinstance(autoware_result, dict)
                else None
            ),
            "warnings": (
                list(dict(autoware_result.get("report", {})).get("warnings", []))
                if isinstance(autoware_result, dict)
                else []
            ),
            "report_path": (
                str(Path(str(autoware_result.get("report_path"))).resolve())
                if isinstance(autoware_result, dict) and autoware_result.get("report_path") is not None
                else None
            ),
        },
        "artifacts": {
            "selection_path": str(selection_path.resolve()),
            "bridge_manifest_path": str(Path(bridge_result["bridge_manifest_path"]).resolve()),
            "smoke_scenario_path": str(Path(bridge_result["smoke_scenario_path"]).resolve()),
            "smoke_input_config_path": str(smoke_input_config_path.resolve()),
            "smoke_output_dir": str(smoke_output_root.resolve()),
            "autoware_report_path": (
                str(Path(str(autoware_result.get("report_path"))).resolve())
                if isinstance(autoware_result, dict) and autoware_result.get("report_path") is not None
                else None
            ),
            "autoware_sensor_contracts_path": (
                dict(autoware_result.get("report", {})).get("artifacts", {}).get("sensor_contracts_path")
                if isinstance(autoware_result, dict)
                else None
            ),
            "autoware_frame_tree_path": (
                dict(autoware_result.get("report", {})).get("artifacts", {}).get("frame_tree_path")
                if isinstance(autoware_result, dict)
                else None
            ),
            "autoware_pipeline_manifest_path": (
                dict(autoware_result.get("report", {})).get("artifacts", {}).get("pipeline_manifest_path")
                if isinstance(autoware_result, dict)
                else None
            ),
            "autoware_dataset_manifest_path": (
                dict(autoware_result.get("report", {})).get("artifacts", {}).get("dataset_manifest_path")
                if isinstance(autoware_result, dict)
                else None
            ),
            "renderer_backend_workflow_summary_path": (
                renderer_backend_workflow_summary.get("artifacts", {}).get("summary_path")
                if isinstance(renderer_backend_workflow_summary, dict)
                else None
            ),
            "renderer_backend_workflow_report_path": (
                renderer_backend_workflow_summary.get("artifacts", {}).get("report_path")
                if isinstance(renderer_backend_workflow_summary, dict)
                else None
            ),
            "renderer_backend_linux_handoff_script_path": (
                renderer_backend_workflow_summary.get("artifacts", {}).get("linux_handoff_script_path")
                if isinstance(renderer_backend_workflow_summary, dict)
                else None
            ),
            "renderer_backend_linux_handoff_bundle_manifest_path": (
                renderer_backend_workflow_summary.get("artifacts", {}).get(
                    "linux_handoff_bundle_manifest_path"
                )
                if isinstance(renderer_backend_workflow_summary, dict)
                else None
            ),
            "history_guard_report_path": (
                str(history_guard_report_path.resolve())
                if history_guard_report_path is not None
                else None
            ),
        },
    }
    if isinstance(smoke_summary, dict):
        runtime_diagnostics = _extract_backend_runtime_diagnostics(
            smoke_summary,
            repo_root=Path(__file__).resolve().parents[3],
        )
        workflow_report["smoke"]["summary"] = {
            "backend": smoke_summary.get("backend"),
            "success": smoke_summary.get("success"),
            "run_status": (smoke_summary.get("run") or {}).get("status")
            if isinstance(smoke_summary.get("run"), dict)
            else None,
            "failure_reason": (smoke_summary.get("run") or {}).get("failure_reason")
            if isinstance(smoke_summary.get("run"), dict)
            else None,
            "runner_smoke_status": (smoke_summary.get("runner_smoke") or {}).get("status")
            if isinstance(smoke_summary.get("runner_smoke"), dict)
            else None,
            "output_inspection_status": (
                (smoke_summary.get("output_inspection") or {}).get("status")
                if isinstance(smoke_summary.get("output_inspection"), dict)
                else None
            ),
            "output_smoke_status": (smoke_summary.get("output_smoke_report") or {}).get("status")
            if isinstance(smoke_summary.get("output_smoke_report"), dict)
            else None,
            "output_smoke_coverage_ratio": (
                (smoke_summary.get("output_smoke_report") or {}).get("coverage_ratio")
                if isinstance(smoke_summary.get("output_smoke_report"), dict)
                else None
            ),
            "output_origin_status": (
                (smoke_summary.get("output_smoke_report") or {}).get("output_origin_status")
                if isinstance(smoke_summary.get("output_smoke_report"), dict)
                else None
            ),
            "output_origin_counts": (
                dict((smoke_summary.get("output_smoke_report") or {}).get("output_origin_counts", {}))
                if isinstance(smoke_summary.get("output_smoke_report"), dict)
                else {}
            ),
            "output_origin_reasons": (
                list((smoke_summary.get("output_smoke_report") or {}).get("output_origin_reasons", []))
                if isinstance(smoke_summary.get("output_smoke_report"), dict)
                else []
            ),
            "output_comparison_status": (smoke_summary.get("output_comparison") or {}).get("status")
            if isinstance(smoke_summary.get("output_comparison"), dict)
            else None,
            "output_comparison_mismatch_reasons": (
                list((smoke_summary.get("output_comparison") or {}).get("mismatch_reasons", []))
                if isinstance(smoke_summary.get("output_comparison"), dict)
                else []
            ),
            "output_comparison_unexpected_output_count": (
                (smoke_summary.get("output_comparison") or {}).get("unexpected_output_count")
                if isinstance(smoke_summary.get("output_comparison"), dict)
                else None
            ),
            "output_comparison_origin_status": (
                (smoke_summary.get("output_comparison") or {}).get("output_origin_status")
                if isinstance(smoke_summary.get("output_comparison"), dict)
                else None
            ),
            "output_comparison_origin_counts": (
                dict((smoke_summary.get("output_comparison") or {}).get("output_origin_counts", {}))
                if isinstance(smoke_summary.get("output_comparison"), dict)
                else {}
            ),
            "sidecar_materialization_status": (
                (smoke_summary.get("sidecar_materialization") or {}).get("status")
                if isinstance(smoke_summary.get("sidecar_materialization"), dict)
                else None
            ),
            "sidecar_materialized_output_count": (
                (smoke_summary.get("sidecar_materialization") or {}).get("materialized_output_count")
                if isinstance(smoke_summary.get("sidecar_materialization"), dict)
                else None
            ),
            **runtime_diagnostics,
        }

    report_path = out_root / "scenario_backend_smoke_workflow_report_v0.json"
    _write_json(report_path, workflow_report)
    if not skip_autoware_bridge and (
        smoke_summary_path.exists()
        or workflow_report["status"] in {
            "HANDOFF_READY",
            "HANDOFF_DOCKER_VERIFIED",
            "HANDOFF_DOCKER_EXECUTED",
            "HANDOFF_DOCKER_OUTPUT_READY",
        }
    ):
        autoware_result = run_autoware_pipeline_bridge(
            backend_smoke_workflow_report_path=str(report_path),
            runtime_backend_workflow_report_path="",
            out_root=out_root / "autoware",
            base_frame=autoware_base_frame,
            strict=bool(autoware_strict),
        )
        if dict(autoware_result.get("report", {})).get("status") == "FAILED":
            workflow_report["status"] = "FAILED"
        autoware_report = dict(autoware_result.get("report", {}))
        workflow_report["autoware"] = {
            "requested": True,
            "status": autoware_report.get("status"),
            "availability_mode": autoware_report.get("availability_mode"),
            "strict": bool(autoware_strict),
            "base_frame": str(autoware_base_frame).strip() or "base_link",
            "available_sensor_count": autoware_report.get("available_sensor_count"),
            "missing_required_sensor_count": autoware_report.get("missing_required_sensor_count"),
            "available_topics": list(autoware_report.get("available_topics", [])),
            "available_modalities": list(autoware_report.get("available_modalities", [])),
            "data_roots": list(autoware_report.get("data_roots", [])),
            "recording_style": autoware_report.get("recording_style"),
            "dataset_ready": autoware_report.get("dataset_ready"),
            "scenario_source": dict(autoware_report.get("scenario_source", {})),
            "required_topics_complete": autoware_report.get("required_topics_complete"),
            "frame_tree_complete": autoware_report.get("frame_tree_complete"),
            "warnings": list(autoware_report.get("warnings", [])),
            "report_path": (
                str(Path(str(autoware_result.get("report_path"))).resolve())
                if autoware_result.get("report_path") is not None
                else None
            ),
        }
        workflow_report["artifacts"]["autoware_report_path"] = workflow_report["autoware"]["report_path"]
        workflow_report["artifacts"]["autoware_sensor_contracts_path"] = autoware_report.get("artifacts", {}).get("sensor_contracts_path")
        workflow_report["artifacts"]["autoware_frame_tree_path"] = autoware_report.get("artifacts", {}).get("frame_tree_path")
        workflow_report["artifacts"]["autoware_pipeline_manifest_path"] = autoware_report.get("artifacts", {}).get("pipeline_manifest_path")
        workflow_report["artifacts"]["autoware_dataset_manifest_path"] = autoware_report.get("artifacts", {}).get("dataset_manifest_path")
        _write_json(report_path, workflow_report)
    return {
        "workflow_report_path": report_path,
        "workflow_report": workflow_report,
        "selection_path": selection_path,
        "bridge_result": bridge_result,
        "smoke_input_config_path": smoke_input_config_path,
    }


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        result = run_scenario_backend_smoke_workflow(
            variant_workflow_report_path=args.variant_workflow_report,
            batch_workflow_report_path=args.batch_workflow_report,
            smoke_config_path=Path(args.smoke_config).resolve(),
            backend=args.backend,
            out_root=Path(args.out_root).resolve(),
            selection_strategy=args.selection_strategy,
            selected_variant_id=args.selected_variant_id,
            lane_spacing_m=float(args.lane_spacing_m),
            smoke_output_dir=args.smoke_output_dir,
            setup_summary_path=args.setup_summary,
            backend_workflow_summary_path=args.backend_workflow_summary,
            backend_bin=args.backend_bin,
            renderer_map=args.renderer_map,
            option_overrides=list(args.set_option),
            renderer_backend_workflow_output_root=args.renderer_backend_workflow_output_root,
            pack_linux_handoff=bool(args.pack_linux_handoff),
            verify_linux_handoff_bundle=bool(args.verify_linux_handoff_bundle),
            run_linux_handoff_docker=bool(args.run_linux_handoff_docker),
            docker_handoff_execute=bool(args.docker_handoff_execute),
            docker_binary=args.docker_binary,
            docker_image=args.docker_image,
            docker_platform=args.docker_platform,
            docker_container_workspace=args.docker_container_workspace,
            refresh_docker_handoff_preflight=bool(args.refresh_docker_handoff_preflight),
            skip_smoke=bool(args.skip_smoke),
            skip_autoware_bridge=bool(args.skip_autoware_bridge),
            autoware_base_frame=args.autoware_base_frame,
            autoware_strict=bool(args.autoware_strict),
            run_history_guard=bool(args.run_history_guard),
            history_guard_metadata_root=args.history_guard_metadata_root,
            history_guard_current_repo_root=args.history_guard_current_repo_root,
            history_guard_compare_ref=args.history_guard_compare_ref,
            history_guard_include_untracked=bool(args.history_guard_include_untracked),
        )
        workflow_report = result["workflow_report"]
        print(f"[ok] status={workflow_report['status']}")
        print(f"[ok] variant_id={workflow_report['selection']['variant_id']}")
        print(f"[ok] smoke_scenario={workflow_report['artifacts']['smoke_scenario_path']}")
        print(f"[ok] report={result['workflow_report_path']}")
        return 0 if workflow_report["status"] in {"BRIDGED_ONLY", "SMOKE_SUCCEEDED", "HANDOFF_READY", "HANDOFF_DOCKER_VERIFIED", "HANDOFF_DOCKER_EXECUTED", "HANDOFF_DOCKER_OUTPUT_READY"} else 2
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] scenario_backend_smoke_workflow.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
