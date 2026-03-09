from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.tools.scenario_runtime_backend_probe import (
    run_scenario_runtime_backend_probe,
)


SCENARIO_RUNTIME_BACKEND_PROBE_SET_REPORT_SCHEMA_VERSION_V0 = (
    "scenario_runtime_backend_probe_set_report_v0"
)
DEFAULT_SCENARIO_RUNTIME_BACKEND_PROBE_SET_ID = "awsim_real_v0"


def _classify_blocking_reason(reason_code: str) -> str:
    normalized = str(reason_code or "").strip().upper()
    if not normalized:
        return "unknown"
    if normalized.startswith("AUTOWARE_") or "TOPIC" in normalized:
        return "consumer_contract"
    if (
        normalized.startswith("LOCAL_RUNTIME_")
        or normalized.endswith("_RUNTIME_MISSING")
        or normalized == "DOCKER_IMAGE_MISSING"
        or normalized == "PLATFORM_UNSUPPORTED_LOCAL_HOST"
    ):
        return "runtime_environment"
    if (
        "DOCKER" in normalized
        or "HOST_INCOMPATIBLE" in normalized
        or "PACKAGED_RUNTIME" in normalized
        or "IMAGE" in normalized
        or "STORAGE" in normalized
    ):
        return "runtime_environment"
    if "RUNTIME" in normalized or "HANDOFF" in normalized:
        return "runtime_execution"
    if "HISTORY_GUARD" in normalized or "PROVENANCE" in normalized:
        return "governance"
    return "unknown"


def _recommended_action_for_blocking_reason(reason_code: str, category: str) -> str:
    normalized = str(reason_code or "").strip().upper()
    if normalized == "HOST_INCOMPATIBLE_PACKAGED_RUNTIME":
        return "Use the linux handoff packaged runtime path."
    if normalized == "AUTOWARE_STATUS_MISMATCH":
        return "Inspect missing and recovered Autoware topics for the selected consumer profile."
    if normalized.startswith("DOCKER_") or "STORAGE" in normalized:
        return "Repair the local Docker image store or use a packaged runtime handoff path."
    if category == "runtime_execution":
        return "Inspect the backend runtime workflow report and rerun the selected runtime strategy."
    if category == "consumer_contract":
        return "Reconcile required consumer topics against the bridge output and consumer profile."
    if category == "governance":
        return "Refresh provenance metadata and rerun the history guard."
    if category == "runtime_environment":
        return "Fix the runtime environment or switch to the recommended handoff path."
    return "Inspect the probe report for the blocking reason details."


def _recommended_action_for_runtime_strategy(
    strategy: str, preferred_runtime_source: str
) -> str:
    normalized_strategy = str(strategy or "").strip()
    normalized_source = str(preferred_runtime_source or "").strip()
    if normalized_strategy == "linux_handoff_packaged_runtime":
        return "Prepare and execute the linux handoff packaged runtime workflow."
    if normalized_strategy in {"docker_runtime", "local_docker_runtime"}:
        return "Repair the Docker runtime and rerun the backend through the Docker execution path."
    if normalized_strategy in {"host_packaged_runtime", "local_packaged_runtime"}:
        return "Rerun the packaged backend directly on the host runtime path."
    if normalized_strategy == "host_native_runtime":
        return "Rerun the native backend directly on the host runtime path."
    if normalized_strategy == "packaged_runtime_required":
        return "Acquire and stage a packaged runtime for the selected backend."
    if normalized_strategy == "docker_or_packaged_runtime_required":
        return "Choose a Docker image or packaged runtime and rerun the backend workflow."
    if normalized_source == "packaged_runtime":
        return "Use the selected packaged runtime path and rerun the backend workflow."
    if normalized_source == "docker_image":
        return "Use the selected Docker image path and rerun the backend workflow."
    return "Inspect the selected runtime strategy and rerun the backend workflow."


def _build_runtime_strategy_plan(
    *,
    strategy: str,
    preferred_runtime_source: str,
    docker_storage_status: str,
    reason_codes: list[str],
) -> dict[str, Any]:
    normalized_strategy = str(strategy or "").strip()
    normalized_source = str(preferred_runtime_source or "").strip()
    normalized_storage = str(docker_storage_status or "").strip()
    normalized_reason_codes = sorted(
        {
            str(reason_code or "").strip()
            for reason_code in reason_codes or []
            if str(reason_code or "").strip()
        }
    )
    if normalized_strategy == "linux_handoff_packaged_runtime":
        return {
            "plan_id": "linux_handoff_packaged_runtime",
            "summary": "Use the packaged runtime through the linux handoff workflow.",
            "steps": [
                "Confirm the selected packaged runtime path is present and current.",
                "Generate or refresh the linux handoff bundle for the packaged runtime.",
                "Execute the linux handoff workflow and rerun the backend smoke path.",
            ],
        }
    if normalized_strategy == "docker_runtime":
        if normalized_storage in {
            "image_store_corrupt",
            "content_store_corrupt",
            "storage_io_error",
        }:
            return {
                "plan_id": "docker_storage_repair",
                "summary": "Repair the local Docker storage before using the Docker runtime path.",
                "steps": [
                    "Repair the local Docker Desktop or containerd image store.",
                    "Re-run the Docker pull or verify step for the selected backend image.",
                    "Rerun the backend smoke workflow through the Docker runtime path.",
                ],
            }
        return {
            "plan_id": "docker_runtime_rerun",
            "summary": "Use the Docker runtime path directly.",
            "steps": [
                "Verify the selected Docker image is present and healthy.",
                "Rerun the backend smoke workflow through the Docker runtime path.",
            ],
        }
    if normalized_strategy == "local_packaged_runtime":
        return {
            "plan_id": "local_packaged_runtime",
            "summary": "Use the host-compatible packaged runtime directly.",
            "steps": [
                "Verify the packaged runtime binary and related assets are present.",
                "Rerun the backend smoke workflow using the packaged runtime on the host.",
            ],
        }
    if normalized_strategy == "host_native_runtime":
        return {
            "plan_id": "host_native_runtime",
            "summary": "Use the native runtime directly on the host.",
            "steps": [
                "Verify the native runtime path is available on the host.",
                "Rerun the backend smoke workflow using the native runtime path.",
            ],
        }
    if normalized_strategy == "packaged_runtime_required":
        if "DOCKER_STORAGE_CORRUPT" in normalized_reason_codes:
            return {
                "plan_id": "packaged_runtime_required_after_docker_failure",
                "summary": "Docker is blocked, so acquire and stage a packaged runtime.",
                "steps": [
                    "Acquire or locate a packaged runtime for the selected backend.",
                    "Stage the packaged runtime into the local runtime workspace.",
                    "Use the packaged runtime path or linux handoff workflow to rerun smoke.",
                ],
            }
        return {
            "plan_id": "packaged_runtime_required",
            "summary": "Acquire and stage a packaged runtime for the selected backend.",
            "steps": [
                "Acquire or locate a packaged runtime for the selected backend.",
                "Stage the packaged runtime into the local runtime workspace.",
                "Rerun the backend smoke workflow with the packaged runtime path.",
            ],
        }
    if normalized_strategy == "docker_or_packaged_runtime_required":
        if normalized_storage in {
            "image_store_corrupt",
            "content_store_corrupt",
            "storage_io_error",
        }:
            return {
                "plan_id": "prefer_packaged_runtime_due_to_docker_storage",
                "summary": "Prefer a packaged runtime because Docker storage is unhealthy.",
                "steps": [
                    "Acquire or locate a packaged runtime for the selected backend.",
                    "Stage the packaged runtime into the local runtime workspace.",
                    "Rerun the backend smoke workflow with the packaged runtime path or linux handoff workflow.",
                ],
            }
        if normalized_source == "docker_or_packaged":
            return {
                "plan_id": "choose_docker_or_packaged_runtime",
                "summary": "Pick an available Docker image or packaged runtime and rerun.",
                "steps": [
                    "Try the recommended Docker pull or packaged runtime acquisition command.",
                    "Use whichever runtime source becomes available first.",
                    "Rerun the backend smoke workflow with the selected runtime source.",
                ],
            }
    return {
        "plan_id": "generic_runtime_strategy",
        "summary": "Inspect the runtime strategy and apply the recommended backend rerun path.",
        "steps": [
            "Inspect the selected runtime strategy and preferred runtime source.",
            "Follow the recommended runtime command for the selected backend.",
            "Rerun the backend smoke workflow and inspect the updated probe result.",
        ],
    }


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a named set of compact runtime/backend probes against existing "
            "scenario_runtime_backend_workflow_report_v0.json artifacts."
        )
    )
    parser.add_argument(
        "--probe-set-id",
        default=DEFAULT_SCENARIO_RUNTIME_BACKEND_PROBE_SET_ID,
        help="Named probe set to execute",
    )
    parser.add_argument(
        "--out-root",
        required=True,
        help="Output directory for the probe set report",
    )
    parser.add_argument(
        "--repo-root",
        default="",
        help="Repo root used to resolve built-in report paths",
    )
    parser.add_argument(
        "--autoware-base-frame",
        default="base_link",
        help="Base frame passed through to each probe rebridge",
    )
    parser.add_argument(
        "--autoware-strict",
        action="store_true",
        help="Run each probe in strict Autoware bridge mode",
    )
    parser.add_argument(
        "--run-history-guard",
        action="store_true",
        help="Run provenance guard during each probe rebridge",
    )
    parser.add_argument(
        "--history-guard-metadata-root",
        default="",
        help="Override metadata root for provenance guard",
    )
    parser.add_argument(
        "--history-guard-current-repo-root",
        default="",
        help="Override current repo root for provenance guard",
    )
    parser.add_argument(
        "--history-guard-compare-ref",
        default="origin/main",
        help="Git ref used by the provenance guard",
    )
    parser.add_argument(
        "--history-guard-include-untracked",
        action="store_true",
        help="Include untracked files in provenance guard diff",
    )
    return parser.parse_args(argv)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _default_probe_set_specs(repo_root: Path) -> dict[str, dict[str, Any]]:
    return {
        DEFAULT_SCENARIO_RUNTIME_BACKEND_PROBE_SET_ID: {
            "probe_set_id": DEFAULT_SCENARIO_RUNTIME_BACKEND_PROBE_SET_ID,
            "description": (
                "Real AWSIM runtime-origin probe set covering tracking READY and "
                "semantic recovery READY paths."
            ),
            "probes": [
                {
                    "probe_id": "tracking_ready",
                    "runtime_backend_workflow_report_path": (
                        repo_root
                        / "artifacts"
                        / "scenario_runtime_backend_real_awsim_tracking_ready_probe"
                        / "scenario_runtime_backend_workflow_report_v0.json"
                    ),
                    "consumer_profile_id": "tracking_fusion_v0",
                    "expect_runtime_status": "SUCCEEDED",
                    "expect_autoware_status": "READY",
                },
                {
                    "probe_id": "semantic_primary_ready",
                    "runtime_backend_workflow_report_path": (
                        repo_root
                        / "artifacts"
                        / "scenario_runtime_backend_real_awsim_probe_v14"
                        / "scenario_runtime_backend_workflow_report_v0.json"
                    ),
                    "consumer_profile_id": "semantic_perception_v0",
                    "expect_runtime_status": "SUCCEEDED",
                    "expect_autoware_status": "READY",
                },
                {
                    "probe_id": "semantic_recovery_ready",
                    "runtime_backend_workflow_report_path": (
                        repo_root
                        / "artifacts"
                        / "scenario_runtime_backend_real_awsim_degraded_runtime_probe"
                        / "scenario_runtime_backend_workflow_report_v0.json"
                    ),
                    "consumer_profile_id": "semantic_perception_v0",
                    "expect_runtime_status": "SUCCEEDED",
                    "expect_autoware_status": "READY",
                },
            ],
        }
    }


def _build_markdown_report(report: dict[str, Any]) -> str:
    probe_rows = list(report.get("probes", []) or [])
    runtime_strategy_counts = dict(report.get("runtime_strategy_counts", {}) or {})
    runtime_strategy_probe_ids = dict(report.get("runtime_strategy_probe_ids", {}) or {})
    runtime_strategy_reason_code_counts = dict(
        report.get("runtime_strategy_reason_code_counts", {}) or {}
    )
    recommended_command_counts = dict(
        report.get("runtime_strategy_recommended_command_counts", {}) or {}
    )
    recommended_command_probe_ids = dict(
        report.get("runtime_strategy_recommended_command_probe_ids", {}) or {}
    )
    blocking_reason_counts = dict(report.get("blocking_reason_counts", {}) or {})
    blocking_reason_probe_ids = dict(report.get("blocking_reason_probe_ids", {}) or {})
    blocking_reason_category_counts = dict(
        report.get("blocking_reason_category_counts", {}) or {}
    )
    blocking_reason_category_probe_ids = dict(
        report.get("blocking_reason_category_probe_ids", {}) or {}
    )
    runtime_strategy_summary_rows = list(
        report.get("runtime_strategy_summary_rows", []) or []
    )
    blocking_reason_summary_rows = list(
        report.get("blocking_reason_summary_rows", []) or []
    )
    lines = [
        "# Scenario Runtime Backend Probe Set",
        "",
        f"- Probe set: `{report.get('probe_set_id') or '-'}`",
        f"- Status: `{report.get('status') or '-'}`",
        f"- Description: {report.get('description') or '-'}",
        f"- Probe count: `{report.get('probe_count') or 0}`",
        f"- Pass count: `{report.get('pass_count') or 0}`",
        f"- Fail count: `{report.get('fail_count') or 0}`",
        f"- Failed probe IDs: `{', '.join(report.get('failed_probe_ids', [])) or '-'}`",
        f"- Runtime-native READY probes: `{', '.join(report.get('runtime_native_ready_probe_ids', [])) or '-'}`",
        f"- Supplemental-dependent probes: `{', '.join(report.get('supplemental_dependency_probe_ids', [])) or '-'}`",
        f"- Recovered required topics: `{', '.join(report.get('recovered_required_topics', [])) or '-'}`",
        f"- Runtime strategy counts: `{json.dumps(runtime_strategy_counts, sort_keys=True) if runtime_strategy_counts else '-'}`",
        f"- Runtime strategy reason counts: `{json.dumps(runtime_strategy_reason_code_counts, sort_keys=True) if runtime_strategy_reason_code_counts else '-'}`",
        f"- Primary runtime strategy: `{report.get('primary_runtime_strategy') or '-'}`",
        f"- Recommended runtime action: `{report.get('recommended_runtime_action') or '-'}`",
        f"- Blocking reason counts: `{json.dumps(blocking_reason_counts, sort_keys=True) if blocking_reason_counts else '-'}`",
        f"- Blocking reason category counts: `{json.dumps(blocking_reason_category_counts, sort_keys=True) if blocking_reason_category_counts else '-'}`",
        f"- Primary blocking reason: `{report.get('primary_blocking_reason_code') or '-'}`",
        f"- Primary blocking category: `{report.get('primary_blocking_category') or '-'}`",
        f"- Recommended resolution focus: `{report.get('recommended_resolution_focus') or '-'}`",
        f"- Recommended next command: `{report.get('recommended_next_command') or '-'}`",
        "",
        "## Runtime Strategies",
        "",
        "| Strategy | Probe IDs |",
        "| --- | --- |",
    ]
    for strategy, probe_ids in sorted(runtime_strategy_probe_ids.items()):
        lines.append(
            f"| {strategy or '-'} | {', '.join(probe_ids or []) or '-'} |"
        )
    lines.extend(
        [
            "",
            "## Runtime Strategy Actions",
            "",
            "| Strategy | Preferred Source | Probe IDs | Action |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in runtime_strategy_summary_rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("strategy") or "-"),
                    str(row.get("preferred_runtime_source") or "-"),
                    str(", ".join(row.get("probe_ids", [])) or "-"),
                    str(row.get("recommended_action") or "-"),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Runtime Strategy Plans",
            "",
            "| Strategy | Plan ID | Probe IDs | Summary |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in report.get("runtime_strategy_plan_rows", []) or []:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("strategy") or "-"),
                    str(row.get("plan_id") or "-"),
                    str(", ".join(row.get("probe_ids", [])) or "-"),
                    str(row.get("plan_summary") or "-"),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Runtime Commands",
            "",
            "| Command | Probe IDs |",
            "| --- | --- |",
        ]
    )
    for command, probe_ids in sorted(recommended_command_probe_ids.items()):
        lines.append(
            f"| {command or '-'} | {', '.join(probe_ids or []) or '-'} |"
        )
    lines.extend(
        [
            "",
            "## Blocking Reasons",
            "",
            "| Reason | Probe IDs |",
            "| --- | --- |",
        ]
    )
    for reason, probe_ids in sorted(blocking_reason_probe_ids.items()):
        lines.append(
            f"| {reason or '-'} | {', '.join(probe_ids or []) or '-'} |"
        )
    lines.extend(
        [
            "",
            "## Blocking Reason Categories",
            "",
            "| Category | Probe IDs |",
            "| --- | --- |",
        ]
    )
    for category, probe_ids in sorted(blocking_reason_category_probe_ids.items()):
        lines.append(
            f"| {category or '-'} | {', '.join(probe_ids or []) or '-'} |"
        )
    lines.extend(
        [
            "",
            "## Blocking Reason Actions",
            "",
            "| Reason | Category | Probe IDs | Action |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in blocking_reason_summary_rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("reason_code") or "-"),
                    str(row.get("category") or "-"),
                    str(", ".join(row.get("probe_ids", [])) or "-"),
                    str(row.get("recommended_action") or "-"),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Recommended Resolution Steps",
            "",
            "| Order | Step |",
            "| --- | --- |",
        ]
    )
    for index, step in enumerate(report.get("recommended_resolution_steps", []) or [], start=1):
        lines.append(f"| {index} | {step} |")
    lines.extend(
        [
            "",
            "## Primary Runtime Plan",
            "",
            f"- Plan ID: `{report.get('primary_runtime_plan_id') or '-'}`",
        ]
    )
    lines.extend(
        [
            "",
            "## Probes",
            "",
            "| Probe | Status | Strategy | Source Runtime | Refreshed Runtime | Source Autoware | Refreshed Autoware | Consumer | Supplemental Dep | Recovered Topics |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for probe in probe_rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(probe.get("probe_id") or "-"),
                    str(probe.get("status") or "-"),
                    str(probe.get("backend_runtime_strategy") or "-"),
                    str(probe.get("source_runtime_status") or "-"),
                    str(probe.get("runtime_status") or "-"),
                    str(probe.get("source_autoware_pipeline_status") or "-"),
                    str(probe.get("autoware_pipeline_status") or "-"),
                    str(probe.get("consumer_profile_id") or "-"),
                    str(bool(probe.get("supplemental_dependency"))),
                    str(", ".join(probe.get("recovered_required_topics", [])) or "-"),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def run_scenario_runtime_backend_probe_set(
    *,
    out_root: Path,
    probe_set_id: str = DEFAULT_SCENARIO_RUNTIME_BACKEND_PROBE_SET_ID,
    repo_root: str | Path | None = None,
    autoware_base_frame: str = "base_link",
    autoware_strict: bool = False,
    run_history_guard: bool = False,
    history_guard_metadata_root: str | Path | None = None,
    history_guard_current_repo_root: str | Path | None = None,
    history_guard_compare_ref: str = "origin/main",
    history_guard_include_untracked: bool = False,
) -> dict[str, Any]:
    out_root = out_root.resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    resolved_repo_root = Path(repo_root).resolve() if repo_root else Path.cwd().resolve()
    catalog = _default_probe_set_specs(resolved_repo_root)
    if probe_set_id not in catalog:
        raise ValueError(f"Unknown scenario runtime backend probe set: {probe_set_id}")
    spec = dict(catalog[probe_set_id])

    probe_results: list[dict[str, Any]] = []
    for probe_spec in spec.get("probes", []):
        runtime_report_path = Path(
            probe_spec["runtime_backend_workflow_report_path"]
        ).resolve()
        probe_result = run_scenario_runtime_backend_probe(
            runtime_backend_workflow_report_path=str(runtime_report_path),
            out_root=out_root / str(probe_spec["probe_id"]).strip(),
            probe_id=str(probe_spec["probe_id"]).strip(),
            consumer_profile_id=str(probe_spec.get("consumer_profile_id", "")).strip(),
            autoware_base_frame=autoware_base_frame,
            autoware_strict=bool(autoware_strict),
            expect_runtime_status=str(probe_spec.get("expect_runtime_status", "")).strip(),
            expect_autoware_status=str(
                probe_spec.get("expect_autoware_status", "")
            ).strip(),
            run_history_guard=bool(run_history_guard),
            history_guard_metadata_root=history_guard_metadata_root,
            history_guard_current_repo_root=history_guard_current_repo_root,
            history_guard_compare_ref=history_guard_compare_ref,
            history_guard_include_untracked=bool(history_guard_include_untracked),
        )
        probe_report = dict(probe_result.get("report", {}))
        probe_summary = dict(probe_report.get("summary", {}))
        rebridge_report = dict(
            probe_result.get("rebridge_result", {}).get("workflow_report", {})
        )
        rebridge_status_summary = dict(rebridge_report.get("status_summary", {}))
        rebridge_comparison = dict(
            rebridge_report.get("rebridge", {}).get("comparison", {})
        )
        recovered_required_topics = list(
            rebridge_comparison.get("recovered_required_topics", []) or []
        )
        source_missing_required_topics = list(
            rebridge_comparison.get("source_missing_required_topics", []) or []
        )
        refreshed_missing_required_topics = list(
            rebridge_comparison.get("refreshed_missing_required_topics", []) or []
        )
        supplemental_dependency = bool(
            recovered_required_topics or probe_summary.get("semantic_topic_recovered")
        )
        probe_results.append(
            {
                "probe_id": probe_report.get("probe_id"),
                "status": probe_report.get("status"),
                "consumer_profile_id": probe_report.get("consumer_profile_id"),
                "source_runtime_status": rebridge_comparison.get("source_runtime_status"),
                "runtime_status": probe_summary.get("runtime_status"),
                "source_autoware_pipeline_status": rebridge_comparison.get(
                    "source_autoware_pipeline_status"
                ),
                "autoware_pipeline_status": probe_summary.get(
                    "autoware_pipeline_status"
                ),
                "semantic_topic_recovered": bool(
                    probe_summary.get("semantic_topic_recovered")
                ),
                "supplemental_dependency": supplemental_dependency,
                "backend_runtime_strategy": rebridge_status_summary.get(
                    "backend_runtime_strategy"
                ),
                "backend_runtime_strategy_source": rebridge_status_summary.get(
                    "backend_runtime_strategy_source"
                ),
                "backend_runtime_preferred_runtime_source": rebridge_status_summary.get(
                    "backend_runtime_preferred_runtime_source"
                ),
                "backend_runtime_strategy_reason_codes": list(
                    rebridge_status_summary.get(
                        "backend_runtime_strategy_reason_codes", []
                    )
                    or []
                ),
                "backend_runtime_recommended_command": rebridge_status_summary.get(
                    "backend_runtime_recommended_command"
                ),
                "backend_runtime_selected_path": rebridge_status_summary.get(
                    "backend_runtime_selected_path"
                ),
                "backend_runtime_docker_storage_status": rebridge_status_summary.get(
                    "backend_runtime_docker_storage_status"
                ),
                "source_missing_required_topics": source_missing_required_topics,
                "refreshed_missing_required_topics": refreshed_missing_required_topics,
                "recovered_required_topics": recovered_required_topics,
                "runtime_backend_workflow_report_path": str(runtime_report_path),
                "report_path": str(probe_result["report_path"]),
                "markdown_path": str(probe_result["markdown_path"]),
                "failure_codes": list(
                    probe_report.get("evaluation", {}).get("failure_codes", []) or []
                ),
            }
        )

    passed_probe_ids = sorted(
        result["probe_id"] for result in probe_results if result.get("status") == "PASS"
    )
    failed_probe_ids = sorted(
        result["probe_id"] for result in probe_results if result.get("status") != "PASS"
    )
    runtime_native_ready_probe_ids = sorted(
        result["probe_id"]
        for result in probe_results
        if result.get("status") == "PASS" and not result.get("supplemental_dependency")
    )
    supplemental_dependency_probe_ids = sorted(
        result["probe_id"]
        for result in probe_results
        if result.get("supplemental_dependency")
    )
    status_counts: dict[str, int] = {}
    runtime_strategy_counts: dict[str, int] = {}
    runtime_strategy_probe_ids: dict[str, list[str]] = {}
    runtime_strategy_preferred_runtime_sources: dict[str, list[str]] = {}
    runtime_strategy_reason_code_counts: dict[str, int] = {}
    runtime_strategy_recommended_command_counts: dict[str, int] = {}
    runtime_strategy_recommended_command_probe_ids: dict[str, list[str]] = {}
    blocking_reason_counts: dict[str, int] = {}
    blocking_reason_probe_ids: dict[str, list[str]] = {}
    blocking_reason_category_counts: dict[str, int] = {}
    blocking_reason_category_probe_ids: dict[str, list[str]] = {}
    recovered_required_topics: set[str] = set()
    source_missing_required_topics: set[str] = set()
    refreshed_missing_required_topics: set[str] = set()
    for result in probe_results:
        status_key = str(result.get("status") or "UNKNOWN")
        status_counts[status_key] = status_counts.get(status_key, 0) + 1
        runtime_strategy = str(result.get("backend_runtime_strategy") or "UNKNOWN")
        runtime_strategy_counts[runtime_strategy] = (
            runtime_strategy_counts.get(runtime_strategy, 0) + 1
        )
        runtime_strategy_probe_ids.setdefault(runtime_strategy, []).append(
            str(result.get("probe_id") or "")
        )
        runtime_strategy_preferred_runtime_sources.setdefault(runtime_strategy, []).append(
            str(result.get("backend_runtime_preferred_runtime_source") or "")
        )
        for reason_code in result.get("backend_runtime_strategy_reason_codes", []) or []:
            reason_code_str = str(reason_code)
            runtime_strategy_reason_code_counts[reason_code_str] = (
                runtime_strategy_reason_code_counts.get(reason_code_str, 0) + 1
            )
            blocking_reason_counts[reason_code_str] = (
                blocking_reason_counts.get(reason_code_str, 0) + 1
            )
            blocking_reason_probe_ids.setdefault(reason_code_str, []).append(
                str(result.get("probe_id") or "")
            )
            category = _classify_blocking_reason(reason_code_str)
            blocking_reason_category_counts[category] = (
                blocking_reason_category_counts.get(category, 0) + 1
            )
            blocking_reason_category_probe_ids.setdefault(category, []).append(
                str(result.get("probe_id") or "")
            )
        for failure_code in result.get("failure_codes", []) or []:
            failure_code_str = str(failure_code)
            blocking_reason_counts[failure_code_str] = (
                blocking_reason_counts.get(failure_code_str, 0) + 1
            )
            blocking_reason_probe_ids.setdefault(failure_code_str, []).append(
                str(result.get("probe_id") or "")
            )
            category = _classify_blocking_reason(failure_code_str)
            blocking_reason_category_counts[category] = (
                blocking_reason_category_counts.get(category, 0) + 1
            )
            blocking_reason_category_probe_ids.setdefault(category, []).append(
                str(result.get("probe_id") or "")
            )
        recommended_command = str(
            result.get("backend_runtime_recommended_command") or ""
        ).strip()
        if recommended_command:
            runtime_strategy_recommended_command_counts[recommended_command] = (
                runtime_strategy_recommended_command_counts.get(recommended_command, 0)
                + 1
            )
            runtime_strategy_recommended_command_probe_ids.setdefault(
                recommended_command, []
            ).append(str(result.get("probe_id") or ""))
        recovered_required_topics.update(result.get("recovered_required_topics", []) or [])
        source_missing_required_topics.update(
            result.get("source_missing_required_topics", []) or []
        )
        refreshed_missing_required_topics.update(
            result.get("refreshed_missing_required_topics", []) or []
        )

    recommended_next_command = ""
    if failed_probe_ids:
        for result in probe_results:
            if result.get("probe_id") in failed_probe_ids:
                recommended_next_command = str(
                    result.get("backend_runtime_recommended_command") or ""
                ).strip()
                if recommended_next_command:
                    break
    if not recommended_next_command and runtime_strategy_recommended_command_counts:
        recommended_next_command = sorted(
            runtime_strategy_recommended_command_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[0][0]

    runtime_strategy_summary_rows = []
    runtime_strategy_plan_rows = []
    for strategy, probe_ids in sorted(runtime_strategy_probe_ids.items()):
        preferred_sources = [
            source
            for source in runtime_strategy_preferred_runtime_sources.get(strategy, [])
            if str(source or "").strip()
        ]
        preferred_runtime_source = None
        if preferred_sources:
            preferred_runtime_source = sorted(
                {str(source) for source in preferred_sources}
            )[0]
        docker_storage_statuses = sorted(
            {
                str(result.get("backend_runtime_docker_storage_status") or "").strip()
                for result in probe_results
                if str(result.get("backend_runtime_strategy") or "") == strategy
                and str(result.get("backend_runtime_docker_storage_status") or "").strip()
            }
        )
        reason_codes = sorted(
            {
                str(reason_code or "").strip()
                for result in probe_results
                if str(result.get("backend_runtime_strategy") or "") == strategy
                for reason_code in result.get("backend_runtime_strategy_reason_codes", [])
                or []
                if str(reason_code or "").strip()
            }
        )
        runtime_plan = _build_runtime_strategy_plan(
            strategy=strategy,
            preferred_runtime_source=preferred_runtime_source or "",
            docker_storage_status=docker_storage_statuses[0] if docker_storage_statuses else "",
            reason_codes=reason_codes,
        )
        runtime_strategy_summary_rows.append(
            {
                "strategy": strategy,
                "probe_ids": sorted(probe_ids),
                "preferred_runtime_source": preferred_runtime_source,
                "recommended_action": _recommended_action_for_runtime_strategy(
                    strategy, preferred_runtime_source or ""
                ),
            }
        )
        runtime_strategy_plan_rows.append(
            {
                "strategy": strategy,
                "probe_ids": sorted(probe_ids),
                "preferred_runtime_source": preferred_runtime_source,
                "docker_storage_statuses": docker_storage_statuses,
                "reason_codes": reason_codes,
                "plan_id": runtime_plan["plan_id"],
                "plan_summary": runtime_plan["summary"],
                "plan_steps": list(runtime_plan.get("steps", []) or []),
            }
        )

    blocking_reason_summary_rows = []
    for reason_code, probe_ids in sorted(blocking_reason_probe_ids.items()):
        category = _classify_blocking_reason(reason_code)
        blocking_reason_summary_rows.append(
            {
                "reason_code": reason_code,
                "category": category,
                "count": blocking_reason_counts.get(reason_code, 0),
                "probe_ids": sorted(probe_ids),
                "recommended_action": _recommended_action_for_blocking_reason(
                    reason_code, category
                ),
            }
        )

    recommended_resolution_focus = None
    primary_runtime_strategy = None
    recommended_runtime_action = None
    primary_runtime_plan_id = None
    recommended_runtime_plan_steps: list[str] = []
    primary_blocking_reason_code = None
    primary_blocking_category = None
    if runtime_strategy_summary_rows:
        primary_strategy_row = sorted(
            runtime_strategy_summary_rows,
            key=lambda row: (
                -len(list(row.get("probe_ids", []) or [])),
                str(row.get("strategy") or ""),
            ),
        )[0]
        primary_runtime_strategy = primary_strategy_row["strategy"]
        recommended_runtime_action = primary_strategy_row["recommended_action"]
    if runtime_strategy_plan_rows:
        primary_runtime_plan_row = sorted(
            runtime_strategy_plan_rows,
            key=lambda row: (
                -len(list(row.get("probe_ids", []) or [])),
                str(row.get("strategy") or ""),
            ),
        )[0]
        primary_runtime_plan_id = primary_runtime_plan_row["plan_id"]
        recommended_runtime_plan_steps = list(
            primary_runtime_plan_row.get("plan_steps", []) or []
        )
    if blocking_reason_summary_rows:
        primary_row = sorted(
            blocking_reason_summary_rows,
            key=lambda row: (-int(row.get("count") or 0), str(row.get("reason_code") or "")),
        )[0]
        recommended_resolution_focus = primary_row["recommended_action"]
        primary_blocking_reason_code = primary_row["reason_code"]
        primary_blocking_category = primary_row["category"]

    recommended_resolution_steps: list[str] = []
    for step in recommended_runtime_plan_steps:
        if step and step not in recommended_resolution_steps:
            recommended_resolution_steps.append(step)
    if recommended_runtime_action and recommended_runtime_action not in recommended_resolution_steps:
        recommended_resolution_steps.append(recommended_runtime_action)
    if recommended_next_command:
        recommended_resolution_steps.append(f"Run: {recommended_next_command}")
    for row in sorted(
        blocking_reason_summary_rows,
        key=lambda row: (-int(row.get("count") or 0), str(row.get("reason_code") or "")),
    ):
        action = str(row.get("recommended_action") or "").strip()
        if action and action not in recommended_resolution_steps:
            recommended_resolution_steps.append(action)

    report = {
        "scenario_runtime_backend_probe_set_report_schema_version": SCENARIO_RUNTIME_BACKEND_PROBE_SET_REPORT_SCHEMA_VERSION_V0,
        "generated_at": _utc_now(),
        "probe_set_id": probe_set_id,
        "description": spec.get("description"),
        "status": "PASS" if not failed_probe_ids else "FAIL",
        "probe_count": len(probe_results),
        "pass_count": len(passed_probe_ids),
        "fail_count": len(failed_probe_ids),
        "passed_probe_ids": passed_probe_ids,
        "failed_probe_ids": failed_probe_ids,
        "runtime_native_ready_probe_ids": runtime_native_ready_probe_ids,
        "supplemental_dependency_probe_ids": supplemental_dependency_probe_ids,
        "status_counts": status_counts,
        "runtime_strategy_counts": runtime_strategy_counts,
        "runtime_strategy_probe_ids": {
            strategy: sorted(probe_ids)
            for strategy, probe_ids in sorted(runtime_strategy_probe_ids.items())
        },
        "runtime_strategy_summary_rows": runtime_strategy_summary_rows,
        "runtime_strategy_plan_rows": runtime_strategy_plan_rows,
        "runtime_strategy_reason_code_counts": runtime_strategy_reason_code_counts,
        "runtime_strategy_recommended_command_counts": runtime_strategy_recommended_command_counts,
        "runtime_strategy_recommended_command_probe_ids": {
            command: sorted(probe_ids)
            for command, probe_ids in sorted(
                runtime_strategy_recommended_command_probe_ids.items()
            )
        },
        "primary_runtime_strategy": primary_runtime_strategy,
        "recommended_runtime_action": recommended_runtime_action,
        "primary_runtime_plan_id": primary_runtime_plan_id,
        "recommended_runtime_plan_steps": recommended_runtime_plan_steps,
        "blocking_reason_counts": blocking_reason_counts,
        "blocking_reason_probe_ids": {
            reason: sorted(probe_ids)
            for reason, probe_ids in sorted(blocking_reason_probe_ids.items())
        },
        "blocking_reason_category_counts": blocking_reason_category_counts,
        "blocking_reason_category_probe_ids": {
            category: sorted(probe_ids)
            for category, probe_ids in sorted(blocking_reason_category_probe_ids.items())
        },
        "blocking_reason_summary_rows": blocking_reason_summary_rows,
        "primary_blocking_reason_code": primary_blocking_reason_code,
        "primary_blocking_category": primary_blocking_category,
        "recommended_resolution_focus": recommended_resolution_focus,
        "recommended_resolution_steps": recommended_resolution_steps,
        "recommended_next_command": recommended_next_command or None,
        "source_missing_required_topics": sorted(source_missing_required_topics),
        "refreshed_missing_required_topics": sorted(refreshed_missing_required_topics),
        "recovered_required_topics": sorted(recovered_required_topics),
        "probes": probe_results,
        "repo_root": str(resolved_repo_root),
    }

    report_path = out_root / "scenario_runtime_backend_probe_set_report_v0.json"
    markdown_path = out_root / "scenario_runtime_backend_probe_set_report_v0.md"
    _write_json(report_path, report)
    markdown_path.write_text(_build_markdown_report(report), encoding="utf-8")
    return {
        "report_path": report_path,
        "markdown_path": markdown_path,
        "report": report,
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_scenario_runtime_backend_probe_set(
        out_root=Path(args.out_root).resolve(),
        probe_set_id=args.probe_set_id,
        repo_root=args.repo_root or None,
        autoware_base_frame=args.autoware_base_frame,
        autoware_strict=bool(args.autoware_strict),
        run_history_guard=bool(args.run_history_guard),
        history_guard_metadata_root=args.history_guard_metadata_root or None,
        history_guard_current_repo_root=args.history_guard_current_repo_root or None,
        history_guard_compare_ref=args.history_guard_compare_ref,
        history_guard_include_untracked=bool(args.history_guard_include_untracked),
    )
    report = dict(result.get("report", {}))
    status = str(report.get("status", "")).strip() or "UNKNOWN"
    print(f"[ok] probe_set_status={status}")
    print(f"[ok] report={result['report_path']}")
    return 0 if status == "PASS" else 2


__all__ = [
    "DEFAULT_SCENARIO_RUNTIME_BACKEND_PROBE_SET_ID",
    "SCENARIO_RUNTIME_BACKEND_PROBE_SET_REPORT_SCHEMA_VERSION_V0",
    "run_scenario_runtime_backend_probe_set",
    "main",
]
