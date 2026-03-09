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
    runtime_strategy_reason_code_counts: dict[str, int] = {}
    runtime_strategy_recommended_command_counts: dict[str, int] = {}
    runtime_strategy_recommended_command_probe_ids: dict[str, list[str]] = {}
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
        for reason_code in result.get("backend_runtime_strategy_reason_codes", []) or []:
            reason_code_str = str(reason_code)
            runtime_strategy_reason_code_counts[reason_code_str] = (
                runtime_strategy_reason_code_counts.get(reason_code_str, 0) + 1
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
        "runtime_strategy_reason_code_counts": runtime_strategy_reason_code_counts,
        "runtime_strategy_recommended_command_counts": runtime_strategy_recommended_command_counts,
        "runtime_strategy_recommended_command_probe_ids": {
            command: sorted(probe_ids)
            for command, probe_ids in sorted(
                runtime_strategy_recommended_command_probe_ids.items()
            )
        },
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
