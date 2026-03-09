from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.tools.scenario_runtime_backend_rebridge import (
    run_scenario_runtime_backend_rebridge,
)

SCENARIO_RUNTIME_BACKEND_PROBE_REPORT_SCHEMA_VERSION_V0 = (
    "scenario_runtime_backend_probe_report_v0"
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a compact probe report from an existing "
            "scenario_runtime_backend_workflow_report_v0.json by re-bridging it."
        )
    )
    parser.add_argument(
        "--runtime-backend-workflow-report",
        required=True,
        help="Path to scenario_runtime_backend_workflow_report_v0.json",
    )
    parser.add_argument(
        "--out-root",
        required=True,
        help="Output directory for the probe report",
    )
    parser.add_argument(
        "--probe-id",
        default="",
        help="Optional explicit probe identifier",
    )
    parser.add_argument(
        "--consumer-profile",
        default="",
        help="Optional Autoware consumer profile override",
    )
    parser.add_argument(
        "--autoware-base-frame",
        default="base_link",
        help="Base frame for rebridge",
    )
    parser.add_argument(
        "--autoware-strict",
        action="store_true",
        help="Fail Autoware bridge if required outputs are missing",
    )
    parser.add_argument(
        "--expect-runtime-status",
        default="",
        help="Optional expected refreshed runtime status",
    )
    parser.add_argument(
        "--expect-autoware-status",
        default="",
        help="Optional expected refreshed Autoware pipeline status",
    )
    parser.add_argument(
        "--run-history-guard",
        action="store_true",
        help="Run provenance guard during rebridge",
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


def _normalized_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _build_probe_summary(
    *,
    rebridge_report: dict[str, Any],
) -> dict[str, Any]:
    status_summary = dict(rebridge_report.get("status_summary", {}))
    backend_section = dict(rebridge_report.get("backend_smoke_workflow", {}))
    smoke_section = dict(backend_section.get("smoke", {}))
    comparison = dict(rebridge_report.get("rebridge", {}).get("comparison", {}))
    return {
        "runtime_status": _normalized_text(rebridge_report.get("status")),
        "backend_smoke_status": _normalized_text(backend_section.get("status")),
        "backend_output_smoke_status": _normalized_text(
            status_summary.get("backend_output_smoke_status")
        ),
        "backend_output_comparison_status": _normalized_text(
            status_summary.get("backend_output_comparison_status")
        ),
        "backend_output_origin_status": _normalized_text(
            status_summary.get("backend_output_origin_status")
        ),
        "autoware_pipeline_status": _normalized_text(
            status_summary.get("autoware_pipeline_status")
        ),
        "autoware_availability_mode": _normalized_text(
            status_summary.get("autoware_availability_mode")
        ),
        "semantic_topic_recovered": bool(
            status_summary.get("autoware_semantic_topic_recovered")
        ),
        "semantic_recovery_source": _normalized_text(
            status_summary.get("autoware_semantic_recovery_source")
        ),
        "source_missing_required_topics": list(
            comparison.get("source_missing_required_topics", []) or []
        ),
        "refreshed_missing_required_topics": list(
            comparison.get("refreshed_missing_required_topics", []) or []
        ),
        "recovered_required_topics": list(
            comparison.get("recovered_required_topics", []) or []
        ),
        "autoware_merged_report_count": status_summary.get(
            "autoware_merged_report_count"
        ),
        "autoware_supplemental_semantic_status": _normalized_text(
            status_summary.get("autoware_supplemental_semantic_status")
        ),
        "topic_catalog_path": backend_section.get("artifacts", {}).get(
            "autoware_topic_catalog_path"
        )
        or rebridge_report.get("artifacts", {}).get("autoware_topic_catalog_path"),
        "consumer_input_manifest_path": backend_section.get("artifacts", {}).get(
            "autoware_consumer_input_manifest_path"
        )
        or rebridge_report.get("artifacts", {}).get(
            "autoware_consumer_input_manifest_path"
        ),
        "smoke_summary_path": smoke_section.get("summary_path"),
    }


def _evaluate_probe(
    *,
    summary: dict[str, Any],
    expect_runtime_status: str,
    expect_autoware_status: str,
) -> dict[str, Any]:
    failures: list[str] = []
    runtime_status = _normalized_text(summary.get("runtime_status"))
    autoware_status = _normalized_text(summary.get("autoware_pipeline_status"))
    expected_runtime = _normalized_text(expect_runtime_status)
    expected_autoware = _normalized_text(expect_autoware_status)
    if expected_runtime and runtime_status != expected_runtime:
        failures.append("RUNTIME_STATUS_MISMATCH")
    if expected_autoware and autoware_status != expected_autoware:
        failures.append("AUTOWARE_STATUS_MISMATCH")
    return {
        "status": "PASS" if not failures else "FAIL",
        "failure_codes": failures,
        "expected_runtime_status": expected_runtime,
        "expected_autoware_status": expected_autoware,
    }


def _build_markdown_report(report: dict[str, Any]) -> str:
    summary = dict(report.get("summary", {}))
    evaluation = dict(report.get("evaluation", {}))
    artifacts = dict(report.get("artifacts", {}))
    return "\n".join(
        [
            "# Scenario Runtime Backend Probe",
            "",
            f"- Probe ID: `{report.get('probe_id') or '-'}`",
            f"- Status: `{report.get('status') or '-'}`",
            f"- Runtime status: `{summary.get('runtime_status') or '-'}`",
            f"- Autoware status: `{summary.get('autoware_pipeline_status') or '-'}`",
            f"- Backend output origin: `{summary.get('backend_output_origin_status') or '-'}`",
            f"- Backend output comparison: `{summary.get('backend_output_comparison_status') or '-'}`",
            f"- Semantic topic recovered: `{summary.get('semantic_topic_recovered')}`",
            f"- Semantic recovery source: `{summary.get('semantic_recovery_source') or '-'}`",
            f"- Recovered required topics: `{', '.join(summary.get('recovered_required_topics', [])) or '-'}`",
            f"- Failure codes: `{', '.join(evaluation.get('failure_codes', [])) or '-'}`",
            "",
            "## Artifacts",
            "",
            f"- Source runtime workflow report: `{artifacts.get('source_runtime_backend_workflow_report_path') or '-'}`",
            f"- Rebridge report: `{artifacts.get('rebridge_report_path') or '-'}`",
            f"- Rebridge markdown: `{artifacts.get('rebridge_markdown_path') or '-'}`",
            f"- Autoware pipeline manifest: `{artifacts.get('autoware_pipeline_manifest_path') or '-'}`",
            f"- Autoware dataset manifest: `{artifacts.get('autoware_dataset_manifest_path') or '-'}`",
        ]
    )


def run_scenario_runtime_backend_probe(
    *,
    runtime_backend_workflow_report_path: str,
    out_root: Path,
    probe_id: str = "",
    consumer_profile_id: str = "",
    autoware_base_frame: str = "base_link",
    autoware_strict: bool = False,
    expect_runtime_status: str = "",
    expect_autoware_status: str = "",
    run_history_guard: bool = False,
    history_guard_metadata_root: str | Path | None = None,
    history_guard_current_repo_root: str | Path | None = None,
    history_guard_compare_ref: str = "origin/main",
    history_guard_include_untracked: bool = False,
) -> dict[str, Any]:
    out_root = out_root.resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    rebridge_result = run_scenario_runtime_backend_rebridge(
        runtime_backend_workflow_report_path=runtime_backend_workflow_report_path,
        backend_smoke_workflow_report_path="",
        batch_workflow_report_path="",
        supplemental_backend_smoke_workflow_report_paths=[],
        out_root=out_root / "rebridge",
        skip_autoware_bridge=False,
        autoware_base_frame=autoware_base_frame,
        autoware_consumer_profile=consumer_profile_id,
        autoware_strict=bool(autoware_strict),
        run_history_guard=bool(run_history_guard),
        history_guard_metadata_root=history_guard_metadata_root,
        history_guard_current_repo_root=history_guard_current_repo_root,
        history_guard_compare_ref=history_guard_compare_ref,
        history_guard_include_untracked=history_guard_include_untracked,
    )
    rebridge_report = dict(rebridge_result.get("workflow_report", {}))
    summary = _build_probe_summary(rebridge_report=rebridge_report)
    evaluation = _evaluate_probe(
        summary=summary,
        expect_runtime_status=expect_runtime_status,
        expect_autoware_status=expect_autoware_status,
    )
    resolved_probe_id = (
        _normalized_text(probe_id)
        or f"{Path(runtime_backend_workflow_report_path).resolve().parent.name}-probe"
    )
    report = {
        "scenario_runtime_backend_probe_report_schema_version": SCENARIO_RUNTIME_BACKEND_PROBE_REPORT_SCHEMA_VERSION_V0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "probe_id": resolved_probe_id,
        "consumer_profile_id": _normalized_text(consumer_profile_id)
        or _normalized_text(
            rebridge_report.get("status_summary", {}).get(
                "autoware_consumer_profile_id"
            )
        )
        or _normalized_text(
            rebridge_report.get("backend_smoke_workflow", {})
            .get("autoware", {})
            .get("consumer_profile_id")
        ),
        "status": evaluation["status"],
        "summary": summary,
        "evaluation": evaluation,
        "artifacts": {
            "source_runtime_backend_workflow_report_path": str(
                Path(runtime_backend_workflow_report_path).resolve()
            ),
            "rebridge_report_path": str(rebridge_result["workflow_report_path"]),
            "rebridge_markdown_path": str(rebridge_result["workflow_markdown_path"]),
            "autoware_pipeline_manifest_path": rebridge_report.get("artifacts", {}).get(
                "autoware_pipeline_manifest_path"
            ),
            "autoware_dataset_manifest_path": rebridge_report.get("artifacts", {}).get(
                "autoware_dataset_manifest_path"
            ),
            "autoware_consumer_input_manifest_path": rebridge_report.get("artifacts", {}).get(
                "autoware_consumer_input_manifest_path"
            ),
            "autoware_topic_catalog_path": rebridge_report.get("artifacts", {}).get(
                "autoware_topic_catalog_path"
            ),
        },
    }
    report_path = out_root / "scenario_runtime_backend_probe_report_v0.json"
    markdown_path = out_root / "scenario_runtime_backend_probe_report_v0.md"
    _write_json(report_path, report)
    markdown_path.write_text(_build_markdown_report(report), encoding="utf-8")
    return {
        "report_path": report_path,
        "markdown_path": markdown_path,
        "report": report,
        "rebridge_result": rebridge_result,
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_scenario_runtime_backend_probe(
        runtime_backend_workflow_report_path=args.runtime_backend_workflow_report,
        out_root=Path(args.out_root).resolve(),
        probe_id=args.probe_id,
        consumer_profile_id=args.consumer_profile,
        autoware_base_frame=args.autoware_base_frame,
        autoware_strict=bool(args.autoware_strict),
        expect_runtime_status=args.expect_runtime_status,
        expect_autoware_status=args.expect_autoware_status,
        run_history_guard=bool(args.run_history_guard),
        history_guard_metadata_root=args.history_guard_metadata_root or None,
        history_guard_current_repo_root=args.history_guard_current_repo_root or None,
        history_guard_compare_ref=args.history_guard_compare_ref,
        history_guard_include_untracked=bool(args.history_guard_include_untracked),
    )
    report = dict(result.get("report", {}))
    status = str(report.get("status", "")).strip()
    print(f"[ok] probe_status={status}")
    print(f"[ok] report={result['report_path']}")
    return 0 if status == "PASS" else 2
