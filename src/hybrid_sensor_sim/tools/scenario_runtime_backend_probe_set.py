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
        "",
        "## Probes",
        "",
        "| Probe | Status | Runtime | Autoware | Consumer | Semantic Recovered |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for probe in probe_rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(probe.get("probe_id") or "-"),
                    str(probe.get("status") or "-"),
                    str(probe.get("runtime_status") or "-"),
                    str(probe.get("autoware_pipeline_status") or "-"),
                    str(probe.get("consumer_profile_id") or "-"),
                    str(bool(probe.get("semantic_topic_recovered"))),
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
        probe_results.append(
            {
                "probe_id": probe_report.get("probe_id"),
                "status": probe_report.get("status"),
                "consumer_profile_id": probe_report.get("consumer_profile_id"),
                "runtime_status": probe_summary.get("runtime_status"),
                "autoware_pipeline_status": probe_summary.get(
                    "autoware_pipeline_status"
                ),
                "semantic_topic_recovered": bool(
                    probe_summary.get("semantic_topic_recovered")
                ),
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
    status_counts: dict[str, int] = {}
    for result in probe_results:
        status_key = str(result.get("status") or "UNKNOWN")
        status_counts[status_key] = status_counts.get(status_key, 0) + 1

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
        "status_counts": status_counts,
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
