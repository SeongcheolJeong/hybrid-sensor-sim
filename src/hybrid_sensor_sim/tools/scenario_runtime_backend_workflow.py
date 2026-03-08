from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.scenarios.matrix_sweep import (
    _parse_csv_positive_floats,
    _parse_csv_text_items,
    _parse_non_negative_int,
)
from hybrid_sensor_sim.tools.scenario_batch_gate_catalog import (
    resolve_scenario_batch_gate_profile_path,
)
from hybrid_sensor_sim.tools.scenario_backend_smoke_workflow import (
    run_scenario_backend_smoke_workflow,
)
from hybrid_sensor_sim.tools.scenario_batch_workflow import (
    run_scenario_batch_workflow,
)
from hybrid_sensor_sim.tools.autonomy_e2e_history_guard import (
    build_autonomy_e2e_history_guard_report,
)
from hybrid_sensor_sim.tools.scenario_runtime_bridge import DEFAULT_LANE_SPACING_M


SCENARIO_RUNTIME_BACKEND_WORKFLOW_REPORT_SCHEMA_VERSION_V0 = (
    "scenario_runtime_backend_workflow_report_v0"
)


def _default_scenario_language_dir() -> str:
    return str(
        Path(__file__).resolve().parents[3]
        / "tests"
        / "fixtures"
        / "autonomy_e2e"
        / "p_validation"
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run scenario batch workflow and feed the selected result into renderer backend smoke."
    )
    parser.add_argument("--logical-scenarios", default="", help="Path to logical scenario JSON file")
    parser.add_argument(
        "--scenario-language-profile",
        default="",
        help="Scenario language profile ID under scenario language directory (without .json)",
    )
    parser.add_argument("--scenario-language-dir", default="", help="Scenario language profile directory")
    parser.add_argument("--matrix-scenario", required=True, help="Scenario JSON path for matrix sweep")
    parser.add_argument("--smoke-config", required=True, help="Base renderer backend smoke JSON config")
    parser.add_argument("--backend", choices=("awsim", "carla"), required=True, help="Renderer backend")
    parser.add_argument("--out-root", required=True, help="Workflow output root")
    parser.add_argument("--sampling", choices=["full", "random"], default="full")
    parser.add_argument("--sample-size", type=int, default=0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-variants-per-scenario", type=int, default=1000)
    parser.add_argument("--execution-max-variants", type=int, default=0)
    parser.add_argument("--sds-version", default="sds_unknown", help="SDS version identifier")
    parser.add_argument("--sim-version", default="sim_engine_v0_prototype", help="Simulation version identifier")
    parser.add_argument("--fidelity-profile", default="dev-fast", help="Fidelity profile")
    parser.add_argument("--matrix-run-id-prefix", default="RUN_CORE_SIM_SWEEP", help="Run ID prefix for matrix cases")
    parser.add_argument(
        "--traffic-profile-ids",
        default="sumo_highway_aggressive_v0,sumo_highway_balanced_v0",
        help="Comma-separated traffic profile IDs",
    )
    parser.add_argument(
        "--traffic-actor-pattern-ids",
        default="sumo_platoon_sparse_v0,sumo_platoon_balanced_v0,sumo_dense_aggressive_v0",
        help="Comma-separated traffic actor-pattern IDs",
    )
    parser.add_argument(
        "--traffic-npc-speed-scale-values",
        default="0.9,1.0,1.1",
        help="Comma-separated positive traffic_npc_speed_scale values",
    )
    parser.add_argument(
        "--tire-friction-coeff-values",
        default="0.4,0.7,1.0",
        help="Comma-separated positive tire_friction_coeff values",
    )
    parser.add_argument(
        "--surface-friction-scale-values",
        default="0.8,1.0",
        help="Comma-separated positive surface_friction_scale values",
    )
    parser.add_argument("--enable-ego-collision-avoidance", action="store_true")
    parser.add_argument("--avoidance-ttc-threshold-sec", default="2.5")
    parser.add_argument("--ego-max-brake-mps2", default="6.0")
    parser.add_argument("--max-cases", default="0")
    parser.add_argument("--gate-profile", default="")
    parser.add_argument("--gate-profile-id", default="")
    parser.add_argument("--gate-profile-dir", default="")
    parser.add_argument("--gate-max-attention-rows", default="")
    parser.add_argument("--gate-max-collision-rows", default="")
    parser.add_argument("--gate-max-timeout-rows", default="")
    parser.add_argument("--gate-max-path-conflict-rows", default="")
    parser.add_argument("--gate-max-merge-conflict-rows", default="")
    parser.add_argument("--gate-max-lane-change-conflict-rows", default="")
    parser.add_argument("--gate-max-avoidance-rows", default="")
    parser.add_argument("--gate-max-avoidance-brake-events", default="")
    parser.add_argument("--gate-max-avoidance-same-lane-conflict-triggers", default="")
    parser.add_argument("--gate-max-avoidance-merge-conflict-triggers", default="")
    parser.add_argument("--gate-max-avoidance-lane-change-conflict-triggers", default="")
    parser.add_argument("--gate-max-avoidance-downstream-route-conflict-triggers", default="")
    parser.add_argument("--gate-min-min-ttc-any-lane-sec", default="")
    parser.add_argument("--gate-min-min-ttc-path-conflict-sec", default="")
    parser.add_argument(
        "--selection-strategy",
        choices=("first_successful_variant", "worst_logical_scenario", "variant_id"),
        default="worst_logical_scenario",
        help="How to choose the variant to bridge into backend smoke",
    )
    parser.add_argument("--selected-variant-id", default="", help="Variant ID for selection_strategy=variant_id")
    parser.add_argument("--lane-spacing-m", type=float, default=DEFAULT_LANE_SPACING_M, help="Lane center spacing used in smoke scenario translation")
    parser.add_argument("--smoke-output-dir", default="", help="Override smoke output directory")
    parser.add_argument("--setup-summary", default="", help="Optional renderer_backend_local_setup.json used to resolve backend runtime selection")
    parser.add_argument("--backend-workflow-summary", default="", help="Optional renderer_backend_workflow_summary.json used to resolve backend runtime selection")
    parser.add_argument("--backend-bin", default="", help="Forwarded to renderer backend smoke")
    parser.add_argument("--renderer-map", default="", help="Forwarded to renderer backend smoke")
    parser.add_argument("--set-option", action="append", default=[], help="Forwarded to renderer backend smoke")
    parser.add_argument("--skip-smoke", action="store_true", help="Run batch workflow and bridge only")
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


def _parse_optional_non_negative_int(raw: Any, *, field: str) -> int | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except ValueError as exc:
        raise ValueError(f"{field} must be an integer, got: {raw}") from exc
    if parsed < 0:
        raise ValueError(f"{field} must be >= 0, got: {parsed}")
    return parsed


def _parse_optional_non_negative_float(raw: Any, *, field: str) -> float | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        parsed = float(text)
    except ValueError as exc:
        raise ValueError(f"{field} must be a number, got: {raw}") from exc
    if parsed < 0.0:
        raise ValueError(f"{field} must be >= 0, got: {parsed}")
    return parsed


def _build_workflow_status(
    *,
    batch_status: str,
    backend_status: str,
    history_guard_status: str | None,
) -> str:
    if history_guard_status == "FAIL":
        return "FAILED"
    if backend_status == "SMOKE_FAILED":
        return "FAILED"
    if batch_status == "FAILED":
        return "FAILED"
    if batch_status == "ATTENTION":
        return "ATTENTION"
    if backend_status == "BRIDGED_ONLY":
        return "BRIDGED_ONLY"
    return "SUCCEEDED"


def _build_status_summary(
    *,
    workflow_status: str,
    batch_report: dict[str, Any],
    backend_report: dict[str, Any],
    history_guard_report: dict[str, Any] | None,
) -> dict[str, Any]:
    decision_trace = [
        {
            "step_id": "history_guard",
            "matched": (
                isinstance(history_guard_report, dict)
                and str(history_guard_report.get("status", "")).strip() == "FAIL"
            ),
            "status_if_matched": "FAILED",
            "reason_code": "AUTONOMY_E2E_HISTORY_GUARD_FAILED",
        },
        {
            "step_id": "backend_smoke_status",
            "matched": backend_report["status"] == "SMOKE_FAILED",
            "status_if_matched": "FAILED",
            "reason_code": "BACKEND_SMOKE_FAILED",
        },
        {
            "step_id": "batch_workflow_status",
            "matched": batch_report["status"] == "FAILED",
            "status_if_matched": "FAILED",
            "reason_code": "BATCH_WORKFLOW_FAILED",
        },
        {
            "step_id": "batch_attention",
            "matched": batch_report["status"] == "ATTENTION",
            "status_if_matched": "ATTENTION",
            "reason_code": "BATCH_WORKFLOW_ATTENTION",
        },
        {
            "step_id": "smoke_skipped",
            "matched": backend_report["status"] == "BRIDGED_ONLY",
            "status_if_matched": "BRIDGED_ONLY",
            "reason_code": "BACKEND_SMOKE_SKIPPED",
        },
    ]
    final_status_source = "default_success"
    status_reason_codes: list[str] = []
    for step in decision_trace:
        if step["matched"]:
            status_reason_codes.append(str(step["reason_code"]))
            if final_status_source == "default_success":
                final_status_source = str(step["step_id"])
    batch_status_summary = dict(batch_report.get("status_summary", {}))
    worst_logical_row = dict(batch_status_summary.get("worst_logical_scenario_row", {}))
    smoke_summary = dict(backend_report.get("smoke", {}).get("summary", {}))
    history_guard_summary = dict(history_guard_report or {})
    return {
        "final_status_source": final_status_source,
        "decision_trace": decision_trace,
        "status_reason_codes": status_reason_codes,
        "worst_logical_scenario_id": worst_logical_row.get("logical_scenario_id"),
        "batch_gate_failure_codes": list(batch_status_summary.get("gate_failure_codes", [])),
        "batch_failing_logical_scenario_ids": list(batch_status_summary.get("failing_logical_scenario_ids", [])),
        "batch_attention_logical_scenario_ids": list(batch_status_summary.get("attention_logical_scenario_ids", [])),
        "backend_variant_id": backend_report.get("selection", {}).get("variant_id"),
        "backend_output_comparison_status": smoke_summary.get("output_comparison_status"),
        "backend_output_comparison_mismatch_reasons": list(
            smoke_summary.get("output_comparison_mismatch_reasons", [])
        ),
        "backend_output_comparison_unexpected_output_count": smoke_summary.get(
            "output_comparison_unexpected_output_count"
        ),
        "backend_output_smoke_status": smoke_summary.get("output_smoke_status"),
        "backend_output_smoke_coverage_ratio": smoke_summary.get(
            "output_smoke_coverage_ratio"
        ),
        "backend_output_inspection_status": smoke_summary.get("output_inspection_status"),
        "backend_runner_smoke_status": smoke_summary.get("runner_smoke_status"),
        "backend_run_status": smoke_summary.get("run_status"),
        "history_guard_status": history_guard_summary.get("status"),
        "history_guard_failure_codes": list(history_guard_summary.get("failure_codes", [])),
        "history_guard_impacted_block_ids": list(
            history_guard_summary.get("impacted_block_ids", [])
        ),
        "workflow_status": workflow_status,
    }


def _build_markdown_report(workflow_report: dict[str, Any]) -> str:
    batch = workflow_report["batch_workflow"]
    backend = workflow_report["backend_smoke_workflow"]
    summary = workflow_report["status_summary"]
    lines = [
        "# Scenario Runtime Backend Workflow",
        "",
        f"- Status: `{workflow_report['status']}`",
        f"- Backend: `{workflow_report['backend']}`",
        f"- Generated at: `{workflow_report['generated_at']}`",
        f"- Final status source: `{summary['final_status_source']}`",
        f"- Status reasons: `{', '.join(summary['status_reason_codes']) or '-'}`",
        f"- History guard: `{summary.get('history_guard_status') or '-'}`",
        "",
        "## Batch Workflow",
        "",
        f"- Status: `{batch['status']}`",
        f"- Report: `{batch['workflow_report_path']}`",
        f"- Worst logical scenario: `{summary['worst_logical_scenario_id'] or '-'}`",
        f"- Failing logical scenarios: `{', '.join(summary['batch_failing_logical_scenario_ids']) or '-'}`",
        f"- Attention logical scenarios: `{', '.join(summary['batch_attention_logical_scenario_ids']) or '-'}`",
        "",
        "## Backend Smoke Workflow",
        "",
        f"- Status: `{backend['status']}`",
        f"- Report: `{backend['workflow_report_path']}`",
        f"- Variant ID: `{summary['backend_variant_id'] or '-'}`",
        f"- Output smoke: `{summary['backend_output_smoke_status'] or '-'}`",
        f"- Output smoke coverage ratio: `{summary['backend_output_smoke_coverage_ratio'] if summary['backend_output_smoke_coverage_ratio'] is not None else '-'}`",
        f"- Output comparison: `{summary['backend_output_comparison_status'] or '-'}`",
        f"- Output comparison mismatch reasons: `{', '.join(summary['backend_output_comparison_mismatch_reasons']) or '-'}`",
        f"- Output comparison unexpected count: `{summary['backend_output_comparison_unexpected_output_count'] if summary['backend_output_comparison_unexpected_output_count'] is not None else '-'}`",
        f"- Output inspection: `{summary['backend_output_inspection_status'] or '-'}`",
        f"- Runner smoke: `{summary['backend_runner_smoke_status'] or '-'}`",
        f"- Run status: `{summary['backend_run_status'] or '-'}`",
        "",
        "## Provenance Guard",
        "",
        f"- Requested: `{workflow_report['history_guard']['requested']}`",
        f"- Status: `{workflow_report['history_guard']['status'] or '-'}`",
        f"- Failure codes: `{', '.join(workflow_report['history_guard'].get('failure_codes', [])) or '-'}`",
        f"- Report: `{workflow_report['artifacts'].get('history_guard_report_path') or '-'}`",
        "",
        "## Artifacts",
        "",
        f"- Batch workflow report: `{workflow_report['artifacts']['batch_workflow_report_path']}`",
        f"- Backend smoke workflow report: `{workflow_report['artifacts']['backend_smoke_workflow_report_path']}`",
        f"- Smoke scenario: `{workflow_report['artifacts']['smoke_scenario_path']}`",
        f"- Smoke input config: `{workflow_report['artifacts']['smoke_input_config_path']}`",
        f"- History guard report: `{workflow_report['artifacts'].get('history_guard_report_path') or '-'}`",
        "",
    ]
    return "\n".join(lines)


def run_scenario_runtime_backend_workflow(
    *,
    logical_scenarios_path: str,
    scenario_language_profile: str,
    scenario_language_dir: str | Path | None,
    matrix_scenario_path: Path,
    smoke_config_path: Path,
    backend: str,
    out_root: Path,
    sampling: str,
    sample_size: int,
    seed: int,
    max_variants_per_scenario: int,
    execution_max_variants: int,
    sds_version: str,
    sim_version: str,
    fidelity_profile: str,
    matrix_run_id_prefix: str,
    traffic_profile_ids: list[str],
    traffic_actor_pattern_ids: list[str],
    traffic_npc_speed_scale_values: list[float],
    tire_friction_coeff_values: list[float],
    surface_friction_scale_values: list[float],
    enable_ego_collision_avoidance: bool,
    avoidance_ttc_threshold_sec: float,
    ego_max_brake_mps2: float,
    max_cases: int,
    gate_profile_path: Path | None = None,
    gate_max_attention_rows: int | None = None,
    gate_max_collision_rows: int | None = None,
    gate_max_timeout_rows: int | None = None,
    gate_max_path_conflict_rows: int | None = None,
    gate_max_merge_conflict_rows: int | None = None,
    gate_max_lane_change_conflict_rows: int | None = None,
    gate_max_avoidance_rows: int | None = None,
    gate_max_avoidance_brake_events: int | None = None,
    gate_max_avoidance_same_lane_conflict_triggers: int | None = None,
    gate_max_avoidance_merge_conflict_triggers: int | None = None,
    gate_max_avoidance_lane_change_conflict_triggers: int | None = None,
    gate_max_avoidance_downstream_route_conflict_triggers: int | None = None,
    gate_min_min_ttc_any_lane_sec: float | None = None,
    gate_min_min_ttc_path_conflict_sec: float | None = None,
    selection_strategy: str,
    selected_variant_id: str,
    lane_spacing_m: float,
    smoke_output_dir: str,
    setup_summary_path: str,
    backend_workflow_summary_path: str,
    backend_bin: str,
    renderer_map: str,
    option_overrides: list[str],
    skip_smoke: bool,
    run_history_guard: bool = False,
    history_guard_metadata_root: str | Path | None = None,
    history_guard_current_repo_root: str | Path | None = None,
    history_guard_compare_ref: str = "origin/main",
    history_guard_include_untracked: bool = False,
) -> dict[str, Any]:
    out_root = out_root.resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    batch_root = out_root / "batch_workflow"
    backend_root = out_root / "backend_smoke_workflow"
    markdown_path = out_root / "scenario_runtime_backend_workflow_report_v0.md"

    batch_result = run_scenario_batch_workflow(
        logical_scenarios_path=logical_scenarios_path,
        scenario_language_profile=scenario_language_profile,
        scenario_language_dir=scenario_language_dir,
        matrix_scenario_path=matrix_scenario_path.resolve(),
        out_root=batch_root,
        sampling=sampling,
        sample_size=sample_size,
        seed=seed,
        max_variants_per_scenario=max_variants_per_scenario,
        execution_max_variants=execution_max_variants,
        sds_version=sds_version,
        sim_version=sim_version,
        fidelity_profile=fidelity_profile,
        matrix_run_id_prefix=matrix_run_id_prefix,
        traffic_profile_ids=traffic_profile_ids,
        traffic_actor_pattern_ids=traffic_actor_pattern_ids,
        traffic_npc_speed_scale_values=traffic_npc_speed_scale_values,
        tire_friction_coeff_values=tire_friction_coeff_values,
        surface_friction_scale_values=surface_friction_scale_values,
        enable_ego_collision_avoidance=enable_ego_collision_avoidance,
        avoidance_ttc_threshold_sec=avoidance_ttc_threshold_sec,
        ego_max_brake_mps2=ego_max_brake_mps2,
        max_cases=max_cases,
        gate_profile_path=gate_profile_path,
        gate_max_attention_rows=gate_max_attention_rows,
        gate_max_collision_rows=gate_max_collision_rows,
        gate_max_timeout_rows=gate_max_timeout_rows,
        gate_max_path_conflict_rows=gate_max_path_conflict_rows,
        gate_max_merge_conflict_rows=gate_max_merge_conflict_rows,
        gate_max_lane_change_conflict_rows=gate_max_lane_change_conflict_rows,
        gate_max_avoidance_rows=gate_max_avoidance_rows,
        gate_max_avoidance_brake_events=gate_max_avoidance_brake_events,
        gate_max_avoidance_same_lane_conflict_triggers=gate_max_avoidance_same_lane_conflict_triggers,
        gate_max_avoidance_merge_conflict_triggers=gate_max_avoidance_merge_conflict_triggers,
        gate_max_avoidance_lane_change_conflict_triggers=gate_max_avoidance_lane_change_conflict_triggers,
        gate_max_avoidance_downstream_route_conflict_triggers=gate_max_avoidance_downstream_route_conflict_triggers,
        gate_min_min_ttc_any_lane_sec=gate_min_min_ttc_any_lane_sec,
        gate_min_min_ttc_path_conflict_sec=gate_min_min_ttc_path_conflict_sec,
    )
    backend_result = run_scenario_backend_smoke_workflow(
        variant_workflow_report_path="",
        batch_workflow_report_path=str(batch_result["workflow_report_path"]),
        smoke_config_path=smoke_config_path.resolve(),
        backend=backend,
        out_root=backend_root,
        selection_strategy=selection_strategy,
        selected_variant_id=selected_variant_id,
        lane_spacing_m=lane_spacing_m,
        smoke_output_dir=smoke_output_dir,
        setup_summary_path=setup_summary_path,
        backend_workflow_summary_path=backend_workflow_summary_path,
        backend_bin=backend_bin,
        renderer_map=renderer_map,
        option_overrides=option_overrides,
        skip_smoke=skip_smoke,
    )

    batch_report = batch_result["workflow_report"]
    backend_report = backend_result["workflow_report"]
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
    workflow_status = _build_workflow_status(
        batch_status=str(batch_report["status"]),
        backend_status=str(backend_report["status"]),
        history_guard_status=(
            str(history_guard_report.get("status", "")).strip()
            if isinstance(history_guard_report, dict)
            else None
        ),
    )

    workflow_report = {
        "scenario_runtime_backend_workflow_report_schema_version": SCENARIO_RUNTIME_BACKEND_WORKFLOW_REPORT_SCHEMA_VERSION_V0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "out_root": str(out_root),
        "backend": backend,
        "status": workflow_status,
        "sampling": sampling,
        "sample_size": int(sample_size),
        "seed": int(seed),
        "selection_strategy": selection_strategy,
        "skip_smoke": bool(skip_smoke),
        "batch_workflow": {
            "status": batch_report["status"],
            "workflow_report_path": str(Path(batch_result["workflow_report_path"]).resolve()),
            "workflow_markdown_path": str(Path(batch_result["workflow_markdown_path"]).resolve()),
            "status_summary": {
                "worst_logical_scenario_row": dict(
                    batch_report.get("status_summary", {}).get("worst_logical_scenario_row", {})
                ),
                "gate_failure_codes": list(
                    batch_report.get("status_summary", {}).get("gate_failure_codes", [])
                ),
                "status_reason_codes": list(
                    batch_report.get("status_summary", {}).get("status_reason_codes", [])
                ),
            },
        },
        "backend_smoke_workflow": {
            "status": backend_report["status"],
            "workflow_report_path": str(Path(backend_result["workflow_report_path"]).resolve()),
            "selection": dict(backend_report.get("selection", {})),
            "runtime_selection": dict(backend_report.get("runtime_selection", {})),
            "bridge": {
                "source_kind": backend_report.get("bridge", {}).get("source_payload_kind"),
                "source_path": backend_report.get("bridge", {}).get("source_payload_path"),
                "smoke_scenario_name": backend_report.get("bridge", {}).get("smoke_scenario_name"),
                "object_count": backend_report.get("bridge", {}).get("object_count"),
            },
            "smoke": dict(backend_report.get("smoke", {})),
        },
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
        "artifacts": {
            "batch_workflow_report_path": str(Path(batch_result["workflow_report_path"]).resolve()),
            "backend_smoke_workflow_report_path": str(Path(backend_result["workflow_report_path"]).resolve()),
            "smoke_scenario_path": str(Path(backend_report["artifacts"]["smoke_scenario_path"]).resolve()),
            "smoke_input_config_path": str(Path(backend_report["artifacts"]["smoke_input_config_path"]).resolve()),
            "history_guard_report_path": (
                str(history_guard_report_path.resolve())
                if history_guard_report_path is not None
                else None
            ),
            "workflow_markdown_path": str(markdown_path.resolve()),
        },
    }
    workflow_report["status_summary"] = _build_status_summary(
        workflow_status=workflow_status,
        batch_report=batch_report,
        backend_report=backend_report,
        history_guard_report=history_guard_report,
    )

    report_path = out_root / "scenario_runtime_backend_workflow_report_v0.json"
    report_path.write_text(
        json.dumps(workflow_report, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(_build_markdown_report(workflow_report), encoding="utf-8")
    return {
        "workflow_report_path": report_path,
        "workflow_markdown_path": markdown_path,
        "workflow_report": workflow_report,
        "batch_result": batch_result,
        "backend_result": backend_result,
    }


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        scenario_language_dir = args.scenario_language_dir or _default_scenario_language_dir()
        gate_profile_path = resolve_scenario_batch_gate_profile_path(
            profile_path_text=args.gate_profile,
            profile_id_text=args.gate_profile_id,
            profile_dir_text=args.gate_profile_dir,
        )
        result = run_scenario_runtime_backend_workflow(
            logical_scenarios_path=args.logical_scenarios,
            scenario_language_profile=args.scenario_language_profile,
            scenario_language_dir=scenario_language_dir,
            matrix_scenario_path=Path(args.matrix_scenario).resolve(),
            smoke_config_path=Path(args.smoke_config).resolve(),
            backend=args.backend,
            out_root=Path(args.out_root).resolve(),
            sampling=args.sampling,
            sample_size=int(args.sample_size),
            seed=int(args.seed),
            max_variants_per_scenario=int(args.max_variants_per_scenario),
            execution_max_variants=int(args.execution_max_variants),
            sds_version=args.sds_version,
            sim_version=args.sim_version,
            fidelity_profile=args.fidelity_profile,
            matrix_run_id_prefix=args.matrix_run_id_prefix,
            traffic_profile_ids=_parse_csv_text_items(args.traffic_profile_ids, field="traffic_profile_ids"),
            traffic_actor_pattern_ids=_parse_csv_text_items(
                args.traffic_actor_pattern_ids,
                field="traffic_actor_pattern_ids",
            ),
            traffic_npc_speed_scale_values=_parse_csv_positive_floats(
                args.traffic_npc_speed_scale_values,
                field="traffic_npc_speed_scale_values",
            ),
            tire_friction_coeff_values=_parse_csv_positive_floats(
                args.tire_friction_coeff_values,
                field="tire_friction_coeff_values",
            ),
            surface_friction_scale_values=_parse_csv_positive_floats(
                args.surface_friction_scale_values,
                field="surface_friction_scale_values",
            ),
            enable_ego_collision_avoidance=bool(args.enable_ego_collision_avoidance),
            avoidance_ttc_threshold_sec=float(args.avoidance_ttc_threshold_sec),
            ego_max_brake_mps2=float(args.ego_max_brake_mps2),
            max_cases=_parse_non_negative_int(args.max_cases, field="max_cases"),
            gate_profile_path=gate_profile_path,
            gate_max_attention_rows=_parse_optional_non_negative_int(
                args.gate_max_attention_rows,
                field="gate_max_attention_rows",
            ),
            gate_max_collision_rows=_parse_optional_non_negative_int(
                args.gate_max_collision_rows,
                field="gate_max_collision_rows",
            ),
            gate_max_timeout_rows=_parse_optional_non_negative_int(
                args.gate_max_timeout_rows,
                field="gate_max_timeout_rows",
            ),
            gate_max_path_conflict_rows=_parse_optional_non_negative_int(
                args.gate_max_path_conflict_rows,
                field="gate_max_path_conflict_rows",
            ),
            gate_max_merge_conflict_rows=_parse_optional_non_negative_int(
                args.gate_max_merge_conflict_rows,
                field="gate_max_merge_conflict_rows",
            ),
            gate_max_lane_change_conflict_rows=_parse_optional_non_negative_int(
                args.gate_max_lane_change_conflict_rows,
                field="gate_max_lane_change_conflict_rows",
            ),
            gate_max_avoidance_rows=_parse_optional_non_negative_int(
                args.gate_max_avoidance_rows,
                field="gate_max_avoidance_rows",
            ),
            gate_max_avoidance_brake_events=_parse_optional_non_negative_int(
                args.gate_max_avoidance_brake_events,
                field="gate_max_avoidance_brake_events",
            ),
            gate_max_avoidance_same_lane_conflict_triggers=_parse_optional_non_negative_int(
                args.gate_max_avoidance_same_lane_conflict_triggers,
                field="gate_max_avoidance_same_lane_conflict_triggers",
            ),
            gate_max_avoidance_merge_conflict_triggers=_parse_optional_non_negative_int(
                args.gate_max_avoidance_merge_conflict_triggers,
                field="gate_max_avoidance_merge_conflict_triggers",
            ),
            gate_max_avoidance_lane_change_conflict_triggers=_parse_optional_non_negative_int(
                args.gate_max_avoidance_lane_change_conflict_triggers,
                field="gate_max_avoidance_lane_change_conflict_triggers",
            ),
            gate_max_avoidance_downstream_route_conflict_triggers=_parse_optional_non_negative_int(
                args.gate_max_avoidance_downstream_route_conflict_triggers,
                field="gate_max_avoidance_downstream_route_conflict_triggers",
            ),
            gate_min_min_ttc_any_lane_sec=_parse_optional_non_negative_float(
                args.gate_min_min_ttc_any_lane_sec,
                field="gate_min_min_ttc_any_lane_sec",
            ),
            gate_min_min_ttc_path_conflict_sec=_parse_optional_non_negative_float(
                args.gate_min_min_ttc_path_conflict_sec,
                field="gate_min_min_ttc_path_conflict_sec",
            ),
            selection_strategy=args.selection_strategy,
            selected_variant_id=args.selected_variant_id,
            lane_spacing_m=float(args.lane_spacing_m),
            smoke_output_dir=args.smoke_output_dir,
            setup_summary_path=args.setup_summary,
            backend_workflow_summary_path=args.backend_workflow_summary,
            backend_bin=args.backend_bin,
            renderer_map=args.renderer_map,
            option_overrides=list(args.set_option),
            skip_smoke=bool(args.skip_smoke),
            run_history_guard=bool(args.run_history_guard),
            history_guard_metadata_root=args.history_guard_metadata_root,
            history_guard_current_repo_root=args.history_guard_current_repo_root,
            history_guard_compare_ref=args.history_guard_compare_ref,
            history_guard_include_untracked=bool(args.history_guard_include_untracked),
        )
        workflow_report = result["workflow_report"]
        print(f"[ok] status={workflow_report['status']}")
        print(f"[ok] batch_status={workflow_report['batch_workflow']['status']}")
        print(f"[ok] backend_smoke_status={workflow_report['backend_smoke_workflow']['status']}")
        print(f"[ok] report={result['workflow_report_path']}")
        return 0 if workflow_report["status"] in {"SUCCEEDED", "ATTENTION", "BRIDGED_ONLY"} else 2
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] scenario_runtime_backend_workflow.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
