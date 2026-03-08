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
    run_scenario_matrix_sweep,
)
from hybrid_sensor_sim.tools.scenario_batch_comparison import (
    build_scenario_batch_comparison_report,
)
from hybrid_sensor_sim.tools.scenario_variant_workflow import (
    run_scenario_variant_workflow,
)


SCENARIO_BATCH_WORKFLOW_REPORT_SCHEMA_VERSION_V0 = "scenario_batch_workflow_report_v0"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run scenario variant workflow, matrix sweep, and cross-batch comparison in one workflow."
    )
    parser.add_argument("--logical-scenarios", default="", help="Path to logical scenario JSON file")
    parser.add_argument(
        "--scenario-language-profile",
        default="",
        help="Scenario language profile ID under scenario language directory (without .json)",
    )
    parser.add_argument("--scenario-language-dir", default="", help="Scenario language profile directory")
    parser.add_argument("--matrix-scenario", required=True, help="Scenario JSON path for matrix sweep")
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
    parser.add_argument("--fail-on-attention", action="store_true")
    parser.add_argument("--gate-max-attention-rows", default="")
    parser.add_argument("--gate-max-collision-rows", default="")
    parser.add_argument("--gate-max-timeout-rows", default="")
    parser.add_argument("--gate-min-min-ttc-any-lane-sec", default="")
    return parser.parse_args(argv)


def _default_scenario_language_dir() -> str:
    return str(
        Path(__file__).resolve().parents[3]
        / "tests"
        / "fixtures"
        / "autonomy_e2e"
        / "p_validation"
    )


def _build_workflow_status(
    *,
    variant_workflow_report: dict[str, Any],
    matrix_sweep_report: dict[str, Any],
    comparison_report: dict[str, Any],
) -> str:
    if variant_workflow_report["execution_status_counts"].get("FAILED", 0) > 0:
        return "FAILED"
    if int(matrix_sweep_report.get("success_case_count", 0)) <= 0:
        return "FAILED"
    if comparison_report["gate"]["status"] == "FAIL":
        return "FAILED"
    if int(comparison_report["comparison_tables"].get("attention_row_count", 0)) > 0:
        return "ATTENTION"
    return "SUCCEEDED"


def _parse_optional_non_negative_int(raw: Any, *, field: str) -> int | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be an integer, got: {raw}") from exc
    if parsed < 0:
        raise ValueError(f"{field} must be >= 0, got: {parsed}")
    return parsed


def _parse_optional_non_negative_float(raw: Any, *, field: str) -> float | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be a number, got: {raw}") from exc
    if parsed < 0.0:
        raise ValueError(f"{field} must be >= 0, got: {parsed}")
    return parsed


def _build_workflow_markdown_report(workflow_report: dict[str, Any]) -> str:
    variant_summary = workflow_report["variant_summary"]
    matrix_summary = workflow_report["matrix_summary"]
    comparison_summary = workflow_report["comparison_summary"]
    gate = comparison_summary["gate"]
    lines = [
        "# Scenario Batch Workflow",
        "",
        "## Overview",
        "",
        f"- Status: `{workflow_report['status']}`",
        f"- Selected variants: `{variant_summary['selected_variant_count']}`",
        f"- Matrix cases: `{matrix_summary['case_count']}`",
        f"- Attention rows: `{comparison_summary['attention_row_count']}`",
        f"- Gate status: `{gate['status']}`",
        f"- Gate failure codes: `{','.join(gate['failure_codes']) or '-'}`",
        "",
        "## Artifacts",
        "",
        f"- Variant workflow report: `{workflow_report['artifacts']['variant_workflow_report_path']}`",
        f"- Variant run report: `{workflow_report['artifacts']['variant_run_report_path']}`",
        f"- Matrix sweep report: `{workflow_report['artifacts']['matrix_sweep_report_path']}`",
        f"- Comparison report: `{workflow_report['artifacts']['comparison_report_path']}`",
        f"- Comparison markdown: `{workflow_report['artifacts']['comparison_markdown_path']}`",
        "",
        "## Attention Rows",
        "",
    ]
    attention_rows = comparison_summary["attention_rows"]
    if attention_rows:
        lines.append("| Source | Row ID | Group | Execution | Object Sim | Collision | Timeout | Failure Code |")
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
        for row in attention_rows:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("source_batch") or "-"),
                        str(row.get("row_id") or "-"),
                        str(row.get("group_id") or "-"),
                        str(row.get("execution_status") or "-"),
                        str(row.get("object_sim_status") or "-"),
                        str(row.get("collision")),
                        str(row.get("timeout")),
                        str(row.get("failure_code") or "-"),
                    ]
                )
                + " |"
            )
    else:
        lines.append("No attention rows.")
    lines.append("")
    return "\n".join(lines)


def run_scenario_batch_workflow(
    *,
    logical_scenarios_path: str,
    scenario_language_profile: str,
    scenario_language_dir: str | Path | None,
    matrix_scenario_path: Path,
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
    gate_max_attention_rows: int | None = None,
    gate_max_collision_rows: int | None = None,
    gate_max_timeout_rows: int | None = None,
    gate_min_min_ttc_any_lane_sec: float | None = None,
) -> dict[str, Any]:
    out_root = out_root.resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    variant_out_root = out_root / "variant_workflow"
    matrix_runs_root = out_root / "matrix_runs"
    matrix_report_path = out_root / "matrix_sweep_report_v0.json"
    comparison_report_path = out_root / "scenario_batch_comparison_report_v0.json"
    comparison_markdown_path = out_root / "scenario_batch_comparison_report_v0.md"
    workflow_markdown_path = out_root / "scenario_batch_workflow_report_v0.md"

    variant_result = run_scenario_variant_workflow(
        logical_scenarios_path=logical_scenarios_path,
        scenario_language_profile=scenario_language_profile,
        scenario_language_dir=scenario_language_dir,
        out_root=variant_out_root,
        sampling=sampling,
        sample_size=sample_size,
        seed=seed,
        max_variants_per_scenario=max_variants_per_scenario,
        execution_max_variants=execution_max_variants,
        sds_version=sds_version,
        sim_version=sim_version,
        fidelity_profile=fidelity_profile,
    )
    matrix_report = run_scenario_matrix_sweep(
        scenario_path=matrix_scenario_path.resolve(),
        out_root=matrix_runs_root,
        report_out=matrix_report_path,
        run_id_prefix=matrix_run_id_prefix,
        traffic_profile_ids=traffic_profile_ids,
        traffic_actor_pattern_ids=traffic_actor_pattern_ids,
        traffic_npc_speed_scale_values=traffic_npc_speed_scale_values,
        tire_friction_coeff_values=tire_friction_coeff_values,
        surface_friction_scale_values=surface_friction_scale_values,
        enable_ego_collision_avoidance=enable_ego_collision_avoidance,
        avoidance_ttc_threshold_sec=avoidance_ttc_threshold_sec,
        ego_max_brake_mps2=ego_max_brake_mps2,
        max_cases=max_cases,
    )
    comparison_report = build_scenario_batch_comparison_report(
        variant_workflow_report_path=Path(variant_result["workflow_report_path"]),
        matrix_sweep_report_path=matrix_report_path,
        out_report=comparison_report_path,
        markdown_out=comparison_markdown_path,
        gate_max_attention_rows=gate_max_attention_rows,
        gate_max_collision_rows=gate_max_collision_rows,
        gate_max_timeout_rows=gate_max_timeout_rows,
        gate_min_min_ttc_any_lane_sec=gate_min_min_ttc_any_lane_sec,
    )
    variant_workflow_report = variant_result["workflow_report"]
    workflow_status = _build_workflow_status(
        variant_workflow_report=variant_workflow_report,
        matrix_sweep_report=matrix_report,
        comparison_report=comparison_report,
    )
    workflow_report = {
        "scenario_batch_workflow_report_schema_version": SCENARIO_BATCH_WORKFLOW_REPORT_SCHEMA_VERSION_V0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "out_root": str(out_root),
        "status": workflow_status,
        "seed": int(seed),
        "sampling": sampling,
        "sample_size": int(sample_size),
        "max_variants_per_scenario": int(max_variants_per_scenario),
        "execution_max_variants": int(execution_max_variants),
        "sds_version": sds_version,
        "sim_version": sim_version,
        "fidelity_profile": fidelity_profile,
        "matrix_scenario_path": str(matrix_scenario_path.resolve()),
        "artifacts": {
            "variant_workflow_root": str(variant_out_root),
            "variant_workflow_report_path": str(Path(variant_result["workflow_report_path"]).resolve()),
            "variant_run_report_path": str(Path(variant_result["variant_run_report_path"]).resolve()),
            "matrix_runs_root": str(matrix_runs_root),
            "matrix_sweep_report_path": str(matrix_report_path.resolve()),
            "comparison_report_path": str(comparison_report_path.resolve()),
            "comparison_markdown_path": str(comparison_markdown_path.resolve()),
            "workflow_markdown_path": str(workflow_markdown_path.resolve()),
        },
        "variant_summary": {
            "variant_count": int(variant_workflow_report["variant_count"]),
            "selected_variant_count": int(variant_workflow_report["selected_variant_count"]),
            "execution_status_counts": dict(variant_workflow_report["execution_status_counts"]),
            "object_sim_status_counts": dict(variant_workflow_report["object_sim_status_counts"]),
            "by_payload_kind": dict(variant_workflow_report["by_payload_kind"]),
            "by_logical_scenario_id": dict(variant_workflow_report["by_logical_scenario_id"]),
            "successful_variant_row_count": int(variant_workflow_report["successful_variant_row_count"]),
            "non_success_variant_row_count": int(variant_workflow_report["non_success_variant_row_count"]),
        },
        "matrix_summary": {
            "case_count": int(matrix_report["case_count"]),
            "success_case_count": int(matrix_report["success_case_count"]),
            "failed_case_count": int(matrix_report["failed_case_count"]),
            "status_counts": dict(matrix_report["status_counts"]),
            "collision_case_count": int(matrix_report["collision_case_count"]),
            "timeout_case_count": int(matrix_report["timeout_case_count"]),
            "min_ttc_any_lane_sec_min": matrix_report.get("min_ttc_any_lane_sec_min"),
            "lowest_ttc_any_lane_run_id": matrix_report.get("lowest_ttc_any_lane_run_id"),
        },
        "comparison_summary": {
            "overview": dict(comparison_report["overview"]),
            "logical_scenario_row_count": int(comparison_report["comparison_tables"]["logical_scenario_row_count"]),
            "matrix_group_row_count": int(comparison_report["comparison_tables"]["matrix_group_row_count"]),
            "attention_row_count": int(comparison_report["comparison_tables"]["attention_row_count"]),
            "attention_rows": list(comparison_report["comparison_tables"]["attention_rows"]),
            "gate": dict(comparison_report["gate"]),
        },
    }
    workflow_report_path = out_root / "scenario_batch_workflow_report_v0.json"
    workflow_report_path.write_text(
        json.dumps(workflow_report, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    workflow_markdown_path.write_text(_build_workflow_markdown_report(workflow_report), encoding="utf-8")
    return {
        "workflow_report_path": workflow_report_path,
        "workflow_markdown_path": workflow_markdown_path,
        "workflow_report": workflow_report,
        "variant_result": variant_result,
        "matrix_report": matrix_report,
        "comparison_report": comparison_report,
    }


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        scenario_language_dir = args.scenario_language_dir
        if not scenario_language_dir:
            scenario_language_dir = _default_scenario_language_dir()
        result = run_scenario_batch_workflow(
            logical_scenarios_path=args.logical_scenarios,
            scenario_language_profile=args.scenario_language_profile,
            scenario_language_dir=scenario_language_dir,
            matrix_scenario_path=Path(args.matrix_scenario).resolve(),
            out_root=Path(args.out_root).resolve(),
            sampling=args.sampling,
            sample_size=int(args.sample_size),
            seed=int(args.seed),
            max_variants_per_scenario=int(args.max_variants_per_scenario),
            execution_max_variants=int(args.execution_max_variants),
            sds_version=args.sds_version,
            sim_version=args.sim_version,
            fidelity_profile=args.fidelity_profile,
            matrix_run_id_prefix=str(args.matrix_run_id_prefix).strip() or "RUN_CORE_SIM_SWEEP",
            traffic_profile_ids=_parse_csv_text_items(args.traffic_profile_ids, field="traffic-profile-ids"),
            traffic_actor_pattern_ids=_parse_csv_text_items(
                args.traffic_actor_pattern_ids,
                field="traffic-actor-pattern-ids",
            ),
            traffic_npc_speed_scale_values=_parse_csv_positive_floats(
                args.traffic_npc_speed_scale_values,
                field="traffic-npc-speed-scale-values",
            ),
            tire_friction_coeff_values=_parse_csv_positive_floats(
                args.tire_friction_coeff_values,
                field="tire-friction-coeff-values",
            ),
            surface_friction_scale_values=_parse_csv_positive_floats(
                args.surface_friction_scale_values,
                field="surface-friction-scale-values",
            ),
            enable_ego_collision_avoidance=bool(args.enable_ego_collision_avoidance),
            avoidance_ttc_threshold_sec=float(args.avoidance_ttc_threshold_sec),
            ego_max_brake_mps2=float(args.ego_max_brake_mps2),
            max_cases=_parse_non_negative_int(args.max_cases, field="max-cases"),
            gate_max_attention_rows=_parse_optional_non_negative_int(
                args.gate_max_attention_rows,
                field="gate-max-attention-rows",
            ),
            gate_max_collision_rows=_parse_optional_non_negative_int(
                args.gate_max_collision_rows,
                field="gate-max-collision-rows",
            ),
            gate_max_timeout_rows=_parse_optional_non_negative_int(
                args.gate_max_timeout_rows,
                field="gate-max-timeout-rows",
            ),
            gate_min_min_ttc_any_lane_sec=_parse_optional_non_negative_float(
                args.gate_min_min_ttc_any_lane_sec,
                field="gate-min-min-ttc-any-lane-sec",
            ),
        )
        workflow_report = result["workflow_report"]
        print(f"[ok] status={workflow_report['status']}")
        print(f"[ok] selected_variant_count={workflow_report['variant_summary']['selected_variant_count']}")
        print(f"[ok] matrix_case_count={workflow_report['matrix_summary']['case_count']}")
        print(f"[ok] attention_row_count={workflow_report['comparison_summary']['attention_row_count']}")
        print(f"[ok] gate_status={workflow_report['comparison_summary']['gate']['status']}")
        print(f"[ok] workflow_report={result['workflow_report_path']}")
        if workflow_report["status"] == "FAILED":
            return 2
        if args.fail_on_attention and workflow_report["status"] == "ATTENTION":
            return 2
        return 0
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] scenario_batch_workflow.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
