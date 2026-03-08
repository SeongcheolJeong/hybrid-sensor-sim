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
from hybrid_sensor_sim.tools.scenario_batch_gate_catalog import (
    resolve_scenario_batch_gate_profile_path,
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
    parser.add_argument("--gate-profile", default="")
    parser.add_argument("--gate-profile-id", default="")
    parser.add_argument("--gate-profile-dir", default="")
    parser.add_argument("--gate-max-attention-rows", default="")
    parser.add_argument("--gate-max-collision-rows", default="")
    parser.add_argument("--gate-max-timeout-rows", default="")
    parser.add_argument("--gate-max-path-conflict-rows", default="")
    parser.add_argument("--gate-max-merge-conflict-rows", default="")
    parser.add_argument("--gate-max-lane-change-conflict-rows", default="")
    parser.add_argument("--gate-min-min-ttc-any-lane-sec", default="")
    parser.add_argument("--gate-min-min-ttc-path-conflict-sec", default="")
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
    if raw is None:
        return None
    value = str(raw).strip()
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
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be a number, got: {raw}") from exc
    if parsed < 0.0:
        raise ValueError(f"{field} must be >= 0, got: {parsed}")
    return parsed


def _coerce_optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_counter(counter_payload: dict[str, Any]) -> str:
    items = []
    for key, value in sorted(counter_payload.items()):
        items.append(f"{key}={value}")
    return ", ".join(items) if items else "-"


def _format_float(value: Any) -> str:
    float_value = _coerce_optional_float(value)
    if float_value is None:
        return "-"
    return f"{float_value:.3f}"


def _markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        out.append("| " + " | ".join(row) + " |")
    return "\n".join(out)


def _build_logical_scenario_health_rows(
    logical_rows: list[dict[str, Any]],
    *,
    gate_policy: dict[str, Any],
) -> list[dict[str, Any]]:
    health_rows: list[dict[str, Any]] = []
    min_ttc_threshold = _coerce_optional_float(gate_policy.get("min_min_ttc_any_lane_sec"))
    max_attention_rows = gate_policy.get("max_attention_rows")
    max_collision_rows = gate_policy.get("max_collision_rows")
    max_timeout_rows = gate_policy.get("max_timeout_rows")
    max_path_conflict_rows = gate_policy.get("max_path_conflict_rows")
    max_merge_conflict_rows = gate_policy.get("max_merge_conflict_rows")
    max_lane_change_conflict_rows = gate_policy.get("max_lane_change_conflict_rows")
    min_ttc_path_conflict_threshold = _coerce_optional_float(gate_policy.get("min_min_ttc_path_conflict_sec"))

    def evaluate_rule(
        *,
        metric_id: str,
        metric_value: Any,
        threshold_value: Any,
        comparison: str,
        failure_code: str,
    ) -> dict[str, Any] | None:
        if threshold_value is None:
            return None
        if comparison == "max_le":
            passed = int(metric_value) <= int(threshold_value)
        elif comparison == "min_ge":
            metric_float = _coerce_optional_float(metric_value)
            threshold_float = float(threshold_value)
            passed = metric_float is not None and metric_float >= threshold_float
        else:
            raise ValueError(f"unsupported gate comparison: {comparison}")
        return {
            "metric_id": metric_id,
            "metric_value": metric_value,
            "threshold_value": threshold_value,
            "comparison": comparison,
            "passed": bool(passed),
            "failure_code": None if passed else failure_code,
        }

    for row in logical_rows:
        execution_status_counts = dict(row.get("execution_status_counts", {}))
        variant_count = int(row.get("variant_count", 0))
        succeeded_count = int(execution_status_counts.get("SUCCEEDED", 0))
        non_success_variant_count = max(variant_count - succeeded_count, 0)
        collision_count = int(row.get("collision_count", 0))
        timeout_count = int(row.get("timeout_count", 0))
        path_conflict_row_count = int(row.get("path_conflict_row_count", 0))
        merge_conflict_row_count = int(row.get("merge_conflict_row_count", 0))
        lane_change_conflict_row_count = int(row.get("lane_change_conflict_row_count", 0))
        attention_variant_count = max(non_success_variant_count, collision_count, timeout_count)
        min_ttc_any_lane_sec = _coerce_optional_float(row.get("min_ttc_any_lane_sec_min"))
        min_ttc_path_conflict_sec = _coerce_optional_float(row.get("min_ttc_path_conflict_sec_min"))
        gate_evaluated_rules = [
            rule
            for rule in [
                evaluate_rule(
                    metric_id="attention_variant_count",
                    metric_value=attention_variant_count,
                    threshold_value=max_attention_rows,
                    comparison="max_le",
                    failure_code="ATTENTION_ROWS_EXCEEDED",
                ),
                evaluate_rule(
                    metric_id="collision_count",
                    metric_value=collision_count,
                    threshold_value=max_collision_rows,
                    comparison="max_le",
                    failure_code="COLLISION_ROWS_EXCEEDED",
                ),
                evaluate_rule(
                    metric_id="timeout_count",
                    metric_value=timeout_count,
                    threshold_value=max_timeout_rows,
                    comparison="max_le",
                    failure_code="TIMEOUT_ROWS_EXCEEDED",
                ),
                evaluate_rule(
                    metric_id="path_conflict_row_count",
                    metric_value=path_conflict_row_count,
                    threshold_value=max_path_conflict_rows,
                    comparison="max_le",
                    failure_code="PATH_CONFLICT_ROWS_EXCEEDED",
                ),
                evaluate_rule(
                    metric_id="merge_conflict_row_count",
                    metric_value=merge_conflict_row_count,
                    threshold_value=max_merge_conflict_rows,
                    comparison="max_le",
                    failure_code="MERGE_CONFLICT_ROWS_EXCEEDED",
                ),
                evaluate_rule(
                    metric_id="lane_change_conflict_row_count",
                    metric_value=lane_change_conflict_row_count,
                    threshold_value=max_lane_change_conflict_rows,
                    comparison="max_le",
                    failure_code="LANE_CHANGE_CONFLICT_ROWS_EXCEEDED",
                ),
                evaluate_rule(
                    metric_id="min_ttc_any_lane_sec_min",
                    metric_value=min_ttc_any_lane_sec,
                    threshold_value=min_ttc_threshold,
                    comparison="min_ge",
                    failure_code="MIN_TTC_BELOW_THRESHOLD",
                ),
                evaluate_rule(
                    metric_id="min_ttc_path_conflict_sec_min",
                    metric_value=min_ttc_path_conflict_sec,
                    threshold_value=min_ttc_path_conflict_threshold,
                    comparison="min_ge",
                    failure_code="MIN_TTC_PATH_CONFLICT_BELOW_THRESHOLD",
                ),
            ]
            if rule is not None
        ]
        gate_failure_codes = [
            str(rule["failure_code"])
            for rule in gate_evaluated_rules
            if not bool(rule["passed"]) and rule.get("failure_code")
        ]
        if not gate_evaluated_rules:
            gate_status = "DISABLED"
            gate_passed = True
        elif gate_failure_codes:
            gate_status = "FAIL"
            gate_passed = False
        else:
            gate_status = "PASS"
            gate_passed = True
        reasons: list[str] = []
        fail = gate_status == "FAIL"
        if int(execution_status_counts.get("FAILED", 0)) > 0:
            reasons.append("EXECUTION_FAILURE_PRESENT")
            fail = True
        if collision_count > 0:
            reasons.append("COLLISION_PRESENT")
            fail = True
        if timeout_count > 0:
            reasons.append("TIMEOUT_PRESENT")
            fail = True
        if path_conflict_row_count > 0:
            reasons.append("PATH_CONFLICT_PRESENT")
        if merge_conflict_row_count > 0:
            reasons.append("MERGE_CONFLICT_PRESENT")
        if lane_change_conflict_row_count > 0:
            reasons.append("LANE_CHANGE_CONFLICT_PRESENT")
        if (
            min_ttc_threshold is not None
            and min_ttc_any_lane_sec is not None
            and min_ttc_any_lane_sec < min_ttc_threshold
        ):
            reasons.append("MIN_TTC_BELOW_THRESHOLD")
            fail = True
        if (
            min_ttc_path_conflict_threshold is not None
            and min_ttc_path_conflict_sec is not None
            and min_ttc_path_conflict_sec < min_ttc_path_conflict_threshold
        ):
            reasons.append("MIN_TTC_PATH_CONFLICT_BELOW_THRESHOLD")
            fail = True
        if non_success_variant_count > 0 and "NON_SUCCESS_VARIANTS_PRESENT" not in reasons:
            reasons.append("NON_SUCCESS_VARIANTS_PRESENT")
        for failure_code in gate_failure_codes:
            if failure_code not in reasons:
                reasons.append(failure_code)
        if fail:
            health_status = "FAIL"
        elif non_success_variant_count > 0:
            health_status = "ATTENTION"
        else:
            health_status = "PASS"
        health_rows.append(
            {
                "logical_scenario_id": str(row.get("logical_scenario_id", "")).strip() or None,
                "health_status": health_status,
                "health_reasons": reasons,
                "variant_count": variant_count,
                "non_success_variant_count": non_success_variant_count,
                "attention_variant_count": attention_variant_count,
                "collision_count": collision_count,
                "timeout_count": timeout_count,
                "path_conflict_row_count": path_conflict_row_count,
                "merge_conflict_row_count": merge_conflict_row_count,
                "lane_change_conflict_row_count": lane_change_conflict_row_count,
                "min_ttc_any_lane_sec_min": min_ttc_any_lane_sec,
                "min_ttc_path_conflict_sec_min": min_ttc_path_conflict_sec,
                "gate_min_min_ttc_any_lane_sec": min_ttc_threshold,
                "gate_min_min_ttc_path_conflict_sec": min_ttc_path_conflict_threshold,
                "gate_status": gate_status,
                "gate_passed": gate_passed,
                "gate_failure_codes": gate_failure_codes,
                "gate_evaluated_rules": gate_evaluated_rules,
            }
        )
    return health_rows


def _build_failing_logical_scenario_rows(
    logical_scenario_health_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, int]]:
    failing_rows = [
        dict(row)
        for row in logical_scenario_health_rows
        if row["health_status"] == "FAIL" or row["gate_status"] == "FAIL"
    ]
    gate_failure_code_counts: dict[str, int] = {}
    health_reason_counts: dict[str, int] = {}
    for row in failing_rows:
        for code in row.get("gate_failure_codes", []):
            code_key = str(code)
            gate_failure_code_counts[code_key] = gate_failure_code_counts.get(code_key, 0) + 1
        for reason in row.get("health_reasons", []):
            reason_key = str(reason)
            health_reason_counts[reason_key] = health_reason_counts.get(reason_key, 0) + 1
    return (
        failing_rows,
        dict(sorted(gate_failure_code_counts.items())),
        dict(sorted(health_reason_counts.items())),
    )


def _sort_float_none_last(value: Any) -> float:
    float_value = _coerce_optional_float(value)
    return float_value if float_value is not None else float("inf")


def _build_workflow_status_summary(
    *,
    workflow_status: str,
    variant_workflow_report: dict[str, Any],
    matrix_report: dict[str, Any],
    comparison_summary: dict[str, Any],
) -> dict[str, Any]:
    decision_trace = [
        {
            "step_id": "variant_execution_failures",
            "matched": int(variant_workflow_report.get("execution_status_counts", {}).get("FAILED", 0)) > 0,
            "status_if_matched": "FAILED",
            "reason_code": "VARIANT_EXECUTION_FAILURE_PRESENT",
        },
        {
            "step_id": "matrix_success_cases",
            "matched": int(matrix_report.get("success_case_count", 0)) <= 0,
            "status_if_matched": "FAILED",
            "reason_code": "MATRIX_NO_SUCCESS_CASES",
        },
        {
            "step_id": "comparison_gate",
            "matched": comparison_summary["gate"]["status"] == "FAIL",
            "status_if_matched": "FAILED",
            "reason_code": "BATCH_GATE_FAILED",
        },
        {
            "step_id": "attention_rows",
            "matched": int(comparison_summary.get("attention_row_count", 0)) > 0,
            "status_if_matched": "ATTENTION",
            "reason_code": "ATTENTION_ROWS_PRESENT",
        },
    ]
    status_reason_codes: list[str] = []
    for step in decision_trace:
        if step["matched"]:
            status_reason_codes.append(str(step["reason_code"]))
    final_status_source = "default_success"
    for step in decision_trace:
        if step["matched"]:
            final_status_source = str(step["step_id"])
            break

    failing_logical_scenario_ids = [
        str(row["logical_scenario_id"])
        for row in comparison_summary.get("failing_logical_scenario_rows", [])
        if row.get("logical_scenario_id")
    ]
    attention_logical_scenario_ids = sorted(
        {
            str(row.get("group_id"))
            for row in comparison_summary.get("attention_rows", [])
            if row.get("source_batch") in {"variant", "variant_workflow"} and row.get("group_id")
        }
    )
    attention_matrix_group_ids = sorted(
        {
            str(row.get("group_id"))
            for row in comparison_summary.get("attention_rows", [])
            if row.get("source_batch") == "matrix_sweep" and row.get("group_id")
        }
    )
    gate_policy = dict(comparison_summary.get("gate", {}).get("policy", {}))
    logical_rows = list(comparison_summary.get("logical_scenario_rows", []))
    matrix_rows = list(comparison_summary.get("matrix_group_rows", []))
    avoidance_trigger_counts_by_interaction_kind: dict[str, int] = {}
    avoidance_brake_event_count_total = 0
    avoidance_row_count = 0
    for row in logical_rows + matrix_rows:
        avoidance_brake_event_count_total += int(row.get("ego_avoidance_brake_event_count_total", 0) or 0)
        avoidance_row_count += int(row.get("ego_avoidance_row_count", 0) or 0)
        for label, count in dict(row.get("ego_avoidance_trigger_counts_by_interaction_kind", {})).items():
            key = str(label)
            avoidance_trigger_counts_by_interaction_kind[key] = (
                avoidance_trigger_counts_by_interaction_kind.get(key, 0) + int(count)
            )

    def matrix_group_gate_failure_codes(row: dict[str, Any]) -> list[str]:
        failure_codes: list[str] = []
        if int(row.get("execution_status_counts", {}).get("FAILED", 0)) > 0:
            failure_codes.append("EXECUTION_FAILURE_PRESENT")
        max_collision_rows = gate_policy.get("max_collision_rows")
        max_timeout_rows = gate_policy.get("max_timeout_rows")
        max_path_conflict_rows = gate_policy.get("max_path_conflict_rows")
        max_merge_conflict_rows = gate_policy.get("max_merge_conflict_rows")
        max_lane_change_conflict_rows = gate_policy.get("max_lane_change_conflict_rows")
        min_ttc_any_lane_sec = _coerce_optional_float(gate_policy.get("min_min_ttc_any_lane_sec"))
        min_ttc_path_conflict_sec = _coerce_optional_float(gate_policy.get("min_min_ttc_path_conflict_sec"))
        if max_collision_rows is not None and int(row.get("collision_count", 0)) > int(max_collision_rows):
            failure_codes.append("COLLISION_ROWS_EXCEEDED")
        if max_timeout_rows is not None and int(row.get("timeout_count", 0)) > int(max_timeout_rows):
            failure_codes.append("TIMEOUT_ROWS_EXCEEDED")
        if max_path_conflict_rows is not None and int(row.get("path_conflict_row_count", 0)) > int(max_path_conflict_rows):
            failure_codes.append("PATH_CONFLICT_ROWS_EXCEEDED")
        if max_merge_conflict_rows is not None and int(row.get("merge_conflict_row_count", 0)) > int(max_merge_conflict_rows):
            failure_codes.append("MERGE_CONFLICT_ROWS_EXCEEDED")
        if max_lane_change_conflict_rows is not None and int(row.get("lane_change_conflict_row_count", 0)) > int(max_lane_change_conflict_rows):
            failure_codes.append("LANE_CHANGE_CONFLICT_ROWS_EXCEEDED")
        row_min_ttc_any = _coerce_optional_float(row.get("min_ttc_any_lane_sec_min"))
        if min_ttc_any_lane_sec is not None and row_min_ttc_any is not None and row_min_ttc_any < min_ttc_any_lane_sec:
            failure_codes.append("MIN_TTC_BELOW_THRESHOLD")
        row_min_ttc_path = _coerce_optional_float(row.get("min_ttc_path_conflict_sec_min"))
        if (
            min_ttc_path_conflict_sec is not None
            and row_min_ttc_path is not None
            and row_min_ttc_path < min_ttc_path_conflict_sec
        ):
            failure_codes.append("MIN_TTC_PATH_CONFLICT_BELOW_THRESHOLD")
        return failure_codes

    logical_health_rows = list(comparison_summary.get("logical_scenario_health_rows", []))
    worst_logical_scenario_row = None
    if logical_health_rows:
        logical_health_priority = {"FAIL": 0, "ATTENTION": 1, "PASS": 2}
        gate_status_priority = {"FAIL": 0, "PASS": 1, "DISABLED": 2}

        def logical_health_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
            return (
                logical_health_priority.get(str(row.get("health_status")), 99),
                gate_status_priority.get(str(row.get("gate_status")), 99),
                -len(list(row.get("gate_failure_codes", []))),
                -int(row.get("collision_count", 0) or 0),
                -int(row.get("timeout_count", 0) or 0),
                -int(row.get("merge_conflict_row_count", 0) or 0),
                -int(row.get("lane_change_conflict_row_count", 0) or 0),
                -int(row.get("path_conflict_row_count", 0) or 0),
                _sort_float_none_last(row.get("min_ttc_path_conflict_sec_min")),
                _sort_float_none_last(row.get("min_ttc_any_lane_sec_min")),
                str(row.get("logical_scenario_id") or ""),
            )

        worst_logical_scenario_row = min(logical_health_rows, key=logical_health_sort_key)

    matrix_group_rows = list(comparison_summary.get("matrix_group_rows", []))

    failing_matrix_group_ids = []
    matrix_group_gate_failure_code_counts: dict[str, int] = {}
    worst_matrix_group_row = None
    matrix_group_rank_rows: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    for row in comparison_summary.get("matrix_group_rows", []):
        if not row.get("matrix_group_id"):
            continue
        failure_codes = matrix_group_gate_failure_codes(row)
        if failure_codes:
            failing_matrix_group_ids.append(str(row["matrix_group_id"]))
            for failure_code in failure_codes:
                matrix_group_gate_failure_code_counts[failure_code] = (
                    matrix_group_gate_failure_code_counts.get(failure_code, 0) + 1
                )
        matrix_group_rank_rows.append(
            (
                (
                    0 if failure_codes else 1,
                    -len(failure_codes),
                    -int(row.get("collision_count", 0) or 0),
                    -int(row.get("timeout_count", 0) or 0),
                    -int(row.get("merge_conflict_row_count", 0) or 0),
                    -int(row.get("lane_change_conflict_row_count", 0) or 0),
                    -int(row.get("path_conflict_row_count", 0) or 0),
                    _sort_float_none_last(row.get("min_ttc_path_conflict_sec_min")),
                    _sort_float_none_last(row.get("min_ttc_any_lane_sec_min")),
                    str(row.get("matrix_group_id") or ""),
                ),
                {
                    "matrix_group_id": str(row.get("matrix_group_id") or ""),
                    "case_count": int(row.get("case_count", 0) or 0),
                    "execution_status_counts": dict(row.get("execution_status_counts", {})),
                    "object_sim_status_counts": dict(row.get("object_sim_status_counts", {})),
                    "collision_count": int(row.get("collision_count", 0) or 0),
                    "timeout_count": int(row.get("timeout_count", 0) or 0),
                    "path_conflict_row_count": int(row.get("path_conflict_row_count", 0) or 0),
                    "merge_conflict_row_count": int(row.get("merge_conflict_row_count", 0) or 0),
                    "lane_change_conflict_row_count": int(row.get("lane_change_conflict_row_count", 0) or 0),
                    "min_ttc_any_lane_sec_min": _coerce_optional_float(row.get("min_ttc_any_lane_sec_min")),
                    "min_ttc_path_conflict_sec_min": _coerce_optional_float(
                        row.get("min_ttc_path_conflict_sec_min")
                    ),
                    "gate_failure_codes": failure_codes,
                },
            )
        )
    failing_matrix_group_ids = sorted(failing_matrix_group_ids)
    if matrix_group_rank_rows:
        worst_matrix_group_row = min(matrix_group_rank_rows, key=lambda item: item[0])[1]
    breached_gate_rules = [
        dict(rule)
        for rule in comparison_summary["gate"].get("evaluated_rules", [])
        if not bool(rule.get("passed", False))
    ]
    breached_gate_metric_ids = [str(rule["metric_id"]) for rule in breached_gate_rules if rule.get("metric_id")]
    return {
        "workflow_status": workflow_status,
        "final_status_source": final_status_source,
        "status_reason_codes": status_reason_codes,
        "status_reason_count": len(status_reason_codes),
        "decision_trace": decision_trace,
        "gate_failure_codes": list(comparison_summary["gate"].get("failure_codes", [])),
        "breached_gate_rules": breached_gate_rules,
        "breached_gate_rule_count": len(breached_gate_rules),
        "breached_gate_metric_ids": breached_gate_metric_ids,
        "failing_logical_scenario_ids": failing_logical_scenario_ids,
        "failing_logical_scenario_count": len(failing_logical_scenario_ids),
        "attention_logical_scenario_ids": attention_logical_scenario_ids,
        "attention_logical_scenario_count": len(attention_logical_scenario_ids),
        "failing_matrix_group_ids": failing_matrix_group_ids,
        "failing_matrix_group_count": len(failing_matrix_group_ids),
        "matrix_group_gate_failure_code_counts": dict(sorted(matrix_group_gate_failure_code_counts.items())),
        "attention_matrix_group_ids": attention_matrix_group_ids,
        "attention_matrix_group_count": len(attention_matrix_group_ids),
        "avoidance_row_count": int(avoidance_row_count),
        "avoidance_brake_event_count_total": int(avoidance_brake_event_count_total),
        "avoidance_trigger_counts_by_interaction_kind": dict(
            sorted(avoidance_trigger_counts_by_interaction_kind.items())
        ),
        "attention_reason_counts": dict(comparison_summary.get("attention_reason_counts", {})),
        "worst_logical_scenario_row": dict(worst_logical_scenario_row) if worst_logical_scenario_row is not None else None,
        "worst_matrix_group_row": dict(worst_matrix_group_row) if worst_matrix_group_row is not None else None,
        "logical_scenario_health_status_counts": dict(
            comparison_summary.get("logical_scenario_health_status_counts", {})
        ),
        "logical_scenario_health_gate_status_counts": dict(
            comparison_summary.get("logical_scenario_health_gate_status_counts", {})
        ),
    }


def _build_workflow_markdown_report(workflow_report: dict[str, Any]) -> str:
    variant_summary = workflow_report["variant_summary"]
    matrix_summary = workflow_report["matrix_summary"]
    comparison_summary = workflow_report["comparison_summary"]
    status_summary = workflow_report["status_summary"]
    gate = comparison_summary["gate"]
    logical_health_rows = comparison_summary["logical_scenario_health_rows"]
    failing_logical_rows = comparison_summary["failing_logical_scenario_rows"]
    logical_rows = comparison_summary["logical_scenario_rows"]
    matrix_rows = comparison_summary["matrix_group_rows"]
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
        f"- Gate profile: `{gate['policy'].get('profile_path') or '-'}`",
        f"- Gate failure codes: `{','.join(gate['failure_codes']) or '-'}`",
        f"- Breached gate metrics: `{','.join(status_summary['breached_gate_metric_ids']) or '-'}`",
        f"- Final status source: `{status_summary['final_status_source']}`",
        f"- Status reason codes: `{','.join(status_summary['status_reason_codes']) or '-'}`",
        f"- Failing logical scenarios: `{','.join(status_summary['failing_logical_scenario_ids']) or '-'}`",
        f"- Attention logical scenarios: `{','.join(status_summary['attention_logical_scenario_ids']) or '-'}`",
        f"- Failing matrix groups: `{','.join(status_summary['failing_matrix_group_ids']) or '-'}`",
        f"- Attention matrix groups: `{','.join(status_summary['attention_matrix_group_ids']) or '-'}`",
        f"- Matrix gate failure counts: `{_format_counter(status_summary['matrix_group_gate_failure_code_counts'])}`",
        f"- Avoidance-active rows: `{status_summary['avoidance_row_count']}`",
        f"- Avoidance brake event count: `{status_summary['avoidance_brake_event_count_total']}`",
        f"- Avoidance trigger counts: `{_format_counter(status_summary['avoidance_trigger_counts_by_interaction_kind'])}`",
        "",
        "## Worst-Case Rows",
        "",
        f"- Worst logical scenario: `{(status_summary['worst_logical_scenario_row'] or {}).get('logical_scenario_id', '-')}`",
        f"- Worst matrix group: `{(status_summary['worst_matrix_group_row'] or {}).get('matrix_group_id', '-')}`",
        "",
        "## Status Decision Trace",
        "",
    ]
    lines.append(
        _markdown_table(
            ["Step", "Matched", "Status If Matched", "Reason Code"],
            [
                [
                    str(step["step_id"]),
                    str(step["matched"]),
                    str(step["status_if_matched"]),
                    str(step["reason_code"]),
                ]
                for step in status_summary["decision_trace"]
            ],
        )
    )
    lines.extend(
        [
            "",
            "## Artifacts",
            "",
            f"- Variant workflow report: `{workflow_report['artifacts']['variant_workflow_report_path']}`",
            f"- Variant run report: `{workflow_report['artifacts']['variant_run_report_path']}`",
            f"- Matrix sweep report: `{workflow_report['artifacts']['matrix_sweep_report_path']}`",
            f"- Comparison report: `{workflow_report['artifacts']['comparison_report_path']}`",
            f"- Comparison markdown: `{workflow_report['artifacts']['comparison_markdown_path']}`",
            f"- Workflow markdown: `{workflow_report['artifacts']['workflow_markdown_path']}`",
            "",
            "## Logical Scenario Health",
            "",
        ]
    )
    if logical_health_rows:
        lines.append(
            _markdown_table(
                [
                    "Logical Scenario",
                    "Health",
                    "Gate",
                    "Gate Failure Codes",
                    "Reasons",
                    "Variants",
                    "Non-Success",
                    "Collisions",
                    "Timeouts",
                    "Min TTC Any",
                ],
                [
                    [
                        str(row["logical_scenario_id"] or "-"),
                        str(row["health_status"]),
                        str(row["gate_status"]),
                        ",".join(str(item) for item in row["gate_failure_codes"]) or "-",
                        ",".join(row["health_reasons"]) or "-",
                        str(row["variant_count"]),
                        str(row["non_success_variant_count"]),
                        str(row["collision_count"]),
                        str(row["timeout_count"]),
                        _format_float(row["min_ttc_any_lane_sec_min"]),
                    ]
                    for row in logical_health_rows
                ],
            )
        )
    else:
        lines.append("No logical scenario health rows.")
    lines.extend(
        [
            "",
            "## Failing Logical Scenarios",
            "",
            f"- Gate failure code counts: `{_format_counter(comparison_summary['failing_logical_scenario_gate_failure_code_counts'])}`",
            f"- Health reason counts: `{_format_counter(comparison_summary['failing_logical_scenario_health_reason_counts'])}`",
            "",
        ]
    )
    if failing_logical_rows:
        lines.append(
            _markdown_table(
                [
                    "Logical Scenario",
                    "Health",
                    "Gate",
                    "Gate Failure Codes",
                    "Reasons",
                    "Path",
                    "Merge",
                    "Lane Change",
                    "Avoidance",
                    "Min TTC Path",
                ],
                [
                    [
                        str(row["logical_scenario_id"] or "-"),
                        str(row["health_status"]),
                        str(row["gate_status"]),
                        ",".join(str(item) for item in row["gate_failure_codes"]) or "-",
                        ",".join(row["health_reasons"]) or "-",
                        str(row.get("path_conflict_row_count", 0)),
                        str(row.get("merge_conflict_row_count", 0)),
                        str(row.get("lane_change_conflict_row_count", 0)),
                        str(row.get("ego_avoidance_brake_event_count_total", 0)),
                        _format_float(row.get("min_ttc_path_conflict_sec_min")),
                    ]
                    for row in failing_logical_rows
                ],
            )
        )
    else:
        lines.append("No failing logical scenarios.")
    lines.extend(
        [
            "",
            "## Logical Scenario Summary",
            "",
        ]
    )
    if logical_rows:
        lines.append(
            _markdown_table(
                [
                    "Logical Scenario",
                    "Variants",
                    "Payload Kinds",
                "Execution",
                "Object Sim",
                "Collisions",
                "Timeouts",
                    "Path",
                    "Merge",
                    "Lane Change",
                    "Avoidance",
                    "Min TTC Any",
                    "Min TTC Path",
                ],
            [
                [
                        str(row["logical_scenario_id"]),
                        str(row["variant_count"]),
                        _format_counter(row["payload_kind_counts"]),
                        _format_counter(row["execution_status_counts"]),
                    _format_counter(row["object_sim_status_counts"]),
                    str(row["collision_count"]),
                    str(row["timeout_count"]),
                    str(row.get("path_conflict_row_count", 0)),
                    str(row.get("merge_conflict_row_count", 0)),
                    str(row.get("lane_change_conflict_row_count", 0)),
                    str(row.get("ego_avoidance_brake_event_count_total", 0)),
                    _format_float(row["min_ttc_any_lane_sec_min"]),
                    _format_float(row.get("min_ttc_path_conflict_sec_min")),
                ]
                for row in logical_rows
            ],
            )
        )
    else:
        lines.append("No logical scenario rows.")
    lines.extend(["", "## Matrix Group Summary", ""])
    if matrix_rows:
        lines.append(
            _markdown_table(
                [
                    "Matrix Group",
                    "Cases",
                "Execution",
                "Object Sim",
                "Collisions",
                "Timeouts",
                    "Path",
                    "Merge",
                    "Lane Change",
                    "Avoidance",
                    "Min TTC Any",
                    "Min TTC Path",
                ],
            [
                [
                        str(row["matrix_group_id"]),
                        str(row["case_count"]),
                        _format_counter(row["execution_status_counts"]),
                    _format_counter(row["object_sim_status_counts"]),
                    str(row["collision_count"]),
                    str(row["timeout_count"]),
                    str(row.get("path_conflict_row_count", 0)),
                    str(row.get("merge_conflict_row_count", 0)),
                    str(row.get("lane_change_conflict_row_count", 0)),
                    str(row.get("ego_avoidance_brake_event_count_total", 0)),
                    _format_float(row["min_ttc_any_lane_sec_min"]),
                    _format_float(row.get("min_ttc_path_conflict_sec_min")),
                ]
                for row in matrix_rows
            ],
            )
        )
    else:
        lines.append("No matrix group rows.")
    lines.extend(["", "## Successful Variants", ""])
    successful_rows = variant_summary["successful_variant_rows"]
    if successful_rows:
        lines.append(
            _markdown_table(
                [
                    "Variant",
                    "Logical Scenario",
                    "Payload Kind",
                    "Execution Path",
                    "Object Sim",
                    "Termination",
                ],
                [
                    [
                        str(row["variant_id"] or "-"),
                        str(row["logical_scenario_id"] or "-"),
                        str(row["rendered_payload_kind"] or "-"),
                        str(row["execution_path"] or "-"),
                        str(row["object_sim_status"] or "-"),
                        str(row["termination_reason"] or "-"),
                    ]
                    for row in successful_rows
                ],
            )
        )
    else:
        lines.append("No successful variants.")
    lines.extend(["", "## Non-Success Variants", ""])
    non_success_rows = variant_summary["non_success_variant_rows"]
    if non_success_rows:
        lines.append(
            _markdown_table(
                [
                    "Variant",
                    "Logical Scenario",
                    "Payload Kind",
                    "Execution Status",
                    "Failure Code",
                    "Execution Path",
                ],
                [
                    [
                        str(row["variant_id"] or "-"),
                        str(row["logical_scenario_id"] or "-"),
                        str(row["rendered_payload_kind"] or "-"),
                        str(row["execution_status"] or "-"),
                        str(row["failure_code"] or "-"),
                        str(row["execution_path"] or "-"),
                    ]
                    for row in non_success_rows
                ],
            )
        )
    else:
        lines.append("No non-success variants.")
    lines.extend(["", "## Attention Rows", ""])
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
    gate_profile_path: Path | None = None,
    gate_max_attention_rows: int | None = None,
    gate_max_collision_rows: int | None = None,
    gate_max_timeout_rows: int | None = None,
    gate_max_path_conflict_rows: int | None = None,
    gate_max_merge_conflict_rows: int | None = None,
    gate_max_lane_change_conflict_rows: int | None = None,
    gate_min_min_ttc_any_lane_sec: float | None = None,
    gate_min_min_ttc_path_conflict_sec: float | None = None,
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
        gate_profile_path=gate_profile_path,
        gate_max_attention_rows=gate_max_attention_rows,
        gate_max_collision_rows=gate_max_collision_rows,
        gate_max_timeout_rows=gate_max_timeout_rows,
        gate_max_path_conflict_rows=gate_max_path_conflict_rows,
        gate_max_merge_conflict_rows=gate_max_merge_conflict_rows,
        gate_max_lane_change_conflict_rows=gate_max_lane_change_conflict_rows,
        gate_min_min_ttc_any_lane_sec=gate_min_min_ttc_any_lane_sec,
        gate_min_min_ttc_path_conflict_sec=gate_min_min_ttc_path_conflict_sec,
    )
    logical_scenario_health_rows = _build_logical_scenario_health_rows(
        list(comparison_report["comparison_tables"]["logical_scenario_rows"]),
        gate_policy=dict(comparison_report["gate"]["policy"]),
    )
    (
        failing_logical_scenario_rows,
        failing_logical_scenario_gate_failure_code_counts,
        failing_logical_scenario_health_reason_counts,
    ) = _build_failing_logical_scenario_rows(logical_scenario_health_rows)
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
            "successful_variant_rows": list(variant_workflow_report["successful_variant_rows"]),
            "non_success_variant_rows": list(variant_workflow_report["non_success_variant_rows"]),
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
            "logical_scenario_health_rows": logical_scenario_health_rows,
            "logical_scenario_health_row_count": len(logical_scenario_health_rows),
            "logical_scenario_health_status_counts": {
                "PASS": sum(1 for row in logical_scenario_health_rows if row["health_status"] == "PASS"),
                "ATTENTION": sum(1 for row in logical_scenario_health_rows if row["health_status"] == "ATTENTION"),
                "FAIL": sum(1 for row in logical_scenario_health_rows if row["health_status"] == "FAIL"),
            },
            "logical_scenario_health_gate_status_counts": {
                "DISABLED": sum(1 for row in logical_scenario_health_rows if row["gate_status"] == "DISABLED"),
                "PASS": sum(1 for row in logical_scenario_health_rows if row["gate_status"] == "PASS"),
                "FAIL": sum(1 for row in logical_scenario_health_rows if row["gate_status"] == "FAIL"),
            },
            "failing_logical_scenario_rows": failing_logical_scenario_rows,
            "failing_logical_scenario_row_count": len(failing_logical_scenario_rows),
            "failing_logical_scenario_gate_failure_code_counts": failing_logical_scenario_gate_failure_code_counts,
            "failing_logical_scenario_health_reason_counts": failing_logical_scenario_health_reason_counts,
            "logical_scenario_rows": list(comparison_report["comparison_tables"]["logical_scenario_rows"]),
            "matrix_group_rows": list(comparison_report["comparison_tables"]["matrix_group_rows"]),
            "attention_row_count": int(comparison_report["comparison_tables"]["attention_row_count"]),
            "attention_rows": list(comparison_report["comparison_tables"]["attention_rows"]),
            "attention_reason_counts": dict(comparison_report["comparison_tables"]["attention_reason_counts"]),
            "gate": dict(comparison_report["gate"]),
        },
    }
    workflow_report["status_summary"] = _build_workflow_status_summary(
        workflow_status=workflow_status,
        variant_workflow_report=variant_workflow_report,
        matrix_report=matrix_report,
        comparison_summary=workflow_report["comparison_summary"],
    )
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
            gate_profile_path=resolve_scenario_batch_gate_profile_path(
                gate_profile=args.gate_profile,
                gate_profile_id=args.gate_profile_id,
                gate_profile_dir=args.gate_profile_dir,
            ),
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
            gate_max_path_conflict_rows=_parse_optional_non_negative_int(
                args.gate_max_path_conflict_rows,
                field="gate-max-path-conflict-rows",
            ),
            gate_max_merge_conflict_rows=_parse_optional_non_negative_int(
                args.gate_max_merge_conflict_rows,
                field="gate-max-merge-conflict-rows",
            ),
            gate_max_lane_change_conflict_rows=_parse_optional_non_negative_int(
                args.gate_max_lane_change_conflict_rows,
                field="gate-max-lane-change-conflict-rows",
            ),
            gate_min_min_ttc_any_lane_sec=_parse_optional_non_negative_float(
                args.gate_min_min_ttc_any_lane_sec,
                field="gate-min-min-ttc-any-lane-sec",
            ),
            gate_min_min_ttc_path_conflict_sec=_parse_optional_non_negative_float(
                args.gate_min_min_ttc_path_conflict_sec,
                field="gate-min-min-ttc-path-conflict-sec",
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
