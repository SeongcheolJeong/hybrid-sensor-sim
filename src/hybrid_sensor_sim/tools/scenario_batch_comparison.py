from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.tools.scenario_batch_gate_catalog import (
    load_scenario_batch_gate_profile,
    resolve_scenario_batch_gate_profile_path,
)

CORE_SIM_MATRIX_SWEEP_SCHEMA_VERSION_V0 = "core_sim_matrix_sweep_report_v0"
SCENARIO_VARIANT_RUN_REPORT_SCHEMA_VERSION_V0 = "scenario_variant_run_report_v0"
SCENARIO_VARIANT_WORKFLOW_REPORT_SCHEMA_VERSION_V0 = "scenario_variant_workflow_report_v0"
SCENARIO_BATCH_COMPARISON_REPORT_SCHEMA_VERSION_V0 = "scenario_batch_comparison_report_v0"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare scenario variant workflow results against object-sim matrix sweep results."
    )
    parser.add_argument("--variant-workflow-report", required=True, help="Path to scenario_variant_workflow report")
    parser.add_argument("--matrix-sweep-report", required=True, help="Path to core_sim_matrix_sweep report")
    parser.add_argument("--out-report", required=True, help="Output JSON comparison report path")
    parser.add_argument(
        "--markdown-out",
        default="",
        help="Optional Markdown report path; defaults next to --out-report",
    )
    parser.add_argument(
        "--gate-profile",
        default="",
        help="Optional JSON gate profile path; explicit CLI gate args override profile values",
    )
    parser.add_argument(
        "--gate-profile-id",
        default="",
        help="Optional gate profile preset ID under --gate-profile-dir (without .json)",
    )
    parser.add_argument(
        "--gate-profile-dir",
        default="",
        help="Optional gate profile directory for --gate-profile-id; defaults to repo fixture profile directory",
    )
    parser.add_argument(
        "--gate-max-attention-rows",
        default="",
        help="Optional maximum allowed attention rows before gate failure",
    )
    parser.add_argument(
        "--gate-max-collision-rows",
        default="",
        help="Optional maximum allowed collision rows before gate failure",
    )
    parser.add_argument(
        "--gate-max-timeout-rows",
        default="",
        help="Optional maximum allowed timeout rows before gate failure",
    )
    parser.add_argument(
        "--gate-max-path-conflict-rows",
        default="",
        help="Optional maximum allowed path-conflict rows before gate failure",
    )
    parser.add_argument(
        "--gate-max-merge-conflict-rows",
        default="",
        help="Optional maximum allowed merge-conflict rows before gate failure",
    )
    parser.add_argument(
        "--gate-max-lane-change-conflict-rows",
        default="",
        help="Optional maximum allowed lane-change-conflict rows before gate failure",
    )
    parser.add_argument(
        "--gate-max-avoidance-rows",
        default="",
        help="Optional maximum allowed avoidance-active rows before gate failure",
    )
    parser.add_argument(
        "--gate-max-avoidance-brake-events",
        default="",
        help="Optional maximum allowed aggregated avoidance brake events before gate failure",
    )
    parser.add_argument(
        "--gate-max-avoidance-same-lane-conflict-triggers",
        default="",
        help="Optional maximum allowed same-lane avoidance trigger count before gate failure",
    )
    parser.add_argument(
        "--gate-max-avoidance-merge-conflict-triggers",
        default="",
        help="Optional maximum allowed merge-conflict avoidance trigger count before gate failure",
    )
    parser.add_argument(
        "--gate-max-avoidance-lane-change-conflict-triggers",
        default="",
        help="Optional maximum allowed lane-change-conflict avoidance trigger count before gate failure",
    )
    parser.add_argument(
        "--gate-max-avoidance-downstream-route-conflict-triggers",
        default="",
        help="Optional maximum allowed downstream-route avoidance trigger count before gate failure",
    )
    parser.add_argument(
        "--gate-min-min-ttc-any-lane-sec",
        default="",
        help="Optional minimum allowed global minimum TTC before gate failure",
    )
    parser.add_argument(
        "--gate-min-min-ttc-path-conflict-sec",
        default="",
        help="Optional minimum allowed global minimum path-conflict TTC before gate failure",
    )
    return parser.parse_args(argv)


def _load_json_dict(path: Path, *, label: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _load_variant_workflow_report(path: Path) -> dict[str, Any]:
    payload = _load_json_dict(path, label="scenario variant workflow report")
    schema_version = str(payload.get("scenario_variant_workflow_report_schema_version", "")).strip()
    if schema_version != SCENARIO_VARIANT_WORKFLOW_REPORT_SCHEMA_VERSION_V0:
        raise ValueError(
            "scenario_variant_workflow_report_schema_version must be "
            f"{SCENARIO_VARIANT_WORKFLOW_REPORT_SCHEMA_VERSION_V0}"
        )
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict):
        raise ValueError("scenario variant workflow report missing artifacts block")
    return payload


def _load_variant_run_report(path: Path) -> dict[str, Any]:
    payload = _load_json_dict(path, label="scenario variant run report")
    schema_version = str(payload.get("scenario_variant_run_report_schema_version", "")).strip()
    if schema_version != SCENARIO_VARIANT_RUN_REPORT_SCHEMA_VERSION_V0:
        raise ValueError(
            "scenario_variant_run_report_schema_version must be "
            f"{SCENARIO_VARIANT_RUN_REPORT_SCHEMA_VERSION_V0}"
        )
    variant_runs = payload.get("variant_runs")
    if not isinstance(variant_runs, list):
        raise ValueError("scenario variant run report missing variant_runs list")
    return payload


def _load_matrix_sweep_report(path: Path) -> dict[str, Any]:
    payload = _load_json_dict(path, label="scenario matrix sweep report")
    schema_version = str(payload.get("core_sim_matrix_sweep_schema_version", "")).strip()
    if schema_version != CORE_SIM_MATRIX_SWEEP_SCHEMA_VERSION_V0:
        raise ValueError(
            "core_sim_matrix_sweep_schema_version must be "
            f"{CORE_SIM_MATRIX_SWEEP_SCHEMA_VERSION_V0}"
        )
    cases = payload.get("cases")
    if not isinstance(cases, list):
        raise ValueError("scenario matrix sweep report missing cases list")
    return payload


def _load_summary_payload(summary_path_value: Any) -> dict[str, Any]:
    summary_path_text = str(summary_path_value or "").strip()
    if not summary_path_text:
        return {}
    summary_path = Path(summary_path_text)
    if not summary_path.is_file():
        return {}
    payload = _load_json_dict(summary_path, label="object sim summary")
    return payload


def _load_lane_risk_summary_payload(lane_risk_summary_path_value: Any) -> dict[str, Any]:
    lane_risk_summary_path_text = str(lane_risk_summary_path_value or "").strip()
    if not lane_risk_summary_path_text:
        return {}
    lane_risk_summary_path = Path(lane_risk_summary_path_text)
    if not lane_risk_summary_path.is_file():
        return {}
    payload = _load_json_dict(lane_risk_summary_path, label="lane risk summary")
    return payload


def _coerce_optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _resolve_gate_policy(
    *,
    gate_profile_path: Path | None,
    gate_max_attention_rows: int | None,
    gate_max_collision_rows: int | None,
    gate_max_timeout_rows: int | None,
    gate_max_path_conflict_rows: int | None,
    gate_max_merge_conflict_rows: int | None,
    gate_max_lane_change_conflict_rows: int | None,
    gate_max_avoidance_rows: int | None,
    gate_max_avoidance_brake_events: int | None,
    gate_max_avoidance_same_lane_conflict_triggers: int | None,
    gate_max_avoidance_merge_conflict_triggers: int | None,
    gate_max_avoidance_lane_change_conflict_triggers: int | None,
    gate_max_avoidance_downstream_route_conflict_triggers: int | None,
    gate_min_min_ttc_any_lane_sec: float | None,
    gate_min_min_ttc_path_conflict_sec: float | None,
) -> dict[str, Any]:
    profile_payload: dict[str, Any] | None = None
    profile_policy: dict[str, Any] = {}
    if gate_profile_path is not None:
        profile_payload = load_scenario_batch_gate_profile(gate_profile_path)
        raw_policy = profile_payload["policy"]
        profile_policy = {
            "max_attention_rows": _parse_optional_non_negative_int(
                raw_policy.get("max_attention_rows"),
                field="policy.max_attention_rows",
            ),
            "max_collision_rows": _parse_optional_non_negative_int(
                raw_policy.get("max_collision_rows"),
                field="policy.max_collision_rows",
            ),
            "max_timeout_rows": _parse_optional_non_negative_int(
                raw_policy.get("max_timeout_rows"),
                field="policy.max_timeout_rows",
            ),
            "max_path_conflict_rows": _parse_optional_non_negative_int(
                raw_policy.get("max_path_conflict_rows"),
                field="policy.max_path_conflict_rows",
            ),
            "max_merge_conflict_rows": _parse_optional_non_negative_int(
                raw_policy.get("max_merge_conflict_rows"),
                field="policy.max_merge_conflict_rows",
            ),
            "max_lane_change_conflict_rows": _parse_optional_non_negative_int(
                raw_policy.get("max_lane_change_conflict_rows"),
                field="policy.max_lane_change_conflict_rows",
            ),
            "max_avoidance_rows": _parse_optional_non_negative_int(
                raw_policy.get("max_avoidance_rows"),
                field="policy.max_avoidance_rows",
            ),
            "max_avoidance_brake_events": _parse_optional_non_negative_int(
                raw_policy.get("max_avoidance_brake_events"),
                field="policy.max_avoidance_brake_events",
            ),
            "max_avoidance_same_lane_conflict_triggers": _parse_optional_non_negative_int(
                raw_policy.get("max_avoidance_same_lane_conflict_triggers"),
                field="policy.max_avoidance_same_lane_conflict_triggers",
            ),
            "max_avoidance_merge_conflict_triggers": _parse_optional_non_negative_int(
                raw_policy.get("max_avoidance_merge_conflict_triggers"),
                field="policy.max_avoidance_merge_conflict_triggers",
            ),
            "max_avoidance_lane_change_conflict_triggers": _parse_optional_non_negative_int(
                raw_policy.get("max_avoidance_lane_change_conflict_triggers"),
                field="policy.max_avoidance_lane_change_conflict_triggers",
            ),
            "max_avoidance_downstream_route_conflict_triggers": _parse_optional_non_negative_int(
                raw_policy.get("max_avoidance_downstream_route_conflict_triggers"),
                field="policy.max_avoidance_downstream_route_conflict_triggers",
            ),
            "min_min_ttc_any_lane_sec": _parse_optional_non_negative_float(
                raw_policy.get("min_min_ttc_any_lane_sec"),
                field="policy.min_min_ttc_any_lane_sec",
            ),
            "min_min_ttc_path_conflict_sec": _parse_optional_non_negative_float(
                raw_policy.get("min_min_ttc_path_conflict_sec"),
                field="policy.min_min_ttc_path_conflict_sec",
            ),
        }
    return {
        "profile_path": str(gate_profile_path.resolve()) if gate_profile_path is not None else None,
        "profile_id": (
            str(profile_payload.get("profile_id", "")).strip() or None
            if profile_payload is not None
            else None
        ),
        "max_attention_rows": (
            gate_max_attention_rows
            if gate_max_attention_rows is not None
            else profile_policy.get("max_attention_rows")
        ),
        "max_collision_rows": (
            gate_max_collision_rows
            if gate_max_collision_rows is not None
            else profile_policy.get("max_collision_rows")
        ),
        "max_timeout_rows": (
            gate_max_timeout_rows
            if gate_max_timeout_rows is not None
            else profile_policy.get("max_timeout_rows")
        ),
        "max_path_conflict_rows": (
            gate_max_path_conflict_rows
            if gate_max_path_conflict_rows is not None
            else profile_policy.get("max_path_conflict_rows")
        ),
        "max_merge_conflict_rows": (
            gate_max_merge_conflict_rows
            if gate_max_merge_conflict_rows is not None
            else profile_policy.get("max_merge_conflict_rows")
        ),
        "max_lane_change_conflict_rows": (
            gate_max_lane_change_conflict_rows
            if gate_max_lane_change_conflict_rows is not None
            else profile_policy.get("max_lane_change_conflict_rows")
        ),
        "max_avoidance_rows": (
            gate_max_avoidance_rows
            if gate_max_avoidance_rows is not None
            else profile_policy.get("max_avoidance_rows")
        ),
        "max_avoidance_brake_events": (
            gate_max_avoidance_brake_events
            if gate_max_avoidance_brake_events is not None
            else profile_policy.get("max_avoidance_brake_events")
        ),
        "max_avoidance_same_lane_conflict_triggers": (
            gate_max_avoidance_same_lane_conflict_triggers
            if gate_max_avoidance_same_lane_conflict_triggers is not None
            else profile_policy.get("max_avoidance_same_lane_conflict_triggers")
        ),
        "max_avoidance_merge_conflict_triggers": (
            gate_max_avoidance_merge_conflict_triggers
            if gate_max_avoidance_merge_conflict_triggers is not None
            else profile_policy.get("max_avoidance_merge_conflict_triggers")
        ),
        "max_avoidance_lane_change_conflict_triggers": (
            gate_max_avoidance_lane_change_conflict_triggers
            if gate_max_avoidance_lane_change_conflict_triggers is not None
            else profile_policy.get("max_avoidance_lane_change_conflict_triggers")
        ),
        "max_avoidance_downstream_route_conflict_triggers": (
            gate_max_avoidance_downstream_route_conflict_triggers
            if gate_max_avoidance_downstream_route_conflict_triggers is not None
            else profile_policy.get("max_avoidance_downstream_route_conflict_triggers")
        ),
        "min_min_ttc_any_lane_sec": (
            gate_min_min_ttc_any_lane_sec
            if gate_min_min_ttc_any_lane_sec is not None
            else profile_policy.get("min_min_ttc_any_lane_sec")
        ),
        "min_min_ttc_path_conflict_sec": (
            gate_min_min_ttc_path_conflict_sec
            if gate_min_min_ttc_path_conflict_sec is not None
            else profile_policy.get("min_min_ttc_path_conflict_sec")
        ),
    }


def _bool_value(value: Any) -> bool:
    return bool(value)


def _extract_path_interaction_fields(lane_risk_summary: dict[str, Any]) -> dict[str, Any]:
    raw_counts = lane_risk_summary.get("path_interaction_counts", {})
    path_interaction_counts = dict(raw_counts) if isinstance(raw_counts, dict) else {}
    return {
        "path_interaction_counts": path_interaction_counts,
        "path_conflict_rows": int(lane_risk_summary.get("path_conflict_rows", 0) or 0),
        "merge_conflict_rows": int(lane_risk_summary.get("merge_conflict_rows", 0) or 0),
        "lane_change_conflict_rows": int(lane_risk_summary.get("lane_change_conflict_rows", 0) or 0),
        "lane_change_clear_rows": int(lane_risk_summary.get("lane_change_clear_rows", 0) or 0),
        "diverge_clear_rows": int(lane_risk_summary.get("diverge_clear_rows", 0) or 0),
        "downstream_route_conflict_rows": int(
            lane_risk_summary.get("downstream_route_conflict_rows", 0) or 0
        ),
        "min_ttc_path_conflict_sec": _coerce_optional_float(
            lane_risk_summary.get("min_ttc_path_conflict_sec")
        ),
        "min_ttc_merge_conflict_sec": _coerce_optional_float(
            lane_risk_summary.get("min_ttc_merge_conflict_sec")
        ),
        "min_ttc_lane_change_conflict_sec": _coerce_optional_float(
            lane_risk_summary.get("min_ttc_lane_change_conflict_sec")
        ),
        "min_ttc_downstream_route_conflict_sec": _coerce_optional_float(
            lane_risk_summary.get("min_ttc_downstream_route_conflict_sec")
        ),
    }


def _extract_avoidance_fields(summary: dict[str, Any]) -> dict[str, Any]:
    raw_counts = summary.get("ego_avoidance_trigger_counts_by_interaction_kind", {})
    trigger_counts = dict(raw_counts) if isinstance(raw_counts, dict) else {}
    return {
        "ego_avoidance_brake_event_count": int(summary.get("ego_avoidance_brake_event_count", 0) or 0),
        "ego_avoidance_applied_brake_mps2_max": _coerce_optional_float(
            summary.get("ego_avoidance_applied_brake_mps2_max")
        ),
        "ego_avoidance_last_trigger_interaction_kind": (
            str(summary.get("ego_avoidance_last_trigger_interaction_kind", "")).strip() or None
        ),
        "ego_avoidance_last_trigger_priority": (
            None
            if summary.get("ego_avoidance_last_trigger_priority") is None
            else int(summary.get("ego_avoidance_last_trigger_priority"))
        ),
        "ego_avoidance_last_trigger_max_gap_m": _coerce_optional_float(
            summary.get("ego_avoidance_last_trigger_max_gap_m")
        ),
        "ego_avoidance_trigger_counts_by_interaction_kind": trigger_counts,
    }


def _normalize_text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            out.append(text)
    return out


def _extract_route_lane_fields(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "ego_lane_id": str(summary.get("ego_lane_id", "")).strip() or None,
        "ego_route_lane_id": str(summary.get("ego_route_lane_id", "")).strip() or None,
        "ego_lane_binding_mode": str(summary.get("ego_lane_binding_mode", "")).strip() or None,
        "ego_route_binding_mode": str(summary.get("ego_route_binding_mode", "")).strip() or None,
        "traffic_npc_lane_id_profile": _normalize_text_list(summary.get("traffic_npc_lane_id_profile")),
        "traffic_npc_route_lane_id_profile": _normalize_text_list(
            summary.get("traffic_npc_route_lane_id_profile")
        ),
        "traffic_npc_lane_binding_modes": _normalize_text_list(summary.get("traffic_npc_lane_binding_modes")),
        "traffic_npc_route_binding_modes": _normalize_text_list(
            summary.get("traffic_npc_route_binding_modes")
        ),
    }


def _build_variant_batch_rows(variant_run_report: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for run in variant_run_report.get("variant_runs", []):
        if not isinstance(run, dict):
            continue
        summary = _load_summary_payload(run.get("summary_path"))
        lane_risk_summary = _load_lane_risk_summary_payload(run.get("lane_risk_summary_path"))
        path_interaction_fields = _extract_path_interaction_fields(lane_risk_summary)
        avoidance_fields = _extract_avoidance_fields(summary)
        route_lane_fields = _extract_route_lane_fields(summary)
        object_sim_status = str(summary.get("status", run.get("object_sim_status", ""))).strip() or None
        termination_reason = str(summary.get("termination_reason", run.get("termination_reason", ""))).strip() or None
        row = {
            "source_batch": "variant_workflow",
            "row_id": str(run.get("variant_id", "")).strip() or None,
            "group_id": str(run.get("logical_scenario_id", "")).strip() or None,
            "group_type": "logical_scenario",
            "execution_status": str(run.get("execution_status", "")).strip() or None,
            "object_sim_status": object_sim_status,
            "termination_reason": termination_reason,
            "rendered_payload_kind": str(run.get("rendered_payload_kind", "")).strip() or None,
            "execution_path": str(run.get("execution_path", "")).strip() or None,
            "collision": _bool_value(summary.get("collision", False)),
            "timeout": _bool_value(summary.get("timeout", False)),
            "min_ttc_any_lane_sec": _coerce_optional_float(summary.get("min_ttc_any_lane_sec")),
            "failure_code": str(run.get("failure_code", "")).strip() or None,
            "failure_reason": str(run.get("failure_reason", "")).strip() or None,
            "summary_path": str(run.get("summary_path", "")).strip() or None,
            "lane_risk_summary_path": str(run.get("lane_risk_summary_path", "")).strip() or None,
        }
        row.update(path_interaction_fields)
        row.update(avoidance_fields)
        row.update(route_lane_fields)
        rows.append(row)
    return rows


def _matrix_group_id(case: dict[str, Any]) -> str:
    traffic_profile_id = str(case.get("traffic_profile_id", "")).strip() or "<missing-profile>"
    traffic_actor_pattern_id = str(case.get("traffic_actor_pattern_id", "")).strip() or "<missing-pattern>"
    return f"{traffic_profile_id}::{traffic_actor_pattern_id}"


def _build_matrix_batch_rows(matrix_sweep_report: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for case in matrix_sweep_report.get("cases", []):
        if not isinstance(case, dict):
            continue
        summary = _load_summary_payload(case.get("summary_path")) if case.get("summary_exists") else {}
        lane_risk_summary = (
            _load_lane_risk_summary_payload(case.get("lane_risk_summary_path"))
            if case.get("lane_risk_summary_exists")
            else {}
        )
        path_interaction_fields = _extract_path_interaction_fields(lane_risk_summary)
        avoidance_fields = _extract_avoidance_fields(summary)
        route_lane_fields = _extract_route_lane_fields(summary)
        status_value = str(case.get("status", summary.get("status", ""))).strip() or None
        termination_reason = str(summary.get("termination_reason", "")).strip() or None
        row = {
            "source_batch": "matrix_sweep",
            "row_id": str(case.get("run_id", "")).strip() or None,
            "group_id": _matrix_group_id(case),
            "group_type": "matrix_group",
            "execution_status": "SUCCEEDED" if int(case.get("returncode", 2)) == 0 else "FAILED",
            "object_sim_status": status_value,
            "termination_reason": termination_reason,
            "rendered_payload_kind": None,
            "execution_path": "matrix_object_sim",
            "collision": _bool_value(case.get("collision", False)),
            "timeout": _bool_value(case.get("timeout", False)),
            "min_ttc_any_lane_sec": _coerce_optional_float(case.get("min_ttc_any_lane_sec")),
            "failure_code": "EXECUTION_ERROR" if int(case.get("returncode", 2)) != 0 else None,
            "failure_reason": str(case.get("stderr_tail", "")).strip() or None,
            "summary_path": str(case.get("summary_path", "")).strip() or None,
            "lane_risk_summary_path": str(case.get("lane_risk_summary_path", "")).strip() or None,
            "traffic_profile_id": str(case.get("traffic_profile_id", "")).strip() or None,
            "traffic_actor_pattern_id": str(case.get("traffic_actor_pattern_id", "")).strip() or None,
            "traffic_npc_speed_scale": _coerce_optional_float(case.get("traffic_npc_speed_scale")),
            "tire_friction_coeff": _coerce_optional_float(case.get("tire_friction_coeff")),
            "surface_friction_scale": _coerce_optional_float(case.get("surface_friction_scale")),
        }
        row.update(path_interaction_fields)
        row.update(avoidance_fields)
        row.update(route_lane_fields)
        rows.append(row)
    return rows


def _build_logical_scenario_rows(variant_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in variant_rows:
        logical_scenario_id = str(row.get("group_id", "")).strip() or "<missing>"
        group = grouped.setdefault(
            logical_scenario_id,
            {
                "variant_count": 0,
                "execution_status_counts": Counter(),
                "object_sim_status_counts": Counter(),
                "payload_kind_counts": Counter(),
                "execution_path_counts": Counter(),
                "collision_count": 0,
                "timeout_count": 0,
                "min_ttc_any_lane_sec_min": None,
                "path_conflict_row_count": 0,
                "merge_conflict_row_count": 0,
                "lane_change_conflict_row_count": 0,
                "diverge_clear_row_count": 0,
                "path_interaction_counts": Counter(),
                "min_ttc_path_conflict_sec_min": None,
                "ego_avoidance_row_count": 0,
                "ego_avoidance_brake_event_count_total": 0,
                "ego_avoidance_trigger_counts_by_interaction_kind": Counter(),
                "ego_avoidance_last_trigger_interaction_kind_counts": Counter(),
                "ego_avoidance_last_trigger_priority_values": set(),
                "ego_avoidance_last_trigger_max_gap_m_values": set(),
                "ego_route_lane_ids": set(),
                "traffic_npc_route_lane_id_profiles": set(),
                "traffic_npc_route_binding_mode_profiles": set(),
                "row_ids": [],
            },
        )
        group["variant_count"] += 1
        group["row_ids"].append(str(row.get("row_id", "")).strip())
        execution_status = str(row.get("execution_status", "")).strip()
        if execution_status:
            group["execution_status_counts"][execution_status] += 1
        object_sim_status = str(row.get("object_sim_status", "")).strip()
        if object_sim_status:
            group["object_sim_status_counts"][object_sim_status] += 1
        payload_kind = str(row.get("rendered_payload_kind", "")).strip()
        if payload_kind:
            group["payload_kind_counts"][payload_kind] += 1
        execution_path = str(row.get("execution_path", "")).strip()
        if execution_path:
            group["execution_path_counts"][execution_path] += 1
        if row.get("collision"):
            group["collision_count"] += 1
        if row.get("timeout"):
            group["timeout_count"] += 1
        group["path_conflict_row_count"] += int(row.get("path_conflict_rows", 0) or 0)
        group["merge_conflict_row_count"] += int(row.get("merge_conflict_rows", 0) or 0)
        group["lane_change_conflict_row_count"] += int(row.get("lane_change_conflict_rows", 0) or 0)
        group["diverge_clear_row_count"] += int(row.get("diverge_clear_rows", 0) or 0)
        for label, count in dict(row.get("path_interaction_counts", {})).items():
            group["path_interaction_counts"][str(label)] += int(count)
        brake_event_count = int(row.get("ego_avoidance_brake_event_count", 0) or 0)
        if brake_event_count > 0:
            group["ego_avoidance_row_count"] += 1
        group["ego_avoidance_brake_event_count_total"] += brake_event_count
        for label, count in dict(row.get("ego_avoidance_trigger_counts_by_interaction_kind", {})).items():
            group["ego_avoidance_trigger_counts_by_interaction_kind"][str(label)] += int(count)
        last_trigger_kind = str(row.get("ego_avoidance_last_trigger_interaction_kind", "")).strip()
        if last_trigger_kind:
            group["ego_avoidance_last_trigger_interaction_kind_counts"][last_trigger_kind] += 1
        last_trigger_priority = row.get("ego_avoidance_last_trigger_priority")
        if last_trigger_priority is not None:
            group["ego_avoidance_last_trigger_priority_values"].add(int(last_trigger_priority))
        last_trigger_max_gap_m = _coerce_optional_float(row.get("ego_avoidance_last_trigger_max_gap_m"))
        if last_trigger_max_gap_m is not None:
            group["ego_avoidance_last_trigger_max_gap_m_values"].add(float(last_trigger_max_gap_m))
        ego_route_lane_id = str(row.get("ego_route_lane_id", "")).strip()
        if ego_route_lane_id:
            group["ego_route_lane_ids"].add(ego_route_lane_id)
        npc_route_lane_profile = tuple(_normalize_text_list(row.get("traffic_npc_route_lane_id_profile")))
        if npc_route_lane_profile:
            group["traffic_npc_route_lane_id_profiles"].add(npc_route_lane_profile)
        npc_route_binding_profile = tuple(_normalize_text_list(row.get("traffic_npc_route_binding_modes")))
        if npc_route_binding_profile:
            group["traffic_npc_route_binding_mode_profiles"].add(npc_route_binding_profile)
        ttc_value = _coerce_optional_float(row.get("min_ttc_any_lane_sec"))
        current_min = group["min_ttc_any_lane_sec_min"]
        if ttc_value is not None and (current_min is None or ttc_value < current_min):
            group["min_ttc_any_lane_sec_min"] = ttc_value
        path_ttc_value = _coerce_optional_float(row.get("min_ttc_path_conflict_sec"))
        current_path_min = group["min_ttc_path_conflict_sec_min"]
        if path_ttc_value is not None and (current_path_min is None or path_ttc_value < current_path_min):
            group["min_ttc_path_conflict_sec_min"] = path_ttc_value
    return [
        {
            "logical_scenario_id": logical_scenario_id,
            "variant_count": int(group["variant_count"]),
            "execution_status_counts": dict(sorted(group["execution_status_counts"].items())),
            "object_sim_status_counts": dict(sorted(group["object_sim_status_counts"].items())),
            "payload_kind_counts": dict(sorted(group["payload_kind_counts"].items())),
            "execution_path_counts": dict(sorted(group["execution_path_counts"].items())),
            "collision_count": int(group["collision_count"]),
            "timeout_count": int(group["timeout_count"]),
            "min_ttc_any_lane_sec_min": group["min_ttc_any_lane_sec_min"],
            "path_conflict_row_count": int(group["path_conflict_row_count"]),
            "merge_conflict_row_count": int(group["merge_conflict_row_count"]),
            "lane_change_conflict_row_count": int(group["lane_change_conflict_row_count"]),
            "diverge_clear_row_count": int(group["diverge_clear_row_count"]),
            "path_interaction_counts": dict(sorted(group["path_interaction_counts"].items())),
            "min_ttc_path_conflict_sec_min": group["min_ttc_path_conflict_sec_min"],
            "ego_avoidance_row_count": int(group["ego_avoidance_row_count"]),
            "ego_avoidance_brake_event_count_total": int(group["ego_avoidance_brake_event_count_total"]),
            "ego_avoidance_trigger_counts_by_interaction_kind": dict(
                sorted(group["ego_avoidance_trigger_counts_by_interaction_kind"].items())
            ),
            "ego_avoidance_last_trigger_interaction_kind_counts": dict(
                sorted(group["ego_avoidance_last_trigger_interaction_kind_counts"].items())
            ),
            "ego_avoidance_last_trigger_priority_values": sorted(group["ego_avoidance_last_trigger_priority_values"]),
            "ego_avoidance_last_trigger_max_gap_m_values": sorted(
                group["ego_avoidance_last_trigger_max_gap_m_values"]
            ),
            "ego_route_lane_ids": sorted(group["ego_route_lane_ids"]),
            "traffic_npc_route_lane_id_profiles": [
                list(profile) for profile in sorted(group["traffic_npc_route_lane_id_profiles"])
            ],
            "traffic_npc_route_binding_mode_profiles": [
                list(profile) for profile in sorted(group["traffic_npc_route_binding_mode_profiles"])
            ],
            "row_ids": list(group["row_ids"]),
        }
        for logical_scenario_id, group in sorted(grouped.items())
    ]


def _build_matrix_group_rows(matrix_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in matrix_rows:
        group_id = str(row.get("group_id", "")).strip() or "<missing>"
        traffic_profile_id = str(row.get("traffic_profile_id", "")).strip() or "<missing-profile>"
        traffic_actor_pattern_id = str(row.get("traffic_actor_pattern_id", "")).strip() or "<missing-pattern>"
        group = grouped.setdefault(
            group_id,
            {
                "traffic_profile_id": traffic_profile_id,
                "traffic_actor_pattern_id": traffic_actor_pattern_id,
                "case_count": 0,
                "execution_status_counts": Counter(),
                "object_sim_status_counts": Counter(),
                "collision_count": 0,
                "timeout_count": 0,
                "min_ttc_any_lane_sec_min": None,
                "path_conflict_row_count": 0,
                "merge_conflict_row_count": 0,
                "lane_change_conflict_row_count": 0,
                "diverge_clear_row_count": 0,
                "path_interaction_counts": Counter(),
                "min_ttc_path_conflict_sec_min": None,
                "ego_avoidance_row_count": 0,
                "ego_avoidance_brake_event_count_total": 0,
                "ego_avoidance_trigger_counts_by_interaction_kind": Counter(),
                "ego_avoidance_last_trigger_interaction_kind_counts": Counter(),
                "ego_avoidance_last_trigger_priority_values": set(),
                "ego_avoidance_last_trigger_max_gap_m_values": set(),
                "ego_route_lane_ids": set(),
                "traffic_npc_route_lane_id_profiles": set(),
                "traffic_npc_route_binding_mode_profiles": set(),
                "traffic_npc_speed_scale_values": set(),
                "tire_friction_coeff_values": set(),
                "surface_friction_scale_values": set(),
                "row_ids": [],
            },
        )
        group["case_count"] += 1
        group["row_ids"].append(str(row.get("row_id", "")).strip())
        execution_status = str(row.get("execution_status", "")).strip()
        if execution_status:
            group["execution_status_counts"][execution_status] += 1
        object_sim_status = str(row.get("object_sim_status", "")).strip()
        if object_sim_status:
            group["object_sim_status_counts"][object_sim_status] += 1
        if row.get("collision"):
            group["collision_count"] += 1
        if row.get("timeout"):
            group["timeout_count"] += 1
        group["path_conflict_row_count"] += int(row.get("path_conflict_rows", 0) or 0)
        group["merge_conflict_row_count"] += int(row.get("merge_conflict_rows", 0) or 0)
        group["lane_change_conflict_row_count"] += int(row.get("lane_change_conflict_rows", 0) or 0)
        group["diverge_clear_row_count"] += int(row.get("diverge_clear_rows", 0) or 0)
        for label, count in dict(row.get("path_interaction_counts", {})).items():
            group["path_interaction_counts"][str(label)] += int(count)
        brake_event_count = int(row.get("ego_avoidance_brake_event_count", 0) or 0)
        if brake_event_count > 0:
            group["ego_avoidance_row_count"] += 1
        group["ego_avoidance_brake_event_count_total"] += brake_event_count
        for label, count in dict(row.get("ego_avoidance_trigger_counts_by_interaction_kind", {})).items():
            group["ego_avoidance_trigger_counts_by_interaction_kind"][str(label)] += int(count)
        last_trigger_kind = str(row.get("ego_avoidance_last_trigger_interaction_kind", "")).strip()
        if last_trigger_kind:
            group["ego_avoidance_last_trigger_interaction_kind_counts"][last_trigger_kind] += 1
        last_trigger_priority = row.get("ego_avoidance_last_trigger_priority")
        if last_trigger_priority is not None:
            group["ego_avoidance_last_trigger_priority_values"].add(int(last_trigger_priority))
        last_trigger_max_gap_m = _coerce_optional_float(row.get("ego_avoidance_last_trigger_max_gap_m"))
        if last_trigger_max_gap_m is not None:
            group["ego_avoidance_last_trigger_max_gap_m_values"].add(float(last_trigger_max_gap_m))
        ego_route_lane_id = str(row.get("ego_route_lane_id", "")).strip()
        if ego_route_lane_id:
            group["ego_route_lane_ids"].add(ego_route_lane_id)
        npc_route_lane_profile = tuple(_normalize_text_list(row.get("traffic_npc_route_lane_id_profile")))
        if npc_route_lane_profile:
            group["traffic_npc_route_lane_id_profiles"].add(npc_route_lane_profile)
        npc_route_binding_profile = tuple(_normalize_text_list(row.get("traffic_npc_route_binding_modes")))
        if npc_route_binding_profile:
            group["traffic_npc_route_binding_mode_profiles"].add(npc_route_binding_profile)
        ttc_value = _coerce_optional_float(row.get("min_ttc_any_lane_sec"))
        current_min = group["min_ttc_any_lane_sec_min"]
        if ttc_value is not None and (current_min is None or ttc_value < current_min):
            group["min_ttc_any_lane_sec_min"] = ttc_value
        path_ttc_value = _coerce_optional_float(row.get("min_ttc_path_conflict_sec"))
        current_path_min = group["min_ttc_path_conflict_sec_min"]
        if path_ttc_value is not None and (current_path_min is None or path_ttc_value < current_path_min):
            group["min_ttc_path_conflict_sec_min"] = path_ttc_value
        for field_name in (
            "traffic_npc_speed_scale_values",
            "tire_friction_coeff_values",
            "surface_friction_scale_values",
        ):
            source_field = field_name.replace("_values", "")
            value = _coerce_optional_float(row.get(source_field))
            if value is not None:
                group[field_name].add(float(value))
    return [
        {
            "matrix_group_id": group_id,
            "traffic_profile_id": group["traffic_profile_id"],
            "traffic_actor_pattern_id": group["traffic_actor_pattern_id"],
            "case_count": int(group["case_count"]),
            "execution_status_counts": dict(sorted(group["execution_status_counts"].items())),
            "object_sim_status_counts": dict(sorted(group["object_sim_status_counts"].items())),
            "collision_count": int(group["collision_count"]),
            "timeout_count": int(group["timeout_count"]),
            "min_ttc_any_lane_sec_min": group["min_ttc_any_lane_sec_min"],
            "path_conflict_row_count": int(group["path_conflict_row_count"]),
            "merge_conflict_row_count": int(group["merge_conflict_row_count"]),
            "lane_change_conflict_row_count": int(group["lane_change_conflict_row_count"]),
            "diverge_clear_row_count": int(group["diverge_clear_row_count"]),
            "path_interaction_counts": dict(sorted(group["path_interaction_counts"].items())),
            "min_ttc_path_conflict_sec_min": group["min_ttc_path_conflict_sec_min"],
            "ego_avoidance_row_count": int(group["ego_avoidance_row_count"]),
            "ego_avoidance_brake_event_count_total": int(group["ego_avoidance_brake_event_count_total"]),
            "ego_avoidance_trigger_counts_by_interaction_kind": dict(
                sorted(group["ego_avoidance_trigger_counts_by_interaction_kind"].items())
            ),
            "ego_avoidance_last_trigger_interaction_kind_counts": dict(
                sorted(group["ego_avoidance_last_trigger_interaction_kind_counts"].items())
            ),
            "ego_avoidance_last_trigger_priority_values": sorted(group["ego_avoidance_last_trigger_priority_values"]),
            "ego_avoidance_last_trigger_max_gap_m_values": sorted(
                group["ego_avoidance_last_trigger_max_gap_m_values"]
            ),
            "ego_route_lane_ids": sorted(group["ego_route_lane_ids"]),
            "traffic_npc_route_lane_id_profiles": [
                list(profile) for profile in sorted(group["traffic_npc_route_lane_id_profiles"])
            ],
            "traffic_npc_route_binding_mode_profiles": [
                list(profile) for profile in sorted(group["traffic_npc_route_binding_mode_profiles"])
            ],
            "traffic_npc_speed_scale_values": sorted(group["traffic_npc_speed_scale_values"]),
            "tire_friction_coeff_values": sorted(group["tire_friction_coeff_values"]),
            "surface_friction_scale_values": sorted(group["surface_friction_scale_values"]),
            "row_ids": list(group["row_ids"]),
        }
        for group_id, group in sorted(grouped.items())
    ]


def _build_attention_rows(batch_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in batch_rows:
        execution_status = str(row.get("execution_status", "")).strip()
        object_sim_status = str(row.get("object_sim_status", "")).strip()
        path_ttc_value = _coerce_optional_float(row.get("min_ttc_path_conflict_sec"))
        risky_path_conflict = (
            path_ttc_value is not None
            and path_ttc_value <= 3.0
            and (
                int(row.get("merge_conflict_rows", 0) or 0) > 0
                or int(row.get("lane_change_conflict_rows", 0) or 0) > 0
                or int(row.get("downstream_route_conflict_rows", 0) or 0) > 0
            )
        )
        if (
            execution_status == "SUCCEEDED"
            and object_sim_status in {"", "success"}
            and not bool(row.get("collision"))
            and not bool(row.get("timeout"))
            and not risky_path_conflict
        ):
            continue
        attention_reasons: list[str] = []
        if execution_status and execution_status != "SUCCEEDED":
            attention_reasons.append("EXECUTION_FAILURE_PRESENT")
        if bool(row.get("collision")):
            attention_reasons.append("COLLISION_PRESENT")
        if bool(row.get("timeout")):
            attention_reasons.append("TIMEOUT_PRESENT")
        if int(row.get("path_conflict_rows", 0) or 0) > 0:
            attention_reasons.append("PATH_CONFLICT_PRESENT")
        if int(row.get("merge_conflict_rows", 0) or 0) > 0:
            attention_reasons.append("MERGE_CONFLICT_PRESENT")
        if int(row.get("lane_change_conflict_rows", 0) or 0) > 0:
            attention_reasons.append("LANE_CHANGE_CONFLICT_PRESENT")
            if (
                str(row.get("ego_route_lane_id", "")).strip()
                or _normalize_text_list(row.get("traffic_npc_route_lane_id_profile"))
            ):
                attention_reasons.append("LANE_CHANGE_ROUTE_LANE_TRACE_PRESENT")
        if risky_path_conflict:
            attention_reasons.append("PATH_TTC_UNDER_3S")
        rows.append(
            {
                "source_batch": str(row.get("source_batch", "")).strip() or None,
                "row_id": str(row.get("row_id", "")).strip() or None,
                "group_id": str(row.get("group_id", "")).strip() or None,
                "execution_status": execution_status or None,
                "object_sim_status": object_sim_status or None,
                "termination_reason": str(row.get("termination_reason", "")).strip() or None,
                "collision": bool(row.get("collision", False)),
                "timeout": bool(row.get("timeout", False)),
                "min_ttc_any_lane_sec": _coerce_optional_float(row.get("min_ttc_any_lane_sec")),
                "path_conflict_rows": int(row.get("path_conflict_rows", 0) or 0),
                "merge_conflict_rows": int(row.get("merge_conflict_rows", 0) or 0),
                "lane_change_conflict_rows": int(row.get("lane_change_conflict_rows", 0) or 0),
                "min_ttc_path_conflict_sec": path_ttc_value,
                "failure_code": str(row.get("failure_code", "")).strip() or None,
                "failure_reason": str(row.get("failure_reason", "")).strip() or None,
                "attention_reasons": attention_reasons,
                "ego_avoidance_brake_event_count": int(row.get("ego_avoidance_brake_event_count", 0) or 0),
                "ego_avoidance_last_trigger_interaction_kind": (
                    str(row.get("ego_avoidance_last_trigger_interaction_kind", "")).strip() or None
                ),
                "ego_avoidance_last_trigger_priority": (
                    None
                    if row.get("ego_avoidance_last_trigger_priority") is None
                    else int(row.get("ego_avoidance_last_trigger_priority"))
                ),
                "ego_avoidance_last_trigger_max_gap_m": _coerce_optional_float(
                    row.get("ego_avoidance_last_trigger_max_gap_m")
                ),
                "ego_route_lane_id": str(row.get("ego_route_lane_id", "")).strip() or None,
                "traffic_npc_route_lane_id_profile": _normalize_text_list(
                    row.get("traffic_npc_route_lane_id_profile")
                ),
                "traffic_npc_route_binding_modes": _normalize_text_list(
                    row.get("traffic_npc_route_binding_modes")
                ),
            }
        )
    return rows


def _build_attention_reason_counts(attention_rows: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in attention_rows:
        for reason in row.get("attention_reasons", []):
            counter[str(reason)] += 1
    return dict(sorted(counter.items()))


def _build_overview(
    *,
    workflow_report: dict[str, Any],
    matrix_report: dict[str, Any],
    variant_rows: list[dict[str, Any]],
    matrix_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    combined_execution_status_counts: Counter[str] = Counter()
    combined_object_sim_status_counts: Counter[str] = Counter()
    min_ttc_any_lane_sec_min: float | None = None
    min_ttc_any_lane_row_id = None
    min_ttc_any_lane_source_batch = None
    collision_row_count = 0
    timeout_row_count = 0
    path_conflict_row_count = 0
    merge_conflict_row_count = 0
    lane_change_conflict_row_count = 0
    ego_avoidance_row_count = 0
    ego_avoidance_brake_event_count_total = 0
    ego_avoidance_trigger_counts_by_interaction_kind: Counter[str] = Counter()
    min_ttc_path_conflict_sec_min: float | None = None
    min_ttc_path_conflict_row_id = None
    min_ttc_path_conflict_source_batch = None

    for row in list(variant_rows) + list(matrix_rows):
        execution_status = str(row.get("execution_status", "")).strip()
        if execution_status:
            combined_execution_status_counts[execution_status] += 1
        object_sim_status = str(row.get("object_sim_status", "")).strip()
        if object_sim_status:
            combined_object_sim_status_counts[object_sim_status] += 1
        if row.get("collision"):
            collision_row_count += 1
        if row.get("timeout"):
            timeout_row_count += 1
        path_conflict_row_count += int(row.get("path_conflict_rows", 0) or 0)
        merge_conflict_row_count += int(row.get("merge_conflict_rows", 0) or 0)
        lane_change_conflict_row_count += int(row.get("lane_change_conflict_rows", 0) or 0)
        brake_event_count = int(row.get("ego_avoidance_brake_event_count", 0) or 0)
        if brake_event_count > 0:
            ego_avoidance_row_count += 1
        ego_avoidance_brake_event_count_total += brake_event_count
        for label, count in dict(row.get("ego_avoidance_trigger_counts_by_interaction_kind", {})).items():
            ego_avoidance_trigger_counts_by_interaction_kind[str(label)] += int(count)
        ttc_value = _coerce_optional_float(row.get("min_ttc_any_lane_sec"))
        if ttc_value is not None and (min_ttc_any_lane_sec_min is None or ttc_value < min_ttc_any_lane_sec_min):
            min_ttc_any_lane_sec_min = ttc_value
            min_ttc_any_lane_row_id = row.get("row_id")
            min_ttc_any_lane_source_batch = row.get("source_batch")
        path_ttc_value = _coerce_optional_float(row.get("min_ttc_path_conflict_sec"))
        if path_ttc_value is not None and (
            min_ttc_path_conflict_sec_min is None or path_ttc_value < min_ttc_path_conflict_sec_min
        ):
            min_ttc_path_conflict_sec_min = path_ttc_value
            min_ttc_path_conflict_row_id = row.get("row_id")
            min_ttc_path_conflict_source_batch = row.get("source_batch")

    return {
        "variant_selected_count": int(workflow_report.get("selected_variant_count", 0)),
        "matrix_case_count": int(matrix_report.get("case_count", 0)),
        "combined_row_count": int(len(variant_rows) + len(matrix_rows)),
        "combined_execution_status_counts": dict(sorted(combined_execution_status_counts.items())),
        "combined_object_sim_status_counts": dict(sorted(combined_object_sim_status_counts.items())),
        "collision_row_count": int(collision_row_count),
        "timeout_row_count": int(timeout_row_count),
        "path_conflict_row_count": int(path_conflict_row_count),
        "merge_conflict_row_count": int(merge_conflict_row_count),
        "lane_change_conflict_row_count": int(lane_change_conflict_row_count),
        "ego_avoidance_row_count": int(ego_avoidance_row_count),
        "ego_avoidance_brake_event_count_total": int(ego_avoidance_brake_event_count_total),
        "ego_avoidance_trigger_counts_by_interaction_kind": dict(
            sorted(ego_avoidance_trigger_counts_by_interaction_kind.items())
        ),
        "min_ttc_any_lane_sec_min": min_ttc_any_lane_sec_min,
        "min_ttc_any_lane_row_id": min_ttc_any_lane_row_id,
        "min_ttc_any_lane_source_batch": min_ttc_any_lane_source_batch,
        "min_ttc_path_conflict_sec_min": min_ttc_path_conflict_sec_min,
        "min_ttc_path_conflict_row_id": min_ttc_path_conflict_row_id,
        "min_ttc_path_conflict_source_batch": min_ttc_path_conflict_source_batch,
    }


def _build_gate_summary(
    *,
    overview: dict[str, Any],
    comparison_tables: dict[str, Any],
    gate_policy: dict[str, Any],
) -> dict[str, Any]:
    policy = {
        "profile_path": gate_policy.get("profile_path"),
        "profile_id": gate_policy.get("profile_id"),
        "max_attention_rows": gate_policy.get("max_attention_rows"),
        "max_collision_rows": gate_policy.get("max_collision_rows"),
        "max_timeout_rows": gate_policy.get("max_timeout_rows"),
        "max_path_conflict_rows": gate_policy.get("max_path_conflict_rows"),
        "max_merge_conflict_rows": gate_policy.get("max_merge_conflict_rows"),
        "max_lane_change_conflict_rows": gate_policy.get("max_lane_change_conflict_rows"),
        "max_avoidance_rows": gate_policy.get("max_avoidance_rows"),
        "max_avoidance_brake_events": gate_policy.get("max_avoidance_brake_events"),
        "max_avoidance_same_lane_conflict_triggers": gate_policy.get(
            "max_avoidance_same_lane_conflict_triggers"
        ),
        "max_avoidance_merge_conflict_triggers": gate_policy.get(
            "max_avoidance_merge_conflict_triggers"
        ),
        "max_avoidance_lane_change_conflict_triggers": gate_policy.get(
            "max_avoidance_lane_change_conflict_triggers"
        ),
        "max_avoidance_downstream_route_conflict_triggers": gate_policy.get(
            "max_avoidance_downstream_route_conflict_triggers"
        ),
        "min_min_ttc_any_lane_sec": gate_policy.get("min_min_ttc_any_lane_sec"),
        "min_min_ttc_path_conflict_sec": gate_policy.get("min_min_ttc_path_conflict_sec"),
    }
    evaluated_rules: list[dict[str, Any]] = []
    failure_codes: list[str] = []
    enabled_rule_count = 0

    def evaluate(*, metric_id: str, metric_value: Any, threshold_value: Any, comparison: str, failure_code: str) -> None:
        nonlocal enabled_rule_count
        if threshold_value is None:
            return
        enabled_rule_count += 1
        passed = False
        if comparison == "max_le":
            metric_float = int(metric_value)
            threshold_float = int(threshold_value)
            passed = metric_float <= threshold_float
        elif comparison == "min_ge":
            metric_float = _coerce_optional_float(metric_value)
            threshold_float = float(threshold_value)
            passed = metric_float is not None and metric_float >= threshold_float
        else:
            raise ValueError(f"unsupported gate comparison: {comparison}")
        if not passed:
            failure_codes.append(failure_code)
        evaluated_rules.append(
            {
                "metric_id": metric_id,
                "metric_value": metric_value,
                "threshold_value": threshold_value,
                "comparison": comparison,
                "passed": bool(passed),
                "failure_code": None if passed else failure_code,
            }
        )

    evaluate(
        metric_id="attention_row_count",
        metric_value=comparison_tables["attention_row_count"],
        threshold_value=policy["max_attention_rows"],
        comparison="max_le",
        failure_code="ATTENTION_ROWS_EXCEEDED",
    )
    evaluate(
        metric_id="collision_row_count",
        metric_value=overview["collision_row_count"],
        threshold_value=policy["max_collision_rows"],
        comparison="max_le",
        failure_code="COLLISION_ROWS_EXCEEDED",
    )
    evaluate(
        metric_id="timeout_row_count",
        metric_value=overview["timeout_row_count"],
        threshold_value=policy["max_timeout_rows"],
        comparison="max_le",
        failure_code="TIMEOUT_ROWS_EXCEEDED",
    )
    evaluate(
        metric_id="path_conflict_row_count",
        metric_value=overview["path_conflict_row_count"],
        threshold_value=policy["max_path_conflict_rows"],
        comparison="max_le",
        failure_code="PATH_CONFLICT_ROWS_EXCEEDED",
    )
    evaluate(
        metric_id="merge_conflict_row_count",
        metric_value=overview["merge_conflict_row_count"],
        threshold_value=policy["max_merge_conflict_rows"],
        comparison="max_le",
        failure_code="MERGE_CONFLICT_ROWS_EXCEEDED",
    )
    evaluate(
        metric_id="lane_change_conflict_row_count",
        metric_value=overview["lane_change_conflict_row_count"],
        threshold_value=policy["max_lane_change_conflict_rows"],
        comparison="max_le",
        failure_code="LANE_CHANGE_CONFLICT_ROWS_EXCEEDED",
    )
    evaluate(
        metric_id="ego_avoidance_row_count",
        metric_value=overview["ego_avoidance_row_count"],
        threshold_value=policy["max_avoidance_rows"],
        comparison="max_le",
        failure_code="AVOIDANCE_ROWS_EXCEEDED",
    )
    evaluate(
        metric_id="ego_avoidance_brake_event_count_total",
        metric_value=overview["ego_avoidance_brake_event_count_total"],
        threshold_value=policy["max_avoidance_brake_events"],
        comparison="max_le",
        failure_code="AVOIDANCE_BRAKE_EVENTS_EXCEEDED",
    )
    evaluate(
        metric_id="ego_avoidance_same_lane_conflict_trigger_count",
        metric_value=overview["ego_avoidance_trigger_counts_by_interaction_kind"].get("same_lane_conflict", 0),
        threshold_value=policy["max_avoidance_same_lane_conflict_triggers"],
        comparison="max_le",
        failure_code="AVOIDANCE_SAME_LANE_TRIGGER_COUNT_EXCEEDED",
    )
    evaluate(
        metric_id="ego_avoidance_merge_conflict_trigger_count",
        metric_value=overview["ego_avoidance_trigger_counts_by_interaction_kind"].get("merge_conflict", 0),
        threshold_value=policy["max_avoidance_merge_conflict_triggers"],
        comparison="max_le",
        failure_code="AVOIDANCE_MERGE_CONFLICT_TRIGGER_COUNT_EXCEEDED",
    )
    evaluate(
        metric_id="ego_avoidance_lane_change_conflict_trigger_count",
        metric_value=overview["ego_avoidance_trigger_counts_by_interaction_kind"].get("lane_change_conflict", 0),
        threshold_value=policy["max_avoidance_lane_change_conflict_triggers"],
        comparison="max_le",
        failure_code="AVOIDANCE_LANE_CHANGE_TRIGGER_COUNT_EXCEEDED",
    )
    evaluate(
        metric_id="ego_avoidance_downstream_route_conflict_trigger_count",
        metric_value=overview["ego_avoidance_trigger_counts_by_interaction_kind"].get(
            "downstream_route_conflict", 0
        ),
        threshold_value=policy["max_avoidance_downstream_route_conflict_triggers"],
        comparison="max_le",
        failure_code="AVOIDANCE_DOWNSTREAM_ROUTE_TRIGGER_COUNT_EXCEEDED",
    )
    evaluate(
        metric_id="min_ttc_any_lane_sec_min",
        metric_value=overview["min_ttc_any_lane_sec_min"],
        threshold_value=policy["min_min_ttc_any_lane_sec"],
        comparison="min_ge",
        failure_code="MIN_TTC_BELOW_THRESHOLD",
    )
    evaluate(
        metric_id="min_ttc_path_conflict_sec_min",
        metric_value=overview["min_ttc_path_conflict_sec_min"],
        threshold_value=policy["min_min_ttc_path_conflict_sec"],
        comparison="min_ge",
        failure_code="MIN_TTC_PATH_CONFLICT_BELOW_THRESHOLD",
    )

    if enabled_rule_count == 0:
        status = "DISABLED"
        passed = True
    elif failure_codes:
        status = "FAIL"
        passed = False
    else:
        status = "PASS"
        passed = True
    return {
        "status": status,
        "passed": bool(passed),
        "enabled_rule_count": int(enabled_rule_count),
        "failure_codes": failure_codes,
        "policy": policy,
        "evaluated_rules": evaluated_rules,
    }


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


def _build_markdown_report(report: dict[str, Any]) -> str:
    overview = report["overview"]
    logical_rows = report["comparison_tables"]["logical_scenario_rows"]
    matrix_rows = report["comparison_tables"]["matrix_group_rows"]
    attention_rows = report["comparison_tables"]["attention_rows"]
    attention_reason_counts = report["comparison_tables"].get("attention_reason_counts", {})
    gate = report["gate"]

    sections: list[str] = []
    sections.append("# Scenario Batch Comparison")
    sections.append("")
    sections.append("## Overview")
    sections.append("")
    sections.append(
        _markdown_table(
            ["Metric", "Value"],
            [
                ["Variant selected count", str(overview["variant_selected_count"])],
                ["Matrix case count", str(overview["matrix_case_count"])],
                ["Combined row count", str(overview["combined_row_count"])],
                ["Combined execution status counts", _format_counter(overview["combined_execution_status_counts"])],
                ["Combined object sim status counts", _format_counter(overview["combined_object_sim_status_counts"])],
                ["Collision row count", str(overview["collision_row_count"])],
                ["Timeout row count", str(overview["timeout_row_count"])],
                ["Path conflict row count", str(overview["path_conflict_row_count"])],
                ["Merge conflict row count", str(overview["merge_conflict_row_count"])],
                ["Lane-change conflict row count", str(overview["lane_change_conflict_row_count"])],
                ["Avoidance-active row count", str(overview["ego_avoidance_row_count"])],
                ["Avoidance brake event count", str(overview["ego_avoidance_brake_event_count_total"])],
                ["Avoidance trigger counts", _format_counter(overview["ego_avoidance_trigger_counts_by_interaction_kind"])],
                ["Minimum TTC any-lane", _format_float(overview["min_ttc_any_lane_sec_min"])],
                ["Minimum TTC source", str(overview.get("min_ttc_any_lane_source_batch") or "-")],
                ["Minimum TTC row id", str(overview.get("min_ttc_any_lane_row_id") or "-")],
                ["Minimum TTC path-conflict", _format_float(overview["min_ttc_path_conflict_sec_min"])],
                ["Minimum TTC path-conflict source", str(overview.get("min_ttc_path_conflict_source_batch") or "-")],
                ["Minimum TTC path-conflict row id", str(overview.get("min_ttc_path_conflict_row_id") or "-")],
            ],
        )
    )
    sections.append("")
    sections.append("## Gate")
    sections.append("")
    sections.append(
        _markdown_table(
            ["Metric", "Value"],
            [
                ["Gate status", str(gate["status"])],
                ["Gate passed", str(gate["passed"])],
                ["Enabled rule count", str(gate["enabled_rule_count"])],
                ["Gate profile path", str(gate["policy"].get("profile_path") or "-")],
                ["Gate profile id", str(gate["policy"].get("profile_id") or "-")],
                ["Failure codes", ",".join(gate["failure_codes"]) or "-"],
            ],
        )
    )
    if gate["evaluated_rules"]:
        sections.append("")
        sections.append(
            _markdown_table(
                ["Rule", "Metric", "Threshold", "Passed", "Failure Code"],
                [
                    [
                        str(rule["metric_id"]),
                        str(rule["metric_value"]),
                        str(rule["threshold_value"]),
                        str(rule["passed"]),
                        str(rule["failure_code"] or "-"),
                    ]
                    for rule in gate["evaluated_rules"]
                ],
            )
        )
    sections.append("")
    sections.append("## Logical Scenario Summary")
    sections.append("")
    sections.append(
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
                    str(row["path_conflict_row_count"]),
                    str(row["merge_conflict_row_count"]),
                    str(row["lane_change_conflict_row_count"]),
                    str(row.get("ego_avoidance_brake_event_count_total", 0)),
                    _format_float(row["min_ttc_any_lane_sec_min"]),
                    _format_float(row["min_ttc_path_conflict_sec_min"]),
                ]
                for row in logical_rows
            ],
        )
    )
    sections.append("")
    sections.append("## Matrix Group Summary")
    sections.append("")
    sections.append(
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
                "Speed Scale",
                "Tire Friction",
                "Surface Friction",
            ],
            [
                [
                    str(row["matrix_group_id"]),
                    str(row["case_count"]),
                    _format_counter(row["execution_status_counts"]),
                    _format_counter(row["object_sim_status_counts"]),
                    str(row["collision_count"]),
                    str(row["timeout_count"]),
                    str(row["path_conflict_row_count"]),
                    str(row["merge_conflict_row_count"]),
                    str(row["lane_change_conflict_row_count"]),
                    str(row.get("ego_avoidance_brake_event_count_total", 0)),
                    _format_float(row["min_ttc_any_lane_sec_min"]),
                    _format_float(row["min_ttc_path_conflict_sec_min"]),
                    ",".join(str(value) for value in row["traffic_npc_speed_scale_values"]) or "-",
                    ",".join(str(value) for value in row["tire_friction_coeff_values"]) or "-",
                    ",".join(str(value) for value in row["surface_friction_scale_values"]) or "-",
                ]
                for row in matrix_rows
            ],
        )
    )
    sections.append("")
    sections.append("## Attention Rows")
    sections.append("")
    sections.append(f"- Attention reason counts: `{_format_counter(attention_reason_counts)}`")
    sections.append("")
    if attention_rows:
        sections.append(
            _markdown_table(
                [
                    "Source",
                    "Row ID",
                    "Group",
                    "Execution",
                    "Object Sim",
                    "Ego Route Lane",
                    "NPC Route Lanes",
                    "Collision",
                    "Timeout",
                    "Min TTC Any",
                    "Path",
                    "Merge",
                    "Lane Change",
                    "Avoidance",
                    "Avoidance Kind",
                    "Reasons",
                    "Failure Code",
                ],
                [
                    [
                        str(row["source_batch"]),
                        str(row["row_id"]),
                        str(row["group_id"]),
                        str(row["execution_status"] or "-"),
                        str(row["object_sim_status"] or "-"),
                        str(row.get("ego_route_lane_id") or "-"),
                        ",".join(row.get("traffic_npc_route_lane_id_profile", [])) or "-",
                        str(row["collision"]),
                        str(row["timeout"]),
                        _format_float(row["min_ttc_any_lane_sec"]),
                        str(row.get("path_conflict_rows", 0)),
                        str(row.get("merge_conflict_rows", 0)),
                        str(row.get("lane_change_conflict_rows", 0)),
                        str(row.get("ego_avoidance_brake_event_count", 0)),
                        str(row.get("ego_avoidance_last_trigger_interaction_kind") or "-"),
                        ",".join(row.get("attention_reasons", [])) or "-",
                        str(row["failure_code"] or "-"),
                    ]
                    for row in attention_rows
                ],
            )
        )
    else:
        sections.append("No attention rows.")
    sections.append("")
    sections.append("## Artifacts")
    sections.append("")
    sections.append(
        _markdown_table(
            ["Artifact", "Path"],
            [
                ["Variant workflow report", str(report["inputs"]["variant_workflow_report_path"])],
                ["Variant run report", str(report["inputs"]["variant_run_report_path"])],
                ["Matrix sweep report", str(report["inputs"]["matrix_sweep_report_path"])],
                ["JSON comparison report", str(report["artifacts"]["json_report_path"])],
                ["Markdown comparison report", str(report["artifacts"]["markdown_report_path"])],
            ],
        )
    )
    sections.append("")
    return "\n".join(sections)


def build_scenario_batch_comparison_report(
    *,
    variant_workflow_report_path: Path,
    matrix_sweep_report_path: Path,
    out_report: Path,
    markdown_out: Path | None = None,
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
) -> dict[str, Any]:
    workflow_report = _load_variant_workflow_report(variant_workflow_report_path)
    variant_run_report_path_value = workflow_report["artifacts"].get("variant_run_report_path")
    variant_run_report_path = Path(str(variant_run_report_path_value)).resolve()
    if not variant_run_report_path.is_file():
        raise FileNotFoundError(f"variant run report not found: {variant_run_report_path}")
    variant_run_report = _load_variant_run_report(variant_run_report_path)
    matrix_report = _load_matrix_sweep_report(matrix_sweep_report_path)

    variant_rows = _build_variant_batch_rows(variant_run_report)
    matrix_rows = _build_matrix_batch_rows(matrix_report)
    logical_scenario_rows = _build_logical_scenario_rows(variant_rows)
    matrix_group_rows = _build_matrix_group_rows(matrix_rows)
    attention_rows = _build_attention_rows(list(variant_rows) + list(matrix_rows))
    gate_policy = _resolve_gate_policy(
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

    out_report = out_report.resolve()
    markdown_out = markdown_out.resolve() if markdown_out is not None else out_report.with_suffix(".md")
    out_report.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)

    report = {
        "scenario_batch_comparison_report_schema_version": SCENARIO_BATCH_COMPARISON_REPORT_SCHEMA_VERSION_V0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "variant_workflow_report_path": str(variant_workflow_report_path.resolve()),
            "variant_run_report_path": str(variant_run_report_path),
            "matrix_sweep_report_path": str(matrix_sweep_report_path.resolve()),
            "gate_profile_path": str(gate_profile_path.resolve()) if gate_profile_path is not None else None,
        },
        "artifacts": {
            "json_report_path": str(out_report),
            "markdown_report_path": str(markdown_out),
        },
        "overview": _build_overview(
            workflow_report=workflow_report,
            matrix_report=matrix_report,
            variant_rows=variant_rows,
            matrix_rows=matrix_rows,
        ),
        "variant_summary": {
            "variant_count": int(workflow_report.get("variant_count", 0)),
            "selected_variant_count": int(workflow_report.get("selected_variant_count", 0)),
            "execution_status_counts": dict(workflow_report.get("execution_status_counts", {})),
            "object_sim_status_counts": dict(workflow_report.get("object_sim_status_counts", {})),
            "by_payload_kind": dict(workflow_report.get("by_payload_kind", {})),
            "by_logical_scenario_id": dict(workflow_report.get("by_logical_scenario_id", {})),
        },
        "matrix_summary": {
            "case_count": int(matrix_report.get("case_count", 0)),
            "success_case_count": int(matrix_report.get("success_case_count", 0)),
            "failed_case_count": int(matrix_report.get("failed_case_count", 0)),
            "status_counts": dict(matrix_report.get("status_counts", {})),
            "collision_case_count": int(matrix_report.get("collision_case_count", 0)),
            "timeout_case_count": int(matrix_report.get("timeout_case_count", 0)),
            "min_ttc_any_lane_sec_min": _coerce_optional_float(matrix_report.get("min_ttc_any_lane_sec_min")),
            "lowest_ttc_any_lane_run_id": str(matrix_report.get("lowest_ttc_any_lane_run_id", "")).strip() or None,
        },
        "comparison_tables": {
            "logical_scenario_rows": logical_scenario_rows,
            "matrix_group_rows": matrix_group_rows,
            "attention_rows": attention_rows,
            "attention_reason_counts": _build_attention_reason_counts(attention_rows),
        },
    }
    report["comparison_tables"]["logical_scenario_row_count"] = len(logical_scenario_rows)
    report["comparison_tables"]["matrix_group_row_count"] = len(matrix_group_rows)
    report["comparison_tables"]["attention_row_count"] = len(attention_rows)
    report["gate"] = _build_gate_summary(
        overview=report["overview"],
        comparison_tables=report["comparison_tables"],
        gate_policy=gate_policy,
    )

    markdown_text = _build_markdown_report(report)
    out_report.write_text(json.dumps(report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    markdown_out.write_text(markdown_text, encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        out_report = Path(args.out_report).resolve()
        markdown_out = Path(args.markdown_out).resolve() if str(args.markdown_out).strip() else None
        report = build_scenario_batch_comparison_report(
            variant_workflow_report_path=Path(args.variant_workflow_report).resolve(),
            matrix_sweep_report_path=Path(args.matrix_sweep_report).resolve(),
            out_report=out_report,
            markdown_out=markdown_out,
            gate_profile_path=resolve_scenario_batch_gate_profile_path(
                gate_profile=args.gate_profile,
                gate_profile_id=args.gate_profile_id,
                gate_profile_dir=args.gate_profile_dir,
            ),
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
            gate_max_avoidance_rows=_parse_optional_non_negative_int(
                args.gate_max_avoidance_rows,
                field="gate-max-avoidance-rows",
            ),
            gate_max_avoidance_brake_events=_parse_optional_non_negative_int(
                args.gate_max_avoidance_brake_events,
                field="gate-max-avoidance-brake-events",
            ),
            gate_max_avoidance_same_lane_conflict_triggers=_parse_optional_non_negative_int(
                args.gate_max_avoidance_same_lane_conflict_triggers,
                field="gate-max-avoidance-same-lane-conflict-triggers",
            ),
            gate_max_avoidance_merge_conflict_triggers=_parse_optional_non_negative_int(
                args.gate_max_avoidance_merge_conflict_triggers,
                field="gate-max-avoidance-merge-conflict-triggers",
            ),
            gate_max_avoidance_lane_change_conflict_triggers=_parse_optional_non_negative_int(
                args.gate_max_avoidance_lane_change_conflict_triggers,
                field="gate-max-avoidance-lane-change-conflict-triggers",
            ),
            gate_max_avoidance_downstream_route_conflict_triggers=_parse_optional_non_negative_int(
                args.gate_max_avoidance_downstream_route_conflict_triggers,
                field="gate-max-avoidance-downstream-route-conflict-triggers",
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
        print(f"[ok] logical_scenario_row_count={report['comparison_tables']['logical_scenario_row_count']}")
        print(f"[ok] matrix_group_row_count={report['comparison_tables']['matrix_group_row_count']}")
        print(f"[ok] attention_row_count={report['comparison_tables']['attention_row_count']}")
        print(f"[ok] gate_status={report['gate']['status']}")
        print(f"[ok] report_out={report['artifacts']['json_report_path']}")
        if report["gate"]["status"] == "FAIL":
            return 2
        if report["gate"]["status"] == "DISABLED" and report["comparison_tables"]["attention_row_count"] > 0:
            return 2
        return 0
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] scenario_batch_comparison.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
