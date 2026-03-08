from __future__ import annotations

import itertools
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.scenarios.object_sim import ObjectSimRunResult
from hybrid_sensor_sim.scenarios.schema import ActorState, ScenarioConfig, load_scenario
from hybrid_sensor_sim.tools.object_sim_runner import run_object_sim_job


CORE_SIM_MATRIX_SWEEP_SCHEMA_VERSION_V0 = "core_sim_matrix_sweep_report_v0"
TRAFFIC_ACTOR_PATTERN_LIBRARY_V0: dict[str, dict[str, Any]] = {
    "sumo_platoon_sparse_v0": {
        "traffic_npc_count": 2,
        "traffic_npc_initial_gap_m": 45.0,
        "traffic_npc_gap_step_m": 28.0,
        "traffic_npc_speed_offset_mps": -2.0,
        "traffic_npc_lane_profile": [0, 1],
        "gap_step_multipliers": [0.0, 1.8],
        "speed_slot_offsets_mps": [0.0, -0.5],
    },
    "sumo_platoon_balanced_v0": {
        "traffic_npc_count": 3,
        "traffic_npc_initial_gap_m": 34.0,
        "traffic_npc_gap_step_m": 22.0,
        "traffic_npc_speed_offset_mps": 0.0,
        "traffic_npc_lane_profile": [0, 1, -1],
        "gap_step_multipliers": [0.0, 1.1, 2.4],
        "speed_slot_offsets_mps": [0.4, 0.0, -0.3],
    },
    "sumo_dense_aggressive_v0": {
        "traffic_npc_count": 4,
        "traffic_npc_initial_gap_m": 24.0,
        "traffic_npc_gap_step_m": 16.0,
        "traffic_npc_speed_offset_mps": 3.0,
        "traffic_npc_lane_profile": [0, 1, 0, -1],
        "gap_step_multipliers": [0.0, 0.9, 2.1, 3.4],
        "speed_slot_offsets_mps": [1.0, 0.7, 0.3, -0.1],
    },
}


def _parse_csv_text_items(raw: str, *, field: str) -> list[str]:
    items = [item.strip() for item in str(raw).split(",") if item.strip()]
    if not items:
        raise ValueError(f"{field} must contain at least one non-empty item")
    ordered: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _parse_csv_positive_floats(raw: str, *, field: str) -> list[float]:
    values: list[float] = []
    seen: set[float] = set()
    for token_raw in str(raw).split(","):
        token = token_raw.strip()
        if not token:
            continue
        try:
            value = float(token)
        except ValueError as exc:
            raise ValueError(f"{field} must contain only numbers, got: {token_raw}") from exc
        if value <= 0.0:
            raise ValueError(f"{field} values must be > 0, got: {value}")
        rounded = round(value, 9)
        if rounded in seen:
            continue
        seen.add(rounded)
        values.append(float(rounded))
    if not values:
        raise ValueError(f"{field} must contain at least one positive value")
    return values


def _parse_non_negative_int(raw: str, *, field: str) -> int:
    value = str(raw).strip()
    if not value:
        return 0
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be an integer, got: {raw}") from exc
    if parsed < 0:
        raise ValueError(f"{field} must be >= 0, got: {parsed}")
    return parsed


def _resolve_profile_slot(profile: list[float], index: int, *, fallback: float, extension_step: float) -> float:
    if index < len(profile):
        return float(profile[index])
    if not profile:
        return float(fallback)
    overflow_index = index - len(profile) + 1
    return float(profile[-1]) + (extension_step * float(overflow_index))


def _resolve_cyclic_int_slot(profile: list[int], index: int, *, fallback: int) -> int:
    if not profile:
        return int(fallback)
    return int(profile[index % len(profile)])


def _coerce_float_list(raw: Any) -> list[float]:
    if not isinstance(raw, list):
        return []
    out: list[float] = []
    for item in raw:
        try:
            out.append(float(item))
        except (TypeError, ValueError):
            continue
    return out


def _coerce_int_list(raw: Any) -> list[int]:
    if not isinstance(raw, list):
        return []
    out: list[int] = []
    for item in raw:
        try:
            out.append(int(item))
        except (TypeError, ValueError):
            continue
    return out


def _infer_first_npc_gap_m(scenario: ScenarioConfig) -> float:
    if scenario.npcs:
        first_npc = min(scenario.npcs, key=lambda row: row.position_m)
        return max(1.0, float(first_npc.position_m - scenario.ego.position_m))
    return 30.0


def apply_traffic_actor_pattern(
    scenario: ScenarioConfig,
    *,
    traffic_actor_pattern_id: str,
    traffic_npc_speed_scale: float,
    tire_friction_coeff: float,
    surface_friction_scale: float,
    enable_ego_collision_avoidance: bool,
    avoidance_ttc_threshold_sec: float,
    ego_max_brake_mps2: float,
) -> ScenarioConfig:
    actor_pattern_payload = TRAFFIC_ACTOR_PATTERN_LIBRARY_V0.get(traffic_actor_pattern_id, {})
    if not actor_pattern_payload:
        allowed = ", ".join(sorted(TRAFFIC_ACTOR_PATTERN_LIBRARY_V0))
        raise ValueError(
            f"traffic_actor_pattern_id must be one of: {allowed}; got: {traffic_actor_pattern_id}"
        )
    effective_npc_count = int(actor_pattern_payload.get("traffic_npc_count", max(1, len(scenario.npcs))))
    base_gap_m = float(actor_pattern_payload.get("traffic_npc_initial_gap_m", _infer_first_npc_gap_m(scenario)))
    gap_step_m = float(actor_pattern_payload.get("traffic_npc_gap_step_m", max(8.0, base_gap_m * 0.6)))
    speed_offset_mps = float(actor_pattern_payload.get("traffic_npc_speed_offset_mps", 0.0))
    base_speed_mps = max(0.0, float(scenario.ego.speed_mps) + speed_offset_mps)
    gap_step_multipliers = _coerce_float_list(actor_pattern_payload.get("gap_step_multipliers"))
    speed_slot_offsets_mps = _coerce_float_list(actor_pattern_payload.get("speed_slot_offsets_mps"))
    lane_slot_profile = _coerce_int_list(actor_pattern_payload.get("traffic_npc_lane_profile"))

    synthesized_npcs: list[ActorState] = []
    for idx in range(effective_npc_count):
        gap_multiplier = _resolve_profile_slot(
            gap_step_multipliers,
            idx,
            fallback=float(idx),
            extension_step=1.0,
        )
        speed_slot_offset_mps = _resolve_profile_slot(
            speed_slot_offsets_mps,
            idx,
            fallback=-0.2 * float(idx),
            extension_step=-0.2,
        )
        lane_index = _resolve_cyclic_int_slot(lane_slot_profile, idx, fallback=0)
        synthesized_npcs.append(
            ActorState(
                actor_id=f"traffic_{idx + 1:03d}",
                position_m=float(scenario.ego.position_m) + base_gap_m + (gap_step_m * gap_multiplier),
                speed_mps=max(0.0, (base_speed_mps + speed_slot_offset_mps) * traffic_npc_speed_scale),
                length_m=4.8,
                lane_index=lane_index,
                lane_id=(
                    scenario.map_context.route_report["route_lane_ids"][lane_index]
                    if (
                        scenario.map_context is not None
                        and scenario.map_context.route_report is not None
                        and 0 <= lane_index < len(scenario.map_context.route_report.get("route_lane_ids", []))
                    )
                    else None
                ),
            )
        )

    return ScenarioConfig(
        scenario_schema_version=scenario.scenario_schema_version,
        scenario_id=scenario.scenario_id,
        duration_sec=scenario.duration_sec,
        dt_sec=scenario.dt_sec,
        ego=scenario.ego,
        npcs=synthesized_npcs,
        npc_speed_jitter_mps=scenario.npc_speed_jitter_mps,
        enable_ego_collision_avoidance=enable_ego_collision_avoidance,
        avoidance_ttc_threshold_sec=avoidance_ttc_threshold_sec,
        ego_max_brake_mps2=ego_max_brake_mps2,
        tire_friction_coeff=tire_friction_coeff,
        surface_friction_scale=surface_friction_scale,
        wall_timeout_sec=scenario.wall_timeout_sec,
        ego_dynamics_mode=scenario.ego_dynamics_mode,
        ego_vehicle_profile=scenario.ego_vehicle_profile,
        ego_target_speed_mps=scenario.ego_target_speed_mps,
        ego_road_grade_percent=scenario.ego_road_grade_percent,
        map_context=scenario.map_context,
    )


def _serialize_actor(actor: ActorState) -> dict[str, Any]:
    payload = {
        "actor_id": actor.actor_id,
        "position_m": actor.position_m,
        "speed_mps": actor.speed_mps,
        "length_m": actor.length_m,
        "lane_index": actor.lane_index,
    }
    if actor.lane_id is not None:
        payload["lane_id"] = actor.lane_id
    return payload


def _serialize_scenario_config(scenario: ScenarioConfig) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "scenario_schema_version": scenario.scenario_schema_version,
        "scenario_id": scenario.scenario_id,
        "duration_sec": scenario.duration_sec,
        "dt_sec": scenario.dt_sec,
        "npc_speed_jitter_mps": scenario.npc_speed_jitter_mps,
        "enable_ego_collision_avoidance": scenario.enable_ego_collision_avoidance,
        "avoidance_ttc_threshold_sec": scenario.avoidance_ttc_threshold_sec,
        "ego_max_brake_mps2": scenario.ego_max_brake_mps2,
        "tire_friction_coeff": scenario.tire_friction_coeff,
        "surface_friction_scale": scenario.surface_friction_scale,
        "ego_dynamics_mode": scenario.ego_dynamics_mode,
        "ego_target_speed_mps": scenario.ego_target_speed_mps,
        "ego_road_grade_percent": scenario.ego_road_grade_percent,
        "ego": _serialize_actor(scenario.ego),
        "npcs": [_serialize_actor(npc) for npc in scenario.npcs],
    }
    if scenario.wall_timeout_sec is not None:
        payload["wall_timeout_sec"] = scenario.wall_timeout_sec
    if scenario.ego_vehicle_profile is not None:
        payload["ego_vehicle_profile"] = dict(scenario.ego_vehicle_profile)
    if scenario.map_context is not None:
        if scenario.map_context.map_path is not None:
            payload["canonical_map_path"] = scenario.map_context.map_path
        else:
            payload["canonical_map"] = scenario.map_context.map_payload
        if scenario.map_context.route_report is not None:
            payload["route_definition"] = {
                "entry_lane_id": scenario.map_context.route_report["selected_entry_lane_id"],
                "exit_lane_id": scenario.map_context.route_report["selected_exit_lane_id"],
                "via_lane_ids": list(scenario.map_context.route_report["via_lane_ids_input"]),
                "cost_mode": scenario.map_context.route_report["route_cost_mode"],
            }
    return payload


def run_scenario_matrix_sweep(
    *,
    scenario_path: Path,
    out_root: Path,
    report_out: Path,
    run_id_prefix: str,
    traffic_profile_ids: list[str],
    traffic_actor_pattern_ids: list[str],
    traffic_npc_speed_scale_values: list[float],
    tire_friction_coeff_values: list[float],
    surface_friction_scale_values: list[float],
    enable_ego_collision_avoidance: bool,
    avoidance_ttc_threshold_sec: float,
    ego_max_brake_mps2: float,
    max_cases: int,
) -> dict[str, Any]:
    scenario = load_scenario(scenario_path)
    out_root.mkdir(parents=True, exist_ok=True)
    report_out.parent.mkdir(parents=True, exist_ok=True)

    case_grid = list(
        itertools.product(
            traffic_profile_ids,
            traffic_actor_pattern_ids,
            traffic_npc_speed_scale_values,
            tire_friction_coeff_values,
            surface_friction_scale_values,
        )
    )
    if max_cases > 0:
        case_grid = case_grid[:max_cases]
    if not case_grid:
        raise ValueError("matrix case grid resolved to empty set")

    status_counts: dict[str, int] = {}
    case_rows: list[dict[str, Any]] = []
    success_case_count = 0
    collision_case_count = 0
    timeout_case_count = 0
    min_ttc_same_lane_sec_min: float | None = None
    min_ttc_any_lane_sec_min: float | None = None
    lowest_ttc_same_lane_run_id = ""
    lowest_ttc_any_lane_run_id = ""

    for index, case in enumerate(case_grid, start=1):
        (
            traffic_profile_id,
            traffic_actor_pattern_id,
            traffic_npc_speed_scale,
            tire_friction_coeff,
            surface_friction_scale,
        ) = case
        run_id = f"{run_id_prefix}_{index:04d}"
        case_scenario = apply_traffic_actor_pattern(
            scenario,
            traffic_actor_pattern_id=traffic_actor_pattern_id,
            traffic_npc_speed_scale=float(traffic_npc_speed_scale),
            tire_friction_coeff=float(tire_friction_coeff),
            surface_friction_scale=float(surface_friction_scale),
            enable_ego_collision_avoidance=bool(enable_ego_collision_avoidance),
            avoidance_ttc_threshold_sec=float(avoidance_ttc_threshold_sec),
            ego_max_brake_mps2=float(ego_max_brake_mps2),
        )
        case_scenario_path = out_root / run_id / "matrix_scenario.json"
        case_scenario_path.parent.mkdir(parents=True, exist_ok=True)
        case_scenario_path.write_text(
            json.dumps(_serialize_scenario_config(case_scenario), indent=2, ensure_ascii=True)
            + "\n",
            encoding="utf-8",
        )
        try:
            job = run_object_sim_job(
                scenario_path=case_scenario_path,
                run_id=run_id,
                out_root=out_root,
                seed=42,
                metadata={
                    "run_source": "scenario_matrix_sweep",
                    "traffic_profile_id": traffic_profile_id,
                    "traffic_actor_pattern_id": traffic_actor_pattern_id,
                    "tire_friction_coeff": float(tire_friction_coeff),
                    "surface_friction_scale": float(surface_friction_scale),
                },
            )
            summary_payload = dict(job["summary"])
            status = str(summary_payload.get("status", "")).strip().lower()
            success_case_count += 1
            returncode = 0
        except Exception as exc:
            summary_payload = {}
            status = ""
            returncode = 2
            stderr_tail = str(exc)
        else:
            stderr_tail = ""
            collision = bool(summary_payload.get("collision", False))
            timeout = bool(summary_payload.get("timeout", False))
            if collision:
                collision_case_count += 1
            if timeout:
                timeout_case_count += 1
            min_ttc_same_lane_sec = summary_payload.get("min_ttc_same_lane_sec")
            min_ttc_any_lane_sec = summary_payload.get("min_ttc_any_lane_sec")
            if min_ttc_same_lane_sec is not None:
                min_ttc_same_lane_sec = float(min_ttc_same_lane_sec)
                if min_ttc_same_lane_sec_min is None or min_ttc_same_lane_sec < min_ttc_same_lane_sec_min:
                    min_ttc_same_lane_sec_min = min_ttc_same_lane_sec
                    lowest_ttc_same_lane_run_id = run_id
            if min_ttc_any_lane_sec is not None:
                min_ttc_any_lane_sec = float(min_ttc_any_lane_sec)
                if min_ttc_any_lane_sec_min is None or min_ttc_any_lane_sec < min_ttc_any_lane_sec_min:
                    min_ttc_any_lane_sec_min = min_ttc_any_lane_sec
                    lowest_ttc_any_lane_run_id = run_id
        status_counts[status] = status_counts.get(status, 0) + 1
        summary_path = (out_root / run_id / "summary.json").resolve()
        case_rows.append(
            {
                "run_id": run_id,
                "traffic_profile_id": traffic_profile_id,
                "traffic_actor_pattern_id": traffic_actor_pattern_id,
                "traffic_npc_speed_scale": float(traffic_npc_speed_scale),
                "tire_friction_coeff": float(tire_friction_coeff),
                "surface_friction_scale": float(surface_friction_scale),
                "returncode": int(returncode),
                "summary_path": str(summary_path),
                "summary_exists": bool(summary_path.exists()),
                "status": status,
                "collision": bool(summary_payload.get("collision", False)),
                "timeout": bool(summary_payload.get("timeout", False)),
                "min_ttc_same_lane_sec": summary_payload.get("min_ttc_same_lane_sec"),
                "min_ttc_any_lane_sec": summary_payload.get("min_ttc_any_lane_sec"),
                "stderr_tail": stderr_tail,
            }
        )

    case_count = len(case_rows)
    failed_case_count = case_count - success_case_count
    report_payload = {
        "core_sim_matrix_sweep_schema_version": CORE_SIM_MATRIX_SWEEP_SCHEMA_VERSION_V0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scenario_path": str(scenario_path.resolve()),
        "out_root": str(out_root.resolve()),
        "run_id_prefix": run_id_prefix,
        "enable_ego_collision_avoidance": bool(enable_ego_collision_avoidance),
        "avoidance_ttc_threshold_sec": float(avoidance_ttc_threshold_sec),
        "ego_max_brake_mps2": float(ego_max_brake_mps2),
        "max_cases": int(max_cases),
        "input_grid": {
            "traffic_profile_ids": traffic_profile_ids,
            "traffic_actor_pattern_ids": traffic_actor_pattern_ids,
            "traffic_npc_speed_scale_values": [float(value) for value in traffic_npc_speed_scale_values],
            "tire_friction_coeff_values": [float(value) for value in tire_friction_coeff_values],
            "surface_friction_scale_values": [float(value) for value in surface_friction_scale_values],
        },
        "case_count": int(case_count),
        "success_case_count": int(success_case_count),
        "failed_case_count": int(failed_case_count),
        "all_cases_success": bool(failed_case_count == 0),
        "status_counts": {key: int(status_counts[key]) for key in sorted(status_counts.keys())},
        "collision_case_count": int(collision_case_count),
        "timeout_case_count": int(timeout_case_count),
        "min_ttc_same_lane_sec_min": min_ttc_same_lane_sec_min,
        "lowest_ttc_same_lane_run_id": lowest_ttc_same_lane_run_id,
        "min_ttc_any_lane_sec_min": min_ttc_any_lane_sec_min,
        "lowest_ttc_any_lane_run_id": lowest_ttc_any_lane_run_id,
        "cases": case_rows,
    }
    report_out.write_text(json.dumps(report_payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return report_payload
