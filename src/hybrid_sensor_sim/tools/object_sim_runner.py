from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Mapping

from hybrid_sensor_sim.scenarios import load_scenario, run_object_sim
from hybrid_sensor_sim.scenarios.schema import ScenarioValidationError


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic object simulation from scenario_definition_v0.")
    parser.add_argument("--scenario", required=True, help="Path to scenario JSON file")
    parser.add_argument("--run-id", required=True, help="Run identifier")
    parser.add_argument("--seed", default="", help="Seed for deterministic behavior")
    parser.add_argument("--out", required=True, help="Output root directory for run artifacts")
    parser.add_argument("--wall-timeout-sec", default="", help="Optional wall-clock timeout override")
    parser.add_argument("--run-source", default="sim_closed_loop", help="Run source type")
    parser.add_argument("--sds-version", default="sds_unknown", help="SDS version identifier")
    parser.add_argument("--sim-version", default="sim_engine_v0_prototype", help="Simulation version identifier")
    parser.add_argument("--fidelity-profile", default="dev-fast", help="Fidelity profile for this run")
    parser.add_argument("--map-id", default="map_unknown", help="Map identifier")
    parser.add_argument("--map-version", default="v0", help="Map version identifier")
    parser.add_argument("--odd-tags", default="", help="Comma-separated ODD tags")
    parser.add_argument("--batch-id", default="", help="Optional batch identifier")
    return parser.parse_args(argv)


def _parse_int(raw: Any, *, default: int, field: str) -> int:
    value = str(raw).strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be an integer, got: {raw}") from exc


def _parse_optional_float(raw: Any, *, field: str) -> float | None:
    value = str(raw).strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be a number, got: {raw}") from exc


def write_object_sim_artifacts(
    *,
    out_root: Path,
    run_id: str,
    summary: dict[str, Any],
    trace_rows: list[dict[str, Any]],
    lane_risk_summary: dict[str, Any],
) -> tuple[Path, Path, Path]:
    run_dir = out_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    trace_path = run_dir / "trace.csv"
    with trace_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "time_sec",
                "ego_position_m",
                "ego_speed_mps",
                "ego_lane_index",
                "ego_lane_id",
                "ego_lane_binding_mode",
                "ego_route_lane_id",
                "ego_route_binding_mode",
                "npc_id",
                "npc_position_m",
                "npc_lane_index",
                "npc_lane_id",
                "npc_lane_binding_mode",
                "npc_route_lane_id",
                "npc_route_binding_mode",
                "lane_delta",
                "same_lane",
                "adjacent_lane",
                "path_conflict",
                "path_conflict_source",
                "path_interaction_kind",
                "ego_route_lane_order",
                "npc_route_lane_order",
                "route_lane_delta",
                "route_relation",
                "gap_m",
                "relative_speed_mps",
                "ttc_sec",
                "ttc_same_lane_sec",
                "ttc_adjacent_lane_sec",
                "path_ttc_sec",
                "route_ttc_sec",
                "ego_avoidance_brake_applied",
                "ego_avoidance_ttc_sec",
                "ego_avoidance_applied_brake_mps2",
                "ego_avoidance_effective_brake_limit_mps2",
                "ego_avoidance_target_actor_id",
                "ego_avoidance_target_interaction_kind",
                "ego_avoidance_target_route_relation",
                "ego_avoidance_target_path_conflict_source",
                "ego_avoidance_target_gap_m",
                "ego_avoidance_target_ttc_sec",
                "ego_avoidance_target_ttc_threshold_sec",
                "ego_avoidance_target_brake_scale",
                "ego_avoidance_target_min_brake_scale",
                "ego_avoidance_target_hold_duration_sec",
                "ego_avoidance_target_priority",
                "ego_avoidance_target_max_gap_m",
                "ego_avoidance_hold_active",
                "ego_avoidance_hold_source",
                "ego_avoidance_hold_remaining_sec",
                "ego_surface_friction_scale",
                "ego_dynamics_mode",
                "ego_dynamics_throttle",
                "ego_dynamics_brake",
                "ego_dynamics_accel_mps2",
                "ego_dynamics_net_force_n",
                "ego_dynamics_speed_tracking_error_mps",
                "ego_dynamics_longitudinal_force_limited",
                "collision",
            ],
        )
        writer.writeheader()
        writer.writerows(trace_rows)

    lane_risk_summary_path = run_dir / "lane_risk_summary.json"
    lane_risk_summary_path.write_text(
        json.dumps(lane_risk_summary, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )

    summary_payload = dict(summary)
    summary_payload["run_id"] = run_id
    summary_payload["trace_path"] = str(trace_path)
    summary_payload["lane_risk_summary_path"] = str(lane_risk_summary_path)
    summary_payload["lane_risk_summary"] = lane_risk_summary

    summary_path = run_dir / "summary.json"
    summary_path.write_text(
        json.dumps(summary_payload, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    return summary_path, trace_path, lane_risk_summary_path


def run_object_sim_job(
    *,
    scenario_path: Path,
    run_id: str,
    out_root: Path,
    seed: int,
    wall_timeout_override: float | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    scenario = load_scenario(scenario_path)
    result = run_object_sim(
        scenario,
        seed=seed,
        wall_timeout_override=wall_timeout_override,
        metadata={
            "run_id": run_id,
            "scenario_path": str(scenario_path.resolve()),
            **dict(metadata or {}),
        },
    )
    summary_path, trace_path, lane_risk_summary_path = write_object_sim_artifacts(
        out_root=out_root,
        run_id=run_id,
        summary=result.summary,
        trace_rows=result.trace_rows,
        lane_risk_summary=result.lane_risk_summary,
    )
    return {
        "summary_path": summary_path,
        "trace_path": trace_path,
        "lane_risk_summary_path": lane_risk_summary_path,
        "summary": result.summary,
        "lane_risk_summary": result.lane_risk_summary,
    }


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        scenario_path = Path(args.scenario).resolve()
        out_root = Path(args.out).resolve()
        out_root.mkdir(parents=True, exist_ok=True)
        seed = _parse_int(args.seed, default=42, field="seed")
        wall_timeout_override = _parse_optional_float(args.wall_timeout_sec, field="wall-timeout-sec")
        odd_tags = [tag.strip() for tag in args.odd_tags.split(",") if tag.strip()]
        job = run_object_sim_job(
            scenario_path=scenario_path,
            run_id=args.run_id,
            out_root=out_root,
            seed=seed,
            wall_timeout_override=wall_timeout_override,
            metadata={
                "run_source": args.run_source,
                "sds_version": args.sds_version,
                "sim_version": args.sim_version,
                "fidelity_profile": args.fidelity_profile,
                "map_id": args.map_id,
                "map_version": args.map_version,
                "odd_tags": odd_tags,
                "batch_id": args.batch_id if args.batch_id else None,
            },
        )
        summary = job["summary"]
        print(f"[ok] run_id={args.run_id}")
        print(f"[ok] summary={job['summary_path']}")
        print(f"[ok] trace={job['trace_path']}")
        print(f"[ok] lane_risk_summary={job['lane_risk_summary_path']}")
        print(f"[ok] status={summary['status']} termination={summary['termination_reason']}")
        return 0
    except (FileNotFoundError, json.JSONDecodeError, ScenarioValidationError, ValueError) as exc:
        print(f"[error] object_sim_runner.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
