from __future__ import annotations

import argparse
import sys
from pathlib import Path

from hybrid_sensor_sim.scenarios.matrix_sweep import (
    _parse_csv_positive_floats,
    _parse_csv_text_items,
    _parse_non_negative_int,
    run_scenario_matrix_sweep,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run object-sim traffic parameter matrix sweep.")
    parser.add_argument("--scenario", required=True, help="Scenario JSON path")
    parser.add_argument("--out-root", required=True, help="Output root for per-case run directories")
    parser.add_argument("--report-out", required=True, help="Output JSON report path")
    parser.add_argument("--run-id-prefix", default="RUN_CORE_SIM_SWEEP", help="Run ID prefix for matrix cases")
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
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        report = run_scenario_matrix_sweep(
            scenario_path=Path(args.scenario).resolve(),
            out_root=Path(args.out_root).resolve(),
            report_out=Path(args.report_out).resolve(),
            run_id_prefix=str(args.run_id_prefix).strip() or "RUN_CORE_SIM_SWEEP",
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
        )
        print(f"[ok] case_count={report['case_count']}")
        print(f"[ok] success_case_count={report['success_case_count']}")
        print(f"[ok] failed_case_count={report['failed_case_count']}")
        print(f"[ok] report_out={Path(args.report_out).resolve()}")
        if report["success_case_count"] <= 0:
            print("[error] scenario_matrix_sweep.py: no successful matrix case", file=sys.stderr)
            return 2
        return 0
    except (FileNotFoundError, ValueError) as exc:
        print(f"[error] scenario_matrix_sweep.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
