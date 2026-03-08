from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.physics.vehicle_dynamics import (
    simulate_vehicle_dynamics,
    validate_control_sequence,
    validate_vehicle_profile,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run vehicle dynamics trace from profile and control sequence.")
    parser.add_argument("--vehicle-profile", required=True, help="Vehicle profile JSON path")
    parser.add_argument("--control-sequence", required=True, help="Control sequence JSON path")
    parser.add_argument("--out", required=True, help="Output simulation JSON path")
    return parser.parse_args(argv)


def _load_json_object(path: Path, label: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def run_vehicle_dynamics_trace(
    *,
    vehicle_profile_path: Path,
    control_sequence_path: Path,
    out_path: Path,
) -> dict[str, Any]:
    profile_payload = _load_json_object(vehicle_profile_path, "vehicle profile")
    sequence_payload = _load_json_object(control_sequence_path, "control sequence")
    vehicle_profile = validate_vehicle_profile(profile_payload)
    (
        dt_sec,
        initial_position_m,
        initial_speed_mps,
        initial_heading_deg,
        initial_lateral_position_m,
        initial_lateral_velocity_mps,
        initial_yaw_rate_rps,
        enable_planar_kinematics,
        enable_dynamic_bicycle,
        commands,
    ) = validate_control_sequence(sequence_payload)
    result = simulate_vehicle_dynamics(
        vehicle_profile=vehicle_profile,
        dt_sec=dt_sec,
        initial_position_m=initial_position_m,
        initial_speed_mps=initial_speed_mps,
        initial_heading_deg=initial_heading_deg,
        initial_lateral_position_m=initial_lateral_position_m,
        initial_lateral_velocity_mps=initial_lateral_velocity_mps,
        initial_yaw_rate_rps=initial_yaw_rate_rps,
        enable_planar_kinematics=enable_planar_kinematics,
        enable_dynamic_bicycle=enable_dynamic_bicycle,
        commands=commands,
    )
    result["vehicle_profile_path"] = str(vehicle_profile_path.resolve())
    result["control_sequence_path"] = str(control_sequence_path.resolve())
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return result


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        result = run_vehicle_dynamics_trace(
            vehicle_profile_path=Path(args.vehicle_profile).resolve(),
            control_sequence_path=Path(args.control_sequence).resolve(),
            out_path=Path(args.out).resolve(),
        )
        print(f"[ok] step_count={result['step_count']}")
        print(f"[ok] final_speed_mps={result['final_speed_mps']}")
        print(f"[ok] out={Path(args.out).resolve()}")
        return 0
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] vehicle_dynamics_trace.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
