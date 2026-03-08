from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.physics.vehicle_dynamics import (
    simulate_vehicle_dynamics,
    simulate_vehicle_dynamics_step,
    validate_control_sequence,
    validate_vehicle_profile,
)
from hybrid_sensor_sim.tools.vehicle_dynamics_trace import (
    main as vehicle_dynamics_trace_main,
)


class VehicleDynamicsTests(unittest.TestCase):
    def test_validate_vehicle_profile_rejects_invalid_schema_version(self) -> None:
        payload = self._profile_payload()
        payload["profile_schema_version"] = "wrong"
        with self.assertRaisesRegex(ValueError, "profile_schema_version must be"):
            validate_vehicle_profile(payload)

    def _profile_payload(self) -> dict[str, object]:
        return {
            "profile_schema_version": "vehicle_profile_v0",
            "wheelbase_m": 2.9,
            "max_accel_mps2": 4.0,
            "max_decel_mps2": 7.0,
            "max_speed_mps": 40.0,
            "mass_kg": 1700.0,
            "rolling_resistance_coeff": 0.015,
            "drag_coefficient": 0.28,
            "frontal_area_m2": 2.3,
            "front_axle_to_cg_m": 1.4,
            "rear_axle_to_cg_m": 1.5,
            "tire_friction_coeff": 1.0,
        }

    def _sequence_payload(self) -> dict[str, object]:
        return {
            "sequence_schema_version": "control_sequence_v0",
            "dt_sec": 0.1,
            "initial_speed_mps": 5.0,
            "initial_position_m": 0.0,
            "initial_heading_deg": 0.0,
            "default_target_speed_mps": 12.0,
            "commands": [
                {"throttle": 0.6, "brake": 0.0, "steering_angle_deg": 0.0},
                {"throttle": 0.8, "brake": 0.0, "steering_angle_deg": 8.0},
                {"throttle": 1.0, "brake": 0.0, "steering_angle_deg": 10.0, "surface_friction_scale": 0.3},
                {"throttle": 0.0, "brake": 0.3, "steering_angle_deg": 0.0},
            ],
        }

    def test_validate_vehicle_profile_rejects_inconsistent_wheelbase(self) -> None:
        payload = self._profile_payload()
        payload["rear_axle_to_cg_m"] = 2.0
        with self.assertRaisesRegex(ValueError, "must equal wheelbase_m"):
            validate_vehicle_profile(payload)

    def test_simulate_vehicle_dynamics_supports_dynamic_bicycle(self) -> None:
        vehicle_profile = validate_vehicle_profile(self._profile_payload())
        sequence_payload = self._sequence_payload()
        sequence_payload["enable_planar_kinematics"] = True
        sequence_payload["enable_dynamic_bicycle"] = True
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

        self.assertEqual(result["vehicle_dynamics_model"], "planar_dynamic_bicycle_force_balance_v1")
        self.assertTrue(result["dynamic_bicycle_enabled"])
        self.assertEqual(result["step_count"], 4)
        self.assertNotEqual(result["final_y_m"], 0.0)
        self.assertTrue(any(bool(step["longitudinal_force_limited"]) for step in result["trace"]))

    def test_vehicle_dynamics_trace_main_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            profile_path = root / "vehicle_profile.json"
            sequence_path = root / "control_sequence.json"
            out_path = root / "trace.json"
            profile_path.write_text(json.dumps(self._profile_payload()), encoding="utf-8")
            sequence_payload = self._sequence_payload()
            sequence_payload["enable_planar_kinematics"] = True
            sequence_payload["enable_dynamic_bicycle"] = False
            sequence_path.write_text(json.dumps(sequence_payload), encoding="utf-8")

            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = vehicle_dynamics_trace_main(
                    [
                        "--vehicle-profile",
                        str(profile_path),
                        "--control-sequence",
                        str(sequence_path),
                        "--out",
                        str(out_path),
                    ]
                )

            self.assertEqual(exit_code, 0)
            result = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual(result["vehicle_dynamics_trace_schema_version"], "vehicle_dynamics_trace_v0")
            self.assertEqual(result["vehicle_dynamics_model"], "planar_bicycle_force_balance_v1")
            self.assertEqual(result["vehicle_profile_path"], str(profile_path.resolve()))
            self.assertEqual(result["control_sequence_path"], str(sequence_path.resolve()))
            self.assertGreater(result["final_speed_mps"], 0.0)

    def test_simulate_vehicle_dynamics_step_matches_first_trace_row(self) -> None:
        vehicle_profile = validate_vehicle_profile(self._profile_payload())
        sequence_payload = self._sequence_payload()
        sequence_payload["enable_planar_kinematics"] = True
        sequence_payload["enable_dynamic_bicycle"] = False
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
        first_step = simulate_vehicle_dynamics_step(
            vehicle_profile=vehicle_profile,
            dt_sec=dt_sec,
            position_m=initial_position_m,
            speed_mps=initial_speed_mps,
            heading_deg=initial_heading_deg,
            lateral_position_m=initial_lateral_position_m,
            lateral_velocity_mps=initial_lateral_velocity_mps,
            yaw_rate_rps=initial_yaw_rate_rps,
            enable_planar_kinematics=enable_planar_kinematics,
            enable_dynamic_bicycle=enable_dynamic_bicycle,
            throttle=float(commands[0]["throttle"]),
            brake=float(commands[0]["brake"]),
            steering_angle_deg=float(commands[0]["steering_angle_deg"]),
            road_grade_percent=float(commands[0]["road_grade_percent"]),
            surface_friction_scale=float(commands[0]["surface_friction_scale"]),
            target_speed_mps=commands[0]["target_speed_mps"],
        )

        trace_row = result["trace"][0]
        self.assertAlmostEqual(float(first_step["speed_mps"]), float(trace_row["speed_mps"]), places=6)
        self.assertAlmostEqual(float(first_step["position_m"]), float(trace_row["position_m"]), places=6)
        self.assertAlmostEqual(float(first_step["accel_mps2"]), float(trace_row["accel_mps2"]), places=6)
        self.assertAlmostEqual(float(first_step["net_force_n"]), float(trace_row["net_force_n"]), places=6)

    def test_vehicle_dynamics_trace_script_bootstraps_src_path(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_vehicle_dynamics_trace.py"
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(proc.returncode, 0)
        self.assertIn("vehicle dynamics trace", proc.stdout.lower())


if __name__ == "__main__":
    unittest.main()
