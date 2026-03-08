from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.scenarios import load_scenario, run_object_sim
from hybrid_sensor_sim.scenarios.schema import ScenarioValidationError
from hybrid_sensor_sim.tools.object_sim_runner import main as object_sim_main


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_sim_engine"


class ObjectSimTests(unittest.TestCase):
    def test_load_scenario_rejects_invalid_schema(self) -> None:
        with self.assertRaisesRegex(ScenarioValidationError, "unsupported scenario_schema_version"):
            load_scenario(
                {
                    "scenario_schema_version": "wrong",
                    "scenario_id": "bad",
                    "duration_sec": 1.0,
                    "dt_sec": 0.1,
                    "ego": {"position_m": 0.0, "speed_mps": 1.0},
                    "npcs": [{"position_m": 5.0, "speed_mps": 1.0}],
                }
            )

    def test_load_scenario_rejects_empty_npcs(self) -> None:
        with self.assertRaisesRegex(ScenarioValidationError, "npcs must be a non-empty list"):
            load_scenario(
                {
                    "scenario_schema_version": "scenario_definition_v0",
                    "scenario_id": "bad",
                    "duration_sec": 1.0,
                    "dt_sec": 0.1,
                    "ego": {"position_m": 0.0, "speed_mps": 1.0},
                    "npcs": [],
                }
            )

    def test_run_object_sim_success_case_is_deterministic(self) -> None:
        scenario = load_scenario(FIXTURE_ROOT / "highway_safe_following_v0.json")
        result = run_object_sim(scenario, seed=42, metadata={"run_id": "SAFE_001"})

        self.assertEqual(result.summary["status"], "success")
        self.assertEqual(result.summary["termination_reason"], "completed")
        self.assertFalse(result.summary["collision"])
        self.assertFalse(result.summary["timeout"])
        self.assertIsNone(result.summary["min_ttc_same_lane_sec"])
        self.assertEqual(result.lane_risk_summary["same_lane_rows"], 200)
        self.assertEqual(result.lane_risk_summary["ttc_under_3s_same_lane_count"], 0)

    def test_run_object_sim_collision_case_is_deterministic(self) -> None:
        scenario = load_scenario(FIXTURE_ROOT / "highway_following_v0.json")
        result = run_object_sim(scenario, seed=42, metadata={"run_id": "FOLLOW_001"})

        self.assertEqual(result.summary["status"], "failed")
        self.assertEqual(result.summary["termination_reason"], "collision")
        self.assertTrue(result.summary["collision"])
        self.assertIsNotNone(result.summary["min_ttc_same_lane_sec"])
        self.assertGreater(result.lane_risk_summary["same_lane_rows"], 0)
        self.assertGreaterEqual(result.lane_risk_summary["ttc_under_3s_same_lane_count"], 1)

    def test_run_object_sim_respects_wall_timeout_override(self) -> None:
        scenario = load_scenario(FIXTURE_ROOT / "highway_safe_following_v0.json")
        result = run_object_sim(
            scenario,
            seed=42,
            wall_timeout_override=1e-9,
            metadata={"run_id": "TIMEOUT_001"},
        )

        self.assertEqual(result.summary["status"], "timeout")
        self.assertTrue(result.summary["timeout"])
        self.assertEqual(result.summary["step_count"], 0)

    def test_object_sim_runner_main_writes_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_root = root / "runs"
            scenario_path = FIXTURE_ROOT / "highway_safe_following_v0.json"
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = object_sim_main(
                    [
                        "--scenario",
                        str(scenario_path),
                        "--run-id",
                        "RUN_SAFE_001",
                        "--seed",
                        "42",
                        "--out",
                        str(out_root),
                    ]
                )

            self.assertEqual(exit_code, 0)
            summary = json.loads((out_root / "RUN_SAFE_001" / "summary.json").read_text(encoding="utf-8"))
            lane_risk = json.loads(
                (out_root / "RUN_SAFE_001" / "lane_risk_summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(summary["run_id"], "RUN_SAFE_001")
            self.assertEqual(summary["status"], "success")
            self.assertEqual(lane_risk["lane_risk_summary_schema_version"], "lane_risk_summary_v0")
            self.assertTrue((out_root / "RUN_SAFE_001" / "trace.csv").exists())

    def test_object_sim_script_bootstraps_src_path(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_object_sim.py"
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(proc.returncode, 0)
        self.assertIn("object simulation", proc.stdout.lower())


if __name__ == "__main__":
    unittest.main()
