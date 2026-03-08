from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.scenarios.matrix_sweep import run_scenario_matrix_sweep
from hybrid_sensor_sim.tools.scenario_matrix_sweep import main as scenario_matrix_sweep_main


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_sim_engine"


class ScenarioMatrixSweepTests(unittest.TestCase):
    def test_run_scenario_matrix_sweep_writes_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = run_scenario_matrix_sweep(
                scenario_path=FIXTURE_ROOT / "highway_safe_following_v0.json",
                out_root=root / "runs",
                report_out=root / "report.json",
                run_id_prefix="RUN_MATRIX",
                traffic_profile_ids=["sumo_highway_balanced_v0"],
                traffic_actor_pattern_ids=["sumo_platoon_sparse_v0"],
                traffic_npc_speed_scale_values=[1.0],
                tire_friction_coeff_values=[1.0],
                surface_friction_scale_values=[1.0],
                enable_ego_collision_avoidance=False,
                avoidance_ttc_threshold_sec=2.5,
                ego_max_brake_mps2=6.0,
                max_cases=0,
            )
            self.assertEqual(report["core_sim_matrix_sweep_schema_version"], "core_sim_matrix_sweep_report_v0")
            self.assertEqual(report["case_count"], 1)
            self.assertEqual(report["success_case_count"], 1)
            self.assertTrue(report["cases"][0]["lane_risk_summary_exists"])
            self.assertTrue((root / "report.json").exists())
            self.assertTrue((root / "runs" / "RUN_MATRIX_0001" / "summary.json").exists())
            self.assertTrue((root / "runs" / "RUN_MATRIX_0001" / "lane_risk_summary.json").exists())

    def test_run_scenario_matrix_sweep_preserves_map_route_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = run_scenario_matrix_sweep(
                scenario_path=FIXTURE_ROOT / "highway_map_route_following_v0.json",
                out_root=root / "runs",
                report_out=root / "report.json",
                run_id_prefix="RUN_MATRIX_MAP",
                traffic_profile_ids=["sumo_highway_balanced_v0"],
                traffic_actor_pattern_ids=["sumo_platoon_sparse_v0"],
                traffic_npc_speed_scale_values=[1.0],
                tire_friction_coeff_values=[1.0],
                surface_friction_scale_values=[1.0],
                enable_ego_collision_avoidance=False,
                avoidance_ttc_threshold_sec=2.5,
                ego_max_brake_mps2=6.0,
                max_cases=1,
            )
            case_scenario = json.loads(
                (root / "runs" / "RUN_MATRIX_MAP_0001" / "matrix_scenario.json").read_text(encoding="utf-8")
            )
            summary = json.loads(
                (root / "runs" / "RUN_MATRIX_MAP_0001" / "summary.json").read_text(encoding="utf-8")
            )

            self.assertEqual(report["case_count"], 1)
            self.assertIn("canonical_map_path", case_scenario)
            self.assertEqual(case_scenario["route_definition"]["entry_lane_id"], "lane_a")
            self.assertTrue(all("lane_id" in npc for npc in case_scenario["npcs"]))
            self.assertTrue(summary["scenario_map_enabled"])
            self.assertTrue(summary["scenario_route_enabled"])
            self.assertEqual(summary["scenario_route_lane_ids"], ["lane_a", "lane_b", "lane_c"])
            self.assertTrue(summary["traffic_npc_lane_id_profile"])

    def test_run_scenario_matrix_sweep_uses_route_relation_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_scenario_matrix_sweep(
                scenario_path=FIXTURE_ROOT / "highway_map_route_following_v0.json",
                out_root=root / "runs",
                report_out=root / "report.json",
                run_id_prefix="RUN_MATRIX_ROUTE_SHIFT",
                traffic_profile_ids=["sumo_highway_balanced_v0"],
                traffic_actor_pattern_ids=["sumo_route_shifted_v0"],
                traffic_npc_speed_scale_values=[1.0],
                tire_friction_coeff_values=[1.0],
                surface_friction_scale_values=[1.0],
                enable_ego_collision_avoidance=False,
                avoidance_ttc_threshold_sec=2.5,
                ego_max_brake_mps2=6.0,
                max_cases=1,
            )
            case_scenario = json.loads(
                (root / "runs" / "RUN_MATRIX_ROUTE_SHIFT_0001" / "matrix_scenario.json").read_text(encoding="utf-8")
            )
            summary = json.loads(
                (root / "runs" / "RUN_MATRIX_ROUTE_SHIFT_0001" / "summary.json").read_text(encoding="utf-8")
            )

            self.assertEqual([npc["lane_index"] for npc in case_scenario["npcs"]], [1, 1])
            self.assertEqual([npc["lane_id"] for npc in case_scenario["npcs"]], ["lane_b", "lane_b"])
            self.assertEqual(summary["traffic_npc_lane_id_profile"], ["lane_b", "lane_b"])

    def test_run_scenario_matrix_sweep_can_generate_lane_change_conflict_pattern(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_scenario_matrix_sweep(
                scenario_path=FIXTURE_ROOT / "highway_map_route_following_v0.json",
                out_root=root / "runs",
                report_out=root / "report.json",
                run_id_prefix="RUN_MATRIX_LANE_CHANGE",
                traffic_profile_ids=["sumo_highway_balanced_v0"],
                traffic_actor_pattern_ids=["sumo_lane_change_conflict_v0"],
                traffic_npc_speed_scale_values=[1.0],
                tire_friction_coeff_values=[1.0],
                surface_friction_scale_values=[1.0],
                enable_ego_collision_avoidance=False,
                avoidance_ttc_threshold_sec=2.5,
                ego_max_brake_mps2=6.0,
                max_cases=1,
            )
            case_scenario = json.loads(
                (root / "runs" / "RUN_MATRIX_LANE_CHANGE_0001" / "matrix_scenario.json").read_text(encoding="utf-8")
            )
            summary = json.loads(
                (root / "runs" / "RUN_MATRIX_LANE_CHANGE_0001" / "summary.json").read_text(encoding="utf-8")
            )
            lane_risk = json.loads(
                (root / "runs" / "RUN_MATRIX_LANE_CHANGE_0001" / "lane_risk_summary.json").read_text(
                    encoding="utf-8"
                )
            )

            self.assertEqual(case_scenario["npcs"][0]["lane_id"], "lane_b")
            self.assertEqual(case_scenario["npcs"][0]["route_lane_id"], "lane_a")
            self.assertEqual(summary["traffic_npc_route_lane_id_profile"], ["lane_a"])
            self.assertEqual(lane_risk["lane_change_conflict_rows"], 61)

    def test_scenario_matrix_sweep_cli_writes_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report_path = root / "report.json"
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = scenario_matrix_sweep_main(
                    [
                        "--scenario",
                        str(FIXTURE_ROOT / "highway_safe_following_v0.json"),
                        "--out-root",
                        str(root / "runs"),
                        "--report-out",
                        str(report_path),
                        "--traffic-profile-ids",
                        "sumo_highway_balanced_v0",
                        "--traffic-actor-pattern-ids",
                        "sumo_platoon_sparse_v0",
                        "--traffic-npc-speed-scale-values",
                        "1.0",
                        "--tire-friction-coeff-values",
                        "1.0",
                        "--surface-friction-scale-values",
                        "1.0",
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["case_count"], 1)

    def test_scenario_matrix_sweep_script_bootstraps_src_path(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_scenario_matrix_sweep.py"
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0)
        self.assertIn("traffic parameter matrix sweep", proc.stdout.lower())


if __name__ == "__main__":
    unittest.main()
