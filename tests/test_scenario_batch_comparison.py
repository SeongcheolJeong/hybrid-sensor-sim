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
from hybrid_sensor_sim.tools.scenario_batch_comparison import (
    SCENARIO_BATCH_COMPARISON_REPORT_SCHEMA_VERSION_V0,
    build_scenario_batch_comparison_report,
)
from hybrid_sensor_sim.tools.scenario_batch_comparison import main as scenario_batch_comparison_main
from hybrid_sensor_sim.tools.scenario_variant_workflow import run_scenario_variant_workflow


P_VALIDATION_FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_validation"
P_SIM_ENGINE_FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_sim_engine"


class ScenarioBatchComparisonTests(unittest.TestCase):
    def test_build_scenario_batch_comparison_report_writes_json_and_markdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workflow_result = run_scenario_variant_workflow(
                logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                out_root=root / "workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=0,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )
            matrix_report = run_scenario_matrix_sweep(
                scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_map_route_following_v0.json",
                out_root=root / "matrix_runs",
                report_out=root / "matrix_report.json",
                run_id_prefix="RUN_MATRIX_COMPARE",
                traffic_profile_ids=["sumo_highway_balanced_v0"],
                traffic_actor_pattern_ids=["sumo_platoon_sparse_v0", "sumo_route_shifted_v0"],
                traffic_npc_speed_scale_values=[1.0],
                tire_friction_coeff_values=[1.0],
                surface_friction_scale_values=[1.0],
                enable_ego_collision_avoidance=False,
                avoidance_ttc_threshold_sec=2.5,
                ego_max_brake_mps2=6.0,
                max_cases=0,
            )
            report = build_scenario_batch_comparison_report(
                variant_workflow_report_path=Path(workflow_result["workflow_report_path"]),
                matrix_sweep_report_path=root / "matrix_report.json",
                out_report=root / "comparison.json",
                markdown_out=root / "comparison.md",
            )

            self.assertEqual(
                report["scenario_batch_comparison_report_schema_version"],
                SCENARIO_BATCH_COMPARISON_REPORT_SCHEMA_VERSION_V0,
            )
            self.assertEqual(report["overview"]["variant_selected_count"], 2)
            self.assertEqual(report["overview"]["matrix_case_count"], matrix_report["case_count"])
            self.assertEqual(report["overview"]["combined_row_count"], 4)
            self.assertEqual(report["comparison_tables"]["logical_scenario_row_count"], 2)
            self.assertEqual(report["comparison_tables"]["matrix_group_row_count"], 2)
            self.assertIn(
                "scn_log_route_relations",
                {row["logical_scenario_id"] for row in report["comparison_tables"]["logical_scenario_rows"]},
            )
            self.assertIn(
                "sumo_highway_balanced_v0::sumo_route_shifted_v0",
                {row["matrix_group_id"] for row in report["comparison_tables"]["matrix_group_rows"]},
            )
            self.assertTrue((root / "comparison.json").is_file())
            self.assertTrue((root / "comparison.md").is_file())
            markdown = (root / "comparison.md").read_text(encoding="utf-8")
            self.assertIn("## Logical Scenario Summary", markdown)
            self.assertIn("scn_direct_object_sim", markdown)
            self.assertIn("sumo_highway_balanced_v0::sumo_platoon_sparse_v0", markdown)

    def test_scenario_batch_comparison_cli_writes_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workflow_result = run_scenario_variant_workflow(
                logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                out_root=root / "workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )
            run_scenario_matrix_sweep(
                scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                out_root=root / "matrix_runs",
                report_out=root / "matrix_report.json",
                run_id_prefix="RUN_MATRIX_COMPARE",
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
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = scenario_batch_comparison_main(
                    [
                        "--variant-workflow-report",
                        str(workflow_result["workflow_report_path"]),
                        "--matrix-sweep-report",
                        str(root / "matrix_report.json"),
                        "--out-report",
                        str(root / "comparison.json"),
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads((root / "comparison.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["comparison_tables"]["logical_scenario_row_count"], 1)
            self.assertTrue((root / "comparison.md").is_file())

    def test_scenario_batch_comparison_script_bootstraps_src_path(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_scenario_batch_comparison.py"
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0)
        self.assertIn("compare scenario variant workflow", proc.stdout.lower())


if __name__ == "__main__":
    unittest.main()
