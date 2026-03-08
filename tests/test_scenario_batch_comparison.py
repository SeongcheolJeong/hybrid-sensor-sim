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
P_MAP_TOOLSET_FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_map_toolset"


class ScenarioBatchComparisonTests(unittest.TestCase):
    def _write_attention_logical_scenarios(self, path: Path) -> None:
        collision_scenario = json.loads(
            (P_SIM_ENGINE_FIXTURE_ROOT / "highway_following_v0.json").read_text(encoding="utf-8")
        )
        path.write_text(
            json.dumps(
                {
                    "logical_scenarios": [
                        {
                            "scenario_id": "scn_collision_attention",
                            "parameters": {"scenario_variant": [1]},
                            "variant_payload_kind": "scenario_definition_v0",
                            "variant_payload_template": collision_scenario,
                        }
                    ]
                },
                indent=2,
                ensure_ascii=True,
            )
            + "\n",
            encoding="utf-8",
        )

    def _write_route_attention_logical_scenarios(self, path: Path) -> None:
        route_scenario = json.loads(
            (P_SIM_ENGINE_FIXTURE_ROOT / "highway_map_route_following_v0.json").read_text(encoding="utf-8")
        )
        route_scenario["canonical_map_path"] = str(
            (P_MAP_TOOLSET_FIXTURE_ROOT / "canonical_lane_graph_v0.json").resolve()
        )
        path.write_text(
            json.dumps(
                {
                    "logical_scenarios": [
                        {
                            "scenario_id": "scn_route_attention",
                            "parameters": {"scenario_variant": [1]},
                            "variant_payload_kind": "scenario_definition_v0",
                            "variant_payload_template": route_scenario,
                        }
                    ]
                },
                indent=2,
                ensure_ascii=True,
            )
            + "\n",
            encoding="utf-8",
        )

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
            self.assertGreater(report["overview"]["path_conflict_row_count"], 0)
            self.assertGreater(report["overview"]["merge_conflict_row_count"], 0)
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
            logical_row = next(
                row
                for row in report["comparison_tables"]["logical_scenario_rows"]
                if row["logical_scenario_id"] == "scn_log_route_relations"
            )
            self.assertGreater(logical_row["path_conflict_row_count"], 0)
            self.assertGreater(logical_row["merge_conflict_row_count"], 0)
            self.assertIsNotNone(logical_row["min_ttc_path_conflict_sec_min"])
            self.assertGreater(logical_row["path_interaction_counts"]["merge_conflict"], 0)
            matrix_row = next(
                row
                for row in report["comparison_tables"]["matrix_group_rows"]
                if row["matrix_group_id"] == "sumo_highway_balanced_v0::sumo_route_shifted_v0"
            )
            self.assertGreater(matrix_row["merge_conflict_row_count"], 0)
            self.assertIsNotNone(matrix_row["min_ttc_path_conflict_sec_min"])
            self.assertTrue((root / "comparison.json").is_file())
            self.assertTrue((root / "comparison.md").is_file())
            markdown = (root / "comparison.md").read_text(encoding="utf-8")
            self.assertIn("## Logical Scenario Summary", markdown)
            self.assertIn("scn_direct_object_sim", markdown)
            self.assertIn("sumo_highway_balanced_v0::sumo_platoon_sparse_v0", markdown)
            self.assertIn("Path conflict row count", markdown)
            self.assertIn("Min TTC Path", markdown)

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
            self.assertEqual(payload["gate"]["status"], "DISABLED")

    def test_scenario_batch_comparison_gate_can_fail_on_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "attention_logical_scenarios.json"
            self._write_attention_logical_scenarios(logical_path)
            workflow_result = run_scenario_variant_workflow(
                logical_scenarios_path=str(logical_path),
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
            report = build_scenario_batch_comparison_report(
                variant_workflow_report_path=Path(workflow_result["workflow_report_path"]),
                matrix_sweep_report_path=root / "matrix_report.json",
                out_report=root / "comparison.json",
                gate_max_collision_rows=0,
            )
            self.assertEqual(report["gate"]["status"], "FAIL")
            self.assertFalse(report["gate"]["passed"])
            self.assertIn("COLLISION_ROWS_EXCEEDED", report["gate"]["failure_codes"])
            markdown = (root / "comparison.md").read_text(encoding="utf-8")
            self.assertIn("## Gate", markdown)
            self.assertIn("COLLISION_ROWS_EXCEEDED", markdown)

    def test_scenario_batch_comparison_gate_profile_can_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "attention_logical_scenarios.json"
            self._write_attention_logical_scenarios(logical_path)
            workflow_result = run_scenario_variant_workflow(
                logical_scenarios_path=str(logical_path),
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
            report = build_scenario_batch_comparison_report(
                variant_workflow_report_path=Path(workflow_result["workflow_report_path"]),
                matrix_sweep_report_path=root / "matrix_report.json",
                out_report=root / "comparison.json",
                gate_profile_path=P_VALIDATION_FIXTURE_ROOT / "scenario_batch_gate_strict_v0.json",
            )
            self.assertEqual(report["gate"]["status"], "FAIL")
            self.assertEqual(
                report["gate"]["policy"]["profile_id"],
                "scenario_batch_gate_strict_v0",
            )
            self.assertEqual(
                report["inputs"]["gate_profile_path"],
                str((P_VALIDATION_FIXTURE_ROOT / "scenario_batch_gate_strict_v0.json").resolve()),
            )
            self.assertIn("ATTENTION_ROWS_EXCEEDED", report["gate"]["failure_codes"])

    def test_scenario_batch_comparison_attention_rows_include_path_interaction_reasons(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "route_attention_logical_scenarios.json"
            self._write_route_attention_logical_scenarios(logical_path)
            workflow_result = run_scenario_variant_workflow(
                logical_scenarios_path=str(logical_path),
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
            report = build_scenario_batch_comparison_report(
                variant_workflow_report_path=Path(workflow_result["workflow_report_path"]),
                matrix_sweep_report_path=root / "matrix_report.json",
                out_report=root / "comparison.json",
            )
            self.assertGreater(report["comparison_tables"]["attention_row_count"], 0)
            attention_row = next(
                row
                for row in report["comparison_tables"]["attention_rows"]
                if row["group_id"] == "scn_route_attention"
            )
            self.assertIn("PATH_CONFLICT_PRESENT", attention_row["attention_reasons"])
            self.assertIn("MERGE_CONFLICT_PRESENT", attention_row["attention_reasons"])
            self.assertIn("PATH_TTC_UNDER_3S", attention_row["attention_reasons"])
            self.assertGreater(attention_row["merge_conflict_rows"], 0)
            self.assertEqual(report["comparison_tables"]["attention_reason_counts"]["PATH_CONFLICT_PRESENT"], 1)
            self.assertEqual(report["comparison_tables"]["attention_reason_counts"]["MERGE_CONFLICT_PRESENT"], 1)
            self.assertEqual(report["comparison_tables"]["attention_reason_counts"]["PATH_TTC_UNDER_3S"], 1)

    def test_scenario_batch_comparison_cli_threshold_can_override_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "attention_logical_scenarios.json"
            self._write_attention_logical_scenarios(logical_path)
            gate_profile_path = root / "gate_profile.json"
            gate_profile_path.write_text(
                json.dumps(
                    {
                        "gate_profile_schema_version": "scenario_batch_gate_profile_v0",
                        "profile_id": "collision_only_profile",
                        "policy": {
                            "max_collision_rows": 0,
                        },
                    },
                    indent=2,
                    ensure_ascii=True,
                )
                + "\n",
                encoding="utf-8",
            )
            workflow_result = run_scenario_variant_workflow(
                logical_scenarios_path=str(logical_path),
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
            report = build_scenario_batch_comparison_report(
                variant_workflow_report_path=Path(workflow_result["workflow_report_path"]),
                matrix_sweep_report_path=root / "matrix_report.json",
                out_report=root / "comparison.json",
                gate_profile_path=gate_profile_path,
                gate_max_collision_rows=1,
            )
            self.assertEqual(report["gate"]["status"], "PASS")
            self.assertEqual(report["gate"]["policy"]["profile_id"], "collision_only_profile")
            self.assertEqual(report["gate"]["policy"]["max_collision_rows"], 1)
            self.assertEqual(report["gate"]["failure_codes"], [])

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

    def test_scenario_batch_comparison_cli_can_resolve_gate_profile_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "attention_logical_scenarios.json"
            self._write_attention_logical_scenarios(logical_path)
            workflow_result = run_scenario_variant_workflow(
                logical_scenarios_path=str(logical_path),
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
                        "--gate-profile-id",
                        "scenario_batch_gate_strict_v0",
                        "--gate-profile-dir",
                        str(P_VALIDATION_FIXTURE_ROOT),
                    ]
                )
            self.assertEqual(exit_code, 2)
            payload = json.loads((root / "comparison.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["gate"]["status"], "FAIL")
            self.assertEqual(payload["gate"]["policy"]["profile_id"], "scenario_batch_gate_strict_v0")


if __name__ == "__main__":
    unittest.main()
