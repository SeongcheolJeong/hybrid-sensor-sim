from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.scenario_batch_workflow import (
    SCENARIO_BATCH_WORKFLOW_REPORT_SCHEMA_VERSION_V0,
    run_scenario_batch_workflow,
)
from hybrid_sensor_sim.tools.scenario_batch_workflow import main as scenario_batch_workflow_main


P_VALIDATION_FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_validation"
P_SIM_ENGINE_FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_sim_engine"
P_MAP_TOOLSET_FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_map_toolset"


class ScenarioBatchWorkflowTests(unittest.TestCase):
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

    def _write_route_attention_scenario(self, path: Path) -> None:
        route_scenario = json.loads(
            (P_SIM_ENGINE_FIXTURE_ROOT / "highway_map_route_following_v0.json").read_text(encoding="utf-8")
        )
        route_scenario["canonical_map_path"] = str(
            (P_MAP_TOOLSET_FIXTURE_ROOT / "canonical_lane_graph_v0.json").resolve()
        )
        path.write_text(
            json.dumps(route_scenario, indent=2, ensure_ascii=True) + "\n",
            encoding="utf-8",
        )

    def _build_route_avoidance_scenario(
        self,
        *,
        scenario_id: str = "scn_route_avoidance",
        merge_ttc_threshold_sec: float = 3.5,
        merge_brake_scale: float = 0.5,
    ) -> dict[str, object]:
        return {
            "scenario_schema_version": "scenario_definition_v0",
            "scenario_id": scenario_id,
            "duration_sec": 1.0,
            "dt_sec": 0.1,
            "canonical_map_path": str((P_MAP_TOOLSET_FIXTURE_ROOT / "canonical_lane_graph_v0.json").resolve()),
            "route_definition": {
                "entry_lane_id": "lane_a",
                "exit_lane_id": "lane_c",
                "via_lane_ids": ["lane_b"],
                "cost_mode": "hops",
            },
            "enable_ego_collision_avoidance": True,
            "avoidance_ttc_threshold_sec": 3.5,
            "ego_max_brake_mps2": 5.0,
            "avoidance_interaction_policy": {
                "merge_conflict": {
                    "ttc_threshold_sec": merge_ttc_threshold_sec,
                    "brake_scale": merge_brake_scale,
                },
            },
            "ego": {"position_m": 0.0, "speed_mps": 10.0, "lane_id": "lane_a"},
            "npcs": [{"position_m": 18.0, "speed_mps": 4.0, "lane_id": "lane_b"}],
        }

    def _write_route_avoidance_logical_scenarios(self, path: Path) -> None:
        path.write_text(
            json.dumps(
                {
                    "logical_scenarios": [
                        {
                            "scenario_id": "scn_route_avoidance",
                            "parameters": {"scenario_variant": [1]},
                            "variant_payload_kind": "scenario_definition_v0",
                            "variant_payload_template": self._build_route_avoidance_scenario(),
                        }
                    ]
                },
                indent=2,
                ensure_ascii=True,
            )
            + "\n",
            encoding="utf-8",
        )

    def _write_ranked_avoidance_logical_scenarios(self, path: Path) -> None:
        path.write_text(
            json.dumps(
                {
                    "logical_scenarios": [
                        {
                            "scenario_id": "scn_avoidance_light",
                            "parameters": {"scenario_variant": [1]},
                            "variant_payload_kind": "scenario_definition_v0",
                            "variant_payload_template": self._build_route_avoidance_scenario(
                                scenario_id="scn_avoidance_light_payload",
                                merge_ttc_threshold_sec=0.1,
                                merge_brake_scale=0.25,
                            ),
                        },
                        {
                            "scenario_id": "scn_route_avoidance_heavy",
                            "parameters": {"scenario_variant": [1]},
                            "variant_payload_kind": "scenario_definition_v0",
                            "variant_payload_template": self._build_route_avoidance_scenario(
                                scenario_id="scn_route_avoidance_heavy_payload",
                                merge_ttc_threshold_sec=3.5,
                                merge_brake_scale=0.5,
                            ),
                        },
                    ]
                },
                indent=2,
                ensure_ascii=True,
            )
            + "\n",
            encoding="utf-8",
        )

    def test_run_scenario_batch_workflow_writes_all_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_scenario_batch_workflow(
                logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                out_root=root / "batch_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
                matrix_run_id_prefix="RUN_BATCH_MATRIX",
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
            workflow_report = result["workflow_report"]
            self.assertEqual(
                workflow_report["scenario_batch_workflow_report_schema_version"],
                SCENARIO_BATCH_WORKFLOW_REPORT_SCHEMA_VERSION_V0,
            )
            self.assertEqual(workflow_report["status"], "SUCCEEDED")
            self.assertEqual(workflow_report["variant_summary"]["selected_variant_count"], 1)
            self.assertEqual(workflow_report["matrix_summary"]["case_count"], 1)
            self.assertEqual(workflow_report["comparison_summary"]["attention_row_count"], 0)
            self.assertEqual(workflow_report["comparison_summary"]["logical_scenario_row_count"], 1)
            self.assertEqual(workflow_report["comparison_summary"]["matrix_group_row_count"], 1)
            self.assertEqual(workflow_report["comparison_summary"]["logical_scenario_health_row_count"], 1)
            self.assertEqual(workflow_report["comparison_summary"]["logical_scenario_health_status_counts"]["PASS"], 1)
            self.assertEqual(workflow_report["status_summary"]["workflow_status"], "SUCCEEDED")
            self.assertEqual(workflow_report["status_summary"]["final_status_source"], "default_success")
            self.assertEqual(workflow_report["status_summary"]["status_reason_codes"], [])
            self.assertEqual(workflow_report["status_summary"]["failing_logical_scenario_ids"], [])
            self.assertEqual(workflow_report["status_summary"]["attention_logical_scenario_ids"], [])
            self.assertEqual(workflow_report["status_summary"]["failing_matrix_group_ids"], [])
            self.assertEqual(workflow_report["status_summary"]["attention_matrix_group_ids"], [])
            self.assertEqual(workflow_report["status_summary"]["breached_gate_rule_count"], 0)
            self.assertEqual(workflow_report["status_summary"]["breached_gate_metric_ids"], [])
            self.assertEqual(workflow_report["status_summary"]["matrix_group_gate_failure_code_counts"], {})
            self.assertEqual(
                workflow_report["status_summary"]["worst_logical_scenario_row"]["logical_scenario_id"],
                "scn_log_route_relations",
            )
            self.assertEqual(
                workflow_report["status_summary"]["worst_matrix_group_row"]["matrix_group_id"],
                "sumo_highway_balanced_v0::sumo_platoon_sparse_v0",
            )
            self.assertEqual(
                [step["step_id"] for step in workflow_report["status_summary"]["decision_trace"]],
                ["variant_execution_failures", "matrix_success_cases", "comparison_gate", "attention_rows"],
            )
            self.assertEqual(
                workflow_report["comparison_summary"]["logical_scenario_health_gate_status_counts"]["DISABLED"],
                1,
            )
            self.assertEqual(len(workflow_report["comparison_summary"]["logical_scenario_rows"]), 1)
            self.assertEqual(len(workflow_report["comparison_summary"]["matrix_group_rows"]), 1)
            self.assertEqual(len(workflow_report["comparison_summary"]["logical_scenario_health_rows"]), 1)
            self.assertEqual(
                workflow_report["comparison_summary"]["logical_scenario_health_rows"][0]["gate_status"],
                "DISABLED",
            )
            self.assertEqual(
                workflow_report["comparison_summary"]["logical_scenario_health_rows"][0]["gate_failure_codes"],
                [],
            )
            self.assertTrue(Path(result["workflow_report_path"]).is_file())
            self.assertTrue(Path(workflow_report["artifacts"]["variant_workflow_report_path"]).is_file())
            self.assertTrue(Path(workflow_report["artifacts"]["matrix_sweep_report_path"]).is_file())
            self.assertTrue(Path(workflow_report["artifacts"]["comparison_report_path"]).is_file())
            self.assertTrue(Path(workflow_report["artifacts"]["comparison_markdown_path"]).is_file())
            self.assertTrue(Path(workflow_report["artifacts"]["workflow_markdown_path"]).is_file())
            self.assertEqual(workflow_report["comparison_summary"]["gate"]["status"], "DISABLED")
            markdown = Path(result["workflow_markdown_path"]).read_text(encoding="utf-8")
            self.assertIn("## Logical Scenario Health", markdown)
            self.assertIn("## Logical Scenario Summary", markdown)
            self.assertIn("## Matrix Group Summary", markdown)
            self.assertIn("## Successful Variants", markdown)
            self.assertIn("## Non-Success Variants", markdown)
            self.assertIn("Gate Failure Codes", markdown)
            self.assertIn("Min TTC Path", markdown)
            self.assertIn("No non-success variants.", markdown)

    def test_scenario_batch_workflow_reports_attention_without_failing_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "attention_logical_scenarios.json"
            self._write_attention_logical_scenarios(logical_path)
            result = run_scenario_batch_workflow(
                logical_scenarios_path=str(logical_path),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                out_root=root / "batch_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=2,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
                matrix_run_id_prefix="RUN_BATCH_MATRIX",
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
            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["status"], "ATTENTION")
            self.assertGreater(workflow_report["comparison_summary"]["attention_row_count"], 0)
            self.assertEqual(workflow_report["status_summary"]["final_status_source"], "attention_rows")
            self.assertIn("ATTENTION_ROWS_PRESENT", workflow_report["status_summary"]["status_reason_codes"])
            self.assertEqual(
                workflow_report["status_summary"]["attention_logical_scenario_ids"],
                ["scn_collision_attention"],
            )
            self.assertEqual(workflow_report["status_summary"]["attention_matrix_group_ids"], [])
            self.assertEqual(
                workflow_report["status_summary"]["worst_logical_scenario_row"]["logical_scenario_id"],
                "scn_collision_attention",
            )
            self.assertEqual(workflow_report["variant_summary"]["selected_variant_count"], 1)
            self.assertEqual(workflow_report["matrix_summary"]["case_count"], 1)
            markdown = Path(result["workflow_markdown_path"]).read_text(encoding="utf-8")
            self.assertIn("# Scenario Batch Workflow", markdown)
            self.assertIn("ATTENTION", markdown)

    def test_scenario_batch_workflow_cli_fail_on_attention(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "attention_logical_scenarios.json"
            self._write_attention_logical_scenarios(logical_path)
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = scenario_batch_workflow_main(
                    [
                        "--logical-scenarios",
                        str(logical_path),
                        "--matrix-scenario",
                        str(P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json"),
                        "--out-root",
                        str(root / "batch_workflow"),
                        "--execution-max-variants",
                        "1",
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
                        "--fail-on-attention",
                    ]
                )
            self.assertEqual(exit_code, 2)
            payload = json.loads(
                (root / "batch_workflow" / "scenario_batch_workflow_report_v0.json").read_text(encoding="utf-8")
            )
            self.assertEqual(payload["status"], "ATTENTION")

    def test_scenario_batch_workflow_gate_failure_sets_failed_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "attention_logical_scenarios.json"
            self._write_attention_logical_scenarios(logical_path)
            result = run_scenario_batch_workflow(
                logical_scenarios_path=str(logical_path),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                out_root=root / "batch_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
                matrix_run_id_prefix="RUN_BATCH_MATRIX",
                traffic_profile_ids=["sumo_highway_balanced_v0"],
                traffic_actor_pattern_ids=["sumo_platoon_sparse_v0"],
                traffic_npc_speed_scale_values=[1.0],
                tire_friction_coeff_values=[1.0],
                surface_friction_scale_values=[1.0],
                enable_ego_collision_avoidance=False,
                avoidance_ttc_threshold_sec=2.5,
                ego_max_brake_mps2=6.0,
                max_cases=0,
                gate_max_collision_rows=0,
            )
            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["status"], "FAILED")
            self.assertEqual(workflow_report["comparison_summary"]["gate"]["status"], "FAIL")
            self.assertEqual(workflow_report["status_summary"]["final_status_source"], "comparison_gate")
            self.assertIn("BATCH_GATE_FAILED", workflow_report["status_summary"]["status_reason_codes"])
            self.assertEqual(
                workflow_report["status_summary"]["failing_logical_scenario_ids"],
                ["scn_collision_attention"],
            )
            self.assertEqual(workflow_report["status_summary"]["failing_matrix_group_ids"], [])
            self.assertIn("collision_row_count", workflow_report["status_summary"]["breached_gate_metric_ids"])
            self.assertEqual(workflow_report["status_summary"]["breached_gate_rule_count"], 1)
            self.assertEqual(
                workflow_report["status_summary"]["worst_logical_scenario_row"]["logical_scenario_id"],
                "scn_collision_attention",
            )
            self.assertEqual(
                workflow_report["comparison_summary"]["logical_scenario_health_status_counts"]["FAIL"],
                1,
            )
            self.assertEqual(
                workflow_report["comparison_summary"]["logical_scenario_health_gate_status_counts"]["FAIL"],
                1,
            )
            self.assertIn(
                "COLLISION_PRESENT",
                workflow_report["comparison_summary"]["logical_scenario_health_rows"][0]["health_reasons"],
            )
            self.assertEqual(
                workflow_report["comparison_summary"]["logical_scenario_health_rows"][0]["gate_status"],
                "FAIL",
            )
            self.assertIn(
                "COLLISION_ROWS_EXCEEDED",
                workflow_report["comparison_summary"]["logical_scenario_health_rows"][0]["gate_failure_codes"],
            )
            self.assertIn(
                "COLLISION_ROWS_EXCEEDED",
                workflow_report["comparison_summary"]["gate"]["failure_codes"],
            )

    def test_scenario_batch_workflow_gate_profile_failure_sets_failed_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "attention_logical_scenarios.json"
            self._write_attention_logical_scenarios(logical_path)
            result = run_scenario_batch_workflow(
                logical_scenarios_path=str(logical_path),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                out_root=root / "batch_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
                matrix_run_id_prefix="RUN_BATCH_MATRIX",
                traffic_profile_ids=["sumo_highway_balanced_v0"],
                traffic_actor_pattern_ids=["sumo_platoon_sparse_v0"],
                traffic_npc_speed_scale_values=[1.0],
                tire_friction_coeff_values=[1.0],
                surface_friction_scale_values=[1.0],
                enable_ego_collision_avoidance=False,
                avoidance_ttc_threshold_sec=2.5,
                ego_max_brake_mps2=6.0,
                max_cases=0,
                gate_profile_path=P_VALIDATION_FIXTURE_ROOT / "scenario_batch_gate_strict_v0.json",
            )
            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["status"], "FAILED")
            self.assertEqual(workflow_report["comparison_summary"]["gate"]["status"], "FAIL")
            self.assertEqual(
                workflow_report["comparison_summary"]["gate"]["policy"]["profile_id"],
                "scenario_batch_gate_strict_v0",
            )
            self.assertEqual(
                workflow_report["comparison_summary"]["logical_scenario_health_rows"][0]["gate_status"],
                "FAIL",
            )
            self.assertIn(
                "ATTENTION_ROWS_EXCEEDED",
                workflow_report["comparison_summary"]["gate"]["failure_codes"],
            )
            self.assertIn(
                "ATTENTION_ROWS_EXCEEDED",
                workflow_report["comparison_summary"]["logical_scenario_health_rows"][0]["gate_failure_codes"],
            )

    def test_scenario_batch_workflow_route_interaction_gate_failure_sets_failed_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "route_attention_logical_scenarios.json"
            self._write_route_attention_logical_scenarios(logical_path)
            result = run_scenario_batch_workflow(
                logical_scenarios_path=str(logical_path),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                out_root=root / "batch_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
                matrix_run_id_prefix="RUN_BATCH_MATRIX",
                traffic_profile_ids=["sumo_highway_balanced_v0"],
                traffic_actor_pattern_ids=["sumo_platoon_sparse_v0"],
                traffic_npc_speed_scale_values=[1.0],
                tire_friction_coeff_values=[1.0],
                surface_friction_scale_values=[1.0],
                enable_ego_collision_avoidance=False,
                avoidance_ttc_threshold_sec=2.5,
                ego_max_brake_mps2=6.0,
                max_cases=0,
                gate_max_merge_conflict_rows=0,
            )
            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["status"], "FAILED")
            self.assertIn(
                "MERGE_CONFLICT_ROWS_EXCEEDED",
                workflow_report["comparison_summary"]["gate"]["failure_codes"],
            )
            self.assertEqual(workflow_report["status_summary"]["final_status_source"], "comparison_gate")
            self.assertIn("BATCH_GATE_FAILED", workflow_report["status_summary"]["status_reason_codes"])
            self.assertEqual(
                workflow_report["status_summary"]["failing_logical_scenario_ids"],
                ["scn_route_attention"],
            )
            self.assertEqual(
                workflow_report["status_summary"]["attention_logical_scenario_ids"],
                ["scn_route_attention"],
            )
            self.assertEqual(workflow_report["status_summary"]["attention_matrix_group_ids"], [])
            self.assertEqual(
                workflow_report["status_summary"]["worst_logical_scenario_row"]["logical_scenario_id"],
                "scn_route_attention",
            )
            health_row = workflow_report["comparison_summary"]["logical_scenario_health_rows"][0]
            self.assertEqual(health_row["gate_status"], "FAIL")
            self.assertIn("MERGE_CONFLICT_PRESENT", health_row["health_reasons"])
            self.assertIn("PATH_CONFLICT_PRESENT", health_row["health_reasons"])
            self.assertIn("MERGE_CONFLICT_ROWS_EXCEEDED", health_row["gate_failure_codes"])
            self.assertEqual(workflow_report["comparison_summary"]["failing_logical_scenario_row_count"], 1)
            failing_row = workflow_report["comparison_summary"]["failing_logical_scenario_rows"][0]
            self.assertEqual(failing_row["logical_scenario_id"], "scn_route_attention")
            self.assertEqual(
                workflow_report["comparison_summary"]["failing_logical_scenario_gate_failure_code_counts"][
                    "MERGE_CONFLICT_ROWS_EXCEEDED"
                ],
                1,
            )
            self.assertEqual(
                workflow_report["comparison_summary"]["attention_reason_counts"]["PATH_CONFLICT_PRESENT"],
                1,
            )
            self.assertIn("merge_conflict_row_count", workflow_report["status_summary"]["breached_gate_metric_ids"])

    def test_scenario_batch_workflow_tracks_matrix_group_failure_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix_scenario_path = root / "matrix_route_attention.json"
            self._write_route_attention_scenario(matrix_scenario_path)
            result = run_scenario_batch_workflow(
                logical_scenarios_path=str(P_VALIDATION_FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=matrix_scenario_path,
                out_root=root / "batch_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
                matrix_run_id_prefix="RUN_BATCH_MATRIX",
                traffic_profile_ids=["sumo_highway_balanced_v0"],
                traffic_actor_pattern_ids=["sumo_platoon_sparse_v0"],
                traffic_npc_speed_scale_values=[1.0],
                tire_friction_coeff_values=[1.0],
                surface_friction_scale_values=[1.0],
                enable_ego_collision_avoidance=False,
                avoidance_ttc_threshold_sec=2.5,
                ego_max_brake_mps2=6.0,
                max_cases=0,
                gate_max_merge_conflict_rows=0,
            )
            workflow_report = result["workflow_report"]
            expected_group_id = "sumo_highway_balanced_v0::sumo_platoon_sparse_v0"
            self.assertEqual(workflow_report["status"], "FAILED")
            self.assertEqual(
                workflow_report["status_summary"]["failing_matrix_group_ids"],
                [expected_group_id],
            )
            self.assertEqual(workflow_report["status_summary"]["attention_matrix_group_ids"], [])
            self.assertEqual(
                workflow_report["status_summary"]["matrix_group_gate_failure_code_counts"]["MERGE_CONFLICT_ROWS_EXCEEDED"],
                1,
            )
            self.assertEqual(
                workflow_report["status_summary"]["worst_matrix_group_row"]["matrix_group_id"],
                expected_group_id,
            )
            markdown = Path(result["workflow_markdown_path"]).read_text(encoding="utf-8")
            self.assertIn(expected_group_id, markdown)

    def test_scenario_batch_workflow_reports_avoidance_trigger_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "route_avoidance_logical_scenarios.json"
            self._write_route_avoidance_logical_scenarios(logical_path)
            matrix_scenario_path = root / "route_avoidance_matrix.json"
            matrix_scenario_path.write_text(
                json.dumps(self._build_route_avoidance_scenario(), indent=2, ensure_ascii=True) + "\n",
                encoding="utf-8",
            )
            result = run_scenario_batch_workflow(
                logical_scenarios_path=str(logical_path),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=matrix_scenario_path,
                out_root=root / "batch_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
                matrix_run_id_prefix="RUN_BATCH_MATRIX",
                traffic_profile_ids=["sumo_highway_balanced_v0"],
                traffic_actor_pattern_ids=["sumo_route_shifted_v0"],
                traffic_npc_speed_scale_values=[0.5],
                tire_friction_coeff_values=[1.0],
                surface_friction_scale_values=[1.0],
                enable_ego_collision_avoidance=True,
                avoidance_ttc_threshold_sec=10.0,
                ego_max_brake_mps2=5.0,
                max_cases=1,
            )
            workflow_report = result["workflow_report"]
            self.assertGreater(workflow_report["status_summary"]["avoidance_row_count"], 0)
            self.assertGreater(workflow_report["status_summary"]["avoidance_brake_event_count_total"], 0)
            self.assertGreater(
                workflow_report["status_summary"]["avoidance_trigger_counts_by_interaction_kind"]["merge_conflict"],
                0,
            )
            self.assertGreater(
                workflow_report["comparison_summary"]["logical_scenario_rows"][0]["ego_avoidance_brake_event_count_total"],
                0,
            )
            self.assertGreater(
                workflow_report["comparison_summary"]["matrix_group_rows"][0]["ego_avoidance_brake_event_count_total"],
                0,
            )
            markdown = Path(result["workflow_markdown_path"]).read_text(encoding="utf-8")
            self.assertIn("Avoidance brake event count", markdown)
            self.assertIn("Avoidance trigger counts", markdown)

    def test_scenario_batch_workflow_avoidance_trigger_gate_sets_failed_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "route_avoidance_logical_scenarios.json"
            self._write_route_avoidance_logical_scenarios(logical_path)
            matrix_scenario_path = root / "route_avoidance_matrix.json"
            matrix_scenario_path.write_text(
                json.dumps(self._build_route_avoidance_scenario(), indent=2, ensure_ascii=True) + "\n",
                encoding="utf-8",
            )
            result = run_scenario_batch_workflow(
                logical_scenarios_path=str(logical_path),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=matrix_scenario_path,
                out_root=root / "batch_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
                matrix_run_id_prefix="RUN_BATCH_MATRIX",
                traffic_profile_ids=["sumo_highway_balanced_v0"],
                traffic_actor_pattern_ids=["sumo_route_shifted_v0"],
                traffic_npc_speed_scale_values=[0.5],
                tire_friction_coeff_values=[1.0],
                surface_friction_scale_values=[1.0],
                enable_ego_collision_avoidance=True,
                avoidance_ttc_threshold_sec=10.0,
                ego_max_brake_mps2=5.0,
                max_cases=1,
                gate_max_avoidance_merge_conflict_triggers=0,
            )
            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["status"], "FAILED")
            self.assertEqual(workflow_report["status_summary"]["final_status_source"], "comparison_gate")
            self.assertIn(
                "AVOIDANCE_MERGE_CONFLICT_TRIGGER_COUNT_EXCEEDED",
                workflow_report["comparison_summary"]["gate"]["failure_codes"],
            )
            self.assertIn(
                "ego_avoidance_merge_conflict_trigger_count",
                workflow_report["status_summary"]["breached_gate_metric_ids"],
            )
            health_row = workflow_report["comparison_summary"]["logical_scenario_health_rows"][0]
            self.assertEqual(health_row["gate_status"], "FAIL")
            self.assertIn(
                "AVOIDANCE_MERGE_CONFLICT_TRIGGER_COUNT_EXCEEDED",
                health_row["gate_failure_codes"],
            )
            self.assertEqual(
                workflow_report["status_summary"]["matrix_group_gate_failure_code_counts"][
                    "AVOIDANCE_MERGE_CONFLICT_TRIGGER_COUNT_EXCEEDED"
                ],
                1,
            )

    def test_scenario_batch_workflow_ranks_avoidance_heavy_logical_scenario_as_worst(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "ranked_avoidance_logical_scenarios.json"
            self._write_ranked_avoidance_logical_scenarios(logical_path)
            result = run_scenario_batch_workflow(
                logical_scenarios_path=str(logical_path),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json",
                out_root=root / "batch_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=2,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
                matrix_run_id_prefix="RUN_BATCH_MATRIX",
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
            workflow_report = result["workflow_report"]
            self.assertIn(workflow_report["status"], {"SUCCEEDED", "ATTENTION"})
            self.assertEqual(
                workflow_report["status_summary"]["worst_logical_scenario_row"]["logical_scenario_id"],
                "scn_route_avoidance_heavy",
            )
            health_rows = {
                row["logical_scenario_id"]: row
                for row in workflow_report["comparison_summary"]["logical_scenario_health_rows"]
            }
            self.assertEqual(
                health_rows["scn_avoidance_light"]["merge_conflict_row_count"],
                health_rows["scn_route_avoidance_heavy"]["merge_conflict_row_count"],
            )
            self.assertGreater(health_rows["scn_avoidance_light"]["merge_conflict_row_count"], 0)
            self.assertEqual(health_rows["scn_avoidance_light"]["ego_avoidance_brake_event_count_total"], 0)
            self.assertGreater(
                health_rows["scn_route_avoidance_heavy"]["ego_avoidance_brake_event_count_total"],
                0,
            )

    def test_scenario_batch_workflow_exposes_avoidance_fields_on_worst_matrix_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "route_avoidance_logical_scenarios.json"
            self._write_route_avoidance_logical_scenarios(logical_path)
            matrix_scenario_path = root / "route_avoidance_matrix.json"
            matrix_scenario_path.write_text(
                json.dumps(self._build_route_avoidance_scenario(), indent=2, ensure_ascii=True) + "\n",
                encoding="utf-8",
            )
            result = run_scenario_batch_workflow(
                logical_scenarios_path=str(logical_path),
                scenario_language_profile="",
                scenario_language_dir=P_VALIDATION_FIXTURE_ROOT,
                matrix_scenario_path=matrix_scenario_path,
                out_root=root / "batch_workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=1,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
                matrix_run_id_prefix="RUN_BATCH_MATRIX",
                traffic_profile_ids=["sumo_highway_balanced_v0"],
                traffic_actor_pattern_ids=["sumo_route_shifted_v0"],
                traffic_npc_speed_scale_values=[0.5],
                tire_friction_coeff_values=[1.0],
                surface_friction_scale_values=[1.0],
                enable_ego_collision_avoidance=True,
                avoidance_ttc_threshold_sec=10.0,
                ego_max_brake_mps2=5.0,
                max_cases=1,
            )
            worst_row = result["workflow_report"]["status_summary"]["worst_matrix_group_row"]
            self.assertIn("ego_avoidance_brake_event_count_total", worst_row)
            self.assertIn("ego_avoidance_trigger_counts_by_interaction_kind", worst_row)
            self.assertGreater(worst_row["ego_avoidance_brake_event_count_total"], 0)
            self.assertGreater(
                worst_row["ego_avoidance_trigger_counts_by_interaction_kind"]["merge_conflict"],
                0,
            )

    def test_scenario_batch_workflow_cli_can_resolve_gate_profile_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "attention_logical_scenarios.json"
            self._write_attention_logical_scenarios(logical_path)
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = scenario_batch_workflow_main(
                    [
                        "--logical-scenarios",
                        str(logical_path),
                        "--matrix-scenario",
                        str(P_SIM_ENGINE_FIXTURE_ROOT / "highway_safe_following_v0.json"),
                        "--out-root",
                        str(root / "batch_workflow"),
                        "--execution-max-variants",
                        "1",
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
                        "--gate-profile-id",
                        "scenario_batch_gate_strict_v0",
                        "--gate-profile-dir",
                        str(P_VALIDATION_FIXTURE_ROOT),
                    ]
                )
            self.assertEqual(exit_code, 2)
            payload = json.loads(
                (root / "batch_workflow" / "scenario_batch_workflow_report_v0.json").read_text(encoding="utf-8")
            )
            self.assertEqual(payload["status"], "FAILED")
            self.assertEqual(payload["comparison_summary"]["gate"]["policy"]["profile_id"], "scenario_batch_gate_strict_v0")

    def test_scenario_batch_workflow_cli_can_resolve_avoidance_gate_profile_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "route_avoidance_logical_scenarios.json"
            self._write_route_avoidance_logical_scenarios(logical_path)
            matrix_scenario_path = root / "route_avoidance_matrix.json"
            matrix_scenario_path.write_text(
                json.dumps(self._build_route_avoidance_scenario(), indent=2, ensure_ascii=True) + "\n",
                encoding="utf-8",
            )
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = scenario_batch_workflow_main(
                    [
                        "--logical-scenarios",
                        str(logical_path),
                        "--matrix-scenario",
                        str(matrix_scenario_path),
                        "--out-root",
                        str(root / "batch_workflow"),
                        "--execution-max-variants",
                        "1",
                        "--traffic-profile-ids",
                        "sumo_highway_balanced_v0",
                        "--traffic-actor-pattern-ids",
                        "sumo_route_shifted_v0",
                        "--traffic-npc-speed-scale-values",
                        "0.5",
                        "--tire-friction-coeff-values",
                        "1.0",
                        "--surface-friction-scale-values",
                        "1.0",
                        "--enable-ego-collision-avoidance",
                        "--avoidance-ttc-threshold-sec",
                        "10.0",
                        "--ego-max-brake-mps2",
                        "5.0",
                        "--max-cases",
                        "1",
                        "--gate-profile-id",
                        "scenario_batch_gate_avoidance_v0",
                        "--gate-profile-dir",
                        str(P_VALIDATION_FIXTURE_ROOT),
                    ]
                )
            self.assertEqual(exit_code, 2)
            payload = json.loads(
                (root / "batch_workflow" / "scenario_batch_workflow_report_v0.json").read_text(encoding="utf-8")
            )
            self.assertEqual(payload["status"], "FAILED")
            self.assertEqual(
                payload["comparison_summary"]["gate"]["policy"]["profile_id"],
                "scenario_batch_gate_avoidance_v0",
            )
            self.assertIn("AVOIDANCE_ROWS_EXCEEDED", payload["comparison_summary"]["gate"]["failure_codes"])
            self.assertIn("AVOIDANCE_BRAKE_EVENTS_EXCEEDED", payload["comparison_summary"]["gate"]["failure_codes"])
            self.assertIn(
                "AVOIDANCE_MERGE_CONFLICT_TRIGGER_COUNT_EXCEEDED",
                payload["comparison_summary"]["gate"]["failure_codes"],
            )

    def test_scenario_batch_workflow_script_bootstraps_src_path(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_scenario_batch_workflow.py"
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0)
        self.assertIn("variant workflow, matrix sweep, and cross-batch comparison", proc.stdout.lower())


if __name__ == "__main__":
    unittest.main()
