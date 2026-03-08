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
            self.assertTrue(Path(result["workflow_report_path"]).is_file())
            self.assertTrue(Path(workflow_report["artifacts"]["variant_workflow_report_path"]).is_file())
            self.assertTrue(Path(workflow_report["artifacts"]["matrix_sweep_report_path"]).is_file())
            self.assertTrue(Path(workflow_report["artifacts"]["comparison_report_path"]).is_file())
            self.assertTrue(Path(workflow_report["artifacts"]["comparison_markdown_path"]).is_file())

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
            self.assertEqual(workflow_report["status"], "ATTENTION")
            self.assertGreater(workflow_report["comparison_summary"]["attention_row_count"], 0)
            self.assertEqual(workflow_report["variant_summary"]["selected_variant_count"], 1)
            self.assertEqual(workflow_report["matrix_summary"]["case_count"], 1)

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
