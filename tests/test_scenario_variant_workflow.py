from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.scenario_variant_workflow import (
    SCENARIO_VARIANT_WORKFLOW_REPORT_SCHEMA_VERSION_V0,
    run_scenario_variant_workflow,
)
from hybrid_sensor_sim.tools.scenario_variant_workflow import main as scenario_variant_workflow_main


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_validation"


class ScenarioVariantWorkflowTests(unittest.TestCase):
    def test_run_scenario_variant_workflow_generates_and_executes_variants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_scenario_variant_workflow(
                logical_scenarios_path=str(FIXTURE_ROOT / "highway_mixed_payloads_v0.json"),
                scenario_language_profile="",
                scenario_language_dir=FIXTURE_ROOT,
                out_root=root / "workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=2,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )
            workflow_report = result["workflow_report"]
            self.assertEqual(
                workflow_report["scenario_variant_workflow_report_schema_version"],
                SCENARIO_VARIANT_WORKFLOW_REPORT_SCHEMA_VERSION_V0,
            )
            self.assertEqual(workflow_report["variant_count"], 2)
            self.assertEqual(workflow_report["selected_variant_count"], 2)
            self.assertEqual(workflow_report["execution_status_counts"]["SUCCEEDED"], 2)
            self.assertEqual(workflow_report["by_payload_kind"]["log_scene_v0"]["variant_count"], 1)
            self.assertEqual(workflow_report["by_payload_kind"]["scenario_definition_v0"]["variant_count"], 1)
            self.assertEqual(
                workflow_report["by_payload_kind"]["scenario_definition_v0"]["execution_path_counts"]["direct_object_sim"],
                1,
            )
            self.assertEqual(workflow_report["successful_variant_row_count"], 2)
            self.assertEqual(workflow_report["non_success_variant_row_count"], 0)
            self.assertEqual(
                {row["rendered_payload_kind"] for row in workflow_report["successful_variant_rows"]},
                {"log_scene_v0", "scenario_definition_v0"},
            )
            self.assertTrue(Path(result["variants_report_path"]).is_file())
            self.assertTrue(Path(result["variant_run_report_path"]).is_file())
            self.assertTrue(Path(result["workflow_report_path"]).is_file())

    def test_run_scenario_variant_workflow_supports_scenario_language_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_scenario_variant_workflow(
                logical_scenarios_path="",
                scenario_language_profile="highway_mixed_payloads_v0",
                scenario_language_dir=FIXTURE_ROOT,
                out_root=root / "workflow",
                sampling="full",
                sample_size=0,
                seed=7,
                max_variants_per_scenario=1000,
                execution_max_variants=2,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )
            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["source_kind"], "scenario_language_profile")
            self.assertEqual(Path(workflow_report["source_path"]).name, "highway_mixed_payloads_v0.json")
            self.assertEqual(workflow_report["selected_variant_count"], 2)
            self.assertEqual(workflow_report["execution_status_counts"]["SUCCEEDED"], 2)

    def test_run_scenario_variant_workflow_emits_non_success_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_path = root / "logical_scenarios.json"
            logical_path.write_text(
                json.dumps(
                    {
                        "logical_scenarios": [
                            {
                                "scenario_id": "scn_skipped",
                                "parameters": {"speed": [1]},
                            },
                            {
                                "scenario_id": "scn_failed",
                                "parameters": {"speed": [1]},
                                "variant_payload_kind": "world_state_v0",
                                "variant_payload_template": {
                                    "world_state_schema_version": "world_state_v0",
                                    "variant_id": "${variant_id}",
                                },
                            },
                        ]
                    },
                    indent=2,
                    ensure_ascii=True,
                )
                + "\n",
                encoding="utf-8",
            )
            result = run_scenario_variant_workflow(
                logical_scenarios_path=str(logical_path),
                scenario_language_profile="",
                scenario_language_dir=FIXTURE_ROOT,
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
            workflow_report = result["workflow_report"]
            self.assertEqual(workflow_report["successful_variant_row_count"], 0)
            self.assertEqual(workflow_report["non_success_variant_row_count"], 2)
            self.assertEqual(
                {row["execution_status"] for row in workflow_report["non_success_variant_rows"]},
                {"SKIPPED", "FAILED"},
            )
            self.assertEqual(
                {row["failure_code"] for row in workflow_report["non_success_variant_rows"]},
                {"MISSING_RENDERED_PAYLOAD", "UNSUPPORTED_RENDERED_PAYLOAD_KIND"},
            )

    def test_scenario_variant_workflow_cli_writes_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = scenario_variant_workflow_main(
                    [
                        "--scenario-language-profile",
                        "highway_mixed_payloads_v0",
                        "--out-root",
                        str(root / "workflow"),
                        "--execution-max-variants",
                        "1",
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(
                (root / "workflow" / "scenario_variant_workflow_report_v0.json").read_text(encoding="utf-8")
            )
            self.assertEqual(payload["selected_variant_count"], 1)
            self.assertEqual(payload["execution_status_counts"]["SUCCEEDED"], 1)

    def test_scenario_variant_workflow_script_bootstraps_src_path(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_scenario_variant_workflow.py"
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0)
        self.assertIn("generate scenario variants and execute", proc.stdout.lower())


if __name__ == "__main__":
    unittest.main()
