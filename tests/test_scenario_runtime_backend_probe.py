from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from hybrid_sensor_sim.tools.scenario_runtime_backend_probe import (
    SCENARIO_RUNTIME_BACKEND_PROBE_REPORT_SCHEMA_VERSION_V0,
    main as scenario_runtime_backend_probe_main,
    run_scenario_runtime_backend_probe,
)


class ScenarioRuntimeBackendProbeTests(unittest.TestCase):
    def test_run_scenario_runtime_backend_probe_passes_matching_expectations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_report = root / "runtime_report.json"
            source_report.write_text("{}", encoding="utf-8")
            fake_rebridge_report = {
                "status": "SUCCEEDED",
                "status_summary": {
                    "backend_output_smoke_status": "COMPLETE",
                    "backend_output_comparison_status": "MATCHED",
                    "backend_output_origin_status": "BACKEND_RUNTIME_ONLY",
                    "autoware_pipeline_status": "READY",
                    "autoware_availability_mode": "runtime",
                    "autoware_semantic_topic_recovered": True,
                    "autoware_semantic_recovery_source": "supplemental_semantic_smoke",
                },
                "rebridge": {
                    "comparison": {
                        "source_missing_required_topics": [
                            "/sensing/camera/camera_front/semantic/image_raw"
                        ],
                        "refreshed_missing_required_topics": [],
                        "recovered_required_topics": [
                            "/sensing/camera/camera_front/semantic/image_raw"
                        ],
                    }
                },
                "backend_smoke_workflow": {
                    "status": "HANDOFF_DOCKER_OUTPUT_READY",
                    "smoke": {"summary_path": str(root / "smoke_summary.json")},
                },
                "artifacts": {
                    "autoware_pipeline_manifest_path": str(root / "autoware_pipeline_manifest.json"),
                    "autoware_dataset_manifest_path": str(root / "autoware_dataset_manifest.json"),
                    "autoware_consumer_input_manifest_path": str(root / "autoware_consumer_input_manifest.json"),
                    "autoware_topic_catalog_path": str(root / "autoware_topic_catalog.json"),
                },
            }
            with patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_probe.run_scenario_runtime_backend_rebridge",
                return_value={
                    "workflow_report_path": root / "rebridge_report.json",
                    "workflow_markdown_path": root / "rebridge_report.md",
                    "workflow_report": fake_rebridge_report,
                },
            ):
                result = run_scenario_runtime_backend_probe(
                    runtime_backend_workflow_report_path=str(source_report),
                    out_root=root / "probe",
                    probe_id="semantic-ready",
                    consumer_profile_id="semantic_perception_v0",
                    expect_runtime_status="SUCCEEDED",
                    expect_autoware_status="READY",
                )
            report = result["report"]
            self.assertEqual(
                report["scenario_runtime_backend_probe_report_schema_version"],
                SCENARIO_RUNTIME_BACKEND_PROBE_REPORT_SCHEMA_VERSION_V0,
            )
            self.assertEqual(report["status"], "PASS")
            self.assertEqual(report["probe_id"], "semantic-ready")
            self.assertEqual(report["summary"]["runtime_status"], "SUCCEEDED")
            self.assertTrue(report["summary"]["semantic_topic_recovered"])
            self.assertEqual(
                report["summary"]["recovered_required_topics"],
                ["/sensing/camera/camera_front/semantic/image_raw"],
            )
            self.assertTrue(result["report_path"].is_file())
            self.assertTrue(result["markdown_path"].is_file())

    def test_scenario_runtime_backend_probe_main_fails_on_expectation_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_report = root / "runtime_report.json"
            source_report.write_text("{}", encoding="utf-8")
            fake_rebridge_report = {
                "status": "DEGRADED",
                "status_summary": {
                    "backend_output_smoke_status": "COMPLETE",
                    "backend_output_comparison_status": "MATCHED",
                    "backend_output_origin_status": "BACKEND_RUNTIME_ONLY",
                    "autoware_pipeline_status": "DEGRADED",
                    "autoware_availability_mode": "runtime",
                },
                "rebridge": {"comparison": {}},
                "backend_smoke_workflow": {"status": "HANDOFF_DOCKER_OUTPUT_READY"},
                "artifacts": {},
            }
            with patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_probe.run_scenario_runtime_backend_rebridge",
                return_value={
                    "workflow_report_path": root / "rebridge_report.json",
                    "workflow_markdown_path": root / "rebridge_report.md",
                    "workflow_report": fake_rebridge_report,
                },
            ):
                exit_code = scenario_runtime_backend_probe_main(
                    [
                        "--runtime-backend-workflow-report",
                        str(source_report),
                        "--out-root",
                        str(root / "probe"),
                        "--expect-runtime-status",
                        "SUCCEEDED",
                        "--expect-autoware-status",
                        "READY",
                    ]
                )
            self.assertEqual(exit_code, 2)
            report = json.loads(
                (root / "probe" / "scenario_runtime_backend_probe_report_v0.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(report["status"], "FAIL")
            self.assertIn("RUNTIME_STATUS_MISMATCH", report["evaluation"]["failure_codes"])
            self.assertIn("AUTOWARE_STATUS_MISMATCH", report["evaluation"]["failure_codes"])

    def test_scenario_runtime_backend_probe_script_bootstraps_src_path(self) -> None:
        script_path = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "run_scenario_runtime_backend_probe.py"
        )
        completed = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("compact probe report", completed.stdout)


if __name__ == "__main__":
    unittest.main()
