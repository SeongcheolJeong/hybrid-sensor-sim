from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from hybrid_sensor_sim.tools.scenario_runtime_backend_probe_set import (
    DEFAULT_SCENARIO_RUNTIME_BACKEND_PROBE_SET_ID,
    SCENARIO_RUNTIME_BACKEND_PROBE_SET_REPORT_SCHEMA_VERSION_V0,
    run_scenario_runtime_backend_probe_set,
)


class ScenarioRuntimeBackendProbeSetTests(unittest.TestCase):
    def test_run_scenario_runtime_backend_probe_set_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tracking = (
                root
                / "artifacts"
                / "scenario_runtime_backend_real_awsim_tracking_ready_probe"
                / "scenario_runtime_backend_workflow_report_v0.json"
            )
            tracking.parent.mkdir(parents=True, exist_ok=True)
            tracking.write_text("{}", encoding="utf-8")
            semantic_primary = (
                root
                / "artifacts"
                / "scenario_runtime_backend_real_awsim_probe_v14"
                / "scenario_runtime_backend_workflow_report_v0.json"
            )
            semantic_primary.parent.mkdir(parents=True, exist_ok=True)
            semantic_primary.write_text("{}", encoding="utf-8")
            degraded = (
                root
                / "artifacts"
                / "scenario_runtime_backend_real_awsim_degraded_runtime_probe"
                / "scenario_runtime_backend_workflow_report_v0.json"
            )
            degraded.parent.mkdir(parents=True, exist_ok=True)
            degraded.write_text("{}", encoding="utf-8")

            def _fake_probe(**kwargs):
                out_root = Path(kwargs["out_root"])
                probe_id = kwargs["probe_id"]
                report_path = out_root / "scenario_runtime_backend_probe_report_v0.json"
                markdown_path = out_root / "scenario_runtime_backend_probe_report_v0.md"
                out_root.mkdir(parents=True, exist_ok=True)
                report = {
                    "probe_id": probe_id,
                    "consumer_profile_id": kwargs["consumer_profile_id"],
                    "status": "PASS",
                    "summary": {
                        "runtime_status": kwargs["expect_runtime_status"],
                        "autoware_pipeline_status": kwargs["expect_autoware_status"],
                        "semantic_topic_recovered": probe_id == "semantic_recovery_ready",
                    },
                    "evaluation": {"failure_codes": []},
                }
                report_path.write_text(json.dumps(report), encoding="utf-8")
                markdown_path.write_text("# ok\n", encoding="utf-8")
                return {
                    "report_path": report_path,
                    "markdown_path": markdown_path,
                    "report": report,
                    "rebridge_result": {
                        "workflow_report": {
                            "status_summary": {
                                "backend_runtime_strategy": "linux_handoff_packaged_runtime",
                                "backend_runtime_strategy_source": "setup_summary.runtime_strategy",
                                "backend_runtime_preferred_runtime_source": "packaged_runtime",
                                "backend_runtime_strategy_reason_codes": [
                                    "HOST_INCOMPATIBLE_PACKAGED_RUNTIME"
                                ],
                                "backend_runtime_recommended_command": "python3 scripts/run_renderer_backend_workflow.py --backend awsim --dry-run",
                                "backend_runtime_selected_path": "/tmp/AWSIM-Demo.x86_64",
                                "backend_runtime_docker_storage_status": "healthy",
                            },
                            "rebridge": {
                                "comparison": {
                                    "source_runtime_status": "DEGRADED"
                                    if probe_id == "semantic_recovery_ready"
                                    else "SUCCEEDED",
                                    "source_autoware_pipeline_status": "DEGRADED"
                                    if probe_id == "semantic_recovery_ready"
                                    else "READY",
                                    "source_missing_required_topics": (
                                        ["/sensing/camera/camera_front/semantic/image_raw"]
                                        if probe_id == "semantic_recovery_ready"
                                        else []
                                    ),
                                    "refreshed_missing_required_topics": [],
                                    "recovered_required_topics": (
                                        ["/sensing/camera/camera_front/semantic/image_raw"]
                                        if probe_id == "semantic_recovery_ready"
                                        else []
                                    ),
                                }
                            }
                        }
                    },
                }

            with patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_probe_set.run_scenario_runtime_backend_probe",
                side_effect=_fake_probe,
            ):
                result = run_scenario_runtime_backend_probe_set(
                    out_root=root / "probe_set",
                    probe_set_id=DEFAULT_SCENARIO_RUNTIME_BACKEND_PROBE_SET_ID,
                    repo_root=root,
                )

            report = result["report"]
            self.assertEqual(
                report["scenario_runtime_backend_probe_set_report_schema_version"],
                SCENARIO_RUNTIME_BACKEND_PROBE_SET_REPORT_SCHEMA_VERSION_V0,
            )
            self.assertEqual(report["status"], "PASS")
            self.assertEqual(report["probe_count"], 3)
            self.assertEqual(report["pass_count"], 3)
            self.assertEqual(report["fail_count"], 0)
            self.assertEqual(
                report["passed_probe_ids"],
                ["semantic_primary_ready", "semantic_recovery_ready", "tracking_ready"],
            )
            self.assertEqual(
                report["runtime_native_ready_probe_ids"],
                ["semantic_primary_ready", "tracking_ready"],
            )
            self.assertEqual(
                report["supplemental_dependency_probe_ids"],
                ["semantic_recovery_ready"],
            )
            self.assertEqual(
                report["recovered_required_topics"],
                ["/sensing/camera/camera_front/semantic/image_raw"],
            )
            self.assertEqual(
                report["runtime_strategy_counts"],
                {"linux_handoff_packaged_runtime": 3},
            )
            self.assertEqual(
                report["runtime_strategy_probe_ids"]["linux_handoff_packaged_runtime"],
                ["semantic_primary_ready", "semantic_recovery_ready", "tracking_ready"],
            )
            self.assertEqual(
                report["runtime_strategy_reason_code_counts"],
                {"HOST_INCOMPATIBLE_PACKAGED_RUNTIME": 3},
            )
            self.assertEqual(
                report["blocking_reason_counts"],
                {"HOST_INCOMPATIBLE_PACKAGED_RUNTIME": 3},
            )
            self.assertEqual(
                report["blocking_reason_probe_ids"]["HOST_INCOMPATIBLE_PACKAGED_RUNTIME"],
                ["semantic_primary_ready", "semantic_recovery_ready", "tracking_ready"],
            )
            self.assertEqual(
                report["runtime_strategy_recommended_command_counts"],
                {
                    "python3 scripts/run_renderer_backend_workflow.py --backend awsim --dry-run": 3
                },
            )
            self.assertEqual(
                report["recommended_next_command"],
                "python3 scripts/run_renderer_backend_workflow.py --backend awsim --dry-run",
            )
            self.assertTrue(result["report_path"].is_file())
            self.assertTrue(result["markdown_path"].is_file())

    def test_run_scenario_runtime_backend_probe_set_fails_when_probe_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for relative in (
                "artifacts/scenario_runtime_backend_real_awsim_tracking_ready_probe/scenario_runtime_backend_workflow_report_v0.json",
                "artifacts/scenario_runtime_backend_real_awsim_probe_v14/scenario_runtime_backend_workflow_report_v0.json",
                "artifacts/scenario_runtime_backend_real_awsim_degraded_runtime_probe/scenario_runtime_backend_workflow_report_v0.json",
            ):
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("{}", encoding="utf-8")

            def _fake_probe(**kwargs):
                out_root = Path(kwargs["out_root"])
                probe_id = kwargs["probe_id"]
                report_path = out_root / "scenario_runtime_backend_probe_report_v0.json"
                markdown_path = out_root / "scenario_runtime_backend_probe_report_v0.md"
                out_root.mkdir(parents=True, exist_ok=True)
                status = "FAIL" if probe_id == "semantic_recovery_ready" else "PASS"
                report = {
                    "probe_id": probe_id,
                    "consumer_profile_id": kwargs["consumer_profile_id"],
                    "status": status,
                    "summary": {
                        "runtime_status": kwargs["expect_runtime_status"],
                        "autoware_pipeline_status": kwargs["expect_autoware_status"],
                        "semantic_topic_recovered": False,
                    },
                    "evaluation": {
                        "failure_codes": ["AUTOWARE_STATUS_MISMATCH"] if status == "FAIL" else []
                    },
                }
                report_path.write_text(json.dumps(report), encoding="utf-8")
                markdown_path.write_text("# ok\n", encoding="utf-8")
                return {
                    "report_path": report_path,
                    "markdown_path": markdown_path,
                    "report": report,
                    "rebridge_result": {
                        "workflow_report": {
                            "status_summary": {
                                "backend_runtime_strategy": "linux_handoff_packaged_runtime",
                                "backend_runtime_strategy_source": "setup_summary.runtime_strategy",
                                "backend_runtime_preferred_runtime_source": "packaged_runtime",
                                "backend_runtime_strategy_reason_codes": [
                                    "HOST_INCOMPATIBLE_PACKAGED_RUNTIME"
                                ],
                                "backend_runtime_recommended_command": "python3 scripts/run_renderer_backend_workflow.py --backend awsim --dry-run",
                                "backend_runtime_selected_path": "/tmp/AWSIM-Demo.x86_64",
                                "backend_runtime_docker_storage_status": "healthy",
                            },
                            "rebridge": {
                                "comparison": {
                                    "source_runtime_status": "DEGRADED",
                                    "source_autoware_pipeline_status": "DEGRADED",
                                    "source_missing_required_topics": [],
                                    "refreshed_missing_required_topics": [],
                                    "recovered_required_topics": [],
                                }
                            }
                        }
                    },
                }

            with patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_probe_set.run_scenario_runtime_backend_probe",
                side_effect=_fake_probe,
            ):
                result = run_scenario_runtime_backend_probe_set(
                    out_root=root / "probe_set",
                    probe_set_id=DEFAULT_SCENARIO_RUNTIME_BACKEND_PROBE_SET_ID,
                    repo_root=root,
                )

            report = result["report"]
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["fail_count"], 1)
            self.assertEqual(report["failed_probe_ids"], ["semantic_recovery_ready"])
            self.assertEqual(
                report["blocking_reason_counts"],
                {
                    "AUTOWARE_STATUS_MISMATCH": 1,
                    "HOST_INCOMPATIBLE_PACKAGED_RUNTIME": 3,
                },
            )
            self.assertEqual(
                report["blocking_reason_probe_ids"]["AUTOWARE_STATUS_MISMATCH"],
                ["semantic_recovery_ready"],
            )
            self.assertEqual(
                report["recommended_next_command"],
                "python3 scripts/run_renderer_backend_workflow.py --backend awsim --dry-run",
            )

    def test_probe_set_script_bootstraps_src_path(self) -> None:
        script_path = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "run_scenario_runtime_backend_probe_set.py"
        )
        completed = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("named set of compact runtime/backend probes", completed.stdout)


if __name__ == "__main__":
    unittest.main()
