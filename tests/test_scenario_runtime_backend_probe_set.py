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
                report["runtime_strategy_summary_rows"],
                [
                    {
                        "strategy": "linux_handoff_packaged_runtime",
                        "probe_ids": [
                            "semantic_primary_ready",
                            "semantic_recovery_ready",
                            "tracking_ready",
                        ],
                        "preferred_runtime_source": "packaged_runtime",
                        "recommended_action": "Prepare and execute the linux handoff packaged runtime workflow.",
                    }
                ],
            )
            self.assertEqual(
                report["runtime_strategy_plan_rows"],
                [
                    {
                        "strategy": "linux_handoff_packaged_runtime",
                        "probe_ids": [
                            "semantic_primary_ready",
                            "semantic_recovery_ready",
                            "tracking_ready",
                        ],
                        "preferred_runtime_source": "packaged_runtime",
                        "docker_storage_statuses": ["healthy"],
                        "reason_codes": ["HOST_INCOMPATIBLE_PACKAGED_RUNTIME"],
                        "plan_id": "linux_handoff_packaged_runtime",
                        "plan_summary": "Use the packaged runtime through the linux handoff workflow.",
                        "plan_steps": [
                            "Confirm the selected packaged runtime path is present and current.",
                            "Generate or refresh the linux handoff bundle for the packaged runtime.",
                            "Execute the linux handoff workflow and rerun the backend smoke path.",
                        ],
                    }
                ],
            )
            self.assertEqual(
                report["primary_runtime_strategy"],
                "linux_handoff_packaged_runtime",
            )
            self.assertEqual(
                report["recommended_runtime_action"],
                "Prepare and execute the linux handoff packaged runtime workflow.",
            )
            self.assertEqual(
                report["primary_runtime_plan_id"],
                "linux_handoff_packaged_runtime",
            )
            self.assertEqual(
                report["recommended_runtime_plan_steps"],
                [
                    "Confirm the selected packaged runtime path is present and current.",
                    "Generate or refresh the linux handoff bundle for the packaged runtime.",
                    "Execute the linux handoff workflow and rerun the backend smoke path.",
                ],
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
                report["blocking_reason_category_counts"],
                {"runtime_environment": 3},
            )
            self.assertEqual(
                report["blocking_reason_category_probe_ids"]["runtime_environment"],
                ["semantic_primary_ready", "semantic_recovery_ready", "tracking_ready"],
            )
            self.assertEqual(
                report["blocking_reason_summary_rows"],
                [
                    {
                        "reason_code": "HOST_INCOMPATIBLE_PACKAGED_RUNTIME",
                        "category": "runtime_environment",
                        "count": 3,
                        "probe_ids": [
                            "semantic_primary_ready",
                            "semantic_recovery_ready",
                            "tracking_ready",
                        ],
                        "recommended_action": "Use the linux handoff packaged runtime path.",
                    }
                ],
            )
            self.assertEqual(
                report["primary_blocking_reason_code"],
                "HOST_INCOMPATIBLE_PACKAGED_RUNTIME",
            )
            self.assertEqual(
                report["primary_blocking_category"],
                "runtime_environment",
            )
            self.assertEqual(
                report["recommended_resolution_focus"],
                "Use the linux handoff packaged runtime path.",
            )
            self.assertEqual(
                report["recommended_resolution_steps"],
                [
                    "Confirm the selected packaged runtime path is present and current.",
                    "Generate or refresh the linux handoff bundle for the packaged runtime.",
                    "Execute the linux handoff workflow and rerun the backend smoke path.",
                    "Prepare and execute the linux handoff packaged runtime workflow.",
                    "Run: python3 scripts/run_renderer_backend_workflow.py --backend awsim --dry-run",
                    "Use the linux handoff packaged runtime path.",
                ],
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
                report["blocking_reason_category_counts"],
                {"consumer_contract": 1, "runtime_environment": 3},
            )
            self.assertEqual(
                report["blocking_reason_category_probe_ids"]["consumer_contract"],
                ["semantic_recovery_ready"],
            )
            self.assertEqual(
                report["primary_runtime_strategy"],
                "linux_handoff_packaged_runtime",
            )
            self.assertEqual(
                report["recommended_runtime_action"],
                "Prepare and execute the linux handoff packaged runtime workflow.",
            )
            self.assertEqual(
                report["primary_runtime_plan_id"],
                "linux_handoff_packaged_runtime",
            )
            self.assertEqual(
                report["recommended_runtime_plan_steps"],
                [
                    "Confirm the selected packaged runtime path is present and current.",
                    "Generate or refresh the linux handoff bundle for the packaged runtime.",
                    "Execute the linux handoff workflow and rerun the backend smoke path.",
                ],
            )
            self.assertEqual(
                report["primary_blocking_reason_code"],
                "HOST_INCOMPATIBLE_PACKAGED_RUNTIME",
            )
            self.assertEqual(
                report["primary_blocking_category"],
                "runtime_environment",
            )
            self.assertEqual(
                report["recommended_resolution_focus"],
                "Use the linux handoff packaged runtime path.",
            )
            self.assertEqual(
                report["recommended_resolution_steps"],
                [
                    "Confirm the selected packaged runtime path is present and current.",
                    "Generate or refresh the linux handoff bundle for the packaged runtime.",
                    "Execute the linux handoff workflow and rerun the backend smoke path.",
                    "Prepare and execute the linux handoff packaged runtime workflow.",
                    "Run: python3 scripts/run_renderer_backend_workflow.py --backend awsim --dry-run",
                    "Use the linux handoff packaged runtime path.",
                    "Inspect missing and recovered Autoware topics for the selected consumer profile.",
                ],
            )
            self.assertEqual(
                report["recommended_next_command"],
                "python3 scripts/run_renderer_backend_workflow.py --backend awsim --dry-run",
            )

    def test_probe_set_builds_docker_storage_repair_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime_report = (
                root
                / "artifacts"
                / "scenario_runtime_backend_real_carla_probe"
                / "scenario_runtime_backend_workflow_report_v0.json"
            )
            runtime_report.parent.mkdir(parents=True, exist_ok=True)
            runtime_report.write_text("{}", encoding="utf-8")

            def _fake_probe(**kwargs):
                out_root = Path(kwargs["out_root"])
                report_path = out_root / "scenario_runtime_backend_probe_report_v0.json"
                markdown_path = out_root / "scenario_runtime_backend_probe_report_v0.md"
                out_root.mkdir(parents=True, exist_ok=True)
                report = {
                    "probe_id": kwargs["probe_id"],
                    "consumer_profile_id": kwargs["consumer_profile_id"],
                    "status": "FAIL",
                    "summary": {
                        "runtime_status": "FAILED",
                        "autoware_pipeline_status": "DEGRADED",
                        "semantic_topic_recovered": False,
                    },
                    "evaluation": {
                        "failure_codes": ["LOCAL_RUNTIME_MISSING"],
                    },
                }
                report_path.write_text(json.dumps(report), encoding="utf-8")
                markdown_path.write_text("# fail\n", encoding="utf-8")
                return {
                    "report_path": report_path,
                    "markdown_path": markdown_path,
                    "report": report,
                    "rebridge_result": {
                        "workflow_report": {
                            "status_summary": {
                                "backend_runtime_strategy": "packaged_runtime_required",
                                "backend_runtime_strategy_source": "setup_summary.runtime_strategy",
                                "backend_runtime_preferred_runtime_source": "packaged",
                                "backend_runtime_strategy_reason_codes": [
                                    "LOCAL_RUNTIME_MISSING",
                                    "DOCKER_STORAGE_CORRUPT",
                                ],
                                "backend_runtime_recommended_command": "python3 scripts/acquire_renderer_backend_package.py --backend carla",
                                "backend_runtime_selected_path": None,
                                "backend_runtime_docker_storage_status": "content_store_corrupt",
                            },
                            "rebridge": {
                                "comparison": {
                                    "source_runtime_status": "FAILED",
                                    "source_autoware_pipeline_status": "DEGRADED",
                                    "source_missing_required_topics": [],
                                    "refreshed_missing_required_topics": [],
                                    "recovered_required_topics": [],
                                }
                            },
                        }
                    },
                }

            with patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_probe_set.run_scenario_runtime_backend_probe",
                side_effect=_fake_probe,
            ), patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_probe_set._default_probe_set_specs",
                return_value={
                    "carla_local_v0": {
                        "probe_set_id": "carla_local_v0",
                        "description": "CARLA local runtime blockers",
                        "probes": [
                            {
                                "probe_id": "carla_runtime_missing",
                                "runtime_backend_workflow_report_path": runtime_report,
                                "consumer_profile_id": "tracking_fusion_v0",
                                "expect_runtime_status": "SUCCEEDED",
                                "expect_autoware_status": "READY",
                            }
                        ],
                    }
                },
            ):
                result = run_scenario_runtime_backend_probe_set(
                    out_root=root / "probe_set",
                    probe_set_id="carla_local_v0",
                    repo_root=root,
                )

            report = result["report"]
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["primary_runtime_strategy"], "packaged_runtime_required")
            self.assertEqual(
                report["primary_runtime_plan_id"],
                "packaged_runtime_required_after_docker_failure",
            )
            self.assertEqual(
                report["recommended_runtime_plan_steps"],
                [
                    "Acquire or locate a packaged runtime for the selected backend.",
                    "Stage the packaged runtime into the local runtime workspace.",
                    "Use the packaged runtime path or linux handoff workflow to rerun smoke.",
                ],
            )
            self.assertEqual(
                report["runtime_strategy_plan_rows"],
                [
                    {
                        "strategy": "packaged_runtime_required",
                        "probe_ids": ["carla_runtime_missing"],
                        "preferred_runtime_source": "packaged",
                        "docker_storage_statuses": ["content_store_corrupt"],
                        "reason_codes": [
                            "DOCKER_STORAGE_CORRUPT",
                            "LOCAL_RUNTIME_MISSING",
                        ],
                        "plan_id": "packaged_runtime_required_after_docker_failure",
                        "plan_summary": "Docker is blocked, so acquire and stage a packaged runtime.",
                        "plan_steps": [
                            "Acquire or locate a packaged runtime for the selected backend.",
                            "Stage the packaged runtime into the local runtime workspace.",
                            "Use the packaged runtime path or linux handoff workflow to rerun smoke.",
                        ],
                    }
                ],
            )
            self.assertEqual(
                report["recommended_resolution_steps"],
                [
                    "Acquire or locate a packaged runtime for the selected backend.",
                    "Stage the packaged runtime into the local runtime workspace.",
                    "Use the packaged runtime path or linux handoff workflow to rerun smoke.",
                    "Acquire and stage a packaged runtime for the selected backend.",
                    "Run: python3 scripts/acquire_renderer_backend_package.py --backend carla",
                    "Fix the runtime environment or switch to the recommended handoff path.",
                    "Repair the local Docker image store or use a packaged runtime handoff path.",
                ],
            )

    def test_builtin_carla_local_probe_set_uses_local_setup_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_setup_summary = (
                root
                / "artifacts"
                / "renderer_backend_local_setup_probe_latest"
                / "renderer_backend_local_setup.json"
            )
            local_setup_summary.parent.mkdir(parents=True, exist_ok=True)
            local_setup_summary.write_text(
                json.dumps(
                    {
                        "runtime_strategy": {
                            "carla": {
                                "strategy": "packaged_runtime_required",
                                "preferred_runtime_source": "packaged",
                                "selected_path": None,
                                "docker_storage_status": "content_store_corrupt",
                                "reason_codes": [
                                    "LOCAL_RUNTIME_MISSING",
                                    "DOCKER_STORAGE_CORRUPT",
                                ],
                                "recommended_command": "python3 scripts/acquire_renderer_backend_package.py --backend carla",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            result = run_scenario_runtime_backend_probe_set(
                out_root=root / "probe_set",
                probe_set_id="carla_local_v0",
                repo_root=root,
            )

            report = result["report"]
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["probe_count"], 1)
            self.assertEqual(report["failed_probe_ids"], ["carla_local_runtime_strategy"])
            self.assertEqual(report["primary_runtime_strategy"], "packaged_runtime_required")
            self.assertEqual(
                report["primary_runtime_plan_id"],
                "packaged_runtime_required_after_docker_failure",
            )
            self.assertEqual(
                report["blocking_reason_counts"],
                {
                    "DOCKER_STORAGE_CORRUPT": 2,
                    "LOCAL_RUNTIME_MISSING": 2,
                },
            )
            self.assertEqual(
                report["blocking_reason_category_counts"],
                {"runtime_environment": 4},
            )
            self.assertEqual(
                report["runtime_strategy_plan_rows"],
                [
                    {
                        "strategy": "packaged_runtime_required",
                        "probe_ids": ["carla_local_runtime_strategy"],
                        "preferred_runtime_source": "packaged",
                        "docker_storage_statuses": ["content_store_corrupt"],
                        "reason_codes": [
                            "DOCKER_STORAGE_CORRUPT",
                            "LOCAL_RUNTIME_MISSING",
                        ],
                        "plan_id": "packaged_runtime_required_after_docker_failure",
                        "plan_summary": "Docker is blocked, so acquire and stage a packaged runtime.",
                        "plan_steps": [
                            "Acquire or locate a packaged runtime for the selected backend.",
                            "Stage the packaged runtime into the local runtime workspace.",
                            "Use the packaged runtime path or linux handoff workflow to rerun smoke.",
                        ],
                    }
                ],
            )
            self.assertEqual(
                report["recommended_next_command"],
                "python3 scripts/acquire_renderer_backend_package.py --backend carla",
            )
            self.assertEqual(
                report["recommended_resolution_focus"],
                "Repair the local Docker image store or use a packaged runtime handoff path.",
            )

    def test_builtin_carla_local_probe_set_prefers_download_space_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_setup_summary = (
                root
                / "artifacts"
                / "renderer_backend_local_setup_probe_latest"
                / "renderer_backend_local_setup.json"
            )
            local_setup_summary.parent.mkdir(parents=True, exist_ok=True)
            local_setup_summary.write_text(
                json.dumps(
                    {
                        "runtime_strategy": {
                            "carla": {
                                "strategy": "packaged_runtime_required",
                                "preferred_runtime_source": "packaged",
                                "selected_path": None,
                                "docker_storage_status": "healthy",
                                "reason_codes": [
                                    "LOCAL_RUNTIME_MISSING",
                                    "DOWNLOAD_SPACE_INSUFFICIENT",
                                ],
                                "recommended_command": "python3 scripts/acquire_renderer_backend_package.py --backend carla",
                                "recommended_download_command": "python3 scripts/acquire_renderer_backend_package.py --backend carla --download-dir /Volumes/LargeDisk/backend_downloads/carla",
                                "recommended_download_dir": "/Volumes/LargeDisk/backend_downloads/carla",
                                "recommended_download_dir_ready": False,
                                "recommended_download_dir_available_space_bytes": 123456789,
                                "download_directory_status": "insufficient",
                                "archive_estimated_size_bytes": 15723108218,
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            result = run_scenario_runtime_backend_probe_set(
                out_root=root / "probe_set",
                probe_set_id="carla_local_v0",
                repo_root=root,
            )

            report = result["report"]
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["primary_runtime_strategy"], "packaged_runtime_required")
            self.assertEqual(
                report["primary_runtime_plan_id"],
                "packaged_runtime_required_with_download_space_blocker",
            )
            self.assertEqual(
                report["blocking_reason_category_counts"],
                {"download_environment": 2, "runtime_environment": 2},
            )
            self.assertEqual(
                report["runtime_strategy_plan_rows"],
                [
                    {
                        "strategy": "packaged_runtime_required",
                        "probe_ids": ["carla_local_runtime_strategy"],
                        "preferred_runtime_source": "packaged",
                        "docker_storage_statuses": ["healthy"],
                        "reason_codes": [
                            "DOWNLOAD_SPACE_INSUFFICIENT",
                            "LOCAL_RUNTIME_MISSING",
                        ],
                        "plan_id": "packaged_runtime_required_with_download_space_blocker",
                        "plan_summary": "Acquire a packaged runtime after switching to a directory with enough free space.",
                        "plan_steps": [
                            "Choose or create a download directory with enough free space for the backend archive.",
                            "Re-run the package acquire command with --download-dir set to that directory.",
                            "Stage the packaged runtime into the local runtime workspace and rerun smoke.",
                        ],
                    }
                ],
            )
            self.assertEqual(
                report["recommended_resolution_focus"],
                "Free space or use a larger download directory before acquiring the packaged runtime.",
            )
            self.assertEqual(
                report["recommended_resolution_steps"],
                [
                    "Choose or create a download directory with enough free space for the backend archive.",
                    "Re-run the package acquire command with --download-dir set to that directory.",
                    "Stage the packaged runtime into the local runtime workspace and rerun smoke.",
                    "Acquire and stage a packaged runtime for the selected backend.",
                    "Run: python3 scripts/acquire_renderer_backend_package.py --backend carla --download-dir /Volumes/LargeDisk/backend_downloads/carla",
                    "Free space or use a larger download directory before acquiring the packaged runtime.",
                    "Fix the runtime environment or switch to the recommended handoff path.",
                ],
            )
            self.assertEqual(
                report["recommended_next_command"],
                "python3 scripts/acquire_renderer_backend_package.py --backend carla --download-dir /Volumes/LargeDisk/backend_downloads/carla",
            )

    def test_builtin_hybrid_runtime_readiness_probe_set_combines_awsim_and_carla(self) -> None:
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
            local_setup_summary = (
                root
                / "artifacts"
                / "renderer_backend_local_setup_probe_latest"
                / "renderer_backend_local_setup.json"
            )
            local_setup_summary.parent.mkdir(parents=True, exist_ok=True)
            local_setup_summary.write_text(
                json.dumps(
                    {
                        "runtime_strategy": {
                            "carla": {
                                "strategy": "packaged_runtime_required",
                                "preferred_runtime_source": "packaged",
                                "selected_path": None,
                                "docker_storage_status": "content_store_corrupt",
                                "reason_codes": [
                                    "LOCAL_RUNTIME_MISSING",
                                    "DOCKER_STORAGE_CORRUPT",
                                ],
                                "recommended_command": "python3 scripts/acquire_renderer_backend_package.py --backend carla",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

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
                            },
                        }
                    },
                }

            with patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_probe_set.run_scenario_runtime_backend_probe",
                side_effect=_fake_probe,
            ):
                result = run_scenario_runtime_backend_probe_set(
                    out_root=root / "probe_set",
                    probe_set_id="hybrid_runtime_readiness_v0",
                    repo_root=root,
                )

            report = result["report"]
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["probe_count"], 4)
            self.assertEqual(report["pass_count"], 3)
            self.assertEqual(report["fail_count"], 1)
            self.assertEqual(
                report["passed_probe_ids"],
                ["semantic_primary_ready", "semantic_recovery_ready", "tracking_ready"],
            )
            self.assertEqual(report["failed_probe_ids"], ["carla_local_runtime_strategy"])
            self.assertEqual(
                report["runtime_strategy_counts"],
                {
                    "linux_handoff_packaged_runtime": 3,
                    "packaged_runtime_required": 1,
                },
            )
            self.assertEqual(
                report["runtime_strategy_probe_ids"]["packaged_runtime_required"],
                ["carla_local_runtime_strategy"],
            )
            self.assertEqual(
                report["recommended_next_command"],
                "python3 scripts/acquire_renderer_backend_package.py --backend carla",
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
