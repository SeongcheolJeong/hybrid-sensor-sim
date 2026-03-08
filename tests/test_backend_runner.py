from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.renderers.backend_runner import (
    execute_and_inspect_backend_runner_request,
    execute_backend_runner_request,
    inspect_backend_runner_request_outputs,
    main,
)


class BackendRunnerTests(unittest.TestCase):
    def test_execute_and_inspect_backend_runner_request_writes_smoke_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_backend = root / "fake_backend.sh"
            fake_backend.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
mkdir -p "${BACKEND_OUTPUT_ROOT}"
printf '{"status":"ok"}\n' > "${BACKEND_OUTPUT_ROOT}/carla_runtime_state.json"
echo "smoke_ok"
""",
                encoding="utf-8",
            )
            fake_backend.chmod(0o755)

            output_root = root / "backend_outputs" / "carla"
            request_path = root / "backend_runner_request.json"
            request_path.write_text(
                json.dumps(
                    {
                        "backend": "carla",
                        "cwd": str(root),
                        "runner_mode": "direct_backend",
                        "output_root": str(output_root),
                        "command": [str(fake_backend)],
                        "env": {
                            "BACKEND_OUTPUT_ROOT": str(output_root),
                        },
                        "expected_outputs": [
                            {
                                "artifact_key": "carla_runtime_state_json",
                                "path": str(output_root / "carla_runtime_state.json"),
                                "kind": "file",
                                "required": False,
                                "description": "CARLA runtime state summary.",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            result = execute_and_inspect_backend_runner_request(request_path=request_path)

            self.assertTrue(result.success)
            self.assertEqual(result.return_code, 0)
            self.assertIn("backend_runner_smoke_manifest", result.artifacts)
            self.assertIn("backend_runner_execution_manifest", result.artifacts)
            self.assertIn("backend_output_inspection_manifest", result.artifacts)
            smoke_manifest = json.loads(
                result.artifacts["backend_runner_smoke_manifest"].read_text(encoding="utf-8")
            )
            inspection_manifest = json.loads(
                result.artifacts["backend_output_inspection_manifest"].read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(smoke_manifest["status"], "SMOKE_SUCCEEDED")
            self.assertTrue(smoke_manifest["success"])
            self.assertEqual(smoke_manifest["return_code"], 0)
            self.assertEqual(smoke_manifest["execution"]["status"], "EXECUTION_SUCCEEDED")
            self.assertEqual(smoke_manifest["inspection"]["status"], "MATCHED")
            self.assertEqual(
                smoke_manifest["artifacts"]["backend_output_inspection_manifest"],
                str(result.artifacts["backend_output_inspection_manifest"]),
            )
            self.assertEqual(
                smoke_manifest["artifacts"]["backend_runner_execution_manifest"],
                str(result.artifacts["backend_runner_execution_manifest"]),
            )
            self.assertEqual(
                smoke_manifest["output_comparison_report"]["status"],
                "MATCHED",
            )
            self.assertEqual(inspection_manifest["status"], "MATCHED")

    def test_execute_backend_runner_request_writes_execution_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_backend = root / "fake_backend.sh"
            fake_backend.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$(dirname "${BACKEND_OUTPUT_FILE}")"
printf '{"status":"ok"}\n' > "${BACKEND_OUTPUT_FILE}"
mkdir -p "$(dirname "${BACKEND_SENSOR_OUTPUT_FILE}")"
printf '{"sensor":"camera_front"}\n' > "${BACKEND_SENSOR_OUTPUT_FILE}"
echo "runner_ok"
echo "runner_warn" >&2
mkdir -p "${BACKEND_OUTPUT_ROOT}/extras"
printf 'debug\n' > "${BACKEND_OUTPUT_ROOT}/extras/unexpected.log"
""",
                encoding="utf-8",
            )
            fake_backend.chmod(0o755)

            output_file = root / "backend_outputs" / "awsim" / "awsim_runtime_state.json"
            sensor_output_file = (
                root
                / "backend_outputs"
                / "awsim"
                / "sensor_exports"
                / "camera_front"
                / "camera_projection.json"
            )
            sensor_output_candidate_file = (
                root
                / "backend_outputs"
                / "awsim"
                / "sensor_exports"
                / "awsim"
                / "camera_front"
                / "camera"
                / "rgb_frame.json"
            )
            request_path = root / "backend_runner_request.json"
            request_path.write_text(
                json.dumps(
                    {
                        "backend": "awsim",
                        "cwd": str(root),
                        "runner_mode": "direct_backend",
                        "command": [str(fake_backend), "--demo"],
                        "env": {
                            "BACKEND_OUTPUT_ROOT": str(root / "backend_outputs" / "awsim"),
                            "BACKEND_OUTPUT_FILE": str(output_file),
                            "BACKEND_SENSOR_OUTPUT_FILE": str(sensor_output_file),
                        },
                        "expected_outputs": [
                            {
                                "artifact_key": "awsim_runtime_state_json",
                                "path": str(output_file),
                                "kind": "file",
                                "required": False,
                                "description": "AWSIM runtime state summary.",
                            },
                            {
                                "artifact_key": "sensor_output_camera_front",
                                "backend": "awsim",
                                "modality": "camera",
                                "backend_filename": "rgb_frame.json",
                                "output_role": "camera_visible",
                                "artifact_type": "awsim_camera_rgb_json",
                                "sensor_name": "camera",
                                "sensor_id": "camera_front",
                                "data_format": "camera_projection_json",
                                "relative_path": "sensor_exports/camera_front/rgb_frame.json",
                                "path": str(
                                    root
                                    / "backend_outputs"
                                    / "awsim"
                                    / "sensor_exports"
                                    / "camera_front"
                                    / "rgb_frame.json"
                                ),
                                "path_candidates": [
                                    str(
                                        root
                                        / "backend_outputs"
                                        / "awsim"
                                        / "sensor_exports"
                                        / "camera_front"
                                        / "camera_projection.json"
                                    ),
                                    str(
                                        root
                                        / "backend_outputs"
                                        / "awsim"
                                        / "sensor_exports"
                                        / "awsim"
                                        / "camera_front"
                                        / "camera"
                                        / "rgb_frame.json"
                                    ),
                                    str(
                                        root
                                        / "backend_outputs"
                                        / "awsim"
                                        / "sensor_exports"
                                        / "camera_front"
                                        / "rgb_frame.json"
                                    ),
                                    str(
                                        root
                                        / "backend_outputs"
                                        / "awsim"
                                        / "sensor_exports"
                                        / "camera_front"
                                        / "camera"
                                        / "rgb_frame.json"
                                    ),
                                    str(sensor_output_candidate_file),
                                ],
                                "kind": "file",
                                "required": False,
                                "description": "Expected exported payload for sensor camera_front.",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            result = execute_backend_runner_request(request_path=request_path)

            self.assertTrue(result.success)
            self.assertEqual(result.return_code, 0)
            self.assertIn("backend_runner_execution_manifest", result.artifacts)
            self.assertIn("backend_runner_stdout", result.artifacts)
            self.assertIn("backend_runner_stderr", result.artifacts)
            self.assertIn("awsim_runtime_state_json", result.artifacts)
            self.assertIn("sensor_output_camera_front", result.artifacts)
            self.assertIn("backend_sensor_output_summary", result.artifacts)
            self.assertIn("backend_output_smoke_report", result.artifacts)
            self.assertIn("backend_output_comparison_report", result.artifacts)
            manifest = json.loads(
                result.artifacts["backend_runner_execution_manifest"].read_text(encoding="utf-8")
            )
            sensor_output_summary = json.loads(
                result.artifacts["backend_sensor_output_summary"].read_text(encoding="utf-8")
            )
            output_smoke_report = json.loads(
                result.artifacts["backend_output_smoke_report"].read_text(encoding="utf-8")
            )
            output_comparison_report = json.loads(
                result.artifacts["backend_output_comparison_report"].read_text(encoding="utf-8")
            )
            stdout = result.artifacts["backend_runner_stdout"].read_text(encoding="utf-8")
            stderr = result.artifacts["backend_runner_stderr"].read_text(encoding="utf-8")
            self.assertEqual(manifest["status"], "EXECUTION_SUCCEEDED")
            self.assertEqual(manifest["return_code"], 0)
            self.assertEqual(manifest["expected_output_summary"]["found_count"], 2)
            self.assertEqual(manifest["expected_output_summary"]["missing_count"], 0)
            self.assertEqual(
                manifest["expected_output_summary"]["by_output_role"][0]["output_role"],
                "camera_visible",
            )
            self.assertEqual(
                manifest["expected_output_summary"]["by_output_role"][0]["found_count"],
                1,
            )
            self.assertEqual(
                manifest["expected_output_summary"]["by_output_role"][0]["backend_filenames"],
                ["rgb_frame.json"],
            )
            self.assertEqual(
                manifest["expected_output_summary"]["by_artifact_type"][0]["artifact_type"],
                "awsim_camera_rgb_json",
            )
            self.assertEqual(
                manifest["artifacts"]["backend_output_smoke_report"],
                str(result.artifacts["backend_output_smoke_report"]),
            )
            self.assertEqual(
                manifest["artifacts"]["backend_output_comparison_report"],
                str(result.artifacts["backend_output_comparison_report"]),
            )
            self.assertEqual(manifest["output_smoke_report"]["status"], "COMPLETE")
            self.assertEqual(
                manifest["output_comparison_report"]["status"],
                "UNEXPECTED_OUTPUTS",
            )
            self.assertTrue(
                any(
                    entry["artifact_key"] == "sensor_output_camera_front"
                    and entry["exists"]
                    and entry["resolved_path"] == str(sensor_output_file)
                    for entry in manifest["expected_outputs"]
                )
            )
            self.assertEqual(sensor_output_summary["sensor_count"], 1)
            self.assertEqual(sensor_output_summary["found_sensor_count"], 1)
            self.assertEqual(sensor_output_summary["status"], "COMPLETE")
            self.assertEqual(sensor_output_summary["output_role_counts"]["camera_visible"], 1)
            self.assertEqual(
                sensor_output_summary["artifact_type_counts"]["awsim_camera_rgb_json"],
                1,
            )
            self.assertEqual(
                sensor_output_summary["output_roles"][0]["output_role"],
                "camera_visible",
            )
            self.assertEqual(
                sensor_output_summary["artifact_types"][0]["artifact_type"],
                "awsim_camera_rgb_json",
            )
            self.assertEqual(sensor_output_summary["sensors"][0]["modality"], "camera")
            self.assertEqual(
                sensor_output_summary["sensors"][0]["found_output_roles"],
                ["camera_visible"],
            )
            self.assertEqual(
                sensor_output_summary["sensors"][0]["outputs"][0]["resolved_path"],
                str(sensor_output_file),
            )
            self.assertEqual(
                sensor_output_summary["sensors"][0]["outputs"][0]["backend_filename"],
                "rgb_frame.json",
            )
            self.assertEqual(
                sensor_output_summary["sensors"][0]["outputs"][0]["artifact_type"],
                "awsim_camera_rgb_json",
            )
            self.assertEqual(output_smoke_report["status"], "COMPLETE")
            self.assertEqual(output_smoke_report["found_output_count"], 2)
            self.assertEqual(output_smoke_report["missing_output_count"], 0)
            self.assertEqual(output_smoke_report["sensor_status_counts"]["COMPLETE"], 1)
            self.assertEqual(
                output_smoke_report["by_output_role"][0]["status"],
                "COMPLETE",
            )
            self.assertEqual(
                output_smoke_report["by_output_role"][0]["found_sensor_ids"],
                ["camera_front"],
            )
            self.assertEqual(
                output_smoke_report["by_sensor"][0]["status"],
                "COMPLETE",
            )
            self.assertEqual(output_comparison_report["status"], "UNEXPECTED_OUTPUTS")
            self.assertEqual(
                output_comparison_report["mismatch_reasons"],
                ["UNEXPECTED_OUTPUTS_PRESENT", "BACKEND_FILENAME_MISMATCH"],
            )
            self.assertEqual(output_comparison_report["discovered_file_count"], 3)
            self.assertEqual(output_comparison_report["matched_file_count"], 2)
            self.assertEqual(output_comparison_report["unexpected_output_count"], 1)
            self.assertEqual(output_comparison_report["candidate_match_count"], 1)
            self.assertEqual(output_comparison_report["canonical_match_count"], 1)
            self.assertEqual(
                output_comparison_report["unexpected_outputs"][0]["relative_path"],
                "extras/unexpected.log",
            )
            self.assertEqual(output_comparison_report["by_sensor"][0]["status"], "MATCHED")
            self.assertEqual(
                output_comparison_report["by_sensor"][0]["mismatch_reasons"],
                ["BACKEND_FILENAME_MISMATCH"],
            )
            self.assertEqual(
                output_comparison_report["by_sensor"][0]["found_output_roles"],
                ["camera_visible"],
            )
            self.assertEqual(
                output_comparison_report["by_sensor"][0]["missing_output_roles"],
                [],
            )
            self.assertEqual(
                output_comparison_report["by_sensor"][0]["role_diffs"][0]["output_role"],
                "camera_visible",
            )
            self.assertEqual(
                output_comparison_report["by_sensor"][0]["role_diffs"][0]["status"],
                "MATCHED",
            )
            self.assertEqual(
                output_comparison_report["by_sensor"][0]["role_diffs"][0]["mismatch_reasons"],
                ["BACKEND_FILENAME_MISMATCH"],
            )
            self.assertEqual(
                output_comparison_report["by_sensor"][0]["role_diffs"][0]["expected_backend_filenames"],
                ["rgb_frame.json"],
            )
            self.assertEqual(
                output_comparison_report["by_sensor"][0]["role_diffs"][0]["discovered_backend_filenames"],
                ["camera_projection.json"],
            )
            self.assertEqual(
                output_comparison_report["by_sensor"][0]["role_diffs"][0]["found_relative_paths"],
                ["sensor_exports/camera_front/camera_projection.json"],
            )
            self.assertIn("runner_ok", stdout)
            self.assertIn("runner_warn", stderr)

    def test_execute_backend_runner_request_materializes_sidecar_outputs_from_ingestion_profile(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_backend = root / "fake_backend_fail.sh"
            fake_backend.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
echo "backend_failed" >&2
exit 1
""",
                encoding="utf-8",
            )
            fake_backend.chmod(0o755)

            output_root = root / "backend_outputs" / "awsim"
            camera_payload = root / "camera_projection_preview.json"
            lidar_payload = root / "lidar_noisy_preview.xyz"
            radar_payload = root / "radar_targets_preview.json"
            camera_payload.write_text('{"kind":"camera"}\n', encoding="utf-8")
            lidar_payload.write_text("1.0 2.0 3.0\n", encoding="utf-8")
            radar_payload.write_text('{"kind":"radar"}\n', encoding="utf-8")
            ingestion_profile_path = root / "backend_ingestion_profile.json"
            ingestion_profile_path.write_text(
                json.dumps(
                    {
                        "entries": [
                            {
                                "sensor_id": "camera_front",
                                "sensor_name": "camera",
                                "data_format": "camera_projection_json",
                                "payload_artifact": str(camera_payload),
                            },
                            {
                                "sensor_id": "lidar_top",
                                "sensor_name": "lidar",
                                "data_format": "lidar_points_xyz",
                                "payload_artifact": str(lidar_payload),
                            },
                            {
                                "sensor_id": "radar_front",
                                "sensor_name": "radar",
                                "data_format": "radar_targets_json",
                                "payload_artifact": str(radar_payload),
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            request_path = root / "backend_runner_request.json"
            request_path.write_text(
                json.dumps(
                    {
                        "backend": "awsim",
                        "cwd": str(root),
                        "runner_mode": "direct_backend",
                        "output_root": str(output_root),
                        "command": [str(fake_backend)],
                        "env": {
                            "BACKEND_OUTPUT_ROOT": str(output_root),
                        },
                        "artifacts": {
                            "backend_ingestion_profile": str(ingestion_profile_path),
                        },
                        "expected_outputs": [
                            {
                                "artifact_key": "awsim_runtime_state_json",
                                "path": str(output_root / "awsim_runtime_state.json"),
                                "kind": "file",
                                "required": False,
                                "description": "AWSIM runtime state summary.",
                            },
                            {
                                "artifact_key": "sensor_output_camera_front",
                                "backend": "awsim",
                                "sensor_id": "camera_front",
                                "sensor_name": "camera",
                                "modality": "camera",
                                "output_role": "camera_visible",
                                "artifact_type": "awsim_camera_rgb_json",
                                "data_format": "camera_projection_json",
                                "backend_filename": "rgb_frame.json",
                                "relative_path": "sensor_exports/camera_front/rgb_frame.json",
                                "path": str(output_root / "sensor_exports" / "camera_front" / "rgb_frame.json"),
                                "kind": "file",
                                "required": False,
                                "description": "Expected exported payload for sensor camera_front.",
                            },
                            {
                                "artifact_key": "sensor_output_lidar_top",
                                "backend": "awsim",
                                "sensor_id": "lidar_top",
                                "sensor_name": "lidar",
                                "modality": "lidar",
                                "output_role": "lidar_point_cloud",
                                "artifact_type": "awsim_lidar_xyz_point_cloud",
                                "data_format": "lidar_points_xyz",
                                "backend_filename": "point_cloud.xyz",
                                "relative_path": "sensor_exports/lidar_top/point_cloud.xyz",
                                "path": str(output_root / "sensor_exports" / "lidar_top" / "point_cloud.xyz"),
                                "kind": "file",
                                "required": False,
                                "description": "Expected exported payload for sensor lidar_top.",
                            },
                            {
                                "artifact_key": "sensor_output_radar_front",
                                "backend": "awsim",
                                "sensor_id": "radar_front",
                                "sensor_name": "radar",
                                "modality": "radar",
                                "output_role": "radar_detections",
                                "artifact_type": "awsim_radar_detections_json",
                                "data_format": "radar_targets_json",
                                "backend_filename": "targets.json",
                                "relative_path": "sensor_exports/radar_front/targets.json",
                                "path": str(output_root / "sensor_exports" / "radar_front" / "targets.json"),
                                "kind": "file",
                                "required": False,
                                "description": "Expected exported payload for sensor radar_front.",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            result = execute_backend_runner_request(request_path=request_path)

            self.assertFalse(result.success)
            self.assertEqual(result.return_code, 1)
            self.assertIn("backend_sidecar_materialization_report", result.artifacts)
            manifest = json.loads(
                result.artifacts["backend_runner_execution_manifest"].read_text(encoding="utf-8")
            )
            sidecar_report = json.loads(
                result.artifacts["backend_sidecar_materialization_report"].read_text(
                    encoding="utf-8"
                )
            )
            comparison_report = json.loads(
                result.artifacts["backend_output_comparison_report"].read_text(encoding="utf-8")
            )
            smoke_report = json.loads(
                result.artifacts["backend_output_smoke_report"].read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["status"], "EXECUTION_FAILED")
            self.assertEqual(manifest["return_code"], 1)
            self.assertEqual(manifest["expected_output_summary"]["found_count"], 4)
            self.assertEqual(manifest["expected_output_summary"]["missing_count"], 0)
            self.assertEqual(sidecar_report["status"], "MATERIALIZED")
            self.assertEqual(sidecar_report["materialized_output_count"], 3)
            self.assertTrue(sidecar_report["runtime_state_materialized"])
            self.assertEqual(comparison_report["status"], "MATCHED")
            self.assertEqual(comparison_report["mismatch_reasons"], [])
            self.assertEqual(smoke_report["status"], "COMPLETE")
            self.assertEqual(smoke_report["missing_output_count"], 0)
            self.assertTrue(
                (output_root / "sensor_exports" / "camera_front" / "rgb_frame.json").is_file()
            )
            self.assertTrue(
                (output_root / "sensor_exports" / "lidar_top" / "point_cloud.xyz").is_file()
            )
            self.assertTrue(
                (output_root / "sensor_exports" / "radar_front" / "targets.json").is_file()
            )
            self.assertTrue((output_root / "awsim_runtime_state.json").is_file())

    def test_execute_backend_runner_request_rejects_invalid_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            request_path = root / "backend_runner_request.json"
            request_path.write_text(
                json.dumps(
                    {
                        "backend": "carla",
                        "cwd": str(root),
                        "runner_mode": "direct_backend",
                        "command": "--invalid",
                    }
                ),
                encoding="utf-8",
            )

            result = execute_backend_runner_request(request_path=request_path)

            self.assertFalse(result.success)
            self.assertIsNone(result.return_code)
            manifest = json.loads(
                result.artifacts["backend_runner_execution_manifest"].read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["status"], "REQUEST_ERROR")
            self.assertIn("non-empty list", manifest["message"])

    def test_backend_runner_main_executes_request(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_backend = root / "fake_backend.sh"
            fake_backend.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
echo "cli_ok"
""",
                encoding="utf-8",
            )
            fake_backend.chmod(0o755)

            request_path = root / "backend_runner_request.json"
            request_path.write_text(
                json.dumps(
                    {
                        "backend": "awsim",
                        "cwd": str(root),
                        "runner_mode": "direct_backend",
                        "command": [str(fake_backend)],
                    }
                ),
                encoding="utf-8",
            )
            output_dir = root / "runner_out"

            exit_code = main([str(request_path), "--output-dir", str(output_dir)])

            self.assertEqual(exit_code, 0)
            manifest = json.loads(
                (output_dir / "backend_runner_execution_manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(manifest["status"], "EXECUTION_SUCCEEDED")

    def test_inspect_backend_runner_request_outputs_compare_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output_root = root / "backend_outputs" / "carla"
            output_file = output_root / "carla_runtime_state.json"
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_text('{"status":"ok"}', encoding="utf-8")
            request_path = root / "backend_runner_request.json"
            request_path.write_text(
                json.dumps(
                    {
                        "backend": "carla",
                        "output_root": str(output_root),
                        "expected_outputs": [
                            {
                                "artifact_key": "carla_runtime_state_json",
                                "path": str(output_file),
                                "kind": "file",
                                "required": False,
                                "description": "CARLA runtime state summary.",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            result = inspect_backend_runner_request_outputs(request_path=request_path)

            self.assertTrue(result.success)
            self.assertEqual(result.return_code, 0)
            self.assertIn("backend_output_inspection_manifest", result.artifacts)
            self.assertIn("backend_output_smoke_report", result.artifacts)
            self.assertIn("backend_output_comparison_report", result.artifacts)
            inspection_manifest = json.loads(
                result.artifacts["backend_output_inspection_manifest"].read_text(encoding="utf-8")
            )
            self.assertEqual(inspection_manifest["status"], "MATCHED")
            self.assertTrue(inspection_manifest["success"])
            self.assertEqual(inspection_manifest["return_code"], 0)
            self.assertEqual(
                inspection_manifest["artifacts"]["backend_output_comparison_report"],
                str(result.artifacts["backend_output_comparison_report"]),
            )

    def test_backend_runner_main_compare_only_returns_nonzero_for_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output_root = root / "backend_outputs" / "awsim"
            request_path = root / "backend_runner_request.json"
            request_path.write_text(
                json.dumps(
                    {
                        "backend": "awsim",
                        "output_root": str(output_root),
                        "expected_outputs": [
                            {
                                "artifact_key": "awsim_runtime_state_json",
                                "path": str(output_root / "awsim_runtime_state.json"),
                                "kind": "file",
                                "required": False,
                                "description": "AWSIM runtime state summary.",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            output_dir = root / "inspect_out"

            exit_code = main(
                ["--compare-only", str(request_path), "--output-dir", str(output_dir)]
            )

            self.assertEqual(exit_code, 2)
            inspection_manifest = json.loads(
                (output_dir / "backend_output_inspection_manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(inspection_manifest["status"], "UNOBSERVED")
            self.assertFalse(inspection_manifest["success"])
            self.assertFalse((output_dir / "backend_runner_stdout.log").exists())

    def test_backend_runner_main_execute_and_inspect_uses_runtime_state_sidecar(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_backend = root / "fake_backend.sh"
            fake_backend.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
echo "audit_only"
""",
                encoding="utf-8",
            )
            fake_backend.chmod(0o755)

            output_root = root / "backend_outputs" / "awsim"
            request_path = root / "backend_runner_request.json"
            request_path.write_text(
                json.dumps(
                    {
                        "backend": "awsim",
                        "cwd": str(root),
                        "runner_mode": "direct_backend",
                        "output_root": str(output_root),
                        "command": [str(fake_backend)],
                        "expected_outputs": [
                            {
                                "artifact_key": "awsim_runtime_state_json",
                                "path": str(output_root / "awsim_runtime_state.json"),
                                "kind": "file",
                                "required": False,
                                "description": "AWSIM runtime state summary.",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            output_dir = root / "smoke_out"

            exit_code = main(
                ["--execute-and-inspect", str(request_path), "--output-dir", str(output_dir)]
            )

            self.assertEqual(exit_code, 0)
            smoke_manifest = json.loads(
                (output_dir / "backend_runner_smoke_manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            inspection_manifest = json.loads(
                (output_dir / "backend_output_inspection_manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(smoke_manifest["status"], "SMOKE_SUCCEEDED")
            self.assertTrue(smoke_manifest["success"])
            self.assertEqual(smoke_manifest["execution"]["status"], "EXECUTION_SUCCEEDED")
            self.assertEqual(smoke_manifest["inspection"]["status"], "MATCHED")
            self.assertTrue(inspection_manifest["success"])
            self.assertEqual(inspection_manifest["status"], "MATCHED")


if __name__ == "__main__":
    unittest.main()
