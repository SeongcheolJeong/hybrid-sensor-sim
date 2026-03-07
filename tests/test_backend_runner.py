from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.renderers.backend_runner import (
    execute_backend_runner_request,
    main,
)


class BackendRunnerTests(unittest.TestCase):
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
                / "awsim"
                / "camera_front"
                / "camera_projection.json"
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
                                "sensor_name": "camera",
                                "sensor_id": "camera_front",
                                "data_format": "camera_projection_json",
                                "relative_path": "sensor_exports/camera_front/camera_projection.json",
                                "path": str(
                                    root
                                    / "backend_outputs"
                                    / "awsim"
                                    / "sensor_exports"
                                    / "camera_front"
                                    / "camera_projection.json"
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
                                    str(sensor_output_file),
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
            manifest = json.loads(
                result.artifacts["backend_runner_execution_manifest"].read_text(encoding="utf-8")
            )
            sensor_output_summary = json.loads(
                result.artifacts["backend_sensor_output_summary"].read_text(encoding="utf-8")
            )
            stdout = result.artifacts["backend_runner_stdout"].read_text(encoding="utf-8")
            stderr = result.artifacts["backend_runner_stderr"].read_text(encoding="utf-8")
            self.assertEqual(manifest["status"], "EXECUTION_SUCCEEDED")
            self.assertEqual(manifest["return_code"], 0)
            self.assertEqual(manifest["expected_output_summary"]["found_count"], 2)
            self.assertEqual(manifest["expected_output_summary"]["missing_count"], 0)
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
            self.assertEqual(
                sensor_output_summary["sensors"][0]["outputs"][0]["resolved_path"],
                str(sensor_output_file),
            )
            self.assertIn("runner_ok", stdout)
            self.assertIn("runner_warn", stderr)

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
            self.assertEqual(
                (output_dir / "backend_runner_stdout.log").read_text(encoding="utf-8").strip(),
                "cli_ok",
            )


if __name__ == "__main__":
    unittest.main()
