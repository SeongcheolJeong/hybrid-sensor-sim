from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.autoware_pipeline_bridge import run_autoware_pipeline_bridge


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


class AutowarePipelineBridgeTests(unittest.TestCase):
    def _write_backend_workflow_fixture(self, root: Path, *, include_lidar: bool) -> Path:
        output_root = root / "backend_outputs"
        output_root.mkdir(parents=True, exist_ok=True)
        camera_output = output_root / "sensor_exports" / "cam_front" / "rgb_frame.json"
        camera_output.parent.mkdir(parents=True, exist_ok=True)
        camera_output.write_text("{}", encoding="utf-8")
        lidar_output = output_root / "sensor_exports" / "lidar_top" / "points.json"
        if include_lidar:
            lidar_output.parent.mkdir(parents=True, exist_ok=True)
            lidar_output.write_text("{}", encoding="utf-8")

        playback_contract_path = root / "renderer_playback_contract.json"
        _write_json(
            playback_contract_path,
            {
                "renderer_sensor_mounts": [
                    {
                        "sensor_id": "cam_front",
                        "sensor_type": "camera",
                        "enabled": True,
                        "attach_to_actor_id": "ego",
                        "extrinsics": {"tx": 1.0, "ty": 0.0, "tz": 1.5},
                    },
                    {
                        "sensor_id": "lidar_top",
                        "sensor_type": "lidar",
                        "enabled": True,
                        "attach_to_actor_id": "ego",
                        "extrinsics": {"tx": 0.0, "ty": 0.0, "tz": 2.0},
                    },
                ]
            },
        )
        backend_output_spec_path = root / "backend_output_spec.json"
        _write_json(
            backend_output_spec_path,
            {
                "backend": "awsim",
                "output_root": str(output_root.resolve()),
                "expected_outputs_by_sensor": [
                    {
                        "sensor_id": "cam_front",
                        "outputs": [
                            {
                                "output_role": "camera_visible",
                                "artifact_type": "awsim_camera_rgb_json",
                                "data_format": "camera_projection_json",
                            }
                        ],
                    },
                    {
                        "sensor_id": "lidar_top",
                        "outputs": [
                            {
                                "output_role": "lidar_point_cloud",
                                "artifact_type": "awsim_lidar_json_point_cloud",
                                "data_format": "lidar_points_json",
                            }
                        ],
                    },
                ],
            },
        )
        backend_sensor_output_summary_path = root / "backend_sensor_output_summary.json"
        sensors = [
            {
                "sensor_id": "cam_front",
                "modality": "camera",
                "outputs": [
                    {
                        "output_role": "camera_visible",
                        "artifact_type": "awsim_camera_rgb_json",
                        "data_format": "camera_projection_json",
                        "artifact_key": "camera_projection_json",
                        "resolved_path": str(camera_output.resolve()),
                        "exists": True,
                    }
                ],
            }
        ]
        if include_lidar:
            sensors.append(
                {
                    "sensor_id": "lidar_top",
                    "modality": "lidar",
                    "outputs": [
                        {
                            "output_role": "lidar_point_cloud",
                            "artifact_type": "awsim_lidar_json_point_cloud",
                            "data_format": "lidar_points_json",
                            "artifact_key": "lidar_points_json",
                            "resolved_path": str(lidar_output.resolve()),
                            "exists": True,
                        }
                    ],
                }
            )
        _write_json(
            backend_sensor_output_summary_path,
            {
                "backend": "awsim",
                "sensors": sensors,
            },
        )
        smoke_summary_path = root / "renderer_backend_smoke_summary.json"
        _write_json(
            smoke_summary_path,
            {
                "backend": "awsim",
                "output_comparison": {"status": "MATCHED", "mismatch_reasons": []},
                "output_smoke_report": {"status": "COMPLETE", "coverage_ratio": 1.0},
                "artifacts": {
                    "renderer_playback_contract": str(playback_contract_path.resolve()),
                    "backend_output_spec": str(backend_output_spec_path.resolve()),
                    "backend_sensor_output_summary": str(backend_sensor_output_summary_path.resolve()),
                },
            },
        )
        backend_workflow_report_path = root / "scenario_backend_smoke_workflow_report_v0.json"
        _write_json(
            backend_workflow_report_path,
            {
                "scenario_backend_smoke_workflow_report_schema_version": "scenario_backend_smoke_workflow_report_v0",
                "backend": "awsim",
                "selection": {"variant_id": "var_001", "logical_scenario_id": "scn_001"},
                "bridge": {"scenario_id": "SCN_001", "source_payload_kind": "scenario_definition_v0"},
                "smoke": {"summary_path": str(smoke_summary_path.resolve())},
                "artifacts": {"smoke_scenario_path": str((root / "smoke_scenario.json").resolve())},
            },
        )
        return backend_workflow_report_path

    def test_bridge_builds_autoware_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            backend_report_path = self._write_backend_workflow_fixture(root, include_lidar=True)
            result = run_autoware_pipeline_bridge(
                backend_smoke_workflow_report_path=str(backend_report_path),
                runtime_backend_workflow_report_path="",
                out_root=root / "autoware_bundle",
                strict=False,
            )
            report = result["report"]
            self.assertEqual(report["status"], "READY")
            self.assertEqual(report["available_sensor_count"], 2)
            self.assertEqual(report["missing_required_sensor_count"], 0)
            self.assertIn("/sensing/camera/cam_front/image_raw", report["available_topics"])
            self.assertTrue(Path(report["artifacts"]["pipeline_manifest_path"]).is_file())
            self.assertTrue(Path(report["artifacts"]["dataset_manifest_path"]).is_file())

    def test_bridge_strict_mode_fails_for_missing_required_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            backend_report_path = self._write_backend_workflow_fixture(root, include_lidar=False)
            result = run_autoware_pipeline_bridge(
                backend_smoke_workflow_report_path=str(backend_report_path),
                runtime_backend_workflow_report_path="",
                out_root=root / "autoware_bundle",
                strict=True,
            )
            report = result["report"]
            self.assertEqual(report["status"], "FAILED")
            self.assertEqual(report["missing_required_sensor_count"], 1)
            self.assertFalse(report["required_topics_complete"])

    def test_script_bootstraps_src_path(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_autoware_pipeline_bridge.py"
        completed = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("Autoware-facing sensor/data contract bundle", completed.stdout)


if __name__ == "__main__":
    unittest.main()
