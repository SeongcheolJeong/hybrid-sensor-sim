from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from hybrid_sensor_sim.tools.scenario_runtime_backend_rebridge import (
    SCENARIO_RUNTIME_BACKEND_REBRIDGE_REPORT_SCHEMA_VERSION_V0,
    run_scenario_runtime_backend_rebridge,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


class ScenarioRuntimeBackendRebridgeTests(unittest.TestCase):
    def _write_backend_workflow_fixture(
        self,
        root: Path,
        *,
        include_lidar: bool,
        include_camera_visible: bool = True,
        include_camera_semantic: bool = False,
        include_radar: bool = False,
        radar_tracks: bool = False,
        backend_status: str = "HANDOFF_DOCKER_OUTPUT_READY",
        smoke_summary_path_only: bool = True,
    ) -> Path:
        output_root = root / "backend_outputs"
        output_root.mkdir(parents=True, exist_ok=True)
        camera_output = output_root / "sensor_exports" / "cam_front" / "rgb_frame.json"
        camera_semantic_output = (
            output_root / "sensor_exports" / "cam_front" / "semantic_frame.json"
        )
        if include_camera_visible:
            camera_output.parent.mkdir(parents=True, exist_ok=True)
            camera_output.write_text("{}", encoding="utf-8")
        if include_camera_semantic:
            camera_semantic_output.parent.mkdir(parents=True, exist_ok=True)
            camera_semantic_output.write_text("{}", encoding="utf-8")
        lidar_output = output_root / "sensor_exports" / "lidar_top" / "points.json"
        if include_lidar:
            lidar_output.parent.mkdir(parents=True, exist_ok=True)
            lidar_output.write_text("{}", encoding="utf-8")
        radar_detection_output = (
            output_root / "sensor_exports" / "radar_front" / "detections.json"
        )
        radar_tracks_output = output_root / "sensor_exports" / "radar_front" / "tracks.json"
        if include_radar:
            radar_detection_output.parent.mkdir(parents=True, exist_ok=True)
            radar_detection_output.write_text("{}", encoding="utf-8")
            if radar_tracks:
                radar_tracks_output.write_text("{}", encoding="utf-8")

        playback_contract_path = root / "renderer_playback_contract.json"
        sensor_mounts: list[dict[str, object]] = [
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
        if include_radar:
            sensor_mounts.append(
                {
                    "sensor_id": "radar_front",
                    "sensor_type": "radar",
                    "enabled": True,
                    "attach_to_actor_id": "ego",
                    "extrinsics": {"tx": 1.2, "ty": 0.0, "tz": 0.9},
                }
            )
        _write_json(playback_contract_path, {"renderer_sensor_mounts": sensor_mounts})
        smoke_input_config_path = root / "smoke_input_config.json"
        _write_json(
            smoke_input_config_path,
            {
                "scenario_path": str((root / "smoke_scenario.json").resolve()),
                "output_dir": str((root / "smoke_run").resolve()),
                "options": {
                    "camera_projection_enabled": bool(include_camera_visible),
                    "camera_sensor_type": (
                        "SEMANTIC_SEGMENTATION" if include_camera_semantic else "VISIBLE"
                    ),
                    "lidar_postprocess_enabled": bool(include_lidar),
                    "radar_postprocess_enabled": bool(include_radar),
                },
            },
        )

        backend_output_spec_path = root / "backend_output_spec.json"
        expected_outputs_by_sensor: list[dict[str, object]] = [
            {"sensor_id": "cam_front", "outputs": []},
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
        ]
        if include_camera_visible:
            expected_outputs_by_sensor[0]["outputs"].append(
                {
                    "output_role": "camera_visible",
                    "artifact_type": "awsim_camera_rgb_json",
                    "data_format": "camera_projection_json",
                }
            )
        if include_camera_semantic:
            expected_outputs_by_sensor[0]["outputs"].append(
                {
                    "output_role": "camera_semantic",
                    "artifact_type": "awsim_camera_semantic_json",
                    "data_format": "camera_semantic_json",
                }
            )
        if include_radar:
            radar_outputs: list[dict[str, object]] = [
                {
                    "output_role": "radar_detections",
                    "artifact_type": "awsim_radar_detections_json",
                    "data_format": "radar_targets_json",
                }
            ]
            if radar_tracks:
                radar_outputs.append(
                    {
                        "output_role": "radar_tracks",
                        "artifact_type": "awsim_radar_tracks_json",
                        "data_format": "radar_tracks_json",
                    }
                )
            expected_outputs_by_sensor.append(
                {"sensor_id": "radar_front", "outputs": radar_outputs}
            )
        _write_json(
            backend_output_spec_path,
            {
                "backend": "awsim",
                "output_root": str(output_root.resolve()),
                "expected_outputs_by_sensor": expected_outputs_by_sensor,
            },
        )

        sensor_summary_path = root / "backend_sensor_output_summary.json"
        sensors: list[dict[str, object]] = [{"sensor_id": "cam_front", "modality": "camera", "outputs": []}]
        if include_camera_visible:
            sensors[0]["outputs"].append(
                {
                    "output_role": "camera_visible",
                    "artifact_type": "awsim_camera_rgb_json",
                    "data_format": "camera_projection_json",
                    "artifact_key": "camera_projection_json",
                    "resolved_path": str(camera_output.resolve()),
                    "exists": True,
                    "output_origin": "backend_runtime",
                }
            )
        if include_camera_semantic:
            sensors[0]["outputs"].append(
                {
                    "output_role": "camera_semantic",
                    "artifact_type": "awsim_camera_semantic_json",
                    "data_format": "camera_semantic_json",
                    "artifact_key": "camera_semantic_json",
                    "resolved_path": str(camera_semantic_output.resolve()),
                    "exists": True,
                    "output_origin": "backend_runtime",
                }
            )
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
                            "output_origin": "backend_runtime",
                        }
                    ],
                }
            )
        if include_radar:
            radar_outputs = [
                {
                    "output_role": "radar_detections",
                    "artifact_type": "awsim_radar_detections_json",
                    "data_format": "radar_targets_json",
                    "artifact_key": "radar_targets_json",
                    "resolved_path": str(radar_detection_output.resolve()),
                    "exists": True,
                    "output_origin": "backend_runtime",
                }
            ]
            if radar_tracks:
                radar_outputs.append(
                    {
                        "output_role": "radar_tracks",
                        "artifact_type": "awsim_radar_tracks_json",
                        "data_format": "radar_tracks_json",
                        "artifact_key": "radar_tracks_json",
                        "resolved_path": str(radar_tracks_output.resolve()),
                        "exists": True,
                        "output_origin": "backend_runtime",
                    }
                )
            sensors.append(
                {"sensor_id": "radar_front", "modality": "radar", "outputs": radar_outputs}
            )
        _write_json(sensor_summary_path, {"backend": "awsim", "sensors": sensors})

        smoke_summary_path = root / "renderer_backend_smoke_summary.json"
        _write_json(
            smoke_summary_path,
            {
                "backend": "awsim",
                "success": True,
                "run": {"status": "EXECUTION_SUCCEEDED", "failure_reason": None},
                "runner_smoke": {"status": "SMOKE_SUCCEEDED", "return_code": 0},
                "output_inspection": {"status": "MATCHED"},
                "output_smoke_report": {
                    "status": "COMPLETE",
                    "coverage_ratio": 1.0,
                    "output_origin_status": "BACKEND_RUNTIME_ONLY",
                    "output_origin_counts": {
                        "backend_runtime": 2 + int(include_lidar) + int(include_radar) + int(radar_tracks),
                        "sidecar_materialized": 0,
                        "missing": 0,
                    },
                    "output_origin_reasons": [],
                },
                "output_comparison": {
                    "status": "MATCHED",
                    "mismatch_reasons": [],
                    "unexpected_output_count": 0,
                    "output_origin_status": "BACKEND_RUNTIME_ONLY",
                    "output_origin_counts": {
                        "backend_runtime": 2 + int(include_lidar) + int(include_radar) + int(radar_tracks),
                        "sidecar_materialized": 0,
                        "missing": 0,
                    },
                },
                "sidecar_materialization": {
                    "status": "BACKEND_RUNTIME_ONLY",
                    "materialized_output_count": 0,
                },
                "artifacts": {
                    "renderer_playback_contract": str(playback_contract_path.resolve()),
                    "backend_output_spec": str(backend_output_spec_path.resolve()),
                    "backend_sensor_output_summary": str(sensor_summary_path.resolve()),
                },
            },
        )
        backend_workflow_report_path = root / "scenario_backend_smoke_workflow_report_v0.json"
        smoke_payload: dict[str, object] = {"summary_path": str(smoke_summary_path.resolve())}
        if not smoke_summary_path_only:
            smoke_payload["summary"] = {
                "output_smoke_status": "COMPLETE",
                "output_comparison_status": "MATCHED",
                "output_origin_status": "BACKEND_RUNTIME_ONLY",
            }
        _write_json(
            backend_workflow_report_path,
            {
                "scenario_backend_smoke_workflow_report_schema_version": "scenario_backend_smoke_workflow_report_v0",
                "backend": "awsim",
                "status": backend_status,
                "selection": {"variant_id": "var_001", "logical_scenario_id": "scn_001"},
                "runtime_selection": {
                    "backend_bin": "/tmp/AWSIM-Demo.x86_64",
                    "renderer_map": "SampleMap",
                    "setup_summary_path": "/tmp/renderer_backend_local_setup.json",
                    "backend_workflow_summary_path": None,
                },
                "bridge": {
                    "scenario_id": "SCN_001",
                    "source_payload_kind": "scenario_definition_v0",
                    "source_payload_path": str((root / "scenario.json").resolve()),
                    "lane_spacing_m": 4.0,
                },
                "smoke": smoke_payload,
                "renderer_backend_workflow": {
                    "status": backend_status,
                    "linux_handoff_ready": True,
                    "blocker_codes": [],
                    "warning_codes": [],
                },
                "autoware": {},
                "artifacts": {
                    "smoke_scenario_path": str((root / "smoke_scenario.json").resolve()),
                    "smoke_input_config_path": str(smoke_input_config_path.resolve()),
                },
            },
        )
        return backend_workflow_report_path

    def _write_batch_workflow_report(self, path: Path) -> Path:
        _write_json(
            path,
            {
                "scenario_batch_workflow_report_schema_version": "scenario_batch_workflow_report_v0",
                "status": "SUCCEEDED",
                "status_summary": {
                    "worst_logical_scenario_row": {"logical_scenario_id": "scn_001"},
                    "gate_failure_codes": [],
                    "status_reason_codes": [],
                },
            },
        )
        return path

    def _write_runtime_workflow_report(
        self,
        path: Path,
        *,
        backend_report_path: Path,
        batch_report_path: Path | None,
        consumer_profile_id: str = "",
    ) -> Path:
        _write_json(
            path,
            {
                "scenario_runtime_backend_workflow_report_schema_version": "scenario_runtime_backend_workflow_report_v0",
                "status": "ATTENTION",
                "backend": "awsim",
                "batch_workflow": {
                    "status": "SUCCEEDED",
                    "status_summary": {
                        "worst_logical_scenario_row": {"logical_scenario_id": "scn_001"},
                        "gate_failure_codes": [],
                        "status_reason_codes": [],
                    },
                },
                "backend_smoke_workflow": {"status": "HANDOFF_DOCKER_OUTPUT_READY"},
                "status_summary": {"autoware_consumer_profile_id": consumer_profile_id},
                "artifacts": {
                    "backend_smoke_workflow_report_path": str(backend_report_path.resolve()),
                    "batch_workflow_report_path": (
                        str(batch_report_path.resolve()) if batch_report_path is not None else None
                    ),
                },
            },
        )
        return path

    def test_rebridge_from_runtime_report_merges_supplemental_autoware_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            primary_backend_report = self._write_backend_workflow_fixture(
                root / "primary",
                include_lidar=True,
                include_camera_visible=True,
                include_camera_semantic=False,
            )
            supplemental_backend_report = self._write_backend_workflow_fixture(
                root / "supplemental",
                include_lidar=False,
                include_camera_visible=False,
                include_camera_semantic=True,
            )
            batch_report = self._write_batch_workflow_report(
                root / "batch" / "scenario_batch_workflow_report_v0.json"
            )
            runtime_report_path = self._write_runtime_workflow_report(
                root / "runtime" / "scenario_runtime_backend_workflow_report_v0.json",
                backend_report_path=primary_backend_report,
                batch_report_path=batch_report,
                consumer_profile_id="semantic_perception_v0",
            )
            primary_backend_payload = json.loads(
                primary_backend_report.read_text(encoding="utf-8")
            )
            primary_backend_payload["autoware"] = {
                "supplemental_backend_smoke_workflow_report_paths": [
                    str(supplemental_backend_report.resolve())
                ],
                "supplemental_semantic_requested": True,
                "supplemental_semantic_status": "HANDOFF_DOCKER_OUTPUT_READY",
                "supplemental_semantic_report_path": str(supplemental_backend_report.resolve()),
            }
            _write_json(primary_backend_report, primary_backend_payload)

            result = run_scenario_runtime_backend_rebridge(
                runtime_backend_workflow_report_path=str(runtime_report_path),
                backend_smoke_workflow_report_path="",
                batch_workflow_report_path="",
                supplemental_backend_smoke_workflow_report_paths=[],
                out_root=root / "rebridge",
                autoware_base_frame="base_link",
                autoware_consumer_profile="",
                autoware_strict=False,
            )

            report = result["workflow_report"]
            self.assertEqual(
                report["scenario_runtime_backend_rebridge_report_schema_version"],
                SCENARIO_RUNTIME_BACKEND_REBRIDGE_REPORT_SCHEMA_VERSION_V0,
            )
            self.assertEqual(report["status"], "SUCCEEDED")
            self.assertEqual(
                report["status_summary"]["autoware_pipeline_status"],
                "READY",
            )
            self.assertEqual(
                report["rebridge"]["comparison"]["source_runtime_status"],
                "ATTENTION",
            )
            self.assertEqual(
                report["rebridge"]["comparison"]["refreshed_runtime_status"],
                "SUCCEEDED",
            )
            self.assertTrue(report["rebridge"]["comparison"]["status_changed"])
            self.assertEqual(report["status_summary"]["autoware_merged_report_count"], 2)
            self.assertEqual(
                report["rebridge"]["comparison"]["refreshed_autoware_merged_report_count"],
                2,
            )
            self.assertTrue(
                report["rebridge"]["comparison"]["merged_report_count_changed"]
            )
            self.assertEqual(
                report["status_summary"]["autoware_missing_required_sensor_count"],
                0,
            )
            self.assertIn(
                "/sensing/camera/cam_front/semantic/image_raw",
                report["status_summary"]["autoware_available_topics"],
            )
            self.assertTrue(
                Path(report["artifacts"]["autoware_report_path"]).is_file()
            )
            self.assertEqual(
                report["rebridge"]["source_runtime_backend_workflow_report_path"],
                str(runtime_report_path.resolve()),
            )

    def test_rebridge_from_backend_report_uses_synthetic_batch_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            backend_report = self._write_backend_workflow_fixture(
                root,
                include_lidar=True,
                include_camera_visible=True,
                include_camera_semantic=False,
                backend_status="SMOKE_SUCCEEDED",
            )
            result = run_scenario_runtime_backend_rebridge(
                runtime_backend_workflow_report_path="",
                backend_smoke_workflow_report_path=str(backend_report),
                batch_workflow_report_path="",
                supplemental_backend_smoke_workflow_report_paths=[],
                out_root=root / "rebridge",
                autoware_base_frame="base_link",
                autoware_consumer_profile="",
                autoware_strict=False,
            )
            report = result["workflow_report"]
            self.assertEqual(report["status"], "SUCCEEDED")
            self.assertEqual(report["batch_workflow"]["status"], "SUCCEEDED")
            self.assertIsNone(report["rebridge"]["comparison"]["source_runtime_status"])
            self.assertTrue(report["rebridge"]["comparison"]["status_changed"])
            self.assertIsNone(report["artifacts"]["source_runtime_backend_workflow_report_path"])
            self.assertIsNone(report["rebridge"]["source_batch_workflow_report_path"])
            self.assertEqual(
                report["status_summary"]["autoware_pipeline_status"],
                "READY",
            )

    def test_rebridge_can_auto_run_semantic_supplemental_from_source_backend_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            primary_backend_report = self._write_backend_workflow_fixture(
                root / "primary",
                include_lidar=True,
                include_camera_visible=True,
                include_camera_semantic=False,
            )
            supplemental_backend_report = self._write_backend_workflow_fixture(
                root / "supplemental",
                include_lidar=False,
                include_camera_visible=False,
                include_camera_semantic=True,
            )
            batch_report = self._write_batch_workflow_report(
                root / "batch" / "scenario_batch_workflow_report_v0.json"
            )
            runtime_report_path = self._write_runtime_workflow_report(
                root / "runtime" / "scenario_runtime_backend_workflow_report_v0.json",
                backend_report_path=primary_backend_report,
                batch_report_path=batch_report,
                consumer_profile_id="semantic_perception_v0",
            )
            primary_backend_payload = json.loads(
                primary_backend_report.read_text(encoding="utf-8")
            )
            primary_backend_payload["selection"].update(
                {
                    "report_kind": "batch_workflow_report",
                    "source_report_path": str(batch_report.resolve()),
                    "selection_strategy": "worst_logical_scenario",
                    "variant_id": "scn_001",
                }
            )
            _write_json(primary_backend_report, primary_backend_payload)

            with patch(
                "hybrid_sensor_sim.tools.scenario_runtime_backend_rebridge.run_scenario_backend_smoke_workflow",
                return_value={
                    "workflow_report_path": supplemental_backend_report,
                    "workflow_report": json.loads(
                        supplemental_backend_report.read_text(encoding="utf-8")
                    ),
                },
            ) as mocked_supplemental:
                result = run_scenario_runtime_backend_rebridge(
                    runtime_backend_workflow_report_path=str(runtime_report_path),
                    backend_smoke_workflow_report_path="",
                    batch_workflow_report_path="",
                    supplemental_backend_smoke_workflow_report_paths=[],
                    out_root=root / "rebridge_auto_semantic",
                    autoware_base_frame="base_link",
                    autoware_consumer_profile="",
                    autoware_strict=False,
                )

            mocked_supplemental.assert_called_once()
            supplemental_kwargs = mocked_supplemental.call_args.kwargs
            self.assertEqual(
                Path(supplemental_kwargs["renderer_backend_workflow_output_root"]).resolve(),
                (root / "rebridge_auto_semantic" / "supplemental_semantic" / "renderer_backend_workflow").resolve(),
            )
            report = result["workflow_report"]
            self.assertEqual(report["status"], "SUCCEEDED")
            self.assertEqual(
                report["status_summary"]["autoware_pipeline_status"],
                "READY",
            )
            self.assertEqual(
                report["status_summary"]["autoware_merged_report_count"],
                2,
            )
            self.assertEqual(
                report["rebridge"]["comparison"]["refreshed_autoware_merged_report_count"],
                2,
            )
            self.assertTrue(
                report["rebridge"]["comparison"]["merged_report_count_changed"]
            )
            self.assertEqual(
                report["backend_smoke_workflow"]["autoware"]["supplemental_semantic_status"],
                "HANDOFF_DOCKER_OUTPUT_READY",
            )
            self.assertEqual(
                report["artifacts"]["supplemental_semantic_backend_smoke_workflow_report_path"],
                str(supplemental_backend_report.resolve()),
            )
            self.assertTrue(
                Path(
                    report["artifacts"]["supplemental_semantic_smoke_config_path"]
                ).is_file()
            )
            self.assertIn(
                "/sensing/camera/cam_front/semantic/image_raw",
                report["status_summary"]["autoware_available_topics"],
            )

    def test_script_help_bootstraps(self) -> None:
        script_path = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "run_scenario_runtime_backend_rebridge.py"
        )
        completed = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        self.assertIn("runtime-backend-workflow-report", completed.stdout)


if __name__ == "__main__":
    unittest.main()
