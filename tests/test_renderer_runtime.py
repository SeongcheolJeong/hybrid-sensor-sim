from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from hybrid_sensor_sim.backends.helios_adapter import HeliosAdapter
from hybrid_sensor_sim.backends.native_physics import NativePhysicsBackend
from hybrid_sensor_sim.orchestrator import HybridOrchestrator
from hybrid_sensor_sim.types import BackendMode, SensorSimRequest


def _write_fake_helios_script(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
out=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--output" ]]; then
    out="$2"
    shift 2
  else
    shift
  fi
done
mkdir -p "${out}/demo/2026-01-01_00-00-00"
rootdir="${out}/demo/2026-01-01_00-00-00"
echo "Output directory: \\"${rootdir}\\""
cat > "${rootdir}/scan_points.xyz" <<EOF
10.0 0.0 0.0
12.0 0.0 0.0
14.0 0.0 0.0
EOF
cat > "${rootdir}/scan_trajectory.txt" <<EOF
0.0 0.0 0.0 0.0 0.0 0.0 0.0
2.0 0.0 0.0 1.0 0.0 0.0 0.0
4.0 0.0 0.0 2.0 0.0 0.0 0.0
EOF
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


class RendererRuntimeTests(unittest.TestCase):
    def test_renderer_runtime_plan_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "carla",
                    "renderer_execute": False,
                    "renderer_command": ["echo", "renderer_plan", "{contract}"],
                    "renderer_camera_sensor_id": "cam_front",
                    "renderer_lidar_sensor_id": "lidar_roof",
                    "renderer_radar_sensor_id": "radar_bumper",
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)
            self.assertIn("renderer_playback_contract", result.artifacts)
            self.assertIn("renderer_execution_plan", result.artifacts)
            self.assertIn("backend_run_manifest", result.artifacts)
            self.assertIn("backend_runner_request", result.artifacts)
            self.assertIn("backend_direct_run_command", result.artifacts)
            self.assertIn("backend_output_spec", result.artifacts)
            self.assertIn("renderer_pipeline_summary", result.artifacts)
            self.assertEqual(result.metrics.get("renderer_runtime_planned"), 1.0)
            self.assertEqual(result.metrics.get("renderer_execute_requested"), 0.0)
            self.assertEqual(result.metrics.get("renderer_runtime_success"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_run_manifest_written"), 1.0)
            self.assertEqual(result.metrics.get("renderer_pipeline_summary_written"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_run_planned_only"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_runner_request_written"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_runner_command_written"), 1.0)

            contract_path = result.artifacts["renderer_playback_contract"]
            plan = json.loads(
                result.artifacts["renderer_execution_plan"].read_text(encoding="utf-8")
            )
            run_manifest = json.loads(
                result.artifacts["backend_run_manifest"].read_text(encoding="utf-8")
            )
            runner_request = json.loads(
                result.artifacts["backend_runner_request"].read_text(encoding="utf-8")
            )
            output_spec = json.loads(
                result.artifacts["backend_output_spec"].read_text(encoding="utf-8")
            )
            pipeline_summary = json.loads(
                result.artifacts["renderer_pipeline_summary"].read_text(encoding="utf-8")
            )
            self.assertFalse(plan["execute"])
            self.assertIn(str(contract_path), plan["command"])
            self.assertEqual(run_manifest["status"], "PLANNED_ONLY")
            self.assertIsNone(run_manifest["return_code"])
            self.assertEqual(pipeline_summary["status"], "PLANNED_ONLY")
            self.assertFalse(pipeline_summary["expected_outputs"]["inspection_available"])
            self.assertFalse(pipeline_summary["output_smoke_report"]["available"])
            self.assertFalse(pipeline_summary["output_comparison"]["available"])
            self.assertEqual(pipeline_summary["output_comparison"]["mismatch_reasons"], [])
            self.assertEqual(
                pipeline_summary["artifacts"]["backend_run_manifest"],
                str(result.artifacts["backend_run_manifest"]),
            )
            self.assertEqual(
                run_manifest["artifacts"]["backend_sensor_bundle_summary"],
                str(result.artifacts["backend_sensor_bundle_summary"]),
            )
            self.assertEqual(runner_request["runner_mode"], "direct_backend")
            self.assertEqual(
                runner_request["artifacts"]["backend_output_spec"],
                str(result.artifacts["backend_output_spec"]),
            )
            self.assertEqual(output_spec["backend"], "carla")
            self.assertEqual(
                run_manifest["artifacts"]["backend_runner_request"],
                str(result.artifacts["backend_runner_request"]),
            )
            self.assertEqual(
                run_manifest["artifacts"]["backend_output_spec"],
                str(result.artifacts["backend_output_spec"]),
            )
            self.assertEqual(
                run_manifest["artifacts"]["backend_direct_run_command"],
                str(result.artifacts["backend_direct_run_command"]),
            )
            self.assertIsNone(run_manifest["artifacts"]["backend_output_smoke_report"])
            self.assertIsNone(run_manifest["artifacts"]["backend_output_comparison_report"])

    def test_renderer_runtime_injects_frame_manifest_arg_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "carla",
                    "renderer_execute": False,
                    "renderer_command": ["echo", "renderer_plan", "{contract}"],
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertIn("backend_frame_inputs_manifest", result.artifacts)
            manifest_path = result.artifacts["backend_frame_inputs_manifest"]
            plan = json.loads(
                result.artifacts["renderer_execution_plan"].read_text(encoding="utf-8")
            )
            self.assertIn("--frame-manifest", plan["command"])
            flag_index = plan["command"].index("--frame-manifest")
            self.assertEqual(plan["command"][flag_index + 1], str(manifest_path))
            self.assertEqual(plan["contract_frame_manifest_args_count"], 2)
            self.assertEqual(plan["contract_ingestion_profile_args_count"], 0)
            self.assertEqual(plan["contract_bundle_summary_args_count"], 0)
            self.assertNotIn("--ingestion-profile", plan["command"])
            self.assertNotIn("--sensor-bundle-summary", plan["command"])
            self.assertEqual(
                result.metrics.get("renderer_contract_frame_manifest_args_count"),
                2.0,
            )
            self.assertEqual(
                result.metrics.get("renderer_contract_ingestion_profile_args_count"),
                0.0,
            )
            self.assertEqual(
                result.metrics.get("renderer_contract_bundle_summary_args_count"),
                0.0,
            )

    def test_renderer_runtime_uses_depth_camera_payload_format(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "carla",
                    "renderer_execute": False,
                    "renderer_command": ["echo", "renderer_plan", "{contract}"],
                    "camera_projection_enabled": True,
                    "camera_projection_trajectory_sweep_enabled": True,
                    "camera_projection_trajectory_sweep_frames": 2,
                    "camera_sensor_type": "DEPTH",
                    "camera_depth_params": {"min": 1.0, "max": 60.0, "type": "LINEAR"},
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            contract = json.loads(
                result.artifacts["renderer_playback_contract"].read_text(encoding="utf-8")
            )
            manifest = json.loads(
                result.artifacts["backend_frame_inputs_manifest"].read_text(encoding="utf-8")
            )
            self.assertEqual(contract["sensor_setup"]["camera"]["sensor_type"], "DEPTH")
            self.assertEqual(manifest["frames"][0]["camera"]["data_format"], "camera_depth_json")

    def test_renderer_runtime_uses_semantic_camera_payload_format(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "carla",
                    "renderer_execute": False,
                    "renderer_command": ["echo", "renderer_plan", "{contract}"],
                    "camera_projection_enabled": True,
                    "camera_projection_trajectory_sweep_enabled": True,
                    "camera_projection_trajectory_sweep_frames": 2,
                    "camera_sensor_type": "SEMANTIC_SEGMENTATION",
                    "camera_semantic_params": {
                        "class_version": "GRANULAR_SEGMENTATION",
                        "include_material_class": True,
                    },
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            contract = json.loads(
                result.artifacts["renderer_playback_contract"].read_text(encoding="utf-8")
            )
            manifest = json.loads(
                result.artifacts["backend_frame_inputs_manifest"].read_text(encoding="utf-8")
            )
            self.assertEqual(
                contract["sensor_setup"]["camera"]["sensor_type"],
                "SEMANTIC_SEGMENTATION",
            )
            self.assertEqual(
                contract["sensor_setup"]["camera"]["semantic_params"]["class_version"],
                "GRANULAR_SEGMENTATION",
            )
            self.assertEqual(manifest["frames"][0]["camera"]["data_format"], "camera_semantic_json")

    def test_renderer_runtime_uses_radar_track_payload_format(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "carla",
                    "renderer_execute": False,
                    "renderer_command": ["echo", "renderer_plan", "{contract}"],
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": True,
                    "radar_trajectory_sweep_enabled": True,
                    "radar_trajectory_sweep_frames": 2,
                    "radar_tracking_params": {
                        "tracks": True,
                        "max_tracks": 4,
                    },
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            contract = json.loads(
                result.artifacts["renderer_playback_contract"].read_text(encoding="utf-8")
            )
            manifest = json.loads(
                result.artifacts["backend_frame_inputs_manifest"].read_text(encoding="utf-8")
            )
            output_spec = json.loads(
                result.artifacts["backend_output_spec"].read_text(encoding="utf-8")
            )
            pipeline_summary = json.loads(
                result.artifacts["renderer_pipeline_summary"].read_text(encoding="utf-8")
            )
            self.assertTrue(contract["sensor_setup"]["radar"]["tracking_params"]["tracks"])
            self.assertEqual(manifest["frames"][0]["radar"]["data_format"], "radar_tracks_json")
            outputs_by_sensor = {
                entry["sensor_id"]: entry for entry in output_spec["expected_outputs_by_sensor"]
            }
            self.assertEqual(outputs_by_sensor["radar_front"]["output_count"], 2)
            radar_roles = {
                item["output_role"]: item for item in outputs_by_sensor["radar_front"]["outputs"]
            }
            self.assertIn("radar_tracks", radar_roles)
            self.assertIn("radar_detections", radar_roles)
            self.assertEqual(
                radar_roles["radar_tracks"]["artifact_type"],
                "carla_radar_tracks_json",
            )
            self.assertEqual(
                radar_roles["radar_tracks"]["backend_filename"],
                "tracks.json",
            )
            self.assertEqual(
                radar_roles["radar_detections"]["artifact_type"],
                "carla_radar_detections_json",
            )
            self.assertTrue(radar_roles["radar_detections"]["embedded_output"])
            self.assertEqual(
                radar_roles["radar_detections"]["carrier_data_format"],
                "radar_tracks_json",
            )
            self.assertEqual(
                radar_roles["radar_detections"]["shared_output_artifact_key"],
                "sensor_output_radar_front",
            )
            role_groups = {
                entry["output_role"]: entry for entry in output_spec["expected_outputs_by_role"]
            }
            self.assertIn("radar_tracks", role_groups)
            self.assertIn("radar_detections", role_groups)
            self.assertEqual(role_groups["radar_tracks"]["expected_count"], 1)
            self.assertEqual(role_groups["radar_detections"]["expected_count"], 1)
            self.assertEqual(role_groups["radar_tracks"]["embedded_output_count"], 0)
            self.assertEqual(role_groups["radar_detections"]["embedded_output_count"], 1)
            self.assertEqual(
                role_groups["radar_tracks"]["backend_filenames"],
                ["tracks.json"],
            )
            self.assertEqual(
                role_groups["radar_detections"]["carrier_data_formats"],
                ["radar_tracks_json"],
            )
            artifact_groups = {
                entry["artifact_type"]: entry
                for entry in output_spec["expected_outputs_by_artifact_type"]
            }
            self.assertIn("carla_radar_tracks_json", artifact_groups)
            self.assertIn("carla_radar_detections_json", artifact_groups)
            self.assertEqual(
                artifact_groups["carla_radar_detections_json"]["embedded_output_count"],
                1,
            )
            pipeline_role_groups = {
                entry["output_role"]: entry for entry in pipeline_summary["expected_outputs"]["by_output_role"]
            }
            self.assertIn("radar_tracks", pipeline_role_groups)
            self.assertIn("radar_detections", pipeline_role_groups)
            self.assertEqual(
                pipeline_role_groups["radar_detections"]["carrier_data_formats"],
                ["radar_tracks_json"],
            )
            self.assertFalse(pipeline_summary["expected_outputs"]["inspection_available"])

    def test_renderer_runtime_can_disable_frame_manifest_arg_injection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "carla",
                    "renderer_execute": False,
                    "renderer_command": ["echo", "renderer_plan", "{contract}"],
                    "renderer_inject_frame_manifest_arg": False,
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertIn("backend_frame_inputs_manifest", result.artifacts)
            plan = json.loads(
                result.artifacts["renderer_execution_plan"].read_text(encoding="utf-8")
            )
            self.assertNotIn("--frame-manifest", plan["command"])
            self.assertEqual(plan["contract_frame_manifest_args_count"], 0)
            self.assertEqual(plan["contract_ingestion_profile_args_count"], 0)
            self.assertEqual(
                result.metrics.get("renderer_contract_frame_manifest_args_count"),
                0.0,
            )
            self.assertEqual(
                result.metrics.get("renderer_contract_ingestion_profile_args_count"),
                0.0,
            )

    def test_renderer_runtime_executes_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            fake_renderer = root / "fake_renderer.sh"
            fake_renderer.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
contract=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--contract" ]]; then
    contract="$2"
    shift 2
  else
    shift
  fi
done
if [[ -z "${contract}" ]]; then
  echo "missing contract" >&2
  exit 4
fi
echo "renderer_ok ${contract}"
""",
                encoding="utf-8",
            )
            fake_renderer.chmod(0o755)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "awsim",
                    "renderer_execute": True,
                    "renderer_command": [str(fake_renderer), "--contract", "{contract}"],
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)
            self.assertIn("renderer_stdout", result.artifacts)
            self.assertIn("renderer_stderr", result.artifacts)
            self.assertIn("backend_run_manifest", result.artifacts)
            self.assertEqual(result.metrics.get("renderer_execute_requested"), 1.0)
            self.assertEqual(result.metrics.get("renderer_return_code"), 0.0)
            self.assertEqual(result.metrics.get("renderer_runtime_success"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_run_execution_succeeded"), 1.0)
            stdout = result.artifacts["renderer_stdout"].read_text(encoding="utf-8")
            run_manifest = json.loads(
                result.artifacts["backend_run_manifest"].read_text(encoding="utf-8")
            )
            self.assertIn("renderer_ok", stdout)
            self.assertEqual(run_manifest["status"], "EXECUTION_SUCCEEDED")
            self.assertEqual(run_manifest["return_code"], 0)
            self.assertEqual(
                run_manifest["artifacts"]["renderer_stdout"],
                str(result.artifacts["renderer_stdout"]),
            )
            self.assertEqual(
                run_manifest["artifacts"]["renderer_stderr"],
                str(result.artifacts["renderer_stderr"]),
            )

    def test_renderer_runtime_execution_failure_writes_backend_run_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            fake_renderer = root / "fake_renderer_fail.sh"
            fake_renderer.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
echo "renderer_fail" >&2
exit 7
""",
                encoding="utf-8",
            )
            fake_renderer.chmod(0o755)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "awsim",
                    "renderer_execute": True,
                    "renderer_command": [str(fake_renderer), "--contract", "{contract}"],
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.artifacts["backend_run_manifest"].exists())
            self.assertEqual(result.metrics.get("renderer_runtime_success"), 0.0)
            self.assertIn("backend_run_manifest", result.artifacts)
            self.assertEqual(result.metrics.get("renderer_return_code"), 7.0)
            self.assertEqual(result.metrics.get("renderer_backend_run_execution_failed"), 1.0)
            run_manifest = json.loads(
                result.artifacts["backend_run_manifest"].read_text(encoding="utf-8")
            )
            self.assertEqual(run_manifest["status"], "EXECUTION_FAILED")
            self.assertEqual(run_manifest["failure_reason"], "NONZERO_EXIT")
            self.assertEqual(run_manifest["return_code"], 7)
            self.assertEqual(
                run_manifest["artifacts"]["renderer_stderr"],
                str(result.artifacts["renderer_stderr"]),
            )

    def test_renderer_runtime_plan_error_writes_backend_run_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "carla",
                    "renderer_execute": False,
                    "renderer_bin": "renderer-bin",
                    "renderer_extra_args": "--invalid",
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertEqual(result.metrics.get("renderer_runtime_success"), 0.0)
            self.assertIn("backend_run_manifest", result.artifacts)
            self.assertEqual(result.metrics.get("renderer_backend_run_plan_error"), 1.0)
            run_manifest = json.loads(
                result.artifacts["backend_run_manifest"].read_text(encoding="utf-8")
            )
            self.assertEqual(run_manifest["status"], "PLAN_ERROR")
            self.assertEqual(run_manifest["failure_reason"], "BUILD_ERROR")
            self.assertIn("renderer_extra_args must be a list", run_manifest["message"])

    def test_renderer_runtime_uses_backend_default_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "awsim",
                    "renderer_execute": False,
                    "renderer_bin": "",
                    "renderer_command": [],
                    "awsim_bin": "awsim-player",
                    "awsim_extra_args": ["--headless"],
                    "renderer_extra_args": ["--fps", "20"],
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)
            self.assertEqual(result.metrics.get("renderer_runtime_success"), 1.0)
            self.assertIn("renderer_execution_plan", result.artifacts)

            contract_path = result.artifacts["renderer_playback_contract"]
            plan = json.loads(
                result.artifacts["renderer_execution_plan"].read_text(encoding="utf-8")
            )
            self.assertFalse(plan["used_command_override"])
            self.assertEqual(plan["command_source"], "backend_default")
            self.assertFalse(plan["backend_wrapper_used"])
            self.assertIsNone(plan["error"])
            self.assertEqual(plan["command"][0], "awsim-player")
            self.assertIn("--headless", plan["command"])
            self.assertIn("--fps", plan["command"])
            self.assertIn("20", plan["command"])
            self.assertIn(str(contract_path), plan["command"])
            self.assertGreaterEqual(plan["contract_scene_args_count"], 2)
            self.assertEqual(plan["contract_sensor_mount_args_count"], 0)
            self.assertEqual(result.metrics.get("renderer_backend_wrapper_used"), 0.0)
            self.assertIn("backend_args_preview", plan)
            preview = plan["backend_args_preview"]
            self.assertEqual(preview["backend"], "awsim")
            self.assertEqual(preview["scene"]["weather"], "default")
            self.assertIn("--weather", preview["scene_cli_args"])
            self.assertEqual(preview["sensor_mounts"], [])

    def test_renderer_runtime_uses_backend_wrapper_when_bin_not_set(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "awsim",
                    "renderer_execute": False,
                    "renderer_bin": "",
                    "awsim_bin": "",
                    "renderer_command": [],
                    "renderer_map": "wrapper_map",
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)
            self.assertIn("renderer_execution_plan", result.artifacts)
            plan = json.loads(
                result.artifacts["renderer_execution_plan"].read_text(encoding="utf-8")
            )
            self.assertEqual(plan["command_source"], "backend_wrapper")
            self.assertTrue(plan["backend_wrapper_used"])
            self.assertTrue(plan["command"][0].endswith("scripts/renderer_launch_awsim.sh"))
            self.assertIn("--map", plan["command"])
            self.assertIn("wrapper_map", plan["command"])
            self.assertIn("--ingestion-profile", plan["command"])
            self.assertIn("--sensor-bundle-summary", plan["command"])
            self.assertEqual(plan["contract_ingestion_profile_args_count"], 2)
            self.assertEqual(plan["contract_bundle_summary_args_count"], 2)
            self.assertEqual(result.metrics.get("renderer_backend_wrapper_used"), 1.0)
            self.assertEqual(plan["backend_args_preview"]["scene"]["map"], "wrapper_map")
            self.assertIn("backend_sensor_bundle_summary", plan)
            self.assertTrue(Path(plan["backend_sensor_bundle_summary"]).exists())
            self.assertIn("backend_runner_request", plan)
            self.assertIn("backend_direct_run_command", plan)
            self.assertEqual(
                result.metrics.get("renderer_contract_ingestion_profile_args_count"),
                2.0,
            )
            self.assertEqual(
                result.metrics.get("renderer_contract_bundle_summary_args_count"),
                2.0,
            )

    def test_renderer_runtime_wrapper_execution_writes_wrapper_invocation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            fake_awsim = root / "fake_awsim.sh"
            fake_awsim.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
echo "awsim_backend_ok"
""",
                encoding="utf-8",
            )
            fake_awsim.chmod(0o755)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "awsim",
                    "renderer_execute": True,
                    "renderer_bin": "",
                    "awsim_bin": "",
                    "renderer_command": [],
                    "renderer_map": "Town07",
                    "renderer_sensor_mounts_only_enabled": False,
                    "renderer_sensor_mount_format": "json",
                    "renderer_camera_sensor_id": "cam_front",
                    "renderer_lidar_sensor_id": "lidar_top",
                    "renderer_radar_sensor_id": "radar_front",
                    "camera_extrinsics": {
                        "enabled": True,
                        "tx": 1.0,
                        "ty": 2.0,
                        "tz": 3.0,
                        "roll_deg": 0.1,
                        "pitch_deg": 0.2,
                        "yaw_deg": 0.3,
                    },
                    "lidar_extrinsics": {
                        "enabled": True,
                        "tx": 4.0,
                        "ty": 5.0,
                        "tz": 6.0,
                        "roll_deg": 0.4,
                        "pitch_deg": 0.5,
                        "yaw_deg": 0.6,
                    },
                    "radar_extrinsics": {
                        "enabled": True,
                        "tx": 7.0,
                        "ty": 8.0,
                        "tz": 9.0,
                        "roll_deg": 0.7,
                        "pitch_deg": 0.8,
                        "yaw_deg": 0.9,
                    },
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            with mock.patch.dict(os.environ, {"AWSIM_BIN": str(fake_awsim)}, clear=False):
                result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertEqual(result.metrics.get("renderer_backend_wrapper_used"), 1.0)
            self.assertIn("backend_invocation", result.artifacts)
            self.assertIn("backend_wrapper_invocation", result.artifacts)
            self.assertIn("backend_run_manifest", result.artifacts)

            backend_invocation = json.loads(
                result.artifacts["backend_invocation"].read_text(encoding="utf-8")
            )
            runner_request = json.loads(
                result.artifacts["backend_runner_request"].read_text(encoding="utf-8")
            )
            self.assertEqual(backend_invocation["command_source"], "backend_wrapper")
            self.assertTrue(backend_invocation["backend_wrapper_used"])
            self.assertEqual(backend_invocation["backend_runner_request"], str(result.artifacts["backend_runner_request"]))
            self.assertEqual(
                backend_invocation["backend_direct_run_command"],
                str(result.artifacts["backend_direct_run_command"]),
            )

            wrapper_invocation = json.loads(
                result.artifacts["backend_wrapper_invocation"].read_text(encoding="utf-8")
            )
            run_manifest = json.loads(
                result.artifacts["backend_run_manifest"].read_text(encoding="utf-8")
            )
            self.assertEqual(wrapper_invocation["wrapper"], "awsim")
            self.assertIn("--ingestion-profile", wrapper_invocation["input_args"])
            self.assertIn("--sensor-bundle-summary", wrapper_invocation["input_args"])
            self.assertEqual(
                wrapper_invocation["sensor_bundle_summary"],
                str(result.artifacts["backend_sensor_bundle_summary"]),
            )
            self.assertIn("--mount-sensor", wrapper_invocation["output_args"])
            self.assertIn("--mount-pose", wrapper_invocation["output_args"])
            self.assertIn("--map", wrapper_invocation["output_args"])
            self.assertIn("Town07", wrapper_invocation["output_args"])
            self.assertNotIn("--sensor-bundle-summary", wrapper_invocation["output_args"])
            self.assertIn(
                "cam_front:1.0:2.0:3.0:0.1:0.2:0.3",
                wrapper_invocation["output_args"],
            )
            self.assertEqual(run_manifest["status"], "EXECUTION_SUCCEEDED")
            self.assertEqual(
                run_manifest["artifacts"]["backend_wrapper_invocation"],
                str(result.artifacts["backend_wrapper_invocation"]),
            )
            self.assertEqual(runner_request["resolved_backend_bin"], str(fake_awsim))
            self.assertEqual(runner_request["backend_bin_source"], "env")
            self.assertIn("--mount-sensor", runner_request["mount_args"])
            self.assertIn("--mount-pose", runner_request["mount_args"])
            self.assertEqual(runner_request["ingestion_args"], [])
            self.assertNotIn("--ingestion-profile", runner_request["command"])
            self.assertNotIn("--sensor-bundle-summary", runner_request["command"])
            direct_command = result.artifacts["backend_direct_run_command"].read_text(encoding="utf-8")
            self.assertIn(str(fake_awsim), direct_command)
            self.assertIn("--mount-sensor", direct_command)

    def test_renderer_runtime_can_execute_via_backend_runner(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            fake_awsim = root / "fake_awsim_direct.sh"
            fake_awsim.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
python3 - "${BACKEND_OUTPUT_SPEC_PATH}" <<'PY'
import json
import pathlib
import sys
payload = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
for entry in payload.get("expected_outputs", []):
    if entry.get("artifact_key") == "awsim_runtime_state_json":
        target = pathlib.Path(entry["path"])
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text('{"backend":"awsim"}', encoding="utf-8")
    if entry.get("artifact_key") == "sensor_output_camera_front":
        candidates = entry.get("path_candidates", [])
        fallback = next(
            (candidate for candidate in candidates if candidate.endswith("camera_projection.json")),
            "",
        )
        target = pathlib.Path(fallback or (candidates[1] if len(candidates) > 1 else entry["path"]))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text('{"sensor":"camera_front"}', encoding="utf-8")
PY
mkdir -p "${BACKEND_OUTPUT_ROOT}/extras"
printf '{"kind":"extra"}\n' > "${BACKEND_OUTPUT_ROOT}/extras/debug_extra.json"
printf "direct_backend\\n"
printf "%s\\n" "$@"
""",
                encoding="utf-8",
            )
            fake_awsim.chmod(0o755)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "awsim",
                    "renderer_execute": True,
                    "renderer_execute_via_runner": True,
                    "renderer_bin": "",
                    "awsim_bin": "",
                    "renderer_command": [],
                    "renderer_map": "Town09",
                    "camera_projection_enabled": True,
                    "camera_projection_trajectory_sweep_enabled": True,
                    "camera_projection_trajectory_sweep_frames": 2,
                    "lidar_postprocess_enabled": True,
                    "lidar_trajectory_sweep_enabled": True,
                    "lidar_trajectory_sweep_frames": 2,
                    "radar_postprocess_enabled": True,
                    "radar_trajectory_sweep_enabled": True,
                    "radar_trajectory_sweep_frames": 2,
                    "renderer_sensor_mounts_only_enabled": False,
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            with mock.patch.dict(os.environ, {"AWSIM_BIN": str(fake_awsim)}, clear=False):
                result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertNotIn("backend_wrapper_invocation", result.artifacts)
            self.assertIn("backend_runner_execution_manifest", result.artifacts)
            self.assertIn("backend_runner_stdout", result.artifacts)
            self.assertIn("backend_runner_stderr", result.artifacts)
            self.assertIn("backend_output_spec", result.artifacts)
            self.assertIn("awsim_runtime_state_json", result.artifacts)
            self.assertIn("sensor_output_camera_front", result.artifacts)
            self.assertIn("renderer_pipeline_summary", result.artifacts)
            self.assertIn("backend_sensor_output_summary", result.artifacts)
            self.assertIn("backend_output_smoke_report", result.artifacts)
            self.assertIn("backend_output_comparison_report", result.artifacts)
            self.assertNotIn("backend_output_inspection_manifest", result.artifacts)
            self.assertNotIn("backend_runner_smoke_manifest", result.artifacts)
            self.assertEqual(result.metrics.get("renderer_execute_via_runner_requested"), 1.0)
            self.assertEqual(
                result.metrics.get("renderer_execute_and_inspect_via_runner_requested"),
                0.0,
            )
            self.assertEqual(result.metrics.get("renderer_backend_runner_execution_used"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_execution_wrapper_used"), 0.0)
            self.assertEqual(result.metrics.get("renderer_pipeline_summary_written"), 1.0)
            self.assertEqual(result.metrics.get("renderer_pipeline_output_inspection_available"), 0.0)
            self.assertEqual(result.metrics.get("renderer_pipeline_runner_smoke_available"), 0.0)
            self.assertEqual(result.metrics.get("renderer_pipeline_sensor_output_summary_available"), 1.0)
            self.assertEqual(result.metrics.get("renderer_pipeline_output_smoke_report_available"), 1.0)
            self.assertEqual(result.metrics.get("renderer_pipeline_output_comparison_available"), 1.0)
            stdout = result.artifacts["renderer_stdout"].read_text(encoding="utf-8")
            self.assertIn("direct_backend", stdout)
            self.assertIn("--mount-sensor", stdout)
            self.assertIn("--ingest-sensor-frame", stdout)
            self.assertNotIn("--ingestion-profile", stdout)
            self.assertNotIn("--sensor-bundle-summary", stdout)

            plan = json.loads(
                result.artifacts["renderer_execution_plan"].read_text(encoding="utf-8")
            )
            backend_invocation = json.loads(
                result.artifacts["backend_invocation"].read_text(encoding="utf-8")
            )
            run_manifest = json.loads(
                result.artifacts["backend_run_manifest"].read_text(encoding="utf-8")
            )
            runner_execution_manifest = json.loads(
                result.artifacts["backend_runner_execution_manifest"].read_text(
                    encoding="utf-8"
                )
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
            output_spec = json.loads(
                result.artifacts["backend_output_spec"].read_text(encoding="utf-8")
            )
            pipeline_summary = json.loads(
                result.artifacts["renderer_pipeline_summary"].read_text(encoding="utf-8")
            )
            self.assertTrue(plan["execute_via_runner"])
            self.assertFalse(plan["execute_and_inspect_via_runner"])
            self.assertEqual(plan["command_source"], "backend_wrapper")
            self.assertEqual(plan["execution_command_source"], "backend_runner")
            self.assertFalse(plan["execution_backend_wrapper_used"])
            self.assertTrue(plan["execution_command"][0].endswith("fake_awsim_direct.sh"))
            self.assertTrue(backend_invocation["execute_via_runner"])
            self.assertFalse(backend_invocation["execute_and_inspect_via_runner"])
            self.assertEqual(backend_invocation["execution_command_source"], "backend_runner")
            self.assertFalse(backend_invocation["execution_backend_wrapper_used"])
            self.assertEqual(run_manifest["status"], "EXECUTION_SUCCEEDED")
            self.assertEqual(run_manifest["command_source"], "backend_runner")
            self.assertFalse(run_manifest["backend_wrapper_used"])
            self.assertIsNone(run_manifest["artifacts"]["backend_wrapper_invocation"])
            self.assertEqual(
                run_manifest["artifacts"]["backend_output_spec"],
                str(result.artifacts["backend_output_spec"]),
            )
            self.assertEqual(
                run_manifest["artifacts"]["backend_runner_execution_manifest"],
                str(result.artifacts["backend_runner_execution_manifest"]),
            )
            self.assertEqual(
                run_manifest["artifacts"]["backend_sensor_output_summary"],
                str(result.artifacts["backend_sensor_output_summary"]),
            )
            self.assertEqual(
                run_manifest["artifacts"]["backend_output_smoke_report"],
                str(result.artifacts["backend_output_smoke_report"]),
            )
            self.assertEqual(
                run_manifest["artifacts"]["backend_output_comparison_report"],
                str(result.artifacts["backend_output_comparison_report"]),
            )
            self.assertEqual(
                run_manifest["artifacts"]["backend_runner_stdout"],
                str(result.artifacts["backend_runner_stdout"]),
            )
            self.assertEqual(runner_execution_manifest["status"], "EXECUTION_SUCCEEDED")
            self.assertEqual(runner_execution_manifest["output_smoke_report"]["status"], "PARTIAL")
            self.assertEqual(runner_execution_manifest["output_comparison_report"]["status"], "MIXED")
            self.assertEqual(
                runner_execution_manifest["output_comparison_report"]["mismatch_reasons"],
                ["MISSING_EXPECTED_OUTPUTS", "UNEXPECTED_OUTPUTS_PRESENT", "BACKEND_FILENAME_MISMATCH"],
            )
            self.assertGreaterEqual(
                runner_execution_manifest["expected_output_summary"]["found_count"],
                1,
            )
            self.assertTrue(
                any(
                    entry.get("artifact_key") == "awsim_runtime_state_json" and entry.get("exists")
                    for entry in runner_execution_manifest["expected_outputs"]
                )
            )
            self.assertTrue(
                any(
                    entry.get("artifact_key") == "sensor_output_camera_front" and entry.get("exists")
                    and "/sensor_exports/camera_front/camera_projection.json"
                    in str(entry.get("resolved_path", ""))
                    for entry in runner_execution_manifest["expected_outputs"]
                )
            )
            self.assertEqual(output_spec["backend"], "awsim")
            self.assertEqual(sensor_output_summary["sensor_count"], 3)
            self.assertEqual(sensor_output_summary["found_sensor_count"], 1)
            self.assertEqual(sensor_output_summary["status"], "PARTIAL")
            self.assertEqual(sensor_output_summary["output_role_counts"]["camera_visible"], 1)
            self.assertEqual(
                sensor_output_summary["artifact_type_counts"]["awsim_camera_rgb_json"],
                1,
            )
            self.assertEqual(output_smoke_report["status"], "PARTIAL")
            self.assertGreaterEqual(output_smoke_report["found_output_count"], 1)
            self.assertGreaterEqual(output_smoke_report["missing_output_count"], 1)
            self.assertEqual(output_smoke_report["sensor_status_counts"]["COMPLETE"], 1)
            self.assertEqual(output_smoke_report["sensor_status_counts"]["MISSING"], 2)
            self.assertEqual(output_comparison_report["status"], "MIXED")
            self.assertEqual(
                output_comparison_report["mismatch_reasons"],
                ["MISSING_EXPECTED_OUTPUTS", "UNEXPECTED_OUTPUTS_PRESENT", "BACKEND_FILENAME_MISMATCH"],
            )
            self.assertEqual(output_comparison_report["unexpected_output_count"], 1)
            self.assertEqual(
                output_comparison_report["unexpected_outputs"][0]["relative_path"],
                "extras/debug_extra.json",
            )
            self.assertEqual(output_comparison_report["by_sensor"][0]["status"], "MATCHED")
            self.assertEqual(
                output_comparison_report["by_sensor"][0]["mismatch_reasons"],
                ["BACKEND_FILENAME_MISMATCH"],
            )
            self.assertEqual(
                output_comparison_report["by_sensor"][0]["role_diffs"][0]["status"],
                "MATCHED",
            )
            self.assertEqual(
                output_comparison_report["by_sensor"][0]["role_diffs"][0]["output_role"],
                "camera_visible",
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
                output_comparison_report["by_sensor"][1]["mismatch_reasons"],
                ["NO_DISCOVERED_FILES", "MISSING_EXPECTED_OUTPUTS"],
            )
            self.assertEqual(
                output_comparison_report["by_sensor"][1]["role_diffs"][0]["status"],
                "MISSING_EXPECTED",
            )
            self.assertEqual(
                output_comparison_report["by_sensor"][1]["role_diffs"][0]["output_role"],
                "lidar_point_cloud",
            )
            self.assertEqual(
                output_comparison_report["by_sensor"][2]["missing_output_roles"],
                ["radar_detections"],
            )
            self.assertEqual(
                output_comparison_report["by_sensor"][2]["role_diffs"][0]["missing_relative_paths"],
                ["sensor_exports/radar_front/targets.json"],
            )
            smoke_roles = {
                entry["output_role"]: entry for entry in output_smoke_report["by_output_role"]
            }
            self.assertEqual(smoke_roles["camera_visible"]["found_sensor_ids"], ["camera_front"])
            self.assertEqual(smoke_roles["camera_visible"]["backend_filenames"], ["rgb_frame.json"])
            self.assertEqual(smoke_roles["lidar_point_cloud"]["missing_sensor_ids"], ["lidar_top"])
            self.assertTrue(
                any(
                    sensor["sensor_id"] == "camera_front"
                    and sensor["available"]
                    and sensor["outputs"][0]["resolved_path"]
                    and "/sensor_exports/camera_front/camera_projection.json"
                    in sensor["outputs"][0]["resolved_path"]
                    and sensor["outputs"][0]["backend_filename"] == "rgb_frame.json"
                    and sensor["outputs"][0]["output_role"] == "camera_visible"
                    and sensor["outputs"][0]["artifact_type"] == "awsim_camera_rgb_json"
                    for sensor in sensor_output_summary["sensors"]
                )
            )
            self.assertEqual(pipeline_summary["status"], "EXECUTION_SUCCEEDED")
            self.assertTrue(pipeline_summary["expected_outputs"]["inspection_available"])
            self.assertFalse(pipeline_summary["output_inspection"]["available"])
            self.assertFalse(pipeline_summary["runner_smoke"]["available"])
            self.assertTrue(pipeline_summary["sensor_outputs"]["summary_available"])
            self.assertTrue(pipeline_summary["output_smoke_report"]["available"])
            self.assertTrue(pipeline_summary["output_comparison"]["available"])
            self.assertEqual(pipeline_summary["output_smoke_report"]["status"], "PARTIAL")
            self.assertEqual(pipeline_summary["output_comparison"]["status"], "MIXED")
            self.assertEqual(
                pipeline_summary["output_comparison"]["mismatch_reasons"],
                ["MISSING_EXPECTED_OUTPUTS", "UNEXPECTED_OUTPUTS_PRESENT", "BACKEND_FILENAME_MISMATCH"],
            )
            self.assertEqual(
                pipeline_summary["output_comparison"]["by_sensor"][0]["role_diffs"][0]["status"],
                "MATCHED",
            )
            self.assertEqual(pipeline_summary["sensor_outputs"]["found_sensor_count"], 1)
            self.assertEqual(
                pipeline_summary["sensor_outputs"]["output_role_counts"]["camera_visible"],
                1,
            )
            self.assertEqual(
                pipeline_summary["sensor_outputs"]["artifact_type_counts"]["awsim_camera_rgb_json"],
                1,
            )
            self.assertEqual(
                pipeline_summary["expected_outputs"]["by_output_role"][0]["output_role"],
                "camera_visible",
            )
            self.assertEqual(
                pipeline_summary["sensor_outputs"]["output_roles"][0]["output_role"],
                "camera_visible",
            )
            self.assertIn(
                "awsim_runtime_state_json",
                pipeline_summary["expected_outputs"]["found_artifact_keys"],
            )
            self.assertIn(
                "sensor_output_camera_front",
                pipeline_summary["expected_outputs"]["found_artifact_keys"],
            )
            self.assertTrue(
                any(
                    sensor["sensor_id"] == "camera_front"
                    and sensor["outputs"][0]["backend_filename"] == "rgb_frame.json"
                    and sensor["outputs"][0]["output_role"] == "camera_visible"
                    and sensor["outputs"][0]["artifact_type"] == "awsim_camera_rgb_json"
                    for sensor in pipeline_summary["sensor_outputs"]["sensors"]
                )
            )
            self.assertEqual(
                pipeline_summary["artifacts"]["backend_runner_execution_manifest"],
                str(result.artifacts["backend_runner_execution_manifest"]),
            )
            self.assertEqual(
                pipeline_summary["artifacts"]["backend_sensor_output_summary"],
                str(result.artifacts["backend_sensor_output_summary"]),
            )
            self.assertEqual(
                pipeline_summary["artifacts"]["backend_output_smoke_report"],
                str(result.artifacts["backend_output_smoke_report"]),
            )
            self.assertEqual(
                pipeline_summary["artifacts"]["backend_output_comparison_report"],
                str(result.artifacts["backend_output_comparison_report"]),
            )

    def test_renderer_runtime_can_execute_and_inspect_via_backend_runner(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            fake_awsim = root / "fake_awsim_direct.sh"
            fake_awsim.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
mkdir -p "${BACKEND_OUTPUT_ROOT}"
printf '{"status":"ok"}\n' > "${BACKEND_OUTPUT_ROOT}/awsim_runtime_state.json"
printf "direct_backend_smoke\\n"
printf "%s\\n" "$@"
""",
                encoding="utf-8",
            )
            fake_awsim.chmod(0o755)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "awsim",
                    "renderer_execute": True,
                    "renderer_execute_via_runner": True,
                    "renderer_execute_and_inspect_via_runner": True,
                    "renderer_fail_on_error": True,
                    "renderer_bin": "",
                    "awsim_bin": "",
                    "renderer_command": [],
                    "renderer_map": "Town09",
                    "camera_projection_enabled": True,
                    "renderer_sensor_mounts_only_enabled": False,
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            with mock.patch.dict(os.environ, {"AWSIM_BIN": str(fake_awsim)}, clear=False):
                result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertFalse(result.success)
            self.assertIn("backend_output_inspection_manifest", result.artifacts)
            self.assertIn("backend_runner_smoke_manifest", result.artifacts)
            self.assertEqual(
                result.metrics.get("renderer_execute_and_inspect_via_runner_requested"),
                1.0,
            )
            self.assertEqual(result.metrics.get("renderer_pipeline_output_inspection_available"), 1.0)
            self.assertEqual(result.metrics.get("renderer_pipeline_runner_smoke_available"), 1.0)
            run_manifest = json.loads(
                result.artifacts["backend_run_manifest"].read_text(encoding="utf-8")
            )
            inspection_manifest = json.loads(
                result.artifacts["backend_output_inspection_manifest"].read_text(
                    encoding="utf-8"
                )
            )
            smoke_manifest = json.loads(
                result.artifacts["backend_runner_smoke_manifest"].read_text(
                    encoding="utf-8"
                )
            )
            pipeline_summary = json.loads(
                result.artifacts["renderer_pipeline_summary"].read_text(encoding="utf-8")
            )
            self.assertEqual(run_manifest["status"], "EXECUTION_FAILED")
            self.assertEqual(run_manifest["failure_reason"], "OUTPUT_CONTRACT_MISMATCH")
            self.assertEqual(run_manifest["return_code"], 2)
            self.assertEqual(
                run_manifest["artifacts"]["backend_output_inspection_manifest"],
                str(result.artifacts["backend_output_inspection_manifest"]),
            )
            self.assertEqual(
                run_manifest["artifacts"]["backend_runner_smoke_manifest"],
                str(result.artifacts["backend_runner_smoke_manifest"]),
            )
            self.assertEqual(inspection_manifest["status"], "MISSING_EXPECTED")
            self.assertFalse(inspection_manifest["success"])
            self.assertEqual(smoke_manifest["status"], "INSPECTION_FAILED")
            self.assertFalse(smoke_manifest["success"])
            self.assertEqual(smoke_manifest["inspection"]["status"], "MISSING_EXPECTED")
            self.assertTrue(pipeline_summary["output_inspection"]["available"])
            self.assertEqual(pipeline_summary["output_inspection"]["status"], "MISSING_EXPECTED")
            self.assertFalse(pipeline_summary["output_inspection"]["success"])
            self.assertTrue(pipeline_summary["runner_smoke"]["available"])
            self.assertEqual(pipeline_summary["runner_smoke"]["status"], "INSPECTION_FAILED")
            self.assertFalse(pipeline_summary["runner_smoke"]["success"])
            self.assertEqual(
                pipeline_summary["artifacts"]["backend_output_inspection_manifest"],
                str(result.artifacts["backend_output_inspection_manifest"]),
            )
            self.assertEqual(
                pipeline_summary["artifacts"]["backend_runner_smoke_manifest"],
                str(result.artifacts["backend_runner_smoke_manifest"]),
            )

    def test_renderer_runtime_carla_wrapper_execution_transforms_sensor_mount_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            fake_carla = root / "fake_carla.sh"
            fake_carla.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
echo "carla_backend_ok"
""",
                encoding="utf-8",
            )
            fake_carla.chmod(0o755)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "carla",
                    "renderer_execute": True,
                    "renderer_bin": "",
                    "carla_bin": "",
                    "renderer_command": [],
                    "renderer_map": "Town03",
                    "renderer_sensor_mounts_only_enabled": False,
                    "renderer_sensor_mount_format": "json",
                    "renderer_camera_sensor_id": "cam_front",
                    "renderer_lidar_sensor_id": "lidar_top",
                    "renderer_radar_sensor_id": "radar_front",
                    "camera_extrinsics": {
                        "enabled": True,
                        "tx": 0.5,
                        "ty": 0.0,
                        "tz": 1.2,
                        "roll_deg": 0.0,
                        "pitch_deg": -1.5,
                        "yaw_deg": 0.0,
                    },
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            with mock.patch.dict(os.environ, {"CARLA_BIN": str(fake_carla)}, clear=False):
                result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertEqual(result.metrics.get("renderer_backend_wrapper_used"), 1.0)
            self.assertIn("backend_wrapper_invocation", result.artifacts)
            self.assertIn("backend_runner_request", result.artifacts)
            wrapper_invocation = json.loads(
                result.artifacts["backend_wrapper_invocation"].read_text(encoding="utf-8")
            )
            runner_request = json.loads(
                result.artifacts["backend_runner_request"].read_text(encoding="utf-8")
            )
            self.assertEqual(wrapper_invocation["wrapper"], "carla")
            self.assertIn("--ingestion-profile", wrapper_invocation["input_args"])
            self.assertIn("--attach-sensor", wrapper_invocation["output_args"])
            self.assertIn("--sensor-pose", wrapper_invocation["output_args"])
            self.assertIn("camera:cam_front:ego", wrapper_invocation["output_args"])
            self.assertIn(
                "cam_front:0.5:0.0:1.2:0.0:-1.5:0.0",
                wrapper_invocation["output_args"],
            )
            self.assertIn("--town", wrapper_invocation["output_args"])
            self.assertIn("Town03", wrapper_invocation["output_args"])
            self.assertEqual(runner_request["resolved_backend_bin"], str(fake_carla))
            self.assertEqual(runner_request["backend_bin_source"], "env")
            self.assertIn("--attach-sensor", runner_request["mount_args"])
            self.assertIn("--sensor-pose", runner_request["mount_args"])
            self.assertEqual(runner_request["ingestion_args"], [])

    def test_renderer_runtime_awsim_wrapper_consumes_frame_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            fake_awsim = root / "fake_awsim.sh"
            fake_awsim.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
echo "awsim_backend_ok"
""",
                encoding="utf-8",
            )
            fake_awsim.chmod(0o755)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "awsim",
                    "renderer_execute": True,
                    "renderer_bin": "",
                    "awsim_bin": "",
                    "renderer_command": [],
                    "camera_projection_enabled": True,
                    "camera_projection_trajectory_sweep_enabled": True,
                    "camera_projection_trajectory_sweep_frames": 2,
                    "lidar_postprocess_enabled": True,
                    "lidar_trajectory_sweep_enabled": True,
                    "lidar_trajectory_sweep_frames": 2,
                    "radar_postprocess_enabled": True,
                    "radar_trajectory_sweep_enabled": True,
                    "radar_trajectory_sweep_frames": 2,
                    "renderer_sensor_mounts_only_enabled": False,
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            with mock.patch.dict(os.environ, {"AWSIM_BIN": str(fake_awsim)}, clear=False):
                result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertIn("backend_wrapper_invocation", result.artifacts)
            wrapper_invocation = json.loads(
                result.artifacts["backend_wrapper_invocation"].read_text(encoding="utf-8")
            )
            output_args = wrapper_invocation["output_args"]
            self.assertIn("--ingestion-profile", wrapper_invocation["input_args"])
            self.assertIn("--sensor-bundle-summary", wrapper_invocation["input_args"])
            self.assertEqual(
                wrapper_invocation["sensor_bundle_summary"],
                str(result.artifacts["backend_sensor_bundle_summary"]),
            )
            ingest_indices = [idx for idx, token in enumerate(output_args) if token == "--ingest-sensor-frame"]
            self.assertEqual(len(ingest_indices), 6)
            ingest_payloads = [output_args[idx + 1] for idx in ingest_indices]
            self.assertTrue(any(payload.startswith("camera:0:") for payload in ingest_payloads))
            self.assertTrue(any(payload.startswith("lidar:0:") for payload in ingest_payloads))
            self.assertTrue(any(payload.startswith("radar:0:") for payload in ingest_payloads))
            for payload in ingest_payloads:
                parts = payload.split(":", 2)
                self.assertEqual(len(parts), 3)
                self.assertTrue(Path(parts[2]).exists())
            meta_indices = [idx for idx, token in enumerate(output_args) if token == "--ingest-sensor-meta"]
            self.assertEqual(len(meta_indices), 3)
            meta_payloads = [output_args[idx + 1] for idx in meta_indices]
            self.assertIn("camera:camera_front:camera_projection_json:ego", meta_payloads)
            self.assertIn("lidar:lidar_top:lidar_points_json:ego", meta_payloads)
            self.assertIn("radar:radar_front:radar_targets_json:ego", meta_payloads)
            self.assertNotIn("--sensor-bundle-summary", output_args)

    def test_renderer_runtime_carla_wrapper_consumes_frame_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            fake_carla = root / "fake_carla.sh"
            fake_carla.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
echo "carla_backend_ok"
""",
                encoding="utf-8",
            )
            fake_carla.chmod(0o755)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "carla",
                    "renderer_execute": True,
                    "renderer_bin": "",
                    "carla_bin": "",
                    "renderer_command": [],
                    "camera_projection_enabled": True,
                    "camera_projection_trajectory_sweep_enabled": True,
                    "camera_projection_trajectory_sweep_frames": 2,
                    "lidar_postprocess_enabled": True,
                    "lidar_trajectory_sweep_enabled": True,
                    "lidar_trajectory_sweep_frames": 2,
                    "radar_postprocess_enabled": True,
                    "radar_trajectory_sweep_enabled": True,
                    "radar_trajectory_sweep_frames": 2,
                    "renderer_sensor_mounts_only_enabled": False,
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            with mock.patch.dict(os.environ, {"CARLA_BIN": str(fake_carla)}, clear=False):
                result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertIn("backend_wrapper_invocation", result.artifacts)
            wrapper_invocation = json.loads(
                result.artifacts["backend_wrapper_invocation"].read_text(encoding="utf-8")
            )
            output_args = wrapper_invocation["output_args"]
            self.assertIn("--ingestion-profile", wrapper_invocation["input_args"])
            self.assertIn("--sensor-bundle-summary", wrapper_invocation["input_args"])
            self.assertEqual(
                wrapper_invocation["sensor_bundle_summary"],
                str(result.artifacts["backend_sensor_bundle_summary"]),
            )
            ingest_indices = [idx for idx, token in enumerate(output_args) if token == "--ingest-frame"]
            self.assertEqual(len(ingest_indices), 6)
            ingest_payloads = [output_args[idx + 1] for idx in ingest_indices]
            self.assertTrue(any(payload.startswith("0:camera:") for payload in ingest_payloads))
            self.assertTrue(any(payload.startswith("0:lidar:") for payload in ingest_payloads))
            self.assertTrue(any(payload.startswith("0:radar:") for payload in ingest_payloads))
            for payload in ingest_payloads:
                parts = payload.split(":", 2)
                self.assertEqual(len(parts), 3)
                self.assertTrue(Path(parts[2]).exists())
            meta_indices = [idx for idx, token in enumerate(output_args) if token == "--ingest-meta"]
            self.assertEqual(len(meta_indices), 3)
            meta_payloads = [output_args[idx + 1] for idx in meta_indices]
            self.assertIn("camera:camera_front:camera_projection_json:ego", meta_payloads)
            self.assertIn("lidar:lidar_top:lidar_points_json:ego", meta_payloads)
            self.assertIn("radar:radar_front:radar_targets_json:ego", meta_payloads)
            self.assertNotIn("--sensor-bundle-summary", output_args)

    def test_renderer_runtime_injects_scene_and_sensor_mount_args_from_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "carla",
                    "renderer_map": "Town06",
                    "renderer_weather": "rain",
                    "renderer_scene_seed": 77,
                    "renderer_ego_actor_id": "ego_01",
                    "renderer_execute": False,
                    "renderer_command": ["echo", "renderer_plan", "{contract}"],
                    "renderer_camera_sensor_id": "cam_front",
                    "renderer_lidar_sensor_id": "lidar_roof",
                    "renderer_radar_sensor_id": "radar_bumper",
                    "renderer_sensor_mounts_only_enabled": False,
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)
            plan = json.loads(
                result.artifacts["renderer_execution_plan"].read_text(encoding="utf-8")
            )
            command = plan["command"]
            self.assertIn("--town", command)
            self.assertIn("Town06", command)
            self.assertIn("--weather", command)
            self.assertIn("rain", command)
            self.assertIn("--seed", command)
            self.assertIn("77", command)
            self.assertIn("--ego-actor-id", command)
            self.assertIn("ego_01", command)

            mount_indices = [idx for idx, token in enumerate(command) if token == "--sensor-mount"]
            self.assertEqual(len(mount_indices), 3)
            mount_payloads = [json.loads(command[idx + 1]) for idx in mount_indices]
            self.assertEqual(mount_payloads[0]["sensor_id"], "cam_front")
            self.assertEqual(mount_payloads[1]["sensor_id"], "lidar_roof")
            self.assertEqual(mount_payloads[2]["sensor_id"], "radar_bumper")
            self.assertGreater(plan["contract_scene_args_count"], 0)
            self.assertGreater(plan["contract_sensor_mount_args_count"], 0)
            preview = plan["backend_args_preview"]
            self.assertEqual(preview["backend"], "carla")
            self.assertEqual(preview["scene"]["map"], "Town06")
            self.assertEqual(len(preview["sensor_mounts"]), 3)
            self.assertEqual(len(preview["sensor_mount_cli_args"]), 6)

    def test_renderer_contract_contains_survey_mapping_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scenario = root / "scenario.json"
            scenario.write_text(
                json.dumps(
                    {
                        "name": "renderer-mapping-scene",
                        "objects": [
                            {
                                "id": "ego",
                                "type": "vehicle",
                                "pose": [0.0, 0.0, 0.0],
                                "waypoints": [[1.0, 0.0, 0.0]],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            request = SensorSimRequest(
                scenario_path=scenario,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "survey_generate_from_scenario": True,
                    "camera_geometry": "pinhole",
                    "camera_distortion": "brown-conrady",
                    "camera_intrinsics": {
                        "fx": 1111.0,
                        "fy": 1112.0,
                        "cx": 640.0,
                        "cy": 360.0,
                        "width": 1280,
                        "height": 720,
                    },
                    "camera_distortion_coeffs": {
                        "k1": 0.01,
                        "k2": 0.001,
                        "p1": 0.0,
                        "p2": 0.0,
                        "k3": 0.0,
                    },
                    "camera_extrinsics": {
                        "enabled": True,
                        "tx": 1.0,
                        "ty": 2.0,
                        "tz": 3.0,
                        "roll_deg": 0.0,
                        "pitch_deg": 1.0,
                        "yaw_deg": 2.0,
                    },
                    "lidar_extrinsics": {
                        "enabled": True,
                        "tx": 4.0,
                        "ty": 5.0,
                        "tz": 6.0,
                        "roll_deg": 1.0,
                        "pitch_deg": 2.0,
                        "yaw_deg": 3.0,
                    },
                    "radar_extrinsics": {
                        "enabled": True,
                        "tx": 7.0,
                        "ty": 8.0,
                        "tz": 9.0,
                        "roll_deg": 2.0,
                        "pitch_deg": 3.0,
                        "yaw_deg": 4.0,
                    },
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": False,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "carla",
                    "renderer_execute": False,
                    "renderer_command": ["echo", "renderer_plan", "{contract}"],
                    "renderer_camera_sensor_id": "cam_front",
                    "renderer_lidar_sensor_id": "lidar_roof",
                    "renderer_radar_sensor_id": "radar_bumper",
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)
            self.assertIn("renderer_playback_contract", result.artifacts)
            self.assertIn("survey_mapping_metadata", result.artifacts)
            self.assertIn("generated_survey", result.artifacts)

            payload = json.loads(
                result.artifacts["renderer_playback_contract"].read_text(encoding="utf-8")
            )
            self.assertIn("survey_mapping", payload)
            self.assertTrue(payload["survey_mapping"]["available"])
            mapping_meta = payload["survey_mapping"]["metadata"]
            self.assertIsInstance(mapping_meta, dict)
            assert isinstance(mapping_meta, dict)
            self.assertEqual(mapping_meta.get("survey_name"), "renderer-mapping-scene")
            self.assertEqual(mapping_meta.get("leg_count"), 2)

            self.assertEqual(
                payload["input_artifacts"]["survey_mapping_metadata"],
                str(result.artifacts["survey_mapping_metadata"]),
            )
            self.assertEqual(
                payload["input_artifacts"]["generated_survey"],
                str(result.artifacts["generated_survey"]),
            )
            self.assertIn("sensor_setup", payload)
            camera_setup = payload["sensor_setup"]["camera"]
            self.assertEqual(camera_setup["geometry_model"], "pinhole")
            self.assertEqual(camera_setup["distortion_model"], "brown-conrady")
            self.assertEqual(camera_setup["intrinsics"]["fx"], 1111.0)
            self.assertEqual(camera_setup["extrinsics_source"], "options")

            lidar_setup = payload["sensor_setup"]["lidar"]
            self.assertEqual(lidar_setup["extrinsics_source"], "options")
            self.assertEqual(lidar_setup["extrinsics"]["tx"], 4.0)
            self.assertFalse(lidar_setup["trajectory_sweep_enabled"])

            radar_setup = payload["sensor_setup"]["radar"]
            self.assertEqual(radar_setup["extrinsics_source"], "options")
            self.assertEqual(radar_setup["extrinsics"]["tx"], 7.0)

            mounts = payload["renderer_sensor_mounts"]
            self.assertEqual(len(mounts), 3)
            self.assertEqual(mounts[0]["sensor_id"], "cam_front")
            self.assertEqual(mounts[1]["sensor_id"], "lidar_roof")
            self.assertEqual(mounts[2]["sensor_id"], "radar_bumper")
            self.assertFalse(mounts[0]["enabled"])
            self.assertFalse(mounts[1]["enabled"])
            self.assertFalse(mounts[2]["enabled"])

    def test_renderer_runtime_writes_backend_frame_inputs_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "carla",
                    "renderer_execute": False,
                    "renderer_command": ["echo", "renderer_plan", "{contract}"],
                    "camera_projection_enabled": True,
                    "camera_projection_trajectory_sweep_enabled": True,
                    "camera_projection_trajectory_sweep_frames": 2,
                    "lidar_postprocess_enabled": True,
                    "lidar_trajectory_sweep_enabled": True,
                    "lidar_trajectory_sweep_frames": 2,
                    "radar_postprocess_enabled": True,
                    "radar_trajectory_sweep_enabled": True,
                    "radar_trajectory_sweep_frames": 2,
                    "renderer_sensor_mounts_only_enabled": False,
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertIn("backend_frame_inputs_manifest", result.artifacts)
            self.assertIn("backend_ingestion_profile", result.artifacts)
            self.assertIn("backend_sensor_bundle_summary", result.artifacts)
            self.assertIn("backend_output_spec", result.artifacts)
            self.assertIn("backend_runner_request", result.artifacts)
            self.assertIn("backend_direct_run_command", result.artifacts)
            self.assertIn("backend_launcher_template", result.artifacts)
            self.assertIn("backend_ingestion_args_sh", result.artifacts)
            manifest = json.loads(
                result.artifacts["backend_frame_inputs_manifest"].read_text(encoding="utf-8")
            )
            ingestion_profile = json.loads(
                result.artifacts["backend_ingestion_profile"].read_text(encoding="utf-8")
            )
            bundle_summary = json.loads(
                result.artifacts["backend_sensor_bundle_summary"].read_text(encoding="utf-8")
            )
            output_spec = json.loads(
                result.artifacts["backend_output_spec"].read_text(encoding="utf-8")
            )
            runner_request = json.loads(
                result.artifacts["backend_runner_request"].read_text(encoding="utf-8")
            )
            launcher_template = json.loads(
                result.artifacts["backend_launcher_template"].read_text(encoding="utf-8")
            )
            backend_invocation = json.loads(
                result.artifacts["backend_invocation"].read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["frame_count"], 2)
            self.assertEqual(len(manifest["frames"]), 2)
            for frame in manifest["frames"]:
                self.assertIn("camera", frame)
                self.assertIn("lidar", frame)
                self.assertIn("radar", frame)
                self.assertTrue(frame["camera"]["available"])
                self.assertTrue(frame["lidar"]["available"])
                self.assertTrue(frame["radar"]["available"])
                self.assertEqual(frame["camera"]["sensor_id"], "camera_front")
                self.assertEqual(frame["lidar"]["sensor_id"], "lidar_top")
                self.assertEqual(frame["radar"]["sensor_id"], "radar_front")
                self.assertEqual(frame["camera"]["data_format"], "camera_projection_json")
                self.assertEqual(frame["lidar"]["data_format"], "lidar_points_json")
                self.assertEqual(frame["radar"]["data_format"], "radar_targets_json")
                self.assertTrue(Path(frame["camera"]["payload_artifact"]).exists())
                self.assertTrue(Path(frame["lidar"]["payload_artifact"]).exists())
                self.assertTrue(Path(frame["radar"]["payload_artifact"]).exists())
                self.assertIn("materialized_payload_artifact", frame["camera"])
                self.assertIn("materialized_payload_artifact", frame["lidar"])
                self.assertIn("materialized_payload_artifact", frame["radar"])

            self.assertEqual(ingestion_profile["backend"], "carla")
            self.assertEqual(ingestion_profile["frame_flag"], "--ingest-frame")
            self.assertEqual(ingestion_profile["meta_flag"], "--ingest-meta")
            self.assertEqual(ingestion_profile["entry_count"], 6)
            self.assertEqual(len(ingestion_profile["entries"]), 6)
            self.assertEqual(bundle_summary["backend"], "carla")
            self.assertEqual(bundle_summary["frame_count"], 2)
            self.assertEqual(bundle_summary["expected_sensors"], ["camera", "lidar", "radar"])
            self.assertEqual(bundle_summary["sensor_frame_counts"]["camera"], 2)
            self.assertEqual(bundle_summary["sensor_frame_counts"]["lidar"], 2)
            self.assertEqual(bundle_summary["sensor_frame_counts"]["radar"], 2)
            self.assertEqual(bundle_summary["complete_frame_count"], 2)
            self.assertEqual(bundle_summary["incomplete_frame_count"], 0)
            self.assertEqual(bundle_summary["availability_patterns"], [{"pattern": "camera+lidar+radar", "count": 2}])
            self.assertEqual(len(bundle_summary["meta_entries"]), 3)
            for frame in bundle_summary["frames"]:
                self.assertTrue(frame["bundle_complete"])
                self.assertEqual(frame["available_sensors"], ["camera", "lidar", "radar"])
                self.assertEqual(frame["missing_sensors"], [])
                self.assertEqual(frame["availability_pattern"], "camera+lidar+radar")
                self.assertEqual(frame["sensor_count"], 3)
                self.assertEqual(
                    frame["sensors"]["camera"]["ingestion"]["frame_flag"],
                    "--ingest-frame",
                )
                self.assertTrue(frame["sensors"]["camera"]["payload_artifact"])
            self.assertEqual(runner_request["backend"], "carla")
            self.assertEqual(runner_request["runner_mode"], "direct_backend")
            self.assertEqual(runner_request["resolved_backend_bin"], "carla")
            self.assertIn("--weather", runner_request["scene_args"])
            self.assertIn("--ingest-frame", runner_request["ingestion_args"])
            self.assertEqual(output_spec["backend"], "carla")
            expected_output_keys = {
                entry["artifact_key"] for entry in output_spec["expected_outputs"]
            }
            self.assertIn("sensor_output_camera_front", expected_output_keys)
            self.assertIn("sensor_output_lidar_top", expected_output_keys)
            self.assertIn("sensor_output_radar_front", expected_output_keys)
            self.assertEqual(output_spec["expected_sensor_output_count"], 3)
            outputs_by_sensor = {
                entry["sensor_id"]: entry for entry in output_spec["expected_outputs_by_sensor"]
            }
            self.assertIn("camera_front", outputs_by_sensor)
            self.assertIn("lidar_top", outputs_by_sensor)
            self.assertIn("radar_front", outputs_by_sensor)
            self.assertEqual(
                outputs_by_sensor["camera_front"]["outputs"][0]["relative_path"],
                "sensor_exports/camera_front/image.json",
            )
            self.assertEqual(
                outputs_by_sensor["camera_front"]["outputs"][0]["backend_filename"],
                "image.json",
            )
            self.assertEqual(
                outputs_by_sensor["camera_front"]["outputs"][0]["modality"],
                "camera",
            )
            self.assertEqual(
                outputs_by_sensor["camera_front"]["outputs"][0]["output_role"],
                "camera_visible",
            )
            self.assertEqual(
                outputs_by_sensor["camera_front"]["outputs"][0]["artifact_type"],
                "carla_camera_rgb_json",
            )
            self.assertEqual(
                outputs_by_sensor["lidar_top"]["outputs"][0]["artifact_type"],
                "carla_lidar_json_point_cloud",
            )
            self.assertEqual(
                outputs_by_sensor["radar_front"]["outputs"][0]["artifact_type"],
                "carla_radar_detections_json",
            )
            self.assertIn(
                "/sensor_exports/carla/camera_front/camera/image.json",
                outputs_by_sensor["camera_front"]["outputs"][0]["path_candidates"][1],
            )
            self.assertEqual(
                runner_request["artifacts"]["backend_output_spec"],
                str(result.artifacts["backend_output_spec"]),
            )
            self.assertEqual(launcher_template["backend"], "carla")
            self.assertEqual(launcher_template["arg_count"], 18)
            self.assertEqual(launcher_template["frame_arg_count"], 12)
            self.assertEqual(launcher_template["meta_arg_count"], 6)
            self.assertIn("--ingest-frame", launcher_template["args"])
            self.assertIn("--ingest-meta", launcher_template["args"])
            self.assertEqual(
                backend_invocation["backend_sensor_bundle_summary"],
                str(result.artifacts["backend_sensor_bundle_summary"]),
            )
            self.assertEqual(
                backend_invocation["backend_output_spec"],
                str(result.artifacts["backend_output_spec"]),
            )
            self.assertEqual(
                backend_invocation["backend_runner_request"],
                str(result.artifacts["backend_runner_request"]),
            )
            self.assertEqual(
                backend_invocation["backend_direct_run_command"],
                str(result.artifacts["backend_direct_run_command"]),
            )
            self.assertEqual(backend_invocation["backend_complete_frame_count"], 2)
            self.assertEqual(
                backend_invocation["backend_launcher_template"],
                str(result.artifacts["backend_launcher_template"]),
            )
            self.assertEqual(backend_invocation["backend_launcher_arg_count"], 18)
            self.assertGreater(backend_invocation["backend_runner_arg_count"], 0)
            shell_artifact = result.artifacts["backend_ingestion_args_sh"]
            self.assertTrue(shell_artifact.exists())
            self.assertTrue(os.access(shell_artifact, os.X_OK))
            shell_text = shell_artifact.read_text(encoding="utf-8")
            self.assertIn("BACKEND_INGEST_ARGS=(", shell_text)
            self.assertIn("--ingest-meta", shell_text)
            self.assertIn("--ingest-frame", shell_text)
            direct_command_text = result.artifacts["backend_direct_run_command"].read_text(
                encoding="utf-8"
            )
            self.assertIn("carla", direct_command_text)
            self.assertIn("--ingest-frame", direct_command_text)

            self.assertEqual(result.metrics.get("renderer_backend_frame_manifest_written"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_frame_count"), 2.0)
            self.assertEqual(result.metrics.get("renderer_backend_sensor_bindings"), 6.0)
            self.assertEqual(
                result.metrics.get("renderer_backend_materialized_frame_payload_count"),
                6.0,
            )
            self.assertEqual(result.metrics.get("renderer_backend_ingestion_profile_written"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_ingestion_entry_count"), 6.0)
            self.assertEqual(result.metrics.get("renderer_backend_bundle_summary_written"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_bundle_frame_count"), 2.0)
            self.assertEqual(result.metrics.get("renderer_backend_bundle_camera_frame_count"), 2.0)
            self.assertEqual(result.metrics.get("renderer_backend_bundle_lidar_frame_count"), 2.0)
            self.assertEqual(result.metrics.get("renderer_backend_bundle_radar_frame_count"), 2.0)
            self.assertEqual(result.metrics.get("renderer_backend_bundle_complete_frame_count"), 2.0)
            self.assertEqual(result.metrics.get("renderer_backend_bundle_incomplete_frame_count"), 0.0)
            self.assertEqual(result.metrics.get("renderer_backend_output_spec_written"), 1.0)
            self.assertGreater(result.metrics.get("renderer_backend_expected_output_count", 0.0), 0.0)
            self.assertEqual(result.metrics.get("renderer_backend_launcher_template_written"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_ingestion_shell_written"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_runner_request_written"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_runner_command_written"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_launcher_arg_count"), 18.0)
            self.assertEqual(result.metrics.get("renderer_backend_launcher_frame_arg_count"), 12.0)
            self.assertEqual(result.metrics.get("renderer_backend_launcher_meta_arg_count"), 6.0)
            self.assertGreater(result.metrics.get("renderer_backend_runner_arg_count", 0.0), 0.0)

    def test_renderer_runtime_backend_frame_manifest_selection_options(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "carla",
                    "renderer_execute": False,
                    "renderer_command": ["echo", "renderer_plan", "{contract}"],
                    "camera_projection_enabled": True,
                    "camera_projection_trajectory_sweep_enabled": True,
                    "camera_projection_trajectory_sweep_frames": 3,
                    "lidar_postprocess_enabled": True,
                    "lidar_trajectory_sweep_enabled": True,
                    "lidar_trajectory_sweep_frames": 3,
                    "radar_postprocess_enabled": True,
                    "radar_trajectory_sweep_enabled": True,
                    "radar_trajectory_sweep_frames": 3,
                    "renderer_sensor_mounts_only_enabled": False,
                    "renderer_backend_frame_start": 1,
                    "renderer_backend_frame_stride": 2,
                    "renderer_backend_max_frames": 1,
                },
            )
            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            manifest = json.loads(
                result.artifacts["backend_frame_inputs_manifest"].read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["selection"]["start"], 1)
            self.assertEqual(manifest["selection"]["stride"], 2)
            self.assertEqual(manifest["selection"]["max_frames"], 1)
            self.assertEqual(manifest["selection"]["selected_indices"], [1])
            self.assertEqual(manifest["frame_count"], 1)
            self.assertEqual(len(manifest["frames"]), 1)
            self.assertEqual(manifest["frames"][0]["renderer_frame_id"], 1)
            ingestion_profile = json.loads(
                result.artifacts["backend_ingestion_profile"].read_text(encoding="utf-8")
            )
            self.assertEqual(ingestion_profile["entry_count"], 3)
            launcher_template = json.loads(
                result.artifacts["backend_launcher_template"].read_text(encoding="utf-8")
            )
            self.assertEqual(launcher_template["arg_count"], 12)
            self.assertEqual(result.metrics.get("renderer_backend_frame_count"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_sensor_bindings"), 3.0)
            self.assertEqual(result.metrics.get("renderer_backend_ingestion_entry_count"), 3.0)
            self.assertEqual(result.metrics.get("renderer_backend_launcher_arg_count"), 12.0)


if __name__ == "__main__":
    unittest.main()
