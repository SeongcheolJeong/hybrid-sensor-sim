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
            self.assertEqual(result.metrics.get("renderer_runtime_planned"), 1.0)
            self.assertEqual(result.metrics.get("renderer_execute_requested"), 0.0)
            self.assertEqual(result.metrics.get("renderer_runtime_success"), 1.0)

            contract_path = result.artifacts["renderer_playback_contract"]
            plan = json.loads(
                result.artifacts["renderer_execution_plan"].read_text(encoding="utf-8")
            )
            self.assertFalse(plan["execute"])
            self.assertIn(str(contract_path), plan["command"])

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
            self.assertEqual(
                result.metrics.get("renderer_contract_frame_manifest_args_count"),
                2.0,
            )

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
            self.assertEqual(
                result.metrics.get("renderer_contract_frame_manifest_args_count"),
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
            self.assertEqual(result.metrics.get("renderer_execute_requested"), 1.0)
            self.assertEqual(result.metrics.get("renderer_return_code"), 0.0)
            self.assertEqual(result.metrics.get("renderer_runtime_success"), 1.0)
            stdout = result.artifacts["renderer_stdout"].read_text(encoding="utf-8")
            self.assertIn("renderer_ok", stdout)

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
            self.assertEqual(result.metrics.get("renderer_backend_wrapper_used"), 1.0)
            self.assertEqual(plan["backend_args_preview"]["scene"]["map"], "wrapper_map")

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

            backend_invocation = json.loads(
                result.artifacts["backend_invocation"].read_text(encoding="utf-8")
            )
            self.assertEqual(backend_invocation["command_source"], "backend_wrapper")
            self.assertTrue(backend_invocation["backend_wrapper_used"])

            wrapper_invocation = json.loads(
                result.artifacts["backend_wrapper_invocation"].read_text(encoding="utf-8")
            )
            self.assertEqual(wrapper_invocation["wrapper"], "awsim")
            self.assertIn("--mount-sensor", wrapper_invocation["output_args"])
            self.assertIn("--mount-pose", wrapper_invocation["output_args"])
            self.assertIn("--map", wrapper_invocation["output_args"])
            self.assertIn("Town07", wrapper_invocation["output_args"])
            self.assertIn(
                "cam_front:1.0:2.0:3.0:0.1:0.2:0.3",
                wrapper_invocation["output_args"],
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
            wrapper_invocation = json.loads(
                result.artifacts["backend_wrapper_invocation"].read_text(encoding="utf-8")
            )
            self.assertEqual(wrapper_invocation["wrapper"], "carla")
            self.assertIn("--attach-sensor", wrapper_invocation["output_args"])
            self.assertIn("--sensor-pose", wrapper_invocation["output_args"])
            self.assertIn("camera:cam_front:ego", wrapper_invocation["output_args"])
            self.assertIn(
                "cam_front:0.5:0.0:1.2:0.0:-1.5:0.0",
                wrapper_invocation["output_args"],
            )
            self.assertIn("--town", wrapper_invocation["output_args"])
            self.assertIn("Town03", wrapper_invocation["output_args"])

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
            manifest = json.loads(
                result.artifacts["backend_frame_inputs_manifest"].read_text(encoding="utf-8")
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
                self.assertTrue(Path(frame["camera"]["payload_artifact"]).exists())
                self.assertTrue(Path(frame["lidar"]["payload_artifact"]).exists())
                self.assertTrue(Path(frame["radar"]["payload_artifact"]).exists())
                self.assertIn("materialized_payload_artifact", frame["camera"])
                self.assertIn("materialized_payload_artifact", frame["lidar"])
                self.assertIn("materialized_payload_artifact", frame["radar"])

            self.assertEqual(result.metrics.get("renderer_backend_frame_manifest_written"), 1.0)
            self.assertEqual(result.metrics.get("renderer_backend_frame_count"), 2.0)
            self.assertEqual(result.metrics.get("renderer_backend_sensor_bindings"), 6.0)
            self.assertEqual(
                result.metrics.get("renderer_backend_materialized_frame_payload_count"),
                6.0,
            )


if __name__ == "__main__":
    unittest.main()
