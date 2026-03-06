from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
