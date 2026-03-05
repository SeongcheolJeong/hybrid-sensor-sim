from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.backends.helios_adapter import HeliosAdapter
from hybrid_sensor_sim.backends.native_physics import NativePhysicsBackend
from hybrid_sensor_sim.orchestrator import HybridOrchestrator
from hybrid_sensor_sim.physics.camera import (
    BrownConradyDistortion,
    CameraIntrinsics,
    project_points_brown_conrady,
)
from hybrid_sensor_sim.types import BackendMode, SensorSimRequest


class CameraPhysicsTests(unittest.TestCase):
    def test_projection_without_distortion(self) -> None:
        intrinsics = CameraIntrinsics(fx=100.0, fy=100.0, cx=50.0, cy=40.0, width=200, height=200)
        distortion = BrownConradyDistortion()
        points = [(1.0, 2.0, 10.0)]
        projected = project_points_brown_conrady(points, intrinsics, distortion, clamp_to_image=False)
        self.assertEqual(len(projected), 1)
        u, v, z = projected[0]
        self.assertAlmostEqual(u, 60.0, places=6)
        self.assertAlmostEqual(v, 60.0, places=6)
        self.assertAlmostEqual(z, 10.0, places=6)

    def test_projection_with_distortion_changes_result(self) -> None:
        intrinsics = CameraIntrinsics(fx=100.0, fy=100.0, cx=0.0, cy=0.0, width=1000, height=1000)
        no_dist = BrownConradyDistortion()
        with_dist = BrownConradyDistortion(k1=0.1, k2=0.01, p1=0.001, p2=0.002, k3=0.001)
        point = [(2.0, 1.0, 10.0)]

        base = project_points_brown_conrady(point, intrinsics, no_dist, clamp_to_image=False)[0]
        distorted = project_points_brown_conrady(point, intrinsics, with_dist, clamp_to_image=False)[0]

        self.assertNotAlmostEqual(base[0], distorted[0], places=6)
        self.assertNotAlmostEqual(base[1], distorted[1], places=6)


class HybridCameraProjectionTests(unittest.TestCase):
    def test_hybrid_generates_camera_projection_preview_from_xyz(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")

            fake_helios = root / "fake_helios.sh"
            fake_helios.write_text(
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
1.0 0.0 10.0
2.0 1.0 10.0
-1.0 1.0 5.0
EOF
""",
                encoding="utf-8",
            )
            fake_helios.chmod(0o755)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "camera_projection_enabled": True,
                    "camera_intrinsics": {
                        "fx": 1000.0,
                        "fy": 1000.0,
                        "cx": 960.0,
                        "cy": 540.0,
                        "width": 1920,
                        "height": 1080,
                    },
                    "camera_distortion_coeffs": {
                        "k1": 0.01,
                        "k2": 0.001,
                        "p1": 0.0001,
                        "p2": -0.0001,
                        "k3": 0.0001,
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)
            self.assertEqual(result.backend, "hybrid(helios+native_physics)")
            self.assertIn("camera_projection_preview", result.artifacts)
            preview_path = result.artifacts["camera_projection_preview"]
            self.assertTrue(preview_path.exists())
            preview = json.loads(preview_path.read_text(encoding="utf-8"))
            self.assertGreater(preview["input_count"], 0)
            self.assertGreater(preview["output_count"], 0)
            self.assertIn("camera_projection_output_count", result.metrics)


if __name__ == "__main__":
    unittest.main()

