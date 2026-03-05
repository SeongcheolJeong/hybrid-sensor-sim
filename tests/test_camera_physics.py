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
    CameraExtrinsics,
    CameraIntrinsics,
    project_points_brown_conrady,
    transform_points_world_to_camera,
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

    def test_world_to_camera_transform_identity_when_disabled(self) -> None:
        points = [(1.0, 2.0, 3.0)]
        extrinsics = CameraExtrinsics(enabled=False)
        transformed = transform_points_world_to_camera(points, extrinsics)
        self.assertEqual(transformed, points)

    def test_world_to_camera_transform_yaw_90(self) -> None:
        points = [(1.0, 0.0, 5.0)]
        extrinsics = CameraExtrinsics(enabled=True, yaw_deg=90.0)
        transformed = transform_points_world_to_camera(points, extrinsics)
        x, y, z = transformed[0]
        self.assertAlmostEqual(x, 0.0, places=6)
        self.assertAlmostEqual(y, 1.0, places=6)
        self.assertAlmostEqual(z, 5.0, places=6)


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

    def test_projection_reference_mode_first_point(self) -> None:
        backend = NativePhysicsBackend()
        request = SensorSimRequest(
            scenario_path=Path("/tmp/scenario"),
            output_dir=Path("/tmp/out"),
            options={"camera_reference_mode": "first_point"},
        )
        points = [(10.0, 20.0, 30.0), (13.0, 26.0, 39.0)]
        transformed, ref = backend._apply_projection_reference_frame(points, request)
        self.assertEqual(ref, (10.0, 20.0, 30.0))
        self.assertEqual(transformed[0], (0.0, 0.0, 0.0))
        self.assertEqual(transformed[1], (3.0, 6.0, 9.0))

    def test_projection_reference_explicit_point(self) -> None:
        backend = NativePhysicsBackend()
        request = SensorSimRequest(
            scenario_path=Path("/tmp/scenario"),
            output_dir=Path("/tmp/out"),
            options={"camera_reference_point": [100.0, 200.0, 300.0]},
        )
        points = [(101.0, 198.0, 310.0)]
        transformed, ref = backend._apply_projection_reference_frame(points, request)
        self.assertEqual(ref, (100.0, 200.0, 300.0))
        self.assertEqual(transformed[0], (1.0, -2.0, 10.0))

    def test_projection_reference_mode_first_point_xy_keeps_depth(self) -> None:
        backend = NativePhysicsBackend()
        request = SensorSimRequest(
            scenario_path=Path("/tmp/scenario"),
            output_dir=Path("/tmp/out"),
            options={"camera_reference_mode": "first_point_xy"},
        )
        points = [(10.0, 20.0, 30.0), (13.0, 26.0, 39.0)]
        transformed, ref = backend._apply_projection_reference_frame(points, request)
        self.assertEqual(ref, (10.0, 20.0, 0.0))
        self.assertEqual(transformed[0], (0.0, 0.0, 30.0))
        self.assertEqual(transformed[1], (3.0, 6.0, 39.0))

    def test_camera_extrinsics_parsing(self) -> None:
        backend = NativePhysicsBackend()
        request = SensorSimRequest(
            scenario_path=Path("/tmp/scenario"),
            output_dir=Path("/tmp/out"),
            options={
                "camera_extrinsics": {
                    "enabled": True,
                    "tx": 1.0,
                    "ty": 2.0,
                    "tz": 3.0,
                    "roll_deg": 4.0,
                    "pitch_deg": 5.0,
                    "yaw_deg": 6.0,
                }
            },
        )
        extrinsics = backend._camera_extrinsics_from_options(request)
        self.assertTrue(extrinsics.enabled)
        self.assertEqual(extrinsics.tx, 1.0)
        self.assertEqual(extrinsics.ty, 2.0)
        self.assertEqual(extrinsics.tz, 3.0)
        self.assertEqual(extrinsics.roll_deg, 4.0)
        self.assertEqual(extrinsics.pitch_deg, 5.0)
        self.assertEqual(extrinsics.yaw_deg, 6.0)

    def test_auto_extrinsics_from_trajectory_xy_and_orientation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trajectory = root / "leg000_trajectory.txt"
            trajectory.write_text(
                "100.0 200.0 10.0 0.0 1.0 2.0 90.0\n"
                "110.0 210.0 20.0 1.0 3.0 4.0 91.0\n",
                encoding="utf-8",
            )
            backend = NativePhysicsBackend()
            request = SensorSimRequest(
                scenario_path=Path("/tmp/scenario"),
                output_dir=Path("/tmp/out"),
                options={
                    "camera_extrinsics": {
                        "enabled": True,
                        "tx": 1.0,
                        "ty": 2.0,
                        "tz": 3.0,
                        "roll_deg": 4.0,
                        "pitch_deg": 5.0,
                        "yaw_deg": 6.0,
                    },
                    "camera_extrinsics_auto_from_trajectory": True,
                    "camera_extrinsics_auto_pose": "last",
                    "camera_extrinsics_auto_use_position": "xy",
                    "camera_extrinsics_auto_use_orientation": True,
                },
            )
            base = backend._camera_extrinsics_from_options(request)
            effective, meta = backend._resolve_effective_extrinsics(
                request=request,
                artifacts={"trajectory_primary": trajectory},
                base_extrinsics=base,
                reference_point=None,
            )
            self.assertEqual(meta["source"], "trajectory_auto")
            self.assertAlmostEqual(effective.tx, 110.0)
            self.assertAlmostEqual(effective.ty, 210.0)
            self.assertAlmostEqual(effective.tz, 3.0)
            self.assertAlmostEqual(effective.roll_deg, 3.0)
            self.assertAlmostEqual(effective.pitch_deg, 4.0)
            self.assertAlmostEqual(effective.yaw_deg, 91.0)

    def test_hybrid_generates_trajectory_sweep_preview(self) -> None:
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
cat > "${rootdir}/scan_trajectory.txt" <<EOF
0.0 0.0 0.0 0.0 0.0 0.0 0.0
5.0 0.0 0.0 1.0 0.0 0.0 20.0
10.0 0.0 0.0 2.0 0.0 0.0 45.0
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
                    "camera_projection_trajectory_sweep_enabled": True,
                    "camera_projection_trajectory_sweep_frames": 3,
                    "camera_projection_clamp_to_image": False,
                    "camera_intrinsics": {
                        "fx": 1000.0,
                        "fy": 1000.0,
                        "cx": 960.0,
                        "cy": 540.0,
                        "width": 1920,
                        "height": 1080,
                    },
                    "camera_extrinsics": {
                        "enabled": True,
                        "tx": 0.0,
                        "ty": 0.0,
                        "tz": 0.0,
                        "roll_deg": 0.0,
                        "pitch_deg": 0.0,
                        "yaw_deg": 0.0,
                    },
                    "camera_extrinsics_auto_use_position": "xy",
                    "camera_extrinsics_auto_use_orientation": True,
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)
            self.assertIn("camera_projection_trajectory_sweep", result.artifacts)
            self.assertIn("camera_projection_trajectory_sweep_frame_count", result.metrics)
            self.assertEqual(result.metrics["camera_projection_trajectory_sweep_frame_count"], 3.0)

            sweep_path = result.artifacts["camera_projection_trajectory_sweep"]
            sweep = json.loads(sweep_path.read_text(encoding="utf-8"))
            self.assertEqual(sweep["frame_count"], 3)
            self.assertEqual(len(sweep["frames"]), 3)
            self.assertGreaterEqual(sweep["frames"][0]["output_count"], 0)


if __name__ == "__main__":
    unittest.main()
