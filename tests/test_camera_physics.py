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

    def test_equidistant_projection_compresses_off_axis_points(self) -> None:
        intrinsics = CameraIntrinsics(fx=100.0, fy=100.0, cx=0.0, cy=0.0, width=1000, height=1000)
        distortion = BrownConradyDistortion()
        point = [(10.0, 0.0, 10.0)]

        rectilinear = project_points_brown_conrady(
            point,
            intrinsics,
            distortion,
            geometry_model="rectilinear",
            clamp_to_image=False,
        )[0]
        equidistant = project_points_brown_conrady(
            point,
            intrinsics,
            distortion,
            geometry_model="equidistant",
            clamp_to_image=False,
        )[0]

        self.assertAlmostEqual(rectilinear[0], 100.0, places=6)
        self.assertAlmostEqual(equidistant[0], 78.5398163397, places=5)
        self.assertLess(equidistant[0], rectilinear[0])
        self.assertAlmostEqual(equidistant[1], 0.0, places=6)

    def test_orthographic_projection_ignores_depth_scaling(self) -> None:
        intrinsics = CameraIntrinsics(fx=100.0, fy=100.0, cx=0.0, cy=0.0, width=1000, height=1000)
        distortion = BrownConradyDistortion()
        points = [(1.0, 2.0, 10.0), (1.0, 2.0, 20.0)]

        projected = project_points_brown_conrady(
            points,
            intrinsics,
            distortion,
            geometry_model="orthographic",
            clamp_to_image=False,
        )

        self.assertEqual(len(projected), 2)
        self.assertAlmostEqual(projected[0][0], 100.0, places=6)
        self.assertAlmostEqual(projected[0][1], 200.0, places=6)
        self.assertAlmostEqual(projected[1][0], projected[0][0], places=6)
        self.assertAlmostEqual(projected[1][1], projected[0][1], places=6)

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
            self.assertEqual(preview["geometry_model"], "pinhole")
            self.assertIn("camera_projection_output_count", result.metrics)

    def test_hybrid_generates_visible_image_signal_samples(self) -> None:
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
0.0 0.0 10.0
1.0 1.0 12.0
2.0 2.0 14.0
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
                    "camera_image_params": {
                        "iso": 120,
                        "shutter_speed": 4000.0,
                        "analog_gain": 1.2,
                        "digital_gain": 1.1,
                        "readout_noise": 0.0,
                        "white_balance": 4500,
                        "gamma": 2.0,
                        "seed": 7,
                        "fixed_pattern_noise": {
                            "dsnu": 0.0,
                            "prnu": 0.0,
                        },
                    },
                    "camera_intrinsics": {
                        "fx": 1000.0,
                        "fy": 1000.0,
                        "cx": 960.0,
                        "cy": 540.0,
                        "width": 1920,
                        "height": 1080,
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            preview = json.loads(
                result.artifacts["camera_projection_preview"].read_text(encoding="utf-8")
            )
            self.assertEqual(preview["sensor_type"], "VISIBLE")
            self.assertEqual(preview["image_chain"]["iso"], 120)
            self.assertAlmostEqual(preview["image_chain"]["white_balance_kelvin"], 4500.0)
            self.assertGreater(len(preview["preview_image_signal_samples"]), 0)
            sample = preview["preview_image_signal_samples"][0]
            self.assertEqual(len(sample["digital_rgb"]), 3)
            self.assertEqual(len(sample["white_balance_gains"]), 3)
            self.assertGreater(sample["signal_photons"], 0.0)
            self.assertGreater(sample["white_balance_gains"][0], sample["white_balance_gains"][2])
            self.assertNotAlmostEqual(
                sample["noisy_signal_rgb_linear"][0],
                sample["noisy_signal_rgb_linear"][2],
                places=6,
            )
            self.assertEqual(result.metrics.get("camera_image_chain_enabled"), 1.0)
            self.assertEqual(result.metrics.get("camera_image_signal_output_count"), 3.0)

    def test_hybrid_applies_lens_vignetting_and_flare_in_visible_preview(self) -> None:
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
0.0 0.0 10.0
5.0 0.0 10.0
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
                    "camera_image_params": {
                        "iso": 120,
                        "shutter_speed": 4000.0,
                        "readout_noise": 0.0,
                        "white_balance": 6500,
                        "seed": 3,
                        "fixed_pattern_noise": {
                            "dsnu": 0.0,
                            "prnu": 0.0,
                        },
                    },
                    "camera_lens_params": {
                        "lens_flare": 1.0,
                        "spot_size": 0.005,
                        "vignetting": {
                            "intensity": 0.8,
                            "alpha": 1.25,
                            "radius": 0.4,
                        },
                    },
                    "camera_intrinsics": {
                        "fx": 1000.0,
                        "fy": 1000.0,
                        "cx": 960.0,
                        "cy": 540.0,
                        "width": 1920,
                        "height": 1080,
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            preview = json.loads(
                result.artifacts["camera_projection_preview"].read_text(encoding="utf-8")
            )
            self.assertEqual(preview["lens_params"]["lens_flare"], 1.0)
            self.assertEqual(preview["lens_params"]["spot_size"], 0.005)
            samples = preview["preview_image_signal_samples"]
            self.assertEqual(len(samples), 2)
            center_sample = samples[0]
            edge_sample = samples[1]
            self.assertGreater(center_sample["vignetting_factor"], edge_sample["vignetting_factor"])
            self.assertGreater(center_sample["lens_flare_strength"], edge_sample["lens_flare_strength"])
            self.assertGreater(center_sample["spot_blur_radius_px"], 0.0)
            self.assertEqual(result.metrics.get("camera_lens_artifact_enabled"), 1.0)

    def test_hybrid_generates_depth_preview_with_rolling_shutter_metadata(self) -> None:
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
2.0 1.0 12.0
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
                    "camera_sensor_type": "DEPTH",
                    "camera_depth_params": {
                        "min": 1.0,
                        "max": 50.0,
                        "type": "LOG",
                        "log_base": 10.0,
                    },
                    "camera_rolling_shutter": {
                        "enabled": True,
                        "row_delay_ns": 1000,
                        "col_delay_ns": 500,
                        "num_time_steps": 10,
                        "num_exposure_samples_per_pixel": 3,
                    },
                    "camera_intrinsics": {
                        "fx": 1000.0,
                        "fy": 1000.0,
                        "cx": 960.0,
                        "cy": 540.0,
                        "width": 1920,
                        "height": 1080,
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            preview = json.loads(
                result.artifacts["camera_projection_preview"].read_text(encoding="utf-8")
            )
            self.assertEqual(preview["sensor_type"], "DEPTH")
            self.assertEqual(preview["output_mode"], "DEPTH")
            self.assertTrue(preview["rolling_shutter"]["enabled"])
            self.assertGreater(preview["rolling_shutter"]["total_readout_s"], 0.0)
            self.assertGreater(len(preview["preview_depth_samples"]), 0)
            self.assertGreater(len(preview["preview_readout_samples"]), 0)
            self.assertEqual(preview["preview_depth_samples"][0]["depth_encoding"], "LOG")
            self.assertIn("camera_depth_output_count", result.metrics)
            self.assertEqual(result.metrics.get("camera_rolling_shutter_enabled"), 1.0)

    def test_hybrid_generates_semantic_preview_with_annotations_and_fallback(self) -> None:
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
0.0 0.0 10.0
1.0 1.0 12.0
8.0 2.0 10.0
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
                    "camera_sensor_type": "SEMANTIC_SEGMENTATION",
                    "camera_semantic_params": {
                        "class_version": "GRANULAR_SEGMENTATION",
                        "include_material_class": True,
                        "include_lane_marking_id": True,
                    },
                    "camera_semantic_point_labels": [
                        {
                            "point_index": 0,
                            "semantic_class_id": 1524,
                            "semantic_class_name": "CROSSWALK",
                            "actor_id": 77,
                            "component_id": 88,
                            "material_class_id": 1200,
                            "lane_marking_id": 321,
                        }
                    ],
                    "camera_intrinsics": {
                        "fx": 1000.0,
                        "fy": 1000.0,
                        "cx": 960.0,
                        "cy": 540.0,
                        "width": 1920,
                        "height": 1080,
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            preview = json.loads(
                result.artifacts["camera_projection_preview"].read_text(encoding="utf-8")
            )
            self.assertEqual(preview["sensor_type"], "SEMANTIC_SEGMENTATION")
            self.assertEqual(preview["semantic_params"]["class_version"], "GRANULAR_SEGMENTATION")
            self.assertGreater(len(preview["preview_semantic_samples"]), 0)
            first_sample = preview["preview_semantic_samples"][0]
            self.assertEqual(first_sample["semantic_class_id"], 1524)
            self.assertEqual(first_sample["semantic_class_name"], "CROSSWALK")
            self.assertEqual(first_sample["actor_id"], 77)
            self.assertEqual(first_sample["component_id"], 88)
            self.assertEqual(first_sample["material_class_id"], 1200)
            self.assertEqual(first_sample["lane_marking_id"], 321)
            self.assertEqual(first_sample["source"], "annotation_override")
            self.assertTrue(any(item["source"] == "heuristic" for item in preview["preview_semantic_samples"][1:]))
            self.assertGreater(len(preview["preview_semantic_legend"]), 0)
            self.assertEqual(result.metrics.get("camera_semantic_output_count"), 3.0)

    def test_hybrid_rolling_shutter_applies_pose_distortion_with_trajectory(self) -> None:
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
5.0 0.0 10.0
EOF
cat > "${rootdir}/scan_trajectory.txt" <<EOF
0.0 0.0 0.0 0.0 0.0 0.0 0.0
2.0 0.0 0.0 1.0 0.0 0.0 0.0
EOF
""",
                encoding="utf-8",
            )
            fake_helios.chmod(0o755)

            base_options = {
                "execute_helios": True,
                "camera_projection_enabled": True,
                "camera_projection_clamp_to_image": False,
                "camera_intrinsics": {
                    "fx": 100.0,
                    "fy": 100.0,
                    "cx": 0.0,
                    "cy": 0.0,
                    "width": 100,
                    "height": 100,
                },
            }

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            no_rs = orchestrator.run(
                SensorSimRequest(
                    scenario_path=survey,
                    output_dir=root / "out_no_rs",
                    options=base_options,
                ),
                BackendMode.HYBRID_AUTO,
            )
            with_rs = orchestrator.run(
                SensorSimRequest(
                    scenario_path=survey,
                    output_dir=root / "out_rs",
                    options={
                        **base_options,
                        "camera_rolling_shutter": {
                            "enabled": True,
                            "row_delay_ns": 0,
                            "col_delay_ns": 10_000_000,
                            "num_time_steps": 1,
                            "num_exposure_samples_per_pixel": 1,
                        },
                    },
                ),
                BackendMode.HYBRID_AUTO,
            )

            self.assertTrue(no_rs.success)
            self.assertTrue(with_rs.success)

            no_rs_preview = json.loads(
                no_rs.artifacts["camera_projection_preview"].read_text(encoding="utf-8")
            )
            with_rs_preview = json.loads(
                with_rs.artifacts["camera_projection_preview"].read_text(encoding="utf-8")
            )
            base_u = no_rs_preview["preview_points_uvz"][0]["u"]
            distorted_u = with_rs_preview["preview_points_uvz"][0]["u"]

            self.assertAlmostEqual(base_u, 50.0, places=4)
            self.assertLess(distorted_u, base_u)
            self.assertTrue(with_rs_preview["rolling_shutter"]["applied"])
            self.assertTrue(with_rs_preview["rolling_shutter"]["trajectory_available"])
            self.assertEqual(with_rs.metrics.get("camera_rolling_shutter_applied"), 1.0)

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
            self.assertEqual(sweep["geometry_model"], "pinhole")
            self.assertEqual(sweep["frame_count"], 3)
            self.assertEqual(len(sweep["frames"]), 3)
            self.assertGreaterEqual(sweep["frames"][0]["output_count"], 0)
            self.assertEqual(sweep["frames"][0]["geometry_model"], "pinhole")

    def test_hybrid_generates_lidar_and_radar_previews(self) -> None:
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
8.0 0.2 0.1
12.0 -0.5 0.3
20.0 1.0 0.0
EOF
cat > "${rootdir}/scan_trajectory.txt" <<EOF
0.0 0.0 0.0 0.0 0.0 0.0 0.0
10.0 0.0 0.0 2.0 0.0 0.0 0.0
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
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": True,
                    "lidar_noise": "gaussian",
                    "lidar_noise_stddev_m": 0.0,
                    "lidar_dropout_probability": 0.0,
                    "radar_postprocess_enabled": True,
                    "radar_max_targets": 8,
                    "radar_range_min_m": 0.5,
                    "radar_range_max_m": 100.0,
                    "radar_false_target_count": 0,
                    "radar_use_ego_velocity_from_trajectory": True,
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)

            self.assertIn("lidar_noisy_preview", result.artifacts)
            lidar_lines = [
                line
                for line in result.artifacts["lidar_noisy_preview"].read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(len(lidar_lines), 3)
            self.assertEqual(result.metrics.get("lidar_output_count"), 3.0)

            self.assertIn("radar_targets_preview", result.artifacts)
            radar_payload = json.loads(
                result.artifacts["radar_targets_preview"].read_text(encoding="utf-8")
            )
            self.assertGreater(radar_payload["target_count"], 0)
            self.assertIn("radial_velocity_mps", radar_payload["targets"][0])
            self.assertNotEqual(radar_payload["targets"][0]["radial_velocity_mps"], 0.0)
            self.assertEqual(result.metrics.get("radar_target_count"), float(radar_payload["target_count"]))

    def test_radar_preview_reports_detectability_accuracy_and_tracks(self) -> None:
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
20.3 0.0 0.0
80.7 0.2 0.0
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
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": True,
                    "radar_clutter": "none",
                    "radar_false_target_count": 0,
                    "radar_max_targets": 8,
                    "radar_point_rcs_dbsm": [12.0, 12.0],
                    "radar_system_params": {
                        "range_quantization": 0.5,
                        "velocity_quantization": 0.1,
                        "range_resolution": 0.4,
                        "velocity_resolution": 0.2,
                    },
                    "radar_detector_params": {
                        "minimum_snr_db": -30.0,
                        "noise_variance_dbw": -95.0,
                        "noise_performance": {
                            "target_detectability": {
                                "probability_detection": 0.95,
                                "target": {
                                    "range": 120.0,
                                    "radar_cross_section": 10.0,
                                },
                            },
                        },
                    },
                    "radar_estimator_params": {
                        "range_accuracy_regions": [
                            {"range": {"min": 50.0, "max": 120.0}, "max_deviation": 1.2}
                        ]
                    },
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
            self.assertIn("radar_targets_preview", result.artifacts)

            preview = json.loads(
                result.artifacts["radar_targets_preview"].read_text(encoding="utf-8")
            )
            self.assertEqual(preview["output_mode"], "TRACKS")
            self.assertGreaterEqual(preview["track_count"], 1)
            self.assertEqual(result.metrics.get("radar_track_output_enabled"), 1.0)
            self.assertEqual(result.metrics.get("radar_track_count"), float(preview["track_count"]))

            self.assertGreaterEqual(preview["target_count"], 2)
            targets = preview["targets"]
            self.assertIn("snr_db", targets[0])
            self.assertIn("detection_probability", targets[0])
            self.assertIn("antenna_gain_db", targets[0])
            self.assertTrue(any(target["range_accuracy_region_index"] == 0 for target in targets))
            self.assertTrue(all(abs(target["range_m"] * 2.0 - round(target["range_m"] * 2.0)) < 1e-6 for target in targets))
            self.assertGreater(targets[0]["detection_probability"], targets[-1]["detection_probability"])
            self.assertIn("confidence", preview["tracks"][0])
            self.assertIn("source_target_id", preview["tracks"][0])

    def test_radar_preview_reports_multipath_and_micro_doppler(self) -> None:
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
40.0 0.0 0.0
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
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": True,
                    "radar_clutter": "none",
                    "radar_false_target_count": 0,
                    "radar_max_targets": 8,
                    "radar_point_rcs_dbsm": [16.0],
                    "radar_detector_params": {
                        "minimum_snr_db": -40.0,
                        "noise_variance_dbw": -95.0,
                        "noise_performance": {
                            "target_detectability": {
                                "probability_detection": 0.99,
                                "target": {
                                    "range": 200.0,
                                    "radar_cross_section": 10.0,
                                },
                            },
                        },
                    },
                    "radar_fidelity": {
                        "multipath": True,
                        "multipath_bounces": 2,
                        "coherence_factor": 0.15,
                        "enable_micro_doppler": True,
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)

            preview = json.loads(
                result.artifacts["radar_targets_preview"].read_text(encoding="utf-8")
            )
            self.assertGreater(preview["multipath_target_count"], 0)
            multipath_targets = [
                target
                for target in preview["targets"]
                if target["measurement_source"] == "MULTIPATH"
            ]
            self.assertTrue(multipath_targets)
            self.assertEqual(multipath_targets[0]["ground_truth_detection_type"], "MULTIPATH")
            self.assertGreater(multipath_targets[0]["path_length_offset_m"], 0.0)
            self.assertGreaterEqual(multipath_targets[0]["ground_truth_hit_index"], 1)
            self.assertIn(multipath_targets[0]["multipath_surface"], {"GROUND_PLANE", "VERTICAL_PLANE"})
            self.assertNotEqual(multipath_targets[0]["micro_doppler_velocity_offset_mps"], 0.0)
            self.assertEqual(result.metrics.get("radar_multipath_enabled"), 1.0)
            self.assertGreater(result.metrics.get("radar_multipath_target_count"), 0.0)
            self.assertEqual(result.metrics.get("radar_micro_doppler_enabled"), 1.0)
            self.assertGreater(result.metrics.get("radar_micro_doppler_target_count"), 0.0)

    def test_radar_preview_applies_directivity_and_adaptive_sampling(self) -> None:
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
20.0 0.0 0.0
19.7 3.5 0.0
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
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": True,
                    "radar_clutter": "none",
                    "radar_false_target_count": 0,
                    "radar_max_targets": 8,
                    "radar_point_rcs_dbsm": [10.0, 10.0],
                    "radar_point_actor_ids": ["1", "bike_7"],
                    "radar_detector_params": {
                        "minimum_snr_db": -40.0,
                        "noise_variance_dbw": -95.0,
                        "noise_performance": {
                            "target_detectability": {
                                "probability_detection": 0.95,
                                "target": {
                                    "range": 80.0,
                                    "radar_cross_section": 10.0,
                                },
                            },
                        },
                    },
                    "radar_antenna_params": {
                        "antenna_definitions": [
                            {
                                "type": "FROM_DIRECTIVITY_AZ_EL_CUTS",
                                "directivity_az_el_cuts": {
                                    "az": {
                                        "type": "FROM_DIRECTIVITY_TABLE_CUT",
                                        "directivity_table_cut": {
                                            "angles": [-0.2, 0.0, 0.2],
                                            "amplitudes": [0.1, 0.3, 1.0],
                                        },
                                    },
                                    "el": {
                                        "type": "FROM_DIRECTIVITY_TABLE_CUT",
                                        "directivity_table_cut": {
                                            "angles": [-0.2, 0.0, 0.2],
                                            "amplitudes": [1.0, 1.0, 1.0],
                                        },
                                    },
                                },
                            }
                        ]
                    },
                    "radar_fidelity": {
                        "raytracing": {
                            "mode": "hardware",
                            "adaptive_sampling_params": {
                                "default_min_rays_per_wavelength": 0.1,
                                "max_subdivision_level": 4,
                                "targets": [
                                    {"actor_id": "bike_7", "min_rays_per_wavelength": 0.8},
                                ],
                            },
                        },
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)

            preview = json.loads(
                result.artifacts["radar_targets_preview"].read_text(encoding="utf-8")
            )
            targets_by_actor = {
                target["adaptive_sampling_actor_id"]: target for target in preview["targets"]
            }
            self.assertIn("1", targets_by_actor)
            self.assertIn("bike_7", targets_by_actor)
            base_target = targets_by_actor["1"]
            boosted_target = targets_by_actor["bike_7"]
            self.assertGreater(boosted_target["antenna_gain_db"], base_target["antenna_gain_db"])
            self.assertGreater(
                boosted_target["adaptive_sampling_density"],
                base_target["adaptive_sampling_density"],
            )
            self.assertTrue(boosted_target["adaptive_sampling_target_override"])
            self.assertGreater(boosted_target["sampling_gain_db"], base_target["sampling_gain_db"])
            self.assertGreater(boosted_target["detection_probability"], base_target["detection_probability"])
            self.assertGreaterEqual(boosted_target["raytracing_subdivision_level"], 1)
            self.assertEqual(boosted_target["raytracing_mode"], "HARDWARE")
            self.assertEqual(result.metrics.get("radar_directivity_model_applied"), 1.0)
            self.assertEqual(result.metrics.get("radar_adaptive_sampling_enabled"), 1.0)
            self.assertGreater(result.metrics.get("radar_adaptive_sampling_target_count"), 0.0)

    def test_lidar_trajectory_sweep_applies_motion_compensation(self) -> None:
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
10.0 0.0 0.0
10.0 0.0 0.0
10.0 0.0 0.0
EOF
cat > "${rootdir}/scan_trajectory.txt" <<EOF
0.0 0.0 0.0 0.0 0.0 0.0 0.0
2.0 0.0 0.0 1.0 0.0 0.0 0.0
6.0 0.0 0.0 2.0 0.0 0.0 0.0
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
                    "camera_projection_enabled": False,
                    "radar_postprocess_enabled": False,
                    "lidar_postprocess_enabled": True,
                    "lidar_trajectory_sweep_enabled": True,
                    "lidar_trajectory_sweep_frames": 3,
                    "lidar_preview_points_per_frame": 8,
                    "lidar_motion_compensation_enabled": True,
                    "lidar_motion_compensation_mode": "linear",
                    "lidar_scan_duration_s": 0.2,
                    "lidar_noise": "none",
                    "lidar_dropout_probability": 0.0,
                    "lidar_extrinsics": {
                        "enabled": True,
                        "tx": 0.0,
                        "ty": 0.0,
                        "tz": 0.0,
                        "roll_deg": 0.0,
                        "pitch_deg": 0.0,
                        "yaw_deg": 0.0,
                    },
                    "lidar_extrinsics_auto_use_position": "none",
                    "lidar_extrinsics_auto_use_orientation": False,
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)
            self.assertIn("lidar_trajectory_sweep", result.artifacts)
            self.assertEqual(result.metrics.get("lidar_trajectory_sweep_frame_count"), 3.0)
            self.assertEqual(result.metrics.get("lidar_motion_compensation_applied"), 1.0)

            payload = json.loads(result.artifacts["lidar_trajectory_sweep"].read_text(encoding="utf-8"))
            self.assertEqual(payload["frame_count"], 3)
            self.assertEqual(len(payload["frames"]), 3)
            x0 = payload["frames"][0]["preview_points_xyz"][0]["x"]
            x1 = payload["frames"][1]["preview_points_xyz"][0]["x"]
            x2 = payload["frames"][2]["preview_points_xyz"][0]["x"]
            self.assertAlmostEqual(x0, 10.2, places=6)
            self.assertAlmostEqual(x1, 10.3, places=6)
            self.assertAlmostEqual(x2, 10.4, places=6)

    def test_lidar_scan_model_filters_points_by_source_angles_and_path(self) -> None:
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
10.0 0.0 0.0
10.0 10.0 0.0
10.0 0.0 1.7633
10.0 0.0 5.0
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
                    "camera_projection_enabled": False,
                    "radar_postprocess_enabled": False,
                    "lidar_postprocess_enabled": True,
                    "lidar_noise": "none",
                    "lidar_dropout_probability": 0.0,
                    "lidar_scan_type": "CUSTOM",
                    "lidar_source_angles": [-10.0, 0.0, 10.0],
                    "lidar_source_angle_tolerance_deg": 1.0,
                    "lidar_scan_field": {
                        "azimuth_min_deg": -5.0,
                        "azimuth_max_deg": 50.0,
                        "elevation_min_deg": -12.0,
                        "elevation_max_deg": 12.0,
                    },
                    "lidar_scan_path": [0.0, 45.0],
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertEqual(result.metrics.get("lidar_scan_model_applied"), 1.0)
            self.assertEqual(result.metrics.get("lidar_output_count"), 3.0)
            self.assertIn("lidar_noisy_preview_json", result.artifacts)
            preview = json.loads(
                result.artifacts["lidar_noisy_preview_json"].read_text(encoding="utf-8")
            )
            self.assertTrue(preview["scan_model_applied"])
            self.assertEqual(preview["source_angles_deg"], [-10.0, 0.0, 10.0])
            self.assertEqual(preview["scan_path_deg"], [0.0, 45.0])
            self.assertEqual(preview["output_count"], 3)
            self.assertEqual(len(preview["preview_points"]), 3)
            self.assertEqual(preview["preview_points"][0]["channel_id"], 1)
            self.assertEqual(preview["preview_points"][0]["scan_path_index"], 0)
            self.assertEqual(preview["preview_points"][1]["scan_path_index"], 1)
            self.assertEqual(preview["preview_points"][2]["channel_id"], 2)

    def test_lidar_emitter_params_affect_channel_signal_and_direction(self) -> None:
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
10.0 0.0 -0.8749
10.0 0.0 0.8749
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
                    "camera_projection_enabled": False,
                    "radar_postprocess_enabled": False,
                    "lidar_postprocess_enabled": True,
                    "lidar_noise": "none",
                    "lidar_dropout_probability": 0.0,
                    "lidar_ground_truth_reflectivity": 1.0,
                    "lidar_intensity": {
                        "units": "SNR",
                    },
                    "lidar_source_angles": [-5.0, 5.0],
                    "lidar_source_angle_tolerance_deg": 0.6,
                    "lidar_emitter_params": {
                        "source_losses": [-6.0, 0.0],
                        "global_source_loss": -1.0,
                        "source_divergence": {"az": 0.003, "el": 0.002},
                        "source_variance": {"az": 0.0004, "el": 0.0},
                        "peak_power": 2.0,
                        "optical_loss": [
                            {"range": 0.0, "loss": 0.0},
                            {"range": 20.0, "loss": -3.0},
                        ],
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertEqual(result.metrics.get("lidar_emitter_model_applied"), 1.0)
            preview = json.loads(
                result.artifacts["lidar_noisy_preview_json"].read_text(encoding="utf-8")
            )
            self.assertEqual(preview["emitter_params"]["source_losses"], [-6.0, 0.0])
            channel_points = sorted(preview["preview_points"], key=lambda point: point["channel_id"])
            self.assertEqual([point["channel_id"] for point in channel_points], [0, 1])
            self.assertLess(channel_points[0]["snr_db"], channel_points[1]["snr_db"])
            self.assertLess(channel_points[0]["intensity"], channel_points[1]["intensity"])
            self.assertAlmostEqual(channel_points[0]["channel_loss_db"], -7.0, places=6)
            self.assertAlmostEqual(channel_points[1]["channel_loss_db"], -1.0, places=6)
            self.assertGreater(channel_points[0]["beam_footprint_area_m2"], 0.0)
            self.assertGreater(channel_points[1]["beam_footprint_area_m2"], 0.0)
            self.assertNotEqual(channel_points[0]["beam_azimuth_offset_deg"], 0.0)

    def test_lidar_channel_profile_generates_sidelobe_returns(self) -> None:
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
10.0 0.0 0.0
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
                    "camera_projection_enabled": False,
                    "radar_postprocess_enabled": False,
                    "lidar_postprocess_enabled": True,
                    "lidar_noise": "none",
                    "lidar_dropout_probability": 0.0,
                    "lidar_ground_truth_reflectivity": 1.0,
                    "lidar_physics_model": {
                        "reflectivity_coefficient": 1.0,
                        "ambient_power_dbw": -30.0,
                        "signal_photon_scale": 10000.0,
                        "ambient_photon_scale": 1000.0,
                        "minimum_detection_snr_db": 0.0,
                        "return_all_hits": True,
                    },
                    "lidar_return_model": {
                        "mode": "MULTI",
                        "max_returns": 5,
                        "selection_mode": "FIRST",
                        "signal_decay": 0.0,
                    },
                    "lidar_shared_channel_profile": {
                        "profile_data": {
                            "file_uri": "scenario://workspace/model_data/cross_detector_profile_1024.exr",
                            "half_angle": 0.05,
                            "scale": 4.0,
                            "sample_count": 4,
                            "sidelobe_gain": 0.08,
                        }
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertEqual(result.metrics.get("lidar_channel_profile_applied"), 1.0)
            self.assertEqual(result.metrics.get("lidar_sidelobe_return_count"), 4.0)
            preview = json.loads(
                result.artifacts["lidar_noisy_preview_json"].read_text(encoding="utf-8")
            )
            self.assertTrue(preview["channel_profile"]["enabled"])
            self.assertEqual(preview["channel_profile"]["profile_data"]["pattern"], "CROSS")
            self.assertEqual(preview["output_count"], 5)
            sidelobe_points = [
                point
                for point in preview["preview_points"]
                if point["ground_truth_detection_type"] == "SIDELOBE"
            ]
            self.assertEqual(len(sidelobe_points), 4)
            self.assertTrue(
                all(point["channel_profile_pattern"] == "CROSS" for point in sidelobe_points)
            )
            self.assertTrue(
                all(point["channel_profile_file_uri"].endswith("cross_detector_profile_1024.exr") for point in sidelobe_points)
            )
            self.assertTrue(
                all(point["channel_profile_scale"] == 4.0 for point in sidelobe_points)
            )
            self.assertTrue(
                any(abs(point["channel_profile_offset_az_deg"]) > 0.0 for point in sidelobe_points)
            )
            self.assertTrue(
                any(abs(point["channel_profile_offset_el_deg"]) > 0.0 for point in sidelobe_points)
            )
            self.assertTrue(
                all(point["snr_db"] < preview["preview_points"][0]["snr_db"] for point in sidelobe_points)
            )

    def test_lidar_channel_profile_loads_exr_sidecar_profile_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            model_data = root / "model_data"
            model_data.mkdir(parents=True, exist_ok=True)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            (model_data / "custom_profile.exr.json").write_text(
                json.dumps(
                    {
                        "weights": [
                            [0.0, 0.0, 0.0],
                            [0.0, 0.0, 1.0],
                            [0.0, 0.0, 0.0],
                        ]
                    }
                ),
                encoding="utf-8",
            )

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
10.0 0.0 0.0
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
                    "camera_projection_enabled": False,
                    "radar_postprocess_enabled": False,
                    "lidar_postprocess_enabled": True,
                    "lidar_noise": "none",
                    "lidar_dropout_probability": 0.0,
                    "lidar_ground_truth_reflectivity": 1.0,
                    "lidar_physics_model": {
                        "reflectivity_coefficient": 1.0,
                        "ambient_power_dbw": -30.0,
                        "signal_photon_scale": 10000.0,
                        "ambient_photon_scale": 1000.0,
                        "minimum_detection_snr_db": 0.0,
                        "return_all_hits": True,
                    },
                    "lidar_return_model": {
                        "mode": "DUAL",
                        "max_returns": 2,
                        "selection_mode": "FIRST",
                        "signal_decay": 0.0,
                    },
                    "lidar_shared_channel_profile": {
                        "profile_data": {
                            "file_uri": "scenario://workspace/model_data/custom_profile.exr",
                            "half_angle": 0.07,
                            "scale": 3.0,
                            "sample_count": 1,
                            "sidelobe_gain": 0.1,
                        }
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertEqual(result.metrics.get("lidar_channel_profile_applied"), 1.0)
            self.assertEqual(result.metrics.get("lidar_channel_profile_file_loaded"), 1.0)
            self.assertEqual(result.metrics.get("lidar_channel_profile_sidecar_used"), 1.0)
            preview = json.loads(
                result.artifacts["lidar_noisy_preview_json"].read_text(encoding="utf-8")
            )
            self.assertEqual(preview["output_count"], 2)
            sidelobe_points = [
                point
                for point in preview["preview_points"]
                if point["ground_truth_detection_type"] == "SIDELOBE"
            ]
            self.assertEqual(len(sidelobe_points), 1)
            sidelobe_point = sidelobe_points[0]
            self.assertEqual(sidelobe_point["channel_profile_pattern"], "FILE")
            self.assertEqual(sidelobe_point["channel_profile_source"], "FILE_JSON_SIDECAR")
            self.assertTrue(
                sidelobe_point["channel_profile_resolved_path"].endswith("custom_profile.exr.json")
            )
            self.assertGreater(sidelobe_point["channel_profile_offset_az_deg"], 0.0)
            self.assertAlmostEqual(sidelobe_point["channel_profile_offset_el_deg"], 0.0, places=6)

    def test_lidar_trajectory_sweep_uses_multi_scan_path_per_frame(self) -> None:
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
10.0 0.0 0.0
0.0 10.0 0.0
EOF
cat > "${rootdir}/scan_trajectory.txt" <<EOF
0.0 0.0 0.0 0.0 0.0 0.0 0.0
1.0 0.0 0.0 1.0 0.0 0.0 0.0
2.0 0.0 0.0 2.0 0.0 0.0 0.0
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
                    "camera_projection_enabled": False,
                    "radar_postprocess_enabled": False,
                    "lidar_postprocess_enabled": True,
                    "lidar_trajectory_sweep_enabled": True,
                    "lidar_trajectory_sweep_frames": 2,
                    "lidar_noise": "none",
                    "lidar_dropout_probability": 0.0,
                    "lidar_scan_type": "CUSTOM",
                    "lidar_multi_scan_path": [[0.0], [90.0]],
                    "lidar_extrinsics": {
                        "enabled": True,
                        "tx": 0.0,
                        "ty": 0.0,
                        "tz": 0.0,
                        "roll_deg": 0.0,
                        "pitch_deg": 0.0,
                        "yaw_deg": 0.0,
                    },
                    "lidar_extrinsics_auto_use_position": "none",
                    "lidar_extrinsics_auto_use_orientation": False,
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            payload = json.loads(result.artifacts["lidar_trajectory_sweep"].read_text(encoding="utf-8"))
            self.assertEqual(payload["frame_count"], 2)
            self.assertEqual(payload["frames"][0]["output_count"], 1)
            self.assertEqual(payload["frames"][1]["output_count"], 1)
            self.assertEqual(payload["frames"][0]["scan_path_deg"], [0.0])
            self.assertEqual(payload["frames"][1]["scan_path_deg"], [90.0])
            self.assertEqual(payload["frames"][0]["preview_points"][0]["scan_path_index"], 0)
            self.assertEqual(payload["frames"][1]["preview_points"][0]["scan_path_index"], 0)

    def test_lidar_preview_emits_intensity_and_snr_metadata(self) -> None:
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
5.0 0.0 0.0
15.0 0.0 0.0
50.0 0.0 0.0
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
                    "camera_projection_enabled": False,
                    "radar_postprocess_enabled": False,
                    "lidar_postprocess_enabled": True,
                    "lidar_noise": "none",
                    "lidar_dropout_probability": 0.0,
                    "lidar_intensity": {
                        "units": "SNR_SCALED",
                        "range": {"min": 0.0, "max": 20.0},
                        "scale": {"min": 0.0, "max": 255.0},
                    },
                    "lidar_physics_model": {
                        "reflectivity_coefficient": 0.5,
                        "ambient_power_dbw": -30.0,
                        "signal_photon_scale": 10000.0,
                        "ambient_photon_scale": 1000.0,
                        "minimum_detection_snr_db": 0.0,
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertEqual(result.metrics.get("lidar_output_count"), 2.0)
            self.assertEqual(result.metrics.get("lidar_intensity_output_count"), 2.0)
            preview = json.loads(
                result.artifacts["lidar_noisy_preview_json"].read_text(encoding="utf-8")
            )
            self.assertEqual(preview["intensity"]["units"], "SNR_SCALED")
            self.assertAlmostEqual(
                preview["physics_model"]["reflectivity_coefficient"],
                0.5,
            )
            self.assertEqual(preview["output_count"], 2)
            self.assertEqual(len(preview["preview_points"]), 2)
            near_point = preview["preview_points"][0]
            mid_point = preview["preview_points"][1]
            self.assertEqual(near_point["intensity_units"], "SNR_SCALED")
            self.assertGreater(near_point["snr_db"], mid_point["snr_db"])
            self.assertGreater(near_point["intensity"], mid_point["intensity"])
            self.assertGreater(near_point["signal_photons"], mid_point["signal_photons"])
            self.assertAlmostEqual(near_point["ground_truth_reflectivity"], 0.35)
            self.assertEqual(near_point["return_id"], 0)

    def test_lidar_preview_generates_multi_returns(self) -> None:
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
10.0 0.0 0.0
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
                    "camera_projection_enabled": False,
                    "radar_postprocess_enabled": False,
                    "lidar_postprocess_enabled": True,
                    "lidar_noise": "none",
                    "lidar_dropout_probability": 0.0,
                    "lidar_ground_truth_reflectivity": 1.0,
                    "lidar_physics_model": {
                        "reflectivity_coefficient": 1.0,
                        "ambient_power_dbw": -30.0,
                        "signal_photon_scale": 10000.0,
                        "ambient_photon_scale": 1000.0,
                        "minimum_detection_snr_db": 0.0,
                    },
                    "lidar_return_model": {
                        "mode": "DUAL",
                        "max_returns": 2,
                        "range_separation_m": 1.0,
                        "signal_decay": 0.5,
                        "minimum_secondary_snr_db": 0.0,
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertEqual(result.metrics.get("lidar_output_count"), 2.0)
            self.assertEqual(result.metrics.get("lidar_secondary_return_count"), 1.0)
            self.assertEqual(result.metrics.get("lidar_multi_return_applied"), 1.0)
            preview = json.loads(
                result.artifacts["lidar_noisy_preview_json"].read_text(encoding="utf-8")
            )
            self.assertEqual(preview["return_model"]["mode"], "DUAL")
            self.assertEqual(preview["output_count"], 2)
            self.assertEqual([point["return_id"] for point in preview["preview_points"]], [0, 1])
            primary_point = preview["preview_points"][0]
            secondary_point = preview["preview_points"][1]
            self.assertGreater(secondary_point["range_m"], primary_point["range_m"])
            self.assertGreater(secondary_point["path_length_offset_m"], 0.0)
            self.assertEqual(secondary_point["ground_truth_hit_index"], 1)
            self.assertEqual(secondary_point["ground_truth_last_bounce_index"], 2)
            self.assertEqual(secondary_point["ground_truth_detection_type"], "RETROREFLECTION")
            self.assertLess(secondary_point["snr_db"], primary_point["snr_db"])

    def test_lidar_preview_generates_geometry_multipath_returns(self) -> None:
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
8.0 1.0 -0.5
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
                    "camera_projection_enabled": False,
                    "radar_postprocess_enabled": False,
                    "lidar_postprocess_enabled": True,
                    "lidar_noise": "none",
                    "lidar_dropout_probability": 0.0,
                    "lidar_ground_truth_reflectivity": 1.0,
                    "lidar_physics_model": {
                        "reflectivity_coefficient": 1.0,
                        "ambient_power_dbw": -30.0,
                        "signal_photon_scale": 10000.0,
                        "ambient_photon_scale": 1000.0,
                        "minimum_detection_snr_db": 0.0,
                        "return_all_hits": True,
                    },
                    "lidar_return_model": {
                        "mode": "MULTI",
                        "max_returns": 3,
                        "selection_mode": "FIRST",
                        "signal_decay": 0.0,
                    },
                    "lidar_multipath_model": {
                        "enabled": True,
                        "mode": "HYBRID",
                        "max_paths": 2,
                        "path_signal_decay": 0.4,
                        "minimum_path_snr_db": 0.0,
                        "max_extra_path_length_m": 20.0,
                        "ground_plane_height_m": -1.5,
                        "ground_reflectivity": 0.5,
                        "wall_plane_x_m": 12.0,
                        "wall_reflectivity": 0.35,
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertEqual(result.metrics.get("lidar_multipath_return_count"), 2.0)
            self.assertEqual(result.metrics.get("lidar_geometry_multipath_applied"), 1.0)
            preview = json.loads(
                result.artifacts["lidar_noisy_preview_json"].read_text(encoding="utf-8")
            )
            self.assertTrue(preview["multipath_model"]["enabled"])
            self.assertEqual(preview["output_count"], 3)
            self.assertEqual([point["return_id"] for point in preview["preview_points"]], [0, 1, 2])
            multipath_points = [
                point
                for point in preview["preview_points"]
                if point["ground_truth_detection_type"] == "MULTIPATH"
            ]
            self.assertEqual(len(multipath_points), 2)
            surfaces = {point["multipath_surface"] for point in multipath_points}
            self.assertEqual(surfaces, {"GROUND_PLANE", "VERTICAL_PLANE"})
            ground_point = next(
                point for point in multipath_points if point["multipath_surface"] == "GROUND_PLANE"
            )
            wall_point = next(
                point for point in multipath_points if point["multipath_surface"] == "VERTICAL_PLANE"
            )
            self.assertGreater(ground_point["range_m"], preview["preview_points"][0]["range_m"])
            self.assertGreater(wall_point["range_m"], ground_point["range_m"])
            self.assertGreater(ground_point["path_length_offset_m"], 0.0)
            self.assertGreater(wall_point["path_length_offset_m"], ground_point["path_length_offset_m"])
            self.assertAlmostEqual(
                ground_point["multipath_reflection_point"]["z"],
                -1.5,
                places=6,
            )
            self.assertAlmostEqual(
                wall_point["multipath_reflection_point"]["x"],
                12.0,
                places=6,
            )

    def test_lidar_return_selection_mode_last_orders_farthest_first(self) -> None:
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
10.0 0.0 0.0
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
                    "camera_projection_enabled": False,
                    "radar_postprocess_enabled": False,
                    "lidar_postprocess_enabled": True,
                    "lidar_noise": "none",
                    "lidar_dropout_probability": 0.0,
                    "lidar_ground_truth_reflectivity": 1.0,
                    "lidar_physics_model": {
                        "reflectivity_coefficient": 1.0,
                        "ambient_power_dbw": -30.0,
                        "signal_photon_scale": 10000.0,
                        "ambient_photon_scale": 1000.0,
                        "minimum_detection_snr_db": 0.0,
                        "return_all_hits": True,
                    },
                    "lidar_return_model": {
                        "mode": "DUAL",
                        "max_returns": 2,
                        "selection_mode": "LAST",
                        "range_separation_m": 1.0,
                        "signal_decay": 0.5,
                        "minimum_secondary_snr_db": 0.0,
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            preview = json.loads(
                result.artifacts["lidar_noisy_preview_json"].read_text(encoding="utf-8")
            )
            self.assertEqual(preview["return_model"]["selection_mode"], "LAST")
            self.assertEqual(preview["output_count"], 2)
            self.assertEqual(
                [point["ground_truth_detection_type"] for point in preview["preview_points"]],
                ["RETROREFLECTION", "TARGET"],
            )
            self.assertGreater(
                preview["preview_points"][0]["range_m"],
                preview["preview_points"][1]["range_m"],
            )

    def test_lidar_range_discrimination_merges_close_returns(self) -> None:
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
10.0 0.0 0.0
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
                    "camera_projection_enabled": False,
                    "radar_postprocess_enabled": False,
                    "lidar_postprocess_enabled": True,
                    "lidar_noise": "none",
                    "lidar_dropout_probability": 0.0,
                    "lidar_ground_truth_reflectivity": 1.0,
                    "lidar_physics_model": {
                        "reflectivity_coefficient": 1.0,
                        "ambient_power_dbw": -30.0,
                        "signal_photon_scale": 10000.0,
                        "ambient_photon_scale": 1000.0,
                        "minimum_detection_snr_db": 0.0,
                        "return_all_hits": True,
                    },
                    "lidar_return_model": {
                        "mode": "DUAL",
                        "max_returns": 2,
                        "selection_mode": "FIRST",
                        "range_discrimination": 1.0,
                        "range_separation_m": 0.4,
                        "signal_decay": 0.5,
                        "minimum_secondary_snr_db": 0.0,
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)

            self.assertTrue(result.success)
            self.assertEqual(result.metrics.get("lidar_range_discrimination_merge_count"), 1.0)
            preview = json.loads(
                result.artifacts["lidar_noisy_preview_json"].read_text(encoding="utf-8")
            )
            self.assertAlmostEqual(preview["return_model"]["range_discrimination"], 1.0)
            self.assertEqual(preview["output_count"], 1)
            merged_point = preview["preview_points"][0]
            self.assertEqual(merged_point["merged_return_count"], 2)
            self.assertEqual(merged_point["ground_truth_detection_type"], "TARGET")
            self.assertGreater(merged_point["range_m"], 10.0)
            self.assertLess(merged_point["range_m"], 10.4)

    def test_lidar_weather_model_reduces_snr_and_adds_noise_points(self) -> None:
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
20.0 0.0 0.0
EOF
""",
                encoding="utf-8",
            )
            fake_helios.chmod(0o755)

            base_options = {
                "execute_helios": True,
                "camera_projection_enabled": False,
                "radar_postprocess_enabled": False,
                "lidar_postprocess_enabled": True,
                "lidar_noise": "none",
                "lidar_dropout_probability": 0.0,
                "lidar_ground_truth_reflectivity": 1.0,
                "lidar_physics_model": {
                    "reflectivity_coefficient": 1.0,
                    "ambient_power_dbw": -30.0,
                    "signal_photon_scale": 10000.0,
                    "ambient_photon_scale": 1000.0,
                    "minimum_detection_snr_db": -20.0,
                },
                "lidar_return_model": {
                    "mode": "DUAL",
                    "max_returns": 2,
                    "selection_mode": "FIRST",
                },
            }
            clear_request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "clear_out",
                options=base_options,
            )
            rain_request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "rain_out",
                options={
                    **base_options,
                    "lidar_environment_model": {
                        "fog_density": 0.0,
                        "extinction_coefficient_scale": 0.05,
                        "backscatter_scale": 0.8,
                        "precipitation_rate": 60.0,
                        "precipitation_type": "rain",
                        "particle_density_scale": 1.0,
                        "particle_diameter_mm": 1.4,
                        "field_seed": 11,
                    },
                    "lidar_noise_performance": {
                        "probability_false_alarm": 0.2,
                    },
                },
            )
            snow_request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "snow_out",
                options={
                    **base_options,
                    "lidar_environment_model": {
                        "fog_density": 0.0,
                        "extinction_coefficient_scale": 0.05,
                        "backscatter_scale": 0.8,
                        "precipitation_rate": 60.0,
                        "precipitation_type": "snow",
                        "particle_density_scale": 1.4,
                        "particle_diameter_mm": 5.0,
                        "terminal_velocity_mps": 1.2,
                        "particle_reflectivity": 0.25,
                        "backscatter_jitter": 0.18,
                        "field_seed": 11,
                    },
                    "lidar_noise_performance": {
                        "probability_false_alarm": 0.2,
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            clear_result = orchestrator.run(clear_request, BackendMode.HYBRID_AUTO)
            rain_result = orchestrator.run(rain_request, BackendMode.HYBRID_AUTO)
            snow_result = orchestrator.run(snow_request, BackendMode.HYBRID_AUTO)

            self.assertTrue(clear_result.success)
            self.assertTrue(rain_result.success)
            self.assertTrue(snow_result.success)
            clear_preview = json.loads(
                clear_result.artifacts["lidar_noisy_preview_json"].read_text(encoding="utf-8")
            )
            rain_preview = json.loads(
                rain_result.artifacts["lidar_noisy_preview_json"].read_text(encoding="utf-8")
            )
            snow_preview = json.loads(
                snow_result.artifacts["lidar_noisy_preview_json"].read_text(encoding="utf-8")
            )
            clear_target = clear_preview["preview_points"][0]
            rain_targets = [
                point
                for point in rain_preview["preview_points"]
                if point["ground_truth_detection_type"] == "TARGET"
            ]
            rain_noise = [
                point
                for point in rain_preview["preview_points"]
                if point["ground_truth_detection_type"] == "NOISE"
            ]
            snow_targets = [
                point
                for point in snow_preview["preview_points"]
                if point["ground_truth_detection_type"] == "TARGET"
            ]
            snow_noise = [
                point
                for point in snow_preview["preview_points"]
                if point["ground_truth_detection_type"] == "NOISE"
            ]
            self.assertTrue(rain_targets)
            self.assertTrue(rain_noise)
            self.assertTrue(snow_targets)
            self.assertTrue(snow_noise)
            self.assertLess(rain_targets[0]["snr_db"], clear_target["snr_db"])
            self.assertLess(snow_targets[0]["snr_db"], clear_target["snr_db"])
            self.assertLess(
                rain_targets[0]["weather_extinction_factor"],
                clear_target["weather_extinction_factor"],
            )
            self.assertLess(
                snow_targets[0]["weather_extinction_factor"],
                rain_targets[0]["weather_extinction_factor"],
            )
            self.assertEqual(rain_noise[0]["precipitation_type"], "RAIN")
            self.assertEqual(snow_noise[0]["precipitation_type"], "SNOW")
            self.assertGreater(snow_noise[0]["particle_field_density"], rain_noise[0]["particle_field_density"])
            self.assertGreater(rain_result.metrics.get("lidar_backscatter_or_noise_count"), 0.0)
            self.assertGreater(snow_result.metrics.get("lidar_backscatter_or_noise_count"), 0.0)
            self.assertEqual(rain_result.metrics.get("lidar_weather_model_applied"), 1.0)
            self.assertEqual(snow_result.metrics.get("lidar_precipitation_model_applied"), 1.0)
            self.assertGreater(snow_result.metrics.get("lidar_precipitation_particle_density"), 0.0)
            self.assertGreater(snow_result.metrics.get("lidar_precipitation_particle_density"), rain_result.metrics.get("lidar_precipitation_particle_density"))
            self.assertGreater(rain_result.metrics.get("lidar_false_alarm_probability"), 0.0)
            self.assertGreater(rain_preview["output_count"], clear_preview["output_count"])
            self.assertGreater(snow_preview["output_count"], clear_preview["output_count"])

    def test_radar_trajectory_sweep_uses_local_velocity_per_frame(self) -> None:
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
20.0 0.0 0.0
EOF
cat > "${rootdir}/scan_trajectory.txt" <<EOF
0.0 0.0 0.0 0.0 0.0 0.0 0.0
2.0 0.0 0.0 1.0 0.0 0.0 0.0
6.0 0.0 0.0 2.0 0.0 0.0 0.0
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
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": False,
                    "radar_postprocess_enabled": True,
                    "radar_trajectory_sweep_enabled": True,
                    "radar_trajectory_sweep_frames": 3,
                    "radar_preview_targets_per_frame": 4,
                    "radar_max_targets": 4,
                    "radar_clutter": "none",
                    "radar_false_target_count": 0,
                    "radar_extrinsics": {
                        "enabled": True,
                        "tx": 0.0,
                        "ty": 0.0,
                        "tz": 0.0,
                        "roll_deg": 0.0,
                        "pitch_deg": 0.0,
                        "yaw_deg": 0.0,
                    },
                    "radar_extrinsics_auto_use_position": "none",
                    "radar_extrinsics_auto_use_orientation": False,
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)
            self.assertIn("radar_targets_trajectory_sweep", result.artifacts)
            self.assertEqual(result.metrics.get("radar_trajectory_sweep_frame_count"), 3.0)

            payload = json.loads(
                result.artifacts["radar_targets_trajectory_sweep"].read_text(encoding="utf-8")
            )
            self.assertEqual(payload["frame_count"], 3)
            self.assertEqual(len(payload["frames"]), 3)

            radial_velocities = []
            for frame in payload["frames"]:
                self.assertGreater(frame["target_count"], 0)
                radial_velocities.append(frame["targets_preview"][0]["radial_velocity_mps"])
            self.assertGreater(radial_velocities[0], radial_velocities[1])
            self.assertGreater(radial_velocities[1], radial_velocities[2])

    def test_hybrid_generates_renderer_playback_contract(self) -> None:
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
            fake_helios.chmod(0o755)

            request = SensorSimRequest(
                scenario_path=survey,
                output_dir=root / "out",
                options={
                    "execute_helios": True,
                    "camera_projection_enabled": False,
                    "lidar_postprocess_enabled": True,
                    "lidar_noise": "none",
                    "lidar_dropout_probability": 0.0,
                    "lidar_trajectory_sweep_enabled": True,
                    "lidar_trajectory_sweep_frames": 3,
                    "radar_postprocess_enabled": True,
                    "radar_clutter": "none",
                    "radar_false_target_count": 0,
                    "radar_trajectory_sweep_enabled": True,
                    "radar_trajectory_sweep_frames": 3,
                    "renderer_bridge_enabled": True,
                    "renderer_backend": "awsim",
                    "renderer_map": "sample_map",
                    "renderer_time_step_s": 0.1,
                    "renderer_start_time_s": 10.0,
                    "renderer_frame_offset": 100,
                    "camera_extrinsics": {
                        "enabled": True,
                        "tx": 1.0,
                        "ty": 2.0,
                        "tz": 3.0,
                        "roll_deg": 0.0,
                        "pitch_deg": 1.0,
                        "yaw_deg": 2.0,
                    },
                },
            )

            orchestrator = HybridOrchestrator(
                helios=HeliosAdapter(helios_bin=fake_helios),
                native=NativePhysicsBackend(),
            )
            result = orchestrator.run(request, BackendMode.HYBRID_AUTO)
            self.assertTrue(result.success)
            self.assertIn("renderer_playback_contract", result.artifacts)
            self.assertEqual(result.metrics.get("renderer_playback_contract_generated"), 1.0)
            self.assertEqual(result.metrics.get("renderer_playback_contract_frame_count"), 3.0)

            payload = json.loads(
                result.artifacts["renderer_playback_contract"].read_text(encoding="utf-8")
            )
            self.assertEqual(payload["renderer_backend"], "awsim")
            self.assertEqual(payload["renderer_scene"]["map"], "sample_map")
            self.assertEqual(payload["frame_count"], 3)
            self.assertEqual(len(payload["frames"]), 3)
            self.assertEqual(payload["frames"][0]["renderer_frame_id"], 100)
            self.assertAlmostEqual(payload["frames"][0]["time_s"], 10.0, places=6)
            self.assertIn("lidar", payload["frames"][0])
            self.assertIn("radar", payload["frames"][0])
            self.assertIn("sensor_setup", payload)
            self.assertEqual(
                payload["sensor_setup"]["camera"]["extrinsics_source"],
                "options",
            )
            self.assertEqual(
                payload["sensor_setup"]["lidar"]["extrinsics_source"],
                "lidar_sweep_frame0",
            )
            self.assertEqual(
                payload["sensor_setup"]["radar"]["extrinsics_source"],
                "radar_sweep_frame0",
            )
            self.assertIn("renderer_sensor_mounts", payload)
            mounts = payload["renderer_sensor_mounts"]
            self.assertEqual(len(mounts), 3)
            self.assertEqual(mounts[0]["sensor_type"], "camera")
            self.assertFalse(mounts[0]["enabled"])
            self.assertEqual(mounts[1]["sensor_type"], "lidar")
            self.assertTrue(mounts[1]["enabled"])
            self.assertEqual(mounts[1]["extrinsics_source"], "lidar_sweep_frame0")
            self.assertEqual(mounts[2]["sensor_type"], "radar")
            self.assertTrue(mounts[2]["enabled"])
            self.assertEqual(mounts[2]["extrinsics_source"], "radar_sweep_frame0")


if __name__ == "__main__":
    unittest.main()
