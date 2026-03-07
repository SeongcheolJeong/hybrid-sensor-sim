from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.backends.native_physics import NativePhysicsBackend
from hybrid_sensor_sim.config import CONFIG_SCHEMA_VERSION, build_sensor_sim_config
from hybrid_sensor_sim.renderers.playback_contract import build_renderer_playback_contract
from hybrid_sensor_sim.types import SensorSimRequest


class SensorConfigTests(unittest.TestCase):
    def test_build_sensor_sim_config_defaults(self) -> None:
        config = build_sensor_sim_config()

        self.assertEqual(config.schema_version, CONFIG_SCHEMA_VERSION)
        self.assertEqual(config.sensor_profile, "default")
        self.assertEqual(config.renderer.backend, "none")
        self.assertEqual(config.renderer.ego_actor_id, "ego")
        self.assertEqual(config.camera.geometry_model, "pinhole")
        self.assertEqual(config.camera.intrinsics.fx, 1200.0)
        self.assertEqual(config.lidar.noise_model, "gaussian")
        self.assertEqual(config.lidar.scan_type, "spin")
        self.assertEqual(config.radar.clutter_model, "basic")
        self.assertEqual(config.radar.range_min_m, 0.5)

    def test_build_sensor_sim_config_translates_legacy_options_and_behaviors(self) -> None:
        config = build_sensor_sim_config(
            sensor_profile="urban_sensor_suite",
            options={
                "renderer_bridge_enabled": True,
                "renderer_backend": "awsim",
                "renderer_execute": True,
                "renderer_map": "city_loop",
                "renderer_weather": "clearnoon",
                "renderer_scene_seed": "42",
                "renderer_ego_actor_id": "ego_vehicle",
                "renderer_camera_sensor_id": "cam_front",
                "renderer_lidar_sensor_id": "lidar_roof",
                "renderer_radar_sensor_id": "radar_bumper",
                "camera_geometry": "equidistant",
                "camera_projection_enabled": "false",
                "camera_intrinsics": {
                    "fx": "1111.0",
                    "fy": 1112.0,
                    "cx": 640,
                    "cy": 360,
                    "width": "1280",
                    "height": "720",
                },
                "camera_distortion_coeffs": {
                    "k1": "0.1",
                    "k2": 0.01,
                    "p1": 0.001,
                    "p2": -0.002,
                    "k3": 0.0003,
                },
                "camera_behaviors": [
                    {
                        "point_at": {
                            "id": 7,
                            "target_center_offset": {"x": 5, "y": 0, "z": 1},
                        }
                    },
                    {"continuous_motion": {"rz": 0.5}},
                ],
                "lidar_scan_type": "custom",
                "lidar_motion_compensation_enabled": "0",
                "lidar_scan_duration_s": "0.2",
                "lidar_noise": "none",
                "lidar_dropout_probability": "0.05",
                "lidar_behaviors": [{"continuous_motion": {"tx": 0.2, "rz": 0.1}}],
                "radar_clutter": "none",
                "radar_range_min_m": "1.5",
                "radar_range_max_m": "250.0",
                "radar_false_target_count": "0",
                "sensor_behaviors": {
                    "radar": [{"point_at": {"id": "ped_1"}}],
                },
            },
        )

        self.assertEqual(config.sensor_profile, "urban_sensor_suite")
        self.assertTrue(config.renderer.bridge_enabled)
        self.assertEqual(config.renderer.backend, "awsim")
        self.assertTrue(config.renderer.execute)
        self.assertEqual(config.renderer.map_name, "city_loop")
        self.assertEqual(config.renderer.scene_seed, 42)
        self.assertEqual(config.camera.sensor_id, "cam_front")
        self.assertEqual(config.camera.attach_to_actor_id, "ego_vehicle")
        self.assertEqual(config.camera.geometry_model, "equidistant")
        self.assertFalse(config.camera.projection_enabled)
        self.assertEqual(config.camera.intrinsics.width, 1280)
        self.assertAlmostEqual(config.camera.distortion_coeffs.k1, 0.1)
        self.assertEqual(len(config.camera.behaviors), 2)
        self.assertEqual(config.camera.behaviors[0].kind, "point_at")
        self.assertEqual(config.camera.behaviors[0].target_actor_id, "7")
        self.assertAlmostEqual(config.camera.behaviors[1].rz, 0.5)
        self.assertEqual(config.lidar.sensor_id, "lidar_roof")
        self.assertEqual(config.lidar.scan_type, "custom")
        self.assertFalse(config.lidar.motion_compensation_enabled)
        self.assertAlmostEqual(config.lidar.scan_duration_s, 0.2)
        self.assertEqual(config.lidar.noise_model, "none")
        self.assertAlmostEqual(config.lidar.dropout_probability, 0.05)
        self.assertEqual(config.radar.sensor_id, "radar_bumper")
        self.assertEqual(config.radar.clutter_model, "none")
        self.assertEqual(config.radar.false_target_count, 0)
        self.assertEqual(config.radar.behaviors[0].target_actor_id, "ped_1")

    def test_renderer_playback_contract_uses_typed_sensor_config(self) -> None:
        contract = build_renderer_playback_contract(
            options={
                "renderer_bridge_enabled": True,
                "renderer_backend": "carla",
                "renderer_ego_actor_id": "ego_vehicle",
                "renderer_camera_sensor_id": "cam_front",
                "renderer_lidar_sensor_id": "lidar_roof",
                "renderer_radar_sensor_id": "radar_front",
                "camera_geometry": "equidistant",
                "camera_behaviors": [{"point_at": {"id": 3}}],
                "lidar_scan_type": "flash",
                "lidar_behaviors": [{"continuous_motion": {"tx": 0.1}}],
                "radar_clutter": "none",
                "sensor_behaviors": {
                    "radar": [{"point_at": {"id": "vehicle_9"}}],
                },
            },
            artifacts={},
        )

        self.assertIsNotNone(contract)
        assert contract is not None
        self.assertEqual(contract["sensor_config_schema_version"], CONFIG_SCHEMA_VERSION)
        self.assertEqual(contract["renderer_backend"], "carla")
        self.assertEqual(contract["renderer_scene"]["ego_actor_id"], "ego_vehicle")
        self.assertEqual(contract["sensor_setup"]["camera"]["geometry_model"], "equidistant")
        self.assertEqual(contract["sensor_setup"]["camera"]["behaviors"][0]["point_at"]["id"], "3")
        self.assertEqual(contract["sensor_setup"]["lidar"]["scan_type"], "flash")
        self.assertEqual(
            contract["sensor_setup"]["radar"]["behaviors"][0]["point_at"]["id"],
            "vehicle_9",
        )
        self.assertEqual(contract["renderer_sensor_mounts"][0]["sensor_id"], "cam_front")
        self.assertEqual(contract["renderer_sensor_mounts"][1]["sensor_id"], "lidar_roof")
        self.assertEqual(contract["renderer_sensor_mounts"][2]["sensor_id"], "radar_front")

    def test_native_backend_writes_sensor_config_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            request = SensorSimRequest(
                scenario_path=root / "scenario.yaml",
                output_dir=root / "out",
                options={
                    "camera_geometry": "equidistant",
                    "renderer_camera_sensor_id": "cam_front",
                },
            )
            request.scenario_path.write_text("scenario: {}", encoding="utf-8")

            backend = NativePhysicsBackend()
            result = backend.simulate(request)

            self.assertTrue(result.success)
            self.assertIn("sensor_sim_config", result.artifacts)
            payload = json.loads(result.artifacts["sensor_sim_config"].read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], CONFIG_SCHEMA_VERSION)
            self.assertEqual(payload["sensors"]["camera"]["sensor_id"], "cam_front")
            self.assertEqual(payload["sensors"]["camera"]["geometry_model"], "equidistant")

