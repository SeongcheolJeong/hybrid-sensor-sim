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
        self.assertEqual(config.camera.image_chain.iso, 100)
        self.assertAlmostEqual(config.camera.image_chain.shutter_speed_us, 6000.0)
        self.assertAlmostEqual(config.camera.lens_params.lens_flare, 0.0)
        self.assertAlmostEqual(config.camera.lens_params.spot_size, 0.0)
        self.assertEqual(config.lidar.noise_model, "gaussian")
        self.assertEqual(config.lidar.scan_type, "SPIN")
        self.assertEqual(config.lidar.scan_frequency_hz, 10.0)
        self.assertEqual(config.lidar.source_angles_deg, [])
        self.assertEqual(config.lidar.intensity.units, "REFLECTIVITY")
        self.assertAlmostEqual(config.lidar.physics_model.reflectivity_coefficient, 1.0)
        self.assertEqual(config.lidar.return_model.mode, "SINGLE")
        self.assertEqual(config.lidar.return_model.max_returns, 1)
        self.assertTrue(config.lidar.environment_model.enable_ambient)
        self.assertAlmostEqual(config.lidar.environment_model.extinction_coefficient_scale, 0.05)
        self.assertAlmostEqual(config.lidar.noise_performance.probability_false_alarm, 0.0)
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
                "camera_sensor_type": "depth",
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
                "camera_depth_params": {
                    "min": 0.5,
                    "max": 120.0,
                    "type": "LOG",
                    "log_base": 2.0,
                    "bit_depth": 32,
                },
                "camera_semantic_params": {
                    "class_version": "GRANULAR_SEGMENTATION",
                    "palette": "applied_granular",
                    "include_actor_id": True,
                    "include_component_id": False,
                    "include_material_class": True,
                    "include_lane_marking_id": True,
                },
                "camera_image_params": {
                    "bloom": 0.2,
                    "shutter_speed": 7200.0,
                    "iso": 200,
                    "analog_gain": 1.5,
                    "digital_gain": 1.2,
                    "readout_noise": 0.01,
                    "white_balance": 5000,
                    "gamma": 2.0,
                    "seed": 13,
                    "fixed_pattern_noise": {
                        "dsnu": 0.0004,
                        "prnu": 0.0006,
                    },
                },
                "camera_lens_params": {
                    "lens_flare": 0.8,
                    "spot_size": 0.004,
                    "vignetting": {
                        "intensity": 0.5,
                        "alpha": 1.25,
                        "radius": 0.9,
                    },
                },
                "camera_rolling_shutter": {
                    "enabled": True,
                    "row_delay_ns": 1000,
                    "col_delay_ns": 500,
                    "num_time_steps": 8,
                    "num_exposure_samples_per_pixel": 4,
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
                "lidar_scan_frequency_hz": "15.0",
                "lidar_spin_direction": "cw",
                "lidar_source_angles": [-10.0, 0.0, 10.0],
                "lidar_source_angle_tolerance_deg": 1.5,
                "lidar_scan_field": {
                    "azimuth_min_deg": -30.0,
                    "azimuth_max_deg": 30.0,
                    "elevation_min_deg": -12.0,
                    "elevation_max_deg": 12.0,
                },
                "lidar_scan_field_offset": {
                    "azimuth_deg": 2.0,
                    "elevation_deg": -1.0,
                },
                "lidar_scan_path": [0.0, 15.0, 30.0],
                "lidar_multi_scan_path": [[0.0], [45.0]],
                "lidar_motion_compensation_enabled": "0",
                "lidar_scan_duration_s": "0.2",
                "lidar_noise": "none",
                "lidar_dropout_probability": "0.05",
                "lidar_intensity": {
                    "units": "snr_scaled",
                    "range": {"min": 0.0, "max": 25.0},
                    "scale": {"min": 0.0, "max": 255.0},
                    "range_scale_map": [
                        {"input": 0.0, "output": 0.0},
                        {"input": 10.0, "output": 128.0},
                        {"input": 20.0, "output": 255.0},
                    ],
                },
                "lidar_physics_model": {
                    "reflectivity_coefficient": 0.7,
                    "atmospheric_attenuation_rate": 0.02,
                    "ambient_power_dbw": -28.0,
                    "signal_photon_scale": 25000.0,
                    "ambient_photon_scale": 500.0,
                    "minimum_detection_snr_db": 6.0,
                    "return_all_hits": True,
                },
                "lidar_return_model": {
                    "mode": "dual",
                    "max_returns": 2,
                    "range_separation_m": 0.8,
                    "signal_decay": 0.45,
                    "minimum_secondary_snr_db": -4.0,
                },
                "lidar_environment_model": {
                    "enable_ambient": False,
                    "fog_density": 0.7,
                    "extinction_coefficient_scale": 0.08,
                    "backscatter_scale": 0.6,
                    "disable_backscatter": False,
                    "precipitation_rate": 35.0,
                },
                "lidar_noise_performance": {
                    "probability_false_alarm": 0.02,
                    "target_detectability": {
                        "probability_detection": 0.85,
                        "target": {
                            "range": 180.0,
                            "reflectivity": 0.65,
                        },
                    },
                },
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
        self.assertEqual(config.camera.sensor_type, "DEPTH")
        self.assertEqual(config.camera.geometry_model, "equidistant")
        self.assertFalse(config.camera.projection_enabled)
        self.assertEqual(config.camera.intrinsics.width, 1280)
        self.assertAlmostEqual(config.camera.distortion_coeffs.k1, 0.1)
        self.assertEqual(config.camera.depth_params.encoding_type, "LOG")
        self.assertAlmostEqual(config.camera.depth_params.log_base, 2.0)
        self.assertEqual(config.camera.semantic_params.class_version, "GRANULAR_SEGMENTATION")
        self.assertEqual(config.camera.semantic_params.palette, "APPLIED_GRANULAR")
        self.assertTrue(config.camera.semantic_params.include_actor_id)
        self.assertFalse(config.camera.semantic_params.include_component_id)
        self.assertTrue(config.camera.semantic_params.include_lane_marking_id)
        self.assertAlmostEqual(config.camera.image_chain.bloom, 0.2)
        self.assertAlmostEqual(config.camera.image_chain.shutter_speed_us, 7200.0)
        self.assertEqual(config.camera.image_chain.iso, 200)
        self.assertAlmostEqual(config.camera.image_chain.analog_gain, 1.5)
        self.assertAlmostEqual(config.camera.image_chain.digital_gain, 1.2)
        self.assertAlmostEqual(config.camera.image_chain.readout_noise, 0.01)
        self.assertAlmostEqual(config.camera.image_chain.white_balance_kelvin, 5000.0)
        self.assertAlmostEqual(config.camera.image_chain.gamma, 2.0)
        self.assertEqual(config.camera.image_chain.seed, 13)
        self.assertAlmostEqual(config.camera.image_chain.fixed_pattern_noise.dsnu, 0.0004)
        self.assertAlmostEqual(config.camera.image_chain.fixed_pattern_noise.prnu, 0.0006)
        self.assertAlmostEqual(config.camera.lens_params.lens_flare, 0.8)
        self.assertAlmostEqual(config.camera.lens_params.spot_size, 0.004)
        self.assertAlmostEqual(config.camera.lens_params.vignetting.intensity, 0.5)
        self.assertAlmostEqual(config.camera.lens_params.vignetting.alpha, 1.25)
        self.assertAlmostEqual(config.camera.lens_params.vignetting.radius, 0.9)
        self.assertTrue(config.camera.rolling_shutter.enabled)
        self.assertEqual(config.camera.rolling_shutter.num_time_steps, 8)
        self.assertEqual(config.camera.rolling_shutter.num_exposure_samples_per_pixel, 4)
        self.assertEqual(len(config.camera.behaviors), 2)
        self.assertEqual(config.camera.behaviors[0].kind, "point_at")
        self.assertEqual(config.camera.behaviors[0].target_actor_id, "7")
        self.assertAlmostEqual(config.camera.behaviors[1].rz, 0.5)
        self.assertEqual(config.lidar.sensor_id, "lidar_roof")
        self.assertEqual(config.lidar.scan_type, "CUSTOM")
        self.assertAlmostEqual(config.lidar.scan_frequency_hz, 15.0)
        self.assertEqual(config.lidar.spin_direction, "CW")
        self.assertEqual(config.lidar.source_angles_deg, [-10.0, 0.0, 10.0])
        self.assertAlmostEqual(config.lidar.source_angle_tolerance_deg, 1.5)
        self.assertAlmostEqual(config.lidar.scan_field_azimuth_min_deg, -30.0)
        self.assertAlmostEqual(config.lidar.scan_field_azimuth_max_deg, 30.0)
        self.assertAlmostEqual(config.lidar.scan_field_elevation_min_deg, -12.0)
        self.assertAlmostEqual(config.lidar.scan_field_elevation_max_deg, 12.0)
        self.assertAlmostEqual(config.lidar.scan_field_azimuth_offset_deg, 2.0)
        self.assertAlmostEqual(config.lidar.scan_field_elevation_offset_deg, -1.0)
        self.assertEqual(config.lidar.scan_path_deg, [0.0, 15.0, 30.0])
        self.assertEqual(config.lidar.multi_scan_path_deg, [[0.0], [45.0]])
        self.assertFalse(config.lidar.motion_compensation_enabled)
        self.assertAlmostEqual(config.lidar.scan_duration_s, 0.2)
        self.assertEqual(config.lidar.noise_model, "none")
        self.assertAlmostEqual(config.lidar.dropout_probability, 0.05)
        self.assertEqual(config.lidar.intensity.units, "SNR_SCALED")
        self.assertAlmostEqual(config.lidar.intensity.input_range.min_value, 0.0)
        self.assertAlmostEqual(config.lidar.intensity.input_range.max_value, 25.0)
        self.assertAlmostEqual(config.lidar.intensity.output_scale.max_value, 255.0)
        self.assertEqual(len(config.lidar.intensity.range_scale_map), 3)
        self.assertAlmostEqual(config.lidar.intensity.range_scale_map[1].output_value, 128.0)
        self.assertAlmostEqual(config.lidar.physics_model.reflectivity_coefficient, 0.7)
        self.assertAlmostEqual(config.lidar.physics_model.atmospheric_attenuation_rate, 0.02)
        self.assertAlmostEqual(config.lidar.physics_model.ambient_power_dbw, -28.0)
        self.assertAlmostEqual(config.lidar.physics_model.signal_photon_scale, 25000.0)
        self.assertAlmostEqual(config.lidar.physics_model.ambient_photon_scale, 500.0)
        self.assertAlmostEqual(config.lidar.physics_model.minimum_detection_snr_db, 6.0)
        self.assertTrue(config.lidar.physics_model.return_all_hits)
        self.assertEqual(config.lidar.return_model.mode, "DUAL")
        self.assertEqual(config.lidar.return_model.max_returns, 2)
        self.assertAlmostEqual(config.lidar.return_model.range_separation_m, 0.8)
        self.assertAlmostEqual(config.lidar.return_model.signal_decay, 0.45)
        self.assertAlmostEqual(config.lidar.return_model.minimum_secondary_snr_db, -4.0)
        self.assertFalse(config.lidar.environment_model.enable_ambient)
        self.assertAlmostEqual(config.lidar.environment_model.fog_density, 0.7)
        self.assertAlmostEqual(config.lidar.environment_model.extinction_coefficient_scale, 0.08)
        self.assertAlmostEqual(config.lidar.environment_model.backscatter_scale, 0.6)
        self.assertFalse(config.lidar.environment_model.disable_backscatter)
        self.assertAlmostEqual(config.lidar.environment_model.precipitation_rate, 35.0)
        self.assertAlmostEqual(config.lidar.noise_performance.probability_false_alarm, 0.02)
        self.assertAlmostEqual(config.lidar.noise_performance.probability_detection, 0.85)
        self.assertAlmostEqual(config.lidar.noise_performance.calibration_target_range_m, 180.0)
        self.assertAlmostEqual(config.lidar.noise_performance.calibration_target_reflectivity, 0.65)
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
                "camera_sensor_type": "SEMANTIC_SEGMENTATION",
                "camera_geometry": "equidistant",
                "camera_depth_params": {"min": 1.0, "max": 80.0, "type": "LINEAR"},
                "camera_semantic_params": {
                    "class_version": "GRANULAR_SEGMENTATION",
                    "include_material_class": True,
                },
                "camera_image_params": {
                    "iso": 160,
                    "white_balance": 5200,
                    "readout_noise": 0.005,
                },
                "camera_lens_params": {
                    "lens_flare": 0.6,
                    "spot_size": 0.003,
                    "vignetting": {"intensity": 0.4, "alpha": 1.1, "radius": 1.0},
                },
                "camera_row_delay_ns": 5000,
                "camera_behaviors": [{"point_at": {"id": 3}}],
                "lidar_scan_type": "flash",
                "lidar_source_angles": [-12.5, -2.0, 8.0],
                "lidar_scan_path": [0.0, 30.0],
                "lidar_intensity": {
                    "units": "power",
                    "range": {"min": 0.0, "max": 1.0},
                    "scale": {"min": 0.0, "max": 100.0},
                },
                "lidar_physics_model": {
                    "reflectivity_coefficient": 0.6,
                    "minimum_detection_snr_db": -12.0,
                },
                "lidar_return_model": {
                    "mode": "multi",
                    "max_returns": 3,
                    "range_separation_m": 0.6,
                },
                "lidar_environment_model": {
                    "fog_density": 0.4,
                    "backscatter_scale": 0.3,
                    "precipitation_rate": 12.0,
                },
                "lidar_noise_performance": {
                    "probability_false_alarm": 0.01,
                },
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
        self.assertEqual(contract["sensor_setup"]["camera"]["sensor_type"], "SEMANTIC_SEGMENTATION")
        self.assertEqual(contract["sensor_setup"]["camera"]["geometry_model"], "equidistant")
        self.assertEqual(contract["sensor_setup"]["camera"]["depth_params"]["max"], 80.0)
        self.assertEqual(
            contract["sensor_setup"]["camera"]["semantic_params"]["class_version"],
            "GRANULAR_SEGMENTATION",
        )
        self.assertTrue(
            contract["sensor_setup"]["camera"]["semantic_params"]["include_material_class"]
        )
        self.assertEqual(contract["sensor_setup"]["camera"]["image_chain"]["iso"], 160)
        self.assertEqual(
            contract["sensor_setup"]["camera"]["image_chain"]["white_balance_kelvin"],
            5200.0,
        )
        self.assertEqual(
            contract["sensor_setup"]["camera"]["image_chain"]["readout_noise"],
            0.005,
        )
        self.assertEqual(contract["sensor_setup"]["camera"]["lens_params"]["lens_flare"], 0.6)
        self.assertEqual(contract["sensor_setup"]["camera"]["lens_params"]["spot_size"], 0.003)
        self.assertEqual(
            contract["sensor_setup"]["camera"]["lens_params"]["vignetting"]["intensity"],
            0.4,
        )
        self.assertTrue(contract["sensor_setup"]["camera"]["rolling_shutter"]["enabled"])
        self.assertEqual(contract["sensor_setup"]["camera"]["behaviors"][0]["point_at"]["id"], "3")
        self.assertEqual(contract["sensor_setup"]["lidar"]["scan_type"], "FLASH")
        self.assertEqual(contract["sensor_setup"]["lidar"]["source_angles_deg"], [-12.5, -2.0, 8.0])
        self.assertEqual(contract["sensor_setup"]["lidar"]["scan_path_deg"], [0.0, 30.0])
        self.assertEqual(contract["sensor_setup"]["lidar"]["intensity"]["units"], "POWER")
        self.assertAlmostEqual(
            contract["sensor_setup"]["lidar"]["physics_model"]["reflectivity_coefficient"],
            0.6,
        )
        self.assertEqual(contract["sensor_setup"]["lidar"]["return_model"]["mode"], "MULTI")
        self.assertEqual(contract["sensor_setup"]["lidar"]["return_model"]["max_returns"], 3)
        self.assertAlmostEqual(
            contract["sensor_setup"]["lidar"]["environment_model"]["fog_density"],
            0.4,
        )
        self.assertAlmostEqual(
            contract["sensor_setup"]["lidar"]["noise_performance"]["probability_false_alarm"],
            0.01,
        )
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
