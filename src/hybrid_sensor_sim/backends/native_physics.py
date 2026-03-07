from __future__ import annotations

import json
import random
from math import atan2, cos, exp, log, log10, pi, sin, sqrt
from pathlib import Path
from typing import Any

try:
    import numpy as np
except ImportError:  # pragma: no cover - numpy is available in test env, but keep runtime optional.
    np = None

from hybrid_sensor_sim.backends.base import SensorBackend
from hybrid_sensor_sim.config import CameraSensorConfig, SensorSimConfig, build_sensor_sim_config
from hybrid_sensor_sim.io.pointcloud_xyz import read_xyz_points, write_xyz_points
from hybrid_sensor_sim.io.trajectory_txt import TrajectoryPose, read_trajectory_poses
from hybrid_sensor_sim.physics.camera import (
    BrownConradyDistortion,
    CameraExtrinsics,
    CameraIntrinsics,
    project_points_brown_conrady,
    transform_points_world_to_camera,
)
from hybrid_sensor_sim.renderers import build_renderer_playback_contract
from hybrid_sensor_sim.renderers import execute_renderer_runtime
from hybrid_sensor_sim.types import SensorSimRequest, SensorSimResult

_APPLIED_LEGACY_CAMERA_SEMANTICS: dict[int, dict[str, Any]] = {
    1: {"name": "BUILDINGS", "parent_class": "LEGACY", "color_rgb": [70, 70, 70], "material_class_id": 2100},
    3: {"name": "OTHER", "parent_class": "LEGACY", "color_rgb": [160, 160, 160], "material_class_id": 2000},
    4: {"name": "PEDESTRIANS", "parent_class": "LEGACY", "color_rgb": [220, 20, 60], "material_class_id": 4000},
    5: {"name": "POLES", "parent_class": "LEGACY", "color_rgb": [153, 153, 153], "material_class_id": 2200},
    6: {"name": "ROADLINES", "parent_class": "LEGACY", "color_rgb": [157, 234, 50], "material_class_id": 1200},
    7: {"name": "ROADS", "parent_class": "LEGACY", "color_rgb": [128, 64, 128], "material_class_id": 1100},
    8: {"name": "SIDEWALKS", "parent_class": "LEGACY", "color_rgb": [244, 35, 232], "material_class_id": 1110},
    9: {"name": "VEGETATION", "parent_class": "LEGACY", "color_rgb": [107, 142, 35], "material_class_id": 3100},
    10: {"name": "VEHICLES", "parent_class": "LEGACY", "color_rgb": [0, 0, 142], "material_class_id": 2300},
    12: {"name": "TRAFFICSIGNS", "parent_class": "LEGACY", "color_rgb": [220, 220, 0], "material_class_id": 2400},
    22: {"name": "SKY", "parent_class": "LEGACY", "color_rgb": [70, 130, 180], "material_class_id": 0},
}

_APPLIED_GRANULAR_CAMERA_SEMANTICS: dict[int, dict[str, Any]] = {
    205: {"name": "GENERIC_BUILDING", "parent_class": "BUILDING_COMPONENTS", "color_rgb": [70, 70, 70], "material_class_id": 2100},
    1002: {"name": "CURB_TOP", "parent_class": "ROAD_EDGE_COMPONENTS", "color_rgb": [196, 196, 196], "material_class_id": 1110},
    1524: {"name": "CROSSWALK", "parent_class": "ROAD_SURFACE_MARKINGS", "color_rgb": [255, 255, 255], "material_class_id": 1200},
    2501: {"name": "LANE_LINE", "parent_class": "ROAD_SURFACE_LINES", "color_rgb": [157, 234, 50], "material_class_id": 1200},
    3005: {"name": "PARKING_SPOT", "parent_class": "ROAD_SURFACE_REGIONS", "color_rgb": [250, 170, 160], "material_class_id": 1100},
    3500: {"name": "GENERIC_BARRIER", "parent_class": "BARRIERS", "color_rgb": [190, 153, 153], "material_class_id": 2200},
    3501: {"name": "FENCE", "parent_class": "BARRIERS", "color_rgb": [190, 153, 153], "material_class_id": 2200},
    4000: {"name": "GENERIC_PEDESTRIAN", "parent_class": "PEDESTRIANS", "color_rgb": [220, 20, 60], "material_class_id": 4000},
    5000: {"name": "GENERIC_ROAD", "parent_class": "ROADS", "color_rgb": [128, 64, 128], "material_class_id": 1100},
    5500: {"name": "GENERIC_SIDEWALK", "parent_class": "SIDEWALKS", "color_rgb": [244, 35, 232], "material_class_id": 1110},
    6003: {"name": "TREE", "parent_class": "FOLIAGE", "color_rgb": [107, 142, 35], "material_class_id": 3100},
    7501: {"name": "GENERIC_CAR", "parent_class": "VEHICLES", "color_rgb": [0, 0, 142], "material_class_id": 2300},
}

_LIDAR_PRECIPITATION_DEFAULTS: dict[str, dict[str, float]] = {
    "RAIN": {
        "particle_diameter_mm": 1.4,
        "terminal_velocity_mps": 7.5,
        "particle_reflectivity": 0.65,
        "density_coefficient": 0.010,
        "extinction_coefficient": 0.040,
        "backscatter_gain": 0.75,
    },
    "SNOW": {
        "particle_diameter_mm": 4.5,
        "terminal_velocity_mps": 1.4,
        "particle_reflectivity": 0.35,
        "density_coefficient": 0.018,
        "extinction_coefficient": 0.026,
        "backscatter_gain": 0.55,
    },
    "HAIL": {
        "particle_diameter_mm": 6.5,
        "terminal_velocity_mps": 11.0,
        "particle_reflectivity": 0.90,
        "density_coefficient": 0.007,
        "extinction_coefficient": 0.045,
        "backscatter_gain": 1.05,
    },
}


class NativePhysicsBackend(SensorBackend):
    def name(self) -> str:
        return "native_physics"

    def simulate(self, request: SensorSimRequest) -> SensorSimResult:
        native_output = request.output_dir / "native_only"
        native_output.mkdir(parents=True, exist_ok=True)
        config = self._sensor_config_from_request(request)
        intrinsics = config.camera.intrinsics.to_camera_intrinsics()
        distortion = config.camera.distortion_coeffs.to_brown_conrady()
        extrinsics = config.camera.extrinsics.to_camera_extrinsics()
        config_path = native_output / "sensor_sim_config.json"
        config_path.write_text(json.dumps(config.to_manifest(), indent=2), encoding="utf-8")
        payload = {
            "mode": "native_only",
            "scenario": str(request.scenario_path),
            "sensor_profile": request.sensor_profile,
            "config_schema_version": config.schema_version,
            "sensor_setup": config.to_manifest(),
            "physics": {
                "camera_distortion": config.camera.distortion_model,
                "lidar_noise": config.lidar.noise_model,
                "radar_clutter": config.radar.clutter_model,
            },
            "camera_intrinsics": {
                "fx": intrinsics.fx,
                "fy": intrinsics.fy,
                "cx": intrinsics.cx,
                "cy": intrinsics.cy,
                "width": intrinsics.width,
                "height": intrinsics.height,
            },
            "camera_distortion_coeffs": {
                "k1": distortion.k1,
                "k2": distortion.k2,
                "p1": distortion.p1,
                "p2": distortion.p2,
                "k3": distortion.k3,
            },
            "camera_extrinsics": {
                "enabled": extrinsics.enabled,
                "tx": extrinsics.tx,
                "ty": extrinsics.ty,
                "tz": extrinsics.tz,
                "roll_deg": extrinsics.roll_deg,
                "pitch_deg": extrinsics.pitch_deg,
                "yaw_deg": extrinsics.yaw_deg,
            },
        }
        out_path = native_output / "native_physics.json"
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return SensorSimResult(
            backend=self.name(),
            success=True,
            artifacts={
                "native_physics": out_path,
                "sensor_sim_config": config_path,
            },
            message="Native simulation completed.",
        )

    def enhance_from_helios(
        self,
        request: SensorSimRequest,
        helios_result: SensorSimResult,
    ) -> SensorSimResult:
        enhanced_output = request.output_dir / "hybrid_enhanced"
        enhanced_output.mkdir(parents=True, exist_ok=True)
        config = self._sensor_config_from_request(request)
        intrinsics = config.camera.intrinsics.to_camera_intrinsics()
        distortion = config.camera.distortion_coeffs.to_brown_conrady()
        extrinsics = config.camera.extrinsics.to_camera_extrinsics()
        config_path = enhanced_output / "sensor_sim_config.json"
        config_path.write_text(json.dumps(config.to_manifest(), indent=2), encoding="utf-8")

        artifacts = {**helios_result.artifacts}
        metrics = dict(helios_result.metrics)
        artifacts["sensor_sim_config"] = config_path
        camera_projection_artifact = self._project_xyz_if_available(
            request=request,
            artifacts=artifacts,
            enhanced_output=enhanced_output,
            camera_config=config.camera,
            intrinsics=intrinsics,
            distortion=distortion,
            extrinsics=extrinsics,
            metrics=metrics,
        )
        if camera_projection_artifact is not None:
            artifacts["camera_projection_preview"] = camera_projection_artifact
        camera_projection_sweep_artifact = self._project_xyz_trajectory_sweep_if_available(
            request=request,
            artifacts=artifacts,
            enhanced_output=enhanced_output,
            camera_config=config.camera,
            intrinsics=intrinsics,
            distortion=distortion,
            extrinsics=extrinsics,
            metrics=metrics,
        )
        if camera_projection_sweep_artifact is not None:
            artifacts["camera_projection_trajectory_sweep"] = camera_projection_sweep_artifact
        lidar_noisy_artifact, lidar_noisy_metadata_artifact = self._generate_lidar_noisy_pointcloud_if_available(
            request=request,
            artifacts=artifacts,
            enhanced_output=enhanced_output,
            metrics=metrics,
        )
        if lidar_noisy_artifact is not None:
            artifacts["lidar_noisy_preview"] = lidar_noisy_artifact
        if lidar_noisy_metadata_artifact is not None:
            artifacts["lidar_noisy_preview_json"] = lidar_noisy_metadata_artifact
        lidar_sweep_artifact = self._generate_lidar_trajectory_sweep_if_available(
            request=request,
            artifacts=artifacts,
            enhanced_output=enhanced_output,
            metrics=metrics,
        )
        if lidar_sweep_artifact is not None:
            artifacts["lidar_trajectory_sweep"] = lidar_sweep_artifact
        radar_targets_artifact = self._generate_radar_targets_if_available(
            request=request,
            artifacts=artifacts,
            enhanced_output=enhanced_output,
            metrics=metrics,
        )
        if radar_targets_artifact is not None:
            artifacts["radar_targets_preview"] = radar_targets_artifact
        radar_targets_sweep_artifact = self._generate_radar_targets_trajectory_sweep_if_available(
            request=request,
            artifacts=artifacts,
            enhanced_output=enhanced_output,
            metrics=metrics,
        )
        if radar_targets_sweep_artifact is not None:
            artifacts["radar_targets_trajectory_sweep"] = radar_targets_sweep_artifact
        coverage_summary_artifact = self._generate_sensor_coverage_summary_if_available(
            request=request,
            artifacts=artifacts,
            enhanced_output=enhanced_output,
            metrics=metrics,
        )
        if coverage_summary_artifact is not None:
            artifacts["sensor_coverage_summary"] = coverage_summary_artifact
        renderer_contract_artifact = self._generate_renderer_playback_contract_if_available(
            request=request,
            artifacts=artifacts,
            enhanced_output=enhanced_output,
            metrics=metrics,
        )
        if renderer_contract_artifact is not None:
            artifacts["renderer_playback_contract"] = renderer_contract_artifact
        renderer_runtime_success = True
        renderer_runtime_message = ""
        renderer_runtime_artifacts, renderer_runtime_metrics, renderer_runtime_message, renderer_runtime_success = (
            self._execute_renderer_runtime_if_available(
                request=request,
                artifacts=artifacts,
                enhanced_output=enhanced_output,
                metrics=metrics,
            )
        )
        artifacts.update(renderer_runtime_artifacts)
        metrics.update(renderer_runtime_metrics)
        metrics["renderer_runtime_success"] = 1.0 if renderer_runtime_success else 0.0

        payload = {
            "mode": "hybrid_enhanced",
            "source_backend": helios_result.backend,
            "source_artifacts": {k: str(v) for k, v in artifacts.items()},
            "config_schema_version": config.schema_version,
            "sensor_setup": config.to_manifest(),
            "enhancements": {
                "camera_geometry": config.camera.geometry_model,
                "distortion_model": config.camera.distortion_model,
                "motion_compensation": request.options.get("motion_compensation", True),
                "camera_intrinsics": {
                    "fx": intrinsics.fx,
                    "fy": intrinsics.fy,
                    "cx": intrinsics.cx,
                    "cy": intrinsics.cy,
                    "width": intrinsics.width,
                    "height": intrinsics.height,
                },
                "camera_distortion_coeffs": {
                    "k1": distortion.k1,
                    "k2": distortion.k2,
                    "p1": distortion.p1,
                    "p2": distortion.p2,
                    "k3": distortion.k3,
                },
                "camera_extrinsics": {
                    "enabled": extrinsics.enabled,
                    "tx": extrinsics.tx,
                    "ty": extrinsics.ty,
                    "tz": extrinsics.tz,
                    "roll_deg": extrinsics.roll_deg,
                    "pitch_deg": extrinsics.pitch_deg,
                    "yaw_deg": extrinsics.yaw_deg,
                },
                "camera_projection_enabled": config.camera.projection_enabled,
                "lidar_postprocess_enabled": config.lidar.postprocess_enabled,
                "lidar_trajectory_sweep_enabled": config.lidar.trajectory_sweep_enabled,
                "radar_postprocess_enabled": config.radar.postprocess_enabled,
                "radar_trajectory_sweep_enabled": config.radar.trajectory_sweep_enabled,
                "renderer_bridge_enabled": config.renderer.bridge_enabled,
                "renderer_backend": config.renderer.backend,
                "renderer_execute": config.renderer.execute,
            },
        }
        out_path = enhanced_output / "hybrid_physics.json"
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        result_success = True
        result_message = "Hybrid enhancement completed."
        if renderer_runtime_message:
            result_message = f"{result_message} {renderer_runtime_message}"
        if not renderer_runtime_success and bool(request.options.get("renderer_fail_on_error", False)):
            result_success = False
            result_message = (
                "Hybrid enhancement completed but renderer runtime failed "
                "(renderer_fail_on_error=true). "
                f"{renderer_runtime_message}"
            )
        return SensorSimResult(
            backend="hybrid(helios+native_physics)",
            success=result_success,
            artifacts={
                **artifacts,
                "hybrid_physics": out_path,
            },
            metrics=metrics,
            message=result_message,
        )

    def _sensor_config_from_request(self, request: SensorSimRequest) -> SensorSimConfig:
        return build_sensor_sim_config(
            sensor_profile=request.sensor_profile,
            options=request.options,
        )

    def _camera_intrinsics_from_options(self, request: SensorSimRequest) -> CameraIntrinsics:
        return self._sensor_config_from_request(request).camera.intrinsics.to_camera_intrinsics()

    def _camera_distortion_from_options(
        self, request: SensorSimRequest
    ) -> BrownConradyDistortion:
        return self._sensor_config_from_request(request).camera.distortion_coeffs.to_brown_conrady()

    def _camera_extrinsics_from_options(self, request: SensorSimRequest) -> CameraExtrinsics:
        return self._sensor_config_from_request(request).camera.extrinsics.to_camera_extrinsics()

    def _project_xyz_if_available(
        self,
        request: SensorSimRequest,
        artifacts: dict[str, Path],
        enhanced_output: Path,
        camera_config: CameraSensorConfig,
        intrinsics: CameraIntrinsics,
        distortion: BrownConradyDistortion,
        extrinsics: CameraExtrinsics,
        metrics: dict[str, float],
    ) -> Path | None:
        if not camera_config.projection_enabled:
            return None

        point_cloud = artifacts.get("point_cloud_primary")
        if point_cloud is None or point_cloud.suffix.lower() != ".xyz" or not point_cloud.exists():
            return None

        max_points = int(request.options.get("camera_projection_max_points", 5000))
        points_xyz = read_xyz_points(point_cloud, max_points=max_points)
        if not points_xyz:
            metrics["camera_projection_input_count"] = 0.0
            metrics["camera_projection_output_count"] = 0.0
            return None

        transformed_points, reference_point = self._apply_projection_reference_frame(
            points_xyz=points_xyz,
            request=request,
        )
        behavior_actor_positions = self._sensor_behavior_actor_position_map(
            request=request,
            points_xyz=transformed_points,
        )
        trajectory_path = artifacts.get("trajectory_primary")
        rolling_poses = self._read_camera_rolling_shutter_poses(
            request=request,
            trajectory_path=trajectory_path,
        )
        rolling_base_pose = self._select_camera_rolling_shutter_base_pose(
            request=request,
            poses=rolling_poses,
        )
        effective_extrinsics, extrinsics_meta = self._resolve_effective_extrinsics(
            request=request,
            artifacts=artifacts,
            base_extrinsics=extrinsics,
            reference_point=reference_point,
        )
        effective_extrinsics, behavior_runtime = self._apply_sensor_behaviors_to_extrinsics(
            request=request,
            sensor_name="camera",
            base_extrinsics=effective_extrinsics,
            behaviors=list(camera_config.behaviors),
            actor_positions=behavior_actor_positions,
            points_xyz=transformed_points,
            eval_time_s=float(request.options.get("camera_behavior_time_s", request.options.get("sensor_behavior_time_s", 0.0))),
        )
        projected, rolling_runtime = self._project_camera_points_with_optional_rolling_shutter(
            request=request,
            points_xyz=transformed_points,
            camera_config=camera_config,
            intrinsics=intrinsics,
            distortion=distortion,
            base_extrinsics=effective_extrinsics,
            trajectory_poses=rolling_poses,
            base_pose=rolling_base_pose,
        )

        metrics["camera_projection_input_count"] = float(len(points_xyz))
        metrics["camera_projection_output_count"] = float(len(projected))
        metrics["camera_extrinsics_auto_applied"] = (
            1.0 if extrinsics_meta.get("source") == "trajectory_auto" else 0.0
        )
        metrics["camera_rolling_shutter_enabled"] = (
            1.0 if camera_config.rolling_shutter.enabled else 0.0
        )
        metrics["camera_rolling_shutter_applied"] = (
            1.0 if bool(rolling_runtime.get("applied")) else 0.0
        )
        metrics["camera_depth_output_count"] = (
            float(len(projected)) if camera_config.sensor_type.upper() == "DEPTH" else 0.0
        )
        metrics["camera_semantic_output_count"] = (
            float(len(projected))
            if camera_config.sensor_type.upper() == "SEMANTIC_SEGMENTATION"
            else 0.0
        )
        metrics["camera_image_chain_enabled"] = (
            1.0
            if camera_config.image_chain.enabled and camera_config.sensor_type.upper() == "VISIBLE"
            else 0.0
        )
        metrics["camera_lens_artifact_enabled"] = (
            1.0
            if camera_config.sensor_type.upper() == "VISIBLE"
            and (
                camera_config.lens_params.lens_flare > 0.0
                or camera_config.lens_params.spot_size > 0.0
                or camera_config.lens_params.vignetting.intensity > 0.0
            )
            else 0.0
        )
        metrics["camera_image_signal_output_count"] = (
            float(len(projected))
            if camera_config.image_chain.enabled and camera_config.sensor_type.upper() == "VISIBLE"
            else 0.0
        )
        metrics["camera_behavior_applied"] = 1.0 if bool(behavior_runtime.get("applied")) else 0.0
        preview_count = int(request.options.get("camera_projection_preview_count", 20))
        preview_payload = self._camera_preview_payload(
            request=request,
            projected=projected,
            camera_config=camera_config,
            intrinsics=intrinsics,
            preview_count=preview_count,
        )
        preview = {
            "input_point_cloud": str(point_cloud),
            "sensor_type": camera_config.sensor_type,
            "geometry_model": camera_config.geometry_model,
            "input_count": len(points_xyz),
            "output_count": len(projected),
            "reference_point_xyz": {
                "x": reference_point[0],
                "y": reference_point[1],
                "z": reference_point[2],
            }
            if reference_point is not None
            else None,
            "camera_extrinsics": {
                "enabled": effective_extrinsics.enabled,
                "tx": effective_extrinsics.tx,
                "ty": effective_extrinsics.ty,
                "tz": effective_extrinsics.tz,
                "roll_deg": effective_extrinsics.roll_deg,
                "pitch_deg": effective_extrinsics.pitch_deg,
                "yaw_deg": effective_extrinsics.yaw_deg,
            },
            "camera_extrinsics_source": extrinsics_meta.get("source", "manual"),
            "camera_extrinsics_trajectory_pose": extrinsics_meta.get("trajectory_pose"),
            "camera_behavior": behavior_runtime,
            "rolling_shutter": self._camera_rolling_shutter_payload(
                camera_config=camera_config,
                intrinsics=intrinsics,
                runtime=rolling_runtime,
            ),
            "depth_params": camera_config.depth_params.to_dict(),
            "semantic_params": camera_config.semantic_params.to_dict(),
            "image_chain": camera_config.image_chain.to_dict(),
            "lens_params": camera_config.lens_params.to_dict(),
            **preview_payload,
        }
        output_path = enhanced_output / "camera_projection_preview.json"
        output_path.write_text(json.dumps(preview, indent=2), encoding="utf-8")
        return output_path

    def _project_xyz_trajectory_sweep_if_available(
        self,
        request: SensorSimRequest,
        artifacts: dict[str, Path],
        enhanced_output: Path,
        camera_config: CameraSensorConfig,
        intrinsics: CameraIntrinsics,
        distortion: BrownConradyDistortion,
        extrinsics: CameraExtrinsics,
        metrics: dict[str, float],
    ) -> Path | None:
        if not camera_config.projection_enabled:
            return None
        if not camera_config.trajectory_sweep_enabled:
            return None

        point_cloud = artifacts.get("point_cloud_primary")
        trajectory_path = artifacts.get("trajectory_primary")
        if (
            point_cloud is None
            or point_cloud.suffix.lower() != ".xyz"
            or not point_cloud.exists()
            or trajectory_path is None
            or not trajectory_path.exists()
        ):
            return None

        max_points = int(request.options.get("camera_projection_max_points", 5000))
        points_xyz = read_xyz_points(point_cloud, max_points=max_points)
        if not points_xyz:
            metrics["camera_projection_trajectory_sweep_frame_count"] = 0.0
            metrics["camera_projection_trajectory_sweep_total_output_count"] = 0.0
            return None

        transformed_points, reference_point = self._apply_projection_reference_frame(
            points_xyz=points_xyz,
            request=request,
        )
        poses = self._read_camera_rolling_shutter_poses(
            request=request,
            trajectory_path=trajectory_path,
        )
        if not poses:
            metrics["camera_projection_trajectory_sweep_frame_count"] = 0.0
            metrics["camera_projection_trajectory_sweep_total_output_count"] = 0.0
            return None

        frame_count = int(request.options.get("camera_projection_trajectory_sweep_frames", 3))
        selected_poses = self._sample_trajectory_poses(poses=poses, frame_count=frame_count)
        clamp_to_image = camera_config.projection_clamp_to_image
        preview_count = int(request.options.get("camera_projection_preview_count", 20))
        frames: list[dict[str, object]] = []
        total_output_count = 0
        rolling_applied = False
        behavior_actor_positions = self._sensor_behavior_actor_position_map(
            request=request,
            points_xyz=transformed_points,
        )
        behavior_time_origin_s = float(selected_poses[0][1].time_s) if selected_poses else 0.0
        camera_coverage_threshold = max(
            self._sensor_config_from_request(request).coverage.camera_min_pixels_on_target,
            1,
        )
        coverage_target_lists: list[list[dict[str, object]]] = []
        coverage_total_observation_count = 0
        coverage_anonymous_observation_count = 0
        coverage_excluded_observation_count = 0
        for pose_index, pose in selected_poses:
            effective = self._build_extrinsics_from_pose(
                request=request,
                base_extrinsics=extrinsics,
                pose=pose,
                reference_point=reference_point,
                force_enable=True,
            )
            effective, behavior_runtime = self._apply_sensor_behaviors_to_extrinsics(
                request=request,
                sensor_name="camera",
                base_extrinsics=effective,
                behaviors=list(camera_config.behaviors),
                actor_positions=behavior_actor_positions,
                points_xyz=transformed_points,
                eval_time_s=float(pose.time_s - behavior_time_origin_s),
            )
            projected, rolling_runtime = self._project_camera_points_with_optional_rolling_shutter(
                request=request,
                points_xyz=transformed_points,
                camera_config=camera_config,
                intrinsics=intrinsics,
                distortion=distortion,
                base_extrinsics=effective,
                trajectory_poses=poses,
                base_pose=pose,
            )
            rolling_applied = rolling_applied or bool(rolling_runtime.get("applied"))
            total_output_count += len(projected)
            frame_preview_payload = self._camera_preview_payload(
                request=request,
                projected=projected,
                camera_config=camera_config,
                intrinsics=intrinsics,
                preview_count=preview_count,
            )
            coverage_target_lists.append(list(frame_preview_payload.get("coverage_targets", [])))
            coverage_total_observation_count += int(
                frame_preview_payload.get("coverage_total_observation_count", 0)
            )
            coverage_anonymous_observation_count += int(
                frame_preview_payload.get("coverage_anonymous_observation_count", 0)
            )
            coverage_excluded_observation_count += int(
                frame_preview_payload.get("coverage_excluded_observation_count", 0)
            )
            frames.append(
                {
                    "pose_index": pose_index,
                    "trajectory_pose": self._trajectory_pose_payload(
                        pose=pose,
                        trajectory_path=trajectory_path,
                    ),
                    "camera_extrinsics": {
                        "enabled": effective.enabled,
                        "tx": effective.tx,
                        "ty": effective.ty,
                        "tz": effective.tz,
                        "roll_deg": effective.roll_deg,
                        "pitch_deg": effective.pitch_deg,
                        "yaw_deg": effective.yaw_deg,
                    },
                    "sensor_type": camera_config.sensor_type,
                    "geometry_model": camera_config.geometry_model,
                    "camera_behavior": behavior_runtime,
                    "rolling_shutter": self._camera_rolling_shutter_payload(
                        camera_config=camera_config,
                        intrinsics=intrinsics,
                        runtime=rolling_runtime,
                    ),
                    "output_count": len(projected),
                    **frame_preview_payload,
                }
            )

        metrics["camera_projection_trajectory_sweep_frame_count"] = float(len(frames))
        metrics["camera_projection_trajectory_sweep_total_output_count"] = float(total_output_count)
        metrics["camera_rolling_shutter_enabled"] = (
            1.0 if camera_config.rolling_shutter.enabled else 0.0
        )
        metrics["camera_rolling_shutter_applied"] = 1.0 if rolling_applied else 0.0
        metrics["camera_depth_trajectory_sweep_total_output_count"] = (
            float(total_output_count) if camera_config.sensor_type.upper() == "DEPTH" else 0.0
        )
        metrics["camera_semantic_trajectory_sweep_total_output_count"] = (
            float(total_output_count)
            if camera_config.sensor_type.upper() == "SEMANTIC_SEGMENTATION"
            else 0.0
        )
        metrics["camera_image_signal_trajectory_sweep_total_output_count"] = (
            float(total_output_count)
            if camera_config.image_chain.enabled and camera_config.sensor_type.upper() == "VISIBLE"
            else 0.0
        )
        metrics["camera_behavior_applied"] = 1.0 if any(
            bool(frame.get("camera_behavior", {}).get("applied"))
            for frame in frames
        ) else 0.0
        preview = {
            "input_point_cloud": str(point_cloud),
            "trajectory_path": str(trajectory_path),
            "sensor_type": camera_config.sensor_type,
            "geometry_model": camera_config.geometry_model,
            "rolling_shutter": self._camera_rolling_shutter_payload(
                camera_config=camera_config,
                intrinsics=intrinsics,
                runtime={
                    "applied": rolling_applied,
                    "trajectory_available": bool(poses),
                    "pose_count": len(poses),
                },
            ),
            "depth_params": camera_config.depth_params.to_dict(),
            "semantic_params": camera_config.semantic_params.to_dict(),
            "image_chain": camera_config.image_chain.to_dict(),
            "lens_params": camera_config.lens_params.to_dict(),
            "ground_truth_fields": self._camera_ground_truth_fields(
                camera_config=camera_config
            ),
            "coverage_metric_name": "pixels_on_target",
            "coverage_total_observation_count": coverage_total_observation_count,
            "coverage_anonymous_observation_count": coverage_anonymous_observation_count,
            "coverage_excluded_observation_count": coverage_excluded_observation_count,
            "coverage_targets": self._merge_sensor_coverage_targets(
                target_lists=coverage_target_lists,
                count_field="pixels_on_target",
                count_threshold=camera_coverage_threshold,
            ),
            "input_count": len(points_xyz),
            "frame_count": len(frames),
            "reference_point_xyz": {
                "x": reference_point[0],
                "y": reference_point[1],
                "z": reference_point[2],
            }
            if reference_point is not None
            else None,
            "frames": frames,
        }
        output_path = enhanced_output / "camera_projection_trajectory_sweep.json"
        output_path.write_text(json.dumps(preview, indent=2), encoding="utf-8")
        return output_path

    def _camera_preview_payload(
        self,
        *,
        request: SensorSimRequest,
        projected: list[dict[str, object]],
        camera_config: CameraSensorConfig,
        intrinsics: CameraIntrinsics,
        preview_count: int,
    ) -> dict[str, object]:
        preview_points = projected[:preview_count]
        all_ground_truth_samples = [
            self._camera_semantic_sample(
                request=request,
                camera_config=camera_config,
                projected_sample=point,
            )
            for point in projected
        ]
        preview_ground_truth_samples = all_ground_truth_samples[:preview_count]
        coverage_summary = self._build_coverage_summary_from_samples(
            samples=all_ground_truth_samples,
            count_field="pixels_on_target",
            actor_field="ground_truth_actor_id",
            semantic_class_field="ground_truth_semantic_class",
            semantic_name_field="semantic_class_name",
            component_field="ground_truth_component_id",
            material_class_field="ground_truth_material_class",
            base_map_field="ground_truth_base_map_element",
            procedural_map_field="ground_truth_procedural_map_element",
            lane_marking_field="ground_truth_lane_marking_id",
            count_threshold=max(
                self._sensor_config_from_request(request).coverage.camera_min_pixels_on_target,
                1,
            ),
            excluded_detection_types=set(),
        )
        payload: dict[str, object] = {
            "output_mode": camera_config.sensor_type,
            "preview_points_uvz": [
                {"u": float(point["u"]), "v": float(point["v"]), "z": float(point["z"])}
                for point in preview_points
            ],
            "preview_ground_truth_samples": preview_ground_truth_samples,
            "preview_depth_samples": [],
            "preview_semantic_samples": [],
            "preview_image_signal_samples": [],
            "ground_truth_fields": self._camera_ground_truth_fields(camera_config=camera_config),
            "coverage_metric_name": "pixels_on_target",
            "coverage_total_observation_count": coverage_summary["total_observation_count"],
            "coverage_anonymous_observation_count": coverage_summary[
                "anonymous_observation_count"
            ],
            "coverage_excluded_observation_count": coverage_summary[
                "excluded_observation_count"
            ],
            "coverage_targets": coverage_summary["targets"],
        }
        if camera_config.sensor_type.upper() == "DEPTH":
            payload["preview_depth_samples"] = [
                {
                    "u": float(point["u"]),
                    "v": float(point["v"]),
                    "z_m": float(point["z"]),
                    "depth_value": self._encode_camera_depth_value(
                        depth_m=float(point["z"]),
                        camera_config=camera_config,
                    ),
                    "depth_encoding": camera_config.depth_params.encoding_type,
                }
                for point in preview_points
            ]
        if camera_config.sensor_type.upper() == "SEMANTIC_SEGMENTATION":
            semantic_samples = preview_ground_truth_samples
            payload["preview_semantic_samples"] = semantic_samples
            payload["preview_semantic_legend"] = self._camera_semantic_legend(semantic_samples)
        else:
            payload["preview_semantic_legend"] = []
        if camera_config.sensor_type.upper() == "VISIBLE" and camera_config.image_chain.enabled:
            payload["preview_image_signal_samples"] = [
                self._camera_image_signal_sample(
                    request=request,
                    camera_config=camera_config,
                    intrinsics=intrinsics,
                    projected_sample=point,
                )
                for point in preview_points
            ]
        if camera_config.rolling_shutter.enabled:
            payload["preview_readout_samples"] = [
                self._camera_rolling_shutter_sample(
                    u=float(point["u"]),
                    v=float(point["v"]),
                    camera_config=camera_config,
                    intrinsics=intrinsics,
                )
                for point in preview_points
            ]
        else:
            payload["preview_readout_samples"] = []
        return payload

    def _camera_ground_truth_fields(
        self,
        *,
        camera_config: CameraSensorConfig,
    ) -> list[str]:
        fields = ["GROUND_TRUTH_SEMANTIC_CLASS"]
        semantic_params = camera_config.semantic_params
        if semantic_params.include_actor_id:
            fields.append("GROUND_TRUTH_ACTOR_ID")
        if semantic_params.include_component_id:
            fields.append("GROUND_TRUTH_COMPONENT_ID")
        if semantic_params.include_material_class:
            fields.append("GROUND_TRUTH_MATERIAL_CLASS")
        if semantic_params.include_material_uuid:
            fields.append("GROUND_TRUTH_MATERIAL_UUID")
        if semantic_params.include_base_map_element:
            fields.append("GROUND_TRUTH_BASE_MAP_ELEMENT")
        if semantic_params.include_procedural_map_element:
            fields.append("GROUND_TRUTH_PROCEDURAL_MAP_ELEMENT")
        if semantic_params.include_lane_marking_id:
            fields.append("GROUND_TRUTH_LANE_MARKING_ID")
        return fields

    def _lidar_ground_truth_fields(self) -> list[str]:
        return [
            "GROUND_TRUTH_SEMANTIC_CLASS",
            "GROUND_TRUTH_ACTOR_ID",
            "GROUND_TRUTH_COMPONENT_ID",
            "GROUND_TRUTH_MATERIAL_CLASS",
            "GROUND_TRUTH_MATERIAL_UUID",
            "GROUND_TRUTH_BASE_MAP_ELEMENT",
            "GROUND_TRUTH_PROCEDURAL_MAP_ELEMENT",
            "GROUND_TRUTH_LANE_MARKING_ID",
        ]

    def _radar_ground_truth_fields(self) -> list[str]:
        return [
            "GROUND_TRUTH_SEMANTIC_CLASS",
            "GROUND_TRUTH_ACTOR_ID",
        ]

    def _rotation_matrix_from_extrinsics(
        self,
        *,
        extrinsics: CameraExtrinsics,
    ) -> tuple[tuple[float, float, float], tuple[float, float, float], tuple[float, float, float]]:
        roll = extrinsics.roll_deg * pi / 180.0
        pitch = extrinsics.pitch_deg * pi / 180.0
        yaw = extrinsics.yaw_deg * pi / 180.0

        cr = cos(roll)
        sr = sin(roll)
        cp = cos(pitch)
        sp = sin(pitch)
        cy = cos(yaw)
        sy = sin(yaw)

        return (
            (
                cy * cp,
                cy * sp * sr - sy * cr,
                cy * sp * cr + sy * sr,
            ),
            (
                sy * cp,
                sy * sp * sr + cy * cr,
                sy * sp * cr - cy * sr,
            ),
            (
                -sp,
                cp * sr,
                cp * cr,
            ),
        )

    def _rotate_sensor_vector_to_world(
        self,
        *,
        extrinsics: CameraExtrinsics,
        vector_xyz: tuple[float, float, float],
    ) -> tuple[float, float, float]:
        rotation = self._rotation_matrix_from_extrinsics(extrinsics=extrinsics)
        vx, vy, vz = vector_xyz
        return (
            rotation[0][0] * vx + rotation[1][0] * vy + rotation[2][0] * vz,
            rotation[0][1] * vx + rotation[1][1] * vy + rotation[2][1] * vz,
            rotation[0][2] * vx + rotation[1][2] * vy + rotation[2][2] * vz,
        )

    def _parse_position_vector(
        self,
        *,
        raw: object,
        reference_point: tuple[float, float, float] | None = None,
    ) -> tuple[float, float, float] | None:
        position: tuple[float, float, float] | None = None
        if isinstance(raw, dict):
            position = (
                float(raw.get("x", raw.get("tx", 0.0))),
                float(raw.get("y", raw.get("ty", 0.0))),
                float(raw.get("z", raw.get("tz", 0.0))),
            )
        elif isinstance(raw, (list, tuple)) and len(raw) >= 3:
            position = (float(raw[0]), float(raw[1]), float(raw[2]))
        if position is None:
            return None
        if reference_point is None:
            return position
        return (
            position[0] - reference_point[0],
            position[1] - reference_point[1],
            position[2] - reference_point[2],
        )

    def _sensor_behavior_actor_position_map(
        self,
        *,
        request: SensorSimRequest,
        points_xyz: list[tuple[float, float, float]],
        reference_point: tuple[float, float, float] | None = None,
    ) -> dict[str, tuple[float, float, float]]:
        positions: dict[str, tuple[float, float, float]] = {}
        for option_name in ("sensor_behavior_actor_positions", "behavior_actor_positions", "actor_positions"):
            raw_positions = request.options.get(option_name)
            if not isinstance(raw_positions, dict):
                continue
            for actor_id, value in raw_positions.items():
                parsed = self._parse_position_vector(raw=value, reference_point=reference_point)
                if parsed is not None:
                    positions[str(actor_id)] = parsed

        sums: dict[str, list[float]] = {}

        def accumulate(actor_key: str, point_xyz: tuple[float, float, float]) -> None:
            bucket = sums.setdefault(actor_key, [0.0, 0.0, 0.0, 0.0])
            bucket[0] += point_xyz[0]
            bucket[1] += point_xyz[1]
            bucket[2] += point_xyz[2]
            bucket[3] += 1.0

        for option_name in ("camera_point_actor_ids", "lidar_point_actor_ids", "radar_point_actor_ids"):
            raw_actor_ids = request.options.get(option_name)
            if not isinstance(raw_actor_ids, list):
                continue
            for point_index, raw_actor_id in enumerate(raw_actor_ids):
                if point_index >= len(points_xyz):
                    break
                actor_id = self._coerce_actor_id(raw_actor_id)
                if actor_id is None:
                    continue
                accumulate(actor_id, points_xyz[point_index])

        raw_semantic_labels = request.options.get("camera_semantic_point_labels")
        if isinstance(raw_semantic_labels, list):
            for point_index, label in enumerate(raw_semantic_labels):
                if point_index >= len(points_xyz) or not isinstance(label, dict):
                    continue
                actor_id = self._coerce_actor_id(label.get("actor_id"))
                if actor_id is None:
                    continue
                accumulate(actor_id, points_xyz[point_index])

        for actor_id, bucket in sums.items():
            if actor_id in positions or bucket[3] <= 0.0:
                continue
            positions[actor_id] = (
                bucket[0] / bucket[3],
                bucket[1] / bucket[3],
                bucket[2] / bucket[3],
            )
        return positions

    def _sensor_behavior_target_position(
        self,
        *,
        actor_positions: dict[str, tuple[float, float, float]],
        target_actor_id: str | None,
        points_xyz: list[tuple[float, float, float]],
    ) -> tuple[float, float, float] | None:
        actor_id = self._coerce_actor_id(target_actor_id)
        if actor_id is None:
            return None
        if actor_id in actor_positions:
            return actor_positions[actor_id]
        try:
            fallback_index = int(actor_id) - 1
        except ValueError:
            return None
        if 0 <= fallback_index < len(points_xyz):
            return points_xyz[fallback_index]
        return None

    def _point_at_pitch_yaw(
        self,
        *,
        direction_xyz: tuple[float, float, float],
        roll_deg: float,
    ) -> tuple[float, float] | None:
        dx, dy, dz = direction_xyz
        direction_norm = sqrt(dx * dx + dy * dy + dz * dz)
        if direction_norm <= 1e-9:
            return None
        roll_rad = roll_deg * pi / 180.0
        cr = cos(roll_rad)
        sr = sin(roll_rad)
        rolled_x = dx
        rolled_y = cr * dy - sr * dz
        rolled_z = sr * dy + cr * dz
        yaw_deg = atan2(rolled_y, rolled_x) * 180.0 / pi
        pitch_deg = -atan2(
            sqrt(rolled_x * rolled_x + rolled_y * rolled_y),
            rolled_z,
        ) * 180.0 / pi
        return pitch_deg, yaw_deg

    def _apply_sensor_behaviors_to_extrinsics(
        self,
        *,
        request: SensorSimRequest,
        sensor_name: str,
        base_extrinsics: CameraExtrinsics,
        behaviors: list[Any],
        actor_positions: dict[str, tuple[float, float, float]],
        points_xyz: list[tuple[float, float, float]],
        eval_time_s: float,
    ) -> tuple[CameraExtrinsics, dict[str, object]]:
        if not behaviors:
            return base_extrinsics, {
                "applied": False,
                "behavior_kind": None,
                "note": "no_behavior",
                "time_s": eval_time_s,
            }
        behavior = behaviors[0]
        behavior_kind = str(getattr(behavior, "kind", "")).strip().lower()
        if behavior_kind == "continuous_motion":
            delta_world = self._rotate_sensor_vector_to_world(
                extrinsics=base_extrinsics,
                vector_xyz=(
                    float(getattr(behavior, "tx", 0.0)) * eval_time_s,
                    float(getattr(behavior, "ty", 0.0)) * eval_time_s,
                    float(getattr(behavior, "tz", 0.0)) * eval_time_s,
                ),
            )
            adjusted = CameraExtrinsics(
                tx=base_extrinsics.tx + delta_world[0],
                ty=base_extrinsics.ty + delta_world[1],
                tz=base_extrinsics.tz + delta_world[2],
                roll_deg=base_extrinsics.roll_deg + float(getattr(behavior, "rx", 0.0)) * eval_time_s * 180.0 / pi,
                pitch_deg=base_extrinsics.pitch_deg + float(getattr(behavior, "ry", 0.0)) * eval_time_s * 180.0 / pi,
                yaw_deg=base_extrinsics.yaw_deg + float(getattr(behavior, "rz", 0.0)) * eval_time_s * 180.0 / pi,
                enabled=True,
            )
            return adjusted, {
                "applied": True,
                "behavior_kind": "continuous_motion",
                "time_s": eval_time_s,
                "translation_delta_m": {
                    "x": delta_world[0],
                    "y": delta_world[1],
                    "z": delta_world[2],
                },
                "rotation_delta_deg": {
                    "roll": float(getattr(behavior, "rx", 0.0)) * eval_time_s * 180.0 / pi,
                    "pitch": float(getattr(behavior, "ry", 0.0)) * eval_time_s * 180.0 / pi,
                    "yaw": float(getattr(behavior, "rz", 0.0)) * eval_time_s * 180.0 / pi,
                },
            }
        if behavior_kind == "point_at":
            target_position = self._sensor_behavior_target_position(
                actor_positions=actor_positions,
                target_actor_id=getattr(behavior, "target_actor_id", None),
                points_xyz=points_xyz,
            )
            if target_position is None:
                return base_extrinsics, {
                    "applied": False,
                    "behavior_kind": "point_at",
                    "time_s": eval_time_s,
                    "target_actor_id": getattr(behavior, "target_actor_id", None),
                    "target_found": False,
                }
            offset = getattr(behavior, "target_center_offset", None)
            offset_x = float(getattr(offset, "x", 0.0)) if offset is not None else 0.0
            offset_y = float(getattr(offset, "y", 0.0)) if offset is not None else 0.0
            offset_z = float(getattr(offset, "z", 0.0)) if offset is not None else 0.0
            target_xyz = (
                target_position[0] + offset_x,
                target_position[1] + offset_y,
                target_position[2] + offset_z,
            )
            pitch_yaw = self._point_at_pitch_yaw(
                direction_xyz=(
                    target_xyz[0] - base_extrinsics.tx,
                    target_xyz[1] - base_extrinsics.ty,
                    target_xyz[2] - base_extrinsics.tz,
                ),
                roll_deg=base_extrinsics.roll_deg,
            )
            if pitch_yaw is None:
                return base_extrinsics, {
                    "applied": False,
                    "behavior_kind": "point_at",
                    "time_s": eval_time_s,
                    "target_actor_id": getattr(behavior, "target_actor_id", None),
                    "target_found": True,
                    "degenerate_target_direction": True,
                }
            adjusted = CameraExtrinsics(
                tx=base_extrinsics.tx,
                ty=base_extrinsics.ty,
                tz=base_extrinsics.tz,
                roll_deg=base_extrinsics.roll_deg,
                pitch_deg=pitch_yaw[0],
                yaw_deg=pitch_yaw[1],
                enabled=True,
            )
            return adjusted, {
                "applied": True,
                "behavior_kind": "point_at",
                "time_s": eval_time_s,
                "target_actor_id": getattr(behavior, "target_actor_id", None),
                "target_found": True,
                "target_position_xyz": {
                    "x": target_xyz[0],
                    "y": target_xyz[1],
                    "z": target_xyz[2],
                },
                "locked_roll_deg": base_extrinsics.roll_deg,
            }
        return base_extrinsics, {
            "applied": False,
            "behavior_kind": behavior_kind or None,
            "note": f"unsupported_behavior_for_{sensor_name}",
            "time_s": eval_time_s,
        }

    def _coverage_identity_list(self, raw: object) -> list[object]:
        if isinstance(raw, list):
            filtered = [item for item in raw if item not in (None, "", 0, "0")]
            return sorted(filtered, key=lambda item: str(item))
        if raw in (None, "", 0, "0"):
            return []
        return [raw]

    def _build_coverage_summary_from_samples(
        self,
        *,
        samples: list[dict[str, object]],
        count_field: str,
        actor_field: str,
        semantic_class_field: str,
        semantic_name_field: str,
        count_threshold: int,
        component_field: str | None = None,
        material_class_field: str | None = None,
        base_map_field: str | None = None,
        procedural_map_field: str | None = None,
        lane_marking_field: str | None = None,
        detection_type_field: str | None = None,
        excluded_detection_types: set[str] | None = None,
    ) -> dict[str, object]:
        excluded_types = {item.upper() for item in (excluded_detection_types or set())}
        targets: dict[str, dict[str, object]] = {}
        anonymous_count = 0
        excluded_count = 0
        for sample in samples:
            if not isinstance(sample, dict):
                continue
            detection_type = (
                str(sample.get(detection_type_field, "")).upper()
                if detection_type_field is not None
                else ""
            )
            if detection_type and detection_type in excluded_types:
                excluded_count += 1
                continue
            actor_id = self._coerce_actor_id(sample.get(actor_field))
            if actor_id is None:
                anonymous_count += 1
                continue
            target = targets.setdefault(
                actor_id,
                {
                    "target_key": f"actor:{actor_id}",
                    "actor_id": actor_id,
                    count_field: 0,
                    "semantic_class_ids": [],
                    "semantic_class_names": [],
                    "component_ids": [],
                    "material_class_ids": [],
                    "base_map_element_ids": [],
                    "procedural_map_element_ids": [],
                    "lane_marking_ids": [],
                    "detection_type_counts": {},
                    "frame_presence_count": 1,
                },
            )
            target[count_field] = int(target[count_field]) + 1
            target["semantic_class_ids"] = self._coverage_identity_list(
                list(target["semantic_class_ids"])
                + [self._coerce_int(sample.get(semantic_class_field), 0)]
            )
            semantic_name = str(sample.get(semantic_name_field, "")).strip()
            if semantic_name:
                target["semantic_class_names"] = self._coverage_identity_list(
                    list(target["semantic_class_names"]) + [semantic_name]
                )
            if component_field is not None:
                target["component_ids"] = self._coverage_identity_list(
                    list(target["component_ids"]) + [self._coerce_int(sample.get(component_field), 0)]
                )
            if material_class_field is not None:
                target["material_class_ids"] = self._coverage_identity_list(
                    list(target["material_class_ids"])
                    + [self._coerce_int(sample.get(material_class_field), 0)]
                )
            if base_map_field is not None:
                target["base_map_element_ids"] = self._coverage_identity_list(
                    list(target["base_map_element_ids"])
                    + [self._coerce_int(sample.get(base_map_field), 0)]
                )
            if procedural_map_field is not None:
                target["procedural_map_element_ids"] = self._coverage_identity_list(
                    list(target["procedural_map_element_ids"])
                    + [self._coerce_int(sample.get(procedural_map_field), 0)]
                )
            if lane_marking_field is not None:
                target["lane_marking_ids"] = self._coverage_identity_list(
                    list(target["lane_marking_ids"])
                    + [self._coerce_int(sample.get(lane_marking_field), 0)]
                )
            if detection_type:
                counts = dict(target["detection_type_counts"])
                counts[detection_type] = int(counts.get(detection_type, 0)) + 1
                target["detection_type_counts"] = counts
        ordered_targets = sorted(
            targets.values(),
            key=lambda item: (-int(item.get(count_field, 0)), str(item.get("actor_id", ""))),
        )
        for target in ordered_targets:
            target["covered"] = int(target.get(count_field, 0)) >= max(count_threshold, 1)
        return {
            "total_observation_count": len(samples),
            "anonymous_observation_count": anonymous_count,
            "excluded_observation_count": excluded_count,
            "targets": ordered_targets,
        }

    def _merge_sensor_coverage_targets(
        self,
        *,
        target_lists: list[list[dict[str, object]]],
        count_field: str,
        count_threshold: int,
    ) -> list[dict[str, object]]:
        merged: dict[str, dict[str, object]] = {}
        for targets in target_lists:
            for target in targets:
                if not isinstance(target, dict):
                    continue
                actor_id = self._coerce_actor_id(target.get("actor_id"))
                if actor_id is None:
                    continue
                aggregate = merged.setdefault(
                    actor_id,
                    {
                        "target_key": f"actor:{actor_id}",
                        "actor_id": actor_id,
                        count_field: 0,
                        "semantic_class_ids": [],
                        "semantic_class_names": [],
                        "component_ids": [],
                        "material_class_ids": [],
                        "base_map_element_ids": [],
                        "procedural_map_element_ids": [],
                        "lane_marking_ids": [],
                        "detection_type_counts": {},
                        "frame_presence_count": 0,
                    },
                )
                aggregate[count_field] = int(aggregate[count_field]) + int(target.get(count_field, 0))
                aggregate["frame_presence_count"] = int(aggregate["frame_presence_count"]) + int(
                    target.get("frame_presence_count", 1)
                )
                for key in (
                    "semantic_class_ids",
                    "semantic_class_names",
                    "component_ids",
                    "material_class_ids",
                    "base_map_element_ids",
                    "procedural_map_element_ids",
                    "lane_marking_ids",
                ):
                    aggregate[key] = self._coverage_identity_list(
                        list(aggregate[key]) + list(target.get(key, []))
                    )
                counts = dict(aggregate["detection_type_counts"])
                for label, value in dict(target.get("detection_type_counts", {})).items():
                    counts[str(label)] = int(counts.get(str(label), 0)) + int(value)
                aggregate["detection_type_counts"] = counts
        ordered_targets = sorted(
            merged.values(),
            key=lambda item: (-int(item.get(count_field, 0)), str(item.get("actor_id", ""))),
        )
        for target in ordered_targets:
            target["covered"] = int(target.get(count_field, 0)) >= max(count_threshold, 1)
        return ordered_targets

    def _read_json_artifact(self, path: Path | None) -> dict[str, object] | None:
        if path is None or not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None

    def _sensor_coverage_payload_from_artifacts(
        self,
        *,
        artifacts: dict[str, Path],
        sweep_key: str,
        preview_key: str,
    ) -> tuple[dict[str, object] | None, Path | None]:
        sweep_artifact = artifacts.get(sweep_key)
        sweep_payload = self._read_json_artifact(sweep_artifact)
        if sweep_payload is not None:
            return sweep_payload, sweep_artifact
        preview_artifact = artifacts.get(preview_key)
        preview_payload = self._read_json_artifact(preview_artifact)
        if preview_payload is not None:
            return preview_payload, preview_artifact
        return None, None

    def _sensor_coverage_summary_from_payload(
        self,
        *,
        payload: dict[str, object] | None,
        source_artifact: Path | None,
    ) -> dict[str, object]:
        targets = payload.get("coverage_targets") if isinstance(payload, dict) else None
        normalized_targets = targets if isinstance(targets, list) else []
        target_count = len(normalized_targets)
        covered_target_count = sum(
            1
            for target in normalized_targets
            if isinstance(target, dict) and bool(target.get("covered"))
        )
        return {
            "available": payload is not None,
            "source_artifact": str(source_artifact) if source_artifact is not None else None,
            "metric_name": payload.get("coverage_metric_name") if isinstance(payload, dict) else None,
            "ground_truth_fields": payload.get("ground_truth_fields", []) if isinstance(payload, dict) else [],
            "total_observation_count": int(payload.get("coverage_total_observation_count", 0))
            if isinstance(payload, dict)
            else 0,
            "anonymous_observation_count": int(
                payload.get("coverage_anonymous_observation_count", 0)
            )
            if isinstance(payload, dict)
            else 0,
            "excluded_observation_count": int(
                payload.get("coverage_excluded_observation_count", 0)
            )
            if isinstance(payload, dict)
            else 0,
            "target_count": target_count,
            "covered_target_count": covered_target_count,
            "coverage_ratio": (float(covered_target_count) / float(target_count)) if target_count > 0 else 0.0,
            "targets": normalized_targets,
        }

    def _combined_sensor_coverage_summary(
        self,
        *,
        config: SensorSimConfig,
        camera_summary: dict[str, object],
        lidar_summary: dict[str, object],
        radar_summary: dict[str, object],
    ) -> dict[str, object]:
        combined_targets: dict[str, dict[str, object]] = {}
        sensor_specs = (
            ("camera", "pixels_on_target", "camera_pixels_on_target", camera_summary),
            ("lidar", "lidar_points_on_target", "lidar_points_on_target", lidar_summary),
            ("radar", "radar_detections_on_target", "radar_detections_on_target", radar_summary),
        )
        available_sensor_names = {
            sensor_name
            for sensor_name, _count_field, _output_count_field, summary in sensor_specs
            if bool(summary.get("available"))
        }
        for sensor_name, count_field, output_count_field, summary in sensor_specs:
            for target in list(summary.get("targets", [])):
                if not isinstance(target, dict):
                    continue
                actor_id = self._coerce_actor_id(target.get("actor_id"))
                if actor_id is None:
                    continue
                combined = combined_targets.setdefault(
                    actor_id,
                    {
                        "target_key": f"actor:{actor_id}",
                        "actor_id": actor_id,
                        "semantic_class_ids": [],
                        "semantic_class_names": [],
                        "camera_pixels_on_target": 0,
                        "lidar_points_on_target": 0,
                        "radar_detections_on_target": 0,
                        "camera_covered": False,
                        "lidar_covered": False,
                        "radar_covered": False,
                    },
                )
                combined["semantic_class_ids"] = self._coverage_identity_list(
                    list(combined["semantic_class_ids"]) + list(target.get("semantic_class_ids", []))
                )
                combined["semantic_class_names"] = self._coverage_identity_list(
                    list(combined["semantic_class_names"])
                    + list(target.get("semantic_class_names", []))
                )
                combined[output_count_field] = int(combined[output_count_field]) + int(
                    target.get(count_field, 0)
                )
                combined[f"{sensor_name}_covered"] = bool(target.get("covered"))

        ordered_targets = sorted(
            combined_targets.values(),
            key=lambda item: (
                -(
                    int(item.get("camera_pixels_on_target", 0))
                    + int(item.get("lidar_points_on_target", 0))
                    + int(item.get("radar_detections_on_target", 0))
                ),
                str(item.get("actor_id", "")),
            ),
        )
        covered_target_count = 0
        blindspot_target_count = 0
        overlap_target_count = 0
        for target in ordered_targets:
            covering_sensor_count = sum(
                1
                for sensor_name in ("camera", "lidar", "radar")
                if bool(target.get(f"{sensor_name}_covered"))
            )
            target["covering_sensor_count"] = covering_sensor_count
            target["covered_by_any_sensor"] = covering_sensor_count > 0
            target["covered_by_all_available_sensors"] = (
                covering_sensor_count == len(available_sensor_names)
                if available_sensor_names
                else False
            )
            if target["covered_by_any_sensor"]:
                covered_target_count += 1
            else:
                blindspot_target_count += 1
            if covering_sensor_count >= 2:
                overlap_target_count += 1
        target_count = len(ordered_targets)
        return {
            "target_count": target_count,
            "covered_target_count": covered_target_count,
            "blindspot_target_count": blindspot_target_count,
            "overlap_target_count": overlap_target_count,
            "coverage_ratio": (float(covered_target_count) / float(target_count)) if target_count > 0 else 0.0,
            "available_sensor_count": len(available_sensor_names),
            "available_sensors": sorted(available_sensor_names),
            "thresholds": config.coverage.to_dict(),
            "targets": ordered_targets,
        }

    def _generate_sensor_coverage_summary_if_available(
        self,
        *,
        request: SensorSimRequest,
        artifacts: dict[str, Path],
        enhanced_output: Path,
        metrics: dict[str, float],
    ) -> Path | None:
        config = self._sensor_config_from_request(request)
        if not config.coverage.enabled:
            metrics["coverage_summary_generated"] = 0.0
            return None
        camera_payload, camera_source = self._sensor_coverage_payload_from_artifacts(
            artifacts=artifacts,
            sweep_key="camera_projection_trajectory_sweep",
            preview_key="camera_projection_preview",
        )
        lidar_payload, lidar_source = self._sensor_coverage_payload_from_artifacts(
            artifacts=artifacts,
            sweep_key="lidar_trajectory_sweep",
            preview_key="lidar_noisy_preview_json",
        )
        radar_payload, radar_source = self._sensor_coverage_payload_from_artifacts(
            artifacts=artifacts,
            sweep_key="radar_targets_trajectory_sweep",
            preview_key="radar_targets_preview",
        )
        camera_summary = self._sensor_coverage_summary_from_payload(
            payload=camera_payload,
            source_artifact=camera_source,
        )
        lidar_summary = self._sensor_coverage_summary_from_payload(
            payload=lidar_payload,
            source_artifact=lidar_source,
        )
        radar_summary = self._sensor_coverage_summary_from_payload(
            payload=radar_payload,
            source_artifact=radar_source,
        )
        combined_summary = self._combined_sensor_coverage_summary(
            config=config,
            camera_summary=camera_summary,
            lidar_summary=lidar_summary,
            radar_summary=radar_summary,
        )
        summary = {
            "schema_version": "1.0",
            "coverage_metrics": config.coverage.to_dict(),
            "ground_truth_fields": {
                "camera": camera_summary.get("ground_truth_fields", []),
                "lidar": lidar_summary.get("ground_truth_fields", []),
                "radar": radar_summary.get("ground_truth_fields", []),
            },
            "sensors": {
                "camera": camera_summary,
                "lidar": lidar_summary,
                "radar": radar_summary,
            },
            "combined": combined_summary,
        }
        output_path = enhanced_output / "sensor_coverage_summary.json"
        output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        metrics["coverage_summary_generated"] = 1.0
        metrics["coverage_camera_target_count"] = float(camera_summary["target_count"])
        metrics["coverage_lidar_target_count"] = float(lidar_summary["target_count"])
        metrics["coverage_radar_target_count"] = float(radar_summary["target_count"])
        metrics["coverage_camera_ratio"] = float(camera_summary["coverage_ratio"])
        metrics["coverage_lidar_ratio"] = float(lidar_summary["coverage_ratio"])
        metrics["coverage_radar_ratio"] = float(radar_summary["coverage_ratio"])
        metrics["coverage_combined_target_count"] = float(combined_summary["target_count"])
        metrics["coverage_combined_covered_target_count"] = float(
            combined_summary["covered_target_count"]
        )
        metrics["coverage_combined_blindspot_target_count"] = float(
            combined_summary["blindspot_target_count"]
        )
        metrics["coverage_combined_overlap_target_count"] = float(
            combined_summary["overlap_target_count"]
        )
        metrics["coverage_combined_ratio"] = float(combined_summary["coverage_ratio"])
        return output_path

    def _project_camera_points_with_optional_rolling_shutter(
        self,
        *,
        request: SensorSimRequest,
        points_xyz: list[tuple[float, float, float]],
        camera_config: CameraSensorConfig,
        intrinsics: CameraIntrinsics,
        distortion: BrownConradyDistortion,
        base_extrinsics: CameraExtrinsics,
        trajectory_poses: list[TrajectoryPose],
        base_pose: TrajectoryPose | None,
    ) -> tuple[list[dict[str, object]], dict[str, object]]:
        base_projected = self._project_camera_samples_for_extrinsics(
            points_xyz=points_xyz,
            camera_config=camera_config,
            intrinsics=intrinsics,
            distortion=distortion,
            extrinsics=base_extrinsics,
        )
        if (
            not camera_config.rolling_shutter.enabled
            or len(trajectory_poses) < 2
            or base_pose is None
        ):
            return base_projected, {
                "enabled": camera_config.rolling_shutter.enabled,
                "applied": False,
                "trajectory_available": len(trajectory_poses) >= 2,
                "pose_count": len(trajectory_poses),
                "base_pose_time_s": base_pose.time_s if base_pose is not None else None,
            }

        distorted_points: list[dict[str, object]] = []
        for point_index, point in enumerate(points_xyz):
            base_projection = self._project_camera_samples_for_extrinsics(
                points_xyz=[point],
                camera_config=camera_config,
                intrinsics=intrinsics,
                distortion=distortion,
                extrinsics=base_extrinsics,
                clamp_to_image=False,
                point_index_offset=point_index,
            )
            if not base_projection:
                continue
            base_sample = base_projection[0]
            base_u = float(base_sample["u"])
            base_v = float(base_sample["v"])
            readout = self._camera_rolling_shutter_sample(
                u=base_u,
                v=base_v,
                camera_config=camera_config,
                intrinsics=intrinsics,
            )
            sample_uvz: list[tuple[float, float, float]] = []
            for relative_time_s in readout["sample_times_s"]:
                pose = self._interpolate_trajectory_pose_at_time(
                    poses=trajectory_poses,
                    target_time_s=base_pose.time_s + float(relative_time_s),
                )
                distorted_extrinsics = self._apply_camera_pose_delta_to_extrinsics(
                    base_extrinsics=base_extrinsics,
                    base_pose=base_pose,
                    sample_pose=pose,
                )
                projected = self._project_camera_samples_for_extrinsics(
                    points_xyz=[point],
                    camera_config=camera_config,
                    intrinsics=intrinsics,
                    distortion=distortion,
                    extrinsics=distorted_extrinsics,
                    point_index_offset=point_index,
                )
                if projected:
                    sample_uvz.append(
                        (
                            float(projected[0]["u"]),
                            float(projected[0]["v"]),
                            float(projected[0]["z"]),
                        )
                    )
            if sample_uvz:
                count = float(len(sample_uvz))
                distorted_points.append(
                    {
                        "u": sum(value[0] for value in sample_uvz) / count,
                        "v": sum(value[1] for value in sample_uvz) / count,
                        "z": sum(value[2] for value in sample_uvz) / count,
                        "point_index": point_index,
                        "world_x": point[0],
                        "world_y": point[1],
                        "world_z": point[2],
                    }
                )

        return distorted_points, {
            "enabled": True,
            "applied": True,
            "trajectory_available": True,
            "pose_count": len(trajectory_poses),
            "base_pose_time_s": base_pose.time_s,
        }

    def _project_camera_samples_for_extrinsics(
        self,
        *,
        points_xyz: list[tuple[float, float, float]],
        camera_config: CameraSensorConfig,
        intrinsics: CameraIntrinsics,
        distortion: BrownConradyDistortion,
        extrinsics: CameraExtrinsics,
        clamp_to_image: bool | None = None,
        point_index_offset: int = 0,
    ) -> list[dict[str, object]]:
        effective_clamp = (
            camera_config.projection_clamp_to_image
            if clamp_to_image is None
            else clamp_to_image
        )
        projected_samples: list[dict[str, object]] = []
        for point_index, point in enumerate(points_xyz):
            camera_points = transform_points_world_to_camera(
                points_xyz=[point],
                extrinsics=extrinsics,
            )
            projected = project_points_brown_conrady(
                points_xyz=camera_points,
                intrinsics=intrinsics,
                distortion=distortion,
                geometry_model=camera_config.geometry_model,
                clamp_to_image=effective_clamp,
            )
            if not projected:
                continue
            u, v, z = projected[0]
            projected_samples.append(
                {
                    "u": u,
                    "v": v,
                    "z": z,
                    "point_index": point_index_offset + point_index,
                    "world_x": point[0],
                    "world_y": point[1],
                    "world_z": point[2],
                }
            )
        return projected_samples

    def _camera_semantic_sample(
        self,
        *,
        request: SensorSimRequest,
        camera_config: CameraSensorConfig,
        projected_sample: dict[str, object],
    ) -> dict[str, object]:
        point_index = int(projected_sample.get("point_index", 0))
        world_point = (
            float(projected_sample.get("world_x", 0.0)),
            float(projected_sample.get("world_y", 0.0)),
            float(projected_sample.get("world_z", 0.0)),
        )
        semantic = self._camera_semantic_override_for_point(
            request=request,
            point_index=point_index,
            default_class_version=camera_config.semantic_params.class_version,
        )
        source = "annotation_override"
        if semantic is None:
            semantic = self._camera_semantic_fallback_label(
                world_point=world_point,
                camera_config=camera_config,
                point_index=point_index,
            )
            source = "heuristic"
        semantic_payload = {
            "u": float(projected_sample["u"]),
            "v": float(projected_sample["v"]),
            "z_m": float(projected_sample["z"]),
            "semantic_class_id": int(semantic["semantic_class_id"]),
            "semantic_class_name": str(semantic["semantic_class_name"]),
            "semantic_parent_class": str(semantic["semantic_parent_class"]),
            "color_rgb": list(semantic["color_rgb"]),
            "source": source,
            "ground_truth_semantic_class": int(semantic["semantic_class_id"]),
        }
        semantic_params = camera_config.semantic_params
        if semantic_params.include_actor_id:
            semantic_payload["actor_id"] = int(semantic["actor_id"])
            semantic_payload["ground_truth_actor_id"] = int(semantic["actor_id"])
        if semantic_params.include_component_id:
            semantic_payload["component_id"] = int(semantic["component_id"])
            semantic_payload["ground_truth_component_id"] = int(semantic["component_id"])
        if semantic_params.include_material_class:
            semantic_payload["material_class_id"] = int(semantic["material_class_id"])
            semantic_payload["ground_truth_material_class"] = int(semantic["material_class_id"])
        if semantic_params.include_material_uuid:
            semantic_payload["material_uuid"] = int(semantic["material_uuid"])
            semantic_payload["ground_truth_material_uuid"] = int(semantic["material_uuid"])
        if semantic_params.include_base_map_element:
            semantic_payload["base_map_element_id"] = int(semantic["base_map_element_id"])
            semantic_payload["ground_truth_base_map_element"] = int(
                semantic["base_map_element_id"]
            )
        if semantic_params.include_procedural_map_element:
            semantic_payload["procedural_map_element_id"] = int(
                semantic["procedural_map_element_id"]
            )
            semantic_payload["ground_truth_procedural_map_element"] = int(
                semantic["procedural_map_element_id"]
            )
        if semantic_params.include_lane_marking_id:
            semantic_payload["lane_marking_id"] = int(semantic["lane_marking_id"])
            semantic_payload["ground_truth_lane_marking_id"] = int(semantic["lane_marking_id"])
        return semantic_payload

    def _ground_truth_class_version(
        self,
        *,
        request: SensorSimRequest,
        sensor_prefix: str | None = None,
        default: str = "LEGACY",
    ) -> str:
        if sensor_prefix:
            prefixed = request.options.get(f"{sensor_prefix}_ground_truth_class_version")
            if isinstance(prefixed, str) and prefixed.strip():
                return prefixed.strip().upper()
        explicit = request.options.get("ground_truth_class_version")
        if isinstance(explicit, str) and explicit.strip():
            return explicit.strip().upper()
        camera_version = request.options.get("camera_semantic_class_version")
        if isinstance(camera_version, str) and camera_version.strip():
            return camera_version.strip().upper()
        semantic_params = request.options.get("camera_semantic_params")
        if isinstance(semantic_params, dict):
            raw = semantic_params.get("class_version")
            if isinstance(raw, str) and raw.strip():
                return raw.strip().upper()
        return default.upper()

    def _ground_truth_fallback_label(
        self,
        *,
        world_point: tuple[float, float, float],
        class_version: str,
        point_index: int,
    ) -> dict[str, object]:
        x, y, _z = world_point
        normalized_class_version = class_version.upper()
        if normalized_class_version == "GRANULAR_SEGMENTATION":
            if abs(y) <= 0.15 and abs(x) <= 0.8:
                class_id = 2501
                lane_marking_id = 9000 + point_index
            elif abs(y) <= 0.4:
                class_id = 5000
                lane_marking_id = 0
            elif abs(y) <= 0.8:
                class_id = 1002
                lane_marking_id = 0
            elif abs(x) <= 1.8 and y > 0.8:
                class_id = 7501
                lane_marking_id = 0
            elif abs(x) <= 3.5 and y > 0.8:
                class_id = 4000
                lane_marking_id = 0
            elif y < -1.0:
                class_id = 205
                lane_marking_id = 0
            elif abs(x) > 6.0:
                class_id = 3501
                lane_marking_id = 0
            else:
                class_id = 6003
                lane_marking_id = 0
        else:
            if abs(y) <= 0.15 and abs(x) <= 0.8:
                class_id = 6
                lane_marking_id = 9000 + point_index
            elif abs(y) <= 0.4:
                class_id = 7
                lane_marking_id = 0
            elif abs(y) <= 0.8:
                class_id = 8
                lane_marking_id = 0
            elif abs(x) <= 1.8 and y > 0.8:
                class_id = 10
                lane_marking_id = 0
            elif abs(x) <= 3.5 and y > 0.8:
                class_id = 4
                lane_marking_id = 0
            elif y < -1.0:
                class_id = 1
                lane_marking_id = 0
            elif abs(x) > 6.0:
                class_id = 12
                lane_marking_id = 0
            else:
                class_id = 9
                lane_marking_id = 0
        entry = self._camera_semantic_entry_for_class(
            class_version=normalized_class_version,
            class_id=class_id,
        )
        return {
            "semantic_class_id": class_id,
            "semantic_class_name": entry["name"],
            "semantic_parent_class": entry["parent_class"],
            "color_rgb": entry["color_rgb"],
            "actor_id": point_index + 1,
            "component_id": 1000 + point_index,
            "material_class_id": int(entry["material_class_id"]),
            "material_uuid": 0,
            "base_map_element_id": 500 + point_index if class_id in {7, 5000, 8, 5500} else 0,
            "procedural_map_element_id": 700 + point_index if class_id in {9, 6003} else 0,
            "lane_marking_id": lane_marking_id,
        }

    def _point_annotation_value(
        self,
        *,
        request: SensorSimRequest,
        option_names: list[str],
        point_index: int,
    ) -> object | None:
        for option_name in option_names:
            values = request.options.get(option_name)
            if isinstance(values, list) and 0 <= point_index < len(values):
                return values[point_index]
        return None

    def _coerce_actor_id(self, raw: object) -> str | None:
        if raw is None:
            return None
        if isinstance(raw, str):
            value = raw.strip()
            return value or None
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            return str(int(raw)) if float(raw).is_integer() else str(raw)
        return None

    def _sensor_ground_truth_annotation(
        self,
        *,
        request: SensorSimRequest,
        sensor_prefix: str,
        point_index: int,
        world_point: tuple[float, float, float],
    ) -> dict[str, object]:
        class_version = self._ground_truth_class_version(
            request=request,
            sensor_prefix=sensor_prefix,
        )
        fallback = self._ground_truth_fallback_label(
            world_point=world_point,
            class_version=class_version,
            point_index=point_index,
        )
        raw_semantic_class = self._point_annotation_value(
            request=request,
            option_names=[
                f"{sensor_prefix}_point_semantic_classes",
                f"{sensor_prefix}_point_semantic_class_ids",
            ],
            point_index=point_index,
        )
        semantic_class_id = self._coerce_int(raw_semantic_class, int(fallback["semantic_class_id"]))
        semantic_entry = self._camera_semantic_entry_for_class(
            class_version=class_version,
            class_id=semantic_class_id,
            fallback_name=self._point_annotation_value(
                request=request,
                option_names=[f"{sensor_prefix}_point_semantic_class_names"],
                point_index=point_index,
            ),
        )
        actor_id = self._coerce_actor_id(
            self._point_annotation_value(
                request=request,
                option_names=[f"{sensor_prefix}_point_actor_ids"],
                point_index=point_index,
            )
        )
        if actor_id is None:
            actor_id = self._coerce_actor_id(fallback["actor_id"])
        return {
            "ground_truth_semantic_class": semantic_class_id,
            "ground_truth_semantic_class_name": str(semantic_entry["name"]),
            "ground_truth_semantic_parent_class": str(semantic_entry["parent_class"]),
            "ground_truth_actor_id": actor_id,
            "ground_truth_component_id": self._coerce_int(
                self._point_annotation_value(
                    request=request,
                    option_names=[f"{sensor_prefix}_point_component_ids"],
                    point_index=point_index,
                ),
                int(fallback["component_id"]),
            ),
            "ground_truth_material_class": self._coerce_int(
                self._point_annotation_value(
                    request=request,
                    option_names=[f"{sensor_prefix}_point_material_classes"],
                    point_index=point_index,
                ),
                int(fallback["material_class_id"]),
            ),
            "ground_truth_material_uuid": self._coerce_int(
                self._point_annotation_value(
                    request=request,
                    option_names=[f"{sensor_prefix}_point_material_uuids"],
                    point_index=point_index,
                ),
                int(fallback["material_uuid"]),
            ),
            "ground_truth_base_map_element": self._coerce_int(
                self._point_annotation_value(
                    request=request,
                    option_names=[f"{sensor_prefix}_point_base_map_elements"],
                    point_index=point_index,
                ),
                int(fallback["base_map_element_id"]),
            ),
            "ground_truth_procedural_map_element": self._coerce_int(
                self._point_annotation_value(
                    request=request,
                    option_names=[f"{sensor_prefix}_point_procedural_map_elements"],
                    point_index=point_index,
                ),
                int(fallback["procedural_map_element_id"]),
            ),
            "ground_truth_lane_marking_id": self._coerce_int(
                self._point_annotation_value(
                    request=request,
                    option_names=[f"{sensor_prefix}_point_lane_marking_ids"],
                    point_index=point_index,
                ),
                int(fallback["lane_marking_id"]),
            ),
        }

    def _camera_semantic_legend(
        self,
        semantic_samples: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        seen: dict[int, dict[str, object]] = {}
        for sample in semantic_samples:
            class_id = int(sample.get("semantic_class_id", 0))
            if class_id in seen:
                continue
            seen[class_id] = {
                "semantic_class_id": class_id,
                "semantic_class_name": sample.get("semantic_class_name", f"CLASS_{class_id}"),
                "semantic_parent_class": sample.get("semantic_parent_class", "UNKNOWN"),
                "color_rgb": sample.get("color_rgb", [0, 0, 0]),
            }
        return list(seen.values())

    def _camera_semantic_override_for_point(
        self,
        *,
        request: SensorSimRequest,
        point_index: int,
        default_class_version: str,
    ) -> dict[str, object] | None:
        raw_overrides = request.options.get("camera_semantic_point_labels")
        if not isinstance(raw_overrides, list):
            annotations = request.options.get("camera_semantic_annotations")
            if isinstance(annotations, dict):
                raw_overrides = annotations.get("point_labels")
        if not isinstance(raw_overrides, list):
            return None
        for raw_override in raw_overrides:
            if not isinstance(raw_override, dict):
                continue
            override_point_index = self._coerce_int(raw_override.get("point_index"))
            if override_point_index != point_index:
                continue
            class_id = self._coerce_int(raw_override.get("semantic_class_id"), 0)
            entry = self._camera_semantic_entry_for_class(
                class_version=(
                    str(raw_override.get("class_version", "")).strip().upper()
                    or default_class_version
                ),
                class_id=class_id,
                fallback_name=raw_override.get("semantic_class_name"),
            )
            return {
                "semantic_class_id": class_id,
                "semantic_class_name": str(
                    raw_override.get("semantic_class_name", entry["name"])
                ),
                "semantic_parent_class": str(
                    raw_override.get("semantic_parent_class", entry["parent_class"])
                ),
                "color_rgb": list(raw_override.get("color_rgb", entry["color_rgb"])),
                "actor_id": self._coerce_int(raw_override.get("actor_id"), point_index + 1),
                "component_id": self._coerce_int(
                    raw_override.get("component_id"),
                    1000 + point_index,
                ),
                "material_class_id": self._coerce_int(
                    raw_override.get("material_class_id"),
                    int(entry["material_class_id"]),
                ),
                "material_uuid": self._coerce_int(raw_override.get("material_uuid"), 0),
                "base_map_element_id": self._coerce_int(
                    raw_override.get("base_map_element_id"),
                    0,
                ),
                "procedural_map_element_id": self._coerce_int(
                    raw_override.get("procedural_map_element_id"),
                    0,
                ),
                "lane_marking_id": self._coerce_int(raw_override.get("lane_marking_id"), 0),
            }
        return None

    def _camera_semantic_fallback_label(
        self,
        *,
        world_point: tuple[float, float, float],
        camera_config: CameraSensorConfig,
        point_index: int,
    ) -> dict[str, object]:
        return self._ground_truth_fallback_label(
            world_point=world_point,
            class_version=camera_config.semantic_params.class_version,
            point_index=point_index,
        )

    def _camera_semantic_entry_for_class(
        self,
        *,
        class_version: str | None,
        class_id: int,
        fallback_name: object | None = None,
    ) -> dict[str, object]:
        palette = self._camera_semantic_palette(class_version=class_version)
        entry = palette.get(class_id)
        if entry is not None:
            return entry
        return {
            "name": str(fallback_name) if fallback_name is not None else f"CLASS_{class_id}",
            "parent_class": "UNKNOWN",
            "color_rgb": self._camera_semantic_color_from_id(class_id),
            "material_class_id": 0,
        }

    def _camera_semantic_palette(self, *, class_version: str | None) -> dict[int, dict[str, object]]:
        normalized = str(class_version or "LEGACY").strip().upper()
        if normalized == "GRANULAR_SEGMENTATION":
            return _APPLIED_GRANULAR_CAMERA_SEMANTICS
        return _APPLIED_LEGACY_CAMERA_SEMANTICS

    def _camera_semantic_color_from_id(self, class_id: int) -> list[int]:
        value = abs(int(class_id))
        return [
            32 + (value * 53) % 192,
            32 + (value * 97) % 192,
            32 + (value * 193) % 192,
        ]

    def _camera_image_signal_sample(
        self,
        *,
        request: SensorSimRequest,
        camera_config: CameraSensorConfig,
        intrinsics: CameraIntrinsics,
        projected_sample: dict[str, object],
    ) -> dict[str, object]:
        point_index = int(projected_sample.get("point_index", 0))
        z_m = max(float(projected_sample.get("z", 0.0)), 1e-6)
        u = float(projected_sample.get("u", 0.0))
        v = float(projected_sample.get("v", 0.0))
        world_point = (
            float(projected_sample.get("world_x", 0.0)),
            float(projected_sample.get("world_y", 0.0)),
            float(projected_sample.get("world_z", 0.0)),
        )
        semantic = self._camera_semantic_override_for_point(
            request=request,
            point_index=point_index,
            default_class_version=camera_config.semantic_params.class_version,
        )
        semantic_source = "annotation_override"
        if semantic is None:
            semantic = self._camera_semantic_fallback_label(
                world_point=world_point,
                camera_config=camera_config,
                point_index=point_index,
            )
            semantic_source = "heuristic"

        image_chain = camera_config.image_chain
        lens = camera_config.lens_params
        base_rgb = [float(channel) / 255.0 for channel in semantic["color_rgb"]]
        distance_attenuation = 1.0 / max(z_m * z_m, 1.0)
        normalized_u = (u - intrinsics.cx) / float(max(intrinsics.width, 1))
        normalized_v = (v - intrinsics.cy) / float(max(intrinsics.height, 1))
        off_axis = sqrt(normalized_u * normalized_u + normalized_v * normalized_v)
        view_falloff = max(0.45, 1.0 - 0.75 * off_axis)
        scene_luminance = max(0.05, min(8.0, 220.0 * distance_attenuation * view_falloff))
        exposure_scale = (
            image_chain.shutter_speed_us / 6000.0
            * max(float(image_chain.iso), 1.0) / 100.0
            * max(image_chain.analog_gain, 1e-6)
            * max(image_chain.digital_gain, 1e-6)
        )
        signal_photons = max(1.0, scene_luminance * exposure_scale * 1000.0)
        white_balance_gains = self._camera_white_balance_gains(
            image_chain.white_balance_kelvin
        )
        vignetting_factor = self._camera_vignetting_factor(
            off_axis=off_axis,
            intensity=lens.vignetting.intensity,
            alpha=lens.vignetting.alpha,
            radius=lens.vignetting.radius,
        )
        spot_blur_radius_px = self._camera_spot_blur_radius_px(
            intrinsics=intrinsics,
            spot_size=lens.spot_size,
        )
        lens_flare_strength = self._camera_lens_flare_strength(
            scene_luminance=scene_luminance,
            bloom=image_chain.bloom,
            lens_flare=lens.lens_flare,
            off_axis=off_axis,
        )

        linear_rgb: list[float] = []
        lens_rgb: list[float] = []
        digital_rgb: list[int] = []
        noisy_signal_rgb: list[float] = []
        bloom_boost = 0.0
        for channel_index, base_value in enumerate(base_rgb):
            channel_rng = random.Random(
                image_chain.seed
                + point_index * 9973
                + channel_index * 101
                + int(round(u)) * 17
                + int(round(v)) * 19
            )
            photons = max(
                0.0,
                signal_photons
                * base_value
                * white_balance_gains[channel_index]
                * vignetting_factor,
            )
            shot_noise = channel_rng.gauss(0.0, sqrt(max(photons, 1.0))) / 1000.0
            dsnu = channel_rng.gauss(0.0, image_chain.fixed_pattern_noise.dsnu)
            prnu = 1.0 + channel_rng.gauss(0.0, image_chain.fixed_pattern_noise.prnu)
            readout_noise = channel_rng.gauss(0.0, image_chain.readout_noise)
            channel_linear = max(0.0, photons / 1000.0)
            channel_linear = channel_linear * prnu + dsnu + shot_noise + readout_noise
            channel_linear = max(0.0, channel_linear)
            linear_rgb.append(channel_linear)
            noisy_signal_rgb.append(channel_linear)
        flare_rgb = [
            lens_flare_strength * 0.85,
            lens_flare_strength * 0.92,
            lens_flare_strength,
        ]
        lens_rgb = [max(0.0, linear_rgb[index] + flare_rgb[index]) for index in range(3)]
        post_lens_rgb = list(lens_rgb)
        if image_chain.bloom > 0.0:
            bloom_threshold = max(max(lens_rgb) - 1.0, 0.0)
            bloom_boost = image_chain.bloom * bloom_threshold * (1.0 + spot_blur_radius_px * 0.05)
            lens_rgb = [value + bloom_boost for value in lens_rgb]
        gamma = max(image_chain.gamma, 1e-6)
        for channel_linear in lens_rgb:
            encoded = pow(max(min(channel_linear, 1.0), 0.0), 1.0 / gamma)
            digital_rgb.append(int(round(encoded * 255.0)))

        return {
            "u": u,
            "v": v,
            "z_m": z_m,
            "semantic_class_id": int(semantic["semantic_class_id"]),
            "semantic_class_name": str(semantic["semantic_class_name"]),
            "semantic_source": semantic_source,
            "base_rgb_linear": base_rgb,
            "white_balance_gains": white_balance_gains,
            "scene_luminance": scene_luminance,
            "signal_photons": signal_photons,
            "vignetting_factor": vignetting_factor,
            "lens_flare_strength": lens_flare_strength,
            "spot_blur_radius_px": spot_blur_radius_px,
            "noisy_signal_rgb_linear": noisy_signal_rgb,
            "post_lens_rgb_linear": post_lens_rgb,
            "post_bloom_rgb_linear": lens_rgb,
            "digital_rgb": digital_rgb,
            "exposure_scale": exposure_scale,
            "iso": image_chain.iso,
            "shutter_speed_us": image_chain.shutter_speed_us,
            "analog_gain": image_chain.analog_gain,
            "digital_gain": image_chain.digital_gain,
            "readout_noise": image_chain.readout_noise,
            "white_balance_kelvin": image_chain.white_balance_kelvin,
            "gamma": image_chain.gamma,
            "bloom_boost": bloom_boost,
        }

    def _camera_white_balance_gains(self, kelvin: float) -> list[float]:
        temperature = min(max(kelvin, 1000.0), 40000.0) / 100.0
        if temperature <= 66.0:
            red = 255.0
            green = 99.4708025861 * log(max(temperature, 1e-6)) - 161.1195681661
            blue = 0.0 if temperature <= 19.0 else 138.5177312231 * log(temperature - 10.0) - 305.0447927307
        else:
            red = 329.698727446 * pow(temperature - 60.0, -0.1332047592)
            green = 288.1221695283 * pow(temperature - 60.0, -0.0755148492)
            blue = 255.0
        gains = [
            min(max(red, 0.0), 255.0) / 255.0,
            min(max(green, 0.0), 255.0) / 255.0,
            min(max(blue, 0.0), 255.0) / 255.0,
        ]
        max_gain = max(gains)
        if max_gain <= 1e-9:
            return [1.0, 1.0, 1.0]
        return [value / max_gain for value in gains]

    def _camera_vignetting_factor(
        self,
        *,
        off_axis: float,
        intensity: float,
        alpha: float,
        radius: float,
    ) -> float:
        if intensity <= 0.0:
            return 1.0
        safe_radius = max(radius, 1e-6)
        normalized = min(max(off_axis / safe_radius, 0.0), 1.0)
        power = pow(normalized, max(alpha, 1e-6))
        return max(0.05, 1.0 - intensity * power)

    def _camera_spot_blur_radius_px(
        self,
        *,
        intrinsics: CameraIntrinsics,
        spot_size: float,
    ) -> float:
        if spot_size <= 0.0:
            return 0.0
        focal_mean = max((intrinsics.fx + intrinsics.fy) * 0.5, 1.0)
        return max(0.0, spot_size * focal_mean)

    def _camera_lens_flare_strength(
        self,
        *,
        scene_luminance: float,
        bloom: float,
        lens_flare: float,
        off_axis: float,
    ) -> float:
        if lens_flare <= 0.0:
            return 0.0
        center_bias = max(0.0, 1.0 - min(off_axis / 0.65, 1.0))
        highlight = max(scene_luminance - 0.75, 0.0)
        return lens_flare * (0.08 + bloom * 0.04) * highlight * center_bias

    def _coerce_int(self, raw: object, default: int = 0) -> int:
        if raw is None:
            return default
        if isinstance(raw, bool):
            return int(raw)
        if isinstance(raw, int):
            return raw
        if isinstance(raw, float):
            return int(raw)
        if isinstance(raw, str):
            try:
                return int(float(raw.strip()))
            except ValueError:
                return default
        return default

    def _encode_camera_depth_value(
        self,
        *,
        depth_m: float,
        camera_config: CameraSensorConfig,
    ) -> float:
        params = camera_config.depth_params
        clamped = min(max(depth_m, params.min_m), params.max_m)
        encoding = params.encoding_type.upper()
        if encoding == "LOG":
            base = max(params.log_base, 1.000001)
            return log(max(clamped, max(params.min_m, 1e-6)), base)
        return clamped

    def _camera_rolling_shutter_payload(
        self,
        *,
        camera_config: CameraSensorConfig,
        intrinsics: CameraIntrinsics,
        runtime: dict[str, object] | None = None,
    ) -> dict[str, object]:
        config = camera_config.rolling_shutter
        row_delay_s = max(config.row_delay_ns, 0.0) / 1_000_000_000.0
        col_delay_s = max(config.col_delay_ns, 0.0) / 1_000_000_000.0
        total_readout_s = (
            row_delay_s * max(intrinsics.height - 1, 0)
            + col_delay_s * max(intrinsics.width - 1, 0)
        )
        exposure_duration_s = total_readout_s / float(max(config.num_time_steps, 1))
        payload = {
            "enabled": config.enabled,
            "row_delay_ns": config.row_delay_ns,
            "col_delay_ns": config.col_delay_ns,
            "row_readout_direction": config.row_readout_direction,
            "col_readout_direction": config.col_readout_direction,
            "num_time_steps": config.num_time_steps,
            "num_exposure_samples_per_pixel": config.num_exposure_samples_per_pixel,
            "total_readout_s": total_readout_s,
            "exposure_duration_s": exposure_duration_s,
        }
        if runtime is not None:
            payload["applied"] = bool(runtime.get("applied", False))
            payload["trajectory_available"] = bool(runtime.get("trajectory_available", False))
            payload["pose_count"] = int(runtime.get("pose_count", 0))
            payload["base_pose_time_s"] = runtime.get("base_pose_time_s")
        return payload

    def _camera_rolling_shutter_sample(
        self,
        *,
        u: float,
        v: float,
        camera_config: CameraSensorConfig,
        intrinsics: CameraIntrinsics,
    ) -> dict[str, object]:
        config = camera_config.rolling_shutter
        row_delay_s = max(config.row_delay_ns, 0.0) / 1_000_000_000.0
        col_delay_s = max(config.col_delay_ns, 0.0) / 1_000_000_000.0
        row_index = self._camera_readout_index(
            coord=v,
            size=intrinsics.height,
            direction=config.row_readout_direction,
        )
        col_index = self._camera_readout_index(
            coord=u,
            size=intrinsics.width,
            direction=config.col_readout_direction,
        )
        readout_offset_s = row_index * row_delay_s + col_index * col_delay_s
        exposure_duration_s = (
            row_delay_s * max(intrinsics.height - 1, 0)
            + col_delay_s * max(intrinsics.width - 1, 0)
        ) / float(max(config.num_time_steps, 1))
        sample_count = max(config.num_exposure_samples_per_pixel, 1)
        sample_times_s = [
            readout_offset_s + exposure_duration_s * ((sample_idx + 0.5) / float(sample_count))
            for sample_idx in range(sample_count)
        ]
        return {
            "u": u,
            "v": v,
            "row_index": row_index,
            "col_index": col_index,
            "readout_offset_s": readout_offset_s,
            "exposure_start_s": readout_offset_s,
            "exposure_end_s": readout_offset_s + exposure_duration_s,
            "sample_times_s": sample_times_s,
        }

    def _camera_readout_index(
        self,
        *,
        coord: float,
        size: int,
        direction: str,
    ) -> int:
        if size <= 0:
            return 0
        index = int(round(coord))
        if index < 0:
            index = 0
        if index >= size:
            index = size - 1
        normalized = direction.upper().strip()
        if normalized in {"BOTTOM_TO_TOP", "RIGHT_TO_LEFT"}:
            return (size - 1) - index
        return index

    def _read_camera_rolling_shutter_poses(
        self,
        *,
        request: SensorSimRequest,
        trajectory_path: Path | None,
    ) -> list[TrajectoryPose]:
        if trajectory_path is None or not trajectory_path.exists():
            return []
        return read_trajectory_poses(
            trajectory_path,
            max_rows=int(request.options.get("camera_extrinsics_auto_max_rows", 20000)),
        )

    def _select_camera_rolling_shutter_base_pose(
        self,
        *,
        request: SensorSimRequest,
        poses: list[TrajectoryPose],
    ) -> TrajectoryPose | None:
        if not poses:
            return None
        if bool(request.options.get("camera_extrinsics_auto_from_trajectory", False)):
            return self._select_trajectory_pose(
                poses=poses,
                selector=str(request.options.get("camera_extrinsics_auto_pose", "first")),
            )
        return poses[0]

    def _apply_camera_pose_delta_to_extrinsics(
        self,
        *,
        base_extrinsics: CameraExtrinsics,
        base_pose: TrajectoryPose,
        sample_pose: TrajectoryPose,
    ) -> CameraExtrinsics:
        dx = sample_pose.x - base_pose.x
        dy = sample_pose.y - base_pose.y
        dz = sample_pose.z - base_pose.z
        droll = sample_pose.roll_deg - base_pose.roll_deg
        dpitch = sample_pose.pitch_deg - base_pose.pitch_deg
        dyaw = sample_pose.yaw_deg - base_pose.yaw_deg
        return CameraExtrinsics(
            tx=base_extrinsics.tx + dx,
            ty=base_extrinsics.ty + dy,
            tz=base_extrinsics.tz + dz,
            roll_deg=base_extrinsics.roll_deg + droll,
            pitch_deg=base_extrinsics.pitch_deg + dpitch,
            yaw_deg=base_extrinsics.yaw_deg + dyaw,
            enabled=base_extrinsics.enabled
            or abs(dx) > 1e-9
            or abs(dy) > 1e-9
            or abs(dz) > 1e-9
            or abs(droll) > 1e-9
            or abs(dpitch) > 1e-9
            or abs(dyaw) > 1e-9,
        )

    def _interpolate_trajectory_pose_at_time(
        self,
        *,
        poses: list[TrajectoryPose],
        target_time_s: float,
    ) -> TrajectoryPose:
        if not poses:
            raise ValueError("poses must not be empty")
        if len(poses) == 1 or target_time_s <= poses[0].time_s:
            return poses[0]
        if target_time_s >= poses[-1].time_s:
            return poses[-1]

        for index in range(1, len(poses)):
            right = poses[index]
            if target_time_s > right.time_s:
                continue
            left = poses[index - 1]
            dt = right.time_s - left.time_s
            if dt <= 1e-9:
                return right
            alpha = (target_time_s - left.time_s) / dt
            return TrajectoryPose(
                x=left.x + (right.x - left.x) * alpha,
                y=left.y + (right.y - left.y) * alpha,
                z=left.z + (right.z - left.z) * alpha,
                time_s=target_time_s,
                roll_deg=left.roll_deg + (right.roll_deg - left.roll_deg) * alpha,
                pitch_deg=left.pitch_deg + (right.pitch_deg - left.pitch_deg) * alpha,
                yaw_deg=left.yaw_deg + (right.yaw_deg - left.yaw_deg) * alpha,
            )
        return poses[-1]

    def _generate_lidar_noisy_pointcloud_if_available(
        self,
        request: SensorSimRequest,
        artifacts: dict[str, Path],
        enhanced_output: Path,
        metrics: dict[str, float],
    ) -> tuple[Path | None, Path | None]:
        if not bool(request.options.get("lidar_postprocess_enabled", True)):
            return None, None

        point_cloud = artifacts.get("point_cloud_primary")
        if point_cloud is None or point_cloud.suffix.lower() != ".xyz" or not point_cloud.exists():
            return None, None

        max_points = int(request.options.get("lidar_postprocess_max_points", 50000))
        points_xyz = read_xyz_points(point_cloud, max_points=max_points)
        if not points_xyz:
            metrics["lidar_input_count"] = 0.0
            metrics["lidar_output_count"] = 0.0
            return None, None

        lidar_config = self._sensor_config_from_request(request).lidar
        base_extrinsics = self._lidar_extrinsics_from_options(request)
        behavior_actor_positions = self._sensor_behavior_actor_position_map(
            request=request,
            points_xyz=points_xyz,
        )
        effective_extrinsics, behavior_runtime = self._apply_sensor_behaviors_to_extrinsics(
            request=request,
            sensor_name="lidar",
            base_extrinsics=base_extrinsics,
            behaviors=list(lidar_config.behaviors),
            actor_positions=behavior_actor_positions,
            points_xyz=points_xyz,
            eval_time_s=float(request.options.get("lidar_behavior_time_s", request.options.get("sensor_behavior_time_s", 0.0))),
        )
        points_lidar = transform_points_world_to_camera(
            points_xyz=points_xyz,
            extrinsics=effective_extrinsics,
        )
        scan_points, scan_meta = self._apply_lidar_scan_model(
            request=request,
            points_xyz=points_lidar,
            lidar_config=lidar_config,
            frame_id=0,
            frame_count=1,
        )
        rng = random.Random(int(request.seed) + 17)
        noisy_points, preview_scan_points = self._apply_lidar_noise_and_dropout_with_metadata(
            request=request,
            lidar_config=lidar_config,
            points_xyz=scan_points,
            metadata_points=scan_meta["points"] if isinstance(scan_meta.get("points"), list) else [],
            rng=rng,
        )

        output_path = enhanced_output / "lidar_noisy_preview.xyz"
        write_xyz_points(output_path, noisy_points)
        metadata_path = enhanced_output / "lidar_noisy_preview.json"
        preview_points = self._lidar_preview_points_payload(
            noisy_points=noisy_points,
            metadata_points=preview_scan_points,
        )
        coverage_summary = self._build_coverage_summary_from_samples(
            samples=preview_points,
            count_field="lidar_points_on_target",
            actor_field="ground_truth_actor_id",
            semantic_class_field="ground_truth_semantic_class",
            semantic_name_field="ground_truth_semantic_class_name",
            component_field="ground_truth_component_id",
            material_class_field="ground_truth_material_class",
            base_map_field="ground_truth_base_map_element",
            procedural_map_field="ground_truth_procedural_map_element",
            lane_marking_field="ground_truth_lane_marking_id",
            detection_type_field="ground_truth_detection_type",
            count_threshold=max(
                self._sensor_config_from_request(request).coverage.lidar_min_points_on_target,
                1,
            ),
            excluded_detection_types={"NOISE"},
        )
        metadata_path.write_text(
            json.dumps(
                {
                    "input_point_cloud": str(point_cloud),
                    "input_count": len(points_xyz),
                    "output_count": len(noisy_points),
                    "scan_type": lidar_config.scan_type,
                    "scan_model_applied": scan_meta["applied"],
                    "scan_frequency_hz": lidar_config.scan_frequency_hz,
                    "spin_direction": lidar_config.spin_direction,
                    "source_angles_deg": list(lidar_config.source_angles_deg),
                    "source_angle_tolerance_deg": lidar_config.source_angle_tolerance_deg,
                    "scan_field_deg": scan_meta["scan_field_deg"],
                    "scan_field_offset_deg": scan_meta["scan_field_offset_deg"],
                    "scan_path_deg": list(scan_meta["scan_path_deg"]),
                    "multi_scan_path_deg": [list(path) for path in lidar_config.multi_scan_path_deg],
                    "intensity": lidar_config.intensity.to_dict(),
                    "physics_model": lidar_config.physics_model.to_dict(),
                    "return_model": lidar_config.return_model.to_dict(),
                    "environment_model": lidar_config.environment_model.to_dict(),
                    "noise_performance": lidar_config.noise_performance.to_dict(),
                    "emitter_params": lidar_config.emitter_params.to_dict(),
                    "channel_profile": lidar_config.channel_profile.to_dict(),
                    "multipath_model": lidar_config.multipath_model.to_dict(),
                    "lidar_extrinsics": {
                        "enabled": effective_extrinsics.enabled,
                        "tx": effective_extrinsics.tx,
                        "ty": effective_extrinsics.ty,
                        "tz": effective_extrinsics.tz,
                        "roll_deg": effective_extrinsics.roll_deg,
                        "pitch_deg": effective_extrinsics.pitch_deg,
                        "yaw_deg": effective_extrinsics.yaw_deg,
                    },
                    "lidar_behavior": behavior_runtime,
                    "ground_truth_fields": self._lidar_ground_truth_fields(),
                    "coverage_metric_name": "lidar_points_on_target",
                    "coverage_total_observation_count": coverage_summary[
                        "total_observation_count"
                    ],
                    "coverage_anonymous_observation_count": coverage_summary[
                        "anonymous_observation_count"
                    ],
                    "coverage_excluded_observation_count": coverage_summary[
                        "excluded_observation_count"
                    ],
                    "coverage_targets": coverage_summary["targets"],
                    "preview_points": preview_points,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        noise_model = str(request.options.get("lidar_noise", "gaussian")).lower().strip()
        noise_stddev = float(request.options.get("lidar_noise_stddev_m", 0.02))
        metrics["lidar_input_count"] = float(len(points_xyz))
        metrics["lidar_output_count"] = float(len(noisy_points))
        metrics["lidar_dropout_ratio"] = (
            1.0 - (float(len(noisy_points)) / float(len(scan_points)))
            if scan_points
            else 0.0
        )
        metrics["lidar_noise_stddev_m"] = float(noise_stddev if noise_model == "gaussian" else 0.0)
        metrics["lidar_scan_model_applied"] = 1.0 if scan_meta["applied"] else 0.0
        metrics["lidar_source_angle_count"] = float(len(lidar_config.source_angles_deg))
        metrics["lidar_scan_path_point_count"] = float(len(scan_meta["scan_path_deg"]))
        metrics["lidar_behavior_applied"] = 1.0 if bool(behavior_runtime.get("applied")) else 0.0
        self._update_lidar_signal_metrics(metrics=metrics, preview_points=preview_points, lidar_config=lidar_config)
        return output_path, metadata_path

    def _generate_lidar_trajectory_sweep_if_available(
        self,
        request: SensorSimRequest,
        artifacts: dict[str, Path],
        enhanced_output: Path,
        metrics: dict[str, float],
    ) -> Path | None:
        if not bool(request.options.get("lidar_postprocess_enabled", True)):
            return None
        if not bool(request.options.get("lidar_trajectory_sweep_enabled", False)):
            return None

        point_cloud = artifacts.get("point_cloud_primary")
        trajectory_path = artifacts.get("trajectory_primary")
        if (
            point_cloud is None
            or point_cloud.suffix.lower() != ".xyz"
            or not point_cloud.exists()
            or trajectory_path is None
            or not trajectory_path.exists()
        ):
            return None

        max_points = int(request.options.get("lidar_postprocess_max_points", 50000))
        points_xyz = read_xyz_points(point_cloud, max_points=max_points)
        if not points_xyz:
            metrics["lidar_trajectory_sweep_frame_count"] = 0.0
            metrics["lidar_trajectory_sweep_total_output_count"] = 0.0
            return None

        poses = read_trajectory_poses(
            trajectory_path,
            max_rows=int(request.options.get("camera_extrinsics_auto_max_rows", 20000)),
        )
        if not poses:
            metrics["lidar_trajectory_sweep_frame_count"] = 0.0
            metrics["lidar_trajectory_sweep_total_output_count"] = 0.0
            return None

        frame_count = int(request.options.get("lidar_trajectory_sweep_frames", 3))
        selected_poses = self._sample_trajectory_poses(poses=poses, frame_count=frame_count)
        preview_points_per_frame = int(request.options.get("lidar_preview_points_per_frame", 64))
        motion_comp_enabled = bool(request.options.get("lidar_motion_compensation_enabled", True))
        motion_comp_mode = str(request.options.get("lidar_motion_compensation_mode", "linear"))
        scan_duration_s = float(request.options.get("lidar_scan_duration_s", 0.1))
        base_extrinsics = self._lidar_extrinsics_from_options(request)
        lidar_config = self._sensor_config_from_request(request).lidar
        behavior_actor_positions = self._sensor_behavior_actor_position_map(
            request=request,
            points_xyz=points_xyz,
        )
        behavior_time_origin_s = float(selected_poses[0][1].time_s) if selected_poses else 0.0
        lidar_coverage_threshold = max(
            self._sensor_config_from_request(request).coverage.lidar_min_points_on_target,
            1,
        )

        frames: list[dict[str, object]] = []
        total_output_count = 0
        scan_applied = False
        coverage_target_lists: list[list[dict[str, object]]] = []
        coverage_total_observation_count = 0
        coverage_anonymous_observation_count = 0
        coverage_excluded_observation_count = 0
        for frame_id, (pose_index, pose) in enumerate(selected_poses):
            effective_extrinsics = self._build_lidar_extrinsics_from_pose(
                request=request,
                base_extrinsics=base_extrinsics,
                pose=pose,
                force_enable=True,
            )
            effective_extrinsics, behavior_runtime = self._apply_sensor_behaviors_to_extrinsics(
                request=request,
                sensor_name="lidar",
                base_extrinsics=effective_extrinsics,
                behaviors=list(lidar_config.behaviors),
                actor_positions=behavior_actor_positions,
                points_xyz=points_xyz,
                eval_time_s=float(pose.time_s - behavior_time_origin_s),
            )
            ego_velocity = self._estimate_ego_velocity_for_pose_index(poses=poses, pose_index=pose_index)
            compensated_world_points = points_xyz
            if motion_comp_enabled:
                compensated_world_points = self._apply_lidar_motion_compensation(
                    points_xyz=points_xyz,
                    ego_velocity=ego_velocity,
                    scan_duration_s=scan_duration_s,
                    mode=motion_comp_mode,
                )

            points_lidar = transform_points_world_to_camera(
                points_xyz=compensated_world_points,
                extrinsics=effective_extrinsics,
            )
            scan_points, scan_meta = self._apply_lidar_scan_model(
                request=request,
                points_xyz=points_lidar,
                lidar_config=lidar_config,
                frame_id=frame_id,
                frame_count=len(selected_poses),
            )
            scan_applied = scan_applied or bool(scan_meta["applied"])
            rng = random.Random(int(request.seed) + 937 + frame_id)
            noisy_points, frame_scan_points = self._apply_lidar_noise_and_dropout_with_metadata(
                request=request,
                lidar_config=lidar_config,
                points_xyz=scan_points,
                metadata_points=scan_meta["points"] if isinstance(scan_meta.get("points"), list) else [],
                rng=rng,
            )
            total_output_count += len(noisy_points)
            preview_points = self._lidar_preview_points_payload(
                noisy_points=noisy_points,
                metadata_points=frame_scan_points,
            )
            frame_coverage_summary = self._build_coverage_summary_from_samples(
                samples=preview_points,
                count_field="lidar_points_on_target",
                actor_field="ground_truth_actor_id",
                semantic_class_field="ground_truth_semantic_class",
                semantic_name_field="ground_truth_semantic_class_name",
                component_field="ground_truth_component_id",
                material_class_field="ground_truth_material_class",
                base_map_field="ground_truth_base_map_element",
                procedural_map_field="ground_truth_procedural_map_element",
                lane_marking_field="ground_truth_lane_marking_id",
                detection_type_field="ground_truth_detection_type",
                count_threshold=lidar_coverage_threshold,
                excluded_detection_types={"NOISE"},
            )
            coverage_target_lists.append(list(frame_coverage_summary["targets"]))
            coverage_total_observation_count += int(
                frame_coverage_summary["total_observation_count"]
            )
            coverage_anonymous_observation_count += int(
                frame_coverage_summary["anonymous_observation_count"]
            )
            coverage_excluded_observation_count += int(
                frame_coverage_summary["excluded_observation_count"]
            )
            frames.append(
                {
                    "frame_id": frame_id,
                    "pose_index": pose_index,
                    "trajectory_pose": self._trajectory_pose_payload(
                        pose=pose,
                        trajectory_path=trajectory_path,
                    ),
                    "ego_velocity_mps": {
                        "vx": ego_velocity[0],
                        "vy": ego_velocity[1],
                        "vz": ego_velocity[2],
                    },
                    "lidar_extrinsics": {
                        "enabled": effective_extrinsics.enabled,
                        "tx": effective_extrinsics.tx,
                        "ty": effective_extrinsics.ty,
                        "tz": effective_extrinsics.tz,
                        "roll_deg": effective_extrinsics.roll_deg,
                        "pitch_deg": effective_extrinsics.pitch_deg,
                        "yaw_deg": effective_extrinsics.yaw_deg,
                    },
                    "motion_compensation_enabled": motion_comp_enabled,
                    "lidar_behavior": behavior_runtime,
                    "output_count": len(noisy_points),
                    "preview_points_xyz": [
                        {"x": x, "y": y, "z": z}
                        for x, y, z in noisy_points[:preview_points_per_frame]
                    ],
                    "preview_points": preview_points[:preview_points_per_frame],
                    "ground_truth_fields": self._lidar_ground_truth_fields(),
                    "coverage_metric_name": "lidar_points_on_target",
                    "coverage_total_observation_count": frame_coverage_summary[
                        "total_observation_count"
                    ],
                    "coverage_anonymous_observation_count": frame_coverage_summary[
                        "anonymous_observation_count"
                    ],
                    "coverage_excluded_observation_count": frame_coverage_summary[
                        "excluded_observation_count"
                    ],
                    "coverage_targets": frame_coverage_summary["targets"],
                    "scan_model_applied": scan_meta["applied"],
                    "scan_path_deg": list(scan_meta["scan_path_deg"]),
                }
            )

        metrics["lidar_trajectory_sweep_frame_count"] = float(len(frames))
        metrics["lidar_trajectory_sweep_total_output_count"] = float(total_output_count)
        metrics["lidar_motion_compensation_applied"] = 1.0 if motion_comp_enabled else 0.0
        metrics["lidar_scan_model_applied"] = 1.0 if scan_applied else 0.0
        metrics["lidar_source_angle_count"] = float(len(lidar_config.source_angles_deg))
        metrics["lidar_scan_path_point_count"] = float(
            len(lidar_config.scan_path_deg)
            + sum(len(path) for path in lidar_config.multi_scan_path_deg)
        )
        metrics["lidar_behavior_applied"] = 1.0 if any(
            bool(frame.get("lidar_behavior", {}).get("applied"))
            for frame in frames
        ) else 0.0
        payload = {
            "input_point_cloud": str(point_cloud),
            "trajectory_path": str(trajectory_path),
            "input_count": len(points_xyz),
            "frame_count": len(frames),
            "scan_type": lidar_config.scan_type,
            "scan_frequency_hz": lidar_config.scan_frequency_hz,
            "spin_direction": lidar_config.spin_direction,
            "source_angles_deg": list(lidar_config.source_angles_deg),
            "source_angle_tolerance_deg": lidar_config.source_angle_tolerance_deg,
            "scan_field_deg": {
                "azimuth_min": lidar_config.scan_field_azimuth_min_deg,
                "azimuth_max": lidar_config.scan_field_azimuth_max_deg,
                "elevation_min": lidar_config.scan_field_elevation_min_deg,
                "elevation_max": lidar_config.scan_field_elevation_max_deg,
            },
            "scan_field_offset_deg": {
                "azimuth": lidar_config.scan_field_azimuth_offset_deg,
                "elevation": lidar_config.scan_field_elevation_offset_deg,
            },
            "scan_path_deg": list(lidar_config.scan_path_deg),
            "multi_scan_path_deg": [list(path) for path in lidar_config.multi_scan_path_deg],
            "intensity": lidar_config.intensity.to_dict(),
            "physics_model": lidar_config.physics_model.to_dict(),
            "return_model": lidar_config.return_model.to_dict(),
            "environment_model": lidar_config.environment_model.to_dict(),
            "noise_performance": lidar_config.noise_performance.to_dict(),
            "emitter_params": lidar_config.emitter_params.to_dict(),
            "channel_profile": lidar_config.channel_profile.to_dict(),
            "multipath_model": lidar_config.multipath_model.to_dict(),
            "ground_truth_fields": self._lidar_ground_truth_fields(),
            "coverage_metric_name": "lidar_points_on_target",
            "coverage_total_observation_count": coverage_total_observation_count,
            "coverage_anonymous_observation_count": coverage_anonymous_observation_count,
            "coverage_excluded_observation_count": coverage_excluded_observation_count,
            "coverage_targets": self._merge_sensor_coverage_targets(
                target_lists=coverage_target_lists,
                count_field="lidar_points_on_target",
                count_threshold=lidar_coverage_threshold,
            ),
            "frames": frames,
        }
        output_path = enhanced_output / "lidar_trajectory_sweep.json"
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        self._update_lidar_signal_metrics(
            metrics=metrics,
            preview_points=[
                point
                for frame in frames
                for point in frame.get("preview_points", [])
                if isinstance(point, dict)
            ],
            lidar_config=lidar_config,
        )
        return output_path

    def _apply_lidar_noise_and_dropout(
        self,
        request: SensorSimRequest,
        points_xyz: list[tuple[float, float, float]],
        rng: random.Random,
    ) -> list[tuple[float, float, float]]:
        noise_model = str(request.options.get("lidar_noise", "gaussian")).lower().strip()
        noise_stddev = float(request.options.get("lidar_noise_stddev_m", 0.02))
        dropout_prob = float(request.options.get("lidar_dropout_probability", 0.01))
        dropout_prob = min(max(dropout_prob, 0.0), 1.0)

        noisy_points: list[tuple[float, float, float]] = []
        for x, y, z in points_xyz:
            if dropout_prob > 0.0 and rng.random() < dropout_prob:
                continue
            if noise_model == "gaussian":
                noisy_points.append(
                    (
                        x + rng.gauss(0.0, noise_stddev),
                        y + rng.gauss(0.0, noise_stddev),
                        z + rng.gauss(0.0, noise_stddev),
                    )
                )
            else:
                noisy_points.append((x, y, z))
        return noisy_points

    def _apply_lidar_noise_and_dropout_with_metadata(
        self,
        *,
        request: SensorSimRequest,
        lidar_config: Any,
        points_xyz: list[tuple[float, float, float]],
        metadata_points: list[dict[str, object]],
        rng: random.Random,
    ) -> tuple[list[tuple[float, float, float]], list[dict[str, object]]]:
        noise_model = str(request.options.get("lidar_noise", "gaussian")).lower().strip()
        noise_stddev = float(request.options.get("lidar_noise_stddev_m", 0.02))
        dropout_prob = float(request.options.get("lidar_dropout_probability", 0.01))
        dropout_prob = min(max(dropout_prob, 0.0), 1.0)

        noisy_points: list[tuple[float, float, float]] = []
        noisy_metadata: list[dict[str, object]] = []
        for index, (x, y, z) in enumerate(points_xyz):
            base_metadata = (
                dict(metadata_points[index])
                if index < len(metadata_points) and isinstance(metadata_points[index], dict)
                else {}
            )
            return_series = self._lidar_signal_return_series(
                request=request,
                lidar_config=lidar_config,
                point_xyz=(x, y, z),
                base_metadata=base_metadata,
            )
            emitter_adjustment = self._lidar_emitter_adjustment(
                lidar_config=lidar_config,
                base_metadata=base_metadata,
                rng=rng,
            )
            for return_point, signal_metadata in return_series:
                if not bool(signal_metadata.get("detected", True)):
                    continue
                if dropout_prob > 0.0 and rng.random() < dropout_prob:
                    continue
                adjusted_return_point = self._lidar_apply_emitter_adjustment_to_point(
                    point_xyz=return_point,
                    az_offset_rad=emitter_adjustment["az_offset_rad"],
                    el_offset_rad=emitter_adjustment["el_offset_rad"],
                )
                rx, ry, rz = adjusted_return_point
                if noise_model == "gaussian":
                    noisy_point = (
                        rx + rng.gauss(0.0, noise_stddev),
                        ry + rng.gauss(0.0, noise_stddev),
                        rz + rng.gauss(0.0, noise_stddev),
                    )
                else:
                    noisy_point = (rx, ry, rz)
                noisy_points.append(noisy_point)
                payload = dict(base_metadata)
                payload.update(signal_metadata)
                payload.update(emitter_adjustment["metadata"])
                payload.pop("detected", None)
                noisy_metadata.append(payload)
        false_alarm_points, false_alarm_metadata = self._generate_lidar_false_alarm_points(
            request=request,
            lidar_config=lidar_config,
            source_point_count=len(points_xyz),
            rng=rng,
        )
        for index, false_point in enumerate(false_alarm_points):
            fx, fy, fz = false_point
            if noise_model == "gaussian":
                noisy_points.append(
                    (
                        fx + rng.gauss(0.0, noise_stddev),
                        fy + rng.gauss(0.0, noise_stddev),
                        fz + rng.gauss(0.0, noise_stddev),
                    )
                )
            else:
                noisy_points.append(false_point)
            noisy_metadata.append(false_alarm_metadata[index])
        return noisy_points, noisy_metadata

    def _apply_lidar_scan_model(
        self,
        *,
        request: SensorSimRequest,
        points_xyz: list[tuple[float, float, float]],
        lidar_config: Any,
        frame_id: int,
        frame_count: int,
    ) -> tuple[list[tuple[float, float, float]], dict[str, object]]:
        if not points_xyz:
            return [], {
                "applied": False,
                "scan_path_deg": [],
                "points": [],
                "scan_field_deg": self._lidar_scan_field_payload(lidar_config),
                "scan_field_offset_deg": self._lidar_scan_field_offset_payload(lidar_config),
            }
        if not self._lidar_scan_model_requested(lidar_config):
            return list(points_xyz), {
                "applied": False,
                "scan_path_deg": [],
                "points": [
                    self._lidar_point_metadata(
                        x=x,
                        y=y,
                        z=z,
                        point_index=index,
                        channel_id=None,
                        source_angle_deg=None,
                        scan_path_index=None,
                    )
                    for index, (x, y, z) in enumerate(points_xyz)
                ],
                "scan_field_deg": self._lidar_scan_field_payload(lidar_config),
                "scan_field_offset_deg": self._lidar_scan_field_offset_payload(lidar_config),
            }

        selected_points: list[tuple[float, float, float]] = []
        metadata: list[dict[str, object]] = []
        scan_path = self._lidar_resolved_scan_path_deg(
            lidar_config=lidar_config,
            frame_id=frame_id,
            frame_count=frame_count,
        )
        source_angles = list(lidar_config.source_angles_deg)
        tolerance = max(lidar_config.source_angle_tolerance_deg, 1e-6)
        az_min = lidar_config.scan_field_azimuth_min_deg
        az_max = lidar_config.scan_field_azimuth_max_deg
        el_min = lidar_config.scan_field_elevation_min_deg
        el_max = lidar_config.scan_field_elevation_max_deg
        az_offset = lidar_config.scan_field_azimuth_offset_deg
        el_offset = lidar_config.scan_field_elevation_offset_deg

        for point_index, (x, y, z) in enumerate(points_xyz):
            range_m = sqrt(x * x + y * y + z * z)
            if range_m < lidar_config.range_min_m or range_m > lidar_config.range_max_m:
                continue
            horizontal = sqrt(x * x + y * y)
            azimuth_deg = self._normalize_angle_deg((atan2(y, x) * 180.0 / pi) - az_offset)
            elevation_deg = (atan2(z, max(horizontal, 1e-9)) * 180.0 / pi) - el_offset
            if not self._lidar_angle_in_field(
                angle_deg=azimuth_deg,
                min_deg=az_min,
                max_deg=az_max,
                wrap=True,
            ):
                continue
            if elevation_deg < el_min or elevation_deg > el_max:
                continue

            channel_id: int | None = None
            source_angle_deg: float | None = None
            if source_angles:
                channel_id, source_angle_deg = self._lidar_match_source_angle(
                    elevation_deg=elevation_deg,
                    source_angles_deg=source_angles,
                    tolerance_deg=tolerance,
                )
                if channel_id is None:
                    continue

            scan_path_index: int | None = None
            if scan_path:
                scan_path_index = self._lidar_match_scan_path(
                    azimuth_deg=azimuth_deg,
                    scan_path_deg=scan_path,
                )
                if scan_path_index is None:
                    continue

            selected_points.append((x, y, z))
            metadata.append(
                self._lidar_point_metadata(
                    x=x,
                    y=y,
                    z=z,
                    point_index=point_index,
                    channel_id=channel_id,
                    source_angle_deg=source_angle_deg,
                    scan_path_index=scan_path_index,
                )
            )

        return selected_points, {
            "applied": True,
            "scan_path_deg": scan_path,
            "points": metadata,
            "scan_field_deg": self._lidar_scan_field_payload(lidar_config),
            "scan_field_offset_deg": self._lidar_scan_field_offset_payload(lidar_config),
        }

    def _lidar_scan_model_requested(self, lidar_config: Any) -> bool:
        return bool(
            lidar_config.source_angles_deg
            or lidar_config.scan_path_deg
            or lidar_config.multi_scan_path_deg
            or lidar_config.scan_type.upper() in {"FLASH", "CUSTOM"}
            or lidar_config.spin_direction.upper() != "CCW"
            or abs(lidar_config.scan_field_azimuth_min_deg + 180.0) > 1e-9
            or abs(lidar_config.scan_field_azimuth_max_deg - 180.0) > 1e-9
            or abs(lidar_config.scan_field_elevation_min_deg + 30.0) > 1e-9
            or abs(lidar_config.scan_field_elevation_max_deg - 30.0) > 1e-9
            or abs(lidar_config.scan_field_azimuth_offset_deg) > 1e-9
            or abs(lidar_config.scan_field_elevation_offset_deg) > 1e-9
        )

    def _lidar_resolved_scan_path_deg(
        self,
        *,
        lidar_config: Any,
        frame_id: int,
        frame_count: int,
    ) -> list[float]:
        if lidar_config.multi_scan_path_deg:
            return list(lidar_config.multi_scan_path_deg[frame_id % len(lidar_config.multi_scan_path_deg)])
        if lidar_config.scan_path_deg:
            return list(lidar_config.scan_path_deg)
        if lidar_config.scan_type.upper() == "SPIN" and frame_count > 1:
            az_min = lidar_config.scan_field_azimuth_min_deg
            az_max = lidar_config.scan_field_azimuth_max_deg
            span = max(az_max - az_min, 1.0)
            step = span / float(frame_count)
            if lidar_config.spin_direction.upper() == "CW":
                return [az_max - step * (frame_id + 0.5)]
            return [az_min + step * (frame_id + 0.5)]
        return []

    def _lidar_match_source_angle(
        self,
        *,
        elevation_deg: float,
        source_angles_deg: list[float],
        tolerance_deg: float,
    ) -> tuple[int | None, float | None]:
        if not source_angles_deg:
            return None, None
        nearest_index = min(
            range(len(source_angles_deg)),
            key=lambda index: abs(source_angles_deg[index] - elevation_deg),
        )
        nearest_angle = float(source_angles_deg[nearest_index])
        if abs(nearest_angle - elevation_deg) > tolerance_deg:
            return None, None
        return nearest_index, nearest_angle

    def _lidar_match_scan_path(
        self,
        *,
        azimuth_deg: float,
        scan_path_deg: list[float],
    ) -> int | None:
        if not scan_path_deg:
            return None
        tolerance_deg = 8.0 if len(scan_path_deg) > 1 else 15.0
        nearest_index = min(
            range(len(scan_path_deg)),
            key=lambda index: abs(self._normalize_angle_deg(azimuth_deg - scan_path_deg[index])),
        )
        nearest_angle = self._normalize_angle_deg(azimuth_deg - scan_path_deg[nearest_index])
        if abs(nearest_angle) > tolerance_deg:
            return None
        return nearest_index

    def _lidar_point_metadata(
        self,
        *,
        x: float,
        y: float,
        z: float,
        point_index: int,
        channel_id: int | None,
        source_angle_deg: float | None,
        scan_path_index: int | None,
    ) -> dict[str, object]:
        horizontal = sqrt(x * x + y * y)
        return {
            "x": x,
            "y": y,
            "z": z,
            "point_index": point_index,
            "range_m": sqrt(x * x + y * y + z * z),
            "azimuth_deg": atan2(y, x) * 180.0 / pi,
            "elevation_deg": atan2(z, max(horizontal, 1e-9)) * 180.0 / pi,
            "channel_id": channel_id,
            "source_angle_deg": source_angle_deg,
            "scan_path_index": scan_path_index,
        }

    def _lidar_preview_points_payload(
        self,
        *,
        noisy_points: list[tuple[float, float, float]],
        metadata_points: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        payload: list[dict[str, object]] = []
        for index, point in enumerate(noisy_points):
            x, y, z = point
            base = metadata_points[index] if index < len(metadata_points) and isinstance(metadata_points[index], dict) else {}
            payload.append(
                {
                    "x": x,
                    "y": y,
                    "z": z,
                    "point_index": base.get("point_index"),
                    "range_m": sqrt(x * x + y * y + z * z),
                    "azimuth_deg": base.get("azimuth_deg", atan2(y, x) * 180.0 / pi),
                    "elevation_deg": base.get(
                        "elevation_deg",
                        atan2(z, max(sqrt(x * x + y * y), 1e-9)) * 180.0 / pi,
                    ),
                    "channel_id": base.get("channel_id"),
                    "source_angle_deg": base.get("source_angle_deg"),
                    "scan_path_index": base.get("scan_path_index"),
                    "intensity": base.get("intensity"),
                    "intensity_units": base.get("intensity_units"),
                    "reflectivity": base.get("reflectivity"),
                    "ground_truth_reflectivity": base.get("ground_truth_reflectivity"),
                    "laser_cross_section": base.get("laser_cross_section"),
                    "signal_power_dbw": base.get("signal_power_dbw"),
                    "ambient_power_dbw": base.get("ambient_power_dbw"),
                    "signal_photons": base.get("signal_photons"),
                    "ambient_photons": base.get("ambient_photons"),
                    "snr": base.get("snr"),
                    "snr_db": base.get("snr_db"),
                    "return_id": base.get("return_id"),
                    "path_length_offset_m": base.get("path_length_offset_m"),
                    "ground_truth_hit_index": base.get("ground_truth_hit_index"),
                    "ground_truth_last_bounce_index": base.get("ground_truth_last_bounce_index"),
                    "weather_extinction_factor": base.get("weather_extinction_factor"),
                    "precipitation_type": base.get("precipitation_type"),
                    "particle_field_density": base.get("particle_field_density"),
                    "particle_diameter_mm": base.get("particle_diameter_mm"),
                    "particle_terminal_velocity_mps": base.get("particle_terminal_velocity_mps"),
                    "particle_reflectivity": base.get("particle_reflectivity"),
                    "particle_backscatter_strength": base.get("particle_backscatter_strength"),
                    "precipitation_extinction_alpha": base.get("precipitation_extinction_alpha"),
                    "channel_loss_db": base.get("channel_loss_db"),
                    "optical_loss_db": base.get("optical_loss_db"),
                    "peak_power_w": base.get("peak_power_w"),
                    "beam_divergence_az_rad": base.get("beam_divergence_az_rad"),
                    "beam_divergence_el_rad": base.get("beam_divergence_el_rad"),
                    "beam_footprint_area_m2": base.get("beam_footprint_area_m2"),
                    "beam_azimuth_offset_deg": base.get("beam_azimuth_offset_deg"),
                    "beam_elevation_offset_deg": base.get("beam_elevation_offset_deg"),
                    "multipath_surface": base.get("multipath_surface"),
                    "multipath_path_length_m": base.get("multipath_path_length_m"),
                    "multipath_base_range_m": base.get("multipath_base_range_m"),
                    "multipath_surface_reflectivity": base.get("multipath_surface_reflectivity"),
                    "multipath_model_mode": base.get("multipath_model_mode"),
                    "multipath_reflection_point": base.get("multipath_reflection_point"),
                    "channel_profile_pattern": base.get("channel_profile_pattern"),
                    "channel_profile_file_uri": base.get("channel_profile_file_uri"),
                    "channel_profile_source": base.get("channel_profile_source"),
                    "channel_profile_resolved_path": base.get("channel_profile_resolved_path"),
                    "channel_profile_weight": base.get("channel_profile_weight"),
                    "channel_profile_scale": base.get("channel_profile_scale"),
                    "channel_profile_offset_az_deg": base.get("channel_profile_offset_az_deg"),
                    "channel_profile_offset_el_deg": base.get("channel_profile_offset_el_deg"),
                    "channel_profile_half_angle_deg": base.get("channel_profile_half_angle_deg"),
                    "merged_return_count": base.get("merged_return_count"),
                    "range_discrimination_m": base.get("range_discrimination_m"),
                    "ground_truth_detection_type": base.get("ground_truth_detection_type"),
                    "ground_truth_semantic_class": base.get("ground_truth_semantic_class"),
                    "ground_truth_semantic_class_name": base.get(
                        "ground_truth_semantic_class_name"
                    ),
                    "ground_truth_semantic_parent_class": base.get(
                        "ground_truth_semantic_parent_class"
                    ),
                    "ground_truth_actor_id": base.get("ground_truth_actor_id"),
                    "ground_truth_component_id": base.get("ground_truth_component_id"),
                    "ground_truth_material_class": base.get("ground_truth_material_class"),
                    "ground_truth_material_uuid": base.get("ground_truth_material_uuid"),
                    "ground_truth_base_map_element": base.get("ground_truth_base_map_element"),
                    "ground_truth_procedural_map_element": base.get(
                        "ground_truth_procedural_map_element"
                    ),
                    "ground_truth_lane_marking_id": base.get("ground_truth_lane_marking_id"),
                }
            )
        return payload

    def _lidar_signal_return_series(
        self,
        *,
        request: SensorSimRequest,
        lidar_config: Any,
        point_xyz: tuple[float, float, float],
        base_metadata: dict[str, object],
    ) -> list[tuple[tuple[float, float, float], dict[str, object]]]:
        primary_metadata = self._lidar_signal_metadata(
            request=request,
            lidar_config=lidar_config,
            point_xyz=point_xyz,
            base_metadata=base_metadata,
        )
        supplemental_returns: list[tuple[tuple[float, float, float], dict[str, object]]] = []
        supplemental_returns.extend(
            self._lidar_environment_backscatter_returns(
                request=request,
                lidar_config=lidar_config,
                point_xyz=point_xyz,
                primary_metadata=primary_metadata,
                base_metadata=base_metadata,
            )
        )
        supplemental_returns.extend(
            self._lidar_channel_profile_returns(
                request=request,
                lidar_config=lidar_config,
                point_xyz=point_xyz,
                primary_metadata=primary_metadata,
                base_metadata=base_metadata,
            )
        )
        supplemental_returns.extend(
            self._lidar_geometry_multipath_returns(
                request=request,
                lidar_config=lidar_config,
                point_xyz=point_xyz,
                primary_metadata=primary_metadata,
                base_metadata=base_metadata,
            )
        )
        max_returns = max(1, int(getattr(lidar_config.return_model, "max_returns", 1)))
        return_mode = str(getattr(lidar_config.return_model, "mode", "SINGLE")).upper()
        if max_returns <= 1 or return_mode == "SINGLE":
            return self._lidar_finalize_return_series(
                lidar_config=lidar_config,
                primary_point=point_xyz,
                primary_metadata=primary_metadata,
                supplemental_returns=supplemental_returns,
            )

        base_range_m = float(primary_metadata.get("range_m", base_metadata.get("range_m", 0.0)))
        if base_range_m <= 1e-9:
            return self._lidar_finalize_return_series(
                lidar_config=lidar_config,
                primary_point=point_xyz,
                primary_metadata=primary_metadata,
                supplemental_returns=supplemental_returns,
            )
        range_separation_m = max(float(lidar_config.return_model.range_separation_m), 0.0)
        signal_decay = min(max(float(lidar_config.return_model.signal_decay), 0.0), 1.0)
        primary_signal_power_linear = pow(
            10.0,
            float(primary_metadata.get("signal_power_dbw", -90.0)) / 10.0,
        )
        primary_signal_photons = float(primary_metadata.get("signal_photons", 0.0))
        ambient_power_dbw = float(primary_metadata.get("ambient_power_dbw", -30.0))
        ambient_photons = float(primary_metadata.get("ambient_photons", 0.0))
        reflectivity = float(primary_metadata.get("reflectivity", 0.0))
        ground_truth_reflectivity = float(primary_metadata.get("ground_truth_reflectivity", 0.0))
        laser_cross_section = float(primary_metadata.get("laser_cross_section", reflectivity))
        primary_threshold_db = float(lidar_config.physics_model.minimum_detection_snr_db)
        secondary_threshold_db = max(
            float(lidar_config.return_model.minimum_secondary_snr_db),
            primary_threshold_db,
        )

        for return_id in range(1, max_returns):
            if signal_decay <= 0.0:
                break
            signal_power_linear = primary_signal_power_linear * pow(signal_decay, float(return_id))
            signal_photons = primary_signal_photons * pow(signal_decay, float(return_id))
            snr = signal_photons / max(ambient_photons, 1e-9)
            snr_db = 10.0 * log10(max(snr, 1e-9))
            detected = bool(lidar_config.physics_model.return_all_hits) or (
                snr_db >= secondary_threshold_db
            )
            range_offset_m = range_separation_m * float(return_id)
            return_point = self._lidar_offset_point_along_ray(
                point_xyz=point_xyz,
                range_offset_m=range_offset_m,
            )
            intensity_value = self._lidar_intensity_value(
                intensity_config=lidar_config.intensity,
                signal_power_linear=signal_power_linear,
                reflectivity=reflectivity,
                ground_truth_reflectivity=ground_truth_reflectivity,
                laser_cross_section=laser_cross_section,
                snr=snr,
            )
            secondary_metadata = dict(primary_metadata)
            secondary_metadata.update(
                {
                    "detected": detected,
                    "range_m": base_range_m + range_offset_m,
                    "intensity": intensity_value,
                    "signal_power_dbw": 10.0 * log10(max(signal_power_linear, 1e-9)),
                    "signal_photons": signal_photons,
                    "snr": snr,
                    "snr_db": snr_db,
                    "return_id": return_id,
                    "path_length_offset_m": range_offset_m,
                    "ground_truth_hit_index": return_id,
                    "ground_truth_last_bounce_index": return_id * 2,
                    "ground_truth_detection_type": "RETROREFLECTION",
                }
            )
            supplemental_returns.append((return_point, secondary_metadata))
        return self._lidar_finalize_return_series(
            lidar_config=lidar_config,
            primary_point=point_xyz,
            primary_metadata=primary_metadata,
            supplemental_returns=supplemental_returns,
        )

    def _lidar_finalize_return_series(
        self,
        *,
        lidar_config: Any,
        primary_point: tuple[float, float, float],
        primary_metadata: dict[str, object],
        supplemental_returns: list[tuple[tuple[float, float, float], dict[str, object]]],
    ) -> list[tuple[tuple[float, float, float], dict[str, object]]]:
        candidates: list[tuple[tuple[float, float, float], dict[str, object]]] = [
            (primary_point, dict(primary_metadata))
        ]
        candidates.extend(supplemental_returns)
        merged_candidates = self._lidar_merge_range_discriminated_returns(
            candidates=candidates,
            lidar_config=lidar_config,
        )
        ordered_returns = self._lidar_order_return_candidates(
            candidates=merged_candidates,
            selection_mode=self._lidar_return_selection_mode(lidar_config),
        )[: self._lidar_requested_return_count(lidar_config)]
        return_series: list[tuple[tuple[float, float, float], dict[str, object]]] = []
        for return_id, (return_point, metadata) in enumerate(ordered_returns):
            payload = dict(metadata)
            payload["return_id"] = return_id
            return_series.append((return_point, payload))
        return return_series

    def _lidar_channel_profile_returns(
        self,
        *,
        request: SensorSimRequest,
        lidar_config: Any,
        point_xyz: tuple[float, float, float],
        primary_metadata: dict[str, object],
        base_metadata: dict[str, object],
    ) -> list[tuple[tuple[float, float, float], dict[str, object]]]:
        channel_profile = lidar_config.channel_profile
        if not bool(channel_profile.enabled):
            return []
        profile_data = channel_profile.profile_data
        scale = max(float(profile_data.scale), 0.0)
        if scale <= 1e-12:
            return []
        profile_spec = self._lidar_channel_profile_spec(
            request=request,
            lidar_config=lidar_config,
        )
        pattern = str(profile_spec.get("pattern", "NONE")).upper().strip()
        taps = profile_spec.get("taps", [])
        if not taps:
            return []
        returns: list[tuple[tuple[float, float, float], dict[str, object]]] = []
        for tap in taps:
            az_offset_rad = float(tap["az"]) * self._lidar_channel_profile_half_angle_rad(
                lidar_config=lidar_config,
                profile_data=profile_data,
            )
            el_offset_rad = float(tap["el"]) * self._lidar_channel_profile_half_angle_rad(
                lidar_config=lidar_config,
                profile_data=profile_data,
            )
            profile_point = self._lidar_apply_emitter_adjustment_to_point(
                point_xyz=point_xyz,
                az_offset_rad=az_offset_rad,
                el_offset_rad=el_offset_rad,
            )
            metadata = self._lidar_channel_profile_metadata(
                request=request,
                lidar_config=lidar_config,
                primary_metadata=primary_metadata,
                base_metadata=base_metadata,
                point_xyz=profile_point,
                pattern=pattern,
                profile_weight=float(tap["weight"]),
                az_offset_rad=az_offset_rad,
                el_offset_rad=el_offset_rad,
                source=str(profile_spec.get("source", "PATTERN")),
                resolved_path=profile_spec.get("resolved_path"),
            )
            returns.append((profile_point, metadata))
        return returns

    def _lidar_channel_profile_half_angle_rad(
        self,
        *,
        lidar_config: Any,
        profile_data: Any,
    ) -> float:
        configured = max(float(profile_data.half_angle_rad), 0.0)
        if configured > 0.0:
            return configured
        divergence_az = abs(float(lidar_config.emitter_params.source_divergence.az))
        divergence_el = abs(float(lidar_config.emitter_params.source_divergence.el))
        return max(divergence_az, divergence_el, 1e-3) * 4.0

    def _lidar_channel_profile_taps(self, *, profile_data: Any) -> list[dict[str, float]]:
        pattern = str(profile_data.pattern).upper().strip()
        if pattern == "CROSS":
            base_taps = [
                {"az": 1.0, "el": 0.0, "weight": 1.0},
                {"az": -1.0, "el": 0.0, "weight": 1.0},
                {"az": 0.0, "el": 1.0, "weight": 1.0},
                {"az": 0.0, "el": -1.0, "weight": 1.0},
            ]
        elif pattern == "GRID":
            base_taps = [
                {"az": -1.0, "el": -1.0, "weight": 0.5},
                {"az": -1.0, "el": 0.0, "weight": 0.75},
                {"az": -1.0, "el": 1.0, "weight": 0.5},
                {"az": 0.0, "el": -1.0, "weight": 0.75},
                {"az": 0.0, "el": 1.0, "weight": 0.75},
                {"az": 1.0, "el": -1.0, "weight": 0.5},
                {"az": 1.0, "el": 0.0, "weight": 0.75},
                {"az": 1.0, "el": 1.0, "weight": 0.5},
            ]
        elif pattern == "RING":
            base_taps = [
                {"az": cos(index * pi / 4.0), "el": sin(index * pi / 4.0), "weight": 0.65}
                for index in range(8)
            ]
        else:
            return []
        sample_count = int(getattr(profile_data, "sample_count", 0))
        if sample_count > 0:
            return base_taps[:sample_count]
        return base_taps

    def _lidar_channel_profile_spec(
        self,
        *,
        request: SensorSimRequest,
        lidar_config: Any,
    ) -> dict[str, object]:
        profile_data = lidar_config.channel_profile.profile_data
        file_uri = str(profile_data.file_uri).strip()
        cache_key = (
            str(request.scenario_path.resolve()),
            file_uri,
            str(profile_data.pattern).upper().strip(),
            float(profile_data.half_angle_rad),
            float(profile_data.scale),
            int(profile_data.sample_count),
            float(profile_data.sidelobe_gain),
        )
        cache = getattr(self, "_lidar_channel_profile_cache", {})
        if cache_key in cache:
            return cache[cache_key]

        pattern = str(profile_data.pattern).upper().strip()
        taps: list[dict[str, float]] = []
        source = "PATTERN"
        resolved_path: str | None = None
        if file_uri:
            file_spec = self._lidar_channel_profile_spec_from_file(
                request=request,
                lidar_config=lidar_config,
            )
            if file_spec is not None:
                taps = file_spec["taps"]
                resolved_path = file_spec.get("resolved_path")
                source = str(file_spec.get("source", "FILE"))
                pattern = "FILE"
        if not taps:
            taps = self._lidar_channel_profile_taps(profile_data=profile_data)
        spec = {
            "pattern": pattern or "NONE",
            "taps": taps,
            "source": source,
            "resolved_path": resolved_path,
        }
        cache[cache_key] = spec
        self._lidar_channel_profile_cache = cache
        return spec

    def _lidar_channel_profile_spec_from_file(
        self,
        *,
        request: SensorSimRequest,
        lidar_config: Any,
    ) -> dict[str, object] | None:
        profile_data = lidar_config.channel_profile.profile_data
        resolved_path = self._resolve_channel_profile_uri(
            request=request,
            file_uri=str(profile_data.file_uri),
        )
        if resolved_path is None:
            return None
        candidate_paths = self._channel_profile_candidate_paths(resolved_path)
        for candidate_path, source in candidate_paths:
            if not candidate_path.exists():
                continue
            taps = self._load_channel_profile_taps_from_path(
                path=candidate_path,
                sample_count=int(profile_data.sample_count),
            )
            if taps:
                return {
                    "taps": taps,
                    "resolved_path": str(candidate_path),
                    "source": source,
                }
        return None

    def _resolve_channel_profile_uri(
        self,
        *,
        request: SensorSimRequest,
        file_uri: str,
    ) -> Path | None:
        normalized = file_uri.strip()
        if not normalized:
            return None
        if normalized.startswith("scenario://workspace/"):
            suffix = normalized.removeprefix("scenario://workspace/")
            return request.scenario_path.parent / suffix
        if normalized.startswith("scenario://"):
            suffix = normalized.removeprefix("scenario://")
            return request.scenario_path.parent / suffix
        path = Path(normalized)
        if path.is_absolute():
            return path
        return request.scenario_path.parent / path

    def _channel_profile_candidate_paths(self, resolved_path: Path) -> list[tuple[Path, str]]:
        candidates: list[tuple[Path, str]] = []
        suffix = resolved_path.suffix.lower()
        if suffix in {".json", ".csv", ".txt", ".npy"}:
            candidates.append((resolved_path, f"FILE_{suffix[1:].upper()}"))
            return candidates
        if suffix == ".exr":
            sidecars = [
                (resolved_path.with_suffix(".json"), "FILE_JSON_SIDECAR"),
                (resolved_path.with_suffix(".csv"), "FILE_CSV_SIDECAR"),
                (resolved_path.with_suffix(".txt"), "FILE_TXT_SIDECAR"),
                (resolved_path.with_suffix(".npy"), "FILE_NPY_SIDECAR"),
                (Path(str(resolved_path) + ".json"), "FILE_JSON_SIDECAR"),
                (Path(str(resolved_path) + ".csv"), "FILE_CSV_SIDECAR"),
                (Path(str(resolved_path) + ".txt"), "FILE_TXT_SIDECAR"),
                (Path(str(resolved_path) + ".npy"), "FILE_NPY_SIDECAR"),
            ]
            candidates.extend(sidecars)
        return candidates

    def _load_channel_profile_taps_from_path(
        self,
        *,
        path: Path,
        sample_count: int,
    ) -> list[dict[str, float]]:
        suffix = path.suffix.lower()
        if suffix == ".json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and isinstance(payload.get("taps"), list):
                taps = []
                for raw_tap in payload["taps"]:
                    if not isinstance(raw_tap, dict):
                        continue
                    taps.append(
                        {
                            "az": float(raw_tap.get("az", 0.0)),
                            "el": float(raw_tap.get("el", 0.0)),
                            "weight": max(float(raw_tap.get("weight", 0.0)), 0.0),
                        }
                    )
                if sample_count > 0:
                    return taps[:sample_count]
                return taps
            grid = payload.get("weights", payload.get("data", payload.get("grid", payload))) if isinstance(payload, dict) else payload
            return self._channel_profile_taps_from_grid(grid=grid, sample_count=sample_count)
        if suffix in {".csv", ".txt"}:
            rows: list[list[float]] = []
            for raw_line in path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                if "," in line:
                    tokens = [token.strip() for token in line.split(",")]
                else:
                    tokens = line.split()
                rows.append([float(token) for token in tokens if token])
            return self._channel_profile_taps_from_grid(grid=rows, sample_count=sample_count)
        if suffix == ".npy" and np is not None:
            grid = np.load(path, allow_pickle=False)
            return self._channel_profile_taps_from_grid(grid=grid, sample_count=sample_count)
        return []

    def _channel_profile_taps_from_grid(
        self,
        *,
        grid: Any,
        sample_count: int,
    ) -> list[dict[str, float]]:
        if np is None:
            return []
        try:
            array = np.asarray(grid, dtype=float)
        except Exception:
            return []
        if array.ndim == 3:
            array = array[..., 0]
        if array.ndim != 2 or array.size == 0:
            return []
        max_value = float(array.max())
        if max_value <= 1e-12:
            return []
        height, width = int(array.shape[0]), int(array.shape[1])
        center_x = (width - 1) * 0.5
        center_y = (height - 1) * 0.5
        norm_x = max(center_x, 1.0)
        norm_y = max(center_y, 1.0)
        taps: list[dict[str, float]] = []
        nonzero_indices = np.argwhere(array > 0.0)
        weights = sorted(
            [
                (
                    float(array[row, col]),
                    int(row),
                    int(col),
                )
                for row, col in nonzero_indices
            ],
            reverse=True,
        )
        if sample_count > 0:
            weights = weights[:sample_count]
        for value, row, col in weights:
            az = (float(col) - center_x) / norm_x
            el = (center_y - float(row)) / norm_y
            if abs(az) <= 1e-9 and abs(el) <= 1e-9:
                continue
            taps.append(
                {
                    "az": max(min(az, 1.0), -1.0),
                    "el": max(min(el, 1.0), -1.0),
                    "weight": value / max_value,
                }
            )
        return taps

    def _lidar_channel_profile_metadata(
        self,
        *,
        request: SensorSimRequest,
        lidar_config: Any,
        primary_metadata: dict[str, object],
        base_metadata: dict[str, object],
        point_xyz: tuple[float, float, float],
        pattern: str,
        profile_weight: float,
        az_offset_rad: float,
        el_offset_rad: float,
        source: str,
        resolved_path: object | None,
    ) -> dict[str, object]:
        profile_data = lidar_config.channel_profile.profile_data
        effective_gain = max(float(profile_data.scale), 0.0) * max(float(profile_data.sidelobe_gain), 0.0) * max(profile_weight, 0.0)
        primary_signal_power_linear = pow(
            10.0,
            float(primary_metadata.get("signal_power_dbw", -120.0)) / 10.0,
        )
        signal_power_linear = primary_signal_power_linear * effective_gain
        ambient_photons = float(primary_metadata.get("ambient_photons", 0.0))
        signal_photons = signal_power_linear * max(float(lidar_config.physics_model.signal_photon_scale), 0.0)
        snr = signal_photons / max(ambient_photons, 1e-9)
        snr_db = 10.0 * log10(max(snr, 1e-9))
        minimum_snr_db = max(
            float(lidar_config.return_model.minimum_secondary_snr_db),
            float(lidar_config.physics_model.minimum_detection_snr_db),
        )
        reflectivity = float(primary_metadata.get("reflectivity", 0.0))
        ground_truth_reflectivity = float(primary_metadata.get("ground_truth_reflectivity", 0.0))
        laser_cross_section = float(primary_metadata.get("laser_cross_section", reflectivity))
        intensity_value = self._lidar_intensity_value(
            intensity_config=lidar_config.intensity,
            signal_power_linear=signal_power_linear,
            reflectivity=reflectivity,
            ground_truth_reflectivity=ground_truth_reflectivity,
            laser_cross_section=laser_cross_section,
            snr=snr,
        )
        metadata = dict(primary_metadata)
        metadata.update(
            {
                "detected": bool(lidar_config.physics_model.return_all_hits) or snr_db >= minimum_snr_db,
                "range_m": float(primary_metadata.get("range_m", 0.0)),
                "azimuth_deg": atan2(point_xyz[1], point_xyz[0]) * 180.0 / pi,
                "elevation_deg": atan2(
                    point_xyz[2],
                    max(sqrt(point_xyz[0] * point_xyz[0] + point_xyz[1] * point_xyz[1]), 1e-9),
                )
                * 180.0
                / pi,
                "intensity": intensity_value,
                "intensity_units": lidar_config.intensity.units,
                "reflectivity": reflectivity,
                "ground_truth_reflectivity": ground_truth_reflectivity,
                "laser_cross_section": laser_cross_section,
                "signal_power_dbw": 10.0 * log10(max(signal_power_linear, 1e-9)),
                "ambient_power_dbw": float(primary_metadata.get("ambient_power_dbw", -30.0)),
                "signal_photons": signal_photons,
                "ambient_photons": ambient_photons,
                "snr": snr,
                "snr_db": snr_db,
                "path_length_offset_m": 0.0,
                "ground_truth_hit_index": 0,
                "ground_truth_last_bounce_index": 0,
                "weather_extinction_factor": float(primary_metadata.get("weather_extinction_factor", 1.0)),
                "channel_profile_pattern": pattern,
                "channel_profile_file_uri": str(profile_data.file_uri),
                "channel_profile_source": source,
                "channel_profile_resolved_path": str(resolved_path) if resolved_path is not None else None,
                "channel_profile_weight": profile_weight,
                "channel_profile_scale": float(profile_data.scale),
                "channel_profile_offset_az_deg": az_offset_rad * 180.0 / pi,
                "channel_profile_offset_el_deg": el_offset_rad * 180.0 / pi,
                "channel_profile_half_angle_deg": self._lidar_channel_profile_half_angle_rad(
                    lidar_config=lidar_config,
                    profile_data=profile_data,
                )
                * 180.0
                / pi,
                "ground_truth_detection_type": "SIDELOBE",
            }
        )
        return metadata

    def _lidar_requested_return_count(self, lidar_config: Any) -> int:
        max_returns = max(1, int(getattr(lidar_config.return_model, "max_returns", 1)))
        count_mode = str(getattr(lidar_config.return_model, "mode", "SINGLE")).upper().strip()
        if count_mode == "SINGLE":
            return 1
        if count_mode == "DUAL":
            return min(max_returns, 2)
        return max_returns

    def _lidar_return_selection_mode(self, lidar_config: Any) -> str:
        selection_mode = str(getattr(lidar_config.return_model, "selection_mode", "FIRST")).upper().strip()
        if selection_mode not in {"FIRST", "LAST", "STRONGEST"}:
            return "FIRST"
        return selection_mode

    def _lidar_order_return_candidates(
        self,
        *,
        candidates: list[tuple[tuple[float, float, float], dict[str, object]]],
        selection_mode: str,
    ) -> list[tuple[tuple[float, float, float], dict[str, object]]]:
        if selection_mode == "STRONGEST":
            return sorted(
                candidates,
                key=lambda item: (
                    -float(item[1].get("snr_db", -1e9)),
                    float(item[1].get("range_m", sqrt(item[0][0] ** 2 + item[0][1] ** 2 + item[0][2] ** 2))),
                ),
            )
        if selection_mode == "LAST":
            return sorted(
                candidates,
                key=lambda item: float(item[1].get("range_m", sqrt(item[0][0] ** 2 + item[0][1] ** 2 + item[0][2] ** 2))),
                reverse=True,
            )
        return sorted(
            candidates,
            key=lambda item: float(item[1].get("range_m", sqrt(item[0][0] ** 2 + item[0][1] ** 2 + item[0][2] ** 2))),
        )

    def _lidar_merge_range_discriminated_returns(
        self,
        *,
        candidates: list[tuple[tuple[float, float, float], dict[str, object]]],
        lidar_config: Any,
    ) -> list[tuple[tuple[float, float, float], dict[str, object]]]:
        range_discrimination_m = max(float(lidar_config.return_model.range_discrimination_m), 0.0)
        if range_discrimination_m <= 1e-9 or len(candidates) <= 1:
            return candidates
        ordered = sorted(
            candidates,
            key=lambda item: float(item[1].get("range_m", sqrt(item[0][0] ** 2 + item[0][1] ** 2 + item[0][2] ** 2))),
        )
        merged: list[tuple[tuple[float, float, float], dict[str, object]]] = []
        cluster: list[tuple[tuple[float, float, float], dict[str, object]]] = []
        previous_range_m: float | None = None
        for candidate in ordered:
            candidate_range_m = float(
                candidate[1].get("range_m", sqrt(candidate[0][0] ** 2 + candidate[0][1] ** 2 + candidate[0][2] ** 2))
            )
            if not cluster:
                cluster = [candidate]
                previous_range_m = candidate_range_m
                continue
            if previous_range_m is not None and candidate_range_m - previous_range_m <= range_discrimination_m:
                cluster.append(candidate)
            else:
                merged.append(
                    self._lidar_merge_return_cluster(
                        cluster=cluster,
                        lidar_config=lidar_config,
                        range_discrimination_m=range_discrimination_m,
                    )
                )
                cluster = [candidate]
            previous_range_m = candidate_range_m
        if cluster:
            merged.append(
                self._lidar_merge_return_cluster(
                    cluster=cluster,
                    lidar_config=lidar_config,
                    range_discrimination_m=range_discrimination_m,
                )
            )
        return merged

    def _lidar_merge_return_cluster(
        self,
        *,
        cluster: list[tuple[tuple[float, float, float], dict[str, object]]],
        lidar_config: Any,
        range_discrimination_m: float,
    ) -> tuple[tuple[float, float, float], dict[str, object]]:
        if len(cluster) == 1:
            point_xyz, metadata = cluster[0]
            payload = dict(metadata)
            payload["merged_return_count"] = int(payload.get("merged_return_count", 1))
            payload["range_discrimination_m"] = range_discrimination_m
            return point_xyz, payload
        weights = [
            max(pow(10.0, float(metadata.get("signal_power_dbw", -120.0)) / 10.0), 1e-12)
            for _, metadata in cluster
        ]
        total_weight = sum(weights)
        dominant_index = max(range(len(cluster)), key=lambda index: weights[index])
        dominant_point, dominant_metadata = cluster[dominant_index]
        merged_point = (
            sum(point[0] * weight for (point, _), weight in zip(cluster, weights)) / total_weight,
            sum(point[1] * weight for (point, _), weight in zip(cluster, weights)) / total_weight,
            sum(point[2] * weight for (point, _), weight in zip(cluster, weights)) / total_weight,
        )
        range_m = sum(float(metadata.get("range_m", 0.0)) * weight for (_, metadata), weight in zip(cluster, weights)) / total_weight
        signal_power_linear = sum(weights)
        signal_photons = sum(float(metadata.get("signal_photons", 0.0)) for _, metadata in cluster)
        ambient_photons = max(float(dominant_metadata.get("ambient_photons", 0.0)), 1e-9)
        snr = signal_photons / ambient_photons
        snr_db = 10.0 * log10(max(snr, 1e-9))
        reflectivity = sum(float(metadata.get("reflectivity", 0.0)) * weight for (_, metadata), weight in zip(cluster, weights)) / total_weight
        ground_truth_reflectivity = sum(
            float(metadata.get("ground_truth_reflectivity", 0.0)) * weight
            for (_, metadata), weight in zip(cluster, weights)
        ) / total_weight
        laser_cross_section = sum(
            float(metadata.get("laser_cross_section", 0.0)) * weight
            for (_, metadata), weight in zip(cluster, weights)
        ) / total_weight
        intensity_value = self._lidar_intensity_value(
            intensity_config=lidar_config.intensity,
            signal_power_linear=signal_power_linear,
            reflectivity=reflectivity,
            ground_truth_reflectivity=ground_truth_reflectivity,
            laser_cross_section=laser_cross_section,
            snr=snr,
        )
        metadata = dict(dominant_metadata)
        metadata.update(
            {
                "detected": bool(lidar_config.physics_model.return_all_hits)
                or snr_db >= float(lidar_config.physics_model.minimum_detection_snr_db),
                "range_m": range_m,
                "intensity": intensity_value,
                "reflectivity": reflectivity,
                "ground_truth_reflectivity": ground_truth_reflectivity,
                "laser_cross_section": laser_cross_section,
                "signal_power_dbw": 10.0 * log10(max(signal_power_linear, 1e-9)),
                "signal_photons": signal_photons,
                "ambient_photons": ambient_photons,
                "snr": snr,
                "snr_db": snr_db,
                "path_length_offset_m": sum(
                    float(item_metadata.get("path_length_offset_m", 0.0)) * weight
                    for (_, item_metadata), weight in zip(cluster, weights)
                )
                / total_weight,
                "weather_extinction_factor": sum(
                    float(item_metadata.get("weather_extinction_factor", 1.0)) * weight
                    for (_, item_metadata), weight in zip(cluster, weights)
                )
                / total_weight,
                "merged_return_count": len(cluster),
                "range_discrimination_m": range_discrimination_m,
            }
        )
        return merged_point, metadata

    def _lidar_geometry_multipath_returns(
        self,
        *,
        request: SensorSimRequest,
        lidar_config: Any,
        point_xyz: tuple[float, float, float],
        primary_metadata: dict[str, object],
        base_metadata: dict[str, object],
    ) -> list[tuple[tuple[float, float, float], dict[str, object]]]:
        multipath_model = lidar_config.multipath_model
        if not bool(multipath_model.enabled):
            return []
        mode = str(multipath_model.mode).upper().strip()
        candidates: list[tuple[tuple[float, float, float], dict[str, object]]] = []
        if mode in {"GROUND_PLANE", "HYBRID"}:
            candidate = self._lidar_plane_multipath_candidate(
                request=request,
                lidar_config=lidar_config,
                primary_metadata=primary_metadata,
                base_metadata=base_metadata,
                reflected_point=self._lidar_reflect_point_across_horizontal_plane(
                    point_xyz=point_xyz,
                    plane_height_m=float(multipath_model.ground_plane_height_m),
                ),
                surface="GROUND_PLANE",
                surface_reflectivity=float(multipath_model.ground_reflectivity),
                plane_coordinate=float(multipath_model.ground_plane_height_m),
            )
            if candidate is not None:
                candidates.append(candidate)
        if mode in {"VERTICAL_PLANE", "HYBRID"}:
            candidate = self._lidar_plane_multipath_candidate(
                request=request,
                lidar_config=lidar_config,
                primary_metadata=primary_metadata,
                base_metadata=base_metadata,
                reflected_point=self._lidar_reflect_point_across_vertical_plane(
                    point_xyz=point_xyz,
                    plane_x_m=float(multipath_model.wall_plane_x_m),
                ),
                surface="VERTICAL_PLANE",
                surface_reflectivity=float(multipath_model.wall_reflectivity),
                plane_coordinate=float(multipath_model.wall_plane_x_m),
            )
            if candidate is not None:
                candidates.append(candidate)
        max_paths = max(1, int(multipath_model.max_paths))
        ordered_candidates = sorted(
            candidates,
            key=lambda item: float(item[1].get("range_m", 0.0)),
        )[:max_paths]
        for hit_index, (_, metadata) in enumerate(ordered_candidates, start=1):
            metadata["ground_truth_hit_index"] = hit_index
            metadata["ground_truth_last_bounce_index"] = 1
        return ordered_candidates

    def _lidar_plane_multipath_candidate(
        self,
        *,
        request: SensorSimRequest,
        lidar_config: Any,
        primary_metadata: dict[str, object],
        base_metadata: dict[str, object],
        reflected_point: tuple[float, float, float] | None,
        surface: str,
        surface_reflectivity: float,
        plane_coordinate: float,
    ) -> tuple[tuple[float, float, float], dict[str, object]] | None:
        if reflected_point is None:
            return None
        base_range_m = float(primary_metadata.get("range_m", 0.0))
        path_length_m = sqrt(
            reflected_point[0] * reflected_point[0]
            + reflected_point[1] * reflected_point[1]
            + reflected_point[2] * reflected_point[2]
        )
        path_length_offset_m = path_length_m - base_range_m
        if path_length_offset_m <= 1e-6:
            return None
        if path_length_offset_m > float(lidar_config.multipath_model.max_extra_path_length_m):
            return None
        reflection_point = self._lidar_multipath_reflection_point(
            reflected_point=reflected_point,
            surface=surface,
            plane_coordinate=plane_coordinate,
        )
        if reflection_point is None:
            return None
        return (
            reflected_point,
            self._lidar_multipath_metadata(
                request=request,
                lidar_config=lidar_config,
                primary_metadata=primary_metadata,
                base_metadata=base_metadata,
                point_xyz=reflected_point,
                reflection_point=reflection_point,
                surface=surface,
                surface_reflectivity=surface_reflectivity,
                path_length_m=path_length_m,
                path_length_offset_m=path_length_offset_m,
            ),
        )

    def _lidar_reflect_point_across_horizontal_plane(
        self,
        *,
        point_xyz: tuple[float, float, float],
        plane_height_m: float,
    ) -> tuple[float, float, float] | None:
        x, y, z = point_xyz
        if (0.0 - plane_height_m) * (z - plane_height_m) <= 0.0:
            return None
        reflected_z = 2.0 * plane_height_m - z
        return (x, y, reflected_z)

    def _lidar_reflect_point_across_vertical_plane(
        self,
        *,
        point_xyz: tuple[float, float, float],
        plane_x_m: float,
    ) -> tuple[float, float, float] | None:
        x, y, z = point_xyz
        if plane_x_m <= 0.0:
            return None
        if (0.0 - plane_x_m) * (x - plane_x_m) <= 0.0:
            return None
        reflected_x = 2.0 * plane_x_m - x
        return (reflected_x, y, z)

    def _lidar_multipath_reflection_point(
        self,
        *,
        reflected_point: tuple[float, float, float],
        surface: str,
        plane_coordinate: float,
    ) -> tuple[float, float, float] | None:
        x, y, z = reflected_point
        if surface == "GROUND_PLANE":
            if abs(z) <= 1e-9:
                return None
            alpha = plane_coordinate / z
            if alpha <= 0.0 or alpha >= 1.0:
                return None
            return (x * alpha, y * alpha, plane_coordinate)
        if abs(x) <= 1e-9:
            return None
        alpha = plane_coordinate / x
        if alpha <= 0.0 or alpha >= 1.0:
            return None
        return (plane_coordinate, y * alpha, z * alpha)

    def _lidar_multipath_metadata(
        self,
        *,
        request: SensorSimRequest,
        lidar_config: Any,
        primary_metadata: dict[str, object],
        base_metadata: dict[str, object],
        point_xyz: tuple[float, float, float],
        reflection_point: tuple[float, float, float],
        surface: str,
        surface_reflectivity: float,
        path_length_m: float,
        path_length_offset_m: float,
    ) -> dict[str, object]:
        primary_range_m = max(float(primary_metadata.get("range_m", 0.0)), 1e-9)
        primary_signal_power_linear = pow(
            10.0,
            float(primary_metadata.get("signal_power_dbw", -120.0)) / 10.0,
        )
        attenuation_rate = max(0.0, float(lidar_config.physics_model.atmospheric_attenuation_rate))
        primary_weather = max(float(primary_metadata.get("weather_extinction_factor", 1.0)), 1e-9)
        multipath_weather = self._lidar_weather_extinction_factor(
            lidar_config=lidar_config,
            range_m=path_length_m,
        )
        geometry_scale = (
            max(primary_range_m * primary_range_m, 1.0)
            / max(path_length_m * path_length_m, 1.0)
            * exp(-attenuation_rate * max(path_length_m - primary_range_m, 0.0))
            * (multipath_weather / primary_weather)
        )
        signal_scale = max(float(lidar_config.multipath_model.path_signal_decay), 0.0) * max(
            surface_reflectivity,
            0.0,
        )
        signal_power_linear = primary_signal_power_linear * geometry_scale * signal_scale
        ambient_power_dbw = float(primary_metadata.get("ambient_power_dbw", -30.0))
        ambient_photons = float(primary_metadata.get("ambient_photons", 0.0))
        signal_photons = signal_power_linear * max(float(lidar_config.physics_model.signal_photon_scale), 0.0)
        snr = signal_photons / max(ambient_photons, 1e-9)
        snr_db = 10.0 * log10(max(snr, 1e-9))
        minimum_path_snr_db = max(
            float(lidar_config.multipath_model.minimum_path_snr_db),
            float(lidar_config.physics_model.minimum_detection_snr_db),
        )
        reflectivity = min(max(float(primary_metadata.get("reflectivity", 0.0)) * surface_reflectivity, 0.0), 1.0)
        ground_truth_reflectivity = min(
            max(float(primary_metadata.get("ground_truth_reflectivity", 0.0)) * surface_reflectivity, 0.0),
            1.0,
        )
        laser_cross_section = reflectivity
        intensity_value = self._lidar_intensity_value(
            intensity_config=lidar_config.intensity,
            signal_power_linear=signal_power_linear,
            reflectivity=reflectivity,
            ground_truth_reflectivity=ground_truth_reflectivity,
            laser_cross_section=laser_cross_section,
            snr=snr,
        )
        azimuth_deg = atan2(point_xyz[1], point_xyz[0]) * 180.0 / pi
        elevation_deg = atan2(
            point_xyz[2],
            max(sqrt(point_xyz[0] * point_xyz[0] + point_xyz[1] * point_xyz[1]), 1e-9),
        ) * 180.0 / pi
        metadata = dict(primary_metadata)
        metadata.update(
            {
                "detected": bool(lidar_config.physics_model.return_all_hits) or snr_db >= minimum_path_snr_db,
                "range_m": path_length_m,
                "azimuth_deg": azimuth_deg,
                "elevation_deg": elevation_deg,
                "intensity": intensity_value,
                "intensity_units": lidar_config.intensity.units,
                "reflectivity": reflectivity,
                "ground_truth_reflectivity": ground_truth_reflectivity,
                "laser_cross_section": laser_cross_section,
                "signal_power_dbw": 10.0 * log10(max(signal_power_linear, 1e-9)),
                "ambient_power_dbw": ambient_power_dbw,
                "signal_photons": signal_photons,
                "ambient_photons": ambient_photons,
                "snr": snr,
                "snr_db": snr_db,
                "path_length_offset_m": path_length_offset_m,
                "ground_truth_hit_index": 1,
                "ground_truth_last_bounce_index": 1,
                "weather_extinction_factor": multipath_weather,
                "precipitation_type": primary_metadata.get("precipitation_type"),
                "particle_field_density": primary_metadata.get("particle_field_density"),
                "particle_diameter_mm": primary_metadata.get("particle_diameter_mm"),
                "particle_terminal_velocity_mps": primary_metadata.get("particle_terminal_velocity_mps"),
                "particle_reflectivity": primary_metadata.get("particle_reflectivity"),
                "particle_backscatter_strength": primary_metadata.get("particle_backscatter_strength"),
                "precipitation_extinction_alpha": primary_metadata.get("precipitation_extinction_alpha"),
                "multipath_surface": surface,
                "multipath_path_length_m": path_length_m,
                "multipath_base_range_m": primary_range_m,
                "multipath_surface_reflectivity": surface_reflectivity,
                "multipath_model_mode": str(lidar_config.multipath_model.mode).upper(),
                "multipath_reflection_point": {
                    "x": reflection_point[0],
                    "y": reflection_point[1],
                    "z": reflection_point[2],
                },
                "ground_truth_detection_type": "MULTIPATH",
            }
        )
        return metadata

    def _lidar_signal_metadata(
        self,
        *,
        request: SensorSimRequest,
        lidar_config: Any,
        point_xyz: tuple[float, float, float],
        base_metadata: dict[str, object],
    ) -> dict[str, object]:
        x, y, z = point_xyz
        range_m = float(base_metadata.get("range_m", sqrt(x * x + y * y + z * z)))
        environment_model = lidar_config.environment_model
        precipitation_profile = self._lidar_precipitation_profile(environment_model=environment_model)
        channel_id = base_metadata.get("channel_id")
        channel_loss_db = self._lidar_channel_loss_db(
            emitter_params=lidar_config.emitter_params,
            channel_id=channel_id if isinstance(channel_id, int) else None,
        )
        optical_loss_db = self._lidar_interpolate_optical_loss_db(
            emitter_params=lidar_config.emitter_params,
            range_m=range_m,
        )
        peak_power_w = max(float(lidar_config.emitter_params.peak_power_w), 0.0)
        divergence_scale, beam_footprint_area_m2 = self._lidar_divergence_scale(
            emitter_params=lidar_config.emitter_params,
            range_m=range_m,
        )
        ground_truth_reflectivity = self._lidar_ground_truth_reflectivity(
            request=request,
            base_metadata=base_metadata,
        )
        reflectivity = max(
            0.0,
            ground_truth_reflectivity * float(lidar_config.physics_model.reflectivity_coefficient),
        )
        attenuation_rate = max(0.0, float(lidar_config.physics_model.atmospheric_attenuation_rate))
        weather_extinction = self._lidar_weather_extinction_factor(
            lidar_config=lidar_config,
            range_m=range_m,
        )
        signal_power_linear = (
            reflectivity
            * max(peak_power_w, 0.0)
            * exp(-attenuation_rate * range_m)
            * weather_extinction
            * pow(10.0, (channel_loss_db + optical_loss_db) / 10.0)
            * divergence_scale
            / max(range_m * range_m, 1.0)
        )
        ambient_power_dbw = float(lidar_config.physics_model.ambient_power_dbw)
        if not bool(environment_model.enable_ambient):
            ambient_power_dbw = -120.0
        ambient_power_linear = pow(10.0, ambient_power_dbw / 10.0)
        signal_photons = signal_power_linear * max(float(lidar_config.physics_model.signal_photon_scale), 0.0)
        ambient_photons = ambient_power_linear * max(float(lidar_config.physics_model.ambient_photon_scale), 0.0)
        snr = signal_photons / max(ambient_photons, 1e-9)
        snr_db = 10.0 * log10(max(snr, 1e-9))
        laser_cross_section = reflectivity
        ground_truth_annotation = self._sensor_ground_truth_annotation(
            request=request,
            sensor_prefix="lidar",
            point_index=int(base_metadata.get("point_index", 0)),
            world_point=(
                float(base_metadata.get("x", point_xyz[0])),
                float(base_metadata.get("y", point_xyz[1])),
                float(base_metadata.get("z", point_xyz[2])),
            ),
        )
        intensity_value = self._lidar_intensity_value(
            intensity_config=lidar_config.intensity,
            signal_power_linear=signal_power_linear,
            reflectivity=reflectivity,
            ground_truth_reflectivity=ground_truth_reflectivity,
            laser_cross_section=laser_cross_section,
            snr=snr,
        )
        detected = bool(lidar_config.physics_model.return_all_hits) or (
            snr_db >= float(lidar_config.physics_model.minimum_detection_snr_db)
        )
        return {
            "detected": detected,
            "range_m": range_m,
            "intensity": intensity_value,
            "intensity_units": lidar_config.intensity.units,
            "reflectivity": reflectivity,
            "ground_truth_reflectivity": ground_truth_reflectivity,
            "laser_cross_section": laser_cross_section,
            "signal_power_dbw": 10.0 * log10(max(signal_power_linear, 1e-9)),
            "ambient_power_dbw": ambient_power_dbw,
            "signal_photons": signal_photons,
            "ambient_photons": ambient_photons,
            "snr": snr,
            "snr_db": snr_db,
            "return_id": 0,
            "path_length_offset_m": 0.0,
            "ground_truth_hit_index": 0,
            "ground_truth_last_bounce_index": 0,
            "weather_extinction_factor": weather_extinction,
            "precipitation_type": str(precipitation_profile["precipitation_type"]),
            "particle_field_density": float(precipitation_profile["particle_field_density"]),
            "particle_diameter_mm": float(precipitation_profile["particle_diameter_mm"]),
            "particle_terminal_velocity_mps": float(precipitation_profile["terminal_velocity_mps"]),
            "particle_reflectivity": float(precipitation_profile["particle_reflectivity"]),
            "particle_backscatter_strength": float(precipitation_profile["particle_backscatter_strength"]),
            "precipitation_extinction_alpha": float(
                precipitation_profile["precipitation_extinction_alpha"]
            ),
            "channel_loss_db": channel_loss_db,
            "optical_loss_db": optical_loss_db,
            "peak_power_w": peak_power_w,
            "beam_divergence_az_rad": float(lidar_config.emitter_params.source_divergence.az),
            "beam_divergence_el_rad": float(lidar_config.emitter_params.source_divergence.el),
            "beam_footprint_area_m2": beam_footprint_area_m2,
            "ground_truth_detection_type": "TARGET",
            **ground_truth_annotation,
        }

    def _lidar_ground_truth_reflectivity(
        self,
        *,
        request: SensorSimRequest,
        base_metadata: dict[str, object],
    ) -> float:
        point_index = int(base_metadata.get("point_index", -1))
        point_reflectivities = request.options.get("lidar_point_reflectivities")
        if isinstance(point_reflectivities, list) and 0 <= point_index < len(point_reflectivities):
            return min(max(float(point_reflectivities[point_index]), 0.0), 1.0)
        channel_id = base_metadata.get("channel_id")
        channel_reflectivities = request.options.get("lidar_channel_reflectivities")
        if (
            isinstance(channel_reflectivities, list)
            and isinstance(channel_id, int)
            and 0 <= channel_id < len(channel_reflectivities)
        ):
            return min(max(float(channel_reflectivities[channel_id]), 0.0), 1.0)
        return min(max(float(request.options.get("lidar_ground_truth_reflectivity", 0.35)), 0.0), 1.0)

    def _lidar_channel_loss_db(
        self,
        *,
        emitter_params: Any,
        channel_id: int | None,
    ) -> float:
        loss_db = float(emitter_params.global_source_loss_db)
        if (
            channel_id is not None
            and 0 <= channel_id < len(emitter_params.source_losses_db)
        ):
            loss_db += float(emitter_params.source_losses_db[channel_id])
        return loss_db

    def _lidar_interpolate_optical_loss_db(self, *, emitter_params: Any, range_m: float) -> float:
        points = sorted(
            [
                {
                    "range": float(point.range_m),
                    "loss": float(point.loss_db),
                }
                for point in getattr(emitter_params, "optical_loss", [])
            ],
            key=lambda point: point["range"],
        )
        if not points:
            return 0.0
        if range_m <= points[0]["range"]:
            return points[0]["loss"]
        if range_m >= points[-1]["range"]:
            return points[-1]["loss"]
        for index in range(1, len(points)):
            left = points[index - 1]
            right = points[index]
            if range_m > right["range"]:
                continue
            delta = right["range"] - left["range"]
            if abs(delta) <= 1e-9:
                return right["loss"]
            alpha = (range_m - left["range"]) / delta
            return left["loss"] + (right["loss"] - left["loss"]) * alpha
        return 0.0

    def _lidar_divergence_scale(self, *, emitter_params: Any, range_m: float) -> tuple[float, float]:
        divergence_az = abs(float(emitter_params.source_divergence.az))
        divergence_el = abs(float(emitter_params.source_divergence.el))
        width_m = max(range_m * divergence_az, 0.1)
        height_m = max(range_m * divergence_el, 0.1)
        beam_footprint_area_m2 = width_m * height_m
        reference_area_m2 = 0.01
        divergence_scale = min(1.0, reference_area_m2 / max(beam_footprint_area_m2, reference_area_m2))
        return divergence_scale, beam_footprint_area_m2

    def _lidar_emitter_adjustment(
        self,
        *,
        lidar_config: Any,
        base_metadata: dict[str, object],
        rng: random.Random,
    ) -> dict[str, object]:
        variance_az = max(float(lidar_config.emitter_params.source_variance.az), 0.0)
        variance_el = max(float(lidar_config.emitter_params.source_variance.el), 0.0)
        az_offset_rad = rng.gauss(0.0, sqrt(variance_az)) if variance_az > 0.0 else 0.0
        el_offset_rad = rng.gauss(0.0, sqrt(variance_el)) if variance_el > 0.0 else 0.0
        channel_id = base_metadata.get("channel_id")
        return {
            "az_offset_rad": az_offset_rad,
            "el_offset_rad": el_offset_rad,
            "metadata": {
                "beam_azimuth_offset_deg": az_offset_rad * 180.0 / pi,
                "beam_elevation_offset_deg": el_offset_rad * 180.0 / pi,
                "channel_loss_db": self._lidar_channel_loss_db(
                    emitter_params=lidar_config.emitter_params,
                    channel_id=channel_id if isinstance(channel_id, int) else None,
                ),
                "peak_power_w": float(lidar_config.emitter_params.peak_power_w),
                "beam_divergence_az_rad": float(lidar_config.emitter_params.source_divergence.az),
                "beam_divergence_el_rad": float(lidar_config.emitter_params.source_divergence.el),
            },
        }

    def _lidar_apply_emitter_adjustment_to_point(
        self,
        *,
        point_xyz: tuple[float, float, float],
        az_offset_rad: float,
        el_offset_rad: float,
    ) -> tuple[float, float, float]:
        x, y, z = point_xyz
        range_m = sqrt(x * x + y * y + z * z)
        if range_m <= 1e-9 or (abs(az_offset_rad) <= 1e-12 and abs(el_offset_rad) <= 1e-12):
            return point_xyz
        base_azimuth_rad = atan2(y, x)
        horizontal = sqrt(x * x + y * y)
        base_elevation_rad = atan2(z, max(horizontal, 1e-9))
        azimuth_rad = base_azimuth_rad + az_offset_rad
        elevation_rad = base_elevation_rad + el_offset_rad
        xy = range_m * cos(elevation_rad)
        return (
            xy * cos(azimuth_rad),
            xy * sin(azimuth_rad),
            range_m * sin(elevation_rad),
        )

    def _lidar_precipitation_profile(self, *, environment_model: Any) -> dict[str, float | int | str]:
        precipitation_rate = max(float(environment_model.precipitation_rate), 0.0)
        configured_type = str(getattr(environment_model, "precipitation_type", "RAIN")).upper().strip() or "RAIN"
        normalized_type = configured_type if configured_type in _LIDAR_PRECIPITATION_DEFAULTS else "RAIN"
        resolved_type = normalized_type if precipitation_rate > 0.0 else "NONE"
        defaults = _LIDAR_PRECIPITATION_DEFAULTS[normalized_type]
        particle_density_scale = max(float(getattr(environment_model, "particle_density_scale", 1.0)), 0.0)
        particle_diameter_override_mm = max(float(getattr(environment_model, "particle_diameter_mm", 0.0)), 0.0)
        particle_diameter_mm = (
            particle_diameter_override_mm
            if particle_diameter_override_mm > 0.0
            else defaults["particle_diameter_mm"]
        )
        terminal_velocity_override_mps = max(
            float(getattr(environment_model, "terminal_velocity_mps", 0.0)),
            0.0,
        )
        terminal_velocity_mps = (
            terminal_velocity_override_mps
            if terminal_velocity_override_mps > 0.0
            else defaults["terminal_velocity_mps"]
        )
        particle_reflectivity_override = float(getattr(environment_model, "particle_reflectivity", 0.0))
        particle_reflectivity = min(
            max(
                particle_reflectivity_override
                if particle_reflectivity_override > 0.0
                else defaults["particle_reflectivity"],
                0.0,
            ),
            1.0,
        )
        backscatter_jitter = max(float(getattr(environment_model, "backscatter_jitter", 0.1)), 0.0)
        field_seed = int(getattr(environment_model, "field_seed", 0))
        particle_field_density = precipitation_rate * particle_density_scale * defaults["density_coefficient"]
        precipitation_extinction_alpha = particle_field_density * defaults["extinction_coefficient"] * (
            particle_diameter_mm / max(defaults["particle_diameter_mm"], 1e-9)
        )
        particle_backscatter_strength = particle_field_density * defaults["backscatter_gain"] * max(
            particle_reflectivity,
            0.05,
        )
        return {
            "configured_type": normalized_type,
            "precipitation_type": resolved_type,
            "precipitation_rate": precipitation_rate,
            "particle_density_scale": particle_density_scale,
            "particle_diameter_mm": particle_diameter_mm,
            "terminal_velocity_mps": terminal_velocity_mps,
            "particle_reflectivity": particle_reflectivity,
            "backscatter_jitter": backscatter_jitter,
            "field_seed": field_seed,
            "particle_field_density": particle_field_density,
            "precipitation_extinction_alpha": precipitation_extinction_alpha,
            "particle_backscatter_strength": particle_backscatter_strength,
        }

    def _lidar_weather_extinction_factor(self, *, lidar_config: Any, range_m: float) -> float:
        fog_density = min(max(float(lidar_config.environment_model.fog_density), 0.0), 1.0)
        extinction_scale = max(float(lidar_config.environment_model.extinction_coefficient_scale), 0.0)
        precipitation_profile = self._lidar_precipitation_profile(
            environment_model=lidar_config.environment_model,
        )
        alpha = (
            fog_density * extinction_scale
            + float(precipitation_profile["precipitation_extinction_alpha"])
        )
        return exp(-2.0 * alpha * max(range_m, 0.0))

    def _lidar_environment_backscatter_returns(
        self,
        *,
        request: SensorSimRequest,
        lidar_config: Any,
        point_xyz: tuple[float, float, float],
        primary_metadata: dict[str, object],
        base_metadata: dict[str, object],
    ) -> list[tuple[tuple[float, float, float], dict[str, object]]]:
        if bool(lidar_config.environment_model.disable_backscatter):
            return []
        fog_density = min(max(float(lidar_config.environment_model.fog_density), 0.0), 1.0)
        precipitation_profile = self._lidar_precipitation_profile(
            environment_model=lidar_config.environment_model,
        )
        backscatter_scale = max(float(lidar_config.environment_model.backscatter_scale), 0.0)
        environment_strength = (
            fog_density + float(precipitation_profile["particle_backscatter_strength"])
        ) * backscatter_scale
        if environment_strength <= 1e-9:
            return []

        range_m = float(primary_metadata.get("range_m", 0.0))
        if range_m <= max(lidar_config.range_min_m + 0.5, 1.0):
            return []

        point_index_raw = base_metadata.get("point_index", 0)
        point_index = int(point_index_raw) if isinstance(point_index_raw, (int, float)) else 0
        channel_id_raw = base_metadata.get("channel_id", 0)
        channel_id = int(channel_id_raw) if isinstance(channel_id_raw, (int, float)) else 0
        rng = random.Random(
            int(request.seed)
            + int(precipitation_profile["field_seed"])
            + point_index * 9973
            + channel_id * 31337
        )
        jitter = min(float(precipitation_profile["backscatter_jitter"]), 2.0)
        backscatter_fraction = min(
            max(
                0.15
                + 0.18 * fog_density
                + 0.24 * float(precipitation_profile["particle_field_density"])
                + rng.gauss(0.0, jitter * 0.03),
                0.1,
            ),
            0.9,
        )
        backscatter_range = max(lidar_config.range_min_m + 0.25, range_m * backscatter_fraction)
        backscatter_point = self._lidar_offset_point_along_ray(
            point_xyz=point_xyz,
            range_offset_m=backscatter_range - range_m,
        )
        ambient_photons = float(primary_metadata.get("ambient_photons", 0.0))
        signal_power_linear = pow(
            10.0,
            float(primary_metadata.get("signal_power_dbw", -120.0)) / 10.0,
        ) * min(
            max(
                environment_strength
                * (0.22 + 0.35 * float(precipitation_profile["particle_reflectivity"])),
                0.02,
            ),
            0.8,
        )
        signal_photons = signal_power_linear * max(float(lidar_config.physics_model.signal_photon_scale), 0.0)
        snr = signal_photons / max(ambient_photons, 1e-9)
        snr_db = 10.0 * log10(max(snr, 1e-9))
        min_backscatter_snr_db = max(
            float(lidar_config.return_model.minimum_secondary_snr_db),
            float(lidar_config.physics_model.minimum_detection_snr_db),
        )
        detected = bool(lidar_config.physics_model.return_all_hits) or snr_db >= min_backscatter_snr_db
        if not detected:
            return []
        reflectivity = min(
            max(
                float(precipitation_profile["particle_reflectivity"]) * environment_strength,
                0.0,
            ),
            1.0,
        )
        intensity_value = self._lidar_intensity_value(
            intensity_config=lidar_config.intensity,
            signal_power_linear=signal_power_linear,
            reflectivity=reflectivity,
            ground_truth_reflectivity=reflectivity,
            laser_cross_section=reflectivity,
            snr=snr,
        )
        metadata = dict(base_metadata)
        metadata.update(
            {
                "detected": True,
                "range_m": backscatter_range,
                "intensity": intensity_value,
                "intensity_units": lidar_config.intensity.units,
                "reflectivity": reflectivity,
                "ground_truth_reflectivity": reflectivity,
                "laser_cross_section": reflectivity,
                "signal_power_dbw": 10.0 * log10(max(signal_power_linear, 1e-9)),
                "ambient_power_dbw": float(primary_metadata.get("ambient_power_dbw", -30.0)),
                "signal_photons": signal_photons,
                "ambient_photons": ambient_photons,
                "snr": snr,
                "snr_db": snr_db,
                "return_id": max(int(primary_metadata.get("return_id", 0)) + 1, 1),
                "path_length_offset_m": backscatter_range - range_m,
                "ground_truth_hit_index": 0,
                "ground_truth_last_bounce_index": 0,
                "weather_extinction_factor": self._lidar_weather_extinction_factor(
                    lidar_config=lidar_config,
                    range_m=backscatter_range,
                ),
                "precipitation_type": str(precipitation_profile["precipitation_type"]),
                "particle_field_density": float(precipitation_profile["particle_field_density"]),
                "particle_diameter_mm": float(precipitation_profile["particle_diameter_mm"]),
                "particle_terminal_velocity_mps": float(precipitation_profile["terminal_velocity_mps"]),
                "particle_reflectivity": float(precipitation_profile["particle_reflectivity"]),
                "particle_backscatter_strength": float(precipitation_profile["particle_backscatter_strength"]),
                "precipitation_extinction_alpha": float(
                    precipitation_profile["precipitation_extinction_alpha"]
                ),
                "ground_truth_detection_type": "NOISE",
            }
        )
        return [(backscatter_point, metadata)]

    def _generate_lidar_false_alarm_points(
        self,
        *,
        request: SensorSimRequest,
        lidar_config: Any,
        source_point_count: int,
        rng: random.Random,
    ) -> tuple[list[tuple[float, float, float]], list[dict[str, object]]]:
        pfa = min(max(float(lidar_config.noise_performance.probability_false_alarm), 0.0), 1.0)
        if pfa <= 1e-12:
            return [], []
        expected_false_count = pfa * max(source_point_count, 1) * 32.0
        false_count = int(expected_false_count)
        if rng.random() < (expected_false_count - false_count):
            false_count += 1
        if false_count <= 0:
            return [], []

        az_min = float(lidar_config.scan_field_azimuth_min_deg)
        az_max = float(lidar_config.scan_field_azimuth_max_deg)
        el_min = float(lidar_config.scan_field_elevation_min_deg)
        el_max = float(lidar_config.scan_field_elevation_max_deg)
        min_range = max(float(lidar_config.range_min_m), 0.5)
        max_range = max(min(float(lidar_config.range_max_m), 40.0), min_range + 0.5)
        points: list[tuple[float, float, float]] = []
        metadata_points: list[dict[str, object]] = []
        for index in range(false_count):
            azimuth_deg = rng.uniform(az_min, az_max)
            elevation_deg = rng.uniform(el_min, el_max)
            range_m = rng.uniform(min_range, max_range)
            azimuth_rad = azimuth_deg * pi / 180.0
            elevation_rad = elevation_deg * pi / 180.0
            xy = range_m * cos(elevation_rad)
            point = (
                xy * cos(azimuth_rad),
                xy * sin(azimuth_rad),
                range_m * sin(elevation_rad),
            )
            signal_power_linear = pow(
                10.0,
                float(lidar_config.physics_model.ambient_power_dbw) / 10.0,
            ) * rng.uniform(0.5, 1.2)
            ambient_photons = pow(10.0, float(lidar_config.physics_model.ambient_power_dbw) / 10.0) * max(
                float(lidar_config.physics_model.ambient_photon_scale),
                0.0,
            )
            signal_photons = signal_power_linear * max(float(lidar_config.physics_model.signal_photon_scale), 0.0)
            snr = signal_photons / max(ambient_photons, 1e-9)
            snr_db = 10.0 * log10(max(snr, 1e-9))
            intensity_value = self._lidar_intensity_value(
                intensity_config=lidar_config.intensity,
                signal_power_linear=signal_power_linear,
                reflectivity=0.0,
                ground_truth_reflectivity=0.0,
                laser_cross_section=0.0,
                snr=snr,
            )
            points.append(point)
            metadata_points.append(
                {
                    "point_index": source_point_count + index,
                    "range_m": range_m,
                    "azimuth_deg": azimuth_deg,
                    "elevation_deg": elevation_deg,
                    "channel_id": None,
                    "source_angle_deg": None,
                    "scan_path_index": None,
                    "intensity": intensity_value,
                    "intensity_units": lidar_config.intensity.units,
                    "reflectivity": 0.0,
                    "ground_truth_reflectivity": 0.0,
                    "laser_cross_section": 0.0,
                    "signal_power_dbw": 10.0 * log10(max(signal_power_linear, 1e-9)),
                    "ambient_power_dbw": float(lidar_config.physics_model.ambient_power_dbw),
                    "signal_photons": signal_photons,
                    "ambient_photons": ambient_photons,
                    "snr": snr,
                    "snr_db": snr_db,
                    "return_id": 0,
                    "path_length_offset_m": 0.0,
                    "ground_truth_hit_index": 0,
                    "ground_truth_last_bounce_index": 0,
                    "weather_extinction_factor": 1.0,
                    "ground_truth_detection_type": "NOISE",
                }
            )
        return points, metadata_points

    def _lidar_intensity_value(
        self,
        *,
        intensity_config: Any,
        signal_power_linear: float,
        reflectivity: float,
        ground_truth_reflectivity: float,
        laser_cross_section: float,
        snr: float,
    ) -> float:
        units = str(intensity_config.units).upper().strip()
        if units == "SNR":
            return snr
        if units == "SNR_SCALED":
            return self._lidar_scale_intensity(raw_value=snr, intensity_config=intensity_config)
        if units == "REFLECTIVITY":
            return reflectivity
        if units == "REFLECTIVITY_SCALED":
            return self._lidar_scale_intensity(raw_value=reflectivity, intensity_config=intensity_config)
        if units == "POWER":
            return signal_power_linear
        if units == "LASER_CROSS_SECTION":
            return laser_cross_section
        if units == "GROUND_TRUTH_REFLECTIVITY":
            return ground_truth_reflectivity
        return reflectivity

    def _lidar_scale_intensity(self, *, raw_value: float, intensity_config: Any) -> float:
        range_scale_map = sorted(
            [
                {
                    "input": float(getattr(point, "input_value", 0.0)),
                    "output": float(getattr(point, "output_value", 0.0)),
                }
                for point in getattr(intensity_config, "range_scale_map", [])
            ],
            key=lambda point: point["input"],
        )
        if range_scale_map:
            if raw_value <= range_scale_map[0]["input"]:
                return range_scale_map[0]["output"]
            if raw_value >= range_scale_map[-1]["input"]:
                return range_scale_map[-1]["output"]
            for index in range(1, len(range_scale_map)):
                left = range_scale_map[index - 1]
                right = range_scale_map[index]
                if raw_value > right["input"]:
                    continue
                delta = right["input"] - left["input"]
                if abs(delta) <= 1e-9:
                    return right["output"]
                alpha = (raw_value - left["input"]) / delta
                return left["output"] + (right["output"] - left["output"]) * alpha
        input_min = float(intensity_config.input_range.min_value)
        input_max = float(intensity_config.input_range.max_value)
        output_min = float(intensity_config.output_scale.min_value)
        output_max = float(intensity_config.output_scale.max_value)
        if input_max <= input_min:
            return output_min
        alpha = (raw_value - input_min) / (input_max - input_min)
        alpha = min(max(alpha, 0.0), 1.0)
        return output_min + (output_max - output_min) * alpha

    def _update_lidar_signal_metrics(
        self,
        *,
        metrics: dict[str, float],
        preview_points: list[dict[str, object]],
        lidar_config: Any,
    ) -> None:
        snr_db_values = [
            float(point["snr_db"])
            for point in preview_points
            if isinstance(point, dict) and point.get("snr_db") is not None
        ]
        metrics["lidar_intensity_output_count"] = float(
            sum(
                1
                for point in preview_points
                if isinstance(point, dict) and point.get("intensity") is not None
            )
        )
        metrics["lidar_detection_snr_threshold_db"] = float(
            lidar_config.physics_model.minimum_detection_snr_db
        )
        return_ids = [
            int(point["return_id"])
            for point in preview_points
            if isinstance(point, dict) and point.get("return_id") is not None
        ]
        noise_points = [
            point
            for point in preview_points
            if isinstance(point, dict) and str(point.get("ground_truth_detection_type", "")).upper() == "NOISE"
        ]
        multipath_points = [
            point
            for point in preview_points
            if isinstance(point, dict) and str(point.get("ground_truth_detection_type", "")).upper() == "MULTIPATH"
        ]
        sidelobe_points = [
            point
            for point in preview_points
            if isinstance(point, dict) and str(point.get("ground_truth_detection_type", "")).upper() == "SIDELOBE"
        ]
        channel_profile_sources = [
            str(point["channel_profile_source"])
            for point in preview_points
            if isinstance(point, dict) and point.get("channel_profile_source")
        ]
        beam_offset_values = [
            abs(float(point["beam_azimuth_offset_deg"]))
            for point in preview_points
            if isinstance(point, dict) and point.get("beam_azimuth_offset_deg") is not None
        ]
        channel_loss_values = [
            float(point["channel_loss_db"])
            for point in preview_points
            if isinstance(point, dict) and point.get("channel_loss_db") is not None
        ]
        merged_counts = [
            int(point["merged_return_count"])
            for point in preview_points
            if isinstance(point, dict) and point.get("merged_return_count") is not None
        ]
        metrics["lidar_secondary_return_count"] = float(
            sum(1 for return_id in return_ids if return_id > 0)
        )
        metrics["lidar_multi_return_applied"] = 1.0 if any(return_id > 0 for return_id in return_ids) else 0.0
        metrics["lidar_max_return_id"] = float(max(return_ids)) if return_ids else 0.0
        metrics["lidar_backscatter_or_noise_count"] = float(len(noise_points))
        metrics["lidar_multipath_return_count"] = float(len(multipath_points))
        metrics["lidar_geometry_multipath_applied"] = 1.0 if multipath_points else 0.0
        metrics["lidar_sidelobe_return_count"] = float(len(sidelobe_points))
        metrics["lidar_channel_profile_applied"] = 1.0 if (
            bool(lidar_config.channel_profile.enabled)
            and (
                str(lidar_config.channel_profile.profile_data.pattern).upper().strip() != "NONE"
                or bool(str(lidar_config.channel_profile.profile_data.file_uri).strip())
            )
            and float(lidar_config.channel_profile.profile_data.scale) > 0.0
        ) else 0.0
        metrics["lidar_channel_profile_scale"] = float(lidar_config.channel_profile.profile_data.scale)
        metrics["lidar_channel_profile_file_loaded"] = 1.0 if any(
            source.startswith("FILE_") for source in channel_profile_sources
        ) else 0.0
        metrics["lidar_channel_profile_sidecar_used"] = 1.0 if any(
            "SIDECAR" in source for source in channel_profile_sources
        ) else 0.0
        precipitation_profile = self._lidar_precipitation_profile(
            environment_model=lidar_config.environment_model,
        )
        metrics["lidar_weather_model_applied"] = 1.0 if (
            float(lidar_config.environment_model.fog_density) > 0.0
            or float(precipitation_profile["precipitation_rate"]) > 0.0
        ) else 0.0
        metrics["lidar_precipitation_model_applied"] = 1.0 if (
            float(precipitation_profile["precipitation_rate"]) > 0.0
        ) else 0.0
        metrics["lidar_precipitation_particle_density"] = float(
            precipitation_profile["particle_field_density"]
        )
        metrics["lidar_precipitation_extinction_alpha"] = float(
            precipitation_profile["precipitation_extinction_alpha"]
        )
        metrics["lidar_false_alarm_probability"] = float(
            lidar_config.noise_performance.probability_false_alarm
        )
        metrics["lidar_emitter_model_applied"] = 1.0 if (
            bool(lidar_config.emitter_params.source_losses_db)
            or abs(float(lidar_config.emitter_params.global_source_loss_db)) > 1e-12
            or abs(float(lidar_config.emitter_params.source_divergence.az)) > 1e-12
            or abs(float(lidar_config.emitter_params.source_divergence.el)) > 1e-12
            or abs(float(lidar_config.emitter_params.source_variance.az)) > 1e-12
            or abs(float(lidar_config.emitter_params.source_variance.el)) > 1e-12
            or abs(float(lidar_config.emitter_params.peak_power_w) - 1.0) > 1e-12
            or bool(lidar_config.emitter_params.optical_loss)
        ) else 0.0
        metrics["lidar_multipath_max_extra_path_length_m"] = float(
            lidar_config.multipath_model.max_extra_path_length_m
        )
        metrics["lidar_selected_return_count"] = float(len(preview_points))
        metrics["lidar_range_discrimination_m"] = float(lidar_config.return_model.range_discrimination_m)
        metrics["lidar_range_discrimination_merge_count"] = float(
            sum(max(0, count - 1) for count in merged_counts)
        )
        metrics["lidar_peak_power_w"] = float(lidar_config.emitter_params.peak_power_w)
        metrics["lidar_max_channel_loss_db"] = max(channel_loss_values) if channel_loss_values else 0.0
        metrics["lidar_max_beam_azimuth_offset_deg"] = max(beam_offset_values) if beam_offset_values else 0.0
        if snr_db_values:
            metrics["lidar_min_snr_db"] = float(min(snr_db_values))
            metrics["lidar_max_snr_db"] = float(max(snr_db_values))
        else:
            metrics["lidar_min_snr_db"] = 0.0
            metrics["lidar_max_snr_db"] = 0.0

    def _lidar_offset_point_along_ray(
        self,
        *,
        point_xyz: tuple[float, float, float],
        range_offset_m: float,
    ) -> tuple[float, float, float]:
        x, y, z = point_xyz
        range_m = sqrt(x * x + y * y + z * z)
        if range_m <= 1e-9:
            return point_xyz
        scale = (range_m + range_offset_m) / range_m
        return (x * scale, y * scale, z * scale)

    def _lidar_scan_field_payload(self, lidar_config: Any) -> dict[str, float]:
        return {
            "azimuth_min": float(lidar_config.scan_field_azimuth_min_deg),
            "azimuth_max": float(lidar_config.scan_field_azimuth_max_deg),
            "elevation_min": float(lidar_config.scan_field_elevation_min_deg),
            "elevation_max": float(lidar_config.scan_field_elevation_max_deg),
        }

    def _lidar_scan_field_offset_payload(self, lidar_config: Any) -> dict[str, float]:
        return {
            "azimuth": float(lidar_config.scan_field_azimuth_offset_deg),
            "elevation": float(lidar_config.scan_field_elevation_offset_deg),
        }

    def _normalize_angle_deg(self, angle_deg: float) -> float:
        normalized = (angle_deg + 180.0) % 360.0 - 180.0
        if normalized == -180.0:
            return 180.0
        return normalized

    def _lidar_angle_in_field(
        self,
        *,
        angle_deg: float,
        min_deg: float,
        max_deg: float,
        wrap: bool,
    ) -> bool:
        if not wrap:
            return min_deg <= angle_deg <= max_deg
        if max_deg - min_deg >= 359.999:
            return True
        normalized_angle = self._normalize_angle_deg(angle_deg)
        normalized_min = self._normalize_angle_deg(min_deg)
        normalized_max = self._normalize_angle_deg(max_deg)
        if normalized_min <= normalized_max:
            return normalized_min <= normalized_angle <= normalized_max
        return normalized_angle >= normalized_min or normalized_angle <= normalized_max

    def _apply_lidar_motion_compensation(
        self,
        points_xyz: list[tuple[float, float, float]],
        ego_velocity: tuple[float, float, float],
        scan_duration_s: float,
        mode: str,
    ) -> list[tuple[float, float, float]]:
        if not points_xyz:
            return []
        if scan_duration_s <= 0.0:
            return list(points_xyz)
        if mode.lower().strip() not in {"linear", "pose_delta"}:
            return list(points_xyz)

        vx, vy, vz = ego_velocity
        n_points = len(points_xyz)
        if n_points <= 1:
            return list(points_xyz)

        compensated: list[tuple[float, float, float]] = []
        denom = float(n_points - 1)
        for idx, (x, y, z) in enumerate(points_xyz):
            alpha = float(idx) / denom
            dt = (alpha - 0.5) * scan_duration_s
            compensated.append((x - vx * dt, y - vy * dt, z - vz * dt))
        return compensated

    def _lidar_extrinsics_from_options(self, request: SensorSimRequest) -> CameraExtrinsics:
        return self._sensor_config_from_request(request).lidar.extrinsics.to_camera_extrinsics()

    def _build_lidar_extrinsics_from_pose(
        self,
        request: SensorSimRequest,
        base_extrinsics: CameraExtrinsics,
        pose: TrajectoryPose,
        force_enable: bool,
    ) -> CameraExtrinsics:
        position_mode = str(
            request.options.get("lidar_extrinsics_auto_use_position", "xy")
        ).lower()
        use_orientation = bool(request.options.get("lidar_extrinsics_auto_use_orientation", True))

        tx, ty, tz = base_extrinsics.tx, base_extrinsics.ty, base_extrinsics.tz
        if position_mode in {"xy", "xyz"}:
            tx = pose.x
            ty = pose.y
        if position_mode == "xyz":
            tz = pose.z

        roll_deg = base_extrinsics.roll_deg
        pitch_deg = base_extrinsics.pitch_deg
        yaw_deg = base_extrinsics.yaw_deg
        if use_orientation:
            roll_deg = pose.roll_deg
            pitch_deg = pose.pitch_deg
            yaw_deg = pose.yaw_deg

        offsets = request.options.get("lidar_extrinsics_auto_offsets", {})
        if isinstance(offsets, dict):
            tx += float(offsets.get("tx", 0.0))
            ty += float(offsets.get("ty", 0.0))
            tz += float(offsets.get("tz", 0.0))
            roll_deg += float(offsets.get("roll_deg", 0.0))
            pitch_deg += float(offsets.get("pitch_deg", 0.0))
            yaw_deg += float(offsets.get("yaw_deg", 0.0))

        enabled = bool(base_extrinsics.enabled)
        if force_enable:
            enabled = True
        return CameraExtrinsics(
            tx=tx,
            ty=ty,
            tz=tz,
            roll_deg=roll_deg,
            pitch_deg=pitch_deg,
            yaw_deg=yaw_deg,
            enabled=enabled,
        )

    def _generate_renderer_playback_contract_if_available(
        self,
        request: SensorSimRequest,
        artifacts: dict[str, Path],
        enhanced_output: Path,
        metrics: dict[str, float],
    ) -> Path | None:
        payload = build_renderer_playback_contract(
            options=request.options,
            artifacts=artifacts,
        )
        if payload is None:
            return None

        output_path = enhanced_output / "renderer_playback_contract.json"
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        metrics["renderer_playback_contract_generated"] = 1.0
        metrics["renderer_playback_contract_frame_count"] = float(payload.get("frame_count", 0))
        return output_path

    def _execute_renderer_runtime_if_available(
        self,
        request: SensorSimRequest,
        artifacts: dict[str, Path],
        enhanced_output: Path,
        metrics: dict[str, float],
    ) -> tuple[dict[str, Path], dict[str, float], str, bool]:
        if not bool(request.options.get("renderer_bridge_enabled", False)):
            return {}, {}, "", True

        contract_path = artifacts.get("renderer_playback_contract")
        if contract_path is None or not contract_path.exists():
            return {}, {}, "Renderer runtime skipped: playback contract is missing.", False

        result = execute_renderer_runtime(
            options=request.options,
            contract_path=contract_path,
            output_dir=enhanced_output,
        )
        if result.success:
            return result.artifacts, result.metrics, result.message, True
        return result.artifacts, result.metrics, result.message, False

    def _generate_radar_targets_if_available(
        self,
        request: SensorSimRequest,
        artifacts: dict[str, Path],
        enhanced_output: Path,
        metrics: dict[str, float],
    ) -> Path | None:
        radar_config = self._sensor_config_from_request(request).radar
        if not bool(radar_config.postprocess_enabled):
            return None

        point_cloud = artifacts.get("point_cloud_primary")
        if point_cloud is None or point_cloud.suffix.lower() != ".xyz" or not point_cloud.exists():
            return None

        max_points = int(request.options.get("radar_postprocess_max_points", 50000))
        points_xyz = read_xyz_points(point_cloud, max_points=max_points)
        if not points_xyz:
            metrics["radar_input_count"] = 0.0
            metrics["radar_target_count"] = 0.0
            return None

        base_extrinsics = self._radar_extrinsics_from_options(request)
        behavior_actor_positions = self._sensor_behavior_actor_position_map(
            request=request,
            points_xyz=points_xyz,
        )
        extrinsics, behavior_runtime = self._apply_sensor_behaviors_to_extrinsics(
            request=request,
            sensor_name="radar",
            base_extrinsics=base_extrinsics,
            behaviors=list(radar_config.behaviors),
            actor_positions=behavior_actor_positions,
            points_xyz=points_xyz,
            eval_time_s=float(request.options.get("radar_behavior_time_s", request.options.get("sensor_behavior_time_s", 0.0))),
        )
        points_radar = transform_points_world_to_camera(points_xyz=points_xyz, extrinsics=extrinsics)
        rng = random.Random(int(request.seed) + 137)
        ego_vx, ego_vy, ego_vz = self._estimate_ego_velocity_from_trajectory(request, artifacts)
        selected, false_added, tracks = self._build_radar_targets_from_points(
            request=request,
            radar_config=radar_config,
            points_radar=points_radar,
            ego_velocity=(ego_vx, ego_vy, ego_vz),
            rng=rng,
            false_target_count=int(radar_config.false_target_count),
        )
        multipath_target_count = sum(
            1
            for target in selected
            if str(target.get("measurement_source", "")).upper() == "MULTIPATH"
        )
        multipath_path_type_counts = self._radar_multipath_path_type_counts(selected)
        adaptive_sampling_target_count = sum(
            1
            for target in selected
            if float(target.get("adaptive_sampling_density", 0.0)) > 0.0
        )
        micro_doppler_target_count = sum(
            1
            for target in selected
            if abs(float(target.get("micro_doppler_velocity_offset_mps", 0.0))) > 1e-12
        )
        radar_coverage_threshold = max(
            self._sensor_config_from_request(request).coverage.radar_min_detections_on_target,
            1,
        )
        coverage_summary = self._build_coverage_summary_from_samples(
            samples=selected,
            count_field="radar_detections_on_target",
            actor_field="ground_truth_actor_id",
            semantic_class_field="ground_truth_semantic_class",
            semantic_name_field="ground_truth_semantic_class_name",
            detection_type_field="ground_truth_detection_type",
            count_threshold=radar_coverage_threshold,
            excluded_detection_types={"FALSE_ALARM"},
        )

        preview = {
            "input_point_cloud": str(point_cloud),
            "input_count": len(points_xyz),
            "target_count": len(selected),
            "track_count": len(tracks),
            "multipath_target_count": multipath_target_count,
            "multipath_path_type_counts": multipath_path_type_counts,
            "adaptive_sampling_target_count": adaptive_sampling_target_count,
            "output_mode": "TRACKS" if radar_config.tracking.output_tracks else "POINTS",
            "ego_velocity_mps": {
                "vx": ego_vx,
                "vy": ego_vy,
                "vz": ego_vz,
            },
            "radar_config": radar_config.to_dict(),
            "radar_behavior": behavior_runtime,
            "ground_truth_fields": self._radar_ground_truth_fields(),
            "coverage_metric_name": "radar_detections_on_target",
            "coverage_total_observation_count": coverage_summary[
                "total_observation_count"
            ],
            "coverage_anonymous_observation_count": coverage_summary[
                "anonymous_observation_count"
            ],
            "coverage_excluded_observation_count": coverage_summary[
                "excluded_observation_count"
            ],
            "coverage_targets": coverage_summary["targets"],
            "radar_extrinsics": {
                "enabled": extrinsics.enabled,
                "tx": extrinsics.tx,
                "ty": extrinsics.ty,
                "tz": extrinsics.tz,
                "roll_deg": extrinsics.roll_deg,
                "pitch_deg": extrinsics.pitch_deg,
                "yaw_deg": extrinsics.yaw_deg,
            },
            "targets": selected,
            "tracks": tracks,
        }
        output_path = enhanced_output / "radar_targets_preview.json"
        output_path.write_text(json.dumps(preview, indent=2), encoding="utf-8")
        metrics["radar_input_count"] = float(len(points_xyz))
        metrics["radar_target_count"] = float(len(selected))
        metrics["radar_track_count"] = float(len(tracks))
        metrics["radar_track_output_enabled"] = 1.0 if radar_config.tracking.output_tracks else 0.0
        metrics["radar_false_target_count"] = float(false_added)
        metrics["radar_false_alarm_probability"] = float(radar_config.detector.probability_false_alarm)
        metrics["radar_multipath_enabled"] = 1.0 if radar_config.fidelity.multipath_enabled else 0.0
        metrics["radar_multipath_target_count"] = float(multipath_target_count)
        metrics["radar_multipath_forward_count"] = float(multipath_path_type_counts.get("FORWARD", 0))
        metrics["radar_multipath_reverse_count"] = float(multipath_path_type_counts.get("REVERSE", 0))
        metrics["radar_multipath_retroreflection_count"] = float(
            multipath_path_type_counts.get("RETROREFLECTION", 0)
        )
        metrics["radar_multipath_cavity_count"] = float(
            multipath_path_type_counts.get("CAVITY_RETROREFLECTION", 0)
        )
        metrics["radar_behavior_applied"] = 1.0 if bool(behavior_runtime.get("applied")) else 0.0
        metrics["radar_micro_doppler_enabled"] = 1.0 if radar_config.fidelity.enable_micro_doppler else 0.0
        metrics["radar_micro_doppler_target_count"] = float(micro_doppler_target_count)
        metrics["radar_directivity_model_applied"] = 1.0 if (
            str(radar_config.system.antenna_model.model_type).upper() == "FROM_DIRECTIVITY_AZ_EL_CUTS"
        ) else 0.0
        metrics["radar_adaptive_sampling_enabled"] = 1.0 if (
            float(radar_config.fidelity.raytracing.default_min_rays_per_wavelength) > 0.0
            or bool(radar_config.fidelity.raytracing.adaptive_targets)
        ) else 0.0
        metrics["radar_adaptive_sampling_target_count"] = float(adaptive_sampling_target_count)
        return output_path

    def _generate_radar_targets_trajectory_sweep_if_available(
        self,
        request: SensorSimRequest,
        artifacts: dict[str, Path],
        enhanced_output: Path,
        metrics: dict[str, float],
    ) -> Path | None:
        radar_config = self._sensor_config_from_request(request).radar
        if not bool(radar_config.postprocess_enabled):
            return None
        if not bool(radar_config.trajectory_sweep_enabled):
            return None

        point_cloud = artifacts.get("point_cloud_primary")
        trajectory_path = artifacts.get("trajectory_primary")
        if (
            point_cloud is None
            or point_cloud.suffix.lower() != ".xyz"
            or not point_cloud.exists()
            or trajectory_path is None
            or not trajectory_path.exists()
        ):
            return None

        max_points = int(request.options.get("radar_postprocess_max_points", 50000))
        points_xyz = read_xyz_points(point_cloud, max_points=max_points)
        if not points_xyz:
            metrics["radar_trajectory_sweep_frame_count"] = 0.0
            metrics["radar_trajectory_sweep_total_target_count"] = 0.0
            return None

        poses = read_trajectory_poses(
            trajectory_path,
            max_rows=int(request.options.get("camera_extrinsics_auto_max_rows", 20000)),
        )
        if not poses:
            metrics["radar_trajectory_sweep_frame_count"] = 0.0
            metrics["radar_trajectory_sweep_total_target_count"] = 0.0
            return None

        frame_count = int(request.options.get("radar_trajectory_sweep_frames", 3))
        selected_poses = self._sample_trajectory_poses(poses=poses, frame_count=frame_count)
        false_target_count = int(radar_config.false_target_count)
        preview_targets_per_frame = int(request.options.get("radar_preview_targets_per_frame", 16))
        base_extrinsics = self._radar_extrinsics_from_options(request)
        behavior_actor_positions = self._sensor_behavior_actor_position_map(
            request=request,
            points_xyz=points_xyz,
        )
        behavior_time_origin_s = float(selected_poses[0][1].time_s) if selected_poses else 0.0
        radar_coverage_threshold = max(
            self._sensor_config_from_request(request).coverage.radar_min_detections_on_target,
            1,
        )
        frames: list[dict[str, object]] = []
        total_targets = 0
        total_tracks = 0
        total_multipath_targets = 0
        total_multipath_path_type_counts: dict[str, int] = {}
        total_adaptive_sampling_targets = 0
        persistent_track_states: dict[int, dict[str, object]] = {}
        next_persistent_track_id = 0
        persistent_track_ids: set[int] = set()
        total_track_reassociations = 0
        max_track_history_length = 0
        max_track_age_s = 0.0
        coverage_target_lists: list[list[dict[str, object]]] = []
        coverage_total_observation_count = 0
        coverage_anonymous_observation_count = 0
        coverage_excluded_observation_count = 0
        for frame_id, (pose_index, pose) in enumerate(selected_poses):
            effective_extrinsics = self._build_radar_extrinsics_from_pose(
                request=request,
                base_extrinsics=base_extrinsics,
                pose=pose,
                force_enable=True,
            )
            effective_extrinsics, behavior_runtime = self._apply_sensor_behaviors_to_extrinsics(
                request=request,
                sensor_name="radar",
                base_extrinsics=effective_extrinsics,
                behaviors=list(radar_config.behaviors),
                actor_positions=behavior_actor_positions,
                points_xyz=points_xyz,
                eval_time_s=float(pose.time_s - behavior_time_origin_s),
            )
            points_radar = transform_points_world_to_camera(
                points_xyz=points_xyz,
                extrinsics=effective_extrinsics,
            )
            ego_velocity = self._estimate_ego_velocity_for_pose_index(poses=poses, pose_index=pose_index)
            rng = random.Random(int(request.seed) + 537 + frame_id)
            targets, false_added, tracks = self._build_radar_targets_from_points(
                request=request,
                radar_config=radar_config,
                points_radar=points_radar,
                ego_velocity=ego_velocity,
                rng=rng,
                false_target_count=false_target_count,
            )
            tracks, persistent_track_states, next_persistent_track_id, frame_track_reassociations = (
                self._annotate_radar_tracks_with_history(
                    radar_config=radar_config,
                    tracks=tracks,
                    previous_track_states=persistent_track_states,
                    next_persistent_track_id=next_persistent_track_id,
                    frame_id=frame_id,
                    frame_time_s=float(pose.time_s - behavior_time_origin_s),
                )
            )
            total_targets += len(targets)
            total_tracks += len(tracks)
            total_track_reassociations += frame_track_reassociations
            persistent_track_ids.update(
                int(track["persistent_track_id"])
                for track in tracks
                if track.get("persistent_track_id") is not None
            )
            for track in tracks:
                max_track_history_length = max(
                    max_track_history_length,
                    int(track.get("track_history_length", 0)),
                )
                max_track_age_s = max(
                    max_track_age_s,
                    float(track.get("track_age_s", 0.0)),
                )
            frame_multipath_target_count = sum(
                1
                for target in targets
                if str(target.get("measurement_source", "")).upper() == "MULTIPATH"
            )
            frame_multipath_path_type_counts = self._radar_multipath_path_type_counts(targets)
            frame_adaptive_sampling_target_count = sum(
                1
                for target in targets
                if float(target.get("adaptive_sampling_density", 0.0)) > 0.0
            )
            frame_coverage_summary = self._build_coverage_summary_from_samples(
                samples=targets,
                count_field="radar_detections_on_target",
                actor_field="ground_truth_actor_id",
                semantic_class_field="ground_truth_semantic_class",
                semantic_name_field="ground_truth_semantic_class_name",
                detection_type_field="ground_truth_detection_type",
                count_threshold=radar_coverage_threshold,
                excluded_detection_types={"FALSE_ALARM"},
            )
            total_multipath_targets += frame_multipath_target_count
            for path_type, count in frame_multipath_path_type_counts.items():
                total_multipath_path_type_counts[path_type] = (
                    total_multipath_path_type_counts.get(path_type, 0) + count
                )
            total_adaptive_sampling_targets += frame_adaptive_sampling_target_count
            coverage_target_lists.append(list(frame_coverage_summary["targets"]))
            coverage_total_observation_count += int(
                frame_coverage_summary["total_observation_count"]
            )
            coverage_anonymous_observation_count += int(
                frame_coverage_summary["anonymous_observation_count"]
            )
            coverage_excluded_observation_count += int(
                frame_coverage_summary["excluded_observation_count"]
            )
            frames.append(
                {
                    "frame_id": frame_id,
                    "pose_index": pose_index,
                    "trajectory_pose": self._trajectory_pose_payload(
                        pose=pose,
                        trajectory_path=trajectory_path,
                    ),
                    "ego_velocity_mps": {
                        "vx": ego_velocity[0],
                        "vy": ego_velocity[1],
                        "vz": ego_velocity[2],
                    },
                    "radar_extrinsics": {
                        "enabled": effective_extrinsics.enabled,
                        "tx": effective_extrinsics.tx,
                        "ty": effective_extrinsics.ty,
                        "tz": effective_extrinsics.tz,
                        "roll_deg": effective_extrinsics.roll_deg,
                        "pitch_deg": effective_extrinsics.pitch_deg,
                        "yaw_deg": effective_extrinsics.yaw_deg,
                    },
                    "target_count": len(targets),
                    "track_count": len(tracks),
                    "radar_behavior": behavior_runtime,
                    "multipath_target_count": frame_multipath_target_count,
                    "multipath_path_type_counts": frame_multipath_path_type_counts,
                    "adaptive_sampling_target_count": frame_adaptive_sampling_target_count,
                    "persistent_track_count": len(
                        {
                            int(track["persistent_track_id"])
                            for track in tracks
                            if track.get("persistent_track_id") is not None
                        }
                    ),
                    "track_reassociation_count": frame_track_reassociations,
                    "false_target_count": false_added,
                    "output_mode": "TRACKS" if radar_config.tracking.output_tracks else "POINTS",
                    "ground_truth_fields": self._radar_ground_truth_fields(),
                    "coverage_metric_name": "radar_detections_on_target",
                    "coverage_total_observation_count": frame_coverage_summary[
                        "total_observation_count"
                    ],
                    "coverage_anonymous_observation_count": frame_coverage_summary[
                        "anonymous_observation_count"
                    ],
                    "coverage_excluded_observation_count": frame_coverage_summary[
                        "excluded_observation_count"
                    ],
                    "coverage_targets": frame_coverage_summary["targets"],
                    "targets_preview": targets[:preview_targets_per_frame],
                    "tracks_preview": tracks[:preview_targets_per_frame],
                }
            )

        metrics["radar_trajectory_sweep_frame_count"] = float(len(frames))
        metrics["radar_trajectory_sweep_total_target_count"] = float(total_targets)
        metrics["radar_trajectory_sweep_total_track_count"] = float(total_tracks)
        metrics["radar_trajectory_sweep_total_multipath_target_count"] = float(
            total_multipath_targets
        )
        metrics["radar_trajectory_sweep_persistent_track_count"] = float(len(persistent_track_ids))
        metrics["radar_trajectory_sweep_track_reassociation_count"] = float(
            total_track_reassociations
        )
        metrics["radar_trajectory_sweep_max_track_history_length"] = float(max_track_history_length)
        metrics["radar_trajectory_sweep_max_track_age_s"] = float(max_track_age_s)
        metrics["radar_trajectory_sweep_total_multipath_forward_count"] = float(
            total_multipath_path_type_counts.get("FORWARD", 0)
        )
        metrics["radar_trajectory_sweep_total_multipath_reverse_count"] = float(
            total_multipath_path_type_counts.get("REVERSE", 0)
        )
        metrics["radar_trajectory_sweep_total_multipath_retroreflection_count"] = float(
            total_multipath_path_type_counts.get("RETROREFLECTION", 0)
        )
        metrics["radar_trajectory_sweep_total_multipath_cavity_count"] = float(
            total_multipath_path_type_counts.get("CAVITY_RETROREFLECTION", 0)
        )
        metrics["radar_trajectory_sweep_total_adaptive_sampling_target_count"] = float(
            total_adaptive_sampling_targets
        )
        metrics["radar_behavior_applied"] = 1.0 if any(
            bool(frame.get("radar_behavior", {}).get("applied"))
            for frame in frames
        ) else 0.0
        payload = {
            "input_point_cloud": str(point_cloud),
            "trajectory_path": str(trajectory_path),
            "input_count": len(points_xyz),
            "frame_count": len(frames),
            "output_mode": "TRACKS" if radar_config.tracking.output_tracks else "POINTS",
            "radar_config": radar_config.to_dict(),
            "multipath_path_type_counts": total_multipath_path_type_counts,
            "persistent_track_count": len(persistent_track_ids),
            "track_reassociation_count": total_track_reassociations,
            "max_track_history_length": max_track_history_length,
            "max_track_age_s": max_track_age_s,
            "ground_truth_fields": self._radar_ground_truth_fields(),
            "coverage_metric_name": "radar_detections_on_target",
            "coverage_total_observation_count": coverage_total_observation_count,
            "coverage_anonymous_observation_count": coverage_anonymous_observation_count,
            "coverage_excluded_observation_count": coverage_excluded_observation_count,
            "coverage_targets": self._merge_sensor_coverage_targets(
                target_lists=coverage_target_lists,
                count_field="radar_detections_on_target",
                count_threshold=radar_coverage_threshold,
            ),
            "frames": frames,
        }
        output_path = enhanced_output / "radar_targets_trajectory_sweep.json"
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return output_path

    def _radar_multipath_path_type_counts(
        self,
        targets: list[dict[str, object]],
    ) -> dict[str, int]:
        counts: dict[str, int] = {}
        for target in targets:
            if str(target.get("measurement_source", "")).upper() != "MULTIPATH":
                continue
            path_type = str(target.get("multipath_path_type", "")).upper().strip()
            if not path_type:
                continue
            counts[path_type] = counts.get(path_type, 0) + 1
        return counts

    def _build_radar_targets_from_points(
        self,
        request: SensorSimRequest,
        radar_config: Any,
        points_radar: list[tuple[float, float, float]],
        ego_velocity: tuple[float, float, float],
        rng: random.Random,
        false_target_count: int,
    ) -> tuple[list[dict[str, object]], int, list[dict[str, object]]]:
        clutter_model = str(radar_config.clutter_model).lower().strip()
        max_targets = int(radar_config.detector.max_detections) if int(radar_config.detector.max_detections) > 0 else int(radar_config.max_targets)
        min_range = max(
            float(radar_config.range_min_m),
            max(float(radar_config.fidelity.near_clipping_distance_m), 0.0),
        )
        max_range = float(radar_config.range_max_m)
        horiz_fov_rad = float(radar_config.horizontal_fov_deg) * pi / 180.0
        vert_fov_rad = float(radar_config.vertical_fov_deg) * pi / 180.0
        angle_noise_deg = float(radar_config.angle_noise_stddev_deg) if clutter_model == "basic" else 0.0
        range_noise_m = float(radar_config.range_noise_stddev_m) if clutter_model == "basic" else 0.0
        velocity_noise_mps = float(radar_config.velocity_noise_stddev_mps) if clutter_model == "basic" else 0.0
        rcs_defaults = request.options.get("radar_point_rcs_dbsm")
        rcs_base_dbsm = float(request.options.get("radar_rcs_base_dbsm", 12.0))
        ego_vx, ego_vy, ego_vz = ego_velocity

        candidates: list[tuple[float, float, dict[str, object]]] = []
        for point_index, (x, y, z) in enumerate(points_radar):
            range_m = sqrt(x * x + y * y + z * z)
            if range_m < min_range or range_m > max_range:
                continue

            xy_norm = sqrt(x * x + y * y)
            azimuth_rad = atan2(y, x)
            elevation_rad = atan2(z, xy_norm if xy_norm > 1e-9 else 1e-9)
            if abs(azimuth_rad) > horiz_fov_rad * 0.5:
                continue
            if abs(elevation_rad) > vert_fov_rad * 0.5:
                continue

            azimuth_deg = azimuth_rad * 180.0 / pi
            elevation_deg = elevation_rad * 180.0 / pi
            intrinsic_rcs_dbsm = rcs_base_dbsm
            if isinstance(rcs_defaults, list) and 0 <= point_index < len(rcs_defaults):
                intrinsic_rcs_dbsm = float(rcs_defaults[point_index])
            sampling_profile = self._radar_adaptive_sampling_profile(
                request=request,
                radar_config=radar_config,
                point_index=point_index,
            )
            detection_metrics = self._radar_detection_metrics(
                radar_config=radar_config,
                range_m=range_m,
                azimuth_deg=azimuth_deg,
                elevation_deg=elevation_deg,
                intrinsic_rcs_dbsm=intrinsic_rcs_dbsm,
                sampling_density=float(sampling_profile["density"]),
            )
            if not bool(detection_metrics["detected"]):
                continue

            radial_velocity = -(ego_vx * x + ego_vy * y + ego_vz * z) / max(range_m, 1e-9)
            micro_doppler_offset_mps = self._radar_micro_doppler_offset_mps(
                radar_config=radar_config,
                point_index=point_index,
                range_m=range_m,
                azimuth_deg=azimuth_deg,
                bounce_index=0,
            )
            ground_truth_annotation = self._sensor_ground_truth_annotation(
                request=request,
                sensor_prefix="radar",
                point_index=point_index,
                world_point=(x, y, z),
            )
            range_sigma_m, range_region_index = self._radar_accuracy_sigma(
                accuracy_config=radar_config.estimator.range_accuracy,
                regions=radar_config.estimator.range_accuracy_regions,
                range_m=range_m,
                azimuth_deg=azimuth_deg,
                elevation_deg=elevation_deg,
            )
            velocity_sigma_mps, velocity_region_index = self._radar_accuracy_sigma(
                accuracy_config=radar_config.estimator.velocity_accuracy,
                regions=radar_config.estimator.velocity_accuracy_regions,
                range_m=range_m,
                azimuth_deg=azimuth_deg,
                elevation_deg=elevation_deg,
            )
            azimuth_sigma_deg, azimuth_region_index = self._radar_accuracy_sigma(
                accuracy_config=radar_config.estimator.azimuth_accuracy,
                regions=radar_config.estimator.azimuth_accuracy_regions,
                range_m=range_m,
                azimuth_deg=azimuth_deg,
                elevation_deg=elevation_deg,
            )
            elevation_sigma_deg, elevation_region_index = self._radar_accuracy_sigma(
                accuracy_config=radar_config.estimator.elevation_accuracy,
                regions=radar_config.estimator.elevation_accuracy_regions,
                range_m=range_m,
                azimuth_deg=azimuth_deg,
                elevation_deg=elevation_deg,
            )
            apply_additive_noise = not bool(radar_config.detector.no_additive_noise)
            total_range_sigma = sqrt(range_sigma_m * range_sigma_m + (range_noise_m if apply_additive_noise else 0.0) ** 2)
            total_velocity_sigma = sqrt(velocity_sigma_mps * velocity_sigma_mps + (velocity_noise_mps if apply_additive_noise else 0.0) ** 2)
            total_azimuth_sigma = sqrt(azimuth_sigma_deg * azimuth_sigma_deg + (angle_noise_deg if apply_additive_noise else 0.0) ** 2)
            total_elevation_sigma = sqrt(elevation_sigma_deg * elevation_sigma_deg + (angle_noise_deg if apply_additive_noise else 0.0) ** 2)

            noisy_range = max(min_range, range_m + (rng.gauss(0.0, total_range_sigma) if total_range_sigma > 0.0 else 0.0))
            noisy_azimuth_deg = azimuth_deg + (rng.gauss(0.0, total_azimuth_sigma) if total_azimuth_sigma > 0.0 else 0.0)
            noisy_elevation_deg = elevation_deg + (rng.gauss(0.0, total_elevation_sigma) if total_elevation_sigma > 0.0 else 0.0)
            noisy_radial_velocity = (
                radial_velocity
                + micro_doppler_offset_mps
                + (rng.gauss(0.0, total_velocity_sigma) if total_velocity_sigma > 0.0 else 0.0)
            )
            noisy_radial_velocity = min(
                max(noisy_radial_velocity, float(radar_config.system.velocity_min_mps)),
                float(radar_config.system.velocity_max_mps),
            )

            noisy_range = self._radar_quantize_value(
                value=noisy_range,
                quantization_step=radar_config.system.range_quantization_m,
            )
            noisy_radial_velocity = self._radar_quantize_value(
                value=noisy_radial_velocity,
                quantization_step=radar_config.system.velocity_quantization_mps,
            )
            noisy_azimuth_deg = self._radar_quantize_value(
                value=noisy_azimuth_deg,
                quantization_step=radar_config.system.angular_quantization.az_deg,
            )
            noisy_elevation_deg = self._radar_quantize_value(
                value=noisy_elevation_deg,
                quantization_step=radar_config.system.angular_quantization.el_deg,
            )
            candidates.append(
                (
                    -float(detection_metrics["signal_power_dbw"]),
                    noisy_range,
                    {
                        "range_m": noisy_range,
                        "azimuth_deg": noisy_azimuth_deg,
                        "elevation_deg": noisy_elevation_deg,
                        "radial_velocity_mps": noisy_radial_velocity,
                        "rcs_dbsm": intrinsic_rcs_dbsm,
                        "signal_power_dbw": float(detection_metrics["signal_power_dbw"]),
                        "noise_power_dbw": float(detection_metrics["noise_power_dbw"]),
                        "snr_db": float(detection_metrics["snr_db"]),
                        "detection_probability": float(detection_metrics["detection_probability"]),
                        "antenna_gain_db": float(detection_metrics["antenna_gain_db"]),
                        "sampling_gain_db": float(detection_metrics["sampling_gain_db"]),
                        "range_resolution_m": float(radar_config.system.range_resolution_m),
                        "velocity_resolution_mps": float(radar_config.system.velocity_resolution_mps),
                        "angular_resolution_deg": {
                            "az": float(radar_config.system.angular_resolution.az_deg),
                            "el": float(radar_config.system.angular_resolution.el_deg),
                        },
                        "range_accuracy_region_index": range_region_index,
                        "velocity_accuracy_region_index": velocity_region_index,
                        "azimuth_accuracy_region_index": azimuth_region_index,
                        "elevation_accuracy_region_index": elevation_region_index,
                        "measurement_source": "DETECTION",
                        "ground_truth_detection_type": "TARGET",
                        "ground_truth_hit_index": 0,
                        "ground_truth_last_bounce_index": 0,
                        "path_length_offset_m": 0.0,
                        "micro_doppler_velocity_offset_mps": micro_doppler_offset_mps,
                        "adaptive_sampling_density": float(sampling_profile["density"]),
                        "adaptive_sampling_actor_id": sampling_profile["actor_id"],
                        "adaptive_sampling_target_override": bool(sampling_profile["target_override"]),
                        "raytracing_subdivision_level": int(sampling_profile["subdivision_level"]),
                        "raytracing_mode": str(radar_config.fidelity.raytracing.mode).upper(),
                        "is_false_alarm": False,
                        **ground_truth_annotation,
                    },
                )
            )
            candidates.extend(
                self._radar_multipath_candidates(
                    request=request,
                    radar_config=radar_config,
                    point_index=point_index,
                    base_range_m=range_m,
                    base_azimuth_deg=azimuth_deg,
                    base_elevation_deg=elevation_deg,
                    base_radial_velocity_mps=radial_velocity,
                    intrinsic_rcs_dbsm=intrinsic_rcs_dbsm,
                    base_detection_metrics=detection_metrics,
                    sampling_profile=sampling_profile,
                    ground_truth_annotation=ground_truth_annotation,
                    rng=rng,
                    min_range=min_range,
                    max_range=max_range,
                )
            )

        candidates.sort(key=lambda item: (item[0], item[1]))
        selected = [item[2] for item in candidates[:max_targets]]

        false_added = 0
        pfa = min(max(float(radar_config.detector.probability_false_alarm), 0.0), 1.0)
        expected_pfa_false_count = pfa * max(len(candidates), 1) * max(max_targets, 1) * 4.0
        pfa_false_count = int(expected_pfa_false_count)
        if rng.random() < (expected_pfa_false_count - pfa_false_count):
            pfa_false_count += 1
        total_false_target_count = max(0, int(false_target_count)) + pfa_false_count
        for _ in range(total_false_target_count):
            if len(selected) >= max_targets:
                break
            false_range = rng.uniform(min_range, max_range)
            false_azimuth = rng.uniform(-0.5 * horiz_fov_rad, 0.5 * horiz_fov_rad)
            false_elevation = rng.uniform(-0.5 * vert_fov_rad, 0.5 * vert_fov_rad)
            false_signal_power_dbw = float(radar_config.detector.noise_variance_dbw) + rng.uniform(
                0.0,
                max(3.0, abs(float(radar_config.detector.minimum_snr_db))),
            )
            selected.append(
                {
                    "range_m": false_range,
                    "azimuth_deg": false_azimuth * 180.0 / pi,
                    "elevation_deg": false_elevation * 180.0 / pi,
                    "radial_velocity_mps": rng.gauss(0.0, 0.2),
                    "rcs_dbsm": rcs_base_dbsm + rng.gauss(0.0, 2.0),
                    "signal_power_dbw": false_signal_power_dbw,
                    "noise_power_dbw": float(radar_config.detector.noise_variance_dbw),
                    "snr_db": false_signal_power_dbw - float(radar_config.detector.noise_variance_dbw),
                    "detection_probability": pfa,
                    "antenna_gain_db": 0.0,
                    "sampling_gain_db": 0.0,
                    "range_resolution_m": float(radar_config.system.range_resolution_m),
                    "velocity_resolution_mps": float(radar_config.system.velocity_resolution_mps),
                    "angular_resolution_deg": {
                        "az": float(radar_config.system.angular_resolution.az_deg),
                        "el": float(radar_config.system.angular_resolution.el_deg),
                    },
                    "range_accuracy_region_index": None,
                    "velocity_accuracy_region_index": None,
                    "azimuth_accuracy_region_index": None,
                    "elevation_accuracy_region_index": None,
                    "measurement_source": "FALSE_ALARM",
                    "ground_truth_detection_type": "FALSE_ALARM",
                    "ground_truth_hit_index": None,
                    "ground_truth_last_bounce_index": None,
                    "path_length_offset_m": 0.0,
                    "micro_doppler_velocity_offset_mps": 0.0,
                    "adaptive_sampling_density": 0.0,
                    "adaptive_sampling_actor_id": None,
                    "adaptive_sampling_target_override": False,
                    "raytracing_subdivision_level": 0,
                    "raytracing_mode": str(radar_config.fidelity.raytracing.mode).upper(),
                    "is_false_alarm": True,
                    "ground_truth_semantic_class": 0,
                    "ground_truth_semantic_class_name": "NONE",
                    "ground_truth_semantic_parent_class": "NONE",
                    "ground_truth_actor_id": None,
                }
            )
            false_added += 1

        for idx, target in enumerate(selected):
            target["id"] = idx
        tracks = self._build_radar_tracks(radar_config=radar_config, detections=selected)
        return selected, false_added, tracks

    def _radar_multipath_candidates(
        self,
        *,
        request: SensorSimRequest,
        radar_config: Any,
        point_index: int,
        base_range_m: float,
        base_azimuth_deg: float,
        base_elevation_deg: float,
        base_radial_velocity_mps: float,
        intrinsic_rcs_dbsm: float,
        base_detection_metrics: dict[str, float | bool],
        sampling_profile: dict[str, object],
        ground_truth_annotation: dict[str, object],
        rng: random.Random,
        min_range: float,
        max_range: float,
    ) -> list[tuple[float, float, dict[str, object]]]:
        if not bool(radar_config.fidelity.multipath_enabled):
            return []

        candidates: list[tuple[float, float, dict[str, object]]] = []
        max_bounces = min(max(int(radar_config.fidelity.multipath_bounces), 1), 6)
        coherence_factor = min(max(float(radar_config.fidelity.coherence_factor), 0.0), 1.0)
        base_signal_power_dbw = float(base_detection_metrics["signal_power_dbw"])
        noise_power_dbw = float(base_detection_metrics["noise_power_dbw"])
        base_detection_probability = float(base_detection_metrics["detection_probability"])
        base_antenna_gain_db = float(base_detection_metrics["antenna_gain_db"])
        cavity_model_enabled = bool(radar_config.fidelity.raytracing.enable_cavity_model)
        angular_step_deg = max(
            float(radar_config.system.angular_resolution.az_deg),
            float(radar_config.fidelity.sub_ray_angular_resolution_deg),
            0.25,
        )
        base_target_point = self._radar_cartesian_from_spherical(
            range_m=base_range_m,
            azimuth_deg=base_azimuth_deg,
            elevation_deg=base_elevation_deg,
        )
        for bounce_index in range(1, max_bounces + 1):
            surface = "GROUND_PLANE" if bounce_index % 2 == 1 else "VERTICAL_PLANE"
            surface_sign = 1.0 if surface == "GROUND_PLANE" else -1.0
            base_path_length_offset_m = max(
                float(radar_config.system.range_resolution_m) * (1.0 + 0.75 * bounce_index),
                base_range_m * (0.05 + 0.03 * bounce_index),
            )
            reflection_point = self._radar_multipath_reflection_point(
                surface=surface,
                surface_sign=surface_sign,
                bounce_index=bounce_index,
                target_point=base_target_point,
            )
            if reflection_point is None:
                continue
            base_path_azimuth_deg = base_azimuth_deg + surface_sign * angular_step_deg * bounce_index
            base_path_elevation_deg = base_elevation_deg + (
                -0.35 * angular_step_deg * bounce_index
                if surface == "GROUND_PLANE"
                else 0.2 * angular_step_deg * bounce_index
            )
            path_specs: list[dict[str, object]] = [
                {
                    "path_type": "FORWARD",
                    "path_length_scale": 1.0,
                    "azimuth_scale": 1.0,
                    "elevation_scale": 1.0,
                    "azimuth_bias_deg": 0.0,
                    "elevation_bias_deg": 0.0,
                    "signal_decay_extra_db": 0.0,
                    "detection_scale": 1.0,
                    "micro_doppler_scale": 1.0,
                    "velocity_noise_scale": 1.0,
                    "hit_index": bounce_index,
                    "last_bounce_index": bounce_index,
                    "last_bounce_point": base_target_point,
                    "return_direction": self._radar_unit_vector_to_origin(base_target_point),
                    "cavity_internal_bounces": 0,
                },
                {
                    "path_type": "REVERSE",
                    "path_length_scale": 1.12,
                    "azimuth_scale": -0.85,
                    "elevation_scale": -0.75,
                    "azimuth_bias_deg": -0.25 * surface_sign * angular_step_deg,
                    "elevation_bias_deg": 0.15 * angular_step_deg,
                    "signal_decay_extra_db": 1.5,
                    "detection_scale": 0.78,
                    "micro_doppler_scale": -0.35,
                    "velocity_noise_scale": 0.8,
                    "hit_index": 0,
                    "last_bounce_index": bounce_index,
                    "last_bounce_point": reflection_point,
                    "return_direction": self._radar_unit_vector_to_origin(reflection_point),
                    "cavity_internal_bounces": 0,
                },
                {
                    "path_type": "RETROREFLECTION",
                    "path_length_scale": 1.34,
                    "azimuth_scale": 0.55,
                    "elevation_scale": 0.55,
                    "azimuth_bias_deg": 0.15 * surface_sign * angular_step_deg,
                    "elevation_bias_deg": -0.12 * angular_step_deg,
                    "signal_decay_extra_db": 2.6,
                    "detection_scale": 0.72,
                    "micro_doppler_scale": 0.65,
                    "velocity_noise_scale": 1.15,
                    "hit_index": bounce_index,
                    "last_bounce_index": 2 * bounce_index,
                    "last_bounce_point": reflection_point,
                    "return_direction": self._radar_unit_vector_to_origin(reflection_point),
                    "cavity_internal_bounces": 0,
                },
            ]
            if cavity_model_enabled:
                cavity_internal_bounces = min(max(bounce_index, 1), 2)
                cavity_exit_point = (
                    0.5 * (reflection_point[0] + base_target_point[0]),
                    0.5 * (reflection_point[1] + base_target_point[1]),
                    max(reflection_point[2], base_target_point[2]) + 0.08 * (bounce_index + 1),
                )
                path_specs.append(
                    {
                        "path_type": "CAVITY_RETROREFLECTION",
                        "path_length_scale": 1.22,
                        "azimuth_scale": 0.35,
                        "elevation_scale": 0.35,
                        "azimuth_bias_deg": 0.0,
                        "elevation_bias_deg": 0.1 * angular_step_deg,
                        "signal_decay_extra_db": 1.9,
                        "detection_scale": 0.84,
                        "micro_doppler_scale": 0.45,
                        "velocity_noise_scale": 0.95,
                        "hit_index": bounce_index,
                        "last_bounce_index": max(
                            bounce_index,
                            2 * bounce_index - cavity_internal_bounces + 1,
                        ),
                        "last_bounce_point": cavity_exit_point,
                        "return_direction": self._radar_unit_vector_to_origin(cavity_exit_point),
                        "cavity_internal_bounces": cavity_internal_bounces,
                    }
                )

            for path_spec in path_specs:
                path_length_offset_m = (
                    base_path_length_offset_m * float(path_spec["path_length_scale"])
                )
                path_range_m = base_range_m + path_length_offset_m
                if path_range_m < min_range or path_range_m > max_range:
                    continue

                path_azimuth_deg = (
                    base_azimuth_deg
                    + base_path_azimuth_deg * float(path_spec["azimuth_scale"])
                    - base_azimuth_deg * float(path_spec["azimuth_scale"])
                    + float(path_spec["azimuth_bias_deg"])
                )
                path_elevation_deg = (
                    base_elevation_deg
                    + (base_path_elevation_deg - base_elevation_deg) * float(path_spec["elevation_scale"])
                    + float(path_spec["elevation_bias_deg"])
                )
                antenna_gain_db = self._radar_antenna_gain_db(
                    radar_config=radar_config,
                    azimuth_deg=path_azimuth_deg,
                    elevation_deg=path_elevation_deg,
                )
                propagation_decay_db = (
                    6.0 * bounce_index
                    + 8.0 * coherence_factor
                    + float(path_spec["signal_decay_extra_db"])
                )
                signal_power_dbw = (
                    base_signal_power_dbw
                    - propagation_decay_db
                    + min(0.0, antenna_gain_db - base_antenna_gain_db)
                )
                snr_db = signal_power_dbw - noise_power_dbw
                if snr_db < float(radar_config.detector.minimum_snr_db):
                    continue

                detection_probability = min(
                    max(
                        base_detection_probability
                        * exp(-0.55 * bounce_index)
                        * max(0.15, 1.0 - 0.6 * coherence_factor)
                        * float(path_spec["detection_scale"]),
                        0.0,
                    ),
                    1.0,
                )
                if detection_probability < 0.1:
                    continue

                micro_doppler_offset_mps = (
                    self._radar_micro_doppler_offset_mps(
                        radar_config=radar_config,
                        point_index=point_index,
                        range_m=path_range_m,
                        azimuth_deg=path_azimuth_deg,
                        bounce_index=bounce_index,
                    )
                    * float(path_spec["micro_doppler_scale"])
                )
                radial_velocity_mps = (
                    base_radial_velocity_mps
                    + micro_doppler_offset_mps
                    + rng.gauss(0.0, 0.03 * bounce_index * float(path_spec["velocity_noise_scale"]))
                )
                radial_velocity_mps = min(
                    max(radial_velocity_mps, float(radar_config.system.velocity_min_mps)),
                    float(radar_config.system.velocity_max_mps),
                )

                range_accuracy_sigma, range_region_index = self._radar_accuracy_sigma(
                    accuracy_config=radar_config.estimator.range_accuracy,
                    regions=radar_config.estimator.range_accuracy_regions,
                    range_m=path_range_m,
                    azimuth_deg=path_azimuth_deg,
                    elevation_deg=path_elevation_deg,
                )
                velocity_accuracy_sigma, velocity_region_index = self._radar_accuracy_sigma(
                    accuracy_config=radar_config.estimator.velocity_accuracy,
                    regions=radar_config.estimator.velocity_accuracy_regions,
                    range_m=path_range_m,
                    azimuth_deg=path_azimuth_deg,
                    elevation_deg=path_elevation_deg,
                )
                azimuth_accuracy_sigma, azimuth_region_index = self._radar_accuracy_sigma(
                    accuracy_config=radar_config.estimator.azimuth_accuracy,
                    regions=radar_config.estimator.azimuth_accuracy_regions,
                    range_m=path_range_m,
                    azimuth_deg=path_azimuth_deg,
                    elevation_deg=path_elevation_deg,
                )
                elevation_accuracy_sigma, elevation_region_index = self._radar_accuracy_sigma(
                    accuracy_config=radar_config.estimator.elevation_accuracy,
                    regions=radar_config.estimator.elevation_accuracy_regions,
                    range_m=path_range_m,
                    azimuth_deg=path_azimuth_deg,
                    elevation_deg=path_elevation_deg,
                )

                path_range_m = max(
                    min_range,
                    path_range_m
                    + (rng.gauss(0.0, range_accuracy_sigma) if range_accuracy_sigma > 0.0 else 0.0),
                )
                path_azimuth_deg += (
                    rng.gauss(0.0, azimuth_accuracy_sigma) if azimuth_accuracy_sigma > 0.0 else 0.0
                )
                path_elevation_deg += (
                    rng.gauss(0.0, elevation_accuracy_sigma) if elevation_accuracy_sigma > 0.0 else 0.0
                )
                radial_velocity_mps += (
                    rng.gauss(0.0, velocity_accuracy_sigma) if velocity_accuracy_sigma > 0.0 else 0.0
                )

                path_range_m = self._radar_quantize_value(
                    value=path_range_m,
                    quantization_step=radar_config.system.range_quantization_m,
                )
                path_azimuth_deg = self._radar_quantize_value(
                    value=path_azimuth_deg,
                    quantization_step=radar_config.system.angular_quantization.az_deg,
                )
                path_elevation_deg = self._radar_quantize_value(
                    value=path_elevation_deg,
                    quantization_step=radar_config.system.angular_quantization.el_deg,
                )
                radial_velocity_mps = self._radar_quantize_value(
                    value=radial_velocity_mps,
                    quantization_step=radar_config.system.velocity_quantization_mps,
                )
                last_bounce_point = tuple(path_spec["last_bounce_point"])
                return_direction = tuple(path_spec["return_direction"])
                candidates.append(
                    (
                        -signal_power_dbw,
                        path_range_m,
                        {
                            "range_m": path_range_m,
                            "azimuth_deg": path_azimuth_deg,
                            "elevation_deg": path_elevation_deg,
                            "radial_velocity_mps": radial_velocity_mps,
                            "rcs_dbsm": intrinsic_rcs_dbsm - 3.0 * bounce_index - 4.0 * coherence_factor,
                            "signal_power_dbw": signal_power_dbw,
                            "noise_power_dbw": noise_power_dbw,
                            "snr_db": snr_db,
                            "detection_probability": detection_probability,
                            "antenna_gain_db": antenna_gain_db,
                            "sampling_gain_db": float(base_detection_metrics.get("sampling_gain_db", 0.0)),
                            "range_resolution_m": float(radar_config.system.range_resolution_m),
                            "velocity_resolution_mps": float(radar_config.system.velocity_resolution_mps),
                            "angular_resolution_deg": {
                                "az": float(radar_config.system.angular_resolution.az_deg),
                                "el": float(radar_config.system.angular_resolution.el_deg),
                            },
                            "range_accuracy_region_index": range_region_index,
                            "velocity_accuracy_region_index": velocity_region_index,
                            "azimuth_accuracy_region_index": azimuth_region_index,
                            "elevation_accuracy_region_index": elevation_region_index,
                            "measurement_source": "MULTIPATH",
                            "ground_truth_detection_type": "MULTIPATH",
                            "ground_truth_hit_index": int(path_spec["hit_index"]),
                            "ground_truth_last_bounce_index": int(path_spec["last_bounce_index"]),
                            "path_length_offset_m": path_length_offset_m,
                            "multipath_path_length_m": base_range_m + path_length_offset_m,
                            "multipath_base_range_m": base_range_m,
                            "multipath_surface": surface,
                            "multipath_path_type": str(path_spec["path_type"]),
                            "multipath_bounce_count": bounce_index,
                            "multipath_reflection_point": {
                                "x": reflection_point[0],
                                "y": reflection_point[1],
                                "z": reflection_point[2],
                            },
                            "multipath_target_scatter_point": {
                                "x": base_target_point[0],
                                "y": base_target_point[1],
                                "z": base_target_point[2],
                            },
                            "multipath_last_bounce_point": {
                                "x": last_bounce_point[0],
                                "y": last_bounce_point[1],
                                "z": last_bounce_point[2],
                            },
                            "multipath_return_direction": {
                                "x": return_direction[0],
                                "y": return_direction[1],
                                "z": return_direction[2],
                            },
                            "multipath_cavity_internal_bounce_count": int(
                                path_spec["cavity_internal_bounces"]
                            ),
                            "coherence_factor": coherence_factor,
                            "micro_doppler_velocity_offset_mps": micro_doppler_offset_mps,
                            "adaptive_sampling_density": float(sampling_profile["density"]),
                            "adaptive_sampling_actor_id": sampling_profile["actor_id"],
                            "adaptive_sampling_target_override": bool(sampling_profile["target_override"]),
                            "raytracing_subdivision_level": int(sampling_profile["subdivision_level"]),
                            "raytracing_mode": str(radar_config.fidelity.raytracing.mode).upper(),
                            "is_false_alarm": False,
                            **ground_truth_annotation,
                        },
                    )
                )
        return candidates

    def _radar_cartesian_from_spherical(
        self,
        *,
        range_m: float,
        azimuth_deg: float,
        elevation_deg: float,
    ) -> tuple[float, float, float]:
        azimuth_rad = azimuth_deg * pi / 180.0
        elevation_rad = elevation_deg * pi / 180.0
        horizontal = range_m * cos(elevation_rad)
        return (
            horizontal * cos(azimuth_rad),
            horizontal * sin(azimuth_rad),
            range_m * sin(elevation_rad),
        )

    def _radar_unit_vector_to_origin(
        self,
        point: tuple[float, float, float],
    ) -> tuple[float, float, float]:
        norm = sqrt(point[0] * point[0] + point[1] * point[1] + point[2] * point[2])
        if norm <= 1e-9:
            return (0.0, 0.0, 0.0)
        return (-point[0] / norm, -point[1] / norm, -point[2] / norm)

    def _radar_multipath_reflection_point(
        self,
        *,
        surface: str,
        surface_sign: float,
        bounce_index: int,
        target_point: tuple[float, float, float],
    ) -> tuple[float, float, float] | None:
        x, y, z = target_point
        if surface == "GROUND_PLANE":
            return (
                max(0.25, x * (0.38 + 0.06 * bounce_index)),
                y * (0.18 + 0.04 * bounce_index),
                0.0,
            )
        if surface == "VERTICAL_PLANE":
            return (
                max(0.25, x * (0.52 + 0.04 * bounce_index)),
                y + surface_sign * max(0.35, 0.12 * abs(y) + 0.15 * bounce_index),
                z * (0.35 + 0.06 * bounce_index),
            )
        return None

    def _radar_micro_doppler_offset_mps(
        self,
        *,
        radar_config: Any,
        point_index: int,
        range_m: float,
        azimuth_deg: float,
        bounce_index: int,
    ) -> float:
        if not bool(radar_config.fidelity.enable_micro_doppler):
            return 0.0
        amplitude = min(0.8, 0.12 + 0.06 * bounce_index)
        phase = (
            (point_index + 1) * 0.73
            + range_m * 0.035
            + azimuth_deg * 0.09
            + bounce_index * 0.41
        )
        return amplitude * sin(phase)

    def _radar_detection_metrics(
        self,
        *,
        radar_config: Any,
        range_m: float,
        azimuth_deg: float,
        elevation_deg: float,
        intrinsic_rcs_dbsm: float,
        sampling_density: float,
    ) -> dict[str, float | bool]:
        antenna_gain_db = self._radar_antenna_gain_db(
            radar_config=radar_config,
            azimuth_deg=azimuth_deg,
            elevation_deg=elevation_deg,
        )
        sampling_gain_db = 10.0 * log10(max(1.0 + 8.0 * max(sampling_density, 0.0), 1e-9))
        signal_power_dbw = (
            float(radar_config.system.transmit_power_dbm)
            - 30.0
            + float(radar_config.system.radiometric_calibration_factor_db)
            + intrinsic_rcs_dbsm
            + antenna_gain_db
            + sampling_gain_db
            - 40.0 * log10(max(range_m, 1e-3))
        )
        noise_power_dbw = float(radar_config.detector.noise_variance_dbw)
        snr_db = signal_power_dbw - noise_power_dbw
        target_detectability = radar_config.detector.target_detectability
        reference_probability = min(
            max(float(target_detectability.probability_detection), 1e-6),
            1.0 - 1e-6,
        )
        calibration_logit = log(reference_probability / (1.0 - reference_probability))
        relative_budget_db = (
            intrinsic_rcs_dbsm
            - float(target_detectability.calibration_target_rcs_dbsm)
            - 40.0
            * log10(
                max(range_m, 1e-3)
                / max(float(target_detectability.calibration_target_range_m), 1e-3)
            )
            + antenna_gain_db
        )
        detection_probability = 1.0 / (1.0 + exp(-(relative_budget_db / 6.0 + calibration_logit)))
        detected = bool(snr_db >= float(radar_config.detector.minimum_snr_db)) and detection_probability >= 0.5
        return {
            "detected": detected,
            "signal_power_dbw": signal_power_dbw,
            "noise_power_dbw": noise_power_dbw,
            "snr_db": snr_db,
            "detection_probability": detection_probability,
            "antenna_gain_db": antenna_gain_db,
            "sampling_gain_db": sampling_gain_db,
        }

    def _radar_antenna_gain_db(
        self,
        *,
        radar_config: Any,
        azimuth_deg: float,
        elevation_deg: float,
    ) -> float:
        antenna_model = getattr(radar_config.system, "antenna_model", None)
        if antenna_model is not None and str(getattr(antenna_model, "model_type", "")).upper() == "FROM_DIRECTIVITY_AZ_EL_CUTS":
            az_gain_db = self._radar_directivity_cut_gain_db(
                cut=getattr(antenna_model, "az_cut", None),
                angle_deg=azimuth_deg,
            )
            el_gain_db = self._radar_directivity_cut_gain_db(
                cut=getattr(antenna_model, "el_cut", None),
                angle_deg=elevation_deg,
            )
            return max(-40.0, az_gain_db + el_gain_db)
        hpbw_az = max(abs(float(radar_config.system.antenna_hpbw.az_deg)), 1e-6)
        hpbw_el = max(abs(float(radar_config.system.antenna_hpbw.el_deg)), 1e-6)
        normalized_az = azimuth_deg / hpbw_az
        normalized_el = elevation_deg / hpbw_el
        return max(-40.0, -3.0 * (normalized_az * normalized_az + normalized_el * normalized_el))

    def _radar_directivity_cut_gain_db(self, *, cut: Any, angle_deg: float) -> float:
        if cut is None:
            return 0.0
        angles_deg = [float(angle) for angle in getattr(cut, "angles_deg", [])]
        amplitudes = [max(float(amplitude), 0.0) for amplitude in getattr(cut, "amplitudes", [])]
        if not angles_deg or not amplitudes:
            return 0.0
        limit = min(len(angles_deg), len(amplitudes))
        pairs = sorted(zip(angles_deg[:limit], amplitudes[:limit]), key=lambda item: item[0])
        normalized_amplitudes = [pair[1] for pair in pairs]
        if not bool(getattr(cut, "do_not_normalize", False)):
            peak = max(normalized_amplitudes)
            if peak > 1e-9:
                normalized_amplitudes = [value / peak for value in normalized_amplitudes]
        sample_amplitude = self._interpolate_scalar_series(
            xs=[pair[0] for pair in pairs],
            ys=normalized_amplitudes,
            x=angle_deg,
        )
        return 20.0 * log10(max(sample_amplitude, 1e-6))

    def _interpolate_scalar_series(self, *, xs: list[float], ys: list[float], x: float) -> float:
        if not xs or not ys:
            return 0.0
        if len(xs) == 1 or len(ys) == 1:
            return float(ys[0])
        if x <= xs[0]:
            return float(ys[0])
        if x >= xs[-1]:
            return float(ys[-1])
        for index in range(1, min(len(xs), len(ys))):
            left_x = xs[index - 1]
            right_x = xs[index]
            if x > right_x:
                continue
            delta = right_x - left_x
            if abs(delta) <= 1e-9:
                return float(ys[index])
            alpha = (x - left_x) / delta
            return float(ys[index - 1]) + (float(ys[index]) - float(ys[index - 1])) * alpha
        return float(ys[-1])

    def _radar_adaptive_sampling_profile(
        self,
        *,
        request: SensorSimRequest,
        radar_config: Any,
        point_index: int,
    ) -> dict[str, object]:
        actor_id = None
        raw_actor_ids = request.options.get("radar_point_actor_ids")
        if isinstance(raw_actor_ids, list) and 0 <= point_index < len(raw_actor_ids):
            actor_id = str(raw_actor_ids[point_index])
        density = max(float(radar_config.fidelity.raytracing.default_min_rays_per_wavelength), 0.0)
        target_override = False
        for target in getattr(radar_config.fidelity.raytracing, "adaptive_targets", []):
            if actor_id is None or str(getattr(target, "actor_id", "")) != actor_id:
                continue
            density = max(density, float(getattr(target, "min_rays_per_wavelength", 0.0)))
            target_override = True
        subdivision_level = 0
        if density > 0.0:
            base_density = 0.2
            subdivision_level = max(1, int((density / base_density) + 0.999999))
            max_subdivision_level = int(getattr(radar_config.fidelity.raytracing, "max_subdivision_level", 0))
            if max_subdivision_level > 0:
                subdivision_level = min(subdivision_level, max_subdivision_level)
        return {
            "actor_id": actor_id,
            "density": density,
            "target_override": target_override,
            "subdivision_level": subdivision_level,
        }

    def _radar_accuracy_sigma(
        self,
        *,
        accuracy_config: Any,
        regions: list[Any],
        range_m: float,
        azimuth_deg: float,
        elevation_deg: float,
    ) -> tuple[float, int | None]:
        for index, region in enumerate(regions):
            if (
                range_m < float(region.range_min_m)
                or range_m > float(region.range_max_m)
                or azimuth_deg < float(region.azimuth_min_deg)
                or azimuth_deg > float(region.azimuth_max_deg)
                or elevation_deg < float(region.elevation_min_deg)
                or elevation_deg > float(region.elevation_max_deg)
            ):
                continue
            sigma = float(region.max_deviation) / max(float(region.num_sigma), 1e-6)
            return max(sigma, 0.0), index
        sigma = float(accuracy_config.max_deviation) / max(float(accuracy_config.num_sigma), 1e-6)
        return max(sigma, 0.0), None

    def _radar_quantize_value(self, *, value: float, quantization_step: float) -> float:
        step = abs(float(quantization_step))
        if step <= 1e-12:
            return value
        return round(value / step) * step

    def _radar_rcs_dbsm_to_linear(self, rcs_dbsm: object) -> float:
        return 10.0 ** (float(rcs_dbsm) / 10.0)

    def _radar_rcs_linear_to_dbsm(self, rcs_linear_m2: float) -> float:
        return 10.0 * log10(max(float(rcs_linear_m2), 1e-12))

    def _radar_track_group_key(self, detection: dict[str, object]) -> str:
        actor_id = detection.get("ground_truth_actor_id")
        if actor_id is not None:
            return f"actor:{actor_id}"
        return f"detection:{detection.get('id')}"

    def _radar_measurement_source_counts(
        self,
        detections: list[dict[str, object]],
    ) -> dict[str, int]:
        counts: dict[str, int] = {}
        for detection in detections:
            source = str(detection.get("measurement_source", "")).upper().strip()
            if not source:
                continue
            counts[source] = counts.get(source, 0) + 1
        return counts

    def _match_radar_track_state(
        self,
        *,
        radar_config: Any,
        track: dict[str, object],
        previous_track_states: dict[int, dict[str, object]],
        candidate_state_ids: set[int],
    ) -> int | None:
        if not candidate_state_ids:
            return None
        range_gate_m = max(3.0, 4.0 * float(radar_config.system.range_resolution_m))
        az_gate_deg = max(8.0, 4.0 * float(radar_config.system.angular_resolution.az_deg))
        el_gate_deg = max(8.0, 4.0 * float(radar_config.system.angular_resolution.el_deg))
        best_state_id: int | None = None
        best_score = float("inf")
        track_actor_id = track.get("ground_truth_actor_id")
        track_semantic_class = track.get("ground_truth_semantic_class")
        for state_id in candidate_state_ids:
            state = previous_track_states[state_id]
            state_actor_id = state.get("ground_truth_actor_id")
            if track_actor_id is not None and state_actor_id is not None and track_actor_id != state_actor_id:
                continue
            state_semantic_class = state.get("ground_truth_semantic_class")
            if (
                track_actor_id is None
                and state_actor_id is None
                and track_semantic_class is not None
                and state_semantic_class is not None
                and track_semantic_class != state_semantic_class
            ):
                continue
            range_delta = abs(float(track.get("range_m", 0.0)) - float(state.get("range_m", 0.0)))
            az_delta = abs(
                self._normalize_angle_deg(
                    float(track.get("azimuth_deg", 0.0)) - float(state.get("azimuth_deg", 0.0))
                )
            )
            el_delta = abs(float(track.get("elevation_deg", 0.0)) - float(state.get("elevation_deg", 0.0)))
            if range_delta > range_gate_m or az_delta > az_gate_deg or el_delta > el_gate_deg:
                continue
            score = range_delta / range_gate_m + az_delta / az_gate_deg + el_delta / el_gate_deg
            if score < best_score:
                best_score = score
                best_state_id = state_id
        return best_state_id

    def _annotate_radar_tracks_with_history(
        self,
        *,
        radar_config: Any,
        tracks: list[dict[str, object]],
        previous_track_states: dict[int, dict[str, object]],
        next_persistent_track_id: int,
        frame_id: int,
        frame_time_s: float,
    ) -> tuple[list[dict[str, object]], dict[int, dict[str, object]], int, int]:
        frame_period_s = 1.0 / max(float(radar_config.system.frame_rate_hz), 1e-6)
        available_state_ids = set(previous_track_states.keys())
        updated_track_states: dict[int, dict[str, object]] = {}
        reassociation_count = 0
        for track in tracks:
            group_key = str(track.get("group_key", "")).strip()
            matched_state_id: int | None = None
            if group_key.startswith("actor:"):
                for state_id in list(available_state_ids):
                    if str(previous_track_states[state_id].get("group_key", "")) == group_key:
                        matched_state_id = state_id
                        break
            if matched_state_id is None:
                matched_state_id = self._match_radar_track_state(
                    radar_config=radar_config,
                    track=track,
                    previous_track_states=previous_track_states,
                    candidate_state_ids=available_state_ids,
                )
            if matched_state_id is None:
                persistent_track_id = next_persistent_track_id
                next_persistent_track_id += 1
                first_seen_time_s = frame_time_s
                history_length = 1
                reassociated = False
            else:
                previous_state = previous_track_states[matched_state_id]
                persistent_track_id = matched_state_id
                first_seen_time_s = float(previous_state.get("first_seen_time_s", frame_time_s))
                history_length = int(previous_state.get("history_length", 0)) + 1
                reassociated = True
                reassociation_count += 1
                available_state_ids.discard(matched_state_id)
            track["persistent_track_id"] = persistent_track_id
            track["track_history_length"] = history_length
            track["track_first_seen_time_s"] = first_seen_time_s
            track["track_last_seen_time_s"] = frame_time_s
            track["track_age_s"] = max(frame_period_s, frame_time_s - first_seen_time_s + frame_period_s)
            track["track_status"] = "CONTINUING" if reassociated else "NEW"
            track["track_reassociated"] = reassociated
            track["trajectory_frame_id"] = frame_id
            updated_track_states[persistent_track_id] = {
                "group_key": group_key,
                "ground_truth_actor_id": track.get("ground_truth_actor_id"),
                "ground_truth_semantic_class": track.get("ground_truth_semantic_class"),
                "range_m": float(track.get("range_m", 0.0)),
                "azimuth_deg": float(track.get("azimuth_deg", 0.0)),
                "elevation_deg": float(track.get("elevation_deg", 0.0)),
                "first_seen_time_s": first_seen_time_s,
                "history_length": history_length,
                "last_seen_time_s": frame_time_s,
            }
        return tracks, updated_track_states, next_persistent_track_id, reassociation_count

    def _build_radar_tracks(
        self,
        *,
        radar_config: Any,
        detections: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        if not bool(radar_config.tracking.output_tracks):
            return []
        limit = int(radar_config.tracking.max_tracks)
        grouped_detections: dict[str, list[dict[str, object]]] = {}
        for detection in detections:
            if bool(detection.get("is_false_alarm")):
                continue
            group_key = self._radar_track_group_key(detection)
            grouped_detections.setdefault(group_key, []).append(detection)
        aggregated_tracks: list[tuple[float, float, dict[str, object], list[dict[str, object]]]] = []
        for group_key, group in grouped_detections.items():
            if not group:
                continue
            cartesian_points = [
                self._radar_cartesian_from_spherical(
                    range_m=float(detection.get("range_m", 0.0)),
                    azimuth_deg=float(detection.get("azimuth_deg", 0.0)),
                    elevation_deg=float(detection.get("elevation_deg", 0.0)),
                )
                for detection in group
            ]
            center_x = sum(point[0] for point in cartesian_points) / len(cartesian_points)
            center_y = sum(point[1] for point in cartesian_points) / len(cartesian_points)
            center_z = sum(point[2] for point in cartesian_points) / len(cartesian_points)
            center_range_m = sqrt(center_x * center_x + center_y * center_y + center_z * center_z)
            horizontal_norm = sqrt(center_x * center_x + center_y * center_y)
            source_target_ids = [
                int(detection["id"])
                for detection in group
                if detection.get("id") is not None
            ]
            linear_signal_weights = [
                max(10.0 ** (float(detection.get("signal_power_dbw", -120.0)) / 10.0), 1e-12)
                for detection in group
            ]
            weight_sum = sum(linear_signal_weights)
            combined_rcs_linear_m2 = sum(
                self._radar_rcs_dbsm_to_linear(detection.get("rcs_dbsm", 0.0))
                for detection in group
            )
            confidence_miss_probability = 1.0
            for detection in group:
                confidence_miss_probability *= 1.0 - min(
                    max(float(detection.get("detection_probability", 0.0)), 0.0),
                    1.0,
                )
            measurement_source_counts = self._radar_measurement_source_counts(group)
            multipath_path_type_counts = self._radar_multipath_path_type_counts(group)
            aggregated_tracks.append(
                (
                    -combined_rcs_linear_m2,
                    center_range_m,
                    {
                        "id": -1,
                        "group_key": group_key,
                        "source_target_id": source_target_ids[0] if source_target_ids else None,
                        "source_target_ids": source_target_ids,
                        "source_target_count": len(group),
                        "source_measurement_source_counts": measurement_source_counts,
                        "source_multipath_target_count": measurement_source_counts.get("MULTIPATH", 0),
                        "source_multipath_path_type_counts": multipath_path_type_counts,
                        "range_m": center_range_m,
                        "azimuth_deg": atan2(center_y, center_x) * 180.0 / pi,
                        "elevation_deg": atan2(center_z, max(horizontal_norm, 1e-9)) * 180.0 / pi,
                        "radial_velocity_mps": sum(
                            float(detection.get("radial_velocity_mps", 0.0)) * weight
                            for detection, weight in zip(group, linear_signal_weights)
                        ) / max(weight_sum, 1e-12),
                        "rcs_dbsm": self._radar_rcs_linear_to_dbsm(combined_rcs_linear_m2),
                        "rcs_linear_m2": combined_rcs_linear_m2,
                        "confidence": 1.0 - confidence_miss_probability,
                        "ground_truth_actor_id": group[0].get("ground_truth_actor_id"),
                        "ground_truth_semantic_class": group[0].get("ground_truth_semantic_class"),
                        "ground_truth_semantic_class_name": group[0].get("ground_truth_semantic_class_name"),
                        "measurement_source": "TRACK",
                        "age_s": 1.0 / max(float(radar_config.system.frame_rate_hz), 1e-6),
                    },
                    group,
                )
            )
        aggregated_tracks.sort(key=lambda item: (item[0], item[1]))
        if limit <= 0:
            limit = len(aggregated_tracks)
        tracks: list[dict[str, object]] = []
        for track_id, (_, _, track, group) in enumerate(aggregated_tracks[:limit]):
            track["id"] = track_id
            for detection in group:
                detection["track_id"] = track_id
            tracks.append(track)
        return tracks

    def _radar_extrinsics_from_options(self, request: SensorSimRequest) -> CameraExtrinsics:
        return self._sensor_config_from_request(request).radar.extrinsics.to_camera_extrinsics()

    def _build_radar_extrinsics_from_pose(
        self,
        request: SensorSimRequest,
        base_extrinsics: CameraExtrinsics,
        pose: TrajectoryPose,
        force_enable: bool,
    ) -> CameraExtrinsics:
        position_mode = str(
            request.options.get("radar_extrinsics_auto_use_position", "xy")
        ).lower()
        use_orientation = bool(request.options.get("radar_extrinsics_auto_use_orientation", True))

        tx, ty, tz = base_extrinsics.tx, base_extrinsics.ty, base_extrinsics.tz
        if position_mode in {"xy", "xyz"}:
            tx = pose.x
            ty = pose.y
        if position_mode == "xyz":
            tz = pose.z

        roll_deg = base_extrinsics.roll_deg
        pitch_deg = base_extrinsics.pitch_deg
        yaw_deg = base_extrinsics.yaw_deg
        if use_orientation:
            roll_deg = pose.roll_deg
            pitch_deg = pose.pitch_deg
            yaw_deg = pose.yaw_deg

        offsets = request.options.get("radar_extrinsics_auto_offsets", {})
        if isinstance(offsets, dict):
            tx += float(offsets.get("tx", 0.0))
            ty += float(offsets.get("ty", 0.0))
            tz += float(offsets.get("tz", 0.0))
            roll_deg += float(offsets.get("roll_deg", 0.0))
            pitch_deg += float(offsets.get("pitch_deg", 0.0))
            yaw_deg += float(offsets.get("yaw_deg", 0.0))

        enabled = bool(base_extrinsics.enabled)
        if force_enable:
            enabled = True
        return CameraExtrinsics(
            tx=tx,
            ty=ty,
            tz=tz,
            roll_deg=roll_deg,
            pitch_deg=pitch_deg,
            yaw_deg=yaw_deg,
            enabled=enabled,
        )

    def _estimate_ego_velocity_from_trajectory(
        self,
        request: SensorSimRequest,
        artifacts: dict[str, Path],
    ) -> tuple[float, float, float]:
        if not bool(request.options.get("radar_use_ego_velocity_from_trajectory", True)):
            default_speed = float(request.options.get("radar_default_ego_speed_mps", 0.0))
            return (default_speed, 0.0, 0.0)

        trajectory_path = artifacts.get("trajectory_primary")
        if trajectory_path is None or not trajectory_path.exists():
            default_speed = float(request.options.get("radar_default_ego_speed_mps", 0.0))
            return (default_speed, 0.0, 0.0)

        poses = read_trajectory_poses(
            trajectory_path,
            max_rows=int(request.options.get("camera_extrinsics_auto_max_rows", 20000)),
        )
        if len(poses) < 2:
            default_speed = float(request.options.get("radar_default_ego_speed_mps", 0.0))
            return (default_speed, 0.0, 0.0)

        first = poses[0]
        last = poses[-1]
        dt = last.time_s - first.time_s
        if dt <= 1e-9:
            return (0.0, 0.0, 0.0)
        return ((last.x - first.x) / dt, (last.y - first.y) / dt, (last.z - first.z) / dt)

    def _estimate_ego_velocity_for_pose_index(
        self,
        poses: list[TrajectoryPose],
        pose_index: int,
    ) -> tuple[float, float, float]:
        if not poses:
            return (0.0, 0.0, 0.0)
        if len(poses) == 1:
            return (0.0, 0.0, 0.0)

        left_index = pose_index - 1 if pose_index > 0 else pose_index
        right_index = pose_index + 1 if pose_index < len(poses) - 1 else pose_index
        if left_index == right_index:
            return (0.0, 0.0, 0.0)

        left = poses[left_index]
        right = poses[right_index]
        dt = right.time_s - left.time_s
        if dt <= 1e-9:
            return (0.0, 0.0, 0.0)
        return (
            (right.x - left.x) / dt,
            (right.y - left.y) / dt,
            (right.z - left.z) / dt,
        )

    def _resolve_effective_extrinsics(
        self,
        request: SensorSimRequest,
        artifacts: dict[str, Path],
        base_extrinsics: CameraExtrinsics,
        reference_point: tuple[float, float, float] | None,
    ) -> tuple[CameraExtrinsics, dict[str, object]]:
        auto_enabled = bool(request.options.get("camera_extrinsics_auto_from_trajectory", False))
        if not auto_enabled:
            return base_extrinsics, {"source": "manual", "trajectory_pose": None}

        trajectory_path = artifacts.get("trajectory_primary")
        if trajectory_path is None or not trajectory_path.exists():
            return base_extrinsics, {"source": "manual", "trajectory_pose": None}

        poses = read_trajectory_poses(
            trajectory_path,
            max_rows=int(request.options.get("camera_extrinsics_auto_max_rows", 20000)),
        )
        if not poses:
            return base_extrinsics, {"source": "manual", "trajectory_pose": None}

        pose = self._select_trajectory_pose(
            poses=poses,
            selector=str(request.options.get("camera_extrinsics_auto_pose", "first")),
        )
        effective = self._build_extrinsics_from_pose(
            request=request,
            base_extrinsics=base_extrinsics,
            pose=pose,
            reference_point=reference_point,
            force_enable=True,
        )
        pose_payload = self._trajectory_pose_payload(
            pose=pose,
            trajectory_path=trajectory_path,
        )
        return effective, {"source": "trajectory_auto", "trajectory_pose": pose_payload}

    def _build_extrinsics_from_pose(
        self,
        request: SensorSimRequest,
        base_extrinsics: CameraExtrinsics,
        pose: TrajectoryPose,
        reference_point: tuple[float, float, float] | None,
        force_enable: bool,
    ) -> CameraExtrinsics:
        position_mode = str(
            request.options.get("camera_extrinsics_auto_use_position", "xy")
        ).lower()
        use_orientation = bool(request.options.get("camera_extrinsics_auto_use_orientation", False))

        tx, ty, tz = base_extrinsics.tx, base_extrinsics.ty, base_extrinsics.tz
        if position_mode in {"xy", "xyz"}:
            tx = pose.x
            ty = pose.y
        if position_mode == "xyz":
            tz = pose.z

        roll_deg = base_extrinsics.roll_deg
        pitch_deg = base_extrinsics.pitch_deg
        yaw_deg = base_extrinsics.yaw_deg
        if use_orientation:
            roll_deg = pose.roll_deg
            pitch_deg = pose.pitch_deg
            yaw_deg = pose.yaw_deg

        offsets = request.options.get("camera_extrinsics_auto_offsets", {})
        if isinstance(offsets, dict):
            tx += float(offsets.get("tx", 0.0))
            ty += float(offsets.get("ty", 0.0))
            tz += float(offsets.get("tz", 0.0))
            roll_deg += float(offsets.get("roll_deg", 0.0))
            pitch_deg += float(offsets.get("pitch_deg", 0.0))
            yaw_deg += float(offsets.get("yaw_deg", 0.0))

        if (
            reference_point is not None
            and bool(request.options.get("camera_reference_apply_to_extrinsics", True))
            and position_mode in {"xy", "xyz"}
        ):
            tx -= reference_point[0]
            ty -= reference_point[1]
            if position_mode == "xyz":
                apply_ref_z = bool(request.options.get("camera_reference_apply_z", True))
                if apply_ref_z:
                    tz -= reference_point[2]

        enabled = bool(base_extrinsics.enabled)
        if force_enable:
            enabled = True
        return CameraExtrinsics(
            tx=tx,
            ty=ty,
            tz=tz,
            roll_deg=roll_deg,
            pitch_deg=pitch_deg,
            yaw_deg=yaw_deg,
            enabled=enabled,
        )

    def _trajectory_pose_payload(
        self,
        pose: TrajectoryPose,
        trajectory_path: Path,
    ) -> dict[str, object]:
        return {
            "x": pose.x,
            "y": pose.y,
            "z": pose.z,
            "time_s": pose.time_s,
            "roll_deg": pose.roll_deg,
            "pitch_deg": pose.pitch_deg,
            "yaw_deg": pose.yaw_deg,
            "trajectory_path": str(trajectory_path),
        }

    def _select_trajectory_pose(
        self,
        poses: list[TrajectoryPose],
        selector: str,
    ) -> TrajectoryPose:
        if not poses:
            raise ValueError("poses must not be empty")
        normalized = selector.lower().strip()
        if normalized == "last":
            return poses[-1]
        if normalized == "middle":
            return poses[len(poses) // 2]
        return poses[0]

    def _sample_trajectory_poses(
        self,
        poses: list[TrajectoryPose],
        frame_count: int,
    ) -> list[tuple[int, TrajectoryPose]]:
        if not poses:
            return []
        if frame_count <= 1:
            return [(0, poses[0])]
        if frame_count >= len(poses):
            return [(idx, pose) for idx, pose in enumerate(poses)]

        sampled: list[tuple[int, TrajectoryPose]] = []
        max_index = len(poses) - 1
        for frame_idx in range(frame_count):
            ratio = float(frame_idx) / float(frame_count - 1)
            pose_index = int(round(ratio * max_index))
            if sampled and sampled[-1][0] == pose_index:
                continue
            sampled.append((pose_index, poses[pose_index]))
        return sampled

    def _apply_projection_reference_frame(
        self,
        points_xyz: list[tuple[float, float, float]],
        request: SensorSimRequest,
    ) -> tuple[list[tuple[float, float, float]], tuple[float, float, float] | None]:
        mode = str(request.options.get("camera_reference_mode", "none")).lower().strip()
        explicit = request.options.get("camera_reference_point")
        apply_z = bool(request.options.get("camera_reference_apply_z", True))
        if mode in {"first_point_xy", "mean_point_xy"} and "camera_reference_apply_z" not in request.options:
            apply_z = False

        reference_point: tuple[float, float, float] | None = None
        if explicit is not None:
            if not isinstance(explicit, list) or len(explicit) != 3:
                raise ValueError("camera_reference_point must be [x, y, z].")
            reference_point = (float(explicit[0]), float(explicit[1]), float(explicit[2]))
        elif mode == "first_point":
            first = points_xyz[0]
            reference_point = (float(first[0]), float(first[1]), float(first[2]))
        elif mode == "first_point_xy":
            first = points_xyz[0]
            reference_point = (float(first[0]), float(first[1]), 0.0)
        elif mode == "mean_point":
            count = float(len(points_xyz))
            mean_x = sum(point[0] for point in points_xyz) / count
            mean_y = sum(point[1] for point in points_xyz) / count
            mean_z = sum(point[2] for point in points_xyz) / count
            reference_point = (mean_x, mean_y, mean_z)
        elif mode == "mean_point_xy":
            count = float(len(points_xyz))
            mean_x = sum(point[0] for point in points_xyz) / count
            mean_y = sum(point[1] for point in points_xyz) / count
            reference_point = (mean_x, mean_y, 0.0)

        if reference_point is None:
            return points_xyz, None

        ref_x, ref_y, ref_z = reference_point
        transformed = [(x - ref_x, y - ref_y, z - ref_z if apply_z else z) for x, y, z in points_xyz]
        return transformed, reference_point
