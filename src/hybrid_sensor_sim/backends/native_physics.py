from __future__ import annotations

import json
import random
from math import atan2, log, log10, pi, sqrt
from pathlib import Path
from typing import Any

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
        lidar_noisy_artifact = self._generate_lidar_noisy_pointcloud_if_available(
            request=request,
            artifacts=artifacts,
            enhanced_output=enhanced_output,
            metrics=metrics,
        )
        if lidar_noisy_artifact is not None:
            artifacts["lidar_noisy_preview"] = lidar_noisy_artifact
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
            "rolling_shutter": self._camera_rolling_shutter_payload(
                camera_config=camera_config,
                intrinsics=intrinsics,
                runtime=rolling_runtime,
            ),
            "depth_params": camera_config.depth_params.to_dict(),
            "semantic_params": camera_config.semantic_params.to_dict(),
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
        for pose_index, pose in selected_poses:
            effective = self._build_extrinsics_from_pose(
                request=request,
                base_extrinsics=extrinsics,
                pose=pose,
                reference_point=reference_point,
                force_enable=True,
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
        payload: dict[str, object] = {
            "output_mode": camera_config.sensor_type,
            "preview_points_uvz": [
                {"u": float(point["u"]), "v": float(point["v"]), "z": float(point["z"])}
                for point in preview_points
            ],
            "preview_depth_samples": [],
            "preview_semantic_samples": [],
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
            semantic_samples = [
                self._camera_semantic_sample(
                    request=request,
                    camera_config=camera_config,
                    projected_sample=point,
                )
                for point in preview_points
            ]
            payload["preview_semantic_samples"] = semantic_samples
            payload["preview_semantic_legend"] = self._camera_semantic_legend(semantic_samples)
        else:
            payload["preview_semantic_legend"] = []
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
        }
        semantic_params = camera_config.semantic_params
        if semantic_params.include_actor_id:
            semantic_payload["actor_id"] = int(semantic["actor_id"])
        if semantic_params.include_component_id:
            semantic_payload["component_id"] = int(semantic["component_id"])
        if semantic_params.include_material_class:
            semantic_payload["material_class_id"] = int(semantic["material_class_id"])
        if semantic_params.include_material_uuid:
            semantic_payload["material_uuid"] = int(semantic["material_uuid"])
        if semantic_params.include_base_map_element:
            semantic_payload["base_map_element_id"] = int(semantic["base_map_element_id"])
        if semantic_params.include_procedural_map_element:
            semantic_payload["procedural_map_element_id"] = int(
                semantic["procedural_map_element_id"]
            )
        if semantic_params.include_lane_marking_id:
            semantic_payload["lane_marking_id"] = int(semantic["lane_marking_id"])
        return semantic_payload

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
        x, y, z = world_point
        class_version = camera_config.semantic_params.class_version.upper()
        if class_version == "GRANULAR_SEGMENTATION":
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
            class_version=class_version,
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
    ) -> Path | None:
        if not bool(request.options.get("lidar_postprocess_enabled", True)):
            return None

        point_cloud = artifacts.get("point_cloud_primary")
        if point_cloud is None or point_cloud.suffix.lower() != ".xyz" or not point_cloud.exists():
            return None

        max_points = int(request.options.get("lidar_postprocess_max_points", 50000))
        points_xyz = read_xyz_points(point_cloud, max_points=max_points)
        if not points_xyz:
            metrics["lidar_input_count"] = 0.0
            metrics["lidar_output_count"] = 0.0
            return None

        rng = random.Random(int(request.seed) + 17)
        noisy_points = self._apply_lidar_noise_and_dropout(
            request=request,
            points_xyz=points_xyz,
            rng=rng,
        )

        output_path = enhanced_output / "lidar_noisy_preview.xyz"
        write_xyz_points(output_path, noisy_points)
        noise_model = str(request.options.get("lidar_noise", "gaussian")).lower().strip()
        noise_stddev = float(request.options.get("lidar_noise_stddev_m", 0.02))
        metrics["lidar_input_count"] = float(len(points_xyz))
        metrics["lidar_output_count"] = float(len(noisy_points))
        metrics["lidar_dropout_ratio"] = (
            1.0 - (float(len(noisy_points)) / float(len(points_xyz)))
            if points_xyz
            else 0.0
        )
        metrics["lidar_noise_stddev_m"] = float(noise_stddev if noise_model == "gaussian" else 0.0)
        return output_path

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

        frames: list[dict[str, object]] = []
        total_output_count = 0
        for frame_id, (pose_index, pose) in enumerate(selected_poses):
            effective_extrinsics = self._build_lidar_extrinsics_from_pose(
                request=request,
                base_extrinsics=base_extrinsics,
                pose=pose,
                force_enable=True,
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
            rng = random.Random(int(request.seed) + 937 + frame_id)
            noisy_points = self._apply_lidar_noise_and_dropout(
                request=request,
                points_xyz=points_lidar,
                rng=rng,
            )
            total_output_count += len(noisy_points)
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
                    "output_count": len(noisy_points),
                    "preview_points_xyz": [
                        {"x": x, "y": y, "z": z}
                        for x, y, z in noisy_points[:preview_points_per_frame]
                    ],
                }
            )

        metrics["lidar_trajectory_sweep_frame_count"] = float(len(frames))
        metrics["lidar_trajectory_sweep_total_output_count"] = float(total_output_count)
        metrics["lidar_motion_compensation_applied"] = 1.0 if motion_comp_enabled else 0.0
        payload = {
            "input_point_cloud": str(point_cloud),
            "trajectory_path": str(trajectory_path),
            "input_count": len(points_xyz),
            "frame_count": len(frames),
            "frames": frames,
        }
        output_path = enhanced_output / "lidar_trajectory_sweep.json"
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
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
        if not bool(request.options.get("radar_postprocess_enabled", True)):
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

        extrinsics = self._radar_extrinsics_from_options(request)
        points_radar = transform_points_world_to_camera(points_xyz=points_xyz, extrinsics=extrinsics)
        rng = random.Random(int(request.seed) + 137)
        clutter_model = str(request.options.get("radar_clutter", "basic")).lower().strip()
        ego_vx, ego_vy, ego_vz = self._estimate_ego_velocity_from_trajectory(request, artifacts)
        selected, false_added = self._build_radar_targets_from_points(
            request=request,
            points_radar=points_radar,
            ego_velocity=(ego_vx, ego_vy, ego_vz),
            rng=rng,
            false_target_count=int(
                request.options.get(
                    "radar_false_target_count",
                    2 if clutter_model == "basic" else 0,
                )
            ),
        )

        preview = {
            "input_point_cloud": str(point_cloud),
            "input_count": len(points_xyz),
            "target_count": len(selected),
            "ego_velocity_mps": {
                "vx": ego_vx,
                "vy": ego_vy,
                "vz": ego_vz,
            },
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
        }
        output_path = enhanced_output / "radar_targets_preview.json"
        output_path.write_text(json.dumps(preview, indent=2), encoding="utf-8")
        metrics["radar_input_count"] = float(len(points_xyz))
        metrics["radar_target_count"] = float(len(selected))
        metrics["radar_false_target_count"] = float(false_added)
        return output_path

    def _generate_radar_targets_trajectory_sweep_if_available(
        self,
        request: SensorSimRequest,
        artifacts: dict[str, Path],
        enhanced_output: Path,
        metrics: dict[str, float],
    ) -> Path | None:
        if not bool(request.options.get("radar_postprocess_enabled", True)):
            return None
        if not bool(request.options.get("radar_trajectory_sweep_enabled", False)):
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
        clutter_model = str(request.options.get("radar_clutter", "basic")).lower().strip()
        default_false_target_count = 2 if clutter_model == "basic" else 0
        false_target_count = int(
            request.options.get("radar_false_target_count", default_false_target_count)
        )
        preview_targets_per_frame = int(request.options.get("radar_preview_targets_per_frame", 16))
        base_extrinsics = self._radar_extrinsics_from_options(request)
        frames: list[dict[str, object]] = []
        total_targets = 0
        for frame_id, (pose_index, pose) in enumerate(selected_poses):
            effective_extrinsics = self._build_radar_extrinsics_from_pose(
                request=request,
                base_extrinsics=base_extrinsics,
                pose=pose,
                force_enable=True,
            )
            points_radar = transform_points_world_to_camera(
                points_xyz=points_xyz,
                extrinsics=effective_extrinsics,
            )
            ego_velocity = self._estimate_ego_velocity_for_pose_index(poses=poses, pose_index=pose_index)
            rng = random.Random(int(request.seed) + 537 + frame_id)
            targets, false_added = self._build_radar_targets_from_points(
                request=request,
                points_radar=points_radar,
                ego_velocity=ego_velocity,
                rng=rng,
                false_target_count=false_target_count,
            )
            total_targets += len(targets)
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
                    "false_target_count": false_added,
                    "targets_preview": targets[:preview_targets_per_frame],
                }
            )

        metrics["radar_trajectory_sweep_frame_count"] = float(len(frames))
        metrics["radar_trajectory_sweep_total_target_count"] = float(total_targets)
        payload = {
            "input_point_cloud": str(point_cloud),
            "trajectory_path": str(trajectory_path),
            "input_count": len(points_xyz),
            "frame_count": len(frames),
            "frames": frames,
        }
        output_path = enhanced_output / "radar_targets_trajectory_sweep.json"
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return output_path

    def _build_radar_targets_from_points(
        self,
        request: SensorSimRequest,
        points_radar: list[tuple[float, float, float]],
        ego_velocity: tuple[float, float, float],
        rng: random.Random,
        false_target_count: int,
    ) -> tuple[list[dict[str, object]], int]:
        clutter_model = str(request.options.get("radar_clutter", "basic")).lower().strip()
        max_targets = int(request.options.get("radar_max_targets", 64))
        min_range = float(request.options.get("radar_range_min_m", 0.5))
        max_range = float(request.options.get("radar_range_max_m", 200.0))
        horiz_fov_rad = float(request.options.get("radar_horizontal_fov_deg", 120.0)) * pi / 180.0
        vert_fov_rad = float(request.options.get("radar_vertical_fov_deg", 30.0)) * pi / 180.0
        angle_noise_deg = float(request.options.get("radar_angle_noise_stddev_deg", 0.1))
        range_noise_m = float(request.options.get("radar_range_noise_stddev_m", 0.05))
        velocity_noise_mps = float(request.options.get("radar_velocity_noise_stddev_mps", 0.1))
        rcs_base_dbsm = float(request.options.get("radar_rcs_base_dbsm", 12.0))
        ego_vx, ego_vy, ego_vz = ego_velocity

        candidates: list[tuple[float, dict[str, object]]] = []
        for x, y, z in points_radar:
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

            radial_velocity = -(ego_vx * x + ego_vy * y + ego_vz * z) / max(range_m, 1e-9)
            noisy_range = range_m
            noisy_azimuth = azimuth_rad
            noisy_elevation = elevation_rad
            noisy_radial_velocity = radial_velocity
            if clutter_model == "basic":
                noisy_range = max(min_range, noisy_range + rng.gauss(0.0, range_noise_m))
                noisy_azimuth += rng.gauss(0.0, angle_noise_deg) * pi / 180.0
                noisy_elevation += rng.gauss(0.0, angle_noise_deg) * pi / 180.0
                noisy_radial_velocity += rng.gauss(0.0, velocity_noise_mps)

            rcs = rcs_base_dbsm - 20.0 * log10(max(noisy_range, 1e-3))
            candidates.append(
                (
                    noisy_range,
                    {
                        "range_m": noisy_range,
                        "azimuth_deg": noisy_azimuth * 180.0 / pi,
                        "elevation_deg": noisy_elevation * 180.0 / pi,
                        "radial_velocity_mps": noisy_radial_velocity,
                        "rcs_dbsm": rcs,
                        "is_false_alarm": False,
                    },
                )
            )

        candidates.sort(key=lambda item: item[0])
        selected = [item[1] for item in candidates[:max_targets]]

        false_added = 0
        for _ in range(false_target_count):
            if len(selected) >= max_targets:
                break
            false_range = rng.uniform(min_range, max_range)
            false_azimuth = rng.uniform(-0.5 * horiz_fov_rad, 0.5 * horiz_fov_rad)
            false_elevation = rng.uniform(-0.5 * vert_fov_rad, 0.5 * vert_fov_rad)
            selected.append(
                {
                    "range_m": false_range,
                    "azimuth_deg": false_azimuth * 180.0 / pi,
                    "elevation_deg": false_elevation * 180.0 / pi,
                    "radial_velocity_mps": rng.gauss(0.0, 0.2),
                    "rcs_dbsm": rcs_base_dbsm - 20.0 * log10(max(false_range, 1e-3)) + rng.gauss(0.0, 2.0),
                    "is_false_alarm": True,
                }
            )
            false_added += 1

        for idx, target in enumerate(selected):
            target["id"] = idx
        return selected, false_added

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
