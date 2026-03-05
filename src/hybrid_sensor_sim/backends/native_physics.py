from __future__ import annotations

import json
from pathlib import Path

from hybrid_sensor_sim.backends.base import SensorBackend
from hybrid_sensor_sim.io.pointcloud_xyz import read_xyz_points
from hybrid_sensor_sim.physics.camera import (
    BrownConradyDistortion,
    CameraExtrinsics,
    CameraIntrinsics,
    project_points_brown_conrady,
    transform_points_world_to_camera,
)
from hybrid_sensor_sim.types import SensorSimRequest, SensorSimResult


class NativePhysicsBackend(SensorBackend):
    def name(self) -> str:
        return "native_physics"

    def simulate(self, request: SensorSimRequest) -> SensorSimResult:
        native_output = request.output_dir / "native_only"
        native_output.mkdir(parents=True, exist_ok=True)
        intrinsics = self._camera_intrinsics_from_options(request)
        distortion = self._camera_distortion_from_options(request)
        extrinsics = self._camera_extrinsics_from_options(request)
        payload = {
            "mode": "native_only",
            "scenario": str(request.scenario_path),
            "sensor_profile": request.sensor_profile,
            "physics": {
                "camera_distortion": request.options.get("camera_distortion", "brown-conrady"),
                "lidar_noise": request.options.get("lidar_noise", "gaussian"),
                "radar_clutter": request.options.get("radar_clutter", "basic"),
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
            artifacts={"native_physics": out_path},
            message="Native simulation completed.",
        )

    def enhance_from_helios(
        self,
        request: SensorSimRequest,
        helios_result: SensorSimResult,
    ) -> SensorSimResult:
        enhanced_output = request.output_dir / "hybrid_enhanced"
        enhanced_output.mkdir(parents=True, exist_ok=True)
        intrinsics = self._camera_intrinsics_from_options(request)
        distortion = self._camera_distortion_from_options(request)
        extrinsics = self._camera_extrinsics_from_options(request)

        artifacts = {**helios_result.artifacts}
        metrics = dict(helios_result.metrics)
        camera_projection_artifact = self._project_xyz_if_available(
            request=request,
            artifacts=artifacts,
            enhanced_output=enhanced_output,
            intrinsics=intrinsics,
            distortion=distortion,
            extrinsics=extrinsics,
            metrics=metrics,
        )
        if camera_projection_artifact is not None:
            artifacts["camera_projection_preview"] = camera_projection_artifact

        payload = {
            "mode": "hybrid_enhanced",
            "source_backend": helios_result.backend,
            "source_artifacts": {k: str(v) for k, v in artifacts.items()},
            "enhancements": {
                "camera_geometry": request.options.get("camera_geometry", "pinhole"),
                "distortion_model": request.options.get("camera_distortion", "brown-conrady"),
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
                "camera_projection_enabled": bool(request.options.get("camera_projection_enabled", True)),
            },
        }
        out_path = enhanced_output / "hybrid_physics.json"
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return SensorSimResult(
            backend="hybrid(helios+native_physics)",
            success=True,
            artifacts={
                **artifacts,
                "hybrid_physics": out_path,
            },
            metrics=metrics,
            message="Hybrid enhancement completed.",
        )

    def _camera_intrinsics_from_options(self, request: SensorSimRequest) -> CameraIntrinsics:
        data = request.options.get("camera_intrinsics", {})
        return CameraIntrinsics(
            fx=float(data.get("fx", 1200.0)),
            fy=float(data.get("fy", 1200.0)),
            cx=float(data.get("cx", 960.0)),
            cy=float(data.get("cy", 540.0)),
            width=int(data.get("width", 1920)),
            height=int(data.get("height", 1080)),
        )

    def _camera_distortion_from_options(
        self, request: SensorSimRequest
    ) -> BrownConradyDistortion:
        data = request.options.get("camera_distortion_coeffs", {})
        return BrownConradyDistortion(
            k1=float(data.get("k1", 0.0)),
            k2=float(data.get("k2", 0.0)),
            p1=float(data.get("p1", 0.0)),
            p2=float(data.get("p2", 0.0)),
            k3=float(data.get("k3", 0.0)),
        )

    def _camera_extrinsics_from_options(self, request: SensorSimRequest) -> CameraExtrinsics:
        data = request.options.get("camera_extrinsics", {})
        return CameraExtrinsics(
            tx=float(data.get("tx", 0.0)),
            ty=float(data.get("ty", 0.0)),
            tz=float(data.get("tz", 0.0)),
            roll_deg=float(data.get("roll_deg", 0.0)),
            pitch_deg=float(data.get("pitch_deg", 0.0)),
            yaw_deg=float(data.get("yaw_deg", 0.0)),
            enabled=bool(data.get("enabled", False)),
        )

    def _project_xyz_if_available(
        self,
        request: SensorSimRequest,
        artifacts: dict[str, Path],
        enhanced_output: Path,
        intrinsics: CameraIntrinsics,
        distortion: BrownConradyDistortion,
        extrinsics: CameraExtrinsics,
        metrics: dict[str, float],
    ) -> Path | None:
        if not bool(request.options.get("camera_projection_enabled", True)):
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
        camera_points = transform_points_world_to_camera(
            points_xyz=transformed_points,
            extrinsics=extrinsics,
        )

        projected = project_points_brown_conrady(
            points_xyz=camera_points,
            intrinsics=intrinsics,
            distortion=distortion,
            clamp_to_image=bool(request.options.get("camera_projection_clamp_to_image", True)),
        )

        metrics["camera_projection_input_count"] = float(len(points_xyz))
        metrics["camera_projection_output_count"] = float(len(projected))
        preview_count = int(request.options.get("camera_projection_preview_count", 20))
        preview = {
            "input_point_cloud": str(point_cloud),
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
                "enabled": extrinsics.enabled,
                "tx": extrinsics.tx,
                "ty": extrinsics.ty,
                "tz": extrinsics.tz,
                "roll_deg": extrinsics.roll_deg,
                "pitch_deg": extrinsics.pitch_deg,
                "yaw_deg": extrinsics.yaw_deg,
            },
            "preview_points_uvz": [
                {"u": u, "v": v, "z": z} for u, v, z in projected[:preview_count]
            ],
        }
        output_path = enhanced_output / "camera_projection_preview.json"
        output_path.write_text(json.dumps(preview, indent=2), encoding="utf-8")
        return output_path

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
