from __future__ import annotations

import json
from pathlib import Path

from hybrid_sensor_sim.backends.base import SensorBackend
from hybrid_sensor_sim.io.pointcloud_xyz import read_xyz_points
from hybrid_sensor_sim.physics.camera import (
    BrownConradyDistortion,
    CameraIntrinsics,
    project_points_brown_conrady,
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

        artifacts = {**helios_result.artifacts}
        metrics = dict(helios_result.metrics)
        camera_projection_artifact = self._project_xyz_if_available(
            request=request,
            artifacts=artifacts,
            enhanced_output=enhanced_output,
            intrinsics=intrinsics,
            distortion=distortion,
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

    def _project_xyz_if_available(
        self,
        request: SensorSimRequest,
        artifacts: dict[str, Path],
        enhanced_output: Path,
        intrinsics: CameraIntrinsics,
        distortion: BrownConradyDistortion,
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

        projected = project_points_brown_conrady(
            points_xyz=points_xyz,
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
            "preview_points_uvz": [
                {"u": u, "v": v, "z": z} for u, v, z in projected[:preview_count]
            ],
        }
        output_path = enhanced_output / "camera_projection_preview.json"
        output_path.write_text(json.dumps(preview, indent=2), encoding="utf-8")
        return output_path
