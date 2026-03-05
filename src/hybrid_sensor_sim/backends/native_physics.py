from __future__ import annotations

import json
from pathlib import Path

from hybrid_sensor_sim.backends.base import SensorBackend
from hybrid_sensor_sim.types import SensorSimRequest, SensorSimResult


class NativePhysicsBackend(SensorBackend):
    def name(self) -> str:
        return "native_physics"

    def simulate(self, request: SensorSimRequest) -> SensorSimResult:
        native_output = request.output_dir / "native_only"
        native_output.mkdir(parents=True, exist_ok=True)
        payload = {
            "mode": "native_only",
            "scenario": str(request.scenario_path),
            "sensor_profile": request.sensor_profile,
            "physics": {
                "camera_distortion": request.options.get("camera_distortion", "brown-conrady"),
                "lidar_noise": request.options.get("lidar_noise", "gaussian"),
                "radar_clutter": request.options.get("radar_clutter", "basic"),
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
        payload = {
            "mode": "hybrid_enhanced",
            "source_backend": helios_result.backend,
            "source_artifacts": {k: str(v) for k, v in helios_result.artifacts.items()},
            "enhancements": {
                "camera_geometry": request.options.get("camera_geometry", "pinhole"),
                "distortion_model": request.options.get("camera_distortion", "brown-conrady"),
                "motion_compensation": request.options.get("motion_compensation", True),
            },
        }
        out_path = enhanced_output / "hybrid_physics.json"
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return SensorSimResult(
            backend="hybrid(helios+native_physics)",
            success=True,
            artifacts={
                **helios_result.artifacts,
                "hybrid_physics": out_path,
            },
            message="Hybrid enhancement completed.",
        )

