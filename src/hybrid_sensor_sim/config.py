from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from hybrid_sensor_sim.physics.camera import (
    BrownConradyDistortion,
    CameraExtrinsics,
    CameraIntrinsics,
)


CONFIG_SCHEMA_VERSION = "1.0"


def _as_dict(raw: Any) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, dict) else {}


def _as_list(raw: Any) -> list[Any]:
    return list(raw) if isinstance(raw, list) else []


def _as_str(raw: Any, default: str) -> str:
    if raw is None:
        return default
    if isinstance(raw, str):
        value = raw.strip()
        return value if value else default
    return str(raw)


def _as_float(raw: Any, default: float) -> float:
    if raw is None:
        return default
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return float(raw)
    if isinstance(raw, str):
        try:
            return float(raw.strip())
        except ValueError:
            return default
    return default


def _as_int(raw: Any, default: int) -> int:
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


def _as_bool(raw: Any, default: bool) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)
    if isinstance(raw, str):
        lowered = raw.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return default


@dataclass(frozen=True)
class Vector3Config:
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0

    def to_dict(self) -> dict[str, float]:
        return {"x": self.x, "y": self.y, "z": self.z}


@dataclass(frozen=True)
class SensorBehaviorConfig:
    kind: str
    target_actor_id: str | None = None
    target_center_offset: Vector3Config | None = None
    tx: float = 0.0
    ty: float = 0.0
    tz: float = 0.0
    rx: float = 0.0
    ry: float = 0.0
    rz: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        if self.kind == "point_at":
            payload: dict[str, Any] = {}
            if self.target_actor_id is not None:
                payload["id"] = self.target_actor_id
            if self.target_center_offset is not None:
                payload["target_center_offset"] = self.target_center_offset.to_dict()
            return {"point_at": payload}
        if self.kind == "continuous_motion":
            return {
                "continuous_motion": {
                    "tx": self.tx,
                    "ty": self.ty,
                    "tz": self.tz,
                    "rx": self.rx,
                    "ry": self.ry,
                    "rz": self.rz,
                }
            }
        return {self.kind: {}}


@dataclass(frozen=True)
class SensorExtrinsicsConfig:
    enabled: bool = False
    tx: float = 0.0
    ty: float = 0.0
    tz: float = 0.0
    roll_deg: float = 0.0
    pitch_deg: float = 0.0
    yaw_deg: float = 0.0

    def to_dict(self) -> dict[str, float | bool]:
        return {
            "enabled": self.enabled,
            "tx": self.tx,
            "ty": self.ty,
            "tz": self.tz,
            "roll_deg": self.roll_deg,
            "pitch_deg": self.pitch_deg,
            "yaw_deg": self.yaw_deg,
        }

    def to_camera_extrinsics(self) -> CameraExtrinsics:
        return CameraExtrinsics(
            tx=self.tx,
            ty=self.ty,
            tz=self.tz,
            roll_deg=self.roll_deg,
            pitch_deg=self.pitch_deg,
            yaw_deg=self.yaw_deg,
            enabled=self.enabled,
        )


@dataclass(frozen=True)
class CameraIntrinsicsConfig:
    fx: float = 1200.0
    fy: float = 1200.0
    cx: float = 960.0
    cy: float = 540.0
    width: int = 1920
    height: int = 1080

    def to_dict(self) -> dict[str, float | int]:
        return {
            "fx": self.fx,
            "fy": self.fy,
            "cx": self.cx,
            "cy": self.cy,
            "width": self.width,
            "height": self.height,
        }

    def to_camera_intrinsics(self) -> CameraIntrinsics:
        return CameraIntrinsics(
            fx=self.fx,
            fy=self.fy,
            cx=self.cx,
            cy=self.cy,
            width=self.width,
            height=self.height,
        )


@dataclass(frozen=True)
class CameraDistortionConfig:
    k1: float = 0.0
    k2: float = 0.0
    p1: float = 0.0
    p2: float = 0.0
    k3: float = 0.0

    def to_dict(self) -> dict[str, float]:
        return {
            "k1": self.k1,
            "k2": self.k2,
            "p1": self.p1,
            "p2": self.p2,
            "k3": self.k3,
        }

    def to_brown_conrady(self) -> BrownConradyDistortion:
        return BrownConradyDistortion(
            k1=self.k1,
            k2=self.k2,
            p1=self.p1,
            p2=self.p2,
            k3=self.k3,
        )


@dataclass(frozen=True)
class RendererConfig:
    bridge_enabled: bool = False
    backend: str = "none"
    execute: bool = False
    map_name: str = ""
    weather: str = "default"
    scene_seed: int = 0
    ego_actor_id: str = "ego"

    def to_dict(self) -> dict[str, Any]:
        return {
            "bridge_enabled": self.bridge_enabled,
            "backend": self.backend,
            "execute": self.execute,
            "map": self.map_name,
            "weather": self.weather,
            "scene_seed": self.scene_seed,
            "ego_actor_id": self.ego_actor_id,
        }


@dataclass(frozen=True)
class CameraSensorConfig:
    sensor_id: str = "camera_front"
    attach_to_actor_id: str = "ego"
    geometry_model: str = "pinhole"
    distortion_model: str = "brown-conrady"
    projection_enabled: bool = True
    trajectory_sweep_enabled: bool = False
    projection_clamp_to_image: bool = True
    intrinsics: CameraIntrinsicsConfig = field(default_factory=CameraIntrinsicsConfig)
    distortion_coeffs: CameraDistortionConfig = field(default_factory=CameraDistortionConfig)
    extrinsics: SensorExtrinsicsConfig = field(default_factory=SensorExtrinsicsConfig)
    behaviors: list[SensorBehaviorConfig] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "sensor_id": self.sensor_id,
            "attach_to_actor_id": self.attach_to_actor_id,
            "geometry_model": self.geometry_model,
            "distortion_model": self.distortion_model,
            "projection_enabled": self.projection_enabled,
            "trajectory_sweep_enabled": self.trajectory_sweep_enabled,
            "projection_clamp_to_image": self.projection_clamp_to_image,
            "intrinsics": self.intrinsics.to_dict(),
            "distortion_coeffs": self.distortion_coeffs.to_dict(),
            "extrinsics": self.extrinsics.to_dict(),
            "behaviors": [behavior.to_dict() for behavior in self.behaviors],
        }


@dataclass(frozen=True)
class LidarSensorConfig:
    sensor_id: str = "lidar_top"
    attach_to_actor_id: str = "ego"
    postprocess_enabled: bool = True
    trajectory_sweep_enabled: bool = False
    motion_compensation_enabled: bool = True
    motion_compensation_mode: str = "linear"
    scan_duration_s: float = 0.1
    noise_model: str = "gaussian"
    noise_stddev_m: float = 0.02
    dropout_probability: float = 0.01
    scan_type: str = "spin"
    range_min_m: float = 0.0
    range_max_m: float = 200.0
    extrinsics: SensorExtrinsicsConfig = field(default_factory=SensorExtrinsicsConfig)
    behaviors: list[SensorBehaviorConfig] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "sensor_id": self.sensor_id,
            "attach_to_actor_id": self.attach_to_actor_id,
            "postprocess_enabled": self.postprocess_enabled,
            "trajectory_sweep_enabled": self.trajectory_sweep_enabled,
            "motion_compensation_enabled": self.motion_compensation_enabled,
            "motion_compensation_mode": self.motion_compensation_mode,
            "scan_duration_s": self.scan_duration_s,
            "noise_model": self.noise_model,
            "noise_stddev_m": self.noise_stddev_m,
            "dropout_probability": self.dropout_probability,
            "scan_type": self.scan_type,
            "range_m": {
                "min": self.range_min_m,
                "max": self.range_max_m,
            },
            "extrinsics": self.extrinsics.to_dict(),
            "behaviors": [behavior.to_dict() for behavior in self.behaviors],
        }


@dataclass(frozen=True)
class RadarSensorConfig:
    sensor_id: str = "radar_front"
    attach_to_actor_id: str = "ego"
    postprocess_enabled: bool = True
    trajectory_sweep_enabled: bool = False
    clutter_model: str = "basic"
    max_targets: int = 64
    range_min_m: float = 0.5
    range_max_m: float = 200.0
    horizontal_fov_deg: float = 120.0
    vertical_fov_deg: float = 30.0
    angle_noise_stddev_deg: float = 0.1
    range_noise_stddev_m: float = 0.05
    velocity_noise_stddev_mps: float = 0.1
    false_target_count: int = 2
    extrinsics: SensorExtrinsicsConfig = field(default_factory=SensorExtrinsicsConfig)
    behaviors: list[SensorBehaviorConfig] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "sensor_id": self.sensor_id,
            "attach_to_actor_id": self.attach_to_actor_id,
            "postprocess_enabled": self.postprocess_enabled,
            "trajectory_sweep_enabled": self.trajectory_sweep_enabled,
            "clutter_model": self.clutter_model,
            "max_targets": self.max_targets,
            "range_m": {
                "min": self.range_min_m,
                "max": self.range_max_m,
            },
            "field_of_view_deg": {
                "horizontal": self.horizontal_fov_deg,
                "vertical": self.vertical_fov_deg,
            },
            "noise": {
                "angle_stddev_deg": self.angle_noise_stddev_deg,
                "range_stddev_m": self.range_noise_stddev_m,
                "velocity_stddev_mps": self.velocity_noise_stddev_mps,
            },
            "false_target_count": self.false_target_count,
            "extrinsics": self.extrinsics.to_dict(),
            "behaviors": [behavior.to_dict() for behavior in self.behaviors],
        }


@dataclass(frozen=True)
class SensorSimConfig:
    schema_version: str = CONFIG_SCHEMA_VERSION
    sensor_profile: str = "default"
    renderer: RendererConfig = field(default_factory=RendererConfig)
    camera: CameraSensorConfig = field(default_factory=CameraSensorConfig)
    lidar: LidarSensorConfig = field(default_factory=LidarSensorConfig)
    radar: RadarSensorConfig = field(default_factory=RadarSensorConfig)

    def to_manifest(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "sensor_profile": self.sensor_profile,
            "renderer": self.renderer.to_dict(),
            "sensors": {
                "camera": self.camera.to_dict(),
                "lidar": self.lidar.to_dict(),
                "radar": self.radar.to_dict(),
            },
        }


def _parse_extrinsics(raw: Mapping[str, Any]) -> SensorExtrinsicsConfig:
    return SensorExtrinsicsConfig(
        enabled=_as_bool(raw.get("enabled"), False),
        tx=_as_float(raw.get("tx"), 0.0),
        ty=_as_float(raw.get("ty"), 0.0),
        tz=_as_float(raw.get("tz"), 0.0),
        roll_deg=_as_float(raw.get("roll_deg"), 0.0),
        pitch_deg=_as_float(raw.get("pitch_deg"), 0.0),
        yaw_deg=_as_float(raw.get("yaw_deg"), 0.0),
    )


def _parse_camera_intrinsics(raw: Mapping[str, Any]) -> CameraIntrinsicsConfig:
    return CameraIntrinsicsConfig(
        fx=_as_float(raw.get("fx"), 1200.0),
        fy=_as_float(raw.get("fy"), 1200.0),
        cx=_as_float(raw.get("cx"), 960.0),
        cy=_as_float(raw.get("cy"), 540.0),
        width=_as_int(raw.get("width"), 1920),
        height=_as_int(raw.get("height"), 1080),
    )


def _parse_camera_distortion(raw: Mapping[str, Any]) -> CameraDistortionConfig:
    return CameraDistortionConfig(
        k1=_as_float(raw.get("k1"), 0.0),
        k2=_as_float(raw.get("k2"), 0.0),
        p1=_as_float(raw.get("p1"), 0.0),
        p2=_as_float(raw.get("p2"), 0.0),
        k3=_as_float(raw.get("k3"), 0.0),
    )


def _parse_behaviors(options: Mapping[str, Any], sensor_name: str) -> list[SensorBehaviorConfig]:
    nested = _as_dict(options.get("sensor_behaviors")).get(sensor_name)
    raw_behaviors = options.get(f"{sensor_name}_behaviors", nested)
    behaviors: list[SensorBehaviorConfig] = []
    for raw_behavior in _as_list(raw_behaviors):
        if not isinstance(raw_behavior, dict):
            continue
        if "point_at" in raw_behavior and isinstance(raw_behavior["point_at"], dict):
            payload = raw_behavior["point_at"]
            offset_raw = _as_dict(payload.get("target_center_offset"))
            offset = None
            if offset_raw:
                offset = Vector3Config(
                    x=_as_float(offset_raw.get("x"), 0.0),
                    y=_as_float(offset_raw.get("y"), 0.0),
                    z=_as_float(offset_raw.get("z"), 0.0),
                )
            behaviors.append(
                SensorBehaviorConfig(
                    kind="point_at",
                    target_actor_id=(
                        _as_str(payload.get("id"), "")
                        if payload.get("id") is not None
                        else None
                    ),
                    target_center_offset=offset,
                )
            )
            continue
        if "continuous_motion" in raw_behavior and isinstance(raw_behavior["continuous_motion"], dict):
            payload = raw_behavior["continuous_motion"]
            behaviors.append(
                SensorBehaviorConfig(
                    kind="continuous_motion",
                    tx=_as_float(payload.get("tx"), 0.0),
                    ty=_as_float(payload.get("ty"), 0.0),
                    tz=_as_float(payload.get("tz"), 0.0),
                    rx=_as_float(payload.get("rx"), 0.0),
                    ry=_as_float(payload.get("ry"), 0.0),
                    rz=_as_float(payload.get("rz"), 0.0),
                )
            )
    return behaviors


def build_sensor_sim_config(
    *,
    sensor_profile: str = "default",
    options: Mapping[str, Any] | None = None,
) -> SensorSimConfig:
    data = options if options is not None else {}
    ego_actor_id = _as_str(data.get("renderer_ego_actor_id"), "ego")

    return SensorSimConfig(
        sensor_profile=sensor_profile,
        renderer=RendererConfig(
            bridge_enabled=_as_bool(data.get("renderer_bridge_enabled"), False),
            backend=_as_str(data.get("renderer_backend"), "none"),
            execute=_as_bool(data.get("renderer_execute"), False),
            map_name=_as_str(data.get("renderer_map"), ""),
            weather=_as_str(data.get("renderer_weather"), "default"),
            scene_seed=_as_int(data.get("renderer_scene_seed"), 0),
            ego_actor_id=ego_actor_id,
        ),
        camera=CameraSensorConfig(
            sensor_id=_as_str(data.get("renderer_camera_sensor_id"), "camera_front"),
            attach_to_actor_id=ego_actor_id,
            geometry_model=_as_str(data.get("camera_geometry"), "pinhole"),
            distortion_model=_as_str(data.get("camera_distortion"), "brown-conrady"),
            projection_enabled=_as_bool(data.get("camera_projection_enabled"), True),
            trajectory_sweep_enabled=_as_bool(
                data.get("camera_projection_trajectory_sweep_enabled"),
                False,
            ),
            projection_clamp_to_image=_as_bool(
                data.get("camera_projection_clamp_to_image"),
                True,
            ),
            intrinsics=_parse_camera_intrinsics(_as_dict(data.get("camera_intrinsics"))),
            distortion_coeffs=_parse_camera_distortion(
                _as_dict(data.get("camera_distortion_coeffs"))
            ),
            extrinsics=_parse_extrinsics(_as_dict(data.get("camera_extrinsics"))),
            behaviors=_parse_behaviors(data, "camera"),
        ),
        lidar=LidarSensorConfig(
            sensor_id=_as_str(data.get("renderer_lidar_sensor_id"), "lidar_top"),
            attach_to_actor_id=ego_actor_id,
            postprocess_enabled=_as_bool(data.get("lidar_postprocess_enabled"), True),
            trajectory_sweep_enabled=_as_bool(
                data.get("lidar_trajectory_sweep_enabled"),
                False,
            ),
            motion_compensation_enabled=_as_bool(
                data.get("lidar_motion_compensation_enabled"),
                True,
            ),
            motion_compensation_mode=_as_str(
                data.get("lidar_motion_compensation_mode"),
                "linear",
            ),
            scan_duration_s=_as_float(data.get("lidar_scan_duration_s"), 0.1),
            noise_model=_as_str(data.get("lidar_noise"), "gaussian"),
            noise_stddev_m=_as_float(data.get("lidar_noise_stddev_m"), 0.02),
            dropout_probability=_as_float(data.get("lidar_dropout_probability"), 0.01),
            scan_type=_as_str(data.get("lidar_scan_type"), "spin"),
            range_min_m=_as_float(data.get("lidar_range_min_m"), 0.0),
            range_max_m=_as_float(data.get("lidar_range_max_m"), 200.0),
            extrinsics=_parse_extrinsics(_as_dict(data.get("lidar_extrinsics"))),
            behaviors=_parse_behaviors(data, "lidar"),
        ),
        radar=RadarSensorConfig(
            sensor_id=_as_str(data.get("renderer_radar_sensor_id"), "radar_front"),
            attach_to_actor_id=ego_actor_id,
            postprocess_enabled=_as_bool(data.get("radar_postprocess_enabled"), True),
            trajectory_sweep_enabled=_as_bool(
                data.get("radar_trajectory_sweep_enabled"),
                False,
            ),
            clutter_model=_as_str(data.get("radar_clutter"), "basic"),
            max_targets=_as_int(data.get("radar_max_targets"), 64),
            range_min_m=_as_float(data.get("radar_range_min_m"), 0.5),
            range_max_m=_as_float(data.get("radar_range_max_m"), 200.0),
            horizontal_fov_deg=_as_float(data.get("radar_horizontal_fov_deg"), 120.0),
            vertical_fov_deg=_as_float(data.get("radar_vertical_fov_deg"), 30.0),
            angle_noise_stddev_deg=_as_float(
                data.get("radar_angle_noise_stddev_deg"),
                0.1,
            ),
            range_noise_stddev_m=_as_float(data.get("radar_range_noise_stddev_m"), 0.05),
            velocity_noise_stddev_mps=_as_float(
                data.get("radar_velocity_noise_stddev_mps"),
                0.1,
            ),
            false_target_count=_as_int(data.get("radar_false_target_count"), 2),
            extrinsics=_parse_extrinsics(_as_dict(data.get("radar_extrinsics"))),
            behaviors=_parse_behaviors(data, "radar"),
        ),
    )
