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
class CameraDepthConfig:
    min_m: float = 0.0
    max_m: float = 1000.0
    log_base: float = 10.0
    encoding_type: str = "LINEAR"
    bit_depth: int = 16

    def to_dict(self) -> dict[str, float | int | str]:
        return {
            "min": self.min_m,
            "max": self.max_m,
            "log_base": self.log_base,
            "type": self.encoding_type,
            "bit_depth": self.bit_depth,
        }


@dataclass(frozen=True)
class CameraSemanticConfig:
    class_version: str = "LEGACY"
    palette: str = "APPLIED_LEGACY"
    label_source: str = "ANNOTATION_OR_HEURISTIC"
    include_actor_id: bool = True
    include_component_id: bool = True
    include_material_class: bool = True
    include_material_uuid: bool = False
    include_base_map_element: bool = False
    include_procedural_map_element: bool = False
    include_lane_marking_id: bool = False

    def to_dict(self) -> dict[str, str | bool]:
        return {
            "class_version": self.class_version,
            "palette": self.palette,
            "label_source": self.label_source,
            "include_actor_id": self.include_actor_id,
            "include_component_id": self.include_component_id,
            "include_material_class": self.include_material_class,
            "include_material_uuid": self.include_material_uuid,
            "include_base_map_element": self.include_base_map_element,
            "include_procedural_map_element": self.include_procedural_map_element,
            "include_lane_marking_id": self.include_lane_marking_id,
        }


@dataclass(frozen=True)
class CameraFixedPatternNoiseConfig:
    dsnu: float = 0.0
    prnu: float = 0.0

    def to_dict(self) -> dict[str, float]:
        return {
            "dsnu": self.dsnu,
            "prnu": self.prnu,
        }


@dataclass(frozen=True)
class CameraImageChainConfig:
    enabled: bool = True
    bloom: float = 0.0
    shutter_speed_us: float = 6000.0
    iso: int = 100
    analog_gain: float = 1.0
    digital_gain: float = 1.0
    readout_noise: float = 0.0
    white_balance_kelvin: float = 6500.0
    gamma: float = 2.2
    seed: int = 0
    fixed_pattern_noise: CameraFixedPatternNoiseConfig = field(
        default_factory=CameraFixedPatternNoiseConfig
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "bloom": self.bloom,
            "shutter_speed_us": self.shutter_speed_us,
            "iso": self.iso,
            "analog_gain": self.analog_gain,
            "digital_gain": self.digital_gain,
            "readout_noise": self.readout_noise,
            "white_balance_kelvin": self.white_balance_kelvin,
            "gamma": self.gamma,
            "seed": self.seed,
            "fixed_pattern_noise": self.fixed_pattern_noise.to_dict(),
        }


@dataclass(frozen=True)
class CameraVignettingConfig:
    intensity: float = 0.0
    alpha: float = 1.0
    radius: float = 1.0

    def to_dict(self) -> dict[str, float]:
        return {
            "intensity": self.intensity,
            "alpha": self.alpha,
            "radius": self.radius,
        }


@dataclass(frozen=True)
class CameraLensConfig:
    lens_flare: float = 0.0
    spot_size: float = 0.0
    vignetting: CameraVignettingConfig = field(default_factory=CameraVignettingConfig)

    def to_dict(self) -> dict[str, Any]:
        return {
            "lens_flare": self.lens_flare,
            "spot_size": self.spot_size,
            "vignetting": self.vignetting.to_dict(),
        }


@dataclass(frozen=True)
class ScalarRangeConfig:
    min_value: float = 0.0
    max_value: float = 1.0

    def to_dict(self) -> dict[str, float]:
        return {
            "min": self.min_value,
            "max": self.max_value,
        }


@dataclass(frozen=True)
class RangeScalePointConfig:
    input_value: float
    output_value: float

    def to_dict(self) -> dict[str, float]:
        return {
            "input": self.input_value,
            "output": self.output_value,
        }


@dataclass(frozen=True)
class LidarIntensityConfig:
    units: str = "REFLECTIVITY"
    input_range: ScalarRangeConfig = field(default_factory=ScalarRangeConfig)
    output_scale: ScalarRangeConfig = field(
        default_factory=lambda: ScalarRangeConfig(min_value=0.0, max_value=255.0)
    )
    range_scale_map: list[RangeScalePointConfig] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "units": self.units,
            "range": self.input_range.to_dict(),
            "scale": self.output_scale.to_dict(),
            "range_scale_map": [point.to_dict() for point in self.range_scale_map],
        }


@dataclass(frozen=True)
class LidarPhysicsModelConfig:
    reflectivity_coefficient: float = 1.0
    atmospheric_attenuation_rate: float = 0.003
    ambient_power_dbw: float = -30.0
    signal_photon_scale: float = 10000.0
    ambient_photon_scale: float = 1000.0
    minimum_detection_snr_db: float = -20.0
    return_all_hits: bool = False

    def to_dict(self) -> dict[str, float | bool]:
        return {
            "reflectivity_coefficient": self.reflectivity_coefficient,
            "atmospheric_attenuation_rate": self.atmospheric_attenuation_rate,
            "ambient_power_dbw": self.ambient_power_dbw,
            "signal_photon_scale": self.signal_photon_scale,
            "ambient_photon_scale": self.ambient_photon_scale,
            "minimum_detection_snr_db": self.minimum_detection_snr_db,
            "return_all_hits": self.return_all_hits,
        }


@dataclass(frozen=True)
class LidarReturnModelConfig:
    mode: str = "SINGLE"
    max_returns: int = 1
    range_separation_m: float = 0.35
    signal_decay: float = 0.55
    minimum_secondary_snr_db: float = -8.0

    def to_dict(self) -> dict[str, float | int | str]:
        return {
            "mode": self.mode,
            "max_returns": self.max_returns,
            "range_separation_m": self.range_separation_m,
            "signal_decay": self.signal_decay,
            "minimum_secondary_snr_db": self.minimum_secondary_snr_db,
        }


@dataclass(frozen=True)
class LidarEnvironmentConfig:
    enable_ambient: bool = True
    fog_density: float = 0.0
    extinction_coefficient_scale: float = 0.05
    backscatter_scale: float = 0.0
    disable_backscatter: bool = False
    precipitation_rate: float = 0.0

    def to_dict(self) -> dict[str, float | bool]:
        return {
            "enable_ambient": self.enable_ambient,
            "fog_density": self.fog_density,
            "extinction_coefficient_scale": self.extinction_coefficient_scale,
            "backscatter_scale": self.backscatter_scale,
            "disable_backscatter": self.disable_backscatter,
            "precipitation_rate": self.precipitation_rate,
        }


@dataclass(frozen=True)
class LidarNoisePerformanceConfig:
    probability_false_alarm: float = 0.0
    probability_detection: float = 0.9
    calibration_target_range_m: float = 210.0
    calibration_target_reflectivity: float = 0.8

    def to_dict(self) -> dict[str, float]:
        return {
            "probability_false_alarm": self.probability_false_alarm,
            "probability_detection": self.probability_detection,
            "target_detectability": {
                "probability_detection": self.probability_detection,
                "target": {
                    "range": self.calibration_target_range_m,
                    "reflectivity": self.calibration_target_reflectivity,
                },
            },
        }


@dataclass(frozen=True)
class LidarRangeLossPointConfig:
    range_m: float
    loss_db: float

    def to_dict(self) -> dict[str, float]:
        return {
            "range": self.range_m,
            "loss": self.loss_db,
        }


@dataclass(frozen=True)
class LidarAngularPairConfig:
    az: float = 0.0
    el: float = 0.0

    def to_dict(self) -> dict[str, float]:
        return {
            "az": self.az,
            "el": self.el,
        }


@dataclass(frozen=True)
class LidarEmitterConfig:
    source_losses_db: list[float] = field(default_factory=list)
    global_source_loss_db: float = 0.0
    source_divergence: LidarAngularPairConfig = field(default_factory=LidarAngularPairConfig)
    source_variance: LidarAngularPairConfig = field(default_factory=LidarAngularPairConfig)
    peak_power_w: float = 1.0
    optical_loss: list[LidarRangeLossPointConfig] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_losses": list(self.source_losses_db),
            "global_source_loss": self.global_source_loss_db,
            "source_divergence": self.source_divergence.to_dict(),
            "source_variance": self.source_variance.to_dict(),
            "peak_power": self.peak_power_w,
            "optical_loss": [point.to_dict() for point in self.optical_loss],
        }


@dataclass(frozen=True)
class CameraRollingShutterConfig:
    enabled: bool = False
    col_delay_ns: float = 0.0
    col_readout_direction: str = "LEFT_TO_RIGHT"
    row_delay_ns: float = 0.0
    row_readout_direction: str = "TOP_TO_BOTTOM"
    num_time_steps: int = 1
    num_exposure_samples_per_pixel: int = 1

    def to_dict(self) -> dict[str, float | int | str | bool]:
        return {
            "enabled": self.enabled,
            "col_delay_ns": self.col_delay_ns,
            "col_readout_direction": self.col_readout_direction,
            "row_delay_ns": self.row_delay_ns,
            "row_readout_direction": self.row_readout_direction,
            "num_time_steps": self.num_time_steps,
            "num_exposure_samples_per_pixel": self.num_exposure_samples_per_pixel,
        }


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
    sensor_type: str = "VISIBLE"
    geometry_model: str = "pinhole"
    distortion_model: str = "brown-conrady"
    projection_enabled: bool = True
    trajectory_sweep_enabled: bool = False
    projection_clamp_to_image: bool = True
    intrinsics: CameraIntrinsicsConfig = field(default_factory=CameraIntrinsicsConfig)
    distortion_coeffs: CameraDistortionConfig = field(default_factory=CameraDistortionConfig)
    depth_params: CameraDepthConfig = field(default_factory=CameraDepthConfig)
    semantic_params: CameraSemanticConfig = field(default_factory=CameraSemanticConfig)
    image_chain: CameraImageChainConfig = field(default_factory=CameraImageChainConfig)
    lens_params: CameraLensConfig = field(default_factory=CameraLensConfig)
    rolling_shutter: CameraRollingShutterConfig = field(default_factory=CameraRollingShutterConfig)
    extrinsics: SensorExtrinsicsConfig = field(default_factory=SensorExtrinsicsConfig)
    behaviors: list[SensorBehaviorConfig] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "sensor_id": self.sensor_id,
            "attach_to_actor_id": self.attach_to_actor_id,
            "sensor_type": self.sensor_type,
            "geometry_model": self.geometry_model,
            "distortion_model": self.distortion_model,
            "projection_enabled": self.projection_enabled,
            "trajectory_sweep_enabled": self.trajectory_sweep_enabled,
            "projection_clamp_to_image": self.projection_clamp_to_image,
            "intrinsics": self.intrinsics.to_dict(),
            "distortion_coeffs": self.distortion_coeffs.to_dict(),
            "depth_params": self.depth_params.to_dict(),
            "semantic_params": self.semantic_params.to_dict(),
            "image_chain": self.image_chain.to_dict(),
            "lens_params": self.lens_params.to_dict(),
            "rolling_shutter": self.rolling_shutter.to_dict(),
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
    scan_frequency_hz: float = 10.0
    spin_direction: str = "CCW"
    source_angles_deg: list[float] = field(default_factory=list)
    source_angle_tolerance_deg: float = 1.0
    scan_field_azimuth_min_deg: float = -180.0
    scan_field_azimuth_max_deg: float = 180.0
    scan_field_elevation_min_deg: float = -30.0
    scan_field_elevation_max_deg: float = 30.0
    scan_field_azimuth_offset_deg: float = 0.0
    scan_field_elevation_offset_deg: float = 0.0
    scan_path_deg: list[float] = field(default_factory=list)
    multi_scan_path_deg: list[list[float]] = field(default_factory=list)
    range_min_m: float = 0.0
    range_max_m: float = 200.0
    intensity: LidarIntensityConfig = field(default_factory=LidarIntensityConfig)
    physics_model: LidarPhysicsModelConfig = field(default_factory=LidarPhysicsModelConfig)
    return_model: LidarReturnModelConfig = field(default_factory=LidarReturnModelConfig)
    environment_model: LidarEnvironmentConfig = field(default_factory=LidarEnvironmentConfig)
    noise_performance: LidarNoisePerformanceConfig = field(default_factory=LidarNoisePerformanceConfig)
    emitter_params: LidarEmitterConfig = field(default_factory=LidarEmitterConfig)
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
            "scan_frequency_hz": self.scan_frequency_hz,
            "spin_direction": self.spin_direction,
            "source_angles_deg": list(self.source_angles_deg),
            "source_angle_tolerance_deg": self.source_angle_tolerance_deg,
            "scan_field_deg": {
                "azimuth_min": self.scan_field_azimuth_min_deg,
                "azimuth_max": self.scan_field_azimuth_max_deg,
                "elevation_min": self.scan_field_elevation_min_deg,
                "elevation_max": self.scan_field_elevation_max_deg,
            },
            "scan_field_offset_deg": {
                "azimuth": self.scan_field_azimuth_offset_deg,
                "elevation": self.scan_field_elevation_offset_deg,
            },
            "scan_path_deg": list(self.scan_path_deg),
            "multi_scan_path_deg": [list(path) for path in self.multi_scan_path_deg],
            "range_m": {
                "min": self.range_min_m,
                "max": self.range_max_m,
            },
            "intensity": self.intensity.to_dict(),
            "physics_model": self.physics_model.to_dict(),
            "return_model": self.return_model.to_dict(),
            "environment_model": self.environment_model.to_dict(),
            "noise_performance": self.noise_performance.to_dict(),
            "emitter_params": self.emitter_params.to_dict(),
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


def _parse_camera_depth(options: Mapping[str, Any]) -> CameraDepthConfig:
    raw = _as_dict(options.get("camera_depth_params"))
    return CameraDepthConfig(
        min_m=_as_float(raw.get("min", options.get("camera_depth_min_m")), 0.0),
        max_m=_as_float(raw.get("max", options.get("camera_depth_max_m")), 1000.0),
        log_base=_as_float(raw.get("log_base", options.get("camera_depth_log_base")), 10.0),
        encoding_type=_as_str(raw.get("type", options.get("camera_depth_type")), "LINEAR").upper(),
        bit_depth=_as_int(raw.get("bit_depth", options.get("camera_depth_bit_depth")), 16),
    )


def _parse_camera_semantic(options: Mapping[str, Any]) -> CameraSemanticConfig:
    raw = _as_dict(options.get("camera_semantic_params"))
    class_version = _as_str(
        raw.get("class_version", options.get("camera_semantic_class_version")),
        "LEGACY",
    ).upper()
    palette_default = "APPLIED_GRANULAR" if class_version == "GRANULAR_SEGMENTATION" else "APPLIED_LEGACY"
    return CameraSemanticConfig(
        class_version=class_version,
        palette=_as_str(raw.get("palette", options.get("camera_semantic_palette")), palette_default).upper(),
        label_source=_as_str(
            raw.get("label_source", options.get("camera_semantic_label_source")),
            "ANNOTATION_OR_HEURISTIC",
        ).upper(),
        include_actor_id=_as_bool(
            raw.get("include_actor_id", options.get("camera_semantic_include_actor_id")),
            True,
        ),
        include_component_id=_as_bool(
            raw.get("include_component_id", options.get("camera_semantic_include_component_id")),
            True,
        ),
        include_material_class=_as_bool(
            raw.get("include_material_class", options.get("camera_semantic_include_material_class")),
            True,
        ),
        include_material_uuid=_as_bool(
            raw.get("include_material_uuid", options.get("camera_semantic_include_material_uuid")),
            False,
        ),
        include_base_map_element=_as_bool(
            raw.get("include_base_map_element", options.get("camera_semantic_include_base_map_element")),
            False,
        ),
        include_procedural_map_element=_as_bool(
            raw.get(
                "include_procedural_map_element",
                options.get("camera_semantic_include_procedural_map_element"),
            ),
            False,
        ),
        include_lane_marking_id=_as_bool(
            raw.get("include_lane_marking_id", options.get("camera_semantic_include_lane_marking_id")),
            False,
        ),
    )


def _parse_camera_image_chain(options: Mapping[str, Any]) -> CameraImageChainConfig:
    raw = _as_dict(options.get("camera_image_params"))
    raw_fpn = _as_dict(raw.get("fixed_pattern_noise"))
    return CameraImageChainConfig(
        enabled=_as_bool(raw.get("enabled", options.get("camera_image_chain_enabled")), True),
        bloom=_as_float(raw.get("bloom", options.get("camera_bloom")), 0.0),
        shutter_speed_us=_as_float(
            raw.get("shutter_speed_us", raw.get("shutter_speed", options.get("camera_shutter_speed_us"))),
            6000.0,
        ),
        iso=_as_int(raw.get("iso", options.get("camera_iso")), 100),
        analog_gain=_as_float(raw.get("analog_gain", options.get("camera_analog_gain")), 1.0),
        digital_gain=_as_float(raw.get("digital_gain", options.get("camera_digital_gain")), 1.0),
        readout_noise=_as_float(
            raw.get("readout_noise", options.get("camera_readout_noise")),
            0.0,
        ),
        white_balance_kelvin=_as_float(
            raw.get("white_balance_kelvin", raw.get("white_balance", options.get("camera_white_balance"))),
            6500.0,
        ),
        gamma=_as_float(raw.get("gamma", options.get("camera_gamma")), 2.2),
        seed=_as_int(raw.get("seed", options.get("camera_image_seed")), 0),
        fixed_pattern_noise=CameraFixedPatternNoiseConfig(
            dsnu=_as_float(raw_fpn.get("dsnu", options.get("camera_fixed_pattern_noise_dsnu")), 0.0),
            prnu=_as_float(raw_fpn.get("prnu", options.get("camera_fixed_pattern_noise_prnu")), 0.0),
        ),
    )


def _parse_camera_lens(options: Mapping[str, Any]) -> CameraLensConfig:
    raw = _as_dict(options.get("camera_lens_params"))
    raw_vignetting = _as_dict(raw.get("vignetting"))
    return CameraLensConfig(
        lens_flare=_as_float(raw.get("lens_flare", options.get("camera_lens_flare")), 0.0),
        spot_size=_as_float(raw.get("spot_size", options.get("camera_spot_size")), 0.0),
        vignetting=CameraVignettingConfig(
            intensity=_as_float(
                raw_vignetting.get("intensity", options.get("camera_vignetting_intensity")),
                0.0,
            ),
            alpha=_as_float(
                raw_vignetting.get("alpha", options.get("camera_vignetting_alpha")),
                1.0,
            ),
            radius=_as_float(
                raw_vignetting.get("radius", options.get("camera_vignetting_radius")),
                1.0,
            ),
        ),
    )


def _parse_camera_rolling_shutter(options: Mapping[str, Any]) -> CameraRollingShutterConfig:
    raw = _as_dict(options.get("camera_rolling_shutter"))
    col_delay_ns = _as_float(raw.get("col_delay_ns", options.get("camera_col_delay_ns")), 0.0)
    row_delay_ns = _as_float(raw.get("row_delay_ns", options.get("camera_row_delay_ns")), 0.0)
    enabled_default = col_delay_ns > 0.0 or row_delay_ns > 0.0
    return CameraRollingShutterConfig(
        enabled=_as_bool(raw.get("enabled", options.get("camera_rolling_shutter_enabled")), enabled_default),
        col_delay_ns=col_delay_ns,
        col_readout_direction=_as_str(
            raw.get("col_readout_direction", options.get("camera_col_readout_direction")),
            "LEFT_TO_RIGHT",
        ).upper(),
        row_delay_ns=row_delay_ns,
        row_readout_direction=_as_str(
            raw.get("row_readout_direction", options.get("camera_row_readout_direction")),
            "TOP_TO_BOTTOM",
        ).upper(),
        num_time_steps=_as_int(
            raw.get("num_time_steps", options.get("camera_num_time_steps")),
            1,
        ),
        num_exposure_samples_per_pixel=_as_int(
            raw.get(
                "num_exposure_samples_per_pixel",
                options.get("camera_num_exposure_samples_per_pixel"),
            ),
            1,
        ),
    )


def _parse_lidar_intensity(options: Mapping[str, Any]) -> LidarIntensityConfig:
    raw = _as_dict(options.get("lidar_intensity"))
    raw_range = _as_dict(raw.get("range"))
    raw_scale = _as_dict(raw.get("scale"))
    range_scale_map: list[RangeScalePointConfig] = []
    for point in _as_list(raw.get("range_scale_map", options.get("lidar_intensity_range_scale_map"))):
        point_raw = _as_dict(point)
        if not point_raw:
            continue
        range_scale_map.append(
            RangeScalePointConfig(
                input_value=_as_float(point_raw.get("input"), 0.0),
                output_value=_as_float(point_raw.get("output"), 0.0),
            )
        )
    return LidarIntensityConfig(
        units=_as_str(raw.get("units", options.get("lidar_intensity_units")), "REFLECTIVITY").upper(),
        input_range=ScalarRangeConfig(
            min_value=_as_float(raw_range.get("min", options.get("lidar_intensity_range_min")), 0.0),
            max_value=_as_float(raw_range.get("max", options.get("lidar_intensity_range_max")), 1.0),
        ),
        output_scale=ScalarRangeConfig(
            min_value=_as_float(raw_scale.get("min", options.get("lidar_intensity_scale_min")), 0.0),
            max_value=_as_float(raw_scale.get("max", options.get("lidar_intensity_scale_max")), 255.0),
        ),
        range_scale_map=range_scale_map,
    )


def _parse_lidar_physics_model(options: Mapping[str, Any]) -> LidarPhysicsModelConfig:
    raw = _as_dict(options.get("lidar_physics_model"))
    return LidarPhysicsModelConfig(
        reflectivity_coefficient=_as_float(
            raw.get("reflectivity_coefficient", options.get("lidar_reflectivity_coefficient")),
            1.0,
        ),
        atmospheric_attenuation_rate=_as_float(
            raw.get(
                "atmospheric_attenuation_rate",
                options.get("lidar_atmospheric_attenuation_rate"),
            ),
            0.003,
        ),
        ambient_power_dbw=_as_float(
            raw.get("ambient_power_dbw", options.get("lidar_ambient_power_dbw")),
            -30.0,
        ),
        signal_photon_scale=_as_float(
            raw.get("signal_photon_scale", options.get("lidar_signal_photon_scale")),
            10000.0,
        ),
        ambient_photon_scale=_as_float(
            raw.get("ambient_photon_scale", options.get("lidar_ambient_photon_scale")),
            1000.0,
        ),
        minimum_detection_snr_db=_as_float(
            raw.get("minimum_detection_snr_db", options.get("lidar_minimum_detection_snr_db")),
            -20.0,
        ),
        return_all_hits=_as_bool(
            raw.get("return_all_hits", options.get("lidar_return_all_hits")),
            False,
        ),
    )


def _parse_lidar_return_model(options: Mapping[str, Any]) -> LidarReturnModelConfig:
    raw = _as_dict(options.get("lidar_return_model"))
    max_returns = max(
        1,
        _as_int(raw.get("max_returns", options.get("lidar_max_returns")), 1),
    )
    mode = _as_str(raw.get("mode", options.get("lidar_return_mode")), "")
    if not mode:
        if max_returns <= 1:
            mode = "SINGLE"
        elif max_returns == 2:
            mode = "DUAL"
        else:
            mode = "MULTI"
    return LidarReturnModelConfig(
        mode=mode.upper(),
        max_returns=max_returns,
        range_separation_m=_as_float(
            raw.get("range_separation_m", options.get("lidar_return_range_separation_m")),
            0.35,
        ),
        signal_decay=_as_float(
            raw.get("signal_decay", options.get("lidar_return_signal_decay")),
            0.55,
        ),
        minimum_secondary_snr_db=_as_float(
            raw.get(
                "minimum_secondary_snr_db",
                options.get("lidar_minimum_secondary_snr_db"),
            ),
            -8.0,
        ),
    )


def _parse_lidar_environment_model(options: Mapping[str, Any]) -> LidarEnvironmentConfig:
    raw = _as_dict(options.get("lidar_environment_model"))
    return LidarEnvironmentConfig(
        enable_ambient=_as_bool(
            raw.get("enable_ambient", options.get("lidar_enable_ambient")),
            True,
        ),
        fog_density=_as_float(raw.get("fog_density", options.get("lidar_fog_density")), 0.0),
        extinction_coefficient_scale=_as_float(
            raw.get(
                "extinction_coefficient_scale",
                options.get("lidar_extinction_coefficient_scale"),
            ),
            0.05,
        ),
        backscatter_scale=_as_float(
            raw.get("backscatter_scale", options.get("lidar_backscatter_scale")),
            0.0,
        ),
        disable_backscatter=_as_bool(
            raw.get("disable_backscatter", options.get("lidar_disable_backscatter")),
            False,
        ),
        precipitation_rate=_as_float(
            raw.get("precipitation_rate", options.get("lidar_precipitation_rate")),
            0.0,
        ),
    )


def _parse_lidar_noise_performance(options: Mapping[str, Any]) -> LidarNoisePerformanceConfig:
    raw = _as_dict(options.get("lidar_noise_performance"))
    raw_target = _as_dict(raw.get("target_detectability"))
    raw_target_target = _as_dict(raw_target.get("target"))
    return LidarNoisePerformanceConfig(
        probability_false_alarm=_as_float(
            raw.get("probability_false_alarm", options.get("lidar_probability_false_alarm")),
            0.0,
        ),
        probability_detection=_as_float(
            raw_target.get("probability_detection", options.get("lidar_probability_detection")),
            0.9,
        ),
        calibration_target_range_m=_as_float(
            raw_target_target.get("range", options.get("lidar_calibration_target_range_m")),
            210.0,
        ),
        calibration_target_reflectivity=_as_float(
            raw_target_target.get(
                "reflectivity",
                options.get("lidar_calibration_target_reflectivity"),
            ),
            0.8,
        ),
    )


def _parse_lidar_angular_pair(raw: Mapping[str, Any], *, az_key: str, el_key: str) -> LidarAngularPairConfig:
    return LidarAngularPairConfig(
        az=_as_float(raw.get(az_key), 0.0),
        el=_as_float(raw.get(el_key), 0.0),
    )


def _parse_lidar_emitter_params(options: Mapping[str, Any]) -> LidarEmitterConfig:
    raw = _as_dict(options.get("lidar_emitter_params"))
    raw_divergence = _as_dict(raw.get("source_divergence"))
    raw_variance = _as_dict(raw.get("source_variance"))
    optical_loss: list[LidarRangeLossPointConfig] = []
    for point in _as_list(raw.get("optical_loss", options.get("lidar_optical_loss"))):
        point_raw = _as_dict(point)
        if not point_raw:
            continue
        optical_loss.append(
            LidarRangeLossPointConfig(
                range_m=_as_float(point_raw.get("range"), 0.0),
                loss_db=_as_float(point_raw.get("loss"), 0.0),
            )
        )
    return LidarEmitterConfig(
        source_losses_db=_parse_float_list(
            raw.get("source_losses", options.get("lidar_source_losses"))
        ),
        global_source_loss_db=_as_float(
            raw.get("global_source_loss", options.get("lidar_global_source_loss")),
            0.0,
        ),
        source_divergence=_parse_lidar_angular_pair(
            {
                "az": raw_divergence.get("az", options.get("lidar_source_divergence_az")),
                "el": raw_divergence.get("el", options.get("lidar_source_divergence_el")),
            },
            az_key="az",
            el_key="el",
        ),
        source_variance=_parse_lidar_angular_pair(
            {
                "az": raw_variance.get("az", options.get("lidar_source_variance_az")),
                "el": raw_variance.get("el", options.get("lidar_source_variance_el")),
            },
            az_key="az",
            el_key="el",
        ),
        peak_power_w=_as_float(raw.get("peak_power", options.get("lidar_peak_power")), 1.0),
        optical_loss=optical_loss,
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


def _parse_float_list(raw: Any) -> list[float]:
    values: list[float] = []
    for item in _as_list(raw):
        values.append(_as_float(item, 0.0))
    return values


def _parse_nested_float_lists(raw: Any) -> list[list[float]]:
    values: list[list[float]] = []
    for item in _as_list(raw):
        if isinstance(item, dict):
            values.append(_parse_float_list(item.get("angles_deg")))
            continue
        values.append(_parse_float_list(item))
    return [entry for entry in values if entry]


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
            sensor_type=_as_str(data.get("camera_sensor_type"), "VISIBLE").upper(),
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
            depth_params=_parse_camera_depth(data),
            semantic_params=_parse_camera_semantic(data),
            image_chain=_parse_camera_image_chain(data),
            lens_params=_parse_camera_lens(data),
            rolling_shutter=_parse_camera_rolling_shutter(data),
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
            scan_type=_as_str(data.get("lidar_scan_type"), "spin").upper(),
            scan_frequency_hz=_as_float(data.get("lidar_scan_frequency_hz"), 10.0),
            spin_direction=_as_str(data.get("lidar_spin_direction"), "CCW").upper(),
            source_angles_deg=_parse_float_list(
                data.get("lidar_source_angles_deg", data.get("lidar_source_angles"))
            ),
            source_angle_tolerance_deg=_as_float(
                data.get("lidar_source_angle_tolerance_deg"),
                1.0,
            ),
            scan_field_azimuth_min_deg=_as_float(
                _as_dict(data.get("lidar_scan_field")).get("azimuth_min_deg"),
                _as_float(data.get("lidar_scan_field_azimuth_min_deg"), -180.0),
            ),
            scan_field_azimuth_max_deg=_as_float(
                _as_dict(data.get("lidar_scan_field")).get("azimuth_max_deg"),
                _as_float(data.get("lidar_scan_field_azimuth_max_deg"), 180.0),
            ),
            scan_field_elevation_min_deg=_as_float(
                _as_dict(data.get("lidar_scan_field")).get("elevation_min_deg"),
                _as_float(data.get("lidar_scan_field_elevation_min_deg"), -30.0),
            ),
            scan_field_elevation_max_deg=_as_float(
                _as_dict(data.get("lidar_scan_field")).get("elevation_max_deg"),
                _as_float(data.get("lidar_scan_field_elevation_max_deg"), 30.0),
            ),
            scan_field_azimuth_offset_deg=_as_float(
                _as_dict(data.get("lidar_scan_field_offset")).get("azimuth_deg"),
                _as_float(data.get("lidar_scan_field_azimuth_offset_deg"), 0.0),
            ),
            scan_field_elevation_offset_deg=_as_float(
                _as_dict(data.get("lidar_scan_field_offset")).get("elevation_deg"),
                _as_float(data.get("lidar_scan_field_elevation_offset_deg"), 0.0),
            ),
            scan_path_deg=_parse_float_list(data.get("lidar_scan_path_deg", data.get("lidar_scan_path"))),
            multi_scan_path_deg=_parse_nested_float_lists(
                data.get("lidar_multi_scan_path_deg", data.get("lidar_multi_scan_path"))
            ),
            range_min_m=_as_float(data.get("lidar_range_min_m"), 0.0),
            range_max_m=_as_float(data.get("lidar_range_max_m"), 200.0),
            intensity=_parse_lidar_intensity(data),
            physics_model=_parse_lidar_physics_model(data),
            return_model=_parse_lidar_return_model(data),
            environment_model=_parse_lidar_environment_model(data),
            noise_performance=_parse_lidar_noise_performance(data),
            emitter_params=_parse_lidar_emitter_params(data),
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
