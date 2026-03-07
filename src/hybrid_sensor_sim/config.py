from __future__ import annotations

from dataclasses import dataclass, field
from math import pi
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
    selection_mode: str = "FIRST"
    range_discrimination_m: float = 0.0
    range_separation_m: float = 0.35
    signal_decay: float = 0.55
    minimum_secondary_snr_db: float = -8.0

    def to_dict(self) -> dict[str, float | int | str]:
        return {
            "mode": self.mode,
            "max_returns": self.max_returns,
            "selection_mode": self.selection_mode,
            "range_discrimination": self.range_discrimination_m,
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
    precipitation_type: str = "RAIN"
    particle_density_scale: float = 1.0
    particle_diameter_mm: float = 0.0
    terminal_velocity_mps: float = 0.0
    particle_reflectivity: float = 0.0
    backscatter_jitter: float = 0.1
    field_seed: int = 0

    def to_dict(self) -> dict[str, float | bool | int | str]:
        return {
            "enable_ambient": self.enable_ambient,
            "fog_density": self.fog_density,
            "extinction_coefficient_scale": self.extinction_coefficient_scale,
            "backscatter_scale": self.backscatter_scale,
            "disable_backscatter": self.disable_backscatter,
            "precipitation_rate": self.precipitation_rate,
            "precipitation_type": self.precipitation_type,
            "particle_density_scale": self.particle_density_scale,
            "particle_diameter_mm": self.particle_diameter_mm,
            "terminal_velocity_mps": self.terminal_velocity_mps,
            "particle_reflectivity": self.particle_reflectivity,
            "backscatter_jitter": self.backscatter_jitter,
            "field_seed": self.field_seed,
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
class LidarProfileDataConfig:
    file_uri: str = ""
    half_angle_rad: float = 0.0
    scale: float = 0.0
    pattern: str = "NONE"
    sample_count: int = 0
    sidelobe_gain: float = 0.05

    def to_dict(self) -> dict[str, float | int | str]:
        return {
            "file_uri": self.file_uri,
            "half_angle": self.half_angle_rad,
            "scale": self.scale,
            "pattern": self.pattern,
            "sample_count": self.sample_count,
            "sidelobe_gain": self.sidelobe_gain,
        }


@dataclass(frozen=True)
class LidarChannelProfileConfig:
    enabled: bool = False
    profile_data: LidarProfileDataConfig = field(default_factory=LidarProfileDataConfig)

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "profile_data": self.profile_data.to_dict(),
        }


@dataclass(frozen=True)
class LidarMultipathConfig:
    enabled: bool = False
    mode: str = "HYBRID"
    max_paths: int = 2
    path_signal_decay: float = 0.3
    minimum_path_snr_db: float = -8.0
    max_extra_path_length_m: float = 80.0
    ground_plane_height_m: float = -1.5
    ground_reflectivity: float = 0.35
    wall_plane_x_m: float = 25.0
    wall_reflectivity: float = 0.25

    def to_dict(self) -> dict[str, float | int | str | bool]:
        return {
            "enabled": self.enabled,
            "mode": self.mode,
            "max_paths": self.max_paths,
            "path_signal_decay": self.path_signal_decay,
            "minimum_path_snr_db": self.minimum_path_snr_db,
            "max_extra_path_length_m": self.max_extra_path_length_m,
            "ground_plane_height_m": self.ground_plane_height_m,
            "ground_reflectivity": self.ground_reflectivity,
            "wall_plane_x_m": self.wall_plane_x_m,
            "wall_reflectivity": self.wall_reflectivity,
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
    channel_profile: LidarChannelProfileConfig = field(default_factory=LidarChannelProfileConfig)
    multipath_model: LidarMultipathConfig = field(default_factory=LidarMultipathConfig)
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
            "channel_profile": self.channel_profile.to_dict(),
            "multipath_model": self.multipath_model.to_dict(),
            "extrinsics": self.extrinsics.to_dict(),
            "behaviors": [behavior.to_dict() for behavior in self.behaviors],
        }


@dataclass(frozen=True)
class RadarAngularConfig:
    az_deg: float = 0.0
    el_deg: float = 0.0

    def to_dict(self) -> dict[str, float]:
        return {"az": self.az_deg, "el": self.el_deg}


@dataclass(frozen=True)
class RadarTargetDetectabilityConfig:
    probability_detection: float = 0.99
    calibration_target_range_m: float = 100.0
    calibration_target_rcs_dbsm: float = 0.0

    def to_dict(self) -> dict[str, float | dict[str, float]]:
        return {
            "probability_detection": self.probability_detection,
            "target": {
                "range": self.calibration_target_range_m,
                "radar_cross_section": self.calibration_target_rcs_dbsm,
            },
        }


@dataclass(frozen=True)
class RadarDetectorConfig:
    noise_variance_dbw: float = -90.0
    minimum_snr_db: float = -10.0
    no_additive_noise: bool = False
    max_detections: int = 0
    probability_false_alarm: float = 0.0
    target_detectability: RadarTargetDetectabilityConfig = field(
        default_factory=RadarTargetDetectabilityConfig
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "noise_variance_dbw": self.noise_variance_dbw,
            "minimum_snr_db": self.minimum_snr_db,
            "no_additive_noise": self.no_additive_noise,
            "max_detections": self.max_detections,
            "noise_performance": {
                "probability_false_alarm": self.probability_false_alarm,
                "target_detectability": self.target_detectability.to_dict(),
            },
        }


@dataclass(frozen=True)
class RadarAccuracyConfig:
    max_deviation: float = 0.0
    num_sigma: float = 1.0

    def to_dict(self) -> dict[str, float]:
        return {
            "max_deviation": self.max_deviation,
            "num_sigma": self.num_sigma,
        }


@dataclass(frozen=True)
class RadarAccuracyRegionConfig:
    range_min_m: float = 0.0
    range_max_m: float = 1.0e9
    azimuth_min_deg: float = -180.0
    azimuth_max_deg: float = 180.0
    elevation_min_deg: float = -180.0
    elevation_max_deg: float = 180.0
    max_deviation: float = 0.0
    num_sigma: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "range": {"min": self.range_min_m, "max": self.range_max_m},
            "azimuth_deg": {"min": self.azimuth_min_deg, "max": self.azimuth_max_deg},
            "elevation_deg": {"min": self.elevation_min_deg, "max": self.elevation_max_deg},
            "max_deviation": self.max_deviation,
            "num_sigma": self.num_sigma,
        }


@dataclass(frozen=True)
class RadarEstimatorConfig:
    range_accuracy: RadarAccuracyConfig = field(default_factory=RadarAccuracyConfig)
    velocity_accuracy: RadarAccuracyConfig = field(default_factory=RadarAccuracyConfig)
    azimuth_accuracy: RadarAccuracyConfig = field(default_factory=RadarAccuracyConfig)
    elevation_accuracy: RadarAccuracyConfig = field(default_factory=RadarAccuracyConfig)
    range_accuracy_regions: list[RadarAccuracyRegionConfig] = field(default_factory=list)
    velocity_accuracy_regions: list[RadarAccuracyRegionConfig] = field(default_factory=list)
    azimuth_accuracy_regions: list[RadarAccuracyRegionConfig] = field(default_factory=list)
    elevation_accuracy_regions: list[RadarAccuracyRegionConfig] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "range_accuracy": self.range_accuracy.to_dict(),
            "velocity_accuracy": self.velocity_accuracy.to_dict(),
            "azimuth_accuracy": self.azimuth_accuracy.to_dict(),
            "elevation_accuracy": self.elevation_accuracy.to_dict(),
            "range_accuracy_regions": [region.to_dict() for region in self.range_accuracy_regions],
            "velocity_accuracy_regions": [region.to_dict() for region in self.velocity_accuracy_regions],
            "azimuth_accuracy_regions": [region.to_dict() for region in self.azimuth_accuracy_regions],
            "elevation_accuracy_regions": [region.to_dict() for region in self.elevation_accuracy_regions],
        }


@dataclass(frozen=True)
class RadarTrackingConfig:
    output_tracks: bool = False
    max_tracks: int = 0

    def to_dict(self) -> dict[str, int | bool]:
        return {
            "tracks": self.output_tracks,
            "max_tracks": self.max_tracks,
        }


@dataclass(frozen=True)
class RadarSystemConfig:
    frame_rate_hz: float = 10.0
    transmit_power_dbm: float = 55.0
    radiometric_calibration_factor_db: float = 0.0
    center_frequency_hz: float = 77.0e9
    range_resolution_m: float = 0.5
    range_quantization_m: float = 0.0
    velocity_min_mps: float = -20.0
    velocity_max_mps: float = 20.0
    velocity_resolution_mps: float = 0.2
    velocity_quantization_mps: float = 0.0
    angular_resolution: RadarAngularConfig = field(
        default_factory=lambda: RadarAngularConfig(az_deg=3.44, el_deg=180.0)
    )
    angular_quantization: RadarAngularConfig = field(default_factory=RadarAngularConfig)
    antenna_hpbw: RadarAngularConfig = field(
        default_factory=lambda: RadarAngularConfig(az_deg=18.0, el_deg=14.0)
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "frame_rate_hz": self.frame_rate_hz,
            "transmit_power_dbm": self.transmit_power_dbm,
            "radiometric_calibration_factor_db": self.radiometric_calibration_factor_db,
            "center_frequency_hz": self.center_frequency_hz,
            "range_resolution_m": self.range_resolution_m,
            "range_quantization_m": self.range_quantization_m,
            "velocity_min_mps": self.velocity_min_mps,
            "velocity_max_mps": self.velocity_max_mps,
            "velocity_resolution_mps": self.velocity_resolution_mps,
            "velocity_quantization_mps": self.velocity_quantization_mps,
            "angular_resolution_deg": self.angular_resolution.to_dict(),
            "angular_quantization_deg": self.angular_quantization.to_dict(),
            "antenna_hpbw_deg": self.antenna_hpbw.to_dict(),
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
    system: RadarSystemConfig = field(default_factory=RadarSystemConfig)
    detector: RadarDetectorConfig = field(default_factory=RadarDetectorConfig)
    estimator: RadarEstimatorConfig = field(default_factory=RadarEstimatorConfig)
    tracking: RadarTrackingConfig = field(default_factory=RadarTrackingConfig)
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
            "system_params": self.system.to_dict(),
            "detector_params": self.detector.to_dict(),
            "estimator_params": self.estimator.to_dict(),
            "tracking_params": self.tracking.to_dict(),
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
        _as_int(
            raw.get(
                "max_returns",
                raw.get("return_count", options.get("lidar_return_count", options.get("lidar_max_returns"))),
            ),
            1,
        ),
    )
    mode_raw = _as_str(raw.get("mode", options.get("lidar_return_mode")), "").upper()
    selection_mode = _as_str(
        raw.get("selection_mode", options.get("lidar_return_selection_mode")),
        "",
    ).upper()
    if not selection_mode and mode_raw in {"STRONGEST", "FIRST", "LAST"}:
        selection_mode = mode_raw
    if mode_raw in {"SINGLE", "DUAL", "MULTI"}:
        mode = mode_raw
    else:
        if max_returns <= 1:
            mode = "SINGLE"
        elif max_returns == 2:
            mode = "DUAL"
        else:
            mode = "MULTI"
    return LidarReturnModelConfig(
        mode=mode,
        max_returns=max_returns,
        selection_mode=selection_mode or "FIRST",
        range_discrimination_m=_as_float(
            raw.get("range_discrimination", options.get("lidar_range_discrimination_m")),
            0.0,
        ),
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
    particle_field = _as_dict(raw.get("particle_field"))
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
        precipitation_type=_as_str(
            raw.get(
                "precipitation_type",
                particle_field.get("type", options.get("lidar_precipitation_type")),
            ),
            "RAIN",
        ).upper(),
        particle_density_scale=_as_float(
            raw.get(
                "particle_density_scale",
                particle_field.get("density_scale", options.get("lidar_particle_density_scale")),
            ),
            1.0,
        ),
        particle_diameter_mm=_as_float(
            raw.get(
                "particle_diameter_mm",
                particle_field.get("diameter_mm", options.get("lidar_particle_diameter_mm")),
            ),
            0.0,
        ),
        terminal_velocity_mps=_as_float(
            raw.get(
                "terminal_velocity_mps",
                particle_field.get(
                    "terminal_velocity_mps",
                    options.get("lidar_particle_terminal_velocity_mps"),
                ),
            ),
            0.0,
        ),
        particle_reflectivity=_as_float(
            raw.get(
                "particle_reflectivity",
                particle_field.get("reflectivity", options.get("lidar_particle_reflectivity")),
            ),
            0.0,
        ),
        backscatter_jitter=_as_float(
            raw.get(
                "backscatter_jitter",
                particle_field.get("backscatter_jitter", options.get("lidar_backscatter_jitter")),
            ),
            0.1,
        ),
        field_seed=_as_int(
            raw.get(
                "field_seed",
                particle_field.get("seed", options.get("lidar_particle_field_seed")),
            ),
            0,
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


def _infer_lidar_channel_profile_pattern(file_uri: str) -> str:
    lowered = file_uri.strip().lower()
    if "cross" in lowered:
        return "CROSS"
    if "sparse" in lowered or "grid" in lowered:
        return "GRID"
    if "ring" in lowered:
        return "RING"
    return "NONE"


def _parse_lidar_channel_profile(options: Mapping[str, Any]) -> LidarChannelProfileConfig:
    raw = _as_dict(options.get("lidar_shared_channel_profile", options.get("shared_channel_profile")))
    raw_profile = _as_dict(raw.get("profile_data"))
    file_uri = _as_str(
        raw_profile.get("file_uri", options.get("lidar_channel_profile_file_uri")),
        "",
    )
    pattern = _as_str(
        raw_profile.get("pattern", options.get("lidar_channel_profile_pattern")),
        "",
    ).upper()
    if not pattern:
        pattern = _infer_lidar_channel_profile_pattern(file_uri)
    scale = _as_float(
        raw_profile.get("scale", options.get("lidar_channel_profile_scale")),
        0.0,
    )
    enabled = _as_bool(
        raw.get("enabled", options.get("lidar_channel_profile_enabled")),
        bool(file_uri or pattern != "NONE" or scale > 0.0),
    )
    return LidarChannelProfileConfig(
        enabled=enabled,
        profile_data=LidarProfileDataConfig(
            file_uri=file_uri,
            half_angle_rad=_as_float(
                raw_profile.get("half_angle", options.get("lidar_channel_profile_half_angle")),
                0.0,
            ),
            scale=scale,
            pattern=pattern,
            sample_count=max(
                0,
                _as_int(
                    raw_profile.get("sample_count", options.get("lidar_channel_profile_sample_count")),
                    0,
                ),
            ),
            sidelobe_gain=_as_float(
                raw_profile.get("sidelobe_gain", options.get("lidar_channel_profile_sidelobe_gain")),
                0.05,
            ),
        ),
    )


def _parse_lidar_multipath_model(options: Mapping[str, Any]) -> LidarMultipathConfig:
    raw = _as_dict(options.get("lidar_multipath_model"))
    return LidarMultipathConfig(
        enabled=_as_bool(raw.get("enabled", options.get("lidar_multipath_enabled")), False),
        mode=_as_str(raw.get("mode", options.get("lidar_multipath_mode")), "HYBRID").upper(),
        max_paths=max(
            1,
            _as_int(raw.get("max_paths", options.get("lidar_multipath_max_paths")), 2),
        ),
        path_signal_decay=_as_float(
            raw.get("path_signal_decay", options.get("lidar_multipath_path_signal_decay")),
            0.3,
        ),
        minimum_path_snr_db=_as_float(
            raw.get("minimum_path_snr_db", options.get("lidar_multipath_minimum_path_snr_db")),
            -8.0,
        ),
        max_extra_path_length_m=_as_float(
            raw.get(
                "max_extra_path_length_m",
                options.get("lidar_multipath_max_extra_path_length_m"),
            ),
            80.0,
        ),
        ground_plane_height_m=_as_float(
            raw.get("ground_plane_height_m", options.get("lidar_ground_plane_height_m")),
            -1.5,
        ),
        ground_reflectivity=_as_float(
            raw.get("ground_reflectivity", options.get("lidar_ground_reflectivity")),
            0.35,
        ),
        wall_plane_x_m=_as_float(
            raw.get("wall_plane_x_m", options.get("lidar_wall_plane_x_m")),
            25.0,
        ),
        wall_reflectivity=_as_float(
            raw.get("wall_reflectivity", options.get("lidar_wall_reflectivity")),
            0.25,
        ),
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


def _as_radians_to_deg(raw: Any, default_deg: float) -> float:
    if raw is None:
        return default_deg
    return _as_float(raw, default_deg * pi / 180.0) * 180.0 / pi


def _radar_model_sections(options: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    radar_model = _as_dict(options.get("radar_model"))
    standard = _as_dict(radar_model.get("standard_params"))
    system = _as_dict(standard.get("system_params"))
    post = _as_dict(standard.get("post_processing_params"))
    detector = _as_dict(post.get("detector_params"))
    detector_noise = _as_dict(detector.get("noise_performance"))
    detector_detectability = _as_dict(detector_noise.get("target_detectability"))
    detector_target = _as_dict(detector_detectability.get("target"))
    estimator = _as_dict(post.get("estimator_params"))
    tracking = _as_dict(post.get("tracking_params", post.get("track_params")))
    format_raw = _as_dict(radar_model.get("format"))
    antenna_params = _as_dict(system.get("antenna_params"))
    antenna_definitions = _as_list(antenna_params.get("antenna_definitions"))
    antenna_definition = _as_dict(antenna_definitions[0]) if antenna_definitions else {}
    beam_params = _as_dict(antenna_definition.get("beam_params"))
    return {
        "standard": standard,
        "field_of_view": _as_dict(standard.get("field_of_view")),
        "angular_resolution": _as_dict(standard.get("angular_resolution")),
        "angular_quantization": _as_dict(standard.get("angular_quantization")),
        "system": system,
        "post": post,
        "detector": detector,
        "detector_noise": detector_noise,
        "detector_detectability": detector_detectability,
        "detector_target": detector_target,
        "estimator": estimator,
        "tracking": tracking,
        "format": format_raw,
        "beam_params": beam_params,
    }


def _parse_radar_accuracy(raw: Mapping[str, Any], *, default_max_deviation: float = 0.0) -> RadarAccuracyConfig:
    return RadarAccuracyConfig(
        max_deviation=_as_float(raw.get("max_deviation"), default_max_deviation),
        num_sigma=max(_as_float(raw.get("num_sigma"), 1.0), 1e-6),
    )


def _parse_radar_accuracy_regions(raw: Any) -> list[RadarAccuracyRegionConfig]:
    regions: list[RadarAccuracyRegionConfig] = []
    for item in _as_list(raw):
        item_raw = _as_dict(item)
        if not item_raw:
            continue
        range_raw = _as_dict(item_raw.get("range"))
        azimuth_raw = _as_dict(item_raw.get("azimuth_deg", item_raw.get("azimuth")))
        elevation_raw = _as_dict(item_raw.get("elevation_deg", item_raw.get("elevation")))
        regions.append(
            RadarAccuracyRegionConfig(
                range_min_m=_as_float(range_raw.get("min"), 0.0),
                range_max_m=_as_float(range_raw.get("max"), 1.0e9),
                azimuth_min_deg=_as_float(azimuth_raw.get("min"), -180.0),
                azimuth_max_deg=_as_float(azimuth_raw.get("max"), 180.0),
                elevation_min_deg=_as_float(elevation_raw.get("min"), -180.0),
                elevation_max_deg=_as_float(elevation_raw.get("max"), 180.0),
                max_deviation=_as_float(item_raw.get("max_deviation"), 0.0),
                num_sigma=max(_as_float(item_raw.get("num_sigma"), 1.0), 1e-6),
            )
        )
    return regions


def _parse_radar_system(options: Mapping[str, Any]) -> RadarSystemConfig:
    sections = _radar_model_sections(options)
    raw = _as_dict(options.get("radar_system_params"))
    system = raw if raw else sections["system"]
    beam_params = _as_dict(_as_dict(options.get("radar_antenna_params")).get("beam_params"))
    if not beam_params:
        beam_params = sections["beam_params"]
    field_of_view = sections["field_of_view"]
    angular_resolution = sections["angular_resolution"]
    angular_quantization = sections["angular_quantization"]
    return RadarSystemConfig(
        frame_rate_hz=_as_float(
            system.get("frame_rate", sections["standard"].get("frame_rate", options.get("radar_frame_rate_hz"))),
            10.0,
        ),
        transmit_power_dbm=_as_float(
            system.get("transmit_power", options.get("radar_transmit_power_dbm")),
            55.0,
        ),
        radiometric_calibration_factor_db=_as_float(
            system.get(
                "radiometric_calibration_factor",
                options.get("radar_radiometric_calibration_factor_db"),
            ),
            0.0,
        ),
        center_frequency_hz=_as_float(
            system.get("center_frequency", options.get("radar_center_frequency_hz")),
            77.0e9,
        ),
        range_resolution_m=_as_float(
            system.get("range_resolution", options.get("radar_range_resolution_m")),
            0.5,
        ),
        range_quantization_m=_as_float(
            system.get("range_quantization", options.get("radar_range_quantization_m")),
            0.0,
        ),
        velocity_min_mps=_as_float(
            _as_dict(system.get("velocity")).get("min", options.get("radar_velocity_min_mps")),
            -20.0,
        ),
        velocity_max_mps=_as_float(
            _as_dict(system.get("velocity")).get("max", options.get("radar_velocity_max_mps")),
            20.0,
        ),
        velocity_resolution_mps=_as_float(
            system.get("velocity_resolution", options.get("radar_velocity_resolution_mps")),
            0.2,
        ),
        velocity_quantization_mps=_as_float(
            system.get("velocity_quantization", options.get("radar_velocity_quantization_mps")),
            0.0,
        ),
        angular_resolution=RadarAngularConfig(
            az_deg=_as_float(
                options.get("radar_angular_resolution_az_deg"),
                _as_radians_to_deg(angular_resolution.get("az"), 3.44),
            ),
            el_deg=_as_float(
                options.get("radar_angular_resolution_el_deg"),
                _as_radians_to_deg(angular_resolution.get("el"), 180.0),
            ),
        ),
        angular_quantization=RadarAngularConfig(
            az_deg=_as_float(
                options.get("radar_angular_quantization_az_deg"),
                _as_radians_to_deg(angular_quantization.get("az"), 0.0),
            ),
            el_deg=_as_float(
                options.get("radar_angular_quantization_el_deg"),
                _as_radians_to_deg(angular_quantization.get("el"), 0.0),
            ),
        ),
        antenna_hpbw=RadarAngularConfig(
            az_deg=_as_float(
                _as_dict(options.get("radar_antenna_hpbw_deg")).get(
                    "az",
                    beam_params.get("hpbw_az", options.get("radar_antenna_hpbw_az_deg")),
                ),
                18.0,
            ),
            el_deg=_as_float(
                _as_dict(options.get("radar_antenna_hpbw_deg")).get(
                    "el",
                    beam_params.get("hpbw_el", options.get("radar_antenna_hpbw_el_deg")),
                ),
                14.0,
            ),
        ),
    )


def _parse_radar_detector(options: Mapping[str, Any]) -> RadarDetectorConfig:
    sections = _radar_model_sections(options)
    raw = _as_dict(options.get("radar_detector_params"))
    detector = raw if raw else sections["detector"]
    detector_noise = _as_dict(detector.get("noise_performance"))
    if not detector_noise:
        detector_noise = sections["detector_noise"]
    detectability = _as_dict(detector_noise.get("target_detectability"))
    if not detectability:
        detectability = sections["detector_detectability"]
    target = _as_dict(detectability.get("target"))
    if not target:
        target = sections["detector_target"]
    return RadarDetectorConfig(
        noise_variance_dbw=_as_float(
            detector.get("noise_variance_dbw", options.get("radar_noise_variance_dbw")),
            -90.0,
        ),
        minimum_snr_db=_as_float(
            detector.get("minimum_snr_db", options.get("radar_minimum_snr_db")),
            -10.0,
        ),
        no_additive_noise=_as_bool(
            detector.get("no_additive_noise", options.get("radar_no_additive_noise")),
            False,
        ),
        max_detections=max(
            0,
            _as_int(detector.get("max_detections", options.get("radar_max_detections")), 0),
        ),
        probability_false_alarm=_as_float(
            detector_noise.get("probability_false_alarm", options.get("radar_probability_false_alarm")),
            0.0,
        ),
        target_detectability=RadarTargetDetectabilityConfig(
            probability_detection=_as_float(
                detectability.get("probability_detection", options.get("radar_probability_detection")),
                0.99,
            ),
            calibration_target_range_m=_as_float(
                target.get("range", options.get("radar_calibration_target_range_m")),
                100.0,
            ),
            calibration_target_rcs_dbsm=_as_float(
                target.get(
                    "radar_cross_section",
                    options.get("radar_calibration_target_rcs_dbsm"),
                ),
                0.0,
            ),
        ),
    )


def _parse_radar_estimator(options: Mapping[str, Any]) -> RadarEstimatorConfig:
    sections = _radar_model_sections(options)
    raw = _as_dict(options.get("radar_estimator_params"))
    estimator = raw if raw else sections["estimator"]
    return RadarEstimatorConfig(
        range_accuracy=_parse_radar_accuracy(
            _as_dict(estimator.get("range_accuracy", {})),
            default_max_deviation=_as_float(options.get("radar_range_accuracy_m"), 0.0),
        ),
        velocity_accuracy=_parse_radar_accuracy(
            _as_dict(estimator.get("velocity_accuracy", {})),
            default_max_deviation=_as_float(options.get("radar_velocity_accuracy_mps"), 0.0),
        ),
        azimuth_accuracy=_parse_radar_accuracy(
            {
                "max_deviation": _as_float(
                    _as_dict(estimator.get("azimuth_accuracy", {})).get("max_deviation"),
                    _as_float(options.get("radar_azimuth_accuracy_deg"), 0.0),
                ),
                "num_sigma": _as_float(
                    _as_dict(estimator.get("azimuth_accuracy", {})).get("num_sigma"),
                    1.0,
                ),
            }
        ),
        elevation_accuracy=_parse_radar_accuracy(
            {
                "max_deviation": _as_float(
                    _as_dict(estimator.get("elevation_accuracy", {})).get("max_deviation"),
                    _as_float(options.get("radar_elevation_accuracy_deg"), 0.0),
                ),
                "num_sigma": _as_float(
                    _as_dict(estimator.get("elevation_accuracy", {})).get("num_sigma"),
                    1.0,
                ),
            }
        ),
        range_accuracy_regions=_parse_radar_accuracy_regions(
            estimator.get("range_accuracy_regions", options.get("radar_range_accuracy_regions"))
        ),
        velocity_accuracy_regions=_parse_radar_accuracy_regions(
            estimator.get(
                "velocity_accuracy_regions",
                options.get("radar_velocity_accuracy_regions"),
            )
        ),
        azimuth_accuracy_regions=_parse_radar_accuracy_regions(
            estimator.get(
                "azimuth_accuracy_regions",
                options.get("radar_azimuth_accuracy_regions"),
            )
        ),
        elevation_accuracy_regions=_parse_radar_accuracy_regions(
            estimator.get(
                "elevation_accuracy_regions",
                options.get("radar_elevation_accuracy_regions"),
            )
        ),
    )


def _parse_radar_tracking(options: Mapping[str, Any]) -> RadarTrackingConfig:
    sections = _radar_model_sections(options)
    raw = _as_dict(options.get("radar_tracking_params"))
    tracking = raw if raw else sections["tracking"]
    format_raw = sections["format"]
    return RadarTrackingConfig(
        output_tracks=_as_bool(
            tracking.get("tracks", format_raw.get("tracks", options.get("radar_output_tracks"))),
            False,
        ),
        max_tracks=max(
            0,
            _as_int(tracking.get("max_tracks", options.get("radar_max_tracks")), 0),
        ),
    )


def build_sensor_sim_config(
    *,
    sensor_profile: str = "default",
    options: Mapping[str, Any] | None = None,
) -> SensorSimConfig:
    data = options if options is not None else {}
    ego_actor_id = _as_str(data.get("renderer_ego_actor_id"), "ego")
    radar_sections = _radar_model_sections(data)
    radar_system = _parse_radar_system(data)
    radar_detector = _parse_radar_detector(data)
    radar_estimator = _parse_radar_estimator(data)
    radar_tracking = _parse_radar_tracking(data)
    radar_field_of_view = radar_sections["field_of_view"]

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
            channel_profile=_parse_lidar_channel_profile(data),
            multipath_model=_parse_lidar_multipath_model(data),
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
            range_min_m=_as_float(
                data.get(
                    "radar_range_min_m",
                    _as_dict(radar_sections["system"].get("range")).get("min"),
                ),
                0.5,
            ),
            range_max_m=_as_float(
                data.get(
                    "radar_range_max_m",
                    _as_dict(radar_sections["system"].get("range")).get("max"),
                ),
                200.0,
            ),
            horizontal_fov_deg=_as_float(
                data.get("radar_horizontal_fov_deg"),
                _as_radians_to_deg(radar_field_of_view.get("az"), 120.0),
            ),
            vertical_fov_deg=_as_float(
                data.get("radar_vertical_fov_deg"),
                _as_radians_to_deg(radar_field_of_view.get("el"), 30.0),
            ),
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
            system=radar_system,
            detector=radar_detector,
            estimator=radar_estimator,
            tracking=radar_tracking,
            extrinsics=_parse_extrinsics(_as_dict(data.get("radar_extrinsics"))),
            behaviors=_parse_behaviors(data, "radar"),
        ),
    )
