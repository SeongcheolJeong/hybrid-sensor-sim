from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.config import SensorSimConfig, build_sensor_sim_config


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _safe_frame_count(payload: dict[str, Any] | None) -> int:
    if not isinstance(payload, dict):
        return 0
    value = payload.get("frame_count")
    if isinstance(value, int) and value >= 0:
        return value
    if isinstance(value, float) and value >= 0.0:
        return int(value)
    frames = payload.get("frames")
    if isinstance(frames, list):
        return len(frames)
    return 0


def _dict_or_none(raw: Any) -> dict[str, Any] | None:
    return raw if isinstance(raw, dict) else None


def _frame_first_dict(payload: dict[str, Any] | None, key: str) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    frames = payload.get("frames")
    if not isinstance(frames, list):
        return None
    for frame in frames:
        if not isinstance(frame, dict):
            continue
        value = frame.get(key)
        if isinstance(value, dict):
            return value
    return None


def _frame_source(
    *,
    sweep_artifact: Path | None,
    sweep_payload: dict[str, Any] | None,
    preview_artifact: Path | None,
    frame_index: int,
) -> dict[str, Any] | None:
    sweep_count = _safe_frame_count(sweep_payload)
    if sweep_artifact is not None and sweep_count > 0:
        return {
            "source_type": "sweep",
            "artifact": str(sweep_artifact),
            "frame_index": min(frame_index, sweep_count - 1),
            "frame_count": sweep_count,
        }
    if preview_artifact is not None:
        return {
            "source_type": "single",
            "artifact": str(preview_artifact),
            "frame_index": 0,
            "frame_count": 1,
        }
    return None


def _build_renderer_sensor_mounts(
    *,
    config: SensorSimConfig,
    camera_extrinsics: dict[str, Any] | None,
    camera_extrinsics_source: str,
    lidar_extrinsics: dict[str, Any] | None,
    lidar_extrinsics_source: str,
    radar_extrinsics: dict[str, Any] | None,
    radar_extrinsics_source: str,
    camera_available: bool,
    lidar_available: bool,
    radar_available: bool,
) -> list[dict[str, Any]]:
    mounts: list[dict[str, Any]] = []

    mounts.append(
        {
            "sensor_id": config.camera.sensor_id,
            "sensor_type": "camera",
            "attach_to_actor_id": config.camera.attach_to_actor_id,
            "enabled": camera_available,
            "extrinsics_source": camera_extrinsics_source,
            "extrinsics": camera_extrinsics,
            "intrinsics": config.camera.intrinsics.to_dict(),
            "distortion_coeffs": config.camera.distortion_coeffs.to_dict(),
        }
    )
    mounts.append(
        {
            "sensor_id": config.lidar.sensor_id,
            "sensor_type": "lidar",
            "attach_to_actor_id": config.lidar.attach_to_actor_id,
            "enabled": lidar_available,
            "extrinsics_source": lidar_extrinsics_source,
            "extrinsics": lidar_extrinsics,
        }
    )
    mounts.append(
        {
            "sensor_id": config.radar.sensor_id,
            "sensor_type": "radar",
            "attach_to_actor_id": config.radar.attach_to_actor_id,
            "enabled": radar_available,
            "extrinsics_source": radar_extrinsics_source,
            "extrinsics": radar_extrinsics,
        }
    )
    return mounts


def build_renderer_playback_contract(
    *,
    options: dict[str, Any],
    artifacts: dict[str, Path],
) -> dict[str, Any] | None:
    config = build_sensor_sim_config(options=options)
    if not config.renderer.bridge_enabled:
        return None

    camera_sweep = artifacts.get("camera_projection_trajectory_sweep")
    lidar_sweep = artifacts.get("lidar_trajectory_sweep")
    radar_sweep = artifacts.get("radar_targets_trajectory_sweep")
    camera_preview = artifacts.get("camera_projection_preview")
    lidar_preview = artifacts.get("lidar_noisy_preview")
    lidar_preview_json = artifacts.get("lidar_noisy_preview_json")
    radar_preview = artifacts.get("radar_targets_preview")
    generated_survey = artifacts.get("generated_survey")
    execution_plan = artifacts.get("execution_plan")
    survey_mapping_metadata = artifacts.get("survey_mapping_metadata")

    camera_sweep_payload = _read_json(camera_sweep) if camera_sweep is not None else None
    lidar_sweep_payload = _read_json(lidar_sweep) if lidar_sweep is not None else None
    radar_sweep_payload = _read_json(radar_sweep) if radar_sweep is not None else None
    survey_mapping_payload = (
        _read_json(survey_mapping_metadata) if survey_mapping_metadata is not None else None
    )
    camera_preview_payload = _read_json(camera_preview) if camera_preview is not None else None
    radar_preview_payload = _read_json(radar_preview) if radar_preview is not None else None

    camera_extrinsics_option_raw = _dict_or_none(options.get("camera_extrinsics"))
    camera_extrinsics_option = (
        config.camera.extrinsics.to_dict() if camera_extrinsics_option_raw is not None else None
    )
    camera_extrinsics_sweep = _frame_first_dict(camera_sweep_payload, "camera_extrinsics")
    camera_extrinsics_preview = (
        camera_preview_payload.get("camera_extrinsics")
        if isinstance(camera_preview_payload, dict)
        else None
    )
    camera_extrinsics: dict[str, Any] | None = None
    camera_extrinsics_source = "none"
    if isinstance(camera_extrinsics_sweep, dict):
        camera_extrinsics = camera_extrinsics_sweep
        camera_extrinsics_source = "camera_sweep_frame0"
    elif isinstance(camera_extrinsics_preview, dict):
        camera_extrinsics = camera_extrinsics_preview
        camera_extrinsics_source = str(
            camera_preview_payload.get("camera_extrinsics_source", "camera_preview")
        )
    elif camera_extrinsics_option:
        camera_extrinsics = camera_extrinsics_option
        camera_extrinsics_source = "options"

    lidar_extrinsics_option_raw = _dict_or_none(options.get("lidar_extrinsics"))
    lidar_extrinsics_option = (
        config.lidar.extrinsics.to_dict() if lidar_extrinsics_option_raw is not None else None
    )
    lidar_extrinsics_sweep = _frame_first_dict(lidar_sweep_payload, "lidar_extrinsics")
    lidar_extrinsics: dict[str, Any] | None = None
    lidar_extrinsics_source = "none"
    if isinstance(lidar_extrinsics_sweep, dict):
        lidar_extrinsics = lidar_extrinsics_sweep
        lidar_extrinsics_source = "lidar_sweep_frame0"
    elif lidar_extrinsics_option:
        lidar_extrinsics = lidar_extrinsics_option
        lidar_extrinsics_source = "options"

    radar_extrinsics_option_raw = _dict_or_none(options.get("radar_extrinsics"))
    radar_extrinsics_option = (
        config.radar.extrinsics.to_dict() if radar_extrinsics_option_raw is not None else None
    )
    radar_extrinsics_sweep = _frame_first_dict(radar_sweep_payload, "radar_extrinsics")
    radar_extrinsics_preview = (
        radar_preview_payload.get("radar_extrinsics")
        if isinstance(radar_preview_payload, dict)
        else None
    )
    radar_extrinsics: dict[str, Any] | None = None
    radar_extrinsics_source = "none"
    if isinstance(radar_extrinsics_sweep, dict):
        radar_extrinsics = radar_extrinsics_sweep
        radar_extrinsics_source = "radar_sweep_frame0"
    elif isinstance(radar_extrinsics_preview, dict):
        radar_extrinsics = radar_extrinsics_preview
        radar_extrinsics_source = "radar_preview"
    elif radar_extrinsics_option:
        radar_extrinsics = radar_extrinsics_option
        radar_extrinsics_source = "options"

    frame_count = max(
        1,
        _safe_frame_count(camera_sweep_payload),
        _safe_frame_count(lidar_sweep_payload),
        _safe_frame_count(radar_sweep_payload),
    )
    frame_step_s = float(options.get("renderer_time_step_s", 0.05))
    start_time_s = float(options.get("renderer_start_time_s", 0.0))
    frame_offset = int(options.get("renderer_frame_offset", 0))

    frames: list[dict[str, Any]] = []
    for frame_id in range(frame_count):
        frame: dict[str, Any] = {
            "frame_id": frame_id,
            "renderer_frame_id": frame_id + frame_offset,
            "time_s": start_time_s + frame_step_s * float(frame_id),
        }
        camera_source = _frame_source(
            sweep_artifact=camera_sweep,
            sweep_payload=camera_sweep_payload,
            preview_artifact=camera_preview,
            frame_index=frame_id,
        )
        if camera_source is not None:
            frame["camera"] = camera_source

        lidar_source = _frame_source(
            sweep_artifact=lidar_sweep,
            sweep_payload=lidar_sweep_payload,
            preview_artifact=lidar_preview,
            frame_index=frame_id,
        )
        if lidar_source is not None:
            frame["lidar"] = lidar_source

        radar_source = _frame_source(
            sweep_artifact=radar_sweep,
            sweep_payload=radar_sweep_payload,
            preview_artifact=radar_preview,
            frame_index=frame_id,
        )
        if radar_source is not None:
            frame["radar"] = radar_source
        frames.append(frame)

    camera_available = (camera_sweep is not None) or (camera_preview is not None)
    lidar_available = (lidar_sweep is not None) or (lidar_preview is not None)
    radar_available = (radar_sweep is not None) or (radar_preview is not None)
    renderer_sensor_mounts = _build_renderer_sensor_mounts(
        config=config,
        camera_extrinsics=camera_extrinsics,
        camera_extrinsics_source=camera_extrinsics_source,
        lidar_extrinsics=lidar_extrinsics,
        lidar_extrinsics_source=lidar_extrinsics_source,
        radar_extrinsics=radar_extrinsics,
        radar_extrinsics_source=radar_extrinsics_source,
        camera_available=camera_available,
        lidar_available=lidar_available,
        radar_available=radar_available,
    )

    return {
        "schema_version": "1.0",
        "sensor_config_schema_version": config.schema_version,
        "renderer_backend": config.renderer.backend,
        "renderer_scene": {
            "map": config.renderer.map_name,
            "weather": config.renderer.weather,
            "scene_seed": config.renderer.scene_seed,
            "ego_actor_id": config.renderer.ego_actor_id,
        },
        "input_artifacts": {
            "point_cloud_primary": str(artifacts["point_cloud_primary"])
            if "point_cloud_primary" in artifacts
            else None,
            "trajectory_primary": str(artifacts["trajectory_primary"])
            if "trajectory_primary" in artifacts
            else None,
            "camera_projection_preview": str(camera_preview) if camera_preview is not None else None,
            "camera_projection_trajectory_sweep": str(camera_sweep) if camera_sweep is not None else None,
            "lidar_noisy_preview": str(lidar_preview) if lidar_preview is not None else None,
            "lidar_noisy_preview_json": str(lidar_preview_json) if lidar_preview_json is not None else None,
            "lidar_trajectory_sweep": str(lidar_sweep) if lidar_sweep is not None else None,
            "radar_targets_preview": str(radar_preview) if radar_preview is not None else None,
            "radar_targets_trajectory_sweep": str(radar_sweep) if radar_sweep is not None else None,
            "generated_survey": str(generated_survey) if generated_survey is not None else None,
            "survey_mapping_metadata": str(survey_mapping_metadata)
            if survey_mapping_metadata is not None
            else None,
            "execution_plan": str(execution_plan) if execution_plan is not None else None,
        },
        "survey_mapping": {
            "available": bool(survey_mapping_payload),
            "metadata": survey_mapping_payload,
            "metadata_artifact": str(survey_mapping_metadata) if survey_mapping_metadata is not None else None,
            "generated_survey_path": str(generated_survey) if generated_survey is not None else None,
        },
        "sensor_setup": {
            "camera": {
                "sensor_type": config.camera.sensor_type,
                "geometry_model": config.camera.geometry_model,
                "distortion_model": config.camera.distortion_model,
                "intrinsics": config.camera.intrinsics.to_dict(),
                "distortion_coeffs": config.camera.distortion_coeffs.to_dict(),
                "depth_params": config.camera.depth_params.to_dict(),
                "semantic_params": config.camera.semantic_params.to_dict(),
                "image_chain": config.camera.image_chain.to_dict(),
                "lens_params": config.camera.lens_params.to_dict(),
                "rolling_shutter": config.camera.rolling_shutter.to_dict(),
                "extrinsics": camera_extrinsics,
                "extrinsics_source": camera_extrinsics_source,
                "behaviors": [behavior.to_dict() for behavior in config.camera.behaviors],
            },
            "lidar": {
                "extrinsics": lidar_extrinsics,
                "extrinsics_source": lidar_extrinsics_source,
                "trajectory_sweep_enabled": config.lidar.trajectory_sweep_enabled,
                "motion_compensation_enabled": config.lidar.motion_compensation_enabled,
                "scan_duration_s": config.lidar.scan_duration_s,
                "scan_type": config.lidar.scan_type,
                "scan_frequency_hz": config.lidar.scan_frequency_hz,
                "spin_direction": config.lidar.spin_direction,
                "source_angles_deg": list(config.lidar.source_angles_deg),
                "source_angle_tolerance_deg": config.lidar.source_angle_tolerance_deg,
                "scan_field_deg": {
                    "azimuth_min": config.lidar.scan_field_azimuth_min_deg,
                    "azimuth_max": config.lidar.scan_field_azimuth_max_deg,
                    "elevation_min": config.lidar.scan_field_elevation_min_deg,
                    "elevation_max": config.lidar.scan_field_elevation_max_deg,
                },
                "scan_field_offset_deg": {
                    "azimuth": config.lidar.scan_field_azimuth_offset_deg,
                    "elevation": config.lidar.scan_field_elevation_offset_deg,
                },
                "scan_path_deg": list(config.lidar.scan_path_deg),
                "multi_scan_path_deg": [list(path) for path in config.lidar.multi_scan_path_deg],
                "intensity": config.lidar.intensity.to_dict(),
                "physics_model": config.lidar.physics_model.to_dict(),
                "return_model": config.lidar.return_model.to_dict(),
                "environment_model": config.lidar.environment_model.to_dict(),
                "noise_performance": config.lidar.noise_performance.to_dict(),
                "behaviors": [behavior.to_dict() for behavior in config.lidar.behaviors],
            },
            "radar": {
                "extrinsics": radar_extrinsics,
                "extrinsics_source": radar_extrinsics_source,
                "trajectory_sweep_enabled": config.radar.trajectory_sweep_enabled,
                "range_min_m": config.radar.range_min_m,
                "range_max_m": config.radar.range_max_m,
                "clutter_model": config.radar.clutter_model,
                "behaviors": [behavior.to_dict() for behavior in config.radar.behaviors],
            },
        },
        "renderer_sensor_mounts": renderer_sensor_mounts,
        "frame_count": frame_count,
        "frame_step_s": frame_step_s,
        "frame_offset": frame_offset,
        "frames": frames,
    }
