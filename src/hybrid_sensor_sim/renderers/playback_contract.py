from __future__ import annotations

import json
from pathlib import Path
from typing import Any


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
    options: dict[str, Any],
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
    ego_actor_id = str(options.get("renderer_ego_actor_id", "ego"))
    mounts: list[dict[str, Any]] = []

    mounts.append(
        {
            "sensor_id": str(options.get("renderer_camera_sensor_id", "camera_front")),
            "sensor_type": "camera",
            "attach_to_actor_id": ego_actor_id,
            "enabled": camera_available,
            "extrinsics_source": camera_extrinsics_source,
            "extrinsics": camera_extrinsics,
            "intrinsics": _dict_or_none(options.get("camera_intrinsics")) or {},
            "distortion_coeffs": _dict_or_none(options.get("camera_distortion_coeffs")) or {},
        }
    )
    mounts.append(
        {
            "sensor_id": str(options.get("renderer_lidar_sensor_id", "lidar_top")),
            "sensor_type": "lidar",
            "attach_to_actor_id": ego_actor_id,
            "enabled": lidar_available,
            "extrinsics_source": lidar_extrinsics_source,
            "extrinsics": lidar_extrinsics,
        }
    )
    mounts.append(
        {
            "sensor_id": str(options.get("renderer_radar_sensor_id", "radar_front")),
            "sensor_type": "radar",
            "attach_to_actor_id": ego_actor_id,
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
    if not bool(options.get("renderer_bridge_enabled", False)):
        return None

    camera_sweep = artifacts.get("camera_projection_trajectory_sweep")
    lidar_sweep = artifacts.get("lidar_trajectory_sweep")
    radar_sweep = artifacts.get("radar_targets_trajectory_sweep")
    camera_preview = artifacts.get("camera_projection_preview")
    lidar_preview = artifacts.get("lidar_noisy_preview")
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

    camera_extrinsics_option = _dict_or_none(options.get("camera_extrinsics"))
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
    elif camera_extrinsics_option is not None:
        camera_extrinsics = camera_extrinsics_option
        camera_extrinsics_source = "options"

    lidar_extrinsics_option = _dict_or_none(options.get("lidar_extrinsics"))
    lidar_extrinsics_sweep = _frame_first_dict(lidar_sweep_payload, "lidar_extrinsics")
    lidar_extrinsics: dict[str, Any] | None = None
    lidar_extrinsics_source = "none"
    if isinstance(lidar_extrinsics_sweep, dict):
        lidar_extrinsics = lidar_extrinsics_sweep
        lidar_extrinsics_source = "lidar_sweep_frame0"
    elif lidar_extrinsics_option is not None:
        lidar_extrinsics = lidar_extrinsics_option
        lidar_extrinsics_source = "options"

    radar_extrinsics_option = _dict_or_none(options.get("radar_extrinsics"))
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
    elif radar_extrinsics_option is not None:
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
        options=options,
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
        "renderer_backend": str(options.get("renderer_backend", "none")),
        "renderer_scene": {
            "map": str(options.get("renderer_map", "")),
            "weather": str(options.get("renderer_weather", "default")),
            "scene_seed": int(options.get("renderer_scene_seed", 0)),
            "ego_actor_id": str(options.get("renderer_ego_actor_id", "ego")),
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
                "geometry_model": str(options.get("camera_geometry", "pinhole")),
                "distortion_model": str(options.get("camera_distortion", "brown-conrady")),
                "intrinsics": _dict_or_none(options.get("camera_intrinsics")) or {},
                "distortion_coeffs": _dict_or_none(options.get("camera_distortion_coeffs")) or {},
                "extrinsics": camera_extrinsics,
                "extrinsics_source": camera_extrinsics_source,
            },
            "lidar": {
                "extrinsics": lidar_extrinsics,
                "extrinsics_source": lidar_extrinsics_source,
                "trajectory_sweep_enabled": bool(
                    options.get("lidar_trajectory_sweep_enabled", False)
                ),
                "motion_compensation_enabled": bool(
                    options.get("lidar_motion_compensation_enabled", True)
                ),
                "scan_duration_s": float(options.get("lidar_scan_duration_s", 0.1)),
            },
            "radar": {
                "extrinsics": radar_extrinsics,
                "extrinsics_source": radar_extrinsics_source,
                "trajectory_sweep_enabled": bool(
                    options.get("radar_trajectory_sweep_enabled", False)
                ),
                "range_min_m": float(options.get("radar_range_min_m", 0.5)),
                "range_max_m": float(options.get("radar_range_max_m", 200.0)),
            },
        },
        "renderer_sensor_mounts": renderer_sensor_mounts,
        "frame_count": frame_count,
        "frame_step_s": frame_step_s,
        "frame_offset": frame_offset,
        "frames": frames,
    }
