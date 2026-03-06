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

    camera_sweep_payload = _read_json(camera_sweep) if camera_sweep is not None else None
    lidar_sweep_payload = _read_json(lidar_sweep) if lidar_sweep is not None else None
    radar_sweep_payload = _read_json(radar_sweep) if radar_sweep is not None else None

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
        },
        "frame_count": frame_count,
        "frame_step_s": frame_step_s,
        "frame_offset": frame_offset,
        "frames": frames,
    }
