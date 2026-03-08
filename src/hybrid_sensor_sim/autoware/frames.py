from __future__ import annotations

from typing import Any


AUTOWARE_FRAME_TREE_SCHEMA_VERSION_V0 = "autoware_frame_tree_v0"


def _mount_extrinsics(mount: dict[str, Any]) -> dict[str, Any]:
    extrinsics = mount.get("extrinsics")
    if not isinstance(extrinsics, dict):
        return {}
    return extrinsics


def build_autoware_frame_tree(
    sensor_mounts: list[dict[str, Any]] | None,
    base_frame: str = "base_link",
) -> dict[str, Any]:
    frames: list[dict[str, Any]] = []
    for mount in sensor_mounts or []:
        if not isinstance(mount, dict):
            continue
        sensor_id = str(mount.get("sensor_id", "")).strip()
        if not sensor_id:
            continue
        extrinsics = _mount_extrinsics(mount)
        frames.append(
            {
                "sensor_id": sensor_id,
                "sensor_type": str(mount.get("sensor_type", "")).strip() or None,
                "enabled": bool(mount.get("enabled", True)),
                "frame_id": sensor_id,
                "parent_frame_id": str(base_frame).strip() or "base_link",
                "attach_to_actor_id": str(mount.get("attach_to_actor_id", "")).strip() or None,
                "translation": {
                    "x": float(extrinsics.get("tx", 0.0) or 0.0),
                    "y": float(extrinsics.get("ty", 0.0) or 0.0),
                    "z": float(extrinsics.get("tz", 0.0) or 0.0),
                },
                "rotation_rpy": {
                    "roll_deg": float(extrinsics.get("roll_deg", 0.0) or 0.0),
                    "pitch_deg": float(extrinsics.get("pitch_deg", 0.0) or 0.0),
                    "yaw_deg": float(extrinsics.get("yaw_deg", 0.0) or 0.0),
                },
                "extrinsics_source": str(mount.get("extrinsics_source", "")).strip() or None,
            }
        )
    frames.sort(key=lambda item: item["sensor_id"])
    return {
        "schema_version": AUTOWARE_FRAME_TREE_SCHEMA_VERSION_V0,
        "base_frame": str(base_frame).strip() or "base_link",
        "sensor_frame_count": len(frames),
        "sensor_frames": frames,
    }
