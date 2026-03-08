from __future__ import annotations

from typing import Any


AUTOWARE_CONSUMER_PROFILE_DEFAULT = ""

_AUTOWARE_CONSUMER_PROFILES: dict[str, dict[str, Any]] = {
    "semantic_perception_v0": {
        "profile_id": "semantic_perception_v0",
        "description": (
            "Camera-visible + semantic segmentation with LiDAR point clouds for "
            "downstream semantic perception consumers."
        ),
        "required_output_roles_by_modality": {
            "camera": ["camera_visible", "camera_semantic"],
            "lidar": ["lidar_point_cloud"],
        },
    },
    "tracking_fusion_v0": {
        "profile_id": "tracking_fusion_v0",
        "description": (
            "Camera-visible + LiDAR point clouds + radar detections and tracks "
            "for tracking/fusion consumers."
        ),
        "required_output_roles_by_modality": {
            "camera": ["camera_visible"],
            "lidar": ["lidar_point_cloud"],
            "radar": ["radar_detections", "radar_tracks"],
        },
    },
}


def list_autoware_consumer_profiles() -> list[dict[str, Any]]:
    return [
        {
            "profile_id": profile_id,
            "description": str(profile.get("description", "")).strip() or None,
            "required_output_roles_by_modality": {
                str(modality): list(roles)
                for modality, roles in dict(
                    profile.get("required_output_roles_by_modality", {})
                ).items()
            },
        }
        for profile_id, profile in sorted(_AUTOWARE_CONSUMER_PROFILES.items())
    ]


def resolve_autoware_consumer_profile(profile_id: str | None) -> dict[str, Any] | None:
    normalized = str(profile_id or "").strip()
    if not normalized:
        return None
    profile = _AUTOWARE_CONSUMER_PROFILES.get(normalized)
    if profile is None:
        available = ", ".join(sorted(_AUTOWARE_CONSUMER_PROFILES))
        raise ValueError(
            f"unsupported Autoware consumer profile: {normalized}. Available profiles: {available}"
        )
    return {
        "profile_id": normalized,
        "description": str(profile.get("description", "")).strip() or None,
        "required_output_roles_by_modality": {
            str(modality): [str(role).strip() for role in roles or [] if str(role).strip()]
            for modality, roles in dict(
                profile.get("required_output_roles_by_modality", {})
            ).items()
        },
    }
