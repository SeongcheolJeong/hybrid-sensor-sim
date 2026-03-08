from __future__ import annotations

import json
from pathlib import Path
from typing import Any


LOG_SCENE_SCHEMA_VERSION_V0 = "log_scene_v0"


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("log scene must be a JSON object")
    return payload


def _require_keys(payload: dict[str, Any], keys: list[str]) -> None:
    missing = [key for key in keys if key not in payload]
    if missing:
        raise ValueError(f"missing required keys: {missing}")


def validate_log_scene_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if str(payload.get("log_scene_schema_version", "")) != LOG_SCENE_SCHEMA_VERSION_V0:
        raise ValueError(
            "log_scene_schema_version must be "
            f"{LOG_SCENE_SCHEMA_VERSION_V0}"
        )
    _require_keys(
        payload,
        [
            "log_id",
            "map_id",
            "ego_initial_speed_mps",
            "lead_vehicle_initial_gap_m",
            "lead_vehicle_speed_mps",
            "duration_sec",
            "dt_sec",
        ],
    )
    log_id = str(payload["log_id"]).strip()
    map_id = str(payload["map_id"]).strip()
    map_version = str(payload.get("map_version", "v0")).strip() or "v0"
    if not log_id:
        raise ValueError("log_id must be non-empty")
    if not map_id:
        raise ValueError("map_id must be non-empty")
    duration_sec = float(payload["duration_sec"])
    dt_sec = float(payload["dt_sec"])
    if duration_sec <= 0:
        raise ValueError("duration_sec must be > 0")
    if dt_sec <= 0:
        raise ValueError("dt_sec must be > 0")
    return {
        "log_scene_schema_version": LOG_SCENE_SCHEMA_VERSION_V0,
        "log_id": log_id,
        "map_id": map_id,
        "map_version": map_version,
        "ego_initial_speed_mps": float(payload["ego_initial_speed_mps"]),
        "lead_vehicle_initial_gap_m": float(payload["lead_vehicle_initial_gap_m"]),
        "lead_vehicle_speed_mps": float(payload["lead_vehicle_speed_mps"]),
        "duration_sec": duration_sec,
        "dt_sec": dt_sec,
    }


def load_log_scene(payload_or_path: dict[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(payload_or_path, dict):
        payload = payload_or_path
    else:
        payload = _load_json_object(Path(payload_or_path).resolve())
    return validate_log_scene_payload(payload)
