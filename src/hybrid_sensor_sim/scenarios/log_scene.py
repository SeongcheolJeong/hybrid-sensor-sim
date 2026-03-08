from __future__ import annotations

import json
from pathlib import Path
from typing import Any


LOG_SCENE_SCHEMA_VERSION_V0 = "log_scene_v0"
ROUTE_RELATION_VALUES = {"same_lane", "downstream", "upstream", "off_route"}


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("log scene must be a JSON object")
    return payload


def _require_keys(payload: dict[str, Any], keys: list[str]) -> None:
    missing = [key for key in keys if key not in payload]
    if missing:
        raise ValueError(f"missing required keys: {missing}")


def _normalize_optional_route_relation(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    relation = str(value).strip().lower()
    if not relation:
        raise ValueError(f"{field} must be non-empty when provided")
    if relation not in ROUTE_RELATION_VALUES:
        allowed = ", ".join(sorted(ROUTE_RELATION_VALUES))
        raise ValueError(f"{field} must be one of: {allowed}")
    return relation


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
    canonical_map = payload.get("canonical_map")
    canonical_map_path = payload.get("canonical_map_path")
    if canonical_map is not None and not isinstance(canonical_map, dict):
        raise ValueError("canonical_map must be a JSON object when provided")
    if canonical_map is not None and canonical_map_path is not None:
        raise ValueError("provide only one of canonical_map or canonical_map_path")
    route_definition = payload.get("route_definition")
    if route_definition is not None and not isinstance(route_definition, dict):
        raise ValueError("route_definition must be an object when provided")
    ego_lane_id_raw = payload.get("ego_lane_id")
    ego_lane_id = None if ego_lane_id_raw is None else str(ego_lane_id_raw).strip()
    if ego_lane_id == "":
        raise ValueError("ego_lane_id must be non-empty when provided")
    ego_route_lane_id_raw = payload.get("ego_route_lane_id")
    ego_route_lane_id = None if ego_route_lane_id_raw is None else str(ego_route_lane_id_raw).strip()
    if ego_route_lane_id == "":
        raise ValueError("ego_route_lane_id must be non-empty when provided")
    lead_vehicle_lane_id_raw = payload.get("lead_vehicle_lane_id")
    lead_vehicle_lane_id = (
        None if lead_vehicle_lane_id_raw is None else str(lead_vehicle_lane_id_raw).strip()
    )
    if lead_vehicle_lane_id == "":
        raise ValueError("lead_vehicle_lane_id must be non-empty when provided")
    lead_vehicle_route_lane_id_raw = payload.get("lead_vehicle_route_lane_id")
    lead_vehicle_route_lane_id = (
        None if lead_vehicle_route_lane_id_raw is None else str(lead_vehicle_route_lane_id_raw).strip()
    )
    if lead_vehicle_route_lane_id == "":
        raise ValueError("lead_vehicle_route_lane_id must be non-empty when provided")
    ego_route_relation = _normalize_optional_route_relation(
        payload.get("ego_route_relation"),
        field="ego_route_relation",
    )
    lead_vehicle_route_relation = _normalize_optional_route_relation(
        payload.get("lead_vehicle_route_relation"),
        field="lead_vehicle_route_relation",
    )
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
        "canonical_map": canonical_map,
        "canonical_map_path": None if canonical_map_path is None else str(canonical_map_path),
        "route_definition": route_definition,
        "ego_lane_id": ego_lane_id,
        "ego_route_lane_id": ego_route_lane_id,
        "ego_route_relation": ego_route_relation,
        "lead_vehicle_lane_id": lead_vehicle_lane_id,
        "lead_vehicle_route_lane_id": lead_vehicle_route_lane_id,
        "lead_vehicle_route_relation": lead_vehicle_route_relation,
    }


def load_log_scene(payload_or_path: dict[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(payload_or_path, dict):
        payload = payload_or_path
    else:
        payload = _load_json_object(Path(payload_or_path).resolve())
    return validate_log_scene_payload(payload)
