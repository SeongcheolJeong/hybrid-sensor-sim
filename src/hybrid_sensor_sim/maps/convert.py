from __future__ import annotations

import json
from pathlib import Path
from typing import Any


SIMPLE_MAP_SCHEMA_VERSION_V0 = "simple_map_v0"
CANONICAL_LANE_GRAPH_SCHEMA_VERSION_V0 = "canonical_lane_graph_v0"


def load_map_payload(path: Path, label: str = "map") -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _as_point_list(value: Any, label: str) -> list[list[float]]:
    if not isinstance(value, list) or len(value) < 2:
        raise ValueError(f"{label} must be a list with at least 2 points")
    result: list[list[float]] = []
    for point in value:
        if not isinstance(point, list) or len(point) != 2:
            raise ValueError(f"{label} point must be [x, y]")
        result.append([float(point[0]), float(point[1])])
    return result


def convert_simple_to_canonical(payload: dict[str, Any]) -> dict[str, Any]:
    if str(payload.get("map_schema_version", "")).strip() != SIMPLE_MAP_SCHEMA_VERSION_V0:
        raise ValueError(
            f"simple map map_schema_version must be {SIMPLE_MAP_SCHEMA_VERSION_V0}"
        )
    roads = payload.get("roads", [])
    if not isinstance(roads, list) or not roads:
        raise ValueError("simple map roads must be a non-empty list")

    lanes: list[dict[str, Any]] = []
    for road in roads:
        if not isinstance(road, dict):
            raise ValueError("each road entry must be an object")
        road_id = str(road.get("road_id", "")).strip()
        if not road_id:
            raise ValueError("road_id must be a non-empty string")
        centerline = _as_point_list(road.get("centerline", []), f"road {road_id} centerline")
        lanes.append(
            {
                "lane_id": road_id,
                "lane_type": str(road.get("lane_type", "driving")),
                "speed_limit_kph": float(road.get("speed_limit_kph", 50.0)),
                "centerline_m": [{"x_m": point[0], "y_m": point[1]} for point in centerline],
                "predecessor_lane_ids": [str(item) for item in road.get("predecessor_lane_ids", [])],
                "successor_lane_ids": [str(item) for item in road.get("successor_lane_ids", [])],
            }
        )
    return {
        "map_schema_version": CANONICAL_LANE_GRAPH_SCHEMA_VERSION_V0,
        "map_id": str(payload.get("map_id", "map_unknown")),
        "lanes": lanes,
    }


def convert_canonical_to_simple(payload: dict[str, Any]) -> dict[str, Any]:
    if str(payload.get("map_schema_version", "")).strip() != CANONICAL_LANE_GRAPH_SCHEMA_VERSION_V0:
        raise ValueError(
            f"canonical map map_schema_version must be {CANONICAL_LANE_GRAPH_SCHEMA_VERSION_V0}"
        )
    lanes = payload.get("lanes", [])
    if not isinstance(lanes, list) or not lanes:
        raise ValueError("canonical map lanes must be a non-empty list")

    roads: list[dict[str, Any]] = []
    for lane in lanes:
        if not isinstance(lane, dict):
            raise ValueError("each lane entry must be an object")
        lane_id = str(lane.get("lane_id", "")).strip()
        if not lane_id:
            raise ValueError("lane_id must be a non-empty string")
        centerline_m = lane.get("centerline_m", [])
        if not isinstance(centerline_m, list) or len(centerline_m) < 2:
            raise ValueError(f"lane {lane_id} centerline_m must contain at least 2 points")
        centerline: list[list[float]] = []
        for point in centerline_m:
            if not isinstance(point, dict) or "x_m" not in point or "y_m" not in point:
                raise ValueError(f"lane {lane_id} centerline_m point must include x_m/y_m")
            centerline.append([float(point["x_m"]), float(point["y_m"])])
        roads.append(
            {
                "road_id": lane_id,
                "lane_type": str(lane.get("lane_type", "driving")),
                "speed_limit_kph": float(lane.get("speed_limit_kph", 50.0)),
                "centerline": centerline,
                "predecessor_lane_ids": [str(item) for item in lane.get("predecessor_lane_ids", [])],
                "successor_lane_ids": [str(item) for item in lane.get("successor_lane_ids", [])],
            }
        )
    return {
        "map_schema_version": SIMPLE_MAP_SCHEMA_VERSION_V0,
        "map_id": str(payload.get("map_id", "map_unknown")),
        "roads": roads,
    }


def convert_map_payload(payload: dict[str, Any], *, to_format: str) -> dict[str, Any]:
    normalized = str(to_format).strip().lower()
    if normalized == "canonical":
        return convert_simple_to_canonical(payload)
    if normalized == "simple":
        return convert_canonical_to_simple(payload)
    raise ValueError("to_format must be 'canonical' or 'simple'")
