from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.maps.convert import CANONICAL_LANE_GRAPH_SCHEMA_VERSION_V0, load_map_payload


CANONICAL_MAP_VALIDATION_REPORT_SCHEMA_VERSION_V0 = "canonical_map_validation_report_v0"
CENTERLINE_CONTINUITY_WARN_THRESHOLD_M = 2.0


def _parse_centerline_points(*, lane_id: str, value: Any, errors: list[str]) -> list[tuple[float, float]]:
    if not isinstance(value, list) or len(value) < 2:
        errors.append(f"lane {lane_id} centerline_m must have at least 2 points")
        return []

    points: list[tuple[float, float]] = []
    for point_index, point in enumerate(value):
        if not isinstance(point, dict):
            errors.append(f"lane {lane_id} centerline_m[{point_index}] must be an object with x_m/y_m")
            continue
        if "x_m" not in point or "y_m" not in point:
            errors.append(f"lane {lane_id} centerline_m[{point_index}] must include x_m/y_m")
            continue
        try:
            x_m = float(point["x_m"])
            y_m = float(point["y_m"])
        except (TypeError, ValueError):
            errors.append(f"lane {lane_id} centerline_m[{point_index}] x_m/y_m must be numeric")
            continue
        points.append((x_m, y_m))
    if len(points) < 2:
        errors.append(f"lane {lane_id} centerline_m must contain at least 2 valid points")
    return points


def _parse_lane_refs(*, lane_id: str, field: str, value: Any, errors: list[str], warnings: list[str]) -> list[str]:
    if not isinstance(value, list):
        errors.append(f"lane {lane_id} {field} must be a list")
        return []

    refs: list[str] = []
    seen: set[str] = set()
    for ref_index, ref in enumerate(value):
        ref_id = str(ref).strip()
        if not ref_id:
            errors.append(f"lane {lane_id} {field}[{ref_index}] must be a non-empty lane_id")
            continue
        if ref_id == lane_id:
            errors.append(f"lane {lane_id} {field} cannot include self")
            continue
        if ref_id in seen:
            warnings.append(f"lane {lane_id} {field} has duplicate reference: {ref_id}")
            continue
        seen.add(ref_id)
        refs.append(ref_id)
    return refs


def _distance_m(point_a: tuple[float, float], point_b: tuple[float, float]) -> float:
    return math.hypot(point_a[0] - point_b[0], point_a[1] - point_b[1])


def validate_canonical_map(payload: dict[str, Any]) -> tuple[list[str], list[str], dict[str, Any]]:
    errors: list[str] = []
    warnings: list[str] = []
    semantic_summary: dict[str, Any] = {
        "lane_count": 0,
        "successor_edge_count": 0,
        "entry_lane_count": 0,
        "exit_lane_count": 0,
        "continuity_gap_warning_count": 0,
        "non_reciprocal_predecessor_warning_count": 0,
        "non_reciprocal_successor_warning_count": 0,
        "unreachable_lane_count": 0,
        "unreachable_lane_ids": [],
        "entry_lane_missing_warning_count": 0,
        "routing_semantic_warning_count": 0,
        "routing_semantic_status": "fail",
    }
    if str(payload.get("map_schema_version", "")).strip() != CANONICAL_LANE_GRAPH_SCHEMA_VERSION_V0:
        errors.append(f"map_schema_version must be {CANONICAL_LANE_GRAPH_SCHEMA_VERSION_V0}")
        return errors, warnings, semantic_summary

    lanes = payload.get("lanes", [])
    if not isinstance(lanes, list) or not lanes:
        errors.append("lanes must be a non-empty list")
        return errors, warnings, semantic_summary

    lane_ids: list[str] = []
    lane_by_id: dict[str, dict[str, Any]] = {}
    lane_centerline_by_id: dict[str, list[tuple[float, float]]] = {}
    lane_predecessors_by_id: dict[str, list[str]] = {}
    lane_successors_by_id: dict[str, list[str]] = {}
    for idx, lane in enumerate(lanes):
        if not isinstance(lane, dict):
            errors.append(f"lane[{idx}] must be an object")
            continue
        lane_id = str(lane.get("lane_id", "")).strip()
        if not lane_id:
            errors.append(f"lane[{idx}] lane_id must be non-empty")
            continue
        if lane_id in lane_by_id:
            errors.append(f"duplicate lane_id: {lane_id}")
            continue
        lane_ids.append(lane_id)
        lane_by_id[lane_id] = lane
        lane_centerline_by_id[lane_id] = _parse_centerline_points(
            lane_id=lane_id,
            value=lane.get("centerline_m", []),
            errors=errors,
        )
        lane_predecessors_by_id[lane_id] = _parse_lane_refs(
            lane_id=lane_id,
            field="predecessor_lane_ids",
            value=lane.get("predecessor_lane_ids", []),
            errors=errors,
            warnings=warnings,
        )
        lane_successors_by_id[lane_id] = _parse_lane_refs(
            lane_id=lane_id,
            field="successor_lane_ids",
            value=lane.get("successor_lane_ids", []),
            errors=errors,
            warnings=warnings,
        )

    lane_id_set = set(lane_ids)
    continuity_gap_warning_count = 0
    non_reciprocal_predecessor_warning_count = 0
    non_reciprocal_successor_warning_count = 0
    for lane_id in lane_by_id:
        predecessors = lane_predecessors_by_id.get(lane_id, [])
        successors = lane_successors_by_id.get(lane_id, [])
        lane_centerline = lane_centerline_by_id.get(lane_id, [])
        lane_end = lane_centerline[-1] if lane_centerline else None
        for predecessor in predecessors:
            if predecessor not in lane_id_set:
                errors.append(f"lane {lane_id} predecessor not found: {predecessor}")
                continue
            if lane_id not in lane_successors_by_id.get(predecessor, []):
                warnings.append(f"lane {lane_id} predecessor linkage not reciprocal: {predecessor}")
                non_reciprocal_predecessor_warning_count += 1
        for successor in successors:
            if successor not in lane_id_set:
                errors.append(f"lane {lane_id} successor not found: {successor}")
                continue
            if lane_id not in lane_predecessors_by_id.get(successor, []):
                warnings.append(f"lane {lane_id} successor linkage not reciprocal: {successor}")
                non_reciprocal_successor_warning_count += 1
            successor_centerline = lane_centerline_by_id.get(successor, [])
            successor_start = successor_centerline[0] if successor_centerline else None
            if lane_end is not None and successor_start is not None:
                centerline_gap_m = _distance_m(lane_end, successor_start)
                if centerline_gap_m > CENTERLINE_CONTINUITY_WARN_THRESHOLD_M:
                    warnings.append(
                        f"lane {lane_id} -> {successor} centerline gap {centerline_gap_m:.3f}m exceeds {CENTERLINE_CONTINUITY_WARN_THRESHOLD_M:.1f}m"
                    )
                    continuity_gap_warning_count += 1

    entry_lane_ids = sorted(lane_id for lane_id in lane_ids if not lane_predecessors_by_id.get(lane_id, []))
    entry_lane_missing_warning_count = 0
    unreachable_lane_ids: list[str] = []
    if not entry_lane_ids:
        warnings.append("no entry lanes found (all lanes have predecessors)")
        entry_lane_missing_warning_count = 1
    else:
        visited: set[str] = set(entry_lane_ids)
        queue: list[str] = list(entry_lane_ids)
        while queue:
            current = queue.pop(0)
            for successor in lane_successors_by_id.get(current, []):
                if successor in lane_id_set and successor not in visited:
                    visited.add(successor)
                    queue.append(successor)
        unreachable_lane_ids = sorted(lane_id_set.difference(visited))
        if unreachable_lane_ids:
            warnings.append("unreachable lanes from entry graph: " + ", ".join(unreachable_lane_ids))

    successor_edge_count = sum(len(lane_successors_by_id.get(lane_id, [])) for lane_id in lane_ids)
    exit_lane_count = sum(1 for lane_id in lane_ids if not lane_successors_by_id.get(lane_id, []))
    routing_semantic_warning_count = (
        continuity_gap_warning_count
        + non_reciprocal_predecessor_warning_count
        + non_reciprocal_successor_warning_count
        + len(unreachable_lane_ids)
        + entry_lane_missing_warning_count
    )
    routing_semantic_status = "pass"
    if errors:
        routing_semantic_status = "fail"
    elif routing_semantic_warning_count > 0:
        routing_semantic_status = "warn"

    semantic_summary = {
        "lane_count": len(lane_ids),
        "successor_edge_count": successor_edge_count,
        "entry_lane_count": len(entry_lane_ids),
        "exit_lane_count": exit_lane_count,
        "continuity_gap_warning_count": continuity_gap_warning_count,
        "non_reciprocal_predecessor_warning_count": non_reciprocal_predecessor_warning_count,
        "non_reciprocal_successor_warning_count": non_reciprocal_successor_warning_count,
        "unreachable_lane_count": len(unreachable_lane_ids),
        "unreachable_lane_ids": unreachable_lane_ids,
        "entry_lane_missing_warning_count": entry_lane_missing_warning_count,
        "routing_semantic_warning_count": routing_semantic_warning_count,
        "routing_semantic_status": routing_semantic_status,
    }
    return errors, warnings, semantic_summary


def build_canonical_map_validation_report(payload: dict[str, Any], *, map_path: Path | None = None) -> dict[str, Any]:
    errors, warnings, semantic_summary = validate_canonical_map(payload)
    return {
        "report_schema_version": CANONICAL_MAP_VALIDATION_REPORT_SCHEMA_VERSION_V0,
        "map_schema_version": str(payload.get("map_schema_version", "")).strip(),
        "map_path": str(map_path) if map_path is not None else None,
        "map_id": str(payload.get("map_id", "")).strip(),
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
        "routing_semantic_summary": semantic_summary,
    }


def load_and_validate_canonical_map(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = load_map_payload(path, "canonical map")
    return payload, build_canonical_map_validation_report(payload, map_path=path.resolve())
