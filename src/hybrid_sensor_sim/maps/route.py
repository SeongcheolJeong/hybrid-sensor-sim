from __future__ import annotations

import heapq
import math
from collections import deque
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.maps.convert import CANONICAL_LANE_GRAPH_SCHEMA_VERSION_V0, load_map_payload


CANONICAL_MAP_ROUTE_REPORT_SCHEMA_VERSION_V0 = "canonical_map_route_report_v0"
ROUTE_COST_MODE_HOPS = "hops"
ROUTE_COST_MODE_LENGTH = "length"


def _as_centerline_points(*, lane_id: str, value: Any) -> list[tuple[float, float]]:
    if not isinstance(value, list) or len(value) < 2:
        raise ValueError(f"lane {lane_id} centerline_m must have at least 2 points")
    points: list[tuple[float, float]] = []
    for point_index, point in enumerate(value):
        if not isinstance(point, dict):
            raise ValueError(f"lane {lane_id} centerline_m[{point_index}] must be an object")
        if "x_m" not in point or "y_m" not in point:
            raise ValueError(f"lane {lane_id} centerline_m[{point_index}] must include x_m/y_m")
        try:
            x_m = float(point["x_m"])
            y_m = float(point["y_m"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"lane {lane_id} centerline_m[{point_index}] x_m/y_m must be numeric") from exc
        points.append((x_m, y_m))
    return points


def _as_lane_refs(*, lane_id: str, field: str, value: Any) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"lane {lane_id} {field} must be a list")
    refs: list[str] = []
    seen: set[str] = set()
    for ref_index, ref in enumerate(value):
        ref_id = str(ref).strip()
        if not ref_id:
            raise ValueError(f"lane {lane_id} {field}[{ref_index}] must be non-empty")
        if ref_id == lane_id:
            raise ValueError(f"lane {lane_id} {field} cannot include self")
        if ref_id in seen:
            continue
        seen.add(ref_id)
        refs.append(ref_id)
    return refs


def _distance_m(point_a: tuple[float, float], point_b: tuple[float, float]) -> float:
    return math.hypot(point_a[0] - point_b[0], point_a[1] - point_b[1])


def _lane_length_m(centerline: list[tuple[float, float]]) -> float:
    total = 0.0
    for idx in range(1, len(centerline)):
        total += _distance_m(centerline[idx - 1], centerline[idx])
    return float(total)


def _resolve_lane_id(*, requested: str, candidates: list[str], field: str) -> str:
    requested_text = str(requested).strip()
    if requested_text:
        if requested_text not in candidates:
            raise ValueError(f"{field} not found in map lanes: {requested_text}")
        return requested_text
    if not candidates:
        raise ValueError(f"{field} candidates are empty")
    return str(candidates[0])


def _normalize_via_lane_ids(*, via_values: list[str], start_lane_id: str, end_lane_id: str, lane_id_set: set[str]) -> list[str]:
    via_lane_ids: list[str] = []
    previous = start_lane_id
    for idx, raw in enumerate(via_values):
        lane_id = str(raw).strip()
        if not lane_id:
            raise ValueError(f"via-lane-id[{idx}] must be non-empty")
        if lane_id not in lane_id_set:
            raise ValueError(f"via-lane-id not found in map lanes: {lane_id}")
        if lane_id == previous or lane_id == end_lane_id:
            continue
        via_lane_ids.append(lane_id)
        previous = lane_id
    return via_lane_ids


def _shortest_path_hops(*, start_lane: str, end_lane: str, successors_by_id: dict[str, list[str]]) -> tuple[list[str], int]:
    if start_lane == end_lane:
        return [start_lane], 1
    queue: deque[tuple[str, list[str]]] = deque([(start_lane, [start_lane])])
    visited: set[str] = {start_lane}
    while queue:
        lane_id, path = queue.popleft()
        for successor in successors_by_id.get(lane_id, []):
            if successor in visited:
                continue
            next_path = [*path, successor]
            if successor == end_lane:
                return next_path, len(visited) + 1
            visited.add(successor)
            queue.append((successor, next_path))
    return [], len(visited)


def _shortest_path_length(
    *,
    start_lane: str,
    end_lane: str,
    successors_by_id: dict[str, list[str]],
    lane_length_by_id: dict[str, float],
) -> tuple[list[str], int]:
    if start_lane == end_lane:
        return [start_lane], 1
    frontier: list[tuple[float, str, list[str]]] = [(0.0, start_lane, [start_lane])]
    best_cost_by_lane: dict[str, float] = {start_lane: 0.0}
    expanded_lanes: set[str] = set()
    while frontier:
        current_cost, lane_id, path = heapq.heappop(frontier)
        if lane_id in expanded_lanes:
            continue
        best_known_cost = best_cost_by_lane.get(lane_id, float("inf"))
        if current_cost > best_known_cost:
            continue
        expanded_lanes.add(lane_id)
        if lane_id == end_lane:
            return path, len(expanded_lanes)
        for successor in successors_by_id.get(lane_id, []):
            next_cost = current_cost + max(0.0, float(lane_length_by_id.get(successor, 0.0)))
            if next_cost >= best_cost_by_lane.get(successor, float("inf")):
                continue
            best_cost_by_lane[successor] = next_cost
            heapq.heappush(frontier, (next_cost, successor, [*path, successor]))
    return [], len(expanded_lanes)


def compute_canonical_route(
    payload: dict[str, Any],
    *,
    entry_lane_id: str = "",
    exit_lane_id: str = "",
    via_lane_ids: list[str] | None = None,
    cost_mode: str = ROUTE_COST_MODE_HOPS,
    map_path: Path | None = None,
) -> dict[str, Any]:
    if str(payload.get("map_schema_version", "")).strip() != CANONICAL_LANE_GRAPH_SCHEMA_VERSION_V0:
        raise ValueError(f"map_schema_version must be {CANONICAL_LANE_GRAPH_SCHEMA_VERSION_V0}")
    lanes_raw = payload.get("lanes", [])
    if not isinstance(lanes_raw, list) or not lanes_raw:
        raise ValueError("lanes must be a non-empty list")

    lane_ids: list[str] = []
    lane_centerline_by_id: dict[str, list[tuple[float, float]]] = {}
    lane_length_by_id: dict[str, float] = {}
    lane_predecessors_by_id: dict[str, list[str]] = {}
    lane_successors_by_id: dict[str, list[str]] = {}
    for idx, lane in enumerate(lanes_raw):
        if not isinstance(lane, dict):
            raise ValueError(f"lane[{idx}] must be an object")
        lane_id_value = str(lane.get("lane_id", "")).strip()
        if not lane_id_value:
            raise ValueError(f"lane[{idx}] lane_id must be non-empty")
        if lane_id_value in lane_centerline_by_id:
            raise ValueError(f"duplicate lane_id: {lane_id_value}")
        lane_ids.append(lane_id_value)
        lane_centerline_by_id[lane_id_value] = _as_centerline_points(lane_id=lane_id_value, value=lane.get("centerline_m", []))
        lane_length_by_id[lane_id_value] = _lane_length_m(lane_centerline_by_id[lane_id_value])
        lane_predecessors_by_id[lane_id_value] = _as_lane_refs(
            lane_id=lane_id_value,
            field="predecessor_lane_ids",
            value=lane.get("predecessor_lane_ids", []),
        )
        lane_successors_by_id[lane_id_value] = _as_lane_refs(
            lane_id=lane_id_value,
            field="successor_lane_ids",
            value=lane.get("successor_lane_ids", []),
        )

    lane_id_set = set(lane_ids)
    for lane_id_value in lane_ids:
        for predecessor in lane_predecessors_by_id.get(lane_id_value, []):
            if predecessor not in lane_id_set:
                raise ValueError(f"lane {lane_id_value} predecessor not found: {predecessor}")
        for successor in lane_successors_by_id.get(lane_id_value, []):
            if successor not in lane_id_set:
                raise ValueError(f"lane {lane_id_value} successor not found: {successor}")

    entry_lane_ids = sorted(lane_id_value for lane_id_value in lane_ids if not lane_predecessors_by_id.get(lane_id_value, []))
    exit_lane_ids = sorted(lane_id_value for lane_id_value in lane_ids if not lane_successors_by_id.get(lane_id_value, []))
    selected_entry_lane_id = _resolve_lane_id(
        requested=entry_lane_id,
        candidates=entry_lane_ids if entry_lane_ids else sorted(lane_ids),
        field="entry-lane-id",
    )
    selected_exit_lane_id = _resolve_lane_id(
        requested=exit_lane_id,
        candidates=exit_lane_ids if exit_lane_ids else sorted(lane_ids),
        field="exit-lane-id",
    )
    normalized_via_lane_ids = _normalize_via_lane_ids(
        via_values=[str(item) for item in (via_lane_ids or [])],
        start_lane_id=selected_entry_lane_id,
        end_lane_id=selected_exit_lane_id,
        lane_id_set=lane_id_set,
    )
    route_cost_mode = str(cost_mode).strip().lower()
    if route_cost_mode not in {ROUTE_COST_MODE_HOPS, ROUTE_COST_MODE_LENGTH}:
        raise ValueError(f"cost_mode must be one of: {ROUTE_COST_MODE_HOPS}, {ROUTE_COST_MODE_LENGTH}")

    segment_nodes = [selected_entry_lane_id, *normalized_via_lane_ids, selected_exit_lane_id]
    route_segments: list[dict[str, Any]] = []
    route_lane_ids: list[str] = []
    visited_lane_count = 0
    for segment_index in range(max(0, len(segment_nodes) - 1)):
        segment_start_lane_id = segment_nodes[segment_index]
        segment_end_lane_id = segment_nodes[segment_index + 1]
        if route_cost_mode == ROUTE_COST_MODE_LENGTH:
            segment_route_lane_ids, segment_visited_lane_count = _shortest_path_length(
                start_lane=segment_start_lane_id,
                end_lane=segment_end_lane_id,
                successors_by_id=lane_successors_by_id,
                lane_length_by_id=lane_length_by_id,
            )
        else:
            segment_route_lane_ids, segment_visited_lane_count = _shortest_path_hops(
                start_lane=segment_start_lane_id,
                end_lane=segment_end_lane_id,
                successors_by_id=lane_successors_by_id,
            )
        if not segment_route_lane_ids:
            if len(segment_nodes) <= 2:
                raise ValueError(f"no route found from entry={selected_entry_lane_id} to exit={selected_exit_lane_id}")
            raise ValueError(
                f"no route found for segment {segment_index + 1}/{len(segment_nodes) - 1}: {segment_start_lane_id}->{segment_end_lane_id}"
            )
        if route_lane_ids:
            route_lane_ids.extend(segment_route_lane_ids[1:])
        else:
            route_lane_ids.extend(segment_route_lane_ids)
        visited_lane_count += int(segment_visited_lane_count)
        segment_total_length_m = sum(float(lane_length_by_id.get(lane_id_value, 0.0)) for lane_id_value in segment_route_lane_ids)
        route_segments.append(
            {
                "segment_index": int(segment_index + 1),
                "start_lane_id": segment_start_lane_id,
                "end_lane_id": segment_end_lane_id,
                "segment_lane_ids": segment_route_lane_ids,
                "segment_lane_count": int(len(segment_route_lane_ids)),
                "segment_hop_count": int(max(0, len(segment_route_lane_ids) - 1)),
                "segment_total_length_m": float(round(segment_total_length_m, 6)),
                "visited_lane_count": int(segment_visited_lane_count),
            }
        )

    route_total_length_m = sum(float(lane_length_by_id.get(lane_id_value, 0.0)) for lane_id_value in route_lane_ids)
    route_lane_count = len(route_lane_ids)
    route_hop_count = max(0, route_lane_count - 1)
    route_avg_lane_length_m = (float(route_total_length_m) / float(route_lane_count)) if route_lane_count > 0 else 0.0
    route_summary = {
        "route_status": "pass",
        "selected_entry_lane_id": selected_entry_lane_id,
        "selected_exit_lane_id": selected_exit_lane_id,
        "route_lane_ids": route_lane_ids,
        "route_lane_count": int(route_lane_count),
        "route_hop_count": int(route_hop_count),
        "route_total_length_m": float(round(route_total_length_m, 6)),
        "route_avg_lane_length_m": float(round(route_avg_lane_length_m, 6)),
        "route_cost_mode": route_cost_mode,
        "route_cost_value": int(route_hop_count) if route_cost_mode == ROUTE_COST_MODE_HOPS else float(round(route_total_length_m, 6)),
        "via_lane_ids_input": normalized_via_lane_ids,
        "route_segment_count": int(max(0, len(segment_nodes) - 1)),
        "route_segments": route_segments,
        "visited_lane_count": int(visited_lane_count),
    }
    return {
        "report_schema_version": CANONICAL_MAP_ROUTE_REPORT_SCHEMA_VERSION_V0,
        "map_schema_version": str(payload.get("map_schema_version", "")).strip(),
        "map_path": str(map_path) if map_path is not None else None,
        "map_id": str(payload.get("map_id", "")).strip(),
        "entry_lane_ids": entry_lane_ids,
        "exit_lane_ids": exit_lane_ids,
        **route_summary,
        "route_summary": route_summary,
    }


def load_and_compute_canonical_route(
    path: Path,
    *,
    entry_lane_id: str = "",
    exit_lane_id: str = "",
    via_lane_ids: list[str] | None = None,
    cost_mode: str = ROUTE_COST_MODE_HOPS,
) -> dict[str, Any]:
    payload = load_map_payload(path, "canonical map")
    return compute_canonical_route(
        payload,
        entry_lane_id=entry_lane_id,
        exit_lane_id=exit_lane_id,
        via_lane_ids=via_lane_ids,
        cost_mode=cost_mode,
        map_path=path.resolve(),
    )
