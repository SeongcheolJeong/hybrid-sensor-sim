from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.scenarios import (
    LOG_SCENE_SCHEMA_VERSION_V0,
    SCENARIO_SCHEMA_VERSION_V0,
    build_scenario_from_log_scene,
    load_log_scene,
    validate_scenario_payload,
)


SCENARIO_RUNTIME_BRIDGE_SCHEMA_VERSION_V0 = "scenario_runtime_bridge_v0"
DEFAULT_LANE_SPACING_M = 4.0


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("scenario runtime bridge source must be a JSON object")
    return payload


def _lane_center_y(lane_index: int, *, lane_spacing_m: float) -> float:
    return float(lane_index) * float(lane_spacing_m)


def _actor_pose(actor: Any, *, lane_spacing_m: float) -> list[float]:
    return [
        float(actor.position_m),
        _lane_center_y(int(actor.lane_index), lane_spacing_m=lane_spacing_m),
        0.0,
    ]


def _build_object_payload(
    *,
    actor: Any,
    actor_type: str,
    lane_spacing_m: float,
) -> dict[str, Any]:
    return {
        "id": str(actor.actor_id),
        "type": actor_type,
        "pose": _actor_pose(actor, lane_spacing_m=lane_spacing_m),
        "speed_mps": float(actor.speed_mps),
        "lane_index": int(actor.lane_index),
        "lane_id": actor.lane_id,
        "lane_binding_mode": str(actor.lane_binding_mode),
        "route_lane_id": actor.route_lane_id,
        "route_binding_mode": str(actor.route_binding_mode),
    }


def _build_ego_trajectory(
    *,
    scenario_config: Any,
    lane_spacing_m: float,
) -> list[list[float]]:
    ego = scenario_config.ego
    start_pose = _actor_pose(ego, lane_spacing_m=lane_spacing_m)
    horizon_sec = max(float(scenario_config.dt_sec), min(float(scenario_config.duration_sec), 5.0))
    end_pose = [
        float(ego.position_m) + max(float(ego.speed_mps), 0.0) * horizon_sec,
        start_pose[1],
        0.0,
    ]
    if end_pose == start_pose:
        end_pose = [start_pose[0] + 1.0, start_pose[1], start_pose[2]]
    return [start_pose, end_pose]


def build_smoke_ready_scenario(
    *,
    source_payload_path: Path,
    source_payload_kind: str,
    lane_spacing_m: float = DEFAULT_LANE_SPACING_M,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_source_path = source_payload_path.resolve()
    if source_payload_kind == LOG_SCENE_SCHEMA_VERSION_V0:
        log_scene = load_log_scene(resolved_source_path)
        raw_scenario_payload = build_scenario_from_log_scene(
            log_scene,
            log_scene_path=resolved_source_path,
        )
        normalized_scenario = validate_scenario_payload(
            raw_scenario_payload,
            source_path=resolved_source_path,
        )
        source_conversion = "log_scene_to_scenario_definition"
    elif source_payload_kind == SCENARIO_SCHEMA_VERSION_V0:
        raw_scenario_payload = _load_json_object(resolved_source_path)
        normalized_scenario = validate_scenario_payload(
            raw_scenario_payload,
            source_path=resolved_source_path,
        )
        source_conversion = "scenario_definition_direct"
    else:
        raise ValueError(f"unsupported source_payload_kind: {source_payload_kind}")

    objects = [
        _build_object_payload(
            actor=normalized_scenario.ego,
            actor_type="vehicle",
            lane_spacing_m=lane_spacing_m,
        )
    ]
    objects.extend(
        _build_object_payload(
            actor=npc,
            actor_type="vehicle",
            lane_spacing_m=lane_spacing_m,
        )
        for npc in normalized_scenario.npcs
    )
    ego_trajectory = _build_ego_trajectory(
        scenario_config=normalized_scenario,
        lane_spacing_m=lane_spacing_m,
    )
    smoke_scenario = {
        "name": str(normalized_scenario.scenario_id),
        "description": f"Smoke bridge for {normalized_scenario.scenario_id}",
        "objects": objects,
        "ego_trajectory": ego_trajectory,
        "metadata": {
            "source_payload_kind": source_payload_kind,
            "source_payload_path": str(resolved_source_path),
            "source_conversion": source_conversion,
            "scenario_schema_version": SCENARIO_SCHEMA_VERSION_V0,
            "scenario_id": str(normalized_scenario.scenario_id),
            "duration_sec": float(normalized_scenario.duration_sec),
            "dt_sec": float(normalized_scenario.dt_sec),
            "lane_spacing_m": float(lane_spacing_m),
            "canonical_map_path": raw_scenario_payload.get("canonical_map_path"),
            "route_definition": raw_scenario_payload.get("route_definition"),
        },
    }
    route_report = None
    if normalized_scenario.map_context is not None:
        route_report = normalized_scenario.map_context.route_report
    if isinstance(route_report, dict):
        smoke_scenario["waypoints"] = [
            [point[0], point[1], point[2]]
            for point in ego_trajectory
        ]

    bridge_manifest = {
        "scenario_runtime_bridge_schema_version": SCENARIO_RUNTIME_BRIDGE_SCHEMA_VERSION_V0,
        "source_payload_kind": source_payload_kind,
        "source_payload_path": str(resolved_source_path),
        "source_conversion": source_conversion,
        "scenario_id": str(normalized_scenario.scenario_id),
        "lane_spacing_m": float(lane_spacing_m),
        "object_count": len(objects),
        "ego_actor_id": str(normalized_scenario.ego.actor_id),
        "ego_lane_index": int(normalized_scenario.ego.lane_index),
        "ego_lane_id": normalized_scenario.ego.lane_id,
        "ego_route_lane_id": normalized_scenario.ego.route_lane_id,
        "npc_actor_ids": [str(npc.actor_id) for npc in normalized_scenario.npcs],
        "npc_lane_indexes": [int(npc.lane_index) for npc in normalized_scenario.npcs],
        "npc_lane_ids": [npc.lane_id for npc in normalized_scenario.npcs],
        "npc_route_lane_ids": [npc.route_lane_id for npc in normalized_scenario.npcs],
        "ego_trajectory_point_count": len(ego_trajectory),
        "route_lane_ids": (
            list(route_report.get("route_lane_ids", []))
            if isinstance(route_report, dict)
            else []
        ),
        "canonical_map_path": raw_scenario_payload.get("canonical_map_path"),
    }
    return smoke_scenario, bridge_manifest


def write_smoke_ready_scenario(
    *,
    source_payload_path: Path,
    source_payload_kind: str,
    out_root: Path,
    lane_spacing_m: float = DEFAULT_LANE_SPACING_M,
) -> dict[str, Any]:
    out_root.mkdir(parents=True, exist_ok=True)
    smoke_scenario, bridge_manifest = build_smoke_ready_scenario(
        source_payload_path=source_payload_path,
        source_payload_kind=source_payload_kind,
        lane_spacing_m=lane_spacing_m,
    )
    smoke_scenario_path = out_root / "scenario_backend_smoke_scenario.json"
    bridge_manifest_path = out_root / "scenario_runtime_bridge_manifest.json"
    smoke_scenario_path.write_text(
        json.dumps(smoke_scenario, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    bridge_manifest["smoke_scenario_path"] = str(smoke_scenario_path.resolve())
    bridge_manifest_path.write_text(
        json.dumps(bridge_manifest, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    return {
        "smoke_scenario_path": smoke_scenario_path,
        "bridge_manifest_path": bridge_manifest_path,
        "smoke_scenario": smoke_scenario,
        "bridge_manifest": bridge_manifest,
    }
