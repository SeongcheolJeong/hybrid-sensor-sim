from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from hybrid_sensor_sim.maps import compute_canonical_route, load_map_payload
from hybrid_sensor_sim.scenarios.log_scene import validate_log_scene_payload
from hybrid_sensor_sim.scenarios.schema import SCENARIO_SCHEMA_VERSION_V0


def _resolve_optional_map_path(*, raw_path: str | None, source_path: Path | None) -> str | None:
    if raw_path is None:
        return None
    candidate = Path(str(raw_path))
    if not candidate.is_absolute():
        if source_path is None:
            return str(candidate)
        candidate = (source_path.parent / candidate).resolve()
    else:
        candidate = candidate.resolve()
    return str(candidate)


def _build_route_definition_from_log_scene(
    *,
    normalized: Mapping[str, Any],
    resolved_map_path: str | None,
) -> tuple[dict[str, Any] | None, str | None, list[str]]:
    route_definition_raw = normalized.get("route_definition")
    canonical_map = normalized.get("canonical_map")
    canonical_map_path = normalized.get("canonical_map_path")
    if canonical_map is None and canonical_map_path is None:
        return None, None, []
    auto_synthesized_route = route_definition_raw is None
    if route_definition_raw is not None:
        route_definition = dict(route_definition_raw)
    else:
        route_definition = {"cost_mode": "hops"}

    entry_lane_id = str(route_definition.get("entry_lane_id", "")).strip()
    exit_lane_id = str(route_definition.get("exit_lane_id", "")).strip()
    via_lane_ids = [str(item) for item in route_definition.get("via_lane_ids", []) or []]
    cost_mode = str(route_definition.get("cost_mode", "hops")).strip().lower() or "hops"
    map_payload = canonical_map if canonical_map is not None else load_map_payload(Path(resolved_map_path), "canonical map")
    route_report = compute_canonical_route(
        map_payload,
        entry_lane_id=entry_lane_id,
        exit_lane_id=exit_lane_id,
        via_lane_ids=via_lane_ids,
        cost_mode=cost_mode,
        map_path=Path(resolved_map_path) if resolved_map_path is not None else None,
    )
    normalized_route_definition = {
        "entry_lane_id": route_report["selected_entry_lane_id"],
        "exit_lane_id": route_report["selected_exit_lane_id"],
        "via_lane_ids": (
            list(route_report["route_lane_ids"][1:-1])
            if auto_synthesized_route
            else list(route_report["via_lane_ids_input"])
        ),
        "cost_mode": route_report["route_cost_mode"],
    }
    default_lane_id = str(route_report["selected_entry_lane_id"])
    return normalized_route_definition, default_lane_id, [str(item) for item in route_report["route_lane_ids"]]


def _resolve_lane_id_from_route_relation(
    *,
    route_lane_ids: list[str],
    anchor_lane_id: str | None,
    relation: str | None,
) -> str | None:
    if relation is None:
        return None
    if relation == "off_route":
        return None
    if not route_lane_ids:
        return None
    if anchor_lane_id in route_lane_ids:
        anchor_index = route_lane_ids.index(str(anchor_lane_id))
    else:
        anchor_index = 0
    offset = {
        "same_lane": 0,
        "downstream": 1,
        "upstream": -1,
    }[relation]
    target_index = anchor_index + offset
    if target_index < 0 or target_index >= len(route_lane_ids):
        return None
    return str(route_lane_ids[target_index])


def build_scenario_from_log_scene(
    log_scene: Mapping[str, Any],
    *,
    log_scene_path: Path | None = None,
) -> dict[str, Any]:
    normalized = validate_log_scene_payload(dict(log_scene))
    resolved_map_path = _resolve_optional_map_path(
        raw_path=normalized.get("canonical_map_path"),
        source_path=log_scene_path,
    )
    route_definition, default_lane_id, route_lane_ids = _build_route_definition_from_log_scene(
        normalized=normalized,
        resolved_map_path=resolved_map_path,
    )
    scenario_payload = {
        "scenario_schema_version": SCENARIO_SCHEMA_VERSION_V0,
        "scenario_id": f"log_replay_{normalized['log_id']}",
        "duration_sec": float(normalized["duration_sec"]),
        "dt_sec": float(normalized["dt_sec"]),
        "ego": {
            "actor_id": "ego",
            "position_m": 0.0,
            "speed_mps": float(normalized["ego_initial_speed_mps"]),
        },
        "npcs": [
            {
                "actor_id": "lead_vehicle",
                "position_m": float(normalized["lead_vehicle_initial_gap_m"]),
                "speed_mps": float(normalized["lead_vehicle_speed_mps"]),
            }
        ],
    }
    if normalized.get("canonical_map") is not None:
        scenario_payload["canonical_map"] = normalized["canonical_map"]
    if resolved_map_path is not None:
        scenario_payload["canonical_map_path"] = resolved_map_path
    if route_definition is not None:
        scenario_payload["route_definition"] = route_definition

    ego_lane_id = normalized.get("ego_lane_id")
    if ego_lane_id is None:
        ego_lane_id = _resolve_lane_id_from_route_relation(
            route_lane_ids=route_lane_ids,
            anchor_lane_id=default_lane_id,
            relation=normalized.get("ego_route_relation"),
        )
    ego_lane_id = ego_lane_id or default_lane_id
    if ego_lane_id is not None:
        scenario_payload["ego"]["lane_id"] = ego_lane_id
    lead_lane_id = normalized.get("lead_vehicle_lane_id")
    if lead_lane_id is None:
        lead_lane_id = _resolve_lane_id_from_route_relation(
            route_lane_ids=route_lane_ids,
            anchor_lane_id=ego_lane_id or default_lane_id,
            relation=normalized.get("lead_vehicle_route_relation"),
        )
    lead_lane_id = lead_lane_id or ego_lane_id or default_lane_id
    if lead_lane_id is not None:
        scenario_payload["npcs"][0]["lane_id"] = lead_lane_id
    return scenario_payload


def build_replay_manifest(
    *,
    log_scene_path: str,
    log_id: str,
    run_id: str,
    scenario_path: str,
    summary_path: str,
    status: str,
    termination_reason: str,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "log_scene_path": log_scene_path,
        "log_id": log_id,
        "run_id": run_id,
        "scenario_path": scenario_path,
        "summary_path": summary_path,
        "status": status,
        "termination_reason": termination_reason,
    }
