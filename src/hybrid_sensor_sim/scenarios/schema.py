from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.maps import (
    ROUTE_COST_MODE_HOPS,
    build_canonical_map_validation_report,
    compute_canonical_route,
    load_map_payload,
)
from hybrid_sensor_sim.physics.vehicle_dynamics import validate_vehicle_profile


SCENARIO_SCHEMA_VERSION_V0 = "scenario_definition_v0"


class ScenarioValidationError(ValueError):
    """Raised when a scenario payload does not match the expected schema."""


@dataclass(frozen=True)
class ActorState:
    actor_id: str
    position_m: float
    speed_mps: float
    length_m: float = 4.8
    lane_index: int = 0
    lane_id: str | None = None
    lane_binding_mode: str = "index_only"


@dataclass(frozen=True)
class ScenarioMapContext:
    map_payload: dict[str, Any]
    map_path: str | None
    validation_report: dict[str, Any]
    route_report: dict[str, Any] | None = None


@dataclass(frozen=True)
class ScenarioConfig:
    scenario_schema_version: str
    scenario_id: str
    duration_sec: float
    dt_sec: float
    ego: ActorState
    npcs: list[ActorState]
    npc_speed_jitter_mps: float = 0.0
    enable_ego_collision_avoidance: bool = False
    avoidance_ttc_threshold_sec: float = 0.0
    ego_max_brake_mps2: float = 0.0
    avoidance_interaction_policy: dict[str, dict[str, float]] | None = None
    tire_friction_coeff: float = 1.0
    surface_friction_scale: float = 1.0
    wall_timeout_sec: float | None = None
    ego_dynamics_mode: str = "kinematic"
    ego_vehicle_profile: dict[str, float] | None = None
    ego_target_speed_mps: float | None = None
    ego_road_grade_percent: float = 0.0
    map_context: ScenarioMapContext | None = None


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ScenarioValidationError("scenario must be a JSON object")
    return payload


def _require_keys(payload: dict[str, Any], keys: list[str]) -> None:
    missing = [key for key in keys if key not in payload]
    if missing:
        raise ScenarioValidationError(f"missing required keys: {missing}")


def _parse_bool(value: Any, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off", ""}:
        return False
    raise ScenarioValidationError(f"{field} must be a boolean")


def _resolve_route_lane_index(*, lane_id: str, route_lane_ids: list[str]) -> int:
    if lane_id not in route_lane_ids:
        raise ScenarioValidationError(f"lane_id not found in scenario route: {lane_id}")
    return int(route_lane_ids.index(lane_id))


def _infer_route_lane_id(*, lane_index: int, route_lane_ids: list[str]) -> str | None:
    if lane_index < 0 or lane_index >= len(route_lane_ids):
        return None
    return str(route_lane_ids[lane_index])


def _as_actor(
    payload: dict[str, Any],
    fallback_id: str,
    *,
    route_lane_ids: list[str] | None = None,
) -> ActorState:
    _require_keys(payload, ["position_m", "speed_mps"])
    lane_id_raw = payload.get("lane_id")
    lane_id = None if lane_id_raw is None else str(lane_id_raw).strip()
    if lane_id == "":
        raise ScenarioValidationError(f"{fallback_id} lane_id must be a non-empty string when provided")
    lane_index_raw = payload.get("lane_index")
    if lane_id is not None:
        if route_lane_ids is None:
            raise ScenarioValidationError(f"{fallback_id} lane_id requires route_definition")
        lane_index = _resolve_route_lane_index(lane_id=lane_id, route_lane_ids=route_lane_ids)
        if lane_index_raw is not None and int(lane_index_raw) != lane_index:
            raise ScenarioValidationError(
                f"{fallback_id} lane_index does not match route-derived lane index for lane_id={lane_id}"
            )
        lane_binding_mode = "explicit_lane_id"
    else:
        lane_index = int(payload.get("lane_index", 0))
        lane_binding_mode = "index_only"
        if route_lane_ids is not None:
            inferred_lane_id = _infer_route_lane_id(lane_index=lane_index, route_lane_ids=route_lane_ids)
            if inferred_lane_id is not None:
                lane_id = inferred_lane_id
                lane_binding_mode = "inferred_from_route"
    return ActorState(
        actor_id=str(payload.get("actor_id", fallback_id)),
        position_m=float(payload["position_m"]),
        speed_mps=float(payload["speed_mps"]),
        length_m=float(payload.get("length_m", 4.8)),
        lane_index=lane_index,
        lane_id=lane_id,
        lane_binding_mode=lane_binding_mode,
    )


def _resolve_optional_path(raw_path: Any, *, source_path: Path | None, field: str) -> Path:
    path_text = str(raw_path).strip()
    if not path_text:
        raise ScenarioValidationError(f"{field} must be a non-empty path")
    candidate = Path(path_text)
    if not candidate.is_absolute():
        base_dir = source_path.parent if source_path is not None else Path.cwd()
        candidate = (base_dir / candidate).resolve()
    else:
        candidate = candidate.resolve()
    return candidate


def _parse_avoidance_interaction_policy(raw_value: Any) -> dict[str, dict[str, float]] | None:
    if raw_value is None:
        return None
    if not isinstance(raw_value, dict):
        raise ScenarioValidationError("avoidance_interaction_policy must be an object")
    allowed_kinds = {
        "same_lane_conflict",
        "merge_conflict",
        "lane_change_conflict",
        "downstream_route_conflict",
    }
    normalized: dict[str, dict[str, float]] = {}
    for interaction_kind, interaction_policy_raw in raw_value.items():
        interaction_key = str(interaction_kind).strip()
        if interaction_key not in allowed_kinds:
            raise ScenarioValidationError(
                "avoidance_interaction_policy keys must be one of: "
                + ", ".join(sorted(allowed_kinds))
            )
        if not isinstance(interaction_policy_raw, dict):
            raise ScenarioValidationError(
                f"avoidance_interaction_policy.{interaction_key} must be an object"
            )
        normalized_policy: dict[str, float] = {}
        if "ttc_threshold_sec" in interaction_policy_raw:
            ttc_threshold_sec = float(interaction_policy_raw["ttc_threshold_sec"])
            if ttc_threshold_sec < 0:
                raise ScenarioValidationError(
                    f"avoidance_interaction_policy.{interaction_key}.ttc_threshold_sec must be >= 0"
                )
            normalized_policy["ttc_threshold_sec"] = ttc_threshold_sec
        if "brake_scale" in interaction_policy_raw:
            brake_scale = float(interaction_policy_raw["brake_scale"])
            if brake_scale < 0 or brake_scale > 1:
                raise ScenarioValidationError(
                    f"avoidance_interaction_policy.{interaction_key}.brake_scale must be between 0 and 1"
                )
            normalized_policy["brake_scale"] = brake_scale
        if normalized_policy:
            normalized[interaction_key] = normalized_policy
    return normalized


def _load_map_context(
    payload: dict[str, Any],
    *,
    source_path: Path | None,
) -> ScenarioMapContext | None:
    canonical_map_payload_raw = payload.get("canonical_map")
    canonical_map_path_raw = payload.get("canonical_map_path")
    if canonical_map_payload_raw is None and canonical_map_path_raw is None:
        if payload.get("route_definition") is not None:
            raise ScenarioValidationError("route_definition requires canonical_map or canonical_map_path")
        return None
    if canonical_map_payload_raw is not None and canonical_map_path_raw is not None:
        raise ScenarioValidationError("provide only one of canonical_map or canonical_map_path")

    map_path: Path | None = None
    if canonical_map_payload_raw is not None:
        if not isinstance(canonical_map_payload_raw, dict):
            raise ScenarioValidationError("canonical_map must be a JSON object")
        map_payload = dict(canonical_map_payload_raw)
    else:
        map_path = _resolve_optional_path(
            canonical_map_path_raw,
            source_path=source_path,
            field="canonical_map_path",
        )
        map_payload = load_map_payload(map_path, "canonical map")

    validation_report = build_canonical_map_validation_report(
        map_payload,
        map_path=map_path,
    )
    if int(validation_report["error_count"]) > 0:
        raise ScenarioValidationError(
            "invalid canonical map: " + "; ".join(validation_report["errors"])
        )

    route_definition_raw = payload.get("route_definition")
    route_report = None
    if route_definition_raw is not None:
        if not isinstance(route_definition_raw, dict):
            raise ScenarioValidationError("route_definition must be an object")
        via_lane_ids = route_definition_raw.get("via_lane_ids", [])
        if via_lane_ids is None:
            via_lane_ids = []
        if not isinstance(via_lane_ids, list):
            raise ScenarioValidationError("route_definition.via_lane_ids must be a list")
        try:
            route_report = compute_canonical_route(
                map_payload,
                entry_lane_id=str(route_definition_raw.get("entry_lane_id", "")).strip(),
                exit_lane_id=str(route_definition_raw.get("exit_lane_id", "")).strip(),
                via_lane_ids=[str(item) for item in via_lane_ids],
                cost_mode=str(route_definition_raw.get("cost_mode", ROUTE_COST_MODE_HOPS)).strip().lower(),
                map_path=map_path,
            )
        except ValueError as exc:
            raise ScenarioValidationError(f"invalid route_definition: {exc}") from exc

    return ScenarioMapContext(
        map_payload=map_payload,
        map_path=str(map_path) if map_path is not None else None,
        validation_report=validation_report,
        route_report=route_report,
    )


def validate_scenario_payload(payload: dict[str, Any], *, source_path: Path | None = None) -> ScenarioConfig:
    _require_keys(
        payload,
        ["scenario_schema_version", "scenario_id", "duration_sec", "dt_sec", "ego", "npcs"],
    )

    scenario_schema_version = str(payload["scenario_schema_version"])
    if scenario_schema_version != SCENARIO_SCHEMA_VERSION_V0:
        raise ScenarioValidationError(
            "unsupported scenario_schema_version: "
            f"{scenario_schema_version}; expected {SCENARIO_SCHEMA_VERSION_V0}"
        )

    map_context = _load_map_context(payload, source_path=source_path)
    route_lane_ids = None
    if map_context is not None and map_context.route_report is not None:
        route_lane_ids = [str(item) for item in map_context.route_report.get("route_lane_ids", [])]

    ego = _as_actor(payload["ego"], "ego", route_lane_ids=route_lane_ids)
    npcs_raw = payload["npcs"]
    if not isinstance(npcs_raw, list) or len(npcs_raw) == 0:
        raise ScenarioValidationError("npcs must be a non-empty list")
    npcs = [_as_actor(npc, f"npc_{idx + 1}", route_lane_ids=route_lane_ids) for idx, npc in enumerate(npcs_raw)]

    duration_sec = float(payload["duration_sec"])
    dt_sec = float(payload["dt_sec"])
    if duration_sec <= 0:
        raise ScenarioValidationError("duration_sec must be > 0")
    if dt_sec <= 0:
        raise ScenarioValidationError("dt_sec must be > 0")

    wall_timeout_raw = payload.get("wall_timeout_sec")
    wall_timeout_sec = None if wall_timeout_raw is None else float(wall_timeout_raw)
    if wall_timeout_sec is not None and wall_timeout_sec <= 0:
        raise ScenarioValidationError("wall_timeout_sec must be > 0 when provided")

    enable_ego_collision_avoidance = _parse_bool(
        payload.get("enable_ego_collision_avoidance", False),
        field="enable_ego_collision_avoidance",
    )
    avoidance_ttc_threshold_sec = float(payload.get("avoidance_ttc_threshold_sec", 0.0))
    ego_max_brake_mps2 = float(payload.get("ego_max_brake_mps2", 0.0))
    avoidance_interaction_policy = _parse_avoidance_interaction_policy(
        payload.get("avoidance_interaction_policy")
    )
    tire_friction_coeff = float(payload.get("tire_friction_coeff", 1.0))
    surface_friction_scale = float(payload.get("surface_friction_scale", 1.0))
    if avoidance_ttc_threshold_sec < 0:
        raise ScenarioValidationError("avoidance_ttc_threshold_sec must be >= 0")
    if ego_max_brake_mps2 < 0:
        raise ScenarioValidationError("ego_max_brake_mps2 must be >= 0")
    if tire_friction_coeff <= 0:
        raise ScenarioValidationError("tire_friction_coeff must be > 0")
    if surface_friction_scale <= 0:
        raise ScenarioValidationError("surface_friction_scale must be > 0")
    if enable_ego_collision_avoidance and (
        avoidance_ttc_threshold_sec <= 0 or ego_max_brake_mps2 <= 0
    ):
        raise ScenarioValidationError(
            "enable_ego_collision_avoidance requires avoidance_ttc_threshold_sec > 0 and ego_max_brake_mps2 > 0"
        )

    ego_dynamics_mode = str(payload.get("ego_dynamics_mode", "kinematic")).strip().lower()
    if ego_dynamics_mode not in {"kinematic", "vehicle_dynamics"}:
        raise ScenarioValidationError("ego_dynamics_mode must be one of: kinematic, vehicle_dynamics")
    ego_vehicle_profile_raw = payload.get("ego_vehicle_profile")
    ego_vehicle_profile = None
    if ego_vehicle_profile_raw is not None:
        if not isinstance(ego_vehicle_profile_raw, dict):
            raise ScenarioValidationError("ego_vehicle_profile must be a JSON object")
        try:
            ego_vehicle_profile = validate_vehicle_profile(ego_vehicle_profile_raw)
        except ValueError as exc:
            raise ScenarioValidationError(f"invalid ego_vehicle_profile: {exc}") from exc
    if ego_dynamics_mode == "vehicle_dynamics" and ego_vehicle_profile is None:
        raise ScenarioValidationError(
            "ego_dynamics_mode=vehicle_dynamics requires ego_vehicle_profile"
        )
    ego_target_speed_raw = payload.get("ego_target_speed_mps")
    ego_target_speed_mps = None
    if ego_target_speed_raw is not None:
        ego_target_speed_mps = float(ego_target_speed_raw)
        if ego_target_speed_mps < 0:
            raise ScenarioValidationError("ego_target_speed_mps must be >= 0")
    elif ego_dynamics_mode == "vehicle_dynamics":
        ego_target_speed_mps = float(ego.speed_mps)
    ego_road_grade_percent = float(payload.get("ego_road_grade_percent", 0.0))
    if ego_road_grade_percent <= -100 or ego_road_grade_percent >= 100:
        raise ScenarioValidationError("ego_road_grade_percent must be between -100 and 100")

    return ScenarioConfig(
        scenario_schema_version=scenario_schema_version,
        scenario_id=str(payload["scenario_id"]),
        duration_sec=duration_sec,
        dt_sec=dt_sec,
        ego=ego,
        npcs=npcs,
        npc_speed_jitter_mps=float(payload.get("npc_speed_jitter_mps", 0.0)),
        enable_ego_collision_avoidance=enable_ego_collision_avoidance,
        avoidance_ttc_threshold_sec=avoidance_ttc_threshold_sec,
        ego_max_brake_mps2=ego_max_brake_mps2,
        avoidance_interaction_policy=avoidance_interaction_policy,
        tire_friction_coeff=tire_friction_coeff,
        surface_friction_scale=surface_friction_scale,
        wall_timeout_sec=wall_timeout_sec,
        ego_dynamics_mode=ego_dynamics_mode,
        ego_vehicle_profile=ego_vehicle_profile,
        ego_target_speed_mps=ego_target_speed_mps,
        ego_road_grade_percent=ego_road_grade_percent,
        map_context=map_context,
    )


def load_scenario(payload_or_path: dict[str, Any] | str | Path) -> ScenarioConfig:
    if isinstance(payload_or_path, dict):
        payload = payload_or_path
        source_path = None
    else:
        source_path = Path(payload_or_path).resolve()
        payload = _load_json_object(source_path)
    return validate_scenario_payload(payload, source_path=source_path)
