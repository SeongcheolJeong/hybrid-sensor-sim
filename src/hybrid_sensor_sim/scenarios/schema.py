from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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
    tire_friction_coeff: float = 1.0
    surface_friction_scale: float = 1.0
    wall_timeout_sec: float | None = None


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


def _as_actor(payload: dict[str, Any], fallback_id: str) -> ActorState:
    _require_keys(payload, ["position_m", "speed_mps"])
    return ActorState(
        actor_id=str(payload.get("actor_id", fallback_id)),
        position_m=float(payload["position_m"]),
        speed_mps=float(payload["speed_mps"]),
        length_m=float(payload.get("length_m", 4.8)),
        lane_index=int(payload.get("lane_index", 0)),
    )


def validate_scenario_payload(payload: dict[str, Any]) -> ScenarioConfig:
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

    ego = _as_actor(payload["ego"], "ego")
    npcs_raw = payload["npcs"]
    if not isinstance(npcs_raw, list) or len(npcs_raw) == 0:
        raise ScenarioValidationError("npcs must be a non-empty list")
    npcs = [_as_actor(npc, f"npc_{idx + 1}") for idx, npc in enumerate(npcs_raw)]

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
        tire_friction_coeff=tire_friction_coeff,
        surface_friction_scale=surface_friction_scale,
        wall_timeout_sec=wall_timeout_sec,
    )


def load_scenario(payload_or_path: dict[str, Any] | str | Path) -> ScenarioConfig:
    if isinstance(payload_or_path, dict):
        payload = payload_or_path
    else:
        payload = _load_json_object(Path(payload_or_path).resolve())
    return validate_scenario_payload(payload)
