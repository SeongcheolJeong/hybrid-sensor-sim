from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from hybrid_sensor_sim.scenarios.log_scene import validate_log_scene_payload
from hybrid_sensor_sim.scenarios.schema import SCENARIO_SCHEMA_VERSION_V0


def build_scenario_from_log_scene(log_scene: Mapping[str, Any]) -> dict[str, Any]:
    normalized = validate_log_scene_payload(dict(log_scene))
    return {
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
