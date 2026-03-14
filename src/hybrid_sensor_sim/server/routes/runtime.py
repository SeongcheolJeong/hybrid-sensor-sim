from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter

from hybrid_sensor_sim.server.models import RuntimeStrategySummaryModel

router = APIRouter(prefix="/api/v1/runtime", tags=["runtime"])


def _load_json(path: Path) -> Optional[dict[str, Any]]:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _local_setup_summary_path() -> Path:
    from hybrid_sensor_sim.server.app import get_repo_root

    return (
        get_repo_root()
        / "artifacts"
        / "renderer_backend_local_setup_probe_latest"
        / "renderer_backend_local_setup.json"
    )


def _probe_report_paths() -> list[Path]:
    from hybrid_sensor_sim.server.app import get_repo_root

    repo_root = get_repo_root()
    return [
        repo_root / "artifacts" / "scenario_runtime_backend_probe_set_real_awsim_v0" / "scenario_runtime_backend_probe_set_report_v0.json",
        repo_root / "artifacts" / "hybrid_runtime_readiness_after_reboot" / "scenario_runtime_backend_probe_set_report_v0.json",
    ]


@router.get("/strategy-summary", response_model=RuntimeStrategySummaryModel)
def get_runtime_strategy_summary() -> RuntimeStrategySummaryModel:
    local_setup_summary = _local_setup_summary_path()
    local_setup = _load_json(local_setup_summary) or {}
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    selection = local_setup.get("selection", {}) if isinstance(local_setup, dict) else {}
    readiness = local_setup.get("readiness", {}) if isinstance(local_setup, dict) else {}
    runtime_strategy = local_setup.get("runtime_strategy", {}) if isinstance(local_setup, dict) else {}
    backends = []
    for backend in ("awsim", "carla"):
        backend_upper = backend.upper()
        backends.append(
            {
                "backend": backend,
                "selected_path": selection.get(f"{backend_upper}_BIN") or selection.get(f"{backend_upper}_DOCKER_IMAGE", ""),
                "readiness": readiness.get(f"{backend}_ready"),
                "host_compatible": readiness.get(f"{backend}_host_compatible"),
                "preferred_runtime_source": runtime_strategy.get(f"{backend}_preferred_runtime_source", ""),
                "strategy": runtime_strategy.get(f"{backend}_strategy", ""),
                "reason_codes": runtime_strategy.get(f"{backend}_strategy_reason_codes", []),
                "recommended_command": runtime_strategy.get(f"{backend}_recommended_command", ""),
            }
        )
    blockers = []
    issues = local_setup.get("issues", []) if isinstance(local_setup.get("issues"), list) else []
    for issue in issues:
        if isinstance(issue, dict):
            blockers.append(issue)
    probe_sets = []
    for path in _probe_report_paths():
        payload = _load_json(path)
        if not payload:
            continue
        probe_sets.append(
            {
                "path": str(path.resolve()),
                "probe_set_id": payload.get("probe_set_id", ""),
                "status": payload.get("status", ""),
                "recommended_next_command": payload.get("recommended_next_command", ""),
            }
        )
    recommended_next_command = ""
    for backend in backends:
        command = str(backend.get("recommended_command", "")).strip()
        if command:
            recommended_next_command = command
            break
    return RuntimeStrategySummaryModel(
        generated_at=generated_at,
        local_setup_path=str(local_setup_summary.resolve()) if local_setup_summary.exists() else "",
        backends=backends,
        blockers=blockers,
        probe_sets=probe_sets,
        recommended_next_command=recommended_next_command,
    )
