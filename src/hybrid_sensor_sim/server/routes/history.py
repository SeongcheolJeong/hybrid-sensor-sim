from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter

router = APIRouter(prefix="/api/v1/history", tags=["history"])


def _load_json(name: str) -> dict[str, Any]:
    from hybrid_sensor_sim.server.app import get_repo_root

    metadata_root = get_repo_root() / "metadata" / "autonomy_e2e"
    path = metadata_root / name
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


@router.get("/summary")
def get_history_summary() -> dict[str, Any]:
    from hybrid_sensor_sim.server.app import get_repo_root

    metadata_root = get_repo_root() / "metadata" / "autonomy_e2e"
    refresh_report = _load_json("history_refresh_report_v0.json")
    inventory = _load_json("project_inventory_v0.json")
    registry = _load_json("migration_registry_v0.json")
    projects = inventory.get("projects", []) if isinstance(inventory.get("projects"), list) else []
    blocks = registry.get("blocks", []) if isinstance(registry.get("blocks"), list) else []
    status_counts: dict[str, int] = {}
    for block in blocks:
        if not isinstance(block, dict):
            continue
        status = str(block.get("migration_status", "unknown"))
        status_counts[status] = status_counts.get(status, 0) + 1
    return {
        "schema_version": "history_summary_v0",
        "metadata_root": str(metadata_root.resolve()),
        "project_count": len(projects),
        "block_count": len(blocks),
        "migration_status_counts": status_counts,
        "refresh_report": refresh_report,
    }
