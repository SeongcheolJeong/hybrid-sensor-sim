from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter

from hybrid_sensor_sim.server.models import ScenarioAssetModel

router = APIRouter(prefix="/api/v1/scenarios", tags=["scenarios"])


def _asset_kind_for_payload(path: Path, payload: dict[str, Any]) -> str:
    schema_version = str(payload.get("schema_version", "")).strip()
    if schema_version:
        return schema_version
    if path.name.endswith("logical_scenarios_v0.json") or path.parent.name == "p_validation":
        return "logical_scenarios_v0"
    return "json_asset"


def _scenario_roots() -> list[Path]:
    from hybrid_sensor_sim.server.app import get_repo_root

    repo_root = get_repo_root()
    return [
        repo_root / "tests" / "fixtures" / "autonomy_e2e",
        repo_root / "configs",
    ]


@router.get("", response_model=list[ScenarioAssetModel])
def list_scenarios() -> list[ScenarioAssetModel]:
    assets: list[ScenarioAssetModel] = []
    for root in _scenario_roots():
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            source_group = path.parent.name
            asset_kind = _asset_kind_for_payload(path, payload)
            schema_version_hint = str(payload.get("schema_version", "")).strip()
            digest = hashlib.sha1(str(path.resolve()).encode("utf-8")).hexdigest()[:12]
            assets.append(
                ScenarioAssetModel(
                    asset_id=f"asset-{digest}",
                    name=path.stem,
                    path=str(path.resolve()),
                    asset_kind=asset_kind,
                    source_group=source_group,
                    schema_version_hint=schema_version_hint,
                    description=str(payload.get("description", "")).strip(),
                )
            )
    return assets
