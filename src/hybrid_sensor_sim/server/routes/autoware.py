from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException

from hybrid_sensor_sim.server.db import ControlPlaneDB
from hybrid_sensor_sim.server.models import AutowareBundleSummaryModel

router = APIRouter(prefix="/api/v1/autoware", tags=["autoware"])


def get_db() -> ControlPlaneDB:
    from hybrid_sensor_sim.server.app import get_app_db

    return get_app_db()


def _load_json(path_text: str) -> Optional[dict[str, Any]]:
    text = str(path_text).strip()
    if not text:
        return None
    path = Path(text)
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


@router.get("/{run_id}/bundle", response_model=AutowareBundleSummaryModel)
def get_autoware_bundle(run_id: str, db: ControlPlaneDB = Depends(get_db)) -> AutowareBundleSummaryModel:
    run = db.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
    artifacts = db.list_run_artifacts(run_id)
    paths = {artifact["display_name"]: artifact["path"] for artifact in artifacts}
    result_payload = run.get("result_payload", {}) if isinstance(run.get("result_payload"), dict) else {}
    pipeline_manifest_path = _find_nested_path(result_payload, "autoware_pipeline_manifest_path") or paths.get("autoware_pipeline_manifest.json", "")
    dataset_manifest_path = _find_nested_path(result_payload, "autoware_dataset_manifest_path") or paths.get("autoware_dataset_manifest.json", "")
    topic_catalog_path = _find_nested_path(result_payload, "autoware_topic_catalog_path") or paths.get("autoware_topic_catalog.json", "")
    consumer_input_manifest_path = _find_nested_path(result_payload, "autoware_consumer_input_manifest_path") or paths.get("autoware_consumer_input_manifest.json", "")
    pipeline_manifest = _load_json(pipeline_manifest_path) or {}
    topic_catalog = _load_json(topic_catalog_path) or {}
    consumer_input_manifest = _load_json(consumer_input_manifest_path) or {}
    available_topics = list(topic_catalog.get("available_topics", [])) if isinstance(topic_catalog.get("available_topics"), list) else []
    missing_required_topics = list(topic_catalog.get("missing_required_topics", [])) if isinstance(topic_catalog.get("missing_required_topics"), list) else []
    consumer_profile = str(pipeline_manifest.get("consumer_profile", "") or consumer_input_manifest.get("consumer_profile_id", ""))
    status = str(pipeline_manifest.get("status", run.get("status", "PLANNED")))
    payloads = {
        "pipeline_manifest": pipeline_manifest,
        "dataset_manifest": _load_json(dataset_manifest_path) or {},
        "topic_catalog": topic_catalog,
        "consumer_input_manifest": consumer_input_manifest,
    }
    return AutowareBundleSummaryModel(
        run_id=run_id,
        status=status,
        available_topics=available_topics,
        missing_required_topics=missing_required_topics,
        consumer_profile=consumer_profile,
        pipeline_manifest_path=str(pipeline_manifest_path),
        dataset_manifest_path=str(dataset_manifest_path),
        topic_catalog_path=str(topic_catalog_path),
        consumer_input_manifest_path=str(consumer_input_manifest_path),
        payloads=payloads,
    )


def _find_nested_path(payload: Any, field_name: str) -> str:
    if isinstance(payload, dict):
        direct = payload.get(field_name)
        if str(direct).strip():
            return str(direct).strip()
        for value in payload.values():
            match = _find_nested_path(value, field_name)
            if match:
                return match
    elif isinstance(payload, list):
        for value in payload:
            match = _find_nested_path(value, field_name)
            if match:
                return match
    return ""
