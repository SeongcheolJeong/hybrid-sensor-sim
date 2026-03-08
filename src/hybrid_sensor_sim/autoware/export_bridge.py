from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.autoware.contracts import build_autoware_sensor_contracts
from hybrid_sensor_sim.autoware.frames import build_autoware_frame_tree
from hybrid_sensor_sim.autoware.pipeline_manifest import (
    build_autoware_dataset_manifest,
    build_autoware_pipeline_manifest,
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def write_autoware_export_bundle(
    *,
    out_root: Path,
    backend: str,
    run_id: str,
    scenario_source: dict[str, Any],
    backend_sensor_output_summary: dict[str, Any],
    backend_output_spec: dict[str, Any],
    playback_contract: dict[str, Any],
    smoke_summary: dict[str, Any],
    strict: bool = False,
    base_frame: str = "base_link",
) -> dict[str, Any]:
    autoware_root = out_root / "autoware"
    autoware_root.mkdir(parents=True, exist_ok=True)
    sensor_mounts = list(playback_contract.get("renderer_sensor_mounts", []) or [])
    frame_tree = build_autoware_frame_tree(sensor_mounts, base_frame=base_frame)
    sensor_contracts = build_autoware_sensor_contracts(
        backend_sensor_output_summary=backend_sensor_output_summary,
        backend_output_spec=backend_output_spec,
        sensor_mounts=sensor_mounts,
    )
    strict_failed = bool(strict and int(sensor_contracts.get("missing_required_sensor_count", 0) or 0) > 0)
    frame_tree_path = autoware_root / "autoware_frame_tree.json"
    sensor_contracts_path = autoware_root / "autoware_sensor_contracts.json"
    pipeline_manifest_path = autoware_root / "autoware_pipeline_manifest.json"
    dataset_manifest_path = autoware_root / "autoware_dataset_manifest.json"
    _write_json(frame_tree_path, frame_tree)
    _write_json(sensor_contracts_path, sensor_contracts)
    artifacts = {
        "frame_tree_path": str(frame_tree_path.resolve()),
        "sensor_contracts_path": str(sensor_contracts_path.resolve()),
        "pipeline_manifest_path": str(pipeline_manifest_path.resolve()),
        "dataset_manifest_path": str(dataset_manifest_path.resolve()),
        "backend_output_spec_path": str((backend_output_spec.get("__source_path") or "")),
        "backend_sensor_output_summary_path": str((backend_sensor_output_summary.get("__source_path") or "")),
    }
    pipeline_manifest = build_autoware_pipeline_manifest(
        run_id=run_id,
        scenario_source=scenario_source,
        backend=backend,
        sensor_contracts=sensor_contracts,
        frame_tree=frame_tree,
        smoke_summary=smoke_summary,
        artifacts=artifacts,
        strict_failed=strict_failed,
    )
    dataset_manifest = build_autoware_dataset_manifest(
        run_id=run_id,
        scenario_id=str(scenario_source.get("scenario_id", "")).strip(),
        backend=backend,
        sensor_contracts=sensor_contracts,
        pipeline_manifest_path=str(pipeline_manifest_path.resolve()),
        sensor_contracts_path=str(sensor_contracts_path.resolve()),
        data_roots=sorted(
            {
                str(backend_output_spec.get("output_root", "")).strip(),
                str(Path(str((backend_sensor_output_summary.get("__source_path") or ""))).resolve().parent),
            }
            - {""}
        ),
    )
    _write_json(pipeline_manifest_path, pipeline_manifest)
    _write_json(dataset_manifest_path, dataset_manifest)
    warnings = list(sensor_contracts.get("warnings", []))
    if strict_failed:
        warnings.append("strict_mode_missing_required_sensor_outputs")
    return {
        "status": pipeline_manifest["status"],
        "strict": bool(strict),
        "base_frame": str(base_frame).strip() or "base_link",
        "available_sensor_count": int(sensor_contracts.get("available_sensor_count", 0) or 0),
        "missing_required_sensor_count": int(sensor_contracts.get("missing_required_sensor_count", 0) or 0),
        "available_topics": list(sensor_contracts.get("available_topics", [])),
        "required_topics_complete": bool(pipeline_manifest.get("required_topics_complete")),
        "frame_tree_complete": bool(pipeline_manifest.get("frame_tree_complete")),
        "warnings": warnings,
        "artifacts": {
            "sensor_contracts_path": str(sensor_contracts_path.resolve()),
            "frame_tree_path": str(frame_tree_path.resolve()),
            "pipeline_manifest_path": str(pipeline_manifest_path.resolve()),
            "dataset_manifest_path": str(dataset_manifest_path.resolve()),
        },
        "sensor_contracts": sensor_contracts,
        "frame_tree": frame_tree,
        "pipeline_manifest": pipeline_manifest,
        "dataset_manifest": dataset_manifest,
    }
