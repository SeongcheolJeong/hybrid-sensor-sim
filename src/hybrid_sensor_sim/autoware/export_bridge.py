from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.config import build_sensor_sim_config
from hybrid_sensor_sim.autoware.contracts import build_autoware_sensor_contracts
from hybrid_sensor_sim.autoware.frames import build_autoware_frame_tree
from hybrid_sensor_sim.autoware.pipeline_manifest import (
    build_autoware_dataset_manifest,
    build_autoware_pipeline_manifest,
)
from hybrid_sensor_sim.renderers.backend_runner import _build_sensor_expected_output_entries


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _sensor_mount(
    *,
    sensor_id: str,
    sensor_type: str,
    attach_to_actor_id: str,
    enabled: bool,
    extrinsics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "sensor_id": str(sensor_id).strip(),
        "sensor_type": str(sensor_type).strip(),
        "attach_to_actor_id": str(attach_to_actor_id).strip() or None,
        "enabled": bool(enabled),
        "extrinsics": dict(extrinsics),
        "extrinsics_source": "planned_config",
    }


def _camera_data_format(sensor_type: str) -> str:
    normalized = str(sensor_type).strip().upper()
    if normalized == "DEPTH":
        return "camera_depth_json"
    if normalized == "SEMANTIC_SEGMENTATION":
        return "camera_semantic_json"
    return "camera_projection_json"


def _build_planned_sensor_mounts_and_entries(
    *,
    smoke_input_config: dict[str, Any],
    backend: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    config = build_sensor_sim_config(options=smoke_input_config)
    sensor_mounts: list[dict[str, Any]] = []
    ingestion_entries: list[dict[str, Any]] = []

    camera_enabled = bool(
        config.camera.projection_enabled or config.camera.trajectory_sweep_enabled
    )
    if camera_enabled:
        sensor_mounts.append(
            _sensor_mount(
                sensor_id=config.camera.sensor_id,
                sensor_type="camera",
                attach_to_actor_id=config.camera.attach_to_actor_id,
                enabled=True,
                extrinsics=config.camera.extrinsics.to_dict(),
            )
        )
        ingestion_entries.append(
            {
                "sensor_id": config.camera.sensor_id,
                "sensor_name": config.camera.sensor_id,
                "data_format": _camera_data_format(config.camera.sensor_type),
            }
        )

    lidar_enabled = bool(
        config.lidar.postprocess_enabled or config.lidar.trajectory_sweep_enabled
    )
    if lidar_enabled:
        sensor_mounts.append(
            _sensor_mount(
                sensor_id=config.lidar.sensor_id,
                sensor_type="lidar",
                attach_to_actor_id=config.lidar.attach_to_actor_id,
                enabled=True,
                extrinsics=config.lidar.extrinsics.to_dict(),
            )
        )
        ingestion_entries.append(
            {
                "sensor_id": config.lidar.sensor_id,
                "sensor_name": config.lidar.sensor_id,
                "data_format": "lidar_points_json",
            }
        )

    radar_enabled = bool(
        config.radar.postprocess_enabled or config.radar.trajectory_sweep_enabled
    )
    if radar_enabled:
        sensor_mounts.append(
            _sensor_mount(
                sensor_id=config.radar.sensor_id,
                sensor_type="radar",
                attach_to_actor_id=config.radar.attach_to_actor_id,
                enabled=True,
                extrinsics=config.radar.extrinsics.to_dict(),
            )
        )
        ingestion_entries.append(
            {
                "sensor_id": config.radar.sensor_id,
                "sensor_name": config.radar.sensor_id,
                "data_format": (
                    "radar_tracks_json"
                    if config.radar.tracking.output_tracks
                    else "radar_targets_json"
                ),
            }
        )
    return sensor_mounts, ingestion_entries


def _build_planned_backend_artifacts(
    *,
    out_root: Path,
    backend: str,
    smoke_input_config: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    planned_root = out_root / "autoware" / "planned_backend"
    planned_root.mkdir(parents=True, exist_ok=True)
    sensor_mounts, ingestion_entries = _build_planned_sensor_mounts_and_entries(
        smoke_input_config=smoke_input_config,
        backend=backend,
    )
    ingestion_profile_path = planned_root / "planned_backend_ingestion_profile.json"
    ingestion_payload = {
        "backend": backend,
        "entry_count": len(ingestion_entries),
        "entries": ingestion_entries,
    }
    _write_json(ingestion_profile_path, ingestion_payload)
    output_root = planned_root / "backend_outputs" / backend
    expected_outputs = _build_sensor_expected_output_entries(
        backend=backend,
        output_root=output_root,
        ingestion_profile_path=ingestion_profile_path,
    )
    expected_outputs_by_sensor: dict[str, list[dict[str, Any]]] = {}
    for entry in expected_outputs:
        sensor_id = str(entry.get("sensor_id", "")).strip()
        if not sensor_id:
            continue
        expected_outputs_by_sensor.setdefault(sensor_id, []).append(
            {
                "artifact_key": str(entry.get("artifact_key", "")).strip(),
                "backend": str(entry.get("backend", "")).strip(),
                "modality": str(entry.get("modality", "")).strip(),
                "backend_filename": str(entry.get("backend_filename", "")).strip(),
                "output_role": str(entry.get("output_role", "")).strip(),
                "artifact_type": str(entry.get("artifact_type", "")).strip(),
                "sensor_name": str(entry.get("sensor_name", "")).strip() or sensor_id,
                "data_format": str(entry.get("data_format", "")).strip(),
                "carrier_data_format": str(entry.get("carrier_data_format", "")).strip(),
                "relative_path": str(entry.get("relative_path", "")).strip(),
                "embedded_output": bool(entry.get("embedded_output", False)),
                "embedded_field": str(entry.get("embedded_field", "")).strip(),
                "shared_output_artifact_key": str(
                    entry.get("shared_output_artifact_key", "")
                ).strip(),
                "path_candidates": list(entry.get("path_candidates", []))
                if isinstance(entry.get("path_candidates"), list)
                else [],
            }
        )
    backend_output_spec = {
        "backend": backend,
        "output_root": str(output_root),
        "expected_outputs": expected_outputs,
        "expected_output_count": len(expected_outputs),
        "expected_outputs_by_sensor": [
            {
                "sensor_id": sensor_id,
                "output_count": len(outputs),
                "outputs": outputs,
            }
            for sensor_id, outputs in sorted(expected_outputs_by_sensor.items())
        ],
    }
    backend_output_spec_path = planned_root / "autoware_planned_backend_output_spec.json"
    _write_json(backend_output_spec_path, backend_output_spec)
    backend_output_spec["__source_path"] = str(backend_output_spec_path.resolve())

    backend_sensor_output_summary = {
        "backend": backend,
        "sensors": [
            {
                "sensor_id": sensor_id,
                "modality": next(
                    (
                        str(output.get("modality", "")).strip()
                        for output in outputs
                        if str(output.get("modality", "")).strip()
                    ),
                    "",
                )
                or None,
                "outputs": [
                    {
                        "output_role": str(output.get("output_role", "")).strip(),
                        "artifact_type": str(output.get("artifact_type", "")).strip() or None,
                        "data_format": str(output.get("data_format", "")).strip() or None,
                        "artifact_key": str(output.get("artifact_key", "")).strip() or None,
                        "resolved_path": str(output.get("path", "")).strip() or None,
                        "exists": False,
                        "path_candidates": list(output.get("path_candidates", []))
                        if isinstance(output.get("path_candidates"), list)
                        else [],
                    }
                    for output in outputs
                ],
            }
            for sensor_id, outputs in sorted(expected_outputs_by_sensor.items())
        ],
    }
    backend_sensor_output_summary_path = (
        planned_root / "autoware_planned_backend_sensor_output_summary.json"
    )
    _write_json(backend_sensor_output_summary_path, backend_sensor_output_summary)
    backend_sensor_output_summary["__source_path"] = str(
        backend_sensor_output_summary_path.resolve()
    )
    return sensor_mounts, backend_output_spec, backend_sensor_output_summary


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
        availability_mode="runtime",
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
        recording_style="backend_smoke_export",
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


def write_autoware_planned_export_bundle(
    *,
    out_root: Path,
    backend: str,
    run_id: str,
    scenario_source: dict[str, Any],
    smoke_input_config: dict[str, Any],
    strict: bool = False,
    base_frame: str = "base_link",
) -> dict[str, Any]:
    autoware_root = out_root / "autoware"
    autoware_root.mkdir(parents=True, exist_ok=True)
    sensor_mounts, backend_output_spec, backend_sensor_output_summary = _build_planned_backend_artifacts(
        out_root=out_root,
        backend=backend,
        smoke_input_config=smoke_input_config,
    )
    frame_tree = build_autoware_frame_tree(sensor_mounts, base_frame=base_frame)
    sensor_contracts = build_autoware_sensor_contracts(
        backend_sensor_output_summary=backend_sensor_output_summary,
        backend_output_spec=backend_output_spec,
        sensor_mounts=sensor_mounts,
        availability_mode="planned",
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
        "backend_output_spec_path": str(backend_output_spec.get("__source_path", "")),
        "backend_sensor_output_summary_path": str(
            backend_sensor_output_summary.get("__source_path", "")
        ),
    }
    pipeline_manifest = build_autoware_pipeline_manifest(
        run_id=run_id,
        scenario_source=scenario_source,
        backend=backend,
        sensor_contracts=sensor_contracts,
        frame_tree=frame_tree,
        smoke_summary={},
        artifacts=artifacts,
        strict_failed=strict_failed,
        availability_mode="planned",
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
                str(
                    Path(str(backend_sensor_output_summary.get("__source_path", ""))).resolve().parent
                ),
            }
            - {""}
        ),
        recording_style="planned_backend_export",
    )
    _write_json(pipeline_manifest_path, pipeline_manifest)
    _write_json(dataset_manifest_path, dataset_manifest)
    warnings = list(sensor_contracts.get("warnings", []))
    if strict_failed:
        warnings.append("strict_mode_missing_required_sensor_outputs")
    return {
        "status": pipeline_manifest["status"],
        "strict": bool(strict),
        "availability_mode": "planned",
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
            "backend_output_spec_path": str(backend_output_spec.get("__source_path", "")),
            "backend_sensor_output_summary_path": str(
                backend_sensor_output_summary.get("__source_path", "")
            ),
        },
        "sensor_contracts": sensor_contracts,
        "frame_tree": frame_tree,
        "pipeline_manifest": pipeline_manifest,
        "dataset_manifest": dataset_manifest,
    }
