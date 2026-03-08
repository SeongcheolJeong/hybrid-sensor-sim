from __future__ import annotations

from typing import Any


AUTOWARE_PIPELINE_MANIFEST_SCHEMA_VERSION_V0 = "autoware_pipeline_manifest_v0"
AUTOWARE_DATASET_MANIFEST_SCHEMA_VERSION_V0 = "autoware_dataset_manifest_v0"


def _pipeline_status(
    *,
    availability_mode: str,
    missing_required_sensor_count: int,
    output_comparison_status: str | None,
    output_smoke_status: str | None,
    strict_failed: bool,
) -> str:
    if strict_failed:
        return "FAILED"
    if availability_mode == "planned":
        if missing_required_sensor_count > 0:
            return "DEGRADED"
        return "PLANNED"
    if availability_mode == "sidecar":
        if missing_required_sensor_count > 0:
            return "SIDECAR_DEGRADED"
        if output_comparison_status and output_comparison_status != "MATCHED":
            return "SIDECAR_DEGRADED"
        if output_smoke_status and output_smoke_status != "COMPLETE":
            return "SIDECAR_DEGRADED"
        return "SIDECAR_READY"
    if availability_mode == "mixed":
        if missing_required_sensor_count > 0:
            return "MIXED_DEGRADED"
        if output_comparison_status and output_comparison_status != "MATCHED":
            return "MIXED_DEGRADED"
        if output_smoke_status and output_smoke_status != "COMPLETE":
            return "MIXED_DEGRADED"
        return "MIXED_READY"
    if missing_required_sensor_count > 0:
        return "DEGRADED"
    if output_comparison_status and output_comparison_status != "MATCHED":
        return "DEGRADED"
    if output_smoke_status and output_smoke_status != "COMPLETE":
        return "DEGRADED"
    return "READY"


def build_autoware_pipeline_manifest(
    *,
    run_id: str,
    scenario_source: dict[str, Any],
    backend: str,
    sensor_contracts: dict[str, Any],
    frame_tree: dict[str, Any],
    smoke_summary: dict[str, Any],
    artifacts: dict[str, str | None],
    strict_failed: bool,
    availability_mode: str = "runtime",
) -> dict[str, Any]:
    warnings = list(sensor_contracts.get("warnings", []))
    mismatch_reasons = list(
        ((smoke_summary.get("output_comparison") or {}).get("mismatch_reasons", []))
        if isinstance(smoke_summary.get("output_comparison"), dict)
        else []
    )
    warnings.extend(f"backend_output_mismatch:{reason}" for reason in mismatch_reasons)
    normalized_availability_mode = str(availability_mode).strip().lower() or "runtime"
    if normalized_availability_mode == "planned":
        warnings.append("planned_backend_exports_only")
    elif normalized_availability_mode == "sidecar":
        warnings.append("sidecar_materialized_backend_exports")
    elif normalized_availability_mode == "mixed":
        warnings.append("mixed_runtime_and_sidecar_backend_exports")
    missing_required_sensor_count = int(sensor_contracts.get("missing_required_sensor_count", 0) or 0)
    available_sensor_count = int(sensor_contracts.get("available_sensor_count", 0) or 0)
    available_modalities = sorted(
        {
            str(sensor.get("modality", "")).strip()
            for sensor in sensor_contracts.get("sensors", [])
            if isinstance(sensor, dict) and str(sensor.get("modality", "")).strip()
        }
    )
    output_comparison_status = (
        (smoke_summary.get("output_comparison") or {}).get("status")
        if isinstance(smoke_summary.get("output_comparison"), dict)
        else None
    )
    output_smoke_status = (
        (smoke_summary.get("output_smoke_report") or {}).get("status")
        if isinstance(smoke_summary.get("output_smoke_report"), dict)
        else None
    )
    status = _pipeline_status(
        availability_mode=normalized_availability_mode,
        missing_required_sensor_count=missing_required_sensor_count,
        output_comparison_status=output_comparison_status,
        output_smoke_status=output_smoke_status,
        strict_failed=strict_failed,
    )
    return {
        "schema_version": AUTOWARE_PIPELINE_MANIFEST_SCHEMA_VERSION_V0,
        "run_id": str(run_id).strip(),
        "backend": str(backend).strip() or None,
        "availability_mode": normalized_availability_mode,
        "scenario_source": scenario_source,
        "scenario_id": str(scenario_source.get("scenario_id", "")).strip() or None,
        "variant_id": str(scenario_source.get("variant_id", "")).strip() or None,
        "logical_scenario_id": str(scenario_source.get("logical_scenario_id", "")).strip() or None,
        "source_payload_kind": str(scenario_source.get("source_payload_kind", "")).strip() or None,
        "source_payload_path": str(scenario_source.get("source_payload_path", "")).strip() or None,
        "smoke_scenario_path": str(scenario_source.get("smoke_scenario_path", "")).strip() or None,
        "bridge_manifest_path": str(scenario_source.get("bridge_manifest_path", "")).strip() or None,
        "status": status,
        "backend_output_smoke_status": output_smoke_status,
        "backend_output_comparison_status": output_comparison_status,
        "backend_output_comparison_mismatch_reasons": mismatch_reasons,
        "backend_output_origin_status": (
            (smoke_summary.get("output_smoke_report") or {}).get("output_origin_status")
            if isinstance(smoke_summary.get("output_smoke_report"), dict)
            else None
        ),
        "backend_output_origin_counts": (
            dict((smoke_summary.get("output_smoke_report") or {}).get("output_origin_counts", {}))
            if isinstance(smoke_summary.get("output_smoke_report"), dict)
            else {}
        ),
        "backend_output_origin_reasons": (
            list((smoke_summary.get("output_smoke_report") or {}).get("output_origin_reasons", []))
            if isinstance(smoke_summary.get("output_smoke_report"), dict)
            else []
        ),
        "frame_tree_path": artifacts.get("frame_tree_path"),
        "sensor_contracts_path": artifacts.get("sensor_contracts_path"),
        "available_sensor_count": available_sensor_count,
        "missing_required_sensor_count": missing_required_sensor_count,
        "available_modalities": available_modalities,
        "frame_tree_sensor_count": int(frame_tree.get("sensor_frame_count", 0) or 0),
        "sensors": list(sensor_contracts.get("sensors", [])),
        "available_topics": list(sensor_contracts.get("available_topics", [])),
        "required_topics_complete": missing_required_sensor_count == 0,
        "frame_tree_complete": int(frame_tree.get("sensor_frame_count", 0) or 0) >= int(sensor_contracts.get("sensor_count", 0) or 0),
        "warnings": warnings,
        "artifacts": artifacts,
    }


def build_autoware_dataset_manifest(
    *,
    run_id: str,
    scenario_id: str,
    backend: str,
    sensor_contracts: dict[str, Any],
    scenario_source: dict[str, Any],
    pipeline_manifest: dict[str, Any],
    frame_tree_path: str,
    pipeline_manifest_path: str,
    sensor_contracts_path: str,
    data_roots: list[str],
    recording_style: str = "backend_smoke_export",
) -> dict[str, Any]:
    available_modalities = sorted(
        {
            str(sensor.get("modality", "")).strip()
            for sensor in sensor_contracts.get("sensors", [])
            if isinstance(sensor, dict) and str(sensor.get("modality", "")).strip()
        }
    )
    available_sensor_ids = sorted(
        {
            str(sensor.get("sensor_id", "")).strip()
            for sensor in sensor_contracts.get("sensors", [])
            if isinstance(sensor, dict) and str(sensor.get("sensor_id", "")).strip()
        }
    )
    return {
        "schema_version": AUTOWARE_DATASET_MANIFEST_SCHEMA_VERSION_V0,
        "run_id": str(run_id).strip(),
        "scenario_id": str(scenario_id).strip() or None,
        "variant_id": str(scenario_source.get("variant_id", "")).strip() or None,
        "logical_scenario_id": str(scenario_source.get("logical_scenario_id", "")).strip() or None,
        "source_payload_kind": str(scenario_source.get("source_payload_kind", "")).strip() or None,
        "source_payload_path": str(scenario_source.get("source_payload_path", "")).strip() or None,
        "smoke_scenario_path": str(scenario_source.get("smoke_scenario_path", "")).strip() or None,
        "bridge_manifest_path": str(scenario_source.get("bridge_manifest_path", "")).strip() or None,
        "backend": str(backend).strip() or None,
        "recording_style": str(recording_style).strip() or "backend_smoke_export",
        "sensor_manifest_path": sensor_contracts_path,
        "pipeline_manifest_path": pipeline_manifest_path,
        "frame_tree_path": str(frame_tree_path).strip() or None,
        "pipeline_status": str(pipeline_manifest.get("status", "")).strip() or None,
        "availability_mode": str(pipeline_manifest.get("availability_mode", "")).strip() or None,
        "required_topics_complete": bool(pipeline_manifest.get("required_topics_complete")),
        "frame_tree_complete": bool(pipeline_manifest.get("frame_tree_complete")),
        "available_sensor_count": int(pipeline_manifest.get("available_sensor_count", 0) or 0),
        "missing_required_sensor_count": int(pipeline_manifest.get("missing_required_sensor_count", 0) or 0),
        "backend_output_origin_status": str(
            pipeline_manifest.get("backend_output_origin_status", "")
        ).strip()
        or None,
        "frame_count": 0,
        "available_sensor_ids": available_sensor_ids,
        "available_modalities": available_modalities,
        "available_topics": list(sensor_contracts.get("available_topics", [])),
        "data_roots": list(data_roots),
    }
