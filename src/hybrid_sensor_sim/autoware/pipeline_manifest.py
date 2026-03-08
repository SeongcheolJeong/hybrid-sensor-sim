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
    missing_required_sensor_count = int(sensor_contracts.get("missing_required_sensor_count", 0) or 0)
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
        "status": status,
        "backend_output_smoke_status": output_smoke_status,
        "backend_output_comparison_status": output_comparison_status,
        "backend_output_comparison_mismatch_reasons": mismatch_reasons,
        "frame_tree_path": artifacts.get("frame_tree_path"),
        "sensor_contracts_path": artifacts.get("sensor_contracts_path"),
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
    return {
        "schema_version": AUTOWARE_DATASET_MANIFEST_SCHEMA_VERSION_V0,
        "run_id": str(run_id).strip(),
        "scenario_id": str(scenario_id).strip() or None,
        "backend": str(backend).strip() or None,
        "recording_style": str(recording_style).strip() or "backend_smoke_export",
        "sensor_manifest_path": sensor_contracts_path,
        "pipeline_manifest_path": pipeline_manifest_path,
        "frame_count": 0,
        "available_modalities": available_modalities,
        "available_topics": list(sensor_contracts.get("available_topics", [])),
        "data_roots": list(data_roots),
    }
