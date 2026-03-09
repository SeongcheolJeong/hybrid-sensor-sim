from __future__ import annotations

from typing import Any

from hybrid_sensor_sim.autoware.profiles import resolve_autoware_consumer_profile


AUTOWARE_PIPELINE_MANIFEST_SCHEMA_VERSION_V0 = "autoware_pipeline_manifest_v0"
AUTOWARE_DATASET_MANIFEST_SCHEMA_VERSION_V0 = "autoware_dataset_manifest_v0"
AUTOWARE_CONSUMER_INPUT_MANIFEST_SCHEMA_VERSION_V0 = "autoware_consumer_input_manifest_v0"
AUTOWARE_PROCESSING_STAGE_BUNDLE_INDEX_SCHEMA_VERSION_V0 = (
    "autoware_processing_stage_bundle_index_v0"
)


def _clean_optional_text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _build_processing_stage_entries(
    *,
    consumer_profile_id: str | None,
    consumer_topics: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    profile = resolve_autoware_consumer_profile(consumer_profile_id)
    if profile is None:
        return []
    topics_by_output_role: dict[str, list[dict[str, Any]]] = {}
    for topic in consumer_topics:
        if not isinstance(topic, dict):
            continue
        output_role = str(topic.get("output_role", "")).strip()
        if not output_role:
            continue
        topics_by_output_role.setdefault(output_role, []).append(topic)
    stage_entries: list[dict[str, Any]] = []
    for stage in list(profile.get("processing_stages", []) or []):
        if not isinstance(stage, dict):
            continue
        stage_id = str(stage.get("stage_id", "")).strip()
        if not stage_id:
            continue
        required_output_roles = [
            str(role).strip()
            for role in list(stage.get("required_output_roles", []) or [])
            if str(role).strip()
        ]
        stage_topics: list[dict[str, Any]] = []
        for output_role in required_output_roles:
            stage_topics.extend(topics_by_output_role.get(output_role, []))
        stage_required_topics = sorted(
            {
                str(topic.get("topic", "")).strip()
                for topic in stage_topics
                if bool(topic.get("required")) and str(topic.get("topic", "")).strip()
            }
        )
        stage_available_topics = sorted(
            {
                str(topic.get("topic", "")).strip()
                for topic in stage_topics
                if bool(topic.get("available")) and str(topic.get("topic", "")).strip()
            }
        )
        stage_missing_required_topics = sorted(
            set(stage_required_topics) - set(stage_available_topics)
        )
        required_sensor_ids = sorted(
            {
                str(topic.get("sensor_id", "")).strip()
                for topic in stage_topics
                if bool(topic.get("required")) and str(topic.get("sensor_id", "")).strip()
            }
        )
        available_sensor_ids = sorted(
            {
                str(topic.get("sensor_id", "")).strip()
                for topic in stage_topics
                if bool(topic.get("available")) and str(topic.get("sensor_id", "")).strip()
            }
        )
        stage_entries.append(
            {
                "stage_id": stage_id,
                "description": str(stage.get("description", "")).strip() or None,
                "required_output_roles": required_output_roles,
                "required_topic_count": len(stage_required_topics),
                "available_topic_count": len(stage_available_topics),
                "missing_required_topic_count": len(stage_missing_required_topics),
                "required_topics": stage_required_topics,
                "available_topics": stage_available_topics,
                "missing_required_topics": stage_missing_required_topics,
                "required_sensor_ids": required_sensor_ids,
                "available_sensor_ids": available_sensor_ids,
                "ready": bool(stage_required_topics) and not stage_missing_required_topics,
            }
        )
    return stage_entries


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
        "consumer_profile_id": _clean_optional_text(sensor_contracts.get("consumer_profile_id")),
        "consumer_profile_description": _clean_optional_text(
            sensor_contracts.get("consumer_profile_description")
        ),
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
    topic_export_root: str,
    topic_export_index_path: str,
    topic_catalog_path: str,
    topic_export_count: int,
    materialized_topic_export_count: int,
    required_topic_count: int,
    missing_required_topic_count: int,
    available_message_types: list[str],
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
        "consumer_profile_id": _clean_optional_text(sensor_contracts.get("consumer_profile_id")),
        "consumer_profile_description": _clean_optional_text(
            sensor_contracts.get("consumer_profile_description")
        ),
        "recording_style": str(recording_style).strip() or "backend_smoke_export",
        "sensor_manifest_path": sensor_contracts_path,
        "pipeline_manifest_path": pipeline_manifest_path,
        "frame_tree_path": str(frame_tree_path).strip() or None,
        "topic_export_root": str(topic_export_root).strip() or None,
        "topic_export_index_path": str(topic_export_index_path).strip() or None,
        "topic_catalog_path": str(topic_catalog_path).strip() or None,
        "topic_export_count": int(topic_export_count),
        "materialized_topic_export_count": int(materialized_topic_export_count),
        "required_topic_count": int(required_topic_count),
        "missing_required_topic_count": int(missing_required_topic_count),
        "available_message_types": list(available_message_types),
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


def build_autoware_consumer_input_manifest(
    *,
    run_id: str,
    backend: str,
    scenario_source: dict[str, Any],
    pipeline_manifest: dict[str, Any],
    dataset_manifest: dict[str, Any],
    topic_catalog: dict[str, Any],
    frame_tree: dict[str, Any],
    artifacts: dict[str, str | None],
) -> dict[str, Any]:
    topic_entries = list(topic_catalog.get("entries", []) or [])
    frame_entries = list(frame_tree.get("sensor_frames", []) or [])
    frame_by_sensor_id: dict[str, dict[str, Any]] = {}
    for entry in frame_entries:
        if not isinstance(entry, dict):
            continue
        sensor_id = str(entry.get("sensor_id", "")).strip()
        if not sensor_id:
            continue
        frame_by_sensor_id[sensor_id] = entry
    consumer_topics: list[dict[str, Any]] = []
    subscription_specs: list[dict[str, Any]] = []
    sensor_input_map: dict[str, dict[str, Any]] = {}
    required_topics: list[str] = []
    available_topics: list[str] = []
    missing_required_topics: list[str] = []
    for entry in topic_entries:
        if not isinstance(entry, dict):
            continue
        topic = str(entry.get("topic", "")).strip()
        if not topic:
            continue
        required = bool(entry.get("required"))
        available = bool(entry.get("available"))
        if required:
            required_topics.append(topic)
        if available:
            available_topics.append(topic)
        if required and not available:
            missing_required_topics.append(topic)
        message_type = _clean_optional_text(entry.get("message_type"))
        frame_id = _clean_optional_text(entry.get("frame_id"))
        sensor_id = _clean_optional_text(entry.get("sensor_id"))
        modality = _clean_optional_text(entry.get("modality"))
        output_role = _clean_optional_text(entry.get("output_role"))
        availability_mode = _clean_optional_text(entry.get("availability_mode"))
        output_origin = _clean_optional_text(entry.get("output_origin"))
        payload_path = _clean_optional_text(entry.get("payload_path"))
        payload_exists = bool(entry.get("payload_exists"))
        payload_materialization_mode = _clean_optional_text(
            entry.get("payload_materialization_mode")
        )
        export_manifest_path = _clean_optional_text(entry.get("export_manifest_path"))
        consumer_topic = {
            "topic": topic,
            "message_type": message_type,
            "frame_id": frame_id,
            "sensor_id": sensor_id,
            "modality": modality,
            "output_role": output_role,
            "required": required,
            "available": available,
            "availability_mode": availability_mode,
            "output_origin": output_origin,
            "payload_path": payload_path,
            "payload_exists": payload_exists,
            "payload_materialization_mode": payload_materialization_mode,
            "export_manifest_path": export_manifest_path,
        }
        consumer_topics.append(consumer_topic)
        subscription_specs.append(
            {
                "topic": topic,
                "message_type": message_type,
                "frame_id": frame_id,
                "required": required,
                "available": available,
                "sensor_id": sensor_id,
                "modality": modality,
                "output_role": output_role,
                "availability_mode": availability_mode,
                "output_origin": output_origin,
                "payload_path": payload_path,
                "payload_exists": payload_exists,
                "payload_materialization_mode": payload_materialization_mode,
            }
        )
        if sensor_id:
            frame_entry = frame_by_sensor_id.get(sensor_id, {})
            sensor_input = sensor_input_map.setdefault(
                sensor_id,
                {
                    "sensor_id": sensor_id,
                    "modality": modality,
                    "frame_id": frame_id or str(frame_entry.get("frame_id", "")).strip() or None,
                    "parent_frame_id": str(frame_entry.get("parent_frame_id", "")).strip() or None,
                    "attach_to_actor_id": str(frame_entry.get("attach_to_actor_id", "")).strip() or None,
                    "required_topic_count": 0,
                    "available_topic_count": 0,
                    "missing_required_topic_count": 0,
                    "required_topics": [],
                    "available_topics": [],
                    "missing_required_topics": [],
                    "subscriptions": [],
                },
            )
            sensor_input["modality"] = sensor_input.get("modality") or modality
            if required:
                sensor_input["required_topic_count"] += 1
                sensor_input["required_topics"].append(topic)
            if available:
                sensor_input["available_topic_count"] += 1
                sensor_input["available_topics"].append(topic)
            if required and not available:
                sensor_input["missing_required_topic_count"] += 1
                sensor_input["missing_required_topics"].append(topic)
            sensor_input["subscriptions"].append(
                {
                    "topic": topic,
                    "message_type": message_type,
                    "output_role": output_role,
                    "required": required,
                    "available": available,
                    "payload_path": payload_path,
                    "payload_exists": payload_exists,
                }
            )
    consumer_topics.sort(key=lambda item: item["topic"] or "")
    subscription_specs.sort(key=lambda item: item["topic"] or "")
    sensor_inputs = []
    for sensor_id in sorted(sensor_input_map):
        sensor_input = sensor_input_map[sensor_id]
        sensor_input["required_topics"] = sorted(sensor_input["required_topics"])
        sensor_input["available_topics"] = sorted(sensor_input["available_topics"])
        sensor_input["missing_required_topics"] = sorted(
            sensor_input["missing_required_topics"]
        )
        sensor_input["subscriptions"] = sorted(
            sensor_input["subscriptions"], key=lambda item: item["topic"] or ""
        )
        sensor_inputs.append(sensor_input)
    static_transforms = []
    for entry in sorted(frame_entries, key=lambda item: str(item.get("sensor_id", "")).strip()):
        if not isinstance(entry, dict):
            continue
        frame_id = str(entry.get("frame_id", "")).strip()
        if not frame_id:
            continue
        static_transforms.append(
            {
                "sensor_id": str(entry.get("sensor_id", "")).strip() or None,
                "sensor_type": str(entry.get("sensor_type", "")).strip() or None,
                "frame_id": frame_id,
                "child_frame_id": frame_id,
                "parent_frame_id": str(entry.get("parent_frame_id", "")).strip() or None,
                "attach_to_actor_id": str(entry.get("attach_to_actor_id", "")).strip() or None,
                "enabled": bool(entry.get("enabled", True)),
                "translation": dict(entry.get("translation", {}))
                if isinstance(entry.get("translation"), dict)
                else {},
                "rotation_rpy": dict(entry.get("rotation_rpy", {}))
                if isinstance(entry.get("rotation_rpy"), dict)
                else {},
            }
        )
    consumer_profile_id = _clean_optional_text(pipeline_manifest.get("consumer_profile_id"))
    processing_stages = _build_processing_stage_entries(
        consumer_profile_id=consumer_profile_id,
        consumer_topics=consumer_topics,
    )
    ready_processing_stage_count = sum(
        1 for stage in processing_stages if bool(stage.get("ready"))
    )
    return {
        "schema_version": AUTOWARE_CONSUMER_INPUT_MANIFEST_SCHEMA_VERSION_V0,
        "run_id": str(run_id).strip(),
        "backend": str(backend).strip() or None,
        "status": str(pipeline_manifest.get("status", "")).strip() or None,
        "availability_mode": str(pipeline_manifest.get("availability_mode", "")).strip()
        or None,
        "consumer_profile_id": consumer_profile_id,
        "consumer_profile_description": _clean_optional_text(
            pipeline_manifest.get("consumer_profile_description")
        ),
        "consumer_ready": bool(
            pipeline_manifest.get("required_topics_complete")
            and pipeline_manifest.get("frame_tree_complete")
            and topic_catalog.get("available_topic_count", 0)
        ),
        "scenario_id": str(scenario_source.get("scenario_id", "")).strip() or None,
        "variant_id": str(scenario_source.get("variant_id", "")).strip() or None,
        "logical_scenario_id": str(scenario_source.get("logical_scenario_id", "")).strip()
        or None,
        "source_payload_kind": str(scenario_source.get("source_payload_kind", "")).strip()
        or None,
        "required_topic_count": int(topic_catalog.get("required_topic_count", 0) or 0),
        "missing_required_topic_count": int(
            topic_catalog.get("missing_required_topic_count", 0) or 0
        ),
        "available_topic_count": int(topic_catalog.get("available_topic_count", 0) or 0),
        "required_topics": sorted(required_topics),
        "available_topics": sorted(available_topics),
        "missing_required_topics": sorted(missing_required_topics),
        "available_message_types": list(topic_catalog.get("available_message_types", [])),
        "subscription_spec_count": len(subscription_specs),
        "sensor_input_count": len(sensor_inputs),
        "static_transform_count": len(static_transforms),
        "processing_stage_count": len(processing_stages),
        "ready_processing_stage_count": ready_processing_stage_count,
        "degraded_processing_stage_count": len(processing_stages) - ready_processing_stage_count,
        "frame_tree_path": artifacts.get("frame_tree_path"),
        "sensor_contracts_path": artifacts.get("sensor_contracts_path"),
        "pipeline_manifest_path": artifacts.get("pipeline_manifest_path"),
        "dataset_manifest_path": artifacts.get("dataset_manifest_path"),
        "topic_export_root": artifacts.get("topic_export_root"),
        "topic_export_index_path": artifacts.get("topic_export_index_path"),
        "topic_catalog_path": artifacts.get("topic_catalog_path"),
        "consumer_topics": consumer_topics,
        "subscription_specs": subscription_specs,
        "sensor_inputs": sensor_inputs,
        "static_transforms": static_transforms,
        "processing_stages": processing_stages,
    }


def build_autoware_processing_stage_bundle_index(
    *,
    consumer_input_manifest: dict[str, Any],
) -> dict[str, Any]:
    topic_by_name: dict[str, dict[str, Any]] = {}
    for topic in list(consumer_input_manifest.get("consumer_topics", []) or []):
        if not isinstance(topic, dict):
            continue
        topic_name = str(topic.get("topic", "")).strip()
        if topic_name:
            topic_by_name[topic_name] = topic

    sensor_input_by_id: dict[str, dict[str, Any]] = {}
    for sensor_input in list(consumer_input_manifest.get("sensor_inputs", []) or []):
        if not isinstance(sensor_input, dict):
            continue
        sensor_id = str(sensor_input.get("sensor_id", "")).strip()
        if sensor_id:
            sensor_input_by_id[sensor_id] = sensor_input

    transform_by_sensor_id: dict[str, dict[str, Any]] = {}
    for transform in list(consumer_input_manifest.get("static_transforms", []) or []):
        if not isinstance(transform, dict):
            continue
        sensor_id = str(transform.get("sensor_id", "")).strip()
        if sensor_id:
            transform_by_sensor_id[sensor_id] = transform

    stage_entries: list[dict[str, Any]] = []
    for stage in list(consumer_input_manifest.get("processing_stages", []) or []):
        if not isinstance(stage, dict):
            continue
        stage_id = str(stage.get("stage_id", "")).strip()
        if not stage_id:
            continue
        required_topics = list(stage.get("required_topics", []) or [])
        available_topics = list(stage.get("available_topics", []) or [])
        missing_required_topics = list(stage.get("missing_required_topics", []) or [])
        topic_entries = [
            dict(topic_by_name[topic_name])
            for topic_name in required_topics
            if topic_name in topic_by_name
        ]
        relevant_sensor_ids = sorted(
            {
                str(sensor_id).strip()
                for sensor_id in (
                    list(stage.get("required_sensor_ids", []) or [])
                    + list(stage.get("available_sensor_ids", []) or [])
                )
                if str(sensor_id).strip()
            }
        )
        sensor_inputs = [
            dict(sensor_input_by_id[sensor_id])
            for sensor_id in relevant_sensor_ids
            if sensor_id in sensor_input_by_id
        ]
        static_transforms = [
            dict(transform_by_sensor_id[sensor_id])
            for sensor_id in relevant_sensor_ids
            if sensor_id in transform_by_sensor_id
        ]
        stage_entries.append(
            {
                "stage_id": stage_id,
                "description": _clean_optional_text(stage.get("description")),
                "ready": bool(stage.get("ready")),
                "required_output_roles": list(stage.get("required_output_roles", []) or []),
                "required_topic_count": int(stage.get("required_topic_count", 0) or 0),
                "available_topic_count": int(stage.get("available_topic_count", 0) or 0),
                "missing_required_topic_count": int(
                    stage.get("missing_required_topic_count", 0) or 0
                ),
                "required_topics": required_topics,
                "available_topics": available_topics,
                "missing_required_topics": missing_required_topics,
                "required_sensor_ids": list(stage.get("required_sensor_ids", []) or []),
                "available_sensor_ids": list(stage.get("available_sensor_ids", []) or []),
                "topic_entries": topic_entries,
                "sensor_inputs": sensor_inputs,
                "static_transforms": static_transforms,
            }
        )

    ready_stage_count = sum(1 for stage in stage_entries if bool(stage.get("ready")))
    return {
        "schema_version": AUTOWARE_PROCESSING_STAGE_BUNDLE_INDEX_SCHEMA_VERSION_V0,
        "consumer_profile_id": _clean_optional_text(
            consumer_input_manifest.get("consumer_profile_id")
        ),
        "consumer_profile_description": _clean_optional_text(
            consumer_input_manifest.get("consumer_profile_description")
        ),
        "consumer_ready": bool(consumer_input_manifest.get("consumer_ready")),
        "processing_stage_bundle_count": len(stage_entries),
        "ready_processing_stage_bundle_count": ready_stage_count,
        "degraded_processing_stage_bundle_count": len(stage_entries) - ready_stage_count,
        "processing_stages": stage_entries,
    }
