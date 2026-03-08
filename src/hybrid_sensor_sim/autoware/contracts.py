from __future__ import annotations

from typing import Any

from hybrid_sensor_sim.autoware.profiles import resolve_autoware_consumer_profile
from hybrid_sensor_sim.autoware.topics import (
    default_autoware_encoding_for_output_role,
    default_autoware_message_type_for_output_role,
    default_autoware_topic_for_output_role,
)


AUTOWARE_SENSOR_CONTRACT_SCHEMA_VERSION_V0 = "autoware_sensor_contract_v0"


def _mounts_by_sensor_id(sensor_mounts: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for mount in sensor_mounts or []:
        if not isinstance(mount, dict):
            continue
        sensor_id = str(mount.get("sensor_id", "")).strip()
        if sensor_id:
            result[sensor_id] = dict(mount)
    return result


def _outputs_by_sensor_id(summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for sensor in summary.get("sensors", []) or []:
        if not isinstance(sensor, dict):
            continue
        sensor_id = str(sensor.get("sensor_id", "")).strip()
        if sensor_id:
            result[sensor_id] = dict(sensor)
    return result


def _expected_outputs_by_sensor_id(spec: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for sensor in spec.get("expected_outputs_by_sensor", []) or []:
        if not isinstance(sensor, dict):
            continue
        sensor_id = str(sensor.get("sensor_id", "")).strip()
        if sensor_id:
            result[sensor_id] = dict(sensor)
    return result


def _required_output_roles_for_modality(modality: str) -> set[str]:
    return {
        "camera": {"camera_visible"},
        "lidar": {"lidar_point_cloud"},
        "radar": {"radar_detections"},
    }.get(str(modality).strip(), set())


def _required_output_roles(
    *,
    modality: str,
    summary_sensor: dict[str, Any] | None,
    spec_sensor: dict[str, Any] | None,
    consumer_profile: dict[str, Any] | None,
) -> set[str]:
    if isinstance(consumer_profile, dict):
        profile_roles = (
            dict(consumer_profile.get("required_output_roles_by_modality", {})).get(
                str(modality).strip(),
                [],
            )
            or []
        )
        normalized_profile_roles = {
            str(role).strip() for role in profile_roles if str(role).strip()
        }
        if normalized_profile_roles:
            return normalized_profile_roles
    explicit_roles: set[str] = set()
    for container in (spec_sensor, summary_sensor):
        for output in (container or {}).get("outputs", []) or []:
            if not isinstance(output, dict):
                continue
            output_role = str(output.get("output_role", "")).strip()
            if output_role:
                explicit_roles.add(output_role)
    return explicit_roles or _required_output_roles_for_modality(modality)


def _derive_sensor_modality(
    *,
    mount: dict[str, Any] | None,
    summary_sensor: dict[str, Any] | None,
    spec_sensor: dict[str, Any] | None,
) -> str:
    for candidate in (
        (mount or {}).get("sensor_type"),
        (summary_sensor or {}).get("modality"),
    ):
        text = str(candidate or "").strip()
        if text:
            return text
    outputs = []
    if isinstance(summary_sensor, dict):
        outputs.extend(summary_sensor.get("outputs", []) or [])
    if isinstance(spec_sensor, dict):
        outputs.extend(spec_sensor.get("outputs", []) or [])
    for output in outputs:
        if not isinstance(output, dict):
            continue
        text = str(output.get("modality", "")).strip()
        if text:
            return text
    return ""


def _merged_output_entries(
    *,
    summary_sensor: dict[str, Any] | None,
    spec_sensor: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for container_name, container in (("spec", spec_sensor), ("summary", summary_sensor)):
        for output in (container or {}).get("outputs", []) or []:
            if not isinstance(output, dict):
                continue
            output_role = str(output.get("output_role", "")).strip()
            if not output_role:
                continue
            entry = merged.setdefault(output_role, {})
            if container_name == "spec":
                entry.update({k: v for k, v in output.items() if k not in {"resolved_path", "exists"}})
            else:
                entry.update(output)
    return merged


def build_autoware_sensor_contracts(
    backend_sensor_output_summary: dict[str, Any],
    backend_output_spec: dict[str, Any],
    sensor_mounts: list[dict[str, Any]] | None = None,
    availability_mode: str = "runtime",
    consumer_profile_id: str = "",
) -> dict[str, Any]:
    mounts_by_sensor = _mounts_by_sensor_id(sensor_mounts)
    summary_by_sensor = _outputs_by_sensor_id(backend_sensor_output_summary)
    spec_by_sensor = _expected_outputs_by_sensor_id(backend_output_spec)
    consumer_profile = resolve_autoware_consumer_profile(consumer_profile_id)
    backend = str(
        backend_sensor_output_summary.get("backend")
        or backend_output_spec.get("backend")
        or ""
    ).strip()
    sensor_ids = sorted(set(mounts_by_sensor) | set(summary_by_sensor) | set(spec_by_sensor))
    contracts: list[dict[str, Any]] = []
    sensors: list[dict[str, Any]] = []
    warnings: list[str] = []
    available_topics: list[str] = []
    missing_required_sensor_count = 0
    available_sensor_count = 0
    normalized_availability_mode = str(availability_mode).strip().lower() or "runtime"
    if normalized_availability_mode not in {"runtime", "planned", "sidecar", "mixed"}:
        raise ValueError(
            f"availability_mode must be 'runtime', 'planned', 'sidecar', or 'mixed', got: {availability_mode}"
        )

    for sensor_id in sensor_ids:
        mount = mounts_by_sensor.get(sensor_id)
        summary_sensor = summary_by_sensor.get(sensor_id)
        spec_sensor = spec_by_sensor.get(sensor_id)
        modality = _derive_sensor_modality(mount=mount, summary_sensor=summary_sensor, spec_sensor=spec_sensor)
        enabled = bool(mount.get("enabled", True)) if isinstance(mount, dict) else True
        merged_outputs = _merged_output_entries(summary_sensor=summary_sensor, spec_sensor=spec_sensor)
        required_roles = (
            _required_output_roles(
                modality=modality,
                summary_sensor=summary_sensor,
                spec_sensor=spec_sensor,
                consumer_profile=consumer_profile,
            )
            if enabled
            else set()
        )
        missing_required_roles: list[str] = []
        available_output_count = 0
        for output_role, output in sorted(merged_outputs.items()):
            try:
                topic = default_autoware_topic_for_output_role(output_role, sensor_id, backend)
                message_type = default_autoware_message_type_for_output_role(output_role)
                encoding = default_autoware_encoding_for_output_role(output_role)
            except ValueError as exc:
                warnings.append(str(exc))
                continue
            exists = bool(output.get("exists", False))
            resolved_path = str(output.get("resolved_path", "")).strip() or None
            output_origin = str(output.get("output_origin", "")).strip() or None
            available = bool(
                (exists and resolved_path)
                or (
                    normalized_availability_mode == "planned"
                    and resolved_path
                    and output_role in required_roles
                )
            )
            if available:
                available_output_count += 1
                available_topics.append(topic)
            required = output_role in required_roles
            if required and not available:
                missing_required_roles.append(output_role)
            contracts.append(
                {
                    "backend": backend or None,
                    "sensor_id": sensor_id,
                    "modality": modality or None,
                    "output_role": output_role,
                    "artifact_type": str(output.get("artifact_type", "")).strip() or None,
                    "autoware_topic": topic,
                    "frame_id": sensor_id,
                    "message_type": message_type,
                    "encoding": encoding,
                    "data_format": str(output.get("data_format", "")).strip() or None,
                    "source_artifact_key": str(output.get("artifact_key", "")).strip() or None,
                    "source_resolved_path": resolved_path,
                    "output_origin": output_origin,
                    "required": required,
                    "available": available,
                    "availability_mode": normalized_availability_mode,
                    "consumer_profile_id": (
                        str(consumer_profile.get("profile_id", "")).strip() or None
                        if isinstance(consumer_profile, dict)
                        else None
                    ),
                }
            )
        for output_role in sorted(required_roles - set(merged_outputs)):
            topic = default_autoware_topic_for_output_role(output_role, sensor_id, backend)
            message_type = default_autoware_message_type_for_output_role(output_role)
            encoding = default_autoware_encoding_for_output_role(output_role)
            missing_required_roles.append(output_role)
            contracts.append(
                {
                    "backend": backend or None,
                    "sensor_id": sensor_id,
                    "modality": modality or None,
                    "output_role": output_role,
                    "artifact_type": None,
                    "autoware_topic": topic,
                    "frame_id": sensor_id,
                    "message_type": message_type,
                    "encoding": encoding,
                    "data_format": None,
                    "source_artifact_key": None,
                    "source_resolved_path": None,
                    "output_origin": None,
                    "required": True,
                    "available": False,
                    "availability_mode": normalized_availability_mode,
                    "consumer_profile_id": (
                        str(consumer_profile.get("profile_id", "")).strip() or None
                        if isinstance(consumer_profile, dict)
                        else None
                    ),
                }
            )
        if enabled and missing_required_roles:
            missing_required_sensor_count += 1
        if available_output_count > 0:
            available_sensor_count += 1
        sensors.append(
            {
                "sensor_id": sensor_id,
                "modality": modality or None,
                "enabled": enabled,
                "available_output_count": available_output_count,
                "missing_required_roles": sorted(set(missing_required_roles)),
                "required_output_roles": sorted(required_roles),
                "availability_mode": normalized_availability_mode,
                "consumer_profile_id": (
                    str(consumer_profile.get("profile_id", "")).strip() or None
                    if isinstance(consumer_profile, dict)
                    else None
                ),
            }
        )

    contracts.sort(key=lambda item: (item["sensor_id"], item["output_role"]))
    sensors.sort(key=lambda item: item["sensor_id"])
    return {
        "schema_version": AUTOWARE_SENSOR_CONTRACT_SCHEMA_VERSION_V0,
        "backend": backend or None,
        "availability_mode": normalized_availability_mode,
        "consumer_profile_id": (
            str(consumer_profile.get("profile_id", "")).strip() or None
            if isinstance(consumer_profile, dict)
            else None
        ),
        "consumer_profile_description": (
            str(consumer_profile.get("description", "")).strip() or None
            if isinstance(consumer_profile, dict)
            else None
        ),
        "sensor_count": len(sensors),
        "available_sensor_count": available_sensor_count,
        "missing_required_sensor_count": missing_required_sensor_count,
        "contract_count": len(contracts),
        "available_topics": sorted(set(available_topics)),
        "warnings": warnings,
        "sensors": sensors,
        "contracts": contracts,
    }
