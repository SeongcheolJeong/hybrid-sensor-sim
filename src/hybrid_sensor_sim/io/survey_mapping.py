from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET


def _safe_name(raw: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in raw.strip())
    return cleaned or "generated_survey"


def _coerce_float(raw: Any) -> float | None:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _scalar_xml_attr_value(raw: Any) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return "true" if raw else "false"
    if isinstance(raw, (int, float)):
        return str(raw)
    if isinstance(raw, str):
        return raw
    return None


_SCANNER_ATTR_KEY_ALIASES = {
    "num_rays": "numRays",
    "channels": "numRays",
    "scan_pattern": "scanPattern",
    "max_range_m": "maxRange_m",
    "min_range_m": "minRange_m",
    "horizontal_fov_deg": "horizontalFov_deg",
    "vertical_fov_deg": "verticalFov_deg",
    "beam_divergence_mrad": "beamDivergence_mrad",
    "pulse_freq_hz": "pulseFreq_hz",
    "scan_freq_hz": "scanFreq_hz",
    "head_rotate_per_sec_deg": "headRotatePerSec_deg",
    "head_rotate_start_deg": "headRotateStart_deg",
    "head_rotate_stop_deg": "headRotateStop_deg",
}


def _normalize_scanner_attr_key(key: str) -> str:
    return _SCANNER_ATTR_KEY_ALIASES.get(key, key)


def _collect_scalar_attrs(
    raw: dict[str, Any],
    *,
    skip_keys: set[str],
    normalize_keys: bool = False,
) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for key, value in raw.items():
        if key in skip_keys:
            continue
        normalized_key = _normalize_scanner_attr_key(str(key)) if normalize_keys else str(key)
        stringified = _scalar_xml_attr_value(value)
        if stringified is not None:
            attrs[normalized_key] = stringified
    return attrs


def _lookup(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _resolve_ego_object(scenario: dict[str, Any]) -> dict[str, Any] | None:
    objects = scenario.get("objects")
    if not isinstance(objects, list):
        return None
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        if str(obj.get("id", "")).lower() == "ego":
            return obj
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        if str(obj.get("type", "")).lower() in {"vehicle", "car", "ego"}:
            return obj
    return objects[0] if objects and isinstance(objects[0], dict) else None


def _extract_pose_xyz(raw: Any) -> tuple[float, float, float] | None:
    if isinstance(raw, (list, tuple)) and len(raw) >= 3:
        x = _coerce_float(raw[0])
        y = _coerce_float(raw[1])
        z = _coerce_float(raw[2])
        if x is not None and y is not None and z is not None:
            return x, y, z
        return None
    if isinstance(raw, dict):
        x = _coerce_float(raw.get("x"))
        y = _coerce_float(raw.get("y"))
        z = _coerce_float(raw.get("z"))
        if x is not None and y is not None and z is not None:
            return x, y, z
    return None


def _extract_pose_from_entry(raw: Any) -> tuple[float, float, float] | None:
    if isinstance(raw, dict):
        for key in ("pose", "position", "platform", "platformSettings", "point", "xyz"):
            if key in raw:
                pose = _extract_pose_xyz(raw.get(key))
                if pose is not None:
                    return pose
    return _extract_pose_xyz(raw)


def _extract_points(raw: Any) -> list[tuple[float, float, float]]:
    points: list[tuple[float, float, float]] = []
    if isinstance(raw, list):
        for item in raw:
            pose = _extract_pose_from_entry(item)
            if pose is not None:
                points.append(pose)
        return points

    if isinstance(raw, dict):
        for key in ("points", "waypoints", "poses", "trajectory"):
            if key in raw:
                points.extend(_extract_points(raw.get(key)))
        if points:
            return points

    pose = _extract_pose_from_entry(raw)
    if pose is not None:
        points.append(pose)
    return points


def _dedupe_consecutive_points(
    points: list[tuple[float, float, float]],
) -> list[tuple[float, float, float]]:
    deduped: list[tuple[float, float, float]] = []
    for point in points:
        if deduped and deduped[-1] == point:
            continue
        deduped.append(point)
    return deduped


def _extract_trajectory_points(
    scenario: dict[str, Any],
) -> tuple[list[tuple[float, float, float]], str]:
    ego_trajectory = _extract_points(scenario.get("ego_trajectory"))
    if ego_trajectory:
        return _dedupe_consecutive_points(ego_trajectory), "ego_trajectory"

    ego_obj = _resolve_ego_object(scenario)
    points: list[tuple[float, float, float]] = []
    object_points_count = 0
    if ego_obj is not None:
        pose = _extract_pose_from_entry(ego_obj)
        if pose is not None:
            points.append(pose)
            object_points_count += 1
        ego_waypoints = _extract_points(ego_obj.get("waypoints"))
        points.extend(ego_waypoints)
        object_points_count += len(ego_waypoints)

    scenario_waypoints = _extract_points(scenario.get("waypoints"))
    points.extend(scenario_waypoints)
    points = _dedupe_consecutive_points(points)

    if not points:
        return [(0.0, 0.0, 0.0)], "default_origin"
    if object_points_count > 0 and scenario_waypoints:
        return points, "objects_and_waypoints"
    if object_points_count > 0:
        return points, "objects"
    return points, "waypoints"


def _resolve_str_option(
    options: dict[str, Any],
    key: str,
    candidates: list[Any],
    default: str,
) -> str:
    if key in options and options.get(key) is not None:
        return str(options.get(key))
    for candidate in candidates:
        if candidate is not None:
            return str(candidate)
    return default


def _resolve_float_option(
    options: dict[str, Any],
    key: str,
    candidates: list[Any],
    default: float,
) -> float:
    if key in options:
        option_value = _coerce_float(options.get(key))
        if option_value is not None:
            return option_value
    for candidate in candidates:
        parsed = _coerce_float(candidate)
        if parsed is not None:
            return parsed
    return default


def _resolve_helios_config(scenario: dict[str, Any]) -> dict[str, Any]:
    raw = scenario.get("helios")
    return raw if isinstance(raw, dict) else {}


def _resolve_lidar_sensor_config(scenario: dict[str, Any]) -> dict[str, Any]:
    sensors = scenario.get("sensors")
    if not isinstance(sensors, dict):
        return {}
    lidar = sensors.get("lidar")
    return lidar if isinstance(lidar, dict) else {}


def _resolve_explicit_legs(scenario: dict[str, Any]) -> list[dict[str, Any]]:
    helios_cfg = _resolve_helios_config(scenario)
    legs_raw = helios_cfg.get("legs")
    if not isinstance(legs_raw, list):
        legs_raw = scenario.get("helios_legs")
    if not isinstance(legs_raw, list):
        return []
    return [item for item in legs_raw if isinstance(item, dict)]


def _extract_leg_scanner_overrides(leg: dict[str, Any]) -> dict[str, str]:
    scanner_raw: dict[str, Any] | None = None
    for key in ("scannerSettings", "scanner_settings", "scanner"):
        candidate = leg.get(key)
        if isinstance(candidate, dict):
            scanner_raw = candidate
            break
    if scanner_raw is None:
        scanner_raw = {}

    overrides: dict[str, str] = {}
    template = _lookup(scanner_raw, "template", "template_id")
    if template is not None:
        overrides["template"] = str(template)

    head_rotate_per_sec_deg = _coerce_float(
        _lookup(scanner_raw, "headRotatePerSec_deg", "head_rotate_per_sec_deg"),
    )
    if head_rotate_per_sec_deg is not None:
        overrides["headRotatePerSec_deg"] = f"{head_rotate_per_sec_deg:.6f}"

    head_rotate_start_deg = _coerce_float(
        _lookup(scanner_raw, "headRotateStart_deg", "head_rotate_start_deg"),
    )
    if head_rotate_start_deg is not None:
        overrides["headRotateStart_deg"] = f"{head_rotate_start_deg:.6f}"

    head_rotate_stop_deg = _coerce_float(
        _lookup(scanner_raw, "headRotateStop_deg", "head_rotate_stop_deg"),
    )
    if head_rotate_stop_deg is not None:
        overrides["headRotateStop_deg"] = f"{head_rotate_stop_deg:.6f}"

    overrides.update(
        _collect_scalar_attrs(
            scanner_raw,
            skip_keys={
                "template",
                "template_id",
                "headRotatePerSec_deg",
                "head_rotate_per_sec_deg",
                "headRotateStart_deg",
                "head_rotate_start_deg",
                "headRotateStop_deg",
                "head_rotate_stop_deg",
            },
            normalize_keys=True,
        )
    )
    return overrides


def _extract_leg_pose(leg: dict[str, Any]) -> tuple[float, float, float] | None:
    for key in ("platformSettings", "platform", "pose", "position"):
        if key in leg:
            pose = _extract_pose_xyz(leg.get(key))
            if pose is not None:
                return pose
    return _extract_pose_xyz(leg)


def _resolve_global_scanner_extra_attrs(
    *,
    options: dict[str, Any],
    lidar_cfg: dict[str, Any],
    helios_scanner_cfg: dict[str, Any],
) -> dict[str, str]:
    skip_common = {
        "id",
        "active",
        "pulse_freq_hz",
        "pulseFreq_hz",
        "scan_freq_hz",
        "scanFreq_hz",
        "head_rotate_per_sec_deg",
        "headRotatePerSec_deg",
        "head_rotate_start_deg",
        "headRotateStart_deg",
        "head_rotate_stop_deg",
        "headRotateStop_deg",
    }
    attrs: dict[str, str] = {}

    lidar_scanner_settings = lidar_cfg.get("scanner_settings")
    if isinstance(lidar_scanner_settings, dict):
        attrs.update(
            _collect_scalar_attrs(
                lidar_scanner_settings,
                skip_keys=skip_common,
                normalize_keys=True,
            )
        )
    attrs.update(
        _collect_scalar_attrs(
            helios_scanner_cfg,
            skip_keys=skip_common,
            normalize_keys=True,
        )
    )

    option_attrs = options.get("survey_scanner_settings_extra_attrs")
    if isinstance(option_attrs, dict):
        attrs.update(
            _collect_scalar_attrs(
                option_attrs,
                skip_keys={"id", "active"},
                normalize_keys=True,
            )
        )
    return attrs


def generate_survey_from_scenario(
    *,
    scenario_path: Path,
    output_dir: Path,
    options: dict[str, Any],
    metadata_out: dict[str, Any] | None = None,
) -> Path:
    if not scenario_path.exists():
        raise ValueError(f"scenario path does not exist: {scenario_path}")
    try:
        scenario = json.loads(scenario_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"scenario json parse error: {exc}") from exc
    if not isinstance(scenario, dict):
        raise ValueError("scenario root must be a JSON object.")

    helios_cfg = _resolve_helios_config(scenario)
    lidar_cfg = _resolve_lidar_sensor_config(scenario)
    helios_scanner_cfg = helios_cfg.get("scanner_settings")
    if not isinstance(helios_scanner_cfg, dict):
        helios_scanner_cfg = {}

    survey_name = _safe_name(
        _resolve_str_option(
            options,
            "survey_generated_name",
            [scenario.get("name")],
            "generated_survey",
        ),
    )
    scene_ref = _resolve_str_option(
        options,
        "survey_scene_ref",
        [_lookup(helios_cfg, "scene_ref", "scene"), scenario.get("scene_ref")],
        "data/scenes/demo/plane_scene.xml#plane_scene",
    )
    platform_ref = _resolve_str_option(
        options,
        "survey_platform_ref",
        [_lookup(helios_cfg, "platform_ref", "platform"), scenario.get("platform_ref")],
        "data/platforms.xml#tripod",
    )
    scanner_ref = _resolve_str_option(
        options,
        "survey_scanner_ref",
        [
            _lookup(helios_cfg, "scanner_ref", "scanner"),
            _lookup(lidar_cfg, "scanner_ref", "scanner"),
            scenario.get("scanner_ref"),
        ],
        "data/scanners_tls.xml#panoscanner",
    )
    scanner_settings_id = _resolve_str_option(
        options,
        "survey_scanner_settings_id",
        [_lookup(helios_cfg, "scanner_settings_id"), scenario.get("scanner_settings_id")],
        "scaset",
    )
    pulse_freq_hz = _resolve_float_option(
        options,
        "survey_pulse_freq_hz",
        [
            _lookup(helios_scanner_cfg, "pulse_freq_hz", "pulseFreq_hz"),
            _lookup(helios_cfg, "pulse_freq_hz", "pulseFreq_hz"),
            _lookup(lidar_cfg, "pulse_freq_hz", "pulseFreq_hz"),
        ],
        100000.0,
    )
    scan_freq_hz = _resolve_float_option(
        options,
        "survey_scan_freq_hz",
        [
            _lookup(helios_scanner_cfg, "scan_freq_hz", "scanFreq_hz"),
            _lookup(helios_cfg, "scan_freq_hz", "scanFreq_hz"),
            _lookup(lidar_cfg, "scan_freq_hz", "scanFreq_hz"),
        ],
        100.0,
    )
    head_rotate_per_sec_deg = _resolve_float_option(
        options,
        "survey_head_rotate_per_sec_deg",
        [
            _lookup(helios_scanner_cfg, "head_rotate_per_sec_deg", "headRotatePerSec_deg"),
            _lookup(helios_cfg, "head_rotate_per_sec_deg", "headRotatePerSec_deg"),
            _lookup(lidar_cfg, "head_rotate_per_sec_deg", "headRotatePerSec_deg"),
        ],
        5.0,
    )
    head_rotate_start_deg = _resolve_float_option(
        options,
        "survey_head_rotate_start_deg",
        [
            _lookup(helios_scanner_cfg, "head_rotate_start_deg", "headRotateStart_deg"),
            _lookup(helios_cfg, "head_rotate_start_deg", "headRotateStart_deg"),
            _lookup(lidar_cfg, "head_rotate_start_deg", "headRotateStart_deg"),
        ],
        -180.0,
    )
    head_rotate_stop_deg = _resolve_float_option(
        options,
        "survey_head_rotate_stop_deg",
        [
            _lookup(helios_scanner_cfg, "head_rotate_stop_deg", "headRotateStop_deg"),
            _lookup(helios_cfg, "head_rotate_stop_deg", "headRotateStop_deg"),
            _lookup(lidar_cfg, "head_rotate_stop_deg", "headRotateStop_deg"),
        ],
        180.0,
    )
    force_global_leg_scanner = bool(options.get("survey_force_global_leg_scanner", False))
    scanner_settings_extra_attrs = _resolve_global_scanner_extra_attrs(
        options=options,
        lidar_cfg=lidar_cfg,
        helios_scanner_cfg=helios_scanner_cfg,
    )

    explicit_legs = _resolve_explicit_legs(scenario)
    explicit_legs_defined_count = len(explicit_legs)
    explicit_legs_used_count = 0
    trajectory_source = "unused_explicit_legs"
    leg_source = "explicit_legs"
    legs: list[tuple[tuple[float, float, float], dict[str, str]]] = []
    if explicit_legs:
        for leg_cfg in explicit_legs:
            pose = _extract_leg_pose(leg_cfg)
            if pose is None:
                continue
            legs.append((pose, _extract_leg_scanner_overrides(leg_cfg)))
            explicit_legs_used_count += 1
    else:
        leg_source = "trajectory"
        trajectory_points, trajectory_source = _extract_trajectory_points(scenario)
        for point in trajectory_points:
            legs.append((point, {}))
    if explicit_legs and not legs:
        leg_source = "trajectory_fallback_from_explicit_legs"
        trajectory_points, trajectory_source = _extract_trajectory_points(scenario)
        for point in trajectory_points:
            legs.append((point, {}))
    if not legs:
        trajectory_source = "default_origin"
        legs.append(((0.0, 0.0, 0.0), {}))

    document = ET.Element("document")
    scanner_settings = ET.SubElement(
        document,
        "scannerSettings",
        {
            "id": scanner_settings_id,
            "active": "true",
            "pulseFreq_hz": str(int(round(pulse_freq_hz))),
            "scanFreq_hz": str(int(round(scan_freq_hz))),
            **scanner_settings_extra_attrs,
        },
    )
    scanner_settings.text = None

    survey = ET.SubElement(
        document,
        "survey",
        {
            "name": survey_name,
            "scene": scene_ref,
            "platform": platform_ref,
            "scanner": scanner_ref,
        },
    )
    default_leg_scanner_attrs = {
        "template": scanner_settings_id,
        "headRotatePerSec_deg": f"{head_rotate_per_sec_deg:.6f}",
        "headRotateStart_deg": f"{head_rotate_start_deg:.6f}",
        "headRotateStop_deg": f"{head_rotate_stop_deg:.6f}",
    }
    for (x, y, z), leg_scanner_overrides in legs:
        leg = ET.SubElement(survey, "leg")
        ET.SubElement(
            leg,
            "platformSettings",
            {
                "x": f"{x:.6f}",
                "y": f"{y:.6f}",
                "z": f"{z:.6f}",
            },
        )
        leg_scanner_attrs = dict(default_leg_scanner_attrs)
        if not force_global_leg_scanner:
            leg_scanner_attrs.update(leg_scanner_overrides)
        if not leg_scanner_attrs.get("template"):
            leg_scanner_attrs["template"] = scanner_settings_id
        ET.SubElement(
            leg,
            "scannerSettings",
            leg_scanner_attrs,
        )

    tree = ET.ElementTree(document)
    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass

    output_dir.mkdir(parents=True, exist_ok=True)
    survey_path = output_dir / f"{survey_name}.xml"
    tree.write(survey_path, encoding="utf-8", xml_declaration=True)
    if metadata_out is not None:
        relevant_option_keys = [
            "survey_generated_name",
            "survey_scene_ref",
            "survey_platform_ref",
            "survey_scanner_ref",
            "survey_scanner_settings_id",
            "survey_pulse_freq_hz",
            "survey_scan_freq_hz",
            "survey_head_rotate_per_sec_deg",
            "survey_head_rotate_start_deg",
            "survey_head_rotate_stop_deg",
            "survey_force_global_leg_scanner",
        ]
        option_override_keys = sorted(
            [
                key
                for key in relevant_option_keys
                if key in options and options.get(key) is not None
            ]
        )
        metadata_out.update(
            {
                "scenario_path": str(scenario_path.resolve()),
                "survey_path": str(survey_path.resolve()),
                "survey_name": survey_name,
                "scene_ref": scene_ref,
                "platform_ref": platform_ref,
                "scanner_ref": scanner_ref,
                "scanner_settings_id": scanner_settings_id,
                "pulse_freq_hz": pulse_freq_hz,
                "scan_freq_hz": scan_freq_hz,
                "head_rotate_per_sec_deg": head_rotate_per_sec_deg,
                "head_rotate_start_deg": head_rotate_start_deg,
                "head_rotate_stop_deg": head_rotate_stop_deg,
                "force_global_leg_scanner": force_global_leg_scanner,
                "leg_count": len(legs),
                "leg_source": leg_source,
                "trajectory_source": trajectory_source,
                "explicit_legs_defined_count": explicit_legs_defined_count,
                "explicit_legs_used_count": explicit_legs_used_count,
                "leg_scanner_override_count": sum(1 for _, override in legs if bool(override)),
                "option_override_keys": option_override_keys,
            }
        )
    return survey_path
