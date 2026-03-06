from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET


def _safe_name(raw: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in raw.strip())
    return cleaned or "generated_survey"


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
        try:
            return float(raw[0]), float(raw[1]), float(raw[2])
        except (TypeError, ValueError):
            return None
    if isinstance(raw, dict):
        try:
            return float(raw.get("x", 0.0)), float(raw.get("y", 0.0)), float(raw.get("z", 0.0))
        except (TypeError, ValueError):
            return None
    return None


def _extract_trajectory_points(scenario: dict[str, Any]) -> list[tuple[float, float, float]]:
    ego_obj = _resolve_ego_object(scenario)
    points: list[tuple[float, float, float]] = []
    if ego_obj is not None:
        pose = _extract_pose_xyz(ego_obj.get("pose"))
        if pose is not None:
            points.append(pose)
        waypoints = ego_obj.get("waypoints")
        if isinstance(waypoints, list):
            for item in waypoints:
                pose = _extract_pose_xyz(item)
                if pose is not None:
                    points.append(pose)

    scenario_waypoints = scenario.get("waypoints")
    if isinstance(scenario_waypoints, list):
        for item in scenario_waypoints:
            pose = _extract_pose_xyz(item)
            if pose is not None:
                points.append(pose)

    if not points:
        points.append((0.0, 0.0, 0.0))
    return points


def _option_float(options: dict[str, Any], key: str, default: float) -> float:
    try:
        return float(options.get(key, default))
    except (TypeError, ValueError):
        return default


def _option_str(options: dict[str, Any], key: str, default: str) -> str:
    value = options.get(key, default)
    return str(value) if value is not None else default


def generate_survey_from_scenario(
    *,
    scenario_path: Path,
    output_dir: Path,
    options: dict[str, Any],
) -> Path:
    if not scenario_path.exists():
        raise ValueError(f"scenario path does not exist: {scenario_path}")
    try:
        scenario = json.loads(scenario_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"scenario json parse error: {exc}") from exc
    if not isinstance(scenario, dict):
        raise ValueError("scenario root must be a JSON object.")

    survey_name = _safe_name(_option_str(options, "survey_generated_name", scenario.get("name", "generated_survey")))
    scene_ref = _option_str(
        options,
        "survey_scene_ref",
        "data/scenes/demo/plane_scene.xml#plane_scene",
    )
    platform_ref = _option_str(options, "survey_platform_ref", "data/platforms.xml#tripod")
    scanner_ref = _option_str(options, "survey_scanner_ref", "data/scanners_tls.xml#panoscanner")
    pulse_freq_hz = _option_float(options, "survey_pulse_freq_hz", 100000.0)
    scan_freq_hz = _option_float(options, "survey_scan_freq_hz", 100.0)
    head_rotate_per_sec_deg = _option_float(options, "survey_head_rotate_per_sec_deg", 5.0)
    head_rotate_start_deg = _option_float(options, "survey_head_rotate_start_deg", -180.0)
    head_rotate_stop_deg = _option_float(options, "survey_head_rotate_stop_deg", 180.0)

    points = _extract_trajectory_points(scenario)

    document = ET.Element("document")
    scanner_settings = ET.SubElement(
        document,
        "scannerSettings",
        {
            "id": "scaset",
            "active": "true",
            "pulseFreq_hz": str(int(round(pulse_freq_hz))),
            "scanFreq_hz": str(int(round(scan_freq_hz))),
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
    for x, y, z in points:
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
        ET.SubElement(
            leg,
            "scannerSettings",
            {
                "template": "scaset",
                "headRotatePerSec_deg": f"{head_rotate_per_sec_deg:.6f}",
                "headRotateStart_deg": f"{head_rotate_start_deg:.6f}",
                "headRotateStop_deg": f"{head_rotate_stop_deg:.6f}",
            },
        )

    tree = ET.ElementTree(document)
    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass

    output_dir.mkdir(parents=True, exist_ok=True)
    survey_path = output_dir / f"{survey_name}.xml"
    tree.write(survey_path, encoding="utf-8", xml_declaration=True)
    return survey_path
