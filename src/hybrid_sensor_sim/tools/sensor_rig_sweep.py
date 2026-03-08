from __future__ import annotations

import argparse
import copy
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from hybrid_sensor_sim.backends.native_physics import NativePhysicsBackend
from hybrid_sensor_sim.io.pointcloud_xyz import write_xyz_points
from hybrid_sensor_sim.types import SensorSimRequest


RIG_SWEEP_SCHEMA_VERSION_V1 = "sensor_rig_sweep_v1"
SENSOR_RIG_SWEEP_REPORT_SCHEMA_VERSION_V1 = "sensor_rig_sweep_report_v1"


@dataclass(frozen=True)
class RigSweepTargetPoint:
    actor_id: str
    xyz: tuple[float, float, float]
    semantic_class_id: int | None = None
    semantic_class_name: str | None = None


@dataclass(frozen=True)
class RigSweepCandidate:
    rig_id: str
    camera_override: dict[str, Any]
    lidar_override: dict[str, Any]
    radar_override: dict[str, Any]
    coverage_override: dict[str, Any]
    config_override: dict[str, Any]
    targets: list[RigSweepTargetPoint] | None = None


@dataclass(frozen=True)
class RigSweepDefinition:
    base_config_path: Path
    rig_candidates_path: Path
    targets: list[RigSweepTargetPoint]
    candidates: list[RigSweepCandidate]


@dataclass(frozen=True)
class RigSweepEvaluation:
    rig_id: str
    heuristic_score: float
    covered_target_count: int
    blindspot_target_count: int
    overlap_target_count: int
    available_sensor_count: int
    active_sensor_count: int
    camera_pixels_on_target: int
    lidar_points_on_target: int
    radar_detections_on_target: int
    sort_key: tuple[object, ...]
    candidate_output_dir: Path
    effective_config_path: Path
    point_cloud_path: Path
    coverage_summary_path: Path | None
    preview_artifacts: dict[str, str]
    coverage_summary: dict[str, Any]


def _load_json_object(path: Path, label: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _as_dict(value: Any, *, field: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a JSON object")
    return dict(value)


def _as_list(value: Any, *, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{field} must be a list")
    return list(value)


def _deep_merge(base: dict[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(dict(merged[key]), value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _resolve_path_relative_to(path_value: str | Path, base_dir: Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path.resolve()
    return (base_dir / path).resolve()


def _coerce_float(raw: Any, *, field: str) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc


def _parse_target_point(raw: Any, *, field: str, index: int) -> RigSweepTargetPoint:
    if not isinstance(raw, dict):
        raise ValueError(f"{field}[{index}] must be an object")
    actor_id = str(raw.get("actor_id", "")).strip()
    if not actor_id:
        raise ValueError(f"{field}[{index}] missing actor_id")
    xyz_raw = raw.get("xyz")
    if not isinstance(xyz_raw, list) or len(xyz_raw) != 3:
        raise ValueError(f"{field}[{index}].xyz must be a 3-element list")
    xyz = (
        _coerce_float(xyz_raw[0], field=f"{field}[{index}].xyz[0]"),
        _coerce_float(xyz_raw[1], field=f"{field}[{index}].xyz[1]"),
        _coerce_float(xyz_raw[2], field=f"{field}[{index}].xyz[2]"),
    )
    semantic_class_id = raw.get("semantic_class_id")
    if semantic_class_id is not None:
        try:
            semantic_class_id = int(semantic_class_id)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field}[{index}].semantic_class_id must be an integer") from exc
    semantic_class_name = raw.get("semantic_class_name")
    if semantic_class_name is not None:
        semantic_class_name = str(semantic_class_name).strip() or None
    return RigSweepTargetPoint(
        actor_id=actor_id,
        xyz=xyz,
        semantic_class_id=semantic_class_id,
        semantic_class_name=semantic_class_name,
    )


def _targets_from_explicit_payload(raw_targets: Any, *, field: str) -> list[RigSweepTargetPoint]:
    targets_raw = _as_list(raw_targets, field=field)
    if not targets_raw:
        raise ValueError(f"{field} must be a non-empty list")
    return [_parse_target_point(entry, field=field, index=index) for index, entry in enumerate(targets_raw)]


def _targets_from_sensor_sim_scenario(scenario_path: Path) -> list[RigSweepTargetPoint]:
    payload = _load_json_object(scenario_path, "scenario")
    targets: list[RigSweepTargetPoint] = []
    if isinstance(payload.get("objects"), list):
        for index, raw_object in enumerate(payload["objects"]):
            if not isinstance(raw_object, dict):
                continue
            actor_id = str(raw_object.get("id", raw_object.get("actor_id", ""))).strip()
            if not actor_id or actor_id == "ego":
                continue
            pose_raw = raw_object.get("pose")
            if isinstance(pose_raw, list) and len(pose_raw) >= 3:
                targets.append(
                    RigSweepTargetPoint(
                        actor_id=actor_id,
                        xyz=(float(pose_raw[0]), float(pose_raw[1]), float(pose_raw[2])),
                        semantic_class_name=str(raw_object.get("type", "OTHER")).upper(),
                    )
                )
        if targets:
            return targets

    if str(payload.get("scenario_schema_version", "")).strip() == "scenario_definition_v0":
        lane_width_m = _coerce_float(payload.get("lane_width_m", 3.5), field="lane_width_m")
        for index, raw_npc in enumerate(_as_list(payload.get("npcs", []), field="npcs")):
            if not isinstance(raw_npc, dict):
                continue
            actor_id = str(raw_npc.get("actor_id", f"npc_{index + 1}")).strip() or f"npc_{index + 1}"
            position_m = _coerce_float(raw_npc.get("position_m", 0.0), field=f"npcs[{index}].position_m")
            lane_index = int(raw_npc.get("lane_index", 0))
            targets.append(
                RigSweepTargetPoint(
                    actor_id=actor_id,
                    xyz=(position_m, lane_index * lane_width_m, 0.0),
                    semantic_class_name="VEHICLES",
                )
            )
        if targets:
            return targets

    raise ValueError(
        "no explicit rig-sweep targets found and scenario did not provide usable non-ego target actors"
    )


def _parse_candidate(raw: Any, *, index: int) -> RigSweepCandidate:
    if not isinstance(raw, dict):
        raise ValueError(f"candidates[{index}] must be an object")
    rig_id = str(raw.get("rig_id", "")).strip()
    if not rig_id:
        raise ValueError(f"candidates[{index}] missing rig_id")
    targets = None
    if raw.get("targets") is not None:
        targets = _targets_from_explicit_payload(raw.get("targets"), field=f"candidates[{index}].targets")
    return RigSweepCandidate(
        rig_id=rig_id,
        camera_override=_as_dict(raw.get("camera_override"), field=f"candidates[{index}].camera_override"),
        lidar_override=_as_dict(raw.get("lidar_override"), field=f"candidates[{index}].lidar_override"),
        radar_override=_as_dict(raw.get("radar_override"), field=f"candidates[{index}].radar_override"),
        coverage_override=_as_dict(raw.get("coverage_override"), field=f"candidates[{index}].coverage_override"),
        config_override=_as_dict(raw.get("config_override"), field=f"candidates[{index}].config_override"),
        targets=targets,
    )


def load_rig_sweep_definition(*, base_config_path: Path, rig_candidates_path: Path) -> RigSweepDefinition:
    base_config = _load_json_object(base_config_path, "base config")
    if "scenario_path" not in base_config:
        raise ValueError("base config must include scenario_path")
    base_config_dir = base_config_path.parent.resolve()
    scenario_path = _resolve_path_relative_to(base_config["scenario_path"], base_config_dir)

    payload = _load_json_object(rig_candidates_path, "rig candidates")
    schema_version = str(payload.get("rig_sweep_schema_version", "")).strip()
    if schema_version != RIG_SWEEP_SCHEMA_VERSION_V1:
        raise ValueError(
            f"rig_sweep_schema_version must be {RIG_SWEEP_SCHEMA_VERSION_V1}"
        )
    candidates_raw = _as_list(payload.get("candidates"), field="candidates")
    if not candidates_raw:
        raise ValueError("candidates must be a non-empty list")
    candidates = [_parse_candidate(candidate, index=index) for index, candidate in enumerate(candidates_raw)]
    if payload.get("targets") is not None:
        targets = _targets_from_explicit_payload(payload.get("targets"), field="targets")
    else:
        targets = _targets_from_sensor_sim_scenario(scenario_path)
    return RigSweepDefinition(
        base_config_path=base_config_path.resolve(),
        rig_candidates_path=rig_candidates_path.resolve(),
        targets=targets,
        candidates=candidates,
    )


def _build_effective_config(*, base_config_path: Path, candidate: RigSweepCandidate, candidate_output_dir: Path) -> dict[str, Any]:
    base_config = _load_json_object(base_config_path, "base config")
    base_dir = base_config_path.parent.resolve()
    effective = copy.deepcopy(base_config)
    effective["scenario_path"] = str(_resolve_path_relative_to(base_config["scenario_path"], base_dir))
    effective["output_dir"] = str(candidate_output_dir.resolve())
    effective.setdefault("options", {})
    if not isinstance(effective["options"], dict):
        raise ValueError("base config options must be a JSON object")
    effective["options"] = _deep_merge(
        dict(effective["options"]),
        candidate.camera_override,
    )
    effective["options"] = _deep_merge(effective["options"], candidate.lidar_override)
    effective["options"] = _deep_merge(effective["options"], candidate.radar_override)
    if candidate.coverage_override:
        effective["options"] = _deep_merge(
            effective["options"],
            {"coverage_metrics": candidate.coverage_override},
        )
    if candidate.config_override:
        effective = _deep_merge(effective, candidate.config_override)
    effective["scenario_path"] = str(_resolve_path_relative_to(effective["scenario_path"], base_dir))
    effective["output_dir"] = str(candidate_output_dir.resolve())
    return effective


def _target_points_to_options(targets: list[RigSweepTargetPoint]) -> dict[str, Any]:
    actor_ids = [target.actor_id for target in targets]
    semantic_class_ids = [target.semantic_class_id for target in targets]
    semantic_class_names = [target.semantic_class_name for target in targets]
    points_xyz = [target.xyz for target in targets]
    options: dict[str, Any] = {}
    for prefix in ("camera", "lidar", "radar"):
        options[f"{prefix}_point_actor_ids"] = actor_ids
        if any(value is not None for value in semantic_class_ids):
            options[f"{prefix}_point_semantic_class_ids"] = semantic_class_ids
        if any(value is not None for value in semantic_class_names):
            options[f"{prefix}_point_semantic_class_names"] = semantic_class_names
    options["rig_sweep_target_points_xyz"] = [list(point) for point in points_xyz]
    return options


def _sum_combined_count(coverage_summary: Mapping[str, Any], field: str) -> int:
    total = 0
    combined = coverage_summary.get("combined", {})
    for target in combined.get("targets", []):
        if not isinstance(target, dict):
            continue
        total += int(target.get(field, 0))
    return total


def _heuristic_score(*, covered_target_count: int, blindspot_target_count: int, lidar_plus_radar: int, camera_pixels_on_target: int, active_sensor_count: int) -> float:
    return round(
        (covered_target_count * 1_000_000.0)
        - (blindspot_target_count * 10_000.0)
        + (lidar_plus_radar * 10.0)
        + float(camera_pixels_on_target)
        - (active_sensor_count * 0.01),
        6,
    )


def _evaluate_candidate(
    *,
    backend: NativePhysicsBackend,
    base_config_path: Path,
    candidate: RigSweepCandidate,
    default_targets: list[RigSweepTargetPoint],
    candidate_output_dir: Path,
) -> RigSweepEvaluation:
    candidate_output_dir.mkdir(parents=True, exist_ok=True)
    effective_config = _build_effective_config(
        base_config_path=base_config_path,
        candidate=candidate,
        candidate_output_dir=candidate_output_dir,
    )
    targets = candidate.targets if candidate.targets is not None else default_targets
    effective_config["options"] = _deep_merge(
        dict(effective_config.get("options", {})),
        _target_points_to_options(targets),
    )
    effective_config_path = candidate_output_dir / "effective_config.json"
    effective_config_path.write_text(
        json.dumps(effective_config, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )

    points_xyz = [target.xyz for target in targets]
    point_cloud_path = candidate_output_dir / "rig_targets.xyz"
    write_xyz_points(point_cloud_path, points_xyz)

    request = SensorSimRequest(
        scenario_path=Path(str(effective_config["scenario_path"])).resolve(),
        output_dir=candidate_output_dir,
        sensor_profile=str(effective_config.get("sensor_profile", "default")),
        seed=int(effective_config.get("seed", 0)),
        options=dict(effective_config.get("options", {})),
    )
    config = backend._sensor_config_from_request(request)
    artifacts: dict[str, Path] = {"point_cloud_primary": point_cloud_path}
    metrics: dict[str, float] = {}

    config_path = candidate_output_dir / "sensor_sim_config.json"
    config_path.write_text(json.dumps(config.to_manifest(), indent=2), encoding="utf-8")
    artifacts["sensor_sim_config"] = config_path

    camera_artifact = backend._project_xyz_if_available(
        request=request,
        artifacts=artifacts,
        enhanced_output=candidate_output_dir,
        camera_config=config.camera,
        intrinsics=config.camera.intrinsics.to_camera_intrinsics(),
        distortion=config.camera.distortion_coeffs.to_brown_conrady(),
        extrinsics=config.camera.extrinsics.to_camera_extrinsics(),
        metrics=metrics,
    )
    if camera_artifact is not None:
        artifacts["camera_projection_preview"] = camera_artifact

    lidar_xyz_artifact, lidar_json_artifact = backend._generate_lidar_noisy_pointcloud_if_available(
        request=request,
        artifacts=artifacts,
        enhanced_output=candidate_output_dir,
        metrics=metrics,
    )
    if lidar_xyz_artifact is not None:
        artifacts["lidar_noisy_preview"] = lidar_xyz_artifact
    if lidar_json_artifact is not None:
        artifacts["lidar_noisy_preview_json"] = lidar_json_artifact

    radar_artifact = backend._generate_radar_targets_if_available(
        request=request,
        artifacts=artifacts,
        enhanced_output=candidate_output_dir,
        metrics=metrics,
    )
    if radar_artifact is not None:
        artifacts["radar_targets_preview"] = radar_artifact

    coverage_summary_path = backend._generate_sensor_coverage_summary_if_available(
        request=request,
        artifacts=artifacts,
        enhanced_output=candidate_output_dir,
        metrics=metrics,
    )
    coverage_summary: dict[str, Any]
    if coverage_summary_path is not None and coverage_summary_path.exists():
        coverage_summary = _load_json_object(coverage_summary_path, "coverage summary")
    else:
        coverage_summary = {
            "schema_version": "1.0",
            "sensors": {},
            "combined": {
                "target_count": 0,
                "covered_target_count": 0,
                "blindspot_target_count": 0,
                "overlap_target_count": 0,
                "available_sensor_count": 0,
                "available_sensors": [],
                "targets": [],
            },
        }

    combined = dict(coverage_summary.get("combined", {}))
    camera_pixels_on_target = _sum_combined_count(coverage_summary, "camera_pixels_on_target")
    lidar_points_on_target = _sum_combined_count(coverage_summary, "lidar_points_on_target")
    radar_detections_on_target = _sum_combined_count(coverage_summary, "radar_detections_on_target")
    available_sensor_count = int(combined.get("available_sensor_count", 0))
    active_sensor_count = available_sensor_count
    covered_target_count = int(combined.get("covered_target_count", 0))
    blindspot_target_count = int(combined.get("blindspot_target_count", 0))
    overlap_target_count = int(combined.get("overlap_target_count", 0))
    lidar_plus_radar = lidar_points_on_target + radar_detections_on_target
    heuristic_score = _heuristic_score(
        covered_target_count=covered_target_count,
        blindspot_target_count=blindspot_target_count,
        lidar_plus_radar=lidar_plus_radar,
        camera_pixels_on_target=camera_pixels_on_target,
        active_sensor_count=active_sensor_count,
    )
    sort_key = (
        -covered_target_count,
        blindspot_target_count,
        -lidar_plus_radar,
        -camera_pixels_on_target,
        active_sensor_count,
        candidate.rig_id,
    )
    preview_artifacts = {
        key: str(path)
        for key, path in artifacts.items()
        if key in {
            "camera_projection_preview",
            "lidar_noisy_preview",
            "lidar_noisy_preview_json",
            "radar_targets_preview",
            "sensor_sim_config",
        }
    }
    return RigSweepEvaluation(
        rig_id=candidate.rig_id,
        heuristic_score=heuristic_score,
        covered_target_count=covered_target_count,
        blindspot_target_count=blindspot_target_count,
        overlap_target_count=overlap_target_count,
        available_sensor_count=available_sensor_count,
        active_sensor_count=active_sensor_count,
        camera_pixels_on_target=camera_pixels_on_target,
        lidar_points_on_target=lidar_points_on_target,
        radar_detections_on_target=radar_detections_on_target,
        sort_key=sort_key,
        candidate_output_dir=candidate_output_dir,
        effective_config_path=effective_config_path,
        point_cloud_path=point_cloud_path,
        coverage_summary_path=coverage_summary_path,
        preview_artifacts=preview_artifacts,
        coverage_summary=coverage_summary,
    )


def run_sensor_rig_sweep(*, base_config_path: Path, rig_candidates_path: Path, out_root: Path) -> dict[str, Any]:
    out_root = out_root.resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    definition = load_rig_sweep_definition(
        base_config_path=base_config_path.resolve(),
        rig_candidates_path=rig_candidates_path.resolve(),
    )
    backend = NativePhysicsBackend()
    candidate_rows: list[RigSweepEvaluation] = []
    candidates_root = out_root / "candidates"
    for candidate in definition.candidates:
        candidate_rows.append(
            _evaluate_candidate(
                backend=backend,
                base_config_path=definition.base_config_path,
                candidate=candidate,
                default_targets=definition.targets,
                candidate_output_dir=candidates_root / candidate.rig_id,
            )
        )

    ordered = sorted(candidate_rows, key=lambda row: row.sort_key)
    rankings: list[dict[str, Any]] = []
    for rank, row in enumerate(ordered, start=1):
        rankings.append(
            {
                "rank": rank,
                "rig_id": row.rig_id,
                "heuristic_score": row.heuristic_score,
                "covered_target_count": row.covered_target_count,
                "blindspot_target_count": row.blindspot_target_count,
                "overlap_target_count": row.overlap_target_count,
                "available_sensor_count": row.available_sensor_count,
                "active_sensor_count": row.active_sensor_count,
                "camera_pixels_on_target": row.camera_pixels_on_target,
                "lidar_points_on_target": row.lidar_points_on_target,
                "radar_detections_on_target": row.radar_detections_on_target,
                "ranking_key": {
                    "covered_target_count_desc": row.covered_target_count,
                    "blindspot_target_count_asc": row.blindspot_target_count,
                    "lidar_plus_radar_desc": row.lidar_points_on_target + row.radar_detections_on_target,
                    "camera_pixels_on_target_desc": row.camera_pixels_on_target,
                    "active_sensor_count_asc": row.active_sensor_count,
                    "rig_id_asc": row.rig_id,
                },
                "candidate_output_dir": str(row.candidate_output_dir),
                "effective_config_path": str(row.effective_config_path),
                "point_cloud_path": str(row.point_cloud_path),
                "coverage_summary_path": str(row.coverage_summary_path) if row.coverage_summary_path is not None else None,
                "preview_artifacts": row.preview_artifacts,
                "coverage_summary": row.coverage_summary,
            }
        )

    report = {
        "sensor_rig_sweep_report_schema_version": SENSOR_RIG_SWEEP_REPORT_SCHEMA_VERSION_V1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base_config_path": str(definition.base_config_path),
        "rig_candidates_path": str(definition.rig_candidates_path),
        "target_count": len(definition.targets),
        "targets": [
            {
                "actor_id": target.actor_id,
                "xyz": [target.xyz[0], target.xyz[1], target.xyz[2]],
                "semantic_class_id": target.semantic_class_id,
                "semantic_class_name": target.semantic_class_name,
            }
            for target in definition.targets
        ],
        "candidate_count": len(rankings),
        "best_rig_id": rankings[0]["rig_id"] if rankings else "",
        "ranking_policy": [
            "covered_target_count desc",
            "blindspot_target_count asc",
            "lidar_points_on_target + radar_detections_on_target desc",
            "camera_pixels_on_target desc",
            "active_sensor_count asc",
            "rig_id asc",
        ],
        "rankings": rankings,
    }
    report_path = out_root / "sensor_rig_sweep_report_v1.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    report["report_path"] = str(report_path)
    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate sensor rig candidates using current native preview and coverage outputs.")
    parser.add_argument("--base-config", required=True, help="Path to base hybrid_sensor_sim JSON config")
    parser.add_argument("--rig-candidates", required=True, help="Path to sensor_rig_sweep_v1 JSON file")
    parser.add_argument("--out", required=True, help="Output directory for report and per-candidate artifacts")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        report = run_sensor_rig_sweep(
            base_config_path=Path(args.base_config).resolve(),
            rig_candidates_path=Path(args.rig_candidates).resolve(),
            out_root=Path(args.out).resolve(),
        )
        print(f"[ok] candidate_count={report['candidate_count']}")
        print(f"[ok] best_rig_id={report['best_rig_id']}")
        print(f"[ok] report={report['report_path']}")
        return 0
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] sensor_rig_sweep.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
