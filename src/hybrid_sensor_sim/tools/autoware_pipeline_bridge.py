from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.autoware.export_bridge import (
    write_autoware_export_bundle,
    write_autoware_planned_export_bundle,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an Autoware-facing sensor/data contract bundle from backend smoke workflow reports."
    )
    parser.add_argument("--backend-smoke-workflow-report", default="", help="Path to scenario_backend_smoke_workflow_report_v0.json")
    parser.add_argument(
        "--supplemental-backend-smoke-workflow-report",
        action="append",
        default=[],
        help="Optional supplemental scenario_backend_smoke_workflow_report_v0.json paths merged into the Autoware bridge",
    )
    parser.add_argument("--runtime-backend-workflow-report", default="", help="Path to scenario_runtime_backend_workflow_report_v0.json")
    parser.add_argument("--out-root", required=True, help="Output root for the Autoware export bundle")
    parser.add_argument("--base-frame", default="base_link", help="Base frame ID for generated frame tree")
    parser.add_argument(
        "--consumer-profile",
        default="",
        help="Optional Autoware consumer profile ID for stricter required topic/output expectations",
    )
    parser.add_argument("--strict", action="store_true", help="Fail if required sensor outputs are missing")
    return parser.parse_args(argv)


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    payload["__source_path"] = str(path.resolve())
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def _detect_repo_root(anchor_path: Path) -> Path | None:
    current = anchor_path.resolve()
    if current.is_file():
        current = current.parent
    while current.parent != current:
        if (current / "src" / "hybrid_sensor_sim").exists() and (current / "tests").exists():
            return current
        current = current.parent
    return None


def _rebase_workspace_path(path_text: str, *, repo_root: Path | None) -> str:
    text = str(path_text).strip()
    if not text:
        return text
    if not text.startswith("/workspace"):
        return text
    if repo_root is None:
        return text
    relative_text = text.removeprefix("/workspace").lstrip("/")
    if not relative_text:
        return str(repo_root.resolve())
    return str((repo_root / relative_text).resolve())


def _normalize_workspace_paths(payload: Any, *, repo_root: Path | None) -> Any:
    if repo_root is None:
        return payload
    if isinstance(payload, dict):
        return {
            key: _normalize_workspace_paths(value, repo_root=repo_root)
            for key, value in payload.items()
        }
    if isinstance(payload, list):
        return [
            _normalize_workspace_paths(value, repo_root=repo_root)
            for value in payload
        ]
    if isinstance(payload, str):
        return _rebase_workspace_path(payload, repo_root=repo_root)
    return payload


def _load_json_object_with_workspace_rebase(
    path: Path,
    *,
    repo_root: Path | None,
) -> dict[str, Any]:
    payload = _load_json_object(path)
    payload = _normalize_workspace_paths(payload, repo_root=repo_root)
    payload["__source_path"] = str(path.resolve())
    return payload


def _load_report(path: Path) -> dict[str, Any]:
    payload = _load_json_object(path)
    return payload


def _resolve_backend_smoke_report(report: dict[str, Any]) -> dict[str, Any]:
    if str(report.get("scenario_backend_smoke_workflow_report_schema_version", "")).strip():
        return report
    if str(report.get("scenario_runtime_backend_workflow_report_schema_version", "")).strip():
        backend_report_path = str(report.get("artifacts", {}).get("backend_smoke_workflow_report_path", "")).strip()
        if not backend_report_path:
            raise ValueError("runtime backend workflow report missing backend smoke workflow report path")
        return _load_report(Path(backend_report_path))
    raise ValueError("unsupported workflow report schema for Autoware pipeline bridge")


def _load_smoke_summary(backend_workflow_report: dict[str, Any]) -> dict[str, Any]:
    summary_path = str(backend_workflow_report.get("smoke", {}).get("summary_path", "")).strip()
    if not summary_path:
        raise ValueError("backend smoke workflow report missing smoke.summary_path")
    summary_path_obj = Path(summary_path).resolve()
    repo_root = _detect_repo_root(summary_path_obj)
    return _load_json_object_with_workspace_rebase(
        summary_path_obj,
        repo_root=repo_root,
    )


def _load_optional_smoke_summary(backend_workflow_report: dict[str, Any]) -> dict[str, Any] | None:
    raw_summary_path = backend_workflow_report.get("smoke", {}).get("summary_path", "")
    summary_path = str(raw_summary_path).strip()
    if raw_summary_path is None or not summary_path or summary_path.lower() == "none":
        return None
    summary_path_obj = Path(summary_path).resolve()
    repo_root = _detect_repo_root(summary_path_obj)
    return _load_json_object_with_workspace_rebase(
        summary_path_obj,
        repo_root=repo_root,
    )


def _load_artifact(
    path_text: str,
    *,
    field: str,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    text = str(path_text).strip()
    if not text:
        raise ValueError(f"missing required artifact path: {field}")
    resolved_text = _rebase_workspace_path(text, repo_root=repo_root)
    return _load_json_object_with_workspace_rebase(
        Path(resolved_text).resolve(),
        repo_root=repo_root,
    )


def _load_smoke_input_config(backend_workflow_report: dict[str, Any]) -> dict[str, Any]:
    return _load_artifact(
        backend_workflow_report.get("artifacts", {}).get("smoke_input_config_path", ""),
        field="smoke_input_config_path",
    )


def _has_runtime_smoke_summary(backend_workflow_report: dict[str, Any]) -> bool:
    summary_path = str(backend_workflow_report.get("smoke", {}).get("summary_path", "")).strip()
    return bool(summary_path)


def _load_runtime_bridge_inputs(
    backend_workflow_report: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    smoke_summary = _load_smoke_summary(backend_workflow_report)
    smoke_repo_root = _detect_repo_root(
        Path(str(smoke_summary.get("__source_path", "")).strip())
    )
    smoke_artifacts = dict(smoke_summary.get("artifacts", {}))
    playback_contract = _load_artifact(
        smoke_artifacts.get("renderer_playback_contract", ""),
        field="renderer_playback_contract",
        repo_root=smoke_repo_root,
    )
    backend_output_spec = _load_artifact(
        smoke_artifacts.get("backend_output_spec", ""),
        field="backend_output_spec",
        repo_root=smoke_repo_root,
    )
    backend_sensor_output_summary = _load_artifact(
        smoke_artifacts.get("backend_sensor_output_summary", ""),
        field="backend_sensor_output_summary",
        repo_root=smoke_repo_root,
    )
    return smoke_summary, playback_contract, backend_output_spec, backend_sensor_output_summary


def _best_entry(*entries: dict[str, Any] | None) -> dict[str, Any]:
    best: dict[str, Any] = {}
    best_score = -1
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        score = 0
        if bool(entry.get("exists")):
            score += 4
        if str(entry.get("resolved_path", "")).strip():
            score += 2
        if str(entry.get("output_origin", "")).strip() == "backend_runtime":
            score += 1
        if score >= best_score:
            best = dict(entry)
            best_score = score
    return best


def _merge_backend_sensor_output_summaries(
    summaries: list[dict[str, Any]],
) -> dict[str, Any]:
    backend = ""
    merged_sensors: dict[str, dict[str, Any]] = {}
    for summary in summaries:
        backend = backend or str(summary.get("backend", "")).strip()
        for sensor in summary.get("sensors", []) or []:
            if not isinstance(sensor, dict):
                continue
            sensor_id = str(sensor.get("sensor_id", "")).strip()
            if not sensor_id:
                continue
            target = merged_sensors.setdefault(
                sensor_id,
                {
                    "sensor_id": sensor_id,
                    "modality": str(sensor.get("modality", "")).strip() or None,
                    "outputs": {},
                },
            )
            if not target.get("modality"):
                target["modality"] = str(sensor.get("modality", "")).strip() or None
            for output in sensor.get("outputs", []) or []:
                if not isinstance(output, dict):
                    continue
                output_role = str(output.get("output_role", "")).strip()
                if not output_role:
                    continue
                target["outputs"][output_role] = _best_entry(
                    target["outputs"].get(output_role),
                    output,
                )
    return {
        "backend": backend or None,
        "sensors": [
            {
                "sensor_id": sensor_id,
                "modality": sensor.get("modality"),
                "outputs": [
                    sensor["outputs"][output_role]
                    for output_role in sorted(sensor["outputs"])
                ],
            }
            for sensor_id, sensor in sorted(merged_sensors.items())
        ],
    }


def _merge_backend_output_specs(specs: list[dict[str, Any]]) -> dict[str, Any]:
    backend = ""
    output_root = ""
    merged_expected_outputs: dict[tuple[str, str], dict[str, Any]] = {}
    merged_sensors: dict[str, dict[str, Any]] = {}
    for spec in specs:
        backend = backend or str(spec.get("backend", "")).strip()
        output_root = output_root or str(spec.get("output_root", "")).strip()
        for sensor in spec.get("expected_outputs_by_sensor", []) or []:
            if not isinstance(sensor, dict):
                continue
            sensor_id = str(sensor.get("sensor_id", "")).strip()
            if not sensor_id:
                continue
            target = merged_sensors.setdefault(sensor_id, {"sensor_id": sensor_id, "outputs": {}})
            for output in sensor.get("outputs", []) or []:
                if not isinstance(output, dict):
                    continue
                output_role = str(output.get("output_role", "")).strip()
                if not output_role:
                    continue
                target["outputs"][output_role] = _best_entry(
                    target["outputs"].get(output_role),
                    output,
                )
        for output in spec.get("expected_outputs", []) or []:
            if not isinstance(output, dict):
                continue
            sensor_id = str(output.get("sensor_id", "")).strip()
            output_role = str(output.get("output_role", "")).strip()
            if not sensor_id or not output_role:
                continue
            merged_expected_outputs[(sensor_id, output_role)] = _best_entry(
                merged_expected_outputs.get((sensor_id, output_role)),
                output,
            )
    return {
        "backend": backend or None,
        "output_root": output_root or None,
        "expected_outputs": [
            merged_expected_outputs[key]
            for key in sorted(merged_expected_outputs)
        ],
        "expected_outputs_by_sensor": [
            {
                "sensor_id": sensor_id,
                "outputs": [
                    sensor["outputs"][output_role]
                    for output_role in sorted(sensor["outputs"])
                ],
            }
            for sensor_id, sensor in sorted(merged_sensors.items())
        ],
    }


def _merge_playback_contracts(playback_contracts: list[dict[str, Any]]) -> dict[str, Any]:
    merged_mounts: dict[str, dict[str, Any]] = {}
    for contract in playback_contracts:
        for mount in contract.get("renderer_sensor_mounts", []) or []:
            if not isinstance(mount, dict):
                continue
            sensor_id = str(mount.get("sensor_id", "")).strip()
            if not sensor_id:
                continue
            merged_mounts[sensor_id] = _best_entry(merged_mounts.get(sensor_id), mount)
    return {
        "renderer_sensor_mounts": [
            merged_mounts[sensor_id]
            for sensor_id in sorted(merged_mounts)
        ]
    }


def _merge_output_origin_counts(smoke_summaries: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"backend_runtime": 0, "sidecar_materialized": 0, "missing": 0}
    for smoke_summary in smoke_summaries:
        output_smoke = (
            dict(smoke_summary.get("output_smoke_report", {}))
            if isinstance(smoke_summary.get("output_smoke_report"), dict)
            else {}
        )
        source_counts = dict(output_smoke.get("output_origin_counts", {}))
        if source_counts:
            for key in counts:
                counts[key] += int(source_counts.get(key, 0) or 0)
            continue
        source_status = str(output_smoke.get("output_origin_status", "")).strip()
        if source_status == "BACKEND_RUNTIME_ONLY":
            counts["backend_runtime"] += 1
        elif source_status == "SIDECAR_ONLY":
            counts["sidecar_materialized"] += 1
        elif source_status == "MIXED":
            counts["backend_runtime"] += 1
            counts["sidecar_materialized"] += 1
    return counts


def _output_origin_status_from_counts(counts: dict[str, int]) -> str | None:
    runtime_count = int(counts.get("backend_runtime", 0) or 0)
    sidecar_count = int(counts.get("sidecar_materialized", 0) or 0)
    if runtime_count > 0 and sidecar_count == 0:
        return "BACKEND_RUNTIME_ONLY"
    if runtime_count == 0 and sidecar_count > 0:
        return "SIDECAR_ONLY"
    if runtime_count > 0 and sidecar_count > 0:
        return "MIXED"
    return None


def _merge_smoke_summaries(smoke_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    backend = str(smoke_summaries[0].get("backend", "")).strip() if smoke_summaries else ""
    comparison_statuses = [
        str((summary.get("output_comparison") or {}).get("status", "")).strip()
        for summary in smoke_summaries
        if isinstance(summary.get("output_comparison"), dict)
    ]
    smoke_statuses = [
        str((summary.get("output_smoke_report") or {}).get("status", "")).strip()
        for summary in smoke_summaries
        if isinstance(summary.get("output_smoke_report"), dict)
    ]
    mismatch_reasons = sorted(
        {
            str(reason).strip()
            for summary in smoke_summaries
            for reason in list((summary.get("output_comparison") or {}).get("mismatch_reasons", []) or [])
            if str(reason).strip()
        }
    )
    unexpected_output_count = sum(
        int((summary.get("output_comparison") or {}).get("unexpected_output_count", 0) or 0)
        for summary in smoke_summaries
        if isinstance(summary.get("output_comparison"), dict)
    )
    output_origin_counts = _merge_output_origin_counts(smoke_summaries)
    output_origin_status = _output_origin_status_from_counts(output_origin_counts)
    output_origin_reasons = sorted(
        {
            str(reason).strip()
            for summary in smoke_summaries
            for reason in list((summary.get("output_smoke_report") or {}).get("output_origin_reasons", []) or [])
            if str(reason).strip()
        }
    )
    coverage_ratios = [
        float((summary.get("output_smoke_report") or {}).get("coverage_ratio"))
        for summary in smoke_summaries
        if isinstance(summary.get("output_smoke_report"), dict)
        and (summary.get("output_smoke_report") or {}).get("coverage_ratio") is not None
    ]
    output_smoke_status = (
        "COMPLETE"
        if smoke_statuses and all(status == "COMPLETE" for status in smoke_statuses)
        else (next((status for status in smoke_statuses if status), None) or None)
    )
    output_comparison_status = (
        "MATCHED"
        if comparison_statuses and all(status == "MATCHED" for status in comparison_statuses)
        else (
            next((status for status in comparison_statuses if status and status != "MATCHED"), None)
            or next((status for status in comparison_statuses if status), None)
        )
    )
    return {
        "backend": backend or None,
        "success": all(bool(summary.get("success", True)) for summary in smoke_summaries),
        "run": {"status": "MERGED", "failure_reason": None},
        "output_smoke_report": {
            "status": output_smoke_status,
            "coverage_ratio": min(coverage_ratios) if coverage_ratios else None,
            "output_origin_status": output_origin_status,
            "output_origin_counts": output_origin_counts,
            "output_origin_reasons": output_origin_reasons,
        },
        "output_comparison": {
            "status": output_comparison_status,
            "mismatch_reasons": mismatch_reasons,
            "unexpected_output_count": unexpected_output_count,
            "output_origin_status": output_origin_status,
            "output_origin_counts": output_origin_counts,
        },
        "artifacts": {},
    }


def _write_merged_runtime_inputs(
    *,
    out_root: Path,
    smoke_summaries: list[dict[str, Any]],
    playback_contracts: list[dict[str, Any]],
    backend_output_specs: list[dict[str, Any]],
    backend_sensor_output_summaries: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    merged_root = out_root / "_merged_runtime_inputs"
    merged_root.mkdir(parents=True, exist_ok=True)
    merged_smoke_summary = _merge_smoke_summaries(smoke_summaries)
    merged_playback_contract = _merge_playback_contracts(playback_contracts)
    merged_backend_output_spec = _merge_backend_output_specs(backend_output_specs)
    merged_backend_sensor_output_summary = _merge_backend_sensor_output_summaries(
        backend_sensor_output_summaries
    )
    merged_smoke_summary_path = merged_root / "merged_renderer_backend_smoke_summary.json"
    merged_playback_contract_path = merged_root / "merged_renderer_playback_contract.json"
    merged_backend_output_spec_path = merged_root / "merged_backend_output_spec.json"
    merged_backend_sensor_output_summary_path = (
        merged_root / "merged_backend_sensor_output_summary.json"
    )
    merged_smoke_summary["artifacts"] = {
        "renderer_playback_contract": str(merged_playback_contract_path.resolve()),
        "backend_output_spec": str(merged_backend_output_spec_path.resolve()),
        "backend_sensor_output_summary": str(
            merged_backend_sensor_output_summary_path.resolve()
        ),
    }
    _write_json(merged_smoke_summary_path, merged_smoke_summary)
    _write_json(merged_playback_contract_path, merged_playback_contract)
    _write_json(merged_backend_output_spec_path, merged_backend_output_spec)
    _write_json(
        merged_backend_sensor_output_summary_path,
        merged_backend_sensor_output_summary,
    )
    merged_smoke_summary["__source_path"] = str(merged_smoke_summary_path.resolve())
    merged_playback_contract["__source_path"] = str(
        merged_playback_contract_path.resolve()
    )
    merged_backend_output_spec["__source_path"] = str(
        merged_backend_output_spec_path.resolve()
    )
    merged_backend_sensor_output_summary["__source_path"] = str(
        merged_backend_sensor_output_summary_path.resolve()
    )
    return (
        merged_smoke_summary,
        merged_playback_contract,
        merged_backend_output_spec,
        merged_backend_sensor_output_summary,
    )


def run_autoware_pipeline_bridge(
    *,
    backend_smoke_workflow_report_path: str,
    supplemental_backend_smoke_workflow_report_paths: list[str] | None = None,
    runtime_backend_workflow_report_path: str,
    out_root: Path,
    base_frame: str = "base_link",
    consumer_profile_id: str = "",
    strict: bool = False,
) -> dict[str, Any]:
    report_path_text = str(backend_smoke_workflow_report_path).strip() or str(runtime_backend_workflow_report_path).strip()
    if not report_path_text:
        raise ValueError("provide exactly one workflow report path")
    if bool(str(backend_smoke_workflow_report_path).strip()) == bool(str(runtime_backend_workflow_report_path).strip()):
        raise ValueError("provide exactly one of backend or runtime workflow report path")
    input_report = _load_report(Path(report_path_text))
    backend_report = _resolve_backend_smoke_report(input_report)
    supplemental_report_paths = [
        str(path).strip()
        for path in list(supplemental_backend_smoke_workflow_report_paths or [])
        if str(path).strip()
    ]
    supplemental_backend_reports = [
        _resolve_backend_smoke_report(_load_report(Path(path)))
        for path in supplemental_report_paths
    ]
    backend_reports = [backend_report, *supplemental_backend_reports]
    selection = dict(backend_report.get("selection", {}))
    bridge = dict(backend_report.get("bridge", {}))
    run_id = (
        str(selection.get("variant_id", "")).strip()
        or str(bridge.get("scenario_id", "")).strip()
        or "autoware_bridge_run"
    )
    scenario_source = {
        "variant_id": str(selection.get("variant_id", "")).strip() or None,
        "logical_scenario_id": str(selection.get("logical_scenario_id", "")).strip() or None,
        "scenario_id": str(bridge.get("scenario_id", "")).strip() or None,
        "source_payload_kind": str(bridge.get("source_payload_kind", "")).strip() or None,
        "source_payload_path": str(bridge.get("source_payload_path", "")).strip() or None,
        "smoke_scenario_path": str(backend_report.get("artifacts", {}).get("smoke_scenario_path", "")).strip() or None,
        "bridge_manifest_path": str(backend_report.get("artifacts", {}).get("bridge_manifest_path", "")).strip() or None,
    }
    smoke_summary = _load_optional_smoke_summary(backend_report)
    if smoke_summary is not None:
        if len(backend_reports) > 1:
            runtime_reports = [
                report for report in backend_reports if _has_runtime_smoke_summary(report)
            ]
            if len(runtime_reports) != len(backend_reports):
                raise ValueError(
                    "supplemental backend smoke workflow reports require runtime smoke summaries"
                )
            smoke_summaries: list[dict[str, Any]] = []
            playback_contracts: list[dict[str, Any]] = []
            backend_output_specs: list[dict[str, Any]] = []
            backend_sensor_output_summaries: list[dict[str, Any]] = []
            for report in runtime_reports:
                (
                    runtime_smoke_summary,
                    runtime_playback_contract,
                    runtime_backend_output_spec,
                    runtime_backend_sensor_output_summary,
                ) = _load_runtime_bridge_inputs(report)
                smoke_summaries.append(runtime_smoke_summary)
                playback_contracts.append(runtime_playback_contract)
                backend_output_specs.append(runtime_backend_output_spec)
                backend_sensor_output_summaries.append(
                    runtime_backend_sensor_output_summary
                )
            (
                smoke_summary,
                playback_contract,
                backend_output_spec,
                backend_sensor_output_summary,
            ) = _write_merged_runtime_inputs(
                out_root=out_root,
                smoke_summaries=smoke_summaries,
                playback_contracts=playback_contracts,
                backend_output_specs=backend_output_specs,
                backend_sensor_output_summaries=backend_sensor_output_summaries,
            )
        else:
            (
                smoke_summary,
                playback_contract,
                backend_output_spec,
                backend_sensor_output_summary,
            ) = _load_runtime_bridge_inputs(backend_report)
        bundle = write_autoware_export_bundle(
            out_root=out_root,
            backend=str(backend_report.get("backend", "")).strip(),
            run_id=run_id,
            scenario_source=scenario_source,
            backend_sensor_output_summary=backend_sensor_output_summary,
            backend_output_spec=backend_output_spec,
            playback_contract=playback_contract,
            smoke_summary=smoke_summary,
            strict=bool(strict),
            base_frame=base_frame,
            consumer_profile_id=consumer_profile_id,
        )
    else:
        if str(backend_report.get("status", "")).strip() not in {
            "HANDOFF_READY",
            "HANDOFF_DOCKER_VERIFIED",
            "HANDOFF_DOCKER_EXECUTED",
        }:
            raise ValueError(
                "backend smoke workflow report missing smoke.summary_path and is not in a handoff-ready state"
            )
        smoke_input_config = _load_smoke_input_config(backend_report)
        bundle = write_autoware_planned_export_bundle(
            out_root=out_root,
            backend=str(backend_report.get("backend", "")).strip(),
            run_id=run_id,
            scenario_source=scenario_source,
            smoke_input_config=smoke_input_config,
            strict=bool(strict),
            base_frame=base_frame,
            consumer_profile_id=consumer_profile_id,
        )
    report = {
        "backend": str(backend_report.get("backend", "")).strip() or None,
        "run_id": run_id,
        "status": bundle["status"],
        "strict": bool(strict),
        "availability_mode": bundle.get("availability_mode"),
        "consumer_profile_id": bundle.get("consumer_profile_id"),
        "consumer_profile_description": bundle.get("consumer_profile_description"),
        "available_sensor_count": bundle["available_sensor_count"],
        "missing_required_sensor_count": bundle["missing_required_sensor_count"],
        "available_topics": bundle["available_topics"],
        "consumer_ready": bool(bundle.get("consumer_ready")),
        "topic_export_count": bundle.get("topic_export_count"),
        "materialized_topic_export_count": bundle.get("materialized_topic_export_count"),
        "required_topic_count": bundle.get("required_topic_count"),
        "missing_required_topic_count": bundle.get("missing_required_topic_count"),
        "available_message_types": list(bundle.get("available_message_types", [])),
        "subscription_spec_count": bundle.get("subscription_spec_count"),
        "sensor_input_count": bundle.get("sensor_input_count"),
        "static_transform_count": bundle.get("static_transform_count"),
        "processing_stage_count": bundle.get("processing_stage_count"),
        "ready_processing_stage_count": bundle.get("ready_processing_stage_count"),
        "degraded_processing_stage_count": bundle.get(
            "degraded_processing_stage_count"
        ),
        "available_modalities": list(bundle.get("available_modalities", [])),
        "data_roots": list(bundle.get("data_roots", [])),
        "recording_style": bundle.get("recording_style"),
        "dataset_ready": bool(bundle.get("dataset_ready")),
        "scenario_source": dict(bundle.get("scenario_source", {})),
        "merged_report_count": len(backend_reports),
        "supplemental_backend_smoke_workflow_report_paths": [
            str(Path(path).resolve()) for path in supplemental_report_paths
        ],
        "required_topics_complete": bundle["required_topics_complete"],
        "frame_tree_complete": bundle["frame_tree_complete"],
        "warnings": list(bundle["warnings"]),
        "artifacts": dict(bundle["artifacts"]),
    }
    report_path = out_root / "autoware_pipeline_bridge_report_v0.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return {
        "report_path": report_path,
        "report": report,
        "bundle": bundle,
    }


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        result = run_autoware_pipeline_bridge(
            backend_smoke_workflow_report_path=args.backend_smoke_workflow_report,
            supplemental_backend_smoke_workflow_report_paths=list(
                args.supplemental_backend_smoke_workflow_report
            ),
            runtime_backend_workflow_report_path=args.runtime_backend_workflow_report,
            out_root=Path(args.out_root).resolve(),
            base_frame=args.base_frame,
            consumer_profile_id=args.consumer_profile,
            strict=bool(args.strict),
        )
        print(f"[ok] autoware_status={result['report']['status']}")
        print(f"[ok] report={result['report_path']}")
        return (
            0
            if result["report"]["status"]
            in {
                "READY",
                "DEGRADED",
                "PLANNED",
                "SIDECAR_READY",
                "SIDECAR_DEGRADED",
                "MIXED_READY",
                "MIXED_DEGRADED",
            }
            else 2
        )
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        print(f"[error] run_autoware_pipeline_bridge.py: {exc}", file=sys.stderr)
        return 2


__all__ = [
    "run_autoware_pipeline_bridge",
    "main",
]
