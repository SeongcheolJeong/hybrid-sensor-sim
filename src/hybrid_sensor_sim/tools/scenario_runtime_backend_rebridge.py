from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.tools.autonomy_e2e_history_guard import (
    build_autonomy_e2e_history_guard_report,
)
from hybrid_sensor_sim.tools.autoware_pipeline_bridge import (
    run_autoware_pipeline_bridge,
)
from hybrid_sensor_sim.tools.scenario_backend_smoke_workflow import (
    _extract_backend_runtime_diagnostics,
)
from hybrid_sensor_sim.tools.scenario_runtime_backend_workflow import (
    SCENARIO_RUNTIME_BACKEND_WORKFLOW_REPORT_SCHEMA_VERSION_V0,
    _build_markdown_report,
    _build_status_summary,
    _build_workflow_status,
)


SCENARIO_RUNTIME_BACKEND_REBRIDGE_REPORT_SCHEMA_VERSION_V0 = (
    "scenario_runtime_backend_rebridge_report_v0"
)
SCENARIO_BACKEND_SMOKE_WORKFLOW_REPORT_SCHEMA_VERSION_V0 = (
    "scenario_backend_smoke_workflow_report_v0"
)
SCENARIO_BATCH_WORKFLOW_REPORT_SCHEMA_VERSION_V0 = "scenario_batch_workflow_report_v0"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild a scenario runtime backend top-level report from existing "
            "backend smoke/runtime workflow artifacts."
        )
    )
    parser.add_argument(
        "--runtime-backend-workflow-report",
        default="",
        help="Existing scenario_runtime_backend_workflow_report_v0.json",
    )
    parser.add_argument(
        "--backend-smoke-workflow-report",
        default="",
        help="Existing scenario_backend_smoke_workflow_report_v0.json",
    )
    parser.add_argument(
        "--batch-workflow-report",
        default="",
        help="Optional scenario_batch_workflow_report_v0.json override",
    )
    parser.add_argument(
        "--supplemental-backend-smoke-workflow-report",
        action="append",
        default=[],
        help="Optional supplemental scenario_backend_smoke_workflow_report_v0.json paths",
    )
    parser.add_argument("--out-root", required=True, help="Output root")
    parser.add_argument(
        "--skip-autoware-bridge",
        action="store_true",
        help="Do not rerun the Autoware bridge; reuse the embedded backend/autoware section as-is",
    )
    parser.add_argument(
        "--autoware-base-frame",
        default="base_link",
        help="Base frame ID for regenerated Autoware artifacts",
    )
    parser.add_argument(
        "--autoware-consumer-profile",
        default="",
        help="Optional Autoware consumer profile override",
    )
    parser.add_argument(
        "--autoware-strict",
        action="store_true",
        help="Fail Autoware bridge if required outputs are missing",
    )
    parser.add_argument(
        "--run-history-guard",
        action="store_true",
        help="Run Autonomy-E2E provenance guard after rebridge",
    )
    parser.add_argument(
        "--history-guard-metadata-root",
        default="",
        help="Override metadata root for provenance guard",
    )
    parser.add_argument(
        "--history-guard-current-repo-root",
        default="",
        help="Override repo root for provenance guard",
    )
    parser.add_argument(
        "--history-guard-compare-ref",
        default="origin/main",
        help="Git compare ref used by provenance guard",
    )
    parser.add_argument(
        "--history-guard-include-untracked",
        action="store_true",
        help="Include untracked files in provenance guard evaluation",
    )
    return parser.parse_args(argv)


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _deep_copy(payload: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(payload))


def _load_runtime_workflow_report(path: Path) -> dict[str, Any]:
    payload = _load_json_object(path)
    schema_version = str(
        payload.get("scenario_runtime_backend_workflow_report_schema_version", "")
    ).strip()
    if schema_version != SCENARIO_RUNTIME_BACKEND_WORKFLOW_REPORT_SCHEMA_VERSION_V0:
        raise ValueError(
            "scenario_runtime_backend_workflow_report_schema_version must be "
            f"{SCENARIO_RUNTIME_BACKEND_WORKFLOW_REPORT_SCHEMA_VERSION_V0}"
        )
    return payload


def _load_backend_smoke_workflow_report(path: Path) -> dict[str, Any]:
    payload = _load_json_object(path)
    schema_version = str(
        payload.get("scenario_backend_smoke_workflow_report_schema_version", "")
    ).strip()
    if schema_version != SCENARIO_BACKEND_SMOKE_WORKFLOW_REPORT_SCHEMA_VERSION_V0:
        raise ValueError(
            "scenario_backend_smoke_workflow_report_schema_version must be "
            f"{SCENARIO_BACKEND_SMOKE_WORKFLOW_REPORT_SCHEMA_VERSION_V0}"
        )
    return payload


def _load_batch_workflow_report(path: Path) -> dict[str, Any]:
    payload = _load_json_object(path)
    schema_version = str(
        payload.get("scenario_batch_workflow_report_schema_version", "")
    ).strip()
    if schema_version != SCENARIO_BATCH_WORKFLOW_REPORT_SCHEMA_VERSION_V0:
        raise ValueError(
            "scenario_batch_workflow_report_schema_version must be "
            f"{SCENARIO_BATCH_WORKFLOW_REPORT_SCHEMA_VERSION_V0}"
        )
    return payload


def _infer_consumer_profile(
    *,
    explicit_consumer_profile: str,
    runtime_report: dict[str, Any] | None,
    backend_report: dict[str, Any],
) -> str:
    explicit = str(explicit_consumer_profile).strip()
    if explicit:
        return explicit
    if isinstance(runtime_report, dict):
        candidate = str(
            runtime_report.get("status_summary", {}).get("autoware_consumer_profile_id", "")
        ).strip()
        if candidate:
            return candidate
    return str(backend_report.get("autoware", {}).get("consumer_profile_id", "")).strip()


def _infer_supplemental_reports(
    *,
    explicit_paths: list[str],
    backend_report: dict[str, Any],
) -> list[str]:
    explicit = [str(path).strip() for path in explicit_paths if str(path).strip()]
    if explicit:
        return explicit
    return [
        str(path).strip()
        for path in list(
            backend_report.get("autoware", {}).get(
                "supplemental_backend_smoke_workflow_report_paths", []
            )
            or []
        )
        if str(path).strip()
    ]


def _ensure_backend_smoke_summary(backend_report: dict[str, Any]) -> dict[str, Any]:
    smoke = dict(backend_report.get("smoke", {}))
    if isinstance(smoke.get("summary"), dict) and smoke["summary"]:
        return backend_report
    summary_path = str(smoke.get("summary_path", "")).strip()
    if not summary_path:
        return backend_report
    smoke_summary = _load_json_object(Path(summary_path).resolve())
    runtime_diagnostics = _extract_backend_runtime_diagnostics(
        smoke_summary,
        repo_root=Path(__file__).resolve().parents[3],
    )
    smoke["summary"] = {
        "backend": smoke_summary.get("backend"),
        "success": smoke_summary.get("success"),
        "run_status": (smoke_summary.get("run") or {}).get("status")
        if isinstance(smoke_summary.get("run"), dict)
        else None,
        "failure_reason": (smoke_summary.get("run") or {}).get("failure_reason")
        if isinstance(smoke_summary.get("run"), dict)
        else None,
        "runner_smoke_status": (smoke_summary.get("runner_smoke") or {}).get("status")
        if isinstance(smoke_summary.get("runner_smoke"), dict)
        else None,
        "output_inspection_status": (
            (smoke_summary.get("output_inspection") or {}).get("status")
            if isinstance(smoke_summary.get("output_inspection"), dict)
            else None
        ),
        "output_smoke_status": (smoke_summary.get("output_smoke_report") or {}).get("status")
        if isinstance(smoke_summary.get("output_smoke_report"), dict)
        else None,
        "output_smoke_coverage_ratio": (
            (smoke_summary.get("output_smoke_report") or {}).get("coverage_ratio")
            if isinstance(smoke_summary.get("output_smoke_report"), dict)
            else None
        ),
        "output_origin_status": (
            (smoke_summary.get("output_smoke_report") or {}).get("output_origin_status")
            if isinstance(smoke_summary.get("output_smoke_report"), dict)
            else None
        ),
        "output_origin_counts": (
            dict((smoke_summary.get("output_smoke_report") or {}).get("output_origin_counts", {}))
            if isinstance(smoke_summary.get("output_smoke_report"), dict)
            else {}
        ),
        "output_origin_reasons": (
            list((smoke_summary.get("output_smoke_report") or {}).get("output_origin_reasons", []))
            if isinstance(smoke_summary.get("output_smoke_report"), dict)
            else []
        ),
        "output_comparison_status": (smoke_summary.get("output_comparison") or {}).get("status")
        if isinstance(smoke_summary.get("output_comparison"), dict)
        else None,
        "output_comparison_mismatch_reasons": (
            list((smoke_summary.get("output_comparison") or {}).get("mismatch_reasons", []))
            if isinstance(smoke_summary.get("output_comparison"), dict)
            else []
        ),
        "output_comparison_unexpected_output_count": (
            (smoke_summary.get("output_comparison") or {}).get("unexpected_output_count")
            if isinstance(smoke_summary.get("output_comparison"), dict)
            else None
        ),
        "output_comparison_origin_status": (
            (smoke_summary.get("output_comparison") or {}).get("output_origin_status")
            if isinstance(smoke_summary.get("output_comparison"), dict)
            else None
        ),
        "output_comparison_origin_counts": (
            dict((smoke_summary.get("output_comparison") or {}).get("output_origin_counts", {}))
            if isinstance(smoke_summary.get("output_comparison"), dict)
            else {}
        ),
        "sidecar_materialization_status": (
            (smoke_summary.get("sidecar_materialization") or {}).get("status")
            if isinstance(smoke_summary.get("sidecar_materialization"), dict)
            else None
        ),
        "sidecar_materialized_output_count": (
            (smoke_summary.get("sidecar_materialization") or {}).get("materialized_output_count")
            if isinstance(smoke_summary.get("sidecar_materialization"), dict)
            else None
        ),
        **runtime_diagnostics,
    }
    updated = _deep_copy(backend_report)
    updated["smoke"] = smoke
    return updated


def _build_backend_autoware_section(
    *,
    backend_report: dict[str, Any],
    autoware_result: dict[str, Any] | None,
    base_frame: str,
    consumer_profile_id: str,
    strict: bool,
) -> dict[str, Any]:
    if not isinstance(autoware_result, dict):
        return dict(backend_report.get("autoware", {}))
    autoware_report = dict(autoware_result.get("report", {}))
    return {
        "requested": True,
        "status": autoware_report.get("status"),
        "availability_mode": autoware_report.get("availability_mode"),
        "strict": bool(strict),
        "base_frame": str(base_frame).strip() or "base_link",
        "consumer_profile_id": consumer_profile_id or autoware_report.get("consumer_profile_id"),
        "consumer_profile_description": autoware_report.get("consumer_profile_description"),
        "available_sensor_count": autoware_report.get("available_sensor_count"),
        "missing_required_sensor_count": autoware_report.get(
            "missing_required_sensor_count"
        ),
        "available_topics": list(autoware_report.get("available_topics", [])),
        "consumer_ready": autoware_report.get("consumer_ready"),
        "topic_export_count": autoware_report.get("topic_export_count"),
        "materialized_topic_export_count": autoware_report.get(
            "materialized_topic_export_count"
        ),
        "required_topic_count": autoware_report.get("required_topic_count"),
        "missing_required_topic_count": autoware_report.get(
            "missing_required_topic_count"
        ),
        "available_message_types": list(
            autoware_report.get("available_message_types", [])
        ),
        "subscription_spec_count": autoware_report.get("subscription_spec_count"),
        "sensor_input_count": autoware_report.get("sensor_input_count"),
        "static_transform_count": autoware_report.get("static_transform_count"),
        "processing_stage_count": autoware_report.get("processing_stage_count"),
        "ready_processing_stage_count": autoware_report.get(
            "ready_processing_stage_count"
        ),
        "degraded_processing_stage_count": autoware_report.get(
            "degraded_processing_stage_count"
        ),
        "available_modalities": list(autoware_report.get("available_modalities", [])),
        "data_roots": list(autoware_report.get("data_roots", [])),
        "recording_style": autoware_report.get("recording_style"),
        "dataset_ready": autoware_report.get("dataset_ready"),
        "scenario_source": dict(autoware_report.get("scenario_source", {})),
        "required_topics_complete": autoware_report.get("required_topics_complete"),
        "frame_tree_complete": autoware_report.get("frame_tree_complete"),
        "warnings": list(autoware_report.get("warnings", [])),
        "topic_export_root": autoware_report.get("artifacts", {}).get("topic_export_root"),
        "topic_export_index_path": autoware_report.get("artifacts", {}).get(
            "topic_export_index_path"
        ),
        "topic_catalog_path": autoware_report.get("artifacts", {}).get(
            "topic_catalog_path"
        ),
        "consumer_input_manifest_path": autoware_report.get("artifacts", {}).get(
            "consumer_input_manifest_path"
        ),
        "merged_report_count": autoware_report.get("merged_report_count"),
        "supplemental_backend_smoke_workflow_report_paths": list(
            autoware_report.get("supplemental_backend_smoke_workflow_report_paths", [])
        ),
        "supplemental_semantic_requested": dict(backend_report.get("autoware", {})).get(
            "supplemental_semantic_requested"
        ),
        "supplemental_semantic_status": dict(backend_report.get("autoware", {})).get(
            "supplemental_semantic_status"
        ),
        "supplemental_semantic_report_path": dict(
            backend_report.get("autoware", {})
        ).get("supplemental_semantic_report_path"),
        "report_path": (
            str(Path(str(autoware_result.get("report_path"))).resolve())
            if autoware_result.get("report_path") is not None
            else None
        ),
    }


def _build_backend_artifacts(
    *,
    backend_report: dict[str, Any],
    autoware_result: dict[str, Any] | None,
) -> dict[str, Any]:
    artifacts = dict(backend_report.get("artifacts", {}))
    if not isinstance(autoware_result, dict):
        return artifacts
    autoware_report = dict(autoware_result.get("report", {}))
    artifacts["autoware_report_path"] = (
        str(Path(str(autoware_result.get("report_path"))).resolve())
        if autoware_result.get("report_path") is not None
        else None
    )
    artifacts["autoware_sensor_contracts_path"] = autoware_report.get("artifacts", {}).get(
        "sensor_contracts_path"
    )
    artifacts["autoware_frame_tree_path"] = autoware_report.get("artifacts", {}).get(
        "frame_tree_path"
    )
    artifacts["autoware_pipeline_manifest_path"] = autoware_report.get("artifacts", {}).get(
        "pipeline_manifest_path"
    )
    artifacts["autoware_dataset_manifest_path"] = autoware_report.get("artifacts", {}).get(
        "dataset_manifest_path"
    )
    artifacts["autoware_consumer_input_manifest_path"] = autoware_report.get(
        "artifacts", {}
    ).get("consumer_input_manifest_path")
    artifacts["autoware_topic_export_root"] = autoware_report.get("artifacts", {}).get(
        "topic_export_root"
    )
    artifacts["autoware_topic_export_index_path"] = autoware_report.get(
        "artifacts", {}
    ).get("topic_export_index_path")
    artifacts["autoware_topic_catalog_path"] = autoware_report.get("artifacts", {}).get(
        "topic_catalog_path"
    )
    return artifacts


def _load_batch_context(
    *,
    runtime_report: dict[str, Any] | None,
    batch_workflow_report_path: str,
) -> tuple[Path | None, dict[str, Any]]:
    batch_path_text = str(batch_workflow_report_path).strip()
    if batch_path_text:
        batch_path = Path(batch_path_text).resolve()
        batch_report = _load_batch_workflow_report(batch_path)
        return batch_path, batch_report
    if isinstance(runtime_report, dict):
        source_batch_path = str(
            runtime_report.get("artifacts", {}).get("batch_workflow_report_path", "")
        ).strip()
        if source_batch_path:
            batch_path = Path(source_batch_path).resolve()
            if batch_path.exists():
                batch_report = _load_batch_workflow_report(batch_path)
                return batch_path, batch_report
        return None, {
            "status": runtime_report.get("batch_workflow", {}).get("status", "SUCCEEDED"),
            "status_summary": dict(
                runtime_report.get("batch_workflow", {}).get("status_summary", {})
            ),
        }
    return None, {"status": "SUCCEEDED", "status_summary": {}}


def _build_rebridge_comparison(
    *,
    source_runtime_report: dict[str, Any] | None,
    refreshed_backend_report: dict[str, Any],
    refreshed_workflow_status: str,
    supplemental_report_paths: list[str],
) -> dict[str, Any]:
    source_status_summary = (
        dict(source_runtime_report.get("status_summary", {}))
        if isinstance(source_runtime_report, dict)
        else {}
    )
    source_status = (
        str(source_runtime_report.get("status", "")).strip()
        if isinstance(source_runtime_report, dict)
        else None
    )
    source_backend_status = (
        str((source_runtime_report.get("backend_smoke_workflow") or {}).get("status", "")).strip()
        if isinstance(source_runtime_report, dict)
        else None
    )
    source_autoware_status = str(
        source_status_summary.get("autoware_pipeline_status", "")
    ).strip() or None
    source_merged_report_count = (
        source_status_summary.get("autoware_merged_report_count")
        if isinstance(source_status_summary.get("autoware_merged_report_count"), int)
        else None
    )

    refreshed_autoware = dict(refreshed_backend_report.get("autoware", {}))
    refreshed_autoware_status = str(refreshed_autoware.get("status", "")).strip() or None
    refreshed_merged_report_count = refreshed_autoware.get("merged_report_count")
    refreshed_backend_status = str(refreshed_backend_report.get("status", "")).strip() or None

    normalized_source_merged = source_merged_report_count or 0
    normalized_refreshed_merged = (
        int(refreshed_merged_report_count)
        if isinstance(refreshed_merged_report_count, int)
        else 0
    )

    return {
        "source_runtime_status": source_status,
        "source_backend_smoke_status": source_backend_status,
        "source_autoware_pipeline_status": source_autoware_status,
        "source_autoware_merged_report_count": source_merged_report_count,
        "refreshed_runtime_status": refreshed_workflow_status,
        "refreshed_backend_smoke_status": refreshed_backend_status,
        "refreshed_autoware_pipeline_status": refreshed_autoware_status,
        "refreshed_autoware_merged_report_count": refreshed_merged_report_count,
        "status_changed": source_status != refreshed_workflow_status,
        "autoware_status_changed": source_autoware_status != refreshed_autoware_status,
        "merged_report_count_changed": normalized_source_merged != normalized_refreshed_merged,
        "supplemental_backend_smoke_workflow_report_count": len(supplemental_report_paths),
    }


def run_scenario_runtime_backend_rebridge(
    *,
    runtime_backend_workflow_report_path: str,
    backend_smoke_workflow_report_path: str,
    batch_workflow_report_path: str,
    supplemental_backend_smoke_workflow_report_paths: list[str] | None,
    out_root: Path,
    skip_autoware_bridge: bool = False,
    autoware_base_frame: str = "base_link",
    autoware_consumer_profile: str = "",
    autoware_strict: bool = False,
    run_history_guard: bool = False,
    history_guard_metadata_root: str | Path | None = None,
    history_guard_current_repo_root: str | Path | None = None,
    history_guard_compare_ref: str = "origin/main",
    history_guard_include_untracked: bool = False,
) -> dict[str, Any]:
    runtime_report_path_text = str(runtime_backend_workflow_report_path).strip()
    backend_report_path_text = str(backend_smoke_workflow_report_path).strip()
    if not runtime_report_path_text and not backend_report_path_text:
        raise ValueError(
            "provide at least one of --runtime-backend-workflow-report or "
            "--backend-smoke-workflow-report"
        )

    out_root = out_root.resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    source_runtime_report = None
    source_runtime_report_path = None
    if runtime_report_path_text:
        source_runtime_report_path = Path(runtime_report_path_text).resolve()
        source_runtime_report = _load_runtime_workflow_report(source_runtime_report_path)
        if not backend_report_path_text:
            backend_report_path_text = str(
                source_runtime_report.get("artifacts", {}).get(
                    "backend_smoke_workflow_report_path", ""
                )
            ).strip()
        if not batch_workflow_report_path:
            batch_workflow_report_path = str(
                source_runtime_report.get("artifacts", {}).get(
                    "batch_workflow_report_path", ""
                )
            ).strip()
    if not backend_report_path_text:
        raise ValueError("unable to resolve backend smoke workflow report path")

    source_backend_report_path = Path(backend_report_path_text).resolve()
    backend_report = _ensure_backend_smoke_summary(
        _load_backend_smoke_workflow_report(source_backend_report_path)
    )
    source_batch_report_path, batch_report = _load_batch_context(
        runtime_report=source_runtime_report,
        batch_workflow_report_path=batch_workflow_report_path,
    )

    consumer_profile_id = _infer_consumer_profile(
        explicit_consumer_profile=autoware_consumer_profile,
        runtime_report=source_runtime_report,
        backend_report=backend_report,
    )
    supplemental_report_paths = _infer_supplemental_reports(
        explicit_paths=list(supplemental_backend_smoke_workflow_report_paths or []),
        backend_report=backend_report,
    )

    autoware_result = None
    refreshed_backend_report = _deep_copy(backend_report)
    if not skip_autoware_bridge:
        autoware_result = run_autoware_pipeline_bridge(
            backend_smoke_workflow_report_path=str(source_backend_report_path),
            supplemental_backend_smoke_workflow_report_paths=supplemental_report_paths,
            runtime_backend_workflow_report_path="",
            out_root=out_root / "autoware",
            base_frame=autoware_base_frame,
            consumer_profile_id=consumer_profile_id,
            strict=bool(autoware_strict),
        )
        refreshed_backend_report["autoware"] = _build_backend_autoware_section(
            backend_report=backend_report,
            autoware_result=autoware_result,
            base_frame=autoware_base_frame,
            consumer_profile_id=consumer_profile_id,
            strict=bool(autoware_strict),
        )
        refreshed_backend_report["artifacts"] = _build_backend_artifacts(
            backend_report=backend_report,
            autoware_result=autoware_result,
        )

    history_guard_report = None
    history_guard_report_path = None
    if run_history_guard:
        guard_root = out_root / "history_guard"
        guard_root.mkdir(parents=True, exist_ok=True)
        history_guard_report_path = (
            guard_root / "autonomy_e2e_history_guard_report_v0.json"
        )
        metadata_root = (
            Path(history_guard_metadata_root).resolve()
            if history_guard_metadata_root
            else Path(__file__).resolve().parents[3] / "metadata" / "autonomy_e2e"
        )
        current_repo_root = (
            Path(history_guard_current_repo_root).resolve()
            if history_guard_current_repo_root
            else Path(__file__).resolve().parents[3]
        )
        history_guard_report = build_autonomy_e2e_history_guard_report(
            current_repo_root=current_repo_root,
            metadata_root=metadata_root,
            compare_ref=history_guard_compare_ref,
            include_untracked=history_guard_include_untracked,
            json_out=history_guard_report_path,
        )

    workflow_status = _build_workflow_status(
        batch_status=str(batch_report.get("status", "SUCCEEDED")),
        backend_status=str(refreshed_backend_report.get("status", "")),
        backend_report=refreshed_backend_report,
        history_guard_status=(
            str(history_guard_report.get("status", "")).strip()
            if isinstance(history_guard_report, dict)
            else None
        ),
    )
    rebridge_comparison = _build_rebridge_comparison(
        source_runtime_report=source_runtime_report,
        refreshed_backend_report=refreshed_backend_report,
        refreshed_workflow_status=workflow_status,
        supplemental_report_paths=supplemental_report_paths,
    )

    batch_workflow_section = (
        dict(source_runtime_report.get("batch_workflow", {}))
        if isinstance(source_runtime_report, dict)
        else {}
    )
    batch_workflow_section.update(
        {
            "status": batch_report.get("status", "SUCCEEDED"),
            "workflow_report_path": (
                str(source_batch_report_path.resolve())
                if source_batch_report_path is not None
                else None
            ),
        }
    )
    batch_workflow_section.setdefault("status_summary", dict(batch_report.get("status_summary", {})))

    workflow_report = {
        "scenario_runtime_backend_rebridge_report_schema_version": SCENARIO_RUNTIME_BACKEND_REBRIDGE_REPORT_SCHEMA_VERSION_V0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "out_root": str(out_root),
        "backend": refreshed_backend_report.get("backend"),
        "status": workflow_status,
        "rebridge": {
            "requested": True,
            "source_runtime_backend_workflow_report_path": (
                str(source_runtime_report_path) if source_runtime_report_path is not None else None
            ),
            "source_backend_smoke_workflow_report_path": str(source_backend_report_path),
            "source_batch_workflow_report_path": (
                str(source_batch_report_path) if source_batch_report_path is not None else None
            ),
            "skip_autoware_bridge": bool(skip_autoware_bridge),
            "supplemental_backend_smoke_workflow_report_paths": [
                str(Path(path).resolve()) for path in supplemental_report_paths
            ],
            "comparison": rebridge_comparison,
        },
        "batch_workflow": batch_workflow_section,
        "backend_smoke_workflow": {
            "status": refreshed_backend_report.get("status"),
            "workflow_report_path": str(source_backend_report_path),
            "selection": dict(refreshed_backend_report.get("selection", {})),
            "runtime_selection": dict(refreshed_backend_report.get("runtime_selection", {})),
            "bridge": dict(refreshed_backend_report.get("bridge", {})),
            "smoke": dict(refreshed_backend_report.get("smoke", {})),
            "renderer_backend_workflow": dict(
                refreshed_backend_report.get("renderer_backend_workflow", {})
            ),
            "autoware": dict(refreshed_backend_report.get("autoware", {})),
        },
        "history_guard": {
            "requested": bool(run_history_guard),
            "status": (
                history_guard_report.get("status")
                if isinstance(history_guard_report, dict)
                else None
            ),
            "failure_codes": (
                list(history_guard_report.get("failure_codes", []))
                if isinstance(history_guard_report, dict)
                else []
            ),
            "warnings": (
                list(history_guard_report.get("warnings", []))
                if isinstance(history_guard_report, dict)
                else []
            ),
            "compare_ref": history_guard_compare_ref if run_history_guard else None,
            "report_path": (
                str(history_guard_report_path.resolve())
                if history_guard_report_path is not None
                else None
            ),
        },
        "artifacts": {
            "source_runtime_backend_workflow_report_path": (
                str(source_runtime_report_path) if source_runtime_report_path is not None else None
            ),
            "batch_workflow_report_path": (
                str(source_batch_report_path.resolve())
                if source_batch_report_path is not None
                else None
            ),
            "backend_smoke_workflow_report_path": str(source_backend_report_path),
            "smoke_scenario_path": refreshed_backend_report.get("artifacts", {}).get(
                "smoke_scenario_path"
            ),
            "smoke_input_config_path": refreshed_backend_report.get("artifacts", {}).get(
                "smoke_input_config_path"
            ),
            "autoware_report_path": refreshed_backend_report.get("artifacts", {}).get(
                "autoware_report_path"
            ),
            "autoware_sensor_contracts_path": refreshed_backend_report.get(
                "artifacts", {}
            ).get("autoware_sensor_contracts_path"),
            "autoware_frame_tree_path": refreshed_backend_report.get("artifacts", {}).get(
                "autoware_frame_tree_path"
            ),
            "autoware_pipeline_manifest_path": refreshed_backend_report.get(
                "artifacts", {}
            ).get("autoware_pipeline_manifest_path"),
            "autoware_dataset_manifest_path": refreshed_backend_report.get(
                "artifacts", {}
            ).get("autoware_dataset_manifest_path"),
            "autoware_consumer_input_manifest_path": refreshed_backend_report.get(
                "artifacts", {}
            ).get("autoware_consumer_input_manifest_path"),
            "autoware_topic_export_root": refreshed_backend_report.get("artifacts", {}).get(
                "autoware_topic_export_root"
            ),
            "autoware_topic_export_index_path": refreshed_backend_report.get(
                "artifacts", {}
            ).get("autoware_topic_export_index_path"),
            "autoware_topic_catalog_path": refreshed_backend_report.get("artifacts", {}).get(
                "autoware_topic_catalog_path"
            ),
            "supplemental_semantic_smoke_config_path": refreshed_backend_report.get(
                "artifacts", {}
            ).get("supplemental_semantic_smoke_config_path"),
            "supplemental_semantic_backend_smoke_workflow_report_path": refreshed_backend_report.get(
                "artifacts", {}
            ).get("supplemental_semantic_backend_smoke_workflow_report_path"),
            "renderer_backend_workflow_summary_path": refreshed_backend_report.get(
                "artifacts", {}
            ).get("renderer_backend_workflow_summary_path"),
            "renderer_backend_workflow_report_path": refreshed_backend_report.get(
                "artifacts", {}
            ).get("renderer_backend_workflow_report_path"),
            "renderer_backend_linux_handoff_script_path": refreshed_backend_report.get(
                "artifacts", {}
            ).get("renderer_backend_linux_handoff_script_path"),
            "renderer_backend_linux_handoff_bundle_manifest_path": refreshed_backend_report.get(
                "artifacts", {}
            ).get("renderer_backend_linux_handoff_bundle_manifest_path"),
            "history_guard_report_path": (
                str(history_guard_report_path.resolve())
                if history_guard_report_path is not None
                else None
            ),
            "workflow_markdown_path": str(
                (out_root / "scenario_runtime_backend_rebridge_report_v0.md").resolve()
            ),
        },
    }
    workflow_report["status_summary"] = _build_status_summary(
        workflow_status=workflow_status,
        batch_report=batch_report,
        backend_report=refreshed_backend_report,
        history_guard_report=history_guard_report,
    )

    report_path = out_root / "scenario_runtime_backend_rebridge_report_v0.json"
    markdown_path = out_root / "scenario_runtime_backend_rebridge_report_v0.md"
    _write_json(report_path, workflow_report)
    markdown_path.write_text(_build_markdown_report(workflow_report), encoding="utf-8")
    return {
        "workflow_report_path": report_path,
        "workflow_markdown_path": markdown_path,
        "workflow_report": workflow_report,
        "autoware_result": autoware_result,
    }


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        result = run_scenario_runtime_backend_rebridge(
            runtime_backend_workflow_report_path=args.runtime_backend_workflow_report,
            backend_smoke_workflow_report_path=args.backend_smoke_workflow_report,
            batch_workflow_report_path=args.batch_workflow_report,
            supplemental_backend_smoke_workflow_report_paths=list(
                args.supplemental_backend_smoke_workflow_report
            ),
            out_root=Path(args.out_root).resolve(),
            skip_autoware_bridge=bool(args.skip_autoware_bridge),
            autoware_base_frame=args.autoware_base_frame,
            autoware_consumer_profile=args.autoware_consumer_profile,
            autoware_strict=bool(args.autoware_strict),
            run_history_guard=bool(args.run_history_guard),
            history_guard_metadata_root=args.history_guard_metadata_root,
            history_guard_current_repo_root=args.history_guard_current_repo_root,
            history_guard_compare_ref=args.history_guard_compare_ref,
            history_guard_include_untracked=bool(
                args.history_guard_include_untracked
            ),
        )
        workflow_report = result["workflow_report"]
        print(f"[ok] status={workflow_report['status']}")
        print(f"[ok] backend_smoke_status={workflow_report['backend_smoke_workflow']['status']}")
        print(f"[ok] report={result['workflow_report_path']}")
        return (
            0
            if workflow_report["status"]
            in {
                "SUCCEEDED",
                "DEGRADED",
                "ATTENTION",
                "BRIDGED_ONLY",
                "HANDOFF_READY",
                "HANDOFF_DOCKER_VERIFIED",
                "HANDOFF_DOCKER_EXECUTED",
            }
            else 2
        )
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] scenario_runtime_backend_rebridge.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
