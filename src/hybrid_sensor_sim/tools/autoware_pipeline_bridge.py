from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.autoware.export_bridge import write_autoware_export_bundle


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an Autoware-facing sensor/data contract bundle from backend smoke workflow reports."
    )
    parser.add_argument("--backend-smoke-workflow-report", default="", help="Path to scenario_backend_smoke_workflow_report_v0.json")
    parser.add_argument("--runtime-backend-workflow-report", default="", help="Path to scenario_runtime_backend_workflow_report_v0.json")
    parser.add_argument("--out-root", required=True, help="Output root for the Autoware export bundle")
    parser.add_argument("--base-frame", default="base_link", help="Base frame ID for generated frame tree")
    parser.add_argument("--strict", action="store_true", help="Fail if required sensor outputs are missing")
    return parser.parse_args(argv)


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
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
    return _load_json_object(Path(summary_path))


def _load_artifact(path_text: str, *, field: str) -> dict[str, Any]:
    text = str(path_text).strip()
    if not text:
        raise ValueError(f"missing required artifact path: {field}")
    return _load_json_object(Path(text))


def run_autoware_pipeline_bridge(
    *,
    backend_smoke_workflow_report_path: str,
    runtime_backend_workflow_report_path: str,
    out_root: Path,
    base_frame: str = "base_link",
    strict: bool = False,
) -> dict[str, Any]:
    report_path_text = str(backend_smoke_workflow_report_path).strip() or str(runtime_backend_workflow_report_path).strip()
    if not report_path_text:
        raise ValueError("provide exactly one workflow report path")
    if bool(str(backend_smoke_workflow_report_path).strip()) == bool(str(runtime_backend_workflow_report_path).strip()):
        raise ValueError("provide exactly one of backend or runtime workflow report path")
    input_report = _load_report(Path(report_path_text))
    backend_report = _resolve_backend_smoke_report(input_report)
    smoke_summary = _load_smoke_summary(backend_report)
    smoke_artifacts = dict(smoke_summary.get("artifacts", {}))
    playback_contract = _load_artifact(
        smoke_artifacts.get("renderer_playback_contract", ""),
        field="renderer_playback_contract",
    )
    backend_output_spec = _load_artifact(
        smoke_artifacts.get("backend_output_spec", ""),
        field="backend_output_spec",
    )
    backend_sensor_output_summary = _load_artifact(
        smoke_artifacts.get("backend_sensor_output_summary", ""),
        field="backend_sensor_output_summary",
    )
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
    )
    report = {
        "backend": str(backend_report.get("backend", "")).strip() or None,
        "run_id": run_id,
        "status": bundle["status"],
        "strict": bool(strict),
        "available_sensor_count": bundle["available_sensor_count"],
        "missing_required_sensor_count": bundle["missing_required_sensor_count"],
        "available_topics": bundle["available_topics"],
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
            runtime_backend_workflow_report_path=args.runtime_backend_workflow_report,
            out_root=Path(args.out_root).resolve(),
            base_frame=args.base_frame,
            strict=bool(args.strict),
        )
        print(f"[ok] autoware_status={result['report']['status']}")
        print(f"[ok] report={result['report_path']}")
        return 0 if result["report"]["status"] in {"READY", "DEGRADED"} else 2
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        print(f"[error] run_autoware_pipeline_bridge.py: {exc}", file=sys.stderr)
        return 2


__all__ = [
    "run_autoware_pipeline_bridge",
    "main",
]
