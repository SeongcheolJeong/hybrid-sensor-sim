from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.scenarios import SCENARIO_VARIANTS_REPORT_SCHEMA_VERSION_V0
from hybrid_sensor_sim.scenarios.schema import ScenarioValidationError
from hybrid_sensor_sim.tools.log_replay_runner import run_log_replay


SCENARIO_VARIANT_RUN_REPORT_SCHEMA_VERSION_V0 = "scenario_variant_run_report_v0"
SUPPORTED_RENDERED_PAYLOAD_KIND_LOG_SCENE_V0 = "log_scene_v0"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute rendered payloads from a scenario_variants_report_v0 artifact."
    )
    parser.add_argument("--variants-report", required=True, help="Path to scenario variants report JSON")
    parser.add_argument("--out", required=True, help="Output root directory for variant runs")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic seed for replay/object sim")
    parser.add_argument("--max-variants", type=int, default=0, help="Maximum variants to execute; 0 means all")
    parser.add_argument("--sds-version", default="sds_unknown", help="SDS version identifier")
    parser.add_argument("--sim-version", default="sim_engine_v0_prototype", help="Simulation version identifier")
    parser.add_argument("--fidelity-profile", default="dev-fast", help="Fidelity profile")
    parser.add_argument("--out-report", default="", help="Optional explicit output path for run report JSON")
    return parser.parse_args(argv)


def _load_variants_report(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("scenario variants report must be a JSON object")
    schema_version = str(payload.get("scenario_variants_report_schema_version", "")).strip()
    if schema_version != SCENARIO_VARIANTS_REPORT_SCHEMA_VERSION_V0:
        raise ValueError(
            "scenario_variants_report_schema_version must be "
            f"{SCENARIO_VARIANTS_REPORT_SCHEMA_VERSION_V0}"
        )
    variants = payload.get("variants")
    if not isinstance(variants, list):
        raise ValueError("scenario variants report missing variants list")
    return payload


def _select_variants(variants: list[dict[str, Any]], *, max_variants: int) -> list[dict[str, Any]]:
    if max_variants < 0:
        raise ValueError("max_variants must be non-negative")
    if max_variants == 0:
        return variants
    return variants[:max_variants]


def _write_rendered_payload(run_dir: Path, rendered_payload: dict[str, Any]) -> Path:
    payload_path = run_dir / "variant_rendered_payload.json"
    payload_path.write_text(json.dumps(rendered_payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return payload_path


def _resolve_rendered_payload_paths(
    *,
    rendered_payload: dict[str, Any],
    rendered_payload_kind: str,
    logical_source_path: Path | None,
    variants_report_path: Path,
) -> dict[str, Any]:
    resolved_payload = dict(rendered_payload)
    if rendered_payload_kind != SUPPORTED_RENDERED_PAYLOAD_KIND_LOG_SCENE_V0:
        return resolved_payload

    canonical_map_path = resolved_payload.get("canonical_map_path")
    if not isinstance(canonical_map_path, str) or not canonical_map_path.strip():
        return resolved_payload
    map_path = Path(canonical_map_path)
    if map_path.is_absolute():
        return resolved_payload

    base_path = logical_source_path if logical_source_path is not None else variants_report_path
    resolved_payload["canonical_map_path"] = str((base_path.parent / map_path).resolve())
    return resolved_payload


def _build_variant_entry_base(variant: dict[str, Any], run_dir: Path) -> dict[str, Any]:
    return {
        "variant_id": str(variant.get("scenario_id", "")).strip(),
        "logical_scenario_id": str(variant.get("logical_scenario_id", "")).strip() or None,
        "rendered_payload_kind": str(variant.get("rendered_payload_kind", "")).strip() or None,
        "variant_run_dir": str(run_dir.resolve()),
        "execution_status": None,
        "failure_code": None,
        "failure_reason": None,
        "object_sim_status": None,
        "termination_reason": None,
        "rendered_payload_path": None,
        "replay_scenario_path": None,
        "summary_path": None,
        "trace_path": None,
        "lane_risk_summary_path": None,
        "manifest_path": None,
    }


def run_scenario_variant_report(
    *,
    variants_report_path: Path,
    out_root: Path,
    seed: int,
    max_variants: int,
    sds_version: str,
    sim_version: str,
    fidelity_profile: str,
) -> dict[str, Any]:
    variants_report = _load_variants_report(variants_report_path)
    logical_source_path_value = str(variants_report.get("source_path", "")).strip()
    logical_source_path = Path(logical_source_path_value).resolve() if logical_source_path_value else None
    all_variants = list(variants_report["variants"])
    selected_variants = _select_variants(all_variants, max_variants=max_variants)
    out_root.mkdir(parents=True, exist_ok=True)

    execution_status_counts: Counter[str] = Counter()
    object_sim_status_counts: Counter[str] = Counter()
    variant_runs: list[dict[str, Any]] = []

    for index, raw_variant in enumerate(selected_variants, start=1):
        if not isinstance(raw_variant, dict):
            raise ValueError(f"variants[{index - 1}] must be a JSON object")
        variant = dict(raw_variant)
        variant_id = str(variant.get("scenario_id", "")).strip() or f"variant_{index:04d}"
        run_dir = out_root / variant_id
        run_dir.mkdir(parents=True, exist_ok=True)
        entry = _build_variant_entry_base(variant, run_dir)
        entry["variant_id"] = variant_id

        rendered_payload = variant.get("rendered_payload")
        if not isinstance(rendered_payload, dict):
            entry["execution_status"] = "SKIPPED"
            entry["failure_code"] = "MISSING_RENDERED_PAYLOAD"
            entry["failure_reason"] = "variant does not contain a rendered_payload object"
            execution_status_counts[str(entry["execution_status"])] += 1
            variant_runs.append(entry)
            continue

        rendered_payload_kind = str(variant.get("rendered_payload_kind", "")).strip()
        resolved_payload = _resolve_rendered_payload_paths(
            rendered_payload=rendered_payload,
            rendered_payload_kind=rendered_payload_kind,
            logical_source_path=logical_source_path,
            variants_report_path=variants_report_path,
        )
        rendered_payload_path = _write_rendered_payload(run_dir, resolved_payload)
        entry["rendered_payload_path"] = str(rendered_payload_path.resolve())
        if rendered_payload_kind != SUPPORTED_RENDERED_PAYLOAD_KIND_LOG_SCENE_V0:
            entry["execution_status"] = "FAILED"
            entry["failure_code"] = "UNSUPPORTED_RENDERED_PAYLOAD_KIND"
            entry["failure_reason"] = (
                "unsupported rendered_payload_kind: "
                f"{rendered_payload_kind or '<missing>'}"
            )
            execution_status_counts[str(entry["execution_status"])] += 1
            variant_runs.append(entry)
            continue

        try:
            replay_result = run_log_replay(
                log_scene_path=rendered_payload_path,
                run_id=variant_id,
                out_root=out_root,
                seed=seed,
                sds_version=sds_version,
                sim_version=sim_version,
                fidelity_profile=fidelity_profile,
            )
            summary = dict(replay_result["summary"])
            entry["execution_status"] = "SUCCEEDED"
            entry["object_sim_status"] = str(summary["status"])
            entry["termination_reason"] = str(summary["termination_reason"])
            entry["replay_scenario_path"] = str(Path(replay_result["scenario_path"]).resolve())
            entry["summary_path"] = str(Path(replay_result["summary_path"]).resolve())
            entry["trace_path"] = str(Path(replay_result["trace_path"]).resolve())
            entry["lane_risk_summary_path"] = str(Path(replay_result["lane_risk_summary_path"]).resolve())
            entry["manifest_path"] = str(Path(replay_result["manifest_path"]).resolve())
            object_sim_status_counts[str(entry["object_sim_status"])] += 1
        except (FileNotFoundError, json.JSONDecodeError, ScenarioValidationError, ValueError) as exc:
            entry["execution_status"] = "FAILED"
            entry["failure_code"] = "EXECUTION_ERROR"
            entry["failure_reason"] = str(exc)

        execution_status_counts[str(entry["execution_status"])] += 1
        variant_runs.append(entry)

    return {
        "scenario_variant_run_report_schema_version": SCENARIO_VARIANT_RUN_REPORT_SCHEMA_VERSION_V0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "variants_report_path": str(variants_report_path.resolve()),
        "logical_source_path": str(variants_report.get("source_path", "")) or None,
        "logical_source_kind": str(variants_report.get("source_kind", "")) or None,
        "out_root": str(out_root.resolve()),
        "seed": int(seed),
        "requested_max_variants": int(max_variants),
        "selected_variant_count": len(selected_variants),
        "source_variant_count": len(all_variants),
        "sds_version": sds_version,
        "sim_version": sim_version,
        "fidelity_profile": fidelity_profile,
        "execution_status_counts": dict(sorted(execution_status_counts.items())),
        "object_sim_status_counts": dict(sorted(object_sim_status_counts.items())),
        "variant_runs": variant_runs,
    }


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        variants_report_path = Path(args.variants_report).resolve()
        out_root = Path(args.out).resolve()
        out_root.mkdir(parents=True, exist_ok=True)
        report = run_scenario_variant_report(
            variants_report_path=variants_report_path,
            out_root=out_root,
            seed=int(args.seed),
            max_variants=int(args.max_variants),
            sds_version=args.sds_version,
            sim_version=args.sim_version,
            fidelity_profile=args.fidelity_profile,
        )
        out_report = (
            Path(args.out_report).resolve()
            if str(args.out_report).strip()
            else out_root / "scenario_variant_run_report_v0.json"
        )
        out_report.parent.mkdir(parents=True, exist_ok=True)
        out_report.write_text(json.dumps(report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        print(f"[ok] selected_variant_count={report['selected_variant_count']}")
        print(f"[ok] execution_status_counts={report['execution_status_counts']}")
        print(f"[ok] report={out_report}")
        if report["execution_status_counts"].get("FAILED", 0) > 0:
            return 2
        return 0
    except (FileNotFoundError, json.JSONDecodeError, ScenarioValidationError, ValueError) as exc:
        print(f"[error] scenario_variant_runner.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
