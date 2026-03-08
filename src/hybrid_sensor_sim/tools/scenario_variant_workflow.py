from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.scenarios.variants import build_scenario_variants_report, load_logical_scenarios_source
from hybrid_sensor_sim.tools.scenario_variant_runner import run_scenario_variant_report


SCENARIO_VARIANT_WORKFLOW_REPORT_SCHEMA_VERSION_V0 = "scenario_variant_workflow_report_v0"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate scenario variants and execute rendered payloads in one workflow."
    )
    parser.add_argument("--logical-scenarios", default="", help="Path to logical scenario JSON file")
    parser.add_argument(
        "--scenario-language-profile",
        default="",
        help="Scenario language profile ID under scenario language directory (without .json)",
    )
    parser.add_argument(
        "--scenario-language-dir",
        default="",
        help="Scenario language profile directory",
    )
    parser.add_argument("--out-root", required=True, help="Workflow output root")
    parser.add_argument("--sampling", choices=["full", "random"], default="full")
    parser.add_argument("--sample-size", type=int, default=0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-variants-per-scenario", type=int, default=1000)
    parser.add_argument("--execution-max-variants", type=int, default=0)
    parser.add_argument("--sds-version", default="sds_unknown", help="SDS version identifier")
    parser.add_argument("--sim-version", default="sim_engine_v0_prototype", help="Simulation version identifier")
    parser.add_argument("--fidelity-profile", default="dev-fast", help="Fidelity profile")
    return parser.parse_args(argv)


def _build_non_success_variant_rows(variant_runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for run in variant_runs:
        execution_status = str(run.get("execution_status", "")).strip()
        if execution_status == "SUCCEEDED":
            continue
        rows.append(
            {
                "variant_id": str(run.get("variant_id", "")).strip() or None,
                "logical_scenario_id": str(run.get("logical_scenario_id", "")).strip() or None,
                "rendered_payload_kind": str(run.get("rendered_payload_kind", "")).strip() or None,
                "execution_status": execution_status or None,
                "failure_code": str(run.get("failure_code", "")).strip() or None,
                "failure_reason": str(run.get("failure_reason", "")).strip() or None,
                "execution_path": str(run.get("execution_path", "")).strip() or None,
            }
        )
    return rows


def _build_success_variant_rows(variant_runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for run in variant_runs:
        execution_status = str(run.get("execution_status", "")).strip()
        if execution_status != "SUCCEEDED":
            continue
        rows.append(
            {
                "variant_id": str(run.get("variant_id", "")).strip() or None,
                "logical_scenario_id": str(run.get("logical_scenario_id", "")).strip() or None,
                "rendered_payload_kind": str(run.get("rendered_payload_kind", "")).strip() or None,
                "execution_status": execution_status,
                "object_sim_status": str(run.get("object_sim_status", "")).strip() or None,
                "termination_reason": str(run.get("termination_reason", "")).strip() or None,
                "execution_path": str(run.get("execution_path", "")).strip() or None,
                "summary_path": str(run.get("summary_path", "")).strip() or None,
            }
        )
    return rows


def _build_by_logical_scenario_id_summary(variant_runs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for run in variant_runs:
        logical_scenario_id = str(run.get("logical_scenario_id", "")).strip() or "<missing>"
        group = grouped.setdefault(
            logical_scenario_id,
            {
                "variant_count": 0,
                "execution_status_counts": Counter(),
                "object_sim_status_counts": Counter(),
                "execution_path_counts": Counter(),
                "payload_kind_counts": Counter(),
                "variant_ids": [],
            },
        )
        group["variant_count"] += 1
        group["variant_ids"].append(str(run.get("variant_id", "")).strip())
        payload_kind = str(run.get("rendered_payload_kind", "")).strip() or "<missing>"
        group["payload_kind_counts"][payload_kind] += 1
        execution_status = str(run.get("execution_status", "")).strip()
        if execution_status:
            group["execution_status_counts"][execution_status] += 1
        object_sim_status = str(run.get("object_sim_status", "")).strip()
        if object_sim_status:
            group["object_sim_status_counts"][object_sim_status] += 1
        execution_path = str(run.get("execution_path", "")).strip()
        if execution_path:
            group["execution_path_counts"][execution_path] += 1

    return {
        logical_scenario_id: {
            "variant_count": int(group["variant_count"]),
            "execution_status_counts": dict(sorted(group["execution_status_counts"].items())),
            "object_sim_status_counts": dict(sorted(group["object_sim_status_counts"].items())),
            "execution_path_counts": dict(sorted(group["execution_path_counts"].items())),
            "payload_kind_counts": dict(sorted(group["payload_kind_counts"].items())),
            "variant_ids": list(group["variant_ids"]),
        }
        for logical_scenario_id, group in sorted(grouped.items())
    }


def run_scenario_variant_workflow(
    *,
    logical_scenarios_path: str,
    scenario_language_profile: str,
    scenario_language_dir: str | Path | None,
    out_root: Path,
    sampling: str,
    sample_size: int,
    seed: int,
    max_variants_per_scenario: int,
    execution_max_variants: int,
    sds_version: str,
    sim_version: str,
    fidelity_profile: str,
) -> dict[str, Any]:
    out_root.mkdir(parents=True, exist_ok=True)
    payload, source_path, source_kind = load_logical_scenarios_source(
        logical_scenarios_path=logical_scenarios_path,
        scenario_language_profile=scenario_language_profile,
        scenario_language_dir=scenario_language_dir,
    )
    variants_report = build_scenario_variants_report(
        payload=payload,
        source_path=source_path,
        source_kind=source_kind,
        sampling=sampling,
        sample_size=sample_size,
        max_variants_per_scenario=max_variants_per_scenario,
        seed=seed,
    )
    variants_report_path = out_root / "scenario_variants_report_v0.json"
    variants_report_path.write_text(
        json.dumps(variants_report, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )

    variant_runs_root = out_root / "variant_runs"
    run_report = run_scenario_variant_report(
        variants_report_path=variants_report_path,
        out_root=variant_runs_root,
        seed=seed,
        max_variants=execution_max_variants,
        sds_version=sds_version,
        sim_version=sim_version,
        fidelity_profile=fidelity_profile,
    )
    run_report_path = variant_runs_root / "scenario_variant_run_report_v0.json"
    run_report_path.write_text(
        json.dumps(run_report, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )

    workflow_report = {
        "scenario_variant_workflow_report_schema_version": SCENARIO_VARIANT_WORKFLOW_REPORT_SCHEMA_VERSION_V0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "out_root": str(out_root.resolve()),
        "source_path": str(source_path.resolve()),
        "source_kind": source_kind,
        "sampling": sampling,
        "sample_size": int(sample_size),
        "seed": int(seed),
        "max_variants_per_scenario": int(max_variants_per_scenario),
        "execution_max_variants": int(execution_max_variants),
        "sds_version": sds_version,
        "sim_version": sim_version,
        "fidelity_profile": fidelity_profile,
        "artifacts": {
            "variants_report_path": str(variants_report_path.resolve()),
            "variant_run_report_path": str(run_report_path.resolve()),
            "variant_runs_root": str(variant_runs_root.resolve()),
        },
        "variant_count": int(variants_report["variant_count"]),
        "selected_variant_count": int(run_report["selected_variant_count"]),
        "execution_status_counts": dict(run_report["execution_status_counts"]),
        "object_sim_status_counts": dict(run_report["object_sim_status_counts"]),
        "by_payload_kind": dict(run_report["by_payload_kind"]),
        "by_logical_scenario_id": _build_by_logical_scenario_id_summary(run_report["variant_runs"]),
        "successful_variant_rows": _build_success_variant_rows(run_report["variant_runs"]),
        "non_success_variant_rows": _build_non_success_variant_rows(run_report["variant_runs"]),
    }
    workflow_report["successful_variant_row_count"] = len(workflow_report["successful_variant_rows"])
    workflow_report["non_success_variant_row_count"] = len(workflow_report["non_success_variant_rows"])
    workflow_report_path = out_root / "scenario_variant_workflow_report_v0.json"
    workflow_report_path.write_text(
        json.dumps(workflow_report, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    return {
        "variants_report_path": variants_report_path,
        "variant_run_report_path": run_report_path,
        "workflow_report_path": workflow_report_path,
        "variants_report": variants_report,
        "run_report": run_report,
        "workflow_report": workflow_report,
    }


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        scenario_language_dir = args.scenario_language_dir
        if not scenario_language_dir:
            scenario_language_dir = str(
                Path(__file__).resolve().parents[3]
                / "tests"
                / "fixtures"
                / "autonomy_e2e"
                / "p_validation"
            )
        result = run_scenario_variant_workflow(
            logical_scenarios_path=args.logical_scenarios,
            scenario_language_profile=args.scenario_language_profile,
            scenario_language_dir=scenario_language_dir,
            out_root=Path(args.out_root).resolve(),
            sampling=args.sampling,
            sample_size=int(args.sample_size),
            seed=int(args.seed),
            max_variants_per_scenario=int(args.max_variants_per_scenario),
            execution_max_variants=int(args.execution_max_variants),
            sds_version=args.sds_version,
            sim_version=args.sim_version,
            fidelity_profile=args.fidelity_profile,
        )
        workflow_report = result["workflow_report"]
        print(f"[ok] variant_count={workflow_report['variant_count']}")
        print(f"[ok] selected_variant_count={workflow_report['selected_variant_count']}")
        print(f"[ok] execution_status_counts={workflow_report['execution_status_counts']}")
        print(f"[ok] workflow_report={result['workflow_report_path']}")
        if workflow_report["execution_status_counts"].get("FAILED", 0) > 0:
            return 2
        return 0
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] scenario_variant_workflow.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
