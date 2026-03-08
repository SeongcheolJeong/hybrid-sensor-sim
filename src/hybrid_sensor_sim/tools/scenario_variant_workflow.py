from __future__ import annotations

import argparse
import json
import sys
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
    }
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
                Path(__file__).resolve().parents[4]
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
