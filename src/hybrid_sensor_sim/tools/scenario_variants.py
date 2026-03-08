from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from hybrid_sensor_sim.scenarios.variants import build_scenario_variants_report, load_logical_scenarios_source


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate concrete scenario variants from logical scenario definitions.")
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
    parser.add_argument("--out", required=True, help="Output JSON path for generated concrete variants")
    parser.add_argument("--sampling", choices=["full", "random"], default="full")
    parser.add_argument("--sample-size", type=int, default=0)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--max-variants-per-scenario", type=int, default=1000)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        out_path = Path(args.out).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        scenario_language_dir = args.scenario_language_dir
        if not scenario_language_dir:
            scenario_language_dir = str(
                Path(__file__).resolve().parents[3]
                / "tests"
                / "fixtures"
                / "autonomy_e2e"
                / "p_validation"
            )
        payload, source_path, source_kind = load_logical_scenarios_source(
            logical_scenarios_path=args.logical_scenarios,
            scenario_language_profile=args.scenario_language_profile,
            scenario_language_dir=scenario_language_dir,
        )
        report = build_scenario_variants_report(
            payload=payload,
            source_path=source_path,
            source_kind=source_kind,
            sampling=args.sampling,
            sample_size=args.sample_size,
            max_variants_per_scenario=args.max_variants_per_scenario,
            seed=args.seed,
        )
        out_path.write_text(json.dumps(report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        if source_kind == "scenario_language_profile":
            print(f"[ok] scenario_language_profile={args.scenario_language_profile}")
        print(f"[ok] logical_scenario_count={report['scenario_count']}")
        print(f"[ok] generated_variant_count={report['variant_count']}")
        print(f"[ok] out={out_path}")
        return 0
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"[error] scenario_variants.py: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
