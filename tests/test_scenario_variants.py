from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.scenarios.variants import (
    build_scenario_variants_report,
    generate_variants,
    load_logical_scenarios_source,
    validate_logical_scenarios_payload,
)
from hybrid_sensor_sim.tools.scenario_variants import main as scenario_variants_main


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_validation"


class ScenarioVariantsTests(unittest.TestCase):
    def test_validate_logical_scenarios_accepts_legacy_payload(self) -> None:
        payload = {
            "logical_scenarios": [
                {
                    "scenario_id": "demo",
                    "parameters": {
                        "speed": [1, 2],
                    },
                }
            ]
        }
        normalized = validate_logical_scenarios_payload(payload)
        self.assertEqual(normalized["logical_scenarios_schema_version"], "logical_scenarios_v0")

    def test_generate_variants_full_is_deterministic(self) -> None:
        payload, source_path, source_kind = load_logical_scenarios_source(
            logical_scenarios_path=str(FIXTURE_ROOT / "highway_cut_in_v0.json"),
        )
        report = build_scenario_variants_report(
            payload=payload,
            source_path=source_path,
            source_kind=source_kind,
            sampling="full",
            sample_size=0,
            max_variants_per_scenario=1000,
            seed=0,
        )
        self.assertEqual(report["scenario_variants_report_schema_version"], "scenario_variants_report_v0")
        self.assertEqual(report["variant_count"], 8)
        self.assertEqual(report["variants"][0]["scenario_id"], "scn_highway_cut_in_0001")

    def test_generate_variants_random_is_seeded(self) -> None:
        payload, _, _ = load_logical_scenarios_source(
            logical_scenarios_path=str(FIXTURE_ROOT / "highway_cut_in_v0.json"),
        )
        variants_a = generate_variants(payload, sampling="random", sample_size=2, max_variants_per_scenario=1000, seed=7)
        variants_b = generate_variants(payload, sampling="random", sample_size=2, max_variants_per_scenario=1000, seed=7)
        self.assertEqual(variants_a, variants_b)
        self.assertEqual(len(variants_a), 2)

    def test_scenario_language_profile_path_is_supported(self) -> None:
        payload, source_path, source_kind = load_logical_scenarios_source(
            scenario_language_profile="highway_cut_in_v0",
            scenario_language_dir=FIXTURE_ROOT,
        )
        self.assertEqual(source_kind, "scenario_language_profile")
        self.assertEqual(source_path.name, "highway_cut_in_v0.json")
        self.assertEqual(payload["logical_scenarios_schema_version"], "logical_scenarios_v0")

    def test_scenario_variants_cli_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "variants.json"
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = scenario_variants_main(
                    [
                        "--logical-scenarios",
                        str(FIXTURE_ROOT / "highway_cut_in_v0.json"),
                        "--out",
                        str(out_path),
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["variant_count"], 8)

    def test_scenario_variants_script_bootstraps_src_path(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_scenario_variants.py"
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0)
        self.assertIn("generate concrete scenario variants", proc.stdout.lower())


if __name__ == "__main__":
    unittest.main()
