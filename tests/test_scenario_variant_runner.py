from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.scenarios.variants import build_scenario_variants_report, load_logical_scenarios_source
from hybrid_sensor_sim.tools.scenario_variant_runner import (
    SCENARIO_VARIANT_RUN_REPORT_SCHEMA_VERSION_V0,
    run_scenario_variant_report,
)
from hybrid_sensor_sim.tools.scenario_variant_runner import main as scenario_variant_runner_main


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_validation"


class ScenarioVariantRunnerTests(unittest.TestCase):
    def _build_variants_report_path(self, root: Path) -> Path:
        payload, source_path, source_kind = load_logical_scenarios_source(
            logical_scenarios_path=str(FIXTURE_ROOT / "highway_map_route_relations_v0.json"),
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
        report_path = root / "variants_report.json"
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        return report_path

    def test_run_scenario_variant_report_executes_rendered_log_scene_variants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report_path = self._build_variants_report_path(root)
            out_root = root / "variant_runs"
            report = run_scenario_variant_report(
                variants_report_path=report_path,
                out_root=out_root,
                seed=7,
                max_variants=2,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )

            self.assertEqual(
                report["scenario_variant_run_report_schema_version"],
                SCENARIO_VARIANT_RUN_REPORT_SCHEMA_VERSION_V0,
            )
            self.assertEqual(report["selected_variant_count"], 2)
            self.assertEqual(report["execution_status_counts"]["SUCCEEDED"], 2)
            self.assertTrue(report["variant_runs"])
            first_run = report["variant_runs"][0]
            self.assertEqual(first_run["rendered_payload_kind"], "log_scene_v0")
            self.assertTrue(Path(first_run["rendered_payload_path"]).is_file())
            self.assertTrue(Path(first_run["manifest_path"]).is_file())
            self.assertTrue(Path(first_run["summary_path"]).is_file())
            self.assertIn(first_run["object_sim_status"], {"success", "collision", "timeout"})

    def test_run_scenario_variant_report_marks_missing_rendered_payload_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report_path = root / "variants_report.json"
            report_path.write_text(
                json.dumps(
                    {
                        "scenario_variants_report_schema_version": "scenario_variants_report_v0",
                        "variants": [
                            {
                                "scenario_id": "variant_no_payload",
                                "logical_scenario_id": "logical_demo",
                                "parameters": {"speed": 1},
                            }
                        ],
                    },
                    indent=2,
                    ensure_ascii=True,
                )
                + "\n",
                encoding="utf-8",
            )
            report = run_scenario_variant_report(
                variants_report_path=report_path,
                out_root=root / "variant_runs",
                seed=7,
                max_variants=0,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )
            self.assertEqual(report["execution_status_counts"]["SKIPPED"], 1)
            self.assertEqual(report["variant_runs"][0]["failure_code"], "MISSING_RENDERED_PAYLOAD")

    def test_run_scenario_variant_report_marks_unsupported_payload_kind_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report_path = root / "variants_report.json"
            report_path.write_text(
                json.dumps(
                    {
                        "scenario_variants_report_schema_version": "scenario_variants_report_v0",
                        "variants": [
                            {
                                "scenario_id": "variant_bad_kind",
                                "logical_scenario_id": "logical_demo",
                                "rendered_payload_kind": "scenario_definition_v0",
                                "rendered_payload": {"scenario_definition_schema_version": "scenario_definition_v0"},
                            }
                        ],
                    },
                    indent=2,
                    ensure_ascii=True,
                )
                + "\n",
                encoding="utf-8",
            )
            report = run_scenario_variant_report(
                variants_report_path=report_path,
                out_root=root / "variant_runs",
                seed=7,
                max_variants=0,
                sds_version="sds_test",
                sim_version="sim_test",
                fidelity_profile="dev-fast",
            )
            self.assertEqual(report["execution_status_counts"]["FAILED"], 1)
            self.assertEqual(report["variant_runs"][0]["failure_code"], "UNSUPPORTED_RENDERED_PAYLOAD_KIND")

    def test_scenario_variant_runner_cli_writes_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report_path = self._build_variants_report_path(root)
            out_root = root / "variant_runs"
            out_report = root / "scenario_variant_run_report.json"
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = scenario_variant_runner_main(
                    [
                        "--variants-report",
                        str(report_path),
                        "--out",
                        str(out_root),
                        "--max-variants",
                        "1",
                        "--out-report",
                        str(out_report),
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(out_report.read_text(encoding="utf-8"))
            self.assertEqual(payload["selected_variant_count"], 1)
            self.assertEqual(payload["execution_status_counts"]["SUCCEEDED"], 1)

    def test_scenario_variant_runner_script_bootstraps_src_path(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_scenario_variant_runner.py"
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0)
        self.assertIn("execute rendered payloads", proc.stdout.lower())


if __name__ == "__main__":
    unittest.main()
