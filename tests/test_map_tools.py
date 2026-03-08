from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.maps import (
    CANONICAL_MAP_ROUTE_REPORT_SCHEMA_VERSION_V0,
    CANONICAL_MAP_VALIDATION_REPORT_SCHEMA_VERSION_V0,
    compute_canonical_route,
    convert_canonical_to_simple,
    convert_simple_to_canonical,
    load_map_payload,
    validate_canonical_map,
)


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_map_toolset"


class MapToolsTests(unittest.TestCase):
    def test_convert_simple_to_canonical_roundtrip(self) -> None:
        simple_payload = load_map_payload(FIXTURE_ROOT / "simple_map_v0.json", "simple map")
        canonical_payload = convert_simple_to_canonical(simple_payload)
        self.assertEqual(canonical_payload["map_schema_version"], "canonical_lane_graph_v0")
        roundtrip_payload = convert_canonical_to_simple(canonical_payload)
        self.assertEqual(roundtrip_payload["map_schema_version"], "simple_map_v0")
        self.assertEqual(roundtrip_payload["roads"][0]["road_id"], "lane_a")

    def test_validate_canonical_map_reports_pass_for_valid_fixture(self) -> None:
        canonical_payload = load_map_payload(FIXTURE_ROOT / "canonical_lane_graph_v0.json", "canonical map")
        errors, warnings, semantic_summary = validate_canonical_map(canonical_payload)
        self.assertEqual(errors, [])
        self.assertEqual(warnings, [])
        self.assertEqual(semantic_summary["routing_semantic_status"], "pass")
        self.assertEqual(semantic_summary["entry_lane_count"], 1)

    def test_compute_canonical_route_supports_hops_and_via(self) -> None:
        canonical_payload = load_map_payload(FIXTURE_ROOT / "canonical_lane_graph_v0.json", "canonical map")
        report = compute_canonical_route(
            canonical_payload,
            entry_lane_id="lane_a",
            exit_lane_id="lane_c",
            via_lane_ids=["lane_b"],
            cost_mode="hops",
            map_path=FIXTURE_ROOT / "canonical_lane_graph_v0.json",
        )
        self.assertEqual(report["report_schema_version"], CANONICAL_MAP_ROUTE_REPORT_SCHEMA_VERSION_V0)
        self.assertEqual(report["route_status"], "pass")
        self.assertEqual(report["route_lane_ids"], ["lane_a", "lane_b", "lane_c"])
        self.assertEqual(report["route_segment_count"], 2)

    def test_run_map_convert_cli_writes_output(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_map_convert.py"
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "canonical.json"
            proc = subprocess.run(
                [
                    sys.executable,
                    str(script_path),
                    "--input",
                    str(FIXTURE_ROOT / "simple_map_v0.json"),
                    "--out",
                    str(out_path),
                    "--to-format",
                    "canonical",
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0)
            payload = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["map_schema_version"], "canonical_lane_graph_v0")

    def test_run_map_validate_cli_writes_report(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        sys.path.insert(0, str(repo_root / "src"))
        script_path = repo_root / "scripts" / "run_map_validate.py"
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "validation_report.json"
            proc = subprocess.run(
                [
                    sys.executable,
                    str(script_path),
                    "--map",
                    str(FIXTURE_ROOT / "canonical_lane_graph_v0.json"),
                    "--report-out",
                    str(report_path),
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0)
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["report_schema_version"], CANONICAL_MAP_VALIDATION_REPORT_SCHEMA_VERSION_V0)
            self.assertEqual(payload["error_count"], 0)

    def test_run_map_route_cli_writes_report(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_map_route.py"
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "route_report.json"
            proc = subprocess.run(
                [
                    sys.executable,
                    str(script_path),
                    "--map",
                    str(FIXTURE_ROOT / "canonical_lane_graph_v0.json"),
                    "--entry-lane-id",
                    "lane_a",
                    "--exit-lane-id",
                    "lane_c",
                    "--via-lane-id",
                    "lane_b",
                    "--report-out",
                    str(report_path),
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0)
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["route_lane_ids"], ["lane_a", "lane_b", "lane_c"])

    def test_script_help_bootstraps(self) -> None:
        for script_name in ("run_map_convert.py", "run_map_validate.py", "run_map_route.py"):
            script_path = Path(__file__).resolve().parents[1] / "scripts" / script_name
            proc = subprocess.run(
                [sys.executable, str(script_path), "--help"],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0)
            self.assertIn("map", proc.stdout.lower())


if __name__ == "__main__":
    unittest.main()
