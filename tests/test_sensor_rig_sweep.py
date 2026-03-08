from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.sensor_rig_sweep import (
    SENSOR_RIG_SWEEP_REPORT_SCHEMA_VERSION_V1,
    load_rig_sweep_definition,
    main as sensor_rig_sweep_main,
    run_sensor_rig_sweep,
)


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_sim_engine"


class SensorRigSweepTests(unittest.TestCase):
    def test_load_rig_sweep_definition_rejects_bad_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base_config_path = root / "base.json"
            rig_path = root / "rig.json"
            base_config_path.write_text(
                json.dumps(
                    {
                        "scenario_path": str(FIXTURE_ROOT / "highway_safe_following_v0.json"),
                        "output_dir": "artifacts/tmp",
                    }
                ),
                encoding="utf-8",
            )
            rig_path.write_text(json.dumps({"rig_sweep_schema_version": "wrong", "candidates": []}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "rig_sweep_schema_version must be"):
                load_rig_sweep_definition(base_config_path=base_config_path, rig_candidates_path=rig_path)

    def test_run_sensor_rig_sweep_ranks_covering_candidate_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_root = Path(tmp) / "rig_sweep"
            report = run_sensor_rig_sweep(
                base_config_path=FIXTURE_ROOT / "rig_sweep_base_config.json",
                rig_candidates_path=FIXTURE_ROOT / "rig_sweep_candidates_v1.json",
                out_root=out_root,
            )
            self.assertEqual(
                report["sensor_rig_sweep_report_schema_version"],
                SENSOR_RIG_SWEEP_REPORT_SCHEMA_VERSION_V1,
            )
            self.assertEqual(report["candidate_count"], 2)
            self.assertEqual(report["best_rig_id"], "front_bundle")
            self.assertEqual(report["rankings"][0]["rig_id"], "front_bundle")
            self.assertGreater(
                report["rankings"][0]["covered_target_count"],
                report["rankings"][1]["covered_target_count"],
            )
            self.assertTrue((out_root / "sensor_rig_sweep_report_v1.json").exists())
            self.assertTrue(
                (out_root / "candidates" / "front_bundle" / "sensor_coverage_summary.json").exists()
            )

    def test_sensor_rig_sweep_cli_writes_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_root = Path(tmp) / "rig_sweep"
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = sensor_rig_sweep_main(
                    [
                        "--base-config",
                        str(FIXTURE_ROOT / "rig_sweep_base_config.json"),
                        "--rig-candidates",
                        str(FIXTURE_ROOT / "rig_sweep_candidates_v1.json"),
                        "--out",
                        str(out_root),
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads((out_root / "sensor_rig_sweep_report_v1.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["best_rig_id"], "front_bundle")

    def test_sensor_rig_sweep_script_bootstraps_src_path(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_sensor_rig_sweep.py"
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0)
        self.assertIn("sensor rig candidates", proc.stdout.lower())


if __name__ == "__main__":
    unittest.main()
