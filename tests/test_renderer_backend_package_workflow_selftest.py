from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.renderer_backend_package_workflow_selftest import (
    main as package_workflow_selftest_main,
    run_renderer_backend_package_workflow_selftest,
)


class RendererBackendPackageWorkflowSelftestTests(unittest.TestCase):
    def test_package_workflow_selftest_script_bootstraps_src_path(self) -> None:
        script_path = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "run_renderer_backend_package_workflow_selftest.py"
        )
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(proc.returncode, 0)
        self.assertIn("package workflow self-test", proc.stdout.lower())

    def test_package_workflow_selftest_runs_local_candidate_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp) / "selftest"

            payload = run_renderer_backend_package_workflow_selftest(
                backend="awsim",
                output_root=output_root,
                archive_source="local_candidate",
            )

            self.assertTrue(payload["success"])
            self.assertEqual(payload["workflow_status"], "SMOKE_SUCCEEDED")
            self.assertEqual(payload["output_comparison_status"], "MATCHED")
            self.assertTrue(payload["acquire_stage_ready"])
            self.assertTrue(payload["archive_exists"])
            self.assertTrue(Path(payload["workflow_summary_path"]).exists())
            self.assertTrue(Path(payload["summary_path"]).exists())
            self.assertTrue(payload["staged_backend_bin"])

    def test_package_workflow_selftest_main_supports_download_url_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp) / "selftest"

            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = package_workflow_selftest_main(
                    [
                        "--backend",
                        "carla",
                        "--archive-source",
                        "download_url",
                        "--output-root",
                        str(output_root),
                    ]
                )

            self.assertEqual(exit_code, 0)
            summary = json.loads(
                (output_root / "renderer_backend_package_workflow_selftest.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertTrue(summary["success"])
            self.assertEqual(summary["backend"], "carla")
            self.assertEqual(summary["archive_source"], "download_url")
            self.assertEqual(summary["workflow_status"], "SMOKE_SUCCEEDED")
            self.assertEqual(summary["output_comparison_status"], "MATCHED")


if __name__ == "__main__":
    unittest.main()
