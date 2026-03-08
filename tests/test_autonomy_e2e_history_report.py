from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.autonomy_e2e_history_refresh import refresh_autonomy_e2e_history
from hybrid_sensor_sim.tools.autonomy_e2e_history_report import (
    build_autonomy_e2e_history_report,
)


class AutonomyE2EHistoryReportTests(unittest.TestCase):
    def test_build_history_report_from_checked_in_metadata(self) -> None:
        metadata_root = Path(__file__).resolve().parents[1] / "metadata" / "autonomy_e2e"
        if not metadata_root.is_dir():
            self.skipTest("checked-in provenance metadata not generated yet")
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            result = build_autonomy_e2e_history_report(
                metadata_root=metadata_root,
                json_out=output_root / "report.json",
                markdown_out=output_root / "report.md",
            )
            self.assertTrue(Path(result["json_path"]).is_file())
            self.assertTrue(Path(result["markdown_path"]).is_file())
            self.assertIn("project_count", result["report"]["overview"])
            self.assertIn("## Project Rows", Path(result["markdown_path"]).read_text(encoding="utf-8"))

    def test_report_script_bootstraps_src_path(self) -> None:
        script_path = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "run_autonomy_e2e_history_report.py"
        )
        import subprocess

        completed = subprocess.run(
            ["python3", str(script_path), "--help"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0)
        self.assertIn("--metadata-root", completed.stdout)


if __name__ == "__main__":
    unittest.main()
