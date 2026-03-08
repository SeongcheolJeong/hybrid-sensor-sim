from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from hybrid_sensor_sim.tools.renderer_backend_linux_handoff_selftest import (
    main as linux_handoff_selftest_main,
    run_renderer_backend_linux_handoff_selftest,
)


class RendererBackendLinuxHandoffSelftestTests(unittest.TestCase):
    def test_linux_handoff_selftest_script_bootstraps_src_path(self) -> None:
        script_path = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "run_renderer_backend_linux_handoff_selftest.py"
        )
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(proc.returncode, 0)
        self.assertIn("synthetic Linux handoff bundle", proc.stdout)

    def test_linux_handoff_selftest_builds_bundle_and_verifies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_linux_handoff_selftest.run_renderer_backend_linux_handoff_in_docker",
                return_value={"return_code": 0, "summary_path": str(root / "docker_summary.json")},
            ) as docker_run:
                summary = run_renderer_backend_linux_handoff_selftest(
                    repo_root=repo_root,
                    output_root=root / "output",
                    execute=False,
                )

            self.assertTrue(summary["success"])
            self.assertFalse(summary["marker_exists"])
            self.assertTrue(Path(summary["bundle_artifacts"]["bundle_path"]).exists())
            self.assertTrue(Path(summary["bundle_artifacts"]["transfer_manifest_path"]).exists())
            self.assertTrue(Path(summary["bundle_artifacts"]["bundle_manifest_path"]).exists())
            self.assertTrue(docker_run.called)
            self.assertTrue(docker_run.call_args.kwargs["skip_run"])

    def test_linux_handoff_selftest_execute_requires_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            marker_relative_path = "artifacts/selftest/marker.txt"
            marker_path = repo_root / marker_relative_path

            def _fake_docker(**_: object) -> dict[str, object]:
                marker_path.parent.mkdir(parents=True, exist_ok=True)
                marker_path.write_text("selftest-ok\n", encoding="utf-8")
                return {"return_code": 0, "summary_path": str(root / "docker_summary.json")}

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_linux_handoff_selftest.run_renderer_backend_linux_handoff_in_docker",
                side_effect=_fake_docker,
            ):
                summary = run_renderer_backend_linux_handoff_selftest(
                    repo_root=repo_root,
                    output_root=root / "output",
                    execute=True,
                    marker_relative_path=marker_relative_path,
                )

            self.assertTrue(summary["success"])
            self.assertTrue(summary["marker_exists"])
            self.assertEqual(summary["marker_content"], "selftest-ok")

    def test_linux_handoff_selftest_main_returns_failure_from_docker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_linux_handoff_selftest.run_renderer_backend_linux_handoff_in_docker",
                return_value={"return_code": 9, "summary_path": str(root / "docker_summary.json")},
            ):
                with contextlib.redirect_stdout(io.StringIO()):
                    exit_code = linux_handoff_selftest_main(
                        [
                            "--repo-root",
                            str(repo_root),
                            "--output-root",
                            str(root / "output"),
                        ]
                    )

            self.assertEqual(exit_code, 9)
            summary = json.loads(
                (root / "output" / "renderer_backend_linux_handoff_selftest.json").read_text(encoding="utf-8")
            )
            self.assertFalse(summary["success"])


if __name__ == "__main__":
    unittest.main()
