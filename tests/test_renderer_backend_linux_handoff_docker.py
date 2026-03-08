from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.renderer_backend_linux_handoff_docker import (
    main as linux_handoff_docker_main,
)


def _write_fake_docker(path: Path, log_path: Path, exit_code: int) -> None:
    path.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf '%s\\n' \"$@\" > {str(log_path)!r}\n"
        f"exit {exit_code}\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


class RendererBackendLinuxHandoffDockerTests(unittest.TestCase):
    def test_linux_handoff_docker_script_bootstraps_src_path(self) -> None:
        script_path = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "run_renderer_backend_linux_handoff_docker.py"
        )
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(proc.returncode, 0)
        self.assertIn("handoff bundle tar.gz", proc.stdout)

    def test_linux_handoff_docker_wrapper_builds_expected_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            (repo_root / "scripts").mkdir(parents=True, exist_ok=True)
            (repo_root / "scripts/run_renderer_backend_linux_handoff.py").write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            bundle_path = root / "bundle.tar.gz"
            bundle_path.write_text("bundle", encoding="utf-8")
            transfer_manifest_path = root / "transfer_manifest.json"
            transfer_manifest_path.write_text("{}", encoding="utf-8")
            bundle_manifest_path = root / "bundle_manifest.json"
            bundle_manifest_path.write_text("{}", encoding="utf-8")
            log_path = root / "docker_args.txt"
            fake_docker = root / "fake_docker.sh"
            _write_fake_docker(fake_docker, log_path, 0)

            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = linux_handoff_docker_main(
                    [
                        "--bundle",
                        str(bundle_path),
                        "--transfer-manifest",
                        str(transfer_manifest_path),
                        "--bundle-manifest",
                        str(bundle_manifest_path),
                        "--repo-root",
                        str(repo_root),
                        "--docker-binary",
                        str(fake_docker),
                        "--docker-platform",
                        "linux/amd64",
                        "--output-root",
                        str(root / "output"),
                        "--skip-run",
                    ]
                )

            self.assertEqual(exit_code, 0)
            summary = json.loads(
                (root / "output" / "renderer_backend_linux_handoff_docker_run.json").read_text(encoding="utf-8")
            )
            args_text = log_path.read_text(encoding="utf-8")
            self.assertIn("--platform", args_text)
            self.assertIn("linux/amd64", args_text)
            self.assertIn("python:3.11-slim", args_text)
            self.assertIn("/workspace/scripts/run_renderer_backend_linux_handoff.py", args_text)
            self.assertIn("--skip-run", args_text)
            self.assertEqual(summary["return_code"], 0)
            self.assertEqual(summary["docker_platform"], "linux/amd64")
            self.assertGreaterEqual(len(summary["mounts"]), 2)

    def test_linux_handoff_docker_wrapper_propagates_exit_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            (repo_root / "scripts").mkdir(parents=True, exist_ok=True)
            (repo_root / "scripts/run_renderer_backend_linux_handoff.py").write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            bundle_path = root / "bundle.tar.gz"
            bundle_path.write_text("bundle", encoding="utf-8")
            transfer_manifest_path = root / "transfer_manifest.json"
            transfer_manifest_path.write_text("{}", encoding="utf-8")
            log_path = root / "docker_args.txt"
            fake_docker = root / "fake_docker.sh"
            _write_fake_docker(fake_docker, log_path, 7)

            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = linux_handoff_docker_main(
                    [
                        "--bundle",
                        str(bundle_path),
                        "--transfer-manifest",
                        str(transfer_manifest_path),
                        "--repo-root",
                        str(repo_root),
                        "--docker-binary",
                        str(fake_docker),
                        "--output-root",
                        str(root / "output"),
                    ]
                )

            self.assertEqual(exit_code, 7)

    def test_linux_handoff_docker_wrapper_uses_custom_container_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            (repo_root / "scripts").mkdir(parents=True, exist_ok=True)
            (repo_root / "scripts/run_renderer_backend_linux_handoff.py").write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            bundle_path = repo_root / "artifacts" / "bundle.tar.gz"
            bundle_path.parent.mkdir(parents=True, exist_ok=True)
            bundle_path.write_text("bundle", encoding="utf-8")
            transfer_manifest_path = repo_root / "artifacts" / "transfer_manifest.json"
            transfer_manifest_path.write_text("{}", encoding="utf-8")
            log_path = root / "docker_args.txt"
            fake_docker = root / "fake_docker.sh"
            _write_fake_docker(fake_docker, log_path, 0)

            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = linux_handoff_docker_main(
                    [
                        "--bundle",
                        str(bundle_path),
                        "--transfer-manifest",
                        str(transfer_manifest_path),
                        "--repo-root",
                        str(repo_root),
                        "--docker-binary",
                        str(fake_docker),
                        "--container-workspace",
                        "/repo",
                        "--output-root",
                        str(root / "output"),
                        "--skip-run",
                    ]
                )

            self.assertEqual(exit_code, 0)
            summary = json.loads(
                (root / "output" / "renderer_backend_linux_handoff_docker_run.json").read_text(encoding="utf-8")
            )
            args_text = log_path.read_text(encoding="utf-8")
            self.assertIn("/repo/scripts/run_renderer_backend_linux_handoff.py", args_text)
            self.assertEqual(summary["container_workspace"], "/repo")
            self.assertEqual(summary["container_paths"]["bundle"], "/repo/artifacts/bundle.tar.gz")

    def test_linux_handoff_docker_wrapper_handles_missing_docker_binary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            (repo_root / "scripts").mkdir(parents=True, exist_ok=True)
            (repo_root / "scripts/run_renderer_backend_linux_handoff.py").write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            bundle_path = root / "bundle.tar.gz"
            bundle_path.write_text("bundle", encoding="utf-8")
            transfer_manifest_path = root / "transfer_manifest.json"
            transfer_manifest_path.write_text("{}", encoding="utf-8")

            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = linux_handoff_docker_main(
                    [
                        "--bundle",
                        str(bundle_path),
                        "--transfer-manifest",
                        str(transfer_manifest_path),
                        "--repo-root",
                        str(repo_root),
                        "--docker-binary",
                        str(root / "missing_docker"),
                        "--output-root",
                        str(root / "output"),
                        "--skip-run",
                    ]
                )

            self.assertEqual(exit_code, 127)
            summary = json.loads(
                (root / "output" / "renderer_backend_linux_handoff_docker_run.json").read_text(encoding="utf-8")
            )
            self.assertEqual(summary["return_code"], 127)
            self.assertTrue(summary["launch_error"])


if __name__ == "__main__":
    unittest.main()
