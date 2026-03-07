from __future__ import annotations

import contextlib
import io
import json
import os
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path

from hybrid_sensor_sim.tools.renderer_backend_package_stage import (
    build_renderer_backend_package_stage,
    main as package_stage_main,
)


def _write_zip(path: Path, entries: dict[str, str]) -> None:
    with zipfile.ZipFile(path, "w") as handle:
        for name, content in entries.items():
            handle.writestr(name, content)


def _write_tar_gz(path: Path, entries: dict[str, str]) -> None:
    with tarfile.open(path, "w:gz") as handle:
        for name, content in entries.items():
            payload = content.encode("utf-8")
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            handle.addfile(info, io.BytesIO(payload))


class RendererBackendPackageStageTests(unittest.TestCase):
    def test_build_stage_extracts_awsim_archive_and_preserves_setup_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / "AWSIM-Demo.zip"
            _write_zip(
                archive,
                {
                    "AWSIM-Demo/AWSIM-Demo.x86_64": "#!/usr/bin/env bash\nexit 0\n",
                    "AWSIM-Demo/README.txt": "demo",
                },
            )
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_DOCKER_IMAGE": "heliosplusplus:cli",
                            "HELIOS_DOCKER_BINARY": "/home/jovyan/helios/build/helios++",
                            "AWSIM_RENDERER_MAP": "EnvAwsimMap",
                        },
                        "acquisition_hints": {
                            "awsim": {
                                "local_download_candidates": [str(archive)],
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            summary = build_renderer_backend_package_stage(
                backend="awsim",
                repo_root=root / "repo",
                setup_summary_path=setup_summary,
                output_root=root / "staged_runtime",
            )

            executable = Path(summary["staging"]["selected_executable_path"])
            self.assertTrue(summary["readiness"]["archive_resolved"])
            self.assertTrue(summary["readiness"]["archive_extracted"])
            self.assertTrue(summary["readiness"]["backend_executable_ready"])
            self.assertEqual(summary["selection"]["AWSIM_RENDERER_MAP"], "EnvAwsimMap")
            self.assertEqual(summary["selection"]["HELIOS_DOCKER_IMAGE"], "heliosplusplus:cli")
            self.assertTrue(executable.exists())
            self.assertTrue(os.access(executable, os.X_OK))
            self.assertIn("AWSIM-Demo.x86_64", executable.name)

    def test_build_stage_uses_setup_candidate_when_archive_omitted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / "CARLA_UE5_Latest.tar.gz"
            _write_tar_gz(
                archive,
                {
                    "CARLA_UE5/CarlaUnreal.sh": "#!/usr/bin/env bash\nexit 0\n",
                },
            )
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_DOCKER_IMAGE": "heliosplusplus:cli",
                            "HELIOS_DOCKER_BINARY": "/home/jovyan/helios/build/helios++",
                        },
                        "acquisition_hints": {
                            "carla": {
                                "local_download_candidates": [str(archive)],
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            summary = build_renderer_backend_package_stage(
                backend="carla",
                repo_root=root / "repo",
                setup_summary_path=setup_summary,
                output_root=root / "carla_stage",
            )

            self.assertEqual(summary["archive_source"], "setup_summary")
            self.assertTrue(summary["readiness"]["backend_executable_ready"])
            self.assertTrue(summary["readiness"]["smoke_ready_docker"])
            self.assertIn("CarlaUnreal.sh", summary["staging"]["selected_executable_name"])

    def test_main_writes_summary_and_env_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / "AWSIM-Demo.zip"
            _write_zip(
                archive,
                {
                    "runtime/AWSIM-Demo-Lightweight.x86_64": "#!/usr/bin/env bash\nexit 0\n",
                },
            )
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_DOCKER_IMAGE": "heliosplusplus:cli",
                            "HELIOS_DOCKER_BINARY": "/home/jovyan/helios/build/helios++",
                        },
                        "acquisition_hints": {
                            "awsim": {
                                "local_download_candidates": [str(archive)],
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            output_root = root / "third_party" / "runtime_backends" / "awsim"

            with contextlib.redirect_stdout(io.StringIO()) as stdout:
                exit_code = package_stage_main(
                    [
                        "--backend",
                        "awsim",
                        "--archive",
                        str(archive),
                        "--setup-summary",
                        str(setup_summary),
                        "--output-root",
                        str(output_root),
                    ]
                )

            self.assertEqual(exit_code, 0)
            summary_path = output_root / "renderer_backend_package_stage.json"
            env_path = output_root / "renderer_backend_package_stage.env.sh"
            self.assertTrue(summary_path.exists())
            self.assertTrue(env_path.exists())
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            env_text = env_path.read_text(encoding="utf-8")
            self.assertIn("AWSIM_BIN", env_text)
            self.assertIn("HELIOS_DOCKER_IMAGE", env_text)
            self.assertEqual(
                payload["staging"]["selected_executable_name"],
                "AWSIM-Demo-Lightweight.x86_64",
            )
            self.assertIn("renderer_backend_package_stage.env.sh", stdout.getvalue())

    def test_stage_reports_missing_executable_when_archive_contents_do_not_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / "AWSIM-Demo.zip"
            _write_zip(
                archive,
                {
                    "AWSIM-Demo/not-a-runtime.txt": "demo",
                },
            )

            summary = build_renderer_backend_package_stage(
                backend="awsim",
                repo_root=root / "repo",
                archive=archive,
                output_root=root / "staged_runtime",
            )

            self.assertFalse(summary["readiness"]["backend_executable_ready"])
            self.assertIn("Could not locate a supported awsim executable", "\n".join(summary["issues"]))
            self.assertIsNone(summary["staging"]["selected_executable_path"])


if __name__ == "__main__":
    unittest.main()
