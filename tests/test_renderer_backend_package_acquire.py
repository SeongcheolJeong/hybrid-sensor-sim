from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from hybrid_sensor_sim.tools.renderer_backend_package_acquire import (
    build_renderer_backend_package_acquire,
    main as package_acquire_main,
)


def _write_zip(path: Path, entries: dict[str, str]) -> None:
    with zipfile.ZipFile(path, "w") as handle:
        for name, content in entries.items():
            handle.writestr(name, content)


class RendererBackendPackageAcquireTests(unittest.TestCase):
    def test_build_acquire_downloads_file_url_and_stages_archive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_archive = root / "source" / "AWSIM-Demo.zip"
            source_archive.parent.mkdir(parents=True, exist_ok=True)
            _write_zip(
                source_archive,
                {
                    "AWSIM-Demo/AWSIM-Demo.x86_64": "#!/usr/bin/env bash\nexit 0\n",
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
                                "download_options": [
                                    {
                                        "name": "AWSIM-Demo.zip",
                                        "url": source_archive.resolve().as_uri(),
                                    }
                                ]
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            summary = build_renderer_backend_package_acquire(
                backend="awsim",
                repo_root=root / "repo",
                setup_summary_path=setup_summary,
                download_dir=root / "downloads",
                output_root=root / "runtime_backends" / "awsim",
            )

            self.assertTrue(summary["readiness"]["download_ready"])
            self.assertTrue(summary["readiness"]["download_performed"])
            self.assertTrue(summary["readiness"]["stage_ready"])
            self.assertEqual(summary["download"]["source"], "setup_summary")
            self.assertIsNotNone(summary["stage"])
            self.assertEqual(
                summary["stage"]["staging"]["selected_executable_name"],
                "AWSIM-Demo.x86_64",
            )
            self.assertTrue(Path(summary["download"]["target_path"]).exists())
            self.assertTrue(Path(summary["artifacts"]["stage_summary_path"]).exists())
            self.assertTrue(Path(summary["artifacts"]["stage_env_path"]).exists())

    def test_build_acquire_prefers_existing_local_archive_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_archive = root / "Downloads" / "AWSIM-Demo.zip"
            local_archive.parent.mkdir(parents=True, exist_ok=True)
            _write_zip(
                local_archive,
                {
                    "AWSIM-Demo/AWSIM-Demo.x86_64": "#!/usr/bin/env bash\nexit 0\n",
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
                                "local_download_candidates": [str(local_archive)],
                                "download_options": [
                                    {
                                        "name": "AWSIM-Demo.zip",
                                        "url": "https://example.invalid/AWSIM-Demo.zip",
                                    }
                                ],
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            summary = build_renderer_backend_package_acquire(
                backend="awsim",
                repo_root=root / "repo",
                setup_summary_path=setup_summary,
                download_dir=root / "other_downloads",
                output_root=root / "runtime_backends" / "awsim",
            )

            self.assertEqual(summary["download"]["source"], "local_candidate")
            self.assertTrue(summary["readiness"]["download_ready"])
            self.assertFalse(summary["readiness"]["download_performed"])
            self.assertEqual(summary["download"]["used_local_archive"], str(local_archive.resolve()))
            self.assertEqual(summary["download"]["target_path"], str(local_archive.resolve()))
            self.assertTrue(summary["readiness"]["stage_ready"])

    def test_build_acquire_works_with_local_archive_candidate_without_download_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_archive = root / "Downloads" / "AWSIM-Demo.zip"
            local_archive.parent.mkdir(parents=True, exist_ok=True)
            _write_zip(
                local_archive,
                {
                    "AWSIM-Demo/AWSIM-Demo.x86_64": "#!/usr/bin/env bash\nexit 0\n",
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
                                "local_download_candidates": [str(local_archive)],
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            summary = build_renderer_backend_package_acquire(
                backend="awsim",
                repo_root=root / "repo",
                setup_summary_path=setup_summary,
                output_root=root / "runtime_backends" / "awsim",
            )

            self.assertTrue(summary["readiness"]["download_url_resolved"])
            self.assertEqual(summary["download"]["source"], "local_candidate")
            self.assertEqual(summary["download"]["url"], None)
            self.assertTrue(summary["readiness"]["stage_ready"])

    def test_build_acquire_dry_run_resolves_url_without_downloading(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "acquisition_hints": {
                            "carla": {
                                "download_options": [
                                    {
                                        "name": "CARLA_UE5_Latest.tar.gz",
                                        "url": "https://example.invalid/CARLA_UE5_Latest.tar.gz",
                                    }
                                ]
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            summary = build_renderer_backend_package_acquire(
                backend="carla",
                repo_root=root / "repo",
                setup_summary_path=setup_summary,
                download_dir=root / "downloads",
                output_root=root / "runtime_backends" / "carla",
                dry_run=True,
            )

            self.assertTrue(summary["readiness"]["download_url_resolved"])
            self.assertFalse(summary["readiness"]["download_ready"])
            self.assertFalse((root / "downloads" / "CARLA_UE5_Latest.tar.gz").exists())
            self.assertIsNone(summary["stage"])

    def test_build_acquire_prefers_archive_style_url_over_release_page(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "acquisition_hints": {
                            "carla": {
                                "download_options": [
                                    {
                                        "name": "CARLA release page",
                                        "url": "https://github.com/carla-simulator/carla/releases/tag/0.10.0",
                                    },
                                    {
                                        "name": "CARLA_UE5_Latest.tar.gz",
                                        "url": "https://example.invalid/CARLA_UE5_Latest.tar.gz",
                                    },
                                ]
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            summary = build_renderer_backend_package_acquire(
                backend="carla",
                repo_root=root / "repo",
                setup_summary_path=setup_summary,
                download_dir=root / "downloads",
                output_root=root / "runtime_backends" / "carla",
                dry_run=True,
            )

            self.assertTrue(summary["readiness"]["download_url_resolved"])
            self.assertEqual(summary["download"]["url"], "https://example.invalid/CARLA_UE5_Latest.tar.gz")
            self.assertEqual(summary["download"]["name"], "CARLA_UE5_Latest.tar.gz")
            self.assertEqual(
                summary["download"]["target_path"],
                str((root / "downloads" / "CARLA_UE5_Latest.tar.gz").resolve()),
            )

    def test_build_acquire_reports_missing_archive_style_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "acquisition_hints": {
                            "carla": {
                                "download_options": [
                                    {
                                        "name": "CARLA release page",
                                        "url": "https://github.com/carla-simulator/carla/releases/tag/0.10.0",
                                    }
                                ]
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            summary = build_renderer_backend_package_acquire(
                backend="carla",
                repo_root=root / "repo",
                setup_summary_path=setup_summary,
                download_dir=root / "downloads",
                output_root=root / "runtime_backends" / "carla",
                dry_run=True,
            )

            self.assertFalse(summary["readiness"]["download_url_resolved"])
            self.assertIn("No archive-style download URL resolved for carla", "\n".join(summary["issues"]))

    def test_build_acquire_reports_missing_download_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            summary = build_renderer_backend_package_acquire(
                backend="awsim",
                repo_root=root / "repo",
                download_dir=root / "downloads",
                output_root=root / "runtime_backends" / "awsim",
                dry_run=True,
            )

            self.assertFalse(summary["readiness"]["download_url_resolved"])
            self.assertIn("No download URL resolved for awsim", "\n".join(summary["issues"]))

    def test_main_writes_summary_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_archive = root / "source" / "AWSIM-Demo.zip"
            source_archive.parent.mkdir(parents=True, exist_ok=True)
            _write_zip(
                source_archive,
                {
                    "AWSIM-Demo/AWSIM-Demo-Lightweight.x86_64": "#!/usr/bin/env bash\nexit 0\n",
                },
            )
            output_root = root / "runtime_backends" / "awsim"

            with contextlib.redirect_stdout(io.StringIO()) as stdout:
                exit_code = package_acquire_main(
                    [
                        "--backend",
                        "awsim",
                        "--download-url",
                        source_archive.resolve().as_uri(),
                        "--download-dir",
                        str(root / "downloads"),
                        "--output-root",
                        str(output_root),
                    ]
                )

            self.assertEqual(exit_code, 0)
            summary_path = output_root / "renderer_backend_package_acquire.json"
            stage_summary_path = output_root / "renderer_backend_package_stage.json"
            stage_env_path = output_root / "renderer_backend_package_stage.env.sh"
            self.assertTrue(summary_path.exists())
            self.assertTrue(stage_summary_path.exists())
            self.assertTrue(stage_env_path.exists())
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["download"]["source"], "explicit")
            self.assertIn("renderer_backend_package_acquire.json", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
