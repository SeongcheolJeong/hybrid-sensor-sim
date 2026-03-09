from __future__ import annotations

import contextlib
import io
import json
import shlex
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

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
            self.assertIsNone(summary["readiness"]["download_space_ready"])
            self.assertFalse((root / "downloads" / "CARLA_UE5_Latest.tar.gz").exists())
            self.assertIsNone(summary["stage"])

    def test_build_acquire_uses_setup_recommended_download_dir_when_omitted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            recommended_dir = root / "external_drive" / "carla_downloads"
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "runtime_strategy": {
                            "carla": {
                                "recommended_download_dir": str(recommended_dir),
                            }
                        },
                        "acquisition_hints": {
                            "carla": {
                                "download_options": [
                                    {
                                        "name": "CARLA_UE5_Latest.tar.gz",
                                        "url": "https://example.invalid/CARLA_UE5_Latest.tar.gz",
                                    }
                                ]
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            summary = build_renderer_backend_package_acquire(
                backend="carla",
                repo_root=root / "repo",
                setup_summary_path=setup_summary,
                output_root=root / "runtime_backends" / "carla",
                dry_run=True,
            )

            self.assertEqual(summary["download"]["download_dir"], str(recommended_dir.resolve()))
            self.assertEqual(summary["download"]["download_dir_source"], "setup_summary_recommended")
            self.assertEqual(
                summary["download"]["target_path"],
                str((recommended_dir / "CARLA_UE5_Latest.tar.gz").resolve()),
            )

    def test_build_acquire_uses_setup_recommended_stage_output_root_when_omitted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            recommended_stage_root = root / "external_drive" / "runtime_backends" / "carla"
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "runtime_strategy": {
                            "carla": {
                                "recommended_stage_output_root": str(recommended_stage_root),
                            }
                        },
                        "acquisition_hints": {
                            "carla": {
                                "download_options": [
                                    {
                                        "name": "CARLA_UE5_Latest.tar.gz",
                                        "url": "https://example.invalid/CARLA_UE5_Latest.tar.gz",
                                    }
                                ]
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            summary = build_renderer_backend_package_acquire(
                backend="carla",
                repo_root=root / "repo",
                setup_summary_path=setup_summary,
                download_dir=root / "downloads",
                dry_run=True,
            )

            self.assertEqual(summary["download"]["output_root"], str(recommended_stage_root.resolve()))
            self.assertEqual(summary["download"]["output_root_source"], "setup_summary_recommended")
            self.assertIn(
                f"--output-root {shlex.quote(str(recommended_stage_root.resolve()))}",
                summary["commands"]["stage"],
            )

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

    def test_build_acquire_reports_insufficient_download_space(self) -> None:
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

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_package_acquire._probe_remote_archive_size_bytes",
                return_value=(1024, "http_head", None),
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_package_acquire.shutil.disk_usage",
                    return_value=(10_000, 9_500, 512),
                ):
                    summary = build_renderer_backend_package_acquire(
                        backend="carla",
                        repo_root=root / "repo",
                        setup_summary_path=setup_summary,
                        download_dir=root / "downloads",
                        output_root=root / "runtime_backends" / "carla",
                        dry_run=True,
                    )

            self.assertFalse(summary["readiness"]["download_space_ready"])
            self.assertEqual(summary["download"]["estimated_size_bytes"], 1024)
            self.assertEqual(summary["download"]["available_download_space_bytes"], 512)
            self.assertEqual(summary["download"]["download_space_status"], "insufficient")
            self.assertIn("Insufficient local download space for carla", "\n".join(summary["issues"]))

    def test_build_acquire_marks_local_archive_space_check_not_required(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_archive = root / "Downloads" / "CARLA_UE5_Latest.tar.gz"
            local_archive.parent.mkdir(parents=True, exist_ok=True)
            local_archive.write_text("archive", encoding="utf-8")
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "acquisition_hints": {
                            "carla": {
                                "local_download_candidates": [str(local_archive)],
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
                output_root=root / "runtime_backends" / "carla",
                dry_run=True,
                download_only=True,
            )

            self.assertTrue(summary["readiness"]["download_space_ready"])
            self.assertEqual(summary["download"]["download_space_status"], "not_required")

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

    def test_main_uses_setup_recommended_download_dir_when_not_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            recommended_dir = root / "fast_volume" / "carla"
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "runtime_strategy": {
                            "carla": {
                                "recommended_download_dir": str(recommended_dir),
                            }
                        },
                        "acquisition_hints": {
                            "carla": {
                                "download_options": [
                                    {
                                        "name": "CARLA_UE5_Latest.tar.gz",
                                        "url": "https://example.invalid/CARLA_UE5_Latest.tar.gz",
                                    }
                                ]
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            with contextlib.redirect_stdout(io.StringIO()) as stdout:
                exit_code = package_acquire_main(
                    [
                        "--backend",
                        "carla",
                        "--setup-summary",
                        str(setup_summary),
                        "--output-root",
                        str(root / "runtime_backends" / "carla"),
                        "--dry-run",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["download"]["download_dir"], str(recommended_dir.resolve()))
            self.assertEqual(payload["download"]["download_dir_source"], "setup_summary_recommended")

    def test_main_uses_setup_recommended_stage_output_root_when_not_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            recommended_stage_root = root / "fast_volume" / "runtime_backends" / "carla"
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "runtime_strategy": {
                            "carla": {
                                "recommended_stage_output_root": str(recommended_stage_root),
                            }
                        },
                        "acquisition_hints": {
                            "carla": {
                                "download_options": [
                                    {
                                        "name": "CARLA_UE5_Latest.tar.gz",
                                        "url": "https://example.invalid/CARLA_UE5_Latest.tar.gz",
                                    }
                                ]
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            with contextlib.redirect_stdout(io.StringIO()) as stdout:
                exit_code = package_acquire_main(
                    [
                        "--backend",
                        "carla",
                        "--setup-summary",
                        str(setup_summary),
                        "--download-dir",
                        str(root / "downloads"),
                        "--dry-run",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["download"]["output_root"], str(recommended_stage_root.resolve()))
            self.assertEqual(payload["download"]["output_root_source"], "setup_summary_recommended")


if __name__ == "__main__":
    unittest.main()
