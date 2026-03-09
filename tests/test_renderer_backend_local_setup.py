from __future__ import annotations

import contextlib
import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from hybrid_sensor_sim.tools.renderer_backend_local_setup import (
    _docker_image_present,
    _inspect_executable_host_compatibility,
    build_renderer_backend_local_setup,
    main as local_setup_main,
)


def _ready_docker_runtime() -> dict[str, object]:
    return {
        "image": "heliosplusplus:cli",
        "binary": "/home/jovyan/helios/build/helios++",
        "mount_point": "/workspace",
        "daemon_ready": True,
        "daemon_message": "",
        "image_ready": True,
        "image_message": "",
        "ready": True,
    }


def _unavailable_docker_runtime() -> dict[str, object]:
    return {
        "image": "heliosplusplus:cli",
        "binary": "/home/jovyan/helios/build/helios++",
        "mount_point": "/workspace",
        "daemon_ready": False,
        "daemon_message": "docker daemon is not reachable.",
        "image_ready": False,
        "image_message": "docker daemon unavailable.",
        "ready": False,
    }


def _ready_carla_docker_runtime() -> dict[str, object]:
    return {
        "image": "carlasim/carla:0.10.0",
        "platform": "linux/amd64",
        "daemon_ready": True,
        "daemon_message": "",
        "image_ready": True,
        "image_message": "",
        "ready": True,
    }


def _unavailable_carla_docker_runtime(
    message: str = "docker image not found: carlasim/carla:0.10.0",
) -> dict[str, object]:
    return {
        "image": "carlasim/carla:0.10.0",
        "platform": "linux/amd64",
        "daemon_ready": True,
        "daemon_message": "",
        "image_ready": False,
        "image_message": message,
        "ready": False,
    }


class RendererBackendLocalSetupTests(unittest.TestCase):
    def setUp(self) -> None:
        self._carla_docker_patch = patch(
            "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_carla_docker_runtime",
            return_value=_unavailable_carla_docker_runtime(),
        )
        self._carla_docker_patch.start()
        self.addCleanup(self._carla_docker_patch.stop)

    def test_inspect_executable_host_compatibility_requires_rosetta_for_macho_x86_64(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / "AWSIM"
            binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            binary.chmod(0o755)
            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._host_platform_summary",
                return_value={"system": "Darwin", "machine": "arm64"},
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_local_setup._rosetta_available",
                    return_value=False,
                ):
                    with patch(
                        "hybrid_sensor_sim.tools.renderer_backend_local_setup.subprocess.run",
                        return_value=subprocess.CompletedProcess(
                            args=["file", "-b", str(binary)],
                            returncode=0,
                            stdout="Mach-O 64-bit executable x86_64\n",
                            stderr="",
                        ),
                    ):
                        compatibility = _inspect_executable_host_compatibility(binary)

        self.assertFalse(compatibility["host_compatible"])
        self.assertEqual(compatibility["binary_format"], "mach-o")
        self.assertEqual(compatibility["binary_architectures"], ["x86_64"])
        self.assertEqual(compatibility["translation_required"], "rosetta")
        self.assertIn("requires Rosetta", compatibility["host_compatibility_reason"])

    def test_inspect_executable_host_compatibility_accepts_rosetta_for_macho_x86_64(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / "AWSIM"
            binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            binary.chmod(0o755)
            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._host_platform_summary",
                return_value={"system": "Darwin", "machine": "arm64"},
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_local_setup._rosetta_available",
                    return_value=True,
                ):
                    with patch(
                        "hybrid_sensor_sim.tools.renderer_backend_local_setup.subprocess.run",
                        return_value=subprocess.CompletedProcess(
                            args=["file", "-b", str(binary)],
                            returncode=0,
                            stdout="Mach-O 64-bit executable x86_64\n",
                            stderr="",
                        ),
                    ):
                        compatibility = _inspect_executable_host_compatibility(binary)

        self.assertTrue(compatibility["host_compatible"])
        self.assertEqual(compatibility["binary_architectures"], ["x86_64"])
        self.assertEqual(compatibility["translation_required"], "rosetta")
        self.assertEqual(compatibility["host_compatibility_reason"], "")

    def test_docker_image_present_uses_images_fallback_when_inspect_fails(self) -> None:
        with patch(
            "hybrid_sensor_sim.tools.renderer_backend_local_setup.subprocess.run",
            side_effect=[
                subprocess.CompletedProcess(
                    args=["docker", "image", "inspect", "heliosplusplus:cli"],
                    returncode=1,
                    stdout="",
                    stderr="Error response from daemon: {\"message\":\"No such image: heliosplusplus:cli\"}",
                ),
                subprocess.CompletedProcess(
                    args=["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"],
                    returncode=0,
                    stdout="heliosplusplus:cli\nubuntu:24.04\n",
                    stderr="",
                ),
            ],
        ):
            image_ready, image_message = _docker_image_present("heliosplusplus:cli")

        self.assertTrue(image_ready)
        self.assertIn("docker images listing", image_message)

    def test_build_renderer_backend_local_setup_marks_source_only_reference_roots(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            docs_root = root / "Documents" / "Autonomy-E2E" / "_reference_repos"
            awsim_root = docs_root / "awsim"
            carla_root = docs_root / "carla"
            helios_root = repo_root / "third_party" / "helios"
            (awsim_root / "Packages").mkdir(parents=True, exist_ok=True)
            (awsim_root / "ProjectSettings").mkdir(parents=True, exist_ok=True)
            (carla_root / "PythonAPI").mkdir(parents=True, exist_ok=True)
            (helios_root / "python/pyhelios").mkdir(parents=True, exist_ok=True)
            (awsim_root / "Packages/manifest.json").write_text("{}", encoding="utf-8")
            (awsim_root / "ProjectSettings/ProjectVersion.txt").write_text("1", encoding="utf-8")

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_unavailable_docker_runtime(),
            ):
                summary = build_renderer_backend_local_setup(
                    repo_root=repo_root,
                    search_roots=[root / "Documents"],
                    output_dir=root / "artifacts",
                    include_default_search_roots=False,
                )

            self.assertFalse(summary["readiness"]["helios_ready"])
            self.assertFalse(summary["readiness"]["helios_binary_ready"])
            self.assertFalse(summary["readiness"]["helios_docker_ready"])
            self.assertFalse(summary["readiness"]["awsim_ready"])
            self.assertFalse(summary["readiness"]["carla_ready"])
            self.assertTrue(summary["backends"]["helios"]["source_only"])
            self.assertTrue(summary["backends"]["awsim"]["source_only"])
            self.assertTrue(summary["backends"]["carla"]["source_only"])
            self.assertIsNone(summary["selection"]["HELIOS_BIN"])
            self.assertIsNone(summary["selection"]["AWSIM_BIN"])
            self.assertIsNone(summary["selection"]["CARLA_BIN"])
            self.assertIn(str(helios_root.resolve()), summary["backends"]["helios"]["reference_roots"])
            self.assertIn(str(awsim_root.resolve()), summary["backends"]["awsim"]["reference_roots"])
            self.assertIn(str(carla_root.resolve()), summary["backends"]["carla"]["reference_roots"])
            self.assertEqual(summary["acquisition_hints"]["helios"]["status"], "missing_runtime")
            self.assertEqual(summary["acquisition_hints"]["awsim"]["status"], "source_only")
            self.assertEqual(summary["acquisition_hints"]["carla"]["status"], "source_only")
            self.assertEqual(
                summary["acquisition_hints"]["awsim"]["recommended_executable_name"],
                "AWSIM-Demo.x86_64",
            )
            self.assertIn(
                "scripts/acquire_renderer_backend_package.py --backend awsim",
                " ".join(summary["acquisition_hints"]["awsim"]["next_actions"]),
            )
            self.assertIn(
                "https://github.com/carla-simulator/carla/releases/tag/0.10.0",
                [
                    item["url"]
                    for item in summary["acquisition_hints"]["carla"]["download_options"]
                    if "url" in item
                ],
            )

    def test_build_renderer_backend_local_setup_discovers_ready_backends(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            (repo_root / "third_party/helios/build").mkdir(parents=True, exist_ok=True)
            helios_bin = repo_root / "third_party/helios/build/helios++"
            helios_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            helios_bin.chmod(0o755)

            search_root = root / "search"
            awsim_bin = search_root / "AWSIM.app/Contents/MacOS/AWSIM"
            awsim_bin.parent.mkdir(parents=True, exist_ok=True)
            awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            awsim_bin.chmod(0o755)
            carla_bin = search_root / "CarlaUE4.sh"
            carla_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            carla_bin.chmod(0o755)

            with patch.dict(
                "os.environ",
                {
                    "AWSIM_RENDERER_MAP": "EnvAwsimMap",
                    "CARLA_RENDERER_MAP": "EnvTown05",
                },
                clear=False,
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                    return_value=_ready_docker_runtime(),
                ):
                    summary = build_renderer_backend_local_setup(
                        repo_root=repo_root,
                        search_roots=[search_root],
                        output_dir=root / "artifacts",
                        include_default_search_roots=False,
                    )

            self.assertTrue(summary["readiness"]["helios_ready"])
            self.assertTrue(summary["readiness"]["helios_binary_ready"])
            self.assertTrue(summary["readiness"]["helios_docker_ready"])
            self.assertTrue(summary["readiness"]["awsim_ready"])
            self.assertTrue(summary["readiness"]["carla_ready"])
            self.assertTrue(summary["readiness"]["awsim_smoke_ready"])
            self.assertTrue(summary["readiness"]["carla_smoke_ready"])
            self.assertEqual(summary["selection"]["HELIOS_BIN"], str(helios_bin.resolve()))
            self.assertEqual(summary["selection"]["HELIOS_DOCKER_IMAGE"], "heliosplusplus:cli")
            self.assertEqual(summary["selection"]["AWSIM_BIN"], str(awsim_bin.resolve()))
            self.assertEqual(summary["selection"]["CARLA_BIN"], str(carla_bin.resolve()))
            self.assertIn(
                "scripts/acquire_renderer_backend_package.py --backend awsim",
                summary["commands"]["awsim_acquire"],
            )
            self.assertIn(
                "scripts/acquire_renderer_backend_package.py --backend carla",
                summary["commands"]["carla_acquire"],
            )
            self.assertEqual(summary["selection"]["AWSIM_RENDERER_MAP"], "EnvAwsimMap")
            self.assertEqual(summary["selection"]["CARLA_RENDERER_MAP"], "EnvTown05")

    def test_build_renderer_backend_local_setup_ignores_selftest_artifact_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            search_root = root / "search"
            ignored_root = search_root / "backend_workflow_selftest_probe" / "inputs"
            ignored_root.mkdir(parents=True, exist_ok=True)
            ignored_bin = ignored_root / "AWSIM-Demo.x86_64"
            ignored_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            ignored_bin.chmod(0o755)
            package_ignored_root = search_root / "backend_package_workflow_selftest_probe" / "workspace" / "third_party" / "runtime_backends" / "awsim" / "expanded"
            package_ignored_root.mkdir(parents=True, exist_ok=True)
            package_ignored_bin = package_ignored_root / "AWSIM-Demo.x86_64"
            package_ignored_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            package_ignored_bin.chmod(0o755)

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                summary = build_renderer_backend_local_setup(
                    repo_root=repo_root,
                    search_roots=[search_root],
                    output_dir=root / "artifacts",
                    include_default_search_roots=False,
                )

            self.assertIsNone(summary["selection"]["AWSIM_BIN"])
            candidate_paths = [entry["path"] for entry in summary["backends"]["awsim"]["candidates"]]
            self.assertNotIn(str(ignored_bin.resolve()), candidate_paths)
            self.assertNotIn(str(package_ignored_bin.resolve()), candidate_paths)
            self.assertEqual(summary["acquisition_hints"]["helios"]["status"], "docker_ready")

    def test_build_renderer_backend_local_setup_discovers_packaged_runtime_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            search_root = root / "search"
            search_root.mkdir(parents=True, exist_ok=True)
            awsim_bin = search_root / "AWSIM-Demo.x86_64"
            awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            awsim_bin.chmod(0o755)
            carla_bin = search_root / "CarlaUnreal.sh"
            carla_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            carla_bin.chmod(0o755)

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                summary = build_renderer_backend_local_setup(
                    repo_root=repo_root,
                    search_roots=[search_root],
                    output_dir=root / "artifacts",
                    include_default_search_roots=False,
                )

            self.assertEqual(summary["selection"]["AWSIM_BIN"], str(awsim_bin.resolve()))
            self.assertEqual(summary["selection"]["CARLA_BIN"], str(carla_bin.resolve()))

    def test_build_renderer_backend_local_setup_prefers_staged_runtime_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            staged_root = repo_root / "third_party" / "runtime_backends" / "awsim"
            expanded_root = staged_root / "expanded" / "AWSIM-Demo"
            expanded_root.mkdir(parents=True, exist_ok=True)
            awsim_bin = expanded_root / "AWSIM-Demo.x86_64"
            awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            awsim_bin.chmod(0o755)
            (staged_root / "renderer_backend_package_stage.json").write_text(
                json.dumps(
                    {
                        "staging": {
                            "selected_executable_path": str(awsim_bin.resolve()),
                        }
                    }
                ),
                encoding="utf-8",
            )

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                summary = build_renderer_backend_local_setup(
                    repo_root=repo_root,
                    search_roots=[],
                    output_dir=root / "artifacts",
                    include_default_search_roots=False,
                )

            self.assertEqual(summary["selection"]["AWSIM_BIN"], str(awsim_bin.resolve()))
            origins = {
                candidate["origin"]
                for candidate in summary["backends"]["awsim"]["candidates"]
                if candidate["path"] == str(awsim_bin.resolve())
            }
            self.assertIn("stage-summary", origins)
            self.assertTrue(summary["readiness"]["awsim_ready"])

    def test_build_renderer_backend_local_setup_marks_host_incompatible_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            search_root = root / "search"
            search_root.mkdir(parents=True, exist_ok=True)
            awsim_bin = search_root / "AWSIM-Demo.x86_64"
            awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            awsim_bin.chmod(0o755)

            def _fake_inspect(path: Path) -> dict[str, object]:
                if path.resolve() == awsim_bin.resolve():
                    return {
                        "host_compatible": False,
                        "host_compatibility_reason": "ELF binary is not supported on Darwin",
                        "binary_format": "elf",
                        "file_description": "ELF 64-bit LSB executable",
                    }
                return {
                    "host_compatible": True,
                    "host_compatibility_reason": "",
                    "binary_format": "script",
                    "file_description": "shell script text executable",
                }

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_executable_host_compatibility",
                    side_effect=_fake_inspect,
                ):
                    summary = build_renderer_backend_local_setup(
                        repo_root=repo_root,
                        search_roots=[search_root],
                        output_dir=root / "artifacts",
                        include_default_search_roots=False,
                    )

            self.assertEqual(summary["selection"]["AWSIM_BIN"], str(awsim_bin.resolve()))
            self.assertTrue(summary["readiness"]["awsim_ready"])
            self.assertFalse(summary["readiness"]["awsim_host_compatible"])
            self.assertFalse(summary["readiness"]["awsim_smoke_ready_binary"])
            self.assertFalse(summary["readiness"]["awsim_smoke_ready_docker"])
            self.assertEqual(summary["acquisition_hints"]["awsim"]["status"], "runtime_incompatible_host")
            self.assertEqual(
                summary["runtime_strategy"]["awsim"]["strategy"],
                "linux_handoff_packaged_runtime",
            )
            self.assertIn(
                "HOST_INCOMPATIBLE_PACKAGED_RUNTIME",
                summary["runtime_strategy"]["awsim"]["reason_codes"],
            )
            self.assertIn(
                "AWSIM runtime binary is resolved but incompatible with the current host.",
                summary["issues"],
            )

    def test_build_renderer_backend_local_setup_prefers_host_compatible_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            search_root = root / "search"
            search_root.mkdir(parents=True, exist_ok=True)
            incompatible_awsim_bin = root / "env" / "AWSIM-Demo.x86_64"
            incompatible_awsim_bin.parent.mkdir(parents=True, exist_ok=True)
            incompatible_awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            incompatible_awsim_bin.chmod(0o755)
            compatible_awsim_bin = search_root / "AWSIM-Demo-Lightweight.x86_64"
            compatible_awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            compatible_awsim_bin.chmod(0o755)

            def _fake_inspect(path: Path) -> dict[str, object]:
                if path.resolve() == incompatible_awsim_bin.resolve():
                    return {
                        "host_compatible": False,
                        "host_compatibility_reason": "ELF binary is not supported on Darwin",
                        "binary_format": "elf",
                        "file_description": "ELF 64-bit LSB executable",
                    }
                return {
                    "host_compatible": True,
                    "host_compatibility_reason": "",
                    "binary_format": "script",
                    "file_description": "shell script text executable",
                }

            with patch.dict(
                "os.environ",
                {
                    "AWSIM_BIN": str(incompatible_awsim_bin.resolve()),
                },
                clear=False,
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                    return_value=_ready_docker_runtime(),
                ):
                    with patch(
                        "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_executable_host_compatibility",
                        side_effect=_fake_inspect,
                    ):
                        summary = build_renderer_backend_local_setup(
                            repo_root=repo_root,
                            search_roots=[search_root],
                            output_dir=root / "artifacts",
                            include_default_search_roots=False,
                        )

            self.assertEqual(summary["selection"]["AWSIM_BIN"], str(compatible_awsim_bin.resolve()))
            self.assertTrue(summary["readiness"]["awsim_host_compatible"])
            selected_origins = {
                candidate["origin"]
                for candidate in summary["backends"]["awsim"]["candidates"]
                if candidate["path"] == str(compatible_awsim_bin.resolve())
            }
            self.assertIn("search-root", selected_origins)

    def test_build_renderer_backend_local_setup_uses_docker_when_binary_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            search_root = root / "search"
            awsim_bin = search_root / "AWSIM.x86_64"
            awsim_bin.parent.mkdir(parents=True, exist_ok=True)
            awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            awsim_bin.chmod(0o755)

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                summary = build_renderer_backend_local_setup(
                    repo_root=repo_root,
                    search_roots=[search_root],
                    output_dir=root / "artifacts",
                    include_default_search_roots=False,
                )

            self.assertFalse(summary["readiness"]["helios_binary_ready"])
            self.assertTrue(summary["readiness"]["helios_docker_ready"])
            self.assertTrue(summary["readiness"]["helios_ready"])
            self.assertFalse(summary["readiness"]["awsim_smoke_ready_binary"])
            self.assertTrue(summary["readiness"]["awsim_smoke_ready_docker"])
            self.assertTrue(summary["readiness"]["awsim_smoke_ready"])
            self.assertEqual(summary["commands"]["awsim_smoke"], summary["commands"]["awsim_smoke_docker"])
            self.assertEqual(summary["acquisition_hints"]["helios"]["recommended_runtime"], "docker")

    def test_build_renderer_backend_local_setup_detects_carla_docker_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_carla_docker_runtime",
                    return_value=_ready_carla_docker_runtime(),
                ):
                    summary = build_renderer_backend_local_setup(
                        repo_root=repo_root,
                        search_roots=[],
                        output_dir=root / "artifacts",
                        include_default_search_roots=False,
                    )

            self.assertFalse(summary["readiness"]["carla_ready"])
            self.assertTrue(summary["readiness"]["carla_docker_ready"])
            self.assertTrue(summary["readiness"]["carla_local_runtime_ready"])
            self.assertEqual(
                summary["selection"]["CARLA_DOCKER_IMAGE"],
                "carlasim/carla:0.10.0",
            )
            self.assertEqual(
                summary["acquisition_hints"]["carla"]["status"],
                "docker_runtime_available",
            )
            self.assertEqual(
                summary["runtime_strategy"]["carla"]["strategy"],
                "local_docker_runtime",
            )
            self.assertEqual(
                summary["runtime_strategy"]["carla"]["preferred_runtime_source"],
                "docker",
            )
            self.assertTrue(summary["acquisition_hints"]["carla"]["docker"]["ready"])
            self.assertNotIn("CARLA runtime binary is not resolved.", summary["issues"])
            self.assertEqual(
                summary["commands"]["carla_docker_pull"],
                "docker pull --platform linux/amd64 carlasim/carla:0.10.0",
            )
            self.assertIn("docker image inspect carlasim/carla:0.10.0", summary["commands"]["carla_docker_verify"])

    def test_build_renderer_backend_local_setup_reports_carla_docker_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            docker_error = "blob sha256:deadbeef: input/output error"

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_carla_docker_runtime",
                    return_value=_unavailable_carla_docker_runtime(docker_error),
                ):
                    summary = build_renderer_backend_local_setup(
                        repo_root=repo_root,
                        search_roots=[],
                        output_dir=root / "artifacts",
                        include_default_search_roots=False,
                    )

            self.assertFalse(summary["readiness"]["carla_docker_ready"])
            self.assertIn(
                f"CARLA docker image unavailable: {docker_error}",
                summary["issues"],
            )
            self.assertEqual(
                summary["acquisition_hints"]["carla"]["docker"]["message"],
                docker_error,
            )

    def test_build_renderer_backend_local_setup_writes_carla_docker_pull_probe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)

            probe_result = {
                "generated_at_utc": "2026-03-09T00:00:00Z",
                "image": "carlasim/carla:0.10.0",
                "platform": "linux/amd64",
                "command": [
                    "docker",
                    "pull",
                    "--platform",
                    "linux/amd64",
                    "carlasim/carla:0.10.0",
                ],
                "success": False,
                "return_code": 1,
                "stdout": "",
                "stderr": "Error response from daemon: write /var/lib/desktop-containerd/daemon/io.containerd.metadata.v1.bolt/meta.db: input/output error",
            }

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_local_setup._run_carla_docker_pull_probe",
                    return_value=probe_result,
                ) as mocked_probe:
                    summary = build_renderer_backend_local_setup(
                        repo_root=repo_root,
                        search_roots=[],
                        output_dir=root / "artifacts",
                        include_default_search_roots=False,
                        probe_carla_docker_pull=True,
                    )

            mocked_probe.assert_called_once()
            self.assertIn("carla_docker_pull", summary["probes"])
            self.assertFalse(summary["probes"]["carla_docker_pull"]["success"])
            self.assertFalse(summary["probe_readiness"]["carla_docker_pull_ready"])
            self.assertIn(
                "CARLA docker pull probe failed: Error response from daemon: write /var/lib/desktop-containerd/daemon/io.containerd.metadata.v1.bolt/meta.db: input/output error",
                summary["issues"],
            )
            probe_path = Path(summary["artifacts"]["carla_docker_pull_probe_path"])
            self.assertTrue(probe_path.exists())
            probe_payload = json.loads(probe_path.read_text(encoding="utf-8"))
            self.assertEqual(probe_payload["return_code"], 1)
            self.assertEqual(
                probe_payload["image"],
                "carlasim/carla:0.10.0",
            )

    def test_build_renderer_backend_local_setup_writes_docker_storage_probe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)

            probe_result = {
                "generated_at_utc": "2026-03-09T00:00:00Z",
                "command": ["docker", "system", "df"],
                "success": False,
                "return_code": 1,
                "stdout": "",
                "stderr": "failed to create lease: write /var/lib/desktop-containerd/daemon/io.containerd.metadata.v1.bolt/meta.db: input/output error",
            }

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_local_setup._run_docker_storage_probe",
                    return_value=probe_result,
                ) as mocked_probe:
                    summary = build_renderer_backend_local_setup(
                        repo_root=repo_root,
                        search_roots=[],
                        output_dir=root / "artifacts",
                        include_default_search_roots=False,
                        probe_docker_storage=True,
                    )

            mocked_probe.assert_called_once()
            self.assertIn("docker_storage", summary["probes"])
            self.assertFalse(summary["probes"]["docker_storage"]["success"])
            self.assertFalse(summary["probe_readiness"]["docker_storage_ready"])
            self.assertEqual(
                summary["probe_readiness"]["docker_storage_status"],
                "image_store_corrupt",
            )
            self.assertIn(
                "Docker storage probe failed: failed to create lease: write /var/lib/desktop-containerd/daemon/io.containerd.metadata.v1.bolt/meta.db: input/output error",
                summary["issues"],
            )
            self.assertEqual(
                summary["acquisition_hints"]["docker"]["storage_probe_status"],
                "image_store_corrupt",
            )
            self.assertEqual(
                summary["runtime_strategy"]["carla"]["strategy"],
                "packaged_runtime_required",
            )
            self.assertIn(
                "DOCKER_STORAGE_CORRUPT",
                summary["runtime_strategy"]["carla"]["reason_codes"],
            )
            self.assertEqual(
                summary["runtime_strategy"]["carla"]["recommended_command"],
                summary["commands"]["carla_acquire"],
            )
            probe_path = Path(summary["artifacts"]["docker_storage_probe_path"])
            self.assertTrue(probe_path.exists())
            probe_payload = json.loads(probe_path.read_text(encoding="utf-8"))
            self.assertEqual(probe_payload["return_code"], 1)
            self.assertEqual(probe_payload["command"], ["docker", "system", "df"])

    def test_build_renderer_backend_local_setup_detects_local_download_archives(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            downloads_root = root / "Downloads"
            downloads_root.mkdir(parents=True, exist_ok=True)
            awsim_archive = downloads_root / "AWSIM-Demo.zip"
            awsim_archive.write_text("archive", encoding="utf-8")
            carla_archive = downloads_root / "CARLA_UE5_Latest.tar.gz"
            carla_archive.write_text("archive", encoding="utf-8")

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_unavailable_docker_runtime(),
            ):
                summary = build_renderer_backend_local_setup(
                    repo_root=repo_root,
                    search_roots=[downloads_root],
                    output_dir=root / "artifacts",
                    include_default_search_roots=False,
                )

            self.assertIn(
                str(awsim_archive.resolve()),
                summary["acquisition_hints"]["awsim"]["local_download_candidates"],
            )
            self.assertIn(
                str(carla_archive.resolve()),
                summary["acquisition_hints"]["carla"]["local_download_candidates"],
            )

    def test_build_renderer_backend_local_setup_writes_probe_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            output_dir = root / "artifacts"
            probe_result = {
                "backend": "hybrid(helios+native_physics)",
                "success": True,
                "message": "probe ok",
                "artifacts": {"point_cloud_primary": str(root / "point.xyz")},
                "metrics": {"point_cloud_file_count": 1.0},
            }

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_local_setup._run_hybrid_config",
                    return_value=probe_result,
                ) as mocked_probe:
                    summary = build_renderer_backend_local_setup(
                        repo_root=repo_root,
                        output_dir=output_dir,
                        include_default_search_roots=False,
                        probe_helios_docker_demo=True,
                        helios_docker_probe_config=repo_root / "configs/hybrid_sensor_sim.helios_docker.json",
                    )

            mocked_probe.assert_called_once()
            self.assertIn("helios_docker_demo", summary["probes"])
            self.assertTrue(summary["probes"]["helios_docker_demo"]["success"])
            probe_path = Path(summary["artifacts"]["helios_docker_probe_path"])
            self.assertTrue(probe_path.exists())
            probe_payload = json.loads(probe_path.read_text(encoding="utf-8"))
            self.assertEqual(probe_payload["message"], "probe ok")

    def test_build_renderer_backend_local_setup_writes_linux_handoff_selftest_probe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            output_dir = root / "artifacts"

            def _fake_selftest(**kwargs: object) -> dict[str, object]:
                summary_path = Path(str(kwargs["summary_path"]))
                payload = {
                    "generated_at_utc": "2026-03-08T00:00:00Z",
                    "success": True,
                    "execute": kwargs["execute"],
                    "summary_path": str(summary_path),
                    "docker": {"return_code": 0},
                }
                summary_path.parent.mkdir(parents=True, exist_ok=True)
                summary_path.write_text(json.dumps(payload), encoding="utf-8")
                return payload

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_local_setup.run_renderer_backend_linux_handoff_selftest",
                    side_effect=_fake_selftest,
                ) as mocked_selftest:
                    summary = build_renderer_backend_local_setup(
                        repo_root=repo_root,
                        output_dir=output_dir,
                        include_default_search_roots=False,
                        probe_linux_handoff_docker_selftest=True,
                        probe_linux_handoff_docker_selftest_execute=True,
                    )

            mocked_selftest.assert_called_once()
            self.assertIn("linux_handoff_docker_selftest", summary["probes"])
            self.assertTrue(summary["probes"]["linux_handoff_docker_selftest"]["success"])
            self.assertTrue(summary["probes"]["linux_handoff_docker_selftest"]["execute"])
            self.assertTrue(summary["probe_readiness"]["linux_handoff_docker_selftest_ready"])
            self.assertTrue(summary["workflow_paths"]["linux_handoff_docker_path_ready"])
            self.assertIn(
                "--probe-linux-handoff-docker-selftest",
                summary["commands"]["linux_handoff_docker_selftest"],
            )
            probe_path = Path(summary["artifacts"]["linux_handoff_docker_selftest_probe_path"])
            self.assertTrue(probe_path.exists())
            probe_payload = json.loads(probe_path.read_text(encoding="utf-8"))
            self.assertEqual(probe_payload["docker"]["return_code"], 0)
            self.assertEqual(probe_payload["generated_at_utc"], "2026-03-08T00:00:00Z")

    def test_build_renderer_backend_local_setup_runs_backend_workflow_selftest_probe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            output_dir = root / "artifacts"

            def _fake_workflow_selftest(**kwargs: object) -> dict[str, object]:
                summary_path = Path(str(kwargs["summary_path"]))
                payload = {
                    "generated_at_utc": "2026-03-08T00:00:00Z",
                    "backend": kwargs["backend"],
                    "summary_path": str(summary_path),
                    "docker_handoff_execute": kwargs["docker_handoff_execute"],
                    "workflow_status": "HANDOFF_DOCKER_VERIFIED",
                    "success": True,
                }
                summary_path.parent.mkdir(parents=True, exist_ok=True)
                summary_path.write_text(json.dumps(payload), encoding="utf-8")
                return payload

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_workflow_selftest.run_renderer_backend_workflow_selftest",
                    side_effect=_fake_workflow_selftest,
                ) as mocked_selftest:
                    summary = build_renderer_backend_local_setup(
                        repo_root=repo_root,
                        output_dir=output_dir,
                        include_default_search_roots=False,
                        probe_backend_workflow_selftest=True,
                        workflow_selftest_backend="carla",
                        probe_backend_workflow_selftest_execute=True,
                    )

            mocked_selftest.assert_called_once()
            self.assertIn("backend_workflow_selftest", summary["probes"])
            self.assertTrue(summary["probes"]["backend_workflow_selftest"]["success"])
            self.assertEqual(summary["probes"]["backend_workflow_selftest"]["backend"], "carla")
            self.assertTrue(summary["probes"]["backend_workflow_selftest"]["docker_handoff_execute"])
            self.assertTrue(summary["probe_readiness"]["backend_workflow_selftest_ready"])
            self.assertEqual(summary["probe_readiness"]["backend_workflow_status"], "HANDOFF_DOCKER_VERIFIED")
            self.assertTrue(summary["workflow_paths"]["backend_workflow_path_ready"])
            self.assertIn(
                "--probe-backend-workflow-selftest --workflow-selftest-backend carla",
                summary["commands"]["backend_workflow_selftest"],
            )
            probe_path = Path(summary["artifacts"]["backend_workflow_selftest_probe_path"])
            self.assertTrue(probe_path.exists())
            probe_payload = json.loads(probe_path.read_text(encoding="utf-8"))
            self.assertEqual(probe_payload["workflow_status"], "HANDOFF_DOCKER_VERIFIED")

    def test_build_renderer_backend_local_setup_runs_backend_package_workflow_selftest_probe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            output_dir = root / "artifacts"

            def _fake_package_workflow_selftest(**kwargs: object) -> dict[str, object]:
                summary_path = Path(str(kwargs["summary_path"]))
                payload = {
                    "generated_at_utc": "2026-03-08T00:00:00Z",
                    "backend": kwargs["backend"],
                    "archive_source": kwargs["archive_source"],
                    "summary_path": str(summary_path),
                    "workflow_status": "SMOKE_SUCCEEDED",
                    "output_comparison_status": "MATCHED",
                    "success": True,
                }
                summary_path.parent.mkdir(parents=True, exist_ok=True)
                summary_path.write_text(json.dumps(payload), encoding="utf-8")
                return payload

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_package_workflow_selftest.run_renderer_backend_package_workflow_selftest",
                    side_effect=_fake_package_workflow_selftest,
                ) as mocked_selftest:
                    summary = build_renderer_backend_local_setup(
                        repo_root=repo_root,
                        output_dir=output_dir,
                        include_default_search_roots=False,
                        probe_backend_package_workflow_selftest=True,
                        package_workflow_selftest_backend="carla",
                        package_workflow_selftest_archive_source="download_url",
                    )

            mocked_selftest.assert_called_once()
            self.assertIn("backend_package_workflow_selftest", summary["probes"])
            self.assertTrue(summary["probes"]["backend_package_workflow_selftest"]["success"])
            self.assertEqual(summary["probes"]["backend_package_workflow_selftest"]["backend"], "carla")
            self.assertEqual(
                summary["probes"]["backend_package_workflow_selftest"]["archive_source"],
                "download_url",
            )
            self.assertTrue(summary["probe_readiness"]["backend_package_workflow_selftest_ready"])
            self.assertEqual(
                summary["probe_readiness"]["backend_package_workflow_status"],
                "SMOKE_SUCCEEDED",
            )
            self.assertTrue(summary["workflow_paths"]["package_workflow_path_ready"])
            self.assertIn(
                "--probe-backend-package-workflow-selftest --package-workflow-selftest-backend carla --package-workflow-selftest-archive-source download_url",
                summary["commands"]["backend_package_workflow_selftest"],
            )
            probe_path = Path(summary["artifacts"]["backend_package_workflow_selftest_probe_path"])
            self.assertTrue(probe_path.exists())
            probe_payload = json.loads(probe_path.read_text(encoding="utf-8"))
            self.assertEqual(probe_payload["output_comparison_status"], "MATCHED")

    def test_local_setup_main_writes_env_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            script_root = root / "fake_repo"
            (script_root / "src/hybrid_sensor_sim/tools").mkdir(parents=True, exist_ok=True)
            fake_script = script_root / "src/hybrid_sensor_sim/tools/renderer_backend_local_setup.py"
            fake_script.write_text("# test anchor\n", encoding="utf-8")

            search_root = root / "search"
            search_root.mkdir(parents=True, exist_ok=True)
            helios_bin = search_root / "helios++"
            helios_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            helios_bin.chmod(0o755)
            awsim_bin = search_root / "AWSIM.x86_64"
            awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            awsim_bin.chmod(0o755)

            output_dir = root / "artifacts"
            with patch.dict("os.environ", {}, clear=True):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_local_setup.Path.home",
                    return_value=root,
                ):
                    with patch(
                        "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                        return_value=_ready_docker_runtime(),
                    ):
                        with patch(
                            "hybrid_sensor_sim.tools.renderer_backend_local_setup.__file__",
                            str(fake_script),
                        ):
                            with contextlib.redirect_stdout(io.StringIO()):
                                exit_code = local_setup_main(
                                    [
                                        "--output-dir",
                                        str(output_dir),
                                        "--search-root",
                                        str(search_root),
                                        "--no-default-search-roots",
                                    ]
                                )

            self.assertEqual(exit_code, 0)
            summary = json.loads(
                (output_dir / "renderer_backend_local_setup.json").read_text(encoding="utf-8")
            )
            env_text = (output_dir / "renderer_backend_local.env.sh").read_text(encoding="utf-8")
            report_path = output_dir / "renderer_backend_local_report.md"
            report_text = report_path.read_text(encoding="utf-8")
            self.assertEqual(summary["selection"]["HELIOS_BIN"], str(helios_bin.resolve()))
            self.assertEqual(summary["selection"]["HELIOS_DOCKER_IMAGE"], "heliosplusplus:cli")
            self.assertEqual(summary["selection"]["AWSIM_BIN"], str(awsim_bin.resolve()))
            self.assertIsNone(summary["selection"]["CARLA_BIN"])
            self.assertEqual(summary["selection"]["CARLA_DOCKER_IMAGE"], "carlasim/carla:0.10.0")
            self.assertFalse(summary["readiness"]["carla_smoke_ready"])
            self.assertFalse(summary["readiness"]["carla_docker_ready"])
            self.assertIn("probe_readiness", summary)
            self.assertIn("workflow_paths", summary)
            self.assertIn("acquisition_hints", summary)
            self.assertIn("export HELIOS_BIN=", env_text)
            self.assertIn("export HELIOS_DOCKER_IMAGE=", env_text)
            self.assertIn("export HELIOS_DOCKER_BINARY=", env_text)
            self.assertIn("export AWSIM_BIN=", env_text)
            self.assertIn("# export CARLA_BIN=<set-me>", env_text)
            self.assertIn("export CARLA_DOCKER_IMAGE=", env_text)
            self.assertIn("# helios_binary_host_compatible=", env_text)
            self.assertIn("# awsim_host_compatible=", env_text)
            self.assertIn("# carla_host_compatible=", env_text)
            self.assertIn("# carla_docker_ready=", env_text)
            self.assertIn("python3 scripts/discover_renderer_backend_local_env.py --probe-helios-docker-demo", env_text)
            self.assertIn(
                "python3 scripts/discover_renderer_backend_local_env.py --probe-linux-handoff-docker-selftest --probe-linux-handoff-docker-selftest-execute",
                env_text,
            )
            self.assertIn(
                "python3 scripts/discover_renderer_backend_local_env.py --probe-backend-workflow-selftest --workflow-selftest-backend awsim",
                env_text,
            )
            self.assertIn(
                "python3 scripts/discover_renderer_backend_local_env.py --probe-backend-package-workflow-selftest --package-workflow-selftest-backend awsim",
                env_text,
            )
            self.assertTrue(report_path.exists())
            self.assertIn("Renderer Backend Local Setup Report", report_text)
            self.assertIn("Workflow Paths", report_text)
            self.assertIn("Probe Readiness", report_text)
            self.assertIn("package_workflow_path_ready", report_text)
            self.assertIn("configs/renderer_backend_smoke.awsim.local.example.json", env_text)
            self.assertIn("configs/renderer_backend_smoke.awsim.local.docker.example.json", env_text)


if __name__ == "__main__":
    unittest.main()
