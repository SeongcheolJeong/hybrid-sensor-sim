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


class RendererBackendLocalSetupTests(unittest.TestCase):
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
            self.assertEqual(summary["selection"]["AWSIM_RENDERER_MAP"], "EnvAwsimMap")
            self.assertEqual(summary["selection"]["CARLA_RENDERER_MAP"], "EnvTown05")

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
            self.assertEqual(summary["selection"]["HELIOS_BIN"], str(helios_bin.resolve()))
            self.assertEqual(summary["selection"]["HELIOS_DOCKER_IMAGE"], "heliosplusplus:cli")
            self.assertEqual(summary["selection"]["AWSIM_BIN"], str(awsim_bin.resolve()))
            self.assertIsNone(summary["selection"]["CARLA_BIN"])
            self.assertFalse(summary["readiness"]["carla_smoke_ready"])
            self.assertIn("export HELIOS_BIN=", env_text)
            self.assertIn("export HELIOS_DOCKER_IMAGE=", env_text)
            self.assertIn("export HELIOS_DOCKER_BINARY=", env_text)
            self.assertIn("export AWSIM_BIN=", env_text)
            self.assertIn("# export CARLA_BIN=<set-me>", env_text)
            self.assertIn("configs/renderer_backend_smoke.awsim.local.example.json", env_text)
            self.assertIn("configs/renderer_backend_smoke.awsim.local.docker.example.json", env_text)


if __name__ == "__main__":
    unittest.main()
