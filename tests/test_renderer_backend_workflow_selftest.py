from __future__ import annotations

import contextlib
from datetime import datetime, timezone
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from hybrid_sensor_sim.tools.renderer_backend_workflow_selftest import (
    main as workflow_selftest_main,
    run_renderer_backend_workflow_selftest,
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


class RendererBackendWorkflowSelftestTests(unittest.TestCase):
    def test_workflow_selftest_script_bootstraps_src_path(self) -> None:
        script_path = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "run_renderer_backend_workflow_selftest.py"
        )
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(proc.returncode, 0)
        self.assertIn("renderer backend workflow self-test", proc.stdout.lower())

    def test_workflow_selftest_runs_refresh_and_docker_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp) / "selftest"
            repo_root = Path(__file__).resolve().parents[1]

            def _fake_handoff_selftest(**kwargs: object) -> dict[str, object]:
                summary_path = Path(str(kwargs["summary_path"]))
                payload = {
                    "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "success": True,
                    "execute": kwargs["execute"],
                    "marker_exists": False,
                    "summary_path": str(summary_path),
                    "docker": {"return_code": 0},
                }
                summary_path.parent.mkdir(parents=True, exist_ok=True)
                summary_path.write_text(json.dumps(payload), encoding="utf-8")
                return payload

            compatibility_payload = {
                "host_compatible": False,
                "host_compatibility_reason": "ELF binary is not supported on Darwin",
                "binary_format": "elf",
                "file_description": "ELF 64-bit LSB executable",
                "binary_architectures": ["x86_64"],
                "translation_required": None,
            }

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_local_setup.run_renderer_backend_linux_handoff_selftest",
                    side_effect=_fake_handoff_selftest,
                ):
                    with patch(
                        "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_executable_host_compatibility",
                        return_value=compatibility_payload,
                    ):
                        with patch(
                            "hybrid_sensor_sim.tools.renderer_backend_workflow._inspect_executable_host_compatibility",
                            return_value=compatibility_payload,
                        ):
                            with patch(
                                "hybrid_sensor_sim.tools.renderer_backend_workflow.run_renderer_backend_linux_handoff_in_docker",
                                return_value={
                                    "return_code": 0,
                                    "summary_path": str(
                                        output_root
                                        / "workflow_run"
                                        / "renderer_backend_linux_handoff_docker_run"
                                        / "renderer_backend_linux_handoff_docker_run.json"
                                    ),
                                },
                            ):
                                payload = run_renderer_backend_workflow_selftest(
                                    repo_root=repo_root,
                                    output_root=output_root,
                                )

            self.assertTrue(payload["success"])
            self.assertEqual(payload["workflow_status"], "HANDOFF_DOCKER_VERIFIED")
            self.assertEqual(payload["workflow"]["docker_handoff"]["return_code"], 0)
            self.assertTrue(payload["workflow"]["docker_handoff"]["preflight"]["refreshed"])
            self.assertEqual(payload["workflow"]["docker_handoff"]["preflight"]["refresh_reason"], "stale")
            self.assertTrue(Path(payload["seeded_setup_summary_path"]).exists())
            self.assertTrue(Path(payload["workflow_summary_path"]).exists())

    def test_workflow_selftest_main_returns_failure_from_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp) / "selftest"
            repo_root = Path(__file__).resolve().parents[1]

            def _fake_handoff_selftest(**kwargs: object) -> dict[str, object]:
                summary_path = Path(str(kwargs["summary_path"]))
                payload = {
                    "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "success": False,
                    "execute": kwargs["execute"],
                    "marker_exists": False,
                    "summary_path": str(summary_path),
                    "docker": {"return_code": 2},
                }
                summary_path.parent.mkdir(parents=True, exist_ok=True)
                summary_path.write_text(json.dumps(payload), encoding="utf-8")
                return payload

            compatibility_payload = {
                "host_compatible": False,
                "host_compatibility_reason": "ELF binary is not supported on Darwin",
                "binary_format": "elf",
                "file_description": "ELF 64-bit LSB executable",
                "binary_architectures": ["x86_64"],
                "translation_required": None,
            }

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_helios_docker_runtime",
                return_value=_ready_docker_runtime(),
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_local_setup.run_renderer_backend_linux_handoff_selftest",
                    side_effect=_fake_handoff_selftest,
                ):
                    with patch(
                        "hybrid_sensor_sim.tools.renderer_backend_local_setup._inspect_executable_host_compatibility",
                        return_value=compatibility_payload,
                    ):
                        with patch(
                            "hybrid_sensor_sim.tools.renderer_backend_workflow._inspect_executable_host_compatibility",
                            return_value=compatibility_payload,
                        ):
                            with contextlib.redirect_stdout(io.StringIO()):
                                exit_code = workflow_selftest_main(
                                    [
                                        "--repo-root",
                                        str(repo_root),
                                        "--output-root",
                                        str(output_root),
                                    ]
                                )

            self.assertEqual(exit_code, 1)
            summary = json.loads(
                (output_root / "renderer_backend_workflow_selftest.json").read_text(encoding="utf-8")
            )
            self.assertFalse(summary["success"])
            self.assertEqual(summary["workflow_status"], "HANDOFF_DOCKER_PREFLIGHT_FAILED")


if __name__ == "__main__":
    unittest.main()
