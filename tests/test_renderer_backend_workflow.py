from __future__ import annotations

import contextlib
from datetime import datetime, timedelta, timezone
import io
import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from hybrid_sensor_sim.tools.renderer_backend_workflow import (
    build_renderer_backend_workflow,
    main as workflow_main,
)


def _utc_iso(offset_seconds: int = 0) -> str:
    return (
        datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)
    ).isoformat().replace("+00:00", "Z")


def _write_fake_helios_script(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
out=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--output" ]]; then
    out="$2"
    shift 2
  else
    shift
  fi
done
mkdir -p "${out}/demo/2026-01-01_00-00-00"
rootdir="${out}/demo/2026-01-01_00-00-00"
echo "Output directory: \\"${rootdir}\\""
cat > "${rootdir}/scan_points.xyz" <<EOF
10.0 0.0 0.0
EOF
cat > "${rootdir}/scan_trajectory.txt" <<EOF
0.0 0.0 0.0 0.0 0.0 0.0 0.0
1.0 0.0 0.0 1.0 0.0 0.0 0.0
EOF
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _write_fake_backend_archive(path: Path) -> None:
    with zipfile.ZipFile(path, "w") as handle:
        handle.writestr(
            "AWSIM-Demo/AWSIM-Demo.x86_64",
            """#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
import json
import os
from pathlib import Path

spec = json.loads(Path(os.environ["BACKEND_OUTPUT_SPEC_PATH"]).read_text(encoding="utf-8"))
for entry in spec.get("expected_outputs", []):
    path = Path(entry["path"])
    if entry.get("kind") == "directory":
        path.mkdir(parents=True, exist_ok=True)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"artifact_key": entry["artifact_key"]}), encoding="utf-8")
PY
""",
        )


class RendererBackendWorkflowTests(unittest.TestCase):
    def _write_base_config(self, *, root: Path, survey: Path, helios_bin: Path, output_dir: Path) -> Path:
        config_path = root / "base_config.json"
        config_path.write_text(
            json.dumps(
                {
                    "mode": "hybrid_auto",
                    "helios_runtime": "binary",
                    "helios_bin": str(helios_bin),
                    "scenario_path": str(survey),
                    "output_dir": str(output_dir),
                    "sensor_profile": "smoke",
                    "seed": 9,
                    "options": {
                        "helios_runtime": "binary",
                        "execute_helios": True,
                        "camera_projection_enabled": True,
                        "camera_projection_trajectory_sweep_enabled": False,
                        "lidar_postprocess_enabled": False,
                        "radar_postprocess_enabled": False,
                        "renderer_bridge_enabled": False,
                        "renderer_backend": "none",
                        "renderer_execute": False,
                        "renderer_fail_on_error": False,
                        "renderer_command": [],
                    },
                }
            ),
            encoding="utf-8",
        )
        return config_path

    def test_workflow_dry_run_reports_blocked_when_backend_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_DOCKER_IMAGE": "heliosplusplus:cli",
                            "HELIOS_DOCKER_BINARY": "/home/jovyan/helios/build/helios++",
                            "AWSIM_BIN": None,
                            "AWSIM_RENDERER_MAP": "SampleMap",
                        },
                        "readiness": {
                            "helios_ready": True,
                        },
                        "acquisition_hints": {
                            "awsim": {
                                "platform_supported": False,
                                "platform_note": "AWSIM quick-start docs assume Ubuntu 22.04 with NVIDIA RTX and driver 570+.",
                            }
                        },
                        "commands": {
                            "awsim_acquire": "python3 scripts/acquire_renderer_backend_package.py --backend awsim",
                        },
                    }
                ),
                encoding="utf-8",
            )
            config_path = self._write_base_config(
                root=root,
                survey=survey,
                helios_bin=fake_helios,
                output_dir=root / "smoke_base_output",
            )

            summary = build_renderer_backend_workflow(
                backend="awsim",
                repo_root=root / "repo",
                workflow_root=root / "workflow",
                setup_summary_path=setup_summary,
                config_path=config_path,
                dry_run=True,
            )

            self.assertEqual(summary["status"], "DRY_RUN_BLOCKED")
            self.assertFalse(summary["success"])
            self.assertIn("AWSIM_BIN is not resolved", "\n".join(summary["issues"]))
            self.assertEqual(summary["recommended_next_command"], summary["commands"]["acquire"])
            self.assertEqual(summary["final_selection"]["AWSIM_BIN"], None)
            self.assertTrue(summary["smoke"]["planned_effective_config_ready"])
            self.assertEqual(
                summary["smoke"]["planned_effective_config"]["options"]["renderer_backend"],
                "awsim",
            )
            blocker_codes = [entry["code"] for entry in summary["blockers"]]
            self.assertIn("BACKEND_BIN_MISSING", blocker_codes)
            self.assertIn("AUTO_ACQUIRE_DISABLED", blocker_codes)
            self.assertIn("BACKEND_PLATFORM_UNSUPPORTED", blocker_codes)
            self.assertNotIn("SMOKE_CONFIG_UNRESOLVED", blocker_codes)

    def test_workflow_can_auto_acquire_and_run_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            archive = root / "AWSIM-Demo.zip"
            _write_fake_backend_archive(archive)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_BIN": str(fake_helios.resolve()),
                            "AWSIM_RENDERER_MAP": "Town12",
                        },
                        "readiness": {
                            "helios_ready": True,
                        },
                        "acquisition_hints": {
                            "awsim": {
                                "platform_supported": False,
                                "platform_note": "AWSIM quick-start docs assume Ubuntu 22.04 with NVIDIA RTX and driver 570+.",
                                "download_options": [
                                    {
                                        "name": "AWSIM-Demo.zip",
                                        "url": archive.resolve().as_uri(),
                                    }
                                ]
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            config_path = self._write_base_config(
                root=root,
                survey=survey,
                helios_bin=fake_helios,
                output_dir=root / "smoke_base_output",
            )

            summary = build_renderer_backend_workflow(
                backend="awsim",
                repo_root=root / "repo",
                workflow_root=root / "workflow",
                setup_summary_path=setup_summary,
                config_path=config_path,
                auto_acquire=True,
                download_dir=root / "downloads",
            )

            self.assertEqual(summary["status"], "SMOKE_SUCCEEDED")
            self.assertTrue(summary["success"])
            self.assertIsNotNone(summary["acquire"])
            self.assertTrue(summary["acquire"]["readiness"]["stage_ready"])
            self.assertTrue(summary["smoke"]["executed"])
            self.assertEqual(summary["smoke"]["summary"]["backend"], "awsim")
            self.assertEqual(summary["smoke"]["summary"]["output_comparison"]["status"], "MATCHED")
            self.assertTrue(summary["smoke"]["planned_effective_config_ready"])
            self.assertTrue(summary["final_selection"]["AWSIM_BIN"])
            self.assertEqual(summary["final_selection"]["AWSIM_RENDERER_MAP"], "Town12")
            self.assertIsNotNone(summary["refreshed_setup"])
            self.assertEqual(
                summary["refreshed_setup"]["selection"]["AWSIM_BIN"],
                summary["final_selection"]["AWSIM_BIN"],
            )
            self.assertEqual(
                summary["commands"]["rerun_smoke"].startswith("python3 scripts/run_renderer_backend_smoke.py"),
                True,
            )
            blocker_codes = [entry["code"] for entry in summary["blockers"]]
            self.assertIn("BACKEND_PLATFORM_UNSUPPORTED", blocker_codes)
            self.assertNotIn("BACKEND_BIN_MISSING", blocker_codes)

    def test_workflow_can_stage_existing_local_archive_without_download(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            local_archive = root / "Downloads" / "AWSIM-Demo.zip"
            local_archive.parent.mkdir(parents=True, exist_ok=True)
            _write_fake_backend_archive(local_archive)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_BIN": str(fake_helios.resolve()),
                            "AWSIM_RENDERER_MAP": "Town13",
                        },
                        "readiness": {
                            "helios_ready": True,
                        },
                        "acquisition_hints": {
                            "awsim": {
                                "platform_supported": False,
                                "platform_note": "AWSIM quick-start docs assume Ubuntu 22.04 with NVIDIA RTX and driver 570+.",
                                "local_download_candidates": [str(local_archive)],
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            config_path = self._write_base_config(
                root=root,
                survey=survey,
                helios_bin=fake_helios,
                output_dir=root / "smoke_base_output",
            )

            summary = build_renderer_backend_workflow(
                backend="awsim",
                repo_root=root / "repo",
                workflow_root=root / "workflow",
                setup_summary_path=setup_summary,
                config_path=config_path,
                auto_acquire=True,
            )

            self.assertEqual(summary["status"], "SMOKE_SUCCEEDED")
            self.assertEqual(summary["acquire"]["download"]["source"], "local_candidate")
            self.assertEqual(
                summary["acquire"]["download"]["used_local_archive"],
                str(local_archive.resolve()),
            )
            self.assertTrue(summary["smoke"]["executed"])
            self.assertEqual(summary["final_selection"]["AWSIM_RENDERER_MAP"], "Town13")
            self.assertIsNotNone(summary["refreshed_setup"])
            self.assertEqual(
                summary["recommended_next_command"],
                None,
            )

    def test_workflow_blocks_on_backend_host_incompatibility(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            awsim_bin = root / "AWSIM-Demo.x86_64"
            awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            awsim_bin.chmod(0o755)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_BIN": str(fake_helios.resolve()),
                            "AWSIM_BIN": str(awsim_bin.resolve()),
                            "AWSIM_RENDERER_MAP": "Town07",
                        },
                        "readiness": {
                            "helios_ready": True,
                            "awsim_host_compatible": False,
                        },
                        "acquisition_hints": {
                            "awsim": {
                                "platform_supported": False,
                                "platform_note": "AWSIM quick-start docs assume Ubuntu 22.04 with NVIDIA RTX and driver 570+.",
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            config_path = self._write_base_config(
                root=root,
                survey=survey,
                helios_bin=fake_helios,
                output_dir=root / "smoke_base_output",
            )

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_workflow._inspect_executable_host_compatibility",
                return_value={
                    "host_compatible": False,
                    "host_compatibility_reason": "ELF binary is not supported on Darwin",
                    "binary_format": "elf",
                    "file_description": "ELF 64-bit LSB executable",
                },
            ):
                summary = build_renderer_backend_workflow(
                    backend="awsim",
                    repo_root=root / "repo",
                    workflow_root=root / "workflow",
                    setup_summary_path=setup_summary,
                    config_path=config_path,
                    dry_run=True,
                )

            self.assertEqual(summary["status"], "DRY_RUN_BLOCKED")
            self.assertFalse(summary["success"])
            self.assertFalse(summary["smoke"]["ready"])
            self.assertFalse(summary["smoke"]["backend_host_compatible"])
            self.assertEqual(summary["smoke"]["backend_binary_format"], "elf")
            self.assertEqual(summary["smoke"]["backend_binary_architectures"], None)
            self.assertEqual(summary["smoke"]["backend_translation_required"], None)
            self.assertIn("ELF binary is not supported on Darwin", summary["issues"])
            self.assertTrue(summary["linux_handoff"]["ready"])
            self.assertIn("AWSIM_BIN", summary["linux_handoff"]["required_env_vars"])
            self.assertIn("HANDOFF_SCENARIO_PATH", summary["linux_handoff"]["required_env_vars"])
            self.assertIn("HELIOS_BIN", summary["linux_handoff"]["required_env_vars"])
            self.assertIn("renderer_backend_workflow_linux_handoff.sh", summary["recommended_next_command"])
            self.assertIn("renderer_backend_workflow_linux_handoff_pack.sh", summary["commands"]["linux_handoff_pack"])
            self.assertIn("renderer_backend_workflow_linux_handoff_unpack.sh", summary["commands"]["linux_handoff_unpack"])
            self.assertIn("renderer_backend_workflow_linux_handoff_docker.sh", summary["commands"]["linux_handoff_docker"])
            self.assertIn("run_renderer_backend_linux_handoff_docker.py", summary["commands"]["linux_handoff_docker_helper"])
            self.assertGreater(summary["linux_handoff"]["transfer_manifest"]["entry_count"], 0)
            self.assertGreater(summary["linux_handoff"]["transfer_manifest"]["packable_entry_count"], 0)
            blocker_codes = [entry["code"] for entry in summary["blockers"]]
            self.assertIn("BACKEND_HOST_INCOMPATIBLE", blocker_codes)
            self.assertNotIn("BACKEND_BIN_MISSING", blocker_codes)

    def test_workflow_main_can_pack_and_verify_linux_handoff_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            awsim_bin = root / "AWSIM-Demo.x86_64"
            awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            awsim_bin.chmod(0o755)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_BIN": str(fake_helios.resolve()),
                            "AWSIM_BIN": str(awsim_bin.resolve()),
                            "AWSIM_RENDERER_MAP": "Town09",
                        },
                        "readiness": {
                            "helios_ready": True,
                            "awsim_host_compatible": False,
                        },
                        "acquisition_hints": {
                            "awsim": {
                                "platform_supported": False,
                                "platform_note": "AWSIM quick-start docs assume Ubuntu 22.04 with NVIDIA RTX and driver 570+.",
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            config_path = self._write_base_config(
                root=root,
                survey=survey,
                helios_bin=fake_helios,
                output_dir=root / "smoke_base_output",
            )
            output_root = root / "workflow"

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_workflow._inspect_executable_host_compatibility",
                return_value={
                    "host_compatible": False,
                    "host_compatibility_reason": "ELF binary is not supported on Darwin",
                    "binary_format": "elf",
                    "file_description": "ELF 64-bit LSB executable",
                },
            ):
                with contextlib.redirect_stdout(io.StringIO()):
                    exit_code = workflow_main(
                        [
                            "--backend",
                            "awsim",
                            "--setup-summary",
                            str(setup_summary),
                            "--config",
                            str(config_path),
                            "--dry-run",
                            "--pack-linux-handoff",
                            "--verify-linux-handoff-bundle",
                            "--output-root",
                            str(output_root),
                        ]
                    )

            self.assertEqual(exit_code, 1)
            summary = json.loads(
                (output_root / "renderer_backend_workflow_summary.json").read_text(encoding="utf-8")
            )
            bundle_path = Path(summary["linux_handoff"]["bundle"]["bundle_path"])
            bundle_manifest_path = Path(summary["linux_handoff"]["bundle"]["bundle_manifest_path"])
            verification_manifest_path = Path(summary["linux_handoff"]["bundle"]["verification_manifest_path"])
            self.assertTrue(bundle_path.exists())
            self.assertTrue(bundle_manifest_path.exists())
            self.assertTrue(verification_manifest_path.exists())
            self.assertTrue(summary["linux_handoff"]["bundle"]["bundle_generated"])
            self.assertTrue(summary["linux_handoff"]["bundle"]["bundle_verified"])
            self.assertTrue(summary["linux_handoff"]["bundle"]["verification"]["verified"])
            self.assertGreater(
                summary["linux_handoff"]["transfer_manifest"]["verifiable_entry_count"],
                0,
            )

    def test_workflow_main_can_run_linux_handoff_in_docker_verify_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            awsim_bin = root / "AWSIM-Demo.x86_64"
            awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            awsim_bin.chmod(0o755)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_BIN": str(fake_helios.resolve()),
                            "AWSIM_BIN": str(awsim_bin.resolve()),
                            "AWSIM_RENDERER_MAP": "Town09",
                        },
                        "readiness": {
                            "helios_ready": True,
                            "awsim_host_compatible": False,
                        },
                        "probes": {
                            "linux_handoff_docker_selftest": {
                                "success": True,
                                "execute": True,
                                "marker_exists": True,
                                "marker_content": "selftest-ok",
                                "docker": {"return_code": 0},
                                "summary_path": str(root / "selftest_summary.json"),
                            }
                        },
                        "commands": {
                            "linux_handoff_docker_selftest": "python3 scripts/discover_renderer_backend_local_env.py --probe-linux-handoff-docker-selftest",
                        },
                    }
                ),
                encoding="utf-8",
            )
            config_path = self._write_base_config(
                root=root,
                survey=survey,
                helios_bin=fake_helios,
                output_dir=root / "smoke_base_output",
            )
            output_root = root / "workflow"

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_workflow._inspect_executable_host_compatibility",
                return_value={
                    "host_compatible": False,
                    "host_compatibility_reason": "ELF binary is not supported on Darwin",
                    "binary_format": "elf",
                    "binary_architectures": ["x86_64"],
                    "file_description": "ELF 64-bit LSB executable",
                },
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_workflow.run_renderer_backend_linux_handoff_in_docker",
                    return_value={
                        "return_code": 0,
                        "summary_path": str(output_root / "renderer_backend_linux_handoff_docker_run" / "renderer_backend_linux_handoff_docker_run.json"),
                    },
                ) as docker_run:
                    with contextlib.redirect_stdout(io.StringIO()):
                        exit_code = workflow_main(
                            [
                                "--backend",
                                "awsim",
                                "--setup-summary",
                                str(setup_summary),
                                "--config",
                                str(config_path),
                                "--dry-run",
                                "--run-linux-handoff-docker",
                                "--docker-binary",
                                "docker-test",
                                "--docker-image",
                                "python:3.11-slim",
                                "--docker-container-workspace",
                                "/repo",
                                "--output-root",
                                str(output_root),
                            ]
                        )

            self.assertEqual(exit_code, 0)
            self.assertTrue(docker_run.called)
            self.assertTrue(docker_run.call_args.kwargs["skip_run"])
            self.assertEqual(docker_run.call_args.kwargs["docker_binary"], "docker-test")
            self.assertEqual(docker_run.call_args.kwargs["docker_platform"], "linux/amd64")
            self.assertEqual(docker_run.call_args.kwargs["container_workspace"], "/repo")
            summary = json.loads(
                (output_root / "renderer_backend_workflow_summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(summary["status"], "HANDOFF_DOCKER_VERIFIED")
            self.assertTrue(summary["success"])
            self.assertTrue(summary["docker_handoff"]["requested"])
            self.assertTrue(summary["docker_handoff"]["executed"])
            self.assertEqual(summary["docker_handoff"]["return_code"], 0)
            self.assertEqual(summary["docker_handoff"]["docker_platform"], "linux/amd64")
            self.assertTrue(summary["docker_handoff"]["preflight"]["available"])
            self.assertTrue(summary["docker_handoff"]["preflight"]["success"])
            self.assertTrue(summary["linux_handoff"]["bundle"]["bundle_generated"])

    def test_workflow_main_propagates_linux_handoff_docker_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            awsim_bin = root / "AWSIM-Demo.x86_64"
            awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            awsim_bin.chmod(0o755)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_BIN": str(fake_helios.resolve()),
                            "AWSIM_BIN": str(awsim_bin.resolve()),
                            "AWSIM_RENDERER_MAP": "Town09",
                        },
                        "readiness": {
                            "helios_ready": True,
                            "awsim_host_compatible": False,
                        },
                    }
                ),
                encoding="utf-8",
            )
            config_path = self._write_base_config(
                root=root,
                survey=survey,
                helios_bin=fake_helios,
                output_dir=root / "smoke_base_output",
            )
            output_root = root / "workflow"

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_workflow._inspect_executable_host_compatibility",
                return_value={
                    "host_compatible": False,
                    "host_compatibility_reason": "ELF binary is not supported on Darwin",
                    "binary_format": "elf",
                    "file_description": "ELF 64-bit LSB executable",
                },
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_workflow.run_renderer_backend_linux_handoff_in_docker",
                    return_value={
                        "return_code": 9,
                        "launch_error": "docker failed",
                        "summary_path": str(output_root / "renderer_backend_linux_handoff_docker_run" / "renderer_backend_linux_handoff_docker_run.json"),
                    },
                ):
                    with contextlib.redirect_stdout(io.StringIO()):
                        exit_code = workflow_main(
                            [
                                "--backend",
                                "awsim",
                                "--setup-summary",
                                str(setup_summary),
                                "--config",
                                str(config_path),
                                "--dry-run",
                                "--run-linux-handoff-docker",
                                "--output-root",
                                str(output_root),
                            ]
                        )

            self.assertEqual(exit_code, 9)
            summary = json.loads(
                (output_root / "renderer_backend_workflow_summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(summary["status"], "HANDOFF_DOCKER_FAILED")
            self.assertFalse(summary["success"])
            self.assertEqual(summary["docker_handoff"]["return_code"], 9)

    def test_workflow_main_blocks_linux_handoff_docker_when_preflight_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            awsim_bin = root / "AWSIM-Demo.x86_64"
            awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            awsim_bin.chmod(0o755)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_BIN": str(fake_helios.resolve()),
                            "AWSIM_BIN": str(awsim_bin.resolve()),
                            "AWSIM_RENDERER_MAP": "Town09",
                        },
                        "readiness": {
                            "helios_ready": True,
                            "awsim_host_compatible": False,
                        },
                        "probes": {
                            "linux_handoff_docker_selftest": {
                                "success": False,
                                "execute": True,
                                "marker_exists": False,
                                "docker": {"return_code": 2},
                                "summary_path": str(root / "failed_selftest_summary.json"),
                            }
                        },
                        "commands": {
                            "linux_handoff_docker_selftest": "python3 scripts/discover_renderer_backend_local_env.py --probe-linux-handoff-docker-selftest",
                        },
                    }
                ),
                encoding="utf-8",
            )
            config_path = self._write_base_config(
                root=root,
                survey=survey,
                helios_bin=fake_helios,
                output_dir=root / "smoke_base_output",
            )
            output_root = root / "workflow"

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_workflow._inspect_executable_host_compatibility",
                return_value={
                    "host_compatible": False,
                    "host_compatibility_reason": "ELF binary is not supported on Darwin",
                    "binary_format": "elf",
                    "file_description": "ELF 64-bit LSB executable",
                },
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_workflow.run_renderer_backend_linux_handoff_in_docker",
                ) as docker_run:
                    with contextlib.redirect_stdout(io.StringIO()):
                        exit_code = workflow_main(
                            [
                                "--backend",
                                "awsim",
                                "--setup-summary",
                                str(setup_summary),
                                "--config",
                                str(config_path),
                                "--dry-run",
                                "--run-linux-handoff-docker",
                                "--output-root",
                                str(output_root),
                            ]
                        )

            self.assertEqual(exit_code, 1)
            self.assertFalse(docker_run.called)
            summary = json.loads(
                (output_root / "renderer_backend_workflow_summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(summary["status"], "HANDOFF_DOCKER_PREFLIGHT_FAILED")
            self.assertFalse(summary["success"])
            self.assertEqual(
                summary["recommended_next_command"],
                "python3 scripts/discover_renderer_backend_local_env.py --probe-linux-handoff-docker-selftest",
            )
            blocker_codes = [entry["code"] for entry in summary["blockers"]]
            self.assertIn("HANDOFF_DOCKER_PREFLIGHT_FAILED", blocker_codes)

    def test_workflow_main_blocks_linux_handoff_docker_when_preflight_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            awsim_bin = root / "AWSIM-Demo.x86_64"
            awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            awsim_bin.chmod(0o755)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_BIN": str(fake_helios.resolve()),
                            "AWSIM_BIN": str(awsim_bin.resolve()),
                            "AWSIM_RENDERER_MAP": "Town09",
                        },
                        "readiness": {
                            "helios_ready": True,
                            "awsim_host_compatible": False,
                        },
                        "probes": {
                            "linux_handoff_docker_selftest": {
                                "success": True,
                                "execute": False,
                                "marker_exists": False,
                                "generated_at_utc": _utc_iso(-7200),
                                "docker": {"return_code": 0},
                                "summary_path": str(root / "stale_selftest_summary.json"),
                            }
                        },
                        "commands": {
                            "linux_handoff_docker_selftest": "python3 scripts/discover_renderer_backend_local_env.py --probe-linux-handoff-docker-selftest",
                        },
                    }
                ),
                encoding="utf-8",
            )
            config_path = self._write_base_config(
                root=root,
                survey=survey,
                helios_bin=fake_helios,
                output_dir=root / "smoke_base_output",
            )
            output_root = root / "workflow"

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_workflow._inspect_executable_host_compatibility",
                return_value={
                    "host_compatible": False,
                    "host_compatibility_reason": "ELF binary is not supported on Darwin",
                    "binary_format": "elf",
                    "file_description": "ELF 64-bit LSB executable",
                },
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_workflow.run_renderer_backend_linux_handoff_in_docker",
                ) as docker_run:
                    with contextlib.redirect_stdout(io.StringIO()):
                        exit_code = workflow_main(
                            [
                                "--backend",
                                "awsim",
                                "--setup-summary",
                                str(setup_summary),
                                "--config",
                                str(config_path),
                                "--dry-run",
                                "--run-linux-handoff-docker",
                                "--docker-handoff-preflight-max-age-seconds",
                                "60",
                                "--output-root",
                                str(output_root),
                            ]
                        )

            self.assertEqual(exit_code, 1)
            self.assertFalse(docker_run.called)
            summary = json.loads(
                (output_root / "renderer_backend_workflow_summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(summary["status"], "HANDOFF_DOCKER_PREFLIGHT_STALE")
            self.assertFalse(summary["success"])
            self.assertTrue(summary["docker_handoff"]["preflight"]["stale"])
            self.assertEqual(
                summary["recommended_next_command"],
                "python3 scripts/discover_renderer_backend_local_env.py --probe-linux-handoff-docker-selftest",
            )
            blocker_codes = [entry["code"] for entry in summary["blockers"]]
            self.assertIn("HANDOFF_DOCKER_PREFLIGHT_STALE", blocker_codes)

    def test_workflow_main_refreshes_stale_docker_preflight_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            awsim_bin = root / "AWSIM-Demo.x86_64"
            awsim_bin.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            awsim_bin.chmod(0o755)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_BIN": str(fake_helios.resolve()),
                            "AWSIM_BIN": str(awsim_bin.resolve()),
                            "AWSIM_RENDERER_MAP": "Town09",
                        },
                        "readiness": {
                            "helios_ready": True,
                            "awsim_host_compatible": False,
                        },
                        "search_roots": [str(root)],
                        "probes": {
                            "linux_handoff_docker_selftest": {
                                "success": True,
                                "execute": False,
                                "generated_at_utc": _utc_iso(-7200),
                                "docker": {"return_code": 0},
                                "summary_path": str(root / "stale_selftest_summary.json"),
                            }
                        },
                        "commands": {
                            "linux_handoff_docker_selftest": "python3 scripts/discover_renderer_backend_local_env.py --probe-linux-handoff-docker-selftest",
                        },
                    }
                ),
                encoding="utf-8",
            )
            config_path = self._write_base_config(
                root=root,
                survey=survey,
                helios_bin=fake_helios,
                output_dir=root / "smoke_base_output",
            )
            output_root = root / "workflow"

            def _fake_refresh_setup(**kwargs: object) -> dict[str, object]:
                output_dir = Path(str(kwargs["output_dir"]))
                refreshed_summary_path = output_dir / "renderer_backend_local_setup.json"
                payload = {
                    "selection": {
                        "HELIOS_BIN": str(fake_helios.resolve()),
                        "AWSIM_BIN": str(awsim_bin.resolve()),
                        "AWSIM_RENDERER_MAP": "Town09",
                    },
                    "readiness": {
                        "helios_ready": True,
                        "awsim_host_compatible": False,
                    },
                    "probes": {
                        "linux_handoff_docker_selftest": {
                            "success": True,
                            "execute": False,
                            "generated_at_utc": _utc_iso(0),
                            "docker": {"return_code": 0},
                            "summary_path": str(output_dir / "linux_handoff_docker_selftest_probe" / "renderer_backend_linux_handoff_selftest.json"),
                        }
                    },
                    "commands": {
                        "linux_handoff_docker_selftest": "python3 scripts/discover_renderer_backend_local_env.py --probe-linux-handoff-docker-selftest",
                    },
                    "artifacts": {
                        "summary_path": str(refreshed_summary_path),
                        "env_path": str(output_dir / "renderer_backend_local.env.sh"),
                        "report_path": str(output_dir / "renderer_backend_local_report.md"),
                        "linux_handoff_docker_selftest_probe_path": str(
                            output_dir / "linux_handoff_docker_selftest_probe" / "renderer_backend_linux_handoff_selftest.json"
                        ),
                    },
                }
                refreshed_summary_path.parent.mkdir(parents=True, exist_ok=True)
                refreshed_summary_path.write_text(json.dumps(payload), encoding="utf-8")
                (output_dir / "renderer_backend_local.env.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
                return payload

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_workflow._inspect_executable_host_compatibility",
                return_value={
                    "host_compatible": False,
                    "host_compatibility_reason": "ELF binary is not supported on Darwin",
                    "binary_format": "elf",
                    "file_description": "ELF 64-bit LSB executable",
                },
            ):
                with patch(
                    "hybrid_sensor_sim.tools.renderer_backend_workflow.build_renderer_backend_local_setup",
                    side_effect=_fake_refresh_setup,
                ) as refresh_setup:
                    with patch(
                        "hybrid_sensor_sim.tools.renderer_backend_workflow.run_renderer_backend_linux_handoff_in_docker",
                        return_value={
                            "return_code": 0,
                            "summary_path": str(output_root / "renderer_backend_linux_handoff_docker_run" / "renderer_backend_linux_handoff_docker_run.json"),
                        },
                    ) as docker_run:
                        with contextlib.redirect_stdout(io.StringIO()):
                            exit_code = workflow_main(
                                [
                                    "--backend",
                                    "awsim",
                                    "--setup-summary",
                                    str(setup_summary),
                                    "--config",
                                    str(config_path),
                                    "--dry-run",
                                    "--run-linux-handoff-docker",
                                    "--refresh-docker-handoff-preflight",
                                    "--docker-handoff-preflight-max-age-seconds",
                                    "60",
                                    "--output-root",
                                    str(output_root),
                                ]
                            )

            self.assertEqual(exit_code, 0)
            self.assertEqual(refresh_setup.call_count, 1)
            self.assertTrue(refresh_setup.call_args.kwargs["probe_linux_handoff_docker_selftest"])
            self.assertFalse(refresh_setup.call_args.kwargs["probe_linux_handoff_docker_selftest_execute"])
            self.assertTrue(docker_run.called)
            summary = json.loads(
                (output_root / "renderer_backend_workflow_summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(summary["status"], "HANDOFF_DOCKER_VERIFIED")
            self.assertTrue(summary["docker_handoff"]["preflight"]["refreshed"])
            self.assertEqual(summary["docker_handoff"]["preflight"]["refresh_reason"], "stale")
            self.assertFalse(summary["docker_handoff"]["preflight"]["stale"])
            self.assertIsNotNone(summary["refreshed_setup"])

    def test_workflow_can_materialize_default_docker_preset_from_setup_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            awsim_bin = root / "AWSIM-Demo.x86_64"
            awsim_bin.write_bytes(b"\x7fELFdemo")
            awsim_bin.chmod(0o755)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_DOCKER_IMAGE": "heliosplusplus:cli",
                            "HELIOS_DOCKER_BINARY": "/home/jovyan/helios/build/helios++",
                            "AWSIM_BIN": str(awsim_bin.resolve()),
                            "AWSIM_RENDERER_MAP": "SampleMap",
                        },
                        "readiness": {
                            "helios_ready": True,
                        },
                        "probes": {
                            "linux_handoff_docker_selftest": {
                                "success": True,
                                "execute": True,
                                "marker_exists": True,
                                "docker": {"return_code": 0},
                                "summary_path": str(root / "selftest_summary.json"),
                            }
                        },
                        "commands": {
                            "linux_handoff_docker_selftest": "python3 scripts/discover_renderer_backend_local_env.py --probe-linux-handoff-docker-selftest",
                        },
                        "acquisition_hints": {
                            "awsim": {
                                "platform_supported": False,
                                "platform_note": "AWSIM quick-start docs assume Ubuntu 22.04 with NVIDIA RTX and driver 570+.",
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch(
                "hybrid_sensor_sim.tools.renderer_backend_workflow._inspect_executable_host_compatibility",
                return_value={
                    "host_compatible": False,
                    "host_compatibility_reason": "ELF binary is not supported on Darwin",
                    "binary_format": "elf",
                    "file_description": "ELF 64-bit LSB executable",
                },
            ):
                summary = build_renderer_backend_workflow(
                    backend="awsim",
                    repo_root=Path(__file__).resolve().parents[1],
                    workflow_root=root / "workflow",
                    setup_summary_path=setup_summary,
                    config_path=None,
                    dry_run=True,
                    run_linux_handoff_docker=True,
                )

            self.assertTrue(summary["smoke"]["planned_effective_config_ready"])
            self.assertIsNone(summary["smoke"]["planned_effective_config_error"])
            self.assertTrue(summary["docker_handoff"]["preflight"]["available"])
            self.assertEqual(summary["smoke"]["planned_effective_config"]["options"]["awsim_bin"], str(awsim_bin.resolve()))

    def test_workflow_main_writes_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            setup_summary = root / "renderer_backend_local_setup.json"
            setup_summary.write_text(
                json.dumps(
                    {
                        "selection": {
                            "HELIOS_DOCKER_IMAGE": "heliosplusplus:cli",
                            "HELIOS_DOCKER_BINARY": "/home/jovyan/helios/build/helios++",
                            "AWSIM_BIN": None,
                        },
                        "readiness": {
                            "helios_ready": True,
                        },
                    }
                ),
                encoding="utf-8",
            )
            output_root = root / "workflow"

            with contextlib.redirect_stdout(io.StringIO()) as stdout:
                exit_code = workflow_main(
                    [
                        "--backend",
                        "awsim",
                        "--setup-summary",
                        str(setup_summary),
                        "--dry-run",
                        "--output-root",
                        str(output_root),
                    ]
                )

            self.assertEqual(exit_code, 1)
            summary_path = output_root / "renderer_backend_workflow_summary.json"
            env_path = output_root / "renderer_backend_workflow.env.sh"
            report_path = output_root / "renderer_backend_workflow_report.md"
            next_step_path = output_root / "renderer_backend_workflow_next_step.sh"
            smoke_config_path = output_root / "renderer_backend_workflow_smoke_config.json"
            rerun_smoke_path = output_root / "renderer_backend_workflow_rerun_smoke.sh"
            linux_handoff_config_path = output_root / "renderer_backend_workflow_linux_handoff_config.json"
            linux_handoff_env_path = output_root / "renderer_backend_workflow_linux_handoff.env.sh"
            linux_handoff_script_path = output_root / "renderer_backend_workflow_linux_handoff.sh"
            linux_handoff_docker_script_path = output_root / "renderer_backend_workflow_linux_handoff_docker.sh"
            linux_handoff_transfer_manifest_path = (
                output_root / "renderer_backend_workflow_linux_handoff_transfer_manifest.json"
            )
            linux_handoff_pack_script_path = output_root / "renderer_backend_workflow_linux_handoff_pack.sh"
            linux_handoff_unpack_script_path = output_root / "renderer_backend_workflow_linux_handoff_unpack.sh"
            self.assertTrue(summary_path.exists())
            self.assertTrue(env_path.exists())
            self.assertTrue(report_path.exists())
            self.assertTrue(next_step_path.exists())
            self.assertTrue(smoke_config_path.exists())
            self.assertTrue(rerun_smoke_path.exists())
            self.assertTrue(linux_handoff_config_path.exists())
            self.assertTrue(linux_handoff_env_path.exists())
            self.assertTrue(linux_handoff_script_path.exists())
            self.assertTrue(linux_handoff_docker_script_path.exists())
            self.assertTrue(linux_handoff_transfer_manifest_path.exists())
            self.assertTrue(linux_handoff_pack_script_path.exists())
            self.assertTrue(linux_handoff_unpack_script_path.exists())
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            env_text = env_path.read_text(encoding="utf-8")
            report_text = report_path.read_text(encoding="utf-8")
            next_step_text = next_step_path.read_text(encoding="utf-8")
            smoke_config = json.loads(smoke_config_path.read_text(encoding="utf-8"))
            rerun_smoke_text = rerun_smoke_path.read_text(encoding="utf-8")
            linux_handoff_config = json.loads(linux_handoff_config_path.read_text(encoding="utf-8"))
            linux_handoff_transfer_manifest = json.loads(
                linux_handoff_transfer_manifest_path.read_text(encoding="utf-8")
            )
            linux_handoff_env_text = linux_handoff_env_path.read_text(encoding="utf-8")
            linux_handoff_script_text = linux_handoff_script_path.read_text(encoding="utf-8")
            linux_handoff_docker_script_text = linux_handoff_docker_script_path.read_text(encoding="utf-8")
            linux_handoff_pack_script_text = linux_handoff_pack_script_path.read_text(encoding="utf-8")
            linux_handoff_unpack_script_text = linux_handoff_unpack_script_path.read_text(encoding="utf-8")
            self.assertEqual(payload["status"], "DRY_RUN_BLOCKED")
            self.assertIsNone(payload["refreshed_setup"])
            blocker_codes = [entry["code"] for entry in payload["blockers"]]
            self.assertIn("AWSIM_BIN", env_text)
            self.assertIn("Renderer Backend Workflow Report", report_text)
            self.assertIn("Blockers", report_text)
            self.assertIn("acquire_renderer_backend_package.py", next_step_text)
            self.assertIn("error", smoke_config)
            self.assertIn("Workflow smoke config is not ready yet", rerun_smoke_text)
            self.assertIn("HANDOFF_SMOKE_CONFIG_PATH", linux_handoff_env_text)
            self.assertIn("run_renderer_backend_smoke.py", linux_handoff_script_text)
            self.assertIn("run_renderer_backend_linux_handoff_docker.py", linux_handoff_docker_script_text)
            self.assertIn("HANDOFF_SKIP_RUN", linux_handoff_docker_script_text)
            self.assertIn("HANDOFF_TRANSFER_MANIFEST_PATH", linux_handoff_pack_script_text)
            self.assertIn("HANDOFF_SKIP_RUN", linux_handoff_unpack_script_text)
            self.assertIn("verifiable_entries", linux_handoff_unpack_script_text)
            self.assertIn("error", linux_handoff_config)
            self.assertFalse(payload["linux_handoff"]["ready"])
            self.assertGreaterEqual(linux_handoff_transfer_manifest["entry_count"], 3)
            self.assertGreaterEqual(linux_handoff_transfer_manifest["verifiable_entry_count"], 3)
            self.assertIn("bundle_manifest_path", linux_handoff_transfer_manifest)
            self.assertIn("Linux Runner Handoff", report_text)
            self.assertIn("SMOKE_CONFIG_UNRESOLVED", blocker_codes)
            self.assertTrue(os.access(next_step_path, os.X_OK))
            self.assertTrue(os.access(rerun_smoke_path, os.X_OK))
            self.assertTrue(os.access(linux_handoff_script_path, os.X_OK))
            self.assertTrue(os.access(linux_handoff_docker_script_path, os.X_OK))
            self.assertTrue(os.access(linux_handoff_pack_script_path, os.X_OK))
            self.assertTrue(os.access(linux_handoff_unpack_script_path, os.X_OK))
            self.assertIn("renderer_backend_workflow_summary.json", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
