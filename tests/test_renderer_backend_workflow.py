from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from hybrid_sensor_sim.tools.renderer_backend_workflow import (
    build_renderer_backend_workflow,
    main as workflow_main,
)


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
                        "commands": {
                            "awsim_acquire": "python3 scripts/acquire_renderer_backend_package.py --backend awsim",
                        },
                    }
                ),
                encoding="utf-8",
            )

            summary = build_renderer_backend_workflow(
                backend="awsim",
                repo_root=root / "repo",
                workflow_root=root / "workflow",
                setup_summary_path=setup_summary,
                dry_run=True,
            )

            self.assertEqual(summary["status"], "DRY_RUN_BLOCKED")
            self.assertFalse(summary["success"])
            self.assertIn("AWSIM_BIN is not resolved", "\n".join(summary["issues"]))
            self.assertEqual(summary["recommended_next_command"], summary["commands"]["acquire"])
            self.assertEqual(summary["final_selection"]["AWSIM_BIN"], None)

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
            self.assertTrue(summary["final_selection"]["AWSIM_BIN"])
            self.assertEqual(summary["final_selection"]["AWSIM_RENDERER_MAP"], "Town12")

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
            self.assertTrue(summary_path.exists())
            self.assertTrue(env_path.exists())
            self.assertTrue(report_path.exists())
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            env_text = env_path.read_text(encoding="utf-8")
            report_text = report_path.read_text(encoding="utf-8")
            self.assertEqual(payload["status"], "DRY_RUN_BLOCKED")
            self.assertIn("AWSIM_BIN", env_text)
            self.assertIn("Renderer Backend Workflow Report", report_text)
            self.assertIn("renderer_backend_workflow_summary.json", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
