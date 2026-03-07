from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.renderer_backend_smoke import main as smoke_main


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


class RendererBackendSmokeTests(unittest.TestCase):
    def _write_base_config(
        self,
        *,
        root: Path,
        survey: Path,
        helios_bin: Path,
        output_dir: Path,
    ) -> Path:
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
                    "seed": 7,
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

    def test_renderer_backend_smoke_main_writes_success_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            smoke_output = root / "smoke_success"
            config_path = self._write_base_config(
                root=root,
                survey=survey,
                helios_bin=fake_helios,
                output_dir=smoke_output,
            )

            fake_backend = root / "fake_backend_success.sh"
            fake_backend.write_text(
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
                encoding="utf-8",
            )
            fake_backend.chmod(0o755)

            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = smoke_main(
                    [
                        "--config",
                        str(config_path),
                        "--backend",
                        "awsim",
                        "--backend-bin",
                        str(fake_backend),
                        "--output-dir",
                        str(smoke_output),
                        "--renderer-map",
                        "Town12",
                        "--set-option",
                        "camera_projection_preview_count=5",
                    ]
                )

            self.assertEqual(exit_code, 0)
            summary = json.loads(
                (smoke_output / "renderer_backend_smoke_summary.json").read_text(
                    encoding="utf-8"
                )
            )
            markdown_report = (smoke_output / "renderer_backend_smoke_report.md").resolve()
            html_report = (smoke_output / "renderer_backend_smoke_report.html").resolve()
            markdown_text = markdown_report.read_text(encoding="utf-8")
            html_text = html_report.read_text(encoding="utf-8")
            effective_config = json.loads(
                (smoke_output / "renderer_backend_smoke_config.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertTrue(summary["success"])
            self.assertEqual(summary["backend"], "awsim")
            self.assertEqual(summary["run"]["status"], "EXECUTION_SUCCEEDED")
            self.assertEqual(summary["output_inspection"]["status"], "MATCHED")
            self.assertEqual(summary["runner_smoke"]["status"], "SMOKE_SUCCEEDED")
            self.assertEqual(summary["output_comparison"]["status"], "MATCHED")
            self.assertIsNotNone(summary["comparison_table"])
            self.assertEqual(summary["comparison_table"]["sensor_status_counts"]["MATCHED"], 1)
            self.assertEqual(summary["comparison_table"]["role_status_counts"]["MATCHED"], 1)
            self.assertEqual(summary["comparison_table"]["role_rows"][0]["output_role"], "camera_visible")
            self.assertEqual(summary["comparison_table"]["role_rows"][0]["status"], "MATCHED")
            self.assertEqual(summary["forced_options"]["renderer_backend"], "awsim")
            self.assertEqual(summary["reports"]["markdown"], str(markdown_report))
            self.assertEqual(summary["reports"]["html"], str(html_report))
            self.assertIn("# Renderer Backend Smoke Report", markdown_text)
            self.assertIn("camera_visible", markdown_text)
            self.assertIn("MATCHED", markdown_text)
            self.assertIn("<h1>Renderer Backend Smoke Report</h1>", html_text)
            self.assertIn("camera_visible", html_text)
            self.assertIn("MATCHED", html_text)
            self.assertEqual(
                effective_config["options"]["renderer_execute_and_inspect_via_runner"],
                True,
            )
            self.assertEqual(effective_config["options"]["renderer_fail_on_error"], True)
            self.assertEqual(effective_config["options"]["renderer_map"], "Town12")
            self.assertEqual(
                effective_config["options"]["camera_projection_preview_count"],
                5,
            )
            self.assertIn("backend_runner_smoke_manifest", summary["artifacts"])

    def test_renderer_backend_smoke_main_surfaces_contract_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            fake_helios = root / "fake_helios.sh"
            _write_fake_helios_script(fake_helios)
            smoke_output = root / "smoke_failure"
            config_path = self._write_base_config(
                root=root,
                survey=survey,
                helios_bin=fake_helios,
                output_dir=smoke_output,
            )

            fake_backend = root / "fake_backend_failure.sh"
            fake_backend.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
mkdir -p "${BACKEND_OUTPUT_ROOT}"
printf '{"status":"ok"}\n' > "${BACKEND_OUTPUT_ROOT}/carla_runtime_state.json"
""",
                encoding="utf-8",
            )
            fake_backend.chmod(0o755)

            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = smoke_main(
                    [
                        "--config",
                        str(config_path),
                        "--backend",
                        "carla",
                        "--backend-bin",
                        str(fake_backend),
                        "--output-dir",
                        str(smoke_output),
                    ]
                )

            self.assertEqual(exit_code, 1)
            summary = json.loads(
                (smoke_output / "renderer_backend_smoke_summary.json").read_text(
                    encoding="utf-8"
                )
            )
            markdown_report = (smoke_output / "renderer_backend_smoke_report.md").resolve()
            html_report = (smoke_output / "renderer_backend_smoke_report.html").resolve()
            markdown_text = markdown_report.read_text(encoding="utf-8")
            html_text = html_report.read_text(encoding="utf-8")
            self.assertFalse(summary["success"])
            self.assertEqual(summary["backend"], "carla")
            self.assertEqual(summary["run"]["status"], "EXECUTION_FAILED")
            self.assertEqual(summary["run"]["failure_reason"], "OUTPUT_CONTRACT_MISMATCH")
            self.assertEqual(summary["output_inspection"]["status"], "MISSING_EXPECTED")
            self.assertEqual(summary["runner_smoke"]["status"], "INSPECTION_FAILED")
            self.assertEqual(summary["output_comparison"]["status"], "MISSING_EXPECTED")
            self.assertIsNotNone(summary["comparison_table"])
            self.assertEqual(
                summary["comparison_table"]["sensor_status_counts"]["MISSING_EXPECTED"],
                1,
            )
            self.assertEqual(
                summary["comparison_table"]["role_status_counts"]["MISSING_EXPECTED"],
                1,
            )
            self.assertEqual(
                summary["comparison_table"]["mismatch_reason_counts"]["MISSING_EXPECTED_OUTPUTS"],
                2,
            )
            self.assertEqual(
                summary["comparison_table"]["role_rows"][0]["output_role"],
                "camera_visible",
            )
            self.assertEqual(
                summary["comparison_table"]["role_rows"][0]["status"],
                "MISSING_EXPECTED",
            )
            self.assertEqual(summary["reports"]["markdown"], str(markdown_report))
            self.assertEqual(summary["reports"]["html"], str(html_report))
            self.assertIn("OUTPUT_CONTRACT_MISMATCH", markdown_text)
            self.assertIn("MISSING_EXPECTED", markdown_text)
            self.assertIn("camera_visible", markdown_text)
            self.assertIn("OUTPUT_CONTRACT_MISMATCH", html_text)
            self.assertIn("MISSING_EXPECTED", html_text)
            self.assertIn("camera_visible", html_text)


if __name__ == "__main__":
    unittest.main()
