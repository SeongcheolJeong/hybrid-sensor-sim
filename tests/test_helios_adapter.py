from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from hybrid_sensor_sim.backends.helios_adapter import HeliosAdapter
from hybrid_sensor_sim.types import SensorSimRequest


class HeliosAdapterTests(unittest.TestCase):
    def _make_request(self, root: Path, **options: object) -> SensorSimRequest:
        scenario = root / "survey.xml"
        scenario.write_text("<document></document>", encoding="utf-8")
        output_dir = root / "out"
        return SensorSimRequest(
            scenario_path=scenario,
            output_dir=output_dir,
            seed=123,
            options=options,
        )

    def test_generates_command_plan_without_execution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_helios = root / "fake_helios.sh"
            fake_helios.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            fake_helios.chmod(0o755)

            assets_dir = root / "assets"
            assets_dir.mkdir()

            request = self._make_request(
                root,
                execute_helios=False,
                assets_paths=[str(assets_dir)],
                las_output=True,
                zip_output=True,
                nthreads=2,
            )
            adapter = HeliosAdapter(helios_bin=fake_helios)
            result = adapter.simulate(request)
            self.assertTrue(result.success)
            self.assertIn("execution_plan", result.artifacts)

            plan = json.loads(result.artifacts["execution_plan"].read_text(encoding="utf-8"))
            command = plan["command"]
            self.assertIn("--output", command)
            self.assertIn("--assets", command)
            self.assertIn("--lasOutput", command)
            self.assertIn("--zipOutput", command)
            self.assertIn("--nthreads", command)

    def test_executes_and_discovers_primary_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_helios = root / "fake_helios.sh"
            fake_helios.write_text(
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
touch "${rootdir}/scan_points.xyz"
touch "${rootdir}/scan_trajectory.txt"
touch "${rootdir}/scan_fullwave.txt"
""",
                encoding="utf-8",
            )
            fake_helios.chmod(0o755)

            request = self._make_request(root, execute_helios=True)
            adapter = HeliosAdapter(helios_bin=fake_helios)
            result = adapter.simulate(request)

            self.assertTrue(result.success)
            self.assertIn("point_cloud_primary", result.artifacts)
            self.assertTrue(result.artifacts["point_cloud_primary"].exists())
            self.assertEqual(result.metrics.get("point_cloud_file_count"), 1.0)
            self.assertEqual(result.metrics.get("trajectory_file_count"), 1.0)
            self.assertEqual(result.metrics.get("fullwave_file_count"), 1.0)
            self.assertIn("output_manifest", result.artifacts)

    def test_returns_failure_for_missing_binary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            request = self._make_request(root, execute_helios=True)
            adapter = HeliosAdapter(helios_bin=root / "missing_binary")
            result = adapter.simulate(request)
            self.assertFalse(result.success)
            self.assertIn("execution_plan", result.artifacts)

    def test_auto_runtime_reports_binary_and_docker_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            request = self._make_request(
                root,
                execute_helios=True,
                helios_runtime="auto",
                helios_docker_image="helios:test",
            )
            adapter = HeliosAdapter(helios_bin=root / "missing_binary")
            with mock.patch.object(
                HeliosAdapter,
                "_docker_daemon_available",
                return_value=(False, "daemon down"),
            ):
                result = adapter.simulate(request)
            self.assertFalse(result.success)
            self.assertIn("binary unavailable", result.message)
            self.assertIn("daemon down", result.message)

    def test_docker_runtime_generates_plan_with_docker_command(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp:
            root = Path(tmp)
            survey = root / "survey.xml"
            survey.write_text("<document></document>", encoding="utf-8")
            assets = root / "assets"
            assets.mkdir(parents=True, exist_ok=True)

            request = self._make_request(
                root,
                execute_helios=False,
                helios_runtime="docker",
                helios_docker_image="helios:test",
                helios_docker_binary="helios++",
                helios_cwd=str(root),
                survey_path=str(survey),
                assets_paths=[str(assets)],
            )
            adapter = HeliosAdapter(helios_bin=root / "missing_binary")
            with mock.patch.object(
                HeliosAdapter,
                "_docker_daemon_available",
                return_value=(True, ""),
            ):
                result = adapter.simulate(request)

            self.assertTrue(result.success)
            plan = json.loads(result.artifacts["execution_plan"].read_text(encoding="utf-8"))
            command = plan["command"]
            self.assertGreater(len(command), 5)
            self.assertEqual(command[0], "docker")
            self.assertEqual(command[1], "run")
            self.assertIn("helios:test", command)
            self.assertIn("--output", command)

    def test_docker_dry_run_does_not_require_daemon(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp:
            root = Path(tmp)
            request = self._make_request(
                root,
                execute_helios=False,
                helios_runtime="docker",
                helios_docker_image="helios:test",
                helios_cwd=str(root),
                survey_path=str(root / "survey.xml"),
            )
            adapter = HeliosAdapter(helios_bin=root / "missing_binary")
            with mock.patch.object(
                HeliosAdapter,
                "_docker_daemon_available",
                return_value=(False, "daemon down"),
            ):
                result = adapter.simulate(request)
            self.assertTrue(result.success)
            self.assertIn("execution_plan", result.artifacts)

    def test_generates_survey_from_scenario_json_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scenario = root / "scenario.json"
            scenario.write_text(
                """
{
  "name": "gen-survey",
  "objects": [
    {
      "id": "ego",
      "type": "vehicle",
      "pose": [0.0, 0.0, 0.0],
      "waypoints": [[1.0, 2.0, 3.0]]
    }
  ]
}
""".strip(),
                encoding="utf-8",
            )
            output_dir = root / "out"
            fake_helios = root / "fake_helios.sh"
            fake_helios.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            fake_helios.chmod(0o755)

            request = SensorSimRequest(
                scenario_path=scenario,
                output_dir=output_dir,
                seed=5,
                options={
                    "execute_helios": False,
                    "survey_generate_from_scenario": True,
                },
            )
            adapter = HeliosAdapter(helios_bin=fake_helios)
            result = adapter.simulate(request)
            self.assertTrue(result.success)
            self.assertIn("generated_survey", result.artifacts)
            generated = result.artifacts["generated_survey"]
            self.assertTrue(generated.exists())
            self.assertEqual(generated.suffix.lower(), ".xml")

            plan = json.loads(result.artifacts["execution_plan"].read_text(encoding="utf-8"))
            self.assertTrue(plan["survey_generated_from_scenario"])
            self.assertEqual(plan["generated_survey_path"], str(generated))
            self.assertIn(str(generated), plan["command"])


if __name__ == "__main__":
    unittest.main()
