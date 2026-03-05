from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()

