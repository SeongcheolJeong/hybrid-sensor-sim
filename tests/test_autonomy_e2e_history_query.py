from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.autonomy_e2e_history_query import (
    query_by_block_id,
    query_by_current_path,
    query_by_project_id,
)


class AutonomyE2EHistoryQueryTests(unittest.TestCase):
    def test_query_checked_in_metadata_by_project_block_and_path(self) -> None:
        metadata_root = Path(__file__).resolve().parents[1] / "metadata" / "autonomy_e2e"
        if not metadata_root.is_dir():
            self.skipTest("checked-in provenance metadata not generated yet")

        project_result = query_by_project_id(
            metadata_root=metadata_root,
            project_id="P_Sim-Engine",
        )
        self.assertEqual(project_result["project"]["project_id"], "P_Sim-Engine")
        self.assertTrue(project_result["blocks"])

        block_result = query_by_block_id(
            metadata_root=metadata_root,
            block_id="p_sim_engine.vehicle_dynamics",
        )
        self.assertEqual(block_result["block"]["block_id"], "p_sim_engine.vehicle_dynamics")
        self.assertIn(
            "src/hybrid_sensor_sim/physics/vehicle_dynamics.py",
            block_result["block"]["current_paths"],
        )

        current_path_result = query_by_current_path(
            metadata_root=metadata_root,
            current_path="src/hybrid_sensor_sim/physics/vehicle_dynamics.py",
        )
        self.assertEqual(
            current_path_result["traceability_entry"]["current_path"],
            "src/hybrid_sensor_sim/physics/vehicle_dynamics.py",
        )
        self.assertIn(
            "p_sim_engine.vehicle_dynamics",
            current_path_result["traceability_entry"]["block_ids"],
        )

    def test_query_script_bootstraps_src_path(self) -> None:
        metadata_root = Path(__file__).resolve().parents[1] / "metadata" / "autonomy_e2e"
        script_path = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "run_autonomy_e2e_history_query.py"
        )
        completed = subprocess.run(
            [
                "python3",
                str(script_path),
                "--metadata-root",
                str(metadata_root),
                "--project-id",
                "P_Sim-Engine",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if not metadata_root.is_dir():
            self.assertNotEqual(completed.returncode, 0)
            return
        self.assertEqual(completed.returncode, 0)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["project"]["project_id"], "P_Sim-Engine")


if __name__ == "__main__":
    unittest.main()
