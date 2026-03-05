from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.io.trajectory_txt import read_trajectory_poses


class TrajectoryIoTests(unittest.TestCase):
    def test_read_trajectory_poses_parses_expected_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "trajectory.txt"
            path.write_text(
                "100.0 200.0 300.0 1.5 0.1 0.2 90.0\n"
                "110.0 210.0 310.0 2.5 1.1 1.2 91.0\n",
                encoding="utf-8",
            )
            poses = read_trajectory_poses(path)
            self.assertEqual(len(poses), 2)
            self.assertAlmostEqual(poses[0].x, 100.0)
            self.assertAlmostEqual(poses[0].time_s, 1.5)
            self.assertAlmostEqual(poses[0].yaw_deg, 90.0)
            self.assertAlmostEqual(poses[1].pitch_deg, 1.2)


if __name__ == "__main__":
    unittest.main()

