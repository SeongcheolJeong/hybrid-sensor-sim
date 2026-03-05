from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TrajectoryPose:
    x: float
    y: float
    z: float
    time_s: float
    roll_deg: float
    pitch_deg: float
    yaw_deg: float


def read_trajectory_poses(path: Path, max_rows: int | None = None) -> list[TrajectoryPose]:
    poses: list[TrajectoryPose] = []
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            parts = stripped.split()
            if len(parts) < 6:
                continue
            try:
                values = [float(token) for token in parts[:7]]
            except ValueError:
                continue

            if len(values) >= 7:
                x, y, z, time_s, roll_deg, pitch_deg, yaw_deg = values[:7]
            else:
                x, y, z, roll_deg, pitch_deg, yaw_deg = values[:6]
                time_s = 0.0
            poses.append(
                TrajectoryPose(
                    x=x,
                    y=y,
                    z=z,
                    time_s=time_s,
                    roll_deg=roll_deg,
                    pitch_deg=pitch_deg,
                    yaw_deg=yaw_deg,
                )
            )
            if max_rows is not None and len(poses) >= max_rows:
                break
    return poses

