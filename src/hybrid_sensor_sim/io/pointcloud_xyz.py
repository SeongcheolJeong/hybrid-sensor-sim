from __future__ import annotations

from pathlib import Path


def read_xyz_points(path: Path, max_points: int = 5000) -> list[tuple[float, float, float]]:
    points: list[tuple[float, float, float]] = []
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            parts = stripped.split()
            if len(parts) < 3:
                continue
            try:
                x, y, z = float(parts[0]), float(parts[1]), float(parts[2])
            except ValueError:
                continue
            points.append((x, y, z))
            if len(points) >= max_points:
                break
    return points

