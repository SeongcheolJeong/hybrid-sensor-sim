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


def write_xyz_points(
    path: Path,
    points: list[tuple[float, float, float]],
    decimals: int = 6,
) -> None:
    lines = []
    fmt = "{:." + str(decimals) + "f}"
    for x, y, z in points:
        lines.append(f"{fmt.format(x)} {fmt.format(y)} {fmt.format(z)}")
    payload = "\n".join(lines)
    if payload:
        payload += "\n"
    path.write_text(payload, encoding="utf-8")
