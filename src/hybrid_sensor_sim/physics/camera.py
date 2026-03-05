from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CameraIntrinsics:
    fx: float
    fy: float
    cx: float
    cy: float
    width: int = 1920
    height: int = 1080


@dataclass(frozen=True)
class BrownConradyDistortion:
    k1: float = 0.0
    k2: float = 0.0
    p1: float = 0.0
    p2: float = 0.0
    k3: float = 0.0


def project_points_brown_conrady(
    points_xyz: list[tuple[float, float, float]],
    intrinsics: CameraIntrinsics,
    distortion: BrownConradyDistortion,
    clamp_to_image: bool = True,
) -> list[tuple[float, float, float]]:
    projections: list[tuple[float, float, float]] = []
    for x, y, z in points_xyz:
        if z <= 0.0:
            continue

        xn = x / z
        yn = y / z
        r2 = xn * xn + yn * yn
        r4 = r2 * r2
        r6 = r4 * r2

        radial = 1.0 + distortion.k1 * r2 + distortion.k2 * r4 + distortion.k3 * r6
        x_tangential = 2.0 * distortion.p1 * xn * yn + distortion.p2 * (r2 + 2.0 * xn * xn)
        y_tangential = distortion.p1 * (r2 + 2.0 * yn * yn) + 2.0 * distortion.p2 * xn * yn

        xd = xn * radial + x_tangential
        yd = yn * radial + y_tangential

        u = intrinsics.fx * xd + intrinsics.cx
        v = intrinsics.fy * yd + intrinsics.cy
        if clamp_to_image:
            if u < 0.0 or v < 0.0 or u >= intrinsics.width or v >= intrinsics.height:
                continue
        projections.append((u, v, z))

    return projections

