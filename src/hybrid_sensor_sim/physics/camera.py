from __future__ import annotations

from dataclasses import dataclass
from math import atan2, cos, pi, sin, sqrt


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


@dataclass(frozen=True)
class CameraExtrinsics:
    tx: float = 0.0
    ty: float = 0.0
    tz: float = 0.0
    roll_deg: float = 0.0
    pitch_deg: float = 0.0
    yaw_deg: float = 0.0
    enabled: bool = False


def _deg_to_rad(value: float) -> float:
    return value * pi / 180.0


def transform_points_world_to_camera(
    points_xyz: list[tuple[float, float, float]],
    extrinsics: CameraExtrinsics,
) -> list[tuple[float, float, float]]:
    if not extrinsics.enabled:
        return points_xyz

    roll = _deg_to_rad(extrinsics.roll_deg)
    pitch = _deg_to_rad(extrinsics.pitch_deg)
    yaw = _deg_to_rad(extrinsics.yaw_deg)

    cr = cos(roll)
    sr = sin(roll)
    cp = cos(pitch)
    sp = sin(pitch)
    cy = cos(yaw)
    sy = sin(yaw)

    # ZYX order: R = Rz(yaw) * Ry(pitch) * Rx(roll)
    r00 = cy * cp
    r01 = cy * sp * sr - sy * cr
    r02 = cy * sp * cr + sy * sr
    r10 = sy * cp
    r11 = sy * sp * sr + cy * cr
    r12 = sy * sp * cr - cy * sr
    r20 = -sp
    r21 = cp * sr
    r22 = cp * cr

    tx, ty, tz = extrinsics.tx, extrinsics.ty, extrinsics.tz
    transformed: list[tuple[float, float, float]] = []
    for x, y, z in points_xyz:
        x_local = x - tx
        y_local = y - ty
        z_local = z - tz
        x_cam = r00 * x_local + r01 * y_local + r02 * z_local
        y_cam = r10 * x_local + r11 * y_local + r12 * z_local
        z_cam = r20 * x_local + r21 * y_local + r22 * z_local
        transformed.append((x_cam, y_cam, z_cam))
    return transformed


def project_points_brown_conrady(
    points_xyz: list[tuple[float, float, float]],
    intrinsics: CameraIntrinsics,
    distortion: BrownConradyDistortion,
    geometry_model: str = "pinhole",
    clamp_to_image: bool = True,
) -> list[tuple[float, float, float]]:
    projections: list[tuple[float, float, float]] = []
    for x, y, z in points_xyz:
        normalized = _project_to_normalized_image_plane(
            x=x,
            y=y,
            z=z,
            geometry_model=geometry_model,
        )
        if normalized is None:
            continue
        xn, yn = normalized

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


def _project_to_normalized_image_plane(
    *,
    x: float,
    y: float,
    z: float,
    geometry_model: str,
) -> tuple[float, float] | None:
    geometry = geometry_model.lower().strip()

    if geometry in {"pinhole", "rectilinear"}:
        if z <= 0.0:
            return None
        return (x / z, y / z)

    if geometry in {"equidistant", "fisheye", "f-theta"}:
        if z <= 0.0:
            return None
        radial_norm = sqrt(x * x + y * y)
        if radial_norm <= 1e-12:
            return (0.0, 0.0)
        theta = atan2(radial_norm, z)
        scale = theta / radial_norm
        return (x * scale, y * scale)

    if geometry == "orthographic":
        if z <= 0.0:
            return None
        return (x, y)

    raise ValueError(f"Unsupported camera geometry model: {geometry_model}")
