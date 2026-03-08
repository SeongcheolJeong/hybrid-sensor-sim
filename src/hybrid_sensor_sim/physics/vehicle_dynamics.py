from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Mapping

VEHICLE_PROFILE_SCHEMA_VERSION_V0 = "vehicle_profile_v0"
CONTROL_SEQUENCE_SCHEMA_VERSION_V0 = "control_sequence_v0"
VEHICLE_DYNAMICS_TRACE_SCHEMA_VERSION_V0 = "vehicle_dynamics_trace_v0"


def _load_float(value: Any, *, field: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a number") from exc


def _parse_bool(value: Any, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if float(value) == 1.0:
            return True
        if float(value) == 0.0:
            return False
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off", ""}:
        return False
    raise ValueError(f"{field} must be a boolean")


def validate_vehicle_profile(payload: dict[str, Any]) -> dict[str, float]:
    if str(payload.get("profile_schema_version", "")) != VEHICLE_PROFILE_SCHEMA_VERSION_V0:
        raise ValueError(
            "profile_schema_version must be "
            f"{VEHICLE_PROFILE_SCHEMA_VERSION_V0}"
        )

    required = ["wheelbase_m", "max_accel_mps2", "max_decel_mps2", "max_speed_mps"]
    missing = [key for key in required if key not in payload]
    if missing:
        raise ValueError(f"vehicle profile missing required keys: {missing}")

    wheelbase = _load_float(payload["wheelbase_m"], field="wheelbase_m")
    max_accel = _load_float(payload["max_accel_mps2"], field="max_accel_mps2")
    max_decel = _load_float(payload["max_decel_mps2"], field="max_decel_mps2")
    max_speed = _load_float(payload["max_speed_mps"], field="max_speed_mps")
    mass_kg = _load_float(payload.get("mass_kg", 1500.0), field="mass_kg")
    rolling_resistance_coeff = _load_float(
        payload.get("rolling_resistance_coeff", 0.0),
        field="rolling_resistance_coeff",
    )
    drag_coefficient = _load_float(payload.get("drag_coefficient", 0.0), field="drag_coefficient")
    frontal_area_m2 = _load_float(payload.get("frontal_area_m2", 2.2), field="frontal_area_m2")
    air_density_kgpm3 = _load_float(
        payload.get("air_density_kgpm3", 1.225),
        field="air_density_kgpm3",
    )
    front_axle_to_cg_m = _load_float(
        payload.get("front_axle_to_cg_m", wheelbase / 2.0),
        field="front_axle_to_cg_m",
    )
    rear_axle_to_cg_m = _load_float(
        payload.get("rear_axle_to_cg_m", wheelbase - front_axle_to_cg_m),
        field="rear_axle_to_cg_m",
    )
    yaw_inertia_kgm2 = _load_float(
        payload.get(
            "yaw_inertia_kgm2",
            max(
                1.0,
                mass_kg
                * (
                    front_axle_to_cg_m * front_axle_to_cg_m
                    + rear_axle_to_cg_m * rear_axle_to_cg_m
                )
                / 2.0,
            ),
        ),
        field="yaw_inertia_kgm2",
    )
    cornering_stiffness_front_nprad = _load_float(
        payload.get("cornering_stiffness_front_nprad", 80000.0),
        field="cornering_stiffness_front_nprad",
    )
    cornering_stiffness_rear_nprad = _load_float(
        payload.get("cornering_stiffness_rear_nprad", 80000.0),
        field="cornering_stiffness_rear_nprad",
    )
    tire_friction_coeff = _load_float(
        payload.get("tire_friction_coeff", 1.0),
        field="tire_friction_coeff",
    )

    if wheelbase <= 0:
        raise ValueError("wheelbase_m must be > 0")
    if max_accel <= 0 or max_decel <= 0 or max_speed <= 0:
        raise ValueError("max_accel_mps2/max_decel_mps2/max_speed_mps must be > 0")
    if mass_kg <= 0:
        raise ValueError("mass_kg must be > 0")
    if rolling_resistance_coeff < 0:
        raise ValueError("rolling_resistance_coeff must be >= 0")
    if drag_coefficient < 0:
        raise ValueError("drag_coefficient must be >= 0")
    if frontal_area_m2 <= 0:
        raise ValueError("frontal_area_m2 must be > 0")
    if air_density_kgpm3 <= 0:
        raise ValueError("air_density_kgpm3 must be > 0")
    if front_axle_to_cg_m <= 0:
        raise ValueError("front_axle_to_cg_m must be > 0")
    if rear_axle_to_cg_m <= 0:
        raise ValueError("rear_axle_to_cg_m must be > 0")
    if yaw_inertia_kgm2 <= 0:
        raise ValueError("yaw_inertia_kgm2 must be > 0")
    if cornering_stiffness_front_nprad <= 0:
        raise ValueError("cornering_stiffness_front_nprad must be > 0")
    if cornering_stiffness_rear_nprad <= 0:
        raise ValueError("cornering_stiffness_rear_nprad must be > 0")
    if tire_friction_coeff <= 0:
        raise ValueError("tire_friction_coeff must be > 0")
    if abs((front_axle_to_cg_m + rear_axle_to_cg_m) - wheelbase) > 1e-6:
        raise ValueError("front_axle_to_cg_m + rear_axle_to_cg_m must equal wheelbase_m")

    return {
        "wheelbase_m": wheelbase,
        "max_accel_mps2": max_accel,
        "max_decel_mps2": max_decel,
        "max_speed_mps": max_speed,
        "mass_kg": mass_kg,
        "rolling_resistance_coeff": rolling_resistance_coeff,
        "drag_coefficient": drag_coefficient,
        "frontal_area_m2": frontal_area_m2,
        "air_density_kgpm3": air_density_kgpm3,
        "front_axle_to_cg_m": front_axle_to_cg_m,
        "rear_axle_to_cg_m": rear_axle_to_cg_m,
        "yaw_inertia_kgm2": yaw_inertia_kgm2,
        "cornering_stiffness_front_nprad": cornering_stiffness_front_nprad,
        "cornering_stiffness_rear_nprad": cornering_stiffness_rear_nprad,
        "tire_friction_coeff": tire_friction_coeff,
    }


def validate_control_sequence(
    payload: dict[str, Any],
) -> tuple[
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    bool,
    bool,
    list[dict[str, float | None]],
]:
    if str(payload.get("sequence_schema_version", "")) != CONTROL_SEQUENCE_SCHEMA_VERSION_V0:
        raise ValueError(
            "sequence_schema_version must be "
            f"{CONTROL_SEQUENCE_SCHEMA_VERSION_V0}"
        )

    dt_sec = _load_float(payload.get("dt_sec", 0.0), field="dt_sec")
    if dt_sec <= 0:
        raise ValueError("dt_sec must be > 0")

    initial_speed_mps = _load_float(payload.get("initial_speed_mps", 0.0), field="initial_speed_mps")
    initial_position_m = _load_float(payload.get("initial_position_m", 0.0), field="initial_position_m")
    initial_heading_deg = _load_float(payload.get("initial_heading_deg", 0.0), field="initial_heading_deg")
    initial_lateral_position_m = _load_float(
        payload.get("initial_lateral_position_m", 0.0),
        field="initial_lateral_position_m",
    )
    initial_lateral_velocity_mps = _load_float(
        payload.get("initial_lateral_velocity_mps", 0.0),
        field="initial_lateral_velocity_mps",
    )
    initial_yaw_rate_rps = _load_float(
        payload.get("initial_yaw_rate_rps", 0.0),
        field="initial_yaw_rate_rps",
    )
    enable_planar_kinematics = _parse_bool(
        payload.get("enable_planar_kinematics", False),
        field="enable_planar_kinematics",
    )
    enable_dynamic_bicycle = _parse_bool(
        payload.get("enable_dynamic_bicycle", False),
        field="enable_dynamic_bicycle",
    )
    if initial_speed_mps < 0:
        raise ValueError("initial_speed_mps must be >= 0")
    if enable_dynamic_bicycle and not enable_planar_kinematics:
        raise ValueError("enable_dynamic_bicycle requires enable_planar_kinematics=true")

    default_target_speed_mps_raw = payload.get("default_target_speed_mps", None)
    default_target_speed_mps: float | None = None
    if default_target_speed_mps_raw is not None:
        default_target_speed_mps = _load_float(
            default_target_speed_mps_raw,
            field="default_target_speed_mps",
        )
        if default_target_speed_mps < 0:
            raise ValueError("default_target_speed_mps must be >= 0")
    default_road_grade_percent = _load_float(
        payload.get("default_road_grade_percent", 0.0),
        field="default_road_grade_percent",
    )
    default_surface_friction_scale = _load_float(
        payload.get("default_surface_friction_scale", 1.0),
        field="default_surface_friction_scale",
    )
    if default_road_grade_percent <= -100 or default_road_grade_percent >= 100:
        raise ValueError("default_road_grade_percent must be between -100 and 100")
    if default_surface_friction_scale <= 0:
        raise ValueError("default_surface_friction_scale must be > 0")

    commands = payload.get("commands", [])
    if not isinstance(commands, list) or len(commands) == 0:
        raise ValueError("commands must be a non-empty list")

    normalized: list[dict[str, float | None]] = []
    for idx, cmd in enumerate(commands):
        if not isinstance(cmd, dict):
            raise ValueError(f"commands[{idx}] must be an object")
        throttle = _load_float(cmd.get("throttle", 0.0), field=f"commands[{idx}].throttle")
        brake = _load_float(cmd.get("brake", 0.0), field=f"commands[{idx}].brake")
        steering_angle_deg = _load_float(
            cmd.get("steering_angle_deg", 0.0),
            field=f"commands[{idx}].steering_angle_deg",
        )
        target_speed_mps_raw = cmd.get("target_speed_mps", default_target_speed_mps)
        target_speed_mps: float | None = None
        if target_speed_mps_raw is not None:
            target_speed_mps = _load_float(
                target_speed_mps_raw,
                field=f"commands[{idx}].target_speed_mps",
            )
            if target_speed_mps < 0:
                raise ValueError(f"commands[{idx}].target_speed_mps must be >= 0")
        road_grade_percent = _load_float(
            cmd.get("road_grade_percent", default_road_grade_percent),
            field=f"commands[{idx}].road_grade_percent",
        )
        surface_friction_scale = _load_float(
            cmd.get("surface_friction_scale", default_surface_friction_scale),
            field=f"commands[{idx}].surface_friction_scale",
        )
        if throttle < 0 or brake < 0:
            raise ValueError(f"commands[{idx}] throttle/brake must be >= 0")
        if abs(steering_angle_deg) >= 89.9:
            raise ValueError(f"commands[{idx}].steering_angle_deg magnitude must be < 89.9")
        if road_grade_percent <= -100 or road_grade_percent >= 100:
            raise ValueError(f"commands[{idx}].road_grade_percent must be between -100 and 100")
        if surface_friction_scale <= 0:
            raise ValueError(f"commands[{idx}].surface_friction_scale must be > 0")
        normalized.append(
            {
                "throttle": min(throttle, 1.0),
                "brake": min(brake, 1.0),
                "steering_angle_deg": steering_angle_deg,
                "road_grade_percent": road_grade_percent,
                "surface_friction_scale": surface_friction_scale,
                "target_speed_mps": target_speed_mps,
            }
        )

    return (
        dt_sec,
        initial_position_m,
        initial_speed_mps,
        initial_heading_deg,
        initial_lateral_position_m,
        initial_lateral_velocity_mps,
        initial_yaw_rate_rps,
        enable_planar_kinematics,
        enable_dynamic_bicycle,
        normalized,
    )


def simulate_vehicle_dynamics_step(
    *,
    vehicle_profile: Mapping[str, float],
    dt_sec: float,
    position_m: float,
    speed_mps: float,
    heading_deg: float = 0.0,
    lateral_position_m: float = 0.0,
    lateral_velocity_mps: float = 0.0,
    yaw_rate_rps: float = 0.0,
    enable_planar_kinematics: bool = False,
    enable_dynamic_bicycle: bool = False,
    throttle: float = 0.0,
    brake: float = 0.0,
    steering_angle_deg: float = 0.0,
    road_grade_percent: float = 0.0,
    surface_friction_scale: float = 1.0,
    target_speed_mps: float | None = None,
) -> dict[str, float | bool | None]:
    if dt_sec <= 0:
        raise ValueError("dt_sec must be > 0")
    if surface_friction_scale <= 0:
        raise ValueError("surface_friction_scale must be > 0")
    if road_grade_percent <= -100 or road_grade_percent >= 100:
        raise ValueError("road_grade_percent must be between -100 and 100")
    if enable_dynamic_bicycle and not enable_planar_kinematics:
        raise ValueError("enable_dynamic_bicycle requires enable_planar_kinematics=true")

    max_speed = float(vehicle_profile["max_speed_mps"])
    max_accel = float(vehicle_profile["max_accel_mps2"])
    max_decel = float(vehicle_profile["max_decel_mps2"])
    wheelbase_m = float(vehicle_profile["wheelbase_m"])
    mass_kg = float(vehicle_profile["mass_kg"])
    rolling_resistance_coeff = float(vehicle_profile["rolling_resistance_coeff"])
    drag_coefficient = float(vehicle_profile["drag_coefficient"])
    frontal_area_m2 = float(vehicle_profile["frontal_area_m2"])
    air_density_kgpm3 = float(vehicle_profile["air_density_kgpm3"])
    front_axle_to_cg_m = float(vehicle_profile["front_axle_to_cg_m"])
    rear_axle_to_cg_m = float(vehicle_profile["rear_axle_to_cg_m"])
    yaw_inertia_kgm2 = float(vehicle_profile["yaw_inertia_kgm2"])
    cornering_stiffness_front_nprad = float(vehicle_profile["cornering_stiffness_front_nprad"])
    cornering_stiffness_rear_nprad = float(vehicle_profile["cornering_stiffness_rear_nprad"])
    tire_friction_coeff = float(vehicle_profile["tire_friction_coeff"])

    clamped_speed_mps = max(0.0, min(max_speed, float(speed_mps)))
    clamped_throttle = max(0.0, min(1.0, float(throttle)))
    clamped_brake = max(0.0, min(1.0, float(brake)))
    position_m = float(position_m)
    heading_rad = math.radians(float(heading_deg))
    x_m = position_m
    y_m = float(lateral_position_m)
    lateral_velocity_mps = float(lateral_velocity_mps)
    yaw_rate_rps = float(yaw_rate_rps)
    gravity_mps2 = 9.80665
    normalized_target_speed_mps = None
    if target_speed_mps is not None:
        normalized_target_speed_mps = max(0.0, min(max_speed, float(target_speed_mps)))

    tractive_force_n = clamped_throttle * max_accel * mass_kg
    brake_force_n = clamped_brake * max_decel * mass_kg
    longitudinal_wheel_force_n = tractive_force_n - brake_force_n
    rolling_force_n = rolling_resistance_coeff * mass_kg * gravity_mps2 if clamped_speed_mps > 0 else 0.0
    drag_force_n = 0.5 * air_density_kgpm3 * drag_coefficient * frontal_area_m2 * clamped_speed_mps * clamped_speed_mps
    slope_angle_rad = math.atan(road_grade_percent / 100.0)
    normal_force_n = mass_kg * gravity_mps2 * max(0.0, math.cos(slope_angle_rad))
    effective_friction_coeff = tire_friction_coeff * surface_friction_scale
    tire_force_limit_n = max(0.0, effective_friction_coeff * normal_force_n)
    longitudinal_wheel_force_limited_n = max(
        -tire_force_limit_n,
        min(tire_force_limit_n, longitudinal_wheel_force_n),
    )
    longitudinal_force_limited = (
        abs(longitudinal_wheel_force_limited_n - longitudinal_wheel_force_n) > 1e-9
    )
    grade_force_n = mass_kg * gravity_mps2 * math.sin(slope_angle_rad)
    resistive_force_n = rolling_force_n + drag_force_n + grade_force_n
    net_force_n = longitudinal_wheel_force_limited_n - resistive_force_n
    accel_mps2 = net_force_n / mass_kg
    next_speed_mps = max(0.0, min(max_speed, clamped_speed_mps + accel_mps2 * dt_sec))
    next_position_m = position_m + (next_speed_mps * dt_sec)

    yaw_accel_rps2 = 0.0
    lateral_accel_mps2 = 0.0
    next_heading_rad = heading_rad
    next_x_m = next_position_m
    next_y_m = y_m
    next_lateral_velocity_mps = lateral_velocity_mps
    next_yaw_rate_rps = yaw_rate_rps
    if enable_planar_kinematics:
        steering_angle_rad = math.radians(float(steering_angle_deg))
        if enable_dynamic_bicycle:
            if next_speed_mps > 0.5:
                speed_for_lateral = next_speed_mps
                slip_front_rad = steering_angle_rad - (
                    (lateral_velocity_mps + front_axle_to_cg_m * yaw_rate_rps) / speed_for_lateral
                )
                slip_rear_rad = -(
                    (lateral_velocity_mps - rear_axle_to_cg_m * yaw_rate_rps) / speed_for_lateral
                )
                slip_front_rad = max(-0.7, min(0.7, slip_front_rad))
                slip_rear_rad = max(-0.7, min(0.7, slip_rear_rad))
                lateral_force_front_n = cornering_stiffness_front_nprad * slip_front_rad
                lateral_force_rear_n = cornering_stiffness_rear_nprad * slip_rear_rad
                lateral_force_abs_total_n = abs(lateral_force_front_n) + abs(lateral_force_rear_n)
                longitudinal_utilization = (
                    abs(longitudinal_wheel_force_limited_n) / tire_force_limit_n
                    if tire_force_limit_n > 0.0
                    else 1.0
                )
                longitudinal_utilization = max(0.0, min(1.0, longitudinal_utilization))
                lateral_force_limit_n = tire_force_limit_n * math.sqrt(
                    max(0.0, 1.0 - longitudinal_utilization * longitudinal_utilization)
                )
                if lateral_force_abs_total_n > lateral_force_limit_n and lateral_force_abs_total_n > 0.0:
                    scale = lateral_force_limit_n / lateral_force_abs_total_n
                    lateral_force_front_n *= scale
                    lateral_force_rear_n *= scale
                lateral_accel_mps2 = (
                    (lateral_force_front_n + lateral_force_rear_n) / mass_kg
                    - speed_for_lateral * yaw_rate_rps
                )
                yaw_accel_rps2 = (
                    front_axle_to_cg_m * lateral_force_front_n
                    - rear_axle_to_cg_m * lateral_force_rear_n
                ) / yaw_inertia_kgm2
            else:
                lateral_accel_mps2 = -0.5 * lateral_velocity_mps
                yaw_accel_rps2 = -0.5 * yaw_rate_rps
            next_lateral_velocity_mps = lateral_velocity_mps + lateral_accel_mps2 * dt_sec
            next_yaw_rate_rps = yaw_rate_rps + yaw_accel_rps2 * dt_sec
        else:
            next_yaw_rate_rps = (next_speed_mps / wheelbase_m) * math.tan(steering_angle_rad)
            next_lateral_velocity_mps = 0.0
        next_heading_rad = heading_rad + next_yaw_rate_rps * dt_sec
        x_dot_mps = next_speed_mps * math.cos(next_heading_rad) - next_lateral_velocity_mps * math.sin(next_heading_rad)
        y_dot_mps = next_speed_mps * math.sin(next_heading_rad) + next_lateral_velocity_mps * math.cos(next_heading_rad)
        next_x_m = x_m + x_dot_mps * dt_sec
        next_y_m = y_m + y_dot_mps * dt_sec
    else:
        next_yaw_rate_rps = 0.0
        next_lateral_velocity_mps = 0.0

    speed_tracking_error_mps = (
        next_speed_mps - normalized_target_speed_mps if normalized_target_speed_mps is not None else None
    )

    return {
        "throttle": clamped_throttle,
        "brake": clamped_brake,
        "target_speed_mps": normalized_target_speed_mps,
        "speed_tracking_error_mps": speed_tracking_error_mps,
        "steering_angle_deg": float(steering_angle_deg),
        "yaw_rate_rps": next_yaw_rate_rps,
        "yaw_accel_rps2": yaw_accel_rps2,
        "lateral_velocity_mps": next_lateral_velocity_mps,
        "lateral_accel_mps2": lateral_accel_mps2,
        "accel_mps2": accel_mps2,
        "tractive_force_n": tractive_force_n,
        "brake_force_n": brake_force_n,
        "longitudinal_wheel_force_n": longitudinal_wheel_force_n,
        "longitudinal_wheel_force_limited_n": longitudinal_wheel_force_limited_n,
        "longitudinal_force_limited": bool(longitudinal_force_limited),
        "rolling_force_n": rolling_force_n,
        "drag_force_n": drag_force_n,
        "grade_force_n": grade_force_n,
        "resistive_force_n": resistive_force_n,
        "net_force_n": net_force_n,
        "normal_force_n": normal_force_n,
        "tire_force_limit_n": tire_force_limit_n,
        "effective_friction_coeff": effective_friction_coeff,
        "surface_friction_scale": surface_friction_scale,
        "road_grade_percent": road_grade_percent,
        "speed_mps": next_speed_mps,
        "position_m": next_position_m,
        "heading_deg": math.degrees(next_heading_rad),
        "x_m": next_x_m,
        "y_m": next_y_m,
    }


def simulate_vehicle_dynamics(
    *,
    vehicle_profile: dict[str, float],
    dt_sec: float,
    initial_position_m: float,
    initial_speed_mps: float,
    initial_heading_deg: float,
    initial_lateral_position_m: float,
    initial_lateral_velocity_mps: float,
    initial_yaw_rate_rps: float,
    enable_planar_kinematics: bool,
    enable_dynamic_bicycle: bool,
    commands: list[dict[str, float | None]],
) -> dict[str, Any]:
    speed_mps = max(0.0, min(float(vehicle_profile["max_speed_mps"]), float(initial_speed_mps)))
    position_m = float(initial_position_m)
    heading_rad = math.radians(float(initial_heading_deg))
    x_m = float(initial_position_m)
    y_m = float(initial_lateral_position_m)
    lateral_velocity_mps = float(initial_lateral_velocity_mps)
    yaw_rate_rps = float(initial_yaw_rate_rps)
    trace: list[dict[str, float | bool | None]] = []

    for idx, cmd in enumerate(commands):
        target_speed_mps_raw = cmd.get("target_speed_mps")
        step = simulate_vehicle_dynamics_step(
            vehicle_profile=vehicle_profile,
            dt_sec=dt_sec,
            position_m=position_m,
            speed_mps=speed_mps,
            heading_deg=math.degrees(heading_rad),
            lateral_position_m=y_m,
            lateral_velocity_mps=lateral_velocity_mps,
            yaw_rate_rps=yaw_rate_rps,
            enable_planar_kinematics=enable_planar_kinematics,
            enable_dynamic_bicycle=enable_dynamic_bicycle,
            throttle=float(cmd["throttle"]),
            brake=float(cmd["brake"]),
            steering_angle_deg=float(cmd.get("steering_angle_deg", 0.0)),
            road_grade_percent=float(cmd.get("road_grade_percent", 0.0)),
            surface_friction_scale=float(cmd.get("surface_friction_scale", 1.0)),
            target_speed_mps=None if target_speed_mps_raw is None else float(target_speed_mps_raw),
        )
        speed_mps = float(step["speed_mps"])
        position_m = float(step["position_m"])
        heading_rad = math.radians(float(step["heading_deg"]))
        x_m = float(step["x_m"])
        y_m = float(step["y_m"])
        lateral_velocity_mps = float(step["lateral_velocity_mps"])
        yaw_rate_rps = float(step["yaw_rate_rps"])
        trace.append(
            {
                "step": float(idx),
                "throttle": round(float(step["throttle"]), 6),
                "brake": round(float(step["brake"]), 6),
                "target_speed_mps": (
                    round(float(step["target_speed_mps"]), 6)
                    if step["target_speed_mps"] is not None
                    else None
                ),
                "speed_tracking_error_mps": (
                    round(float(step["speed_tracking_error_mps"]), 6)
                    if step["speed_tracking_error_mps"] is not None
                    else None
                ),
                "steering_angle_deg": round(float(step["steering_angle_deg"]), 6),
                "yaw_rate_rps": round(float(step["yaw_rate_rps"]), 6),
                "yaw_accel_rps2": round(float(step["yaw_accel_rps2"]), 6),
                "lateral_velocity_mps": round(float(step["lateral_velocity_mps"]), 6),
                "lateral_accel_mps2": round(float(step["lateral_accel_mps2"]), 6),
                "accel_mps2": round(float(step["accel_mps2"]), 6),
                "tractive_force_n": round(float(step["tractive_force_n"]), 6),
                "brake_force_n": round(float(step["brake_force_n"]), 6),
                "longitudinal_wheel_force_n": round(float(step["longitudinal_wheel_force_n"]), 6),
                "longitudinal_wheel_force_limited_n": round(float(step["longitudinal_wheel_force_limited_n"]), 6),
                "longitudinal_force_limited": bool(step["longitudinal_force_limited"]),
                "rolling_force_n": round(float(step["rolling_force_n"]), 6),
                "drag_force_n": round(float(step["drag_force_n"]), 6),
                "grade_force_n": round(float(step["grade_force_n"]), 6),
                "resistive_force_n": round(float(step["resistive_force_n"]), 6),
                "net_force_n": round(float(step["net_force_n"]), 6),
                "normal_force_n": round(float(step["normal_force_n"]), 6),
                "tire_force_limit_n": round(float(step["tire_force_limit_n"]), 6),
                "effective_friction_coeff": round(float(step["effective_friction_coeff"]), 6),
                "surface_friction_scale": round(float(step["surface_friction_scale"]), 6),
                "road_grade_percent": round(float(step["road_grade_percent"]), 6),
                "speed_mps": round(float(step["speed_mps"]), 6),
                "position_m": round(float(step["position_m"]), 6),
                "heading_deg": round(float(step["heading_deg"]), 6),
                "x_m": round(float(step["x_m"]), 6),
                "y_m": round(float(step["y_m"]), 6),
            }
        )

    speed_tracking_error_values = [
        float(row["speed_tracking_error_mps"])
        for row in trace
        if row.get("speed_tracking_error_mps") is not None
    ]
    speed_tracking_target_step_count = len(speed_tracking_error_values)
    speed_tracking_error_mps_min = min(speed_tracking_error_values) if speed_tracking_error_values else 0.0
    speed_tracking_error_mps_avg = (
        sum(speed_tracking_error_values) / float(speed_tracking_target_step_count)
        if speed_tracking_error_values
        else 0.0
    )
    speed_tracking_error_mps_max = max(speed_tracking_error_values) if speed_tracking_error_values else 0.0
    speed_tracking_error_abs_mps_avg = (
        sum(abs(value) for value in speed_tracking_error_values) / float(speed_tracking_target_step_count)
        if speed_tracking_error_values
        else 0.0
    )
    speed_tracking_error_abs_mps_max = (
        max(abs(value) for value in speed_tracking_error_values) if speed_tracking_error_values else 0.0
    )

    return {
        "vehicle_dynamics_trace_schema_version": VEHICLE_DYNAMICS_TRACE_SCHEMA_VERSION_V0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "vehicle_dynamics_model": (
            "planar_dynamic_bicycle_force_balance_v1"
            if enable_dynamic_bicycle
            else (
                "planar_bicycle_force_balance_v1"
                if enable_planar_kinematics
                else "longitudinal_force_balance_v1"
            )
        ),
        "planar_kinematics_enabled": bool(enable_planar_kinematics),
        "dynamic_bicycle_enabled": bool(enable_dynamic_bicycle),
        "step_count": len(trace),
        "initial_speed_mps": round(max(0.0, float(initial_speed_mps)), 6),
        "initial_position_m": round(float(initial_position_m), 6),
        "initial_heading_deg": round(float(initial_heading_deg), 6),
        "initial_lateral_position_m": round(float(initial_lateral_position_m), 6),
        "initial_lateral_velocity_mps": round(float(initial_lateral_velocity_mps), 6),
        "initial_yaw_rate_rps": round(float(initial_yaw_rate_rps), 6),
        "final_speed_mps": round(speed_mps, 6),
        "final_position_m": round(position_m, 6),
        "final_heading_deg": round(math.degrees(heading_rad), 6),
        "final_lateral_position_m": round(y_m, 6),
        "final_lateral_velocity_mps": round(lateral_velocity_mps, 6),
        "final_yaw_rate_rps": round(yaw_rate_rps, 6),
        "final_x_m": round(x_m, 6),
        "final_y_m": round(y_m, 6),
        "speed_tracking_target_step_count": int(speed_tracking_target_step_count),
        "speed_tracking_error_mps_min": round(speed_tracking_error_mps_min, 6),
        "speed_tracking_error_mps_avg": round(speed_tracking_error_mps_avg, 6),
        "speed_tracking_error_mps_max": round(speed_tracking_error_mps_max, 6),
        "speed_tracking_error_abs_mps_avg": round(speed_tracking_error_abs_mps_avg, 6),
        "speed_tracking_error_abs_mps_max": round(speed_tracking_error_abs_mps_max, 6),
        "trace": trace,
    }
