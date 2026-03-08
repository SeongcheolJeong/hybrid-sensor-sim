from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from hybrid_sensor_sim.physics.vehicle_dynamics import (
    VEHICLE_PROFILE_SCHEMA_VERSION_V0,
    simulate_vehicle_dynamics_step,
)
from hybrid_sensor_sim.scenarios.schema import ActorState, ScenarioConfig


@dataclass(frozen=True)
class ObjectSimRunResult:
    summary: dict[str, Any]
    trace_rows: list[dict[str, Any]]
    lane_risk_summary: dict[str, Any]


class CoreSimRunner:
    def __init__(self, scenario: ScenarioConfig, seed: int) -> None:
        self.scenario = scenario
        self.seed = seed
        self.rng = random.Random(seed)
        self.time_sec = 0.0
        self.step_count = 0
        self.min_ttc_same_lane_sec = float("inf")
        self.min_ttc_adjacent_lane_sec = float("inf")
        self.min_ttc_path_conflict_sec = float("inf")
        self.min_ttc_sec = float("inf")
        self.collision = False
        self.timeout = False

        self.ego = ActorState(**vars(scenario.ego))
        self.npcs = [ActorState(**vars(npc)) for npc in scenario.npcs]
        if scenario.npc_speed_jitter_mps > 0:
            jitter_bound = float(scenario.npc_speed_jitter_mps)
            for index, npc in enumerate(self.npcs):
                jitter = self.rng.uniform(-jitter_bound, jitter_bound)
                self.npcs[index] = ActorState(
                    actor_id=npc.actor_id,
                    position_m=npc.position_m,
                    speed_mps=npc.speed_mps + jitter,
                    length_m=npc.length_m,
                    lane_index=npc.lane_index,
                    lane_id=npc.lane_id,
                    lane_binding_mode=npc.lane_binding_mode,
                )

        self.trace_rows: list[dict[str, Any]] = []
        self.ego_avoidance_brake_event_count = 0
        self.ego_avoidance_applied_brake_mps2_max = 0.0
        self.ego_avoidance_trigger_counts_by_interaction_kind: dict[str, int] = {}
        self.ego_avoidance_last_trigger_actor_id: str | None = None
        self.ego_avoidance_last_trigger_interaction_kind: str | None = None
        self.ego_avoidance_last_trigger_route_relation: str | None = None
        self.ego_avoidance_last_trigger_ttc_sec: float | None = None
        self.ego_avoidance_last_trigger_gap_m: float | None = None
        self.ego_avoidance_last_trigger_ttc_threshold_sec: float | None = None
        self.ego_avoidance_last_trigger_brake_scale: float | None = None
        self.ego_dynamics_longitudinal_force_limited_event_count = 0
        self.route_lane_ids = (
            [str(item) for item in scenario.map_context.route_report.get("route_lane_ids", [])]
            if scenario.map_context is not None and scenario.map_context.route_report is not None
            else []
        )
        self.route_lane_order_by_id = {
            lane_id: idx for idx, lane_id in enumerate(self.route_lane_ids)
        }

    def run(self) -> dict[str, Any]:
        started_wall = time.perf_counter()
        started_at = datetime.now(timezone.utc).isoformat()

        while self.time_sec < self.scenario.duration_sec and not self.collision and not self.timeout:
            if self.scenario.wall_timeout_sec is not None:
                elapsed_wall = time.perf_counter() - started_wall
                if elapsed_wall > self.scenario.wall_timeout_sec:
                    self.timeout = True
                    break
            self._step()

        finished_wall = time.perf_counter()
        finished_at = datetime.now(timezone.utc).isoformat()

        if self.collision:
            status = "failed"
            termination_reason = "collision"
        elif self.timeout:
            status = "timeout"
            termination_reason = "timeout"
        else:
            status = "success"
            termination_reason = "completed"

        min_ttc_same_lane = (
            None if self.min_ttc_same_lane_sec == float("inf") else round(self.min_ttc_same_lane_sec, 6)
        )
        min_ttc_adjacent_lane = (
            None if self.min_ttc_adjacent_lane_sec == float("inf") else round(self.min_ttc_adjacent_lane_sec, 6)
        )
        min_ttc_path_conflict = (
            None if self.min_ttc_path_conflict_sec == float("inf") else round(self.min_ttc_path_conflict_sec, 6)
        )
        finite_ttc_values = [
            value
            for value in (
                self.min_ttc_same_lane_sec,
                self.min_ttc_adjacent_lane_sec,
                self.min_ttc_path_conflict_sec,
            )
            if value != float("inf")
        ]
        min_ttc_any_lane = None if not finite_ttc_values else round(min(finite_ttc_values), 6)

        return {
            "scenario_schema_version": self.scenario.scenario_schema_version,
            "scenario_id": self.scenario.scenario_id,
            "status": status,
            "termination_reason": termination_reason,
            "seed": self.seed,
            "step_count": self.step_count,
            "sim_duration_sec": round(self.time_sec, 6),
            "wall_time_sec": round(finished_wall - started_wall, 6),
            "min_ttc_sec": min_ttc_same_lane,
            "min_ttc_same_lane_sec": min_ttc_same_lane,
            "min_ttc_adjacent_lane_sec": min_ttc_adjacent_lane,
            "min_ttc_path_conflict_sec": min_ttc_path_conflict,
            "min_ttc_any_lane_sec": min_ttc_any_lane,
            "collision": self.collision,
            "timeout": self.timeout,
            "started_at": started_at,
            "finished_at": finished_at,
        }

    def _step(self) -> None:
        dt = self.scenario.dt_sec
        self.time_sec += dt
        self.step_count += 1
        avoidance_action = self._apply_ego_collision_avoidance(dt)
        ego_dynamics_step = self._update_ego(dt, avoidance_action)
        updated_npcs: list[ActorState] = []
        for npc in self.npcs:
            updated_npcs.append(
                ActorState(
                    actor_id=npc.actor_id,
                    position_m=npc.position_m + (npc.speed_mps * dt),
                    speed_mps=npc.speed_mps,
                    length_m=npc.length_m,
                    lane_index=npc.lane_index,
                    lane_id=npc.lane_id,
                    lane_binding_mode=npc.lane_binding_mode,
                )
            )
        self.npcs = updated_npcs

        for npc in self.npcs:
            gap_m = npc.position_m - self.ego.position_m - 0.5 * (npc.length_m + self.ego.length_m)
            rel_speed_mps = self.ego.speed_mps - npc.speed_mps
            ttc_sec = None
            ttc_same_lane_sec = None
            ttc_adjacent_lane_sec = None
            route_ttc_sec = None
            path_ttc_sec = None
            lane_delta = abs(int(npc.lane_index) - int(self.ego.lane_index))
            same_lane = bool(npc.lane_index == self.ego.lane_index)
            adjacent_lane = bool(lane_delta == 1)
            ego_route_lane_order, npc_route_lane_order, route_lane_delta, route_relation = self._classify_route_relation(
                npc
            )
            path_conflict, path_conflict_source, path_interaction_kind = self._classify_path_interaction(
                same_lane=same_lane,
                adjacent_lane=adjacent_lane,
                route_relation=route_relation,
            )

            if path_conflict and gap_m <= 0:
                self.collision = True
            elif rel_speed_mps > 0 and (same_lane or adjacent_lane):
                ttc_value = gap_m / rel_speed_mps
                if same_lane:
                    self.min_ttc_same_lane_sec = min(self.min_ttc_same_lane_sec, ttc_value)
                    self.min_ttc_sec = min(self.min_ttc_sec, ttc_value)
                    ttc_same_lane_sec = round(ttc_value, 6)
                    ttc_sec = ttc_same_lane_sec
                elif adjacent_lane:
                    self.min_ttc_adjacent_lane_sec = min(self.min_ttc_adjacent_lane_sec, ttc_value)
                    ttc_adjacent_lane_sec = round(ttc_value, 6)
            if rel_speed_mps > 0 and path_conflict and gap_m > 0:
                path_ttc_value = gap_m / rel_speed_mps
                self.min_ttc_path_conflict_sec = min(self.min_ttc_path_conflict_sec, path_ttc_value)
                path_ttc_sec = round(path_ttc_value, 6)
            if rel_speed_mps > 0 and gap_m > 0 and route_relation in {"same_lane", "downstream", "upstream"}:
                route_ttc_sec = round(gap_m / rel_speed_mps, 6)

            self.trace_rows.append(
                {
                    "time_sec": round(self.time_sec, 6),
                    "ego_position_m": round(self.ego.position_m, 6),
                    "ego_speed_mps": round(self.ego.speed_mps, 6),
                    "ego_lane_index": int(self.ego.lane_index),
                    "ego_lane_id": self.ego.lane_id,
                    "ego_lane_binding_mode": self.ego.lane_binding_mode,
                    "npc_id": npc.actor_id,
                    "npc_position_m": round(npc.position_m, 6),
                    "npc_lane_index": int(npc.lane_index),
                    "npc_lane_id": npc.lane_id,
                    "npc_lane_binding_mode": npc.lane_binding_mode,
                    "lane_delta": lane_delta,
                    "same_lane": same_lane,
                    "adjacent_lane": adjacent_lane,
                    "path_conflict": path_conflict,
                    "path_conflict_source": path_conflict_source,
                    "path_interaction_kind": path_interaction_kind,
                    "ego_route_lane_order": ego_route_lane_order,
                    "npc_route_lane_order": npc_route_lane_order,
                    "route_lane_delta": route_lane_delta,
                    "route_relation": route_relation,
                    "gap_m": round(gap_m, 6),
                    "relative_speed_mps": round(rel_speed_mps, 6),
                    "ttc_sec": ttc_sec,
                    "ttc_same_lane_sec": ttc_same_lane_sec,
                    "ttc_adjacent_lane_sec": ttc_adjacent_lane_sec,
                    "path_ttc_sec": path_ttc_sec,
                    "route_ttc_sec": route_ttc_sec,
                    "ego_avoidance_brake_applied": bool(avoidance_action.get("brake_applied", False)),
                    "ego_avoidance_ttc_sec": avoidance_action.get("ttc_sec"),
                    "ego_avoidance_applied_brake_mps2": avoidance_action.get("applied_brake_mps2"),
                    "ego_avoidance_effective_brake_limit_mps2": avoidance_action.get(
                        "effective_brake_limit_mps2"
                    ),
                    "ego_avoidance_target_actor_id": avoidance_action.get("target_actor_id"),
                    "ego_avoidance_target_interaction_kind": avoidance_action.get("target_interaction_kind"),
                    "ego_avoidance_target_route_relation": avoidance_action.get("target_route_relation"),
                    "ego_avoidance_target_path_conflict_source": avoidance_action.get(
                        "target_path_conflict_source"
                    ),
                    "ego_avoidance_target_gap_m": avoidance_action.get("target_gap_m"),
                    "ego_avoidance_target_ttc_sec": avoidance_action.get("target_ttc_sec"),
                    "ego_avoidance_target_ttc_threshold_sec": avoidance_action.get("target_ttc_threshold_sec"),
                    "ego_avoidance_target_brake_scale": avoidance_action.get("target_brake_scale"),
                    "ego_surface_friction_scale": round(self.scenario.surface_friction_scale, 6),
                    "ego_dynamics_mode": ego_dynamics_step.get("ego_dynamics_mode"),
                    "ego_dynamics_throttle": ego_dynamics_step.get("throttle"),
                    "ego_dynamics_brake": ego_dynamics_step.get("brake"),
                    "ego_dynamics_accel_mps2": ego_dynamics_step.get("accel_mps2"),
                    "ego_dynamics_net_force_n": ego_dynamics_step.get("net_force_n"),
                    "ego_dynamics_speed_tracking_error_mps": ego_dynamics_step.get(
                        "speed_tracking_error_mps"
                    ),
                    "ego_dynamics_longitudinal_force_limited": ego_dynamics_step.get(
                        "longitudinal_force_limited"
                    ),
                    "collision": self.collision,
                }
            )

    def _classify_route_relation(self, npc: ActorState) -> tuple[int | None, int | None, int | None, str]:
        ego_route_lane_order = self.route_lane_order_by_id.get(self.ego.lane_id)
        npc_route_lane_order = self.route_lane_order_by_id.get(npc.lane_id)
        route_lane_delta = (
            None
            if ego_route_lane_order is None or npc_route_lane_order is None
            else int(npc_route_lane_order - ego_route_lane_order)
        )
        route_relation = "unavailable"
        if self.route_lane_ids:
            if self.ego.lane_id in self.route_lane_order_by_id and npc.lane_id in self.route_lane_order_by_id:
                if route_lane_delta == 0:
                    route_relation = "same_lane"
                elif route_lane_delta is not None and route_lane_delta > 0:
                    route_relation = "downstream"
                else:
                    route_relation = "upstream"
            else:
                route_relation = "off_route"
        return ego_route_lane_order, npc_route_lane_order, route_lane_delta, route_relation

    def _classify_path_interaction(
        self,
        *,
        same_lane: bool,
        adjacent_lane: bool,
        route_relation: str,
    ) -> tuple[bool, str, str]:
        route_path_conflict = route_relation in {"same_lane", "downstream"}
        if route_path_conflict and same_lane:
            return True, "combined", "same_lane_conflict"
        if same_lane:
            return True, "lane_index", "same_lane_conflict"
        if adjacent_lane and route_relation == "downstream":
            return True, "route", "merge_conflict"
        if adjacent_lane and route_relation == "same_lane":
            return True, "route", "lane_change_conflict"
        if route_relation == "downstream":
            return True, "route", "downstream_route_conflict"
        if adjacent_lane and route_relation in {"off_route", "upstream"}:
            return False, "none", "diverge_clear"
        if adjacent_lane:
            return False, "none", "lane_change_clear"
        if route_relation == "upstream":
            return False, "none", "upstream_clear"
        if route_relation == "off_route":
            return False, "none", "off_route_clear"
        return False, "none", "none"

    def _update_ego(self, dt_sec: float, avoidance_action: Mapping[str, Any]) -> dict[str, Any]:
        if self.scenario.ego_dynamics_mode == "vehicle_dynamics":
            return self._update_ego_with_vehicle_dynamics(dt_sec, avoidance_action)
        return self._update_ego_kinematic(dt_sec, avoidance_action)

    def _update_ego_kinematic(self, dt_sec: float, avoidance_action: Mapping[str, Any]) -> dict[str, Any]:
        speed_mps = self.ego.speed_mps
        if bool(avoidance_action.get("brake_applied", False)):
            applied_brake_mps2 = float(avoidance_action.get("applied_brake_mps2", 0.0) or 0.0)
            speed_mps = max(0.0, speed_mps - (applied_brake_mps2 * dt_sec))
            self.ego_avoidance_brake_event_count += 1
            self.ego_avoidance_applied_brake_mps2_max = max(
                self.ego_avoidance_applied_brake_mps2_max,
                applied_brake_mps2,
            )
        self.ego = ActorState(
            actor_id=self.ego.actor_id,
            position_m=self.ego.position_m + (speed_mps * dt_sec),
            speed_mps=speed_mps,
            length_m=self.ego.length_m,
            lane_index=self.ego.lane_index,
            lane_id=self.ego.lane_id,
            lane_binding_mode=self.ego.lane_binding_mode,
        )
        return {
            "ego_dynamics_mode": "kinematic",
            "throttle": None,
            "brake": None,
            "accel_mps2": None,
            "net_force_n": None,
            "speed_tracking_error_mps": None,
            "longitudinal_force_limited": None,
        }

    def _update_ego_with_vehicle_dynamics(
        self,
        dt_sec: float,
        avoidance_action: Mapping[str, Any],
    ) -> dict[str, Any]:
        vehicle_profile = self.scenario.ego_vehicle_profile
        if vehicle_profile is None:
            raise ValueError("ego_vehicle_profile must be configured for vehicle_dynamics mode")
        target_speed_mps = (
            float(self.scenario.ego_target_speed_mps)
            if self.scenario.ego_target_speed_mps is not None
            else float(self.ego.speed_mps)
        )
        speed_error_mps = target_speed_mps - float(self.ego.speed_mps)
        throttle = 0.0
        brake = 0.0
        max_accel_mps2 = float(vehicle_profile["max_accel_mps2"])
        max_decel_mps2 = float(vehicle_profile["max_decel_mps2"])
        if speed_error_mps > 1e-6 and max_accel_mps2 > 0:
            throttle = min(1.0, max(0.0, (speed_error_mps / dt_sec) / max_accel_mps2))
        elif speed_error_mps < -1e-6 and max_decel_mps2 > 0:
            brake = min(1.0, max(0.0, ((-speed_error_mps) / dt_sec) / max_decel_mps2))
        if bool(avoidance_action.get("brake_applied", False)) and max_decel_mps2 > 0:
            avoidance_brake_ratio = min(
                1.0,
                max(
                    0.0,
                    float(avoidance_action.get("applied_brake_mps2", 0.0) or 0.0) / max_decel_mps2,
                ),
            )
            brake = max(brake, avoidance_brake_ratio)
            throttle = 0.0
            self.ego_avoidance_brake_event_count += 1
            self.ego_avoidance_applied_brake_mps2_max = max(
                self.ego_avoidance_applied_brake_mps2_max,
                float(avoidance_action.get("applied_brake_mps2", 0.0) or 0.0),
            )
        dynamics_step = simulate_vehicle_dynamics_step(
            vehicle_profile=vehicle_profile,
            dt_sec=dt_sec,
            position_m=self.ego.position_m,
            speed_mps=self.ego.speed_mps,
            throttle=throttle,
            brake=brake,
            road_grade_percent=self.scenario.ego_road_grade_percent,
            surface_friction_scale=self.scenario.surface_friction_scale,
            target_speed_mps=target_speed_mps,
        )
        if bool(dynamics_step["longitudinal_force_limited"]):
            self.ego_dynamics_longitudinal_force_limited_event_count += 1
        self.ego = ActorState(
            actor_id=self.ego.actor_id,
            position_m=float(dynamics_step["position_m"]),
            speed_mps=float(dynamics_step["speed_mps"]),
            length_m=self.ego.length_m,
            lane_index=self.ego.lane_index,
            lane_id=self.ego.lane_id,
            lane_binding_mode=self.ego.lane_binding_mode,
        )
        return {
            "ego_dynamics_mode": "vehicle_dynamics",
            "throttle": round(float(dynamics_step["throttle"]), 6),
            "brake": round(float(dynamics_step["brake"]), 6),
            "accel_mps2": round(float(dynamics_step["accel_mps2"]), 6),
            "net_force_n": round(float(dynamics_step["net_force_n"]), 6),
            "speed_tracking_error_mps": (
                round(float(dynamics_step["speed_tracking_error_mps"]), 6)
                if dynamics_step["speed_tracking_error_mps"] is not None
                else None
            ),
            "longitudinal_force_limited": bool(dynamics_step["longitudinal_force_limited"]),
        }

    @staticmethod
    def _avoidance_interaction_priority(interaction_kind: str) -> int:
        if interaction_kind == "same_lane_conflict":
            return 0
        if interaction_kind == "merge_conflict":
            return 1
        if interaction_kind == "lane_change_conflict":
            return 2
        if interaction_kind == "downstream_route_conflict":
            return 3
        return 4

    def _resolve_avoidance_policy(self, interaction_kind: str) -> dict[str, float]:
        normalized_policy = dict(self.scenario.avoidance_interaction_policy or {})
        interaction_policy = dict(normalized_policy.get(interaction_kind, {}))
        return {
            "ttc_threshold_sec": float(
                interaction_policy.get("ttc_threshold_sec", self.scenario.avoidance_ttc_threshold_sec)
            ),
            "brake_scale": float(interaction_policy.get("brake_scale", 1.0)),
        }

    def _apply_ego_collision_avoidance(self, dt_sec: float) -> dict[str, Any]:
        result: dict[str, Any] = {
            "brake_applied": False,
            "ttc_sec": None,
            "applied_brake_mps2": None,
            "effective_brake_limit_mps2": None,
            "target_actor_id": None,
            "target_interaction_kind": None,
            "target_route_relation": None,
            "target_path_conflict_source": None,
            "target_gap_m": None,
            "target_ttc_sec": None,
            "target_ttc_threshold_sec": None,
            "target_brake_scale": None,
        }
        if not self.scenario.enable_ego_collision_avoidance:
            return result
        if self.scenario.avoidance_ttc_threshold_sec <= 0 or self.scenario.ego_max_brake_mps2 <= 0:
            return result
        lead_candidates: list[dict[str, Any]] = []
        for npc in self.npcs:
            same_lane = bool(int(npc.lane_index) == int(self.ego.lane_index))
            _, _, _, route_relation = self._classify_route_relation(npc)
            path_conflict, path_conflict_source, path_interaction_kind = self._classify_path_interaction(
                same_lane=same_lane,
                adjacent_lane=bool(abs(int(npc.lane_index) - int(self.ego.lane_index)) == 1),
                route_relation=route_relation,
            )
            gap_m = npc.position_m - self.ego.position_m - 0.5 * (npc.length_m + self.ego.length_m)
            rel_speed_mps = self.ego.speed_mps - npc.speed_mps
            if path_conflict and npc.position_m > self.ego.position_m and gap_m > 0 and rel_speed_mps > 0:
                lead_candidates.append(
                    {
                        "npc": npc,
                        "gap_m": gap_m,
                        "target_ttc_sec": gap_m / rel_speed_mps,
                        "route_relation": route_relation,
                        "path_conflict_source": path_conflict_source,
                        "path_interaction_kind": path_interaction_kind,
                        "avoidance_policy": self._resolve_avoidance_policy(path_interaction_kind),
                    }
                )
        if not lead_candidates:
            return result
        overall_best = min(
            lead_candidates,
            key=lambda row: (
                round(float(row["target_ttc_sec"]), 6),
                self._avoidance_interaction_priority(str(row["path_interaction_kind"])),
                round(float(row["gap_m"]), 6),
                str(row["npc"].actor_id),
            ),
        )
        actionable_candidates = [
            row
            for row in lead_candidates
            if float(row["avoidance_policy"]["ttc_threshold_sec"]) > 0
            and float(row["avoidance_policy"]["brake_scale"]) > 0
            and float(row["target_ttc_sec"]) <= float(row["avoidance_policy"]["ttc_threshold_sec"])
        ]
        selected_target = (
            min(
                actionable_candidates,
                key=lambda row: (
                    round(float(row["target_ttc_sec"]), 6),
                    self._avoidance_interaction_priority(str(row["path_interaction_kind"])),
                    round(float(row["gap_m"]), 6),
                    str(row["npc"].actor_id),
                ),
            )
            if actionable_candidates
            else overall_best
        )
        result["target_actor_id"] = str(selected_target["npc"].actor_id)
        result["target_interaction_kind"] = str(selected_target["path_interaction_kind"])
        result["target_route_relation"] = str(selected_target["route_relation"])
        result["target_path_conflict_source"] = str(selected_target["path_conflict_source"])
        result["target_gap_m"] = round(float(selected_target["gap_m"]), 6)
        result["target_ttc_sec"] = round(float(selected_target["target_ttc_sec"]), 6)
        result["target_ttc_threshold_sec"] = round(
            float(selected_target["avoidance_policy"]["ttc_threshold_sec"]), 6
        )
        result["target_brake_scale"] = round(float(selected_target["avoidance_policy"]["brake_scale"]), 6)
        ttc_sec = float(selected_target["target_ttc_sec"])
        result["ttc_sec"] = round(ttc_sec, 6)
        if not actionable_candidates:
            return result
        friction_brake_limit_mps2 = (
            self.scenario.tire_friction_coeff * self.scenario.surface_friction_scale * 9.80665
        )
        policy_brake_limit_mps2 = (
            max(0.0, self.scenario.ego_max_brake_mps2)
            * float(selected_target["avoidance_policy"]["brake_scale"])
        )
        effective_brake_limit_mps2 = min(
            policy_brake_limit_mps2,
            max(0.0, friction_brake_limit_mps2),
        )
        if effective_brake_limit_mps2 <= 0:
            result["effective_brake_limit_mps2"] = 0.0
            return result
        result["brake_applied"] = True
        result["applied_brake_mps2"] = round(effective_brake_limit_mps2, 6)
        result["effective_brake_limit_mps2"] = round(effective_brake_limit_mps2, 6)
        trigger_interaction_kind = str(selected_target["path_interaction_kind"])
        self.ego_avoidance_last_trigger_actor_id = str(selected_target["npc"].actor_id)
        self.ego_avoidance_last_trigger_interaction_kind = trigger_interaction_kind
        self.ego_avoidance_last_trigger_route_relation = str(selected_target["route_relation"])
        self.ego_avoidance_last_trigger_ttc_sec = round(ttc_sec, 6)
        self.ego_avoidance_last_trigger_gap_m = round(float(selected_target["gap_m"]), 6)
        self.ego_avoidance_last_trigger_ttc_threshold_sec = round(
            float(selected_target["avoidance_policy"]["ttc_threshold_sec"]), 6
        )
        self.ego_avoidance_last_trigger_brake_scale = round(
            float(selected_target["avoidance_policy"]["brake_scale"]), 6
        )
        self.ego_avoidance_trigger_counts_by_interaction_kind[trigger_interaction_kind] = (
            self.ego_avoidance_trigger_counts_by_interaction_kind.get(trigger_interaction_kind, 0) + 1
        )
        return result


def build_lane_risk_summary(*, run_id: str, summary: Mapping[str, Any], trace_rows: list[dict[str, Any]]) -> dict[str, Any]:
    same_lane_rows = [row for row in trace_rows if bool(row.get("same_lane", False))]
    adjacent_lane_rows = [row for row in trace_rows if bool(row.get("adjacent_lane", False))]
    path_conflict_rows = [row for row in trace_rows if bool(row.get("path_conflict", False))]
    other_lane_rows = [
        row
        for row in trace_rows
        if not bool(row.get("same_lane", False))
        and not bool(row.get("adjacent_lane", False))
        and not bool(row.get("path_conflict", False))
    ]

    def _collect_numeric(rows: list[dict[str, Any]], key: str) -> list[float]:
        values: list[float] = []
        for row in rows:
            raw = row.get(key)
            if raw is None:
                continue
            try:
                values.append(float(raw))
            except (TypeError, ValueError):
                continue
        return values

    min_gap_same_lane = _collect_numeric(same_lane_rows, "gap_m")
    min_gap_adjacent_lane = _collect_numeric(adjacent_lane_rows, "gap_m")
    min_gap_path_conflict = _collect_numeric(path_conflict_rows, "gap_m")
    ttc_same_lane = _collect_numeric(same_lane_rows, "ttc_same_lane_sec")
    ttc_adjacent_lane = _collect_numeric(adjacent_lane_rows, "ttc_adjacent_lane_sec")
    ttc_path_conflict = _collect_numeric(path_conflict_rows, "path_ttc_sec")
    path_interaction_labels = [
        "same_lane_conflict",
        "merge_conflict",
        "lane_change_conflict",
        "downstream_route_conflict",
        "lane_change_clear",
        "diverge_clear",
        "upstream_clear",
        "off_route_clear",
        "none",
    ]
    path_interaction_rows = {
        label: [row for row in trace_rows if str(row.get("path_interaction_kind", "none")) == label]
        for label in path_interaction_labels
    }
    merge_conflict_rows = path_interaction_rows["merge_conflict"]
    lane_change_conflict_rows = path_interaction_rows["lane_change_conflict"]
    lane_change_clear_rows = path_interaction_rows["lane_change_clear"]
    diverge_clear_rows = path_interaction_rows["diverge_clear"]
    downstream_route_conflict_rows = path_interaction_rows["downstream_route_conflict"]
    ttc_merge_conflict = _collect_numeric(merge_conflict_rows, "path_ttc_sec")
    ttc_lane_change_conflict = _collect_numeric(lane_change_conflict_rows, "path_ttc_sec")
    ttc_downstream_route_conflict = _collect_numeric(downstream_route_conflict_rows, "path_ttc_sec")
    route_relation_labels = ["same_lane", "downstream", "upstream", "off_route", "unavailable"]
    route_relation_rows = {
        label: [row for row in trace_rows if str(row.get("route_relation", "unavailable")) == label]
        for label in route_relation_labels
    }
    route_same_lane_rows = route_relation_rows["same_lane"]
    route_downstream_rows = route_relation_rows["downstream"]
    route_upstream_rows = route_relation_rows["upstream"]
    route_off_route_rows = route_relation_rows["off_route"]
    route_unavailable_rows = route_relation_rows["unavailable"]
    route_ttc_same_lane = _collect_numeric(route_same_lane_rows, "route_ttc_sec")
    route_ttc_downstream = _collect_numeric(route_downstream_rows, "route_ttc_sec")
    route_ttc_upstream = _collect_numeric(route_upstream_rows, "route_ttc_sec")
    min_gap_route_same_lane = _collect_numeric(route_same_lane_rows, "gap_m")
    min_gap_route_downstream = _collect_numeric(route_downstream_rows, "gap_m")
    min_gap_route_upstream = _collect_numeric(route_upstream_rows, "gap_m")
    route_lane_ids = summary.get("scenario_route_lane_ids", [])
    if not isinstance(route_lane_ids, list):
        route_lane_ids = []
    return {
        "lane_risk_summary_schema_version": "lane_risk_summary_v0",
        "run_id": run_id,
        "step_rows_total": len(trace_rows),
        "same_lane_rows": len(same_lane_rows),
        "adjacent_lane_rows": len(adjacent_lane_rows),
        "path_conflict_rows": len(path_conflict_rows),
        "other_lane_rows": len(other_lane_rows),
        "collision_flag": bool(summary.get("collision", False)),
        "same_lane_collision_rows": sum(1 for row in same_lane_rows if bool(row.get("collision", False))),
        "adjacent_lane_collision_rows": sum(
            1 for row in adjacent_lane_rows if bool(row.get("collision", False))
        ),
        "path_conflict_collision_rows": sum(
            1 for row in path_conflict_rows if bool(row.get("collision", False))
        ),
        "path_interaction_counts": {
            label: len(path_interaction_rows[label]) for label in path_interaction_labels
        },
        "merge_conflict_rows": len(merge_conflict_rows),
        "lane_change_conflict_rows": len(lane_change_conflict_rows),
        "lane_change_clear_rows": len(lane_change_clear_rows),
        "diverge_clear_rows": len(diverge_clear_rows),
        "downstream_route_conflict_rows": len(downstream_route_conflict_rows),
        "min_gap_same_lane_m": None if not min_gap_same_lane else round(min(min_gap_same_lane), 6),
        "min_gap_adjacent_lane_m": None if not min_gap_adjacent_lane else round(min(min_gap_adjacent_lane), 6),
        "min_gap_path_conflict_m": (
            None if not min_gap_path_conflict else round(min(min_gap_path_conflict), 6)
        ),
        "min_ttc_same_lane_sec": summary.get("min_ttc_same_lane_sec"),
        "min_ttc_adjacent_lane_sec": summary.get("min_ttc_adjacent_lane_sec"),
        "min_ttc_path_conflict_sec": summary.get("min_ttc_path_conflict_sec"),
        "min_ttc_merge_conflict_sec": (
            None if not ttc_merge_conflict else round(min(ttc_merge_conflict), 6)
        ),
        "min_ttc_lane_change_conflict_sec": (
            None if not ttc_lane_change_conflict else round(min(ttc_lane_change_conflict), 6)
        ),
        "min_ttc_downstream_route_conflict_sec": (
            None if not ttc_downstream_route_conflict else round(min(ttc_downstream_route_conflict), 6)
        ),
        "min_ttc_any_lane_sec": summary.get("min_ttc_any_lane_sec"),
        "ttc_under_3s_same_lane_count": sum(1 for value in ttc_same_lane if value <= 3.0),
        "ttc_under_3s_adjacent_lane_count": sum(1 for value in ttc_adjacent_lane if value <= 3.0),
        "ttc_under_3s_path_conflict_count": sum(1 for value in ttc_path_conflict if value <= 3.0),
        "ttc_under_3s_merge_conflict_count": sum(1 for value in ttc_merge_conflict if value <= 3.0),
        "ttc_under_3s_lane_change_conflict_count": sum(
            1 for value in ttc_lane_change_conflict if value <= 3.0
        ),
        "ttc_under_3s_downstream_route_conflict_count": sum(
            1 for value in ttc_downstream_route_conflict if value <= 3.0
        ),
        "route_semantics_enabled": bool(route_lane_ids),
        "route_lane_ids": list(route_lane_ids),
        "route_relation_counts": {
            label: len(route_relation_rows[label]) for label in route_relation_labels
        },
        "route_same_lane_rows": len(route_same_lane_rows),
        "route_downstream_rows": len(route_downstream_rows),
        "route_upstream_rows": len(route_upstream_rows),
        "route_off_route_rows": len(route_off_route_rows),
        "route_unavailable_rows": len(route_unavailable_rows),
        "min_gap_route_same_lane_m": (
            None if not min_gap_route_same_lane else round(min(min_gap_route_same_lane), 6)
        ),
        "min_gap_route_downstream_m": (
            None if not min_gap_route_downstream else round(min(min_gap_route_downstream), 6)
        ),
        "min_gap_route_upstream_m": (
            None if not min_gap_route_upstream else round(min(min_gap_route_upstream), 6)
        ),
        "min_ttc_route_same_lane_sec": (
            None if not route_ttc_same_lane else round(min(route_ttc_same_lane), 6)
        ),
        "min_ttc_route_downstream_sec": (
            None if not route_ttc_downstream else round(min(route_ttc_downstream), 6)
        ),
        "min_ttc_route_upstream_sec": (
            None if not route_ttc_upstream else round(min(route_ttc_upstream), 6)
        ),
        "ttc_under_3s_route_same_lane_count": sum(1 for value in route_ttc_same_lane if value <= 3.0),
        "ttc_under_3s_route_downstream_count": sum(1 for value in route_ttc_downstream if value <= 3.0),
        "ttc_under_3s_route_upstream_count": sum(1 for value in route_ttc_upstream if value <= 3.0),
    }


def run_object_sim(
    scenario: ScenarioConfig,
    *,
    seed: int,
    wall_timeout_override: float | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> ObjectSimRunResult:
    effective_timeout = wall_timeout_override
    if effective_timeout is None:
        effective_timeout = scenario.wall_timeout_sec
    if effective_timeout is not None and effective_timeout <= 0:
        raise ValueError("wall_timeout_override must be > 0")

    effective_scenario = ScenarioConfig(
        scenario_schema_version=scenario.scenario_schema_version,
        scenario_id=scenario.scenario_id,
        duration_sec=scenario.duration_sec,
        dt_sec=scenario.dt_sec,
        ego=scenario.ego,
        npcs=scenario.npcs,
        npc_speed_jitter_mps=scenario.npc_speed_jitter_mps,
        enable_ego_collision_avoidance=scenario.enable_ego_collision_avoidance,
        avoidance_ttc_threshold_sec=scenario.avoidance_ttc_threshold_sec,
        ego_max_brake_mps2=scenario.ego_max_brake_mps2,
        avoidance_interaction_policy=scenario.avoidance_interaction_policy,
        tire_friction_coeff=scenario.tire_friction_coeff,
        surface_friction_scale=scenario.surface_friction_scale,
        wall_timeout_sec=effective_timeout,
        ego_dynamics_mode=scenario.ego_dynamics_mode,
        ego_vehicle_profile=scenario.ego_vehicle_profile,
        ego_target_speed_mps=scenario.ego_target_speed_mps,
        ego_road_grade_percent=scenario.ego_road_grade_percent,
        map_context=scenario.map_context,
    )
    runner = CoreSimRunner(scenario=effective_scenario, seed=seed)
    summary = runner.run()
    metadata_dict = dict(metadata or {})
    run_source = str(metadata_dict.get("run_source", "sim_closed_loop"))
    sds_version = str(metadata_dict.get("sds_version", "sds_unknown"))
    sim_version = str(metadata_dict.get("sim_version", "sim_engine_v0_prototype"))
    fidelity_profile = str(metadata_dict.get("fidelity_profile", "dev-fast"))
    scenario_map_id = (
        str(effective_scenario.map_context.map_payload.get("map_id", "map_unknown"))
        if effective_scenario.map_context is not None
        else "map_unknown"
    )
    metadata_map_id = str(metadata_dict.get("map_id", "") or "").strip()
    map_id = (
        scenario_map_id
        if metadata_map_id in {"", "map_unknown"}
        else metadata_map_id
    )
    map_version = str(metadata_dict.get("map_version", "v0"))
    odd_tags = metadata_dict.get("odd_tags", [])
    if isinstance(odd_tags, str):
        odd_tags = [tag.strip() for tag in odd_tags.split(",") if tag.strip()]
    else:
        odd_tags = [str(tag).strip() for tag in list(odd_tags) if str(tag).strip()]
    lifecycle_state = "FAILED" if summary["status"] in {"failed", "timeout"} else "LOGGED"
    route_report = effective_scenario.map_context.route_report if effective_scenario.map_context is not None else None
    validation_report = (
        effective_scenario.map_context.validation_report if effective_scenario.map_context is not None else None
    )
    summary.update(
        {
            "scenario_path": metadata_dict.get("scenario_path"),
            "run_timestamp": summary["started_at"],
            "run_source": run_source,
            "sds_version": sds_version,
            "sim_version": sim_version,
            "fidelity_profile": fidelity_profile,
            "map_id": map_id,
            "map_version": map_version,
            "odd_tags": odd_tags,
            "lifecycle_state": lifecycle_state,
            "batch_id": metadata_dict.get("batch_id"),
            "scenario_map_enabled": bool(effective_scenario.map_context is not None),
            "scenario_map_path": (
                effective_scenario.map_context.map_path if effective_scenario.map_context is not None else None
            ),
            "scenario_map_validation_error_count": (
                int(validation_report["error_count"]) if validation_report is not None else 0
            ),
            "scenario_map_validation_warning_count": (
                int(validation_report["warning_count"]) if validation_report is not None else 0
            ),
            "scenario_map_routing_semantic_status": (
                validation_report["routing_semantic_summary"]["routing_semantic_status"]
                if validation_report is not None
                else None
            ),
            "scenario_route_enabled": bool(route_report is not None),
            "scenario_route_lane_ids": (
                list(route_report.get("route_lane_ids", [])) if route_report is not None else []
            ),
            "scenario_route_lane_count": (
                int(route_report.get("route_lane_count", 0)) if route_report is not None else 0
            ),
            "scenario_route_cost_mode": (
                str(route_report.get("route_cost_mode", "")) if route_report is not None else None
            ),
            "scenario_route_cost_value": route_report.get("route_cost_value") if route_report is not None else None,
            "route_aware_runtime_enabled": bool(route_report is not None),
            "enable_ego_collision_avoidance": bool(effective_scenario.enable_ego_collision_avoidance),
            "avoidance_ttc_threshold_sec": float(effective_scenario.avoidance_ttc_threshold_sec),
            "ego_max_brake_mps2": float(effective_scenario.ego_max_brake_mps2),
            "avoidance_interaction_policy": dict(effective_scenario.avoidance_interaction_policy or {}),
            "tire_friction_coeff": float(effective_scenario.tire_friction_coeff),
            "surface_friction_scale": float(effective_scenario.surface_friction_scale),
            "ego_dynamics_mode": effective_scenario.ego_dynamics_mode,
            "ego_dynamics_coupled": bool(effective_scenario.ego_dynamics_mode == "vehicle_dynamics"),
            "ego_dynamics_target_speed_mps": effective_scenario.ego_target_speed_mps,
            "ego_dynamics_road_grade_percent": float(effective_scenario.ego_road_grade_percent),
            "ego_dynamics_vehicle_profile_schema_version": (
                VEHICLE_PROFILE_SCHEMA_VERSION_V0
                if effective_scenario.ego_vehicle_profile is not None
                else None
            ),
            "ego_dynamics_longitudinal_force_limited_event_count": int(
                runner.ego_dynamics_longitudinal_force_limited_event_count
            ),
            "ego_avoidance_brake_event_count": int(runner.ego_avoidance_brake_event_count),
            "ego_avoidance_applied_brake_mps2_max": round(
                float(runner.ego_avoidance_applied_brake_mps2_max),
                6,
            ),
            "ego_avoidance_trigger_counts_by_interaction_kind": dict(
                sorted(runner.ego_avoidance_trigger_counts_by_interaction_kind.items())
            ),
            "ego_avoidance_last_trigger_actor_id": runner.ego_avoidance_last_trigger_actor_id,
            "ego_avoidance_last_trigger_interaction_kind": runner.ego_avoidance_last_trigger_interaction_kind,
            "ego_avoidance_last_trigger_route_relation": runner.ego_avoidance_last_trigger_route_relation,
            "ego_avoidance_last_trigger_ttc_sec": runner.ego_avoidance_last_trigger_ttc_sec,
            "ego_avoidance_last_trigger_gap_m": runner.ego_avoidance_last_trigger_gap_m,
            "ego_avoidance_last_trigger_ttc_threshold_sec": runner.ego_avoidance_last_trigger_ttc_threshold_sec,
            "ego_avoidance_last_trigger_brake_scale": runner.ego_avoidance_last_trigger_brake_scale,
            "traffic_npc_count": int(len(effective_scenario.npcs)),
            "traffic_npc_lane_profile": [int(npc.lane_index) for npc in effective_scenario.npcs],
            "traffic_npc_lane_id_profile": [npc.lane_id for npc in effective_scenario.npcs],
            "traffic_npc_lane_binding_modes": [npc.lane_binding_mode for npc in effective_scenario.npcs],
            "ego_lane_id": effective_scenario.ego.lane_id,
            "ego_lane_binding_mode": effective_scenario.ego.lane_binding_mode,
            "traffic_npc_gap_profile_m": [
                round(float(npc.position_m - effective_scenario.ego.position_m), 6)
                for npc in effective_scenario.npcs
            ],
            "traffic_npc_initial_speed_profile_mps": [round(float(npc.speed_mps), 6) for npc in effective_scenario.npcs],
            "metric_values": [
                {"metric_id": "collision_flag", "value": 1 if summary["collision"] else 0, "unit": "bool"},
                {"metric_id": "timeout_flag", "value": 1 if summary["timeout"] else 0, "unit": "bool"},
                {"metric_id": "min_ttc_sec", "value": summary["min_ttc_sec"], "unit": "sec"},
                {
                    "metric_id": "min_ttc_same_lane_sec",
                    "value": summary.get("min_ttc_same_lane_sec"),
                    "unit": "sec",
                },
                {
                    "metric_id": "min_ttc_adjacent_lane_sec",
                    "value": summary.get("min_ttc_adjacent_lane_sec"),
                    "unit": "sec",
                },
                {
                    "metric_id": "min_ttc_path_conflict_sec",
                    "value": summary.get("min_ttc_path_conflict_sec"),
                    "unit": "sec",
                },
                {
                    "metric_id": "min_ttc_any_lane_sec",
                    "value": summary.get("min_ttc_any_lane_sec"),
                    "unit": "sec",
                },
            ],
        }
    )
    lane_risk_summary = build_lane_risk_summary(
        run_id=str(metadata_dict.get("run_id", "")),
        summary=summary,
        trace_rows=runner.trace_rows,
    )
    return ObjectSimRunResult(summary=summary, trace_rows=runner.trace_rows, lane_risk_summary=lane_risk_summary)
