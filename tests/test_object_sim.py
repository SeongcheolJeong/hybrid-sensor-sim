from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.scenarios import load_scenario, run_object_sim
from hybrid_sensor_sim.scenarios.schema import ScenarioValidationError
from hybrid_sensor_sim.tools.object_sim_runner import main as object_sim_main


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_sim_engine"
MAP_FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_map_toolset"


class ObjectSimTests(unittest.TestCase):
    def test_load_scenario_infers_lane_ids_from_route_lane_indexes(self) -> None:
        canonical_map = json.loads(
            (
                MAP_FIXTURE_ROOT
                / "canonical_lane_graph_v0.json"
            ).read_text(encoding="utf-8")
        )
        scenario = load_scenario(
            {
                "scenario_schema_version": "scenario_definition_v0",
                "scenario_id": "map_lane_index_only",
                "duration_sec": 1.0,
                "dt_sec": 0.1,
                "canonical_map": canonical_map,
                "route_definition": {
                    "entry_lane_id": "lane_a",
                    "exit_lane_id": "lane_c",
                    "via_lane_ids": ["lane_b"],
                    "cost_mode": "hops",
                },
                "ego": {"position_m": 0.0, "speed_mps": 10.0, "lane_index": 0},
                "npcs": [{"position_m": 20.0, "speed_mps": 8.0, "lane_index": 1}],
            }
        )

        self.assertEqual(scenario.ego.lane_id, "lane_a")
        self.assertEqual(scenario.ego.lane_binding_mode, "inferred_from_route")
        self.assertEqual(scenario.npcs[0].lane_id, "lane_b")
        self.assertEqual(scenario.npcs[0].lane_binding_mode, "inferred_from_route")

    def test_load_scenario_rejects_invalid_schema(self) -> None:
        with self.assertRaisesRegex(ScenarioValidationError, "unsupported scenario_schema_version"):
            load_scenario(
                {
                    "scenario_schema_version": "wrong",
                    "scenario_id": "bad",
                    "duration_sec": 1.0,
                    "dt_sec": 0.1,
                    "ego": {"position_m": 0.0, "speed_mps": 1.0},
                    "npcs": [{"position_m": 5.0, "speed_mps": 1.0}],
                }
            )

    def test_load_scenario_rejects_empty_npcs(self) -> None:
        with self.assertRaisesRegex(ScenarioValidationError, "npcs must be a non-empty list"):
            load_scenario(
                {
                    "scenario_schema_version": "scenario_definition_v0",
                    "scenario_id": "bad",
                    "duration_sec": 1.0,
                    "dt_sec": 0.1,
                    "ego": {"position_m": 0.0, "speed_mps": 1.0},
                    "npcs": [],
                }
            )

    def test_load_scenario_requires_vehicle_profile_for_dynamics_mode(self) -> None:
        with self.assertRaisesRegex(
            ScenarioValidationError,
            "ego_dynamics_mode=vehicle_dynamics requires ego_vehicle_profile",
        ):
            load_scenario(
                {
                    "scenario_schema_version": "scenario_definition_v0",
                    "scenario_id": "bad_dyn",
                    "duration_sec": 1.0,
                    "dt_sec": 0.1,
                    "ego_dynamics_mode": "vehicle_dynamics",
                    "ego": {"position_m": 0.0, "speed_mps": 1.0},
                    "npcs": [{"position_m": 5.0, "speed_mps": 1.0}],
                }
            )

    def test_load_scenario_rejects_invalid_avoidance_interaction_policy(self) -> None:
        with self.assertRaisesRegex(
            ScenarioValidationError,
            "avoidance_interaction_policy.merge_conflict.brake_scale must be between 0 and 1",
        ):
            load_scenario(
                {
                    "scenario_schema_version": "scenario_definition_v0",
                    "scenario_id": "bad_avoidance_policy",
                    "duration_sec": 1.0,
                    "dt_sec": 0.1,
                    "enable_ego_collision_avoidance": True,
                    "avoidance_ttc_threshold_sec": 2.0,
                    "ego_max_brake_mps2": 5.0,
                    "avoidance_interaction_policy": {
                        "merge_conflict": {"brake_scale": 1.5},
                    },
                    "ego": {"position_m": 0.0, "speed_mps": 1.0},
                    "npcs": [{"position_m": 5.0, "speed_mps": 1.0}],
                }
            )

        with self.assertRaisesRegex(
            ScenarioValidationError,
            "avoidance_interaction_policy.merge_conflict.priority must be >= 0",
        ):
            load_scenario(
                {
                    "scenario_schema_version": "scenario_definition_v0",
                    "scenario_id": "bad_avoidance_policy_priority",
                    "duration_sec": 1.0,
                    "dt_sec": 0.1,
                    "enable_ego_collision_avoidance": True,
                    "avoidance_ttc_threshold_sec": 2.0,
                    "ego_max_brake_mps2": 5.0,
                    "avoidance_interaction_policy": {
                        "merge_conflict": {"priority": -1},
                    },
                    "ego": {"position_m": 0.0, "speed_mps": 1.0},
                    "npcs": [{"position_m": 5.0, "speed_mps": 1.0}],
                }
            )

    def test_load_scenario_resolves_lane_ids_from_map_route(self) -> None:
        scenario = load_scenario(FIXTURE_ROOT / "highway_map_route_following_v0.json")

        self.assertIsNotNone(scenario.map_context)
        self.assertEqual(scenario.ego.lane_id, "lane_a")
        self.assertEqual(scenario.ego.lane_index, 0)
        self.assertEqual(scenario.npcs[0].lane_id, "lane_b")
        self.assertEqual(scenario.npcs[0].lane_index, 1)
        self.assertEqual(scenario.map_context.route_report["route_lane_ids"], ["lane_a", "lane_b", "lane_c"])

    def test_load_scenario_rejects_lane_id_not_on_route(self) -> None:
        with self.assertRaisesRegex(ScenarioValidationError, "lane_id not found in scenario route"):
            load_scenario(
                {
                    "scenario_schema_version": "scenario_definition_v0",
                    "scenario_id": "bad_map_lane",
                    "duration_sec": 1.0,
                    "dt_sec": 0.1,
                    "canonical_map": {
                        "map_schema_version": "canonical_lane_graph_v0",
                        "map_id": "demo_map_v0",
                        "lanes": [
                            {
                                "lane_id": "lane_a",
                                "centerline_m": [{"x_m": 0.0, "y_m": 0.0}, {"x_m": 10.0, "y_m": 0.0}],
                                "predecessor_lane_ids": [],
                                "successor_lane_ids": ["lane_b"],
                            },
                            {
                                "lane_id": "lane_b",
                                "centerline_m": [{"x_m": 10.0, "y_m": 0.0}, {"x_m": 20.0, "y_m": 0.0}],
                                "predecessor_lane_ids": ["lane_a"],
                                "successor_lane_ids": [],
                            },
                        ],
                    },
                    "route_definition": {
                        "entry_lane_id": "lane_a",
                        "exit_lane_id": "lane_b",
                    },
                    "ego": {"position_m": 0.0, "speed_mps": 1.0, "lane_id": "lane_unknown"},
                    "npcs": [{"position_m": 5.0, "speed_mps": 1.0, "lane_id": "lane_a"}],
                }
            )

    def test_load_scenario_accepts_explicit_route_lane_id(self) -> None:
        scenario = load_scenario(
            {
                "scenario_schema_version": "scenario_definition_v0",
                "scenario_id": "lane_change_route_binding",
                "duration_sec": 0.2,
                "dt_sec": 0.1,
                "canonical_map_path": str(MAP_FIXTURE_ROOT / "canonical_lane_graph_v0.json"),
                "route_definition": {
                    "entry_lane_id": "lane_a",
                    "exit_lane_id": "lane_c",
                    "via_lane_ids": ["lane_b"],
                    "cost_mode": "hops",
                },
                "ego": {"position_m": 0.0, "speed_mps": 8.0, "lane_id": "lane_a"},
                "npcs": [
                    {
                        "position_m": 15.0,
                        "speed_mps": 7.0,
                        "lane_id": "lane_b",
                        "route_lane_id": "lane_a",
                    }
                ],
            }
        )

        self.assertEqual(scenario.npcs[0].lane_id, "lane_b")
        self.assertEqual(scenario.npcs[0].route_lane_id, "lane_a")
        self.assertEqual(scenario.npcs[0].route_binding_mode, "explicit_route_lane_id")

    def test_run_object_sim_success_case_is_deterministic(self) -> None:
        scenario = load_scenario(FIXTURE_ROOT / "highway_safe_following_v0.json")
        result = run_object_sim(scenario, seed=42, metadata={"run_id": "SAFE_001"})

        self.assertEqual(result.summary["status"], "success")
        self.assertEqual(result.summary["termination_reason"], "completed")
        self.assertFalse(result.summary["collision"])
        self.assertFalse(result.summary["timeout"])
        self.assertIsNone(result.summary["min_ttc_same_lane_sec"])
        self.assertEqual(result.lane_risk_summary["same_lane_rows"], 200)
        self.assertEqual(result.lane_risk_summary["ttc_under_3s_same_lane_count"], 0)
        self.assertFalse(result.lane_risk_summary["route_semantics_enabled"])
        self.assertEqual(result.lane_risk_summary["route_relation_counts"]["unavailable"], 200)

    def test_run_object_sim_collision_case_is_deterministic(self) -> None:
        scenario = load_scenario(FIXTURE_ROOT / "highway_following_v0.json")
        result = run_object_sim(scenario, seed=42, metadata={"run_id": "FOLLOW_001"})

        self.assertEqual(result.summary["status"], "failed")
        self.assertEqual(result.summary["termination_reason"], "collision")
        self.assertTrue(result.summary["collision"])
        self.assertIsNotNone(result.summary["min_ttc_same_lane_sec"])
        self.assertGreater(result.lane_risk_summary["same_lane_rows"], 0)
        self.assertGreaterEqual(result.lane_risk_summary["ttc_under_3s_same_lane_count"], 1)

    def test_run_object_sim_vehicle_dynamics_mode_updates_ego_speed(self) -> None:
        scenario = load_scenario(FIXTURE_ROOT / "highway_safe_following_vehicle_dynamics_v0.json")
        result = run_object_sim(scenario, seed=42, metadata={"run_id": "SAFE_DYN_001"})

        self.assertEqual(result.summary["status"], "success")
        self.assertEqual(result.summary["ego_dynamics_mode"], "vehicle_dynamics")
        self.assertTrue(result.summary["ego_dynamics_coupled"])
        self.assertEqual(result.summary["ego_dynamics_vehicle_profile_schema_version"], "vehicle_profile_v0")
        self.assertEqual(result.summary["ego_dynamics_target_speed_mps"], 16.0)
        self.assertGreater(float(result.trace_rows[-1]["ego_speed_mps"]), 12.0)
        self.assertEqual(result.trace_rows[0]["ego_dynamics_mode"], "vehicle_dynamics")
        self.assertIsNotNone(result.trace_rows[0]["ego_dynamics_throttle"])
        self.assertIsNotNone(result.trace_rows[0]["ego_dynamics_accel_mps2"])

    def test_run_object_sim_exposes_map_route_context(self) -> None:
        scenario = load_scenario(FIXTURE_ROOT / "highway_map_route_following_v0.json")
        result = run_object_sim(scenario, seed=42, metadata={"run_id": "MAP_ROUTE_001"})

        self.assertEqual(result.summary["status"], "failed")
        self.assertEqual(result.summary["termination_reason"], "collision")
        self.assertTrue(result.summary["collision"])
        self.assertTrue(result.summary["scenario_map_enabled"])
        self.assertTrue(result.summary["scenario_route_enabled"])
        self.assertEqual(result.summary["map_id"], "demo_map_v0")
        self.assertEqual(result.summary["scenario_map_routing_semantic_status"], "pass")
        self.assertEqual(result.summary["scenario_route_lane_ids"], ["lane_a", "lane_b", "lane_c"])
        self.assertEqual(result.summary["scenario_route_lane_count"], 3)
        self.assertEqual(result.summary["traffic_npc_lane_id_profile"], ["lane_b", "lane_a"])
        self.assertEqual(result.summary["ego_lane_binding_mode"], "explicit_lane_id")
        self.assertEqual(
            result.summary["traffic_npc_lane_binding_modes"],
            ["explicit_lane_id", "explicit_lane_id"],
        )
        self.assertEqual(result.trace_rows[0]["ego_lane_id"], "lane_a")
        self.assertEqual(result.trace_rows[0]["ego_lane_binding_mode"], "explicit_lane_id")
        self.assertEqual(result.trace_rows[0]["npc_lane_id"], "lane_b")
        self.assertEqual(result.trace_rows[0]["npc_lane_binding_mode"], "explicit_lane_id")
        self.assertEqual(result.trace_rows[0]["route_relation"], "downstream")
        self.assertEqual(result.trace_rows[1]["route_relation"], "same_lane")
        self.assertEqual(result.trace_rows[0]["path_interaction_kind"], "merge_conflict")
        self.assertEqual(result.trace_rows[1]["path_interaction_kind"], "same_lane_conflict")
        self.assertIsNotNone(result.summary["min_ttc_adjacent_lane_sec"])
        self.assertTrue(result.summary["route_aware_runtime_enabled"])
        self.assertAlmostEqual(result.summary["min_ttc_path_conflict_sec"], 0.1, places=6)
        self.assertTrue(result.lane_risk_summary["route_semantics_enabled"])
        self.assertEqual(result.lane_risk_summary["route_lane_ids"], ["lane_a", "lane_b", "lane_c"])
        self.assertEqual(result.lane_risk_summary["route_same_lane_rows"], 32)
        self.assertEqual(result.lane_risk_summary["route_downstream_rows"], 32)
        self.assertEqual(result.lane_risk_summary["route_upstream_rows"], 0)
        self.assertEqual(result.lane_risk_summary["path_conflict_rows"], 64)
        self.assertEqual(result.lane_risk_summary["merge_conflict_rows"], 32)
        self.assertEqual(result.lane_risk_summary["path_interaction_counts"]["merge_conflict"], 32)
        self.assertEqual(result.lane_risk_summary["path_interaction_counts"]["same_lane_conflict"], 32)
        self.assertAlmostEqual(result.lane_risk_summary["min_ttc_path_conflict_sec"], 0.1, places=6)
        self.assertAlmostEqual(result.lane_risk_summary["min_ttc_merge_conflict_sec"], 0.1, places=6)
        self.assertGreater(result.lane_risk_summary["ttc_under_3s_path_conflict_count"], 0)
        self.assertGreater(result.lane_risk_summary["ttc_under_3s_merge_conflict_count"], 0)
        self.assertEqual(result.lane_risk_summary["route_relation_counts"]["off_route"], 0)
        self.assertIsNone(result.lane_risk_summary["min_ttc_route_same_lane_sec"])
        self.assertAlmostEqual(result.lane_risk_summary["min_ttc_route_downstream_sec"], 0.1, places=6)
        self.assertGreater(result.lane_risk_summary["ttc_under_3s_route_downstream_count"], 0)
        self.assertTrue(result.trace_rows[0]["path_conflict"])
        self.assertEqual(result.trace_rows[0]["path_conflict_source"], "route")
        self.assertAlmostEqual(float(result.trace_rows[0]["path_ttc_sec"]), 3.1, places=6)

    def test_run_object_sim_exposes_inferred_route_lane_bindings(self) -> None:
        canonical_map = json.loads(
            (
                MAP_FIXTURE_ROOT
                / "canonical_lane_graph_v0.json"
            ).read_text(encoding="utf-8")
        )
        scenario = load_scenario(
            {
                "scenario_schema_version": "scenario_definition_v0",
                "scenario_id": "map_lane_index_inferred",
                "duration_sec": 1.0,
                "dt_sec": 0.1,
                "canonical_map": canonical_map,
                "route_definition": {
                    "entry_lane_id": "lane_a",
                    "exit_lane_id": "lane_c",
                    "via_lane_ids": ["lane_b"],
                    "cost_mode": "hops",
                },
                "ego": {"position_m": 0.0, "speed_mps": 10.0, "lane_index": 0},
                "npcs": [{"position_m": 15.0, "speed_mps": 9.0, "lane_index": 1}],
            }
        )
        result = run_object_sim(scenario, seed=42, metadata={"run_id": "MAP_ROUTE_INFER_001"})

        self.assertEqual(result.summary["ego_lane_id"], "lane_a")
        self.assertEqual(result.summary["ego_lane_binding_mode"], "inferred_from_route")
        self.assertEqual(result.summary["traffic_npc_lane_id_profile"], ["lane_b"])
        self.assertEqual(result.summary["traffic_npc_lane_binding_modes"], ["inferred_from_route"])
        self.assertEqual(result.trace_rows[0]["ego_lane_binding_mode"], "inferred_from_route")
        self.assertEqual(result.trace_rows[0]["npc_lane_binding_mode"], "inferred_from_route")
        self.assertEqual(result.trace_rows[0]["route_relation"], "downstream")
        self.assertTrue(result.trace_rows[0]["path_conflict"])
        self.assertEqual(result.trace_rows[0]["path_conflict_source"], "route")

    def test_run_object_sim_exposes_lane_change_clear_semantics_without_route(self) -> None:
        scenario = load_scenario(
            {
                "scenario_schema_version": "scenario_definition_v0",
                "scenario_id": "lane_change_clear",
                "duration_sec": 0.2,
                "dt_sec": 0.1,
                "ego": {"position_m": 0.0, "speed_mps": 8.0, "lane_index": 0},
                "npcs": [{"position_m": 15.0, "speed_mps": 7.0, "lane_index": 1}],
            }
        )
        result = run_object_sim(scenario, seed=42, metadata={"run_id": "LANE_CHANGE_CLEAR_001"})

        self.assertFalse(result.trace_rows[0]["path_conflict"])
        self.assertEqual(result.trace_rows[0]["path_conflict_source"], "none")
        self.assertEqual(result.trace_rows[0]["path_interaction_kind"], "lane_change_clear")
        self.assertEqual(result.lane_risk_summary["lane_change_clear_rows"], 2)
        self.assertEqual(result.lane_risk_summary["path_interaction_counts"]["lane_change_clear"], 2)

    def test_run_object_sim_exposes_lane_change_conflict_with_route_lane_id(self) -> None:
        scenario = load_scenario(
            {
                "scenario_schema_version": "scenario_definition_v0",
                "scenario_id": "lane_change_conflict_route_lane",
                "duration_sec": 0.2,
                "dt_sec": 0.1,
                "canonical_map_path": str(MAP_FIXTURE_ROOT / "canonical_lane_graph_v0.json"),
                "route_definition": {
                    "entry_lane_id": "lane_a",
                    "exit_lane_id": "lane_c",
                    "via_lane_ids": ["lane_b"],
                    "cost_mode": "hops",
                },
                "ego": {"position_m": 0.0, "speed_mps": 10.0, "lane_id": "lane_a"},
                "npcs": [
                    {
                        "actor_id": "lane_change_actor",
                        "position_m": 18.0,
                        "speed_mps": 7.0,
                        "lane_id": "lane_b",
                        "route_lane_id": "lane_a",
                    }
                ],
            }
        )
        result = run_object_sim(scenario, seed=42, metadata={"run_id": "LANE_CHANGE_CONFLICT_001"})

        self.assertEqual(result.trace_rows[0]["npc_lane_id"], "lane_b")
        self.assertEqual(result.trace_rows[0]["npc_route_lane_id"], "lane_a")
        self.assertEqual(result.trace_rows[0]["route_relation"], "same_lane")
        self.assertTrue(result.trace_rows[0]["path_conflict"])
        self.assertEqual(result.trace_rows[0]["path_conflict_source"], "route")
        self.assertEqual(result.trace_rows[0]["path_interaction_kind"], "lane_change_conflict")
        self.assertEqual(result.summary["traffic_npc_route_lane_id_profile"], ["lane_a"])
        self.assertEqual(result.summary["traffic_npc_route_binding_modes"], ["explicit_route_lane_id"])
        self.assertEqual(result.lane_risk_summary["lane_change_conflict_rows"], 2)
        self.assertEqual(result.lane_risk_summary["path_interaction_counts"]["lane_change_conflict"], 2)
        self.assertAlmostEqual(result.lane_risk_summary["min_ttc_lane_change_conflict_sec"], 4.2, places=6)

    def test_run_object_sim_exposes_diverge_clear_semantics_with_route(self) -> None:
        canonical_map = {
            "map_schema_version": "canonical_lane_graph_v0",
            "map_id": "diverge_demo_map_v0",
            "lanes": [
                {
                    "lane_id": "lane_a",
                    "centerline_m": [{"x_m": 0.0, "y_m": 0.0}, {"x_m": 10.0, "y_m": 0.0}],
                    "predecessor_lane_ids": [],
                    "successor_lane_ids": [],
                },
                {
                    "lane_id": "lane_b",
                    "centerline_m": [{"x_m": 0.0, "y_m": 3.5}, {"x_m": 10.0, "y_m": 3.5}],
                    "predecessor_lane_ids": [],
                    "successor_lane_ids": [],
                },
            ],
        }
        scenario = load_scenario(
            {
                "scenario_schema_version": "scenario_definition_v0",
                "scenario_id": "diverge_clear_route",
                "duration_sec": 0.2,
                "dt_sec": 0.1,
                "canonical_map": canonical_map,
                "route_definition": {"entry_lane_id": "lane_a", "exit_lane_id": "lane_a", "cost_mode": "hops"},
                "ego": {"position_m": 0.0, "speed_mps": 8.0, "lane_id": "lane_a"},
                "npcs": [{"position_m": 15.0, "speed_mps": 7.0, "lane_index": 1}],
            }
        )
        result = run_object_sim(scenario, seed=42, metadata={"run_id": "DIVERGE_CLEAR_001"})

        self.assertEqual(result.trace_rows[0]["route_relation"], "off_route")
        self.assertFalse(result.trace_rows[0]["path_conflict"])
        self.assertEqual(result.trace_rows[0]["path_interaction_kind"], "diverge_clear")
        self.assertEqual(result.lane_risk_summary["diverge_clear_rows"], 2)
        self.assertEqual(result.lane_risk_summary["path_interaction_counts"]["diverge_clear"], 2)

    def test_route_aware_avoidance_brakes_for_downstream_route_conflict(self) -> None:
        canonical_map = json.loads((MAP_FIXTURE_ROOT / "canonical_lane_graph_v0.json").read_text(encoding="utf-8"))
        scenario = load_scenario(
            {
                "scenario_schema_version": "scenario_definition_v0",
                "scenario_id": "route_avoidance_downstream",
                "duration_sec": 1.0,
                "dt_sec": 0.1,
                "canonical_map": canonical_map,
                "route_definition": {
                    "entry_lane_id": "lane_a",
                    "exit_lane_id": "lane_c",
                    "via_lane_ids": ["lane_b"],
                    "cost_mode": "hops",
                },
                "enable_ego_collision_avoidance": True,
                "avoidance_ttc_threshold_sec": 3.5,
                "ego_max_brake_mps2": 5.0,
                "ego": {"position_m": 0.0, "speed_mps": 10.0, "lane_id": "lane_a"},
                "npcs": [{"position_m": 18.0, "speed_mps": 4.0, "lane_id": "lane_b"}],
            }
        )
        result = run_object_sim(scenario, seed=42, metadata={"run_id": "ROUTE_AVOID_001"})

        self.assertEqual(result.summary["status"], "success")
        self.assertTrue(result.summary["route_aware_runtime_enabled"])
        self.assertGreater(result.summary["ego_avoidance_brake_event_count"], 0)
        self.assertIsNotNone(result.summary["min_ttc_path_conflict_sec"])
        self.assertTrue(result.trace_rows[0]["path_conflict"])
        self.assertEqual(result.trace_rows[0]["route_relation"], "downstream")
        self.assertTrue(result.trace_rows[0]["ego_avoidance_brake_applied"])
        self.assertEqual(result.trace_rows[0]["path_conflict_source"], "route")
        self.assertEqual(result.summary["ego_avoidance_last_trigger_actor_id"], "npc_1")
        self.assertEqual(result.summary["ego_avoidance_last_trigger_interaction_kind"], "merge_conflict")
        self.assertEqual(result.summary["ego_avoidance_last_trigger_route_relation"], "downstream")
        self.assertEqual(
            result.summary["ego_avoidance_trigger_counts_by_interaction_kind"],
            {"merge_conflict": result.summary["ego_avoidance_brake_event_count"]},
        )
        self.assertEqual(result.trace_rows[0]["ego_avoidance_target_actor_id"], "npc_1")
        self.assertEqual(result.trace_rows[0]["ego_avoidance_target_interaction_kind"], "merge_conflict")
        self.assertEqual(result.trace_rows[0]["ego_avoidance_target_route_relation"], "downstream")

    def test_route_aware_avoidance_brakes_for_lane_change_conflict(self) -> None:
        canonical_map = json.loads((MAP_FIXTURE_ROOT / "canonical_lane_graph_v0.json").read_text(encoding="utf-8"))
        scenario = load_scenario(
            {
                "scenario_schema_version": "scenario_definition_v0",
                "scenario_id": "route_avoidance_lane_change_conflict",
                "duration_sec": 0.5,
                "dt_sec": 0.1,
                "canonical_map": canonical_map,
                "route_definition": {
                    "entry_lane_id": "lane_a",
                    "exit_lane_id": "lane_c",
                    "via_lane_ids": ["lane_b"],
                    "cost_mode": "hops",
                },
                "enable_ego_collision_avoidance": True,
                "avoidance_ttc_threshold_sec": 3.0,
                "ego_max_brake_mps2": 5.0,
                "ego": {"position_m": 0.0, "speed_mps": 10.0, "lane_id": "lane_a"},
                "npcs": [
                    {
                        "actor_id": "lane_change_risk",
                        "position_m": 18.0,
                        "speed_mps": 4.0,
                        "lane_id": "lane_b",
                        "route_lane_id": "lane_a",
                    }
                ],
            }
        )
        result = run_object_sim(scenario, seed=42, metadata={"run_id": "ROUTE_AVOID_LANE_CHANGE_001"})

        self.assertGreater(result.summary["ego_avoidance_brake_event_count"], 0)
        self.assertEqual(result.summary["ego_avoidance_last_trigger_actor_id"], "lane_change_risk")
        self.assertEqual(result.summary["ego_avoidance_last_trigger_interaction_kind"], "lane_change_conflict")
        self.assertEqual(result.summary["ego_avoidance_last_trigger_route_relation"], "same_lane")
        self.assertEqual(
            result.summary["ego_avoidance_trigger_counts_by_interaction_kind"],
            {"lane_change_conflict": result.summary["ego_avoidance_brake_event_count"]},
        )
        self.assertEqual(result.trace_rows[0]["ego_avoidance_target_actor_id"], "lane_change_risk")
        self.assertEqual(result.trace_rows[0]["ego_avoidance_target_interaction_kind"], "lane_change_conflict")

    def test_route_aware_avoidance_prioritizes_more_urgent_merge_conflict(self) -> None:
        canonical_map = json.loads((MAP_FIXTURE_ROOT / "canonical_lane_graph_v0.json").read_text(encoding="utf-8"))
        scenario = load_scenario(
            {
                "scenario_schema_version": "scenario_definition_v0",
                "scenario_id": "route_avoidance_merge_priority",
                "duration_sec": 0.5,
                "dt_sec": 0.1,
                "canonical_map": canonical_map,
                "route_definition": {
                    "entry_lane_id": "lane_a",
                    "exit_lane_id": "lane_c",
                    "via_lane_ids": ["lane_b"],
                    "cost_mode": "hops",
                },
                "enable_ego_collision_avoidance": True,
                "avoidance_ttc_threshold_sec": 3.0,
                "ego_max_brake_mps2": 5.0,
                "ego": {"position_m": 0.0, "speed_mps": 10.0, "lane_id": "lane_a"},
                "npcs": [
                    {"actor_id": "same_lane_far", "position_m": 12.0, "speed_mps": 9.5, "lane_id": "lane_a"},
                    {"actor_id": "merge_risk", "position_m": 18.0, "speed_mps": 4.0, "lane_id": "lane_b"},
                ],
            }
        )
        result = run_object_sim(scenario, seed=42, metadata={"run_id": "ROUTE_AVOID_MERGE_PRIORITY_001"})

        self.assertEqual(result.summary["status"], "success")
        self.assertGreater(result.summary["ego_avoidance_brake_event_count"], 0)
        self.assertEqual(result.summary["ego_avoidance_last_trigger_actor_id"], "merge_risk")
        self.assertEqual(result.summary["ego_avoidance_last_trigger_interaction_kind"], "merge_conflict")
        self.assertEqual(result.summary["ego_avoidance_last_trigger_route_relation"], "downstream")
        self.assertEqual(
            result.summary["ego_avoidance_trigger_counts_by_interaction_kind"],
            {"merge_conflict": result.summary["ego_avoidance_brake_event_count"]},
        )
        self.assertEqual(result.trace_rows[0]["ego_avoidance_target_actor_id"], "merge_risk")
        self.assertEqual(result.trace_rows[0]["ego_avoidance_target_interaction_kind"], "merge_conflict")
        self.assertTrue(result.trace_rows[0]["ego_avoidance_brake_applied"])
        self.assertAlmostEqual(float(result.trace_rows[0]["ego_avoidance_target_ttc_sec"]), 2.2, places=6)

    def test_route_aware_avoidance_applies_interaction_specific_policy(self) -> None:
        canonical_map = json.loads((MAP_FIXTURE_ROOT / "canonical_lane_graph_v0.json").read_text(encoding="utf-8"))
        scenario = load_scenario(
            {
                "scenario_schema_version": "scenario_definition_v0",
                "scenario_id": "route_avoidance_policy_override",
                "duration_sec": 0.5,
                "dt_sec": 0.1,
                "canonical_map": canonical_map,
                "route_definition": {
                    "entry_lane_id": "lane_a",
                    "exit_lane_id": "lane_c",
                    "via_lane_ids": ["lane_b"],
                    "cost_mode": "hops",
                },
                "enable_ego_collision_avoidance": True,
                "avoidance_ttc_threshold_sec": 1.0,
                "ego_max_brake_mps2": 6.0,
                "avoidance_interaction_policy": {
                    "merge_conflict": {"ttc_threshold_sec": 3.0, "brake_scale": 0.5},
                    "same_lane_conflict": {"ttc_threshold_sec": 0.5, "brake_scale": 1.0},
                },
                "ego": {"position_m": 0.0, "speed_mps": 10.0, "lane_id": "lane_a"},
                "npcs": [
                    {"actor_id": "same_lane_far", "position_m": 16.0, "speed_mps": 4.0, "lane_id": "lane_a"},
                    {"actor_id": "merge_risk", "position_m": 18.0, "speed_mps": 4.0, "lane_id": "lane_b"},
                ],
            }
        )

        result = run_object_sim(scenario, seed=42, metadata={"run_id": "ROUTE_AVOID_POLICY_001"})

        self.assertEqual(result.summary["status"], "success")
        self.assertEqual(
            result.summary["avoidance_interaction_policy"],
            {
                "merge_conflict": {"brake_scale": 0.5, "ttc_threshold_sec": 3.0},
                "same_lane_conflict": {"brake_scale": 1.0, "ttc_threshold_sec": 0.5},
            },
        )
        self.assertGreater(result.summary["ego_avoidance_brake_event_count"], 0)
        self.assertEqual(result.summary["ego_avoidance_last_trigger_actor_id"], "merge_risk")
        self.assertEqual(result.summary["ego_avoidance_last_trigger_interaction_kind"], "merge_conflict")
        self.assertAlmostEqual(result.summary["ego_avoidance_last_trigger_ttc_threshold_sec"], 3.0, places=6)
        self.assertAlmostEqual(result.summary["ego_avoidance_last_trigger_brake_scale"], 0.5, places=6)
        self.assertAlmostEqual(result.summary["ego_avoidance_applied_brake_mps2_max"], 3.0, places=6)
        self.assertEqual(result.trace_rows[0]["ego_avoidance_target_actor_id"], "merge_risk")
        self.assertEqual(result.trace_rows[0]["ego_avoidance_target_interaction_kind"], "merge_conflict")
        self.assertAlmostEqual(float(result.trace_rows[0]["ego_avoidance_target_ttc_threshold_sec"]), 3.0, places=6)
        self.assertAlmostEqual(float(result.trace_rows[0]["ego_avoidance_target_brake_scale"]), 0.5, places=6)

    def test_route_aware_avoidance_policy_priority_breaks_equal_ttc_tie(self) -> None:
        canonical_map = json.loads((MAP_FIXTURE_ROOT / "canonical_lane_graph_v0.json").read_text(encoding="utf-8"))
        scenario = load_scenario(
            {
                "scenario_schema_version": "scenario_definition_v0",
                "scenario_id": "route_avoidance_policy_priority",
                "duration_sec": 0.5,
                "dt_sec": 0.1,
                "canonical_map": canonical_map,
                "route_definition": {
                    "entry_lane_id": "lane_a",
                    "exit_lane_id": "lane_c",
                    "via_lane_ids": ["lane_b"],
                    "cost_mode": "hops",
                },
                "enable_ego_collision_avoidance": True,
                "avoidance_ttc_threshold_sec": 3.0,
                "ego_max_brake_mps2": 6.0,
                "avoidance_interaction_policy": {
                    "same_lane_conflict": {"priority": 5},
                    "merge_conflict": {"priority": 0},
                },
                "ego": {"position_m": 0.0, "speed_mps": 10.0, "lane_id": "lane_a"},
                "npcs": [
                    {"actor_id": "same_lane_risk", "position_m": 17.0, "speed_mps": 4.0, "lane_id": "lane_a"},
                    {"actor_id": "merge_risk", "position_m": 17.0, "speed_mps": 4.0, "lane_id": "lane_b"},
                ],
            }
        )

        result = run_object_sim(scenario, seed=42, metadata={"run_id": "ROUTE_AVOID_POLICY_PRIORITY_001"})

        self.assertEqual(result.summary["ego_avoidance_last_trigger_actor_id"], "merge_risk")
        self.assertEqual(result.summary["ego_avoidance_last_trigger_interaction_kind"], "merge_conflict")
        self.assertEqual(result.summary["ego_avoidance_last_trigger_priority"], 0)
        self.assertEqual(result.trace_rows[0]["ego_avoidance_target_actor_id"], "merge_risk")
        self.assertEqual(result.trace_rows[0]["ego_avoidance_target_priority"], 0)

    def test_route_aware_avoidance_policy_max_gap_filters_far_target(self) -> None:
        canonical_map = json.loads((MAP_FIXTURE_ROOT / "canonical_lane_graph_v0.json").read_text(encoding="utf-8"))
        scenario = load_scenario(
            {
                "scenario_schema_version": "scenario_definition_v0",
                "scenario_id": "route_avoidance_policy_max_gap",
                "duration_sec": 0.5,
                "dt_sec": 0.1,
                "canonical_map": canonical_map,
                "route_definition": {
                    "entry_lane_id": "lane_a",
                    "exit_lane_id": "lane_c",
                    "via_lane_ids": ["lane_b"],
                    "cost_mode": "hops",
                },
                "enable_ego_collision_avoidance": True,
                "avoidance_ttc_threshold_sec": 3.0,
                "ego_max_brake_mps2": 6.0,
                "avoidance_interaction_policy": {
                    "same_lane_conflict": {"max_gap_m": 5.0},
                    "merge_conflict": {"priority": 1},
                },
                "ego": {"position_m": 0.0, "speed_mps": 10.0, "lane_id": "lane_a"},
                "npcs": [
                    {"actor_id": "same_lane_far", "position_m": 15.0, "speed_mps": 4.0, "lane_id": "lane_a"},
                    {"actor_id": "merge_risk", "position_m": 18.0, "speed_mps": 4.0, "lane_id": "lane_b"},
                ],
            }
        )

        result = run_object_sim(scenario, seed=42, metadata={"run_id": "ROUTE_AVOID_POLICY_MAX_GAP_001"})

        self.assertEqual(result.summary["ego_avoidance_last_trigger_actor_id"], "merge_risk")
        self.assertEqual(result.summary["ego_avoidance_last_trigger_interaction_kind"], "merge_conflict")
        self.assertIsNone(result.summary["ego_avoidance_last_trigger_max_gap_m"])
        self.assertEqual(result.trace_rows[0]["ego_avoidance_target_actor_id"], "merge_risk")
        self.assertIsNone(result.trace_rows[0]["ego_avoidance_target_max_gap_m"])

    def test_run_object_sim_respects_wall_timeout_override(self) -> None:
        scenario = load_scenario(FIXTURE_ROOT / "highway_safe_following_v0.json")
        result = run_object_sim(
            scenario,
            seed=42,
            wall_timeout_override=1e-9,
            metadata={"run_id": "TIMEOUT_001"},
        )

        self.assertEqual(result.summary["status"], "timeout")
        self.assertTrue(result.summary["timeout"])
        self.assertEqual(result.summary["step_count"], 0)

    def test_object_sim_runner_main_writes_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_root = root / "runs"
            scenario_path = FIXTURE_ROOT / "highway_safe_following_v0.json"
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = object_sim_main(
                    [
                        "--scenario",
                        str(scenario_path),
                        "--run-id",
                        "RUN_SAFE_001",
                        "--seed",
                        "42",
                        "--out",
                        str(out_root),
                    ]
                )

            self.assertEqual(exit_code, 0)
            summary = json.loads((out_root / "RUN_SAFE_001" / "summary.json").read_text(encoding="utf-8"))
            lane_risk = json.loads(
                (out_root / "RUN_SAFE_001" / "lane_risk_summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(summary["run_id"], "RUN_SAFE_001")
            self.assertEqual(summary["status"], "success")
            self.assertEqual(lane_risk["lane_risk_summary_schema_version"], "lane_risk_summary_v0")
            self.assertTrue((out_root / "RUN_SAFE_001" / "trace.csv").exists())

    def test_object_sim_script_bootstraps_src_path(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_object_sim.py"
        proc = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(proc.returncode, 0)
        self.assertIn("object simulation", proc.stdout.lower())


if __name__ == "__main__":
    unittest.main()
