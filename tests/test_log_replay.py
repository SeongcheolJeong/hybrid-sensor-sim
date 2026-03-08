from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.scenarios.log_scene import load_log_scene
from hybrid_sensor_sim.scenarios.replay import build_scenario_from_log_scene
from hybrid_sensor_sim.tools.log_replay_runner import main as log_replay_main
from hybrid_sensor_sim.tools.log_scene_augment import augment_log_scene, main as log_scene_augment_main


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "autonomy_e2e" / "p_sim_engine"


class LogReplayTests(unittest.TestCase):
    def test_load_log_scene_rejects_invalid_schema(self) -> None:
        with self.assertRaisesRegex(ValueError, "log_scene_schema_version must be"):
            load_log_scene(
                {
                    "log_scene_schema_version": "wrong",
                    "log_id": "LOG_001",
                    "map_id": "map_a",
                    "ego_initial_speed_mps": 1.0,
                    "lead_vehicle_initial_gap_m": 10.0,
                    "lead_vehicle_speed_mps": 1.0,
                    "duration_sec": 1.0,
                    "dt_sec": 0.1,
                }
            )

    def test_build_scenario_from_log_scene_matches_expected_mapping(self) -> None:
        log_scene = load_log_scene(FIXTURE_ROOT / "log_scene_v0.json")
        scenario = build_scenario_from_log_scene(log_scene)

        self.assertEqual(scenario["scenario_schema_version"], "scenario_definition_v0")
        self.assertEqual(scenario["scenario_id"], "log_replay_LOG_0001")
        self.assertEqual(scenario["ego"]["position_m"], 0.0)
        self.assertEqual(scenario["ego"]["speed_mps"], 18.0)
        self.assertEqual(scenario["npcs"][0]["position_m"], 55.0)
        self.assertEqual(scenario["npcs"][0]["speed_mps"], 17.0)

    def test_build_scenario_from_log_scene_synthesizes_route_from_canonical_map(self) -> None:
        log_scene = load_log_scene(FIXTURE_ROOT / "log_scene_map_route_v0.json")
        scenario = build_scenario_from_log_scene(
            log_scene,
            log_scene_path=FIXTURE_ROOT / "log_scene_map_route_v0.json",
        )

        self.assertEqual(
            scenario["canonical_map_path"],
            str((FIXTURE_ROOT.parent / "p_map_toolset" / "canonical_lane_graph_v0.json").resolve()),
        )
        self.assertEqual(scenario["route_definition"]["entry_lane_id"], "lane_a")
        self.assertEqual(scenario["route_definition"]["exit_lane_id"], "lane_c")
        self.assertEqual(scenario["route_definition"]["via_lane_ids"], ["lane_b"])
        self.assertEqual(scenario["ego"]["lane_id"], "lane_a")
        self.assertEqual(scenario["npcs"][0]["lane_id"], "lane_a")

    def test_augment_log_scene_is_deterministic(self) -> None:
        log_scene = load_log_scene(FIXTURE_ROOT / "log_scene_v0.json")
        augmented = augment_log_scene(
            log_scene,
            ego_speed_scale=1.1,
            lead_gap_offset_m=-5.0,
            lead_speed_offset_mps=2.0,
            suffix="aug",
        )

        self.assertEqual(augmented["log_id"], "LOG_0001_aug")
        self.assertEqual(augmented["ego_initial_speed_mps"], 19.8)
        self.assertEqual(augmented["lead_vehicle_initial_gap_m"], 50.0)
        self.assertEqual(augmented["lead_vehicle_speed_mps"], 19.0)
        self.assertEqual(augmented["augmentation"]["source_log_id"], "LOG_0001")

    def test_log_replay_main_writes_generated_scenario_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_root = root / "runs"
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = log_replay_main(
                    [
                        "--log-scene",
                        str(FIXTURE_ROOT / "log_scene_v0.json"),
                        "--run-id",
                        "LOG_REPLAY_001",
                        "--out",
                        str(out_root),
                        "--seed",
                        "42",
                    ]
                )

            self.assertEqual(exit_code, 0)
            run_dir = out_root / "LOG_REPLAY_001"
            scenario = json.loads((run_dir / "replay_scenario.json").read_text(encoding="utf-8"))
            manifest = json.loads((run_dir / "log_replay_manifest.json").read_text(encoding="utf-8"))
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(scenario["scenario_id"], "log_replay_LOG_0001")
            self.assertEqual(manifest["run_id"], "LOG_REPLAY_001")
            self.assertEqual(summary["run_source"], "log_replay_closed_loop")
            self.assertEqual(summary["map_id"], "map_highway_segment_v0")

    def test_log_replay_main_propagates_map_route_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_root = root / "runs"
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = log_replay_main(
                    [
                        "--log-scene",
                        str(FIXTURE_ROOT / "log_scene_map_route_v0.json"),
                        "--run-id",
                        "LOG_REPLAY_MAP_001",
                        "--out",
                        str(out_root),
                        "--seed",
                        "42",
                    ]
                )

            self.assertEqual(exit_code, 0)
            run_dir = out_root / "LOG_REPLAY_MAP_001"
            scenario = json.loads((run_dir / "replay_scenario.json").read_text(encoding="utf-8"))
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(scenario["route_definition"]["entry_lane_id"], "lane_a")
            self.assertEqual(summary["scenario_route_lane_ids"], ["lane_a", "lane_b", "lane_c"])
            self.assertEqual(summary["scenario_route_lane_count"], 3)
            self.assertEqual(summary["ego_lane_id"], "lane_a")

    def test_log_scene_augment_main_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_path = root / "augmented.json"
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = log_scene_augment_main(
                    [
                        "--input",
                        str(FIXTURE_ROOT / "log_scene_v0.json"),
                        "--out",
                        str(out_path),
                        "--ego-speed-scale",
                        "1.1",
                        "--lead-gap-offset-m",
                        "-5",
                        "--lead-speed-offset-mps",
                        "2.0",
                        "--suffix",
                        "aug",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["log_id"], "LOG_0001_aug")

    def test_log_replay_and_augment_scripts_bootstrap_src_path(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        for script_name, needle in (
            ("run_log_replay.py", "replay a log_scene_v0 payload"),
            ("run_log_scene_augment.py", "augment a log_scene_v0 payload"),
        ):
            script_path = repo_root / "scripts" / script_name
            proc = subprocess.run(
                [sys.executable, str(script_path), "--help"],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0)
            self.assertIn(needle, proc.stdout.lower())


if __name__ == "__main__":
    unittest.main()
