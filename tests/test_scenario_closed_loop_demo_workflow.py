from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.scenario_closed_loop_demo_workflow import (
    SCENARIO_CLOSED_LOOP_DEMO_REPORT_SCHEMA_VERSION_V0,
    run_scenario_closed_loop_demo,
)

REPO_ROOT = Path("/Users/seongcheoljeong/Documents/Test")
SCENARIO_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "autonomy_e2e" / "p_sim_engine" / "highway_safe_following_v0.json"
MAP_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "autonomy_e2e" / "p_map_toolset" / "simple_map_v0.json"
ROUTE_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "autonomy_e2e" / "p_sim_engine" / "highway_map_route_following_v0.json"


class ScenarioClosedLoopDemoWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory(prefix="closed-loop-demo-")
        self.tmp_path = Path(self.tmpdir.name)

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_preflight_blocks_when_runtime_assets_are_missing(self) -> None:
        linux_root = self.tmp_path / "linux-runtime"
        linux_root.mkdir(parents=True, exist_ok=True)
        out_root = self.tmp_path / "preflight-run"

        result = run_scenario_closed_loop_demo(
            scenario_path=str(SCENARIO_FIXTURE),
            linux_runtime_root=str(linux_root),
            autoware_workspace_root=str(self.tmp_path / "missing-autoware"),
            awsim_runtime_root=str(self.tmp_path / "missing-awsim"),
            map_path=str(MAP_FIXTURE),
            route_path=str(ROUTE_FIXTURE),
            out_root=out_root,
            preflight_only=True,
            allow_non_linux_host=True,
        )

        self.assertEqual(result["status"], "BLOCKED")
        self.assertIn("AUTOWARE_WORKSPACE_MISSING", result["status_reason_codes"])
        self.assertIn("ROS2_MISSING", result["status_reason_codes"])
        self.assertIn("AWSIM_RUNTIME_MISSING", result["status_reason_codes"])
        self.assertIn("TOPIC_BRIDGE_MISSING", result["status_reason_codes"])
        report = result["report"]
        self.assertEqual(
            report["scenario_closed_loop_demo_report_schema_version"],
            SCENARIO_CLOSED_LOOP_DEMO_REPORT_SCHEMA_VERSION_V0,
        )
        self.assertTrue(Path(result["report_path"]).exists())

    def test_successful_demo_with_helper_scripts_and_capture(self) -> None:
        linux_root = self.tmp_path / "linux-runtime"
        bin_root = linux_root / "bin"
        bin_root.mkdir(parents=True, exist_ok=True)
        awsim_root = self.tmp_path / "awsim-runtime"
        awsim_root.mkdir(parents=True, exist_ok=True)
        awsim_binary = awsim_root / "AWSIM-Demo-Lightweight.x86_64"
        awsim_binary.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        awsim_binary.chmod(0o755)

        autoware_root = self.tmp_path / "autoware"
        fake_bin = self.tmp_path / "fake-bin"
        fake_bin.mkdir(parents=True, exist_ok=True)
        ros2_bin = fake_bin / "ros2"
        ros2_bin.write_text("#!/usr/bin/env bash\necho ros2-fake\n", encoding="utf-8")
        ros2_bin.chmod(0o755)
        setup_bash = autoware_root / "install" / "setup.bash"
        setup_bash.parent.mkdir(parents=True, exist_ok=True)
        setup_bash.write_text(
            f"#!/usr/bin/env bash\nexport PATH={str(fake_bin)}:$PATH\n",
            encoding="utf-8",
        )

        self._write_helper(
            bin_root / "launch_awsim_closed_loop.sh",
            "trap 'exit 0' TERM INT\nwhile true; do sleep 1; done\n",
        )
        self._write_helper(
            bin_root / "launch_autoware_closed_loop.sh",
            "trap 'exit 0' TERM INT\nwhile true; do sleep 1; done\n",
        )
        self._write_helper(bin_root / "send_route_goal.sh", "touch \"$RUN_OUT_ROOT/route_goal_sent.txt\"\n")
        self._write_helper(bin_root / "check_localization_ready.sh", "exit 0\n")
        self._write_helper(bin_root / "check_perception_ready.sh", "exit 0\n")
        self._write_helper(bin_root / "check_planning_ready.sh", "exit 0\n")
        self._write_helper(bin_root / "check_control_ready.sh", "exit 0\n")
        self._write_helper(bin_root / "check_vehicle_motion.sh", "exit 0\n")
        self._write_helper(bin_root / "check_route_completed.sh", "exit 0\n")
        self._write_helper(
            bin_root / "capture_awsim_video.sh",
            "mkdir -p \"$(dirname \"$AWSIM_CAMERA_CAPTURE_PATH\")\"\n"
            "touch \"$AWSIM_CAMERA_CAPTURE_PATH\"\n"
            "trap 'exit 0' TERM INT\nwhile true; do sleep 1; done\n",
        )
        self._write_helper(
            bin_root / "record_rosbag.sh",
            "mkdir -p \"$ROSBAG_ROOT\"\n"
            "touch \"$ROSBAG_ROOT/demo.db3\"\n"
            "trap 'exit 0' TERM INT\nwhile true; do sleep 1; done\n",
        )

        autoware_bundle_root = self.tmp_path / "autoware-bundle"
        autoware_bundle_root.mkdir(parents=True, exist_ok=True)
        pipeline_manifest_path = autoware_bundle_root / "autoware_pipeline_manifest.json"
        dataset_manifest_path = autoware_bundle_root / "autoware_dataset_manifest.json"
        topic_catalog_path = autoware_bundle_root / "autoware_topic_catalog.json"
        consumer_input_manifest_path = autoware_bundle_root / "autoware_consumer_input_manifest.json"
        pipeline_manifest_path.write_text(
            '{"status":"READY","consumer_profile":"semantic_perception_v0"}\n',
            encoding="utf-8",
        )
        dataset_manifest_path.write_text('{"dataset_ready":true}\n', encoding="utf-8")
        topic_catalog_path.write_text(
            '{"available_topics":["/sensing/camera/camera_front/image_raw"],"missing_required_topics":[]}\n',
            encoding="utf-8",
        )
        consumer_input_manifest_path.write_text(
            '{"consumer_profile_id":"semantic_perception_v0","processing_stages":[{"stage_id":"semantic_camera_ingest","status":"READY"}]}\n',
            encoding="utf-8",
        )

        previous_path = os.environ.get("PATH", "")
        os.environ["PATH"] = f"{fake_bin}:{previous_path}"
        try:
            result = run_scenario_closed_loop_demo(
                scenario_path=str(SCENARIO_FIXTURE),
                linux_runtime_root=str(linux_root),
                autoware_workspace_root=str(autoware_root),
                awsim_runtime_root=str(awsim_root),
                map_path=str(MAP_FIXTURE),
                route_path=str(ROUTE_FIXTURE),
                out_root=self.tmp_path / "success-run",
                autoware_pipeline_manifest_path=str(pipeline_manifest_path),
                autoware_dataset_manifest_path=str(dataset_manifest_path),
                autoware_topic_catalog_path=str(topic_catalog_path),
                autoware_consumer_input_manifest_path=str(consumer_input_manifest_path),
                run_duration_sec=0.5,
                heartbeat_timeout_sec=3.0,
                poll_interval_sec=0.1,
                startup_grace_sec=1.0,
                record_video=True,
                record_rviz=False,
                record_rosbag=True,
                strict_capture=True,
                allow_non_linux_host=True,
            )
        finally:
            os.environ["PATH"] = previous_path

        self.assertEqual(result["status"], "SUCCEEDED")
        self.assertTrue(Path(result["report_path"]).exists())
        self.assertTrue(Path(result["markdown_report_path"]).exists())
        self.assertTrue(Path(result["telemetry_path"]).exists())
        report = result["report"]
        self.assertTrue(report["status_summary"]["awsim_launch_ready"])
        self.assertTrue(report["status_summary"]["autoware_launch_ready"])
        self.assertTrue(report["status_summary"]["control_ready"])
        self.assertTrue(report["status_summary"]["vehicle_motion_confirmed"])
        self.assertTrue(report["capture"]["video_paths"])
        self.assertTrue(report["capture"]["rosbag_path"])

    def _write_helper(self, path: Path, body: str) -> None:
        path.write_text("#!/usr/bin/env bash\nset -e\n" + body, encoding="utf-8")
        path.chmod(0o755)


if __name__ == "__main__":
    unittest.main()
