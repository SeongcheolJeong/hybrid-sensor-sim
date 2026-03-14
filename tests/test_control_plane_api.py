from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from hybrid_sensor_sim.server.db import ControlPlaneDB
from hybrid_sensor_sim.server.jobs import JobManager

REPO_ROOT = Path("/Users/seongcheoljeong/Documents/Test")
SCENARIO_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "autonomy_e2e" / "p_sim_engine" / "highway_safe_following_v0.json"
AUTOWARE_ARTIFACT_ROOT = (
    REPO_ROOT
    / "artifacts"
    / "scenario_runtime_backend_rebridge_auto_semantic_probe_v2"
    / "autoware"
    / "autoware"
)


class ControlPlaneAPITests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory(prefix="control-plane-api-")
        self.tmp_path = Path(self.tmpdir.name)
        self.db_path = self.tmp_path / "index.sqlite"
        self._original_path = os.environ.get("PATH", "")
        os.environ["CONTROL_PLANE_REPO_ROOT"] = str(REPO_ROOT)
        os.environ["CONTROL_PLANE_DB_PATH"] = str(self.db_path)
        self._reset_server_app_globals()
        from hybrid_sensor_sim.server.app import build_app

        self.client = TestClient(build_app())

    def tearDown(self) -> None:
        self.client.close()
        self._reset_server_app_globals()
        os.environ.pop("CONTROL_PLANE_REPO_ROOT", None)
        os.environ.pop("CONTROL_PLANE_DB_PATH", None)
        os.environ["PATH"] = self._original_path
        self.tmpdir.cleanup()

    def _reset_server_app_globals(self) -> None:
        import hybrid_sensor_sim.server.app as server_app

        server_app._APP_DB = None
        server_app._APP_JOB_MANAGER = None

    def _wait_for_run(self, run_id: str, timeout_s: float = 30.0) -> dict[str, object]:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            response = self.client.get(f"/api/v1/runs/{run_id}")
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            if payload["status"] not in {"PLANNED", "RUNNING"}:
                return payload
            time.sleep(0.2)
        self.fail(f"run {run_id} did not finish within {timeout_s}s")

    def test_health_endpoint(self) -> None:
        response = self.client.get("/api/v1/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_projects_endpoints(self) -> None:
        response = self.client.get("/api/v1/projects")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload[0]["project_id"], "default")

        response = self.client.get("/api/v1/projects/default")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["project_id"], "default")

        response = self.client.post(
            "/api/v1/projects",
            json={"name": "Control Plane Demo", "description": "test project", "root_path": str(REPO_ROOT)},
        )
        self.assertEqual(response.status_code, 200)
        created = response.json()
        self.assertEqual(created["project_id"], "control-plane-demo")

    def test_scenarios_and_runtime_summary_endpoints(self) -> None:
        scenarios = self.client.get("/api/v1/scenarios")
        self.assertEqual(scenarios.status_code, 200)
        payload = scenarios.json()
        self.assertTrue(any(item["name"] == "highway_safe_following_v0" for item in payload))

        runtime = self.client.get("/api/v1/runtime/strategy-summary")
        self.assertEqual(runtime.status_code, 200)
        runtime_payload = runtime.json()
        self.assertIn("backends", runtime_payload)
        self.assertIn("recommended_next_command", runtime_payload)

        history = self.client.get("/api/v1/history/summary")
        self.assertEqual(history.status_code, 200)
        history_payload = history.json()
        self.assertEqual(history_payload["schema_version"], "history_summary_v0")
        self.assertGreaterEqual(history_payload["project_count"], 1)

    def test_object_sim_run_and_artifacts(self) -> None:
        out_root = self.tmp_path / "object-sim-run"
        launch = self.client.post(
            "/api/v1/runs/object-sim",
            json={
                "project_id": "default",
                "payload": {
                    "scenario_path": str(SCENARIO_FIXTURE),
                    "run_id": "CP_OBJECT_SIM_TEST",
                    "out_root": str(out_root),
                    "seed": 7,
                },
            },
        )
        self.assertEqual(launch.status_code, 200)
        run_id = launch.json()["run_id"]

        run_detail = self._wait_for_run(run_id)
        self.assertEqual(run_detail["status"], "SUCCEEDED")
        self.assertTrue(run_detail["summary_json_path"].endswith("summary.json"))

        artifacts = self.client.get(f"/api/v1/runs/{run_id}/artifacts")
        self.assertEqual(artifacts.status_code, 200)
        artifact_payload = artifacts.json()
        self.assertTrue(any(item["display_name"] == "summary.json" for item in artifact_payload))

        stream = self.client.get(f"/api/v1/runs/{run_id}/status-stream")
        self.assertEqual(stream.status_code, 200)
        self.assertIn("SUCCEEDED", stream.text)

        content = self.client.get(
            "/api/v1/artifacts/content",
            params={"path": run_detail["summary_json_path"]},
        )
        self.assertEqual(content.status_code, 200)
        content_payload = content.json()
        self.assertEqual(content_payload["status"], "SUCCEEDED")

    def test_probe_set_run_indexes_summary_artifacts(self) -> None:
        out_root = self.tmp_path / "probe-set-run"
        launch = self.client.post(
            "/api/v1/runs/probe-set",
            json={
                "project_id": "default",
                "payload": {
                    "probe_set_id": "awsim_real_v0",
                    "out_root": str(out_root),
                },
            },
        )
        self.assertEqual(launch.status_code, 200)
        run_id = launch.json()["run_id"]

        run_detail = self._wait_for_run(run_id, timeout_s=60.0)
        self.assertEqual(run_detail["status"], "FAILED")
        self.assertTrue(run_detail["summary_json_path"].endswith("scenario_runtime_backend_probe_set_report_v0.json"))

    def test_job_manager_normalizes_probe_status_vocabulary(self) -> None:
        db = ControlPlaneDB(db_path=self.db_path, repo_root=REPO_ROOT)
        jobs = JobManager(db, repo_root=REPO_ROOT)
        self.assertEqual(jobs._canonical_run_status("PASS"), "READY")
        self.assertEqual(jobs._canonical_run_status("FAIL"), "FAILED")

    def test_autoware_bundle_endpoint(self) -> None:
        db = ControlPlaneDB(db_path=self.db_path, repo_root=REPO_ROOT)
        run_id = "CP_AUTOWARE_BUNDLE_TEST"
        artifact_root = self.tmp_path / run_id
        artifact_root.mkdir(parents=True, exist_ok=True)
        db.create_run(
            run_id=run_id,
            run_type="runtime_backend",
            project_id="default",
            source_kind="api_request",
            artifact_root=str(artifact_root),
            request_payload={},
        )
        db.update_run(
            run_id,
            status="SUCCEEDED",
            summary_json_path=str(AUTOWARE_ARTIFACT_ROOT / "autoware_pipeline_manifest.json"),
            result_payload={
                "autoware_pipeline_manifest_path": str(AUTOWARE_ARTIFACT_ROOT / "autoware_pipeline_manifest.json"),
                "autoware_dataset_manifest_path": str(AUTOWARE_ARTIFACT_ROOT / "autoware_dataset_manifest.json"),
                "autoware_topic_catalog_path": str(AUTOWARE_ARTIFACT_ROOT / "autoware_topic_catalog.json"),
                "autoware_consumer_input_manifest_path": str(AUTOWARE_ARTIFACT_ROOT / "autoware_consumer_input_manifest.json"),
            },
            status_reason_codes=[],
            recommended_next_command="",
            error_message="",
        )
        db.replace_run_artifacts(
            run_id,
            [
                {
                    "artifact_type": "json",
                    "path": str(AUTOWARE_ARTIFACT_ROOT / "autoware_pipeline_manifest.json"),
                    "mime_type": "application/json",
                    "display_name": "autoware_pipeline_manifest.json",
                },
                {
                    "artifact_type": "json",
                    "path": str(AUTOWARE_ARTIFACT_ROOT / "autoware_dataset_manifest.json"),
                    "mime_type": "application/json",
                    "display_name": "autoware_dataset_manifest.json",
                },
                {
                    "artifact_type": "json",
                    "path": str(AUTOWARE_ARTIFACT_ROOT / "autoware_topic_catalog.json"),
                    "mime_type": "application/json",
                    "display_name": "autoware_topic_catalog.json",
                },
                {
                    "artifact_type": "json",
                    "path": str(AUTOWARE_ARTIFACT_ROOT / "autoware_consumer_input_manifest.json"),
                    "mime_type": "application/json",
                    "display_name": "autoware_consumer_input_manifest.json",
                },
            ],
        )

        response = self.client.get(f"/api/v1/autoware/{run_id}/bundle")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["run_id"], run_id)
        self.assertIn("/sensing/lidar/lidar_top/pointcloud", payload["available_topics"])
        self.assertTrue(payload["pipeline_manifest_path"].endswith("autoware_pipeline_manifest.json"))

    def test_closed_loop_demo_run_endpoint(self) -> None:
        env = self._build_closed_loop_fixture_env()
        launch = self.client.post(
            "/api/v1/runs/closed-loop-demo",
            json={
                "project_id": "default",
                "payload": {
                    "scenario_path": str(SCENARIO_FIXTURE),
                    "linux_runtime_root": str(env["linux_root"]),
                    "autoware_workspace_root": str(env["autoware_root"]),
                    "awsim_runtime_root": str(env["awsim_root"]),
                    "map_path": str(REPO_ROOT / "tests" / "fixtures" / "autonomy_e2e" / "p_map_toolset" / "simple_map_v0.json"),
                    "route_path": str(REPO_ROOT / "tests" / "fixtures" / "autonomy_e2e" / "p_sim_engine" / "highway_map_route_following_v0.json"),
                    "autoware_pipeline_manifest_path": str(env["pipeline_manifest_path"]),
                    "autoware_dataset_manifest_path": str(env["dataset_manifest_path"]),
                    "autoware_topic_catalog_path": str(env["topic_catalog_path"]),
                    "autoware_consumer_input_manifest_path": str(env["consumer_input_manifest_path"]),
                    "out_root": str(self.tmp_path / "closed-loop-run"),
                    "run_duration_sec": 1.5,
                    "heartbeat_timeout_sec": 3.0,
                    "poll_interval_sec": 0.1,
                    "startup_grace_sec": 1.0,
                    "record_video": True,
                    "record_rosbag": True,
                    "strict_capture": True,
                    "allow_non_linux_host": True,
                    "awsim_launch_command": "trap 'exit 0' TERM INT; while true; do sleep 1; done",
                    "autoware_launch_command": "trap 'exit 0' TERM INT; while true; do sleep 1; done",
                    "route_goal_command": "true",
                    "localization_check_command": "true",
                    "perception_check_command": "true",
                    "planning_check_command": "true",
                    "control_check_command": "true",
                    "vehicle_motion_check_command": "true",
                    "route_completion_check_command": "false",
                    "video_capture_command": "mkdir -p \"$(dirname \\\"$AWSIM_CAMERA_CAPTURE_PATH\\\")\"; touch \"$AWSIM_CAMERA_CAPTURE_PATH\"; trap 'exit 0' TERM INT; while true; do sleep 1; done",
                    "rosbag_record_command": "mkdir -p \"$ROSBAG_ROOT\"; touch \"$ROSBAG_ROOT/demo.db3\"; trap 'exit 0' TERM INT; while true; do sleep 1; done",
                },
            },
        )
        self.assertEqual(launch.status_code, 200)
        run_id = launch.json()["run_id"]
        run_detail = self._wait_for_run(run_id, timeout_s=20.0)
        self.assertEqual(run_detail["status"], "SUCCEEDED")
        self.assertTrue(run_detail["summary_json_path"].endswith("scenario_closed_loop_demo_report_v0.json"))

        artifacts = self.client.get(f"/api/v1/runs/{run_id}/artifacts")
        self.assertEqual(artifacts.status_code, 200)
        artifact_names = {item["display_name"] for item in artifacts.json()}
        self.assertIn("scenario_closed_loop_demo_report_v0.json", artifact_names)
        self.assertIn("run_telemetry.json", artifact_names)
        self.assertIn("awsim_camera_capture.mp4", artifact_names)

    def _build_closed_loop_fixture_env(self) -> dict[str, Path]:
        linux_root = self.tmp_path / "linux-runtime"
        bin_root = linux_root / "bin"
        bin_root.mkdir(parents=True, exist_ok=True)
        awsim_root = self.tmp_path / "awsim-runtime"
        awsim_root.mkdir(parents=True, exist_ok=True)
        (awsim_root / "AWSIM-Demo-Lightweight.x86_64").write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        (awsim_root / "AWSIM-Demo-Lightweight.x86_64").chmod(0o755)

        autoware_root = self.tmp_path / "autoware"
        fake_bin = self.tmp_path / "fake-bin"
        fake_bin.mkdir(parents=True, exist_ok=True)
        ros2_bin = fake_bin / "ros2"
        ros2_bin.write_text("#!/usr/bin/env bash\necho ros2-fake\n", encoding="utf-8")
        ros2_bin.chmod(0o755)
        setup_bash = autoware_root / "install" / "setup.bash"
        setup_bash.parent.mkdir(parents=True, exist_ok=True)
        setup_bash.write_text(f"#!/usr/bin/env bash\nexport PATH={str(fake_bin)}:$PATH\n", encoding="utf-8")

        self._write_helper(bin_root / "launch_awsim_closed_loop.sh", "trap 'exit 0' TERM INT\nwhile true; do sleep 1; done\n")
        self._write_helper(bin_root / "launch_autoware_closed_loop.sh", "trap 'exit 0' TERM INT\nwhile true; do sleep 1; done\n")
        self._write_helper(bin_root / "send_route_goal.sh", "exit 0\n")
        for helper_name in (
            "check_localization_ready.sh",
            "check_perception_ready.sh",
            "check_planning_ready.sh",
            "check_control_ready.sh",
            "check_vehicle_motion.sh",
        ):
            self._write_helper(bin_root / helper_name, "exit 0\n")
        self._write_helper(bin_root / "check_route_completed.sh", "exit 1\n")
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

        bundle_root = self.tmp_path / "autoware-bundle"
        bundle_root.mkdir(parents=True, exist_ok=True)
        pipeline_manifest_path = bundle_root / "autoware_pipeline_manifest.json"
        dataset_manifest_path = bundle_root / "autoware_dataset_manifest.json"
        topic_catalog_path = bundle_root / "autoware_topic_catalog.json"
        consumer_input_manifest_path = bundle_root / "autoware_consumer_input_manifest.json"
        pipeline_manifest_path.write_text('{"status":"READY","consumer_profile":"semantic_perception_v0"}\n', encoding="utf-8")
        dataset_manifest_path.write_text('{"dataset_ready":true}\n', encoding="utf-8")
        topic_catalog_path.write_text('{"available_topics":["/sensing/camera/camera_front/image_raw"],"missing_required_topics":[]}\n', encoding="utf-8")
        consumer_input_manifest_path.write_text('{"consumer_profile_id":"semantic_perception_v0","processing_stages":[{"stage_id":"semantic_camera_ingest","status":"READY"}]}\n', encoding="utf-8")

        os.environ["PATH"] = f"{str(fake_bin)}:{self._original_path}"
        return {
            "linux_root": linux_root,
            "autoware_root": autoware_root,
            "awsim_root": awsim_root,
            "pipeline_manifest_path": pipeline_manifest_path,
            "dataset_manifest_path": dataset_manifest_path,
            "topic_catalog_path": topic_catalog_path,
            "consumer_input_manifest_path": consumer_input_manifest_path,
        }

    def _write_helper(self, path: Path, body: str) -> None:
        path.write_text("#!/usr/bin/env bash\nset -e\n" + body, encoding="utf-8")
        path.chmod(0o755)


if __name__ == "__main__":
    unittest.main()
