from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.autoware_pipeline_bridge import run_autoware_pipeline_bridge


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


class AutowarePipelineBridgeTests(unittest.TestCase):
    def _write_backend_workflow_fixture(
        self,
        root: Path,
        *,
        include_lidar: bool,
        include_camera_visible: bool = True,
        include_camera_semantic: bool = False,
        embed_camera_semantic_in_visible: bool = False,
        include_radar: bool = False,
        radar_tracks: bool = False,
    ) -> Path:
        output_root = root / "backend_outputs"
        output_root.mkdir(parents=True, exist_ok=True)
        camera_output = output_root / "sensor_exports" / "cam_front" / "rgb_frame.json"
        camera_semantic_output = (
            output_root / "sensor_exports" / "cam_front" / "semantic_frame.json"
        )
        if include_camera_visible:
            camera_output.parent.mkdir(parents=True, exist_ok=True)
            camera_payload = {}
            if embed_camera_semantic_in_visible:
                camera_payload = {
                    "companion_sensor_types": ["SEMANTIC_SEGMENTATION"],
                    "preview_semantic_samples": [{"u": 120.0, "v": 32.0, "label": "vehicle"}],
                }
            camera_output.write_text(
                json.dumps(camera_payload, indent=2, ensure_ascii=True) + "\n",
                encoding="utf-8",
            )
        if include_camera_semantic:
            camera_semantic_output.parent.mkdir(parents=True, exist_ok=True)
            camera_semantic_output.write_text("{}", encoding="utf-8")
        lidar_output = output_root / "sensor_exports" / "lidar_top" / "points.json"
        if include_lidar:
            lidar_output.parent.mkdir(parents=True, exist_ok=True)
            lidar_output.write_text("{}", encoding="utf-8")
        radar_detection_output = output_root / "sensor_exports" / "radar_front" / "detections.json"
        radar_tracks_output = output_root / "sensor_exports" / "radar_front" / "tracks.json"
        if include_radar:
            radar_detection_output.parent.mkdir(parents=True, exist_ok=True)
            radar_detection_output.write_text("{}", encoding="utf-8")
            if radar_tracks:
                radar_tracks_output.write_text("{}", encoding="utf-8")

        playback_contract_path = root / "renderer_playback_contract.json"
        sensor_mounts = [
            {
                "sensor_id": "cam_front",
                "sensor_type": "camera",
                "enabled": True,
                "attach_to_actor_id": "ego",
                "extrinsics": {"tx": 1.0, "ty": 0.0, "tz": 1.5},
            },
            {
                "sensor_id": "lidar_top",
                "sensor_type": "lidar",
                "enabled": True,
                "attach_to_actor_id": "ego",
                "extrinsics": {"tx": 0.0, "ty": 0.0, "tz": 2.0},
            },
        ]
        if include_radar:
            sensor_mounts.append(
                {
                    "sensor_id": "radar_front",
                    "sensor_type": "radar",
                    "enabled": True,
                    "attach_to_actor_id": "ego",
                    "extrinsics": {"tx": 1.2, "ty": 0.0, "tz": 0.9},
                }
            )
        _write_json(
            playback_contract_path,
            {
                "renderer_sensor_mounts": sensor_mounts,
            },
        )
        backend_output_spec_path = root / "backend_output_spec.json"
        expected_outputs_by_sensor = [
            {
                "sensor_id": "cam_front",
                "outputs": [],
            },
            {
                "sensor_id": "lidar_top",
                "outputs": [
                    {
                        "output_role": "lidar_point_cloud",
                        "artifact_type": "awsim_lidar_json_point_cloud",
                        "data_format": "lidar_points_json",
                    }
                ],
            },
        ]
        if include_camera_visible:
            expected_outputs_by_sensor[0]["outputs"].append(
                {
                    "output_role": "camera_visible",
                    "artifact_type": "awsim_camera_rgb_json",
                    "data_format": "camera_projection_json",
                }
            )
        if include_camera_semantic:
            expected_outputs_by_sensor[0]["outputs"].append(
                {
                    "output_role": "camera_semantic",
                    "artifact_type": "awsim_camera_semantic_json",
                    "data_format": "camera_semantic_json",
                }
            )
        if include_radar:
            radar_expected_outputs = [
                {
                    "output_role": "radar_detections",
                    "artifact_type": "awsim_radar_detections_json",
                    "data_format": "radar_targets_json",
                }
            ]
            if radar_tracks:
                radar_expected_outputs.append(
                    {
                        "output_role": "radar_tracks",
                        "artifact_type": "awsim_radar_tracks_json",
                        "data_format": "radar_tracks_json",
                    }
                )
            expected_outputs_by_sensor.append(
                {
                    "sensor_id": "radar_front",
                    "outputs": radar_expected_outputs,
                }
            )
        _write_json(
            backend_output_spec_path,
            {
                "backend": "awsim",
                "output_root": str(output_root.resolve()),
                "expected_outputs_by_sensor": expected_outputs_by_sensor,
            },
        )
        backend_sensor_output_summary_path = root / "backend_sensor_output_summary.json"
        sensors = [
            {
                "sensor_id": "cam_front",
                "modality": "camera",
                "outputs": [],
            }
        ]
        if include_camera_visible:
            sensors[0]["outputs"].append(
                {
                    "output_role": "camera_visible",
                    "artifact_type": "awsim_camera_rgb_json",
                    "data_format": "camera_projection_json",
                    "artifact_key": "camera_projection_json",
                    "resolved_path": str(camera_output.resolve()),
                    "exists": True,
                }
            )
        if include_camera_semantic:
            sensors[0]["outputs"].append(
                {
                    "output_role": "camera_semantic",
                    "artifact_type": "awsim_camera_semantic_json",
                    "data_format": "camera_semantic_json",
                    "artifact_key": "camera_semantic_json",
                    "resolved_path": str(camera_semantic_output.resolve()),
                    "exists": True,
                }
            )
        if include_lidar:
            sensors.append(
                {
                    "sensor_id": "lidar_top",
                    "modality": "lidar",
                    "outputs": [
                        {
                            "output_role": "lidar_point_cloud",
                            "artifact_type": "awsim_lidar_json_point_cloud",
                            "data_format": "lidar_points_json",
                            "artifact_key": "lidar_points_json",
                            "resolved_path": str(lidar_output.resolve()),
                            "exists": True,
                        }
                    ],
                }
            )
        if include_radar:
            radar_outputs = [
                {
                    "output_role": "radar_detections",
                    "artifact_type": "awsim_radar_detections_json",
                    "data_format": "radar_targets_json",
                    "artifact_key": "radar_targets_json",
                    "resolved_path": str(radar_detection_output.resolve()),
                    "exists": True,
                }
            ]
            if radar_tracks:
                radar_outputs.append(
                    {
                        "output_role": "radar_tracks",
                        "artifact_type": "awsim_radar_tracks_json",
                        "data_format": "radar_tracks_json",
                        "artifact_key": "radar_tracks_json",
                        "resolved_path": str(radar_tracks_output.resolve()),
                        "exists": True,
                    }
                )
            sensors.append(
                {
                    "sensor_id": "radar_front",
                    "modality": "radar",
                    "outputs": radar_outputs,
                }
            )
        _write_json(
            backend_sensor_output_summary_path,
            {
                "backend": "awsim",
                "sensors": sensors,
            },
        )
        smoke_summary_path = root / "renderer_backend_smoke_summary.json"
        _write_json(
            smoke_summary_path,
            {
                "backend": "awsim",
                "output_comparison": {"status": "MATCHED", "mismatch_reasons": []},
                "output_smoke_report": {"status": "COMPLETE", "coverage_ratio": 1.0},
                "artifacts": {
                    "renderer_playback_contract": str(playback_contract_path.resolve()),
                    "backend_output_spec": str(backend_output_spec_path.resolve()),
                    "backend_sensor_output_summary": str(backend_sensor_output_summary_path.resolve()),
                },
            },
        )
        backend_workflow_report_path = root / "scenario_backend_smoke_workflow_report_v0.json"
        _write_json(
            backend_workflow_report_path,
            {
                "scenario_backend_smoke_workflow_report_schema_version": "scenario_backend_smoke_workflow_report_v0",
                "backend": "awsim",
                "selection": {"variant_id": "var_001", "logical_scenario_id": "scn_001"},
                "bridge": {"scenario_id": "SCN_001", "source_payload_kind": "scenario_definition_v0"},
                "smoke": {"summary_path": str(smoke_summary_path.resolve())},
                "artifacts": {"smoke_scenario_path": str((root / "smoke_scenario.json").resolve())},
            },
        )
        return backend_workflow_report_path

    def test_bridge_builds_autoware_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            backend_report_path = self._write_backend_workflow_fixture(root, include_lidar=True)
            result = run_autoware_pipeline_bridge(
                backend_smoke_workflow_report_path=str(backend_report_path),
                runtime_backend_workflow_report_path="",
                out_root=root / "autoware_bundle",
                strict=False,
            )
            report = result["report"]
            self.assertEqual(report["status"], "READY")
            self.assertEqual(report["available_sensor_count"], 2)
            self.assertEqual(report["missing_required_sensor_count"], 0)
            self.assertEqual(report["topic_export_count"], 2)
            self.assertEqual(report["materialized_topic_export_count"], 2)
            self.assertEqual(report["required_topic_count"], 2)
            self.assertEqual(report["missing_required_topic_count"], 0)
            self.assertIn("sensor_msgs/msg/Image", report["available_message_types"])
            self.assertIn("sensor_msgs/msg/PointCloud2", report["available_message_types"])
            self.assertTrue(report["dataset_ready"])
            self.assertTrue(report["consumer_ready"])
            self.assertEqual(report["recording_style"], "backend_smoke_export")
            self.assertEqual(report["available_modalities"], ["camera", "lidar"])
            self.assertEqual(report["scenario_source"]["variant_id"], "var_001")
            self.assertIn("/sensing/camera/cam_front/image_raw", report["available_topics"])
            self.assertTrue(Path(report["artifacts"]["pipeline_manifest_path"]).is_file())
            self.assertTrue(Path(report["artifacts"]["dataset_manifest_path"]).is_file())
            self.assertTrue(
                Path(report["artifacts"]["consumer_input_manifest_path"]).is_file()
            )
            topic_index = json.loads(
                Path(report["artifacts"]["topic_export_index_path"]).read_text(encoding="utf-8")
            )
            topic_catalog = json.loads(
                Path(report["artifacts"]["topic_catalog_path"]).read_text(encoding="utf-8")
            )
            self.assertEqual(topic_index["topic_count"], 2)
            self.assertEqual(topic_index["materialized_payload_count"], 2)
            self.assertEqual(topic_catalog["required_topic_count"], 2)
            self.assertEqual(topic_catalog["missing_required_topic_count"], 0)
            self.assertEqual(topic_catalog["available_topic_count"], 2)
            self.assertTrue(Path(topic_index["topics"][0]["export_manifest_path"]).is_file())
            self.assertTrue(Path(topic_index["topics"][0]["payload_path"]).exists())
            dataset_manifest = json.loads(
                Path(report["artifacts"]["dataset_manifest_path"]).read_text(encoding="utf-8")
            )
            consumer_manifest = json.loads(
                Path(report["artifacts"]["consumer_input_manifest_path"]).read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(dataset_manifest["variant_id"], "var_001")
            self.assertEqual(dataset_manifest["logical_scenario_id"], "scn_001")
            self.assertEqual(dataset_manifest["pipeline_status"], "READY")
            self.assertEqual(dataset_manifest["recording_style"], "backend_smoke_export")
            self.assertEqual(dataset_manifest["available_sensor_ids"], ["cam_front", "lidar_top"])
            self.assertEqual(dataset_manifest["topic_export_count"], 2)
            self.assertEqual(dataset_manifest["materialized_topic_export_count"], 2)
            self.assertEqual(dataset_manifest["required_topic_count"], 2)
            self.assertEqual(dataset_manifest["missing_required_topic_count"], 0)
            self.assertIn("sensor_msgs/msg/Image", dataset_manifest["available_message_types"])
            self.assertTrue(consumer_manifest["consumer_ready"])
            self.assertEqual(consumer_manifest["available_topic_count"], 2)
            self.assertEqual(consumer_manifest["required_topic_count"], 2)
            self.assertEqual(consumer_manifest["missing_required_topic_count"], 0)
            self.assertEqual(len(consumer_manifest["consumer_topics"]), 2)
            self.assertEqual(consumer_manifest["subscription_spec_count"], 2)
            self.assertEqual(consumer_manifest["sensor_input_count"], 2)
            self.assertEqual(consumer_manifest["static_transform_count"], 2)
            self.assertEqual(consumer_manifest["processing_stage_count"], 0)
            self.assertEqual(consumer_manifest["ready_processing_stage_count"], 0)
            self.assertEqual(consumer_manifest["degraded_processing_stage_count"], 0)
            self.assertEqual(len(consumer_manifest["subscription_specs"]), 2)
            self.assertEqual(len(consumer_manifest["sensor_inputs"]), 2)
            self.assertEqual(len(consumer_manifest["static_transforms"]), 2)
            self.assertEqual(consumer_manifest["sensor_inputs"][0]["sensor_id"], "cam_front")
            self.assertEqual(
                consumer_manifest["sensor_inputs"][0]["required_topic_count"], 1
            )
            self.assertEqual(
                consumer_manifest["static_transforms"][0]["parent_frame_id"], "base_link"
            )
            self.assertTrue(consumer_manifest["consumer_topics"][0]["payload_exists"])

    def test_bridge_ready_for_tracking_fusion_consumer_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            backend_report_path = self._write_backend_workflow_fixture(
                root,
                include_lidar=True,
                include_radar=True,
                radar_tracks=True,
            )
            result = run_autoware_pipeline_bridge(
                backend_smoke_workflow_report_path=str(backend_report_path),
                runtime_backend_workflow_report_path="",
                out_root=root / "autoware_bundle",
                consumer_profile_id="tracking_fusion_v0",
                strict=False,
            )
            report = result["report"]
            self.assertEqual(report["status"], "READY")
            self.assertEqual(report["availability_mode"], "runtime")
            self.assertEqual(report["consumer_profile_id"], "tracking_fusion_v0")
            self.assertEqual(report["required_topic_count"], 4)
            self.assertEqual(report["missing_required_topic_count"], 0)
            self.assertEqual(report["missing_required_sensor_count"], 0)
            self.assertTrue(report["consumer_ready"])
            self.assertEqual(report["subscription_spec_count"], 4)
            self.assertEqual(report["sensor_input_count"], 3)
            self.assertEqual(report["static_transform_count"], 3)
            self.assertEqual(report["processing_stage_count"], 4)
            self.assertEqual(report["ready_processing_stage_count"], 4)
            self.assertEqual(report["degraded_processing_stage_count"], 0)
            self.assertEqual(report["processing_stage_bundle_count"], 4)
            self.assertEqual(report["ready_processing_stage_bundle_count"], 4)
            self.assertEqual(report["degraded_processing_stage_bundle_count"], 0)
            self.assertIn("/sensing/radar/radar_front/tracks", report["available_topics"])
            self.assertIn(
                "autoware_auto_perception_msgs/msg/TrackedObjects",
                report["available_message_types"],
            )
            self.assertTrue(
                Path(report["artifacts"]["processing_stage_bundle_root"]).is_dir()
            )
            self.assertTrue(
                Path(report["artifacts"]["processing_stage_bundle_index_path"]).is_file()
            )
            consumer_manifest = json.loads(
                Path(report["artifacts"]["consumer_input_manifest_path"]).read_text(
                    encoding="utf-8"
                )
            )
            bundle_index = json.loads(
                Path(report["artifacts"]["processing_stage_bundle_index_path"]).read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(len(consumer_manifest["consumer_topics"]), 4)
            radar_sensor_input = next(
                item
                for item in consumer_manifest["sensor_inputs"]
                if item["sensor_id"] == "radar_front"
            )
            self.assertEqual(consumer_manifest["processing_stage_count"], 4)
            self.assertEqual(consumer_manifest["ready_processing_stage_count"], 4)
            self.assertEqual(consumer_manifest["degraded_processing_stage_count"], 0)
            self.assertEqual(bundle_index["processing_stage_bundle_count"], 4)
            self.assertEqual(bundle_index["ready_processing_stage_bundle_count"], 4)
            self.assertEqual(bundle_index["degraded_processing_stage_bundle_count"], 0)
            self.assertEqual(radar_sensor_input["required_topic_count"], 2)
            self.assertEqual(len(radar_sensor_input["subscriptions"]), 2)
            self.assertEqual(radar_sensor_input["frame_id"], "radar_front")
            self.assertEqual(
                [stage["stage_id"] for stage in consumer_manifest["processing_stages"]],
                [
                    "camera_preprocess",
                    "pointcloud_preprocess",
                    "radar_preprocess",
                    "tracking_fusion",
                ],
            )

    def test_bridge_marks_sidecar_only_exports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            backend_report_path = self._write_backend_workflow_fixture(root, include_lidar=True)
            backend_report = json.loads(backend_report_path.read_text(encoding="utf-8"))
            smoke_summary_path = Path(backend_report["smoke"]["summary_path"])
            smoke_summary = json.loads(smoke_summary_path.read_text(encoding="utf-8"))
            sensor_summary_path = Path(
                smoke_summary["artifacts"]["backend_sensor_output_summary"]
            )
            sensor_summary = json.loads(sensor_summary_path.read_text(encoding="utf-8"))
            for sensor in sensor_summary["sensors"]:
                for output in sensor.get("outputs", []):
                    output["output_origin"] = "sidecar_materialized"
            sensor_summary_path.write_text(
                json.dumps(sensor_summary, indent=2, ensure_ascii=True) + "\n",
                encoding="utf-8",
            )
            smoke_summary["output_smoke_report"]["output_origin_status"] = "SIDECAR_ONLY"
            smoke_summary["output_smoke_report"]["output_origin_counts"] = {
                "backend_runtime": 0,
                "sidecar_materialized": 2,
                "missing": 0,
            }
            smoke_summary["output_smoke_report"]["output_origin_reasons"] = [
                "SIDECAR_OUTPUTS_PRESENT"
            ]
            smoke_summary["output_comparison"]["output_origin_status"] = "SIDECAR_ONLY"
            smoke_summary["output_comparison"]["output_origin_counts"] = {
                "backend_runtime": 0,
                "sidecar_materialized": 2,
                "missing": 0,
            }
            smoke_summary_path.write_text(
                json.dumps(smoke_summary, indent=2, ensure_ascii=True) + "\n",
                encoding="utf-8",
            )

            result = run_autoware_pipeline_bridge(
                backend_smoke_workflow_report_path=str(backend_report_path),
                runtime_backend_workflow_report_path="",
                out_root=root / "autoware_bundle",
                strict=False,
            )
            report = result["report"]
            self.assertEqual(report["status"], "SIDECAR_READY")
            self.assertEqual(report["availability_mode"], "sidecar")

    def test_bridge_degrades_for_semantic_consumer_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            backend_report_path = self._write_backend_workflow_fixture(root, include_lidar=True)
            result = run_autoware_pipeline_bridge(
                backend_smoke_workflow_report_path=str(backend_report_path),
                runtime_backend_workflow_report_path="",
                out_root=root / "autoware_bundle",
                consumer_profile_id="semantic_perception_v0",
                strict=False,
            )
            report = result["report"]
            self.assertEqual(report["status"], "DEGRADED")
            self.assertEqual(report["availability_mode"], "runtime")
            self.assertEqual(report["consumer_profile_id"], "semantic_perception_v0")
            self.assertEqual(report["missing_required_sensor_count"], 1)
            self.assertEqual(report["required_topic_count"], 3)
            self.assertEqual(report["missing_required_topic_count"], 1)
            self.assertEqual(report["processing_stage_count"], 3)
            self.assertEqual(report["ready_processing_stage_count"], 2)
            self.assertEqual(report["degraded_processing_stage_count"], 1)
            self.assertFalse(report["consumer_ready"])
            self.assertEqual(report["topic_export_count"], 3)
            self.assertEqual(report["materialized_topic_export_count"], 2)
            self.assertIn(
                "/sensing/camera/cam_front/semantic/image_raw",
                json.loads(
                    Path(report["artifacts"]["topic_catalog_path"]).read_text(
                        encoding="utf-8"
                    )
                )["missing_required_topics"],
            )

    def test_bridge_merges_supplemental_semantic_runtime_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            primary_report_path = self._write_backend_workflow_fixture(
                root / "primary",
                include_lidar=True,
                include_camera_visible=True,
                include_camera_semantic=False,
            )
            supplemental_report_path = self._write_backend_workflow_fixture(
                root / "supplemental",
                include_lidar=False,
                include_camera_visible=False,
                include_camera_semantic=True,
            )
            result = run_autoware_pipeline_bridge(
                backend_smoke_workflow_report_path=str(primary_report_path),
                supplemental_backend_smoke_workflow_report_paths=[
                    str(supplemental_report_path)
                ],
                runtime_backend_workflow_report_path="",
                out_root=root / "autoware_bundle",
                consumer_profile_id="semantic_perception_v0",
                strict=False,
            )
            report = result["report"]
            self.assertEqual(report["status"], "READY")
            self.assertEqual(report["merged_report_count"], 2)
            self.assertEqual(report["missing_required_sensor_count"], 0)
            self.assertEqual(report["missing_required_topic_count"], 0)
            self.assertIn(
                "/sensing/camera/cam_front/semantic/image_raw",
                report["available_topics"],
            )

    def test_bridge_promotes_embedded_camera_semantic_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            backend_report_path = self._write_backend_workflow_fixture(
                root,
                include_lidar=True,
                include_camera_visible=True,
                include_camera_semantic=False,
                embed_camera_semantic_in_visible=True,
            )
            result = run_autoware_pipeline_bridge(
                backend_smoke_workflow_report_path=str(backend_report_path),
                runtime_backend_workflow_report_path="",
                out_root=root / "autoware_bundle",
                consumer_profile_id="semantic_perception_v0",
                strict=False,
            )
            report = result["report"]
            self.assertEqual(report["status"], "READY")
            self.assertEqual(report["missing_required_topic_count"], 0)
            self.assertEqual(report["missing_required_sensor_count"], 0)
            self.assertIn(
                "/sensing/camera/cam_front/semantic/image_raw",
                report["available_topics"],
            )
            consumer_manifest = json.loads(
                Path(report["artifacts"]["consumer_input_manifest_path"]).read_text(
                    encoding="utf-8"
                )
            )
            self.assertTrue(consumer_manifest["consumer_ready"])
            semantic_topic = next(
                topic
                for topic in consumer_manifest["consumer_topics"]
                if topic["topic"] == "/sensing/camera/cam_front/semantic/image_raw"
            )
            self.assertTrue(str(semantic_topic["payload_path"]).endswith("/rgb_frame.json"))
            self.assertIsNone(semantic_topic["output_origin"])
            topic_catalog = json.loads(
                Path(report["artifacts"]["topic_catalog_path"]).read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(topic_catalog["missing_required_topic_count"], 0)

    def test_bridge_rebases_workspace_paths_from_handoff_smoke_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            (repo_root / "src" / "hybrid_sensor_sim").mkdir(parents=True, exist_ok=True)
            (repo_root / "tests").mkdir(parents=True, exist_ok=True)
            smoke_root = repo_root / "artifacts" / "handoff_smoke"
            output_root = smoke_root / "renderer_runtime" / "backend_outputs"
            camera_output = output_root / "awsim" / "sensor_exports" / "cam_front" / "rgb_frame.json"
            lidar_output = output_root / "awsim" / "sensor_exports" / "lidar_top" / "point_cloud.xyz"
            camera_output.parent.mkdir(parents=True, exist_ok=True)
            lidar_output.parent.mkdir(parents=True, exist_ok=True)
            camera_output.write_text("{}", encoding="utf-8")
            lidar_output.write_text("{}", encoding="utf-8")

            playback_contract_path = smoke_root / "renderer_playback_contract.json"
            _write_json(
                playback_contract_path,
                {
                    "renderer_sensor_mounts": [
                        {
                            "sensor_id": "cam_front",
                            "sensor_type": "camera",
                            "enabled": True,
                            "attach_to_actor_id": "ego",
                            "extrinsics": {"tx": 1.0, "ty": 0.0, "tz": 1.5},
                        },
                        {
                            "sensor_id": "lidar_top",
                            "sensor_type": "lidar",
                            "enabled": True,
                            "attach_to_actor_id": "ego",
                            "extrinsics": {"tx": 0.0, "ty": 0.0, "tz": 2.0},
                        },
                    ]
                },
            )

            backend_output_spec_path = smoke_root / "renderer_runtime" / "backend_output_spec.json"
            _write_json(
                backend_output_spec_path,
                {
                    "backend": "awsim",
                    "output_root": "/workspace/artifacts/handoff_smoke/renderer_runtime/backend_outputs/awsim",
                    "expected_outputs_by_sensor": [
                        {
                            "sensor_id": "cam_front",
                            "outputs": [
                                {
                                    "output_role": "camera_visible",
                                    "artifact_type": "awsim_camera_rgb_json",
                                    "data_format": "camera_projection_json",
                                }
                            ],
                        },
                        {
                            "sensor_id": "lidar_top",
                            "outputs": [
                                {
                                    "output_role": "lidar_point_cloud",
                                    "artifact_type": "awsim_lidar_xyz_point_cloud",
                                    "data_format": "lidar_points_xyz",
                                }
                            ],
                        },
                    ],
                },
            )
            backend_sensor_output_summary_path = smoke_root / "renderer_runtime" / "backend_sensor_output_summary.json"
            _write_json(
                backend_sensor_output_summary_path,
                {
                    "backend": "awsim",
                    "sensors": [
                        {
                            "sensor_id": "cam_front",
                            "modality": "camera",
                            "outputs": [
                                {
                                    "output_role": "camera_visible",
                                    "artifact_type": "awsim_camera_rgb_json",
                                    "data_format": "camera_projection_json",
                                    "artifact_key": "sensor_output_camera_front",
                                    "resolved_path": "/workspace/artifacts/handoff_smoke/renderer_runtime/backend_outputs/awsim/sensor_exports/cam_front/rgb_frame.json",
                                    "exists": True,
                                }
                            ],
                        },
                        {
                            "sensor_id": "lidar_top",
                            "modality": "lidar",
                            "outputs": [
                                {
                                    "output_role": "lidar_point_cloud",
                                    "artifact_type": "awsim_lidar_xyz_point_cloud",
                                    "data_format": "lidar_points_xyz",
                                    "artifact_key": "sensor_output_lidar_top",
                                    "resolved_path": "/workspace/artifacts/handoff_smoke/renderer_runtime/backend_outputs/awsim/sensor_exports/lidar_top/point_cloud.xyz",
                                    "exists": True,
                                }
                            ],
                        },
                    ],
                },
            )
            smoke_summary_path = smoke_root / "renderer_backend_smoke_summary.json"
            _write_json(
                smoke_summary_path,
                {
                    "backend": "awsim",
                    "output_comparison": {"status": "MATCHED", "mismatch_reasons": []},
                    "output_smoke_report": {"status": "COMPLETE", "coverage_ratio": 1.0},
                    "artifacts": {
                        "renderer_playback_contract": "/workspace/artifacts/handoff_smoke/renderer_playback_contract.json",
                        "backend_output_spec": "/workspace/artifacts/handoff_smoke/renderer_runtime/backend_output_spec.json",
                        "backend_sensor_output_summary": "/workspace/artifacts/handoff_smoke/renderer_runtime/backend_sensor_output_summary.json",
                    },
                },
            )
            backend_workflow_report_path = smoke_root / "scenario_backend_smoke_workflow_report_v0.json"
            _write_json(
                backend_workflow_report_path,
                {
                    "scenario_backend_smoke_workflow_report_schema_version": "scenario_backend_smoke_workflow_report_v0",
                    "backend": "awsim",
                    "selection": {"variant_id": "var_handoff", "logical_scenario_id": "scn_handoff"},
                    "bridge": {"scenario_id": "SCN_HANDOFF", "source_payload_kind": "scenario_definition_v0"},
                    "smoke": {"summary_path": str(smoke_summary_path.resolve())},
                    "artifacts": {"smoke_scenario_path": str((smoke_root / "smoke_scenario.json").resolve())},
                },
            )

            result = run_autoware_pipeline_bridge(
                backend_smoke_workflow_report_path=str(backend_workflow_report_path),
                runtime_backend_workflow_report_path="",
                out_root=repo_root / "autoware_bundle",
                strict=False,
            )
            report = result["report"]
            self.assertEqual(report["status"], "READY")
            contracts_payload = json.loads(
                Path(report["artifacts"]["sensor_contracts_path"]).read_text(encoding="utf-8")
            )
            contract_by_role = {
                (entry["sensor_id"], entry["output_role"]): entry
                for entry in contracts_payload["contracts"]
            }
            self.assertEqual(
                contract_by_role[("cam_front", "camera_visible")]["source_resolved_path"],
                str(camera_output.resolve()),
            )
            self.assertEqual(
                contract_by_role[("lidar_top", "lidar_point_cloud")]["source_resolved_path"],
                str(lidar_output.resolve()),
            )

    def test_bridge_strict_mode_fails_for_missing_required_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            backend_report_path = self._write_backend_workflow_fixture(root, include_lidar=False)
            result = run_autoware_pipeline_bridge(
                backend_smoke_workflow_report_path=str(backend_report_path),
                runtime_backend_workflow_report_path="",
                out_root=root / "autoware_bundle",
                strict=True,
            )
            report = result["report"]
            self.assertEqual(report["status"], "FAILED")
            self.assertEqual(report["missing_required_sensor_count"], 1)
            self.assertFalse(report["required_topics_complete"])

    def test_bridge_builds_planned_bundle_from_handoff_ready_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            smoke_input_config_path = root / "smoke_input_config.json"
            _write_json(
                smoke_input_config_path,
                {
                    "renderer_backend": "awsim",
                    "renderer_camera_sensor_id": "cam_front",
                    "renderer_lidar_sensor_id": "lidar_top",
                    "renderer_radar_sensor_id": "radar_front",
                    "camera_extrinsics": {"tx": 1.0, "tz": 1.5},
                    "lidar_extrinsics": {"tz": 2.0},
                    "radar_extrinsics": {"tx": 0.5, "tz": 0.8},
                    "radar_tracking_params": {"tracks": True},
                },
            )
            backend_workflow_report_path = root / "scenario_backend_smoke_workflow_report_v0.json"
            _write_json(
                backend_workflow_report_path,
                {
                    "scenario_backend_smoke_workflow_report_schema_version": "scenario_backend_smoke_workflow_report_v0",
                    "status": "HANDOFF_READY",
                    "backend": "awsim",
                    "selection": {"variant_id": "var_002", "logical_scenario_id": "scn_002"},
                    "bridge": {
                        "scenario_id": "SCN_002",
                        "source_payload_kind": "scenario_definition_v0",
                    },
                    "smoke": {"summary_path": None},
                    "artifacts": {
                        "smoke_input_config_path": str(smoke_input_config_path.resolve()),
                        "smoke_scenario_path": str((root / "smoke_scenario.json").resolve()),
                    },
                },
            )

            result = run_autoware_pipeline_bridge(
                backend_smoke_workflow_report_path=str(backend_workflow_report_path),
                runtime_backend_workflow_report_path="",
                out_root=root / "autoware_bundle",
                consumer_profile_id="tracking_fusion_v0",
                strict=False,
            )
            report = result["report"]
            self.assertEqual(report["status"], "PLANNED")
            self.assertEqual(report["availability_mode"], "planned")
            self.assertEqual(report["missing_required_sensor_count"], 0)
            self.assertEqual(report["topic_export_count"], 4)
            self.assertEqual(report["materialized_topic_export_count"], 0)
            self.assertEqual(report["required_topic_count"], 4)
            self.assertEqual(report["missing_required_topic_count"], 0)
            self.assertTrue(report["consumer_ready"])
            self.assertIn(
                "autoware_auto_perception_msgs/msg/TrackedObjects",
                report["available_message_types"],
            )
            self.assertTrue(report["required_topics_complete"])
            self.assertTrue(report["frame_tree_complete"])
            self.assertIn("/sensing/camera/cam_front/image_raw", report["available_topics"])
            self.assertIn("/sensing/lidar/lidar_top/pointcloud", report["available_topics"])
            self.assertIn("/sensing/radar/radar_front/tracks", report["available_topics"])
            self.assertIn("/sensing/radar/radar_front/detections", report["available_topics"])
            topic_index = json.loads(
                Path(report["artifacts"]["topic_export_index_path"]).read_text(encoding="utf-8")
            )
            topic_catalog = json.loads(
                Path(report["artifacts"]["topic_catalog_path"]).read_text(encoding="utf-8")
            )
            self.assertEqual(topic_index["topic_count"], 4)
            self.assertEqual(topic_index["materialized_payload_count"], 0)
            self.assertEqual(topic_catalog["required_topic_count"], 4)
            self.assertEqual(topic_catalog["missing_required_topic_count"], 0)
            self.assertTrue(
                all(entry["payload_path"] is None for entry in topic_index["topics"])
            )
            dataset_manifest = json.loads(
                Path(report["artifacts"]["dataset_manifest_path"]).read_text(encoding="utf-8")
            )
            consumer_manifest = json.loads(
                Path(report["artifacts"]["consumer_input_manifest_path"]).read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(dataset_manifest["recording_style"], "planned_backend_export")
            self.assertEqual(dataset_manifest["topic_export_count"], 4)
            self.assertEqual(dataset_manifest["required_topic_count"], 4)
            self.assertEqual(dataset_manifest["missing_required_topic_count"], 0)
            self.assertEqual(consumer_manifest["required_topic_count"], 4)
            self.assertEqual(consumer_manifest["missing_required_topic_count"], 0)
            self.assertEqual(len(consumer_manifest["consumer_topics"]), 4)
            self.assertEqual(consumer_manifest["subscription_spec_count"], 4)
            self.assertEqual(consumer_manifest["sensor_input_count"], 3)
            self.assertEqual(consumer_manifest["static_transform_count"], 3)
            self.assertEqual(consumer_manifest["processing_stage_count"], 4)
            self.assertEqual(consumer_manifest["ready_processing_stage_count"], 4)
            self.assertEqual(consumer_manifest["degraded_processing_stage_count"], 0)
            self.assertTrue(
                all(
                    transform["parent_frame_id"] == "base_link"
                    for transform in consumer_manifest["static_transforms"]
                )
            )

    def test_script_bootstraps_src_path(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_autoware_pipeline_bridge.py"
        completed = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("Autoware-facing sensor/data contract bundle", completed.stdout)


if __name__ == "__main__":
    unittest.main()
