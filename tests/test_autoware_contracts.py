from __future__ import annotations

import unittest

from hybrid_sensor_sim.autoware.contracts import build_autoware_sensor_contracts
from hybrid_sensor_sim.autoware.frames import (
    AUTOWARE_FRAME_TREE_SCHEMA_VERSION_V0,
    build_autoware_frame_tree,
)
from hybrid_sensor_sim.autoware.topics import (
    default_autoware_message_type_for_output_role,
    default_autoware_topic_for_output_role,
)


class AutowareContractsTests(unittest.TestCase):
    def test_default_topic_and_message_mappings(self) -> None:
        self.assertEqual(
            default_autoware_topic_for_output_role("camera_visible", "cam_front", "awsim"),
            "/sensing/camera/cam_front/image_raw",
        )
        self.assertEqual(
            default_autoware_topic_for_output_role("lidar_point_cloud", "lidar_top", "awsim"),
            "/sensing/lidar/lidar_top/pointcloud",
        )
        self.assertEqual(
            default_autoware_message_type_for_output_role("radar_tracks"),
            "autoware_auto_perception_msgs/msg/TrackedObjects",
        )

    def test_build_autoware_frame_tree_uses_sensor_mounts(self) -> None:
        frame_tree = build_autoware_frame_tree(
            [
                {
                    "sensor_id": "cam_front",
                    "sensor_type": "camera",
                    "attach_to_actor_id": "ego",
                    "extrinsics": {
                        "tx": 1.0,
                        "ty": 0.0,
                        "tz": 1.5,
                        "yaw_deg": 5.0,
                    },
                }
            ]
        )
        self.assertEqual(frame_tree["schema_version"], AUTOWARE_FRAME_TREE_SCHEMA_VERSION_V0)
        self.assertEqual(frame_tree["base_frame"], "base_link")
        self.assertEqual(frame_tree["sensor_frame_count"], 1)
        self.assertEqual(frame_tree["sensor_frames"][0]["frame_id"], "cam_front")
        self.assertEqual(frame_tree["sensor_frames"][0]["attach_to_actor_id"], "ego")
        self.assertEqual(frame_tree["sensor_frames"][0]["translation"]["z"], 1.5)

    def test_build_sensor_contracts_marks_missing_required_outputs(self) -> None:
        contracts = build_autoware_sensor_contracts(
            backend_sensor_output_summary={
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
                                "artifact_key": "camera_projection_json",
                                "resolved_path": "/tmp/cam_front.json",
                                "exists": True,
                            }
                        ],
                    }
                ],
            },
            backend_output_spec={
                "backend": "awsim",
                "expected_outputs_by_sensor": [],
            },
            sensor_mounts=[
                {
                    "sensor_id": "cam_front",
                    "sensor_type": "camera",
                    "enabled": True,
                },
                {
                    "sensor_id": "lidar_top",
                    "sensor_type": "lidar",
                    "enabled": True,
                },
            ],
        )
        self.assertEqual(contracts["available_sensor_count"], 1)
        self.assertEqual(contracts["missing_required_sensor_count"], 1)
        self.assertIn("/sensing/camera/cam_front/image_raw", contracts["available_topics"])
        lidar_contracts = [
            entry
            for entry in contracts["contracts"]
            if entry["sensor_id"] == "lidar_top"
        ]
        self.assertEqual(len(lidar_contracts), 1)
        self.assertEqual(lidar_contracts[0]["output_role"], "lidar_point_cloud")
        self.assertFalse(lidar_contracts[0]["available"])
        self.assertTrue(lidar_contracts[0]["required"])

    def test_build_sensor_contracts_supports_planned_availability_and_explicit_roles(self) -> None:
        contracts = build_autoware_sensor_contracts(
            backend_sensor_output_summary={
                "backend": "awsim",
                "sensors": [
                    {
                        "sensor_id": "cam_depth",
                        "modality": "camera",
                        "outputs": [
                            {
                                "output_role": "camera_depth",
                                "artifact_type": "awsim_camera_depth_json",
                                "data_format": "camera_depth_json",
                                "artifact_key": "sensor_output_cam_depth",
                                "resolved_path": "/tmp/cam_depth/depth_frame.json",
                                "exists": False,
                            }
                        ],
                    }
                ],
            },
            backend_output_spec={
                "backend": "awsim",
                "expected_outputs_by_sensor": [
                    {
                        "sensor_id": "cam_depth",
                        "outputs": [
                            {
                                "output_role": "camera_depth",
                                "artifact_type": "awsim_camera_depth_json",
                                "data_format": "camera_depth_json",
                                "relative_path": "sensor_exports/cam_depth/depth_frame.json",
                                "path_candidates": [],
                            }
                        ],
                    }
                ],
            },
            sensor_mounts=[
                {
                    "sensor_id": "cam_depth",
                    "sensor_type": "camera",
                    "enabled": True,
                }
            ],
            availability_mode="planned",
        )
        self.assertEqual(contracts["availability_mode"], "planned")
        self.assertEqual(contracts["missing_required_sensor_count"], 0)
        self.assertIn("/sensing/camera/cam_depth/depth/image_raw", contracts["available_topics"])
        depth_contracts = [
            entry
            for entry in contracts["contracts"]
            if entry["sensor_id"] == "cam_depth"
        ]
        self.assertEqual(len(depth_contracts), 1)
        self.assertEqual(depth_contracts[0]["output_role"], "camera_depth")
        self.assertTrue(depth_contracts[0]["required"])
        self.assertTrue(depth_contracts[0]["available"])
        self.assertEqual(depth_contracts[0]["availability_mode"], "planned")


if __name__ == "__main__":
    unittest.main()
