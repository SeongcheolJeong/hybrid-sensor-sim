from __future__ import annotations


def default_autoware_topic_for_output_role(output_role: str, sensor_id: str, backend: str) -> str:
    role = str(output_role).strip()
    sensor = str(sensor_id).strip()
    if not role:
        raise ValueError("output_role must be non-empty")
    if not sensor:
        raise ValueError("sensor_id must be non-empty")
    _ = str(backend).strip()
    mapping = {
        "camera_visible": f"/sensing/camera/{sensor}/image_raw",
        "camera_depth": f"/sensing/camera/{sensor}/depth/image_raw",
        "camera_semantic": f"/sensing/camera/{sensor}/semantic/image_raw",
        "lidar_point_cloud": f"/sensing/lidar/{sensor}/pointcloud",
        "radar_detections": f"/sensing/radar/{sensor}/detections",
        "radar_tracks": f"/sensing/radar/{sensor}/tracks",
    }
    if role not in mapping:
        raise ValueError(f"unsupported output_role for Autoware topic mapping: {role}")
    return mapping[role]


def default_autoware_message_type_for_output_role(output_role: str) -> str:
    role = str(output_role).strip()
    mapping = {
        "camera_visible": "sensor_msgs/msg/Image",
        "camera_depth": "sensor_msgs/msg/Image",
        "camera_semantic": "sensor_msgs/msg/Image",
        "lidar_point_cloud": "sensor_msgs/msg/PointCloud2",
        "radar_detections": "autoware_auto_perception_msgs/msg/DetectedObjects",
        "radar_tracks": "autoware_auto_perception_msgs/msg/TrackedObjects",
    }
    if role not in mapping:
        raise ValueError(f"unsupported output_role for Autoware message mapping: {role}")
    return mapping[role]


def default_autoware_encoding_for_output_role(output_role: str) -> str:
    role = str(output_role).strip()
    mapping = {
        "camera_visible": "rgb8",
        "camera_depth": "32FC1",
        "camera_semantic": "mono8",
        "lidar_point_cloud": "",
        "radar_detections": "",
        "radar_tracks": "",
    }
    if role not in mapping:
        raise ValueError(f"unsupported output_role for Autoware encoding mapping: {role}")
    return mapping[role]
