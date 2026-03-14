export const DEFAULT_OBJECT_SIM_PAYLOAD = {
  scenario_path:
    "/Users/seongcheoljeong/Documents/Test/tests/fixtures/autonomy_e2e/p_sim_engine/highway_safe_following_v0.json",
  run_id: "CP_OBJECT_SIM_001",
  seed: 7,
  out_root: "/Users/seongcheoljeong/Documents/Test/artifacts/control_plane/runs/object_sim",
};

export const DEFAULT_BATCH_WORKFLOW_PAYLOAD = {
  project_id: "default",
  scenario_language_profile: "highway_mixed_payloads_v0",
  matrix_scenario_path:
    "/Users/seongcheoljeong/Documents/Test/tests/fixtures/autonomy_e2e/p_sim_engine/highway_safe_following_v0.json",
  out_root: "/Users/seongcheoljeong/Documents/Test/artifacts/control_plane/runs/batch_workflow",
  execution_max_variants: 1,
  traffic_profile_ids: ["sumo_highway_balanced_v0"],
  traffic_actor_pattern_ids: ["sumo_platoon_sparse_v0"],
  traffic_npc_speed_scale_values: [1],
  tire_friction_coeff_values: [1],
  surface_friction_scale_values: [1],
};

export const DEFAULT_BACKEND_SMOKE_PAYLOAD = {
  project_id: "default",
  smoke_config_path: "/Users/seongcheoljeong/Documents/Test/configs/renderer_backend_smoke.awsim.example.json",
  backend: "awsim",
  out_root: "/Users/seongcheoljeong/Documents/Test/artifacts/control_plane/runs/backend_smoke",
  skip_smoke: true,
};

export const DEFAULT_RUNTIME_BACKEND_PAYLOAD = {
  project_id: "default",
  scenario_language_profile: "highway_mixed_payloads_v0",
  matrix_scenario_path:
    "/Users/seongcheoljeong/Documents/Test/tests/fixtures/autonomy_e2e/p_sim_engine/highway_safe_following_v0.json",
  smoke_config_path: "/Users/seongcheoljeong/Documents/Test/configs/renderer_backend_smoke.awsim.example.json",
  backend: "awsim",
  out_root: "/Users/seongcheoljeong/Documents/Test/artifacts/control_plane/runs/runtime_backend",
  execution_max_variants: 1,
  skip_smoke: true,
};

export const DEFAULT_REBRIDGE_PAYLOAD = {
  project_id: "default",
  runtime_backend_workflow_report_path:
    "/Users/seongcheoljeong/Documents/Test/artifacts/scenario_runtime_backend_actual_awsim_run/scenario_runtime_backend_workflow_report_v0.json",
  out_root: "/Users/seongcheoljeong/Documents/Test/artifacts/control_plane/runs/rebridge",
};

export const DEFAULT_PROBE_SET_PAYLOAD = {
  project_id: "default",
  probe_set_id: "hybrid_runtime_readiness_v0",
  out_root: "/Users/seongcheoljeong/Documents/Test/artifacts/control_plane/runs/probe_set",
};

export const DEFAULT_CLOSED_LOOP_DEMO_PAYLOAD = {
  project_id: "default",
  scenario_path:
    "/Users/seongcheoljeong/Documents/Test/tests/fixtures/autonomy_e2e/p_sim_engine/highway_safe_following_v0.json",
  linux_runtime_root: "/opt/hybrid-runtime",
  autoware_workspace_root: "/opt/autoware",
  awsim_runtime_root: "/opt/awsim/AWSIM-Demo-Lightweight",
  map_path:
    "/Users/seongcheoljeong/Documents/Test/tests/fixtures/autonomy_e2e/p_map_toolset/simple_map_v0.json",
  route_path:
    "/Users/seongcheoljeong/Documents/Test/tests/fixtures/autonomy_e2e/p_sim_engine/highway_map_route_following_v0.json",
  autoware_pipeline_manifest_path:
    "/Users/seongcheoljeong/Documents/Test/artifacts/autoware_primary_semantic_embedded_probe_v1/autoware/autoware_pipeline_manifest.json",
  autoware_dataset_manifest_path:
    "/Users/seongcheoljeong/Documents/Test/artifacts/autoware_primary_semantic_embedded_probe_v1/autoware/autoware_dataset_manifest.json",
  autoware_topic_catalog_path:
    "/Users/seongcheoljeong/Documents/Test/artifacts/autoware_primary_semantic_embedded_probe_v1/autoware/autoware_topic_catalog.json",
  autoware_consumer_input_manifest_path:
    "/Users/seongcheoljeong/Documents/Test/artifacts/autoware_primary_semantic_embedded_probe_v1/autoware/autoware_consumer_input_manifest.json",
  out_root: "/Users/seongcheoljeong/Documents/Test/artifacts/control_plane/runs/closed_loop_demo",
  run_duration_sec: 45,
  heartbeat_timeout_sec: 60,
  poll_interval_sec: 2,
  startup_grace_sec: 5,
  record_video: true,
  record_rviz: false,
  record_rosbag: false,
  strict_capture: false,
  preflight_only: false,
  allow_non_linux_host: false,
  awsim_launch_command: "",
  autoware_launch_command: "",
  route_goal_command: "",
  localization_check_command: "",
  perception_check_command: "",
  planning_check_command: "",
  control_check_command: "",
  vehicle_motion_check_command: "",
  route_completion_check_command: "",
  video_capture_command: "",
  rviz_capture_command: "",
  rosbag_record_command: "",
};
