# Hybrid HELIOS Sensor Sim

This repository implements a hybrid integration strategy for [HELIOS](https://github.com/3dgeo-heidelberg/helios):

- Use HELIOS as the external geometry/raycast backend.
- Keep project-specific physics improvements (noise, distortion, post-processing) in local code.
- Orchestrate both through a single runtime path with fallback behavior.

## Why hybrid

- Avoid a hard fork of HELIOS while still using its core strengths.
- Preserve flexibility for domain-specific sensor behavior upgrades.
- Keep maintenance cost lower than full in-house reimplementation.

## Structure

- `src/hybrid_sensor_sim/backends/helios_adapter.py`: external HELIOS execution adapter.
- `src/hybrid_sensor_sim/backends/native_physics.py`: local physics enhancement layer.
- `src/hybrid_sensor_sim/scenarios/schema.py`: migrated `scenario_definition_v0` schema validation and actor normalization.
- `src/hybrid_sensor_sim/scenarios/object_sim.py`: deterministic 1D object-sim core with collision/minTTC/lane-risk outputs, optional ego vehicle-dynamics coupling, and canonical map/route lane-id consumption.
- `src/hybrid_sensor_sim/scenarios/log_scene.py`: migrated `log_scene_v0` validation.
- `src/hybrid_sensor_sim/scenarios/replay.py`: `log_scene_v0` to `scenario_definition_v0` conversion helpers with canonical-map route synthesis.
- `src/hybrid_sensor_sim/scenarios/variants.py`: migrated `logical_scenarios_v0` variant generation with deterministic `full` and `random` sampling.
- `src/hybrid_sensor_sim/scenarios/matrix_sweep.py`: migrated object-sim matrix sweep with traffic actor-pattern synthesis, per-case report aggregation, and map/route propagation.
- `src/hybrid_sensor_sim/maps/convert.py`: migrated `simple_map_v0 <-> canonical_lane_graph_v0` conversion helpers.
- `src/hybrid_sensor_sim/maps/validate.py`: canonical lane graph semantic validation and report generation.
- `src/hybrid_sensor_sim/maps/route.py`: canonical lane graph route computation for `hops` and `length` cost modes.
- `src/hybrid_sensor_sim/config.py`: typed Sensor Sim config translation layer for camera/lidar/radar/renderer blocks.
- `src/hybrid_sensor_sim/io/survey_mapping.py`: scenario JSON to HELIOS survey XML mapper.
- `src/hybrid_sensor_sim/renderers/playback_contract.py`: renderer playback contract builder for CARLA/AWSIM bridge.
- `src/hybrid_sensor_sim/orchestrator.py`: mode selection and pipeline chaining.
- `docs/hybrid_helios_plan.md`: functional roadmap and risk management.
- `docs/p_sim_engine_migration_audit.md`: audit of historical `P_Sim-Engine` work and concrete migration targets into this repository.
- `docs/autonomy_e2e_migration_master_plan.md`: selective migration scope and phased execution plan for `Autonomy-E2E` sources.
- `scripts/setup_helios.sh`: bootstrap helper for cloning/building HELIOS.
- `scripts/run_renderer_backend_smoke.py`: AWSIM/CARLA smoke launcher that forces direct backend execution plus output-contract inspection.
- `scripts/discover_renderer_backend_local_env.py`: discovers local HELIOS/AWSIM/CARLA runtime candidates and writes a reusable env file plus readiness summary.
- `scripts/acquire_renderer_backend_package.py`: resolves an official AWSIM/CARLA package URL from `renderer_backend_local_setup.json`, downloads it, and optionally stages it into a runnable backend directory.
- `scripts/stage_renderer_backend_package.py`: extracts packaged AWSIM/CARLA archives into `third_party/runtime_backends/<backend>` and writes a staging env file for smoke runs.
- `scripts/run_renderer_backend_workflow.py`: runs `discover/load setup -> optional acquire -> smoke` as one workflow and writes a single workflow summary.
- `scripts/run_renderer_backend_package_workflow_selftest.py`: synthesizes a packaged backend archive and exercises `acquire -> stage -> refresh discover -> smoke`.
- `scripts/run_vehicle_dynamics_trace.py`: runs a migrated vehicle dynamics trace using `vehicle_profile_v0` and `control_sequence_v0`.
- `scripts/run_object_sim.py`: runs migrated `scenario_definition_v0` object-sim and writes `summary.json`, `trace.csv`, and `lane_risk_summary.json`.
- `scripts/run_log_replay.py`: converts `log_scene_v0` into a generated scenario and runs object-sim on it.
- `scripts/run_log_scene_augment.py`: creates deterministic speed/gap variants from `log_scene_v0`.
- `scripts/run_scenario_variants.py`: expands `logical_scenarios_v0` inputs into concrete parameter combinations.
  - optional `variant_payload_template` lets the report carry rendered concrete payloads such as `log_scene_v0` variants
- `scripts/run_scenario_variant_runner.py`: executes rendered payloads from `scenario_variants_report_v0` and writes a single variant-run report.
  - supports `rendered_payload_kind=log_scene_v0` via replay and `rendered_payload_kind=scenario_definition_v0` via direct object-sim execution
  - report includes `successful_variant_rows` and `non_success_variant_rows` for quick triage
- `scripts/run_scenario_variant_workflow.py`: generates variants and immediately executes rendered payloads, writing a workflow report plus the underlying variant/run reports.
- `scripts/run_scenario_matrix_sweep.py`: runs object-sim over traffic/friction parameter grids and writes a sweep report.
- `scripts/run_sensor_rig_sweep.py`: evaluates rig candidates against current native preview and coverage outputs.
- `scripts/run_map_convert.py`: converts `simple_map_v0` and `canonical_lane_graph_v0`.
- `scripts/run_map_validate.py`: validates canonical lane graph semantics and writes a validation report.
- `scripts/run_map_route.py`: computes canonical lane routes with optional via-lane constraints.

## Quick start

```bash
PYTHONPATH=src python3 -m hybrid_sensor_sim.cli --config configs/hybrid_sensor_sim.example.json
```

Run tests:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q
```

## Autonomy-E2E migration quick start

Vehicle dynamics trace:

```bash
python3 scripts/run_vehicle_dynamics_trace.py \
  --vehicle-profile tests/fixtures/autonomy_e2e/p_sim_engine/vehicle_profile_v0.json \
  --control-sequence tests/fixtures/autonomy_e2e/p_sim_engine/control_sequence_v0.json \
  --out artifacts/vehicle_dynamics_trace_v0.json
```

Deterministic object sim:

```bash
python3 scripts/run_object_sim.py \
  --scenario tests/fixtures/autonomy_e2e/p_sim_engine/highway_safe_following_v0.json \
  --run-id RUN_SAFE_001 \
  --seed 42 \
  --out artifacts/object_sim_runs
```

Object sim with opt-in ego vehicle dynamics:

```bash
python3 scripts/run_object_sim.py \
  --scenario tests/fixtures/autonomy_e2e/p_sim_engine/highway_safe_following_vehicle_dynamics_v0.json \
  --run-id RUN_SAFE_DYN_001 \
  --seed 42 \
  --out artifacts/object_sim_dynamics_runs
```

Object sim with canonical map route consumption:

```bash
python3 scripts/run_object_sim.py \
  --scenario tests/fixtures/autonomy_e2e/p_sim_engine/highway_map_route_following_v0.json \
  --run-id RUN_MAP_ROUTE_001 \
  --seed 42 \
  --out artifacts/object_sim_map_route_runs
```

Log replay and augmentation:

```bash
python3 scripts/run_log_replay.py \
  --log-scene tests/fixtures/autonomy_e2e/p_sim_engine/log_scene_v0.json \
  --run-id LOG_REPLAY_001 \
  --out artifacts/log_replay_runs

python3 scripts/run_log_replay.py \
  --log-scene tests/fixtures/autonomy_e2e/p_sim_engine/log_scene_map_route_v0.json \
  --run-id LOG_REPLAY_MAP_001 \
  --out artifacts/log_replay_map_runs

python3 scripts/run_log_scene_augment.py \
  --input tests/fixtures/autonomy_e2e/p_sim_engine/log_scene_v0.json \
  --out artifacts/log_scene_aug_v0.json \
  --ego-speed-scale 1.1 \
  --lead-gap-offset-m -5 \
  --lead-speed-offset-mps 2.0 \
  --suffix aug
```

Scenario variants:

```bash
python3 scripts/run_scenario_variants.py \
  --logical-scenarios tests/fixtures/autonomy_e2e/p_validation/highway_cut_in_v0.json \
  --out artifacts/scenario_variants_highway_cut_in_v0.json \
  --sampling full

python3 scripts/run_scenario_variants.py \
  --logical-scenarios tests/fixtures/autonomy_e2e/p_validation/highway_map_route_relations_v0.json \
  --out artifacts/scenario_variants_highway_map_route_relations_v0.json \
  --sampling full

python3 scripts/run_scenario_variants.py \
  --logical-scenarios tests/fixtures/autonomy_e2e/p_validation/highway_mixed_payloads_v0.json \
  --out artifacts/scenario_variants_highway_mixed_payloads_v0.json \
  --sampling full

python3 scripts/run_scenario_variant_workflow.py \
  --scenario-language-profile highway_mixed_payloads_random_v0 \
  --out-root artifacts/scenario_variant_workflow_random_runs \
  --sampling random \
  --sample-size 1 \
  --execution-max-variants 0

python3 scripts/run_scenario_variant_runner.py \
  --variants-report artifacts/scenario_variants_highway_map_route_relations_v0.json \
  --out artifacts/scenario_variant_runs

python3 scripts/run_scenario_variant_workflow.py \
  --scenario-language-profile highway_mixed_payloads_v0 \
  --out-root artifacts/scenario_variant_workflow_runs \
  --execution-max-variants 2
```

`scenario_variant_workflow_report_v0.json` includes:

- `by_payload_kind`: grouped execution summary for each rendered payload kind
- `by_logical_scenario_id`: grouped execution summary for each logical scenario
- `successful_variant_rows`: compact successful variant table with execution path and summary artifact
- `non_success_variant_rows`: compact failed/skipped variant table for quick triage

Both `run_scenario_variants.py` and `run_scenario_variant_workflow.py` resolve default scenario-language profiles from:

- `tests/fixtures/autonomy_e2e/p_validation`

Object-sim matrix sweep:

```bash
python3 scripts/run_scenario_matrix_sweep.py \
  --scenario tests/fixtures/autonomy_e2e/p_sim_engine/highway_safe_following_v0.json \
  --out-root artifacts/scenario_matrix_runs \
  --report-out artifacts/scenario_matrix_report.json \
  --traffic-profile-ids sumo_highway_balanced_v0 \
  --traffic-actor-pattern-ids sumo_platoon_sparse_v0,sumo_platoon_balanced_v0 \
  --traffic-npc-speed-scale-values 0.9,1.0 \
  --tire-friction-coeff-values 0.7,1.0 \
  --surface-friction-scale-values 0.8,1.0
```

Sensor rig sweep:

```bash
python3 scripts/run_sensor_rig_sweep.py \
  --base-config tests/fixtures/autonomy_e2e/p_sim_engine/rig_sweep_base_config.json \
  --rig-candidates tests/fixtures/autonomy_e2e/p_sim_engine/rig_sweep_candidates_v1.json \
  --out artifacts/sensor_rig_sweep
```

Map convert / validate / route:

```bash
python3 scripts/run_map_convert.py \
  --input tests/fixtures/autonomy_e2e/p_map_toolset/simple_map_v0.json \
  --out artifacts/canonical_lane_graph_v0.json \
  --to-format canonical

python3 scripts/run_map_validate.py \
  --map tests/fixtures/autonomy_e2e/p_map_toolset/canonical_lane_graph_v0.json \
  --report-out artifacts/canonical_map_validation_report_v0.json

python3 scripts/run_map_route.py \
  --map tests/fixtures/autonomy_e2e/p_map_toolset/canonical_lane_graph_v0.json \
  --entry-lane-id lane_a \
  --exit-lane-id lane_c \
  --via-lane-id lane_b \
  --report-out artifacts/canonical_map_route_report_v0.json
```

Autonomy-E2E fixtures currently mirrored into this repo:

- `tests/fixtures/autonomy_e2e/p_sim_engine/vehicle_profile_v0.json`
- `tests/fixtures/autonomy_e2e/p_sim_engine/control_sequence_v0.json`
- `tests/fixtures/autonomy_e2e/p_sim_engine/highway_following_v0.json`
- `tests/fixtures/autonomy_e2e/p_sim_engine/highway_safe_following_v0.json`
- `tests/fixtures/autonomy_e2e/p_sim_engine/highway_safe_following_vehicle_dynamics_v0.json`
- `tests/fixtures/autonomy_e2e/p_sim_engine/highway_map_route_following_v0.json`
- `tests/fixtures/autonomy_e2e/p_sim_engine/log_scene_v0.json`
- `tests/fixtures/autonomy_e2e/p_sim_engine/log_scene_map_route_v0.json`
- `tests/fixtures/autonomy_e2e/p_sim_engine/log_scene_map_route_relations_v0.json`
- `tests/fixtures/autonomy_e2e/p_sim_engine/rig_sweep_base_config.json`
- `tests/fixtures/autonomy_e2e/p_sim_engine/rig_sweep_candidates_v1.json`
- `tests/fixtures/autonomy_e2e/p_validation/highway_cut_in_v0.json`
- `tests/fixtures/autonomy_e2e/p_validation/highway_map_route_relations_v0.json`
- `tests/fixtures/autonomy_e2e/p_validation/highway_mixed_payloads_v0.json`
- `tests/fixtures/autonomy_e2e/p_validation/highway_mixed_payloads_random_v0.json`
- `tests/fixtures/autonomy_e2e/p_map_toolset/simple_map_v0.json`
- `tests/fixtures/autonomy_e2e/p_map_toolset/canonical_lane_graph_v0.json`

Survey mapping dry-run demo (no HELIOS execution, plan+mapping artifacts only):

```bash
PYTHONPATH=src python3 -m hybrid_sensor_sim.cli --config configs/hybrid_sensor_sim.survey_mapping_demo.json
```

Expected artifacts under `artifacts/survey_mapping_demo/helios_raw`:

- `helios_execution_plan.json`
- `survey_mapping_metadata.json`
- generated survey XML under `generated_surveys/`

## HELIOS execution modes

- Runtime selection (`options.helios_runtime`):
  - `binary`: use local `HELIOS_BIN` / built binary.
  - `docker`: run HELIOS inside container (`docker run`).
  - `auto`: try binary first, then docker fallback.
- Execution control:
  - `execute_helios=false`: creates execution plan only (safe dry run).
  - `execute_helios=true`: executes HELIOS and records `stdout/stderr`.
- Scenario mapping:
  - set `survey_generate_from_scenario=true` to generate survey XML from scenario JSON.
  - generated survey path is recorded in `helios_execution_plan.json` (`generated_survey_path`).
  - mapping summary is embedded in `helios_execution_plan.json` (`survey_mapping_metadata`) and emitted as `survey_mapping_metadata.json`.
  - trajectory source priority:
    - `ego_trajectory` (if present),
    - else `objects[].pose/waypoints` + `waypoints`.
  - explicit legs are supported via `helios.legs` or `helios_legs`.
  - `sensors.lidar` can provide defaults (`pulse_freq_hz`, `scan_freq_hz`, head rotate fields).
  - custom scanner attributes can be passed through:
    - global scanner settings: `sensors.lidar.scanner_settings`, `helios.scanner_settings`,
    - explicit override via options: `survey_scanner_settings_extra_attrs`,
    - per-leg scanner settings: `helios.legs[].scanner` (scalar fields are forwarded).
    - canonical key normalization is applied (examples): `num_rays -> numRays`, `max_range_m -> maxRange_m`, `horizontal_fov_deg -> horizontalFov_deg`.
  - options override scenario defaults:
    - refs: `survey_scene_ref`, `survey_platform_ref`, `survey_scanner_ref`
    - scanner setting template id: `survey_scanner_settings_id`
    - force global leg scanner attributes: `survey_force_global_leg_scanner=true`
- Post-processing:
  - detects generated output directory and primary files (`.xyz/.las/.laz`, trajectory, pulse, fullwave),
  - writes output manifest for downstream physics chain,
  - projects `.xyz` point cloud into camera image plane using intrinsics + Brown-Conrady distortion coefficients.

## Docker notes

- Docker daemon must be running (`docker info` must succeed).
- Use [configs/hybrid_sensor_sim.helios_docker.json](/Users/seongcheoljeong/Documents/Test/configs/hybrid_sensor_sim.helios_docker.json) and set a valid `helios_docker_image`.
- Current docker mode assumes survey/assets/output paths are under this workspace root.
- Build helper: `bash scripts/docker_build_helios.sh [image_tag]`
- Low-memory CLI-only build helper: `bash scripts/docker_build_helios_cli.sh [image_tag]`
- Run helper: `bash scripts/run_hybrid_docker_demo.sh [config_path]`
- Docker demo config uses:
  - `helios_docker_image=heliosplusplus:cli`
  - `helios_docker_binary=/home/jovyan/helios/build/helios++`
  - `assets_paths=["python/pyhelios", "."]`

## Camera projection notes

## Object-sim notes

- `scenario_definition_v0` keeps the original canonical schema and now supports optional:
  - `ego_dynamics_mode=vehicle_dynamics`
  - `ego_vehicle_profile`
  - `ego_target_speed_mps`
  - `ego_road_grade_percent`
- `scenario_definition_v0` also supports optional canonical-map inputs:
  - `canonical_map` or `canonical_map_path`
  - `route_definition`
  - actor-level `lane_id`
- When a route is provided, actor `lane_id` values are normalized into the existing `lane_index` surface using the route lane order.
- When a route is provided and an actor only carries `lane_index`, object-sim now infers `lane_id` when that index maps onto the route lane order and exposes the result as `*_lane_binding_mode`.
- `log_scene_v0` can now optionally carry the same canonical-map inputs:
  - `canonical_map` or `canonical_map_path`
  - `route_definition`
  - `ego_lane_id`
  - `lead_vehicle_lane_id`
- `log_scene_v0` can also drive lane synthesis from route semantics:
  - `ego_route_relation`
  - `lead_vehicle_route_relation`
- When `log_scene_v0` provides a canonical map but no `route_definition`, replay synthesizes a default route and propagates it into the generated scenario.
- When explicit lane IDs are omitted, replay can now resolve actor lane IDs from those route relations.
- `scenario_matrix_sweep` preserves canonical map, route definition, and actor `lane_id` values in each generated `matrix_scenario.json`.
- `scenario_matrix_sweep` traffic actor patterns can also carry route-relation profiles, so map-aware sweeps can synthesize progression along the route instead of only reusing raw lane slots.
- `lane_risk_summary.json` now exposes route-aware counters in addition to legacy `same_lane` and `adjacent_lane` counters:
  - `route_relation_counts`
  - `route_same_lane_rows`
  - `route_downstream_rows`
  - `route_upstream_rows`
  - `min_ttc_route_downstream_sec`
  - `ttc_under_3s_route_downstream_count`
- Object-sim runtime now also uses route semantics directly for path-conflict TTC and ego avoidance:
  - `summary.json`: `route_aware_runtime_enabled`, `min_ttc_path_conflict_sec`
  - `trace.csv`: `path_conflict`, `path_conflict_source`, `path_ttc_sec`
  - `lane_risk_summary.json`: `path_conflict_rows`, `min_ttc_path_conflict_sec`
- Default behavior remains the historical fixed-speed kinematic core.
- Vehicle-dynamics coupling is currently longitudinal only; lane handling remains `1D + lane_index`.

- A typed sensor config manifest is emitted as `sensor_sim_config.json` in both native-only and hybrid-enhanced outputs.
- Supported camera geometry models in the local physics path:
  - `pinhole`
  - `rectilinear`
  - `equidistant`
  - `orthographic`
- Supported camera output modes in the local physics preview path:
  - `camera_sensor_type=VISIBLE`
  - `camera_sensor_type=DEPTH`
  - `camera_sensor_type=SEMANTIC_SEGMENTATION`
- Depth output controls:
  - `camera_depth_params.min`
  - `camera_depth_params.max`
  - `camera_depth_params.type=LINEAR|LOG|RAW`
  - `camera_depth_params.log_base`
  - `camera_depth_params.bit_depth`
- Semantic output controls:
  - `camera_semantic_params.class_version=LEGACY|GRANULAR_SEGMENTATION`
  - `camera_semantic_params.palette`
  - `camera_semantic_params.label_source`
  - `camera_semantic_params.include_actor_id`
  - `camera_semantic_params.include_component_id`
  - `camera_semantic_params.include_material_class`
  - `camera_semantic_params.include_lane_marking_id`
  - optional explicit per-point overrides via `camera_semantic_point_labels`
- Image chain controls:
  - `camera_image_params.shutter_speed` or `camera_image_params.shutter_speed_us`
  - `camera_image_params.iso`
  - `camera_image_params.analog_gain`
  - `camera_image_params.digital_gain`
  - `camera_image_params.readout_noise`
  - `camera_image_params.white_balance` or `camera_image_params.white_balance_kelvin`
  - `camera_image_params.gamma`
  - `camera_image_params.bloom`
  - `camera_image_params.fixed_pattern_noise.dsnu`
  - `camera_image_params.fixed_pattern_noise.prnu`
  - `camera_image_params.seed`
- Lens controls:
  - `camera_lens_params.lens_flare`
  - `camera_lens_params.spot_size`
  - `camera_lens_params.vignetting.intensity`
  - `camera_lens_params.vignetting.alpha`
  - `camera_lens_params.vignetting.radius`
- Coverage controls:
  - `coverage_metrics.enabled`
  - `coverage_metrics.combine_sensors`
  - `coverage_metrics.thresholds.camera.min_pixels_on_target`
  - `coverage_metrics.thresholds.lidar.min_points_on_target`
  - `coverage_metrics.thresholds.radar.min_detections_on_target`
- Rolling shutter timing controls:
  - `camera_rolling_shutter.enabled`
  - `camera_rolling_shutter.row_delay_ns`
  - `camera_rolling_shutter.col_delay_ns`
  - `camera_rolling_shutter.row_readout_direction`
  - `camera_rolling_shutter.col_readout_direction`
  - `camera_rolling_shutter.num_time_steps`
  - `camera_rolling_shutter.num_exposure_samples_per_pixel`
- For large world coordinates, use `camera_reference_mode`:
  - `none` (default): raw coordinates.
  - `first_point` / `mean_point`: xyz recenter.
  - `first_point_xy` / `mean_point_xy`: xy recenter while keeping original z-depth.
- `camera_extrinsics` is applied as `p_cam = R(roll,pitch,yaw) * (p_world - t)`:
  - `tx,ty,tz`: camera translation.
  - `roll_deg,pitch_deg,yaw_deg`: ZYX Euler rotation in degrees.
- Sensor behavior controls:
  - only the first behavior is applied, matching the current Applied beta behavior contract
  - `camera_behaviors`, `lidar_behaviors`, `radar_behaviors`, or nested `sensor_behaviors.{camera,lidar,radar}`
  - `point_at.id`
  - `point_at.target_center_offset.{x,y,z}`
  - `continuous_motion.{tx,ty,tz,rx,ry,rz}`
  - preview-time evaluation controls:
    - `camera_behavior_time_s`
    - `lidar_behavior_time_s`
    - `radar_behavior_time_s`
    - fallback `sensor_behavior_time_s`
  - explicit target positions can be supplied with `sensor_behavior_actor_positions` / `actor_positions`
- Optional auto extrinsics from HELIOS trajectory:
  - enable `camera_extrinsics_auto_from_trajectory=true`
  - choose pose with `camera_extrinsics_auto_pose=first|middle|last`
  - choose merge policy:
    - `camera_extrinsics_auto_use_position=xy|xyz`
    - `camera_extrinsics_auto_use_orientation=true|false`
  - optional offsets via `camera_extrinsics_auto_offsets`.
- Optional trajectory sweep projection preview:
  - enable `camera_projection_trajectory_sweep_enabled=true`
  - set `camera_projection_trajectory_sweep_frames` (default `3`)
  - emits `camera_projection_trajectory_sweep.json` with multi-pose frame previews.
  - preview artifacts record `geometry_model` per preview/frame.
  - all camera preview modes now emit `preview_ground_truth_samples` plus `ground_truth_fields` and aggregated `coverage_targets`.
  - applied behavior runtime is emitted as `camera_behavior`.
  - depth mode emits `preview_depth_samples`.
  - semantic mode emits `preview_semantic_samples` and `preview_semantic_legend`.
  - visible mode emits `preview_image_signal_samples` with exposure, white-balance, vignetting, flare, spot blur radius, photon, and digital RGB preview values.
  - rolling shutter mode emits `preview_readout_samples` and timing metadata.
  - when HELIOS trajectory poses are available, rolling shutter preview/sweep applies per-sample pose distortion and records whether the distortion path was actually applied.

## LiDAR/Radar post-physics notes

- LiDAR noisy preview:
  - enable `lidar_postprocess_enabled=true`
  - noise/dropout controls: `lidar_noise`, `lidar_noise_stddev_m`, `lidar_dropout_probability`
  - scan engine controls:
    - `lidar_scan_type=SPIN|FLASH|CUSTOM`
    - `lidar_scan_frequency_hz`
    - `lidar_spin_direction=CCW|CW`
    - `lidar_source_angles`
    - `lidar_source_angle_tolerance_deg`
    - `lidar_scan_field.{azimuth_min_deg,azimuth_max_deg,elevation_min_deg,elevation_max_deg}`
    - `lidar_scan_field_offset.{azimuth_deg,elevation_deg}`
    - `lidar_scan_path`
    - `lidar_multi_scan_path`
  - signal/intensity controls:
    - `lidar_intensity.units=REFLECTIVITY|REFLECTIVITY_SCALED|SNR|SNR_SCALED|POWER|LASER_CROSS_SECTION|GROUND_TRUTH_REFLECTIVITY`
    - `lidar_intensity.range.{min,max}`
    - `lidar_intensity.scale.{min,max}`
    - `lidar_intensity.range_scale_map`
    - `lidar_physics_model.reflectivity_coefficient`
    - `lidar_physics_model.atmospheric_attenuation_rate`
    - `lidar_physics_model.ambient_power_dbw`
    - `lidar_physics_model.signal_photon_scale`
    - `lidar_physics_model.ambient_photon_scale`
    - `lidar_physics_model.minimum_detection_snr_db`
    - `lidar_physics_model.return_all_hits`
  - multi-return controls:
    - `lidar_return_model.mode=SINGLE|DUAL|MULTI`
    - `lidar_return_model.max_returns`
    - `lidar_return_model.selection_mode=FIRST|LAST|STRONGEST`
    - `lidar_return_model.range_discrimination`
    - `lidar_return_model.range_separation_m`
    - `lidar_return_model.signal_decay`
    - `lidar_return_model.minimum_secondary_snr_db`
    - Applied-style alias: `return_count` can be used instead of `max_returns`
  - geometry-aware multipath controls:
    - `lidar_multipath_model.enabled`
    - `lidar_multipath_model.mode=GROUND_PLANE|VERTICAL_PLANE|HYBRID`
    - `lidar_multipath_model.max_paths`
    - `lidar_multipath_model.path_signal_decay`
    - `lidar_multipath_model.minimum_path_snr_db`
    - `lidar_multipath_model.max_extra_path_length_m`
    - `lidar_multipath_model.ground_plane_height_m`
    - `lidar_multipath_model.ground_reflectivity`
    - `lidar_multipath_model.wall_plane_x_m`
    - `lidar_multipath_model.wall_reflectivity`
  - environment/noise controls:
    - `lidar_environment_model.enable_ambient`
    - `lidar_environment_model.fog_density`
    - `lidar_environment_model.extinction_coefficient_scale`
    - `lidar_environment_model.backscatter_scale`
    - `lidar_environment_model.disable_backscatter`
    - `lidar_environment_model.precipitation_rate`
    - `lidar_environment_model.precipitation_type=RAIN|SNOW|HAIL`
    - `lidar_environment_model.particle_density_scale`
    - `lidar_environment_model.particle_diameter_mm`
    - `lidar_environment_model.terminal_velocity_mps`
    - `lidar_environment_model.particle_reflectivity`
    - `lidar_environment_model.backscatter_jitter`
    - `lidar_environment_model.field_seed`
    - `lidar_noise_performance.probability_false_alarm`
    - `lidar_noise_performance.target_detectability.probability_detection`
    - `lidar_noise_performance.target_detectability.target.{range,reflectivity}`
  - emitter/channel controls:
    - `lidar_emitter_params.source_losses`
    - `lidar_emitter_params.global_source_loss`
    - `lidar_emitter_params.source_divergence.{az,el}`
    - `lidar_emitter_params.source_variance.{az,el}`
    - `lidar_emitter_params.peak_power`
    - `lidar_emitter_params.optical_loss`
  - channel profile / sidelobe controls:
    - `lidar_shared_channel_profile.profile_data.file_uri`
    - `lidar_shared_channel_profile.profile_data.half_angle`
    - `lidar_shared_channel_profile.profile_data.scale`
    - file-backed ingestion:
      - direct `json/csv/txt/npy`
      - `.exr` URI with `.json/.csv/.txt/.npy` sidecar fallback in environments without EXR decoder
    - synthetic helper fields:
      - `pattern=CROSS|GRID|RING`
      - `sample_count`
      - `sidelobe_gain`
  - emits `lidar_noisy_preview.xyz` and `lidar_noisy_preview.json`.
  - `lidar_noisy_preview.json` preview points include:
    - geometry metadata: `range_m`, `azimuth_deg`, `elevation_deg`, `channel_id`, `scan_path_index`
    - signal metadata: `intensity`, `intensity_units`, `reflectivity`, `ground_truth_reflectivity`, `laser_cross_section`, `signal_power_dbw`, `ambient_power_dbw`, `signal_photons`, `ambient_photons`, `snr`, `snr_db`, `return_id`
    - ground-truth metadata: `ground_truth_semantic_class`, `ground_truth_semantic_class_name`, `ground_truth_actor_id`, `ground_truth_component_id`, `ground_truth_material_class`, `ground_truth_material_uuid`, `ground_truth_base_map_element`, `ground_truth_procedural_map_element`, `ground_truth_lane_marking_id`
    - multi-return/weather metadata: `path_length_offset_m`, `ground_truth_hit_index`, `ground_truth_last_bounce_index`, `weather_extinction_factor`, `ground_truth_detection_type`
    - precipitation particle metadata: `precipitation_type`, `particle_field_density`, `particle_diameter_mm`, `particle_terminal_velocity_mps`, `particle_reflectivity`, `particle_backscatter_strength`, `precipitation_extinction_alpha`
    - multipath metadata: `multipath_surface`, `multipath_path_length_m`, `multipath_base_range_m`, `multipath_surface_reflectivity`, `multipath_model_mode`, `multipath_reflection_point`
    - channel profile metadata: `channel_profile_pattern`, `channel_profile_file_uri`, `channel_profile_weight`, `channel_profile_scale`, `channel_profile_offset_az_deg`, `channel_profile_offset_el_deg`, `channel_profile_half_angle_deg`
    - channel profile loading metadata: `channel_profile_source`, `channel_profile_resolved_path`
    - selection metadata: `merged_return_count`, `range_discrimination_m`
    - emitter metadata: `channel_loss_db`, `optical_loss_db`, `peak_power_w`, `beam_divergence_az_rad`, `beam_divergence_el_rad`, `beam_footprint_area_m2`, `beam_azimuth_offset_deg`, `beam_elevation_offset_deg`
  - LiDAR preview and sweep payloads also expose `ground_truth_fields`, `coverage_metric_name`, and aggregated `coverage_targets`.
  - LiDAR preview/sweep now emit resolved `lidar_extrinsics` and `lidar_behavior`.
- LiDAR trajectory sweep preview:
  - enable `lidar_trajectory_sweep_enabled=true`
  - set `lidar_trajectory_sweep_frames` and `lidar_preview_points_per_frame`
  - motion compensation controls:
    - `lidar_motion_compensation_enabled`
    - `lidar_motion_compensation_mode` (`linear`)
    - `lidar_scan_duration_s`
  - optional auto-extrinsics from trajectory:
    - `lidar_extrinsics_auto_use_position=none|xy|xyz`
    - `lidar_extrinsics_auto_use_orientation=true|false`
    - `lidar_extrinsics_auto_offsets`
  - emits `lidar_trajectory_sweep.json` with `preview_points_xyz` and structured `preview_points` metadata (`range_m`, `azimuth_deg`, `elevation_deg`, `channel_id`, `scan_path_index`, `intensity`, `snr_db`, `return_id`, `ground_truth_hit_index`, `weather_extinction_factor`, `precipitation_type`, `particle_field_density`, `channel_loss_db`).
- Radar target preview:
  - enable `radar_postprocess_enabled=true`
  - core controls: `radar_max_targets`, `radar_range_min_m`, `radar_range_max_m`
  - clutter/false alarms: `radar_clutter`, `radar_false_target_count`
  - system params:
    - `radar_system_params.frame_rate`
    - `radar_system_params.transmit_power`
    - `radar_system_params.radiometric_calibration_factor`
    - `radar_system_params.center_frequency`
    - `radar_system_params.range_resolution`
    - `radar_system_params.range_quantization`
    - `radar_system_params.velocity.{min,max}`
    - `radar_system_params.velocity_resolution`
    - `radar_system_params.velocity_quantization`
    - `radar_antenna_params.beam_params.{hpbw_az,hpbw_el}`
    - `radar_antenna_params.antenna_definitions[].type`
    - `radar_antenna_params.antenna_definitions[].directivity_az_el_cuts.{az,el}.directivity_table_cut.{angles,amplitudes,do_not_normalize}`
  - detector params:
    - `radar_detector_params.noise_variance_dbw`
    - `radar_detector_params.minimum_snr_db`
    - `radar_detector_params.no_additive_noise`
    - `radar_detector_params.max_detections`
    - `radar_detector_params.noise_performance.probability_false_alarm`
    - `radar_detector_params.noise_performance.target_detectability.target.{range,radar_cross_section}`
    - `radar_detector_params.noise_performance.target_detectability.probability_detection`
  - estimator params:
    - `radar_estimator_params.{range,velocity,azimuth,elevation}_accuracy.max_deviation`
    - `radar_estimator_params.*_accuracy_regions`
  - tracking params:
    - `radar_tracking_params.tracks`
    - `radar_tracking_params.max_tracks`
    - `radar_tracking_params.max_coast_frames`
    - `radar_tracking_params.emit_coasted_tracks`
    - `radar_tracking_params.coast_confidence_decay`
  - track mode combines detections with the same `ground_truth_actor_id`:
    - `rcs_dbsm` is an incoherent sum across grouped detections
    - `range_m`, `azimuth_deg`, `elevation_deg` are the geometric center of grouped detections
    - tracks expose:
      - `source_target_ids`
      - `source_target_count`
      - `source_measurement_source_counts`
      - `source_multipath_target_count`
      - `source_multipath_path_type_counts`
  - fidelity params:
    - `radar_fidelity.level`
    - `radar_fidelity.multipath`
    - `radar_fidelity.multipath_bounces`
    - `radar_fidelity.coherence_factor`
    - `radar_fidelity.enable_micro_doppler`
    - `radar_fidelity.near_clipping_distance`
    - `radar_fidelity.sub_ray_angular_resolution`
    - `radar_fidelity.raytracing.mode`
    - `radar_fidelity.raytracing.enable_cavity_model`
    - `radar_fidelity.raytracing.adaptive_sampling_params.default_min_rays_per_wavelength`
    - `radar_fidelity.raytracing.adaptive_sampling_params.max_subdivision_level`
    - `radar_fidelity.raytracing.adaptive_sampling_params.targets[].{actor_id,min_rays_per_wavelength}`
  - ego-motion velocity source: `radar_use_ego_velocity_from_trajectory`
  - emits `radar_targets_preview.json`.
  - preview includes `snr_db`, `detection_probability`, `antenna_gain_db`, accuracy-region indices, optional `tracks`, and path-type-aware radar multipath metadata:
    - `measurement_source=DETECTION|MULTIPATH|FALSE_ALARM`
    - `ground_truth_semantic_class`, `ground_truth_semantic_class_name`, `ground_truth_actor_id`
    - `ground_truth_detection_type`
    - `ground_truth_hit_index`
    - `ground_truth_last_bounce_index`
    - `path_length_offset_m`
    - `multipath_path_type=FORWARD|REVERSE|RETROREFLECTION|CAVITY_RETROREFLECTION`
    - `multipath_path_length_m`
    - `multipath_base_range_m`
    - `multipath_surface`
    - `multipath_bounce_count`
    - `multipath_reflection_point`
    - `multipath_target_scatter_point`
    - `multipath_last_bounce_point`
    - `multipath_return_direction`
    - `multipath_cavity_internal_bounce_count`
    - `coherence_factor`
    - `micro_doppler_velocity_offset_mps`
    - aggregated counts:
      - `multipath_path_type_counts`
    - directivity/adaptive sampling metadata:
      - `sampling_gain_db`
      - `adaptive_sampling_density`
      - `adaptive_sampling_actor_id`
      - `adaptive_sampling_target_override`
      - `raytracing_subdivision_level`
      - `raytracing_mode`
  - Radar preview and sweep payloads also expose `ground_truth_fields`, `coverage_metric_name`, and aggregated `coverage_targets`.
  - Radar preview/sweep now emit resolved `radar_behavior`.
- Radar trajectory sweep preview:
  - enable `radar_trajectory_sweep_enabled=true`
  - set `radar_trajectory_sweep_frames` and `radar_preview_targets_per_frame`
  - optional auto-extrinsics from trajectory:
    - `radar_extrinsics_auto_use_position=none|xy|xyz`
    - `radar_extrinsics_auto_use_orientation=true|false`
    - `radar_extrinsics_auto_offsets`
  - emits `radar_targets_trajectory_sweep.json` with per-frame `targets_preview`, optional `tracks_preview`, and `multipath_target_count`.
  - trajectory tracks now expose continuity metadata:
    - `persistent_track_id`
    - `track_history_length`
    - `track_first_seen_time_s`
    - `track_last_seen_time_s`
    - `track_age_s`
    - `track_status=NEW|CONTINUING|COASTING`
    - `track_reassociated`
    - `track_coast_frame_count`
  - top-level sweep summary also exposes:
    - `persistent_track_count`
    - `track_reassociation_count`
    - `coasted_track_count`
    - `terminated_track_count`
    - `terminated_tracks`
    - `max_track_history_length`
    - `max_track_age_s`
  - hybrid outputs now also emit `sensor_coverage_summary.json` with per-sensor target counts plus combined overlap/blindspot coverage summary.

## Renderer bridge notes

- Enable contract output with `renderer_bridge_enabled=true`.
- Choose renderer with `renderer_backend` (`awsim`/`carla`/`none`).
- Scene controls:
  - `renderer_map`, `renderer_weather`, `renderer_scene_seed`, `renderer_ego_actor_id`
- Playback timeline controls:
  - `renderer_time_step_s`, `renderer_start_time_s`, `renderer_frame_offset`
- Output:
  - emits `renderer_playback_contract.json`
  - references available sensor artifacts (`camera/lidar/radar` preview or sweep) per frame.
  - when survey mapping is enabled, contract also carries `survey_mapping` metadata and related artifact paths.
  - includes `sensor_setup` block with camera/lidar/radar calibration context (`intrinsics`, `distortion`, `extrinsics`, and source).
  - camera setup now also carries `sensor_type`, `depth_params`, `semantic_params`, `image_chain`, `lens_params`, and `rolling_shutter`.
  - contract now also carries the typed `coverage_metrics` block.
  - includes `renderer_sensor_mounts` block for renderer-side sensor attach specs (`sensor_id`, `sensor_type`, `attach_to_actor_id`, `extrinsics`).
- Runtime executor:
  - set `renderer_execute=true` to run renderer command.
  - choose command source:
    - `renderer_command` (explicit command list; supports `{contract}` token)
    - or `renderer_bin` + `renderer_extra_args`
    - or backend defaults (`awsim_bin` / `carla_bin` + `awsim_extra_args` / `carla_extra_args`) when `renderer_bin` is empty
    - if backend bin is empty and wrapper is enabled, use local wrappers: `scripts/renderer_launch_awsim.sh`, `scripts/renderer_launch_carla.sh`
  - optional contract-driven argument injection:
    - scene args: `renderer_inject_scene_args` (`renderer_scene_*_flag` for map/weather/seed/ego)
    - sensor mount args: `renderer_inject_sensor_mount_args`, `renderer_sensor_mount_flag`, `renderer_sensor_mounts_only_enabled`, `renderer_sensor_mount_format=json|compact`
  - wrapper notes:
    - wrapper mode is controlled by `renderer_backend_wrapper_enabled` and optional path overrides (`renderer_backend_wrapper`, `awsim_wrapper`, `carla_wrapper`).
    - wrappers expect `AWSIM_BIN` / `CARLA_BIN` env vars when real execution is enabled.
    - wrappers translate `--sensor-mount` payloads to backend attach args:
      - AWSIM: `--mount-sensor <sensor_id:sensor_type:actor>` and (when extrinsics exist) `--mount-pose <sensor_id:tx:ty:tz:roll:pitch:yaw>`
      - CARLA: `--attach-sensor <sensor_type:sensor_id:actor>` and (when extrinsics exist) `--sensor-pose <sensor_id:tx:ty:tz:roll:pitch:yaw>`
    - wrappers translate `--frame-manifest` to backend ingestion args:
      - AWSIM: repeated `--ingest-sensor-frame <sensor:renderer_frame_id:payload_path>`
        plus `--ingest-sensor-meta <sensor:sensor_id:data_format:attach_actor>`
      - CARLA: repeated `--ingest-frame <renderer_frame_id:sensor:payload_path>`
        plus `--ingest-meta <sensor:sensor_id:data_format:attach_actor>`
    - wrappers can consume `--ingestion-profile <backend_ingestion_profile.json>` directly (takes precedence over frame-manifest parsing when both are present).
    - wrappers also consume `--sensor-bundle-summary <backend_sensor_bundle_summary.json>` for debug/runtime inspection and do not forward that flag to the backend binary.
  - execution plan includes `backend_args_preview` for normalized scene/sensor-mount argument inspection.
  - runtime artifacts:
    - `backend_invocation.json`: normalized backend command + preview snapshot.
    - `backend_run_manifest.json`: execution-status manifest with `PLANNED_ONLY|PLAN_ERROR|EXECUTION_SUCCEEDED|EXECUTION_FAILED|PROCESS_ERROR|SKIPPED`, failure reason, return code, and artifact pointers.
    - `renderer_pipeline_summary.json`: single-file runtime summary combining plan/run status, ingestion coverage, and expected-output inspection.
    - `backend_frame_inputs_manifest.json`: contract frame sources resolved into backend-consumable payload pointers, enriched with `sensor_id` / `data_format` / `attach_to_actor_id`.
      - depth cameras are tagged as `camera_depth_json`.
      - semantic cameras are tagged as `camera_semantic_json`.
    - `backend_ingestion_profile.json`: backend-specific ingest flag/value expansion generated from frame manifest.
    - `backend_sensor_bundle_summary.json`: per-frame sensor availability/completeness summary with backend ingestion bindings and payload pointers.
    - `backend_launcher_template.json`: deduplicated backend launch args (`meta_args` + `frame_args`) for direct runner integration.
    - `backend_ingestion_args.sh`: shell-ready `BACKEND_INGEST_ARGS` array generated from launcher template.
    - `backend_runner_request.json`: wrapper-free direct backend launch request assembled from scene args, mount args, and launcher template args.
    - `backend_output_spec.json`: backend-specific expected output schema and canonical output paths.
      - includes sensor-specific expected export files derived from the ingestion profile (`sensor_exports/<sensor_id>/...`).
      - sensor-specific entries also include `relative_path` and `path_candidates` for backend namespaced layouts such as `sensor_exports/<backend>/<sensor_id>/...`.
      - filenames are backend-specific, for example CARLA camera exports default to `image.json` while AWSIM camera exports default to `rgb_frame.json`.
      - sensor entries are additionally classified by `output_role` and `artifact_type` so runtime/pipeline layers can distinguish visible camera, depth camera, semantic camera, lidar point clouds, radar detections, and radar tracks.
      - grouped views are exposed through `expected_outputs_by_role` and `expected_outputs_by_artifact_type`.
      - grouped contract views also retain `sensor_ids`, `data_formats`, `backend_filenames`, and embedded-output metadata so actual backend exports can be compared role-by-role.
      - `radar_tracks_json` exports also expose an embedded `radar_detections` logical output role from the same artifact when track mode is enabled.
    - `backend_direct_run_command.sh`: executable shell command generated from `backend_runner_request.json`.
    - `backend_runner_execution_manifest.json`: standalone runner execution status and artifact pointers, including grouped expected-output discovery by `output_role` and `artifact_type`.
    - `backend_output_inspection_manifest.json`: compare-only inspection summary for existing backend outputs, generated without executing the backend binary.
    - `backend_runner_smoke_manifest.json`: combined post-run smoke audit manifest that links standalone execution and follow-up inspection in one artifact.
    - `backend_sensor_output_summary.json`: sensor-grouped output discovery summary generated from expected-output inspection, including `status`, `coverage_ratio`, `output_role_counts`, `artifact_type_counts`, `output_roles`, and `artifact_types`.
    - `backend_output_smoke_report.json`: completeness-oriented output smoke report with overall `COMPLETE|PARTIAL|MISSING|UNOBSERVED` status plus grouped summaries by sensor, `output_role`, and `artifact_type`.
      - grouped summaries now retain `found_sensor_ids` / `missing_sensor_ids`, `data_formats`, `carrier_data_formats`, `backend_filenames`, and `embedded_output_count`.
    - `backend_output_comparison_report.json`: output-root discovery/comparison report that scans actual backend files, highlights unexpected exports, and distinguishes `CANONICAL`, `CANDIDATE`, and `EMBEDDED_SHARED` matches against the contract.
      - includes top-level and per-sensor `mismatch_reasons`, plus `found_output_roles`, `missing_output_roles`, and matched/unexpected relative paths for faster triage.
      - each sensor also includes `role_diffs` so `camera/lidar/radar` role-level mismatches can be inspected directly, including `expected_backend_filenames`, `discovered_backend_filenames`, and `BACKEND_FILENAME_MISMATCH`.
    - `backend_runner_stdout.log` / `backend_runner_stderr.log`: stdout/stderr captured by standalone runner execution.
    - `backend_wrapper_invocation.json`: wrapper input/output args snapshot (when wrapper path is used and execution is enabled).
  - direct execution:
    - `renderer_execute_via_runner=true` executes the backend using `backend_runner_request.json` instead of the wrapper/renderer command path.
    - in that mode, plan/invocation/run-manifest keep both the planned wrapper path and the actual `execution_command` / `execution_command_source=backend_runner`.
    - the same request can be executed standalone via `python -m hybrid_sensor_sim.renderers.backend_runner <backend_runner_request.json>`.
    - standalone runner execution inspects `expected_outputs` from `backend_output_spec.json`, records found/missing output artifacts in `backend_runner_execution_manifest.json`, and writes both `backend_output_smoke_report.json` and `backend_output_comparison_report.json` for completeness and unexpected-output checks.
    - `python -m hybrid_sensor_sim.renderers.backend_runner --compare-only <backend_runner_request.json>` skips backend execution and re-runs output inspection/comparison against an existing `output_root`.
    - `python -m hybrid_sensor_sim.renderers.backend_runner --execute-and-inspect <backend_runner_request.json>` performs direct backend execution and then writes `backend_runner_smoke_manifest.json` after a follow-up inspection pass.
    - `renderer_execute_and_inspect_via_runner=true` makes renderer runtime use the same execute-plus-inspect flow and surfaces `backend_output_inspection_manifest.json` and `backend_runner_smoke_manifest.json` in runtime artifacts.
    - combine `renderer_execute_and_inspect_via_runner=true` with `renderer_fail_on_error=true` to fail hybrid runs on backend output contract mismatches, not only process exit failures.
    - `python3 scripts/run_renderer_backend_smoke.py --config <config.json> --backend awsim --backend-bin <awsim_bin>` writes:
      - `renderer_backend_smoke_config.json`
      - `renderer_backend_smoke_summary.json`
      - `renderer_backend_smoke_report.md`
      - `renderer_backend_smoke_report.html`
      - runtime artifacts under the chosen `output_dir`
      - `comparison_table.sensor_rows` and `comparison_table.role_rows` for quick mismatch triage
      - the Markdown/HTML reports expose the same sensor/role mismatch tables for faster human review
    - config files may use `${ENV_NAME}` or `${ENV_NAME:-default}` placeholders for local binary/map/output wiring
  - contract argument controls:
    - `renderer_contract_flag` (default `--contract`)
    - `renderer_inject_contract_arg` / `renderer_contract_positional`
    - frame manifest arg: `renderer_inject_frame_manifest_arg` (default `true`), `renderer_frame_manifest_flag` (default `--frame-manifest`), `renderer_frame_manifest_positional`
    - ingestion profile arg: `renderer_inject_ingestion_profile_arg` (default wrapper mode only), `renderer_ingestion_profile_flag` (default `--ingestion-profile`), `renderer_ingestion_profile_positional`
    - bundle summary arg: `renderer_inject_bundle_summary_arg` (default wrapper mode only), `renderer_bundle_summary_flag` (default `--sensor-bundle-summary`), `renderer_bundle_summary_positional`
    - frame manifest selection: `renderer_backend_frame_start` (default `0`), `renderer_backend_frame_stride` (default `1`), `renderer_backend_max_frames` (default all)
  - safety behavior:
    - `renderer_fail_on_error=true` makes hybrid result fail when renderer runtime fails.
  - emits `renderer_runtime/renderer_execution_plan.json` (+ stdout/stderr logs on execute).

## Example configs

- [configs/hybrid_sensor_sim.example.json](/Users/seongcheoljeong/Documents/Test/configs/hybrid_sensor_sim.example.json): minimal dry-run/fallback config.
- [configs/hybrid_sensor_sim.helios_demo.json](/Users/seongcheoljeong/Documents/Test/configs/hybrid_sensor_sim.helios_demo.json): HELIOS demo survey config (requires built `HELIOS_BIN`).
- [configs/hybrid_sensor_sim.helios_docker.json](/Users/seongcheoljeong/Documents/Test/configs/hybrid_sensor_sim.helios_docker.json): docker runtime demo config.
- [configs/hybrid_sensor_sim.helios_docker.auto_extrinsics.json](/Users/seongcheoljeong/Documents/Test/configs/hybrid_sensor_sim.helios_docker.auto_extrinsics.json): docker demo with trajectory-based auto extrinsics.
- [configs/renderer_backend_smoke.awsim.example.json](/Users/seongcheoljeong/Documents/Test/configs/renderer_backend_smoke.awsim.example.json): AWSIM smoke preset with camera/lidar/radar contract coverage enabled.
- [configs/renderer_backend_smoke.carla.example.json](/Users/seongcheoljeong/Documents/Test/configs/renderer_backend_smoke.carla.example.json): CARLA smoke preset with the same sensor contract surface.
- [configs/renderer_backend_smoke.awsim.local.example.json](/Users/seongcheoljeong/Documents/Test/configs/renderer_backend_smoke.awsim.local.example.json): env-driven AWSIM local smoke preset using `${HELIOS_BIN}`, `${AWSIM_BIN}`, `${AWSIM_RENDERER_MAP}`.
- [configs/renderer_backend_smoke.carla.local.example.json](/Users/seongcheoljeong/Documents/Test/configs/renderer_backend_smoke.carla.local.example.json): env-driven CARLA local smoke preset using `${HELIOS_BIN}`, `${CARLA_BIN}`, `${CARLA_RENDERER_MAP}`.
- [configs/renderer_backend_smoke.awsim.local.docker.example.json](/Users/seongcheoljeong/Documents/Test/configs/renderer_backend_smoke.awsim.local.docker.example.json): env-driven AWSIM local smoke preset using HELIOS docker runtime (`${HELIOS_DOCKER_IMAGE}`, `${AWSIM_BIN}`).
- [configs/renderer_backend_smoke.carla.local.docker.example.json](/Users/seongcheoljeong/Documents/Test/configs/renderer_backend_smoke.carla.local.docker.example.json): env-driven CARLA local smoke preset using HELIOS docker runtime (`${HELIOS_DOCKER_IMAGE}`, `${CARLA_BIN}`).

### Local runtime discovery

- `python3 scripts/discover_renderer_backend_local_env.py` writes:
  - `artifacts/renderer_backend_local_setup/renderer_backend_local_setup.json`
  - `artifacts/renderer_backend_local_setup/renderer_backend_local.env.sh`
- `python3 scripts/discover_renderer_backend_local_env.py --probe-helios-docker-demo` also writes:
  - `artifacts/renderer_backend_local_setup/helios_docker_probe.json`
- `python3 scripts/discover_renderer_backend_local_env.py --probe-linux-handoff-docker-selftest --probe-linux-handoff-docker-selftest-execute` also writes:
  - `artifacts/renderer_backend_local_setup/linux_handoff_docker_selftest_probe/renderer_backend_linux_handoff_selftest.json`
- the summary reports:
  - selected `HELIOS_BIN`, `AWSIM_BIN`, `CARLA_BIN`
  - selected `HELIOS_DOCKER_IMAGE`
  - `helios_binary_ready`, `helios_binary_host_compatible`, `helios_docker_ready`, `helios_ready`
  - `awsim_ready`, `awsim_host_compatible`, `carla_ready`, `carla_host_compatible`
  - `awsim_smoke_ready_binary`, `awsim_smoke_ready_docker`, `awsim_smoke_ready`
  - `carla_smoke_ready_binary`, `carla_smoke_ready_docker`, `carla_smoke_ready`
  - reference repo roots versus executable runtime candidates
  - candidate-level binary format, architecture, translation requirements, and host compatibility, so unsupported runtimes are surfaced before smoke runs
  - `acquisition_hints` with backend-specific download/build guidance and platform constraints
  - package executable names such as `AWSIM-Demo.x86_64` and `CarlaUnreal.sh`, plus locally downloaded archives like `AWSIM-Demo.zip` and `CARLA_UE5_Latest.tar.gz`
  - staged runtime metadata under `third_party/runtime_backends/<backend>/renderer_backend_package_stage.json` is also reused, so a previously staged backend is rediscovered without re-entering the path manually
- when `heliosplusplus:cli` is present in Docker Desktop, discovery can mark HELIOS as docker-ready even if `HELIOS_BIN` is unset.
- `--probe-helios-docker-demo` runs the configured docker demo and records actual HELIOS execution success/failure in `probes.helios_docker_demo`.
- `--probe-linux-handoff-docker-selftest` runs the synthetic Linux handoff Docker self-test and records the result in `probes.linux_handoff_docker_selftest`.
- `--probe-backend-workflow-selftest` runs the higher-level workflow self-test and records the result in `probes.backend_workflow_selftest`.
- `--probe-backend-package-workflow-selftest` runs the packaged backend workflow self-test and records the result in `probes.backend_package_workflow_selftest`.
- the Linux handoff Docker self-test summary carries `generated_at_utc`; workflow also falls back to the probe summary file mtime or setup summary mtime when it needs freshness metadata from older summaries.
- `run_renderer_backend_workflow.py --run-linux-handoff-docker` reads that probe as a Docker preflight summary. It now tracks `generated_at_utc`, `age_seconds`, `max_age_seconds`, `timestamp_source`, and `stale`.
- workflow reports:
  - `HANDOFF_DOCKER_PREFLIGHT_FAILED` when the cached probe is fresh but failed
  - `HANDOFF_DOCKER_PREFLIGHT_STALE` when the cached probe is older than `--docker-handoff-preflight-max-age-seconds`
- `--refresh-docker-handoff-preflight` reruns local setup with `--probe-linux-handoff-docker-selftest` when the cached Docker handoff preflight is missing or stale.
- workflow smoke config materialization now uses setup-summary selections as env overrides, so docker presets with `${AWSIM_BIN}` / `${HELIOS_DOCKER_IMAGE}` style placeholders can be resolved without first exporting those variables into the shell.
- use `--no-default-search-roots` when you want discovery to only scan explicit `--search-root` inputs plus the repo root.
- discovery now ignores known synthetic self-test artifact directories while recursively scanning broad roots, so workflow self-test stubs do not pollute normal AWSIM/CARLA runtime selection.
- local setup summary now also includes:
  - `probe_readiness`
  - `workflow_paths`
  - `artifacts.report_path`
- `probe_readiness` now also surfaces `backend_package_workflow_selftest_ready` / `backend_package_workflow_status`, and `workflow_paths` includes `package_workflow_path_ready`.
- `renderer_backend_local_report.md` gives a compact runtime/probe/path readiness view without manually inspecting the full JSON.

### Local backend package staging

- `python3 scripts/stage_renderer_backend_package.py --backend awsim --archive ~/Downloads/AWSIM-Demo.zip`
- `python3 scripts/stage_renderer_backend_package.py --backend carla --archive ~/Downloads/CARLA_UE5_Latest.tar.gz`
- if `renderer_backend_local_setup.json` already exists and has `acquisition_hints.<backend>.local_download_candidates`, `--archive` can be omitted:
  - `python3 scripts/stage_renderer_backend_package.py --backend awsim --setup-summary artifacts/renderer_backend_local_setup/renderer_backend_local_setup.json`
- the staging tool writes:
  - `third_party/runtime_backends/<backend>/renderer_backend_package_stage.json`
  - `third_party/runtime_backends/<backend>/renderer_backend_package_stage.env.sh`
- the summary reports:
  - selected archive path/source
  - extracted runtime directory
  - selected backend executable path/name
  - merged env selection for `HELIOS_*` plus staged `AWSIM_BIN` or `CARLA_BIN`
  - `smoke_ready_binary` / `smoke_ready_docker`
- the env file is meant to be sourced directly before smoke runs:
  - `source third_party/runtime_backends/awsim/renderer_backend_package_stage.env.sh`
  - `python3 scripts/run_renderer_backend_smoke.py --config configs/renderer_backend_smoke.awsim.local.docker.example.json --backend awsim`

### Local backend package acquire

- `python3 scripts/acquire_renderer_backend_package.py --backend awsim --setup-summary artifacts/renderer_backend_local_setup/renderer_backend_local_setup.json`
- `python3 scripts/acquire_renderer_backend_package.py --backend carla --setup-summary artifacts/renderer_backend_local_setup/renderer_backend_local_setup.json --dry-run`
- behavior:
  - if `acquisition_hints.<backend>.local_download_candidates` already points to an existing archive, that local archive is reused before any network download
  - resolves the first `acquisition_hints.<backend>.download_options[*].url`
  - downloads the archive into `~/Downloads` by default
  - reuses an existing archive unless `--overwrite-download` is set
  - stages the archive automatically unless `--download-only` is set
- emits:
  - `third_party/runtime_backends/<backend>/renderer_backend_package_acquire.json`
  - plus the staging artifacts from `stage_renderer_backend_package.py` when staging is enabled

### Local backend workflow

- `python3 scripts/run_renderer_backend_workflow.py --backend awsim --setup-summary artifacts/renderer_backend_local_setup/renderer_backend_local_setup.json --auto-acquire`
- `python3 scripts/run_renderer_backend_workflow.py --backend carla --setup-summary artifacts/renderer_backend_local_setup/renderer_backend_local_setup.json --dry-run`
- `python3 scripts/run_renderer_backend_workflow.py --backend awsim --setup-summary artifacts/renderer_backend_local_setup/renderer_backend_local_setup.json --dry-run --pack-linux-handoff --verify-linux-handoff-bundle`
- `python3 scripts/run_renderer_backend_workflow.py --backend awsim --setup-summary artifacts/renderer_backend_local_setup/renderer_backend_local_setup.json --dry-run --run-linux-handoff-docker`
- `python3 scripts/run_renderer_backend_workflow.py --backend awsim --setup-summary artifacts/renderer_backend_local_setup/renderer_backend_local_setup.json --dry-run --run-linux-handoff-docker --docker-handoff-preflight-max-age-seconds 3600 --refresh-docker-handoff-preflight`
- `python3 scripts/run_renderer_backend_linux_handoff.py --bundle <handoff_bundle.tar.gz> --transfer-manifest <renderer_backend_workflow_linux_handoff_transfer_manifest.json> --bundle-manifest <renderer_backend_workflow_linux_handoff_bundle_manifest.json> --repo-root <linux_repo_checkout>`
- `python3 scripts/run_renderer_backend_linux_handoff_selftest.py --output-root artifacts/renderer_backend_linux_handoff_selftest --execute`
- behavior:
  - loads or generates local setup summary
  - reuses resolved `HELIOS_*`, backend binary, and renderer map selections
  - blocks smoke when the selected backend binary exists but is not executable on the current host
  - when the runtime is host-incompatible, materializes a Linux-runner handoff config/env/script instead of stopping at a blocker message
  - `--pack-linux-handoff` also builds the handoff tarball locally
- `--verify-linux-handoff-bundle` unpacks that tarball into a local verification root and checks per-file checksums before any runner-side execution
  - `--run-linux-handoff-docker` runs the handoff helper inside a local Linux container; by default this is verify-only, and `--docker-handoff-execute` opts into executing the extracted handoff script too
  - `--docker-handoff-preflight-max-age-seconds <n>` bounds how long a cached Docker handoff preflight probe is trusted
  - `--refresh-docker-handoff-preflight` reruns the Docker handoff self-test via local setup when the cached probe is missing or stale
  - if backend runtime is missing and `--auto-acquire` is set, runs acquire+stage automatically
  - runs `renderer_backend_smoke.py` when all prerequisites are ready
- emits:
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow_summary.json`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow.env.sh`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow_report.md`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow_next_step.sh`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow_smoke_config.json`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow_rerun_smoke.sh`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow_linux_handoff_config.json`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow_linux_handoff.env.sh`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow_linux_handoff.sh`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow_linux_handoff_docker.sh`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow_linux_handoff_transfer_manifest.json`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow_linux_handoff_pack.sh`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow_linux_handoff_unpack.sh`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow_linux_handoff_bundle_manifest.json`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_workflow_linux_handoff_verification.json`
  - `artifacts/renderer_backend_workflow/<backend>/renderer_backend_linux_handoff_docker_run/renderer_backend_linux_handoff_docker_run.json`
  - `artifacts/renderer_backend_workflow/<backend>/local_setup_refreshed/renderer_backend_local_setup.json`
  - `artifacts/renderer_backend_workflow/<backend>/local_setup_refreshed/renderer_backend_local.env.sh`
  - plus smoke artifacts/reports when smoke executes
- the workflow summary/report now includes structured blocker codes, a recommended next command, and Linux handoff transfer/env requirements when the selected runtime must move to a Linux runner
- the Linux handoff path also emits a transfer manifest with per-file verification data, a local pack script, a bundle manifest, and a Linux unpack/verify script so the required inputs can be bundled and revalidated before smoke execution
- `scripts/run_renderer_backend_linux_handoff.py` is the runner-side helper that consumes the bundle plus manifests, revalidates checksums, and optionally executes the extracted handoff script
- `scripts/run_renderer_backend_linux_handoff_docker.py` runs the same handoff helper in a local Linux Docker container; the generated `renderer_backend_workflow_linux_handoff_docker.sh` defaults to verify-only (`HANDOFF_SKIP_RUN=1`) so local container checks stay safe by default
- both handoff runner scripts now bootstrap `src/` themselves, so `python3 scripts/run_renderer_backend_linux_handoff*.py ...` works without manually exporting `PYTHONPATH`
- `scripts/run_renderer_backend_linux_handoff_selftest.py` builds a synthetic handoff bundle and runs it through the Docker helper, so the full `bundle -> container verify/run` path can be smoke-tested without AWSIM/CARLA
- `scripts/run_renderer_backend_workflow_selftest.py` runs a higher-level workflow self-test:
  - seeds a stale cached Docker preflight probe
  - synthesizes a host-incompatible backend runtime stub
  - runs `run_renderer_backend_workflow.py --dry-run --run-linux-handoff-docker --refresh-docker-handoff-preflight`
  - verifies that workflow refreshes the stale preflight and reaches `HANDOFF_DOCKER_VERIFIED`
- example:
  - `python3 scripts/run_renderer_backend_workflow_selftest.py --output-root artifacts/renderer_backend_workflow_selftest_probe`
- emitted artifacts:
  - `artifacts/renderer_backend_workflow_selftest/<backend>/renderer_backend_workflow_selftest.json` when you choose a backend-specific output root
  - or whatever `--output-root` you pass, including:
    - `seed_setup/renderer_backend_local_setup.json`
    - `workflow_run/renderer_backend_workflow_summary.json`
    - the refreshed local setup and Docker handoff workflow artifacts under `workflow_run/`

## Next implementation target

- Add optional `vehicle_dynamics` coupling into the current object-sim ego longitudinal update.
- Deepen map-aware scenario/object-sim consumption on top of the new canonical map utilities.
