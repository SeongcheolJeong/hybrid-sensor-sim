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
- `src/hybrid_sensor_sim/config.py`: typed Sensor Sim config translation layer for camera/lidar/radar/renderer blocks.
- `src/hybrid_sensor_sim/io/survey_mapping.py`: scenario JSON to HELIOS survey XML mapper.
- `src/hybrid_sensor_sim/renderers/playback_contract.py`: renderer playback contract builder for CARLA/AWSIM bridge.
- `src/hybrid_sensor_sim/orchestrator.py`: mode selection and pipeline chaining.
- `docs/hybrid_helios_plan.md`: functional roadmap and risk management.
- `scripts/setup_helios.sh`: bootstrap helper for cloning/building HELIOS.

## Quick start

```bash
PYTHONPATH=src python3 -m hybrid_sensor_sim.cli --config configs/hybrid_sensor_sim.example.json
```

Run tests:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q
```

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

- A typed sensor config manifest is emitted as `sensor_sim_config.json` in both native-only and hybrid-enhanced outputs.
- Supported camera geometry models in the local physics path:
  - `pinhole`
  - `rectilinear`
  - `equidistant`
  - `orthographic`
- Supported camera output modes in the local physics preview path:
  - `camera_sensor_type=VISIBLE`
  - `camera_sensor_type=DEPTH`
- Depth output controls:
  - `camera_depth_params.min`
  - `camera_depth_params.max`
  - `camera_depth_params.type=LINEAR|LOG|RAW`
  - `camera_depth_params.log_base`
  - `camera_depth_params.bit_depth`
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
  - depth mode emits `preview_depth_samples`.
  - rolling shutter mode emits `preview_readout_samples` and timing metadata.

## LiDAR/Radar post-physics notes

- LiDAR noisy preview:
  - enable `lidar_postprocess_enabled=true`
  - noise/dropout controls: `lidar_noise`, `lidar_noise_stddev_m`, `lidar_dropout_probability`
  - emits `lidar_noisy_preview.xyz`.
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
  - emits `lidar_trajectory_sweep.json`.
- Radar target preview:
  - enable `radar_postprocess_enabled=true`
  - core controls: `radar_max_targets`, `radar_range_min_m`, `radar_range_max_m`
  - clutter/false alarms: `radar_clutter`, `radar_false_target_count`
  - ego-motion velocity source: `radar_use_ego_velocity_from_trajectory`
  - emits `radar_targets_preview.json`.
- Radar trajectory sweep preview:
  - enable `radar_trajectory_sweep_enabled=true`
  - set `radar_trajectory_sweep_frames` and `radar_preview_targets_per_frame`
  - optional auto-extrinsics from trajectory:
    - `radar_extrinsics_auto_use_position=none|xy|xyz`
    - `radar_extrinsics_auto_use_orientation=true|false`
    - `radar_extrinsics_auto_offsets`
  - emits `radar_targets_trajectory_sweep.json`.

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
  - camera setup now also carries `sensor_type`, `depth_params`, and `rolling_shutter`.
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
  - execution plan includes `backend_args_preview` for normalized scene/sensor-mount argument inspection.
  - runtime artifacts:
    - `backend_invocation.json`: normalized backend command + preview snapshot.
    - `backend_frame_inputs_manifest.json`: contract frame sources resolved into backend-consumable payload pointers, enriched with `sensor_id` / `data_format` / `attach_to_actor_id`.
      - depth cameras are tagged as `camera_depth_json`.
    - `backend_ingestion_profile.json`: backend-specific ingest flag/value expansion generated from frame manifest.
    - `backend_launcher_template.json`: deduplicated backend launch args (`meta_args` + `frame_args`) for direct runner integration.
    - `backend_ingestion_args.sh`: shell-ready `BACKEND_INGEST_ARGS` array generated from launcher template.
    - `backend_wrapper_invocation.json`: wrapper input/output args snapshot (when wrapper path is used and execution is enabled).
  - contract argument controls:
    - `renderer_contract_flag` (default `--contract`)
    - `renderer_inject_contract_arg` / `renderer_contract_positional`
    - frame manifest arg: `renderer_inject_frame_manifest_arg` (default `true`), `renderer_frame_manifest_flag` (default `--frame-manifest`), `renderer_frame_manifest_positional`
    - ingestion profile arg: `renderer_inject_ingestion_profile_arg` (default wrapper mode only), `renderer_ingestion_profile_flag` (default `--ingestion-profile`), `renderer_ingestion_profile_positional`
    - frame manifest selection: `renderer_backend_frame_start` (default `0`), `renderer_backend_frame_stride` (default `1`), `renderer_backend_max_frames` (default all)
  - safety behavior:
    - `renderer_fail_on_error=true` makes hybrid result fail when renderer runtime fails.
  - emits `renderer_runtime/renderer_execution_plan.json` (+ stdout/stderr logs on execute).

## Example configs

- [configs/hybrid_sensor_sim.example.json](/Users/seongcheoljeong/Documents/Test/configs/hybrid_sensor_sim.example.json): minimal dry-run/fallback config.
- [configs/hybrid_sensor_sim.helios_demo.json](/Users/seongcheoljeong/Documents/Test/configs/hybrid_sensor_sim.helios_demo.json): HELIOS demo survey config (requires built `HELIOS_BIN`).
- [configs/hybrid_sensor_sim.helios_docker.json](/Users/seongcheoljeong/Documents/Test/configs/hybrid_sensor_sim.helios_docker.json): docker runtime demo config.
- [configs/hybrid_sensor_sim.helios_docker.auto_extrinsics.json](/Users/seongcheoljeong/Documents/Test/configs/hybrid_sensor_sim.helios_docker.auto_extrinsics.json): docker demo with trajectory-based auto extrinsics.

## Next implementation target

- Add strict schema mapping from project scenario format to HELIOS survey XML.
- Add renderer runtime executors (CARLA/AWSIM) that consume playback contracts directly.
