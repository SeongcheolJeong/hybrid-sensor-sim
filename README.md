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

## HELIOS execution modes

- Runtime selection (`options.helios_runtime`):
  - `binary`: use local `HELIOS_BIN` / built binary.
  - `docker`: run HELIOS inside container (`docker run`).
  - `auto`: try binary first, then docker fallback.
- Execution control:
  - `execute_helios=false`: creates execution plan only (safe dry run).
  - `execute_helios=true`: executes HELIOS and records `stdout/stderr`.
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

## LiDAR/Radar post-physics notes

- LiDAR noisy preview:
  - enable `lidar_postprocess_enabled=true`
  - noise/dropout controls: `lidar_noise`, `lidar_noise_stddev_m`, `lidar_dropout_probability`
  - emits `lidar_noisy_preview.xyz`.
- Radar target preview:
  - enable `radar_postprocess_enabled=true`
  - core controls: `radar_max_targets`, `radar_range_min_m`, `radar_range_max_m`
  - clutter/false alarms: `radar_clutter`, `radar_false_target_count`
  - ego-motion velocity source: `radar_use_ego_velocity_from_trajectory`
  - emits `radar_targets_preview.json`.

## Example configs

- [configs/hybrid_sensor_sim.example.json](/Users/seongcheoljeong/Documents/Test/configs/hybrid_sensor_sim.example.json): minimal dry-run/fallback config.
- [configs/hybrid_sensor_sim.helios_demo.json](/Users/seongcheoljeong/Documents/Test/configs/hybrid_sensor_sim.helios_demo.json): HELIOS demo survey config (requires built `HELIOS_BIN`).
- [configs/hybrid_sensor_sim.helios_docker.json](/Users/seongcheoljeong/Documents/Test/configs/hybrid_sensor_sim.helios_docker.json): docker runtime demo config.
- [configs/hybrid_sensor_sim.helios_docker.auto_extrinsics.json](/Users/seongcheoljeong/Documents/Test/configs/hybrid_sensor_sim.helios_docker.auto_extrinsics.json): docker demo with trajectory-based auto extrinsics.

## Next implementation target

- Add strict schema mapping from project scenario format to HELIOS survey XML.
- Implement camera/lidar/radar post-physics from detected HELIOS artifacts.
