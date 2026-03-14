# Linux Closed-Loop Handoff Guide

## Purpose

This guide is for continuing the AWSIM + Autoware closed-loop lane on a different Linux PC.

Use it when:

- the code is already on GitHub
- the target machine is Linux and GPU-capable
- you want `git clone -> preflight -> first demo run` without reverse-engineering the helper contract

## What GitHub Already Contains

The repository already contains:

- the closed-loop workflow:
  - [/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_closed_loop_demo.py](/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_closed_loop_demo.py)
- the control-plane API and UI:
  - [/Users/seongcheoljeong/Documents/Test/scripts/run_control_plane_api.py](/Users/seongcheoljeong/Documents/Test/scripts/run_control_plane_api.py)
  - [/Users/seongcheoljeong/Documents/Test/apps/control-plane](/Users/seongcheoljeong/Documents/Test/apps/control-plane)
- the helper-script templates:
  - [/Users/seongcheoljeong/Documents/Test/examples/closed_loop/linux_runtime_root/bin](/Users/seongcheoljeong/Documents/Test/examples/closed_loop/linux_runtime_root/bin)
- the sample launch payloads:
  - [/Users/seongcheoljeong/Documents/Test/examples/closed_loop/closed_loop_demo_request_v0.json](/Users/seongcheoljeong/Documents/Test/examples/closed_loop/closed_loop_demo_request_v0.json)

GitHub does not contain:

- AWSIM packaged runtime binaries
- Autoware workspace contents
- ROS2 installation
- recorded videos
- rosbags

## Linux Host Prerequisites

The target machine should provide:

- Ubuntu or a similar Linux host
- NVIDIA GPU driver and a working `nvidia-smi`
- `python3` and `python3-venv`
- `ros2`
- an Autoware workspace with `install/setup.bash`
- an unpacked AWSIM packaged runtime
- `ffmpeg`
- Node.js 20+ and `npm`

Recommended absolute paths:

- repo root: `/home/operator/work/hybrid-sensor-sim`
- Linux runtime root: `/opt/hybrid-runtime`
- Autoware workspace: `/opt/autoware`
- AWSIM runtime root: `/opt/awsim/AWSIM-Demo-Lightweight`

## Clone And Bootstrap

Clone:

```bash
git clone https://github.com/SeongcheolJeong/hybrid-sensor-sim.git /home/operator/work/hybrid-sensor-sim
cd /home/operator/work/hybrid-sensor-sim
```

Python:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e . fastapi uvicorn pydantic
```

Frontend:

```bash
cd /home/operator/work/hybrid-sensor-sim/apps/control-plane
npm install
cd /home/operator/work/hybrid-sensor-sim
```

Copy the helper templates into the Linux runtime root:

```bash
sudo mkdir -p /opt/hybrid-runtime
sudo rsync -av /home/operator/work/hybrid-sensor-sim/examples/closed_loop/linux_runtime_root/ /opt/hybrid-runtime/
sudo chmod +x /opt/hybrid-runtime/bin/*.sh
```

Then edit the copied helper scripts under `/opt/hybrid-runtime/bin/` and replace the `TODO` sections with the real AWSIM, Autoware, route-goal, readiness-check, capture, and rosbag commands for that machine.

## Required And Optional Helpers

Required for the first real closed-loop demo:

- `launch_awsim_closed_loop.sh`
- `launch_autoware_closed_loop.sh`
- `send_route_goal.sh`
- `check_localization_ready.sh`
- `check_perception_ready.sh`
- `check_planning_ready.sh`
- `check_control_ready.sh`
- `check_vehicle_motion.sh`
- `capture_awsim_video.sh`

Optional on the first pass:

- `check_route_completed.sh`
- `capture_rviz_video.sh`
- `record_rosbag.sh`

If you omit optional helpers, keep the related flags disabled.

## First Preflight Run

Start with `--preflight-only`. This validates:

- Linux host guard
- ROS2
- Autoware workspace
- AWSIM runtime root
- Autoware topic/manifest inputs
- helper contract completeness

Command:

```bash
cd /home/operator/work/hybrid-sensor-sim
source .venv/bin/activate

python3 /home/operator/work/hybrid-sensor-sim/scripts/run_scenario_closed_loop_demo.py \
  --scenario-path /home/operator/work/hybrid-sensor-sim/tests/fixtures/autonomy_e2e/p_sim_engine/highway_safe_following_v0.json \
  --linux-runtime-root /opt/hybrid-runtime \
  --autoware-workspace-root /opt/autoware \
  --awsim-runtime-root /opt/awsim/AWSIM-Demo-Lightweight \
  --map-path /home/operator/work/hybrid-sensor-sim/tests/fixtures/autonomy_e2e/p_map_toolset/simple_map_v0.json \
  --route-path /home/operator/work/hybrid-sensor-sim/tests/fixtures/autonomy_e2e/p_sim_engine/highway_map_route_following_v0.json \
  --autoware-pipeline-manifest /home/operator/work/hybrid-sensor-sim/artifacts/autoware_primary_semantic_embedded_probe_v1/autoware/autoware_pipeline_manifest.json \
  --autoware-dataset-manifest /home/operator/work/hybrid-sensor-sim/artifacts/autoware_primary_semantic_embedded_probe_v1/autoware/autoware_dataset_manifest.json \
  --autoware-topic-catalog /home/operator/work/hybrid-sensor-sim/artifacts/autoware_primary_semantic_embedded_probe_v1/autoware/autoware_topic_catalog.json \
  --autoware-consumer-input-manifest /home/operator/work/hybrid-sensor-sim/artifacts/autoware_primary_semantic_embedded_probe_v1/autoware/autoware_consumer_input_manifest.json \
  --out-root /home/operator/work/hybrid-sensor-sim/artifacts/linux_closed_loop_preflight \
  --record-video=false \
  --record-rviz=false \
  --record-rosbag=false \
  --preflight-only
```

Inspect:

- `/home/operator/work/hybrid-sensor-sim/artifacts/linux_closed_loop_preflight/scenario_closed_loop_demo_report_v0.json`
- `/home/operator/work/hybrid-sensor-sim/artifacts/linux_closed_loop_preflight/scenario_closed_loop_demo_report_v0.md`

Expected first good result:

- status `PLANNED` when all blockers are cleared

## First Control-Plane Flow

API:

```bash
cd /home/operator/work/hybrid-sensor-sim
source .venv/bin/activate
python3 /home/operator/work/hybrid-sensor-sim/scripts/run_control_plane_api.py --host 0.0.0.0 --port 8000
```

Frontend:

```bash
cd /home/operator/work/hybrid-sensor-sim/apps/control-plane
npm run dev -- --host 0.0.0.0 --port 4173
```

Then:

1. open the Runtime page
2. click `Launch Closed-Loop Demo`
3. paste the preflight or full-demo payload from [/Users/seongcheoljeong/Documents/Test/examples/closed_loop/closed_loop_demo_request_v0.json](/Users/seongcheoljeong/Documents/Test/examples/closed_loop/closed_loop_demo_request_v0.json)
4. submit
5. inspect:
   - blocker codes
   - recommended next command
   - run detail artifacts
   - telemetry and capture outputs

## Sample API Request

You can also call the API directly with the checked-in sample file:

- [/Users/seongcheoljeong/Documents/Test/examples/closed_loop/closed_loop_demo_request_v0.json](/Users/seongcheoljeong/Documents/Test/examples/closed_loop/closed_loop_demo_request_v0.json)

Example:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/runs/closed-loop-demo \
  -H 'Content-Type: application/json' \
  -d @<(jq '.preflight_only_request' /home/operator/work/hybrid-sensor-sim/examples/closed_loop/closed_loop_demo_request_v0.json)
```

When preflight passes, switch to:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/runs/closed-loop-demo \
  -H 'Content-Type: application/json' \
  -d @<(jq '.full_demo_request' /home/operator/work/hybrid-sensor-sim/examples/closed_loop/closed_loop_demo_request_v0.json)
```

## Blocker Interpretation

| Code | Meaning | What to fix |
| --- | --- | --- |
| `LINUX_RUNTIME_MISSING` | the workflow is not on a Linux host, or `linux_runtime_root` is missing | run on Linux and populate `/opt/hybrid-runtime` |
| `ROS2_MISSING` | `ros2` is not available after sourcing Autoware | fix ROS2 installation or the Autoware `install/setup.bash` |
| `AUTOWARE_WORKSPACE_MISSING` | the Autoware workspace path is wrong or incomplete | point to a real workspace that contains `install/setup.bash` |
| `AWSIM_RUNTIME_MISSING` | the AWSIM runtime root does not contain a runnable packaged binary | unpack AWSIM and point `--awsim-runtime-root` to it |
| `TOPIC_BRIDGE_MISSING` | the required Autoware manifest/topic inputs are missing or incomplete | provide the manifest paths or a runtime/backend report that contains them |
| `CONTROL_LOOP_MISSING` | one or more required helper scripts are absent or still placeholders | fill in the required scripts under `/opt/hybrid-runtime/bin/` |
| `VIDEO_CAPTURE_FAILED` | requested capture output did not materialize | fix `ffmpeg` or the capture helper and rerun |

## Expected Outputs

The workflow writes:

- `scenario_closed_loop_demo_report_v0.json`
- `scenario_closed_loop_demo_report_v0.md`
- `run_telemetry.json`
- `awsim_camera_capture.mp4`
- optional `rviz_capture.mp4`
- optional `rosbag/`

The report also records:

- readiness fields
- blocker codes
- recommended next command
- artifact paths
- heartbeat timeline

## Publish Discipline On The Linux PC

After making Linux-host changes:

```bash
cd /home/operator/work/hybrid-sensor-sim
source .venv/bin/activate

PYTHONPATH=src python3 -m unittest discover -s tests -q

python3 /home/operator/work/hybrid-sensor-sim/scripts/run_autonomy_e2e_history_refresh.py \
  --source-repo-root /path/to/Autonomy-E2E \
  --current-repo-root /home/operator/work/hybrid-sensor-sim \
  --output-root /home/operator/work/hybrid-sensor-sim/metadata/autonomy_e2e

python3 /home/operator/work/hybrid-sensor-sim/scripts/run_autonomy_e2e_history_guard.py \
  --metadata-root /home/operator/work/hybrid-sensor-sim/metadata/autonomy_e2e \
  --current-repo-root /home/operator/work/hybrid-sensor-sim \
  --compare-ref origin/main
```
