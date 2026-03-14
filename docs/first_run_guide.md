# First-Run Guide

## Purpose

This guide is for someone opening this repository for the first time.

It answers three practical questions:

1. what to read first
2. what to run first
3. how to tell whether the repository is healthy on this machine

Use this together with:

1. [README.md](/Users/seongcheoljeong/Documents/Test/README.md)
2. [architecture_guide.md](/Users/seongcheoljeong/Documents/Test/docs/architecture_guide.md)

## Current Practical Goal

The repository currently optimizes for:

`scenario -> object sim -> native sensor sim -> runtime/backend smoke -> real AWSIM run -> Autoware ingest contract`

This is narrower than a full cloud, HIL, data-explorer, or neural-sim product line.

## Current Status Summary

- strongest verified path: `AWSIM`
- current blocked lane: `CARLA runtime availability`
- recommended first report:
  - `scenario_runtime_backend_probe_set_report_v0.json`
- optional operator surface:
  - React + FastAPI control plane at `apps/control-plane` and `src/hybrid_sensor_sim/server`
- optional Linux-host-only lane:
  - `scripts/run_scenario_closed_loop_demo.py` for AWSIM + Autoware ROS2 closed-loop demo orchestration
  - `docs/linux_closed_loop_handoff.md` for clone-to-first-run continuation on a Linux GPU host

## Read Order

Read in this order:

1. [README.md](/Users/seongcheoljeong/Documents/Test/README.md)
2. [architecture_guide.md](/Users/seongcheoljeong/Documents/Test/docs/architecture_guide.md)
3. this guide
4. [autonomy_e2e_migration_master_plan.md](/Users/seongcheoljeong/Documents/Test/docs/autonomy_e2e_migration_master_plan.md)
5. [p_sim_engine_migration_audit.md](/Users/seongcheoljeong/Documents/Test/docs/p_sim_engine_migration_audit.md)

If you need historical traceability:

6. [autonomy_e2e_history_integration.md](/Users/seongcheoljeong/Documents/Test/docs/autonomy_e2e_history_integration.md)
7. [/Users/seongcheoljeong/Documents/Test/metadata/autonomy_e2e](/Users/seongcheoljeong/Documents/Test/metadata/autonomy_e2e)

## First Health Check

Run these first.

### 1. Full regression

```bash
cd /Users/seongcheoljeong/Documents/Test
PYTHONPATH=src python3 -m unittest discover -s tests -q
```

Expected:

- the full suite passes

### 2. Provenance guard

```bash
cd /Users/seongcheoljeong/Documents/Test
python3 /Users/seongcheoljeong/Documents/Test/scripts/run_autonomy_e2e_history_guard.py \
  --metadata-root /Users/seongcheoljeong/Documents/Test/metadata/autonomy_e2e \
  --current-repo-root /Users/seongcheoljeong/Documents/Test \
  --compare-ref origin/main
```

Expected:

- `PASS`

### 3. Local runtime probe

```bash
cd /Users/seongcheoljeong/Documents/Test
python3 /Users/seongcheoljeong/Documents/Test/scripts/discover_renderer_backend_local_env.py \
  --output-dir /Users/seongcheoljeong/Documents/Test/artifacts/renderer_backend_local_setup_probe_latest \
  --probe-docker-storage
```

Check:

- [/Users/seongcheoljeong/Documents/Test/artifacts/renderer_backend_local_setup_probe_latest/renderer_backend_local_setup.json](/Users/seongcheoljeong/Documents/Test/artifacts/renderer_backend_local_setup_probe_latest/renderer_backend_local_setup.json)

### 4. Optional control-plane startup

API:

```bash
cd /Users/seongcheoljeong/Documents/Test
python3 /Users/seongcheoljeong/Documents/Test/scripts/run_control_plane_api.py --host 127.0.0.1 --port 8000
```

Frontend:

```bash
cd /Users/seongcheoljeong/Documents/Test/apps/control-plane
npm install
npm run dev
```

Use this if you want the same object-sim, runtime, probe-set, and Autoware workflows through the operator UI instead of the CLI.

## First Functional Run

### 1. Object simulation sanity check

```bash
cd /Users/seongcheoljeong/Documents/Test
python3 /Users/seongcheoljeong/Documents/Test/scripts/run_object_sim.py \
  --scenario /Users/seongcheoljeong/Documents/Test/tests/fixtures/autonomy_e2e/p_sim_engine/highway_safe_following_v0.json \
  --run-id RUN_SAFE_001 \
  --seed 42 \
  --out /Users/seongcheoljeong/Documents/Test/artifacts/first_run_object_sim
```

Check:

- [/Users/seongcheoljeong/Documents/Test/artifacts/first_run_object_sim/RUN_SAFE_001/summary.json](/Users/seongcheoljeong/Documents/Test/artifacts/first_run_object_sim/RUN_SAFE_001/summary.json)
- [/Users/seongcheoljeong/Documents/Test/artifacts/first_run_object_sim/RUN_SAFE_001/trace.csv](/Users/seongcheoljeong/Documents/Test/artifacts/first_run_object_sim/RUN_SAFE_001/trace.csv)

### 2. Runtime readiness probe set

```bash
cd /Users/seongcheoljeong/Documents/Test
python3 /Users/seongcheoljeong/Documents/Test/scripts/run_scenario_runtime_backend_probe_set.py \
  --probe-set-id hybrid_runtime_readiness_v0 \
  --out-root /Users/seongcheoljeong/Documents/Test/artifacts/first_run_runtime_probe_set
```

Check:

- [/Users/seongcheoljeong/Documents/Test/artifacts/first_run_runtime_probe_set/scenario_runtime_backend_probe_set_report_v0.json](/Users/seongcheoljeong/Documents/Test/artifacts/first_run_runtime_probe_set/scenario_runtime_backend_probe_set_report_v0.json)
- [/Users/seongcheoljeong/Documents/Test/artifacts/first_run_runtime_probe_set/scenario_runtime_backend_probe_set_report_v0.md](/Users/seongcheoljeong/Documents/Test/artifacts/first_run_runtime_probe_set/scenario_runtime_backend_probe_set_report_v0.md)

This report is the fastest single runtime summary:

- AWSIM real readiness
- semantic primary vs semantic recovery
- CARLA local blocker state
- runtime strategy
- recommended next command

## Which Entry Point To Use

### Scenario logic

- [/Users/seongcheoljeong/Documents/Test/scripts/run_object_sim.py](/Users/seongcheoljeong/Documents/Test/scripts/run_object_sim.py)
- [/Users/seongcheoljeong/Documents/Test/scripts/run_log_replay.py](/Users/seongcheoljeong/Documents/Test/scripts/run_log_replay.py)

### Batch and validation

- [/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_variant_workflow.py](/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_variant_workflow.py)
- [/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_batch_workflow.py](/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_batch_workflow.py)

### Runtime and backend smoke

- [/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_backend_smoke_workflow.py](/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_backend_smoke_workflow.py)
- [/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_runtime_backend_workflow.py](/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_runtime_backend_workflow.py)
- [/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_closed_loop_demo.py](/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_closed_loop_demo.py)

## Continue On Linux Host

If you are moving the closed-loop lane onto a Linux GPU machine, start with:

- [/Users/seongcheoljeong/Documents/Test/docs/linux_closed_loop_handoff.md](/Users/seongcheoljeong/Documents/Test/docs/linux_closed_loop_handoff.md)

That runbook covers:

- clone and bootstrap on Linux
- helper-script templates
- first `--preflight-only` run
- control-plane closed-loop launch flow

### Existing runtime artifacts

- [/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_runtime_backend_rebridge.py](/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_runtime_backend_rebridge.py)
- [/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_runtime_backend_probe.py](/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_runtime_backend_probe.py)
- [/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_runtime_backend_probe_set.py](/Users/seongcheoljeong/Documents/Test/scripts/run_scenario_runtime_backend_probe_set.py)

### Autoware contract export

- [/Users/seongcheoljeong/Documents/Test/scripts/run_autoware_pipeline_bridge.py](/Users/seongcheoljeong/Documents/Test/scripts/run_autoware_pipeline_bridge.py)

## Current Runtime Reality

### AWSIM

- local packaged runtime exists
- this machine uses Linux handoff, not direct native macOS execution
- `tracking_fusion_v0` is real runtime-origin `READY`
- `semantic_perception_v0` can be `READY`
  - from primary semantic output
  - or through semantic supplemental recovery
- true closed-loop demo orchestration now exists, but it expects a separate Linux host with ROS2, Autoware, AWSIM, and capture tooling
- the Linux host continuation path now has checked-in helper templates and sample launch payloads under `examples/closed_loop/`

### CARLA

- no real local runtime parity yet
- current blocker is operational, not architectural
- the current repo already diagnoses:
  - download-space blockers
  - stage-space blockers
  - Docker storage corruption
  - recommended acquire/stage commands

## Best First Artifact To Inspect

If you inspect only one report first, inspect:

- [/Users/seongcheoljeong/Documents/Test/artifacts/scenario_runtime_backend_probe_set_real_awsim_v0/scenario_runtime_backend_probe_set_report_v0.json](/Users/seongcheoljeong/Documents/Test/artifacts/scenario_runtime_backend_probe_set_real_awsim_v0/scenario_runtime_backend_probe_set_report_v0.json)

Why:

- it shows the best current AWSIM state
- it shows semantic gap closure
- it shows CARLA blockers
- it gives the next action

## Publish Discipline

After any real change:

1. run tests
2. refresh provenance
3. run history guard
4. commit
5. push

Commands:

```bash
cd /Users/seongcheoljeong/Documents/Test
PYTHONPATH=src python3 -m unittest discover -s tests -q

python3 /Users/seongcheoljeong/Documents/Test/scripts/run_autonomy_e2e_history_refresh.py \
  --source-repo-root /Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E \
  --current-repo-root /Users/seongcheoljeong/Documents/Test \
  --output-root /Users/seongcheoljeong/Documents/Test/metadata/autonomy_e2e

python3 /Users/seongcheoljeong/Documents/Test/scripts/run_autonomy_e2e_history_guard.py \
  --metadata-root /Users/seongcheoljeong/Documents/Test/metadata/autonomy_e2e \
  --current-repo-root /Users/seongcheoljeong/Documents/Test \
  --compare-ref origin/main
```
