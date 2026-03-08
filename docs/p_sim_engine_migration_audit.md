# P_Sim-Engine Migration Audit

## Scope

This note audits historical work under:

- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/30_Projects/P_Sim-Engine`

and maps it onto the current codebase:

- `/Users/seongcheoljeong/Documents/Test`

The goal is not to preserve the old prototype as-is.
The goal is to decide which blocks should be migrated, which are already superseded, and which remain actual gaps.

## Evidence

Recent `P_Sim-Engine` commits touching `30_Projects/P_Sim-Engine`:

1. `2cd5299` `feat: restore sensor rig sweep fidelity-tier runtime path`
2. `93e1c93` `feat: expose phase2 sensor quality summaries in runtime artifacts`
3. `fefd2a9` `feat: wire phase2 sensor fidelity through release surfaces`
4. `5eb472a` `feat: add phase3 core sim matrix sweep hook`
5. `558335a` `feat: add interop export-import consistency checks`
6. `276f197` `Add friction-limited ego avoidance to core sim`
7. `06ea545` `Add tire-friction-limited dynamics behavior`
8. `117cddd` `feat: scaffold phase3 runtime adapter for awsim and carla`

Primary prototype files reviewed:

- `prototype/sensor_sim_bridge.py`
- `prototype/sensor_rig_sweep.py`
- `prototype/vehicle_dynamics_stub.py`
- `prototype/log_replay_runner.py`
- `prototype/core_sim_matrix_sweep_runner.py`
- `prototype/sim_runtime_adapter_stub.py`
- `prototype/prepare_runtime_assets.py`

## Main Finding

`P_Sim-Engine` and the current repository are not weak in the same places.

- `P_Sim-Engine` was stronger on:
  - object-sim style ego dynamics contracts
  - log replay conversion
  - scenario/batch sweep utilities
- The current repository is stronger on:
  - camera/lidar/radar parameter surface
  - ground truth and coverage outputs
  - runtime contracts, backend launchers, smoke inspection, package staging, and Linux handoff

So the correct migration strategy is:

1. `Do not` port old runtime scaffolds wholesale.
2. `Do` port object-sim and sweep utilities where the current repository still has no equivalent.
3. Use old files as behavioral references, not as target architecture.

## Block Mapping

| P_Sim-Engine block | Historical evidence | Current repo equivalent | Assessment | Migration action |
| --- | --- | --- | --- | --- |
| `sensor_sim_bridge.py` | phase-2 sensor fidelity + quality summaries | `src/hybrid_sensor_sim/backends/native_physics.py`, `src/hybrid_sensor_sim/config.py`, `src/hybrid_sensor_sim/renderers/playback_contract.py` | Mostly superseded | Do not port file structure. Only mine summary field naming or small heuristic report ideas if they still help release reporting. |
| `sensor_rig_sweep.py` | `2cd5299` restored fidelity-tier rig sweep path | No dedicated rig-candidate evaluator in current repo | Real gap | Add a rig sweep tool on top of current `coverage_targets`, trajectory sweep outputs, and runtime contracts. |
| `vehicle_dynamics_stub.py` | `06ea545`, `276f197`, plus mass/grade/planar/dynamic bicycle history | No object-sim or vehicle-dynamics module in current repo | Real gap and highest-value carryover | Migrate vehicle profile schema, control sequence schema, and planar/dynamic bicycle core into a new current-repo module. |
| `log_replay_runner.py` | closed-loop log-scene -> scenario scaffold | No current equivalent | Gap | Add a log/scenario conversion utility only after a canonical object-sim scenario schema is defined. |
| `core_sim_matrix_sweep_runner.py` | `5eb472a` matrix sweep hook | No current batch sweep runner | Gap | Add a batch sweep tool for scenario × dynamics × sensor/runtime settings once vehicle dynamics lands. |
| `sim_runtime_adapter_stub.py` | `117cddd` runtime adapter scaffold | `src/hybrid_sensor_sim/renderers/runtime_executor.py`, `src/hybrid_sensor_sim/renderers/backend_runner.py`, `src/hybrid_sensor_sim/tools/renderer_backend_workflow.py` | Strongly superseded | Do not migrate implementation. At most harvest a few naming conventions for stream ids or actor metadata. |
| `prepare_runtime_assets.py` | runtime asset download/extract/host checks | `src/hybrid_sensor_sim/tools/renderer_backend_package_acquire.py`, `renderer_backend_package_stage.py`, `renderer_backend_workflow.py`, local setup/handoff tools | Strongly superseded, current repo is better | Do not port. Current package/workflow stack already exceeds this block in inspectability and host-routing. |
| interop export/import reports | `558335a`, `1261070`, `b97cc2d` | Partial overlap only via runtime artifacts | Partial gap | Revisit later if OpenDRIVE/OpenSCENARIO roundtrip evidence becomes a concrete deliverable. Not P0. |

## Reusable Source Files

These old files are still worth mining directly:

### P0 candidates

- `prototype/vehicle_dynamics_stub.py`
  - vehicle profile validation
  - control sequence validation
  - mass/drag/rolling resistance inputs
  - road grade and surface friction controls
  - planar kinematics / dynamic bicycle switch

### P1 candidates

- `prototype/sensor_rig_sweep.py`
  - rig candidate schema
  - ranking/report structure
- `prototype/core_sim_matrix_sweep_runner.py`
  - matrix expansion/report pattern
- `prototype/log_replay_runner.py`
  - log-scene to scenario conversion pattern

### Reference only

- `prototype/sim_runtime_adapter_stub.py`
- `prototype/prepare_runtime_assets.py`
- `prototype/sensor_sim_bridge.py`

## Current-Reco Gaps Confirmed In This Repo

The current repository does not yet have first-class equivalents for:

1. vehicle profile + control sequence dynamics simulation
2. object-sim scenario replay and batch sweeps
3. dedicated rig-candidate sweep/ranking tool

This is consistent with the current source tree, which is centered on:

- `src/hybrid_sensor_sim/backends/`
- `src/hybrid_sensor_sim/renderers/`
- `src/hybrid_sensor_sim/tools/`

and not on an object-sim module tree.

## Recommended Migration Order

### 1. Vehicle dynamics first

Source:

- `P_Sim-Engine/prototype/vehicle_dynamics_stub.py`

Target:

- `src/hybrid_sensor_sim/physics/vehicle_dynamics.py`
- `src/hybrid_sensor_sim/tools/vehicle_dynamics_trace.py`
- supporting tests under `tests/`

Why first:

- This is the clearest functionality gap.
- It creates the basis for object-sim and replay work.
- It is more important than migrating any old runtime adapter scaffold.

### 2. Rig sweep second

Source:

- `P_Sim-Engine/prototype/sensor_rig_sweep.py`

Target:

- `src/hybrid_sensor_sim/tools/sensor_rig_sweep.py`

Expected adaptation:

- use current `coverage_metrics`
- use current camera/lidar/radar trajectory sweeps
- use current runtime smoke summaries as optional scoring inputs

### 3. Batch sweep third

Source:

- `P_Sim-Engine/prototype/core_sim_matrix_sweep_runner.py`

Target:

- `src/hybrid_sensor_sim/tools/scenario_matrix_sweep.py`

Expected adaptation:

- do not preserve old `core_sim_runner.py` contract
- rebuild around current config/options plus future vehicle dynamics module

### 4. Log replay after schema stabilization

Source:

- `P_Sim-Engine/prototype/log_replay_runner.py`

Target:

- deferred until a current-repo scenario schema exists

## What Not To Repeat

Avoid spending time porting these old blocks:

1. old runtime launch-manifest scaffolds
2. old package/extract scripts
3. old stub sensor bridge architecture

The current repository already moved past those layers.
Porting them would add duplication, not capability.

## Next Concrete Work Item

The highest-value next migration from `P_Sim-Engine` is:

1. extract the validated vehicle profile / control sequence / friction / dynamic bicycle logic
2. place it into a current-repo `vehicle_dynamics` module
3. add a small trace tool + tests

That is the first migration that increases current feature coverage instead of re-implementing already superseded runtime scaffolding.
