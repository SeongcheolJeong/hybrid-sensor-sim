# Autonomy-E2E Migration Master Plan

## Goal

Selectively migrate the parts of `Autonomy-E2E` that increase the current repository's usable feature coverage.

Current target repository:

- `/Users/seongcheoljeong/Documents/Test`

Historical source repository:

- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E`

This is a `selective migration` plan, not a repository merge plan.

## Current Execution Status

Implemented from the first migration wave:

1. `vehicle_dynamics_stub.py` migration
2. deterministic `scenario_definition_v0` object-sim baseline
3. `log_scene_v0` replay conversion
4. `log_scene_v0` augmentation helper
5. `logical_scenarios_v0` variant generation
6. object-sim matrix sweep runner

Current repository paths:

- `src/hybrid_sensor_sim/physics/vehicle_dynamics.py`
- `src/hybrid_sensor_sim/scenarios/schema.py`
- `src/hybrid_sensor_sim/scenarios/object_sim.py`
- `src/hybrid_sensor_sim/scenarios/log_scene.py`
- `src/hybrid_sensor_sim/scenarios/replay.py`
- `src/hybrid_sensor_sim/scenarios/variants.py`
- `src/hybrid_sensor_sim/scenarios/matrix_sweep.py`
- `src/hybrid_sensor_sim/tools/object_sim_runner.py`
- `src/hybrid_sensor_sim/tools/log_replay_runner.py`
- `src/hybrid_sensor_sim/tools/log_scene_augment.py`
- `src/hybrid_sensor_sim/tools/scenario_variants.py`
- `src/hybrid_sensor_sim/tools/scenario_matrix_sweep.py`

Still pending from this master plan:

1. rig sweep migration
2. map convert / validate / route layer

## Boundary

The current repository should stay focused on:

1. sensor simulation
2. runtime/backend execution
3. renderer/runtime interoperability
4. near-term object-sim functionality only when it directly unlocks scenario execution and sensor/runtime usefulness

It should `not` become a full copy of:

1. cloud batch infrastructure
2. analytics/data lake
3. release reporting
4. full external-stack CI orchestration

## Project Triage

| Source project | Migrate? | Why | Priority |
| --- | --- | --- | --- |
| `P_Sim-Engine` | Yes, selectively | strongest source for object-sim, vehicle dynamics, rig sweep, replay utilities | `P0` |
| `P_Map-Toolset-MVP` | Yes, selectively | useful for canonical lane/map graph once map-aware scenario/object sim is added | `P1` |
| `P_Validation-Tooling-MVP` | Yes, selectively | logical-to-concrete scenario variant generation is reusable; release reporting is not | `P1/P2` |
| `P_Cloud-Engine` | Partial, later | batch/matrix execution patterns become useful only after object-sim runner exists here | `P2` |
| `P_Data-Lake-and-Explorer` | No direct migration now | analytics/data-lake is outside current repository boundary | `Defer` |
| `P_E2E_Stack` | No direct migration now | orchestration/Make/CI stack is broader than this repository's purpose | `Defer` |
| `P_Autoware-Workspace-CI-MVP` | No direct migration now | HIL/workspace CI belongs in integration or infra repo, not this codebase | `Defer` |

## Migration Candidates By Project

## 1. P_Sim-Engine

Primary source:

- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/30_Projects/P_Sim-Engine/prototype`

Detailed audit:

- `docs/p_sim_engine_migration_audit.md`

### Migrate now

1. `vehicle_dynamics_stub.py`
   - move into a real current-repo module
   - preserve:
     - `vehicle_profile_v0`
     - `control_sequence_v0`
     - mass/drag/rolling resistance
     - road grade
     - surface friction
     - planar kinematics
     - dynamic bicycle mode
2. `sensor_rig_sweep.py`
   - rebuild on top of current:
     - camera/lidar/radar preview outputs
     - coverage metrics
     - trajectory sweep artifacts
3. `log_replay_runner.py`
   - now migrated as the current `log_scene_v0 -> scenario_definition_v0 -> object_sim` replay path
4. `core_sim_matrix_sweep_runner.py`
   - now migrated as the current library-first object-sim sweep runner

### Reference only

1. `sensor_sim_bridge.py`
   - current repo already exceeds this
2. `sim_runtime_adapter_stub.py`
   - current runtime stack is stronger
3. `prepare_runtime_assets.py`
   - current package acquire/stage/workflow stack already supersedes it

## 2. P_Map-Toolset-MVP

Primary source:

- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/30_Projects/P_Map-Toolset-MVP/prototype`

### Migrate later

1. `convert_map_format.py`
   - useful when current repo starts to consume map abstractions instead of raw runtime scene strings
2. `validate_canonical_map.py`
   - useful for map-ground-truth validation
3. `compute_canonical_route.py`
   - useful for route-aware scenario generation and lane-aware validation

### Preconditions

1. current repo needs a map abstraction layer first
2. object-sim/scenario tooling has to consume lane/map structure

## 3. P_Validation-Tooling-MVP

Primary source:

- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/30_Projects/P_Validation-Tooling-MVP/prototype`

### Migrate selectively

1. `generate_scenario_variants.py`
   - now migrated into the current repository's `logical_scenarios_v0` utility layer

### Do not migrate now

1. release markdown generation
2. gate profiles
3. requirement/report summaries

These are stack-level concerns, not core sim/runtime capabilities.

## 4. P_Cloud-Engine

Primary source:

- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E/30_Projects/P_Cloud-Engine/prototype`

### Migrate later

1. `cloud_batch_runner.py`
   - only after current repo owns:
     - object-sim runner
     - scenario sweep inputs
     - stable per-run summary schema
2. `generate_batch_from_catalog.py`
   - only after scenario catalog/schema exists here

### Do not migrate now

1. batch result/report plumbing
2. cloud/data lake integration assumptions

## 5. P_Data-Lake-and-Explorer

### Keep out of this repo for now

Useful ideas:

1. manifest normalization
2. query patterns

But not direct migration candidates for this repository's current scope.

## 6. P_E2E_Stack

### Use as reference, not migration source

Keep out of the codebase:

1. large Makefile orchestration
2. release pipeline glue
3. broad CI execution lanes

Use only as a planning reference for:

1. boundary management
2. progress tracking style
3. phase separation

## 7. P_Autoware-Workspace-CI-MVP

### Defer

This belongs to stack integration or a separate integration repo.
It is not a core library/tooling block for the current repository.

## Phased Migration Order

## Phase A: Object-Sim Foundation

Status:

- `P0`

Source:

- `P_Sim-Engine`

Work:

1. migrate `vehicle_dynamics_stub.py` into:
   - `src/hybrid_sensor_sim/physics/vehicle_dynamics.py`
   - `src/hybrid_sensor_sim/tools/vehicle_dynamics_trace.py`
2. define a current-repo scenario/object-sim direction around this module
3. add tests and a small runnable trace CLI

Success criteria:

1. validated vehicle profile + control sequence schemas
2. deterministic trace artifact
3. planar + dynamic bicycle modes
4. friction/grade effects preserved

## Phase B: Rig / Scenario Utility Layer

Status:

- `P1`

Source:

- `P_Sim-Engine`
- `P_Validation-Tooling-MVP`

Work:

1. add rig candidate sweep tool
2. connect rig evaluation to current camera/lidar/radar coverage outputs

Success criteria:

1. rig ranking works on current sensor outputs
2. native preview/coverage outputs can be compared without the old sensor bridge

## Phase C: Map Layer

Status:

- `P1/P2`

Source:

- `P_Map-Toolset-MVP`

Work:

1. add a minimal internal canonical lane/map representation
2. port map validation and route helper pieces

Success criteria:

1. map-aware scenario generation becomes possible
2. lane-aware object-sim work has a map substrate

## Phase D: Batch Sweep Layer

Status:

- `P2`

Source:

- `P_Cloud-Engine`
- `P_Sim-Engine`

Work:

1. extend the migrated matrix sweep runner when scenario/map layers become richer
2. standardize per-run summary contracts across scenario, sweep, and future rig evaluation

Success criteria:

1. scenario × dynamics × rig/runtime sweeps run without the old cloud prototype
2. outputs remain local and inspectable

## Phase E: Deferred Stack Integration

Status:

- `Later`

Source:

- `P_E2E_Stack`
- `P_Autoware-Workspace-CI-MVP`
- `P_Data-Lake-and-Explorer`

Work:

1. only revisit after core sim/runtime capability gaps are closed

## What To Avoid

Avoid these migration mistakes:

1. porting old runtime wrappers that are already weaker than the current runtime stack
2. pulling cloud/data/report/reporting layers into the core codebase
3. copying prototype folder structures directly instead of translating them into current architecture
4. mixing system-level orchestration with sim-runtime feature work

## Current Recommended Execution Order

1. implement `P_Sim-Engine` rig sweep on top of current coverage outputs
2. add map conversion/validation from `P_Map-Toolset-MVP`
3. deepen object-sim with optional `vehicle_dynamics` coupling after the baseline stays stable
4. only then extend batch/orchestration patterns from `P_Cloud-Engine`

## Immediate Next Action

The next concrete code migration should be:

1. start a dedicated `sensor_rig_sweep` migration from `P_Sim-Engine`
2. then migrate `P_Map-Toolset-MVP` convert/validate/route utilities

That order increases current feature coverage fastest while keeping repository scope under control.
