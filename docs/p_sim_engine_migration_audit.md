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

## Current Status

Implemented in the current repository:

1. `vehicle_dynamics_stub.py` -> `src/hybrid_sensor_sim/physics/vehicle_dynamics.py`
2. vehicle trace CLI -> `src/hybrid_sensor_sim/tools/vehicle_dynamics_trace.py`
3. `core_sim_runner.py` baseline -> `src/hybrid_sensor_sim/scenarios/schema.py`, `src/hybrid_sensor_sim/scenarios/object_sim.py`, `src/hybrid_sensor_sim/tools/object_sim_runner.py`
4. `log_replay_runner.py` baseline -> `src/hybrid_sensor_sim/scenarios/log_scene.py`, `src/hybrid_sensor_sim/scenarios/replay.py`, `src/hybrid_sensor_sim/tools/log_replay_runner.py`
5. `augment_log_scene.py` baseline -> `src/hybrid_sensor_sim/tools/log_scene_augment.py`
6. `generate_scenario_variants.py` baseline -> `src/hybrid_sensor_sim/scenarios/variants.py`, `src/hybrid_sensor_sim/tools/scenario_variants.py`
7. `core_sim_matrix_sweep_runner.py` baseline -> `src/hybrid_sensor_sim/scenarios/matrix_sweep.py`, `src/hybrid_sensor_sim/tools/scenario_matrix_sweep.py`
8. `sensor_rig_sweep.py` baseline -> `src/hybrid_sensor_sim/tools/sensor_rig_sweep.py`
9. second-wave `vehicle_dynamics` coupling into `src/hybrid_sensor_sim/scenarios/object_sim.py`
10. canonical map / route consumption into `src/hybrid_sensor_sim/scenarios/schema.py` and `src/hybrid_sensor_sim/scenarios/object_sim.py`
11. replay and matrix-sweep map/route propagation into `src/hybrid_sensor_sim/scenarios/replay.py` and `src/hybrid_sensor_sim/scenarios/matrix_sweep.py`
12. route-aware lane-risk semantics in `src/hybrid_sensor_sim/scenarios/object_sim.py`
13. route-driven `lane_index -> lane_id` inference and `lane_binding_mode` exposure in `src/hybrid_sensor_sim/scenarios/schema.py` and `src/hybrid_sensor_sim/scenarios/object_sim.py`
14. route-aware runtime path-conflict handling for collision avoidance and TTC in `src/hybrid_sensor_sim/scenarios/object_sim.py`
15. route-relation-driven lane synthesis in `src/hybrid_sensor_sim/scenarios/replay.py` and `src/hybrid_sensor_sim/scenarios/matrix_sweep.py`
16. route-relation-driven rendered payload generation in `src/hybrid_sensor_sim/scenarios/variants.py`
17. rendered variant execution in `src/hybrid_sensor_sim/tools/scenario_variant_runner.py`
   - `scenario_variants_report_v0` now drives replay/object-sim execution directly for `rendered_payload_kind=log_scene_v0` and `rendered_payload_kind=scenario_definition_v0`

Still pending from the same migration track:

1. deeper map-aware behavior beyond lane-id normalization, lane-binding inference, route synthesis, route-relation-driven scenario generation, rendered payload generation, rendered variant execution, route-aware runtime path-conflict handling, route summary wiring, and route-aware lane-risk aggregation

## Block Mapping

| P_Sim-Engine block | Historical evidence | Current repo equivalent | Assessment | Migration action |
| --- | --- | --- | --- | --- |
| `sensor_sim_bridge.py` | phase-2 sensor fidelity + quality summaries | `src/hybrid_sensor_sim/backends/native_physics.py`, `src/hybrid_sensor_sim/config.py`, `src/hybrid_sensor_sim/renderers/playback_contract.py` | Mostly superseded | Do not port file structure. Only mine summary field naming or small heuristic report ideas if they still help release reporting. |
| `sensor_rig_sweep.py` | `2cd5299` restored fidelity-tier rig sweep path | `src/hybrid_sensor_sim/tools/sensor_rig_sweep.py` | Implemented baseline | Keep the current `sensor_rig_sweep_v1` input and rank candidates using current native preview plus coverage outputs instead of reviving the old sensor bridge. |
| `vehicle_dynamics_stub.py` | `06ea545`, `276f197`, plus mass/grade/planar/dynamic bicycle history | No object-sim or vehicle-dynamics module in current repo | Real gap and highest-value carryover | Migrate vehicle profile schema, control sequence schema, and planar/dynamic bicycle core into a new current-repo module. |
| `log_replay_runner.py` | closed-loop log-scene -> scenario scaffold | `src/hybrid_sensor_sim/scenarios/replay.py`, `src/hybrid_sensor_sim/tools/log_replay_runner.py` | Implemented baseline | Keep the current `log_scene_v0 -> scenario_definition_v0 -> object_sim` path as the canonical replay surface. |
| `core_sim_matrix_sweep_runner.py` | `5eb472a` matrix sweep hook | `src/hybrid_sensor_sim/scenarios/matrix_sweep.py`, `src/hybrid_sensor_sim/tools/scenario_matrix_sweep.py` | Implemented baseline | Keep the current library-first matrix runner and add `vehicle_dynamics` coupling later rather than reviving the old subprocess architecture. |
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

### Already mined into current repo

- `prototype/sensor_rig_sweep.py`
  - rig candidate ranking/report structure translated into current `sensor_rig_sweep_v1`

- `prototype/core_sim_matrix_sweep_runner.py`
  - traffic parameter grid and report pattern
- `prototype/log_replay_runner.py`
  - log-scene to scenario conversion pattern
- `P_Validation-Tooling-MVP/prototype/generate_scenario_variants.py`
  - logical scenario expansion and deterministic random sampling

### Reference only

- `prototype/sim_runtime_adapter_stub.py`
- `prototype/prepare_runtime_assets.py`
- `prototype/sensor_sim_bridge.py`

## Current-Reco Gaps Confirmed In This Repo

The current repository does not yet have first-class equivalents for:

1. richer map-aware behavior beyond route-lane normalization, lane-binding inference, propagation, route-aware runtime path-conflict handling, and route-aware lane-risk aggregation

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

### 2. Deeper map-aware scenario behavior next

Source:

- `P_Map-Toolset-MVP/prototype/convert_map_format.py`
- `P_Map-Toolset-MVP/prototype/validate_canonical_map.py`
- `P_Map-Toolset-MVP/prototype/compute_canonical_route.py`

Current target:

- `src/hybrid_sensor_sim/maps/convert.py`
- `src/hybrid_sensor_sim/maps/validate.py`
- `src/hybrid_sensor_sim/maps/route.py`
- future object-sim/scenario consumers under `src/hybrid_sensor_sim/scenarios/`

## What Not To Repeat

Avoid spending time porting these old blocks:

1. old runtime launch-manifest scaffolds
2. old package/extract scripts
3. old stub sensor bridge architecture

The current repository already moved past those layers.
Porting them would add duplication, not capability.

## Next Concrete Work Item

The highest-value next migration from `Autonomy-E2E` is now:

1. deepen map-aware behavior on top of the new canonical-map-to-route normalization and propagation path
2. keep the current longitudinal vehicle-dynamics coupling stable while map-aware behavior is added
3. only then deepen map-aware scenario generation and validation

That order increases current feature coverage without destabilizing the newly migrated scenario and rig-sweep blocks.
