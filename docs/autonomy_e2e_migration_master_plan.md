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
7. sensor rig sweep on current native preview/coverage outputs
8. map convert / validate / route layer
9. rendered variant execution runner for `scenario_variants_report_v0`
10. scenario variant workflow for `logical_scenarios_v0 -> rendered payload execution`
11. scenario batch comparison surface for `scenario_variant_workflow_report_v0 -> core_sim_matrix_sweep_report_v0`
12. scenario batch workflow for `logical_scenarios_v0 -> rendered payload execution -> matrix sweep -> batch comparison`
13. threshold-based batch gate for TTC/collision/timeout policy
14. reusable batch gate profiles plus richer workflow Markdown tables for logical-scenario and matrix-group triage
15. gate preset selection by profile id plus workflow Markdown variant triage tables
16. logical-scenario health summaries for batch workflow triage
17. per-scenario gate evaluation inside logical-scenario health summaries
18. merge/diverge/lane-change interaction tagging inside route-aware object sim
19. propagation of route interaction summaries into batch comparison and workflow reporting
20. route-interaction-aware batch gate thresholds and attention reasons
21. per-scenario route-interaction gate failures in logical-scenario health reporting
22. compact failing logical-scenario triage and attention-reason aggregation in batch reports
23. top-level workflow status summaries for failing and attention logical scenarios
24. explicit workflow decision traces for batch status selection
25. matrix-group triage surfaced in top-level batch status summaries
26. top-level batch gate breach summaries by metric and matrix group
27. compact worst-case logical-scenario and matrix-group rows in top-level batch summaries

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
- `src/hybrid_sensor_sim/tools/scenario_batch_comparison.py`
- `src/hybrid_sensor_sim/tools/scenario_batch_gate_catalog.py`
- `src/hybrid_sensor_sim/tools/scenario_batch_workflow.py`
- `src/hybrid_sensor_sim/tools/sensor_rig_sweep.py`
- `src/hybrid_sensor_sim/maps/convert.py`
- `src/hybrid_sensor_sim/maps/validate.py`
- `src/hybrid_sensor_sim/maps/route.py`

Still pending from this master plan:

1. deeper map-aware behavior inside scenario/object-sim flows

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
   - now migrated on top of current:
     - camera/lidar/radar preview outputs
     - coverage metrics
     - native point/actor annotations
3. `log_replay_runner.py`
   - now migrated as the current `log_scene_v0 -> scenario_definition_v0 -> object_sim` replay path
4. `core_sim_matrix_sweep_runner.py`
   - now migrated as the current library-first object-sim sweep runner
5. second-wave object-sim ego `vehicle_dynamics` coupling
   - now migrated as optional `ego_dynamics_mode=vehicle_dynamics` longitudinal coupling
6. canonical map route consumption inside `scenario_definition_v0`
   - now migrated as optional `canonical_map/canonical_map_path + route_definition + actor lane_id`
7. replay and matrix-sweep route propagation
   - `log_scene_v0` can synthesize and forward canonical routes
   - matrix sweep now preserves canonical map, route definition, and actor `lane_id` in each case scenario
8. route-aware lane-risk semantics
   - `lane_risk_summary_v0` now carries `same/downstream/upstream/off_route` route relation counts and route-aware TTC/gap summaries
9. route-driven lane binding inference
   - route-backed scenarios now infer `lane_id` from `lane_index` when possible and expose `*_lane_binding_mode` in summary and trace outputs
10. route-aware runtime path-conflict handling
   - object-sim now uses route semantics directly for path-conflict TTC and ego avoidance, not only for post-run reporting
11. route-aware avoidance target introspection
   - object-sim now exposes the chosen avoidance target actor and interaction kind in both summary and trace outputs
12. interaction-specific avoidance policy
   - object-sim now supports per-interaction TTC threshold and brake-scale overrides for route-aware avoidance decisions
13. avoidance-aware batch reporting
   - scenario batch comparison and workflow now propagate object-sim avoidance trigger counts, last-trigger interaction kinds, and grouped avoidance counters into overview, logical-scenario, matrix-group, attention, and top-level status-summary surfaces
14. avoidance-aware gate policy
   - batch gate policy now supports direct thresholds for avoidance activity and interaction-specific trigger counts, and the same failure codes are reused in logical-scenario health and matrix-group triage
15. full-stack avoidance gate reuse
   - avoidance thresholds now flow consistently through comparison gate evaluation, logical-scenario health, matrix-group triage, and top-level batch status summaries
16. richer interaction-specific avoidance selection
   - object-sim avoidance policy now supports interaction-specific `priority` and `max_gap_m`, making target selection and actionable filtering more controllable without coupling to vehicle dynamics
17. avoidance-aware worst-case batch triage
   - batch workflow worst-case logical-scenario and matrix-group ranking now considers avoidance brake-event totals and interaction-specific trigger mix before TTC-only tie-breaks, then uses preserved avoidance policy traces (`priority` and `max_gap_m`) as the next tie-break layer so avoidance-heavy route interactions remain visible and deterministic in top-level triage
18. avoidance-focused gate preset catalog
   - the batch gate preset catalog now includes an avoidance-specific profile for route-interaction-heavy scenarios, and both comparison/workflow CLIs are covered for preset-id resolution
19. avoidance policy trace surfaced in batch triage
   - batch comparison and workflow worst-case rows now preserve interaction-policy traces such as selected avoidance `priority` and `max_gap_m`, so top-level triage can explain not only that braking happened but under which route-interaction policy it happened
20. interaction-specific avoidance gate presets
   - the batch gate preset catalog now includes dedicated merge-conflict and downstream-route avoidance profiles so route-interaction-heavy scenarios can be gated without turning on the broader avoidance-wide preset
21. explicit route-lane surface for lane-change conflicts
   - scenario actors can now carry `route_lane_id` separately from their current `lane_id`, replay can emit `ego_route_lane_id` / `lead_vehicle_route_lane_id`, and matrix patterns can synthesize `traffic_npc_route_lane_profile`, which together open direct `lane_change_conflict` generation without rewriting the current-lane surface
22. lane-change avoidance preset and triage propagation
   - the gate preset catalog now includes `scenario_batch_gate_avoidance_lane_change_v0` for explicit lane-change-trigger gating
   - batch comparison/workflow triage now preserves route-lane traces through grouped rows, attention rows, and logical-scenario health rows so explicit lane-change evidence survives into top-level batch summaries
23. lane-change status-summary escalation
   - batch attention and health reasons now emit `LANE_CHANGE_ROUTE_LANE_TRACE_PRESENT` when a lane-change conflict is backed by explicit route-lane traces
   - top-level workflow triage now surfaces lane-change-specific logical-scenario IDs, matrix-group IDs, and gate-failure-code counts directly in `status_summary`
24. lane-change brake floors and stronger worst-case prioritization
   - interaction-specific avoidance policy now supports `min_brake_scale` in addition to `ttc_threshold_sec`, `brake_scale`, `priority`, and `max_gap_m`, which lets `lane_change_conflict` scenarios keep a minimum braking floor without over-tightening the full policy
   - top-level worst-case ranking now treats lane-change gate breaches as an explicit severity layer, so lane-change-heavy failures rank ahead of lighter route-interaction cases even when avoidance counts otherwise tie
25. hold-aware lane-change avoidance
   - interaction-specific avoidance policy now also supports `hold_duration_sec`, allowing `lane_change_conflict` braking to persist briefly after the target stops being immediately actionable
   - object-sim summary and trace now expose hold-event counts, hold-active step counts, hold counts by interaction kind, and selected hold duration for direct triage
26. hold-aware batch ranking and summaries
   - batch comparison overview, grouped rows, and workflow status summaries now preserve hold metrics and selected `hold_duration_sec` values
   - worst-case ranking now treats hold-active steps and hold events as stronger evidence than raw brake totals before applying policy-trace tie-breaks
11. route-relation-driven replay and sweep generation
   - `log_scene_v0` and matrix actor patterns can now synthesize actor lane assignments from route relations instead of relying only on explicit lane IDs or raw lane slots
12. rendered payload generation in scenario variants
   - `logical_scenarios_v0` can now emit rendered concrete payloads, including route-relation-driven `log_scene_v0` variants
13. rendered variant execution
   - `scenario_variants_report_v0` can now drive replay/object-sim execution directly through `src/hybrid_sensor_sim/tools/scenario_variant_runner.py`
   - supported payload kinds now include `log_scene_v0` and `scenario_definition_v0`
   - runner output now includes compact successful and non-success triage rows
14. scenario variant workflow
   - `src/hybrid_sensor_sim/tools/scenario_variant_workflow.py` now provides a single workflow entry point from logical scenarios to executed rendered payloads
   - workflow output now includes payload-kind grouping, logical-scenario grouping, plus compact successful and failed/skipped triage rows
   - scenario-language profile execution now resolves through the repo-local `tests/fixtures/autonomy_e2e/p_validation` directory
   - random sampling is now validated for mixed-payload scenario-language profiles

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
   - now migrated as `simple_map_v0 <-> canonical_lane_graph_v0` conversion helpers
2. `validate_canonical_map.py`
   - now migrated as canonical lane graph semantic validation
3. `compute_canonical_route.py`
   - now migrated as standalone route computation for `hops` and `length`

### Current status

1. standalone map utilities are now present
2. object-sim/scenario tooling now consumes the canonical map layer for route-based lane normalization and summary wiring

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

- `Done`

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
5. object-sim can optionally use longitudinal `vehicle_dynamics` coupling without breaking the legacy kinematic path

## Phase B: Rig / Scenario Utility Layer

Status:

- `Done`

Source:

- `P_Sim-Engine`
- `P_Validation-Tooling-MVP`

Work:

1. keep extending the migrated rig sweep tool as sensor outputs grow richer
2. connect rig evaluation to current camera/lidar/radar coverage outputs

## Phase C: Map-Aware Scenario Consumption

Status:

- `Done`

Source:

- `P_Map-Toolset-MVP`

Work:

1. connect canonical lane graph inputs to scenario/object-sim
2. decide the minimum map-aware behavior surface that does not destabilize the current deterministic runner
3. preserve the current standalone map utilities while adding scenario consumers on top

Success criteria:

1. `scenario_definition_v0` can reference `canonical_map_path` or embed `canonical_map`
2. route-based lane-id normalization works without changing the legacy `lane_index` runtime surface
3. object-sim summary and trace expose map/route consumption results

## Phase D: Deeper Map-Aware Behavior

Status:

- `Next`

Work:

1. consume canonical map semantics for richer runtime behavior than current route-order normalization and route-aware lane-risk aggregation
2. decide whether replay or matrix sweep should infer more lane semantics than current route/lane-id propagation

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

1. deepen object-sim with optional `vehicle_dynamics` coupling after the baseline stays stable
2. connect scenario/object-sim tooling to the new canonical map layer
3. only then extend batch/orchestration patterns from `P_Cloud-Engine`

## Immediate Next Action

The next concrete code migration should be:

1. deepen route-aware runtime behavior on top of the new hold-aware avoidance surface
2. connect scenario batch outputs to runtime/backend smoke so scenario evaluation and backend execution share one end-to-end path

That order increases current feature coverage fastest while keeping repository scope under control.
