# Autonomy-E2E History Integration

## Purpose

This repository is the canonical implementation codebase:

- repo root: [/Users/seongcheoljeong/Documents/Test](/Users/seongcheoljeong/Documents/Test)
- GitHub: [SeongcheolJeong/hybrid-sensor-sim](https://github.com/SeongcheolJeong/hybrid-sensor-sim)

Historical source repository:

- `/Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E`

The goal is not to merge repositories.
The goal is to preserve traceable answers to:

1. which `Autonomy-E2E` blocks were migrated
2. which were superseded or deferred
3. which current files/tests/scripts/docs came from which historical source blocks

## Provenance Truth

Checked-in provenance truth lives under:

- [/Users/seongcheoljeong/Documents/Test/metadata/autonomy_e2e](/Users/seongcheoljeong/Documents/Test/metadata/autonomy_e2e)

Tracked files:

1. `project_inventory_v0.json`
2. `source_git_history_snapshot_v0.json`
3. `migration_registry_v0.json`
4. `result_traceability_index_v0.json`
5. `history_refresh_report_v0.json`

These files are the authoritative provenance layer.
Narrative docs explain them, but do not replace them.

## Integration Baseline

GitHub-era provenance rollout baseline commit:

- `8d2353f` `Reuse staged backend selections in scenario smoke workflows`

This is the baseline commit from which the checked-in provenance layer is managed.

## Block ID Rules

Traceability is maintained at capability-block level, not line-level blame.

Examples:

- `p_sim_engine.vehicle_dynamics`
- `p_sim_engine.object_sim_core`
- `p_sim_engine.log_replay`
- `p_map_toolset.canonical_route`
- `p_validation.scenario_variants`
- `p_cloud_engine.local_batch_pattern`
- `p_e2e_stack.runtime_evidence_compare_reference`
- `p_autoware_workspace_ci.data_contract_bridge`

Rules:

1. prefix with normalized project family
2. use stable capability names
3. one block should represent one migration/result decision

## Status Semantics

Every registry block must use one of:

1. `migrated`
2. `partial`
3. `superseded`
4. `reference_only`
5. `deferred`
6. `not_started`

Interpretation:

- `migrated`: current repo has real code/test/entry surface
- `partial`: current repo overlaps but does not fully close the historical block
- `superseded`: old block purpose exists, but current repo uses a stronger architecture
- `reference_only`: historical source is kept for behavioral/reference value only
- `deferred`: tracked but intentionally not implemented now
- `not_started`: known but still untouched

## Refresh Workflow

Refresh checked-in provenance metadata:

```bash
python3 /Users/seongcheoljeong/Documents/Test/scripts/run_autonomy_e2e_history_refresh.py \
  --source-repo-root /Users/seongcheoljeong/Documents/Autonomy-E2E/Autonomy-E2E \
  --current-repo-root /Users/seongcheoljeong/Documents/Test \
  --output-root /Users/seongcheoljeong/Documents/Test/metadata/autonomy_e2e \
  --recent-commit-limit 20
```

Build a report:

```bash
python3 /Users/seongcheoljeong/Documents/Test/scripts/run_autonomy_e2e_history_report.py \
  --metadata-root /Users/seongcheoljeong/Documents/Test/metadata/autonomy_e2e \
  --json-out /Users/seongcheoljeong/Documents/Test/artifacts/autonomy_e2e_history_report_v0.json \
  --markdown-out /Users/seongcheoljeong/Documents/Test/artifacts/autonomy_e2e_history_report_v0.md
```

Query by block or current path:

```bash
python3 /Users/seongcheoljeong/Documents/Test/scripts/run_autonomy_e2e_history_query.py \
  --metadata-root /Users/seongcheoljeong/Documents/Test/metadata/autonomy_e2e \
  --current-path src/hybrid_sensor_sim/physics/vehicle_dynamics.py
```

Guard migration changes before publishing:

```bash
python3 /Users/seongcheoljeong/Documents/Test/scripts/run_autonomy_e2e_history_guard.py \
  --metadata-root /Users/seongcheoljeong/Documents/Test/metadata/autonomy_e2e \
  --current-repo-root /Users/seongcheoljeong/Documents/Test \
  --compare-ref origin/main
```

## How To Add a New Migrated Block

Whenever a new migration/result block lands:

1. implement code
2. add tests
3. update narrative docs
4. update the provenance registry through the refresh path
5. confirm reverse traceability includes the new current paths

A block is not considered complete unless the provenance layer can answer:

- source block id
- source project
- current equivalent files
- tests
- fixtures
- scripts
- docs
- migration status

## How To Mark Superseded / Deferred / Reference Blocks

Use:

- `superseded` when the current repository has a stronger replacement and the old structure should not be revived
- `reference_only` when historical behavior or naming is useful but direct implementation is not the target
- `deferred` when the repository intentionally tracks the block but keeps it out of scope for now

Do not drop these blocks from inventory.
The point is to preserve explicit decisions, not just migrated code.

## Git Governance

Canonical branch policy:

1. `main` is the published baseline branch
2. new feature work should default to `codex/*` branches
3. push validated milestones instead of accumulating long dirty worktrees

Commit discipline:

1. code
2. tests
3. docs
4. provenance metadata, when migration state changes

Push discipline:

1. validate first
2. commit coherent blocks
3. push promptly

Guard discipline:

1. if changed paths under `src/`, `scripts/`, `tests/`, or `configs/` map to historical blocks, refresh `metadata/autonomy_e2e` before publish
2. if new governed paths are not in the reverse traceability index, treat that as a migration-governance failure

## Scope Reminder

This repository does not aim to become a full copy of:

1. cloud batch infrastructure
2. analytics/data lake
3. release-reporting stack
4. broad CI/orchestration stack

It keeps those projects in provenance inventory, but not as direct implementation targets unless explicitly promoted later.
