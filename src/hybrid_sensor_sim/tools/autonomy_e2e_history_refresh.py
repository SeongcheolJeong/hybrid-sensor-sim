from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.io.autonomy_e2e_provenance import (
    AUTONOMY_E2E_GIT_HISTORY_SNAPSHOT_SCHEMA_VERSION_V0,
    AUTONOMY_E2E_HISTORY_REFRESH_REPORT_SCHEMA_VERSION_V0,
    AUTONOMY_E2E_MIGRATION_REGISTRY_SCHEMA_VERSION_V0,
    AUTONOMY_E2E_PROJECT_INVENTORY_SCHEMA_VERSION_V0,
    build_reverse_traceability_index,
)


INTEGRATION_BASELINE_COMMIT = "8d2353f"
THIRTY_PROJECTS_DIR_NAME = "30_Projects"
DEFAULT_RECENT_COMMIT_LIMIT = 20

PROJECT_SPECS = {
    "P_Sim-Engine": {
        "project_id": "P_Sim-Engine",
        "project_category": "sim",
        "migration_scope": "selected_source",
        "current_assessment": (
            "Primary migration source for object sim, replay, rig sweep, route-aware "
            "batch evaluation, and scenario-to-runtime smoke bridging."
        ),
    },
    "P_Map-Toolset-MVP": {
        "project_id": "P_Map-Toolset-MVP",
        "project_category": "map",
        "migration_scope": "selected_source",
        "current_assessment": (
            "Selected map utility source for canonical conversion, validation, and route "
            "computation."
        ),
    },
    "P_Validation-Tooling-MVP": {
        "project_id": "P_Validation-Tooling-MVP",
        "project_category": "validation",
        "migration_scope": "selected_source",
        "current_assessment": (
            "Selected scenario-variation source; release-reporting pieces remain out of "
            "scope."
        ),
    },
    "P_Cloud-Engine": {
        "project_id": "P_Cloud-Engine",
        "project_category": "cloud",
        "migration_scope": "reference_only",
        "current_assessment": (
            "Reference for local batch execution patterns only; cloud-specific orchestration "
            "is not migrated."
        ),
    },
    "P_Data-Lake-and-Explorer": {
        "project_id": "P_Data-Lake-and-Explorer",
        "project_category": "data",
        "migration_scope": "excluded",
        "current_assessment": (
            "Tracked for provenance only; analytics and data-lake workflow are outside this "
            "repository boundary."
        ),
    },
    "P_E2E_Stack": {
        "project_id": "P_E2E_Stack",
        "project_category": "stack",
        "migration_scope": "reference_only",
        "current_assessment": (
            "Reference for orchestration and parity planning; the current repository does "
            "not migrate the stack directly."
        ),
    },
    "P_Autoware-Workspace-CI-MVP": {
        "project_id": "P_Autoware-Workspace-CI-MVP",
        "project_category": "ci",
        "migration_scope": "reference_only",
        "current_assessment": (
            "Reference for integration/HIL CI ideas only; no direct migration into this "
            "repository."
        ),
    },
}

IGNORED_PROTOTYPE_FILENAMES = {
    ".DS_Store",
    ".gitignore",
    "README.md",
    "ci_error_summary.py",
    "generate_release_report.py",
}
IGNORED_PROTOTYPE_DIRNAMES = {
    "__pycache__",
    "runtime_assets",
    "batch_runs",
    "runs",
}

BLOCK_CATALOG: list[dict[str, Any]] = [
    {
        "block_id": "p_sim_engine.vehicle_dynamics",
        "project_id": "P_Sim-Engine",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Sim-Engine/prototype/vehicle_dynamics_stub.py",
        ],
        "migration_status": "migrated",
        "current_paths": [
            "src/hybrid_sensor_sim/physics/vehicle_dynamics.py",
        ],
        "current_test_paths": [
            "tests/test_vehicle_dynamics.py",
        ],
        "current_fixture_paths": [
            "tests/fixtures/autonomy_e2e/p_sim_engine/vehicle_profile_v0.json",
            "tests/fixtures/autonomy_e2e/p_sim_engine/control_sequence_v0.json",
        ],
        "current_script_paths": [
            "scripts/run_vehicle_dynamics_trace.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/p_sim_engine_migration_audit.md",
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["library", "cli", "test", "fixture", "doc"],
        "result_summary": (
            "Migrated vehicle profile/control sequence validation plus planar and dynamic "
            "bicycle trace execution."
        ),
        "open_gaps": [],
        "notes": "Canonical current equivalent for the old vehicle dynamics stub.",
    },
    {
        "block_id": "p_sim_engine.object_sim_core",
        "project_id": "P_Sim-Engine",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Sim-Engine/prototype/core_sim_runner.py",
        ],
        "migration_status": "migrated",
        "current_paths": [
            "src/hybrid_sensor_sim/scenarios/schema.py",
            "src/hybrid_sensor_sim/scenarios/object_sim.py",
        ],
        "current_test_paths": [
            "tests/test_object_sim.py",
        ],
        "current_fixture_paths": [
            "tests/fixtures/autonomy_e2e/p_sim_engine/highway_following_v0.json",
            "tests/fixtures/autonomy_e2e/p_sim_engine/highway_safe_following_v0.json",
            "tests/fixtures/autonomy_e2e/p_sim_engine/highway_safe_following_vehicle_dynamics_v0.json",
            "tests/fixtures/autonomy_e2e/p_sim_engine/highway_map_route_following_v0.json",
        ],
        "current_script_paths": [
            "scripts/run_object_sim.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/p_sim_engine_migration_audit.md",
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["library", "cli", "test", "fixture", "doc"],
        "result_summary": (
            "Deterministic object-sim baseline with route-aware avoidance, lane-change "
            "interaction semantics, and map-aware risk outputs."
        ),
        "open_gaps": [
            "Deeper map-aware runtime behavior beyond current route-interaction tagging."
        ],
        "notes": "Current scenario_definition_v0 baseline is the canonical object-sim result.",
    },
    {
        "block_id": "p_sim_engine.log_replay",
        "project_id": "P_Sim-Engine",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Sim-Engine/prototype/log_replay_runner.py",
        ],
        "migration_status": "migrated",
        "current_paths": [
            "src/hybrid_sensor_sim/scenarios/log_scene.py",
            "src/hybrid_sensor_sim/scenarios/replay.py",
        ],
        "current_test_paths": [
            "tests/test_log_replay.py",
        ],
        "current_fixture_paths": [
            "tests/fixtures/autonomy_e2e/p_sim_engine/log_scene_v0.json",
            "tests/fixtures/autonomy_e2e/p_sim_engine/log_scene_map_route_v0.json",
            "tests/fixtures/autonomy_e2e/p_sim_engine/log_scene_map_route_relations_v0.json",
            "tests/fixtures/autonomy_e2e/p_sim_engine/log_scene_map_lane_change_conflict_v0.json",
        ],
        "current_script_paths": [
            "scripts/run_log_replay.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/p_sim_engine_migration_audit.md",
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["library", "cli", "test", "fixture", "doc"],
        "result_summary": "Log-scene to scenario replay with route-lane propagation.",
        "open_gaps": [],
        "notes": "Replay path now serves both direct object-sim and scenario smoke bridging.",
    },
    {
        "block_id": "p_sim_engine.log_scene_augment",
        "project_id": "P_Sim-Engine",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Sim-Engine/prototype/augment_log_scene.py",
        ],
        "migration_status": "migrated",
        "current_paths": [],
        "current_test_paths": [
            "tests/test_log_replay.py",
        ],
        "current_fixture_paths": [
            "tests/fixtures/autonomy_e2e/p_sim_engine/log_scene_v0.json",
        ],
        "current_script_paths": [
            "scripts/run_log_scene_augment.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/p_sim_engine_migration_audit.md",
        ],
        "working_result_kind": ["cli", "test", "fixture", "doc"],
        "result_summary": "Pure log-scene augmentation helper exposed through current CLI.",
        "open_gaps": [],
        "notes": "Augmentation behavior is preserved through tool surface rather than a standalone scenario module.",
    },
    {
        "block_id": "p_sim_engine.matrix_sweep",
        "project_id": "P_Sim-Engine",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Sim-Engine/prototype/core_sim_matrix_sweep_runner.py",
        ],
        "migration_status": "migrated",
        "current_paths": [
            "src/hybrid_sensor_sim/scenarios/matrix_sweep.py",
        ],
        "current_test_paths": [
            "tests/test_scenario_matrix_sweep.py",
        ],
        "current_fixture_paths": [
            "tests/fixtures/autonomy_e2e/p_sim_engine/highway_safe_following_v0.json",
        ],
        "current_script_paths": [
            "scripts/run_scenario_matrix_sweep.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/p_sim_engine_migration_audit.md",
        ],
        "working_result_kind": ["library", "cli", "test", "fixture", "doc"],
        "result_summary": "Library-first matrix sweep runner with route and avoidance propagation.",
        "open_gaps": [],
        "notes": "Current matrix sweep is the canonical replacement for the subprocess-heavy prototype.",
    },
    {
        "block_id": "p_sim_engine.sensor_rig_sweep",
        "project_id": "P_Sim-Engine",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Sim-Engine/prototype/sensor_rig_sweep.py",
        ],
        "migration_status": "migrated",
        "current_paths": [],
        "current_test_paths": [
            "tests/test_sensor_rig_sweep.py",
        ],
        "current_fixture_paths": [
            "tests/fixtures/autonomy_e2e/p_sim_engine/rig_sweep_base_config.json",
            "tests/fixtures/autonomy_e2e/p_sim_engine/rig_sweep_candidates_v1.json",
        ],
        "current_script_paths": [
            "scripts/run_sensor_rig_sweep.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/p_sim_engine_migration_audit.md",
        ],
        "working_result_kind": ["cli", "test", "fixture", "doc"],
        "result_summary": "Rig sweep translated onto current native preview and coverage outputs.",
        "open_gaps": [],
        "notes": "Old world-state bridge was intentionally not revived.",
    },
    {
        "block_id": "p_sim_engine.route_aware_semantics",
        "project_id": "P_Sim-Engine",
        "source_kind": "workflow_pattern",
        "source_paths": [
            "30_Projects/P_Sim-Engine/prototype/core_sim_runner.py",
            "30_Projects/P_Sim-Engine/prototype/log_replay_runner.py",
            "30_Projects/P_Sim-Engine/prototype/core_sim_matrix_sweep_runner.py",
        ],
        "migration_status": "migrated",
        "current_paths": [
            "src/hybrid_sensor_sim/scenarios/schema.py",
            "src/hybrid_sensor_sim/scenarios/object_sim.py",
            "src/hybrid_sensor_sim/scenarios/replay.py",
            "src/hybrid_sensor_sim/scenarios/matrix_sweep.py",
        ],
        "current_test_paths": [
            "tests/test_object_sim.py",
            "tests/test_log_replay.py",
            "tests/test_scenario_matrix_sweep.py",
        ],
        "current_fixture_paths": [
            "tests/fixtures/autonomy_e2e/p_sim_engine/highway_map_route_following_v0.json",
            "tests/fixtures/autonomy_e2e/p_sim_engine/log_scene_map_route_relations_v0.json",
            "tests/fixtures/autonomy_e2e/p_sim_engine/log_scene_map_lane_change_conflict_v0.json",
        ],
        "current_script_paths": [],
        "current_doc_paths": [
            "README.md",
            "docs/p_sim_engine_migration_audit.md",
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["library", "test", "fixture", "doc"],
        "result_summary": "Map/route semantics, route-lane binding, and lane-change route-lane surfaces integrated into scenario flows.",
        "open_gaps": [
            "Deeper runtime semantics for merge/diverge decisions remain open."
        ],
        "notes": "This is a current-repo expansion beyond the original prototype scope.",
    },
    {
        "block_id": "p_sim_engine.route_aware_avoidance",
        "project_id": "P_Sim-Engine",
        "source_kind": "workflow_pattern",
        "source_paths": [
            "30_Projects/P_Sim-Engine/prototype/core_sim_runner.py",
            "30_Projects/P_Sim-Engine/prototype/vehicle_dynamics_stub.py",
        ],
        "migration_status": "migrated",
        "current_paths": [
            "src/hybrid_sensor_sim/scenarios/object_sim.py",
        ],
        "current_test_paths": [
            "tests/test_object_sim.py",
        ],
        "current_fixture_paths": [
            "tests/fixtures/autonomy_e2e/p_sim_engine/highway_map_route_following_v0.json",
            "tests/fixtures/autonomy_e2e/p_sim_engine/log_scene_map_lane_change_conflict_v0.json",
        ],
        "current_script_paths": [
            "scripts/run_object_sim.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/p_sim_engine_migration_audit.md",
        ],
        "working_result_kind": ["library", "cli", "test", "fixture", "doc"],
        "result_summary": (
            "Interaction-specific avoidance with priority, gap, brake-floor, and hold "
            "policies plus route-aware target tracing."
        ),
        "open_gaps": [
            "Further merge/diverge runtime decisions are still pending."
        ],
        "notes": "Current avoidance logic is stricter and more inspectable than the old core sim behavior.",
    },
    {
        "block_id": "p_sim_engine.batch_workflow",
        "project_id": "P_Sim-Engine",
        "source_kind": "workflow_pattern",
        "source_paths": [
            "30_Projects/P_Sim-Engine/prototype/core_sim_matrix_sweep_runner.py",
            "30_Projects/P_Sim-Engine/prototype/core_sim_runner.py",
        ],
        "migration_status": "migrated",
        "current_paths": [],
        "current_test_paths": [
            "tests/test_scenario_batch_comparison.py",
            "tests/test_scenario_batch_workflow.py",
        ],
        "current_fixture_paths": [
            "tests/fixtures/autonomy_e2e/p_validation/scenario_batch_gate_strict_v0.json",
            "tests/fixtures/autonomy_e2e/p_validation/scenario_batch_gate_avoidance_v0.json",
            "tests/fixtures/autonomy_e2e/p_validation/scenario_batch_gate_avoidance_merge_v0.json",
            "tests/fixtures/autonomy_e2e/p_validation/scenario_batch_gate_avoidance_downstream_route_v0.json",
            "tests/fixtures/autonomy_e2e/p_validation/scenario_batch_gate_avoidance_lane_change_v0.json",
        ],
        "current_script_paths": [
            "scripts/run_scenario_batch_comparison.py",
            "scripts/run_scenario_batch_workflow.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/p_sim_engine_migration_audit.md",
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["workflow", "cli", "test", "fixture", "doc"],
        "result_summary": "Current batch evaluation, triage, gate preset, and workflow surface.",
        "open_gaps": [
            "Real backend smoke still needs more direct package-based validation."
        ],
        "notes": "Batch workflow is a current-repo synthesis rather than a direct file port.",
    },
    {
        "block_id": "p_sim_engine.scenario_runtime_smoke_bridge",
        "project_id": "P_Sim-Engine",
        "source_kind": "workflow_pattern",
        "source_paths": [
            "30_Projects/P_Sim-Engine/prototype/sim_runtime_scenario_contract_runner.py",
            "30_Projects/P_Sim-Engine/prototype/sim_runtime_scene_result_runner.py",
            "30_Projects/P_Sim-Engine/prototype/sim_runtime_probe_runner.py",
        ],
        "migration_status": "migrated",
        "current_paths": [
            "src/hybrid_sensor_sim/tools/scenario_runtime_bridge.py",
            "src/hybrid_sensor_sim/tools/scenario_backend_smoke_workflow.py",
            "src/hybrid_sensor_sim/tools/scenario_runtime_backend_workflow.py",
            "src/hybrid_sensor_sim/tools/scenario_runtime_backend_rebridge.py",
        ],
        "current_test_paths": [
            "tests/test_scenario_backend_smoke_workflow.py",
            "tests/test_scenario_runtime_backend_workflow.py",
            "tests/test_scenario_runtime_backend_rebridge.py",
        ],
        "current_fixture_paths": [],
        "current_script_paths": [
            "scripts/run_scenario_backend_smoke_workflow.py",
            "scripts/run_scenario_runtime_backend_workflow.py",
            "scripts/run_scenario_runtime_backend_rebridge.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/p_sim_engine_migration_audit.md",
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["workflow", "cli", "test", "doc"],
        "result_summary": (
            "Scenario batch outputs can now bridge into runtime smoke and reuse staged "
            "backend selections, and previously generated runtime/backend smoke reports can "
            "be re-bridged into refreshed Autoware and top-level workflow artifacts."
        ),
        "open_gaps": [
            "Real packaged AWSIM/CARLA execution still remains to be closed."
        ],
        "notes": "This block captures the scenario-to-runtime integration path.",
    },
    {
        "block_id": "p_sim_engine.sensor_sim_bridge",
        "project_id": "P_Sim-Engine",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Sim-Engine/prototype/sensor_sim_bridge.py",
        ],
        "migration_status": "superseded",
        "current_paths": [
            "src/hybrid_sensor_sim/orchestrator.py",
            "src/hybrid_sensor_sim/backends/native_physics.py",
            "src/hybrid_sensor_sim/renderers/playback_contract.py",
        ],
        "current_test_paths": [
            "tests/test_camera_physics.py",
            "tests/test_sensor_config.py",
        ],
        "current_fixture_paths": [],
        "current_script_paths": [],
        "current_doc_paths": [
            "docs/p_sim_engine_migration_audit.md",
            "docs/sensor_sim_master_plan.md",
        ],
        "working_result_kind": ["library", "test", "doc"],
        "result_summary": "Superseded by stronger native-physics and playback-contract surfaces.",
        "open_gaps": [],
        "notes": "Old bridge structure is intentionally not revived.",
    },
    {
        "block_id": "p_sim_engine.runtime_asset_prepare",
        "project_id": "P_Sim-Engine",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Sim-Engine/prototype/prepare_runtime_assets.py",
            "30_Projects/P_Sim-Engine/prototype/sim_runtime_adapter_stub.py",
        ],
        "migration_status": "superseded",
        "current_paths": [
            "src/hybrid_sensor_sim/tools/renderer_backend_linux_handoff_docker.py",
            "src/hybrid_sensor_sim/tools/renderer_backend_package_acquire.py",
            "src/hybrid_sensor_sim/tools/renderer_backend_package_stage.py",
            "src/hybrid_sensor_sim/tools/renderer_backend_workflow.py",
        ],
        "current_test_paths": [
            "tests/test_renderer_backend_linux_handoff_docker.py",
            "tests/test_renderer_backend_package_acquire.py",
            "tests/test_renderer_backend_package_stage.py",
            "tests/test_renderer_backend_workflow.py",
        ],
        "current_fixture_paths": [],
        "current_script_paths": [
            "scripts/acquire_renderer_backend_package.py",
            "scripts/stage_renderer_backend_package.py",
            "scripts/run_renderer_backend_workflow.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/p_sim_engine_migration_audit.md",
        ],
        "working_result_kind": ["workflow", "cli", "test", "doc"],
        "result_summary": "Superseded by inspectable package acquire/stage/workflow stack.",
        "open_gaps": [],
        "notes": "Current workflow exceeds the old runtime asset scaffolding.",
    },
    {
        "block_id": "p_sim_engine.neural_sensor_reference",
        "project_id": "P_Sim-Engine",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Sim-Engine/prototype/neural_scene_bridge.py",
            "30_Projects/P_Sim-Engine/prototype/render_neural_sensor_stub.py",
        ],
        "migration_status": "reference_only",
        "current_paths": [],
        "current_test_paths": [],
        "current_fixture_paths": [],
        "current_script_paths": [],
        "current_doc_paths": [
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["doc"],
        "result_summary": "Tracked only as future neural-render reference, not implemented here.",
        "open_gaps": ["Neural scene rendering is out of current repository scope."],
        "notes": "Reference only; no direct implementation target currently.",
    },
    {
        "block_id": "p_sim_engine.runtime_interop_reference",
        "project_id": "P_Sim-Engine",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Sim-Engine/prototype/sim_runtime_interop_contract_runner.py",
            "30_Projects/P_Sim-Engine/prototype/sim_runtime_interop_export_runner.py",
            "30_Projects/P_Sim-Engine/prototype/sim_runtime_interop_import_runner.py",
            "30_Projects/P_Sim-Engine/prototype/sim_runtime_probe_runner.py",
            "30_Projects/P_Sim-Engine/prototype/sim_runtime_scenario_contract_runner.py",
            "30_Projects/P_Sim-Engine/prototype/sim_runtime_scene_result_runner.py",
        ],
        "migration_status": "partial",
        "current_paths": [
            "src/hybrid_sensor_sim/renderers/runtime_executor.py",
            "src/hybrid_sensor_sim/renderers/backend_runner.py",
            "src/hybrid_sensor_sim/tools/renderer_backend_smoke.py",
            "src/hybrid_sensor_sim/tools/renderer_backend_workflow.py",
        ],
        "current_test_paths": [
            "tests/test_renderer_runtime.py",
            "tests/test_backend_runner.py",
            "tests/test_renderer_backend_smoke.py",
            "tests/test_renderer_backend_workflow.py",
        ],
        "current_fixture_paths": [],
        "current_script_paths": [
            "scripts/run_renderer_backend_smoke.py",
            "scripts/run_renderer_backend_workflow.py",
        ],
        "current_doc_paths": [
            "docs/p_sim_engine_migration_audit.md",
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["library", "workflow", "cli", "test", "doc"],
        "result_summary": (
            "Partial functional overlap through runtime execution, backend runner, smoke, "
            "package, and handoff tooling."
        ),
        "open_gaps": [
            "Interop export/import parity is not treated as a concrete deliverable yet."
        ],
        "notes": "Tracked as partial overlap and future reference.",
    },
    {
        "block_id": "p_map_toolset.convert_map",
        "project_id": "P_Map-Toolset-MVP",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Map-Toolset-MVP/prototype/convert_map_format.py",
        ],
        "migration_status": "migrated",
        "current_paths": [
            "src/hybrid_sensor_sim/maps/convert.py",
        ],
        "current_test_paths": [
            "tests/test_map_tools.py",
        ],
        "current_fixture_paths": [
            "tests/fixtures/autonomy_e2e/p_map_toolset/simple_map_v0.json",
            "tests/fixtures/autonomy_e2e/p_map_toolset/canonical_lane_graph_v0.json",
        ],
        "current_script_paths": [
            "scripts/run_map_convert.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["library", "cli", "test", "fixture", "doc"],
        "result_summary": "Canonical/simple map conversion helpers migrated.",
        "open_gaps": [],
        "notes": "",
    },
    {
        "block_id": "p_map_toolset.validate_canonical_map",
        "project_id": "P_Map-Toolset-MVP",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Map-Toolset-MVP/prototype/validate_canonical_map.py",
        ],
        "migration_status": "migrated",
        "current_paths": [
            "src/hybrid_sensor_sim/maps/validate.py",
        ],
        "current_test_paths": [
            "tests/test_map_tools.py",
        ],
        "current_fixture_paths": [
            "tests/fixtures/autonomy_e2e/p_map_toolset/canonical_lane_graph_v0.json",
        ],
        "current_script_paths": [
            "scripts/run_map_validate.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["library", "cli", "test", "fixture", "doc"],
        "result_summary": "Canonical map validation migrated.",
        "open_gaps": [],
        "notes": "",
    },
    {
        "block_id": "p_map_toolset.canonical_route",
        "project_id": "P_Map-Toolset-MVP",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Map-Toolset-MVP/prototype/compute_canonical_route.py",
        ],
        "migration_status": "migrated",
        "current_paths": [
            "src/hybrid_sensor_sim/maps/route.py",
        ],
        "current_test_paths": [
            "tests/test_map_tools.py",
        ],
        "current_fixture_paths": [
            "tests/fixtures/autonomy_e2e/p_map_toolset/canonical_lane_graph_v0.json",
        ],
        "current_script_paths": [
            "scripts/run_map_route.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["library", "cli", "test", "fixture", "doc"],
        "result_summary": "Canonical route computation migrated.",
        "open_gaps": [],
        "notes": "",
    },
    {
        "block_id": "p_validation.scenario_variants",
        "project_id": "P_Validation-Tooling-MVP",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Validation-Tooling-MVP/prototype/generate_scenario_variants.py",
        ],
        "migration_status": "migrated",
        "current_paths": [
            "src/hybrid_sensor_sim/scenarios/variants.py",
        ],
        "current_test_paths": [
            "tests/test_scenario_variants.py",
            "tests/test_scenario_variant_runner.py",
            "tests/test_scenario_variant_workflow.py",
        ],
        "current_fixture_paths": [
            "tests/fixtures/autonomy_e2e/p_validation/highway_cut_in_v0.json",
            "tests/fixtures/autonomy_e2e/p_validation/highway_map_route_relations_v0.json",
            "tests/fixtures/autonomy_e2e/p_validation/highway_mixed_payloads_v0.json",
            "tests/fixtures/autonomy_e2e/p_validation/highway_mixed_payloads_random_v0.json",
        ],
        "current_script_paths": [
            "scripts/run_scenario_variants.py",
            "scripts/run_scenario_variant_runner.py",
            "scripts/run_scenario_variant_workflow.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["library", "workflow", "cli", "test", "fixture", "doc"],
        "result_summary": "Logical scenario expansion plus rendered payload execution workflow migrated.",
        "open_gaps": [],
        "notes": "Release-report generation remains intentionally out of scope.",
    },
    {
        "block_id": "p_validation.release_report_reference",
        "project_id": "P_Validation-Tooling-MVP",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Validation-Tooling-MVP/prototype/generate_release_report.py",
        ],
        "migration_status": "reference_only",
        "current_paths": [],
        "current_test_paths": [],
        "current_fixture_paths": [],
        "current_script_paths": [],
        "current_doc_paths": [
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["doc"],
        "result_summary": "Tracked as release/reporting reference only.",
        "open_gaps": ["Release reporting is outside current repository scope."],
        "notes": "",
    },
    {
        "block_id": "p_cloud_engine.local_batch_pattern",
        "project_id": "P_Cloud-Engine",
        "source_kind": "workflow_pattern",
        "source_paths": [
            "30_Projects/P_Cloud-Engine/prototype/cloud_batch_runner.py",
            "30_Projects/P_Cloud-Engine/prototype/generate_batch_from_catalog.py",
            "30_Projects/P_Cloud-Engine/prototype/check_batch_against_catalog.py",
        ],
        "migration_status": "reference_only",
        "current_paths": [
            "src/hybrid_sensor_sim/scenarios/matrix_sweep.py",
            "src/hybrid_sensor_sim/tools/scenario_batch_workflow.py",
        ],
        "current_test_paths": [
            "tests/test_scenario_batch_workflow.py",
        ],
        "current_fixture_paths": [],
        "current_script_paths": [
            "scripts/run_scenario_batch_workflow.py",
        ],
        "current_doc_paths": [
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["workflow", "cli", "test", "doc"],
        "result_summary": "Local batch execution pattern only; cloud integration not migrated.",
        "open_gaps": ["Cloud orchestration remains out of scope."],
        "notes": "",
    },
    {
        "block_id": "p_e2e_stack.runtime_evidence_compare_reference",
        "project_id": "P_E2E_Stack",
        "source_kind": "workflow_pattern",
        "source_paths": [
            "30_Projects/P_E2E_Stack/prototype/compare_runtime_evidence.py",
            "30_Projects/P_E2E_Stack/prototype/compare_runtime_native_summaries.py",
            "30_Projects/P_E2E_Stack/prototype/run_runtime_available_workflow_dispatch.sh",
            "30_Projects/P_E2E_Stack/prototype/STACK_MASTER_PLAN.md",
            "30_Projects/P_E2E_Stack/prototype/REFERENCE_MIGRATION_MAP.md",
        ],
        "migration_status": "reference_only",
        "current_paths": [
            "src/hybrid_sensor_sim/tools/renderer_backend_workflow.py",
            "src/hybrid_sensor_sim/tools/scenario_batch_comparison.py",
        ],
        "current_test_paths": [
            "tests/test_renderer_backend_workflow.py",
            "tests/test_scenario_batch_comparison.py",
        ],
        "current_fixture_paths": [],
        "current_script_paths": [
            "scripts/run_renderer_backend_workflow.py",
            "scripts/run_scenario_batch_comparison.py",
        ],
        "current_doc_paths": [
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["workflow", "cli", "test", "doc"],
        "result_summary": "Reference for parity/orchestration patterns only.",
        "open_gaps": ["Full stack orchestration is intentionally excluded."],
        "notes": "",
    },
    {
        "block_id": "p_autoware_workspace_ci.data_contract_bridge",
        "project_id": "P_Autoware-Workspace-CI-MVP",
        "source_kind": "workflow_pattern",
        "source_paths": [
            "30_Projects/P_Autoware-Workspace-CI-MVP/prototype/hil_sequence_runner_stub.py",
            "30_Projects/P_Autoware-Workspace-CI-MVP/prototype/examples/hil_interface_v0.json",
        ],
        "migration_status": "partial",
        "current_paths": [
            "src/hybrid_sensor_sim/autoware/__init__.py",
            "src/hybrid_sensor_sim/autoware/contracts.py",
            "src/hybrid_sensor_sim/autoware/frames.py",
            "src/hybrid_sensor_sim/autoware/topics.py",
            "src/hybrid_sensor_sim/autoware/pipeline_manifest.py",
            "src/hybrid_sensor_sim/autoware/export_bridge.py",
            "src/hybrid_sensor_sim/autoware/profiles.py",
            "src/hybrid_sensor_sim/tools/autoware_pipeline_bridge.py",
            "src/hybrid_sensor_sim/tools/scenario_backend_smoke_workflow.py",
            "src/hybrid_sensor_sim/tools/scenario_runtime_backend_workflow.py",
        ],
        "current_test_paths": [
            "tests/test_autoware_contracts.py",
            "tests/test_autoware_pipeline_bridge.py",
            "tests/test_scenario_backend_smoke_workflow.py",
            "tests/test_scenario_runtime_backend_workflow.py",
        ],
        "current_fixture_paths": [],
        "current_script_paths": [
            "scripts/run_autoware_pipeline_bridge.py",
            "scripts/run_scenario_backend_smoke_workflow.py",
            "scripts/run_scenario_runtime_backend_workflow.py",
        ],
        "current_doc_paths": [
            "README.md",
            "docs/autonomy_e2e_history_integration.md",
            "docs/p_sim_engine_migration_audit.md",
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["library", "workflow", "cli", "test", "doc"],
        "result_summary": (
            "Autoware-facing data-contract bridge that converts backend smoke outputs "
            "into topics, frame-tree, and pipeline manifests."
        ),
        "open_gaps": [
            "No ROS2 live publisher or launch integration in the current round.",
            "Real packaged AWSIM/CARLA exports still need to be validated against this contract.",
        ],
        "notes": "This is a JSON-first compatibility layer, not a full Autoware stack migration.",
    },
    {
        "block_id": "p_autoware_workspace_ci.hil_sequence_reference",
        "project_id": "P_Autoware-Workspace-CI-MVP",
        "source_kind": "workflow_pattern",
        "source_paths": [
            "30_Projects/P_Autoware-Workspace-CI-MVP/prototype/hil_sequence_runner_stub.py",
        ],
        "migration_status": "reference_only",
        "current_paths": [],
        "current_test_paths": [],
        "current_fixture_paths": [],
        "current_script_paths": [],
        "current_doc_paths": [
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["doc"],
        "result_summary": "Reference for future integration CI/HIL discussions only.",
        "open_gaps": ["HIL/workspace CI is outside current repository scope."],
        "notes": "",
    },
    {
        "block_id": "p_data_lake.run_ingest_reference",
        "project_id": "P_Data-Lake-and-Explorer",
        "source_kind": "prototype_file",
        "source_paths": [
            "30_Projects/P_Data-Lake-and-Explorer/prototype/build_dataset_manifest.py",
            "30_Projects/P_Data-Lake-and-Explorer/prototype/ingest_scenario_runs.py",
            "30_Projects/P_Data-Lake-and-Explorer/prototype/query_scenario_runs.py",
        ],
        "migration_status": "deferred",
        "current_paths": [],
        "current_test_paths": [],
        "current_fixture_paths": [],
        "current_script_paths": [],
        "current_doc_paths": [
            "docs/autonomy_e2e_migration_master_plan.md",
        ],
        "working_result_kind": ["doc"],
        "result_summary": "Tracked provenance only; no direct migration into this repository.",
        "open_gaps": ["Analytics/data-lake stack stays outside the repository boundary."],
        "notes": "",
    },
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh Autonomy-E2E provenance metadata for the current repository."
    )
    parser.add_argument("--source-repo-root", required=True)
    parser.add_argument("--current-repo-root", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--recent-commit-limit", type=int, default=DEFAULT_RECENT_COMMIT_LIMIT)
    parser.add_argument("--write-doc-report", action="store_true")
    parser.add_argument("--fail-on-unmapped-selected-source", action="store_true")
    return parser.parse_args(argv)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _run_git(repo_root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=False,
        capture_output=True,
        text=True,
    )


def _git_available(repo_root: Path) -> bool:
    return _run_git(repo_root, ["rev-parse", "--is-inside-work-tree"]).returncode == 0


def _git_head_commit(repo_root: Path) -> str | None:
    completed = _run_git(repo_root, ["rev-parse", "HEAD"])
    if completed.returncode != 0:
        return None
    line = completed.stdout.strip()
    return line or None


def _git_branch(repo_root: Path) -> str | None:
    completed = _run_git(repo_root, ["rev-parse", "--abbrev-ref", "HEAD"])
    if completed.returncode != 0:
        return None
    line = completed.stdout.strip()
    return line or None


def _git_worktree_dirty(repo_root: Path) -> bool:
    completed = _run_git(repo_root, ["status", "--porcelain"])
    if completed.returncode != 0:
        return False
    return bool(completed.stdout.strip())


def _git_recent_commits(
    repo_root: Path,
    relative_paths: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    if not _git_available(repo_root):
        return []
    args = ["log", f"-n{max(int(limit), 0)}", "--format=%H%x1f%s%x1f%aI"]
    if relative_paths:
        args.extend(["--", *relative_paths])
    completed = _run_git(repo_root, args)
    if completed.returncode != 0:
        return []
    commits: list[dict[str, Any]] = []
    for line in completed.stdout.splitlines():
        parts = line.split("\x1f")
        if len(parts) != 3:
            continue
        commit, subject, author_date = (part.strip() for part in parts)
        if not commit:
            continue
        touched_paths = _git_commit_touched_paths(repo_root, commit, relative_paths)
        commits.append(
            {
                "commit": commit,
                "subject": subject,
                "author_date": author_date,
                "touched_paths": touched_paths,
            }
        )
    return commits


def _git_commit_touched_paths(
    repo_root: Path,
    commit: str,
    relative_paths: list[str],
) -> list[str]:
    args = ["show", "--name-only", "--format=", commit]
    if relative_paths:
        args.extend(["--", *relative_paths])
    completed = _run_git(repo_root, args)
    if completed.returncode != 0:
        return []
    return sorted(
        {
            line.strip()
            for line in completed.stdout.splitlines()
            if line.strip()
        }
    )


def _git_latest_touch_commit(repo_root: Path, relative_paths: list[str]) -> str | None:
    if not relative_paths:
        return None
    completed = _run_git(repo_root, ["log", "-n", "1", "--format=%H", "--", *relative_paths])
    if completed.returncode != 0:
        return None
    line = completed.stdout.strip()
    return line or None


def _git_intro_commit(repo_root: Path, relative_paths: list[str]) -> str | None:
    if not relative_paths:
        return None
    completed = _run_git(repo_root, ["log", "--format=%H", "--", *relative_paths])
    if completed.returncode != 0:
        return None
    lines = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    if not lines:
        return None
    return lines[-1]


def _relative_paths(values: list[str]) -> list[str]:
    return [str(Path(value)) for value in values if str(value).strip()]


def _eligible_prototype_file(path: Path) -> bool:
    if path.name in IGNORED_PROTOTYPE_FILENAMES:
        return False
    if any(part in IGNORED_PROTOTYPE_DIRNAMES for part in path.parts):
        return False
    return path.is_file() and path.suffix == ".py"


def _list_projects(source_repo_root: Path) -> list[Path]:
    projects_root = source_repo_root / THIRTY_PROJECTS_DIR_NAME
    if not projects_root.is_dir():
        return []
    return sorted(candidate for candidate in projects_root.iterdir() if candidate.is_dir())


def _prototype_files_for_project(project_path: Path, source_repo_root: Path) -> list[str]:
    prototype_root = project_path / "prototype"
    if not prototype_root.is_dir():
        return []
    return sorted(
        str(candidate.relative_to(source_repo_root))
        for candidate in prototype_root.rglob("*")
        if candidate.is_file()
        and not any(part in IGNORED_PROTOTYPE_DIRNAMES for part in candidate.parts)
    )


def _prototype_code_files_for_project(project_path: Path, source_repo_root: Path) -> list[str]:
    prototype_root = project_path / "prototype"
    if not prototype_root.is_dir():
        return []
    return sorted(
        str(candidate.relative_to(source_repo_root))
        for candidate in prototype_root.rglob("*")
        if _eligible_prototype_file(candidate)
    )


def _build_registry(
    *,
    source_repo_root: Path,
    current_repo_root: Path,
    recent_commit_limit: int,
) -> dict[str, Any]:
    blocks: list[dict[str, Any]] = []
    for spec in BLOCK_CATALOG:
        current_path_fields = (
            list(spec.get("current_paths", []))
            + list(spec.get("current_test_paths", []))
            + list(spec.get("current_fixture_paths", []))
            + list(spec.get("current_script_paths", []))
            + list(spec.get("current_doc_paths", []))
        )
        block = {
            "block_id": spec["block_id"],
            "project_id": spec["project_id"],
            "source_kind": spec["source_kind"],
            "source_paths": _relative_paths(list(spec.get("source_paths", []))),
            "source_commits": [
                entry["commit"]
                for entry in _git_recent_commits(
                    source_repo_root,
                    _relative_paths(list(spec.get("source_paths", []))),
                    min(recent_commit_limit, 5),
                )
            ],
            "source_evidence_summary": spec["result_summary"],
            "migration_status": spec["migration_status"],
            "current_paths": _relative_paths(list(spec.get("current_paths", []))),
            "current_test_paths": _relative_paths(list(spec.get("current_test_paths", []))),
            "current_fixture_paths": _relative_paths(list(spec.get("current_fixture_paths", []))),
            "current_script_paths": _relative_paths(list(spec.get("current_script_paths", []))),
            "current_doc_paths": _relative_paths(list(spec.get("current_doc_paths", []))),
            "current_intro_commit": _git_intro_commit(current_repo_root, current_path_fields),
            "current_latest_touch_commit": _git_latest_touch_commit(
                current_repo_root,
                current_path_fields,
            ),
            "working_result_kind": list(spec.get("working_result_kind", [])),
            "result_summary": spec["result_summary"],
            "open_gaps": list(spec.get("open_gaps", [])),
            "notes": spec.get("notes", ""),
        }
        blocks.append(block)
    return {
        "schema_version": AUTONOMY_E2E_MIGRATION_REGISTRY_SCHEMA_VERSION_V0,
        "generated_at_utc": _utc_now(),
        "source_repo_root": str(source_repo_root),
        "current_repo_root": str(current_repo_root),
        "integration_baseline_commit": INTEGRATION_BASELINE_COMMIT,
        "current_repo_head_commit": _git_head_commit(current_repo_root),
        "blocks": sorted(blocks, key=lambda item: item["block_id"]),
    }


def _build_project_inventory(
    *,
    source_repo_root: Path,
    registry: dict[str, Any],
    recent_commit_limit: int,
) -> dict[str, Any]:
    projects_root = source_repo_root / THIRTY_PROJECTS_DIR_NAME
    registry_blocks_by_project: dict[str, list[dict[str, Any]]] = {}
    for block in registry["blocks"]:
        registry_blocks_by_project.setdefault(block["project_id"], []).append(block)

    projects: list[dict[str, Any]] = []
    for project_name, spec in PROJECT_SPECS.items():
        project_path = projects_root / project_name
        prototype_path = project_path / "prototype"
        prototype_files = (
            _prototype_files_for_project(project_path, source_repo_root)
            if project_path.exists()
            else []
        )
        recent_commits = (
            _git_recent_commits(
                source_repo_root,
                [str(project_path.relative_to(source_repo_root))],
                recent_commit_limit,
            )
            if project_path.exists()
            else []
        )
        project_blocks = registry_blocks_by_project.get(project_name, [])
        current_equivalent_paths = sorted(
            {
                path
                for block in project_blocks
                for field_name in (
                    "current_paths",
                    "current_test_paths",
                    "current_fixture_paths",
                    "current_script_paths",
                    "current_doc_paths",
                )
                for path in block.get(field_name, [])
            }
        )
        covered_source_paths = {
            path
            for block in project_blocks
            for path in block.get("source_paths", [])
        }
        eligible_prototype_files = (
            _prototype_code_files_for_project(project_path, source_repo_root)
            if project_path.exists()
            else []
        )
        uncovered_selected_files = sorted(
            path for path in eligible_prototype_files if path not in covered_source_paths
        )
        if not project_blocks:
            integration_status = "unmapped"
        elif uncovered_selected_files:
            integration_status = "partially_mapped"
        else:
            integration_status = "mapped"
        projects.append(
            {
                "project_id": spec["project_id"],
                "project_name": project_name,
                "source_path": (
                    str(project_path.relative_to(source_repo_root))
                    if project_path.exists()
                    else str(project_path.relative_to(source_repo_root))
                ),
                "prototype_path": (
                    str(prototype_path.relative_to(source_repo_root))
                    if prototype_path.exists()
                    else str(prototype_path.relative_to(source_repo_root))
                ),
                "project_category": spec["project_category"],
                "migration_scope": spec["migration_scope"],
                "integration_status": integration_status,
                "prototype_files": prototype_files,
                "recent_source_commits": recent_commits,
                "current_repo_equivalent_paths": current_equivalent_paths,
                "current_assessment": spec["current_assessment"],
                "uncovered_selected_prototype_files": uncovered_selected_files,
            }
        )
    return {
        "schema_version": AUTONOMY_E2E_PROJECT_INVENTORY_SCHEMA_VERSION_V0,
        "generated_at_utc": _utc_now(),
        "source_repo_root": str(source_repo_root),
        "integration_baseline_commit": INTEGRATION_BASELINE_COMMIT,
        "projects": sorted(projects, key=lambda item: item["project_id"]),
    }


def _build_git_history_snapshot(
    *,
    source_repo_root: Path,
    recent_commit_limit: int,
) -> dict[str, Any]:
    projects_root = source_repo_root / THIRTY_PROJECTS_DIR_NAME
    projects: list[dict[str, Any]] = []
    for project_name in sorted(PROJECT_SPECS):
        project_path = projects_root / project_name
        relative_project_path = str(project_path.relative_to(source_repo_root))
        git_available = _git_available(source_repo_root) and project_path.exists()
        projects.append(
            {
                "project_id": project_name,
                "git_available": git_available,
                "branch": _git_branch(source_repo_root) if git_available else None,
                "head_commit": _git_head_commit(source_repo_root) if git_available else None,
                "recent_commits": (
                    _git_recent_commits(
                        source_repo_root,
                        [relative_project_path],
                        recent_commit_limit,
                    )
                    if git_available
                    else []
                ),
            }
        )
    return {
        "schema_version": AUTONOMY_E2E_GIT_HISTORY_SNAPSHOT_SCHEMA_VERSION_V0,
        "generated_at_utc": _utc_now(),
        "source_repo_root": str(source_repo_root),
        "source_head_commit": _git_head_commit(source_repo_root),
        "integration_baseline_commit": INTEGRATION_BASELINE_COMMIT,
        "projects": projects,
    }


def _load_existing_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None
    return payload


def _extract_commit_ids(snapshot: dict[str, Any] | None) -> set[str]:
    if not snapshot:
        return set()
    commit_ids: set[str] = set()
    for project in snapshot.get("projects", []):
        if not isinstance(project, dict):
            continue
        for commit_entry in project.get("recent_commits", []):
            if not isinstance(commit_entry, dict):
                continue
            commit = str(commit_entry.get("commit", "")).strip()
            if commit:
                commit_ids.add(commit)
    return commit_ids


def _selected_scope_unmapped_files(
    inventory: dict[str, Any],
) -> list[str]:
    unmapped: list[str] = []
    for project in inventory.get("projects", []):
        if not isinstance(project, dict):
            continue
        if str(project.get("migration_scope", "")).strip() != "selected_source":
            continue
        for path in project.get("uncovered_selected_prototype_files", []):
            text = str(path).strip()
            if text:
                unmapped.append(text)
    return sorted(set(unmapped))


def _orphan_current_paths(registry: dict[str, Any], current_repo_root: Path) -> list[str]:
    orphan_paths: list[str] = []
    for block in registry.get("blocks", []):
        if not isinstance(block, dict):
            continue
        for field_name in (
            "current_paths",
            "current_test_paths",
            "current_fixture_paths",
            "current_script_paths",
            "current_doc_paths",
        ):
            for relative_path in block.get(field_name, []):
                path = current_repo_root / str(relative_path)
                if not path.exists():
                    orphan_paths.append(str(relative_path))
    return sorted(set(orphan_paths))


def _changed_registry_blocks(
    old_registry: dict[str, Any] | None,
    new_registry: dict[str, Any],
) -> list[str]:
    if old_registry is None:
        return [block["block_id"] for block in new_registry["blocks"]]
    old_blocks = {
        block["block_id"]: block
        for block in old_registry.get("blocks", [])
        if isinstance(block, dict) and str(block.get("block_id", "")).strip()
    }
    changed: list[str] = []
    for block in new_registry["blocks"]:
        block_id = block["block_id"]
        if old_blocks.get(block_id) != block:
            changed.append(block_id)
    return sorted(changed)


def refresh_autonomy_e2e_history(
    *,
    source_repo_root: str | Path,
    current_repo_root: str | Path,
    output_root: str | Path,
    recent_commit_limit: int = DEFAULT_RECENT_COMMIT_LIMIT,
) -> dict[str, Any]:
    source_root = Path(source_repo_root).resolve()
    current_root = Path(current_repo_root).resolve()
    metadata_root = Path(output_root).resolve()
    metadata_root.mkdir(parents=True, exist_ok=True)

    inventory_path = metadata_root / "project_inventory_v0.json"
    snapshot_path = metadata_root / "source_git_history_snapshot_v0.json"
    registry_path = metadata_root / "migration_registry_v0.json"
    traceability_path = metadata_root / "result_traceability_index_v0.json"
    refresh_report_path = metadata_root / "history_refresh_report_v0.json"

    old_snapshot = _load_existing_json(snapshot_path)
    old_registry = _load_existing_json(registry_path)

    source_repo_available = (source_root / THIRTY_PROJECTS_DIR_NAME).is_dir()
    warnings: list[str] = []
    if not source_repo_available:
        warnings.append("SOURCE_REPO_UNAVAILABLE")

    registry = _build_registry(
        source_repo_root=source_root,
        current_repo_root=current_root,
        recent_commit_limit=recent_commit_limit,
    )
    if source_repo_available:
        inventory = _build_project_inventory(
            source_repo_root=source_root,
            registry=registry,
            recent_commit_limit=recent_commit_limit,
        )
        snapshot = _build_git_history_snapshot(
            source_repo_root=source_root,
            recent_commit_limit=recent_commit_limit,
        )
    else:
        empty_source = source_root
        inventory = {
            "schema_version": AUTONOMY_E2E_PROJECT_INVENTORY_SCHEMA_VERSION_V0,
            "generated_at_utc": _utc_now(),
            "source_repo_root": str(empty_source),
            "integration_baseline_commit": INTEGRATION_BASELINE_COMMIT,
            "projects": [
                {
                    "project_id": spec["project_id"],
                    "project_name": project_id,
                    "source_path": f"{THIRTY_PROJECTS_DIR_NAME}/{project_id}",
                    "prototype_path": f"{THIRTY_PROJECTS_DIR_NAME}/{project_id}/prototype",
                    "project_category": spec["project_category"],
                    "migration_scope": spec["migration_scope"],
                    "integration_status": "unmapped",
                    "prototype_files": [],
                    "recent_source_commits": [],
                    "current_repo_equivalent_paths": [],
                    "current_assessment": spec["current_assessment"],
                    "uncovered_selected_prototype_files": [],
                }
                for project_id, spec in sorted(PROJECT_SPECS.items())
            ],
        }
        snapshot = {
            "schema_version": AUTONOMY_E2E_GIT_HISTORY_SNAPSHOT_SCHEMA_VERSION_V0,
            "generated_at_utc": _utc_now(),
            "source_repo_root": str(empty_source),
            "source_head_commit": None,
            "integration_baseline_commit": INTEGRATION_BASELINE_COMMIT,
            "projects": [
                {
                    "project_id": project_id,
                    "git_available": False,
                    "branch": None,
                    "head_commit": None,
                    "recent_commits": [],
                }
                for project_id in sorted(PROJECT_SPECS)
            ],
        }

    traceability_index = build_reverse_traceability_index(registry, current_root)
    new_unmapped_prototype_files = _selected_scope_unmapped_files(inventory)
    orphan_current_paths = _orphan_current_paths(registry, current_root)
    previous_commit_ids = _extract_commit_ids(old_snapshot)
    current_commit_ids = _extract_commit_ids(snapshot)
    new_source_commits = sorted(current_commit_ids - previous_commit_ids)
    changed_registry_blocks = _changed_registry_blocks(old_registry, registry)

    refresh_report = {
        "schema_version": AUTONOMY_E2E_HISTORY_REFRESH_REPORT_SCHEMA_VERSION_V0,
        "generated_at_utc": _utc_now(),
        "source_repo_root": str(source_root),
        "current_repo_root": str(current_root),
        "integration_baseline_commit": INTEGRATION_BASELINE_COMMIT,
        "current_repo_head_commit": _git_head_commit(current_root),
        "current_repo_worktree_dirty": _git_worktree_dirty(current_root),
        "source_repo_available": source_repo_available,
        "inventory_status": "generated",
        "history_snapshot_status": "generated" if source_repo_available else "warning",
        "registry_status": "generated",
        "traceability_status": "generated",
        "warnings": warnings,
        "diff_summary": {
            "new_source_commits": new_source_commits,
            "new_unmapped_prototype_files": new_unmapped_prototype_files,
            "orphan_current_paths": orphan_current_paths,
            "changed_registry_blocks": changed_registry_blocks,
        },
    }

    _write_json(inventory_path, inventory)
    _write_json(snapshot_path, snapshot)
    _write_json(registry_path, registry)
    _write_json(traceability_path, traceability_index)
    _write_json(refresh_report_path, refresh_report)

    return {
        "metadata_root": str(metadata_root),
        "project_inventory_path": str(inventory_path),
        "git_history_snapshot_path": str(snapshot_path),
        "migration_registry_path": str(registry_path),
        "result_traceability_index_path": str(traceability_path),
        "history_refresh_report_path": str(refresh_report_path),
        "refresh_report": refresh_report,
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = refresh_autonomy_e2e_history(
        source_repo_root=args.source_repo_root,
        current_repo_root=args.current_repo_root,
        output_root=args.output_root,
        recent_commit_limit=max(int(args.recent_commit_limit), 0),
    )
    if args.write_doc_report:
        from hybrid_sensor_sim.tools.autonomy_e2e_history_report import (
            build_autonomy_e2e_history_report,
        )

        metadata_root = Path(result["metadata_root"])
        build_autonomy_e2e_history_report(
            metadata_root=metadata_root,
            json_out=metadata_root / "autonomy_e2e_history_report_v0.json",
            markdown_out=metadata_root / "autonomy_e2e_history_report_v0.md",
        )
    if (
        args.fail_on_unmapped_selected_source
        and result["refresh_report"]["diff_summary"]["new_unmapped_prototype_files"]
    ):
        return 2
    return 0


__all__ = [
    "DEFAULT_RECENT_COMMIT_LIMIT",
    "INTEGRATION_BASELINE_COMMIT",
    "refresh_autonomy_e2e_history",
    "main",
]
