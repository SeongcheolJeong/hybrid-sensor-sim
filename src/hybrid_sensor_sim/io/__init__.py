"""I/O utilities for sensor simulation artifacts."""

from hybrid_sensor_sim.io.autonomy_e2e_provenance import (
    AUTONOMY_E2E_GIT_HISTORY_SNAPSHOT_SCHEMA_VERSION_V0,
    AUTONOMY_E2E_HISTORY_REFRESH_REPORT_SCHEMA_VERSION_V0,
    AUTONOMY_E2E_MIGRATION_REGISTRY_SCHEMA_VERSION_V0,
    AUTONOMY_E2E_PROJECT_INVENTORY_SCHEMA_VERSION_V0,
    AUTONOMY_E2E_RESULT_TRACEABILITY_INDEX_SCHEMA_VERSION_V0,
    build_reverse_traceability_index,
    load_git_history_snapshot,
    load_migration_registry,
    load_project_inventory,
    load_result_traceability_index,
    validate_migration_registry,
)

__all__ = [
    "AUTONOMY_E2E_GIT_HISTORY_SNAPSHOT_SCHEMA_VERSION_V0",
    "AUTONOMY_E2E_HISTORY_REFRESH_REPORT_SCHEMA_VERSION_V0",
    "AUTONOMY_E2E_MIGRATION_REGISTRY_SCHEMA_VERSION_V0",
    "AUTONOMY_E2E_PROJECT_INVENTORY_SCHEMA_VERSION_V0",
    "AUTONOMY_E2E_RESULT_TRACEABILITY_INDEX_SCHEMA_VERSION_V0",
    "build_reverse_traceability_index",
    "load_git_history_snapshot",
    "load_migration_registry",
    "load_project_inventory",
    "load_result_traceability_index",
    "validate_migration_registry",
]
