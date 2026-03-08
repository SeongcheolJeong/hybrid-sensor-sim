from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AUTONOMY_E2E_PROJECT_INVENTORY_SCHEMA_VERSION_V0 = (
    "autonomy_e2e_project_inventory_v0"
)
AUTONOMY_E2E_GIT_HISTORY_SNAPSHOT_SCHEMA_VERSION_V0 = (
    "autonomy_e2e_git_history_snapshot_v0"
)
AUTONOMY_E2E_MIGRATION_REGISTRY_SCHEMA_VERSION_V0 = (
    "autonomy_e2e_migration_registry_v0"
)
AUTONOMY_E2E_RESULT_TRACEABILITY_INDEX_SCHEMA_VERSION_V0 = (
    "autonomy_e2e_result_traceability_index_v0"
)
AUTONOMY_E2E_HISTORY_REFRESH_REPORT_SCHEMA_VERSION_V0 = (
    "autonomy_e2e_history_refresh_report_v0"
)

ALLOWED_MIGRATION_STATUSES = {
    "migrated",
    "partial",
    "superseded",
    "reference_only",
    "deferred",
    "not_started",
}
ALLOWED_PATH_KINDS = {"library", "test", "fixture", "script", "doc", "config"}
ALLOWED_WORKING_RESULT_KINDS = {
    "library",
    "cli",
    "test",
    "fixture",
    "doc",
    "workflow",
    "config",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_json_object(path: str | Path, *, label: str) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _normalize_string_list(value: Any, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    normalized: list[str] = []
    for item in value:
        text = str(item).strip()
        if not text:
            raise ValueError(f"{field_name} must not contain empty items")
        normalized.append(text)
    return normalized


def _run_git(
    repo_root: str | Path,
    args: list[str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(Path(repo_root).resolve()), *args],
        check=False,
        capture_output=True,
        text=True,
    )


def _git_path_intro_commit(repo_root: str | Path, repo_relative_path: str) -> str | None:
    completed = _run_git(
        repo_root,
        ["log", "--format=%H", "--", repo_relative_path],
    )
    if completed.returncode != 0:
        return None
    lines = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    if not lines:
        return None
    return lines[-1]


def _git_path_latest_touch_commit(
    repo_root: str | Path,
    repo_relative_path: str,
) -> str | None:
    completed = _run_git(
        repo_root,
        ["log", "-n", "1", "--format=%H", "--", repo_relative_path],
    )
    if completed.returncode != 0:
        return None
    line = completed.stdout.strip()
    return line or None


def load_project_inventory(path: str | Path) -> dict[str, Any]:
    payload = _load_json_object(path, label="Autonomy-E2E project inventory")
    schema_version = str(payload.get("schema_version", "")).strip()
    if schema_version != AUTONOMY_E2E_PROJECT_INVENTORY_SCHEMA_VERSION_V0:
        raise ValueError(
            "schema_version must be "
            f"{AUTONOMY_E2E_PROJECT_INVENTORY_SCHEMA_VERSION_V0}"
        )
    return payload


def load_git_history_snapshot(path: str | Path) -> dict[str, Any]:
    payload = _load_json_object(path, label="Autonomy-E2E git history snapshot")
    schema_version = str(payload.get("schema_version", "")).strip()
    if schema_version != AUTONOMY_E2E_GIT_HISTORY_SNAPSHOT_SCHEMA_VERSION_V0:
        raise ValueError(
            "schema_version must be "
            f"{AUTONOMY_E2E_GIT_HISTORY_SNAPSHOT_SCHEMA_VERSION_V0}"
        )
    return payload


def validate_migration_registry(payload: dict[str, Any]) -> None:
    schema_version = str(payload.get("schema_version", "")).strip()
    if schema_version != AUTONOMY_E2E_MIGRATION_REGISTRY_SCHEMA_VERSION_V0:
        raise ValueError(
            "schema_version must be "
            f"{AUTONOMY_E2E_MIGRATION_REGISTRY_SCHEMA_VERSION_V0}"
        )
    blocks = payload.get("blocks")
    if not isinstance(blocks, list):
        raise ValueError("migration registry missing blocks list")
    seen_block_ids: set[str] = set()
    for block in blocks:
        if not isinstance(block, dict):
            raise ValueError("migration registry blocks must be objects")
        block_id = str(block.get("block_id", "")).strip()
        if not block_id:
            raise ValueError("migration registry block missing block_id")
        if block_id in seen_block_ids:
            raise ValueError(f"duplicate block_id in migration registry: {block_id}")
        seen_block_ids.add(block_id)
        project_id = str(block.get("project_id", "")).strip()
        if not project_id:
            raise ValueError(f"migration registry block {block_id} missing project_id")
        migration_status = str(block.get("migration_status", "")).strip()
        if migration_status not in ALLOWED_MIGRATION_STATUSES:
            raise ValueError(
                f"migration registry block {block_id} has invalid migration_status: "
                f"{migration_status}"
            )
        for field_name in (
            "source_paths",
            "source_commits",
            "current_paths",
            "current_test_paths",
            "current_fixture_paths",
            "current_script_paths",
            "current_doc_paths",
            "working_result_kind",
            "open_gaps",
        ):
            values = _normalize_string_list(block.get(field_name, []), field_name=field_name)
            if field_name == "working_result_kind":
                invalid_values = sorted(
                    value for value in values if value not in ALLOWED_WORKING_RESULT_KINDS
                )
                if invalid_values:
                    raise ValueError(
                        f"migration registry block {block_id} has invalid working_result_kind "
                        f"values: {', '.join(invalid_values)}"
                    )


def load_migration_registry(path: str | Path) -> dict[str, Any]:
    payload = _load_json_object(path, label="Autonomy-E2E migration registry")
    validate_migration_registry(payload)
    return payload


def load_result_traceability_index(path: str | Path) -> dict[str, Any]:
    payload = _load_json_object(path, label="Autonomy-E2E result traceability index")
    schema_version = str(payload.get("schema_version", "")).strip()
    if schema_version != AUTONOMY_E2E_RESULT_TRACEABILITY_INDEX_SCHEMA_VERSION_V0:
        raise ValueError(
            "schema_version must be "
            f"{AUTONOMY_E2E_RESULT_TRACEABILITY_INDEX_SCHEMA_VERSION_V0}"
        )
    paths = payload.get("paths")
    if not isinstance(paths, list):
        raise ValueError("result traceability index missing paths list")
    for entry in paths:
        if not isinstance(entry, dict):
            raise ValueError("result traceability index paths must be objects")
        current_path = str(entry.get("current_path", "")).strip()
        if not current_path:
            raise ValueError("result traceability entry missing current_path")
        path_kind = str(entry.get("path_kind", "")).strip()
        if path_kind not in ALLOWED_PATH_KINDS:
            raise ValueError(f"result traceability entry has invalid path_kind: {path_kind}")
    return payload


def build_reverse_traceability_index(
    registry: dict[str, Any],
    current_repo_root: str | Path,
) -> dict[str, Any]:
    validate_migration_registry(registry)
    repo_root = Path(current_repo_root).resolve()
    entries_by_path: dict[str, dict[str, Any]] = {}
    field_specs = (
        ("current_paths", "core_logic"),
        ("current_test_paths", "regression"),
        ("current_fixture_paths", "fixture"),
        ("current_script_paths", "runner"),
        ("current_doc_paths", "audit_doc"),
    )
    for block in registry["blocks"]:
        block_id = str(block["block_id"]).strip()
        project_id = str(block["project_id"]).strip()
        for field_name, result_role in field_specs:
            for current_path in block.get(field_name, []):
                relative_path = str(current_path).strip()
                if not relative_path:
                    continue
                path_kind = _infer_path_kind(relative_path)
                if path_kind is None:
                    raise ValueError(
                        f"unable to infer path_kind for current path: {relative_path}"
                    )
                entry = entries_by_path.setdefault(
                    relative_path,
                    {
                        "current_path": relative_path,
                        "path_kind": path_kind,
                        "block_ids": [],
                        "project_ids": [],
                        "result_role": result_role,
                        "current_intro_commit": None,
                        "current_latest_touch_commit": None,
                    },
                )
                if block_id not in entry["block_ids"]:
                    entry["block_ids"].append(block_id)
                if project_id not in entry["project_ids"]:
                    entry["project_ids"].append(project_id)
                if entry["result_role"] != result_role and entry["result_role"] != "mixed":
                    entry["result_role"] = "mixed"
    for relative_path, entry in entries_by_path.items():
        entry["block_ids"].sort()
        entry["project_ids"].sort()
        entry["current_intro_commit"] = _git_path_intro_commit(repo_root, relative_path)
        entry["current_latest_touch_commit"] = _git_path_latest_touch_commit(
            repo_root,
            relative_path,
        )
    return {
        "schema_version": AUTONOMY_E2E_RESULT_TRACEABILITY_INDEX_SCHEMA_VERSION_V0,
        "generated_at_utc": _utc_now(),
        "current_repo_root": str(repo_root),
        "paths": sorted(entries_by_path.values(), key=lambda item: item["current_path"]),
    }


def _infer_path_kind(relative_path: str) -> str | None:
    path = Path(relative_path)
    top_level = path.parts[0] if path.parts else ""
    if relative_path == "README.md" or path.suffix == ".md":
        return "doc"
    if top_level == "src":
        return "library"
    if top_level == "tests":
        if "fixtures" in path.parts:
            return "fixture"
        return "test"
    if top_level == "scripts":
        return "script"
    if top_level == "docs":
        return "doc"
    if top_level == "configs":
        return "config"
    return None


__all__ = [
    "ALLOWED_MIGRATION_STATUSES",
    "ALLOWED_PATH_KINDS",
    "ALLOWED_WORKING_RESULT_KINDS",
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
