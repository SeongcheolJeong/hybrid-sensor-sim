from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.io.autonomy_e2e_provenance import (
    load_migration_registry,
    load_project_inventory,
    load_result_traceability_index,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query checked-in Autonomy-E2E provenance metadata."
    )
    parser.add_argument("--metadata-root", required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--project-id", default="")
    group.add_argument("--block-id", default="")
    group.add_argument("--current-path", default="")
    return parser.parse_args(argv)


def _load_metadata(metadata_root: str | Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    root = Path(metadata_root).resolve()
    inventory = load_project_inventory(root / "project_inventory_v0.json")
    registry = load_migration_registry(root / "migration_registry_v0.json")
    traceability = load_result_traceability_index(root / "result_traceability_index_v0.json")
    return inventory, registry, traceability


def query_by_project_id(
    *,
    metadata_root: str | Path,
    project_id: str,
) -> dict[str, Any]:
    inventory, registry, _ = _load_metadata(metadata_root)
    project_id_text = str(project_id).strip()
    for project in inventory["projects"]:
        if project.get("project_id") == project_id_text:
            return {
                "query_kind": "project_id",
                "project": project,
                "blocks": [
                    block
                    for block in registry["blocks"]
                    if block.get("project_id") == project_id_text
                ],
            }
    raise ValueError(f"unknown project_id: {project_id_text}")


def query_by_block_id(
    *,
    metadata_root: str | Path,
    block_id: str,
) -> dict[str, Any]:
    _, registry, traceability = _load_metadata(metadata_root)
    block_id_text = str(block_id).strip()
    for block in registry["blocks"]:
        if block.get("block_id") == block_id_text:
            related_paths = [
                path
                for path in traceability["paths"]
                if block_id_text in path.get("block_ids", [])
            ]
            return {
                "query_kind": "block_id",
                "block": block,
                "related_paths": related_paths,
            }
    raise ValueError(f"unknown block_id: {block_id_text}")


def query_by_current_path(
    *,
    metadata_root: str | Path,
    current_path: str | Path,
) -> dict[str, Any]:
    inventory, registry, traceability = _load_metadata(metadata_root)
    current_path_text = str(current_path).strip()
    current_path_obj = Path(current_path_text)
    if current_path_obj.is_absolute():
        try:
            current_path_text = str(
                current_path_obj.resolve().relative_to(Path(__file__).resolve().parents[3])
            )
        except ValueError:
            current_path_text = current_path_text
    for entry in traceability["paths"]:
        if entry.get("current_path") == current_path_text:
            block_ids = set(entry.get("block_ids", []))
            project_ids = set(entry.get("project_ids", []))
            return {
                "query_kind": "current_path",
                "current_path": current_path_text,
                "traceability_entry": entry,
                "blocks": [
                    block
                    for block in registry["blocks"]
                    if block.get("block_id") in block_ids
                ],
                "projects": [
                    project
                    for project in inventory["projects"]
                    if project.get("project_id") in project_ids
                ],
            }
    raise ValueError(f"unknown current_path: {current_path_text}")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.project_id:
        payload = query_by_project_id(
            metadata_root=args.metadata_root,
            project_id=args.project_id,
        )
    elif args.block_id:
        payload = query_by_block_id(
            metadata_root=args.metadata_root,
            block_id=args.block_id,
        )
    else:
        payload = query_by_current_path(
            metadata_root=args.metadata_root,
            current_path=args.current_path,
        )
    print(json.dumps(payload, indent=2, ensure_ascii=True))
    return 0


__all__ = [
    "main",
    "query_by_block_id",
    "query_by_current_path",
    "query_by_project_id",
]
