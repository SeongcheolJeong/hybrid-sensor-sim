from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.io.autonomy_e2e_provenance import (
    load_git_history_snapshot,
    load_migration_registry,
    load_project_inventory,
    load_result_traceability_index,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a human-readable Autonomy-E2E provenance report."
    )
    parser.add_argument("--metadata-root", required=True)
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--markdown-out", required=True)
    return parser.parse_args(argv)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _count_by_field(rows: list[dict[str, Any]], field_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get(field_name, "")).strip() or "<missing>"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _build_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Autonomy-E2E History Report",
        "",
        "## Overview",
        "",
        f"- Integration baseline commit: `{report['overview'].get('integration_baseline_commit')}`",
        f"- Current repo head commit: `{report['overview'].get('current_repo_head_commit')}`",
        f"- Source repo head commit: `{report['overview'].get('source_head_commit')}`",
        f"- Project count: `{report['overview'].get('project_count')}`",
        f"- Registry block count: `{report['overview'].get('block_count')}`",
        f"- Traceability path count: `{report['overview'].get('traceability_path_count')}`",
        f"- Selected-source unmapped prototype files: `{report['overview'].get('selected_scope_unmapped_count')}`",
        f"- Orphan current paths: `{report['overview'].get('orphan_current_path_count')}`",
        "",
        "## Project Status Counts",
        "",
    ]
    for key, value in report["overview"]["project_scope_counts"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Registry Status Counts", ""])
    for key, value in report["overview"]["registry_status_counts"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## Project Rows",
            "",
            "| Project | Category | Scope | Integration | Prototype Files | Equivalents | Unmapped Selected Files |",
            "| --- | --- | --- | --- | ---: | ---: | ---: |",
        ]
    )
    for row in report["project_rows"]:
        lines.append(
            "| {project_id} | {project_category} | {migration_scope} | {integration_status} | "
            "{prototype_file_count} | {current_equivalent_count} | {unmapped_selected_file_count} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Unmapped Selected Prototype Files",
            "",
        ]
    )
    if report["drift"]["new_unmapped_prototype_files"]:
        for path in report["drift"]["new_unmapped_prototype_files"]:
            lines.append(f"- `{path}`")
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Orphan Current Paths",
            "",
        ]
    )
    if report["drift"]["orphan_current_paths"]:
        for path in report["drift"]["orphan_current_paths"]:
            lines.append(f"- `{path}`")
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def build_autonomy_e2e_history_report(
    *,
    metadata_root: str | Path,
    json_out: str | Path,
    markdown_out: str | Path,
) -> dict[str, Any]:
    metadata_path = Path(metadata_root).resolve()
    inventory = load_project_inventory(metadata_path / "project_inventory_v0.json")
    snapshot = load_git_history_snapshot(metadata_path / "source_git_history_snapshot_v0.json")
    registry = load_migration_registry(metadata_path / "migration_registry_v0.json")
    traceability = load_result_traceability_index(
        metadata_path / "result_traceability_index_v0.json"
    )
    refresh_report_path = metadata_path / "history_refresh_report_v0.json"
    refresh_report = (
        json.loads(refresh_report_path.read_text(encoding="utf-8"))
        if refresh_report_path.is_file()
        else {}
    )

    project_rows: list[dict[str, Any]] = []
    for project in inventory["projects"]:
        project_rows.append(
            {
                "project_id": project["project_id"],
                "project_category": project["project_category"],
                "migration_scope": project["migration_scope"],
                "integration_status": project["integration_status"],
                "prototype_file_count": len(project.get("prototype_files", [])),
                "current_equivalent_count": len(project.get("current_repo_equivalent_paths", [])),
                "unmapped_selected_file_count": len(
                    project.get("uncovered_selected_prototype_files", [])
                ),
            }
        )

    report = {
        "schema_version": "autonomy_e2e_history_report_v0",
        "generated_at_utc": refresh_report.get("generated_at_utc"),
        "overview": {
            "integration_baseline_commit": inventory.get("integration_baseline_commit"),
            "current_repo_head_commit": refresh_report.get("current_repo_head_commit"),
            "source_head_commit": snapshot.get("source_head_commit"),
            "project_count": len(inventory["projects"]),
            "block_count": len(registry["blocks"]),
            "traceability_path_count": len(traceability["paths"]),
            "project_scope_counts": _count_by_field(inventory["projects"], "migration_scope"),
            "project_integration_counts": _count_by_field(
                inventory["projects"],
                "integration_status",
            ),
            "registry_status_counts": _count_by_field(
                registry["blocks"],
                "migration_status",
            ),
            "selected_scope_unmapped_count": len(
                refresh_report.get("diff_summary", {}).get(
                    "new_unmapped_prototype_files",
                    [],
                )
            ),
            "orphan_current_path_count": len(
                refresh_report.get("diff_summary", {}).get("orphan_current_paths", [])
            ),
        },
        "project_rows": project_rows,
        "drift": {
            "new_source_commits": refresh_report.get("diff_summary", {}).get(
                "new_source_commits",
                [],
            ),
            "new_unmapped_prototype_files": refresh_report.get("diff_summary", {}).get(
                "new_unmapped_prototype_files",
                [],
            ),
            "orphan_current_paths": refresh_report.get("diff_summary", {}).get(
                "orphan_current_paths",
                [],
            ),
            "changed_registry_blocks": refresh_report.get("diff_summary", {}).get(
                "changed_registry_blocks",
                [],
            ),
            "warnings": refresh_report.get("warnings", []),
        },
    }

    json_path = Path(json_out).resolve()
    markdown_path = Path(markdown_out).resolve()
    _write_json(json_path, report)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(_build_markdown(report), encoding="utf-8")
    return {
        "report": report,
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    build_autonomy_e2e_history_report(
        metadata_root=args.metadata_root,
        json_out=args.json_out,
        markdown_out=args.markdown_out,
    )
    return 0


__all__ = ["build_autonomy_e2e_history_report", "main"]
