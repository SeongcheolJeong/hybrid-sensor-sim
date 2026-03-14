from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hybrid_sensor_sim.io.autonomy_e2e_provenance import load_result_traceability_index


AUTONOMY_E2E_HISTORY_GUARD_REPORT_SCHEMA_VERSION_V0 = (
    "autonomy_e2e_history_guard_report_v0"
)

DEFAULT_COMPARE_REF = "origin/main"
GUARDED_RESULT_PREFIXES = ("src/", "scripts/", "tests/", "configs/", "apps/", "examples/")
DOC_RESULT_PREFIXES = ("docs/",)
ROOT_DOC_FILES = {"README.md"}
METADATA_PREFIX = "metadata/autonomy_e2e/"
PROVENANCE_SYSTEM_PREFIXES = (
    "src/hybrid_sensor_sim/tools/autonomy_e2e_history_",
    "scripts/run_autonomy_e2e_history_",
    "tests/test_autonomy_e2e_history_",
)
PROVENANCE_SYSTEM_EXACT_PATHS = {
    "src/hybrid_sensor_sim/io/__init__.py",
    "src/hybrid_sensor_sim/io/autonomy_e2e_provenance.py",
    "src/hybrid_sensor_sim/tools/__init__.py",
    "docs/autonomy_e2e_history_integration.md",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _run_git(
    repo_root: Path,
    args: list[str],
    *,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=check,
        capture_output=True,
        text=True,
    )


def _git_head_commit(repo_root: Path) -> str | None:
    completed = _run_git(repo_root, ["rev-parse", "HEAD"])
    if completed.returncode != 0:
        return None
    return completed.stdout.strip() or None


def _git_worktree_dirty(repo_root: Path) -> bool:
    completed = _run_git(repo_root, ["status", "--porcelain"])
    if completed.returncode != 0:
        return False
    return bool(completed.stdout.strip())


def _compare_ref_available(repo_root: Path, compare_ref: str) -> bool:
    completed = _run_git(repo_root, ["rev-parse", "--verify", compare_ref])
    return completed.returncode == 0


def _git_path_set(repo_root: Path, args: list[str]) -> set[str]:
    completed = _run_git(repo_root, args)
    if completed.returncode != 0:
        return set()
    return {
        line.strip()
        for line in completed.stdout.splitlines()
        if line.strip()
    }


def collect_changed_paths(
    *,
    current_repo_root: str | Path,
    compare_ref: str = DEFAULT_COMPARE_REF,
    include_untracked: bool = False,
) -> dict[str, Any]:
    repo_root = Path(current_repo_root).resolve()
    compare_ref_present = _compare_ref_available(repo_root, compare_ref)

    changed_paths: set[str] = set()
    if compare_ref_present:
        changed_paths |= _git_path_set(
            repo_root,
            ["diff", "--name-only", "--diff-filter=ACMR", f"{compare_ref}...HEAD"],
        )
    changed_paths |= _git_path_set(
        repo_root,
        ["diff", "--name-only", "--diff-filter=ACMR"],
    )
    changed_paths |= _git_path_set(
        repo_root,
        ["diff", "--cached", "--name-only", "--diff-filter=ACMR"],
    )
    if include_untracked:
        changed_paths |= _git_path_set(
            repo_root,
            ["ls-files", "--others", "--exclude-standard"],
        )

    return {
        "compare_ref": compare_ref,
        "compare_ref_available": compare_ref_present,
        "changed_paths": sorted(changed_paths),
        "head_commit": _git_head_commit(repo_root),
        "worktree_dirty": _git_worktree_dirty(repo_root),
    }


def _is_guarded_result_path(path: str) -> bool:
    return path.startswith(GUARDED_RESULT_PREFIXES)


def _is_doc_result_path(path: str) -> bool:
    return path.startswith(DOC_RESULT_PREFIXES) or path in ROOT_DOC_FILES


def _is_metadata_path(path: str) -> bool:
    return path.startswith(METADATA_PREFIX)


def _is_provenance_system_path(path: str) -> bool:
    return path in PROVENANCE_SYSTEM_EXACT_PATHS or path.startswith(
        PROVENANCE_SYSTEM_PREFIXES
    )


def evaluate_autonomy_e2e_history_guard(
    *,
    current_repo_root: str | Path,
    metadata_root: str | Path,
    compare_ref: str = DEFAULT_COMPARE_REF,
    include_untracked: bool = False,
    changed_paths: list[str] | None = None,
    compare_ref_available: bool | None = None,
    head_commit: str | None = None,
    worktree_dirty: bool | None = None,
) -> dict[str, Any]:
    repo_root = Path(current_repo_root).resolve()
    metadata_path = Path(metadata_root).resolve()
    traceability = load_result_traceability_index(
        metadata_path / "result_traceability_index_v0.json"
    )
    traceability_by_path = {
        entry["current_path"]: entry
        for entry in traceability["paths"]
    }

    if changed_paths is None:
        git_state = collect_changed_paths(
            current_repo_root=repo_root,
            compare_ref=compare_ref,
            include_untracked=include_untracked,
        )
        changed_paths = git_state["changed_paths"]
        compare_ref_available = git_state["compare_ref_available"]
        head_commit = git_state["head_commit"]
        worktree_dirty = git_state["worktree_dirty"]
    else:
        changed_paths = sorted({str(path).strip() for path in changed_paths if str(path).strip()})
        if compare_ref_available is None:
            compare_ref_available = True
        if head_commit is None:
            head_commit = _git_head_commit(repo_root)
        if worktree_dirty is None:
            worktree_dirty = _git_worktree_dirty(repo_root)

    metadata_changed_paths = [path for path in changed_paths if _is_metadata_path(path)]
    provenance_system_changed_paths = [
        path for path in changed_paths if _is_provenance_system_path(path)
    ]

    mapped_guarded_paths: list[str] = []
    mapped_doc_paths: list[str] = []
    unmapped_guarded_paths: list[str] = []
    other_changed_paths: list[str] = []
    impacted_block_ids: set[str] = set()
    impacted_project_ids: set[str] = set()

    for path in changed_paths:
        if _is_metadata_path(path) or _is_provenance_system_path(path):
            continue
        entry = traceability_by_path.get(path)
        if _is_guarded_result_path(path):
            if entry is None:
                unmapped_guarded_paths.append(path)
                continue
            mapped_guarded_paths.append(path)
        elif _is_doc_result_path(path):
            if entry is not None:
                mapped_doc_paths.append(path)
            else:
                other_changed_paths.append(path)
                continue
        else:
            other_changed_paths.append(path)
            continue
        if entry is not None:
            impacted_block_ids.update(entry.get("block_ids", []))
            impacted_project_ids.update(entry.get("project_ids", []))

    failure_codes: list[str] = []
    warnings: list[str] = []
    if not compare_ref_available:
        warnings.append("COMPARE_REF_UNAVAILABLE")
    if unmapped_guarded_paths:
        failure_codes.append("UNMAPPED_CHANGED_PATHS")
    metadata_refresh_required = bool(mapped_guarded_paths)
    if metadata_refresh_required and not metadata_changed_paths:
        failure_codes.append("MIGRATION_CHANGES_WITHOUT_METADATA_REFRESH")

    status = "PASS" if not failure_codes else "FAIL"
    if not changed_paths:
        status = "PASS"

    return {
        "schema_version": AUTONOMY_E2E_HISTORY_GUARD_REPORT_SCHEMA_VERSION_V0,
        "generated_at_utc": _utc_now(),
        "current_repo_root": str(repo_root),
        "metadata_root": str(metadata_path),
        "compare_ref": compare_ref,
        "compare_ref_available": compare_ref_available,
        "head_commit": head_commit,
        "worktree_dirty": bool(worktree_dirty),
        "include_untracked": bool(include_untracked),
        "changed_path_count": len(changed_paths),
        "changed_paths": changed_paths,
        "metadata_changed": bool(metadata_changed_paths),
        "metadata_changed_paths": metadata_changed_paths,
        "metadata_refresh_required": metadata_refresh_required,
        "mapped_guarded_paths": mapped_guarded_paths,
        "mapped_doc_paths": mapped_doc_paths,
        "provenance_system_changed_paths": provenance_system_changed_paths,
        "unmapped_guarded_paths": unmapped_guarded_paths,
        "other_changed_paths": other_changed_paths,
        "impacted_block_ids": sorted(impacted_block_ids),
        "impacted_project_ids": sorted(impacted_project_ids),
        "failure_codes": failure_codes,
        "warnings": warnings,
        "status": status,
    }


def build_autonomy_e2e_history_guard_report(
    *,
    current_repo_root: str | Path,
    metadata_root: str | Path,
    compare_ref: str = DEFAULT_COMPARE_REF,
    include_untracked: bool = False,
    json_out: str | Path | None = None,
) -> dict[str, Any]:
    report = evaluate_autonomy_e2e_history_guard(
        current_repo_root=current_repo_root,
        metadata_root=metadata_root,
        compare_ref=compare_ref,
        include_untracked=include_untracked,
    )
    if json_out is not None:
        output_path = Path(json_out).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, ensure_ascii=True) + "\n")
    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Check whether changes against the canonical GitHub baseline are covered by "
            "Autonomy-E2E provenance metadata."
        )
    )
    parser.add_argument("--metadata-root", required=True)
    parser.add_argument(
        "--current-repo-root",
        default=str(Path(__file__).resolve().parents[3]),
    )
    parser.add_argument("--compare-ref", default=DEFAULT_COMPARE_REF)
    parser.add_argument("--json-out", default="")
    parser.add_argument("--include-untracked", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    report = build_autonomy_e2e_history_guard_report(
        current_repo_root=args.current_repo_root,
        metadata_root=args.metadata_root,
        compare_ref=args.compare_ref,
        include_untracked=args.include_untracked,
        json_out=args.json_out or None,
    )
    print(json.dumps(report, indent=2, ensure_ascii=True))
    return 0 if report["status"] == "PASS" else 2


__all__ = [
    "AUTONOMY_E2E_HISTORY_GUARD_REPORT_SCHEMA_VERSION_V0",
    "build_autonomy_e2e_history_guard_report",
    "collect_changed_paths",
    "evaluate_autonomy_e2e_history_guard",
    "main",
]
