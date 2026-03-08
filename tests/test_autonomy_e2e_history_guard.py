from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.io.autonomy_e2e_provenance import (
    AUTONOMY_E2E_RESULT_TRACEABILITY_INDEX_SCHEMA_VERSION_V0,
)
from hybrid_sensor_sim.tools.autonomy_e2e_history_guard import (
    build_autonomy_e2e_history_guard_report,
    evaluate_autonomy_e2e_history_guard,
)


class AutonomyE2EHistoryGuardTests(unittest.TestCase):
    def _write_traceability_metadata(self, metadata_root: Path) -> None:
        metadata_root.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": AUTONOMY_E2E_RESULT_TRACEABILITY_INDEX_SCHEMA_VERSION_V0,
            "generated_at_utc": "2026-03-08T00:00:00Z",
            "paths": [
                {
                    "current_path": "src/tracked_module.py",
                    "path_kind": "library",
                    "block_ids": ["p_sim_engine.object_sim_core"],
                    "project_ids": ["P_Sim-Engine"],
                    "result_role": "core_logic",
                    "current_intro_commit": "1111111",
                    "current_latest_touch_commit": "2222222",
                },
                {
                    "current_path": "docs/mapped_doc.md",
                    "path_kind": "doc",
                    "block_ids": ["p_sim_engine.object_sim_core"],
                    "project_ids": ["P_Sim-Engine"],
                    "result_role": "audit_doc",
                    "current_intro_commit": "1111111",
                    "current_latest_touch_commit": "2222222",
                },
            ],
        }
        (
            metadata_root / "result_traceability_index_v0.json"
        ).write_text(json.dumps(payload, indent=2) + "\n")

    def _init_temp_repo(self, repo_root: Path) -> None:
        subprocess.run(["git", "init", "-b", "main"], cwd=repo_root, check=True)
        subprocess.run(
            ["git", "config", "user.email", "test@example.com"],
            cwd=repo_root,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Test User"],
            cwd=repo_root,
            check=True,
        )
        (repo_root / "src").mkdir(parents=True, exist_ok=True)
        (repo_root / "src" / "tracked_module.py").write_text("value = 1\n")
        subprocess.run(["git", "add", "."], cwd=repo_root, check=True)
        subprocess.run(["git", "commit", "-m", "baseline"], cwd=repo_root, check=True)
        subprocess.run(
            ["git", "update-ref", "refs/remotes/origin/main", "HEAD"],
            cwd=repo_root,
            check=True,
        )

    def test_guard_fails_when_mapped_changes_lack_metadata_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            repo_root.mkdir()
            metadata_root = repo_root / "metadata" / "autonomy_e2e"
            self._write_traceability_metadata(metadata_root)
            report = evaluate_autonomy_e2e_history_guard(
                current_repo_root=repo_root,
                metadata_root=metadata_root,
                changed_paths=["src/tracked_module.py"],
                compare_ref_available=True,
                head_commit="head123",
                worktree_dirty=True,
            )
            self.assertEqual(report["status"], "FAIL")
            self.assertIn(
                "MIGRATION_CHANGES_WITHOUT_METADATA_REFRESH",
                report["failure_codes"],
            )
            self.assertEqual(report["impacted_block_ids"], ["p_sim_engine.object_sim_core"])

    def test_guard_passes_when_metadata_changes_with_mapped_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            repo_root.mkdir()
            metadata_root = repo_root / "metadata" / "autonomy_e2e"
            self._write_traceability_metadata(metadata_root)
            report = evaluate_autonomy_e2e_history_guard(
                current_repo_root=repo_root,
                metadata_root=metadata_root,
                changed_paths=[
                    "src/tracked_module.py",
                    "metadata/autonomy_e2e/migration_registry_v0.json",
                ],
                compare_ref_available=True,
                head_commit="head123",
                worktree_dirty=True,
            )
            self.assertEqual(report["status"], "PASS")
            self.assertTrue(report["metadata_changed"])

    def test_guard_fails_on_unmapped_governed_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            repo_root.mkdir()
            metadata_root = repo_root / "metadata" / "autonomy_e2e"
            self._write_traceability_metadata(metadata_root)
            report = evaluate_autonomy_e2e_history_guard(
                current_repo_root=repo_root,
                metadata_root=metadata_root,
                changed_paths=["src/unmapped_module.py"],
                compare_ref_available=True,
                head_commit="head123",
                worktree_dirty=True,
            )
            self.assertEqual(report["status"], "FAIL")
            self.assertIn("UNMAPPED_CHANGED_PATHS", report["failure_codes"])
            self.assertEqual(report["unmapped_guarded_paths"], ["src/unmapped_module.py"])

    def test_guard_treats_provenance_system_changes_as_exempt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            repo_root.mkdir()
            metadata_root = repo_root / "metadata" / "autonomy_e2e"
            self._write_traceability_metadata(metadata_root)
            report = evaluate_autonomy_e2e_history_guard(
                current_repo_root=repo_root,
                metadata_root=metadata_root,
                changed_paths=[
                    "src/hybrid_sensor_sim/tools/autonomy_e2e_history_refresh.py",
                ],
                compare_ref_available=True,
                head_commit="head123",
                worktree_dirty=True,
            )
            self.assertEqual(report["status"], "PASS")
            self.assertEqual(
                report["provenance_system_changed_paths"],
                ["src/hybrid_sensor_sim/tools/autonomy_e2e_history_refresh.py"],
            )

    def test_guard_script_reports_failures_for_repo_diff(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            repo_root.mkdir()
            self._init_temp_repo(repo_root)
            metadata_root = repo_root / "metadata" / "autonomy_e2e"
            self._write_traceability_metadata(metadata_root)
            (repo_root / "src" / "tracked_module.py").write_text("value = 2\n")

            script_path = (
                Path(__file__).resolve().parents[1]
                / "scripts"
                / "run_autonomy_e2e_history_guard.py"
            )
            json_out = repo_root / "guard_report.json"
            completed = subprocess.run(
                [
                    "python3",
                    str(script_path),
                    "--metadata-root",
                    str(metadata_root),
                    "--current-repo-root",
                    str(repo_root),
                    "--compare-ref",
                    "origin/main",
                    "--json-out",
                    str(json_out),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 2)
            payload = json.loads(json_out.read_text())
            self.assertIn(
                "MIGRATION_CHANGES_WITHOUT_METADATA_REFRESH",
                payload["failure_codes"],
            )

    def test_build_guard_report_writes_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            repo_root.mkdir()
            metadata_root = repo_root / "metadata" / "autonomy_e2e"
            self._write_traceability_metadata(metadata_root)
            json_out = repo_root / "report.json"
            report = build_autonomy_e2e_history_guard_report(
                current_repo_root=repo_root,
                metadata_root=metadata_root,
                compare_ref="origin/main",
                json_out=json_out,
            )
            self.assertTrue(json_out.is_file())
            written = json.loads(json_out.read_text())
            self.assertEqual(
                written["schema_version"],
                report["schema_version"],
            )


if __name__ == "__main__":
    unittest.main()
