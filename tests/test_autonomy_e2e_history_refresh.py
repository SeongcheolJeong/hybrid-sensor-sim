from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.tools.autonomy_e2e_history_refresh import (
    INTEGRATION_BASELINE_COMMIT,
    refresh_autonomy_e2e_history,
)


def _git(repo_root: Path, *args: str) -> None:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr or completed.stdout)


def _init_repo(repo_root: Path) -> None:
    _git(repo_root, "init")
    _git(repo_root, "config", "user.email", "test@example.com")
    _git(repo_root, "config", "user.name", "Test User")


class AutonomyE2EHistoryRefreshTests(unittest.TestCase):
    def test_refresh_writes_all_metadata_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_root = root / "source"
            current_root = root / "current"
            output_root = root / "metadata"
            source_root.mkdir()
            current_root.mkdir()
            _init_repo(source_root)
            _init_repo(current_root)

            source_file = (
                source_root
                / "30_Projects"
                / "P_Sim-Engine"
                / "prototype"
                / "vehicle_dynamics_stub.py"
            )
            source_file.parent.mkdir(parents=True, exist_ok=True)
            source_file.write_text("VALUE = 1\n", encoding="utf-8")
            _git(source_root, "add", "30_Projects/P_Sim-Engine/prototype/vehicle_dynamics_stub.py")
            _git(source_root, "commit", "-m", "add vehicle dynamics stub")

            current_file = (
                current_root
                / "src"
                / "hybrid_sensor_sim"
                / "physics"
                / "vehicle_dynamics.py"
            )
            current_file.parent.mkdir(parents=True, exist_ok=True)
            current_file.write_text("VALUE = 2\n", encoding="utf-8")
            (current_root / "README.md").write_text("# current\n", encoding="utf-8")
            _git(current_root, "add", "src/hybrid_sensor_sim/physics/vehicle_dynamics.py", "README.md")
            _git(current_root, "commit", "-m", "add current file")

            result = refresh_autonomy_e2e_history(
                source_repo_root=source_root,
                current_repo_root=current_root,
                output_root=output_root,
                recent_commit_limit=5,
            )

            self.assertTrue(Path(result["project_inventory_path"]).is_file())
            self.assertTrue(Path(result["git_history_snapshot_path"]).is_file())
            self.assertTrue(Path(result["migration_registry_path"]).is_file())
            self.assertTrue(Path(result["result_traceability_index_path"]).is_file())
            self.assertTrue(Path(result["history_refresh_report_path"]).is_file())

            inventory = json.loads(
                Path(result["project_inventory_path"]).read_text(encoding="utf-8")
            )
            self.assertEqual(inventory["integration_baseline_commit"], INTEGRATION_BASELINE_COMMIT)
            projects = {project["project_id"]: project for project in inventory["projects"]}
            self.assertIn("P_Sim-Engine", projects)
            self.assertTrue(
                any(
                    path.endswith("vehicle_dynamics_stub.py")
                    for path in projects["P_Sim-Engine"]["prototype_files"]
                )
            )
            refresh_report = result["refresh_report"]
            self.assertFalse(refresh_report["current_repo_worktree_dirty"])

    def test_refresh_handles_missing_source_repo_with_warning(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            current_root = root / "current"
            current_root.mkdir()
            _init_repo(current_root)
            (current_root / "README.md").write_text("# current\n", encoding="utf-8")
            _git(current_root, "add", "README.md")
            _git(current_root, "commit", "-m", "init current")

            result = refresh_autonomy_e2e_history(
                source_repo_root=root / "missing_source",
                current_repo_root=current_root,
                output_root=root / "metadata",
                recent_commit_limit=5,
            )
            self.assertIn("SOURCE_REPO_UNAVAILABLE", result["refresh_report"]["warnings"])

    def test_refresh_ignores_runtime_assets_and_batch_runs_from_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_root = root / "source"
            current_root = root / "current"
            output_root = root / "metadata"
            source_root.mkdir()
            current_root.mkdir()
            _init_repo(source_root)
            _init_repo(current_root)

            kept_file = (
                source_root
                / "30_Projects"
                / "P_Sim-Engine"
                / "prototype"
                / "vehicle_dynamics_stub.py"
            )
            ignored_runtime_asset = (
                source_root
                / "30_Projects"
                / "P_Sim-Engine"
                / "prototype"
                / "runtime_assets"
                / "carla"
                / "sample.py"
            )
            ignored_batch_run = (
                source_root
                / "30_Projects"
                / "P_Cloud-Engine"
                / "prototype"
                / "batch_runs"
                / "RUN_0001"
                / "trace.csv"
            )
            kept_file.parent.mkdir(parents=True, exist_ok=True)
            ignored_runtime_asset.parent.mkdir(parents=True, exist_ok=True)
            ignored_batch_run.parent.mkdir(parents=True, exist_ok=True)
            kept_file.write_text("VALUE = 1\n", encoding="utf-8")
            ignored_runtime_asset.write_text("VALUE = 2\n", encoding="utf-8")
            ignored_batch_run.write_text("trace\n", encoding="utf-8")
            _git(source_root, "add", ".")
            _git(source_root, "commit", "-m", "seed prototype files")

            (current_root / "README.md").write_text("# current\n", encoding="utf-8")
            _git(current_root, "add", "README.md")
            _git(current_root, "commit", "-m", "init current")

            result = refresh_autonomy_e2e_history(
                source_repo_root=source_root,
                current_repo_root=current_root,
                output_root=output_root,
                recent_commit_limit=5,
            )

            inventory = json.loads(
                Path(result["project_inventory_path"]).read_text(encoding="utf-8")
            )
            projects = {project["project_id"]: project for project in inventory["projects"]}
            self.assertIn(
                "30_Projects/P_Sim-Engine/prototype/vehicle_dynamics_stub.py",
                projects["P_Sim-Engine"]["prototype_files"],
            )
            self.assertFalse(
                any("runtime_assets" in path for path in projects["P_Sim-Engine"]["prototype_files"])
            )
            self.assertFalse(
                any("batch_runs" in path for path in projects["P_Cloud-Engine"]["prototype_files"])
            )

    def test_refresh_script_bootstraps_src_path(self) -> None:
        script_path = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "run_autonomy_e2e_history_refresh.py"
        )
        completed = subprocess.run(
            ["python3", str(script_path), "--help"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0)
        self.assertIn("--source-repo-root", completed.stdout)


if __name__ == "__main__":
    unittest.main()
