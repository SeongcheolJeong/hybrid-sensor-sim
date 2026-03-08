from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from hybrid_sensor_sim.io.autonomy_e2e_provenance import (
    AUTONOMY_E2E_MIGRATION_REGISTRY_SCHEMA_VERSION_V0,
    build_reverse_traceability_index,
    validate_migration_registry,
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


class AutonomyE2EProvenanceTests(unittest.TestCase):
    def test_validate_migration_registry_rejects_duplicate_block_id(self) -> None:
        payload = {
            "schema_version": AUTONOMY_E2E_MIGRATION_REGISTRY_SCHEMA_VERSION_V0,
            "blocks": [
                {
                    "block_id": "p_sim_engine.vehicle_dynamics",
                    "project_id": "P_Sim-Engine",
                    "migration_status": "migrated",
                    "source_paths": ["30_Projects/P_Sim-Engine/prototype/vehicle_dynamics_stub.py"],
                    "source_commits": [],
                    "current_paths": ["src/hybrid_sensor_sim/physics/vehicle_dynamics.py"],
                    "current_test_paths": [],
                    "current_fixture_paths": [],
                    "current_script_paths": [],
                    "current_doc_paths": [],
                    "working_result_kind": ["library"],
                    "open_gaps": [],
                },
                {
                    "block_id": "p_sim_engine.vehicle_dynamics",
                    "project_id": "P_Sim-Engine",
                    "migration_status": "migrated",
                    "source_paths": ["30_Projects/P_Sim-Engine/prototype/vehicle_dynamics_stub.py"],
                    "source_commits": [],
                    "current_paths": ["src/hybrid_sensor_sim/physics/vehicle_dynamics.py"],
                    "current_test_paths": [],
                    "current_fixture_paths": [],
                    "current_script_paths": [],
                    "current_doc_paths": [],
                    "working_result_kind": ["library"],
                    "open_gaps": [],
                },
            ],
        }
        with self.assertRaisesRegex(ValueError, "duplicate block_id"):
            validate_migration_registry(payload)

    def test_build_reverse_traceability_index_infers_doc_and_library_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            _init_repo(repo_root)
            vehicle_dynamics = repo_root / "src" / "hybrid_sensor_sim" / "physics" / "vehicle_dynamics.py"
            vehicle_dynamics.parent.mkdir(parents=True, exist_ok=True)
            vehicle_dynamics.write_text("VALUE = 1\n", encoding="utf-8")
            readme = repo_root / "README.md"
            readme.write_text("# demo\n", encoding="utf-8")
            _git(repo_root, "add", "src/hybrid_sensor_sim/physics/vehicle_dynamics.py", "README.md")
            _git(repo_root, "commit", "-m", "initial")

            registry = {
                "schema_version": AUTONOMY_E2E_MIGRATION_REGISTRY_SCHEMA_VERSION_V0,
                "blocks": [
                    {
                        "block_id": "p_sim_engine.vehicle_dynamics",
                        "project_id": "P_Sim-Engine",
                        "migration_status": "migrated",
                        "source_paths": ["30_Projects/P_Sim-Engine/prototype/vehicle_dynamics_stub.py"],
                        "source_commits": ["abc123"],
                        "current_paths": ["src/hybrid_sensor_sim/physics/vehicle_dynamics.py"],
                        "current_test_paths": [],
                        "current_fixture_paths": [],
                        "current_script_paths": [],
                        "current_doc_paths": ["README.md"],
                        "working_result_kind": ["library", "doc"],
                        "open_gaps": [],
                    }
                ],
            }

            traceability = build_reverse_traceability_index(registry, repo_root)
            entries = {entry["current_path"]: entry for entry in traceability["paths"]}
            self.assertEqual(entries["src/hybrid_sensor_sim/physics/vehicle_dynamics.py"]["path_kind"], "library")
            self.assertEqual(entries["README.md"]["path_kind"], "doc")
            self.assertEqual(
                entries["src/hybrid_sensor_sim/physics/vehicle_dynamics.py"]["block_ids"],
                ["p_sim_engine.vehicle_dynamics"],
            )
            self.assertTrue(entries["README.md"]["current_intro_commit"])
            self.assertTrue(entries["README.md"]["current_latest_touch_commit"])


if __name__ == "__main__":
    unittest.main()
