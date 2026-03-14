from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

DEFAULT_DB_PATH = (
    Path(__file__).resolve().parents[3] / "artifacts" / "control_plane" / "index.sqlite"
)
DEFAULT_REPO_ROOT = Path(__file__).resolve().parents[3]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


class ControlPlaneDB:
    def __init__(self, db_path: Optional[Path] = None, repo_root: Optional[Path] = None) -> None:
        self.db_path = Path(db_path or DEFAULT_DB_PATH).resolve()
        self.repo_root = Path(repo_root or DEFAULT_REPO_ROOT).resolve()
        ensure_parent(self.db_path)
        self._initialize()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(str(self.db_path), check_same_thread=False)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS projects (
                    project_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    root_path TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    run_type TEXT NOT NULL,
                    project_id TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    status_reason_codes TEXT NOT NULL,
                    artifact_root TEXT NOT NULL,
                    summary_json_path TEXT NOT NULL,
                    summary_markdown_path TEXT NOT NULL,
                    recommended_next_command TEXT NOT NULL,
                    request_payload TEXT NOT NULL,
                    result_payload TEXT NOT NULL,
                    error_message TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS run_artifacts (
                    artifact_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    artifact_type TEXT NOT NULL,
                    path TEXT NOT NULL,
                    mime_type TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
        self.ensure_default_project()

    def ensure_default_project(self) -> None:
        if self.get_project("default") is not None:
            return
        self.create_project(
            project_id="default",
            name="Hybrid Sensor Sim",
            description="Default local control-plane workspace",
            root_path=str(self.repo_root),
        )

    def create_project(self, *, project_id: str, name: str, description: str, root_path: str) -> dict[str, Any]:
        created_at = utc_now_iso()
        with self.connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO projects (project_id, name, description, root_path, created_at) VALUES (?, ?, ?, ?, ?)",
                (project_id, name, description, root_path, created_at),
            )
        return self.get_project(project_id) or {}

    def list_projects(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT project_id, name, description, root_path, created_at FROM projects ORDER BY created_at ASC"
            ).fetchall()
        return [dict(row) for row in rows]

    def get_project(self, project_id: str) -> Optional[dict[str, Any]]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT project_id, name, description, root_path, created_at FROM projects WHERE project_id = ?",
                (project_id,),
            ).fetchone()
        return dict(row) if row else None

    def create_run(
        self,
        *,
        run_id: str,
        run_type: str,
        project_id: str,
        source_kind: str,
        artifact_root: str,
        request_payload: dict[str, Any],
    ) -> dict[str, Any]:
        requested_at = utc_now_iso()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO runs (
                    run_id, run_type, project_id, source_kind, requested_at, started_at, finished_at,
                    status, status_reason_codes, artifact_root, summary_json_path, summary_markdown_path,
                    recommended_next_command, request_payload, result_payload, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    run_type,
                    project_id,
                    source_kind,
                    requested_at,
                    None,
                    None,
                    "PLANNED",
                    json.dumps([], ensure_ascii=True),
                    artifact_root,
                    "",
                    "",
                    "",
                    json.dumps(request_payload, ensure_ascii=True),
                    json.dumps({}, ensure_ascii=True),
                    "",
                ),
            )
        return self.get_run(run_id) or {}

    def update_run(
        self,
        run_id: str,
        **updates: Any,
    ) -> dict[str, Any]:
        if not updates:
            return self.get_run(run_id) or {}
        normalized: dict[str, Any] = {}
        for key, value in updates.items():
            if key in {"status_reason_codes", "request_payload", "result_payload"}:
                normalized[key] = json.dumps(value, ensure_ascii=True)
            else:
                normalized[key] = value
        assignments = ", ".join(f"{key} = ?" for key in normalized)
        params = list(normalized.values()) + [run_id]
        with self.connect() as connection:
            connection.execute(f"UPDATE runs SET {assignments} WHERE run_id = ?", params)
        return self.get_run(run_id) or {}

    def list_runs(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM runs ORDER BY requested_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._decode_run(dict(row)) for row in rows]

    def get_run(self, run_id: str) -> Optional[dict[str, Any]]:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if not row:
            return None
        return self._decode_run(dict(row))

    def replace_run_artifacts(self, run_id: str, artifacts: list[dict[str, Any]]) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM run_artifacts WHERE run_id = ?", (run_id,))
            for artifact in artifacts:
                connection.execute(
                    "INSERT INTO run_artifacts (run_id, artifact_type, path, mime_type, display_name, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        artifact["artifact_type"],
                        artifact["path"],
                        artifact["mime_type"],
                        artifact["display_name"],
                        artifact.get("created_at", utc_now_iso()),
                    ),
                )

    def list_run_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT artifact_id, run_id, artifact_type, path, mime_type, display_name, created_at FROM run_artifacts WHERE run_id = ? ORDER BY display_name ASC",
                (run_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def is_known_artifact_path(self, path: Path) -> bool:
        resolved = str(path.resolve())
        with self.connect() as connection:
            artifact_row = connection.execute(
                "SELECT 1 FROM run_artifacts WHERE path = ? LIMIT 1",
                (resolved,),
            ).fetchone()
            if artifact_row is not None:
                return True
            run_row = connection.execute(
                """
                SELECT 1
                FROM runs
                WHERE summary_json_path = ?
                   OR summary_markdown_path = ?
                   OR artifact_root = ?
                LIMIT 1
                """,
                (resolved, resolved, resolved),
            ).fetchone()
        return run_row is not None

    def _decode_run(self, run_row: dict[str, Any]) -> dict[str, Any]:
        decoded = dict(run_row)
        for key in ("status_reason_codes", "request_payload", "result_payload"):
            raw = decoded.get(key, "")
            try:
                decoded[key] = json.loads(raw) if raw else ([] if key == "status_reason_codes" else {})
            except json.JSONDecodeError:
                decoded[key] = [] if key == "status_reason_codes" else {}
        return decoded
