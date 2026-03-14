from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from hybrid_sensor_sim.server.db import ControlPlaneDB, DEFAULT_DB_PATH, DEFAULT_REPO_ROOT
from hybrid_sensor_sim.server.jobs import JobManager
from hybrid_sensor_sim.server.routes import autoware, history, projects, runs, runtime, scenarios

_APP_DB: ControlPlaneDB | None = None
_APP_JOB_MANAGER: JobManager | None = None
CONTROL_PLANE_REPO_ROOT_ENV = "CONTROL_PLANE_REPO_ROOT"
CONTROL_PLANE_DB_PATH_ENV = "CONTROL_PLANE_DB_PATH"


def get_repo_root() -> Path:
    configured = os.environ.get(CONTROL_PLANE_REPO_ROOT_ENV, "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return DEFAULT_REPO_ROOT.resolve()


def get_db_path() -> Path:
    configured = os.environ.get(CONTROL_PLANE_DB_PATH_ENV, "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return DEFAULT_DB_PATH.resolve()


def build_app() -> FastAPI:
    global _APP_DB, _APP_JOB_MANAGER
    repo_root = get_repo_root()
    db_path = get_db_path()
    _APP_DB = ControlPlaneDB(db_path=db_path, repo_root=repo_root)
    _APP_JOB_MANAGER = JobManager(_APP_DB, repo_root=repo_root)

    app = FastAPI(
        title="Hybrid Sensor Sim Control Plane",
        version="0.1.0",
        description="Applied-style control-plane API over the hybrid sensor sim engine",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.state.repo_root = repo_root
    app.state.db_path = db_path

    @app.get("/api/v1/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    app.include_router(projects.router)
    app.include_router(scenarios.router)
    app.include_router(runs.router)
    app.include_router(runtime.router)
    app.include_router(autoware.router)
    app.include_router(history.router)
    return app


def get_app_db() -> ControlPlaneDB:
    global _APP_DB
    if _APP_DB is None:
        _APP_DB = ControlPlaneDB(db_path=get_db_path(), repo_root=get_repo_root())
    return _APP_DB


def get_app_job_manager() -> JobManager:
    global _APP_JOB_MANAGER
    if _APP_JOB_MANAGER is None:
        _APP_JOB_MANAGER = JobManager(get_app_db(), repo_root=get_repo_root())
    return _APP_JOB_MANAGER
