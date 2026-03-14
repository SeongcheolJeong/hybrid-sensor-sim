from __future__ import annotations

import json
import time
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

from hybrid_sensor_sim.server.db import ControlPlaneDB
from hybrid_sensor_sim.server.jobs import JobManager
from hybrid_sensor_sim.server.models import RunArtifactModel, RunDetailModel, RunIndexEntryModel, RunLaunchRequest

router = APIRouter(prefix="/api/v1", tags=["runs"])


def get_db() -> ControlPlaneDB:
    from hybrid_sensor_sim.server.app import get_app_db

    return get_app_db()


def get_job_manager() -> JobManager:
    from hybrid_sensor_sim.server.app import get_app_job_manager

    return get_app_job_manager()


@router.get("/runs", response_model=list[RunIndexEntryModel])
def list_runs(limit: int = Query(default=100, ge=1, le=500), db: ControlPlaneDB = Depends(get_db)) -> list[RunIndexEntryModel]:
    return [RunIndexEntryModel(**_to_index_entry(row)) for row in db.list_runs(limit=limit)]


@router.post("/runs/object-sim", response_model=RunIndexEntryModel)
def launch_object_sim(request: RunLaunchRequest, jobs: JobManager = Depends(get_job_manager)) -> RunIndexEntryModel:
    run_row = jobs.submit(run_type="object_sim", project_id=request.project_id, payload=request.payload)
    return RunIndexEntryModel(**_to_index_entry(run_row))


@router.post("/runs/batch-workflow", response_model=RunIndexEntryModel)
def launch_batch_workflow(request: RunLaunchRequest, jobs: JobManager = Depends(get_job_manager)) -> RunIndexEntryModel:
    run_row = jobs.submit(run_type="batch_workflow", project_id=request.project_id, payload=request.payload)
    return RunIndexEntryModel(**_to_index_entry(run_row))


@router.post("/runs/backend-smoke", response_model=RunIndexEntryModel)
def launch_backend_smoke(request: RunLaunchRequest, jobs: JobManager = Depends(get_job_manager)) -> RunIndexEntryModel:
    run_row = jobs.submit(run_type="backend_smoke", project_id=request.project_id, payload=request.payload)
    return RunIndexEntryModel(**_to_index_entry(run_row))


@router.post("/runs/runtime-backend", response_model=RunIndexEntryModel)
def launch_runtime_backend(request: RunLaunchRequest, jobs: JobManager = Depends(get_job_manager)) -> RunIndexEntryModel:
    run_row = jobs.submit(run_type="runtime_backend", project_id=request.project_id, payload=request.payload)
    return RunIndexEntryModel(**_to_index_entry(run_row))


@router.post("/runs/rebridge", response_model=RunIndexEntryModel)
def launch_rebridge(request: RunLaunchRequest, jobs: JobManager = Depends(get_job_manager)) -> RunIndexEntryModel:
    run_row = jobs.submit(run_type="rebridge", project_id=request.project_id, payload=request.payload)
    return RunIndexEntryModel(**_to_index_entry(run_row))


@router.post("/runs/probe-set", response_model=RunIndexEntryModel)
def launch_probe_set(request: RunLaunchRequest, jobs: JobManager = Depends(get_job_manager)) -> RunIndexEntryModel:
    run_row = jobs.submit(run_type="probe_set", project_id=request.project_id, payload=request.payload)
    return RunIndexEntryModel(**_to_index_entry(run_row))


@router.post("/runs/closed-loop-demo", response_model=RunIndexEntryModel)
def launch_closed_loop_demo(request: RunLaunchRequest, jobs: JobManager = Depends(get_job_manager)) -> RunIndexEntryModel:
    run_row = jobs.submit(run_type="closed_loop_demo", project_id=request.project_id, payload=request.payload)
    return RunIndexEntryModel(**_to_index_entry(run_row))


@router.get("/runs/{run_id}", response_model=RunDetailModel)
def get_run(run_id: str, db: ControlPlaneDB = Depends(get_db)) -> RunDetailModel:
    row = db.get_run(run_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
    return RunDetailModel(**row)


@router.get("/runs/{run_id}/artifacts", response_model=list[RunArtifactModel])
def list_run_artifacts(run_id: str, db: ControlPlaneDB = Depends(get_db)) -> list[RunArtifactModel]:
    if db.get_run(run_id) is None:
        raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
    return [RunArtifactModel(**artifact) for artifact in db.list_run_artifacts(run_id)]


@router.get("/runs/{run_id}/status-stream")
def stream_run_status(run_id: str, db: ControlPlaneDB = Depends(get_db)) -> StreamingResponse:
    if db.get_run(run_id) is None:
        raise HTTPException(status_code=404, detail=f"run not found: {run_id}")

    def event_stream():
        last_payload = ""
        while True:
            row = db.get_run(run_id)
            if row is None:
                break
            payload = json.dumps(
                {
                    "run_id": row["run_id"],
                    "status": row["status"],
                    "started_at": row.get("started_at"),
                    "finished_at": row.get("finished_at"),
                    "status_reason_codes": row.get("status_reason_codes", []),
                    "recommended_next_command": row.get("recommended_next_command", ""),
                },
                ensure_ascii=True,
            )
            if payload != last_payload:
                yield f"data: {payload}\n\n"
                last_payload = payload
            if row["status"] not in {"PLANNED", "RUNNING"}:
                break
            time.sleep(1.0)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.get("/artifacts/content")
def get_artifact_content(path: str = Query(..., description="Absolute path to artifact content"), db: ControlPlaneDB = Depends(get_db)):
    artifact_path = Path(path).resolve()
    from hybrid_sensor_sim.server.app import get_repo_root

    repo_root = get_repo_root()
    allowed_by_repo = repo_root in artifact_path.parents or artifact_path == repo_root
    allowed_by_run_index = db.is_known_artifact_path(artifact_path)
    if not allowed_by_repo and not allowed_by_run_index:
        raise HTTPException(status_code=400, detail="artifact path must be inside the repository or indexed by the control plane")
    if not artifact_path.exists() or not artifact_path.is_file():
        raise HTTPException(status_code=404, detail=f"artifact not found: {artifact_path}")
    suffix = artifact_path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            payload = _normalize_json_artifact_payload(payload)
        return JSONResponse(payload)
    return PlainTextResponse(artifact_path.read_text(encoding="utf-8"))


def _to_index_entry(row: dict[str, object]) -> dict[str, object]:
    return {
        "run_id": row["run_id"],
        "run_type": row["run_type"],
        "project_id": row["project_id"],
        "source_kind": row.get("source_kind", "api_request"),
        "requested_at": row["requested_at"],
        "started_at": row.get("started_at"),
        "finished_at": row.get("finished_at"),
        "status": row["status"],
        "artifact_root": row.get("artifact_root", ""),
        "summary_json_path": row.get("summary_json_path", ""),
        "summary_markdown_path": row.get("summary_markdown_path", ""),
        "recommended_next_command": row.get("recommended_next_command", ""),
    }


def _normalize_json_artifact_payload(payload: dict[str, object]) -> dict[str, object]:
    normalized = dict(payload)
    status = normalized.get("status")
    if isinstance(status, str):
        lowered = status.strip().lower()
        legacy_map = {
            "success": "SUCCEEDED",
            "succeeded": "SUCCEEDED",
            "pass": "READY",
            "failed": "FAILED",
            "fail": "FAILED",
            "error": "FAILED",
            "ready": "READY",
            "degraded": "DEGRADED",
            "attention": "ATTENTION",
            "blocked": "BLOCKED",
            "planned": "PLANNED",
            "running": "RUNNING",
        }
        normalized["status"] = legacy_map.get(lowered, status)
    return normalized
