from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from hybrid_sensor_sim.server.db import ControlPlaneDB
from hybrid_sensor_sim.server.models import ProjectCreateRequest, ProjectModel

router = APIRouter(prefix="/api/v1/projects", tags=["projects"])


def get_db() -> ControlPlaneDB:
    from hybrid_sensor_sim.server.app import get_app_db

    return get_app_db()


@router.get("", response_model=list[ProjectModel])
def list_projects(db: ControlPlaneDB = Depends(get_db)) -> list[ProjectModel]:
    return [ProjectModel(**row) for row in db.list_projects()]


@router.get("/{project_id}", response_model=ProjectModel)
def get_project(project_id: str, db: ControlPlaneDB = Depends(get_db)) -> ProjectModel:
    row = db.get_project(project_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"project not found: {project_id}")
    return ProjectModel(**row)


@router.post("", response_model=ProjectModel)
def create_project(request: ProjectCreateRequest, db: ControlPlaneDB = Depends(get_db)) -> ProjectModel:
    project_id = request.name.strip().lower().replace(" ", "-")
    row = db.create_project(
        project_id=project_id,
        name=request.name.strip(),
        description=request.description.strip(),
        root_path=request.root_path.strip(),
    )
    return ProjectModel(**row)
