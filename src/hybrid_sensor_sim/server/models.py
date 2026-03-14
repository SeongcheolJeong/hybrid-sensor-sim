from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

RunStatus = Literal[
    "SUCCEEDED",
    "READY",
    "DEGRADED",
    "ATTENTION",
    "FAILED",
    "BLOCKED",
    "PLANNED",
    "RUNNING",
]


class ProjectCreateRequest(BaseModel):
    name: str = Field(..., min_length=1)
    description: str = ""
    root_path: str = ""


class ProjectModel(BaseModel):
    schema_version: str = "project_v0"
    project_id: str
    name: str
    description: str = ""
    root_path: str = ""
    created_at: str


class ScenarioAssetModel(BaseModel):
    schema_version: str = "scenario_asset_v0"
    asset_id: str
    name: str
    path: str
    asset_kind: str
    source_group: str
    schema_version_hint: str = ""
    description: str = ""


class RunLaunchRequest(BaseModel):
    project_id: str = "default"
    payload: dict[str, Any] = Field(default_factory=dict)


class RunArtifactModel(BaseModel):
    artifact_id: int
    run_id: str
    artifact_type: str
    path: str
    mime_type: str
    display_name: str
    created_at: str


class RunIndexEntryModel(BaseModel):
    schema_version: str = "run_index_entry_v0"
    run_id: str
    run_type: str
    project_id: str
    source_kind: str
    requested_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    status: str
    artifact_root: str
    summary_json_path: str = ""
    summary_markdown_path: str = ""
    recommended_next_command: str = ""


class RunDetailModel(BaseModel):
    schema_version: str = "run_detail_v0"
    run_id: str
    run_type: str
    project_id: str
    source_kind: str
    requested_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    status: str
    status_reason_codes: list[str] = Field(default_factory=list)
    artifact_root: str
    summary_json_path: str = ""
    summary_markdown_path: str = ""
    recommended_next_command: str = ""
    request_payload: dict[str, Any] = Field(default_factory=dict)
    result_payload: dict[str, Any] = Field(default_factory=dict)
    error_message: str = ""


class RuntimeStrategySummaryModel(BaseModel):
    schema_version: str = "runtime_strategy_summary_v0"
    generated_at: str
    local_setup_path: str = ""
    backends: list[dict[str, Any]] = Field(default_factory=list)
    blockers: list[dict[str, Any]] = Field(default_factory=list)
    probe_sets: list[dict[str, Any]] = Field(default_factory=list)
    recommended_next_command: str = ""


class AutowareBundleSummaryModel(BaseModel):
    schema_version: str = "autoware_bundle_summary_v0"
    run_id: str
    status: str
    available_topics: list[str] = Field(default_factory=list)
    missing_required_topics: list[str] = Field(default_factory=list)
    consumer_profile: str = ""
    pipeline_manifest_path: str = ""
    dataset_manifest_path: str = ""
    topic_catalog_path: str = ""
    consumer_input_manifest_path: str = ""
    payloads: dict[str, Any] = Field(default_factory=dict)
