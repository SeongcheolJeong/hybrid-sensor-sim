export type RunStatus =
  | "SUCCEEDED"
  | "READY"
  | "DEGRADED"
  | "ATTENTION"
  | "FAILED"
  | "BLOCKED"
  | "PLANNED"
  | "RUNNING";

export interface ProjectModel {
  schema_version: string;
  project_id: string;
  name: string;
  description: string;
  root_path: string;
  created_at: string;
}

export interface ScenarioAssetModel {
  schema_version: string;
  asset_id: string;
  name: string;
  path: string;
  asset_kind: string;
  source_group: string;
  schema_version_hint?: string | null;
  description: string;
}

export interface RunArtifactModel {
  artifact_id: number;
  run_id: string;
  artifact_type: string;
  path: string;
  mime_type: string;
  display_name: string;
  created_at: string;
}

export interface RunIndexEntryModel {
  schema_version: string;
  run_id: string;
  run_type: string;
  project_id: string;
  source_kind: string;
  requested_at: string;
  started_at?: string | null;
  finished_at?: string | null;
  status: RunStatus;
  artifact_root: string;
  summary_json_path: string;
  summary_markdown_path: string;
  recommended_next_command: string;
}

export interface RunDetailModel {
  schema_version: string;
  run_id: string;
  run_type: string;
  project_id: string;
  source_kind: string;
  requested_at: string;
  started_at?: string | null;
  finished_at?: string | null;
  status: RunStatus;
  status_reason_codes: string[];
  artifact_root: string;
  summary_json_path: string;
  summary_markdown_path: string;
  recommended_next_command: string;
  request_payload: Record<string, unknown>;
  result_payload: Record<string, unknown>;
  error_message: string;
}

export interface RuntimeBackendInfo {
  backend: string;
  selected_path?: string;
  readiness?: string | boolean;
  host_compatible?: boolean;
  preferred_runtime_source?: string;
  strategy?: string;
  reason_codes?: string[];
  recommended_command?: string;
  [key: string]: unknown;
}

export interface RuntimeBlocker {
  code?: string;
  category?: string;
  action?: string;
  [key: string]: unknown;
}

export interface RuntimeProbeSetSummary {
  path: string;
  probe_set_id: string;
  status: string;
  recommended_next_command: string;
  [key: string]: unknown;
}

export interface RuntimeStrategySummaryModel {
  schema_version: string;
  generated_at: string;
  local_setup_path: string;
  backends: RuntimeBackendInfo[];
  blockers: RuntimeBlocker[];
  probe_sets: RuntimeProbeSetSummary[];
  recommended_next_command: string;
}

export interface AutowareBundleSummaryModel {
  schema_version: string;
  run_id: string;
  status: string;
  available_topics: string[];
  missing_required_topics: string[];
  consumer_profile: string;
  pipeline_manifest_path: string;
  dataset_manifest_path: string;
  topic_catalog_path: string;
  consumer_input_manifest_path: string;
  payloads: Record<string, unknown>;
}

export interface HistorySummaryModel {
  schema_version: string;
  metadata_root: string;
  project_count: number;
  block_count: number;
  migration_status_counts: Record<string, number>;
  refresh_report: Record<string, unknown>;
}
