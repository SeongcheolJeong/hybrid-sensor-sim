import type {
  AutowareBundleSummaryModel,
  HistorySummaryModel,
  ProjectModel,
  RunArtifactModel,
  RunDetailModel,
  RunIndexEntryModel,
  RuntimeStrategySummaryModel,
  ScenarioAssetModel,
} from "./types";

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000/api/v1";

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    ...init,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed: ${response.status}`);
  }
  return (await response.json()) as T;
}

async function requestArtifact(path: string): Promise<Record<string, unknown> | string> {
  const response = await fetch(`${API_BASE}/artifacts/content?path=${encodeURIComponent(path)}`);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed: ${response.status}`);
  }
  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    return (await response.json()) as Record<string, unknown>;
  }
  return await response.text();
}

export function listProjects() {
  return requestJson<ProjectModel[]>("/projects");
}

export function createProject(payload: Record<string, unknown>) {
  return requestJson<ProjectModel>("/projects", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function listScenarios() {
  return requestJson<ScenarioAssetModel[]>("/scenarios");
}

export function listRuns() {
  return requestJson<RunIndexEntryModel[]>("/runs");
}

export function getRun(runId: string) {
  return requestJson<RunDetailModel>(`/runs/${runId}`);
}

export function getRunArtifacts(runId: string) {
  return requestJson<RunArtifactModel[]>(`/runs/${runId}/artifacts`);
}

export function getArtifactContent(path: string) {
  return requestArtifact(path);
}

export function getRuntimeStrategySummary() {
  return requestJson<RuntimeStrategySummaryModel>("/runtime/strategy-summary");
}

export function getHistorySummary() {
  return requestJson<HistorySummaryModel>("/history/summary");
}

export function getAutowareBundle(runId: string) {
  return requestJson<AutowareBundleSummaryModel>(`/autoware/${runId}/bundle`);
}

function postRun(path: string, payload: Record<string, unknown>) {
  const requestBody = {
    project_id: typeof payload.project_id === "string" ? payload.project_id : "default",
    payload: (() => {
      const normalized = { ...payload };
      delete normalized.project_id;
      return normalized;
    })(),
  };
  return requestJson<RunIndexEntryModel>(path, {
    method: "POST",
    body: JSON.stringify(requestBody),
  });
}

export function createObjectSimRun(payload: Record<string, unknown>) {
  return postRun("/runs/object-sim", payload);
}

export function createBatchWorkflowRun(payload: Record<string, unknown>) {
  return postRun("/runs/batch-workflow", payload);
}

export function createBackendSmokeRun(payload: Record<string, unknown>) {
  return postRun("/runs/backend-smoke", payload);
}

export function createRuntimeBackendRun(payload: Record<string, unknown>) {
  return postRun("/runs/runtime-backend", payload);
}

export function createRebridgeRun(payload: Record<string, unknown>) {
  return postRun("/runs/rebridge", payload);
}

export function createProbeSetRun(payload: Record<string, unknown>) {
  return postRun("/runs/probe-set", payload);
}

export function createClosedLoopDemoRun(payload: Record<string, unknown>) {
  return postRun("/runs/closed-loop-demo", payload);
}

export function createStatusEventSource(runId: string) {
  return new EventSource(`${API_BASE}/runs/${runId}/status-stream`);
}
