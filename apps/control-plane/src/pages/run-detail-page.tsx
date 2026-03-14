import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useParams } from "@tanstack/react-router";

import { DataTable, createColumnHelper } from "../components/data-table";
import { JsonViewer } from "../components/json-viewer";
import { MetricCard } from "../components/metric-card";
import { Panel } from "../components/panel";
import { StatusChip } from "../components/status-chip";
import { createStatusEventSource, getArtifactContent, getRun, getRunArtifacts } from "../lib/api";
import type { RunArtifactModel } from "../lib/types";

const artifactColumnHelper = createColumnHelper<RunArtifactModel>();
const artifactColumns = [
  artifactColumnHelper.accessor("display_name", { header: "Artifact" }),
  artifactColumnHelper.accessor("artifact_type", { header: "Type" }),
  artifactColumnHelper.accessor("path", { header: "Path", cell: (info) => <span className="font-mono text-xs text-cp-text-muted">{info.getValue()}</span> }),
];

export function RunDetailPage() {
  const { runId } = useParams({ from: "/runs/$runId" });
  const [selectedArtifactPath, setSelectedArtifactPath] = useState<string | null>(null);
  const [streamEvents, setStreamEvents] = useState<string[]>([]);

  const runDetail = useQuery({ queryKey: ["run", runId], queryFn: () => getRun(runId) });
  const artifacts = useQuery({ queryKey: ["run-artifacts", runId], queryFn: () => getRunArtifacts(runId) });
  const selectedArtifact = useMemo(() => artifacts.data?.find((artifact) => artifact.path === selectedArtifactPath) ?? artifacts.data?.[0] ?? null, [artifacts.data, selectedArtifactPath]);
  const artifactContent = useQuery({
    queryKey: ["artifact-content", selectedArtifact?.path],
    queryFn: () => getArtifactContent(selectedArtifact!.path),
    enabled: Boolean(selectedArtifact?.path),
  });
  const closedLoopReport =
    runDetail.data?.run_type === "closed_loop_demo" &&
    typeof runDetail.data.result_payload.report === "object" &&
    runDetail.data.result_payload.report !== null
      ? (runDetail.data.result_payload.report as Record<string, unknown>)
      : null;
  const closedLoopSummary =
    closedLoopReport && typeof closedLoopReport.status_summary === "object" && closedLoopReport.status_summary !== null
      ? (closedLoopReport.status_summary as Record<string, unknown>)
      : null;
  const closedLoopCapture =
    closedLoopReport && typeof closedLoopReport.capture === "object" && closedLoopReport.capture !== null
      ? (closedLoopReport.capture as Record<string, unknown>)
      : null;
  const closedLoopVideos = Array.isArray(closedLoopCapture?.video_paths) ? (closedLoopCapture?.video_paths as string[]) : [];

  useEffect(() => {
    const source = createStatusEventSource(runId);
    source.onmessage = (event) => {
      setStreamEvents((current) => [`${new Date().toISOString()} ${event.data}`, ...current].slice(0, 20));
    };
    return () => source.close();
  }, [runId]);

  return (
    <div className="space-y-6">
      <Panel title="Run Detail" subtitle={runDetail.data?.run_type ?? "Loading run metadata..."}>
        <div className="mb-4 flex items-center gap-3">
          <span className="font-mono text-sm text-cp-text-muted">{runId}</span>
          {runDetail.data ? <StatusChip status={runDetail.data.status} /> : null}
        </div>
        <div className="grid gap-4 xl:grid-cols-4">
          <MetricCard label="Type" value={runDetail.data?.run_type ?? "-"} />
          <MetricCard label="Artifacts" value={artifacts.data?.length ?? 0} />
          <MetricCard label="Requested" value={runDetail.data?.requested_at ?? "-"} />
          <MetricCard label="Next action" value={runDetail.data?.recommended_next_command ?? "-"} />
        </div>
      </Panel>

      {closedLoopSummary ? (
        <Panel title="Closed-Loop Summary" subtitle="Live loop readiness, vehicle motion, and capture status surfaced by the closed-loop workflow.">
          <div className="grid gap-4 xl:grid-cols-4">
            <MetricCard label="AWSIM launch" value={String(closedLoopSummary.awsim_launch_ready ? "READY" : "BLOCKED")} />
            <MetricCard label="Autoware launch" value={String(closedLoopSummary.autoware_launch_ready ? "READY" : "BLOCKED")} />
            <MetricCard label="Control loop" value={String(closedLoopSummary.control_ready ? "READY" : "DEGRADED")} hint={`Perception ${closedLoopSummary.perception_ready ? "READY" : "BLOCKED"} / Planning ${closedLoopSummary.planning_ready ? "READY" : "BLOCKED"}`} />
            <MetricCard label="Vehicle motion" value={String(closedLoopSummary.vehicle_motion_confirmed ? "CONFIRMED" : "MISSING")} hint={`Route completed: ${closedLoopSummary.route_completed ? "yes" : "no"}`} />
          </div>
          <div className="mt-4 grid gap-4 xl:grid-cols-[0.9fr_1.1fr]">
            <JsonViewer
              value={{
                missing_required_topics: closedLoopSummary.missing_required_topics ?? [],
                degraded_processing_stage_ids: closedLoopSummary.degraded_processing_stage_ids ?? [],
                capture_ready: closedLoopSummary.capture_ready,
                video_path_count: closedLoopSummary.video_path_count,
              }}
            />
            <JsonViewer value={{ video_paths: closedLoopVideos, rosbag_path: closedLoopCapture?.rosbag_path ?? "" }} />
          </div>
        </Panel>
      ) : null}

      <div className="grid gap-6 xl:grid-cols-[0.95fr_1.05fr]">
        <Panel title="Artifacts" subtitle="Select an artifact to inspect its JSON or markdown content.">
          <DataTable data={artifacts.data ?? []} columns={artifactColumns} />
          <div className="mt-4 flex flex-wrap gap-2">
            {(artifacts.data ?? []).map((artifact) => (
              <button
                key={artifact.path}
                className={`rounded-full border px-3 py-1.5 text-xs ${selectedArtifact?.path === artifact.path ? "border-cp-accent/40 bg-cp-accent/15 text-cp-accent" : "border-cp-border text-cp-text-muted"}`}
                onClick={() => setSelectedArtifactPath(artifact.path)}
              >
                {artifact.display_name}
              </button>
            ))}
          </div>
        </Panel>

        <Panel title="Artifact Inspector" subtitle={selectedArtifact?.path ?? "Select an artifact from the list."}>
          <JsonViewer value={artifactContent.data ?? runDetail.data?.result_payload ?? { message: "No artifact selected." }} />
        </Panel>
      </div>

      <Panel title="Status Stream" subtitle="SSE events from the backend job runner.">
        <ul className="space-y-2 rounded-2xl border border-cp-border bg-cp-surface/60 p-4 font-mono text-xs text-cp-text-muted">
          {streamEvents.length === 0 ? <li>No SSE events yet.</li> : null}
          {streamEvents.map((event) => (
            <li key={event}>{event}</li>
          ))}
        </ul>
      </Panel>
    </div>
  );
}
