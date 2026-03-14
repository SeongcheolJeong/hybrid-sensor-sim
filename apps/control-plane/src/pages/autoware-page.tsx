import { useMemo, useState } from "react";
import { useQueries, useQuery } from "@tanstack/react-query";

import { JsonViewer } from "../components/json-viewer";
import { MetricCard } from "../components/metric-card";
import { Panel } from "../components/panel";
import { StatusChip } from "../components/status-chip";
import { getArtifactContent, getAutowareBundle, listRuns } from "../lib/api";
import type { RunIndexEntryModel } from "../lib/types";

function pickCandidateRun(runs: RunIndexEntryModel[]) {
  const preferred = runs.find((run) => run.run_type === "runtime_backend");
  return preferred ?? runs[0] ?? null;
}

export function AutowarePage() {
  const runs = useQuery({ queryKey: ["runs"], queryFn: listRuns });
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);

  const candidateRun = useMemo(() => {
    const runtimeRuns = (runs.data ?? []).filter((run) => ["runtime_backend", "backend_smoke", "rebridge"].includes(run.run_type));
    return runtimeRuns.find((run) => run.run_id === selectedRunId) ?? pickCandidateRun(runtimeRuns);
  }, [runs.data, selectedRunId]);

  const bundle = useQuery({
    queryKey: ["autoware-bundle", candidateRun?.run_id],
    queryFn: () => getAutowareBundle(candidateRun!.run_id),
    enabled: Boolean(candidateRun?.run_id),
  });

  const artifactQueries = useQueries({
    queries: [
      { queryKey: ["autoware-topic-catalog", bundle.data?.topic_catalog_path], queryFn: () => getArtifactContent(bundle.data!.topic_catalog_path!), enabled: Boolean(bundle.data?.topic_catalog_path) },
      { queryKey: ["autoware-consumer-manifest", bundle.data?.consumer_input_manifest_path], queryFn: () => getArtifactContent(bundle.data!.consumer_input_manifest_path!), enabled: Boolean(bundle.data?.consumer_input_manifest_path) },
    ],
  });

  const consumerManifest = artifactQueries[1].data;
  const processingStages = Array.isArray((consumerManifest as { processing_stages?: unknown[] } | undefined)?.processing_stages)
    ? (((consumerManifest as { processing_stages?: unknown[] }).processing_stages ?? []) as Record<string, unknown>[])
    : [];
  const degradedStages = processingStages.filter((stage) => String(stage.status ?? "").toUpperCase() === "DEGRADED");
  const readyStages = processingStages.filter((stage) => String(stage.status ?? "").toUpperCase() === "READY");

  return (
    <div className="space-y-6">
      <Panel title="Autoware Bundle" subtitle="Topic catalog, frame tree, processing stages, and consumer input contracts built from runtime smoke outputs.">
        <div className="mb-4 flex flex-wrap items-center gap-3">
          <label className="text-xs uppercase tracking-[0.18em] text-cp-text-muted">Selected run</label>
          <select
            className="rounded-full border border-cp-border bg-cp-surface/80 px-4 py-2 text-sm text-cp-text"
            value={candidateRun?.run_id ?? ""}
            onChange={(event) => setSelectedRunId(event.target.value || null)}
          >
            {(runs.data ?? []).filter((run) => ["runtime_backend", "backend_smoke", "rebridge"].includes(run.run_type)).map((run) => (
              <option key={run.run_id} value={run.run_id}>{run.run_id}</option>
            ))}
          </select>
          {bundle.data ? <StatusChip status={bundle.data.status} /> : null}
        </div>
        <div className="grid gap-4 xl:grid-cols-4">
          <MetricCard label="Consumer profile" value={bundle.data?.consumer_profile ?? "N/A"} hint={bundle.data?.status ?? "No bundle"} />
          <MetricCard label="Topics" value={bundle.data?.available_topics.length ?? 0} hint={`${bundle.data?.missing_required_topics.length ?? 0} missing required`} />
          <MetricCard label="Stages" value={processingStages.length} hint={`${readyStages.length} ready / ${degradedStages.length} degraded`} />
          <MetricCard label="Payloads" value={bundle.data ? Object.keys(bundle.data.payloads ?? {}).length : 0} hint={bundle.data?.pipeline_manifest_path ?? "No pipeline manifest"} />
        </div>
      </Panel>

      <div className="grid gap-6 xl:grid-cols-[1fr_1fr]">
        <Panel title="Processing Stages" subtitle="Stage-level readiness for downstream Autoware-style ingest consumers.">
          <div className="space-y-3">
            {processingStages.length === 0 ? (
              <div className="rounded-2xl border border-cp-border bg-cp-surface/60 p-4 text-sm text-cp-text-muted">
                Select a run with a consumer input manifest to inspect stage readiness.
              </div>
            ) : null}
            {processingStages.map((stage) => (
              <div key={String(stage.stage_id ?? stage.name ?? JSON.stringify(stage))} className="rounded-2xl border border-cp-border bg-cp-surface/60 p-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="text-xs uppercase tracking-[0.18em] text-cp-text-muted">{String(stage.stage_id ?? "stage")}</div>
                    <div className="mt-1 font-semibold text-cp-text">{String(stage.name ?? stage.stage_id ?? "Unnamed stage")}</div>
                  </div>
                  <StatusChip status={String(stage.status ?? "UNKNOWN")} />
                </div>
                <div className="mt-3 grid gap-3 md:grid-cols-2">
                  <div className="text-sm text-cp-text-muted">
                    Required topics: {Array.isArray(stage.required_topics) ? stage.required_topics.length : 0}
                  </div>
                  <div className="text-sm text-cp-text-muted">
                    Missing topics: {Array.isArray(stage.missing_required_topics) ? stage.missing_required_topics.length : 0}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </Panel>
        <Panel title="Topic Catalog" subtitle="Autoware-facing topics and their availability/origin.">
          <JsonViewer value={artifactQueries[0].data ?? { message: "Select a run with a topic catalog artifact." }} />
        </Panel>
      </div>

      <div className="grid gap-6 xl:grid-cols-[1fr_1fr]">
        <Panel title="Consumer Input Manifest" subtitle="Processing-stage grouped downstream ingest contract.">
          <JsonViewer value={artifactQueries[1].data ?? { message: "Select a run with a consumer input manifest artifact." }} />
        </Panel>
        <Panel title="Bundle Payloads" subtitle="Pipeline and dataset manifests returned by the Autoware bundle summary API.">
          <JsonViewer value={bundle.data?.payloads ?? { message: "No Autoware bundle payloads returned yet." }} />
        </Panel>
      </div>
    </div>
  );
}
