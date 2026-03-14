import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";

import { DataTable, createColumnHelper } from "../components/data-table";
import { JsonViewer } from "../components/json-viewer";
import { MetricCard } from "../components/metric-card";
import { Panel } from "../components/panel";
import { RunLaunchDialog } from "../components/run-launch-dialog";
import { StatusChip } from "../components/status-chip";
import { DEFAULT_BACKEND_SMOKE_PAYLOAD, DEFAULT_CLOSED_LOOP_DEMO_PAYLOAD, DEFAULT_PROBE_SET_PAYLOAD, DEFAULT_REBRIDGE_PAYLOAD, DEFAULT_RUNTIME_BACKEND_PAYLOAD } from "../lib/defaults";
import { createBackendSmokeRun, createClosedLoopDemoRun, createProbeSetRun, createRebridgeRun, createRuntimeBackendRun, getRuntimeStrategySummary, listRuns } from "../lib/api";
import type { RunIndexEntryModel } from "../lib/types";

const columnHelper = createColumnHelper<RunIndexEntryModel>();
const columns = [
  columnHelper.accessor("run_id", {
    header: "Run",
    cell: (info) => (
      <Link to="/runs/$runId" params={{ runId: info.getValue() }} className="font-mono text-xs text-cp-accent hover:underline">
        {info.getValue()}
      </Link>
    ),
  }),
  columnHelper.accessor("run_type", { header: "Type" }),
  columnHelper.accessor("status", { header: "Status", cell: (info) => <StatusChip status={info.getValue()} /> }),
  columnHelper.accessor("recommended_next_command", { header: "Recommended action", cell: (info) => <span className="font-mono text-xs text-cp-text-muted">{info.getValue() ?? "-"}</span> }),
];

export function RuntimePage() {
  const queryClient = useQueryClient();
  const runtimeSummary = useQuery({ queryKey: ["runtime-strategy-summary"], queryFn: getRuntimeStrategySummary });
  const runs = useQuery({ queryKey: ["runs"], queryFn: listRuns });
  const runtimeRuns = (runs.data ?? []).filter((run) => ["backend_smoke", "runtime_backend", "rebridge", "probe_set", "closed_loop_demo"].includes(run.run_type));
  const closedLoopRuns = runtimeRuns.filter((run) => run.run_type === "closed_loop_demo");
  const backendMap = new Map((runtimeSummary.data?.backends ?? []).map((backend) => [backend.backend, backend]));
  const blockers = runtimeSummary.data?.blockers ?? [];
  const probeSets = runtimeSummary.data?.probe_sets ?? [];

  const backendSmokeMutation = useMutation({ mutationFn: createBackendSmokeRun, onSuccess: async () => queryClient.invalidateQueries({ queryKey: ["runs"] }) });
  const runtimeBackendMutation = useMutation({ mutationFn: createRuntimeBackendRun, onSuccess: async () => queryClient.invalidateQueries({ queryKey: ["runs"] }) });
  const rebridgeMutation = useMutation({ mutationFn: createRebridgeRun, onSuccess: async () => queryClient.invalidateQueries({ queryKey: ["runs"] }) });
  const probeMutation = useMutation({ mutationFn: createProbeSetRun, onSuccess: async () => queryClient.invalidateQueries({ queryKey: ["runs"] }) });
  const closedLoopMutation = useMutation({ mutationFn: createClosedLoopDemoRun, onSuccess: async () => queryClient.invalidateQueries({ queryKey: ["runs"] }) });

  return (
    <div className="space-y-6">
      <section className="grid gap-4 xl:grid-cols-4">
        <MetricCard
          label="Recommended action"
          value={runtimeSummary.data?.recommended_next_command ?? "N/A"}
          hint={String(blockers[0]?.reason_code ?? "No blocker code")}
        />
        <MetricCard
          label="AWSIM"
          value={backendMap.get("awsim")?.strategy ?? "Unknown"}
          hint={backendMap.get("awsim")?.preferred_runtime_source ?? "No AWSIM strategy"}
        />
        <MetricCard
          label="CARLA"
          value={backendMap.get("carla")?.strategy ?? "Unknown"}
          hint={backendMap.get("carla")?.preferred_runtime_source ?? "No CARLA strategy"}
        />
        <MetricCard
          label="Closed loop demos"
          value={closedLoopRuns.length}
          hint={closedLoopRuns[0]?.status ?? `${probeSets.length} probe sets indexed`}
        />
      </section>

      <Panel
        title="Runtime Workflows"
        subtitle="Launch backend smoke, runtime backend workflows, rebridge refreshes, closed-loop demos, and probe sets from one page."
        action={
          <div className="flex flex-wrap gap-3">
            <RunLaunchDialog title="Launch Backend Smoke" defaultPayload={DEFAULT_BACKEND_SMOKE_PAYLOAD} onSubmit={(payload) => backendSmokeMutation.mutateAsync(payload)} />
            <RunLaunchDialog title="Launch Runtime Backend Workflow" defaultPayload={DEFAULT_RUNTIME_BACKEND_PAYLOAD} onSubmit={(payload) => runtimeBackendMutation.mutateAsync(payload)} />
            <RunLaunchDialog title="Launch Rebridge" defaultPayload={DEFAULT_REBRIDGE_PAYLOAD} onSubmit={(payload) => rebridgeMutation.mutateAsync(payload)} />
            <RunLaunchDialog title="Launch Closed-Loop Demo" defaultPayload={DEFAULT_CLOSED_LOOP_DEMO_PAYLOAD} onSubmit={(payload) => closedLoopMutation.mutateAsync(payload)} />
            <RunLaunchDialog title="Launch Probe Set" defaultPayload={DEFAULT_PROBE_SET_PAYLOAD} onSubmit={(payload) => probeMutation.mutateAsync(payload)} />
          </div>
        }
      >
        <DataTable data={runtimeRuns} columns={columns} />
      </Panel>

      <div className="grid gap-6 xl:grid-cols-[1fr_1fr]">
        <Panel title="Backend Strategies" subtitle="Current backend runtime strategy summary from the control-plane API.">
          <JsonViewer value={runtimeSummary.data?.backends ?? []} />
        </Panel>
        <Panel title="Blocking Reasons" subtitle="This is the same runtime blocker surface used to drive recommended commands and plans.">
          <JsonViewer value={blockers} />
        </Panel>
      </div>

      <Panel title="Probe Sets" subtitle="Read-only runtime health probe-set summaries indexed by the backend API.">
        <JsonViewer value={probeSets} />
      </Panel>

      <Panel title="Closed-Loop Demo Runs" subtitle="AWSIM + Autoware orchestration attempts, including blockers, recommended commands, and video artifacts.">
        <DataTable data={closedLoopRuns} columns={columns} />
      </Panel>
    </div>
  );
}
