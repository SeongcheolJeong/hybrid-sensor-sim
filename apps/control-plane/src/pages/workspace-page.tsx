import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";

import { DataTable, createColumnHelper } from "../components/data-table";
import { MetricCard } from "../components/metric-card";
import { Panel } from "../components/panel";
import { StatusBarChart } from "../components/status-bar-chart";
import { StatusChip } from "../components/status-chip";
import { getHistorySummary, getRuntimeStrategySummary, listRuns } from "../lib/api";
import type { RunIndexEntryModel } from "../lib/types";

const runColumnHelper = createColumnHelper<RunIndexEntryModel>();

const runColumns = [
  runColumnHelper.accessor("run_id", {
    header: "Run",
    cell: (info) => (
      <Link to="/runs/$runId" params={{ runId: info.getValue() }} className="font-mono text-xs text-cp-accent hover:underline">
        {info.getValue()}
      </Link>
    ),
  }),
  runColumnHelper.accessor("run_type", { header: "Type" }),
  runColumnHelper.accessor("status", { header: "Status", cell: (info) => <StatusChip status={info.getValue()} /> }),
  runColumnHelper.accessor("recommended_next_command", {
    header: "Next action",
    cell: (info) => <span className="font-mono text-xs text-cp-text-muted">{info.getValue() ?? "-"}</span>,
  }),
];

export function WorkspacePage() {
  const runtimeSummary = useQuery({ queryKey: ["runtime-strategy-summary"], queryFn: getRuntimeStrategySummary });
  const historySummary = useQuery({ queryKey: ["history-summary"], queryFn: getHistorySummary });
  const runs = useQuery({ queryKey: ["runs"], queryFn: listRuns });

  const backendMap = new Map((runtimeSummary.data?.backends ?? []).map((backend) => [String(backend.backend).toLowerCase(), backend]));
  const awsimBackend = backendMap.get("awsim");
  const carlaBackend = backendMap.get("carla");
  const migratedBlockCount = historySummary.data?.migration_status_counts?.migrated ?? 0;
  const latestClosedLoopRun = (runs.data ?? []).find((run) => run.run_type === "closed_loop_demo");

  const statusCounts = (runs.data ?? []).reduce<Record<string, number>>((counts, run) => {
    counts[run.status] = (counts[run.status] ?? 0) + 1;
    return counts;
  }, {});

  return (
    <div className="space-y-6">
      <section className="grid gap-4 xl:grid-cols-5">
        <MetricCard label="Runs" value={runs.data?.length ?? 0} hint="Indexed runs in the local control-plane DB" />
        <MetricCard
          label="AWSIM Runtime"
          value={awsimBackend?.host_compatible || awsimBackend?.readiness === true ? "READY" : "BLOCKED"}
          hint={String(awsimBackend?.strategy ?? awsimBackend?.preferred_runtime_source ?? "No runtime summary yet")}
        />
        <MetricCard
          label="CARLA Runtime"
          value={carlaBackend?.host_compatible || carlaBackend?.readiness === true ? "READY" : "BLOCKED"}
          hint={String(carlaBackend?.strategy ?? "Pending local runtime")}
        />
        <MetricCard
          label="AWSIM Closed Loop"
          value={latestClosedLoopRun?.status ?? "PLANNED"}
          hint={latestClosedLoopRun?.recommended_next_command ?? "No closed-loop demo run indexed yet"}
        />
        <MetricCard label="History Blocks" value={historySummary.data?.block_count ?? 0} hint={`${migratedBlockCount} migrated blocks tracked in provenance`} />
      </section>

      <div className="grid gap-6 xl:grid-cols-[1.15fr_0.85fr]">
        <Panel title="Runtime Strategy Summary" subtitle="Current runtime readiness and blockers for AWSIM and CARLA.">
          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-3 rounded-2xl border border-cp-border bg-cp-surface/70 p-4">
              <div className="text-xs uppercase tracking-[0.18em] text-cp-text-muted">Primary runtime strategy</div>
              <div className="text-lg font-semibold">{String(awsimBackend?.strategy ?? carlaBackend?.strategy ?? "Unavailable")}</div>
              <div className="text-sm text-cp-text-muted">Backends, blockers, and probe sets are indexed directly from the local strategy summary.</div>
            </div>
            <div className="space-y-3 rounded-2xl border border-cp-border bg-cp-surface/70 p-4">
              <div className="text-xs uppercase tracking-[0.18em] text-cp-text-muted">Recommended action</div>
              <div className="font-mono text-xs text-cp-text">{runtimeSummary.data?.recommended_next_command ?? "No command available"}</div>
            </div>
          </div>
          <div className="mt-4 grid gap-3 md:grid-cols-2">
            {(runtimeSummary.data?.blockers ?? []).map((row, index) => (
              <div key={`${row.code ?? index}`} className="rounded-2xl border border-cp-border bg-cp-surface/60 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-cp-text-muted">{String(row.category ?? "unknown")}</div>
                <div className="mt-2 font-semibold text-cp-text">{String(row.code ?? "N/A")}</div>
                <div className="mt-2 text-sm text-cp-text-muted">{String(row.action ?? "No action recorded")}</div>
              </div>
            ))}
          </div>
        </Panel>

        <Panel title="Run Status Distribution" subtitle="Indexed local runs grouped by status.">
          <StatusBarChart
            title="Run statuses"
            rows={Object.entries(statusCounts).map(([label, value]) => ({ label, value }))}
          />
        </Panel>
      </div>

      <Panel title="Recent Runs" subtitle="Newest indexed runs across simulation, validation, runtime, and probe workflows.">
        <DataTable data={(runs.data ?? []).slice(0, 10)} columns={runColumns} />
      </Panel>
    </div>
  );
}
