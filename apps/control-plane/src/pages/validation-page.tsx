import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";

import { DataTable, createColumnHelper } from "../components/data-table";
import { Panel } from "../components/panel";
import { RunLaunchDialog } from "../components/run-launch-dialog";
import { StatusChip } from "../components/status-chip";
import { DEFAULT_BATCH_WORKFLOW_PAYLOAD, DEFAULT_PROBE_SET_PAYLOAD } from "../lib/defaults";
import { createBatchWorkflowRun, createProbeSetRun, listRuns } from "../lib/api";
import type { RunIndexEntryModel } from "../lib/types";

const runColumnHelper = createColumnHelper<RunIndexEntryModel>();

const columns = [
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
  runColumnHelper.accessor("recommended_next_command", { header: "Next action", cell: (info) => <span className="font-mono text-xs text-cp-text-muted">{info.getValue() ?? "-"}</span> }),
];

export function ValidationPage() {
  const queryClient = useQueryClient();
  const runs = useQuery({ queryKey: ["runs"], queryFn: listRuns });
  const validationRuns = (runs.data ?? []).filter((run) => run.run_type === "batch_workflow" || run.run_type === "probe_set");

  const batchMutation = useMutation({
    mutationFn: createBatchWorkflowRun,
    onSuccess: async () => queryClient.invalidateQueries({ queryKey: ["runs"] }),
  });
  const probeMutation = useMutation({
    mutationFn: createProbeSetRun,
    onSuccess: async () => queryClient.invalidateQueries({ queryKey: ["runs"] }),
  });

  return (
    <div className="space-y-6">
      <Panel
        title="Validation Runs"
        subtitle="Batch workflows and probe sets share the same local job model and feed the runtime readiness summaries."
        action={
          <div className="flex gap-3">
            <RunLaunchDialog title="Launch Batch Workflow" defaultPayload={DEFAULT_BATCH_WORKFLOW_PAYLOAD} onSubmit={(payload) => batchMutation.mutateAsync(payload)} />
            <RunLaunchDialog title="Launch Probe Set" defaultPayload={DEFAULT_PROBE_SET_PAYLOAD} onSubmit={(payload) => probeMutation.mutateAsync(payload)} />
          </div>
        }
      >
        <DataTable data={validationRuns} columns={columns} />
      </Panel>
    </div>
  );
}
