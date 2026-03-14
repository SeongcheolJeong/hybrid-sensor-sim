import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";

import { DataTable, createColumnHelper } from "../components/data-table";
import { Panel } from "../components/panel";
import { RunLaunchDialog } from "../components/run-launch-dialog";
import { StatusChip } from "../components/status-chip";
import { DEFAULT_OBJECT_SIM_PAYLOAD } from "../lib/defaults";
import { createObjectSimRun, listRuns, listScenarios } from "../lib/api";
import type { RunIndexEntryModel, ScenarioAssetModel } from "../lib/types";

const runColumnHelper = createColumnHelper<RunIndexEntryModel>();
const scenarioColumnHelper = createColumnHelper<ScenarioAssetModel>();

const runColumns = [
  runColumnHelper.accessor("run_id", {
    header: "Run",
    cell: (info) => (
      <Link to="/runs/$runId" params={{ runId: info.getValue() }} className="font-mono text-xs text-cp-accent hover:underline">
        {info.getValue()}
      </Link>
    ),
  }),
  runColumnHelper.accessor("status", { header: "Status", cell: (info) => <StatusChip status={info.getValue()} /> }),
  runColumnHelper.accessor("requested_at", { header: "Requested" }),
  runColumnHelper.accessor("recommended_next_command", { header: "Next action", cell: (info) => <span className="font-mono text-xs text-cp-text-muted">{info.getValue() ?? "-"}</span> }),
];

const scenarioColumns = [
  scenarioColumnHelper.accessor("name", { header: "Name" }),
  scenarioColumnHelper.accessor("asset_kind", { header: "Kind" }),
  scenarioColumnHelper.accessor("path", { header: "Path", cell: (info) => <span className="font-mono text-xs text-cp-text-muted">{info.getValue()}</span> }),
];

export function SimulationPage() {
  const queryClient = useQueryClient();
  const runs = useQuery({ queryKey: ["runs"], queryFn: listRuns });
  const scenarios = useQuery({ queryKey: ["scenarios"], queryFn: listScenarios });
  const objectRuns = (runs.data ?? []).filter((run) => run.run_type === "object_sim");
  const launchMutation = useMutation({
    mutationFn: createObjectSimRun,
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["runs"] });
    },
  });

  return (
    <div className="space-y-6">
      <Panel
        title="Object Sim Runs"
        subtitle="Launch deterministic object-sim jobs against current scenario fixtures or custom payloads."
        action={<RunLaunchDialog title="Launch Object Sim" defaultPayload={DEFAULT_OBJECT_SIM_PAYLOAD} onSubmit={(payload) => launchMutation.mutateAsync(payload)} />}
      >
        <DataTable data={objectRuns} columns={runColumns} />
      </Panel>

      <Panel title="Scenario Library" subtitle="Current scenario-like assets available through the backend API.">
        <DataTable data={scenarios.data ?? []} columns={scenarioColumns} />
      </Panel>
    </div>
  );
}
