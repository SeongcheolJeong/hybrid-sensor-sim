import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { DataTable, createColumnHelper } from "../components/data-table";
import { Panel } from "../components/panel";
import { createProject, listProjects, listScenarios } from "../lib/api";
import type { ProjectModel, ScenarioAssetModel } from "../lib/types";

const projectColumnHelper = createColumnHelper<ProjectModel>();
const scenarioColumnHelper = createColumnHelper<ScenarioAssetModel>();

const projectColumns = [
  projectColumnHelper.accessor("project_id", { header: "Project ID", cell: (info) => <span className="font-mono text-xs">{info.getValue()}</span> }),
  projectColumnHelper.accessor("name", { header: "Name" }),
  projectColumnHelper.accessor("description", { header: "Description" }),
  projectColumnHelper.accessor("root_path", { header: "Root Path", cell: (info) => <span className="font-mono text-xs text-cp-text-muted">{info.getValue()}</span> }),
];

const scenarioColumns = [
  scenarioColumnHelper.accessor("asset_id", { header: "Asset", cell: (info) => <span className="font-mono text-xs">{info.getValue()}</span> }),
  scenarioColumnHelper.accessor("name", { header: "Name" }),
  scenarioColumnHelper.accessor("asset_kind", { header: "Kind" }),
  scenarioColumnHelper.accessor("source_group", { header: "Group" }),
  scenarioColumnHelper.accessor("path", { header: "Path", cell: (info) => <span className="font-mono text-xs text-cp-text-muted">{info.getValue()}</span> }),
];

export function ProjectsPage() {
  const queryClient = useQueryClient();
  const [draft, setDraft] = useState({ name: "", description: "", root_path: "" });
  const projects = useQuery({ queryKey: ["projects"], queryFn: listProjects });
  const scenarios = useQuery({ queryKey: ["scenarios"], queryFn: listScenarios });
  const mutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => createProject(payload),
    onSuccess: async () => {
      setDraft({ name: "", description: "", root_path: "" });
      await queryClient.invalidateQueries({ queryKey: ["projects"] });
    },
  });

  return (
    <div className="space-y-6">
      <Panel title="Projects" subtitle="Control-plane project registry stored in the local SQLite index.">
        <div className="grid gap-3 md:grid-cols-[1fr_1.2fr_1.6fr_auto]">
          <input className="rounded-2xl border border-cp-border bg-cp-surface/80 px-4 py-3 text-sm" placeholder="name" value={draft.name} onChange={(event) => setDraft((state) => ({ ...state, name: event.target.value }))} />
          <input className="rounded-2xl border border-cp-border bg-cp-surface/80 px-4 py-3 text-sm" placeholder="description" value={draft.description} onChange={(event) => setDraft((state) => ({ ...state, description: event.target.value }))} />
          <input className="rounded-2xl border border-cp-border bg-cp-surface/80 px-4 py-3 text-sm" placeholder="/absolute/project/root" value={draft.root_path} onChange={(event) => setDraft((state) => ({ ...state, root_path: event.target.value }))} />
          <button
            className="rounded-full border border-cp-accent/40 bg-cp-accent/15 px-4 py-3 text-sm font-medium text-cp-accent disabled:opacity-50"
            disabled={!draft.name || !draft.root_path || mutation.isPending}
            onClick={() => mutation.mutate({
              name: draft.name,
              description: draft.description,
              root_path: draft.root_path,
            })}
          >
            {mutation.isPending ? "Saving..." : "Create"}
          </button>
        </div>
        {mutation.error ? <div className="mt-3 text-sm text-cp-danger">{String(mutation.error)}</div> : null}
        <div className="mt-5">
          <DataTable data={projects.data ?? []} columns={projectColumns} />
        </div>
      </Panel>

      <Panel title="Scenario Assets" subtitle="Indexed scenario-like assets discovered by the backend API.">
        <DataTable data={scenarios.data ?? []} columns={scenarioColumns} />
      </Panel>
    </div>
  );
}
