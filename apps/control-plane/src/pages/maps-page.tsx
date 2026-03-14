import { useQuery } from "@tanstack/react-query";

import { DataTable, createColumnHelper } from "../components/data-table";
import { Panel } from "../components/panel";
import { listScenarios } from "../lib/api";
import type { ScenarioAssetModel } from "../lib/types";

const columnHelper = createColumnHelper<ScenarioAssetModel>();
const columns = [
  columnHelper.accessor("asset_id", { header: "Asset", cell: (info) => <span className="font-mono text-xs">{info.getValue()}</span> }),
  columnHelper.accessor("name", { header: "Name" }),
  columnHelper.accessor("schema_version_hint", { header: "Schema", cell: (info) => info.getValue() ?? "-" }),
  columnHelper.accessor("path", { header: "Path", cell: (info) => <span className="font-mono text-xs text-cp-text-muted">{info.getValue()}</span> }),
];

export function MapsPage() {
  const scenarios = useQuery({ queryKey: ["scenarios"], queryFn: listScenarios });
  const mapAssets = (scenarios.data ?? []).filter((asset) => asset.path.includes("p_map_toolset") || asset.name.toLowerCase().includes("map"));

  return (
    <div className="space-y-6">
      <Panel title="Map Assets" subtitle="Current map-related fixtures and route definitions surfaced through the scenario asset index.">
        <DataTable data={mapAssets} columns={columns} />
      </Panel>
    </div>
  );
}
