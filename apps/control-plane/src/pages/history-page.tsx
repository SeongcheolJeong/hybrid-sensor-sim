import { useQuery } from "@tanstack/react-query";

import { JsonViewer } from "../components/json-viewer";
import { MetricCard } from "../components/metric-card";
import { Panel } from "../components/panel";
import { getHistorySummary } from "../lib/api";

export function HistoryPage() {
  const historySummary = useQuery({ queryKey: ["history-summary"], queryFn: getHistorySummary });

  return (
    <div className="space-y-6">
      <section className="grid gap-4 xl:grid-cols-4">
        <MetricCard label="Projects" value={historySummary.data?.project_count ?? 0} hint="Autonomy-E2E project inventory entries" />
        <MetricCard label="Blocks" value={historySummary.data?.block_count ?? 0} hint="Migration registry blocks" />
        <MetricCard label="Migrated" value={historySummary.data?.migration_status_counts?.migrated ?? 0} hint="Implemented capability blocks" />
        <MetricCard
          label="Warnings"
          value={Array.isArray(historySummary.data?.refresh_report?.warnings) ? historySummary.data.refresh_report.warnings.length : 0}
          hint={String(historySummary.data?.refresh_report?.history_refresh_report_path ?? "No refresh report path")}
        />
      </section>

      <Panel title="History Summary" subtitle="Current repo to historical source traceability and latest refresh warnings.">
        <JsonViewer value={historySummary.data ?? { message: "No history summary returned." }} />
      </Panel>
    </div>
  );
}
