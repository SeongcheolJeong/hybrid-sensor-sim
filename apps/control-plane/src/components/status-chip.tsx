import type { RunStatus } from "../lib/types";

const STATUS_CLASS: Record<string, string> = {
  SUCCEEDED: "bg-cp-success/20 text-cp-success border border-cp-success/40",
  READY: "bg-cp-success/20 text-cp-success border border-cp-success/40",
  DEGRADED: "bg-cp-warning/20 text-cp-warning border border-cp-warning/40",
  ATTENTION: "bg-cp-warning/20 text-cp-warning border border-cp-warning/40",
  FAILED: "bg-cp-danger/20 text-cp-danger border border-cp-danger/40",
  BLOCKED: "bg-cp-danger/20 text-cp-danger border border-cp-danger/40",
  PLANNED: "bg-cp-accent/20 text-cp-accent border border-cp-accent/40",
  RUNNING: "bg-cp-accent/20 text-cp-accent border border-cp-accent/40",
  QUEUED: "bg-cp-muted/50 text-cp-text border border-cp-border/60",
  UNKNOWN: "bg-cp-muted/50 text-cp-text border border-cp-border/60",
};

export function StatusChip({ status }: { status: RunStatus | string }) {
  return (
    <span className={`inline-flex rounded-full px-2.5 py-1 text-xs font-semibold uppercase tracking-[0.18em] ${STATUS_CLASS[status] ?? STATUS_CLASS.UNKNOWN}`}>
      {status}
    </span>
  );
}
