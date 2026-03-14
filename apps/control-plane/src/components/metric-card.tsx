export function MetricCard({
  label,
  value,
  hint,
}: {
  label: string;
  value: string | number;
  hint?: string;
}) {
  return (
    <div className="rounded-2xl border border-cp-border bg-cp-surface/80 p-4">
      <div className="text-xs uppercase tracking-[0.2em] text-cp-text-muted">{label}</div>
      <div className="mt-2 text-2xl font-semibold text-cp-text">{value}</div>
      {hint ? <div className="mt-2 text-sm text-cp-text-muted">{hint}</div> : null}
    </div>
  );
}
