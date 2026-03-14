export function JsonViewer({ value }: { value: unknown }) {
  const rendered = typeof value === "string" ? value : JSON.stringify(value, null, 2);
  return (
    <pre className="max-h-[32rem] overflow-auto rounded-2xl border border-cp-border bg-cp-surface/70 p-4 font-mono text-xs text-cp-text">
      {rendered}
    </pre>
  );
}
