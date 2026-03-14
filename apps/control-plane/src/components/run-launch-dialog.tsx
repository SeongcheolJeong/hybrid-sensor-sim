import * as Dialog from "@radix-ui/react-dialog";
import { useMemo, useState } from "react";

import { JsonViewer } from "./json-viewer";

export function RunLaunchDialog({
  title,
  defaultPayload,
  onSubmit,
}: {
  title: string;
  defaultPayload: Record<string, unknown>;
  onSubmit: (payload: Record<string, unknown>) => Promise<unknown>;
}) {
  const [open, setOpen] = useState(false);
  const [jsonText, setJsonText] = useState(() => JSON.stringify(defaultPayload, null, 2));
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const parsedPreview = useMemo(() => {
    try {
      return JSON.parse(jsonText);
    } catch {
      return null;
    }
  }, [jsonText]);

  async function handleSubmit() {
    try {
      setSubmitting(true);
      setError(null);
      const payload = JSON.parse(jsonText);
      await onSubmit(payload);
      setOpen(false);
    } catch (submissionError) {
      setError(submissionError instanceof Error ? submissionError.message : String(submissionError));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Trigger asChild>
        <button className="rounded-full border border-cp-accent/40 bg-cp-accent/15 px-4 py-2 text-sm font-medium text-cp-accent hover:bg-cp-accent/20">
          Launch
        </button>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 bg-black/60 backdrop-blur-sm" />
        <Dialog.Content className="fixed left-1/2 top-1/2 max-h-[90vh] w-[min(92vw,1040px)] -translate-x-1/2 -translate-y-1/2 overflow-auto rounded-3xl border border-cp-border bg-cp-panel p-6 shadow-2xl">
          <div className="flex items-start justify-between gap-4">
            <div>
              <Dialog.Title className="text-xl font-semibold text-cp-text">{title}</Dialog.Title>
              <Dialog.Description className="mt-1 text-sm text-cp-text-muted">
                Edit the JSON payload directly. The backend accepts the current workflow payloads as-is.
              </Dialog.Description>
            </div>
            <Dialog.Close asChild>
              <button className="rounded-full border border-cp-border px-3 py-1.5 text-xs uppercase tracking-[0.16em] text-cp-text-muted">Close</button>
            </Dialog.Close>
          </div>
          <div className="mt-5 grid gap-5 lg:grid-cols-[1.2fr_0.8fr]">
            <label className="block">
              <span className="mb-2 block text-xs uppercase tracking-[0.18em] text-cp-text-muted">Payload</span>
              <textarea
                className="h-[28rem] w-full rounded-2xl border border-cp-border bg-cp-surface/80 p-4 font-mono text-xs text-cp-text outline-none focus:border-cp-accent"
                value={jsonText}
                onChange={(event) => setJsonText(event.target.value)}
              />
            </label>
            <div>
              <div className="mb-2 text-xs uppercase tracking-[0.18em] text-cp-text-muted">Parsed preview</div>
              {parsedPreview ? <JsonViewer value={parsedPreview} /> : <div className="rounded-2xl border border-cp-danger/40 bg-cp-danger/10 p-4 text-sm text-cp-danger">JSON parse error. Fix the payload before launching.</div>}
            </div>
          </div>
          {error ? <div className="mt-4 rounded-2xl border border-cp-danger/40 bg-cp-danger/10 p-3 text-sm text-cp-danger">{error}</div> : null}
          <div className="mt-5 flex justify-end gap-3">
            <Dialog.Close asChild>
              <button className="rounded-full border border-cp-border px-4 py-2 text-sm text-cp-text-muted">Cancel</button>
            </Dialog.Close>
            <button
              className="rounded-full border border-cp-success/40 bg-cp-success/15 px-4 py-2 text-sm font-medium text-cp-success disabled:cursor-not-allowed disabled:opacity-50"
              disabled={!parsedPreview || submitting}
              onClick={() => void handleSubmit()}
            >
              {submitting ? "Launching..." : "Launch run"}
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
