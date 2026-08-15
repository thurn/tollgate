import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { TerminalSquare, X } from "lucide-react";
import type { QueueItemView, RepositorySnapshot } from "../../lib/types";
import { oidHex } from "../../lib/types";
import { Button } from "../../components/ui/Button";
import { StatusGlyph, itemStatus } from "../../components/StatusGlyph";
import { formatDuration, isTauri, shortId } from "../../lib/utils";
import { getLogs, openRawLog } from "../../lib/api";

export function ItemInspector({ view, repository, onClose }: {
  view: QueueItemView | null;
  repository: RepositorySnapshot;
  onClose: () => void;
}) {
  const inspector = useRef<HTMLElement>(null);
  const returnFocus = useRef<HTMLElement | null>(null);
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;
  const itemId = view?.item.id;
  useEffect(() => {
    if (!itemId) return;
    returnFocus.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    window.setTimeout(() => inspector.current?.focus(), 0);
    const close = (event: KeyboardEvent) => { if (event.key === "Escape") onCloseRef.current(); };
    document.addEventListener("keydown", close);
    return () => { document.removeEventListener("keydown", close); window.setTimeout(() => returnFocus.current?.focus(), 0); };
  }, [itemId]);
  if (!view) return null;

  const status = itemStatus(view.item.state);
  const steps = view.buildset?.frozen_steps ?? repository.configuration.steps;
  return <aside ref={inspector} id="item-inspector" className="inspector" role="dialog" aria-modal="true" aria-label={`Details: ${view.item.metadata.subject}`} tabIndex={-1}>
    <header className="inspector__top"><div><StatusGlyph state={view.item.state} /><span><small>{view.item.kind === "independent-check" ? "CHECK" : "QUEUE ITEM"}</small><strong>{status.label}</strong></span></div><Button variant="ghost" size="icon" onClick={onClose} aria-label="Close details"><X /></Button></header>
    <div className="inspector__content">
      <section className="inspector__heading"><h2>{view.item.metadata.subject}</h2><p>{view.item.metadata.branch ?? "Detached source"} · <code>{shortId(oidHex(view.item.source_oid), 10)}</code></p></section>
      <dl className="detail-list">
        <div><dt>Elapsed</dt><dd>{formatDuration(view.elapsed_ms)}</dd></div>
        <div><dt>Attempt</dt><dd>{view.buildset?.attempt ?? "—"}</dd></div>
        <div><dt>Tested commit</dt><dd><code>{shortId(oidHex(view.generation?.tested_oid), 10) || "—"}</code></dd></div>
        <div><dt>Promotion</dt><dd>{view.item.promotion_authorized ? "Authorized" : "Not authorized"}</dd></div>
      </dl>
      {view.item.terminal_reason && <div className="notice"><strong>{view.item.terminal_reason}</strong></div>}
      <section className="steps"><h3>Steps</h3>{steps.map((step) => { const result = view.buildset?.step_results.find((candidate) => candidate.name === step.name); const failed = result && !["success", "running", "pending", "skipped"].includes(result.result_class); return <div key={step.name}><span className={`step-state ${result?.result_class === "success" ? "is-success" : failed ? "is-failure" : result ? "is-active" : ""}`} /><strong>{step.name}</strong><small>{result ? `${result.result_class} · ${formatDuration(result.elapsed_ms)}` : "waiting"}</small></div>; })}</section>
      <LogPanel view={view} repository={repository} stepNames={steps.map((step) => step.name)} />
    </div>
  </aside>;
}

function LogPanel({ view, repository, stepNames }: { view: QueueItemView; repository: RepositorySnapshot; stepNames: string[] }) {
  const firstStep = stepNames[0] ?? "default";
  const [open, setOpen] = useState(false);
  const [step, setStep] = useState(firstStep);
  useEffect(() => { setStep(firstStep); setOpen(false); }, [view.item.id, firstStep]);
  const query = useQuery({
    queryKey: ["simple-log", view.item.id, view.buildset?.id, step],
    queryFn: () => getLogs(repository.state.id, view.item.id, view.buildset?.id, step, 0, true),
    enabled: open && isTauri(),
    refetchInterval: open && view.item.state === "running" ? 1_000 : false,
    retry: false,
  });
  const content = useMemo(() => query.data?.map((entry) => entry.text).join("") ?? "", [query.data]);
  return <section className="logs">
    <button className="logs__toggle" onClick={() => setOpen((value) => !value)}><TerminalSquare />{open ? "Hide log" : "View log"}</button>
    {open && <div className="logs__panel">
      <div><select aria-label="Log step" value={step} onChange={(event) => setStep(event.target.value)}>{stepNames.map((name) => <option key={name}>{name}</option>)}</select><button onClick={() => void openRawLog(repository.state.id, view.item.id, view.buildset?.id, step)} disabled={!isTauri()}>Open raw</button></div>
      <pre>{isTauri() ? query.isLoading ? "Loading…" : query.isError ? "Log unavailable." : content || "No output." : "Logs are available in the desktop app."}</pre>
    </div>}
  </section>;
}
