import * as Tabs from "@radix-ui/react-tabs";
import { AnimatePresence, motion } from "framer-motion";
import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import { AlertTriangle, Archive, ArrowUp, BadgeCheck, Box, ChevronRight, Clock3, Copy, ExternalLink, FileCheck2, FolderSearch, GitBranch, GitCommitHorizontal, HardDrive, Layers3, MoreHorizontal, Network, Pin, PinOff, RotateCcw, ScrollText, Search, ServerCog, ShieldCheck, TerminalSquare, Trash2, X } from "lucide-react";
import type { ArtifactRecord, Buildset, PassCertificate, QueueItemView, RepositorySnapshot } from "../../lib/types";
import { oidHex } from "../../lib/types";
import { Button } from "../../components/ui/Button";
import { Badge } from "../../components/ui/Badge";
import { StatusGlyph, itemStatus } from "../../components/StatusGlyph";
import { cn, formatBytes, formatDuration, relativeTime, shortId } from "../../lib/utils";
import { getLogs, openRawLog, type LogFrameView } from "../../lib/api";

export function ItemInspector({ view, repository, onClose, onCancel, onPromote, onRetry, onOpenWorktree, onOpenArtifact, onRevealArtifact, onPinArtifact, onPruneArtifact, artifactMutationPending }: { view: QueueItemView | null; repository: RepositorySnapshot; onClose: () => void; onCancel: () => void; onPromote: () => void; onRetry: () => void; onOpenWorktree: (path: string) => void; onOpenArtifact: (path: string) => void; onRevealArtifact: (path: string) => void; onPinArtifact: (artifact: ArtifactRecord, pinned: boolean) => void; onPruneArtifact: (artifact: ArtifactRecord) => void; artifactMutationPending: boolean }) {
  const inspector = useRef<HTMLElement>(null);
  const returnFocus = useRef<HTMLElement | null>(null);
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;
  const itemId = view?.item.id;
  const [selectedBuildsetId, setSelectedBuildsetId] = useState<string | undefined>();
  useEffect(() => { setSelectedBuildsetId(view?.buildset?.id); }, [itemId, view?.buildset?.id]);
  const selectedBuildset = view?.attempts?.find((attempt) => attempt.id === selectedBuildsetId) ?? view?.buildset;
  const selectedGeneration = view?.attempt_generations?.find((generation) => generation.id === selectedBuildset?.validation_generation_id) ?? view?.generation;
  const selectedCertificate = view?.certificates?.find((certificate) => certificate.buildset_id === selectedBuildset?.id) ?? (view?.certificate && view.certificate.buildset_id === selectedBuildset?.id ? view.certificate : undefined);
  useEffect(() => {
    if (!itemId) return;
    returnFocus.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    window.setTimeout(() => inspector.current?.focus(), 0);
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        const activeDialog = event.target instanceof Element ? event.target.closest('[role="dialog"]') : null;
        if (event.defaultPrevented || (activeDialog && activeDialog !== inspector.current)) return;
        event.preventDefault(); onCloseRef.current(); return;
      }
      if (event.key !== "Tab" || !inspector.current) return;
      const focusable = [...inspector.current.querySelectorAll<HTMLElement>('button:not([disabled]), [href], input:not([disabled]), [tabindex]:not([tabindex="-1"])')];
      if (!focusable.length) { event.preventDefault(); inspector.current.focus(); return; }
      const first = focusable[0]!; const last = focusable[focusable.length - 1]!;
      if (document.activeElement === inspector.current) { event.preventDefault(); (event.shiftKey ? last : first).focus(); }
      else if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    };
    document.addEventListener("keydown", handleKey);
    return () => { document.removeEventListener("keydown", handleKey); window.setTimeout(() => returnFocus.current?.focus(), 0); };
  }, [itemId]);
  const independent = view?.item.kind === "independent-check";
  const terminal = view ? ["promoted", "promoted-local-push-pending", "externally-integrated", "failed", "merge-conflict", "dependency-failed", "canceled", "superseded", "infrastructure-exhausted", "check-passed", "check-failed"].includes(view.item.state) : false;
  const retryable = view ? ["failed", "infrastructure-exhausted", "check-failed"].includes(view.item.state) : false;
  return <AnimatePresence initial={false}>{view && <motion.aside ref={inspector} id="item-inspector" className="inspector" role="dialog" aria-modal="true" aria-label={`${independent ? "Independent check" : "Queue item"} details: ${view.item.metadata.subject}`} tabIndex={-1} initial={{ width: 0, opacity: 0 }} animate={{ width: 440, opacity: 1 }} exit={{ width: 0, opacity: 0 }} transition={{ type: "spring", stiffness: 420, damping: 40 }}><div className="inspector__inner">
    <div className="inspector__top"><div className="inspector__status"><StatusGlyph state={view.item.state} /><span><small>{independent ? "INDEPENDENT CHECK" : "QUEUE ITEM"}</small><strong>{itemStatus(view.item.state).label}</strong></span></div><div><Button variant="ghost" size="icon" aria-label="Item actions" disabled title="No additional actions are available"><MoreHorizontal size={17} /></Button><Button variant="ghost" size="icon" onClick={onClose} aria-label="Close details"><X size={17} /></Button></div></div>
    <div className="inspector__heading"><h2>{view.item.metadata.subject}</h2><div><span><GitBranch size={13} />{view.item.metadata.branch ?? "detached"}</span><button type="button" className="inline-copy" title="Copy full source object ID" onClick={() => void navigator.clipboard.writeText(oidHex(view.item.source_oid))}><GitCommitHorizontal size={13} /><code>{shortId(oidHex(view.item.source_oid), 9)}</code><Copy size={12} /></button></div></div>
    <Tabs.Root className="inspector__tabs" defaultValue={localStorage.getItem("tollgate.inspector.tab") ?? "overview"} onValueChange={(value) => localStorage.setItem("tollgate.inspector.tab", value)}>
      <Tabs.List aria-label="Item details"><Tabs.Trigger value="overview">Overview</Tabs.Trigger><Tabs.Trigger value="steps">Steps</Tabs.Trigger><Tabs.Trigger value="logs">Logs</Tabs.Trigger><Tabs.Trigger value="artifacts">Artifacts</Tabs.Trigger><Tabs.Trigger value="certificate">Proof</Tabs.Trigger></Tabs.List>
      <div className="inspector__content">
        <Tabs.Content value="overview"><Overview view={view} repository={repository} buildset={selectedBuildset} generation={selectedGeneration} /></Tabs.Content>
        <Tabs.Content value="steps"><Steps view={view} repository={repository} buildset={selectedBuildset} certificate={selectedCertificate} onBuildset={setSelectedBuildsetId} /></Tabs.Content>
        <Tabs.Content value="logs"><Logs view={view} repository={repository} buildset={selectedBuildset} /></Tabs.Content>
        <Tabs.Content value="artifacts"><Artifacts repository={repository} buildset={selectedBuildset} onOpen={onOpenArtifact} onReveal={onRevealArtifact} onPin={onPinArtifact} onPrune={onPruneArtifact} pending={artifactMutationPending} /></Tabs.Content>
        <Tabs.Content value="certificate"><Certificate certificate={selectedCertificate} buildset={selectedBuildset} independent={independent} /></Tabs.Content>
      </div>
    </Tabs.Root>
    <div className="inspector__footer">{!terminal && <Button variant="danger" size="sm" onClick={onCancel}>{independent ? "Cancel check" : "Cancel & dequeue"}</Button>}{retryable && <Button variant="secondary" size="sm" onClick={onRetry}><RotateCcw size={14} />Retry</Button>}{!independent && !terminal && <Button variant="secondary" size="sm" onClick={onPromote} disabled={repository.state.execution_state !== "active" || repository.queue.findIndex((candidate) => candidate.item.id === view.item.id) <= 0}><ArrowUp size={14} />Move forward</Button>}<Button variant="secondary" size="sm" disabled={!view.item.metadata.worktree_path} title={view.item.metadata.worktree_path ? "Reveal the captured feature worktree" : "This item has no retained worktree path"} onClick={() => view.item.metadata.worktree_path && onOpenWorktree(view.item.metadata.worktree_path)}><ExternalLink size={14} />Open worktree</Button></div>
  </div></motion.aside>}</AnimatePresence>;
}

function Artifacts({ repository, buildset, onOpen, onReveal, onPin, onPrune, pending }: { repository: RepositorySnapshot; buildset?: Buildset; onOpen: (path: string) => void; onReveal: (path: string) => void; onPin: (artifact: ArtifactRecord, pinned: boolean) => void; onPrune: (artifact: ArtifactRecord) => void; pending: boolean }) {
  const artifacts = repository.artifacts.filter((artifact) => artifact.buildset_id === buildset?.id);
  if (!artifacts.length) return <div className="proof-empty"><Archive size={28} /><h3>No retained artifacts</h3><p>{buildset ? "This buildset did not publish any configured artifact, or retention has not completed yet." : "Artifacts appear after a buildset finishes and its publication manifest is verified."}</p></div>;
  return <div className="step-list"><div className="step-list__summary"><span><strong>{artifacts.length}</strong> retained files</span><span><strong>{formatBytes(artifacts.reduce((total, artifact) => total + artifact.size, 0))}</strong> logical size</span></div>{artifacts.map((artifact) => <motion.div layout className="step-row artifact-row" key={artifact.artifact_id}><span className="step-row__status"><Archive /></span><div><strong>{artifact.source_path}</strong><code>{shortId(artifact.hash, 14)} · {formatBytes(artifact.size)}</code><small>{artifact.retention_state === "pinned" ? "Pinned · exempt from timed retention" : `Expires ${relativeTime(artifact.expires_at)}`}</small></div><span className="step-row__meta artifact-actions"><Badge tone={artifact.retention_state === "pinned" ? "violet" : "neutral"}>{artifact.retention_state}</Badge><Button title="Open with the default app" aria-label={`Open ${artifact.source_path}`} variant="ghost" size="icon" onClick={() => onOpen(artifact.retained_path)}><ExternalLink size={13} /></Button><Button title="Reveal in Finder" aria-label={`Reveal ${artifact.source_path} in Finder`} variant="ghost" size="icon" onClick={() => onReveal(artifact.retained_path)}><FolderSearch size={13} /></Button><Button title={artifact.retention_state === "pinned" ? "Return to timed retention" : "Pin indefinitely"} aria-label={artifact.retention_state === "pinned" ? `Unpin ${artifact.source_path}` : `Pin ${artifact.source_path}`} variant="ghost" size="icon" disabled={pending} onClick={() => onPin(artifact, artifact.retention_state !== "pinned")}>{artifact.retention_state === "pinned" ? <PinOff size={13} /> : <Pin size={13} />}</Button><Button title="Prune retained file" aria-label={`Prune ${artifact.source_path}`} variant="ghost" size="icon" disabled={pending} onClick={() => onPrune(artifact)}><Trash2 size={13} /></Button></span></motion.div>)}</div>;
}

function Overview({ view, repository, buildset, generation }: { view: QueueItemView; repository: RepositorySnapshot; buildset?: Buildset; generation?: import("../../lib/types").ValidationGeneration }) {
  const independent = view.item.kind === "independent-check";
  return <div className="inspector-section-stack">
    <section className="detail-section"><h3>{independent ? "Independent validation" : "Speculative composition"}</h3>{independent && <p className="section-intro">This run tests the captured source commit directly. Its result is diagnostic evidence only and cannot advance <code>release</code>.</p>}<div className="composition-card">{!independent && <><div><span className="composition-card__node"><Box size={13} /></span><p><small>ANCHORED BASE</small><strong>release · <code>{shortId(oidHex(generation?.anchored_base_oid), 9)}</code></strong></p></div><span className="composition-card__stem" /></>}{generation?.ordered_item_ids.map((id, index) => <div key={id}><span className={cn("composition-card__node", index === generation?.ordered_item_ids.length - 1 && "is-current")}>{independent ? <GitCommitHorizontal size={13} /> : index + 1}</span><p><small>{independent ? "CAPTURED SOURCE" : index === generation?.ordered_item_ids.length - 1 ? "THIS CHANGE" : `PREFIX CHANGE ${index + 1}`}</small><strong>{independent || index === generation?.ordered_item_ids.length - 1 ? view.item.metadata.subject : `Queued predecessor #${index + 1}`}</strong></p></div>)}<span className="composition-card__stem" /><div><span className="composition-card__node is-tested"><ShieldCheck size={13} /></span><p><small>EXACT TESTED COMMIT</small><strong><code>{shortId(oidHex(generation?.tested_oid), 12)}</code></strong></p></div></div></section>
    <section className="detail-section"><h3>Validation identity</h3><dl className="detail-list"><div><dt>Generation</dt><dd><code>{shortId(generation?.identity_digest, 12)}</code></dd></div><div><dt>Expected parent</dt><dd><code>{shortId(oidHex(generation?.expected_parent_oid), 12)}</code></dd></div><div><dt>Configuration</dt><dd><code>{shortId(generation?.configuration_digest, 12)}</code></dd></div><div><dt>Engine epoch</dt><dd>{generation?.engine_epoch ?? repository.state.engine_epoch}</dd></div><div><dt>Queue revision</dt><dd>{repository.state.queue_revision}</dd></div></dl></section>
    <section className="detail-section"><h3>Execution</h3><div className="execution-grid"><div><ServerCog /><small>SLOT</small><strong>{shortId(buildset?.slot_id, 8) || "Not assigned"}</strong></div><div><Clock3 /><small>ELAPSED</small><strong>{formatDuration(buildset?.started_at ? Math.max(0, new Date(buildset.finished_at ?? Date.now()).getTime() - new Date(buildset.started_at).getTime()) : undefined)}</strong></div><div><Layers3 /><small>ATTEMPT</small><strong>{buildset?.attempt ?? "—"} of 3</strong></div><div><HardDrive /><small>CACHE</small><strong>{buildset?.slot_id ? "Persistent slot" : "Not reported"}</strong></div></div></section>
    {(view.item.remote_state !== "disabled" || view.item.cleanup_state === "needs-attention" || view.item.terminal_reason) && <section className="detail-section"><h3>Operational state</h3><dl className="detail-list"><div><dt>Remote</dt><dd>{view.item.remote_state}</dd></div><div><dt>Cleanup</dt><dd>{view.item.cleanup_state}</dd></div>{view.item.terminal_reason && <div><dt>Reason</dt><dd>{view.item.terminal_reason}</dd></div>}</dl></section>}
    {view.item.dependencies.length > 0 && <section className="dependency-callout"><Network size={17} /><div><strong>Hard Git dependency</strong><p>This change leaves the gate if its prerequisite cannot land.</p></div><ChevronRight size={16} /></section>}
  </div>;
}

function Steps({ view, repository, buildset, certificate, onBuildset }: { view: QueueItemView; repository: RepositorySnapshot; buildset?: Buildset; certificate?: PassCertificate; onBuildset: (id: string) => void }) {
  const steps = evidenceSteps(buildset, repository);
  const votingResults = certificate?.voting_results.length ?? 0;
  return <div className="step-list"><div className="step-list__summary"><span><strong>{steps.length}</strong> frozen steps</span><span><strong>{view.attempts?.length ?? (buildset ? 1 : 0)}</strong> retained attempt{(view.attempts?.length ?? 0) === 1 ? "" : "s"}</span><span><strong>{votingResults}</strong> proven voting results</span></div>{(view.attempts?.length ?? 0) > 1 && <div className="inspector-badge-row" aria-label="Retained build attempts">{view.attempts!.map((attempt) => { const generation = view.attempt_generations?.find((candidate) => candidate.id === attempt.validation_generation_id); return <button type="button" key={attempt.id} aria-pressed={attempt.id === buildset?.id} onClick={() => onBuildset(attempt.id)} title={`Generation ${shortId(generation?.identity_digest, 10)} · ${relativeTime(attempt.created_at)}`}><Badge tone={attempt.state === "passed" || attempt.state === "passed-with-warnings" ? "success" : attempt.state === "failed" ? "warning" : "neutral"}>g{shortId(generation?.identity_digest, 5)} · a{attempt.attempt} · {attempt.state}</Badge></button>; })}</div>}{steps.map((step) => {
    const result = buildset?.step_results.find((candidate) => candidate.name === step.name);
    const detail = result ? `${result.result_class}${result.exit_code !== undefined ? ` · exit ${result.exit_code}` : result.signal !== undefined ? ` · signal ${result.signal}` : ""} · ${formatDuration(result.elapsed_ms)}` : buildset ? "Awaiting result" : "Not started";
    return <motion.div className={cn("step-row", result?.result_class === "success" && "is-passed")} key={step.name} layout><span className="step-row__status"><TerminalSquare /></span><div><strong>{step.name}</strong><code>{step.command.kind === "shell" ? step.command.script : step.command.argv.join(" ")}</code><small>{detail}</small></div><span className="step-row__meta">{step.voting && <Badge tone="neutral">voting</Badge>}<small>{result?.log_hash ? `log ${shortId(result.log_hash, 8)}` : ""}</small></span></motion.div>;
  })}</div>;
}

function Logs({ view, repository, buildset }: { view: QueueItemView; repository: RepositorySnapshot; buildset?: Buildset }) {
  const steps = evidenceSteps(buildset, repository);
  const defaultStep = steps.find((candidate) => candidate.name === "test")?.name ?? steps[0]?.name;
  const [step, setStep] = useState(() => localStorage.getItem("tollgate.log.step") ?? defaultStep);
  const [stream, setStream] = useState<"all" | "stdout" | "stderr">("all");
  const [follow, setFollow] = useState(() => localStorage.getItem("tollgate.log.follow") !== "false");
  const [search, setSearch] = useState("");
  const [frames, setFrames] = useState<LogFrameView[]>([]);
  const [gap, setGap] = useState(false);
  const [truncated, setTruncated] = useState(false);
  const output = useRef<HTMLPreElement>(null);
  useEffect(() => { if (!steps.some((candidate) => candidate.name === step)) setStep(defaultStep); }, [defaultStep, step, steps]);
  useEffect(() => { setFrames([]); setGap(false); setTruncated(false); }, [view.item.id, buildset?.id, step]);
  const nextSequence = (frames.at(-1)?.frame.broker_sequence ?? -1) + 1;
  const query = useQuery({ queryKey: ["logs", view.item.id, buildset?.id, step, nextSequence], queryFn: () => getLogs(repository.state.id, view.item.id, buildset?.id, step, nextSequence, frames.length === 0), refetchInterval: view.item.state === "running" && buildset?.id === view.buildset?.id && follow ? 750 : false, retry: false, gcTime: 0 });
  useEffect(() => {
    if (!query.data?.length) return;
    setFrames((current) => {
      const known = new Set(current.map((entry) => entry.frame.broker_sequence));
      const incoming = query.data!.filter((candidate) => !known.has(candidate.frame.broker_sequence));
      if (!incoming.length) return current;
      const combined = [...current, ...incoming].sort((left, right) => left.frame.broker_sequence - right.frame.broker_sequence);
      for (let index = 1; index < combined.length; index += 1) if (combined[index]!.frame.broker_sequence !== combined[index - 1]!.frame.broker_sequence + 1) setGap(true);
      const offsets: Partial<Record<"stdout" | "stderr", number>> = {};
      for (const entry of combined) {
        const expected = offsets[entry.frame.stream];
        if (expected !== undefined && entry.frame.stream_offset !== expected) setGap(true);
        offsets[entry.frame.stream] = entry.frame.stream_offset + entry.frame.payload_len;
      }
      if (combined[0]!.frame.broker_sequence > 1) setTruncated(true);
      let retainedBytes = combined.reduce((total, entry) => total + entry.frame.payload_len, 0);
      let firstRetained = 0;
      while (firstRetained < combined.length && (combined.length - firstRetained > 2_000 || retainedBytes > 8 * 1024 * 1024)) {
        retainedBytes -= combined[firstRetained]!.frame.payload_len;
        firstRetained += 1;
      }
      if (firstRetained > 0) { setTruncated(true); return combined.slice(firstRetained); }
      return combined;
    });
  }, [query.data, truncated]);
  const visibleFrames = useMemo(() => {
    const matching = frames.filter((entry) => stream === "all" || entry.frame.stream === stream);
    let bytes = 0; let start = matching.length;
    while (start > 0 && matching.length - start < 512 && bytes + matching[start - 1]!.frame.payload_len <= 2 * 1024 * 1024) {
      start -= 1; bytes += matching[start]!.frame.payload_len;
    }
    return matching.slice(start);
  }, [frames, stream]);
  const actual = visibleFrames.map((frame) => frame.text).join("");
  const contentMatch = !search || actual.toLowerCase().includes(search.toLowerCase());
  const content = query.isLoading && !frames.length ? "Loading durable log…" : query.isError ? `Log unavailable: ${String(query.error)}` : actual || (buildset ? "No output was emitted for this step." : "This buildset has not started, so no log exists yet.");
  useEffect(() => { if (follow && output.current) output.current.scrollTop = output.current.scrollHeight; }, [content, follow]);
  return <div className="log-panel"><div className="log-panel__toolbar"><span><TerminalSquare size={14} /><select aria-label="Log step" value={step} onChange={(event) => { setStep(event.target.value); localStorage.setItem("tollgate.log.step", event.target.value); }}>{steps.map((candidate) => <option key={candidate.name}>{candidate.name}</option>)}</select></span><div><select aria-label="Log stream" value={stream} onChange={(event) => setStream(event.target.value as typeof stream)}><option value="all">stdout + stderr</option><option value="stdout">stdout</option><option value="stderr">stderr</option></select><button className={follow ? "is-active" : ""} onClick={() => setFollow((value) => { localStorage.setItem("tollgate.log.follow", String(!value)); return !value; })}>{follow ? "Following" : "Follow"}</button></div></div><div className="log-panel__search"><Search size={13} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Find in decoded buffer…" aria-label="Search log" /><button disabled={!actual} onClick={() => void navigator.clipboard.writeText(actual)}><Copy size={12} />Copy</button><button disabled={!buildset || query.isError} onClick={() => void openRawLog(repository.state.id, view.item.id, buildset?.id, step)} title="Open the immutable framed log with the default app"><ExternalLink size={12} />Open raw</button></div><pre ref={output} tabIndex={0} aria-label="Build log">{content}</pre><div className="log-panel__footer" aria-live="polite"><span>{view.item.state === "running" ? "Live" : "Durable"} · {frames.length} buffered frames{search ? contentMatch ? " · match" : " · no match" : ""}</span><span>{query.isError ? "Read failed" : gap ? "Sequence gap detected" : truncated ? "Older frames bounded · raw log complete" : frames.some((frame) => frame.invalid_utf8) ? "Lossy UTF-8 indicated" : "Offsets continuous"}</span></div></div>;
}

function evidenceSteps(buildset: Buildset | undefined, repository: RepositorySnapshot): Array<{ name: string; command: { kind: "shell"; runner?: string[]; script: string } | { kind: "argv"; argv: string[] }; voting: boolean }> {
  if (buildset?.frozen_steps?.length) return buildset.frozen_steps;
  if (buildset?.step_results.length) return buildset.step_results.map((result) => ({ id: `legacy-${result.name}`, name: result.name, command: { kind: "shell", runner: [], script: "Legacy command metadata unavailable" }, working_directory: ".", needs: [], soft_needs: [], voting: true, final_step: false, timeout_ns: 0, cpu_tokens: 0, memory_bytes: 0, semaphores: [] }));
  return repository.configuration.steps;
}

function Certificate({ certificate, buildset, independent }: { certificate?: PassCertificate; buildset?: Buildset; independent: boolean }) {
  if (!certificate || certificate.buildset_id !== buildset?.id) return <div className="proof-empty"><ScrollText size={28} /><h3>{independent ? "Independent checks do not issue gate proof" : "This attempt has no pass certificate"}</h3><p>{independent ? "The durable result and logs remain available, but this run can never authorize promotion or stand in for a dependent-prefix certificate." : "Only successful gate attempts carry bound proof. Select a passing retained attempt to inspect it."}</p></div>;
  const hasWarnings = certificate.warnings.length > 0;
  return <div className="certificate"><div className="certificate__seal"><BadgeCheck size={28} /><div><small>{hasWarnings ? "PASS CERTIFICATE · WARNINGS" : "PASS CERTIFICATE"}</small><strong>{hasWarnings ? "Exact validation passed with warnings" : "Exact validation verified"}</strong><span>Issued {relativeTime(certificate.created_at)}</span></div></div>{hasWarnings && <section className="dependency-callout"><AlertTriangle size={17} /><div><strong>Non-voting warnings</strong><p>{certificate.warnings.join(" · ")}</p></div></section>}<dl className="detail-list"><div><dt>Tested object</dt><dd><code>{shortId(oidHex(certificate.tested_oid), 14)}</code></dd></div><div><dt>Tree object</dt><dd><code>{shortId(oidHex(certificate.tree_oid), 14)}</code></dd></div><div><dt>Expected parent</dt><dd><code>{shortId(oidHex(certificate.expected_parent_oid), 14)}</code></dd></div><div><dt>Voting results</dt><dd>{certificate.voting_results.length} successful</dd></div><div><dt>Checkout</dt><dd className="success-text"><FileCheck2 size={13} />{certificate.checkout_verified ? "clean & exact" : "not verified"}</dd></div><div><dt>Certificate</dt><dd><code>{shortId(certificate.id, 14)}</code></dd></div></dl><div className="certificate__raw" role="status"><Archive size={14} />Certificate and content hashes remain durable; logs and artifacts follow retention policy</div></div>;
}
