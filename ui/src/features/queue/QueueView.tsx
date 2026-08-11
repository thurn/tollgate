import { AnimatePresence, motion } from "framer-motion";
import { ArrowDown, CheckCircle2, ChevronRight, CircleGauge, Clock3, FlaskConical, GitCommitHorizontal, GitMerge, Info, Layers3, ListFilter, Search, ShieldOff, Sparkles } from "lucide-react";
import type { RepositorySnapshot } from "../../lib/types";
import { Button } from "../../components/ui/Button";
import { Badge } from "../../components/ui/Badge";
import { QueueCard } from "./QueueCard";
import { cn, shortId } from "../../lib/utils";
import { StatusGlyph, itemStatus } from "../../components/StatusGlyph";
import { oidHex } from "../../lib/types";

export function QueueView({ repository, selectedItemId, onSelect, onApprove }: { repository: RepositorySnapshot; selectedItemId: string | null; onSelect: (id: string | null) => void; onApprove: () => void }) {
  const queue = repository.queue;
  const ready = queue.filter((item) => item.item.state === "ready").length;
  const running = queue.filter((item) => item.item.state === "running").length;
  return <div className="queue-view">
    <section className="page-hero">
      <div><div className="eyebrow"><GitMerge size={13} />DEPENDENT GATE · REVISION {repository.state.queue_revision}</div><h1>Speculative queue</h1><p>Every change is validated against the exact prefix that would land before it.</p></div>
      <div className="hero-metrics">
        <div><span className="metric-icon tone-info"><CircleGauge size={17} /></span><p><strong>{running}</strong><small>running now</small></p></div>
        <div><span className="metric-icon tone-success"><CheckCircle2 size={17} /></span><p><strong>{ready}</strong><small>validation passed</small></p></div>
        <div><span className="metric-icon tone-violet"><Layers3 size={17} /></span><p><strong>{queue.length}</strong><small>in gate</small></p></div>
      </div>
    </section>
    {queue.length > 0 && <section className="promotion-path" aria-label="Promotion path">
      <span className="promotion-path__master"><GitMerge size={14} /><strong>master</strong><code>{shortId(repository.state.master_oid.bytes, 7)}</code></span>
      <ChevronRight size={14} />
      <div className="promotion-path__segments">{queue.map((entry, index) => <motion.span layout key={entry.item.id} className={cn(entry.item.state === "ready" && "is-ready", entry.item.state === "running" && "is-running")}><i />#{index + 1}</motion.span>)}</div>
      <div className="promotion-path__window"><Sparkles size={13} />active window <strong>{repository.state.active_window}</strong></div>
    </section>}
    <div className="queue-toolbar">
      <div className="queue-toolbar__title"><h2>Promotion order</h2><Badge tone="neutral">{queue.length} {queue.length === 1 ? "change" : "changes"}</Badge><span><Info size={13} />Earlier failures rebuild affected descendants automatically.</span></div>
      <div className="queue-toolbar__actions"><button className="search-control" disabled title="Queue filtering is not available"><Search size={14} /><span>Filter unavailable</span></button><Button variant="ghost" size="icon" aria-label="Filter options" disabled title="Filter options are not available"><ListFilter size={16} /></Button></div>
    </div>
    {queue.length ? <div className="queue-list"><AnimatePresence initial={false}>{queue.map((entry, index) => <QueueCard key={entry.item.id} view={entry} position={index + 1} eligible={index < repository.state.active_window} selected={selectedItemId === entry.item.id} onSelect={() => onSelect(selectedItemId === entry.item.id ? null : entry.item.id)} />)}</AnimatePresence><div className="queue-tail"><span><ArrowDown size={13} /></span><p><strong>New approvals join here</strong><small>Appending never invalidates earlier prefixes.</small></p></div></div> : <EmptyQueue repository={repository} onApprove={onApprove} />}
    {repository.checks.length > 0 && <section className="checks-panel" aria-labelledby="independent-checks-title"><div className="checks-panel__header"><div><span className="metric-icon tone-violet"><FlaskConical size={16} /></span><div><h2 id="independent-checks-title">Independent checks</h2><small>Direct validation runs, isolated from promotion order</small></div></div><Badge tone="neutral"><ShieldOff size={12} />no promotion authority</Badge></div><div className="checks-list"><AnimatePresence initial={false}>{repository.checks.map((entry) => <motion.button layout key={entry.item.id} className={cn("check-row", selectedItemId === entry.item.id && "is-selected")} onClick={() => onSelect(selectedItemId === entry.item.id ? null : entry.item.id)} initial={{ opacity: 0, y: 5 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, height: 0 }}><StatusGlyph state={entry.item.state} /><span className="check-row__identity"><strong>{entry.item.metadata.subject}</strong><small><GitCommitHorizontal size={12} /><code>{shortId(oidHex(entry.item.source_oid), 9)}</code></small></span><span className="check-row__runtime"><small><Clock3 size={12} />{entry.elapsed_ms ? `${Math.round(entry.elapsed_ms / 1000)}s` : "not started"}</small><Badge tone={itemStatus(entry.item.state).tone}>{itemStatus(entry.item.state).label}</Badge></span><ChevronRight size={15} /></motion.button>)}</AnimatePresence></div></section>}
  </div>;
}

function EmptyQueue({ repository, onApprove }: { repository: RepositorySnapshot; onApprove: () => void }) {
  const active = repository.state.execution_state === "active";
  const reason = repository.state.block_reasons[0];
  return <motion.div className="empty-queue" initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }}><div className="empty-queue__visual"><span /><span /><span /><GitMerge size={27} /></div><h2>{active ? "The gate is clear" : repository.state.execution_state === "paused" ? "The gate is paused" : "The gate needs attention"}</h2><p>{active ? <>Approve a clean, single-commit worktree to validate its exact prospective commit on <code>master</code>.</> : reason?.message ?? `Repository state: ${repository.state.execution_state}.`}</p><Button variant="primary" onClick={onApprove} disabled={!active}>Approve a change</Button><small>{active ? <>Or run <code>tg approve</code> from a feature worktree</> : reason?.recovery_action ?? "Resolve the repository state before approving work."}</small></motion.div>;
}
