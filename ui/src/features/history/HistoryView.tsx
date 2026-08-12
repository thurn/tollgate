import { motion } from "framer-motion";
import { CalendarDays, GitCommitHorizontal, GitMerge, RotateCcw, Search, ShieldCheck, TerminalSquare, X } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import type { DomainEvent, RepositorySnapshot } from "../../lib/types";
import { Badge } from "../../components/ui/Badge";
import { isTauri, relativeTime } from "../../lib/utils";
import { getHistoryItems } from "../../lib/api";

function eventAppearance(event: DomainEvent) {
  if (event.kind.includes("promotion")) return { icon: GitMerge, tone: "success" as const, title: "Change promoted to release" };
  if (event.kind.includes("certificate") || event.kind.includes("passed")) return { icon: ShieldCheck, tone: "success" as const, title: "Pass certificate issued" };
  if (event.kind.includes("failed")) return { icon: X, tone: "danger" as const, title: "Validation failed" };
  if (event.kind.includes("started")) return { icon: TerminalSquare, tone: "info" as const, title: "Validation step started" };
  if (event.kind.includes("enqueued")) return { icon: GitCommitHorizontal, tone: "violet" as const, title: "Change approved into gate" };
  return { icon: RotateCcw, tone: "neutral" as const, title: event.kind.replaceAll(".", " ") };
}

export function HistoryView({ repository, onSelect }: { repository: RepositorySnapshot; onSelect: (id: string) => void }) {
  const [query, setQuery] = useState("");
  const [category, setCategory] = useState("all");
  const [completedPage, setCompletedPage] = useState(0);
  const completedQuery = useQuery({ queryKey: ["history-items", repository.state.id, completedPage], queryFn: () => getHistoryItems(repository.state.id, completedPage * 24, 24), enabled: isTauri(), placeholderData: (previous) => previous });
  const completed = completedQuery.data?.items ?? (completedPage === 0 ? repository.history_items : []);
  const completedTotal = completedQuery.data?.total ?? repository.history_items.length;
  const completedPages = Math.max(1, Math.ceil(completedTotal / 24));
  useEffect(() => { setCompletedPage((page) => Math.min(page, completedPages - 1)); }, [completedPages]);
  const events = useMemo(() => [...repository.history.filter((event) => {
    const matchesQuery = `${event.sequence} ${event.kind} ${event.actor} ${JSON.stringify(event.payload)}`.toLowerCase().includes(query.toLowerCase());
    const matchesCategory = category === "all" || (category === "failures" ? /failed|blocked|conflict|canceled/.test(event.kind + JSON.stringify(event.payload)) : category === "promotion" ? /promotion|push|reconcile|pull/.test(event.kind) : /configuration|environment/.test(event.kind));
    return matchesQuery && matchesCategory;
  })].reverse().slice(0, 200), [category, query, repository.history]);
  const first = repository.history.at(0); const last = repository.history.at(-1);
  return <div className="content-page"><section className="page-hero page-hero--compact"><div><div className="eyebrow"><CalendarDays size={13} />IMMUTABLE AUDIT JOURNAL</div><h1>History</h1><p>The latest 500 durable service events, preserved in sequence order.</p></div></section>{completedTotal > 0 && <section className="history-results" aria-label="Completed validations"><div className="history-results__heading"><h2>Completed validations</h2><span>{completedTotal} retained · page {completedPage + 1} of {completedPages}</span></div><div aria-busy={completedQuery.isFetching}>{completed.map((view) => <button key={view.item.id} onClick={() => onSelect(view.item.id)}><span>{view.item.metadata.subject}</span><small>{view.item.state} · {view.item.id.slice(0, 8)}</small></button>)}</div>{completedPages > 1 && <nav className="history-pagination" aria-label="Completed validation pages"><button disabled={completedPage === 0 || completedQuery.isFetching} onClick={() => setCompletedPage((page) => page - 1)}>Newer</button><button disabled={completedPage >= completedPages - 1 || completedQuery.isFetching} onClick={() => setCompletedPage((page) => page + 1)}>Older</button></nav>}</section>}<div className="filter-bar"><div className="filter-search"><Search size={15} /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search sequence, kind, actor, OID, branch, or terminal reason…" aria-label="Search history" /></div><select aria-label="Filter history category" value={category} onChange={(event) => setCategory(event.target.value)}><option value="all">All event types</option><option value="failures">Failures & blocks</option><option value="promotion">Promotion & remote</option><option value="configuration">Configuration</option></select></div><div className="history-layout"><div className="history-feed"><div className="history-date"><span>{query || category !== "all" ? `${events.length} matches` : `${events.length} recent retained events`}</span><i /></div>{events.length ? events.map((event, index) => <HistoryEvent key={event.id} event={event} index={index} />) : <p className="muted-empty">{query || category !== "all" ? "No events match these filters." : "No audit events yet."}</p>}</div><aside className="history-stats"><h3>Journal window</h3><dl><div><dt>Events retained</dt><dd>{repository.history.length}</dd></div><div><dt>Rendered window</dt><dd>{events.length} / 200</dd></div><div><dt>First sequence</dt><dd>{first ? `#${first.sequence}` : "—"}</dd></div><div><dt>Latest sequence</dt><dd>{last ? `#${last.sequence}` : "—"}</dd></div><div><dt>Repository sequence</dt><dd>#{repository.state.event_sequence}</dd></div></dl></aside></div></div>;
}

function HistoryEvent({ event, index }: { event: DomainEvent; index: number }) {
  const appearance = eventAppearance(event); const Icon = appearance.icon;
  return <motion.article className="history-event" initial={{ opacity: 0, x: -6 }} animate={{ opacity: 1, x: 0 }} transition={{ delay: Math.min(index * 0.035, .25) }}><span className={`history-event__icon tone-${appearance.tone}`}><Icon size={15} /></span><div><div className="history-event__title"><strong>{appearance.title}</strong><Badge tone={appearance.tone}>{event.actor}</Badge></div><p>Event <code>#{event.sequence}</code> · {event.kind}</p><span>{relativeTime(event.created_at)}</span></div></motion.article>;
}
