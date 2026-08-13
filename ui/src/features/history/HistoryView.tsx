import { Check, GitMerge, RotateCcw, X } from "lucide-react";
import type { DomainEvent, RepositorySnapshot } from "../../lib/types";
import { relativeTime } from "../../lib/utils";

function eventLabel(event: DomainEvent) {
  if (/promotion/.test(event.kind)) return { label: "Promoted", icon: GitMerge, tone: "success" };
  if (/certificate|passed/.test(event.kind)) return { label: "Passed", icon: Check, tone: "success" };
  if (/failed|blocked|conflict/.test(event.kind)) return { label: "Failed", icon: X, tone: "danger" };
  return { label: event.kind.replaceAll(".", " "), icon: RotateCcw, tone: "neutral" };
}

export function HistoryView({ repository, onSelect }: { repository: RepositorySnapshot; onSelect: (id: string) => void }) {
  const events = [...repository.history].reverse().slice(0, 100);
  return <div className="content-page">
    <header className="page-heading"><div><h1>History</h1><p>Recent validations and service events</p></div></header>
    {repository.history_items.length > 0 && <section className="completed-list"><h2>Completed</h2>{repository.history_items.slice(0, 12).map((view) => <button key={view.item.id} onClick={() => onSelect(view.item.id)}><span>{view.item.metadata.subject}</span><small>{view.item.state}</small></button>)}</section>}
    <section className="history-feed">
      {events.length ? events.map((event) => <HistoryEvent key={event.id} event={event} />) : <p className="muted-empty">No history yet.</p>}
    </section>
  </div>;
}

function HistoryEvent({ event }: { event: DomainEvent }) {
  const appearance = eventLabel(event); const Icon = appearance.icon;
  return <article className="history-event"><span className={`history-event__icon tone-${appearance.tone}`}><Icon /></span><div><strong>{appearance.label}</strong><small>#{event.sequence} · {event.actor} · {relativeTime(event.created_at)}</small></div></article>;
}
