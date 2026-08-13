import { FlaskConical } from "lucide-react";
import type { RepositorySnapshot } from "../../lib/types";
import { QueueCard } from "./QueueCard";

export function QueueView({ repository, selectedItemId, onSelect }: { repository: RepositorySnapshot; selectedItemId: string | null; onSelect: (id: string | null) => void }) {
  return <div className="queue-view">
    <header className="page-heading"><div><h1>Queue</h1><p>{repository.queue.length} change{repository.queue.length === 1 ? "" : "s"} waiting for release</p></div></header>
    {repository.state.block_reasons[0] && <div className="notice"><strong>{repository.state.block_reasons[0].message}</strong><span>{repository.state.block_reasons[0].recovery_action}</span></div>}
    {repository.queue.length ? <section className="queue-list" aria-label="Promotion queue">
      {repository.queue.map((entry, index) => <QueueCard key={entry.item.id} view={entry} position={index + 1} selected={selectedItemId === entry.item.id} onSelect={() => onSelect(selectedItemId === entry.item.id ? null : entry.item.id)} />)}
    </section> : <section className="empty-state"><h2>Queue clear</h2><p>No changes are waiting for release.</p></section>}
    {repository.checks.length > 0 && <section className="checks-list"><h2><FlaskConical />Checks</h2>{repository.checks.map((entry, index) => <QueueCard key={entry.item.id} view={entry} position={index + 1} selected={selectedItemId === entry.item.id} onSelect={() => onSelect(selectedItemId === entry.item.id ? null : entry.item.id)} />)}</section>}
  </div>;
}
