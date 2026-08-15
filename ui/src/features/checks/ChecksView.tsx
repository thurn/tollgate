import type { QueueItemState, QueueItemView, RepositorySnapshot } from "../../lib/types";
import { QueueCard } from "../queue/QueueCard";
import { HistoryLoader } from "../runs/HistoryLoader";

const terminalStates = new Set<QueueItemState>([
  "promoted", "externally-integrated", "failed", "merge-conflict", "dependency-failed", "canceled", "superseded", "infrastructure-exhausted", "check-passed", "check-failed",
]);

export function ChecksView({ repository, historyItems, selectedItemId, onSelect, hasMore, loadingMore, onLoadMore }: {
  repository: RepositorySnapshot;
  historyItems: QueueItemView[];
  selectedItemId: string | null;
  onSelect: (id: string | null) => void;
  hasMore: boolean;
  loadingMore: boolean;
  onLoadMore: () => unknown;
}) {
  const active = repository.checks.filter((view) => !terminalStates.has(view.item.state));
  const completed = historyItems.filter((view) => view.item.kind === "independent-check");
  const seen = new Set<string>();
  const checks = [...active, ...completed].filter((view) => !seen.has(view.item.id) && !!seen.add(view.item.id));

  return <div className="runs-view">
    <header className="page-heading"><div><h1>Checks</h1><p>Every independent check, active and completed</p></div></header>
    {checks.length ? <section className="runs-list" aria-label="Independent checks">
      {checks.map((entry) => <QueueCard key={entry.item.id} view={entry} selected={selectedItemId === entry.item.id} onSelect={() => onSelect(selectedItemId === entry.item.id ? null : entry.item.id)} />)}
    </section> : <section className="empty-state"><h2>No checks yet</h2><p>Independent checks will appear here when they are run.</p></section>}
    <HistoryLoader hasMore={hasMore} loading={loadingMore} onLoadMore={onLoadMore} label="checks" />
  </div>;
}
