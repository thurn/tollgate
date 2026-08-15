import { CircleAlert, ChevronRight } from "lucide-react";
import { isMasterPushFailure, type QueueItemView, type RepositorySnapshot } from "../../lib/types";
import { QueueCard } from "../queue/QueueCard";
import { HistoryLoader } from "./HistoryLoader";

export function RunsView({ repository, historyItems, selectedItemId, onSelect, hasMore, loadingMore, onLoadMore }: {
  repository: RepositorySnapshot;
  historyItems: QueueItemView[];
  selectedItemId: string | null;
  onSelect: (id: string | null) => void;
  hasMore: boolean;
  loadingMore: boolean;
  onLoadMore: () => unknown;
}) {
  const failedMasterPush = isMasterPushFailure(repository.master_push) ? repository.master_push : undefined;
  const completed = historyItems.filter((view) => view.item.kind === "gate");
  const positions = new Map(repository.queue.map((view, index) => [view.item.id, index + 1]));
  const seen = new Set<string>();
  const runs = [...repository.queue, ...completed].filter((view) => !seen.has(view.item.id) && !!seen.add(view.item.id));

  return <div className="runs-view">
    <header className="page-heading"><div><h1>Runs</h1><p>{failedMasterPush ? "A master push needs your attention" : "Every gate run, active and completed"}</p></div></header>
    {repository.state.block_reasons[0] && <div className="notice"><strong>{repository.state.block_reasons[0].message}</strong><span>{repository.state.block_reasons[0].recovery_action}</span></div>}
    {failedMasterPush && <MasterPushFailure view={failedMasterPush} onSelect={() => onSelect(failedMasterPush.item.id)} />}
    {runs.length ? <section className="runs-list" aria-label="Gate runs">
      {runs.map((entry) => <QueueCard key={entry.item.id} view={entry} position={positions.get(entry.item.id)} selected={selectedItemId === entry.item.id} onSelect={() => onSelect(selectedItemId === entry.item.id ? null : entry.item.id)} />)}
    </section> : !failedMasterPush && <section className="empty-state"><h2>No runs yet</h2><p>Gate runs will appear here when they are submitted.</p></section>}
    <HistoryLoader hasMore={hasMore} loading={loadingMore} onLoadMore={onLoadMore} />
  </div>;
}

function MasterPushFailure({ view, onSelect }: { view: QueueItemView; onSelect: () => void }) {
  const failedStep = view.failure_attribution?.steps[0] ?? view.buildset?.step_results.find((step) => !["success", "skipped"].includes(step.result_class));
  return <section className="master-push-failure" role="alert" aria-labelledby="master-push-failure-title">
    <span className="master-push-failure__icon"><CircleAlert /></span>
    <div className="master-push-failure__body">
      <span className="master-push-failure__eyebrow">Action required</span>
      <h2 id="master-push-failure-title">Master push failed</h2>
      <p><strong>{view.item.metadata.subject}</strong> was not pushed to the remote.</p>
      <small>{failedStep ? <>Step <code>{failedStep.name}</code> failed. Open the entry for logs and diagnostics.</> : "Open the entry for failure details and logs."}</small>
    </div>
    <button className="master-push-failure__action" onClick={onSelect}>View failure <ChevronRight /></button>
  </section>;
}
