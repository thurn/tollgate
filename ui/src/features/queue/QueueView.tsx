import { CircleAlert, ChevronRight, FlaskConical } from "lucide-react";
import { isMasterPushFailure, type QueueItemView, type RepositorySnapshot } from "../../lib/types";
import { QueueCard } from "./QueueCard";

export function QueueView({ repository, selectedItemId, onSelect }: { repository: RepositorySnapshot; selectedItemId: string | null; onSelect: (id: string | null) => void }) {
  const failedMasterPush = isMasterPushFailure(repository.master_push) ? repository.master_push : undefined;
  return <div className="queue-view">
    <header className="page-heading"><div><h1>Queue</h1><p>{failedMasterPush ? "A master push needs your attention" : `${repository.queue.length} change${repository.queue.length === 1 ? "" : "s"} waiting for release`}</p></div></header>
    {repository.state.block_reasons[0] && <div className="notice"><strong>{repository.state.block_reasons[0].message}</strong><span>{repository.state.block_reasons[0].recovery_action}</span></div>}
    {failedMasterPush && <MasterPushFailure view={failedMasterPush} onSelect={() => onSelect(failedMasterPush.item.id)} />}
    {repository.queue.length ? <section className="queue-list" aria-label="Promotion queue">
      {repository.queue.map((entry, index) => <QueueCard key={entry.item.id} view={entry} position={index + 1} selected={selectedItemId === entry.item.id} onSelect={() => onSelect(selectedItemId === entry.item.id ? null : entry.item.id)} />)}
    </section> : !failedMasterPush && <section className="empty-state"><h2>Queue clear</h2><p>No changes are waiting for release.</p></section>}
    {repository.checks.length > 0 && <section className="checks-list"><h2><FlaskConical />Checks</h2>{repository.checks.map((entry, index) => <QueueCard key={entry.item.id} view={entry} position={index + 1} selected={selectedItemId === entry.item.id} onSelect={() => onSelect(selectedItemId === entry.item.id ? null : entry.item.id)} />)}</section>}
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
