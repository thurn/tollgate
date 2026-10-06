import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { ArrowDownToLine, ArrowUpFromLine, GitMerge } from "lucide-react";
import type { RemoteOperation } from "../../lib/api";
import { oidHex, type BlockReason, type GitOid, type RemoteSyncResult, type RepositorySnapshot } from "../../lib/types";
import { Button } from "../../components/ui/Button";
import { shortId } from "../../lib/utils";

/** What `tg reconcile` will do, frozen when the confirmation opens. */
interface ReconcilePreview {
  observedStaging: GitOid;
  recordedStaging: GitOid;
  queueRevision: number;
  holds: BlockReason[];
  abandonedPushes: number;
}

function reconcilePreview(repository: RepositorySnapshot): ReconcilePreview {
  const { state } = repository;
  return {
    observedStaging: repository.observed_master_oid,
    recordedStaging: state.staging_oid,
    queueRevision: state.queue_revision,
    holds: [...state.block_reasons, ...state.release_block_reasons],
    abandonedPushes: repository.queue.filter((view) => view.item.state === "promoted-local-push-pending").length,
  };
}

/** Whether the repository moved since `preview` was taken, so confirming would be refused. */
function previewIsStale(preview: ReconcilePreview, repository: RepositorySnapshot) {
  return preview.queueRevision !== repository.state.queue_revision
    || oidHex(preview.observedStaging) !== oidHex(repository.observed_master_oid);
}

const short = (oid: GitOid) => shortId(oidHex(oid), 10);

/** Pull, push, and reconcile: the remote and ref-adoption operations of `tg pull`, `tg push`, and `tg reconcile`. */
export function RemoteOperations({ repository, onRun }: {
  repository: RepositorySnapshot;
  onRun: (operation: RemoteOperation) => Promise<RemoteSyncResult>;
}) {
  const remote = repository.configuration;
  const operation = useMutation({ mutationFn: onRun });
  const [preview, setPreview] = useState<ReconcilePreview | null>(null);
  const running = (kind: RemoteOperation["kind"]) => operation.isPending && operation.variables?.kind === kind;
  const stale = preview != null && previewIsStale(preview, repository);

  function confirmReconcile() {
    if (!preview) return;
    operation.mutate({ kind: "reconcile", expectedObservedMaster: preview.observedStaging, expectedQueueRevision: preview.queueRevision });
    setPreview(null);
  }

  return <section className="remote-ops" aria-labelledby="remote-ops-title">
    <header className="remote-ops__header">
      <div>
        <h2 id="remote-ops-title">Remote</h2>
        <p>{remote.remote_enabled ? <><code>{remote.remote_name}/{remote.remote_branch}</code> follows <code>{repository.state.release_ref}</code></> : "Remote synchronization is off for this repository"}</p>
      </div>
      <div className="remote-ops__actions">
        {remote.remote_enabled && <>
          <Button size="sm" onClick={() => operation.mutate({ kind: "pull" })} loading={running("pull")} disabled={operation.isPending}><ArrowDownToLine aria-hidden />Pull</Button>
          <Button size="sm" onClick={() => operation.mutate({ kind: "push" })} loading={running("push")} disabled={operation.isPending}><ArrowUpFromLine aria-hidden />Push</Button>
        </>}
        <Button size="sm" onClick={() => setPreview(reconcilePreview(repository))} loading={running("reconcile")} disabled={operation.isPending || preview != null} aria-haspopup="dialog"><GitMerge aria-hidden />Reconcile…</Button>
      </div>
    </header>
    {preview && <div className="remote-ops__confirm" role="alertdialog" aria-labelledby="reconcile-title" aria-describedby="reconcile-impact">
      <h3 id="reconcile-title">Reconcile with the observed refs?</h3>
      <ul id="reconcile-impact">
        <li>{oidHex(preview.observedStaging) === oidHex(preview.recordedStaging)
          ? <>Adopts <code>{repository.state.staging_ref}</code> at <code>{short(preview.observedStaging)}</code>, where Tollgate recorded it.</>
          : <>Adopts <code>{repository.state.staging_ref}</code> at <code>{short(preview.observedStaging)}</code>; Tollgate recorded <code>{short(preview.recordedStaging)}</code>.</>}</li>
        <li>Adopts <code>{repository.state.release_ref}</code> where it is now (recorded at <code>{short(repository.state.release_oid)}</code>).</li>
        <li>Clears the blocks that external ref movement or the remote raised, and the release holds. {preview.holds.length > 0
          ? <>Active now: {preview.holds.map((reason, index) => <span key={reason.code}>{index > 0 && ", "}<code>{reason.code}</code></span>)}.</>
          : "None is active now."}</li>
        {preview.abandonedPushes > 0 && <li>Abandons {preview.abandonedPushes} unfinished remote {preview.abandonedPushes === 1 ? "push" : "pushes"}.</li>}
        <li>Applies only at queue revision {preview.queueRevision}.</li>
      </ul>
      {stale && <p className="remote-ops__stale" role="status">The repository changed since this preview. Review it again before reconciling.</p>}
      <div className="remote-ops__confirm-actions">
        <Button size="sm" variant="ghost" onClick={() => setPreview(null)}>Cancel</Button>
        {stale
          ? <Button size="sm" onClick={() => setPreview(reconcilePreview(repository))}>Review again</Button>
          : <Button size="sm" variant="danger" onClick={confirmReconcile}>Reconcile</Button>}
      </div>
    </div>}
    {operation.isSuccess && <p className="release-feedback" role="status"><strong>{operation.data.message}</strong> <span className="remote-ops__result">{operation.data.action} · local <code>{short(operation.data.local_master)}</code>{operation.data.remote_master && <> · remote <code>{short(operation.data.remote_master)}</code></>}</span></p>}
    {operation.isError && <p className="release-feedback release-feedback--error" role="alert">{operation.error instanceof Error ? operation.error.message : String(operation.error)}</p>}
  </section>;
}
