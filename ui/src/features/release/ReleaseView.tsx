import { useMutation } from "@tanstack/react-query";
import { ChevronRight, RotateCcw } from "lucide-react";
import type { RemoteOperation } from "../../lib/api";
import { oidHex, type GitOid, type QueueItemView, type ReleaseRetryResult, type ReleaseState, type RemoteSyncResult, type RepositorySnapshot } from "../../lib/types";
import { Badge } from "../../components/ui/Badge";
import { Button } from "../../components/ui/Button";
import { StatusGlyph, itemStatus } from "../../components/StatusGlyph";
import { cn, formatDuration, shortId } from "../../lib/utils";
import { QueueCard } from "../queue/QueueCard";
import { failingSteps, hasReleaseStage, releaseLagLabel, releaseRuns } from "./release";
import { RemoteOperations } from "./RemoteOperations";

const stateBadges: Record<ReleaseState, { label: string; tone: "success" | "info" | "danger" }> = {
  green: { label: "Released", tone: "success" },
  pending: { label: "Validating", tone: "info" },
  failing: { label: "Failing", tone: "danger" },
};

export function ReleaseView({ repository, selectedItemId, onSelect, onRetry, onRemote }: {
  repository: RepositorySnapshot;
  selectedItemId: string | null;
  onSelect: (id: string | null) => void;
  onRetry: () => Promise<ReleaseRetryResult>;
  onRemote: (operation: RemoteOperation) => Promise<RemoteSyncResult>;
}) {
  const state = repository.state;
  const stage = hasReleaseStage(repository);
  const { runs, active, last } = releaseRuns(repository);
  const activeOnTip = !!active && oidHex(active.item.source_oid) === oidHex(state.staging_oid);
  const retry = useMutation({ mutationFn: onRetry });
  const holds = [...state.release_block_reasons, ...(state.promotion_pause ? [state.promotion_pause] : [])];
  const featured = active ?? last;
  const toggle = (id: string) => onSelect(selectedItemId === id ? null : id);
  const badge = stateBadges[state.release_state];
  const subtitle = !stage ? "Release follows staging on every promotion"
    : state.release_state === "failing" ? "The latest release run failed; release stays at its last validated commit"
    : state.release_state === "pending" ? "Newer staging commits are waiting for the release stage"
    : "Release matches staging";

  return <div className="runs-view">
    <header className="page-heading">
      <div><h1>Release</h1><p>{subtitle}</p></div>
      {stage && <Button onClick={() => retry.mutate()} loading={retry.isPending} disabled={activeOnTip} title={activeOnTip ? "A release run for the staging tip is already queued or running" : undefined}>
        <RotateCcw aria-hidden />Retry release run
      </Button>}
    </header>
    <section className="release-refs" aria-label="Release refs">
      <RefCell label="Staging" refName={state.staging_ref} oid={state.staging_oid} />
      <RefCell label="Release" refName={state.release_ref} oid={state.release_oid} />
      <div className="release-refs__cell">
        <span>Lag</span>
        <strong>{releaseLagLabel(state.release_lag)}</strong>
        {stage && <Badge tone={badge.tone} dot>{badge.label}</Badge>}
      </div>
    </section>
    {holds.map((reason) => <div key={reason.code} className="notice" role="status"><strong>{reason.message}</strong><span>{reason.recovery_action}</span></div>)}
    {retry.isSuccess && <p className="release-feedback" role="status">{retry.data.action === "queued"
      ? <>Release run queued for <code>{shortId(oidHex(retry.data.target_oid), 8)}</code>.</>
      : <>A release run for <code>{shortId(oidHex(retry.data.target_oid), 8)}</code> is already queued or running.</>}</p>}
    {retry.isError && <p className="release-feedback release-feedback--error" role="alert">{retry.error instanceof Error ? retry.error.message : String(retry.error)}</p>}
    <RemoteOperations key={state.id} repository={repository} onRun={onRemote} />
    {!stage ? <section className="empty-state"><h2>No release stage</h2><p>Add steps with <code>stage = "release"</code> to validate releases after promotion.</p></section> : <>
      {featured && <FeaturedRun view={featured} current={featured === active} onOpen={() => toggle(featured.item.id)} />}
      {runs.length ? <section className="runs-list" aria-label="Release runs">
        {runs.map((view) => <QueueCard key={view.item.id} view={view} selected={selectedItemId === view.item.id} onSelect={() => toggle(view.item.id)} />)}
      </section> : <section className="empty-state"><h2>No release runs yet</h2><p>A release run starts after the next promotion to staging.</p></section>}
    </>}
  </div>;
}

function RefCell({ label, refName, oid }: { label: string; refName: string; oid: GitOid }) {
  return <div className="release-refs__cell">
    <span>{label}</span>
    <strong><code>{shortId(oidHex(oid), 10)}</code></strong>
    <small>{refName}</small>
  </div>;
}

function FeaturedRun({ view, current, onOpen }: { view: QueueItemView; current: boolean; onOpen: () => void }) {
  const status = itemStatus(view.item.state, view.item.kind);
  const failing = failingSteps(view);
  const steps = view.buildset?.step_results ?? [];
  return <section className={cn("release-run", failing.length > 0 && "is-failing")} aria-labelledby="release-run-title">
    <header className="release-run__header">
      <StatusGlyph state={view.item.state} kind={view.item.kind} />
      <div><small>{current ? "Current run" : "Last run"}</small><h2 id="release-run-title">{status.label}</h2></div>
      <button className="release-run__open" onClick={onOpen}>Open run <ChevronRight aria-hidden /></button>
    </header>
    <dl className="release-run__facts">
      <div><dt>Target</dt><dd><code>{shortId(oidHex(view.item.source_oid), 10)}</code></dd></div>
      <div><dt>Range</dt><dd><code>{shortId(oidHex(view.generation?.anchored_base_oid), 8)}..{shortId(oidHex(view.generation?.tested_oid), 8)}</code></dd></div>
      <div><dt>Elapsed</dt><dd>{formatDuration(view.elapsed_ms)}</dd></div>
      {view.item.retry_of_item_id && <div><dt>Retry of</dt><dd><code>{shortId(view.item.retry_of_item_id, 8)}</code></dd></div>}
    </dl>
    {steps.length > 0 && <ul className="release-run__steps" aria-label="Step results">
      {steps.map((step) => {
        const failed = !["success", "running", "pending", "skipped"].includes(step.result_class);
        return <li key={step.name}>
          <span className={cn("step-state", step.result_class === "success" ? "is-success" : failed ? "is-failure" : "is-active")} />
          <strong>{step.name}</strong>
          <small>{step.result_class} · {formatDuration(step.elapsed_ms)}</small>
        </li>;
      })}
    </ul>}
    {failing.length > 0 && <p className="release-run__failing">Failing {failing.length === 1 ? "step" : "steps"}: {failing.map((name, index) => <span key={name}>{index > 0 && ", "}<code>{name}</code></span>)}</p>}
  </section>;
}
