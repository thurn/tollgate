import type { QueueItemView, ReleaseLag, RepositorySnapshot } from "../../lib/types";
import { timestampMs } from "../../lib/utils";

const failedStates = new Set(["check-failed", "infrastructure-exhausted"]);

/** `release`'s lag behind `staging`, in commits and age. */
export function releaseLagLabel(lag: ReleaseLag, now = Date.now()) {
  if (lag.commits === 0) return "none";
  const commits = `${lag.commits} commit${lag.commits === 1 ? "" : "s"}`;
  if (lag.since == null) return commits;
  const minutes = Math.max(0, Math.round((now - timestampMs(lag.since)) / 60_000));
  const age = minutes < 1 ? "<1m" : minutes < 60 ? `${minutes}m` : minutes < 48 * 60 ? `${Math.floor(minutes / 60)}h ${minutes % 60}m` : `${Math.floor(minutes / 1_440)}d`;
  return `${commits} · ${age}`;
}

export function hasReleaseStage(repository: RepositorySnapshot) {
  return repository.configuration.steps.some((step) => step.stage === "release");
}

/** The oldest unfinished release run (the one running or next to run) and the newest finished one that ran. */
export function releaseRuns(repository: RepositorySnapshot) {
  const runs = repository.release_runs ?? [];
  const terminal = (view: QueueItemView) => ["check-passed", "check-failed", "canceled", "superseded", "infrastructure-exhausted"].includes(view.item.state);
  const active = runs.filter((view) => !terminal(view)).sort((a, b) => a.item.enqueue_sequence - b.item.enqueue_sequence)[0];
  const last = runs.filter((view) => terminal(view) && view.item.state !== "superseded").sort((a, b) => b.item.enqueue_sequence - a.item.enqueue_sequence)[0];
  return { runs, active, last };
}

/** Voting steps that failed in a failed release run. */
export function failingSteps(view: QueueItemView) {
  if (!failedStates.has(view.item.state)) return [];
  const attributed = view.failure_attribution?.steps.map((step) => step.name) ?? [];
  if (attributed.length) return attributed;
  const results = view.buildset?.step_results ?? [];
  return (view.buildset?.frozen_steps ?? [])
    .filter((step) => step.voting && results.some((result) => result.name === step.name && !["success", "skipped"].includes(result.result_class)))
    .map((step) => step.name);
}
