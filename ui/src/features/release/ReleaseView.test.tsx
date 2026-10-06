import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { App } from "../../app/App";
import { demoSnapshot } from "../../lib/demo-data";
import type { RemoteOperation } from "../../lib/api";
import type { ReleaseRetryResult, RemoteSyncResult, RepositorySnapshot } from "../../lib/types";
import { ReleaseView } from "./ReleaseView";
import { failingSteps, releaseLagLabel, releaseRuns } from "./release";

beforeEach(() => localStorage.clear());
afterEach(cleanup);

function repository(change: (repository: RepositorySnapshot) => void = () => {}) {
  const value = structuredClone(demoSnapshot.repositories[0]!);
  change(value);
  return value;
}

function renderView(value: RepositorySnapshot, onRetry = vi.fn<() => Promise<ReleaseRetryResult>>(), onSelect = vi.fn(), onRemote = vi.fn<(operation: RemoteOperation) => Promise<RemoteSyncResult>>()) {
  const client = new QueryClient();
  const view = (next: RepositorySnapshot) => <QueryClientProvider client={client}><ReleaseView repository={next} selectedItemId={null} onSelect={onSelect} onRetry={onRetry} onRemote={onRemote} /></QueryClientProvider>;
  const { rerender } = render(view(value));
  return { onRetry, onSelect, onRemote, rerender: (next: RepositorySnapshot) => rerender(view(next)) };
}

function remoteResult(value: RepositorySnapshot, change: Partial<RemoteSyncResult> = {}): RemoteSyncResult {
  return { action: "up-to-date", local_master: value.state.release_oid, remote_master: value.state.release_oid, queue_revision: value.state.queue_revision, affected_item_ids: [], message: "remote result", ...change };
}

/** The demo repository whose staging moved outside Tollgate while its remote diverged. */
function blockedRemoteRepository(change: (repository: RepositorySnapshot) => void = () => {}) {
  const value = structuredClone(demoSnapshot.repositories[1]!);
  change(value);
  return value;
}

function retryResult(value: RepositorySnapshot, action: ReleaseRetryResult["action"]): ReleaseRetryResult {
  return { repository_id: value.state.id, action, item_id: "019fef58-0000-7000-8000-000000000001", target_oid: value.state.staging_oid, staging_oid: value.state.staging_oid, release_oid: value.state.release_oid, retry_of_item_id: null, superseded_item_ids: [] };
}

test("shows both refs, the lag, and the last run's step results", () => {
  const value = repository();
  renderView(value);
  const refs = screen.getByRole("region", { name: "Release refs" });
  expect(refs).toHaveTextContent(value.state.staging_oid.bytes.slice(0, 10));
  expect(refs).toHaveTextContent(value.state.release_oid.bytes.slice(0, 10));
  expect(refs).toHaveTextContent(value.state.staging_ref);
  expect(refs).toHaveTextContent(value.state.release_ref);
  expect(refs).toHaveTextContent(`${value.state.release_lag.commits} commits`);
  const steps = screen.getByRole("list", { name: "Step results" });
  const names = within(steps).getAllByRole("listitem").map((entry) => entry.querySelector("strong")?.textContent);
  expect(names).toEqual(["fast", "full"]);
  expect(within(steps).getAllByRole("listitem")[1]).toHaveTextContent("exit-failure");
  const runs = screen.getByRole("region", { name: "Release runs" });
  expect(within(runs).getAllByRole("article")).toHaveLength(value.release_runs!.length);
});

test("lists release holds and the promotion pause", () => {
  const value = repository((draft) => {
    draft.state.release_block_reasons = [{ code: "push-blocked", message: "Push did not land", recovery_action: "Run tg push" }];
    draft.state.promotion_pause = { code: "release-lag", message: "Promotion waits on release", recovery_action: "Approve a fix" };
  });
  renderView(value);
  const holds = screen.getAllByRole("status");
  expect(holds.map((hold) => hold.querySelector("strong")?.textContent)).toEqual(["Push did not land", "Promotion waits on release"]);
});

test("retries the release run and reports the queued run", async () => {
  const value = repository();
  const onRetry = vi.fn(async () => retryResult(value, "queued"));
  renderView(value, onRetry);
  fireEvent.click(screen.getByRole("button", { name: /Retry release run/ }));
  await waitFor(() => expect(onRetry).toHaveBeenCalledTimes(1));
  expect(await screen.findByRole("status")).toHaveTextContent(value.state.staging_oid.bytes.slice(0, 8));
});

test("shows why a retry had nothing to run", async () => {
  const value = repository();
  renderView(value, vi.fn(async () => { throw new Error("release already holds the staging tip"); }));
  fireEvent.click(screen.getByRole("button", { name: /Retry release run/ }));
  expect(await screen.findByRole("alert")).toHaveTextContent("release already holds the staging tip");
});

test("disables retry while a run for the staging tip is active and features that run", () => {
  const value = repository((draft) => {
    const running = structuredClone(draft.release_runs![0]!);
    running.item.id = "019fef58-7247-7973-9c03-e6aff447a5c9";
    running.item.state = "running";
    running.item.enqueue_sequence = 99;
    running.item.terminal_reason = undefined;
    running.item.retry_of_item_id = draft.release_runs![0]!.item.id;
    running.buildset!.step_results = [{ name: "fast", result_class: "running", elapsed_ms: 1_000, log_hash: "", stdout_end: 0, stderr_end: 0 }];
    draft.release_runs = [running, ...draft.release_runs!];
  });
  const { onSelect } = renderView(value);
  expect(screen.getByRole("button", { name: /Retry release run/ })).toBeDisabled();
  const featured = screen.getByRole("region", { name: "Running" });
  expect(featured).toHaveTextContent(value.release_runs![1]!.item.id.slice(0, 8));
  fireEvent.click(within(featured).getByRole("button", { name: /Open run/ }));
  expect(onSelect).toHaveBeenCalledWith("019fef58-7247-7973-9c03-e6aff447a5c9");
});

test("explains a repository without a release stage and offers no retry", () => {
  const value = repository((draft) => {
    draft.configuration.steps = draft.configuration.steps.filter((step) => step.stage !== "release");
    draft.release_runs = [];
  });
  renderView(value);
  expect(screen.queryByRole("button", { name: /Retry release run/ })).not.toBeInTheDocument();
  expect(screen.queryByRole("region", { name: "Release runs" })).not.toBeInTheDocument();
});

test("the Release tab opens a run in the item inspector", async () => {
  render(<QueryClientProvider client={new QueryClient()}><App /></QueryClientProvider>);
  fireEvent.click(await screen.findByRole("button", { name: "Release" }));
  expect(screen.getByRole("heading", { level: 1, name: "Release" })).toBeInTheDocument();
  const refs = screen.getByLabelText("Tollgate refs");
  expect(refs).toHaveTextContent(demoSnapshot.repositories[0]!.state.release_oid.bytes.slice(0, 7));
  const runs = screen.getByRole("region", { name: "Release runs" });
  fireEvent.click(within(within(runs).getAllByRole("article")[0]!).getAllByRole("button")[0]!);
  const inspector = await screen.findByRole("dialog");
  expect(inspector).toHaveTextContent(demoSnapshot.repositories[0]!.release_runs![0]!.generation!.anchored_base_oid.bytes.slice(0, 8));
});

test("release helpers read lag age, run order, and failing steps", () => {
  const now = Date.UTC(2026, 7, 15, 12, 0, 0);
  expect(releaseLagLabel({ commits: 3, since: [2026, 227, 11, 13, 0, 0, 0, 0, 0] }, now)).toContain("47m");
  expect(releaseLagLabel({ commits: 3, since: [2026, 227, 13, 13, 0, 0, 2, 0, 0] }, now)).toContain("47m");
  expect(releaseLagLabel({ commits: 3, since: null }, now)).not.toContain("·");
  expect(releaseLagLabel({ commits: 0, since: null }, now)).not.toContain("commit");
  const value = repository();
  const { active, last } = releaseRuns(value);
  expect(active).toBeUndefined();
  expect(last?.item.id).toBe(value.release_runs![0]!.item.id);
  expect(failingSteps(last!)).toEqual(["full"]);
  expect(failingSteps(value.release_runs![1]!)).toEqual([]);
});

test("pulls and pushes through the remote operations and reports each result", async () => {
  const value = blockedRemoteRepository();
  const onRemote = vi.fn(async (operation: RemoteOperation) => remoteResult(value, { action: operation.kind === "pull" ? "adopted-remote" : "pushed", message: `${operation.kind} done` }));
  renderView(value, undefined, undefined, onRemote);
  const remote = screen.getByRole("region", { name: "Remote" });
  fireEvent.click(within(remote).getByRole("button", { name: /Pull/ }));
  await waitFor(() => expect(onRemote).toHaveBeenLastCalledWith({ kind: "pull" }, expect.anything()));
  expect(await within(remote).findByRole("status")).toHaveTextContent("adopted-remote");
  fireEvent.click(within(remote).getByRole("button", { name: /Push/ }));
  await waitFor(() => expect(within(remote).getByRole("status")).toHaveTextContent("pushed"));
  expect(onRemote.mock.calls.map(([operation]) => operation)).toEqual([{ kind: "pull" }, { kind: "push" }]);
});

test("offers no pull or push without a remote but still offers reconcile", () => {
  renderView(repository());
  const remote = screen.getByRole("region", { name: "Remote" });
  expect(within(remote).queryByRole("button", { name: /Pull/ })).not.toBeInTheDocument();
  expect(within(remote).queryByRole("button", { name: /Push/ })).not.toBeInTheDocument();
  expect(within(remote).getByRole("button", { name: /Reconcile/ })).toBeEnabled();
});

test("reconcile previews its impact and runs only after confirmation with the previewed state", async () => {
  const value = blockedRemoteRepository((draft) => {
    draft.queue = [structuredClone(demoSnapshot.repositories[0]!.queue[0]!)];
    draft.queue[0]!.item.state = "promoted-local-push-pending";
  });
  const onRemote = vi.fn<(operation: RemoteOperation) => Promise<RemoteSyncResult>>(async () => remoteResult(value, { action: "reconciled-local", local_master: value.observed_master_oid }));
  renderView(value, undefined, undefined, onRemote);
  const remote = screen.getByRole("region", { name: "Remote" });

  fireEvent.click(within(remote).getByRole("button", { name: /Reconcile/ }));
  const confirmation = within(remote).getByRole("alertdialog");
  expect(confirmation).toHaveTextContent(value.observed_master_oid.bytes.slice(0, 10));
  expect(confirmation).toHaveTextContent(value.state.staging_oid.bytes.slice(0, 10));
  expect(confirmation).toHaveTextContent(value.state.block_reasons[0]!.code);
  expect(confirmation).toHaveTextContent(String(value.state.queue_revision));
  expect(within(confirmation).getAllByRole("listitem").some((entry) => entry.textContent?.includes("1 unfinished"))).toBe(true);
  fireEvent.click(within(confirmation).getByRole("button", { name: "Cancel" }));
  expect(within(remote).queryByRole("alertdialog")).not.toBeInTheDocument();
  expect(onRemote).not.toHaveBeenCalled();

  fireEvent.click(within(remote).getByRole("button", { name: /Reconcile/ }));
  fireEvent.click(within(within(remote).getByRole("alertdialog")).getByRole("button", { name: "Reconcile" }));
  await waitFor(() => expect(onRemote).toHaveBeenCalledTimes(1));
  expect(onRemote.mock.calls[0]![0]).toEqual({ kind: "reconcile", expectedObservedMaster: value.observed_master_oid, expectedQueueRevision: value.state.queue_revision });
  expect(await within(remote).findByRole("status")).toHaveTextContent("reconciled-local");
});

test("a reconcile preview that the repository outdated must be reviewed again", () => {
  const value = blockedRemoteRepository();
  const { onRemote, rerender } = renderView(value);
  const remote = screen.getByRole("region", { name: "Remote" });
  fireEvent.click(within(remote).getByRole("button", { name: /Reconcile/ }));
  rerender(blockedRemoteRepository((draft) => { draft.state.queue_revision += 1; }));
  const confirmation = within(remote).getByRole("alertdialog");
  expect(within(confirmation).queryByRole("button", { name: "Reconcile" })).not.toBeInTheDocument();
  fireEvent.click(within(confirmation).getByRole("button", { name: "Review again" }));
  expect(within(remote).getByRole("alertdialog")).toHaveTextContent(String(value.state.queue_revision + 1));
  expect(within(within(remote).getByRole("alertdialog")).getByRole("button", { name: "Reconcile" })).toBeEnabled();
  expect(onRemote).not.toHaveBeenCalled();
});

test("a refused remote operation shows the service error", async () => {
  const value = blockedRemoteRepository();
  renderView(value, undefined, undefined, vi.fn(async () => { throw "remote diverged from release"; }));
  const remote = screen.getByRole("region", { name: "Remote" });
  fireEvent.click(within(remote).getByRole("button", { name: /Push/ }));
  expect(await within(remote).findByRole("alert")).toHaveTextContent("remote diverged from release");
});
