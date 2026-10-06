import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { App } from "../../app/App";
import { demoSnapshot } from "../../lib/demo-data";
import type { ReleaseRetryResult, RepositorySnapshot } from "../../lib/types";
import { ReleaseView } from "./ReleaseView";
import { failingSteps, releaseLagLabel, releaseRuns } from "./release";

beforeEach(() => localStorage.clear());
afterEach(cleanup);

function repository(change: (repository: RepositorySnapshot) => void = () => {}) {
  const value = structuredClone(demoSnapshot.repositories[0]!);
  change(value);
  return value;
}

function renderView(value: RepositorySnapshot, onRetry = vi.fn<() => Promise<ReleaseRetryResult>>(), onSelect = vi.fn()) {
  render(<QueryClientProvider client={new QueryClient()}><ReleaseView repository={value} selectedItemId={null} onSelect={onSelect} onRetry={onRetry} /></QueryClientProvider>);
  return { onRetry, onSelect };
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
