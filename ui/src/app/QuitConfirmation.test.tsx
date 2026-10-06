import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { demoSnapshot } from "../lib/demo-data";
import type { AppSnapshot, QueueItemKind, QueueItemState, QueueItemView } from "../lib/types";
import { App } from "./App";
import { activeWork } from "./activeWork";

const tauri = vi.hoisted(() => ({
  handlers: new Map<string, Set<() => void>>(),
  invoke: vi.fn<(command: string, args?: unknown) => Promise<unknown>>(),
}));

vi.mock("@tauri-apps/api/event", () => ({
  listen: async (event: string, handler: () => void) => {
    const handlers = tauri.handlers.get(event) ?? new Set();
    handlers.add(handler);
    tauri.handlers.set(event, handlers);
    return () => handlers.delete(handler);
  },
}));
vi.mock("@tauri-apps/api/core", () => ({ invoke: tauri.invoke }));

const QUIT_EVENT = "tollgate://quit-confirmation-required";

function setItem(view: QueueItemView, kind: QueueItemKind, state: QueueItemState, subject: string) {
  view.item.kind = kind;
  view.item.state = state;
  view.item.metadata.subject = subject;
}

/**
 * The demo snapshot with nothing in flight except one running gate item, one preparing check, and
 * one running release run in the first repository; the second repository is idle.
 */
function busySnapshot(change: (snapshot: AppSnapshot) => void = () => {}) {
  const snapshot = structuredClone(demoSnapshot);
  const [busy, idle] = snapshot.repositories;
  for (const view of [...busy!.queue, ...busy!.checks, ...(busy!.release_runs ?? [])]) view.item.state = "queued";
  setItem(busy!.queue[0]!, "gate", "running", "synthetic running gate item");
  setItem(busy!.queue[1]!, "gate", "promoted", "synthetic promoted gate item");
  setItem(busy!.checks[0]!, "independent-check", "preparing", "synthetic preparing check");
  setItem(busy!.release_runs![0]!, "release", "running", "synthetic running release run");
  busy!.resources.active_runs = 3;
  idle!.resources.active_runs = 0;
  change(snapshot);
  return snapshot;
}

let snapshot: AppSnapshot;

beforeEach(() => {
  localStorage.clear();
  tauri.handlers.clear();
  snapshot = busySnapshot();
  tauri.invoke.mockReset();
  tauri.invoke.mockImplementation(async (command) => {
    if (command === "snapshot") return structuredClone(snapshot);
    if (command === "history_items") return { items: [], total: 0, offset: 0 };
    if (command === "confirm_quit") return null;
    throw new Error(`unexpected command ${command}`);
  });
  (window as unknown as Record<string, unknown>).__TAURI_INTERNALS__ = {};
});

afterEach(() => {
  cleanup();
  delete (window as unknown as Record<string, unknown>).__TAURI_INTERNALS__;
});

async function renderApp() {
  render(<QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><App /></QueryClientProvider>);
  await screen.findByRole("heading", { name: "Runs" });
  await waitFor(() => expect(tauri.handlers.get(QUIT_EVENT)?.size).toBe(1));
}

function requestQuit() {
  act(() => { for (const handler of tauri.handlers.get(QUIT_EVENT) ?? []) handler(); });
}

const quitCalls = () => tauri.invoke.mock.calls.filter(([command]) => command === "confirm_quit");

test("asks for confirmation when the shell reports active work, listing exactly that work", async () => {
  await renderApp();
  expect(screen.queryByRole("alertdialog")).not.toBeInTheDocument();
  requestQuit();
  const dialog = await screen.findByRole("alertdialog");
  const work = await within(dialog).findByRole("list", { name: "Active work" });
  const repositories = within(work).getAllByRole("listitem").filter((entry) => entry.parentElement === work);
  expect(repositories).toHaveLength(1);
  expect(repositories[0]).toHaveTextContent(snapshot.repositories[0]!.state.name);
  expect(repositories[0]!.querySelector(".quit-confirm__count")).toHaveTextContent("3");
  const items = within(repositories[0]!).getByRole("list");
  expect(within(items).getAllByRole("listitem").map((entry) => entry.querySelector(".quit-confirm__subject")?.textContent)).toEqual([
    "synthetic running gate item", "synthetic preparing check", "synthetic running release run",
  ]);
  expect(dialog).not.toHaveTextContent("synthetic promoted gate item");
  expect(dialog).not.toHaveTextContent(snapshot.repositories[1]!.state.name);
  expect(within(dialog).getByRole("button", { name: "Keep running" })).toHaveFocus();
  expect(quitCalls()).toHaveLength(0);
});

test("confirming calls the shell's confirm_quit command once", async () => {
  await renderApp();
  requestQuit();
  const dialog = await screen.findByRole("alertdialog");
  fireEvent.click(within(dialog).getByRole("button", { name: /Quit/ }));
  await waitFor(() => expect(quitCalls()).toHaveLength(1));
  expect(await within(dialog).findByRole("status")).toBeInTheDocument();
  expect(within(dialog).getByRole("button", { name: /Quit/ })).toBeDisabled();
  expect(within(dialog).getByRole("button", { name: "Keep running" })).toBeDisabled();
});

test("keeping the app running dismisses the request without quitting", async () => {
  await renderApp();
  requestQuit();
  fireEvent.click(within(await screen.findByRole("alertdialog")).getByRole("button", { name: "Keep running" }));
  expect(screen.queryByRole("alertdialog")).not.toBeInTheDocument();
  requestQuit();
  fireEvent.keyDown(await screen.findByRole("alertdialog"), { key: "Escape" });
  expect(screen.queryByRole("alertdialog")).not.toBeInTheDocument();
  expect(quitCalls()).toHaveLength(0);
});

test("shows a failed confirm_quit and lets the user retry", async () => {
  await renderApp();
  const respond = tauri.invoke.getMockImplementation()!;
  tauri.invoke.mockImplementation(async (command, args) => {
    if (command === "confirm_quit") throw new Error("quit refused");
    return respond(command, args);
  });
  requestQuit();
  const dialog = await screen.findByRole("alertdialog");
  fireEvent.click(within(dialog).getByRole("button", { name: /Quit/ }));
  expect(await within(dialog).findByRole("alert")).toHaveTextContent("quit refused");
  expect(within(dialog).getByRole("button", { name: /Quit/ })).toBeEnabled();
  expect(within(dialog).getByRole("button", { name: "Keep running" })).toBeEnabled();
});

test("lists the live snapshot, so work that finished while asking is not shown", async () => {
  await renderApp();
  snapshot = busySnapshot((draft) => {
    for (const repository of draft.repositories) {
      repository.resources.active_runs = 0;
      for (const view of [...repository.queue, ...repository.checks, ...(repository.release_runs ?? [])]) view.item.state = "check-passed";
    }
  });
  requestQuit();
  const dialog = await screen.findByRole("alertdialog");
  await waitFor(() => expect(within(dialog).queryByRole("list", { name: "Active work" })).not.toBeInTheDocument());
  expect(within(dialog).getByRole("status")).toBeInTheDocument();
  expect(within(dialog).getByRole("button", { name: /Quit/ })).toBeEnabled();
});

test("active work follows the shell's rule: in-flight items or a running buildset", () => {
  expect(activeWork(undefined)).toEqual([]);
  const idle = busySnapshot((draft) => {
    for (const repository of draft.repositories) {
      repository.resources.active_runs = 0;
      for (const view of [...repository.queue, ...repository.checks, ...(repository.release_runs ?? [])]) view.item.state = "queued";
    }
  });
  expect(activeWork(idle)).toEqual([]);
  const buildsetOnly = structuredClone(idle);
  buildsetOnly.repositories[1]!.resources.active_runs = 1;
  expect(activeWork(buildsetOnly)).toEqual([{ repositoryId: buildsetOnly.repositories[1]!.state.id, repositoryName: buildsetOnly.repositories[1]!.state.name, items: [], runningBuildsets: 1 }]);
  const busy = busySnapshot();
  expect(activeWork(busy).map((repository) => repository.items.map((view) => view.item.id))).toEqual([[
    busy.repositories[0]!.queue[0]!.item.id, busy.repositories[0]!.checks[0]!.item.id, busy.repositories[0]!.release_runs![0]!.item.id,
  ]]);
});
