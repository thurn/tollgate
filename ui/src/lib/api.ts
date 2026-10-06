import { invoke } from "@tauri-apps/api/core";
import type { AppSnapshot, GitOid, HistoryItemsPage, QueueItemView, ReleaseRetryResult, RemoteSyncResult, Timestamp } from "./types";
import { demoSnapshot } from "./demo-data";
import { isTauri } from "./utils";

export async function getSnapshot(): Promise<AppSnapshot> {
  if (!isTauri()) return structuredClone(demoSnapshot);
  return invoke<AppSnapshot>("snapshot");
}

/** `tg release retry`: rerun the newest staging tip as a release run. */
export async function retryRelease(repositoryId: string): Promise<ReleaseRetryResult> {
  if (!isTauri()) {
    const repository = demoSnapshot.repositories.find((candidate) => candidate.state.id === repositoryId);
    if (!repository) throw new Error(`repository ${repositoryId} is not registered`);
    return {
      repository_id: repositoryId, action: "queued", item_id: crypto.randomUUID(), target_oid: repository.state.staging_oid,
      staging_oid: repository.state.staging_oid, release_oid: repository.state.release_oid,
      retry_of_item_id: repository.release_runs?.[0]?.item.id ?? null, superseded_item_ids: [],
    };
  }
  return invoke("release_retry", { repositoryId });
}

/** A remote operation from the Release route: `tg pull`, `tg push`, or `tg reconcile`. */
export type RemoteOperation =
  | { kind: "pull" }
  | { kind: "push" }
  /** Reconcile carries the previewed observed `staging` and queue revision; the service refuses it if either changed. */
  | { kind: "reconcile"; expectedObservedMaster: GitOid; expectedQueueRevision: number };

export async function runRemoteOperation(repositoryId: string, operation: RemoteOperation): Promise<RemoteSyncResult> {
  if (!isTauri()) return demoRemoteResult(repositoryId, operation);
  if (operation.kind === "reconcile") {
    return invoke("reconcile", { repositoryId, expectedObservedMaster: operation.expectedObservedMaster, expectedQueueRevision: operation.expectedQueueRevision });
  }
  return invoke(operation.kind, { repositoryId });
}

function demoRemoteResult(repositoryId: string, operation: RemoteOperation): RemoteSyncResult {
  const repository = demoSnapshot.repositories.find((candidate) => candidate.state.id === repositoryId);
  if (!repository) throw new Error(`repository ${repositoryId} is not registered`);
  if (operation.kind === "reconcile") {
    return { action: "reconciled-local", local_master: operation.expectedObservedMaster, remote_master: null, queue_revision: operation.expectedQueueRevision + 1, affected_item_ids: [], message: "Adopted the observed refs and cleared the repository blocks." };
  }
  if (!repository.configuration.remote_enabled) throw new Error("remote synchronization is disabled for this repository");
  return { action: "up-to-date", local_master: repository.state.release_oid, remote_master: repository.state.release_oid, queue_revision: repository.state.queue_revision, affected_item_ids: [], message: operation.kind === "pull" ? "The remote has nothing new." : "The remote already has release." };
}

export async function getItemDetails(repositoryId: string, itemId: string): Promise<QueueItemView> {
  return invoke("item_details", { repositoryId, itemId });
}

export async function getHistoryItems(repositoryId: string, offset: number, limit: number): Promise<HistoryItemsPage> {
  if (!isTauri()) {
    const items = demoSnapshot.repositories.find((repository) => repository.state.id === repositoryId)?.history_items ?? [];
    return structuredClone({ items: items.slice(offset, offset + limit), total: items.length, offset });
  }
  return invoke("history_items", { repositoryId, offset, limit });
}

export interface LogFrameView {
  frame: {
    stream: "stdout" | "stderr";
    stream_offset: number;
    broker_sequence: number;
    monotonic_ns: number;
    wall_time: Timestamp;
    payload_len: number;
  };
  text: string;
  invalid_utf8: boolean;
}

export async function getLogs(repositoryId: string, itemId: string, buildsetId: string | undefined, step?: string, startSequence = 0, tail = false): Promise<LogFrameView[]> {
  if (!isTauri()) return [];
  return invoke("logs", { repositoryId, itemId, buildsetId, step, startSequence, tail });
}

export async function openRawLog(repositoryId: string, itemId: string, buildsetId: string | undefined, step?: string): Promise<void> {
  return invoke("open_raw_log", { repositoryId, itemId, buildsetId, step });
}
