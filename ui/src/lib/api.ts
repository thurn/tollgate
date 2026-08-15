import { invoke } from "@tauri-apps/api/core";
import type { AppSnapshot, HistoryItemsPage, QueueItemView } from "./types";
import { demoSnapshot } from "./demo-data";
import { isTauri } from "./utils";

export async function getSnapshot(): Promise<AppSnapshot> {
  if (!isTauri()) return structuredClone(demoSnapshot);
  return invoke<AppSnapshot>("snapshot");
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
    wall_time: string;
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
