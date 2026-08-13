import { invoke } from "@tauri-apps/api/core";
import type { AppSnapshot, QueueItemView } from "./types";
import { demoSnapshot } from "./demo-data";
import { isTauri } from "./utils";

export async function getSnapshot(): Promise<AppSnapshot> {
  if (!isTauri()) return structuredClone(demoSnapshot);
  return invoke<AppSnapshot>("snapshot");
}

export async function getItemDetails(repositoryId: string, itemId: string): Promise<QueueItemView> {
  return invoke("item_details", { repositoryId, itemId });
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
