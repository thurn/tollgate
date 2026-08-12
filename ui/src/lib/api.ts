import { invoke } from "@tauri-apps/api/core";
import type { AppSnapshot, DoctorReport } from "./types";
import { demoSnapshot } from "./demo-data";
import { isTauri } from "./utils";

export async function getSnapshot(): Promise<AppSnapshot> {
  if (!isTauri()) return structuredClone(demoSnapshot);
  return invoke<AppSnapshot>("snapshot");
}

export async function initializeRepository(path: string, run?: string) {
  return invoke("initialize_repository", { path, run, detachMaster: false });
}

export async function approve(repositoryId: string, revision = "HEAD", worktreePath?: string) {
  return invoke("approve", { repositoryId, revision, worktreePath });
}

export async function submitCandidate(repositoryId: string, revision = "HEAD", worktreePath?: string) {
  return invoke("submit_candidate", { repositoryId, revision, worktreePath });
}

export async function authorizeCandidate(repositoryId: string, itemId: string, expectedRevision: number) {
  return invoke("authorize_candidate", { repositoryId, itemId, expectedRevision });
}

export async function check(repositoryId: string, revision = "HEAD", worktreePath?: string) {
  return invoke("check", { repositoryId, revision, worktreePath });
}

export async function retryItem(repositoryId: string, itemId: string, cold: boolean) {
  return invoke("retry_item", { repositoryId, itemId, cold });
}

export async function cancel(repositoryId: string, itemId: string, expectedRevision: number) {
  return invoke("cancel_item", { repositoryId, itemId, expectedRevision });
}

export async function reorderQueue(repositoryId: string, selectedIds: string[], expectedRevision: number) {
  return invoke("reorder_queue", { repositoryId, selectedIds, expectedRevision });
}

export async function pull(repositoryId: string) {
  return invoke<{ action: string; message: string }>("pull", { repositoryId });
}

export async function push(repositoryId: string) {
  return invoke<{ action: string; message: string }>("push", { repositoryId });
}

export async function reconcile(repositoryId: string, expectedObservedMaster: import("./types").GitOid, expectedQueueRevision: number) {
  return invoke<{ action: string; message: string }>("reconcile", { repositoryId, expectedObservedMaster, expectedQueueRevision });
}

export async function updateFeature(repositoryId: string, worktreePath: string) {
  return invoke<{ message: string }>("update_feature", { repositoryId, worktreePath });
}

export async function createWorktree(repositoryId: string, branch: string, destination?: string) {
  return invoke<{ message: string; path: string }>("create_worktree", { repositoryId, branch, destination });
}

export async function removeWorktree(repositoryId: string, path: string) {
  return invoke<{ message: string }>("remove_worktree", { repositoryId, path });
}

export async function setPaused(repositoryId: string, paused: boolean) {
  return invoke("set_paused", { repositoryId, paused });
}

export async function reloadEnvironment() {
  return invoke("reload_environment");
}

export async function applyConfiguration(repositoryId: string) {
  return invoke("apply_configuration", { repositoryId });
}

export async function regenerateConfiguration(repositoryId: string) {
  return invoke("regenerate_configuration", { repositoryId });
}

export async function resetSlot(repositoryId: string, slotId: string) {
  return invoke("reset_slot", { repositoryId, slotId });
}

export async function snapshotCache(repositoryId: string) {
  return invoke<{ message: string }>("snapshot_cache", { repositoryId });
}

export async function purgeCache(repositoryId: string, allSlots: boolean) {
  return invoke<{ message: string }>("purge_cache", { repositoryId, allSlots });
}

export async function setArtifactPinned(repositoryId: string, artifactId: string, pinned: boolean) {
  return invoke<{ message: string }>("set_artifact_pinned", { repositoryId, artifactId, pinned });
}

export async function pruneArtifact(repositoryId: string, artifactId: string) {
  return invoke<{ message: string }>("prune_artifact", { repositoryId, artifactId });
}

export async function removeRepository(repositoryId: string) {
  return invoke<{ message: string }>("remove_repository", { repositoryId });
}

export interface LogFrameView { frame: { stream: "stdout" | "stderr"; stream_offset: number; broker_sequence: number; monotonic_ns: number; wall_time: string; payload_len: number }; text: string; invalid_utf8: boolean }
export async function getLogs(repositoryId: string, itemId: string, buildsetId: string | undefined, step?: string, startSequence = 0, tail = false): Promise<LogFrameView[]> {
  if (!isTauri()) return [];
  return invoke("logs", { repositoryId, itemId, buildsetId, step, startSequence, tail });
}
export async function getHistoryItems(repositoryId: string, offset: number, limit: number): Promise<import("./types").HistoryItemsPage> {
  return invoke("history_items", { repositoryId, offset, limit });
}
export async function getItemDetails(repositoryId: string, itemId: string): Promise<import("./types").QueueItemView> {
  return invoke("item_details", { repositoryId, itemId });
}
export async function openRawLog(repositoryId: string, itemId: string, buildsetId: string | undefined, step?: string): Promise<void> {
  return invoke("open_raw_log", { repositoryId, itemId, buildsetId, step });
}

export async function getDoctorReport(repositoryId: string): Promise<DoctorReport> {
  if (!isTauri()) return { repository_id: repositoryId, generated_at: new Date().toISOString(), healthy: true, checks: [{ name: "Browser preview", status: "healthy", detail: "Demo diagnostics are available only for interface review." }] };
  return invoke("doctor", { repositoryId });
}

export async function confirmQuit() {
  return invoke("confirm_quit");
}

export interface CliInstallStatus { bundled_available: boolean; installed: boolean; destination: string; directory_on_path: boolean }
export async function getCliInstallStatus(): Promise<CliInstallStatus> {
  if (!isTauri()) return { bundled_available: false, installed: false, destination: "~/.local/bin/tg", directory_on_path: false };
  return invoke("cli_install_status");
}
export async function installCli(): Promise<CliInstallStatus> {
  return invoke("install_cli");
}

export interface NotificationPreferences { quiet_mode: boolean; muted_repositories: string[] }
export async function getNotificationPreferences(): Promise<NotificationPreferences> {
  if (!isTauri()) return { quiet_mode: false, muted_repositories: [] };
  return invoke("notification_preferences");
}
export async function setNotificationPreferences(quietMode: boolean, mutedRepositories: string[]): Promise<NotificationPreferences> {
  return invoke("set_notification_preferences", { quietMode, mutedRepositories });
}
