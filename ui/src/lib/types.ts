export type RepositoryExecutionState = "active" | "paused" | "configuration-pending" | "blocked";
export type QueueItemState =
  | "constructing" | "queued" | "preparing" | "running" | "ready" | "promoting"
  | "promoted-local-push-pending" | "promoted" | "externally-integrated" | "failed"
  | "merge-conflict" | "dependency-failed" | "canceled" | "superseded" | "infrastructure-exhausted"
  | "check-passed" | "check-failed";
export type QueueItemKind = "gate" | "independent-check";
export type BuildsetState = "pending" | "preparing" | "running" | "passed" | "passed-with-warnings" | "failed" | "interrupted" | "canceled" | "invalidated" | "infrastructure-exhausted";
export type RemoteState = "disabled" | "preflight-pending" | "ready" | "pushing" | "push-blocked" | "synchronized" | "abandoned";
export type CleanupState = "not-eligible" | "pending" | "running" | "completed" | "needs-attention";

export interface GitOid { format: "sha1" | "sha256"; bytes: string }
export interface BlockReason { code: string; message: string; recovery_action: string }
export interface RepositoryState {
  id: string; name: string; path: string; integration_ref: string; master_oid: GitOid;
  queue_revision: number; event_sequence: number; engine_epoch: number;
  execution_state: RepositoryExecutionState; block_reasons: BlockReason[];
  active_configuration_digest: string; active_window: number; active_window_floor: number;
  active_window_ceiling: number; remote_enabled: boolean;
}
export interface SourceMetadata {
  subject: string; message_hash: string; author_name: string; author_email: string;
  branch?: string; worktree_path?: string; signature_state: string; approved_at: string; purpose?: string;
}
export interface QueueItem {
  id: string; repository_id: string; kind: QueueItemKind; enqueue_sequence: number; source_oid: GitOid; source_ref: string;
  metadata: SourceMetadata; state: QueueItemState; terminal_reason?: string; remote_state: RemoteState;
  cleanup_state: CleanupState; dependencies: string[]; promotion_authorized: boolean;
  promotion_authorized_at?: string; promotion_authorized_by?: string; current_generation_id?: string;
  buildset_id?: string; certificate_id?: string;
}
export interface ValidationGeneration {
  id: string; item_id: string; anchored_base_oid: GitOid; ordered_item_ids: string[];
  ordered_source_oids: GitOid[]; prefix_oids: GitOid[]; expected_parent_oid: GitOid;
  tested_oid: GitOid; configuration_digest: string; step_graph_digest: string;
  engine_epoch: number; identity_digest: string; invalidated_by?: string;
}
export interface Buildset {
  id: string; item_id: string; validation_generation_id: string; tested_oid: GitOid;
  expected_parent_oid: GitOid; environment_fingerprint: string; slot_id?: string;
  state: BuildsetState; retry_of?: string; attempt: number; created_at: string;
  started_at?: string; finished_at?: string; frozen_steps?: FrozenStep[]; step_results: BuildsetStepResult[];
}
export interface FrozenStep { id: string; name: string; command: { kind: "shell"; runner: string[]; script: string } | { kind: "argv"; argv: string[] }; working_directory: string; needs: string[]; soft_needs: string[]; voting: boolean; final_step: boolean; timeout_ns: number; cpu_tokens: number; memory_bytes: number; rss_limit_bytes?: number; semaphores: string[] }
export interface BuildsetStepResult { name: string; result_class: string; exit_code?: number; signal?: number; elapsed_ms: number; log_hash: string; stdout_end: number; stderr_end: number }
export interface SuccessfulStepResult { step_id: string; attempt_id: string; log_stdout_end: number; log_stderr_end: number; log_hash: string }
export interface PassCertificate {
  id: string; buildset_id: string; queue_item_id: string; validation_generation_id: string;
  tested_oid: GitOid; tree_oid: GitOid; expected_parent_oid: GitOid; configuration_digest: string;
  step_graph_digest: string; engine_epoch: number; environment_fingerprint: string;
  voting_results: SuccessfulStepResult[]; warnings: string[]; checkout_verified: boolean;
  completed_event_sequence: number; created_at: string;
}
export interface EffectiveStep {
  name: string; command: { kind: "shell"; script: string } | { kind: "argv"; argv: string[] };
  working_directory: string; needs: string[]; soft_needs: string[]; voting: boolean;
  final_step: boolean; timeout_ns: number; cpu_tokens: number; memory_bytes: number;
  rss_limit_bytes?: number; semaphores: string[]; include: string[]; exclude: string[];
  environment: Record<string, string>; remove_environment: string[]; artifacts: unknown[];
}
export interface DomainEvent { id: string; repository_id: string; sequence: number; actor: string; command_id?: string; kind: string; payload: unknown; created_at: string }
export interface QueueItemView { item: QueueItem; generation?: ValidationGeneration; buildset?: Buildset; attempts?: Buildset[]; attempt_generations?: ValidationGeneration[]; certificate?: PassCertificate; certificates?: PassCertificate[]; included_items: string[]; elapsed_ms?: number }
export interface HistoryItemsPage { items: QueueItemView[]; total: number; offset: number }
export interface ConfigurationView { digest: string; step_graph_digest: string; steps: EffectiveStep[]; remote_enabled: boolean; runner: string[] }
export interface VolumeView { id: string; roles: string[]; available_bytes: number; warning_threshold: number; critical_threshold: number; emergency_allowance: number; state: "healthy" | "warning" | "critical" }
export interface ResourceView { max_buildsets: number; repository_concurrency: number; cpu_tokens: number; memory_bytes: number; active_runs: number; queued_runs: number; cpu_reserved: number; memory_reserved: number; named_semaphores: Record<string, number>; authoritative_volume_available: number; recovery_reserve: number; volumes: VolumeView[] }
export interface SlotView { id: string; path: string; state: string; checkout_oid?: GitOid; health: string; last_used?: string }
export interface SeedView { id: string; path: string; profile: string; generation: number; logical_size: number; state: string }
export interface ArtifactRecord { artifact_id: string; buildset_id: string; source_path: string; retained_path: string; hash: string; size: number; retention_state: "retained" | "pinned"; created_at: string; expires_at: string }
export interface DiagnosticCheck { name: string; status: "healthy" | "attention"; detail: string; recovery_action?: string }
export interface DoctorReport { repository_id: string; generated_at: string; checks: DiagnosticCheck[]; healthy: boolean }
export interface RepositorySnapshot { state: RepositoryState; observed_master_oid: GitOid; queue: QueueItemView[]; checks: QueueItemView[]; history_items: QueueItemView[]; history: DomainEvent[]; configuration: ConfigurationView; resources: ResourceView; slots: SlotView[]; seeds: SeedView[]; artifacts: ArtifactRecord[] }
export interface EnvironmentView { snapshot_id: string; fingerprint: string; path: string; variable_count: number }
export interface UnavailableRepository { id: string; name: string; path: string; error: string; recovery_action: string }
export interface AppSnapshot { version: string; generated_at: string; repositories: RepositorySnapshot[]; unavailable_repositories: UnavailableRepository[]; environment: EnvironmentView }

export function oidHex(oid?: GitOid) { return oid?.bytes ?? ""; }
