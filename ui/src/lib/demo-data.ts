import type { AppSnapshot, GitOid, QueueItemState, QueueItemView } from "./types";

const oid = (value: string): GitOid => ({ format: "sha1", bytes: value.padEnd(40, value.slice(-1)) });
const now = new Date();
const ago = (minutes: number) => new Date(now.getTime() - minutes * 60_000).toISOString();

function item(index: number, state: QueueItemState, subject: string, branch: string, elapsed: number): QueueItemView {
  const id = `019fef58-7147-7${index}73-9c03-e6aff447a5b${index}`;
  const source = oid(["7a41be2", "e09c147", "4d62aa1", "cb38f70"][index - 1] ?? "114acff");
  const tested = oid(["91d3f0e", "f27c1b4", "60be92c", "aa16d92"][index - 1] ?? "abcedef");
  const parent = index === 1 ? oid("b2cdf95") : oid(["91d3f0e", "f27c1b4", "60be92c"][index - 2] ?? "b2cdf95");
  return {
    item: {
      id, repository_id: "019fef58-aaaa-7000-8000-e6aff447a5ba", kind: "gate", enqueue_sequence: index,
      source_oid: source, source_ref: `refs/tollgate/sources/${id}`,
      metadata: { subject, message_hash: "e15bc42", author_name: index === 3 ? "Mira Chen" : "You", author_email: "dev@example.com", branch, worktree_path: `/Users/dev/worktrees/${branch}`, signature_state: "verified", approved_at: ago(32 - index * 3) },
      promotion_authorized: true,
      state, remote_state: state === "ready" ? "ready" as const : "disabled" as const,
      cleanup_state: "not-eligible" as const, dependencies: index === 3 ? ["019fef58-7147-7273-9c03-e6aff447a5b2"] : [],
      current_generation_id: `019fef58-9000-7${index}00-8000-e6aff447a5ba`,
      buildset_id: `019fef58-8000-7${index}00-8000-e6aff447a5ba`,
      certificate_id: state === "ready" ? `019fef58-6000-7${index}00-8000-e6aff447a5ba` : undefined,
    },
    generation: {
      id: `019fef58-9000-7${index}00-8000-e6aff447a5ba`, item_id: id,
      anchored_base_oid: oid("b2cdf95"), ordered_item_ids: Array.from({ length: index }, (_, n) => `item-${n + 1}`),
      ordered_source_oids: [source], prefix_oids: [tested], expected_parent_oid: parent, tested_oid: tested,
      configuration_digest: "0aa71db513e868d166f3640ef91bf93c", step_graph_digest: "c1d06b0345e664abafcb13713532aac8",
      engine_epoch: 1, identity_digest: `e6d3ac0c${index}`,
    },
    buildset: {
      id: `019fef58-8000-7${index}00-8000-e6aff447a5ba`, item_id: id,
      validation_generation_id: `019fef58-9000-7${index}00-8000-e6aff447a5ba`, tested_oid: tested,
      expected_parent_oid: parent, environment_fingerprint: "44acbc2fa5", slot_id: `019fef58-5000-7${index}00-8000-e6aff447a5ba`,
      state: state === "ready" ? "passed" as const : state === "running" ? "running" as const : "pending" as const,
      attempt: 1, created_at: ago(27 - index * 2), started_at: state !== "queued" ? ago(18 - index * 2) : undefined,
      finished_at: state === "ready" ? ago(2) : undefined,
      step_results: state === "queued" ? [] : [{ name: "test", result_class: state === "ready" ? "success" : "running", elapsed_ms: elapsed, log_hash: "9a1f9d2e", stdout_end: 1842, stderr_end: 0 }],
    },
    certificate: state === "ready" ? {
      id: `019fef58-6000-7${index}00-8000-e6aff447a5ba`, buildset_id: `019fef58-8000-7${index}00-8000-e6aff447a5ba`,
      queue_item_id: id, validation_generation_id: `019fef58-9000-7${index}00-8000-e6aff447a5ba`, tested_oid: tested,
      tree_oid: oid("c482f11"), expected_parent_oid: parent, configuration_digest: "0aa71db513e868d166f3640ef91bf93c",
      step_graph_digest: "c1d06b0345e664abafcb13713532aac8", engine_epoch: 1, environment_fingerprint: "44acbc2fa5",
      voting_results: [], warnings: [], checkout_verified: true, completed_event_sequence: 93, created_at: ago(2),
    } : undefined,
    included_items: Array.from({ length: index }, (_, n) => `#${n + 1}`), elapsed_ms: elapsed,
  };
}

export const demoSnapshot: AppSnapshot = {
  version: "0.1.0",
  generated_at: now.toISOString(),
  unavailable_repositories: [],
  environment: { snapshot_id: "env-019fef58", fingerprint: "44acbc2fa591b40e", path: "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin", variable_count: 42 },
  repositories: [
    {
      state: {
        id: "019fef58-aaaa-7000-8000-e6aff447a5ba", name: "tollgate", path: "/Users/dev/tollgate",
        integration_ref: "refs/heads/master", master_oid: oid("b2cdf95"), queue_revision: 17, event_sequence: 94,
        engine_epoch: 1, execution_state: "active", block_reasons: [], active_configuration_digest: "0aa71db513e868d166f3640ef91bf93c",
        active_window: 20, active_window_floor: 3, active_window_ceiling: 20, remote_enabled: false,
      },
      observed_master_oid: oid("b2cdf95"),
      queue: [
        item(1, "ready", "Make promotion intents crash-safe", "feature/promotion-intents", 676_000),
        item(2, "running", "Stream logs with resumable offsets", "feature/log-broker", 428_000),
        item(3, "running", "Add APFS seed manifest validation", "feature/apfs-seeds", 196_000),
        item(4, "queued", "Polish queue dependency visualization", "feature/queue-ui", 0),
      ],
      checks: [],
      history_items: [],
      history: [
        { id: "event-94", repository_id: "019fef58-aaaa-7000-8000-e6aff447a5ba", sequence: 94, actor: "app", kind: "buildset.step-started", payload: {}, created_at: ago(1) },
        { id: "event-93", repository_id: "019fef58-aaaa-7000-8000-e6aff447a5ba", sequence: 93, actor: "app", kind: "certificate.issued", payload: {}, created_at: ago(2) },
        { id: "event-92", repository_id: "019fef58-aaaa-7000-8000-e6aff447a5ba", sequence: 92, actor: "app", kind: "buildset.passed", payload: {}, created_at: ago(2) },
        { id: "event-90", repository_id: "019fef58-aaaa-7000-8000-e6aff447a5ba", sequence: 90, actor: "ui", kind: "queue.item-enqueued", payload: {}, created_at: ago(9) },
        { id: "event-81", repository_id: "019fef58-aaaa-7000-8000-e6aff447a5ba", sequence: 81, actor: "app", kind: "promotion.completed", payload: {}, created_at: ago(85) },
      ],
      configuration: {
        digest: "0aa71db513e868d166f3640ef91bf93c", step_graph_digest: "c1d06b0345e664abafcb13713532aac8", remote_enabled: false,
        runner: ["/bin/zsh", "-c"],
        steps: [
          { name: "format", command: { kind: "shell", script: "cargo fmt --check" }, working_directory: ".", needs: [], soft_needs: [], voting: true, final_step: false, timeout_ns: 3_600_000_000_000, cpu_tokens: 1, memory_bytes: 268435456, semaphores: [], include: ["**/*.rs"], exclude: [], environment: {}, remove_environment: [], artifacts: [] },
          { name: "test", command: { kind: "shell", script: "cargo test --workspace" }, working_directory: ".", needs: ["format"], soft_needs: [], voting: true, final_step: false, timeout_ns: 3_600_000_000_000, cpu_tokens: 4, memory_bytes: 6442450944, semaphores: [], include: [], exclude: [], environment: {}, remove_environment: [], artifacts: [] },
          { name: "clippy", command: { kind: "shell", script: "cargo clippy --all-targets" }, working_directory: ".", needs: ["format"], soft_needs: [], voting: true, final_step: false, timeout_ns: 3_600_000_000_000, cpu_tokens: 2, memory_bytes: 4294967296, semaphores: [], include: [], exclude: [], environment: {}, remove_environment: [], artifacts: [] },
        ],
      },
      resources: { max_buildsets: 8, repository_concurrency: 4, cpu_tokens: 12, memory_bytes: 25769803776, active_runs: 2, queued_runs: 1, cpu_reserved: 7, memory_reserved: 10737418240, named_semaphores: { unity: 1 }, authoritative_volume_available: 128849018880, recovery_reserve: 10737418240, volumes: [{ id: "fs-2a", roles: ["artifacts", "authoritative", "database", "logs"], available_bytes: 128849018880, warning_threshold: 16106127360, critical_threshold: 10737418240, emergency_allowance: 536870912, state: "healthy" }] },
      slots: [
        { id: "slot-01", path: "/Library/Caches/Tollgate/slots/01", state: "idle", checkout_oid: oid("91d3f0e"), health: "healthy", last_used: ago(2) },
        { id: "slot-02", path: "/Library/Caches/Tollgate/slots/02", state: "running", checkout_oid: oid("f27c1b4"), health: "healthy", last_used: ago(1) },
        { id: "slot-03", path: "/Library/Caches/Tollgate/slots/03", state: "running", checkout_oid: oid("60be92c"), health: "healthy", last_used: ago(1) },
        { id: "slot-04", path: "/Library/Caches/Tollgate/slots/04", state: "idle", checkout_oid: oid("244ca16"), health: "healthy", last_used: ago(68) },
      ],
      seeds: [{ id: "seed-01", path: "/Library/Caches/Tollgate/seeds/default/1", profile: "default", generation: 1, logical_size: 1879048192, state: "published" }], artifacts: [],
    },
    {
      state: {
        id: "019fef58-bbbb-7000-8000-e6aff447a5ba", name: "atlas-web", path: "/Users/dev/atlas-web",
        integration_ref: "refs/heads/master", master_oid: oid("3af01be"), queue_revision: 5, event_sequence: 22,
        engine_epoch: 1, execution_state: "blocked", block_reasons: [{ code: "push-diverged", message: "origin/master moved unexpectedly", recovery_action: "Review local and remote tips, then reconcile." }], active_configuration_digest: "acf214",
        active_window: 10, active_window_floor: 3, active_window_ceiling: 20, remote_enabled: true,
      },
      observed_master_oid: oid("61fa3c2"),
      queue: [], checks: [], history_items: [], history: [],
      configuration: { digest: "acf214", step_graph_digest: "bb19fc", steps: [], remote_enabled: true, runner: ["/bin/sh", "-c"] },
      resources: { max_buildsets: 4, repository_concurrency: 2, cpu_tokens: 8, memory_bytes: 17179869184, active_runs: 0, queued_runs: 0, cpu_reserved: 0, memory_reserved: 0, named_semaphores: {}, authoritative_volume_available: 68719476736, recovery_reserve: 10737418240, volumes: [{ id: "fs-2a", roles: ["artifacts", "authoritative", "database", "execution-cache", "logs"], available_bytes: 68719476736, warning_threshold: 16106127360, critical_threshold: 10737418240, emergency_allowance: 536870912, state: "healthy" }] }, slots: [], seeds: [], artifacts: [],
    },
  ],
};
