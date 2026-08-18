# Tollgate Technical Design

Status: proposed v1 design | Date: 2026-08-10 | Product: Tollgate desktop application and `tg` CLI

## 1. Summary

Tollgate is a 100% local continuous-integration and Git promotion system for macOS. A Tauri v2 desktop application is the command center and the sole live authority for scheduling, execution, queue mutation, and promotion. The `tg` CLI is a fast and scriptable client of the same Rust command service. There is no daemon. Closing the last window leaves the ordinary Dock application running; explicitly quitting it stops active CI and durable queue processing resumes when the app is next opened.

Tollgate's defining behavior is a single-repository, Zuul-style dependent gate through local `release` to remote `master`. Approving clean, single-commit changes A, B, and C creates an ordered queue and concurrently validates the exact prospective prefix commits `release+A`, `release+A+B`, and `release+A+B+C`, subject to local resource capacity. A passing head is promoted with an old-OID compare-and-swap. Already-passing descendants advance without rerunning only when their exact tested parent has just been promoted and every other validity input is unchanged. If an earlier independent change fails, conflicts, is canceled, or becomes stale, it leaves the active queue and every affected descendant is rebuilt and retested without it. True Git dependencies leave the queue together when their prerequisite fails.

The non-negotiable invariant is:

> Every commit that Tollgate writes to local `refs/heads/release` or pushes to the configured remote branch is the exact Git object ID for which every applicable voting validation completed successfully under the frozen configuration for that item's validation generation.

Tollgate maximizes incremental build reuse with persistent detached Git worktree slots, preserved ignored files, APFS clone-on-write seed snapshots, slot affinity, and optional use of already-installed shared caches such as `sccache`. The execution engine remains language-agnostic: a validation step is primarily a name and a shell command. Data-driven initialization templates may propose commands for Rust, Node, Python, Unity, or mixed repositories, but the scheduler has no language-specific test logic.

## 2. Goals

### 2.1 Product goals

- Replace a personal Buildbot-style CI server with a local desktop application.
- Prevent unvalidated commits from being promoted by Tollgate to local `release` or remote `master`.
- Apply Zuul's dependent-gate semantics to local branches and worktrees.
- Keep the common path small: create a worktree, make one commit, run `tg approve`, and continue working.
- Make queue state, running capacity, steps, logs, timing, failures, promotion, push state, and history legible in the desktop UI.
- Make the same operational capabilities available through `tg` for scripts and fast interaction.
- Survive application crashes, explicit quits, restarts, external Git movement, and partial promotion/push operations without accepting a stale result.
- Reuse multi-gigabyte incremental output such as Rust `target/` and Unity `Library/` automatically.
- Work with arbitrary commands and mixed-language repositories without embedding build-system policy in the scheduler.
- Manage multiple explicitly registered repositories while giving each repository an independent gate.

### 2.2 v1 platform and scale goals

- macOS Tahoe 26 or newer.
- Apple Silicon only.
- One logged-in macOS user owns a registered repository at a time.
- Up to 50 registered repositories, 10 active repositories, 100 queued items per repository, 8 concurrent runs, and 100 steps per run.
- Sustained aggregate log ingestion of 10 MiB/s with bounded higher bursts.
- At least one year of audit metadata without noticeable UI degradation.
- Up to 500 GiB of logical cache trees, constrained by a configurable physical-space policy.

## 3. Non-goals for v1

- A daemon, launch agent, headless CI server, or execution that continues after the app quits.
- Linux, Windows, or pre-Tahoe macOS support.
- Multiple integration branches in one repository; v1 owns only local `release` and targets one configured remote branch.
- Cross-repository speculative queues, cross-repository atomic promotion, or dependency cycles.
- Multiple macOS users concurrently operating the same repository.
- Containers, virtual machines, or a general security sandbox.
- Built-in secret storage, vaulting, or credential management.
- Automatic installation of third-party tools or package managers.
- Language-specific result parsing, including JUnit, Cargo, pytest, or Unity test-case views.
- Early failure detection by matching streamed output.
- A general replacement for Git. `tg` wraps only operations where gate-aware behavior provides a concrete safety guarantee.
- Telemetry, analytics, remote log upload, or automatic crash reporting.

## 4. Terms

| Term | Meaning |
| --- | --- |
| Authoritative repository | The user's real Git common directory and refs. Tollgate promotes only local `release`; after certification it may safely fast-forward user-owned local `master` under the configured post-promotion policy. |
| Execution mirror | A disposable bare Git repository under Tollgate's cache root. Synthetic commits and CI worktrees live here, isolating CI Git operations from authoritative refs. |
| Source commit | The immutable, single-parent commit OID captured by `tg approve`. |
| Queue item | One approval of one source commit, plus ordering, hard dependencies, attempts, and history. |
| Queue revision | A monotonically increasing version of mutable queue state. Every enqueue, dequeue, reorder, promotion, or administrative queue mutation advances it. It is used for command conflict detection and UI reconciliation, not validation identity. |
| Validation generation | The immutable validation identity assigned to one queue item: its anchored base, ordered prefix through that item, synthetic OIDs, dependency inputs, effective configuration, step graph, and engine epoch. Changes after the item do not affect it. |
| Synthetic commit | A prospective linear commit created by applying one source commit's patch to the preceding prefix. It preserves source author, committer, timestamps, and message, and contains no Tollgate metadata. |
| Buildset | Validation of one exact synthetic commit under one frozen validation generation and step graph. |
| Step attempt | One execution of a configured command within a buildset. |
| Slot | A persistent detached worktree in the execution mirror, including its retained ignored files. One slot owns an entire buildset at a time. |
| Seed | An immutable published APFS clone generation of eligible ignored artifacts captured at a safe successful boundary. It preserves original cache metadata for later writable slot clones and is never modified in place. |
| Pass certificate | Durable evidence tying one tested OID and validation generation to all required successful step results. |
| Structural staleness | Invalidity caused by a changed OID, parent, queue prefix/order, configuration, applicable voting-step graph, or promotion base. Age alone is not staleness. |

## 5. Normative invariants

The implementation must make the following invariants explicit in domain types, database constraints where possible, state-transition guards, and tests.

### I1. Exact promotion

For every completed Tollgate promotion event, all of the following are true:

- `release`'s new OID equals the pass certificate's tested OID.
- The new commit's first and only parent equals `release`'s OID immediately before promotion.
- The certificate belongs to the current queue head and matches that item's currently assigned validation generation.
- Every applicable voting step in the frozen step graph has a successful terminal result.
- The tracked checkout and initialized submodules matched the tested commit after validation.
- The frozen configuration and step-graph digests still match the item's validation generation.
- The ref update compared the actual old `release` OID with the expected OID and succeeded atomically.

There is no merge, cherry-pick, amend, signing, message edit, or commit creation after validation and before promotion.

### I2. Immutable approval

Approval captures a full source OID, creates a hidden retention ref, and never follows later branch movement. A branch amendment is a new approval. Queue/history records never use a branch name as commit identity.

### I3. Exact validation reuse

A result is usable only while its validation generation remains the one currently assigned to its queue item. Removing, inserting, reordering, or replacing an item invalidates exactly the items whose prefix inputs changed. Appending an item later in the queue does not invalidate earlier prefixes. Promoting a passing head does not invalidate a descendant whose tested parent is the exact promoted OID and whose other validation inputs are unchanged. A result cannot be transferred between an independent check and a gate buildset merely because commands happen to be similar.

### I4. Frozen execution inputs

Before a buildset starts, Tollgate freezes the tested OID, expected parent, step graph, command strings, configuration digest, resource declarations, shell runner, and environment snapshot. Later changes never mutate those inputs in place. An applied configuration change may invalidate and terminate the buildset under Section 11.5; an environment reload affects only future buildsets and retries under Section 12.1.

### I5. Single live authority

Exactly one Tollgate app process and one repository supervisor may mutate a repository's live state. The CLI never schedules, promotes, or writes queue state independently. Repository and app locks fail closed after checking ownership.

### I6. Authoritative-ref isolation

CI commands execute only in worktrees attached to the disposable execution mirror. They do not share the authoritative repository's common Git directory. The mirror is a correctness boundary against accidental ref and maintenance operations, not a security boundary against malicious code running as the same user.

### I7. No unobserved success

An active command whose supervisor relationship is lost can never become successful. App quit/crash marks the active buildset interrupted, terminates its process group, and reruns the whole buildset from the beginning after restart. Step-level success already durably recorded in a fully completed buildset remains valid.

### I8. Explicit remote divergence

Remote push never overwrites an unexpected remote OID. A push uses an exact lease. Failed or divergent push state is durable and blocks later promotion until resolved.

### I9. Safe ownership boundaries

Cleanup, reset, pruning, and cache operations resolve and verify Tollgate-owned paths before mutation. They never delete a primary developer worktree, authoritative Git data, audit metadata, or a branch whose OID has moved since approval.

## 6. User and application model

### 6.1 Application lifecycle

The Tauri application is a normal Dock application and the only command center. Closing its last window does not terminate the app. Explicit Quit while work is active requires confirmation, terminates all active process groups, records interruption, checkpoints state, and exits. A crash is handled equivalently on recovery, using ephemeral worker supervisors to terminate orphan-prone child process trees.

The app does not launch at login by default. An explicit preference may register the ordinary app as a login item. Invoking a live or state-changing `tg` command while the app is absent launches the app through Launch Services without forcing a window to the foreground, waits for the IPC endpoint, and submits the request. `--no-launch` makes unavailability an immediate error.

While any validation runs, the app holds a macOS idle-system-sleep assertion but permits display sleep, explicit sleep, lid-close sleep, and forced thermal/battery sleep. Suspended intervals are recorded and excluded from runnable timeout accounting.

### 6.2 Repository model

Registration is always explicit through `tg init`, `tg repo add`, or Open Repository in the app. Tollgate never scans development directories. Worktrees are deduplicated by their Git common directory and a repository UUID stored in Tollgate's repository-local state.

Each registered repository has:

- one Tollgate-owned local integration branch named `release`, never checked out in a developer worktree;
- one user-owned local branch named `master`, normally checked out in the primary worktree and tracking remote `master`;
- one ordered dependent gate;
- zero or more independent check runs;
- its own queue revision, validation generations, history, configuration, execution mirror, slots, and seeds;
- optional remote/push configuration.

The app may supervise several repositories concurrently using one global resource pool. Cross-repository queue semantics do not exist in v1.

Initialization leaves the developer's checkout unchanged and creates local `release` at the exact local `master` OID. Tollgate exclusively owns `release`, which must not be checked out in any worktree while the repository is active. Local `master` remains user-owned, may track `origin/master`, and may be committed to or pushed through ordinary Git. After a certified local promotion, or after its exact remote push when pushing is enabled, Tollgate fast-forwards local `master` by default. For active items captured directly from `master`, Tollgate retains their current speculative tested object in the authoritative repository and may project the last such item onto that exact object whenever certified history advances. The expected-old transaction accepts only the unchanged source tip or one of that item's prior generated tips, so newly certified commits appear beneath the submitted work without overwriting later user commits. A checked-out `master` is updated only when its index and worktree are clean; a non-checked-out `master` uses an expected-old ref transaction. Git reports linked worktree roots nested beneath the primary checkout as untracked directories, so the synchronization safety check excludes only exact nested paths that are still registered, directly discoverable worktrees of the same repository. Every other staged, modified, or non-ignored untracked path blocks synchronization and is included in the needs-attention event. Dirty, missing, or otherwise divergent state produces a non-blocking needs-attention result. `sync_user_master = false` opts out. Direct pushes are explicitly uncertified external movement; Tollgate's remote lease detects them and requires adoption into `release` before certified promotion continues.

### 6.3 Normal workflow

1. The user creates or opens a feature worktree based on the gated `release` tip or an appropriate queued dependency. The primary checkout may remain on user-owned `master`.
2. The worktree contains one source commit. Ignored build output is allowed; staged changes, tracked modifications, and non-ignored untracked files are not.
3. `tg candidate` captures `HEAD`, validates its shape and dependencies, creates `refs/tollgate/sources/<item-id>`, durably enqueues a non-promotable item, and returns its ID. `--retain-worktree` captures an immutable retained cleanup policy for workflows that continue using the same source worktree after promotion. `tg approve HEAD` remains a combined submit-and-authorize convenience and accepts the same policy.
4. The app constructs synthetic prefixes, assigns new validation generations to affected items, and schedules eligible buildsets.
5. `tg approve <candidate-id>` durably grants promotion authority to that exact retained source and every active hard dependency in its retained ancestry as one queue-revision transaction. The authorized closure may temporarily bypass unrelated unauthorized items, while hard dependencies remain ahead of their dependents. Tollgate retains admission order independently from this temporary execution order. When distributed approvals make the authorized set a contiguous admission prefix before any conflicting promotion, Tollgate restores that order, cancels redundant bypass work, and reactivates only exact completed generations whose complete validation identity and certificate still match. A passing authorized head is promoted automatically, so an unrelated candidate awaiting human authority cannot indefinitely block approved work.
6. After local promotion, or after push succeeds when push is enabled, Tollgate synchronizes user-owned local `master` under the safe default-on policy, then automatically cleans up the source worktree and branch if all safety checks still pass.

Automatic cleanup requires a non-primary linked worktree that is still at the captured source OID and has no tracked, staged, or non-ignored untracked changes. Branch deletion is an old-OID compare-and-swap. Ignored files in an eligible linked worktree are disposable by default. A retained-worktree candidate finishes promotion with cleanup `not-eligible`, preserving its recorded worktree and branch. Candidate authorization and retry preserve the captured cleanup policy. If any automatic cleanup check fails, cleanup becomes `needs-attention`; promotion is never rolled back. The hidden source ref retains the commit for audit.

## 7. System architecture

### 7.1 Process topology

Tollgate has three kinds of process:

1. **Tauri application.** One long-lived process contains the Rust application service, repository supervisors, global scheduler, Git adapter, SQLite stores, log broker, process supervisor, and Tauri bridge. It remains alive with zero windows.
2. **`tg` CLI.** A short-lived Rust client resolves the repository, launches the app if allowed, performs a versioned IPC handshake, sends a command, and optionally subscribes to events. It never becomes an executor.
3. **Ephemeral step supervisor.** One small helper per running command establishes a process group, applies priority and resource settings, monitors the app parent, launches the configured shell command, and reports exit status. It exists only for the command's lifetime and is not a daemon.

The React/TypeScript frontend is a projection of the Rust service. It sends typed commands through Tauri invokes and receives snapshots and ordered channels. The frontend is never authoritative: optimistic UI may improve responsiveness, but it must reconcile to service-issued sequence numbers and command results.

### 7.2 Rust workspace boundaries

The implementation should use a Cargo workspace with narrow dependency direction:

| Crate/component | Responsibility |
| --- | --- |
| `tollgate-domain` | IDs, immutable commands/events, queue state machine, validity rules, scheduler inputs, error taxonomy. No Tauri, SQLite, or process dependencies. |
| `tollgate-git` | Typed system-Git adapter, repository discovery, mirror synchronization, synthetic commit construction, worktrees, ref transactions, fetch/push leases. |
| `tollgate-store` | SQLite schema, migrations, transactional repositories, event journal, intents/outbox, backups, retention metadata. |
| `tollgate-runner` | Slots, environment bootstrap, process supervision, logs, timeouts, tracked-clean checks, artifacts, APFS seed management. |
| `tollgate-scheduler` | Per-repository queue supervisors and global resource/fairness scheduler. |
| `tollgate-service` | Typed command handlers shared by Tauri and IPC, authorization to repository scope, snapshots, subscriptions. |
| `tollgate-ipc` | Unix-domain-socket framing, protocol negotiation, peer checks, request/response and event stream types. |
| `tg` | CLI parsing, app launch, rendering, JSON schema, wait/stream behavior. |
| `tollgate-worker` | Minimal ephemeral command supervisor. |
| `src-tauri` | macOS lifecycle, Tauri commands/channels, notifications, window restoration, power assertions, app packaging. |
| `ui` | React/TypeScript views, navigation, virtualization, ANSI log rendering, generated Rust API types. |

Infrastructure crates implement domain traits; the domain must not call a shell, database, wall clock, random generator, or filesystem directly. All state-machine tests use deterministic fake ports.

### 7.3 Concurrency model

The app owns one actor-like repository supervisor per active repository and one global scheduler. All mutating repository commands are serialized through that supervisor. Long-running Git and process operations use task handles, but completion is delivered back as a versioned domain event; workers never mutate queue rows directly.

Each repository command includes the caller's last observed repository revision when it acts on ordering or destructive state. Commands that would apply to a changed queue return a conflict with a fresh preview. SQLite permits many snapshot readers, but the app service is the only writer. The CLI does not hold long read transactions.

### 7.4 Local IPC

The app exposes one `SOCK_STREAM` Unix-domain socket in the per-user application-support directory. The socket and parent directory are mode `0600`/`0700`, and both server and client verify the peer effective UID with macOS peer credentials before exchanging application frames. There is no bearer token and no TCP listener.

The v1 wire protocol uses a fixed magic, protocol version, frame kind, flags, correlation ID, and unsigned 32-bit big-endian payload length. Control JSON frames are limited to 8 MiB; binary log frames are limited to 1 MiB; zero length is permitted only for defined control kinds. A length over the negotiated maximum, unknown mandatory frame kind, malformed UTF-8/JSON, duplicate live correlation ID, or payload/declared-length mismatch closes the connection and emits no command. State-changing requests carry client-instance and command UUIDs and receive exactly one stored idempotent response.

Request/response frames coexist with resumable event streams. A subscription supplies repository event sequence and per-log-stream offsets. The server returns contiguous durable frames after those positions or an explicit `gap/pruned` response with the earliest available offset; it never silently jumps. High-volume log frames contain repository ID, run/attempt ID, stream, per-stream byte offset, broker observation sequence, and payload. Every handshake identifies CLI version, protocol range, app version, schema version, maximum frame sizes, and supported frame kinds. The highest mutually supported protocol version is selected; no overlap fails before commands with a structured upgrade instruction.

The Tauri frontend calls the same `tollgate-service` handlers in-process. Tauri commands are used for bounded request/response operations; Tauri channels carry ordered logs and state changes. Generated TypeScript types and a checked-in protocol schema prevent Rust/UI drift.

## 8. Filesystem and durable state

### 8.1 Layout

Authoritative runtime repository state is colocated with the Git common directory. The personal policy is stored separately in the repository root so it can be managed by a local configuration overlay:

| Path | Contents |
| --- | --- |
| `<git-common-dir>/tollgate/state.sqlite3` | Queue, buildsets, steps, events, intents, history, slots/seeds metadata, retention metadata. |
| `<git-common-dir>/tollgate/logs/` | Append-only active logs and compressed completed logs. |
| `<git-common-dir>/tollgate/artifacts/` | Retained run artifacts governed by their own budget. |
| `<repository-root>/.tollgate/config.toml` | Required trusted local repository configuration and the sole live policy source. |
| `<git-common-dir>/tollgate/backups/` | Rolling online database backups and migration snapshots. |
| `refs/tollgate/sources/` | Approved source-object retention refs. |
| `refs/tollgate/tested/` | Exact tested objects copied back from the mirror while active/audited. |

Disposable execution data lives by default at `~/Library/Caches/Tollgate/<repository-id>/`:

| Path | Contents |
| --- | --- |
| `mirror.git/` | Bare execution mirror. |
| `builder/` | Dedicated synthetic-commit builder worktree. |
| `slots/<slot-id>/` | Persistent detached CI worktrees and ignored build output. |
| `seeds/<profile>/<generation>/` | Read-only APFS clone snapshots and manifests. |
| `quarantine/` | Reset/corrupt slot data awaiting deletion. |

Global app support contains only the explicit repository registry, UI/preferences state, IPC endpoint/lock, protocol metadata, and updater state. It does not duplicate queue authority. UI navigation state includes last repository, route, selection, filters, log follow/scroll state, sidebar state, and window geometry.

The cache root is configurable per repository. Before cloning, Tollgate verifies source and destination have the same device identity and that the volume advertises APFS clone capability. A non-APFS or cross-volume root remains usable with persistent slots, but new-slot seeding is cold unless the user explicitly authorizes a physical copy after seeing its estimated size. Clone-required operations never silently degrade to physical copies.

### 8.2 SQLite policy

SQLite stores normalized current state plus an append-only domain-event journal. This is not full event sourcing: current tables are authoritative for normal reads, while the journal supplies auditability, ordered UI changes, and recovery evidence.

Required database policy:

- Bundle SQLite 3.51.3 or newer, which contains the 2026 WAL-reset fix, rather than relying on the OS library. Each Tollgate release pins and reports one exact bundled SQLite source version; upgrading it runs the database fault/integrity suite.
- Use WAL mode, foreign keys, a busy timeout, and a single app writer connection/task.
- Use `synchronous=FULL` for queue mutation, result completion, promotion/push intents, and migration boundaries. Less critical UI preference writes may use normal durability in the global preference store.
- Keep read transactions short and checkpoint deliberately so long-lived UI/CLI readers cannot starve WAL truncation.
- Allocate monotonically increasing repository event sequence numbers in the same transaction as each state change.
- Run quick integrity checks at ordinary startup and a full integrity check after unclean shutdown or before migration.
- Create rolling backups through SQLite's online backup API before migrations and periodically after promotion. Never copy only the main database file while a WAL may contain committed state.

### 8.3 Logical data model

The v1 schema must represent each of these logical entities. Migrations may split an entity across tables or add indexes/materialized read models, but may not omit an entity or merge the independent state dimensions defined below into one overloaded status:

- `repository_state`: repository UUID, schema/engine epoch, integration ref, current observed OIDs, queue revision, pause/block state, and active configuration digest.
- `queue_items`: UUIDv7/short ID, source OID/ref/worktree snapshot, source metadata, enqueue order, queue-item state, terminal reason, immutable cleanup policy, and separate remote and cleanup states.
- `item_dependencies`: hard Git dependency edges.
- `source_promotions`: permanent exact mapping from queue item/source OID to promoted synthetic OID, old `release` OID, certificate, and promotion event.
- `validation_generations`: item, anchored base OID, ordered prefix/digest through the item, dependency inputs, prefix OIDs, configuration and step-graph digests, engine epoch, and invalidation lineage.
- `buildsets`: item/validation generation, exact tested OID and parent, environment snapshot fingerprint, slot, status, `retry_of_buildset_id`, and whole-buildset attempt number.
- `steps` and `step_attempts`: frozen command/resource data, timing, result class, exit/signal/timeout, log ranges, retry number.
- `pass_certificates`: tested OID, all validity inputs, successful voting result set, tracked-clean result, creation event.
- `configuration_snapshots`: schema version, canonical effective bytes, configuration and step-graph digests, activation event, and supersession lineage.
- `operation_intents`: typed approval, tested-object, result, promotion, push, cleanup, artifact, seed, pruning, backup, and migration intents with expected identities/old values, independent intent state, timestamps, attempts, and recovery evidence. Promotion and push may use specialized child tables for their OID fields.
- `slots`, `seed_generations`, and `cache_manifests`: ownership paths, compatibility keys, source OIDs, logical size, last use, health.
- `artifacts`: run/step, source path, retained path, hash, size, retention/pin state.
- `log_streams` and `log_chunks`: run/step/stream identity, per-stream offsets, broker sequences, sealed chunk hashes, compression/pruning state, and retained range metadata.
- `remote_observations`: remote identity, exact ref, observed OID/nonexistence, observation method/time, and owning push/pull intent.
- `volume_state` and `volume_reservations`: stable volume identity, path roles, warning/critical thresholds, emergency allowance, observed free space, and active operation allowances.
- `command_results`: client-instance/command UUID, command kind, request digest, terminal response, and event sequence for idempotent replay.
- `backup_records`: database identity/schema version, online-backup path/hash, verification result, and migration relationship.
- `events`: ordered immutable audit events with actor (`app`, `cli`, `ui`, or recovery), command ID, and redacted payload.

OID columns store the hash algorithm plus raw bytes, not an assumed 40-character SHA-1 string. V1 supports repositories whose active object format is SHA-1 or SHA-256 as defined in Section 9.1.

The schema enforces the following uniqueness and cardinality rules wherever SQLite can express them, with matching domain guards for rules involving terminal-state predicates:

- at most one active queue item for a `(repository_id, source_oid)` pair;
- exactly one currently assigned validation generation for each active queue item;
- at most one nonterminal buildset for a validation generation;
- at most one pass certificate for a successful buildset;
- at most one unfinished promotion intent and one unfinished push intent per repository;
- one durable result for each `(client_instance_id, command_id)`, so replaying a state-changing IPC request returns the original result;
- unique repository event sequence numbers and unique queue enqueue sequence numbers; and
- foreign keys from certificates, intents, attempts, logs, and artifacts to immutable owning records, with deletion prohibited while the record is active or audited.

Every operation spanning SQLite and Git or the filesystem uses the same explicit intent shape: `prepared`, `external-applied`, and `completed`, plus `canceled` or `needs-attention` terminal outcomes. The preparation transaction records all expected identities, old values, destination paths, and the command ID before the external action. The external action uses an old-value, nonexistence, exclusive-create, or owned-path assertion. The completion transaction records observed evidence and emits the domain event. Recovery never infers success from an intent alone: it compares the recorded expectations with Git refs, object IDs, path ownership markers, hashes, and manifests, then either completes the exact operation once, proves that no external change occurred and retries/cancels it, or blocks for attention. This protocol applies to approval refs, tested-object retention, result completion, promotion, push, worktree/branch cleanup, artifact retention, seed publication, pruning, and migration.

### 8.4 Retention

- Queue, result, timing, promotion, push, configuration-digest, and audit metadata are retained indefinitely.
- Full logs default to 90 days and 10 GiB per repository. Active-queue logs are never pruned. Completed logs are compressed in the background. Pruned log ranges remain explicit metadata.
- Retained artifacts default to 30 days and 50 GiB per repository. Pinned runs/artifacts are exempt and reported separately.
- Execution caches use a global default budget equal to the smaller of 200 GiB or 25% of currently free space. Minimum-free-space reserves are configured and enforced per underlying volume identity, because repository state, logs/artifacts, and execution caches may reside on different volumes.
- Cache pressure prunes superseded seeds, excess cold idle slots, and older non-current seeds in that order. It never touches active slots, the last usable warm seed for an active repository, logs, artifacts, or audit state.

Logical size, estimated unique physical size, and volume free space must be labeled separately. APFS shared-extent accounting is not exact enough to present an estimate as a guaranteed byte count.

## 9. Git and speculative gating

### 9.1 System Git as the source of behavior

Tollgate uses the installed Git CLI rather than embedding `libgit2`. Initialization runs a version/feature probe and records the resolved executable. All parsing uses documented plumbing or stable NUL-delimited porcelain. User-facing/localized output is never parsed. Git commands receive explicit repository paths, controlled configuration overrides where behavior must be invariant, and bounded stdin/stdout/stderr handling.

Using system Git preserves native behavior for worktrees, credential helpers, local hooks on user-initiated pushes, submodules, partial clones, object formats, and transport protocols. Tollgate still wraps operations in typed results and classifies lock contention, conflicts, missing objects, authentication errors, and ref mismatches separately.

V1 accepts repositories whose `git rev-parse --show-object-format` result is exactly `sha1` or `sha256` and rejects any other object format during registration. Every plumbing command uses full OIDs in the repository's reported format. A versioned Git-semantics profile records the resolved executable's canonical path, file identity, Git version, object format, environment overrides, configuration overrides, merge strategy, and every correctness-sensitive argv. The profile digest contributes to the engine compatibility epoch. Tollgate reruns checked-in golden transplant fixtures when the executable identity or version changes; it blocks the repository rather than constructing new generations if the fixtures differ until the new profile is explicitly accepted with an epoch bump.

All Tollgate-internal plumbing operations, including mirror fetches, synthetic construction, hidden-ref maintenance, and authoritative local ref transactions, set `core.hooksPath` to an empty Tollgate-owned directory. They never execute repository hooks. `tg push` remains a user-initiated network operation and may run the normal local `pre-push` hook; hook rejection is reported as a push failure and cannot change a pass certificate. Remote receive hooks remain authoritative at the server.

### 9.2 Approval contract

`tg approve [<rev>]` defaults to `HEAD` in the invoking worktree and succeeds only when:

- the authoritative repository is registered and owned by the app;
- the integration branch exists and is not checked out;
- the selected worktree has no staged changes, tracked modifications, or non-ignored untracked files;
- the resolved source is one commit object with exactly one parent;
- the source is not already an ancestor of `release`;
- the source OID is not already active in the queue;
- every unmerged source ancestor is either literal `release` history, an active known source item, a prefix OID from a current speculative generation, or a previously promoted source item whose promoted OID is still in `release` history;
- effective configuration is valid enough to construct a gate buildset.

Root and merge commits are rejected in v1. Ignored files do not make a developer worktree dirty. Approval captures source subject, full message hash, author/committer metadata, signature verification state, branch/ref OID if present, worktree path/identity, and approval time for display and cleanup; only the OID defines content.

The source retention ref and queue item are created as one recoverable operation:

1. A full-durability transaction allocates the item/enqueue IDs and inserts an approval intent containing repository, command ID, source OID, exact retention-ref name, expected ref nonexistence, source-worktree snapshot, and prospective queue mutation. No active queue item is visible yet.
2. The hooks-disabled Git profile creates the source retention ref with an explicit nonexistence assertion, constructs the prospective generation, and retains its tested OID under `refs/tollgate/speculative/<generation-id>` in the authoritative object database.
3. A second full-durability transaction compares the queue revision captured by preflight, then inserts the queue item/dependencies, advances the queue revision and event sequence, marks the intent complete, and stores the idempotent command result. A revision mismatch rolls back the whole database transaction, removes the just-created refs with exact-old-OID assertions, and returns current stale-queue context.

Recovery for a prepared approval intent has only three outcomes: a missing ref proves no Git change and permits retry/cancel; a ref equal to the recorded source OID permits exactly-once database completion; any other ref blocks as ownership ambiguity. Removal is allowed only for a canceled prepared intent whose ref still equals the recorded source OID, using an old-OID delete assertion. A queue item is never created for a mismatching ref, and a ref is never removed merely because its name has the Tollgate prefix.

Re-approving a changed OID from the same source branch follows Zuul's new-patchset behavior: atomically mark the old item `superseded`, dequeue it, invalidate affected descendants, and append the new item at the tail. It does not recover the previous position automatically.

### 9.3 Hard Git dependencies

Queue order alone creates a speculative relationship, not a hard dependency. A hard dependency exists when a source commit is actually based on another unmerged source commit.

For source B, Tollgate walks from B's parent through first-parent ancestry until it reaches a literal ancestor of current `release`. Every intervening commit is resolved by exact OID, never by branch name, patch ID, or content similarity:

- An OID matching an active queue item's source OID creates an active hard-dependency edge.
- An OID matching a prefix in a current validation generation creates hard-dependency edges to the active items represented through that point in the prefix. A source commit rebased onto the displayed tested prefix is therefore a supported candidate input.
- An OID matching a promoted item's source OID is a satisfied dependency only when that item's recorded promoted synthetic OID is a literal ancestor of current `release`.
- An OID matching only an invalidated, failed, canceled, superseded, or otherwise historical speculative generation returns a retryable `stale-queue-prefix` result containing the current release OID, queue revision, current prefix OID, and recovery procedure.
- An unknown OID returns an actionable source-ancestry rejection rather than silently approving additional code or escaping as an internal invariant failure.

Tollgate retains the durable mapping from each promoted source OID to its exact promoted synthetic OID indefinitely with audit metadata. This allows B, whose original parent is source A, to recognize that dependency after A has landed as synthetic commit `S_A`. Satisfied dependencies remain historical provenance but no longer constrain active queue order; active edges do.

Hard dependencies impose these rules:

- a dependent cannot be ordered before its prerequisite;
- queue promotion/reordering preserves the dependency DAG;
- if a prerequisite fails, conflicts, is canceled, or is superseded, active dependents leave the queue with `dependency-failed`;
- an independent item that merely follows a failure in queue order remains and is rebuilt without the failed patch.

### 9.4 Execution mirror synchronization

The execution mirror is initialized as a disposable bare repository and is never treated as authoritative. Before constructing affected validation generations, the Git adapter ensures that the mirror contains:

- the exact observed authoritative local `release` OID;
- every active source OID and its required ancestry;
- any submodule/config objects needed for checkout according to normal Git behavior;
- internal mirror refs under `refs/tollgate/` that make active synthetic objects reachable.

Mirror synchronization uses explicit local fetches/refspecs. Deleting or rebuilding the mirror invalidates no durable result by itself: a completed tested object retained in the authoritative repository can still be verified. Missing active synthetic objects are deterministically reconstructed only if the stored validation-generation inputs reproduce the same OIDs; otherwise those buildsets are invalidated and rerun.

Every current speculative generation also has an authoritative retention ref under `refs/tollgate/speculative/<generation-id>`. This makes a displayed `generation.tested_oid` immediately usable as a rebase base from any repository worktree and keeps historical OIDs recognizable long enough to distinguish ordinary stale-prefix churn from unsupported ancestry.

### 9.5 Synthetic prefix construction

Let `M0` be the observed local `release`, and let source commits be A, B, and C in queue order. Tollgate constructs one shared linear prefix chain for the affected queue suffix:

- `S_A = transplant(A, M0)`
- `S_B = transplant(B, S_A)`
- `S_C = transplant(C, S_B)`

Buildsets then validate `S_A`, `S_B`, and `S_C` concurrently. They are not independently synthesized chains; all descendants reuse the exact preceding synthetic object. Therefore after `M0 -> S_A`, the already-tested `S_B` has exactly the right parent for the next fast-forward.

Construction is serialized in a dedicated clean builder worktree:

1. Reset builder `HEAD`, index, and tracked files to the preceding prefix OID; remove non-ignored untracked files.
2. Apply the source commit's single-parent patch with the versioned Git-semantics profile. V1 uses Git's `ort` three-way cherry-pick machinery in no-commit mode, with `rerere`, signing, and hooks disabled and no user-supplied strategy options. The checked-in profile owns the exact argv and `-c` overrides; changing either bumps the engine epoch unless the golden fixtures remain byte-identical.
3. Treat a conflict or empty application as an unmergeable queue result; never invent conflict resolution or an empty promotion commit.
4. Write the resulting tree object.
5. Parse the raw source commit object. V1 requires, in order, `tree`, exactly one `parent`, `author`, and `committer`, followed by zero or more recognized optional `encoding`, `gpgsig`, and `gpgsig-sha256` headers in their source order, then a blank separator and the message. It accepts at most one of each optional header, parses continuation lines without normalization, and rejects duplicate required headers, malformed identities/dates, and every unrecognized extra header rather than silently discarding metadata.
6. If rewriting is required, serialize the new raw commit in the fixed order `tree`, `parent`, `author`, `committer`, optional `encoding`, blank line, and the source message bytes. Preserve author and committer header bytes, including timestamps and time zones, exactly. Omit recognized signature headers because the changed tree or parent invalidates them, while retaining source signature verification as audit metadata. Add no Tollgate author, trailer, queue ID, note, or other commit metadata. If tree and parent already match the source object, reuse the source object byte-for-byte, including any accepted signature headers.
7. Verify the new object's parent, tree, message bytes, and preserved identity fields before recording it in the affected validation generations.

Raw commit-object creation is preferable to invoking `git commit`: it avoids hooks, signing defaults, locale, current timestamps, and accidental message cleanup. Given the same parent, applied tree, and source metadata, reconstruction yields the same OID.

Builder commands use only the recorded Git-semantics profile. The test suite contains golden fixtures for clean application, three-way application, rename and mode changes, symlinks, submodule gitlinks, non-UTF-8 messages, an encoding header, SHA-1 and SHA-256 object formats, signed sources, empty application, and conflicts. Filters affect checkout/final-clean verification according to the repository's frozen Git configuration, but synthetic tree construction operates on Git objects and the index and never re-adds worktree bytes through a clean/smudge filter.

### 9.6 Queue revisions, validation generations, and invalidation

Every queue mutation advances the repository's monotonic queue revision. Commands use that revision to reject stale reorder, cancellation, cleanup, and other impact-sensitive requests. A queue revision is not evidence that code must be retested.

Candidate submission captures the revision used for ancestry classification and synthesis, then compares it again inside the same SQLite transaction that makes the queue item visible. A still-current prefix is accepted, a prefix already promoted into `release` is authoritative history, and any lost compare-and-swap returns structured stale-queue data. Queue promotion, extension, cancellation, supersession, or replacement must never surface as an internal service invariant failure.

Each active item is instead assigned an immutable validation generation identified by a digest of:

- the authoritative base OID that anchors its speculative prefix;
- ordered item IDs and source OIDs from that base through the item;
- active and satisfied hard-dependency inputs for that prefix;
- every computed prefix OID through the item;
- the effective configuration and step-graph digests;
- the engine compatibility epoch.

Appending or changing an item after a given item does not change that earlier item's validation generation. Removing, inserting, reordering, replacing, failing, or conflicting an item changes the generations of exactly the items whose prefix inputs changed. Those items' existing buildsets become retained invalidated history and new buildsets are created. An exact retained generation and its completed buildset may be reactivated only if the queue later returns to the identical complete validation identity. Staleness is structural and has no age-based TTL.

Successful promotion advances the queue revision and removes the promoted head from the active queue, but it does not change a surviving descendant's validation generation when the descendant's expected parent is the exact promoted OID and every other validation input remains unchanged. The descendant keeps its existing buildset and certificate. Candidate authorization likewise reuses a completed certificate only when it belongs to the exact generation selected by the current plan. A retained generation may become current again when the queue returns to precisely the same complete validation identity before a conflicting promotion; this is reactivation of the original generation and certificate, not transfer to a differently identified generation. The authorization event records every restored item. A later reconstruction may use the promoted OID as a new anchor, but it cannot transfer an old certificate to different inputs.

Authorization priority, cancellation/dequeue, conclusive failure, conflict, re-approval, retry enqueue, and manual reorder recompute only affected validation generations. An accepted external `release` movement or adopted remote-base movement, an applied configuration change, an engine-epoch change, or recovery that cannot prove inputs unchanged invalidates every affected unpromoted generation.

### 9.7 Zuul-style queue behavior

The gate follows Zuul's dependent-pipeline behavior:

- New candidates are initially ordered by durable enqueue sequence, subject to hard dependencies.
- Granting authority to a retained candidate permits its closure to bypass unrelated unauthorized items. Admission order remains a separate durable baseline, so authorization timing is not permanent user-directed priority.
- If later independent approvals make the authorized candidates a contiguous admission prefix, Tollgate restores admission order before promotion. It cancels replacement work and reactivates retained evidence only for byte-for-byte identical validation identities; unmatched items receive new generations normally.
- Manual reorder (`tg reorder`) replaces the admission-order baseline, preserves the dependency DAG, and restarts every item whose prefix changed. Later authorization convergence respects that explicit order.
- Each eligible item is tested with every active item ahead of it.
- A conclusive failure or merge conflict removes that item from the active queue.
- Affected independent descendants discard their results and restart without the failed item.
- Hard dependents are removed rather than rebuilt without their prerequisite.
- Passing heads promote in order. A descendant that already passed advances immediately only when its exact tested parent is the just-promoted OID and its assigned validation generation remains unchanged.
- Manual retry of a failed item creates a new tail item for the same immutable source OID. Moving it forward is a separate explicit reorder.
- Duplicate active source OIDs are rejected.

Each repository maintains a Zuul-style adaptive active window. The initial window is 20, the floor is 3, a successful promotion increases it linearly by one, and a conclusive failure halves it down to the floor. Configuration may set a ceiling or fixed behavior. The active window limits how many queue items may request execution; global resources and repository concurrency still set the actual number of simultaneous buildsets.

### 9.8 Queue item and buildset states

V1 uses separate closed enums for independent state dimensions. Repository execution state is `active`, `paused`, `configuration-pending`, or `blocked`; pause and block never overwrite an item's actual validation state. A block has one or more typed reasons and recorded recovery actions.

Queue-item state is exactly:

- pre-promotion: `constructing`, `queued`, `preparing`, `running`, `ready`, `promoting`;
- locally integrated: `promoted-local-push-pending`, used only after local CAS when remote push is enabled;
- terminal success: `promoted`, `externally-integrated`;
- terminal non-success: `failed`, `merge-conflict`, `dependency-failed`, `canceled`, `superseded`, `infrastructure-exhausted`.

Remote state is orthogonal and exactly `disabled`, `preflight-pending`, `ready`, `pushing`, `push-blocked`, `synchronized`, or `abandoned`. `abandoned` requires the explicit reconciled remote-promise abandonment in Section 10.4 and remains permanent audit history. Cleanup state is orthogonal and exactly `not-eligible`, `pending`, `running`, `completed`, or `needs-attention`. A successfully promoted item remains successful even when cleanup needs attention. When push is enabled, local CAS changes the item to `promoted-local-push-pending`; only exact remote observation or explicit abandonment changes it to `promoted` and releases the next promotion barrier.

Buildset state is exactly `pending`, `preparing`, `running`, `passed`, `passed-with-warnings`, `failed`, `interrupted`, `canceled`, `invalidated`, or `infrastructure-exhausted`. A pass certificate may be created from `passed` or `passed-with-warnings`; both require every applicable voting step to succeed. An invalidated buildset is immutable history, and reassignment creates a new buildset rather than reopening it.

State transitions are event-driven and exhaustive. `constructing -> queued -> preparing -> running -> ready -> promoting` is the only ordinary promotion path. Preparation failure returns to a new infrastructure attempt or ends at `infrastructure-exhausted`; voting failure ends at `failed`; construction conflict ends at `merge-conflict`; cancellation, supersession, and dependency loss use their named terminal states. `promoting` returns to `ready` only when the local ref is still the expected old OID and the intent is safely canceled; an unexpected ref enters repository `blocked`. No terminal item or buildset returns to a nonterminal state. Retry always creates a new queue item or buildset attempt as defined elsewhere.

The queue-item transition table is:

| From | Event and guard | To |
| --- | --- | --- |
| item created or surviving prefix changes | generation construction starts | `constructing` |
| `constructing` | synthetic OID, generation, and pending buildset are durably prepared | `queued` |
| `constructing` | transplant conflict/empty application | `merge-conflict` |
| `queued` | slot/resources acquired and buildset preparation starts | `preparing` |
| `preparing` | durable worker start handshake completes | `running` |
| `preparing` or `running` | transient infrastructure attempt ends below retry limit | `queued` with a new linked buildset attempt |
| `preparing` or `running` | infrastructure retry limit exhausted | `infrastructure-exhausted` |
| `running` | buildset passes and certificate is durably issued | `ready` |
| `running` | applicable voting validation fails | `failed` |
| `constructing`, `queued`, `preparing`, `running`, or `ready` | structural inputs change but item survives | `constructing` for a new validation generation; old work becomes `invalidated` |
| `constructing`, `queued`, `preparing`, `running`, or `ready` | cancel, supersede, or prerequisite loss | corresponding `canceled`, `superseded`, or `dependency-failed` |
| `constructing`, `queued`, `preparing`, `running`, or `ready` | adopted external history provably contains the item's source or current synthetic OID | `externally-integrated` |
| `ready` | promotion intent durably prepared | `promoting` |
| `promoting` | local CAS succeeds and push is disabled | `promoted` |
| `promoting` | local CAS succeeds and push is enabled | `promoted-local-push-pending` |
| `promoting` | CAS provably did not occur and intent is safely canceled | `ready` |
| `promoted-local-push-pending` | exact remote observation equals promoted OID | `promoted` |
| `promoted-local-push-pending` | explicit reconcile abandons frozen remote promise | `promoted`, with remote state `abandoned` |

An unexpected ref, ambiguous intent, push failure, or cleanup failure changes the appropriate repository/remote/cleanup dimension and leaves the queue-item state shown above; it is not represented as an invented queue transition.

The buildset transition table is:

| From | Event and guard | To |
| --- | --- | --- |
| `pending` | slot/resources acquired and start intent prepared | `preparing` |
| `preparing` | durable worker start handshake completes | `running` |
| `pending` or `preparing` | canceled before user command starts | `canceled` |
| `preparing` or `running` | structural generation invalidation | `invalidated` |
| `preparing` or `running` | app quit/crash, supervisor relationship loss, externally delivered HUP/INT/KILL/TERM (including a shell's conventional `128 + signal` status), or retriable setup failure below its retry limit | `interrupted` |
| `preparing` or `running` | infrastructure failure consumes the final allowed attempt | `infrastructure-exhausted` |
| `running` | all applicable voting steps and final verification succeed, no warnings | `passed` |
| `running` | all applicable voting steps and final verification succeed, non-voting failure exists | `passed-with-warnings` |
| `running` | voting failure, timeout, RSS violation, or dirty final checkout | `failed` |
| `running` | explicit item/check cancellation | `canceled` |
| `passed` or `passed-with-warnings` | later structural invalidation before promotion | `invalidated` |

No other buildset transition exists. A new attempt after `interrupted` is a new buildset record linked through `retry_of_buildset_id` under the same still-current generation; it does not change the old record back to pending. Its steps receive new attempt records and must all run again. A retry after structural invalidation belongs to the new generation.

The domain crate contains a transition table mapping every `(state, event)` pair to allowed preconditions, emitted state, and side-effect intent. Unlisted pairs are errors. Database transition methods require the expected old enum and repository event sequence in their update predicate, so a stale completion cannot overwrite a newer state.

`tg cancel <queue-item>` means dequeue: terminate its active buildset, remove it, and rebuild affected descendants. Canceling an independent check run simply terminates that run. v1 has no ambiguous step-only cancel that silently causes a queued buildset to restart.

### 9.9 Pass certificates

A pass certificate is created only after the buildset is terminal and includes:

- buildset, queue item, and validation-generation IDs;
- tested OID, tree OID, and exact expected parent OID;
- effective configuration and step-graph digests;
- engine compatibility epoch and captured environment fingerprint;
- complete set of applicable voting step IDs and their successful final attempt IDs;
- non-voting warnings;
- final `HEAD`, index, tracked-worktree, and initialized-submodule cleanliness checks;
- log completion offsets and integrity hashes;
- completion event sequence and monotonic/wall-clock timing.

Certificates are not reusable across independent `tg check` and gate buildsets. Pruning old logs or artifacts after a terminal promotion does not invalidate historical certificates; pruning never applies to an active queue certificate.

## 10. Promotion, pull, and push

### 10.1 Local promotion preconditions

The repository supervisor may begin promotion only when:

- the app and repository locks are held;
- the repository is neither paused nor blocked;
- the item is the active queue head;
- its certificate passes every current validity check;
- a synchronous re-open, parse, canonicalization, and digest of `<repository-root>/.tollgate/config.toml` equals the certificate's frozen configuration digest; filesystem watching is never sufficient evidence for this check;
- a synchronous probe confirms that the recorded Git executable path/file identity, versioned Git-semantics profile, object format, and engine epoch still match the validation generation;
- authoritative local `release` still equals the certificate's expected parent;
- `release` is not checked out in any authoritative worktree;
- every active certificate log reaches its recorded completion offset and matches its recorded integrity hash; missing or corrupt active evidence blocks promotion rather than silently weakening the certificate;
- the tested object has been copied from the mirror into `refs/tollgate/tested/<buildset-id>` and re-verified in the authoritative object database;
- when push is enabled, a fresh fetch proves the configured remote is at the expected remote OID.

Tested-object retention itself uses an intent before promotion: record the expected tested OID and nonexistent destination ref, fetch/copy that object into the authoritative object database through an explicit refspec to `refs/tollgate/tested/<buildset-id>`, verify object type/content/parent/tree and the exact ref OID, then complete the intent. A missing ref permits retry; an exact ref permits completion; a different ref blocks. Promotion never relies on an unreachable object that exists only in the disposable mirror.

### 10.2 Crash-safe local compare-and-swap

SQLite and Git cannot share one transaction, so promotion uses a durable intent protocol:

1. In a full-durability SQLite transaction, insert `promotion_intent(expected_old, tested_new, certificate_id)` and emit `promotion.started`.
2. Re-read and verify all preconditions outside the database transaction.
3. Use `git update-ref` with both new and expected old OIDs. If audit/ref cleanup is combined, use `update-ref --stdin` with `start`, `prepare`, and `commit` so all lockable ref changes succeed together.
4. In a second full-durability SQLite transaction, mark the intent complete, the item promoted, and the next head eligible; emit `promotion.completed` with the actual OID.

All authoritative internal ref changes use the frozen hooks-disabled Git profile. A multi-ref `update-ref` transaction gives all-or-none command success and per-ref atomic replacement, but readers outside the transaction may observe ref updates at different instants. No correctness decision therefore depends on a simultaneous unlocked read of `release` and a hidden ref. The supervisor serializes its own reads, and recovery verifies each ref independently against the intent before finalizing or blocking.

Recovery inspects every incomplete intent:

- If `release == tested_new`, re-verify the certificate and finalize the database event idempotently.
- If `release == expected_old`, no ref change occurred. Retry only if every precondition is still valid; otherwise cancel the intent and regenerate.
- If `release` is any other OID, do not guess. Enter external-movement reconciliation.

The implementation must fault-inject a crash before and after every durable boundary and Git operation.

### 10.3 Consecutive ready promotion

After promoting A, the supervisor reevaluates B rather than blindly consuming a ready flag. If B's tested commit has A's exact promoted OID as parent and B's assigned validation generation still matches its certificate, B may promote immediately. Promotion of A advances the queue revision but does not by itself change B's validation generation. This repeats for C and later ready descendants. Each commit gets its own local CAS, remote barrier when enabled, audit event, and cleanup decision.

### 10.4 Optional remote push

Push is off by default. When enabled, each promotion has two durable phases: local CAS, then leased remote push. Before local CAS, Tollgate fetches the remote and requires its branch to equal the recorded expected remote OID. After CAS it pushes the exact tested OID with an explicit expected-value lease. It never uses an unqualified force.

Remote observation and push use exact refs rather than human output or an implicitly updated tracking branch. Local `release` maps to the configured remote branch, normally `master`:

1. Fetch configured remote `refs/heads/master` into a dedicated Tollgate-owned remote-observation ref with an explicit refspec, record the fetched OID or explicit nonexistence, and never fetch directly into local `master` or local `release`.
2. Require that observation to equal the push intent's expected remote OID before local CAS. Missing, inaccessible, and divergent remote refs are distinct results.
3. After local CAS, push exactly `<tested-oid>:refs/heads/master` with `--force-with-lease=refs/heads/master:<expected-remote-oid>` and machine-readable status. Expected nonexistence uses Git's explicit empty expected-value lease form and is never conflated with an unknown observation. A configured local `pre-push` hook may reject this user-initiated operation; Tollgate records that as a failed attempt.
4. After transport success, query the exact remote ref directly using stable Git plumbing equivalent to `ls-remote --refs <remote> refs/heads/master`. Only an observed OID equal to the tested OID changes remote state to `synchronized`.

If exact observation succeeds, record it, change the locally integrated item from `promoted-local-push-pending` to `promoted`, and allow the next promotion. If push or observation fails after local CAS, local `release` remains correctly promoted, the item's remote state becomes `push-blocked`, and the repository promotion barrier remains closed. Running validations may finish; no later item promotes. `tg push` retries only the exact sequence of Tollgate-certified local promotions. Source cleanup waits for the push barrier when push is enabled.

An unfinished push intent freezes remote identity, URL, branch, expected OID/nonexistence, and local promoted chain. An ordinary configuration apply cannot disable pushing, change that remote target, or retarget the intent. The user must first complete the frozen push or use an explicit `tg reconcile` action that previews and records abandonment of the remote promise. Abandonment never rolls back local `release`; it marks the item `promoted`, records remote state as deliberately unsynchronized/disabled history, invalidates affected later generations under the newly applied policy, and only then releases the barrier and cleanup decision. Other configuration changes may be activated while push is blocked, but they cannot release the promotion barrier or mutate the frozen push intent.

Network unavailability does not cancel running CI. It prevents promotion at the remote preflight, or creates `push-blocked` if connectivity disappears after local CAS.

### 10.5 External local movement

Tollgate watches authoritative refs/worktree metadata and, more importantly, revalidates them before every transition where correctness depends on them.

- External fast-forward of local `release`: pause dispatch briefly, invalidate affected validation generations, adopt the new tip, remove queued source OIDs that are literal ancestors of the new tip, and rebuild the rest under the active local configuration. Additionally, if the new history contains an active item's exact current synthetic tested OID, verify that OID's stored generation inputs and object bytes, mark that item `externally-integrated` without creating a Tollgate pass or promotion event, and recompute descendants from the adopted base. Patch equivalence alone never qualifies. If the new tip contains a synthetic OID whose ownership or generation cannot be proved, block for reconciliation rather than applying the source patch again.
- Non-fast-forward, deletion, unrelated replacement, or local `release` becoming checked out: block the repository until explicit reconciliation. Movement of user-owned local `master` alone is not an integration-ref event.
- Ref movement during CAS: CAS fails without writing; apply the same classification.

Patch equivalence or patch IDs never prove that an item was externally integrated. Only literal ancestry permits automatic dequeue.

### 10.6 `tg pull`

`tg pull` is the safe Tollgate integration-branch pull operation:

1. Fetch the configured remote without updating either checked-out local `master` or Tollgate-owned local `release` implicitly.
2. If remote `master` is a strict fast-forward of local `release`, CAS local `release` to the remote OID, record an external-base event, and rebuild the queue under the active local configuration.
3. If local is equal or ahead, report no inbound update and show unpushed certified commits.
4. If the refs diverge, create no merge/rebase; block for `tg reconcile`.

When automatic pushing is enabled, remote `master` is authoritative: periodic fetch and the mandatory pre-promotion fetch adopt an inbound fast-forward immediately, invalidating runs that can no longer promote/push as tested. Fetch errors are visible but do not stop tests. With pushing disabled, automatic fetch only notifies; the user chooses when `tg pull` adopts the remote.

When automatic user-master synchronization is disabled or needs attention, ordinary `git pull --ff-only` remains the fallback in the primary `master` worktree. It updates the user-owned branch and its files, never Tollgate-owned `release`.

### 10.7 `tg push` and reconciliation

`tg push` pushes only a contiguous chain of exact Tollgate-promoted local `release` commits to the configured remote branch. It is not a feature-branch push wrapper. It refuses when local `release` contains an uncertified external commit, when the remote lease is unknown/stale, or when history diverges.

`tg reconcile` is an explicit guided operation for rewritten/deleted/diverged refs. It presents local release, last certified release, remote master, active queue base, and pending intents. Accepting a new base never blesses it as Tollgate-validated; it records external adoption, invalidates all active validation generations, and rebuilds. Any Git history repair itself remains an explicit user-directed Git action unless a previewed fast-forward/CAS is sufficient.

### 10.8 Gate-aware Git conveniences

- `tg update`: rebase the current clean, unqueued feature branch onto current gated `release`, then verify it has one unique source commit and report its new OID.
- `tg worktree create`: create a feature branch/worktree from the gated tip using configurable placement defaults.
- `tg worktree remove`: apply queued/landed/dirty safety checks before removal.

General feature fetching, staging, committing, inspection, and pushing remain ordinary Git commands.

## 11. Configuration and command model

### 11.1 Single trusted configuration

V1 has exactly one configuration file: `<repository-root>/.tollgate/config.toml`. It is repository-local, always trusted, defines the entire pipeline and policy, and is read only from the registered user worktree, never from a speculative checkout. Tollgate does not interpret a root `.tollgate.toml` or any other committed configuration source.

The configuration may define steps, voting/final behavior, resource ceilings, integration/remote policy, cache policy, and all other runtime settings. Tollgate expands defaults and produces one canonical representation and digest. Canonicalization fixes map ordering, default expansion, normalized relative paths, duration and size units, and schema version so the same effective configuration always hashes identically.

Keeping the Tollgate file local does not prevent CI behavior from evolving with source code. The stable configuration can invoke a tracked entry point such as `./ci`; each speculative checkout runs the version of that script contained in the exact tested commit. Repositories may commit an example configuration for people to copy, but Tollgate never discovers or activates it automatically. A future export/import workflow may improve portability without adding a second source of live policy.

Built-in defaults may fill optional fields, but `tg init` must establish at least one command before a repository can gate.

### 11.2 Minimal, language-agnostic schema

The smallest useful configuration is intentionally small:

```toml
version = 1

[[step]]
name = "ci"
run = "./ci"
```

The execution engine understands only generic fields:

- stable step name and shell command;
- optional explicit argv runner instead of a shell string;
- working directory relative to the slot root;
- ordered dependencies (`needs`), with declaration order forming an implicit sequential chain when no DAG is supplied;
- timeout, default 60 minutes;
- voting flag, default true;
- environment additions/removals;
- CPU/memory reservations, optional hard RSS cap, and named semaphores;
- include/exclude path globs;
- retained artifact paths/globs and whether each is required;
- cleanup/finalizer command where explicitly needed;
- cache path policy overrides and a user-controlled cache epoch.

Post-promotion policy is independent of validation execution. `sync_user_master` defaults to true and may be set to false to keep local `master` entirely user-managed. Synchronization never changes the tested object, certification decision, authoritative `release` ref, or remote lease result.

There is no language, framework, test suite, package manager, parser, or deployment primitive in the runtime schema. Multiple independent DAG roots may run concurrently within one slot only when explicitly configured; otherwise steps are sequential so they can safely share incremental output.

An effective gate buildset must contain at least one voting step unless the trusted local configuration explicitly permits a no-job item. `tg init` never generates a no-job gate.

The v1 execution contract is:

- Step names match `[A-Za-z0-9][A-Za-z0-9._-]{0,63}` and are unique after exact byte comparison. Artifact names and named semaphores use the same rule within their scopes.
- Each step specifies exactly one of nonempty `run` or `argv`. `run` is appended as the final single argument to the frozen repository runner, whose default argv prefix is `['/bin/sh', '-c']`; it is never evaluated by the login shell used for environment capture. `argv` is a nonempty array executed directly without shell parsing. Empty argv elements are allowed, but executable/runner elements and every string must be NUL-free. A repository may configure a different nonempty runner argv prefix, and that argv is part of the canonical configuration and validation generation.
- `working_directory` is a normalized UTF-8 path relative to the slot root. Absolute paths, `..`, an empty non-root component, and resolution through a symlink outside the slot are rejected.
- `needs` is an array of hard step-name dependencies. `soft_needs` is an array whose skipped prerequisite is acceptable; failure of either kind of prerequisite still prevents the dependent from running. A name cannot occur in both. Unknown names, self-edges, and cycles are configuration errors.
- With no explicit `needs` or `soft_needs` anywhere, declaration order forms a sequential chain. Once any explicit edge exists, the declared graph is used exactly and unconnected roots may run concurrently only when `allow_concurrent_roots = true`; otherwise the scheduler serializes otherwise-runnable roots in declaration order.
- `timeout` is runnable duration and defaults to `60m`. Durations canonicalize to integer nanoseconds and must be between one second and seven days inclusive. CPU, memory, RSS, and semaphore requests must be nonnegative, finite, and individually satisfiable by the configured global maximum or configuration activation fails.
- `voting` defaults to true and is the only step property controlling certificate eligibility. There is no separate v1 `required` step flag. A failed applicable voting step prevents a certificate; a failed non-voting step produces `passed-with-warnings` if all applicable voting steps succeed.
- `final` defaults to false. Final steps wait until all non-final branches are terminal and run after ordinary success, validation failure, timeout, or RSS failure while supervision remains intact. They do not start after user cancellation/dequeue, structural invalidation, app/worker interruption, or forced shutdown. A non-final step may not depend on a final step. Final steps otherwise have ordinary dependencies and their failures vote according to `voting`. Finalizers finish before artifact collection and the final checkout verification.
- Environment additions are a string-to-string map and removals are a unique string array. Names must match `[A-Za-z_][A-Za-z0-9_]*`; values must be NUL-free; a name present in both collections is rejected. Captured shell environment is the base, configuration removals apply second, configuration additions third, and Tollgate's documented read-only context variables last; configuration cannot override those context variables.
- Artifact declarations contain a unique name, one or more safe relative patterns, `required` defaulting to false, and an explicit retention policy. Here `required` describes artifact presence only, not step voting.

Configuration parsing rejects unknown fields. Canonicalization sorts maps and set-like arrays, preserves declaration order where it has execution meaning, expands validation-affecting defaults, normalizes paths/durations/sizes, and includes the schema version and runner argv. The default-true `sync_user_master` post-promotion preference is omitted from canonical bytes for compatibility with existing v1 configuration snapshots; an explicit false value is retained. `tg config explain` emits the complete effective policy. This document's field meanings and the checked-in machine-readable v1 schema are one compatibility contract; changing a validation-affecting meaning requires a schema or engine-epoch change.

### 11.3 Path matchers

Optional include/exclude globs are evaluated against the approved source commit's own single-commit diff, not the cumulative speculative prefix. Paths are repository-root-relative UTF-8 strings with `/` separators and case-sensitive matching. The grammar supports literal characters, `?`, `*` within one component, `**` across components, and bracket character classes; absolute patterns, parent traversal, malformed classes, and platform-native separator ambiguity are rejected. For a rename or copy, both old and new paths participate. Deletions participate using the old path. Diff acquisition uses stable NUL-delimited Git plumbing and an unrepresentable path is a configuration/buildset error rather than a silent mismatch.

No matcher means always applicable. Include patterns select a step when any changed path matches; matching excludes then remove it. A step with matchers that is not selected is `skipped`, which is distinct from success and is omitted from that buildset's applicable voting-step set. A dependent whose hard `needs` prerequisite was skipped is a configuration error for that buildset; a skipped `soft_needs` prerequisite permits the dependent to run. Matcher evaluation and the resulting applicable voting-step IDs are frozen in the validation generation before execution.

### 11.4 Initialization templates

`tg init` detects a small set of manifest/lock files and proposes editable shell commands from versioned data templates for Rust, Node, Python, Unity, generic script/Make/Just, and mixed repositories. Accepted commands are materialized into the repository-local configuration. Detection never remains hidden runtime behavior.

Templates may detect an already-installed third-party tool such as `sccache`, `mise`, `uv`, or a package manager and propose using it. Tollgate never installs such a tool. Template updates do not mutate existing repositories; `tg config regenerate` produces a reviewable diff.

### 11.5 Configuration changes

Changing the local configuration is an explicit gate-wide operation:

1. When Tollgate detects file contents different from the active digest, the repository enters `configuration-pending`. New dispatch and promotion stop, while active commands may finish and record results under their frozen configuration.
2. The app and `tg config apply` validate the candidate and preview every running or ready item that will restart. If the candidate's canonical digest matches the active digest, the pending state clears without invalidation.
3. After explicit confirmation, Tollgate activates the new canonical digest, advances the queue revision, invalidates every active generation that has not already crossed local CAS, terminates any still-running old buildsets, and rebuilds the affected queue. A `promoted-local-push-pending` head and its frozen push intent are historical local-promotion facts and are not invalidated or mutated by a later configuration digest; Section 10.4 keeps their barrier closed until exact push completion or explicit abandonment.
4. An invalid configuration blocks that repository until repaired or reverted. It never falls back silently to defaults or a previous digest.

Running buildsets never change configuration in place. Tracked scripts invoked by a configured command remain ordinary tested source content and need no special configuration activation behavior.

File watching exists only to make pending changes visible promptly. Before constructing a validation generation, dispatching a buildset, retrying infrastructure work, or promoting, the supervisor synchronously opens and canonicalizes the configuration and compares its digest with the repository's active digest. A missing, unreadable, concurrently changing, or invalid file enters `configuration-pending` or `blocked` and prevents the transition. To avoid accepting a torn read, Tollgate reads from an open descriptor, records file identity and metadata before and after parsing, and retries once when they change; a second change is reported as unstable configuration.

`tg config explain` and the UI show each effective field, its explicit or defaulted value, the active and pending digests, and which buildsets a candidate change would invalidate.

## 12. Execution environment and step semantics

### 12.1 Shell environment bootstrap

Dock-launched macOS apps do not reliably inherit the Terminal toolchain environment. At app startup, Tollgate resolves the configured login shell and invokes a shell-family adapter in interactive login mode to execute a small bundled environment-dump helper. The helper emits a magic-framed, length-safe environment representation, avoiding assumptions about `env -0` support or startup noise.

v1 supplies adapters for zsh, bash, and fish and permits explicit bootstrap argv for other shells. Bootstrap has a short timeout. A prompt, hang, nonzero exit, invalid frame, or missing shell blocks new CI until the user repairs it or explicitly accepts the clearly labeled minimal fallback.

Each successful bootstrap creates an immutable in-memory environment snapshot with an ID and salted/redacted fingerprint. Values are not stored in SQLite or logs; history stores variable names, snapshot ID, and fingerprint. A buildset freezes one snapshot before its first step, and every step in that buildset uses it. Explicit project variables from the active configuration override captured variables. Diagnostics show effective `PATH`, shell adapter, and executable resolution without revealing values marked sensitive.

“Reload shell environment” is available in the app and as `tg env reload`. Reload is prospective: it creates the snapshot used by future buildsets and whole-buildset retries, while running buildsets keep their existing in-memory snapshot. Passed buildsets and certificates remain valid and promotable after reload; the environment fingerprint is audit evidence, not a value that must equal the app's current snapshot at promotion. Old snapshot values remain in memory only while active buildsets reference them. After app restart, interrupted buildsets rerun under the newly captured snapshot because previous values were intentionally not persisted. A change to explicit environment additions or removals in `config.toml` remains a configuration change and follows Section 11.5.

Tollgate provides no secret store. Commands inherit the user's approved local environment, and Git uses existing credential helpers or SSH agents. The local configuration may explicitly remove variables. Users must understand that validation is unsandboxed code running with their account's authority.

### 12.2 Per-buildset preparation

A slot owns the entire buildset:

1. Acquire a slot, the global simultaneous-buildset permit, and the repository concurrency permit. Hold them for the buildset's lifetime.
2. Verify slot ownership/health and stop any leftover processes.
3. Reset `HEAD`, index, and tracked files to the exact tested OID.
4. Remove non-ignored untracked files while retaining ignored files.
5. Initialize/update submodules only when the generic configuration asks for it; initialization templates may propose the command.
6. Freeze command, configuration, environment-snapshot, and resource inputs and persist `buildset.started` before launching the first step.

The runner exports generic read-only context such as `CI=1`, queue/check mode, repository ID, item ID, source OID, tested OID, expected parent, validation-generation ID, slot path, and attempt count. These variables are execution context, not commit metadata.

Each step also receives a read-only `TOLLGATE_DIAGNOSTICS_FILE` path outside the
checkout. A step may write bounded JSONL records containing a stable diagnostic
code, human message, repository-relative paths, and an optional explicit `argv`
repair. Tollgate rejects malformed, oversized, symlinked, path-traversing, or
otherwise invalid diagnostic output and never derives repair commands from log
text. Diagnostics are sealed into the step attempt and buildset result.

For a failed voting step, comparable retained evidence requires the same tested
OID role, configuration digest, step-graph digest, engine epoch, and environment
fingerprint. A base success plus candidate failure is `candidate-introduced`;
the same failure on both is `inherited-from-base`; contradictory outcomes for
the exact candidate are `flaky-or-non-hermetic`; incomplete or mixed evidence is
`origin-unknown`. A diagnostic matrix may establish a new internally consistent
environment group after restart by running the exact base once and candidate
twice in cold slots.

Repair verification is explicit and never mutates retained source. Tollgate
first reproduces the diagnosed failure in a disposable checkout, executes one
unambiguous structured repair under the captured step environment, reruns all
applicable voting steps, and retains a binary patch only when they pass. The
patch is review material for a new immutable candidate, not certification of a
modified tree.

### 12.3 Process behavior

Commands are non-interactive by default:

- no pseudo-terminal;
- stdin closed;
- stdout and stderr captured as separate streams with per-stream byte offsets, plus a broker-assigned global observation sequence;
- configured `run` string through the frozen runner argv, or direct explicit `argv`, with the exact semantics in Section 11.2;
- slot root/default configured directory as cwd;
- one root process group per step, with same-session subordinate process groups retained under supervision while they remain descendants of the root command;
- Background CPU priority and macOS background I/O policy by default;
- configurable graceful termination signal and grace period, default 10 seconds, followed by process-group `SIGKILL` and a default 5-second reap/verification bound;
- monotonic runnable-time accounting with a default 60-minute timeout;
- an earlier no-output warning that does not change result.

The app holds the idle-sleep assertion while commands run. Explicit sleep pauses processes; wake detection annotates logs/timing and resumes the runnable clock.

Worker supervision uses an inherited lifetime channel rather than trusting a bare PID:

1. The app creates a private Unix socketpair, generates a random worker nonce, inserts the frozen step-attempt/start intent, and spawns `tollgate-worker` with one socket endpoint. Both endpoints are close-on-exec except for the descriptor intentionally inherited by the worker; the command child never inherits either endpoint.
2. The single-threaded worker creates the command child behind a start gate and establishes a new process group before user code can execute. The gated child exits without exec if the worker side of the gate closes, so a worker crash before registration cannot strand untracked user code. The worker reports the nonce, worker identity, child PID, process-group ID, and start-gate state. The app verifies the spawned worker identity, durably records `step.started`, and only then sends `start`.
3. The worker owns `waitpid`, termination escalation, and stdout/stderr pipe collection. It sends framed output and a terminal report over the lifetime channel. The app durably appends log frames before acknowledging them, so bounded channel backpressure may slow a producer but a UI never does.
4. Success is eligible only when the same uninterrupted lifetime channel delivers the matching terminal report, `waitpid` proves exit zero, both output pipes reach EOF, all log bytes are durable, the process group is reaped or proven empty, and the runner subsequently completes final checkout verification. A worker exit, channel loss, nonce/identity mismatch, unreaped group, malformed report, or ambiguous exit is `interrupted` regardless of any low-level exit marker.
5. If the app dies or closes its endpoint without an orderly shutdown command, the worker observes EOF, terminates the process group with the frozen escalation policy, writes only an exclusive-create interruption marker beneath the owned slot, and exits. PID/start-time or macOS process events may supplement diagnostics but are not the lifetime authority, avoiding PID-reuse ambiguity.
6. If the worker dies first, the app detects channel closure, terminates the recorded process group independently, marks the attempt interrupted, and quarantines the slot if it cannot prove the group empty. On restart, any database `running` state is interrupted even when a worker marker claims a clean exit.

Commands may create subordinate process groups within the root command's Unix session; the worker continues tracking their process identities, includes their resident memory in the step limit, and reaps any group that outlives the root command. Commands that deliberately daemonize, create a new session, join an unrelated process group, or otherwise escape the supervised process tree are unsupported in v1. A real containment rejection is retained as structured step diagnostic evidence rather than being represented only by the termination signal. Configuration diagnostics warn about known launchers with escaping behavior; cancellation guarantees apply to supervised descendants.

### 12.4 Result classification and retries

| Result | Queue meaning |
| --- | --- |
| Exit 0 | Provisional step success; certificate eligibility still requires all applicable voting steps, finalizers, artifacts, log completion, and the buildset-level final clean check. |
| Ordinary nonzero exit | Conclusive validation failure; no automatic retry. |
| External HUP/INT/KILL/TERM, including a shell's conventional `128 + signal` exit | Interrupted infrastructure attempt; whole-buildset retry. |
| Timeout | Conclusive validation failure; no automatic retry. |
| Configured hard RSS violation | Conclusive validation failure. |
| Final checkout discrepancy | Conclusive `workspace-dirty` buildset failure even when every command exited zero. |
| Dependency failure | Step skipped; buildset outcome derives from the failed dependency. |
| User cancellation/dequeue | Canceled, not failed. |
| App quit/crash or lost supervisor | Interrupted infrastructure result; whole buildset reruns. |
| Spawn/setup/slot transient error | Whole-buildset infrastructure retry at the same queue position and validation generation, up to three attempts by default. The retry freezes the current environment snapshot. |
| Exhausted infrastructure attempts | Terminal infrastructure failure; item leaves the queue with a distinct reason. |

Following Zuul, `fail-fast` is false by default. Independent DAG branches continue after another branch fails so users get complete feedback; dependent steps skip. Early failure-output pattern matching is out of scope.

Attempts are never spliced together: a retry must independently complete every applicable voting step before it can receive a pass certificate.

### 12.5 Final checkout verification

After all relevant steps/finalizers and before issuing a pass certificate, Tollgate verifies:

- `HEAD` is the exact tested OID;
- the index matches that OID;
- all tracked main-worktree files match the index after applying configured Git filters normally;
- initialized submodule `HEAD`s match recorded gitlinks and their tracked worktrees are clean;
- no Git operation left a merge, cherry-pick, rebase, or other in-progress state.

Ignored files may differ. Any tracked/index/HEAD discrepancy is `workspace-dirty` validation failure. This check cannot defend against a deliberately malicious command that temporarily tests other content and restores it; v1's trust model is approved, unsandboxed local code.

### 12.6 Independent checks

`tg check [<rev>]` creates an independent buildset using the same slots, caches, configuration rules, logs, and history, but no dependent queue item or promotion authority. Checks are lower priority than promotion-critical gate work. Their results never become gate certificates, even if an OID and commands appear identical.

## 13. Logs and retained artifacts

### 13.1 Log pipeline

The runner writes every stdout/stderr frame to an append-only per-attempt log before publishing it. Each frame has stream, per-stream byte offset, broker observation sequence, monotonic timestamp, wall timestamp, and payload length. Offsets prove gap-free order within stdout or stderr. The broker sequence is only the order in which Tollgate observed reads from the two independent pipes; it does not claim causal ordering between bytes concurrently written to stdout and stderr. A bounded broadcast channel may drop live UI delivery under pressure, but never process output or durable log bytes; clients resume by durable frame/stream offsets.

Active logs remain uncompressed. Completed logs are indexed and compressed in seekable chunks so the UI can request ranges, tail efficiently, search, and preserve offsets. Log completion includes final offsets and hashes in the pass certificate. Invalid UTF-8 is preserved in storage and rendered lossily with an indicator. ANSI escape rendering is supported without granting terminal input.

The UI uses virtualization and bounded decoded buffers. Follow mode, pause, stdout/stderr filtering, search, copy, and “open raw log” are available. A slow/closed window never blocks a command.

### 13.2 Retained artifacts

Steps may declare generic paths/globs to retain. At step/buildset completion Tollgate resolves paths beneath the slot, rejects unsafe escaping symlinks, computes size/hash, and APFS-clones files into repository-local artifact history when source/destination allow it; otherwise it copies only after enforcing budgets. Missing required artifacts fail the step's post phase; optional misses are warnings.

Retained artifacts are immutable run outputs and use the 30-day/50-GiB policy. Incremental caches remain mutable slot/seed data and are never retained as historical artifacts merely because they are large. The UI can reveal an artifact in Finder, open it with the default app, pin it, or prune it.

## 14. Persistent slots and artifact reuse

### 14.1 Default persistence rule

Before every buildset, Tollgate resets tracked state and removes non-ignored untracked files but deliberately preserves ignored files. This automatically retains conventional `target/`, `Library/`, `node_modules/`, `.venv/`, and unknown tool outputs without teaching Tollgate those formats.

The correctness assumption is explicit: build tools are responsible for invalidating their incremental output after a source/toolchain change, as they are in a developer checkout. Tollgate provides cold retries and resets when that assumption fails. Passing CI does not certify a cache as universally healthy.

Private policy can classify ignored paths as:

- `preserve`: retain only in that persistent slot;
- `clone`: include in compatible warm seeds (default for ordinary ignored artifacts);
- `shared`: point commands to an explicitly configured cross-slot content-addressed cache;
- `discard`: remove before every buildset;
- `sensitive`: retain only in-slot, never snapshot or artifact-copy.

The default is derived from actual ignored paths created in Tollgate-owned slots, not ignored files from a developer worktree. A user-controlled `cache_epoch` invalidates all earlier seeds without requiring Tollgate to understand a toolchain.

### 14.2 Seed capture

APFS cloning is a required v1 implementation path, not a best-effort optimization. Tollgate uses a small native Rust/macOS clone adapter backed by `clonefileat`/`fclonefileat` and related fd-relative metadata operations. It does not use `/bin/cp -c` for a clone-required operation because that command may fall back to `copyfile` and succeed with a physical copy. Physical copying is a distinct, explicitly authorized operation with its own preview, intent, budget check, and audit event.

The clone adapter obeys this contract:

- Open and retain file descriptors for the verified source root and Tollgate-owned staging parent; enumerate relative to those descriptors without following symlinks or crossing a filesystem device boundary.
- Require source and destination to be on the same clone-capable volume. Every regular file is created at a previously nonexistent destination with a force-clone operation using no-follow/beneath-resolution protections and ACL preservation. A successful clone syscall is the evidence that copy-on-write sharing occurred; one failure fails the generation, with no per-file copy fallback.
- Recreate directories explicitly and apply recorded modes, timestamps, flags, ACLs, and extended attributes after their children. Recreate only relative symbolic links whose lexical resolution remains under the eligible cache root; reject absolute or escaping links. Reject devices, sockets, FIFOs, mount crossings, unsafe ownership, and entries that change type or identity while enumerated.
- Preserve hard-link relationships within the selected tree by mapping source `(device, inode)` pairs: clone the first regular-file occurrence and create later occurrences with `linkat`. A hard link to an unselected path does not cause that external path to be imported.
- Record original writable modes and flags in the manifest. A published seed is immutable by Tollgate state and ownership discipline: it is never used as a command working directory and is never modified in place. Publication does not recursively remove write bits or add immutable flags that would leak into subsequently cloned slot files.
- Build only beneath an exclusive, randomly named staging directory in the final seed's parent. Validate the completed tree against the manifest, durably write and fsync the manifest and relevant directories, atomically rename the staging directory to its generation name, and fsync the parent. Any error or crash leaves an unpublished staging path that recovery quarantines; consumers only open generations whose completed intent and manifest agree.

An idle donor is one whose buildset is durably terminal, whose worker lifetime channels are closed, and whose recorded process groups have been reaped or proven absent. Source metadata is checked before and after enumeration. A change aborts capture rather than publishing a mixed-time view; the same-user malicious mutation excluded by the threat model remains outside this guarantee.

Automatic seed snapshots occur after:

- a successful bootstrap validation of `release`; and
- a successful promoted-head validation when its cache profile materially improves the current seed.

They do not occur after every passing speculative descendant. `tg cache snapshot` may capture an idle eligible slot manually.

At a safe boundary, with the donor slot idle and all processes reaped:

1. Enumerate eligible ignored paths with stable Git porcelain plus trusted cache overrides.
2. Reject paths with unsafe type/ownership/symlink behavior.
3. Clone them through the clone adapter to a staging seed directory on the same volume; every regular file must report successful clone creation.
4. Write a manifest containing repository/profile/cache epoch, source tested OID, queue-prefix ancestry, configuration/cache-policy digest, OS/architecture, path metadata, logical size, and hashes for small structural files.
5. Validate and publish the staging directory as an immutable seed generation using the durability sequence above.

Tollgate never snapshots a running slot. A failed bootstrap leaves its persistent slot warm but does not automatically publish it as the shared seed.

### 14.3 New-slot provisioning

Creating a slot is a durable state machine rather than a directory copy:

1. Allocate a slot ID/path and record `provisioning`.
2. Create a registered, locked, detached worktree from the execution mirror at the requested tested OID.
3. Select a compatible seed, preferring the longest matching speculative prefix, then nearest known commit, then newest compatible seed.
4. Clone manifested paths with the same force-clone adapter into a staging area, restore the manifest's original writable metadata, validate the result, and atomically install each top-level cache path. Never overwrite the worktree's `.git` link or tracked files, and never accept a physical-copy fallback in this path.
5. Reset/clean tracked state again and validate the worktree registration.
6. Run an optional idempotent generic slot-setup command.
7. Mark the slot `ready` only after every check succeeds.

If no compatible seed exists, provision cold. If seeding fails, quarantine the partial path and retry cold rather than using a torn mixture. A new slot is never cloned from a live donor.

Compatibility requires repository ID, Apple Silicon/macOS cache profile, cache epoch, and trusted cache-policy compatibility. Source/config proximity affects preference, not strict compatibility, because the chosen default trusts build-tool invalidation. Toolchain and shell fingerprints are recorded for diagnosis. Projects needing strict separation increment the epoch or define profiles.

### 14.4 Slot selection and affinity

The scheduler scores idle slots by:

1. exact previous tested OID;
2. longest matching queue prefix/nearest known ancestry;
3. matching configuration/cache profile;
4. most recent successful use;
5. provisioning and fairness cost.

Affinity is a tie-breaker after correctness, priority, and cross-repository fairness. A warm slot cannot jump ahead of a promotion-critical buildset that lacks it.

### 14.5 Shared third-party caches

Tollgate may supply stable per-user/per-repository directories and environment variables for already-installed content-addressed caches. Initialization templates can propose `sccache` or package-manager stores. These shared caches must be safe for concurrent writers according to their own tool; Tollgate never shares a mutable build tree such as one Cargo `target/` or Unity `Library/` between running slots.

### 14.6 Recovery and hard reset

- `tg retry <item> --cold`: create/reuse a clean slot without importing a seed. Existing caches remain.
- `tg slot reset <slot>`: cancel its run if explicitly confirmed, move the slot to quarantine, unregister/recreate the worktree cold, and preserve queue/history.
- `tg cache purge`: delete eligible seeds.
- `tg cache purge --all-slots`: rebuild every idle/confirmed slot cold in addition to deleting seeds.

The UI provides the same actions with affected paths, logical size, estimated physical impact, and queue consequences. Automatic cold retry after an ordinary test failure is off by default. Projects may opt failures of a selected step into one whole-buildset cold retry, but it must remain visible as a distinct attempt.

## 15. Scheduling and resource control

### 15.1 Global resource pool

The app owns global budgets for:

- maximum simultaneous buildsets;
- CPU reservation tokens;
- memory reservation tokens;
- per-volume warning/critical free-space thresholds and emergency allowances;
- named semaphores/exclusive resources.

Disk admission is tracked per mounted volume, not per path or as one global byte counter. On activation the service resolves the authoritative Git/SQLite root, log/artifact root, and every cache root to stable volume identities and records which roles share a volume. Each volume has a warning free-space threshold and a lower critical free-space threshold selected during setup:

- Below the warning threshold, Tollgate stops new buildset admission, slot provisioning, seed capture/copy, compression, and optional artifact retention on that volume, then prunes only eligible owned data according to policy.
- Before approval-ref creation, tested-object transfer, result completion, promotion, push-intent recording, migration, or backup, Tollgate reserves a conservative operation allowance on every affected volume. The transition does not start unless the post-operation estimate remains above the critical threshold.
- If an active command drives its slot/log volume below the critical threshold, Tollgate stops admitting output, terminates the supervised process as an infrastructure interruption, flushes the interruption evidence, and quarantines the slot if cleanup cannot be completed safely. Validation failure is never reported for storage exhaustion.
- If the authoritative Git/SQLite volume falls below the critical threshold, the repository blocks before any non-recovery mutation. Recovery writes and an orderly worker termination use a separately accounted emergency allowance established at initialization.
- APFS logical size is not charged as immediate physical allocation, but the admission model assumes future copy-on-write divergence and continuously monitors real free space. Clone success never waives reserve enforcement.

Each repository may set a concurrency cap and scheduler weight. A buildset acquires one global simultaneous-buildset permit, one repository concurrency permit, and one slot before preparation, and holds them until all steps, finalizers, artifact collection, and final checkout verification finish.

CPU reservations, memory reservations, and named semaphores belong to individual steps. Immediately before launching a runnable step, the scheduler acquires that step's complete resource request atomically; partial acquisition is prohibited. It releases those resources after the process and step post-processing finish. Concurrent DAG roots request independently, and steps in the same buildset contend for a named semaphore exactly like steps in different buildsets. Aggregate reserved CPU and memory are the sums for currently running steps, not every future step in an admitted buildset.

The obvious user control is “maximum simultaneous CI runs,” while initialization derives conservative token defaults from physical CPU and RAM and shows them for confirmation. CPU/memory declarations are admission-control reservations, not cgroup quotas. Tollgate monitors aggregate process-tree CPU and RSS and pauses new step admission at the configured threshold. It does not kill a compiler for a transient unconfigured spike. An optional per-step hard RSS cap is enforced by monitoring that step's process tree and treating a violation as failure.

### 15.2 Priority and fairness

Default priority classes are:

1. Gate-head validations that can directly unlock promotion.
2. Earlier speculative descendants within each active window.
3. Independent checks.
4. Bootstrap, seed refresh, compression, and cache maintenance.

The scheduler selects both buildsets awaiting slots and runnable steps awaiting step resources. A step inherits its buildset's priority class and queue position. Within a class, weighted round-robin across repositories prevents starvation. Queue position orders items within a repository. Slot affinity breaks otherwise-equal buildset choices only. Maintenance yields immediately to user validation work.

### 15.3 Pause and blocking

Repository `pause` stops new dispatch and promotion while allowing active commands to finish and record results. `resume` rechecks structural validity before using any completed result. Global pause applies the same rule to all repositories. Cancel/dequeue is separate and destructive to active work.

Blocking states such as remote divergence, invalid trusted config, checked-out `release`, exhausted push failure, state corruption, or ambiguous recovery stop promotion and any work whose inputs cannot be proven. Unrelated repositories continue.

### 15.4 Background behavior

Worker groups run in Background priority by default, with a global preference for Normal mode and a temporary “Boost active runs” action. Priority does not replace concurrency/resource limits. Power assertions exist only while at least one command is active and are reference counted.

## 16. CLI contract

### 16.1 Command families

The initial CLI surface is:

| Command | Contract |
| --- | --- |
| `tg init` | Register repository, create Tollgate-owned local `release` at the exact local `master` OID without changing the checkout, create the trusted local config, validate Git/shell/APFS/ref ownership, configure resources, provision a slot, and offer bootstrap CI. |
| `tg repo add/remove/list` | Explicit registry management. Remove unregisters by default; it does not erase durable repository state. |
| `tg candidate [<rev>] [--wait] [--retain-worktree]` | Capture clean immutable source without promotion authority; optionally wait for validation and optionally preserve the source worktree after promotion. |
| `tg approve [<rev>] [--wait] [--retain-worktree]` | Capture clean immutable source with promotion authority, enqueue, return item ID; optionally wait. Candidate-ID authorization uses the cleanup policy captured at submission. |
| `tg push-master [--wait\|--status]` | Rebase a clean stale local `master` range onto certified `release` when needed, authorize each linear commit oldest-first, and return after scheduling by default. While validation runs, project an unchanged clean local tip onto rebuilt speculative history whenever certified `release` advances; `--wait` additionally waits for the tail result, while read-only `--status` reports the latest durably identified master push and any failed step. |
| `tg queue` | Ordered active queue, queue revision, per-item validation generations, dependencies, states, and prefix OIDs. |
| `tg status [<id>]` | Repository/item/buildset/slot summary. |
| `tg wait <id>` | Subscribe until terminal/blocked outcome; handle Ctrl-C without canceling CI. |
| `tg logs <id> [--step ...] [--follow]` | Offset-resumable log output. |
| `tg cancel <id>` | Preview and dequeue queue item or cancel independent check. |
| `tg retry <failed-id> [--cold]` | Fresh tail enqueue of the same source OID. |
| `tg reorder <id>...` | Preview and reorder within hard-dependency constraints. |
| `tg check [<rev>] [--wait]` | Independent validation with no promotion. |
| `tg diagnose <id> [--no-replay] [--verify-repair]` | Attribute a voting failure from comparable evidence; by default run a cold base/candidate/candidate matrix, and optionally verify one structured repair into an immutable patch artifact. |
| `tg pause/resume` | Non-destructive repository gate hold. |
| `tg pull` | Gate-aware fetch and fast-forward adoption. |
| `tg push` | Push only contiguous certified local `release` commits to the configured remote branch with a lease. |
| `tg reconcile` | Guided external movement/divergence recovery. |
| `tg update` | Safe one-commit feature rebase onto current gated `release`. |
| `tg worktree create/remove` | Gate-aware feature worktree lifecycle. |
| `tg env reload/show` | Bootstrap and diagnose shell environment. |
| `tg config validate/explain/regenerate/apply` | Validate, inspect, regenerate, preview, and explicitly activate the single local configuration. |
| `tg slot list/reset` | Slot health and cold recreation. |
| `tg cache status/snapshot/purge` | Cache policy, seeds, budgets, hard reset. |
| `tg history` | Audit/results query and retained data controls. |
| `tg doctor` | Cross-layer diagnosis with local redacted export. |

State-changing commands accept a command UUID for idempotency internally; a CLI retry after lost IPC response cannot duplicate an approval or cancel. Candidate authorization is also convergent across distinct commands: if a concurrent dependent authorization already covered the requested active candidate, the later command durably records an unchanged successful result and may continue waiting for that candidate.

### 16.2 Output and exit behavior

`tg approve` returns after durable enqueue rather than waiting 10+ minutes. `--wait` is equivalent to enqueue followed by `tg wait`. JSON wait output is compact newline-delimited state: after the command result, the CLI emits the selected queue item, repository execution state, and block reasons only when that record changes. Polling uses a candidate-scoped service read and never transfers periodic application snapshots or detailed buildset history. `tg status <candidate-id>` likewise uses a direct candidate-details read while preserving its detailed evidence output; only repository-wide status transfers an application snapshot. Read commands have stable versioned `--json`; human output may evolve. JSON uses full IDs/OIDs and enum values, while human output may show unambiguous prefixes.

Wait/log streams resume from event/log sequence offsets after transient IPC disconnect. Ctrl-C detaches the client only and exits 130 without canceling CI. The v1 numeric exit contract is: `0` command accepted or waited-for success/promotion; `1` conclusive validation failure or merge conflict; `2` command syntax, invalid argument, invalid configuration, or unknown target; `3` canceled, superseded, or dependency-failed target; `4` blocked, push-blocked, reconciliation-required, or cleanup-needs-attention; `5` infrastructure exhaustion, app launch/service failure, IPC/protocol incompatibility, or internal consistency failure. Read-only commands return `0` when the requested state is successfully reported even if that state describes a failed item. Future meanings require a versioned JSON/protocol contract change rather than reusing a code silently.

### 16.3 App launch and installation

Live commands auto-launch the app unless `--no-launch`. A command that requires the app never mutates SQLite directly. Offline historical reads may eventually use a read-only library, but v1 should prefer app service consistency.

The signed/notarized Apple-Silicon app bundle contains the matching `tg` binary. First launch offers to create/update a symlink in `~/.local/bin/tg` and diagnoses whether that directory is on imported `PATH`. App updates replace both components. A Homebrew cask may automate the same layout later.

## 17. Desktop user experience

### 17.1 Navigation

The app has a persistent left sidebar containing only explicitly registered repositories. Badges summarize running, queued, failed, and blocked states. Repositories are never auto-discovered.

On reopen, the app restores the previously viewed repository, route, selected queue/history item and step, filters, log position/follow state, sidebar state, and window geometry. Back/forward navigation and recent selection history behave like a normal desktop browser shell. An aggregate Home view may exist but is never forced on launch.

### 17.2 Repository queue view

The primary repository view presents the queue as an ordered speculative chain. Each row/card shows:

- queue position, hard-dependency edges, and active-window eligibility;
- source branch/worktree label and immutable source OID;
- exact tested prefix OID and its expected parent;
- which earlier patches are included;
- queue revision, validation generation, and stale/invalidation lineage;
- step summary, current slot, elapsed/estimated timing;
- pass/failure/conflict/cancel state;
- local promotion, remote push, and cleanup state.

Selecting an item opens its frozen configuration, prefix composition, attempts, step DAG/list, timing, logs, artifacts, certificate, and audit timeline. Ready descendants are visually different from promoted items. A passing item with non-voting failures says “passed with warnings,” not green success.

### 17.3 Slots and resources

A resource panel shows global run capacity, CPU/memory reservations versus observed use, named semaphores, disk reserve, running processes, and scheduler order. Slot detail shows checkout OID, last/next use, cache profile, seed origin, logical/estimated physical size, health, and reset controls.

### 17.4 Operations

Every normal queue/CI operation available in the CLI is available in the UI: approve from a selected clean worktree, cancel/dequeue, retry/cold retry, reorder, pause/resume, pull, push retry, reconcile, check, worktree cleanup, slot reset, seed/cache purge, retained artifact/log management, and environment reload.

Operations that invalidate descendants, kill a process, delete a worktree/branch, discard caches, adopt an external base, or reorder the queue present a concrete impact preview. UI commands carry observed queue revision and are rejected/repreviewed if state changed before confirmation.

### 17.5 History and storage

History is queryable by repository, source/tested/promoted OID, branch label, queue item, result, step, time, and terminal reason. Invalidated attempts remain visible and link to the event that invalidated them. Promotion/push recovery and external-base adoption are part of the same timeline.

Storage settings show separate budgets/usage for logs, retained artifacts, slots, and seeds; pinned items and minimum reserves are explicit. Pruned content is represented by tombstone metadata.

### 17.6 Notifications

macOS notifications are failure/attention-only: conclusive validation failure, exhausted infrastructure attempts, setup/bootstrap failure, push failure, a user-master synchronization refusal, or repository block requiring reconciliation. Success, promotion, starts, retries, and ordinary invalidation do not notify. Clicking a notification opens the exact item/step. Per-repository mute and global quiet mode remain available.

### 17.7 Frontend implementation

React/TypeScript is appropriate for the complex queue, virtualized history, and terminal-style logs. All authoritative logic stays in Rust. The UI should use generated API types, an explicit query/cache layer for snapshots, ordered event reducers keyed by repository sequence, and an ANSI renderer that does not expose an interactive terminal.

Accessibility requirements include keyboard access to every operation, non-color state labels/icons, VoiceOver names for queue/dependency state, reduced-motion support, and system light/dark appearance.

## 18. Initialization and diagnostics

### 18.1 `tg init` flow

Initialization is resumable and includes:

1. Resolve/canonicalize the Git common directory and create repository UUID/lock scope.
2. Probe system Git version/features, object format, worktree state, local `master`, and any pre-existing local `release`.
3. Atomically create local `release` at the exact local `master` OID, or refuse to overwrite a divergent pre-existing branch; leave the current checkout unchanged.
4. Register the repository explicitly with the app.
5. Bootstrap the login-shell environment and show tool resolution.
6. Detect repository templates and write the editable trusted local configuration.
7. Detect every authoritative/log/artifact/cache volume, APFS clone capability, shared-volume roles, and propose global/repository resource budgets plus per-volume warning/critical/emergency storage thresholds.
8. Create/synchronize the execution mirror.
9. Provision the first slot.
10. With explicit confirmation, run validation on current `release` to verify commands and create the first successful warm seed.

`--no-bootstrap` skips step 10 and warns that the first approval starts cold. A failing baseline records `baseline-failing`, sends a failure notification, and leaves the gate usable because the next change may fix `release`. It does not automatically publish a failed slot as a seed.

### 18.2 `tg doctor`

Diagnostics verify:

- app/CLI/protocol versions and single-instance lock;
- repository/common-dir identity and permissions;
- SQLite integrity/schema/backups;
- integration ref ownership and external movement;
- hidden source/tested refs and required object reachability;
- execution mirror synchronization;
- worktree registrations, HEAD/index cleanliness, and stale processes;
- shell bootstrap, PATH, command executable resolution, and environment fingerprint;
- APFS clone capability and seed manifests;
- resource/storage budgets, stable volume-role mapping, and per-volume warning/critical/emergency free-space thresholds;
- remote URL, fetch reachability, credentials without exposing secrets, and lease state.

A diagnostics bundle is local, redacted, previewable, and user-shared only. It contains no environment values or full logs by default.

## 19. Recovery model

### 19.1 Startup reconciliation

The app acquires its single-instance lock, opens/migrates global preferences, then activates registered repositories independently. For each repository it:

1. Acquires the repository ownership lock.
2. Opens SQLite, checks integrity, and reads the last clean-shutdown marker.
3. Resolves the authoritative Git common directory, Tollgate-owned `release`, user-owned `master`, worktrees, hidden refs, and object reachability.
4. Reconciles incomplete approval, promotion, push, cleanup, artifact, seed, and pruning intents.
5. Reconciles mirror/worktree/slot registrations and terminates or quarantines leftover workers.
6. Marks active buildsets interrupted and creates new attempts or validation generations as required.
7. Reloads the trusted local configuration and captures a new shell-environment snapshot.
8. Blocks or resumes the repository based on proof, never on optimistic inference.

One corrupt/blocked repository does not prevent other registered repositories or the app UI from starting.

Intent reconciliation uses this evidence matrix:

| Intent | Evidence sufficient to complete | Safe no-effect/cancel evidence | Otherwise |
| --- | --- | --- | --- |
| Approval | retention ref equals recorded source OID and all recorded source/dependency inputs still verify | ref is absent | block; delete only with the recorded old-OID assertion after explicit intent cancellation |
| Tested-object retention | tested ref equals recorded tested OID and object parent/tree/content verify | tested ref is absent | block |
| Result completion | matching live worker terminal frame was received without lifetime-channel loss, logs are complete/durable, process group is reaped, and final checkout verification is recorded | any missing supervision evidence means the buildset is interrupted, never successful | block on contradictory durable evidence |
| Promotion | authoritative `release` equals recorded tested-new OID and certificate still verifies | `release` equals expected-old and all other owned refs show no committed change | external-movement block |
| Push | direct remote observation equals recorded tested OID | remote still equals exact expected-old/nonexistence and local promoted chain still verifies | `push-blocked`/divergence |
| Cleanup | each worktree path, registration, branch ref, and old OID independently matches the completed sub-operation evidence | unchanged owned worktree/ref still matches the pre-cleanup snapshot | `needs-attention`; never recreate or delete by guess |
| Artifact publication | final path has exclusive ownership marker and manifest/hash/size match | final path absent and only owned staging exists | quarantine conflicting/partial paths |
| Seed publication | generation path, completed intent, manifest, entry metadata, and per-file clone-success records agree | final generation absent and only owned staging exists | quarantine and provision cold |
| Pruning | tombstone and owned quarantine path identify the exact generation/artifact selected | original still exists unchanged and quarantine does not | block on identity mismatch; deletion may resume only inside verified quarantine |
| Migration/backup | schema version, migration journal, and verified online-backup identity agree | old schema and database identity remain intact | preserve both database/backup and block |

Completion based on recovery evidence emits the same idempotent domain result as the uninterrupted path, with actor `recovery`. Recovery never uses a worker interruption marker as success evidence and never treats a transport exit code, filename prefix, or SQLite intent alone as proof of an external effect.

### 19.2 Quit and crash

On explicit Quit, stop admitting work, persist shutdown intent, signal all worker groups, wait a bounded grace period, force-kill, checkpoint databases, release power assertions/locks, and mark clean shutdown. Running buildsets rerun in full next time.

On app crash, workers detect parent exit and kill command groups. On restart, any `running` database state without a completed durable result becomes interrupted even if an exit marker suggests success. A worker cannot author a pass certificate.

### 19.3 Mirror, slot, and seed loss

- Missing/corrupt mirror: recreate from authoritative retained refs, invalidate any buildset whose tested object cannot be proven/reconstructed, and preserve history.
- Missing idle slot: unregister stale worktree metadata and provision a replacement.
- Missing active slot after crash: mark interrupted and replace.
- Invalid seed manifest/clone failure: quarantine seed and provision cold.
- Low disk: apply the per-volume warning/critical policy in Section 15.1. Stop new work at warning reserve, interrupt active writers that cross the critical reserve, preserve the emergency recovery allowance, and never begin a Git/SQLite transition whose conservative allowance cannot be reserved.

### 19.4 Database corruption

If integrity checks fail, freeze the repository and offer restore from the newest verified online backup. Never overwrite the corrupt database before preserving it for diagnosis. After restore, replay only independently provable Git intent outcomes and rebuild ephemeral queue execution state. If no backup is valid, provide a guided export/reconstruction path from authoritative refs, logs, and event files; do not silently fabricate missing pass certificates.

### 19.5 App upgrades

Schema migration creates and verifies an online backup first. The app has an `engine_compatibility_epoch` separate from semantic version. Routine UI/fix releases preserve completed certificates. A release that changes commit construction, config freezing, result meaning, or certificate rules increments the epoch and invalidates unpromoted buildsets.

## 20. Security and privacy

### 20.1 Threat model

Tollgate is a personal local tool, not a hostile multi-tenant executor. Approval is consent to execute the selected commit and effective commands as the logged-in user. Commands can access the user's files and network with that account's authority. The execution mirror protects authoritative Git refs from accidental commands; it does not contain malicious same-user code.

The app is signed/notarized and uses hardened runtime where compatible, but is not Mac App Store sandboxed because it must access user-selected repositories, launch arbitrary toolchains, and manage worktrees. Repository registration and destructive path scope remain explicit.

### 20.2 Local interfaces

- No listening TCP/HTTP server.
- Unix socket restricted to the user and peer-UID checked.
- Single-instance and repository locks contain PID/start-time identity to diagnose stale metadata; OS locks provide actual exclusion.
- Tauri commands expose only typed service operations, not a generic shell bridge to the frontend.
- User-controlled paths are canonicalized; mutations require descendant checks against recorded Tollgate-owned roots.
- Artifact/seed copy rejects escaping symlinks, devices, sockets, and unsafe ownership/mode cases.

### 20.3 Secrets and logs

Tollgate does not persist the captured shell environment or provide a vault. It does not claim automatic redaction of arbitrary command output. Diagnostics redact known path/user/remote fields and omit logs/environment values unless explicitly selected. Remote auth remains system Git's responsibility.

### 20.4 Network and telemetry

Network access occurs only for explicit/configured Git fetch/push and an optional user-enabled app update check. There is no telemetry, analytics, automatic crash report, or remote CI service dependency.

## 21. Validation strategy

### 21.1 Domain and property tests

The pure domain state machine must have exhaustive transition tests and property-based randomized sequences. Core properties include:

- no `promotion.completed` without a current valid certificate;
- promoted OID always equals certified tested OID;
- promoted commit parent always equals old release;
- queue order always topologically respects hard dependencies;
- any prefix change invalidates exactly the affected descendant buildsets;
- appending a tail item never invalidates an earlier item's validation generation;
- promoting a head preserves each ready descendant whose expected parent and validation generation remain exact;
- independent failure retains/rebuilds later independent items;
- dependency failure removes dependents;
- duplicate/stale command IDs are idempotent;
- pause prevents dispatch/promotion but preserves completed evidence;
- a pending configuration change prevents dispatch and promotion without mutating active buildset inputs;
- applying a new configuration digest invalidates every active result under the old digest that has not crossed local CAS, while preserving a frozen pending-push fact and its closed barrier;
- adaptive window stays within floor/ceiling and follows success/failure rules;
- repository pause/block, item validation, remote synchronization, cleanup, and buildset states remain orthogonal and accept only transitions listed in the domain transition table;
- local CAS with push enabled produces `promoted-local-push-pending`, cannot release the next promotion before exact remote observation, and never loses local success when push or cleanup needs attention;
- command UUID replay returns the stored result without repeating a Git/filesystem effect;
- configuration canonicalization is deterministic, rejects unknown fields/invalid graphs, and freezes exactly one runner/environment/matcher/applicable-voting-step contract.

A small serial reference gate should model “test one item, promote, repeat.” Random all-pass dependent-gate executions must produce the same final commit chain as the parallel scheduler.

### 21.2 Git integration tests

Use temporary real Git repositories and the supported system Git. Cover:

- A/B/C all pass and promote without descendant rerun;
- A fails; B/C rebuild without A;
- B fails after A promotes; C rebuilds on A;
- conflicts at every position;
- independent versus stacked hard dependencies;
- approval of B before and after source A lands as synthetic `S_A`, using the durable source-to-promoted mapping;
- failed, canceled, superseded, and unknown dependency OIDs;
- old-base sources and direct-parent OID reuse;
- author/committer/timestamps/message byte preservation;
- signed source behavior;
- rejection of unknown/malformed raw commit headers, stripping of recognized invalidated signatures, and byte-for-byte reuse when tree and parent are unchanged;
- checked-in golden transplant OIDs for the complete Git-semantics profile under SHA-1 and SHA-256;
- proof that internal plumbing cannot invoke repository hooks and that a user `pre-push` hook rejection becomes only a push failure;
- source branch amendment/supersession;
- checked-out `release` rejection while checked-out user-owned `master` remains allowed;
- external local fast-forward, rewind, deletion, and CAS race;
- external fast-forward containing an exact active synthetic tested OID, including provable adoption and ambiguous-ownership blocking;
- hidden-ref/object reachability and garbage collection;
- SHA-1 and SHA-256 repositories produce and promote correct native-format objects;
- submodule and LFS-compatible checkout cleanliness;
- source worktree/branch cleanup CAS and dirty safety.

### 21.3 Fault-injection tests

Inject process death or I/O error before/after every boundary in approval, tested-object retention, result completion, promotion, push, cleanup, seed publication, artifact retention, pruning, backup, and migration. Restart and assert one of three permitted outcomes: the exact operation is finalized once from matching external evidence; no external change occurred and it is safely retried/canceled; or mismatching/insufficient evidence blocks for attention. No recovery path infers success from an intent or path name alone.

Promotion tests must kill between SQLite intent commit, object transfer, Git lock prepare, ref commit, and SQLite completion. Push tests cover remote movement at every preflight/push point.

### 21.4 Runner and cache tests

- stdout/stderr ordering, binary/invalid UTF-8, backpressure, reconnect by offset, compression seekability;
- noninteractive stdin and no-PTY behavior;
- timeout excluding sleep, cancellation escalation, parent-crash worker kill;
- worker/app start-gate races, worker-first death, app-first death, lifetime-channel loss at every exit/output boundary, nonce/identity mismatch, PID reuse, unreaped process groups, and proof that no ambiguous terminal report becomes success;
- per-stream log offsets and broker observation ordering across deliberately interleaved stdout/stderr;
- background priority and process-tree RSS monitoring;
- tracked/index/HEAD/submodule dirty detection;
- ignored retention versus non-ignored cleanup;
- APFS clone identity, COW divergence, same-volume checks, safe-boundary snapshots;
- forced per-file clone success with no `cp`/physical fallback, hard-link preservation, safe relative symlinks, ACL/xattr/mode restoration, special-file rejection, and source mutation during enumeration;
- crash/error at every staging entry, manifest/fsync, rename, and parent-fsync boundary, proving that only completed immutable generations are consumable and writable cache modes survive seed round trips;
- new-slot seed choice, torn/corrupt seed fallback, cache epoch, quarantine, disk pressure;
- cold retry/reset/purge path-scope safety;
- shared-cache concurrency without sharing mutable build trees;
- prospective environment reload: running buildsets keep their snapshot, passed certificates remain valid, and retries use the new snapshot.

Cache tests must verify results on real APFS in Tahoe CI hardware; filesystem mocks cannot establish clone semantics.

### 21.5 Scheduler and performance tests

Simulate 50 repositories, 100-item queues, active-window changes, 8 slots, named semaphores, memory pressure, slot affinity, and maintenance work. Assert gate-head priority, weighted fairness, no partial token acquisition, per-step resource release, same-buildset semaphore contention, and no starvation. Simulate state, log/artifact, and cache roots on shared and separate volume identities; verify warning admission stops, critical writer interruption, emergency recovery allowance, and conservative promotion-space reservation independently per volume.

Benchmark state snapshots/history queries at one year of events, 10 MiB/s log ingest with slow/no frontend, queue rebuild after early-item failure, and UI virtualization at scale ceilings.

### 21.6 UI and end-to-end tests

Frontend tests use recorded typed service fixtures for every state and transition. End-to-end tests launch the real Tauri app, CLI, Git repositories, and shell commands on macOS Tahoe. Critical flows include init/bootstrap, approve/wait, A/B/C visualization without descendant reruns, tail append without earlier invalidation, failure/retry/reorder, local configuration pending/preview/apply/revert, prospective environment reload, pull/reconcile, push block/retry, explicit frozen-push abandonment followed by policy apply, app window close/reopen restoration, app crash recovery, cache reset, and automatic safe worktree cleanup.

Accessibility tests cover keyboard-only queue/log operations, VoiceOver labels, non-color state communication, and reduced motion.

### 21.7 Release acceptance criteria

v1 is not releasable until:

- all promotion fault points preserve I1;
- randomized gate-model tests find no divergent final history;
- no supervised child command survives app crash/quit beyond the bounded termination interval; deliberately daemonizing/session-escaping commands are diagnosed as unsupported;
- APFS cold/new-slot/warm-slot behavior is measured on Tahoe Apple Silicon;
- external release/remote movement never produces an automatic merge, rewrite, or unleased push;
- a 10 MiB/s log producer cannot block on a hidden or slow UI;
- every CLI queue mutation has an equivalent UI action and both call the same service handler;
- destructive filesystem tests prove operations remain inside recorded Tollgate-owned roots;
- every successful seed and slot-import path proves force-clone success for each regular file and never silently substitutes a physical copy;
- release binaries contain no fake validation-completion adapter and cannot manufacture a certificate without a fully completed real buildset.

## 22. Implementation roadmap

### Phase 1: domain and Git proof

Implement domain IDs/state machine, real-Git repository discovery, approval retention refs, dependency detection, execution mirror, deterministic synthetic chain, pass-certificate type, and local CAS promotion in a test harness. Exit criterion: A/B/C and all failure/conflict/external-movement Git scenarios pass with fault injection; no UI or real commands are required yet.

### Phase 2: durable app service and CLI

Add SQLite schema/events/intents/backups, repository actors, global service, UDS IPC, app lifecycle shell, `tg init/approve/queue/status/cancel/retry/promote/wait`, and crash recovery. A fake validation-completion adapter is compiled only into test binaries, accepts only repositories created beneath the test harness's freshly allocated temporary root, and cannot be selected by configuration or IPC. Non-test builds keep authoritative promotion disabled until a real runner-issued certificate is available. Exit criterion: restart-safe queue and promotion protocol through the CLI in temporary repositories, plus a build/link test proving release artifacts contain no fake completion adapter or bypass.

### Phase 3: runner, logs, slots, and caches

Add shell bootstrap, worker process groups, real generic steps/DAGs, timeouts/retries, tracked-clean verification, logs, independent checks, retained artifacts, resource scheduler, execution slots, APFS seeds, budgets, reset/purge, and bootstrap CI. Exit criterion: real Rust/Node/Python/Unity-shaped command fixtures demonstrate warm-slot/new-slot reuse and safe crash/cancel behavior.

### Phase 4: Tauri command center

Build React navigation/sidebar, repository queue/prefix visualization, slot/resource panels, step/log/artifact/history views, all shared-service operations, navigation restoration, storage/config/doctor surfaces, failure notifications, and accessibility. Exit criterion: every normal CLI mutation has a tested UI path, and log/UI scale targets pass.

### Phase 5: remote synchronization and gate-aware Git wrappers

Implement `tg pull`, leased `tg push`, push barriers, automatic remote fast-forward adoption when authoritative, divergence/reconcile UI, `tg update`, and worktree create/remove/automatic cleanup. Exit criterion: fault-injected local/remote races never violate exact promotion or overwrite an unexpected remote.

### Phase 6: release hardening

Complete Tahoe Apple-Silicon performance/soak testing, signed/notarized packaging, bundled CLI install/update, schema migration tests, diagnostics export, storage-pressure tests, documentation, and recovery drills. Exit criterion: all release acceptance criteria and scale goals pass on physical hardware.

## 23. Key risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Incremental cache poisoning | Build-tool invalidation is explicit trust assumption; cold retry, slot reset, cache epoch/purge, per-slot isolation, no automatic cold retry masking failures. |
| APFS clones consume unexpected physical space after mutation | Global budget and reserve, free-space monitoring, generational pruning, separate logical/physical estimates, cold refusal before reserve violation. |
| Shell startup hangs or produces a different environment | Shell-family helper protocol, short timeout, blocking diagnostic, explicit fallback/reload, recorded fingerprint. |
| CI process survives app crash | Ephemeral parent-watching supervisor, process groups, restart reconciliation, never accept worker-authored success. |
| Git/SQLite cross-system partial commit | Durable intents, old-OID ref CAS, idempotent recovery, exhaustive fault injection. |
| CI command mutates Git state | Disposable execution mirror, detached slot worktrees, final HEAD/index/tracked checks. |
| External local or remote movement wastes long CI | Ref monitoring plus mandatory transition checks, periodic fetch when remote authoritative, immediate affected-generation invalidation, leased push. |
| High-volume logs freeze app or block builds | File-first append, bounded broadcasts, offset resume, Tauri channels, seekable compression, virtualized UI. |
| A local configuration edit unexpectedly changes the gate | Pending-change block, explicit impact preview and activation, applicable voting-step validation, one canonical frozen digest, synchronous transition-time revalidation, and full unpromoted-result invalidation on apply. |
| SQLite WAL/version defects | Bundle a fixed current SQLite, use one writer/checkpointer, short readers, online backups, integrity checks. |
| Automatic cleanup deletes user work | Never primary worktree; exact OID and cleanliness verification; branch CAS; path ownership; needs-attention fallback. |

## 24. References

Normative product behavior is adapted from official Zuul documentation:

- [Zuul Project Gating](https://zuul-ci.org/docs/zuul/latest/gating.html): dependent speculative prefixes, failure removal/retesting, hard dependency behavior, and adaptive queue windows.
- [Zuul Pipeline](https://zuul-ci.org/docs/zuul/latest/config/pipeline.html): dependent versus independent managers, voting/failure behavior, dequeue semantics, and fail-fast configuration.
- [Zuul Job](https://zuul-ci.org/docs/zuul/latest/config/job.html): voting jobs, semaphores, timeouts, three setup attempts, and DAG dependencies.
- [Zuul Admin Client](https://zuul-ci.org/docs/zuul/latest/client.html): enqueue, dequeue, and queue promotion/reorder semantics.

Implementation primitives are based on primary platform documentation:

- [Git `update-ref`](https://git-scm.com/docs/git-update-ref): expected-old-OID ref updates and multi-ref transaction commands.
- [Git `worktree`](https://git-scm.com/docs/git-worktree): detached/locked worktrees and stable NUL-delimited porcelain.
- [Git `push`](https://git-scm.com/docs/git-push): explicit force-with-lease and atomic push behavior.
- [Apple File System Programming Guide](https://developer.apple.com/library/archive/documentation/FileManagement/Conceptual/APFS_Guide/Introduction/Introduction.html): APFS file/directory cloning and copy-on-write behavior.
- [Tauri v2: Calling Rust from the Frontend](https://v2.tauri.app/develop/calling-rust/): async commands and channels for ordered high-throughput streams.
- [Tauri v2 plugins](https://v2.tauri.app/plugin/): single-instance, notification, window-state, and updater support.
- [SQLite WAL](https://sqlite.org/wal.html), [transactions](https://www.sqlite.org/lang_transaction.html), and [online backup](https://sqlite.org/backup.html): durable state, reader/writer behavior, checkpointing, and safe backup.
- [Apple power-management assertion types](https://developer.apple.com/documentation/iokit/iopmlib_h/iopmassertiontypes): preventing idle system sleep while permitting display sleep/explicit sleep.
- [macOS Tahoe compatibility](https://support.apple.com/en-us/122867): deployment-platform context.
