# Staged Release: Fast Gate, Asynchronous Release Validation

Status: proposed. Extends [technical-design.md](technical-design.md); where
the two disagree for a repository that opts in, this page wins.

## 1. Summary

Today one step graph does two jobs. It gates every promotion, and it is the
only evidence before the remote push. So the slowest check decides how fast
anything lands.

This design splits validation into two **stages** with two Tollgate-owned
refs:

| Ref | Advances when | Read by |
| --- | --- | --- |
| `staging` | a candidate passes the **gate** stage (fast) | `tg worktree create`, `tg update`, the gated queue, user-owned local `master` sync |
| `release` | a `staging` commit passes the **release** stage (slow) | the remote push; nothing else moves remote `master` |

- **Work lands on `staging` fast.** The gate stage holds only cheap, broad
  checks.
- **The slow suite runs after promotion,** on the newest `staging` tip, off
  every agent's critical path.
- **The remote only ever sees fully validated tips.** `release` and remote
  `master` advance to an exact OID only after the release stage passes on it.
- **A release failure never rolls anything back.** `staging` keeps moving; the
  fix lands as an ordinary follow-up commit, and the next green release run
  advances `release` past the broken commit.

A repository with no release-stage steps behaves exactly as today: `staging`
and `release` move together in one ref transaction.

## 2. Motivation

The `dreamtides_web` unattended run measured it:

- The gate (`npm ci`, then a full lint, typecheck and test suite) took 224 s
  at low host load and up to 656 s under load.
- Every bead waited for it before the next could start, about a third of each
  bead's wall time.
- Only 1 of 19 gates failed. Nearly all of that waiting bought nothing.

The tests that catch the most real breakage (typecheck, lint of changed
files, tests related to the diff) run in well under a minute. The rest is
worth running on every commit, but it is not worth blocking on.

### Existing mechanisms that do not fit

- **`voting = false` steps** still run before promotion, hold the repository
  permit and slot until they finish, and record failure only as a
  certificate warning with no notification (`runner` `run_buildset_scheduled`;
  `service` `execute_item`).
- **`tg check release`** runs the same step graph as the gate, so it cannot
  be "the slow half". It also needs a clean invoking worktree and has no
  trigger, coalescing, or ref effect.

## 3. Goals and non-goals

Goals:

- Promotion to `staging` costs only the gate stage.
- Remote `master` never shows a commit that has not passed both stages at
  that exact OID.
- The release stage keeps up with any promotion rate by testing the newest
  tip, never every commit.
- Release failures are durable, attributed, queryable, and notified.
- Opt-in per repository, with no behavior change for repositories that do
  not opt in.

Non-goals:

- **Automatic revert** of a red `staging` commit.
- **Bisecting** a red release run. Attribution names the failing steps and
  the tested range; the range is usually one or two commits.
- **More than two stages.** The model generalizes (one ref per stage), but
  v1 ships two.
- **Per-commit release evidence.** `release` may skip over a red commit once
  a later tip is green; remote history can contain that red commit.

## 4. Terms

| Term | Meaning |
| --- | --- |
| Gate stage | Steps with `stage = "gate"` (the default). They certify a promotion to `staging`. |
| Release stage | Steps with `stage = "release"`. They certify a `release` advance. |
| `staging` | The Tollgate-owned integration branch. Every promotion CASes it. Feature worktrees base on it. |
| `release` | The Tollgate-owned released branch. It is always an ancestor of, or equal to, `staging`. It maps to the configured remote branch. |
| Release run | A buildset of release-stage steps on one exact `staging` OID. Never promotes `staging`. |
| Release certificate | Evidence tying one tested `staging` OID to all applicable release-stage voting steps passing. |
| Release lag | Commits and wall time by which `release` trails `staging`. |

## 5. Invariants

The existing invariants hold with `staging` in place of `release`, except
I8, which moves to `release`. New invariants:

- **R1. Ancestry.** `release` is always `staging` or a first-parent ancestor
  of it. Both are linear.
- **R2. Exact release.** For every `release` advance:
  - the new OID equals a release certificate's tested OID;
  - every applicable release-stage voting step passed on that OID, with a
    clean checkout afterwards;
  - the new OID is a descendant of the old `release` and an ancestor of, or
    equal to, current `staging`;
  - every commit in `old..new` was promoted to `staging` by Tollgate or
    adopted under 10.5;
  - the certificate's frozen release-stage digest equals the active
    configuration's.
- **R3. No rollback.** A release failure never moves `staging` or `release`
  backwards and never cancels gate work.
- **R4. Remote follows `release` only.** Remote push uses only `release` OIDs,
  with an exact lease on the previous pushed `release`. I8 applies to that
  push.
- **R5. Opt-out equivalence.** With no release-stage steps, every promotion
  updates `staging` and `release` to the same OID in one `update-ref --stdin`
  transaction, and the push follows as in technical-design 10.4.

## 6. Configuration

New and changed keys:

```toml
version = 1
# "staging" (default), "release", or false. Which Tollgate ref user-owned
# local master follows.
sync_user_master = "staging"

[resources]
repository_concurrency = 1   # gate and independent-check buildsets
release_concurrency = 1      # release runs; a separate permit
max_release_lag = 0          # 0 = unlimited; else pause promotion past N unreleased commits

[[step]]
name = "fast"
run = "npm run review:gate"

[[step]]
name = "full"
stage = "release"
run = "npm run review:full"
```

- **`stage`** on `[[step]]`: `"gate"` (default) or `"release"`.
  - `validate_graph` rejects a gate step that `needs` or `soft_needs` a
    release step. Release steps may need gate steps; those run again inside
    the release run (they are cheap, and a release run is self-contained).
  - The "at least one voting step" rule applies to the gate stage.
- **`sync_user_master`** accepts `true` (an alias for `"staging"`), `false`,
  `"staging"`, or `"release"`.
- **`release_concurrency`** is a per-repository permit, separate from
  `repository_concurrency`. A release run still takes one global buildset
  permit and one slot.
- **`max_release_lag`** is the safety valve for unattended runs. When
  `release` trails `staging` by more than N commits and the latest release
  run failed, new promotions wait (`blocked: release-lag`). Gate validation
  continues.
- **Digests.** Gate generations freeze a gate-stage graph digest. Editing
  release-stage steps therefore does not invalidate the queue. Release runs
  freeze a release-stage digest.

Update `schemas/config-v1.schema.json`, `StepFile`, `EffectiveStep`,
`FrozenStep`, `freeze_steps`, and `tg config explain` (which should print the
stage).

## 7. Refs and migration

- **Registration** creates both `staging` and `release` at local `master`'s
  OID.
- **Upgrade of an existing repository** (startup reconciliation, under the
  repository lock):
  1. Create `refs/heads/staging` at the current `release` OID with an
     expected-nonexistent CAS.
  2. Re-anchor active generations to `staging`. The OID is unchanged, so no
     generation is invalidated.
  3. Record a `refs.staged-release-migrated` event.
- **`INTEGRATION_REF`** becomes `refs/heads/staging`. A new `RELEASE_REF` is
  `refs/heads/release`. About 70 call sites read `INTEGRATION_REF`; each is
  classified as a gate concern (`staging`) or a publish concern (`release`).
  Most are gate concerns.
- **Checkout protection** extends to both refs: neither may be checked out in
  any authoritative worktree.
- **JSON.** Repository state replaces `integration_ref` with `staging_ref`
  and `staging_oid`, and adds `release_ref`, `release_oid`, `release_lag`,
  and `release_state`. The JSON contract version is bumped.

## 8. Release runs

### 8.1 Item kind

Add `QueueItemKind::Release`. It is a separate kind, not a `purpose` string,
so retry, attribution, notification, and UI handle it explicitly. (Check
retry currently hard-codes purpose `"check"`, which would lose a string
label.)

A release run:

- tests one existing `staging` OID directly. No synthetic prefix is built;
  the expected parent is the commit's own parent;
- runs only release-stage steps (the runner filters `applicable` by stage);
- never enters the dependent gate queue, never counts against
  `active_window`, and never promotes `staging`.

### 8.2 Triggering and coalescing

Per repository, keep at most one **running** and one **queued** release run.

- **After each `staging` promotion,** if release-stage steps exist and the
  new tip is not already certified, ensure a queued release run targets the
  new tip.
  - If one is queued, retarget it to the new tip (record `superseded` for
    the old target).
  - If one is running, leave it running. Never cancel a running release run
    for a newer tip; otherwise sustained promotion would starve `release`.
- **On startup,** and after `tg pull` or reconciliation, if
  `release != staging` and no run is active, queue one for the tip.
- **The trigger runs outside `promote_ready`'s mutation lock.**
  `promote_ready` holds `runtime.mutation`, and the check enqueue path takes
  the same lock. Record a durable `release.run-requested` intent inside the
  promotion transaction, and let a separate task, spawned after the lock is
  released, enqueue it. Recovery replays outstanding intents.
- **Clean-worktree checks do not apply.** The run is created from a ref, not
  a user worktree. Use a probe like `probe_retained_check` on a warm slot,
  not `CheckMode::RetainedCold`.

### 8.3 Path filters across a range

Step `include` and `exclude` filters evaluate the union of changed paths in
`release..tested` (first-parent), not just the tested commit's own diff.
Otherwise a batched run under-selects steps.

### 8.4 Scheduling

- Priority sits between speculative gate descendants and independent checks.
- A release run holds a `release_concurrency` permit, never a
  `repository_concurrency` permit, so it cannot delay gate admission beyond
  the global buildset and slot limits.
- It uses normal slot affinity and seeds.

### 8.5 Outcomes

- **Pass:** write a release certificate, then attempt a `release` advance
  (section 9).
- **Fail:** record `release_state = failing` with the tested OID, range,
  run ID, and voting-step attribution. Notify once per failing streak (and
  once on recovery). Do not retry automatically. The next promotion triggers
  a new run on the new tip.
- **Infrastructure interruption:** rerun the same target under the normal
  12.4 rules.
- **`tg release retry`** reruns the latest target, for flaky tests. It keeps
  the `Release` kind.

## 9. Advancing `release` and pushing

This replaces the push half of technical-design 10.4 for opted-in
repositories.

1. **Preconditions.** All of R2, plus:
   - the repository is not blocked;
   - no push intent is pending;
   - the release-stage digest of the on-disk configuration equals the
     certificate's.
2. **Remote preflight** (push enabled). Fetch into the remote-observation ref
   and require it to equal the last pushed `release` OID.
3. **Release intent.** In one durable transaction, record
   `release_intent(expected_old, new, certificate_id)` and the push intent.
4. **Local CAS** of `release` from `expected_old` to `new`.
5. **Leased push** of `new` to the remote branch, then exact observation, as
   10.4 steps 3–4.
6. **Complete.** Mark the intents complete and set `release_state = green`
   (or leave `failing` if a newer run already failed).

Recovery follows the 10.2 pattern for `release`:

- `release == new`: finalize.
- `release == expected_old`: retry if preconditions still hold, else cancel.
- anything else: external-movement reconciliation.

A failed or divergent push sets `push-blocked`. That blocks further `release`
advances only. `staging` promotions continue, because the remote barrier
moves from promotion to release advance.

**Network unavailability** no longer blocks local promotion at all. It delays
`release` advance only.

## 10. Changes to existing flows

- **Promotion** (technical-design 10.1–10.3) CASes `staging`. It drops the
  remote preflight and push barrier for opted-in repositories, and records
  the `release.run-requested` intent.
- **Source worktree cleanup** waits for `staging` promotion, not for the push.
- **User `master` sync** follows `sync_user_master`: by default `staging`,
  so the developer's local `master` is ahead of the remote. The safety rules
  in 6.2 are unchanged.
- **`tg worktree create` and `tg update`** base on `staging`.
- **`tg approve --wait` and `tg wait`** return at `staging` promotion. Add
  `tg wait --released <candidate-id|oid>`, which returns when `release`
  contains that OID or the latest release run covering it fails.
- **`tg pull`.**
  - If remote `master` is a strict fast-forward of `staging`, CAS both
    `staging` and `release` to it and rebuild.
  - If it is a fast-forward of `release` but not of `staging`, block for
    `tg reconcile`.
  - Equal or ahead: no inbound update.
- **`tg push`** pushes the contiguous certified `release` chain only.
- **`tg reconcile`** presents `staging`, `release`, the last pushed OID,
  remote `master`, pending release and push intents, and the active release
  run.
- **External movement** (10.5) applies to both refs:
  - An external fast-forward of `staging` is handled as today.
  - An external move of `release` is accepted only as a fast-forward that
    stays an ancestor of `staging`. It is recorded as uncertified adoption,
    and the next release run must still pass before the next push.
  - Anything else blocks.
- **`tg push-master`** is unchanged in shape. Its closure lands on `staging`.

## 11. Status, CLI, and UI

- **`tg status`** prints `staging`, `release`, lag (commits and age), and the
  release state with the active run.
- **`tg release status`** prints the latest release runs with target, range,
  state, and failing steps.
- **`tg release retry`** is described in 8.5.
- **Desktop UI.** The repository header shows both refs and lag. A Release
  panel lists runs and opens them in the existing item inspector.
- **Notifications:**
  - release run failed (once per streak);
  - release recovered;
  - push blocked;
  - promotion paused by `max_release_lag`.
- **`failure_attribution`** works for release runs because release-stage
  steps vote within their own run.

## 12. Validation strategy

- **Domain and property tests:**
  - R1–R5 across random interleavings of promotions, release passes and
    failures, pushes, crashes, and pulls;
  - coalescing keeps at most one queued and one running release run;
  - `release` never skips past an uncertified tip.
- **Opt-out equivalence:** the existing promotion and push test suite runs
  unchanged against a configuration with no release steps. `staging ==
  release` after every event.
- **Fault injection** before and after every durable boundary of the release
  intent, CAS, push, and observation.
- **Migration:** an existing repository with an active queue upgrades with
  no generation invalidated, `staging` created at `release`, and the
  queue promoting normally afterwards.
- **Range path filters:** a batched run over commits touching disjoint paths
  selects the union.
- **Lock discipline:** a test that promotes under load and asserts the
  release trigger never runs inside the mutation lock.
- **End to end:** register a fixture repository with a fast gate step and a
  slow release step, land three candidates in quick succession, and observe:
  - `staging` advances three times;
  - a single coalesced release run advances `release` to the newest tested
    tip;
  - the remote receives only that OID.

## 13. Implementation plan

Each item is one reviewable commit through the `wt` flow, with self-install
after promotion per AGENTS.md.

1. **Config.** `stage`, `release_concurrency`, `max_release_lag`, the
   `sync_user_master` values, per-stage digests, schema, and `config
   explain`. No behavior change yet: release-stage steps are rejected by
   validation until item 3 lands.
2. **Refs.** `staging` creation and migration, the `INTEGRATION_REF` split,
   both refs checkout-protected, JSON fields, and the opt-out two-ref
   transaction (R5). Behavior is identical for every existing repository.
3. **Release runs.** `QueueItemKind::Release`, the runner's stage filter,
   range path filters, the permit and priority, the trigger intent and
   coalescing, and outcomes. `release` does not move yet.
4. **Release advance and push.** Release intent, CAS, the relocated
   preflight and push barrier, recovery, `max_release_lag`, and the
   `pull`/`push`/`reconcile`/external-movement changes.
5. **Surfaces.** `tg release`, `tg wait --released`, status output,
   notifications, and the UI panel.
6. **Docs.** Fold this page into `technical-design.md` and the README, then
   delete this page.

## 14. Downstream changes (outside this repository)

These land after item 4, in their own repositories and skills:

- **`wt` and `wt-sequence` skills:**
  - base worktrees on `staging`;
  - a task is complete when its candidate is promoted to `staging`; report
    `release` lag and state;
  - `tg wait --released` is required only when the user asks for the result
    to be live;
  - a red release run is repaired by a follow-up commit, not an amend;
  - the "local `master` is never synchronized" text changes to match
    `sync_user_master`.
- **`executor`, `independent-review`, `weaver`, `archivist`,
  `visual-review`:** audit each `release` mention. Most mean "base new work
  here" and become `staging`.
- **Hive.** Audit `executor_hook.py`, `cli.py`, `launch_context.py`,
  `resource_records.py`, `dashboard_transport.py`, and
  `hive_bootstrap/launcher.py` for ref assumptions. Most `release` mentions
  there are about releasing bead assignments, not the ref.
- **Tollgate AGENTS.md** keeps building self-installs from the latest
  `release` OID. That now means fully validated, which is the right input.
- **`dreamtides_web`:**
  - split its local policy into a gate stage under 60 s (dependencies,
    prepare, full typecheck, changed-file lint, capped related tests) and a
    release stage (`review:full`);
  - a bead closes when its commit is on `staging`;
  - a red release run files a CI-fix bead that runs next;
  - phase gates require `release == staging`.

## 15. Decisions for review

Defaults chosen in this design that the operator may overturn:

1. **Ref names:** `staging` (fast) and `release` (fully validated).
2. **`sync_user_master` default:** `"staging"`. The developer's local
   `master` is ahead of the remote.
3. **Release steps are self-contained:** gate steps a release step `needs`
   rerun inside the release run, rather than reusing gate results.
4. **A running release run is never cancelled** for a newer tip; a queued one
   is retargeted.
5. **No automatic retry** of a failed release run; the next promotion or
   `tg release retry` starts the next one.
6. **`max_release_lag = 0`** (unlimited) by default. For an unattended run,
   set it so a long red streak eventually pauses promotion.
7. **Opt-out repositories still get a `staging` ref,** so every skill can
   base on `staging` uniformly.
