# Tollgate

Tollgate is a local dependent gate for macOS. It validates the exact prospective Git commits that would land on remote `master`, then promotes only a commit carrying a valid pass certificate to a Tollgate-owned local `staging` branch. A Tollgate-owned local `release` branch follows `staging`, either together with it or, when the repository configures a release stage, only after a slower release run passes on the exact commit; only `release` is pushed to the remote.

This repository contains the Rust domain, Git adapter, SQLite store, runner, scheduler, service and IPC protocol; the `tg` CLI and ephemeral worker; and a Tauri v2 + React command center.

## Development

Prerequisites are an Apple Silicon Mac, current system Git, Rust stable, and Node.js 20 or newer.

```sh
cargo test --workspace
npm --prefix ui install
npm --prefix ui test
npm --prefix ui run build
scripts/prepare-sidecar.sh debug
cargo run -p tollgate-app
```

The Tauri CLI is pinned as a development dependency. Produce the app with `npm --prefix ui run bundle`, or the app + DMG artifact with `npm --prefix ui run bundle:dmg`. Local builds receive an ad-hoc resource seal. Release builds set `APPLE_SIGNING_IDENTITY` to a Developer ID Application identity and `TOLLGATE_NOTARY_PROFILE` to a `notarytool` keychain profile; the packaging command then submits, staples, and validates both the app and final DMG.

The browser development view (`npm --prefix ui run dev`) uses a representative typed fixture. A native Tauri build always calls the Rust service.

### Install a local checkout

After updating the checkout, build and install the app, bundled CLI, and worker with:

```sh
git pull --ff-only
./scripts/install-local.sh
```

The script installs to `/Applications/Tollgate.app`, updates `~/.local/bin/tg`,
and restarts Tollgate. The app binds its socket before activating repositories, so
the script treats a bound socket plus a passing `tg --no-launch doctor` as healthy.
It then prints each repository's activation progress (`tg repo activation --wait`)
until every repository is active or has failed; a failed repository is reported
with its error and recovery action. Set `TOLLGATE_INSTALL_DIR` to use a different
Applications directory and `TOLLGATE_ACTIVATION_TIMEOUT` (seconds, default 900) to
bound the progress wait.

Artifact patterns may contain `{{buildset_id}}`, resolved from Tollgate's execution
identity rather than the shell environment. For example,
`patterns = ["artifacts/{{buildset_id}}/report.json"]` collects only that buildset's
report. The step can write it using the read-only `TOLLGATE_BUILDSET_ID` variable;
old files from other executions cannot satisfy a required artifact with this pattern.

## First repository

Launch the app and choose **Add repository**, or run:

```sh
tg init --run './ci'
```

Tollgate writes its trusted policy to `<repository-root>/.tollgate/config.toml`. The smallest valid file is:

```toml
version = 1

[[step]]
name = "ci"
run = "./ci"
```

Initialization leaves the current checkout unchanged apart from creating the local `.tollgate/config.toml` policy. Local `master` remains a normal user-owned branch that may track and push directly to `origin/master`; Tollgate creates and exclusively manages two un-checked-out local branches at the same initial commit: `staging`, which every promotion advances and new work bases on, and `release`, which the remote push publishes. Without a release stage the two always hold the same commit.

After certification, Tollgate fast-forwards a clean, non-divergent local `master` and its checked-out files by default. `sync_user_master` in `.tollgate/config.toml` names the branch local `master` follows: `"staging"` (the default; `true` is an alias), `"release"`, or `false` to opt out. When a clean checked-out `master` instead has a linear range of unsubmitted commits, Tollgate rebases that range onto the newly certified commit without submitting or authorizing it. A conflict, dirty checkout, merge commit, or concurrent movement leaves `master` untouched and records that synchronization needs attention. Certified pushes map local `release` to the configured remote branch, normally `master`.

A direct push from local `master` is deliberately outside Tollgate certification. Its exact remote lease prevents Tollgate from overwriting that movement; run `tg pull` to adopt the new remote tip into `staging` and `release` before the next certified push.

An agent can submit a clean commit for speculative validation without permission to promote it:

```sh
tg candidate HEAD --wait
tg status <candidate-id>
```

Long-lived stacked workflows can capture an intermediate candidate with
`tg candidate --retain-worktree HEAD`. The retained cleanup policy is immutable candidate
metadata: authorization and retry preserve it, promotion leaves its source worktree and branch
available, and JSON status reports `"cleanup_policy": "retain-worktree"`.

In JSON mode, `tg status <candidate-id>` returns only that candidate's detailed
status through a candidate-specific service read. It also reports `local_master.status`, `policy_enabled`, `contains_tested`, and the observed local-master OID. `needs-attention` means local master lacks the tested integration commit even if remote push succeeded; `disabled` reflects an explicit sync opt-out. Use candidate status after a blocking wait to verify this independent outcome. Omitting the ID retains the
repository-wide snapshot used to inspect the current speculative queue and its
generation prefixes.

JSON `--wait` output is newline-delimited and compact: after the command result,
Tollgate emits an item wait-status record only when the item or repository block
state changes. Waiting never streams periodic repository or detailed buildset
snapshots; use `tg status <candidate-id>` for the full candidate evidence view.

`--wait` returns when validation has produced promotion-grade evidence (or a conclusive failure), while `staging` and `release` remain unchanged. A user later grants authority to the exact retained candidate with `tg approve <candidate-id>`. Ordinary worktree candidates have no active source dependencies; only the explicit `push-master` workflow authorizes an ancestor closure from the user's submitted local commit chain. Granting authority lets the candidate or that explicit closure bypass unrelated candidates still awaiting authority, rebuilding only the affected suffix. Authorization time establishes the relative order of independently authorized candidates, including approvals serialized from the same queue revision; exact completed evidence is reused only when its complete validation identity still matches that order. An explicit `tg reorder` replaces the retained admission order. `tg cancel <candidate-id>` cancels an active item; if the item has already become terminal, cancellation is a successful audited no-op that reports its retained terminal state and reason. For the original one-phase user workflow, `tg approve HEAD` still submits and authorizes in one command.

If a concurrent approval already granted authority to that candidate as an
active dependency, repeating `tg approve <candidate-id>` succeeds without
changing the queue revision; `--wait` then follows the already-authorized item.

Tollgate combines independent candidates in its disposable validation slots when their patches merge cleanly. When a new candidate conflicts with an earlier speculative item, Tollgate retains it in a separate `staging`-anchored validation lane. Authorizing either contender may move it to the promotable head; after it promotes, incompatible contenders become merge-conflicted against the new `staging`. These synthesized prefixes and lanes are internal execution artifacts, never source-branch bases. If promoted `staging` itself advanced incompatibly, rebase only onto the latest `staging`, resolve and regenerate, then resubmit:

Candidate submission is durable even when Tollgate cannot construct a prospective tested commit. A conflict or empty application against promoted `staging` creates a terminal `merge-conflict` candidate with no validation generation or tested OID; it never turns the submission command into a candidate rejection. `tg candidate` returns the retained candidate immediately and reports the terminal state and reason when construction already failed, while `tg candidate --wait` returns the conclusive validation exit code. Attempting to authorize a candidate that became terminal reports that retained state and reason without granting promotion authority.

```sh
tg --no-launch --json status
git rebase staging
# resolve the reported paths, regenerate derived files, and continue the rebase
tg --no-launch --json status
tg --no-launch --json candidate HEAD
```

Ordinary `candidate` and `approve` submissions reject commits containing unpromoted source ancestry and return the promoted `staging` OID (the structured error's `staging_oid` detail) as the only supported rebase target. The explicit `push-master` workflow is the exception: it preserves the user's already-authored linear local commit chain and records dependencies between those commits while submitting them oldest-first.

To submit every clean, linear commit on local `master` after the certified
`staging` tip and have Tollgate push the resulting certified chain, run:

```sh
tg push-master
```

The command first rebases a clean, stale local `master` range onto the current
certified `staging` when necessary, authorizes the commits oldest-first, and
returns after scheduling. As certified history advances, Tollgate projects the
latest speculative tested chain back onto an unchanged, clean local `master`,
placing the newly certified commits beneath the submitted commits without a
temporary divergence. New commits or working-tree changes prevent automatic
projection and are left untouched. Use `tg push-master --wait` when a foreground
result is useful. `tg push-master --status` reports the latest master push,
including its failed validation step and the exact log command to inspect.
The Queue screen retains the latest failed master push as an action-required
entry after it leaves the active queue. Remote pushing must be enabled for the
repository. With a release stage, the chain reaches the remote when a passing
release run advances `release` past it. Bare `tg push` retains its narrower
recovery meaning: retrying a push of commits that Tollgate has already certified.

## Staged release

A slow suite need not delay every promotion. Steps with `stage = "release"`
form a release stage that runs after promotion instead of before it:

```toml
version = 1

[resources]
max_release_lag = 5   # optional; 0 (the default) never pauses promotion

[[step]]
name = "fast"
run = "./ci fast"

[[step]]
name = "full"
stage = "release"
run = "./ci full"
```

- Gate-stage steps (the default) certify each promotion to `staging`, so work
  lands as soon as the fast checks pass. A release step may `needs` gate steps,
  which rerun inside the release run; a gate step may not need a release step.
- After each promotion Tollgate queues a release run on the newest `staging`
  tip. Promotions arriving while a run is in progress coalesce into one queued
  run for the newest tip; a started run is never canceled for a newer tip.
- A passing release run advances `release` to exactly the commit it tested
  and, with pushing enabled, pushes that commit with an exact lease. The remote
  only ever receives commits that passed both stages.
- A failing release run moves nothing and notifies once per failing streak.
  `staging` keeps accepting work; land the fix as an ordinary candidate, and
  the next passing release run advances `release` past the broken commit.
- A failed or diverged release push, a remote outside certified history, or an
  unreleased commit Tollgate neither promoted nor adopted holds back `release`
  only; depending on the hold, `tg push`, `tg pull`, or `tg reconcile` clears it.
- With a positive `max_release_lag`, promotion pauses while `release` trails
  `staging` by more than that many commits after a failed release run. Gate
  validation continues, and `tg approve --release-fix <candidate-id>` moves the
  fix ahead of other candidates and past the pause.

Inspect and drive the release stage with:

```sh
tg status                     # both refs, the lag, the release state, and any holds
tg release status             # recent release runs with targets, ranges, and failing steps
tg release retry --wait       # rerun the newest staging tip, for a flaky release step
tg wait --released <candidate-id>   # wait until release contains the promoted candidate
```

`tg wait --released` exits `0` once `release` contains the target and `1` when
the release run covering it fails. The desktop app's Release view shows the
same refs, lag, holds, and runs, and offers **Retry release run**. Its Remote
panel runs **Pull** and **Push** when the repository pushes to a remote, and
**Reconcile** in every repository. Reconcile first previews the observed
`staging` it adopts, the blocks and holds it clears, and the remote pushes it
abandons, and it applies only if the queue revision and observed `staging` still
match that preview.

## Diagnosing CI failures

`tg status <candidate-id>` attributes each failed voting step when comparable
evidence already exists. Tollgate reports `candidate-introduced`,
`inherited-from-base`, `flaky-or-non-hermetic`, or `origin-unknown`; a comparison
is valid only when the frozen configuration, step graph, engine epoch, and tool
environment match.

Run `tg diagnose <candidate-id>` to attribute the failure immediately from
retained evidence with the same tested OID, configuration digest, step-graph
digest, engine epoch, and environment fingerprint. This is the default and does
not schedule queue work. Add `--replay` when ambiguity or suspected flakiness
justifies another experiment. Tollgate reuses matching retained or in-flight
checks, runs one candidate stability probe, and checks the exact anchored base
only when comparable base evidence is missing.

A step may publish structured diagnostics by writing one JSON object per line
to the read-only `TOLLGATE_DIAGNOSTICS_FILE` environment variable:

```json
{"code":"generated-output-drift","message":"Generated reports are stale","failure_kind":"validation","paths":["reports/current.csv"],"repair":{"kind":"argv","argv":["tool","generate"]}}
```

The optional `failure_kind` is `validation` or `infrastructure` and remains
separate from the base-versus-candidate origin comparison. Tollgate bounds and
validates this JSONL channel; it does not infer repairs by
scraping logs. `tg diagnose <candidate-id> --verify-repair` explicitly runs one
unambiguous structured repair in a fresh clone, reruns every applicable voting
step, and retains a binary patch only if they pass. The original source and
candidate remain immutable: the patch must be reviewed and submitted as a new
candidate.

## Safety model

- CI runs in detached worktrees belonging to a disposable execution mirror.
- Candidate submission retains the immutable source under `refs/tollgate/sources/` and each speculative tested generation under `refs/tollgate/speculative/`; promotion authority is recorded separately.
- Promotion retains and re-verifies the tested object, then uses an expected-old-OID `git update-ref` compare-and-swap.
- Only `release` is pushed, with an exact lease. With a release stage, `release` advances only to the exact commit a passing release run tested.
- SQLite runs WAL + foreign keys + `synchronous=FULL` and uses durable external-operation intents.
- Output is appended to disk before live delivery; a hidden or slow UI cannot block only the UI path.
- APFS seed creation uses `clonefile`, never a copy command that can silently fall back to physical copying.
- The Unix socket is mode `0600`, its parent is `0700`, and both sides verify the effective peer UID.

The complete normative behavior is in [docs/technical-design.md](docs/technical-design.md).
