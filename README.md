# Tollgate

Tollgate is a local dependent gate for macOS. It validates the exact prospective Git commits that would land on remote `master`, then promotes only a commit carrying a valid pass certificate through a Tollgate-owned local `release` branch.

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
restarts Tollgate, and verifies the service with `tg --no-launch doctor`. Set
`TOLLGATE_INSTALL_DIR` to use a different Applications directory.

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

Initialization leaves the current checkout unchanged apart from creating the local `.tollgate/config.toml` policy. Local `master` remains a normal user-owned branch that may track and push directly to `origin/master`; Tollgate creates and exclusively manages an un-checked-out local `release` branch at the same initial commit. After certification, Tollgate fast-forwards a clean, non-divergent local `master` and its checked-out files by default. When a clean checked-out `master` instead has a linear range of unsubmitted commits, Tollgate rebases that range onto the new certified `release` without submitting or authorizing it. A conflict, dirty checkout, merge commit, or concurrent movement leaves `master` untouched and records that synchronization needs attention. Set `sync_user_master = false` in `.tollgate/config.toml` to opt out. Certified pushes map local `release` to the configured remote branch, normally `master`.

A direct push from local `master` is deliberately outside Tollgate certification. Its exact remote lease prevents Tollgate from overwriting that movement; run `tg pull` to adopt the new remote tip into `release` before the next certified promotion.

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
status through a candidate-specific service read. Omitting the ID retains the
repository-wide snapshot used to inspect the current speculative queue and its
generation prefixes.

JSON `--wait` output is newline-delimited and compact: after the command result,
Tollgate emits an item wait-status record only when the item or repository block
state changes. Waiting never streams periodic repository or detailed buildset
snapshots; use `tg status <candidate-id>` for the full candidate evidence view.

`--wait` returns when validation has produced promotion-grade evidence (or a conclusive failure), while `release` remains unchanged. A user later grants authority to the exact retained candidate with `tg approve <candidate-id>`. Ordinary worktree candidates have no active source dependencies; only the explicit `push-master` workflow authorizes an ancestor closure from the user's submitted local commit chain. Granting authority lets the candidate or that explicit closure bypass unrelated candidates still awaiting authority, rebuilding only the affected suffix. Tollgate retains the admission order and all exact completed evidence: if independent later approvals close every authorization gap before promotion, it restores that order, cancels redundant bypass work, and reuses every certificate whose complete validation identity still matches. An explicit `tg reorder` replaces the retained admission order. `tg cancel <candidate-id>` cancels it. For the original one-phase user workflow, `tg approve HEAD` still submits and authorizes in one command.

If a concurrent approval already granted authority to that candidate as an
active dependency, repeating `tg approve <candidate-id>` succeeds without
changing the queue revision; `--wait` then follows the already-authorized item.

Tollgate may combine independent candidates in its disposable validation slots when their patches merge cleanly. Those synthesized prefixes are internal execution artifacts, never source-branch bases. If synthesis conflicts with an earlier candidate, keep the task commit based on promoted `release` and retry after the earlier candidate is promoted, canceled, or reordered. If `release` itself advanced incompatibly, rebase only onto the latest `release`, resolve and regenerate, then resubmit:

```sh
tg --no-launch --json status
git rebase release
# resolve the reported paths, regenerate derived files, and continue the rebase
tg --no-launch --json status
tg --no-launch --json candidate HEAD
```

Ordinary `candidate` and `approve` submissions reject commits containing unpromoted source ancestry and return the promoted `release_oid` as the only supported rebase target. The explicit `push-master` workflow is the exception: it preserves the user's already-authored linear local commit chain and records dependencies between those commits while submitting them oldest-first.

To submit every clean, linear commit on local `master` after the certified
`release` tip and automatically push the resulting certified chain, run:

```sh
tg push-master
```

The command first rebases a clean, stale local `master` range onto the current
certified `release` when necessary, authorizes the commits oldest-first, and
returns after scheduling. As certified history advances, Tollgate projects the
latest speculative tested chain back onto an unchanged, clean local `master`,
placing the newly certified commits beneath the submitted commits without a
temporary divergence. New commits or working-tree changes prevent automatic
projection and are left untouched. Use `tg push-master --wait` when a foreground
result is useful. `tg push-master --status` reports the latest master push,
including its failed validation step and the exact log command to inspect.
The Queue screen retains the latest failed master push as an action-required
entry after it leaves the active queue. Remote pushing must be enabled for the
repository. Bare `tg push` retains its narrower recovery meaning: retrying a
push of commits that Tollgate has already certified.

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
{"code":"generated-output-drift","message":"Generated reports are stale","paths":["reports/current.csv"],"repair":{"kind":"argv","argv":["tool","generate"]}}
```

Tollgate bounds and validates this JSONL channel; it does not infer repairs by
scraping logs. `tg diagnose <candidate-id> --verify-repair` explicitly runs one
unambiguous structured repair in a fresh clone, reruns every applicable voting
step, and retains a binary patch only if they pass. The original source and
candidate remain immutable: the patch must be reviewed and submitted as a new
candidate.

## Safety model

- CI runs in detached worktrees belonging to a disposable execution mirror.
- Candidate submission retains the immutable source under `refs/tollgate/sources/` and each speculative tested generation under `refs/tollgate/speculative/`; promotion authority is recorded separately.
- Promotion retains and re-verifies the tested object, then uses an expected-old-OID `git update-ref` compare-and-swap.
- SQLite runs WAL + foreign keys + `synchronous=FULL` and uses durable external-operation intents.
- Output is appended to disk before live delivery; a hidden or slow UI cannot block only the UI path.
- APFS seed creation uses `clonefile`, never a copy command that can silently fall back to physical copying.
- The Unix socket is mode `0600`, its parent is `0700`, and both sides verify the effective peer UID.

The complete normative behavior is in [docs/technical-design.md](docs/technical-design.md).
