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

## First repository

Launch the app and choose **Add repository**, or run:

```sh
tg init --run './ci'
```

Tollgate writes its trusted policy to `<git-common-dir>/tollgate/config.toml`. The smallest valid file is:

```toml
version = 1

[[step]]
name = "ci"
run = "./ci"
```

Initialization leaves the current checkout unchanged. Local `master` remains a normal user-owned branch that may track and push directly to `origin/master`; Tollgate creates and exclusively manages an un-checked-out local `release` branch at the same initial commit. After certification, Tollgate fast-forwards a clean, non-divergent local `master` and its checked-out files by default. Set `sync_user_master = false` in `tollgate/config.toml` to opt out. Certified pushes map local `release` to the configured remote branch, normally `master`.

A direct push from local `master` is deliberately outside Tollgate certification. Its exact remote lease prevents Tollgate from overwriting that movement; run `tg pull` to adopt the new remote tip into `release` before the next certified promotion.

An agent can submit a clean commit for speculative validation without permission to promote it:

```sh
tg candidate HEAD --wait
tg status <candidate-id>
```

In JSON mode, `tg status <candidate-id>` returns only that candidate's detailed
status through a candidate-specific service read. Omitting the ID retains the
repository-wide snapshot used to inspect the current speculative queue and its
generation prefixes.

JSON `--wait` output is newline-delimited and compact: after the command result,
Tollgate emits an item wait-status record only when the item or repository block
state changes. Waiting never streams periodic repository or detailed buildset
snapshots; use `tg status <candidate-id>` for the full candidate evidence view.

`--wait` returns when validation has produced promotion-grade evidence (or a conclusive failure), while `release` remains unchanged. A user later grants authority to the exact retained candidate with `tg approve <candidate-id>`; that authority atomically covers its active hard dependencies because they are part of the retained source history. Granting authority to a retained candidate lets it bypass unrelated candidates still awaiting authority, rebuilding only the affected suffix. Tollgate retains the admission order and all exact completed evidence: if independent later approvals close every authorization gap before promotion, it restores that order, cancels redundant bypass work, and reuses every certificate whose complete validation identity still matches. An explicit `tg reorder` replaces the retained admission order. `tg cancel <candidate-id>` cancels it. For the original one-phase user workflow, `tg approve HEAD` still submits and authorizes in one command.

If a concurrent approval already granted authority to that candidate as an
active dependency, repeating `tg approve <candidate-id>` succeeds without
changing the queue revision; `--wait` then follows the already-authorized item.

If synthesis conflicts with an earlier candidate, the `generation.tested_oid` shown by `tg --json status` is a supported recovery base. Tollgate retains every displayed speculative generation under `refs/tollgate/speculative/`, so the OID is available in every worktree of the registered repository. For a single task commit, use this flow:

```sh
tg --no-launch --json status
git rebase --onto <current-prefix-oid> HEAD^
# resolve the reported paths, regenerate derived files, and continue the rebase
tg --no-launch --json status
tg --no-launch --json candidate HEAD
```

An extension after the selected prefix is safe: Tollgate recognizes the still-active prefix, records hard dependencies on the queue items represented by it, and synthesizes the task after the current queue tip. If that prefix was canceled, superseded, or otherwise replaced before submission, JSON mode returns `stale-queue-prefix` with `release_oid`, `queue_revision`, `current_prefix_oid`, and a retry procedure. Refresh status and repeat against the new prefix; expected queue churn is never an internal-invariant error.

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

Run `tg diagnose <candidate-id>` for a stronger experiment. Tollgate checks the
exact anchored base once and the exact tested candidate twice in cold,
disposable slots, then recomputes attribution from those runs. `--no-replay`
shows retained evidence without scheduling work.

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
