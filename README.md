# Tollgate

Tollgate is a local dependent gate for macOS. It validates the exact prospective Git commits that would land on `master`, then promotes only a commit carrying a valid pass certificate.

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
tg init --run 'cargo test --all-targets' --detach-master
```

Tollgate writes its trusted policy to `<git-common-dir>/tollgate/config.toml`. The smallest valid file is:

```toml
version = 1

[[step]]
name = "ci"
run = "./ci"
```

`master` must not be checked out in a developer-visible worktree while its gate is active. Initialization never changes that checkout silently: `--detach-master` explicitly detaches a clean primary worktree at the identical commit, or you can switch it to a feature branch yourself.

An agent can submit a clean commit for speculative validation without permission to promote it:

```sh
tg candidate HEAD --wait
tg status <candidate-id>
```

`--wait` returns when validation has produced promotion-grade evidence (or a conclusive failure), while `master` remains unchanged. A user later grants authority to the exact retained candidate with `tg approve <candidate-id>`; `tg cancel <candidate-id>` cancels it. For the original one-phase user workflow, `tg approve HEAD` still submits and authorizes in one command.

## Safety model

- CI runs in detached worktrees belonging to a disposable execution mirror.
- Candidate submission retains the immutable source under `refs/tollgate/sources/`; promotion authority is recorded separately.
- Promotion retains and re-verifies the tested object, then uses an expected-old-OID `git update-ref` compare-and-swap.
- SQLite runs WAL + foreign keys + `synchronous=FULL` and uses durable external-operation intents.
- Output is appended to disk before live delivery; a hidden or slow UI cannot block only the UI path.
- APFS seed creation uses `clonefile`, never a copy command that can silently fall back to physical copying.
- The Unix socket is mode `0600`, its parent is `0700`, and both sides verify the effective peer UID.

The complete normative behavior is in [docs/technical-design.md](docs/technical-design.md).
