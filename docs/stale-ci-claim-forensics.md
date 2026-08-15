# Stale Quest CI-claim forensic report

Date: 2026-08-14 America/Los_Angeles (events below are 2026-08-15 UTC)

Repository examined: `/Users/dthurn/quest_prototype`

Tollgate repository ID: `019ff601-cf8c-7b83-b38b-7c7aa5433cd3`

## Conclusion

Tollgate did not certify or promote a failing tree. The stale statement in
`pre-existing-issues.txt` was already false when `06bad643f824347114e6481f26b8ae79502d7e95`
was submitted.

The primary cause was a repository workflow and agent/worktree classification
error:

1. Two precursor Cumulus candidates (`a7b8e94420` and `e8cad2cd0c`) failed the
   exact Trox voting step because the Cumulus edits shifted localized source
   locations while leaving five tracked CSV reports stale.
2. The task agent called the failure “pre-existing” because localized copy had
   not changed, recorded that claim in `pre-existing-issues.txt`, and overlooked
   that source-location metadata is also a generated contract affected by line
   layout.
3. After the second failure, the agent ran the canonical extractor. The only
   difference from failed `e8cad2cd0c` to successful `06bad643f` is the five CSV
   files. The issue text is byte-for-byte unchanged.
4. The agent then ran strict Trox and `review:full`, both passed, amended the CSV
   refresh into `06bad643f`, and submitted it without deleting the now-resolved
   prose entry.

There is a secondary missing repository invariant: no deterministic check keeps
free-form issue prose consistent with current voting-step results. This is not a
Tollgate cache, prefix, checkout, certification, promotion, or failure-attribution
bug.

## Event timeline

All timestamps are UTC. The SQLite event sequence is included where it provides
the durable ordering.

| Time | Event |
|---|---|
| 03:33:24.969 | Event 3922: precursor Cumulus candidate `01a0037b-3e06-7fe3-b96f-61ea052021f4`, source/tested `a7b8e94420fb1d1356d94e00c886097040b3953d`, submitted. |
| 03:33:38.194 | Event 3925: precursor fails `voting-validation-failed`; dependencies passed and Trox emitted the five `csv-out-of-date` diagnostics. |
| 03:35:12.921 | Event 3928: amended precursor `01a0037c-e3ff-7ef0-8115-fc0f150e21dd`, source/tested `e8cad2cd0c95e000fe70f0dc4edc8fe90dae70cd`, submitted. |
| 03:35:26.088 | Event 3931: amended precursor fails the same five Trox diagnostics. |
| 03:37:34.039 | The originating task transcript records `npm run trox:extract` in the Cumulus worktree. |
| 03:37:52.531 | The transcript records strict Trox success: 624 files, 7,074,760 bytes, 5,125 messages, followed by 49 runtime tests and a runtime-build check. |
| 03:37:57.097 | The transcript starts `npm run review:full`; the task later records 508 test files / 5,141 tests plus regeneration, strict localization, lint, and typecheck passing. |
| 03:40:16.552 | The transcript shows exactly the five CSVs modified: en-US 192, ar 280, es 214, ja 170, and ru 236 rows on each side of the diff. |
| 03:40:21.124 | The extractor output is amended into `06bad643f824347114e6481f26b8ae79502d7e95`; the stale issue text remains unchanged. |
| 03:40:26.707 | Event 3937: successful Cumulus candidate `01a00381-ad52-7b42-af69-c1e117097a59` submitted. |
| 03:40:33.473 | Event 3940: promotion authorized; no earlier evidence was reused. |
| 03:42:43.608 | Event 3943: following candidate `01a00383-c354-72c3-b350-3857b6e6174c` submitted while Cumulus validation is still running. Its frozen prefix contains Cumulus first. |
| 03:43:21.560 | Event 3945: Cumulus becomes ready with certificate `01a00384-5dd8-7520-9986-2426a9cc1369`. |
| 03:43:24.351 | Event 3947: exact tested OID `06bad643f` promoted locally. |
| 03:43:35.450 | Event 3950: remote state becomes synchronized. |
| 03:43:40.213 | Event 3951: source cleanup completes; event 3952 records the cleanup operation. |
| 03:43:40.215 | Event 3953: the following candidate begins against the frozen two-item prefix. |
| 03:45:07.701 | Event 3957: following candidate promotion authorized. |
| 03:46:16.070 | Event 3959: following candidate becomes ready with certificate `01a00387-0786-74d1-ba6b-6e011da55bd5`. |
| 03:46:22.132 | Event 3961: synthetic tested OID `8f6dabb947155b7038681bb1f84914179f5666b0` promoted locally. |
| 03:46:30.147 | Event 3964: remote state becomes synchronized. |
| 03:46:34.734 | Event 3965: source cleanup completes; event 3966 records the cleanup operation. |
| 03:49:05.084 | Event 3970: RON-enum candidate `01a00389-975d-7d83-ad6e-b79135ad7cea` submitted against `8f6dabb947`. |
| 03:49:19.555 | Event 3973: exact `973f3cead7` generation fails Trox with the five stale-CSV diagnostics. No certificate or promotion authority exists. |
| 03:51:40 | The RON task transcript records the canonical extractor after inspecting the failed candidate logs. |
| 03:52:56 | The RON task transcript removes the stale `pre-existing-issues.txt` entry after verifying the extractor changed only `source_locations`. |
| 03:53:08 | The RON source commit is amended with refreshed CSVs and the ledger removal. |
| 03:53:21.249 | Event 3978: replacement `01a0038d-7fb6-7222-a3e0-c13da318e5fd`, source/tested `3ec4c4231ade884640cb7c679a917ef2c6760f71`, submitted. |
| 03:53:25.488 | Event 3981: replacement promotion authorized. |
| 03:56:30.602 | Event 3988: replacement becomes ready with certificate `01a00390-6809-79f3-af9e-4d2183d8d30b`. |
| 03:56:35.665 | Event 3990: exact tested OID `3ec4c4231a` promoted locally. |
| 03:56:52.358 | Event 3993: remote state becomes synchronized. A separate `user-master.sync-needs-attention` event concerns the user's divergent `master` checkout, not the certified release or remote push. |
| 03:56:56.956 | Event 3994: source cleanup completes; event 3995 records the cleanup operation. |

## Candidate identity matrix

The configuration digest for every row is
`88141b18bbca9a589ee195ec2cc2a43ca2d7e73cd07021ff1bfcc1266d8c801a`.
The step-graph digest for every row is
`810484672579147e94a3cccebce869510c6dabc6b390b65e2939f7a1cf38e6db`.
The engine epoch is `1` throughout.

| Candidate | Source OID | Anchored base | Ordered prefix OIDs | Tested OID | Tested tree OID |
|---|---|---|---|---|---|
| `01a0037b-3e06-7fe3-b96f-61ea052021f4` | `a7b8e94420fb1d1356d94e00c886097040b3953d` | `15b4ee38b4ccfe9d898caa4e2cc7d0e2c7c544d6` | `[a7b8e94420fb1d1356d94e00c886097040b3953d]` | `a7b8e94420fb1d1356d94e00c886097040b3953d` | `437e3edb216d2b6e63e1c90b2fe9b980b7ca7815` |
| `01a0037c-e3ff-7ef0-8115-fc0f150e21dd` | `e8cad2cd0c95e000fe70f0dc4edc8fe90dae70cd` | `15b4ee38b4ccfe9d898caa4e2cc7d0e2c7c544d6` | `[e8cad2cd0c95e000fe70f0dc4edc8fe90dae70cd]` | `e8cad2cd0c95e000fe70f0dc4edc8fe90dae70cd` | `c61ce1887f7d3c61c69e03072c9377a16dd85bbc` |
| `01a00381-ad52-7b42-af69-c1e117097a59` | `06bad643f824347114e6481f26b8ae79502d7e95` | `15b4ee38b4ccfe9d898caa4e2cc7d0e2c7c544d6` | `[06bad643f824347114e6481f26b8ae79502d7e95]` | `06bad643f824347114e6481f26b8ae79502d7e95` | `11225a68b1bc179ba18121ac7e863041521a1166` |
| `01a00383-c354-72c3-b350-3857b6e6174c` | `61f7cddf61b16d634972865ce5f30626772bb2ee` | `15b4ee38b4ccfe9d898caa4e2cc7d0e2c7c544d6` | `[06bad643f824347114e6481f26b8ae79502d7e95, 8f6dabb947155b7038681bb1f84914179f5666b0]` | `8f6dabb947155b7038681bb1f84914179f5666b0` | `dc7efa8a434571ab0fab0c10af5c412f8479631f` |
| `01a00389-975d-7d83-ad6e-b79135ad7cea` | `973f3cead76445e10a9264c089458e3e1faab308` | `8f6dabb947155b7038681bb1f84914179f5666b0` | `[973f3cead76445e10a9264c089458e3e1faab308]` | `973f3cead76445e10a9264c089458e3e1faab308` | `c0b42ed338d188953d10a5a1680e8ae873619d59` |
| `01a0038d-7fb6-7222-a3e0-c13da318e5fd` | `3ec4c4231ade884640cb7c679a917ef2c6760f71` | `8f6dabb947155b7038681bb1f84914179f5666b0` | `[3ec4c4231ade884640cb7c679a917ef2c6760f71]` | `3ec4c4231ade884640cb7c679a917ef2c6760f71` | `3c8fa91fc786291f6ad1607e48772ecb6b187f21` |

| Candidate | Environment fingerprint | Step results | Certificate | Promoted release OID |
|---|---|---|---|---|
| `01a0037b-3e06-7fe3-b96f-61ea052021f4` | `f86e66b9ff139fa0e37cc451396060e8e3cbccbb3c6bc69f91502d7ee7c05b33` | dependencies success; Trox exit 1; review skipped | none | none |
| `01a0037c-e3ff-7ef0-8115-fc0f150e21dd` | `f86e66b9ff139fa0e37cc451396060e8e3cbccbb3c6bc69f91502d7ee7c05b33` | dependencies success; Trox exit 1; review skipped | none | none |
| `01a00381-ad52-7b42-af69-c1e117097a59` | `f86e66b9ff139fa0e37cc451396060e8e3cbccbb3c6bc69f91502d7ee7c05b33` | dependencies, Trox, review success | `01a00384-5dd8-7520-9986-2426a9cc1369` | `06bad643f824347114e6481f26b8ae79502d7e95` |
| `01a00383-c354-72c3-b350-3857b6e6174c` | `f86e66b9ff139fa0e37cc451396060e8e3cbccbb3c6bc69f91502d7ee7c05b33` | dependencies, Trox, review success | `01a00387-0786-74d1-ba6b-6e011da55bd5` | `8f6dabb947155b7038681bb1f84914179f5666b0` |
| `01a00389-975d-7d83-ad6e-b79135ad7cea` | `f86e66b9ff139fa0e37cc451396060e8e3cbccbb3c6bc69f91502d7ee7c05b33` | dependencies success; Trox exit 1; review skipped | none | none |
| `01a0038d-7fb6-7222-a3e0-c13da318e5fd` | `afd1be6c92315a2cc6006359c19b2bf04466725ed2365c49e7bdc92de5d4d1f0` | dependencies, Trox, review success | `01a00390-6809-79f3-af9e-4d2183d8d30b` | `3ec4c4231ade884640cb7c679a917ef2c6760f71` |

The following composition candidate's source OID is
`61f7cddf61b16d634972865ce5f30626772bb2ee`, but its tested OID is the
synthetic prefix commit `8f6dabb947155b7038681bb1f84914179f5666b0`. Git
independently confirms that commit has parent
`06bad643f824347114e6481f26b8ae79502d7e95` and tree
`dc7efa8a434571ab0fab0c10af5c412f8479631f`; this is the expected composition,
not cross-candidate reuse.

## Certificate and log integrity

The three certificates record the tree OIDs above, the common configuration and
step-graph digests, their buildset environment fingerprint, every successful
voting-step attempt and sealed log hash, `checkout_verified: true`, and no
warnings. Their tested-object refs still resolve exactly:

- `refs/tollgate/tested/01a00381-b2d4-75e1-b4ab-9bd784f6aaf7` -> `06bad643f824347114e6481f26b8ae79502d7e95`
- `refs/tollgate/tested/01a00383-c998-7712-9d81-68e234375094` -> `8f6dabb947155b7038681bb1f84914179f5666b0`
- `refs/tollgate/tested/01a0038d-8461-7372-ae2b-e69dee96c736` -> `3ec4c4231ade884640cb7c679a917ef2c6760f71`

An independent BLAKE3 utility recomputed every retained `.tlog` hash and matched
the SQLite attempt record and certificate. The complete step-log evidence for
the certified buildsets, plus the decisive failed Trox attempts, is:

| Buildset | Result | Trox log BLAKE3 |
|---|---|---|
| `01a0037b-4369-7d83-9dff-48f615759a38` | exit 1 | `eb122101d6dc574149c26b764172672df94e0abcb71d1f3b2729a7009e2237ca` |
| `01a0037c-e91a-7411-b820-26f04653a810` | exit 1 | `10772a38ea4206989a19d24e06acc14d9cef2c63ac0c15ed1244fe1a5d8d7b57` |
| `01a00381-b2d4-75e1-b4ab-9bd784f6aaf7` | success | `44b37f35b66e4fe504c28507e3ffb6c22042eb9e5a21d7490631c3ef012b63c8` |
| `01a00383-c998-7712-9d81-68e234375094` | success | `811e4e3bb124b23279a78f481f7a25bcb28cd7584a0ccefdac4a5d15a6d88e05` |
| `01a00389-9bbd-7d32-a361-289eeba2b1a2` | exit 1 | `12d61643f7427dcef5c5010a34680ed5d0a80010c68c0279227cd8e4ad5c1b76` |
| `01a0038d-8461-7372-ae2b-e69dee96c736` | success | `4f5ed26e823a2d8852850d4c1768bdebeace871eb4e57d19a1bcbf525457836e` |

| Certified buildset | Dependencies log | Review log |
|---|---|---|
| `01a00381-b2d4-75e1-b4ab-9bd784f6aaf7` | `33ee5e00ed42ad3cae28c05b08d7ac0cf00df9d355a882aa42e7975663dd29bb` | `cb6f0ea13c537a0c68cedabd4334e0952f7e3d359ed530e724053d4a6684b63a` |
| `01a00383-c998-7712-9d81-68e234375094` | `e25dc470b73ee4d0d55526a941cd1ec993938809da20c7e40bb3c222505a235a` | `a69825d556b3b9e541ecfb0ff5da63f5800305bb45ec90f6209003e6ef625028` |
| `01a0038d-8461-7372-ae2b-e69dee96c736` | `bab470c24cc96729ec46c9402f7e2da761e2220bddb1c862b1a7e76749717225` | `708ef9ba5c8bd7b156ef74d550fefa09cf8a1a692164320eed4b1a4b1b32a133` |

The successful retained Trox logs for `06bad643f` and `8f6dabb947` both report
624 files, 7,074,760 bytes, 5,125 messages, zero terms, and 49 passing runtime
tests. The later failed log reports the five specific stale CSVs. The replacement
log reports 624 files, 7,074,383 bytes and the same passing runtime suite.

The exact 1,740-byte configuration snapshot is stored under digest
`88141b18bbca9a589ee195ec2cc2a43ca2d7e73cd07021ff1bfcc1266d8c801a`.
It freezes the dependencies, Trox, and review commands and freezes `TROX_ROOT` at
`/Users/dthurn/.cache/quest-prototype/trox-6cc60f9d47cb`. The tool revision is
therefore part of the executed configuration and is also printed in the logs.

## Why the issue was already false at submission

The immutable history provides a direct before/after experiment:

- `15b4ee38b` by itself passes strict Trox.
- `a7b8e94420` and `e8cad2cd0c` contain the stale prose entry and fail strict
  Trox with the same five diagnostics.
- `git diff e8cad2cd0c..06bad643f` contains only the five localization CSVs.
  Structural CSV comparison shows 192/280/214/170/236 changed rows respectively,
  and every changed row differs only in `source_locations`; no row is added or
  removed.
- `pre-existing-issues.txt` has no diff between `e8cad2cd0c` and `06bad643f`.
- The originating transcript explicitly runs `npm run trox:extract`, then strict
  Trox and `review:full`, before amending those five files into `06bad643f`.

The transcript also captures the mistaken reasoning before the failed candidates:
the agent calls the reports “already-filed” and “pre-existing” because focused
Cumulus tests passed and localized copy did not change. That reasoning ignored
source-location metadata. Tollgate rejected both stale trees and accepted only
the refreshed tree.

## Cache, checkout, and prefix analysis

Tollgate has no validation-result cache in this path. Every validation generation
gets a new buildset and reruns its frozen steps. Consequently there is no result
cache key that could have omitted a tested tree or allowed non-equivalent trees
to share a Trox success.

The only cache lookup is a performance seed for the ignored
`tools/game-data/target` path. It is keyed by repository, cache epoch, OS,
architecture, and cache-policy digest. It deliberately is not certificate
evidence. After any seed import, Tollgate still provisions the slot at the exact
generation `tested_oid` and runs all voting steps. The tracked source files and
five generated CSV inputs come from that exact Git tree, not the seed. The
buildset freezes the tested OID, expected parent, generation ID, environment
fingerprint, and step definitions.

Slot provisioning performs a hard reset to the exact tested OID and cleans
untracked files. After successful voting steps, checkout verification independently
requires:

- `HEAD` equals the exact tested OID;
- index equals `HEAD`;
- tracked worktree equals the index; and
- no merge, cherry-pick, or rebase marker exists.

Only then can `workspace_verified` be true and a pass certificate be issued.
Promotion synchronously rereads the disk configuration, revalidates the certificate
against the current generation and exact release parent, verifies every durable
log seal, retains the tested object, and moves the release ref with an exact
compare-and-swap. The promoted OIDs and certificate tested/tree OIDs match.

The historical slot is mutable and has since been reused, so its present contents
are not treated as evidence. Independent confirmation instead uses the immutable
tested refs and Git objects, certificate tree OIDs, recomputed durable-log seals,
and clean synthetic checkouts of the exact OIDs. Those checks reproduce every
pass/fail boundary recorded at the time without trusting current slot state.

## Later RON failure attribution

Attribution was correct and tree-specific:

- The certified base `8f6dabb947` passes strict Trox in isolation.
- The exact RON source `973f3cead7`, whose parent is `8f6dabb947`, fails strict
  Trox with five CSV diagnostics.
- Applying the RON source changes without the CSV refresh to a synthetic clone
  reproduces the failure.
- The canonical extractor changes exactly `source_locations` for 1,142 rows in
  each of all five CSVs. It changes no entry ID, row ID, English, translation,
  status, note, placeholder, or source revision.
- The refreshed exact tree `3ec4c4231a` passes strict Trox and `review:full`.
  Its diff from `973f3cead7` is only the five CSV refreshes plus removal of the
  stale issue entry.

Tollgate marks the exact `973f3cead7` validation generation as failed and binds
the diagnostics to that buildset/tested OID. It does not claim to infer causality
by rerunning every base; the independently certified base and synthetic fixture
establish candidate introduction here. `rebuild_after_failure` removes a failed
item from any later predicted prefixes, so failure evidence is not transferred
to unrelated suffixes.

The RON task transcript shows a separate workflow weakness: before submission it
ran a comparison-oriented local `npm run review` with `JOURNEY_REVIEW_BASE=release`,
not the full Trox voting lane. Tollgate then caught the candidate-induced stale
reports. After inspecting the exact candidate log, the task ran the extractor,
verified the structural CSV-only change, deleted the resolved prose claim, and
submitted the passing replacement.

## Tollgate code paths

| Responsibility | Exact path |
|---|---|
| Candidate source retention and validation-generation construction | `crates/tollgate-service/src/lib.rs:3608-3760` creates the immutable source ref, constructs the ordered prefix from the authoritative release, and derives the generation. |
| Generation identity | `crates/tollgate-domain/src/model.rs:116-159` hashes anchored base, ordered item/source pairs, every prefix OID, configuration digest, step-graph digest, and engine epoch. |
| Prefix synthesis | `crates/tollgate-git/src/lib.rs:712-818` applies each source without committing, writes the tree, preserves the source object only when parent and tree are exact, otherwise creates a rewritten synthetic commit, then resets the builder to that OID. |
| Cache lookup and buildset freezing | `crates/tollgate-service/src/lib.rs:7179-7345` performs only seed lookup, creates a fresh buildset bound to the generation/tested OID and environment, provisions the slot at that OID, and imports an optional performance seed. |
| Slot checkout | `crates/tollgate-git/src/lib.rs:864-910` resets/recreates a detached locked worktree at the exact tested OID and cleans untracked files. |
| Voting result and checkout verification | `crates/tollgate-runner/src/lib.rs:1063-1099` requires all applicable voting steps to succeed and then calls `verify_workspace`; `1154-1192` performs the exact Git checks. |
| Certificate issuance | `crates/tollgate-service/src/lib.rs:7485-7575` persists fresh attempts and resolves the passing tree; `7682-7743` issues a certificate containing tested/tree/parent/config/graph/engine/environment/log identities and checkout verification. |
| Failure attribution | `crates/tollgate-service/src/lib.rs:7744-7787` fails the current item/buildset as `voting-validation-failed`; `8436-8491` rebuilds any later prefix containing that failed item. |
| Certificate contract | `crates/tollgate-domain/src/model.rs:228-287` binds the certificate to the current item/generation/tested OID/parent/config/graph/engine and requires checkout verification; promotion additionally requires the observed release parent. |
| Promotion eligibility and exact release update | `crates/tollgate-service/src/lib.rs:8790-8876` checks head authorization/readiness, durable log hashes, disk configuration, and certificate validity; `9138-9170` retains the tested object and performs the exact release compare-and-swap. |
| Ref compare-and-swap | `crates/tollgate-git/src/lib.rs:949-967` updates the release ref only from the expected old OID to the certified tested OID. |

## Synthetic verification

All behavioral reproduction was performed in disposable clones/fixtures rather
than changing Quest's mutable checkout:

1. Exact `15b4ee38b` base: strict Trox passes.
2. Synthetic Cumulus-unrefreshed tree: apply `15b4ee38b..06bad643f` while
   deliberately excluding the five CSVs and the prose ledger; strict Trox fails
   with the same five diagnostics.
3. Add the five CSV blobs from `06bad643f`: strict Trox passes with the exact
   certified file/message counts.
4. Exact `8f6dabb947` base: strict Trox passes.
5. Exact unrefreshed RON tree `973f3cead7`: strict Trox fails with the same five
   diagnostics.
6. Exact refreshed RON tree `3ec4c4231a`: strict Trox passes.

Focused Tollgate tests cover the same generic invariants with synthetic Git
repositories: exact prefix/parent construction, refusal to overwrite contradictory
retained evidence, and candidate certificate reuse only for an unchanged predicted
generation. These focused tests passed in the forensic worktree:

- `tollgate-git::constructs_a_shared_synthetic_prefix_with_preserved_parent_chain`
- `tollgate-git::tested_object_ref_never_overwrites_contradictory_owned_evidence`
- `tollgate-service::candidates_validate_without_promotion_and_reuse_exact_predicted_prefixes`

## Recommended repository guard

Do not teach Tollgate to parse `pre-existing-issues.txt`. It is arbitrary prose,
is not part of the certificate model, and cannot deterministically identify the
command, baseline, expected diagnostic, or whether a later change resolved it.

Use a repository-owned structured registry for deterministic CI claims, while
leaving ordinary prose issues free-form. A `voting-step-failure` entry should at
minimum contain a stable issue ID, exact voting-step name, anchored baseline OID,
expected diagnostic codes, and status. A repository checker should reject:

- an open failure claim for a voting step that passed in the current full review;
- a new “pre-existing” claim whose diagnostic cannot be reproduced on its stated
  anchored baseline; and
- a resolved entry left open after its deterministic probe passes.

Run the checker as the final stage of `npm run review:full`, and include that
command in Tollgate's voting `review` step. Also run it in the worktree workflow
after any canonical extractor/regenerator and immediately before committing.
For this specific failure class, the workflow must treat source-line movement as
localization-relevant even when player-facing text is unchanged, and must run
`trox:extract` followed by strict `trox:check` before calling stale location
reports pre-existing.

This guard belongs in Quest because Quest defines the ledger semantics, Trox
diagnostic codes, and review commands. Tollgate should continue enforcing exact
tree/configuration/environment/log evidence and remain agnostic about repository
prose.

Tollgate can still prevent the surrounding attribution failure without knowing
Quest's prose format. Voting steps can emit structured diagnostic codes, paths,
and an explicit generator command; Tollgate can compare exact base and candidate
runs, run a cold base/candidate/candidate diagnostic matrix, and verify the
generator in a disposable checkout. That makes “candidate-introduced” versus
“inherited” an evidence-backed gate result and produces a reviewable repair
patch while preserving the immutable failed candidate.
