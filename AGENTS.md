Use the `wt` skill for all work unless explicitly asked to work "on master".

~/.llms/skills/wt/SKILL.md (note this is not project-local)

Never edit files in the main repo directly unless explicitly asked to work "on master".

Perform follow up work on the same worktree as the intial work until promotion.

When work is complete, create one detailed local commit and immediately submit
it with `tg candidate HEAD` for speculative validation without promotion
authority. Do not wait for user approval before committing or scheduling the
candidate. After explicit promotion approval, authorize the exact candidate
with `tg approve <candidate-id>`; Tollgate owns any required regeneration,
certified promotion to `master`, and leased remote push. Worktree branches are
local-only and must never be pushed to a remote.

Do not create new branches unless explicitly requested.

Do not print a summary of changes.
