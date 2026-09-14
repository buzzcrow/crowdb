---
name: implement-requirement
description: Implement one CROWDB backlog requirement from its high-level design through focused verification and cleanup.
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Implement Requirement

Use for an item in `doc/backlog/backlog.md`. Its detail is the high-level
design and testable contract; do not create a separate design draft.

1. Read the matched requirement and relevant code. Read the backlog index only
   for selection, dependency ordering, or status; read design sections only to
   resolve behavior or module interaction.
2. Update a missing high-level decision with `/doc-backlog`; block only on a
   real human choice.
3. Create one `doc/working/plan-<topic>.md` with `/doc-working-plan`; keep all
   file, symbol, sequencing, and test detail there.
4. Implement in plan order, keep it current, and commit coherent tasks.
5. Run affected acceptance tests and fmt/lint gates separately.
6. Update permanent design only for an in-scope architecture change.
7. Delete the completed requirement, backlog entry, and plan in a final cleanup
   commit.

Find tests with `pixi task list`; prefix server-spawning tests with
`pixi run clean-env &&`. Before push run fmt and `pixi run rs-lint`.

Proceed autonomously except when a design has multiple valid choices with no
clear winner, or a test still fails after five root-cause-driven attempts.
Then record the alternatives or failed command, attempts, diagnosis, and exact
failure under plan `## Blocked`; commit that state and ask the user.

`Open Questions` contains only decisions requiring user input. Keep unfinished
implementation work in the plan instead of presenting it as an open issue.

This overrides ordinary commit cadence. Every commit passes its applicable
gate.
