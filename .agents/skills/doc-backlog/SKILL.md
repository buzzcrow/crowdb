---
name: doc-backlog
description: Write a CROWDB requirement document as a testable high-level design and contract.
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Backlog Requirement

Write `doc/backlog/R**-<component>-<topic>.md` as one requirement's high-level
design and testable contract. It is removed by `/implement-requirement`.

Use this exact order:

1. `### R**: <component> — <Title>` after the license.
2. Optional `Status` when the item is explicitly deferred or blocked; state the
   reason and what would unblock it.
3. `Problem`: current behavior, impact, root cause, root-design link, and
   concrete operator/system scenarios.
4. `Solution`: architecture, invariants, component responsibilities, numbered
   work items naming real modules, and edge-case outcomes. If unsettled, list
   the human decisions and alternatives instead of inventing one.
5. `Dependencies`: incoming/outgoing requirements, named artifacts, and
   fallback for unlanded work.
6. `Acceptance`: one test case per solution and edge-case claim.
7. `Open Questions`, only for human decisions; include alternatives and trade-offs.

Each acceptance case states setup -> action -> assertion, names the invariant,
and ends with `Unit test`, `Integration test`, or `E2E test`. End with exact
pixi test, fmt, and clippy commands.

The requirement is the implementation's high-level design. Keep signatures,
file lists, sequencing, and other execution detail in the working plan; do not
create a separate working design. Requirement numbers are allowed here. Use an
ASCII flow only for three or more components or a primary/fallback path. Do not
use tables.
