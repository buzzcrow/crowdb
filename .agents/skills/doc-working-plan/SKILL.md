---
name: doc-working-plan
description: Write and maintain a requirement execution plan.
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Working Plan

Write `doc/working/plan-<topic>.md` from the requirement's solution and
acceptance criteria. It holds implementation detail, is kept live, and is
deleted after completion.

- Start with the license, `# <Title> Plan`, upstream links, and one-line goal.
- Group dependency-ordered tasks by phase or component.
- Use `- [ ] **Name**: action. Files: paths.`; `[~]` marks the only active
  task and `[x]` a verified task.
- Split tasks until each is completable and maps to identifiable diff hunks.
- Record relevant symbols, signatures, ordering, edge cases, and migration
  steps needed to execute the high-level design without duplicating it.
- Include a consolidated file list and tests grouped by unit, integration, E2E.
- Keep status truthful as work proceeds.
- Add `## Blocked` only under `/implement-requirement` conditions, recording
  the decision/failure, alternatives or retries, and why work cannot continue.

For a persistent plan, state the exception in its header and remove completed
tasks instead of deleting the file.
