---
name: doc
description: Route an unclear CROWDB documentation task or maintain general docs; do not combine with a known doc-type skill.
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Documentation

Start at `doc/doc_index.md`; open only the row and section matching the task.

## Locations

- `doc/design/<area>/design-crowdb-<area>.md`: architecture and rationale.
- `doc/design/<area>/design-crowdb-<area>-<topic>.md`: permanent topic detail.
- `doc/design/kv/kv-*-flow-analysis.md`: permanent KV path analysis.
- `doc/user-manual/user-guide.md`: operations; generated HTML is not hand-edited.
- `doc/backlog/`: requirement index and analysis.
- `doc/working/`: implementation plans and explicitly requested design drafts.

## Rules

- Keep one source of truth. Link by section anchor instead of copying text.
- Fix conflicting upstream design before downstream docs or code.
- Index permanent document changes, not backlog or working files.
- Permanent design describes current state; backlog requirements hold
  high-level proposed design, and working plans hold execution detail.
- Split independent topics; delete working files when their work completes.
- Prefer concrete, tight prose and raw-readable bullets. Remove filler,
  repetition, rhetorical openings, adjective lists, and excessive em dashes.
- Rebuild `user-guide.html` with
  `pixi run -- python doc/user-manual/build_html.py` whenever its Markdown
  source changes.

When the target is a design, backlog requirement, design draft, or working
plan, use only its matching `/doc-*` guide instead of this router.
