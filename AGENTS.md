<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB

Distributed storage platform: Paxos consensus, per-key slots, WAL durability,
crowdb-tree engine, and a disk-block allocator. Rust workspace with C++ storage
and transport exposed through FFI.

## Project rules

- Run every build, test, lint, and executable through `pixi run`.
- `unsafe_code = deny` by default. Keep unsafe confined to existing scoped FFI
  or low-level modules; raise any new exception before adding it.
- Keep hot paths lock-free. Before adding a lock, stop and ask the user with
  the contention, ordering, and complexity trade-off.
- Add Rust tests under each crate's `tests/`; crates with `test-util` enable it
  for their own tests through self dev-dependencies.
- Preserve existing work. Never hard-reset, revert, or restore files from an
  older commit without explicit approval. Use stash or a temporary branch.
- Ordinary interactive work is committed only when asked, as one coherent
  commit. Requirement work follows `/implement-requirement`.
- Commit messages are single-line subjects without bodies, trailers, doc
  references, or requirement numbers. Code comments also omit those references.
- Before committing, run the relevant gate through pixi:
  Rust fmt and clippy, changed C++ format/tree-lint, and affected tests. Fix
  ordinary failures up to three times; report confirmed pre-existing failures.
  Requirement work uses `/implement-requirement` retry and blocking rules.
- Playwright uses an installed system browser. Do not install one locally.
- Use 60-second shell timeouts by default. Start hang-prone commands in the
  background and poll. Show complete output; do not hide errors with filters.
- Markdown is primarily read raw. Prefer bullets; use tables for real
  comparisons only. `doc/doc_index.md` always uses tables. When a table is
  used, pad columns with spaces so the raw markdown aligns visually.

## Skill dispatch

- Repository skills live only under `.agents/skills/`; do not create legacy
  aliases or compatibility symlinks such as `.devin`.
- Code changes: `/coding`; add `/console-ui-e2e` only for visible UI or
  Playwright work.
- Test diagnosis: `/debug-test`, or `/console-ui-e2e` for browser/UI failures.
- Docs: one matched `/doc-*` guide; `/doc` only for general or unclear targets.
- Requirements: `/implement-requirement`; open the detail directly and consult
  the backlog index only for selection, ordering, or status.
- Pre-push or explicitly requested code review: `/review`.
- Design questions: one section selected through `doc/doc_index.md`.
- Operations/user behavior: `doc/user-manual/user-guide.md`.
