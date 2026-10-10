<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Tree range metrics plan

Upstream: [R228](../backlog/R228-tree-range-metrics.md).

- [x] Define and expose an exact root summary with root version, coverage slot,
  live KV count, logical key/value bytes, and reachable page counts.
- [x] Update the cached summary only after durable snapshot anchor publication;
  rebuild it from the selected durable root on reopen.
- [x] Expose the summary through the C API and Rust tree FFI.
- [x] Carry summary qualification and values through topology status and show
  exact/unavailable state in the tree inspector.
- [ ] Carry exact child aggregates in persisted inner-page metadata.
- [ ] Add bounded range summaries and split/placement consumers.
- [ ] Add crash, split, overwrite/delete, overflow, compaction, and legacy-page
  acceptance coverage before enabling data-weighted placement.

Verification so far: `pixi run cargo test -p crowdb-tree-ffi --lib` and the
`crowdb-tree` C++ targets build successfully.
