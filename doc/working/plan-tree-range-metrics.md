<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Tree range metrics Plan

Upstream: [R228](../backlog/R228-tree-range-metrics.md), [tree storage design](../design/tree/design-crowdb-tree-storage.md).

Goal: publish durable-root metrics that distinguish live logical data from reachable page and overflow storage, while keeping reads O(1) and unknown legacy state unavailable.

- [x] **Summary contract**: extend the tree summary/C API/FFI and protocol view with overflow, reachable-byte, and logical-byte fields; document zero-versus-unavailable semantics. Files: `lib/crowdb-tree/include/crowdb-tree/btree/diagnostics.h`, `lib/crowdb-tree/include/crowdb-tree/c_api.h`, `lib/crowdb-tree/ffi/src/stats.rs`, `lib/crowdb-protocol/src/mgmt.rs`, design docs.
- [x] **Durable publication accounting**: compute summary accounting from the selected snapshot, publish only after anchor durability, and reconstruct structural counts on reopen. Files: `lib/crowdb-tree/src/snapshot/persist.cpp`, `lib/crowdb-tree/src/btree/crowdb-tree.cpp`.
- [x] **Acceptance coverage**: empty, overwrite/delete, overflow and reopen cases verify logical and structural metrics and coverage fencing. Files: `lib/crowdb-tree/tests/integration/persist_test.cpp`, `lib/crowdb-tree/tests/integration/overflow_test.cpp`.
- [ ] **Verification and cleanup**: run affected C++/Rust tests and formatting/lint gates, then remove the completed backlog entry and this plan in the final cleanup commit.

Files: `lib/crowdb-tree/include/crowdb-tree/btree/diagnostics.h`, `lib/crowdb-tree/include/crowdb-tree/c_api.h`, `lib/crowdb-tree/src/btree/crowdb-tree.cpp`, `lib/crowdb-tree/src/snapshot/persist.cpp`, `lib/crowdb-tree/ffi/src/stats.rs`, `lib/crowdb-tree/ffi/src/sys.rs`, `lib/crowdb-protocol/src/mgmt.rs`, `lib/crowdb-tree/tests/integration/persist_test.cpp`, `doc/design/tree/design-crowdb-tree-storage.md`.

Tests: `pixi run test-cpp`; `pixi run cargo test -p crowdb-tree-ffi`; `pixi run rs-fmt-check`; `pixi run tree-fmt`; `pixi run tree-lint`; `pixi run rs-lint`.
