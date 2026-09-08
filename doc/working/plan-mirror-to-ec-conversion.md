<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Mirror-to-EC Conversion Plan

Upstream: [working design](design-mirror-to-ec-conversion.md),
[requirement](../backlog/R93-chunkdb-mirror-to-ec-conversion.md), and
[chunkdb design](../design/chunkdb/design-crowdb-chunkdb.md).

Goal: convert closed shared mirror-strip groups into durable EC strips through
real DiskIO while preserving read visibility, offsets, and cleanup safety.

## Phase 1: Contracts and Lifecycle

- [~] **Protocol and configuration**: add validated conversion policy, trigger
  types, FlatBuffers messages, and RPC IDs/wrappers. Files:
  `app/crowdb-chunkdb/src/chunkdb_config.rs`,
  `lib/crowdb-protocol/src/types/chunkdb.rs`,
  `lib/crowdb-protocol/src/fbs/chunkdb.fbs`, protocol wrapper files.
- [ ] **Tentative EC replacement**: add lifecycle methods that allocate an EC
  replacement with exact geometry and safely discard unpublished segments.
  Files: `app/crowdb-chunkdb/src/lifecycle/handler.rs`.
- [ ] **Client API**: expose single and batch conversion through routed
  crowdb-rpc calls. Files: `lib/crowdb-chunkdb-client/src/client.rs`,
  `lib/crowdb-chunkdb-client/src/rpc_transport.rs`.

## Phase 2: Conversion Engine

- [ ] **DiskIO router**: discover owners and implement replica read fallback,
  parallel writes, and per-disk fsync. Files:
  `app/crowdb-chunkdb/src/conversion/io.rs`.
- [ ] **Selection and encoding**: select immutable compatible runs and encode
  parity directly from mirror shards. Files:
  `app/crowdb-chunkdb/src/conversion.rs`.
- [ ] **Atomic orchestration**: allocate, write, fsync, fenced-replace, retry
  ambiguity, and safely discard on definite failure. Files:
  `app/crowdb-chunkdb/src/conversion.rs`.
- [ ] **Policy runner and throttle**: add bounded background scans, concurrency,
  bandwidth limiting, and clean shutdown. Files:
  `app/crowdb-chunkdb/src/conversion.rs`.
- [ ] **Metrics and server wiring**: expose atomic metrics plus RPC/HTTP manual
  triggers and start the runner. Files: `app/crowdb-chunkdb/src/metrics.rs`,
  `app/crowdb-chunkdb/src/main.rs`,
  `app/crowdb-chunkdb/src/service/chunkdb_rpc_service/`.

## Phase 3: Tests and Review

- [ ] **Unit tests**: cover configuration, selection geometry/closure/policy,
  throttle bounds, and metrics snapshots. Files:
  `app/crowdb-chunkdb/tests/conversion_test.rs`.
- [ ] **Integration tests**: cover 24-to-3 replacement, active prefix append,
  failure rollback, restart cleanup, deletion race, and concurrency. Files:
  `app/crowdb-chunkdb/tests/conversion_test.rs`.
- [ ] **Client E2E**: exercise management calls over real crowdb-rpc. Files:
  `lib/crowdb-chunkdb-client/tests/conversion_api_test.rs`.
- [ ] **Small-write full E2E**: write real shared mirror strips, convert, verify
  EC data/parity/decode and retained-layout reads with the simple process
  cluster. Files:
  `lib/crowdb-chunk-client/tests/small_object_writer_e2e.rs`,
  `lib/crowdb-chunk-client/tests/common/e2e_stack.rs`.
- [ ] **Focused gates**: run each affected test target separately, then fmt,
  lint, and review the diff for correctness and hot-path cost.

## Phase 4: Permanent Design and Cleanup

- [ ] **Formal design**: fold current-state conversion invariants and
  operations into a chunkdb sub-design and index it. Files:
  `doc/design/chunkdb/design-crowdb-chunkdb-mirror-to-ec.md`,
  `doc/design/chunkdb/design-crowdb-chunkdb.md`, `doc/doc_index.md`.
- [ ] **Requirement cleanup**: delete the backlog detail/entry and both working
  documents after every acceptance item passes. Files:
  `doc/backlog/R93-chunkdb-mirror-to-ec-conversion.md`,
  `doc/backlog/backlog.md`, `doc/working/design-mirror-to-ec-conversion.md`,
  `doc/working/plan-mirror-to-ec-conversion.md`.
- [ ] **Full gate**: run `pixi run -- cargo fmt --all -- --check`,
  `pixi run rs-lint`, and `pixi run test-suite`; diagnose new failures and
  record confirmed baselines.

## Consolidated Files

- Protocol: `lib/crowdb-protocol/src/types/chunkdb.rs`,
  `lib/crowdb-protocol/src/fbs/chunkdb.fbs`, generated/wrapper RPC modules.
- Server: `app/crowdb-chunkdb/src/conversion.rs`,
  `app/crowdb-chunkdb/src/conversion/io.rs`,
  `app/crowdb-chunkdb/src/chunkdb_config.rs`,
  `app/crowdb-chunkdb/src/lifecycle/handler.rs`,
  `app/crowdb-chunkdb/src/metrics.rs`, `app/crowdb-chunkdb/src/main.rs`, and
  `app/crowdb-chunkdb/src/service/chunkdb_rpc_service/`.
- Client: `lib/crowdb-chunkdb-client/src/client.rs`,
  `lib/crowdb-chunkdb-client/src/rpc_transport.rs`.
- Tests: `app/crowdb-chunkdb/tests/conversion_test.rs`,
  `lib/crowdb-chunkdb-client/tests/conversion_api_test.rs`,
  `lib/crowdb-chunk-client/tests/common/e2e_stack.rs`, and
  `lib/crowdb-chunk-client/tests/small_object_writer_e2e.rs`.
- Documentation: `doc/design/chunkdb/design-crowdb-chunkdb-mirror-to-ec.md`,
  `doc/design/chunkdb/design-crowdb-chunkdb.md`, `doc/doc_index.md`, backlog
  and working files.

## Test Commands

- Unit/integration: `pixi run -- cargo test -p crowdb-chunkdb --test conversion_test`.
- Client transport: `pixi run -- cargo test -p crowdb-chunkdb-client --test conversion_api_test`.
- Full process: `pixi run -- cargo test -p crowdb-chunk-client --test small_object_writer_e2e`.
- Protocol: `pixi run -- cargo test -p crowdb-protocol`.
- Required final gates: `pixi run -- cargo fmt --all -- --check`,
  `pixi run rs-lint`, `pixi run test-suite`.
