<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Mirror-to-EC Conversion Plan

Upstream: [working design](design-mirror-to-ec-conversion.md),
[requirement](../backlog/R93-chunkdb-mirror-to-ec-conversion.md), and
[chunkdb design](../design/chunkdb/design-crowdb-chunkdb.md).

Goal: convert closed shared mirror-strip groups into durable EC strips through
real DiskIO while preserving read visibility, offsets, and cleanup safety.

## Phase 1: Contracts and Lifecycle

- [x] **Protocol and configuration**: add validated conversion policy, trigger
  types, FlatBuffers messages, and RPC IDs/wrappers. Files:
  `app/crowdb-chunkdb/src/chunkdb_config.rs`,
  `lib/crowdb-protocol/src/types/chunkdb.rs`,
  `lib/crowdb-protocol/src/fbs/chunkdb.fbs`, protocol wrapper files.
- [~] **Tentative EC replacement**: add lifecycle methods that allocate an EC
  replacement with exact geometry and safely discard unpublished segments.
  Files: `app/crowdb-chunkdb/src/lifecycle/handler.rs`.
- [ ] **Client API**: expose single and batch conversion through routed
  crowdb-rpc calls. Files: `lib/crowdb-chunkdb-client/src/client.rs`,
  `lib/crowdb-chunkdb-client/src/rpc_transport.rs`.

## Phase 2: Conversion Engine

- [ ] **Persistent task model**: add versioned task envelope, typed payload,
  canonical/ready/lease keys, stable task/allocation identities, atomic index
  transitions, progress, and task-store scans. Files:
  `app/crowdb-chunkdb/src/task.rs`, `app/crowdb-chunkdb/src/task/store.rs`,
  `lib/crowdb-protocol/src/types/chunkdb.rs`, protocol key modules.
- [ ] **Task manager and scanner**: add admission, claim generation, retry,
  cancellation, event plus safety scan, source discovery, and queue-driven
  executor sizing. Files: `app/crowdb-chunkdb/src/task/manager.rs`,
  `app/crowdb-chunkdb/src/task/scanner.rs`,
  `app/crowdb-chunkdb/src/task/executor.rs`.
- [ ] **Idempotent task allocation**: associate every DiskDB allocation with a
  stable task/sub-allocation identity so an unknown response can be recovered
  without leaking or allocating a second block set. Files:
  `lib/crowdb-protocol/src/types/diskdb.rs`, DiskDB schema/client/server and
  `app/crowdb-chunkdb/src/allocator/`.
- [ ] **Client incremental EC**: retain each closed mirror shadow, update parity
  per one-MiB shard, account the 8+4 group to memory budget, and keep partial
  groups active during scale-in. Files: `lib/crowdb-common/rust/src/ec.rs`,
  `lib/crowdb-chunk-client/src/writer/small_pipeline.rs`, small-writer modules.
- [ ] **Client task fast path**: begin a durable task, write retained data plus
  parity without mirror reads, fsync, and complete fenced replacement. Files:
  `lib/crowdb-chunk-client/`, `lib/crowdb-chunkdb-client/`.
- [ ] **DiskIO router**: discover owners and implement replica read fallback,
  parallel writes, and per-disk fsync. Files:
  `app/crowdb-chunkdb/src/conversion/io.rs`.
- [ ] **Selection and encoding**: select immutable compatible runs and encode
  parity directly from mirror shards. Files:
  `app/crowdb-chunkdb/src/conversion.rs`.
- [ ] **Chunkdb takeover handler**: claim abandoned or scanner-created tasks,
  read mirrors, allocate, encode, write, fsync, fenced-replace, retry ambiguity,
  and safely discard on definite failure. Files:
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
  task discovery/claim/takeover, failure rollback, restart cleanup, deletion
  race, and concurrency. Files:
  `app/crowdb-chunkdb/tests/conversion_test.rs`.
- [ ] **Crash-point E2E**: kill and restart client/chunkdb before admission,
  during allocation/write/fsync, at ambiguous publication, and during cleanup;
  assert one authoritative layout, readable bytes, and no referenced/orphaned
  block reclamation. Files: chunkdb and chunk-client E2E suites.
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
