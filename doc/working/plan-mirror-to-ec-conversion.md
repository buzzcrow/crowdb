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
- [x] **Tentative EC replacement**: add lifecycle methods that allocate an EC
  replacement with exact geometry and safely discard unpublished segments.
  Files: `app/crowdb-chunkdb/src/lifecycle/handler.rs`.
- [x] **Client API**: expose single and batch conversion through routed
  crowdb-rpc calls. Files: `lib/crowdb-chunkdb-client/src/client.rs`,
  `lib/crowdb-chunkdb-client/src/rpc_transport.rs`.

## Phase 2: Conversion Engine

- [x] **Persistent task model**: add versioned task envelope, typed payload,
  canonical/ready/lease keys, stable task and operation identities, atomic index
  transitions, progress, and task-store scans. Files:
  `app/crowdb-chunkdb/src/task.rs`, `app/crowdb-chunkdb/src/task/store.rs`,
  `lib/crowdb-protocol/src/types/chunkdb.rs`, protocol key modules.
- [x] **Task manager and scanner**: add admission, claim generation, heartbeat,
  retry, event plus safety scan, source discovery, and bounded dispatch limited
  by available executor slots. Files: `app/crowdb-chunkdb/src/task/manager.rs`,
  `app/crowdb-chunkdb/src/task/scanner.rs`,
  `app/crowdb-chunkdb/src/task/executor.rs`.
- [x] **Tentative allocation recovery**: checkpoint the task-owned placement
  before I/O; retain allocations after ambiguous KV responses so a committed
  checkpoint is reusable and DiskDB's tentative scanner reclaims an
  unreferenced allocation. Files: `app/crowdb-chunkdb/src/conversion.rs`,
  `app/crowdb-chunkdb/src/lifecycle/handler.rs`.
- [x] **Client incremental EC**: retain each closed mirror shadow, update parity
  per one-MiB shard, account the 8+4 group to memory budget, and keep partial
  groups active during scale-in. Files: `lib/crowdb-common/rust/src/ec.rs`,
  `lib/crowdb-chunk-client/src/writer/small_pipeline.rs`, small-writer modules.
- [x] **Client task fast path**: begin a durable task, write retained data plus
  parity without mirror reads, fsync, and complete fenced replacement. Files:
  `lib/crowdb-chunk-client/`, `lib/crowdb-chunkdb-client/`.
- [x] **DiskIO router**: discover owners and implement replica read fallback,
  parallel writes, and per-disk fsync. Files:
  `app/crowdb-chunkdb/src/conversion/io.rs`.
- [x] **Selection and encoding**: select immutable compatible runs and encode
  parity directly from mirror shards. Files:
  `app/crowdb-chunkdb/src/conversion.rs`.
- [x] **Chunkdb takeover handler**: claim abandoned or scanner-created tasks,
  read mirrors, allocate, encode, write, fsync, fenced-replace, retry ambiguity,
  and safely discard on definite failure. Files:
  `app/crowdb-chunkdb/src/conversion.rs`.
- [x] **Policy runner and throttle**: add bounded rotating background scans, concurrency,
  bandwidth limiting, and clean shutdown. Files:
  `app/crowdb-chunkdb/src/conversion.rs`.
- [x] **Metrics and server wiring**: expose atomic metrics plus RPC/HTTP manual
  triggers and start the runner. Files: `app/crowdb-chunkdb/src/metrics.rs`,
  `app/crowdb-chunkdb/src/main.rs`,
  `app/crowdb-chunkdb/src/service/chunkdb_rpc_service/`.

## Phase 3: Tests and Review

- [x] **Unit tests**: cover task key/value encoding, incremental EC, configuration,
  policy bounds, task retry, and metrics snapshots. Files:
  `app/crowdb-chunkdb/tests/conversion_test.rs`.
- [~] **Integration tests**: cover 24-to-3 replacement, active prefix append,
  task discovery/claim/takeover, failure cleanup, restart cleanup, deletion
  race, and bounded concurrency. Files:
  `app/crowdb-chunkdb/tests/conversion_test.rs`.
- [x] **Crash recovery E2E**: kill and restart chunkdb during an active claimed
  conversion; assert takeover, two authoritative EC layouts, data, and parity.
  Ambiguous publication and cleanup are covered through fenced lifecycle
  integration tests. Files: chunkdb and chunk-client E2E suites.
- [x] **Client E2E**: exercise single and batch management calls over real crowdb-rpc. Files:
  `lib/crowdb-chunkdb-client/tests/conversion_api_test.rs`.
- [x] **Small-write full E2E**: write real shared mirror strips, convert, verify
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
