<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Small-Write IO Repair Plan

Upstream: [requirement](../backlog/R112-chunkio-small-write-io-error-handling.md),
[working design](design-small-write-io-repair.md), and
[small-writer design](../design/chunkio/design-crowdb-chunkio-small-object-writer.md).

Goal: repair failed shared-write mirror replicas in place and publish object
locations only after complete, fenced mirror durability.

## Phase 1: Durable Metadata Primitives

- [x] **Protocol model**: add monotonic strip sequence, cleanup intent,
  layout-validity, allocate-replacement, and replace-range messages. Files:
  `lib/crowdb-protocol/src/types/chunkdb.rs`,
  `lib/crowdb-protocol/src/fbs/chunkdb.fbs`,
  `lib/crowdb-protocol/src/fbs/msg_type.fbs`.
- [x] **Range transaction**: implement validation, idempotency, set-difference
  commit, durable cleanup intent, and deferred cleanup. Files:
  `app/crowdb-chunkdb/src/lifecycle/handler.rs`, lifecycle support modules.
- [x] **Replacement placement**: allocate one geometry-compatible tentative
  mirror segment with surviving/failing exclusions. Files:
  `app/crowdb-chunkdb/src/allocator.rs`, selector and lifecycle modules.
- [x] **RPC/client wiring**: expose both operations and preserve typed conflict.
  Files: `app/crowdb-chunkdb/src/service/`,
  `lib/crowdb-chunkdb-client/src/`.
- [x] **Lifecycle tests**: verify N-to-M capacity fence, monotonic sequence,
  idempotent retry, conflict, segment set differences, and cleanup recovery.
  Files: `app/crowdb-chunkdb/tests/`.

## Phase 2: Small-Write Repair

- [x] **Typed failures and exclusions**: preserve failed disk identity and add
  shared TTL negative list. Files: `lib/crowdb-chunk-client/src/error.rs`,
  `negative_list.rs`, `disk_io/`, `client.rs`.
- [x] **Repair seams and policy**: extend the lifecycle seam with replacement operations and add retry/TTL/grace
  configuration, and budget validation. Files:
  `lib/crowdb-chunk-client/src/traits.rs`, `config.rs`, `writer/small_pool.rs`.
- [x] **Shadow and ledger**: retain a full open-block image plus batch metadata
  through durable completion. Files:
  `lib/crowdb-chunk-client/src/writer/small_pipeline.rs`.
- [x] **Replica repair state machine**: allocate, rewrite full shadow, fenced
  swap, retry, and update current chunk without rotation. Files:
  `lib/crowdb-chunk-client/src/writer/small_pipeline.rs`.
- [x] **Failure/retirement boundary**: fail accepted work exactly once and
  restore minimum pipelines after exhaustion, including drain and rotation.
  Files: `lib/crowdb-chunk-client/src/writer/small_manager.rs`,
  `writer/small_pipeline.rs`.
- [x] **Metrics**: add lock-free repair, exclusion, latency, shadow, exhaustion,
  and pipeline replacement metrics. Files:
  `lib/crowdb-chunk-client/src/metrics.rs`.
- [x] **Focused tests**: cover shadow lifetime, new/used strip repair, two
  failures, each exhaustion class, drain, rotation, and metrics. Files:
  `lib/crowdb-chunk-client/tests/small_write_error_test.rs`.

## Phase 3: Real End-to-End Coverage

- [x] **Fault-capable fixture**: decorate the real routed DiskIO writer with
  deterministic selected-call failures while keeping all successful IO real.
  Files: `lib/crowdb-chunk-client/tests/small_object_writer_e2e.rs`.
- [x] **Repair E2E**: verify real allocation, disk write, metadata swap, data
  integrity, and metrics after one failed replica. Files:
  `lib/crowdb-chunk-client/tests/small_object_writer_e2e.rs`.
- [x] **Boundary E2E**: verify predecessor readability on replacement-chunk
  exhaustion and repair-before-drain. Files:
  `lib/crowdb-chunk-client/tests/small_object_writer_e2e.rs`.

## Phase 4: Documentation and Cleanup

- [x] **Affected gates**: run protocol, chunkdb-client, chunkdb, and
  chunk-client tests separately; run fmt and clippy.
- [x] **Implementation commit**: commit code, tests, working design, and plan.
- [ ] **Formal design**: fold verified behavior into chunkdb and small-writer
  design, updating `doc/doc_index.md` only if scope changes.
- [ ] **Requirement cleanup**: remove R112 detail/index entry and completed
  working files in a separate commit.
- [ ] **Full gate**: run format, lint, and `test-suite`; report confirmed
  unrelated failures.

## Consolidated Files

- `lib/crowdb-protocol/src/types/chunkdb.rs`
- `lib/crowdb-protocol/src/fbs/chunkdb.fbs`
- `lib/crowdb-protocol/src/fbs/msg_type.fbs`
- `lib/crowdb-chunkdb-client/src/`
- `app/crowdb-chunkdb/src/allocator.rs`
- `app/crowdb-chunkdb/src/lifecycle/`
- `app/crowdb-chunkdb/src/service/`
- `lib/crowdb-chunk-client/src/`
- `app/crowdb-chunkdb/tests/`
- `lib/crowdb-chunk-client/tests/`
- `doc/design/chunkdb/design-crowdb-chunkdb.md`
- `doc/design/chunkio/design-crowdb-chunkio-small-object-writer.md`

## Tests

- Unit: protocol round trips, negative-list expiry, policy budget, shadow
  lifetime, metrics.
- Integration: lifecycle range transaction and every small repair failure
  class.
- E2E: real multi-service replica replacement, readback, rotation, and drain.
- Gates: `pixi run -- cargo fmt --all -- --check`, `pixi run rs-lint`, and
  `pixi run test-suite`.
