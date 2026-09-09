<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Batched Strip Reservation and Incremental EC Plan

Upstream: [working design](design-small-write-reservation-ec.md),
[R113](../backlog/R113-chunkio-batch-strip-allocation.md),
[R136](../backlog/R136-chunkio-reserve-confirm-strip-flow.md), and
[R137](../backlog/R137-chunkio-incremental-ec-conversion.md).

Goal: land the three requirements in dependency order with same-machine
small-write performance evidence and no data-path regression.

## Phase 0: Evidence and Design

- [x] **Baseline and architecture audit**: retain four-endpoint small-write
  baseline, resolve ownership/fencing, and create working artifacts. Files:
  `bench-log/r113-r136-r137-baseline-20260910/`, `doc/working/`.
- [x] **Protocol inventory**: enumerate generated FlatBuffer/Rust/client/server
  touch points and test seams. Files: `lib/crowdb-protocol/`, client crates,
  ChunkDB and DiskDB services.

## Phase 1: Complete R113

- [x] **Concurrent ordered batch allocation**: allocate N strips concurrently
  from one snapshot with all-or-rollback behavior. Files:
  `app/crowdb-chunkdb/src/allocator.rs`, `lifecycle/handler.rs`.
- [x] **Large-writer batch prefetch**: request bounded strip batches and preserve
  stale-revision behavior. Files: `lib/crowdb-chunk-client/src/chunk/chunk_writer.rs`.
- [x] **R113 tests and perf**: run unit/integration/E2E cases and the four-endpoint
  perf comparison, then commit R113. Files: affected `tests/`, `bench-log/`.

## Phase 2: Implement R136

- [~] **Reservation protocol and values**: add record/state and reserve, consume,
  confirm, cancel, renew messages through generated and owned APIs. Files:
  `lib/crowdb-protocol/`, `lib/crowdb-chunkdb-client/`.
- [ ] **ChunkDB reservation lifecycle**: implement fencing, idempotence, ordered
  confirmation, cleanup intents, and recovery admission. Files:
  `app/crowdb-chunkdb/src/lifecycle/`, `service/`, `task/`.
- [ ] **Client reservation prefetch**: consume a bounded ready queue, run confirm
  and refill on the metadata chain, and retain attached fallback. Files:
  `lib/crowdb-chunk-client/src/writer/`.
- [ ] **R136 recovery and E2E tests**: cover retries, stale lease, writer/service
  death, seal, and zero leaks. Files: affected crate `tests/`.
- [ ] **R136 perf**: compare the four endpoints and disable/fix the new default on
  material TPS/p99 regression before committing. Files: `bench-log/`.

## Phase 3: Implement R137

- [ ] **Special group selector/allocation**: atomically allocate eight mirror
  triples plus parity and persist candidate metadata. Files:
  `app/crowdb-chunkdb/src/selector/`, `allocator.rs`, protocol types.
- [ ] **Incremental parity lifetime**: retain only four parity accumulators and
  write only parity after the eighth input. Files:
  `lib/crowdb-chunk-client/src/writer/small_conversion.rs`.
- [ ] **Optimal publication and task fallback**: reselect survivors, fence EC
  publication/cleanup, and retain a retryable task when not optimal. Files:
  `app/crowdb-chunkdb/src/lifecycle/`, `task/`, `conversion.rs`.
- [ ] **R137 failure and E2E tests**: cover early tails, topology changes,
  restart, memory bounds, payload accounting, and reconstruction. Files:
  affected crate `tests/`.
- [ ] **R137 perf**: compare four endpoints, then run the complete ten-case
  small-write regression matrix. Files: `bench-log/`.

## Phase 4: Review, Formal Design, and Cleanup

- [ ] **Affected gates**: run every affected test task separately, format,
  clippy, C++ gates if changed, and full `test-suite`.
- [ ] **Review**: run `/review`, fix correctness and hot-path findings, and rerun
  affected gates.
- [ ] **Formal design**: fold final behavior and benchmark evidence into indexed
  ChunkIO/ChunkDB/DiskDB design documents; delete this draft.
- [ ] **Requirement cleanup**: delete R113/R136/R137 and backlog entries, delete
  the completed plan, and commit cleanup separately.

## Consolidated Files

- Protocol: `lib/crowdb-protocol/src/fbs/`, `lib/crowdb-protocol/src/types/`.
- ChunkDB: `app/crowdb-chunkdb/src/{allocator,selector,lifecycle,service,task,conversion}*`.
- DiskDB: `app/crowdb-diskdb/src/{model,scanner,service}*`.
- Clients: `lib/crowdb-chunkdb-client/`, `lib/crowdb-chunk-client/src/`.
- Tests: affected crates' `tests/`, `tools/bench-chunkio-small-write-regression.sh`.
- Docs: indexed component designs and temporary working artifacts.

## Tests

- Unit: selector scoring, state transitions, idempotence, stale fencing, parity
  lifetime, and batch-size calculation.
- Integration: allocation rollback, RPC round trips, commit/free ownership, task
  recovery, and topology re-selection.
- E2E: real ChunkDB+DiskDB+DiskIO writes, crash boundaries, early seal, reads,
  reconstruction, leak checks, and benchmark accounting.
