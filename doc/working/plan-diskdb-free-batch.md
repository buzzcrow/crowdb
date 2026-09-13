<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Durable Concurrent-Free Coalescing Plan

Upstream requirement: `doc/backlog/R79-diskdb-free-batch.md`

Root designs: `doc/design/diskdb/design-crowdb-diskdb-zone-management.md`
and `doc/design/diskdb/design-crowdb-diskdb.md`

Goal: coalesce concurrent free RPCs into fewer bounded KV proposals while
retaining persist-before-success and the lock-free, conservative bitmap model.

## Phase 1 — Separate Free Preparation and Commit

- [ ] **Extract deduplicated free preparation**: validate segment disk IDs and
  build immutable `FreeBlockValue` records plus post-persist identities without
  touching tentative state, zone backlog, or metrics. Symbols:
  `alloc::free_blocks`, `FreeBatchResult`. Files:
  `app/crowdb-diskdb/src/model/alloc.rs`.
- [ ] **Extract post-persist accounting**: apply tentative-cache removal,
  `DdbDiskGroup::free_block` backlog increment, and per-disk metrics once for
  each distinct prepared segment only after durable success. Files:
  `app/crowdb-diskdb/src/model/{alloc,disk_group,disk}.rs`.
- [ ] **Retain the direct path**: compose prepare →
  `DdbKvClient::persist_free_batch` → post-persist as the batching-disabled
  behavior and prove its existing response/error semantics. Files:
  `app/crowdb-diskdb/src/model/alloc.rs`, existing diskdb allocation/recovery
  tests.

## Phase 2 — Lock-Free Coalescer

- [ ] **Add queue dependency and types**: add a focused lock-free MPSC queue
  dependency, define `PendingFreeRequest`, captured bind/group/records,
  completion sender, close state, queue-depth counter, and atomic drainer flag.
  Files: `app/crowdb-diskdb/Cargo.toml`,
  `app/crowdb-diskdb/src/persistence/{mod,free_batch}.rs`,
  `app/crowdb-diskdb/src/lib.rs`.
- [ ] **Implement submit and ownership CAS**: enqueue without a mutex, reject
  after close, acquire one drainer, and await per-request completion. Ensure
  cancellation of an RPC waiter does not cancel persistence already admitted.
  Files: `app/crowdb-diskdb/src/persistence/free_batch.rs`.
- [ ] **Build bounded same-bind drains**: pop whole requests, group only equal
  captured binds, cap total records at `free_flush_max_batch`, and persist an
  oversized request alone. Retain deferred different-bind work without
  reordering a single request's completion. Files: free batch module.
- [ ] **Implement lost-wakeup-safe handoff**: drain queued work while one write
  is in flight, release ownership only after an empty check, and reacquire or
  hand off when enqueue races the release. Files: free batch module and
  deterministic concurrency tests.
- [ ] **Resolve exact outcomes**: on success run post-persist accounting and
  resolve covered requests; on error resolve all with cloned/classified errors
  and never re-enqueue automatically. Files: free batch module,
  `app/crowdb-diskdb/src/model/alloc.rs`.

## Phase 3 — Service, Configuration, and Lifecycle

- [ ] **Own one coalescer in the RPC service**: construct it with the shared KV
  client and config handle, and route `handle_free` through direct or coalesced
  submission based on the captured dynamic toggle. Files:
  `app/crowdb-diskdb/src/service/diskdb_rpc_service/{service,mutations}.rs`,
  `app/crowdb-diskdb/src/main.rs`.
- [ ] **Correct configuration semantics**: retain field names/defaults, change
  comments and validation to maximum-record-batch semantics, and snapshot the
  toggle/limit for each admitted request. Files:
  `app/crowdb-diskdb/src/ddb_config.rs`, config TOML, config tests.
- [ ] **Order shutdown**: close mutation admission, close the free coalescer,
  await queue/in-flight zero, then stop RPC and runtime-owned services; make
  repeated close idempotent. Files: `app/crowdb-diskdb/src/main.rs`, service
  lifecycle modules, lifecycle tests.
- [ ] **Add coalescing metrics**: register request/record input, KV batch/record
  output, queue depth, oversize, failure, drain latency, and derived ratio
  fields without per-record hot-path locks. Files:
  `app/crowdb-diskdb/src/metrics.rs` and reporting/tests.

## Phase 4 — Verification and Evidence

- [ ] **Test concurrency boundaries**: use held KV completions/barriers to
  cover immediate singleton flush, concurrent aggregation, exact cap, bind
  separation, oversized atomic request, enqueue/release race, and cancelled
  waiter. Files: `app/crowdb-diskdb/tests/free_batch_test.rs` and test-util KV
  hooks.
- [ ] **Test failure and accounting**: inject durable failure and outcome
  unknown, assert all covered responses fail with no accounting, then retry and
  assert exactly-once facts/backlog/metrics. Files: free batch and diskdb E2E
  tests.
- [ ] **Test dynamic config and shutdown**: toggle batching around queued work,
  close with queued/in-flight requests, race rejected admission, and assert an
  empty terminal state. Files: `app/crowdb-diskdb/tests/{config_reload,lifecycle}_test.rs`.
- [ ] **Add benchmark case**: drive concurrent frees to one bound group with
  batching off/on; compare identical facts, errors, KV proposal count,
  coalescing ratio, and latency. Files: `tools/bench-diskdb-regression.sh`,
  `doc/design/diskdb/diskdb-allocate-flow-analysis.md` if it is the selected
  permanent evidence location.
- [ ] **Reconcile permanent docs**: replace minimum-threshold/shutdown-repair
  language with immediate opportunistic coalescing and durable response
  semantics. Files: both diskdb root designs and user/config documentation.
- [ ] **Run focused gates**: execute every R79 verification command and fix
  ordinary failures before closure. Files: affected workspace.

## Consolidated File List

- `app/crowdb-diskdb/Cargo.toml`
- `app/crowdb-diskdb/src/persistence/{mod,free_batch}.rs`
- `app/crowdb-diskdb/src/model/{alloc,disk_group,disk}.rs`
- `app/crowdb-diskdb/src/service/diskdb_rpc_service/{service,mutations}.rs`
- `app/crowdb-diskdb/src/{ddb_config,metrics,main,lib}.rs`
- `app/crowdb-diskdb/conf/crowdb_diskdb_config.toml`
- `app/crowdb-diskdb/tests/{free_batch,diskdb_e2e,config_reload,lifecycle}_test.rs`
- `tools/bench-diskdb-regression.sh`
- `doc/design/diskdb/{design-crowdb-diskdb,design-crowdb-diskdb-zone-management}.md`
- selected diskdb benchmark evidence document

## Tests

Unit tests:

- Deduplication/preparation and post-persist accounting helpers.
- Batch cap, same-bind grouping, oversize, error fan-out, and handoff helpers.
- Config defaults, validation, and dynamic snapshots.

Integration tests:

- Deterministic singleton/concurrent/failure/cancellation queue behavior.
- Direct versus coalesced durable free facts and exactly-once accounting.
- Dynamic reload and graceful close ordering.

E2E tests:

- Concurrent-free benchmark showing fewer KV proposals with identical durable
  facts and zero errors.
