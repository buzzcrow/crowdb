<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Large-Write Error Handling Plan

Upstream: [working design](design-large-write-error-handling.md),
[requirement](../backlog/R110-chunkdb-chunkio-error-handling.md), and
[chunk IO design](../design/chunkio/design-crowdb-chunkio.md).

Goal: make large EC writes replace individual failed durable shard writes and
never acknowledge an object whose required redundancy is incomplete.

## Phase 1: Shared Policy and State

- [x] **Failed-disk backoff**: extend the lock-free failed-disk snapshot with
  repeated-failure TTL backoff and deterministic tests. Files:
  `lib/crowdb-chunk-client/src/negative_list.rs`,
  `lib/crowdb-chunk-client/tests/negative_list_test.rs`.
- [x] **Client-wide wiring**: share one failed-disk list across small and large
  writers and add validated repair attempts. Files:
  `lib/crowdb-chunk-client/src/client.rs`,
  `lib/crowdb-chunk-client/src/config.rs`,
  `lib/crowdb-chunk-client/src/writer/small_pool.rs`,
  `lib/crowdb-chunk-client/src/writer/small_manager.rs`,
  `lib/crowdb-chunk-client/src/writer/large_async_object.rs`,
  `lib/crowdb-chunk-client/src/writer/large_object.rs`.

## Phase 2: Large-Write Recovery

- [x] **Segment completion**: implement bounded placement-safe replacement,
  deterministic fenced publication, refresh/conflict handling, and tentative
  cleanup. Files: `lib/crowdb-chunk-client/src/chunk/segment_writer.rs`,
  `lib/crowdb-chunk-client/src/chunk.rs`.
- [x] **Data/parity integration**: route both data and parity durable writes
  through segment completion while preserving bounded concurrency. Files:
  `lib/crowdb-chunk-client/src/chunk/ec_strip_writer.rs`,
  `lib/crowdb-chunk-client/src/chunk/parity_writer.rs`,
  `lib/crowdb-chunk-client/src/chunk/chunk_writer.rs`.
- [x] **Metrics**: expose lock-free large-write replacement accounting. Files:
  `lib/crowdb-chunk-client/src/metrics.rs`,
  `lib/crowdb-chunk-client/src/client.rs`.

## Phase 3: Verification

- [x] **Focused failure assertions**: cover data failure, parity failure,
  persistent exhaustion, exact identity retention, and lock-free backoff.
  Files: `lib/crowdb-chunk-client/tests/large_object_writer_e2e.rs`,
  `lib/crowdb-chunk-client/tests/negative_list_test.rs`.
- [x] **Real-process E2E**: cover successful data/parity replacement,
  read-back/parity, and failure cleanup through the full stack. Files:
  `lib/crowdb-chunk-client/tests/large_object_writer_e2e.rs`,
  `lib/crowdb-chunk-client/tests/common/e2e_stack.rs`.
- [x] **Affected gates**: run each new test target, all chunk-client tests,
  ChunkDB replacement/full-stack tests, protocol tests, fmt, and clippy.

## Phase 4: Review and Cleanup

- [x] **Correctness review**: review crash points, ambiguous RPC outcomes,
  hot-path allocations, and abort cleanup against the working design.
- [ ] **Implementation commit**: commit code, tests, design, and plan with a
  single-line subject.
- [ ] **Formal design**: fold current behavior into the permanent chunk IO and
  chunk-task designs; remove temporary wording.
- [ ] **Requirement cleanup**: delete the R110 backlog detail and entry plus
  both working documents, update the documentation index if its scope changes,
  and commit cleanup separately.
- [ ] **Full gate**: run `pixi run -- cargo fmt --all -- --check`,
  `pixi run rs-lint`, and `pixi run test-suite`.

## Consolidated Files

- `lib/crowdb-chunk-client/src/{client,config,metrics,negative_list}.rs`
- `lib/crowdb-chunk-client/src/chunk.rs`
- `lib/crowdb-chunk-client/src/chunk/{chunk_writer,ec_strip_writer,parity_writer,segment_writer}.rs`
- `app/crowdb-chunkdb/src/lifecycle/handler.rs`
- `lib/crowdb-chunk-client/src/writer/{large_async_object,large_object,small_manager,small_pool}.rs`
- `lib/crowdb-chunk-client/tests/{negative_list_test,large_object_writer_e2e}.rs`
- `lib/crowdb-chunk-client/tests/common/e2e_stack.rs`
- `doc/design/chunkio/design-crowdb-chunkio.md`
- `doc/design/chunkdb/design-crowdb-chunkdb-mirror-to-ec.md`
- `doc/backlog/{backlog.md,R110-chunkdb-chunkio-error-handling.md}`
- `doc/working/{design-large-write-error-handling,plan-large-write-error-handling}.md`

## Tests

- Unit: failed-disk expiry/backoff and policy validation.
- Integration: data/parity replacement, exhaustion cleanup, revision race,
  identity preservation, existing large/small writers and reader.
- E2E: full-stack replacement/read-back/parity and persistent-failure cleanup.
