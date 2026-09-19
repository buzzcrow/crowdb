<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 CLI and Memory Benchmark Plan

Upstream: [R173](../backlog/R173-s3-console-cluster-cli.md)

Goal: finish the disk-backed persistent CLI path and a bounded memory-backed S3
benchmark without duplicating S3 or benchmark behavior in the CLI.

## Phase 1: Persistent cluster and CRUD

- [x] **Audit landed behavior**: confirm the permanent design, shared lifecycle,
  persisted record, command surface, and current tests. Files:
  `doc/design/console/design-crowdb-console.md`, shared S3 ops, CLI S3 adapter.
- [x] **Make lifecycle transactional**: track invocation-owned processes,
  roll them back on failed first start, publish the marker only after access is
  ready, and make repeated stop/restart truthful. Files:
  `lib/crowdb-console-shared/src/ops/s3.rs` and tests.
- [x] **Complete data operations**: add inclusive single-range get, keep URL and
  query construction shared, stream exact file/stdin/stdout bytes, and cover
  S3 error passthrough and pagination. Files: shared S3 ops, CLI S3 adapter,
  shared and CLI tests.
- [x] **Prove durable restart**: real-process fresh start, CRUD/list, complete
  stop, same-directory restart, and post-restart byte read. Files: S3
  mini-cluster E2E tests.

## Phase 2: Memory benchmark model

- [x] **Reuse benchmark primitives**: identify the common result envelope,
  latency recorder, bounded runner, deterministic RNG, and output writer; add
  only S3-specific request/result fields in shared code. Files: console shared
  benchmark modules and CLI bench adapters.
- [x] **Add memory benchmark fixture**: deploy a bounded memory-backed local
  stack with explicit per-component backing, memory budget, readiness, and
  invocation cleanup. Keep chunk-KV readable metadata on memory storage and
  record the correctness scope. Files: shared S3 ops and cluster deployer.
- [x] **Implement workloads**: write, read, range-read, list, and deterministic
  weighted mix with warm-up, duration/operation bounds, concurrency, prepared
  datasets, validation, per-kind counters, and owned cleanup. Files: shared S3
  benchmark module and tests.
- [x] **Expose CLI**: add `bench s3` clap verbs as thin adapters and render the
  common JSON result. Files: `app/crowdb-cli/src/commands/bench/` and CLI tests.
- [x] **Run bounded benchmark acceptance**: execute all five workloads against
  the memory fixture, verify request-path validation and explicit budget
  failure, then retain a reproducible result artifact outside source control.

## Phase 3: Gates and cleanup

- [~] **Run required gates**: run focused tests, access-server S3 tests, boto3
  E2E, fmt, and clippy separately. Stop a single test investigation after ten
  minutes and record a confirmed unresolved failure under `Blocked`.
- [ ] **Close the requirement**: update permanent design for the final memory
  benchmark contract, remove the backlog entry and this plan, and commit the
  verified cleanup.

## Files

- `lib/crowdb-console-shared/src/ops/s3.rs`
- `lib/crowdb-console-shared/src/bench/`
- `lib/crowdb-console-shared/tests/s3_mini_cluster_test.rs`
- `app/crowdb-cli/src/commands/s3.rs`
- `app/crowdb-cli/src/commands/bench/`
- `app/crowdb-cli/tests/`
- `doc/design/console/design-crowdb-console.md`

## Tests

- Unit: location validation, range construction, mix parsing and deterministic
  selection, budget arithmetic, and result aggregation.
- Integration: CLI parsing/rendering, S3 error passthrough, workload validation,
  and invocation rollback.
- E2E: file-backed durability and memory-backed benchmark workloads.
