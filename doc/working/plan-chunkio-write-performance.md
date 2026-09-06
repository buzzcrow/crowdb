<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk IO Write Flow and Performance Plan

Upstream: [R135](../backlog/R135-chunkio-end-to-end-performance.md) and
[working design](design-chunkio-write-path-review.md).

Goal: make the large-write API and simple E2E flow correct, then measure and
refine it through a three-node `NullDisk` benchmark.

## Review and Correctness

- [x] **Diagnose simple write**: reproduce the isolated E2E test and locate the
  first pending operation. Files: `lib/crowdb-chunk-client/tests/`,
  `lib/crowdb-chunk-client/src/`, `lib/crowdb-test-harness/`.
- [x] **Correct completion ordering**: sequence fsync after data/parity writes
  and add focused tests. Files: `lib/crowdb-chunk-client/src/chunk/`,
  `lib/crowdb-chunk-client/tests/`.
- [x] **Add public client API**: own discovery, routing, preparation, execution,
  and results in the library. Files: `lib/crowdb-chunk-client/src/client.rs`,
  `lib/crowdb-chunk-client/src/client/`, `lib/crowdb-chunk-client/src/lib.rs`.
- [x] **Pass simple E2E**: use the public API for multi-strip and chunk-rotation
  writes on one real stack. Files: `lib/crowdb-chunk-client/tests/`,
  `lib/crowdb-test-harness/`.

## Distributed Flow

- [x] **Add DiskIO routing**: publish immutable disk-owner snapshots and bounded
  endpoint pools. Files: `lib/crowdb-chunk-client/src/disk_io/`,
  `lib/crowdb-kv-client/src/`.
- [x] **Deploy three DiskIO services**: extend combined local deployment,
  readiness, persistence, destroy, and logs. Files:
  `lib/crowdb-console-shared/src/`, `app/crowdb-cli/src/commands/cluster.rs`.
- [x] **Pass distributed E2E**: write an EC strip across three disk groups and
  verify routing and accounting. Files: `lib/crowdb-chunk-client/tests/`.

## Performance

- [x] **Remove payload copy**: preserve owned payload through DiskIO RPC
  completion. Files: `lib/crowdb-diskio-client/src/`,
  `lib/crowdb-chunk-client/src/disk_io/`.
- [x] **Refine critical path**: measure and apply bounded write/EC/finalization
  overlap while preserving memory and ordering. Files:
  `lib/crowdb-chunk-client/src/`, `lib/crowdb-chunk-client/tests/`.
- [x] **Add library benchmark runner**: implement bounded deterministic large
  writes and aggregate results. Files: `lib/crowdb-chunk-client/src/benchmark.rs`.
- [x] **Add thin CLI verb**: map arguments and format the library result. Files:
  `app/crowdb-cli/src/commands/bench/`, `app/crowdb-cli/Cargo.toml`.
- [x] **Add regression sentinel**: run and retain the three-node matrix and
  `bw_mib` samples. Files: `tools/bench-chunkio-write-regression.sh`.

## Documentation and Gates

- [x] **Run affected tests**: unit, simple E2E, distributed E2E, CLI integration,
  and sentinel separately through Pixi.
- [ ] **Fold design**: update permanent ChunkIO, ChunkDB, protocol, and DiskIO
  designs and remove
  temporary artifacts and R135.
- [ ] **Run final gates**: format, lint, and full ordered local CI through Pixi.

## Write Path Enhancement

- [x] **Repair DiskIO activation**: wake a sleeping poll thread when its first
  SQE becomes pending, coalesce wakeups, register valid dummy-disk descriptors,
  and add focused C++ tests. Files: `lib/crowdb-common/cpp/src/diskio_uring.cpp`,
  `lib/crowdb-common/cpp/include/crowdb-common/diskio_uring.h`,
  `lib/crowdb-common/cpp/tests/diskio_uring_test.cpp`,
  `app/crowdb-diskio/src/dio_main.cpp`, `app/crowdb-diskio/tests/`.
- [x] **Simplify durable writes**: define one DiskIO write-completion contract,
  remove chunk-client fsync scheduling, keep production BlockDisk synchronous,
  and make dummy benchmarks explicitly non-durable. Files:
  `lib/crowdb-chunk-client/src/`, `lib/crowdb-diskio-client/src/`,
  `app/crowdb-diskio/src/`, `lib/crowdb-chunk-client/tests/`,
  `lib/crowdb-diskio-client/tests/`.
- [x] **Bound data-write overlap**: feed EC before waiting for independent
  writes, retain bounded completions in the strip/chunk owner, and safely drain
  submitted work on seal and abort. Files: `lib/crowdb-chunk-client/src/chunk/`,
  `lib/crowdb-chunk-client/src/config.rs`, `lib/crowdb-chunk-client/tests/`.
- [x] **Simplify preparation ownership**: replace timer/atomic strip polling
  with consumption-driven bounded preparation, consolidate chunk/strip depth
  controls, and continuously prepare unknown-size chunks. Files:
  `lib/crowdb-chunk-client/src/chunk/`,
  `lib/crowdb-chunk-client/src/writer/large_async_object.rs`,
  `lib/crowdb-chunk-client/src/config.rs`, `lib/crowdb-chunk-client/tests/`.
- [x] **Add incremental chunk append**: add monotonic `modify_ts`, send the
  observed revision on append, return only new strips on a match, and return
  full current chunk information on mismatch. Files: `lib/crowdb-protocol/`,
  `lib/crowdb-chunkdb/`, `lib/crowdb-chunkdb-client/`,
  `lib/crowdb-chunk-client/`, and affected tests.
- [x] **Split topology refresh**: expose independent ChunkDB and DiskIO refresh
  operations and test their failure boundaries. Files:
  `lib/crowdb-chunk-client/src/client.rs`, `lib/crowdb-chunkdb-client/`,
  `lib/crowdb-chunk-client/tests/`.
- [ ] **Rerun performance matrix**: run the retained one- and four-writer
  NullDisk sentinel without descriptor warnings, compare all client/service
  metrics, and update the working analysis with the new bottleneck. Files:
  `tools/bench-chunkio-write-regression.sh`,
  `doc/working/design-chunkio-write-path-review.md`.

## Files

- `lib/crowdb-chunk-client/`
- `lib/crowdb-diskio-client/`
- `lib/crowdb-test-harness/`
- `lib/crowdb-console-shared/`
- `app/crowdb-cli/`
- `tools/bench-chunkio-write-regression.sh`
- `doc/design/chunkio/design-crowdb-chunkio.md`
- `doc/design/diskio/design-crowdb-diskio.md`
- `doc/design/chunkdb/design-crowdb-chunkdb.md`
- `doc/design/protocol/design-crowdb-protocol.md`

## Tests

- Unit: idle activation, wake coalescing, dummy descriptor registration,
  completion ordering, bounded write/preparation depth, incremental append,
  preparation stalls, and result aggregation.
- Integration: durable versus non-durable DiskIO writes, routing ownership and
  refresh errors, append revision mismatch, and CLI adapter.
- E2E: simple stack, chunk rotation, distributed EC write.
- Regression: `tools/bench-chunkio-write-regression.sh`.
