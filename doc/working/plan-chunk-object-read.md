<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk Object Read Flow Plan

Upstream: [R107](../backlog/R107-chunkdb-chunk-read-flow.md),
[working design](design-chunk-object-read.md), and
[chunk IO design](../design/chunkio/design-crowdb-chunkio.md).

Goal: reconstruct full, ranged, and streamed objects through current chunk
layouts with mirror fallback, EC recovery, and bounded layout validity.

## Phase 1: Physical read foundation

- [x] **DiskIO read seam**: add validated arbitrary-offset reads and production routing. Files: `lib/crowdb-chunk-client/src/disk_io/disk_writer.rs`, `lib/crowdb-chunk-client/src/disk_io/routing.rs`, `lib/crowdb-chunk-client/src/client.rs`.
- [x] **Strip reconstruction**: implement mirror fallback, EC fast reads, EC recovery, and geometry validation. Files: `lib/crowdb-chunk-client/src/chunk/strip_reader.rs`, `lib/crowdb-chunk-client/src/error.rs`.
- [x] **Focused physical tests**: cover mirror and EC success/failure matrices. Files: `lib/crowdb-chunk-client/tests/chunk_reader_test.rs`.

## Phase 2: Object API

- [x] **Location mapping**: implement validation, explicit strip interval mapping, parallel assembly, and layout-expiry retry. Files: `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`.
- [x] **Bounded stream**: expose windowed `ChunkReadStream` and policy. Files: `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`, `lib/crowdb-chunk-client/src/config.rs`.
- [x] **Client integration**: expose full/range/stream methods and public types. Files: `lib/crowdb-chunk-client/src/client.rs`, `lib/crowdb-chunk-client/src/chunk.rs`, `lib/crowdb-chunk-client/src/lib.rs`.
- [x] **Focused object tests**: cover invalid mappings, partial ranges, active durable cursors, retry fencing, and stream bounds. Files: `lib/crowdb-chunk-client/tests/chunk_reader_test.rs`.

## Phase 3: Core E2E

- [x] **Large-write reads**: write/read full, partial-tail, rotated, cross-chunk range, and bounded stream through real services. Files: `lib/crowdb-chunk-client/tests/chunk_reader_e2e.rs`.
- [x] **Small-write reads**: read shared objects plus converted EC and mirror-tail locations through real services. Files: `lib/crowdb-chunk-client/tests/chunk_reader_e2e.rs`.
- [x] **Failure reads**: validate mirror failure and EC decode/data-loss behavior using only I/O failure injection around real services; mirror fallback is covered by the focused strip test because the single-node fixture cannot allocate three node-distinct replicas. Files: `lib/crowdb-chunk-client/tests/chunk_reader_e2e.rs`, `lib/crowdb-chunk-client/tests/chunk_reader_test.rs`.

## Phase 4: Review and completion

- [~] **Affected gates**: run each chunk-reader unit/E2E task, existing large/small E2E, fmt, and affected clippy separately.
- [ ] **Review**: inspect correctness, crash/layout fencing, and hot-path costs; resolve findings.
- [ ] **Permanent design**: fold the accepted design into chunk IO documentation and index; remove working design.
- [ ] **Cleanup**: remove R107 detail/index entry and completed plan in a separate commit.
- [ ] **Full pre-push gate**: run workspace fmt, `rs-lint`, and `test-suite`; report confirmed baseline failures.

## Consolidated Files

- `lib/crowdb-chunk-client/src/{client,config,error,lib}.rs`
- `lib/crowdb-chunk-client/src/chunk/{chunk_reader,strip_reader}.rs`
- `lib/crowdb-chunk-client/src/disk_io/{disk_writer,routing}.rs`
- `lib/crowdb-chunk-client/tests/{chunk_reader_test,chunk_reader_e2e}.rs`
- `doc/design/chunkio/design-crowdb-chunkio*.md`
- `doc/doc_index.md`
- `doc/backlog/{backlog,R107-chunkdb-chunk-read-flow}.md`
- `doc/working/{design,plan}-chunk-object-read.md`

## Tests

- Unit/integration: `pixi run -- cargo test -p crowdb-chunk-client --test chunk_reader_test`
- E2E: `pixi run clean-env && pixi run -- cargo test -p crowdb-chunk-client --test chunk_reader_e2e`
- Regression E2E: existing large- and small-object writer E2E binaries.
- Gates: workspace fmt, `pixi run rs-lint`, and `pixi run test-suite`.
