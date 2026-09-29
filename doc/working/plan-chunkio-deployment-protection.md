<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk IO Deployment Protection Plan

Upstream: [R192](../backlog/R192-chunkio-deployment-protection.md), [chunk IO](../design/chunkio/design-crowdb-chunkio.md), [chunk placement](../design/chunkdb/design-crowdb-chunkdb.md), [KV](../design/kv/design-crowdb-kv.md).

Goal: make production a protected cluster of at least three nodes, retain writes and reads after one node fails, and expose single-node only as an explicit unprotected test profile using one 1 MiB mirror strip.

## Prerequisite

- [ ] **Finish protocol ownership**: complete R191's typed S3/Iceberg allocation and separate write policies before changing the shared strip engine; retain its separate working plan and current in-progress diff. Files: `doc/working/plan-access-storage-isolation.md`, protocol, chunk-client, access libraries.

## Protection contract

- [ ] **Mode configuration**: add an explicit production/test-single-node mode and validate the mode with KV membership and ChunkDB placement before accepting writes. No automatic transition between modes. Files: `app/crowdb-chunkdb/src/chunkdb_config.rs`, `app/crowdb-chunkdb/src/main.rs`, access/standalone startup config, container templates.
- [ ] **Allocation guard**: in test-single-node mode, admit only one-copy mirror strips of 1 MiB logical capacity and disable conversion/EC; in production, reject one-copy and layouts unable to survive any one node loss. Check initial allocation, append, repair, and conversion. Files: `app/crowdb-chunkdb/src/lifecycle/`, `app/crowdb-chunkdb/src/selector/`.

## Strip data path

- [ ] **Strip dispatch**: derive each strip's kind, data capacity, and geometry from persisted strip metadata. Have `ChunkWriter` delegate push/finish/abort and cross-boundary splitting through `StripWriter`, without assuming EC. Files: `lib/crowdb-chunk-client/src/chunk/{chunk_writer,strip,ec_strip_writer,mirror_strip_writer}.rs`.
- [ ] **Mirror writer**: replace the placeholder with durable mirror writes, including one-copy 1 MiB strips, partial blocks, cross-block inputs, error propagation, and retry/repair rules. Reuse physical write behavior with `writer/mirror_flow.rs` where it preserves small-write batching. Files: `lib/crowdb-chunk-client/src/chunk/`, `lib/crowdb-chunk-client/src/writer/`.
- [ ] **Read dispatch**: confirm mirror/EC strip readers use persisted geometry and implement their own failure recovery. Files: `lib/crowdb-chunk-client/src/chunk/{strip_reader,chunk_reader}.rs`.

## Failure and recovery

- [ ] **Three-node degraded operation**: retain three-voter KV membership after one node fails. Select a protected two-node degraded write layout, reject single-copy fallback, and repair/rebalance when the third node returns. Files: KV deployment configuration, `app/crowdb-chunkdb/src/selector/`, `app/crowdb-chunkdb/src/lifecycle/`.
- [ ] **Failure acceptance**: exercise loss of each node independently, read prior committed data, write/read new data on survivors, and verify repair after recovery. Files: integration tests and container/cluster E2E.

## Verification and cleanup

- [ ] **Focused tests**: mode validation, allocation guards, mirror/EC strip boundaries, data error, and degraded placement. Files: relevant crate `tests/`.
- [ ] **Gates and permanent design**: run `pixi run rs-fmt-check`, `pixi run rs-lint`, affected tests and container E2E, then update chunk IO, ChunkDB, and KV design sections.
- [ ] **Final cleanup**: remove R192, its backlog entry, and this plan in the final cleanup commit after acceptance passes.

## Files

- Runtime and policy: `app/crowdb-chunkdb/`, access runtime configuration, `container/single-node-container/`.
- Physical data path: `lib/crowdb-chunk-client/`.
- KV quorum and membership: `lib/crowdb-kv/` and deployment config.
- Documentation: `doc/design/chunkio/`, `doc/design/chunkdb/`, `doc/design/kv/`.

## Tests

- Unit: configuration and per-strip capacity/geometry.
- Integration: allocation and recovery constraints, typed small/large writes, data errors.
- E2E: single-node test image and three-node one-node-out read/write/repair.
