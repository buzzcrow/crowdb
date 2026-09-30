<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk IO Deployment Protection Plan

Upstream: [R192](../backlog/R192-chunkio-deployment-protection.md), [chunk IO](../design/chunkio/design-crowdb-chunkio.md), [chunk placement](../design/chunkdb/design-crowdb-chunkdb.md), [KV](../design/kv/design-crowdb-kv.md).

Goal: make production a protected cluster of at least three nodes, retain writes and reads after one node fails, and expose single-node only as an explicit unprotected test profile using 1 MiB one-copy mirror strips. A chunk may contain several strips.

## Prerequisite

- [ ] **Finish protocol ownership**: complete R191's typed S3/Iceberg allocation and separate write policies before changing the shared strip engine; retain its separate working plan and current in-progress diff. Files: `doc/working/plan-access-storage-isolation.md`, protocol, chunk-client, access libraries.

## Protection contract

- [~] **Mode configuration**: explicit production/test-single-node modes now exist in ChunkDB and access configuration, with a KV group-0 voting-replica check at ChunkDB startup. Verify this against the real container bootstrap and all production startup paths. No automatic transition between modes. Files: `app/crowdb-chunkdb/src/chunkdb_config.rs`, `app/crowdb-chunkdb/src/main.rs`, access/standalone startup config, container templates.
- [~] **Legacy fixture isolation**: colocated EC subprocess fixtures and the local combined deployment now opt into `test_unsafe_placement`; ChunkDB rejects it in release builds. Run the full Rust E2E suite to identify fixtures that still assume production can use unsafe placement. Files: ChunkDB config, `lib/crowdb-test-harness/src/chunkdb.rs`, and `lib/crowdb-console-shared/src/lifecycle.rs`.
- [x] **Per-type deployment limits**: chunk capacity is independent of strip size. S3, Iceberg, tree-page, and stream chunk capacities plus applicable RPC workers and connection counts, including ChunkDB conversion/repair DiskIO transport, are in component config files with bounded single-node profile values. Tree pages allocate multiple 1 MiB strips with one copy. Verified by config tests, C++ tests, and single-node container E2E. Files: access config, chunk KV config, ChunkDB conversion IO, stream runtime, C++ tree RPC transport, container templates.
- [~] **Allocation guard**: test-single-node initial allocation, append, conversion allocation, and published replacement now enforce one-copy 1 MiB mirror strips; startup disables conversion. Production rejects one-copy mirrors. Check reservation, direct repair, and EC layouts against loss of any one node, including direct replacement paths. Files: `app/crowdb-chunkdb/src/lifecycle/`, `app/crowdb-chunkdb/src/selector/`.

## Strip data path

- [x] **Strip dispatch**: `ChunkWriter` derives each persisted strip's kind and capacity, delegates writes across a mirror/EC boundary, and a fresh reader reopens both from disk. Verified by the mixed-strip `chunk_writer_test`. Files: `lib/crowdb-chunk-client/src/chunk/{chunk_writer,strip,ec_strip_writer,mirror_strip_writer}.rs`.
- [ ] **Mirror writer**: replace the placeholder with durable mirror writes, including one-copy 1 MiB strips, partial blocks, cross-block inputs, error propagation, and retry/repair rules. Reuse physical write behavior with `writer/mirror_flow.rs` where it preserves small-write batching. Files: `lib/crowdb-chunk-client/src/chunk/`, `lib/crowdb-chunk-client/src/writer/`.
- [ ] **Read dispatch**: confirm mirror/EC strip readers use persisted geometry and implement their own failure recovery. Files: `lib/crowdb-chunk-client/src/chunk/{strip_reader,chunk_reader}.rs`.

## Failure and recovery

- [~] **Three-node degraded operation**: retain three-voter KV membership after one node fails. New EC and mirrored small-write allocations now select two protected mirror copies when exactly two storage nodes remain. Verify real node loss, preserve existing committed writes, reject single-copy fallback, and repair/rebalance degraded strips when the third node returns. Files: KV deployment configuration, `app/crowdb-chunkdb/src/selector/`, `app/crowdb-chunkdb/src/lifecycle/`.
- [~] **Failure acceptance**: a simulated three-rack production cluster starts KV/storage/access processes, writes an S3 object, restarts, and reads it. A node-3 outage test reads an earlier object, but a new large S3 PUT returns 503. The protected fixture keeps its single ChunkDB instance on node 1 so node-3 loss isolates storage availability; full ChunkDB range failover remains separate work. `MirrorChunkWriter` now accepts a protected two-copy rollover layout, but chunk-stream journal validation still rejects that layout. After fixing it, exercise each node independently, new writes/reads, and repair after recovery. Files: `lib/crowdb-console-shared/tests/s3_mini_cluster_test.rs`, `lib/crowdb-chunk-client/src/chunk/mirror_chunk_writer.rs`, and cluster E2E.

## Blocked

- Failed command: `pixi run clean-env && pixi run cargo test -p crowdb-console-shared --test s3_mini_cluster_test protected_cluster_reads_and_writes_after_node_three_stops -- --ignored --exact --nocapture` (exit 101, fifth root-cause-driven run). Exact test failure: `write new object with one node stopped: UpstreamRpc { node_id: "s3", status: "HTTP 503: ... <Code>ServiceUnavailable</Code> ..." }`. First divergent server error in `crowdb-chunk-kv-server-20260930-020901.967-326637.log`: `chunk KV journal stream append failed error=stream metadata or data is corrupt: stream chunk mirror count differs from configuration`.
- Attempts: initial outage run showed a 503; S3 application logging identified `PutOutcome::Timeout`; S3 library logging located the Chunk-KV operation deadline; `MirrorChunkWriter` geometry fix exposed a stopped ChunkDB range owner; pinning the protected fixture's ChunkDB instance to surviving node 1 exposed the current journal geometry rejection. Each run kept the same old-read/new-write outage acceptance.
- Diagnosis: after a failed three-copy mirror write, the stream rotates to a new two-copy chunk, which is the required protected degraded layout. The writer accepts it, while `lib/crowdb-chunk-stream/src/production_chunk.rs` still compares `mirror.segments.len()` with configured `mirror_copies` and classifies the stream as corrupt. Alternatives are to make journal validation accept persisted protected layouts with at least two distinct copies, or to route journal writes through another recovery path that explicitly records a degraded policy; the first follows the existing degraded allocation contract. Full three-instance ChunkDB range failover and repair after recovery remain unfinished.

## Verification and cleanup

- [~] **Focused tests**: mode validation, allocation guards, mirror/EC strip boundaries, single-copy write error propagation, and degraded placement have focused cases. Add real DiskIO read/write faults and full-node outage cases. Files: relevant crate `tests/`.
- [~] **Gates and permanent design**: `tree-lint`, `test-cpp`, single-node container E2E, `rs-fmt-check`, `rs-lint`, and focused affected-crate tests passed after the configuration changes. Complete the three-node outage acceptance and update KV design before final cleanup.
- [~] **Full-stack test stability**: `cross_domain_rebalance_hands_one_safe_move_to_target_diskdb` timed out waiting for an Accepted journal once in a concurrent 36-test run and once alone, then passed six isolated runs and a full concurrent rerun. The first divergence inside DiskDB's asynchronous relocation worker remains unconfirmed; keep the acceptance result separate from the one-node outage work.
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
