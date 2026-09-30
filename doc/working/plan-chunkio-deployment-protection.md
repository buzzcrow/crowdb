<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk IO Deployment Protection Plan

Upstream: [R192](../backlog/R192-chunkio-deployment-protection.md), [chunk IO](../design/chunkio/design-crowdb-chunkio.md), [chunk placement](../design/chunkdb/design-crowdb-chunkdb.md), [KV](../design/kv/design-crowdb-kv.md).

Goal: make the three-node production profile tolerate one node failure with two-copy mirrors and repairable degraded EC, while exposing single-node only as an explicit zero-failure-budget test profile using 1 MiB one-copy mirror strips. A chunk may contain several strips. Larger failure budgets belong to [R193](../backlog/R193-chunkdb-node-failure-budget.md).

## Current sequence

- Finish R191's protocol-owned write and read acceptance alongside R192's deployment work; its remaining tasks are in [the access storage plan](plan-access-storage-isolation.md).
- Keep the durable task scanner running when initial DiskIO discovery fails, retry discovery, and verify a real pending placement task completes after routes return. The focused three-node full-stack case now passes.
- Set the three-node mirror policy to two copies across access, chunk-KV journal, and tree pages; mirror strip I/O must handle a configured count from one through five. Verify failed-replica replacement.
- Keep EC as EC with two surviving nodes, persist degraded placement, and confirm the existing placement task scanner retries until the third node returns and restores full protection.
- Run the one-node and three-node end-to-end acceptance, including a failed rotation returning an error, then complete design and requirement cleanup. R193's six-node rules remain deferred.

## Prerequisite

- [ ] **Finish protocol ownership**: complete R191's typed S3/Iceberg allocation and separate write policies before changing the shared strip engine; retain its separate working plan and current in-progress diff. Files: `doc/working/plan-access-storage-isolation.md`, protocol, chunk-client, access libraries.

## Protection contract

- [~] **Mode configuration**: explicit production/test-single-node modes exist in ChunkDB and access configuration, with a KV group-0 voting-replica check at ChunkDB startup. ChunkDB and access now validate `max_node_failures = 1` for production and `0` for the single-node test profile; tracked production and container configs declare the value. Check remaining service startup paths. No automatic transition between modes. Files: `app/crowdb-chunkdb/src/chunkdb_config.rs`, `app/crowdb-chunkdb/src/main.rs`, access/standalone startup config, container templates.
- [~] **Unsafe fixture isolation**: colocated EC subprocess fixtures and the local combined deployment now opt into `test_unsafe_placement`; ChunkDB rejects it in release builds. The full ChunkDB package test suite passes; run remaining service E2E suites to identify fixtures that still assume production can use unsafe placement. Files: ChunkDB config, `lib/crowdb-test-harness/src/chunkdb.rs`, and `lib/crowdb-console-shared/src/lifecycle.rs`.
- [x] **Per-type deployment limits**: chunk capacity is independent of strip size. S3, Iceberg, tree-page, and stream chunk capacities plus applicable RPC workers and connection counts, including ChunkDB conversion/repair DiskIO transport, are in component config files with bounded single-node profile values. Tree pages allocate multiple 1 MiB strips with one copy. Verified by config tests, C++ tests, and single-node container E2E. Files: access config, chunk KV config, ChunkDB conversion IO, stream runtime, C++ tree RPC transport, container templates.
- [~] **Allocation guard**: test-single-node initial allocation, append, conversion allocation, and published replacement now enforce one-copy 1 MiB mirror strips; startup disables conversion. Production rejects one-copy mirrors. Set the healthy three-node mirror policy to two copies, validate healthy EC against one-node loss, and distinguish two-node degraded EC from unsafe test placement. Check reservation and direct replacement paths. Files: `app/crowdb-chunkdb/src/lifecycle/`, `app/crowdb-chunkdb/src/selector/`.

## Strip data path

- [x] **Strip dispatch**: `ChunkWriter` derives each persisted strip's kind and capacity, delegates writes across a mirror/EC boundary, and a fresh reader reopens both from disk. Verified by the mixed-strip `chunk_writer_test`. Files: `lib/crowdb-chunk-client/src/chunk/{chunk_writer,strip,ec_strip_writer,mirror_strip_writer}.rs`.
- [~] **Mirror writer**: `MirrorStripWriter` now writes and fsyncs every persisted segment without assuming a fixed copy count; direct stream mirrors and tree pages accept up to five copies. Cross-strip and one-copy error tests pass. A focused two-copy small-write fault test verifies failed-copy replacement without chunk rotation. The small-write pipeline now seals the failed chunk and retries once in a new chunk only when replica repair is exhausted; mock tests cover success and failure after rotation. A three-rack test stops all real DiskIO processes and verifies that two-copy repair plus one rotation returns an error within 15 seconds. The protected combined-access HTTP test writes consecutive non-aligned small objects through both two-copy protocol pools and reads each back. Verify a two-copy write crossing a strip block boundary. Files: `lib/crowdb-chunk-client/src/chunk/`, `lib/crowdb-chunk-client/src/writer/`.
- [x] **Read dispatch**: `StripReader` selects mirror or EC recovery from each persisted strip; `ChunkReader` uses recorded offsets and capacities. Focused mirror fallback, EC reconstruction, and mixed-strip reopen/read tests pass. Files: `lib/crowdb-chunk-client/src/chunk/{strip_reader,chunk_reader}.rs`.

## Failure and recovery

- [~] **Three-node degraded operation**: retain three-voter KV membership after one node fails. New EC requests keep EC geometry and use a separate two-survivor placement permission; the allocator records degraded placement for repair. Balanced two-node EC selection, full-stack allocation, node-2 and node-3 S3 outage acceptance, and repair after node return pass. A focused two-copy writer fault test verifies replacement without rotation, and a three-node full-stack test verifies that the replacement lands on the unused survivor. Memory-backed and production DiskIO process-failure stream tests verify an error after the one allowed rotation. Preserve old reads and reject one-copy fallback. Files: KV deployment configuration, `app/crowdb-chunkdb/src/selector/`, `app/crowdb-chunkdb/src/lifecycle/`, `app/crowdb-chunkdb/src/placement_repair.rs`, `lib/crowdb-chunk-stream/src/`.
- [x] **Mirror policy propagation**: production defaults select two copies for access small writes, chunk-KV journal/stream, and tree pages. Rust mirror writes use the strip's segment count; the C++ tree transport and pipeline support one through five slots. Focused Rust 2/5-copy tests and all 48 ChunkPageStore C++ tests passed. Files: `lib/crowdb-chunk-client/src/config.rs`, `app/crowdb-chunk-kv-server/src/config.rs`, `lib/crowdb-chunk-stream/src/`, `lib/crowdb-tree/src/backend/chunk/`, access config.
- [x] **Background task readiness**: ChunkDB starts the executor and placement scanner even when initial DiskIO discovery fails and retries route discovery. A three-node full-stack test starts with failed discovery, reconnects the same adapter, re-creates the task store and scanner after a failed attempt, then verifies full placement and fragment contents after the node returns. The protected S3 outage test now confirms that a real ChunkDB process restart resumes and completes placement repair. Files: `app/crowdb-chunkdb/src/{main.rs,conversion/io.rs}`, `app/crowdb-chunkdb/tests/`, `lib/crowdb-console-shared/tests/`.
- [~] **Failure acceptance**: the simulated three-rack production cluster now starts a ChunkDB, DiskDB, and DiskIO instance on each node alongside KV and access services. Its focused E2E tests stop all four processes on one node, wait for ChunkDB range reassignment, read an existing S3 object, and write and read a new 2 MiB object. All three failed-node choices pass with the expanded fixture. A pre-submission ChunkDB connection failure now reroutes safely. The extent-page store no longer labels a missing key after uncertain CAS as conflicting contents; the prior failure log did not distinguish absence from a different stored value, so its exact cause remains unconfirmed. The node-2 case confirms a degraded S3 EC strip is persisted during the outage, restores the node, restarts every process, verifies the strip regains rack/node/disk protection, and reads the outage write. Add direct failure of a live DiskIO write or read. Files: `lib/crowdb-console-shared/tests/s3_mini_cluster_test.rs` and cluster E2E.

## Prior failure evidence

- Failed command: `pixi run clean-env && pixi run cargo test -p crowdb-console-shared --test s3_mini_cluster_test protected_cluster_reads_and_writes_after_node_three_stops -- --ignored --exact --nocapture` (exit 101, fifth root-cause-driven run). Exact test failure: `write new object with one node stopped: UpstreamRpc { node_id: "s3", status: "HTTP 503: ... <Code>ServiceUnavailable</Code> ..." }`. First divergent server error in `crowdb-chunk-kv-server-20260930-020901.967-326637.log`: `chunk KV journal stream append failed error=stream metadata or data is corrupt: stream chunk mirror count differs from configuration`.
- Attempts: initial outage run showed a 503; S3 application logging identified `PutOutcome::Timeout`; S3 library logging located the Chunk-KV operation deadline; `MirrorChunkWriter` geometry fix exposed a stopped ChunkDB range owner; pinning the protected fixture's ChunkDB instance to surviving node 1 exposed the current journal geometry rejection. Each run kept the same old-read/new-write outage acceptance.
- Diagnosis at the time: the old three-copy mirror policy used every node, so a failed copy had no unused survivor for replacement. Rotation allocated two copies, but `lib/crowdb-chunk-stream/src/production_chunk.rs` compared their count with the configured three and classified the stream as corrupt. The new contract uses two-copy mirrors from the start; this failure remains regression evidence, not the desired fallback design. Later three-ChunkDB outage tests verified range failover and placement repair after recovery.
- Passing rerun: `pixi run cargo test -p crowdb-console-shared --test s3_mini_cluster_test protected_cluster_reads_and_writes_after_node_three_stops -- --ignored --exact --nocapture` (1 passed, about 70 seconds). Later focused reruns covered all three failed-node choices; the node-2 process restart also verified persisted degraded EC placement and repair convergence.

## Verification and cleanup

- [~] **Focused tests**: mode validation, allocation guards, mirror/EC strip boundaries, single-copy write error propagation through real DiskIO, read error propagation after the only DiskIO process exits, degraded placement, and full-node outage have focused cases. A protected two-copy real DiskIO process failure now exhausts replacement and one rotation within the 15-second fault budget. A production chunk-stream test stops its real DiskIO process after a successful append and verifies one rotation, an error within 15 seconds, and an unchanged journal tail. Files: relevant crate `tests/`.
- [~] **Gates and permanent design**: after the current changes, `rs-fmt-check`, `rs-lint`, `tree-lint`, full ChunkDB, chunk-client, and chunk-stream test suites, access configuration tests, all 48 ChunkPageStore C++ tests, protected-cluster restart and node-2/node-3 outage E2E, and `test-single-node-container` passed. The ChunkDB design now describes two-copy mirrors and degraded EC. Update remaining permanent access and KV design after final acceptance.
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
