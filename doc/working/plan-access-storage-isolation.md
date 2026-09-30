<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Protocol-Owned Chunk Storage Plan

Upstream: [R191](../backlog/R191-access-storage-isolation.md), [access architecture](../design/access-server/design-crowdb-access-server.md), [chunk IO](../design/chunkio/design-crowdb-chunkio.md).

Goal: give S3 and Iceberg separate chunk identities, write pools, and storage ownership inside one access process.

Scope boundary: R191 separates protocol ownership. [R192](../backlog/R192-chunkio-deployment-protection.md) owns the deployment profiles, mirror and EC strip dispatch, and one-node-failure availability; the two plans can be verified in parallel.

Current checkpoint: a three-rack protected-storage integration test concurrently writes and reads S3 and Iceberg small and large payloads, then checks distinct chunk type prefixes, two-copy small mirrors, and 2+1 versus 4+2 large EC. A second three-rack test now starts one combined access-server process, writes and reads small and large objects through both HTTP listeners, and verifies the same chunk type and strip layout separation in ChunkDB. The single-node container E2E starts both listeners and verifies that either occupied listener makes a second combined access process exit promptly. The failure test does not yet inject a runtime storage-path failure or verify monitor health for the failed process.

## Protocol and allocation

- [x] **Canonical types**: add stable S3 and Iceberg table values after `Stream`, update FlatBuffer and Rust/C++ conversions, and reject mismatched ID prefixes before placement. Verified by protocol ID and ChunkDB full-stack tests. Files: `lib/crowdb-protocol/src/{types/chunkdb.rs,chunk_id.rs,fbs/chunkdb.fbs}`, `lib/crowdb-chunkdb-client/src/rpc_transport.rs`, `app/crowdb-chunkdb/src/{service/chunkdb_rpc_service/wire.rs,lifecycle/handler.rs}`.
- [x] **Typed client writes**: `ChunkType` flows through `SmallWritePolicy`, the large session's `ChunkClientConfig`, `SmallPoolRuntime`, and `ChunkPrefetch` into generated IDs and stored type. `Repo` remains the default for internal callers. Mock tests cover on-demand allocation and multiple prefetched chunks. Real service tests verify S3 mirror-to-EC conversion and Iceberg small and large writes across chunk rotation without losing their type. The Iceberg HTTP file service now forces its large-write default and supplied policy to `IcebergTable`. Files: `lib/crowdb-chunk-client/src/{config.rs,client.rs,writer/small_pipeline.rs,writer/small_pool.rs,writer/large_object.rs,writer/large_async_object.rs,chunk/chunk_prefetch.rs}`.
- [x] **Type identity tests**: numeric prefix values, new ID and stored type agreement, and rejection of mismatched explicit IDs are covered. No historical application data requires `Repo` compatibility. Files: `lib/crowdb-protocol/tests/`, `app/crowdb-chunkdb/tests/full_stack_test.rs`, `lib/crowdb-chunk-client/tests/`.

## Protocol ownership

- [x] **S3 storage boundary**: `S3StorageClients`, S3 small/large policy selection, metadata, and foreground object operations live in `crowdb-access-s3`; the application resolves process config and serves HTTP requests. The protected combined-listener test writes and reads S3 small and large objects, and confirms S3 chunk identity and layout. Files: `app/crowdb-access-server/src/{main.rs,storage.rs}`, `lib/crowdb-access-s3/src/`.
- [x] **Iceberg storage boundary**: catalog/chunk client construction, default and configured large file-write policy, foreground small/shared/large writer preparation, native file-block adapters, GC I/O budgeting, streaming read construction, and uploaded file-record construction live in `crowdb-access-iceberg`. The application streams HTTP bodies through `IcebergFileWriter` and retains listener and worker startup. HTTP uploads and multipart publication share one file-format rule. Focused file-body tests, signed upload/multipart integration, and the ordinary 10 KiB through 100 MiB size matrix pass. The size matrix uses a 120-second request budget so the 100 MiB case can finish on the null-DiskIO fixture. Files: `app/crowdb-access-server/src/iceberg/`, `lib/crowdb-access-iceberg/src/`.
- [x] **Independent configuration**: protocol-specific small and large EC, capacity, memory, and prefetch settings are parsed separately, and each library constructs its own write policy. Configuration and storage-policy tests pass, and a focused S3/Iceberg pool test verifies that loading and scaling either pool does not change the other's metrics. Three-rack protected-storage tests verify concurrent client writes with different policies and small and large HTTP writes through both listeners of one process, including distinct chunk types and EC schemes. Files: `app/crowdb-access-server/src/config.rs`, protocol storage modules, `container/single-node-container/templates/access.toml`, config docs.
- [~] **Combined lifecycle**: the entry point signals the other listener when either returns and awaits both pool drains. The container E2E verifies prompt process exit for each listener's startup bind failure while the original process remains ready. The monitor profile test pins both the authenticated Iceberg probe and S3 readiness probe to the combined access service, while probe and supervisor tests cover failure reporting. A prepared small write returns an error if its manager has no published pipelines instead of waiting indefinitely. An unexpected Iceberg namespace, multipart, table, or GC worker exit fails the listener and drains its GC pool before the combined process stops the other listener. S3, Iceberg foreground, and Iceberg GC distinguish a terminated small-write manager from a temporarily empty route set; manager termination fails the owning listener. A focused fault-injection test checks that the terminated manager becomes observable. `ProductionS3Operations` marks chunk health unavailable after a `ServiceUnavailable` request. Verify runtime manager-failure propagation through the combined process and monitor, including both pool drains. Files: `app/crowdb-access-server/src/main.rs`, `container/crowdb-monitor/src/`, `lib/crowdb-chunk-client/src/writer/`.
- [x] **GC pool isolation**: when enabled, Iceberg GC constructs a separate chunk client with the Iceberg small-write policy and uses its own I/O budget. The budgeted storage adapters live in the Iceberg library; both focused budget tests pass after the move. The official-SDK case previously ran GC backlog advancement, Iceberg table operations, and S3 small writes together; all 64 S3 writes completed while the SDK workload succeeded. Rerun that case after the policy correction. Files: `app/crowdb-access-server/src/iceberg/runtime.rs`, `app/crowdb-access-server/tests/iceberg_gc_control_test.rs`.

## Verification and cleanup

- [~] **Unit and integration**: focused protocol, chunk client, ChunkDB, S3, Iceberg, GC isolation, and independent pool-scaling tests pass. Run the remaining package and monitor gates before cleanup.
- [~] **Container acceptance**: single-node container E2E passes with both listeners, protocol writes, startup listener bind-failure propagation, crash and hang recovery, and persisted-volume restart. The protected three-rack client and combined HTTP integrations verify chunk types and differing EC policies. Add container chunk-type assertions.
- [~] **Gates and docs**: the permanent access design now describes protocol-owned storage policy and file authority. Run `pixi run rs-fmt-check`, `pixi run rs-lint`, affected Rust tests, `pixi run tree-lint`, and `pixi run test-cpp` for C++ changes after final acceptance; reconcile any remaining permanent access/chunkdb design detail.
- [ ] **Final cleanup**: delete R191, its backlog index entry, and this plan in a final cleanup commit after all acceptance cases pass.

## Files

- Protocol and allocation: `lib/crowdb-protocol/`, `lib/crowdb-chunkdb-client/`, `app/crowdb-chunkdb/`, `lib/crowdb-chunk-client/`.
- Protocol ownership: `lib/crowdb-access-s3/`, `lib/crowdb-access-iceberg/`, `app/crowdb-access-server/`.
- Deployment and documentation: `container/single-node-container/`, `container/crowdb-monitor/`, `doc/design/access-server/`, `doc/design/chunkdb/`.

## Tests

- Unit: protocol enum/ID conversion, typed small and large allocation, independent policies.
- Integration: ChunkDB prefix validation, S3/Iceberg read/write, GC pool isolation.
- E2E: one container process, both listeners, different policies, restart and failure health behavior.
