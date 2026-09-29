<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Protocol-Owned Chunk Storage Plan

Upstream: [R191](../backlog/R191-access-storage-isolation.md), [access architecture](../design/access-server/design-crowdb-access-server.md), [chunk IO](../design/chunkio/design-crowdb-chunkio.md).

Goal: give S3 and Iceberg separate chunk identities, write pools, and storage ownership inside one access process while preserving reads of historical `Repo` chunks.

Scope boundary: R191 keeps the existing strip engine and container protection policy while separating protocol ownership. [R192](../backlog/R192-chunkio-deployment-protection.md) follows with explicit production/test modes, mirror and EC strip dispatch, and one-node-failure availability.

## Protocol and allocation

- [x] **Canonical types**: add stable S3 and Iceberg table values after `Stream`, update FlatBuffer and Rust/C++ conversions, and reject mismatched ID prefixes before placement. Verified by protocol ID and ChunkDB full-stack tests. Files: `lib/crowdb-protocol/src/{types/chunkdb.rs,chunk_id.rs,fbs/chunkdb.fbs}`, `lib/crowdb-chunkdb-client/src/rpc_transport.rs`, `app/crowdb-chunkdb/src/{service/chunkdb_rpc_service/wire.rs,lifecycle/handler.rs}`.
- [ ] **Typed client writes**: carry `ChunkType` through `SmallWritePolicy`, `LargeWritePolicy`, `SmallPoolRuntime`, and `ChunkPrefetch`; use it for generated IDs and stored type in all initial, rotated, and on-demand allocations. Default remains `Repo` for other callers. Files: `lib/crowdb-chunk-client/src/{config.rs,client.rs,writer/small_pipeline.rs,writer/small_pool.rs,writer/large_object.rs,writer/large_async_object.rs,chunk/chunk_prefetch.rs}`.
- [ ] **Type compatibility tests**: assert old values/readability, new ID and stored type agreement, and rejection of mismatched explicit IDs. Files: `lib/crowdb-protocol/tests/`, `app/crowdb-chunkdb/tests/full_stack_test.rs`, `lib/crowdb-chunk-client/tests/`.

## Protocol ownership

- [ ] **S3 storage boundary**: move `S3StorageClients` connection and write policy selection from the application into `crowdb-access-s3`; assign S3 type to both small and large writes. Keep S3 metadata operations in the S3 library. Files: `app/crowdb-access-server/src/{main.rs,storage.rs}`, `lib/crowdb-access-s3/src/`.
- [ ] **Iceberg storage boundary**: move catalog/chunk client construction and file write policy into `crowdb-access-iceberg`; assign Iceberg table type to foreground file writes, retain the isolated GC pool, and keep catalog metadata in that library. Files: `app/crowdb-access-server/src/iceberg/`, `lib/crowdb-access-iceberg/src/`.
- [ ] **Independent configuration**: add protocol-owned small and large EC, memory, and prefetch settings, with existing common values as migration defaults; ensure one service's overrides never alter the other's policy. Files: `app/crowdb-access-server/src/config.rs`, protocol runtime modules, `container/single-node-container/templates/access.toml`, config docs.
- [ ] **Combined lifecycle**: make the application entry point only configure logging, build protocol services, serve both listeners, and drain both pools on shutdown or either service failure. Keep both monitor probes. Files: `app/crowdb-access-server/src/main.rs`, `container/crowdb-monitor/src/`.

## Verification and cleanup

- [ ] **Unit and integration**: run protocol, chunk client, chunkdb, S3, Iceberg, and monitor tests, including independent pool scaling and legacy `Repo` reads.
- [ ] **Container acceptance**: build and run single-node container E2E with differing S3/Iceberg EC and prefetch settings, both listeners, restart, and failure propagation.
- [ ] **Gates and docs**: run `pixi run rs-fmt-check`, `pixi run rs-lint`, affected Rust tests, `pixi run tree-lint`, `pixi run test-cpp` for C++ changes, then update permanent access/chunkdb design.
- [ ] **Final cleanup**: delete R191, its backlog index entry, and this plan in a final cleanup commit after all acceptance cases pass.

## Files

- Protocol and allocation: `lib/crowdb-protocol/`, `lib/crowdb-chunkdb-client/`, `app/crowdb-chunkdb/`, `lib/crowdb-chunk-client/`.
- Protocol ownership: `lib/crowdb-access-s3/`, `lib/crowdb-access-iceberg/`, `app/crowdb-access-server/`.
- Deployment and documentation: `container/single-node-container/`, `container/crowdb-monitor/`, `doc/design/access-server/`, `doc/design/chunkdb/`.

## Tests

- Unit: protocol enum/ID conversion, typed small and large allocation, independent policies.
- Integration: ChunkDB prefix validation, S3/Iceberg read/write and legacy references, GC pool isolation.
- E2E: one container process, both listeners, different policies, restart and failure health behavior.
