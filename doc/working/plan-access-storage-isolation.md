<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Protocol-Owned Chunk Storage Plan

Upstream: [R191](../backlog/R191-access-storage-isolation.md), [access architecture](../design/access-server/design-crowdb-access-server.md), [chunk IO](../design/chunkio/design-crowdb-chunkio.md).

Goal: give S3 and Iceberg separate chunk identities, write pools, and storage ownership inside one access process while preserving reads of historical `Repo` chunks.

Scope boundary: R191 keeps the existing strip engine and container protection policy while separating protocol ownership. [R192](../backlog/R192-chunkio-deployment-protection.md) follows with explicit production/test modes, mirror and EC strip dispatch, and one-node-failure availability.

## Protocol and allocation

- [x] **Canonical types**: add stable S3 and Iceberg table values after `Stream`, update FlatBuffer and Rust/C++ conversions, and reject mismatched ID prefixes before placement. Verified by protocol ID and ChunkDB full-stack tests. Files: `lib/crowdb-protocol/src/{types/chunkdb.rs,chunk_id.rs,fbs/chunkdb.fbs}`, `lib/crowdb-chunkdb-client/src/rpc_transport.rs`, `app/crowdb-chunkdb/src/{service/chunkdb_rpc_service/wire.rs,lifecycle/handler.rs}`.
- [~] **Typed client writes**: carry `ChunkType` through `SmallWritePolicy`, the large session's `ChunkClientConfig`, `SmallPoolRuntime`, and `ChunkPrefetch`; use it for generated IDs and stored type in all initial, rotated, and on-demand allocations. Default remains `Repo` for other callers. Mock small-write and large prefetch tests added; full rotation/conversion coverage remains. Files: `lib/crowdb-chunk-client/src/{config.rs,client.rs,writer/small_pipeline.rs,writer/small_pool.rs,writer/large_object.rs,writer/large_async_object.rs,chunk/chunk_prefetch.rs}`.
- [~] **Type compatibility tests**: numeric prefix values, new ID and stored type agreement, and rejection of mismatched explicit IDs are covered. Add S3 and Iceberg reads of historical `Repo` references through their application paths. Files: `lib/crowdb-protocol/tests/`, `app/crowdb-chunkdb/tests/full_stack_test.rs`, `lib/crowdb-chunk-client/tests/`.

## Protocol ownership

- [~] **S3 storage boundary**: `S3StorageClients` construction and S3 small/large policy selection live in `crowdb-access-s3`; the application still resolves process config and owns request orchestration. Verify foreground operations and metadata ownership with end-to-end tests. Files: `app/crowdb-access-server/src/{main.rs,storage.rs}`, `lib/crowdb-access-s3/src/`.
- [~] **Iceberg storage boundary**: catalog/chunk client construction and large file-write policy live in `crowdb-access-iceberg`; the application still owns part of file request orchestration and the separate GC client startup. Complete the protocol-owned foreground boundary. Files: `app/crowdb-access-server/src/iceberg/`, `lib/crowdb-access-iceberg/src/`.
- [~] **Independent configuration**: protocol-specific small and large EC, capacity, memory, and prefetch settings are parsed separately, and each library constructs its own write policy. Verify that one service's overrides never alter the other's pool, including concurrent production writes. Files: `app/crowdb-access-server/src/config.rs`, protocol storage modules, `container/single-node-container/templates/access.toml`, config docs.
- [~] **Combined lifecycle**: the entry point now signals the other listener when either returns and awaits both pool drains. Verify listener-failure propagation in a focused integration test; keep both monitor probes. Files: `app/crowdb-access-server/src/main.rs`, `container/crowdb-monitor/src/`.

## Verification and cleanup

- [ ] **Unit and integration**: run protocol, chunk client, chunkdb, S3, Iceberg, and monitor tests, including independent pool scaling and legacy `Repo` reads.
- [~] **Container acceptance**: single-node container E2E passed with both listeners, protocol writes, crash and hang recovery, and persisted-volume restart. Add explicit listener-failure propagation and chunk-type assertions. Verify differing EC policies in a protected production E2E.
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
