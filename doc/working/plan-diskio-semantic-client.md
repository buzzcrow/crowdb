<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskIO Semantic Client Plan

Upstream: `doc/backlog/R150-diskio-semantic-client.md`

Goal: make `crowdb-diskio-client` own authoritative routing, bounded connection generations, semantic I/O, retries, durability, and status, then remove duplicate ownership from production callers.

## Phase 1: Semantic and transport foundation

- [x] **Define semantic operations**: add checked segment targets, relative ranges, normal/priority lanes, buffered/durable write policy, operation deadlines, typed input/topology/transport/backpressure/disk/partial/durability/ambiguous errors, and lock-free status snapshots. Keep FlatBuffer and transport handles private. Files: `lib/crowdb-diskio-client/src/lib.rs`, `lib/crowdb-diskio-client/src/client.rs`, new focused modules under `lib/crowdb-diskio-client/src/`.
- [x] **Complete the safe RPC pool surface**: expose connection health/close through the existing safe FFI wrapper; make immutable endpoint pools select healthy members, replace degraded generations exactly, and retire removed endpoints after retained operations drain. Files: `lib/crowdb-rpc/include/crowdb-rpc/c_api.h`, `lib/crowdb-rpc/src/c_api.cpp`, `lib/crowdb-rpc/ffi/src/sys.rs`, `lib/crowdb-rpc/ffi/src/server.rs`, `lib/crowdb-rpc/ffi/src/connection_pool.rs`, `lib/crowdb-rpc/ffi/tests/connection_pool_test.rs`.
- [x] **Hide and harden wire transport**: move request encoding/response decoding into an internal transport seam, retain caller `Bytes` until completion, validate exact read length, preserve `ordering_zone_offset=segment_base`, and classify RPC and DiskIO results without exposing `CallFuture`. Files: `lib/crowdb-diskio-client/src/transport.rs`, `lib/crowdb-diskio-client/src/client.rs`, `lib/crowdb-diskio-client/tests/semantic_io_test.rs`.

## Phase 2: Authoritative routing and lifecycle

- [x] **Build complete route generations**: inject or construct `ServiceRegistryClient` and `HardwareClient`, join live DiskIO owners with hardware disks, validate unique owner plus rack/node/disk-group identity and endpoint syntax, prewarm both lanes, and atomically publish only complete generations. Files: `lib/crowdb-diskio-client/src/topology.rs`, `lib/crowdb-diskio-client/src/client.rs`, `lib/crowdb-diskio-client/Cargo.toml`, `lib/crowdb-diskio-client/tests/topology_test.rs`.
- [x] **Bound lanes and retries**: keep separate fixed normal/priority pool indices; reuse unchanged endpoint groups, retire changed/removed groups by exact generation, retry safe reads and identical writes within one deadline, never retry partial/permanent failures, and make ambiguous write/fsync outcomes explicit. Files: `lib/crowdb-diskio-client/src/runtime.rs`, `lib/crowdb-diskio-client/src/status.rs`, `lib/crowdb-diskio-client/tests/retry_test.rs`, `lib/crowdb-diskio-client/tests/connection_lifecycle_test.rs`.
- [x] **Cover semantic invariants**: test arithmetic/bounds/alignment before transport admission, payload ownership, buffered versus durable acknowledgement, lane isolation, immutable generation publication, late-generation invalidation, and status under concurrency. Files: `lib/crowdb-diskio-client/tests/address_test.rs`, `lib/crowdb-diskio-client/tests/semantic_io_test.rs`, `lib/crowdb-diskio-client/tests/topology_test.rs`, `lib/crowdb-diskio-client/tests/connection_lifecycle_test.rs`.

## Phase 3: Production caller migration

- [x] **Migrate chunk foreground I/O**: replace `RoutedDiskWriter` connection/server/topology ownership with the semantic client, map typed failures consistently, keep the `DiskWriter` test seam, and route conversion/repair methods through the priority lane. Files: `lib/crowdb-chunk-client/src/client.rs`, `lib/crowdb-chunk-client/src/disk_io.rs`, `lib/crowdb-chunk-client/src/disk_io/routing.rs`, `lib/crowdb-chunk-client/src/disk_io/disk_writer.rs`, `lib/crowdb-chunk-client/src/error.rs`, tests and config callers.
- [x] **Migrate ChunkDB conversion and repair**: make `ConversionDiskIo` a narrow priority-lane adapter over the shared semantic client; remove its endpoint parser, route snapshot, connection refresh, and response-code translation. Files: `app/crowdb-chunkdb/src/conversion/io.rs`, `app/crowdb-chunkdb/src/main.rs`, `app/crowdb-chunkdb/src/repair.rs`, affected tests.
- [x] **Centralize native route export**: have the DiskIO client produce opaque retained native routes so chunk/application code never assembles `OwnedClientRoute`; migrate chunk-KV storage startup and test harnesses off direct production transport methods. Files: `lib/crowdb-diskio-client/src/native.rs`, `lib/crowdb-chunk-client/src/client.rs`, `app/crowdb-chunk-kv-server/src/storage.rs`, `lib/crowdb-test-harness/src/diskio.rs`, affected E2E helpers.
- [x] **Prohibit duplicate production ownership**: verify production chunk callers no longer import DiskIO `Connection`/`RpcServer`, parse DiskIO endpoints, decode return codes, or own refresh-created connection vectors. Files: production Rust callers and an inventory test/tool if needed.

## Phase 4: Design, gates, and cleanup

- [x] **Update permanent ownership designs**: document semantic addresses, authoritative immutable routes, fixed lane pools, generation lifetime, retries, durability, and the DiskIO/chunk/native boundary. Files: `doc/design/diskio/design-crowdb-diskio.md`, `doc/design/chunkio/design-crowdb-chunkio.md`, `doc/design/chunkdb/design-crowdb-chunkdb.md`, `doc/design/rpc/design-crowdb-rpc.md`.
- [~] **Run focused and migration gates**: run DiskIO client, chunk client, ChunkDB, RPC FFI/C++ and server acceptance commands, with `clean-env` for server-spawning tests. Files: none.
- [ ] **Run production regression and lint**: run chunk-KV regression, rustfmt, rs-lint, Clippy, tree-lint, and diff checks; diagnose ordinary failures up to the workflow limit. Files: none.
- [ ] **Remove completed requirement artifacts**: delete the R150 detail, backlog entry, and this plan only after every acceptance gate passes. Files: `doc/backlog/R150-diskio-semantic-client.md`, `doc/backlog/backlog.md`, `doc/working/plan-diskio-semantic-client.md`.

## Consolidated Files

- Semantic client: `lib/crowdb-diskio-client/`.
- Shared RPC lifecycle: `lib/crowdb-rpc/ffi/`, `lib/crowdb-rpc/include/crowdb-rpc/c_api.h`, `lib/crowdb-rpc/src/c_api.cpp`.
- Callers: `lib/crowdb-chunk-client/`, `app/crowdb-chunkdb/`, `app/crowdb-chunk-kv-server/src/storage.rs`, `lib/crowdb-test-harness/src/diskio.rs`.
- Architecture and workflow: DiskIO/ChunkIO/ChunkDB/RPC designs and R150 artifacts.

## Tests

- Unit: address arithmetic and alignment; full topology validation; complete-generation publication; status snapshot; lane and retry classification.
- Integration: exact payload and read length; buffered/durable writes; generation replacement and late failure; degraded pool replacement; normal/priority isolation.
- E2E: real DiskIO semantic read/write/fsync; migrated chunk read/write/conversion/reconstruction; unchanged-refresh connection bound; restart under chunk-KV splitting load.
- Performance: `tools/bench-chunk-kv-regression.sh`.
