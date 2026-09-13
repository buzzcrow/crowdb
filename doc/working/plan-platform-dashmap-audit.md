<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Platform DashMap Audit Plan

Upstream: `doc/backlog/R149-platform-dashmap-audit.md`

Goal: remove correctness-sensitive and request-hot sharded locks, including the newly added chunk-KV transport cache, while retaining only explicitly bounded lifecycle uses.

## Phase 1: Inventory and shared primitives

- [x] **Refresh production inventory**: search production Rust again and classify the current 27 fields across 19 source files, including the new chunk-KV RPC cache. Files: `doc/working/plan-platform-dashmap-audit.md`.
- [x] **Add lock-free RPC pool index**: implement an RCU endpoint index of immutable, generated connection pools; connect outside the index, compare-and-swap installation, return generation with a selected connection, exact-generation invalidation, clear, and optional endpoint bound. Files: `lib/crowdb-rpc/ffi/src/connection_pool.rs`, `lib/crowdb-rpc/ffi/src/lib.rs`, `lib/crowdb-rpc/ffi/Cargo.toml`, `lib/crowdb-rpc/ffi/tests/connection_pool_test.rs`.

## Phase 2: Client routing and transport

- [x] **Share DiskDB routes**: make all `DiskdbClient` clones share one atomic endpoint snapshot and one lock-free incremental disk index; publish endpoint refreshes atomically and test clone visibility and complete snapshots. Files: `lib/crowdb-diskdb-client/src/client.rs`, `lib/crowdb-diskdb-client/src/routing.rs`, `lib/crowdb-diskdb-client/Cargo.toml`, `lib/crowdb-diskdb-client/tests/routing_snapshot_test.rs`.
- [x] **Publish chunk allocator endpoints**: replace endpoint retain/insert refresh with one `ArcSwap<HashMap<...>>` publication coherent with disk routing. Files: `app/crowdb-chunkdb/src/allocator/pool.rs`, `app/crowdb-chunkdb/tests/allocator_pool_test.rs`.
- [x] **Consolidate RPC transport caches**: migrate KV client, Paxos, DiskDB client, ChunkDB client, KV forwarding, and the newly added chunk-KV client to the shared pool; propagate selected generations into retryable-error invalidation and preserve owner bounds. Files: `lib/crowdb-kv-client/src/transport/rpc_transport.rs`, `lib/crowdb-kv/src/rpc/px_rpc_transport.rs`, `lib/crowdb-diskdb-client/src/rpc_transport.rs`, `lib/crowdb-chunkdb-client/src/rpc_transport.rs`, `lib/crowdb-kv/src/rpc/kv_rpc_service.rs`, `lib/crowdb-chunk-kv-client/src/transport.rs`, crate manifests and transport tests.

## Phase 3: Paxos concurrency

- [x] **Close learner frontier races**: move chosen/applied gap tracking to ordered lock-free maps, recheck the frontier after insertion, and remove only the inserted identity when stale; add deterministic delayed-insert tests and waiter notification coverage. Files: `lib/crowdb-kv/src/paxos/learner.rs`, `lib/crowdb-kv/tests/paxos_test/learner_dedup_test.rs`.
- [x] **Replace dedup mutation**: add a lock-free client index with fixed 64-entry atomic windows preserving exact lookup and idempotent duplicate recording; test concurrent retention and misses. Files: `lib/crowdb-kv/src/paxos/learner.rs`, `lib/crowdb-kv/tests/paxos_test/learner_dedup_test.rs`.

## Phase 4: Atomic routing and ownership

- [x] **Publish coherent KV topology**: merge leaders, replicas, read cursors, endpoint statistics, and write high-watermarks into generation-tagged route snapshots; use compare-and-swap hint updates and exact-generation eviction. Files: `lib/crowdb-kv-client/src/client/topology.rs`, `lib/crowdb-kv-client/src/client/core.rs`, `lib/crowdb-kv-client/src/client/admin.rs`, `lib/crowdb-kv-client/src/client/retry.rs`, `lib/crowdb-kv-client/src/service/watch_notify.rs`.
- [x] **Publish store and group registries**: replace hot store/group/disk-group indices with RCU snapshots or ordered lock-free maps while preserving compare-if-current replacement and one-time tenure cancellation. Files: `app/crowdb-kv-server/src/store_registry.rs`, `lib/crowdb-kv/src/cluster/px_kv_store.rs`, `app/crowdb-diskdb/src/model/disk_group_container.rs`, affected tests.
- [x] **Publish DiskDB disk membership**: rebuild and atomically publish `disk_index` with allocation routes after membership changes. Files: `app/crowdb-diskdb/src/model/disk_group.rs`, callers and tests.

## Phase 5: Lifecycle and bounded caches

- [x] **Make chunk locks lock-free**: use an ordered lock-free index and exact-entry idle removal so overlapping acquisition/reaping retains one mutex identity. Files: `app/crowdb-chunkdb/src/lifecycle/lock_map.rs`, `app/crowdb-chunkdb/src/lifecycle/handler.rs`, `app/crowdb-chunkdb/tests/lifecycle_test.rs`.
- [x] **Bound tentative allocations**: index tentative blocks by identity and allocation timestamp with an atomic budget, evict oldest incarnations, converge under concurrency, and retain durable fallback/exact removal. Files: `app/crowdb-diskdb/src/model/disk_group.rs`, allocation callers, `app/crowdb-diskdb/tests/disk_alloc_test.rs`.
- [x] **Single-flight discovery refresh**: retain the classified cache, replace mutable round-robin state with atomic cursors, and coalesce simultaneous per-service refreshes without holding a guard across I/O. Files: `lib/crowdb-kv-client/src/service/discovery.rs`, tests.

## Phase 6: Documentation, performance, and cleanup

- [x] **Freeze retained production inventory**: add a repository check whose allowlist records the purpose and bounded guard lifetime of every retained production `DashMap`; exclude tests and benchmarks and fail any unclassified new use. Files: `tools/check-production-dashmap.py`, `pixi.toml`, retained-use source files.
- [x] **Correct design claims**: update the affected permanent designs to describe RCU/ordered-map publication and explicitly bounded retained sharded locks. Files: `doc/design/kv/design-crowdb-kv.md`, `doc/design/kv/design-crowdb-kv-server.md`, `doc/design/kv/design-crowdb-kv-rpc.md`, `doc/design/kv/design-crowdb-kv-rpc-client.md`, `doc/design/kv/design-crowdb-kv-group0.md`, `doc/design/rpc/design-crowdb-rpc-diskdb-migration.md`, `doc/design/diskdb/design-crowdb-diskdb-space-metrics.md`, `doc/design/diskdb/design-crowdb-diskdb-zone-management.md`, `doc/design/chunkdb/design-crowdb-chunkdb.md`, `doc/design/chunkdb/design-crowdb-chunkdb-rpc.md`, `doc/design/chunkdb/design-crowdb-chunkdb-range-binding.md`.
- [ ] **Verify functional gates**: run the R149 acceptance commands for learner, RPC migration, multi-group store, KV/DiskDB/ChunkDB clients, lifecycle, and disk allocation. Files: none.
- [ ] **Verify regression sentinels and lint**: run all five regression scripts, production inventory, Rust formatting, clippy, and tree lint; record any confirmed pre-existing failure. Files: none.
- [ ] **Remove completed requirement artifacts**: delete R149 detail, backlog entry, and this plan after all acceptance gates pass. Files: `doc/backlog/R149-platform-dashmap-audit.md`, `doc/backlog/backlog.md`, `doc/working/plan-platform-dashmap-audit.md`.

## Consolidated Files

- Shared RPC: `lib/crowdb-rpc/ffi/`, six Rust transport modules, affected manifests and tests.
- Routing and registries: DiskDB/ChunkDB/KV client routing modules, KV store/group registries, DiskDB group models.
- Paxos and lifecycle: learner/dedup modules, chunk lifecycle lock map, tentative allocation paths.
- Policy and docs: `tools/check-production-dashmap.sh`, `pixi.toml`, seven permanent design documents, backlog artifacts.

## Tests

- Unit: inventory allowlist; shared-route and atomic-refresh invariants; chosen/applied stale cleanup; dedup window; topology coherence; exact chunk-lock reaping; tentative bound; discovery single-flight.
- Integration: shared RPC single-install and generation invalidation; Paxos group replacement; existing client, RPC migration, lifecycle, and allocation suites.
- E2E: none.
- Performance: KV read/write, RPC, DiskDB, and ChunkDB regression sentinels.
