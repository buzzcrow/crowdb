<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# ChunkDB Slot Routing Plan

Upstream: [R202](../backlog/R202-chunkdb-key-partition-design.md).
Follow-ups: [R103](../backlog/R103-chunkdb-range-migration.md),
[R207](../backlog/R207-chunkdb-repo-metadata-chunk-kv.md).

Implement typed chunk operations and isolated tasks over two independent slot
maps, with all chunk metadata in fixed nonzero direct KV groups.

Status: Plan prepared on 2026-10-03. Coding is on hold by user instruction.
No implementation task is active or verified. Delete this temporary plan after
implementation and verified requirement completion.

## Delivery boundary and readiness

- No design issue prevents preparing or starting this fixed-topology work once
  the user releases the coding hold. Detailed API/schema choices below are
  implementation work, not requests for another architecture approval.
- Test on three nodes with three selected nonzero chunk-storage KV groups;
  group 0 is a separate control-plane group. Node count is not group count or
  slot count. Use the harness's supported Paxos replication configuration.
- Initialize 1024 slots once. Persist one bitmap record per ChunkDB instance
  and one per selected KV group. Configure the two assignments independently;
  test service assignments that cross storage-group boundaries.
- Use fresh, isolated test state. Never erase existing deployment state to make
  tests pass. Detect incompatible legacy routing/types and reject unsupported
  upgrades without rewriting data. Online conversion is deferred.
- Freeze both ownership maps for this delivery. Instance restart with the same
  logical ownership and endpoint refresh is in scope; live ownership transfer,
  server rebalance and KV-group add/remove/remap are deferred to R103. Disable
  automatic reassignment by the old monitor for the new layout.
- Ordinary clients consume only the service map. ChunkDB consumes the storage
  map. Payload IO remains on DiskIO; neither map places payload bytes in KV.
- Use shared service assignments with explicitly separate system/repo operation
  and task scopes in one process as the initial implementation choice. Separate
  processes and load-based assignment policy are not required for this delivery.
- R207 integration is excluded. Do not replace direct-KV conditional atomic
  writes with chunk-kv batches.
- [R201](../backlog/R201-tree-memtable-write-handoff.md) is a known tree
  correctness dependency for final concurrent-write/recovery acceptance. Its
  working-tree changes are in progress and unverified here. Preserve them;
  coordinate overlapping tree edits when coding resumes. Routing unit work can
  proceed independently, but affected acceptance cannot be declared passed by
  reducing load or suppressing the known failure.

## Phase 1: Fixed layout and control-plane publication

- [ ] **Slot contract**: introduce a shared logical-slot type and fixed 1024-slot
  bitmap codec, with a layout version and canonical ChunkId hash input. Keep
  surviving chunk-type wire values stable. Define new-layout slot derivation
  explicitly; do not silently reinterpret persisted 16-bit bucket bindings.
  Files: `lib/crowdb-protocol/src/chunk_id.rs`,
  `lib/crowdb-protocol/src/types/common.rs`, protocol `tests/`.
- [ ] **Binding records**: add separate keys/values for per-instance service
  bitmaps and per-group storage bitmaps. Include independently identified map
  versions; retain one record for an instance with an empty bitmap. Validate
  bitmap size, owner identity, complete coverage, overlap and nonzero eligible
  destinations before building immutable in-memory slot lookup arrays.
  Files: `lib/crowdb-protocol/src/key/chunkdb.rs`, protocol binding types,
  `app/crowdb-chunkdb/src/routing.rs`.
- [ ] **Atomic initialization**: publish each complete initial map through the
  group-0 atomic write primitives, including a version/header in the same
  publication. Make retries idempotent and detect conflicting initialization.
  Readers use a consistent scan cutoff or header revalidation before publishing
  a cache. A per-owner CAS alone is insufficient for a complete map.
  Files: `app/crowdb-kv-server/src/group0_control_plane.rs`,
  `app/crowdb-kv-server/src/background/domain_monitor/chunkdb.rs`.
- [ ] **Freeze old writers**: adapt the monitor and client-side binding strategy
  to recognize the new layout and reject incompatible changes. Heartbeat loss
  must not reassign initialized slots in this delivery. Keep endpoint discovery
  separate from ownership; do not convert old per-range rows automatically.
  Files: KV domain monitor, `lib/crowdb-kv-client/src/binding/chunkdb_strategy.rs`,
  `app/crowdb-chunkdb/src/chunkdb_config.rs`.

## Phase 2: ChunkDB persistence and task authority

- [ ] **Storage route wiring**: remove `default_binding_table(0, 0)` from
  production startup. Load and validate the storage map before accepting
  allocation. Route canonical chunk, task, index and reservation writes from
  the owning chunk ID; preserve their existing group-local CAS/batch boundaries.
  Missing routes or unavailable groups return errors without fallback.
  Files: `app/crowdb-chunkdb/src/{main,routing,storage}.rs`,
  `app/crowdb-chunkdb/src/task/store.rs`, lifecycle reservation paths.
- [ ] **Service guard**: replace interval membership with slot ownership in
  request admission, readiness and quota allocation. Empty ownership permits no
  partition work. Keep ownership snapshots coherent for an operation; fail
  closed on missing/invalid initialization. Cover allocation with supplied IDs
  and server-generated IDs, including zero-slot instances and bounded retries.
  Files: `app/crowdb-chunkdb/src/range_guard.rs`, `main.rs`,
  `app/crowdb-chunkdb/src/lifecycle/handler.rs`.
- [ ] **Task scan scope**: add explicit domain/slot index scope so ready,
  finalize and expired-lease scans select owned work before applying limits.
  Deduplicate destination-group scans and preserve pagination/progress; avoid
  repeatedly reading an unowned prefix and filtering away the whole batch.
  Canonical tasks retain their owning chunk identity for routing.
  Files: `lib/crowdb-protocol/src/key/chunk_task.rs`,
  `app/crowdb-chunkdb/src/task/{store,scanner,manager}.rs`.
- [ ] **Separate operation domains**: wire system and repo lifecycle admission
  and task runtimes separately, with independent scan/claim scope and execution
  capacity. Reuse task algorithms without a mixed queue. Enforce slot and domain
  authority at admission, claim and publication, including reservations, repair,
  conversion, finalize, ownership queries and listing.
  Files: ChunkDB `main.rs`, `lifecycle/`, `task/`, associated protocol task types.
- [ ] **Fixed-owner recovery**: rebuild caches and task state from remote KV;
  preserve task-claim generation/lease checks and existing writer fencing.
  Verify restart cannot make an old in-flight completion authoritative after
  its claim is replaced. Do not implement a new cross-server handoff protocol.
  Files: ChunkDB lifecycle/task recovery and `tests/`.

## Phase 3: Every chunk client uses service routing

- [ ] **Rust service routing**: consume the per-instance bitmap map, refresh
  endpoints through discovery and reject invalid layouts. Resolve existing
  chunks by slot; choose a nonempty owner for server-generated allocation.
  Remove compatibility behavior that retries mutations through an old owner
  without current authority. Clients must not require storage-map access.
  Files: `lib/crowdb-kv-client/src/binding/range.rs`,
  `lib/crowdb-chunkdb-client/src/client.rs`, `lib/crowdb-chunk-client/src/client.rs`.
- [ ] **Native route interface**: replace the single retained ChunkDB route
  exported by `native_storage_routes` with a lifetime-safe service resolver or
  equivalent shared route snapshot. Specify ownership and asynchronous refresh
  across the existing Rust/C++ seam without introducing hot-path locks.
  Files: chunk client, `app/crowdb-chunk-kv-server/src/storage.rs`,
  `lib/crowdb-tree/ffi/`, tree `backend/chunk/rpc_chunk_transport.h`.
- [ ] **Native request routing**: resolve query, advance, seal and other
  existing-chunk RPCs from chunk ID; route allocation to an eligible owner.
  Handle stale endpoints/NotMyRange by refreshing and bounded retry while
  preserving operation idempotency and unknown-outcome behavior. Fixed topology
  still requires recovery against the correct owner after process restart.
  Files: `lib/crowdb-tree/src/backend/chunk/rpc_chunk_transport.cpp`,
  `lib/crowdb-tree/tests/integration/rpc_chunk_transport_test.cpp`.

## Phase 4: Chunk purpose and PageIndex persistence

- [ ] **Concrete user-data types**: remove generic Repo producers, defaults and
  fallback conversions; require explicit supported types. Reserve the retired
  wire value and reject it without reusing it. Update CLI/bench and adapter
  callers without adding a speculative Dataset implementation.
  Files: protocol `types/chunkdb.rs`, `fbs/chunkdb.fbs` and conversions,
  `lib/crowdb-chunk-client/src/config.rs`, affected CLI/S3/Iceberg callers.
- [ ] **Durable stream purpose**: carry Wal versus business Stream through
  production runtime creation, manifest/recovery, owner-key validation,
  MirrorChunkWriter allocation/reopen and rollover/repair. Do not infer purpose
  only from a live caller or silently retag legacy system Stream IDs.
  Files: `lib/crowdb-protocol/src/chunk_stream.rs`,
  `lib/crowdb-chunk-client/src/chunk/mirror_chunk_writer.rs`,
  `lib/crowdb-chunk-stream/src/`, chunk-kv storage assembly.
- [ ] **Typed page-store writes**: carry BtreePage/PageIndex purpose from snapshot
  preparation through synchronous/asynchronous page-store writes and packing.
  Keep different purposes in separate chunks. Specify directory/anchor placement
  and bootstrap references before changing the persisted representation.
  Files: `lib/crowdb-tree/src/snapshot/persist.cpp`, page-store interfaces,
  `lib/crowdb-tree/src/backend/chunk/{chunk_page_store,chunk_transport}.h`,
  `lib/crowdb-tree/src/backend/chunk/chunk_page_store.cpp`.
- [ ] **Mapping recovery and reclamation**: allocate PageIndex in the transport,
  publish coherent snapshot references and recover without needing the mapping
  table to locate its own chunks. Preserve split sharing/reference ownership and
  reclaim page/mapping chunks only after durable reachability permits it.
  Files: tree snapshot/chunk backend, mapping persistence, RPC transport,
  `lib/crowdb-tree/tests/integration/{chunk_page_store,snapshot_export}_test.cpp`.

## Phase 5: Verification and documentation

- [ ] **Focused verification**: implement and run the unit and integration cases
  below with failure injection appropriate to each changed boundary. Resolve
  R201 overlap before claiming concurrent tree acceptance.
  Files: affected crate `tests/`, tree `tests/{unit,integration}/`.
- [ ] **Three-node acceptance**: extend the existing harness with three selected
  nonzero groups and independently distributed service/storage slots; record
  allocation, task recovery, tree/WAL and S3/Iceberg results. Prove group 0 has
  binding/configuration records but no per-chunk maintenance state.
  Files: `lib/crowdb-test-harness/src/cluster.rs`,
  `app/crowdb-chunkdb/tests/common/cluster.rs`, focused new integration tests.
- [ ] **Permanent contract update**: update implemented architecture, actual
  initialization/route/task costs and fixed-layout limits. Mark R103 integration
  cases deferred rather than passed; keep R207 separate. Remove this plan only
  after verified completion under the implementation workflow.
  Files: `doc/design/chunkdb/`, affected tree/stream designs, R202.

## Consolidated file ownership

- Protocol: slot/hash and binding codecs, chunk types, stream purpose, task keys.
- KV client/server: binding readers, both assignment writers, atomic publication.
- ChunkDB: startup/config, routing/guard, lifecycle/storage, task runtime and tests.
- ChunkDB/chunk clients: explicit allocation types and service-only discovery.
- Stream and chunk-kv server: durable WAL purpose and native tree assembly.
- Tree and FFI: native routing, typed page-store/packing, snapshot recovery and
  reclamation. Preserve the concurrent R201 memtable work.
- Test harness and permanent docs: fixed three-node topology and acceptance.

## Verification matrix

- Unit: all 1024 slots; deterministic hashing; bitmap coverage/overlap/length;
  zero-slot owner; distinct map versions; forbidden group 0; explicit chunk
  types; domain/slot task index ordering and pagination; stream owner validation.
- Integration: atomic initialization/retry/conflict; interrupted or inconsistent
  map reads; unsupported layout rejection; chunk/task/index/reservation atomicity
  on each group; lost replies; disjoint task scopes and capacity; expired claims;
  supplied/generated IDs; native routing to multiple owners; endpoint refresh;
  typed snapshot cold recovery, interrupted publication and shared-chunk reclaim.
- E2E: three nodes, three nonzero data groups plus group 0; non-aligned service
  and storage maps; Wal, BtreePage, PageIndex, business Stream, S3 and Iceberg
  paths; same-owner restart and Paxos failover without changing slot placement.
  Inspect per-group state and verify acknowledged records remain recoverable.
- Deferred: server ownership transfer, group add/remove/remap, legacy conversion,
  migration throughput and all R207 chunk-kv backend tests. These are not waivers
  for same-owner restart, direct-KV atomicity or PageIndex recovery.
- Run builds/tests/lints only through `pixi run`. Use 60-second shell timeouts;
  start longer suites in the background and poll without truncating failures.
- Focused Rust suites: protocol chunk ID/task key/value tests; KV client
  `chunkdb_partition_test`; KV server `domain_monitor_test`; ChunkDB routing,
  lifecycle, ownership and task tests; chunk-stream production/restart tests.
- C++: focused mapping/chunk-page-store/RPC transport/snapshot suites, followed
  by relevant `pixi run test-tree-ct`, `pixi run test-cpp` and FFI tests.
- Gates when coding resumes: `pixi run rs-fmt-check`, affected Rust clippy,
  changed C++ formatting and `pixi run tree-lint`. Broaden tests only for changed
  paths or unresolved failures; no runtime tests have been run for this plan.

## Results

- Source review found reusable group-local conditional atomic writes, task claim
  generations, lock-free binding caches and group-0 batch publication.
- Remaining work is tracked above; no implementation completion is implied.
