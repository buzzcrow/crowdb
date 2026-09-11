<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R149: platform — DashMap Correctness and Hot-Path Replacement

## Problem

Production code currently has 26 `DashMap` fields across 18 Rust source files.
The uses mix three different roles: request-path routing, mutable concurrency
state, and low-frequency lifecycle registries. `DashMap` protects hash shards
with reader/writer locks; a point lookup is concurrent but is not lock-free.
This conflicts with the repository's lock-free hot-path rule when the lookup is
performed for every KV, Paxos, diskdb, or chunkdb request.

The audit found two concrete correctness/lifetime problems:

- `lib/crowdb-diskdb-client/src/client.rs` derives `Clone` while storing
  `endpoint_cache` and `disk_to_dg` as direct `DashMap` fields. `DashMap::clone`
  copies its shards and entries, so later route updates made through one client
  clone are invisible to the others. A clone can continue routing with stale
  endpoint or disk ownership data.
- `lib/crowdb-kv/src/paxos/learner.rs` separately loads a contiguous frontier,
  inserts a slot into `out_of_order` or `applied_out_of_order`, and drains the
  next slots. A delayed duplicate learn can insert a slot after another task
  has already advanced beyond it. That stale entry is below the frontier and
  is never drained. Replacing the container alone does not fix this compound
  race.

Other audited uses have availability or performance risks:

- KV client, Paxos, diskdb client, chunkdb client, and KV forwarding transports
  take a DashMap shard lock on every connection lookup. Their slow paths hold
  an entry write guard while establishing connections. KV and Paxos transport
  failures clear the endpoint's current pool without proving that it is the
  same generation used by the failed request, so an old failure can discard a
  newly established pool. The KV forwarder uses `get -> connect -> insert`,
  allowing concurrent duplicate connections and replacement.
- `TopologyCache` publishes leader and replica lists through separate maps.
  Refresh inserts and evicts entries one at a time, so readers can observe a
  mixed topology generation. Diskdb endpoint caches similarly use
  `retain -> insert`, creating an observable partial-refresh window.
- KV store/group lookup, disk-group/disk lookup, client read-selection
  counters, endpoint statistics, and the chunk lifecycle lock registry are on
  request paths. Their values are either immutable `Arc`s or atomics after
  lookup, so the DashMap shard lock adds cross-key contention without
  protecting the value's actual state.
- `DdbDiskGroup::tentative_blocks` documents a hard maximum but implements it
  as `len -> arbitrary first entry removal -> insert`. Concurrent insertions
  can exceed the maximum, and the removed entry is not necessarily the oldest.
- `ServiceDiscoveryClient` drops its cache guard before I/O, which is safe, but
  simultaneous expiry allows many callers to refresh the same service. This is
  a group-0 load amplification issue rather than a data correctness issue.

No audited production path was found to carry a DashMap guard across `.await`.
The per-chunk `tokio::Mutex` remains an intentional same-chunk serialization
boundary; this requirement only removes the unrelated DashMap shard lock used
to find that mutex.

Root designs affected by the implementation are
`doc/design/kv/design-crowdb-kv.md`,
`doc/design/kv/design-crowdb-kv-server.md`,
`doc/design/kv/design-crowdb-kv-rpc.md`,
`doc/design/kv/design-crowdb-kv-rpc-client.md`,
`doc/design/rpc/design-crowdb-rpc-diskdb-migration.md`,
`doc/design/diskdb/design-crowdb-diskdb-space-metrics.md`, and
`doc/design/chunkdb/design-crowdb-chunkdb.md`. Several currently call DashMap
lock-free and must be corrected even for retained uses.

Concrete scenarios include a cloned diskdb client missing a newly learned
`disk_id -> disk_group_id` route, a duplicate chosen notification leaving a
permanent learner gap entry, an old RPC timeout clearing a replacement
connection pool, and a topology reader selecting replicas from a different
generation than its leader entry.

## Solution

Replace correctness-sensitive and request-hot DashMap uses with shared,
generation-aware lock-free structures; retain low-frequency registries only
where their lock behavior is explicit and bounded.

1. **Freeze and classify the inventory**

   - Add a repository check that records every allowed production DashMap use
     by module and purpose. A new use fails the check unless the allowlist says
     why it is off the hot path and why guard lifetime is bounded.
   - Keep benchmark-only and test-only DashMaps outside the production gate.
   - Initially allow the management operation registry and per-group snapshot
     handle registry. They are low-frequency lifecycle data, have no guard
     across `.await`, and do not justify an RCU rewrite.
   - Allow the service-discovery cache temporarily, but add per-service
     single-flight refresh and replace its mutable `usize` round-robin value
     with an atomic cursor. Reassess removal only after measuring group-0
     discovery traffic.

2. **Make diskdb client clones share routing state**

   - Change `DiskdbClient` so every clone shares endpoint and disk ownership
     state. Do not rely on `DashMap::clone`.
   - Publish the complete `disk_group_id -> endpoint` refresh as one immutable
     RCU snapshot. Keep incrementally learned `disk_id -> disk_group_id`
     entries in a shared lock-free map, or merge them into a versioned routing
     snapshot without losing updates concurrent with refresh.
   - Apply the same atomic endpoint-snapshot pattern to
     `app/crowdb-chunkdb/src/allocator/pool.rs`; a reader sees either the old
     complete map or the new complete map, never the `retain -> insert` middle.

3. **Repair and replace learner concurrency maps**

   - Replace `out_of_order` and `applied_out_of_order` with a lock-free ordered
     slot structure, using the existing `crossbeam-skiplist` dependency unless
     a bounded atomic slot window benchmarks better.
   - Make the frontier update algorithm close the delayed-insert race. After
     insertion it must recheck the frontier and remove its exact stale entry;
     a delayed duplicate must not remove a newer replacement node.
   - Replace the `dedup` shard-locked mutation path with a lock-free client
     index and a fixed-size per-client atomic window. Preserve exact
     `(client_id, sequence) -> slot` lookup, the 64-entry retention floor, and
     idempotent duplicate recording.

4. **Make RPC connection pools generation-safe**

   - Replace each transport's DashMap with a lock-free endpoint index whose
     value is an immutable or atomically replaceable `Arc<PoolState>` carrying
     a generation and connection vector.
   - Establish replacement connections without holding an index guard. Install
     a completed pool with compare-and-swap; a losing installer drops its
     duplicate pool without replacing the winner.
   - Return the pool generation with the selected connection. A retryable
     failure may invalidate the cache only when the current entry is the same
     generation. A late failure from an old connection cannot clear a newer
     pool.
   - Use the same pool abstraction in KV client, Paxos, diskdb client, chunkdb
     client, and KV forwarding transports instead of maintaining five subtly
     different cache algorithms.

5. **Publish routing and ownership as RCU snapshots**

   - Merge each KV client's leader and replica routes into one immutable
     `TopologySnapshot` published with `ArcSwap`. A refresh publishes one
     generation. A `NotLeaderHint` update uses an RCU compare-and-swap loop so
     a stale refresh cannot silently overwrite a newer hint.
   - Replace `KvStoreRegistry`, `PxKvStore::groups`, and
     `DdbDiskGroupContainer::disk_groups` with lock-free read snapshots or a
     lock-free ordered map. Preserve atomic replace-if-current behavior and
     cancellation of a replaced Paxos group's old tenure.
   - Publish `DdbDiskGroup::disk_index` as an immutable RCU map rebuilt with
     disk membership changes. Keep allocation's existing
     `allocating_disks: ArcSwap<_>` publication coherent with that update.

6. **Remove shard locks from client and chunk lifecycle selection**

   - Move per-group read round-robin cursors and per-endpoint statistics into
     `Arc` route objects containing atomics, reached from the RCU topology
     snapshot. Remove stale statistics when their route generation retires.
   - Store write-slot high-watermarks in a lock-free key index with atomic
     `fetch_max` values. Topology eviction removes only the matching route
     generation so a delayed eviction cannot delete a reused group's newer
     high-watermark.
   - Replace `ChunkLockMap::locks` with a lock-free ordered map. Acquisition
     retains an exact entry/`Arc<Mutex<()>>` before awaiting the mutex. Idle
     reaping removes only that exact entry when no waiter or owner retains the
     mutex; it must not remove a replacement installed for the same chunk ID.

7. **Give tentative allocations a real bounded policy**

   - Replace `tentative_blocks` with an allocation-timestamp-ordered lock-free
     index plus an atomic size budget, or explicitly redefine the limit as a
     measured soft limit. The default implementation must evict the oldest
     incarnation and converge back to the configured bound under concurrent
     insertion.
   - Preserve the durable-KV fallback after eviction and exact-identity removal
     used by commit/free reconciliation.

8. **Update design claims and benchmark every replacement**

   - Update all affected permanent documents in the Problem section. Do not
     describe a sharded-lock map as lock-free.
   - Record read/write contention and throughput before and after changes. A
     replacement that regresses the matching regression sentinel is not
     accepted without a documented trade-off and explicit approval.

## Dependencies

- Uses the existing `crossbeam-skiplist` and `arc-swap` workspace dependencies.
  Introducing a different concurrent-map dependency requires benchmark and
  memory-reclamation justification in the working design.
- Independent of R101's CAS wire and Paxos semantics. R101 must continue to use
  its lock-free `cas_transient_map` and must not add another DashMap while R149
  is pending.
- RPC pool consolidation touches the transports delivered by R115, R116, and
  R117 but does not change their wire protocol.
- Diskdb direct-free work may consume the shared routing-state fix, but R149
  does not depend on direct free landing first.

## Acceptance

- Given two clones of one `DiskdbClient`, when clone A refreshes an endpoint or
  learns a disk route, clone B immediately resolves the same shared state and
  does not use its pre-clone snapshot. Shared-route invariant. Unit test.
- Given a diskdb endpoint refresh concurrent with routing reads, every read
  observes either the complete old snapshot or the complete new snapshot and
  never a partially retained map. Atomic-publication invariant. Unit test.
- Given duplicate chosen notifications paused between frontier read and insert,
  when another task advances past that slot, the delayed task leaves no entry
  below `contiguous_chosen`. Chosen-frontier cleanup invariant. Unit test.
- Given the same schedule for asynchronous apply completion, the delayed task
  leaves no entry below `contiguous_applied` and every applied waiter is
  notified after contiguous progress. Applied-frontier cleanup invariant. Unit
  test.
- Given more than 64 concurrent and duplicate records for one client, exact
  retained sequences resolve to their committed slots, duplicates do not
  create extra entries, and an unrecorded sequence remains a miss. Dedup-window
  invariant. Unit test.
- Given concurrent cold connection acquisition for one endpoint, exactly one
  complete pool generation becomes current and no map/index guard is held
  during connect. Single-install invariant. Integration test.
- Given generation N fails after generation N+1 is installed, invalidating N
  leaves N+1 current and usable. Connection-generation invariant. Integration
  test.
- Given concurrent topology refresh, `NotLeaderHint`, and route reads, a reader
  obtains leader and replicas from one published generation and a stale update
  cannot overwrite a newer generation. Topology-coherence invariant. Unit
  test.
- Given replacement of a live Paxos group, readers see either the old or new
  `Arc<PxGroup>`, the old tenure is cancelled once, and no request observes a
  missing intermediate entry. Group-replacement invariant. Integration test.
- Given concurrent lookup and idle reaping of the same chunk lock, all callers
  that overlap in time serialize on the same mutex and an old reaper cannot
  remove a replacement entry. Per-chunk serialization invariant. Unit test.
- Given concurrent tentative inserts above the configured capacity, the cache
  converges to the bound by evicting oldest allocation timestamps and every
  evicted lookup can fall back to durable KV. Bounded-cache invariant. Unit
  test.
- Given the production DashMap inventory check, any unclassified new use fails
  while approved management, snapshot, and temporary discovery uses pass.
  Hot-path lock policy invariant. Unit test.
- Given the completed replacement, KV read/write, RPC, diskdb, and chunkdb
  regression sentinels remain within their configured thresholds. Hot-path
  performance invariant. Integration test.

Run:

- `pixi run -- cargo test -p crowdb-kv --test paxos_test learner`
- `pixi run -- cargo test -p crowdb-kv --test rpc_migration_test`
- `pixi run -- cargo test -p crowdb-kv --test store_test multi_group`
- `pixi run -- cargo test -p crowdb-kv-client`
- `pixi run -- cargo test -p crowdb-diskdb-client`
- `pixi run -- cargo test -p crowdb-chunkdb-client`
- `pixi run -- cargo test -p crowdb-chunkdb --test lifecycle_test`
- `pixi run -- cargo test -p crowdb-diskdb --test disk_alloc_test`
- `pixi run -- bash tools/bench-kv-read-regression.sh`
- `pixi run -- bash tools/bench-kv-write-regression.sh`
- `pixi run -- bash tools/bench-rpc-regression.sh`
- `pixi run -- bash tools/bench-diskdb-regression.sh`
- `pixi run -- bash tools/bench-chunkdb-regression.sh`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run -- cargo clippy --all-targets -- -D warnings`
