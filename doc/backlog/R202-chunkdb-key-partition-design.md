<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R202: chunkdb — Key partition model and storage ownership

Status: Implementation remains deferred for architecture review. Current scope
is explicitly all Chunk metadata and associated tasks hashed to selected nonzero
direct Paxos KV groups. Group 0 is excluded from their storage destinations.
First complete and verify this model; later
[R207](R207-chunkdb-repo-metadata-chunk-kv.md) migrates selected user-data state
to chunk-kv. R207 implementation and its unresolved publication contract do not
block this requirement.

#### Problem

ChunkDB currently has service hash-range bindings and a separate KV store/group
mapping used by ChunkStore and TaskStore. The selected model has two independent
maps over fixed chunk-ID hash slots: dynamic per-server slot bitmaps identify a
ChunkDB instance, while pxgroup bitmaps identify each slot's persistent group.
A service range need not map to one pxgroup. The previous
1024-range default created 1024 group-0 records even with one instance; the
current 12-range default is a temporary implementation workaround. The selected
replacement separates fixed logical hash granularity from persisted record
count: use a bitmap per destination pxgroup, not one record per logical ID.
This storage mapping does not replace the separate dynamic server-slot map.
Each map has one record per owner: one per ChunkDB server for execution and
one per pxgroup for storage, never one per slot or contiguous service range.
Here pxgroup means a Paxos KV group.

The [root design](../design/chunkdb/design-crowdb-chunkdb.md) and
[range binding design](../design/chunkdb/design-crowdb-chunkdb-range-binding.md)
need explicit type, operation-domain, storage and migration boundaries. This
requirement establishes them on the existing direct-KV backend before adding
another backend.

##### Current implementation and gaps

Source inspection on 2026-10-03 found:

- `ChunkStore` and `TaskStore` hold `CrowdbKvClient`; startup shares a
  `BindingCache` initialized with `default_binding_table(0, 0)`. All supported
  Chunk records, tasks, indexes and reservations currently use direct KV.
  The group-0 default is an implementation gap to remove: high-volume Chunk
  metadata and maintenance state must not occupy the control-plane group.
  This is not a partially active chunk-kv path.
  See [storage](../../app/crowdb-chunkdb/src/storage.rs),
  [task store](../../app/crowdb-chunkdb/src/task/store.rs),
  [routing](../../app/crowdb-chunkdb/src/routing.rs) and
  [startup](../../app/crowdb-chunkdb/src/main.rs).
- Types are `Repo=0`, `Wal=1`, `BtreePage=2`, `PageIndex=3`, `Stream=4`, `S3=5`
  and `IcebergTable=6`. S3/Iceberg adapters use their specific types, while
  generic writer defaults, CLI benchmarks and wire fallback conversions still
  use Repo. See [types](../../lib/crowdb-protocol/src/types/chunkdb.rs) and
  [writer defaults](../../lib/crowdb-chunk-client/src/config.rs).
- Chunk-kv journal storage uses chunk-stream, but `MirrorChunkWriter` allocates
  Stream and expects it on reopen. Owner-key validation allows stream identity
  only for Stream; system Wal is not yet used by this allocation path. See
  [writer](../../lib/crowdb-chunk-client/src/chunk/mirror_chunk_writer.rs) and
  [owner validation](../../lib/crowdb-protocol/src/chunk_stream.rs).
- Tree page mapping is already persistent: snapshot code writes mapping segment
  images, a directory and an anchor through the same page store as page data.
  The chunk backend's transport allocates BtreePage for the packed logical
  image. No dedicated PageIndex allocation was identified. Separate mapping
  allocation is missing; the mapping table itself is not missing or volatile.
  See [snapshot persistence](../../lib/crowdb-tree/src/snapshot/persist.cpp),
  [chunk page store](../../lib/crowdb-tree/src/backend/chunk/chunk_page_store.cpp)
  and [transport](../../lib/crowdb-tree/src/backend/chunk/rpc_chunk_transport.cpp).
- One task store/manager/scanner/executor pipeline and common index scans do
  not yet separate system and repo operation domains. Task `partition_id`
  carries a chunk identity, not a service-range or storage-partition identity.
  See [task keys](../../lib/crowdb-protocol/src/key/chunk_task.rs).
- Chunk/finalize-task creation, task/index transitions and reservation updates
  depend on direct-KV conditional atomic batches. Keep their related records
  in the selected group and preserve those guarantees during partition changes.
- Stream bindings live in group 0; stream manifests/extents and chunk-kv tree
  root catalogs use configured nonzero direct KV groups. These control records
  are distinct from Chunk records and need not use ChunkStore's group.

Examples requiring a clear contract include starting one instance without
unjustified binding overhead, adding an instance without accidentally moving
storage, transferring a metadata group without losing a finalize task, and
persisting a mapping table in a separately typed chunk that can be found on
cold recovery.

#### Solution

##### Current scope and staging

- All system and repo Chunk metadata, tasks, task indexes and reservations
  use selected nonzero direct Paxos KV groups via chunk-hash routing. Group 0
  stores the relevant routing configuration/bindings, not per-chunk state.
  Sharing a backend does not require sharing
  one group, task namespace or operation domain. Exact group mapping is part
  of this requirement's partition decision.
- Chunk payload bytes remain on DiskIO-managed storage allocated through
  DiskDB. Saying "all chunks in KV groups" here refers to their metadata and
  maintenance state, not their payload bytes.
- Complete type definitions, separate operation logic, task isolation,
  service/storage partition mapping and safe change on this backend first.
  Validate it independently with the user-data chunk-kv backend disabled.
- R207 subsequently migrates selected repo metadata and associated state to
  chunk-kv. Its range-local publication API, ordered-key layout, migration and
  Paxos-relief measurements are outside current implementation scope.

##### Hash partitions and group routing

- Hash chunk IDs into a fixed logical ID space, with 1024 IDs (`0..1023`) as
  the current design baseline. Conceptually `logical_id = H(chunk_id) % N`,
  where `N` and the hash/ID-encoding rules are fixed for the initialized layout.
  N is not the current server count, KV-group count or binding-record count.
- Two-stage routing intentionally separates heavy chunk-operation execution
  from persistence capacity. ChunkDB handles chunk lifecycle transactions and
  IO/task orchestration; persistence is one part of that work. Servers keep no
  authoritative local durable chunk/task state: pxgroups hold it, so another
  server can take over assigned slots without copying a local chunk database.
  Ephemeral caches, in-flight operations and local coordination may exist;
  statelessness does not eliminate fencing or task-claim recovery at handoff.
- Store one group-0 binding record per selected pxgroup containing a bitmap
  of its logical IDs. Bit i means that the group owns data for logical ID i.
  A 1024-bit bitmap is 128 bytes before record metadata/encoding overhead.
  Do not create 1024 records, groups, trees or workers for those 1024 bits.
- A group's bitmap can describe disjoint logical IDs. A logical ID, also called
  a slot, is a stable data-placement unit, not automatically an independently
  provisioned storage object or service binding record.
- **Client-to-service map:** group 0 holds one binding record per ChunkDB
  server, whose value contains the 1024-bit bitmap of selected slots. A server
  owns 0..X slot ranges/sets, including disjoint slots, represented together in
  that one record. "Range" here does not require a contiguous interval or a
  separate record. Dynamic assignment changes server bitmap membership without
  resizing or rehashing the slots. An all-zero bitmap means no partition work,
  never permission to serve every chunk.
- **Service-to-storage map:** ChunkDB resolves the same slot through per-pxgroup
  bitmaps to a selected nonzero `(store_id, group_id)`. A service range may
  contain slots stored in different groups, and one group's slots may be
  handled by different ChunkDB instances. There is no one-to-one alignment
  requirement between service ranges and group bitmaps.
- The ordinary chunk client computes the slot from chunk ID and uses only the
  per-server bitmap map to find the ChunkDB endpoint. It does not need pxgroup
  placement to issue chunk RPCs. The selected ChunkDB instance enforces its
  service ownership and uses the storage map to locate persistent records.
  These are hash-slot routes, not ordered ranges of encoded record keys.
- Persist server bitmap assignments separately from group bitmap bindings in
  group 0. With S servers and G selected groups, the steady-state owner-binding
  tables contain S + G records and 128 * (S + G) bytes of raw slot bitmaps at
  N=1024, excluding record metadata, generation heads and migration progress.
  Neither table expands into per-slot or per-contiguous-range records.
  Give the two maps separate generation/fencing semantics: service rebalance
  changes client routing; storage migration changes ChunkDB's group routing.
  Both bitmap maps require complete coverage and unique current authority over supported
  slots, but changing one map must not silently change the other.
- In each active routing domain, every logical ID has exactly one owning group:
  published bitmaps cover the full fixed space with no overlap. Readers must
  observe one complete committed mapping generation. A CAS on one group's
  record alone does not atomically clear a source bit and set a target bit;
  the group-0 publication protocol must prevent mixed-generation authority.
  A derived in-memory logical-ID lookup may accelerate routing without adding
  one persistent record per ID.
- Define an explicit eligible set of nonzero KV groups for these destinations.
  Group membership in the cluster alone
  does not make a group eligible to store Chunk records.
- A chunk's canonical record and associated task/index/reservation records
  follow the owning chunk's route, not independent hashes of their record keys.
  Every task belonging to that chunk therefore resides in the same group.
  Preserve existing group-local conditional atomic updates; the selected
  per-chunk lifecycle has no cross-group transaction requirement. Key ordering
  can support scans within a group but does not choose the destination group.
- Provision and validate the selected groups and publish complete routing before
  accepting allocations. Empty/missing routing, a group-0 destination or an
  unavailable selected group cannot trigger a fallback write to group 0.
- Reassigning slots between server bitmaps does not itself
  move records or alter pxgroup bitmaps. It requires fenced service/task
  handoff. A service range does not implicitly require its own group or tree.
- Logical IDs and their hash meaning do not split, merge or resize during
  normal scaling. More fixed IDs provide finer assignment/migration granularity
  without more per-ID records; balance still depends on actual workload skew.
- Adding or removing selected groups triggers migration of selected logical
  IDs and all their associated data under
  [R103](R103-chunkdb-range-migration.md), with an explicit fenced routing
  generation before ownership bits change. The fixed ID space is unchanged.
  Recomputing `hash % current_group_count`, dynamically changing N or flipping
  bits without migrating their data must not silently relocate authority.
- Existing per-chunk state in group 0 needs an explicit source-to-nonzero-group
  upgrade before the new path takes authority. Source reads/copy for that
  upgrade are not a supported steady-state group-0 storage mode; retire source
  state only after the migration proves all related records safe.

##### Chunk types and operation domains

Use [AGENTS.md terminology](../../AGENTS.md#terminology) for repo chunk as the
collective discussion term for user data. Do not retain a generic Repo type.

- **Wal:** system journal chunks, including chunk-kv WAL implemented through
  chunk-stream. Preserve this dedicated type and stream identity across
  allocation, reopen, rollover and repair.
- **BtreePage:** persisted system tree page data.
- **PageIndex:** persistent tree page mapping table payload. Retain the type
  and separate its allocation from BtreePage. Keep enough bootstrap references
  to locate mapping chunks without first loading the mapping they contain.
- **S3 and IcebergTable:** distinct user-data types in the repo operation domain.
- **Stream:** non-system business stream data in the repo domain; chunk-stream
  is a reusable mechanism, not a reason to type system WAL as Stream.
- **Dataset:** future user-data chunks get their own type. This document does
  not claim Dataset chunk support already exists or introduce its API.

All of these types' metadata and tasks use direct KV in current scope. System
and repo allocation, mutation, query/list, reservation, recovery and maintenance
have separate lifecycle orchestration and task authority. Shared codecs, IO,
wire envelopes and task algorithms may be reused, but a mixed task queue with
only a late type filter is not the selected isolation model. Separate process
placement is not mandatory; persistent scopes and admission must be explicit.

##### Invariants

- **I1 — Explicit ownership.** Distinguish chunk type/domain, service owner,
  direct-KV store/group, storage tree owner, task authority and payload placement.
  Each server owns 0..X chunk-ID hash partitions; each partition identifies its
  service owner. The independent per-slot bitmap map identifies storage; a
  service partition may span groups and does not imply a tree of its own.
  ChunkDB is stateless with respect to durable chunk/task state; local caches
  and in-flight work do not become a second persistence authority.
- **I2 — Complete typed routing.** Every supported chunk/key has exactly one
  hash-selected nonzero direct-KV authority. Published per-group bitmaps cover
  the fixed logical ID space once, without gaps or overlap. Independently,
  published per-server bitmaps cover each slot with one current ChunkDB owner;
  invalid or retired types cannot fall back to generic Repo, and missing or
  failed storage routes cannot fall back to group 0.
- **I3 — Consistent lifecycle.** Chunk/task/index/reservation updates retain
  their required conditional atomic outcomes and discoverable maintenance.
  All records belonging to one chunk follow that chunk ID's hash route into
  one group. Per-chunk lifecycle uses group-local transactions, not cross-group
  transactions or independent task-key routing.
- **I4 — Separate operations and tasks.** System and repo domains have separate
  lifecycle ownership, scan/claim scope and dispatch admission even on the same
  backend. Logical isolation does not imply failure isolation from a KV group
  that both domains physically share.
- **I5 — Safe change.** Service handoff, logical-ID data transfer and legacy
  layout conversion have distinct fenced cutovers. Restart, stale caches and
  uncertain replies preserve acknowledgements and reject obsolete authority.
- **I6 — Typed durable bootstrap.** Wal purpose survives stream recovery; page
  data uses BtreePage and mapping persistence uses PageIndex. A published
  snapshot references a consistent recoverable generation without mapping
  lookup recursion. Unavailable ownership metadata never proves a segment absent.
- **I7 — Compact fixed granularity.** Use fixed logical IDs, one bitmap binding
  per ChunkDB server and one per selected group, not per-ID/range records. Measure distribution,
  routing/cache, maintenance and migration costs for the 1024-ID baseline.
  Twelve old service ranges do not define the new logical ID space.

##### Work items

1. In protocol types, writer defaults, RPC conversions and CLI producers,
   remove generic Repo allocation/default/fallback behavior, retain Wal and
   PageIndex, and preserve surviving wire values. Define legacy-data handling
   without silently reinterpreting old chunk IDs or reusing retired values.
2. In MirrorChunkWriter, ProductionStreamRuntime, owner validation and chunk-kv
   storage assembly, propagate durable stream purpose so system journals use
   Wal and business streams use Stream across their complete lifecycle.
3. In tree snapshot persistence, ChunkPageStore, pack pipeline and transport,
   carry page/mapping purpose into allocation. Define typed references,
   directory/anchor placement, snapshot publication, cold restart, split sharing
   and reclamation for PageIndex and BtreePage chunks.
4. In ChunkDB lifecycle/storage/task wiring, separate system/repo orchestration
   and task domains while keeping both on direct KV. Cover allocation, listing,
   reservations, finalize, conversion, repair, relocation and owner queries.
   Preserve the existing group-local conditional publication contract.
5. In routing, ChunkdbRangeStrategy, RangeGuard and KV domain monitor, wire
   fixed logical IDs, the dynamic client-facing per-server bitmap map and the
   independent ChunkDB-facing pxgroup bitmap map. Publish/validate their
   generations separately and support 0..X service ranges per server.
   Persist one service binding per server and one storage binding per pxgroup;
   do not persist per-slot or per-contiguous-service-range records. Route every associated
   record by its owning chunk ID; reject work on servers owning no partitions.
   Remove the group-0 startup default, validate/provision destinations and gate
   writes on complete routing. Fix the hash/ID encoding for the layout and
   measure assignment granularity and independently scoped layer mappings.
6. Define the layout/generation interface consumed by R103 for service handoff
   and KV-group expansion/shrink data migration, including legacy group-0 state.
   R103 owns copy/catch-up, fencing, cutover, recovery and source cleanup.
   Define upgrade of the existing 12-/1024 service-range records to one bitmap
   record per server, separately from populating pxgroup bitmap bindings.
   Existing 16-bit bucket/service-range IDs are not automatically the selected
   fixed logical IDs. Service assignment records do not become storage bitmap
   records. Both binding writers reject
   mixed layouts and incompatible hash-space definitions; conversion preserves
   or explicitly migrates the data behind each mapping.
7. Verify the direct-KV implementation and record supported limits, initialization/
   WAL cost, lookup/cache cost and maintenance/migration overhead. Update
   permanent designs to match the implemented current contract. Leave repo
   backend migration to R207 after this stage is stable.

#### Dependencies

- [R103](R103-chunkdb-range-migration.md) owns service handoff and partition data
  migration when selected KV groups are added, removed or remapped. Select its
  partition/authority interface alongside this requirement. Owner-only changes
  retain storage; group changes move complete chunk/task/index/reservation
  state. Neither operation implicitly moves DiskIO payload.
- Existing direct-KV transfer/split facilities are candidate storage primitives.
  If a required conversion is unavailable, preserve the existing layout rather
  than rewriting populated boundaries.
- [R207](R207-chunkdb-repo-metadata-chunk-kv.md) is an outgoing follow-up only.
  It consumes this requirement's stable types, operation domains and routing
  contract; its backend/publication decisions are not R202 prerequisites.
- [R201](R201-tree-memtable-write-handoff.md) fixes independent tree write/flush
  correctness. Smaller binding batches or skipped tests do not fix that race.

#### Acceptance

- Given supported system/repo types with no repo chunk-kv backend configured,
  allocate, update and recover chunks/tasks; assert every record uses its
  hash-selected nonzero direct-KV group, associated records share the owning
  chunk's group, and payload stays on DiskIO (I1–I3). Integration test.
- Given empty routing, a configured group-0 destination or an unavailable
  selected group, attempt allocation/update; assert validation or explicit
  failure and no fallback per-chunk writes to group 0 (I2). Integration test.
- Given servers owning zero, one and several disjoint hash partitions, route
  requests and task work; assert only the partition owner accepts them and
  zero ownership never becomes allow-all (I1, I2, I4). Integration test.
- Given a chunk with task/index/reservation keys whose encoded byte ranges
  differ, persist and transition them; assert each operation derives the same
  group from the owning chunk ID and commits without a cross-group transaction
  (I2, I3). Integration test.
- Given existing Chunk/task/index/reservation records in group 0, run the
  reviewed migration with crashes and lost replies; assert complete recovery
  in selected nonzero groups, one authority and safe source cleanup (I3, I5).
  Integration test.
- Given invalid/retired values and generic writer callers, allocate a chunk;
  assert explicit type selection, deterministic rejection without Repo fallback
  and unchanged surviving wire values (I2). Unit test.
- Given system and business streams, create/reopen/rotate/repair their chunks;
  assert Wal versus Stream purpose and owner attribution remain correct and
  metadata stays in direct KV (I2, I6). Integration test.
- Given page and mapping changes, snapshot and cold-restart the tree; interrupt
  publication and repeat after split sharing/reclamation; assert BtreePage/
  PageIndex payload separation, coherent snapshot recovery and no bootstrap
  recursion or premature free (I6). Integration test.
- Given tasks of the same kind in both domains, scan/claim/expire/retry and stall
  repo dispatch; assert disjoint lifecycle/claim scopes and independent system
  admission while their KV routes remain available (I4). Integration test.
- Given chunk/task/reservation writes, inject lost replies and restart; assert
  group-local atomic outcomes, required maintenance discovery and rejection
  of stale task publication (I3, I5). Integration test.
- Given a repair target and unavailable owner metadata, query ownership and
  recover; assert uncertainty is not absence and live/task-owned allocations
  remain protected (I3, I6). Integration test.
- Given logical IDs 0..1023 and per-group bitmaps, route all IDs and owning
  chunks/tasks; assert deterministic unique group/owner coverage independently
  of server count and encoded record-key order (I1–I3). Unit test.
- Given missing/overlapping bits, wrong bitmap size or mixed mapping generations,
  load routing; assert rejection without publishing ambiguous authority (I2, I5).
  Unit test.
- Given N=1024, S servers and G selected groups, initialize and reload bindings;
  assert S server records and G group records, each with a 128-byte raw bitmap,
  including an all-zero bitmap for a server with no slots; assert no per-slot
  or per-contiguous-range binding records and
  no per-ID tree/group/worker allocation (I7). Integration test.
- Given a ChunkDB server with ephemeral caches and in-flight tasks, replace it
  with a cold server and hand off its bitmap slots; assert durable state is
  recovered from pxgroups without copying local ChunkDB files and delayed
  old-owner publication is fenced (I1, I4, I5). Integration test.
- Given an ordinary client configured with service routing only and a service
  range spanning slots in several pxgroups, issue chunk RPCs; assert the client
  finds the correct instance without storage bindings and that instance routes
  each chunk and its tasks to the correct group (I1–I3). Integration test.
- Given one group's slots served by several instances, change the per-server
  bitmaps; assert fenced service/task handoff, unchanged storage bitmaps
  and no metadata copying solely for service rebalance (I1, I4, I5).
  Integration test.
- Given a pxgroup bitmap migration with unchanged server bitmaps, update storage
  routing; assert clients retain their service routes while ChunkDB resolves
  the migrated slots to the new group generation (I1, I5). Integration test.
- Given group expansion/shrink, migrate selected IDs through R103; assert N,
  chunk-to-logical-ID mapping and unmoved IDs stay unchanged while the moved
  IDs' full data and published bitmap ownership cut over together (I3, I5).
  Integration test.
- Given service join/leave and a supported KV storage transfer, exercise their
  separate cutovers with stale requests and crashes; assert preserved records,
  current authority and unchanged payload placement (I1, I3, I5). Integration test.
- Given populated legacy 12-/1024 service-range bindings, attempt incompatible
  routing publication from both writers; assert rejection preserves records. Run the
  reviewed explicit conversion; assert no gaps, mixed generations or lost
  metadata/tasks (I2, I5). Integration test.
- Given legacy Repo, system Stream and mapping-in-BtreePage records, apply the
  selected migration policy or reject an unsupported upgrade; assert preserved
  reference/identity semantics and no silent reclassification (I2, I5, I6).
  Integration test.
- Given representative direct-KV deployments, measure initialization, routing,
  memory, task and migration overhead; assert the selected count and supported
  limits meet the user-approved budget (I7). Integration test.

#### Open Questions

- What load policy and granularity govern dynamic slot assignment among server
  bitmaps? Keep their assignment independent of pxgroup bitmap membership and
  compare load, task scheduling and service handoff costs. The two-level routing
  structure and fixed slot space are decided.
- Which eligible nonzero groups host each domain, and what measured skew,
  migration cost and capacity budget guides assignment of the fixed IDs?
- What group-0 publication schema gives readers a complete generation of each
  bitmap map and makes source/target ownership changes recoverable? Select it
  with R103's fencing protocol; one record per owner does not make a two-owner
  bit transfer atomic without a publication protocol.
- Separate system/repo service binding tables or separate operation/task scopes
  over shared service assignments? Choose alongside the group mapping.
- Separate processes or separately admitted runtimes in one process? Both
  preserve logical task isolation; physical group sharing has shared outages.
- Migrate existing type/layout data or require an explicitly approved reset
  for development deployments? Legacy Repo purpose and old system Stream IDs
  cannot be silently inferred or rewritten without their references.

Implementation verification commands; no runtime tests were run by the
requirement-writing change. Run changed C++ format/tree-lint and reviewed tree
mapping/migration suites through `pixi run` when implementing those paths.

```sh
pixi run cargo test -p crowdb-protocol --test chunk_id_test --test chunk_task_key_test --test chunk_task_value_test
pixi run cargo test -p crowdb-chunk-stream --test production_chunk_test --test stream_test
pixi run cargo test -p crowdb-kv-client --test chunkdb_partition_test
pixi run cargo test -p crowdb-kv-server --test domain_monitor_test
pixi run cargo test -p crowdb-chunkdb --test routing_test --test lifecycle_test --test owner_metadata_test --test conversion_test
pixi run rs-fmt-check
pixi run cargo clippy -p crowdb-protocol -p crowdb-chunk-client -p crowdb-chunk-stream -p crowdb-kv-client -p crowdb-kv-server -p crowdb-chunkdb -p crowdb-chunkdb-client --all-targets -- -D warnings
```
