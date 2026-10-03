<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R103: chunkdb — Dynamic Slot Ownership and KV-Group Expansion/Shrink

Status: Deferred by user decision on 2026-10-03. The implemented fixed topology
uses three nodes and three selected nonzero Chunk-storage KV groups, plus
control-plane group 0. Start this follow-up when dynamic ownership or storage
capacity changes are requested.

#### Problem

[fixed slot routing](../design/chunkdb/design-crowdb-chunkdb-range-binding.md)
establishes two independent maps over 1024 fixed chunk-ID hash slots:

- An ordinary client resolves slot to ChunkDB server. Group 0 stores one bitmap
  record per server, including servers owning zero slots.
- ChunkDB resolves slot to a selected nonzero Paxos KV group (pxgroup). Group 0
  stores one bitmap record per group. All records belonging to a chunk follow
  its chunk ID into that group.

The initial R202 delivery freezes these assignments. It supports fixed-owner
restart and recovery, but does not make changing either bitmap safe. ChunkDB
servers perform chunk lifecycle and heavy task/IO orchestration without an
authoritative local chunk database. Changing a server owner therefore requires
execution-authority handoff, while changing a storage group requires moving
durable state. These operations have different costs and failure modes.

The current [slot routing design](../design/chunkdb/design-crowdb-chunkdb-range-binding.md)
rejects ownership changes and describes the fixed service/storage maps.
Service-only handoff avoids metadata copying only when storage placement
remains unchanged. Existing ChunkDB routing, range guards and task
claims are useful foundations, but neither a routing refresh nor a grace-period
timer proves exclusive authority or complete storage transfer.

Concrete scenarios are adding a ChunkDB server for more processing capacity,
draining a server, taking over slots after a failed owner, adding a KV group for
capacity, evacuating a group before removal, and resuming an interrupted move.
A source group may hold slots served by several servers; a server may own slots
stored in several groups. A one-to-one server/group assumption breaks both
handoff and migration.

#### Solution

##### Goal and scope

Make the fixed R202 layout safely changeable without changing chunk identity or
losing acknowledged chunk/task state. Deliver two independently usable operations:

1. **Service slot handoff.** Transfer selected slots between ChunkDB server
   bitmaps. Keep storage-group bindings and persistent records in place.
   Revoke old execution authority and recover tasks on the new server.
2. **Storage slot migration.** Transfer selected slots between pxgroup bitmaps
   when adding, draining, removing or rebalancing storage groups. Move the
   complete associated record set before activating the new destination.
   Keep service assignments unchanged unless a separate handoff is requested.

A supported combined change must coordinate these operations explicitly; it
must never infer one mapping from the other. Initial implementation may serialize
overlapping changes rather than execute both concurrently.

In scope:

- Explicit move requests, destination validation, durable progress, independent
  authority generations, retry/recovery and scoped cleanup.
- Service join/leave/failure handoff and stale client/worker rejection.
- KV-group expansion/shrink and slot redistribution within the direct-KV backend.
- Migration of canonical Chunk records, all associated tasks and task indexes,
  reservations and other per-chunk transaction participants.
- Safe refusal when source completeness or exclusive authority cannot be proven.
- Observable move status and an unambiguous completion condition for operators.

Out of scope:

- R202's initial fixed layout, chunk-type cleanup and PageIndex implementation.
- Changing the 1024-slot space, rehashing IDs, or hash-by-current-group-count.
- Moving DiskIO payload or reallocating DiskDB blocks as a side effect.
- Moving stream catalogs, tree roots or unrelated records merely because they
  share a KV group; ownership is determined by the per-chunk record contract.
- Direct-KV-to-chunk-kv conversion, which belongs to
  [R207](R207-chunkdb-repo-metadata-chunk-kv.md).
- Automatic cluster-wide capacity planning, creating/removing Paxos replicas,
  or deleting a shared KV group. Removing a group from Chunk storage is distinct
  from destroying the group.
- Mandatory conversion of pre-R202 layouts or legacy Repo/Stream/PageIndex data.
  R202 uses fresh test state. A legacy group-0 source may be supported later only
  with an explicit compatible record/layout conversion contract; it is not a
  prerequisite for this requirement's R202-layout migration.

##### Fixed granularity and publication

- The move unit is one or more existing logical slots, selected by the owning
  chunk ID. Related task IDs and encoded key order do not choose the destination.
  Unmoved slots retain their hash meaning and bindings.
- Keep one steady-state bitmap record per server and per pxgroup. Do not create
  a binding record, KV group or worker for each of the 1024 slots. Durable
  per-operation progress is separate from these owner records.
- Publish each changed map as one complete committed generation. Readers cannot
  observe independently cleared source bits and set target bits as active
  authority. Service and storage maps have separate generations.
- Use group-0 atomic publication for routing/control state, but do not mistake it
  for an atomic transaction with copied records in other groups. Preparation,
  authority revocation, validation and activation form a recoverable protocol.
- Fence mutations at their authoritative acceptance/publication boundary. A
  cached ownership check before asynchronous work is insufficient. Cover late
  completions, task claims, reservation updates and operations already admitted
  before revocation; authority cannot depend only on a process-local lock.
- Preserve group-local conditional atomic chunk operations. Source and target
  revisions belong to their own groups; transferred values cannot make an old
  source revision a valid target CAS token.

##### Service handoff

1. Validate source/target servers and requested slots against the current map;
   persist operation identity and expected service generation.
2. Stop or revoke old admission and task authority for those slots. Resolve
   admitted work and persisted claims using a durable fence/recovery contract.
   Failure detection alone is not proof that an old server stopped running.
3. Publish source/target server bitmaps as one committed service generation.
   Activate the new owner only under the new authority.
4. Refresh ordinary clients and recover task execution from the unchanged KV
   groups. Preserve claim generations and prevent stale completion publication.
5. Complete when the new owner serves the slots and obsolete execution grants
   cannot commit. Do not copy a local ChunkDB database or change storage bitmaps.

An empty source bitmap means no remaining partition work, not allow-all.
An unreachable owner may delay handoff until fencing is proven; it must not
force a second active owner for availability.

##### Storage migration and group drain

1. **Prepare.** Validate a provisioned nonzero eligible target and persist the
   slot set, source/target groups, expected storage generation and progress.
   A newly added group has no authority simply because it is eligible.
2. **Copy.** Enumerate complete per-chunk state for the selected slots, including
   indexes ordered by time/priority rather than chunk ID. Choose a bounded
   slot-write pause or a consistent snapshot plus reliable change capture.
   Handle creations, updates, deletes and claims without omissions or resurrected
   records. A partial target copy is never serving authority.
3. **Fence and reconcile.** Coordinate every server that owns affected slots;
   prevent old-group writes and delayed task publication. Bring the target to
   the final acknowledged source state and validate all transaction participants.
4. **Cut over.** Publish source/target pxgroup bitmaps as one storage generation
   only after completeness and fencing are proven. Enable operations against
   the target; leave service bitmaps unchanged for a storage-only move.
5. **Clean up.** Retire old reads/claims safely and remove only moved source
   records. Cleanup and retries are idempotent. Lost replies are reconciled
   from durable authority/progress rather than interpreted as failure or success.

For shrink, mark a group draining, stop new assignments to it, move all its slots
and finish relevant cleanup. Remove it from the Chunk-storage eligible set only
when no ownership or migration obligation needs it. Unrelated group contents
remain intact. Group expansion/shrink never changes the fixed hash function.

##### Failure and cancellation outcomes

- Before source revocation, a failed target leaves the source authoritative.
  Retry or abandon a partial copy without exposing it to clients.
- During the fenced interval, operations may fail or retry; never acknowledge
  writes that have no durable authoritative destination.
- If the source is unavailable before target completeness is proven, wait for
  source recovery or a proven complete recovery source. Do not promote a partial
  copy to finish a drain.
- After cutover, target unavailability does not reactivate the old source.
  Restore the target or perform a new fenced move.
- Controller restart or an uncertain cutover reply resumes from persisted
  operation identity and mapping generation before granting authority or cleanup.
- Cancellation before cutover must reconcile fences and partial target data;
  cancellation after cutover cannot roll back by restoring old bitmap bits.
- Serialize or explicitly reject conflicting requests on the same slots.
  Independent slots may progress within bounded copy/scan/task resources.
- Unavailable owner metadata is uncertainty, not evidence that payload is unused.
  Preserve system/repo task separation throughout failures and recovery.

##### Invariants

- **M1 — Exclusive authority.** Only the current service and storage authorities
  can commit chunk mutations or task outcomes, including delayed work.
- **M2 — Complete movement.** All acknowledged per-chunk state and maintenance
  discoverability survive; atomic-operation participants share one active group.
- **M3 — Recoverable progress.** Retries, restart and uncertain replies cannot
  create authority from an incomplete copy or reactivate an obsolete source.
- **M4 — Safe retirement.** Cleanup is scoped and idempotent; drain completion
  requires zero remaining ownership and migration obligations.
- **M5 — Independent layers.** Service-only changes copy no metadata;
  storage-only changes preserve service assignments. Neither moves payload,
  changes slot identity nor merges system/repo task authority.
- **M6 — Coherent maps.** Each active slot has one owner in each committed map;
  intermediate records and partial publications grant no authority.

##### Work items

1. Extend the KV domain monitor and ChunkdbRangeStrategy beyond fixed-layout
   initialization with explicit service/storage move admission, conflict checks,
   durable progress and separately published bitmap generations.
2. Extend RangeGuard, lifecycle handlers and task manager/scanner/executor with
   service revocation and durable stale-publication rejection; integrate the
   native tree and Rust client routing paths completed by R202.
3. Extend ChunkStore, TaskStore and reservation persistence with complete
   slot-state enumeration/copy/validation under the selected change-capture
   strategy. Preserve destination-local CAS semantics.
4. Implement storage fencing, recovery, cancellation and idempotent cleanup;
   expose move/drain progress and actionable failure state.
5. Verify independent and combined changes, resource bounds and failure cases;
   update permanent ChunkDB routing/migration documentation.

#### Dependencies

- [fixed slot routing](../design/chunkdb/design-crowdb-chunkdb-range-binding.md) supplies tested fixed slots,
  independent bitmap maps, typed operations, task isolation, all client paths
  and group-local atomicity. Start from its completed fixed-layout contract.
  Until R103 is implemented, keep initialized assignments unchanged; do not
  enable the old automatic rebalance path as a substitute.
- Existing direct-KV scan/snapshot/transfer facilities are candidate primitives.
  Confirm snapshot consistency, delete capture and hash-slot enumeration before
  choosing a copy strategy. Ordered key-range transfer alone is insufficient.
- [R207](R207-chunkdb-repo-metadata-chunk-kv.md) may reuse this authority/recovery
  model later but needs its own backend transaction and placement contract.
  R207 does not block either operation in R103.

#### Acceptance

- Given the completed R202 fixed topology, request a server-only slot move;
  assert new-owner service, unchanged group bindings/data, no payload copy and
  zero-slot old-owner rejection (M1, M5, M6). Integration test.
- Given a failed or partitioned old server, request takeover and reconnect its
  delayed workers; assert takeover waits for proven fencing and stale writes,
  reservations and task completions fail after activation (M1, M3). Integration test.
- Given an eligible additional group, move selected slots while handling allowed
  foreground/task traffic; assert complete target state, preserved acknowledgements,
  unchanged service map and unmoved slots (M1, M2, M5). E2E test.
- Given a group serving slots owned by several servers, drain it; assert all
  affected owners follow storage cutover and removal waits for cleanup, while
  unrelated data and DiskIO payload remain intact (M2, M4, M5). E2E test.
- Given non-contiguous chunk/task/index/reservation keys, copy with creates,
  deletes and claims; assert complete enumeration, no resurrected deletes,
  discoverable tasks and valid destination-local CAS (M2). Integration test.
- Given interrupted source/target bitmap publication, reload both maps; assert
  readers use complete generations with one authority per slot and no partial
  target activation (M3, M6). Integration test.
- Given source/target/controller failure or lost replies at every move stage,
  restart and retry; assert acknowledged data survives, incomplete targets are
  not promoted and obsolete sources never reactivate (M1–M3). Integration test.
- Given cancellation before and after cutover, reconcile the operation; assert
  safe pre-cutover termination or forward recovery after cutover, with no bitmap
  rollback that restores obsolete authority (M1, M3, M4). Integration test.
- Given conflicting service/storage changes for the same slots, submit both;
  assert explicit serialization/rejection or a proven coordinated transition,
  never inferred mapping changes or two writers (M1, M5, M6). Integration test.
- Given interrupted/repeated cleanup alongside unrelated slots, resume cleanup;
  assert no duplicate release, foreign deletion or premature group retirement
  (M3, M4). Integration test.
- Given invalid group 0 targets, incompatible layouts or attempted slot resizing,
  submit a move; assert rejection without modifying current authority (M5, M6).
  Unit test.
- Given both operation domains and unavailable ownership metadata during a move,
  scan tasks and query payload ownership; assert no cross-domain claims and
  uncertainty does not authorize reclamation (M1, M2, M5). Integration test.
- Given bounded migration concurrency under foreground load, run independent
  moves and inspect status; assert configured resource limits, observable progress
  and eventual completion after recoverable faults clear (M3, M4). E2E test.

#### Open Questions

- Prefer a bounded write pause per moved slot, or online snapshot plus reliable
  catch-up? The former reduces protocol complexity but pauses affected writes;
  the latter needs proven change capture and increases implementation cost.
- Should group-add select slots automatically from measured load, or initially
  require explicit slot selection? Manual selection keeps the first migration
  contract smaller; automatic policy needs agreed load/capacity objectives.
- What pause/latency and migration resource budgets are acceptable? Establish
  measured limits before claiming online migration performance.

Implementation selects the durable fence and state schema against R202's actual
interfaces; these are required technical proofs, not permission to weaken the
invariants. This rewrite ran no runtime tests. Extend the named suites with the
acceptance cases before treating these commands as migration coverage:

```sh
pixi run cargo test -p crowdb-chunkdb --test routing_test --test lifecycle_test --test owner_metadata_test --test conversion_test
pixi run cargo test -p crowdb-chunkdb-client --test client_test
pixi run cargo test -p crowdb-kv-client --test chunkdb_partition_test
pixi run cargo test -p crowdb-kv-server --test domain_monitor_test
pixi run test-tree-ct
pixi run rs-fmt-check
pixi run cargo clippy -p crowdb-chunkdb -p crowdb-chunkdb-client -p crowdb-kv-client -p crowdb-kv-server --all-targets -- -D warnings
```
