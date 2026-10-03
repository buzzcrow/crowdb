<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R207: chunkdb — Repo chunk metadata and tasks on chunk-kv

Status: Deferred by user beyond the current direct-KV scope. This document
records the selected future architecture and its unresolved contracts. Start
implementation only when this follow-up is selected and its publication,
range-layout and migration policies have been settled. R202 does not depend
on completing this requirement.

#### Problem

All production ChunkDB Chunk records, maintenance tasks, task indexes and
reservations currently use `CrowdbKvClient` and direct Paxos KV groups.
Their routing cache loads an independently published 1024-slot storage map
whose eligible destinations are selected nonzero groups; group 0 holds only
control-plane bindings. System and user-data lifecycle/task runtimes have
separate authority and execution capacity. S3 and IcebergTable have distinct
chunk types,
but neither has a chunk-kv metadata backend. Existing chunk-kv functionality
must not be mistaken for an already connected ChunkDB persistence path.
See [ChunkStore](../../app/crowdb-chunkdb/src/storage.rs),
[TaskStore](../../app/crowdb-chunkdb/src/task/store.rs),
[routing](../../app/crowdb-chunkdb/src/routing.rs) and
[startup](../../app/crowdb-chunkdb/src/main.rs).

Growing user-data metadata and maintenance traffic therefore grow Paxos
storage and write load. The goal is to move that user-data state onto chunk-kv
while retaining the system state needed to operate and recover chunk-kv on
direct KV. Changing the client alone cannot preserve the current publication,
task and recovery contracts.

The [ChunkDB root design](../design/chunkdb/design-crowdb-chunkdb.md) describes
the existing lifecycle; [fixed slot routing](../design/chunkdb/design-crowdb-chunkdb-range-binding.md) owns the
current type/layer definitions and direct-KV partition contract. The
[chunk-kv design](../design/chunkds/design-crowdb-chunk-kv.md) supplies ordered
storage ranges. This follow-up owns their integration, not a replacement
partition engine or a global transaction manager.

##### Audited gaps and potential failures

Source inspection on 2026-10-03 found the following; no future-path performance
or runtime correctness is claimed by this audit.

- **Backend and lifecycle dispatch:** ChunkStore and TaskStore use concrete
  direct-KV clients. Allocation, updates, listing, reservations, task scanning
  and owner queries have no complete repo backend path. A shared backend switch
  would not establish the required separate system/repo operation logic.
- **Recursive dependency:** chunk-kv's journal uses chunk-stream, whose
  `MirrorChunkWriter` currently allocates `Stream`. Routing all Stream chunks
  to chunk-kv before distinguishing system `Wal` would make journal metadata
  depend on the storage it must recover. Owner-key validation and reopen also
  currently expect Stream. The binding's `owner_kind="chunk-kv-partition"`
  does not itself change the allocated type. See
  [mirror writer](../../lib/crowdb-chunk-client/src/chunk/mirror_chunk_writer.rs),
  [owner validation](../../lib/crowdb-protocol/src/chunk_stream.rs) and
  [storage assembly](../../app/crowdb-chunk-kv-server/src/storage.rs).
- **Atomic publication:** chunk creation writes a Chunk, finalize task and
  index using direct-KV `batch_write_cas`. Task transitions update canonical
  state and secondary indexes together; reservation operations conditionally
  publish chunk and reservation records together. Chunk-kv's current
  [batch_mutate](../../lib/crowdb-chunk-kv-client/src/compose/batch.rs) is
  explicitly non-transactional; its point mutations do not expose an equivalent
  conditional multi-key commit. Same-range placement alone does not add it.
- **Ordered key placement:** `/chunk/`, `/reservation/` and binary task tags
  have different key prefixes; task indexes order priority/time before chunk
  identity. Direct KV explicitly selects their destination group. Routing the
  same bytes independently by ordered range may separate related records.
  Even a shared prefix does not prevent a later split inside that prefix.
  This is a future chunk-kv integration concern, not a cross-group transaction
  gap in R202: its hash of the owning chunk ID routes all associated records
  to the same direct-KV group regardless of their encoded key order.
  `ChunkTaskKey.partition_id` currently carries a chunk identity, not a
  chunk-kv catalog partition ID. See
  [task keys](../../lib/crowdb-protocol/src/key/chunk_task.rs) and
  [reservation storage](../../app/crowdb-chunkdb/src/storage/reservation.rs).
- **Task isolation:** current startup wires a shared task store/manager/scanner/
  executor pipeline. Common ready/finalize/expired-lease scans do not define
  separate persistent system/repo task domains. Moving only Chunk records
  would leave tasks behind or make them undiscoverable.
- **Unsafe reclamation:** `SegmentOwnerResolver` reads both chunk references
  and active task targets. An unavailable repo backend must remain an error;
  interpreting it as absence could free a repair target or live payload.
  See [owner resolver](../../app/crowdb-chunkdb/src/task/owner.rs).
- **Residual Paxos traffic:** stream cursor advancement still calls
  `advance_chunk_write`, persisting lower-layer Chunk metadata through direct
  KV. Stream manifests/extents and tree root catalogs also use direct KV.
  Offloading upper-layer records does not prove that total Paxos operations
  per user mutation fall. See
  [cursor advancement](../../lib/crowdb-chunk-stream/src/production_chunk.rs),
  [stream metadata](../../lib/crowdb-chunk-stream/src/kv.rs) and storage assembly.
- **Ambiguous migration:** S3/Iceberg records and tasks already in direct KV
  cannot be switched merely by changing a type-routing constant. Old and new
  clients, expired task claims, incomplete copies and lost cutover replies
  need one durable storage authority. Source and destination revisions are
  backend-local and must not be treated as interchangeable CAS tokens.

#### Solution

##### Selected future distribution

Use [AGENTS.md terminology](../../AGENTS.md#terminology): repo chunk is a
collective discussion term for user-data chunks, never a generic implementation
type. Inherit the type and separate operation contracts from
[ChunkDB type and operation domains](../design/chunkdb/design-crowdb-chunkdb.md#39-chunk-types-for-different-use-cases).

- **System:** Wal journal chunks, BtreePage data chunks and PageIndex chunks
  holding persistent tree page mapping tables keep their Chunk metadata,
  associated maintenance tasks, indexes and reservations in hash-selected
  nonzero direct Paxos KV groups under the R202 routing contract.
- **Repo:** S3, IcebergTable and business Stream chunk metadata, together with
  their tasks, indexes and reservations, move to chunk-kv. Future Dataset chunks
  follow this layer with their own type; this requirement does not create a
  Dataset API or claim that its type is already implemented.
- **Payload:** all chunk bytes remain on DiskIO-managed storage allocated by
  DiskDB. This requirement changes the location of metadata and task state,
  not the physical payload placement.
- **Bootstrap/control state:** group-0 bindings/catalogs, stream manifests and
  extent pages, and tree root catalogs keep their explicit lower-layer
  authorities. Page mapping payload in PageIndex chunks is distinct from
  those chunks' metadata and from the root references used to locate them.
  Relocating these control collections is outside this requirement.

The dependency direction is:

```text
repo operation domain -> chunk-kv metadata range -> system WAL/tree/PageIndex chunks
                                                    -> direct KV metadata + DiskIO payload
```

System recovery must not traverse the repo operation domain. A system task
may repair a physical tree chunk containing repo records; it remains a system
chunk task. It must not claim or execute the repo tasks stored inside that tree.

##### Operation and range contracts

- Dispatch by explicit type/purpose into separate system and repo lifecycle
  orchestration. Reads, mutations, listing, task RPCs, reservations and owner
  reconciliation all retain the layer. Codecs, wire envelopes, payload IO and
  task algorithms may be reused; persistence authority and admission remain
  independent. A repo outage never triggers direct-KV fallback after cutover.
- Chunk-kv already partitions by ordered range. Map the repo domain to those
  ranges while preserving ChunkDB service routing as a separate authority.
  R202 owns service partition topology; this requirement consumes it rather
  than redefining a storage range whenever a service owner changes.
- Define the unit of conditional publication within one storage range. Either
  add explicit range-local multi-record atomic commit with split-safe placement,
  or use canonical single-key publication plus a reviewed recoverable index/
  task-discovery design. The latter must retain required lifecycle outcomes.
  Do not wrap non-transactional batch_mutate and label it atomic.
- Define range-local task namespaces, due/lease scan scopes, claim ownership
  and dispatch admission. Ordinary repo lifecycle does not require a global
  due-time queue or cross-range transactions. Split/transfer must preserve the
  chosen publication unit and task discoverability; a key prefix alone is not
  a split fence. Namespace routing must not accidentally claim existing S3/
  Iceberg object or catalog keys hosted by chunk-kv.
- Separate system/repo task runtimes so upper-layer backlog or unavailable
  storage cannot exhaust system task admission. Same-process versus separate
  processes and exact capacity budgets remain open implementation choices.
- Coordinate a tree snapshot's page and mapping generations using the R202
  PageIndex contract. Do not publish a root that requires its own unavailable
  mapping table to find the mapping chunks during cold recovery.

##### Upgrade and failure outcomes

- Bootstrap the lower system path before enabling repo operations. Verify the
  target catalog, publication contract and task namespace before activation.
  Until explicit enablement, the current direct-KV behavior remains unchanged.
- Backend migration covers the canonical chunk, tasks, indexes and reservations
  as one recoverable logical unit. Persist migration identity and generation,
  define source/destination authority at each stage, fence old writes/claims,
  reconcile lost replies, and retire source records only after recovery proves
  the destination authoritative. Exact online/offline policy is still open.
- Repo storage unavailable after cutover: return explicit retryable failures;
  do not accept writes in direct KV or report data absent for reclamation.
  Independently provisioned system operations and task admission remain usable.
- Task execution or client response is uncertain: preserve operation identity
  and reconcile durable state before retrying; duplicate work must not publish
  two state transitions or release a resource twice.
- Old service/storage generations after split or handoff: reject stale mutation
  authority and refresh routing. Read fallback is allowed only by the reviewed
  migration/read-consistency contract, not by endpoint reachability alone.

##### Invariants

- **I1 — Explicit distribution.** System metadata/task state stays in direct KV;
  enabled repo metadata/task state uses chunk-kv, with unchanged payload placement.
- **I2 — Acyclic recovery.** System lookup, journal progress, mapping recovery
  and maintenance never require repo metadata service availability.
- **I3 — Separate operation domains.** System and repo lifecycles, task scans,
  claims and admission retain independent authority despite shared code.
- **I4 — Range-local consistency.** Publication and legal split boundaries
  preserve required chunk/task/reservation outcomes and complete task discovery.
- **I5 — Durable authority.** Cutover, restart, stale caches and lost replies
  retain acknowledged state and admit only the current writer/task authority.
- **I6 — Safe reclamation.** Unavailable metadata is not absence; current chunk
  references and active task targets protect their exact allocation incarnation.
- **I7 — Measured relief.** Report total Paxos bytes, records and operations per
  user mutation, including lower-layer cursor/checkpoint/task traffic, at equal
  durability and workload. Do not claim savings from record relocation alone.

##### Work items

1. In ChunkDB lifecycle/storage/routing and startup, add the repo chunk-kv
   operation path and explicit enablement while retaining the R202 system path.
   Cover point operations, listing, reservations and administrative/task RPCs.
2. In the protocol key model and chunk-kv client/server, select and implement
   range-local publication, namespace placement, legal split boundaries and
   recovery. Define backend-specific revision and retry identity semantics.
3. In TaskStore/manager/scanner/executor and maintenance coordinators, connect
   the isolated repo task domain to chunk-kv. Preserve claim fencing, finalize
   discovery, conversion, repair, relocation and reservation cleanup.
4. In SegmentOwnerResolver and DiskDB ownership queries, retain the complete
   chunk/task proof across both domains, failures, storage transfers and retry.
5. In routing catalogs and migration control, implement the selected backend
   conversion and restart protocol. Reuse R103's service handoff and direct-KV
   partition migration authority/recovery principles; never
   use a service binding flip as proof that records were copied or fenced.
6. In chunk-kv startup/storage assembly, verify typed system WAL and PageIndex
   prerequisites, bootstrap order and cold recovery before serving repo data.
   Existing legacy identities follow the reviewed R202 compatibility policy.
7. Measure the current direct-KV baseline and complete integrated path under
   equivalent S3/Iceberg and business-stream workloads. Include task load,
   outage/recovery behavior, memory, latency and all residual Paxos traffic.
   Update permanent designs only with the implemented contract.

#### Dependencies

- [fixed slot routing](../design/chunkdb/design-crowdb-chunkdb-range-binding.md) owns current direct-KV routing,
  type cleanup, typed Wal/PageIndex and separate operation/task domains. This
  requirement consumes those boundaries. It must not block R202 completion or
  force a premature change to its all-KV-group scope.
- [R103](R103-chunkdb-range-migration.md) owns service handoff and direct-KV
  partition data migration for group expansion/shrink. This requirement extends
  its authority/recovery principles to a different backend; direct-KV group
  migration and direct-KV-to-chunk-kv conversion remain separate operations.
- Existing chunk-kv client/server ordered-range split/transfer are primitives.
  [R144](R144-chunk-kv-partition-merge.md) merge remains deferred; do not promise
  merge for repo metadata before a reviewed implementation exists.
- Publication semantics and key/split rules are prerequisites to enabling the
  new path. Their absence keeps this follow-up disabled; it does not justify
  weakened atomicity or an implicit runtime fallback.
- Preserve the implemented [tree engine](../design/tree/design-crowdb-tree-engine.md)
  concurrent admission and prefix-safe flush handoff. Backend migration must
  retain its acknowledged-write and recovery guarantees.

#### Acceptance

- Given current deployment configuration without repo backend enablement,
  allocate/update chunks and tasks; assert all remain in direct KV and require
  no new repo backend connection (I1). Integration test.
- Given enabled system/repo domains, exercise allocation, query, listing,
  mutations and reservations for supported types; assert selected metadata/
  task destinations, independent operation paths and unchanged payload route
  (I1, I3). Integration test.
- Given chunk-kv cold startup and system Wal/BtreePage/PageIndex chunks, recover
  tree and journal before repo activation; assert no recursion into repo
  metadata and consistent page/mapping recovery (I2). Integration test.
- Given repo storage outage and task backlog, run system lookup/maintenance;
  assert system admission remains usable and repo operations fail explicitly
  without direct-KV fallback or cross-domain claims (I1–I3). Integration test.
- Given chunk/task/reservation publication, interrupt each selected commit or
  recovery boundary and lose replies; restart/retry and assert preserved
  lifecycle outcomes and discoverable required tasks (I4, I5). Integration test.
- Given related canonical/index keys and existing non-ChunkDB application keys,
  route them and attempt legal/illegal splits; assert namespace isolation and
  preservation of the selected publication unit (I3, I4). Integration test.
- Given ready and leased tasks in a transferred or split range, resume scanning
  and stale workers; assert complete discovery, current claims only and no
  duplicate resource release (I3–I5). Integration test.
- Given a DiskDB segment named by a repo repair task, query ownership while
  chunk-kv is unavailable and after recovery; assert uncertainty retains the
  segment and the exact task target remains protected (I6). Integration test.
- Given existing direct-KV repo chunks and associated state, run the selected
  migration with crashes before/after cutover and lost replies; assert one
  authoritative backend, preserved acknowledgements and safe cleanup (I1, I5,
  I6). Integration test.
- Given concurrent service handoff and a supported storage transfer, issue
  stale client/task operations; assert generations are reconciled independently
  and service ownership changes do not invent storage authority (I5).
  Integration test.
- Given invalid target configuration or an unsupported legacy migration, enable
  the backend; assert rejection before authority changes and preservation of
  source records (I1, I5). Integration test.
- Given equal-durability representative workloads, compare baseline and
  integrated paths; assert the user-approved performance budget and report all
  residual Paxos traffic and task/recovery overhead (I7). Integration test.

#### Open Questions

- Range-local conditional multi-key commit, or canonical single-key state with
  recoverable derived indexes/tasks? Choose based on publication/recovery
  complexity and storage-engine support; same-range routing alone is insufficient.
- What exact key namespace and indivisible split unit preserve publication and
  due/lease discovery? Prefer a range-local design; compare the cost of aligned
  index placement with explicit recoverable index maintenance.
- Should existing repo state move through online fenced migration, a maintenance
  window, or an explicitly approved reset for development deployments? Define
  rollback only where authority has not been irrevocably transferred.
- Separate processes or separately admitted runtimes in one process, and what
  capacity budgets keep system repair available under repo backlog?
- What measured Paxos byte/operation and latency budget constitutes success?
  Backend separation and lower metadata cardinality alone do not quantify it.

The following are implementation verification commands, not tests run by this
documentation-only change. Add reviewed migration/outage cases to the affected
suites when implementing this requirement.

```sh
pixi run cargo test -p crowdb-chunk-kv --test partition_test --test journal_failure_test
pixi run cargo test -p crowdb-chunk-kv-client --test routing_test --test compose_test
pixi run cargo test -p crowdb-chunk-stream --test production_chunk_test --test stream_test
pixi run cargo test -p crowdb-chunkdb --test routing_test --test lifecycle_test --test owner_metadata_test --test conversion_test
pixi run cargo test -p crowdb-protocol --test chunk_task_key_test --test chunk_task_value_test
pixi run rs-fmt-check
pixi run cargo clippy -p crowdb-protocol -p crowdb-chunk-client -p crowdb-chunk-stream -p crowdb-chunk-kv -p crowdb-chunk-kv-client -p crowdb-chunk-kv-server -p crowdb-chunkdb -p crowdb-chunkdb-client --all-targets -- -D warnings
```
