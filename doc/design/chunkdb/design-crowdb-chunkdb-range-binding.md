<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: ChunkDB Slot Routing

Depends on: [ChunkDB architecture](design-crowdb-chunkdb.md),
[KV group 0](../kv/design-crowdb-kv-group0.md).
Satisfies: ChunkDB stateless execution and explicit nonzero persistence.

ChunkDB uses independent service and storage maps over the same fixed hash
space. This document covers their publication, request routing, task authority,
and fixed/dynamic service ownership policies.

## Table of Contents

- [1. Slot identity and invariants](#1-slot-identity-and-invariants)
- [2. Control-plane records](#2-control-plane-records)
- [3. Publication and startup](#3-publication-and-startup)
- [4. Client and storage routing](#4-client-and-storage-routing)
- [5. Maintenance authority](#5-maintenance-authority)
- [6. Costs and operating limits](#6-costs-and-operating-limits)
- [7. Data-group execution fences](#7-data-group-execution-fences)
- [8. Durable handoff records](#8-durable-handoff-records)
- [9. Submission epoch policy](#9-submission-epoch-policy)

## 1. Slot identity and invariants

`ChunkSlot::for_chunk` hashes the canonical 16-byte big-endian ChunkId with
XXH64 seed zero and takes modulo 1024. Slots are 0..1023. The hash includes
both ID words and the concrete purpose prefix. Neither server count nor group
count participates in this calculation.

- **I1 — Independent owners:** each slot has one service instance and one
  selected nonzero `(store_id, group_id)` destination. A service bitmap can
  span several storage groups, and a storage group can serve several instances.
- **I2 — Complete publication:** each map covers all slots exactly once, with
  no overlaps, missing owners, malformed bitmaps, or mixed generations.
- **I3 — Chunk-local atomicity:** chunk, canonical task, maintenance indexes,
  and reservation records route by the owning ChunkId. Their byte-key order
  never selects a group. Conditional multi-record writes remain group-local.
- **I4 — Fail closed:** missing routes, unavailable selected groups, invalid
  types, and zero-slot service owners cannot become group-0 or any-owner fallbacks.
- **I5 — Fixed authority:** under fixed policy, refreshing an initialized map may not change its
  generation or ownership. Endpoint changes do not change either slot map.

These are hash partitions, distinct from chunk-kv ordered key ranges. The
maps do not place payload bytes in KV; payload remains on DiskIO/DiskDB.

## 2. Control-plane records

Group 0 contains two independent map namespaces:

- `/chunkdb/slot_service/<instance_id>`: one service binding per instance,
  including an all-zero bitmap for an instance owning no slots.
- `/chunkdb/slot_storage/<store_id>/<group_id>`: one storage binding per
  explicitly selected nonzero group.
- `/chunkdb/slot_head/service` and `/chunkdb/slot_head/storage`: independent
  heads containing layout version, generation, and owner count.

Each binding contains owner identity, generation, and a 1024-bit bitmap.
JSON encoding adds metadata overhead; the raw bitmap is exactly 128 bytes.
These routing maps have no per-slot or per-contiguous-range records. Service
endpoints are registered separately and can refresh after same-instance restart.

## 3. Publication and startup

`ChunkSlotMapClient` validates the full bootstrap and publishes each map's
head and owner records in one head-conditional group-0 batch. A retry reads and
reconciles the committed result; a conflicting initializer fails. Both maps
must exist before ChunkDB accepts allocations, even though the maps have
independent publication boundaries.

Map reads use fixed-cutoff pagination and recheck the head revision before
publishing a validated immutable cache. Legacy range bindings, orphan records,
and existing group-0 chunk/task/reservation state prevent unsupported bootstrap.
They are preserved for an explicit conversion rather than silently rewritten.

Startup requires an explicit instance identity and validated storage/service
maps. Optional bootstrap configuration supplies the eligible groups and
service instances. The deterministic bootstrap uses balanced service slot bands
and interleaves slots among storage groups; this assignment is initial policy,
not the definition of a slot. Readiness requires initialized maps and valid
storage routing. An instance with zero service slots is ready but rejects work
outside its empty bitmap; local disks are not a service-readiness requirement.
The fixed-layout monitor audits ownership and does not reassign
slots when a heartbeat expires. Legacy assignment writers reject updates.

Console automatic service plans deploy the selected ChunkDB cohort before
starting Chunk-KV journal bootstrap. A serial deployment must not await journal
allocation while another published ChunkDB owner still needs to start.

## 4. Client and storage routing

Ordinary Rust clients load only the service map and service discovery.
`RangeBindingClient` retains an immutable map and endpoint snapshot. Existing
chunk operations resolve the ChunkId slot; generated allocation selects a live,
nonempty service owner, whose server generates an ID within its owned bitmap
using bounded retries. Caller-supplied IDs are checked before side effects.

`RangeGuard` admits only the installed instance bitmap. `BindingCache` builds a
constant-time storage lookup from the independent storage map. Neither hot
routing path introduces a mutex. Cold restart loads durable records from remote
KV; local locks, caches, and in-flight operations are not persistence authority.

Native tree clients use a retained service resolver with asynchronously refreshed
immutable snapshots. Per-call route leases retain old connections through
completion, and the page store retains resolver/DiskIO owners for its lifetime.
Native RPC completion pools are separate from ordinary Rust RPC pools.
Explicit `NotMyRange` permits bounded refresh and retry. Unknown allocation or
mutation outcomes return to the caller without blind resubmission; read-only
queries can refresh and retry. No original-owner fallback is supported.

## 5. Maintenance authority

One process hosts separate system and user-data lifecycle/task runtimes.
Wal, BtreePage, and PageIndex are system purposes; Stream, S3, and IcebergTable
are user-data purposes. Each runtime has its own scan scope, claims, queue,
execution capacity, and recovery. Shared transport, codecs, IO, and algorithms
do not create shared task authority.

Ready, lease, and finalize indexes encode domain and slot before scheduling
fields. Scans select owned slot runs in each eligible destination and paginate
before applying result limits; eligible work is merged by its scheduling order.
Canonical tasks retain the owning chunk ID. Stores reject foreign slots and
domains at read, admission, claim, and publication boundaries. Execution verifies
the durable claim before handler side effects; claim generations and writer
epochs fence delayed completion after same-owner restart.

The reservation budget is apportioned by service slot share and split equally
between system and user-data runtimes. Execution limits are independently
reserved; shared reservation gauges aggregate domain contributions. Domains can
share a physical Paxos group and therefore still share its availability limits.

## 6. Costs and operating limits

For S service owners and G storage destinations, binding records total S + G,
plus two heads. Raw bitmaps total `128 * (S + G)` bytes. With three instances
and three groups this is six owner records and 768 raw bitmap bytes, plus heads
and encoding overhead. Each initial map requires one conditional consensus
batch; allocation does not rewrite a binding record.

Each loaded map builds a 1024-entry lookup. Hot route lookup is constant-time
and allocation-free. Map reload costs scale with owner records and paginate
in bounded batches. Maintenance scans scale with owned contiguous slot runs
and selected destinations, rather than allocating a worker or tree per slot.
Arbitrary fragmented bitmaps are supported but increase scan requests; balanced
bootstrap bands reduce that cost. Slot balance does not guarantee byte or load
balance when a few chunks dominate traffic.

Under fixed policy, topology is fixed while serving. Same-owner restart, endpoint refresh, and
Paxos replica failover preserve both maps. Adding/removing service owners or
storage groups, remapping slots, and legacy conversion require an external
fenced handoff/migration protocol and are currently rejected. Service changes
need task-authority handoff; storage changes must move complete chunk/task/
index/reservation state before ownership cutover. Neither operation implicitly
moves DiskIO payload. Chunk-kv is not a ChunkDB metadata backend; its ordinary
batch API does not supply the required conditional multi-record atomicity.

## 7. Data-group execution fences

KV supports a separate execution fence at `/chunkdb/ownership-fence/<slot>`
inside the selected data group. The slot segment uses canonical decimal in
0..1023; slot fence operations reject group zero. This namespace is independent
of routing-map records in group 0 and of DiskDB's ownership namespace.
Fixed-policy ChunkDB does not initialize or
use these execution fences.

`ChunkSlotAuthority` identifies an instance, a nonzero 128-bit process
incarnation and a nonzero per-slot generation. A same-ID process restart has
a distinct incarnation. The canonical comparison value has exactly 33 bytes:
version 1, big-endian instance ID, incarnation bytes and big-endian generation.
Unknown versions, zero identity fields and malformed keys are rejected. A
routing-map generation and a slot-authority generation are separate values.

`batch_write_owned` compares the applied authority without mutating its fence
record. Business-record revision CAS remains independent and must be supplied
for conditional chunk/task transitions. Ordinary Put, Delete and Batch cannot
mutate either reserved ownership namespace. A conditional ownership batch
may change only the reserved key used as its revision precondition; slot
fences cannot be deleted or replaced by malformed authority values.

Owner writes use concurrent atomic admission for one fence key and leader
tenure. A fence CAS closes that admission, drains previously admitted work
through apply, and then publishes the changed value. Other slots retain their
own admissions. Cancelling the caller does not release a pending proposal;
an unresolved proposal blocks handover and readiness until the leader's
recovery barrier resolves it. Topology replacement preserves the admission
state, while a new leader tenure requires its own recovery barrier. Delayed
writes retaining the old comparison value fail after fence publication.

## 8. Durable handoff records

Group zero stores one service handoff cohort at `/chunkdb/slot_handoff/service`.
`ChunkServiceHandoff` validates its source service generation, unchanged storage
generation, distinct moved slots, previous and target incarnations, selected
data groups and per-slot fence receipts. Target authority advances exactly one
generation; a missing previous authority is reserved for bootstrap without
admitted writers. Transfer and receipt ordering is canonical.

The phases are Prepare, Fence, Publish and Activate. A cohort is reserved with
revision CAS on the complete service-map head; its unchanged head value and
the prepared record are written atomically. Advancing the head revision keeps
concurrent controllers from reserving different cohorts against the same source.
An unfinished cohort is resumed rather than replaced.

Data-group fencing requires a persisted Fence-phase snapshot. An already
applied target identity is reconciled using its revision instead of issuing
another CAS. Otherwise the source value must match the recorded previous
identity. Unknown outcomes are reread through the KV recovery barrier, without
choosing another target or epoch. Before saving a new receipt, the client checks
the target authority and applied revision in its selected data group.

Progress updates CAS the observed handoff revision. They cannot change cohort
identity, remove receipts or return to an earlier phase. Decoding rejects
duplicate slots, conflicting receipts and Publish/Activate records without
complete fence coverage. Publish cannot be saved as a standalone progress
update: it belongs in the same transaction as complete routing and authority
publication. Fixed-policy execution does not initiate handoff or activate
dynamic assignments.

## 9. Submission epoch policy

Dynamic service ownership is selected explicitly with `service_ownership =
"dynamic"`; fixed deployments keep their existing policy. Console deployment
uses `dynamic_ownership` to select the policy when initializing a new layout.
An initialized fixed deployment cannot silently switch policy. Storage groups
and DiskIO payload placement are unaffected by service ownership changes.

- **I6 — Slot-local admission:** each execution captures its slot owner and
  ownership epoch. Immediately before each client KV submission, RangeGuard
  compares those values against its current immutable snapshot. Updating slot
  10 does not invalidate an execution on unchanged slot 5. Global publication
  generation controls snapshot freshness; it is not a slot ownership epoch.
- **I7 — Epoch monotonicity:** owner change, regrant and same-ID restart advance
  the affected durable slot epochs. Returning to a previous owner never reuses
  an epoch. Incarnation remains diagnostic data in the existing authority
  encoding, with no independent admission comparison. It cannot replace durable
  epoch advancement.
- **I8 — Submission boundary:** entering client KV accepts that submission.
  Accepted work may finish after ownership changes. The race between the final
  local check and submission is intentional. No strict exclusion of old writes
  after cutover is promised; existing business-record CAS governs conflicts.

The complete service and authority maps share a publication generation.
Authority bitmaps are keyed by the canonical hexadecimal authority encoding
under `/chunkdb/slot_authority/`; their head is `/chunkdb/slot_head/authority`.
Publication CASes the service head and writes both complete tables and heads
atomically. Readers validate coverage, paired owner IDs and matching generations
and recheck the service-head revision. Epoch rollback and owner changes that
reuse an epoch are rejected. Lost publication replies reconcile the already
published map instead of incrementing epochs again.

RPC dispatch captures authority before spawning asynchronous execution. Task
claims retain their capture through queueing, handler work, renewals and
completion. Maintenance operations capture the snapshot for their scan/work
iteration. Cache entries carry the captured owner/epoch, so a delayed old fill
cannot supply metadata to an execution under a newer epoch. Submission checks
use local constant-time lookup without network requests, new locks, sleeps or
old-write draining. The data-group fence/handoff APIs described above are
separate primitives and are not used by this policy; DiskDB fencing is preserved.

An ownership rejection before submission is a routing signal. Clients refresh
the complete service snapshot and reexecute on its owner, including the same
process under a newer epoch. An old execution never adopts a newer epoch while
retaining its old state. The Rust RPC client keeps the same request correlation ID through explicit
ownership retries and refreshes immediately, under one ten-second operation
budget. Business operation IDs and revision CAS remain unchanged. Unknown
submitted transport outcomes return `OutcomeUnknown` for mutation callers to
reconcile; they never enter the ownership retry path. Read-only operations may
refresh and retry.

The version-3 Group 0 monitor balances compatible prepared services advertised
under `/srv/chunkdb-epoch-v1/`. It uses the persisted descriptor's heartbeat and
failure grace bounds, stable instance-ID tie-breaking, a one-slot count
hysteresis and a 64-slot movement budget per tick. Missing observations retain
a durable first-suspected timestamp across monitor replacement; recovery clears
that timestamp so a later failure gets a fresh grace period. Only the exact
Group 0 leader tenure can publish a complete revision-conditional batch. The
fixed version-2 monitor continues to audit without reassigning slots.
