<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: crowdb-tree Chunk Storage

Depends on: [`design-crowdb-tree.md`](design-crowdb-tree.md),
[`design-crowdb-tree-storage.md`](design-crowdb-tree-storage.md),
[`../chunkdb/design-crowdb-chunkdb.md`](../chunkdb/design-crowdb-chunkdb.md)

Satisfies: [`design-crowdb-tree-storage.md`](design-crowdb-tree-storage.md)
§1 (page-store abstraction)

This document defines typed immutable Chunk backing, root publication and
protection, physical collection, and explicit migration of logical-address
manifests. Host ownership and WAL recovery remain in the Chunk-KV design.

## Table of Contents

- [1. Runtime and Backend Ownership](#1-runtime-and-backend-ownership)
- [2. Fixed Packs and Root Identity](#2-fixed-packs-and-root-identity)
- [3. Publication and Recovery](#3-publication-and-recovery)
- [4. Readers and Process Fencing](#4-readers-and-process-fencing)
- [5. Range Rebuild and Immutable Snapshots](#5-range-rebuild-and-immutable-snapshots)
- [6. Typed Pack Collection](#6-typed-pack-collection)
- [7. Explicit Legacy Migration](#7-explicit-legacy-migration)
- [8. Legacy Physical Retirement](#8-legacy-physical-retirement)
- [9. Accounting and Configuration](#9-accounting-and-configuration)
- [10. Correctness Invariants](#10-correctness-invariants)

## 1. Runtime and Backend Ownership

`ChunkRuntimePageStore` is a backend facade injected into `TreeCore` with a
shared `TreeRuntime`. It owns no private execution pool. The host shares CPU,
backend execution, and the resource budget; each Tree retains its own lane,
account, immutable publication state, and cache. Construction requires a
fixed-pack transport, a staging/protection catalog, and a host pin-release
service. Unsupported capabilities fail before an ordinary Tree is opened.

The C opaque store and Rust `PageStore::open_chunk_runtime` retain the catalog
and transport domain. One catalog claim denotes one writer incarnation. Closing
that writer drains accepted work and publishes its quiescence; reopening
requires a fresh claim. Reader and snapshot owners can outlive the Tree.
Their final releases enqueue paid pin work on the host service. The host keeps
that service and the transports available until late owners and accepted
release work have drained.

`ChunkTransport` provides fixed allocation identity, mirrored immutable I/O,
request-age fencing, ordered drain, and deletion receipts. The production
adapter speaks ChunkDB and DiskIO protocols directly. Local-only links do not
extract the Chunk/RPC implementation from the common archive.

## 2. Fixed Packs and Root Identity

`BackingRef` identifies an immutable typed backing. A Chunk pack reference
contains its stage identity, Chunk identity, byte offset, length, checksum, and
purpose. It is not an address into a mutable logical byte image. BtreePage and
PageIndex payloads have distinct Chunk purposes. Purpose alone never proves
ownership or authorizes reuse or deletion.

A stage has a stable operation identity and a fixed allocation identity before
allocation is submitted. Its durable state records allocation and reloadability
progress. Mirrored bytes become reloadable only after all required writes and
barriers succeed. A cancelled or failed attempt retains sufficient stage and
allocation information for later reconciliation; it does not publish partial
logical coverage.

`ChunkRootManifest` contains Tree identity, root generation, owner epoch,
snapshot sequence, page generation, coverage, GC floor, root PID, next PID,
WAL replay offset, and a bounded directory of immutable index packs. Index
entries contain exact frame/backing and dependency references. Root and index
records have independent magic, version, length, identity, and checksum checks.
Root generation and snapshot sequence are separate monotonic identities.

## 3. Publication and Recovery

A checkpoint captures a finite source frontier and immutable page state. It
writes or reuses verified primary backing, prepares the complete reference
set, and commits one manifest candidate against the captured root generation
and writer authority. Candidate preparation is windowed. Reference ownership
precedes the root CAS so a crash cannot expose a root whose dependencies are
collectible.

Only successful durable root publication advances the durable frontier and
WAL replay offset. Handoff coverage and staged writeback do not authorize WAL
trimming. A lost commit reply is resolved by looking up the same operation and
root identity; it is not retried as a different publication.

Ordinary recovery loads the current root. Exact-generation recovery loads only
the named retained root and never falls back to another generation. Absence at
generation zero denotes a new Tree; malformed or unsupported existing bytes
never silently open an empty Tree. Recovery validates the root, each index,
frame identity, graph summaries, and overflow dependencies before publication.
Bounded windows retain backend and protection owners through the final I/O.

A later checkpoint may reuse immutable primary and overflow backing when its
captured identity still matches. Reuse does not imply logical liveness outside
the captured graph and does not turn an uncommitted stage into a durable root.

## 4. Readers and Process Fencing

Reads use the immutable captured mapping and its typed backing domain. Frame
and overflow verification check the requested identity and stored checksums.
The per-Tree cache pins verified immutable frames; cache eviction changes
residency, not publication or durable reachability. Layout validity and read
request-age checks are enforced at the transport boundary.

The catalog binds writer registrations and live pins to the host process
lease. Pin, candidate, root, and collection mutations share an authority
revision. A definite CAS conflict discards derived state and rereads within a
finite retry bound. Transport ambiguity is returned for durable lookup; it is
not treated as a CAS conflict.

Writer quiescence excludes future writes after accepted work drains. It does
not exclude readers. Removing a crashed process's pins requires authoritative
process exclusion, including the applicable request-age boundary, and the
recovery evidence required by the catalog. A new owner epoch alone cannot
erase live old readers. The same-epoch writer incarnation is also checked.

## 5. Range Rebuild and Immutable Snapshots

`rebuild_range` captures a fixed immutable source and copies the selected native
image into independently owned target backing. The target has an independent
root lineage, mapping, PID allocation state, writer claim, and mutable source
set. Source and target mutations cannot affect each other. Logical bounds are
intersected at get/scan entry, including when a physically complete imported
page contains keys outside the target range.

Native and byte snapshot windows carry their allocation and backend owners.
The immutable snapshot interface retains a captured root for repeated get,
scan, and export operations without recapturing live Tree state. Closing the
source Tree does not revoke that captured view. Final owners release their
protection through the host service.

Topology artifacts that name an exact root retain durable generation pins.
The pin is installed before the artifact becomes publishable and is released
only after authoritative catalog state no longer names that root. A retry or
crash may retain the pin longer; it cannot shorten the protected lifetime.

## 6. Typed Pack Collection

Collection reconciles candidate states, retained root references, live pins,
and stage ownership. Retired stages pass through a durable deleting state
before physical side effects. That transition excludes new references and
pins for the reclaimed identity. Unresolved candidate outcomes remain
protected until the same operation can be resolved.

Before deletion, the collector verifies the exact allocation receipt and
excludes late writers and delayed allocation requests. DiskIO ordered drain
completes before storage is released. ChunkDB's durable Deleted record retains
strips while cleanup is incomplete; completion requires the expected identity,
zero capacity, and no remaining strips. A deletion retry resumes the same
identity. The reclaimed stage and physical accounting change only after that
proof, including recovery of a previously acknowledged deletion.

Catalog history pruning and physical deletion are separate operations. A
manifest disappearing from metadata is never by itself proof that its bytes
have been freed. Shared physical accounting deduplicates the immutable
allocation identity across roots and observers.

## 7. Explicit Legacy Migration

Logical-address format-7 roots are accepted only by the explicit read-only
migration adapter. Ordinary runtime open reports an unsupported legacy format
and directs callers to `migrate_legacy_chunk`. The old page-store constructor
is not a fallback writer for runtime Trees.

Migration requires stopped legacy writers and a registered, fenced migration
owner. It pins the exact source generation and validates the full legacy
manifest, segmented references, frame revisions, bounds, checksums, and graph.
It copies the native image into new fixed packs and publishes a typed root
through the normal candidate and authority protocol.

An immutable receipt containing the original manifest and pack directory is
persisted before the root CAS. Until the CAS succeeds the old root remains
recoverable. After an ambiguous reply, migration resolves the current typed
root and that receipt without reissuing an unrelated publication. The exact
source-generation pin remains held while the commit outcome is unknown. It is
released idempotently after the committed typed root is established, or after a
definite failure leaves the original root authoritative. Migration data
completion does not imply legacy physical retirement.

## 8. Legacy Physical Retirement

`ChunkKvStorage::collect_legacy_storage` is an explicit host maintenance entry
for one production metadata namespace. Each call accepts a work limit from 1
through 64; its returned reclaimed counts cover that invocation, while
`complete` covers only persisted, known manifest receipts. Production split
descendants inherit that namespace. Arbitrary sharing across independently managed catalogs is
not covered by this proof.

The collector first installs a durable namespace marker that permanently
rejects new format-7 publication. It then checks each Tree's current root and
freezes legacy reader and generation-pin admission under the same per-Tree
authority CAS used by publication. A current legacy root returns `LegacyRoot`
until migration completes. Existing generation pins must be released through
their real transition protocol.

Publication provenance and each admitted legacy reader are recorded durably.
Complete lineage inventory is established only with its first publication;
a later registration cannot retroactively establish missing history. Pack
owner Trees must belong to that proved namespace inventory.
Every relevant process must have authoritative exclusion evidence; a closed
writer is insufficient. The namespace scan and reader-proof scans retain
bounded cursors. No deletion begins until the namespace proof completes.
History pruning preserves an immutable legacy migration receipt before
removing the corresponding manifest, with at most one newly copied legacy
image in a pruning pass.

For each known Chunk, retirement persists its exact allocation receipt before
ordered drain and deletion. Completion requires the same creation identity and
an acknowledged physical release. Unknown replies retain the claim; a later
invocation queries and resumes that identity. Missing or changed allocation
evidence remains typed pending. Receipts survive completed deletion for
idempotence and inspection. The host maintenance loop resumes only namespaces
whose explicit start marker already exists; a normal maintenance pass cannot
start an upgrade. Pending owner/pin proof waits for the next scheduled pass
instead of creating an immediate busy retry.

Historical manifests without complete publication/reader provenance remain
`MissingOwnerProof`. Their receipts and physical data are retained. The latest
writer registration cannot reconstruct older reader owners. Similarly, a
historical allocation whose manifest was already removed and has no receipt
is outside this known-manifest census: enumerating an unattributed Chunk does
not prove which Tree owned it. Neither case is reported as reclaimed. These
limits do not prevent data migration or normal typed-pack collection.

## 9. Accounting and Configuration

Host resource admission covers runtime state, accepted operations, metadata,
frame/payload storage, I/O windows, and retained output owners. Fixed-pack
identity and physical allocation receipts underpin the backend ledger.
`TreeBackingStats` exposes allocated physical capacity, including mirrors;
`TreeWorkCollector` exports immutable collection windows and cumulative work
counters. Host consumers deduplicate observations with the same scope. Capacity
includes old or staged output until physical deletion is acknowledged, so it
does not measure only the latest root's useful contents. Classification metrics
do not create separate ownership records or scan the backing graph.

The host samples `chunk_pin_release_slots` through the existing resource log
windows. `limit` is the queue capacity; `used` and `allocated` count registered
pins, including pins awaiting final release. `waiters` counts those pending
releases, and `denied_host` counts capacity rejections. Samples do not pump the
queue or reset counters. Averages and maxima describe sampled occupancy, while
release latency remains unmeasured by this row. Chunk-KV configures this shared
capacity with `storage.tree_pin_release_slots`, defaulting to 1,048,576. Local
memory/block hosts have no Chunk pin-release queue and emit no such row.

Legacy `ChunkPageStoreStats` has no invented runtime equivalent. Its old C/Rust
getter reports `NotSupported` for a runtime facade. Callers use the typed
backing and work interfaces.

`ChunkRuntimeOptions` specifies Tree and owner identity, optional exact root
generation, I/O alignment, mirror copies, and maximum Chunk bytes. Defaults are
64-KiB alignment, two mirrors, and a 256-MiB Chunk limit. Host worker, operation,
byte-budget, and cache limits remain separate configuration. A configured hard
limit is checked before work that depends on it is accepted.

## 10. Correctness Invariants

- **I1 — Durable before visible:** every reachable fixed pack and dependency is
  durable and protected before its root candidate commits.
- **I2 — One fenced successor:** publication compares root generation, owner
  epoch, writer incarnation, and process authority.
- **I3 — Immutable identity:** backing references name fixed verified content;
  purpose or logical offset is never sufficient identity.
- **I4 — Exact recovery:** exact-root open cannot silently substitute another
  generation or interpret an unsupported format as an empty Tree.
- **I5 — Independent rebuild:** target ownership and mutations are independent
  of the captured source, and reads enforce the logical range.
- **I6 — Readers survive writer close:** retained snapshots keep their backend
  and protection owners until final release is processed.
- **I7 — Process exclusion is explicit:** epoch advancement and writer
  quiescence do not prove that old readers are gone.
- **I8 — Physical proof precedes accounting release:** neither metadata removal
  nor an ambiguous deletion reply decrements confirmed physical ownership.
- **I9 — Bounded accepted work:** operations retain their prepaid CPU, metadata,
  I/O, and terminal ownership until the real backend work completes.
- **I10 — Durable frontier controls WAL:** handoff and staged writeback cannot
  independently advance WAL retention.
- **I11 — Migration preserves recovery:** original-root receipts precede CAS;
  ambiguous outcomes resolve the same generation and operation.
- **I12 — Missing legacy evidence retains bytes:** unknown historical owners or
  unattributed allocations are explicit limits, never inferred GC permission.
