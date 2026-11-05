<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: crowdb-tree Durable Storage

Depends on: [Tree overview](design-crowdb-tree.md), [Tree engine](design-crowdb-tree-engine.md)
Satisfies: [Tree overview](design-crowdb-tree.md) §3.1

This document specifies page storage, durable frame encoding, cache ownership,
checkpoint/recovery and physical reclamation. The [Chunk storage design](design-crowdb-tree-chunk-storage.md)
defines its catalog and mirrored pack protocol.

## Table of Contents

- [1. PageStore Abstraction](#1-pagestore-abstraction)
- [2. Backends](#2-backends)
- [3. On-Disk Page Format](#3-on-disk-page-format-zero-copy-frame)
- [4. Buffer Pool and Cache Residency](#4-buffer-pool-and-cache-residency)
- [5. Internal WAL Decision](#5-internal-wal-decision)
- [6. Snapshot, Recovery, and Export/Import](#6-snapshot-recovery-and-exportimport)
- [7. Interaction with Consensus WAL GC](#7-interaction-with-consensus-wal-gc)
- [8. Mapping Table](#8-mapping-table)
- [9. Garbage Collection](#9-garbage-collection)
- [10. New-Member Install Flow](#10-new-member-install-flow)
- [11. Root and Physical Metrics](#11-root-and-physical-metrics)

## 1. PageStore Abstraction

The engine identifies logical pages independently of their stored address.
`BackingRef` carries the storage domain, address/length, content/fence proof and
an owning capability. A captured reader resolves dependencies through that
capability, including overflow owned by an older checkpoint.

`AsyncPageStore` expresses accepted asynchronous work through completion
callbacks. `ScheduledPageStore` accounts host I/O and keeps backend ownership
through callbacks. Synchronous stores use the host's backend executor; native
asynchronous stores use their completion interface. A Tree does not create a
private executor for each operation.

`CheckpointBackend` separates reloadable page writeback from durable root
commit. A stored page can support eviction before it belongs to a committed
checkpoint. Only root commit establishes a durable replay point.

**I1 — Domain ownership:** incompatible runtime/backend bindings are rejected.
Opening a second writable Tree on the same owned backend domain cannot bypass
the first owner merely by constructing another facade.

## 2. Backends

- `TextPageStore` is a human-readable Local debug store with byte IU.
- `MemPageStore` keeps its byte-addressed data in sparse fixed-size 64 KiB
  memory pages. Growth allocates only touched pages and never relocates existing
  data. Reads of unwritten holes return zeroes within the logical high-water
  mark. Its backing data is storage capacity, separate from Tree work credits;
  `resident_bytes()` reports allocated data-page capacity without counting holes.
- `BlockPageStore` allocates aligned extents in memory or local block files.
  Its allocation metadata, directories and anchors are retained by the backend
  independently of the facade.
- `ChunkRuntimePageStore` uses immutable fixed page packs, typed backing and a
  process-fenced root catalog. Its physical mirror ledger is canonical across
  compatible facades for the same transport, retirement host and logical tree.

### 2.1 TextPageStore — On-Disk Layout (Debug)

The debug backend stores page/metadata images and anchors as text records and
maps logical addresses to those records. It implements the same Local
checkpoint ordering and frame validation contracts; byte IU removes alignment
padding but does not remove checksums or root publication ordering.

### 2.2 BlockPageStore — On-Disk Layout (Production)

Block storage grows through fixed-size `.blk-*` files or in-memory blocks.
`SpaceAllocator` owns allocation/exclusion state. Frame addresses identify aligned
IU extents; directory and anchor images identify the selected durable graph.
Local recovery and compaction retain the backend domain so delayed readers and
callbacks cannot access a destroyed allocator or file handle.

### 2.3 Async I/O Architecture

`BlockAsyncPageStore` can submit Linux io_uring operations through its owned I/O
backend. A synchronous store is adapted by the host backend executor. Both
paths retain aligned buffers, I/O tickets and backend capabilities through the
real terminal callback. CPU decoding runs on the bounded Tree CPU runtime.

Inline completion is allowed. A transport error after uncertain acceptance
cannot release a ticket while a late callback may still access its buffer.
Duplicate completion does not decrement resource or work counters twice.

### 2.4 fsync Policy

Local checkpoint writes all required page and directory data before publishing
its anchor. The selected sync policy determines the backend durability barriers;
skip-sync configurations intentionally provide weaker crash durability.
Logical flush and representation writeback do not claim checkpoint durability,
even if their individual page writes have completed.

### 2.5 Block Compaction

Sparse-block compaction selects a bounded relocation set and reserves complete
read, output, metadata and I/O resources. It writes relocated frames, publishes
the updated checkpoint representation and retires old extents through actual
backend ownership. A live reader or captured older root can keep an old extent
excluded from reuse after the current root moves elsewhere.

Physical accounting distinguishes committed, candidate, retired and free
capacity. Relocation does not count as free; the block release service updates
capacity only after the actual extent-release transition.

## 3. On-Disk Page Format (Zero-Copy Frame)

Encoded base pages use the same byte layout on disk and after loading. Leaf
views binary-search keys and read cells directly from an owned validated frame.
The live logical page is separate: it may combine several base ranges and
immutable overlay runs. Persistence materializes that logical state into a
new frame; ordinary writes do not insert into the captured base frame.

### 3.1 Frame geometry

`Config.frame_bytes` defaults to 64 KiB. Primary page capacity is capped by
both that configuration and the public frame protocol payload limit. Leaf
split/merge thresholds count record-region bytes; planning adds the 128-byte
header and 8-byte trailer. A merged leaf must fit below three quarters of the
split target, preserving hysteresis.

Values above `max_inline_value` use overflow frames with independent backing
owners. A zero configured threshold derives the limit from one quarter of the
configured frame size. The leaf retains a pointer cell and logical value
length; the full overflow chain is not copied into every read or captured leaf.
Key size and complete operation allocation limits are checked before mutation.

### 3.2 Slotted layout

A leaf frame contains:

- a 128-byte header with type, format, logical page identity, range fences,
  slot/record offsets and subtree summary;
- a sorted directory of 12-byte slots, each containing three `u32` values:
  record offset, key length and cell length;
- record bytes growing backward from the trailer, containing key and encoded
  cell (slot, flags and inline value or overflow reference);
- an 8-byte trailer containing logical length and CRC32C.

Inner frames contain child PIDs and an 8-byte separator slot directory.
Routing uses the separator upper bound. Builders consume sorted bounded input
and stamp a complete immutable frame; later logical mutation publishes a new
page descriptor or replacement frame.

### 3.3 Validation, alignment and compression

Frame validation checks type/version, lengths, slot bounds, identity, ranges
and checksum before exposing views. A corrupt referenced frame is an error;
recovery must not silently discard committed content or create an empty Tree.
Backend IU alignment and stored-envelope padding are separate from the logical
frame capacity. Readers retain the actual stored allocation and buffer owner.

`Config.compression` defaults to `kNone`. With LZ4 enabled, the stored envelope
records the codec, original/stored lengths and CRC of stored bytes. Incompressible
frames remain raw. Decoding restores a validated frame before leaf access.
CMake discovers system LZ4; the Rust FFI build can enable it through
`CROWDB_TREE_LZ4_LIB`. Without the codec, new output remains raw and an existing
LZ4 envelope returns `NotSupported`, preserving the stored data.

## 4. Buffer Pool and Cache Residency

`SharedPageCache` stores reloadable immutable frames under bounded page, byte
and waiter limits. It deduplicates in-flight loads and separates demand and
maintenance work. Bindings and waiter state are prepaid before accepted work
can need them; a queue full of ordinary reads does not invalidate an accepted
flush's routing credit.

Logical page identity, current residency and physical frame lifetime are
separate. Evicting a cache entry or publishing a proxy removes that residency
reference; a read result or snapshot can still own the frame. The allocation
charge persists until the last frame owner releases it.

Dirty logical pages require a reloadable backing representation before their
resident data can be dropped. Bounded writeback installs such backing without
advancing durable coverage. Idle host windows continue writeback and trimming
after writes stop. Clean captured backing can be reused by checkpoint without
loading its primary frame when its proof is still valid.

**I2 — Eviction safety:** a proxy retains a readable typed backing capability.
No cache eviction rewrites logical content, advances the source floor or
invalidates an already returned owner.

## 5. Internal WAL Decision

crowdb-tree has no operation redo log. The host consensus WAL owns the durable
chosen-operation sequence; Tree checkpoints own durable materialized state.
Recovery loads the selected root and replays the external WAL suffix after its
coverage. Chunk catalog metadata is a root/ownership protocol, not a second
user-operation WAL.

## 6. Snapshot, Recovery, and Export/Import

A checkpoint captures one immutable published view after finite prefix handoff.
It retains sources, mapping, typed primary backing and overflow dependencies.
`CapturedFrameStream` reuses valid durable receipts or materializes bounded frames,
then `LocalCheckpoint` or the Chunk root protocol commits the complete graph.
See [checkpoint flow](design-crowdb-tree-engine-snapshot-flow.md).

Snapshot persistence is page-granular. Changed Tree pages and pages explicitly
selected for GC or relocation receive new backing; unchanged pages and
mapping/reference segment images retain their stored identities. Local's small
segment directory may be rewritten when segment entries change. With no
persistent-state change, the existing durable root is reused without data or
metadata writes or a new snapshot sequence. Root publication changes
reachability; Tree GC reclaims old physical data only after retained roots,
candidates, readers and accepted I/O no longer require it.

The current runtime metadata path still rebuilds Local segment images and Chunk
indexes. It does not yet satisfy this incremental metadata contract; zero
data-frame writes alone do not establish a no-op checkpoint.

Local recovery validates anchor/directory metadata, frame identity and
structural relationships before exposing a Tree. Logical descriptors may
retain cold frames and load them through the owned backend as required.
Corrupt or incompatible roots fail explicitly; they do not silently produce
an empty writable database.

A checkpoint can commit while newer writes exist. Representation rebase checks
the captured content/route/run identities and retains newer overlays. A stale
rebase is deferred; it never overwrites current content or revokes the
successfully committed captured root.

### 6.1 Snapshot Export / Import

Native export emits actual captured frames, including cold data that a normal
checkpoint could reuse without reading. Portable export emits logical entries.
Both iterate a stable immutable view using bounded materialization windows;
outputs retain their actual owner after iterator or Tree destruction.

Import validates frames, root/child graph, ranges, slot fences and overflow
dependencies before replacing a generation through the coordinator. Corruption,
admission failure or cancellation during staging leaves the old generation and
its writer usable. Imported resident overflow retains independent owners and
can later be checkpointed without a global mutable resolver.

Local compatibility migration reads the supported durable format into a staged
runtime generation, validates it and publishes through the ordinary checkpoint
protocol. Chunk compatibility migration has a distinct explicit stop-writes
contract, described by the Chunk storage design.

## 7. Interaction with Consensus WAL GC

The host separately tracks applied, handed-off and durable coverage. A Tree
writeback report cannot authorize WAL truncation. `persist_snapshot` returns
the committed durable prefix; the learner combines that prefix with its quorum
and safe-slot policy before advancing its external GC floor.

Logical retention is bounded by the supplied safe/snapshot floor. Old snapshots
continue owning the representation they captured even after the current Tree
prunes tombstones or superseded values. Logical GC and physical backend
reclamation therefore have different completion times.

**I3 — Replay safety:** after restart, operations above the selected durable
coverage remain necessary even if their former process had completed handoff
or staged writeback.

## 8. Mapping Table

`MappingView` is a persistent radix COW structure from logical page IDs to
immutable published descriptors. Transactions specify unique ordered page IDs
and expected old owners. Root acquisition retains the entire view needed by
that reader; later publication cannot redirect its IDs to another generation.

COW copies touched paths and segments. Each path aggregates logical page
contributions and bounded maintenance summaries, including maximum captured
leaf descriptor/base-array size and oldest candidate timestamps. Removing or
replacing a child updates those summaries, including shrinking maxima.

Local durable metadata uses CRC-protected mapping segment images, a segment
directory and an anchor. In-memory pointers are never persisted. Recovery
translates validated stored references into typed backing and immutable
descriptors; it does not expose a mutable tagged-slot mapping to ordinary Tree
writers. Chunk roots instead describe fixed immutable packs and their indexes.

Generation replacement, import, split, maintenance and checkpoint rebase all
publish through `PublicationCoordinator`. Cache readiness changes only a
representation's residency and does not independently mutate the logical root.

**I4 — Publication safety:** a stale expected identity cannot clear or replace a
newer page. Metadata summaries and mapping contents belong to the same
published state.

## 9. Garbage Collection

Logical tombstone/version retention follows the external floor. Physical
reclamation follows real ownership and backend proof:

- Epoch retirement frees source nodes/descriptors only after protected readers
  leave, while independently shared cell payloads may outlive the source.
- Root/page/array/frame retirement waits for old views, output handles and cache
  owners. A logical retirement marker does not itself free allocation bytes.
- Local extent release waits for exclusion and retained backend owners.
- Chunk collection requires candidate identity, process/catalog authority,
  reference protection and confirmed drain/delete. Shared mirror capacity is
  deduplicated by canonical physical identity.

Bounded host maintenance drives these queues when writes stop. A retained user
handle can legitimately delay convergence; diagnostics expose that reason and
the age of unfinished work instead of reporting those bytes as already free.

## 10. New-Member Install Flow

The sender captures a stable durable/logical prefix and streams native frames
or portable entries. The receiver stages and validates an independent
generation, commits its durable root, then replays the consensus WAL suffix.
Installation failure preserves the prior generation. Range rebuild and split
handoff honor their half-open range and captured slot floor; child writes do
not invalidate the parent's retained captured owners.

## 11. Root and Physical Metrics

`TreeStatsSnapshot` reports incrementally maintained logical contributions with
version/commit identity. Known and unknown page sizes remain explicit: an
unmaterialized overlay does not invent exact encoded bytes. Dirty, reloadable,
checkpoint-pending and merge-candidate membership have separate meanings.

The durable root summary is fenced by root version and covered slot. It excludes
later WAL/L0 writes; unknown child summaries propagate unknown rather than zero.
Live keys/value bytes exclude tombstones and overwritten values. Overflow
payloads contribute logical length, while overflow frames contribute structural
capacity.

Physical backing metrics expose scope identity for host deduplication. A
metrics-only sampler may outlive the Tree without pinning backend or page
owners. Reference-reason byte counts overlap and must not be summed as physical
capacity. Ordinary collectors use incremental state; a complete graph/owner
reconciliation is an explicit diagnostic with completeness and quiescence
reported in its result.
