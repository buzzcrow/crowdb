<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: crowdb-tree Chunk Storage

Depends on: [`design-crowdb-tree.md`](design-crowdb-tree.md),
[`design-crowdb-tree-storage.md`](design-crowdb-tree-storage.md),
[`../chunkdb/design-crowdb-chunkdb.md`](../chunkdb/design-crowdb-chunkdb.md)

Satisfies: [`design-crowdb-tree-storage.md`](design-crowdb-tree-storage.md)
§1 (page-store abstraction)

This document specifies the chunk-backed `PageStore`: immutable mirrored page
packs, generation-fenced manifests, recovery, range rebuild, ownership
materialization, and the boundaries for later physical reclamation.

## Table of Contents

- [1. Architecture](#1-architecture)
- [2. Page Packs and Addressing](#2-page-packs-and-addressing)
- [3. Async Execution and Mirroring](#3-async-execution-and-mirroring)
- [4. Manifest Publication and Recovery](#4-manifest-publication-and-recovery)
- [5. Reads and Cache Validity](#5-reads-and-cache-validity)
- [6. Range Rebuild and Sharing](#6-range-rebuild-and-sharing)
- [7. Materialization and Reclamation](#7-materialization-and-reclamation)
- [8. Correctness Invariants](#8-correctness-invariants)
- [9. Configuration and Metrics](#9-configuration-and-metrics)
- [10. Performance Evidence](#10-performance-evidence)

---

## 1. Architecture

`ChunkPageStore` implements both `PageStore` and `AsyncPageStore`. Tree
algorithms depend only on those interfaces. The C API owns stores through an
opaque `ct_page_store`; `ct_open` accepts an injected store and does not name
or construct the chunk backend. The Rust chunk-KV owner selects the backend,
provides the ownership epoch, and injects production catalog and transport
dependencies.

`ChunkTransport` supplies chunk allocation, append, layout lookup, mirror I/O,
and sealing. `RootCatalog` stores immutable reference-segment images and
publishes one current manifest per tree. The production transport speaks the
ChunkDB and DiskIO protocols directly; deterministic in-memory implementations
exercise the same contracts in tests.

The common static tree archive contains the chunk and RPC-backed implementation,
but an ordinary local-tree link extracts neither archive member nor its RPC
symbols. Chunk implementation headers remain private, while the stable C ABI
exposes construction, transport routes, statistics, and orphan accounting.

## 2. Page Packs and Addressing

The tree writes a logical byte image. The chunk backend groups its dirty ranges
into immutable packs of at most `pack_bytes`. Every pack has a logical offset
and a `ChunkPageRef` containing chunk identity, physical offset, logical length,
and checksum.

Mapping-table slots do not store physical chunk offsets. Their tagged 64-bit
page-reference words name a logical location whose covering pack is resolved
through immutable segmented reference tables. Untagged words remain valid for
local stores and legacy mapping images. Mixed mapping images use their versioned
codec so the two forms cannot be confused.

Manifest format 3 permits sorted, non-overlapping sparse pack layouts. A gap
means that no live page references that logical pack range. Recovery and page
reference decoding validate arithmetic bounds, reference-segment checksums, and
complete pack coverage for every addressed page.

The backend allocates three-way mirror strips. One B+tree chunk carries at most
256 MiB of logical pack data. A pack never crosses that limit: the current chunk
is sealed and the whole pack is appended to a new chunk. A reopened store starts
with a fresh writable chunk. DiskIO owns device alignment; tree page records are
tail-padded to the configured page alignment while logical lengths and checksums
exclude padding.

## 3. Async Execution and Mirroring

All backend operations enter a bounded lock-free queue. One ordered continuation
worker preserves submission order, assigns stable nonzero operation IDs, skips
cancelled queued work, and drains accepted completions during shutdown. Queue
exhaustion completes immediately with `ResourceExhausted` and still follows the
exactly-once callback contract.

Pack construction admits at most `max_concurrent_packs`. Each pack fans out
three mirror writes through sender composition. The pack is durable only after
all mirrors acknowledge it. Failure or cancellation stops new admission, drains
launched work, records possible unpublished objects, and prevents manifest
publication.

The tree uses one backend-neutral completion descriptor. Every terminal future
transition signals it exactly once after publishing completion state with
release ordering. Linux uses `eventfd`; the portable fallback uses a
nonblocking pipe. Rust polls the descriptor through its existing async reactor.

## 4. Manifest Publication and Recovery

`ChunkManifest` records tree identity, generation, owner epoch, logical size,
pack entries, reference-segment directory, sharing counters, and checksums.
Manifest directories and reference-segment images are immutable.

A checkpoint captures the current catalog generation before it constructs any
packs. Its new manifest is numbered `expected_generation + 1`. After pack and
metadata durability barriers, `RootCatalog::publish` compares the same expected
generation and ownership epoch. Only that compare-and-publish operation makes
the checkpoint visible. Stale, failed, or ambiguous work remains unreachable
and is accounted as orphan data.

Recovery opens the newest epoch-valid, fully verified manifest. It may fall
back only to the preceding complete retained generation. Corruption and
temporary availability remain distinct typed outcomes; an unavailable mirror
does not poison mappings or convert a retryable read into corruption.

## 5. Reads and Cache Validity

A read resolves its mapping word and immutable reference segment, obtains a
chunk layout, then reads and verifies the covering pack. Layouts are cached only
until their `valid_until` deadline and are refreshed before later use.

The ordered worker owns one immutable verified pack cache. Adjacent asynchronous
page reads covered by that pack reuse its bytes and avoid another DiskIO call.
A different pack replaces the cache entry. Synchronous maintenance reads do not
mutate this executor-owned cache. Cancellation is observed before submission,
after transport completion, and before bytes or roots become visible.

## 6. Range Rebuild and Sharing

Each persisted page carries checksummed lower and upper reachability fences.
Range rebuild pins one source generation and walks native pages in bounded
batches. Separator fences skip disjoint subtrees before their pages are loaded.
Wholly contained pages reuse immutable pack and metadata references; boundary
leaves, crossing ancestors, escaping siblings, and overflow chains are rebuilt.

Each child has an independent root, mapping table, high-water allocation range,
manifest lineage, and mutable state. Immutable reference-segment and mapping
images may be shared until a changed entry triggers segment-level copy-on-write.
Concurrent rebuild workers share only the immutable pinned source.

## 7. Materialization and Reclamation

Materialization first snapshots all valid retained tree anchors and derives the
live anchor, directory, mapping-image, and page extents. The snapshot generation
gate remains held while the chunk backend repacks those extents, so a foreground
snapshot cannot invalidate the reachability image mid-pass.

Packs outside the live extents are omitted. Shared live packs are copied into
child-owned chunks under a byte budget, reference ordinals are made dense, and
a sparse format-3 manifest is published through the normal generation fence.
Later checkpoints preserve absent sparse ranges unless a new write dirties them.
A failed reuse verification rewrites already materialized bytes; it never turns
a live pack into a hole.

Historical manifests are immutable. Catalog retention and in-memory pins decide
when manifest objects become logically reclaimable. The backend reports orphan
and reclaim candidates but does not physically release strips. Expired-writer
sealing and physical strip GC are independent maintenance services so either can
resume after a tree or ChunkDB restart.

## 8. Correctness Invariants

- **I1 — Durable before visible:** every referenced mirror and metadata image is
  durable before its manifest can become current.
- **I2 — One fenced successor:** generation `g + 1` publishes only by comparing
  generation `g` and the current ownership epoch.
- **I3 — Whole-pack rotation:** no page pack crosses the 256-MiB logical chunk
  limit.
- **I4 — Verified addressing:** every tagged page location resolves through a
  checksummed immutable segment to complete manifest coverage.
- **I5 — Sparse absence is stable:** an omitted pack range remains absent until
  an overlapping logical write dirties it.
- **I6 — Sharing is immutable:** lineages share only immutable packs and metadata
  images; their first mutation uses copy-on-write.
- **I7 — Rebuild isolation:** a child contains exactly its half-open key range
  without foreign keys, sibling edges, or overflow references.
- **I8 — Retention precedes release:** no physical strip is released while a
  retained manifest or in-memory pin can name it.
- **I9 — Exactly-once completion:** every admitted async operation completes its
  callback once, including cancellation, overload, and shutdown.

## 9. Configuration and Metrics

The bounded defaults are:

- `pack_bytes`: 4 MiB;
- `max_chunk_bytes`: 256 MiB hard ceiling;
- `page_alignment` and `iu_size`: 64 KiB;
- `max_concurrent_packs`: 8;
- `max_pending_ops`: 256;
- `layout_validity_ms`: at most 30 seconds; and
- `materialization_bytes_per_pass`: 64 MiB.

`ChunkPageStoreStats` exposes publication generations, pack writes and reuse,
pack bytes, reads and cache hits, layout queries, mirror attempts and failures,
retention pins, orphan bytes, materialization work, shared ownership, RPC and
DiskIO operation latency, coalesced reads, completion wakeups, metadata sharing,
manifest publication latency, and recovery latency. The C ABI and Rust wrapper
preserve the same counter meanings.

## 10. Performance Evidence

A fixed 4-MiB in-memory transport workload measured these aggregate means on
11 September 2026:

- 64-KiB cold read: 0.79 ms real time, 89.6 MiB/s CPU throughput;
- coalesced read: 4.4 us real time, 15.6 GiB/s CPU throughput; and
- 4-MiB snapshot: 26.2 ms real time, 153.8 MiB/s.

The benchmark validates bounded staging and cache behavior without claiming
production network latency. Runtime RPC and DiskIO counters provide the
production evidence needed to revisit the defaults.
