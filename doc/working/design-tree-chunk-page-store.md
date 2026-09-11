<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Tree Chunk Page Store (R140)

This implementation design refines
[`../backlog/R140-tree-chunk-page-store.md`](../backlog/R140-tree-chunk-page-store.md)
within the tree architecture in
[`../design/tree/design-crowdb-tree.md`](../design/tree/design-crowdb-tree.md) and
[`../design/tree/design-crowdb-tree-storage.md`](../design/tree/design-crowdb-tree-storage.md).
The existing page store, snapshot, mapping-table, and recovery contracts are
landed; this work adds shared immutable storage without changing their local
on-disk format.

## 1. Backend-Neutral Construction and Completion

`ct_page_store` is an opaque C handle that owns a `PageStore` and optional
`AsyncPageStore`. `ct_open` accepts the handle in `ct_options`; when present it
borrows the store for the tree lifetime and does not construct a concrete
backend. Existing options continue to construct and own file, block, or memory
stores, preserving source and persisted-format compatibility.

`AsyncPageStore` becomes available on every platform. Its allocation-free
submission boundary is a callback pointer plus context:

```cpp
using AsyncCompletion = void (*)(void *, Status);
virtual SubmitResult submit_read(PageAddr, void *, size_t,
                                 AsyncCompletion, void *) = 0;
```

The local adapter maps that token to `DiskIOUring`; the chunk adapter embeds it
in its RPC operation state. Sender composition is private to implementation
translation units. The tree owns one `CompletionEvent`, backed by nonblocking
`eventfd` on Linux and a nonblocking pipe elsewhere. Every pending C future is
completed with release ordering and signals that event exactly once. Rust
registers this descriptor in the same `AsyncFd` pump used for local completions.

Failures preserve `InvalidArgument`, `ResourceExhausted`, `Unavailable`,
`Corruption`, and `Internal` through `Status`, `ct_status`, and `CtError`.

## 2. Immutable Chunk Storage

`ChunkPageStore` is private to crowdb-tree and is created only by
`ct_chunk_page_store_open`. Its injected transport owns allocation, layout,
read, mirrored write, and metadata publication RPCs. The production adapter
encodes ChunkDB and DiskIO FlatBuffers directly with `crowdb-rpc`; tests inject
an in-memory transport.

Pages are accumulated into packs bounded by `pack_bytes` and
`max_concurrent_packs`. A page-reference ordinal resolves through immutable
segmented tables to `ChunkPageRef {chunk_id, offset, length, checksum}`. Mapping
slots store ordinals, not physical offsets. The backend caches immutable chunk
layouts until `valid_until`; adjacent misses in the same pack are coalesced,
and expired layouts are refreshed before data is returned.

The first release allocates only three-way mirror strips. Each active B+tree
chunk has a 256 MiB logical-data limit. A pack that does not fit causes the
current chunk to seal and is written wholly to a new chunk. Reopening a tree
always allocates a fresh chunk; the old active chunk remains readable and is
sealed by the deferred orphan-management requirement. DiskIO owns device
alignment. The tree encoder pads every page record to the 64-KiB page size so
page starts are independently aligned while logical lengths and checksums omit
padding.

Three mirror writes are composed as one cancellable operation. A pack is
durable only after all mirrors acknowledge it. Metadata images are then made
durable and the injected `RootCatalog::publish(expected_epoch, manifest)` is
the sole visibility point. Publication failure leaves the prior root current
and records all new objects as orphans.

## 3. Manifest, Retention, and Recovery

`ChunkManifest` contains tree identity, generation, owner epoch, root PID,
next PID, range policy, mapping-segment directory, page-reference-segment
directory, and checksums. Directories and their images are immutable. Recovery
loads the newest epoch-valid manifest, validates every checksum and bound, and
falls back only to a preceding complete generation. Availability failures do
not mutate mappings or mark the tree corrupt.

Opening a manifest creates a pin. The catalog exposes the oldest reclaimable
generation, while in-memory pins may extend retention. Orphan scanning removes
objects unreachable from any retained manifest. Metrics report retained
manifests, pinned bytes, oldest-pin age, and orphan bytes.

## 4. Range Policy and Verified Fences

`KeyRange` is fixed at construction and is either `Unbounded` or
`Bounded(start, end)`, with independently unbounded endpoints. Empty bounded
ranges are valid. One comparison helper enforces the half-open predicate at
apply, get, seek, scan, recovery, and page installation.

Every encoded leaf and inner page carries verified lower and upper fences.
Inner fences cover every reachable child; leaf fences cover all entries,
overflow values, and the permitted right sibling. Decode rejects inverted,
inconsistent, or out-of-policy fences as corruption. Backend selection is not
consulted by tree algorithms.

## 5. Range Rebuild

`rebuild_range(source_manifest, target_range, catalog, owner_epoch)` pins one
source generation and traverses with a bounded native-page iterator. Disjoint
subtrees are skipped. Wholly contained pages reuse immutable references.
Intersecting leaves are decoded and filtered, excluding foreign tombstones and
overflow chains; their ancestor paths and any leaf whose sibling escapes the
target are rebuilt.

Each destination receives an independent manifest, mapping table, and writable
segments. Immutable source segment images may be named by both directories.
Destination PID and ordinal allocation begins above the corresponding source
high-water mark. A later mutation performs segment-level COW, so sharing never
extends to mutable memory.

Workers share the pinned source only. Each owns its iterator, builder, and
publication state, so parallel rebuild adds no synchronization to point reads,
writes, or page resolution.

## 6. Materialization and Reclamation

Materialization pins child generation `g`, marks PIDs reachable from its
bounded root, and writes child-owned mapping/reference segments with
unreachable slots cleared. Packs referenced by multiple current children are
copied into child-exclusive packs. Publication uses compare-generation; if a
foreground checkpoint has published `g + 1`, the stale output becomes orphan
work and the pass retries from the new manifest.

Failures retain the prior correct generation. Historical manifests are never
rewritten. Objects become logically reclaimable only when catalog retention
and every in-memory pin have advanced beyond their last reference. This
requirement persists reclaim candidates but does not free chunk strips. The
separate B+tree chunk-GC requirement consumes those candidates and owns
physical strip release.

## 7. Source Hierarchy

Private C++ implementation files are grouped by subsystem:

- `src/btree/` contains tree algorithms, ranges, and rebuild;
- `src/mtable/` contains mapping persistence, page frames, codecs, and the
  mapping-table page service; and
- `src/backend/` contains backend-neutral adapters, with concrete local and
  chunk implementations below `src/backend/local/` and
  `src/backend/chunk/`.

Canonical public headers follow the same `btree/`, `mtable/`, and `backend/`
grouping. `include/crowdb-tree/crowdb-tree.h` is the umbrella C++ interface;
root forwarding headers preserve existing include paths. Implementation-only
contracts remain below `src/`. CMake and the Rust FFI build recurse below
`src/`, so directory grouping does not change archive composition.

## Scope

- `third-party/stdexec/`: repository-pinned sender implementation.
- `lib/crowdb-tree/include/crowdb-tree/{async_page_store,chunk_page_store,key_range,c_api,options,status}.h`:
  backend-neutral APIs and typed contracts.
- `lib/crowdb-tree/src/{btree,mtable,backend}/`: grouped private tree,
  mapping, local-backend, and chunk-backend implementation.
- `lib/crowdb-tree/src/{c_api,async_completion,stdexec_adapter}.*`: C ABI and
  cross-backend completion infrastructure.
- `lib/crowdb-tree/ffi/{build.rs,src/options.rs,src/sys.rs,src/tree.rs,src/error.rs}`:
  C ABI construction and completion integration.
- `lib/crowdb-tree/{CMakeLists.txt,tests/unit,tests/integration}`: build wiring
  and acceptance coverage.
- `doc/design/tree/design-crowdb-tree-chunk-storage.md` and
  `doc/doc_index.md`: permanent design and index entry after implementation.

## Complexity

High. The work changes persistence addressing and async completion while
preserving the local format and lock-free read path. Crash-safe publication,
structural range isolation, immutable metadata sharing, and stale-generation
cleanup require independent correctness tests.

## Test Design

- Open existing local fixtures through legacy options and injected local
  handles; mutate, snapshot, reopen, and assert byte/recovery compatibility.
- Complete injected reads immediately, after submission, delayed, failed, and
  stopped; assert one terminal signal, one event wake, and retained operation
  lifetime. Track allocations after pools warm and assert no continuation
  allocation.
- Run three injected mirror senders with success, failure, and cancellation;
  assert publication occurs once only after three successes.
- Snapshot adjacent pages with small pack limits; assert bounded concurrent
  packs, ordinal mappings, durable-before-visible ordering, and prior-root
  authority under every injected failure point.
- Read adjacent pages before and after layout expiry; assert coalescing within
  the validity window and one metadata refresh after it.
- Inject unavailable and corrupt reads; assert distinct typed outcomes,
  unchanged mappings, and continued resident reads.
- Corrupt checksums and fences during recovery and rebuild; assert no root is
  published.
- Rebuild both sides of a mixed leaf and crossing internal/sibling paths;
  traverse without API filtering and assert exact union, empty intersection,
  no foreign overflow reference, and correct empty/unbounded endpoints.
- Rebuild two ranges concurrently; assert independent manifests and an
  unchanged readable source.
- Mutate shared child segments, retain historical pins, inject maintenance
  failures, and race materialization with checkpoint; assert COW isolation,
  monotonic cleanup, generation fencing, and retention metrics.
- Inspect ordinary and chunk-server linked symbols and public headers; assert
  only the chunk server extracts the private backend and no stdexec type leaks.
- Benchmark fixed cold/warm/coalesced/snapshot/recovery workloads and record
  bounds for pack size, concurrency, prefetch, and materialization budget.

## Module Structure

```text
lib/crowdb-tree/
├── include/crowdb-tree/
│   ├── crowdb-tree.h             umbrella C++ interface
│   ├── btree/                    tree and range interfaces
│   ├── mtable/                   mapping-table page service
│   └── backend/                  backend-neutral store interfaces
├── src/
│   ├── btree/                    tree algorithms, frames, range rebuild
│   ├── mtable/                   mapping and page services
│   ├── backend/
│   │   ├── local/                memory, text, and block stores
│   │   └── chunk/                RPC backend, manifests, page packs
│   ├── c_api.cpp                 backend-neutral construction
│   └── stdexec_adapter.cpp       sender adapters
└── tests/
    ├── unit/                    sender, codec, fence, rebuild cases
    └── integration/             publication, recovery, retention, ABI
```

## Config Extensions

- `pack_bytes`: 4 MiB.
- `max_chunk_bytes`: 256 MiB, fixed first-release ceiling.
- `max_concurrent_packs`: 8.
- `layout_validity_ms`: catalog-provided, capped at 30 seconds.
- `read_coalesce_bytes`: one pack, capped by `pack_bytes`.
- `materialization_bytes_per_pass`: 64 MiB initial bound.
- `rpc_completion_capacity`: fixed at construction; exhaustion returns
  `ResourceExhausted` without using a fallback map.

The production group-0 root-catalog implementation is injected by the
chunk-KV owner. R140 supplies the contract and its in-memory acceptance
implementation without introducing a group-0 dependency into crowdb-tree.
