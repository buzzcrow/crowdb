<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R140: crowdb-tree — Chunk page store and range rebuild

## Problem

crowdb-tree persists pages to a local filesystem or local block device through
`PageStore`. This binds a durable tree to one machine. Moving ownership of a
future range-partitioned KV service would therefore require copying its tree
files, even though chunkdb already provides shared, replicated chunk storage.

The storage contract in
`doc/design/tree/design-crowdb-tree-storage.md` covers local `TextPageStore` and
`BlockPageStore` backends only. The tree overview explicitly defers whole-tree
range split and merge. It does not define a chunk address, immutable snapshot
manifest, atomic root publication, or how a child range is rebuilt while old
B+tree pages contain keys on both sides of the split boundary.

Concrete scenarios are a KV range owner reopening its tree on another node,
one owner splitting `[a, z)` into `[a, m)` and `[m, z)`, and several workers
dumping disjoint subtrees concurrently. Reusing an unfiltered boundary page can
leak out-of-range keys; rewriting every page defeats the no-migration and fast
parallel-dump goals.

## Solution

Extend crowdb-tree with a native C++ chunk page backend selected when a tree is
created and a range rebuild operation that reuses only pages proven to be
wholly inside the target range.

1. Add a permanent chunk-storage section under `doc/design/tree/` defining the
   page-pack layout, generation-addressed page references, checksums, snapshot
   manifest, root publication, recovery, retention, and asynchronous completion
   contract. Split immutable page-pack publication from the existing local
   byte-device persistence path; fixed anchors, free-gap allocation, and block
   deletion remain local-backend concepts.
2. Adopt NVIDIA `stdexec` as the internal C++ sender/receiver implementation
   and separate the backend-neutral async contract from `DiskIOUring`. Keep one
   allocation-free completion-token submission primitive at the virtual backend
   boundary, wrap local io_uring and `crowdb-rpc` submissions as senders, and
   compose all higher flows with sender algorithms. Report terminal completion
   through the existing C tree future plus a backend-independent completion
   eventfd. Do not use `std::future`, Folly Future/Promise, or hand-written
   callback state machines above the transport adapter. Keep `stdexec` headers
   out of public tree and C ABI headers, and compile its adapters on every
   supported GCC and Clang toolchain.
3. Implement a private, B+tree-specific chunk backend inside crowdb-tree. It
   directly encodes the shared ChunkDB and DiskIO FlatBuffers commands through
   C++ `crowdb-rpc`; it is not a public C++ chunk client. Keep its source in an
   isolated translation unit inside the same static `libcrowdb-tree.a` as the
   local backends. Organize private implementation files by subsystem under
   `src/{btree,maptable,memtable,snapshot,backend}/`, with concrete local and
   chunk stores below `src/backend/local/` and `src/backend/chunk/`. Group
   canonical public interfaces below matching subsystem directories; do not
   retain redundant flat forwarding headers. Keep only the umbrella, ABI,
   configuration, status, and shared primitives at the include root.
   `include/crowdb-tree/crowdb-tree.h` is the umbrella C++ interface. Both
   CMake and `crowdb-tree-ffi/build.rs` continue discovering sources
   recursively. Add an opaque backend handle to the C ABI so the Rust caller
   selects and supplies the store when creating a tree. Do not use a Cargo
   feature, a second tree library, runtime dynamic loading, or a plugin ABI to
   choose storage.
4. Keep the core `ct_open` path dependent only on the backend-neutral handle;
   it must not directly reference the chunk constructor. `crowdb-chunk-kv`
   calls the chunk constructor and therefore pulls that archive member and its
   C++ RPC symbols into its final server binary. Ordinary `crowdb-kv-server`
   creates a local backend and does not reference or extract the chunk archive
   member. Neither path depends on the Rust `crowdb-chunk-client` or
   `crowdb-chunkdb-client`.
5. Accumulate immutable 64-KiB pages into bounded contiguous page packs and
   submit a bounded number of packs concurrently to chunks containing only
   three-way mirror strips in the first release. Limit each B+tree chunk to
   256 MiB of logical data and rotate before an append would cross that limit.
   A reopened tree never resumes its former active chunk: it allocates a new
   chunk, while R146 detects and seals the abandoned active chunk at its
   acknowledged cursor. DiskIO owns device-write alignment exactly as it does
   for small writes. The B+tree pack encoder additionally pads each encoded
   page's tail to the 64-KiB page size, so every page begins at a page-size
   boundary; the stored logical length and checksum exclude padding.
   The manifest maps compact page-reference ordinals to
   `(chunk_id, offset, length, checksum)` through immutable segmented reference
   tables, and the mapping table preserves its atomic 64-bit fast path by
   storing those ordinals instead of byte offsets. Mapping and reference-table
   segment directories may name images also named by another manifest; images
   are immutable and reference-counted by manifest identity.
   Treat each durable mapping table as part of one tree manifest lineage; a
   page ID has meaning only with that tree/manifest and is not a range-owner
   identity. A snapshot becomes visible only after every referenced pack range
   is durable and its manifest is atomically published through an injected,
   epoch-fenced root catalog.
6. Cache validated chunk layouts, coalesce adjacent page misses in one pack,
   and re-query metadata after the layout validity window. Allocation, topology
   refresh, root publication, and GC stay outside the page lookup hot path.
   Pre-size the `crowdb-rpc` completion slab and use immutable route selection
   so lookup never enters its fallback pending map or a connection-pool mutex.
7. Persist page fence keys and child references needed to decide whether a page
   is outside, wholly inside, or intersects a requested half-open range. Reject
   corrupt or inconsistent bounds instead of trusting them during rebuild.
   Configure one immutable key-range policy at tree creation: `Unbounded` for
   ordinary crowdb-kv trees and `Bounded(start, end)` for chunk-KV partitions.
   Use one tree implementation with a centralized policy check at public
   operation, recovery, and page-install boundaries; do not scatter backend
   `if/else` branches through page algorithms.
8. Add range rebuild for `[start, end)`: skip disjoint subtrees, reuse immutable
   pages wholly contained by the range when their references remain valid, and
   decode/filter/rebuild intersecting boundary pages and their ancestor path.
   A leaf containing keys on both sides of a split is never referenced by
   either child: decode it, emit separate filtered child leaves, and omit
   tombstones and overflow references belonging to the other range. Build a
   new root for each child and rewrite every intersecting internal path so no
   separator or child reference can reach across its bounds. Rewrite an
   included range's final leaf when its sibling link points outside the range.
   Give each child an independent manifest and segment directory, but initially
   permit its mapping and page-reference directories to reference immutable
   segment images from the selected source snapshot. Sharing is at immutable
   image granularity only: the children never share an in-memory
   `MappingTable` or a writable segment.
   A child mutation in a shared segment writes a child-owned full segment image
   by COW. Source ordinals remain resolvable through shared immutable reference
   segments; child reference allocation starts above the source ordinal
   high-water mark. Rewritten split-boundary pages receive child-owned entries,
   while reused page IDs retain their source entries. Initialize both
   children's next-page-ID above the source PID high-water mark so their later
   allocations cannot alias an inherited PID. This makes initial mapping
   preparation proportional to the two small directories plus changed boundary
   segments, not all live PIDs. Empty and unbounded endpoints have explicit
   representations.
9. Permit independent range rebuild workers to read one pinned source manifest
   through a bounded, resumable native-page iterator and publish separate
   destination manifests. They share no mutable builder state and add no lock
   to tree read, write, or page-resolution hot paths.
10. Retain source page-pack references until all published manifests that can
   reference them are outside the reclamation watermark. An interrupted rebuild
   leaves its unpublished objects unreachable and eligible for orphan cleanup;
   it never changes the source tree or deletes unrelated objects sharing a
   chunk. Logical isolation is defined by manifest/root reachability, not by
   immediate physical erasure: one immutable page pack may temporarily contain
   an unreachable mixed source page or pages referenced by different children.
   After split commit, run bounded mapping materialization and page-pack repack
   without stopping service. Mark PIDs reachable from the current child root;
   for each still-shared mapping segment, write a child-owned image with all
   unmarked slots cleared, materialize the corresponding live page-reference
   entries, and publish both in a new child manifest generation.
   Copy pages from packs referenced by both children into child-exclusive packs
   and update those child-owned mapping images. New writes naturally perform
   the same segment-level COW, so foreground change and background GC converge
   toward fully independent mapping images and packs. A retained parent or
   earlier child snapshot keeps its old immutable directory, segment images,
   and packs pinned; it is never rewritten. Old objects become logically
   reclaimable only after every referencing manifest and in-memory snapshot pin
   expires. Emit durable reclaim candidates for R147 instead of freeing chunk
   strips inside the tree's logical GC pass.
   Materialization failure leaves a correct but physically shared immutable
   image or pack and retries; it does not invalidate either child.
   Each pass pins one child manifest generation and publishes through the
   tree's existing single-writer snapshot/manifest gate only if that generation
   is still current. A concurrent checkpoint wins; stale cleanup output becomes
   orphan work and is rebased or retried, never published over newer data.
11. Expose metrics for RPC and DiskIO latency, chunk layout queries, cache hits,
    coalesced reads, page-pack writes, completion wakeups, reused and rebuilt
    pages, shared and materialized metadata segments, materialization scan and
    write bytes, retained manifests, pinned bytes, oldest-pin age, manifest
    publication latency, recovery time, and orphan bytes. Use cold-read,
    warm-read, scan, snapshot, and recovery benchmarks to choose bounded pack,
    concurrency, prefetch, and materialization-budget defaults.
12. Preserve typed tree/backend outcomes across C++ and the C ABI: invalid
    request, backpressure/resource exhaustion, unavailable after bounded mirror
    retries, corruption, and internal invariant failure. A page miss that is
    definitely unavailable but has not failed checksum or structural validation
    must not mutate mapping state or latch the whole tree as corrupt. Snapshot,
    compaction, or GC failure before manifest publication leaves the prior root
    authoritative. Never translate a read error into `NotFound` at the R142
    binding.

## Dependencies

- Depends on the `PageStore`, snapshot, mapping-table, and recovery contracts in
  `doc/design/tree/design-crowdb-tree-storage.md`.
- Depends on the FlatBuffers schemas under `lib/crowdb-protocol/src/fbs/` and
  the callback-based C++ transport in `lib/crowdb-rpc/`. It does not depend on
  `crowdb-chunk-client` or `crowdb-chunkdb-client`.
- Depends on a repository-pinned NVIDIA `stdexec` revision that supports the
  workspace's C++20 GCC and Clang toolchains. Use only the `stdexec` facilities
  corresponding to C++26 `std::execution` in core tree code; isolate any
  non-standard `exec` extension behind a crowdb-tree adapter.
- Defines an injected root-catalog contract but does not depend on R143. R143
  supplies its group0-backed production implementation; R140 tests use an
  in-memory epoch-fenced catalog so the tree backend does not depend on group0.
- R142 consumes the chunk backend constructor and range rebuild API. The chunk
  backend is compiled into crowdb-tree's static archive but is not extracted
  into ordinary crowdb-kv-server because that binary never references its
  constructor.
- R146 adds restart-safe sealing for active B+tree chunks abandoned by a tree
  process. R140 allocates a fresh active chunk on every reopen and does not wait
  for the abandoned chunk to be sealed.
- R147 consumes R140's durable reclaim candidates and performs physical B+tree
  chunk-strip reclamation. R140's logical page and manifest GC remains correct
  before R147 lands, but dead strip capacity is retained.

## Acceptance

- Given an existing local-backend tree, when the common static tree archive is
  linked and a local backend is selected at tree creation, assert its persisted
  bytes and recovery behavior remain compatible. Invariant: adding shared
  storage does not regress local storage. Integration test.
- Given the ordinary crowdb-kv-server build, when its dependency graph and
  linked symbols are inspected, assert neither `crowdb-chunk-client`,
  `crowdb-chunkdb-client`, the chunk-backend constructor, nor its backend object
  code is present; given crowdb-chunk-kv-server, assert the same archive member
  is linked and selected at tree creation. Invariant: one static tree library
  supports runtime store selection without imposing chunk code on the ordinary
  server. Integration test.
- Given a delayed C++ RPC page read, when `ct_get_async` returns pending and the
  RPC callback completes, assert the backend-independent completion eventfd
  wakes the Rust future exactly once without polling or blocking a Tokio
  worker. Invariant: remote async completion is independent of io_uring.
  Integration test.
- Given immediate-before-connect, immediate-after-start, delayed, error, and
  stopped RPC completions, when the custom RPC sender runs, assert its receiver
  observes exactly one matching `set_value`, `set_error`, or `set_stopped`
  signal and the operation state remains alive through completion. Invariant:
  the callback-to-sender adapter has no lost-wakeup, double-completion, or
  use-after-free race. Unit test.
- Given warmed RPC/buffer pools and one pending tree future whose sender
  operation state is embedded in that future, when a page read completes and
  its continuation advances, assert no Promise shared state, `std::function`,
  or additional continuation heap allocation occurs. Invariant: sender
  composition does not add per-stage allocation to a cache miss. Unit test.
- Given three mirror-write senders where one fails or receives stop, when they
  are composed with `when_all`, assert the page pack is not acknowledged and
  remaining work observes cancellation; when all succeed, assert publication
  advances once. Invariant: sender composition preserves three-replica
  durability. Unit test.
- Given a build without liburing and a configured chunk backend, when an async
  page miss completes through `crowdb-rpc`, assert the tree returns the value
  without using the synchronous fallback. Invariant: `AsyncPageStore` support
  is not gated by local io_uring availability. Integration test.
- Given adjacent dirty pages and a configured pack limit, when a snapshot runs,
  assert they are emitted in bounded page packs, written concurrently to three
  mirrors, and represented by ordinal manifest references. Invariant: remote
  snapshot I/O is bounded and not serialized per page. Integration test.
- Given page data approaches 256 MiB in one active B+tree chunk, when the next
  page pack would cross the limit, assert the backend seals that
  chunk and writes the complete pack to a new three-way-mirrored chunk.
  Invariant: no B+tree chunk exceeds 256 MiB and no page pack straddles chunks.
  Integration test.
- Given compressed or short encoded pages, when a page pack is emitted, assert
  each page begins on a 64-KiB boundary, its tail is padded, and its logical
  length and checksum exclude padding; assert DiskIO accepts the resulting
  small-write-aligned requests without tree-side device-alignment logic.
  Invariant: page framing and device I/O alignment have distinct owners. Unit
  test.
- Given a tree process restarts while its previous active chunk is below 256
  MiB, when the tree reopens, assert its first subsequent pack uses a newly
  allocated chunk and the old chunk remains readable at its acknowledged
  cursor for R146 to seal. Invariant: restart never resumes an ambiguously owned
  active chunk. Integration test.
- Given adjacent unloaded pages in one pack and a valid cached layout, when a
  scan crosses them, assert the backend coalesces the requested bytes without a
  metadata query per page; after layout expiry, assert it re-queries before
  returning bytes. Invariant: coalescing never bypasses layout validity.
  Integration test.
- Given dirty pages and a chunk-backed tree, when a snapshot succeeds, assert
  every referenced page and metadata chunk is durable before the new manifest
  is visible. Invariant: no published root references incomplete data.
  Integration test.
- Given a failure before manifest publication, when the tree reopens, assert it
  selects the preceding complete manifest and reports the new chunks as
  reclaimable orphans. Invariant: recovery observes an all-or-nothing snapshot.
  Integration test.
- Given snapshot, compaction, or GC IO fails before publication, when the same
  live tree continues to serve its resident state, assert the prior root remains
  authoritative and maintenance can retry without reopen. Invariant:
  unpublished maintenance work cannot corrupt the active tree. Integration
  test.
- Given one cold page is unavailable after bounded mirror retries but no CRC or
  structure check fails, when its error crosses the C ABI, assert it remains a
  typed availability error, the mapping entry is unchanged, and another
  resident read succeeds; given CRC corruption, assert a distinct corruption
  result is returned. Invariant: recoverable availability and integrity loss
  are never conflated with each other or `NotFound`. Integration test.
- Given a manifest whose page checksum or fence keys are corrupt, when recovery
  or range rebuild reads it, assert the operation fails with corruption and
  does not publish a root. Invariant: routing never relies on unverified page
  bounds. Unit test.
- Given keys on both sides of split key `m`, when `[a, m)` and `[m, z)` are
  rebuilt, assert their union equals the source and their intersection is empty.
  Invariant: a split neither loses nor duplicates a live key. Integration test.
- Given wholly contained, disjoint, and boundary-crossing pages, when a child
  range is rebuilt, assert contained pages are reused, disjoint pages are not
  read, a mixed leaf is referenced by neither child, and its filtered
  replacements contain only in-range keys. Invariant: reuse is permitted only
  with proof of containment. Unit test.
- Given an internal page and leaf sibling link that cross split key `m`, when
  both child roots are traversed without applying an API-level range filter,
  assert every reachable separator, child page, overflow value, and sibling
  leaf belongs to that child's bounds. Invariant: isolation is structural and
  does not depend on callers remembering to filter results. Integration test.
- Given a bounded child whose inherited mapping segment contains reachable and
  unreachable source PIDs, when recovery installs that segment and any page is
  first resolved, assert normal traversal cannot reach the unrelated PID and a
  page whose verified fences escape the child range is rejected as corruption.
  Invariant: a shared mapping image grants storage resolution, not key-range
  authority. Integration test.
- Given two child manifests created from one source snapshot, when split
  publication completes, assert their directories may reference the same
  immutable mapping and page-reference segment images but their in-memory
  tables and writable segments are distinct; after either child changes a slot
  in that segment, assert only that child publishes COW images and inherited
  ordinals still resolve. Invariant: metadata sharing is immutable and
  lineage-scoped. Integration test.
- Given several durable parent snapshots and live in-memory snapshot handles,
  when one selected parent snapshot is split and mapping materialization runs,
  assert only the two current child lineages receive new mapping images while
  every older snapshot remains readable and pins its original images and page
  packs. Invariant: split and mapping GC never rebuild or mutate historical
  snapshots. Integration test.
- Given a historical snapshot remains pinned indefinitely, when current-child
  materialization completes, assert the current mapping and packs become
  child-exclusive but the historical objects remain retained and pinned-byte
  and oldest-pin metrics identify the blocker. Invariant: an old snapshot may
  delay physical reclamation but cannot delay current-lineage cleanup or split
  publication. Integration test.
- Given one physical page pack containing a mixed source page plus pages reused
  by one or both children, when the split publishes, assert child manifests
  traverse only their permitted page ordinals even though inherited metadata
  may still resolve unrelated unreachable ordinals; after the pack's final
  manifest reference clears, assert the whole pack becomes reclaimable.
  Invariant: physical byte coexistence cannot create logical key reachability
  or premature reclamation. Integration test.
- Given a child still references source mapping images and page packs, when
  bounded maintenance and foreground COW make progress, assert each published
  generation removes only PIDs unreachable from its current bounded root and
  eventually references child-owned mapping images and packs; injected failure
  leaves the previous generation readable and retryable. Invariant: cleanup is
  monotonic, crash-safe, and outside split cutover. Integration test.
- Given mapping materialization pins generation `g` while a foreground
  checkpoint publishes `g + 1`, when the stale materialization finishes,
  assert it cannot replace `g + 1`, its unpublished objects are reclaimed, and
  cleanup retries from the newer manifest. Invariant: background cleanup never
  rolls back acknowledged tree state. Integration test.
- Given empty, prefix-adjacent, minimum-unbounded, and maximum-unbounded ranges,
  when rebuild completes, assert scans exactly match the half-open range
  predicate. Invariant: all endpoint forms use one ordering contract. Unit test.
- Given multiple disjoint target ranges, when rebuild workers run concurrently,
  assert each publishes an independently recoverable manifest and the source
  remains readable. Invariant: parallel dump has no shared mutable tree state.
  E2E test.
- Given a source manifest still referenced by a live partition, when chunk GC
  advances for newer manifests, assert its chunks remain; after every reference
  passes the watermark, assert they become reclaimable. Invariant: reclamation
  never precedes the last manifest reference. Integration test.
- Given a snapshot and a range rebuild with known page counts, when both finish,
  assert chunk I/O, page reuse/rebuild, filtered-key, publication-latency, and
  orphan metrics match the observed operations. Invariant: every material
  persistence outcome is observable. Integration test.
- Given each supported GCC and Clang toolchain, when the common static tree
  archive compiles against the pinned `stdexec` revision and is linked once by
  each server, assert public headers remain stdexec-free and record template
  compile time and binary-size deltas. Invariant: the async implementation does
  not leak into crowdb-tree's public ABI. Integration test.
- Given fixed cold-read, warm-read, coalesced-scan, snapshot, and recovery
  workloads, when candidate pack, concurrency, and prefetch settings are
  measured, assert the selected defaults satisfy the latency, throughput,
  memory, metadata-query, and read-amplification limits declared by the
  permanent design. Invariant: remote-I/O defaults are evidence-based and
  bounded. Integration test.

Required gates:

- `pixi run tree-fmt`
- `pixi run tree-lint`
- `pixi run test-tree-ct`
- `pixi run test-tree-ffi`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`

## Code Review Findings and Required Changes

The selected design is one private, B+tree-specific C++ chunk backend in
crowdb-tree, with native async flows composed through pinned NVIDIA `stdexec`.
All backends are selected when a tree is created and live in one static tree
archive. There is no Cargo feature for backend choice, Rust page-I/O adapter,
public C++ chunk client, dynamically loaded backend, Folly Future/Promise path,
or second tree library.

### Existing constraints

- `lib/crowdb-tree/CMakeLists.txt` declares `crowdb-tree` as a static library,
  and `crowdb-tree-ffi/build.rs` also compiles the C++ sources into a static
  archive that is linked into the final Rust executable. `ct_options.backend`
  already selects file, block, or memory storage at tree creation; the missing
  piece is an injected backend handle so `ct_open` no longer has to construct
  every concrete backend itself.
- The current synchronous `PageStore` models a mutable local byte address
  space. `persist.cpp` allocates extents, writes fixed anchor slots, discovers
  gaps, and calls `size()` and `delete_block()`. The mapping table packs a
  local byte address and IU-sized length into one 64-bit unloaded-page word.
  Chunk storage instead needs immutable page-pack references resolved through a
  manifest; it cannot faithfully implement the current byte-device contract.
- `AsyncPageStore` already has callback-based read, write, and barrier methods,
  but its use in `Config`, demand loading, and snapshot writing is gated by
  `CROWDB_HAVE_LIBURING`. `ct_open` constructs only
  `BlockAsyncPageStore`, paired with `DiskIOUring`.
- Rust async tree futures currently wait only on fds returned by
  `ct_uring_eventfds`. If a future were completed by a `crowdb-rpc` worker
  without one of those fds, `drive_ct_future` would repeatedly
  `yield_now`; it would not receive a completion wakeup.
- `crowdb-rpc` already provides a callback-based C++ client. TCP uses its own
  epoll/kqueue workers and RDMA uses its completion path. DiskIO C++ tests build
  FlatBuffers requests and receive payloads without Rust. The current CMake
  generation list does not include the ChunkDB schema, so the tree build must
  generate it and its schema dependencies for the isolated backend translation
  unit.
- Shared FlatBuffers define the wire format only. The backend must still
  implement the B+tree subset of allocation revision handling, writer fencing,
  acknowledged cursors, mirror writes, layout validity, retries, routing,
  ambiguous completion recovery, and metrics.

### Chosen backend boundary

The backend source lives in `lib/crowdb-tree/src/backend/chunk/` and remains
private to the tree engine. Local stores live under `src/backend/local/`, while
B+tree, mapping-table, memory-table, and snapshot implementation files live
under `src/btree/`, `src/maptable/`, `src/memtable/`, and `src/snapshot/`.
Public interfaces are grouped under matching subdirectories without flat
forwarding headers; callers use the canonical subsystem path or the umbrella.
CMake and `crowdb-tree-ffi/build.rs` recursively compile the isolated backend
object into the same static `libcrowdb-tree.a`; crowdb-tree is not a shared
library and the backend is not loaded dynamically. Backend selection happens
per tree instance through an opaque store handle passed at creation.

The backend-neutral `ct_open` translation unit must not call the chunk
constructor. This preserves static archive extraction: only
`crowdb-chunk-kv`, which explicitly creates a chunk backend handle, introduces
the unresolved constructor/RPC symbols that cause the linker to extract the
chunk object. `crowdb-kv-server` keeps using the local constructor and does not
gain a chunk-client dependency or chunk backend code in its final executable.

```text
crowdb-kv-server -> crowdb-kv -> crowdb-tree-ffi -> local backend handle

crowdb-chunk-kv-server -> crowdb-chunk-kv -> crowdb-tree-ffi
                              |                    `-> crowdb-tree C++ core
                              `-> chunk backend handle
                                      |-> crowdb-tree chunk backend object
                                      |-> crowdb-rpc C++
                                      `-> shared FlatBuffers schemas
```

The backend exposes no allocate/read/write object API to other users. It
accepts tree page batches, root-catalog operations, and immutable topology
snapshots, then issues only the ChunkDB and DiskIO commands required for
B+tree page packs. `crowdb-chunk-kv` may refresh group0 topology and inject an
immutable snapshot through a low-frequency control call; page reads and writes
never call Rust.

Generation-addressed page packs in 256-MiB, three-way-mirrored chunks are the
first-version format. Every encoded page starts at a 64-KiB boundary; DiskIO
retains responsibility for physical-device alignment. Content deduplication is
out of scope. A contained interior subtree may be reused when
its inherited fences are verified. A mixed boundary leaf is reused by neither
child; each child gets a newly filtered leaf. Boundary ancestors, child roots,
and the final included leaf are rewritten so separator, child, overflow, and
right-sibling reachability cannot escape the destination range. The immutable
source page may remain physically present in a retained page pack, but it is
not reachable from either child manifest and is reclaimed only after the last
pack reference disappears.

Mapping and the ordinal-to-page-reference table follow the same immutable-COW
rule at segment granularity. Each child gets its own manifest and directories
immediately, but unchanged directory entries may point to the selected source
snapshot's immutable segment images. There is no recursive overlay lookup: a
directory entry directly names the one image to load. The recovered in-memory
tables are independent. A child slot update writes child-owned segment images;
bounded background mark/materialize passes clear unreachable inherited slots
and eventually eliminate shared images.

The selected base snapshot is split once. Older durable generations and
`PinnedSnapshot` handles are not rebuilt: they retain their own manifest or
page pins until normal retention releases them. Current code keeps durable A/B
generations and implements `PinnedSnapshot` by pinning page frames rather than
copying a mapping table, so snapshot count does not multiply initial split
mapping work.

The cost is deliberately split into two phases. Publication copies directory
entries and writes changed boundary metadata, so it is O(number of metadata
segments), not O(number of pages). Eventual cleanup performs one bounded mark
of the current child root plus COW writes for still-shared segments and live
page-pack bytes. That work is O(current reachable pages), but it is resumable,
outside the split fence, and never repeated for each historical snapshot.
Historical snapshot count affects retained bytes and reclamation latency only.

### Async decision

Use sender/receiver for the native async composition. `std::future` is not
suitable because the tree cannot block for a page miss and needs continuation
placement, fan-out/fan-in, cancellation, and error composition. Folly
Future/Promise supplies continuations, but would introduce a Folly-specific
shared-state API and executor semantics that are not the C++ standard direction.

Use a pinned revision of
[NVIDIA stdexec](https://github.com/NVIDIA/stdexec), the reference
implementation of the C++26 `std::execution` sender model. It is header-only,
supports C++20 with the workspace's compilers, and supplies the required lazy
composition, `when_all`, `let_value`, `continues_on`, and stop-token model. Do
not track its `main` branch. Keep stdexec types internal so the public C ABI and
persisted format do not depend on a specific implementation revision.

Do not put network RPC under `DiskIOUring`. io_uring remains the local
file/block submission engine; `crowdb-rpc` retains its epoll/kqueue or RDMA
transport loop. The two engines integrate by exposing senders with the same
completion signatures:

1. Add narrow `UringReadSender`, `UringWriteSender`, `RpcCallSender`, and
   backend operation senders. Their operation states own all buffers, request
   handles, receiver state, and cancellation registration until exactly one
   terminal signal.
2. Replace `AsyncPageStore`'s `std::function` completion with a fixed completion
   token consisting of a function pointer and operation-state context. A custom
   sender owns that operation state and passes its completion trampoline to the
   virtual backend. Backend callbacks are permitted only at this adapter edge;
   tree workflows never build nested callback chains. Concrete sender types
   stay behind an internal template boundary and never cross the C ABI.
3. Model one page-pack write as three mirror-write senders joined by
   `stdexec::when_all`, followed by acknowledged-cursor and manifest steps via
   `let_value`. Errors and `set_stopped` prevent publication. Never use
   `sync_wait` on a tree, RPC, Tokio, or I/O worker.
4. Make execution placement explicit. RPC and io_uring adapters deliver their
   initial completion on their native worker. Any decode, mapping install,
   retry decision, or next-stage construction that is not constant bounded
   work transfers through `continues_on` to a tree continuation scheduler.
   That scheduler uses a bounded, preallocated queue and adds no mutex or
   per-operation allocation to the page lookup hot path.
5. Add a backend-independent completion notifier owned by each tree handle.
   The final receiver publishes the `ct_future` result and then signals this
   eventfd. The Rust pump waits on the generic completion fd, never on a
   transport's internal epoll/io_uring notification fd.
6. Propagate a stop token from tree close, request cancellation, and failed
   `when_all` branches into queued or in-flight operations. If the underlying
   transport cannot cancel a request, retain the operation state and buffers
   until its late completion is safely discarded; cancellation is not
   permission to free callback state early.
7. During shutdown, stop admission, request stop, drain accepted operation
   states to one terminal completion each, stop the completion pump, and only
   then destroy the tree and transports.

Use only standard-track facilities from the `stdexec` namespace in tree logic.
If a temporary `exec` extension is unavoidable, confine it to one adapter and
test the equivalent C++26 replacement contract. This keeps migration to the
standard library mechanical when the deployed toolchain provides
`std::execution`.
