<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Tree Chunk Page Store Plan

Source:
[`../design/tree/design-crowdb-tree-chunk-storage.md`](../design/tree/design-crowdb-tree-chunk-storage.md)
and [`../backlog/R140-tree-chunk-page-store.md`](../backlog/R140-tree-chunk-page-store.md).

Goal: add an injected, asynchronously completed immutable chunk page backend
and structurally safe range rebuild while preserving local tree behavior.

## Phase 1: Backend and Completion Boundaries

- [x] **Reorganize sources and headers**: group B+tree, mapping-table page
  service, memory-table, snapshot, and backend implementations under
  `src/{btree,maptable,memtable,snapshot}/` and
  `src/backend/{local,chunk}/`; group canonical headers under matching public
  subfolders, keep only shared interfaces at the root, and provide an umbrella
  header. Files:
  `lib/crowdb-tree/{include,src,CMakeLists.txt}`, `ffi/build.rs`.
- [x] **Remove forwarding headers**: switch internal and test includes to the
  canonical subsystem paths, then delete redundant root forwarding headers.
  Files: `lib/crowdb-tree/include/crowdb-tree/`,
  `lib/crowdb-tree/{src,tests}/`.

- [x] **Decouple async I/O**: make `AsyncPageStore` platform-neutral. Files:
  `include/crowdb-tree/backend/async_page_store.h`, `include/crowdb-tree/config.h`,
  `src/backend/local/block_async_page_store.cpp`, `src/btree/crowdb-tree.cpp`,
  `src/snapshot/persist.cpp`.
- [x] **Inject stores**: add opaque store ownership and make `ct_open` consume a
  supplied handle without naming chunk construction. Files:
  `include/crowdb-tree/c_api.h`, `src/c_api.cpp`, `ffi/src/sys.rs`,
  `ffi/src/config.rs`, `ffi/src/tree.rs`.
- [x] **Wake all futures**: add one backend-independent completion descriptor
  and use it from C++ and Rust futures. Files: `src/async_completion.cpp`,
  `src/c_api.cpp`, `ffi/src/reactor.rs`, `ffi/src/tree.rs`.
- [x] **Verify boundary tests**: cover terminal races, local
  compatibility, no-liburing async, and exact wakeup. Files:
  `tests/unit/async_sender_test.cpp`, `tests/integration/c_api_test.cpp`,
  `ffi/tests/ffi_test.rs`.

## Phase 2: Chunk Manifest and Page Packs

- [x] **Pin stdexec**: add the repository dependency and isolate the adapter
  from public headers. Files: `third-party/stdexec/`, `CMakeLists.txt`,
  `ffi/build.rs`, `src/stdexec_adapter.cpp`.
- [x] **Define manifest core**: implement page references, checksums,
  generations, an epoch-fenced root catalog, and orphan accounting.
  Files: `src/backend/chunk/chunk_page_store.h`,
  `src/backend/chunk/chunk_page_store.cpp`.
- [x] **Segment manifest metadata**: add ordinal reference tables, immutable
  segment directories, pins, retention watermarks, and orphan reclamation.
  Files: `src/backend/chunk/chunk_page_store.h`,
  `src/backend/chunk/chunk_page_store.cpp`.
- [x] **Implement native chunk transport**: add direct C++ ChunkDB allocation,
  append, query, seal, and DiskIO mirror read/write RPC adapters with a bounded
  completion slab and immutable topology injection. Files:
  `src/backend/chunk/`, protocol/CMake generation wiring, `crowdb-chunk-kv`.
- [x] **Implement chunk rotation and page framing**: use mirror strips, pad page
  tails to 64 KiB, rotate whole packs at 256 MiB, allocate a fresh chunk after
  reopen, and emit logical reclaim candidates. Files:
  `src/backend/chunk/`, chunk backend tests.
- [x] **Move chunk operations off the caller**: replace inline completion with
  a bounded, lock-free, ordered executor; return stable operation IDs, cancel
  queued work, reject overflow with resource exhaustion, and drain callbacks
  during shutdown. Files: `src/backend/chunk/chunk_async_executor.*`,
  `src/backend/chunk/chunk_page_store.*`,
  `tests/integration/chunk_page_store_test.cpp`.
- [x] **Harden reads and publication**: cache one immutable checksummed pack for
  adjacent reads, observe cancellation around transport completion, hard-cap
  logical chunk data at 256 MiB, and carry one generation fence from manifest
  construction through publication. Files: `src/backend/chunk/`,
  `tests/integration/chunk_page_store_test.cpp`.
- [x] **Implement concurrent pack pipeline**: add bounded concurrent pack
  writes, stdexec mirror fan-in, coalesced reads, and in-flight cancellation.
  Preserve the landed layout refresh, bounded mirror retry, publication, and
  typed failures. Files: `src/backend/chunk/`, protocol/CMake generation
  wiring.
- [x] **Verify chunk persistence**: cover pack bounds, durability ordering,
  all-or-nothing recovery, maintenance failure, layout refresh, mirror retry,
  typed errors, retention, and metrics. Files:
  `tests/unit/chunk_page_store_test.cpp`,
  `tests/integration/chunk_page_store_test.cpp`.

## Phase 3: Range Policy and Rebuild

- [x] **Centralize range policy**: validate public operations, recovery, and
  installed pages against immutable bounds. Files:
  `include/crowdb-tree/btree/key_range.h`, `src/btree/key_range.cpp`,
  `include/crowdb-tree/config.h`, `src/btree/crowdb-tree.cpp`,
  `src/snapshot/persist.cpp`.
- [x] **Persist page fences**: encode and verify checksummed leaf/inner
  reachability bounds including siblings and overflow chains. Legacy native
  frames without fences are structurally validated and upgraded during import;
  fixed-size inner frames identify their leftmost and rightmost reachable leaf
  pages, whose exact key fences remain in those leaf frames. Files:
  `include/crowdb-tree/maptable/frame_page.h`, `src/maptable/frame_page.cpp`,
  `src/maptable/page_codec.cpp`.
- [x] **Complete range rebuild**: replace whole-snapshot collection with a
  resumable 4-MiB native-frame cursor and disjoint-subtree skipping. Preserve
  overwritten source pages only while their cursor is active, release consumed
  pins, reject post-generation PIDs, and fail retryably if concurrent mutation
  exceeds the preservation budget. Retain the landed filtered boundary and
  sibling rebuild, independent roots, high-water allocation, shared immutable
  pack references, and concurrent workers. Files:
  `include/crowdb-tree/btree/{tree.h,range_rebuild.h}`,
  `src/btree/{crowdb-tree.cpp,range_rebuild.cpp}`,
  `include/crowdb-tree/c_api.h`, `src/c_api.cpp`.
- [x] **Verify structural isolation**: cover split union/intersection, disjoint
  skip, mixed leaves, crossing paths/siblings, shared immutable metadata,
  endpoint forms, concurrent workers, and corrupt fences. Files:
  `tests/unit/range_rebuild_test.cpp`,
  `tests/integration/range_rebuild_test.cpp`.

## Phase 4: Materialization, Metrics, and Link Isolation

- [x] **Materialize shared chunk packs**: copy owner-qualified shared packs in
  bounded passes, COW the affected reference-table segments, publish through
  the tree snapshot generation gate, abandon ambiguous chunk cursors, and keep
  stale or failed work retryable. Persist explicit pack ownership with legacy
  manifest compatibility and reject incompatible inherited storage geometry.
  Files: `include/crowdb-tree/{backend/page_store.h,btree/tree.h,c_api.h}`,
  `src/{backend/chunk,c_api.cpp,snapshot/persist.cpp}`, `ffi/`.
- [x] **Share and materialize mapping images**: make mapping slots reference
  pack ordinals, share immutable mapping-directory images, mark reachable
  slots, and clear unreachable slots before exclusive publication. Immutable
  mapping images are now inherited by generation, changed pages COW their
  segment, and bounded reachability passes clear unrelated PIDs with stale
  generation retry. Mapping images now distinguish legacy local byte locations
  from chunk page-reference ordinals with a compatible tagged 64-bit word and
  versioned mixed segment images. Version-3 manifests validate those references
  through immutable reference segments, and live-extent repack drops dead packs
  while later checkpoints preserve sparse holes. Files:
  `src/btree/range_rebuild.cpp`, `src/maptable/`,
  `src/backend/chunk/chunk_page_store.cpp`.
- [x] **Complete observability**: pack reuse/write and materialization bytes,
  cache/layout queries, mirror attempts/failures, retention pins, and orphan
  bytes are exposed. Register the remaining chunk latency, coalescing, rebuild,
  publication, and recovery metrics. Files:
  `include/crowdb-tree/crowdb-tree.h`, `src/btree/crowdb-tree.cpp`,
  `src/backend/chunk/chunk_page_store.cpp`, `src/btree/range_rebuild.cpp`.
- [x] **Verify retention and races**: cover historical pins, current-lineage
  cleanup, mixed packs, failure retry, stale materialization, reclamation
  watermark, and metric counts. Files:
  `tests/integration/chunk_materialization_test.cpp`.
- [x] **Verify archive isolation**: inspect ordinary and chunk link surfaces,
  plus GCC/Clang public-header builds. Files:
  `tools/test-tree-chunk-link-isolation.sh`, pixi task definitions.
- [x] **Benchmark defaults**: record fixed workload limits and tune bounded
  defaults. Files: tree benchmark sources and permanent chunk-storage design.

## Phase 5: Gates and Documentation

- [x] **Run affected tests separately**: `pixi run tree-fmt`,
  `pixi run tree-lint`, `pixi run test-tree-ct`, and
  `pixi run test-tree-ffi`, and the chunk-KV integration tests. The final
  implementation passed 566 C++ tests, 35 Rust FFI tests, and the
  focused ASAN sparse-repack/reuse/reopen cases.
- [x] **Fold permanent design**: create
  `doc/design/tree/design-crowdb-tree-chunk-storage.md`, update the tree root and
  `doc/doc_index.md`, then remove the working design.
- [ ] **Run repository gates**: `pixi run -- cargo fmt --all -- --check`,
  `pixi run rs-lint`, and `pixi run test-suite`.
- [ ] **Cleanup requirement**: remove the R140 detail and backlog row, delete
  this completed plan, and commit cleanup separately.

## Consolidated Files

- Tree API and implementation: `lib/crowdb-tree/include/crowdb-tree/`,
  `lib/crowdb-tree/src/`.
- Rust FFI: `lib/crowdb-tree/ffi/`.
- Build and dependency wiring: `lib/crowdb-tree/CMakeLists.txt`, `pixi.toml`,
  `pixi.lock`, `third-party/stdexec/`.
- Tests and tools: `lib/crowdb-tree/tests/`,
  `lib/crowdb-tree/ffi/tests/`, `tools/`.
- Documentation: `doc/design/tree/`, `doc/doc_index.md`, `doc/backlog/`,
  `doc/working/`.

## Tests

- Unit: completion races/allocation, mirror composition, manifest codecs,
  fence validation, range endpoints and rebuild classification.
- Integration: local compatibility, async wakeup without liburing, chunk
  snapshot/recovery/failure, structural split isolation, COW/materialization,
  retention, metrics, and archive extraction.
- E2E: parallel disjoint range rebuild against a shared pinned source.

## Remaining Audit Findings

- The production `crowdb-rpc`/FlatBuffers transport, ordered async executor,
  immutable layout cache, checksummed pack-read cache, shutdown drain, and
  queued/in-flight cancellation are present. Page-pack writes now use bounded
  concurrent `stdexec::when_all` mirror fan-out, native RPC callbacks, and one
  process-wide lock-free fallback I/O queue for embedded synchronous
  transports. Framing memory is bounded by the configured in-flight pack
  window; failures and close stop admission, drain late completions, and keep
  cursor advancement and manifest publication on the ordered tree worker.
- Reference segments and mapping images are immutable directory entries shared
  across child lineages and COW-replaced when their entries change. Mapping
  materialization marks reachable PIDs in bounded passes, clears unrelated
  inherited slots, and restarts if a foreground flush changes the tree
  generation. Chunk mapping slots use a compact reference-ordinal tag while
  local and legacy images retain their byte-location encoding. Resolution
  validates immutable reference-segment coverage. Current-lineage repack uses
  both retained A/B anchors' live extents, publishes sparse pack layouts, and
  tracks later writes so dead holes are not resurrected by checkpoints.
- Checksummed page fences are persisted and verified against each native graph;
  legacy frames are upgraded after structural validation. Range rebuild now uses
  separator-guided resumable native traversal in 4-MiB batches, folds in-frame
  overlays before export, skips disjoint subtrees before demand load, filters
  boundary leaves, preserves high-water page allocation, and supports
  concurrent independent workers. Overwritten source versions are pinned only
  until consumed; concurrent mutation is bounded by a retryable preservation
  budget.
  Child chunk manifests now inherit byte-verified immutable source packs,
  reference segments, and matching mapping images; later checkpoints COW only
  changed logical packs and their affected metadata segments. An iterator page
  is reused only when the inherited source generation and its durable mapping
  descriptor still match, so unsnapshotted and concurrent source changes are
  copied into child-owned pages.
- Chunk-store counters now cover RPC/DiskIO latency, coalescing, completion
  wakeups, publication and recovery latency, metadata ownership, retention,
  orphan accounting, and bounded materialization scan/write work. Link-map and
  symbol checks keep chunk and RPC archive members out of ordinary tree links,
  and GCC/Clang compile the public umbrella without private async headers.
- The fixed 4-MiB in-memory transport benchmark (three aggregate repetitions,
  11 September 2026) measured a 0.79-ms 64-KiB cold read, a 4.4-us coalesced
  read, and a 26.2-ms 4-MiB snapshot. These results retain the bounded defaults
  of 4-MiB packs, eight concurrent packs, and 64-MiB materialization passes;
  production RPC latency remains visible through the runtime counters.
