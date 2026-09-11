<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Tree Chunk Page Store Plan

Source: [`design-tree-chunk-page-store.md`](design-tree-chunk-page-store.md) and
[`../backlog/R140-tree-chunk-page-store.md`](../backlog/R140-tree-chunk-page-store.md).

Goal: add an injected, asynchronously completed immutable chunk page backend
and structurally safe range rebuild while preserving local tree behavior.

## Phase 1: Backend and Completion Boundaries

- [~] **Reorganize private sources**: group B+tree, mapping-table, and backend
  implementations under `src/btree/`, `src/mapping_table/`, and
  `src/backend/{local,chunk}/` while retaining public include paths and static
  archive behavior. Files: `lib/crowdb-tree/src/`, `CMakeLists.txt`,
  `ffi/build.rs`.

- [x] **Decouple async I/O**: make `AsyncPageStore` platform-neutral. Files:
  `include/crowdb-tree/async_page_store.h`, `include/crowdb-tree/options.h`,
  `src/block_async_page_store.cpp`, `src/crowdb-tree.cpp`, `src/persist.cpp`.
- [x] **Inject stores**: add opaque store ownership and make `ct_open` consume a
  supplied handle without naming chunk construction. Files:
  `include/crowdb-tree/c_api.h`, `src/c_api.cpp`, `ffi/src/sys.rs`,
  `ffi/src/options.rs`, `ffi/src/tree.rs`.
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
  Files: `src/chunk_page_store.h`, `src/chunk_page_store.cpp`.
- [ ] **Segment manifest metadata**: add ordinal reference tables, immutable
  segment directories, pins, retention watermarks, and orphan reclamation.
  Files: `src/chunk_page_store.h`, `src/chunk_page_store.cpp`.
- [ ] **Implement native chunk transport**: add direct C++ ChunkDB allocation,
  append, query, seal, and DiskIO mirror read/write RPC adapters with a bounded
  completion slab and immutable topology injection. Files:
  `src/backend/chunk/`, protocol/CMake generation wiring, `crowdb-chunk-kv`.
- [ ] **Implement chunk rotation and page framing**: use mirror strips, pad page
  tails to 64 KiB, rotate whole packs at 256 MiB, allocate a fresh chunk after
  reopen, and emit logical reclaim candidates. Files:
  `src/backend/chunk/`, chunk backend tests.
- [ ] **Implement asynchronous chunk store**: add bounded concurrent pack
  writes, stdexec mirror fan-in, layout caching, coalesced reads, refresh,
  publication, cancellation, shutdown drain, and typed failures. Files:
  `src/backend/chunk/`, protocol/CMake generation wiring.
- [ ] **Verify chunk persistence**: cover pack bounds, durability ordering,
  all-or-nothing recovery, maintenance failure, layout refresh, mirror retry,
  typed errors, retention, and metrics. Files:
  `tests/unit/chunk_page_store_test.cpp`,
  `tests/integration/chunk_page_store_test.cpp`.

## Phase 3: Range Policy and Rebuild

- [x] **Centralize range policy**: validate public operations, recovery, and
  installed pages against immutable bounds. Files:
  `include/crowdb-tree/key_range.h`, `src/key_range.cpp`,
  `include/crowdb-tree/options.h`, `src/crowdb-tree.cpp`, `src/persist.cpp`.
- [ ] **Persist page fences**: encode and verify leaf/inner reachability bounds
  including siblings and overflow chains. Native installation now verifies the
  complete child graph, separator bounds, leaf order, and overflow reachability;
  serialized lower/upper fence keys remain open. Files:
  `include/crowdb-tree/frame_page.h`, `src/frame_page.cpp`,
  `src/page_codec.cpp`.
- [ ] **Implement range rebuild**: add bounded native iteration, leaf-frame
  reuse, filtered boundary/sibling rebuilding, independent roots, high-water
  allocation, and concurrent workers. Files: `include/crowdb-tree/chunk_page_store.h`,
  `src/range_rebuild.cpp`, `include/crowdb-tree/c_api.h`, `src/c_api.cpp`.
- [ ] **Verify structural isolation**: cover split union/intersection, disjoint
  skip, mixed leaves, crossing paths/siblings, shared immutable metadata,
  endpoint forms, concurrent workers, and corrupt fences. Files:
  `tests/unit/range_rebuild_test.cpp`,
  `tests/integration/range_rebuild_test.cpp`.

## Phase 4: Materialization, Metrics, and Link Isolation

- [ ] **Materialize child ownership**: mark reachability, COW shared segments,
  repack shared pages, generation-fence publication, and retry stale work.
  Files: `src/range_rebuild.cpp`, `src/chunk_manifest.cpp`.
- [ ] **Add observability**: register chunk latency, layout, coalescing, pack,
  rebuild, sharing, retention, publication, recovery, and orphan metrics.
  Files: `include/crowdb-tree/crowdb-tree.h`, `src/crowdb-tree.cpp`,
  `src/chunk_page_store.cpp`, `src/range_rebuild.cpp`.
- [ ] **Verify retention and races**: cover historical pins, current-lineage
  cleanup, mixed packs, failure retry, stale materialization, reclamation
  watermark, and metric counts. Files:
  `tests/integration/chunk_materialization_test.cpp`.
- [ ] **Verify archive isolation**: inspect ordinary and chunk server symbols,
  plus GCC/Clang public-header builds. Files:
  `tools/test-tree-chunk-link-isolation.sh`, pixi task definitions.
- [ ] **Benchmark defaults**: record fixed workload limits and tune bounded
  defaults. Files: tree benchmark sources and permanent chunk-storage design.

## Phase 5: Gates and Documentation

- [ ] **Run affected tests separately**: `pixi run tree-fmt`,
  `pixi run tree-lint`, `pixi run test-tree-ct`, and
  `pixi run test-tree-ffi`, and the chunk-KV integration tests. The previous
  baseline passed 473 C++ tests and 34 Rust FFI tests; rerun every affected
  acceptance after the remaining implementation lands.
- [ ] **Fold permanent design**: create
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

- The current `ChunkPageStore` keeps replica bytes inside `MemoryRootCatalog`;
  it has no production `crowdb-rpc`/FlatBuffers transport, genuinely delayed
  completion, coalesced DiskIO read, or stdexec mirror fan-in.
- Reference segments are embedded in each manifest instead of immutable
  directory-addressed images. Mapping slots still hold local byte addresses.
- Page fences are reconstructed during native snapshot validation rather than
  persisted. Range rebuild copies frames into the destination and does not
  skip disjoint unloaded subtrees or share immutable mapping/reference images.
- Child materialization/repack, archive extraction checks, and fixed-workload
  benchmark evidence remain absent.
