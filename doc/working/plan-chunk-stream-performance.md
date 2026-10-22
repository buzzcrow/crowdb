<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk Stream Performance Plan

Upstream: [R244](../backlog/R244-chunk-stream-incremental-directory-publication.md)
and [Chunk-Stream design](../design/chunkds/design-crowdb-chunk-stream.md).

Goal: make synchronized mirror writes complete ordinary append without Stream
KV work, with compact locations, incremental directories, and bounded recovery.

## Status and scope

- Code audit: 2026-10-23, against the shared working tree. The first observed
  HEAD was `6a2a8796`; unrelated deployment work is proceeding independently.
- Implementation starts after completed OCI deployment cleanup at `c3d7d64d`.
  T01 is active: build matched release binaries, record baseline identities and
  run the existing MemDisk/mem-block sentinels without changing their gates.
- T02/T03 record Q1–Q4 design decisions only. The new append completion,
  framing, ownership, directory and performance contracts are not implemented.
- Keep this plan live during implementation and delete it only with completed
  requirement cleanup. Existing unrelated changes and runtime resources stay
  outside this work's ownership.

## Gap assessment

The gap is medium-to-large: the existing transport, mirror repair, framing,
bounded queue, CAS adapter, and instrumentation are reusable, but the Stream
state machine and persisted directory model need coordinated changes. Deleting
two calls in `write_batch` cannot produce a correct working implementation.

- **Compact locations — medium, D01/A01.** `ChunkLocation`,
  `framed_physical_length`, and `physical_range_for_subrange` already exist.
  However, `write_batch`, `Extent`, `StreamExtentPage::frame_lengths`,
  `resolve_extent`, and `PhysicalRead` all assume one physical frame per extent.
  Encoding, storage format, recovery, and reads must change together.
- **Common physical format — large, D01/A15.** The selected codec must serve
  Rust and C++, user-data writers/readers, native buffers, Stream, and tree
  chunk/page storage. The current `u16` payload size and 64 KiB limit prevent a
  single large append frame; the selected bounded groups retain that maximum
  and add Stream-only roles/completeness fields to the common format. The
  existing Rust/C++ CRC input ranges differ; define
  new-version bytes and legacy decoding explicitly, with shared golden vectors.
  User data keeps ordinary whole-object publication and streaming without a
  known total length. Do not add Stream grouping or whole-object buffering.
- **Combined write/sync RPC — medium, D02/A02.** DiskIO already has ordered
  writes and async flush. The missing work crosses `FBDiskWriteRequest`, wire
  constructors, client durability handling, server completion, `DiskWriter`,
  and both ordinary and replacement writes in `MirrorStripFlow`. Passing
  `Durability::Fsync` today still performs two requests. A new boolean alone is
  unsafe with an old server that ignores it and returns ordinary write success.
- **In-memory progress — medium, D03/D04/A03/A04.**
  `production_chunk::update_state` copies persisted
  `acknowledged_cursor` into `ChunkStateView::cursor`. With deferred checkpoints,
  a growth/repair/query refresh can rewind local synchronized progress. The next
  write and `seal` also check that same atomic cursor. Separate logical progress
  ownership from layout refresh before removing cursor commits.
- **Read availability and tail reconstruction — large, D06/A06/A07.**
  `recover_active_frames` is bounded by a supplied persisted cursor and issues
  a header read plus verified-frame read per frame. `ChunkReader` derives
  `durable_bytes` from `acknowledged_cursor`; `StripReader` returns
  `NotYetAvailable` beyond that bound. Fresh acknowledged data cannot simply
  bypass these limits through the existing reader. Verified-frame reads also
  query layout per call, so frame-by-frame recovery creates avoidable metadata
  traffic. A bounded scan/read contract and batched parsing are needed.
- **Finalization and ownership — large, D05/D09/A05/A10.** A content scanner
  already exists: `FinalizeChunkTaskHandler` calls `scan_complete_frame_boundary`,
  and `seal_chunk_bytes` can persist an exact final boundary. Reuse this work,
  but the scanner currently selects one available mirror and reads per frame;
  it does not implement the selected Q1 group boundary or Q2 rule. The legacy
  `seal_expired_writer_chunks` seals at the persisted cursor, while the task
  handler treats an already sealed chunk as complete. Deferred cursors must not
  let expiry finalize a stale length or delete data after a failed scan. Idle
  liveness checks do not implement the selected Group-0 lease protocol. The
  server already has serving grants, monotonic deadlines, and durable transfer
  release proofs to compose with Stream ownership.
- **Segmented directory — large, D07/D08/A08/A09.**
  `StreamManifest::extent_pages` is flat; page identity includes head epoch and
  generation. `publish_state` reconstructs all extents, and `open` loads every
  page and collects all extents. Revision reuse and eight-way writes can stay,
  but page identity, root/index, tail builder, lookup, and open must be replaced.
- **Retention and cache — large, D11/A12.** Reclamation deletes pages by
  generation, and `ExtentPageCache` copies a growing map and invalidates it by
  generation. Neither supports history shared across roots. Persisted root
  references, consumer pins, candidate-page protection, and bounded cache
  accounting must be explicit before shared pages become reclaimable.
- **Live views and WAL integration — large, D10/A11/A13.** `open_read_only`
  deliberately stops at the published manifest boundary. Chunk-KV checkpoints,
  split artifacts, replay, and reclamation carry Stream generation pins. With
  an older checkpoint and newer in-memory tail, implement the selected owner
  flush/checkpoint capture or explicitly lagging persisted fallback. Associate
  those pins with independently versioned roots; exact split/transfer cutovers
  cannot silently use an older fallback.
- **Observation and performance evidence — medium, D12/A14.** Existing phase
  histograms and MemDisk/mem-block fixtures are usable. `observe_metadata` is a
  synchronous slice of the flat fence list, so it needs bounded indexed
  pagination rather than eagerly loading history. Add counters for write RPCs,
  local publication, index reuse, root publications, recovery, and actual WAL
  latency. Historical 7.44-ms measurements predate the first optimization and
  are not a fresh implementation baseline.

The two relatively isolated first gains are combined write/sync RPCs and compact
location arithmetic. The largest dependent work is the directory/retention
model and replacing the cursor authority across write, read, recovery, and
ownership. Basic parsing exists; its boundary, replica, and IO behavior need
adaptation rather than a new transport or storage engine.

## Decisions and execution dependencies

- Q1 selects verifiable head/follower groups with a 64 KiB total frame maximum.
  One append is one validity unit and one extent. Magic retains purpose/version;
  the common header has ordinary/head/follower roles. Heads declare length/count,
  followers identify head offset and ordinal, and complete groups validate count,
  order, lengths, CRC, and chunk identity. Single-frame appends are count-one
  heads. No partial append is exposed and no commit frame/RPC is added. Rust/C++
  share bytes and CRC coverage; only Stream emits grouped roles. User-data/tree
  frames remain ordinary under the common codec. No design blocker is identified;
  implementation must verify native buffers, geometry, and cross-language parity.
- Q2 selects the shortest complete-append prefix among healthy available
  mirrors after owner exclusion, within the original IO budget. Failed or
  corrupt observations are not artificial short candidates; conflicts fail
  explicitly, absence of a completed healthy candidate yields unavailable, and
  no arbitration is awaited.
- Q3 selects Group-0 authority with conservative local lease deadlines and
  admission/ACK checks. Explicit drained, durable release permits immediate
  new-grant activation; an unreachable owner requires safe expiry and old-IO
  exclusion. Renewal is background work, not per-append KV.
- Q4 selects owner-ordered flush/checkpoint when reachable, otherwise an
  explicitly lagging consistent persisted prefix. Capture returns its bound
  and source; a newer exact cutover cannot use a stale fallback. This fallback
  is not an upper bound for writer failover recovery.
- Versioned directory decoding and synchronized-wire rollout must be explicit.
  Adding a FlatBuffers field does not itself establish old-server support.
  Preserve existing data; do not use reset/reallocation as a migration strategy.
- T01 and compatibility-safe T05–T07 can precede the remaining ownership work.
  Common codec T08 and directory work T09–T13 follow compatibility task T04;
  T14–T19 establish the state, recovery, lifecycle, and view contracts.
  T20 integrates the new completion
  boundary only after these dependencies exist. T21–T26 provide consumer and
  production acceptance. No intermediate speed sample substitutes for them.

## Phase 0 — Establish evidence and settle contracts

- [~] **T01 — Capture a matched implementation baseline**: retain release
  identities, exact three-node fixture parameters, mirror count, checkpoints,
  queue/batch geometry, RSS, phase distributions, head/page counts, errors, and
  cleanup receipts for Stream and actual Chunk-KV. Use retained MemDisk and
  mem-block; do not compare against historical NullDisk figures as if matched.
  Reuse existing metrics before adding new ones. Maps to D12/A14.
  Files: `tools/benchmark/bench-chunk-stream-regression.sh`,
  `tools/benchmark/bench-chunk-kv-regression.sh`,
  `lib/crowdb-chunk-stream/benches/production_bounds/`.
- [x] **T02 — Record Q1/Q2 and framing failure assertions**: R244 and the design
  select verifiable bounded frame groups, whole-append validity, one extent per
  group, and the shortest healthy available complete-append prefix. A07/A15/A16
  require missing/reordered/truncated followers, bad CRC/identity, interrupted
  large appends at every boundary, failed observations, and conflicting mirrors
  to preserve the previous complete-append end or return an explicit error.
  Codec, recovery, fault tests, and performance remain implementation tasks;
  this check records decisions only. Files:
  `doc/backlog/R244-chunk-stream-incremental-directory-publication.md`,
  `doc/design/chunkds/design-crowdb-chunk-stream.md`.
- [x] **T03 — Record Q3/Q4 and capture/expiry ordering**: the design and R244
  select Group-0 lease/release/expiry, local admission/ACK checks, old-IO
  exclusion, owner flush/checkpoint capture, and explicitly lagging fallback.
  A10/A11/A13 record their assertions. Implementation remains in T18/T19/T21;
  this check does not claim tests or code are complete. Files:
  `doc/backlog/R244-chunk-stream-incremental-directory-publication.md`,
  `doc/design/chunkds/design-crowdb-chunk-stream.md`.
- [ ] **T04 — Define format and wire compatibility**: inventory old bincode
  manifests/pages, shared frames, and current wire consumers; define version
  detection and supported read/migration/rollout behavior. Finalize legacy Stream
  chunks at their persisted durable cursor before grouped writes, preserving
  acknowledged data without inventing group boundaries in uncommitted V1 bytes.
  New synchronized write success
  must prove peer support or use an explicitly safe compatibility path. Do not
  silently reinterpret old pages or accept an ignored sync flag. Conversion of
  retained directories, if required, is resumable with old roots retained until
  the new head is published; no full-history conversion on every open. Files:
  `lib/crowdb-protocol/src/chunk_stream.rs`,
  `lib/crowdb-protocol/src/key/chunk_stream.rs`,
  `lib/crowdb-protocol/src/frame.rs`,
  `lib/crowdb-protocol/cpp/include/crowdb-protocol/frame.h`,
  `lib/crowdb-protocol/src/fbs/diskio.fbs`,
  `lib/crowdb-chunk-stream/src/kv.rs`,
  `lib/crowdb-diskio-client/src/client.rs`.

## Phase 1 — Combine write and synchronization

- [ ] **T05 — Add the synchronized wire contract**: append the sync option
  without reusing retired slots; update contiguous and view wire constructors
  and protocol tests. Exercise both generated Rust and C++ schemas through
  their existing generators; do not hand-edit generated files. Apply T04's
  compatibility rule. Maps to D02/A02. Files:
  `lib/crowdb-protocol/src/fbs/diskio.fbs`,
  `lib/crowdb-protocol/build.rs`, `lib/crowdb-rpc/CMakeLists.txt`,
  `lib/crowdb-diskio-client/src/client.rs`,
  `lib/crowdb-protocol/tests/diskio_wire_test.rs` (new).
- [ ] **T06 — Complete write/flush through one server response**: make
  `DiskioServer::handle_write` await ordered write and, when requested, flush
  before its sole reply. Retain buffer/disk/connection lifetimes through both
  callbacks. Short/failed writes and failed flushes cannot produce success or
  double completion. Keep buffered behavior and independent fsync. Maps to
  D02/A02. Files: `app/crowdb-diskio/src/rpc/dio_server.cpp`,
  `app/crowdb-diskio/src/rpc/dio_server.h`,
  `app/crowdb-diskio/tests/dio_server_write_test.cpp` (new),
  `app/crowdb-diskio/CMakeLists.txt`.
- [ ] **T07 — Propagate synchronization through mirror writes and repair**:
  replace the client write-reply/fsync-reply sequence for supported peers.
  Preserve bounded view splitting and same-strip ordering; flush coverage must
  include preceding fragments before append completion. Extend the `DiskWriter`
  seam and update ordinary and replacement writes in `MirrorStripFlow`, not
  just the nonfailure path. Audit every `DiskWriter` implementation, especially
  `MetricsDiskWriter`, so an adapter cannot drop the sync option or restore the
  second RPC. Update affected test seams if trait signatures change. Assert one
  synchronized RPC per replica through the production adapter chain for an
  unsplit single-strip write. Maps to D02/A02/A03. Files:
  `lib/crowdb-diskio-client/src/semantic.rs`,
  `lib/crowdb-diskio-client/src/semantic/write_views.rs`,
  `lib/crowdb-chunk-client/src/client.rs`,
  `lib/crowdb-chunk-client/src/disk_io/disk_writer.rs`,
  `lib/crowdb-chunk-client/src/disk_io/routing.rs`,
  `lib/crowdb-chunk-client/src/chunk/mirror_flow.rs`,
  `lib/crowdb-chunk-client/src/chunk/mirror_flow/repair.rs`,
  `lib/crowdb-diskio-client/tests/synchronized_write_test.rs` (new),
  `lib/crowdb-chunk-client/tests/mirror_strip_writer_test.rs`,
  `lib/crowdb-chunk-client/tests/common/mod.rs`, and affected `DiskWriter`
  test implementations found by repository search.

## Phase 2 — Unify framing and build compact directories

- [ ] **T08 — Implement the selected common frame revision**: update the Rust
  and C++ physical codecs together, with shared golden vectors, identical CRC
  coverage, versioned legacy handling, checked sizes, and geometry helpers.
  Implement T02's Stream head/follower roles, head length/count, follower head
  offset/ordinal, and group validation; preserve count-one heads. CRC covers
  header/group fields/payload, with separate expected chunk-ID validation.
  User-data and tree consumers emit ordinary frames; retain whole-object
  publication and unknown-length streaming. Preserve native receive-buffer
  ownership, incremental CRC, bounded IO, and scatter/gather;
  do not require an allocation of the entire large append. Update every common
  frame consumer and native payload capacity. Tree scope is its common chunk
  codec, not unrelated internal tree-node layouts. Maps to D01/A01/A07/A15.
  Files: `lib/crowdb-protocol/src/frame.rs`,
  `lib/crowdb-protocol/cpp/include/crowdb-protocol/frame.h`,
  `lib/crowdb-protocol/tests/frame_test.rs`,
  `lib/crowdb-chunk-client/src/writer/large_object.rs`,
  `lib/crowdb-chunk-client/src/writer/large_async_object.rs`,
  `lib/crowdb-chunk-client/src/writer/small_pipeline/batch.rs`,
  `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`,
  `lib/crowdb-access-s3/src/native_buffer.rs`,
  `lib/crowdb-access-s3/src/native_buffer/allocator.rs`,
  `lib/crowdb-access-s3/src/streaming.rs`,
  `lib/crowdb-access-iceberg/src/file/blocks.rs`,
  `lib/crowdb-tree/src/backend/chunk/chunk_pack_pipeline.cpp`,
  `lib/crowdb-tree/src/backend/chunk/chunk_page_store.cpp`, and their existing
  frame/native-buffer/object/tree test suites.

- [ ] **T09 — Introduce versioned directory records**: replace the flat head
  fence list with a bounded root and independent page references. Represent
  multi-frame extents with logical intervals and `ChunkLocation` geometry;
  keep page identity independent of owner epoch/head generation. Implement
  T04's decoder/migration rules and purpose/identity checks. Maps to D01/D07,
  A01/A08/A09. Files: `lib/crowdb-protocol/src/chunk_stream.rs`,
  `lib/crowdb-protocol/src/key/chunk_stream.rs`,
  `lib/crowdb-protocol/tests/frame_test.rs`,
  `lib/crowdb-protocol/tests/stream_directory_test.rs` (new).
- [ ] **T10 — Build bounded tail locations**: replace one `Extent` per frame
  and the worker's whole-history builder with tail pages and necessary paths.
  Reuse canonical framing arithmetic, retain short-frame boundaries, and never
  clone the full active/history list to publish an in-memory append view. Maps
  to D01/D07, A01/A08. Files: `lib/crowdb-chunk-stream/src/metadata.rs`,
  `lib/crowdb-chunk-stream/src/stream.rs`,
  `lib/crowdb-chunk-stream/src/stream/append.rs`,
  `lib/crowdb-chunk-stream/src/directory.rs` (new),
  `lib/crowdb-chunk-stream/tests/metadata_test.rs`.
- [ ] **T11 — Publish only changed directory paths**: replace
  `build_extent_pages` over all retained extents with an incremental publication
  batch. Persist new pages before root CAS, retain the prior root on failure,
  reuse CAS revisions, and reconcile unknown outcomes using the same candidate
  identities. Protect in-flight candidate pages. A large seal may flush several
  new segments, without retagging completed history. Maps to D07/D08/A08/A09.
  Files: `lib/crowdb-chunk-stream/src/storage.rs`,
  `lib/crowdb-chunk-stream/src/kv.rs`,
  `lib/crowdb-chunk-stream/src/kv/pages.rs`,
  `lib/crowdb-chunk-stream/src/stream/publication.rs`,
  `lib/crowdb-chunk-stream/tests/kv_metadata_test.rs`.
- [ ] **T12 — Load and read the index lazily**: remove eager
  `load_extent_pages`/`collect_extents` from open. Traverse only required paths,
  cache pages by independent identity within an explicit budget, and expand
  compact locations into bounded verified frame ranges. Batch adjacent reads
  and reuse a validated layout view rather than querying metadata per frame.
  Maps to D01/D07/D10, A01/A08/A11. Files:
  `lib/crowdb-chunk-stream/src/stream.rs`,
  `lib/crowdb-chunk-stream/src/stream/read.rs`,
  `lib/crowdb-chunk-stream/src/extent_cache.rs`,
  `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`,
  `lib/crowdb-chunk-stream/tests/read_publication_test.rs`.
- [ ] **T13 — Reclaim by roots and whole-chunk retention**: associate consumer
  generation horizons with retained root references; protect current, pinned,
  and in-flight roots/pages. Replace generation-only page deletion. Trim cannot
  free a chunk while another extent or consumer still needs it. Bound cleanup
  scans/deletes and make restart retryable. Maps to D11/A12. Files:
  `lib/crowdb-chunk-stream/src/storage.rs`,
  `lib/crowdb-chunk-stream/src/kv.rs`,
  `lib/crowdb-chunk-stream/src/stream/worker.rs`,
  `lib/crowdb-chunk-stream/src/memory.rs`,
  `lib/crowdb-chunk-stream/tests/stream_test.rs`,
  `lib/crowdb-chunk-stream/tests/kv_metadata_test.rs`.

## Phase 3 — Establish content-derived progress before changing completion

- [ ] **T14 — Separate synchronized progress from `chunkinfo` refresh**: give
  worker/local write progress an explicit owner independent of the latest
  persisted cursor. Growth, repair, query, and liveness refresh must preserve
  synchronized local progress and repair images. Remove Stream publication
  caused solely by internal layout growth. Keep other chunk consumers' cursor
  APIs intact where they retain their own contract. Maps to D03/D04/A03/A04.
  Files: `lib/crowdb-chunk-stream/src/production_chunk.rs`,
  `lib/crowdb-chunk-stream/src/storage.rs`,
  `lib/crowdb-chunk-stream/src/stream/capacity.rs`,
  `lib/crowdb-chunk-stream/src/memory.rs`,
  `lib/crowdb-chunk-stream/tests/production_chunk_test.rs`,
  `lib/crowdb-chunk-stream/tests/rollover_growth_test.rs`.
- [ ] **T15 — Implement bounded tail observation and scanning**: establish a
  chunk-layer read mode/bound that can inspect the last allocated chunk beyond
  an older checkpoint without globally widening ordinary object reads. Read
  bounded windows, parse multiple frames locally, and carry frames across read
  and strip boundaries. Track tentative frame progress separately from committed
  group progress; never expose a partial append or skip its hole. Apply T02's
  complete-append boundary and Q2's shortest
  healthy available prefix; exclude mid-scan failures and corruption rather
  than inventing a short cursor. Bound the entire concurrent observation by
  one IO budget, reject conflicting overlap, and return unavailable with no
  candidate. Cover unwritten tail and exact capacity end; return physical
  cursor, logical length, and compact locations. Share suitable scan primitives
  with the finalizer without making
  the Stream library depend on the ChunkDB application. Maps to D06/A06/A07.
  Files: `lib/crowdb-chunk-stream/src/stream/recovery.rs`,
  `lib/crowdb-chunk-stream/src/production_chunk.rs`,
  `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`,
  `lib/crowdb-chunk-client/src/chunk/strip_reader.rs`,
  `lib/crowdb-chunk-client/src/chunk/frame_scan.rs` (new if shared),
  `lib/crowdb-chunk-stream/tests/durable_cursor_recovery_test.rs`.
- [ ] **T16 — Align abandoned-chunk finalization with recovery**: audit both
  `FinalizeChunkTaskHandler` and `seal_expired_writer_chunks`. Expiry must not
  finalize an old checkpoint as the complete length or make a later scanner
  skip valid data. Apply the agreed ownership/write-age boundary before scan
  and seal; do not delete a Stream chunk merely because its persisted cursor
  is zero or the first replica's frame is unreadable. Preserve chunk-layer
  isolation and existing non-Stream consumers. Maps to D05/D06/D09, A05/A07/A10.
  Files: `app/crowdb-chunkdb/src/finalize.rs`,
  `app/crowdb-chunkdb/src/lifecycle/handler.rs`,
  `app/crowdb-chunkdb/src/lifecycle/handler/seal.rs`,
  `app/crowdb-chunkdb/src/task/store.rs`,
  `app/crowdb-chunkdb/tests/finalization_test.rs`.
- [ ] **T17 — Make lifecycle transitions restartable**: attach new allocations
  before data admission; at rollover finalize the current physical/logical
  bounds and directory tail before successor writes. Update writer open,
  close, and externally sealed rotation to recover only the last referenced
  chunk and tolerate each allocation/seal/head-CAS crash window. Maps to
  D05/D06/D08, A05/A06/A09. Files: `lib/crowdb-chunk-stream/src/stream.rs`,
  `lib/crowdb-chunk-stream/src/stream/capacity.rs`,
  `lib/crowdb-chunk-stream/src/stream/worker.rs`,
  `lib/crowdb-chunk-stream/src/stream/publication.rs`,
  `lib/crowdb-chunk-stream/tests/rollover_growth_test.rs`,
  `lib/crowdb-chunk-stream/tests/stream_test.rs`.
- [ ] **T18 — Enforce the selected Group-0 writer lease**: implement Q3 for busy
  workers, long IO, paused/resumed owners, renewal failure, delayed old writes,
  and takeover. Acknowledgment requires current authority even when all writes
  succeeded; no per-append KV fallback. Compose serving grants and Stream owner
  identity/epoch with conservative monotonic deadlines and background renewal.
  Supply an owner-authority snapshot through the injected runtime for standalone
  streams as well as Chunk-KV; do not make the library depend on server code.
  Explicit transfer stops admission, drains writes, checkpoints, and durably
  releases authority before the new grant starts immediately. Unreachable
  owners require safe expiry and old-write exclusion before tail scan. Reuse
  existing lifecycle protections without adding a hot-path lock or an unapproved
  unsafe scope. Maps to
  D09/A10. Files: `lib/crowdb-chunk-stream/src/stream/liveness.rs`,
  `lib/crowdb-chunk-stream/src/stream/worker.rs`,
  `lib/crowdb-chunk-stream/src/production_chunk.rs`,
  `lib/crowdb-chunk-stream/tests/idle_authority_test.rs`,
  `app/crowdb-chunk-kv-server/src/serving/lease.rs`,
  `app/crowdb-chunk-kv-server/src/serving/worker.rs`,
  `app/crowdb-chunk-kv-server/src/serving/transfer.rs`,
  `app/crowdb-chunk-kv-server/src/control_store.rs`.
- [ ] **T19 — Capture owner-flushed or checkpoint fallback views**: implement
  Q4 for exact-epoch `open_read_only` and current-epoch opens. A reachable owner
  handles an ordered barrier, persists a complete-group ChunkDB cursor, then
  publishes the matching directory checkpoint before returning epoch/chunk/end.
  If publication fails after the cursor persists, retain the prior published
  checkpoint; do not combine mismatched roots/epochs/cursors. If unreachable,
  capture the consistent persisted ChunkDB prefix with its matching root,
  explicit source, and potentially older end. Reject a fallback below an exact
  requested cutover. Capture once, pin required root/data state, and preserve
  EOF through writes/rollover. Readers never adopt, seal, allocate, or publish
  writer state themselves; owner-triggered checkpoint is allowed. Maps to
  D10/A11. Files:
  `lib/crowdb-chunk-stream/src/stream.rs`,
  `lib/crowdb-chunk-stream/src/production.rs`,
  `lib/crowdb-chunk-stream/src/stream/read.rs`,
  `lib/crowdb-chunk-stream/tests/read_publication_test.rs`,
  `app/crowdb-chunk-kv-server/src/storage.rs`.
- [ ] **T20 — Integrate mirror-completion append**: after T05–T19 dependencies
  pass, replace `resolve_cursor_advance` and per-batch `publish_state` with
  synchronized-replica completion and bounded in-memory publication. Keep
  admission, exact ranges, queue accounting, ambiguous outcomes, and watchdog
  ownership intact. Optional checkpoints use time/byte budgets and are disabled
  for the strict append sentinel. Maps to D03/D08, A03/A04/A05/A06. Files:
  `lib/crowdb-chunk-stream/src/stream/append.rs`,
  `lib/crowdb-chunk-stream/src/stream/worker.rs`,
  `lib/crowdb-chunk-stream/src/storage.rs`,
  `lib/crowdb-chunk-stream/src/metrics.rs`,
  `lib/crowdb-chunk-stream/tests/stream_test.rs`.

## Phase 4 — Integrate consumers, observations, and faults

- [ ] **T21 — Preserve actual WAL/retry/transfer semantics**: adapt
  `StreamPartitionJournal`, checkpoint generation/root retention, replay,
  checkpoint trim, split, and transfer to T02/T03 and independent page roots.
  Exact final split/transfer cutovers cannot silently accept a lagging fallback.
  Keep consumer framing/retry results separate from physical Stream framing.
  Generation no longer implies a complete new copy of all pages. Maps to
  D10/D11/A11/A12/A13. Files:
  `lib/crowdb-chunk-kv/src/partition/journal.rs`,
  `lib/crowdb-chunk-kv/src/partition.rs`,
  `lib/crowdb-chunk-kv/src/partition/split/`,
  `lib/crowdb-chunk-kv/src/partition/transfer.rs`,
  `app/crowdb-chunk-kv-server/src/storage.rs`,
  `app/crowdb-chunk-kv-server/src/storage/split_recovery.rs`.
- [ ] **T22 — Keep inspection bounded under the index**: replace synchronous
  flat-list slicing in `observe_metadata` with the selected bounded indexed
  observation/pagination contract and consistent generation/root tokens. Update
  journal/server adapters without preloading all history to retain an old
  synchronous signature. Preserve exact numeric encoding and finite results.
  Maps to D07/D12/A08. Files:
  `lib/crowdb-chunk-stream/src/stream/observation.rs`,
  `lib/crowdb-chunk-kv/src/partition/observation.rs`,
  `lib/crowdb-chunk-kv/src/partition/journal.rs`,
  `app/crowdb-chunk-kv-server/src/server/observation/storage.rs`,
  `lib/crowdb-chunk-stream/tests/observation_test.rs`,
  `app/crowdb-chunk-kv-server/tests/partition_observation_test.rs`.
- [ ] **T23 — Attribute the new path**: retain periodic and cumulative
  count/avg/p50/p99/max, and add synchronized-RPC, local-publication, root/page
  reuse/write, scan/query, replica repair, retention, and cleanup counters.
  Separate `chunkinfo` changes from Stream publications and actual WAL/client
  latency from standalone append. Replace the fixed three-copy physical-byte
  multiplier with configured/actual replica accounting and separate repair
  amplification. Maps to D12/A14. Files:
  `lib/crowdb-chunk-stream/src/perf.rs`,
  `lib/crowdb-chunk-stream/src/metrics.rs`,
  `app/crowdb-chunk-kv-server/src/metrics.rs`,
  `lib/crowdb-chunk-stream/benches/production_bounds/result.rs`,
  `lib/crowdb-chunk-stream/tests/perf_metrics_test.rs`,
  `app/crowdb-chunk-kv-server/tests/perf_log_test.rs`.
- [ ] **T24 — Complete the deterministic failure matrix**: cover A01–A16 with
  exact expected outcomes from T02/T03. Replace old cursor-authority assertions
  rather than deleting their crash coverage. Exercise 1/16/256 directory
  segments, frame/read-window/strip boundaries, one retained extent in a trimmed
  chunk, shared roots, concurrent GC/publication, stale checkpoints, and process
  takeover. Run real production adapters for cursor/read/finalizer coupling;
  an in-memory Stream seam alone cannot prove those cases. Files: Stream,
  chunk-client, DiskIO-client, ChunkDB, and server test files listed below.

## Phase 5 — Verify production behavior and performance

- [ ] **T25 — Run retained-byte production acceptance**: verify service
  restart, mirror failure, late old-writer IO, read-only cutovers, real WAL replay,
  retry preservation, checkpoint/trim, split, and transfer. Retain failed case
  names and complete service logs; use `CROWDB_E2E_PRINT_SERVICE_ERRORS=1` when
  debugging. MemDisk process tests retain its media; media-loss tests require
  backing that survives the tested fault. Maps to A05/A10/A11/A12/A13. Files:
  `e2e-test/tests/chunk_stream.rs`, `e2e-test/tests/chunk_kv.rs`,
  `e2e-test/tests/chunk_kv/recovery.rs` and existing split/transfer suites.
  Run affected ordinary user-data/native-buffer/tree paths for A15 as well.
- [ ] **T26 — Measure matched performance and growth**: repeat T01 fixtures
  with identical counts and release configuration. Enforce A14's existing gates
  plus zero ordinary append KV work, one synchronized RPC per replica for an
  unsplit single-strip batch, compact large-payload metadata, bounded index/open
  work, and one-chunk recovery. Sweep retained history and separate cold recovery
  from steady-state append; report phase distributions and actual WAL/client
  latency. Keep compilation outside measurement and verified cleanup receipts.
  Files: `tools/benchmark/bench-chunk-stream-regression.sh`,
  `tools/benchmark/bench-chunk-kv-regression.sh`,
  `lib/crowdb-chunk-stream/benches/production_bounds/`.
- [ ] **T27 — Complete gates and reconcile documentation**: run affected
  tests, Rust fmt/clippy, and changed C++ format/tree-lint; update Stream,
  DiskIO, and chunk-IO documentation to the verified result. Record evidence
  against every R244 acceptance case. Requirement/plan/backlog cleanup happens
  only after all required behavior and performance acceptance pass, following
  `/implement-requirement`; planning itself creates no implementation commit.
  Files: upstream design/requirement, this plan, `doc/backlog/backlog.md`,
  `doc/design/diskio/design-crowdb-diskio.md`,
  `doc/design/chunkio/design-crowdb-chunkio.md`.

## Consolidated file scope

The task entries above are the authoritative per-hunk list. Consolidated areas:

- Protocol: `lib/crowdb-protocol/src/{chunk_stream.rs,frame.rs}`,
  `src/key/chunk_stream.rs`, `src/fbs/diskio.fbs`, `build.rs`, and frame/directory/
  wire tests; the shared C++ frame header and common tree chunk consumers are
  included. C++ schema generation remains in `lib/crowdb-rpc/CMakeLists.txt`.
- DiskIO: `app/crowdb-diskio/src/rpc/dio_server.{cpp,h}`, its test registration,
  and ordered-write/flush server tests; `lib/crowdb-diskio-client/src/client.rs`,
  `semantic.rs`, and `semantic/write_views.rs`, plus synchronized-wire tests.
- Chunk IO: `lib/crowdb-chunk-client/src/disk_io/{disk_writer,routing}.rs`,
  `src/client.rs` (`MetricsDiskWriter` forwarding),
  `src/chunk/{mirror_flow,chunk_reader,strip_reader}.rs`,
  `src/chunk/mirror_flow/repair.rs`, and a shared scan helper only if extraction
  is useful; existing mirror/reader tests remain covered.
- Stream: `lib/crowdb-chunk-stream/src/{storage,metadata,kv,memory,production,
  production_chunk,extent_cache,metrics,perf}.rs`, `src/kv/pages.rs`,
  `src/stream.rs`, `src/stream/`, the new directory module, integration tests,
  and production benchmark modules. Update `src/lib.rs` exports as APIs change.
- ChunkDB lifecycle: `app/crowdb-chunkdb/src/{finalize.rs,lifecycle/handler.rs}`,
  `src/lifecycle/handler/seal.rs`, `src/task/store.rs`, and finalization tests.
- Consumers: `lib/crowdb-chunk-kv/src/partition{.rs,/journal.rs,/observation.rs,
  /split/,/transfer.rs}`, server storage/observation/metrics adapters and their
  tests, production E2E files, and the two regression scripts.
- Common framing consumers: user-data small/large and async-native writers,
  S3 native receive buffers and streaming, Iceberg block capacity, common chunk
  reads, and C++ tree chunk/page storage as enumerated in T08. Stream-only
  completeness semantics do not change whole-object publication behavior.
- Owner authority: server `serving/{lease,worker,transfer}.rs` and
  `control_store.rs`, composed with Stream liveness and completion.
- New files named in tasks are proposed locations, not files claimed to exist.
  Exact byte packing may refine the scope; record a concrete blocker in R244
  before changing the selected grouped framing contract.

## Verification groups and completion evidence

- Unit/pure-contract coverage: canonical frame/location arithmetic, independent
  page identities, version rejection, wire sync encoding, checked limits, and
  directory lookup/build transitions. Use crate `tests/` for new Rust tests;
  primary files are protocol `frame_test.rs`, proposed `diskio_wire_test.rs`
  and `stream_directory_test.rs`, and Stream `metadata_test.rs`.
  Shared-format golden vectors cross Rust/C++ and every purpose, including
  CRC coverage, incomplete/corrupt input, legacy bytes, and length bounds.
- Integration coverage: synchronized server response lifetime/errors, mirror
  repair, production local-cursor refresh, bounded scans, CAS ambiguity,
  lazy directory/cache, retention/GC, ownership, and finite views. Keep Stream's
  self `test-util` dev-dependency and equivalent crate-local test seams.
- E2E coverage: actual synchronized RPCs, retained bytes over restarts, real
  Chunk-KV retry/replay/split/transfer, and matched memory-backed performance.
- Store implementation receipts under an owned
  `.crowdb-runtime/artifacts/r244-chunk-stream/` run directory. Each receipt names
  binary/config identities, command, exit status, case errors, metrics, and
  cleanup. No new runtime artifacts have been created for this planning audit.
- Ordinary commands use a 60-second shell timeout. Start long/hang-prone build,
  E2E, and benchmark commands in the background, retain complete output, and
  poll; command completion and cleanup, not a timeout extension, establish pass.

Commands for implementation, not commands executed by this documentation update:

```bash
pixi run clean-env
pixi run cargo test -p crowdb-protocol -p crowdb-chunk-stream -p crowdb-chunk-client -p crowdb-diskio-client -p crowdb-chunkdb -p crowdb-chunk-kv -p crowdb-chunk-kv-server --all-targets
pixi run cargo test -p crowdb-common --lib metrics::
pixi run build-cpp
pixi run ctest --test-dir app/crowdb-diskio/build --output-on-failure
pixi run test-e2e-chunk-stream
pixi run test-e2e-chunk-kv
pixi run clean-env
pixi run bash tools/benchmark/bench-chunk-stream-regression.sh
pixi run clean-env
pixi run bash tools/benchmark/bench-chunk-kv-regression.sh
pixi run rs-fmt-check
pixi run clang-format --dry-run --Werror app/crowdb-diskio/src/rpc/dio_server.cpp app/crowdb-diskio/src/rpc/dio_server.h
pixi run tree-lint
pixi run cargo clippy -p crowdb-protocol -p crowdb-common -p crowdb-chunk-stream -p crowdb-chunk-client -p crowdb-diskio-client -p crowdb-chunkdb -p crowdb-chunk-kv -p crowdb-chunk-kv-server --all-targets -- -D warnings
```

Run focused groups per changed task before the final combined gates. Discover
the exact available tasks before implementation, and do not run `clean-env` or
shared service teardown during this read-only planning audit.
If `DiskWriter` signature changes affect the access-server test adapter, also
run `pixi run cargo check -p crowdb-access-server --tests` before declaring the
seam migration complete.

The common frame revision also requires consumer/native-buffer and C++ codec
coverage. Include every changed C++ frame consumer in format checks:

```bash
pixi run cargo test -p crowdb-access-s3 -p crowdb-access-iceberg -p crowdb-access-server -p crowdb-tree-ffi --all-targets
pixi run ctest --test-dir lib/crowdb-tree/build --output-on-failure
pixi run test-e2e-s3
pixi run test-e2e-iceberg
pixi run clang-format --dry-run --Werror lib/crowdb-protocol/cpp/include/crowdb-protocol/frame.h lib/crowdb-tree/src/backend/chunk/chunk_pack_pipeline.cpp lib/crowdb-tree/src/backend/chunk/chunk_page_store.cpp
pixi run cargo clippy -p crowdb-access-s3 -p crowdb-access-iceberg -p crowdb-access-server -p crowdb-tree-ffi --all-targets -- -D warnings
```
