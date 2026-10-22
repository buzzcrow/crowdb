<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R244: chunk-stream/chunk-io/chunk-kv — Shared framing, mirrored append performance, and segmented directories

#### Problem

The dedicated performance design discussion resumed on 2026-10-22. The
[Chunk-Stream design](../design/chunkds/design-crowdb-chunk-stream.md) records
the agreed contract; this requirement records differences from the previous
design and the remaining human decisions. The implementation has not been
changed by this design update, and existing failing performance assertions
remain unmet until measured acceptance passes.

The 2026-10-23 code audit, implementation gaps, dependency order, file scope,
and execution tasks are in the
[performance plan](../working/plan-chunk-stream-performance.md).

The 2026-10-23 discussion selects Q1's verifiable bounded head/follower groups,
Q2's shortest healthy available-mirror complete-append prefix, Q3's Group-0
owner lease with explicit release or expiry, and Q4's owner flush with
persisted-ChunkDB fallback. A large payload is one indivisible append; partial
frame groups are never valid Stream data. All consumers share one physical
format and codecs, while only Stream enables grouped roles. User data retains
whole-object publication/failure semantics. Implementation remains outstanding.

The implemented normal append path encodes physical frames, writes and fsyncs
mirror replicas through separate RPCs, advances the ChunkDB durable cursor,
rebuilds all retained extent pages, writes them under a fresh generation, and
CAS-publishes the manifest before returning success:

- `write_batch` adds one `Extent` per frame, even for a single large payload
  whose canonical frames can be addressed with one `ChunkLocation`.
- `MirrorStripFlow` waits for replica writes and then submits separate fsync
  requests. `DiskioClient` also implements `Durability::Fsync` with a second
  wire request; a high-level durability argument alone does not remove the RTT.
- `advance_chunk_write` is a ChunkDB RPC and durable metadata transition for
  every batch. Its persisted cursor also bounds the existing recovery parser.
- `publish_state` rebuilds all retained extents. The KV adapter writes every
  page under the new epoch/generation before head CAS. Old contents are copied
  into new keys, so fixed-size appends accumulate growing historical work.
- The head contains all page fences, and writer open loads all referenced
  extent pages. Reducing page writes alone leaves unbounded head/open work.
- Chunk-internal growth also calls Stream publication. This leaks independently
  persisted physical `chunkinfo` changes into logical Stream metadata.
- `StreamPartitionJournal` and `ChunkKvStorage` inherit these costs. Standalone
  business-Stream measurements do not establish actual WAL/client latency.

The 2026-10-22 pre-optimization 4 KiB single-producer NullDisk sample averaged
7.44 ms: mirror operations 1.02 ms, cursor resolution 2.51 ms, directory
publication 3.82 ms; directory CPU build was only 12 us. Revision reuse and
eight-way page publication improved the 32-producer NullDisk sample from 1017
to 1838 ops/s, but 1154 publications still wrote 10933 pages. MemDisk showed
the same general bottleneck. These are historical measurements, not results
for the design in this requirement.

The completed deployment investigation retains phase distributions and cleanup
receipts in its archived acceptance artifacts; T01 repeats matched measurements
for this implementation. The current regression
script selects `mem-block` KV/WAL and retained MemDisk bytes. Memory-backed
media does not explain away RPC, consensus, or scheduling waits.

#### Solution

The following items are the design-difference ledger and required work. Each
names the old behavior, agreed replacement, and affected design sections.
Invariants I1–I10 are defined in the linked design. Unsettled choices are listed
under Open Questions instead of being selected here.

1. **D01 — Compact large-payload extents; design sections 1, 3, 4, 6.**
   Replace per-frame `Extent` generation in `write_batch`, the protocol directory
   representation, `resolve_extent`, and Stream reads with one compact location
   per append group. `ChunkLocation` already supplies framed-length and logical-
   subrange arithmetic; extend it for grouped geometry with checked lengths. A 4 MiB payload occupies 65
   frames with current geometry but needs one location. Correct the old design's
   raw-byte mapping and extra chunk-ID payload-trailer descriptions: Stream
   frames own physical framing/provenance and returned lengths count caller bytes.
   Implement Q1's selected common revision across all frame producers/consumers,
   including user-data small/large writers, native buffers, and C++ tree codecs.
   Retain a 64 KiB physical-frame maximum; magic identifies purpose/version and
   a common role field distinguishes ordinary/head/follower. Only Stream groups
   frames: head total length/count, follower head-offset association and ordinal,
   per-frame validation, and total-length validation establish one complete
   append. Single-frame Stream appends are count-one heads. No standalone commit
   frame/RPC is added. One incomplete group cannot advance any visible cursor,
   extent, checkpoint, or recovered logical tail. User-data uploads use ordinary
   frames and whole-object publication. V1 capacity is not the new role-dependent
   capacity. The current Rust/C++ CRC input ranges differ; unify the new revision
   on CRC32C of header/group fields/payload, with separate expected chunk-ID
   validation. Cross-language vectors establish the one supported format.
   Invariants I6, I7.

   Q1 considered a single variable-size frame per append. Bounded verifiable
   groups are selected because they retain bounded checksum/read units and native
   buffer ownership while making the whole append valid or invalid. Existing
   batching already aggregates frames into writes; the 34-byte V1 frame overhead
   is about 0.052%, so a large frame alone does not remove RPC waits. Groups add
   neither per-frame extents nor an extra commit RPC. If implementation discovers
   a concrete blocker, record its evidence and revisit that blocker explicitly.

2. **D02 — One synchronized write RPC; design sections 2, 4, 8.**
   Extend the `FBDiskWriteRequest` contract with a boolean sync option. Propagate
   it through `DiskWriter`, `RoutedDiskWriter`, `DiskioClient`, `MirrorStripFlow`,
   and `DiskioServer`. The server performs ordered write then flush and returns
   one success only after both. Buffered writes omit the flush; explicit fsync
   remains available for independent callers. Preserve same-strip ordering,
   bounded transport splitting, immutable-byte retries, error propagation, and
   replica repair. Strict wire validation and matching DiskIO/chunk-IO documentation
   belong to implementation, not a claim that the wire already supports it.
   Invariants I1, I9, I10.

3. **D03 — Data persistence completes append; design sections 2, 4, 5, 9.**
   Replace per-batch `advance_cursor`/`advance_chunk_write` as an acknowledgment
   prerequisite. Wait for synchronized completion on the configured required
   replica set, update the worker's in-memory cursor/tail/mappings, and complete
   requests. Three-copy configuration waits for three replicas; the current
   Chunk-KV default is two. Read-time availability does not reduce required
   write protection. Remove ordinary append directory/head writes and do
   not infer absence from an older persisted cursor. Preserve ordered ranges
   and explicit uncertain outcomes. This supersedes the previous design and
   previous R244 contract requiring every batch to commit a fenced ChunkDB
   cursor. Invariants I1, I3, I6.

4. **D04 — Isolate `chunkinfo`; design sections 2, 3, 5.**
   Chunk IO/ChunkDB keep persisting their own internal growth, replica, layout,
   and repair transitions. `StreamChunkStore`, `ProductionStreamChunkStore`, and
   capacity handling must not turn those changes into Stream manifest updates.
   Logical references use stable chunk IDs. Current capacity/layout comes from
   the chunk layer; cached checkpoint capacity cannot reject a valid newer
   layout. A layout refresh must not rewind synchronized in-memory progress to
   an older persisted cursor. Remove coupling, not required chunk-internal
   persistence. Invariant I8.

5. **D05 — Publish chunk attachment before writing; design sections 3, 5.**
   Initial allocation and the Stream reference/logical start are durable before
   admitting bytes. At rollover, finish and seal the current chunk, persist its
   complete logical mappings in the tail segment, then attach a successor with
   a fenced head publication before writing it. Only the current chunk is sealed;
   historical chunks and completed directory segments are unchanged. Interrupted
   seal/attach is restartable; an unattached allocation cannot contain
   acknowledged Stream bytes. Invariants I1, I3, I4.

6. **D06 — Recover only the last chunk from content; design sections 3, 5, 9.**
   Change `recover_active_frames` and writer open so the latest cursor is the
   end of a validated contiguous complete-append prefix, not a bound supplied
   by the last per-batch ChunkDB cursor. Scan from the last referenced chunk's
   beginning or a validated synchronized checkpoint. Rebuild logical length and compact
   locations with bounded physical reads. Earlier sealed chunks already have
   final mappings; open does not read their content. An interrupted seal may
   require scanning the one last chunk even if its chunk state is already
   sealed. Wrong-chunk data never extends the tail and retained corruption
   requires repair or an error. Under selected Q1, every missing/truncated group
   stops at the previous complete append; valid interior frames never become a
   partial append, and later groups cannot skip a hole. Under selected Q2, after
   owner exclusion observe healthy available mirrors with bounded IO and use
   their shortest complete-append prefix. Failed or partial observations are not
   short candidates; do not wait for external arbitration, and fail conflicting
   valid contents.
   Old tests excluding every byte beyond a persisted cursor must change with
   the new authority. Chunk-layer read availability, expired-writer
   sealing, and background finalization must honor the same recovered boundary;
   a stale cursor cannot authorize shortening or deleting retained data.
   Invariants I1, I4, I7.

7. **D07 — Segment history and update the tail; design sections 3, 5, 6.**
   Replace generation-bound page identities and the flat fence list in
   `StreamManifest`, `StreamExtentPage`, KV keys/adapters, and publication with
   independently versioned pages and a bounded-fanout directory index. Append
   mappings live in memory; seal/checkpoint changes the current tail segment
   and necessary index paths. Full segments remain immutable. Trim separately
   changes the retained prefix. Epoch adoption does not retag/copy history.
   Opening and cache retention are lazy and bounded; reducing writes while
   still reading or serializing the entire directory is insufficient. In-memory
   tail snapshots must not copy the full active/history extent list per append.
   A seal/checkpoint can flush several new segments without rewriting completed
   history.
   Invariants I2, I7, I10.

8. **D08 — Lifecycle/checkpoint publication; design sections 3, 5, 8.**
   Persist fresh referenced pages before head CAS and preserve revision reuse,
   immutable-page conflict checks, ambiguous-head reconciliation, purpose, and
   stale-publisher fences. Initial attachment, rollover, trim, close, and owner
   changes publish Stream state. Optional time/byte checkpoints only reduce
   recovery work and are not append commit points. Do not trigger a checkpoint
   per batch or rewrite historical segments. Disable optional checkpoints in
   the strict append sentinel; retain initial-attachment-plus-rollover head
   publication bounds during its measured workload. Invariants I1, I2, I3.

9. **D09 — Ownership without cursor-based fencing; design sections 1, 5, 9.**
   Keep epoch and ownership invariants, and never reuse the previous process's
   active chunk for new appends. Replace claims that a cursor query/advance
   establishes the fence with the selected Q3 Group-0 lease contract. Use
   conservative local deadlines, background renewal, and authority checks at
   admission/acknowledgment. Explicit transfer stops admission, drains writes,
   checkpoints the final tail, and durably releases the old grant; a new grant
   can activate immediately after that proof. An unreachable owner requires
   safe lease expiry and bounded old-request handling before tail recovery.
   Compose Chunk-KV's existing serving grants/transfer proofs with Stream
   ownership, rather than add conflicting authorities. A cached epoch,
   lifecycle CAS, or raw DiskIO success alone cannot authorize stale replies.
   Do not reintroduce per-append KV operations. Invariants I1, I3.

10. **D10 — Finite views and WAL consumers; design sections 1, 5, 6.**
    Update `open_read_only`, reader snapshots, and `StreamPartitionJournal` for
    the selected Q4 rule. A reachable owner handles an ordered flush/checkpoint
    barrier, persists the synchronized ChunkDB boundary and then the directory
    checkpoint, and returns one finite view after root publication. A failed
    publication retains the older matching checkpoint; readers never combine a
    raw newer ChunkDB cursor with an unrelated root/epoch. An unreachable owner
    yields the consistent persisted ChunkDB checkpoint prefix, explicitly allowing lag.
    The reader does not adopt or publish writer state; an owner checkpoint
    triggered by its request is allowed. This replaces the earlier requirement
    that every fresh read-only open include all acknowledged bytes even when
    the owner is unavailable. Return the captured end and source; reject an
    exact requested cutover beyond it. Writer failover still recovers newer
    acknowledged contents instead of treating fallback as final EOF. Preserve
    WAL retry/replay and split/transfer semantics. Invariants I1, I4, I6.

11. **D11 — Reclaim shared history safely; design sections 6, 7.**
    Replace generation-only page deletion and one-generation cache assumptions
    with independent-page reachability and explicit retention budgets. Older
    pages can be shared by newer roots. Trim publishes its watermark before
    release and frees a chunk only when all retained mappings/pins permit it;
    removing one frame extent cannot authorize releasing its whole chunk.
    Protect in-flight publication pages until the head outcome is resolved.
    Bound orphan/unreachable-page cleanup and preserve idempotent restart.
    Invariants I2, I5, I10.

12. **D12 — Measure the changed path; design sections 8, 9.**
    Update `stream_perf_metrics`, server perf registration, production bounds,
    and Stream/Chunk-KV sentinels. Replace the formal design's historical
    NullDisk reference table with current MemDisk fixture requirements while
    preserving existing regression floors/ceilings. Use matched `mem-block`,
    retained MemDisk, release artifacts, explicit mirror count/queue geometry,
    and no injected latency. Count synchronized write RPCs, Stream publications
    separately from internal `chunkinfo` changes, compact extents, reused/written
    pages, and recovery work. Report count/avg/p50/p99/max for standalone append
    and actual WAL/client mutations. Demonstrate lower matched latency and
    bounded growth without skipping synchronization responses or validation.
    Update the documentation/backlog navigation summaries to match this scope.
    Invariants I1, I2, I4, I6, I8, I9, I10.

#### Dependencies

- The agreed design replaces per-batch cursor authority. Q1–Q4 all have selected
  rules, not completed implementations. Implement grouped framing, ownership,
  recovery, and live-view contracts before claiming the new completion boundary.
- Reuse the common frame codec, `ChunkLocation`, required-replica writes, existing
  chunk-internal repair/persistence, manifest CAS, and atomic perf histograms.
  `StreamChunkStore` cursor outcomes and old durable-cursor tests need replacement
  where they encode the superseded commit boundary.
- The synchronized wire option affects protocol, Rust client/adapter, and C++
  DiskIO server. Update their tests and matching DiskIO/chunk-IO documentation
  together; those neighboring contracts are not already implemented by this
  documentation update.
- A shared frame revision covers Rust/C++ codecs, user-data readers/writers,
  native S3 buffers, Iceberg file blocks, Stream, common tree chunk/page storage,
  and the finalizer. Only Stream enables head/follower semantics; its append
  size is known before encoding. User data uses the common ordinary-frame mode and
  whole-object publication/failure, including unknown-length uploads. Preserve
  native buffer ownership and versioned location/capacity arithmetic without
  adding Stream markers or group buffering to user-data writers.
- Stream purpose, group binding, Chunk-KV ownership, consumer retention, and
  allocation/reuse protections remain required. The product has not shipped:
  support only the selected current format and matched service contract. Remove
  older-format decoders, directory migration and old-service fallbacks. Unknown
  versions, malformed records and missing synchronization proof fail explicitly;
  correctness and performance acceptance remain unchanged.
- R243 deployment acceptance remains separate and its failed performance checks
  stay visible. Runtime artifacts support attribution but are not prerequisites.
  R148 migration, sharding, and EC conversion must reuse the resulting
  directory/retention contract and are not prerequisites for this work.
- Retained MemDisk supports byte validation and reopen while its backing media
  stays alive. Tests that destroy media or model power loss need durable backing
  or a deterministic durable-storage seam. NullDisk generated bytes do not prove
  recovery or replace matched retained-byte measurements.

#### Acceptance

- A01 (D01): Given small, exact-full-frame, 1 MiB, 4 MiB, and mixed-size payloads
  -> append and read head/middle/tail and cross-boundary subranges -> assert one
  location per large payload, exact bytes/logical lengths, checked physical
  arithmetic, and no invalid merge across short interior frames. Invariants I6,
  I7. Integration test.
- A02 (D02): Given buffered/synchronized writes with delayed or failing write
  and flush completions -> exercise the actual DiskIO wire/server -> assert one
  synchronized write request/reply without a client fsync request, no early
  success, exact errors, bounded splitting, and strip ordering. Invariants I1,
  I9, I10. Integration test.
- A03 (D03): Given a referenced active chunk, configured mirror counts, and a
  slow/failed replica -> append within capacity -> assert acknowledgment only
  after required synchronized replica completion, exact ordered ranges, zero
  per-batch ChunkDB cursor operations, zero Stream directory/head writes, and
  unresolved outcomes never classified using a stale cursor. Invariants I1,
  I3, I6. Integration test.
- A04 (D04): Given a Stream and internal strip growth/replica replacement ->
  persist and reopen the chunk layout -> assert updated `chunkinfo`, readable
  bytes, unchanged Stream generation/page identities, and no publication caused
  solely by the internal transition; synchronized local progress never regresses
  to the persisted checkpoint cursor. Invariants I1, I8. Integration test.
- A05 (D05): Given failures or lease expiry after allocation, reference
  publication, data sync, seal, page writes, and successor head CAS -> restart
  and finalize at each point -> assert
  every acknowledged chunk is discoverable, successor writes require attachment,
  history is unchanged, and at most the last referenced chunk needs parsing;
  unattached allocations contain no acknowledged bytes. Invariants I1, I3, I4.
  Integration test.
- A06 (D06): Given acknowledged frames newer than the persisted checkpoint and
  retained sealed history -> cold open through bounded production reads -> assert
  cursor, logical tail, and compact mappings reconstructed from content, every
  acknowledged byte retained, no earlier chunk-content scans, and scan counts
  bounded by one chunk's geometry rather than Stream length. Invariants I1,
  I4, I7. Integration test.
- A07 (D06/Q1/Q2): Given truncated frames, wrong identities, CRC corruption,
  conflicting/unequal replica suffixes, and interrupted multi-frame writes ->
  recover using the selected Q2 rule and agreed Q1 format -> assert a
  deterministic complete-append prefix and repair/error outcomes. Interrupt a
  large append after every follower boundary and within its final frame: none
  of that append becomes visible or advances cursor/checkpoint/extent state.
  Even a batch of multiple appends preserves each request's group boundary.
  Assert no silent truncation of required acknowledged data,
  no adoption of invalid bytes, no finalizer deletion based on an old zero
  cursor or unreadable first replica, and no stale checkpoint cursor used as EOF.
  Reject impossible/overflowing declared group lengths/counts and mismatched
  follower roles/head offsets/ordinals without unbounded allocation or reads.
  Invariants I1, I4, I6. Integration test.
- A08 (D07/D08): Given histories of 1, 16, and 256 full directory segments ->
  append identical suffixes, checkpoint, roll, open, and adopt a higher epoch
  -> assert unchanged historical identities/bytes, no historical directory
  reads/builds/writes for ordinary append, bounded root/fanout/changed paths,
  no whole-active-index copy per append, lazy open, and bounded cache retention.
  Invariants I2, I3, I7, I10.
  Integration test.
- A09 (D08): Given competing publishers and failures between new-page writes
  and head CAS, response loss, or conflicting immutable keys -> resolve/restart
  -> assert a complete authoritative root, unchanged prior retained roots,
  purpose/stale-publisher fences, and no duplicate completion. Invariants I1,
  I2, I3. Integration test.
- A10 (D09/Q3): Given an old writer paused across ownership/lease expiration
  and delayed old DiskIO requests -> take over, resolve the tail, and resume the
  old process -> assert Group-0 grant deadlines prevent stale acknowledgments,
  explicit drained release allows immediate activation without waiting for TTL,
  unreachable-owner activation waits for safe expiry/in-flight exclusion, and
  retained new-owner data is preserved with no per-append KV workaround.
  Invariants I1, I3. Integration test.
- A11 (D10/Q4): Given an acknowledged uncheckpointed suffix, an active writer,
  and an existing finite reader -> capture while the owner is reachable, then
  unreachable, and append/roll/replay -> assert owner flush returns its captured
  synchronized end, fallback returns the consistent persisted checkpoint end
  with lag/source explicit, and an exact newer cutover is unavailable. Preserve
  captured ends/epochs; the reader never adopts/seals/allocates or publishes
  writer state, while owner-triggered checkpoint publication is allowed. Writer
  recovery retains newer acknowledged data. Interrupt between ChunkDB cursor
  persistence and root publication: fallback keeps the prior matching complete-
  group bound and never combines mismatched epochs/roots/cursors. Invariants I1,
  I4, I6. Integration test.
- A12 (D11): Given roots sharing old pages, reader/checkpoint/overlay pins,
  in-flight candidate publications, and several extents in one chunk -> trim,
  reclaim concurrently with publication, release pins, and restart cleanup ->
  assert live shared pages and partially retained chunks survive,
  in-flight candidate pages stay protected, only eligible whole chunks and
  unreachable pages are freed, and pass budgets/idempotence hold. Invariants I2,
  I5, I10. Integration test.
- A13 (D03–D11): Given both Wal and Stream purposes and actual Chunk-KV mutation
  traffic -> crash/reopen, checkpoint/trim, split, and transfer around mirror
  completion/lifecycle publication -> assert retained acknowledged bytes,
  exact offsets, retry results, owner fences, and finite cutovers on the shared
  implementation with correct purpose. Invariants I1, I3, I4, I5, I6, I7, I8.
  E2E test.
- A14 (D12): Given matched isolated release fixtures using retained MemDisk,
  `mem-block`, identical explicit mirror counts, and growing histories -> run
  one-/32-producer 4 KiB, large-payload, rollover, read/replay/trim, and actual
  WAL/client workloads -> assert unchanged regression minima, lower matched
  append latency, zero errors/watchdogs, bounded RSS/queues/directory work,
  one synchronized RPC per replica for an unsplit single-strip batch, no
  ordinary append KV operations, and initial-attachment-plus-rollover head
  counts with optional checkpoints off. Record binary/config identities,
  phase count/avg/p50/p99/max,
  index/extent/recovery/cleanup counters, and checked cleanup receipts.
  Invariants I1, I2, I4, I6, I8, I9, I10. E2E test.
- A15 (D01/Q1): Given approved shared-format golden vectors and known-/unknown-
  length user data -> encode/parse with Rust and C++, use native and ordinary
  S3/Iceberg writes, Stream append, and tree frame paths -> assert identical
  framing/CRC interpretation, exact bytes and subrange arithmetic, Stream-only
  complete-append group validation, ordinary user-data mode without Stream
  markers,
  bounded streaming memory, and whole-object failure/publication guarantees.
  Invariants I1, I6, I7, I10. E2E test.
- A16 (D03/D06/Q2): Given available mirrors with unequal valid ends, a mirror
  failing mid-observation, a corrupt replica, and no readable replica -> recover
  -> assert the shortest healthy complete-append prefix is selected within the
  original IO budget, failed/partial/corrupt observations never force an
  artificial shorter cursor, no arbitration wait occurs, and absence of a
  healthy completed candidate yields unavailable. Subsequent success still requires all configured mirror
  writes and repair as needed. Invariants I1, I3, I4. Integration test.

Required verification commands for implementation, not for this documentation
update. The C++ gates apply to the synchronized DiskIO request/server changes:

```bash
pixi run clean-env
pixi run cargo test -p crowdb-protocol -p crowdb-chunk-stream -p crowdb-chunk-client -p crowdb-diskio-client -p crowdb-chunkdb -p crowdb-chunk-kv -p crowdb-chunk-kv-server --all-targets
pixi run cargo test -p crowdb-common --lib metrics::
pixi run build-cpp
pixi run ctest --test-dir app/crowdb-diskio/build --output-on-failure
pixi run clean-env
pixi run bash tools/benchmark/bench-chunk-stream-regression.sh
pixi run clean-env
pixi run bash tools/benchmark/bench-chunk-kv-regression.sh
pixi run rs-fmt-check
pixi run clang-format --dry-run --Werror app/crowdb-diskio/src/rpc/dio_server.cpp app/crowdb-diskio/src/rpc/dio_server.h
pixi run tree-lint
pixi run cargo clippy -p crowdb-protocol -p crowdb-common -p crowdb-chunk-stream -p crowdb-chunk-client -p crowdb-diskio-client -p crowdb-chunkdb -p crowdb-chunk-kv -p crowdb-chunk-kv-server --all-targets -- -D warnings
```

For the selected common frame revision, also run the affected consumers and
cross-language C++ tests; the exact changed-file format scope follows the plan:

```bash
pixi run cargo test -p crowdb-access-s3 -p crowdb-access-iceberg -p crowdb-access-server -p crowdb-tree-ffi --all-targets
pixi run ctest --test-dir lib/crowdb-tree/build --output-on-failure
pixi run test-e2e-s3
pixi run test-e2e-iceberg
pixi run cargo clippy -p crowdb-access-s3 -p crowdb-access-iceberg -p crowdb-access-server -p crowdb-tree-ffi --all-targets -- -D warnings
```

#### Open Questions

None. Q1–Q4 are resolved in D01/D03/D06/D09/D10. No user decision is needed
to start implementation. Concrete blockers discovered during implementation
that require a design decision belong here, with evidence and alternatives.
