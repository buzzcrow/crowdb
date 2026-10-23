<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk Stream Performance Plan

Upstream: [R244](../backlog/R244-chunk-stream-incremental-directory-publication.md)
and [Chunk-Stream design](../design/chunkds/design-crowdb-chunk-stream.md).

Goal: make synchronized mirror writes complete ordinary append without Stream
KV work, with compact locations, incremental directories, and bounded recovery.

## Status and scope

- Deployment requirement R243 is complete at `c3d7d64d`; R244 is in progress.
- T01/T02/T03/T05–T07 foundations are committed. T15 is the active recovery task.
  T13's indexed retention primitives are committed; expiry/read-IO grace and
  runtime/consumer wiring remain. Directory, shared-codec, ordinary writer and local-progress
  foundations exist; the main Stream worker still needs grouped/indexed wiring,
  safe ownership/recovery, bounded retention and production acceptance.
- User instruction: this product has never shipped. Support only the chosen
  current format and matched service contract. Do not add old-format migration,
  old-service fallback or capability negotiation. Remove compatibility code and
  its fixtures. Existing development runtime artifacts remain preserved; a test
  fixture creates data in its current format.
- Matched retained-MemDisk baseline remains under
  `r244-chunk-stream/baseline/`; six Stream performance gates are still red.
  No postimplementation performance acceptance is claimed.
- The two pre-existing unrelated user edits stay outside this requirement.

- With compatibility removed, finish independent directory retention foundations
  before switching the main worker. T04's remaining predecessor Stream codec is
  removed together with T08/T20 wiring; the ordinary codec and migration/service
  fallback cleanup are already committed. No old-format support is reintroduced.

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
  canonical bytes with shared golden vectors.
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
  Current-format data survives reopen and faults; no old-format conversion is required.
- T01 and T05–T07 can precede the remaining ownership work.
  Common codec T08 and directory work T09–T13 follow format cleanup T04;
  T14–T19 establish the state, recovery, lifecycle, and view contracts.
  T20 integrates the new completion
  boundary only after these dependencies exist. T21–T26 provide consumer and
  production acceptance. No intermediate speed sample substitutes for them.

## Matched baseline evidence

- HEAD `fceee28e` code baseline; ten executable/library SHA256 identities in
  `r244-chunk-stream/baseline/binaries.json`. Service, test-util fixture and bench
  builds are outside measurement. Three logical native nodes use mem-block KV/
  WAL and retained MemDisk; original mirror counts, queue/batch geometry and
  regression gates are unchanged. All five deployment KVM guests are gracefully
  stopped before measurement, with disks preserved.
- Stream: all seven workloads complete with zero data errors/watchdogs. Six
  original performance checks fail, as expected for the design gap. 4 KiB/1T:
  147.47 ops/s, avg 6780 us, p99 7865 us; mirror avg 1222 us, cursor avg 2503 us,
  directory publication avg 2969 us, directory build avg 11.79 us. 4 KiB/32T:
  1841.59 ops/s, avg 17371 us, p99 36948 us. Large 1 MiB: 42.01 MiB/s;
  rollover 4 MiB: 97.88 MiB/s; random 4 KiB: 22101.41 ops/s; replay: 159.42
  MiB/s; GC: 11110.75 MiB/s. Complete counts/distributions/RSS/queues and fixture
  data/logs are in `baseline/stream/` and `baseline/stream.log`.
- Actual Chunk-KV: unchanged regression passes 30000 operations, 32 producers,
  512-byte values, 25% reads; 2836 ops/s, avg 11015 us, p99 32363 us, zero errors.
  Split/owner redistribution and retained replay pass. `baseline/chunk-kv/`
  contains server phase perf logs, configs, RSS and results. Both exact fixture
  shutdown receipts have `ok=true,error=null`. These are baseline results,
  not completion of the new append/ownership/format contract.

## Synchronized write implementation evidence

- Request/response append-only schema slots, missing synchronization proof and flush
  failure values pass 16 protocol tests. The Rust and C++ schemas are generated
  by their ordinary build paths, with no generated-file edits.
- The semantic client propagates synchronization through contiguous and bounded
  view requests. Matching peers require one write request/reply; missing
  synchronization proof fails without a fallback request. Five actual TCP cases
  cover no early completion, missing-proof errors, buffered behavior, ordered view splitting and typed write/flush
  failures without extra requests.
- The actual C++ write handler retains disk/connection/payload through ordered
  write and flush. Nine new delayed/failure/lifetime/ordering tests pass; all
  165 DiskIO/build-tree CTests pass. Invalid control/payload/zone input performs
  no IO. Full tree-lint exits 0 (existing warnings remain); the two new C++
  files' focused lint has no diagnostics after ordinary style cleanup.
- Mirror and replacement paths now use synchronized seams. Metrics forwarding
  delegates those seams directly; it cannot reintroduce two requests. Two
  production adapter-chain TCP tests pass for three required replicas and a
  delayed replacement, with exactly one synchronized request per copy and no
  explicit fsync. The full two-client suite passes 141 tests.
- Final gates pass: full `test-cpp` has 709 native tests plus 85 Rust FFI/tool
  tests, zero ignored; receipt `unit/cpp-abe7f7e1cc1f42b2b83021beea62b0ba/`
  `results.json`. Three affected crates' all-target clippy, Access-server and
  E2E-test compilation, Rust fmt and changed C++ format pass. One ordinary
  clippy failure was redundant default updates in the new test allocator;
  removed them and repeated the focused test/gate successfully. Logs are
  `synchronized-{full-cpp,client-suite,rust-gate2,fmt-final,cpp-fmt}.log` under
  `r244-chunk-stream/`. The two-client suite includes purpose, mirror repair,
  reads and native-buffer forwarding. New bytes/recovery/lifecycle contracts
  still require the later production acceptance.
- This changes RPC synchronization only. Append still commits ChunkDB cursors
  and Stream directories until the ownership/framing/recovery dependencies are
  implemented; the six failing baseline performance gates remain unmet.

## Format execution notes (T04/T08 implementation target)

- Only canonical revision-2 framing is the implementation target. User-data/
  tree use ordinary roles; Stream uses verifiable complete append groups.
- Share CRC32C input, footer chunk-ID validation, checked role capacities and
  bounded parser memory in Rust/C++. Reject unsupported revisions and roles.
- No old-format conversion, rollout ordering or capability negotiation is needed.
  The predecessor Stream worker and its codec are removed with grouped wiring.
- Preserve current-format crash/reopen, corruption, full-append and cross-language
  tests. Keep ordinary user-data publication atomic for whole objects.

## Shared codec implementation evidence (partial T04/T08)

- `348f300e` introduced shared revision-2 roles/geometry, a bounded complete-group
  validator and independent Rust/C++ vectors. Header/payload CRC32C and separate
  expected chunk-ID validation agree across languages.
- Group encoding preserves Bytes payload owners, checks canonical full interior
  frames, association/ordinal/count/total length and constant-state validation.
- These foundations passed protocol/native gates recorded in `shared-frame-*`
  artifacts. Old-format support from that milestone is being deleted under T04.
  Current remaining group integration is not claimed complete.

## Object reader framing evidence (partial T08)

- Ordinary object reads validate one canonical revision-2 geometry from compact
  locations and actual verified headers. Bounded read windows and credits retain
  original payload owners, including frames crossing physical buffers.
- Current cleanup removes revision-1 geometry and fixtures. Focused tests retain
  exact cross-frame/subrange bytes, pointer ownership and CRC mirror failover.
- Stream last-chunk reads still need their separate authorized content-derived
  boundary; this does not widen arbitrary ordinary object reads.

## Ordinary producer execution notes (T08)

- Small/large/async/native user-data writers emit ordinary revision-2 frames.
  Native regions reserve exact role-dependent overhead and preserve owners.
- Tree packs use the same ordinary format. Compact references carry plain logical
  lengths with no format tag or revision byte. Manifest encode/decode accepts one
  current version. Checksums cover the complete current record.
- Streaming uploads retain whole-object publication and bounded memory; no Stream
  head/follower roles or whole-object buffering are added to ordinary producers.

## Ordinary producer implementation evidence (partial T08)

- All audited ordinary Rust producers now request revision 2, including both
  object writers, shared small batches, S3 native buffers/streaming and
  Access-server S3/Iceberg frame selection. Region offsets, capacities and
  exact-size owner accounting use the common V2 geometry. Native finalization
  checks revision before touching reserved regions and regenerates a prepared
  header if its requested ordinary purpose changes.
- Tree's new format-7 references carry explicit per-pack frame revision through
  the existing length field's reserved high bit; old format-6 references decode
  as V1. Checksums cover the revision tag. New ordinary BtreePage/PageIndex
  frames and current geometry are used by synchronous, asynchronous and
  ownership-materialization paths. Reused locations retain their revision.
  Extracted catalog encoding/checksums and verified pack reads into private
  owning modules, rather than extending the large store implementation.
- Three focused native cases pass for compact reference serialization, unknown
  or mislabeled manifest versions, actual retained V1 bytes/catalog reads and
  a subsequent V2 publication without altering the old bytes. Initial native
  build repairs were test target include propagation, checksum visibility during
  extraction, and a missed private-header declaration; no product test assertion
  has failed in this group. Rust all-target clippy for all four affected crates
  passes. The four-crate Rust run passes 1132 cases; the two existing external
  Python SDK cases remain explicitly ignored by the default command. Full native
  testing found two stale 34-byte/68-byte V1 expectations. Auditing that family
  also found the production Config default still used a 65,502-byte pack; change
  it to 65,501 so one default pack stays within one V2 frame. Added a dedicated
  default-boundary assertion. All six focused cases pass. The repeated full
  native/FFI gate passes 719 native tests and 85 Rust FFI/tool tests, zero ignored,
  and covers the corrected default through the Rust FFI. Receipt:
  `unit/cpp-91a82bf210354f3abb1c9d72b9a33b78/results.json`. Full tree-lint exits
  zero with existing warnings; the final focused private codec/read/revision and
  header lint has no diagnostics. Logs: `frame-consumer-tests2.log`,
  `frame-consumer-full-cpp2.log`, `frame-tree-focused-final.log`,
  `frame-consumer-tree-lint.log` and `frame-tree-lint-final.log`.
- This completes ordinary producer migration, not Stream group integration,
  indexed directories, owner leases or content-derived recovery. T08 remains
  unchecked until those consumers are wired and verified.

## Directory execution notes (T04/T09–T13 implementation target)

- Only independent immutable indexed records are supported. Keep fixed bounded
  heads, compact grouped append locations, SHA-addressed pages and path copying.
- Root publication protects candidates before page writes and resolves a guarded
  head CAS. Retention control revisions fence concurrent publication/pins/GC.
- No frozen old schema, source snapshot, migration marker or conversion cursor.
- Reader pins, sealed chunk spans, bounded root/page/data collection and restart
  cleanup still need implementation before ordinary append can omit checkpoints.

## Indexed record implementation evidence (partial T09)

- `5c9b4bcc` defines bounded heads, SHA-addressed independent leaf/branch records
  and checked scope, height, fences, counts, logical/physical continuity.
- T04 removes old-format discrimination: one grouped append location contains
  logical start and ChunkLocation, without a per-entry frame-format enum.
- Independent page/root/publication-guard/control namespaces survive owner and
  generation changes. No migration namespace or format dispatcher remains.
- Indexed storage foundations exist. The main worker, retention/GC and new append
  completion remain unfinished and must be integrated before acceptance.

## Incremental index implementation evidence (partial T10–T12)

- Added bounded immutable DirectoryIndex traversal and prepared mutations. Append
  loads the rightmost path, retains completed leaf/branch identities and encodes
  only changed tail paths/new segments. Trim copies the left path and keeps a
  location crossing the watermark whole. Preparation does not write KV state or
  authorize chunk release; roots remain independently readable.
- Lookup loads at most height + 1 records. Range scans bound both record IO and
  returned locations, with continuation offsets tied to the captured immutable
  root. A page budget below the root traversal depth fails before IO, preventing
  a continuation that cannot make progress. One checkpoint accepts at most 65536
  locations; excess work backpressures instead of materializing unlimited history.
- Production KV implements identity-addressed page reads. Every loaded record is
  verified for content identity, purpose and reference fences before traversal.
- Seven integration cases cover 1/16/256 historical segments, full/unfinished
  tail reuse, height growth, bounded paginated ranges, compact 4 MiB groups,
  complete grouped locations, cross-leaf continuity, trim and missing/corrupt records.
  All 70 Stream tests, all-target clippy and fmt pass. Receipts:
  `directory-index-tests.log`, `directory-index-full-stream.log`,
  `directory-index-clippy4.log`. These APIs are not yet the active append/read
  state machine. Guarded KV publication, root retention, bounded
  active-tail snapshots/cache and append completion remain unfinished.

## Guarded KV publication evidence (partial T11/T13)

- Added an identity/revision-bearing indexed-head snapshot and production KV
  publication of prepared DirectoryMutation values. Candidate scope, base root,
  monotonic generation/epoch/trim/tail and active original/checkpoint locations
  are validated before writes. New pages and immutable root records use
  create-if-absent CAS and exact-byte conflict/unknown-outcome reconciliation.
- A bounded durable guard contains the candidate, base head and at most 128
  new page identities. It precedes page storage. Head replacement and guard
  removal are one conditional KV batch; failures retain the old head and guard.
  The per-stream retention-control revision advances atomically on guard creation
  and head replacement, fencing GC against both new guards and same-ID orphan
  resurrection. Existing and future pin/retirement/cleanup transitions must
  compose with this same revision; a plain check followed by deletion is unsafe.
- Fixed a concurrent first-publication observation: a control read can precede
  another publisher's atomic control/guard creation. A subsequently found guard
  with that older absent-control observation returns backpressure, not corruption.
  Concurrent identical candidates across 16 fresh stream scopes exercise this.
- Two protocol and seven real-KV cases pass, including unknown record shapes,
  scope/identity/allocation bounds, retained roots, page conflicts/protected retry,
  stale owner CAS, active checkpoint disagreement, unknown-schema rejection and
  both GC revision races. Full Protocol/Stream suites pass 349 cases, zero
  ignored; all-target clippy and fmt pass. Logs:
  `directory-publication-{protocol,tests,clippy4,full-suite2,fmt2}.log`.
- The predecessor Stream worker is not switched yet:
  root pins/GC, owner authority, content recovery and the new finite-view/ACK
  contract must be wired together. No performance acceptance is claimed here.

## Independent page cache evidence (partial T12)

- KV directory reads now use a fixed 64-slot, directly mapped ArcSwap cache.
  Page IDs remain reusable across root epochs/generations; no historical map is
  cloned or retained. Cold insert verifies the bounded record's SHA identity and
  copies only its bytes, avoiding retention of a larger transport allocation.
  Collisions replace one slot; there is no new lock or unsafe scope.
- DirectoryCacheSnapshot exposes hits, misses, replacements and resident
  pages/bytes/slot limit. Resident page bytes are bounded by 64 * 64 KiB,
  independently of retained history; caller-owned in-flight buffers follow their
  existing admission bounds. Cache residency does not replace root/data pins.
- Three additional real-KV cases verify reuse across publication generations,
  512 historical pages under the fixed slot/byte bound, and refusal to cache
  corrupt/oversized records. Full Protocol/Stream suites pass 352 cases, zero
  ignored; all-target clippy and fmt pass. Logs:
  `directory-cache-{clippy4,full-suite,fmt}.log`. The predecessor worker remains until the indexed state machine is wired.

## Shared metadata backend evidence (T09–T12 wiring foundation)

- `9e911373` shares one conditional-record state machine between KV and the
  deterministic Memory backend. The memory adapter uses its existing test lock;
  no production lock or new unsafe scope was added.
- The factory binds metadata group and scope. Both backends use the same immutable
  page writes, publication guard/control ordering and revision-fenced head CAS.
- Retained roots and failed-publication protection tests remain. Compatibility
  conversion tests and their source/marker/progress machinery were removed.
- The main worker still needs to consume this factory; custom deterministic
  wrappers must forward it when that integration replaces the predecessor API.

## Local synchronized progress evidence (partial T14)

- ProductionStreamChunkStore now tracks persisted and synchronized physical
  cursors separately. Stream V2 chunks admit subsequent grouped writes from
  synchronized progress; layout/query/repair refreshes only advance, never rewind,
  that local position. Growth descriptors preserve it. Liveness renewal continues
  to use the persisted checkpoint and does not silently publish a newer cursor.
- The first grouped write consumes a locally fresh allocation token; a cold
  adapter cannot overwrite a prior process's group even when its persisted
  cursor is zero. A failed first attempt cannot turn an unknown old range into
  a fresh allocation. Grouped chunks reject legacy-frame writes. Legacy V1/raw
  test seams retain their cursor-authority behavior until grouped wiring.
- Only successful synchronized completion of all strip writes promotes local
  progress. Explicit seal accepts the synchronized grouped boundary and persists
  it through the existing chunk-layer seal. MemoryStreamStore models the same
  grouped progress/seal contract under its existing test-only mutex. The mock
  production allocator's seal response now matches actual ChunkDB by returning
  the exact persisted seal_bytes cursor.
- Three new production-adapter cases cover stale checkpoint queries, renewal,
  growth, replica repair/full images, further append and final seal, mixed
  revisions and cold overwrite refusal. One memory case covers multiple groups
  with zero cursor advances before explicit seal. Focused 13 cases and clippy
  pass. All 91 Stream cases and fmt pass, zero ignored. Logs:
  `local-progress-{clippy4,tests3,full-stream,fmt}.log`.
- The existing worker still encodes V1 and waits for cursor/directory publication.
  Removing growth-triggered Stream publication and using these local positions
  in the grouped worker remain wiring tasks. No lease, recovery, finite-view or
  performance acceptance is claimed by this adapter preparation.

## Current capacity isolation evidence (partial T14)

- StreamChunkStore reports current chunk-layer capacity. Production uses its
  authoritative layout cache and Memory reports backing capacity. This is now
  required of test adapters too; no missing-query publication branch remains.
  Growth never creates a Stream head/directory publication solely for capacity.
- The predecessor writer/read-only open refresh capacity from the chunk layer after its
  durable query rather than rejecting newer valid storage against a cached
  manifest capacity. The read-only view still stops at its captured root; it
  never combines a newer physical cursor with an older logical directory.
- Layout cache replacement now compares modify_ts through ArcSwap RCU. Delayed
  old replies cannot replace a newer layout; cursor/checkpoint counters and the
  sealed flag do not move backward. This adds no lock. The allocator mock's
  seal timestamp/cursor now match actual ChunkDB transitions.
- Two growth cases prove unchanged attachment/append publication counts without
  growth-only writes, and a persisted 1.5 MiB legacy cursor with a still-1 MiB
  manifest hint. Cold read-only capture retains EOF zero; writer reopen recovers
  exact retained bytes. Production-adapter coverage injects an older layout
  reply after growth and verifies capacity/progress remain current. Focused
  14 cases and clippy pass. All 92 Stream cases and fmt pass, zero ignored. Logs:
  `capacity-isolation-{clippy3,tests,full-stream,fmt}.log`.
- The seal boundary correction below rejects stale shortening. T16 must still
  align expiry/finalization: a grouped owner's synchronized position can exceed
  its persisted cursor. Do not enable deferred append completion until owner
  exclusion and content-derived finalization protect that progress.

## Persisted cursor seal boundary evidence (partial T16)

- Raw Stream/Wal sealing rejects an exact byte boundary below an already
  persisted acknowledged cursor before touching reservations, strips, task state
  or cleanup. Larger synchronized boundaries remain legal. The check runs under
  existing lifecycle sequencing; no lock or unsafe scope was added.
- Two actual pinned KV/DiskDB/ChunkDB lifecycle cases cover both Stream and Wal,
  rejection at byte 16 after cursor 17 with complete state/version equality,
  exact-byte seal, and sealing a newer boundary from an older zero checkpoint.
  Full ChunkDB tests, all-target clippy and fmt pass; counts/logs are retained in
  `stream-seal-{tests,clippy2,full-chunkdb,fmt}.log`.
- This prevents regression of a persisted cursor only. Expiry/finalizer still
  require allocation format authority, grouped content scans and owner/old-IO
  exclusion before deferred-cursor ACK can be enabled. In particular zero in
  old metadata cannot prove grouped data absence.

## Unpublished-format cleanup execution

- User confirms no released old format/service must remain supported. Removed
  frozen Chunk bodies and reservation fallbacks, all Stream source/marker/
  progress/conversion modules and keys, and their dedicated fixtures. The
  compact indexed extent now has no per-entry format enum.
- Chunk metadata accepts only its explicit current envelope. The journal test
  now uses the actual current encoder; its CAS/flush/reopen assertions remain.
- Indexed head decode returns the current bounded head directly. No migration
  head dispatcher or legacy-to-indexed path remains. Raw Stream metadata decode
  was removed; the predecessor worker's own metadata API still exists only
  until the indexed worker replaces it.
- Missing synchronized-write completion proof returns a durability error with
  no fallback fsync or repeated write. Removed nested fallback accounting.
- Ordinary object reads have one revision-2 geometry. C++ tree manifests accept
  only the current version, references carry plain logical lengths, and reads/
  checksums no longer branch on per-reference frame revisions or old manifests.
- The shared revision-1 frame codec and old Stream worker remain to be removed
  together with grouped recovery/finalizer and indexed-worker wiring; they are
  unfinished implementation, not an old-format support requirement. No claim of
  completed T04/T08 or new append/performance acceptance is made here.
- Initial affected suite failed in `journal_handoff_test` because its fixture
  bypassed the production metadata encoder and wrote a raw body. Fixed that
  fixture at its creation point, without restoring any compatibility decoder.
  The focused 20 cases and all 652 cases across Protocol, Stream, ChunkDB,
  DiskIO-client and Chunk-client pass with zero ignored tests. Strict all-target
  clippy and Rust/C++ format pass. Full tree-lint exits zero; changed-scope lint
  is clean after removing redundant parentheses (the unrelated existing
  make_shared warning in the pipeline remains). Full `test-cpp` passes 719
  native cases and 85 Rust FFI/tool cases with zero ignored tests; receipt
  `unit/cpp-e2ca7816ffc74dc18ef850d7f4d540f0/results.json`. Complete logs are
  `no-compat-{focused2,full-suite,clippy2,tree-lint,tree-lint-focused,full-cpp,
  fmt-final,cpp-fmt}.log` under `r244-chunk-stream/`.

## Canonical ordinary-frame cleanup

- Removed ordinary revision-1 magic variants in Rust/C++, both retained CRC
  golden vectors and the alternate footer-inclusive CRC acceptance. The one
  supported CRC covers header/group fields/payload; expected chunk identity is
  separately verified. C++ now has only revision 2, without old-revision header
  helpers. Current ordinary encoders/readers and tree packs use revision 2. Negative tests reject removed magic values and noncanonical CRC.
- Native-body tests check wrong-purpose rejection before touching framing bytes.
  The actual ChunkDB finalization test now writes current PageIndex frames.
  These consumer tests plus frame tests pass 33 cases, zero ignored.
- Product E2E small/large/repair/read fixtures still used old magic, the 65,502-
  byte payload cap or 14-byte ordinary headers. Updated them to current 65,501-
  byte payloads and 15-byte headers while preserving physical bytes, mirror
  repair and subrange assertions. Object location arithmetic uses current
  ordinary geometry; predecessor Stream accounting is private to that worker. Compile them through all-target
  clippy. Actual OCI E2E needs the final matched current image; do not run an old
  deployment image against the new strict client and call that acceptance.
- The predecessor Rust Stream worker still emits revision 1; remove that last
  Stream codec alongside grouped recovery/finalizer and indexed-worker wiring.
  This is current implementation being replaced, with no old-format read or
  migration commitment. The final shared codec supports only revision 2.

- Final canonical-frame gates pass: all 481 Protocol/Stream/Chunk-client tests,
  33 focused framing/native-body/finalizer cases, and two real TCP mirror-chain
  cases; zero ignored. All-target clippy also compiles affected Access, ChunkDB,
  Stream and product E2E consumers. Full `test-cpp` passes 720 native cases and
  85 Rust FFI/tool cases, zero ignored; receipt
  `unit/cpp-d0bbea06f8ee4685b2684386f3ef71da/results.json`. Full tree-lint exits
  zero with existing warnings and no diagnostics in the changed codec/tests.
  Rust/C++ format pass. Logs: `canonical-frame-{full-suite,consumers,mirror-chain,
  clippy2,tree-lint,full-cpp,fmt,cpp-fmt}.log`. Actual product OCI E2E remains T26.

## Bounded retention inventory foundation (partial T13)

- `DirectoryRecords::scan_keys` reads only bounded keys/revisions through KV's
  bounded scan. Typed root/page/publication inventory pages enforce a maximum of
  128 keys, strict ordering/scope/shape, exclusive continuation and explicit
  incomplete zero-progress errors. No record bodies are loaded for enumeration.
- Memory uses its existing test lock and an ordered metadata map, selecting one
  bounded lexicographic range without sorting or cloning all metadata. Production
  gets no new lock or unsafe scope.
- Two actual shared-backend tests pass: five root generations/pages paginate
  exactly, scopes/groups do not leak, scans do not publish heads, invalid limits
  fail, a malformed 128 KiB body is not decoded by key enumeration, and a damaged
  key fails closed. Full Stream/clippy/fmt gates pass with the atomic-root tests
  below.
- This is an inventory primitive, not completed collection. Root retirement,
  chunk spans, reachability and control-revision-conditional deletion still need
  implementation. Enumeration never authorizes deletion on its own.

## Atomic retained-root publication (partial T11/T13)

- Retained root records are now written in the same head-revision CAS batch as
  the stable head, retention control and guard removal. Page writes remain
  protected and precede that batch. A rejected head CAS cannot leave a root
  record that looks published. No separate publication receipt is needed.
- Existing root bytes are checked for conflicts before the batch. Competing
  normal publishers cannot overwrite a retained root: they consume the same
  expected head revision, so only one batch can win.
- Deterministic and actual KV cases pass: injected failure/revision conflict
  leaves pages guarded but no new root; retry keeps page identities and commits
  head/root with identical bytes and the same KV revision. Existing earlier
  roots survive. All nine indexed KV/cache tests also pass.
- This establishes a published-root identity for later pins/retirement, not
  completed pin/GC/consumer integration. Full Stream passes 82 tests, zero ignored;
  all-target clippy and Rust fmt pass. Logs are
  `directory-{root-atomic-tests,retention-full-stream,retention-clippy,
  retention-fmt}.log` under `r244-chunk-stream/`.

## Exact root pins (partial T13/T19)

- Current-format pin records contain a full bounded directory head and fixed
  needed/captured interval, not an old-generation conversion or compatibility
  descriptor. Keys use tag `0x0019`; records are at most 1 KiB with 512 identities
  per stream. Key scans remain bounded to 128 entries. No new production lock.
- New identities require an exact retained published root. Existing identical
  pins and live reader renewal survive retirement of the root-index row;
  retained page roots remain in the pin descriptor. Durable consumers have no
  expiry; requested reader leases are at most 60 seconds and cannot renew after
  expiry. Stale snapshots cannot release or renew superseding pin revisions,
  including identity reuse. Lifecycle writes cannot recreate missing control.
- Pin creation/renewal/release mutate retention control in the same CAS batch.
  Control is captured before root validation/inventory, so concurrent root
  retirement or new protection invalidates a stale cleanup CAS. Missing roots,
  expired pins, conflicting identities and full budgets fail explicitly.
- Three protocol and six shared Memory/actual-KV tests pass, including root
  retirement, rejected stale cleanup, exact old EOF, revision/ABA, expiry,
  bounded admission and missing-control fencing. An initial retirement fixture
  incorrectly built a successor from an empty root; it now extends its actual
  base. Full Protocol/Stream: 361 passed, zero ignored; strict all-target clippy
  and Rust fmt pass. Logs: `root-pin-{tests2,full-suite,clippy}.log`.
- This is metadata state, not a read IO lease or completed reclamation. T19 must
  enforce local conservative deadlines and background renewal. T13 must add
  expiry/read-IO grace, retirement policy, sealed spans, page/data reachability
  and bounded conditional cleanup before enabling deletion.

## Bounded root-index retirement (partial T13)

- `retire_directory_roots_before` examines at most 128 oldest retained root
  identities and atomically deletes only those below the supplied generation
  horizon. The horizon cannot exceed the observed head. It captures control
  before revalidating the exact head and conditionally mutates control with the
  deletions. Conflicts retry; unknown outcomes reconcile absence.
- Current head and exact pins survive; new identities cannot acquire retired
  roots. This removes no pages or physical bytes. Repeating bounded passes
  continues from the remaining oldest keys and stops idempotently at the
  horizon. No migration, tombstone or compatibility representation is added.
- Shared Memory/actual-KV tests retire four of five roots in two-record passes,
  reject stale heads and invalid budgets/horizons, keep the current head and
  all pages unchanged, and read the exact pinned old view afterward. Actual KV
  leased-reader renewal after retirement now uses this API as well.
- Full Stream: 90 tests, zero ignored; focused pin tests, strict all-target
  clippy and Rust fmt pass. Logs: `root-retirement-{tests,full-stream,pin-tests,
  clippy-final,fmt}.log`. Main worker/consumer wiring and page/data collection
  remain unfinished.

## Required current adapters (partial T04/T14)

- `StreamChunkStore::current_capacity` is required and returns a capacity, with
  no unsupported/`None` branch. Open reads current chunk geometry directly.
  Growth updates local capacity without a legacy-store publication fallback.
- Indexed directory binding and liveness renewal are required adapter methods.
  Test wrappers explicitly forward both capabilities to their real backend;
  missing implementations cannot silently compile with an unsupported binding
  or successful no-op renewal.
- Existing successor-growth/stale-capacity, local synchronized-progress,
  publication ordering, liveness, reads and reopen tests cover the current
  contract. Full Stream passes 88 tests, zero ignored. Strict all-target clippy
  and Rust fmt gate this cleanup; logs are `adapter-cleanup-{tests,clippy,fmt}.log`.

## Bounded directory-page collection (partial T13)

- An opaque bounded proof cursor captures retention control before validating
  the exact current head and independently addressed candidate bytes. One
  continuation allocation is reused across passes; no historical mark set or
  full directory is materialized. Each pass observes at most 128 protections.
- Current and retained roots, every exact pin, and every publication guard are
  checked. Root fences select one path with at most directory-height page IO;
  guards protect explicit new pages and the complete base subtree even when
  the candidate root is not readable or its base root-index row has retired.
  Expired pins remain conservative protection pending safe expiry cleanup.
- Control changes invalidate partial proofs. Only a complete proof permits
  deletion in a control-revision CAS that also advances control. Uncertain
  writes reconcile absence; malformed/missing proof records fail closed. The
  bounded local cache evicts confirmed deletions. Restart simply begins a fresh
  proof against current protection, without trusting incomplete old marks.
- Six Memory/actual-KV tests cover shared and changed pages, retired roots with
  exact pins, ten retained generations with one-observation continuation,
  pin/publication invalidation, unpublished pages/base subtrees, invalid budgets
  and corrupt candidates. Current reads/head remain unchanged. A protocol test
  verifies ID-only record decoding derives checked fences and rejects unknown
  envelopes, trailing bytes and wrong scope. This is the current format only.
- Full Protocol/Stream: 370 passed, zero ignored; final six focused tests,
  strict all-target clippy and Rust fmt pass. Lint repairs moved a fixture test
  into its existing target, boxed the large continuation once, and replaced two
  redundant closures. Logs: `page-collection-{tests-final,full-suite,
  clippy-final,fmt}.log`. Whole-chunk spans/data collection, expiry grace,
  abandoned-guard cleanup and runtime wiring remain unfinished.

## Resolved publication-guard retirement (partial T13)

- Added bounded typed guard loading and exclusive-continuation retirement. A
  guard at/below the exact revalidated stable head generation is resolved or
  superseded: strictly increasing head generations prevent its original CAS
  from winning again. A guard newer than the head remains protected. No timeout
  or old-format fallback is used as authority.
- Cleanup captures control before head/guard observations, validates records,
  and atomically advances control with eligible deletes. It reuses the root
  retirement conditional-delete/reconciliation boundary and does not delete
  pages. A guard with an excluded owner but still-next generation needs the
  later ownership proof; this path does not guess exclusion from elapsed time.
- Two additional tests cover five guards in single-record pages: four resolved
  or losing candidates retire, the live successor remains, and its retry still
  publishes correctly. The old unshared base subtree becomes collectable only
  after retirement. Actual KV malformed guards block both retirement and page
  deletion. All 98 Stream tests, strict all-target clippy and Rust fmt pass.
  Logs: `guard-retirement-{tests,full-stream,clippy,fmt}.log`.

## Exact sealed chunk spans (partial T13)

- One immutable current-format span records chunk identity/purpose, original
  logical base, complete final logical end and physical end. The independent
  key uses tag `0x001a`; bounded key-only scans enumerate spans without decoding
  bodies. Fixed record bounds reject wrong scope/geometry and unknown envelopes.
- Ending an active attachment now requires exactly one matching span in the
  prepared mutation. It must match its original attachment, successor base,
  prior checkpoint and complete final append. A same-chunk checkpoint cannot
  invent a sealed span. The stable head/root, span, control and guard removal
  commit in one CAS; conflicting immutable span bytes fail.
- Protocol and Memory/actual-KV tests cover invalid/missing/partial spans, failed
  head publication with no span residue, retry, equal head/span KV revisions,
  cold loading, empty chunks and partial trim retaining the original full span.
  Full Protocol/Stream: 379 passed, zero ignored; strict all-target clippy and
  Rust fmt pass. Logs: `sealed-span-{tests-final,full-suite-final,clippy-final,
  fmt}.log`. Whole-chunk reachability/release and main-worker wiring remain.

## Predecessor whole-chunk trim correction (partial T13)

- The existing trim loop released whole chunks one extent at a time. Two focused
  cases failed before the fix: trimming one completed frame of a sealed chunk
  with a retained suffix, and trimming one record while its chunk remained
  active. Both incorrectly released 100,068 bytes instead of zero.
- Cleanup now groups contiguous extents by chunk, checks the complete final
  logical end and excludes the current active chunk. One release removes the
  whole eligible group. The byte budget stops between whole chunks, while one
  eligible chunk can exceed that budget to ensure progress. Trim still publishes
  its logical watermark before any release.
- All three regression cases pass after the correction, including later append
  to the trimmed active chunk, partial suffix reads, rollover, whole final release
  and a one-byte GC budget. Full Stream: 106 passed, zero ignored; strict
  all-target clippy and Rust fmt pass. Logs:
  `whole-chunk-trim-{before,after,full-stream,clippy,fmt}.log`.
- This fixes the currently running worker. It does not claim new indexed
  root/pin retention or the no-KV append completion boundary is wired.

## Whole-chunk retention proof and retryable release (partial T13)

- Page and sealed-chunk collection share the bounded protection walker and an
  opaque `DirectoryCollection` continuation; the unpublished page-only type/API
  was replaced without forwarding aliases. Each pass observes at most 128
  records, with no growing history set or new production lock.
- Complete span end must not exceed current trim. Current/retained heads protect
  visible intervals plus their whole active chunk, pins protect fixed captured
  intervals, and guards protect both candidate/base data. Partial overlap keeps
  the whole chunk. Expired pins remain conservative until safe expiry cleanup.
- A successful proof CAS advances control and retickets the unchanged span by
  its KV row revision. The span itself remains durable work until physical
  release succeeds; no extra tombstone/job namespace is needed. A newer ticket
  invalidates the old one before IO. Completion removes the span under a fresh
  control fence; interrupted physical or metadata completion can restart/reprove.
- `release_trimmed` now requires the exact physical end in every adapter. The
  production adapter queries current ChunkDB state, rejects active/mismatched
  seals and treats deleted/absent IDs idempotently. Memory enforces the same
  seal boundary. The predecessor worker supplies its checked whole-chunk end.
- Five additional tests with real grouped payloads cover current trim, retained
  roots, one-byte exact pins, late empty views, interruption before/after data
  release, idempotent retry, rejected active physical chunks and superseded
  tickets. Shared actual-KV/Memory cases keep the active successor untouched.
  Production-adapter tests also reject active/short boundaries and repeat a
  completed release as zero bytes. Full Stream: 111 passed, zero ignored; final
  ten span tests, strict all-target clippy and Rust fmt pass. Logs:
  `chunk-collection-{check,tests,full-stream,tests-final,clippy-final,fmt}.log`.
- T13 still needs the runtime driver/budgets and consumer wiring, safe automatic
  pin expiry/read-IO grace and ownership-proven cleanup of next-generation
  abandoned candidates. Move to T15/T16 recovery now; do not enable no-KV append
  ACKs before ownership, finalization and finite-view dependencies are complete.

## Remaining bounded-publication wiring

- New indexed retention still requires runtime whole-span collection and
  safe expiry integration. Preserve the predecessor whole-chunk trim
  regression cases when replacing that worker.
- An active 256 MiB chunk can hold more than 65,536 tiny append locations.
  `MAX_DIRECTORY_MUTATION_EXTENTS` and the 128-page publication guard cannot be
  raised to accommodate a whole chunk in one allocation. During rollover,
  stage bounded path-copy batches under durable candidate guards; retain each
  staged root until the final bounded head CAS publishes the complete root.
  Intermediate staging must not publish stable heads or immutable roots under
  the final epoch/generation identity. Ordinary append still performs no KV
  work when optional checkpoints are off, and rollover performs one head CAS.
- Staging restart/retry and orphan cleanup must use retention-control revisions
  and ownership fences. This is new-format publication construction, not an old
  directory migration. Add tiny-payload history coverage beyond one mutation
  budget and crash between stages before marking indexed-worker wiring done.
- All new sealed chunks have an exact original logical base in their attachment.
  Persist exact sealed chunk spans for whole-chunk retention. No unknown-base
  compatibility fallback or old-consumer generation conversion is needed.

## Phase 0 — Establish evidence and settle contracts

- [x] **T01 — Capture a matched implementation baseline**: retain release
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
- [ ] **T04 — Remove unpublished format/service compatibility**: the user
  confirms no product release or supported persisted old format exists. Remove
  old raw Chunk/Stream decoders, frozen schema copies, directory conversion,
  migration guards/progress/source keys, old-server sync fallback and revision-1
  reader/tree support. Keep one canonical frame format and current directory
  schema; malformed/unknown records and missing sync proof fail explicitly.
  Wire the grouped/indexed worker and delete its predecessor together. Retain
  new-format reopen/fault tests and unchanged performance gates. No format
  negotiation or per-chunk capability fields are needed for matched services.
  Files: protocol frame/metadata codecs, Stream directory/worker, object readers,
  DiskIO semantic writes, native tree catalog/frame codecs and affected tests.

## Phase 1 — Combine write and synchronization

- [x] **T05 — Add the synchronized wire contract**: append the sync option
  without reusing retired slots; update contiguous and view wire constructors
  and protocol tests. Exercise both generated Rust and C++ schemas through
  their existing generators; do not hand-edit generated files. Apply T04's
  compatibility rule. Maps to D02/A02. Files:
  `lib/crowdb-protocol/src/fbs/diskio.fbs`,
  `lib/crowdb-protocol/build.rs`, `lib/crowdb-rpc/CMakeLists.txt`,
  `lib/crowdb-diskio-client/src/client.rs`,
  `lib/crowdb-protocol/tests/diskio_wire_test.rs` (new).
- [x] **T06 — Complete write/flush through one server response**: make
  `DiskioServer::handle_write` await ordered write and, when requested, flush
  before its sole reply. Retain buffer/disk/connection lifetimes through both
  callbacks. Short/failed writes and failed flushes cannot produce success or
  double completion. Keep buffered behavior and independent fsync. Maps to
  D02/A02. Files: `app/crowdb-diskio/src/rpc/dio_server.cpp`,
  `app/crowdb-diskio/src/rpc/dio_server.h`,
  `app/crowdb-diskio/tests/dio_server_write_test.cpp` (new),
  `app/crowdb-diskio/CMakeLists.txt`.
- [x] **T07 — Propagate synchronization through mirror writes and repair**:
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
  coverage, strict current-format handling, checked sizes, and geometry helpers.
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
  T04's single-format rules and purpose/identity checks. Maps to D01/D07,
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
- [~] **T15 — Implement bounded tail observation and scanning**: establish a
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
  CRC coverage, incomplete/corrupt input, unsupported revisions, and length bounds.
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
seam integration complete.

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
