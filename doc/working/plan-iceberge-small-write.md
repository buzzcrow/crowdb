<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Iceberg Small Write Plan

Upstream: [R197](../backlog/R197-iceberge-small-write.md),
[native design](../design/access-server/iceberge/design-crowdb-iceberg.md),
[upload flow](../design/access-server/iceberge/design-crowdb-iceberg-upload-flow.md).

Goal: remove shared and repeated KV work from normal Iceberg operations while
retaining one safe publication point per table or file and explicit cleanup.

## Current focus: Native small-object transfer

The user resumed consolidated acceptance and GC verification after the current
implementation. Preserve per-pipeline publication ordering; optimize routing,
owner slicing, asynchronous prefetch and exact receive allocation.

- [x] **Owner-backed small batches**: accept complete framed owners in the
  shared object writer; place multi-frame objects without forced rotation;
  set chunk IDs in the worker; send owner views through offset-aware DiskIO
  scatter/gather; retain views for repair and release them on strip close.
  Files: `lib/crowdb-chunk-client/src/writer/`, `io.rs`, `disk_io/`,
  `chunk/mirror_flow.rs`, `client.rs`.
- [x] **Small receive and checksums**: enable native owner handoff for small
  Iceberg uploads, use small frame magic, and compute checksums synchronously
  within the per-object upload writer without a blocking task or digest queue.
  Files: `app/crowdb-access-server/src/iceberg/file_http/stream.rs`,
  `upload_flow/`, `lib/crowdb-access-s3/src/native_buffer.rs`.
- [x] **Configured prefetch**: expose `small_strip_prefetch_count` alongside
  256 MiB `chunk_capacity_bytes` and 32-strip groups refilled at half; audit refill and metadata barriers
  for unnecessary demand-path waits. Files: service `config.rs`, chunk-client
  `writer/small_pipeline.rs`.
- [x] **Small profiles**: prepare 1 KiB, 512 KiB, and labelled 65,502-byte
  aligned cases with one object handoff, expected frame count, zero payload
  copying on native mirror success, shared-strip packing, and batch metrics.
  Compare the implementation with the preceding committed snapshot.

## Phase 1: Baseline and independent table creation

- [x] **Baseline and work preservation**: inventory the current partial edits,
  run focused tests, and record KV counts and the 32-worker failure before
  changing semantics. Files: `app/crowdb-access-server/tests/iceberg_file_http_test.rs`,
  this plan.
- [x] **Create authority**: remove per-create namespace admission writes and
  repair the in-progress `FilesReady` transition; keep same-name publication
  conditional and prevent stale namespace identity from becoming visible.
  Files: `lib/crowdb-access-iceberg/src/commit/create/`,
  `lib/crowdb-access-iceberg/src/namespace/`.
- [x] **Create verification**: race same-name and distinct-name creates,
  namespace drop, retry, and crash recovery. Files:
  `lib/crowdb-access-iceberg/tests/`,
  `app/crowdb-access-server/tests/iceberg_file_http_test.rs`.

## Phase 2: One table commit point

- [x] **Commit publication**: remove the steady-state phase journal and
  post-publication head settlement where the published head and operation
  identity determine the outcome. Keep CAS on the table head and exact
  uncertain-response recovery. Files: `lib/crowdb-access-iceberg/src/commit/`,
  `lib/crowdb-access-iceberg/src/table/`.
- [x] **Commit validation**: remove normal-path manifest and per-file proofs;
  keep request and base-version checks in memory. Files:
  `lib/crowdb-access-iceberg/src/commit/`,
  `app/crowdb-access-server/src/iceberg/table_write.rs`.
- [x] **Commit verification**: exercise same-head races, lost responses,
  retries, and no per-file GET/write on publication. Files:
  `lib/crowdb-access-iceberg/tests/`, `app/crowdb-access-server/tests/`.

## Phase 3: File publication and multipart

- [x] **Single location record**: store the complete `FileRecord` at its
  location key, CAS only when absent, use the conflict value for same-content
  retries, and read with one file GET. Migrate GC and multipart callers from
  the file-ID record and mapping pair without losing old-record recovery.
  Files: `lib/crowdb-access-iceberg/src/file/`,
  `lib/crowdb-access-iceberg/src/record/`,
  `lib/crowdb-access-iceberg/src/gc/`.
- [x] **Multipart admission**: remove catalog-wide credit reserve/release;
  keep per-session state, capacity limits, completion idempotency, and
  create-only publication. Files: `lib/crowdb-access-iceberg/src/file/`.
- [x] **File verification**: direct PUT, GET, multipart, same-path races,
  restart, and prefix enumeration with KV-operation assertions. Files:
  `lib/crowdb-access-iceberg/tests/`,
  `app/crowdb-access-server/tests/iceberg_file_http_test.rs`.

## Phase 4: Cleanup and resource budgets

- [x] **Live GC split**: retire automatic deletion of published files solely
  for missing snapshot references. Retain the scanner and recovery for
  expired multipart sessions, unpublished parts, and abandoned assembly
  blocks; remove foreground deletion-fence/claim GETs only where the new
  cleanup protocol makes them unnecessary. Verify Chunk-KV WAL ownership
  before removing any per-block write intent. Files:
  `lib/crowdb-access-iceberg/src/file/`,
  `lib/crowdb-access-iceberg/src/gc/`,
  `app/crowdb-access-server/src/iceberg/gc_runtime.rs`.
- [x] **Explicit object cleanup**: add separately authorized S3
  `DeleteObject` and `DeleteObjects` with bounded retained-reference scans,
  per-key results, conditional deletion state, and deferred block reclaim.
  The client no-future-reference contract is documented in R197. Files:
  `lib/crowdb-access-iceberg/src/file/`,
  `lib/crowdb-access-iceberg/src/gc/`,
  `app/crowdb-access-server/src/iceberg/file_*.rs`.
- [x] **Drop purge scheduling**: preserve `purgeRequested=false`; for true,
  persist drop time in the existing marker and start bounded purge work after
  20 minutes, even across restart or delayed admission. Separate this from
  other GC retention and keep the worker active without live orphan sweep.
  Files: `lib/crowdb-access-iceberg/src/table/lifecycle/`,
  `lib/crowdb-access-iceberg/src/gc/`,
  `app/crowdb-access-server/src/iceberg/gc_runtime.rs`.
- [x] **Resource and read audit**: replace the fixed four-request spool gate
  with a budget for its actual resource and remove repeated root, head,
  session, journal, and post-success GETs. Record any indispensable extra
  read with its concrete failing interleaving before retaining it. Files:
  `app/crowdb-access-server/src/iceberg/`,
  `lib/crowdb-access-iceberg/src/`.
- [x] **Cleanup verification**: test retained references, mixed S3 deletion,
  expired multipart sessions and unpublished parts, assembly restart,
  active reads, drop purge, retry, restart, and same-name re-creation. Files:
  `lib/crowdb-access-iceberg/tests/`, `app/crowdb-access-server/tests/`.

## Phase 5: Measurement, gates, and cleanup

- [x] **Measured comparison**: use real HTTP and TPC-H/TPC-DS loader
  profiles at 1 and 32 clients; compare KV reads/writes, 503s, throughput,
  and p50/p95/p99 by phase. Files: focused workload/test artifacts and this
  plan.
- [~] **Architecture and gates**: update the permanent Iceberg design, run
  `pixi run rs-fmt-check`, `pixi run rs-lint`, affected test tasks, and the
  focused cluster workload separately. Files:
  `doc/design/access-server/iceberge/`, touched Rust files.
- [ ] **Final cleanup**: remove this plan, R197 detail, and its backlog index
  entry after all acceptance checks pass; commit the cleanup. Files:
  `doc/working/plan-iceberge-small-write.md`,
  `doc/backlog/R197-iceberge-small-write.md`, `doc/backlog/backlog.md`.

## Consolidated Files

- `lib/crowdb-access-iceberg/src/commit/`, `namespace/`, `table/`, `file/`,
  `gc/`, and `record/`.
- `app/crowdb-access-server/src/iceberg/` and its focused integration tests.
- `lib/crowdb-access-iceberg/tests/`, the permanent Iceberg design, R197,
  the backlog index, and this plan.

## Tests

- Unit: location-key encoding, state transitions, and GC deadline arithmetic.
- Integration: same-name/different-name creation, one-head commit, file
  publication, cleanup, drop purge, and crash recovery.
- E2E: S3 direct/multipart and delete protocol, TPC-H/TPC-DS 32-worker
  small-cluster run, and per-phase KV/latency comparison.

## Findings

- The first implementation snapshot is commit `2b599b64`. Post-commit
  acceptance ran the complete Iceberg library suite, the service GC control
  and capacity cases, retained-reference HTTP deletion, and a 32-worker
  TPC-H/TPC-DS small-cluster load. The loader completed all 8 TPC-H and
  24 TPC-DS tables without a 503; earlier failed runs completed only 4/8
  and 17/24, so their upload latency is not a comparable baseline.
- The native 5 MiB HTTP profile starts the Iceberg GC disabled. Its last
  run measured PUT 237 ms with 3 catalog GETs and 1 CAS, UploadPart 205 ms
  with 24 GETs and 3 scans in the cluster-wide counter window, and GET
  37 ms with 4 GETs. Concurrent Chunk-KV maintenance can contribute to
  these aggregate counters. The 5 MiB PUT and UploadPart each recorded
  zero completed small writes, six successful large strips, and six writer
  feeds; the published PUT used one Chunk location. The single-node
  fixture uses a 1 MiB mirror strip and a 0.9 MiB routing boundary. The
  HTTP receive path aggregates socket frames in a 1 MiB native buffer;
  the older `FileTreeWriter` 64 KiB leaf path is not used by these requests.
  A deterministic store-count test observes one part-record GET and one
  CAS per independent UploadPart; the earlier session reload has been
  removed.
- The GC scheduler now defaults to one scan every 30 minutes after an
  immediate startup scan. The sample config uses the same interval, while
  test processes explicitly use 100 ms. A 20-minute purge deadline is an
  eligibility threshold; the periodic scheduler may start it up to another
  scan interval later. Catalog status waits remain capped at 60 seconds.
- The implementation snapshot was committed before the final acceptance
  pass so later workload and GC fixes can be compared as a separate change.
  Ordinary existing-table updates write immutable metadata and publish with
  one `TableHead` CAS; an old retry hidden by a later generation may return
  an uncertain outcome. File publication uses one location-key CAS. Explicit
  cleanup uses a tombstone and a durable GC candidate before allowing same-path
  reupload. Its extra KV work is limited to the requested heavy delete path.
- The implementation gate passed `rs-fmt-check`, `rs-lint`, seven changed Iceberg library
  test targets, and the native HTTP single/batch deletion and same-path reupload
  test. The initial full library run exposed seven test targets that still
  asserted the superseded dual-key, foreground GC fence, or deep staged-file
  proof contract; their updated targeted runs pass. The post-commit full
  library suite and service GC control suite also pass. A request-isolated
  cluster latency comparison remains open; the aggregate counter window
  includes Chunk-KV maintenance.

- Baseline `pixi run test-access-iceberg` failed two create tests and hung on
  an old test barrier that required the removed namespace admission write.
  The create phase transition and validation still required admission.
- Current `file_recovery` already scans and aborts expired multipart sessions.
  The GC runtime retires all live-table GC tasks, so physical multipart parts
  and abandoned assembly blocks for an active catalog still need their own
  safe, bounded cleanup path.
- The lower disk leak scanner reports deferred when caller ownership is not
  available; it cannot replace durable `FileWriteIntent` proof yet.
- The create path now bypasses shared namespace admission, supports the direct
  `FilesReady -> Publishing` transition, and no longer rereads metadata after
  writing it. The removed namespace-write barrier test was updated to the
  accepted concurrent-drop semantics.
- The purge marker now stores its durable creation time; the GC task derives
  its 20-minute deadline from that marker rather than scheduler admission.
  Terminal table operations and terminal multipart sessions do not extend
  purge to the one-day retry window; unfinished work still blocks reclamation.
- A separate active-catalog multipart cleanup task now scans only terminal,
  expired multipart parts and assembly checkpoints. It retains bounded GC
  candidate cursors, then removes session payload pages and the terminal
  session record after all parts and assembly checkpoints are gone. It does
  not scan published file records. A trial of
  active-catalog `FileWriteIntent` sweeping was removed: a `Publishing`
  multipart session may still own its final-file blocks before a file record
  appears. Retired-catalog and purge write-intent cleanup remains. Active
  orphan intents need an exact owner proof before reclaim; replaced part
  revisions and the future single-key file migration need the same audit.
- Focused library tests cover active-catalog expired parts and aborted assembly,
  session/payload cleanup, and published-file retention. The complete Iceberg
  library suite and service GC control/capacity suites passed before the final
  terminal-protection adjustment; rerun them at the final gate.
- The 32-client direct PUT server test completed with 32 HTTP 200 responses.
  The concurrent TPC-H/TPC-DS loader run failed: several table creates returned
  `ServiceUnavailableException: Catalog is not ready`; other tables committed.
  During the same run, multipart terminal credit release repeatedly reported
  catalog maintenance or bounded capacity busy. These are observed signals,
  not yet a proven single root cause. No small-file latency or KV-count
  conclusion is drawn from the 8 MiB direct PUT test.
- New multipart sessions now use their own durable session record without
  catalog-wide credit reservation or terminal release. Legacy credit records
  still recover in the background. Re-run the loader to distinguish the
  remaining table-create failure from this removed contention point.

## Current implementation checkpoint

- Native small-object receipt directly hands one complete framed owner to the
  object writer without the generic transfer channel. MD5/SHA-256 is computed
  inline. Exact owners are 1,058 bytes for 1 KiB and 524,594 bytes for 512 KiB.
  Receive admission refuses physical sizes larger than its aggregate budget.
- Owner views cross strip boundaries without payload copying or forced strip
  rotation. Per-pipeline disk -> readable cursor -> next batch remains ordered.
- Hash admission probes other routes atomically. Default shared capacity is
  256 MiB; mirror refill allocates 32 strips at 16 remaining, appending after
  the entire previous group. Allocation runs independently of cursor updates
  and outside the existing lifecycle guard. Active chunk lease renewal protects
  prefetched reservations and prepared replacement chunks.
- HTTP parser prefix copies remain unchanged and use one bandwidth metric
  recording count and bytes. AWS chunk encoding keeps its decoding fallback.
- Production rs-lint and feature-enabled Access Server clippy passed. The initial
  61 focused tests passed; the 18 real-process shared writer cases passed,
  covering crossing strips, repair, rotation, EC conversion and restart.
  The real ChunkDB hidden-reservation append/lease test passed.
- The baseline snapshot is cd5bede8, measured in a separate managed worktree.
  Identical HTTP profiles use 128 uploads each, 1 and 32 concurrent requests,
  1 KiB, 512 KiB and 65,502-byte aligned payloads, with every file read back.
  Baseline results are retained under target/r197-validation/baseline-small.log.
  Baseline setup required explicit native binary paths and the harness FFI
  library; these were environment failures before request execution.
- Next: run current matching HTTP profiles, consolidate library/GC acceptance,
  update permanent architecture and performance evidence, commit implementation,
  then remove completed backlog and plan entries only after all required gates.

## Remaining flow constraints

- EC conversion reserves a complete data-width group with parity ownership;
  halfway 32-strip refill applies to mirror-only pipelines. Conversion stays
  disabled in the single-node fixture and is tested with multiple nodes.
- Legacy tree multipart assembly retains bounded copying and durable checkpoint
  ownership. Streamed native files compose locations rather than assembling
  another payload. Published unreferenced files are never automatically swept.
- Full chunk metadata publication still uses the existing lifecycle guard;
  allocation is outside it and revalidates before publishing. No new lock was
  added. Ambiguous publication retains physical allocations for recovery.

## Real-protocol aggregation failure and fix

- First current HTTP profile passed 1 KiB at concurrency one, then failed at
  concurrency 32: a 29-object aggregate reached the RPC transport's fixed
  16-view bound, was misreported as an ambiguous disk write and entered repair.
  The first divergence was descriptor admission before DiskIO submission.
- DiskIO semantic view writes now validate the whole range before any IO and
  submit consecutive at-most-16-view frames by reference, with one final fsync
  when requested. This retains payload owners, uses no payload coalescing,
  changes no KV or cursor publication, and preserves the transport's existing
  descriptor memory bound. The strip batch completes only after every frame.
- Regression exercises 33 views, contiguous readback, exactly three writes and
  one fsync, and oversized-range rejection before any write. HTTP profiles
  additionally assert measured native frame count and one receive allocation
  per object, and report accepted prefix-copy bytes separately.

- The first successful current matrix improved 512 KiB at 32 clients from
  7.074 s to 3.369 s, but regressed 1 KiB from 0.388 s to 0.816 s. Baseline
  repeat after route warmup still used one pipeline, with no reservation in the
  1 KiB measurement. The 128 queued-object scale-out threshold exceeds this
  32-client case, so this is not extra pipeline initialization.
- Sequential transport subdivision adds a completion round trip per 16 views.
  Disjoint ranges within one strip batch now overlap at bounded depth four,
  respecting a lower configured semantic admission limit. An error stops new
  subdivision and drains submitted writes before returning; fsync and cursor
  publication still follow the complete batch. A regression test observes two
  simultaneous operations with artificial DiskIO latency and exact readback.
- Consolidated server acceptance found one stale configuration assertion for
  the former 1 GiB default; it now checks 256 MiB and 32 strips. This is an
  expected contract update, not a weakened runtime assertion.

- Final matching warmup matrix passes all six groups with 768 measured PUTs
  and readbacks, expected frame/allocation/handoff counters, and zero large
  routes. At 32 clients, 512 KiB improves 9.575 -> 4.337 s, aligned payloads
  1.083 -> 0.986 s; 1 KiB is 0.393 -> 0.421 s (7.1% sample regression).
  The 1 KiB sequential sample is unchanged. The permanent upload-flow report
  records all percentiles, aggregate KV counts and limitations. No claim of
  universal speedup is made. File publication remains the dominant summed
  1 KiB stage. Pending shared admission now terminates on pool shutdown rather
  than polling closed routes indefinitely, with a focused regression test.

## Implementation commit checkpoint

- Full Iceberg library acceptance passed. Current quality gates passed production
  rs-lint and feature-enabled service clippy. Focused gates passed 12 native
  receive tests, 10 frame tests, 41 small-pool tests, RPC owner-buffer tests and
  real DiskIO semantic overlap/readback, five reservation/GC lease cases and
  five real ChunkDB append/seal/delete concurrency cases.
- Commit the native small-object implementation before consolidated service,
  shared-writer and loader acceptance. Any acceptance fixes become separate
  commits so they can be compared to this implementation snapshot.

- Native implementation snapshot is c27f6fce. Consolidated service acceptance
  is running with iceberg-e2e enabled and serial test execution. Minor permanent
  document reconciliation remains in the final cleanup commit.

- Protocol audit found that an older or inconsistent reservation backend could
  ignore the appended offset and return a group overlapping hidden strips.
  The client now verifies the returned first offset against its request in
  memory before accepting the group, without an extra KV read. An injected
  old-backend reply must fail before any overlapping object write/publication.

- Service HTTP acceptance completed nine real-process cases, including 32 direct
  PUTs, multipart restart, explicit cleanup, size routing and slow sockets. The
  next parser test still expected all object DELETE routes to be unsupported;
  it now distinguishes object cleanup from multipart abort. Authorization is
  verified by the existing native HTTP cleanup cases. Continue remaining server
  targets without repeating the already passed long HTTP matrix.
- The injected bad-refill test blocks resource allocation until the preceding
  group is fully confirmed, then corrupts only the returned offset. This avoids
  the mock's strict metadata-revision fence causing an unrelated stale-writer
  failure before the intended offset-validation assertion.

- Consolidated acceptance fixes: updated DELETE parsing and the file-read barrier
  to include complete location-key records; the barrier now again verifies that
  concurrent table commits cannot return mixed metadata. Request construction
  reserves the bounded response budget, then shrinks to the retained Vec capacity
  after serialization; table reads reserve their actual 4 MiB response ceiling.
  Namespace/list/commit-body tests exhaust byte budgets rather than four slots.
- The bad-refill regression now fills the initial strip plus all four reserved
  strips before accepting the injected response. Its first true failure exposed
  a worker shutdown hang: the pre-batch error path drained an open receiver.
  Closing the receiver first terminates admission and safely fails queued work.
  The focused regression and all 42 small-pool tests pass.

- Full default server acceptance exposed current-head retries incorrectly returning
  uncertain despite a visible successful operation. Added one optional 32-byte
  binding to the existing TableHead, hashing principal plus route/body digest.
  The sole head CAS stores it; visible matching retries read the selected immutable
  metadata and replay, changed input conflicts, hidden old outcomes remain uncertain.
  No phase journal, extra commit KV write or existence GET is added. Old heads
  without a binding remain conservatively uncertain. Failed requirements leave no
  journal, and manifest proofs remain absent as designed.

- Post-fix complete default Access Server and complete Iceberg library suites pass.
  Native file storage restart passes. With the pinned Python environment, all three
  native catalog/namespace restart and operation-budget cases pass; all nonignored
  GC control cases and simulated-full-disk capacity recovery pass. Existing
  malformed-record injection and retry deferrals are retained in the GC suite.
  Remaining final acceptance: 18 shared-writer E2E cases and final loader/5 MiB
  routing runs. The baseline worktree has been archived with its snapshot.
