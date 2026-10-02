<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Iceberg Small Write Plan

Upstream: [R197](../backlog/R197-iceberge-small-write.md),
[native design](../design/access-server/iceberge/design-crowdb-iceberg.md),
[upload flow](../design/access-server/iceberge/design-crowdb-iceberg-upload-flow.md).

Goal: remove shared and repeated KV work from normal Iceberg operations while
retaining one safe publication point per table or file and explicit cleanup.

## Phase 1: Baseline and independent table creation

- [~] **Baseline and work preservation**: inventory the current partial edits,
  run focused tests, and record KV counts and the 32-worker failure before
  changing semantics. Files: `app/crowdb-access-server/tests/iceberg_file_http_test.rs`,
  this plan.
- [~] **Create authority**: remove per-create namespace admission writes and
  repair the in-progress `FilesReady` transition; keep same-name publication
  conditional and prevent stale namespace identity from becoming visible.
  Files: `lib/crowdb-access-iceberg/src/commit/create/`,
  `lib/crowdb-access-iceberg/src/namespace/`.
- [ ] **Create verification**: race same-name and distinct-name creates,
  namespace drop, retry, and crash recovery. Files:
  `lib/crowdb-access-iceberg/tests/`,
  `app/crowdb-access-server/tests/iceberg_file_http_test.rs`.

## Phase 2: One table commit point

- [ ] **Commit publication**: remove the steady-state phase journal and
  post-publication head settlement where the published head and operation
  identity determine the outcome. Keep CAS on the table head and exact
  uncertain-response recovery. Files: `lib/crowdb-access-iceberg/src/commit/`,
  `lib/crowdb-access-iceberg/src/table/`.
- [ ] **Commit validation**: remove normal-path manifest and per-file proofs;
  keep request and base-version checks in memory. Files:
  `lib/crowdb-access-iceberg/src/commit/`,
  `app/crowdb-access-server/src/iceberg/table_write.rs`.
- [ ] **Commit verification**: exercise same-head races, lost responses,
  retries, and no per-file GET/write on publication. Files:
  `lib/crowdb-access-iceberg/tests/`, `app/crowdb-access-server/tests/`.

## Phase 3: File publication and multipart

- [ ] **Single location record**: store the complete `FileRecord` at its
  location key, CAS only when absent, use the conflict value for same-content
  retries, and read with one file GET. Migrate GC and multipart callers from
  the file-ID record and mapping pair without losing old-record recovery.
  Files: `lib/crowdb-access-iceberg/src/file/`,
  `lib/crowdb-access-iceberg/src/record/`,
  `lib/crowdb-access-iceberg/src/gc/`.
- [ ] **Multipart admission**: remove catalog-wide credit reserve/release;
  keep per-session state, capacity limits, completion idempotency, and
  create-only publication. Files: `lib/crowdb-access-iceberg/src/file/`.
- [ ] **File verification**: direct PUT, GET, multipart, same-path races,
  restart, and prefix enumeration with KV-operation assertions. Files:
  `lib/crowdb-access-iceberg/tests/`,
  `app/crowdb-access-server/tests/iceberg_file_http_test.rs`.

## Phase 4: Cleanup and resource budgets

- [~] **Live GC split**: retire automatic deletion of published files solely
  for missing snapshot references. Retain the scanner and recovery for
  expired multipart sessions, unpublished parts, and abandoned assembly
  blocks; remove foreground deletion-fence/claim GETs only where the new
  cleanup protocol makes them unnecessary. Verify Chunk-KV WAL ownership
  before removing any per-block write intent. Files:
  `lib/crowdb-access-iceberg/src/file/`,
  `lib/crowdb-access-iceberg/src/gc/`,
  `app/crowdb-access-server/src/iceberg/gc_runtime.rs`.
- [ ] **Explicit object cleanup**: add separately authorized S3
  `DeleteObject` and `DeleteObjects` with bounded retained-reference scans,
  per-key results, conditional deletion state, and deferred block reclaim.
  The client no-future-reference contract is documented in R197. Files:
  `lib/crowdb-access-iceberg/src/file/`,
  `lib/crowdb-access-iceberg/src/gc/`,
  `app/crowdb-access-server/src/iceberg/file_*.rs`.
- [~] **Drop purge scheduling**: preserve `purgeRequested=false`; for true,
  persist drop time in the existing marker and start bounded purge work after
  20 minutes, even across restart or delayed admission. Separate this from
  other GC retention and keep the worker active without live orphan sweep.
  Files: `lib/crowdb-access-iceberg/src/table/lifecycle/`,
  `lib/crowdb-access-iceberg/src/gc/`,
  `app/crowdb-access-server/src/iceberg/gc_runtime.rs`.
- [ ] **Resource and read audit**: replace the fixed four-request spool gate
  with a budget for its actual resource and remove repeated root, head,
  session, journal, and post-success GETs. Record any indispensable extra
  read with its concrete failing interleaving before retaining it. Files:
  `app/crowdb-access-server/src/iceberg/`,
  `lib/crowdb-access-iceberg/src/`.
- [ ] **Cleanup verification**: test retained references, mixed S3 deletion,
  expired multipart sessions and unpublished parts, assembly restart,
  active reads, drop purge, retry, restart, and same-name re-creation. Files:
  `lib/crowdb-access-iceberg/tests/`, `app/crowdb-access-server/tests/`.

## Phase 5: Measurement, gates, and cleanup

- [ ] **Measured comparison**: use real HTTP and TPC-H/TPC-DS loader
  profiles at 1 and 32 clients; compare KV reads/writes, 503s, throughput,
  and p50/p95/p99 by phase. Files: focused workload/test artifacts and this
  plan.
- [ ] **Architecture and gates**: update the permanent Iceberg design, run
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

- The implementation snapshot is being committed before the final acceptance
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
  proof contract; their updated targeted runs pass. The 32-worker loader,
  full GC acceptance, and latency/KV comparison remain for the post-commit
  acceptance pass.

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
