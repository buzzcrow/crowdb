# Iceberg Reclamation Plan

Upstream: [R183](../backlog/R183-access-iceberg-reclamation.md).

Goal: implement bounded, restartable reclamation with durable reachability proof,
exclusive-chunk deletion and shared-chunk range deletion dispatch.

## Execution

- [x] **Exclusive chunk deletion**: ownership-checked client dispatch and native
  evidence of disk segment release before layout removal. Files:
  `lib/crowdb-chunk-client/src/reclamation.rs`, `app/crowdb-chunkdb/tests/full_stack_test.rs`.
- [x] **Shared range contract**: verify the confirmed independent u32 byte fields
  dispatch exact ranges through the existing API without implementing range reclamation.
  Preserve unsupported work. Files: chunk-client, protocol and FileIO blocks.
- [x] **Durable GC records and bounds**: task, candidate, traversal, deletion intent,
  pins, retention, retry and progress records with conditional updates. Files:
  `lib/crowdb-access-iceberg/src/gc/`, `lib/crowdb-protocol/src/fbs/iceberg.fbs`.
- [x] **Publication and reader fences**: published/staged credentials, direct FileIO,
  metadata loads and file publication register durable protection. Request and
  clock-skew bounds come from catalog authority; deleting candidates cannot be
  read or republished. Final head fencing is followed by another root scan.
  Files: `gc/protection.rs`, `file/repository.rs`, `table/load.rs`, Access Server admission.
- [~] **Candidate discovery**: durable bounded scans for purge, retired catalogs,
  abandoned operations/uploads, expired bindings and orphan generations; retain
  active-root and retry-result dependencies. File records and expired terminal
  multipart parts have durable candidates. Abandoned assembly checkpoints and
  writes without a published authority still need their own bounded source.
  Files: GC repository and discovery.
- [x] **Canonical reachability**: current and pinned historical metadata are parsed
  against captured heads. An immutable traversal stack and compressed binary
  mark index are content-addressed; one task CAS publishes both continuations.
  Missing frames/pages fail closed, including when proving nonmembership.
  Retained operations and table-wide upload/credential pins conservatively defer
  the pass. Files: `gc/proof/`, `gc/worker/live.rs`, `record/gc.rs`.
- [ ] **Deletion worker**: revalidate fences and retention, persist children before
  deleting directory roots, dispatch exclusive/range deletion, conditionally remove
  records, retain uncertain outcomes and quarantined corruption. The retired pass
  checks system bindings before initial and final file scans, then removes expired
  primary/overflow retry and management/audit records and non-GC catalog records.
  GC records and authority remain for terminal cleanup. Files: GC worker.
- [ ] **Operator and runtime integration**: authenticated pause/resume/inspect,
  pin/unpin, rate and retry controls; separate budgets and background progress.
  Files: Access Server Iceberg runtime/config/management.
- [ ] **Acceptance and cleanup**: verify crash/resume, races, resource isolation,
  capacity exhaustion/recovery, SDK foreground regressions and required gates;
  update permanent architecture and close only demonstrated acceptance.

## Storage findings

- Existing ChunkDB delete persists a Deleted tombstone with strips before freeing
  blocks; failed cleanup remains retryable and strips are cleared after release.
- The range-delete RPC currently reports Unimplemented. Keep pending work and
  expose that status until the independent shared-range storage work lands.
- Current native Iceberg file blocks all use small-write shared chunks, including
  large files represented as bounded trees. File length does not prove exclusive
  ownership. Exclusive deletion requires storage ownership evidence.

## Verification

- Live proof/protection: 11 focused tests pass for v1/v2/v3 graph traversal,
  manifest/status/DV/statistics links, historical readers, missing proof/stack
  pages, restart and lost replies, late credentials, in-flight file publication,
  persisted request/skew bounds and selective live-file sweep. The final library
  all-target run passes 659 tests. Access Server's Iceberg-enabled all-target suite
  passes 80 tests, including durable staged-credential pin assertions. Workspace
  fmt and affected library/server all-target clippy pass with warnings denied.
  SDK/engine tests behind separate feature gates are not claimed by this run.
- Retired candidate/record cleanup: terminal multipart parts use their sealed tree
  as a candidate with persisted request/skew grace and source revalidation before
  physical steps. Seventeen GC worker tests cover pending system bindings before
  and after the first protection scan, primary/overflow collisions, active-root
  management replay, audit and orphan projection cleanup, and retaining aborted
  assembly checkpoints. The library all-target suite passes 665 tests and the
  Iceberg-enabled server suite passes 80 tests. Workspace fmt and affected
  all-target clippy pass with warnings denied.

- Chunk-client deletion dispatch: 5 focused tests passed (exclusive ownership,
  failed-delete retry, unsupported shared ranges, invalid/active chunks and exact
  unaligned byte ranges, 256 MiB/1 GiB endpoints and protocol overflow rejection).
- GC task/page codecs and bounds: 3 focused tests passed.
- Directory deletion cursor: 2 tests passed, including persisted pending deletion
  replay after child bytes disappear and corruption before child discovery.
- Reader/head fencing: 3 tests passed before the inactive worker integration.
- Avro OCF checkpoint/resume: the 8-test existing framing suite passed, including
  the new across-block SHA state and corrupt-checkpoint checks.
- Native ChunkDB full-stack allocate/seal/delete passed against real KV and DiskDB
  services; the test checks every captured segment is free after the chunk layout
  is cleared. No native test skip was used.
- Inactive worker: the existing 9 tests cover restart at every step, reader/purge fencing, deferred ranges,
  lost delete replies, timeout admission release, corruption quarantine and
  repeated sweep accounting, retention starting at candidate discovery and
  rediscovery of files arriving after the initial scan.
  Completion currently covers files,
  not the retired catalog's full record range.
- Canonical ownership: 3 focused tests pass for cross-generation deduplication,
  lost claim/candidate replies and invalid authority rejection. A worker test
  passes for retirement adopting a paused/resumed purge's existing pending cursor
  after the child block was physically removed; stale-owner progress is rejected.
  Missing claims fail closed before physical deletion. All 11 worker tests and
  3 claim tests pass; affected-crate all-target clippy passes with warnings denied.
  These cases remain covered by the current full library run.
- Canonical link extraction: 3 tests cover v1/v2/v3 metadata references, bounded
  and foreign inputs, paginated manifest entries and DV referenced data files.
  These extraction tests supplement the current authenticated live-table proof tests.
- The Iceberg library all-target suite passed after fixing stale prepared commit
  publication to settle its original conflict before attempting file publication.
  Final rerun after the latest retention/sweep changes also passed.
- Workspace fmt check and clippy for the Iceberg library, chunk client and Access
  Server (with `iceberg` enabled, all targets, warnings denied) passed.
  GC is not wired into the Access Server runtime.
- Access Server `--features iceberg --all-targets` passed, including table reads,
  delegated credentials, lifecycle and commit HTTP regressions. The default
  feature gate alone runs no Iceberg tests and is not Iceberg acceptance evidence.

## Remaining integration

- The live worker now consumes the immutable proof and rechecks its table fence
  before candidate deletion. Old mutable mark/pending enumeration is removed from
  the traversal API. The mark index has at most 128 branch decisions per file ID;
  neither the traversal stack nor the index is loaded as a whole graph.
- Request, credential and publication pin integration is complete. Overflowing
  lifetimes fail admission; operation/multipart grace uses persisted request/skew
  bounds. A credential admitted between initial proof and final fencing cancels
  that pass and releases the table. A completed live pass can leave retained or
  deferred candidates for a subsequent task; Complete does not mean all files
  were reclaimed.
- Inactive worker acquires the purge head fence before scanning pins, recovers
  a lost final release reply, and preserves deferred shared-range deletion intents.
- Range units are confirmed: offset and length remain independent u32 byte
  fields. Dispatch exact unaligned frame ranges; reject overflow without issuing
  deletion. Unsupported responses retain Deferred work; no shared chunk fallback.
- Worker `run` now has lock-free independent concurrency admission, a per-step
  timeout, durable backoff/quarantine, and a separate timeout for recording recovery
  progress. Complete CPU/I/O/rate budgeting and runtime/control wiring remain.
- `reclaimed_bytes` counts completed files' logical lengths, not actual freed disk
  allocation; inline files and parity make those different metrics. Shared range
  deferral does not count as completion. Sweep-round receipts prevent recounting
  a completed candidate during later rounds and survive control-only revisions.
- Scope metadata-log retention to its retained metadata files; use explicit reader
  pins as historical snapshot roots. The pinned Java 1.11.0 `ReachableFileUtil`
  distinguishes recursive metadata enumeration from snapshot/data traversal.
- Finish generation/operation/projection cleanup, system retry-slot/overflow cleanup,
  authenticated controls and independent runtime admission. Keep R183 open until
  the complete acceptance matrix has executable evidence.
- File-scoped immutable GcClaim records now select one generation-indexed candidate.
  Discovery reuses that record without resetting progress or retention. Sweep
  verifies the claim before dispatch. Under inactive authority, retirement can
  adopt an unfinished live/purge candidate; purge can adopt a live candidate.
  Adoption preserves the exact pending cursor and extends, never shortens,
  retention. Paused/quarantined owners are not automatically adopted. Remaining
  work includes operator recovery and stale live-task cancellation; this is
  not permission to enable background GC yet.
- Inactive tasks now persist a bounded Rescan phase after protection checks and
  before each sweep. Purge retains/revalidates its table fence; retirement checks
  remain mandatory. Rescan discovers files that landed after the original scan,
  preserving existing deletion cursors and retention deadlines.

- Before runtime activation, bound table-fence occupancy and validate foreground
  availability under large sweeps. Complete resource accounting across proof KV
  writes and chunk reads, cancellation/recovery controls and scheduler fairness.
- Performance follow-up: metadata is reparsed per bounded link batch and shared
  manifests can be revisited across snapshot roots. Keep the bounded proof and
  publication semantics when optimizing these paths; measure in the separate
  performance project before selecting caches or batched storage changes.
- Multipart assembly checkpoints contain a bounded frontier of chunk roots in a
  separate `ICFW` block. Terminal sessions retain that block and its child trees
  until a durable per-root cursor can verify and reclaim them. Writes that fail
  before any durable FileRecord or checkpoint have no catalog candidate source;
  storage-level ownership discovery is still required for those orphans.
- Retired completion currently means non-GC catalog records were scanned. The
  authority tombstone, task, claims, candidates and proof pages remain for
  inspection. A final GC-metadata cleanup must keep incomplete-owner replay
  fail-closed.

- Unit/integration: chunk-client dispatch, ChunkDB partial free/retry, GC record
  validation, deterministic reachability and retention, pin/publication races,
  bounded continuation, retry collisions and deferred shared ranges.
- E2E: native exclusive block release; foreground workloads during cleanup;
  clear/purge and restart; configured disk exhaustion and resumed progress.
- Gates: `pixi run cargo test -p crowdb-access-iceberg --all-targets`,
  `pixi run cargo test -p crowdb-access-server --all-targets`, affected chunk tests,
  `pixi run cargo fmt --all -- --check`, `pixi run rs-lint`.

## Scope

- Existing uncommitted work is preserved. No commits without an explicit request.
- Engine interoperability and ORC remain in their previously deferred tracks.
