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
- [x] **File and multipart-part discovery**: durable bounded, separate-scope scans
  discover file records and expired terminal multipart parts without walking
  unrelated catalog records. Retain active-root and retry-result dependencies.
  Files: GC repository and discovery.
- [x] **Assembly checkpoint reclamation**: authenticated `ICFW` frontier roots
  use a durable root index and existing tree cursor. Conflicted final trees are
  traversed once; published sessions reclaim only their checkpoint block.
  Terminal-session and retention checks precede each physical step; cleanup
  requires a completed claim. Files: FileIO checkpoint decoder, GC assembly
  worker, candidate/discovery/codec, `tests/gc_assembly_test.rs`.
- [x] **Pre-authority write discovery**: native FileIO persists catalog-sharded
  exact-location intents before shared-write DiskIO. Uncertain KV replies are
  read back; unresolved chunk writes defer reclamation until the readable cursor
  or terminal state settles them. Intents sweep after tree candidates, with
  reachable-owner and unfinished-tree protection and durable publication fences.
  Files: FileIO `write_intent`, native blocks, shared writer, GC `worker/writes`,
  records and failure/restart tests.
- [x] **Canonical reachability**: current and pinned historical metadata are parsed
  against captured heads. An immutable traversal stack and compressed binary
  mark index are content-addressed; one task CAS publishes both continuations.
  Missing frames/pages fail closed, including when proving nonmembership.
  Retained operations and table-wide upload/credential pins conservatively defer
  the pass. Files: `gc/proof/`, `gc/worker/live.rs`, `record/gc.rs`.
- [x] **Deletion worker**: final catalog scans require completed
  candidates and no paused/quarantined owner. A retirement marker fences stale
  GC mutations before bounded cleanup of candidates, claims, proof pages, owner
  fences and old tasks. Retain only the retired authority, winning task result
  and retirement marker. Uncertain progress writes are read back; stale unfenced
  live proofs terminate without deleting files. Files: GC retirement/terminal
  worker and failure/restart tests. Focused tests, native restart E2E and gates pass.
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

- Iceberg library all-target tests and Access Server Iceberg-enabled all-target
  tests pass. Focused coverage includes checkpoint forests, live shared-root
  protection, write-intent readback, exact byte ranges, deferred writes, stale
  publication, owner adoption, retired cleanup and lost progress/delete replies.
- Native storage E2E passes: exact pre-authority block intents are present before
  publication; checkpoint restoration, multipart recovery and byte-range reads
  survive catalog storage restart. Command:
  `CROWDB_RUNTIME_ROOT=/nv/cpp/crowdb/.crowdb-runtime/artifacts/reclamation-validation pixi run cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_file_storage_test`.
- The default runtime's persistent port claims caused an existing harness
  listen/RPC offset assertion before storage work began. The isolated runtime
  above avoids those claims without changing or deleting the persistent cluster.
- Chunk-client small-object and reclamation tests pass, including pre-DiskIO
  callback failure and readable-cursor/terminal-state reconciliation.
- Targeted clippy with Iceberg E2E targets and warnings denied, Rust fmt check,
  and workspace `pixi run rs-lint` pass.
- GC runtime remains disabled. Resource-isolation, capacity exhaustion/recovery
  and full SDK foreground-during-GC acceptance remain in the final task.

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
- Finish authenticated controls and independent runtime admission. Keep R183 open until
  the complete acceptance matrix has executable evidence.
- File-scoped immutable GcClaim records now select one generation-indexed candidate.
  Discovery reuses that record without resetting progress or retention. Sweep
  verifies the claim before dispatch. Under inactive authority, retirement can
  adopt an unfinished live/purge candidate; purge can adopt a live candidate.
  Adoption preserves the exact pending cursor and extends, never shortens,
  retention. Paused/quarantined owners are not automatically adopted. Remaining
  work includes operator recovery; stale unfenced live tasks now terminate after
  a confirmed authority change. This is
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
  Measure the new per-block durable intent cost and bounded callback batching
  there as well; do not remove write-before-authority coverage to improve throughput.
- Multipart assembly checkpoints now have a bounded, authenticated forest cursor.
  A completed claim permits terminal-session cleanup after part reclamation;
  published checkpoints never delete the frontier shared with the final file.
  Native block intents also cover writes preceding a FileRecord/checkpoint and
  superseded checkpoints. Intents belonging to a reachable file are conservatively
  retained until that owner becomes unreachable; the selected terminal checkpoint
  has its separate immediate-after-retention cleanup path.
- Terminal catalog cleanup leaves three bounded receipts, rather than a record
  per reclaimed file. Stale tasks cannot recreate candidates through the GC
  repository once the retirement marker is installed. Late unfinished records
  fail closed instead of being removed as completed work.

- Unit/integration: chunk-client dispatch, ChunkDB partial free/retry, GC record
  validation, deterministic reachability and retention, pin/publication races,
  bounded continuation, retry collisions and deferred shared ranges.
- E2E: native exclusive block release; foreground workloads during cleanup;
  clear/purge and restart; configured disk exhaustion and resumed progress.
- Gates: `pixi run cargo test -p crowdb-access-iceberg --all-targets`,
  `pixi run cargo test -p crowdb-access-server --all-targets`, affected chunk tests,
  `pixi run cargo fmt --all -- --check`, `pixi run rs-lint`.

## Scope

- Preserve unrelated work; commit verified requirement tasks coherently.
- Engine interoperability and ORC remain in their previously deferred tracks.
