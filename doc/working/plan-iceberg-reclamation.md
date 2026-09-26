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
- [x] **Operator controls**: authenticated task start/inspect/pause/resume/retry,
  operator pin/unpin, validated rate limits and durable progress output. Files:
  GC repository, Access Server management runtime and control tests. A
  quarantined task resumes its exact prior phase; retired task admission
  verifies the completed clear operation and selected epoch.
- [x] **Background admission and fairness**: verify bounded task enumeration,
  dedicated GC clients, one-step CPU/time and memory/work caps, independent KV
  and chunk budgets, and cancellation/restart progress. Alternate retired and
  active task turns so a long retired catalog cannot starve foreground catalog
  cleanup. Preserve failure-record reserve. Files: Access Server GC runtime,
  GC limits/worker, budget and scheduler tests.
- [x] **Crash and race acceptance**: exercise inactive purge/clear with readers,
  credential protection, changed authority and lost replies across worker or
  server restart; never delete before the last protector expires or releases.
  Files: GC worker/fence tests and native control tests.
- [x] **Foreground saturation acceptance**: run namespace, commit and FileIO
  requests with the official SDK while GC has a sustained task backlog; assert
  foreground requests remain within the test deadline and GC remains bounded.
  The pinned PyIceberg environment includes `s3fs`; the test obtains the
  standard REST credential response explicitly before using PyIceberg FileIO.
  Files: Access Server native E2E tests and pixi environment.
- [x] **Capacity acceptance**: configured disk exhaustion and recovery,
  foreground Iceberg SDK operations during GC, affected tests and gates. Native
  full-disk FileIO failure/recovery and committed-file readability pass; the
  full-disk GC-workspace case now runs with deterministic admission failure at
  candidate persistence while the native simulated disk reports zero free
  bytes. The task retains its continuation, obeys retry backoff, and completes
  after block release and compaction without touching another table's committed
  file. A separate fault-injected mark test verifies the same fail-closed
  behavior for proof pages.
- [~] **Architecture cleanup**: update permanent design, remove temporary plan
  and requirement only after the acceptance matrix passes.

## Blocked

- Full Access Server acceptance is not green. The existing native S3 test
  `signed_standard_put_get_and_multipart_publish_unbound_files` originally
  returned 409 on its first PUT: its fixture minted a grant for a random table
  with no published head or staged draft, which the GC reader pin correctly
  rejects. The fixture now creates a real staged draft before issuing the
  grant, preserving unbound file-kind coverage without bypassing protection.
  The test then advances through PUT, GET and multipart upload but fails while
  reading the successful CompleteMultipartUpload response with
  `UnexpectedEof` in the chunk-size line at
  `app/crowdb-access-server/tests/iceberg_file_http_test.rs:237`.
- Six isolated/root-cause-directed runs reached this point: the initial full
  server suite, a single-test reproduction, two instrumented single-test
  reproductions, and the staged-draft fixture runs. The first fixture run
  exposed disabled table routes because the persisted delegation bound was
  zero; setting the test bound to fifteen minutes resolved that and exposed
  the multipart response failure. Do not relax `ReaderPins::protect_files` or
  claim the full acceptance gate passed. Next diagnose the listener-side
  multipart response stream and rerun the full suite in a fresh runtime.
- Workspace `pixi run rs-lint` is independently blocked by the concurrent
  uncommitted `container/crowdb-monitor` crate: one unused import and four
  missing `# Errors` sections. Targeted Iceberg clippy passed; do not alter
  unrelated container work as part of R183.

## Storage findings

- Existing ChunkDB delete persists a Deleted tombstone with strips before freeing
  blocks; failed cleanup remains retryable and strips are cleared after release.
- The range-delete RPC currently reports Unimplemented. Keep pending work and
  expose that status until the independent shared-range storage work lands.
- Current native Iceberg file blocks all use small-write shared chunks, including
  large files represented as bounded trees. File length does not prove exclusive
  ownership. Exclusive deletion requires storage ownership evidence.

## Verification

- Retired and active catalog cleanup alternate under a 48-record retired
  backlog and one-item scan pages; the active purge task advances while the
  retired worker remains in discovery. A reader and delegated-credential pin
  survive worker reconstruction, and physical deletion starts only after both
  pins release. Existing changed-generation, lost-reply and restart tests
  cover the other crash/race boundaries.
- Four concurrent official PyIceberg workers each create, commit, reload and
  drop three tables while reading committed metadata through PyIceberg FileIO;
  GC advances during those requests against a 128-record purge backlog.
  Command: `CROWDB_ICEBERG_E2E_PYTHON=.pixi/envs/iceberg-e2e/bin/python CROWDB_RUNTIME_ROOT=.crowdb-runtime/artifacts/gc-sdk-pressure-20260926e pixi run cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_gc_control_test official_sdk_foreground_progresses_under_gc_backlog -- --ignored --nocapture`.
- A native capacity test fills the configured simulated disk, injects denial
  at GC candidate persistence, confirms the durable task and file remain,
  checks retry backoff, then frees/compacts disk blocks and completes the GC
  task. The unrelated committed file remains readable after completion.

- Iceberg library all-target tests pass; the current Access Server
  Iceberg-enabled all-target gate is blocked as recorded above. Focused coverage includes checkpoint forests, live shared-root
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
- Targeted clippy with Iceberg E2E targets and warnings denied and Rust fmt check
  pass. Workspace `pixi run rs-lint` is blocked as recorded above.
- GC runtime remains disabled by default. Foreground saturation and the
  full-disk workspace-failure acceptance above have focused coverage; the
  Access Server all-target gate remains blocked as recorded above.
- Authenticated native-process control and opt-in scheduler restart E2E pass;
  the scheduler advances a durable task while foreground configuration remains
  available. KV/chunk admission tests deny dispatch after independent budgets
  and verify step reset. This does not yet demonstrate saturated foreground
  isolation. Official PyIceberg namespace and table create, commit, load and
  drop succeed against the same native listener while GC advances a task.
- Native capacity E2E fills the configured simulated disk through DiskDB, then
  forces a new FileIO chunk allocation to fail while an already committed file
  remains loadable and readable. Releasing blocks and compacting a zone allows
  the same file write, publication and read to succeed. Command:
  `CROWDB_RUNTIME_ROOT=/nv/cpp/crowdb/.crowdb-runtime/artifacts/gc-capacity-validation3 pixi run cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_gc_capacity_test`.

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

- Before default runtime activation, validate foreground availability under
  large purge and retired-catalog sweeps. Complete resource accounting across proof KV
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

## Next Integration

- No new live-table deletion tasks. An unreachable live orphan may remain until
  drop or clear. The operator command accepts tombstoned tables only; the
  scheduler never runs a legacy live deletion step and releases any owned head
  fence before completing such a task. Candidate `Sealing`, unused `Unseal`, and
  duplicate commit file lookup are removed.
- Enabled background GC admits durable purge markers and completed clear
  operations into deterministic tasks. Native admission and advancement tests
  pass; uncertain task-creation replies and multi-restart completion remain in
  the crash acceptance matrix.
- Add saturated foreground namespace/commit/FileIO acceptance and full-storage
  GC-workspace recovery. Existing tests establish fail-closed workspace denial
  and independent file-write capacity recovery, not those combined conditions.
