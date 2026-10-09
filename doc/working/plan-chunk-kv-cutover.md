<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk KV Cutover Plan

Upstream: [Chunk KV Server design](../design/chunkds/design-crowdb-chunk-kv-server.md),
sections 6 and 7.
Goal: make split and ownership transfer recoverable at every durable boundary,
with catalog head publication defining the external cutover and no lost writes.

The independent CI/test fixes were committed as `1a471f43`, without pushing.
The user requested a local commit of the current changes on 2026-10-09;
no push is authorized. Remaining acceptance work is tracked below.

## Progress as of 2026-10-09 (intel7960)

- Task count: **15 complete, 1 active, 14 pending** (30 total).
  Completed tasks mean implemented and covered by focused passing regressions;
  they do not imply full production acceptance.
- Core split implementation is complete: retained original parent tree/WAL,
  one child, durable handoff before memory routing, inherited parent suffix then
  child WAL recovery, exact candidate reuse after an uncertain status response,
  current-writer routing and retained-parent lazy page recovery.
- Native block-backend acceptance passes: journal restart, native 32-MiB
  split/data verification, Iceberg inspection and S3 multipart inspection.
  The I/O completion notification regression is fixed. Syscall tracing confirms
  72–76 ms in Paxos KV's file-backend `fdatasync`, with waits in XFS log paths.
  A separate file sampled concurrently reaches 112 ms (p95 51 ms). DiskIO sync
  metrics also show about 71 ms; these use real file-backed disk images with
  direct I/O. Neither native path uses an in-memory backend, and Linux sync
  remains enabled. The user accepted occasional slow file-simulation sync;
  its performance investigation is closed. Native fixture API preparation
  uses the documented independent budget, while UI actions remain at 3 seconds.
- Eight of the nine measured Linux tasks pass on the production fixes: C++,
  Core, Storage, Access, UI, Boto3, Iceberg E2E and Java SDK. The additional Rust
  SDK task passes 5/5 with the full native retirement grace. The latest full UI
  task passes 225/225 after correcting its membership-readiness observation;
  C++ passes 942/942. Console remains failing at native catalog inspection, so
  production acceptance is not complete. Actual results are in `test.md`.
- Temporary diagnostic instrumentation has been removed.
- The production split/block fixes were committed locally as `407b5cab`; no
  push was performed. Verified follow-up acceptance changes are committed
  separately after the independent matrix completes.
- The full matrix found a separate block-WAL restart defect: direct reads
  did not align header/record buffers, returned EINVAL, and replay skipped the
  unreadable segment. Aligned reads and fail-closed replay fix the defect;
  all 108 WAL tests and all four production stream restart tests pass.
  The subsequent Core run exposed legitimate zero-byte new tails; a focused
  follow-up restricts cleanup to each disk's empty final segment and keeps
  nonempty unreadable segments and empty intermediate segments fail-closed.
  All 118 group tests and 109 WAL tests pass after this follow-up.
  The complete Core task subsequently passes 987 cases with zero ignored
  cases in 126.92 seconds. Complete Storage passes 839 cases with zero ignored
  cases in 828.45 seconds. Access passes 1036 ordinary cases; its separately
  scheduled Boto3 SDK case also passes. Native catalog observation is still
  failing: moving read-only
  checks ahead of process mutations was insufficient. The first Page request
  can return 409 as automatic weighted transfers continue after a transient
  4/4/4 snapshot. A stronger experimental preparation check required an unchanged
  complete generation for a normal policy cooldown and failed within the
  original ten-minute preparation deadline. Both unsuccessful experiments are
  archived and withdrawn; original strict assertions remain. Review whether
  inspection fixtures
  should use stable placement or production balancing must converge; no
  production policy change, stale-generation bypass or browser retry is made.
  Console-shared simulated S3 cluster acceptance passes serially (8/8); parallel
  startup previously hit 2.13-second block WAL sync and leadership churn.
  The independent UI/SDK/C++ matrix is complete and passing; the Console scope
  decision is pending.
- Remaining work, in order: finish focused boundary/lifetime audits and add regressions
  for confirmed gaps; run final performance/review/lint gates and the complete
  measured matrix; reconcile permanent design documentation.
- Transfer release/publication recovery, abort-versus-publication ordering and
  native overlay lifetime remain audit/coverage tasks. They are not confirmed
  defects and do not authorize speculative behavior changes or new hot-path locks.

## Current acceptance boundary requiring review

- A current Serving catalog is an observation, not a promise that its global
  generation remains unchanged during page navigation. Exact cursor rejection
  with 409 is correct after a new publication.
- Default weighted placement continues after count balance reaches 4/4/4.
  Reordering browser lifecycle cases does not make this fixture static.
  Continuous catalog-generation readiness did not complete within the existing
  ten-minute preparation bound; do not extend the bound or bypass 409.
- Selection has a rule-level cycle risk requiring a real regression before a
  production change: a 20-byte move between four-partition owners with total
  weights 100 and 60 gives 3/5 partitions and weights 80/80. Count-first repair
  can choose the same partition back, restoring 4/4 and weights 100/60. The
  present runtime evidence shows repeated count-balanced and imbalanced states,
  but does not yet prove this exact deterministic cycle.
- Decide whether UI inspection should use an explicit stable placement fixture
  with default weighted acceptance kept separate, or investigate convergence
  under the existing default policy. The user's decision is pending. Production
  selection, catalog fencing and handoff behavior remain unchanged.

## Constraints

- Preserve existing work; do not commit until requested.
- Keep request, replay and publication paths lock-free. Use existing immutable
  snapshots, epoch checks and revision CAS; do not add mutexes or a global queue.
- If correctness cannot be achieved without a lock or a material performance
  cost, stop that implementation and present contention, ordering, complexity
  and alternatives for review before introducing it.
- Do not bypass epoch fencing, lower an effective epoch, reuse an aborted
  candidate epoch, add caller retries or weaken assertions. Production
  deadlines and ordinary UI budgets remain unchanged. The user's 2026-10-09
  file-simulation latency exception permits native API preparation requests
  a ten-second budget and native fixture cases a three-minute total budget;
  page actions and assertions remain at three seconds.
- Distinguish a confirmed defect from an unverified crash/race window. Tests
  must establish the first divergence and preserve acknowledged data.
- Run builds, tests and lint through `pixi run`; runtime suites run sequentially.

## 1. Evidence and contract review

- [x] **Review and narrow current fix**: preserve the existing split handoff,
  exact-root publication CAS, checkpoint pins and WAL replay order. Withdraw
  speculative root rebasing and startup lifecycle changes; retain only delayed
  shared-tree authority acquisition for a prepared transfer target. Verify
  preparation metadata and aborted-target pin cleanup remain compatible.
  Files: `app/crowdb-chunk-kv-server/src/storage/`,
  `app/crowdb-chunk-kv-server/tests/transfer_storage_test.rs`.

Review findings: the epoch-handoff FFI extension relaxed the existing exact-root
lineage invariant, recovery-only pins changed checkpoint policy, and the new
`AwaitingFence` startup quiescing had no corresponding abort recovery. Those
changes were withdrawn. Their files are archived outside source under
`.crowdb-runtime/artifacts/cutover-review-withdrawn/`. Remaining crash-window
items below are audits; they are not justification for changing established
behavior without a concrete failing regression and review.


- [ ] **Capture failed transition**: capture authoritative catalog head/pages,
  transfer/split transitions and tree authority at the native source-restart
  failure before teardown. Determine whether release or owner publication had
  occurred; preserve traces and remove temporary production instrumentation.
  Files: `app/crowdb-web/tests/native_cluster_provisioning_test.rs`,
  `app/crowdb-access-server/src/iceberg/http.rs`.
- [ ] **Review publication boundaries**: compare T1–T8 and S1–S6 with both
  monitor publication implementations, transition revision CAS, serving grants
  and startup ordering. Record concrete counterexamples and required changes;
  do not infer safety from process-local handles.
  Verified distinction: initial unpublished handoff recovery derives the
  dispatch frontier from child WAL evidence or an empty child's parent tail;
  published child recovery uses its final readiness/catalog frontier, including
  when its own WAL is empty. Later next-epoch parent records are not inherited.
  Files: `app/crowdb-kv-server/src/background/domain_monitor/chunk_kv.rs`,
  `app/crowdb-kv-server/src/background/domain_monitor/chunk_kv/catalog.rs`,
  `app/crowdb-chunk-kv-server/src/catalog/transition.rs`,
  `doc/design/chunkds/design-crowdb-chunk-kv-server.md`.

## 2. Transfer preparation and recovery

- [x] **Regression for prepared target fencing source**: prepare a target while
  catalog still assigns source; prove the root authority remains at the source epoch and can be reopened at
  that epoch. Preserve the existing partition checkpoint pin policy. Verify
  target cannot publish a mutable root before catalog authorization.
  Files: `app/crowdb-chunk-kv-server/tests/transfer_storage_test.rs` (new).
- [x] **Separate pinned reads from effective tree authority**: target preparation
  opens the exact pinned immutable root without claiming the shared source
  tree. Acquire mutable root authority only for the matching published target
  assignment; preserve the existing exact-generation CAS and checkpoint pins.
  Inspect WAL binding claims separately; do not conflate a private target WAL
  with the shared base tree. Split existing oversized storage implementation
  along root-authority ownership before adding substantial code.
  Files: `app/crowdb-chunk-kv-server/src/storage.rs`,
  `app/crowdb-chunk-kv-server/src/storage/` (new owned modules),
  `app/crowdb-chunk-kv-server/src/serving/worker.rs`.
- [ ] **Recover release-before-publication**: startup reads exact durable release
  or lease-exclusion evidence before admitting old-source writes. Resume the
  transfer obligation while source remains fenced; preserve complete durable
  tail recovery on source loss. Avoid startup ordering that prevents the worker
  needed for reconciliation from running.
  Files: `app/crowdb-chunk-kv-server/src/main.rs`,
  `app/crowdb-chunk-kv-server/src/catalog/recovery.rs`,
  `app/crowdb-chunk-kv-server/src/serving/transition_runtime.rs`.
- [ ] **Repair publication-before-marker**: independently cover owner publication
  and Serving publication. Recover authority and committed phase from exact
  published entries plus matching transition proofs; reject conflicting epochs,
  ranges, artifacts and transition identities. Repair is idempotent and cannot
  reactivate source after cutover.
  Files: `app/crowdb-chunk-kv-server/src/server/activation.rs`,
  `app/crowdb-kv-server/src/background/domain_monitor/chunk_kv/catalog.rs`,
  `app/crowdb-chunk-kv-server/tests/catalog_transition_test.rs`,
  `app/crowdb-chunk-kv-server/tests/transfer_recovery_test.rs` (new).

## 3. Split handoff and recovery

- [x] **Implement durable handoff boundary**: `SplitTransition.handoff_proof`
  reuses `TailOverlayArtifact` for the pinned child base and initial parent
  replay frontier. The reducer records it independently of final readiness;
  Group-0 persistence resolves uncertain commits and rejects clearing or
  replacing the proof or bound plan. Readiness may extend the tail but must
  preserve the pinned base; committed handoff cannot abort or rewind phases.
  Four control-store regressions cover these boundaries and legacy decoding.
  Production preparation now creates only a child tree/WAL and keeps the
  original parent worker and storage. The successful status write precedes
  dispatch; status persistence runs outside the mutation worker. Startup
  defers the old full-range parent's admission while unpublished handoff
  recovery is pending. Child recovery reuses the exact pinned base, filtered
  parent suffix and its own WAL; its first own record bounds the inherited
  sequence when the two writers have diverged. Existing child dispatch is
  finalized in place after publication errors rather than reopening its WAL.
  Library regressions cover original storage identity, concurrent
  reads/writes during status and prefix publication, both cold WAL cases and
  restored parent/child dispatch and consecutive splits on the original worker.
  The native tree test also covers writes made while status persistence is
  pending. CI lint passes. Production acceptance exposed cleanup starvation
  and an outdated worker lifecycle reference on the second split; both have
  focused passing regressions. The focused implementation and recovery regressions are complete.
  Production acceptance and the final matrix are tracked separately below.
  Additional regressions cover an advanced retained-parent checkpoint and
  512 original native-tree records (32 MiB incompressible values) across three
  splits and checkpointing. The complete affected library, server and protocol
  tests pass; native production acceptance remains pending.
  Files: `lib/crowdb-protocol/src/chunk_kv/split.rs`,
  `app/crowdb-chunk-kv-server/src/serving/split.rs`,
  `app/crowdb-chunk-kv-server/src/control_store.rs`,
  `app/crowdb-chunk-kv-server/tests/control_store_test.rs`.

Recovery order is already decided: open the exact base, replay the inherited
parent journal with range filtering, then replay the child's own journal.
`replay_parent_overlay` and `replay_child_overlay` implement that existing
composition. Continued parent appends during handoff status persistence must be
covered by its replay metadata; this is an implementation task, not a request
to reconsider the recovery design or introduce another handoff protocol.
Preserve sequence and identity validation when updating that metadata. The
single-child `prepare_split` path calls `begin_split_finalization`, which closes
admission and drains writes; do not adopt that behavior when removing the
obsolete two-target session.

- [x] **Restore retained-parent storage identity**: remove the obsolete
  two-destination rebuild from the production split session. Retain the parent
  tree and journal; prepare only the new child tree and journal. Trace the
  existing `prepare_retained_parent` representation and update session wiring
  and recovery tests without relaxing root lineage or epoch fencing.
  Files: `lib/crowdb-chunk-kv/src/partition/split.rs`,
  `app/crowdb-chunk-kv-server/src/storage.rs`.
- [x] **Persist handoff before routing**: the successful durable handoff status
  update is the local handoff commit point; install in-memory parent-to-child
  routing only afterward. Reuse the existing transition and artifact fields
  where possible to identify the unchanged parent tree/WAL and new child
  tree/WAL, pinned bases,
  source replay frontier `C`, owner epochs and transition identity. Do not use
  final `ChildPrepared` readiness for this earlier state. A restart after the
  update but before route installation resumes the same split and replays the
  original parent WAL and child WAL; absence of the record must imply that the
  child has acknowledged no mutations. An uncertain update result requires
  reading the exact durable record before deciding whether dispatch may change;
  parent writes continue during resolution. Once
  committed, do not discard the split as an unpublished candidate. Catalog
  publication remains the external routing and ownership commit point.
  Fixing `C` must not stop parent journal appends or reads while the status is
  persisted. Cover the parent-journal suffix written during that interval in
  child catch-up and recovery before changing routing. Do not await the durable
  status update inside the mutation worker or introduce a handoff pending queue.
  No new hot-path locks are authorized.
  Files: `lib/crowdb-chunk-kv/src/partition/split.rs`,
  `app/crowdb-chunk-kv-server/src/serving/split.rs`,
  `app/crowdb-chunk-kv-server/src/serving/transition_runtime.rs`,
  `lib/crowdb-protocol/src/chunk_kv.rs`.
- [x] **Prove unpublished local handoff recovery**: crash after writer handoff,
  before readiness persistence and before head publication. Track writes accepted
  on both halves and prove recovery includes every suffix while catalog still
  names the original parent. Identify the durable handoff record needed to make
  this recoverable; an in-memory dispatcher is insufficient.
  Files: `app/crowdb-chunk-kv-server/src/serving/worker.rs`,
  `app/crowdb-chunk-kv-server/tests/split_recovery_test.rs`,
  `lib/crowdb-chunk-kv/src/` (owning split/recovery modules).
- [ ] **Verify safe split abort**: reject abort after durable handoff and resume
  the same split to publication. Reject stale abort proofs;
  do not delete a child WAL just because the old catalog omits the child.
  Files: `app/crowdb-chunk-kv-server/src/serving/split.rs`,
  `app/crowdb-chunk-kv-server/src/server/` (local split owner),
  `app/crowdb-chunk-kv-server/tests/split_abort_recovery_test.rs` (new).
- [ ] **Reconcile published split before marker**: exact catalog successor entries
  prove commit when phase persistence was interrupted; both halves recover and
  activate only with matching authority. Preserve old-route compatibility without
  selecting a historical writer over the current assigned writer.
  Files: `app/crowdb-chunk-kv-server/src/server/activation.rs`,
  `app/crowdb-chunk-kv-server/src/server/reconcile.rs`,
  `app/crowdb-chunk-kv-server/tests/split_recovery_test.rs`.
- [x] **Prefer the current writer over historical split dispatch**: exact
  catalog/registry assignments take precedence for current requests. A stale
  request may use a historical split dispatcher only when its resolved writer
  still matches the catalog assignment, or the original unpublished parent
  remains authoritative. A later transfer epoch must not read an obsolete
  local split view. The regression first reproduced `old-view` instead of
  `current-view`; stale and current point routes now pass, together with the
  existing old-parent seek, scan and continuation regressions.
  Files: `app/crowdb-chunk-kv-server/src/server/routing.rs`,
  `app/crowdb-chunk-kv-server/tests/split_current_assignment_test.rs`.

- [x] **Reuse unpublished local split dispatch during catalog refresh**:
  after local handoff, the registry contains the next-epoch retained parent
  while catalog still assigns the original full-range parent. Match the exact
  original dispatcher as well as its two writers; otherwise refresh reopens
  the same retained tree and can fence its live root publisher. The regression
  reproduces the missing local assignment before the fix and passes afterward,
  including reconciliation without a newly recovered partition. Preserve exact
  identity matching and root publication CAS. The focused regression passes;
  full native acceptance is tracked separately below.
  Files: `app/crowdb-chunk-kv-server/src/server.rs`,
  `app/crowdb-chunk-kv-server/tests/split_current_assignment_test.rs`.

- [ ] **Catch up catalog when a grant changes generation**: a grant notification
  previously installed new authority and returned without loading its catalog,
  leaving the old generation fenced until the five-second periodic refresh.
  Refresh immediately on a generation mismatch; preserve exact generation and
  assignment authorization. Verify native S3/Iceberg requests across catalog
  changes without increasing their request budgets. Native acceptance pending.
  File: `app/crowdb-chunk-kv-server/src/main.rs`.

- [x] **Verify retained-parent lazy recovery across a narrower logical range**:
  production transfer exposed valid original pages rejected by the demand-load
  range check. Demand loading now checks CRC and structure; request fences and
  rebuilt snapshot checks remain. The narrowed-root regression and native
  512-record balance/data verification pass. Full Console acceptance remains
  tracked separately below.

## 4. Abort, stale work and reclamation

- [x] **Preserve the candidate after an unconfirmed handoff write**: the focused
  lost-response test exposed a library retry rejected by its own pin; production
  bypassed that rejection by unpinning and rebuilding. Keep the exact child
  base/WAL and pins for a live retry, while parent reads and appends continue.
  Installed dispatch owns subsequent finalization. An authoritative pre-handoff
  abort releases the cached child pin and permits another split. Verify retry,
  abort and production storage wiring. Both library regressions and the focused
  server control-store, split-recovery and transition-worker tests pass.
  Final Storage and native acceptance are tracked separately below.
  Files: `lib/crowdb-chunk-kv/src/partition/split/preparation.rs`,
  `lib/crowdb-chunk-kv/tests/split_prepare_retry_test.rs`,
  `app/crowdb-chunk-kv-server/src/storage.rs`.

- [ ] **Serialize abort against publication without locks**: verify monitor fence,
  transition revision and catalog expected-head CAS jointly prevent a stale
  readiness snapshot publishing after abort. Add controlled interleaving tests
  before implementing any needed atomic publication change.
  Files: `app/crowdb-kv-server/src/background/domain_monitor/chunk_kv/catalog.rs`,
  `app/crowdb-chunk-kv-server/src/catalog/transition.rs`,
  `app/crowdb-chunk-kv-server/tests/catalog_transition_test.rs`.
- [ ] **Retire exact candidate resources**: durably abort before cleanup; stale
  workers cannot activate the candidate. Preserve source references, base pins,
  replay cursors, retry floors and forwarding grace. Verify candidate epochs are
  allocated monotonically across abort and restart; cleanup is idempotent.
  Files: `app/crowdb-chunk-kv-server/src/serving/worker.rs`,
  `app/crowdb-kv-server/src/background/domain_monitor/chunk_kv/`,
  `app/crowdb-chunk-kv-server/tests/transfer_recovery_test.rs` (new).

## 5. Verification and completion

- [x] **Recover direct block WAL without losing segments**: align physical
  reads, copy only the requested available bytes and preserve EOF semantics.
  Refuse recovery when a nonempty segment cannot be opened or decoded,
  rather than silently dropping its acceptor history. Zero-byte final files
  from interrupted segment creation use narrowly scoped cleanup; all 118
  group tests and 109 WAL tests pass after the complete Core gate exposed it.
  Cover sealed/unsealed
  replay, cross-block ranges, EOF and unreadable segments; verify the native
  full-service restart case. All 108 WAL tests and four stream restart tests
  pass. Files: `lib/crowdb-kv/src/wal/block_backend.rs`,
  `lib/crowdb-kv/src/wal/replay.rs`,
  `lib/crowdb-kv/tests/wal_test/block_restore_test.rs`.

- [x] **Use block storage defaults and cluster fixtures**: set the system
  tree default to `block` and WAL default to `block-device`, including CLI,
  configuration defaults and deserialized configuration fallback. Pin these
  backends in process-cluster fixtures and retain explicit backend tests.
  Verify default parsing, actual runtime backend selection and restart recovery.
  All eighteen CLI/configuration tests and all three selected native browser
  cases pass. Runtime argv, block tree files and WAL block metrics confirm
  the selected backends; the journal case verifies owner restart recovery.
  On 2026-10-09 the user accepted occasional slow sync through file-backed
  device simulation/VFS and requested moving past that latency investigation.
  Durable synchronization remains enabled; correctness tests remain required.
  Files: `app/crowdb-kv-server/src/cli.rs`,
  `lib/crowdb-kv/src/common/config.rs`, cluster launch fixtures.

- [x] **Wake idle I/O pollers on kernel completions**: Hybrid mode consumed
  CQEs without setting the dispatch flag and listened only for submissions
  during idle event-wait. The native metadata workload repeatedly paid about
  75 milliseconds per I/O stage. Register the poll thread's private eventfd
  for kernel completions and notify external reactors after draining CQEs.
  The new idle-reactor regression fails three times before the fix and passes
  100 repetitions afterward; all fourteen DiskIOUring tests pass. However,
  the subsequent native rerun still shows approximately 75-millisecond stages
  and Iceberg/multipart timeouts. This fixes a demonstrated notification defect
  but does not explain all remaining latency. Subsequent syscall tracing
  confirms real slow filesystem sync. The user accepted that file-simulation
  latency and requested moving past its investigation; final functional
  acceptance and the full matrix remain separate tasks.
  Files: `lib/crowdb-common/cpp/src/diskio_uring.cpp`,
  `lib/crowdb-common/cpp/tests/diskio_uring_test.cpp`.

- [x] **Remove per-byte WAL codec overhead without changing the format**:
  the current debug build took 2.572 seconds to decode and 3.023 seconds to
  encode 512 records containing 32 MiB of values, before storage reads. Use
  bulk byte serialization for mutation keys, values and condition/result
  values. The same measurement becomes 7.372 and 9.296 milliseconds.
  A regression compares every mutation and result variant against the
  original sequence encoding byte for byte; all seven frame tests pass.
  Temporary codec instrumentation is removed. All 51 affected library tests
  pass, and native journal interruption/restart passes with cold recovery
  around 0.6 seconds. The final matrix is tracked separately below.
  Files: `lib/crowdb-chunk-kv/src/types.rs`,
  `lib/crowdb-chunk-kv/tests/frame_test.rs`.

- [ ] **Unit and controlled integration matrix**: test before/after every durable
  step, ambiguous head responses, stale revisions and grants, concurrent source
  checkpoints, target restart, source restart and abort/publication races.
  Assert ownership exclusivity, WAL order, acknowledged-value preservation,
  exact-root lineage and eventual recovery. Files: tests listed above.
- [ ] **Audit native overlay source lifetime**: native overlays retain a raw
  source-tree pointer. Prove the source survives outstanding child reads when
  finalization fails or dispatch/session handles disappear; do not rely on a
  worker-channel reference cycle for lifetime. Exercise dropping external
  source handles before inheritance completes and concurrent overlay removal.
  Establish a failing regression before changing ownership; any fix must keep
  request paths free of additional locks.
  Files: `lib/crowdb-chunk-kv/src/partition/tree.rs`,
  `lib/crowdb-tree/ffi/src/tree.rs`,
  `lib/crowdb-tree/src/btree/memtable_sources.cpp`.
- [~] **Native process regression**: the latest 512-record/32-MiB run passes
  split/balance placement and data verification. With block defaults, selected
  journal interruption (6.2 seconds), Iceberg (15.7 seconds) and S3 multipart
  inspection (43.6 seconds) all pass; the enclosing native case takes 398.17
  seconds including setup and teardown. No new crash was observed.
  The accepted file-simulation sync delay uses a separate native API preparation
  budget; UI actions retain three seconds. Rerun the ordered lifecycle-interruption and
  Iceberg inspection pair, full native inspection, journal windows and production
  split/weighted placement phases. Retain real crash artifacts if a process
  crashes. Files: `app/crowdb-web/tests/native_cluster_provisioning_test.rs`,
  `app/crowdb-web/tests/common/native_balance.rs`.
- [ ] **Performance and review gate**: review hot paths for new serialization,
  blocking work and unbounded scans/replay; compare appropriate existing workload
  measurements before/after. Run affected tests, fmt and CI lint. No perf claim
  without measurements. Files: affected crates and task scripts.
- [ ] **Finish original test matrix**: rerun full Storage, Console, Access and
  separately scheduled Rust Iceberg SDK tests; update intel7960 table with actual
  measurements. Explain any remaining Linux skips individually. Files:
  `doc/working/test.md`, `tools/test-metrics/measure.py`.
- [ ] **Close documentation gaps**: remove design Open Issues only when covered
  by passing regressions; review final code against S1–S6/T1–T8. Delete this
  temporary plan after all tasks and original matrix are complete.
  Files: `doc/design/chunkds/design-crowdb-chunk-kv-server.md`, this plan.

## File inventory

- Design and evidence: the upstream design, this plan, `doc/working/test.md`.
- Catalog and monitor: `app/crowdb-kv-server/src/background/domain_monitor/chunk_kv*`,
  `app/crowdb-chunk-kv-server/src/catalog*`.
- Storage and execution: `app/crowdb-chunk-kv-server/src/storage*`, `src/serving/`,
  `src/server/`, `src/main.rs`, owning `lib/crowdb-chunk-kv/src/` modules.
- Unit/integration: Chunk-KV server crate `tests/` files named in tasks.
- E2E: native provisioning and balance fixtures, existing lifecycle and Iceberg
  inspection browser specs; use the installed system browser.
