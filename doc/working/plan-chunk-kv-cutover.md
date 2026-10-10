<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk KV Cutover Plan

Upstream: [Chunk KV Server design](../design/chunkds/design-crowdb-chunk-kv-server.md),
sections 6–9. Results: [intel7960 test matrix](test.md).
Goal: recover split/transfer at durable boundaries without lost writes, and
make automatic placement stable, inexpensive and explainable.

Temporary plan: completed implementation detail is condensed below; historical
source and evidence remain in local commits and the referenced artifacts.
Delete this plan after the remaining audits and acceptance are complete.

## Current placement decision — 2026-10-09

- R228 remains deferred by the user. R229 membership CAS was completed
  separately on 2026-10-10; continue only the Console/native acceptance and
  cutover audits here.
- Data Weight is deferred to [tree range metrics](../backlog/R228-tree-range-metrics.md).
  Placement uses split counts with existing tolerance/cooldown; byte coefficient
  must be zero. Console Weight and weighted explanation are hidden.
- Tree metrics implementation is not part of this cutover work. No tree writer,
  split/move execution or recovery changes are authorized by this deferral.
- Size-driven weighted acceptance is superseded by the deferred requirement,
  not marked passed or ignored. Retain count convergence/data integrity coverage.
- Existing automatic split has count-shortfall and coarse-pack size triggers;
  the latter and size ranking require replacement after tree metrics acceptance.

## Status — 2026-10-09

- Local commits: `1a471f43`, `407b5cab`, `a664e357`, `38389b0c`; no push performed.
- Split retains the original parent tree/WAL and creates one child. Durable
  handoff precedes memory routing; catalog publication is the external cutover.
  Recovery opens the exact base, range-filters the inherited parent WAL, then
  replays the child's WAL. Initial unpublished handoff derives the dispatch
  frontier from child WAL evidence; final readiness/catalog fixes the published
  frontier, even when the child's WAL is empty.
- Verified fixes include prepared-target immutable root access, uncertain
  handoff candidate reuse, current-writer routing, unpublished dispatcher reuse,
  narrowed-range lazy recovery, aligned block-WAL reads/fail-closed replay,
  narrowly scoped empty-final-tail cleanup, block tree/WAL defaults, I/O completion
  wakeup and format-compatible bulk WAL byte encoding. Focused regressions pass.
- Before the balance implementation, eight of nine Linux matrix tasks pass;
  extra Rust Iceberg SDK passes 5/5.
  Baseline full UI passes 225/225, C++ 942/942, Core 987/987, Storage 839/839.
  Separately scheduled SDK cases ran and passed; no functional Linux skip is
  accepted. This historical baseline is superseded by the complete current
  Console acceptance below. Exact counts/times are in `test.md`.
- No new crash observed in the latest acceptance attempts. Previous crash
  investigation is retained under `/tmp/crowdb-core-investigation`.
- File-backed simulated device sync delays are confirmed and accepted by the
  user; sync stays enabled. Native API preparation may use 10 seconds and native
  fixture cases 180 seconds; ordinary UI assertions/actions remain 3 seconds.
- Transfer/publication race and native overlay lifetime items below are audits,
  not confirmed defects or authorization for speculative production changes.

## Constraints

- Run builds, tests, lint and executables through `pixi run`; runtime suites
  run sequentially. Commit only when asked; never push without authorization.
- Preserve exact identities, epochs, root lineage, revision CAS and recovery
  pins. No epoch guessing, caller retries, weakened assertions or deadline inflation.
- Keep hot paths lock-free. Review any unavoidable lock, queue, blocking work or
  material performance cost with the user before implementation.
- The user authorizes balance algorithm/configuration and UI implementation.
  Split/move execution, handoff and recovery changes still require prior review.
  No new hot-path locks or speculative cutover changes are authorized.

## 1. Placement policy and deferred tree metrics

- [x] **Verify count-only placement and hidden Weight**: byte coefficient defaults
  to zero and nonzero configurations are rejected. Both selectors retain shared
  count scoring, 20% tolerance, 25% improvement, cooldown and safety filters.
  Equal split counts must not move merely because shared-pack estimates differ.
  Hide graph/property Weight and balance explanations; verify native Page flow.
  Files: protocol/server config, balance tests, console components/catalog spec.
  Verified: 24 focused Rust cases, 7 UI unit cases, Rust fmt/clippy, UI lint/build
  pass. Actual native Page flow passes: browser 2.3s, owned cluster 345.98s;
  zero ignored. Weight/property/explanation absence and Page inspection asserted.
- [x] **Finish Console acceptance**: complete gate passes 350 Rust test
  executions, zero failed/ignored, and all 23 native browser cases across four
  fixture phases. Approved allocation fix reroutes only typed pre-send connection
  failures to a changed owner endpoint once; ambiguous sent requests are never
  replayed. Both regressions and all eight S3 cluster cases pass. Native Start
  asserts the original response before process state and avoids duplicate cleanup
  submissions. Measured ChunkKV Start is 789/3800ms; response budget is 5s,
  process/DOM stays 3s. No core startup change. Full UI passes 229/229.
  Evidence and matrix: `test.md`, balance-policy artifact directory.
- Tree metrics/data-weight work is deferred to
  [R228](../backlog/R228-tree-range-metrics.md), requiring core review before
  implementation. Size-triggered split currently uses coarse pack estimates;
  count-shortfall split remains available. No writer/checkpoint change here.
- The previous size-weight acceptance failed at 968.43s because opened manifest
  estimates did not update after ordinary flush. It is superseded by the deferred
  metrics contract, not counted passed or ignored. Evidence remains under
  `.crowdb-runtime/artifacts/balance-policy-20261009/`.

## 2. Remaining cutover boundary audits

Only change behavior when a concrete regression proves a gap; any core fix
requires the user's review. Existing durable handoff/replay contracts stay intact.

- [x] **Review publication boundaries**: compare S1–S6/T1–T8 with monitor and
  local publication, revision CAS, grants and startup. Distinguish initial
  unpublished handoff recovery from final published replay bounds.
  Files: monitor `chunk_kv/catalog.rs`, server `catalog/transition.rs`, design.
  Both publish immutable pages before expected-head revision CAS and recognize
  exact successor entries idempotently; phase persistence is a subsequent CAS.
  The remaining startup/abort races below are not proven by that comparison.
- [ ] **Recover release before publication**: prove old-source admission stays
  fenced by exact release/lease evidence and reconciliation can still start.
  Files: server `main.rs`, `catalog/recovery.rs`, `serving/transition_runtime.rs`.
  Review finding: old Serving catalog plus old valid grant reaches generic
  `activate_recovered_partition`; the source branch does not read transfer
  release proof. `suspend_for_transfer` fences the live lifecycle in memory.
  Need a controlled restart regression before changing activation; do not claim
  this window safe from ordinary transfer/state-machine tests.
  Proposed reviewed scope: load durable transfer evidence once on the grant/
  recovery control path, match exact source instance/partition/epoch, and keep
  recovered source non-serving after release even if old catalog/grant remains.
  Retain transition processing so publication can finish. No per-request read,
  lock, WAL rerouting or epoch change. Controlled restart regression must prove
  old-source mutation rejection and eventual target recovery. Await approval.
- [ ] **Repair publication before phase marker**: cover owner and Serving
  publication independently; exact catalog entries and proofs allow idempotent
  repair and cannot reactivate the source. Files: server `server/activation.rs`,
  monitor `chunk_kv/catalog.rs`, `catalog_transition_test.rs`, transfer recovery tests.
- [x] **Verify split abort**: reject abort after durable handoff or with stale
  proof; preserve child WAL omitted by the old catalog and resume the same split.
  Files: server `serving/split.rs`, local split owner, split abort/recovery tests.
  `control_store_test`, `split_prepare_retry_test` and handoff recovery tests pass;
  committed proof cannot be cleared/replaced, unknown uncommitted status permits
  exact abort, and committed dispatch resumes the same base.
- [ ] **Reconcile published split before marker**: recover both exact successor
  assignments and matching authority; historical routing cannot select an old
  writer over the current assignment. Files: server activation/reconcile and
  `split_recovery_test.rs`.
- [ ] **Verify grant-generation refresh**: immediate catalog refresh is already
  implemented; prove new grants do not leave routes fenced until periodic refresh.
  Files: server `main.rs`, native S3/Iceberg catalog-change acceptance.
- [ ] **Order abort against publication**: controlled interleavings must prove
  monitor fencing, transition revision and expected-head CAS exclude stale
  publication. Files: monitor catalog, local transition and catalog transition tests.
- [ ] **Retire exact candidate resources**: durable abort precedes idempotent
  cleanup; preserve source references, base pins, retry floors and grace. Verify
  monotonic candidate epochs across abort/restart. Files: transition worker,
  monitor, transfer recovery tests.
- [ ] **Audit native overlay lifetime**: prove raw source-tree pointers remain
  valid when external/session handles disappear, finalization fails or overlays
  are removed concurrently. Establish a failing regression before ownership changes.
  Files: `lib/crowdb-chunk-kv/src/partition/tree.rs`,
  `lib/crowdb-tree/ffi/src/tree.rs`, `lib/crowdb-tree/src/btree/memtable_sources.cpp`.
  C++ stores a raw source-tree pointer; Rust's safe install method takes a
  temporary borrow and does not retain source ownership. Normal successful
  finalization clears the overlay, but failed finalization/source-handle loss
  and readers concurrent with clear still need a lifetime regression. No new
  crash has been observed and no ownership fix is applied.

- **Membership CAS contract (R229) completed 2026-10-10**: the complete members
  record, epoch CAS, Installing/Ready fencing, exact-epoch recovery and shared
  KV-client/server conflict protection are implemented. The sequential topology
  fixture passes 5/5 (33.1s); the full UI run retains two unrelated timing/data
  flow failures. The permanent reconfiguration design records the final
  contract; this cutover plan no longer tracks membership implementation.

Native lifecycle follow-up: recorded restart opens its first tree root at
09:21:27.697, then three more roots at 09:21:30.298–30.485; the RPC listener
starts at 09:21:30.532 (about 3.15s after process startup). Recovery is sequential
and includes journal replay. These timestamps do not attribute the delay to
fsync. The user authorized a measured, modest Start-response budget after
reviewing timing distribution; 10s is not the default target. The focused run passes with a 5s response budget: ChunkDB 215ms, DiskIO
366ms, ChunkKV 789ms, Access 178ms (browser 9.2s; owned fixture 375.59s).
Full Console provides a second Start sample: ChunkDB 255ms, DiskIO 466ms,
ChunkKV 3800ms, Access 148ms. Keep the 5s native response budget: 3s fails
legitimate recovery, while these samples do not justify 10s. These samples
and the prior 3.15s listener sample establish variation, not percentiles. Keep per-service millisecond output for full-suite verification. Process/DOM assertions stay at 3s. Recovery and
readiness behavior are unchanged. Complete UI passes 229/229; the native
full Console rerun passes (350 Rust executions, zero failed/ignored; all
23 native browser cases, including the three dedicated phases).

## 3. Final verification and documentation

- [ ] **Controlled crash/race coverage**: consolidate before/after durable-step
  tests, ambiguous replies, stale revisions/grants, concurrent checkpoints and
  both restarts. Assert exclusive ownership, WAL order, acknowledged values,
  exact-root lineage and eventual recovery. Capture authoritative metadata before
  teardown for any newly failing transition; retain actual crash/core evidence.
- [x] **Complete Console matrix**: rerun native inspection, journal interruption
  and dedicated production split/count-placement phases after confirmed changes. Eight
  independent tasks already pass; rerun them only where new diffs affect them.
  Complete task passes: 350 Rust executions, zero failed/ignored; all 23 native
  browser cases run across four fixture phases. Updated `test.md` with the
  approximate 1588.13s complete task interval and separate phase timings.
- [ ] **Performance and quality gates**: inspect scans/serialization/locking and
  bound planning/diagnostic work. Run affected tests, fmt, clippy, relevant C++
  gates and UI lint/specs. Make performance claims only from measured workloads.
- [ ] **Reconcile permanent docs**: formula, thresholds and UI contract are
  updated. Track deferred tree metrics separately and resolve remaining cutover
  Open Issues only after review and passing regressions; remove this plan after
  final acceptance.

## Latest focused audit verification

- Independent native phases now pass without ignored cases: prerequisite-plan
  resumption 19.32s, journal owner interruption 54.26s, and production split/
  count convergence with complete data checks 350.85s (4/4/4).
  The plan fixture pauses after ChunkDB rather than before its three instances
  exist. UI observes registered DiskIO restart every 2s when Capacity is not
  loaded; 20 focused hook tests pass. Serial deployment responses are checked
  separately before durable progress. Core storage/transition behavior is unchanged.
  Logs: `native-plan-response-order.out`, `native-journal-final.out`, and
  `native-count-final.out` under the balance-policy artifact directory.
- Full workspace Rust clippy, fmt, CI task mapping and UI lint/build pass.
  Complete UI passes 166 unit plus 63 browser cases (229/229), 307.40s.
  An earlier complete rerun passed automatic service
  deployment but failed bucket clicking because creation consumed 2.92s of the
  same action window. The test now asserts the bucket creation response before
  selecting its UI item; each original action/response budget remains 3s.
- Reload failure is diagnosed separately: the old page's canceled server-list
  fetch persisted `failed` over a saved `waiting` plan. Page unload now stops
  the runner and guards cancellation paths before failure publication; cached
  history restoration reloads persisted authority. The 21 hook tests and entire
  eight-case rack/node spec pass (32.5s browser). Full UI acceptance passes
  in `ui-reload-acceptance-final.out`; no timeout or assertion is weakened.
- 21 server cases pass across catalog transition, control store, split recovery,
  current assignment, transfer state machine and transfer storage.
- 10 library cases pass across native split handoff, split handoff recovery,
  ingress and prepared-candidate retry. Zero ignored. These are focused evidence,
  not complete real-process coverage of every listed crash/race window.
- Current count-only Core passes 994/994, zero ignored; UI passes 166 unit and
  63/63 browser cases. Earlier Group 1990 replica 19902 remained unknown/term 1
  while its leader was term 2 in a failed observation window. Preserved trace:
  `.crowdb-runtime/artifacts/balance-policy-20261009/ui-leader-election-failure/`.
  Log: `.crowdb-runtime/artifacts/balance-policy-20261009/count-only-core.out`.
  Tree metrics remain deferred to R228.

## Evidence and file inventory

- Successful final independent matrix:
  `.crowdb-runtime/artifacts/measure-tests/20261009T022131.804646Z/`.
  Core/Storage/Access: `20261009T002906.349723Z/` under the same directory.
- Latest UI/Core/Storage pass: `20261009T053116.399322Z/` under the same
  measurement directory. Latest incomplete Console attempt:
  `20261009T052410.043897Z/`. Balance-specific focused results and final
  clippy output: `.crowdb-runtime/artifacts/balance-policy-20261009/`.
  Withdrawn readiness/order experiments:
  `.crowdb-runtime/artifacts/native-catalog-readiness-experiments/`.
  Earlier speculative cutover changes:
  `.crowdb-runtime/artifacts/cutover-review-withdrawn/`.
- Production: protocol policy/status; Group-0 monitor/catalog; ChunkKV server
  storage, serving, routing, activation/reconcile; owning ChunkKV/tree libraries.
- Unit/integration: server planning, control-store, split/transfer recovery,
  catalog transition and native overlay tests. E2E: web native provisioning/
  balance and existing catalog, journal, Iceberg/S3 browser specs.
