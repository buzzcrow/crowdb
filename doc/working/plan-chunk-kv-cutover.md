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

## Status — 2026-10-09

- Local commits: `1a471f43`, `407b5cab`, `a664e357`; no push performed.
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
  accepted. Console remains incomplete. Exact counts/times are in `test.md`.
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

## 1. Placement policy review and implementation

Confirmed user direction:

- Partition count and estimated data size contribute to one balance evaluation;
  neither independently overrides the other in a later planning pass.
- Use explicit coefficients and percentage-based tolerance. Small imbalance
  alone does not trigger a move; a move must produce sufficient improvement.
- Range split deliberately does not produce equal-sized children. Use their
  observed estimates, and accept a placement when no worthwhile move exists.
- Weight calculation uses existing cached metadata/retained-pack estimates and
  bounded samples. Never iterate keys/data, count the tree, flush, or scan remote
  storage for balance. Iterating reported partition metadata is permitted.
- Explain weight and move/no-move decisions in UI using the backend's actual
  calculation. Cooldown supplements tolerance; it does not ensure convergence.

Current evidence and gap:

- Both selectors now use the shared weighted score. The former independent
  count correction could undo a weighted move; monitor regression covers the
  replacement policy without relying on cooldown history.
- Strict page cursor 409 after catalog publication is correct. A transient
  4/4/4 observation does not promise stable catalog generation.
- Browser ordering and continuous-generation readiness experiments failed and
  were withdrawn. Do not make the page pass by ignoring 409 or disabling balance.

- [x] **Specify unified score and UI contract**: design §8 defines 80/20 data/count
  weight, 20% relative tolerance and 25% minimum global squared-loss improvement.
  UI spec CKV-06–08 defines weight below Range, owner totals and explanations.
  The user authorized balance/UI implementation; split/move execution stays intact.
- [x] **Verify non-reversal with actual monitor**: a 4/4 placement at 140/20 bytes
  selects the 60-byte partition; the resulting 3/5 at 80/80 stays within tolerance
  across repeated monitor ticks without cooldown hiding a reverse proposal.
  Missing assignment samples defer planning. Five monitor regressions pass;
  this verifies the replacement policy, not the exact earlier runtime cycle.
- [x] **Implement confirmed policy/configuration**: add validated
  coefficients/tolerance/improvement fields with explicit legacy decoding and
  persisted monitor-descriptor handling. Replace independent count-first and
  weighted decisions with the same score before/after a candidate move; retain
  safety filters, transfer bounds and deterministic ties. Reconcile split versus
  placement scheduling without forcing equal split sizes or endless splitting.
  Files: `lib/crowdb-protocol/src/chunk_kv.rs`,
  `app/crowdb-kv-server/src/background/domain_monitor/chunk_kv/balance*`,
  `app/crowdb-chunk-kv-server/src/serving/balance.rs`, deployment/config callers.
- [x] **Keep observation cheap**: reuse existing heartbeat/cached partition
  statistics, aggregate each reported load once, and avoid repeated sample sums
  for every candidate. Specify missing/stale sample behavior and estimate error
  tolerance; introduce no data scans or hot-path I/O/locks. Diagnostic metadata
  publication is bounded, best effort, and runs after planning.
  Files: monitor planning state and owner load aggregation, heartbeat producers.
- [x] **Expose decision explanations**: report policy/version, observation age,
  count/byte contributions, final owner weight, current imbalance, thresholds,
  predicted candidate improvement, and safety/cooldown/no-benefit reasons.
  Bound output to owner summaries and selected/best rejected candidates; do not
  persist or emit all candidate combinations every tick. UI consumes the same
  backend result rather than recomputing the policy.
  Files: `lib/crowdb-protocol/src/chunk_kv/balance.rs`, monitor observation/API,
  `app/crowdb-web/ui/src/` owning ChunkKV inspection components.
- [~] **Verify policy and Console acceptance**: unit-test zero data, rounding,
  tolerance edges, unequal splits and insufficient benefit; monitor-test repeated
  decisions and estimate jitter without reverse moves. Preserve capacity/health/
  overlay/cooldown limits and acknowledged data. Update native preparation to
  approved policy readiness rather than exact counts; rerun dedicated real split/
  weighted acceptance and strict native browser checks. Add UI assertions for
  explanations when implemented. Files: selector tests, native balance fixture,
  existing catalog/lifecycle browser specs; `doc/working/test.md`. UI spec is
  `doc/design/console/design-crowdb-console-ui.md` §20; change it before UI code.
  Focused Rust checks pass 51/51; UI unit tests, lint/build and workspace CI
  clippy pass. Actual native Page test passes and its screenshot confirms Weight
  below Range. Full Console verification remains active: one attempt mixed
  binaries across an additive diagnostic field change, and multipart ListParts
  separately returned zero entries after successful part uploads. Rebuild and
  reproduce before attributing that failure to cutover or changing core logic.
  Latest full UI passes 227/227, including all 63 browser cases; Core passes
  993/993 and Storage 841/841, all with zero ignored cases. A subsequent
  consistent full Console run stops earlier at the node-3 S3 outage test: large
  object allocation connects to the stopped DiskDB endpoint and returns 503.
  The pool refreshes after transport failure but deliberately does not replay an
  ambiguous allocation. Diagnose this independently; do not add blind retries.
  The consistent, isolated normal native restart/multipart case passes in
  24.03 seconds; this does not yet classify its earlier parallel-suite failure.
  The former fourfold hot-value load left retained estimates unchanged and
  deviation at 10.83%, correctly within tolerance. Its attempt was interrupted,
  not accepted or ignored. The fixture now grows the largest seeded range's
  owner using 1-MiB values without forcing a checkpoint or changing production
  observation. This run fails after 968.43 seconds: ordinary memtable flush does
  not publish a checkpoint, and the opened manifest estimate stays unchanged.
  Actual inherited/current Journal browser acceptance passes in 18.7 seconds.
  Final workspace CI clippy passes. Weighted acceptance remains incomplete.
- [ ] **Review current-data estimation boundary before tree changes**: the
  unified policy correctly stops count-driven moves, which previously could
  incidentally checkpoint later writes. `ChunkPageStore::estimated_bytes()`
  reads the opened manifest; heartbeats can be current while this byte estimate
  remains from an older checkpoint. Increasing fixture values alone does not
  fix that boundary. Review a bounded, read-only estimate of current pending
  data using cached allocation/page metadata; avoid cumulative write/WAL bytes,
  tree walks, forced checkpoints, new locks or changes to split/move authority.
  Any writer-side bookkeeping or checkpoint behavior change requires the user's
  prior core-logic review. Do not claim this task is implemented.
  Files: `lib/crowdb-tree/src/backend/chunk/chunk_page_store.*`,
  `lib/crowdb-chunk-kv/src/partition/tree.rs`, owner load observation.
  Evidence: `.crowdb-runtime/artifacts/balance-policy-20261009/` contains
  `native-weighted-observed-load.out`, `weighted-observation.out` and owner
  registry observations. No core split/move or checkpoint change was made.

## 2. Remaining cutover boundary audits

Only change behavior when a concrete regression proves a gap; any core fix
requires the user's review. Existing durable handoff/replay contracts stay intact.

- [ ] **Review publication boundaries**: compare S1–S6/T1–T8 with monitor and
  local publication, revision CAS, grants and startup. Distinguish initial
  unpublished handoff recovery from final published replay bounds.
  Files: monitor `chunk_kv/catalog.rs`, server `catalog/transition.rs`, design.
- [ ] **Recover release before publication**: prove old-source admission stays
  fenced by exact release/lease evidence and reconciliation can still start.
  Files: server `main.rs`, `catalog/recovery.rs`, `serving/transition_runtime.rs`.
- [ ] **Repair publication before phase marker**: cover owner and Serving
  publication independently; exact catalog entries and proofs allow idempotent
  repair and cannot reactivate the source. Files: server `server/activation.rs`,
  monitor `chunk_kv/catalog.rs`, `catalog_transition_test.rs`, transfer recovery tests.
- [ ] **Verify split abort**: reject abort after durable handoff or with stale
  proof; preserve child WAL omitted by the old catalog and resume the same split.
  Files: server `serving/split.rs`, local split owner, split abort/recovery tests.
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

## 3. Final verification and documentation

- [ ] **Controlled crash/race coverage**: consolidate before/after durable-step
  tests, ambiguous replies, stale revisions/grants, concurrent checkpoints and
  both restarts. Assert exclusive ownership, WAL order, acknowledged values,
  exact-root lineage and eventual recovery. Capture authoritative metadata before
  teardown for any newly failing transition; retain actual crash/core evidence.
- [ ] **Complete Console matrix**: rerun native inspection, journal interruption
  and dedicated production split/weighted phases after confirmed changes. Eight
  independent tasks already pass; rerun them only where new diffs affect them.
  Update actual timings/counts in `test.md`; account for Linux skips individually.
- [ ] **Performance and quality gates**: inspect scans/serialization/locking and
  bound planning/diagnostic work. Run affected tests, fmt, clippy, relevant C++
  gates and UI lint/specs. Make performance claims only from measured workloads.
- [ ] **Reconcile permanent docs**: formula, thresholds and UI contract are
  updated. Resolve the current-data estimate boundary and remaining cutover
  Open Issues only after review and passing regressions; remove this plan after
  final acceptance.

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
