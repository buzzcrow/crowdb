<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk-KV Overlay Split Cutover Plan

Upstream: [R174](../backlog/R174-chunk-kv-overlay-split-cutover.md),
[R175](../backlog/R175-chunk-kv-child-tree-balance.md),
[R173](../backlog/R173-s3-console-cluster-cli.md),
[chunk-KV design](../design/chunkds/design-crowdb-chunk-kv.md), and
[chunk-KV server design](../design/chunkds/design-crowdb-chunk-kv-server.md).

Goal: complete R174's local, no-request-blocking split: retain the existing
parent as the left partition, create one child, route post-prepare writes
directly to independent writers, and publish g2 only after both local writers
are durable. R175 child-owner balance begins immediately after R174 acceptance.
After R175 acceptance, continue with R173 so the CLI can deploy the cluster and
perform bucket and object CRUD.

## Scope Decision

- R174 is accepted. Its foreground path does not make a request wait for
  shared-view publish, tree checkpoint, materialization, or catalog refresh,
  and the sustained four-partition restart regression passes.
- R175 is accepted at the operational cutoff: the smooth live-target handoff,
  bounded initialization path, WAL-only foreground writes, exact fence, and
  primary recovery proofs pass their focused gates. Remaining hardening is
  recorded under Open Issues.
- R173 is active. Its operator acceptance target is a CLI-deployed persistent
  mini cluster with working bucket/object CRUD and list; an existing data
  directory is reopened, while an empty location is initialized.
- Generation is catalog reference information. An old client generation resolves
  through local split lineage. Ownership epoch remains an internal writer/WAL/tree
  durability fence, not an RPC routing precondition.

## Session Status — 2026-09-19

### Current Evidence

The fully rebuilt E2E at `bench-log/chunk-kv-regression-20260918-231648` ran
30,000 × 4 KiB writes with concurrency 32 and a 60-second control window. It
completed its online workload with 0 errors in 43.014 s (697 ops/s, p99
154.205 ms), catalog generation 3, and two partitions. Node1 restart then
failed immediately on all three attempts with `tree root owner epoch is
stale`. A shorter reproduction identified `tree_id=1, current_epoch=3,
requested_epoch=2`: proactive R175 balance preparation advanced the shared
tree authority while the R174 source catalog still named epoch 2. This is an
out-of-scope transition leaking into the R174 regression, not evidence that
the same-process local split itself requires another root epoch. After
disabling proactive balance, the run at
`bench-log/chunk-kv-regression-20260918-233758` no longer produced that epoch
divergence. It completed 10,000 × 4 KiB writes with 0 errors and p99 109.279
ms. The later runs through
`bench-log/chunk-kv-regression-20260919-002026` isolated two repeated-split
defects: parent lookup selected the old dispatcher before the current retained
writer, and the one-session-per-parent registry rejected the next epoch under
the same stable parent ID. Both are corrected and covered by
`repeated_local_split_uses_current_retained_writer`; three consecutive writer
installations now complete without either old error.

Those runs also found that group-0 published the retained catalog entry with
the pre-split `parent_artifact`, even though the live retained writer used
`retained_parent_artifact`. That combined a new owner epoch with the old
tree/stream identity, caused the next split's source-stream validation to fail,
and made restart replay the wrong tree. The publisher now uses the retained
artifact, with a domain-monitor regression assertion. The first rebuilt run,
`bench-log/chunk-kv-regression-20260919-002916`, completed 10,000 × 4 KiB
writes with 0 errors and p99 109.352 ms, then advanced restart to an exact
manifest mismatch: the artifact recorded tree manifest 4/root manifest 2,
while reopening root manifest 2 exposed tree checkpoint 2 rather than 4.

The mismatch was an observation bug, not a failed root publication.
`ct_snapshot_state` returned the mutable in-memory tree `version_`; range
rebuild, split-view publication, and flush advance that counter independently
of the durable snapshot sequence. The tree now records the durable snapshot
sequence and applied frontier separately at successful snapshot commit and
recovery. The native exact-generation regression rebuilds a range, advances
and checkpoints it, then reopens the recorded root generation and observes the
same checkpoint. The change also exposed a catalog cleanup assumption that
only the child owned an overlay. Materialization now clears each retained or
child overlay independently and removes the shared transition identity only
after both are independent.

The rebuilt real-process run at
`bench-log/chunk-kv-regression-20260919-011301` completed 10,000 × 4 KiB writes
with 0 errors and p99 106.813 ms, completed three consecutive local splits to
four hosted partitions, then restarted node 1 in 1.051 s, recovered all four
partitions, and read a pre-restart value. The exact-manifest and stale stream
errors did not recur. R174 remains active while the existing split-writer WAL
tail retry is verified; R175 remains disabled.

The mixed real-process run at
`bench-log/chunk-kv-regression-20260919-015121` completed 10,000 operations at
4 KiB with 25% reads, concurrency 32, 0 errors, and p99 92.922 ms. It reached
catalog generation 12 and five local partitions with zero admission
backpressure, then restarted node 1 in 1.053 s, recovered all five partitions,
and read a pre-restart value. Direct ingress now has a worker-ordered route
frontier and serves through shared-view publication. Split-writer streams have
stable identities before routing and now remain valid retry inputs after they
contain acknowledged mutations.

The final R174 real-process run at
`bench-log/chunk-kv-regression-20260919-081253` completed 10,000 mixed
operations at 4 KiB, 25% reads, and concurrency 32 with 0 errors and p99
94.951 ms. It completed three consecutive local splits to four partitions,
reported two retained split finalizations with zero catch-up lag and admission
backpressure, restarted node 1 in 1.051 s, recovered all four partitions, and
read a pre-restart value. The deterministic retry regression also preloads
both stable writer journals at `C+1` and proves preparation preserves their
acknowledged tails. R174 is accepted; R175 is now active.

The simplified data-path gate run at
`bench-log/chunk-kv-regression-20260919-084055` completed 10,000 mixed
operations at 4 KiB, 25% reads, and concurrency 32 with 0 errors and p99
94.244 ms. It completed three splits to four partitions with two recorded
split finalizations, zero catch-up lag and admission backpressure, restarted in
3.113 s, recovered five partition handles, and reached ready state. The
cutover marker now seeds both writers from the bounded parent retry cache
instead of rescanning parent WAL history.

The 60,000-operation run at
`bench-log/chunk-kv-regression-20260919-091542` kept 4 KiB mixed traffic live
for 109.919 s at concurrency 32 and 25% reads. It completed all 60,000
operations with zero errors, p99 237.644 ms, and three hosted partitions at
workload completion. Three local split writer pairs were installed with
explicit retained/child stream identities. Restart then reproduced the open
bug below. This run also verified that a completed epoch's process-local
materialization marker must be keyed by `(partition_id, owner_epoch)`; keying
only by partition ID let a successor epoch appear independently recoverable
and start the next split before its own overlay was materialized.

The corrected 60,000-operation run at
`bench-log/chunk-kv-regression-20260919-095848` completed in 108.926 s with
zero errors, p99 222.639 ms, three split finalizations, four local partitions,
zero catch-up lag and admission backpressure, then restarted in 2.083 s,
recovered all four partitions, and read a pre-restart value. The exact-root
failure did not recur.

## Open Issues

- R175's proof-backed happy path and primary restart paths are operational, but
  the complete source/target crash matrix still lacks process-level fault
  injection. In particular, a target that disappears after group 0 records
  `AwaitingFence` can cause an availability pause until that durable target is
  restarted; the epoch/catalog proofs still prevent dual writers. Per the
  implementation cutoff, leave this for a later hardening pass.
- R175 background transfer materialization, forwarding-grace expiry, and final
  source object reclamation remain follow-up work. They are outside the
  foreground handoff and do not block the R173 persistent mini-cluster path.

## Resolved Bugs

- **R174 exact latest-root recovery after materialization**: the retained
  partition `(1, 1)` recorded split base checkpoint 10751 and cutover 17695.
  Its overlay was later cleared from the catalog, which is only permitted
  after materialization reports complete and a checkpoint at least through
  cutover succeeds. Restart nevertheless reopened the latest durable root at
  checkpoint 10751, while the retained writer WAL correctly began at 17696.
  Recovery therefore failed with `expected 10752, got 17696 at offset 0`.
  This was not a missing WAL record. Before the owner installed the split
  catalog, its heartbeat matched the new retained writer to the old catalog
  entry by partition ID alone. Since that old entry had no overlay, group-0
  prematurely cleared the new epoch's overlay without materialization. The
  fallback now requires the exact partition ID, epoch, range, and stream.
  Reproduction:
  `bench-log/chunk-kv-regression-20260919-091542/chunk-kv-1-log/`.
  Regression and passing run:
  `prepared_split_lineage_suppresses_catalog_recovery` and
  `bench-log/chunk-kv-regression-20260919-095848`.
- **Inherited retry positions must not move a writer WAL watermark**:
  retained and child writers inherit bounded parent retry results so an old
  request can return without another append. Those results contain parent
  stream positions and must not advance a new writer's replay offset. The
  mutation worker now selects the oldest retained result belonging to its own
  stream, with a deterministic split-session regression.

### Current Status

- [x] **Verify repeated local split and restart**: repeated same-ID successor
  sessions, current-writer selection, and the retained catalog artifact are
  corrected. Split lifecycle logs now identify planned, readiness, installed,
  and catalog-committed transitions with writer epochs and durable frontiers.
  The native exact-generation regression proves the root generation and
  durable checkpoint reopen as one identity, and the real-process restart
  recovered all four partitions after three local splits. Files:
  `app/crowdb-chunk-kv-server/src/{catalog/transition.rs,storage.rs,main.rs}`
  `app/crowdb-kv-server/src/background/domain_monitor/chunk_kv/catalog.rs`,
  `lib/crowdb-tree/{include/crowdb-tree/btree/tree.h,src/{c_api.cpp,snapshot/persist.cpp},ffi/tests/ffi_test.rs}`,
  and `lib/crowdb-chunk-kv/src/partition.rs`.
- [x] **Add a restart regression test for both split halves**: construct a
  committed retained-parent-plus-child catalog, stop the source process, and
  assert that recovery opens both entries through the prepared-overlay path and
  serves a pre-split key from each side. This must fail when either catalog
  artifact omits its parent-stream overlay. Files:
  `app/crowdb-chunk-kv-server/tests/` and storage/server test helpers.
  The deterministic partition regression reopens both writer ranges from the
  same parent journal and verifies an in-range pre-split tail key on each side;
  the catalog cutover regression independently requires the exact retained and
  child overlays on both published entries.
- [x] **Finish direct ingress cleanup**: the session path has no buffering or
  admission drain. One ordered worker control item fixes `C`, installs two
  live writers, and makes raced old-queue requests forward by key. Native
  writer trees borrow the source L0 generations for immediate reads and
  conditional mutations; source rotation and filtered bulk publication occur
  afterward. `begin_split_finalization()` remains only on the legacy
  single-child API, not the server split-session path.
- [x] **Serve stale local topology through one lineage**: after g2 publication,
  a g1 parent route resolves to its local dispatcher. Point, multi-get, batch,
  seek, and scan use current writer epochs internally; seek crosses the split
  boundary, directional scans merge both writers, and continuation retains the
  old logical topology. Cross-writer reads validate both current assignments.

### Next-Test Order

1. Rebuild every process that serializes or publishes split state:
   `crowdb-cli`, `crowdb-kv-server`, `crowdb-chunk-kv-server`, and
   `crowdb-chunk-kv-client`.
2. Add and run the two-half restart regression before changing recovery code
   again; assert the exact retained-parent catalog overlay rather than infer it
   from a later tree failure.
3. Run the same 30,000-write continuous E2E again with a 60-second control
   observation window. Require: 0 write errors, catalog generation at least 2,
   at least one retained local split-finalization metric, and successful node1
   restart/read replay.
4. After restart passes, run the mixed read/write split load that forces gets
   through tree storage, then implement direct dual-writer ingress and repeat
   both E2Es. Completed by the final R174 run above.

### Resolved Design Decision

- **Route frontier and memtable seal are independent.** The parent mutation
  worker processes an ordered `SplitCutover` marker that records the last
  parent-ordered right-range sequence `C` and installs direct range routing.
  It does not seal the parent memtable. The child starts its WAL and live
  memtable immediately; the parent may continue adding only left-range entries
  to its current memtable. A later rotation places that generation in the
  shared split queue, and background publication filters the child side at
  `C`. Raced requests reaching the old queue after the marker are forwarded by
  key. This removes the need for `SplitIngressRoute::Buffering` without making
  seal, checkpoint, or artifact publication part of the foreground handoff.

### Resolved Recovery Correction

- **Readiness publication is not a data-durability boundary.** The persisted
  `ParentPreparing` transition already assigns stable retained-parent and
  child stream identities. Acknowledged post-route mutations are durable in
  those independent journals even before `SplitReadinessProof` publication;
  shared-view publication owns only the old parent's `<= C` history. Retry now
  permits non-empty split-writer streams, rebuilds the filtered parent base,
  and uses the existing prepared-overlay recovery path to replay each writer's
  `> C` journal tail. Transfer preparation retains its empty-target check. No
  extra cutover state or foreground durability barrier is required.

## Data-Path Gate Audit and Handling Notes

The final data path has three layers. Client topology is never serving
authority, and background durability or cleanup never becomes request
admission state.

### Foreground Request Path

- Validate the request envelope and deadline, resolve the key or logical range
  against the current catalog plus any recorded local split lineage, and
  observe stale client topology only for metrics and refresh hints.
- Authorize every writer the operation will actually access against one current
  catalog and aggregate serving grant. The grant generation and exact
  `(partition_id, owner_epoch)` assignment remain mandatory even when the
  client's generation or epoch is stale.
- Pass the resolved writer's current epoch into its partition API. Normal
  per-writer request and byte admission are the only bounded mutation queues.
- Preserve a minimum journal position across lineage dispatch. The writer
  decides whether it belongs to its own stream, is covered by an inherited
  source frontier, or is unrelated and invalid.
- Point and seek normally authorize one writer. A boundary-crossing seek or
  logical-parent scan authorizes the additional writer only when it is used.

### Background Publication Path

- `SplitCutover` fixes `C` and installs the complete two-writer route at one
  parent-worker queue position. Base rebuild, checkpoint, parent-tail catch-up,
  and all other work that does not require `C` finish before that marker. The
  marker uses the bounded in-memory retry cache instead of replaying parent
  history; only a previously acknowledged destination tail may be replayed
  before route installation. Requests never wait for checkpoint or filtered
  publication.
- The detached old-parent generation contains only history through `C`.
  Post-route mutations are already isolated in retained-parent or child
  WALs/memtables. Bulk-publish the generation once per range and release it only
  after both durable frontiers are recorded.
- Catalog publication, materialization, heartbeat observation, and balance
  eligibility run independently. They may delay topology cleanup or a later
  transition but are never consulted by request dispatch.

### Recovery and Reclamation Path

- Exact catalog CAS, artifact identity, root generation, stream manifest,
  replay/cutover offsets, and sequence continuity remain fail-closed recovery
  invariants rather than client-routing predicates.
- Reconciliation adopts an exact matching local writer. When no such runtime
  exists, including process restart, it recovers the exact catalog artifact;
  range shrink alone neither proves reuse nor forces replacement.
- Tree generations, source stream bytes, retry results, and shared packs remain
  pinned until every catalog, overlay, forwarding, recovery, and retry-floor
  reference is gone.

### Immediate Cleanup Sequence

- [x] **Preserve lineage read ordering and simplify ingress storage**: pass
  minimum journal positions unchanged to split writers so their inherited
  frontier validation remains effective, replace the single-variant
  `SplitIngressRoute` with `ArcSwapOption<SplitIngress>`, and add deterministic
  inherited, unrelated-stream, and raced-route tests. Files:
  `lib/crowdb-chunk-kv/src/partition.rs`, server ordered-read helpers, and
  partition/server tests. `SplitIngress` is now the directly swapped route,
  inherited parent positions reach `Partition::wait_applied`, unrelated stream
  positions fail closed, and both affected crate suites pass.
- [x] **Authorize only resolved writers**: consolidate point, seek, and scan
  resolution so client topology remains a routing reference while current
  catalog/grant assignments authorize exactly the writer set used. Preserve
  old logical-parent scan and continuation behavior without adding a combined
  lifecycle state. Files: `app/crowdb-chunk-kv-server/src/server.rs` and server
  tests. Point operations now receive the selected writer directly; seek
  authorizes its primary writer first and authorizes the second writer only
  when the first lookup crosses the split boundary. Scans authorize both
  writers because they visit both ranges. A retained-only grant regression
  proves a local seek does not depend on unused child authority.
- [x] **Minimize the worker cutover marker**: keep destination tree rebuild,
  checkpoint, and parent-tail catch-up outside the parent queue marker. At `C`,
  seed both live writers from the mutation worker's bounded retry cache instead
  of rescanning parent WAL history; replay only an existing destination tail,
  attach the fixed view, and install the route. The paused preparation test
  proves parent mutations progress before the marker and the paused
  bulk-publish test proves they progress after it. Files:
  `lib/crowdb-chunk-kv/src/{partition.rs,partition/split.rs}` and partition
  tests. The retry regression still preloads both destination WALs at `C+1`,
  while the writer-install regression proves a pre-cutover request result is
  served from the seeded cache without another append.
- [x] **Verify the simplified gate contract**: run chunk-KV and server tests,
  Rust formatting and lint, then the real-process mixed split regression. The
  regression must retain zero request errors and successful restart replay.
  Files: affected test suites and `tools/bench-chunk-kv-regression.sh`. Rust
  formatting, `rs-lint`, both affected suites, and the real-process mixed
  split/restart regression pass.

- [x] **Centralize local lineage resolution**: introduce one resolver for
  current writers and old-parent dispatchers, then use it from point, multi-get,
  batch, seek, and scan. Do not remove an API-specific route check until its
  resolver preserves the old logical parent range. Files:
  `app/crowdb-chunk-kv-server/src/server.rs` and server tests.
- [x] **Remove buffered split ingress**: replace the buffer route with two live
  writer routes and make worker construction available before shared-view bulk
  persistence. Files: `lib/crowdb-chunk-kv/src/{partition.rs,partition/split.rs}`
  and partition tests.
- [x] **Move shared-view bulk persistence after ingress installation**: publish
  and snapshot both filtered views in background, record durable frontiers, and
  release the shared generation only after both succeed. Files:
  `lib/crowdb-chunk-kv/src/{partition/split.rs,partition/tree.rs}` and tree
  integration tests.
## Writer Handoff and Routing

- [x] **Replace buffered cutover with direct dual-writer ingress**: remove
  `SplitIngressRoute::Buffering`, `SplitIngressBuffer`, and sequential
  `forward_into`. Establish retained-parent and child writer memtables, then
  atomically install one key router before the shared-view bulk work. Every
  subsequent mutation goes directly to its selected writer WAL and memtable;
  completion never waits for checkpoint, shared-view publish, materialization,
  or catalog generation. The shared view is forked from the old parent
  memtable into a split-owned shared view at prepare entry, retain it as the
  immutable common view. Rebuild and flush the shared
  view into both range trees through one native split-session handle without
  materializing or deleting individual entries. The shared view is not a
  generic frozen table and ordinary flush never owns it. The handle releases
  the shared generation only after both filtered page batches have durable snapshots;
  persist durable WAL ownership before group-0 exposes g2.
  Files: `lib/crowdb-tree/{include/crowdb-tree/c_api.h,src/btree/,ffi/src/}`,
  `lib/crowdb-chunk-kv/src/{partition.rs,partition/split.rs,partition/tree.rs}`,
  `app/crowdb-chunk-kv-server/src/{serving/worker.rs,server.rs,main.rs}`, and
  split lifecycle tests.

- [x] **Persist the immutable shared view in background**: bulk publish the
  prepare-entry shared view by range into both trees, durable-snapshot both
  frontiers, and only then release it. Do not read/delete individual memtable
  entries. The live writer memtables/WALs remain independent throughout. Files:
  `lib/crowdb-tree/{include/crowdb-tree/c_api.h,src/btree/,ffi/src}`,
  `lib/crowdb-chunk-kv/src/{partition/split.rs,partition/tree.rs}`, and tree
  and partition integration tests.
- [x] **Serve old topology through local lineage**: remove stale-generation and
  client-epoch rejection from multi-get, batch mutation, seek, and scan; route
  a g1 parent point/seek by key to the local pair and merge a g1 scan across
  both writer ranges. Keep malformed request, real deadline, local drain, and
  unavailable durability/lease failures as request rejection. Files:
  `app/crowdb-chunk-kv-server/src/`, `lib/crowdb-chunk-kv-client/src/`,
  protocol catalog types, and client/server E2E tests.
- [x] **Separate owner lease from catalog reference**: authorize a locally
  ready split lineage under a live owner lease without requiring request or
  catalog generation, or a client's old epoch, to equal the new writer. Keep
  exact epoch checks inside WAL/tree ownership and durable artifact validation.
  Files: `app/crowdb-chunk-kv-server/src/{server.rs,serving/lease.rs,main.rs}`
  and lease/server tests.
- [x] **Move physical persistence off cutover**: prune and checkpoint the
  retained parent, checkpoint the child overlay, materialize inherited packs,
  and retain/reclaim parent stream and tree references only after the child
  snapshot and retry floor release the parent-suffix pin.
  Files: chunk-KV partition maintenance, server transition recovery, and tree
  integration tests.
  Both writer base checkpoints precede final handoff; the cutover records
  durable parent-tail overlays without checkpointing dirty writer pages.
  Ownership materialization and overlay cleanup run in their independent
  background tasks after catalog publication.

## Child-Tree Balance (R175, starts after R174 acceptance)

The existing transfer types and two-generation catalog publication are
scaffolding. They are accepted only after the following live-target path passes
its remote-owner E2E; proactive balance remains disabled until then.

- [x] **Make the handoff proof exact without adding phases**: retain the
  initial readiness artifact as preparation cursor `P`, extend only the target
  artifact to final cursor `C`, and validate exact base/root/stream identity,
  monotonic cursors, target WAL start `C+1`, and release proof. The release plus
  `TargetCatchingUp` catalog entry is narrow authority for unconditional target
  WAL append; it is not read or conditional-mutation authority. Reject balance
  while the child still names a split-parent overlay. Files:
  `lib/crowdb-protocol/src/chunk_kv.rs`, group-0 transition planning/storage,
  protocol tests, and server transition tests.
  Validation now preserves the initial readiness artifact at `P`, requires its
  exact base identity, binds explicit release to target overlay `C`, requires
  final readiness exactly at `C`, and rejects invalid proofs without mutating
  the in-memory reducer.
- [x] **Keep one live async target initialization**: open the exact shared
  range-bounded root, replay through `P` while the source serves, then retain
  the same target tree, memtable, source journal handle, and coroutine through
  final catch-up. Add an ordered worker control operation that advances only
  `P+1..C`; never remove and reopen the live target during a normal handoff.
  Exact-root reopen remains the crash-recovery path. Files:
  `lib/crowdb-chunk-kv/src/partition.rs`, its owned partition modules,
  `app/crowdb-chunk-kv-server/src/{storage.rs,serving/worker.rs}`, and
  deterministic incremental-catch-up tests.
  `TransferCatchUp` now runs through the existing mutation worker, retains the
  prepared handle, validates the exact base and monotonic source suffix, and
  advances only `P+1..C`. The transition worker uses it whenever the live
  target remains hosted and falls back to exact-root recovery only when that
  handle is absent.
- [x] **Coroutine-await reads and conditions, append ordinary writes**: while
  the target is initializing, read and conditional-mutation handlers await the
  shared initialization future within their existing deadline. Ordinary
  unconditional mutations append to the target WAL from `C+1` immediately and
  apply only after the source suffix. Do not add a transfer pending queue,
  readiness polling, executor-thread wait, or hot-path lock. Files:
  `lib/crowdb-chunk-kv/src/partition.rs`,
  `app/crowdb-chunk-kv-server/src/server.rs`, RPC tests, and client deadline
  tests. Only `TargetCatchingUp` enters this branch. Each coroutine wait uses
  the smaller of the request deadline and server cap, a lock-free process-wide
  counter bounds total waiters, and capacity/timeout returns `TargetNotReady`.
  The client honors its delay and returns failure after three unsuccessful
  initialization attempts with the original request identity.
  The shared future now wakes all coroutine waiters after exact catch-up.
  Ordinary writes durably append to the target WAL before initialization and
  are replayed after the source suffix; retries reuse that WAL result. Serving
  requests retain their original direct path and do not touch waiter state.
- [x] **Complete the bounded writer handoff**: stop assigning source-WAL
  records, drain only records already assigned there, persist `C`, publish
  `TargetCatchingUp`, and make the source return the target hint without
  another append. Once initialization covers `C`, publish `Serving` and issue
  the exact target grant without waiting for checkpoint or materialization.
  Files: server authority, transition runtime, group-0 catalog monitor, routed
  client handling, and E2E transition tests.
  Source fencing now has one explicit lifecycle that rejects new mutation
  admission while the existing worker drains every previously admitted write
  before recording `C`. The target catalog path accepts WAL-only writes,
  rejects stale routes with the target hint, replays those target records after
  `C`, and reaches `Serving` without a checkpoint or materialization barrier.
- [~] **Recover every balance phase from proofs**: resolve source/target crash,
  ambiguous catalog publication, and lease expiry from transition, catalog,
  manifest, tail, and grant state. Never infer authority from loaded pages,
  heartbeats, or volatile memtables. Files: server monitor/control store,
  transition state machine, startup recovery, and failure-injection tests.
  Live fencing now requires a fresh group-0 observation of the exact prepared
  target. If the source dies before explicit fencing, recovery discards the
  unpublished target overlay and adopts the lease-excluded source stream so
  the complete durable tail is replayed. The remaining process-level crash
  matrix is recorded under Open Issues and does not block R173.
- [ ] **Materialize and reclaim balance state in background**: checkpoint the
  target overlay, materialize shared packs, retain source tree/stream/retry
  history through catalog and forwarding grace, then remove source objects and
  forwarding state only after every pin clears. Files: chunk-KV maintenance,
  server transition cleanup, metrics, and GC integration tests.

## Exact Manifest Recovery

- [x] **Retain exact generations through restart**: persist a transition-scoped
  manifest pin before publishing an artifact that references the generation,
  constrain manifest and referenced-pack reclamation by the oldest live pin,
  and release it only after catalog publication clears the overlay and
  transition identity. Use immutable snapshots and CAS rather than a read-path
  lock. Files: chunk root-catalog callbacks and storage, split/balance
  transition persistence, materialization cleanup, and GC tests.
  Callback and KV-backed root catalogs persist idempotent transition pins,
  reclamation honors the oldest generation, catalog installation releases a
  pin only after its overlay disappears, and the real-process restart reopened
  every exact retained/child generation.
## Verification and Cleanup

- [x] **Add deterministic lifecycle tests**: cover grant renewal during
  Preparing, child-tail restart before checkpoint, writer-boundary exactly-once
  behavior, bounded post-cutover admission, stale point route, and catalog
  ambiguity. Files: crate `tests/*_test.rs` and server/client integration
  tests.
  Grant renewal, both-half overlay restart, stale routes, ordered lineage,
  catalog ambiguity, and generation pins are covered. A deterministic
  paused-publish test proves both writers accept mutations and
  child conditions see inherited data before bulk publication completes. A
  retry regression preloads acknowledged `C+1` mutations in both writer
  journals, then verifies preparation preserves and serves both tails after
  rebuilding the shared `<= C` history.
- [x] **Add sustained split E2E**: keep routed 1 MiB-target hot traffic live
  through every observed split, capture p50/p99/p999/errors and correlated
  split metrics, then verify restart replay. Files:
  `tools/bench-chunk-kv-regression.sh`, client load tool, and
  `doc/working/chunk-kv-split-repro.md`. The acceptance test fails if
  post-split workload does not progress within 30 seconds; it does not extend
  the client deadline to mask a foreground split stall.
  The workload supports a deterministic read percentage and defaults to 25%
  reads of keys written earlier in each 100-operation window. The mixed run
  above completed repeated splits and exact restart recovery within bounds.
- [ ] **Run acceptance gates and clean up**: run the R174/R175 gates and the
  sustained split/balance workflow, remove both completed requirements and this
  plan, and update the backlog index in the final cleanup commit. Files:
  `doc/design/chunkds/`, `doc/backlog/`, and this plan.

## File List

- `lib/crowdb-chunk-kv/src/{partition.rs,partition/split.rs,partition/tree.rs,metrics.rs,types.rs}`
- `lib/crowdb-protocol/src/` and protocol tests
- `app/crowdb-chunk-kv-server/src/{main.rs,server.rs,serving/lease.rs}`
- `lib/crowdb-chunk-kv-client/src/` and client tests
- `lib/crowdb-chunk-kv/tests/partition_test.rs`
- `tools/bench-chunk-kv-regression.sh`
- `doc/design/chunkds/design-crowdb-chunk-kv*.md`
- `doc/design/chunkds/design-crowdb-chunk-stream.md`

## Tests

- Unit: partition lifecycle, artifact, protocol, journal-tail, retry-result,
  and server authority tests.
- Integration: native child recovery, writer handoff, stale point routing,
  catalog publication, and tree materialization tests.
- E2E: three-node sustained split with routed client traffic and restart replay.
