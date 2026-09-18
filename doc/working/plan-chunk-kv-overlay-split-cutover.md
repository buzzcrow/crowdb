<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk-KV Overlay Split Cutover Plan

Upstream: [R174](../backlog/R174-chunk-kv-overlay-split-cutover.md),
[R175](../backlog/R175-chunk-kv-child-tree-balance.md),
[chunk-KV design](../design/chunkds/design-crowdb-chunk-kv.md), and
[chunk-KV server design](../design/chunkds/design-crowdb-chunk-kv-server.md).

Goal: complete R174's local, no-request-blocking split: retain the existing
parent as the left partition, create one child, route post-prepare writes
directly to independent writers, and publish g2 only after both local writers
are durable. R175 child-owner balance begins immediately after R174 acceptance.

## Scope Decision

- R174 is the only active requirement. A split must not make a request wait for
  shared-view publish, tree checkpoint, materialization, or catalog refresh.
- R175 is the next requirement. Existing transfer and balance code is
  unverified scaffolding until R174 acceptance; it must not affect R174's
  request path. Proactive child-owner balance is disabled in the group-0
  planner until R174 is accepted, then becomes the starting point for R175
  implementation and acceptance. Dead-owner recovery remains independent.
- Generation is catalog reference information. An old client generation resolves
  through local split lineage. Ownership epoch remains an internal writer/WAL/tree
  durability fence, not an RPC routing precondition.

## Session Status — 2026-09-18

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
ms, then exposed a pure local-split lifecycle defect: a second split plan for
an already split local writer repeatedly failed with `split plan does not
identify this parent range`; restart then failed with `tree apply completion
is unknown`.

### Current Bug and Next Diagnosis

- [~] **Verify repeated local split and restart**: a second local split now
  selects the exact current writer before falling back to its old-parent
  dispatcher, covered by `repeated_local_split_uses_current_retained_writer`.
  Rerun the real-process regression, verify the retained parent entry carries
  `tail_overlay`, and trace
  `ChunkKvStorage::recover_partition()` through
  `Partition::recover_native_prepared_overlay()`. Files:
  `app/crowdb-chunk-kv-server/src/{catalog/transition.rs,storage.rs,main.rs}`
  and `lib/crowdb-chunk-kv/src/partition.rs`.
- [ ] **Add a restart regression test for both split halves**: construct a
  committed retained-parent-plus-child catalog, stop the source process, and
  assert that recovery opens both entries through the prepared-overlay path and
  serves a pre-split key from each side. This must fail when either catalog
  artifact omits its parent-stream overlay. Files:
  `app/crowdb-chunk-kv-server/tests/` and storage/server test helpers.
- [ ] **Finish direct ingress cleanup**: `SplitIngressRoute::Buffering`,
  `begin_split_finalization()`, and the old parent drain remain in
  `partition/split.rs`. The local session cleanup prevents their state from
  persisting after handoff, but does not yet satisfy the desired design where
  writers receive post-prepare WAL/memtable mutations before shared-view bulk
  publish. Remove the buffer and move shared-view bulk persistence behind
  immediate writer ingress.

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
   both E2Es. R175 balance remains out of scope until these R174 gates pass.

## Data-Path Gate Audit and Handling Notes

| Path | Existing gate | Request effect | Decision and implementation note |
|---|---|---|---|
| Split mutation | `SplitIngressRoute::Buffering` then `forward_into` | Completion waits for final checkpoint/publish; bounded queue can reject | Remove. Construct/install the two writer ingress route before shared-view bulk work. Each mutation selects its writer by split key and appends directly to that writer WAL/memtable. |
| Split mutation | Buffer capacity returns `Overloaded` | A split-time burst can reject writes | Remove with the buffer route. Normal per-writer admission remains the only bounded write admission. |
| Split finalization | `begin_split_finalization()` waits for old admitted mutations | Old drain is a cutover gate | Remove from foreground handoff. The only atomic foreground change is replacing parent ingress with the two-writer route; all durable shared-view work continues behind it. |
| Split read | Buffering reads old parent while buffered writes have no visible writer state | Read-your-write and split-time visibility are incomplete | Remove buffering. Once direct ingress is installed, reads use the same key router and selected writer tree/memtable. |
| Shared memtable | Shared-view publish/checkpoint lies between buffer and writer installation | Bulk persistence delays client completion | Keep bulk range publish, but move it behind direct ingress. It owns only prepare-entry data; release after both durable frontiers, never by per-entry read/delete. |
| Point routing | Stale point route is observed but current catalog/grant still selects one entry | Mostly corrected, but lease/catalog coupling remains | Keep stale-route metric only. Resolve key through local lineage first; pass the selected writer's internal epoch to the partition API. |
| Multi-get and batch | `matches_routing()` rejects stale generation/epoch | Old clients fail while point requests succeed | Remove RPC topology equality check. Validate each key/range, then use the same local lineage resolver as point requests. |
| Seek | Current catalog chooses one writer | A g1 parent seek can miss the child side | Resolve the supplied key through the parent dispatcher when local lineage exists, then call that writer. |
| Scan | Current catalog range and continuation topology are required | g1 parent scan returns refresh/not-my-range after g2 | Add a dispatcher scan: clip once against the old logical parent range, scan retained writer then child in forward order (reverse order for reverse scan), and encode continuation with logical parent topology plus writer boundary. |
| Serving authority | Grant generation and exact `(partition, epoch)` assignment gate authorization | Local ready writers can be fenced by catalog-reference mismatch | Retain live owner lease, but authorize known local split lineage independently of client generation. Epoch remains checked only at the selected writer/WAL/tree boundary. |
| Catalog reconcile | Exact id/epoch/range/stream comparison triggers recovery | Retained parent may be re-opened as a replacement | Keep exact comparison for ordinary recovery. For a recorded local split artifact, adopt its writer identity directly; never recover a retained parent merely because g2 shrank its range. |
| Grant activation and registry | Split-finalizing parent was treated as recovering | Group-0 stops renewing g1 lease during prepare | Keep it advertised as serving while it owns old ingress; this is already corrected. Activation must recognize a local dispatcher instead of reactivating the old parent epoch. |
| Heartbeat | Materialization previously ran inline in the heartbeat loop | A slow bulk pass expires the serving lease | Keep materialization in its independent task; heartbeat only observes, renews, and installs grants. This is already corrected. |
| Registry | `SplitFinalizing` was previously reported as recovering | Group-0 incorrectly removes a still-serving old ingress | Keep `SplitFinalizing` non-recovering until direct ingress replaces it. This is already corrected. |
| Balance eligibility | `transition_id.is_none()` and unresolved overlay checks | Delays a later balance, not foreground I/O | Retain. This is control-plane serialization and must not be consulted by request dispatch. |
| Catalog publication | Exact g1 parent CAS and exact artifact proof | Prevents stale control-plane overwrite | Retain. It is an atomic publication invariant, not a client-routing predicate. |
| Journal recovery | Stream name, replay offset, cutover offset, sequence continuity | Detects loss/replay corruption | Retain. Historical stream plus offsets is the parent journal lineage; no process-local parent object is required after restart. |

### Implementation Comments

- `partition.rs`: replace the buffer route with a route that contains two live
  writers and the split key. Its mutation method must be a single atomic route
  selection followed by writer admission; it must not await split maintenance.
- `partition/split.rs`: preparation creates the immutable shared view and the
  writer runtime state separately. The shared view is bulk-published from its
  fixed generation; later writer WAL/memtable data is excluded by construction.
  The durable artifact records both shared-view writer frontiers and historical
  stream replay/cutover offsets.
- `server.rs`: centralize lineage resolution. Point, multi-get, batch, seek,
  and scan all call it, rather than each comparing a client route to catalog g2.
  The resolver may choose a current writer or an old-parent dispatcher, but it
  never proxies to a remote node.
- `serving/lease.rs`: separate lease liveness from catalog-reference equality.
  A request needs a live local owner lease and a locally known lineage; it does
  not need to name the latest generation. Durable writer epoch validation stays
  below this layer.
- `main.rs` and reconciliation: a recorded local artifact suppresses recovery
  for both retained parent and child. Catalog refresh installs catalog/grant
  state only; it must not perform a second retained-parent cutover.
- Tests must assert the negative properties: no queued split mutations, no
  per-entry shared-view drain, no stale-route rejection for supported APIs, and
  no request latency tied to shared-view checkpoint duration.

### Immediate Cleanup Sequence

- [ ] **Centralize local lineage resolution**: introduce one resolver for
  current writers and old-parent dispatchers, then use it from point, multi-get,
  batch, seek, and scan. Do not remove an API-specific route check until its
  resolver preserves the old logical parent range. Files:
  `app/crowdb-chunk-kv-server/src/server.rs` and server tests.
- [ ] **Remove buffered split ingress**: replace the buffer route with two live
  writer routes and make worker construction available before shared-view bulk
  persistence. Files: `lib/crowdb-chunk-kv/src/{partition.rs,partition/split.rs}`
  and partition tests.
- [ ] **Move shared-view bulk persistence after ingress installation**: publish
  and snapshot both filtered views in background, record durable frontiers, and
  release the shared generation only after both succeed. Files:
  `lib/crowdb-chunk-kv/src/{partition/split.rs,partition/tree.rs}` and tree
  integration tests.
## Writer Handoff and Routing

- [ ] **Replace buffered cutover with direct dual-writer ingress**: remove
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

- [ ] **Persist the immutable shared view in background**: bulk publish the
  prepare-entry shared view by range into both trees, durable-snapshot both
  frontiers, and only then release it. Do not read/delete individual memtable
  entries. The live writer memtables/WALs remain independent throughout. Files:
  `lib/crowdb-tree/{include/crowdb-tree/c_api.h,src/btree/,ffi/src}`,
  `lib/crowdb-chunk-kv/src/{partition/split.rs,partition/tree.rs}`, and tree
  and partition integration tests.
- [ ] **Serve old topology through local lineage**: remove stale-generation and
  client-epoch rejection from multi-get, batch mutation, seek, and scan; route
  a g1 parent point/seek by key to the local pair and merge a g1 scan across
  both writer ranges. Keep malformed request, real deadline, local drain, and
  unavailable durability/lease failures as request rejection. Files:
  `app/crowdb-chunk-kv-server/src/`, `lib/crowdb-chunk-kv-client/src/`,
  protocol catalog types, and client/server E2E tests.
- [ ] **Separate owner lease from catalog reference**: authorize a locally
  ready split lineage under a live owner lease without requiring request or
  catalog generation, or a client's old epoch, to equal the new writer. Keep
  exact epoch checks inside WAL/tree ownership and durable artifact validation.
  Files: `app/crowdb-chunk-kv-server/src/{server.rs,serving/lease.rs,main.rs}`
  and lease/server tests.
- [ ] **Move physical persistence off cutover**: prune and checkpoint the
  retained parent, checkpoint the child overlay, materialize inherited packs,
  and retain/reclaim parent stream and tree references only after the child
  snapshot and retry floor release the parent-suffix pin.
  Files: chunk-KV partition maintenance, server transition recovery, and tree
  integration tests.

## Child-Tree Balance (R175, starts after R174 acceptance)

The code named below is prior scaffolding, not an accepted R175 implementation.
Each item remains pending until reviewed against the completed R174 lineage and
verified by its own remote-owner E2E.

- [ ] **Define one reusable tail-handoff artifact**: extend the persisted
  transfer transition with a pinned source base manifest, source stream and
  retry floor, preparation cursor, exact handoff cursor, target stream start,
  target epoch, readiness limits, forwarding grace, and source-release proof.
  Reject balance while a child still references its split-parent suffix.
  Files: `lib/crowdb-protocol/src/chunk_kv.rs`, group-0 transition storage,
  protocol tests, and transition tests.
- [ ] **Prepare the remote target while the source serves**: open the exact
  shared range-bounded manifest on the target, validate page and stream
  identities, replay the source tail into a durable target overlay, and enforce
  record, byte, estimated-time, and deadline readiness bounds before requesting
  a source fence. Drop unpublished target state on a preparation failure.
  Files: server transition runtime/storage, chunk-KV overlay recovery, and
  deterministic target preparation tests.
- [ ] **Hand off one writer at cursor C**: close source assignment, drain only
  requests that already selected it, persist the release proof and final source
  cursor, then publish `TargetCatchingUp`. The source returns a target hint and
  never appends again; the target returns bounded `NotReady` until the sealed
  suffix reaches C, then installs its writer epoch and becomes `Serving`.
  Files: protocol RPC/catalog types, server authority and transition runtime,
  routed client retry handling, and E2E transition tests.
- [ ] **Recover every balance phase from proofs**: resolve source/target crash,
  ambiguous catalog publication, and lease expiry from transition, catalog,
  manifest, tail, and grant state. Never infer authority from loaded pages,
  heartbeats, or volatile memtables. Files: server monitor/control store,
  transition state machine, startup recovery, and failure-injection tests.
- [ ] **Materialize and reclaim balance state in background**: checkpoint the
  target overlay, materialize shared packs, retain source tree/stream/retry
  history through catalog and forwarding grace, then remove source objects and
  forwarding state only after every pin clears. Files: chunk-KV maintenance,
  server transition cleanup, metrics, and GC integration tests.

## Exact Manifest Recovery

- [ ] **Retain exact generations through restart**: persist a transition-scoped
  manifest pin before publishing an artifact that references the generation,
  constrain manifest and referenced-pack reclamation by the oldest live pin,
  and release it only after catalog publication clears the overlay and
  transition identity. Use immutable snapshots and CAS rather than a read-path
  lock. Files: chunk root-catalog callbacks and storage, split/balance
  transition persistence, materialization cleanup, and GC tests.
## Verification and Cleanup

- [ ] **Add deterministic lifecycle tests**: cover grant renewal during
  Preparing, child-tail restart before checkpoint, writer-boundary exactly-once
  behavior, bounded post-cutover admission, stale point route, and catalog
  ambiguity. Files: crate `tests/*_test.rs` and server/client integration
  tests.
- [ ] **Add sustained split E2E**: keep routed 1 MiB-target hot traffic live
  through every observed split, capture p50/p99/p999/errors and correlated
  split metrics, then verify restart replay. Files:
  `tools/bench-chunk-kv-regression.sh`, client load tool, and
  `doc/working/chunk-kv-split-repro.md`. The current 12,000 × 4 KiB,
  concurrency-32 run fails in round two: 64 requests enter buffered cutover,
  exceed the five-second RPC deadline, and are later persisted by the server.
  The acceptance test must fail if post-split workload does not progress within
  30 seconds; do not mask the defect by extending the client deadline.
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
