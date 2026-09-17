<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk-KV Overlay Split and Child Balance Plan

Upstream: [R174](../backlog/R174-chunk-kv-overlay-split-cutover.md),
[R175](../backlog/R175-chunk-kv-child-tree-balance.md),
[chunk-KV design](../design/chunkds/design-crowdb-chunk-kv.md), and
[chunk-KV server design](../design/chunkds/design-crowdb-chunk-kv-server.md).

Goal: keep split preparation serving, retain a recoverable shared parent
journal suffix for each child, and reduce cutover to a bounded local old-writer
to child-writer handoff without a foreground child checkpoint; after a child is
independently materialized, move it to a better owner through the same durable
tail and bounded handoff contract.

## Contract and Baseline

- [x] **Map current split and authority boundaries**: trace parent sequencer,
  child journals, child recovery, artifact validation, serving-grant refresh,
  routed retry, and the 12,000-operation failure. Record exact extension
  points and invariants before changing behavior. Files:
  `lib/crowdb-chunk-kv/src/partition.rs`,
  `lib/crowdb-chunk-kv/src/partition/split.rs`,
  `lib/crowdb-chunk-kv/src/partition/tree.rs`,
  `app/crowdb-chunk-kv-server/src/{main.rs,server.rs,serving/lease.rs}`,
  `lib/crowdb-chunk-kv-client/src/`.
- [x] **Make Preparing grant-safe**: treat an already hosted
  `SplitPreparing` parent as active for serving-grant refresh while retaining
  the existing mutation and read lifecycle checks. Add server and partition
  coverage for repeated grant installation through preparation and the lease
  deadline. Files: `lib/crowdb-chunk-kv/src/partition.rs`,
  `app/crowdb-chunk-kv-server/src/{main.rs,server.rs}`, and their tests.
- [x] **Define shared parent-tail artifact fields**: extend protocol and
  partition types so each child identifies its base checkpoint, parent stream
  identity, retained retry floor, source-cutover cursor, and child journal
  start at `C + 1`. The sealed shared parent suffix is the recovery source until
  no retained child snapshot references it; do not use a volatile memtable or
  duplicate parent frames into child streams during preparation.
  Files: `lib/crowdb-protocol/src/`, `lib/crowdb-chunk-kv/src/{types.rs,partition.rs}`,
  and protocol/partition tests.

## Preparation and Recovery

- [x] **Read filtered parent tails during preparation and recovery**: preserve
  parent sequencing while optionally warming child overlays from the sealed
  parent suffix. Validate source records, filter by child range, preserve
  request results, and apply only `(B, C]`; no pre-cutover catch-up or child
  journal duplication is required. Files:
  `lib/crowdb-chunk-kv/src/partition/{split.rs,tree.rs}`, journal adapters,
  and `lib/crowdb-chunk-kv/tests/partition_test.rs`.
- [x] **Recover child base plus parent and child tails without a final
  checkpoint**: open a prepared child from its base checkpoint, filter replay
  the pinned parent suffix through `C`, then replay its own child journal from
  `C + 1`; validate exact range, source identity, sequence continuity, and
  retry results before any catalog activation. Files:
  `lib/crowdb-chunk-kv/src/partition.rs`,
  `lib/crowdb-chunk-kv/src/partition/split.rs`, and partition integration
  tests.
- [x] **Expose truthful split metrics**: separately record preparation,
  base-checkpoint, tail bytes/records and lag, grant renewal failures, actual
  cutover drain/tail, background checkpoint, and client-forwarding outcomes.
  Files: `lib/crowdb-chunk-kv/src/metrics.rs`, server status/metrics wiring,
  and metric tests.

## Writer Handoff and Routing

- [x] **Install local child writers at cutover**: stop assigning new work to
  the parent sequencer at exact `C`, drain only requests that already hold the
  parent writer handle, and atomically reselect all other ingress to one local
  child WAL/memtable. Do not await tail warming, child checkpoint, or
  materialization in this path. Files: `lib/crowdb-chunk-kv/src/partition.rs`,
  split manager/server transition code, and partition tests.
- [x] **Publish and handle local stale routes**: bind local child writer
  readiness to the exact catalog artifact, accept parent and child minimum
  journal positions after publication, and directly dispatch stale point routes
  to hosted children without a second parent writer. Keep parent scan tokens
  refresh-only. Files:
  `app/crowdb-chunk-kv-server/src/`, `lib/crowdb-chunk-kv-client/src/`,
  protocol catalog types, and client/server E2E tests.
- [~] **Move physical persistence off cutover**: checkpoint child overlays,
  materialize inherited packs, retain/reclaim parent stream and tree references
  only after every retained child snapshot and retry floor releases its parent
  suffix pin.
  Files: chunk-KV partition maintenance, server transition recovery, and tree
  integration tests.

## Child-Tree Balance

- [x] **Define one reusable tail-handoff artifact**: extend the persisted
  transfer transition with a pinned source base manifest, source stream and
  retry floor, preparation cursor, exact handoff cursor, target stream start,
  target epoch, readiness limits, forwarding grace, and source-release proof.
  Reject balance while a child still references its split-parent suffix.
  Files: `lib/crowdb-protocol/src/chunk_kv.rs`, group-0 transition storage,
  protocol tests, and transition tests.
- [x] **Prepare the remote target while the source serves**: open the exact
  shared range-bounded manifest on the target, validate page and stream
  identities, replay the source tail into a durable target overlay, and enforce
  record, byte, estimated-time, and deadline readiness bounds before requesting
  a source fence. Drop unpublished target state on a preparation failure.
  Files: server transition runtime/storage, chunk-KV overlay recovery, and
  deterministic target preparation tests.
- [x] **Hand off one writer at cursor C**: close source assignment, drain only
  requests that already selected it, persist the release proof and final source
  cursor, then publish `TargetCatchingUp`. The source returns a target hint and
  never appends again; the target returns bounded `NotReady` until the sealed
  suffix reaches C, then installs its writer epoch and becomes `Serving`.
  Files: protocol RPC/catalog types, server authority and transition runtime,
  routed client retry handling, and E2E transition tests.
- [x] **Recover every balance phase from proofs**: resolve source/target crash,
  ambiguous catalog publication, and lease expiry from transition, catalog,
  manifest, tail, and grant state. Never infer authority from loaded pages,
  heartbeats, or volatile memtables. Files: server monitor/control store,
  transition state machine, startup recovery, and failure-injection tests.
- [~] **Materialize and reclaim balance state in background**: checkpoint the
  target overlay, materialize shared packs, retain source tree/stream/retry
  history through catalog and forwarding grace, then remove source objects and
  forwarding state only after every pin clears. Files: chunk-KV maintenance,
  server transition cleanup, metrics, and GC integration tests.

## Permanent Design

- [x] **Specify split overlay and local handoff**: write the durable fields,
  ordered preparation/cutover/recovery steps, minimum-position behavior,
  request-result ownership, stale-route behavior, retention pins, failure
  matrix, metrics, and named invariants into the chunk-KV, chunk-stream,
  server, and routed-client designs as current architecture.
- [x] **Specify child balance and remote handoff**: document placement
  eligibility, independent-child prerequisite, target readiness budgets,
  `TargetCatchingUp`, source-release and activation proofs, lease interaction,
  recovery decisions, background materialization, reclamation, observability,
  and named invariants in the permanent designs.

## Verification and Cleanup

- [~] **Add deterministic lifecycle tests**: cover grant renewal during
  Preparing, child-tail restart before checkpoint, writer-boundary exactly-once
  behavior, bounded post-cutover admission, stale point route, and catalog
  ambiguity. Files: crate `tests/*_test.rs` and server/client integration
  tests.
- [ ] **Add sustained split E2E**: keep routed 1 MiB-target hot traffic live
  through every observed split, capture p50/p99/p999/errors and correlated
  split metrics, then verify restart replay. Files:
  `tools/bench-chunk-kv-regression.sh`, client load tool, and
  `doc/working/chunk-kv-split-repro.md`. The 12,000 × 4 KiB, concurrency-32
  run now completes all three hot write rounds with zero errors and p99 below
  136 ms. Multi-generation convergence remains blocked when a later prepared
  overlay names tree manifest 3 but target/catalog recovery reopens manifest 2
  at the same applied sequence; exact historical tree-root open/pinning is
  still required before this item can close.
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
