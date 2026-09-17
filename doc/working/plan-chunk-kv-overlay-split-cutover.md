<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk-KV Overlay Split Cutover Plan

Upstream: [R174](../backlog/R174-chunk-kv-overlay-split-cutover.md),
[chunk-KV design](../design/chunkds/design-crowdb-chunk-kv.md), and
[chunk-KV server design](../design/chunkds/design-crowdb-chunk-kv-server.md).

Goal: keep split preparation serving, make each child own a recoverable tail,
and reduce cutover to a bounded old-writer to child-writer handoff without a
foreground child checkpoint.

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
- [~] **Define child-owned tail artifact fields**: extend protocol and
  partition types so each child identifies its base checkpoint, durable child
  journal tail, source-cutover cursor, retained request-result floor, and
  recovery representation. Child streams are the chosen durable tail owner;
  do not use a volatile parent memtable or an indefinitely shared parent suffix.
  Files: `lib/crowdb-protocol/src/`, `lib/crowdb-chunk-kv/src/{types.rs,partition.rs}`,
  and protocol/partition tests.

## Preparation and Recovery

- [ ] **Replicate filtered durable tails during preparation**: preserve parent
  sequencing while appending each record's child representation and retained
  result to the selected child journal, then apply it to the child overlay.
  Ensure conditional failures, no-ops, request digest conflicts, source order,
  and byte/record lag are recoverable. Files:
  `lib/crowdb-chunk-kv/src/partition/{split.rs,tree.rs}`, journal adapters,
  and `lib/crowdb-chunk-kv/tests/partition_test.rs`.
- [ ] **Recover child base plus tail without a final checkpoint**: open a
  prepared child from its base checkpoint and replay its own durable tail;
  validate exact child range, source cutover, sequence continuity, and retry
  results before any catalog activation. Files:
  `lib/crowdb-chunk-kv/src/partition.rs`,
  `lib/crowdb-chunk-kv/src/partition/split.rs`, and partition integration
  tests.
- [ ] **Expose truthful split metrics**: separately record preparation,
  base-checkpoint, tail bytes/records and lag, grant renewal failures, actual
  cutover drain/tail, background checkpoint, and client-forwarding outcomes.
  Files: `lib/crowdb-chunk-kv/src/metrics.rs`, server status/metrics wiring,
  and metric tests.

## Writer Handoff and Routing

- [ ] **Install child writers at cutover**: stop assigning new work to the
  parent sequencer at exact `C`, drain only assigned work, verify durable child
  tails through `C`, and route later mutations to bounded child queues and
  child WAL/memtables. Do not await child checkpoint or materialization in this
  path. Files: `lib/crowdb-chunk-kv/src/partition.rs`, split manager/server
  transition code, and partition tests.
- [ ] **Publish and handle stale routes**: bind child writer readiness to the
  exact catalog artifact, use child minimum journal positions after publication,
  and forward or owner-hint stale point routes without a second parent writer.
  Keep parent scan tokens refresh-only. Files:
  `app/crowdb-chunk-kv-server/src/`, `lib/crowdb-chunk-kv-client/src/`,
  protocol catalog types, and client/server E2E tests.
- [ ] **Move physical persistence off cutover**: checkpoint child overlays,
  materialize inherited packs, retain/reclaim parent stream and tree references,
  and expire forwarding only after catalog, retry, and recovery pins clear.
  Files: chunk-KV partition maintenance, server transition recovery, and tree
  integration tests.

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
  `doc/working/chunk-kv-split-repro.md`.
- [ ] **Run acceptance gates and merge design**: run R174 gates, update
  permanent chunk-KV/server design with landed behavior, remove R174 and this
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

## Tests

- Unit: partition lifecycle, artifact, protocol, journal-tail, retry-result,
  and server authority tests.
- Integration: native child recovery, writer handoff, stale point routing,
  catalog publication, and tree materialization tests.
- E2E: three-node sustained split with routed client traffic and restart replay.
