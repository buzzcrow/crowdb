<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Large-Value Write Snapshot Sentinel Plan

Upstream requirement: `doc/backlog/R68-kv-write-largeval-bench.md`

Supporting designs: `doc/design/kv/kv-write-flow-analysis.md` and
`doc/design/tree/design-crowdb-tree-engine-snapshot-flow.md`

Goal: make the write regression prove that a real 16 KiB dataset snapshot
completed without write errors or election churn.

## Phase 1 — Observable Contract

- [ ] **Inventory existing group metrics**: identify the registered election
  counter names and current snapshot page/latency signals; record which values
  are cumulative so the script can calculate per-run deltas. Files:
  `lib/crowdb-kv/src/cluster/local_replica.rs`,
  `app/crowdb-kv-server/src/engine_collector.rs`, CLI metric collection.
- [ ] **Add snapshot completion metrics**: register a per-group completion
  counter, failure counter, and latency summary; update them exactly once
  around `persist_snapshot_blocking`, including panic/zero-result failure.
  Files: `lib/crowdb-kv/src/cluster/{group,group_maintenance}.rs`, metrics
  registration/view modules.
- [ ] **Expose required benchmark fields**: extend write server metrics with
  election, snapshot completion/failure, and maximum snapshot latency values
  without adding the unsupported error breakdown or p999. Files:
  `app/crowdb-cli/src/commands/bench/{result,kv/write}.rs` and tests.
- [ ] **Make deltas explicit**: add script helpers that capture metrics before
  and after each repetition and calculate only the selected group/run delta;
  exclude initial cluster election and earlier snapshots. Files:
  `tools/bench-kv-write-regression.sh`, `tools/bench-regression-common.sh` only
  if the helper is reusable.

## Phase 2 — Case-Local Benchmark Parameters

- [ ] **Parameterize one run**: extend `run_bench` with explicit duration,
  keyspace, and value size while retaining environment defaults for existing
  calls; include those fields and correctness errors in TSV output. Files:
  `tools/bench-kv-write-regression.sh`.
- [ ] **Add the large-value group**: deploy with the existing mem-block/e2e
  topology and run `largeval_16k_run1..3`, cleaning group 0/1 and baselining
  metrics before each 15-second, 100,000-key, 16 KiB, 1T:1C workload. Files:
  `tools/bench-kv-write-regression.sh`.
- [ ] **Enforce sentinel assertions**: return nonzero for workload or
  correctness errors, positive election delta, missing successful snapshot,
  or snapshot failure; print the evidence path and preserve server/CLI logs.
  Files: `tools/bench-kv-write-regression.sh`.
- [ ] **Add parser fixtures**: factor result validation so shell fixture tests
  cover valid output and each failure condition without a live cluster. Files:
  `tools/bench-kv-write-regression.sh`, focused script fixture/test files under
  `tools/` if needed.

## Phase 3 — Live Evidence

- [ ] **Run the selected sentinel**: execute only `largeval_16k`, confirm all
  three snapshots completed inside their measurement intervals, and retain
  complete result/log artifacts. Files: benchmark log directory only.
- [ ] **Diagnose any first divergence**: if an assertion fails, preserve the
  failing requirement as open, trace server logs and snapshot/election metric
  boundaries, and create a separate requirement before changing production
  scheduling or election policy. Files: new backlog item only if required.
- [ ] **Record reference results**: document hardware, kernel, exact command,
  dataset, three result rows, snapshot latency/count, election delta, and
  interpretation. Files: `doc/design/kv/kv-write-flow-analysis.md`, script
  reference comment.
- [ ] **Run focused gates**: run R68's tests, selected benchmark command, fmt,
  and clippy with complete output. Files: affected workspace.

## Consolidated File List

- `lib/crowdb-kv/src/cluster/{group,group_maintenance,local_replica}.rs`
- `lib/crowdb-kv` metrics registration/view files and focused tests
- `app/crowdb-kv-server/src/engine_collector.rs` if snapshot metrics bridge
  through engine collection
- `app/crowdb-cli/src/commands/bench/{result,kv/write}.rs` and tests
- `tools/bench-kv-write-regression.sh`
- `tools/bench-regression-common.sh` only for a shared metric helper
- `doc/design/kv/kv-write-flow-analysis.md`

## Tests

Unit tests:

- Snapshot success/failure metric accounting.
- Benchmark metric aggregation and per-run delta calculation.
- Fixture validation for every sentinel pass/fail condition.

Integration tests:

- Existing 512-byte cases retain default command parameters and output shape.
- One case-local 16 KiB invocation emits the intended command and fields.

E2E tests:

- Three clean 15-second large-value write runs, each with zero errors/election
  delta and at least one measured successful snapshot.
