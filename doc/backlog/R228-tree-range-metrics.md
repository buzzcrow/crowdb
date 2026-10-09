<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R228: crowdb-tree — Root and range statistics for split and placement

Status: Deferred implementation by user request. Establish the metrics contract
and review page-format, maintenance and performance costs before implementation.
Data-weighted placement and its console display remain disabled meanwhile.

## Problem

[Tree storage design](../design/tree/design-crowdb-tree-storage.md) defines leaf
records and inner separators/child PIDs. Leaves expose local base-record and delta
counts; inner pages do not carry child-subtree page, byte or live-KV aggregates.
The existing tree Stats API reports buffer-pool, flush and snapshot activity,
not current reachable tree size or current live record count. Snapshot pages
written, resident cache pages and cumulative writes are not these metrics.

[Chunk-KV placement](../design/chunkds/design-crowdb-chunk-kv-server.md) currently
balances assigned split counts. Automatic split supplements the partition-count
target with coarse retained-pack size thresholds/ranking. Those pack estimates
lag ordinary flushes, and split trees share historical chunks. They cannot
reliably determine the current range's data size or justify size-driven split.
Dividing an old estimate by a split ratio and accumulating later writes also
accumulates error without a structural calibration point.

A growing tree needs continuous, reasonably accurate size metrics to decide
whether another split is needed. Parent and child need independent range
statistics even while they share physical packs. Overwrites, deletes and
compaction must not make a historical estimate permanently drift.

## Solution

- **TREE-METRICS-ROOT:** Metrics describe a specific current tree root/version
  and its key range. Report observation/root identity and coverage frontier;
  do not silently mix roots, epochs or ranges. Memtable and WAL sizes do not
  contribute to tree size. Writes enter tree metrics when the normal flush
  incorporates them; callers must see this coverage boundary.
- **TREE-METRICS-LOGICAL:** Distinguish reachable inner, leaf, overflow and delta
  page counts; reachable page bytes; live KV count; live key/value bytes; and
  physical retained/shared pack storage. Logical range data and physical
  storage are separate metrics. A key's current visible value counts once;
  overwritten versions and tombstones do not count as live KV/data. Overflow
  data must contribute even when stored outside a leaf. State any approximation
  and its source explicitly rather than claiming exact live bytes.
- **TREE-METRICS-STRUCTURAL:** Carry child-subtree aggregates in index metadata
  so a full-tree/root aggregate or a range estimate can be obtained from index
  pages without scanning leaf records or values. Leaf summaries originate from
  existing rebuild/fold/flush work. Aggregates can be rebuilt from current
  structure, not cumulative writes or recursively inherited split ratios.
  A range boundary crossing a leaf may produce a documented bounded estimate;
  report that uncertainty rather than extrapolating arbitrary key samples.
- **TREE-METRICS-NONBLOCKING:** Maintain summaries through existing single-writer
  flush/page rebuild work. Do not add request-path locks, wait queues, per-key
  ancestor walks, forced flush/checkpoint, or synchronous remote scans to
  heartbeat or balance. Background index traversal is bounded and cancellable;
  heartbeat reads cached identity-fenced results and never waits for traversal.
  Review additional write amplification, atomic bookkeeping and memory costs.
- **TREE-METRICS-SHARING:** Split evaluates each resulting root/range's logical
  data; shared historical chunks are not counted as both ranges' logical data.
  Transfer preserves metrics for the same logical tree/range. Cleanup/GC of
  historical physical packs does not reduce current live logical data.
- **TREE-METRICS-RECOVERY:** Persist summaries consistently with their page/root
  publication. Reopen reconstructs/verifies metrics against the selected root.
  Abort or failed publication cannot install candidate statistics on the serving
  root. Legacy pages without summaries report unavailable until bounded rebuild;
  unknown is never zero and never authorizes a size-driven split or move.

1. Define metric semantics and coverage in the tree storage design, and review
   the persisted index/leaf summary format and legacy compatibility. Name the
   range-boundary error model and separate live logical data from page capacity.
2. Extend tree page builders/rebuild/flush and root publication to maintain and
   aggregate summaries. Include overwrite, delete, delta folding, overflow,
   range narrowing, split, transfer, compaction and reopen. Avoid new hot-path
   synchronization; measure cost before accepting the maintenance strategy.
3. Expose cached root/range statistics through the C API, Rust FFI and chunk-KV
   runtime/heartbeat. Index traversal must have page/I/O/time budgets and expose
   progress, freshness, unavailable state and identity fences.
4. Replace retained-pack split size triggering/ranking with these tree metrics;
   retain safe nonempty separator selection and count-target bootstrap. Use
   logical-byte distribution to improve boundary selection when summaries allow
   it, without promising perfectly equal ranges or scanning leaf values.
5. Re-enable data-weighted placement only after metrics acceptance, with explicit
   coefficients/tolerance and no count-only pass that reverses useful data moves.
   Update the console spec before restoring Weight, including metric coverage,
   uncertainty and the actual split/move decision explanation.

## Dependencies

- Existing tree/root publication and chunk-KV parent-plus-child recovery remain
  authoritative. This work must not redefine handoff, WAL or catalog cutover.
- Tree storage/engine and Chunk-KV server designs are upstream contracts.
- Shared-pack reclamation is separate from logical statistics; it is not required
  to obtain a range estimate and must not be used to force a physical copy.
- Until implemented, placement uses split count only and UI hides Weight. The
  count-target split path remains available; coarse pack sizing remains an
  explicitly limited interim mechanism, not a reliable current-data metric.

## Acceptance

- Empty tree -> read metrics -> known zero live KV/data and documented structural
  root-page cost, distinct from unavailable. TREE-METRICS-LOGICAL. Unit test.
- Known records with overflow values -> normal flush and aggregate via indexes
  -> expected live count/bytes and reachable page categories without value reads.
  TREE-METRICS-STRUCTURAL. Integration test.
- Same keys repeatedly overwritten/deleted -> normal fold/flush -> current live
  metrics reflect latest state, not accumulated writes; WAL-only changes leave
  tree metrics unchanged and coverage explicit. TREE-METRICS-ROOT. Integration test.
- Uneven range tree -> split while sharing old packs -> parent/child statistics
  describe their own ranges within declared boundary error; aggregate does not
  duplicate shared physical pack bytes as logical data. TREE-METRICS-SHARING.
  Integration test.
- Publish root, crash/reopen and replay/flush -> metrics follow selected root and
  coverage; fail/abort candidate publication -> serving statistics unchanged.
  TREE-METRICS-RECOVERY. Integration test.
- Legacy/cold tree -> bounded index traversal with cancellation -> no leaf/value
  scan or unbounded heartbeat wait; unavailable/freshness and progress explicit.
  TREE-METRICS-NONBLOCKING. Integration test.
- Repeated split, overwrite, delete, compaction and reopen -> rebuild aggregates
  from current structure -> no history-dependent drift; stale root/range results
  rejected. TREE-METRICS-STRUCTURAL. Integration test.
- Known growing tree -> normal flush crosses configured logical-byte threshold
  -> eligible split planned; below threshold or unknown -> no size-triggered split;
  count-target bootstrap remains independently tested. Integration test.
- Metrics accepted, explicit data-weight policy enabled -> weighted move/split
  decisions use matching cached statistics; console shows the backend decision,
  coverage and estimate qualification. E2E test.
- Fixed read/write/flush workload -> compare before/after -> report throughput,
  latency, page writes, memory, index I/O and traversal budgets; no new request
  locks, queues or forced checkpoint. Integration test.

## Open Questions

- Review summary encoding and update granularity: exact subtree live aggregates
  require maintenance during folding/rebuild; bounded estimates may reduce write
  amplification but must state error and recalibration guarantees.
- Review crossing-leaf ranges: report a boundary uncertainty interval using leaf
  summaries, or perform a separate bounded background boundary-leaf inspection.
  Neither option allows routine full leaf/value scans.

Verification commands:

- `pixi run test-cpp`
- `pixi run cargo test -p crowdb-tree-ffi`
- `pixi run cargo test -p crowdb-chunk-kv-server --test balance_test`
- `pixi run cargo test -p crowdb-kv-server --test chunk_kv_balance_planning_test`
- `pixi run cargo test -p crowdb-web --test native_cluster_provisioning_test`
- `pixi run tree-fmt`
- `pixi run tree-lint`
- `pixi run rs-fmt-check`
- `pixi run cargo clippy -p crowdb-tree-ffi -p crowdb-chunk-kv-server -p crowdb-kv-server -p crowdb-web --all-targets -- -D warnings`
