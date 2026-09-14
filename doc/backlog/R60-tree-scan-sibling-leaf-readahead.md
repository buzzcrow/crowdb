<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R60: Scan — Sibling-Leaf Readahead on Cold Scans

**Status**: Deferred pending a cold file/block-backed scan benchmark that
forces leaf eviction and records page reads, scan retries, latency, and
throughput. Implement readahead only if that baseline shows serialized leaf
loads are a material part of scan latency or limit NVMe throughput.

**Problem**: the scan path demand-loads each L1 leaf inline. The sync
path (`Crowdbtree::scan`) resolves a leaf via the page cache when the
merge loop reaches it — the read stalls the loop until the leaf is
resident. The async path (`scan_async_attempt`) resolves one pending
page per reactor round trip and retries the whole scan, so a multi-leaf
cold range pays one reactor round trip per cold leaf, serialized with
the merge work on prior leaves.

A scan knows its next leaf before finishing the current one: the merge
loop reads `base->right_sibling()` in `crowdb-tree.cpp` right after
descending into a leaf, before iterating that leaf's entries. So
the page id of the next leaf is available while the current leaf is
still being merged. Today nothing is done with it until the current
leaf exhausts and `refill_l1` walks to the next — at which point the
read stalls.

**Solution**: issue a readahead (prefetch) for the right-sibling leaf
as soon as its page id is known, overlapping the next leaf's I/O with
the current leaf's merge work.

- **Sync path** (`Crowdbtree::scan` / `try_scan_no_load`): after reading
  `right_sibling`, call the page cache's prefetch/async-resolve seam
  for that page id (non-blocking — enqueues the read, does not wait).
  When `refill_l1` later walks to that leaf, the read is already in
  flight or complete, so the stall is hidden. If the leaf is already
  resident the prefetch is a no-op.
- **Async path** (`scan_async_attempt`): instead of resolving one pending page
  per reactor round trip and waiting until the current leaf is exhausted to
  discover the next miss, submit the right-sibling read as soon as the current
  resident leaf exposes its page id. This fixed one-leaf lookahead overlaps
  the next NVMe read with current-leaf merge and packing work while bounding
  memory and unused reads. It does not create multiple in-flight reads within
  one scan because the following sibling id is not known yet.
- **Prefetch depth**: start with a fixed depth of one. Tunable 2–4-leaf windows
  are out of scope until the cold benchmark shows one-leaf lookahead is useful
  but insufficient.
- **Eligibility invariant**: only a range scan may request sibling readahead.
  Issue it only when the scan can continue beyond the current leaf, has not
  exhausted its item limit or byte budget, has time remaining before its
  deadline, and uses a file/block-backed async page store. A resident sibling
  needs only the existing CPU prefetch; an unloaded or already-loading sibling
  must not receive a duplicate read. Point `get` operations and memory-mode
  scans never request sibling I/O.
- **I/O mechanism**: use a request-scoped engine-managed asynchronous read,
  not a process-wide operating-system sequential-access hint. The block path
  uses `O_DIRECT`, and a global hint could be ineffective or pollute unrelated
  buffered-file workloads. At most one speculative sibling page may be loaded
  and then left unused when the current leaf satisfies the scan.

**Scope**:
- `lib/crowdb-tree/src/btree/crowdb-tree.cpp` — `scan` / `try_scan_no_load`:
  after `page_id = base->right_sibling()`, issue a prefetch. The
  prefetch seam must already exist or be added to the page cache
  using the configured `AsyncPageStore` and reactor. If no request-scoped
  async-resolve seam exists, this is blocked on adding one.
- `lib/crowdb-tree/src/btree/crowdb-tree.cpp` — `scan_async_attempt`: submit
  the right-sibling read while merging the current resident leaf and reuse or
  await that read if the scan reaches the sibling before completion.
- Benchmark prerequisite: add a file/block-backed multi-leaf scan case that
  forces eviction and records page reads, scan retries, latency, and
  throughput before changing the read path.
- Tests: `test-tree-ct` scan tests must pass unchanged (readahead is
  observational — output is identical). Extend or mirror
  `AsyncScan.MissAfterEvictionCompletesViaReactor` for fixed one-leaf
  readahead only after the benchmark meets the implementation threshold.

**Complexity**: Medium. The prerequisite benchmark is required because the
maintained scan regression runs in memory mode and cannot establish a cold
NVMe benefit. The sync-path prefetch is small IF a
prefetch/async-resolve seam exists in the page cache (likely needs a
small addition — the cache today resolves on demand). The async-path
coordination is the bulk: the retry loop must distinguish a sibling read that
is already in flight from an unloaded page, avoid duplicate submissions, and
resume from the correct key when the read completes.
Measurement is required to confirm the win (cold ranges only — the
bench today runs mem-mode where leaves are resident, so a cold/disk
bench config is a prerequisite for validation).

**Dependencies**:
- Blocked on the cold file/block-backed benchmark and baseline measurements.
- May need a page-cache prefetch seam (check first). If the cache has
  no non-blocking resolve, add one as a sub-task.
- Independent of R57 (staging copies) and R58 (merge loop compares) —
  they touch different parts of the scan path. R60 overlaps I/O with
  merge work; R57/R58 speed up the merge work itself. Complementary.
- If the existing cold-scan regression measurements expose a regression
  without identifying its source, profile the affected case on demand before
  investing in readahead.

**Acceptance**:
- Scan output is byte-identical to today across all `test-tree-ct` scan
  tests — Integration test.
- With a cold file/block-backed tree, perform a point `get`, a memory-mode
  scan, and a scan whose range ends in the current leaf; assert that none
  submits a sibling read, preserving the scan-only eligibility invariant —
  Integration test.
- With an unloaded sibling already being loaded, scan through the current
  leaf; assert that the scan reuses or awaits the existing operation and does
  not submit a duplicate read, preserving the one-read-per-page invariant —
  Integration test.
- With an expired scan deadline or an exhausted item/byte budget, finish the
  current leaf; assert that no sibling read is submitted, preserving the
  request-bound work invariant — Integration test.
- A cold file/block-backed benchmark forces eviction and reports page reads,
  scan retries, latency, and throughput for the no-readahead baseline —
  Integration test.
- Before the production read path changes, a fixed one-leaf experiment shows
  a material reduction in latency or increase in throughput against that
  baseline; otherwise R60 is closed without implementation — Integration
  test.
- Readahead memory is bounded (per-scan in-flight cap, default window
  = 1); a full-keyspace cold scan does not grow unbounded RSS — Integration
  test.
- No regression on `tools/bench-kv-scan-regression.sh` (mem-mode configs
  unchanged — readahead is a no-op when leaves are resident) — Integration
  test.

**Note**: the gap lives in
`doc/design/kv/kv-scan-flow-analysis.md` Gap Analysis → Performance →
"No sibling-leaf readahead on cold scans".

Verification commands:
- `pixi run test-tree-ct`
- `pixi run tree-fmt`
- `pixi run tree-lint`
- `pixi run rs-fmt -- --check`
- `pixi run rs-lint`
