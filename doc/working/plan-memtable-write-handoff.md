<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# MemTable Write Handoff Plan

Upstream: [R201](../backlog/R201-tree-memtable-write-handoff.md).

Implement concurrent L0 mutation and finite, prefix-safe flush handoff without
temporarily losing acknowledged values. Preserve unrelated working-tree edits.
The user will return to the other task for subsequent accumulated S3 validation.

## Implementation

- [x] **Concurrent insertion**: replace the table spinlock with level-zero CAS
  membership and independently linked upper levels; atomically replace cells,
  fix concurrent height generation and slot extrema, establish focused baseline.
  Files: `lib/crowdb-tree/{include/crowdb-tree,src}/memtable/skip_list.*`,
  `lib/crowdb-tree/tests/unit/skip_list_test.cpp`.
- [x] **Version ownership**: immutable descriptors with monotonic pruning bound,
  highest prefix anchor and all future versions; coherent cursor candidates;
  lock-free deferred retirement, protected writer access and table teardown.
  Files: tree `memtable/*`, `epoch.*`, corresponding unit tests.
- [x] **Batch admission**: combined closed/count state and RAII ownership;
  all apply variants and empty batches finish slot bookkeeping before release;
  optional safe completion pruning cannot fail an already completed apply.
  Files: tree `memtable/*`, extracted `src/btree/ingest.cpp`, `btree/tree.h`.
- [x] **Finite flush and sources**: prepare successor before close, capture F
  before successor publication, wait only captured writers; select prefix
  versions without unlinking or relocating residual records; retain source
  owners and filter covered L0 versions using the captured catalog floor.
  Files: extracted tree flush/source modules, scan/get paths, double-buffer tests.
- [x] **Lifecycle and async**: reset/import/split generation ownership and
  exception-safe replacement; no blocking waits on async workers; borrowed
  results and overlay tables preserve their allocation owners.
  Files: tree lifecycle/split, FFI and affected KV maintenance callers.
- [x] **Observability**: batch-aggregated successful overwrite/retention/merge
  events and logical/retired memory, pending table diagnostics; no retry counts
  masquerading as successful transitions. Files: tree metrics and benchmarks.
- [x] **Implementation verification and handoff**: deterministic interleaving regressions,
  affected integration/FFI tests, sanitizer checks, before/after performance;
  enable slow-upload regression and update affected permanent architecture.
  Local gates and performance comparison pass; external acceptance is separate.
- [ ] **External acceptance (follow-up task)**: real tree-backed journal CAS
  with persist/reopen and accumulated S3/CLI/SDK recipes. Keep the requirement
  and this plan until those results support final cleanup.

## Files

- `lib/crowdb-tree/include/crowdb-tree/{memtable,btree,epoch.h}`
- `lib/crowdb-tree/src/{memtable,btree,epoch.cpp}`
- `lib/crowdb-tree/tests/{unit,integration}`, `lib/crowdb-tree/bench`
- Affected `lib/crowdb-tree-ffi`, KV/ChunkDB tests and maintenance call sites.
- S3 slow-upload harness and permanent tree/KV design sections.

## Verification

- Unit: skip-list CAS collisions, selective prefix retention, epoch ownership,
  closed/count transitions, failed allocation/duplicate publication accounting.
- Tree integration: finite frontier, paused old writer, prefix overwritten by
  future slot in either arrival order, retained source visibility at 843/853
  with F=852, observed-version forward/reverse scans, lifecycle/split.
- Commands: `pixi run test-tree-ct`, `pixi run test-cpp`,
  `pixi run cargo test -p crowdb-tree-ffi --tests`; relevant KV and ChunkDB
  tests preceded by `pixi run clean-env`; `pixi run tree-lint`, C++ format,
  `pixi run rs-fmt-check` and affected Rust clippy if Rust changes.
- E2E handoff: the R201 focused cases, three accumulated default S3 runs,
  default-concurrency CLI and sequential pinned SDK recipes. External results
  are required before final requirement cleanup.
- Performance: identical build/configuration before and after; single/multiple
  writers, small batches, hot keys, mostly ordered overwrite, gaps and scan/flush
  mixtures. Report results without a percentage acceptance threshold.

## Results

- Baseline: `598a6548`. Unrelated documentation changes are preserved and are
  excluded from implementation commits; the R201 requirement is not edited.
- Final C++ tree/common suite: 607/607 pass. `pixi run test-cpp` also passes,
  including RPC, diskio and both FFI suites. The final two latency-counter
  regressions and the portable-export failure guard are included in the final
  607-test run. The final Rust tree FFI run passes 49/49 tests.
- Final ASan and TSan: 77/77 focused tests pass in each build, including
  handoff, retention, failed apply/publication, import/range/split, external
  ownership, asynchronous flush and metrics. TSan uses `setarch x86_64 -R`
  and `-Wno-error=tsan` for the existing Folly fence warning. After the final
  portable-export exception fix, another 22/22 failure/export/ownership tests
  pass under each sanitizer; no sanitizer diagnostics were reported.
- Changed C++ files pass `pixi run tree-lint` and clang-format. Rust workspace
  formatting and affected FFI/KV/ChunkKV clippy pass. KV/ChunkKV tests pass
  (736 tests).
- `mt.version.copy.l` uses the existing latency-summary family. Count is
  descriptor reconstruction attempts, including CAS retries and aborted
  preparation. It preserves per-attempt sum/maximum while publishing locally
  aggregated samples once per batch. No payload-copy bandwidth metric is added.
- Fresh KV-server, diskdb and chunkdb executables have been built. ChunkDB
  passes 144/144 tests (including 38 full-stack tests); chunk-stream passes
  43/43, including three real-process failure/restart tests. Both suites run
  after `pixi run clean-env`. The final portable-export guard converts failures
  to status values before they can cross the C ABI.
- Deterministic C++ regression proves x@853 remains visible through F=852
  publication while L1 still contains x@843. The real tree-backed ChunkDB
  journal-CAS/persist/reopen acceptance case and accumulated S3/CLI/SDK recipes
  remain pending the user's follow-up validation; do not claim those resolved
  or delete the requirement/backlog entry.

## Performance comparison

- Same benchmark source is built against baseline `598a6548` and the new engine.
  Release GCC 15.3.0, `-O3 -DNDEBUG`, test hooks disabled; Intel i9-7960X,
  16 physical cores / 32 threads, Linux x86-64, affinity CPUs 0–7.
- Each case runs in its own process, baseline then new engine, three repetitions
  with `--benchmark_min_time=0.3s`. Tables report medians of the repetitions.
  No builds or other test suites run concurrently. Desktop background load and
  frequency scaling are not disabled, so this is a local comparison, not a
  production capacity claim. Raw JSON/logs are under `/tmp/r201-perf-final/`.
- Values are 64 bytes. Distinct mode overwrites 128 keys per writer; hot mode
  writes one shared key. Normally 2,048 batches per writer; the gap case uses
  128 batches per writer and holds slot 1 absent while slots 2–513 execute.
  Mixed mode continuously scans up to 64 rows and flushes on another thread.
- Input preparation and tree destruction are outside timing. Thread startup,
  apply and concurrent maintenance are included. p50/p99 are batch latencies;
  throughput counts records. Metrics registration is disabled in both engines.
  Successful logical event totals and CAS retries come from engine statistics.

| Workload                           | Old kops/s | New kops/s | New/old | p50 us old / new | p99 us old / new |
| ---------------------------------- | ---------- | ---------- | ------- | ---------------- | ---------------- |
| 1 writers / batch 1 / distinct     | 668.1      | 357.3      | 0.53    | 1.13 / 2.32      | 1.76 / 3.89      |
| 4 writers / batch 1 / distinct     | 538.3      | 466.6      | 0.87    | 4.29 / 6.82      | 38.47 / 21.85    |
| 8 writers / batch 1 / distinct     | 997.3      | 946.3      | 0.95    | 4.28 / 6.71      | 34.50 / 23.15    |
| 1 writers / batch 16 / distinct    | 1393.5     | 775.4      | 0.56    | 10.01 / 17.59    | 17.68 / 33.83    |
| 4 writers / batch 16 / distinct    | 1993.6     | 1399.9     | 0.70    | 26.43 / 38.06    | 68.89 / 77.61    |
| 1 writers / batch 1 / hot          | 770.1      | 429.8      | 0.56    | 0.95 / 1.73      | 1.31 / 3.27      |
| 4 writers / batch 1 / hot          | 493.6      | 419.9      | 0.85    | 5.14 / 6.53      | 38.97 / 42.64    |
| 4 writers / batch 1 / hot + gap    | 246.9      | 28.2       | 0.11    | 8.08 / 66.89     | 56.78 / 883.62   |
| 4 writers / batch 1 / scan + flush | 512.5      | 466.6      | 0.91    | 4.88 / 6.47      | 35.20 / 20.35    |

| Workload                           | CPU/wall old / new | Peak RSS MiB old / new | New charged MiB | CAS retries | Overwrite / keep / merge |
| ---------------------------------- | ------------------ | ---------------------- | --------------- | ----------- | ------------------------ |
| 1 writers / batch 1 / distinct     | 1.00 / 1.00        | 28.3 / 29.3            | 0.79            | 0           | 1920 / 1920 / 1920       |
| 4 writers / batch 1 / distinct     | 3.38 / 3.67        | 29.6 / 32.8            | 3.05            | 0           | 7680 / 7680 / 7590       |
| 8 writers / batch 1 / distinct     | 5.67 / 7.10        | 31.6 / 39.0            | 5.95            | 0           | 15360 / 15360 / 14946    |
| 1 writers / batch 16 / distinct    | 1.00 / 1.00        | 32.8 / 48.3            | 12.54           | 0           | 32640 / 32640 / 32640    |
| 4 writers / batch 16 / distinct    | 3.58 / 3.84        | 47.6 / 108.0           | 49.72           | 0           | 130560 / 130560 / 130472 |
| 1 writers / batch 1 / hot          | 1.00 / 1.00        | 28.3 / 29.3            | 0.78            | 0           | 2047 / 2047 / 2047       |
| 4 writers / batch 1 / hot          | 3.45 / 3.67        | 29.8 / 33.8            | 3.23            | 16204       | 6129 / 8191 / 8191       |
| 4 writers / batch 1 / hot + gap    | 3.04 / 3.44        | 28.1 / 31.8            | 2.80            | 879         | 394 / 511 / 0            |
| 4 writers / batch 1 / scan + flush | 4.23 / 4.39        | 37.7 / 37.2            | 0.01            | 0           | 1204 / 1204 / 969        |

- CPU/wall approximates cores consumed during the measured interval. Peak RSS
  is each case process's high-water mark, including allocation warmup and input
  data. Charged bytes and logical/CAS totals are averaged per benchmark iteration
  and sampled after workers finish; they are not rates. The old engine has no
  equivalent version-retention/CAS counters.
- Sequential overwrite pays for admission, immutable descriptor construction,
  safe-prefix retention and deferred ownership. Four/eight distinct writers
  improve p99 here but do not exceed old throughput. Independent progress is
  demonstrated by deterministic paused-writer tests, not inferred from speedup.
- The sustained hot-key gap is the largest cost: growing retained reference
  sets and CAS contention reduce throughput to about 11% of the baseline.
  Baseline drops prefix versions required for correctness, so its faster result
  is not an equivalent correctness guarantee. This is the selected design's
  measured cost; no performance percentage gate was selected.
- Ordinary ordered overwrite records matching keep/merge counts, demonstrating
  logical collapse. Without maintenance, deferred descriptors remain charged
  until collection; most benchmark modes intentionally suppress rotation.
  The ownership/metrics regression separately proves physical reclamation after
  the borrower releases. Mixed scan/flush has ongoing collection.
- This end-to-end comparison includes shared admission counters and retirement;
  it does not isolate their individual CPU cost. The copy latency counter gives
  runtime descriptor preparation cost when metrics are enabled. Longer-term
  metrics overhead and production workload validation remain follow-up evidence.

Reproduce a case against either executable with:

```sh
pixi run taskset -c 0-7 lib/crowdb-tree/build-perf/crowdb_tree_bench \
  --benchmark_filter='^bm_memtable_handoff/4/1/2/' \
  --benchmark_min_time=0.3s --benchmark_repetitions=3 \
  --benchmark_out_format=json --benchmark_out=/tmp/memtable-handoff.json
```
