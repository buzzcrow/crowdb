<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# MemTable handoff benchmark

Local comparison recorded on 2026-10-03 for implementation `139149b7`.
The benchmark is `memtable_handoff_bench.cpp`; the current handoff contract is
in [the engine design](../../../doc/design/tree/design-crowdb-tree-engine.md).
Correctness and independent writer progress are acceptance requirements;
performance has no percentage gate.

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
