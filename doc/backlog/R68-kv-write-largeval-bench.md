<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R68: KV benchmark — Large-Value Write Snapshot Sentinel

**Problem**: R67 moved crowdb-tree flush, snapshot persistence, and sparse
block compaction off Tokio worker threads in `group_maintenance::run_pass`.
The regression evidence does not currently exercise the claimed large-value
state. `tools/bench-kv-scan-regression.sh` preloads 64-byte values; its
`largeval_16k` case changes only the scan command's expected value size and
does not rewrite the stored data. The write sentinel likewise fixes
`VALUE_SIZE=512` and has no case that builds a large snapshot while writes and
election heartbeats are active.

The old requirement also asked for fields the benchmark does not emit:
`put_errors`, `batch_write_errors`, `retries_exhausted`, and p999 latency are
absent from `BenchResult`. More importantly, a zero-error run does not prove
the maintenance path was exercised. With the E2E election profile, the
time-triggered snapshot begins around nine seconds, so a ten-second run can
finish before a large snapshot completes.

The result is a false-negative sentinel: it may be green without serializing
large values at all. The relevant architecture and baseline are
`doc/design/kv/design-crowdb-kv-wal.md`,
`doc/design/tree/design-crowdb-tree-engine-snapshot-flow.md`, and
`doc/design/kv/kv-write-flow-analysis.md`. The concrete failure scenario is a
large snapshot monopolizing a C++ critical section or Tokio worker long enough
for the 300–600 ms election timeout to fire, producing write errors or an
election-count increase.

**Solution**: Add a self-validating large-value write case that creates real
16 KiB values, proves snapshot completion during the measurement window, and
detects election churn.

1. Generalize `tools/bench-kv-write-regression.sh` so a case supplies its own
   duration, keyspace, value size, and repetition count without changing the
   existing 512-byte scaling cases. Add `largeval_16k` with 16 KiB values,
   100,000-key space, 1 loader, 1 connection, the existing three-node
   mem-block cluster shape, and a 15-second measured duration.
2. Run the large-value case three times from clean group state in one selected
   script invocation. Keep the same deployed cluster to exercise repeated
   clean/restart-of-user-data behavior, but reset group data and metric
   baselines before each repetition. Each run records total operations,
   operations/s, avg/p50/p99 write latency, WAL append count, total errors,
   correctness errors, completed snapshot count and maximum snapshot latency,
   and election-count delta.
3. Add explicit per-group maintenance snapshot metrics if current counters
   cannot provide those fields: a completion count, success/failure result,
   and latency summary updated around `persist_snapshot_blocking`. Extend the
   write benchmark's server-metric collection only with the fields required by
   this sentinel. Calculate per-run deltas so startup election and earlier
   repetitions do not contaminate the assertion.
4. Make the script fail when a repetition has any workload/correctness error,
   no successful snapshot completion, a snapshot failure, or a positive
   election-count delta after the pre-run baseline. Preserve complete CLI and
   server logs as evidence. Do not weaken the workload, timeouts, or assertions
   when a run fails; investigate the first divergence and file a separate fix
   requirement if production code outside benchmark observability must change.
5. Record the three reference runs, hardware/kernel identity, workload shape,
   snapshot evidence, and result interpretation in
   `doc/design/kv/kv-write-flow-analysis.md`. Keep raw TSV output in the
   benchmark log directory rather than committing transient run files.

Changing election timeouts, changing snapshot thresholds globally, fixing a
newly discovered maintenance/write bug, adding p999 to the shared benchmark
schema, and modifying the existing scan sentinel are not part of this
requirement.

**Dependencies**:

- R67's landed `spawn_blocking` maintenance changes are the behavior under
  verification.
- The E2E election profile must retain a deterministic snapshot trigger inside
  the 15-second case. If profile defaults change before implementation, the
  benchmark must set case-local maintenance thresholds rather than lengthen or
  weaken the election timeout.
- R66 is independent: this case continues to use mem-block WAL and KV backends
  to isolate maintenance scheduling from physical disk latency.

**Acceptance**:

- Setup the large-value case and inspect the emitted command; run one
  repetition; assert every write uses a 16,384-byte value, the keyspace is
  100,000, the duration is 15 seconds, and existing cases retain their prior
  defaults. Invariant: case-local parameters cannot leak between sentinel
  configurations. Integration test.
- Setup one clean three-node group with snapshot metrics baselined; run the
  large-value workload; assert at least one successful snapshot completed in
  the measured interval and its latency was recorded. Invariant: a green case
  actually exercises large-value snapshot persistence. E2E test.
- Setup the scripted three-repetition selection; run it; assert each row has
  zero total and correctness errors, zero election delta, at least one
  successful snapshot, and no snapshot failure, or that the script exits
  nonzero while retaining evidence. Invariant: election churn and missing
  coverage cannot pass silently. E2E test.
- Setup synthetic benchmark JSON/metric snapshots with an error, election
  increase, missing snapshot, and snapshot failure in turn; parse each; assert
  every invalid result fails and a valid result passes. Invariant: the sentinel
  enforces its contract independently of live-cluster variance. Unit test.
- Setup a completed reference run on Linux; update the flow analysis; assert it
  records hardware/kernel, all three measurements, snapshot evidence, and the
  exact workload. Invariant: future comparisons have reproducible context.
  E2E test.

Verification commands:

- `pixi run test-kv-core`
- `pixi run -- cargo test -p crowdb-cli --all-targets`
- `KV_WRITE_BENCH_CASES=largeval_16k pixi run -- bash tools/bench-kv-write-regression.sh`
- `pixi run -- cargo fmt --all --check`
- `pixi run rs-lint`
