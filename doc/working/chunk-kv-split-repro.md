<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk-KV split and RSS reproduction

This is a temporary handoff for the split failure observed while reviewing
[R173](../backlog/R173-s3-console-cluster-cli.md). Run from the repository root.
The workload uses three local KV, DiskDB, DiskIO, ChunkDB, and chunk-KV
instances. KV and WAL use `mem-block`; DiskIO uses `mem` dummy disks. The
script configures one rack with unsafe EC, puts into 30,000 distinct keys, and
triggers automatic chunk-KV partition split at the configured 5 MiB target.
The load arrives in three consecutive ranges with the shared `object/hot`
prefix, so each range continues growing the hot lexical range after its prior
split. This is a chunk-KV storage-path test, not an S3 benchmark.

## Before running

- Preserve the range-rebuild fix in
  `lib/crowdb-tree/src/btree/range_rebuild.cpp`: it splits an oversized source
  leaf into fixed-size destination frames. Compare results against this same
  code before making another change.
- Check that no other local benchmark stack is running on the fixed service
  ports. The script's startup cleanup can stop auxiliary DiskDB, DiskIO, and
  ChunkDB processes from earlier `bench-log/chunk-kv-regression-*` runs.
  Check with:

  ```bash
  pixi run -- pgrep -af 'crowdb-(kv-server|diskdb|diskio|chunkdb|chunk-kv-server)'
  ```
- Allow several minutes. Start the command in a background/PTY session and
  poll; do not impose a 60-second timeout on the complete script. The script
  has its own 240-second load and convergence timeouts and cleans up its own
  deployment on exit.

## Reproduce

```bash
cd /nv/cpp/crowdb
CHUNK_KV_BENCH_OPERATIONS=30000 \
CHUNK_KV_BENCH_CONCURRENCY=32 \
CHUNK_KV_BENCH_VALUE_BYTES=4096 \
pixi run -- bash tools/bench-chunk-kv-regression.sh
```

`CHUNK_KV_BENCH_HOT_SPLIT_ROUNDS` controls the number of consecutive key
ranges (default `3`); `CHUNK_KV_BENCH_OPERATIONS` must divide evenly by it.
`CHUNK_KV_BENCH_HOT_KEY_PREFIX` changes the shared key prefix.
`CHUNK_KV_BENCH_TARGET_PARTITION_BYTES` changes the split target (default
`5242880`). Use a lower target with a proportionally smaller workload for a
quick local stability run.

The script builds release binaries unless `CHUNK_KV_BENCH_SKIP_BUILD=1` is set.
Use the default on a fresh checkout. The run directory is printed as
`bench-log/chunk-kv-regression-<timestamp>/`. A successful run must exit 0,
report `errors=0`, converge to at least 12 balanced partitions, restart one
chunk-KV server, and read a previously written key. The script writes
`results.tsv` after those checks, then checks latency, RSS delta, split, and
replay bounds.

To check the focused C++ fix before the full workload:

```bash
pixi run -- cmake --build lib/crowdb-tree/build -j --target crowdb_tree_tests
pixi run -- ./lib/crowdb-tree/build/crowdb_tree_tests --gtest_filter='RangeRebuild.*'
```

## Monitor memory

While the workload is active, identify the three `crowdb-kv-server`, three
`crowdb-chunk-kv-server`, and three `crowdb-diskio` PIDs with the `pgrep`
command above. For each PID, run:

```bash
pixi run -- awk '/^Name:|^VmRSS:|^VmHWM:/' /proc/12345/status
pixi run -- free -h
```

Replace `12345` with a numeric process ID; sample each process separately.
Sample during the put load and again while split is preparing; a process's
`VmHWM` disappears when the script cleans up. Record each process separately
before summing RSS. A sum of independent `VmHWM` values is an upper bound, not
a simultaneous cluster peak. Also record `free -h` to distinguish high RSS
from host memory exhaustion.

The 2026-09-17 pre-fix load (30,000 × 4 KiB, concurrency 32) completed its
puts with `errors=0` and about 409 ops/s. A post-load sample showed about
16.3 GiB combined RSS for the three KV and three chunk-KV processes; their
individual `VmHWM` values summed to about 18.5 GiB. The host did not exhaust
memory. These values do not establish a safe default budget for R173.

## Failure timeline and logs

- Pre-fix run: `bench-log/chunk-kv-regression-20260917-113336/`. Chunk-KV 1
  entered `SplitPreparing` at 03:34:41 UTC. The first tree error at 03:34:45
  was `range rebuild: filtered leaf does not fit destination frame`. Later
  logs showed a full chunk RPC completion slab and repeated corruption
  reports. The script ended with `automatic split and owner balance did not
  converge`.
- Before the RPC completion fallback fix:
  `bench-log/chunk-kv-regression-20260917-115123/`. The frame-fit error did
  not recur, but the same load failed with `320 load operations failed`.
  Chunk-KV 1 reported serving grants expiring during `SplitPreparing` from
  03:52:38 UTC; chunk-KV 2 reported `chunk RPC completion slab is full` at
  03:52:55 UTC. The chunk transport now uses the RPC client's pending-map
  fallback when a completion slab slot is occupied, instead of failing that
  tree write.
- The 1 MiB-target sustained-load follow-up remains a client-availability
  failure: `bench-log/chunk-kv-regression-20260917-141403/` ran 12,000 × 4 KiB
  puts at concurrency 32 in three hot ranges. The first two 4,000-operation
  rounds had `errors=0`; the third had `96` errors and `p99_us=5001226`.
  Server 1 entered `SplitPreparing`, repeatedly rejected recovered-partition
  activation for that lifecycle, then let its serving grant reach the safety
  deadline. Its captured metrics show `split_finalization_duration_us=3413093`,
  `lease_rejections=18017`, and `split_fence_lag_records=0`. Thus the moved
  child checkpoint is a partial fence-duration improvement, not a resolution
  of split-time client availability; R174 owns the remaining overlay-cutover
  and serving-grant work.
- An earlier attempt in `bench-log/chunk-kv-regression-20260917-112752/`
  failed before load because chunk-KV bootstrap timed out reaching DiskDB.
  Do not count its RSS as workload data.

Start diagnosis with the earliest error in each run. Do not increase timeouts,
weaken assertions, or count a failed split as a successful benchmark. After a
change, rerun the same workload and verify both `errors=0` and split/replay
convergence. `pixi run tree-lint` and `pixi run test-cpp` passed for the
range-rebuild fix; rerun the relevant gates after further code changes.

## Overlay-cutover follow-up

The 2026-09-17 R174/R175 runs used 12,000 × 4 KiB puts, concurrency 32, three
hot ranges, and a 1 MiB partition target:

- `chunk-kv-regression-20260917-184714` exposed missing child activation from
  the catalog split proof: 1,312 third-round errors and repeated prepared-child
  grant rejection.
- `chunk-kv-regression-20260917-185834` confirmed child activation but exposed
  heartbeat misclassification of `SplitPreparing`: 128 third-round errors.
- `chunk-kv-regression-20260917-190434` completed all 12,000 writes with zero
  errors, then stopped at five partitions because materialization retained the
  completed split transition ID and later balance could not match its source.
- `chunk-kv-regression-20260917-191402` again completed all writes with zero
  errors (worst p99 135,438 µs) and advanced the catalog through generation 26
  after releasing both overlay and split marker. It then failed closed during
  multi-generation balance: the overlay base expected tree manifest 3 at
  applied sequence 12007, while target/catalog recovery reopened manifest 2 at
  the same sequence.

The remaining failure is an exact tree-root pin/open problem, not client retry,
lease duration, or split-fence latency. Preserve the failing logs and implement
historical tree-manifest open for both initial target preparation and
`CatchupPublished` recovery before accepting the sustained gate.
