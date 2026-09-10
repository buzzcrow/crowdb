<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Small IO Flow Analysis

This document records the full-stack small-write and read benchmark flow,
baseline measurements, correctness limits, and ordered optimization work. The
data-path contracts are defined by
[`design-crowdb-chunkio.md`](design-crowdb-chunkio.md),
[`design-crowdb-chunkio-small-object-writer.md`](design-crowdb-chunkio-small-object-writer.md),
and [`design-crowdb-chunkio-reader.md`](design-crowdb-chunkio-reader.md).

## Contents

1. [Benchmark boundary](#1-benchmark-boundary)
2. [Small-write flow](#2-small-write-flow)
3. [Read flow](#3-read-flow)
4. [Baseline results](#4-baseline-results)
5. [First divergence](#5-first-divergence)
6. [Bottleneck analysis](#6-bottleneck-analysis)
7. [Ordered improvement plan](#7-ordered-improvement-plan)

## 1. Benchmark Boundary

The sentinels use a co-located three-node cluster with three instances each of
KV, DiskDB, ChunkDB, and DiskIO. KV data and WAL use `mem-block`; fsync is
disabled. DiskIO uses `NullDisk`, so requests traverse client routing,
crowdb-rpc, the DiskIO request handler, and io_uring without retaining payload
data. Read preparation performs real writes before the measurement window and
retains the resulting locations.

| Parameter            | Value                                      |
| -------------------- | ------------------------------------------ |
| Timed duration       | 20 seconds per case                        |
| Client concurrency   | 1, 4, 32; up to 128 and 256 for small IO  |
| DiskIO connections   | 8 in sentinels, 4 in CLI; configurable    |
| DiskIO RPC workers   | 1 client and 1 server worker by default   |
| Small object sizes   | 1 KiB and 8 KiB writes; 8 KiB reads       |
| Large read object    | 16 MiB                                     |
| Large read EC        | 8+4, 1 MiB shards                          |
| Read dataset         | 16 objects per selected class              |
| Mixed read           | 50% small and 50% large by request count  |
| Write pipeline limit | 32                                         |
| Write batch limit    | 1 MiB or 1,024 objects                     |

A result is valid only when every admitted operation is accounted for,
`errors=0`, `incomplete=0`, and the watchdog count is zero. Throughput after the
first error is diagnostic only because immediate failures can increase the
request counter without doing IO.

## 2. Small-Write Flow

```text
worker
  -> prepare_small_write
  -> enqueue whole object in shared writer pool
  -> queue-byte/object threshold may add a pipeline
  -> completed pipeline immediately drains the objects currently queued
  -> drained objects form the next mirror strip without a batching delay
  -> ChunkDB allocation and DiskDB/KV metadata commit
  -> DiskIO mirror writes to NullDisk
  -> completion returned to every object
```

Pipeline scale-out is based only on queued bytes or queued object count. A new
pipeline is added only when every active route is under queue pressure, so a
startup burst on pipeline zero cannot create 31 empty pipelines. Scale-in also
uses state rather than elapsed time: all routes must be empty and idle, and the
global reservation count must be zero.

The benchmark exposes both logical batches and physical aggregation. For every
normal mirror write it counts the number of represented objects, submitted
buffers, logical bytes, and actual full-strip payload bytes. Repair and
mirror-to-EC traffic retain their separate counters. It also records the peak
pipeline count. Dedicated E2E coverage stops admission and observes scale-in
to the configured minimum from queue and busy state. Neither scale-out nor
scale-in uses elapsed idle time as a decision input.

The pipeline follows the RPC send-queue scheduling model: completion of the
current batch immediately fetches all currently available work, bounded by the
object and byte caps. The 500 ms batch watchdog only reports a stuck durability
operation. It never delays a batch, triggers a flush, or cancels work.

## 3. Read Flow

```text
untimed preparation
  -> real small or large write
  -> ChunkDB/DiskDB/KV metadata commit
  -> retain writer-produced Location

timed request
  -> query ChunkDB layout
  -> read requested mirror or EC segment through routed DiskIO RPC
  -> on segment error, try mirror fallback or partial EC recovery
  -> persist observed failed segments in ChunkDB
  -> assemble and return object
```

The failure-observation path distinguishes durable segment failures from
transient transport, overload, timeout, alignment, and short-read results.
Only durable failures are persisted in `unavailable_segments`; transient
failures participate in fallback or EC recovery for the current request only.

## 4. Baseline Results

Measurements were taken on 2026-09-09 on the Intel Core i9-7960X development
host. DRAM totals are passive host-wide PMU samples and include all co-located
services.

### 4.1 Small Write

| Size  | Threads |       TPS | MiB/s | p50 ms | p99 ms | Objects/batch | Peak pipes | Errors | Valid |
| ----- | ------: | --------: | ----: | -----: | -----: | ------------: | ---------: | -----: | :---: |
| 1 KiB |       1 |    424.42 |   0.4 |  2.309 |  3.532 |          1.00 |          1 |      0 | yes   |
| 1 KiB |       4 |    856.48 |   0.8 |  4.627 |  6.095 |          2.00 |          1 |      0 | yes   |
| 1 KiB |      32 |  6,275.96 |   6.1 |  5.032 |  9.448 |          8.26 |          2 |      0 | yes   |
| 1 KiB |     128 | 19,508.32 |  19.1 |  6.166 | 13.627 |         15.36 |          5 |      0 | yes   |
| 1 KiB |     256 | 31,313.08 |  30.6 |  7.549 | 19.388 |         21.21 |          8 |      0 | yes   |
| 8 KiB |       1 |    390.94 |   3.1 |  2.492 |  3.909 |          1.00 |          1 |      0 | yes   |
| 8 KiB |       4 |    675.29 |   5.3 |  5.323 | 10.882 |          2.03 |          1 |      0 | yes   |
| 8 KiB |      32 |  4,464.84 |  34.9 |  6.701 | 18.595 |          8.65 |          2 |      0 | yes   |
| 8 KiB |     128 | 13,461.05 | 105.2 |  8.791 | 23.066 |         11.25 |          7 |      0 | yes   |
| 8 KiB |     256 | 23,446.56 | 183.2 |  9.811 | 27.503 |         18.97 |          9 |      0 | yes   |

The timer-free single-worker p50 is 2.31 ms for 1 KiB and 2.49 ms for 8 KiB;
the earlier roughly 56 ms rows were invalidated by the io_uring wake defect and
the batching deadline. The highest measured rates in this run are 31,313.08 TPS
for 1 KiB and 23,446.56 TPS for 8 KiB, both at 256 workers. These are observed
maxima, not demonstrated saturation peaks. Aggregation rises naturally with
queue occupancy even though there is no batching delay. All watchdog counters,
errors, and incomplete counts are zero. The retained matrix is
`bench-log/chunkio-small-completion-drain-20260909/results.tsv`.

A dedicated 1 KiB, one-worker distribution rerun completed 8,489 requests at
424.42 TPS: p50 2.309 ms, p90 2.665 ms, p95 2.918 ms, p99 3.532 ms, and max
26.575 ms. Maximum observed queue delay was only 90 us. Server and client stage
windows place the steady-state cost primarily in two serial durability phases:
the three parallel NullDisk mirror writes average roughly 0.72-0.85 ms, followed
by the fenced `advance_chunk_write` metadata commit averaging roughly
1.1-1.4 ms. The retained distribution run is
`bench-log/chunkio-small-latency-distribution-20260909/results.tsv`.

### 4.2 Read Before the Correctness Fix

| Workload | Threads | Success | Errors    | Success TPS | MiB/s | p50 ms | p99 ms  | Valid |
| -------- | ------: | ------: | --------: | ----------: | ----: | -----: | -------: | :---: |
| Small    |       1 |     387 |         0 |       19.35 |   0.2 | 51.643 |  52.110  | yes   |
| Small    |       4 |  31,988 |         0 |    1,595.21 |  12.5 |  2.479 |   3.242  | yes   |
| Small    |       8 |  24,599 |   220,333 |    1,229.86 |   9.6 |  2.250 |   3.299  | no    |
| Small    |      16 |  16,478 |   527,755 |      823.84 |   6.4 |  0.962 |   2.872  | no    |
| Small    |      32 |   7,479 | 1,397,831 |      373.94 |   2.9 |  1.062 |   1.841  | no    |
| Small    |     128 |  29,419 | 2,880,180 |    1,470.86 |  11.5 |  2.008 |   4.631  | no    |
| Small    |     256 |  11,676 | 2,985,510 |      583.74 |   4.6 |  4.633 |  58.562  | no    |
| Large    |       1 |      24 |         0 |        1.19 |  19.0 | 850.917 | 952.716 | yes   |
| Large    |       4 |     312 |        20 |       15.43 | 246.9 | 227.787 | 387.001 | no    |
| Large    |      32 |     629 |       445 |       30.26 | 484.2 | 260.863 | 818.468 | no    |
| Mixed    |       1 |      88 |         0 |        4.37 |  35.8 | 119.247 | 866.818 | yes   |
| Mixed    |       4 |     652 |        61 |       31.40 | 228.2 | 22.008 | 358.932 | no    |
| Mixed    |      32 |     551 |     1,134 |       26.36 | 253.5 | 248.930 | 1,009.263 | no    |

Only error-free rows are baselines. Failed rows do not establish a peak because
the failure cascade truncates real IO and then spins on metadata-marked
segments.

### 4.3 Final Read Baseline

NullDisk now completes its generated-content contract at the requested length,
transient read errors no longer update durable layout metadata, cluster reset
waits for fresh service registrations, and flushing keeps frozen memtables
visible until their entries are published in L1. All final 20-second cases
completed with zero errors and zero incomplete requests.

| Workload | Threads | Success TPS | MiB/s | p50 ms | p99 ms |
| -------- | ------: | ----------: | ----: | -----: | -----: |
| Small    |       1 |      783.11 |   6.1 |  1.268 |  2.270 |
| Small    |       4 |    3,418.85 |  26.7 |  1.164 |  1.725 |
| Small    |       8 |    8,691.62 |  67.9 |  0.874 |  1.661 |
| Small    |      16 |   21,254.08 | 166.0 |  0.730 |  1.183 |
| Small    |      32 |   36,815.96 | 287.6 |  0.843 |  1.558 |
| Small    |     128 |   57,098.87 | 446.1 |  2.139 |  3.799 |
| Small    |     256 |   56,879.50 | 444.4 |  4.359 |  6.947 |
| Large    |       1 |       13.79 | 220.7 | 71.241 | 88.609 |
| Large    |       4 |       70.20 | 1,123.2 | 55.410 | 86.171 |
| Large    |      32 |      173.76 | 2,780.2 | 181.237 | 275.468 |
| 50/50 mix |      1 |       24.30 | 194.9 | 40.443 | 94.923 |
| 50/50 mix |      4 |      135.18 | 1,082.4 | 28.849 | 82.793 |
| 50/50 mix |     32 |      352.08 | 2,818.1 | 36.473 | 236.431 |

Small read reaches its useful saturation region around 128 workers: increasing
to 256 does not improve TPS and roughly doubles p50. The main retained matrix
is `bench-log/chunkio-read-final-20260909/results.tsv`; the two preparation
cases rerun after the readiness fix are in
`bench-log/chunkio-read-readiness-fix-20260909/results.tsv`.

### 4.4 Read After the io_uring Wake Fix

The common DiskIO io_uring poller previously accepted an idle-ring submission,
woke from `epoll_wait`, and then waited up to 50 ms for a completion before it
published the new SQE. It also reused the external completion `eventfd` as its
private submission wakeup. A private wake `eventfd` now separates those roles,
and the poller publishes submissions immediately after wakeup.

| Threads |   Success |       TPS | p50 ms | p99 ms | Errors | Valid |
| ------: | --------: | --------: | -----: | -----: | -----: | :---: |
|       1 |    15,735 |    786.71 |  1.269 |  2.011 |      0 | yes   |
|       4 |    68,215 |  3,410.15 |  1.168 |  1.712 |      0 | yes   |
|      32 |   744,514 | 37,198.96 |  0.841 |  1.462 |      0 | yes   |
|     256 | 1,116,927 | 55,789.01 |  4.441 |  7.224 |      0 | yes   |

At one thread this changes p50 from 51.7 ms to 1.27 ms and throughput from
19.36 to 786.71 successful reads/s. Stage histograms show ChunkDB query at
about 0.8 ms and DiskIO read at about 0.4 ms for one thread. At 256 threads,
ChunkDB query remains about 0.4 ms while DiskIO read grows to about 4.7 ms and
dominates the request. The retained stage result is
`bench-log/chunkio-read-stage-final-20260909/results.tsv`.

## 5. Resolved First Divergences

The earliest confirmed read divergence is a short DiskIO read from NullDisk:

```text
disk I/O error: PartialWrite
```

The return-code name is shared with writes, but the server emits it when a read
completion returns fewer bytes than requested. The eight- and sixteen-worker
small-read runs both eventually show this exact error. Earlier occurrences on
other mirror replicas had already been persisted as unavailable, so the next
short read exhausts all three replicas.

The failure then amplifies:

1. DiskIO returns a transient short-read or transport failure.
2. `StripReader` adds the segment to `failed_segments` without an error class.
3. `ChunkReader` persists every observed segment in `unavailable_segments`.
4. Mirror reads eventually have no eligible replica; EC recovery eventually
   has more missing shards than parity can reconstruct.
5. Subsequent requests fail before payload IO and rapidly inflate the error
   counter.

The 32-worker client also records `rpc.send.queue.full.c=19`. This confirms RPC
backpressure is reached, but the observed short read is the earliest directly
reported trigger. Queue-full events and short reads are both transient and
must not be converted into durable media-failure metadata.

The fix makes NullDisk synthesize the complete requested buffer after any
nonnegative backing completion. DiskIO RPC/short-read failures are typed as
transient, while confirmed missing disks, missing zones, and media IO errors
remain durable. Mirror and EC readers persist only the durable class. A real
ChunkDB E2E verifies that an injected transient mirror failure leaves
`unavailable_segments` empty and that the original client can still read the
object.

The next divergence was the 50-ms low-concurrency latency in the common
io_uring poller described in section 4.4. A C++ regression submits consecutive
operations after the ring becomes idle and verifies that neither waits for the
poll timeout.

At 256 workers, the next first divergence was a transient metadata `NotFound`.
Repeated probes showed the same key becoming visible tens of microseconds
later without a rewrite. `Crowdbtree::flush()` had removed the draining frozen
memtables before publishing their entries in L1, creating an L0/L1 visibility
gap for concurrent readers. The draining tables now remain in `frozen_` until
L1 publication completes. A stable sentinel in the concurrent freeze/drain
test proves that an immutable key never disappears during this transition.

Cluster reset had a separate readiness race: DiskDB could start before DiskIO,
and stale service heartbeats could satisfy registration checks. Reset now
starts DiskIO first and requires post-restart heartbeats for DiskIO, DiskDB,
and ChunkDB. Combined deployment also waits until every ChunkDB `/ready`
refreshes and exposes the published range assignment.

## 6. Bottleneck Analysis

### 6.1 Small Writes

A post-instrumentation 8-KiB/32-worker run completed 62,567 objects in 32,424
batches. Three mirror copies produced 97,272 normal DiskIO writes representing
187,701 object buffers, or 1.93 buffers per physical request on average. The
requests carried 1.43 GiB of replicated new logical bytes but 94.99 GiB of
full-strip payload. Peak pipelines reached 32. This confirms that the next
small-write optimization should reduce full-strip rewrite amplification and
improve batch fill together, rather than optimizing object admission alone.
The retained result is
`bench-log/chunkio-small-write-20260909-095047/results.tsv`.

Two queue-accounting problems explained the excessive scale-out. A carried
object that did not fit the current batch was subtracted from queue pressure
twice, underflowing the unsigned counter. Synchronized batch boundaries also
looked idle and caused scale-in/scale-out thrashing while admitted objects still
held reservations. The corrected policy has real E2E coverage for carried
objects, byte-pressure scale-out, object-pressure scale-out, and final
state-driven scale-in.

With a 16-object pressure threshold, 8-KiB/256 reached 23,257.11 successful TPS,
used at most 9 pipelines, and performed 51,288 physical mirror RPCs for
1,397,154 represented objects: 27.24 objects per physical request. Compared
with the earlier 8-KiB/32 run, throughput is 7.46 times higher and aggregation
is 14.1 times higher while the peak pipeline count falls from 32 to 9. The
retained result is
`bench-log/chunkio-small-tune-balanced-q16-256-20260909/results.tsv`.

### 6.2 Reads

The added `chunkio.chunk.query.e2e` and `chunkio.diskio.read.e2e` histograms
separate metadata from payload IO. KV engine get is about 4-5 microseconds and
KV RPC get is about 36-82 microseconds, so KV get is not the observed limit.
At 256 threads the DiskIO stage, including RPC, queueing, io_uring, and response
delivery, accounts for nearly all of the 4.44-ms median.

Connections and both RPC worker counts are now independently configurable:

- benchmark client: `--diskio-connections` and `--diskio-rpc-workers`;
- DiskIO service: `--rpc-workers`;
- combined deployment: `--diskio-rpc-workers`;
- regression scripts: `CHUNKIO_*_DISKIO_CONNECTIONS`,
  `CHUNKIO_*_DISKIO_RPC_WORKERS`, and `CHUNKIO_*_SERVER_RPC_WORKERS`.

An A/B small-write run at 8 KiB and 256 threads measured 22,802.91 successful
TPS with 8 connections and 1+1 workers, versus 23,993.26 TPS with 16
connections and 4+4 workers. The 5.2% single-run increase is modest and is not
yet sufficient to change the defaults. The valid results are retained in
`bench-log/chunkio-small-diskio-default-ab-retry-20260909/results.tsv` and
`bench-log/chunkio-small-diskio-c16-cw4-sw4-final-20260909/results.tsv`.

The initial read A/B attempts exposed the ownership-readiness and memtable
visibility bugs above, so they are not retained as performance results. The
final default-configuration matrix is fully valid; a repeated worker-count A/B
remains future measurement work.

### 6.3 Foreground EC A/B Sentinel

The steady-state sentinel runs mirror-only and foreground-EC modes for 60
seconds with identical inputs, reports reservation startup latency and parity
bytes, and requires EC throughput to remain at least 70% of its paired mirror
run. At 1 KiB and 32 threads, mirror produced 33,729.19 objects/s and EC
produced 33,807.27 objects/s (100.23%). The retained paired result is
`bench-log/chunkio-small-write-20260910-081757`.

The clean 128-thread reproduction produced 107,420.80 mirror objects/s and
98,835.24 EC objects/s (92.01%), with zero errors, incomplete objects, and
watchdogs. Foreground parity wrote 432,013,312 bytes; reservation wait totaled
10,384,486 microseconds for mirror and 9,561,283 microseconds for EC. The
retained result is
`bench-log/chunkio-small-write-128-repro-20260910`.

An earlier 128-thread sample in the first matrix was invalidated when one KV
server exited and leader loss caused watchdog expirations. The exact clean
reproduction did not reproduce the exit or the throughput failure, so the
sentinel threshold remains unchanged.

## 7. Ordered Improvement Plan

1. Make pipeline retirement independent of a slow replacement-allocation RPC.
   A sustained 32-pipeline run can scale in partway and then block the manager
   while one retiring pipeline waits on DiskDB; pending conversion groups must
   remain recoverable by ChunkDB tasks.
2. Repeat the 20-second DiskIO connection and worker matrix with at least three
   samples per cell now that the metadata visibility and readiness bugs are
   fixed.
3. Add DiskIO server queue depth, per-connection in-flight, and response-queue
   latency metrics. The current aggregate DiskIO stage cannot distinguish the
   single io_uring pipeline from RPC response delivery at 256 threads.
4. Keep the default at 8 connections and 1+1 workers until the valid repeated
   matrix demonstrates a stable latency or throughput improvement.
