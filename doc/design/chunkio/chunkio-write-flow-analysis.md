<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk IO Large-Write Flow Analysis

Large-object write flow from the benchmark workload through chunk
preparation, fetch, EC encode, DiskIO RPC, and chunk seal. The benchmark
sentinel is `tools/bench-chunkio-write-regression.sh`. The write pipeline
architecture is in
[`design-crowdb-chunkio.md`](design-crowdb-chunkio.md); this doc traces the
measured hot path and records benchmark results.

## 1. Flow

```text
prepare_large_writes (before timer)
  -> ChunkIoClient::prepare_large_writes
     N sessions, each allocates first chunk (2 strips)
     distributed round-robin across workers
  -> timer starts

per object:
  -> pop PreparedLargeWrite from queue
  -> push replacement session to back of queue
     (concurrent chunk allocation, off the data path)
  -> write_stream / write_buffers
     -> [Fetch]           AsyncRead → 1 MiB Bytes blocks
        stream:   socket read into owned BytesMut, freeze, send
        direct:   caller-owned Bytes slices, no copy
     -> [ChunkWriter::push]
        block-granularity drive loop:
          push data block → DiskWriter::write (1 RPC per block)
          strip full → spawn parity task, auto-rotate to next strip
          chunk full → seal, pull next Chunk from prefetch
     -> [Parity Task]      EC encode → parity blocks → DiskWriter::write
        encode_parity_from_shards (isa-l, pre-split shards)
        data + parity handles grouped per strip
     -> [Completion]       join in-flight parity handles
        seal_chunk RPC
        return ProtoLocation
  -> record latency + per-stage timings
  -> next object until deadline or object_count

timer stops after all workers drain
DRAM BW measured from after prefetch to all writes complete
```

## 2. Measured Stages

The benchmark result (`LargeWriteBenchmarkResult`) aggregates per-object
timings across all concurrent workers. Each stage time is total microseconds
divided by object count — milliseconds per object, averaged across writers.
Stages overlap and must not be summed into latency.

| Stage                | What it measures                                            |
| -------------------- | ----------------------------------------------------------- |
| Source/read copy     | Time in the fetch stage reading from `AsyncRead` into blocks. Zero in direct-buffer mode. |
| Fetch assembly copy  | Buffer assembly copies inside the writer. Zero in both modes when the pipeline holds pre-split shards. |
| EC encode            | `encode_parity_from_shards` — isa-l parity computation per strip. |
| Write-completion wait| Time joining in-flight DiskIO write handles + `seal_chunk` RPC. Dominated by loopback RPC round-trips. |

Additional result fields:

- **logical_mib_per_sec** — application bytes written (object size × objects) /
  elapsed. The user-visible throughput.
- **physical_mib_per_sec** — EC-expanded bytes (logical × 1.5 for 8+4) /
  elapsed. What the DiskIO layer actually transports.
- **objects_per_sec** — objects completed / elapsed.
- **latency_p50_us / p99_us** — per-object wall-clock latency from admission
  to seal completion.
- **dram_read/write/total_mib_s** — host uncore IMC counter bandwidth over
  the workload window (after prefetch). Multiplex-corrected. `None` when the
  PMU is unavailable.
- **preparation_stalls** — count and microseconds where a worker's
  replacement session fell behind and the writer waited for chunk allocation.

## 3. Workload Configuration

The regression sentinel uses fixed parameters across all cases:

| Parameter              | Value    | Notes                                              |
| ---------------------- | -------- | -------------------------------------------------- |
| Object size            | 16 MiB   | Two 8-MiB data strips per object.                  |
| EC scheme              | 8+4      | 8 data + 4 parity = 12 blocks per strip, 1 MiB each. |
| Block size             | 1 MiB    | One DiskIO RPC per block.                          |
| Chunk size             | 1 GiB    | Multiple objects per chunk; rotation is off the measured path at 16 MiB objects. |
| Prefetch chunks        | 10       | Write sessions prepared before the timer starts.   |
| Prefetch strips/chunk  | 2        | Strips pre-appended ahead of the write cursor.     |
| Duration               | 20 s     | Admission deadline; in-flight objects drain after. |
| Storage                | NullDisk | Real io_uring submission, no stable-media durability. |
| Deployment             | 3-node   | Co-located KV + DiskDB + ChunkDB + DiskIO, loopback. |

Cases vary only in concurrency (1, 4, 32) and input mode (stream vs direct
buffers). Stream mode traverses the production `AsyncRead` fetch path; direct
mode passes caller-owned `Bytes` blocks with zero fetch copies.

## 4. Benchmark Results

### 4.1 Intel Core i9-7960X — 2026-09-08

Intel Core i9-7960X (16c/32t, Skylake-X), 4 DDR4 channels at 2667 MT/s
(~85 GB/s peak), Linux 6.11, `perf_event_paranoid=-1`.

| Mode           | Writers | Logical MiB/s | Physical MiB/s |      p50 / p99 ms | DRAM read avg | DRAM write avg | DRAM total avg |
| -------------- | ------: | ------------: | -------------: | ----------------: | ------------: | -------------: | -------------: |
| Stream         |       1 |         162.5 |          243.8 |  99.191 / 111.225 |       2,496.6 |        1,393.0 |        3,889.6 |
| Direct buffers |       1 |         229.1 |          343.7 |   69.191 / 85.641 |       1,996.1 |        1,228.7 |        3,224.8 |
| Stream         |       4 |       1,965.6 |        2,948.5 |   31.917 / 51.806 |      12,699.7 |       10,943.0 |       23,642.7 |
| Direct buffers |       4 |       2,672.1 |        4,008.1 |   22.811 / 40.000 |      11,799.6 |       12,051.5 |       23,851.1 |
| Stream         |      32 |       3,249.3 |        4,873.9 | 151.716 / 267.992 |      19,562.5 |       15,603.7 |       35,166.2 |
| Direct buffers |      32 |       3,547.9 |        5,321.9 | 138.654 / 261.388 |      14,816.5 |       12,766.6 |       27,583.1 |

Per-object client-stage time (milliseconds per object, aggregated across
concurrent writers; stages overlap, do not sum):

| Flow step             | Stream 1 | Direct 1 | Stream 4 | Direct 4 | Stream 32 | Direct 32 |
| --------------------- | -------: | -------: | -------: | -------: | --------: | --------: |
| Source/read copy      |   20.247 |        0 |    3.737 |        0 |     8.704 |         0 |
| Fetch assembly copy   |        0 |        0 |        0 |        0 |         0 |         0 |
| EC encode             |   20.175 |   13.164 |   15.213 |   12.357 |    22.045 |    20.222 |
| Write-completion wait |   53.990 |   54.238 |   11.741 |   10.118 |   122.786 |   120.972 |

### 4.2 AMD Ryzen 9 5950X — _pending_

| Mode           | Writers | Logical MiB/s | Physical MiB/s | p50 / p99 ms | DRAM read avg | DRAM write avg | DRAM total avg |
| -------------- | ------: | ------------: | -------------: | -----------: | ------------: | -------------: | -------------: |
| Stream         |       1 |               |                |              |               |                |                |
| Direct buffers |       1 |               |                |              |               |                |                |
| Stream         |       4 |               |                |              |               |                |                |
| Direct buffers |       4 |               |                |              |               |                |                |
| Stream         |      32 |               |                |              |               |                |                |
| Direct buffers |      32 |               |                |              |               |                |                |

| Flow step             | Stream 1 | Direct 1 | Stream 4 | Direct 4 | Stream 32 | Direct 32 |
| --------------------- | -------: | -------: | -------: | -------: | --------: | --------: |
| Source/read copy      |          |          |          |          |           |           |
| Fetch assembly copy   |          |          |          |          |           |           |
| EC encode             |          |          |          |          |           |           |
| Write-completion wait |          |          |          |          |           |           |

## 5. Analysis

### 5.1 Throughput Scaling

Single-writer throughput is low (162–229 MiB/s logical) because each 16 MiB
object requires 24 sequential-ish DiskIO RPCs (8 data + 4 parity per strip ×
2 strips) over loopback. The completion-wait stage dominates at 54 ms/object
— this is the aggregate time joining 24 RPC handles plus the `seal_chunk`
RPC, all serialized through one tokio worker.

At 4 writers, throughput jumps 12× (1,966–2,672 MiB/s) because the 24 RPCs
per object now overlap across objects. The loopback transport and DiskIO
io_uring pipelines parallelize naturally. Completion wait drops to 10–12
ms/object as the per-object join overlaps with other objects' writes.

At 32 writers, throughput rises only 1.6× over 4 writers (3,249–3,548 MiB/s)
while p50 latency balloons to 138–152 ms. The loopback RPC/DiskIO queue is
saturated: more concurrency increases queueing delay rather than bandwidth.
Completion wait rises to 121–123 ms/object.

### 5.2 Stream vs Direct

Direct-buffer mode is 1.41× faster than stream at 1 writer (229 vs 163 MiB/s)
because it eliminates the source-read copy (20.2 ms/object). At 4+ writers
the gap narrows to 1.36× (2,672 vs 1,966) as the RPC bottleneck dominates
over the fetch copy. At 32 writers the gap is 1.09× — the pipeline is
RPC-bound, not fetch-bound.

The fetch assembly copy is zero in both modes, confirming the shard-based EC
design: the pipeline holds data as pre-split 1 MiB `Bytes` blocks and
`encode_parity_from_shards` consumes them directly without re-splitting.

### 5.3 EC Encode

EC encode time is stable at 12–22 ms/object across all concurrency levels.
It does not scale with concurrency because isa-l parity computation is
CPU-bound and each parity task runs on one tokio worker. At 32 writers,
EC encode (20–22 ms) is no longer the bottleneck — completion wait (121–123
ms) dominates by 6×.

### 5.4 DRAM Bandwidth

Host DRAM bandwidth scales with concurrency: 3.2–3.9 GB/s at 1 writer,
23.6–23.9 GB/s at 4, and 27.6–35.2 GB/s at 32. The read/write split is
roughly balanced at 4+ writers (EC reads source data, writes parity +
RPC transport copies), which matches the expected traffic pattern.

At 32 writers, DRAM total (28–35 GB/s) is well below the 85 GB/s peak,
confirming the bottleneck is RPC queueing, not memory bandwidth. The
stream_32t case shows higher DRAM usage (35.2 vs 27.6 GB/s) because the
source-read copy adds memory traffic without adding useful throughput.

### 5.5 Bottleneck

The bottleneck is the loopback RPC/DiskIO transport queue. At 32 writers,
completion wait (121–123 ms/object) is 6× the EC encode time and 14× the
source-read time. The RPC path transports 24 MiB per object through
loopback sockets with kernel copies on both sides — this is the dominant
cost, not EC, not memory, not chunk allocation.

Further work should profile and reduce RPC/kernel transport cost (zero-copy
send, larger batch sizes, connection pooling) and EC/cache contention.
Increasing prefetch or adding wrapper layers will not address the measured
bottleneck.
