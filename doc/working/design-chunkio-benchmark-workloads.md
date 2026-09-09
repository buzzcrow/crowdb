<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk IO Benchmark Workloads

## Scope

R135 already provides the three-node combined deployment and large-object
write benchmark. This extension makes that fixture the common performance
surface for small writes and small, large, and mixed reads. Benchmark storage
is deliberately non-persistent: KV data and WAL use `mem-block`, while DiskIO
uses `NullDisk`. Read preparation still performs real chunk-client writes, so
the locations and all ChunkDB, DiskDB, and KV metadata are real. NullDisk read
payload bytes are synthetic and only their length is validated.

## Library workloads

`crowdb-chunk-client::benchmark` owns workload generation and aggregation.
The CLI only maps arguments, connects a client, and formats results.

- Large write retains its current streaming/direct-buffer runner.
- Small write reserves through `prepare_small_write`, submits deterministic
  owned bytes, awaits each location, and includes `shutdown_small_writes` in
  completion. Results expose object/byte rate, p50/p99, errors, and the
  queue-driven pipeline, batch, and scale counters.
- Read preparation builds a bounded reusable keyset outside the timed window.
  Small objects are written through the shared small-write pool and drained;
  large objects use the existing prepared large-write API. The timed workers
  repeatedly choose a location from this keyset and call `read_object`.
- Mixed read selection is deterministic. `mixed_large_percent` controls the
  fraction of requests that use the large-object keyset; it is not a byte
  ratio.

Every runner has a maximum request/object count and an optional admission
duration. Once admission stops it drains already accepted operations. Worker
state is private, counters are merged after join, and no benchmark hot-path
lock is introduced. At most `concurrency` payload buffers and the bounded
prepared keyset are retained.

`ChunkIoClientConfig::diskio_connections_per_endpoint` builds a fixed pool for
each discovered DiskIO endpoint. Segment requests select a connection through
an atomic round-robin counter. Refresh builds complete replacement pools
off-path before `ArcSwap` publication. This prevents maximum large-read load
from multiplexing every 1 MiB response through one connection without adding
a route lock.

## CLI and sentinel

Keep `bench chunkio write` as the compatible large-write command and add
`write-small`, `read-small`, `read-large`, and `read-mix`. Shared controls are
object/request count, duration, concurrency, deterministic seed, and metrics
interval. Small-write controls expose object size and queue-based scaling
thresholds with one initial and at most 32 pipelines. Read controls expose
dataset size; large and mixed reads also expose large-object EC/block/chunk
policy.

The existing large-write sentinel is retained. A small-write sentinel runs
1 KiB and 8 KiB objects through low, saturated, and maximum-concurrency cases.
A read sentinel prepares real metadata once per process invocation and runs
small, large, and mixed cases. All deployments pass `--kv-backend mem-block
--wal-backend mem-block --no-fsync`; combined deployment supplies NullDisk
for payload IO. Sentinels gate completion, exact byte accounting, zero errors,
and required service metrics, but do not claim payload-content verification.
ChunkDB `/ready` returns success only after a strict range guard has loaded at
least one owned range, so combined deployment cannot finish while published
bindings have not yet reached the serving process.

## Correctness boundaries

- Preparation failure produces a failed result and no timed sample.
- A successful read must return exactly the location's logical length. Its
  bytes are not compared with the prepared source under NullDisk.
- Small-write shutdown failure is part of the sample failure.
- Empty datasets, zero sizes/counts/concurrency, percentages over 100, and
  invalid policies are rejected before connecting or starting load.
- Read preparation is bounded by `dataset_objects`; benchmark request count or
  duration never grows retained metadata in the client.
