<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Chunk IO Data Path (Overview)

The chunk IO data path is the client-side layer that writes and reads
large-object data as EC-encoded strips across diskio servers, using chunkdb
for chunk lifecycle management (allocate, append, seal, delete). It
lives in the `crowdb-chunk-client` crate and is consumed by object store
layers and application upload handlers. The chunkdb server design
(chunk lifecycle, placement, EC integration) is in
[`doc/design/chunkdb/design-crowdb-chunkdb.md`](../chunkdb/design-crowdb-chunkdb.md);
the diskio block IO engine is in
[`doc/design/diskio/design-crowdb-diskio.md`](../diskio/design-crowdb-diskio.md).
This doc does not repeat their architecture — it covers the data path
that sits between them: the write pipeline, its backpressure and memory
model, and the design choices that make a 1 TB upload cost the same
~15 MB of RAM as a 50 MB one. The shared mirrored path for small objects is
specified in the
[small-object writer design](design-crowdb-chunkio-small-object-writer.md).

## Table of Contents

- [1. Non-Goals](#1-non-goals)
- [2. Key Design Decisions](#2-key-design-decisions)
- [3. Write Flow](#3-write-flow)
- [4. Backpressure and Memory Budget](#4-backpressure-and-memory-budget)
- [5. EC Integration](#5-ec-integration)
- [6. Chunk Rotation and Location](#6-chunk-rotation-and-location)
- [7. Completion and Error Handling](#7-completion-and-error-handling)
- [8. Interaction with Neighbors](#8-interaction-with-neighbors)
- [9. Tunables and Defaults](#9-tunables-and-defaults)
- [10. Application API and Routing](#10-application-api-and-routing)
- [11. Performance Workload](#11-performance-workload)

## 1. Non-Goals

- **No small-object writer details.** Shared-chunk packing is a separate
  component specified by the
  [small-object writer design](design-crowdb-chunkio-small-object-writer.md).
- **Reader is a separate component.** Location resolution, mirror/EC fetch,
  partial decode, range reads, and bounded streaming are specified in
  [Chunk Object Reader](design-crowdb-chunkio-reader.md).
- **No GC of leaked partial chunks.** Best-effort cleanup on abort
  leaves Active chunks for a future reaper; this doc does not specify
  the reaper.
- **No object protocol server.** Applications call the client library
  directly. An S3-compatible or other object protocol server can use the same
  API later without owning chunk placement or DiskIO routing.

## 2. Key Design Decisions

- **Block-granularity pipeline, not strip-granularity.** The first disk
  write starts after 1 MiB (one data block), not after the full 8 MiB
  strip. Three strips stay in flight simultaneously (N parity, N+1
  data, N+2 fetch) without unbounded memory. Strip-granularity would
  double time-to-first-byte and halve steady-state throughput.
- **Push-based "always store" contract.** `ChunkIoWriter::on_data`
  never rejects a buffer; it awaits until internal capacity is free.
  This puts the retry-loop in one place (the writer) instead of every
  caller. A `FeedStatus` (`Continue` / `Pause`) answers "would the next
  push block?" so a dedicated upload task can ignore it and block,
  while a shared handler task can pre-check via the non-async
  `require_data` hint and return 503 instead of stalling.
- **Bounded preparation, not eager allocation.** A 1 TB object does
  not allocate all 250K strips at once. Consumption-driven channels stay only
  `prefetch_strips_per_chunk` strips (default 2) and
  `chunk_preparation_depth` chunks (default 1) ahead,
  keeping allocation rate and KV metadata pressure bounded regardless
  of object size while keeping the cursor fed.
- **Warm admission, then continuous replacement.** Services that know their
  write policy prepare a configurable queue of write sessions before admitting
  load. The first chunk of every queued session is allocated concurrently.
  Consuming a session starts its replacement while the active object writes,
  so ChunkDB convergence and ordinary allocation are outside the data path.
- **Shard-based EC, no re-split copy.** The pipeline already holds data
  as separate 1 MB `Bytes` blocks. Re-splitting a contiguous buffer
  just to feed `crowdb_common::ec::encode` would copy 4 MB per strip for
  no benefit. `encode_parity_from_shards` takes pre-split shards
  directly and reuses the existing isa-l FFI path — no new C++ code.
- **Repair one failed shard, not the strip.** A durable data or parity write
  failure keeps every successful shard and retries only the failed segment
  through ChunkDB placement and fenced publication. Exhaustion aborts the
  unpublished object; reduced redundancy is never reported as success.
- **Memory budget per pool, not per object.** A `WriterPool` tracks a
  total `memory_budget` and an atomic `in_use` counter; `try_acquire`
  rejects with `MemoryBudgetExhausted` when full, enabling backpressure
  up the call stack. Per-writer footprint is constant (~15 MB peak for
  4+1 EC, 1 MB blocks, defaults), so `max_concurrent = budget /
  per-writer-footprint` — a 1 TiB and a 50 MiB upload cost the same RAM.
- **Real-process E2E is the primary write-flow coverage.** Large- and
  small-object E2E tests run the client against KV, DiskDB, DiskIO, and
  ChunkDB processes, query committed chunk metadata, and read payloads back
  from their allocated segments. Large-write coverage also verifies stored EC
  parity. The `ChunkAllocator` and `DiskWriter` seams support focused tests for
  fault injection, backpressure, cancellation, accounting, and exact boundary
  conditions that a real cluster cannot trigger deterministically; they are
  auxiliary coverage rather than substitutes for the end-to-end flow.
- **Drive loop in `ChunkWriter`, not the object layer.** The
  strip-level drive loop (push block → write to disk → auto-rotate
  strips when full → spawn parity) lives inside `ChunkWriter::push`.
  The object layer calls `push` + `is_full` + `seal` — it does not
  track strip boundaries, block indices, or parity handoff. This
  eliminates the `StripPlacement` bridge type and keeps the object
  layer thin.
- **Own flatbuffer types directly.** `ChunkWriter` owns `Arc<Chunk>`,
  `EcStripWriter` holds `Arc<Chunk>` + strip index, and `seal()`
  returns `ProtoLocation` directly. No parallel wrapper structs
  (`StripPlacement`, `Location`) — the flatbuffer types are the canonical
  representation throughout the write path.

## 3. Write Flow

The writer runs three concurrent stages: a fetch stage, a
`ChunkWriter` (which owns the strip-level drive loop + internal strip
prefetch), and a bounded pool of background parity tasks.
`LargeObjectWriter` exposes two driving modes over the same pipeline —
stream mode (`write_stream`, pulling from an `AsyncRead`) and push mode
(the `ChunkIoWriter` trait, §2). In stream mode a fetch stage pulls
from `AsyncRead`; in push mode the caller's bytes go directly to the
block channel. The `ChunkWriter` drive loop is identical in both modes.

```
                    ┌────────────────────┐
                    │  ChunkPrefetch     │  background, bounded:
                    │  1 chunk ahead     │  pre-allocates next Chunk
                    │  (2 strips each)   │  for a 16 MiB 8+4 write
                    └─────────────┬──────────┘
                             │ pre-allocated Chunk
                             ▼
  AsyncRead ──► [Fetch] ──► block_buf ──► [ChunkWriter::push] ─────► disk (data)
             read ≤1MB    (1 MB per     │  write 1 data block → 1 disk
             directly into    block)        │  (immediately, no EC wait)
             send to write                  │
             max_cached_buffer              │  when all 4 blocks of strip N written:
             = 4 MB (default)               ├──── spawn parity ────────────────────
             (backpressure if full)         │                     ▼
                                          │   [Parity Task N] (background)
                                          │   EC encode → 4 parity blocks
                                          │   write parity → disks 9..12
                                          │   (no join — handles collected)
                                          ▼
                                     auto-rotate to strip N+1, block 0
                                     (no wait for parity)
                                          │
                                          │  internal strip prefetch:
                                          │   append_chunk ahead of cursor
                                          │   (bounded: prefetch_strips_per_chunk=2)
```

The flow, step by step:

- **Fetch.** Reads from the `AsyncRead` stream directly into the owned
  `BytesMut` block. A socket read may return less than 1 MiB; the fetch stage
  accumulates in that same allocation, freezes it without copying, then sends it to
  `ChunkWriter::push` immediately — it does not wait for the full 4 MB
  strip. The fetch stage sends sequential `Bytes` blocks and does not
  track block indices or strip boundaries; `ChunkWriter` owns indexing.
  On EOF with a partial last block, the partial block is sent.
- **ChunkWriter::push (drive loop).** The central coordinator. It
  owns `Arc<Chunk>` and shares it with `EcStripWriter` by ref count.
  On each `push`, if the current strip is full, it finishes the strip
  (submits bounded data/parity writes and collects completion handles,
  no join), checks `is_full()` (chunk-level), and either returns
  `Pause` (chunk full — caller rotates chunks) or opens the next strip
  (from `chunk.strips` if pre-appended by the internal strip prefetch,
  or via inline `append_chunk` RPC). It then pushes the block to the
  new strip's data segment via `DiskWriter::write` — one disk per
  block. Independent data blocks are submitted without serial completion
  waits, and strip N+1 overlaps strip N's parity completion.
- **Parity.** One background task per strip. It receives the strip's
  data blocks, EC-encodes via `encode_parity_from_shards` (§5) into
  `code_num` parity blocks and writes them to the remaining segments via
  `DiskWriter::write`.
  Data and parity handles are grouped into one completion per strip.
  `ChunkWriter` retains at most `parity_depth` strip completions and joins all
  remaining completions at `seal`
  time (not at strip finish) — this decouples parity durability from
  strip rotation, allowing strip N+1's data writes to start before
  strip N's parity completes.
- **Strip prefetch (internal to ChunkWriter).** A background task
  appends strips to the current chunk via `append_chunk` ahead of the
  write cursor, bounded by `prefetch_strips_per_chunk` (default 2). A normal
  `append_chunk` response contains only new strips plus `modify_ts`; the
  writer merges them locally. A stale revision response supplies the complete
  current chunk and is retried once. The result replaces `self.chunk` (Arc-swap — old Arc in
  any in-flight `EcStripWriter` stays alive). For known-size objects,
  the prefetch stops after enough strips are allocated; for
  unknown-size objects, it stays `prefetch_strips_per_chunk` ahead.
- **Chunk prefetch (ChunkPrefetch, object layer).** Pre-allocates the
  next `Chunk` with `prefetch_strips_per_chunk` strips ahead of rotation, up to
  `chunk_preparation_depth` ahead. On chunk rotation, the object layer
  pulls the next `Chunk` from the prefetch receiver (fast path —
  pre-allocated) or calls `on_demand` (slow path — prefetch fell
  behind). The `Chunk` is passed to `ChunkWriter::open`, which wraps it
  in `Arc` and starts the internal strip prefetch.

### 3.1 Partial Last Strip

Partial strips occur only at EOF, never mid-chunk. When EOF arrives
before all `data_num` blocks of the current strip are filled, the main
write task writes only the filled data blocks, releases the empty ones,
hands the partial set off to parity for partial EC (§5), and records
`sealed_length` for `seal_chunk`.

## 4. Backpressure and Memory Budget

Two independent limits bound the pipeline; neither depends on object
size.

- **`max_cached_buffer`** (default 4 MB = one strip) bounds un-written
  data in the fetch channel — blocks sent to the main write task but
  not yet written to disk. When disk write is slower than network
  receive, the cache fills; once full, the fetch stage blocks and
  throttles the stream to the disk write speed.
- **`parity_depth`** (default 2) bounds in-flight parity tasks. When
  the parity pool is full, the main write task blocks at hand-off —
  backpressure on the write path, decoupled from the fetch cache.

If prealloc falls behind, the main write task awaits on the bounded
strip channel; the fetch stage keeps filling `max_cached_buffer`, then
blocks when full. No data is lost.

### 4.1 Per-Writer Footprint

For 4+1 EC, 1 MB blocks, defaults:

- `max_cached_buffer` — 4 MB un-written data in fetch cache.
- 1 block being written — 1 MB.
- Up to `parity_depth` (2) parity tasks, each holding the strip's data
  blocks (4 × 1 MB = 4 MB, shared via `Bytes` ref count — not copied,
  but resident until EC compute completes) + 1 parity block (1 MB).

Peak: 4 + 1 + 2 × (4 + 1) = **15 MB**. The conservative 15 MB assumes
both in-flight parity tasks hold data refs simultaneously; realistic
steady-state peak is ~11 MB, since EC compute is fast relative to disk
write + fsync and parity tasks stagger.

### 4.2 WriterPool

`WriterPool` tracks a `memory_budget` and an atomic `in_use` counter.
`try_acquire` returns `MemoryBudgetExhausted` when the budget is full;
release decrements `in_use` on `Drop`. `max_concurrent =
memory_budget / per-writer-footprint`. Many concurrent large-object
uploads are thus bounded by available RAM, not by object size — the
pool rejects new writes when the budget is exhausted, propagating
backpressure up the call stack.

## 5. EC Integration

The pipeline holds data as separate 1 MB `Bytes` blocks; the existing
`crowdb_common::ec::encode` re-splits a contiguous buffer into shards,
which would force a 4 MB copy per strip just to re-split. Instead,
`encode_parity_from_shards(scheme, data_shards)` takes pre-split data
shards directly and reuses the existing isa-l FFI path — no new C++
code.

`data_shards.len()` is `data_num` for a full strip, or `< data_num`
for a partial strip. isa-l supports partial EC: missing shards are
treated as zero for the encoding matrix, so no padding is written to
disk — the reader reads only `sealed_length` bytes. The function
returns `code_num` parity shards.

Edge cases:

- Full strip → standard EC.
- Partial strip → parity from present shards; reader reads only
  `sealed_length` bytes.
- Single-block object (< 1 MB) → 1 partial data shard, 1 parity shard.

## 6. Chunk Rotation and Location

Very large objects (`> max_chunk_size`) span multiple chunks. Chunk
size is always a multiple of strip data capacity — the writer only
appends whole strips — so rotation happens at strip boundaries, never
mid-strip. When `ChunkWriter::push` finishes a strip and `is_full()`
returns true, it returns `FeedStatus::Pause` without pushing the
buffer. The object layer then calls `seal()` (joins all in-flight
parity handles, `seal_chunk` RPC, returns `ProtoLocation`), records
the location, pulls the next `Chunk` from `ChunkPrefetch`, and opens a
new `ChunkWriter`. The buffer is re-pushed to the new chunk. The
`ProtoLocation` array accumulates one entry per rotated chunk, ordered
by `logical_offset`.

### 6.1 Location

`ProtoLocation` (from `crowdb-protocol::chunkdb::rpc::Location`) records
which chunk holds a contiguous byte range of an object, the byte range
within that chunk, and the object-level logical offset/length so a
multi-chunk object reads back as one contiguous stream. An object
spanning N chunks has N locations ordered by logical offset, contiguous
and non-overlapping. The within-chunk offset is always 0 for the
large-object writer (dedicated chunks filled from the start); it
exists for future shared-chunk packing and range reads. Serialization
is flatbuffers via the flatbuffer `Message` trait (`encode_to_vec` / `decode`).

Edge cases:

- Empty object (size 0) → `Vec<Location>` is empty; no chunk allocated.
- `logical_length` may be < `length` in future hole-punch scenarios; the
  writer always sets them equal.
- `max_chunk_size` not a multiple of strip data capacity → rotation
  still happens at strip boundaries; the actual chunk size is the
  multiple-of-strip value at or just above the threshold.

## 7. Completion and Error Handling

`write_stream` must seal the final chunk and return the `ProtoLocation`
array on success. On error or abort it must not leak partial chunks and
must return already-sealed `ProtoLocation`s for caller cleanup.

- **Completion** — `ChunkWriter::seal()`: finish the current strip if
  it has data (spawn partial parity, no join), join all in-flight
  parity handles, `seal_chunk` RPC, return `ProtoLocation`. If the
  chunk is empty (0 bytes written), `delete_chunk` instead of
  `seal_chunk`.
- **Error / abort** (`on_error`) — stop strip and chunk prefetch, retain and
  drain every submitted write completion, then `delete_chunk` on the partial
  chunk. Draining before deletion prevents a late write from hitting reused
  storage. The public prepared-write API returns an error rather than a
  partially successful object.
- **Single-segment replacement** — a failed data or parity completion retains
  its `Bytes`, inserts the failed disk into the client-wide lock-free negative
  list, and queries current chunk metadata. ChunkDB allocates one tentative
  placement excluding every existing strip disk and nodes already at the
  strip's failure-domain limit. The client writes that segment and publishes a
  geometry-identical strip through exact revision-and-range replacement.
  Successful shards are untouched.
- **Fencing and retry** — publication uses a deterministic operation ID.
  Ambiguous results retry the identical request. A definite revision conflict
  discards the tentative segment, re-queries, and retries only while the exact
  failed identity remains. Repeated disk failures extend their exclusion TTL.
  Attempts are bounded by `large_write_repair_attempts`, three by default.
- **Repair exhaustion** — `ChunkWriter::seal` returns the failure before
  `seal_chunk`; its owner drains remaining completions and deletes the active
  chunk. It never seals a strip with missing parity or publishes degraded
  large-object locations.
- **Dropped writer** — dropping does not perform async metadata cleanup.
  Applications call `on_error` when abandoning a started push-mode write;
  an Active partial chunk left by process loss remains for future lifecycle
  GC.

Edge cases:

- `on_data` after `on_finish` / `on_error` → `IoError::Finished`.
- `on_finish` twice → `IoError::Finished`.
- `on_error` with no sealed chunks → `Ok(vec![])`.
- EC encode failure → pipeline aborts immediately. EC encode is a CPU/ISA-L
  failure rather than a disk placement failure and cannot use segment
  replacement.
- `delete_chunk` fails during cleanup → log + continue (best-effort;
  the partial chunk stays Active and is reaped by a future GC task).

## 8. Interaction with Neighbors

- **chunkdb** — the writer calls allocate / append / seal / delete /
  update_chunk_strip / query via `ChunkAllocator`. chunkdb handles
  placement and lifecycle; the writer is unaware of internal placement
  logic — it receives `Segment` placements and writes to them.
- **diskio** — the writer writes data and parity blocks through the single
  durable-completion contract `DiskWriter::write`.
  `RoutedDiskWriter` owns discovery and routes every segment by disk ID to the
  unique live DiskIO owner. The fixed-connection `DiskioBlockWriter` remains a
  low-level adapter for focused fixtures.
- **crowdb-common EC** — `encode_parity_from_shards` is the shard-based +
  partial-encode entry point used by parity tasks.
- **crowdb-protocol** — `ChunkId`, `*ChunkRequest` types, `Segment`,
  `Chunk`, `Location` (`ProtoLocation`) message.

## 9. Tunables and Defaults

| Knob | Default | Role |
| --- | --- | --- |
| `max_chunk_size` | 1 GB | Chunk rotation threshold. |
| `prefetch_strips_per_chunk` | 2 strips | Strip preparation ahead of write cursor. |
| `parity_depth` | 2 strips | Completed-strip write groups allowed in flight. |
| `chunk_preparation_depth` | 1 chunk | Chunk preparation ahead of rotation. |
| fetch granularity | 1 MB | One data block per fetch call. |
| `max_cached_buffer` | 4 MB | Un-written data budget in fetch channel (one strip). |
| `memory_budget` | (pool) | `WriterPool` total; `max_concurrent = budget / per-writer-footprint`. |

## 10. Application API and Routing

`ChunkIoClient` is the application boundary. `connect` takes management seeds
and discovers ChunkDB endpoints, DiskIO registrations, hardware disks, and disk
group ownership. `prepare_large_write` starts bounded chunk preparation when
the object size and `LargeWritePolicy` become known. The single-use
`PreparedLargeWrite::write_stream` consumes an `AsyncRead`.
`PreparedLargeWrite::write_buffers` transfers caller-owned `Bytes` blocks
directly into the same writer, bypassing fetch and its input copy. Both return
`LargeWriteResult` with locations, logical and EC-expanded physical bytes,
chunk and strip counts, elapsed time, and preparation stalls.

`prepare_large_writes` starts several single-use sessions concurrently and
waits until each owns its first chunk. A server calls it before opening its load
gate, keeps the returned queue, and starts one replacement session whenever it
consumes one. Queue depth is an application admission setting; the benchmark
defaults to ten through `--prefetch-chunks`. Unused sessions are explicitly
aborted so their Active chunks are deleted.

The write and read hot paths read an immutable disk-ID route snapshot through
`ArcSwap`. Every endpoint owns a fixed connection pool configured by
`ChunkIoClientConfig::diskio_connections_per_endpoint`; an atomic counter
selects the next connection without a route lock. Refresh constructs complete
replacement pools off-path and publishes them atomically. Missing ownership
and duplicate owners are topology errors; the client never chooses an
arbitrary DiskIO endpoint. The application and CLI do not construct
allocators, RPC servers, connections, chunks, strips, or parity workers.

Topology refresh follows server ownership. `refresh_chunkdb_routes` refreshes
ChunkDB endpoints and range bindings; `refresh_diskio_routes` refreshes
DiskIO service and disk-owner routes. Metrics wrappers only observe the two
narrow seams and do not own scheduling or discovery.

## 11. Performance Workload

Chunk IO performance workloads live in `crowdb-chunk-client`; CLI commands
only map arguments, start the standard process metrics collector, and format
results.

- `run_large_write_benchmark` retains the deterministic bounded-source writer.
  Before its timer starts, it prepares write sessions and distributes them
  across workers. Each worker starts a replacement allocation when consuming a
  session.
- `run_small_write_benchmark` uses `prepare_small_write`, awaits each real
  Location, drains the shared pool, and reports batch fill, queue delay,
  active/draining pipeline gauges, queue-driven scale-out/scale-in, throughput,
  latency, and exact object accounting.
- `run_read_benchmark` prepares a bounded reusable dataset through the real
  small- and/or large-write APIs before timing. Small, large, and deterministic
  mixed request modes call `read_object`; mixed ratio is by request count.
  Successful reads validate logical length. Under NullDisk they deliberately
  do not compare payload contents.

The CLI commands are `bench chunkio write`, `write-small`, `read-small`,
`read-large`, and `read-mix`. Results separate preparation from timed load and
include aggregate throughput, latency, errors, stop reason, and exact admitted
versus completed accounting.

The CLI workload admits objects until a shared duration deadline (20 seconds by
default). A worker checks the deadline before admitting its next object; an
object already admitted is allowed to finish, including all DiskIO completions
and chunk seal, before the worker exits. Reported elapsed time includes this
drain tail. The object count remains a safety cap and a deterministic test mode,
not the normal regression stopping condition.

The regression fixture starts three co-located logical nodes in one rack:
three KV servers, three DiskDB, three ChunkDB, and three DiskIO processes
backed by `NullDisk`. KV state and WAL use `mem-block` with fsync disabled, so
benchmark metadata also avoids real media. Read preparation still writes real
ChunkDB/DiskDB/KV metadata and writer-produced Locations; only payload storage
is synthetic. An 8+4 strip in this intentionally compact local topology
requires the local-test-only unsafe EC placement option; disk ownership and
routing remain strict. Unsafe placement still balances blocks across the
available nodes within a rack; it relaxes the failure-domain limit without
concentrating the strip on the first node. The retained logs
contain `bw_read_mib` and `bw_write_mib` when host PMU counters are available.
Their sum is observed host memory traffic during the workload, not physical DIMM peak bandwidth and not
an application-byte estimate. Loopback TCP, EC expansion, RPC framing, kernel
copies, EC calculation, synchronous writes, and metadata work all keep end-to-end logical
throughput below that hardware envelope.

### 11.1 Intel Core i9-7960X Baseline

Reference run 2026-09-08: Intel Core i9-7960X (16c/32t, Skylake-X), 4 DDR4
channels at 2667 MT/s (~85 GB/s peak), Linux 6.11, `perf_event_paranoid=-1`.

The sentinel uses 16 MiB objects, EC 8+4, 1 MiB blocks, ten prefetched chunks,
and two strips prefetched per active chunk. Each worker creates one deterministic
random 1 MiB source block and reuses it. Stream cases traverse the production
`AsyncRead`; direct cases pass reference-counted `Bytes` blocks to the same
writer without a fetch copy. Both traverse EC, routed RPC, the DiskIO handler,
and real io_uring submission. RPC sends every 1 MiB data or parity payload through
the loopback socket. The storage target is the only substitution: NullDisk
replaces production BlockDisk and does not request stable-media durability.

| Mode           | Writers | Logical MiB/s | Physical MiB/s |      p50 / p99 ms | DRAM read avg | DRAM write avg | DRAM total avg |
| -------------- | ------: | ------------: | -------------: | ----------------: | ------------: | -------------: | -------------: |
| Stream         |       1 |         162.5 |          243.8 |  99.191 / 111.225 |       2,496.6 |        1,393.0 |        3,889.6 |
| Direct buffers |       1 |         229.1 |          343.7 |   69.191 / 85.641 |       1,996.1 |        1,228.7 |        3,224.8 |
| Stream         |       4 |       1,965.6 |        2,948.5 |   31.917 / 51.806 |      12,699.7 |       10,943.0 |       23,642.7 |
| Direct buffers |       4 |       2,672.1 |        4,008.1 |   22.811 / 40.000 |      11,799.6 |       12,051.5 |       23,851.1 |
| Stream         |      32 |       3,249.3 |        4,873.9 | 151.716 / 267.992 |      19,562.5 |       15,603.7 |       35,166.2 |
| Direct buffers |      32 |       3,547.9 |        5,321.9 | 138.654 / 261.388 |      14,816.5 |       12,766.6 |       27,583.1 |

Per-object client-stage time is measured inside the production writer. Times
overlap and therefore must not be added to predict object latency.

| Flow step             | Stream 1 | Direct 1 | Stream 4 | Direct 4 | Stream 32 | Direct 32 |
| --------------------- | -------: | -------: | -------: | -------: | --------: | --------: |
| Source/read copy      |   20.247 |        0 |    3.737 |        0 |     8.704 |         0 |
| Fetch assembly copy   |        0 |        0 |        0 |        0 |         0 |         0 |
| EC encode             |   20.175 |   13.164 |   15.213 |   12.357 |    22.045 |    20.222 |
| Write-completion wait |   53.990 |   54.238 |   11.741 |   10.118 |   122.786 |   120.972 |

All values are milliseconds per object, aggregated across concurrent writers;
stages overlap and must not be summed into latency. Every object has two data
strips, 16 data blocks, eight parity blocks, and therefore 24 full 1 MiB DiskIO
RPCs. This is exactly 16 MiB logical and 24 MiB physical traffic: EC 8+4 gives
the expected 1.5 physical/logical ratio. The run recorded zero preparation
stalls and zero `append_chunk` calls: initial chunk allocation returned both
strips requested by `prefetch_strips_per_chunk=2`.

Stream mode has one application payload copy per block: socket/source into its
final owned block. Direct-buffer mode has zero application payload copies;
`Bytes` clones, channels, EC input, routing, and RPC buffer wrapping transfer
ownership only. Both modes still copy or DMA bytes across the loopback transport
and DiskIO boundary. EC reads 16 MiB and creates 8 MiB parity per object, while
the RPC path transports 24 MiB, so physical memory traffic must be multiple
times frontend bandwidth. The multiplex-corrected host read/write counters are reported above,
but is not a complete byte ledger and can be below RPC bandwidth because of its
sampling scope and interval.

### 11.2 AMD Ryzen 9 5950X Baseline

Reference run: _pending._

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

### 11.3 Bottleneck Conclusion

Independent blocks are already parallel: each of the eight data writes is
spawned as soon as its block enters `EcStripWriter`, then four parity writes are
spawned together after EC completion. `ChunkWriter` groups the 12 handles per
strip, permits `parity_depth` strip groups in flight, and joins them before
seal. There is no serial per-block completion wait.

Chunk preparation is also off the measured hot path. The remaining single-write
gap is input ownership plus cache pressure: direct buffers reach 229.1 MiB/s,
1.41 times the stream result, and spend no time in source reads. Four direct
writers peak at 2,672.1 logical MiB/s. At 32 writers throughput rises to 3,547.9
MiB/s but completion wait rises to 121 ms/object. The loopback RPC/DiskIO queue
is saturated well before 32 writers; more concurrency increases latency rather
than bandwidth. Further work should profile and reduce RPC/kernel transport cost
and EC/cache contention. Increasing prefetch or adding wrapper layers will not
address the measured bottleneck.
