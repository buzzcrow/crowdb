<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# ChunkIO Write Path Review (R135)

This temporary review follows
[`R135`](../backlog/R135-chunkio-end-to-end-performance.md), the
[`ChunkIO design`](../design/chunkio/design-crowdb-chunkio.md), and the
[`DiskIO design`](../design/diskio/design-crowdb-diskio.md). The functional
write-flow work and regression benchmark have landed. The enhancement is now
implemented and remeasured. This document records the code-ownership review,
root cause, implemented changes, clean benchmark result, and remaining
production-device measurement.

## 1. Status

`doc/working/plan-chunkio-write-performance.md` is complete. The accepted
behavior is folded into the permanent ChunkIO, ChunkDB, protocol, and DiskIO
designs. This working document is retained for user review.

Formatting and Rust lint pass. The full ordered local CI passes the C++, Rust,
frontend unit, CLI, server, and storage/client suites. Its final Playwright
stage reports seven console capacity failures whose fixtures attempt to create
a disk group without a registered live DiskDB. Those failures are outside this
change set and do not exercise ChunkIO.

## 2. Current Ownership

The intended ownership boundaries are sound at the top level:

- `ChunkIoClient` is the application boundary. It discovers ChunkDB and DiskIO,
  constructs write sessions, and exposes topology refresh.
- `PreparedLargeWrite` is a single-use application session. It starts chunk
  preparation when object metadata becomes available and returns durable
  locations plus per-object accounting.
- `LargeAsyncObjectWriter` owns the object stream, chunk rotation, completed
  locations, and object-level cleanup.
- `ChunkWriter` owns one chunk and progresses through its strips.
- `EcStripWriter` owns EC state and the writes for one strip.
- `ChunkPrefetch` prepares future chunks.
- `ChunkAllocator` is the narrow ChunkDB lifecycle seam.
- `DiskWriter` is the narrow DiskIO seam and disk-owner routing boundary.

`ChunkAllocator` and `DiskWriter` are justified interfaces. They separate two
external services and allow focused tests without recreating production
protocol or transport objects. `PreparedLargeWrite` is also justified because
preparation has a real lifetime distinct from stream execution.

The former `MetricsChunkAllocator` and `MetricsDiskWriter` decorators were
removed. Metrics are recorded at the existing orchestration boundaries, so
the public facade no longer grows wrapper types solely for instrumentation.

## 3. Responsibility Problems

### 3.1 `ChunkWriter` owns scheduling as well as chunk state

`ChunkWriter` currently owns chunk state, strip progression, strip allocation,
an atomic cursor, a polling prefetch task, parity task scheduling, durability
completion, sealing, abort cleanup, and preparation metrics. This makes a
single type responsible for both the chunk state machine and two asynchronous
schedulers.

Keep `ChunkWriter` responsible for ordered strip progression and chunk
completion. Replace its polling strip-prefetch subsystem with a bounded set of
append operations driven directly by strip consumption. Opening a strip should
free one preparation permit; no timer or shared atomic cursor is needed.

### 3.2 Strip preparation transfers cumulative chunks

Every `append_chunk` response contains the full cumulative `Chunk`. The
prefetch channel therefore transfers progressively larger metadata values, and
old `Arc<Chunk>` snapshots remain alive while strip tasks use them. This grows
poorly as a chunk approaches its 1 GiB limit.

The normal append response returns only the newly allocated strips plus brief
parent identity/version information. The request carries the parent version
observed by the client. The response indicates whether that version is still
current. When unrelated parent metadata changed, the response either includes
the refreshed parent fields or explicitly requires a full chunk query. It must
not return the cumulative strip list on every normal append. Do not introduce
a parallel hierarchy of wrapper types for ChunkDB protocol objects.

### 3.3 Chunk-prefetch controls overlap

`ChunkClientConfig` has `prealloc_depth`, `chunk_prefetch_depth`, and
`prefetch_chunk_count`. The chunk-prefetch channel uses `prealloc_depth`, even
though that field describes strips, and `chunk_prefetch_depth` appears unused.
Unknown-size writes preallocate an initial fixed count and then fall back to
on-demand allocation instead of maintaining a lead.

Use two controls with one meaning each:

- `strip_preparation_depth`: prepared strips ahead within the current chunk.
- `chunk_preparation_depth`: prepared chunks ahead of object rotation.

Known- and unknown-size streams should both maintain the same bounded lead.

### 3.4 Topology refresh follows server boundaries

`ChunkIoClient` exposes `refresh_chunkdb_routes` and
`refresh_diskio_routes`. The former refreshes ChunkDB endpoints and range
bindings; the latter atomically republishes DiskIO service and disk ownership
routes. Each server-specific client owns its discovery details and failure
domain.

### 3.5 Public surface is too broad

The crate re-exports low-level chunk, strip, parity, worker, routing, and writer
types. Application callers should normally need `ChunkIoClient`, policy and
result types, and read/write session APIs. Keep low-level seams public only
where external embedders or integration tests require them; keep orchestration
implementation types crate-private.

## 4. Measured Bottleneck

The retained three-node `NullDisk`, EC 4+1 benchmark produced:

- One writer: 22.0 MiB/s logical and 27.6 MiB/s physical.
- Four writers: 129.6 MiB/s logical and 162.0 MiB/s physical.
- ChunkDB append latency: approximately 2-5 ms.
- DiskIO 1 MiB write latency: approximately 28-49 ms.
- DiskIO fsync latency: approximately 19-33 ms.

`EcStripWriter::push` awaits each 1 MiB data write before feeding EC and
accepting the next block. A 28-49 ms serialized operation limits one writer to
roughly 20-35 MiB/s. The observed 22 MiB/s falls directly in that range. Four
writers create four-way concurrency and reach the expected aggregate range.

The client serialization explains how DiskIO latency becomes the throughput
limit, but it does not explain why a NullDisk operation takes tens of
milliseconds. The first code-level divergence is in `DiskIOUring`:

1. `submit_lockfree` fills an SQE and sets `pending_submit`, but a normal
   successful enqueue returns without waking its poll thread.
2. The poll thread publishes pending SQEs only at the top of its loop.
3. After its small hybrid busy-poll budget is exhausted, that thread sleeps in
   `epoll_wait(..., 50 ms)`.
4. The submission eventfd is written when the queue is full, during shutdown,
   or after completions are dispatched, but not for the common transition from
   an idle queue to one pending request.
5. A request arriving after the poll thread sleeps can therefore wait almost
   50 ms before its SQE is published to the kernel. The expected average idle
   penalty is about 25 ms, which matches the measured 28-49 ms write latency.

This missing idle-to-active wakeup is the primary root cause of the low
NullDisk result. Serial data submission in `EcStripWriter` amplifies the defect
into a per-writer throughput ceiling. ChunkDB allocation, EC compute, source
buffering, and memory bandwidth do not explain the retained result.

The server-side RPC metrics support this attribution: request parsing is
normally tens to hundreds of microseconds and response `writev` is normally
tens of microseconds. Those stages are orders of magnitude below the client
write latency and cannot account for a repeated delay near 50 ms.

### 4.1 Post-enhancement result

The retained rerun at `/tmp/crowdb-chunkio-write-enhanced-3` completed with
zero errors, complete service metrics, and no unregistered-descriptor or
`DiskNotExist` warnings:

- One writer: 134.5 MiB/s logical, 168.1 MiB/s physical, and 89,401 us
  reported p50 object latency.
- Four writers: 1,818.0 MiB/s logical, 2,272.5 MiB/s physical, 137,833 us p50,
  and 143,356 us p99 object latency.
- Against the original retained 22.0/129.6 MiB/s logical measurements, this is
  approximately 6.1x at one writer and 14.0x at four writers.
- The four-writer client measured 6.4 ms average per 1 MiB DiskIO completion;
  RPC `read_to_parse` and `writev` averaged 0.9 ms and 1.0 ms respectively.
  The former repeated 28-49 ms idle delay is absent.

The one-writer case includes startup convergence rather than steady write
cost: its two chunk allocations averaged 383 ms with a 1 s histogram maximum,
and preparation stalls totaled 766 ms. ChunkDB range bindings can become
visible to clients before the selected server's one-second range refresh.
`NotMyRange` retry now covers that interval. This latency should be separated
from steady-state write throughput in a future longer-running case.

The remaining four-writer NullDisk limit is no longer ChunkDB allocation or a
serialized client write. It is the aggregate in-memory RPC/io_uring path under
high concurrency: client CPU was about 203% user plus 323% system, RPC stages
rose toward 1 ms, and DiskIO completion averaged 6.4 ms while carrying 2.27
GiB/s physical payload. NullDisk is explicitly non-durable, so this result is
a scheduling/transport ceiling, not a claim about production block-device
durability throughput. A production-device benchmark is the next measurement
needed before further write-path optimization.

## 5. Benchmark Validity Defect

The DiskIO logs repeatedly report that NullDisk file descriptors are not
registered and are being routed to pipeline 0. `NullDisk` owns a valid memfd,
but `build_disk_set` registers only real `BlockDisk` descriptors with
`DiskIOUring`.

The current engine has one pipeline, so fallback routing selects the same
pipeline. However, each write and fsync emits a warning. Per-operation logging
contaminates latency and makes this an invalid fast-path baseline. This is
separate from the missing poll-thread wakeup and is not sufficient by itself to
explain the repeatable latency near the 50 ms wait bound.

Before optimizing the Rust client:

1. Wake the owning poll thread when a submission changes a pipeline from idle
   to pending, without issuing one wake syscall for every request in a burst.
2. Add focused idle, sustained-load, and wake-coalescing latency tests for
   `DiskIOUring`.
3. Register every valid descriptor used by `UringEngine`, including NullDisk
   and MemDisk descriptors where applicable.
4. Assert in the regression fixture that no unregistered-descriptor warning is
   emitted.
5. Rerun the same one- and four-writer cases and retain the service metrics.
6. Use the clean result to decide how much remaining latency belongs to RPC
   transport, the DiskIO handler, io_uring, memfd I/O, and synchronous write.

## 6. Implemented Write Scheduling

The enhanced path uses the existing domain owners without a new wrapper layer.

1. `LargeAsyncObjectWriter` reads bounded blocks and passes them to the current
   `ChunkWriter`.
2. `ChunkWriter` selects the current strip and enforces a per-writer completion
   bound.
3. `EcStripWriter` feeds the block into EC and submits its synchronous data
   write without waiting for unrelated data shards to complete.
4. The strip retains bounded data-write completions. At strip completion it
   submits synchronous parity writes after parity calculation.
5. Chunk sealing waits for every data and parity write required by the chunk,
   then persists the ChunkDB seal. It does not issue a second fsync phase.
6. Abort stops new submissions, drains already submitted DiskIO work before
   freeing storage, and deletes unsealed chunks.

This requires an explicit asynchronous submission/completion contract at the
`DiskWriter` boundary. The contract should remain small: submit one durable
write, then await its completion. Concurrency belongs to `ChunkWriter` and
`EcStripWriter`; it should not be hidden inside metrics or routing decorators.

## 7. Durability Decision

Each 1 MiB block is written by one synchronous write operation. Production
storage uses direct I/O and the write completion is the durability boundary;
the chunk client does not split a block write from a later fsync operation.
Parity blocks use the same contract. ChunkDB seal is persisted only after all
data and parity write completions have succeeded.

`O_DIRECT` alone controls page-cache behavior and does not universally provide
stable-media completion semantics. The DiskIO implementation must pair direct
I/O with an explicit synchronous-write mechanism supported by the selected
platform, such as `O_DSYNC` or per-operation `RWF_DSYNC`, so the API contract
matches the intended durability. This mechanism belongs in DiskIO, not in the
chunk client.

File-backed simulation follows the same one-write API. Tests may explicitly
disable the real sync flag to isolate scheduling and transport performance,
but the result must be labeled non-durable. The client must not send separate
per-strip or per-chunk fsync RPCs in either mode.

## 8. Failure Behavior

- A data or parity completion failure fails the object and prevents sealing.
- A preparation failure is returned when the corresponding strip or chunk is
  required; successfully prepared resources are cleaned up during abort.
- A declared object size that exceeds actual source length must not leak
  prefetched chunks.
- A declared object size smaller than the stream must either fail explicitly
  or continue with bounded on-demand preparation; behavior must be documented.
- A topology miss should refresh the owning service's route once and retry only
  when the operation is safe to retry.
- Submitted DiskIO writes are not treated as cancelled until completion is
  observed; storage is not freed for reuse before those completions drain.

## 9. Scope

- `lib/crowdb-chunk-client/src/client.rs`: narrow the facade, complete topology
  ownership, and relocate metrics decorators if accepted.
- `lib/crowdb-chunk-client/src/config.rs`: consolidate preparation controls.
- `lib/crowdb-chunk-client/src/chunk/chunk_prefetch.rs`: maintain bounded chunk
  lead for known- and unknown-size streams.
- `lib/crowdb-chunk-client/src/chunk/chunk_writer.rs`: remove polling and own
  bounded strip/write completion scheduling.
- `lib/crowdb-chunk-client/src/chunk/ec_strip_writer.rs`: separate EC feed and
  DiskIO submission from completion waits.
- `lib/crowdb-chunk-client/src/chunk/parity_writer.rs`: use the same durable
  write completion contract for parity and remove separate fsync scheduling.
- `lib/crowdb-chunk-client/src/disk_io/`: keep one small DiskIO seam and
  lock-free production routing; split refresh operations by server boundary.
- `lib/crowdb-protocol/` and ChunkDB client/server lifecycle code: make append
  responses return new strips plus brief parent version/change information.
- `app/crowdb-diskio/src/dio_main.cpp`: configure synchronous direct writes and
  register valid dummy-disk descriptors.
- `lib/crowdb-common/cpp/src/diskio_uring.cpp`: wake an idle poll thread when
  work becomes pending, with wake coalescing.
- `tools/bench-chunkio-write-regression.sh`: reject contaminated DiskIO runs
  and retain clean comparison results.
- Chunk-client unit and E2E tests: cover bounds, completion ordering, cleanup,
  topology refresh, and mismatched size hints.

## 10. Complexity

High. The individual changes are small, but they change asynchronous ownership
and durability ordering. The implementation must preserve bounded memory,
avoid locks on the hot path, drain submitted I/O safely, and distinguish
metadata preparation latency from DiskIO completion latency.

## 11. Test Design

1. Idle submission wakeup: allow the hybrid poll thread to sleep -> submit one
   operation -> assert its SQE is published without waiting for the 50 ms
   timeout. Invariant: idle-to-active transition explicitly wakes the owner.
2. Wake coalescing: submit a burst while the poll thread is active -> assert
   all completions arrive and wake writes do not scale one-for-one with
   requests. Invariant: the fix does not replace latency with wake overhead.
3. NullDisk registration: start DiskIO with NullDisk -> issue non-durable test
   writes -> assert success and no unregistered-descriptor warning. Invariant:
   every valid descriptor submitted to `UringEngine` has a route.
4. Concurrent data submission: delay each mock DiskIO completion -> write one
   full EC strip -> assert more than one data write becomes in flight while the
   configured bound is never exceeded. Invariant: one slow shard does not
   serialize independent shard submission.
5. Completion ordering: delay data and parity writes -> finish a chunk ->
   assert seal starts only after every synchronous write completes and no
   separate fsync is submitted. Invariant: durable metadata never precedes
   durable data.
6. Strip preparation bound: delay ChunkDB append -> consume multiple strips ->
   assert outstanding and buffered appends stay within
   `strip_preparation_depth`. Invariant: no polling task or unbounded metadata
   accumulation.
7. Unknown-size chunk lead: stream across several small chunks -> assert the
   allocator continuously maintains at most `chunk_preparation_depth` chunks
   ahead. Invariant: unknown-size operation does not become permanently
   on-demand after its initial prefetch.
8. Short source cleanup: declare a multi-chunk size and end the source early ->
   assert unused allocated chunks are deleted. Invariant: preparation cannot
   leak active chunks.
9. Topology refresh: change ChunkDB binding and DiskIO ownership independently
   -> invoke the corresponding refresh operation -> assert only that server's
   routes change. Invariant: refresh ownership and failure are server-specific.
10. Incremental append response: append without parent metadata change ->
    assert only new strips and the parent tag are returned; change parent
    metadata -> assert the response indicates refresh/full information is
    required. Invariant: normal append cost does not grow with chunk size.
11. Performance comparison: run the unchanged one- and four-writer regression
   before and after scheduling changes -> compare logical/physical bandwidth,
   object latency, DiskIO latency/inflight, wake latency, preparation stalls,
   CPU, and memory bandwidth. Invariant: a performance claim is backed by the
   full-path benchmark with zero errors and complete service metrics.

## 12. Module Structure

```text
lib/crowdb-chunk-client/src/
├── client.rs                 application facade and prepared session
├── config.rs                 object, strip, and chunk bounds
├── metrics.rs                client-path instrumentation
├── traits.rs                 ChunkDB lifecycle seam
├── disk_io/
│   ├── disk_writer.rs        durable write submission/completion seam
│   └── routing.rs            disk ID -> DiskIO endpoint snapshot
├── writer/
│   └── large_async_object.rs object stream and chunk rotation
└── chunk/
    ├── chunk_prefetch.rs     bounded future-chunk preparation
    ├── chunk_writer.rs       one-chunk state and completion ownership
    ├── ec_strip_writer.rs    one-strip EC and data submissions
    └── parity_writer.rs      parity durable-write submission helper
```

## 13. Resolved Decisions

### 13.1 Chunk modification revision

Each chunk carries an incrementing `modify_ts`. Despite the name, this is a
monotonic per-chunk modification revision rather than a wall-clock timestamp.
ChunkDB increments it whenever chunk lifecycle metadata, strip metadata, or
other client-visible chunk information changes, including append and alter
operations.

An append request contains the chunk ID and the client's observed `modify_ts`.
If it matches the current revision, ChunkDB returns only the newly appended
strips and the resulting revision. If it does not match, ChunkDB returns the
complete current chunk information. The client refreshes its state, checks the
changed fields, and retries only when the requested operation remains valid
and safe.

The first implementation uses one revision and a full response on mismatch.
Splitting a chunk into lifecycle, strip, and data components with independent
revisions or a modified-field mask could reduce refresh cost, but it adds
protocol and merge complexity and is deferred until measurements justify it.

### 13.2 Production and test durability

Production `BlockDisk` uses direct I/O with durable write completion. DiskIO
selects the platform-specific synchronous-write mechanism; the chunk client
observes only the durable completion contract and does not issue a separate
fsync.

Unit tests and simulated performance benchmarks do not require stable-media
durability. NullDisk, MemDisk, and file-backed test fixtures use explicitly
non-durable writes so durability latency does not obscure scheduling, RPC, EC,
or memory-path measurements. Benchmark output and configuration must identify
the mode as non-durable, and durability correctness is covered separately by
BlockDisk integration tests.
