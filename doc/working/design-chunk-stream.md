<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk Stream (R141)

This implementation design refines
[`../backlog/R141-chunk-stream.md`](../backlog/R141-chunk-stream.md) and composes
the existing chunk writer, chunk reader, and group-0 metadata contracts without
making the stream interpret record framing or partition routing.

## 1. Ownership and Public API

`crowdb-chunk-stream` is an async Rust library. A `StreamName` is an opaque,
stable 128-bit identifier owned by one logical partition. New names use a
time-ordered layout and render as 32 hexadecimal digits, matching `ChunkId`
display without sharing its type. `ChunkStream::create`
and `ChunkStream::open` resolve its injected registry binding and return one
handle for a caller-supplied writer epoch. The handle exposes `tail`, vectored
`append`, `append_chunk_bound`, `read_at`, bounded `read_from`, monotonic
`trim_prefix`, and `close`. Nonempty appends return `AppendResult {stream_name,
chunk_id: Some(_), begin, end}`. A zero-length ordinary append performs no IO
and returns `chunk_id: None`. `append_chunk_bound` accepts body+CRC bytes and
makes the worker append the selected chunk's canonical 128-bit ID; its returned
range includes that trailer.

Each open writable handle owns one Tokio task and one bounded MPSC queue.
Producers reserve request and byte admission before enqueueing. The task is the
only mutable owner of the active chunk, manifest generation, extent builder,
tail, trim offset, rollover state, and ambiguous-write recovery. Reads use
immutable published metadata and do not acquire the writer state.

## 2. Injected Boundaries

Three async traits keep cluster coordination out of the core state machine:

- `StreamRegistry` stores only `StreamBinding {stream_name,
  metadata_group_id, binding_generation, state, owner_kind}`. Production uses
  the readable text-key namespace in group 0; tests use an in-memory
  implementation. Creation accepts an explicit metadata group and defaults to
  group 1. The chunk-KV server creates that group and drives binding activation
  through the R143 lifecycle API.
- `StreamMetadataStore` stores one stable CAS-protected manifest head and
  immutable versioned extent pages under one nonzero KV group. Publication
  compares the head's KV revision and validates a monotonic writer epoch.
- `StreamChunkStore` allocates three-way mirrored WAL chunks, writes one
  contiguous range to every mirror, advances/queries the acknowledged cursor,
  seals chunks, reads ranges, and releases complete strips.

The traits return `StreamError`, whose variants distinguish invalid requests,
backpressure, stale writers, definitely absent appends, ambiguous resolution,
write-stalled state, unavailable reads, corruption, and internal failures.

## 3. Durable Metadata

`StreamManifest` contains the name, metadata group, writer epoch, generation,
trim offset, sealed tail, optional active chunk descriptor, and ordered
extent-page fences. The manifest uses one stable head key protected by R101
compare-and-set on its KV revision. Every changed tail page is written under a
fresh versioned COW key, and every published extent page is immutable.

`StreamExtentPage` stores parallel arrays `chunk_ids[N]`,
`logical_offsets[N+1]`, and `physical_offsets[N]`. Validation requires equal
array geometry, checked arithmetic, strictly advancing logical offsets, and
gap-free coverage. Page fences are `[first_logical, end_logical)` and permit a
binary search before loading one page. The open active chunk is outside these
arrays; its logical tail is derived from its acknowledged physical cursor.

## 4. Append and Rollover

An idle worker immediately receives and submits the first request. Before
starting IO it drains only requests already queued, stopping at request count,
byte budget, and remaining chunk capacity. Zero-length requests complete at the
current tail without IO. An oversized request is rejected before allocation.

The batch receives consecutive logical subranges in queue order and is copied
once into retained staging storage. For each chunk-bound request the worker
adds the selected `ChunkId` after the caller's body+CRC; capacity and logical
ranges include those bytes. The chunk store writes the aggregate range to all
three mirrors, then advances the epoch-fenced acknowledged cursor once. Only
after the cursor is durable are all request futures completed. A definitely
failed write completes every batch member with failure and faults no later
range. An ambiguous cursor result is resolved by querying the durable cursor
and checksum; bytes are never resubmitted at a guessed offset. An unresolved
outcome stalls later writes until reopen.

An append never straddles chunks. The active chunk has a hard 256-MiB logical
capacity. The worker prepares one successor, seals the old chunk, adds its
extent, writes a new immutable version of the affected extent page, and
CAS-publishes the new head before writing the request to it. Prepared but
unpublished chunks and metadata are orphan work.

Production uses a new `MirrorChunkWriter` in `crowdb-chunk-client`. It accepts
owned `Bytes` buffers directly, appends mirror strips asynchronously, advances
the fenced cursor, and never starts the EC pipeline. It owns exactly one chunk;
stream rollover remains in `crowdb-chunk-stream`. EC conversion is disabled by
default and belongs to the deferred scale-out requirement.

## 5. Reads and Trim

Reads reject offsets below `trim_offset` and beyond the durable tail.
`ChunkStream::reader(offset, hint)` creates a seekable sequential reader;
`ReadHint` is either a finite byte count or `ToEnd`. The reader returns EOF at
the captured durable end and leaves EOF handling to its caller. It resolves
only metadata covering the seek target, serves cached bytes while prefetching
later ranges, and retains at most `read_window_bytes` (8 MiB by default, within
the 4-8 MiB target). Independent readers may issue bounded physical reads
concurrently, but each reader emits bytes in logical order. Active reads are
capped by an acknowledged-cursor snapshot. The provenance-aware reader form
also yields the physical `chunk_id` for each logical segment. R142 uses it to
compare a frame's chunk trailer with the chunk that supplied those bytes.

`trim_prefix(g)` rejects regression and values above the durable tail. It first
publishes a manifest containing the new logical trim point, then performs a
bounded idempotent cleanup pass. Only complete strips whose logical end is at
or below `g` are releasable; a boundary strip remains intact.

## 6. Recovery, Fencing, and Retention

Open loads the CAS-current stream head and only the metadata needed for its
mode. A reader resolves its seek target without scanning page bodies before
that target. A writer loads the last extent page and never resumes a chunk from
a prior process: it seals the prior active chunk at its acknowledged cursor,
then allocates a fresh 256-MiB mirror chunk. Before initial publication and
every CAS retry, it rejects an observed head epoch above its local epoch; only
a higher local epoch holding R142 ownership authority may adopt an older head.
Thus the ownership epoch cannot regress even if a stale writer learns a newer
KV revision. Full extent pages are immutable, short extents left by restart
are retained, and trim GC removes them without an extent-merge pass.

Every in-flight append batch and read window has a diagnostic watchdog. It
records age, queue delay, logical range, request/byte count, and current stage
without cancelling, retrying, or completing the operation.

## 7. Bounds and Metrics

Initial configurable bounds are 1,024 queued requests, 16 MiB queued bytes, 64
requests or 1 MiB per batch, 256 MiB active chunks, 1,024 extents per page, an
8 MiB read window, one prepared successor, one current CAS head, and 64 MiB of
GC per pass. Metrics cover admission, queueing, batches,
logical/physical bytes, mirror failures, sync/append latency, watchdog
observations, rollover, lookup, read windows, replay, stale writers, recovered
tails, trim lag, reclaimed bytes, and orphans.

## 8. Orphan Ownership and Benchmarks

Chunk-stream orphan state has three distinct sources. An allocated chunk that
crashes before its extent entry is published is empty and is handled by R146's
expired-writer scan. Published chunks removed by trim are normal idempotent GC.
Unpublished or superseded metadata records remain metadata-store GC and require
a retained-head watermark; R146 cannot infer their reachability.

Every stream chunk allocation carries a stream-specific chunk type and an
owner key containing the owner-kind prefix plus `StreamName`. R146 owns the
compatible chunk-record extension, lease expiry, sealing, and deletion of
zero-length chunks. R141 supplies the stream identity on allocation and reports
unreachable metadata separately.

Benchmarks use the existing NullDisk-backed ChunkDB/DiskIO harness. Fixed
workloads cover multiple concurrent writers, multiple concurrent readers,
single-writer queue saturation, rollover, random seek, sequential replay, and
bounded prefetch. The write executor remains one ordered task per stream;
parallelism comes from independent streams and bounded read IO, not concurrent
mutation of one stream tail.
