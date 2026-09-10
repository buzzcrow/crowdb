<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk Stream (R141)

This implementation design refines
[`../backlog/R141-chunk-stream.md`](../backlog/R141-chunk-stream.md) and composes
the existing chunk writer, chunk reader, and group-0 metadata contracts without
putting record framing or partition routing into the stream layer.

## 1. Ownership and Public API

`crowdb-chunk-stream` is an async Rust library. A `StreamName` is an opaque,
stable byte identifier owned by one logical partition. `ChunkStream::create`
and `ChunkStream::open` resolve its injected registry binding and return one
handle for a caller-supplied writer epoch. The handle exposes `tail`, vectored
`append`, `read_at`, bounded `read_from`, monotonic `trim_prefix`, and `close`.

Each open writable handle owns one Tokio task and one bounded MPSC queue.
Producers reserve request and byte admission before enqueueing. The task is the
only mutable owner of the active chunk, manifest generation, extent builder,
tail, trim offset, rollover state, and ambiguous-write recovery. Reads use
immutable published metadata and do not acquire the writer state.

## 2. Injected Boundaries

Three async traits keep cluster coordination out of the core state machine:

- `StreamRegistry` stores only `StreamBinding {stream_name,
  metadata_group_id, binding_generation, state, owner_kind}`. Production uses
  group 0; tests use an in-memory implementation.
- `StreamMetadataStore` stores immutable manifests and extent pages under one
  nonzero KV group and publishes by expected generation plus writer epoch.
- `StreamChunkStore` allocates three-way mirrored WAL chunks, writes one
  contiguous range to every mirror, advances/queries the acknowledged cursor,
  seals chunks, reads ranges, and releases complete strips.

The traits return `StreamError`, whose variants distinguish invalid requests,
backpressure, stale writers, definitely absent appends, ambiguous resolution,
write-stalled state, unavailable reads, corruption, and internal failures.

## 3. Durable Metadata

`StreamManifest` contains the name, metadata group, writer epoch, generation,
trim offset, sealed tail, optional active chunk descriptor, ordered extent-page
fences, and the retained predecessor generation. Immutable metadata keys are
scoped by `(stream_name, writer_epoch, generation)`.

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
once into retained staging storage. The chunk store writes that range to all
three mirrors, then advances the epoch-fenced acknowledged cursor once. Only
after the cursor is durable are all request futures completed. A definitely
failed write completes every batch member with failure and faults no later
range. An ambiguous cursor result is resolved by querying the durable cursor
and checksum; bytes are never resubmitted at a guessed offset. An unresolved
outcome stalls later writes until reopen.

An append never straddles chunks. The worker prepares one successor, seals the
old chunk, adds its extent, writes the affected immutable extent page, and
publishes a new manifest before writing the request to the successor. Prepared
but unpublished chunks and metadata are orphan work.

## 5. Reads and Trim

Reads reject offsets below `trim_offset` and beyond the durable tail. They
binary-search manifest fences and page offsets, coalesce adjacent physical
ranges in one chunk, and submit at most `read_window_bytes`. Results remain in
logical order even when storage completes out of order. Active reads are capped
by the acknowledged cursor returned with the manifest view.

`trim_prefix(g)` rejects regression and values above the durable tail. It first
publishes a manifest containing the new logical trim point, then performs a
bounded idempotent cleanup pass. Only complete strips whose logical end is at
or below `g` are releasable; a boundary strip remains intact.

## 6. Recovery, Fencing, and Retention

Open selects the highest complete generation authorized for the requested
writer epoch, validates all page fences and arrays, queries the active durable
cursor, and derives the tail with checked arithmetic. A higher epoch may adopt
the same stream; a lower epoch cannot advance cursors or publish metadata.
Current and predecessor manifests remain retained until the metadata store's
watermark permits cleanup.

Every in-flight append batch and read window has a diagnostic watchdog. It
records age, queue delay, logical range, request/byte count, and current stage
without cancelling, retrying, or completing the operation.

## 7. Bounds and Metrics

Initial configurable bounds are 1,024 queued requests, 16 MiB queued bytes, 64
requests or 1 MiB per batch, 64 MiB active chunks, 256 extents per page, an 8
MiB read window, one prepared successor, two retained manifests, and 64 MiB of
GC per pass. Metrics cover admission, queueing, batches, logical/physical bytes,
mirror failures, sync/append latency, watchdog observations, rollover, lookup,
read windows, replay, stale writers, recovered tails, trim lag, reclaimed
bytes, and orphans.

## Open Questions

- Production registry and metadata adapters need the protocol key/value schema
  and the R143 binding lifecycle; initial library tests use injected stores.
- Production active-chunk writing needs a mirrored writer over the existing
  chunkdb cursor and DiskIO seams; the current object writer is EC-oriented and
  its mirror writer remains a placeholder.
- Benchmark-derived defaults and production watchdog intervals remain open
  until fixed workloads run on production-equivalent hardware.
- Metadata-group rebinding, per-stream metadata sharding, and EC conversion are
  deliberately deferred follow-up work.
