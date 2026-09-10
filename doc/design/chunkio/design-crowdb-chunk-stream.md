<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Chunk Stream

The chunk stream presents finite mirrored chunks as one durable, ordered
logical byte space. It supplies the storage boundary used by the partition WAL
without owning WAL framing, request identity, or replay policy.

Depends on: [chunk IO](design-crowdb-chunkio.md),
[chunk reader](design-crowdb-chunkio-reader.md),
[chunkdb](../chunkdb/design-crowdb-chunkdb.md), and
[group-0 sysdata](../kv/design-crowdb-kv-group0.md).

## Contents

1. [Ownership and API](#1-ownership-and-api)
2. [Storage contracts](#2-storage-contracts)
3. [Metadata](#3-metadata)
4. [Append protocol](#4-append-protocol)
5. [Rollover and recovery](#5-rollover-and-recovery)
6. [Reads](#6-reads)
7. [Trim and reclamation](#7-trim-and-reclamation)
8. [Bounds and observation](#8-bounds-and-observation)
9. [Errors and invariants](#9-errors-and-invariants)
10. [Open Issues](#open-issues)

## 1. Ownership and API

`crowdb-chunk-stream` is an asynchronous Rust library. `StreamName` is an
opaque 128-bit identity bound to a logical partition, never to a process or
node. `ChunkStream` exposes `create`, `open`, `tail`, vectored `append`, exact
`read_at`, bounded sequential `read_from`, `trim_prefix`, and `close`.

Each writable handle owns one bounded Tokio MPSC queue and one worker task. The
task exclusively mutates the active descriptor, extent list, manifest
generation, tail, trim point, and rollover state. Producers and readers use
atomics plus immutable `ArcSwap` manifest snapshots; the hot path adds no lock.

The stream is an unframed byte container. The partition journal defines record
headers, checksums, request identities, replay behavior, and consumer
watermarks above this API.

## 2. Storage Contracts

Three injected async traits isolate the state machine from placement and
transport:

- `StreamRegistry` loads and creates the small group-0 binding. It is used only
  by create/open, never append or read.
- `StreamMetadataStore` loads immutable manifests and extent pages and
  publishes with an expected `(writer_epoch, generation)` comparison. `None`
  means the stream must not exist.
- `StreamChunkStore` allocates mirrored chunks, writes one contiguous range to
  every mirror, advances and inspects the fenced durable cursor, seals chunks,
  reads physical ranges, and idempotently releases trimmed chunks.

The `test-util` feature provides one deterministic in-memory implementation of
all three traits, including write suspension, cursor outcomes, and metadata
publication failure injection. Production adapters retain the same ownership
and durability contract.

## 3. Metadata

Group 0 stores only `StreamBinding {stream_name, metadata_group_id,
binding_generation, state, owner_kind}`. One nonzero KV group stores the stream
metadata under immutable keys scoped by stream name, writer epoch, and
generation.

`StreamManifest` contains the writer epoch, generation, previous generation,
logical trim point, sealed tail, optional active descriptor, page fences, and
closed state. `StreamExtentPage` contains parallel arrays:

- `chunk_ids[N]`
- `logical_offsets[N + 1]`
- `physical_offsets[N]`

Entry `i` maps `[logical_offsets[i], logical_offsets[i + 1])` to the same-sized
physical range beginning at `physical_offsets[i]`. Validation rejects bad
array lengths, empty or unordered extents, gaps between retained pages,
identity or fence mismatches, invalid active cursors, and arithmetic overflow.
After trim compaction, coverage may begin at or before `trim_offset`; it must
continue exactly through `sealed_tail`.

The open chunk stays outside the sealed extent pages. Its durable logical end
is `logical_start + acknowledged_cursor - physical_start`, using checked
arithmetic.

## 4. Append Protocol

Admission reserves a bounded request slot and queued bytes. A zero-length
append returns the current tail without allocation or IO. An append above the
configured maximum is rejected before enqueue.

An idle worker submits the first request immediately. It drains only requests
already queued and stops at the request limit, byte limit, or active chunk
capacity. It does not use a batching delay. Vectored request bytes are assembled
in queue order into one retained staging buffer and sent through one
`write_mirrors` call. The worker then performs one fenced cursor advance.

Completion occurs only after all mirror writes and the durable cursor update.
Each request receives its exact non-overlapping logical subrange. A failed
batch completes no member successfully and stalls subsequent writes until
reopen. An ambiguous cursor response is inspected without resubmission: the
worker accepts it only when the durable cursor equals the proposed end and the
last-advance checksum matches the staging checksum; an unchanged cursor proves
absence; every other state stalls.

## 5. Rollover and Recovery

One append never straddles chunks. If it cannot fit, the worker seals the old
chunk, appends its logical mapping to the extent builder, allocates a successor,
and publishes a new immutable manifest generation before writing the request.
Objects not reachable from a successfully published generation are orphans.

Open loads and validates every page in the authoritative generation, then
queries the active chunk's durable cursor. If the old active chunk was sealed
before an interrupted rollover publication, open promotes it into the sealed
extent list, allocates a successor, and publishes the repaired generation.
This makes both sides of the rollover publication crash boundary recoverable.

A higher ownership epoch adopts the same manifest and byte history into a new
generation. Querying the active cursor establishes the new chunk-store fence;
publishing compares against the exact prior epoch and generation. A lower
epoch is rejected, and a previously open lower-epoch writer can no longer
write or advance the cursor.

## 6. Reads

Reads reject ranges below `trim_offset` or beyond the current durable tail.
For sealed data, the reader binary-searches manifest fences, loads only the
target extent pages, validates each loaded page against its fence, and resolves
the physical offset with checked arithmetic. Pages touched by one request are
cached for that request. Active reads use the immutable acknowledged cursor
snapshot.

`read_from` captures the current tail and emits at most `read_window_bytes` per
pull, preserving logical order and bounded retained memory. Each physical read
has an observation watchdog that never cancels or retries the underlying
future.

## 7. Trim and Reclamation

`trim_prefix(g)` rejects regression and `g > tail`. It first publishes the new
logical trim point. Only afterward does it release complete sealed extents
whose logical end is at or below `g`. A boundary extent remains allocated.

Cleanup is idempotent and bounded by `gc_bytes_per_pass`, with at least one
eligible extent processed so progress cannot stop behind a large chunk. A
second generation removes successfully released extents from the directory.
Calling trim again at the same watermark retries unfinished physical cleanup.
Reads below the published watermark remain inaccessible throughout every crash
point.

## 8. Bounds and Observation

Defaults bound the queue at 1,024 requests and 16 MiB, a batch at 64 requests
and 1 MiB, one append at 64 MiB, an extent page at 256 entries, a sequential
read window at 8 MiB, and one GC pass at 64 MiB. All bounds are configurable
and validated as nonzero.

Lock-free counters report submitted/completed/failed requests, logical and
three-mirror physical bytes, batch/request counts, rollovers, read bytes,
reclaimed bytes, and watchdog observations. Append and read watchdogs log the
stream identity, epoch, logical range or offset, operation age, queue age,
batch size, and stage at every interval while retaining the original future as
the sole completion owner.

## 9. Errors and Invariants

`StreamError` distinguishes invalid requests, admission backpressure, stale
writers, definitely absent appends, ambiguous resolution, stalled writes,
unavailable reads, corruption, and internal invariant failures.

The central invariants are:

- returned append ranges form one durable ordered tail;
- append never updates group 0 or stream KV metadata per batch;
- ambiguous data is never resubmitted at a guessed offset;
- one append belongs to one chunk;
- logical offsets never renumber after rollover or trim;
- logical trim publication precedes physical deletion;
- immutable page coverage is gap-free over the retained sealed interval;
- an ownership epoch can advance but cannot regress; and
- watchdogs observe without cancelling, retrying, or completing IO.

## Open Issues

- Production group-0 and metadata-group adapters remain part of the R143
  activation lifecycle; the library currently exposes their complete injected
  contracts and in-memory implementations.
- The production three-mirror chunk writer still needs to bind the stream
  contract to chunk-client allocation, cursor, seal, read, and orphan cleanup.
- Production chunk-reader integration still needs adjacent-range coalescing and
  bounded out-of-order prefetch; the core currently issues ordered range reads.
- Orphan enumeration and retained-generation cleanup need production metadata
  watermarks; interrupted rollover recovery is implemented, but unreachable
  prepared objects are not yet surfaced as a cleanup report.
- Hardware workloads must select final queue, watchdog, page, read-window, and
  GC limits. Metadata-group rebinding, per-stream sharding, and EC conversion
  remain deferred scale-out work.
