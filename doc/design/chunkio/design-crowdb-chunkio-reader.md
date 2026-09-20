<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Chunk Object Reader

The chunk object reader reconstructs full, ranged, and streamed objects from
writer-produced locations across current mirror and erasure-coded layouts.

Depends on: [chunk IO](design-crowdb-chunkio.md),
[chunkdb](../chunkdb/design-crowdb-chunkdb.md), and
[mirror-to-EC conversion](../chunkdb/design-crowdb-chunkdb-mirror-to-ec.md).

## Contents

1. [API](#1-api)
2. [Location mapping](#2-location-mapping)
3. [Strip reads](#3-strip-reads)
4. [Read consistency](#4-read-consistency)
5. [Memory bounds](#5-memory-bounds)
6. [Errors](#6-errors)
7. [Durable repair handoff](#7-durable-repair-handoff)
8. [Invariants](#8-invariants)

## 1. API

`ChunkIoClient` owns one `ChunkReader` sharing its metadata client and
lock-free DiskIO route snapshot. It exposes:

- `read_object`: return the complete logical object as `Bytes`.
- `read_range`: return one half-open logical interval.
- `read_range_partial`: return ordered successful intervals and exact failed
  intervals without substituting bytes.
- `read_stream`: return a pull-based `ChunkReadStream` whose item size is
  bounded by policy.

`ChunkReadPolicy` configures stream-window bytes, a 256-MiB process-wide
EC-recovery budget, layout safety margin, bounded layout retries, and the
ad-hoc full-fragment threshold.

## 2. Location mapping

Locations are sorted by `logical_offset` and must cover one gap-free logical
interval. Zero-length entries are ignored. Physical length must cover logical
length, and all additions use checked arithmetic.

For each intersecting location, the reader queries current chunk metadata. It
validates that stored strip intervals are ordered and non-overlapping, binary
searches the first overlap, and walks explicit
`[chunk_offset, chunk_offset + capacity)` intervals. It never derives geometry
from strip vector index or assumes uniform width. This keeps locations stable
when several mirror strips become one equal-capacity EC strip.

Independent locations are fetched concurrently and assembled by logical
offset. The output length must equal the requested logical length.

## 3. Strip reads

Mirror reads try usable replicas in metadata order. Each request uses the
exact segment-relative offset and length; DiskIO supports arbitrary byte
ranges.

EC reads derive the shard width from stored segment geometry. The fast path
reads only data-shard intersections needed by the request. If a required data
read fails, the reader reads the same byte interval from surviving data and
parity shards and invokes ISA-L decode. It starts only enough surviving reads
to obtain `data_num` shards and issues another read only after a candidate
fails. A 16-KiB request therefore reconstructs 16 KiB rather than rebuilding
the whole shard. Missing shards beyond `code_num` are unrecoverable.

An EC strip with incomplete parity still permits direct reads from healthy
data shards. A failed data read in that state is unrecoverable because parity
that has not reached the durable `Parity` state is never a decode source.

Zero-filled data shards beyond a partial strip's durable length participate in
decode without issuing DiskIO. Returned data is always clipped to exact
location length, so stored KiB rounding and parity padding never escape.

## 4. Read consistency

Each metadata query advertises a maximum layout validity duration. The reader
records the local query start and subtracts a safety margin. If all dependent
DiskIO does not finish before that deadline, it discards every byte from the
attempt, re-queries, and retries within policy. ChunkDB retains replaced
segments through the same window.

Sealed strips expose `sealed_length`. An active shared mirror strip may also
expose bytes below the chunk's durable `acknowledged_cursor`; later bytes are
`NotYetAvailable`.

Only an unparseable returned frame or a write-frame checksum mismatch proves
physical corruption. The reader reports its exact serving segment to ChunkDB;
ChunkDB first fences the DiskDB BusyBlock to `Corrupt`, then adds the segment
to `unavailable_segments` and admits `RepairStrip`. Network, timeout, and
unknown I/O failures may be decoded around but never create these markers.
This metadata update can advance
the revision of an active shared chunk. Its owning writer refreshes the chunk,
verifies state and writer epoch, recognizes an ambiguously committed cursor,
and retries the cursor advance against the new revision.

## 5. Memory bounds

Partial EC recovery reserves decode shard buffers plus output from a
client-wide Tokio semaphore before issuing recovery I/O. A request larger than
one reservation is split into same-offset slices. The limit covers recovery
scratch, not the caller-owned result or ordinary direct-read buffers. No lock
is introduced.

`read_object` necessarily owns the complete returned object. Callers use
`ChunkReadStream` for large objects; each pull reads at most
`stream_window_bytes` and retains no prefetched second window.

## 6. Errors

`ReadError` separates malformed locations/ranges, deleted chunks,
not-yet-durable data, expired layouts, metadata failures, DiskIO failures, EC
decode failures, data loss, and exact failed logical ranges. Strict reads fail
at the first missing range. Partial reads preserve healthy ranges before and
after failures. A stream emits its contiguous successful prefix, one ranged
error, and then terminates; it never silently truncates or zero-fills. Empty
objects and empty ranges perform no RPC or DiskIO.

## 7. Durable repair handoff

Verified corruption is reported through a versioned chunk-routed RPC carrying
the expected revision, strip sequence, exact segment incarnation, and
operation ID. A small read still decodes only its requested slice; repair
admission proceeds independently and may begin before that read returns.
Matching client failures accumulate for one second. At a complete fragment or
the lesser of 1 MiB and half a fragment, one bounded full-fragment request is
sent to ChunkDB and same-key readers share its result future. Saturation falls
back to slice recovery and the durable repair task.

ChunkDB coalesces full-fragment requests across clients under 32 concurrent
jobs and a 512-MiB shared decode/result budget by default. The existing
`RepairStrip` task provides the only allocation and publication authority;
rebuilt bytes can reach waiters after target fsync while fenced publication
continues. Task key/value, scheduling, publication, and crash behavior are
defined by
[Mirror-to-EC Conversion and Chunk Tasks](../chunkdb/design-crowdb-chunkdb-mirror-to-ec.md).

## 8. Invariants

- I1: every returned byte is covered by a validated location and strip.
- I2: bytes from an expired layout are never returned.
- I3: active-strip reads never exceed the durable acknowledged cursor.
- I4: EC recovery never exceeds its shared scratch-memory budget.
- I5: stream items never exceed the configured window.
- I6: unrecoverable redundancy loss is explicit and never zero-filled.
- I7: a verified-corrupt segment is durably marked before its recovery is
  returned; an ordinary I/O error is never persisted as corruption.
- I8: incomplete parity is never used to reconstruct a data shard.
