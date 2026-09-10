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

`ChunkReadPolicy` configures stream-window bytes, EC-recovery scratch bytes,
layout safety margin, and bounded layout retries.

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

Observed failed segment identities are durably added to the exact old strip
before bytes from the attempt are returned. This metadata update can advance
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

Every live DiskIO failure and every segment already marked unavailable is
retained as an observation. `ChunkReader` submits the observed chunk revision,
exact old strip, and a geometry-identical replacement containing the sorted
`unavailable_segments` set through `replace_chunk_strip_range`. A deterministic
operation ID makes an ambiguous response replayable.

Successful fallback waits for this marker, not for a full rebuild. ChunkDB's
rotating scanner turns the marker into a persistent `RepairStrip` task. The
background handler rebuilds full mirror or EC shards so later reads return to
the direct path. Task key/value, scheduling, publication, and crash behavior
are defined by
[Mirror-to-EC Conversion and Chunk Tasks](../chunkdb/design-crowdb-chunkdb-mirror-to-ec.md).

## 8. Invariants

- I1: every returned byte is covered by a validated location and strip.
- I2: bytes from an expired layout are never returned.
- I3: active-strip reads never exceed the durable acknowledged cursor.
- I4: EC recovery never exceeds its shared scratch-memory budget.
- I5: stream items never exceed the configured window.
- I6: unrecoverable redundancy loss is explicit and never zero-filled.
- I7: a successful fallback is not returned until its failed segment identity
  is durable in chunk metadata.
- I8: incomplete parity is never used to reconstruct a data shard.
