<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk Object Read Flow (R107)

This implementation design refines [R107](../backlog/R107-chunkdb-chunk-read-flow.md)
against the landed [chunkdb](../design/chunkdb/design-crowdb-chunkdb.md) and
[chunk IO](../design/chunkio/design-crowdb-chunkio.md) designs. Large writes,
small writes, mirror-to-EC conversion, routed DiskIO, and bounded layout
validity are already present.

## 1. Read-capable DiskIO seam

The existing `DiskWriter` object already owns production disk routing and is
shared by `ChunkIoClient`. Add an arbitrary-offset `read` operation to this
seam so reads use the same immutable route snapshot as writes. The default
implementation returns an explicit unsupported error, preserving focused
writer test doubles; production `DiskioBlockWriter`, `RoutedDiskWriter`, and
the metrics wrapper implement or forward it.

```rust
async fn read(
    &self,
    segment: &Segment,
    unit_bytes: u64,
    segment_offset: u64,
    length: u32,
) -> Result<Bytes>;
```

The method validates that the requested byte interval lies within the
segment, but it does not require unit alignment. DiskIO already accepts an
arbitrary zone offset and byte size.

## 2. Strip reader

`StripReader` validates stored strip geometry and reads one intersection.
All `chunk_offset`, `capacity`, `sealed_length`, and `unit_kb` fields are
converted from KiB exactly once with checked arithmetic.

- Mirror: try available replicas in metadata order and return the first
  successful byte range. All failures become `DataLoss`.
- EC fast path: issue only data-shard reads required by the requested range.
- EC fallback: if any required data read fails, read every remaining data and
  parity shard in parallel, represent failures as missing shards, decode with
  the strip's stored scheme, then slice the reconstructed data. More missing
  shards than parity becomes `DataLoss`.
- A strip with an unset seal timestamp, zero sealed length, invalid body, or
  an intersection beyond sealed data is rejected; no partial bytes escape.

## 3. Chunk reader and consistency

`ChunkReader` accepts locations, validates ordered gap-free logical coverage,
maps each requested logical interval to physical chunk intervals, and reads
independent locations concurrently. Within a chunk it binary-searches stored
strip starts and walks explicit half-open intervals; strip vector indices and
uniform widths are never used as geometry.

Each query records its local start instant. Its layout deadline is the
advertised validity duration minus a bounded safety margin. If DiskIO finishes
after that deadline, all bytes from that attempt are discarded and the chunk
is re-queried. Retry count is bounded by policy. This matches chunkdb's
deferred segment cleanup contract.

`read_object` derives the complete logical interval. `read_range` reads only
intersecting strip bytes and verifies exact output length. Empty input and an
empty range return empty bytes without RPC or DiskIO.

## 4. Bounded stream

`ChunkReadStream` splits the logical object into windows no larger than the
configured memory budget and calls the same range reader for each window.
The stream retains at most one completed output window. Its `next_chunk`
interface makes backpressure explicit and avoids a second buffering runtime.
The default window is 64 MiB.

## 5. Client API and errors

`ChunkIoClient` exposes `read_object`, `read_range`, and `read_stream` using
its existing allocator and routed DiskIO object. `ReadError` distinguishes
invalid locations/ranges, deleted chunks, not-yet-available bytes, exhausted
layout retries, metadata failures, and unrecoverable data loss. Successful
fallback is transparent; background repair belongs to the later unified read
error-handling work.

## Scope

- `lib/crowdb-chunk-client/src/disk_io/disk_writer.rs`: read contract and validation.
- `lib/crowdb-chunk-client/src/disk_io/routing.rs`: routed production reads.
- `lib/crowdb-chunk-client/src/chunk/strip_reader.rs`: mirror and EC strip reads.
- `lib/crowdb-chunk-client/src/chunk/chunk_reader.rs`: location mapping, retries, range assembly, stream.
- `lib/crowdb-chunk-client/src/client.rs`: application-facing read methods and metric forwarding.
- `lib/crowdb-chunk-client/src/error.rs`, `src/chunk.rs`, `src/lib.rs`: public types and exports.
- `lib/crowdb-chunk-client/tests/chunk_reader_test.rs`: focused geometry and failure tests.
- `lib/crowdb-chunk-client/tests/chunk_reader_e2e.rs`: real-service write/read scenarios.
- Chunkdb and chunk IO permanent designs: current read contract after acceptance.

## Complexity

High. Stored geometry is mixed-width after conversion, partial EC tails need
zero-filled absent data shards during decode, and layout expiry must fence all
bytes from an obsolete map. The implementation adds no locks and keeps reads
parallel within strips and across locations.

## Test Design

- Empty/zero-length: no locations or only zero-length entries -> read -> empty
  bytes and zero metadata/I/O calls.
- Geometry: mixed mirror and equal-capacity converted EC intervals -> range
  spanning boundaries -> exact source slice; reordered/overlapping/gapped
  locations fail before returning bytes.
- Mirror: fail the first replica -> read -> later replica bytes; fail all ->
  `DataLoss`.
- EC: read intact data shards -> exact bytes without parity reads; fail one and
  four shards -> decode succeeds; fail beyond parity -> `DataLoss`.
- Partial EC: write a non-unit-aligned tail -> read -> exact logical bytes with
  no zero padding.
- Layout validity: delay reads past the first advertised deadline while
  changing the returned layout -> retry -> only current-layout bytes returned.
- Active chunk: point at an unclosed strip -> read -> `NotYetAvailable`.
- Real large write: full and partial objects through the complete cluster ->
  client read and partial range match input.
- Real rotation: force multiple chunks -> full read, cross-chunk range, and
  bounded stream -> ordered bytes and each emitted window within budget.
- Real small write: one and 100 shared objects -> each location reads only its
  bytes; a converted group plus mirror tail remains transparent.

## Module Structure

```text
lib/crowdb-chunk-client/
├── src/
│   ├── chunk/
│   │   ├── chunk_reader.rs   # object/range mapping, retry, stream
│   │   └── strip_reader.rs   # mirror/EC physical reads
│   ├── client.rs             # public client convenience API
│   ├── disk_io/
│   │   ├── disk_writer.rs    # common read/write seam
│   │   └── routing.rs        # lock-free production routing
│   └── error.rs              # ReadError
└── tests/
    ├── chunk_reader_test.rs
    └── chunk_reader_e2e.rs
```

## Config Extensions

`ChunkReadPolicy` contains `stream_window_bytes`, `layout_safety_margin`, and
`max_layout_retries`. Defaults are 64 MiB, 5 ms, and three retries. A parts-
based client can override policy for deterministic tests.

