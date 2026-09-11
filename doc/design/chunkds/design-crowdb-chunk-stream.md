<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Chunk Stream

The chunk stream presents finite mirrored chunks as one durable, ordered
logical byte space. It supplies the storage boundary used by the partition WAL
without owning WAL framing, request identity, or replay policy.

Depends on: [chunk IO](../chunkio/design-crowdb-chunkio.md),
[chunk reader](../chunkio/design-crowdb-chunkio-reader.md),
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
opaque, time-ordered 128-bit identity bound to a logical partition, never to a
process or node. It renders as 32 hexadecimal digits. `ChunkStream` exposes
`create`, `open`, `tail`, vectored `append`, exact `read_at`, seekable bounded
sequential reading, `trim_prefix`, and `close`.

Every nonempty `append` returns the selected chunk identity and exact logical
range. A zero-length ordinary append returns no chunk identity and performs no
IO. `append_chunk_bound` accepts caller-framed body+CRC bytes and makes the
stream worker add the selected chunk's canonical 128-bit ID after rollover
selection. The returned logical range includes that trailer. This keeps chunk
choice in the ordered worker while allowing a journal to bind each frame to its
physical source.

Each writable handle owns one bounded Tokio MPSC queue and one worker task. The
task exclusively mutates the active descriptor, extent list, manifest
generation, tail, trim point, and rollover state. Producers and readers use
atomics plus immutable `ArcSwap` manifest snapshots; the hot path adds no lock.

The stream does not interpret record headers, checksums, request identities, or
replay semantics. Ordinary append stores exact caller bytes. Chunk-bound append
adds only the fixed physical-provenance trailer requested by the caller; the
partition journal owns its surrounding frame and consumer watermarks.

## 2. Storage Contracts

Three injected async traits isolate the state machine from placement and
transport:

- `StreamRegistry` loads and creates the small group-0 binding. It is used only
  by create/open, never append or read. Binding keys are readable text. Stream
  creation accepts a metadata-group selection and defaults to group 1.
- `StreamMetadataStore` loads the stable manifest head and immutable extent
  pages. It writes fresh versioned pages before publishing the head through a
  KV revision compare-and-set. `None` means the stream must not exist.
- `StreamChunkStore` allocates mirrored chunks, writes one contiguous range to
  every mirror, advances and inspects the fenced durable cursor, seals chunks,
  reads physical ranges, and idempotently releases trimmed chunks.

The production chunk store uses a direct-buffer `MirrorChunkWriter` from
`crowdb-chunk-client`. It owns one chunk, appends mirror strips asynchronously,
and never constructs the EC pipeline. Stream rollover remains above it.

The `test-util` feature provides one deterministic in-memory implementation of
all three traits, including write suspension, cursor outcomes, and metadata
publication failure injection. Production adapters retain the same ownership
and durability contract.

## 3. Metadata

Group 0 stores only `StreamBinding {stream_name, metadata_group_id,
binding_generation, state, owner_kind}`. One nonzero KV group stores one stable
head per stream plus immutable extent pages under versioned keys.

`StreamManifest` contains the writer epoch, generation, logical trim point,
sealed tail, optional active descriptor, page fences, and closed state. Every
head mutation uses conditional KV write; no competing writer may blind-write
that key. `StreamExtentPage` contains parallel arrays:

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

For a chunk-bound request, the worker adds the selected `ChunkId` after the
caller's bytes before assembling the aggregate write. Admission, remaining
capacity, cursor advancement, and returned ranges include the trailer. Several
requests may still share one physical batch; each receives its own trailer.

## 5. Rollover and Recovery

One append never straddles chunks, and one stream chunk contains at most 256
MiB of logical data. If an append cannot fit, the worker seals the old chunk,
appends its logical mapping to the extent builder, writes a fresh immutable
version of the affected tail page, allocates a successor, and CAS-publishes the
successor head before writing the request. A crash before the head CAS leaves
the previous head complete and the new page unreachable. Objects not reachable
from authoritative metadata are orphans.

Writer open resolves the authoritative tail metadata and last extent, then
queries the last chunk's durable cursor. It never appends to a chunk owned by a
previous process. The old chunk is sealed at its acknowledged cursor and a
fresh 256-MiB mirrored chunk is published before new bytes are admitted. Before
initial publication and every CAS retry, the writer compares the observed head
epoch with its local ownership epoch. A lower epoch returns `StaleWriter`; only
a higher epoch holding current ownership may adopt an older head. KV revision
CAS serializes publications, while the epoch comparison prevents authority
regression. Full extent pages remain immutable; short restart extents are
accepted and require no merge pass.

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

A sequential reader starts at a caller-supplied logical offset and accepts a
finite byte hint or `ToEnd`. It captures the corresponding durable end, returns
EOF there, and leaves EOF handling to its caller. Cached bytes are returned
while adjacent physical ranges are prefetched out of order within one bounded
window; delivery remains in logical order. The default retained window is 8
MiB and at most eight physical reads from that window run concurrently.
Independent readers may run concurrently. Each physical read has an observation
watchdog that never cancels or retries the underlying future.
A provenance-aware reader also yields each logical segment's physical chunk
identity. A journal compares that identity with its frame trailer. The durable
acknowledged cursor remains the read and recovery upper bound; identity
validation never promotes residual bytes beyond it.

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

Defaults bound the queue at 1,024 requests and 64 MiB, a batch at 64 requests
and 1 MiB, one append at 64 MiB, one chunk at 256 MiB, an extent page at 1,024
entries, a sequential read window at 8 MiB with eight concurrent physical
reads, and one GC pass at 64 MiB. All bounds are configurable and validated as
nonzero.

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

- Production registry/metadata adapters, mirror writer, reader prefetch, and
  orphan reporting remain implementation work. NullDisk-backed concurrency
  workloads select final queue, watchdog, page, read-window, and GC limits.
  Metadata-group rebinding, per-stream sharding, and EC conversion are deferred
  scale-out work.
