<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R141: chunk-stream — Mirrored logical byte stream

## Problem

Chunks are finite durable objects, while a WAL needs one ordered logical byte
space that can append indefinitely, reopen at a logical offset, read forward
across chunk boundaries, and discard a durable prefix. Existing chunk IO can
write and read a `Location`, but it does not organize multiple WAL chunks as
one named stream or persist their logical-to-physical relationship.

Putting the complete stream map in group 0 would make every rollover and GC
operation contend with cluster-wide control metadata. Keeping it only in a
server process would make restart and ownership transfer unsafe. The term
`group` is also ambiguous: a stream metadata group is a CROWDB KV group, not a
DiskDB disk-group or a chunk strip-reservation group.

The first deployment expects few streams. It needs a direct, high-throughput
single-group design now; moving one stream's metadata between KV groups or
sharding one stream across groups is future scale-out work.

Root design links are `doc/design/chunkio/design-crowdb-chunkio.md` for chunk
writes and `Location`, `doc/design/chunkio/design-crowdb-chunkio-reader.md` for
range reads, `doc/design/chunkdb/design-crowdb-chunkdb.md` for chunk and strip
lifecycle, and `doc/design/kv/design-crowdb-kv-group0.md` for group-0 sysdata.

## Solution

Create `crowdb-chunk-stream`, a single-writer logical byte stream assembled
from three-way mirrored chunk strips. Store its extent metadata in one bound
KV group and only its registry binding in group 0.

1. Add a permanent chunk-stream design defining identifiers, metadata schema,
   append and read semantics, rollover, recovery, prefix trim and physical GC,
   writer fencing, crash ordering, limits, and metrics. Explicitly defer
   metadata-group migration, per-stream metadata sharding, and EC conversion.
2. Add a `crowdb-chunk-stream` library crate with asynchronous byte-stream
   operations: `create`, `open`, `tail`, vectored `append`, `read_at`, seekable
   sequential readers, `trim_prefix`, and `close`. A reader accepts either a
   finite byte-count hint or `ToEnd`, returns EOF at its captured durable end,
   and retains a bounded prefetch window. A nonempty `append` returns
   `{stream_name, chunk_id, begin, end}` only after all three mirror writes and the active
   chunk's acknowledged cursor are durable. Add `append_chunk_bound` for R142:
   the caller supplies its complete body and CRC, then the stream worker chooses
   the target chunk and appends that chunk's canonical 128-bit ID as a trailer
   in the same durable request. The returned range includes the trailer. R142
   owns all other record framing, request identity, and replay interpretation.
   A zero-length ordinary append performs no IO and returns the current range
   with no chunk identity.
3. Give each stream a stable, time-ordered 128-bit `stream_name`, rendered as
   32 hexadecimal digits, and bind it to exactly one metadata KV group. Store
   a small, readable-key group-0 registry record containing `stream_name`,
   `metadata_group_id`, binding generation, state, and optional owner-kind.
   Group 0 is consulted on create/open and binding refresh, never on append or
   read. The registry does not contain the extent array or committed tail.
   R143 creates and activates production bindings; its configured metadata
   group defaults to group 1. R141 accepts an injected registry so standalone
   tests do not require a server.
4. Store stream metadata under the stream prefix in its bound KV group. Use a
   stable head key protected by R101 compare-and-set for the `StreamManifest`.
   Write every changed tail page under a fresh versioned COW key; published
   extent pages are immutable. The manifest contains `stream_name`, writer
   epoch, generation, `trim_offset`, sealed logical tail, active chunk identity
   and physical start, active logical start, and extent-page fences. An extent
   page stores parallel arrays:

   - `chunk_ids[N]`
   - `logical_offsets[N + 1]`
   - `physical_offsets[N]`

   Entry `i` maps logical range
   `[logical_offsets[i], logical_offsets[i + 1])` to bytes beginning at
   `physical_offsets[i]` in `chunk_ids[i]`; logical and physical lengths are
   equal. Logical ranges are ordered, gap-free, non-overlapping, and never
   renumbered after prefix trim. Page-level first/end logical offsets permit a
   binary search without loading the whole stream map.
5. Keep the open active chunk outside the sealed extent arrays. Its logical end
   is derived as `active_logical_start + acknowledged_cursor -
   active_physical_start`, using checked arithmetic. Appending writes at the
   expected physical cursor on all three mirrors and then advances chunkdb's
   fenced acknowledged cursor. It does not update the stream KV group for every
   append. If cursor publication has an ambiguous outcome, the stream worker
   queries the durable cursor and validates the batch range and checksum before
   completing its original futures. It either proves that batch committed once
   or faults the stream for recovery; callers never retry bytes at a guessed
   offset. R142 owns request identity above this boundary.
6. Give each open stream one bounded MPSC append queue and one single-owner
   async worker. Admission reserves both request count and bytes before enqueue.
   The worker assigns each accepted request its logical range in queue order.
   When idle, it takes the first request immediately and drains only
   requests already queued, stopping at configured request count, byte size,
   or remaining strip/chunk space. It never waits on a batching timer. Assemble
   the ordered buffers in one retained staging area, adding the selected
   `ChunkId` after each chunk-bound request, issue one contiguous write to the
   three mirrors concurrently, advance the acknowledged cursor once, and
   complete every request with its individual logical subrange. Admission and
   rollover capacity include the trailer bytes. The first request therefore
   has no artificial aggregation delay, while concurrent journal traffic
   naturally forms larger writes. A failed batch advances no request and faults
   the ordered stream until recovery resolves its outcome.
7. Add a one-chunk direct-buffer `MirrorChunkWriter` to
   `crowdb-chunk-client`. It consumes owned byte buffers, appends three mirror
   strips asynchronously, and never constructs the EC pipeline. The stream
   layer prepares the next mirrored WAL chunk before the active chunk fills.
   Each chunk has a hard 256-MiB logical capacity.
   If one append does not fit, roll before writing it; an append never straddles
   chunks, and an append larger than one chunk's supported data capacity is
   rejected without advancing the tail. On rollover, seal the old chunk, write
   a new immutable version of the affected tail page, install the new active
   descriptor in a candidate manifest, and finally CAS-publish that manifest
   on the stable head key in the metadata KV group. Recovery reads the
   CAS-current head and validates every referenced page; prepared chunks or
   metadata records not reachable from it are orphans. A writer reopen never
   resumes the prior process's chunk: it seals that chunk at its acknowledged
   cursor and allocates a fresh chunk. Short sealed extents remain valid and
   require no merge pass.
8. Resolve reads by rejecting offsets below `trim_offset` or above the durable
   tail, binary-searching extent-page fences and then `logical_offsets`, and
   translating to `physical_offsets[i] + (logical - logical_offsets[i])`.
   Continue across extent, chunk, and strip boundaries through the existing
   chunk reader. The read path validates array lengths, monotonicity, exact
   coverage, checked arithmetic, chunk state, and acknowledged cursors before
   returning bytes. Cache immutable manifest and extent pages, coalesce adjacent
   physical ranges in one chunk, and prefetch only within a bounded read window
   of 8 MiB by default; cached bytes may be returned while later ranges are in
   flight. Multiple readers may run concurrently, but each emits results in
   logical-offset order even if IO completes out of order. A provenance-aware
   reader also returns the physical `chunk_id` for each logical segment so R142
   can compare a chunk-bound frame trailer with its actual source chunk.
9. Implement `trim_prefix(g)` for a caller-supplied durable consumer watermark.
   Reject regression and `g > durable_tail`. First publish a new manifest with
   `trim_offset = g`; only then remove fully trimmed extent entries/pages and
   release complete chunk strips whose mapped logical end is `<= g`. If `g`
   falls inside a strip, retain that boundary strip and treat its earlier bytes
   as dead; strip is the minimum physical reclamation unit in the first
   version. Deletion is idempotent and retryable, and never deletes the active
   strip or bytes at or after `g`.
10. Fence all active-chunk cursor advances and manifest generations with the
    monotonically increasing partition ownership epoch supplied by R142. Every
    head update uses R101 CAS on the KV revision; no writer may blind-write the
    head. Before a first publication or any CAS retry, compare the observed head
    epoch with the local epoch. A lower local epoch returns `StaleWriter`; an
    equal epoch may continue its sequenced transition; a higher epoch may adopt
    the head only while holding R142 ownership authority. This monotonic epoch
    check plus the CAS revision prevents an old owner from learning a newer
    revision and publishing an epoch regression. R143's binding need not select
    a metadata epoch. Within an epoch, one stream task sequences append,
    rollover, trim, and close without adding a lock to the append hot path.
11. Bind the durable stream identity to the logical partition, not a process or
    node. One R142 partition handle exclusively drives its stream handle. R143
    supplies group-0 binding and ownership decisions through R142 and never
    appends, reads, or performs stream GC directly. Ownership transfer reopens
    the same stream and chunks under a higher epoch; split creates separate
    child stream identities as part of the child artifacts.
12. Add the stream-specific chunk type and the backward-compatible chunk-record
    `owner_key` extension needed by the production writer. Every new stream
    chunk carries an owner-kind prefix plus `StreamName`; old records decode as
    shared/unattributed. R146 builds on this landed schema with the generic
    restart-safe lease sweep that seals abandoned non-empty chunks and deletes
    abandoned zero-length chunks across all chunk users. Superseded or
    unreachable stream metadata is a separate watermark-driven metadata-GC
    concern because chunkdb cannot infer metadata reachability.
13. Bound active append buffers, in-flight mirror writes, extent-page size,
    read window, prepared successor chunks, current head snapshots, orphan
    metadata cleanup, and GC work per pass. Attach an observation watchdog to
    every in-flight append
    batch and read window. At each watchdog interval, record and log
    `stream_name`, epoch, logical range, operation age, queue delay, batch
    request/byte count, and current IO/metadata stage, then continue awaiting
    the same durability future. The watchdog never cancels, retries, or
    completes an operation whose outcome may be committed; transport timeouts
    and recovery own those decisions. Add metrics for logical and physical
    append bytes, queue depth/bytes/delay, batch size/fill, append/sync latency,
    watchdog observations, rollover stalls, logical-to-physical lookup latency,
    extent-page cache hits, read-window/coalescing size, replay bytes, mirror
    failures, stale-writer rejects, tail recovery, trim lag, reclaimable strips,
    reclaimed bytes, and orphan metadata/chunks.
14. Return typed outcomes that let R142 contain failures without guessing from
    text: invalid request, admission backpressure, stale/fenced writer,
    definitely-not-committed append, internally resolving ambiguous append,
    read unavailable, corrupt data/metadata, and internal invariant failure.
    An ambiguous append is not returned to the caller until R141 proves commit
    or absence; if it cannot prove either within policy, the stream handle
    becomes write-stalled and requires reopen, while already durable reads stay
    available where their chunks are healthy.

Edge outcomes are explicit: an empty stream has `tail = trim_offset = 0` and no
chunk until its first append; zero-length append performs no IO; logical
offsets never change after rollover or trim; loss of one previously committed
mirror can use normal chunk recovery, but a new append requires all three
target writes; missing or corrupt committed bytes fail recovery or read rather
than creating a logical gap; an append never straddles chunks; and
metadata-group scale-out is not attempted in this requirement.

## Dependencies

- Depends on `crowdb-chunk-client` and chunkdb for three-way mirror allocation,
  fenced acknowledged cursors, sealing, range reads, complete-strip deletion,
  and orphan detection/reporting. It reuses the existing `Location` mapping
  semantics but owns the multi-chunk stream index. R141 adds the direct-buffer
  one-chunk `MirrorChunkWriter`, the Stream chunk type, and compatible owner
  metadata; R146 later adds expired-owner cleanup and extends the contract
  uniformly to all chunk types.
- Depends on group-0 sysdata and `crowdb-kv-client` for the stream registry, and
  on an ordinary nonzero CROWDB KV group for manifests and extent pages. R141
  adds protocol key/value types for both. Tests may inject in-memory registry
  and metadata-store implementations.
- Depends on R101 KV compare-and-set for stable-head publication fencing. All
  competing head mutations use conditional writes; immutable versioned extent
  pages use fresh keys and become reachable only through a successful head
  CAS.
- R142 owns WAL framing and consumes the byte stream for append, replay,
  checkpoint watermark, transfer, and split. R143 supplies production binding
  records and ownership epochs through R142.
- R148 owns disabled-by-default metadata rebinding/sharding and sealed-chunk EC
  conversion after the single-group mirrored baseline is measured.

## Acceptance

- Given a registered stream bound to metadata group 7, when it is created and
  reopened, assert group 0 contains only the binding while group 7 contains its
  manifest and extent pages. Invariant: control registration is separate from
  stream metadata. Integration test.
- Given a hot stream with no rollover, when many async appends complete, assert
  group 0 and the stream metadata group receive no per-append write and each
  returned logical range ends at or below the active chunk's durable
  acknowledged cursor.
  Invariant: metadata placement does not put KV consensus in the append hot
  path. Integration test.
- Given an empty append queue, when one request arrives, assert the worker
  submits it without waiting for a batch timer; given requests accumulate while
  a prior batch is in flight, assert the next drain aggregates already queued
  requests up to configured byte, count, and remaining-space bounds. Invariant:
  aggregation improves concurrency throughput without imposing a fixed
  single-request latency floor. Unit test.
- Given one aggregate contains several ordered appends, when its three mirror
  writes and one cursor advance complete, assert every caller receives the same
  `stream_name` and its exact non-overlapping logical subrange in enqueue order;
  on failure, assert none completes successfully. Invariant: physical
  aggregation preserves logical append boundaries and all-or-nothing batch
  acknowledgement. Integration test.
- Given several R142 chunk-bound appends are batched around a rollover, when
  the worker selects their target chunks, assert it appends the matching
  canonical `ChunkId` after each supplied body+CRC, includes each trailer in
  capacity and logical-range accounting, and returns that same ID to its
  caller. Invariant: frame construction cannot race post-write chunk
  discovery. Integration test.
- Given a sequence of vectored appends crosses several chunk and strip
  boundaries, when read from logical offset 0, assert the exact concatenated
  bytes and stable logical ranges are returned. Invariant: chunk boundaries are
  invisible in the logical byte stream. Integration test.
- Given extent arrays with N chunk IDs, N+1 logical offsets, and N physical
  offsets, when logical offsets at the first byte, an interior byte, and an
  extent boundary are resolved, assert the exact chunk and physical byte are
  selected; malformed lengths, gaps, overlaps, or overflow are rejected.
  Invariant: offset translation is total and unambiguous. Unit test.
- Given a large extent index split across pages, when a reader starts near the
  tail, assert it binary-searches page fences and the target logical-offset
  array without reading preceding extent pages. Invariant: seek work is
  logarithmic in live extent pages plus sequential result IO. Unit test.
- Given an async sequential read spans adjacent ranges in one chunk and then a
  second chunk, when IO completes out of order, assert the first ranges are
  coalesced, memory stays within the read-window budget, and bytes are yielded
  in logical order. Invariant: asynchronous read concurrency cannot reorder or
  over-buffer the stream. Integration test.
- Given finite and `ToEnd` readers seek into cached and uncached ranges, when
  multiple readers run concurrently, assert each returns EOF at its captured
  durable end, emits ordered bytes, and retains no more than its configured
  8-MiB default window. Invariant: prefetch concurrency cannot make read memory
  unbounded or change reader ordering. Integration test.
- Given active-chunk cursor publication returns an ambiguous result, when the
  worker resolves the durable cursor and batch checksum, assert it completes
  the original append futures exactly once if committed or faults the stream
  without resubmitting at another offset. Invariant: ambiguous completion
  cannot duplicate stream bytes or create a logical gap. E2E test.
- Given a provenance-aware reader encounters a frame trailer naming another
  chunk or complete-looking residual bytes beyond the acknowledged cursor,
  when R142 validates recovery input, assert the identity mismatch truncates
  that frame and no byte beyond the cursor is returned. Invariant: physical
  identity strengthens recovery but never promotes unacknowledged data.
  Integration test.
- Given an append does not fit in the current chunk, when it is submitted,
  assert rollover completes before any byte is written and the append occupies
  one logical range in the successor; an append larger than the maximum is
  rejected with an unchanged tail. Invariant: one append never exposes a
  crash-visible partial prefix across chunks. Integration test.
- Given the production stream writer, when it writes and reopens around the
  256-MiB limit, assert it uses the direct three-way mirror path without
  starting EC work, rotates at the limit, and allocates a fresh chunk after
  reopen. Invariant: chunk capacity and restart ownership are explicit and a
  stale process's chunk is never resumed. Integration test.
- Given a crash before or after rollover manifest publication, when the stream
  reopens, assert it selects either the complete old generation or the complete
  new generation, never a partial extent map, and reports unreachable prepared
  objects for cleanup. Invariant: rollover publication is atomic. Integration
  test.
- Given owner A publishes or delays a head CAS while higher-epoch owner B takes
  over, when either ordering occurs, assert B can adopt a complete A head, A's
  CAS using an older revision fails after B publishes, and A cannot retry from
  B's revision because its epoch is lower. Invariant: metadata authority cannot
  regress to a stale ownership epoch. Integration test.
- Given durable consumer watermark `g` falls between strips, when prefix trim
  completes, assert reads below `g` fail, complete earlier strips are released,
  and reading from `g` returns the original suffix. Invariant: trim removes no
  live byte. Integration test.
- Given `g` falls inside a strip, when prefix trim completes, assert the strip
  remains allocated, its prefix is logically inaccessible, and later complete
  strips remain readable. Invariant: physical GC rounds retention toward
  safety at strip granularity. Unit test.
- Given GC crashes after publishing `trim_offset` but before deleting all old
  strips, when it retries, assert it completes deletion idempotently; given a
  crash before trim publication, assert no strip is deleted. Invariant: logical
  removal precedes physical reclamation. Integration test.
- Given a partition transfers from server A to B, when A is fenced and B opens
  the same stream under a higher epoch, assert B recovers its tail and appends
  without moving WAL chunks; later writes under A's epoch remain unreachable.
  Invariant: stream identity belongs to the partition, not the server. E2E test.
- Given the chunk-KV production call graph, when stream operations are
  inspected, assert only R142 partition lifecycle code receives a stream
  handle and R143 uses R142 lifecycle methods. Invariant: the server controls
  authority without owning stream mechanics. Unit test.
- Given an append batch or read window remains pending across several watchdog
  intervals, when observations fire, assert stage/age/range metrics and logs
  advance while the original operation retains sole ownership of completion;
  after it completes, assert no watchdog fires again. Invariant: watchdog is
  diagnostic and cannot create cancellation, retry, or double completion. Unit
  test.
- Given each injected R141 failure class, when it crosses the public API,
  assert callers receive the matching typed outcome without parsing text and
  an unresolved append never permits a later append to pass it. Invariant:
  failure containment preserves one logical tail and is machine-decidable.
  Unit test.
- Given fixed append sizes, queue depths, rollovers, random seeks, sequential
  replay, and prefix-trim workloads, when benchmarks run, assert throughput,
  p50/p99 latency, memory, KV metadata writes, lookup amplification, and GC
  bandwidth satisfy limits selected in the permanent design. Invariant: stream
  defaults are evidence-based and bounded. Integration test.

Required gates:

- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
- `pixi run test-chunk-client`
- `pixi run -- cargo test -p crowdb-chunk-stream --all-targets`
- `pixi run clean-env && pixi run test-server`

## Open Issues

- `MirrorChunkWriter` provides the one-chunk, owned-buffer, three-copy direct
  path without EC. `ProductionStreamChunkStore` connects it to fenced cursor
  reconciliation, `ChunkReader`, sealing, and whole-chunk trim release using
  an atomic per-chunk view rather than an append-path lock. Chunk-KV server
  construction and a real chunkdb/diskio restart test remain open production
  lifecycle wiring.
- `KvStreamRegistry` and `KvStreamMetadataStore` now provide group-0 binding,
  nonzero-group immutable extent pages, and an R101 CAS manifest head. Server
  lifecycle wiring and a real-KV crash-order integration test remain with the
  production adapter work.
- The seekable reader supports finite/`ToEnd` hints, keeps one bounded next
  window in flight while the caller consumes the current window, and runs a
  configurable bounded number of physical reads concurrently without changing
  logical delivery order. Memory and concurrency benchmarks remain open.
- Extent-page identities derive from their stable first logical offset, so a
  trimmed generation retains a nonzero first page index without renumbering
  logical bytes. Watermark-driven cleanup of superseded page generations still
  needs the persistent metadata-GC policy.
- Stream names are process-unique and time ordered and reopen always rotates
  the old active chunk. The backward-compatible chunk owner-key schema and
  reporting abandoned chunks to R146 remain open because the current `Chunk`
  record has no owner field.
- Non-blocking lifecycle follow-up: until deferred R146 lands, a crashed
  writer's abandoned Active stream chunk remains allocated. R141 never resumes
  it and allocates a fresh chunk; the abandoned owner epoch remains visible to
  R146's future scan, so this is an operational-cleanup gap rather than a
  stream-safety gap.
