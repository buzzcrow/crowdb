<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Chunk Stream

The chunk stream presents finite mirrored chunks as one durable, ordered
logical byte space. Append completion depends on mirrored data persistence;
Stream metadata records lifecycle transitions and bounded chunk record checkpoints.
The stream owns physical framing and addressing; the partition WAL owns
business-record framing, request identity, and replay policy.

Depends on: [chunk IO](../chunkio/design-crowdb-chunkio.md),
[chunk reader](../chunkio/design-crowdb-chunkio-reader.md),
[DiskIO](../diskio/design-crowdb-diskio.md),
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
10. [Open issues](#10-open-issues)

## 1. Ownership and API

`crowdb-chunk-stream` is an asynchronous Rust library. `StreamName` is an
opaque, time-ordered 128-bit identity bound to a logical partition, never to a
process or node. It renders as 32 hexadecimal digits. `ChunkStream` exposes
`create`, `open`, `open_read_only`, `tail`, vectored `append`, exact `read_at`,
seekable bounded sequential reading, `trim_prefix`, and `close`.
An owner-side flush/checkpoint barrier captures and persists a synchronized
prefix on demand without changing ordinary append completion.

Every nonempty append returns the selected chunk identity and exact logical
range. Logical lengths count caller payload bytes, excluding physical frame
headers and footers. A zero-length ordinary append returns no chunk identity
and performs no IO. `append_chunk_bound` and `append_chunk_bound_batch` keep
chunk selection in the ordered worker; the outer Stream frames carry the
selected chunk identity and CRC. They add no payload trailer. The stream does
not interpret the caller's business-record format or retry identity.

One append payload is one indivisible validity unit. A successful append makes
the whole payload available; recovery never exposes a proper prefix of that
payload. Physical frame and IO splitting are internal. Batching preserves each
request's append boundary rather than making the entire batch one group.

Each writable handle owns one bounded Tokio MPSC queue and one worker task.
The task exclusively mutates the active chunk's in-memory cursor and mappings,
logical tail, and lifecycle publication state. Producers and readers use
atomics and immutable `ArcSwap` views. Append and snapshot publication add no
hot-path lock. Batching preserves request order and exact returned ranges.

`open_read_only` validates one exact writer epoch and captures a finite view.
It does not adopt writer authority, allocate a successor, seal a chunk, or
publish writer state itself. It may request a flush/checkpoint from the current
owner. If the owner is unavailable it uses the consistent prefix persisted in
ChunkDB and records that checkpoint as its finite bound; that view can lag
successful appends. A caller requiring a newer exact cutover must not silently
accept the older fallback. The handle supports source-tail replay during split
and balance with these explicit bounds.

## 2. Storage Contracts

Three injected async traits separate Stream state from placement and transport:

- `StreamRegistry` loads and creates the stable group-0 `StreamRecord`.
  It is used by create/open, never by ordinary append or data reads. Typed
  binary keys contain the Stream ID; metadata defaults to nonzero group 1.
- `StreamMetadataStore` selects the nonzero group containing ordered
  `StreamChunkRecord` values. A shared record store implements conditional
  lifecycle transitions, reverse seek, candidates, interval leases and GC.
- `StreamChunkStore` allocates and seals mirrored chunks, writes and synchronizes
  the required replicas, exposes current chunk layout for bounded reads and
  recovery, and idempotently releases trimmed chunks. It does not require a
  separate ChunkDB cursor advance for each append.

Chunk IO and ChunkDB own `chunkinfo`: strips, physical capacity, replica
placement, growth, replacement, and their durable internal transitions. Each
such transition updates chunk information independently. Stream metadata refers
to stable chunk identities and logical mappings; internal chunk changes are
transparent and never trigger Stream chunk record publication.
Recovery resolves current capacity and layout through the chunk layer instead
of trusting capacity cached in an older Stream checkpoint.

DiskIO writes carry a boolean sync option. For a synchronized request the server
performs the ordered write, then the required flush, and returns one response
after both succeed. The client does not wait for a write reply and then send a
second fsync RPC. Buffered writes do not request the flush. When transport
limits require several requests, preserve strip-block write ordering and
complete required synchronization before acknowledging the append.

Every allocation preserves the durable `StreamPurpose`: system journals use
`ChunkType::Wal`, business streams use `ChunkType::Stream`. Both use the
canonical `stream/` owner key followed by the 128-bit stream identity. Reopen
rejects purpose disagreement between StreamRecord, StreamChunkRecord, chunk identity, and
chunk information, including a missing or mismatched owner key. Chunk-KV
journals require Wal streams.

`ProductionStreamRuntime` shares the process KV client and discovered chunk IO
routes. The Chunk-KV server chooses the metadata store; the StreamRecord
chooses its metadata group. `create_registered` initializes a control-plane
StreamRecord. The `test-util` implementation supplies deterministic mirror-write,
sync, tail-content, ownership, and metadata-publication fault injection.

## 3. Metadata

### 3.1 Identity and ordered keys

Group 0 stores the stable `StreamRecord`: purpose, Stream ID, metadata group,
record version, registration state, consumer kind and ID, and creation time in
Unix milliseconds. Writer leases are separate Group-0 records. Append,
preparation, rollover, close and writer relocation preserve the identity value
and original creation time. Consumer identity describes who owns the Stream;
it does not identify the currently leased process.

A nonzero metadata group stores `StreamChunkRecord` values under typed keys:

```text
record namespace | stream_id[16] | kind[1] | order[8 big endian]
Attached:  kind = 0, order = logical_start
Candidate: kind = 1, order = reservation_sequence
```

Each record has a chunk ID, purpose, state, reservation/attachment sequence,
writer epoch/session, publication generation, trim watermark, closed status and
compressed mappings. Attached records contain explicit half-open
`[logical_start, logical_end)` bounds. A Candidate has no assigned logical
range and accepts no user data. Attached Active and Sealed records sort by
logical start; candidates occupy a distinct suffix range. Reverse scanning the
attached prefix returns the current tail without a separate moving head.

The tail record carries Stream trim/closed state and the publication counter.
A successor inherits that control state. An empty Stream has an empty Active
record from creation; close seals it and persists closed status. The tail
record is retained even when every byte is trimmed. The Active record's end
is a durable checkpoint, not a limit on recovery or the live in-memory tail.

There is no persisted Stream-wide physical offset and no duplicated
`physical_end` in a StreamChunkRecord. Chunk-local physical positions derive
from append geometry; the chunk's exact seal length is authoritative at release.

### 3.2 Compact complete-append mappings

One mapping describes one complete append group, regardless of frame count.
Store append lengths, from which both logical and chunk-local physical offsets
are derived. With grouped-frame payload capacity 65,489 and overhead 47 bytes,
a group of length `L` advances physical position by
`L + 47 * ceil(L / 65489)`. A physical frame maximum of 64 KiB does not impose
a 64 KiB append maximum.

Lengths are encoded in blocks of at most 256 entries. Equal lengths use a
constant run; other blocks choose canonical variable-length u32 values or raw
u32 values by encoded size. In-memory samples store each block's logical and
physical base and encoded position. A random seek decodes at most one block.
These blocks are an encoding detail inside one record, not persisted pages or
a Stream-owned index tree.

The checksummed record validates key/start identity, purpose, exact decoded
logical coverage, group lengths and bounded physical geometry. A new checkpoint
preserves all previously published append boundaries. Rollover validates exact
continuity between the finalized predecessor and successor. Bounds detect
inconsistent coverage; CRC32C also detects same-length corruption.

### 3.3 Physical framing

All consumers use one versioned physical frame format and common Rust/C++
codecs. Each frame is at most 64 KiB including header and footer. Magic identifies
purpose and version; a common header role identifies ordinary, head, or follower
frames. Only Stream uses grouped roles. User-data and tree consumers use ordinary
frames; user-data completeness remains a whole-object publication condition.
Native buffers retain bounded streaming and zero-copy ownership.

Each nonempty Stream append is exactly one verifiable frame group in one chunk.
Its head declares total payload bytes and frame count, including the head. Even
a single-frame append is a head with count one. Followers identify their head
by its physical offset in the same chunk and carry their ordinal. Frame lengths,
roles, group identity, order, count, total payload length, CRC, and expected chunk
identity must all agree before the group is valid. There is no separate commit
frame or commit RPC. CRC32C covers the common header, group fields, and payload;
the footer chunk identity is validated separately against the expected chunk.
All codecs use the same CRC input and byte layout.
Declared lengths/counts are checked against append limits, remaining chunk
capacity, and checked arithmetic before scanning or reserving memory. A parser
does not allocate buffers proportional to an untrusted declared group size.

Role-dependent header sizes determine payload capacities. Interior frames fill
their role's payload capacity and only the last may be short. Location arithmetic
derives group geometry from the format version, role capacities, and total
logical length, without one mapping per frame.

Revision 2 retains the 14-byte fixed prefix: little-endian magic, payload offset,
payload size, and write timestamp. The low magic byte is 2; the high byte keeps
the public purpose. The role byte follows the prefix. Ordinary frames use role
0 and a 15-byte header, leaving 65,501 payload bytes. Stream heads use role 1
and add total payload length (u64) and frame count (u32); followers use role 2
and add head physical offset (u64) and ordinal (u32). Both grouped headers are
27 bytes and leave 65,489 payload bytes. Group fields are little-endian.
The 20-byte footer stores the little-endian CRC32C followed by the big-endian
chunk identity. Revision-2 CRC starts at zero and excludes the footer.

Only the canonical revision-2 frame format is accepted. Unknown revisions,
roles outside their purpose and noncanonical header offsets are rejected.

`StreamChunkRecord` uses a single current `CSCR` version-1 envelope with
exact body length and CRC32C covering header and body. Decode rejects unknown
versions, trailing bytes, invalid states, excessive counts and oversized bodies
before using mappings. No migration or older-service fallback is supported.

### 3.4 Atomic publication

A lifecycle publication conditionally updates the captured tail key revision.
Rollover atomically finalizes the predecessor, deletes its registered Candidate
reservation and inserts the new Active record under its actual logical start.
An empty predecessor and successor may share the same logical-start key; only
the successor remains attached there. Same-chunk checkpoint, trim and close
update only the tail record. Historical chunk records retain their bytes.

The first attachment reserves Candidate sequence zero, then atomically deletes
that reservation and inserts the empty Active record, guarded by the Candidate's
concrete revision. It checks that no attached tail exists after reservation.
It never uses an absent attached key as its initialization fence: that key can
be deleted by later trimming, so an excluded initializer could otherwise insert
it again. A later initializer can replace an older owner's initial reservation;
its new revision excludes the delayed publication without a mutable Stream head.

A revision conflict invalidates the captured proof. Re-read the tail and any
Candidate identity before retrying. For an unknown outcome, compare the complete
durable tail with the proposed transition; an exact match is committed. A
superseding writer/session rejects stale publication. Pin and Candidate changes
also fence through the tail revision, preventing stale retention observations
from racing publication or reclamation.

## 4. Append Protocol

Admission reserves bounded request slots and queued bytes. Oversized appends are
rejected before enqueue. An idle worker dispatches the first request immediately
and drains only requests already queued. It stops at the request limit, byte
target, or current chunk capacity, without adding a batching delay. One large
request may exceed the batch byte target while remaining within append bounds.

The normal path for an already attached chunk is:

1. Select non-overlapping logical ranges and encode one complete frame group
   per append. Keep one location per payload rather than one extent per frame.
2. Submit mirror writes concurrently, requesting synchronization in the write
   requests. The chunk layer owns strip ordering, repair images, replica
   replacement, and durable updates of its own chunk information.
3. Wait for successful synchronized completion from every required replica in
   the configured mirror set. Three-copy configuration waits for three replicas;
   no fixed replica count is assumed by the Stream API. A smaller set of readable
   replicas does not lower the write-completion requirement. The chunk layer
   restores required protection after replacement before returning success.
4. Publish the completed in-memory cursor, logical tail, and extent snapshot,
   then complete each request with its exact logical range.

There is no per-batch `advance_chunk_write`, group-0 update, Stream chunk record
write, or metadata CAS. Encoding, buffer ownership, and staging remain bounded;
frame splitting does not justify repeated payload coalescing or per-frame KV
writes. Periodic lease maintenance is separate from the append data path.

An ambiguous mirror response is not evidence of absence. Resolve it through the
chunk write/recovery contract; do not retry at a guessed offset or use an old
ChunkDB cursor to declare new bytes absent. A failure on one request cannot let
later requests skip a logical hole. Watchdogs observe the original future and
do not cancel, resubmit, or complete it. An interrupted append contributes no
partial logical range. Recovery may retain a complete append whose response was
lost, but never its proper prefix; consumer retry semantics handle that uncertain
completion. Differing valid replica tails follow section 5.2.

## 5. Rollover and Recovery

### 5.1 Lifecycle ordering

One append never straddles chunks. A chunk contains at most the configured
physical framed-byte limit, 256 MiB by default. Chunk IO may allocate physical
strips incrementally; this remains internal to the chunk layer.

Capacity preparation runs ahead of consumption. Measured write bandwidth
and preparation duration size the initialized spare capacity with a two-interval
margin. At least one initialized Candidate is allocated and registered while
the Active chunk accepts appends. Normal writes use ready capacity; exceptional
allocation/initialization delays apply backpressure instead of exposing unsafe
capacity.

When an append cannot fit, drain preparation and writes for the current chunk,
physically seal its exact final length, take the prepared Candidate, and commit
the atomic record transition. The successor accepts data only after attachment.
Switching does not allocate or rewrite historical metadata. Seal and attachment
are distinct durability boundaries: a crash after seal but before publication
leaves the one last referenced chunk to reconcile. An unattached Candidate has
no acknowledged user data and can be reclaimed after owner exclusion.

Different segment IO streams can drain concurrently, with at most eight drains
in flight. Every submitted drain must finish, including after a peer fails;
physical sealing requires all drains to succeed and current writer authority.

### 5.2 Tail reconstruction

The latest physical cursor is the end of the contiguous prefix of complete
append groups in the last referenced chunk. Recovery starts
at that chunk's beginning or a validated synchronized checkpoint and parses
forward within current authoritative capacity. It checks frame kind, lengths,
CRC, chunk identity, and address arithmetic. It tracks a tentative frame cursor
inside a group and advances the recovered physical cursor and logical tail only
after validating the entire group. It then adds one compact location.

Before publishing a fresh attachment or admitting writes into newly allocated
capacity, zero and synchronize each new physical strip on every required mirror.
Initialization uses one submission per replica, without automatic retries. An
uncertain result leaves that capacity unavailable to the writer; the owner cannot
repeat initialization after later data writes. A replacement strip receives its
complete retained prefix and a zero suffix with the same single-attempt contract
before its metadata publication.

Ordinary append writes exactly its complete group range. It never writes a marker
past that end: a delayed duplicate must not overlap the next acknowledged group.
Unwritten headers are supplied by the initialized suffix. Capacity preparation
reserves a complete 14-byte header below the hard chunk limit, growing before
writing when necessary. This keeps the previous complete prefix bounded inside
already initialized capacity if growth stops before its new strips are ready.
An exact hard-capacity end requires no extra header. Allocation, initialization
and repair traffic are accounted separately from ordinary append bytes.

The bounded repair shadow retains only the unfinished strip prefix. Validation
failures before the first data IO do not consume that owner's fresh-write
admission. Block alignment must preserve all physical bytes outside the requested
range even when the partial-block cache is absent.

An unwritten suffix, incomplete final frame, or incomplete group bounds the
candidate prefix at the previous complete append. No later group can skip that
hole. Individually valid frames in an incomplete group do not advance the tail.
Corruption in required retained data is not silently treated as EOF: use mirror
validation/repair or fail recovery. Bytes with a wrong chunk identity cannot
extend the tail. Syntactically valid bytes beyond the last checkpoint remain
candidates; a stale persisted cursor is not an authority to discard them.

The shared parser keeps one at-most-64-KiB frame and constant group/hash state.
Input windows are at most 1 MiB; output batches contain at most 1,024 complete
locations. Observed bytes and parsed frames are distinct from committed group
progress. An exact-capacity end needs no extra header read. Required grouped
checkpoints are exact append boundaries. A SHA-256 digest covers only the encoded
complete prefix; incomplete groups do not enter it. A failed observation cannot
subsequently be finalized as a healthy shorter prefix.

After owner exclusion, observe the current available mirrors concurrently
within a bounded IO budget. Use the shortest complete-append prefix among healthy
mirrors that finish observation. A mirror that fails or times out during the
read is unavailable, not a zero-length or partially observed prefix. Corrupt
replicas are not valid shorter candidates; use healthy copies or report an
error. All candidates must agree on retained overlapping contents. Conflicting
valid contents fail explicitly rather than waiting for an external arbiter.
No surviving readable candidate means recovery is unavailable. Repair restores
configured protection before the next successful append. Replica comparison and
checkpoints use complete-group boundaries, never an interior frame boundary.

Only the last referenced chunk may require content parsing. Earlier chunks
have finalized bounds and chunk record mappings and are not scanned. A last
chunk whose seal and final mappings are already published needs no tail scan.
Recovery after an interrupted seal may still reconstruct that one last chunk.
The ChunkDB finalizer may reclaim and tombstone a physically empty tail before
the Stream publishes its successor. Recovery accepts that exact tombstone only
with its seal timestamp, valid purpose and nonzero writer epoch, zero capacity,
zero sealed/acknowledged/checkpoint boundaries, and no remaining strips or cleanup
intents. It retains the logical position, leaves the tombstone unchanged, and
attaches a fresh chunk under current writer authority. A missing chunk or a
nonempty or unproven tombstone never becomes an empty prefix.
Bound reads, buffered bytes, parsed frames, and total scan bytes by the active
chunk geometry, independently of retained Stream length.

### 5.3 Writer ownership

Writer open validates purpose and current ownership before exposing a writable
handle. A lower epoch is rejected. A higher epoch adopts retained history
without copying historical chunk records, resolves the old tail, seals/finalizes it, and
attaches a fresh chunk before new appends. Writers never resume appending to a
previous process's chunk.

Group-0 owner leases remain authoritative between metadata checkpoints. The
writer uses a conservative local monotonic deadline derived from its grant and
checks authority before accepting work and before acknowledging it. Background
renewal is independent of append batches. Expiry or explicit fencing stops both
new admission and successful acknowledgment, including after a process pause.
Metadata CAS still fences lifecycle publications; DiskIO success alone does
not authorize an acknowledgment and DiskIO does not own Stream leases.

Explicit transfer stops the old owner's admission, drains its in-flight work,
persists the final synchronized boundary, and records a fenced release proof
in Group 0. Once release and the new grant are authoritative, the new owner can
activate without waiting for the old lease's natural expiry. A verbal/local
promise or an unresolved release response is not the durable release proof.

If the old owner cannot be contacted, activation waits for its lease's safe
expiry boundary, including the existing clock-skew allowance. Resolve in-flight
old writes using the bounded write-age/drain rules before fixing the old tail.
The new owner recovers that tail and publishes a newly allocated chunk; it
never resumes writing the old owner's chunk. Group-0 unavailability cannot
extend a local grant past its conservative deadline.

### 5.4 Read-only capture

Read-only open validates the requested epoch and captures the tail chunk record,
trim point, active identity, and one finite validated tail:

- When the current owner is reachable, request an ordered flush/checkpoint
  barrier. The owner captures its synchronized prefix, persists the ChunkDB
  complete-group boundary, then publishes the corresponding chunk record checkpoint
  before returning the matching epoch/chunk/physical cursor/logical end. A failed
  record publication leaves the prior checkpoint authoritative even if ChunkDB
  already holds a newer cursor. Later append does not widen that view. Already
  synchronized appends do not require another data fsync to capture the prefix.
- When the owner is unreachable, use the captured persisted ChunkDB boundary
  with matching Stream metadata. Reconstruct missing mappings only through that
  boundary without writer adoption or publication. The result is an explicitly
  older checkpoint view, possibly empty for the active chunk.

Epoch, chunk identity, chunk record/checkpoint, and end must form one consistent view;
both physical and logical checkpoint bounds end complete groups. A raw newer
ChunkDB cursor is not combined with an unrelated chunk record or epoch. Do not combine
observations across ownership changes. Expose the captured bound
and whether it came from owner flush or checkpoint fallback. A requested exact
upper cutover beyond that bound is unavailable, not a successful shorter replay.
Checkpoint fallback is a read-visibility rule: writer failover still recovers
the content-derived tail and cannot discard acknowledged data beyond it.

## 6. Reads

Reads reject ranges below `trim_offset` or beyond their captured logical tail.
For persisted data the reader reverse-seeks the greatest attached key whose
logical start is at or below the requested offset, validates coverage, and
locates its complete append through the record's compressed mapping samples.
Active unpublished data uses append-only in-memory mappings. Expand only the
intersecting physical frames and return the requested payload after validation.
Reads cross append and chunk boundaries while preserving logical order.

A sequential reader accepts a start and finite byte hint or `ToEnd`, captures
its end once, and returns EOF there. The default retained window is 8 MiB, each
physical request is at most 1 MiB, and at most eight requests run concurrently.
Independent readers may run concurrently. Observation watchdogs do not cancel
or retry underlying IO.

The lock-free record cache has 32 slots, each admitting at most 1 MiB of
encoded mappings plus seek samples. Oversized records remain readable but are
not cached. A record lookup loads only its target chunk, independently of
historical Stream length. Snapshot publication uses immutable ArcSwap views;
finite readers retain their captured view and interval lease through trim.

A provenance-aware reader yields physical chunk identity along with payload.
The journal owns business-record validation, sequence continuity, filtering,
and retry-result semantics. Physical Stream frame validation applies regardless
of consumer. Readers and overlays preserve their captured bounds through later
append, rollover, and ownership changes.

## 7. Trim and Reclamation

`trim_prefix(g)` rejects regression and `g > tail`. Persist the new trim
watermark on the tail before reclaiming data. Only complete Sealed chunks whose
logical end is at or below the watermark are eligible. Partial trim preserves
the chunk record's original bounds and mappings. The current tail is retained.

A finite reader leases its logical interval and exact Active chunk identity.
The latter protects unpublished local mappings. Pin creation, renewal and
release conditionally fence the tail revision; collection captures that revision
before observing pins. A changed revision invalidates the entire proof.
Reader pins are limited to 512 identities of at most 1 KiB each. They renew
within a 60-second lifetime and enforce an earlier local monotonic deadline.
Expired pins protect IO for a further 31 seconds before bounded cleanup.
Expiry cannot be renewed into a new live view. WAL consumers publish their
safe replay/retry watermark before trim; split/transfer checkpoint suppression
and finite captures preserve their retained dependencies.

Collection combines the trim watermark, caller-proven generation horizon,
whole-chunk state and all live interval protections. It scans a bounded prefix
with a logical-start continuation and processes at most one eligible physical
chunk per pass. Mark the record Deleting under the captured tail revision,
release the chunk using its exact derived complete physical length, and delete
the record only after release is confirmed. A failed release or delete leaves
a retryable Deleting record. Repeating trim resumes cleanup. At least one
eligible whole chunk can progress even if it exceeds the physical byte target.

A generation alone never authorizes deleting retained data. There are no
Stream directory roots, page reachability walks or historical metadata copies.

## 8. Bounds and Observation

Defaults bound admission at 1,024 requests and 64 MiB, a batch at 64 requests
and a 1 MiB byte target, one append at 64 MiB, and one chunk at 256 MiB with one compressed record
per chunk. Sequential reading uses an 8 MiB window,
eight concurrent physical requests of at most 1 MiB each, and GC uses a 64 MiB
pass budget. Bounds are configurable and validated; record/decode/cache budgets,
and optional checkpoint budgets must also bound their own work. Replica count
is configured, not implied by a physical-byte metric.

One encoded chunk record is limited to 24 MiB, and its mapping count is bounded
by the physical chunk geometry. Decode requires canonical 256-entry blocks,
except the final partial block. Collection enumerates at most 128 keys and loads
values individually under its byte target, allowing at most one final record
to cross that target; it never buffers every enumerated value at once.

Lock-free counters and phase histograms distinguish:

- append admission, queue, capacity preparation, batching, framing, synchronized
  mirror RPC completion, in-memory publication, and end-to-end completion;
- chunk-internal layout updates from Stream lifecycle publications, seal,
  ownership maintenance, and optional checkpoints;
- total chunk sealing latency and its segment-drain portion, including failures;
- chunk record build, get, scan and conditional publication latency, mapping
  counts/encoded bytes, and cache hits/misses;
- recovery chunks/bytes/frames scanned, replica resolution/repair, cursor and
  extent reconstruction, and read-only capture;
- logical/physical bytes, queue high-water marks, retained buffers/cache, read
  amplification, reclaimed bytes, orphan backlog, and watchdog observations.

Periodic perf output and cumulative benchmark output retain count, average,
p50, p99, and maximum. Phase scopes and whether counts describe requests,
batches, or replicas are explicit. Nested percentiles are not added. Ordinary
append samples must not conceal control-plane waits inside a mirror stage.
Publication counts and latency include initial attachment and writer adoption;
opening a read-only handle does not count as a publication.

Performance acceptance uses release artifacts and a matched three-node native
fixture with `mem-block` KV/WAL and retained MemDisk payload bytes. Record the
replica count, buffer/queue settings, checkpoint policy, binary identities, and
fixture parameters. The fixture removes physical-media latency without skipping
framing, transport, synchronization responses, or read validation. It proves
process/reopen behavior while retained media exists, not power-loss durability;
durability fault tests use storage that survives the tested failure.

The regression workloads and minimum gates are:

- 4 KiB append, one producer: at least 250 ops/s and p99 at most 10 ms.
- 4 KiB append, 32 producers: at least 3,000 ops/s and p99 at most 20 ms.
- 1 MiB append, eight producers: at least 125 MiB/s and p99 at most 150 ms.
- 4 MiB rollover, eight producers: at least 200 MiB/s and p99 at most 500 ms.
- 4 KiB random reads, 32 producers: at least 30,000 ops/s and p99 at most 5 ms.
- 320 MiB replay, one producer: at least 300 MiB/s and p99 at most 2 s.
- 576 MiB prefix trim: at least 1,000 MiB/s and p99 at most 100 ms.

These are regression floors, not a claim that multi-millisecond memory-backed
append latency is the performance objective. Matched before/after distributions
must show the effect of removed KV waits and the combined write/sync RPC.
Separately report standalone Stream append, actual Chunk-KV WAL, and client
mutation latency.

Sentinels require zero errors/watchdogs, bounded admission, less than 256 MiB
peak workload RSS growth, no per-append ChunkDB cursor or chunk record writes,
history-independent normal append work, and at most one tail chunk scanned on
recovery. With optional checkpoints disabled and close outside measurement,
chunk record publications do not exceed initial attachment plus rollover count. Cache
and lookup accounting follow accessed records and configured budgets. GC must
release at least one complete eligible chunk.

## 9. Errors and Invariants

`StreamError` distinguishes invalid requests, backpressure, stale writers,
definitely absent operations, unresolved outcomes, stalled writes, unavailable
reads, corruption, and internal invariant failures. Absence cannot be inferred
solely from a stale checkpoint cursor.

- **I1 — Durable completion.** A successful append has exact ordered logical
  ranges and synchronized data on every required mirror replica. Per-append
  ChunkDB cursor and chunk record updates are not completion prerequisites.
- **I2 — Bounded record work.** Historical chunk records retain their bytes;
  publication touches only the tail and successor, and open/seek loads only
  bounded target records.
- **I3 — Publication and ownership.** Data is not admitted to an unattached
  chunk; attached records describe complete append groups; epochs never regress;
  Group-0 lease expiry or release excludes stale acknowledgments/publications.
- **I4 — Bounded recovery and views.** Only the last referenced chunk can need
  parsing. Recovery preserves acknowledged bytes beyond old checkpoints and
  establishes a validated contiguous prefix. Read-only ends are finite; an
  owner-unavailable checkpoint fallback explicitly permits an older view and
  cannot silently satisfy a newer requested cutover.
- **I5 — Retention.** Logical trim precedes deletion and reclamation respects
  whole-chunk liveness, logical intervals and every retained consumer pin.
- **I6 — Consumer separation.** Wal and business streams share the same storage
  contract. The consumer owns business-record and retry-result semantics.
- **I7 — Logical addressing.** One append stays in one chunk, framing bytes never
  enter logical offsets, compatible multi-frame payloads use compact locations,
  recovery accepts the entire append or none of it, and rollover/trim never
  renumber retained bytes.
- **I8 — Chunk isolation.** Internal chunk layout and `chunkinfo` changes do not
  mutate Stream chunk record generations or copy Stream metadata.
- **I9 — Synchronized write RPC.** A synchronized write response follows both
  write and flush; it does not require a separate client fsync round trip.
- **I10 — Resource and observation bounds.** Queues, batches, buffers, record
  caches, recovery, and GC are bounded; the hot path adds no lock, and watchdogs
  observe without taking over IO completion.

## 10. Open Issues

There are no unresolved decisions for the append/recovery contract. Verifiable
bounded frame groups, all-required-mirror completion, Group-0 owner leases, and
explicit live-view bounds define the contract. Byte packing, decoder fixtures,
and rollout are implementation work under those constraints.

Metadata-group rebinding, per-stream sharding, and sealed-chunk EC conversion
remain separate scale-out topics.
