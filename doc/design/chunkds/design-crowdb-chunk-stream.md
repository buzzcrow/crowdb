<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Chunk Stream

The chunk stream presents finite mirrored chunks as one durable, ordered
logical byte space. Append completion depends on mirrored data persistence;
Stream metadata records lifecycle transitions and bounded directory checkpoints.
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

- `StreamRegistry` loads and creates the small group-0 binding. It is used by
  create/open, never by ordinary append or data reads. Binding keys are readable
  text; metadata-group selection defaults to nonzero group 1.
- `StreamMetadataStore` loads the stable manifest head and independently
  versioned directory segments. It persists newly referenced pages before
  publishing the head through KV revision compare-and-set.
- `StreamChunkStore` allocates and seals mirrored chunks, writes and synchronizes
  the required replicas, exposes current chunk layout for bounded reads and
  recovery, and idempotently releases trimmed chunks. It does not require a
  separate ChunkDB cursor advance for each append.

Chunk IO and ChunkDB own `chunkinfo`: strips, physical capacity, replica
placement, growth, replacement, and their durable internal transitions. Each
such transition updates chunk information independently. Stream metadata refers
to stable chunk identities and logical mappings; internal chunk changes are
transparent and never trigger Stream generation changes or directory rewrites.
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
rejects purpose disagreement between binding, manifest, chunk identity, and
chunk information, including a missing or mismatched owner key. Chunk-KV
journals require Wal streams.

`ProductionStreamRuntime` shares the process KV client and discovered chunk IO
routes. The Chunk-KV server chooses the metadata store; the Active binding
chooses its metadata group. `create_registered` initializes a control-plane
binding. The `test-util` implementation supplies deterministic mirror-write,
sync, tail-content, ownership, and metadata-publication fault injection.

## 3. Metadata

### 3.1 Ownership and lifetime

Group 0 stores `StreamBinding {purpose, stream_name, metadata_group_id,
binding_generation, state, owner_kind}` and authoritative owner-lease identity,
writer epoch, lease sequence, expiry, and release/transfer state. Chunk-KV
composes the Stream owner with its existing serving-grant authority instead of
creating conflicting leases. Other Stream owners obey the same Group-0 lease
contract. A nonzero KV group stores the Stream head and immutable directory
pages. Physical frames remain in mirrored chunks;
directory records are KV values, not files within those chunks.

`StreamManifest` contains purpose, stream identity, writer epoch, publication
generation, trim point, sealed logical tail, a bounded directory root, an
optional active chunk identity and logical start, and closed state. An optional
checkpoint describes an already synchronized physical/logical prefix. Neither
that checkpoint nor a cached ChunkDB cursor caps recovery of newer valid data.
An active chunk's latest cursor and mappings normally reside in memory.

Allocation and Stream reference publication precede the first append to a new
chunk. Before a successor accepts bytes, the predecessor has a durable seal and
finalized mappings in the published directory. Thus every referenced chunk
before the tail has a complete immutable description.

### 3.2 Segmented directory

The extent directory is divided into bounded segments. Completed segments are
immutable; additions change only the current tail segment. A full tail segment
is finalized and followed by a new one. Trim may separately change the retained
prefix boundary. Each page reference carries its own persisted identity and
version, independent of the head's epoch and generation. Ownership adoption or
head publication does not copy historical pages just to retag them.

A bounded-fanout index locates segments by logical range. The head holds a
bounded root, not a flat list of every segment fence. Publishing a tail change
writes only changed leaves and necessary ancestor paths. Opening a writer loads
the root and tail path, not all retained extents; reads load the paths they use.
Work follows changed segments and index height rather than the entire Stream
length.

The partitioning into bounded index pages does not require an immutable active
tail after every append. Active mappings use an in-memory snapshot. They become
durable directory entries at seal or an explicit checkpoint. In-memory view
publication also changes bounded tail pages and paths; it does not clone the
entire active or historical extent list for each append. A seal/checkpoint may
flush several newly filled segments, but it rewrites no previously completed
historical segment.

### 3.3 Extent geometry

An extent describes a logical interval and a `ChunkLocation` containing a chunk
identity, first physical frame offset, and logical payload length. One large
payload uses one extent even when encoded into several physical frames.

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
logical length, without one directory entry per frame.

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

Metadata records use a CSTR envelope with an explicit little-endian version,
record kind, zero flags and exact body length. Indexed heads and independent
pages each have one supported schema. Unknown versions/kinds, malformed lengths,
raw bodies and trailing bytes fail without selecting another schema. No format
migration or older-service fallback is supported.

The directory keeps one extent per append group and preserves its boundary.
Recovery reconstructs that boundary from the head and followers. It does not
merge groups in a way that loses append validity or use a valid follower as an
independent append.

Validation rejects empty, overlapping, unordered, or overflowing ranges,
inconsistent page references, and gaps through the retained sealed interval.
Logical and physical lengths are checked separately; physical framing overhead
never increases the caller's logical offset.

### 3.4 Publication

New pages are protected and persisted before the manifest CAS, with at most
eight page writes in flight. The retained immutable root record, stable head,
retention control and candidate-guard removal commit in one head-revision CAS
batch. A root record is therefore evidence of publication; a rejected CAS does
not create one. A partial publication leaves the previous root authoritative.
Unchanged pages retain their identities. The KV adapter may reuse the revision
from an exact prior observed or successful head CAS. Conflict or ambiguous
publication requires observing the durable head; the cache grants no authority.

Directory publication occurs at Stream lifecycle changes, including initial
chunk attachment, rollover, trim, close, and ownership changes. Optional
checkpoints may use explicit time or byte budgets. Checkpointing is independent
of append completion and never rewrites all historical segments. Chunk-internal
growth or repair does not publish a Stream checkpoint.

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

There is no per-batch `advance_chunk_write`, group-0 update, directory page
write, or manifest CAS. Encoding, buffer ownership, and staging remain bounded;
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

When the next append cannot fit, the worker stops writes to the current chunk,
waits for outstanding writes, seals it with the final physical length, and
persists its final logical mappings in the changed directory tail. It then
allocates a successor and CAS-publishes a head that includes the finalized
predecessor and the successor's identity and logical start. The successor
accepts no data before this publication succeeds. Historical sealed chunks and
completed directory segments are not updated.

Every intermediate state is recoverable. An allocation that was never attached
cannot contain acknowledged Stream bytes and is an orphan candidate. A seal
that completed before the directory CAS leaves only the last referenced chunk
to reconcile. A crash before successor publication cannot leave two chunks with
acknowledged data whose logical mappings both require content reconstruction.

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
have finalized lengths and directory mappings and are not scanned. A last
chunk whose seal and final mappings are already published needs no tail scan.
Recovery after an interrupted seal may still reconstruct that one last chunk.
Bound reads, buffered bytes, parsed frames, and total scan bytes by the active
chunk geometry, independently of retained Stream length.

### 5.3 Writer ownership

Writer open validates purpose and current ownership before exposing a writable
handle. A lower epoch is rejected. A higher epoch adopts retained history
without copying immutable pages, resolves the old tail, seals/finalizes it, and
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

Read-only open validates the requested epoch and captures the directory root,
trim point, active identity, and one finite validated tail:

- When the current owner is reachable, request an ordered flush/checkpoint
  barrier. The owner captures its synchronized prefix, persists the ChunkDB
  complete-group boundary, then publishes the corresponding directory checkpoint
  before returning the matching epoch/chunk/physical cursor/logical end. A failed
  root publication leaves the prior checkpoint authoritative even if ChunkDB
  already holds a newer cursor. Later append does not widen that view. Already
  synchronized appends do not require another data fsync to capture the prefix.
- When the owner is unreachable, use the captured persisted ChunkDB boundary
  with matching Stream metadata. Reconstruct missing mappings only through that
  boundary without writer adoption or publication. The result is an explicitly
  older checkpoint view, possibly empty for the active chunk.

Epoch, chunk identity, root/checkpoint, and end must form one consistent view;
both physical and logical checkpoint bounds end complete groups. A raw newer
ChunkDB cursor is not combined with an unrelated root or epoch. Do not combine
observations across ownership changes. Expose the captured bound
and whether it came from owner flush or checkpoint fallback. A requested exact
upper cutover beyond that bound is unavailable, not a successful shorter replay.
Checkpoint fallback is a read-visibility rule: writer failover still recovers
the content-derived tail and cannot discard acknowledged data beyond it.

## 6. Reads

Reads reject ranges below `trim_offset` or beyond their captured logical tail.
For sealed data the reader searches the bounded directory index, loads the
target segments, and resolves the `ChunkLocation`. Active reads use immutable
in-memory mappings or the reconstructed captured tail. The read path expands a
large location into necessary complete physical frames, validates them, and
returns only the requested payload. Reads can cross extent or chunk boundaries
while keeping logical delivery order.

A sequential reader accepts a start and finite byte hint or `ToEnd`, captures
its end once, and returns EOF there. The default retained window is 8 MiB, each
physical request is at most 1 MiB, and at most eight requests run concurrently.
Independent readers may run concurrently. Observation watchdogs do not cancel
or retry underlying IO.

Immutable directory-page caches use independently versioned page identities and
an explicit retention budget. Pages may be shared by multiple head generations;
head changes do not invalidate unchanged history. Snapshot publication is
lock-free and an older lookup cannot replace a newer active-tail view. Neither
opening nor the cache may eagerly materialize an unbounded historical index.

A provenance-aware reader yields physical chunk identity along with payload.
The journal owns business-record validation, sequence continuity, filtering,
and retry-result semantics. Physical Stream frame validation applies regardless
of consumer. Readers and overlays preserve their captured bounds through later
append, rollover, and ownership changes.

## 7. Trim and Reclamation

`trim_prefix(g)` rejects regression and `g > tail`. It first publishes the new
logical trim point. Only afterward may it release complete sealed chunks whose
logical end is at or below `g`. A chunk containing the boundary or retained
extents remains allocated even if individual mappings precede `g`.

Ending an active attachment publishes an immutable sealed-chunk span in the
same CAS batch as the new stable head/root and guard removal. The span records
the original logical base, complete final logical end and exact physical end.
It matches the prior attachment, the complete final append and the successor's
original logical base; a same-chunk checkpoint does not create a sealed span.
Empty chunks have zero logical/physical length. A partial logical trim preserves
the original complete span. Missing/inconsistent spans reject the transition,
and a failed head publication cannot leave a span that appears committed.

Cleanup is idempotent and bounded by `gc_bytes_per_pass`, with at least one
eligible whole chunk processed so a large chunk cannot stop progress. Released
chunks are removed from the directory through bounded changes to the retained
prefix. Repeating trim at the same watermark retries unfinished cleanup.

Directory reclamation follows reachability from current and retained roots.
An older page version can remain referenced by a newer root; generation alone
does not authorize deletion. `reclaim_metadata_before` uses the caller's
retention horizon together with live-root reachability, with a bounded deletion
budget. Orphan and unreachable-page cleanup remains safe and idempotent after
restart. A failed candidate publication does not invalidate its prior root.
In-flight candidate pages remain protected until their publication outcome is
resolved; temporary unreachability alone cannot authorize deleting them.

Reader, checkpoint, replay, retry, split/transfer, and forwarding-grace pins
constrain data and metadata reclamation. Read-only reopening is not a substitute
for a pin. Trim and cleanup never remove a byte, chunk, or shared page required
by a retained view.

Each pin stores a complete bounded directory head and a fixed interval
`[needed_offset, captured_tail]` within that head's visible bounds. Retiring an
epoch/generation root-index record does not discard an existing pin's view.
Creating a new pin identity requires that exact published root to remain in
the index; idempotent acquisition of an existing identical pin and renewal of
a live reader pin do not require that index record. Pins protect the head's
directory pages and only their captured interval for data retention.

Pin records are bounded to 1 KiB and 512 identities per stream. Durable consumer
pins have no expiry; reader pins have at most a 60-second requested lifetime.
Renewal must extend a still-live deadline. Creation, renewal and release update
the retention control in the same CAS batch, ordered against root retirement
and cleanup. A snapshot carries its pin-row revision; stale renewal or release
cannot affect a renewed pin or a reused identity. Loading metadata does not
grant an IO lease: readers separately enforce conservative local deadlines,
and collection must allow clock skew and outstanding read IO before removing
expired protection. Expiry cannot be renewed into a new live view.

Root-index retirement uses a caller-supplied generation horizon no newer than
the observed current head. Each pass examines at most 128 oldest root keys and
deletes only records below that horizon, under a retention-control CAS captured
before revalidating the head. Concurrent publication or pin changes force a
retry. Retirement changes neither the head nor directory pages or physical
chunks; page and data collection require their separate reachability proof.

Directory-page collection uses an opaque constant-size continuation for one
enumerated page. It captures retention control before validating the current
head and candidate bytes, then checks current and retained roots, every pin,
and in-flight publication protection in bounded batches. A root's logical and
level fences select at most one height-bounded path to the candidate; collection
does not materialize the tree or a historical mark set. Guards protect their
explicit new-page list and their base root even when the candidate root is not
yet readable. Until expiry grace is resolved, expired pin records conservatively
remain protection.

Each continuation step observes at most 128 protection records. A control change
invalidates partial proof and requires restart. Only completion of every stage
permits a page delete in the same control-revision CAS that advances control;
new publication or pin protection therefore cannot race with a stale deletion.
Corrupt or missing proof records fail closed. A confirmed deletion evicts the
local bounded cache entry; interrupted collection can restart without trusting
an incomplete persisted sweep. Physical chunk collection has its own span and
logical-interval proof and cannot follow from a page's generation or deletion.

Publication guards can be retired once the revalidated stable head has reached
or exceeded the candidate's generation. Generations increase strictly, so the
candidate's original head CAS can no longer win; an identical already-committed
candidate is resolved as well. Cleanup scans at most 128 guard keys with an
exclusive continuation, validates each body, and deletes eligible records under
the captured retention-control revision. It does not delete their pages. A
candidate newer than the head keeps protection until publication or independent
owner exclusion resolves its outcome; elapsed time alone grants no authority.

Page and whole-chunk collection share one bounded continuation over the same
protection set. A chunk's complete logical end must be at or below current trim.
Current/retained heads protect their visible intervals and their whole active
chunk; a pin protects only its fixed needed/captured interval. Any overlap keeps
the whole sealed chunk. Publication candidates and their base heads both protect
data. Logical trim is monotonic and allocations use fresh chunk identities, so
later views cannot reintroduce a retired prefix or append to an old sealed chunk.

A successful whole-chunk proof conditionally rewrites the unchanged span with
retention control, issuing a ticket bound to that span-row revision. The span
remains durable retry work. The chunk adapter queries current metadata and
releases only an exactly matching sealed physical boundary; active/differently
sealed chunks fail, while deleted or absent IDs are idempotent zero-byte work.
Only after successful physical release does a control-fenced batch remove the
span. Interruption before or after physical release therefore leaves retryable
work; restart reproves it and repeats the idempotent operation. A newer ticket
supersedes an older one before IO. No extra migration marker or compatibility
state is involved. Pin release must follow drained read IO; automatic expiry
collection additionally requires the clock/read-IO grace contract.

## 8. Bounds and Observation

Defaults bound admission at 1,024 requests and 64 MiB, a batch at 64 requests
and a 1 MiB byte target, one append at 64 MiB, one chunk at 256 MiB, and a
directory segment at 1,024 extents. Sequential reading uses an 8 MiB window,
eight concurrent physical requests of at most 1 MiB each, and GC uses a 64 MiB
pass budget. Bounds are configurable and validated; index fanout, cache budget,
and optional checkpoint budgets must also bound their own work. Replica count
is configured, not implied by a physical-byte metric.

Lock-free counters and phase histograms distinguish:

- append admission, queue, capacity preparation, batching, framing, synchronized
  mirror RPC completion, in-memory publication, and end-to-end completion;
- chunk-internal layout updates from Stream lifecycle publications, seal,
  ownership maintenance, and optional checkpoints;
- directory pages/bytes read, built, reused, and written, root CAS operations,
  extent count, and index height;
- recovery chunks/bytes/frames scanned, replica resolution/repair, cursor and
  extent reconstruction, and read-only capture;
- logical/physical bytes, queue high-water marks, retained buffers/cache, read
  amplification, reclaimed bytes, orphan backlog, and watchdog observations.

Periodic perf output and cumulative benchmark output retain count, average,
p50, p99, and maximum. Phase scopes and whether counts describe requests,
batches, or replicas are explicit. Nested percentiles are not added. Ordinary
append samples must not conceal control-plane waits inside a mirror stage.

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
peak workload RSS growth, no per-append ChunkDB cursor or directory writes,
history-independent normal append work, and at most one tail chunk scanned on
recovery. With optional checkpoints disabled and close outside measurement,
head publications do not exceed initial attachment plus rollover count. Cache
and lookup accounting follow accessed paths and configured budgets. GC must
release at least one complete eligible chunk.

## 9. Errors and Invariants

`StreamError` distinguishes invalid requests, backpressure, stale writers,
definitely absent operations, unresolved outcomes, stalled writes, unavailable
reads, corruption, and internal invariant failures. Absence cannot be inferred
solely from a stale checkpoint cursor.

- **I1 — Durable completion.** A successful append has exact ordered logical
  ranges and synchronized data on every required mirror replica. Per-append
  ChunkDB cursor and directory updates are not completion prerequisites.
- **I2 — Bounded directory work.** Historical segments keep their identities;
  publications touch changed boundary segments and bounded index paths, and
  open/read load only the paths they need.
- **I3 — Publication and ownership.** Data is not admitted to an unattached
  chunk; every visible root references complete pages; epochs never regress;
  Group-0 lease expiry or release excludes stale acknowledgments/publications.
- **I4 — Bounded recovery and views.** Only the last referenced chunk can need
  parsing. Recovery preserves acknowledged bytes beyond old checkpoints and
  establishes a validated contiguous prefix. Read-only ends are finite; an
  owner-unavailable checkpoint fallback explicitly permits an older view and
  cannot silently satisfy a newer requested cutover.
- **I5 — Retention.** Logical trim precedes deletion and reclamation respects
  whole-chunk liveness, shared-page reachability, and every retained consumer pin.
- **I6 — Consumer separation.** Wal and business streams share the same storage
  contract. The consumer owns business-record and retry-result semantics.
- **I7 — Logical addressing.** One append stays in one chunk, framing bytes never
  enter logical offsets, compatible multi-frame payloads use compact locations,
  recovery accepts the entire append or none of it, and rollover/trim never
  renumber retained bytes.
- **I8 — Chunk isolation.** Internal chunk layout and `chunkinfo` changes do not
  mutate Stream directory generations or copy Stream metadata.
- **I9 — Synchronized write RPC.** A synchronized write response follows both
  write and flush; it does not require a separate client fsync round trip.
- **I10 — Resource and observation bounds.** Queues, batches, buffers, index
  caches, recovery, and GC are bounded; the hot path adds no lock, and watchdogs
  observe without taking over IO completion.

## 10. Open Issues

There are no unresolved decisions for the append/recovery contract. Verifiable
bounded frame groups, all-required-mirror completion, Group-0 owner leases, and
explicit live-view bounds define the contract. Byte packing, decoder fixtures,
and rollout are implementation work under those constraints.

Metadata-group rebinding, per-stream sharding, and sealed-chunk EC conversion
remain separate scale-out topics.
