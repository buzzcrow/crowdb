<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Small-Object Shared-Chunk Writer

The small-object write path batches independent objects into shared mirrored
chunks while preserving per-object completion, bounded memory, and a durable
prefix after process loss.

Depends on: [chunk IO data path](design-crowdb-chunkio.md),
[chunkdb lifecycle](../chunkdb/design-crowdb-chunkdb.md), and
[diskio](../diskio/design-crowdb-diskio.md).

Satisfies: bounded small-object admission, lock-free routing, shared-chunk
batching, fenced durable cursors, in-place mirror repair, elastic pipelines,
and orphan sealing.

## Table of Contents

- [1. Scope and Concepts](#1-scope-and-concepts)
- [2. Admission and Object Handles](#2-admission-and-object-handles)
- [3. Routing and Pipeline Ownership](#3-routing-and-pipeline-ownership)
- [4. Batching and Physical Layout](#4-batching-and-physical-layout)
- [5. Durable Cursor and Completion](#5-durable-cursor-and-completion)
- [6. Chunk Lifecycle and Recovery](#6-chunk-lifecycle-and-recovery)
- [7. Elasticity, Failure, and Shutdown](#7-elasticity-failure-and-shutdown)
- [8. Policy and Metrics](#8-policy-and-metrics)
- [9. Correctness Invariants](#9-correctness-invariants)

## 1. Scope and Concepts

One client-owned pool multiplexes small objects across a bounded set of
pipelines. Each pipeline exclusively owns one active Repo chunk containing
mirror strips and may own one empty prepared replacement. A pipeline batches
whole objects and writes the physical range to every mirror. Ordinary completion
returns independent locations while cursor progress runs asynchronously; callers
can opt into completion after the readable cursor is durably confirmed.

The shared path can incrementally form 8+4 EC groups while writing mirrors.
It retains one open-strip image and four parity accumulators, but never retains
eight completed data images. Incomplete or failed groups remain mirrored and
can be completed by later background conversion.

## 2. Admission and Object Handles

`prepare_small_write_for_key(object_size, key)` validates the policy and uses
the complete tenant/bucket/object identity as a preferred pipeline hint. The
single-frame interface charges bytes as immutable input fragments arrive.
`prepare_shared_object_write_for_key` atomically reserves the complete declared
payload on an available pipeline before receiving it. It probes the other
routes when the preferred route cannot reserve enough capacity. Neither path
uses a global or per-tenant semaphore.

The returned single-use writer retains caller-owned byte fragments. Successful
input is never partially accepted. Overflow or underflow is terminal, drops
retained fragments, releases the reservation, and returns an exact size error.
An empty object completes locally without starting the pool. Abort before
submission also completes locally and releases all resources.

Finishing an exact non-empty object transfers its fragments, byte charge, and
completion channel to the pool. Cancellation after that transfer does not
cancel or reshape a physical batch: the worker completes the durable operation,
and an undeliverable result leaves a reclaimable range in the shared chunk.

## 3. Routing and Pipeline Ownership

Admission reads an immutable `ArcSwap` route snapshot. The hash selects the
first candidate; a full or closed route causes probing of other candidates
before waiting. Moving a retained byte charge reserves capacity on the target
atomically before releasing the previous route's charge. Failed transfers keep
the object intact. The submission path adds no mutex or read-write lock.

When all routes are unavailable, the caller waits for a capacity notification
with a control-interval fallback that also observes newly published routes.
Socket admission leaves the body unread until retained-byte capacity is
reserved, so TCP applies backpressure. Queue capacity and byte reservations
bound pending work independently.

For retirement, the manager publishes a snapshot without the route before it
signals the worker. The worker closes its receiver, establishing the acceptance
boundary, and drains every accepted object. A sender racing an old snapshot is
therefore either accepted and drained or rejected intact for rerouting.

## 4. Batching and Physical Layout

A pipeline waits for the first object only while it has no work. After the
previous batch completes, it immediately drains whole objects that are already
queued until reaching the byte limit, object-count limit, or remaining chunk space. It
never delays an admitted object to wait for a batching timer. Concurrent work
naturally accumulates behind the in-flight batch and is aggregated on the next
completion-driven drain. An object larger than the normal batch target is
written alone as long as it is within the object limit.

Native small uploads allocate one exact-sized receive owner containing payload
and all frame regions. The receive task can fill it across multiple socket
reads, prepares frame checksums in place, and submits the complete owner once.
The worker sets each frame's Chunk ID after placement. Generic immutable input
fragments are framed with separate header/footer views without copying payload.

Objects are packed consecutively and may span mirror strips within one chunk.
The worker slices shared `Bytes` views at strip boundaries and sends only each
new range to DiskIO through scatter/gather writes. It does not rotate merely
because a frame or object reaches the strip boundary. Every returned location
covers one object's consecutive physical frames.

The current strip retains immutable prefix views for repair. Ordinary mirror
success creates no continuous strip shadow. Repair materializes the complete
prefix only after a failure; EC conversion materializes the closed strip image
for parity computation. Closing a strip releases its retained views. A view
crossing the boundary can retain its object's owner until the final open-strip
prefix is released, so memory headroom includes a maximum-sized owner per pipe.

### Transport descriptor bound

A strip aggregate may retain more views than one RPC frame permits. The DiskIO
semantic client validates the entire segment range, then submits consecutive
bounded scatter/gather frames at increasing offsets. Disjoint ranges within
one batch overlap at depth at most four, limited by semantic admission. An
error stops further submissions and drains pending completions before repair.
Views keep their original owners; this does not coalesce payload. Requested fsync follows the complete
range once, and cursor publication follows successful completion of all frames.
A descriptor bound cannot be treated as a disk failure or trigger repair.


A caller can attach a `SmallWriteIntent` to durable completion. Once the batch
has assigned exact locations, every attached callback completes before any of
the batch's DiskIO. Failure aborts the batch and its pipeline; cancellation of
the caller does not detach ownership registration from the physical operation.
The callback has no default storage policy. Legacy tree FileIO uses it for a
catalog-sharded block ledger; native streamed files rely on chunk allocation
ownership and Chunk-KV WAL. Range reclamation defers active shared-chunk ranges
that extend beyond the acknowledged cursor, preserving uncertain writes.

Chunk allocation reserves a bounded group of hidden strips. A reserved strip
becomes `Consumed` immediately before its first DiskIO and is confirmed into
the readable layout only after its mirror write succeeds. Confirmation runs on the ordered metadata chain. Allocation/refill has its
own background task and does not delay that chain; reserve failures fall back to a
bounded batch of already attached mirror strips. Seal cancels never-consumed
reservations and removes attached strips beyond the written length. Objects and batches may straddle strips, but never chunks. Mirror-only groups
contain 32 strips by default; at 16 remaining strips a new group is allocated
asynchronously after the full preceding reservation, including hidden strips.
The client verifies the returned append offset before accepting a refill; an
older backend that ignores this field fails safely rather than overlapping
hidden strips. Upgrade ChunkDB before enabling this client prefetch behavior.
Resource allocation releases the existing chunk lifecycle guard; publication
reacquires it and revalidates ownership and sequence. No extra persistent
high-water record is required.

When automatic conversion is enabled and at least eight strips remain, one
special reservation allocates eight mirror sets with the configured copy
count and four parity segments from a joint placement plan. Each completed
strip updates the four incremental parity accumulators and releases its input
image. After strip eight,
the client writes and fsyncs only the parity segments. ChunkDB then reselects
one healthy survivor per mirror set against current topology and atomically
publishes the 8+4 EC strip. If optimal publication is unavailable, mirrors stay
authoritative and the ordinary durable conversion task is admitted.

The configured pool memory budget subtracts one maximum-sized retained owner (at least 1 MiB) per maximum
pipeline and one foreground conversion group, then divides the remaining
buffer capacity between possible routes. Conversion is a cold-path bounded
operation and may use its own permit; ordinary object admission never does.
Published EC capacity is divided by its data width when deriving the next
reservation, so consecutive groups keep the same per-shard geometry.

## 5. Durable Cursor and Completion

Every shared chunk carries a nonzero writer epoch, acknowledged physical byte
cursor, optional closed-strip sequence, writer lease deadline, and metadata
revision. Cursor advancement executes under chunkdb's existing per-chunk
lifecycle guard and requires:

- an Active chunk and matching nonzero writer epoch;
- the caller's expected metadata revision;
- a strictly increasing cursor within chunk capacity; and
- a valid, nondecreasing closed-strip marker.

The operation updates the cursor and marker, renews the lease from server time,
increments the revision, persists the record, and refreshes the cache. Stale
epochs or revisions conflict; backward or out-of-range cursors are invalid.

The response barrier is:

1. Frame the owner in place and write immutable slices concurrently to every
   mirror, splitting views at strip boundaries when necessary.
2. Repair each failed replica from the retained prefix and fence the new segment into
   chunk metadata or, while it remains hidden, into its durable reservation.
3. Publish all object-specific locations together.
4. Coalesce cursor progress in the background metadata chain. Strip close and
   batched append execute there in revision order.

The physical mirror-strip flow is shared with chunk streams: it writes mirrors
in parallel, excludes failed disks, writes a prefix-complete replacement image,
and publishes the fenced strip swap. Each single-owner caller retains its own
current-strip prefix and controls its publication barrier. Journal streams also
fsync the final mirror set before advancing their durable cursor and resolve an
uncertain replacement result against chunk metadata before retrying it.

No location is visible before its complete physical range exists on every
configured mirror. Cursor persistence is an asynchronous availability and
orphan-recovery checkpoint; readers can transiently report `NotYetAvailable`
until it catches up.

Callers publishing immediately readable immutable authorities use
`SharedObjectWriter::finish_durable`. A batch containing a durable-completion
request waits for the existing metadata chain, then confirms any remaining
cursor suffix before delivering locations. Metadata failure fails that batch
instead of exposing an unreadable reference. The pipeline does not start its
next batch before this barrier completes. Arrivals queue during the wait and
form the next batch; multiple pipelines provide concurrency. Shared-object
writers require this barrier by default. The single-frame `on_finish` retains its
asynchronous cursor behavior; no additional lock or reader-side retry is needed.

## 6. Chunk Lifecycle and Recovery

A pipeline allocates its first chunk and its initial strip batch before
publication. The worker owns the write cursor while one background metadata
chain owns revision-ordered cursor commits and batched strip appends. When
remaining chunk capacity falls below the larger of one prefetch group and the
object limit, it starts at most one asynchronous replacement allocation so ordinary rotation does not wait for allocation.

Idle workers renew the current chunk and any ready replacement at half the
writer lease interval. A matching live chunk writer lease also protects hidden
reservation groups, avoiding a separate renewal write for every group.

Closing a strip releases its prefix views immediately. Retirement seals a non-empty
current chunk at its acknowledged cursor and deletes an empty current or
replacement chunk. A write or metadata failure
fails the affected batch and every already accepted queued object, removes the
route, and retires its chunks.

Chunkdb periodically scans Active shared chunks. When a persisted writer lease
has expired, it acquires the normal lifecycle guard, rechecks the record, seals
at the persisted cursor, cancels and frees never-consumed reservations, closes
only acknowledged strips, persists, and refreshes the cache. Recovery retains
consumed reservations until the writer lease deadline plus the configured
reuse grace has elapsed, so a timed-out write cannot reach a recycled extent.
Consumed reservations without planned cursors remain allocated fail-safe. The
old writer can no longer renew or advance the sealed chunk. DiskIO performs raw
I/O and does not validate allocation ownership.

## 7. Elasticity, Failure, and Shutdown

One manager owns pipeline membership. It publishes a scale-out candidate only
after the candidate's first chunk is ready; initialization failure leaves the
old snapshot intact. When all routes reach their queue or byte-capacity high-water marks, the
manager can add one pipeline, up to 32 by default.
Scale-out depends only on queued work, not worker utilization, request age, or
elapsed idle time. When the whole pool has no queued or active work, an extra
route can be unpublished and drained while preserving the configured minimum.

Unexpected worker termination removes the failed route and creates replacements
until the minimum is restored. Explicit shutdown closes admission, unpublishes
all routes, drains accepted work, joins all workers, and seals or deletes their
owned chunks. Dropping the pool closes admission but cannot await cleanup.

A mirror write failure records the submitted segment's disk in one client-wide,
TTL-based lock-free negative list. Repair allocation excludes all live failed
disks and the nodes holding surviving replicas. The worker writes the complete
shadow to a tentative replacement, then publishes a one-strip fenced metadata
swap. Allocation and replacement-write failures may select a new target within
the bounded attempt count. An ambiguous metadata result retries the identical
operation ID and replacement, so it cannot allocate or install a duplicate.
A definite conflict discards the tentative block when it is not referenced.
Exhaustion fails the batch once, records an unavailable replica for an already
acknowledged prefix, and retires the pipeline for background recovery.

## 8. Policy and Metrics

Defaults accept objects up to 8 MiB and target batches up to 1 MiB, budget
1.25 GiB pool-wide, use one to 32 pipelines, and allow 1,024 queued objects per
pipeline. Queue high-water marks are 4 MiB or 128 objects. Shared chunks have
a 256 MiB client-side capacity, prefetch 32 strips per ordinary group, and use
two mirrors. Foreground conversion uses the configured data-width group and
its joint parity placement; single-node deployments disable conversion and
select one mirror explicitly. The access service routes by the logical data
capacity of a strip multiplied by its threshold ratio, not socket read size.

Configuration validates nonzero bounds, reachable queue high-water marks,
ordered pipeline limits, and a budget covering retained owners, one conversion
group, and request buffers. The retained `scale_in_delay` and `cooldown` fields
are configuration-compatible but do not participate in scale decisions.

Foreground parity writes use a dedicated connection per DiskIO endpoint and
64 KiB byte-range requests, followed by a parity-only fsync barrier. Ordinary
mirror traffic retains its configured connection pool and request sizing.

Atomic metrics cover submitted, completed, and failed objects; reserved bytes;
batch sizes and fill; queue delay; active and draining pipelines; scale changes;
strip-tail waste; and batch-watchdog expirations. The watchdog reports a batch
that remains in flight beyond its configured interval, then continues awaiting
the same durability future. It neither delays queue draining nor cancels an
operation whose physical or metadata outcome may already be committed. The
default 500 ms observation cadence matches the RPC pending-request reaper
scan interval; underlying RPC clients retain their own 5- or 10-second request
timeouts. Repair metrics expose attempts, repaired and exhausted
replicas, failed-disk exclusions, active pipelines, pipeline replacements, repairs
that avoided rotation, complete-shadow bytes, and repair latency totals and
maxima. Snapshots compute aggregates without locking submission.

## 9. Correctness Invariants

- **SW-I1 — Incremental bounded input.** Received object frames are charged to
  one route at a time; shared-object admission atomically reserves its declared
  payload, and rerouting preserves bounded accounting.
- **SW-I2 — Single acceptance.** A submission racing retirement is accepted by
  exactly one draining receiver or returned intact for rerouting.
- **SW-I3 — Exclusive ownership.** Exactly one live pipeline epoch can advance
  a shared chunk.
- **SW-I4 — Whole placement.** An object remains in one chunk; views can cross strips without
  introducing padding or changing its consecutive frame layout.
- **SW-I5 — Physical completion.** Object locations are published only after
  every mirror write succeeds. Native FileIO additionally waits for readable
  cursor publication; generic callers can select physical-only completion.
- **SW-I6 — Monotonic prefix.** The acknowledged cursor moves strictly forward,
  and recovery seals only at that persisted prefix.
- **SW-I7 — Terminal completion.** Every accepted object completes or fails
  exactly once; caller cancellation may discard delivery but not batch work.
- **SW-I8 — Repair publication.** A replacement segment is written from the
  complete retained prefix and fenced into metadata before any affected location is
  published.
- **SW-I9 — Queue-only elasticity.** Pipeline membership changes are driven by
  queued bytes, queued objects, and empty/non-busy state, never elapsed time.
- **SW-I10 — Hidden reservation.** A reserved or consumed strip is never
  readable through `Chunk.strips`; only fenced confirmation publishes it.
- **SW-I11 — EC publication.** Mirror replicas remain authoritative until four
  parity segments are durable and one current-topology survivor from every
  mirror set is atomically published.
