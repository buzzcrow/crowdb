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
whole objects, writes the physical range to every mirror, durably advances the
chunk cursor, then returns an independent `Location` to each caller.

The shared path can incrementally form 8+4 EC groups while writing mirrors.
It retains one open-strip image and four parity accumulators, but never retains
eight completed data images. Incomplete or failed groups remain mirrored and
can be completed by later background conversion.

## 2. Admission and Object Handles

`prepare_small_write(object_size)` validates the policy and object limit, then
reserves the object's complete declared size from a pool-wide byte semaphore.
It never reserves a partial object. Client clones share the same pool and
budget.

The returned single-use writer retains caller-owned byte fragments. Successful
input is never partially accepted. Overflow or underflow is terminal, drops
retained fragments, releases the reservation, and returns an exact size error.
An empty object completes locally without starting the pool. Abort before
submission also completes locally and releases all resources.

Finishing an exact non-empty object transfers its fragments, reservation, and
completion channel to the pool. Cancellation after that transfer does not
cancel or reshape a physical batch: the worker completes the durable operation,
and an undeliverable result leaves a reclaimable range in the shared chunk.

## 3. Routing and Pipeline Ownership

Admission reads an immutable `ArcSwap` route snapshot. Power-of-two selection
chooses the less loaded of two routes using atomic queued-byte counters. Each
route contains a bounded MPSC sender and atomics; the submission path takes no
mutex or read-write lock.

A send reserves route counters before `try_send`. A full queue retries another
route. A closed queue returns the unchanged object, restores the counters, and
retries from a fresh snapshot.

For retirement, the manager publishes a snapshot without the route before it
signals the worker. The worker closes its receiver, establishing the acceptance
boundary, and drains every accepted object. A sender racing an old snapshot is
therefore either accepted and drained or rejected intact for rerouting.

## 4. Batching and Physical Layout

A pipeline waits for the first object only while it has no work. After the
previous batch completes, it immediately drains whole objects that are already
queued until reaching the byte limit, object-count limit, or strip space. It
never delays an admitted object to wait for a batching timer. Concurrent work
naturally accumulates behind the in-flight batch and is aggregated on the next
completion-driven drain. An object larger than the normal batch target is
written alone as long as it is within the object limit.

Each pipeline reserves and retains one uninitialized-capacity 1 MiB shadow for its open
mirror strip. Object fragments are packed contiguously at exact logical
offsets. Each physical update sends only the newly written byte range to every
mirror; DiskIO aligns partial physical blocks and preserves their prior bytes.
The retained prefix remains available for repair and mirror-to-EC conversion.
`Bytes` clones shared by concurrent writes are reference-counted; the worker
recovers the unique mutable shadow after completion without another
steady-state copy.
Each returned location covers only its object's exact bytes.

Chunk allocation reserves a bounded group of hidden strips. A reserved strip
becomes `Consumed` immediately before its first DiskIO and is confirmed into
the readable layout only after its mirror write succeeds. Confirmation and
refill run on the background metadata chain; reserve failures fall back to a
bounded batch of already attached mirror strips. Seal cancels never-consumed
reservations and removes attached strips beyond the written length. Objects
and batches never straddle a strip or chunk.

When automatic conversion is enabled and at least eight strips remain, one
special reservation allocates eight three-copy mirror sets and four parity
segments from a joint placement plan. Each completed strip updates the four
incremental parity accumulators and releases its input image. After strip eight,
the client writes and fsyncs only the parity segments. ChunkDB then reselects
one healthy survivor per mirror set against current topology and atomically
publishes the 8+4 EC strip. If optimal publication is unavailable, mirrors stay
authoritative and the ordinary durable conversion task is admitted.

The pool divides its 96 MiB memory ceiling evenly between ordinary object and
shadow admission and conversion parity. One pool-global atomic gate admits at
most one foreground conversion group. A new group waits only for the previous
group's parity permits to be returned; a group larger than the reserved half is
left mirrored for background conversion. Published EC capacity is divided by
its data width when deriving the next reservation, so consecutive groups keep
the same per-shard geometry.

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

1. Append the object bytes to the open-strip shadow and write that byte range
   concurrently to every mirror.
2. Repair each failed replica from that shadow and fence the new segment into
   chunk metadata or, while it remains hidden, into its durable reservation.
3. Publish all object-specific locations together.
4. Coalesce cursor progress in the background metadata chain. Strip close and
   batched append execute there in revision order.

No location is visible before its complete physical range exists on every
configured mirror. Cursor persistence is an asynchronous availability and
orphan-recovery checkpoint; readers can transiently report `NotYetAvailable`
until it catches up.

## 6. Chunk Lifecycle and Recovery

A pipeline allocates its first chunk and its initial strip batch before
publication. The worker owns the write cursor while one background metadata
chain owns revision-ordered cursor commits and batched strip appends. When
remaining chunk capacity falls below the object limit, it prepares at most one
replacement so ordinary rotation does not wait for allocation.

Closing a strip releases its shadow immediately. Retirement seals a non-empty
current chunk at its acknowledged cursor and deletes an empty current or
replacement chunk. A write or metadata failure
fails the affected batch and every already accepted queued object, removes the
route, and retires its chunks.

Chunkdb periodically scans Active shared chunks. When a persisted writer lease
has expired, it acquires the normal lifecycle guard, rechecks the record, seals
at the persisted cursor, cancels and frees never-consumed reservations, closes
only acknowledged strips, persists, and refreshes the cache. New consumed
reservations persist each planned cursor and send the segment allocation
generation with DiskIO writes. Recovery can therefore cancel and recycle them:
DiskIO durably advances the allocation generation before reuse and rejects a
delayed old write. Legacy consumed reservations without planned cursors remain
allocated fail-safe. The old writer can no longer renew or advance the sealed
chunk.

## 7. Elasticity, Failure, and Shutdown

One manager owns pipeline membership. It publishes a scale-out candidate only
after the candidate's first chunk is ready; initialization failure leaves the
old snapshot intact. A route whose queued bytes or queued object count reaches
its configured high-water mark can add one pipeline, up to 32 by default.
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

Defaults accept objects and batches up to 1 MiB, reserve 96 MiB pool-wide, use
one to 32 pipelines, allow 1,024 queued objects per pipeline, and scale out
when one route queues at least 4 MiB or 128 objects. Shared chunks have a 1 GiB
client-side capacity and write three mirrors. Configuration validates nonzero
bounds, reachable queue high-water marks, ordered pipeline limits, and a
budget covering one 1 MiB shadow per maximum pipeline plus one admitted
maximum-size object. The retained `scale_in_delay` and `cooldown` fields are
configuration-compatible but do not participate in scale decisions.

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

- **SW-I1 — Whole-object reservation.** No object input is accepted before its
  complete declared size is reserved from the pool budget.
- **SW-I2 — Single acceptance.** A submission racing retirement is accepted by
  exactly one draining receiver or returned intact for rerouting.
- **SW-I3 — Exclusive ownership.** Exactly one live pipeline epoch can advance
  a shared chunk.
- **SW-I4 — Whole placement.** No object or batch crosses a strip or chunk
  boundary.
- **SW-I5 — Physical completion.** Object locations are published only after
  every mirror write succeeds; metadata availability advances asynchronously.
- **SW-I6 — Monotonic prefix.** The acknowledged cursor moves strictly forward,
  and recovery seals only at that persisted prefix.
- **SW-I7 — Terminal completion.** Every accepted object completes or fails
  exactly once; caller cancellation may discard delivery but not batch work.
- **SW-I8 — Repair publication.** A replacement segment is written from the
  complete shadow and fenced into metadata before any affected location is
  published.
- **SW-I9 — Queue-only elasticity.** Pipeline membership changes are driven by
  queued bytes, queued objects, and empty/non-busy state, never elapsed time.
- **SW-I10 — Hidden reservation.** A reserved or consumed strip is never
  readable through `Chunk.strips`; only fenced confirmation publishes it.
- **SW-I11 — EC publication.** Mirror replicas remain authoritative until four
  parity segments are durable and one current-topology survivor from every
  mirror set is atomically published.
