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
batching, fenced durable cursors, elastic pipelines, and orphan sealing.

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

The shared path does not perform erasure coding or reclaim abandoned object
ranges. Mirror-to-EC conversion and in-chunk reclamation consume its durable
strip and cursor metadata as later background work.

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

A pipeline starts a deadline when it receives the first object, then collects
whole objects until reaching the byte limit, object-count limit, strip space,
or deadline. An object larger than the normal batch target is written alone as
long as it is within the object limit.

Fragments are copied once into a zero-filled buffer aligned to the mirror
segment unit. Each returned location covers only its object's exact logical
bytes; alignment padding belongs to no object. Segment-relative writes validate
offset and length alignment, overflow, and segment bounds before reaching
DiskIO.

If the next object does not fit the strip, the worker zero-fills the tail,
durably closes the strip, and appends or enters the next strip. If it does not
fit the chunk, the worker seals the current chunk and switches to its prepared
replacement, or allocates one on demand. Objects and batches never straddle a
strip or chunk.

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

The batch commit barrier is:

1. Write the aligned batch concurrently to every mirror and await all durable
   completions.
2. Advance the fenced chunk cursor, including a newly closed strip when needed.
3. Replace the worker's local chunk revision and cursor from the response.
4. Publish all object-specific locations together.

No location is visible before its complete physical range and durable metadata
prefix exist on every configured mirror.

## 6. Chunk Lifecycle and Recovery

A pipeline allocates its first chunk before publication. The worker alone owns
the chunk value, epoch, revision, and cursor. It appends mirror strips as the
cursor advances. When remaining capacity falls below the object limit, it
prepares at most one replacement so ordinary rotation does not wait for
allocation.

Retirement seals a non-empty current chunk at its acknowledged cursor and
deletes an empty current or replacement chunk. A write or metadata failure
fails the affected batch and every already accepted queued object, removes the
route, and retires its chunks.

Chunkdb periodically scans Active shared chunks. When a persisted writer lease
has expired, it acquires the normal lifecycle guard, rechecks the record, seals
at the persisted cursor, closes only acknowledged strips, persists, and
refreshes the cache. The old writer can no longer renew or advance the sealed
chunk.

## 7. Elasticity, Failure, and Shutdown

One manager owns pipeline membership. It publishes a scale-out candidate only
after the candidate's first chunk is ready; initialization failure leaves the
old snapshot intact. A route whose queued bytes or queued object count reaches
its configured high-water mark can add one pipeline after a cooldown.
Scale-out depends only on queued work, not worker utilization or request age.
When the whole pool has no queued or active work, a sufficiently idle route
can be unpublished and drained while preserving the configured minimum.

Unexpected worker termination removes the failed route and creates replacements
until the minimum is restored. Explicit shutdown closes admission, unpublishes
all routes, drains accepted work, joins all workers, and seals or deletes their
owned chunks. Dropping the pool closes admission but cannot await cleanup.

## 8. Policy and Metrics

Defaults accept objects and batches up to 1 MiB, reserve 64 MiB pool-wide, use
one to 32 pipelines, allow 1,024 queued objects per pipeline, and scale out
when one route queues at least 4 MiB or 128 objects. Shared chunks have a 1 GiB
client-side capacity and write three mirrors. Configuration validates nonzero
bounds, reachable queue high-water marks, a budget at least as large as the
object limit, ordered pipeline limits, and progress-capable durations.

Atomic metrics cover submitted, completed, and failed objects; reserved bytes;
batch sizes and fill; queue delay; active and draining pipelines; scale changes;
and strip-tail waste. Snapshots compute aggregates without locking submission.

## 9. Correctness Invariants

- **SW-I1 — Whole-object reservation.** No object input is accepted before its
  complete declared size is reserved from the pool budget.
- **SW-I2 — Single acceptance.** A submission racing retirement is accepted by
  exactly one draining receiver or returned intact for rerouting.
- **SW-I3 — Exclusive ownership.** Exactly one live pipeline epoch can advance
  a shared chunk.
- **SW-I4 — Whole placement.** No object or batch crosses a strip or chunk
  boundary.
- **SW-I5 — Durable acknowledgement.** Object locations are published only
  after every mirror write and the fenced cursor commit succeed.
- **SW-I6 — Monotonic prefix.** The acknowledged cursor moves strictly forward,
  and recovery seals only at that persisted prefix.
- **SW-I7 — Terminal completion.** Every accepted object completes or fails
  exactly once; caller cancellation may discard delivery but not batch work.
