<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Small-Write IO Repair (R112)

This implementation design refines
[R112](../backlog/R112-chunkio-small-write-io-error-handling.md) against the
landed [small-object writer](../design/chunkio/design-crowdb-chunkio-small-object-writer.md),
[chunk IO](../design/chunkio/design-crowdb-chunkio.md), and
[chunkdb](../design/chunkdb/design-crowdb-chunkdb.md) contracts. The shared
writer, DiskIO routing, tentative block allocation, per-chunk lifecycle lock,
and single-strip update RPC are landed. The shared negative list, fenced range
replacement, and bounded retired-layout lifetime are not landed, so this work
introduces those reusable primitives before wiring repair into the writer.

## 1. Failure Identity and Negative List

The pipeline associates each `DiskWriter::write_at` result with the submitted
segment, so a failure retains its exact `DiskId` even when the lower-level
error is string-only. Allocation and metadata conflicts remain distinct error
variants.

One `FailedDiskList` belongs to `ChunkIoClient` and is shared by every writer
and small-write pipeline. An `ArcSwap` publishes immutable disk-to-expiry map
snapshots and returns only live entries without a read-path lock. A failed
replica is inserted before any replacement allocation.

## 2. Fenced Strip-Range Replacement

Chunk metadata gains a monotonic `next_strip_sequence` and persisted cleanup
intents. Appends consume the sequence counter instead of deriving identity
from vector length.

`ReplaceChunkStripRangeRequest` contains the chunk ID, expected revision,
start index, exact old strip fingerprints, replacement strips, and stable
operation ID. Under the existing per-chunk lifecycle lock, chunkdb:

1. verifies lifecycle state, revision, old range identity, first offset,
   first sequence, and equal logical capacity;
2. computes segment set differences and commits only new-only segments;
3. publishes the replacement and cleanup intent in one chunk record update;
4. returns success after metadata publication even when old-only cleanup is
   pending; and
5. recognizes an identical retry as already committed.

Old-only segments remain allocated until the layout-validity grace expires.
The reconciliation task retries cleanup and verifies that a segment is absent
from current metadata before freeing it. Conflicting revision or range state
maps to a typed `MetadataConflict` at the chunk client.

The existing `update_chunk_strip` client method becomes a one-strip wrapper
over this transaction.

## 3. Replacement Allocation

`AllocateReplacementSegmentRequest` supplies the owning chunk, the old
segment geometry, surviving segments, and failed/negative-list disk IDs.
Chunkdb resolves topology and asks `MirrorPlacement` for one compatible target
that preserves mirror anti-affinity. Its allocator returns one tentative
segment; the range transaction commits it.

The `ChunkAllocator` seam exposes replacement allocation, fenced publication,
and tentative discard beside ordinary lifecycle calls. The production
implementation is backed by `ChunkdbClient` RPCs.

## 4. Open-Block Shadow and Batch Repair

Each `OwnedChunk` keeps one zero-initialized full mirror-block shadow for its
current strip. Every successful or in-flight patch is copied into this shadow
at its block-relative offset. The shadow is immutable while replica writes or
repair use it and is discarded only when the strip closes or the chunk
rotates.

The foreground write ledger retains the batch, candidate locations, strip
identity, write range, and full shadow snapshot until all replicas are durable
and the cursor advance commits. Failed replicas are repaired sequentially:

1. add the failed disk to the shared negative list;
2. allocate a placement-safe replacement;
3. durably write the complete shadow to offset zero;
4. replace only the failed segment through the fenced one-strip transaction;
5. retry allocation or replacement writes with a new target up to
   `repair_attempts_per_replica`; an ambiguous metadata response retries the
   identical request and operation ID without allocating another target.

Healthy segment identities and previously acknowledged bytes never change.
Object completions are fanned out only after every replica is durable and the
cursor is fenced. Exhaustion fails the batch and every accepted queued object
exactly once, removes the pipeline from routing, and lets the manager restore
the configured minimum.

## 5. Rotation, Drain, and Conversion Boundary

Repair completes before cursor advance, strip close, chunk seal, scale-in
retirement, or replacement-chunk use. Failure in a new chunk cannot modify a
sealed predecessor. An unused prepared chunk is deleted during retirement.

The durable closed-strip marker is the conversion ownership boundary. A later
conversion and foreground repair starting from the same revision serialize on
the range transaction; exactly one wins and the loser receives a conflict.

## 6. Metrics

`SmallWriteMetrics` adds atomic counters for repair attempts, repaired
replicas, negative-list hits, exhausted repairs, failed objects, pipeline
replacements, and repairs avoiding rotation, plus shadow-byte and active-repair
gauges and repair latency accumulation/maxima. No blocking lock is added to
the write path.

## Scope

- `lib/crowdb-protocol/src/types/chunkdb.rs`: replacement, cleanup, sequence,
  and layout-validity wire types.
- `lib/crowdb-protocol/src/fbs/chunkdb.fbs` and `msg_type.fbs`: RPC schema.
- `lib/crowdb-chunkdb-client/src/`: replacement allocation/range methods and
  typed error preservation.
- `app/crowdb-chunkdb/src/allocator.rs`, `lifecycle/`, and `service/`: placement,
  fenced replacement, cleanup reconciliation, and RPC handlers.
- `lib/crowdb-chunk-client/src/error.rs`, `traits.rs`, `negative_list.rs`,
  `writer/small_pipeline.rs`, `writer/small_pool.rs`, and `metrics.rs`: repair
  state machine, shadow ownership, shared exclusions, and observability.
- Chunkdb lifecycle/RPC tests and chunk-client unit/integration/E2E tests.
- Permanent chunkdb and chunkio designs after verification.

## Complexity

High. Correctness spans durable metadata idempotency, tentative allocation,
segment lifetime, physical write identity, shared batching, and concurrent
pipeline retirement. The main risk is freeing a segment still visible to a
reader or publishing a location after only partial mirror durability.

## Test Design

1. Unit: open a mirror block, append multiple batches, close it, and assert the
   full shadow contains acknowledged prefix/current bytes and is released only
   at close.
2. Integration: fail one new-strip replica, repair it, and assert one new-only
   commit, unchanged healthy replicas, full durability, and all batch results.
3. Integration: patch a strip with acknowledged prefix, fail one replica, and
   assert replacement bytes preserve prefix and current batch offsets.
4. Integration: fail two replicas and assert sequential replacement from the
   same immutable shadow before completion.
5. Integration: exhaust allocation, write, and metadata retries separately;
   assert the batch and queued objects resolve once, pipeline retirement, and
   restoration to `min_pipelines`.
6. Lifecycle integration: replace N strips with M capacity-compatible strips,
   assert monotonic sequences, set-difference commit/free, stale-revision
   conflict, identical retry success, and deferred cleanup.
7. Restart integration: persist a replacement with cleanup pending, restart
   chunkdb, reconcile after grace, and assert only old-only blocks are freed.
8. E2E: use the real simple cluster to write a prefix and batch, inject one
   DiskIO failure, observe repair to a different disk, then read every mirror
   and verify data plus metrics.
9. E2E: rotate chunks, fail the first new-chunk batch through retry exhaustion,
   and assert predecessor locations remain readable.
10. Integration: begin drain during repair and assert repair finishes before
    seal or every accepted completion fails before worker exit.
11. Integration: race repair and a conversion-shaped range replacement from
    one revision; assert one commit and one typed conflict without cross-free.

## Module Structure

```text
lib/crowdb-chunk-client/src/
├── negative_list.rs             # shared TTL disk exclusions
├── traits.rs                    # lifecycle and replacement seams
└── writer/small_pipeline.rs     # shadow, ledger, repair state machine
app/crowdb-chunkdb/src/
├── allocator.rs                 # replacement placement/allocation
├── lifecycle/handler.rs         # fenced range transaction
└── service/chunkdb_rpc_service/ # replacement RPC handlers
```

## Config Extensions

- `failed_disk_ttl`: live exclusion duration.
- `repair_attempts_per_replica`: bounded attempts, nonzero.
- `layout_validity_grace`: minimum retired-segment lifetime.

The small-write budget must cover one full shadow per maximum pipeline in
addition to queued object reservations.

## Server Wiring

Chunkdb registers allocate-replacement and replace-range RPCs beside existing
lifecycle mutations. Startup reconciliation resumes expired cleanup intents.
The chunk client constructs one negative list and passes it to all pool
pipelines.
