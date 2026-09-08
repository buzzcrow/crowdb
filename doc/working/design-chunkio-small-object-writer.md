<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk IO Small-Object Shared-Chunk Writer (R106)

This implementation design depends on the landed
[chunk IO data-path design](../design/chunkio/design-crowdb-chunkio.md),
[chunkdb lifecycle design](../design/chunkdb/design-crowdb-chunkdb.md), and
[R106 requirement](../backlog/R106-chunkio-small-object-writer.md). R94, R105,
and the chunkdb allocate/append/seal/delete lifecycle are present. R93 and R112
remain later integrations and are not required for foreground correctness.

## 1. Object-Scoped Ingress

`ChunkIoClient::prepare_small_write(object_size)` validates the configured
policy, rejects an object above `object_limit`, and awaits one whole-object
permit from `SmallWritePool` before returning a `SmallObjectWriter`. Reserving
before any input is accepted prevents fragmented writers from each holding a
partial reservation while waiting for the rest.

`SmallObjectWriter` stores the declared size, an `OwnedSemaphorePermit`, a
vector of caller-owned `Bytes`, the retained byte count, and an `Open` or
terminal state. Its production constructor is crate-private:

```rust
pub async fn ChunkIoClient::prepare_small_write(
    &self,
    object_size: usize,
) -> Result<SmallObjectWriter>;

pub(crate) fn SmallObjectWriter::new(
    pool: Arc<SmallWritePool>,
    object_size: usize,
    reservation: OwnedSemaphorePermit,
) -> Self;
```

`on_data` accepts a buffer only when its complete length fits the declared
remainder. Success retains the buffer and returns `Continue` until the declared
size is reached, then `Pause`. An overflow drops all retained fragments and the
reservation, sets the terminal state, and returns
`IoError::ObjectSizeMismatch`. `on_finish` similarly rejects underflow. A
correct non-empty object becomes one immutable `PendingObject`; an empty object
completes locally. `on_error` drops unsubmitted input and completes locally.
Every later method returns `IoError::Finished`.

The trait contract permits a terminal validation error to reject the offending
buffer. A successful `on_data` still always retains the complete buffer.
`require_data` is true only while the writer is open and declared bytes remain.

Cancellation after submission does not cancel or reshape a physical batch.
The worker completes the durable batch and sends the result through a oneshot;
if the receiver was dropped, the range becomes reclaimable in-chunk garbage.
This preserves the batch barrier without a pending-object commit record.

## 2. Policy and Pool Lifecycle

`ChunkIoClientConfig` owns a `SmallWritePolicy`. The policy contains:

```rust
pub struct SmallWritePolicy {
    pub object_limit: usize,
    pub memory_budget: usize,
    pub queue_capacity: usize,
    pub min_pipelines: usize,
    pub max_pipelines: usize,
    pub max_batch_bytes: usize,
    pub max_batch_objects: usize,
    pub batch_deadline: Duration,
    pub scale_out_delay: Duration,
    pub scale_in_delay: Duration,
    pub control_interval: Duration,
    pub cooldown: Duration,
    pub chunk_capacity: u64,
    pub mirror_copies: u32,
    pub writer_lease: Duration,
}
```

Defaults use a 1 MiB object limit and batch target, a 64 MiB pool budget,
1--8 pipelines, bounded 1,024-object queues, and 1 GiB shared chunks composed
of 1 MiB mirror strips. Validation requires nonzero limits, budget at least the
object limit, ordered pipeline bounds, a batch-object bound, and durations
suitable for progress.

Each `ChunkIoClient` constructs one `Arc<SmallWritePool>` from its allocator and
disk writer. Clones retain that same `Arc`. `from_parts` uses the same default
policy as `connect`; a parts-based policy override is exposed for focused tests.
The pool starts its manager lazily through an atomic start state. Concurrent
first callers wait on a watch channel until the manager has prepared and
published `min_pipelines`, or receive the shared initialization error.

The pool owns a closed flag, byte semaphore, routing generation, atomically
published `Arc<Vec<Arc<PipelineRoute>>>`, manager command sender, and atomic
metrics. Shutdown closes admission, asks the manager to retire all workers, and
awaits their joins through an explicit async method; `Drop` only closes
admission because it cannot await metadata cleanup.

## 3. Lock-Free Routing and Drain Boundary

`SmallWritePool::submit` loads the routing snapshot once per attempt. The
normal policy selects two candidates from an atomic pseudo-random sequence and
chooses the lower `queued_bytes`; a test-only round-robin policy selects one
deterministically. It increments a selected route's queued counters before
`try_send`. A full queue retries another route after yielding; a closed queue
restores the counters from the returned unchanged `PendingObject`, reloads the
snapshot, and reroutes it. No mutex or `RwLock` is used on admission or
submission.

Each route contains only a bounded MPSC sender and atomics. The worker owns the
receiver. For retirement, the manager first publishes a snapshot without the
route and then signals the worker. The worker calls `Receiver::close`, defining
the acceptance boundary, drains accepted entries, and exits. A send racing the
old snapshot is therefore either accepted and drained or rejected with the
same object for rerouting.

## 4. Whole-Object Batching

`SmallPipeline` waits for the first object, then drains whole objects until the
batch reaches `max_batch_bytes`, `max_batch_objects`, the open mirror strip's
remaining logical space, or `batch_deadline`. The deadline begins with the
first object; an empty queue does not count as queue latency for scaling.

Objects are copied once into a zero-filled physical batch buffer because one
durable disk write must aggregate fragments from unrelated callers. Each
object descriptor records its response channel, chunk byte offset, and exact
logical length. The physical buffer begins at the current aligned cursor and
is padded to the strip unit size. Padding is not included in a `Location`.
Objects larger than `max_batch_bytes` are written alone.

If the next object does not fit the current 1 MiB strip, the pipeline flushes
the current batch, advances the cursor to the strip end using deterministic
zero tail padding, marks the strip closed, and appends or enters the next
strip. If it does not fit the chunk, the worker seals the current non-empty
chunk, switches to its prepared replacement, and places the whole object
there. An object and batch never straddle strips or chunks.

## 5. Segment-Relative Durable Writes

`DiskWriter` gains `write_at(seg, unit_bytes, segment_offset, data)`. The
existing `write` method delegates with offset zero. Shared writes validate that
the offset and data length are aligned to `unit_bytes`, arithmetic does not
overflow, and the end is at most `seg.unit_count * unit_bytes`. Production
DiskIO routing adds `segment_offset` to the segment's base byte offset. Invalid
ranges fail before a diskio request.

For a mirror strip, the pipeline writes the same physical buffer to every
segment concurrently and waits for all replicas. It retains all `PendingObject`
data until those writes and the metadata cursor commit succeed. A failed
mirror write fails the current batch plus every accepted queued object exactly
once, unpublishes the pipeline, and retires it. Previously acknowledged ranges
are unchanged.

## 6. Fenced Shared-Chunk Cursor

`Chunk` adds `writer_epoch`, `acknowledged_cursor`,
`closed_strip_sequence`, and `writer_lease_deadline_ms`. Zero epoch denotes a
dedicated or unfenced chunk. `AllocateChunkRequest` optionally installs a
nonzero epoch and lease duration. The new lifecycle operation is:

```rust
pub struct AdvanceChunkWriteRequest {
    pub chunk_id: Option<ChunkId>,
    pub writer_epoch: u64,
    pub expected_modify_ts: u64,
    pub acknowledged_cursor: u64,
    pub closed_strip_sequence: u32,
    pub writer_lease_ms: u64,
}

pub struct AdvanceChunkWriteResponse {
    pub chunk: Option<Chunk>,
}
```

Under the existing per-chunk lifecycle guard, chunkdb requires `Active`, an
equal nonzero epoch, an equal expected revision, a strictly forward cursor no
greater than capacity, and a nondecreasing valid closed-strip sequence. It
increments the revision, updates cursor and closed markers, renews the lease
from server time, persists, and refreshes the cache. A stale epoch or revision
returns `StateConflict`; a backward or out-of-capacity cursor is invalid.

The pipeline's batch commit barrier is:

1. write the aligned range durably to all mirror replicas;
2. call `advance_chunk_write` with the resulting physical cursor and any newly
   closed strip;
3. update the local revision/cursor from the response; and
4. publish every object-specific `Location` together.

No location is sent before step 2 commits. Later batches start at the returned
cursor, so they cannot overwrite an acknowledged prefix.

An orphan sweep periodically lists Active chunks carrying a nonzero epoch. If
the persisted lease deadline has expired, it acquires the normal lifecycle
guard, rechecks the record, sets `Sealed`, sets `sealed_length` to the persisted
cursor, closes only strips wholly before or containing that cursor, persists,
and refreshes the cache. A stale writer cannot renew or advance the sealed
chunk. Closed-strip metadata is the durable boundary consumed by mirror-to-EC
conversion.

## 7. Chunk Ownership and Preparation

Each pipeline generates a random nonzero writer epoch and allocates a Repo
chunk with one 1 MiB mirror strip before publication. The worker alone owns the
chunk value, revision, cursor, and optional replacement chunk. It appends one
strip at a time as the cursor reaches the next strip and starts preparing one
replacement when remaining chunk capacity falls below the object limit.

Retirement seals a chunk with acknowledged bytes and deletes an empty allocated
chunk. It then seals or deletes a prepared replacement by the same rule. No
chunk ID can be advanced by two pipelines because epochs are unique and chunk
state never transfers between workers.

## 8. Elastic Manager

The manager is the sole owner of the live pipeline vector. Every control tick
samples route atomics: queued bytes, oldest queued timestamp, busy time, batch
fill, and a disk saturation hint. It scales out by one only after queue age and
busy state stay high for `scale_out_delay`, the cooldown has elapsed, and the
pool is below `max_pipelines`. The candidate is published only after its first
chunk is ready. Initialization failure leaves the old snapshot untouched.

It scales in by one only when the pool exceeds `min_pipelines`, a route has no
queued bytes and has remained inactive for `scale_in_delay`, and cooldown has
elapsed. It unpublishes before signaling drain and awaits retirement. Worker
termination is also reported to the manager; it unpublishes the failed route,
fails accepted work through the worker cleanup path, and creates replacements
until `min_pipelines` is restored.

## 9. Metrics

`SmallWriteMetrics` uses atomics for submitted, completed, failed, reserved
bytes, batches, batch objects/bytes, queue delay, active/draining pipelines,
scale changes, and tail waste. Snapshots compute totals, averages, and current
gauges without draining counters or taking a blocking lock. Callers may attach
the pool metrics to `ChunkClientMetrics`; focused tests inspect a direct
snapshot.

## Scope

- `lib/crowdb-chunk-client/src/client.rs`: shared pool ownership and public
  preparation/shutdown API.
- `lib/crowdb-chunk-client/src/config.rs`: `SmallWritePolicy` and validation.
- `lib/crowdb-chunk-client/src/error.rs`: object-limit and size-mismatch errors.
- `lib/crowdb-chunk-client/src/io.rs`: validation exception in the push contract.
- `lib/crowdb-chunk-client/src/traits.rs`: fenced cursor lifecycle seam.
- `lib/crowdb-chunk-client/src/disk_io/`: segment-relative write support.
- `lib/crowdb-chunk-client/src/writer/small_object.rs`: object ingress state.
- `lib/crowdb-chunk-client/src/writer/small_pool.rs`: reservation and routing.
- `lib/crowdb-chunk-client/src/writer/small_pipeline.rs`: batching and chunk owner.
- `lib/crowdb-chunk-client/src/writer/small_manager.rs`: elasticity and drain.
- `lib/crowdb-chunk-client/src/metrics.rs`: small-write atomic metrics.
- `lib/crowdb-protocol/src/types/chunkdb.rs`: cursor metadata and request types.
- `lib/crowdb-protocol/src/fbs/chunkdb.fbs`: cursor RPC wire schema.
- `lib/crowdb-protocol/src/fbs/msg_type.fbs`: cursor RPC message IDs.
- `lib/crowdb-protocol/src/fb_wrappers/chunkdb.rs`: cursor response wrapper.
- `lib/crowdb-chunkdb-client/src/`: cursor transport and client method.
- `app/crowdb-chunkdb/src/lifecycle/`: fenced advance and orphan sealing.
- `app/crowdb-chunkdb/src/service/chunkdb_rpc_service/`: cursor RPC handler/wire.
- `app/crowdb-chunkdb/src/main.rs`: orphan-sweep wiring.
- `lib/crowdb-chunk-client/tests/`: ingress, aggregation, routing, failure, and
  shutdown coverage.
- `app/crowdb-chunkdb/tests/`: fencing and orphan-seal integration coverage.

## Complexity

High. The implementation crosses the public client API, a lock-free routed
async worker pool, physical alignment and batching, a new durable lifecycle
fence, generated FlatBuffers, server background recovery, and failure fan-out.
Correctness depends on preserving ownership and acknowledgement boundaries
across all of them.

## Test Design

1. Ingress unit tests prepare bounded handles, feed fragmented and overflowing
   inputs, finish underflow/empty inputs, abort, and call terminal handles;
   assert one submission at exact size, precise errors, and full permit release.
2. Pool unit tests exhaust the byte semaphore and race stale snapshots with
   receiver close; assert whole-object waiting, unchanged reroute, and exactly
   one completion. Deterministic routing verifies client clones share a pool.
3. Pipeline integration tests enqueue 64 16 KiB objects and mixed object sizes;
   assert batch counts, exact non-overlapping locations, object-local logical
   ranges, deadline flushing, oversized-alone batches, strip/chunk rotation,
   deterministic tail padding, and direct replica readback.
4. Commit-barrier tests delay one mirror and the advance operation independently;
   assert no completion is visible early. A second batch verifies the first
   acknowledged prefix remains byte-identical.
5. Disk writer unit tests exercise misaligned offsets/lengths, overflow, and
   segment bounds; assert zero downstream requests on validation errors.
6. Chunkdb lifecycle tests allocate with an epoch, advance monotonically, reject
   stale epochs/revisions/backward cursors, expire the lease across a reconstructed
   handler, seal at the persisted cursor, and expose only persisted closed strips.
7. Manager tests drive paused time through scale-out, initialization failure,
   cooldown/max bounds, scale-in/drain, worker failure replacement, and shutdown;
   assert snapshot membership and exact accepted-object outcomes.
8. Metrics tests update every event concurrently and assert monotonic atomics,
   consistent gauges, and distributions without a submission-path lock.
9. E2E tests use real chunkdb/diskdb/diskio fixtures to write shared objects,
   query metadata, and read exact ranges from healthy replicas.

## Module Structure

```text
lib/crowdb-chunk-client/src/
├── client.rs                 # client-owned pool and public factory
├── config.rs                 # small-write policy
├── disk_io/
│   ├── disk_writer.rs        # validated segment-relative write
│   └── routing.rs            # disk-ID route with relative offset
├── metrics.rs                # atomic small-write metrics
└── writer/
    ├── small_object.rs       # one declared object and reservation
    ├── small_pool.rs         # admission, snapshot, route/retry
    ├── small_pipeline.rs     # batch, mirror write, cursor commit, rotation
    └── small_manager.rs      # membership controller and worker joins

app/crowdb-chunkdb/src/
├── lifecycle/
│   └── handler.rs            # fenced advance and orphan seal
└── service/chunkdb_rpc_service/
    ├── mutations.rs          # advance request handler
    └── wire.rs               # response and error mapping
```

## Config Extensions

The small policy is library-owned and has safe defaults. It does not add a
server CLI knob. Chunkdb reuses the lifecycle sweep cadence for expired writer
leases, avoiding a second independent scan interval. A zero writer epoch keeps
existing dedicated-chunk callers wire-compatible.

## Server Wiring

The chunkdb crowdb-rpc service registers one new request message and dispatches
it to `LifecycleHandler::advance_chunk_write`. The existing lifecycle sweep task
invokes orphan sealing after idle-lock reaping. Client routing and retry reuse
the same chunk-ID endpoint selection as append and seal.

## Open Questions

None. Cancellation uses durable completion plus abandoned-range reclamation,
which is the only option that preserves the v1 batch barrier without adding a
new persistent per-object protocol.
