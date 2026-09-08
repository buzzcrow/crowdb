<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R106: chunkio — Small-Object Shared-Chunk Writer

**Problem**

The landed chunk I/O writer stores one large object in dedicated chunks.
`SmallObjectWriter` exists only as a placeholder that returns
`IoError::Internal`. Sending each small object through the large-object
path wastes chunk capacity and chunkdb metadata. Writing every object
directly to diskio also turns a high-TPS workload into small, poorly
aggregated I/O.

The small-object path must multiplex independent object writes onto
fewer shared chunks. Its core is an elastic producer/consumer pipeline:

- callers declare the bounded object size and feed one object through
  one `ChunkIoWriter` handle;
- completed objects enter a process-local shared pool;
- workers aggregate multiple whole objects into larger physical writes;
- each worker exclusively owns one active shared chunk;
- the worker pool scales out under sustained queueing and scales in
  after demand falls; and
- every caller receives its own `Vec<Location>`, even when its bytes
  share a chunk and physical write with other objects.

The object-facing and shared-pipeline responsibilities must remain
separate. `ChunkIoWriter::on_data` feeds fragments of one object and
`on_finish` returns that object's locations. Routing individual
`on_data` fragments would allow one object to reach several pipelines
and prevents cross-caller aggregation. The object handle must retain
one bounded object and submit it exactly once at `on_finish`; the
shared pool performs routing, batching, chunk placement, and completion
fan-out.

**Current behavior + impact**:
`lib/crowdb-chunk-client/src/writer/small_object.rs` returns
`IoError::Internal` from every operation. Applications can only use the
large-object writer, allocating one chunk per small object. There is no
shared admission budget, cross-object batch, elastic pipeline pool, or
independent shared-chunk location result.

**Design pointers**: chunkio design §2 (push contract, bounded
preparation, proto types), §4 (backpressure and memory budget), §6.1
(`Location`), §7 (completion and failure), and §8 (chunkdb/diskio
boundaries). chunkdb design §5.2 (mirror strips), §5.3 (chunk
semantics), §5.5 (Repo type), §8 (`allocate_chunk` and
`append_chunk`), and §9 (Active →
Sealed lifecycle). R93 owns background mirror-to-EC conversion. R107
reads the per-object locations produced here. R112 adds small-write
error handling to the batch and pipeline boundaries defined here.

**Use scenarios**:

- **Concurrent small objects**: 1,000 callers each feed and finish a
  16 KiB object. The handles submit 1,000 completed objects to the
  shared pool. Workers combine them into approximately 1 MiB batches
  and write those batches to shared chunks. Every caller receives only
  its own location.

- **Independent locations in one batch**: objects A (4 KiB), B
  (16 KiB), and C (64 KiB) enter one physical batch in chunk X. They
  receive three locations with the same `chunk_id`, distinct
  non-overlapping `offset`/`length` ranges, and object-local
  `logical_offset = 0`. Reading one location returns only that
  object's bytes.

- **Fragmented object input**: one caller supplies a 64 KiB object in
  four `on_data` calls. The handle retains all four fragments under one
  byte reservation and submits one immutable object at `on_finish`.
  The fragments cannot be routed to different pipelines.

- **Scale out under sustained queueing**: all current workers remain
  busy and the oldest queued object exceeds the scale-out delay. The
  manager initializes one new pipeline and its first shared chunk,
  then publishes it for routing. Already accepted objects stay with
  their original pipeline.

- **Scale in after a burst**: one pipeline remains idle past the
  scale-in delay. The manager removes it from the routing snapshot,
  closes its receiver, drains entries accepted before the close
  boundary, seals its non-empty chunk, and retires it.

- **Chunk rotation near capacity**: objects A and B fit in the current
  chunk but C does not. The worker writes A and B, seals the chunk, and
  writes all of C to its prepared replacement. The batch is partitioned
  only between objects; no small object straddles chunks.

- **Sparse traffic**: one object arrives without peers. The batch
  deadline flushes it without waiting indefinitely. Waiting to fill a
  batch does not by itself trigger scale-out.

**Solution**

**One-line approach**: implement per-object `SmallObjectWriter` handles
backed by a bounded, lock-free-routed pool of elastic, single-owner
shared-chunk pipelines.

**Numbered work items**:

1. **Per-object ingress**
   (`lib/crowdb-chunk-client/src/writer/small_object.rs`) — implement
   `SmallObjectWriter` as one object-scoped `ChunkIoWriter`. `on_data`
   retains caller-owned `Bytes` fragments under the object's permits.
   `on_finish` verifies the declared size and moves all fragments into
   one immutable `PendingObject`, submits it once to the shared pool,
   and waits for that object's completion. The handle never selects a
   pipeline per
   fragment and never owns a chunk.

2. **Ingress limits and lifecycle** (`writer/small_object.rs` and
   `lib/crowdb-chunk-client/src/io.rs`) — clarify the shared trait
   contract: `on_data` waits for capacity and retains the complete
   buffer when it returns `Ok`, but may reject the offending buffer
   with a terminal validation error. Use a 1 MiB v1 hard limit and
   allow policy to choose a lower routing cutoff. Reject a declared
   size above that cutoff before creating a handle.
   Add `IoError::ObjectTooLarge` and `IoError::ObjectSizeMismatch` in
   `lib/crowdb-chunk-client/src/error.rs`. Supplying more than the
   declaration or finishing with fewer bytes releases all permits,
   enters a terminal error state, and returns the size-mismatch error;
   it does not migrate a partially buffered object to the large writer.
   `on_error` before submission releases the object and returns no
   locations. Empty `on_finish` returns an empty location array without
   entering the pool. Calls after finish, abort, or terminal error
   return `IoError::Finished`.

3. **Client-owned pool, public API, and routing**
   (`lib/crowdb-chunk-client/src/client.rs`, `config.rs`, and
   `writer/small_pool.rs`) — add `SmallWritePolicy` to
   `ChunkIoClientConfig` and an async
   `ChunkIoClient::prepare_small_write(object_size)` factory. Validate
   the declared size against both the small-object limit and total pool
   budget, then reserve its full byte count atomically before returning
   the handle. This avoids partial-reservation deadlock between
   fragmented concurrent objects. Unknown-size inputs use the large
   path or a caller-side bounded buffering adapter. Each client owns
   exactly one lazily started `Arc<SmallWritePool>` and manager task;
   client clones and every returned object handle share it. Restrict
   the low-level `SmallObjectWriter` constructor so production callers
   cannot accidentally create isolated pools. Test construction from
   low-level seams takes the same policy explicitly. The pool owns the
   atomically published routing
   snapshot, per-pipeline atomic load counters, bounded queues, and
   total byte budget. Route each completed object once, using
   power-of-two choices over queued bytes; retain round-robin as a
   deterministic test policy. Submission performs a snapshot load and
   bounded MPSC send, without a mutex or `RwLock`.

4. **Routing during drain** (`writer/small_pool.rs`) — when a stale
   snapshot points to a closed draining queue, the failed send returns
   the unchanged `PendingObject`. Reload the snapshot and retry another
   running pipeline. The queue close is the acceptance boundary:
   entries accepted before it are drained by the retiring worker;
   entries rejected after it are rerouted. The worker that exclusively
   owns Tokio's MPSC receiver calls `Receiver::close()` after a manager
   retirement signal; the manager awaits worker termination. No entry
   is duplicated or silently dropped.

5. **Batch aggregation and object map**
   (`lib/crowdb-chunk-client/src/writer/small_pipeline.rs`) — drain
   whole `PendingObject`s until `max_batch_bytes`,
   `max_batch_objects`, or `batch_deadline` is reached. Assemble one
   aligned physical buffer within the remaining range of one 1 MiB
   mirror block and
   retain one descriptor per object with
   its exact chunk offset and logical length. Physical alignment and
   tail padding are outside every object's range. An admitted object
   larger than the batch target is written alone; the target is not an
   object-size limit. If the next whole object does not fit the open
   block, flush the current batch, close that block with deterministic
   tail padding, and place the object in the next block. Neither an
   object nor a batch straddles mirror blocks.

6. **Shared-chunk write and completion fan-out**
   (`writer/small_pipeline.rs` and
   `lib/crowdb-chunk-client/src/disk_io/disk_writer.rs`, plus
   `lib/crowdb-protocol/src/types/chunkdb.rs`,
   `lib/crowdb-protocol/src/fbs/chunkdb.fbs`,
   `lib/crowdb-chunkdb-client/src/`, and the chunkdb lifecycle
   handler) — extend
   `DiskWriter` with a segment-relative byte offset and reject writes
   whose aligned physical range exceeds the segment. The existing
   zero-offset call remains a convenience for whole-block writers.
   Each pipeline prepares one `ChunkType::Repo` chunk with 1 MiB mirror
   blocks before entering
   `Running`. Shared versus dedicated is a client-side packing and
   ownership policy, not a wire-level chunk type. The pipeline
   exclusively owns the chunk and its append cursor. Append enough
   strips before writing a batch. Retain one logical object map; later
   batches write at the current segment-relative cursor and
   cannot overwrite an acknowledged prefix. Add a fenced
   `advance_chunk_write` lifecycle RPC and chunk metadata for writer
   epoch, acknowledged physical cursor, closed-strip sequence, and
   lease deadline. Allocation installs the pipeline's epoch; advances
   require that epoch and the expected chunk revision, move only
   forward, and renew the lease under chunkdb's existing per-chunk
   lifecycle lock; no new client-side lock is added. After lease
   expiry, chunkdb seals the orphan at its persisted cursor; R106 never
   adopts another epoch's
   Active chunk. Marking a strip closed is the proof R93 uses for
   conversion. A batch has one commit barrier: publish none of its
   object locations until every physical range and mirror replica is
   durable and the cursor advance is durably committed. This RPC is
   owned by R106 and does not depend on R112's replacement operation.
   The objects in one successful batch receive independent results:

   ```text
   A -> Location { chunk_id: X, offset: 0,     length: 4 KiB,
                   logical_offset: 0, logical_length: 4 KiB }
   B -> Location { chunk_id: X, offset: 4 KiB, length: 16 KiB,
                   logical_offset: 0, logical_length: 16 KiB }
   C -> Location { chunk_id: X, offset: 20 KiB, length: 64 KiB,
                   logical_offset: 0, logical_length: 64 KiB }
   ```

   Each non-empty small object returns exactly one location. Locations
   may share a `chunk_id`, but their byte ranges never overlap.

7. **Chunk preparation and rotation**
   (`writer/small_pipeline.rs`) — keep at most one replacement shared
   chunk prepared. Admit only whole objects into the current chunk's
   remaining capacity. If the next object does not fit, flush the
   current batch, seal the non-empty chunk, switch to the replacement,
   and place the object there. Delete an allocated chunk that retires
   while still empty. Tail waste is bounded by the configured
   small-object limit.

8. **Scale controller**
   (`lib/crowdb-chunk-client/src/writer/small_manager.rs`) — use one
   async manager task as the sole owner of pipeline membership. On each
   control interval, sample queued bytes, oldest queue age, worker busy
   time, batch fill, and diskio saturation:

   - scale out by one when queue delay stays above the high watermark,
     workers remain busy, and diskio is not saturated;
   - publish a new pipeline only after its worker, queue, and first
     shared chunk are ready;
   - scale in by one when a candidate stays empty and inactive past the
     low-watermark window and the pool exceeds `min_pipelines`;
   - publish a snapshot without the candidate, signal its worker to
     close the receiver and transition from `Running` to `Draining`,
     drain accepted entries, seal its non-empty chunk, stop it, and
     await termination;
   - preserve `min_pipelines` and `max_pipelines`; and
   - apply a cooldown after each membership change to prevent flapping.

   Queue age is the primary latency signal; queued bytes prevent object
   count from hiding byte pressure. Batch fill is diagnostic because
   adding pipelines reduces aggregation.

9. **Backpressure and memory accounting** — validate that the pool
   budget is at least the configured small-object cutoff. Reserve the
   declared object's full size in `prepare_small_write` and hold the
   permit until completion, abort, or terminal failure. Bound each
   pipeline queue and the whole pool's retained bytes. A successful
   `on_data` follows the always-store contract within that reservation;
   a size-mismatch error retains neither the offending buffer nor
   earlier fragments. `require_data` reports whether the handle is open
   and has declared bytes remaining. Pool pressure delays handle
   preparation rather than allowing fragmented handles to deadlock.
   Scaling changes throughput capacity, not the memory ceiling.

10. **Baseline failure boundary** — retain object bytes until all
    required mirrors durably complete. On allocation failure, diskio
    failure, or worker panic, stop routing to the affected pipeline and
    complete its in-flight batch and accepted queued entries with one
    error each. Do not reassign accepted work because a partial physical
    write makes replay ambiguous. Already acknowledged locations remain
    valid. Restore `min_pipelines` for future objects. R112 adds
    replacement, retry, negative-list, and escalation behavior.

11. **Metrics** (`lib/crowdb-chunk-client/src/metrics.rs`) — expose
    submitted/completed/failed small objects, reserved bytes, batches
    written, batch object/byte distributions, batch fill ratio, queue
    delay, active/draining pipeline counts, scale-out/scale-in totals,
    and chunk tail-waste bytes. Submission and worker hot paths update
    atomic counters; histogram collection must not introduce a
    blocking lock there.

**Edge-case outcomes**:

- Empty object → return `Vec<Location>::new()`; allocate and write
  nothing.
- Declared object crosses the configured limit → return
  `IoError::ObjectTooLarge` before creating a handle.
- Supplied bytes differ from the declared size → release retained bytes,
  return `IoError::ObjectSizeMismatch`, and submit nothing.
- Declared object exceeds the pool budget → reject handle preparation
  before reserving or accepting data.
- Object exceeds `max_batch_bytes` but not the object limit → write it
  alone and return one location.
- Current chunk cannot hold the next object → rotate before that
  object; never split it across chunks.
- Current 1 MiB mirror block cannot hold the next object → close it with
  deterministic tail padding and place the whole object in the next
  block of the same chunk.
- Batch deadline expires below target size → write the partial batch.
- New pipeline initialization fails → keep the existing routing
  snapshot and continue with the current pool.
- Scale-in races a sender → drain sends accepted before queue close;
  reroute the unchanged object after a rejected send.
- Worker fails after accepting objects → R106 fails those objects
  exactly once; R112 may repair and resume before exposing the failure.
- Pool byte budget is exhausted → another handle waits in
  `prepare_small_write`; existing handles can finish without acquiring
  more permits, and memory remains bounded.
- Pool shutdown → stop admission, signal workers to close receivers,
  drain or fail accepted objects, await every worker, and seal or
  delete each pipeline's chunk according to whether it contains
  acknowledged data.

**Flow diagram**:

```text
 caller A -> SmallObjectWriter A --\
 caller B -> SmallObjectWriter B ---+-> SmallWritePool routing snapshot
 caller C -> SmallObjectWriter C --/                 |
                                                   | bounded MPSC
                      +----------------------------+------------------+
                      |                                               |
                      v                                               v
            Pipeline 1 (Running)                            Pipeline N (Running)
            queue: A, B, C                                  queue: D, E
            owns chunk X                                    owns chunk Y
                      |                                               |
                      v                                               v
            batch [A | B | C]                               batch [D | E]
                      |                                               |
                      v                                               v
            durable mirror write                            durable mirror write
                      |                                               |
              +-------+-------+                              +--------+--------+
              |       |       |                              |                 |
              v       v       v                              v                 v
           Loc[A]  Loc[B]  Loc[C]                         Loc[D]            Loc[E]

 manager: Starting -> Running -> Draining -> Stopped
          scale out publishes initialized Running pipelines
          scale in unpublishes before queue close, drain, and seal
```

**Dependencies**

- **Depends on**:
  - **R94 / landed chunkio interface** — `ChunkIoWriter`,
    `ProtoLocation`, `ChunkAllocator`, and `DiskWriter`.
  - **R105 / diskio** — durable aligned mirror writes.
  - **chunkdb lifecycle (landed, R85)** — allocate, append, seal, and
    delete operations for Repo chunks with mirror strips; R106 adds the
    fenced writer-epoch/cursor advance and orphan-seal operation.
- **Integrates with**:
  - **R93** converts completed mirror strips to EC. R106 remains
    correct with sealed mirror chunks before R93 lands but uses 3×
    space until conversion is available.
  - **R112** repairs small-write failures within R106's batch and
    pipeline boundaries. Without R112, R106 fails the affected batch
    and pipeline but does not publish an unsafe location.
- **Depended on by**: **R107** reads the independent location arrays.
  The in-chunk GC requirement may reclaim an object range abandoned
  after submission.

**Acceptance**

**Object ingress**:

- Given one `ChunkIoClient` and handles prepared by two client clones,
  when both handles finish, assert both objects enter the same pool and
  can appear in one batch. Integration test.
- Given an open handle and four 16 KiB `Bytes` fragments, when
  `on_finish` is called, assert the pool receives one immutable 64 KiB
  object and the handle returns only that object's result. Unit test.
- Given a handle declared as 64 KiB but only 32 KiB is supplied, when
  `on_finish` is called, assert `IoError::ObjectSizeMismatch`, no pool
  submission, and full permit release. Unit test.
- Given a configured 1 MiB object limit, when a 2 MiB handle is
  prepared, assert `IoError::ObjectTooLarge`, zero reservation, and no
  handle or pool submission. Unit test.
- Given a handle declared as 64 KiB and retained fragments below it,
  when the next fragment crosses the declaration, assert
  `IoError::ObjectSizeMismatch`, zero pool submissions, and full byte-
  permit release. Unit test.
- Given an open handle with no data, when `on_finish` is called, assert
  an empty location array and no chunk allocation. Unit test.
- Given retained fragments that have not been submitted, when
  `on_error` is called, assert an empty location array, no pool
  submission, and full byte-permit release. Unit test.
- Given a finished, aborted, or terminal-error handle, when any writer
  method is called again, assert `IoError::Finished`. Unit test.

**Aggregation and independent locations**:

- Given one pipeline and 64 completed 16 KiB objects before the batch
  deadline, when the worker drains its queue, assert one 1 MiB logical
  batch and 64 single-location completions. Integration test.
- Given 4 KiB, 16 KiB, 64 KiB, and 256 KiB objects in one batch, when
  the mirrors durably complete, assert four non-overlapping locations
  with exact lengths, `logical_offset = 0`, and
  `logical_length = length`. Integration test.
- Given one physical range in a multi-range batch is still pending or
  fails, when earlier ranges finish, assert no object in that batch is
  acknowledged before the batch-wide commit barrier. Integration test.
- Given every mirror write completes but `advance_chunk_write` has not
  committed, when callers await the batch, assert no location is
  published; after the fenced cursor commit, all object completions are
  released. Integration test.
- Given one acknowledged batch followed by another batch in the same
  mirror strip, when the second batch writes at the saved cursor,
  assert the first batch remains byte-identical and the new locations
  begin after its aligned physical range. Integration test.
- Given less space remains in the open 1 MiB block than the next
  object's declared size, when it is placed, assert the old block
  closes with deterministic tail padding and the unsplit object begins
  in the next block of the same chunk. Integration test.
- Given a segment-relative write whose offset is misaligned or whose
  end exceeds the segment, when `DiskWriter` validates it, assert the
  write is rejected before any diskio request. Unit test.
- Given an object larger than `max_batch_bytes` but within the object
  limit, when it is submitted, assert it is written alone and returns
  one location. Integration test.
- Given several objects sharing a chunk, when the test queries chunk
  metadata and reads their ranges directly from healthy replicas,
  assert every location resolves to only its original object bytes.
  E2E test.

**Ownership and rotation**:

- Given two running pipelines, when both accept objects, assert no
  chunk ID is advanced by more than one pipeline. Integration test.
- Given less remaining chunk capacity than the next object's size,
  when the worker places the object, assert the old chunk is sealed and
  the entire object receives one location in the replacement chunk.
  Integration test.
- Given an empty prepared chunk on pipeline retirement, when the worker
  stops, assert the chunk is deleted rather than sealed. Integration
  test.
- Given acknowledged locations in an Active chunk and a simulated
  process restart, when its writer lease expires, assert chunkdb seals
  the orphan at the persisted cursor, a stale epoch cannot advance it,
  acknowledged bytes are not overwritten, and R93 sees only strips
  carrying the persisted closed marker. Integration test.

**Scale out and routing**:

- Given one busy pipeline with queue age above the high watermark for
  one scale-out window, when the controller ticks, assert exactly one
  new pipeline becomes routable only after its first chunk is ready.
  Integration test.
- Given new-pipeline initialization failure, when scale-out is
  attempted, assert the routing snapshot is unchanged and current
  pipelines continue processing. Integration test.
- Given repeated high signals during the cooldown and a pool at
  `max_pipelines`, when the controller ticks, assert no extra pipelines
  are published. Unit test.

**Scale in and drain**:

- Given two pipelines and one idle past the low-watermark window, when
  the controller scales in, assert the pipeline is unpublished, its
  queue closes, accepted entries finish, its non-empty chunk seals, and
  the pool remains at `min_pipelines`. Integration test.
- Given a sender holding a stale routing snapshot during scale-in, when
  its send races queue close, assert either the draining pipeline
  accepted and completes the object or the failed send returns and
  reroutes the same object. Assert no duplicate completion. Unit test.

**Bounds and sparse traffic**:

- Given a full pool byte budget, when another object handle is prepared,
  assert preparation waits without reserving partial bytes; after an
  existing handle releases its whole reservation, preparation resumes
  and retained bytes never exceed the ceiling. Unit test.
- Given `memory_budget < small_object_limit`, when configuration is
  validated, assert initialization fails rather than admitting an
  object whose reservation can never succeed. Unit test.
- Given one object without peers, when the batch deadline expires,
  assert a partial batch is written and no scale-out occurs solely
  because it waited for aggregation. Unit test.

**Failure boundary and shutdown**:

- Given a batch and queued objects behind it with R112 repair disabled,
  when a durable mirror write fails, assert every accepted object
  receives one error, no new location is published, the pipeline leaves
  routing, and previously acknowledged ranges remain readable.
  Integration test.
- Given a worker panic, when the manager observes termination, assert
  accepted objects fail exactly once and a replacement restores
  `min_pipelines` for future submissions. Integration test.
- Given active pipelines during pool shutdown, when admission closes,
  assert every accepted object completes or fails, every worker stops,
  and each chunk is sealed or deleted according to whether it contains
  acknowledged data. Integration test.
- Given object, batch, queue, scaling, and tail-waste activity, when
  metrics are sampled concurrently, assert all counters and
  distributions reflect the completed events without adding a
  submission-path lock. Unit test.

**Test commands**: `pixi run -- cargo test -p crowdb-chunk-client
small_object`, `pixi run -- cargo fmt --all -- --check`, `pixi run --
cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **Cancellation after submission**: completing the physical write and
  handing the abandoned range to in-chunk GC preserves batch integrity
  but creates garbage; persisting a pending object commit record
  enables recovery but adds metadata to the hot path. Removing bytes
  from an assembled batch is not safe.
