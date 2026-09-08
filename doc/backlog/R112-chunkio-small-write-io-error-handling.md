<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R112: chunkio / chunkdb / diskdb / diskio — Small-Write IO Error Handling

**Problem**

R106 writes many small objects through elastic pipelines. Each
pipeline owns one active `ChunkType::Repo` chunk in shared-packing mode,
aggregates whole objects into a physical batch, writes mirror strips,
and returns an independent `Location` to every object after durable
completion.

A small-write failure is physically shared but logically independent.
One failed disk operation can cover several objects, while every caller
still needs exactly one success or error. The handler must repair at the
failed mirror-replica range without losing previously acknowledged
objects in the same chunk or publishing a location for an
unacknowledged batch.

The shared-chunk shape adds constraints that the large-write error path
does not have:

- a batch contains several object ranges whose completions are coupled
  until the physical write is durable;
- a patch may extend a mirror strip that already contains acknowledged
  objects, so the pipeline's 1 MiB block shadow must preserve the prior
  durable prefix as well as the current batch bytes;
- pipeline scale-in closes and drains a queue, so repair must finish or
  fail accepted objects before the worker retires;
- chunk rotation separates batches only at object boundaries, so a
  failure must not invalidate the sealed predecessor chunk; and
- R93 conversion must never replace strip metadata concurrently with an
  active small-write repair.

**Current behavior + impact**: R106 is not implemented, and its
baseline failure contract can only fail the affected pipeline. There is
no small-write replica replacement, failed-disk exclusion, retry
budget, metadata repair, or recovery escalation. Replaying individual
objects based on a partial disk write would be unsafe because physical
bytes may exist without durable completion and no object location has
yet been published.

**Design pointers**: R106 defines object batching, single-owner chunks,
location publication, rotation, and pipeline drain. chunkio design §7
defines the existing whole-strip retry boundary and §8 defines the
`ChunkAllocator` and `DiskWriter` seams. chunkdb design §5.2 defines
mirror strips, §10.6 defines `update_chunk_strip`, and §9 defines chunk
lifecycle. diskdb design §8 defines hardware status and allocation
eligibility. R110 defines the shared negative-list and replacement
pattern. R83 owns post-failure recovery. R93 owns mirror-to-EC
conversion after the foreground write boundary. R107 defines stored-
geometry mapping and the bounded layout-validity window that fences
retired segment reuse.

**Use scenarios**:

- **Failed replica on a new mirror strip**: one replica fails durable
  completion while the other two succeed. The worker retains the
  batch image, allocates one replacement block away from the failed
  disk, writes the image durably, swaps that replica in chunk metadata,
  and then completes every object in the batch.

- **Failed patch on a used mirror strip**: the strip already contains
  acknowledged objects A and B; a batch appending C and D fails on one
  replica. The pipeline's complete 1 MiB shadow already contains A and
  B's prefix plus the C and D patch. Replacing the replica cannot erase
  or relocate A and B, and C and D receive locations only after repair
  succeeds.

- **Persistent failure**: replacement allocation or write fails until
  the retry budget is exhausted. The current batch and accepted queued
  objects fail once, the pipeline leaves routing, the failed disks are
  reported, and R83 can rebuild any committed degraded state.

- **Failure during rotation**: the old shared chunk seals successfully,
  but allocation or the first write on the replacement chunk fails.
  Locations already returned in the old chunk remain valid. Only
  objects assigned to the new chunk fail or retry.

- **Failure while draining**: scale-in has closed a pipeline queue and
  the last accepted batch encounters a disk error. The worker runs the
  same bounded repair. It seals and retires after success, or fails the
  accepted objects and retires after exhaustion.

- **Conversion failure after write success**: all mirrors durably
  complete and object locations return. R93 later fails EC conversion.
  The strip remains mirrored and readable; the foreground error handler
  does not reopen completed objects.

**Solution**

**One-line approach**: extend each R106 pipeline with a batch-level
repair state machine and one 1 MiB open-block shadow that replaces
failed mirror replicas in the same Active chunk, reuses R110's failed-
disk exclusion, and publishes per-object locations only after the batch
returns to full mirror durability.

**Numbered work items**:

1. **Batch repair ledger**
   (`lib/crowdb-chunk-client/src/writer/small_pipeline.rs`) — retain
   each in-flight batch's object descriptors, physical strip ranges,
   target segments, shadow ranges, and completion senders until the
   batch commits or fails. Once object fragments are copied into the
   open-block shadow, transfer their byte accounting and release the
   separate fragment storage; the shadow is the repair image. Track
   repair per physical mirror replica; do not infer per-object success
   from a short or failed disk operation.

2. **Failure classification** (`writer/small_pipeline.rs`) —
   distinguish chunk/strip allocation failure, mirror durable-write
   failure, replacement allocation failure, metadata-swap failure, and
   internal worker failure. Only mirror write and replacement failures
   enter replica repair. Invalid metadata, invariant violations, and
   worker panic fail the pipeline without replaying accepted work. Add
   `IoError::MetadataConflict` and preserve chunkdb's typed stale-
   revision or `Aborted` result through `ChunkAllocator`; do not flatten
   it into `IoError::AllocationFailed(String)`.

3. **Replacement allocation seam**
   (`lib/crowdb-chunk-client/src/traits.rs`, the chunkdb protocol/client,
   and `app/crowdb-chunkdb/src/`) — add a testable
   `ReplacementAllocator` trait beside `ChunkAllocator`. Add a chunkdb
   `allocate_replacement_segment` RPC whose lifecycle handler invokes
   `MirrorPlacement` with the surviving replicas plus negative-list
   disks as exclusions. It returns one tentative segment owned by the
   chunk, with compatible unit size/count, on a disk/node/rack that
   preserves the configured mirror anti-affinity; the metadata swap
   commits it. The production client implementation uses that RPC, not
   a direct diskdb allocation. Test doubles inject allocation, write,
   metadata, and cleanup failures without services.

4. **Shared negative list**
   (`lib/crowdb-chunk-client/src/negative_list.rs`) — reuse R110's
   TTL-based disk exclusion across large writes, reads, and every R106
   pipeline in one client. Add the failed disk before requesting a
   replacement. Replacement allocation must exclude all live entries;
   pipeline scale decisions remain based on queue load, not disk
   placement.

5. **One-block pipeline shadow**
   (`writer/small_pipeline.rs`) — use R106's 1 MiB small-write mirror
   block and keep one complete shadow only for each pipeline's
   currently open block. Initialize unwritten bytes deterministically,
   copy every admitted batch into its block-relative range, and retain
   an immutable snapshot through durable completion or repair. The
   shadow therefore contains the acknowledged prefix, current batch,
   and deterministic tail without reading any disk. Release it as soon
   as the block closes; a later foreground batch never patches a closed
   block. Charge the shadow to the pool budget before making a pipeline
   routable, and validate that the configured maximum pipeline count,
   shadows, queues, and object reservations fit the total budget.

6. **Replica replacement**
   (`writer/small_pipeline.rs`) — allocate one mirror block on a healthy
   disk, durably write the complete 1 MiB shadow, then call
   the one-strip form of `replace_chunk_strip_range` with the complete
   strip to replace only the failed segment. Other replicas and every
   existing object offset remain unchanged. Extend the lifecycle
   handler to compute segment
   identity set differences and require the replacement range's first
   chunk offset, first strip sequence, and total logical capacity to
   equal the old range's. Then commit only segments present in the new
   strip and absent from the
   old strip, publish metadata, and retire only segments absent from the
   new strip. A one-replica repair must never commit or free the
   unchanged healthy replicas; R93's
   whole-strip conversion uses the same rule with disjoint sets. After
   a successful repair, keep the chunk Active and resume from the same
   append cursor and shadow. A repairable disk failure does not seal or
   rotate the chunk.

7. **Idempotent metadata range swap and cleanup intent**
   (`lib/crowdb-protocol/src/types/chunkdb.rs`,
   `lib/crowdb-protocol/src/fbs/chunkdb.fbs`,
   `lib/crowdb-chunkdb-client/src/`, and
   `app/crowdb-chunkdb/src/lifecycle/handler.rs`) — add an expected
   chunk revision and stable operation identity to a generalized
   `replace_chunk_strip_range` request. It carries the start index, the
   exact expected old strip sequence/fingerprint, and one or more
   replacement strips; preserve `update_chunk_strip` as a one-for-one
   client wrapper. Define the operation identity from the chunk ID,
   expected revision, old range fingerprint, and replacement strip
   fingerprints so it survives client retries and server restarts.
   Add a monotonic `next_strip_sequence` to chunk metadata; append
   consumes and increments it instead of deriving a sequence from
   `strips.len()`, so an N-to-M splice cannot create duplicates.
   Under the per-chunk lifecycle lock, apply the replacement only when
   the revision and old range match. If the current revision is the
   operation's successor and the installed range exactly matches the
   replacement, return the current chunk as the already-committed
   result without recommitting or freeing segments. Any other current
   strip or revision is a conflict for the caller to reconcile. On
   metadata failure, roll back only newly committed segments. Treat
   old-only segment cleanup as post-commit cleanup. Atomically publish
   the replacement together with a cleanup intent containing the
   operation identity, exact old-only segment set, and `not_before`
   time beyond R107's maximum layout-validity window plus clock-skew
   margin. Do not make retired segments reusable before that time.
   Advertise the configured validity duration in `QueryChunkResponse`;
   readers measure it from local request start, not response receipt.
   Clear the intent only after an allocation-
   qualified free confirms those segments still belong to the expected
   chunk and are absent from its current strip metadata. A retry of an
   already committed operation resumes only its pending cleanup. Its
   failure must not make the metadata transaction replayable or change
   a committed operation into an uncommitted result.

8. **Retry and object completion** (`writer/small_pipeline.rs`) —
   bound repair attempts per failed replica. A batch commits only when
   all configured mirror replicas are durable and chunk metadata
   references their current segments. Then fan out the independent
   R106 locations. On retry exhaustion, return one error to every
   object in the uncommitted batch; never return locations for only a
   subset based on physical write progress.

9. **Pipeline failure boundary and degraded mirror state**
   (`lib/crowdb-chunk-client/src/writer/small_manager.rs`) — on repair
   exhaustion, unpublish and close the pipeline. Fail accepted entries
   still queued behind the batch exactly once because their placement
   outcome is not known to another worker. Extend R110's protocol-level
   degradation record to identify unavailable segment identities for
   both mirror and EC strips. Persist mirror degradation as a fenced
   one-strip range replacement whose segment set is unchanged and whose strip
   health identifies the unavailable replica. If the failed mirror
   strip contains a
   previously acknowledged prefix, persist that record before retiring
   so R83 and R111 can find it. If the strip
   contains only the unacknowledged batch, clean up the partial extent.
   Preserve previously acknowledged locations and restore
   `min_pipelines` for future submissions.

10. **Rotation and drain integration** — complete repair before sealing
   or retiring the affected active chunk. A sealed predecessor chunk is
   outside the failure scope of a replacement-chunk error. An empty
   replacement chunk is deleted on terminal failure. A draining
   pipeline accepts no new objects but uses the same retry budget for
   entries accepted before queue close.

11. **R93 conversion boundary**
    (`app/crowdb-chunkdb/src/conversion.rs`) — migrate conversion to
    the fenced range-replacement request defined in work item 7.
    R93 may convert a sealed chunk or a capacity-compatible group of
    closed mirror strips whose durable state proves that R106 will not
    append to it again. R112 owns
    failures before that close boundary; R93 owns encode, parity-write,
    and metadata-swap failures after conversion starts. Revision and
    operation fencing serializes conversion with competing repair.

12. **Escalation and metrics** — after exhaustion, report failed disks
    through the R110/R83 path and record batch repair attempts,
    repaired replicas, repair latency, negative-list hits, exhausted
    repairs, failed objects, pipeline replacements, shadow bytes, and
    successful repairs that avoided chunk rotation. Metrics updates on
    the write hot path use atomics and do not add a blocking lock.

**Edge-case outcomes**:

- One failed replica, two healthy replicas → rebuild only the failed
  replica and complete all batch objects after full durability.
- Two failed replicas with an intact pipeline shadow → write the same
  immutable shadow to two placement-safe replacements sequentially and
  commit the batch once.
- Healthy replicas cannot be read during repair but the pipeline shadow
  is intact → replace the failed replica without a read dependency.
- Required open-block shadow is missing or invalid → fail the pipeline
  as an internal invariant violation; never fabricate prior bytes.
- Failure before any object in the batch is acknowledged → return no
  locations for that batch.
- Failure while patching a used strip → preserve prior object offsets
  and bytes when replacing the block.
- Repair exhaustion on a strip with acknowledged data → record the
  failed replica through R110's degraded-strip state before escalation.
- Metadata response is lost after commit → repeat the same operation
  identity and return the committed result without freeing the
  replacement.
- Old-segment cleanup fails after metadata commit → retain a cleanup
  intent for retry, return the committed strip result, and never replay
  the replacement transaction.
- A reader holds the old layout during replacement → retain every old
  segment until its persisted layout-validity grace expires; a reader
  that exceeds its deadline discards its bytes and retries current
  metadata before returning data.
- Metadata revision or old strip differs → report a conflict and
  reconcile; never overwrite an unrelated repair or conversion.
- All candidate disks are excluded → return `IoError::AllocationFailed`
  with the no-healthy-disk cause, fail the pipeline, and escalate.
- Failure on a prepared but empty replacement chunk → delete the chunk;
  the sealed predecessor remains readable.
- Pipeline enters `Draining` during repair → finish the repair before
  seal, or exhaust and fail accepted entries before stop.
- R93 conversion fails after foreground success → leave the mirrored
  strip readable and let R93 retry; do not notify completed writers.

**Flow diagram**:

```text
 R106 pipeline              diskio                 diskdb/chunkdb
      |                        |                          |
      | durable mirror write   |                          |
      |----------------------->|                          |
      |<---------- failure ----|                          |
      |                        |                          |
      | add disk to shared negative list                 |
      | retain batch descriptors + immutable 1 MiB shadow|
      |                                                   |
      | allocate replacement, excluding failed disks     |
      |-------------------------------------------------->|
      |<-------------------------------------- new segment |
      |                        |                          |
      | write 1 MiB shadow    |                          |
      |----------------------->|                          |
      |<---------- durable ----|                          |
      |                                                   |
      | replace strip range (failed -> replacement)       |
      |-------------------------------------------------->|
      |<----------------------------------------- committed |
      |                                                   |
      | fan out independent Location results              |
      |----> object A  object B  object C                 |
      |                                                   |
      | retry exhausted -> fail batch + queue -> R83      |
```

**Dependencies**

- **Depends on**:
  - **R106** — supplies the batch ledger inputs, exclusive pipeline
    ownership, object completion senders, rotation, and drain boundary.
  - **R105 / diskio** — reports durable-write failure without treating
    partial physical progress as object success.
  - **R110** — supplies the shared negative list and failure-reporting
    pattern; its degradation artifact must cover unavailable mirror as
    well as EC segment identities.
  - **R107 layout validity** — maps by stored strip geometry and bounds
    how long a queried segment layout may be used, allowing safe deferred
    reclamation after replacement.
  - **chunkdb (landed, R85)** — supplies `update_chunk_strip` and chunk
    lifecycle operations; R112 generalizes the update into a retry-safe
    range replacement with revision and operation fencing.
  - **diskdb through chunkdb placement** — allocates one tentative
    replacement segment after chunkdb applies surviving-replica
    anti-affinity and failed-disk exclusions.
- **Integrates with**:
  - **R83** — rebuilds persistent failures after inline repair
    exhaustion. R112 can fail safely before R83 lands, but automated
    recovery requires both.
  - **R93** — begins after the R106/R112 foreground durability
    boundary and owns conversion failures.
- **Depended on by**: **R93** uses the range transaction for grouped
  mirror-to-EC conversion. R111 integrates with the same degraded-strip
  and layout-validity contracts but is not a write-path prerequisite.

**Acceptance**

**Replica repair**:

- Given a pipeline opens a 1 MiB mirror block, when batches advance its
  cursor and the block later closes, assert one complete shadow is
  maintained throughout the open lifetime and released at close. Unit
  test.
- Given a batch on a new three-copy mirror strip and one durable-write
  failure, when inline repair runs, assert one replacement allocation,
  one durable replacement write, one one-strip range swap, unchanged
  healthy replicas, and successful independent object locations.
  Integration test.
- Given an acknowledged prefix and a new patch in one mirror block,
  when one replica fails, assert the replacement writes the complete
  1 MiB shadow, preserves the prefix, includes the patch, leaves earlier
  locations unchanged, and publishes new locations only after metadata
  commit. Integration test.
- Given two failed replicas and an intact shadow, when repair runs
  within budget, assert both replacements receive identical complete
  shadows and the batch commits once. Integration test.
- Given healthy-replica reads are unavailable but the shadow is intact,
  when repair runs, assert no read is issued and replacement succeeds
  from the shadow. Integration test.
- Given the required open-block shadow is unavailable, when repair
  starts, assert no replacement metadata is published and the pipeline
  fails with an internal invariant error. Unit test.
- Given a repairable replica write failure in an Active chunk, when
  shadow-based replacement commits, assert the chunk ID, Active state,
  append cursor, and existing locations are preserved and no seal or
  rotation RPC is issued. Integration test.

**Negative list and retries**:

- Given the pool cannot reserve another pipeline's 1 MiB shadow within
  its configured total budget, when scale-out is attempted, assert the
  new pipeline is not published and existing pipelines continue.
  Integration test.
- Given a failed disk in the shared negative list, when any R106
  pipeline requests replacement allocation, assert the request excludes
  that disk. Integration test.
- Given two healthy mirror replicas occupy known disks, nodes, and
  racks, when chunkdb allocates the failed replica's replacement,
  assert `MirrorPlacement` preserves the configured anti-affinity and
  compatible unit geometry in addition to excluding failed disks.
  Integration test.
- Given repeated failure of one replacement, when attempts reach the
  configured limit, assert one terminal batch error, one pipeline
  removal, failed-disk reporting, and no object location publication.
  Integration test.
- Given every disk in the placement group is excluded, when replacement
  allocation runs, assert `IoError::AllocationFailed` retains the
  no-healthy-disk cause and recovery escalation occurs. Unit test.

**Metadata and idempotency**:

- Given a durable replacement block and a timed-out metadata response,
  when the same operation identity is retried, assert one final segment
  reference, no second replacement allocation, and no free of the
  installed replacement. Integration test.
- Given metadata commit succeeds and freeing the old segments fails,
  when the handler restarts and resumes the persisted cleanup intent,
  assert the replacement remains installed, the operation remains
  committed, and only still-owned old segments are offered for cleanup.
  Integration test.
- Given a reader queried the old layout immediately before a strip
  replacement, when the replacement commits, assert the old segments
  cannot be freed or reused until the advertised layout window and
  safety margin expire. Integration test.
- Given a stale expected revision or a different current strip, when
  replacement is attempted, assert a conflict and no metadata or block
  mutation. Integration test.
- Given a successful replacement, when `query_chunk` runs and the
  layout-validity grace later expires, assert only the failed segment
  changed, unchanged healthy segments remain allocated, the failed old
  segment alone is freed, and all object offsets remain identical.
  Integration test.

**Pipeline, rotation, and drain**:

- Given an old sealed chunk and failure on the first batch of its
  replacement, when repair exhausts, assert old locations remain
  readable and only replacement-chunk objects fail. E2E test.
- Given a draining pipeline with an in-flight repair, when scale-in
  completes, assert the repair commits before seal or all accepted
  objects fail before the worker stops. Integration test.
- Given queued objects behind an exhausted batch, when the pipeline
  closes, assert every accepted completion resolves exactly once and
  the manager restores `min_pipelines`. Integration test.
- Given terminal failure after a replacement chunk was prepared but
  before its first object write, when the pipeline retires, assert the
  empty chunk and tentative segments are deleted. Integration test.

**R93 boundary**:

- Given foreground mirror success followed by an injected conversion
  failure, when R93 runs, assert the strip remains mirrored and readable
  and no completed R106 handle receives another result. Integration
  test.
- Given repair and R93 conversion start from the same chunk revision,
  when both submit fenced strip updates, assert exactly one commits and
  the loser observes a typed conflict without freeing or overwriting
  the winner's segments. Integration test.

**Metrics**:

- Given successful repair, negative-list retry, exhausted repair, and
  pipeline replacement events, when metrics are sampled, assert each
  counter and latency distribution reflects the event without a write-
  path lock. Unit test.

**Test commands**: `pixi run -- cargo test -p crowdb-chunk-client
small_write_error`, `pixi run -- cargo fmt --all -- --check`,
`pixi run -- cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **Negative-list scope**: per-client state is simple and shares
  failures across its pipelines; per-process state also protects other
  clients but needs a common lifetime and metrics owner; node-wide
  state propagates knowledge furthest but requires a service boundary.
  R110 and R111 must use the same choice.
