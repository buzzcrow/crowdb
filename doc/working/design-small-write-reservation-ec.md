<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Batched Strip Reservation and Incremental EC (R113, R136, R137)

This implementation design combines [R113](../backlog/R113-chunkio-batch-strip-allocation.md),
[R136](../backlog/R136-chunkio-reserve-confirm-strip-flow.md), and
[R137](../backlog/R137-chunkio-incremental-ec-conversion.md). It extends the
[small-object writer](../design/chunkio/design-crowdb-chunkio-small-object-writer.md),
[chunkdb lifecycle](../design/chunkdb/design-crowdb-chunkdb.md), and
[mirror-to-EC task design](../design/chunkdb/design-crowdb-chunkdb-mirror-to-ec.md).
The attached small-write batch path and DiskDB tentative/commit lifecycle are
already landed.

## 1. Implementation Order

1. Finish R113 first: make ChunkDB allocate a strip batch concurrently and make
   the large writer request bounded batches. This is an independently testable
   RPC-count reduction and preserves current metadata semantics.
2. Add R136 reservation records and RPCs while retaining the attached path as a
   compatibility fallback. ChunkDB remains the only placement authority.
3. Add R137's special 28-block reservation group and switch incremental parity
   to four retained parity blocks with no retained data images.
4. Enable reservation groups for the shared writer only after functional and
   performance gates pass. Large writes keep ordinary batched attachment.

## 2. R113 Batched Attached Allocation

`ChunkAllocator::allocate_strips` allocates independent strips concurrently
from one topology snapshot, returns them in strip-sequence order, and rolls back
every successful allocation if any member fails. `allocate_chunk` and
`append_chunk` use it and persist the resulting ordered batch once.

The large writer replaces its one-strip helper with:

```rust
async fn append_strips(
    chunkdb: &dyn ChunkAllocator,
    chunk: Chunk,
    ec_scheme: EcScheme,
    strip_count: u32,
) -> Result<Chunk>;
```

The prefetch task selects `min(prefetch_strips_per_chunk, remaining,
chunk_runway)` and sends one cumulative chunk per batch. Stale-revision retry
uses the refreshed chunk and recomputes the remaining runway.

## 3. R136 Durable Reservations

### 3.1 Metadata and RPC

Reservations are stored with the owning `Chunk`, outside `Chunk.strips`, so the
chunk lifecycle guard and one KV record provide revision ordering. Each record
contains a deterministic reservation ID, writer epoch, lease generation and
deadline, placement epoch, state, logical sequence/offset, and tentative strip.

```rust
enum StripReservationState { Reserved, Consumed }

struct StripReservation {
    reservation_id: ChunkId,
    writer_epoch: u64,
    lease_generation: u64,
    lease_deadline_ms: u64,
    placement_epoch: u64,
    state: StripReservationState,
    strip: ChunkStrip,
}
```

The protocol adds reserve, consume, confirm, cancel, and renew operations.
Every mutation carries the chunk revision, writer epoch, reservation ID, and
lease generation. Identical retries return the current result; stale generation
returns a state conflict.

### 3.2 Lifecycle

Reserve allocates tentative blocks and persists reservation ownership without
changing readable chunk layout. Consume is durable before DiskIO is allowed.
The client keeps a bounded queue of already-consumed reservations so this
metadata barrier remains on the prefetch line rather than the object path.

After mirror DiskIO succeeds, confirmation atomically removes the reservation,
attaches its strip in sequence, advances the acknowledged cursor supplied by
the ordered metadata line, and records the blocks for commit. Block commit may
finish after publication because tentative blocks remain exclusively allocated;
recovery repeats the idempotent commit.

Cancel applies only to `Reserved`. A consumed reservation is never recycled on
lease expiry. Recovery either confirms a consumed reservation covered by the
durable acknowledged prefix or retains it for operator-visible retry; this
prevents delayed DiskIO from targeting a reused extent. Reserved entries are
marked expired under the lifecycle guard and reclaimed through a persisted
cleanup intent.

The client falls back to R113 attached batches when reserve/refill fails. A
pipeline never mixes reservation and attached ordering within the same logical
sequence range.

## 4. R137 Special Conversion Group

### 4.1 Placement and metadata

One group allocation produces eight three-copy mirror candidate sets plus four
parity blocks. The selector computes all roles from one topology snapshot and
must identify one candidate from each mirror set whose combination with parity
has the optimum current EC placement score. Allocation is all-or-rollback.

The durable group record contains the eight reservation IDs, four parity
segments, allocation-time survivor preferences, placement epoch, and conversion
state. It is invisible to readers until each mirror is confirmed normally.

### 4.2 Incremental write and publication

After each complete mirror strip succeeds, the client passes its existing
1 MiB shadow view to four incremental parity accumulators and releases the
input image. It does not retain eight data images. After input eight, it writes
and fsyncs only parity. Survivor mirror segments are already durable data shards.

Before fenced range publication, ChunkDB reruns survivor selection against the
current topology and healthy candidates. It publishes one EC strip and one
cleanup intent only when the selected 8+4 set is still optimal. Otherwise all
mirrors remain authoritative and a deterministic `MirrorToEc` task stays
retryable until relocation makes an optimal publication possible.

Early seal with one through seven used mirrors cancels parity ownership and
unused reservations, retains used mirrors, and creates no partial EC layout.

## 5. Scope

- `lib/crowdb-protocol/src/fbs/chunkdb.fbs`, generated wrappers, and Rust RPC
  types: reservation/group records and lifecycle messages.
- `app/crowdb-chunkdb/src/allocator.rs`, `selector/`, `lifecycle/`, `task/`, and
  RPC service: batch allocation, reservation lifecycle, special placement,
  recovery, publication, and cleanup.
- `lib/crowdb-chunkdb-client/` and `lib/crowdb-chunk-client/`: transports,
  prefetch, reservation queue, incremental parity, and fallback.
- `app/crowdb-diskdb/`: idempotent tentative commit/free validation and expired
  reservation cleanup support; no placement policy moves into DiskDB.
- Integration/E2E tests in each affected crate's `tests/` and the small-write
  regression script results.

## 6. Complexity

High. The difficult parts are crash-safe ownership across three services,
revision-ordered publication, survivor re-selection after topology change, and
keeping every new metadata action off the small-write response hot path. No new
lock is introduced; existing per-chunk lifecycle serialization remains the
metadata authority.

## 7. Test Design

- Allocate/append N strips with one injected member failure: call the batch
  path; assert ordered success or complete rollback with no busy blocks.
- Run a large writer with batch size N: cross N strips; assert one append RPC per
  batch, exact locations, and reconstructed data.
- Reserve N strips: query the chunk before and after confirmation; assert only
  confirmed strips are readable.
- Retry reserve/consume/confirm/cancel and send stale generations: assert exact
  idempotence and fencing.
- Kill the writer before consume, during DiskIO, and after DiskIO: run recovery;
  assert reserved blocks are reclaimed, consumed blocks are never aliased, and
  acknowledged ranges are confirmed or remain tracked.
- Seal with surplus reservations: assert confirmed prefix, cancelled tail, and
  zero leaked busy blocks after cleanup.
- Allocate a special group: assert 24 mirror candidates plus four parity blocks
  and an optimal selectable 8+4 set.
- Complete eight strips: assert only four parity writes, bounded input images,
  correct reconstruction, and fenced retirement of sixteen replicas.
- Change topology before publication: assert survivor re-selection or one
  persistent retryable task with no replica freed.
- Seal after each of one through seven strips: assert mirrors remain readable
  and all unused group resources are reclaimed.
- Run the same-machine small-write benchmark before and after every enabled
  phase; assert zero errors/incomplete writes, unchanged payload accounting,
  and no material TPS or p99 regression. Run the full matrix before cleanup.

## 8. Module Structure

```text
app/crowdb-chunkdb/src/
├── allocator.rs                 # ordered concurrent strip/group allocation
├── selector/conversion.rs       # special mirror-candidate + EC placement
├── lifecycle/reservation.rs     # fenced reserve/consume/confirm/cancel
└── task/conversion.rs           # retryable optimal-placement completion
lib/crowdb-chunk-client/src/
└── writer/
    ├── small_pipeline.rs        # pipeline orchestration
    ├── small_reservation.rs     # bounded reservation/refill state
    └── small_conversion.rs      # incremental parity and publication
```

## 9. Config Extensions

Reuse `small_strip_prefetch_count` as the ordinary reservation bound. EC-enabled
shared writers round full conversion batches to eight; a final capacity tail is
mirror-only. Add lease renewal and recovery scan intervals only on ChunkDB;
defaults retain the current attached path until the new flow passes perf gates.

## 10. Server Wiring

ChunkDB registers the new RPC handlers beside existing lifecycle mutations and
runs reservation recovery through the existing task scanner/executor. DiskDB
continues to expose allocate/commit/free; reservation identity validation is
carried by the existing allocation timestamp and owner chunk.

## 11. Performance Evidence

The same-machine R113 four-endpoint run is retained under
`bench-log/r113-r136-r137-after-r113-20260910/`. All cases completed with zero
errors, zero incomplete objects, and exact mirror payload accounting. Relative
to valid pre-change cases, 1 KiB/1T improved from 2,489.69 to 2,621.36 TPS,
1 KiB/256T from 438,200.34 to 575,163.52 TPS, and 8 KiB/256T from 104,320.37
to 120,843.78 TPS. The post-change 8 KiB/1T result is 2,214.55 TPS; its first
baseline attempt hit a transient range-readiness failure and is excluded from
the A/B ratio.
