<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R136: chunkio — Reserved strip prefetch with deferred confirm

**Status:** Backlog. This is a follow-up to R113's attached-strip batch
prefetch. It is not part of the current small-write implementation.

## Problem

R113 prefetches several strips by appending them to an Active chunk before
they are consumed. This removes allocation and `append_chunk` latency from the
normal write response path, but every prefetched strip is already confirmed in
chunk metadata. ChunkDB must later detach and release unused tail strips when
the chunk seals.

The desired alternative is a separate flow: prefetch several unconfirmed strip
reservations, write to a reservation when the chunk needs it, and confirm only
the consumed strip into the chunk. Allocation, append, and confirmation remain
off the foreground response path, whose barrier is only the required DiskIO
completion.

This is not a small client-side visibility change. It introduces ownership and
recovery states shared by chunkio, chunkdb, and diskdb.

## Solution

Add a lease-scoped `RESERVED` strip lifecycle distinct from an attached,
confirmed strip:

1. The writer asks chunkdb for a bounded batch of strip reservations.
2. ChunkDB remains the placement authority and asks diskdb to reserve the
   required segments. The reservation records owner chunk, writer/lease ID,
   expiry, placement epoch, and an idempotency token.
3. Reservations are returned to the writer but are not added to `Chunk.strips`.
4. When the active strip needs capacity, the writer consumes one reservation
   and starts DiskIO without waiting for chunk metadata persistence.
5. A revision-ordered background task confirms the consumed reservation.
   ChunkDB atomically attaches the strip to the chunk, advances the logical
   cursor, and commits the reserved blocks.
6. Chunk seal cancels surplus reservations. Expired writer leases and an
   idempotent reaper reclaim reservations that were never confirmed.

The client may keep multiple reservations outstanding. Configuration must
bound the count and bytes per writer and globally. Refill uses a low-water
mark and must not add a lock to the data hot path.

### State model

```text
FREE -> RESERVED -> CONSUMED -> CONFIRMED
          |            |
          +-> EXPIRED <-+
                    |
                    +-> FREE
```

- `RESERVED`: segments have exclusive allocation ownership but are absent
  from the chunk.
- `CONSUMED`: DiskIO may have started; confirmation is pending.
- `CONFIRMED`: chunk metadata references the strip and its blocks are durable.
- `EXPIRED`: the reservation is fenced from confirmation and can be reclaimed.

`confirm` and `cancel` must be idempotent. A lease epoch fences a delayed
confirm from attaching blocks after expiry and reuse. DiskDB must never recycle
a physical extent while an earlier DiskIO can still target it.

### Response and visibility contract

- Foreground small writes wait for their mirror DiskIO and then return.
- Reserve, confirm, cursor advancement, refill, and surplus cancellation run
  on the metadata line and do not extend normal write latency.
- A returned location may be temporarily `NotYetAvailable` until confirmation
  publishes the corresponding chunk metadata.
- A confirm failure retires the pipeline. Subsequent writes and `finish` report
  that failure; recovery decides whether a consumed reservation is confirmed
  or reclaimed.
- Sealing must wait for confirmation of all locations included in the sealed
  length, then cancel every unused reservation.

## Dependencies

- R113 establishes batched prefetch policy, low-water refill, background
  metadata sequencing, and unused-tail cleanup for attached strips.
- ChunkDB protocol additions for reserve, confirm, cancel, and lease renewal.
- DiskDB reservation records with expiry, incarnation fencing, and idempotent
  commit/free operations.
- A recovery owner that can reconcile consumed data after writer or chunkdb
  failure without exposing a sealed chunk that references reclaimed blocks.

## Acceptance

- Reserve N strips without changing `Chunk.strips`; query confirms that only
  confirmed strips are visible.
- Consume one of several reservations, complete DiskIO, and return without
  waiting for a delayed confirm RPC. A later query observes the attached strip.
- Confirm and cancel retries with the same token are idempotent.
- A stale lease cannot confirm an expired reservation after its blocks have
  been reclaimed or reassigned.
- Crash before consume reclaims all reservations after expiry.
- Crash after DiskIO but before confirm deterministically confirms or frees the
  consumed reservation according to the recovery record; it never aliases the
  extent to a second owner.
- Seal waits for confirmations inside `seal_length`, cancels surplus reserved
  strips, and leaves no reservation records or busy blocks behind.
- Multiple prefetched reservations keep allocation and confirmation off the
  1-thread normal write response path. Benchmark payload remains exactly one
  mirror copy per replica, with zero incomplete or failed objects.
- Failure-injection E2E tests cover writer death, chunkdb restart, diskdb
  restart, delayed confirm, duplicate confirm, expiry race, and seal race.

## Open Questions

- Whether DiskIO may start in `RESERVED`, or requires a durable `CONSUMED`
  transition first. The former meets the latency goal but needs stronger
  generation fencing against expiry and reuse.
- Whether chunkdb owns reservation leases entirely or diskdb independently
  expires block reservations. A single authority is simpler; diskdb still
  needs a safe orphan-reclamation path.
- Whether recovery confirms written-but-unconfirmed data or discards it. Returned
  locations favor confirm-forward recovery unless the API permits loss before
  `finish`.
- Bound transient `NotYetAvailable` behavior and decide whether readers retry
  locally or receive an explicit confirmation token.
