<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R171: chunkdb / chunk-client — Ad-hoc EC Read Recovery

**Problem**: `ChunkReader` already reconstructs only the requested byte slice
when a readable EC shard fails: a 64-KiB read uses 64-KiB shard slices and
ISA-L decode rather than rebuilding an entire disk block. It then persists an
`unavailable_segments` marker, and a later ChunkDB repair task re-reads and
rebuilds the whole fragment. This keeps the foreground read latency bounded,
but it discards useful work when the failure or request already requires a
whole disk block. Concurrent reads of the same failed fragment independently
decode the same bytes, and a full-block reconstruction is not handed to the
durable repair flow that could publish it.

The root read design is [ChunkIO Reader](../design/chunkio/design-crowdb-chunkio-reader.md)
sections 3 and 7. The durable repair state machine is the
[ChunkDB placement-repair flow](../design/chunkdb/design-crowdb-chunkdb.md#73-physical-validation-and-degraded-placement-repair):
it allocates a DiskDB BusyBlock as `Tentative`, writes and fsyncs the
replacement, CAS-publishes chunk metadata, then confirms that same BusyBlock.
This requirement changes neither the small-range recovery behavior nor
DiskDB's allocation/confirmation contract.

**Solution**: Preserve slice recovery for small reads and add a bounded,
ChunkDB-owned full-block ad-hoc recovery path.

1. Keep `ChunkReader`'s range-based EC recovery as the foreground fast path.
   It must calculate shard-relative offsets and reconstruct only the requested
   slice. A successful 64-KiB read must not allocate a replacement block or
   read a full fragment merely to repair it. It continues to persist the exact
   failed segment marker before returning bytes.

2. Define an explicit full-block threshold and eligibility policy in the
   chunk-client read policy. Only a request covering a complete EC fragment,
   or a bounded accumulation of matching failures that reaches that threshold,
   may request ad-hoc full-block recovery. Partial ranges never silently grow
   into full-block reads. The policy includes memory, concurrency, and queue
   bounds; rejected or saturated requests retain the existing marker-and-
   background-repair behavior.

3. Add a versioned routed request from chunk-client to the owning ChunkDB
   instance containing the chunk id, expected chunk revision, strip sequence,
   exact failed segment incarnation, and recovery operation identity. ChunkDB
   validates that the exact segment is still present and unavailable in the
   referenced EC strip. A stale layout, a healed segment, an incompatible
   strip, or an insufficient number of surviving shards returns a typed result
   and performs no allocation or metadata mutation.

4. Add a ChunkDB in-memory ad-hoc recovery manager keyed by chunk id, strip
   sequence, and failed segment incarnation. It coalesces duplicate callers,
   bounds retained full-block bytes and jobs, and shares a completed rebuilt
   block with all waiters. It is a latency optimization only: no correctness
   decision, allocation identity, publication authority, or cleanup permission
   may depend on process memory. The manager has no durable task record of its
   own.

5. The elected in-memory operation performs the existing durable repair
   sequence: allocate exactly one DiskDB BusyBlock (`Tentative`), read enough
   surviving full shards, EC-rebuild the failed block, write and fsync the
   target, checkpoint the existing `RepairStrip` job, CAS-replace the exact
   old strip at its expected revision, then confirm that BusyBlock. It may
   return the rebuilt bytes to waiters immediately after rebuild and fsync;
   publication and confirmation continue asynchronously through the same
   durable job. A process crash before publication leaves the single tentative
   BusyBlock for DiskDB's
   [owner-reconciliation scanner](../design/diskdb/design-crowdb-diskdb.md#tentative-owner-reconciliation-and-relocation);
   a crash after CAS resumes target confirmation from the repair checkpoint.
   No second target is allocated for a duplicate or resumed operation.

6. Integrate the manager with the existing marker-to-`RepairStrip` admission
   path. A successful client fallback still records `unavailable_segments`.
   The foreground full-block request either attaches to the already-admitted
   repair operation or causes deterministic admission; it must not create a
   competing mover. If foreground recovery cannot start or loses its fence,
   the normal background task remains responsible for repair.

7. Expose aggregate metrics for slice recoveries, full-block recovery starts,
   coalesced waiters, bytes reused, queue/memory rejections, stale operations,
   publication outcomes, and fallback-to-background counts. Do not put chunk,
   disk, or segment identifiers in metric labels.

**Dependencies**: Depends on the landed range-based reader in
`lib/crowdb-chunk-client`, ChunkDB's fenced `RepairStrip` target checkpoint and
publication sequence, and DiskDB's tentative BusyBlock owner-reconciliation
scanner for pre-publication crash cleanup. It adds a ChunkDB RPC surface and
uses the existing ChunkDB range routing and DiskIO routing. Background
placement repair remains independent; this requirement owns read-triggered EC
repair coalescing and full-block reuse.

**Acceptance**:

- Given a 64-KiB read whose required EC data shard has a durable read failure
  and enough surviving shards, when the client reads it, assert ISA-L receives
  only 64-KiB shard slices, the returned bytes are correct, no target is
  allocated, and the failed segment marker is persisted. Invariant: small
  reads do not amplify to full-block repair. Integration test.
- Given a complete failed EC fragment request with enough surviving shards,
  when the owner accepts ad-hoc recovery, assert it reconstructs and fsyncs
  one full target, returns correct bytes before metadata publication, and one
  durable repair job subsequently publishes then confirms that exact target.
  Invariant: bytes can be returned early while publication remains fenced. E2E
  test.
- Given concurrent full-block requests for the same chunk, strip, and failed
  segment, when they arrive before reconstruction completes, assert one EC
  decode, one DiskDB allocation, one target write, and identical bytes for all
  waiters. Invariant: process-local coalescing reduces duplicate work without
  changing durable ownership. Integration test.
- Given a stale expected revision, healed segment, or different segment
  incarnation, when an ad-hoc request reaches ChunkDB, assert it allocates no
  target and joins or defers to the current durable repair state. Invariant:
  stale reads cannot publish over a newer layout. Integration test.
- Given full-block recovery memory or concurrency is exhausted, when a client
  requests it, assert the client retains the failure marker and normal
  background `RepairStrip` admission repairs the segment. Invariant: overload
  never drops repair intent or bypasses limits. Integration test.
- Given a crash after target fsync but before Chunk CAS, when DiskDB's
  tentative-owner scanner runs, assert the unreferenced BusyBlock is retained
  or freed only according to its owner disposition and grace policy. Given a
  crash after CAS but before confirm, assert the checkpointed repair job
  confirms the same target without another allocation. Invariant: memory
  cache loss cannot leak or duplicate a target. E2E test.

Run `pixi run test-chunk-client`, `pixi run test-chunkdb`,
`pixi run rs-fmt -- --check`, and `pixi run cargo clippy -p crowdb-chunk-client
-p crowdb-chunkdb --all-targets -- -D warnings`.
