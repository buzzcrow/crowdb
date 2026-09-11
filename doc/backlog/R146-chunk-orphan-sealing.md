<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R146: chunkdb — Seal abandoned chunks across all chunk users

**Status: Deferred.** The existing chunkdb expired-writer sweep is the base
mechanism. Implement the generic owner identity after R141's production stream
writer supplies owner metadata, then apply the same contract to all chunk
types: `Stream`, `Repo`, `Wal`, `BtreePage`, and `PageIndex`.

## Problem

Any chunk writer that crashes or loses ownership leaves an Active chunk whose
unused tail cannot be reclaimed safely. This affects every chunk type:

- **`Stream`:** a stream writer crashes mid-append, leaving an
  Active chunk with unacknowledged bytes beyond the cursor.
- **`Repo`:** an object writer crashes while it owns an Active chunk.
- **`Wal`:** a WAL writer crashes after appending but before sealing.
- **`BtreePage` / `PageIndex` (R140):** a B+tree process crashes during
  rotation; the old active chunk cannot be sealed and its tail is orphaned.

R140 rotates a B+tree's active chunk at 256 MiB and seals it during an orderly
rotation or shutdown. A process crash cannot perform that seal. Restart also
must not resume the old active chunk because the former writer's final outcome
may be ambiguous; it opens a fresh chunk instead. Without management cleanup,
the abandoned chunk remains Active indefinitely and its unused tail cannot be
reclaimed safely.

Chunkdb already persists `writer_epoch`, `writer_lease_deadline_ms`, and
`acknowledged_cursor` for shared chunks and scans expired writers under the
normal lifecycle guard. The B+tree backend must participate in that durable
contract, including while an open tree is idle. An in-memory owner registry or
one-shot startup task would be lost when chunkdb restarts and could seal a
chunk that is still owned by a live but temporarily idle tree.

The root lifecycle contract is
`doc/design/chunkdb/design-crowdb-chunkdb.md`; R140 defines the B+tree chunk
writer and its restart behavior.

## Solution

Reuse chunkdb's persisted writer lease and expired-writer sweep for **all
chunk types** (`Stream`, `Repo`, `Wal`, `BtreePage`, `PageIndex`), with durable
owner identity and periodic renewal independent of write traffic.

1. Extend chunk metadata with `owner_key: bytes` and add the `Stream` chunk
   type. A nonempty owner key starts with an owner-kind prefix followed by its
   canonical identity: a 128-bit `StreamName` for stream chunks, the matching
   tree identity for B+tree chunks, and the object identity for repository
   chunks. Validate that the prefix matches `chunk_type`. An empty owner key is
   retained only for records created before the extension and means
   shared/unattributed ownership; it must not be invented by new writers.
2. Have every active B+tree chunk use a nonzero random writer
   epoch, a persisted acknowledged byte cursor, and a configurable writer lease
   whose default is the existing 30 seconds. Every append and cursor advance
   carries that epoch and the expected chunk revision. A stale process cannot
   advance or seal the chunk after ownership has changed. The same contract
   already applies to stream, repository, and WAL chunks; the tree backend
   extends it to `BtreePage` and `PageIndex`.
3. Add a low-frequency writer-lease renewal operation to the private native
   chunk backend and chunkdb lifecycle API for each chunk type. Renew from
   chunkdb's server clock while the owner holds the chunk, even when no writes
   occur. Schedule renewal no later than one-third of the configured lease. Run
   it on the backend maintenance path, not the hot path, and use the existing
   per-chunk lifecycle guard rather than adding a new lock.
4. On clean rotation or shutdown, stop append admission, drain accepted
   writes, persist the final acknowledged cursor, seal the non-empty chunk,
   and delete an empty chunk. On process restart, never renew or append to
   any chunk from the previous process; allocate a new writer epoch and new
   chunk.
5. Extend chunkdb's bounded Active-chunk scan to include all chunk types.
   For an expired lease, acquire the lifecycle guard, re-read
   and recheck the epoch, lease, revision, state, and acknowledged cursor,
   then seal a nonempty chunk at exactly that cursor or delete a zero-length
   chunk and release its reservations. Bytes beyond the acknowledged cursor
   remain unreachable. A concurrent valid renewal wins and prevents cleanup.
6. Keep writer ownership and lease authority in durable chunk metadata. Every
   chunkdb instance startup restarts the bounded periodic scan over its
   currently bound ranges; its scan cursor may restart from the beginning.
   Range reassignment routes the same persisted chunk record to the new owner.
   No task correctness depends on the lifetime of any writer process or
   chunkdb process.
7. Reconcile and free never-consumed reservations through the existing cleanup
   intent path. Preserve consumed reservations until the writer lease and reuse
   grace expire, matching the shared small-write recovery contract.
8. Expose renewal success/failure, expired chunks found, chunks sealed or
   deleted, cursor bytes retained, reservation bytes reclaimed, scan lag, and
   retry counts.
   Repeated metadata or DiskDB failure leaves the chunk Active and retryable;
   it never guesses a later cursor.

## Dependencies

- Depends on R140 for B+tree chunk type usage, fresh-on-restart allocation,
  writer epochs, and acknowledged cursors, and on R141 for stream chunk
  identity and production allocation.
- Reuses chunkdb's durable writer fields, per-chunk lifecycle guard, bounded
  `list_chunks` scan, reservation reconciliation, and server-time lease logic.
  R146 makes their owner identity explicit and extends the sweep uniformly to
  `Stream`, `Repo`, `Wal`, `BtreePage`, and `PageIndex`.
- R147 consumes the resulting sealed chunks for physical strip reclamation.
- R142 supplies production tree ownership and shutdown sequencing but is not
  required for chunkdb's expiration test harness.

## Acceptance

- Given each chunk type and canonical owner identity, when its metadata is
  encoded and decoded, assert `owner_key` round-trips and a mismatched
  owner-kind prefix is rejected. Given an old record without the field, assert
  it decodes as shared/unattributed without changing its bytes. Invariant:
  cleanup can identify attributed owners without breaking existing records.
  Unit test.
- Given a live but write-idle chunk owner (any type) holds an Active chunk,
  when more than one lease period passes, assert maintenance renewals keep its
  persisted lease current and the chunkdb sweep does not seal it. Invariant:
  elapsed write idleness is not proof that a live owner disappeared. Integration
  test.
- Given a writer process crashes after acknowledging cursor `c` with later bytes
  unacknowledged (any chunk type), when the writer lease expires and chunkdb
  sweeps the record, assert the chunk is sealed at exactly `c` and bytes beyond
  `c` are not readable through the manifest. Invariant: recovery never promotes
  ambiguous writes. E2E test.
- Given a renewal races the expiration sweep, when both acquire the existing
  lifecycle guard, assert either the renewed lease remains Active or the
  expired epoch is sealed, with no append accepted after sealing. Invariant:
  lease renewal and sealing have one revision-ordered outcome. Integration
  test.
- Given any writer restarts before its old lease expires, when it writes again,
  assert it allocates a new chunk and writer epoch and never renews or appends
  to the old chunk. Invariant: process restart creates a new ownership
  generation. Integration test.
- Given chunkdb restarts after the writer crashes but before lease expiry,
  when the replacement chunkdb instance reloads its range and the durable
  deadline passes, assert its resumed sweep seals the old chunk. Invariant:
  orphan sealing survives chunkdb restart. E2E test.
- Given an expired attributed chunk has acknowledged cursor zero, when the
  sweep owns its lifecycle revision, assert the chunk and its unused
  reservations are deleted rather than sealed. Invariant: an allocation that
  published no durable byte does not become an empty sealed chunk. Integration
  test.
- Given chunkdb range ownership moves while an expired chunk is pending (any
  type), when the new owner begins its bounded scan, assert it seals the same
  durable record once and the former owner cannot mutate it. Invariant:
  cleanup follows range ownership without losing or duplicating state
  transitions. E2E test.
- Given an expired chunk has unused reservations and cleanup RPCs fail, when the
  sweep retries, assert sealing stays durable, qualified frees are idempotent,
  and no consumed extent is reused before the grace deadline. Invariant:
  cleanup failure cannot reopen the chunk or permit stale-writer corruption.
  Integration test.

Required gates:

- `pixi run tree-fmt`
- `pixi run tree-lint`
- `pixi run test-tree-ct`
- `pixi run test-tree-ffi`
- `pixi run -- cargo test -p crowdb-chunkdb --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-stream --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
