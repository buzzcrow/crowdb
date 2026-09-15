<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R168: access server / S3 — Shared small-object reclamation

## Status

**Deferred until R95 qualified chunk-range deletion is implemented and basic
S3 deletion is stable.** Before then, R159 removes metadata and records exact
pending ranges as safe logical garbage.

## Problem

Small objects use the shared chunk writer to avoid private-chunk waste. Their
bytes cannot be reclaimed by deleting the whole chunk, and an unqualified byte
range could erase neighboring live objects or a reused generation. R159 needs
a restart-safe way to turn pending logical garbage into safe range deletion.

The lifecycle boundary is
`doc/design/accessserver/design-crowdb-access-server-s3.md` §5.

## Solution

1. Persist a range-cleanup record containing object generation, chunk identity,
   writer/layout generation, exact offset/length, reader-validity deadline, and
   stable cleanup identity before invoking physical deletion.
2. After metadata invisibility and the grace period, call R95's idempotent
   `DeleteChunkRange(chunk_id, offset, size)` interface. The RPC exists before
   R95 but returns not-implemented and performs no mutation until chunkdb owns
   the required range validation and used-bitmap lifecycle.
3. Reconcile unknown results from chunk state, retry bounded transient failures,
   and quarantine generation/ownership mismatches for inspection. Never widen,
   merge, or align a range across live neighbors merely to reclaim space.
4. Track logical garbage, eligible bytes, completed bytes, retry age, and
   quarantine without high-cardinality labels. Apply backpressure to cleanup
   independently of foreground DELETE.
5. Mark the S3 cleanup complete only after the qualified range is known deleted;
   absence of cleanup never changes the object's already-absent visibility.

## Dependencies

- Depends on R95 and R159.
- Uses R153 immutable generation/data references and R161 cleanup admission.
- R169 may compact fragmented shared chunks after individual dead ranges are
  marked; it is not required for correctness here.

## Acceptance

- Given adjacent live and deleted small objects in one shared chunk, when the
  qualified range deletion runs after grace, assert only the deleted object's
  exact bytes become reclaimable. Invariant: shared reclamation cannot damage
  a neighbor. E2E test.
- Given stale chunk/writer generation or a range that has been reused, when
  cleanup runs, assert R95 rejects/quarantines it without mutation. Invariant:
  offset and length alone are never deletion authority. Integration test.
- Given lost replies, restart, and repeated cleanup delivery, when the
  reconciler settles, assert one terminal result is recorded and visibility
  stays absent throughout. Invariant: range cleanup is idempotent and separate
  from logical deletion. E2E test.
- Given a sustained delete backlog, when configured cleanup limits are reached,
  assert foreground object operations retain their reserved resources and
  garbage metrics remain bounded-cardinality. Invariant: reclamation cannot
  starve the data path. Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run -- cargo test -p crowdb-chunkdb --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
