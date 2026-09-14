<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R159: access server / S3 — DeleteObject and owned-chunk reclamation

## Problem

Removing visible metadata and reclaiming bytes are separate operations. A
large object owns complete chunks that can be deleted cheaply, but immediate
reclamation can race readers holding the old immutable generation. A retry or
lost response must not enqueue conflicting cleanup or make deletion depend on
shared-chunk GC.

The deletion model is
`doc/design/accessserver/design-crowdb-access-server-s3.md` §5.

## Solution

1. Give each logical delete a stable operation identity. Atomically replace the
   current visibility record with an absent generation/tombstone and retain the
   previous immutable generation as cleanup input. Repeated delete follows the
   selected S3 idempotency contract.
2. Classify every data reference as dedicated whole chunks or shared ranges.
   Dedicated chunks enter a durable, bounded cleanup queue after the reader-
   validity grace period. Their reclamation does not use R92 or R95.
3. Delete dedicated chunks idempotently through `crowdb-chunk-client`, record
   per-chunk completion, retry transient failures with limits, and reconcile an
   unknown response by reading chunk state.
4. For a shared range, make metadata invisible first, record durable pending
   range cleanup, and invoke the qualified R95 chunk-range-delete contract when
   available. Until then, the range remains logical garbage and deletion still
   succeeds. The call-site comment names the R95 dependency.
5. Apply the same cleanup record to overwritten generations. Never reclaim a
   generation while an admitted reader may still hold it.

## Dependencies

- Depends on R153 and R154 generation/publication identities.
- Uses whole-chunk delete from `crowdb-chunk-client` for large objects.
- R95 and R168 are required only for physical shared-range reclamation; their
  absence cannot block metadata deletion or whole-chunk reclamation.
- R169 later compacts residual shared garbage and metadata tombstones.

## Acceptance

- Given a reader pinned to a large object's old generation, when DELETE removes
  visibility and cleanup runs before and after the grace period, assert new
  readers see absence and old chunks disappear only after reader validity.
  Invariant: visibility removal precedes safe reclamation. E2E test.
- Given a lost delete or chunk-delete response and repeated retries, when the
  reconciler runs, assert one logical deletion completes and every owned chunk
  reaches a known terminal state. Invariant: deletion and cleanup are
  idempotent. Integration test.
- Given a shared small object while R95 is unavailable or returns failure, when
  DELETE completes, assert metadata is absent and durable pending cleanup names
  the exact qualified range. Invariant: unavailable physical reclamation does
  not restore visibility or delete neighboring bytes. Integration test.
- Given an overwrite and delete race, when their metadata compares resolve,
  assert cleanup targets only generations no longer selected by visibility.
  Invariant: cleanup cannot reclaim the winning generation. Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
