<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R95: chunkdb — Qualified chunk range deletion and orphan scan

## Problem

Shared chunks contain ranges owned by different objects or multipart parts.
Deleting a whole chunk for one unreachable range can erase live neighbors.
Uploads may also write a chunk and fail before their part or final object
reference is recorded. A cleanup queue populated by each MPU mutation would
add metadata writes to the upload path and still miss those pre-record crashes.

## Solution

1. Complete `DeleteChunkRange(chunk_id, offset, size)` in the chunkdb protocol,
   client and server. The existing stub remains explicitly not implemented
   until range validation, used-bitmap updates, idempotency and in-chunk GC can
   prove that the exact physical range is safe to retire.
2. Use a bounded, chunk-centered scanner to find old chunks and ranges whose
   bytes have no live owner. Do not create per-MPU cleanup intents, candidate
   records or a durable cleanup queue. Start with a configurable age threshold
   of one day; a candidate must be older than the threshold after its last
   write. Age alone never authorizes deletion.
3. Compare each candidate against published S3 objects, Iceberg file
   descriptors, active multipart sessions and parts, frozen completion
   selections, in-flight writers and reader protection. A lost part-publication
   response or missing intermediate MPU record is not proof that a chunk is
   unused. Refuse deletion when reference or reader state cannot be confirmed.
4. Keep S3 MPU session, current-part and immutable part-generation keys under
   a common upload prefix in Chunk-KV so the scanner can enumerate one upload's
   references with a bounded prefix scan. The S3 multipart authority owns that
   key layout and the metadata-only Abort/expiry transition; old part
   generations remain available until the scanner proves their bytes are
   unreachable. An aborted or expired upload becomes a candidate only after
   the age gate and reference checks.
5. Recheck chunk identity, layout generation and exact range against current
   authority immediately before reclaim. Treat lost delete replies
   idempotently and keep an in-memory scan cursor and bounded work budget.
   Restart may rescan old chunks. Report examined,
   eligible, deferred and reclaimed bytes without per-upload metric labels.

## Dependencies

- R92 supplies in-chunk strip reclamation after R95 qualifies dead ranges.
- The S3 multipart authority supplies grouped MPU keys and durable session,
  part and completion references. The scanner also recognizes Iceberg's frozen
  MPU selection.
- Reader protection and published generation references must be queryable
  before physical deletion is enabled; R168 may use the qualified range-delete
  interface for ordinary S3 object deletion.

## Acceptance

- Given a shared chunk with live and unreachable ranges, when the scanner
  evaluates a candidate older than one day, assert only the exact unreachable
  range is passed to `DeleteChunkRange`. Invariant: a live neighbor is never
  reclaimed. E2E test.
- Given an MPU part that was replaced, aborted or expired, when its grouped KV
  prefix is scanned, assert old locations remain protected by any active or
  frozen selection and become eligible only after the age and reference
  checks. Invariant: no extra MPU cleanup record is required. Integration test.
- Given a write that crashed before part metadata publication, when the chunk
  scan runs, assert it defers the chunk until the age gate and in-flight writer
  proof settle, then discovers the orphan without a part record. Invariant:
  missing upload metadata alone cannot reclaim data. Integration test.
- Given a current reader, changed layout generation or uncertain authority,
  when range deletion is attempted, assert no bytes are reused. After the
  reader releases and identity is confirmed, repeat the same delete through a
  lost reply and assert one idempotent result. Invariant: reclamation is fenced
  by current chunk and reader authority. Integration test.
- Given a large backlog and foreground load, when scanning runs, assert it
  respects item, byte and time budgets and reports deferred/reclaimed progress.
  Invariant: cleanup cannot monopolize the data path. Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-chunkdb --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
