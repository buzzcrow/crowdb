<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R154: access server / S3 — Atomic object publication and upload recovery

## Problem

An S3 PUT spans durable metadata and one or more chunk writes. Publishing
metadata before all chunks are sealed exposes partial data; writing chunks
first without a durable upload identity leaks storage after crashes. Timeouts
also make publication outcomes ambiguous and can publish two generations for
one logical retry.

The visibility model is
`doc/design/accessserver/design-crowdb-access-server-s3.md` §3.

## Solution

1. Persist an upload intent keyed by stable upload ID before allocating object
   data. It records tenant, bucket, key, expected predecessor generation,
   request identity, state, and owned data references as they become durable.
2. Let R155 write, finish parity, seal dedicated chunks, and persist the final
   length and integrity result before publication is eligible.
3. Publish one immutable generation and atomically replace the object's
   visibility value with a compare on its expected predecessor. The visibility
   value includes the upload ID so an ambiguous response can be reconciled by
   read rather than guessed.
4. Mark the upload intent complete after observing its exact published
   generation. A retry with the same identity returns that result; a conflicting
   body or predecessor fails without publishing another generation.
5. Run a bounded startup and periodic reconciler over non-complete intents. It
   completes an already-published intent or records idempotent whole-chunk
   cleanup for unpublished private data. It never promotes a partially sealed
   upload.
6. On overwrite, retain the replaced immutable generation until R159's reader
   grace and cleanup contract permits reclamation. Publication does not wait
   for old-data deletion.

## Dependencies

- Depends on R153 metadata keys and generations.
- R155 supplies streaming write completion and R164 supplies integrity data.
- Uses Chunk-KV compare/update semantics and chunk-client seal/delete APIs.
- R159 owns reclamation of replaced generations; its absence may retain old
  chunks but cannot weaken publication correctness.

## Acceptance

- Given failures before allocation, during data writes, after seal, during
  visibility compare, and after publication response loss, when reconciliation
  runs, assert readers see only the old or complete new generation. Invariant:
  partial object bytes are never visible. E2E test.
- Given a lost publication response, when the same upload ID retries, assert a
  visibility read returns the original generation and no second generation is
  created. Invariant: ambiguous completion is idempotent. Integration test.
- Given two different uploads race on the same predecessor, when both publish,
  assert only one compare succeeds and the loser remains unpublished and
  reclaimable. Invariant: one predecessor has at most one winning replacement
  through that compare. Integration test.
- Given the service restarts with intents in each state, when the reconciler
  resumes, assert published work completes and unpublished private chunks are
  eventually deleted exactly once. Invariant: recovery is durable and bounded.
  E2E test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
