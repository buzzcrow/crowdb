<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R159: access server / S3 — DeleteObject

## Problem

Object visibility must disappear cheaply and idempotently. Reading the old
record, creating a delete identity, or using CAS would add metadata round trips
to a normal object operation without improving the S3 overwrite model.
Physical reclamation is separate: a shared chunk cannot be deleted for one
object, and immediate dedicated-chunk deletion can race an admitted reader.

The deletion model is
`doc/design/accessserver/design-crowdb-access-server-s3.md` §5.

## Solution

1. Resolve the active bucket name, then issue exactly one unconditional
   Chunk-KV delete for `tenant / bucket ID / object key`. Do not read, CAS,
   tombstone, version, or allocate an operation identity on this object path.
2. Treat missing and repeated deletes as S3 success. A definite KV rejection
   returns a stable error; a transport timeout is explicitly ambiguous because
   the delete may have applied.
3. Return after the logical KV result. Physical cleanup is asynchronous and
   never extends DELETE latency.
4. Reclaim dedicated chunks only through later safe garbage discovery. Shared
   ranges remain logical garbage until qualified range deletion is available;
   never delete their whole containing chunk.
5. Define the chunkdb `DeleteChunkRange(chunk_id, offset, size)` client and RPC
   boundary now. Until R95 implements chunk-local range lifecycle, the chunkdb
   handler returns an explicit not-implemented result and mutates no bytes.

## Dependencies

- Depends on the direct object key and one-mutation contract.
- Uses Chunk-KV routed point deletion.
- Qualified shared-range reclamation is deferred and does not block logical
  deletion.

## Acceptance

- Given an existing or missing object, when DELETE runs, assert it performs one
  unconditional point delete and returns the S3 idempotent result without a
  metadata read or CAS. Invariant: DELETE has one KV operation. Integration
  test.
- Given a definite KV error or transport timeout, when DELETE returns, assert
  the outcomes are respectively coded failure and ambiguous timeout without
  synchronous chunk deletion. Invariant: unknown metadata state cannot trigger
  destructive cleanup. Integration test.
- Given an object in a shared chunk, when metadata deletion succeeds, assert no
  whole-chunk delete occurs. Invariant: deleting one object cannot erase its
  neighbors. Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
