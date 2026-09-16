<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R166: access server / S3 — Basic end-to-end acceptance

## Status

**Active.** The compact real-service topology proves SDK CRUD, but its
Chunk-KV restart recovery currently fails. Expanded faults, scale-out, and all
performance evidence are deferred to R172.

## Problem

Unit coverage cannot prove that the S3 listener, credentials, Chunk-KV routing,
ChunkDB, DiskDB, DiskIO, and durable storage compose after process replacement.
The required basic service must retain committed CRUD visibility across each
single-service restart before it is declared delivered.

## Solution

1. Keep the owned `boto3` E2E topology with KV, one disk group, one DiskDB,
   one DiskIO, unsafe-colocated 2+1 EC ChunkDB, Chunk-KV, and two access
   listeners. This is a composition topology, not a failure-domain claim.
2. Verify bucket/object CRUD, range GET, ListObjectsV2, ETag, SigV4, and lost
   response retry against real storage.
3. Restart access-server, ChunkDB, DiskDB, DiskIO, and Chunk-KV after durable
   PUT/overwrite/delete, and verify exact committed bytes, ETags, and namespace
   state after each restart.
4. Fix the earliest failed recovery boundary rather than extending a timeout or
   hiding the failure behind caller retries. Retain logs and the topology on
   failure.

## Dependencies

- Depends on the delivered basic S3 behavior from R152–R164.
- R172 extends this suite after its single-service recovery baseline passes.

## Acceptance

- Given a full owned topology, when boto3 runs supported basic bucket and
  object operations, assert exact S3-compatible results. E2E test.
- Given each individual service restart after committed object changes, when
  the service is ready, assert HEAD, GET, range, list, and deletion state are
  exact without frontend-local authority. E2E test.

Required gates:

- `pixi run -e s3-e2e test-s3-e2e`
- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run -- cargo clippy -p crowdb-access-server -p crowdb-access-s3 --all-targets -- -D warnings`
