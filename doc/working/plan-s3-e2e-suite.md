<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 Basic End-to-End Acceptance Plan

Upstream: [R166](../backlog/R166-s3-e2e-suite.md).

## Baseline

- [x] **Run owned-stack CRUD**: boto3 compatibility, two access listeners,
  real sparse block storage, 2+1 unsafe-colocated EC, range, list, ETag,
  SigV4, and lost response are covered. Files:
  `app/crowdb-access-server/tests/s3_full_stack_test.rs`,
  `app/crowdb-access-server/tests/s3_e2e/`.
- [x] **Verify access, ChunkDB, and DiskDB replacement**: committed objects
  remain visible across these process restarts. Files: same.

## Recovery failure

- [ ] **Repair Chunk-KV restart recovery**: after DiskIO replacement, restarting
  Chunk-KV repeatedly fails to recover a persisted stream/tree with `chunk
  layout expired before its reads completed`; access-server returns 503. Locate
  and fix the stale/expired layout boundary, add a regression test, then rerun
  the full owned-stack test. Files: Chunk-KV stream recovery, chunk reader, and
  S3 full-stack test as evidence directs.

## Verification and cleanup

- [ ] **Run R166 gates**: full s3-e2e, access-s3 tests, fmt, and focused strict
  clippy.
- [ ] **Delete R166 and this plan** only after all individual-restart checks
  pass. R172 remains active for its expanded fault and scale-out matrix.
