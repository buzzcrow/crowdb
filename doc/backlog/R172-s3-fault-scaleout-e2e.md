<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R172: access server / S3 — Fault and scale-out E2E matrix

## Problem

Basic S3 CRUD now composes with the lightweight one-zone topology, but its
recovery and horizontal-routing guarantees need an explicit, reproducible test
matrix. A benchmark is not required for this requirement.

The current R166 baseline exposes an earlier recovery defect: after DiskIO is
replaced and Chunk-KV is restarted, Chunk-KV cannot reopen a persisted
stream/tree because its chunk reader exhausts valid layouts. The server retries
the durable transition and S3 HEAD returns 503. R166 owns repairing that
single-service baseline before R172 expands the matrix.

## Solution

1. Extend `app/crowdb-access-server/tests/s3_full_stack_test.rs` with a named
   fault matrix using real block-disk storage. Keep the existing coverage for
   access-server restart, ChunkDB restart, DiskDB restart, DiskIO restart,
   Chunk-KV restart, lost reply, bucket/object CRUD, range GET, ListObjectsV2,
   ETag, and SigV4 boto3 requests.
2. Add missing cases: interrupted PUT and retry identity, stale Group-0 route
   refresh, slow reader backpressure, pool exhaustion, concurrent overwrite /
   delete / GET, Chunk-KV owner handoff, and cleanup after delete.
3. Run the same namespace through two access-server listeners and multiple
   Chunk-KV owners. Assert no listener-local authority, exact CRUD visibility,
   and idempotent retries after ownership change.
4. Retain raw logs and test topology on failure. Performance, copy accounting,
   and throughput comparisons are deferred to a separate requirement.

## Dependencies

- Depends on R152–R166 and uses the S3 E2E harness.
- Requires R144 for partition merge coverage and R103 for ChunkDB range
  migration coverage; their cases remain explicitly skipped until landed.

## Acceptance

- Given every completed restart and lost-reply case, when CRUD is retried,
  assert bytes, ETags, bucket namespace, and idempotent outcomes survive.
  Invariant: committed visibility survives process replacement. E2E test.
- Given each missing transport, concurrency, and resource-pressure case, when
  the fault settles, assert no permanent unavailable marker is created for a
  transient failure and no request leaks frontend-local state. Invariant:
  retries preserve logical consistency. E2E test.
- Given two access servers and multiple Chunk-KV owners, when requests move
  between endpoints and owners, assert the same CRUD result and no duplicate
  mutation. Invariant: S3 routing is horizontally portable. E2E test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-server --features s3-e2e --test s3_full_stack_test`
- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run -- cargo clippy -p crowdb-access-server -p crowdb-access-s3 --all-targets -- -D warnings`
