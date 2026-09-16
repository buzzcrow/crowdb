<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 End-to-End Suite Plan

Upstream: [R166](../backlog/R166-s3-e2e-suite.md),
[S3 design](../design/accessserver/design-crowdb-access-server-s3.md) §7, and
[Access Server design](../design/accessserver/design-crowdb-access-server.md) §10.

Goal: prove the advertised S3 subset, storage-boundary correctness, failure
semantics, stateless scale-out, and measured data-path behavior against owned
real services rather than mocks.

## Harness and compatibility

- [x] **Own the compact production stack**: start KV, one DiskDB, one DiskIO,
  one disk group/disk/zone, unsafe-colocated ChunkDB, Chunk-KV, credential
  issuance, and access-server with 2+1 EC; wait for readiness and tear down all
  children. Files: `app/crowdb-access-server/tests/s3_full_stack_test.rs`,
  `pixi.toml`.
- [~] **Complete SDK and raw HTTP matrix**: cover every supported bucket/object
  operation, conditions, range, pagination, stable errors, unsupported
  selectors, and SigV4 through boto3 plus signed raw requests. Files:
  `app/crowdb-access-server/tests/s3_e2e/basic.py`.
- [ ] **Exercise storage boundaries**: deterministic fragmented bodies for
  empty/tiny, frame, block, EC strip, configured test-chunk, and multi-chunk
  boundaries; include UTF-8/binary-safe encoded keys and exact ETag/range/body
  reference checks. Files: E2E Python and access-server test configuration.

## Recovery and scale-out

- [ ] **Restart and retry durable transitions**: restart access-server and
  storage owners around completed PUT/overwrite/delete, repeat requests after
  lost-response simulation, and assert atomic visibility/idempotence. Reuse
  process fixtures and expose only deterministic fault hooks.
- [ ] **Backpressure and race coverage**: exercise slow request/response peers,
  exhausted native credits, overwrite/read/delete races, chunk errors, and
  routing refresh while bounding memory and cleanup targets.
- [ ] **Stateless frontend scale-out**: run two independently configured access
  servers over the same metadata/chunk authorities and alternate retries,
  reads, pagination, overwrites, and deletes between them.

## Performance evidence

- [ ] **Add reproducible benchmark matrix**: compare direct chunk client,
  loopback S3 PUT/GET/range, object sizes, and concurrency; record environment,
  throughput, latency percentiles, TTFB, CPU/RSS, network bytes, allocations,
  copy/view counters, and raw artifacts without universal hardware thresholds.
- [ ] **Run acceptance and gates**: run focused tests, compact full-stack boto3,
  chunk-client tests, scoped fmt, workspace lint, then remove R166 and this
  plan in the final cleanup commit. Preserve the known Hyper formatting issue
  under R152 rather than rewriting the fork.

## Files

- `app/crowdb-access-server/tests/s3_full_stack_test.rs`
- `app/crowdb-access-server/tests/s3_e2e/basic.py`
- `app/crowdb-access-server/src/main.rs`
- `pixi.toml`
- focused fault/benchmark helpers under the access-server test tree

## Tests

- SDK/raw wire compatibility matrix against the owned compact stack.
- Exact boundary and randomized-fragmentation round trips.
- Restart, lost-response, dependency-failure, slow-peer, race, and scale-out
  scenarios with deterministic settling conditions.
- Explicit benchmark artifacts and telemetry reconciliation.
