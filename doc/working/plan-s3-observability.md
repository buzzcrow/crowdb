<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 Observability Plan

Upstream: [R165](../backlog/R165-s3-observability.md) and
[Access Server design](../design/accessserver/design-crowdb-access-server.md) §§8–10.

Goal: make every supported S3 terminal outcome and zero-copy fallback auditable
with bounded, non-blocking telemetry and dependency-aware service health.

## Request lifecycle

- [x] **Complete bounded request metrics**: record terminal outcome exactly
  once, total latency, response-ready/TTFB latency, request/response bytes,
  current and peak concurrency, and stable phase totals per operation/outcome.
  Keep authentication ahead of storage dispatch and exclude namespace or
  credential labels. Files: `lib/crowdb-access-s3/src/metrics.rs`,
  `app/crowdb-access-server/src/s3/dispatcher.rs`, tests.
- [x] **Propagate correlation identity**: carry the request ID through the
  operation context and cleanup records without logging keys or credentials.
  Files: `app/crowdb-access-server/src/s3/`,
  `lib/crowdb-access-s3/src/`.

## Data-path accounting

- [x] **Unify native and chunk counters**: expose pool ownership, direct and
  prefetched bytes, framed owner/views, payload-copy fallback, checksum work,
  backpressure, metadata/chunk retries, and cleanup backlog in bounded
  snapshots. Files: `lib/crowdb-access-s3/src/native_buffer.rs`,
  `lib/crowdb-access-s3/src/metrics.rs`, `lib/crowdb-chunk-client/src/metrics.rs`.
- [x] **Assert fallback separation**: verify normal owner, prefetched edge,
  vectored, and generic-copy paths cannot be reported as the same zero-copy
  outcome. Files: affected crate integration tests and S3 E2E harness.

## Operational health

- [x] **Model liveness and readiness**: report listener, metadata, chunk,
  native-pool, cleanup, and authentication state; liveness remains independent
  of exporter health and readiness fails only when safe admission is
  impossible. Files: `lib/crowdb-access-s3/src/metrics.rs`,
  `app/crowdb-access-server/src/`.
- [x] **Expose health endpoints/status**: serve bounded header-only or JSON
  status outside the S3 operation namespace without starting it when S3 is
  disabled. Files: `app/crowdb-access-server/src/`, integration tests.

## Verification and cleanup

- [~] **Acceptance tests**: cover all terminal classes, phase reconciliation,
  cardinality independence, exporter failure, dependency degradation, and
  compact-stack boto3 evidence. Files: access S3/server tests and E2E harness.
- [ ] **Required gates**: run affected tests separately, workspace fmt,
  `pixi run rs-lint`, then delete this plan and R165 backlog entries in the
  final cleanup commit. Files: workspace.

## Files

- `lib/crowdb-access-s3/src/metrics.rs`
- `lib/crowdb-access-s3/src/native_buffer.rs`
- `lib/crowdb-chunk-client/src/metrics.rs`
- `app/crowdb-access-server/src/s3/`
- `app/crowdb-access-server/src/main.rs`
- affected crate tests and `app/crowdb-access-server/tests/s3_full_stack_test.rs`

## Tests

- Unit: bounded arrays, outcome/phase accounting, health truth table.
- Integration: dispatcher terminal outcomes, concurrency, TTFB, exporter and
  dependency degradation.
- E2E: compact 2+1 unsafe-colocated stack with boto3 and copy/view snapshots.
