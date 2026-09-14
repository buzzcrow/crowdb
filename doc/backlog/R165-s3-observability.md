<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R165: access server / S3 — Metrics, tracing, and operational status

## Problem

A stateless streaming service needs enough visibility to distinguish HTTP,
metadata, chunk, admission, copy, and cleanup bottlenecks. Per-object labels or
unbounded trace fields would themselves become a scale and privacy problem,
while aggregated success counters can hide fallback copies and pending garbage.

The measurement boundaries are
`doc/design/accessserver/design-crowdb-access-server.md` §§8–10.

## Solution

1. Emit bounded-cardinality request counts, status classes, latency,
   time-to-first-byte, bytes, concurrency, and phase durations by operation and
   stable outcome class. Do not label metrics by bucket, key, upload ID, chunk,
   node, disk, or client credential.
2. Measure native pool ownership, view counts, backpressure, send-queue
   flushes, coalesced bytes, body-prefix copies, checksum work, metadata retries,
   chunk retries, and cleanup backlog so claimed zero-copy scope is auditable.
3. Propagate one request/operation correlation ID through access server,
   Chunk-KV, chunk client, and cleanup records. Traces may include sampled,
   redacted namespace context but no secrets or reusable transport credentials.
4. Add readiness and liveness status for listener, metadata client, chunk
   client, native pools, reconciler backlog, and authentication provider.
   Readiness fails when new operations cannot reach a supported safe outcome.
5. Keep observability non-blocking and bounded; exporter failure drops or
   aggregates telemetry rather than blocking the data path.

## Dependencies

- Depends on R152 and integrates R154–R164 phase/error contracts.
- R166 consumes the metrics as performance and correctness evidence.
- R170 adds accelerated-transfer metrics later and must preserve the same
  fallback separation principle.

## Acceptance

- Given success, protocol error, timeout, throttling, ambiguous retry, and
  cleanup failure for every basic operation, when metrics are collected,
  assert counts and phase durations reconcile with requests and bytes.
  Invariant: every terminal outcome is observable exactly once. Integration
  test.
- Given normal pooled, prefix-copy, vectored, queue-flush, and coalesce paths,
  when transfers complete, assert copy/view counters distinguish each path.
  Invariant: fallback work cannot be counted as zero-copy. E2E test.
- Given many unique object keys and credentials, when telemetry is exported,
  assert metric series count remains bounded and secrets/raw keys are absent.
  Invariant: observability cardinality is independent of namespace size. Unit
  test.
- Given exporter failure and dependency degradation, when traffic continues,
  assert data-path work does not block and readiness reflects only service
  ability, not exporter availability. Invariant: telemetry failure cannot halt
  storage service. Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
