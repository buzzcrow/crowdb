<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R233: SDK — Local disk cache and checkpoint staging

Status: Deferred until R231/R232 provide measured HTTP upload and read baselines.

Problem:

- Training reads may reuse immutable payloads; checkpoint writers may need a
  short local-write pause before background upload. The
  [Dataset contract](../design/access-server/dataset/spec-crowdb-dataset.md)
  defines remote Snapshot publication, not local checkpoint completion.
- Treating a local write as a remotely durable checkpoint would report success
  before another machine can recover it. Local staging can absorb bursts but
  cannot compensate indefinitely for remote bandwidth below sustained writes.

Solution:

1. Add optional Rust cache/staging components under `sdk/`, exposed through the
   same Python wrapper as the HTTP client. Rust owns journal recovery, disk
   quotas, eviction and background upload; Python exposes configuration and
   completion state without duplicating these mechanisms. Keep default HTTP
   clients diskless. Key read cache entries by endpoint/security scope, Dataset,
   immutable Snapshot, field/range and digest. Validate bytes before admission;
   never cache mutable latest as immutable identity. A cache hit does not extend
   remote retention or bypass server authorization/lease policy.
2. Separate evictable clean cache from uncommitted staging. Bound disk usage,
   memory and upload concurrency. Evict clean entries first; apply explicit
   backpressure or fail admission when staging reaches quota. Never silently
   evict the only pending copy. Keep hot reads lock-free; any proposed lock
   requires the repository's explicit contention/trade-off review.
3. Use a recoverable local journal and durable file/directory synchronization.
   Expose distinct Staging, LocalDurable, Uploading, RemoteCommitted and Failed
   states. LocalDurable means local bytes and replay information are durable;
   it does not survive loss of that disk. RemoteCommitted requires payload
   durability and successful Snapshot publication. `flush`/remote wait returns
   that Snapshot or an error/ambiguous operation identity, never local success.
4. Resume after process restart using the same upload and publication tokens.
   Local corrupt/incomplete files fail validation; partial remote uploads remain
   invisible. Retain pending local data after transient failure and report quota,
   disk-full and shutdown outcomes. Cleanup follows remote commitment or explicit
   discard; remote ownership rules still govern uploaded bytes.
5. Keep model serialization and coordinated multi-rank checkpoint assembly in
   adapters. Publish one complete Snapshot only after all declared checkpoint
   components exist. Measure pause time, remote completion latency, sustained
   throughput, disk/memory bounds and restart recovery against direct streaming.
   Do not claim a performance improvement without these workload measurements.

Dependencies:

- R231 upload sessions and publication recovery; R232 HTTP SDK and metrics.
  HTTP streaming remains the fallback without local storage or when caching is
  disabled. No RDMA or fat client is required for this feature.
- R220 may reuse these components after proving the same visibility contract.

Acceptance:

1. Given identical fields in different snapshots/security scopes -> populate
   and corrupt cache entries and advance latest -> assert no cross-scope reuse,
   checksum rejection and fixed Snapshot identity. Cache invariant. Integration test.
2. Given a slow server and small disk quota -> stage repeatedly and exhaust the
   disk -> assert bounded usage, clean eviction, explicit backpressure and
   preservation of pending bytes. Capacity invariant. Integration test.
3. Given a local checkpoint -> stop after each durable state and restart ->
   assert LocalDurable never implies RemoteCommitted, journal replay preserves
   identity, and remote wait returns only a published Snapshot. Durability invariant. Integration test.
4. Given response loss, partial upload and corrupt local data -> recover/retry
   or shut down -> assert no partial publication, duplicate Snapshot or silent
   discard and explicit failure outcomes. Recovery invariant. Integration test.
5. Given a declared multi-rank checkpoint and measured baseline -> delay one
   component and compare staged/direct runs -> assert complete-only publication
   and record pause/completion latency, throughput and resource bounds.
   Checkpoint completeness invariant. E2E test.
6. Given the same staged operation -> inspect and resume it through Rust and
   the Python wrapper -> assert identical journal identity, durability states,
   quota errors and remote completion. Shared implementation invariant. Integration test.

Verification commands (SDK workspace supplied by R232):

- `pixi run -- cargo test --manifest-path sdk/Cargo.toml --workspace`
- `pixi run -- python -m unittest discover -s sdk -p 'test_*.py'`
- `pixi run clean-env && pixi run test-e2e-dataset`
- `pixi run -- cargo fmt --manifest-path sdk/Cargo.toml --all -- --check`
- `pixi run -- cargo clippy --manifest-path sdk/Cargo.toml --workspace --all-targets -- -D warnings`
- `pixi run rs-fmt-check`
- `pixi run rs-lint`
