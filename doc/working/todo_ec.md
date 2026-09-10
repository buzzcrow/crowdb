<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# EC Implementation Follow-ups

Status: complete. No open implementation item remains from this list.

## Resolved

- **Consumed-reservation recycling fence:** DiskIO writes now carry the segment
  allocation generation and allocation base. DiskIO serializes writes per base,
  durably journals a newer generation before data submission, and rejects stale
  generations across restart. New reservation records persist planned cursors,
  so expired consumed reservations can be cancelled and reclaimed safely.
  Legacy consumed records without that proof remain allocated fail-safe.
- **Completed-group crash takeover:** a bounded rotating reservation scanner
  directly admits complete special groups with deterministic task identities.
  It also cancels expired incomplete groups under the generation-fence rule, so
  an old key prefix cannot starve later records.
- **Global reservation admission bound:** ChunkDB enforces lock-free atomic
  block and byte quotas. The configured cluster limits are divided by owned
  range share, rebuilt from durable records at startup, refreshed when range
  ownership changes, and exported through usage gauges and a rejection counter.
- **Benchmark reset readiness:** strict-range ChunkDB `/ready` now succeeds only
  after the process has loaded an owned bucket range. Cluster reset therefore
  waits for usable range ownership, not only registry publication.
- **Foreground EC throughput attribution:** the 60-second A/B sentinel reports
  parity bytes, reservation wait, watchdogs, and paired EC/mirror throughput.
  It rejects errors, incomplete objects, watchdogs, and an EC/mirror ratio below
  70%.

## Verification

- 1 KiB/32 threads: mirror 33,729.19 objects/s, EC 33,807.27 objects/s
  (100.23%), zero errors, incomplete objects, and watchdogs. Result:
  `bench-log/chunkio-small-write-20260910-081757`.
- Clean 1 KiB/128-thread reproduction: mirror 107,420.80 objects/s, EC
  98,835.24 objects/s (92.01%), zero errors, incomplete objects, and watchdogs.
  Foreground parity wrote 432,013,312 bytes. Result:
  `bench-log/chunkio-small-write-128-repro-20260910`.
- One earlier 128-thread sample was invalid after a KV server exited and the
  resulting leader loss triggered watchdogs. The exact clean reproduction did
  not reproduce either failure, so it is not an open EC implementation issue.
- Rust format, full clippy, workspace test build, unit tests, server suites,
  DiskIO client tests, and 114 DiskIO C++ tests pass. The two unrelated
  crowdb-tree GC snapshot tests still fail in the unchanged tree code; tree lint
  is unavailable because the environment's clang sysroot lacks standard and
  third-party headers. Changed DiskIO C++ files pass clang-format.
