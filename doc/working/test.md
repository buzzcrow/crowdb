<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Test tracking

Track remaining issues and environment measurements. One row per domain;
record a complete run and keep its previous wall time. Detailed timings and
redacted diagnostics stay in the recorded artifact roots.

## intel7960

Linux Docker bridge; clients run on the host and use inspected container IPs
and fixed ports. Wall times include startup, clients and teardown, excluding
shared builds. Record complete domain runs, including failures. Current Console
and Deployment baselines use two case slots. New runs use 1024 CPU shares per
container without a CPU quota; complete native nodes retain 2048 MiB / 512 PIDs. Exact image and resource settings stay in run artifacts.

### Container domains

| Domain       | Date       | Passed / failed / skipped | Wall s   | Previous s |
| ------------ | ---------- | ------------------------- | -------- | ---------- |
| KV           | 2026-10-13 | 50 / 0 / 0                | 126.685  | 96.977     |
| Diskdb       | 2026-10-13 | 5 / 0 / 0                 | 17.637   | 15.142     |
| Diskio       | 2026-10-13 | 8 / 0 / 0                 | 27.230   | 21.935     |
| Chunkdb      | 2026-10-13 | 11 / 0 / 0                | 87.330   | 148.686    |
| Chunk client | 2026-10-13 | 40 / 0 / 0                | 349.746  | 642.098    |
| Chunk stream | 2026-10-13 | 4 / 0 / 0                 | 42.558   | 69.960     |
| Chunk-KV     | 2026-10-13 | 2 / 0 / 0                 | 30.160   | 37.811     |
| S3           | 2026-10-13 | 43 / 0 / 0                | 877.167  | 1545.602   |
| Iceberg      | 2026-10-13 | 32 / 0 / 0                | 2187.829 | 1762.795   |
| Dataset      | 2026-10-13 | 2 / 0 / 0                 | 23.609   | 35.568     |
| Console      | 2026-10-13 | 153 / 0 / 0               | 2223.972 | 2075.462   |
| Deployment   | 2026-10-13 | 33 / 0 / 0                | 310.503  | 314.358    |

- All domains pass on the `9c0f8805` debug release. Iceberg uses one case slot
  under the 4-CPU / 16384-MiB budget, including the original approximately
  20-minute retirement grace; its previous baseline used two slots. This
  concurrency difference prevents a direct performance comparison. CPU shares
  apply throughout the rerun; worker limits were introduced during that run.
  The final one-worker configuration also passes focused concurrent PUT and
  all six special profiles. Diagnostics: `/tmp/crowdb-r230-shares-public`.
- Special profiles pass on the final image: bridge SSH 148.600 s, Linux host
  7.759 s, containerd host/cluster 111.815 / 112.784 s, five-engine Iceberg
  127.531 s and Python/DuckDB 20.458 s. New runs use CPU shares and one Tokio
  worker per service.
- Important component UTs remain. Latest focused gates: DiskDB 177, monitor 75,
  CLI 6, fixture 106, UI 183, KV-server 64; these are component repair checks,
  not another complete-domain baseline.

## amd5950

No recorded measurements.

## macm5pro

No container measurements. Linux VM routing remains unverified.

## Remaining issues

- [ ] **Bridge cleanup timing**: a run overlapping special profiles exceeded
  the unchanged 45-second cleanup response bound. Independent diagnosis and
  final uninstrumented acceptance pass, including all cleanup assertions.
  Retain the failure evidence and investigate if it recurs; remote four-core
  CI performance remains unmeasured.
- [ ] **macOS Iceberg SDK**: Maven `dependency:go-offline` stalled for more than
  eight minutes. Rerun with the pinned dependency cache available.
- [ ] **WAL disk loss**: full fail-out and snapshot rejoin coverage awaits the
  fail-out implementation (`design-crowdb-kv-wal.md`, section 8.1).
- [ ] **Per-group WAL disks**: startup currently derives disks from store-level
  config; isolation coverage awaits per-group `wal_disks` configuration.
- [ ] **Network partition**: add sever/restore acceptance with an owned partition
  mechanism. Existing crash/restart acceptance does not prove partition behavior.
- [ ] **CLI command and benchmark review**: redesign command definitions and
  benchmark regression ownership in a later review. Current CLI lifecycle moves
  to container clusters; keep `tools/benchmark/bench-chunk-kv-regression.sh`
  until that review decides its scope.
