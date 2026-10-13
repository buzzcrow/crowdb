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
| Diskdb       | 2026-10-13 | 5 / 0 / 0                 | 12.679   | 17.637     |
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

### Component tests

One row per owning domain, combining its Rust/C++ packages and SDK protocol
peers. Wall times include compilation inside each component command; the shared
C++ prerequisite build is excluded. Cache state affects comparisons. All gates
pass without skipped cases; UI also passes its lint/type checks.

| Domain              | Date       | Passed / failed / skipped | Wall s  | Previous s |
| ------------------- | ---------- | ------------------------- | ------- | ---------- |
| access              | 2026-10-13 | 117 / 0 / 0               | 125.050 | —          |
| chunk-client        | 2026-10-13 | 125 / 0 / 0               | 41.324  | —          |
| chunk-kv            | 2026-10-13 | 148 / 0 / 0               | 213.867 | —          |
| chunk-stream        | 2026-10-13 | 50 / 0 / 0                | 26.958  | —          |
| chunkdb             | 2026-10-13 | 158 / 0 / 0               | 113.006 | —          |
| common              | 2026-10-13 | 112 / 0 / 0               | 12.033  | —          |
| console             | 2026-10-13 | 264 / 0 / 0               | 190.015 | —          |
| console-ui          | 2026-10-13 | 183 / 0 / 0               | 17.968  | —          |
| dataset             | 2026-10-13 | 37 / 0 / 0                | 7.199   | —          |
| deployment          | 2026-10-13 | 75 / 0 / 0                | 45.902  | —          |
| diskdb              | 2026-10-13 | 183 / 0 / 0               | 40.249  | —          |
| diskio              | 2026-10-13 | 58 / 0 / 0                | 20.891  | —          |
| iceberg             | 2026-10-13 | 716 / 0 / 0               | 359.299 | —          |
| kv                  | 2026-10-13 | 738 / 0 / 0               | 144.680 | —          |
| protocol            | 2026-10-13 | 221 / 0 / 0               | 23.567  | —          |
| rpc                 | 2026-10-13 | 81 / 0 / 0                | 4.793   | —          |
| s3                  | 2026-10-13 | 114 / 0 / 0               | 13.606  | —          |
| test-infrastructure | 2026-10-13 | 134 / 0 / 0               | 5.052   | —          |
| tree                | 2026-10-13 | 598 / 0 / 0               | 70.605  | —          |

- The C++ gate retains 765 cases and removes 205 repeated Common/RPC executions
  inherited through nested CTest projects. S3 benchmark workload accounting now
  uses a deterministic protocol peer; its 200-operation and result assertions
  remain, without a host production-server launcher.
- Test infrastructure includes Rust, fixture/dispatch and OCI checks.
  CI ownership covers 28 packages and 379 named flows.
- Measurements: `.crowdb-runtime/artifacts/unit/` and
  `.crowdb-runtime/artifacts/measure-tests/`. The earlier Access whole-task
  baseline was 481.483 s; the unified task now passes 1051 cases in 378.41 s.

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
