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
| test-infrastructure | 2026-10-13 | 138 / 0 / 0               | 5.041   | —          |
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

AMD Ryzen 9 5950X, Linux Docker bridge; same component tasks and container
domains as Intel. Image revision `2529950f`; release binaries are optimized
and stripped, while the Intel container baseline used a debug release.
Timing comparisons therefore include a build-profile difference.

### Container domains

| Domain       | Date       | Passed / failed / skipped | Wall s   | Previous s |
| ------------ | ---------- | ------------------------- | -------- | ---------- |
| kv           | 2026-10-13 | 50 / 0 / 0                | 174.310 | —          |
| diskdb       | 2026-10-13 | 5 / 0 / 0                 | 9.290 | —          |
| diskio       | 2026-10-13 | 8 / 0 / 0                 | 15.500 | —          |
| chunkdb      | 2026-10-13 | 11 / 0 / 0                | 86.940 | —          |
| chunk-client | 2026-10-13 | 40 / 0 / 0                | 274.390 | —          |
| chunk-stream | 2026-10-13 | 4 / 0 / 0                 | 31.490 | —          |
| chunk-kv     | 2026-10-13 | 2 / 0 / 0                 |   19.140 | —          |
| iceberg      | 2026-10-13 | 31 / 1 / 0                | 1965.300 | —          |
| dataset      | 2026-10-13 | 2 / 0 / 0                 | 16.200 | —          |
| console      | 2026-10-13 | 148 / 5 / 0               | 1955.340 | —          |
| deployment   | 2026-10-13 | 33 / 0 / 0                | 328.790 | —          |
| s3           | 2026-10-13 | 43 / 0 / 0                | 1053.930 | —          |

- Wall times are the complete suite elapsed times, including startup, clients and teardown; shared builds are excluded.
- 1024 CPU shares per container, no CPU quota, one Tokio worker per service;
  4-CPU budget, two requested case slots; actual capacity is recorded per run.
- The first matrix stopped after Iceberg; Dataset and Deployment were rerun separately and passed. Console recorded five failures; the single-case retry reproduced the `io_uring_queue_init: Operation not permitted` / missing DiskIO owner failure under the host Docker seccomp profile.
  Iceberg failed one case (`apache_rest_compatibility_kit_supported_catalog_surface`);
  31 cases passed. The first failure was Gradle distribution download timeout; after the cache
  was populated, the unchanged case built successfully but exceeded its existing 960-second
  client budget during the cold RCK build. The complete traces are in the recorded logs.
- Stability fix: native Console fixtures now launch their node containers with
  `seccomp=unconfined`, allowing the required `io_uring_setup` path; the generic
  bridge fixture remains unchanged. The focused fixture regression tests pass; the
  The rebuilt-image six-node rerun reached service startup; the original missing-root race now
  publishes the initial partition, but a later retry still sees a transient `disk group 3 has no
  live DiskIO owner` while Chunk-KV connects. The remaining trace is in
  `.crowdb-runtime/artifacts/console-retry-six-bootstrap-fix/`; the DiskIO ownership race remains
  to be fixed before marking Console stable.
- Container artifacts: `.crowdb-runtime/artifacts/measure-tests/20261013T092119.686610Z/`, `.crowdb-runtime/artifacts/measure-tests/20261013T102929.186483Z/`, `.crowdb-runtime/artifacts/measure-tests/20261013T113259.109082Z/`, and `.crowdb-runtime/artifacts/console-retry-six/`.

### Component tests

| Domain              | Date       | Passed / failed / skipped | Wall s   | Previous s |
| ------------------- | ---------- | ------------------------- | -------- | ---------- |
| access              | 2026-10-13 | 117 / 0 / 0               |   95.677 | —          |
| chunk-client        | 2026-10-13 | 125 / 0 / 0               |   27.105 | —          |
| chunk-kv            | 2026-10-13 | 148 / 0 / 0               |  180.001 | —          |
| chunk-stream        | 2026-10-13 | 50 / 0 / 0                |   19.845 | —          |
| chunkdb             | 2026-10-13 | 158 / 0 / 0               |  146.525 | —          |
| common              | 2026-10-13 | 112 / 0 / 0               |   22.158 | —          |
| console             | 2026-10-13 | 264 / 0 / 0               |  196.454 | —          |
| console-ui          | 2026-10-13 | 183 / 0 / 0               |    9.391 | —          |
| dataset             | 2026-10-13 | 37 / 0 / 0                |   23.330 | —          |
| deployment          | 2026-10-13 | 75 / 0 / 0                |   55.542 | —          |
| diskdb              | 2026-10-13 | 183 / 0 / 0               |   39.004 | —          |
| diskio              | 2026-10-13 | 58 / 0 / 0                |   14.720 | —          |
| iceberg             | 2026-10-13 | 716 / 0 / 0               |  427.969 | —          |
| kv                  | 2026-10-13 | 738 / 0 / 0               |  250.138 | —          |
| protocol            | 2026-10-13 | 221 / 0 / 0               |   12.738 | —          |
| rpc                 | 2026-10-13 | 81 / 0 / 0                |   63.609 | —          |
| s3                  | 2026-10-13 | 114 / 0 / 0               |   31.354 | —          |
| test-infrastructure | 2026-10-13 | 4 / 0 / 0                 |    1.130 | —          |
| tree                | 2026-10-13 | 598 / 0 / 0               |  127.933 | —          |

- All seven component tasks pass; UI lint/type checks also pass.
  The explicitly rerun boto3 case is counted once. Shared C++ prerequisite
  build time is excluded; SDK dependency preparation is included.
- Test infrastructure currently records its 4 Rust cases; fixture/dispatch
  and OCI checks are pending to match the Intel row.
- Component artifacts: `.crowdb-runtime/artifacts/measure-tests/20261013T073206.432338Z/`
  and `.crowdb-runtime/artifacts/unit/`.
- Image construction succeeds after installing host `uidmap`; Docker import
  loads the image but reports a config-digest mismatch. The container runner
  validates source identity and uses the loaded image; retain this limitation.

## macm5pro

No container measurements. Linux VM routing remains unverified.

## Remaining issues

- [ ] **Remote CI acceptance**: CI #214 identifies a stale Docker socket after
  daemon shutdown/package removal. Cleanup now verifies connection refusal
  before removing that inode; active listeners and non-socket files fail safely.
  Require a complete successful remote run before merge.
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
