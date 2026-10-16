<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Test tracking

Track remaining issues and environment measurements. One row per domain;
record a complete run and keep its previous wall time. Detailed timings and
redacted diagnostics stay in the recorded artifact roots.

## intel7960

Linux Docker bridge; clients run on the host and use inspected container IPs
and fixed ports. Wall times include startup, clients and teardown, excluding
shared builds. Record complete domain runs, including failures. Requested
concurrency and actual case capacity are recorded separately. New runs use 1024
CPU shares per container without a CPU quota; complete native nodes retain
2048 MiB / 512 PIDs. Exact image and resource settings stay in run artifacts.

### Container domains

| Domain       | Date       | Passed / failed / skipped | Wall s   | Previous s |
| ------------ | ---------- | ------------------------- | -------- | ---------- |
| KV           | 2026-10-16 | 50 / 0 / 0                | 167.177  | 126.685    |
| Diskdb       | 2026-10-16 | 5 / 0 / 0                 | 12.531   | 12.679     |
| Diskio       | 2026-10-16 | 8 / 0 / 0                 | 20.851   | 27.230     |
| Chunkdb      | 2026-10-16 | 11 / 0 / 0                | 145.752  | 87.330     |
| Chunk client | 2026-10-16 | 40 / 0 / 0                | 538.994  | 349.746    |
| Chunk stream | 2026-10-16 | 4 / 0 / 0                 | 60.849   | 42.558     |
| Chunk-KV     | 2026-10-16 | 2 / 0 / 0                 | 35.266   | 30.160     |
| S3           | 2026-10-16 | 43 / 0 / 0                | 1009.182 | 877.167    |
| Iceberg      | 2026-10-16 | 32 / 0 / 0                | 1984.972 | 2187.829   |
| Dataset      | 2026-10-16 | 5 / 0 / 0                 | 121.596  | 23.609     |
| Console      | 2026-10-16 | 159 / 0 / 0               | 3211.651 | 2223.972   |
| Deployment   | 2026-10-16 | 33 / 0 / 0                | 307.108  | 310.503    |

- The 2026-10-16 full run completed on the working tree based on `664f04a1`:
  all twelve domains passed, with 392 scenario executions and no failures or
  skips. KV includes four additional isolation/recovery probes beyond the
  388 named flows. The first ten domains used optimized, stripped image
  `5ede9f3a4f76` and source tree `03c0eb9e7f40`; Console and Deployment used
  image `7a176ef2218b` and source tree `8bba22a39c62` after the UI repair.
  Container wall time uses `timings.scenarios_wall`, excluding shared client
  and image builds. Each task requests two case slots and a 4-CPU admission
  budget; per-domain memory budgets and actual slot capacity stay in artifacts.
- Previous rows used the `9c0f8805` debug release. Iceberg uses one case slot
  and includes the approximately 20-minute retirement grace in both runs. Build
  profile, cache state and concurrency differences prevent direct performance
  comparison. Historical diagnostics: `/tmp/crowdb-r230-shares-public`.
- Historical special profiles passed on that earlier image: bridge SSH
  148.600 s, Linux host 7.759 s, containerd host/cluster 111.815 / 112.784 s,
  five-engine Iceberg 127.531 s and Python/DuckDB 20.458 s. These profiles
  are separate from the standard twelve-domain full suite.
- Current container results and logs:
  `.crowdb-runtime/artifacts/measure-tests/20261016T031530.418485Z/` and
  `.crowdb-runtime/artifacts/measure-tests/20261016T054410.910103Z/`.
  Consolidated verification and source/resource identities:
  `.crowdb-runtime/artifacts/measure-tests/20261016T031530.418485Z/full-test-summary.json`.
- The first full attempt exposed a lost RPC write-interest rearm during
  ChunkDB conversion recovery. Its Chunk Client result was 39 passed / 1 failed.
  After fixing the event ordering, the complete domain passes 40 / 0 with the
  original timeout. Initial diagnostics remain under
  `.crowdb-runtime/artifacts/measure-tests/20261016T014707.773546Z/`.
- The next full Console attempt completed 159 flows with 152 passed / 7 failed.
  It exposed stale storage/menu contracts, an authority recovery ordering gap
  in a browser test and a scroll restoration race. After repair, the complete
  Console and Deployment reruns passed 159 / 0 and 33 / 0 respectively
  under `.crowdb-runtime/artifacts/measure-tests/20261016T054410.910103Z/`,
  using image `7a176ef2218b` and source tree `8bba22a39c62`. The runtime source
  difference from the earlier image is confined to UI scroll restoration.

### Component tests

One row per owning domain, combining its Rust/C++ packages and SDK protocol
peers. The 2026-10-16 component run passes all 4,658 cases, including an explicit run
of the installed-wheel Dataset Split HTTP test skipped by the standard Access
task. All twelve container domains also pass.

Wall times include compilation inside each component command; the shared C++
prerequisite and OCI builds are excluded. The shared image was rebuilt after the RPC fix,
and component tasks ran sequentially with existing compilation caches. Cache
state and dependency recompilation affect comparisons.

| Domain              | Date       | Passed / failed / skipped | Wall s  | Previous s |
| ------------------- | ---------- | ------------------------- | ------- | ---------- |
| access              | 2026-10-16 | 154 / 0 / 0               | 209.090 | 125.050    |
| chunk-client        | 2026-10-16 | 125 / 0 / 0               | 13.000  | 41.324     |
| chunk-kv            | 2026-10-16 | 153 / 0 / 0               | 46.565  | 213.867    |
| chunk-stream        | 2026-10-16 | 50 / 0 / 0                | 12.421  | 26.958     |
| chunkdb             | 2026-10-16 | 160 / 0 / 0               | 137.869 | 113.006    |
| common              | 2026-10-16 | 112 / 0 / 0               | 10.097  | 12.033     |
| console             | 2026-10-16 | 293 / 0 / 0               | 177.636 | 190.015    |
| console-ui          | 2026-10-16 | 275 / 0 / 0               | 18.570  | 17.968     |
| dataset             | 2026-10-16 | 175 / 0 / 0               | 83.589  | 7.199      |
| deployment          | 2026-10-16 | 130 / 0 / 0               | 89.446  | 45.902     |
| diskdb              | 2026-10-16 | 201 / 0 / 0               | 37.024  | 40.249     |
| diskio              | 2026-10-16 | 70 / 0 / 0                | 9.451   | 20.891     |
| iceberg             | 2026-10-16 | 741 / 0 / 0               | 352.876 | 359.299    |
| kv                  | 2026-10-16 | 790 / 0 / 0               | 92.181  | 144.680    |
| protocol            | 2026-10-16 | 240 / 0 / 0               | 9.585   | 23.567     |
| rpc                 | 2026-10-16 | 83 / 0 / 0                | 77.733  | 4.793      |
| s3                  | 2026-10-16 | 114 / 0 / 0               | 9.400   | 13.606     |
| test-infrastructure | 2026-10-16 | 191 / 0 / 0               | 7.779   | 5.041      |
| tree                | 2026-10-16 | 601 / 0 / 0               | 69.870  | 70.605     |

- C++/FFI passes 779 cases. Common/RPC cases inherited through nested CTest
  projects are counted only under their owning project.
- Dataset includes the standalone SDK workspace and ten installed Python wheel
  consumer tests. The SDK source-distribution build reruns the same twenty Rust
  tests; those repeated executions are not counted twice.
- Test infrastructure includes four Rust cases, 155 fixture/dispatch cases and
  32 OCI checks. CI ownership covers 28 packages and 388 named container flows.
- UI passes its type checks. Workspace Rust fmt/Clippy, standalone SDK boundary,
  fmt/Clippy and CI test-task coverage checks pass.
- Component command logs: `.crowdb-runtime/artifacts/unit/`. Aggregated results:
  `.crowdb-runtime/artifacts/measure-tests/20261016T031530.418485Z/component-summary.json`.

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
