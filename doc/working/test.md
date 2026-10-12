<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Test tracking

Track remaining issues and environment measurements. One row per domain;
record a complete run and keep its previous wall time. Partial migration gates
remain in the [execution plan](plan-container-e2e.md). Detailed timings and
redacted diagnostics stay under `.crowdb-runtime/artifacts/`.

## intel7960

Linux Docker bridge; clients run on the host and use inspected container IPs
and fixed ports. Wall times include startup, clients and teardown, excluding
shared builds. Record complete domain runs, including failures. Current Console
and Deployment use two case slots; complete native nodes retain 1 CPU /
2048 MiB / 512 PIDs. Exact image and resource settings stay in run artifacts.

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
| Iceberg      | 2026-10-12 | 32 / 0 / 0                | 1616.697 | 1633.442   |
| Dataset      | 2026-10-13 | 2 / 0 / 0                 | 23.609   | 35.568     |
| Console      | 2026-10-13 | 153 / 0 / 0               | 2223.972 | 2075.462   |
| Deployment   | 2026-10-13 | 33 / 0 / 0                | 310.503  | 314.358    |

- All rows except Iceberg use the final `9c0f8805` debug release. Iceberg is
  still running; its row remains the earlier baseline until the run finishes.
- Special profiles have separate measurements: bridge SSH 148.191 s, Linux
  host 7.310 s, containerd host/cluster 125.432 / 129.661 s, five-engine Iceberg
  196.874 s, Python/DuckDB 29.099 s. Their final reruns remain pending.
- Important component UTs remain. Latest focused gates: DiskDB 177, monitor 75,
  CLI 6, fixture 106, UI 183, KV-server 64; these are component repair checks,
  not another complete-domain baseline.

## amd5950

No recorded measurements.

## macm5pro

No container measurements. Linux VM routing remains unverified.

## Remaining issues

- [ ] **Container acceptance**: Console and Deployment pass on the final
  image. Finish remaining domain/profile runs and CI admission/export checks.
  Keep resources and deadlines unchanged until measured failures require a
  decision.
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
