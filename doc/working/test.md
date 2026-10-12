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
| KV           | 2026-10-12 | 50 / 0 / 0                | 96.977   | 105.519    |
| Diskdb       | 2026-10-12 | 5 / 0 / 0                 | 15.142   | 14.048     |
| Diskio       | 2026-10-12 | 8 / 0 / 0                 | 21.935   | 21.507     |
| Chunkdb      | 2026-10-12 | 11 / 0 / 0                | 148.686  | 159.207    |
| Chunk client | 2026-10-12 | 40 / 0 / 0                | 642.098  | 628.765    |
| Chunk stream | 2026-10-12 | 4 / 0 / 0                 | 69.960   | 75.707     |
| Chunk-KV     | 2026-10-12 | 2 / 0 / 0                 | 37.811   | 41.265     |
| S3           | 2026-10-12 | 43 / 0 / 0                | 1545.602 | 1456.774   |
| Iceberg      | 2026-10-12 | 32 / 0 / 0                | 1616.697 | 1633.442   |
| Dataset      | 2026-10-12 | 2 / 0 / 0                 | 35.568   | 31.301     |
| Console      | 2026-10-13 | 149 / 4 / 0               | 2075.462 | 2160.528   |
| Deployment   | 2026-10-13 | 33 / 0 / 0                | 314.358  | 310.999    |

- Console and Deployment above use the `de0fe0eb` debug release.
  The other domain rows are earlier baselines; final release gates are pending.
- Special profiles have separate measurements: bridge SSH 148.191 s, Linux
  host 7.310 s, containerd host/cluster 125.432 / 129.661 s, five-engine Iceberg
  196.874 s, Python/DuckDB 29.099 s. Their final reruns remain pending.
- Important component UTs remain. Latest focused gates: DiskDB 177, monitor 75,
  CLI 6, fixture 106, UI 183, KV-server 63; these are component repair checks,
  not another complete-domain baseline.

## amd5950

No recorded measurements.

## macm5pro

No container measurements. Linux VM routing remains unverified.

## Remaining issues

- [ ] **Container acceptance**: complete final-image domain gates and final
  Pixi/CI admission/export checks. Latest failures are Access starting during unavailable DiskIO ownership,
  native load batch deadline, and CLI cadence being sent under an ignored API
  field. Canvas/multipart/publication recovery pass. Corrected CLI configuration
  is now checked in actual running intents; remaining container gates are pending.
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
