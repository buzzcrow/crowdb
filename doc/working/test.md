<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Test tracking

Track remaining issues and environment measurements. One row per domain;
record a complete run and keep its previous wall time. Partial migration gates
remain in the [execution plan](plan-container-e2e.md). Detailed timings and
redacted diagnostics stay under `.crowdb-runtime/artifacts/`.

## intel7960

Linux Docker bridge; host clients use inspected IPs and fixed ports. Measurements
include startup, clients and teardown, excluding shared image/client builds.
Concurrency cap is 2. S3 uses 0.2 CPU/server with 4 CPU / 8192 MiB and one case
slot. Console/Deployment use 0.25 CPU/server with the same total budget and one
slot; the previous Deployment run used 0.5 CPU/server and two slots. Iceberg
uses two slots with 16 CPU / 32768 MiB. KV includes both isolation pairs.

### Container domains

Console's latest complete measurement covers 96 owners; the accepted inventory
is now 140. Twenty-five newer browser cases, two CLI recovery cases and seventeen
native client cases passed separately. These batch
results do not replace the complete-domain measurement.

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
| Console      | 2026-10-12 | 96 / 0 / 0                | 691.960  | 644.632    |
| Deployment   | 2026-10-12 | 33 / 0 / 0                | 601.028  | 337.716    |

- KV/storage/Dataset/Iceberg rows use the coalescer image (`fca2b4fa`);
  S3 uses the streaming fix (`b7b1247b`); Console/Deployment use the voting/epoch
  fix (`cb159e7b`). Exact build identities remain in their artifacts.
- Current image (`71f61c49`) also includes validated stream geometry, distinct
  explicit restart execution and actual process observations from owning nodes.
  OCI `sha256:5e2ba122c8e58047bc1c7c5610d1fba339452f80fa4817b4beee442b97744f15`.
  Optimized binaries retain debug info. Data/log/crash roots and host core policy
  are recorded per run; receipt: `target/crowdb.oci.tar.json`.
- Special profiles: bridge SSH admission 148.191s; Linux host 7.310s;
  containerd host/cluster 125.432s / 129.661s; five-engine Iceberg ecosystem
  196.874s; Python/DuckDB 29.099s. These are separate from domain counts.

### Historical tasks before container classification

| Test package       | Date       | Tests     | Seconds | Status                   |
| ------------------ | ---------- | --------- | ------- | ------------------------ |
| `test-cpp`         | 2026-10-09 | 942/942   | 131.26  | ✓                        |
| `test-core`        | 2026-10-09 | 994/994   | ~140.23 | ✓                        |
| `test-storage`     | 2026-10-09 | 841/841   | 916.53  | ✓                        |
| `test-access`      | 2026-10-09 | 1036/1037 | 412.60  | Pending: coverage gap    |
| `test-console`     | 2026-10-09 | 350/350   | ~1588.1 | ✓                        |
| `test-console-ui`  | 2026-10-09 | 229/229   | 307.40  | ✓                        |
| `test-boto3-e2e`   | 2026-10-09 | 255/255   | 428.89  | ✓                        |
| `test-iceberg-e2e` | 2026-10-09 | 29/32     | 789.37  | Pending: coverage gap    |
| `test-iceberg-sdk` | 2026-10-09 | 11/11     | 571.23  | ✓                        |

## amd5950

No recorded measurements.

## macm5pro

No container measurements. Linux VM routing remains unverified.

### Historical macOS tasks — machine model unrecorded

These results are previous macOS baselines, not measured macm5pro acceptance.

| Test package       | Date       | Tests   | Seconds | Status                                  |
| ------------------ | ---------- | ------- | ------- | --------------------------------------- |
| `test-cpp`         | 2026-10-07 | 860/860 | 55      | ✓                                       |
| `test-core`        | 2026-10-07 | 977/978 | 368     | ✓ 1 ignored                             |
| `test-storage`     | 2026-10-07 | 811/815 | 1064    | ✓ 4 ignored                             |
| `test-access`      | 2026-10-07 | 906/906 | —       | ✓                                       |
| `test-console`     | 2026-10-07 | 77/79   | 61      | ✓ 2 ignored                             |
| `test-console-ui`  | 2026-10-07 | 63/63   | 192     | ✓                                       |
| `test-boto3-e2e`   | 2026-10-07 | 32/32   | 295     | ✓                                       |
| `test-iceberg-e2e` | 2026-10-07 | —       | 338     | ✓                                       |
| `test-iceberg-sdk` | 2026-10-07 | —       | —       | X Maven dependency resolution stalled   |

## Remaining issues

- [ ] **Migration closure**: remaining real-server Console lifecycle/cluster
  flows need canonical container owners. Preserve old assertions and important component UTs until
  equivalent owners pass. Final Pixi/CI taxonomy and complete-domain measurements remain pending.
  KV/KV and KV/Web isolation acceptance passes.
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
