<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Test tracking

Persistent record of remaining test issues and environment measurements.
Migration details belong in the [execution plan](plan-container-e2e.md);
commands and fixture requirements belong in the [E2E README](../../e2e-test/README.md).

## Tracking rules

- Track one row per product domain, not per case, SDK, fault point or test binary.
- Record date, executed/passed/failed/skipped counts and total wall time from a
  complete domain run. Keep the previous measurement to compare regressions.
- Record environment and concurrency alongside the result. Parallel case times
  cannot be added to obtain elapsed time. Record shared image build separately.
- Detailed timings, slow cases and failure evidence remain in run artifacts
  under `.crowdb-runtime/artifacts/`. Link the report when updating a row.
- Partial migration gates are not complete domain measurements. Use `—` when
  unmeasured; do not infer totals from separately executed migration batches.

## Container domain measurements

Rows measure current complete registered domain tasks; remaining migration
coverage is tracked below. Intel7960 runs use concurrency 2; wall time includes
startup, clients and teardown, excluding the shared image and host client build.
Latest S3 run uses one case slot at 0.5 CPU/server (16 CPU / 16384 MiB
budget); its three joint faults and protected restart add four cases.
Iceberg uses two case slots at 0.5 CPU/server (24 CPU / 32768 MiB budget);
its full catalog retirement grace accounts for 1205 seconds of client time.
KV includes the KV/KV and KV/Web isolation pairs. Console measures
`console.operations` (87 accepted owners); unaccepted managed browser drafts
are excluded. Console uses one effective case slot at 0.25 CPU/server with a
4 CPU / 8192 MiB budget; the previous run used higher capacity.

| Domain       | Environment | Date       | Passed / failed / skipped | Wall s   | Previous s | Report                                                                                                    |
| ------------ | ----------- | ---------- | ------------------------- | -------- | ---------- | --------------------------------------------------------------------------------------------------------- |
| KV           | intel7960   | 2026-10-12 | 50 / 0 / 0                | 96.977   | 105.519    | [report](../../.crowdb-runtime/artifacts/container-e2e/run-7587d4ce83f142c5b6238a8b941d47d8/results.json) |
| Diskdb       | intel7960   | 2026-10-12 | 5 / 0 / 0                 | 14.048   | 14.198     | [report](../../.crowdb-runtime/artifacts/container-e2e/run-9a6b10db224442a99877a821df96ddf7/results.json) |
| Diskio       | intel7960   | 2026-10-12 | 8 / 0 / 0                 | 21.507   | 20.802     | [report](../../.crowdb-runtime/artifacts/container-e2e/run-30a61c56a8ca495fa636b0b0b846271e/results.json) |
| Chunkdb      | intel7960   | 2026-10-12 | 11 / 0 / 0                | 159.207  | 70.267     | [report](../../.crowdb-runtime/artifacts/container-e2e/run-479c24973b70443c9c7e35d4bee8ee2c/results.json) |
| Chunk client | intel7960   | 2026-10-12 | 40 / 0 / 0                | 628.765  | 456.101    | [report](../../.crowdb-runtime/artifacts/container-e2e/run-957c7ecf692c4e3b9c76c697e2643e11/results.json) |
| Chunk stream | intel7960   | 2026-10-12 | 4 / 0 / 0                 | 75.707   | 36.149     | [report](../../.crowdb-runtime/artifacts/container-e2e/run-fbaa0d20cd084f34bf45bf8dd73ffeb0/results.json) |
| Chunk-KV     | intel7960   | 2026-10-12 | 2 / 0 / 0                 | 41.265   | 22.520     | [report](../../.crowdb-runtime/artifacts/container-e2e/run-cbb0e11572254c5789262ebc13b8ae69/results.json) |
| S3           | intel7960   | 2026-10-12 | 42 / 1 / 0                | 1470.416 | 1135.695   | [report](../../.crowdb-runtime/artifacts/container-e2e/run-52be5dd3b8cb4e7abff03daa44b0509e/results.json) |
| Iceberg      | intel7960   | 2026-10-12 | 31 / 1 / 0                | 1633.442 | 1515.042   | [report](../../.crowdb-runtime/artifacts/container-e2e/run-a668558dcb8541919c78c4f6e1b4f13d/results.json) |
| Dataset      | intel7960   | 2026-10-12 | 2 / 0 / 0                 | 31.301   | 18.024     | [report](../../.crowdb-runtime/artifacts/container-e2e/run-438521b1303d4b828e483e1b86c6b559/results.json) |
| Console      | intel7960   | 2026-10-12 | 87 / 0 / 0                | 676.846  | 627.744    | [report](../../.crowdb-runtime/artifacts/container-e2e/run-6e890de5f4ab479686588a4d23719d23/results.json) |
| Deployment   | intel7960   | 2026-10-12 | 33 / 0 / 0                | 337.197  | 294.359    | [report](../../.crowdb-runtime/artifacts/container-e2e/run-fb8ec94000ef4856bfd294ddcf7c77de/results.json) |

## Environments

- **intel7960**: Linux Docker bridge containers; host clients access inspected
  private IPs and fixed service ports. Prior complete domain measurements use release source
  `ec0703725994ae746c7c7cc2d42a8b889a72402d`, OCI digest
  `sha256:40b41c9f808a262d78236e5b1acbe685ca7de37e5b695089ea3e0f9c029e5979`.
  Earlier domain rows retain their own image identity in the linked reports.
  The rebuilt image from `d9dd29c41e94ad48a58940fc01b7ad700bdd0cf3`
  (OCI `sha256:bb3c6951ef7f3203ba49354678032bf3016c3db9004412875630a1b0783b57b0`)
  fixes non-voting heartbeat catch-up. Full domain runs are recorded above;
  S3 and Iceberg failed once each. A new image incorporating the KV coalescer
  race fix was built/imported from `fca2b4fa9f7f51014ef7cff61d940429af393899`,
  OCI `sha256:c79e4be09c63ea8be0de36d06d3796ee3a5254ee3179e808238b3a256362f657`.
  Its KV domain passes 50/50. Exact S3 regression passes all three phases
  (32.588 / 45.012 / 57.879 seconds); all six RCK cases execute and pass.
  Complete S3/Iceberg and other domain remeasurement remains pending.
  Optimized binaries retain debug info; mounted data/log/crash paths and actual
  host core collector are recorded per run. Receipt: `target/crowdb.oci.tar.json`.
- **Bridge SSH admission**: the explicit five-node peak profile passes in
  123.686 seconds on the rebuilt image, 0.5 CPU/node with a 3.5 CPU /
  6144 MiB reservation; [report](../../.crowdb-runtime/artifacts/container-e2e/run-e05c9cea49344b21a6d94b724c1071c9/results.json).
  Four host browser flows pass with zero skips. Partial-startup and active-browser
  interruption clean up owned resources/clients in 1.128 / 1.385 seconds.
  This profile is separate from the ordinary Deployment domain measurement.
- **Manual Iceberg ecosystem**: all five pinned engines plus cross-client
  snapshot handoff/recreation pass in 192.879 seconds on the rebuilt image,
  4 CPU / 10240 MiB; [report](../../.crowdb-runtime/artifacts/container-e2e/run-a1ee514316574f269327bbbd858ec139/results.json).
  Focused Python/DuckDB passes in 19.425 seconds, 3 CPU / 6144 MiB
  (`run-c25d6063eab24c588d5d32cbde2a030d`, reports under `/tmp/crowdb-root-ecosystem-python`).
  Partial-startup and active-Trino interruption clean up owned resources and
  host clients in 0.833 / 2.338 seconds. These manual profiles are separate from
  ordinary Iceberg domain counts. Private mounted data/raw logs are retained;
  published artifacts contain credential-redacted reports only.
- **Component ownership**: after removing unused native process harnesses,
  affected ChunkDB/chunk-client/chunk-stream/DiskDB-client component tests pass
  328/328, shared directory/ownership tests pass 4/4, and all eight changed
  consumer crates pass all-target clippy. Existing component flows are retained.
- **Release startup**: 14 rejection cases pass in 7.879 seconds on the rebuilt
  image, including missing/short/duplicate Iceberg writer credentials, rejected
  legacy Web configuration and forbidden Docker launch registry. Native duplicate
  owners are removed; token-validation and managed-router component UTs remain.
  Evidence: `/tmp/crowdb-startup-owner-migration/release-startup/timing.json`.
- **Linux host profile**: explicit host-network admission/recovery passes 1/1
  in 6.740 seconds on intel7960, 2 CPU / 1024 MiB node quota
  plus the host client reservation; [report](../../.crowdb-runtime/artifacts/container-e2e/run-9869315124ca45cb9ba3e3b3b245be90/results.json).
  This special profile is separate from the bridge Deployment measurement.
- **Containerd Linux hosts**: explicit profiles reuse the same OCI digest.
  Manual/single host passes in 135.334 seconds ([report](../../.crowdb-runtime/artifacts/container-e2e/run-03a731dc50974a01b4a7603177c4ff3e/results.json));
  three-host quorum/recovery passes in 122.033 seconds at 0.75 CPU/node,
  4 CPU / 6144 MiB budget ([report](../../.crowdb-runtime/artifacts/container-e2e/run-101ddfb3c22d4a7587b78eccd9c49b9d/results.json)).
- **macOS**: historical native task measurements below. Container migration
  acceptance has not been measured; Linux VM routing is not assumed.
- **amd5950**: no recorded measurements.

Historical task measurements predate final container classification; keep them
as environment baselines, not as evidence of migration completion.

### intel7960

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

### macOS

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

- [ ] **Current regression failures**: S3 thousand-key batch deletion timed out
  at its unchanged 120-second deadline, including an isolated rerun. A reproduced
  KV coalescer enqueue-loss race is fixed and 122 group tests pass; packaged
  exact regression passes on the rebuilt release image. Iceberg RCK archive
  metadata is fixed and all six real cases pass. Complete S3/Iceberg runs on
  the rebuilt image remain pending; domain rows still report previous failures.

- [ ] **Migration closure**: remaining real-server Console lifecycle/cluster
  flows and CLI S3 bootstrap replay need canonical
  container owners. Preserve old assertions and important component UTs until
  equivalent owners pass. Final Pixi/CI taxonomy and complete-domain measurements remain pending.
  KV/KV and KV/Web isolation acceptance passes.
- [ ] **Console mode**: formal Docker Web rejects all non-read requests with
  403; the old browser flow expects logical writes and physical-mutation 503.
  Choose a supported writable Console profile or change Docker write policy
  before retiring that flow. Readonly release browser acceptance already passes.
- [ ] **macOS Iceberg SDK**: Maven `dependency:go-offline` stalled for more than
  eight minutes. Rerun with the pinned dependency cache available.
- [ ] **WAL disk loss**: full fail-out and snapshot rejoin coverage awaits the
  fail-out implementation (`design-crowdb-kv-wal.md`, section 8.1).
- [ ] **Per-group WAL disks**: startup currently derives disks from store-level
  config; isolation coverage awaits per-group `wal_disks` configuration.
- [ ] **Network partition**: add sever/restore acceptance with an owned partition
  mechanism. Existing crash/restart acceptance does not prove partition behavior.
- [ ] **Benchmark classification**: decide ownership of
  `tools/benchmark/bench-chunk-kv-regression.sh` after migration is complete;
  retain its current behavior meanwhile.
