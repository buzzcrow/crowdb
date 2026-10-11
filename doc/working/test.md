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
| KV           | intel7960   | 2026-10-11 | 50 / 0 / 0                | 94.794   | 167.938    | [report](../../.crowdb-runtime/artifacts/container-e2e/run-b1dedf87060b4ca083095864b0996bd0/results.json) |
| Diskdb       | intel7960   | 2026-10-11 | 5 / 0 / 0                 | 14.198   | —          | [report](../../.crowdb-runtime/artifacts/container-e2e/run-cfc58fa353574c6898cfe40463c7a814/results.json) |
| Diskio       | intel7960   | 2026-10-11 | 8 / 0 / 0                 | 20.802   | 21.449     | [report](../../.crowdb-runtime/artifacts/container-e2e/run-7ca260ddd87c489e949438bc9d7b9d59/results.json) |
| Chunkdb      | intel7960   | 2026-10-11 | 11 / 0 / 0                | 70.267   | —          | [report](../../.crowdb-runtime/artifacts/container-e2e/run-370769d47f72492d8cfaef8ae77c61ea/results.json) |
| Chunk client | intel7960   | 2026-10-11 | 40 / 0 / 0                | 456.101  | —          | [report](../../.crowdb-runtime/artifacts/container-e2e/run-0bc1b0c5c6184c14a4248bccb13391d2/results.json) |
| Chunk stream | intel7960   | 2026-10-11 | 4 / 0 / 0                 | 36.149   | 52.184     | [report](../../.crowdb-runtime/artifacts/container-e2e/run-8c9d46d75aa04f7c8b409cd8cb285aa5/results.json) |
| Chunk-KV     | intel7960   | 2026-10-11 | 2 / 0 / 0                 | 22.520   | —          | [report](../../.crowdb-runtime/artifacts/container-e2e/run-e8fbdfdc8be94d408309d5623227794d/results.json) |
| S3           | intel7960   | 2026-10-11 | 43 / 0 / 0                | 1135.695 | 418.700    | [report](../../.crowdb-runtime/artifacts/container-e2e/run-04461b8645414e83b7aa98cd5aa63ae2/results.json) |
| Iceberg      | intel7960   | 2026-10-11 | 32 / 0 / 0                | 1515.042 | 1542.472   | [report](../../.crowdb-runtime/artifacts/container-e2e/run-9dd41619b7a04bd3b7e0693e3b432b74/results.json) |
| Dataset      | intel7960   | 2026-10-11 | 2 / 0 / 0                 | 18.024   | —          | [report](../../.crowdb-runtime/artifacts/container-e2e/run-d869ca04e48d46dabadb36e4db510c81/results.json) |
| Console      | intel7960   | 2026-10-11 | 87 / 0 / 0                | 627.744  | 605.125    | [report](../../.crowdb-runtime/artifacts/container-e2e/run-29a68eb2454c4fe5baf55c3d6cd8b610/results.json) |
| Deployment   | intel7960   | 2026-10-11 | 33 / 0 / 0                | 294.359  | —          | [report](../../.crowdb-runtime/artifacts/container-e2e/run-5f0be57000ed4481968a78dad965273d/results.json) |

## Environments

- **intel7960**: Linux Docker bridge containers; host clients access inspected
  private IPs and fixed service ports. Prior complete domain measurements use release source
  `ec0703725994ae746c7c7cc2d42a8b889a72402d`, OCI digest
  `sha256:40b41c9f808a262d78236e5b1acbe685ca7de37e5b695089ea3e0f9c029e5979`.
  Earlier domain rows retain their own image identity in the linked reports.
  The rebuilt image from `d9dd29c41e94ad48a58940fc01b7ad700bdd0cf3`
  (OCI `sha256:bb3c6951ef7f3203ba49354678032bf3016c3db9004412875630a1b0783b57b0`)
  fixes non-voting heartbeat catch-up; final full-domain remeasurement is pending.
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
