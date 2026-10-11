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
Latest S3 run uses one case slot at its CI quota, 0.2 CPU/server (4 CPU /
8192 MiB budget); the previous run used 0.5 CPU/server and failed once.
Its three joint faults and protected restart add four cases.
Iceberg uses two case slots at 0.5 CPU/server (16 CPU / 32768 MiB budget);
its full catalog retirement grace accounts for 1205 seconds of client time.
KV includes the KV/KV and KV/Web isolation pairs. Console measures
`console.operations` (99 accepted owners; latest complete measurement covers the
preceding 96-owner inventory); unaccepted managed browser drafts
are excluded. Console uses one effective case slot at 0.25 CPU/server with a
4 CPU / 8192 MiB budget; the previous run used the same quota with fewer cases.
Deployment now uses the same CI quota and one case slot; its previous run used
0.5 CPU/server with two slots. The changed quota explains the wall-time increase.
The three newly migrated shell browser flows pass separately; they are excluded
from the complete Console measurement above.

| Domain       | Environment | Date       | Passed / failed / skipped | Wall s   | Previous s | Report                                                                                                    |
| ------------ | ----------- | ---------- | ------------------------- | -------- | ---------- | --------------------------------------------------------------------------------------------------------- |
| KV           | intel7960   | 2026-10-12 | 50 / 0 / 0                | 96.977   | 105.519    | [report](../../.crowdb-runtime/artifacts/container-e2e/run-7587d4ce83f142c5b6238a8b941d47d8/results.json) |
| Diskdb       | intel7960   | 2026-10-12 | 5 / 0 / 0                 | 15.142   | 14.048     | [report](../../.crowdb-runtime/artifacts/container-e2e/run-b4e60f42ad89430184f7cf03467f2d8f/results.json) |
| Diskio       | intel7960   | 2026-10-12 | 8 / 0 / 0                 | 21.935   | 21.507     | [report](../../.crowdb-runtime/artifacts/container-e2e/run-c974a86a524b43d48d33bfdc9070b100/results.json) |
| Chunkdb      | intel7960   | 2026-10-12 | 11 / 0 / 0                | 148.686  | 159.207    | [report](../../.crowdb-runtime/artifacts/container-e2e/run-0c0b062166874c109f3935021cdba787/results.json) |
| Chunk client | intel7960   | 2026-10-12 | 40 / 0 / 0                | 642.098  | 628.765    | [report](../../.crowdb-runtime/artifacts/container-e2e/run-3cb038d49e0b45399f6b71edaeb724c0/results.json) |
| Chunk stream | intel7960   | 2026-10-12 | 4 / 0 / 0                 | 69.960   | 75.707     | [report](../../.crowdb-runtime/artifacts/container-e2e/run-2d2d84b928b34e99a843d31f33589230/results.json) |
| Chunk-KV     | intel7960   | 2026-10-12 | 2 / 0 / 0                 | 37.811   | 41.265     | [report](../../.crowdb-runtime/artifacts/container-e2e/run-28da907f564941dba981824882c880e3/results.json) |
| S3           | intel7960   | 2026-10-12 | 43 / 0 / 0                | 1545.602 | 1456.774   | [report](../../.crowdb-runtime/artifacts/container-e2e/run-6e60f23a5a18442ebda5d08fe5ec1842/results.json) |
| Iceberg      | intel7960   | 2026-10-12 | 32 / 0 / 0                | 1616.697 | 1633.442   | [report](../../.crowdb-runtime/artifacts/container-e2e/run-da3c624fb35d493e9101d5b3cca7a273/results.json) |
| Dataset      | intel7960   | 2026-10-12 | 2 / 0 / 0                 | 35.568   | 31.301     | [report](../../.crowdb-runtime/artifacts/container-e2e/run-9bae8737664e45198e61bae2c48d4780/results.json) |
| Console      | intel7960   | 2026-10-12 | 96 / 0 / 0                | 691.960  | 644.632    | [report](../../.crowdb-runtime/artifacts/container-e2e/run-ab39e156c2c24da39a5db1bfbcba1a7c/results.json) |
| Deployment   | intel7960   | 2026-10-12 | 33 / 0 / 0                | 601.028  | 337.716    | [report](../../.crowdb-runtime/artifacts/container-e2e/run-5cfba02195364ce483aac4b5a3eaf1ee/results.json) |

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
  Its KV and Iceberg domains pass 50/50 and 32/32. Exact S3 regression passes all three phases
  (32.588 / 45.012 / 57.879 seconds); all six RCK cases execute and pass.
  All registered domains and special profiles completed on the coalescer image;
  its complete S3 failed once. The streamed bulk-delete fix is built/imported from
  `b7b1247b172f43562c9ffccfd95afef109bc3dde`, OCI
  `sha256:703141e93338f2d0969c32a4a2d4512a0964cf999b8f67e8bd72de352970d0c1`.
  Console passes 89/89 and full S3 passes 43/43 on this image. The thousand-key
  S3 test keeps its original deadlines and passes all phases at CI quota
  (34.710 / 50.016 / 66.055 seconds). Focused Iceberg RCK passes all six
  upstream cases in 62.324 seconds at 0.25 CPU/server, 4 CPU / 12288 MiB.
  Current formal image includes replica voting/epoch fixes, source
  `cb159e7b64606f50dff8ee8ff28f0236331f890b`, OCI
  `sha256:a54e48cdd00b58aff064679ee2a6cf9c42d2890312cc61e2cc868739a554275a`.
  Complete Console 96/96 and Deployment 33/33 pass at their CI quotas; other
  domain rows retain the older image identities in their reports.
  Optimized binaries retain debug info; mounted data/log/crash paths and actual
  host core collector are recorded per run. Receipt: `target/crowdb.oci.tar.json`.
- **Bridge SSH admission**: the explicit five-node peak profile passes in
  148.191 seconds on the coalescer image, 0.5 CPU/node with a 4 CPU /
  6144 MiB budget; [report](../../.crowdb-runtime/artifacts/container-e2e/run-91a781fe0ee84fec87330d03220c01d5/results.json).
  Four host browser flows pass with zero skips. Partial-startup and active-browser
  interruption clean up owned resources/clients in 1.128 / 1.385 seconds.
  This profile is separate from the ordinary Deployment domain measurement.
- **Manual Iceberg ecosystem**: all five pinned engines plus cross-client
  snapshot handoff/recreation pass in 196.874 seconds on the coalescer image,
  4 CPU / 10240 MiB; [report](../../.crowdb-runtime/artifacts/container-e2e/run-a1b52d865abb482ab9809868b226bced/results.json).
  Focused Python/DuckDB passes in 29.099 seconds, 4 CPU / 6144 MiB
  (`run-26ef383037904b3f812af8415b6bef83`, reports under `/tmp/crowdb-coalesce-final-ecosystem-python`).
  Partial-startup and active-Trino interruption clean up owned resources and
  host clients in 0.833 / 2.338 seconds. These manual profiles are separate from
  ordinary Iceberg domain counts. Private mounted data/raw logs are retained;
  published artifacts contain credential-redacted reports only.
- **Component ownership**: after removing unused native process harnesses,
  affected ChunkDB/chunk-client/chunk-stream/DiskDB-client component tests pass
  328/328, shared directory/ownership tests pass 4/4, and all eight changed
  consumer crates pass all-target clippy. Existing component flows are retained. The complete core gate passes 978/978
  after adding the coalescer race regression.
- **Release startup**: 14 rejection cases pass in 7.879 seconds on the rebuilt
  image, including missing/short/duplicate Iceberg writer credentials, rejected
  legacy Web configuration and forbidden Docker launch registry. Native duplicate
  owners are removed; token-validation and managed-router component UTs remain.
  Evidence: `/tmp/crowdb-startup-owner-migration/release-startup/timing.json`.
- **Linux host profile**: explicit host-network admission/recovery passes 1/1
  in 7.310 seconds on intel7960, 2 CPU / 1024 MiB node quota
  plus the host client reservation; [report](../../.crowdb-runtime/artifacts/container-e2e/run-b2fc758df17b4a3a9bc588c983cd5ec0/results.json).
  This special profile is separate from the bridge Deployment measurement.
- **Containerd Linux hosts**: explicit profiles reuse the same OCI digest.
  Manual/single host passes in 125.432 seconds ([report](../../.crowdb-runtime/artifacts/container-e2e/run-d6948f2eab7c4d3fbecd3ad6fb4bb2c1/results.json));
  three-host quorum/recovery passes in 129.661 seconds at 0.75 CPU/node,
  4 CPU / 6144 MiB budget ([report](../../.crowdb-runtime/artifacts/container-e2e/run-834a3c13f29d49e9b59bac3c9f3b6780/results.json)).
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
