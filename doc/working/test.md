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
KV includes the KV/KV and KV/Web isolation pairs. Console measures
`console.operations` (78 accepted owners); unaccepted managed browser drafts
are excluded. Other domains remain
unmeasured as complete tasks.

| Domain       | Environment | Date       | Passed / failed / skipped | Wall s  | Previous s | Report                                                                                                    |
| ------------ | ----------- | ---------- | ------------------------- | ------- | ---------- | --------------------------------------------------------------------------------------------------------- |
| KV           | intel7960   | 2026-10-11 | 50 / 0 / 0                | 167.938 | —          | [report](../../.crowdb-runtime/artifacts/container-e2e/run-a96dd4c4985945a9912bcc72a51b6c80/results.json) |
| Diskdb       | intel7960   | 2026-10-11 | 5 / 0 / 0                 | 14.198  | —          | [report](../../.crowdb-runtime/artifacts/container-e2e/run-cfc58fa353574c6898cfe40463c7a814/results.json) |
| Diskio       | intel7960   | 2026-10-11 | 8 / 0 / 0                 | 21.449  | —          | [report](../../.crowdb-runtime/artifacts/container-e2e/run-517a0c2c29c04e0ca5ee8feb558e7394/results.json) |
| Chunkdb      | intel7960   | 2026-10-11 | 11 / 0 / 0                | 70.267  | —          | [report](../../.crowdb-runtime/artifacts/container-e2e/run-370769d47f72492d8cfaef8ae77c61ea/results.json) |
| Chunk client | intel7960   | —          | —                         | —       | —          | —                                                                                                         |
| Chunk stream | intel7960   | —          | —                         | —       | —          | —                                                                                                         |
| Chunk-KV     | intel7960   | 2026-10-11 | 2 / 0 / 0                 | 22.520  | —          | [report](../../.crowdb-runtime/artifacts/container-e2e/run-e8fbdfdc8be94d408309d5623227794d/results.json) |
| S3           | intel7960   | 2026-10-11 | 38 / 0 / 0                | 433.351 | —          | [report](../../.crowdb-runtime/artifacts/container-e2e/run-d6434ad353d34b5bb60a0751b4b400c2/results.json) |
| Iceberg      | intel7960   | —          | —                         | —       | —          | —                                                                                                         |
| Dataset      | intel7960   | 2026-10-11 | 2 / 0 / 0                 | 18.024  | —          | [report](../../.crowdb-runtime/artifacts/container-e2e/run-d869ca04e48d46dabadb36e4db510c81/results.json) |
| Console      | intel7960   | 2026-10-11 | 78 / 0 / 0                | 161.415 | —          | [report](../../.crowdb-runtime/artifacts/container-e2e/run-3d2b8ad6fc5349f7a2a961ecd4ec7bd3/results.json) |
| Deployment   | intel7960   | —          | —                         | —       | —          | —                                                                                                         |

## Environments

- **intel7960**: Linux Docker bridge containers; host clients access inspected
  private IPs and fixed service ports. Latest verified release source
  `7a47012129be3e3015c463db5f17726593c6e8df`, OCI digest
  `sha256:f2e9efbc72ba47411c1a0237b5bcbc6795ab6d0b5eae124453dc6e5c4dd881a9`.
  Optimized binaries retain debug info; mounted data/log/crash paths and actual
  host core collector are recorded per run. Receipt: `target/crowdb.oci.tar.json`.
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
  flows, S3 mini-cluster and Iceberg durable crash matrices need canonical
  container owners. Preserve old assertions and important component UTs until
  equivalent owners pass. Final Pixi/CI taxonomy, mixed-domain isolation and
  complete-domain measurements remain pending.
- [ ] **Storage outage budget**: original stream/reader and S3 repair proofs
  require failure within 15 seconds after Diskio exit. Vanished container IPs
  become network blackholes and exceed that bound. Choose an overall production
  operation budget or a distinct network-blackhole contract; keep original
  process-refusal assertions meanwhile. Evidence and alternatives are in the
  execution plan's `Blocked` section.
- [ ] **Console mode**: formal Docker Web rejects all non-read requests with
  403; the old browser flow expects logical writes and physical-mutation 503.
  Choose a supported writable Console profile or change Docker write policy
  before retiring that flow. Readonly release browser acceptance already passes.
- [ ] **CLI benchmark exit**: existing log-directory replacement test expects
  nonzero exit for port 1; asynchronous benchmark currently exits 0. Preserve
  its assertion and diagnose the production exit contract.
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
