<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CI Regression Plan

Upstream: [CI run 37274006189](https://github.com/buzzcrow/crowdb/actions/runs/37274006189).
Goal: reproduce all workflow jobs locally, repair ordinary failures, and retain
open issues for blockers before continuing other jobs.
This is a persistent issue record requested by the user; keep unresolved items
and remove resolved issues after verification.

## Job verification

- [x] **Lint**: exact workflow fmt, task inventory and workspace Clippy passed.
  TypeScript and tree lint also passed.
- [x] **UnitTests**: `pixi run clean-env` and `pixi run test-unit` passed.
- [x] **CppTests**: C++ build, tree/common/RPC/DiskIO and Rust FFI passed.
- [x] **ServerTests**: original job failed on two EC fixtures (open issue below);
  continuation passed every other target with those two tests explicitly skipped.
- [x] **ConsoleTests**: shared and CLI passed; full web rerun passed after
  fixing cancellation Group 0 setup and pre-store PKV health fallback.
- [x] **UITests**: full job passed, 153 frontend UT and 60 browser tests.
  The updated rack/node spec also passed all eight tests, including added
  six-service pre-Group-0 acceptance. Together these cover 61 browser cases.
- [~] **S3E2E**: run `pixi run -e s3-e2e test-boto3-e2e`.
- [ ] **IcebergE2E**: run `pixi run -e iceberg-e2e test-iceberg-e2e`.
- [ ] **IcebergSDK**: run `pixi run -e iceberg-e2e test-iceberg-sdk`.

Full local logs: `.crowdb-runtime/artifacts/local-ci/`.
Runtime jobs execute sequentially because cleanup removes owned subprocesses.

## Open issues

- **OPEN: native S3 read after all six service restarts returns 502.** Initial
  writes/reads and all Node 1 restarts succeed; the next GET multipart.bin
  returns 502 after 4936 ms. Reproduce with
  `pixi run env CROWDB_NATIVE_UI_E2E=1 CROWDB_NATIVE_UI_E2E_GREP='Chunk ownership' cargo test -p crowdb-web --test native_cluster_provisioning_test one_rack_three_nodes_provision_all_services_without_metadata_repairs -- --ignored --nocapture`.
  Logs: `.crowdb-runtime/artifacts/native-restart-failure-86554`.
  Skip this failure as requested; the failing assertion remains intact.
  Isolated real three-node ownership browser acceptance passed separately.
- **OPEN: remote CI logs require authentication.** The jobs API confirms five
  failed jobs (Lint, UnitTests, ServerTests, ConsoleTests, UITests). Every log
  endpoint returns HTTP 403 and no GitHub credential/session is available.
  Issues are recorded locally, not published on GitHub. Continue local workflow
  reproduction; remote rerun results are not yet verified.

- **OPEN: two ChunkDB EC fixtures expect repair for rack-only degradation.**
  `degraded_ec_markers_recreate_one_task_per_large_strip_after_admission_gap`
  fails at full_stack_test.rs:749; isolated reproduction fails identically.
  `expanded_topology_converges_degraded_ec_matrix` indexes an empty task list
  at full_stack_test.rs:504. The initial six-node 10+2 layout meets node/disk
  loss budgets, so no repair marker/task is admitted. The permanent ChunkDB
  design explicitly makes rack protection a preference, not a repair trigger.
  This is a pre-existing fixture/contract mismatch outside the console changes.
  Keep assertions intact and skip these two tests as requested; continue all
  other ChunkDB and ServerTests targets. Logs: `local-ci/ServerTests.log` and
  `local-ci/Chunkdb-degraded-marker.log` under the artifact directory.
