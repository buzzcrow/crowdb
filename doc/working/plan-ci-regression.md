<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CI Regression Plan

Upstream: [CI run 37274006189](https://github.com/buzzcrow/crowdb/actions/runs/37274006189).
Goal: reproduce all workflow jobs locally, repair ordinary failures, and retain
open issues for blockers before continuing other jobs.
This is a persistent issue record requested by the user; keep unresolved items
and remove resolved issues after verification.

## Job verification results

- **Lint — PASS**: exact workflow fmt, task inventory and workspace Clippy passed.
  TypeScript and tree lint also passed.
- **UnitTests — PASS**: `pixi run clean-env` and `pixi run test-unit` passed.
- **CppTests — PASS**: C++ build, tree/common/RPC/DiskIO and Rust FFI passed.
- **ServerTests — PASS AFTER FIX AND CONTINUATION**: full rerun passed every
  target through Access, then failed monitor bootstrap with `AddrInUse`.
  The monitor fixture now uses claimed service listener ranges instead of
  OS ephemeral ports, and respects the selected runtime root. The focused
  bootstrap/restart tests and complete monitor suite passed afterward. The
  two EC fixtures start with actual node degradation and enough expansion
  capacity; all 38 ChunkDB full-stack tests passed without skips.
- **ConsoleTests — PASS**: shared and CLI passed; full web rerun passed after
  fixing cancellation Group 0 setup and pre-store PKV health fallback.
- **UITests — PASS**: the final clean, serial full job passed all 153 frontend
  unit tests and 61 browser tests, including six-service pre-Group-0 acceptance.
- **S3E2E — PASS**: full job passed, including all 32 full-stack tests, boto3,
  AWS CLI, rclone and individual service recovery scenarios.
- **IcebergE2E — PASS**: full job passed, including PyIceberg, native files,
  GC, capacity recovery and file/table durable-write crash matrices.
- **IcebergSDK — PASS**: full job passed, including Java Catalog, commit and
  response-loss tests, all three native Java FileIO tests and selected Apache
  Iceberg RCK catalog tests. Follow-up native FileIO rerun passed all three
  tests with standalone SDK JVMs and no Maven thread cleanup warnings.
  Fixture configuration is inherited through the child environment because
  Maven consumes stdin before launching an external JVM.

Full local logs: `.crowdb-runtime/artifacts/local-ci/`.
Runtime jobs execute sequentially because cleanup removes owned subprocesses.
The follow-up's first UI run overlapped the ChunkDB suite and failed port
allocation and reset timing checks (59/61 passed). Its trace is preserved in
`local-ci/ui-followup-failure`. Both affected specs passed serially (7/7),
then the complete clean UI job passed (61/61, 4.5 min).
Final UI log: `local-ci/ui-final-serial.log`.

## Open issues

- **OPEN: historical native S3 read after six service restarts returned 502;
  currently not reproduced.** Initial
  writes/reads and all Node 1 restarts succeed; the next GET multipart.bin
  returned 502 after 4936 ms. Historical failing invocation:
  `pixi run env CROWDB_NATIVE_UI_E2E=1 CROWDB_NATIVE_UI_E2E_GREP='Chunk ownership' cargo test -p crowdb-web --test native_cluster_provisioning_test one_rack_three_nodes_provision_all_services_without_metadata_repairs -- --ignored --nocapture`.
  Logs: `.crowdb-runtime/artifacts/native-restart-failure-86554`.
  The failing assertion remains intact.
  Isolated real three-node ownership browser acceptance passed separately.
  S3E2E's individual Group 0, Access, ChunkDB, DiskDB, DiskIO and Chunk-KV
  restart recovery tests all passed; the historical failure came from the combined native
  console restart fixture.
  Follow-up: two cold runs using current binaries passed. The strengthened
  native fixture verifies ownership UI before/after restarts and reads the
  persisted S3 object after each individual restart; it passed in 45.28 s.
  Existing exact split-recovery/commit-proof tests also passed. Keep this issue
  open because the historical failure's root cause has not been reproduced or
  confirmed; do not describe passing reruns as a proven production fix.
- **OPEN: remote CI logs require authentication.** The jobs API confirms five
  failed jobs (Lint, UnitTests, ServerTests, ConsoleTests, UITests). Every log
  endpoint returns HTTP 403 and no GitHub API credential/browser session is available.
  Issues are recorded locally, not published on GitHub. Continue local workflow
  reproduction; remote rerun results are not yet verified.
