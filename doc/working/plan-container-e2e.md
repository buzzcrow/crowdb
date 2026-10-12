<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).
Finish canonical release-container acceptance and test classification.

## Current state

- Branch `task-deploy`. Migration/classification is implemented, old browser
  launchers are retired, and important component UTs remain. Console has 153
  canonical owners. Assertion audit: `/tmp/crowdb-r230-accepted-assertions.md`;
  completed implementation/repair evidence remains in Git history.
- Formal optimized debug release from `1fff5fb0`, built/imported and verified:
  OCI `sha256:0ad53a329df28d5f5e45916975fc6af5d6f1e22bd0219026c5f45a59501d7bef`;
  Docker `sha256:e7e13284dc8f166c68ee06445406146bb92cec74c1d05dbdcc57d5d9fab86332`.
  Receipt: `target/crowdb.oci.tar.json`. Each run pins image and client identities.
- Complete native nodes remain 1 CPU / 2048 MiB / 512 PIDs. User direction:
  continue tests; discuss resources only when measured failures require it.
- Latest complete Console: 149 / 4 / 0, 2075.462 s. On the current image,
  focused six-node ownership and CLI replay pass. CLI verifies all three actual
  running DiskDB intent configurations use 3 seconds before and after restart.
- Native load passes alone: 32 MiB mutation 42396 ms, complete test 60.38 s,
  retained bytes 57379752 before and after restart. Original load/inspection
  pair still fails: mutation passes in 56551 ms, then owner restart HTTP exceeds
  20 seconds. Load nodes peak at 892.8 MiB, OOM/max-limit events are zero.
- R139 owns complete configuration/performance profiles. CLI command/benchmark
  redesign remains later review; preserve the chunk-KV benchmark script.

## Remaining tasks

- [~] **Final browser domains**: package the lease-excluded transfer repair,
  rerun the original native load/inspection pair, then all 153 Console owners
  and complete Deployment on the final image. Deployment last passes 33 / 0 / 0
  in 314.358 s. Preserve deadlines, resources and distinct assertions.
  Drivers `/tmp/crowdb-r230-canvas-final-gates.py` (PID 1437674) and
  `/tmp/crowdb-r230-domain-gates.py` (PID 642645) remain stopped; do not resume
  independent network allocation during active browser/client batches.
- [ ] **Remaining domains**: finish KV, DiskDB, DiskIO, ChunkDB, Chunk client,
  Chunk stream, Chunk-KV, S3, Iceberg and Dataset with KV isolation proof.
  Queue `/tmp/crowdb-r230-domain-gates.py` (PID 642645) is stopped after early
  KV/ChunkDB clients ended; DiskDB/DiskIO also passed. The browser driver resumes
  it after its own gates end. Log `/tmp/crowdb-r230-domain-gates.log`.
  Repeat early short domains on the final image if needed for uniform records.
  Two non-browser runners reserve 16 CPU / 24576 MiB each on the 32-CPU host.
- [ ] **Classification and diagnostics**: verify final Pixi/CI selectors,
  resource admission and redacted artifact exports. Record complete domain
  timings. Files: `pixi.toml`, `.github/workflows/`, `e2e-test/publish.py`,
  `doc/working/test.md`. Important UTs remain; latest focused gates include
  DiskDB 177, monitor 75, KV-server 63, CLI 6, fixture 106 and UI 182.
- [ ] **Closure**: update one tracking row per domain, remaining issues and
  intel7960/amd5950/macm5pro sections. Once required gates pass, remove R230
  detail, backlog entry and this plan in a gated final cleanup commit.
  AMD/macOS and remote CI remain unmeasured; do not claim those gates passed.

## Verification

- Unit/component: focused regression before/after proof, affected component
  suites, fixture tests, UI types and browser-client types. Run changed-code
  fmt/clippy gates separately before committing.
- E2E: complete domain results, original distinct assertions, CLI retained
  identity/data/lifecycle, KV concurrent isolation. Prepare all stable-batch
  networks before clients and retain them until every peer client has ended.
- Export only redacted text through `e2e-test/publish.py`; retain private
  originals for diagnosis. Current failed-run export:
  `/tmp/crowdb-r230-console-lifetimes-public`.

## Open Questions

- CI resource admission: the standard 4-CPU preflight rejects the six-node case
  before allocation (`at least 6.5 CPU / 12800 MiB`), recorded in
  `/tmp/crowdb-r230-ci-capacity.log`. The previous CPU-sharing/larger-runner
  question remains unanswered. Continue local tests; revisit resources when
  required. No quotas or admission rules changed.

## Current gate failure and next repair

- Focused current-image results: six-node ownership, isolated native load and
  actual-config CLI replay pass. Pair artifacts:
  `/tmp/crowdb-r230-native-focused-pair/run-2e681f7ec7764850aea55d997f4cee2c`.
- Persisted evidence: split is committed at catalog generation 2; lease-expired
  transfer is TargetReady at epoch 3, while catalog still names source epoch 2.
  Publication incorrectly requires the catching-up marker. Tick fails with
  `range catalog transition identity was reused`, so epoch-2 recovery is fenced
  and monitor repeats the unready restart approximately every 11 seconds.
- Exact regression reproduces that publication failure before the repair.
  Repair permits direct publication only with the lease-exclusion proof and
  exact source assignment; retained completed-split markers without overlays
  are accepted. Existing live-fence/catching-up publication remains unchanged.
- Package the gated repair, rerun the original pair, then full Console and
  Deployment. Resume remaining domains after stable browser batches finish.
  Do not close R230 while required gates remain pending.
