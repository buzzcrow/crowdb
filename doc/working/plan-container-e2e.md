<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).
Finish canonical release-container acceptance and test classification.

## Current state

- Branch `task-deploy`, client revision `7a94486c`. Migration/classification is
  implemented; old browser launchers are retired and important component UTs
  remain. Console has 153 canonical owners. Assertion audit:
  `/tmp/crowdb-r230-accepted-assertions.md`; implementation evidence is in Git.
- Formal optimized release from `7a94486c`, with debug info retained:
  OCI `sha256:cb8e4d77314e1d38a9c24b8985d3b398da1265a9b4c72636077fadb70aa05666`;
  Docker `sha256:5e59f9713d441fba837929ba9874882b3e892c28020b94a384aead34e2b01072`.
  Receipt: `target/crowdb.oci.tar.json`. Clients pin this image and their bundle.
- Native complete nodes remain 1 CPU / 2048 MiB / 512 PIDs by user direction.
  Diagnose actual resource failures before changing limits. Host clients use
  inspected IPs and fixed ports; owned bridge lifetimes cover every batch peer.
- Latest complete Console run: 151 / 2 / 0, 2172.056 s, with the same
  resource limits. Catalog observation passes; multipart upload and CLI
  Group-0 restart remain failing. The prior complete run took 1804.006 s.
- R139 owns performance/configuration profiles. Later CLI command/benchmark
  redesign remains a review item; preserve the chunk-KV benchmark script.

## Remaining tasks

- [~] **Console acceptance**: finish the current 153-owner run, diagnose the
  first failing boundary, and pass the affected selection and complete domain.
  Files: `e2e-test/clients/console-ui/`, `e2e-test/tests/console/`, affected server
  modules only where evidence identifies a code defect.
  Complete run: `/tmp/crowdb-r230-console-final/run-044064d59d34492a90d30f05884910c0`;
  log: `/tmp/crowdb-r230-console-final-lifetimes.log`. Multipart part 1 returned
  HTTP 503 after 5.260 s; Access reports an ambiguous DiskIO write deadline,
  before the missing object-row assertion. No timeout/resource change made.
  CLI Group-0 restart additionally logs a stale stream writer epoch, followed
  by expired serving grants and unavailable readiness. Exact isolated multipart
  and concurrent CLI reproduction with cgroup CPU/memory collection run in
  `/tmp/crowdb-r230-s3-reproduction.py`; logs use the same filename prefix. Multipart exact reproduction passes;
  runtime resource samples have no OOM. Concurrent CLI reproduction also passes. Full-domain failures remain recorded.
- [ ] **Remaining domains**: run Deployment alone, then KV, DiskDB, DiskIO,
  ChunkDB, Chunk client, Chunk stream, Chunk-KV, S3, Iceberg and Dataset.
  KV must include concurrent isolation proof. Queue:
  `/tmp/crowdb-r230-domain-gates.py`, log `/tmp/crowdb-r230-domain-gates.log`.
  Deployment passes 33 / 0 / 0 in 307.253 s on `9225badd`. Queue driver PID
  642645 is temporarily stopped after that gate until new-image Deployment, CLI and complete Console gates finish.
  Driver `/tmp/crowdb-r230-catalog-final-gates.py` resumes it automatically after
  these browser gates; log `/tmp/crowdb-r230-catalog-final-gates.log`. Complete
  Console additionally records cgroup CPU/memory observations in
  `/tmp/crowdb-r230-console-catalog-resource.log`.
  Keep production/client inputs stable during gates; do not mutate unrelated
  Docker bridges during browser tests.
- [ ] **Classification and diagnostics**: verify final Pixi/CI selectors and
  artifact export. Publish only redacted diagnostics, preserve relevant UTs,
  and record complete domain wall times. Files: `pixi.toml`, `.github/workflows/`,
  `e2e-test/publish.py`, `doc/working/test.md`.
- [ ] **Closure**: simplify tracking to one row per domain, remaining issues,
  and intel7960/amd5950/macm5pro environment sections. Once required gates pass,
  remove R230 detail, backlog entry and this plan in a gated cleanup commit.
  AMD/macOS and remote CI remain unmeasured; do not claim those gates passed.

## Current repair

- Group-0 publication can succeed before the separate transition status write.
  Materialization then changes/removes overlays and markers; exact original
  successor comparison incorrectly rejects its own already-published catalog.
  The regression fails before the fix. Publication now recognizes only exact
  stable successor identities with cleared overlays/markers, rejects changed
  epochs and misplaced markers, and preserves the newer catalog.
- Files: `app/crowdb-kv-server/src/background/domain_monitor/chunk_kv/catalog.rs`,
  `app/crowdb-kv-server/tests/common/catalog_publication.rs`, shared fixtures in
  `tests/domain_monitor_test.rs`, and the permanent Chunk-KV server design.
- Split cases with retained/released markers and the transfer interruption case
  pass, while a changed owner epoch remains rejected. All 63 KV-server component
  tests, targeted clippy and workspace fmt pass. Logs:
  `/tmp/crowdb-r230-catalog-resume-{before,after,tests,clippy}.log`.
- Formal debug release built/imported and digest verified. New-image Deployment,
  CLI recovery and complete Console are running in the driver above. This proven publication defect does not yet establish that
  every intermittent failure has been resolved. Multipart remains under diagnosis.

## Verification

- Unit/component: affected Rust tests, fixture tests, UI component tests/types.
  Latest accepted gates: 177 DiskDB, 75 monitor, 5 CLI, 106 fixtures, 181 UI.
  Run appropriate changed-code fmt/clippy gates separately before committing.
- E2E: canonical complete domain results, exact browser regression when fixed,
  CLI replay identity/data/lifecycle checks, KV concurrent isolation.
- Export: `e2e-test/publish.py`; retain private originals for diagnosis.

## Open Questions

- CI resource admission: the standard 4-CPU capacity preflight rejects the
  six-node case before allocation (`at least 6.5 CPU / 12800 MiB`), recorded in
  `/tmp/crowdb-r230-ci-capacity.log`. A previous question about sharing CPU or
  using a larger runner remains unanswered. Continue local tests; revisit
  resources when evidence requires it. No quotas or admission rules changed.
