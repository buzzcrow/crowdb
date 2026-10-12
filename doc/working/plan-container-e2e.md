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
- Formal optimized debug release from `de0fe0eb`, built/imported and verified:
  OCI `sha256:a3c632c64a8d8d846b7886c19e33f0e24b9cb9d78d2effc1007bd086c27ca7c6`;
  Docker `sha256:128142f43b7978684e653ebcd5661fffcd1c1798d69e902cee6ad391cd3e0a13`.
  Receipt: `target/crowdb.oci.tar.json`. Each run pins image and client identities.
- Complete native nodes remain 1 CPU / 2048 MiB / 512 PIDs. User direction:
  continue tests, diagnose actual resource failures before changing limits.
  Original complete-run cgroup samples show no OOM; peak memory 1884.1 MiB.
- Latest complete Console run on `7a94486c`: 151 / 2 / 0, 2160.528 s. Multipart,
  split/transfer, Journal, load and Group-0 replay pass. The two remaining failures
  were zero-extent viewport animation and default 10-second DiskDB discovery.
- CLI exposes a normal recorded heartbeat cadence (default 10 seconds); test
  creation requests 3 seconds. Exact concurrent CLI replay passes. A real
  ReactFlow component test fails before hidden-panel motion cancellation and
  passes afterward. New-image logical and canvas scopes pass in 34.013 s and
  24.335 s. All 182 UI/types, 6 CLI/clippy and 106 fixture checks pass.
- R139 owns complete configuration/performance profiles. CLI command/benchmark
  redesign remains later review; preserve the chunk-KV benchmark script.

## Remaining tasks

- [~] **Final browser domains**: finish new-image Deployment and all 153 Console
  owners. Driver: `/tmp/crowdb-r230-canvas-final-gates.py`; log:
  `/tmp/crowdb-r230-canvas-final-gates.log`. It runs exact logical/canvas scopes
  first (both passed), then complete Deployment and Console. Keep production and
  E2E client inputs stable. Files: `e2e-test/`, affected modules only if a new
  failure identifies an upstream defect.
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
