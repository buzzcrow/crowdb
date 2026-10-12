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
- Formal optimized debug release from `9c0f8805`, built/imported and verified:
  OCI `sha256:4e34adf470ae00773bcf24b37c3c328e9707e5160346b5e24618b91d4558e853`;
  Docker `sha256:15a8ceb58cc9a6928ccf89075de215856943eb1308b4b878c5e0ed86796031eb`.
  Receipt: `target/crowdb.oci.tar.json`. Each run pins image and client identities.
- Complete native nodes remain 1 CPU / 2048 MiB / 512 PIDs. User direction:
  continue tests; discuss resources only when measured failures require it.
- Final-image Console passes 153 / 0 / 0 in 2223.972 s; Deployment passes
  33 / 0 / 0 in 310.503 s. Original native load/inspection pair also passes.
  Full-run native load: 52805 ms mutation; retained bytes 56855736 before and
  after restart. Actual-config CLI replay and six-node ownership pass.
- R139 owns complete configuration/performance profiles. CLI command/benchmark
  redesign remains later review; preserve the chunk-KV benchmark script.

## Remaining tasks

- [~] **Remaining domains**: finish KV, DiskDB, DiskIO, ChunkDB, Chunk client,
  Chunk stream, Chunk-KV, S3, Iceberg and Dataset on the final image, with KV
  isolation proof. Resume was blocked by a source-guard false positive for an
  unstaged host development helper. Focused before/after proof and all 106
  fixture tests pass after excluding that exact helper; runtime/build inputs
  remain guarded. Preserve the concurrent worktree edits to that helper/README.
  Two independent runners reserve 16 CPU / 24576 MiB each on this 32-CPU host.
- [ ] **Special profiles**: rerun bridge SSH admission, Linux host,
  containerd host/cluster, ecosystem and Python/DuckDB after all domain clients
  end. Keep browser/host networks stable within each batch.
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
