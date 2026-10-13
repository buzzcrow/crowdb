<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).

## Current state

- Migration and classification are implemented; important component UTs remain.
  Formal optimized debug release: `9c0f8805`, OCI
  `sha256:4e34adf470ae00773bcf24b37c3c328e9707e5160346b5e24618b91d4558e853`,
  Docker `sha256:15a8ceb58cc9a6928ccf89075de215856943eb1308b4b878c5e0ed86796031eb`.
  Receipt: `target/crowdb.oci.tar.json`; each run pins client/image identities.
- Final-image Console 153/153, Deployment 33/33 and nine independent domains
  pass. Iceberg completes 31/32 in 1762.795 s, including its original approximately
  20-minute retirement grace. Measured domain rows are in [test.md](test.md).
- All six special profiles pass: bridge SSH admission, Linux host, containerd
  host/cluster, five-engine ecosystem and Python/DuckDB. Their identities and
  measurements are under `/tmp/crowdb-r230-profile-*-final/`.
- Unit/component repair gates pass: DiskDB 177, monitor 75, KV-server 64,
  CLI 6, fixture 106 and UI 183. Pixi/CI classification verifies 28 packages.
- Preserve concurrent worktree edits to the host development helper and README.
  Its unstaged cleanup command is excluded from image identity only after an
  audited staging/build read check and before/after fixture proof. Release
  source, Dockerfile and entrypoint changes remain guarded.

## Remaining tasks

- [ ] **Iceberg domain**: finish the complete domain rerun under CPU sharing.
  Original 32 × 8-MiB direct PUT now passes, including the final one-worker
  configuration (client 26.40 s, fixture 41.912 s). Full run retains the original
  retirement grace: `/tmp/crowdb-r230-iceberg-shares-full`.
- [x] **Resource gates**: all six special profiles pass under 1024 CPU shares
  per container and `TOKIO_WORKER_THREADS=1`, retaining memory/PID limits.
  Fixture 106, OCI 13, CLI tests/clippy/fmt, two-cluster isolation, final CLI
  lifecycle and six-node browser pass. Six-node fixture 94.473 s with a declared
  4-CPU budget and 12800 MiB reserved; remote four-core performance is unmeasured.
  Bridge cleanup timed out once under overlapping profiles; isolated diagnosis
  and the final uninstrumented run pass. Retain evidence as a timing issue,
  without assertions/deadline changes. Public diagnostics:
  `/tmp/crowdb-r230-shares-public`. Temporary instrumentation is removed.
- [ ] **Closure**: export final redacted diagnostics, update domain measurements
  and remaining issues, then delete the requirement, backlog entry and this plan
  in a gated cleanup commit. Keep benchmark/CLI redesign for later review and
  R139 configuration profiles deferred. Preserve concurrent development-helper
  and README edits. AMD/macOS and remote CI remain unmeasured.

## Verification

- Root-cause fix plus focused before/after evidence, affected component checks
  and exact acceptance; never increase deadlines, retry mutations or weaken
  assertions to make a symptom pass.
- Prepare browser-batch networks before clients and retain them until all peers
  end. Keep independent profiles from changing bridges during browser clients.
- Redacted exports: `/tmp/crowdb-r230-console-final-public`,
  `/tmp/crowdb-r230-deployment-final-public`,
  `/tmp/crowdb-r230-domain-final-public`; private originals remain for diagnosis.
