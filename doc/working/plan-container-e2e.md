<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).
Goal: finish final integration and measured regression, then close the requirement.

## Resume context

- Branch `task-deploy`; all original browser flows now have accepted root owners.
  Console inventory is 153. Last owner `console::browser_data::six_node_round_trips`
  passes on the formal release image from `b4625eb7`; image retains debug info.
  OCI `sha256:85da4c8377a7ef0f96f8228b90a895d4606d5623412e11ee01ca224b757ed9a1`,
  Docker ID `sha256:8a60f0750a4036a1213d020e8676c14257803c191dc9390ea53c6d0cb1418bb7`,
  runtime `3768da5acd2cbb509b9d201dbb26a7ddc14d12fefcca535c061ea5d9f804c62d`.
- Six-node proof keeps 3 storage + 3 diskless release containers, 36 actual
  service processes, automatic plans without Retry, exact KV/S3 bytes, three
  SDK Iceberg rows, and six-way dynamic service slots (170/171 each, 1024 total).
  Mounted disks and ownership are checked before data operations. Original
  defaults/cancel/tree assertions remain in their dedicated accepted owners.
  Evidence: `/tmp/crowdb-six-node-thread-observation.log`,
  `run-74b4ee8598c34dfca3e50f7be09eeed5`. Original browser ownership is retired.
- Fixed ports repeat across owned networks. Native full-service nodes reserve
  512 PIDs/threads (actual complete nodes about 300; the old 256 limit caused
  Cannot fork). CPU/memory remain bounded; six-node reservation is 6.5 CPU /
  12800 MiB, CI Console 8 CPU / 16384 MiB. Process diagnostics retain thread counts.
- Normal DiskDB deployment supports a 3-second keepalive cadence, carried by
  durable service plans. Fresh-disk initialization uses bounded 128-record batches
  and lazy bitmap words. The 4 KiB zero bitmap uses a 31-byte LZ4 constant;
  nonempty snapshots also use LZ4. Old-data compatibility is waived by user.
  Native logs show fresh-disk sync/load rounds of 27–36 ms. Component verification:
  protocol/DiskDB 397, deployment configuration/plans 4, UI plans 21, full UI 180,
  fixture 106, TypeScript, Rust fmt, workspace clippy and 28-package CI graph pass.
- Configuration profile requirements are tracked in R139 (`fbbe80e4`), including
  single-container, cluster low/high performance, test tuning and resolved values.
  CLI command and benchmark redesign are later review items; preserve the chunk-KV
  benchmark regression script. Important deterministic component UTs remain.
- Assertion references are retained in Git history and the prior accepted batches;
  local audit copy `/tmp/crowdb-r230-accepted-assertions.md`. Latest complete Console
  measurement still covers only the old 96-owner inventory; do not claim 153 yet.
- New package/task classification changes require a final formal image receipt
  before full-domain gates. Keep production sources and HEAD stable during build,
  and all E2E inputs stable while acceptance clients run.

- Ownership/classification audit passes: refresh production executable
  and in-process launcher inventory, prove every distinct source assertion belongs
  to accepted E2E or retained UT, preserve legitimate deployment callers of local
  port allocation, and verify canonical Pixi aggregate delegates run each required
  domain once. Check regular and special CI selectors, quotas, system browser and
  redacted artifacts. Retired browser port allocator and all old UI launchers are
  removed; `test-console-ui` is component-only; `test-suite` uses one required root
  container aggregate plus distinct SDK component proofs.

## Remaining tasks

- [ ] **Final release regression and measurements**: stage/import the final
  release artifact with debug info; rerun complete affected domains at documented
  quotas, including the expanded Console set and lifecycle response checks.
  Update `doc/working/test.md` at domain granularity. Publish only redacted
  measurements/diagnostics. Record remote CI, AMD and macOS as unmeasured.
- [ ] **Close R230**: after all local acceptance and classification gates pass,
  remove the requirement, backlog entry and this plan in a coherent cleanup commit.
