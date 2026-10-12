<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).

## Current state

- Branch `task-deploy`; migration/classification is implemented. All former
  browser launchers are retired and important component UTs remain. Console
  has 153 root owners; assertion mappings remain in Git history and the local
  audit `/tmp/crowdb-r230-accepted-assertions.md`.
- Complete pre-repair Console run: 148 passed / 5 failed / 0 skipped,
  3259.845 s excluding client builds. `doc/working/test.md` records this complete
  failed run. Original logs: `/tmp/crowdb-r230-final-console.log`; run
  `run-38147325f7e14587addbca1072e8b79a`. Redacted export:
  `/tmp/crowdb-r230-console-baseline-public` (3228 files).
- Resource configuration stays unchanged by user direction. Native complete
  nodes keep 1 CPU / 2048 MiB / 512 PIDs. Diagnose actual resource failures
  before changing quotas; do not switch to CPU weights or lower quotas.
- Formal image from `f053b9c0`: OCI
  `sha256:66bf2a785032e26bafa4ac9bb1e06bf1a813eb9138b75b04058b344b3d0063b5`,
  Docker `sha256:49d25046ce13a23a442f6a8f0f54776dc3faba0a2fd7339111c934f061e90cd5`,
  debug retained. It includes the proven disk rotation correction (177 DiskDB
  tests, workspace clippy). Exact repair gates:
  `/tmp/crowdb-r230-repair-gates.log`.
- `c2745d61` orders durable managed-service recovery and shutdown using the
  release profile dependency order. Lexicographic recovery previously started
  Access/Chunk-KV before DiskDB and spent the original readiness budget waiting
  for unavailable prerequisites. All 75 monitor component tests, clippy and
  Rust fmt pass. New release staging:
  `/tmp/crowdb-r230-monitor-release-stage.log` (running).
- R139 owns deployment performance/configuration profile requirements. CLI
  command and benchmark redesign remain later review items; preserve the
  chunk-KV benchmark regression script.

## Accepted repair gates

- Disk rotation regression: focused UT fails before the correction, then all
  177 DiskDB tests pass. The exact split/transfer browser, convergence and data
  checks pass on `f053b9c0`: 351.705 s versus the 667.365 s failing baseline.
- Final debug release from `c2745d61` built/imported: OCI
  `sha256:67479dd0537f4bfa40fe603cd1fd51744dcafac8aa284067bd4b07e39ae67fda`,
  Docker `sha256:adec413288450a686a7cfdfd3a11dea216552214fe0884b533f0c2dc77fbd5a5`,
  runtime `2215f61456df7436da5295581ee5c1781a8c183ee9a20b077977295c64da7b55`.
  Exact Group-0 replay, confirmed DiskDB replay and all hardware badge assertions
  pass (`/tmp/crowdb-r230-final-repair-gates.log`). CLI checks durable service
  intent instead of collecting live PIDs merely to decide deployment; deadlines
  and acknowledged-object assertions remain unchanged. Badge observation pauses
  and restores the actual DiskDB through normal management APIs.
- Shell node stop and Journal owner recovery pass exactly on `f053b9c0`.
  Journal Back returned HTTP 409 in 12 ms after normal catalog publication;
  the flow now refreshes the map and reselects the same partition's current
  owner before stopping it. No unavailable-owner/data assertion was removed.
- Browser batches preserve each caught client's error until its own deferred
  teardown. Successful peers keep their successful state and stable networks.
  All 106 fixture tests and changed browser TypeScript checks pass.

## Remaining tasks

- [ ] Finish final quality gates and commit the accepted client/fixture repair
  batch. Current logs: `/tmp/crowdb-r230-repair-final-{clippy,fmt,fixture}.log`.
- [ ] Run complete Console and remaining affected container domains using the
  final debug release image. Each gate pins its image and client bundle.
  Record complete domain timings and only redacted diagnostics; AMD/macOS and
  remote CI remain unmeasured.
- [ ] Verify final Pixi/CI selectors, resource admission, artifact exports and
  retained UT gates. The 4-CPU capacity preflight is confirmed to reject the
  six-node profile before allocation: `selected cases require at least 6.5 CPU
  / 12800 MiB` (`/tmp/crowdb-r230-ci-capacity.log`). No resource setting changed.
- [ ] Once all required gates pass, simplify `test.md` to domain measurements
  and remaining issues, then remove R230 detail/backlog entry and this plan in
  the final cleanup commit. Do not close R230 while acceptance remains pending.

## Open Questions

- CI CPU admission: keep the 1 CPU/container ceilings and allow CPU sharing on
  the standard 4-CPU runner, with constrained verification, or continue strict
  reservation and use a larger runner? Asked asynchronously while independent
  local tests continue. Memory remains a hard admission limit.
