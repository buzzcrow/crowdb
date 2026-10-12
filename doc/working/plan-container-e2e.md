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

## Running final gates

- Accepted repair batch committed `57e680b6`; final workspace clippy, Rust fmt,
  fixture 106 and browser TypeScript pass. No server CPU/memory quota changed.
- Complete Console: `/tmp/crowdb-r230-console-acceptance.log`,
  `run-1b64e21b7207408486f7c93831407c18`; concurrency cap 2, admission budget
  16 CPU / 32768 MiB, native node 1 CPU / 2048 MiB. Image pinned to `c2745d61`.
- Remaining domains are queued behind Console in
  `/tmp/crowdb-r230-domain-gates.py` (running; log
  `/tmp/crowdb-r230-domain-gates.log`). Deployment browser domain runs alone;
  then two non-browser domain runners run concurrently, each with 16 CPU /
  24576 MiB admission budgets. KV requires the concurrent isolation proof.
  Keep production and E2E client inputs stable until their respective gates end.

- Concurrency regression catches one remaining badge setup race: pausing only
  DiskDB-1 lets DiskDB-2/3 take over new groups and replace deliberate Suspect
  state during initialization. Real node logs confirm the takeover. Current
  flow pauses all three actual allocators and resumes acknowledged stops in
  finally; original six badge assertions remain. Exact gate pending.
- Domain queue now waits for a passing complete run under
  `/tmp/crowdb-r230-console-final`, so a failed interim Console run cannot
  trigger unrelated network mutations during further browser repair gates.

- Full concurrency regression additionally exposes a same-session deployment
  progress race: Retry and automatic reconciliation send revision 4 concurrently
  (PUTs 7 ms apart; one HTTP 409). A delayed-response regression UT reproduces
  it before the fix. Start/Retry now queue behind the current reconciliation;
  all 22 plan UTs and full UI 181/types pass, with cross-browser conflicts still
  visible and no mutation retry added. New formal UI release and exact browser
  gate are required before the next complete Console run.
- The waiting badge recheck process was stopped before it allocated any fixture,
  because the pending production UI repair requires a newly packaged image.

- Completed concurrency run on `c2745d61`: 151 / 2 / 0, 2167.298 s,
  excluding shared builds. Both newly exposed failures are reproduced and fixed.
- Debug release from `9225badd` built/imported: OCI
  `sha256:342f0376c3ee878a9f7c2779760e29e497458ad2681ea633a62f8950ba7b62f5`,
  Docker `sha256:b3bd59255e79d66409089f85a44833797e17f8c3c949ae30c16cd33019447377`,
  runtime `2f846afbf04c6146bd812f725dbd6eb4848e9cd591ccf5d1b73fc674282717ff`.
  Exact badge (all three allocators paused) and plan reconciliation gates both
  pass (`/tmp/crowdb-r230-ui-repair-gates.log`). Badge TypeScript also passes.
  All original assertions and HTTP/DOM deadlines remain.
