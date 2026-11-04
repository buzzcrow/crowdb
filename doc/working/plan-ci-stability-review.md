<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CI Stability Review Plan

Upstream: [Physical Node Test Plan](../dev/physical-node-test-plan.md),
[container acceptance](../../e2e-test/README.md), and
[CI workflow](../../.github/workflows/ci.yml).

Goal: track the remaining physical-test stability issue.

## Execution status

The requested test execution and resource closure finished on 2026-11-04.
One complex issue remains recorded below under the user's instruction to finish
independent tests and defer complex repairs. Completed tasks are removed.

All evidence paths below are relative to
`.crowdb-runtime/artifacts/physical-nodes/physical-20261031T194149Z/`.

- **Local CI reruns**: UnitTests 1,181 passed, Console API 92 passed, and Console
  Native Operations 19 passed with the approved ten-second backend convergence
  budget and three-second diagnostic. UI/process budgets stay unchanged. The
  original Native failure is retained; its eventual recovery and control-plane
  delay cause remain unproven. Evidence:
  `ci/requested-full-rerun-20261104/approved-convergence/`.
- **Physical qualification**: P09 and independent P14 passed. P13 supplementary
  qualification passed its corrected expired-lease matcher, positive exit
  statuses, strict stderr, 24,311 healthy responses and unchanged 35-process
  scope. P16 supplementary consistent-inventory qualification passed: 8,961
  busy records, 1,641 chunk records, 8,756 physical segments, no missing busy
  allocations or overlaps, 35 unchanged process identities and 370 healthy
  responses. Its 16 controls passed. Evidence:
  `repair/readiness-probe-budget-2s/saved-p13-supplemental-review/original-gates/result.json`
  and `repair/readiness-probe-budget-2s/p16-consistent-audit-staging/physical-attempt-2/result.json`.
  Original failures remain retained. Results stay scoped to their actual source
  and image; this is not fresh full-matrix acceptance.
- **Probe review**: all 141 CI warnings occurred in fault/restart scenarios;
  119 have same-endpoint recovery evidence. The other 22 Native records lack
  exact saved Ready generations. P14's five and P13's 40 physical failures have
  subsequent-generation recovery. No stable-load failure is established, so the
  production two-second probe budget stays unchanged. Evidence:
  `repair/readiness-probe-budget-2s/probe-review-completion-20261104/review.json`.
- **Resource closure**: all seven exact owned containers and volumes were
  retired after complete service/crash-log capture and verified B-to-A copying.
  Resource absence, closed listeners, evidence hashes and SSH access passed.
  Docker's varying mount order caused one pre-delete refusal; a checked
  complete-entry sort allowed the exact comparison, without dropping fields.
  That refusal is retained. No new volume archives or broad pruning. Evidence:
  `repair/readiness-probe-budget-2s/resource-closure-preparation/driver/disposition.json`.

## Unfinished task

- [ ] **P13 WAL release failure path**: During lease-fenced shutdown, B's two
  original survivor processes reported `partition writes are stalled`. Identify
  the affected partition/stream and first typed failure in
  `release_drained_writer`, journal release, stream command dispatch/completion,
  or worker release. Existing logs do not distinguish these paths or prove a
  successful historical release. Reproduce the smallest proven cause before
  selecting a repair, then verify successor lease exclusion and acknowledged
  data replay. Stop after two unsuccessful repair attempts and retain evidence.
  Files: `app/crowdb-chunk-kv-server/src/server/drain.rs`,
  `lib/crowdb-chunk-kv/src/partition/drain.rs`,
  `lib/crowdb-chunk-kv/src/partition/journal.rs`, and
  `lib/crowdb-chunk-stream/src/stream.rs`.
  Evidence: `repair/readiness-probe-budget-2s/wal-drain-log-repair/unresolved-issue.json`.
  The separate unconditional success-log bug in
  `app/crowdb-chunk-kv-server/src/main.rs` is fixed; three graceful-drain tests,
  Rust fmt and focused clippy passed. The physical image was not rebuilt for
  this logging-only change; it does not repair the release path.
