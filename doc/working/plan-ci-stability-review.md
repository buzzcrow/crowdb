<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CI Stability Review Plan

Upstream: [Physical Node Test Plan](../dev/physical-node-test-plan.md),
[container acceptance](../../e2e-test/README.md), and
[CI workflow](../../.github/workflows/ci.yml).

Goal: track the unresolved physical qualification and probe-observation work.

## Current results

On 2026-11-04, the current-image physical P09 and independent P14 passed.
P13 failed its survivor lease-evidence assertion; its data and recovery
observations passed, but its final exit, stderr, and health qualification did
not run. P16 failed its fixed-cutoff occupancy audit after the data and capacity
checks passed. Earlier physical results remain scoped to their original image;
these targeted results do not constitute full-matrix acceptance.

The three CI jobs associated with runs
[41329198767](https://github.com/buzzcrow/crowdb/actions/runs/41329198767) and
[41361029738](https://github.com/buzzcrow/crowdb/actions/runs/41361029738)
were rerun locally: UnitTests passed 1,181 tests and Console API passed 92
cases. Console Native Operations first failed one case; its other 18 cases
passed, the failed case passed separately, and a second complete run passed all
19. The user subsequently approved a ten-second limit for this backend
dependency-convergence prerequisite, with diagnostics after three seconds;
UI and process-observation assertion budgets stay unchanged. The updated
focused case and complete Native job passed (19/19, 420.334 seconds). The two
dependency checks took 6 ms and 5 ms, so the three-second diagnostic did not
fire. The original failed migration window remains retained; this pass does
not establish its eventual recovery or the cause of its control-plane delays.
Evidence: `ci/requested-full-rerun-20261104/approved-convergence/`.

The artifact root below is
`.crowdb-runtime/artifacts/physical-nodes/physical-20261031T194149Z/`.
The five current test containers and seven scoped volumes remain retained.

## Unfinished tasks

For each issue, stop after two unsuccessful repair attempts, record the result,
and continue independent work. Complex issues are recorded for later analysis.

- [ ] **P16 occupancy audit**: Determine why `/chunk/` metadata changed during
  the fixed-cutoff audit over slots 2415597..=2415605. The immediate read-only
  journal capture found two chunk updates and no busy updates, but did not
  establish predecessor values, changed fields, or whether allocations overlap.
  Obtain sufficient evidence for a consistent audit, preserve the original
  completeness and nonoverlap checks, and requalify P16. Files:
  `repair/readiness-probe-budget-2s/host-readiness-contract-continuation/independent-p16-attempt-1/run/`,
  especially `P16-immediate-journal/`.
- [ ] **P13 survivor qualification**: Integrate and verify the staged test
  correction for the valid expired-lease fencing path, then rerun the complete
  P13 action with its original data, observer, exit-status, stderr, and health
  checks. The staged focused controls passed, but do not replace the original
  failed physical qualification. Files:
  `repair/readiness-probe-budget-2s/p13-lease-evidence-staging/` and
  `repair/readiness-probe-budget-2s/host-readiness-contract-continuation/targeted-physical-driver/run/`.
- [ ] **P13 WAL drain**: Investigate the two survivor processes' incomplete WAL
  writer drain during lease fencing, including stalled partition writes and
  expired-lease withdrawal. The current logs do not prove a successful drain or
  identify its cause. Files: the P13 survivor application and RPC logs under
  `faults/readiness-probe-budget-2s-P13-survivor-exit-logs/`.
- [ ] **Review failures with a two-second probe budget**: Inspect the saved failures,
  particularly those without per-endpoint recovery evidence, and decide whether
  any timeout should change. Keep sampling after failures and report the
  configured budget, actual elapsed time, error, and recovery; do not silently
  increase the two-second budget. P14 recorded five 2000 ms ChunkDB deadlines
  during its fault, with 2000/2000/2001/2007/2000 ms elapsed and recovery in
  the next generation. The CI reruns recorded further fault/restart warnings,
  some with exact recovery unknown. Files:
  `repair/readiness-probe-budget-2s/host-readiness-contract-continuation/p13-probe-inventory/`,
  `repair/readiness-probe-budget-2s/host-readiness-contract-continuation/independent-p14-attempt-1/run/`,
  and `ci/requested-full-rerun-20261104/attempt-{1,2}/access-warning-audit.json`.
  The approved ten-second convergence runs add eight warnings with the same
  2000 ms dependency and 2100 ms outer budgets; actual elapsed times are 0,
  2000, or 2001 ms. Their exact per-endpoint recovery remains unconfirmed.
  File: `ci/requested-full-rerun-20261104/approved-convergence/access-warning-audit.json`.
- [ ] **Finish physical qualification and resource closure**: After addressing or
  explicitly disposing of the remaining failures, reconcile all results by
  source and image, preserve their logs, and decide the exact cleanup of the
  owned test resources. Physical work is stopped at the user's request after
  CI verification and publication. Do not represent mixed-image results as a
  fresh full-matrix pass. The previous detailed plan and its deferred review
  descriptions remain at `ci/requested-full-rerun-20261104/plan-before-open-items.md`.
