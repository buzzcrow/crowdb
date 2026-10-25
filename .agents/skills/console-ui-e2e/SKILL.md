---
name: console-ui-e2e
description: Change visible CROWDB console UI or work on its Playwright E2E tests; not for backend-only changes or general questions.
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console UI E2E

Apply to `app/crowdb-web/ui/src/**` and `e2e-test/clients/console-ui/**`, including
browser-test failures and runtime regressions. Run the affected spec for every
visible change; add a regression assertion for every UI bug fix.

## Structure and cost

- Specs: `e2e/flows/NN-<area>-<function>.spec.ts`. Prefixes order the
  single-worker suite: `0x` shell, `1x` physical, `2x` KV cluster, `3x` KV
  data, `4x` inspector, `5x` capacity/DiskDB, `9x` cross-function.
- Extend the existing page-function spec; add one only for a new function.
- Register each mutating flow under the root runner with an owned release
  fixture. Use inspected node IPs and fixed ports; host browser clients never
  launch server binaries. Fresh fixtures provide empty-backend isolation.
- Preserve original distinct assertions when coalescing flow owners. Important
  deterministic UI logic remains in component tests.
- Record `// Baseline: Xs (date)`; investigate runtime above 2x.
- Measure the current exact test before changing it; saved timing reports may
  describe an earlier worktree. Compare Playwright's per-test time, not the
  command wall time that includes UI build and web-server startup.
- Instrument setup, mutation response, lifecycle poll, DOM refresh, and
  teardown separately with `stepTimer`. Optimize the first slow boundary.
- Before removing repeated setup from a cross-function flow, locate the
  dedicated specs that retain each behavior and assertion.

## Assertions

- Trace handler -> JSON -> hook -> props -> DOM before asserting unfamiliar
  state. Probe raw API data when its shape is uncertain.
- Prefer role, label, test-id, and scoped locators. Add `data-testid` for
  ambiguity; never resolve it with page-level `.first()` or `.last()`.
- Assertions time out at 3 seconds; leader election at 10 seconds.
  `expect.poll` uses `intervals: [100]`.
- Poll lifecycle state; never sleep.
- Do not assert on toast alerts. If one intercepts a click, click via `evaluate`.
- Never swallow errors, weaken assertions, add retries, or inflate waits.
  Use `waitForResponse` only when the response is an assertion target, not as
  a substitute for observing lifecycle or DOM state.

## Diagnose

For a failing, flaky, or slow E2E test, read
[references/diagnosis.md](references/diagnosis.md). Ordinary UI implementation
does not need that reference.

## Run

Follow [test and CI progression](../debug-test/SKILL.md#test-and-ci-progression)
when sequencing suites or CI jobs. Fix a failure with its affected spec/group,
then resume the remaining selection; use this skill for browser diagnosis.

```sh
pixi run clean-env
pixi run test-container-e2e --scope console.operations --case <registered-owner> --concurrency 1 --node-cpus 0.25 --cpus 8 --memory-mib 16384
```

Use the root runner for the full Console container domain. `pixi run test-console-ui`
runs UI component tests and types without servers or Docker. Browser tests use
an installed system browser; do not install a private browser. If blocked,
keep the regression assertion and report why.
