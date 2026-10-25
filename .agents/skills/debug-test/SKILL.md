---
name: debug-test
description: Run CROWDB test and CI jobs incrementally, and diagnose non-browser failures from first divergence to root cause.
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Debug Test

Do not increase timeouts, suppress errors, weaken assertions, add caller-side
retries, or otherwise make only the symptom pass.

## Test and CI progression

For expensive suites or multiple jobs, preserve an ordered list of commands,
completed results, logs, relevant source/image inputs, and the next unrun item.
Use existing runner records where available.

- Run until an error, then fix it before advancing the affected sequence. Do
  not first run every job against a known defect. Prioritize bugs that make
  subsequent tests slow, hang, or fail through the same dependency.
- For container E2E, use `--fail-fast` to stop queued cases while letting
  already owned fixtures finish and clean up. Read failure records throughout
  the log, not only its tail or completed-case count. Do not interrupt both a
  launcher and its child; forwarded duplicate signals can break cleanup.
- Reproduce and repair the failure with its test or smallest meaningful test
  group. Run that group's regression and relevant quality gates after the fix.
- Resume the remaining tests/jobs from the saved position. Retain earlier
  passing results; repeat them only when the fix changes their behavior or
  inputs. Preserve original ordering and coverage when selecting a remainder.
- Independent work already in flight may finish when it is unaffected. Stop
  dispatching work that depends on the defect or would repeat its slowdown.
- Complete shared build prerequisites before parallel test jobs. Never run
  concurrent builders against the same output directory; use independent
  output directories when parallel builds are necessary. Perform shared
  environment cleanup before dispatch; do not repeat `clean-env` while other
  local jobs still own running fixtures. Host browser fixtures that require
  stable Docker networks also share that constraint across local CI jobs;
  keep their browser phases separate from other jobs' bridge creation/removal.
- Before long suites, inspect actual fixture slots and declared CPU/memory
  costs. An outer parallel job count does not prove cases run concurrently.
  Distinguish build, fixture startup, client execution, and cleanup time.
- Reuse valid builds and images. Rebuild when their relevant inputs change;
  avoid unrelated feature/profile changes and repeated rebuilds while debugging.
- After the requested jobs have each been repaired and locally verified, run
  the requested or required overall verification once as a closing gate. If
  that gate finds another defect, repair its group and resume the affected
  remainder rather than restarting valid completed groups from the beginning.
- Report actual coverage, failures, and unrun/blocked work. A focused pass does
  not by itself establish that a whole suite or CI job passed.

## Diagnose a failure

1. Check relevant environment variables, stale processes, logs, persisted
   state, and build artifacts.
2. Reproduce the exact test. If it passes alone, compare the affected suite
   under its default concurrency and serial execution; inspect shared IDs,
   ports, temp directories, and leaked processes.
3. List setup, action, and assertion steps. Find the first divergence using
   logs, APIs, persisted data, and timing.
4. Classify it as code/design, timing/order, or environment.
5. For an unexplained process exit, analyze its crash report before changing code.
6. Add focused temporary instrumentation only when evidence is insufficient;
   remove it afterward.
7. Fix the earliest upstream cause. Rerun the test, affected suite, and quality
   gate. Add a regression test for a code bug.

For browser or console UI tests, use `/console-ui-e2e` instead. During active
requirement work, its retry and blocking rules remain authoritative.
