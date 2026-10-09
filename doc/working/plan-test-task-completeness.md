<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Access and Iceberg Test Task Completeness Plan

Upstream: [test matrix](test.md), existing Pixi SDK preparation tasks.
Goal: each named task runs all its functional acceptance cases in one invocation,
including environment-specific preparation, without manual follow-up or retries.

## Tasks

- [x] **Close task coverage**: `test-access` currently leaves
  `official_boto3_recognizes_a_copy_error_after_http_200_and_keepalives` to another
  task. Dispatch that exact test in the pinned Boto3 environment from the same
  parent task. Reuse its SDK environment setup; avoid running the full Boto3
  suite unnecessarily. `test-iceberg-e2e` must invoke the existing Java FileIO
  preparation task for all three `official_java_` cases after its Python/native
  phases. Keep sequential owned cluster execution and fail-fast behavior.
  Files: `tools/pixi-tasks/test-access.sh`, `test-iceberg-e2e.sh`, existing SDK
  setup scripts and `pixi.toml` only if a narrowly scoped task is needed.
- [ ] **Verify exact case inventory**: distinguish helper subprocess entry points
  from acceptance cases. The native fault-listener child is executed by its
  parent crash matrix, not a standalone acceptance case. Keep manual stress
  profiles explicit; list any remaining exclusions and reasons instead of
  counting another task's results as this task's completion.
- [~] **Verify one-call acceptance in CI**: run `pixi run test-access` and
  `pixi run -e iceberg-e2e test-iceberg-e2e` sequentially, capture complete logs
  and independent wall times. Confirm the Boto3 case and all three Java cases
  execute and pass in their parent task invocation. No retry loop, weakened
  assertion or core code change. Missing required dependencies fail the task.
- [ ] **Reconcile counts and matrix**: deduplicate cases by test binary/name
  across ordinary and explicit ignored-test dispatches. Do not add ignored
  lines to totals after that exact case passed in a later phase of the same
  invocation. Mark complete only after the entire task exits successfully;
  update counts, actual timings and retained logs in `test.md`.

## Verification

- Existing Rust assertions remain unchanged; this is task orchestration work.
- Check modified shell syntax through `pixi run bash -n <script>`.
- Run `pixi run check-ci-test-tasks` after task dispatch changes.
- Split/move recovery boundary regressions remain in the cutover plan and resume
  after these two test-task coverage gaps are closed. R228/R229 remain deferred.

## Current verification

- Parent task dispatch is implemented; assertions and core code are unchanged.
- Shell syntax and CI component mapping pass; the precise Boto3 case is run
  locally before push (1/1, zero ignored, 1.65s). Java cases reuse the existing native preparation script.
- CI push includes `codex/deploy` so this branch starts the complete component
  jobs without merging. One-call acceptance and updated full counts/timings
  remain pending CI results; previous partial rows are not marked complete.
