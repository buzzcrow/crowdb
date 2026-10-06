<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB Test Task Backlog

<!-- DO NOT DELETE THIS FILE — it is a persistent backlog, not a per-task draft. -->

**Override:** This file is **persistent** — it is not deleted after the
requirement (R9) is complete. Only completed tasks are removed; the file
itself remains as the ongoing test task backlog. This overrides the
`/implement-requirement` workflow's cleanup step which would normally delete
`plan-<topic>.md`.

Unfinished test tasks, grouped by layer. Each task has a checkbox for tracking.
For test strategy, layer scope, and coverage details, see [`design/kv/design-crowdb-kv-test.md`](../design/kv/design-crowdb-kv-test.md).

## Current CI Test Design

Local and CI tests use the same component tasks from `pixi.toml`. Each component
script owns its package list and execution order. CI runs ten parallel jobs;
manual workflows cover the container image and the Iceberg SDK matrix.

| Job          | Pixi task(s)                         | Coverage                                      |
| ------------ | ------------------------------------ | --------------------------------------------- |
| Lint         | checks, fmt, clippy                  | Formatting, lints and CI task reachability    |
| CppTests     | `test-cpp`                           | C++ and Rust FFI                              |
| UnitTests    | `test-core`                          | Core Rust libraries                           |
| StorageTests | `test-storage`                       | Native storage, services and streams          |
| AccessTests  | `test-access`                        | Access services, dataset and monitor          |
| S3E2E        | `-e s3-e2e test-boto3-e2e`           | S3 access and boto3 acceptance                |
| IcebergE2E   | `-e iceberg-e2e test-iceberg-e2e`    | PyIceberg, native storage and recovery        |
| IcebergSDK   | `-e iceberg-e2e test-iceberg-sdk`    | Java SDK and Apache RCK                       |
| ConsoleTests | `test-console`                       | Console shared, CLI and web server tests      |
| UITests      | `test-console-ui`                    | Vitest and real-backend Playwright            |

`test-suite` runs these component tasks sequentially for local full-suite
verification. It also runs the Rust Iceberg SDK task because that task is
manual-only in CI. The component tasks can be run independently, for example:

```sh
pixi run test-core
pixi run test-storage
pixi run test-access
pixi run test-console
pixi run test-console-ui
```

Component scripts build the service binaries they need, so a component does not
depend on artifacts left by another task. Subprocess suites clean disposable
runtime state before starting. Iceberg tasks use the pinned `iceberg-e2e`
environment for Python, Maven and Java.

`IcebergSDK` and `DockerPreview` are manual workflows. `DockerPreview` runs
`pixi run test-single-node-container` on a Linux amd64 Docker host. The release
workflow runs the same image checks before publication.

### CI component check

`pixi run check-ci-test-tasks` validates every workspace package against
`COMPONENT_PACKAGES` in `tools/ci-checks/check-ci-test-tasks.py`. It follows the
component task calls in Pixi and the checked-in workflow scripts, so a package
that is not covered by a CI-reachable component fails validation. It also checks
that the manual-only container and Iceberg SDK tasks remain reachable from their
workflows and absent from regular CI.

### Adding tests

1. Add package tests under the owning crate's `tests/` directory.
2. If a package is new, add it to the appropriate component script and to
   `COMPONENT_PACKAGES` in `tools/ci-checks/check-ci-test-tasks.py`.
3. Add explicit selectors for feature-gated or ignored tests. Compiling an
   ignored test does not count as executing it; exclude subprocess helper entries.
4. Run `pixi run check-ci-test-tasks`, the affected component task and workflow
   validation.
5. For a timing update, run
   `pixi run bash tools/test-metrics/measure.sh test-core test-storage` (replace
   the tasks as needed). Store the generated artifacts under
   `.crowdb-runtime/artifacts/measure-tests/`.

## Suite Timing

Timing measurements are recorded by component task. The old package-level table
was removed when Pixi tasks were consolidated; historical package rows are not
invocable tasks anymore and must not be used as current CI expectations. New
measurements should record the exact component task, environment, date, test
count and exit code. Timing includes incremental builds and subprocess
startup/shutdown, so cold builds and feature changes can affect the result.

| Component task       | Environment   | Date | Tests | Time | Status |
| -------------------- | ------------- | ---- | ----- | ---- | ------ |
| `test-cpp`           | default       | —    | —     | —    | ⏳     |
| `test-core`          | default       | —    | —     | —    | ⏳     |
| `test-storage`       | default       | —    | —     | —    | ⏳     |
| `test-access`        | default       | —    | —     | —    | ⏳     |
| `test-console`       | default       | —    | —     | —    | ⏳     |
| `test-console-ui`    | default + iceberg-e2e | — | — | — | ⏳ |
| `test-boto3-e2e`      | s3-e2e        | —    | —     | —    | ⏳     |
| `test-iceberg-e2e`    | iceberg-e2e   | —    | —     | —    | ⏳     |
| `test-iceberg-sdk`    | iceberg-e2e   | —    | —     | —    | ⏳     |

The measurement helper writes logs and aggregate results below
`.crowdb-runtime/artifacts/measure-tests/`. Keep slow individual-test notes next
to the component measurement that produced them.

---

## WAL Subsystem

Source: `lib/crowdb-kv/src/wal/`. Tests: 12 files, ~92 tests.

- [ ] **WAL disk-loss recovery (full fail-out)**: the full fail-out procedure
  (step-out RPC + reconfiguration, `design-crowdb-kv-wal.md` §8.1) is not yet
  implemented. The test should verify the node fails out of the group and
  rejoins via snapshot install after the disk is replaced. **Blocked** on the
  fail-out feature landing.

## Store

Source: `lib/crowdb-kv/src/store/`. Tests: 8 files, 26 tests.

- [ ] **Per-group WAL disk isolation**: `WalConfig.wal_disks` is per-`WalEngine`,
  not per-group within a store — the server startup path
  (`create_group_with_wal`) derives `wal_disks` from the store-level config, so
  groups cannot be assigned different physical disks. **Blocked** on a
  store-level config change to support per-group `wal_disks` override.

## Deployment

Source: `app/crowdb-kv-server/`. Tests: 9 files.

- [ ] **Network partition between processes**: verify cluster behavior when
  network connectivity between processes is severed and restored. **Blocked**:
  no network partition simulation infrastructure exists in the testkit.
  Needs a partition/drop mechanism (e.g. a proxy layer or toxiproxy-style
  interceptor) before the test can be written.
