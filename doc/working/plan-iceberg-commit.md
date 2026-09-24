# Iceberg Commit Plan

Upstream: [R182](../backlog/R182-access-iceberg-table-commit.md),
[program plan](plan-iceberg-functional-catalog.md).

Goal: complete atomic commit acceptance without bypassing selected-file validation.

## Execution

- [x] **Official SDK errors and counts**: use pinned Java 1.11.0 typed requests
  and its commit error handler to verify requirement conflicts, stale schema,
  malformed updates, ordered rollback, lifecycle identity, and exact 1000/1001
  requirement/update limits plus 4096/4097 aggregate requirement-text bytes.
  Files: Java fixture and `iceberg_table_sdk_test.rs`.
- [ ] **Selected auxiliary semantics**: implement partition-statistics schema,
  ordered rows and counts before removing `UnsupportedPartitionStatistics`.
  Audit delete rewrites, retained history and aggregate bounds. Files:
  `lib/crowdb-access-iceberg/src/commit/files/auxiliary.rs`, `commit/proof.rs`,
  relevant Parquet readers and crate tests.
- [ ] **Publication fault acceptance**: exercise native process interruption at
  candidate and head publication; cover create/staged operation boundaries,
  exact identity recovery on another listener, changed-input conflicts and
  unreachable losing candidates. Existing in-memory reply-loss tests and
  successful restart fixtures do not satisfy this matrix.
- [ ] **Remaining SDK error cases**: add deterministic head-CAS loss and disabled
  selected-operation errors through the official client. Failed requirements and
  post-drop/recreated-name checks do not substitute for publication races.
- [ ] **Closure audit**: map every R182 acceptance case to executed verification;
  retain unsupported shared dependencies until implemented, then close R182.

## Files and verification

- SDK fixture: `app/crowdb-access-server/tests/common/iceberg_java/src/main/java/TestIcebergCommitErrors.java`.
- SDK runner: `app/crowdb-access-server/tests/iceberg_table_sdk_test.rs`.
- Unit/integration: Iceberg library all-targets; access-server default and
  Iceberg-enabled affected suites. Additional byte/work/admission limits remain
  to audit; SDK count tests alone do not prove leak-free admission.
- E2E: ignored Java SDK runner with `iceberg-e2e`; native fault matrix pending.
- Gates: `pixi run cargo fmt --all -- --check`, `pixi run rs-lint`, explicit
  access-server E2E-feature clippy. No user-guide or deferred engine-test work.

## Verified checkpoint

- All four official Java SDK tests pass together under default concurrency.
  New error checks inspect HTTP status, parsed wire error type, exact SDK exception
  class and unchanged canonical metadata location after every rejected live-table
  commit. The first update of a later-failing ordered batch remains invisible.
- The exact 1000-count case uses numeric schema requirements; 1000 UUID
  requirements correctly fail the independent 4096-byte aggregate text budget.
  Both count and text limits retain their original production values.
- Test setup initially inherited an invalid `/opt/jdk11` JAVA_HOME; use
  `JAVA_HOME=$PWD/.pixi/envs/iceberg-e2e/lib/jvm` and
  `CROWDB_ICEBERG_E2E_MVN=$PWD/.pixi/envs/iceberg-e2e/bin/mvn`.
  The checking handler delegates parsing and exception mapping to the official
  `ErrorHandler`; a plain Consumer receives the raw body instead of parsed fields.
- No production code, retry policy, timeouts or unsafe scope changed. Existing
  Maven SLF4J binding warnings remain visible and nonfatal.
- Ten HTTP table write/lifecycle acceptance tests, workspace fmt/clippy and
  explicit `iceberg-e2e` all-target clippy pass. Full native fault and engine
  matrices were not run or claimed by this checkpoint.
