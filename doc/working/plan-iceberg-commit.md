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
- [x] **Remaining SDK error cases**: add deterministic head-CAS loss and disabled
  selected-operation errors through the official client. Failed requirements and
  post-drop/recreated-name checks do not substitute for publication races.
  First pause a real update immediately before its head CAS, publish a competing
  HTTP update, then release the SDK request. Check Publishing/Rejected journal
  phases, same retained input, exact conflict replay and unreachable candidate.
  Files: test-only store, `iceberg_commit_sdk_test.rs`, `TestIcebergCommitRace.java`.
  Disabled partition-statistics uses a typed SDK update and metadata-only fixture
  references to prove the pre-file-validation 406 gate; it does not validate a
  real statistics file. Replace this rejection fixture when support is enabled.
- [ ] **Closure audit**: map every R182 acceptance case to executed verification;
  retain unsupported shared dependencies until implemented, then close R182.

## Files and verification

- SDK fixture: `app/crowdb-access-server/tests/common/iceberg_java/src/main/java/TestIcebergCommitErrors.java`.
- SDK runner: `app/crowdb-access-server/tests/iceberg_table_sdk_test.rs`.
- CAS race: `app/crowdb-access-server/tests/iceberg_commit_sdk_test.rs`,
  `tests/common/iceberg_store.rs` and Java `TestIcebergCommitRace.java`.
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

## CAS and disabled-operation checkpoint

- The real publisher is paused immediately before the storage head CAS, after
  reaching Publishing with written candidate metadata. Another HTTP commit
  publishes first. Both journals retain the identical input head and target
  generation; the SDK loser reaches Rejected with 409 CommitFailedException.
- Same-key replay returns the identical error body; changed input conflicts.
  Exactly two commit journals remain, with no rebase/new commit from either
  replay. Load selects the winner, list exposes one table, and loser-only
  properties never become visible. This uses the in-memory CAS implementation,
  not native multi-process failure injection.
- The disabled selected partition-statistics gate returns HTTP/wire 406 and
  UnsupportedOperationException; the pinned SDK maps it to RESTException.
  Earlier property/snapshot updates in that batch leave the head unchanged.
- Five Java SDK tests, server Iceberg-enabled all-targets, fmt, workspace clippy
  and explicit E2E-feature clippy pass. No production code or limits changed.
