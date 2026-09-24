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
- [x] **Partition-statistics integer prerequisite**: extend canonical Parquet
  column decoding to INT32 for spec IDs and file/DV counts: plain, dictionary,
  delta and byte-stream-split, including signed overflow and resource boundaries.
  Files: `file/parquet/pages.rs`, `pages/values.rs`, `values/delta.rs`, integer
  column tests. Keep the publication rejection until full validation exists.
- [x] **Nullable canonical pages**: implement bounded definition-level decoding,
  v1 level framing and v2 uncompressed levels/compressed value sections; validate
  exact value/null counts before yielding page values. Files: `file/parquet/pages/`,
  nullable-column fixtures and tests. Repeated columns remain unsupported here.
- [x] **Partition-statistics schema**: validate unified field IDs and types,
  version-dependent required statistics columns and the confirmed deleted-source
  omission. Reject conflicting retained specs and charge projection/schema work
  against the caller's aggregate budget. Files: `manifest/parquet/statistics.rs`,
  `statistics/projection.rs`, auxiliary integration and schema fixtures/tests.
- [x] **Remaining physical scalar decoding**: decode BOOLEAN, FLOAT, DOUBLE and
  fixed-length byte arrays under page limits, retaining floating-point bits.
  Support plain/dictionary, Boolean RLE and fixed delta/split encodings.
  Files: `file/parquet/pages/values/`, scalar fixtures/tests.
- [x] **Ordinary delete-rewrite boundary**: confirm writer/engine responsibility
  for row-set equivalence. Equality-delete replacement paths remain admissible
  while invalid equality IDs reject; expired ordinary position-delete removal
  does not require a replacement DV. Existing lost-DV and incomplete replacement
  rejection tests stay enabled. Files: `snapshot_validation/preservation.rs`,
  `snapshot_delete_preservation_test.rs`. These are catalog validation tests,
  not execution-engine compaction or row-equivalence acceptance.
- [x] **Selected auxiliary semantics**: reconcile statistics with selected manifest
  inventories, preserve unknown optional values and deleted-source omissions, and
  reuse accepted immutable references only through selected prior provenance.
  Ordinary and staged publication now validate statistics instead of returning
  the provisional 406. Real Java SDK publication/replay, schema/spec evolution,
  v2-to-v3 upgrade, staged creation and native listener restart acceptance pass.
  Files: `commit/files/auxiliary/`, `manifest/parquet/statistics/`,
  `TestIcebergPartitionStatistics.java`.
- [~] **Publication fault acceptance**: exercise native process interruption at
  candidate and head publication; cover create/staged operation boundaries,
  exact identity recovery on another listener, changed-input conflicts and
  unreachable losing candidates. Existing in-memory reply-loss tests and
  successful restart fixtures do not satisfy this matrix.
  - Use a test-executable child listener with native routed KV and native file
    blocks. Keep fault injection entirely in `tests/common/`, wrapping the existing
    storage traits rather than adding production environment switches.
  - Enumerate request-local durable CAS and block writes in a successful baseline;
    pause immediately before and after each boundary, notify the parent, then kill
    the child process. Repeat for create, stage, staged publication and update.
  - Retry the original identity/body through a separate listener and assert exact
    final response replay, single visible generation and changed-input conflicts.
    Verify stage invisibility and recoverable durable name reservations.
  - Pause before candidate head selection, publish a competitor, kill the paused
    listener and verify durable conflict replay plus unreachable candidate files.
    Test response loss after final journal persistence independently of head CAS.
- [x] **Remaining SDK error cases**: add deterministic head-CAS loss and disabled
  selected-operation errors through the official client. Failed requirements and
  post-drop/recreated-name checks do not substitute for publication races.
  First pause a real update immediately before its head CAS, publish a competing
  HTTP update, then release the SDK request. Check Publishing/Rejected journal
  phases, same retained input, exact conflict replay and unreachable candidate.
  Files: test-only store, `iceberg_commit_sdk_test.rs`, `TestIcebergCommitRace.java`.
  The provisional unsupported-statistics fixture is now an unavailable-file
  rejection fixture (400) with unchanged canonical head; valid files are covered
  by native SDK publication tests.
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

## Verified implementation summary

- Library tests cover ordered metadata updates, retained generations, schema/spec/
  sort evolution, v1/v2/v3 defaults and lineage, direct upgrades, name mapping,
  immutable candidates, deterministic CAS conflicts and exact durable replay.
- Delete validation preserves ordinary writer-owned rewrite semantics while
  retaining file authority, IDs/sequence/partition, position bounds and DV merge/
  preservation checks. No Catalog row-set equivalence evaluator was introduced.
- Statistics validation covers physical scalar decoding, typed NULL-FIRST tuple
  ordering, transform/spec membership, Unicode/decimal/time/UUID/NaN normalization,
  count consistency and selected-manifest inventory reconciliation. Optional
  unknown counters are not invented; historical zero-count partitions remain
  compatible with the SDK. Deleted-source projection collisions aggregate counts.
- Already accepted statistics survive evolution only with exact prior-head,
  snapshot, location and size binding. Canonical authority, lengths and digest
  verification still run; copied or changed references receive full validation.
- Native SDK acceptance exposed over-reservation for wide statistics. Readers now
  divide the unchanged aggregate budget into enforced per-column page allowances,
  scratch/header reserve and checked tuple retention. Neither the 64-MiB cap nor
  configured page ceiling is increased. Two-/ten-field regressions pass; tiny
  aggregate and page budgets still reject.
- Official Java 1.11.0 uses native S3FileIO and Parquet statistics, publishes v2,
  replays identical typed requests with UUIDv7 identity, evolves schema/specs,
  upgrades to v3 with retained statistics, computes new v3 statistics and publishes
  a staged table containing statistics. Both tables are read after listener restart.
- The SDK fixture includes Parquet Hadoop and Hadoop MapReduce reader dependencies.
  Populate runtime artifacts with Maven `dependency:resolve -DincludeScope=runtime`
  before offline E2E execution. No client-side workaround, retry loop or production
  timeout increase was added.
- Use an isolated ephemeral runtime root for native suites: unrelated preserved
  persistent port claims in the default root can violate the test allocator's
  fixed ChunkDB listen/RPC offset. Do not delete persistent user runtime state.
- Latest native command: isolated runtime root plus
  `cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_file_http_test official_java_catalog_commits_native_parquet_snapshots_and_staged_tables -- --ignored --nocapture`,
  via Pixi with the pinned Java/Maven environment. Publication and restart pass.
- Latest focused coverage: statistics rows (13), inventory (7), retained files (3);
  four official table SDK tests pass. Full library/server and lint gates are
  rerun before each coherent commit. Native interruption acceptance remains
  independent and is not claimed by ordinary successful restart tests.
