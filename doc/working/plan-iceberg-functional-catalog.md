# Iceberg Functional Catalog Plan

Upstream: [R177](../backlog/R177-access-iceberg-catalog-foundation.md),
[R179](../backlog/R179-access-iceberg-namespace.md),
[R180](../backlog/R180-access-iceberg-fileio.md),
[R181](../backlog/R181-access-iceberg-table-lifecycle.md),
[R182](../backlog/R182-access-iceberg-table-commit.md),
[R184](../backlog/R184-access-iceberg-rest-conformance.md).

Goal: finish the native functional catalog without confusing working vertical
slices with complete specification and release acceptance.

Persistent-plan exception: this coordinates several requirements. Keep a short
verified summary, remove completed execution tasks, and delete this plan only
after the program finishes. Human decisions live only in R177. No user-guide work.

## Completed summary

Verified integration checkpoint: `a832e699` (2026-09-24).

- Independent writer credentials; namespace CRUD, bounded listing, durable
  retries, parent admission, restart recovery and stale-index repair.
- Native immutable FileIO, SigV4 delegation, bounded streaming/ranges, durable
  multipart, XML responses/checksums and background recovery.
- Bounded metadata/manifest/Parquet/Puffin/DV validation; partition and Variant
  bounds; generation-bound provenance and direct-parent delete preservation.
- Ordered updates, confirmed direct v1-to-v3 upgrades and SDK-safe name mapping;
  immediate/staged create, immutable candidate publication, one head CAS,
  deterministic conflicts, exact retry and bounded recovery.
- Runtime table reads/create/commit/credentials. Draft grants bind exact identity
  and original writer; response headroom and configuration-aware ETags are checked.
- 527 library tests, 58 Iceberg-enabled server tests, three Java SDK tests,
  native Parquet/staged/upgrade/restart acceptance, fmt and clippy pass.
  Real Java 1.11.0 writes v1 data, upgrades to v3, appends with retained history,
  publishes a staged table, and reads both after catalog-process restart.

This does not close R179–R184. Existing tests do not substitute for unexecuted
acceptance cases, full engine matrices, lifecycle operations or physical GC.

## Remaining tasks in dependency order

- [ ] **Namespace acceptance — R179**: complete the future rename-in versus
  namespace-drop seam and remaining acceptance audit. Official PyIceberg CRUD
  now passes on two listeners before/after native storage and listener restart;
  the separate 500-ms clear/restart fixture also passes. Table-create
  admission already has fault/race coverage; do not reimplement it.
  Files: namespace modules, `iceberg_full_stack_test.rs`,
  [namespace execution plan](plan-iceberg-namespace.md).
- [ ] **Logical table drop — R181**: journal tombstoning and visibility removal;
  preserve response-loss replay, recreated-name safety and all file authority.
  Persist a pending purge proof task for purge requests, never report physical
  deletion complete. Compose REST admission and background recovery.
  Files: library `table/`, `operation/`, `record/`; server `iceberg/`; tests.
- [ ] **Same/cross-namespace rename — R181**: reserve destination before parent
  admission; arbitrate head/name-epoch publication, settle the old mapping, and
  recover every crash boundary. Old names are not aliases. Race destination
  namespace drop and subsequent name recreation against rename-in.
  Files: table lifecycle and namespace helping/probes, server routes, tests.
- [ ] **Selected-use gaps — R180/R182**: implement partition-statistics schema,
  ordered-row and count validation before removing its explicit rejection.
  Audit equality-delete rewrites, position-delete removal without replacement DV,
  retained history and aggregate admission against the declared profile. Existing
  DV replacement validation alone does not prove all delete rewrites.
  Preserve explicit rejection for encrypted data and unsupported selected formats;
  encryption-key metadata parsing is not encrypted-file support.
  Files: `commit/proof.rs`, auxiliary/snapshot validators and SDK fixtures.
- [ ] **Projection integration — R180**: connect generation-local projection
  publication/loading only with equivalent authority/validation checks. Current
  canonical-only table loading is correct; the tested projection helper is not a
  production fast path. Missing/partial/corrupt projections remain optional and
  fall back to exact canonical bytes. No cross-generation deduplication.
  Files: `metadata_projection/`, `table/load.rs`, commit integration.
- [ ] **FileIO acceptance closure — R180**: audit remaining cross-instance
  multipart crash/response-loss cases, official data/equality-delete uploads
  through identical ordinary S3 requests, timed native credential refresh and
  independent resource-budget intersections. Reuse existing state machines.
  Files: [FileIO execution plan](plan-iceberg-fileio.md), native/SDK fixtures.
- [ ] **REST/capability consistency — R184**: reconcile persisted format flags,
  currently foundation-default config overrides and actually installed routes.
  Cover supported/unsupported combinations, precise errors, data-access/prefix/
  snapshot/purge parameters, retired retries and credential lifecycle races.
  Add bounded protocol metrics without credentials or high-cardinality labels.
  Files: `catalog/capability.rs`, `wire/config.rs`, server `iceberg/`, tests.
- [ ] **Commit acceptance closure — R182**: extend official-client and
  multi-process fault coverage to every declared create/commit/error/limit case;
  test candidate/head publication interruption, not just a completed-table
  process restart. Compose new rename/drop fences without introducing a second
  publisher or rebasing an uncertain operation.
  Files: commit tests, `iceberg_file_http_test.rs`, native fault harness.
- [ ] **Release conformance — R184**: run the Apache REST Compatibility Kit,
  official Rust client and R177 OI-2 engine profiles. Include row-level deletes,
  defaults, lineage, statistics, time travel, expiry and table lifecycle.
  Produce a pinned executable capability matrix; untested profiles stay pending.
  Files: conformance environments, SDK/engine fixtures and capability tests.
- [ ] **Requirement closure**: compare each requirement's acceptance cases with
  executable evidence; update affected permanent architecture only as needed.
  Remove each completed requirement/index entry and its plan together.
  The full R177/R184 milestone remains open while GC acceptance is deferred.

## Human decisions

Only [R177 Open Questions](../backlog/R177-access-iceberg-catalog-foundation.md#open-questions)
is authoritative:

- OI-2: first release engine/version/deployment matrix.
- OI-3: capacity and write-stop policy before physical GC.

These are not missing implementations. Continue tasks independent of a pending
decision; do not infer approval from an existing runtime default or passing test.

OI-1 is resolved: functionality and performance are separate acceptance tracks.
Fix evidence-backed obvious performance bugs; record architectural optimization
work below for a consolidated backlog after functional implementation. Never
trade away durability, fencing, bounds or assertions for a passing timing result.

## Performance work to consolidate later

- Historical namespace diagnostics measured roughly 45–75 ms per durable phase
  and intermittent failure under a 500-ms total bound. Refresh measurements before
  attributing current cost to any component; these are not current p95/p99 values.
- Profile journal/retry-ledger round trips and durable payload/checkpoint writes
  on identical storage, concurrency and data. Prior redundant writes already
  received no-op/read-before-put fixes; do not reimplement them blindly.
- Consider batching or pipeline changes only with measured evidence and preserved
  publication/clear/replay invariants. Record before/after latency, KV round trips,
  storage I/O, CPU and memory alongside failure-injection regression results.
- Create one consolidated optimization backlog later. No new performance backlog
  or latency guarantee is introduced by the functional-test split itself.

## Deferred work and safety boundaries

- R183 physical GC stays deferred. Clear, drop, expiry, abort and CAS loss may
  remove logical visibility but never authorize physical deletion by TTL alone.
  Retain ownership, generations, purge intent and recovery evidence.
- R186 owns selected ORC validation. Container probing/upload is not selection
  support; the initial selected data/delete profile remains plaintext Parquet.
- R185 decoded-cache optimization is outside this milestone.
- Active request/session limits do not bound cumulative retained orphan storage.
  Until OI-3 is settled, do not claim unattended sustained-write safety.
- New runtime catalogs persist five-minute requests and fifteen-minute delegation.
  Restart cannot widen legacy bounds. Explicit clear can expand them under the
  full maintenance grace; legacy zero-delegation catalogs require a subsequent
  listener restart to enable table routes. Never clear user state to run a test.

## Verification and execution notes

- Library: `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`.
- HTTP: `pixi run clean-env && pixi run -- cargo test -p crowdb-access-server --features iceberg --all-targets`.
  Default server tests alone skip the Iceberg suites.
- SDK: `pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_table_sdk_test -- --ignored --nocapture --test-threads=1`.
- Native: `pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_file_http_test official_java_catalog_commits -- --ignored --nocapture`.
- For Java tests, use default Pixi for Cargo; set
  `JAVA_HOME=$PWD/.pixi/envs/iceberg-e2e/lib/jvm` and
  `CROWDB_ICEBERG_E2E_MVN=$PWD/.pixi/envs/iceberg-e2e/bin/mvn`.
- Prefix native tests with clean-env using the same isolated
  `CROWDB_RUNTIME_ROOT=$PWD/.crowdb-runtime/ephemeral/iceberg-catalog-e2e`;
  preserve unrelated persistent port claims. Do not clean while another test runs.
- Gates: `pixi run -- cargo fmt --all -- --check`, `pixi run rs-lint`, and
  `pixi run -- cargo clippy -p crowdb-access-server --features iceberg-e2e --all-targets -- -D warnings`.
- Long native/full-suite commands run in the background and are polled, rather
  than being mistaken for failures at the default sixty-second shell cutoff.
- Maven SDK shutdown-thread/logging warnings are nonfatal in the passing native
  fixture. Pinned SDK dependency order must precede Hadoop's older transitives.
