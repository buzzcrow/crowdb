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
acceptance cases, full engine matrices, requirement-closure audits or physical GC.

Verified lifecycle implementation checkpoint (2026-09-24):

- Logical table drop, durable pending purge proof tasks and same/cross-namespace
  rename now use bounded journals and one exact head CAS. Conditional cleanup and
  terminal replay preserve recreated names; no file traversal or physical deletion.
- Writer-only DELETE/rename REST routes, standard empty 204 responses, exact
  request binding and a third background-recovery journal sweep are connected.
- Library tests cover every successful-path durable reply loss, delayed head-CAS
  replies, destination namespace drop, recreation before/after recovery, retired
  contexts, commit/lifecycle arbitration and recovery without client retry.
- HTTP tests cover permissions, replay, errors, metadata/location preservation and
  commits after a cross-namespace move. Official Java SDK exercises rename/drop,
  ordinary native Parquet reads and access-listener restart. Existing grants retain
  their lifetime; deleted names cannot obtain fresh credentials. Physical purge is
  deferred even after logical success.
- Full Iceberg library/server suites, fmt, workspace clippy and explicit
  Iceberg-E2E feature clippy pass. No unsafe exception, runtime lock, timeout
  increase, assertion reduction or test-side retry was introduced.

## Remaining tasks in dependency order

- [ ] **Namespace acceptance — R179**: complete the rename-in versus
  namespace-drop acceptance audit; its library race seam is now covered. Official PyIceberg CRUD
  now passes on two listeners before/after native storage and listener restart;
  the separate 500-ms clear/restart fixture also passes. Table-create
  admission already has fault/race coverage; do not reimplement it.
  Files: namespace modules, `iceberg_full_stack_test.rs`,
  [namespace execution plan](plan-iceberg-namespace.md).
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
  Library commit/drop/rename fence arbitration is covered; extend native crash
  interruption evidence rather than reimplementing those fences.
  Files: commit tests, `iceberg_file_http_test.rs`, native fault harness.
- [ ] **Release conformance — R184**: run the Apache REST Compatibility Kit,
  and official Rust client. Engine acceptance is deferred to the separate testing
  project in Next, not part of the current implementation phase. Include row-level deletes,
  defaults, lineage, statistics, time travel, expiry and table lifecycle.
  Produce a pinned executable capability matrix; untested profiles stay pending.
  Files: conformance environments, SDK fixtures and capability tests.
- [ ] **Requirement closure**: compare each requirement's acceptance cases with
  executable evidence; update affected permanent architecture only as needed.
  Remove each completed requirement/index entry and its plan together.
  The full R177/R184 milestone remains open while GC acceptance is deferred.

## Next — Separate engine testing project

- [ ] **Engine interoperability — deferred by user**: the user will create a
  separate testing project later. Do not start Spark, Flink or Trino tests now.
  Select and pin engine versions/deployment profiles when that project starts;
  no immediate first-engine decision is needed.
- Preserve the acceptance scope: create/evolve/write/commit/load, time travel,
  row-level deletes, rename/expire/drop, cross-engine results and server restarts.
  Reuse existing SDK/native evidence, but do not treat it as engine certification.
- Keep R184 engine acceptance pending until that project supplies executable
  results. Its project location and test commands are intentionally not invented.

## Human decisions

Only [R177 Open Questions](../backlog/R177-access-iceberg-catalog-foundation.md#open-questions)
is authoritative. No human decision is currently pending; implementation and
acceptance tasks remain open.

OI-1 is resolved: functionality and performance are separate acceptance tracks.
OI-2 is deferred by agreement to the user's later testing project, listed in Next.
OI-3 is resolved: provisioned disk capacity and chunk allocation failure provide
the capacity boundary, including configured limits for file-backed simulated
disks. R183 owns remaining GC/full-capacity recovery requirements;
no separate Iceberg quota or pre-full stop threshold is required.
Fix evidence-backed obvious performance bugs; record architectural optimization
work below for a consolidated backlog after functional implementation. Never
trade away durability, fencing, bounds or assertions for a passing timing result.

## Performance work to consolidate later

- SDK diagnostic: the first expanded in-memory Java lifecycle run returned 503
  at purge on 2026-09-24. One instrumented rerun and two fixed diagnostic batches
  (five and ten runs) passed without changing timeouts, adding retries or suppressing
  assertions. No server diagnostic was captured for the original failure; its root
  cause remains unconfirmed. Keep this as a follow-up observation, not a fixed bug
  or a reason to claim a stronger latency guarantee. Preserve the unchanged SDK
  command and capture request-admission/deadline diagnostics if it recurs.

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
  Existing disk allocation fails when eligible capacity cannot create new chunks.
  Keep failure bounded and retain committed authority/recovery evidence. R183
  tracks full-capacity acceptance; do not claim automatic space reclamation.
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
