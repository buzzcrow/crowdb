# Iceberg Functional Catalog Plan

Upstream: [R177](../backlog/R177-access-iceberg-catalog-foundation.md),
[R184](../backlog/R184-access-iceberg-rest-conformance.md).

Goal: finish the native functional catalog without confusing working vertical
slices with complete specification and release acceptance.

Persistent-plan exception: this coordinates several requirements. Keep a short
verified summary, remove completed execution tasks, and delete this plan only
after the program finishes. Human decisions live only in R177. No user-guide work.

## Completed summary

- R179 namespace and R181 table lifecycle acceptance are closed, with independent
  writer credentials, bounded listing, rename/drop fencing, durable replay and
  native/official-client recovery evidence. Implementations: `442f26c7`,
  `4bbc2226`.
- R182 atomic commits are closed: `af4ae819`, cleanup `0fef46c0`. Native process
  kills cover 182 before/after durable-write cases across create, stage, publish
  and update; independent listeners preserve exact replay and one visible head.
  Head-CAS loser, bounded admission, statistics evolution and official SDK
  publication/restart pass. Ordinary rewrite row-set equivalence stays engine-owned.
- R180 FileIO is closed: `0a848834`. Immutable streaming/range files, durable
  multipart, delegated credentials and validated generation-local REFS projections
  pass acceptance. Corrupt/partial projections fall back to canonical JSON;
  ALL and commit admission still parse canonical metadata.
- Native PUT/Complete process kills pass all 44 cases twice. Background recovery
  settles credits without extra Complete requests. Native credential lifecycle,
  official Java FileIO/selected data-delete/catalog fixtures and Chunk-KV restart
  pass; this is not an all-service DiskIO restart or physical-GC claim.
- Evidence-backed FileIO fixes align bounded copy windows, avoid competing active
  recovery, duplicate JSON scans and repeated directory reads, and overlap one
  frame of copy I/O. The unchanged 5-MiB raw multipart fixture passes three runs;
  no request timeout, caller retry, authority check or durability gate was relaxed.
- Final gates: 616 library tests, 70 Iceberg-enabled server tests, default server
  tests, 14 no-default transport tests, fmt, workspace lint and explicit
  Iceberg-E2E clippy pass. Existing Maven warnings remain visible. Only the Pixi
  toolchain was verified; locked LZ4 dependencies exceed the declared Rust 1.75
  MSRV, so Rust 1.75 compatibility is not claimed.
- ORC, physical GC and broad engine/performance acceptance remain separately
  scoped below. Unconfirmed diagnostic deadlines are retained as observations,
  not claimed fixes or pending human design choices.

## Remaining tasks in dependency order

R179–R182 are complete. Continue foreground R184; R183 and R186 stay deferred.
- [ ] **REST/capability consistency — R184**: reconcile persisted format flags,
  currently foundation-default config overrides and actually installed routes.
  Cover supported/unsupported combinations, precise errors, data-access/prefix/
  snapshot/purge parameters, retired retries and credential lifecycle races.
  Add bounded protocol metrics without credentials or high-cardinality labels.
  Files: `catalog/capability.rs`, `wire/config.rs`, server `iceberg/`, tests.
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

- A native fault-matrix diagnostic run returned `Store(Client(Deadline))` from
  the independent verification client's first file-record load, after HTTP replay
  succeeded. No request timeout or caller retry was changed; two subsequent complete
  44-case runs passed. The cause of that one five-second client deadline remains
  unconfirmed. Capture fresh client routing/transport and backend timing if it
  recurs; do not describe it as fixed by FileIO scheduling changes.
- Native multipart diagnostics exposed unequal competing copy windows, tiny
  checkpoint-only leaves, repeated directory reads and duplicate JSON digest
  passes. These targeted costs are removed. One-frame assembly overlap preserves
  checkpoint/replay/error invariants. Broader batching, shared decoded caches,
  sustained throughput and recovery-page scaling remain measurement work, not
  implied guarantees from the original-bound functional fixture passing.

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
- Full namespace CRUD uses the bounded 300,000-ms functional profile with
  delegation disabled; raw HTTP client timeouts remain five seconds. The separate
  maintenance fixture retains 500 ms. Their passing results are not evidence that
  every namespace mutation meets 500 ms. Earlier redundant immutable-payload and
  terminal-cleanup writes were fixed without altering publication CAS.
- Maintenance fixture repair errors for synthetic reserved name mappings without
  journals are expected from `verify_name_index`; retain the diagnostics rather
  than interpreting them as production corruption or suppressing them.
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
- Namespace SDK: `pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_namespace_sdk_test -- --ignored --nocapture`.
  Set `CROWDB_ICEBERG_E2E_PYTHON=$PWD/.pixi/envs/iceberg-e2e/bin/python` and the
  Java environment below. The pinned PyIceberg method requests complete lists;
  Java RESTCatalog implements token continuation. Neither client is patched.
- Native namespace: `pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_full_stack_test -- --nocapture`.
  Use the same Python variable and an isolated cleaned runtime root as below.
- Native SDK: `pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_file_http_test official_java_ -- --ignored --nocapture --test-threads=1`.
- Native FileIO faults/lifecycle: `pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_file_http_test native_file_ -- --ignored --nocapture --test-threads=1`.
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
