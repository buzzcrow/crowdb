<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Iceberg Functional Catalog Plan

Upstream: [Native Iceberg Storage](../design/access-server/iceberge/design-crowdb-iceberg.md).

Goal: preserve follow-up ownership and performance observations after completion
of native catalog correctness and REST/official-SDK conformance.

Persistent-plan exception: this coordinates the remaining cache, ORC and engine
work. Remove completed execution tasks; delete this plan after the program ends.

## Completed summary

The core milestone is complete. Native fault/restart acceptance, official Java
1.11.0 and Rust 0.10.0 clients, the six supported Apache RCK cases, route/version
admission, actual Parquet rows/deletes, upgrade/expiry/restart, and bounded logical
TiB traversal have executable evidence. The permanent design records the matrix
and exclusions. Full RCK, ORC and compute-engine certification are not claimed.

Closure gates on 2026-09-27 passed: complete Iceberg library suite, default and
Iceberg-E2E server all-targets (with the pinned Python environment), workspace
fmt/lint and Iceberg-E2E clippy. The expanded native Java catalog case passed
with v1/v2/v3 actual row reads and restart. A discovered post-drop FileIO pin
regression is fixed, with GC proof/fence/worker coverage. No timeout or retry
assertion was relaxed.

## Next — R189 container client/engine project

- [ ] **Client and engine interoperability — R189**: after R187 is
  publish-ready, test Python dataframe, local SQL, Spark, Flink and Trino
  workflows in the separate container project. Pin versions and profiles there;
  do not infer engine certification from the completed SDK acceptance.
- Preserve the acceptance scope: create/evolve/write/commit/load, time travel,
  row-level deletes, rename/expire/drop, cross-engine results and server restarts.
  Reuse existing SDK/native evidence, but do not treat it as engine certification.
- Keep R189 client/engine acceptance pending until that project supplies
  executable results. Its environment and commands are specified when built.

## Decisions and remaining ownership

The completed catalog contract and confirmed compatibility decisions live in
[Native Iceberg Storage](../design/access-server/iceberge/design-crowdb-iceberg.md).
No human decision remains for REST/official-SDK correctness. R189 owns the
separate engine project; R186 owns selected ORC, and R185 owns optional caches.
Provisioned disk capacity remains the allocation boundary. Functional acceptance
is separate from latency targets; preserve the observations below.

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

- R183 physical GC is separate and remains runtime opt-in. Clear, drop, expiry,
  abort and CAS loss may remove logical visibility but never authorize physical
  deletion by TTL alone. Retain ownership, generations, purge intent and recovery
  evidence.
- R186 owns selected ORC validation. Container probing/upload is not selection
  support; the initial selected data/delete profile remains plaintext Parquet.
- R185 decoded-cache optimization is outside this milestone.
- Active request/session limits do not bound cumulative retained orphan storage.
  Existing disk allocation fails when eligible capacity cannot create new chunks.
  Keep failure bounded and retain committed authority/recovery evidence. R183
  provides full-capacity acceptance; do not claim automatic space reclamation.
- New runtime catalogs persist five-minute requests and fifteen-minute delegation.
  Restart cannot widen legacy bounds. Explicit clear can expand them under the
  full maintenance grace; legacy zero-delegation catalogs require a subsequent
  listener restart to enable table routes. Never clear user state to run a test.

## Verification and execution notes

- Library: `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`.
- HTTP: `pixi run clean-env && pixi run -- cargo test -p crowdb-access-server --all-targets`.
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
