<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Access server acceptance

Protocol authority: [native Iceberg design](../../../doc/design/access-server/iceberge/design-crowdb-iceberg.md).
Client recipes: [container development](../../../container/single-node-container/README.md).

- REST/official-SDK conformance, native durability and ecosystem compatibility
  are distinct evidence. Passing SDK tests cannot certify compute engines.
- The [manual ecosystem fixture](../../../container/single-node-container/tests/ecosystem/README.md)
  uses a separate locked environment and workflow dispatch. Its matrix records
  only actual pinned client operations against the container and a volume restart.
- [S3 language SDKs](common/s3_sdks/README.md) and the Rust Iceberg SDK retain
  independent manual workflows. They are not default test dependencies.
- Selected ORC and optional caches remain independent backlog ownership under
  [ORC validation](../../../doc/backlog/R186-access-iceberg-orc-validation.md)
  and [cache optimization](../../../doc/backlog/R185-access-iceberg-cache-invalidation.md).
  The functional fixtures select Parquet and establish no cache speed guarantee.
- Physical GC remains an independent opt-in runtime contract. Logical drop,
  clear, expiry and CAS loss do not authorize physical deletion by TTL alone.
  Provisioned allocation capacity bounds storage; cumulative orphan storage
  is not bounded by active request limits.

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
