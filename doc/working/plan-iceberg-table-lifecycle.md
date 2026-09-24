# Iceberg Table Lifecycle Acceptance Plan

Upstream: [R181](../backlog/R181-access-iceberg-table-lifecycle.md).
Coordination: [functional catalog plan](plan-iceberg-functional-catalog.md).

Goal: close table lifecycle acceptance before commit and FileIO closure, without
claiming deferred selected-file validation, projections or physical purge.

## Execution

- [x] **Read acceptance**: add HTTP generation-change interleavings for ALL/REFS
  and conditional requests; cover mixed stale/reserved/tombstoned/current table
  mappings across pages and HEAD. Existing metadata unit tests and static SDK
  representations remain the version-fidelity evidence. Files: server
  `iceberg_table_acceptance_test.rs`, `tests/common/iceberg_store.rs`.
- [x] **Lifecycle acceptance**: strengthen interrupted rename visibility checks,
  inject lost tombstone publication replies through HTTP for both purge modes,
  and verify unsupported calls preserve authority. Files: library
  `table_lifecycle_test.rs`, server acceptance tests and store helper.
- [~] **Gates and closure**: run library/server all-targets, official Java read
  and lifecycle tests, native Java lifecycle/restart, fmt and clippy. Map seven
  acceptance bullets to tests, then delete requirement/index/plan together.

## Evidence inventory

New evidence: all four server acceptance tests and the seven-test library
lifecycle suite pass. ALL/REFS conditional/nonconditional reads pause at the
selected FileRecord while a real commit advances the head; the old read fails
without metadata or a false 304. Mixed five-page index tests filter stale aliases,
reservations, absent heads and tombstones with exact scan counts. Lost drop head
CAS replies replay to 204 in both purge modes, preserve files, and produce exactly
zero/one durable purge proof task. Unsupported routes preserve all file/table
authority. Every interrupted same/cross-namespace rename runs concurrent list/load
checks before and after recovery, preserving canonical bytes and one visible name.

The initial tombstone fixture failed record validation because it omitted its
required pending operation; corrected the fixture, not production validation.
Clippy's similar-name findings were fixed by renaming local test bindings.

Fresh verification passes: library and Iceberg-enabled server all-targets, all
three official Java SDK tests (9.51 seconds), native Java Parquet/lifecycle and
listener-restart acceptance (99.23 seconds), fmt, workspace clippy and explicit
Iceberg-E2E feature clippy. Native Maven retains its previously recorded SLF4J and
shutdown-thread warnings; assertions and timeouts are unchanged. No production
behavior change was needed to satisfy this acceptance audit.

- Metadata: `table_metadata_*_test.rs`, `metadata_transition_test.rs`,
  `table_load_test.rs`; canonical bytes and unknown fields preserved, versions
  validated. Selected partition-statistics and delete-rewrite gaps stay R180/R182.
- Loads: server `iceberg_table_http_test.rs`, Java `TestIcebergCatalogReads`;
  generation races previously exercised only at library level.
- Rename/drop: `table_lifecycle_test.rs`, `table_lifecycle_race_test.rs`, server
  `iceberg_table_lifecycle_test.rs`, Java `TestIcebergCatalogWrites`.
- Destination fences: every interrupted rename phase, delayed CAS, source and
  destination recreation are already covered. Do not reimplement these machines.
- Listing: `table_list_test.rs` covers stale pages, budgets and token identity;
  mixed mapping states need explicit combined list/exists evidence.
- Purge: durable proof task only; library checks no block reads/writes. HTTP
  replay exists but lost head-publication reply needs direct coverage.
- Unsupported: existing 406 checks need exact authority preservation assertions.

## Verification

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`.
- `pixi run clean-env && pixi run -- cargo test -p crowdb-access-server --features iceberg --all-targets`.
- `pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_table_sdk_test -- --ignored --nocapture --test-threads=1`.
- `pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_file_http_test official_java_catalog_commits -- --ignored --nocapture`.
- SDK/native environment and isolated runtime roots follow the functional plan.
- `pixi run -- cargo fmt --all -- --check`; `pixi run rs-lint`;
  `pixi run -- cargo clippy -p crowdb-access-server --features iceberg-e2e --all-targets -- -D warnings`.
