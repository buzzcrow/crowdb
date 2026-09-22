<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Iceberg Foundation Plan

Upstream: [R177](../backlog/R177-access-iceberg-catalog-foundation.md),
[R178](../backlog/R178-access-iceberg-catalog-domain.md),
[R179](../backlog/R179-access-iceberg-namespace.md).

Goal: establish the catalog foundation before namespace operations, with durable
recovery and independently bounded protocol admission.

## Contract preparation

- [x] **Resolve review findings**: define namespace reserve-before-admit, complete
  unpaginated listing, maintenance and clear deadlines, system retry records, and
  independent namespace revisions. Files: `doc/backlog/R177-*` through `R179-*`,
  affected `R181-*`, `R182-*`, `R184-*`, and `R185-*`.

## Foundation

- [x] **Identity and keys**: add the workspace crate, nonzero random typed IDs,
  versioned system/catalog key scopes, strict decoding, range endpoints, bounded
  variable fields, and unsupported-by-default capability types. Files:
  `Cargo.toml`, `lib/crowdb-access-iceberg/Cargo.toml`, `src/lib.rs`, `src/key.rs`,
  `src/key/`, `src/catalog.rs`, `src/catalog/`, `src/error.rs`, `tests/`.
- [x] **Catalog records**: add versioned FlatBuffer root and authority records
  with bounded decoding, phase validation and key/identity matching.
  Keep domain and REST models separate. Files: `lib/crowdb-protocol/src/fbs/`,
  its generated-code integration, `lib/crowdb-access-iceberg/src/record/`.
  The generated-code-only unsafe exception was raised before implementation, as
  AGENTS.md requires. The rule requires disclosure, not a separate approval gate;
  continue with an isolated generated module and no hand-written unsafe.
- [x] **Operation records**: add management operation, audit and REST retry binding
  records using the versioned envelope. Files:
  `lib/crowdb-protocol/src/fbs/iceberg.fbs`,
  `lib/crowdb-access-iceberg/src/operation/`, `src/record/`.
- [x] **Storage adapter**: wrap routed Chunk-KV point CAS and scans, preserving
  typed outcomes and persisted request identities. Files:
  `lib/crowdb-access-iceberg/src/catalog/storage.rs` and integration tests.
- [x] **Management recovery**: implement initialize/status/rename/clear, durable
  management receipts, bounded audit, retained results, maintenance and persisted
  completion deadlines. Add crash and concurrent-operation tests. Files:
  `lib/crowdb-access-iceberg/src/catalog/repository.rs`, `src/operation/`.
- [x] **REST retry boundary**: implement optional UUIDv7 keys, principal/digest/domain
  bindings, retention and capacity admission, final 4xx replay, and non-final 5xx
  recovery. Files: `lib/crowdb-access-iceberg/src/operation/`, `src/wire/`.

## Service and verification

- [x] **Service boundary**: add independently feature-gated Iceberg configuration,
  authenticated management commands, bearer authentication, startup dependency
  checks, separate listener, bounded admission, graceful drain and `/v1/config`.
  Files: `app/crowdb-access-server/Cargo.toml`, `src/main.rs`, `src/lib.rs`,
  `src/iceberg/`, `lib/crowdb-access-iceberg/src/wire/`.
- [x] **Unit coverage**: validate IDs, binary-safe key boundaries, unknown versions,
  record bounds, capabilities, epoch overflow, and deadline arithmetic. Files:
  `lib/crowdb-access-iceberg/tests/*_test.rs`.
- [x] **Integration coverage**: test same/different identity retries, root CAS loss,
  crash recovery, consecutive clear, admission expiry, and authorization. Files:
  `lib/crowdb-access-iceberg/tests/*_test.rs`.
- [x] **E2E coverage**: run HTTP/config and multi-instance clear scenarios against
  production clients; prefix server-spawning tests with `pixi run clean-env &&`.
  Files: `app/crowdb-access-server/tests/iceberg_*_test.rs`.
- [~] **Gates and cleanup**: run affected tests, fmt, and clippy separately; commit
  coherent verified tasks. Remove R178 and its backlog entry only after all its
  acceptance claims pass. Keep this plan while the requirement remains unfinished.

## Commands

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --features iceberg --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
- `pixi run -- cargo clippy -p crowdb-access-server --features iceberg --all-targets -- -D warnings`
- `pixi run -e iceberg-e2e test-pyiceberg-e2e`

## Follow-on

- R179 starts after catalog context, retry, and service contracts pass their gates.
  Its namespace admission protocol must be tested against single-key CAS rather
  than a transactional in-memory substitute.

## Verification so far

- The foundation now has 34 passing tests for identity/key validation,
  scope/range isolation, binary-safe names, capability coherence, rename identity,
  epoch overflow, persisted clear timing, FlatBuffer corruption/version handling,
  phase validation and record/key identity matching.
- `pixi run -- cargo clippy -p crowdb-access-iceberg --all-targets -- -D warnings`
  passed; `pixi run rs-lint` passed across the workspace.
- Workspace formatting, test-task coverage, and `git diff --check` passed.
- `pixi run -- cargo test -p crowdb-protocol --all-targets` passed after adding
  the schema; workspace fmt and clippy passed again with the generated module.
- Management/retry records, routed storage, bearer authorization, CLI, isolated
  HTTP listener, and background recovery are implemented. Library tests cover
  every management write's lost reply, concurrent initialize convergence, delayed
  maintenance CAS, restart with shorter configuration, retained grace proof,
  final 409 replay, recoverable 503, principal/digest/domain mismatch, slot
  collisions and expiry. TCP config/authentication tests pass.
- The ledger uses 4096 fixed hash slots per system ledger. A collision with an
  unfinished or retained operation returns Busy; no live slot is evicted. Audit
  and management slots share the identity mapping. Retired response bodies remain
  catalog-scoped until reclamation lands.
- Concurrent management calls may return Busy after bounded helping; convergence
  tests reconcile the root and verify exactly one winning identity and every
  loser's conflict, rather than requiring one initial call to finish under load.
- Full-stack verification passed with real Group 0, DiskDB, DiskIO, ChunkDB,
  routed Chunk-KV and two Iceberg processes plus PyIceberg. It covers backend
  restart during maintenance, repeated clear and original-result replay,
  interrupted root CAS before and after application, durable retry results,
  bounded scan continuation, configuration, warehouse selection and authentication.
  An initial run exposed the existing ChunkDB harness's paired-port assumption
  against persistent reservations. The E2E task uses a separate disposable runtime
  registry; persistent reservations and unrelated harness code remain untouched.
- An intermittent concurrent-initialize test failure was traced to two random
  identities mapping to slot 2576. Concurrency fixtures now select disjoint slots;
  capacity collision/retention is tested independently. The convergence case runs
  100 independent races without weakening its single-winner assertion.
- Library and protocol tests, S3-plus-Iceberg and Iceberg-only access-server
  tests, workspace fmt/clippy and Iceberg/E2E-feature clippy passed. The named
  E2E task also passed, including its build and isolated environment wiring with
  PyIceberg 0.11.1.
- Foundation acceptance exercises authoritative admission and connection expiry
  with L=0 and D=0. Lease arithmetic is unit-tested; lease cache holders and
  delegated FileIO are not exposed. Their live expiry scenarios remain owned by
  R180/R185. REST retry persistence is tested directly on routed storage; R179
  wires it to namespace mutation endpoints before HTTP idempotency is advertised.
