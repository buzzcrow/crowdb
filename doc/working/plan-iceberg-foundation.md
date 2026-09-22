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
- [~] **Storage records**: add versioned FlatBuffer root, authority, operation,
  audit and retry binding records with bounded decoding and identity validation.
  Keep domain and REST models separate. Files: `lib/crowdb-protocol/src/fbs/`,
  its generated-code integration, `lib/crowdb-access-iceberg/src/record/`.
  The generated-code-only unsafe exception has been raised with the user; do not
  add it until answered.
- [ ] **Storage adapter**: wrap routed Chunk-KV point CAS and scans, preserving
  typed outcomes and persisted request identities. Files:
  `lib/crowdb-access-iceberg/src/catalog/storage.rs` and integration tests.
- [ ] **Management recovery**: implement initialize/status/rename/clear, durable
  management receipts, bounded audit, retained results, maintenance and persisted
  completion deadlines. Add crash and concurrent-operation tests. Files:
  `lib/crowdb-access-iceberg/src/catalog/repository.rs`, `src/operation/`.
- [ ] **REST retry boundary**: implement optional UUIDv7 keys, principal/digest/domain
  bindings, retention and capacity admission, final 4xx replay, and non-final 5xx
  recovery. Files: `lib/crowdb-access-iceberg/src/operation/`, `src/wire/`.

## Service and verification

- [ ] **Service boundary**: add independently feature-gated Iceberg configuration,
  authenticated management commands, bearer authentication, startup dependency
  checks, separate listener, bounded admission, graceful drain and `/v1/config`.
  Files: `app/crowdb-access-server/Cargo.toml`, `src/main.rs`, `src/lib.rs`,
  `src/iceberg/`, `lib/crowdb-access-iceberg/src/wire/`.
- [ ] **Unit coverage**: validate IDs, binary-safe key boundaries, unknown versions,
  record bounds, capabilities, epoch overflow, and deadline arithmetic. Files:
  `lib/crowdb-access-iceberg/tests/*_test.rs`.
- [ ] **Integration coverage**: test same/different identity retries, root CAS loss,
  crash recovery, consecutive clear, admission expiry, and authorization. Files:
  `lib/crowdb-access-iceberg/tests/*_test.rs`.
- [ ] **E2E coverage**: run HTTP/config and multi-instance clear scenarios against
  production clients; prefix server-spawning tests with `pixi run clean-env &&`.
  Files: `app/crowdb-access-server/tests/iceberg_*_test.rs`.
- [ ] **Gates and cleanup**: run affected tests, fmt, and clippy separately; commit
  coherent verified tasks. Remove R178 and its backlog entry only after all its
  acceptance claims pass. Keep this plan while the requirement remains unfinished.

## Commands

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --features iceberg --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
- `pixi run -- cargo clippy -p crowdb-access-server --features iceberg --all-targets -- -D warnings`

## Follow-on

- R179 starts after catalog context, retry, and service contracts pass their gates.
  Its namespace admission protocol must be tested against single-key CAS rather
  than a transactional in-memory substitute.

## Verification so far

- The initial foundation has 11 passing tests for identity/key validation,
  scope/range isolation, binary-safe names, capability coherence, rename identity,
  epoch overflow, and persisted clear timing.
- `pixi run -- cargo clippy -p crowdb-access-iceberg --all-targets -- -D warnings`
  passed; `pixi run rs-lint` passed across the workspace.
- Workspace formatting, test-task coverage, and `git diff --check` passed.
- These checks do not complete R178: durable records/repositories, security,
  retry-ledger persistence, management commands, HTTP and crash/E2E coverage remain.

## Blocked

- The storage-record task awaits the user response to the generated-code-only
  `unsafe_code` exception, raised under the repository AGENTS.md rule before adding
  it. No exception or generated module has been added. Approval permits the same
  isolated FlatBuffers wrapper pattern already used in `crowdb-protocol`; declining
  it leaves the schema integration pending rather than substituting another
  persistence format. The independent identity, key, capability and timing task
  is implemented and verified.
