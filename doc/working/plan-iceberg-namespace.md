# Iceberg Namespace Plan

Upstream: [namespace requirement](../backlog/R179-access-iceberg-namespace.md).

Goal: expose recoverable namespace operations without weakening authoritative
identity, empty-drop safety, or bounded REST responses.

## Execution

- [x] **Writer credential**: add required `CROWDB_ICEBERG_WRITE_TOKEN`, distinct
  principal and namespace-write capability; keep management/clear privileges
  separate and verify startup validation and writer management denial.
  Files: library auth, runtime configuration, library/server/full-stack tests.

- [x] **Identifiers and properties**: validate multipart storage and REST names,
  establish explicit identifier bounds, and validate atomic property changes.
  Files: `lib/crowdb-access-iceberg/src/namespace/`, crate integration tests.
- [x] **Authority and index records**: encode validated namespace authorities and
  reserved/published mappings in the existing FlatBuffers envelope; bind records
  to catalog, stable identity and parent/name keys. Reserve fixed encoding space
  for operation markers so lifecycle changes cannot overflow a full authority.
  Files: namespace authority/key modules, `src/record/`, protocol schema, tests.
- [x] **Storage operations**: add bounded parent-scoped scans and conditional
  mapping deletion, retaining routed continuations and backend request identities.
  Verify recreation safety against real Chunk-KV. Files: namespace storage,
  catalog storage, access-server full-stack fixture.
- [ ] **Durable operation records**: persist admission/publication/abort phases,
  immutable mutation input and outcome evidence. Keep these keys separate from
  retained HTTP responses. Avoid embedding multiple near-64-KiB authorities in
  one 64-KiB envelope. Files: namespace operation/record modules, protocol schema.
- [ ] **Admission and recovery**: persist reserve-before-admit transitions and
  publication evidence; resolve pending admission before subsequent parent writes.
  Implement create, load, update, drop, stale repair, and durable two-range probes.
  Files: namespace repository/admission/recovery modules and concurrency tests.
- [ ] **Listing**: bind authenticated tokens to catalog, parent identity/spelling,
  page parameters and scan cursor; bound scan work and unpaginated spool resources.
  Files: namespace listing/token modules, access-server spool implementation.
- [ ] **REST integration**: add bounded request parsing, endpoint advertisement,
  role checks, error mapping, and shared retry-ledger participation. Do not modify
  the user guide. Files: library wire modules, access-server Iceberg modules.
  Size URL and JSON limits for the identifier/property bounds; the foundation's
  16-KiB retry-body bound must not reject a valid large namespace response after
  publication. Preserve bounded storage records using a durable response layout.
- [ ] **Verification**: run boundary/codec, failure-injection, concurrent recovery,
  and official-client acceptance tests; run formatting and clippy separately.
  Files: library tests, access-server tests and official-client fixture.
- [ ] **Completion**: update the matched permanent architecture, remove the
  completed requirement/index entry and this plan after all acceptance gates.

## Files

- `lib/crowdb-access-iceberg/src/{namespace,record,catalog,operation,wire}/`
- `lib/crowdb-protocol/src/fbs/iceberg.fbs`
- `lib/crowdb-access-iceberg/tests/`
- `app/crowdb-access-server/src/iceberg/`
- `app/crowdb-access-server/tests/`
- `doc/design/access-server/iceberge/design-crowdb-iceberg.md`

## Tests

- Unit/integration: `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`.
- Server: `pixi run clean-env && pixi run -- cargo test -p crowdb-access-server --all-targets`;
  repeat with the `iceberg` feature enabled.
- E2E: `pixi run -e iceberg-e2e test-pyiceberg-e2e`.
- Formatting: `pixi run -- cargo fmt --all -- --check`.
- Lint: `pixi run rs-lint`.

## Verified checkpoint

- Writer credential validation, read access and management denial pass library,
  HTTP and real-process tests. The official client authenticates using the writer
  token. Missing or invalid writer configuration fails before backend connection.

- Multipart/property and namespace-record integration tests pass, including
  encoded authority overhead, fixed lifecycle-marker capacity, parent-scoped
  range bounds and continuation rejection.
- Protocol tests pass. Existing Iceberg catalog and HTTP tests remain passing.
- The real-stack official-client task passes with new direct storage assertions
  for one-item namespace scan pages, an empty table-child range, conditional
  deletion mismatch and replay after a name is recreated. Namespace REST endpoints
  are not implemented or advertised yet; this is not namespace REST acceptance.
- Formatting, workspace clippy and feature-enabled access-server clippy pass.

## Authorization decision

The user selected a separate writer credential. Writer may read and mutate
namespaces, but may not initialize, rename or clear the catalog. Reader remains
read-only; manager and clearer retain administrative privileges without inheriting
namespace writes. Bind retries to the distinct writer principal. The design
decision is resolved; remaining implementation work is tracked above.
