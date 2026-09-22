# Iceberg Namespace Plan

Upstream: [namespace requirement](../backlog/R179-access-iceberg-namespace.md).

Goal: expose recoverable namespace operations without weakening authoritative
identity, empty-drop safety, or bounded REST responses.

Execution checkpoint: development resumed. The user clarified that only weekly
quota remaining below 25% stops development; context usage does not. No test
failure is pending. Continue recovery integration, listing and REST work without
treating this checkpoint as requirement completion. Outstanding human decisions
remain centralized in R177.

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
- [x] **Durable payloads**: store immutable hashed payload pages for operation
  input, authority snapshots and large retry responses without exceeding the
  64-KiB record limit. Verify lost replies, corruption and cross-domain isolation.
  Files: operation payload modules, retry ledger, storage envelope, protocol schema.
- [x] **Durable operation records**: persist admission/publication/abort phases,
  immutable mutation input and outcome evidence. Keep these keys separate from
  retained HTTP responses. Avoid embedding multiple near-64-KiB authorities in
  one 64-KiB envelope. Files: namespace operation/record modules, protocol schema.
  Payload pages are 32 KiB with a 2-MiB aggregate cap. Namespace operation records
  have their own key scope, distinct from retained HTTP responses. Freeze mutation
  snapshots once a write phase starts; persist forward-only child-probe cursors.
- [x] **Authoritative reads**: walk stable parent identities, qualify every mapping
  against its authority and full identifier, distinguish corruption from absence,
  and reject maintenance/retired contexts. Files: namespace repository and tests.
- [x] **Property mutation driver**: persist input and snapshots, publish properties
  with whole-authority CAS, retain pending-operation evidence until the outcome is
  durable, and recover lost replies without changing the original result. Rebase
  only after a definitive conflicting revision; bound helping and retries.
  Files: namespace update/recovery modules, journal transitions and tests.
- [x] **Create and admission**: persist a name reservation before the actual parent
  CAS; preserve uncertain admission evidence until the journal advances, then
  publish authority and mapping. Persist abort outcomes before reservation cleanup.
  Bound recursive helping with one shared phase budget. Verify every lost create
  write, duplicate names, different-child contention and a pre-admission drop fence.
  Files: namespace create/admission/publication/recovery modules and tests.
- [x] **Drop driver**: fence admission, persist both child-range probes, restore
  nonempty namespaces, tombstone empty namespaces and conditionally clean mappings.
  Bound cross-operation helping and stale-page traversal. Validate create/drop
  races and every empty/nonempty drop write-reply loss. Files: namespace drop,
  fence/probe/finish modules and tests.
- [x] **Marker settlement**: route property preparation and retries through the
  holder-bound create/update/drop helper with shared phase budgets. Verify every
  interrupted parent admission and nonempty drop followed by a property writer.
  Files: namespace repository/update modules and cross-action recovery tests.
- [x] **Background operation recovery**: scan four journal entries per page,
  resume each with 16 shared phase steps, and run a listener-owned periodic sweep
  with a one-second deadline and catalog-bound cursor. Recover terminal mapping
  cleanup too; isolate manual crash checkpoints from active recovery workers.
  Verify abandoned creation using two real listener processes and no client retry.
  Files: namespace recovery/scan, listener runtime and library/full-stack tests.
- [~] **Recovery integration**: verify remaining stale-index repair and the
  table-create/rename-in admission seam. Real-backend drop restart tests pass. Until table
  records land, any table-child record fails closed rather than proving emptiness.
  Files: namespace recovery, server runtime and integration tests.
- [x] **Listing**: bind authenticated tokens to catalog, parent identity/spelling,
  page parameters and scan cursor; bound scan work and unpaginated spool resources.
  Files: namespace listing/token modules, access-server spool implementation.
  Bounded authority-validated pages and HMAC-SHA256 tokens now have three focused
  tests: stale empty pages, parameter/key/recreated-parent binding, and corruption.
  Complete-response spool caps bytes/items/scans/concurrency and streams 16-KiB
  frames. Three HTTP tests cover decoding, absent/empty/continuing tokens,
  admission release and each exhaustion dimension without truncated success.
  Official PyIceberg namespace list/load and raw HEAD pass against real listeners.
- [ ] **REST integration**: add bounded request parsing, endpoint advertisement,
  role checks, error mapping, and shared retry-ledger participation.
  Files: library wire modules, access-server Iceberg modules.
  Write routes, UUIDv7/shared-ledger handling, terminal 4xx replay and large result
  paging are implemented with three passing focused HTTP tests. Official-client
  CRUD is not accepted yet: see the bounded-latency E2E blocker below.
  Size URL and JSON limits for the identifier/property bounds. Validate against
  the 2-MiB retry-body bound before publication; larger-than-16-KiB results use
  immutable pages and a final response manifest rather than an oversized record.
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

- Namespace list/load/exists are now exposed and advertised, with bounded complete
  spooling. Library tests total 92; namespace HTTP tests, config HTTP regression,
  real-backend official-client reads/restarts, fmt and clippy pass. Namespace
  mutations still await shared HTTP retry-ledger integration and advertisement.

- Empty/nonempty namespace drop has six passing tests; the library has 85 passing
  tests. Coverage includes every lost drop write reply, create versus drop,
  recreated-name cleanup, corruption in both ranges, and a live child after 260
  stale mappings with an intervening bounded-work exhaustion. Foreign property
  markers fail before helping another namespace; property helping consumes the
  caller's shared create/drop phase budget. Cross-action property recovery adds
  two passing fault-matrix tests, bringing the library total to 87. Periodic
  repair and table lifecycle integration remain pending.
  Real Chunk-KV restart after a lost tombstone write reply recovers the original
  204 result, retains the tombstone and protects a recreated name from old replay.
  Formatting, workspace clippy, feature-enabled server clippy and real-backend
  create/property/drop restart tests pass at this checkpoint.

- Native top-level and nested create plus admission recovery pass eight tests.
  Completed abort outcomes
  replay unchanged and conditional reservation deletion preserves a recreated name.
  Real Chunk-KV restart after a lost nested mapping-publication reply preserves
  the chosen NamespaceId and completes both parent and child marker cleanup.

- Authoritative namespace load/exists and durable property publication pass 13
  new repository tests, including every lost write reply, competing property CAS,
  identity-bound replay, stale/recreated parents, corruption and size limits.
  Real Chunk-KV restart
  after a lost property-publication reply preserves exactly one property revision
  and replays the original response; workspace and feature-enabled clippy pass.
  Listing and REST remain incomplete; these tests are not full REST acceptance.

- Operation payload and journal gates pass alongside protocol tests,
  feature-enabled server tests, workspace/feature clippy and formatting. Real
  Chunk-KV restart preserves a namespace journal and a 70-KiB retry response.
- Phase CAS tests cover publication versus abort, lost phase replies, fixed
  mutation snapshots, forward child-range cursors and retired catalog rejection.
  These verify journal semantics, not complete namespace REST acceptance.

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

## Blocked

Only the real-stack namespace CRUD acceptance task is blocked; continue unrelated
work under the user's authorization. The latency decision is centralized in R177.

- Command: `pixi run clean-env && RUST_LOG=crowdb_access_server=debug CROWDB_RUNTIME_ROOT="$PWD/.crowdb-runtime/ephemeral/iceberg-e2e" CROWDB_ICEBERG_E2E_PYTHON="$PWD/.pixi/envs/iceberg-e2e/bin/python" pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_full_stack_test -- --nocapture`.
- Setup: two real listeners, real durable Chunk-KV, and the existing 500-ms catalog
  request bound used by the clear/restart fixture. PyIceberg performs namespace
  CRUD without caller-side retries.
- First divergence: root or nested `create_namespace` returns HTTP 503 rather
  than success. Server diagnostics confirm the request deadline expires; no
  namespace validation or publication corruption was reported.
- Five runs: initial CRUD integration; structured error/deadline diagnostics;
  per-phase timing (roughly 45–75 ms per durable phase, body read about 100 μs);
  authoritative read-before-put for existing immutable payload pages; and
  authoritative no-op checks before terminal marker/reservation cleanup.
- The latter changes remove redundant writes without changing publication CAS,
  and focused loss/replay tests pass. A complete client CRUD pass was observed,
  but a following client's root create still exceeded 500 ms. Latest run fails
  `catalog_recovery_survives_real_chunk_kv_restart` at the official-client check
  with `ServiceUnavailableError: ServiceUnavailableException: Catalog is not ready`.
- Temporary phase/body instrumentation was removed. Do not increase timeouts,
  add caller retries, suppress the failure, or mark the requirement complete.
  Resume this acceptance task after confirmation of the latency profile or
  authorization for further critical-path redesign.
