# Iceberg Namespace Plan

Upstream: [R179](../backlog/R179-access-iceberg-namespace.md).
Current integration: [functional catalog plan](plan-iceberg-functional-catalog.md).

Goal: finish namespace acceptance without weakening identity, admission or recovery.

## Completed summary

- Independent writer role, bounded multipart identifiers/properties, authority and
  name mappings, parent-scoped scans and conditional stale-index cleanup.
- Durable payloads and create/update/drop journals, reserve-before-parent-admit,
  shared bounded helping, exact terminal outcomes and response-loss recovery.
- Qualified reads, authenticated pagination, bounded complete-response spooling,
  HTTP mutations and retry ledgers, background recovery and index repair.
- Table-create admission is integrated, including namespace-drop races and
  interrupted immediate/staged publication. Rename-in now reserves before parent
  admission and participates in bounded reservation/admission helping.
- Official PyIceberg CRUD passes against two listeners before and after native
  Chunk-KV/listener restart in a separate functional profile. A retained namespace
  and its exact properties survive the restart and resolve through both listeners.
- The original clear/restart test passes with its 500-ms bound retained across
  repository reconstruction and fault injection. Its client checks remain reads;
  the complete CRUD assertions execute in the separate test, not disappear.
  Both real-stack tests pass serially and under default test concurrency
  (2026-09-24). No production performance
  changes or added client retries were made.
- Rename-in versus destination drop is covered at every interrupted write boundary
  and with delayed head-CAS replies. A losing rename releases only its reservation;
  a winning rename blocks namespace tombstoning. Source/destination recreation and
  commit/drop races preserve exact authority. The closure audit below identifies
  remaining end-to-end evidence; R179 is not closed.

## Remaining execution

- [ ] **Property-limit E2E**: exercise valid entry/key/value/encoded-authority
  boundaries and one-over-limit updates through a listener. Reload after each
  rejected update, including removal/update overlap (422), and assert unchanged
  properties. Library boundary tests alone do not satisfy this E2E acceptance.
  Files: `app/crowdb-access-server/tests/iceberg_namespace_write_http_test.rs`,
  `app/crowdb-access-server/tests/common/iceberg_client.py`.
- [ ] **Official-client listing boundaries**: extend the SDK fixture beyond
  ordinary complete listing to explicit start/continuation, stale-only pages,
  exhaustion and subsequent successful requests proving resource release.
  Reuse bounded HTTP fixtures; first inspect the pinned SDK's pagination API,
  rather than assuming its behavior or counting raw requests as SDK execution.
  Cover missing namespace load/drop errors as well as the existing conflict and
  not-empty cases. Files: `iceberg_namespace_http_test.rs`,
  `tests/common/iceberg_client.py`, `iceberg_full_stack_test.rs` under the server.
- [ ] **Final gates and closure**: after these gaps are covered, rerun the native
  two-listener CRUD/restart and unchanged 500-ms maintenance fixtures against the
  lifecycle integration, plus tests/fmt/clippy. Reconcile every acceptance item,
  then remove the requirement, index entry and this plan together. Do not close
  on a partial CRUD pass or move missing SDK evidence into deferred engine tests.

## Acceptance evidence audit — 2026-09-24

This is a source audit at `a52cfb72`, not a fresh test run. The implementation
checkpoint passed its library/server/SDK gates; final namespace closure gates
remain pending. Test names below are under `lib/crowdb-access-iceberg/tests/`
unless identified as server tests. Numbering follows R179's acceptance bullets.

- **1 — identifiers**: `namespace_model_test.rs` covers encoded level/byte
  boundaries and malformed inputs; `namespace_record_test.rs` covers storage
  records. Server `iceberg_namespace_http_test.rs` covers single URL decoding.
- **2 — concurrent create/replay**: `namespace_admission_test.rs`,
  `namespace_create_test.rs` and `namespace_journal_test.rs` cover competing
  reservations, lost write replies and recovery through another instance.
- **3 — properties**: `namespace_model_test.rs` and `namespace_record_test.rs`
  cover cardinality, byte and encoded-envelope bounds. Server
  `iceberg_namespace_write_http_test.rs` checks overlap status and replay, but
  does not establish the complete boundary-and-unchanged-authority E2E matrix.
- **4 — paged authority filtering**: `namespace_list_test.rs` covers stale-only
  pages, corruption and context-bound tokens; `namespace_repository_test.rs`
  checks authoritative parent/name identity.
- **5–6 — child/drop arbitration**: `namespace_admission_test.rs`,
  `namespace_drop_test.rs`, `namespace_recovery_test.rs`,
  `table_create_namespace_test.rs` and `table_lifecycle_race_test.rs` cover
  namespace/table creation and rename-in, interrupted phases and delayed CAS.
  `table_lifecycle_test.rs` additionally covers recreation and exact replay.
  These are integration fault seams, not process kills at every native phase.
- **7 — bounded empty proof**: `namespace_drop_test.rs` checks stale entries
  before live children, corruption in either child range and exhausted work;
  admission/recovery tests cover unresolved reservations.
- **8 — revision independence**: `namespace_update_test.rs`,
  `namespace_repository_test.rs` and `namespace_recovery_test.rs` cover property
  publication, concurrent writers and helping interrupted nonempty drops.
- **9 — listing E2E**: server `iceberg_namespace_http_test.rs` covers token modes,
  item/byte/scan exhaustion and spool concurrency/release. The official Python
  fixture currently checks ordinary listing; raw HTTP checks do not replace the
  required official-client exhaustion/continuation evidence. Still pending.
- **10 — parent identity**: `namespace_create_test.rs`,
  `namespace_repository_test.rs` and `namespace_list_test.rs` cover missing
  parents, recreation, descendant isolation and token rejection.
- **11 — official endpoint/error matrix**: server `common/iceberg_client.py`
  covers CRUD, exists, duplicate and nonempty errors through PyIceberg. Explicit
  SDK missing-load/drop and pagination/error cases remain in the task above.
- **12 — credentials**: `wire_test.rs` covers role separation and invalid/duplicate
  credentials; server `iceberg_auth_test.rs` covers startup rejection and
  namespace-write HTTP tests exercise independent credentials. The Python fixture
  checks reader denial. Include these suites in the final gate; do not infer
  full role isolation from reader denial alone.

No new human decision is needed. These are acceptance tasks, not R177 open
questions, and do not require changing production semantics or performance bounds.

## Resolved latency blocker and performance evidence

R177 OI-1 is confirmed: functional acceptance and performance are separate.
The dedicated CRUD fixture uses the existing runtime request ceiling of 300,000 ms
with delegation disabled because it tests namespace-only behavior. Existing raw
HTTP client five-second timeouts and CRUD assertions are unchanged. This ceiling
is not a latency claim. The maintenance fixture retains exactly 500 ms, including
its reconstructed/fault-injecting repositories; default repository bounds must
not accidentally expand it through the new monotonic clear-bound behavior.

Historical diagnostics are retained for the eventual performance backlog:

- Recorded command:
  `pixi run clean-env && RUST_LOG=crowdb_access_server=debug CROWDB_RUNTIME_ROOT="$PWD/.crowdb-runtime/ephemeral/iceberg-e2e" CROWDB_ICEBERG_E2E_PYTHON="$PWD/.pixi/envs/iceberg-e2e/bin/python" pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_full_stack_test -- --nocapture`.
- Setup: two real listeners, durable Chunk-KV, persisted 500-ms request bound
  inherited from clear/restart tests, and PyIceberg CRUD without test-side retries.
- First divergence: root or nested create returns 503. Recorded server diagnostics
  identify request deadline exhaustion, not validation/publication corruption.
- Five diagnostic/fix runs: initial CRUD integration; structured deadline errors;
  per-phase timing (about 45–75 ms per durable phase, body read about 100 μs);
  authoritative read-before-put for immutable payloads; no-op checks before
  terminal marker/reservation cleanup.
- Redundant writes were removed without changing publication CAS, and focused
  loss/replay tests pass. One complete CRUD pass was followed by another client's
  root create exceeding 500 ms. Recorded failure:
  `catalog_recovery_survives_real_chunk_kv_restart` at its official-client check,
  `ServiceUnavailableError: ServiceUnavailableException: Catalog is not ready`.
- Instrumentation was removed. The split fixtures now pass; they do not establish
  that all namespace mutations meet 500 ms. Broader critical-path/batching work
  goes into the functional plan's performance inventory and later consolidated
  backlog; fix obvious bugs only with measured root causes and regression tests.
- The maintenance run logs rejected repair attempts for synthetic reserved
  mappings without journals seeded by `verify_name_index`; the separate CRUD
  fixture did not show these errors. Do not treat this fixture setup as evidence
  of production corruption or hide the diagnostics to improve the result.

## Verification

- Library: `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`.
- Server: `pixi run clean-env && pixi run -- cargo test -p crowdb-access-server --features iceberg --all-targets`.
- Official client: `pixi run -e iceberg-e2e test-pyiceberg-e2e`.
- Gates: `pixi run -- cargo fmt --all -- --check`; `pixi run rs-lint`;
  `pixi run -- cargo clippy -p crowdb-access-server --features iceberg-e2e --all-targets -- -D warnings`.
- Use an isolated runtime root for native tests, preserving unrelated persistent
  port claims. Do not clean another running fixture's state.
