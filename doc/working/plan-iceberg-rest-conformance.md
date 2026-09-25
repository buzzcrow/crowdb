# Iceberg REST Conformance Plan

Upstream: [R184](../backlog/R184-access-iceberg-rest-conformance.md).
Program: [functional catalog plan](plan-iceberg-functional-catalog.md).

Goal: finish the foreground REST implementation and official-client evidence;
leave engine and reclamation-dependent acceptance explicitly pending.

## Scope and starting point

- Planning checkpoint only: no production changes or new tests have been run for
  this requirement. R179–R182 already supply the storage and mutation foundation.
- Implement tasks 1–4 first, then extend client evidence in task 5. Each task can
  be committed independently after its affected tests and quality gates pass.
- Do not run Spark/Flink/Trino, physical GC or broad performance experiments.
  Do not update the user guide. Human decisions belong in R177, not this plan.
- Use the backed-up OpenAPI and table spec under
  `doc/design/access-server/iceberge/` before selecting behavior. Java fixtures
  currently pin Iceberg 1.11.0. Pin and inspect upstream sources before adding a
  Rust client or Compatibility Kit; their compatibility is not yet established.

## Findings from the initial code inspection

- `wire/config.rs` builds all format overrides from `Capabilities::default()`;
  these are false even when table routes are installed.
- New catalog authorities persist zero capability bits. Listener startup, REST,
  FileIO and credential refresh reject every nonzero capability set. This is a
  foundation-era constraint, not an implemented per-version admission policy.
  The legacy activation policy is tracked as R177 OI-6; independent tasks proceed.
- `http.rs` separately assembles endpoint strings and dispatches by broad path
  prefixes. Table builders can be installed without namespaces, but dispatch
  rejects every non-config route in that combination: discovery can overstate
  callable routes. Test and fix this before changing format persistence.
- Table mutation dispatch accepts POST/DELETE before parsing the complete target;
  inspect unsupported subpaths before retry-ledger admission. Do not equate a
  rejected request with proof that no recovery/ledger record was written.
- No common REST protocol metrics are wired. S3 has an existing bounded atomic
  metrics pattern, but must not become the Iceberg metric authority.
- The OpenAPI access-delegation header is an optional list; the server may choose
  any or none of the offered mechanisms. Prefix is optional. Do not require every
  client to send the header or invent support for arbitrary nonempty prefixes.

## Tasks in execution order

- [x] **1. Unified route discovery — medium**: introduce a bounded endpoint
  descriptor/classifier used by both config discovery and dispatch admission.
  Keep identifiers encoded until the owning decoder validates them. Do not
  duplicate an independent route list for metrics later.
  Files: server `iceberg/http.rs`, new `iceberg/routes.rs`, library
  `wire/config.rs`, new server `tests/iceberg_route_test.rs`.
  - Enumerate foundation, namespace, table-read, table-write and credentials
    combinations, including builder combinations unavailable in production.
  - Match method and complete path before body reads or retry-ledger mutation;
    preserve authentication precedence and standard HEAD response bodies.
  - Verify register, views, transactions, scan planning, token issuance and
    unsupported methods/subpaths are absent from discovery and cannot mutate.
  - Check advertised templates against the backed-up OpenAPI, including the
    optional-prefix convention. Do not add config/token/reporting routes to the
    advertised set merely because their names appear elsewhere in the spec.
  - Exit: real HTTP calls agree with discovery, disabled calls preserve authority
    and ledger bytes, and existing Java discovery/list/load fixtures still pass.

- [~] **2. Common protocol and authorization boundaries — medium**: add a
  table-driven conformance matrix and repair only demonstrated differences.
  Files: server `iceberg/http.rs`, `namespace_read.rs`, `namespace_request.rs`,
  `table_read.rs`, `table_write/request.rs`, `table_write/lifecycle.rs`,
  `table_credentials.rs`; existing namespace/table/credential HTTP tests.
  - Cover malformed percent escapes/UTF-8, multipart namespaces, duplicate query
    parameters and sensitive headers, warehouse, snapshots, ETags and purge.
    Separate fields that OpenAPI permits ignoring from malformed known fields.
  - Inspect pinned SDK behavior for absent/list-valued access-delegation headers
    and credential refresh before changing FileIO configuration responses.
  - Cover all four bearer roles plus invalid/missing credentials for every route
    class. Preserve the independent writer role; do not introduce tenant ACLs or
    OAuth issuance as incidental changes. Reject ambiguous authentication inputs.
  - Assert no destination/name/location/credential disclosure on rejected calls.
    Reuse clear, rename, staged-owner and expired-grant fixtures rather than
    rebuilding the catalog state machine.
  - Check bounded headers/URI/JSON/response admission and cancellation outcomes;
    reuse existing durable crash evidence where production paths are unchanged.
  - Exit: stable status/error types and exact authority/ledger behavior at each
    rejected boundary, with no widened timeout or client retry policy.

- [ ] **3. Persisted format capability reconciliation — high**: establish one
  effective profile from durable authority, supported implementation and installed
  services, then use it consistently for discovery and admission.
  Files: library `catalog/capability.rs`, `catalog/state.rs`,
  `catalog/repository.rs`, `record/authority.rs`, `wire/config.rs`; server
  `iceberg/runtime.rs`, `http.rs`, `file_http.rs`, `table_credentials.rs` and
  table read/write admission; catalog/wire/HTTP/recovery tests.
  - First trace initialize, clear, rename, restart and retired-context behavior.
    Specify what legacy zero bits mean and how activation becomes durable before
    coding migration. Do not reinterpret zero as unrestricted support, silently
    rewrite stored authority at startup, or clear user data to enable features.
  - Preserve persisted request/delegation bounds and configuration generations.
    If migration requires a new operator choice, record concrete alternatives in
    R177 and continue independent tasks; do not guess the policy.
  - Test parse/read/create/write separately for v1/v2/v3 and each upgrade edge,
    including confirmed direct v1-to-v3 intermediate validation. Audit selected
    and retained versions rather than checking only the incoming JSON number.
  - Define disabled-version behavior for load, mutations and upgrades; preserve
    deterministic replay and maintenance fencing. Native byte storage is not
    selected-format validation and must not infer FileKind from a Parquet PUT.
  - Test legacy/new profiles, partial valid profiles, unsupported bits, listener
    dependencies, restart and clear across REST/FileIO/credential refresh.
  - Exit: config does not understate or overstate actual version admission, and
    old catalogs cannot silently acquire broader persisted capabilities.

- [ ] **4. Bounded protocol observability — medium, cancellation edge medium-high**:
  add lock-free counters and bounded latency measurements using fixed labels.
  Files: new library `metrics.rs` and tests; server `iceberg/http.rs`, `body.rs`,
  request-body readers, table retry/outcome paths and runtime status integration.
  - Define endpoint/outcome, retry/conflict and selected version enums. Never use
    namespace, table, raw URL, principal, token or payload as a metric label.
  - Distinguish dispatch latency from response-body completion; count actual
    admitted/request-consumed and emitted body bytes, not Content-Length alone.
    Handle HEAD, empty bodies, streamed errors, timeout, cancellation and Drop
    without double counting or keeping permits alive.
  - Reuse the repository's status/export conventions; do not expose an unauthenticated
    diagnostics route on the catalog listener by default.
  - Apache client report-metrics POST is a separate protocol operation from
    server observability. Keep it unadvertised until bounded schema validation,
    authorization and meaningful handling are implemented; do not return fake
    success solely to satisfy a client fixture.
  - Exit: deterministic unit/body tests prove counts and cleanup; endpoint labels
    remain bounded even under arbitrary paths and error input.

- [ ] **5. Official-client and compatibility evidence — medium-high**: extend
  existing Java/native fixtures, add a pinned official Rust client harness and
  investigate the Apache REST Compatibility Kit's actual runner/artifacts.
  Files: server `tests/common/iceberg_java/`, new Rust/kit fixtures under tests,
  `tests/iceberg_table_sdk_test.rs`, `tests/iceberg_file_http_test.rs`,
  dedicated Pixi test environment and dependency manifests only as needed.
  - First run config/namespace/create/load/commit against one listener, then two
    listeners with response loss and retired-context retry. Reuse native storage.
  - Build an executable matrix of version, endpoint, selected format, SDK version,
    fixture and result. Include upgrades, defaults, lineage, deletes/DV, statistics,
    snapshot refs/time travel, rename/drop and logical snapshot expiry.
  - Reuse existing verified cases; add missing cases rather than rerunning every
    historical process-kill scenario after a documentation-only matrix change.
  - Report upstream-client feature gaps and exact kit omissions explicitly. Do
    not patch clients, waive errors, or count a custom fixture as the Apache kit.
  - Exit: foreground rows have executable evidence; engine and GC-dependent rows
    remain pending and full R184 closure is not claimed.

## Verification

- Unit: capability bit/profile tests, wire/config/parameter tests, bounded metrics
  counters and body lifecycle. Place all Rust tests under each crate's `tests/`.
- Integration: real HTTP route/role combinations, durable rejection, legacy
  authority and context changes, admission and cancellation. Reuse existing
  `tests/common/iceberg_store.rs` and native fixtures.
- E2E: pinned Java, Rust and Apache kit; one native stack at a time. Set the same
  isolated `CROWDB_RUNTIME_ROOT` for `pixi run clean-env` and the test command.
  Preserve unrelated persistent state. Never clean a running stack.
- Start with changed test targets, then
  `pixi run cargo test -p crowdb-access-iceberg --all-targets`
  and default/Iceberg server all-targets.
  Keep the no-default Iceberg transport gate when touching the common boundary.
- Gates: `pixi run cargo fmt --all -- --check`, `pixi run rs-lint`, and
  `pixi run cargo clippy -p crowdb-access-server --features iceberg-e2e --all-targets -- -D warnings`.
- Java and native commands/environment are in the functional plan's verification
  section. Rust/kit commands must be recorded after the actual harness is pinned.
- No new runtime locks or unsafe exceptions. Investigate timing failures instead
  of weakening assertions, widening deadlines or adding test-side retries.

## Completion boundaries

- Main implementation checkpoint: tasks 1–4 and their targeted acceptance.
- Foreground interoperability checkpoint: task 5, excluding explicitly deferred
  engine and reclamation gates.
- Full R184 closure: only after the user's separate engine project and relevant
  R183 evidence satisfy the remaining acceptance. Keep the requirement and this
  plan until then; keep completed summaries concise.
