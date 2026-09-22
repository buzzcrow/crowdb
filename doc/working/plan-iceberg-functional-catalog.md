# Iceberg Functional Catalog Plan

Upstream: [R177 blueprint](../backlog/R177-access-iceberg-catalog-foundation.md),
[R179 namespaces](../backlog/R179-access-iceberg-namespace.md),
[R180 FileIO](../backlog/R180-access-iceberg-fileio.md),
[R181 lifecycle](../backlog/R181-access-iceberg-table-lifecycle.md),
[R182 commits](../backlog/R182-access-iceberg-table-commit.md),
[R183 reclamation](../backlog/R183-access-iceberg-reclamation.md),
[R184 conformance](../backlog/R184-access-iceberg-rest-conformance.md).

Goal: implement a usable native catalog in dependency order without presenting
deferred storage reclamation as completed correctness work.

Persistent-plan exception: this file coordinates multiple requirements. Remove
completed tasks and their obsolete upstream links; retain the plan until the
program finishes. Each requirement keeps its own detailed execution plan.

Status: the user approved this ordering and implementation of independent work.
Collect unresolved human decisions in R177 for confirmation when the user returns;
do not stop unrelated tasks. No user-guide tasks.

## Review checkpoint

- R178 supplies catalog management, authentication, recovery, and config. The
  current HTTP dispatcher accepts only authenticated `GET /v1/config`.
- R179 has identifiers, properties, authority/mapping records, bounded scans,
  conditional deletion, separate writer credentials, payload pages, and durable
  create/property/drop drivers. Shared recovery integration, periodic repair,
  listing, and namespace REST remain unfinished.
- R180 through R184 have no corresponding completed feature implementations.
  Shared infrastructure is reusable, but is not acceptance of these requirements.
- A listening config service already works. A namespace catalog needs R179.
  A native catalog that clients can create tables in, write to, and read from
  needs R180, R181, R182, and the relevant R184 integration and client tests.
- R177's full correctness milestone includes R183 and all R184 acceptance.
  An earlier functional checkpoint must not be labelled that full milestone.

## Approved reclamation deferral

- Defer R183 execution, not its backlog or safety contract. R180 explicitly
  allows unreachable staged/orphan data to leak before reclamation; R181 permits
  logical drop without cleanup; R182 keeps losing candidates unreachable.
- Keep physical deletion of Iceberg-owned files and chunks disabled, including
  implicit cleanup by upload expiry, multipart abort, table drop, and catalog
  clear. Logical expiration, bounded recovery, and publication fencing still run.
- Preserve ownership, generations, durable operation outcomes, upload state, and
  purge intent needed for later candidate discovery. Do not remove the last
  evidence of retained storage while recycling bounded foreground state.
- For `purgeRequested=true`, persist a pending proof task before reporting the
  logical drop complete, as R181 requires. Do not report physical purge complete
  or expose a public file DELETE route. Worker status/control remains unavailable
  until implemented and verified.
- Without reclamation, cumulative retained storage is not bounded by per-request
  or session limits. Use a capacity-limited trial with monitored free capacity;
  stop admitting writes before exhaustion. This is not a sustainable long-running
  production storage policy.
- Retention, reader/credential leases, and operator pins must be enforced before
  any future deleter is enabled. Deferral is not permission to replace positive
  reachability proof with TTL-only deletion.
- Run R184's foreground integration early, but leave its reclamation-dependent
  acceptance and original completion status pending. Update upstream milestone
  wording reflects the approved split while retaining the full milestone.

## Dependency-ordered execution

- [ ] **Finish namespace recovery integration**: unify marker settlement across
  create/property/drop entry points and add bounded periodic repair. Preserve
  verified reserve-before-admit and durable publication recovery behavior.
  Files: `lib/crowdb-access-iceberg/src/namespace/`, library tests, existing
  `plan-iceberg-namespace.md`.
- [ ] **Finish namespace drop**: fence admission, reconcile publishable children,
  persist both child-range probes, restore nonempty parents, and conditionally
  remove mappings without deleting recreated names. Exercise the future table
  create and rename-in admission contract. Files: namespace modules and tests.
- [ ] **Finish namespace surface**: authenticated continuation tokens, bounded
  complete-list spool, REST handlers, independent writer authorization, retry
  ledger integration, and official-client CRUD/restart tests. Close R179 only
  after its full gates. Files: namespace/wire modules, server Iceberg modules,
  library/server tests and namespace execution plan.
- [ ] **Implement immutable file authority**: canonical locations, bounded file
  records, inline/chunk selection, seal validation, immutable publication,
  streaming PUT/HEAD/range GET, and delegated table-prefix credentials. Files:
  library `src/file/`, record/schema extensions, server FileIO routes, tests.
- [ ] **Complete FileIO contract**: durable bounded multipart and recovery,
  projection fallback, streaming manifest validation, and verified format hints.
  Preserve abandoned-state discovery without physical cleanup. Run official
  FileIO tests before closing R180. Files: file and metadata projection modules,
  server integration and tests; a new per-requirement FileIO plan.
- [ ] **Implement selected table metadata**: bounded heads and mappings,
  v1/v2/v3 validation, canonical-byte preservation, selected-generation ALL/REFS
  loads, ETags, exists and listing. Use test fixture heads only; do not invent a
  second production create publisher. Files: library `src/table/`, tests.
- [ ] **Complete table lifecycle**: fenced same/cross-namespace rename, logical
  drop, durable pending purge intent, retry/recovery and REST integration. Verify
  rename-in versus namespace drop before closing R181. Files: table lifecycle,
  server handlers, tests; a new per-requirement lifecycle plan.
- [ ] **Implement table creation**: immediate/staged create, immutable initial
  metadata, namespace admission, one initial head publisher, and expiration
  recovery. Connect native FileIO; verify an official-client create/load/write
  vertical slice as capabilities become available. Files: library `src/commit/`,
  table/FileIO integration, server handlers and tests.
- [ ] **Complete atomic commits**: bounded requirement/update evaluation against
  one generation, complete declared v1/v2/v3 semantics, upgrades, head CAS,
  terminal replay and orphan evidence. Verify conflicts and crash points before
  closing R182. Files: commit modules, wire models and tests; a new commit plan.
- [ ] **Gate the functional checkpoint**: complete common REST composition,
  discovery, credentials, errors, metrics, cancellation and admission. Run the
  pinned compatibility kit, Java/Rust clients and supported engine profiles;
  publish executable version/capability results. Explicitly record pending GC
  coverage rather than closing R184. Files: library `src/rest/`, server runtime,
  conformance fixtures, test environment and a per-requirement REST plan.
- [ ] **Implement reclamation later**: durable candidates, bounded reachability,
  retention/pins, deletion proofs, isolated worker budgets and operator controls.
  Reconcile data retained during the functional checkpoint. Close R183 only after
  deletion safety and restart gates. Files: library `src/gc/`, server operator
  integration and tests; a new reclamation plan.
- [ ] **Close full conformance**: run remaining reclamation-dependent and complete
  cross-feature acceptance, then close R184 and the original correctness
  milestone. R185 cache optimization remains outside this plan. Files: client
  fixtures, affected permanent design, requirement/index and execution plans.

## Consolidated files and verification

- Production: `lib/crowdb-access-iceberg/src/`, scoped additions to
  `lib/crowdb-protocol/src/fbs/iceberg.fbs`, and
  `app/crowdb-access-server/src/iceberg/`.
- Tests: `lib/crowdb-access-iceberg/tests/`,
  `app/crowdb-access-server/tests/`, protocol tests when schema changes, and
  pinned conformance environments. All Rust tests stay outside production files.
- Unit: encoding/size boundaries, identifier and metadata validation, every
  supported requirement/update variant, format and upgrade fixtures.
- Integration: real Chunk-KV/chunk storage, competing publishers, every durable
  crash boundary, uncertain CAS replies, bounded streams/scans and restart replay.
- E2E: namespace CRUD first; then FileIO/multipart, table lifecycle and commits;
  finally compatibility kit/client/engine and full format matrices. Carry these
  incrementally rather than waiting until R184 to expose integration failures.
- Per requirement: affected library/protocol/server tests, existing
  `pixi run -e iceberg-e2e test-pyiceberg-e2e`, separately
  `pixi run -- cargo fmt --all -- --check` and `pixi run rs-lint`, plus relevant
  feature-enabled gates. Prefix server-spawning tests with `pixi run clean-env &&`.
  Passing config-only client tests does not count as full catalog acceptance.
- Keep coherent verified commits and truthful checkpoints. A seven-hour absence
  is not a delivery estimate for six requirements; start with remaining R179
  execution/recovery and continue in the approved order, bypassing only tasks that
  depend on unresolved human decisions recorded in R177.
- During active execution, check the current session's reported weekly quota
  roughly every ten minutes. Stop development only when weekly quota remaining
  falls below 25%; preserve the current diff and record unfinished work. Context
  window usage is not a stopping criterion. Read the weekly rate-limit window
  from local session token-count events without requiring an interactive command.
