<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console Dataset Workspace Plan

Upstream: [R236](../backlog/R236-console-dataset-workspace.md),
[Dataset control lifecycle](../design/access-server/dataset/design-dataset-control.md),
[Console UI](../design/console/design-crowdb-console-ui.md).
Goal: deliver Dataset before S3 with real Snapshot/Split inspection and a complete
local import, publication and independent Split revision editing flow.

## Handoff and scope

- Implementation authorized on 2026-10-15. Backend/HTTP/SDK acceptance completed
  on 2026-10-16; Console implementation and browser baselines are next. Read the
  actual API handoff below, not just the initially proposed names.
- Own Console listener discovery/proxy/capability enforcement, Dataset navigation,
  single Sample/Schema matrix, local test/demo import, revision editing and E2E.
  Backend authority/index/selection/composition/publication belongs to R237.
  Do not synthesize these functions by downloading full Manifests in React.
- UI mutations use real services and survive documented failure/recovery paths.
  Test/demo positioning reduces supported sources, not data correctness.
- No external connector UI, training monitor, rich media decoder, SQL/ETL builder,
  physical locator decoder, automatic dataset generation or new metrics dashboard.
  No Dataset deletion; no in-place Snapshot editing; no independent Split pruning.
- Fixed defaults: multiple parentless roots allowed; initial view is all Samples;
  text/hex preview <=64 KiB; 20 rows x 12 fields per matrix window; 20/max100/1 MiB
  list pages; one shared revision for the complete Split set; 32 history visits.
- Preserve concurrent R235 edits. Inspect working tree before touching shell,
  deployment/config or shared tests. Implementation follows repository
  skills/gated requirement commit cadence.

## Dependency handoff checklist

The backend handoff below is verified. The Console acceptance path must consume
these real operations and owned native fixtures.

- [x] **H1 Wire capabilities**: record v2 OpenAPI path, server/SDK versions and
  availability endpoint. Expected support: bounded catalog/inspection, metadata
  preparation status, compositions, branch publication and Split revisions.
- [x] **H2 Publication contract**: record exact candidate/attempt/expected-head
  fields, success receipt and retry/status operations. Record how an explicit
  new commit attempt preserves candidate/parent after head conflict.
- [x] **H3 Composition and revision contract**: record draft expected-revision
  edits, preview progression, source pinning, invalidation, initial revision
  binding, independent revision CAS, abandon and recovery operations.
- [x] **H4 Evidence and fixtures**: record passing dataset.http/dataset.sdk native
  artifact paths and source fixture creation helpers. Verify old identity discovery,
  bounded inspection and same-name Split revision identity against live API.

## Shared UI state contract

### Implemented upstream wire handoff

- Server version 0.3.0; independent Rust/Python SDK version 0.1.0. The contract is
  [OpenAPI v2](../design/access-server/dataset/openapi-crowdb-dataset-v2.yaml),
  base `/api/dataset/v2`; GET `/capabilities` reports configured features.
  Dataset routes use `/namespaces/dataset-ns/datasets/{dataset}`. Uploads,
  composition and Snapshot reclaim require the configured upload service;
  inspection, Dataset management and independent Split revisions do not.
- All u64 wire values are canonical decimal strings. Opaque IDs are nonzero
  32-character lowercase hex; generations are 64-character lowercase hex.
  Sample IDs are canonical padded base64 and remain binary identities.
- Composition POST begins a durable operation with explicit nullable parent,
  all/selection/frozen-Split source and optional pinned inherited Split revision.
  PUT applies at most twenty Sample edits with an edit ID and expected revision.
  GET returns current status; identical historical edit retries return their
  original result. Evaluate advances explicit expected-page windows. Prepare
  advances candidate, initial Split and upload ownership until publication_ready.
- Publish sends `{attempt,snapshot,expected_head}` with required nullable head.
  A successful receipt is authoritative; GET `commits/{attempt}` reconciles lost
  responses. A head conflict requires a reviewed new attempt and observed head,
  preserving the prepared candidate and declared parent. Latest advances only
  on successful data publication; stable is separate.
- Independent Split drafts use expected draft revision and page progression.
  Their publication compares expected Split revision, never Dataset head.
  Historical revision membership remains immutable. Abandon and cleanup are
  separate bounded operations for both kinds of draft; cleanup reports deferred
  physical work explicitly. Status/receipt inspection precedes ambiguous retries.
- Inspection POST matrix takes `sample_ids` and returns `entries`; it transfers
  metadata only. Explicit preview returns state, nullable length/digest/data and
  truncated. Column/schema and row/sample paging remain independent. Native
  HTTP/SDK fixture evidence is recorded below; these contracts also passed standalone
  HTTP and externally installed SDK acceptance.
- Native HTTP restart acceptance passed in
  `.crowdb-runtime/artifacts/container-e2e/run-c4b496a7d5584827a4cbccf4e952fff5`
  (case `access_dataset_split::split_commit_restart_preserves_candidates_receipts_and_historical_members`,
  12.13 seconds). Rust and installed Python native Split flows passed in
  `run-095555cada884960806c49e4a521c547` (case
  `dataset::sdk_split_revision_round_trip`). Both used source
  `b9b0448c85380ce7d77b9144276660c70faad5e9` and image config
  `sha256:8f7142e2104536efe0053a0b1f8d08a90c1e41e3309f9da387d0cd946de13d02`.
  `AccessDatasetCluster` owns the real storage/listener; Rust `split_control`
  and Python `split_consumer.py` demonstrate public-API fixture creation.

- `DatasetSelection` is a discriminated union (new): root, Dataset, Snapshot,
  Split revision/set, named Split, Sample row, Schema field header or Sample/Field
  cell. Carry full parent scope plus immutable Snapshot/revision IDs where needed.
  Header selection never borrows an unrelated Sample ID.
- Domain state owns expanded tree identities, each child-page cursor, matrix
  row/column windows, exact-ID query, selected resource and scroll. Navigation
  snapshots contain only bounded identities/query metadata, no field bytes,
  complete Manifest, upload bodies or credentials.
- Draft state is separate from navigation: authority operation ID, expected draft
  revision, source references, file fingerprints/upload IDs and current action.
  Store bounded non-secret recovery references locally; authority owns complete
  durable draft edits. Browser File objects are not persistence or server paths.
- A selected r2 remains r2 when current becomes r3. Refresh current-pointer badges
  separately; offer explicit Go to current. Reclaimed destinations preserve
  context and show unavailable, never fall forward silently.

## Phase U — dependency-ordered implementation tasks

- [x] **U1 Measure shell baseline and register new acceptance owners**: inspect
  current native browser fixture/selection/credentials paths; run affected shell
  and navigation owner baselines before visible changes. Reserve Dataset `8x`
  specs (Iceberg 6x, S3 7x, cross-domain 9x); register new owned native cases in
  `CONSOLE_OPERATION_CASES`. Files: `e2e-test/scenarios.py`, native fixture/browser
  helpers, `clients/console-ui/e2e/flows/00-shell.spec.ts`,
  `91-navigation-state.spec.ts`, new `80-dataset-browse.spec.ts`,
  `81-dataset-import.spec.ts`, `82-dataset-splits.spec.ts`.
  Exit: every new mutation flow has one fixture owner; proposed case names listed
  below resolve in root runner, no host browser process starts service binaries.
- [x] **U2 Wire configured Dataset service**: add Dataset origin/config discovery,
  server-held bearer use and capability reporting. Audit local/remote/managed
  Access launch, monitor credentials and default ports without changing other
  protocol identities. Add API-prefix-safe v2 proxy/streaming routes with explicit
  GET/status vs mutation enforcement. Files: `app/crowdb-web/src/access.rs`,
  `access/credentials.rs`, new `access/dataset.rs`, `services/access.rs`,
  `services/deployment/`, relevant `lib/crowdb-console-shared/src/config.rs` and
  `deployment/`, monitor credentials/launch only when needed.
  Exit: configured origin only, no browser bearer/arbitrary URL; missing/unready/
  unauthorized are distinct; readonly blocks direct API mutations; managed mode
  supports Dataset data operations without enabling topology mutations.
- [x] **U3 Add domain and typed frontend adapter**: Dataset enum/module/route,
  header order, embedding whitelist/initialDomain, view mount and state provider.
  Define API DTO parsing from R237 and keep exact integer/opaque IDs as strings.
  Files: UI `types.ts`, `App.tsx`, `embed.ts`, `shell/domainTabs.ts`,
  `contexts/{DomainContext,SelectionContext}.tsx`, new `views/DatasetView.tsx`,
  `dataset/{api,types,state,navigation}.ts(x)`.
  Exit: seven actual domains in correct order; preexisting Capacity alias still
  works; no duplicate connection dialog or styles outside Console scope.
- [x] **U4 Build lazy catalog and Snapshot forest**: root lists authoritative
  Datasets; expand Dataset roots then declared child Snapshots independently.
  Include a paged Splits group per Snapshot with selected revision and history.
  Root/Dataset summaries use supplied identity/head/count only. Direct links reveal
  bounded ancestor context; prepared candidates remain in operation progress,
  not readable tree branches. Files: new `dataset/{DatasetTree,CatalogContent,
  SnapshotSummary,RevisionPicker}.tsx`, typed query hooks.
  Exit: SDK-created old/new resources and off-latest branches discoverable; no
  descendant prefetch, fake totals or parent inference from ID sorting.
- [x] **U5 Build combined matrix and properties**: Sample ID sticky column, field
  name/requiredness headers, paged rows/columns and metadata-only cells. Header,
  row and cell clicks produce distinct structured properties. Display inline/
  chunk/tombstone/missing/inherited states, exact length and declared digest.
  Snapshot shows all Samples; named Split uses scoped membership with same Schema.
  Files: new `dataset/{SampleMatrix,FieldProperties,SchemaProperties}.tsx`,
  `dataset/useSampleWindow.ts`, tests.
  Exit: zero-field and wide Schema, UTF-8/binary IDs, empty Split, removed Samples,
  inheritance/tombstones and API partial/limit states render accurately. Browsing
  transfers no payload bytes or whole Manifest; field checksum is labelled declared.
- [x] **U6 Add navigation, limits and explicit preview**: capture bounded visit
  identities/windows/scroll, abort obsolete queries, suspend hidden observation,
  retain primary success when optional metadata fails. Add text/hex Preview action
  for selected cell: check complete known length <=64 KiB before transfer, consume
  verified read framing fully, then display. Unknown length must be resolved or
  rejected; never GET-and-truncate. Files: Dataset navigation/query hooks,
  shared contexts as necessary, new `dataset/FieldPreview.tsx`, proxy read stream.
  Exit: Back/Forward restores exact r2/cell, stale cursor disables Next, corrupt/
  cancelled transfer discards output, keyboard/resize do not reset selection.
- [x] **U7 Implement local import and data publication wizard**: root Create
  Dataset, Dataset Import/New Snapshot, Snapshot Create child and Split Create
  child use one stateful flow. Stages: choose Dataset/source -> Schema/member edits
  -> map local data -> upload -> optional Split definitions -> validate/review ->
  Prepare -> Publish. Separate server draft revision from frozen candidate.
  Files: new `dataset/import/{ImportDialog,SourceStep,SchemaStep,MappingStep,
  UploadStep,ReviewStep,PublicationStep}.tsx`, hooks/reducer and ResourceActions
  integration. Exit: arbitrary historical/parentless source, All/Selection/
  S1-r2-Split source, additions/removals/tombstones and inherited required fields
  reach real authority with complete membership; never submit a browser delta as
  a complete Manifest. Samples/Splits freeze together for initial publication.
- [x] **U7a Specify and validate local mapping format**: use version 1 JSON envelope
  with Schema and Sample/Field mappings aligned to SDK data model; field source is
  inline canonical base64 or local file reference. Sample IDs use canonical base64
  with explicit UTF-8 helper input in UI. A file reference matches a user-selected
  file token/path relative to selection, never grants arbitrary disk access.
  Define missing/duplicate/ambiguous file errors; reject unsupported versions,
  duplicate IDs and fields before uploads. Preserve user input on validation.
  Files: new `dataset/import/mapping.ts`, fixtures/tests and UI user docs.
  Exit: ordinary few-file form works without a mapping file; batch JSON is bounded
  at 8 MiB and produces paged server edits, not an unbounded local registry.
- [x] **U7b Transfer and recover uploads**: bounded incremental file digesting and
  chunked upload, at most 4 MiB per part/10000 parts, default two concurrent part
  requests and four-part retained-buffer cap. Honor stricter server admission.
  Show bytes/part/completion states and exact errors. Reuse immutable receipts on
  identical retry; reselected files must match length/digest before continuing.
  Files: new `dataset/import/{upload,digest,recovery}.ts`, worker if needed,
  proxy streaming, unit tests. Exit: memory does not grow with file size; reload
  recovers server IDs, never assumes lost File handles or changed bytes are valid.
- [x] **U7c Handle prepare/publish conflicts and cleanup**: persist bounded recovery
  identities; use operation status for Outcome unknown. Head conflict presents
  observed change and explicit reviewed new attempt on same candidate; never
  silently switches parent or repeats completed publication. Close does not mean
  abort; explicit abandon shows claimed-upload/candidate cleanup progress.
  Files: wizard reducer/recovery components and HTTP adapter.
  Exit: dropped responses, reload, stale head, edit-after-prepare and cancellation
  each have deterministic state, input preserved and no duplicate Snapshot.
- [x] **U8 Implement independent Split revision editor**: set-level draft supports
  create/edit/rename/remove, explicit IDs/rules, seed/weight partition, independent
  overlap/coverage settings, inherited/frozen vs reevaluated membership. Preview
  uses revision-bound server work and invalidates after edit. Whole-set review ->
  prepare -> publish sends expected Split revision, not Dataset head.
  Files: new `dataset/splits/{SplitEditor,DefinitionEditor,PolicyFields,
  SplitReview,RevisionHistory}.tsx`, reducer/hooks/tests.
  Exit: r2 publishes on S1 without S2 or latest movement; concurrent r1 edit
  conflicts with recoverable input; deleting last Split creates empty r2 and r1
  remains readable. Reuse editor for initial Splits in U7 without duplicating rules.
- [x] **U9 Add Snapshot lifecycle controls and capabilities**: Set stable,
  Retain/Release/Reclaim with precise targets, actual protection reason, pending
  status and reconciliation. Copy/reference actions keep complete identities;
  readonly/managed behavior enforced through both UI and API. Files:
  `dataset/ResourceActions.tsx`, properties/dialogs, proxy tests.
  Exit: no unsupported Dataset delete or Split-history purge; stale eligibility
  never overrides backend checks; optional failures preserve primary view.
- [~] **U10 Run real acceptance and finalize docs**: complete test matrix below,
  record timing/artifacts, update Console architecture/UI contract/domain counts/
  8x test ownership and local mapping examples. Update Dataset integration docs
  only where UI configuration affects them. Files: Console design docs, API-prefix/
  embedding docs, `e2e-test/README.md`, backlog index. Exit: every R236 Acceptance
  bullet verified; then remove completed backlog/plan per requirement workflow.

## UI state transitions to assert

- Import: Editing -> Uploading -> Validating -> Ready to prepare -> Prepared ->
  Publishing -> Completed. Error retains stage-specific inputs; unknown outcome
  reconciles to authoritative Prepared/Completed/conflict. Editing frozen content
  creates a new operation, never reuses its token with divergent bytes.
- Split edit: Editing(base r1) -> Evaluating(draft revision) -> Prepared ->
  Publishing(expected r1) -> Current r2. Stale r1 stays a conflict; no last-writer-
  wins action or automatic rebasing. Explicit review opens a new edit/attempt.
- Create child from Split: pin S1/r2/name once -> draft S2 data -> review inherited
  memberships intersected with S2 -> publish S2 with initial revision. Source r3
  updates may show a notice but cannot change the draft's inputs.
- Resource load: loading/ready/empty/partial/stale/unavailable/limit are explicit.
  An old resource requiring metadata preparation has actionable progress, not an
  endless spinner or empty result. First-page refresh resets incompatible cursors.

## Tests, owners and acceptance trace

### Component and unit tests

- U3: `domainTabs`/Header, `App.test.tsx`, Domain/Selection context tests and
  `test/embedding-contract.test.tsx`: domain order, module exclusion, prefixes,
  independent state, readonly visibility and Capacity compatibility.
- U4-U6: new Dataset component tests: independent lazy branches, binary ID display,
  20x12 windows, Schema/header vs row/cell selection, stale responses, hidden polling,
  explicit preview integrity/size, revision-aware history and focus restoration.
- U7/U8: pure reducers/mapping/transfer tests for invalid inputs, bounded buffers,
  revision invalidation, unknown outcomes, identical retries and source pinning.
  Do not assert only implementation structure; assert user-visible state and
  forbidden network/payload operations. Mutation success requires native tests.

### Console/API integration

- New `app/crowdb-web/tests/dataset_proxy_test.rs`: fixed origin, capability/errors,
  server bearer not returned to browser, streaming/cancel, prefix and readonly
  enforcement. Extend `access_routes_test.rs`, deployment/config tests and
  `managed_mode_test.rs` only for Dataset-specific behavior.
- Verify route method policy against actual v2 draft/status methods; never treat
  every POST as identical (read requests may be POST) or let generic proxy bypass
  readonly writes. No request-selected host redirects or credential leakage.
- Cross-check API request counters for one list page and preview. UI paging must
  not trigger implicit full authority scans, full Manifest reads or read-plan acks.

### Native browser E2E

All names below are planned new owners, not runnable until U1 registration.
Use existing console.operations dispatch; register fixtures/scopes explicitly
if adding a narrower scope. Keep one worker per mutable runtime and installed
system browser only. Seed data with API where the UI action is not under test.

- `console::browser_dataset::browse` / `80-dataset-browse.spec.ts`: discover
  SDK-created Dataset; forest with siblings and stable/latest on different branches;
  lazily page children/Splits; inspect wide matrix, binary IDs, inheritance and
  tombstones; restore scope and scroll after switching to S3/Iceberg/Back.
  Covers U3-U6 and R236 navigation/inspection/bounded observation assertions.
- `console::browser_dataset::import` / `81-dataset-import.spec.ts`: UI creates
  Dataset; maps local inline/payload fields; uploads >one part; defines initial
  Splits; prepares/publishes; verifies actual SDK-read bytes. Then derives child
  from historical parent/Split and checks membership and latest. Covers U7/U8
  local flow, real durability, full membership and initial revision visibility.
- `console::browser_dataset::split_revisions` / `82-dataset-splits.spec.ts`:
  two editors of r1, publish r2, stale conflict, same data Snapshot/latest, r1 read
  remains valid; rename/remove/empty set, partition/policy validation, new child
  from frozen r2 while r3 appears. Covers revision isolation and provenance.
- `console::browser_dataset::recovery` / import/split specs: controlled native
  response-loss/failure fixture, reload after upload/Prepare, reselect file, resolve
  outcome by operation ID, explicit retry/abandon. Ensure only one candidate/new
  revision; retained bytes remain available and partial payload is never success.
- `console::browser_dataset::capabilities` / browse/lifecycle tests: readonly
  direct API rejection, managed data actions, listener outage, unsupported v2,
  reclaim protection and pending cleanup. Destructive checks last. Covers U2/U9.
- Reuse shell `00/01` and cross-function `91-navigation-state` regression owners
  rather than repeating all legacy dialogs. Record exact existing test baselines;
  new tests record first passing baseline with date and no invented previous time.

## Fixture and runtime requirements

- Use one native three-node storage fixture with Dataset listener enabled and
  existing real KV/Chunk-KV/ChunkDB/DiskIO backing. Dataset and S3/Iceberg protocols
  share configured cluster identity; data operations never provision a second
  hidden cluster. Container ports from inspected node IPs and fixed reservations.
- Dataset fixture builders produce 21+ Samples, 13+ fields, multiple roots/siblings,
  two Split revisions, empty Split, inline/tombstone/inherited/chunk values and
  non-UTF8 IDs. Use modest data sizes except explicit bounded-transfer cases.
- API seeding is allowed for browse tests; it does not replace UI upload, prepare,
  publish, revision-edit or child-source actions in their dedicated tests.
- Follow `stepTimer`, 3-second assertions/10-second election and 100 ms polling;
  no fixed sleeps, screenshot-only acceptance, blanket retries or larger default
  deadlines. Print failing case/error and retain full service logs/artifacts.

## Consolidated files

- Console backend: `app/crowdb-web/src/access*`, services Access/deployment/config,
  scoped console-shared/monitor credential wiring and crate tests.
- Frontend: `types.ts`, `App.tsx`, `embed.ts`, shared shell/contexts, new
  `views/DatasetView.tsx` and `dataset/{api,state,navigation,tree,matrix,import,splits}`
  responsibilities with facade/children as appropriate. Keep modules near repository
  size guidelines; do not add a monolithic Workbench or second generic state framework.
- Tests: component tests alongside TS components, Rust integration in crate tests,
  e2e owned `80/81/82` flows, shared 00/01/91 regressions, scenarios/native fixtures.
- Docs: Console architecture/UI, mapping-file usage and E2E ownership. R237 owns
  Dataset persisted semantics/OpenAPI/SDK docs; modify only when an actual agreed
  contract correction is needed, not to accommodate a frontend shortcut.

## Commands and release gates

Task names verified by `pixi task list`; no gate is claimed passed during planning.
Use 60-second shell observation windows/background polling for long builds. All
executables run through Pixi. Before server-spawning tests, use clean-env only for
the owned runtime; coordinate rather than interrupt another model's active test.

```sh
pixi run test-console-ui
pixi run cargo test -p crowdb-web --test access_routes_test
pixi run cargo test -p crowdb-web --test managed_mode_test
pixi run test-console
pixi run test-container-e2e-fixture
pixi run test-container-e2e --scope console.operations --list
```

After U1 adds the owner, run the exact changed flow (repeat for each affected
registered Dataset owner, not just browse):

```sh
pixi run clean-env && pixi run test-container-e2e --scope console.operations --case console::browser_dataset::browse --concurrency 1 --node-cpus 0.25 --cpus 8 --memory-mib 16384
pixi run clean-env && pixi run test-container-e2e --scope console.operations --case console::browser_dataset::import --concurrency 1 --node-cpus 0.25 --cpus 8 --memory-mib 16384
pixi run clean-env && pixi run test-container-e2e --scope console.operations --case console::browser_dataset::split_revisions --concurrency 1 --node-cpus 0.25 --cpus 8 --memory-mib 16384
pixi run clean-env && pixi run test-container-e2e --scope console.operations --case console::browser_dataset::recovery --concurrency 1 --node-cpus 0.25 --cpus 8 --memory-mib 16384
pixi run clean-env && pixi run test-container-e2e --scope console.operations --case console::browser_dataset::capabilities --concurrency 1 --node-cpus 0.25 --cpus 8 --memory-mib 16384
```

Final relevant integration/regression and static gates:

```sh
pixi run clean-env && pixi run test-container-e2e --scope console.operations --concurrency 1 --node-cpus 0.25 --cpus 8 --memory-mib 16384
pixi run rs-fmt-check
pixi run rs-lint
pixi run ts-lint
```

If shared API/SDK code changes during R236, rerun its affected R237 gates too;
otherwise consume recorded passing backend acceptance. Build native image from
the actual changed checkout before browser acceptance using existing image workflow;
record image identity and source revision. An old image passing is not evidence.

## Evidence and completion checklist

- Planning: current UI spec, Iceberg patterns, domain/history, proxy, Dataset model,
  SDK and runner reviewed. Backend acceptance complete; H1-H4 verified.
- Baseline timings and fixture/image identities: pending.
- Component/API/native case results and artifact locations: pending.
- All task exits and upstream acceptance cases verified, modes/accessibility/
  bounds/failure recovery included: pending.
- Permanent docs updated and no dangling R237 cleanup links: pending.
- Remove completed requirement/index entry/plan only after full acceptance, under
  implement-requirement cleanup. Persistent failure follows five root-cause attempts;
  record Blocked only when its stated conditions apply.

### Console baseline — 2026-10-16

- `console::shell::domain_toggle` passed, Playwright 1.7 seconds; artifact
  `run-d40d1530dbec4349b10fca86f590c80f`.
- `console::native_browser::storage_windows_and_access_recovery` passed four
  browser cases; navigation/history baseline was 7.0 seconds, whole browser phase
  49.8 seconds. Artifact `run-c80bf1892d1b448f877aafc4cef4a197`.
- Both used the verified backend image before visible Console changes. U1 new
  Dataset owner registration remains pending. U2 proxy/deployment implementation
  is active; no Dataset UI acceptance is claimed.

### Console transport verification — 2026-10-16

- U2 configured origin and private derived Dataset bearer are wired through Access,
  monitor, local/remote deployment and durable listener overrides. Readonly permits
  exact matrix observation and rejects mutations; metadata responses remain capped
  at 1 MiB and upload requests at 4 MiB. Browser adapter uses explicit bounded preview.
- Passed Dataset proxy (2), service plan (2), monitor credential (5), node setup (6)
  tests, root Rust format and Clippy. Prior affected access/managed tests also passed.
- UI first adapter/matrix implementation is in progress. Existing UI suite passed
  254 tests and frontend type check; no new Dataset browser acceptance yet.
- `pixi run ts-lint` exposes a pre-existing task path error: it references removed
  `app/crowdb-web/ui/tsconfig.e2e.json`. Correct to the owned browser client config
  before the final gate; frontend types are separately checked by test-console-ui.

### Dataset workbench implementation — 2026-10-16

- U1 registers five owned native Dataset cases and initial 80/81/82 flow specs.
  Actual browser acceptance is pending; the runner correctly rejected the first
  attempt because release image provenance requires committed server/UI sources.
- U3-U9 first implementation includes Dataset domain/module/query selection,
  bounded forest/matrix/history, explicit preview, local mapping/MD5/parts,
  composition and complete Split revision editing, receipts and lifecycle actions.
  These are implementation checkpoints, not completed acceptance tasks.
- Current component suite passes 257 tests including RFC MD5 vectors, binary ID
  identity and ambiguous mapping rejection. TypeScript frontend passes. Native
  tests and remaining failure-recovery/capability coverage will drive refinement.
- Fixed the stale ts-lint path by staging the same client-owned layout and three
  shared serializers/types as the native runner. Included DOM.Iterable in its
  complete client config; no assertion or error is suppressed.

### Acceptance refinements still required

- Native HTTP origins are insecure contexts: use the existing `randomUUID`
  fallback and incremental mapping digest instead of assuming crypto.subtle /
  crypto.randomUUID. Add a regression before accepting browser import.
- Complete recovery for failed begin (no draft response), resumed mapping/upload,
  definite rejection vs ambiguous mutation, Split cleanup and prepared editing.
  Prevent invalid Selection JSON from keeping a previously valid rule silently.
- Current UI calls GET Snapshot reclaim status, but v2 exposes only POST. Add a
  bounded lifecycle observation contract (retained/retiring/completed and observed
  protection), tests and API docs; do not infer reclaim eligibility from UI data.
- Manual Access/service-plan/node-setup forms still need Dataset port after their
  exact existing baselines. Dataset API defaults are already supplied.
- Expand real tests beyond initial happy paths: multipart payload, historical
  branching/independent root, binary ID/tombstone/inheritance, stale Split CAS,
  frozen Split child, lost response, cancellation, read-only/managed/embedding.
- Native runner accepts one `--case`; use one invocation per owner or the registered
  `console.native.browser.dataset` scope. Repeated --case arguments select only
  the last. First running owner is capabilities, not all seven listed arguments.
- Fixture unit suite first found only two expected inventory-count assertions
  (154 -> 159 from five new owners); updated counts, focused selection tests pass.

### First native acceptance corrections — 2026-10-16

- First five-owner run: capabilities passed (2.7 s); browse failed at a 5 s
  prepare deadline; import/recovery exposed duplicate React sibling keys; Split
  creation exposed insecure-context UUID use. Artifact run-17846a6cb1c74000978ea73168aa57eb.
- Restored 60 s mutation budget while v2 metadata remains at most 5 s. Unique
  Actions key fixes duplicate DOM; native assertion checks one Actions region
  before and after refresh. UUID uses existing fallback, mapping hash uses MD5.
- Added bounded lifecycle GET with Rust/Python SDK and OpenAPI contract. HTTP
  observation and Split contract tests pass (3). Root fmt/Clippy, SDK fmt/Clippy,
  SDK tests, independent Python wheel consumer (10), UI tests (257), TypeScript
  and fixture unit tests (150) pass at this checkpoint.
- Exact Access form baselines: auxiliary_dialog_defaults 5.6 s and access_plan
  2.2 s. Added Dataset port and updated corresponding assertions. Native rerun pending.
- Added begin-response recovery, Split scratch cleanup, invalid-rule input
  validation, weighted group rename consistency, metadata error messages and
  activity module control. Remaining advanced native scenarios are not yet accepted.

### Recovery and native flow expansion — 2026-10-16

- Image revision 13a2de838bacfa30e1d89690db342cf4b337569c, config
  sha256:77300ff4537cbb4f144d47a3007e6dff41232fdea235cf85d74bf72be41e73ca.
  Native run-807474b8a38442bd9bea597f2bba0b24: import 6.5 s, Split revisions
  8.2 s, reload recovery 5.9 s, capabilities 2.4 s passed. Browse progressed
  past preparation but exposed selection kind overwritten by spread scope;
  fixed construction order, preserving the original header/row/cell assertions.
- Added bounded selected ancestry, explicit legacy inspection preparation,
  metadata byte-sized edit windows, source-pinned inherited Split definitions,
  initial Split set editing, cancellation and recorded upload cleanup, replacement
  operation after explicit abandon/cleanup, and known-rejected creation recovery.
  Optional lifecycle failure now preserves Snapshot summary.
- Expanded native owners for multipart payload/MD5/binary IDs, inherited/tombstoned
  child data, competing Split editors, historical frozen-source child, readonly
  reverse proxy/module exclusion, and real upstream publication-response loss
  followed by reload/reconciliation. No synthetic service responses are used.
- UI tests pass 262 with editor uniqueness, invalid rule, partition rename,
  readonly and byte-budget regressions. Expanded native acceptance is pending.

### Contract detail checks during full Console regression — 2026-10-16

- Full Console operations started on image source 9f924107287378ad664b29eacd9a925b6c1220df,
  config sha256:6ca6eef7b05986eb68f832668e2d0e51ff384b0e3efb5aa1e2110648f5c4d06f,
  artifact run-d8d8be2ccd134b29941341fce26b990b. Initial service/recovery cases pass;
  browser and overall result still pending. Effective case admission is one slot.
- Added the remaining explicit Split-plus-rule source option, selected historical
  revision as editor base, Go to current revision, separately selectable canonical
  Sample identity, and initial Split validation details. Matrix selection no longer
  reloads an unchanged window; rule fields resynchronize when removing a sibling.
- Identical retry now records a newly observed confirmed publication conflict, so
  head review remains available after an initially unknown response. Added tests
  for this transition, exact reclamation confirmation targets and matrix selection.
- Rust/Python native consumer examples now assert the lifecycle observation route;
  SDK Clippy passes. Rerun their actual native consumer owner before final cleanup.
- These UI refinements postdate the running full-domain image and require the final
  focused Dataset owner rerun on a newly built matching image.

### Final verification in progress — 2026-10-16

- Latest UI suite: 265 tests and TypeScript checks passed; artifact
  `unit/ui-eaf7ffd28a7c4a84ba27e1bb3f94ee83/results.json`. Root `ts-lint` passed.
- Latest matching image source `a700daa3787705829aa85c85fcff0cf55b1381ac`,
  config `sha256:6fca70d0f2cbb85e8d6511da6e2a00fece779cf983dc7044e441f0eae3b7824d`.
- Full Console regression exposed existing hardware test mismatch:
  `console::web_authority::bare_metal_hardware_routes_share_confirmed_group_zero_state`
  fails at `web_authority.rs:264`, deleting `/dev/test`: expected 204, actual 409.
  Guard added by preexisting `dc4cdf2b` rejects physical disk deletion without
  verified Offline/Remove lifecycle; the test still dates from `5e54be81`.
  Dataset changes do not alter this handler or guard. Keep the safety guard and
  original assertion; record this unrelated regression separately from Dataset
  acceptance. Full run and latest Dataset/SDK native checks remain pending.
- Native SDK rerun passed on the latest image: Rust example and independently
  packaged/installed Python consumer, including lifecycle observations;
  artifact `run-073a4b9dfea74e168b95ad2ac2267d5e`. Extracted sdist Rust tests passed.
- Latest root Rust/SDK fmt and Clippy and OpenAPI local-reference validation pass.
- Full run browse (13.7 s) and capabilities (4.7 s) passed. Actual browse screenshot
  inspected: seven-domain order, parent tree and combined matrix/properties are
  visible without overlapping panels. Latest image browse also passes (15.8 s).
- Import/Split native traces exposed bootstrap ChunkKV split/transfer overlapping
  browser actions: final evaluation took 2993 ms plus status GET, and Split
  publication was still pending after 3 s. Latest import reproduced a different
  slow mutation during the same startup placement. Dataset fixtures now reuse
  the native Journal initial-placement readiness check; original 3 s UI assertions
  remain unchanged. Exact import rerun pending.
- Recovery fault injection originally closed before response headers, allowing
  Chromium to transparently replay the POST and receive the successful receipt.
  Proxy now sends actual headers and one actual body byte before disconnecting,
  exercising partial/lost responses without fabricating server state or JSON.
- Additional unrelated full-regression mismatches use removed physical-media
  entry points: six_node_round_trips expects Add Disk menu; fixed_ports_and_failed_retry
  posts unverified device_path and receives the existing 409 owner-verification guard.

- Expanded import passed (14.1 s), artifact `run-23c2adbac4a143d89605c73fb5055f6b`: multipart digest, publication, child data preservation, tombstone and inherited Split intersection. Composition copies effective bindings into the child, so source metadata correctly identifies the child; parent and child preview bytes are asserted equal. Sparse-Manifest ancestor source remains covered by `inspection_pages_test`.
- Expanded Split reached successful frozen-r2 child publication with two selected members and historical editor-base checks. Intermittent global browser GET stalls remain under diagnosis; native network logging is temporary and will be removed.

- Native run `run-45afb606e2ab447ea8a150b533e6c082` found a genuine editor race: Add Split was enabled before the asynchronous base revision arrived, whose response then erased the new field. Split editor now keeps edits/save disabled until base loading succeeds, with a deferred-response component regression. Temporary browser netlog instrumentation was removed.

- Chromium NetLog isolated global GET stalls to host automatic proxy discovery, before request transmission. In `run-d152bd9a3752460586c957cbcff340b2`, capabilities request source 1110 waited in source 1111 `PROXY_RESOLUTION_SERVICE_WAITING_FOR_INIT_PAC` for 3020 ms; the actual server response took 25 ms. Owned local browser fixtures now use `--no-proxy-server` for both executable and channel launches. Original 3 s assertions remain intact; no application retries or latency allowances were added.
