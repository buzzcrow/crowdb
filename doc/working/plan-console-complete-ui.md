<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Complete Console UI Plan

Upstream: [R203](../backlog/R203-console-complete-ui.md).
Goal: seven domains with explicit scope, real operations/diagnostics, persistent
standalone bootstrap, and the same UI in Container with topology writes disabled.

## Baseline and scope

- User approved the seven-domain design and authorized planning and implementation.
  The Console UI specification defines the interaction contracts and acceptance scenarios; this plan records implementation gaps.
- Commit each tab's verified implementation separately before proceeding to the
  next tab. Shared adapters belong with the tab that introduces them; remaining
  acceptance gaps stay explicit in this plan.
- The user is redesigning ChunkDB range ownership and routing. Defer that
  integration and retain the simple Chunk browse/detail/placement flow against
  existing APIs. Do not redesign the backend, hard-code Group 0 as the permanent
  Chunk store, or present current routing as validated range distribution.
  The user's observation that chunks currently collect in Group 0 is context
  for the deferral, not a verified storage contract. Reconnect the UI when the
  new range contract is available. This deferral does not cover the separate
  Chunk-KV split/tree/journal workbench or response/rendering bounds.
- Preserve the user's running localhost cluster and its default directory.
  Browser inspection is read-only; tests must use isolated runtime roots/ports.
- Initial Iceberg scope is Namespace/Table metadata CRUD. Row DML and object-to-
  Chunk reverse lookup remain human decisions in R203; other work proceeds.
- Existing startup/persistence edits are uncommitted and need integration/lint
  verification; three initial startup tests passed before the last scope changes.

## Phase 1 — startup and shell

- [x] **Standalone recovery**: finish atomic local persistence and process recovery,
  validate Group 0 authority and multi-node/DiskDB behavior, retain configured
  process identity. Files: `app/crowdb-web/src/{main,state,standalone}.rs`,
  `src/standalone/recovery.rs`, `src/diskdb/lifecycle.rs`, `src/mgmt/cluster_init.rs`,
  `tests/standalone_startup_test.rs`. Acceptance: A1–A5.
- [x] **Five-domain shell**: typed domain identities, per-domain scope and status,
  health/empty/error distinction, header and pane reuse. Files: `ui/src/App.tsx`,
  `types/index.ts`, `contexts/`, `shell/`, `views/`. Acceptance: A1, A6, A10, A23.
- [x] **KV/Cluster boundaries**: Cluster layout stops at physical services; KV left
  tree and init/operator scope own logical resources; guard Group 0 writes and
  demo. Files: `App.tsx`, `topology/`, `panels/KvOperatorPanel.tsx`, Web KV routes.
  Acceptance: A7–A9, A22–A23.

## Phase 2 — Chunk diagnostics

- [x] **Typed Chunk queries**: connect routed ListChunks/detail to Web, explicit
  prefix/paging and placement enrichment. Files: chunkdb client, Web Chunk routes,
  console shared ops. Acceptance: A12, A14.
- [x] **Chunk view**: list/scope sidebar, actual types and stable Strip sequence,
  Mirror/EC fragment cards, bounded rendering and cross-domain links. Files:
  `views/ChunkView.tsx`, new Chunk domain components/types/API. Acceptance: A13–A15.

## Phase 3 — native Access operations

- [x] **Access adapter**: validated deployment endpoint, capability/role handling, typed
  Iceberg REST and signed S3 requests, bounded transfer and error semantics.
  Files: new Web access domain modules, shared access operations, UI API/types.
  Acceptance: A16–A20.
- [x] **Iceberg UI**: Namespace/Table tree, metadata forms, Schema/Snapshots/Files,
  truthful capability gaps, explicit scope and demo resources. Files: new Iceberg
  view/domain components and tests. Acceptance: A16–A17, A23.
- [x] **S3 UI**: Bucket/prefix tree, paged object CRUD, bounded preview and streamed
  upload/download, multipart/cancel state and demo. Files: new S3 domain view/
  components/tests. Acceptance: A18–A20, A23.

## Phase 4 — managed deployment and verification

- [x] **Container integration**: convert confirmed managed snapshots to shared UI
  models, server-enforced capability boundaries and real logical/data operations.
  Files: Web managed routes, UI mode loader, Container web acceptance.
  Acceptance: A11, A21.
- [x] **Focused gates and commits**: measure affected baseline E2E specs before
  changes; run isolated acceptance, Rust fmt/clippy, TS lint and affected E2E;
  commit coherent verified tasks. Preserve live persistent services.
- [ ] **Permanent design and cleanup**: update Console/UI architecture for delivered
  scope. Remove R203/index/plan only after its full accepted scope is delivered.

## Seven-domain implementation sequence

- [x] **Independent domains**: split Capacity and Chunk in the enum, header,
  embedding URL parser, Sidebar/selection dispatch, and placement links. Add
  Chunk-KV as its own workbench. `domain=Chunk` now means the Chunk explorer;
  existing capacity links and fixtures must use `domain=Capacity` explicitly.
  Files: UI `types`, `main`, `App`, `shell`, `views`, `topology`, `chunk`.
- [x] **Chunk-KV catalog**: bounded catalog-page endpoint pinned to head
  generation, exact identities, unavailable states; present range map and selected
  partition artifact/overlay. Resolve placement only from matching known service
  endpoints; unmatched placement remains explicit. Head/page limits: 1/2 MiB;
  at most 100 returned partitions; five-second observation deadline.
  Files: Web `chunk_kv.rs`, UI `chunk-kv/`, `chunk_kv_catalog_test.rs`, E2E 55.
- [~] **Chunk-KV runtime observation**: add authoritative server placement and runtime
  observations, then bounded tree/journal inspection without reading all pages.
  Files: Web `chunk_kv/`, UI `chunk-kv/`, protocol/client observation adapters.
- [x] **Paxos overview**: default KV to Group/Replica management; retain Data
  subview with explicit scope and Group 0 protection. Files: `views/KvView`,
  KV panels and corresponding topology/data E2E specs.
- [x] **Typed services**: consolidate all six service lifecycle operations in
  Cluster, eliminate non-KV fallthrough, add validated deployment forms and
  current-cluster dependency inputs. Files: Web lifecycle domain, shared launch
  adapters, `useClusterMenus`, deploy dialogs, physical server projections.
  Local deployment/lifecycle and UI dispatch are implemented and tested;
  remote auxiliary deployment is explicitly unsupported.
- [ ] **Native deployment acceptance**: exercise Chunk-KV/Access deployment
  through the new routes and close launch/publication crash-recovery gaps.
- [ ] **Chunk sources — deferred by user**: reconnect range ownership and actual
  Repo metadata routing after the backend redesign; then implement per-source
  cursors and coverage. Keep the current type/prefix query, exact-ID detail,
  Strip layout and disk/node links as the interim functional flow. The native
  browser flow has verified these operations on the isolated preview; it does
  not validate multi-range distribution. Files: Web `chunk`, ChunkDB/Chunk-KV
  metadata adapters, UI `chunk`.
- [x] **Capacity and Access**: distinguish unknown usage, suspend inactive
  polling, connect S3 and Iceberg to the same cluster's Access deployment;
  finish bounded native file inspection and real data acceptance.
- [ ] **Capacity population bounds**: replace eager node/disk topology fanout
  with bounded scoped reads and rendering. The current usage poll is still a
  cluster-wide observation while Cluster or Capacity is visible.
- [ ] **Integration acceptance**: scope restoration, layout-to-disk/node
  navigation, split parent/child overlays, owner movement, stale cursors, large
  populations, unavailable backend, and capability restrictions. Verify every
  visible step against the affected browser specs; commit coherent gated work.

Baseline before domain separation: shell embedding 5 tests passed; Chunk layout
2 tests passed (0.888 s and 0.814 s). Browser command:
`pixi run bash -c 'cd app/crowdb-web/ui && npx playwright test --config=e2e/realBackend.config.ts e2e/flows/00-shell-embedding.spec.ts e2e/flows/54-chunk-layout.spec.ts'`.

## Verification commands and coverage

- Unit: scope transitions, prefix/type parsing, Strip layout identity, capability
  combination, safe error/value presentation, request bodies.
- Integration: `standalone_startup_test`, `managed_mode_test`, DiskDB lifecycle,
  Chunk query adapters, Access protocol/credential/streaming paths.
- E2E: shell/embedding, Cluster physical boundary, KV init/operator, Chunk scope/
  placement, Iceberg and S3 functions, Container read-only hardware with writable
  authorized data. Use existing installed system browser.
- Commands are recorded in R203; append actual outcomes here per phase. No full
  UI suite unless specifically needed/authorized; focused specs first.

## Results

- Chunk-KV storage checkpoint: added opened Tree checkpoint and constant-cost
  native memory/page plus maintenance counters; bounded Journal manifest views
  show trim, sealed tail, active chunk, and 100 extent-page fences per window.
  Continuations require stream generation and replace the rendered window.
  Root-catalog/pack statistics were excluded because the existing native API
  traverses retained metadata; it is not a bounded diagnostic read.
- Chunk-KV baseline: 2 browser cases, 0.918/0.236 s. Updated 3 cases passed
  (1.3/0.217/0.509 s), including exact u64 counters, 100+5 replacement windows,
  selected-fence clearing, stale manifest rejection, oversize rejection and
  refresh from head. TypeScript passed. Stream inspection test passed without
  changing read/publication counters; 2 owner tests passed, including a real
  native Tree with pending mutations remaining unflushed. Group 0/Web integration
  passed generation/offset forwarding and mismatched continuation rejection.
- Remaining Chunk-KV work: management endpoint discovery for managed profiles,
  actual bounded Tree page and extent-record inspection, chunk links, retention
  and transition inspection. Extent-page fences are not decoded journal records.
- Rust fmt and all-target Clippy passed for Web, Chunk-KV Server, Chunk-KV and
  Chunk Stream. Release Web/Chunk-KV Server built successfully. Only Web was
  restarted on 9090; its health check passed and all six original KV/DiskDB
  process identities remained unchanged.
- Read-only live Capacity verification found a misleading global failure badge
  for unavailable scan status. Capacity now reports the missing observation
  source and Degraded status; focused scanner regression passed (0.751 s).

- Capacity checkpoint: shared inventory/usage coverage determines known totals
  at every scope and in the Inspector. Missing reports retain hardware and show
  Unknown; free bytes use the reported value, including reserved-space gaps.
  Missing scanner status is distinct from a scanner that has never run.
- Capacity has one completion-paced poll, canceled outside Cluster/Capacity or
  when the document is hidden. Aborted results cannot overwrite later samples;
  individual endpoint failures preserve successful observations.
- Capacity browser baseline: 14 passed; updated 50–53 selection: 15 passed
  (44.7 s command time). New unknown/partial coverage case: 0.874 s. Four
  focused unit cases and TypeScript passed. Existing live datacenter totals
  still conditionally skip when that fixture publishes no usage.
- Lifecycle timing investigation: isolated 2.8 s against 2.7 s baseline;
  ordered trace located 4.1 s server/topology requests behind the delete DOM
  assertion. The preceding bind test left Store 590 installed. Added cleanup;
  lifecycle then took 3.8 s. DiskDB setup now reserves all three ports and waits
  for its actual returned endpoint to appear in the registry. Run topology
  assertions before the shared fixture's final process stop/restart sequence.
  Final disk-group spec: all five passed, lifecycle 2.6 s; focused unit and
  TypeScript gates passed again after fixture changes.
- Follow up native KV restart readiness separately: in one diagnostic run the
  process/health checks passed but a subsequent Group 0 operation kept dialing
  an unavailable RPC endpoint. This Capacity checkpoint does not establish
  post-restart Group 0 recovery correctness.

- S3 fixed-cluster checkpoint: removed the endpoint editor and resolve the
  Access origin on domain entry with abortable discovery and explicit retry.
  Native SigV4 session credentials are retained. XML metadata is capped at
  4 MiB, bucket rendering at 100/page, and accumulated object/upload lists at
  1,000. These limits remain visible; object prefixes narrow later browsing.
- S3 baseline: 2 browser cases passed (0.683/0.609 s). Updated S3/Iceberg
  selection: 8 passed; existing S3 cases 0.648/0.585 s, deployment retry
  0.318 s, population/XML bounds 2.6 s. TypeScript passed. The managed native
  KV/Iceberg/S3/Chunk acceptance flow also passed in 4.5 s with these assets.

- Iceberg file-inspection checkpoint: Avro manifest lists/entries and Parquet
  footer, row-group and column metadata are rendered as fields and layouts.
  Four Iceberg browser cases passed (0.576/0.952/0.321/0.521 s), including
  exact snapshot IDs, replacement pages, no right property panel, automatic
  reader entry and separate native write-token authorization/clearing.
- Native acceptance used the user's TPC loader in the isolated persistent
  preview: TPC-H SF 0.001, eight tables. All eight chains reached their actual
  Parquet footers through snapshots, manifest lists and deflate manifests.
  Lineitem contains 6,005 rows and 16 columns; its 218,725-byte file has a
  2,319-byte footer. Inspection reported zero logical data-page bytes.
  The SDK's location without a trailing slash exposed an identity-check bug;
  authoritative Table ID now determines membership, with a regression test.
- Loader acceptance used an isolated PyArrow FileIO adapter to check exact
  object existence with a read because its default missing-path fallback uses
  unsupported directory listing. Normal credentials, upload checksums and
  footer verification were retained. A failed preflight probe was retained as
  a cleanup candidate in the loader report; this is not a fully clean loader run.
- Managed native browser flow passed in 4.5 s: KV, Iceberg create/inspect/clean,
  S3 object preview, Chunk physical placement and object cleanup. The first run
  still selected the removed Chunk subtab; the test now uses the top-level
  domain and 3-second action deadlines. Its exact leftover S3 demo was removed.
- Iceberg Rust gates: 9 Parquet metadata tests, 10 schema/statistics tests and
  2 HTTP inspection tests passed; all-target Clippy and formatting passed.
  Parent-proof rescans and independent backend column pagination remain bounded
  limitations; the TPC run is functional acceptance, not large-population proof.

- Cluster typed-service checkpoint: local CDB, DiskIO, Chunk-KV and Access
  deployment forms, stable service identities, tree/canvas/properties and
  typed stop/restart/remove routes are implemented. Lifecycle integration:
  4 passed, including a real CDB deployment against Group 0; private launch
  environment/restart: 2 passed; Access routing: 8 passed. Web/shared/Monitor
  all-target Clippy and Rust formatting passed.
- Cluster browser acceptance: 5 passed, including existing real KV deployment
  and cascade flows and the four auxiliary forms (2.4 s). Frontend TypeScript
  passed. Native Chunk-KV/Access deployment via the new forms, crash recovery
  between launch and publication, and overall topology population bounds remain
  integration work. These results do not mark the full redesign complete.

- Focused standalone/managed/DiskDB startup verification: 10 passed, including
  three-member Group 0 recovery, DiskDB replay with preserved custom ports, and
  stale local Rack cache replacement from Group 0. Invalid test profile corrected
  from `fast-test` to existing `e2e`; built missing DiskDB binary before rerun.
- Shell baseline on installed `/usr/bin/microsoft-edge`: embedding 1.6s,
  domain transition 0.372s, Docker writes 0.377s; all 5 tests passed. An initial
  attempt pointed at absent Google Chrome; no browser installation was needed.

- Initial `pixi run cargo test -p crowdb-web --test standalone_startup_test --
  --nocapture`: 3 passed; later sealed-intent/DiskDB recovery edits still need
  focused verification.
- Running UI inspected: Cluster duplicates logical Store/Group/Replica under KV
  Server; KV already uses logical navigation; Chunk currently repeats capacity
  topology. Preserve existing successful operations while unifying these scopes.

## Delivery and verification — 2026-10-03

- Five shared domains implemented. Cluster stops at services, KV owns logical
  resources, Capacity owns disks and real Chunk/Strip placement, Iceberg uses
  native Catalog metadata operations, S3 uses signed native object operations.
- Access does not currently register public origins in Group 0. Container uses
  profile-provided public origins; standalone saves validated origins as local
  launch inputs. Native credentials remain browser-session inputs.
- Recovery now confirms live KV registrations before reporting readiness. The
  first implementation exposed an unseeded discovery client; initializing the
  shared operation context fixes that without caller retries or relaxed tests.
- Final focused Rust checks: standalone startup 4, managed mode 5, Access routes
  6 passed. Earlier KV/DiskDB and Monitor focused tests also passed.
- Frontend: TypeScript lint and 89 unit tests passed. Focused shell, Cluster CRUD,
  server lifecycle, cross-jump, KV CRUD/demo, Capacity, Chunk, Iceberg and S3
  browser checks: 29 passed. Final authorization/credential-scope changes:
  shell/Iceberg/S3 7 passed.
- Real native S3 against cached MinIO: signed bucket CRUD, special object key,
  9 MiB multipart round trip and cleanup passed (2.4 s).
- Real isolated CROWDB managed service chain: authenticated KV CRUD, Iceberg
  metadata demo, S3 demo, actual S3 Chunk/Strip/disk placement and direct
  hardware/Init/Reset rejection passed (4.6 s).
- Rust fmt and Web/Monitor all-target Clippy with warnings denied passed.
- Production release Web restarted at localhost:9090. Existing KV PIDs
  68190/68329/68442 and DiskDB PIDs 68226/68365/68479 were preserved.

## Remaining acceptance work

- Docker image acceptance remains unverified: `pixi run build-single-node-container`
  built/staged the runtime successfully, then Docker Hub base-image retrieval
  failed because the configured proxy 192.168.31.238:7897 refused connection.
  The isolated host-native managed chain validates shared UI/backend behavior,
  but does not replace image packaging acceptance.
- Existing Capacity test skips its totals comparison when DiskDB does not publish
  usage in its fixture. Scanner/maintenance controls pass; that skipped condition
  is not proof of totals correctness.
- Atomic config publication failure and sealed bootstrap-intent replay now pass
  focused regressions. Extend coverage to process interruption during Group 0
  publication; not every interruption boundary has been exercised.
- Extend Chunk acceptance with multi-owner partial recovery and layout-change
  races; current coverage combines validation, a Mirror/EC UI fixture and a real
  routed S3 Chunk from the managed chain.
- Multipart part inspection now uses bounded native marker pages. Extend
  cancellation/unknown-result fault coverage before closing A19 in full.
- Keep R203 and this plan until the remaining acceptance work and the backlog's
  product questions are resolved. Do not claim all A1–A23 complete.

- Added bounded 20-strip pages with stable selected sequence, multipart part
  marker navigation, credential-change scope clearing, and native disk type
  mapping including ZoneSSD/SMRHDD. Final focused checks recorded below.

- Final new browser regressions: Chunk windowing and S3 part marker/credential
  scope checks, 4 passed; native managed chain rechecked after final assets,
  1 passed. Isolated test supervisor stopped after verification.

- Final startup tests: 5 passed, including non-writable publication preserving
  previous bytes and restart consuming sealed intent to initialize Group 0.
- Live Capacity inspection: physical disk topology survives, but Group 0's
  DiskDB instance query returns `[]`. Existing DiskDB logs repeatedly report
  `owned disk-group has no bind dg_id=1`; no runtime/config data was changed to
  conceal this condition. Native managed acceptance has valid registration/binds.
  Investigate this existing standalone deployment state separately.

## Iceberg file inspector — approved design, 2026-10-03

- [ ] **Inspection service (partially implemented)**: add authenticated, generation-bound reference
  inspection to Access table routes; reuse ManifestListReader/ManifestReader and
  footer parsing with bounded work and responses. Extend footer diagnostic fields.
  Files: Access `iceberg/inspection/`, table routes, Iceberg `file/parquet/`.
- [ ] **Explorer UI**: retain Catalog operations and replace primary JSON views
  with the reference tree, structured metadata and Parquet layout/column detail.
  Use two columns for Iceberg: no right property panel; refresh/table actions and
  column details belong in the center.
  Files: Web `ui/src/iceberg/`, `views/IcebergView.tsx`, native JSON transport.
- [ ] **Focused acceptance**: parser/integration tests, Iceberg browser spec,
  isolated TPC loader data, fmt/Clippy/TypeScript. Baseline existing Iceberg
  browser case: 0.732 s. Preserve the persistent localhost deployment.
- [ ] **Document and commit**: update Console UI architecture and record results;
  keep the broader requirement/plan open for unrelated remaining acceptance.

Tests: Iceberg footer metadata tests; Access table inspection HTTP tests; Web
Access proxy tests; `e2e/flows/60-iceberg-catalog.spec.ts`; real TPC metadata chain.

- Iceberg two-column layout verified: the right panel is absent, table refresh
  and actions remain in the center. The browser spec passes both catalog and
  file-inspection cases (0.778 s / 0.969 s): exact 64-bit snapshot identity,
  100-entry replacement pages, signed continuation history, selected footer
  retained during tree paging, and credential-change clearing.
- Frontend inspection responses are capped at 4 MiB; four branch pages and
  32 previous cursors are retained. Structured collections and schema tables
  render bounded pages. Inspection admission reserves 64 MiB per parser through
  the existing atomic 128 MiB budget. Full native data-chain acceptance remains
  pending; the browser fixture is not evidence for native parser completeness.

- Fixed-cluster entry verified: opening Iceberg automatically loads Catalog and
  namespaces without endpoint/token fields or a Load action. Read credentials
  stay in Web process state, are injected only for GET/HEAD, and cannot be
  redirected by the configuration API. Native mutations retain authorization.
  Proxy tests: 7 passed. Browser cases: 3 passed (0.688/0.893/0.343 s).
  Web/Monitor all-target Clippy and frontend TypeScript passed.


## Seven-domain checkpoint verification

- Frontend TypeScript and 29 focused unit tests passed. Shell, Chunk layout, and
  Chunk-KV E2E: 9 passed. Existing Chunk cases took 0.870 s / 0.488 s, within
  baseline. New Chunk-KV cases took 0.676 s / 0.238 s.
- Capacity E2E 50–53: 14 passed. The existing datacenter totals test conditionally
  skipped its live totals comparison because its DiskDB fixture did not report
  usage; tree/Inspector navigation passed. This is not proof of live usage totals.
- Real Group 0 catalog integration passed: missing catalog, bounded 100+5
  windows, exact u64 fields, stale generation, invalid offset, corrupt checksum.
- Rust fmt and Web all-target Clippy passed after factoring the long test setup.
- Tree pages, stream extents/watermarks, multi-backend Chunk enumeration, expanded
  deployment lifecycle, Paxos overview, and same-cluster S3 setup remain pending.
- Release Web rebuilt and the existing 9090 Web process replaced; all six
  original KV/DiskDB PIDs survived. The earlier test Access binding was removed
  with a config backup. `/healthz` returns 200, and the real Chunk-KV catalog
  route returns an explicit 404 because this cluster has no initialized catalog.
  Browser inspection confirms seven visible top-level domains and that state.

## Paxos overview and fenced runtime observations

- KV now defaults to Overview; Data mounts on first use and inherits Store,
  Group and Replica selections. Ordinary Put requires an explicit group.
  Scans abort on scope/view changes; continuations cannot overwrite a later
  scope. All Groups is bounded to 10 groups and displayed rows to 1,000.
- Baseline KV topology/basic/advanced browser selection: 11 passed. Updated
  selection: 12 passed. Final shell/reconfiguration/basic/catalog selection:
  16 passed. New overview case: 0.609 s; bounded continuation/cancellation:
  6.5 s. The first bound test incorrectly exhausted its fixture after counting
  initial auto-scans; its unlimited source now tests the actual display cap.
- Chunk-KV owner observation verifies catalog generation, owner epoch,
  partition/tree/stream identity and returns independently sampled runtime
  counters. Web accepts only a matching configured Chunk-KV RPC/HTTP mapping,
  caps owner responses at 64 KiB and rechecks Group 0 generation. No new locks,
  key scans, data-page reads or journal reads are introduced.
- Service management tests: 3 passed, including durable/applied progression
  after a real journal mutation and Serving without a live authority grant.
  Web Group 0 integration passed missing endpoints, stale epoch, wrong owner,
  wrong stream and oversized owner responses. Updated Chunk-KV E2E: 2 passed
  (0.771 s / 0.208 s). TypeScript, focused selection/topology unit tests,
  Rust fmt and Web/Chunk-KV all-target Clippy passed.
- Runtime page/tree/extent inspection and discovery of management origins from
  managed deployments remain open. The catalog/runtime observation is not a
  claim of complete Chunk-KV diagnostics or a completed seven-domain rollout.

## Managed Chunk-KV discovery checkpoint

- Registered Chunk-KV services publish optional explicit Node IDs and HTTP
  management origins. Standalone deployment and the single-node profile populate
  these fields; old registrations remain readable without guessed placement.
- Runtime discovery uses an exact Group 0 owner key, checks instance/RPC identity
  and heartbeat freshness, and rejects records above 256 KiB. Managed navigation
  projects all registered service types and preserves full instance identifiers.
- Protocol identity test, four config tests, Web catalog integration, five managed
  mode tests, four service lifecycle tests, and four monitor profile/render tests
  passed. Frontend lint and three projection unit tests passed; shell/catalog
  E2E: nine passed. Rust fmt and affected all-target Clippy passed.
- Renderer validation caught a one-based Node template reference; corrected to
  the renderer's zero-based Node index and reran both profile/render suites.
- Native discovery acceptance remains pending; existing preview processes still
  use their previous binaries until the next controlled Web/Chunk-KV refresh.

## Root Console and default Chunk browsing steering

- Treat the Console user as root until UI authentication is introduced. Remove
  manual S3 keys and catalog/management token forms while preserving upstream
  protocol authorization through server-held credentials. Container mode must
  continue to reject topology and Capacity disk management mutations.
- Entering Chunk automatically scans one bounded page. Preserve exact ID lookup
  and type selection, remove the ID-prefix form and additional filters. Never
  automatically exhaust continuations to find matches.

- Default Chunk browsing implemented: active-only initial scan, automatic type
  selection, exact lookup retained, prefix form removed, 100-row window
  replacement and cancelled stale requests. Type filtering remains limited to
  the current scan window; it does not exhaust the database to fill a page.
- Chunk browser E2E: three passed (0.804 s, 0.530 s, 0.646 s), compared with
  the measured two-case baseline of 1.0 s / 0.525 s. TypeScript passed.
- Capacity steering: retain Disk overview, select a Zone below it, inspect a
  bounded block bitmap with blue used / green free / gray unknown, and return
  to the parent Disk without changing domain.

- Capacity Zone detail implemented in the Disk view with parent return, explicit
  refresh, 64-zone navigation pages and a 4096-block canvas window. Corrected
  the native little-endian bitmap interpretation; absent bytes remain Unknown.
  Inactive/changed selections cancel requests and disk changes reset Zone state.
- Zone E2E passed in 1.1 s (measured baseline 0.656 s). It verifies native bit
  colors, missing-byte gray, bounded Zone/block pages, direct selection and
  parent return. Two decoder unit tests and TypeScript passed. The existing
  backend still sends one full selected-zone snapshot; block-range transport
  pagination is not implemented by this UI checkpoint.


## Capacity owner repair and visual refinement

- DiskGroup creation binds an ordinary Store 0 data group before assigning its
  DiskDB owner. Existing bindings survive; absence of a data group is explicit.
  Four owner-assignment tests passed, including CAS creation and preservation.
- Repaired the original deployment's missing DG 1/2/3 binds to existing Store 0,
  Group 1 after saving the prior metadata. All three DiskDB registrations
  recovered without restarting the original KV/DiskDB processes. Zone 41 on
  disk 8827da0d7f28b34d-dbb7a12e686cf5d1 returned 32,768 free blocks.
- Disk identity joins normalize dashed and undashed representations. Zone pages
  contain 32 buttons with direct-number selection; inline detail uses Close,
  not a parent-navigation link. Bitmap and disk-map colors are muted and action
  buttons use a darker blue for white-text contrast.
- TypeScript, production frontend build and the focused Zone browser test passed
  (1.2 s), including map/bitmap colors, button colors, IDs and bounded paging.
- Root Console verification is in progress. The real managed chain passed KV,
  Iceberg and S3 CRUD plus Chunk inspection without browser credentials (4.2 s);
  topology and disk-maintenance APIs rejected writes with 503.


## Root Console checkpoint

- Removed management/catalog/S3 credential forms. Root logical operations need
  no browser bearer. Web injects Catalog reader/writer credentials and signs S3
  requests; browser-provided authorization does not choose upstream privileges.
- Container denies topology/deployment and all four disk-maintenance routes;
  standalone retains those controls. Group 0 data-write protection remains.
- Access deployment initializes a durable Console S3 user before child startup,
  stores its credentials with private permissions, and reuses them across
  restart/redeployment. Existing operation admission serializes initialization.
- Fixed an existing Store-list bootstrap shortcut that returned empty success
  despite observed topology and unavailable Group 0. The authority regression
  now passes without weakening its expected 502 response.
- Focused backend suites passed: Access 8, managed mode 5, management 4, owner
  assignment 4, lifecycle 4. Previously completed bare-metal authority 3 and
  launch registry 2 also passed. Rust fmt and Web/Monitor all-target Clippy passed.
- Browser root/shell/Iceberg/S3 cases passed (13); live managed acceptance passed
  in 4.2 s on isolated Web 43993. It verifies native data operations, real Chunk
  placement and backend hardware rejection without submitting browser secrets.
- Explicit native Access deployment acceptance passed against the isolated
  preview's storage chain: private credential creation, signed S3 bucket listing,
  Catalog reads, restart/reuse, and removal. Run with
  `CROWDB_NATIVE_DEPLOYMENT_DATA` and `CROWDB_NATIVE_DEPLOYMENT_SEED` through
  `pixi run cargo test -p crowdb-web --test native_access_deployment_test -- --ignored`.
  This fixture is opt-in so ordinary tests cannot target an existing cluster.


## Journal-to-Chunk navigation checkpoint

- Journal active chunks link directly into the Chunk explorer using their exact
  128-bit ID. The destination clears incompatible type/list scope and performs
  one detail read, without scanning to find the chunk. Returning to Chunk-KV
  retains the selected partition and Journal view.
- Focused Chunk/Chunk-KV E2E: seven passed. The new cross-domain assertion checks
  the exact requested ID and absence of an initial scan; the extended partition
  case took 1.3 s against its freshly measured 1.2 s baseline. TypeScript and
  production frontend build passed.
- Actual Tree page structure and decoded extent records still require bounded
  native inspection interfaces. Current checkpoint/counter and extent-fence
  views do not claim those records are already available.

## Current-cluster data acceptance

- Deployed CDB, three DiskIO instances, Chunk-KV and Access into the current
  Console cluster. Chunk-KV readiness now probes `/ready`; the old `/health`
  input became `/health/health` in the shared readiness helper and terminated
  an otherwise healthy deployment.
- Disk discovery preserves unsigned 64-bit identity words as decimal strings
  across the Rust FFI JSON boundary. DiskIO accepts those strings and legacy
  nonnegative integers; malformed identities retain the existing disk set.
- Published `crowdb-tpc-loader` 0.1.1 loaded SF 0.001 into
  `console_tpch_demo`: eight tables, 8,695 rows and eight Parquet files.
  The live Iceberg tree exposes snapshots, manifest lists, manifests and files;
  Parquet inspection shows footer, row groups and columns with zero data-page
  bytes read.
- Copied the 32 source Iceberg objects into S3 bucket `console-iceberg-demo`,
  with an additional `README.json` index. Every copied object was read back and
  compared byte-for-byte. Live S3 listing and metadata preview passed.
- Chunk ALL lists 12 real records. Their metadata is currently in Store 0,
  Group 0; Group 1 has no chunk records. Clicking an Iceberg chunk shows its
  strip, disk and zone. ChunkDB range redesign remains deferred.
- This is a development data deployment: one rack uses the debug CDB unsafe
  placement mode and one-copy writes. DiskIO uses memory disks, so this demo
  data does not survive a DiskIO process restart. Access catalog initialization
  was performed with the supported management CLI before startup.
- Verification: DiskIO CTest 130 passed, lifecycle tests four passed, focused
  Chunk browser baseline three passed, Rust fmt and affected all-target Clippy
  passed, and C++ tree-lint exited successfully. Live browser acceptance covered
  all three data tabs. Runtime reports and private credentials remain outside Git.

## Chunk window and physical-layout refinement

- Chunk listing provides Prev / Next with remembered scan start cursors;
  type changes and refresh reset the history. One window replaces another,
  and the list scrolls independently of the selected layout.
- A vertical Chunk container holds horizontal Strip rows. Mirror replica and
  EC data/parity blocks display complete disk IDs and Node, Diskgroup, Zone,
  unit offset and exact byte offset directly in the diagram. Structured Chunk
  fields replace the central JSON dump. Strip detail retains navigation links.
- Focused browser regression: three passed (0.942 s layout, 0.589 s strip
  paging, 0.785 s scan paging). Coverage includes one/two/three Mirror copies,
  EC data/parity blocks, complete placement fields and returning to the first
  scan window. TypeScript checks and production build passed; the live Iceberg
  Chunk shows Node 3 / Diskgroup 3 / Zone 2 with its exact byte offset.
- Combined Chunk and Chunk-KV browser verification: seven passed, including
  Journal-to-Chunk exact lookup and preserved partition selection on return.

## Multi-node UI rebuild follow-ups

- [ ] **Default node service set**: each new Node should default to one KV,
  DiskDB, ChunkDB, DiskIO, Chunk-KV and Access Server instance. Stage startup
  by prerequisites (Group 0, disk groups/disks, metadata groups and catalog),
  expose pending/failed phases and retry without recreating the Node. Use
  normal multi-node deployment and protected placement, never silently select
  single-node or unsafe placement. Deferred by the user while the current
  manual UI rebuild is validated. Files: AddNodeDialog, service deployment.
- [x] **Per-service Node menu**: Node context menus must independently manage
  each service type and instance (deploy/start/stop/restart/remove), showing
  existing instances and lifecycle status. Keep this alongside whole-node
  defaults. Deferred with the default service-set work. Files: useClusterMenus,
  DeployServiceDialog. Verified Node submenus and exact instance lifecycle routing.
- Reset regression discovered during live rebuild: auxiliary processes and
  launch records survived while UI reported success. Fixed auxiliary teardown
  before KV shutdown and removal of retained launches; five lifecycle tests
  and affected Clippy passed. Old runtime isolated; fresh default workspace
  now has three logical racks and rebuilding nodes through UI is in progress.
- [ ] **One-click valid create defaults**: every Create/Deploy dialog pre-fills
  valid, conflict-free IDs, listener ports and parent/dependency references.
  With prerequisites satisfied, accepting defaults creates the resource.
  Check conflicts across service types on the same host, not only within the
  current form. Missing prerequisites must be explicit rather than supplying
  defaults that inevitably fail. Include repeated creation and reopened-dialog
  coverage. Deferred with the other creation-flow refinements.

- Creation-flow implementation: backend deployment defaults now choose unused
  instance IDs and ports across service types, including DiskDB listener ranges,
  Access S3 endpoints and active local sockets. Deployment revalidates and
  claims listener ports. Add Node defaults to the six-service plan; prerequisites
  are explicit and the Node menu can resume missing deployments without
  duplicating existing services. Complete live multi-node acceptance remains.
- Verification: six service integration tests, 39 create-form unit tests, two
  focused service-menu/plan browser tests, frontend build/lint and affected
  Rust fmt/Clippy passed. Auxiliary browser case 2.7s versus 2.4s baseline.

## Single-dialog node creation and one-rack acceptance

- [x] **Single Add Node flow**: keep creation and deployment progress in one
  dialog. Retry only failed initial services with fresh defaults; never create
  the Node or a successful service twice. The six-service queue survives dialog
  closure, resumes when dependencies appear, and stops before Reset. Plans are
  console-session state; after a page reload use the Node menu to resume missing
  services. Durable server-side plans are not implemented.
- [x] **Creation regression**: 41 frontend unit tests and all 12 affected browser
  cases passed (11 in the combined run, the corrected CRUD case separately).
  CRUD now explicitly finishes the retained progress dialog; 6.6s test time.
  Focused service plan / single-dialog cases: 0.789s / 0.926s. Build/lint passed.
- [~] **One Rack, three Nodes live acceptance**: Rack 1 now contains Nodes 1–3.
  Each has KV and DiskDB, and Group 0 has three healthy replicas. Three CDBs
  deployed automatically after initialization. DiskIO awaits file-backed disks;
  Chunk-KV awaits metadata groups; Access deployment needs further diagnosis.
- [ ] **Rack preference, node protection**: rack diversity is an optimization,
  not an admission requirement. Permit normal multi-node protected placement
  within one rack while reporting actual rack protection. Keep node/disk loss
  limits and multi-rack preference. Verify selectors and conversion publication.

- Rack preference correction verified: normal selectors and conversion publication
  allow one rack while enforcing node/disk recovery budgets. Actual rack protection
  remains visible in assessments. Passed 24 selector tests, 8 configuration tests,
  the new production single-rack allocation/conversion test and 2 production
  node-loss/recovery full-stack tests; affected fmt and all-target Clippy passed.
- Live Group 0 initialization automatically deployed CDB on each of Nodes 1–3.
  Access currently fails because `/chunk-kv/catalog-head` is not initialized;
  make this a waiting dependency instead of attempting startup early.


## Current handoff and UI-first execution

- Current fixture update (2026-10-03): the API bring-up below is now complete.
  All six service types run on each of Nodes 1–3; Store 0 / Groups 0 and 1
  report healthy three-replica groups. Three disks and DiskIO instances exist.
  Iceberg `ui_demo` has eight TPC-H tables / 8,695 rows; S3 `ui-iceberg-demo`
  has 32 copied objects verified by read-back. Console proxy listings and a
  bounded Chunk list/exact detail succeed. Manual binding, owner and catalog
  repairs were required: these are open bugs, not completed flow fixes.
  The consolidated current issue list and fixture details are in
  [ui-todo.md](ui-todo.md); older bring-up observations below are historical.
- Latest direction: prepare one Rack / three Nodes, Group 0 and ordinary Group 1
  with scripts/CLI, inject Iceberg and S3 data, then prioritize the main UI.
  Do not spend the next pass on exhaustive manual click-through acceptance.
- Live workspace: `.crowdb-runtime/persistent/console/default`, web port 9090.
  Node KV HTTP ports 19910/19911/19912, RPC 20010/20011/20012; DiskDB bases
  29920/29923/29926. All are normal multi-node services. Three CDB instances
  now exist (instance 3 on Node 1, instance 1 on Node 2, instance 2 on Node 3).
- Group 0 has three replicas. Store 1 / Group 1 was created with three replicas.
  DiskGroup binding currently requires an ordinary group in Store 0, so also
  provision Store 0 / Group 1 before storage setup; do not assume Store 1 suffices.
- Create Disk Group on Node 1 is currently waiting on Group-0 RPC, not merely
  rendering. Read-only disk-group and instance requests time out too; `/healthz`
  still succeeds. Latest web log reports retries exhausted to 127.0.0.1:20012.
  Node 3 KV log reports watch push send queue full and `std::bad_alloc` in RPC
  transport around 10:45 UTC. Root cause is not established. Preserve logs;
  check actual RPC liveness before retrying creation or claiming success.
- Three dedicated sparse disk files exist under `acceptance-disks/node-N.img`,
  each logically 1 TiB; none has yet been registered. Use explicit paths and
  matching geometry; never use a physical device implicitly.
- Fix service labels uniformly: KV-N, DDB-N, CDB-N, DIO-N, CKV-N, AS-N in tree,
  topology and instance menus. Keep full service type and exact backend ID in
  properties. Preserve IDs as strings. This request is not yet implemented.
- Scripted bring-up sequence: verify three KV nodes and quorum → ordinary data
  group → DiskGroup/data binding/owner and disk registration → DiskIO on all
  three nodes → Chunk-KV bootstrap/catalog → Access catalog initialization and
  endpoints → small Iceberg dataset → copy representative objects into S3.
- Validate data with bounded reads: Chunk ALL first page and exact detail,
  Iceberg table/snapshot/manifest/data-file footer, S3 bucket/prefix/object.
  Record actual counts, source and any incomplete results. Range redesign stays
  deferred. No full-cluster scans or data-page loads for Parquet inspection.
- Main UI priorities: consistent service identity and readiness; one-window
  create progress/errors; bounded Chunk windows and compact colored strip/block
  layout; structured Iceberg content; usable S3 object list/details; Capacity
  32-zone pages with inline zone bitmap. The four data-browser page designs remain pending discussion.
- Recent commits: e701caa6 single-dialog service plans; af7fb4df rack preference;
  da04706f restart after binary replacement; ccbdbbee dependency-aware startup.
  Focused checks passed. Service plans are session-only and need resuming via
  Node menu after reload. Access catalog initialization still needs verification.

- User clarified the permanent UI design is the specification itself. Rewrote
  `doc/design/console/design-crowdb-console-ui.md` as observable contracts;
  do not create a duplicate UI-spec document. The current spec covers Cluster,
  Capacity, KV, and shared interactions only. Chunk, Iceberg, Chunk-KV, and S3
  page designs and acceptance remain pending discussion; earlier task notes
  for those pages are not approved specification contracts.
- Restarting KV Node 3 through the API restored Group-0 RPC: disk-group list and
  DiskDB instance list now return HTTP 200. No DiskGroup remained on Node 1.
  The original RPC failure root cause is still unverified; recovery is not a fix.
