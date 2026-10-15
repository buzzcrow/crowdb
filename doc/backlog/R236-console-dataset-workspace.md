<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R236: console — Dataset Workspace, Snapshot Tree and Publication Flow

**Status**: In progress. Backend/API/SDK acceptance is complete; execute
[the Console plan](../working/plan-console-dataset-workspace.md). The user has confirmed a
parent-based Snapshot tree, one combined Snapshot view, and the complete browser
import/prepare/publish flow and full named Split UI in the first release.
[Dataset control lifecycle](../design/access-server/dataset/design-dataset-control.md) owns the required Split model, API and
Rust/Python SDK contract. Successful new publication
advances latest, and child creation defaults to preserving all parent data.
Split-only edits publish independent Split revisions without creating a data
Snapshot or changing Dataset latest. The UI is for local import, testing and
demonstration; other input sources are handled through SDK ingestion.
Planning defaults: allow independent parentless roots, text/hex preview capped at
64 KiB, public versioned Dataset inspection APIs, and R237's explicit Split policy
defaults. These are implementation defaults adopted for the handoff, not claimed
as additional user decisions. Permanent specifications are updated during execution.

**Problem**:

- Dataset now owns native sample-oriented data, but Console exposes only
  Iceberg and S3 data workspaces. Operators cannot discover Datasets, select an
  immutable Snapshot, inspect sample fields, or understand retention in Console.
- Root designs: [Console UI](../design/console/design-crowdb-console-ui.md),
  especially navigation, bounded observation, Iceberg and S3;
  [Dataset architecture](../design/access-server/dataset/design-crowdb-access-dataset.md);
  [Dataset specification](../design/access-server/dataset/spec-crowdb-dataset.md);
  [Dataset HTTP v1](../design/access-server/dataset/openapi-crowdb-dataset-v1.yaml).
- Iceberg's shared tree, compact overview, Snapshot table, structured properties
  and lazy paging are suitable interaction patterns. Its Manifest/File hierarchy
  is not the Dataset model: Dataset has one logical Manifest per Snapshot and
  heterogeneous Sample/Field values. Manifest partitions are internal storage.
- The implemented `SchemaRecord` in `lib/crowdb-access-dataset/src/manifest.rs`
  has a version and field name/required flags, without field types, MIME types,
  nested structures, split definitions, or semantic media metadata. The design
  narrative is broader than this current wire model. UI must expose actual data.
- Sample IDs are opaque bytes, represented by canonical base64 in HTTP v1.
  The specification's string terminology must be reconciled with the wire and
  implementation before documenting UI identity rules; no conversion may lose
  identity or interpret an ID as a filesystem path.
- Public HTTP already pages the latest ancestry in
  `app/crowdb-access-server/src/dataset/api/management.rs`. It does not list all
  Datasets or all publication candidates. `GetManifest` assembles the complete
  Manifest, including inline bytes. `app/crowdb-web/src/access.rs` recognizes
  only S3 and Iceberg. Frontend-only pagination cannot fill these gaps.
- The intended model allows any valid published Snapshot in the same Dataset
  as parent. Current `dataset/api/publication.rs` and Dataset authority publication
  require `parent == latest`; `advance_head` also uses parent as its expected head.
  This prevents historical branching. Separate ancestry from head concurrency
  before promising a working branch-creation UI. One parent per Snapshot produces
  a tree, or a forest if multiple parentless roots are allowed, not a merge DAG.
- Split exists in the architecture narrative as a logical Snapshot subset, but
  current Manifest records, HTTP models and SDK have no named Split records,
  membership definitions or Split management APIs. `planner.rs` implements
  read-time Selection (prefix, range, explicit IDs, keyword, field match and
  boolean composition), not a persisted inheritance filter or named Split.
  `read_session/planning.rs` selects from the current Manifest before field
  projection. Keyword/field match examines explicit inline fields only, without
  resolving inherited values or searching chunk payloads. This is not yet an
  effective-value predicate over inherited Samples.
- Concrete flows: discover an SDK-created Dataset after restarting Console;
  inspect an inherited optional field without reading a large payload; keep a
  selected Snapshot stable while another client publishes; inspect why release
  does not make an ancestor reclaimable.

**Solution**:

Ownership: backend work described in items 3/4/7 (Dataset authority, metadata
indexes, child composition and publication) is implemented and tested in R237.
R236 consumes those APIs and owns Console proxy/configuration, UI and browser
acceptance. Implement no duplicate store, planner or publication authority here.

1. **Add a peer Dataset domain in the Console shell.** Extend
   `app/crowdb-web/ui/src/shell` and shared domain/navigation state with the order
   Cluster, KV, Chunk, Chunk-KV, Iceberg, Dataset, S3. Keep the shared resizable
   three-panel layout, Actions strip, properties, keyboard behavior and embedding
   configuration. Dataset owns independent query, expansion and selected state.
   - Left tree: Datasets → Dataset → root Snapshot → child Snapshot, recursively
     following declared parent links. Siblings are branches from the same parent;
     Latest/Stable badges identify references without changing that structure.
     Samples and Fields belong in the center, avoiding a huge sample tree.
     Page roots and each parent's children independently and expand lazily. Never
     fetch all Snapshots or sort IDs to reconstruct ancestry in the browser.
     Direct links reveal ancestors with bounded work; partial ancestry stays
     explicit rather than inventing a root or loading the whole tree.
     Each Snapshot also has a labelled Splits group containing its named Splits;
     that group is a peer of child Snapshot nodes, never an ancestry level.
   - Show the fixed `dataset-ns` namespace in properties and creation context;
     it adds no single-child wrapper and has no namespace management action.
   - Update permanent UI domain counts and acceptance text only when this design
     is agreed; the current document mixes six-domain and seven-domain wording.

2. **Make Dataset and Snapshot scopes explicit.** Add the Dataset workspace in
   `app/crowdb-web/ui/src` using the Iceberg interaction conventions.
   - Root center lists Dataset name, immutable Dataset ID, latest and stable.
     No invented creation time, size, sample total, or health indicator.
   - Dataset center has a compact identity/head summary and a bounded Snapshot
     inventory with parent, state, Latest/Stable badges and observed retention.
     The left tree supplies the relationships. Inventory and tree include
     published branches outside latest ancestry, not merely its current chain.
     Provide direct entry by immutable Snapshot ID and links from latest/stable.
   - Snapshot center is one workspace: compact summary followed by a Samples
     matrix whose column definitions present the Schema. There are no Samples /
     Schema tabs and no Manifest or Files navigation level. Keep Schema version
     with the summary, Sample ID as a sticky first column, and field headers
     visible while scrolling rows. An ordinary narrow schema is visible directly;
     wide schemas use a bounded column window, not a fully fetched hidden schema.
     Properties expose parent, operation identity, schema version and available
     Manifest binding information, with diagnostics secondary to structured data.
   - Resolve latest/stable once on entry. Every detail query carries the resolved
     ID. Head changes update observed badges and offer explicit navigation to
     the new head; they never silently switch the sample/schema view.
   - Empty Dataset means no published Snapshot, not an error or empty Manifest.
     Prepared/Aborted operations never masquerade as readable Snapshots. An
     operation record alone does not prove a successful head transition.

3. **Combine Schema and Samples without fetching payloads.** Extend Dataset
   authority inspection and the workspace together.
   - Schema describes the Snapshot's allowed fields: each matrix column header
     shows field name and required/optional. Clicking a header shows its complete
     definition in properties. It does not select a Sample or read field bytes.
     Current Schema has no type/MIME/nested structure; do not invent any.
   - Samples are concrete rows identified by opaque Sample ID. Each cell is the
     field at that row/column, showing a compact storage/value state and known
     byte length. Row selection shows Sample identity/context in properties;
     cell selection shows that exact Sample/Field's metadata in the same panel.
     Neither selection replaces the matrix or opens a second view. Exact-ID
     lookup and independently bounded row/column paging remain reachable.
   - Default matrix window is 20 Samples by 12 fields. A field-header selection
     is distinct from a Sample/Field selection, including in history. A schema
     with no fields still displays the Sample ID column and explicit schema state.
   - Field cells distinguish inline, chunk-backed, tombstoned and missing values,
     plus inherited source Snapshot when resolution succeeds. Show known length
     and declared checksum; a declared checksum is not proof bytes were verified.
     Do not include inline bytes in metadata list responses.
   - Sample membership is the selected Manifest's complete membership. An omitted
     Sample is absent even if an ancestor contains it. A surviving Sample may
     inherit omitted optional fields; tombstones stop that inheritance. Missing
     and tombstoned are distinct, and neither is an empty value.
   - Display valid UTF-8 sample IDs for readability with a separately copyable
     canonical base64 identity. Binary IDs display canonical base64 with an
     encoding label. Searches and navigation preserve exact bytes.
   - Local row filtering is explicitly labelled as applying to loaded rows.
     Named Split scope and authority-evaluated inheritance filters use R237 and
     must not be implemented by filtering only the loaded page. Shuffle controls
     and training/read-plan monitoring remain outside this UI requirement.
     Unknown details remain unknown, not guessed totals.
   - Locators remain opaque. Physical Chunk drill-down is deferred until a
     chunk-owned decoder supplies an authoritative bounded inspection contract.

4. **Provide durable, bounded metadata access.** Extend
   `lib/crowdb-access-dataset` and `app/crowdb-access-server/src/dataset` with
   Dataset discovery, Snapshot inventory/root/child enumeration, schema/sample/
   field inspection, and retention observations.
   - Discovery reads authoritative stored Dataset identities, including those
     created before this UI and by other clients. No browser-local registry or
     hidden full scan. Add durable parent-to-child/root discovery, including
     Snapshots created by older clients. Publication and reclaim must keep these
     indexes consistent; a candidate is not a readable published child. Preserve
     HTTP v1's latest-ancestry listing for its existing callers; it is insufficient
     to implement the branch tree and must not be relabelled as all Snapshots.
   - List defaults are 20 rows, maximum 100, at most 1 MiB per response;
     byte limits may yield shorter pages. Every request also bounds metadata
     decoding, scanned records and ancestry traversal, with a five-second Access
     metadata deadline and six-second Console proxy deadline. Reaching a work
     bound returns explicit partial/continuation or a limit error, never a false
     end or a false missing-field result.
   - These limits apply before full Manifest materialization, not after slicing
     a full result. Existing stored records/partitions and publication records
     can contain large data; inspect their decode costs and introduce bounded
     authority-owned metadata projections/indexes where necessary. Old data must
     either support bounded resumable inspection or report a specific capability
     limit; it must not appear as empty. No silent storage-format replacement.
   - New opaque continuations bind Dataset identity, immutable Snapshot where
     applicable, query and position. Membership changes cannot corrupt discovery
     paging; stale cursors return a refreshable conflict. Preserve the existing
     public Snapshot continuation semantics instead of redefining them in UI.
   - Browsing performs no chunk-payload reads, starts no training Read Plan and
     acknowledges no application progress. Bound inherited-field resolution;
     unresolved work remains partial instead of silently declaring a value absent.

5. **Connect the configured Dataset service.** Extend `app/crowdb-web/src/access.rs`
   and Access service configuration/discovery, with corresponding shared config
   and deployment wiring where needed.
   - Console uses the configured Dataset listener and server-held credential.
     Do not introduce a connection dialog, browser bearer storage or arbitrary
     endpoint parameters. Missing configuration, unavailable service, denied
     access and empty inventory are separate states with Retry where applicable.
   - Enforce read-only mutation restrictions at the Console API boundary as well
     as in controls. Container mode permits supported Dataset data operations
     without permitting topology/deployment changes. API prefixes and enabled
     modules apply consistently to Dataset as to existing domains.

6. **First-release actions: creation and Snapshot lifecycle.** Use the
   existing authority through `app/crowdb-web` and Dataset-scoped Actions.
   - Root: Create Dataset with the fixed namespace and validated name. Duplicate
     conflicts retain input; success reveals the authoritative Dataset. Dataset
     deletion is unsupported and must not be offered.
   - Published Snapshot: Set stable, Retain, Release and Reclaim, with exact target
     ID. Publication never implicitly changes stable. Retention is an idempotent
     reference, not an editable numeric count. Release does not promise deletion.
   - Authority decides reclaim eligibility using latest, stable, explicit
     retention, parent reachability and active reads. A UI observation cannot
     replace the mutation-time check. Show the backend's conflict reason and
     distinguish pending cleanup from completed reclamation. Reclaimed chunk
     count is not reclaimed bytes or proof that external sources were deleted.
   - Confirm destructive reclamation with scope and consequences; preserve
     unknown outcomes on timeout and reconcile before repeating operations.
   - Dataset Actions include Import / New Snapshot. A Snapshot's Create child
     action starts that same flow with its immutable ID selected as parent,
     including historical parents that are not latest.

7. **Support the complete browser import and publication flow.** Extend the
   Dataset workspace, Console proxy and `app/crowdb-access-server/src/dataset`
   together. Published Snapshot inspection remains immutable; editing takes place
   in an explicitly labelled draft with a return path to its originating scope.
   - UI import is local files plus explicit Sample/Field mapping, including a
     bounded batch mapping-file input. This is a real API-backed test/demo flow,
     not a mock dataset generator or a general ETL workbench. External sources
     use SDK ingestion; no S3/Iceberg connector browser or credentials form is
     required. Keep mapping format aligned with SDK models rather than inventing
     a separate browser-only data contract.
   - Start from a new/existing Dataset and choose a valid parent. First Snapshot
     has no parent. Any published, available Snapshot in the same Dataset can be
     selected as a parent; a missing, foreign, Prepared or Aborted parent is
     rejected by authority. Selecting a historical parent never rewrites it.
   - Define field names and requiredness, then import Sample IDs and map fields
     to inline bytes or local payload files. Provide an explicit mapping review;
     filenames never silently become Sample identity or field semantics. Permit
     additions, replacements, Sample removal and optional-field tombstones.
     Required fields and child schema compatibility follow authority validation.
   - The confirmed default for Create child is to preserve parent membership and
     apply reviewed changes. An explicit Replace membership import mode treats
     imported Sample IDs as the complete set. Display additions, removals and
     inherited/overridden/tombstoned fields before preparation. Final submission
     contains complete current membership; never submit only added rows and
     accidentally remove the parent's other Samples. Large draft composition
     must remain bounded; existing v1 metadata limits produce an explicit limit
     error or require a separately designed staged API, never hidden full scans.
     Preserving all parent data is an import-composition contract: current
     publication requires complete Sample membership and required-field entries;
     simply submitting an empty delta does not inherit all Samples automatically.
   - The optional Filter parent data step resolves a Selection against the
     chosen immutable parent, freezes the selected membership for review, then
     applies explicit additions/removals/field overrides. The resulting Snapshot
     stores complete membership rather than a live predicate against latest.
     Reuse Selection syntax where appropriate, but define metadata predicates
     against resolved inherited inline values, honoring tombstones. Do not
     silently reuse the current explicit-fields-only matcher. Chunk-backed bytes
     are not searched. Bound evaluation and show partial/limit outcomes; never
     publish a partial filter result as the complete intended membership.
   - Upload with Dataset-scoped sessions, declared size and MD5, immutable parts
     of at most 4 MiB and at most 10000 parts per file. Compute integrity with
     bounded buffers and bound upload concurrency. Show per-file progress,
     completion, failure and retry; completed uploads are not published Samples.
     An identical part retry reuses its receipt; changed bytes cannot overwrite
     an accepted part. Preparation requires all referenced uploads complete.
   - Review -> Prepare -> Publish are explicit stages. Freeze parent, schema,
     complete membership, upload references and operation token for preparation.
     Prepare returns a durable candidate ID with a Prepared label; normal
     published browsing remains unchanged. Publish reuses the same operation
     token/content and candidate ID. Only successful reconciled publication
     selects the new published tree node; stable remains unchanged.
   - Drafts after Prepare are immutable for that operation. Editing creates a
     new operation with explicit cleanup of the old candidate, not divergent
     retry under the previous token. A lost response is Outcome unknown; inspect
     operation/upload status and reconcile identical requests instead of creating
     another Snapshot. Persist recoverable draft control state/operation and
     upload identities outside navigation history. After reload, local files may
     need reselection and integrity checking; never claim their bytes are durable
     until the upload authority confirms acceptance.
   - Cancel stops further browser work and aborts only eligible unclaimed uploads.
     Prepared candidates need explicit authority reclaim/abandon handling; closing
     a dialog must not imply reclamation of their claimed payloads. Show pending
     cleanup and retain the candidate/operation identity for recovery.
   - Publication must separate `parent_snapshot_id` from the expected latest
     reference/generation. Confirmed behavior: successful new publication on any
     branch makes it latest. Concurrency policy: this succeeds only
     if the observed head generation still matches. Concurrent head
     changes return a conflict, preserve the selected parent and candidate, and
     require review of the head change. Never silently reparent or overwrite a
     newer head; historical successful retries cannot rewind latest. Update
     authority, HTTP contract and SDK compatibility together; a UI-only
     relaxation is insufficient. Idempotent retries of an already successful
     historical publication return its original result without advancing latest
     again; they are not new publications.

8. **Bounded preview, separately from metadata inspection.** Add an
   explicit Field Preview action using Dataset's verified read path, not raw
   locator reads. First release supports text/hex only and at most 64 KiB for
   a complete field. Larger fields are labelled beyond the preview limit and
   rejected before payload transfer; no hidden full download followed by slicing.
   Verify full-field integrity before showing successful preview. Cancel on
   scope changes and discard failed/partial results. Do not infer media types
   from field names or execute embedded content. Rich media and streaming
   downloads can be separately agreed without changing Sample identity.

9. **Preserve bounded navigation and failure behavior.** Integrate Dataset into
   the shared session history, at most 32 retained visits/cursor positions.
   - Retain Dataset/Snapshot/Sample/Field identity, selected header or cell,
     row/column windows, tree expansion, query and scroll;
     never retain Manifest bodies, previews or bearer credentials in history.
     Back/Forward refetches the bounded view against the saved immutable ID.
   - Hidden domains stop observation polling. Late responses cannot overwrite a
     newer selection. Unavailable optional details preserve primary metadata.
     Reclaimed Snapshots retain an explicit unavailable selection context rather
     than falling forward to latest; stale continuations disable Next until reset.

10. **Integrate the complete Split flow from R237.** Extend the Dataset workspace
    and Console proxy against the same API used by Rust/Python clients.
    - Expand a Snapshot's Splits group lazily with independent continuation and
      loaded/end states. A Snapshot with no Splits and an empty named Split are
      different states. Selection identity includes Dataset, Snapshot, Split revision and Split
      name; same names in different Snapshots never share selection or cursors.
    - Select Snapshot for all Samples; select Split for its paged member rows in
      the same matrix with the same Schema. Show exact Split name, owning Snapshot/revision,
      complete member count when available, definition/provenance and policy in
      structured summary/properties. Row/column windows and Back/Forward retain
      the Split scope; leaving it does not alter stored membership.
      Splits group labels the selected revision and offers bounded revision history.
      Default entry resolves current revision once. Publishing r3 never silently
      replaces an open r2 view; offer explicit navigation to current revision.
    - Import/New Snapshot includes a Splits step after Sample composition and
      before final review. Create/edit/rename/remove draft Splits using explicit
      IDs, supported rules or the deterministic weighted partition convenience.
      Show seed/weights, actual evaluated counts, overlap/coverage policy and
      bounded violation examples. Preview and validation use authority; partial
      evaluation cannot enable Prepare as if it were complete.
    - Create child defaults to all parent data and captures the parent's Split
      revision once for inherited memberships.
      Its source selector offers All, a parent Split, a rule, or Split plus rule.
      Review retained/removed/unassigned membership and explicitly request rule
      reevaluation or manual assignment for additions. Editing Samples invalidates
      older previews. Do not auto-assign new data to train/validation silently.
      Create child from S1/r2/train pins that exact membership, keeps S1 as parent,
      and records Split provenance; later r3 edits do not change the child. The
      Snapshot tree remains S1 -> child, never S1 -> train -> child.
    - Published Split Actions offer read/preview, edit/rename/remove and Create
      child using this Split. Editing opens a Split-set draft at the selected base
      revision. Review/prepare/publish creates a new revision on the same Snapshot
      and updates its current Split reference; Dataset latest and sample data stay
      unchanged. Concurrent edits show a revision conflict with the draft retained.
      Deleting a Split removes it only from the new revision; history remains.
    - For a new data Snapshot, Prepare freezes Samples and any initial Split set
      together; Publish selects the new Snapshot and reveals its initial revision.
      Standalone Split publication selects the new revision on the same Snapshot.
      Failed/unknown
      operations retain recovery identities and show unresolved validation or
      cleanup. SDK-created Splits are discoverable after reload, without a local
      browser registry. Readonly mode permits inspection and blocks mutations.

**Dependencies**:

- [Dataset control lifecycle](../design/access-server/dataset/design-dataset-control.md) supplies authoritative Split definitions,
  membership, inheritance/filtering, publication and SDK/API parity. Full Split
  UI is mandatory for this requirement's completion. Its absence is a visible
  capability limitation during development, not grounds to drop the feature.

- Existing Dataset HTTP v1, durable authority, publication, retention and verified
  reads are the semantic base. New inspection DTOs are additions, not a second
  Snapshot authority. Document agreed API additions in the Dataset contract.
- Historical-parent publication requires the authority/head-contract correction
  above; current `parent == latest` behavior is not an acceptable fallback for
  this requirement. Maintain legacy client compatibility or document a versioned
  migration rather than silently weakening concurrency checks.
- Existing Console tree, history, properties, mode enforcement and native E2E
  runtime are shared infrastructure. Coordinate shell changes with R235; Dataset
  must not require that hardware lifecycle work to finish.
- R220 direct-client access and R233 local caching are not prerequisites.
  Standard HTTP is the fallback. Missing Dataset service shows unavailable;
  S3/Iceberg content is never substituted for Dataset state.
- The shared backend acceptance is complete; the planning defaults
  above settle remaining scope choices. Reopen a decision only if implementation
  evidence invalidates the stated contract, not merely because code is unfinished.

**Acceptance**:

- A populated Console -> select each domain and return to Dataset -> exact tab
  order and independent selections persist; fixed namespace has no management
  controls. Invariant: peer-domain navigation and namespace identity. E2E test.
- Datasets created by SDK before Console startup -> browse multiple directory
  pages and create another Dataset -> stored identities are discoverable, with
  bounded pages and no browser-only inventory. Invariant: authority discovery.
  Integration test.
- Empty Dataset and a multi-Snapshot Dataset -> inspect both, then open a known
  published Snapshot outside latest ancestry -> correct empty/history/direct-ID
  states, with no candidate passed off as readable. Invariant: visible scope.
  E2E test.
- Several sibling branches and deep ancestry, including SDK-created Snapshots
  outside latest ancestry -> expand roots/children and follow a direct link ->
  exact parent structure, independently paged children and bounded ancestor
  reveal without a full scan. Invariant: complete branch discovery. E2E test.
- Concurrent publish/reclaim and restart during index maintenance -> browse the
  Snapshot tree -> no Prepared node is readable, no published branch is silently
  lost, and continuations reconcile changes. Invariant: authoritative tree index.
  Integration test.
- Narrow, wide and empty-field schemas with many Samples -> open Snapshot,
  select a header/row/cell and page -> one matrix with requiredness in headers,
  exact selected properties, bounded rows/columns and no payload reads.
  Invariant: one combined Schema/Sample view. E2E test.
- Selected historical Snapshot -> publish a new latest and change stable from
  another client -> badges refresh while Schema/Samples remain on the selected
  immutable ID. Invariant: Snapshot consistency. E2E test.
- Parent/child fixtures with omitted Samples, inherited optional fields,
  tombstones, missing fields and empty inline values -> inspect child -> each
  value state and source is correct, with no inherited omitted Sample.
  Invariant: explicit ancestry and complete membership. Integration test.
- UTF-8 and binary Sample IDs, long exact IDs and schema without types -> display,
  copy and navigate -> identities round-trip and no media/type inference appears.
  Invariant: lossless identity and observed schema. Unit test.
- Large inline Manifests, many Datasets, deep ancestry and old records -> request
  one metadata page -> scan/decode/time/response bounds hold before materialization,
  no payload read occurs, and partial/limit states are explicit. Invariant:
  bounded metadata observation. Integration test.
- Snapshot/query-scoped continuations -> reuse against another scope or stale
  discovery generation -> reject/reset without mixing records; valid continuation
  returns the next bounded window. Invariant: continuation binding. Integration test.
- Missing listener, denied access, service outage and readonly/container modes
  -> browse and attempt mutations through both UI and API -> correct capability
  states, configured target only, and no credential exposure. Invariant: service
  boundary and mode enforcement. Integration test.
- Existing and missing Dataset names -> submit Create -> conflict retains input
  or success reveals one empty Dataset; no delete action appears. Invariant:
  creation identity. E2E test.
- New Dataset and a historical published parent -> import IDs/inline fields/
  local files, review mappings, Prepare and Publish -> one complete Snapshot
  appears at the selected parent, with verified field reads and stable unchanged.
  Invariant: end-to-end browser publication. E2E test.
- Parent membership and optional/required fields -> preserve membership with
  additions/deletions, then explicitly replace membership in another child ->
  final membership and inheritance match review; incompatible required fields,
  duplicate IDs and incomplete uploads fail before publication. Invariant:
  complete Manifest composition. Integration test.
- Large uploads, interrupted parts, divergent retry and oversized metadata ->
  upload/retry/prepare -> bounded buffers/concurrency, immutable accepted parts,
  completion verification and explicit limits without partial publication.
  Invariant: bounded durable import. Integration test.
- Prepared operation and ambiguous responses -> reload, reselect local files
  where necessary, reconcile and retry identical content -> same candidate and
  Snapshot identity; divergent edits use a new operation. Invariant: recoverable
  idempotent publication. E2E test.
- Historical parent plus concurrent latest advancement -> publish -> agreed head
  conflict policy preserves parent/candidate and cannot rewind the newer head;
  missing/foreign/unpublished parents fail validation. Invariant: ancestry is
  independent of head concurrency. Integration test.
- Parent with inherited inline fields, tombstones and chunk-backed values ->
  apply the inheritance filter and review -> membership is frozen to
  that parent, predicates use resolved inline values, chunks are not searched,
  and incomplete bounded evaluation blocks publication. Invariant: explicit
  complete inheritance filtering under R237.
  Integration test.
- Unclaimed upload, Prepared candidate and shared payload -> cancel/close or
  explicitly abandon -> only eligible ownership is released, pending cleanup is
  reported and another Snapshot's bytes remain readable. Invariant: safe draft
  cancellation. Integration test.
- Published Snapshots protected by each retention condition -> set stable,
  retain/release and attempt reclaim, including a protection race and timeout ->
  backend checks win, stable is independent, and pending/unknown/completed states
  remain distinct. Invariant: ownership-safe reclamation. Integration test.
- Small valid text/binary field, corrupt field, oversized field and cancelled
  preview -> request Preview -> verified text/hex only within cap, no large
  payload admission, and no partial success or leaked read resources. Invariant:
  explicit bounded verified reads. Integration test.
- Selected Sample/Field -> navigate away/back while reads finish or Snapshot is
  reclaimed -> restore bounded query/scroll or explicit unavailable state; late
  responses never replace selection and hidden polling stops. Invariant: bounded
  history and stale-response isolation. E2E test.
- Local files and batch mappings -> import and preview using actual services ->
  published values match input within declared bounds; the flow requires no
  external-source connection UI. Invariant: local test/demo import. E2E test.

- SDK-created named Splits and a no-Split/empty-Split fixture -> expand the tree,
  select Split and navigate back -> exact authoritative rows, shared Schema,
  independent bounded paging and restored scope. Invariant: real Split browsing.
  E2E test.
- Imported Samples -> define/edit/rename/remove draft Splits, preview rules or
  weighted partitions, Prepare and Publish -> complete API-validated Splits appear
  in the tree and Rust/Python reads agree. Invalid policy or stale/partial preview
  blocks preparation. Invariant: full Split authoring flow. E2E test.
- Published Split -> Create child using Split, edit inherited memberships and
  publish -> original stays unchanged, child membership matches review and latest
  advances; readonly mode cannot mutate through UI or proxy. Invariant: explicit
  Split lifecycle and mode enforcement. E2E test.
- Selected S1/r1 Split and two editing sessions -> publish r2 then publish stale
  edit -> r2 becomes current on S1, the stale edit conflicts, Dataset latest does
  not move, and r1 remains browsable/readable. Invariant: independent Split revision
  editing. E2E test.
- S1/r2/train -> Create child while another client publishes r3 -> child preview
  and published data use r2 membership, parent link is S1, and history preserves
  revision identity. Invariant: fixed Split-source inheritance. E2E test.

Implementation verification commands (not required for this design-only edit):

```sh
pixi run test-console-ui
pixi run test-console
pixi run test-access
pixi run test-dataset-sdk
pixi run test-dataset-sdk-python
pixi run test-e2e-console
pixi run rs-fmt-check
pixi run rs-lint
pixi run ts-lint
pixi run sdk-fmt-check
pixi run sdk-lint
```
