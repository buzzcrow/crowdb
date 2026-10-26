<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Dataset Control Lifecycle

Depends on: [Dataset architecture](design-crowdb-access-dataset.md).
Satisfies: [Dataset specification](spec-crowdb-dataset.md).
Wire contract: [control API v2](openapi-crowdb-dataset-v2.yaml).

This document describes Snapshot composition, whole-set Split revisions and their
bounded control operations. Payload delivery retains the existing binary framing.

## Table of contents

1. [Identity and authority](#1-identity-and-authority)
2. [Composition and inheritance](#2-composition-and-inheritance)
3. [Split evaluation and publication](#3-split-evaluation-and-publication)
4. [Bounded observation and reads](#4-bounded-observation-and-reads)
5. [Ownership and cleanup](#5-ownership-and-cleanup)
6. [Compatibility and invariants](#6-compatibility-and-invariants)

## 1. Identity and authority

A Dataset has opaque latest/stable Snapshot references. A Snapshot may declare any
complete available same-Dataset parent or no parent. Its parent is independent of the head
generation used for publication CAS. Multiple roots and off-latest branches remain
discoverable. A preparation binding or a Published flag alone is not a complete result.
The final Snapshot resource root is installed after Manifest/index rows, the initial
Split revision and necessary payload ownership are ready. The current root uses
version 2 of the existing candidate binding; new staging uses version 3 and is not
readable. Old version 1 bindings use existing publication evidence, or read-only
validation of their Published content identity/digest and complete initial Split.
They never acquire new publication proofs during reads or head replacement. Complete results are
readable, enumerable, retainable and usable as parents/stable targets without winning
latest. ID reads consult this root rather than publication receipts or mutable heads.

Publication selects a complete candidate with an exact expected head generation.
It writes only the fenced head, without attempt/response records, success proofs
or Composition committing/published progress. Compatibility attempt IDs are echoed
in the immediate response and do not create request history. Retrying the same
current latest confirms its current effect; after another Snapshot replaces it,
the stale condition returns the observed latest/stable/generation without rewinding
the head or querying again. Unknown storage outcomes propagate unchanged. The
caller decides how to use an observed base; the server never rebases content.
Existing receipts and attempt-bearing heads are decoded read-only. Before replacing
an old head without complete resource roots, the existing bounded legacy ancestry
anchor preserves those resources; new version 4 heads need no receipt helping or
anchor lookup. Stable-only legacy roots are included in that compatibility index.

The v1 JSON adapter prepares content once before acquiring payload owners. It then
completes that resource and selects it using the existing latest-parent condition;
it does not rerun Manifest/index preparation. Authority validation replaces the
old operation/head preflight. Responses use the known immutable metadata plus one
latest observation, without rereading operation/Snapshot records or copying the
Manifest for a second preparation.

`PUT .../datasets/{dataset}/head` selects an existing complete Snapshot from
`snapshot` and explicit nullable `expected_head`, under `snapshot-head-update`.
It reads no draft or attempt and returns the same latest/stable/generation shape
as GET head. Stable is preserved; failed conditions include `observed_head` in that
shape. GET head reads Dataset identity and head once each. Rust exposes
`ControlClient::update_snapshot_head` and `Head::from_conflict`; Python exposes
`update_snapshot_head` and `ConflictError.observed_head` for synchronous and async
clients. Missing capability never invokes a composition or v1 publication fallback.

Each Snapshot owns immutable Split-set revisions and an independently fenced
currentSplit pointer. Names are scoped to Dataset/Snapshot/revision. Publishing a
new revision replaces the complete set, including rename/removal; old revisions
remain readable until explicit deletion or owner retirement closes admission.
An empty set is a revision, distinct from an absent pointer.

## 2. Composition and inheritance

The native `prepare_composition_resource` accepts a caller-owned Snapshot ID and
complete parent/source/schema/changes/Split inputs. It computes a bounded final
Manifest directly, then writes its immutable partitions and inspection index,
initial Split membership and final payload owners. It completes the existing
resource root last; latest/stable are updated separately. There is no operation
mapping, draft, edit receipt, working row, window intent, staging owner or durable
computation cursor. The native input and final Manifest are bounded to 8 MiB and
the request to 30 seconds, with twenty-Sample source windows. Partial field patches
keep other effective parent fields; source filtering does not prevent an explicit
upsert from reintroducing a parent Sample. Each patch ID appears once. Frozen
Split sources and inherited definitions name exact revisions; retries never resolve
currentSplit again. Patch application moves existing rows and tracks encoded size
incrementally rather than serializing the growing Manifest after every change.

A retry recomputes incomplete work and reuses matching complete content. The
existing content binding follows all final Manifest/index rows, so retrying a
failed owner claim does not rewrite those rows; it still checks unfinished initial
membership and owners before completion. The Snapshot ID fixes its Manifest and initial Split binding; different final content
conflicts, including removing an existing initial Split. Identical complete retries
write no state. Unknown completion replies return directly without a confirmation
read. Resource preparation checks Dataset identity without loading the head.
Managed upload metadata is validated once per unique upload before output writes,
and the final Snapshot owner is acquired before completion. The final Manifest
finds every owner for cleanup; there is no parallel ownership-progress list. Opaque
legacy locators inherited from a parent retain their existing storage contract;
new chunk patches must be valid managed upload locators.

Resource prefixes and the immutable Snapshot record expose interrupted output to
owner retirement. Existing member pages are sealed after the grace period, and
recomputation cannot complete a retired Snapshot. The first Snapshot write stores
its fixed internal `gc_after`; a bounded full scan discovers abandoned resources
even before their content binding exists. Terminal deletion seals that binding,
including a missing key, so delayed creation cannot complete the resource.
`PUT snapshots/{snapshot}` exposes this operation with `composition-preparation`,
when an upload service is configured. Rust `PrepareSnapshot` and sync/async Python
`prepare_snapshot` preserve the same ID/input and never query workflow status or
downgrade. Explicit nullable parent/schema and strict DTOs reject process IDs.
Upload values carry id/length/md5, converted to locators without a metadata query;
the authority checks each unique upload before claiming it. Descriptor mismatches
return conflict. HTTP body/input/Manifest are bounded to 8 MiB. Declared oversized
bodies use the existing bounded discard before response, avoiding an unread-body
reset that can truncate the 413 JSON. Composition and standalone Split workflow
writes return 501 before storage access, and the workflow SDK methods are removed.
Only legacy HTTP observation, abandonment and cleanup remain for existing records.

Legacy `CompositionRecord` stores old input revisions, source IDs, edits, work
progress, candidate and ownership lists. These formats remain cleanup inputs;
current routes neither append edits nor resume them. Status only reads the stored
draft and never helps receipts, queries commit ledgers or advances publication.
Legacy Committing is presented as Prepared without changing stored bytes. A caller
may select an already complete resource with a direct head update; the status does
not prove that a historical request selected the head.

The default source is all parent Samples. Selection filters and a frozen named
Split can restrict the source, with their intersection when both are present.
A source S1/r2/train produces a child with parent S1. It copies exact frozen
membership/provenance, not the future currentSplit value. Inherited Split members
intersect surviving child IDs; newly added IDs remain unassigned unless explicit
assignments or reevaluated definitions include them.

Sample upsert replaces the supplied field bindings while preserving other effective
fields; remove excludes its ID. Effective inherited values are resolved server-side
and materialized as bindings in the complete child Manifest. Inspection reports the
child as the binding source after this copy; chunk payloads remain shared. Sparse
Manifests created through direct publication can still resolve omitted optional
fields from an ancestor and report that ancestor as the source. An unchanged
required field must remain represented in the final complete membership. Tombstone, missing and empty inline
value remain distinct. The current Schema contains only version/name/requiredness.

Computation reads windows of at most twenty Samples into a bounded request-local
Manifest builder. It writes final content directly; no redo window or working row
precedes it. Final resource keys reject conflicting immutable content. The complete
root makes data and initial Splits available together, and the independent data
head transition selects that resource as latest.

## 3. Split evaluation and publication

Standalone Split preparation accepts an owner Snapshot, caller-owned revision ID,
policy and complete definitions in one resource PUT. Preparation checks source
availability; deletion may interrupt unfinished work. It requires valid overlap/
coverage policy. Defaults are disjoint and allow-unassigned. Frozen inherited
membership is explicit; reevaluation is a deliberate definition change. The caller
captures the intended currentSplit revision/generation before computation and keeps
that base separate from the immutable result.

Predicates support all, explicit IDs, prefix, range, keyword, inline field match and
boolean composition over the selected universe. Effective optional inheritance and
tombstones are resolved through metadata. Payload contents are not searched.

Weighted partition uses evaluator version 1: SHA-256 of `crowdb-split-v1\0`, seed
as u64 big endian, Sample-ID length as u32 big endian, then exact ID bytes. Interpret
the first eight digest bytes as u64 big endian. Its bucket is
`(u128(hash) * sum(weights)) >> 64`; ordered cumulative weights select the name.
Weight sums are checked u32 values. This is deterministic approximate partitioning,
not a promise of exact cardinality. Overlap/coverage validation remains independent.

Membership pages are immutable and checksum-bound. Preparation persists the whole
revision before currentSplit CAS. A conflict preserves the candidate and caller
inputs; retry never silently rebases definitions or overwrites another revision.
The native `prepare_split_revision` operation accepts caller-owned revision ID,
policy and complete definitions. It recomputes incomplete work from those inputs,
writing only final member pages, the resource enumeration index and complete
revision root. The index precedes pages; the root is installed last. No draft,
edit receipt, window intent or phase cursor is written. Input and final bindings
are capped at 1 MiB; computation uses twenty-Sample windows within a 30-second
request deadline. It retains one window and the bounded bindings, encodes each
member page once, and tracks binding size incrementally. An identical complete
resource is reusable without recomputation; different input at that ID conflicts.
Unknown writes propagate without confirmation reads. Frozen inherited sources use
the supplied revision, regardless of later head changes. Snapshot retirement
finds both complete and partial output through resource prefixes without drafts;
complete revisions have no independent automatic expiry. Explicit revision deletion
and its bounded resweeps are described under ownership and cleanup.
The complete `SplitRevisionRecord` is the final result: an explicit revision ID
can be read, listed or used as an inherited source without winning currentSplit.
Read admission still checks the owning Snapshot. Reads load that immutable record
directly without Split publication proofs, currentSplit lookups or helper writes.
Standalone Split publication writes only currentSplit. It creates no attempt,
response receipt, per-revision success proof or draft committing/published state.
A retry returns the current generation when it still names the same revision;
after a later revision replaces it, the stale request returns a precondition
conflict without replaying historical success or rewinding the pointer. A definite
CAS conflict uses its returned head; a transport error remains unknown. Legacy
receipts and attempt-bearing heads remain readable without backfilling receipts.
`PUT snapshots/{snapshot}/split-revisions/{revision}` exposes preparation with the
`split-preparation` capability. Rust `PrepareSplitRevision` and synchronous/async
Python `prepare_split_revision` retain the exact resource ID and complete input on
retry; an error carries the revision identity. Missing capability never invokes a
draft or v1 mutation. Standalone begin/edit/evaluate/prepare/publish workflow routes
return 501 before storage access; SDK workflow methods are removed. Only legacy
draft observation, abandonment and cleanup remain for existing records. Initial Split computation in `prepare_snapshot` shares the same pure result
preparation against its fixed incomplete owner; it writes no draft or progress.
Legacy internal draft drivers remain for migration/cleanup and are not called by
the resource preparation routes.
`PUT snapshots/{snapshot}/split-head` accepts the target revision and both explicit
nullable expected revision/generation. It requires the `split-head-update`
capability and has no draft or attempt identity. The failed head condition is
returned as `observed_head`, including an observed absence. It is an observation,
not a promise that another writer has not changed the head since. The caller may
review the new base and use it in a new conditional request; no rebase is implicit.
Rust exposes the observation through `SplitHead::from_conflict`; Python exposes
`ConflictError.observed_head`. Missing legacy observations do not trigger an
automatic head query or a mutation downgrade.

## 4. Bounded observation and reads

Inspection projections contain complete summaries, Schema rows, Sample IDs, field
metadata and separate value bindings. Ordinary inspection and indexed read planning
do not load a complete Manifest or chunk payload. Inventory and parent/child indexes
filter visibility through the complete resource root, with read-only compatibility
for old publication evidence, not merely candidate record state.

Preparing an index overlaps at most sixteen immutable row writes, with at most
2 MiB of encoded keys and values queued. Encoding holds at most one additional
row under the existing per-row bound. No row is copied into another batch buffer.
All row writes must succeed before inventory, parent/child and ready summaries
are installed. Failure leaves the index unavailable; repeating the request checks
the same immutable rows. This bounds parallel work while preserving the existing
number of conditional mutations and each row's identity.

List pages default to twenty items and cap at one hundred with a 1 MiB metadata
window. Exact matrices admit twenty IDs by twelve fields. IDs preserve up to 16 KiB
of binary data; Schema admits up to 256 fields with names up to 1024 bytes. Projection
and selection use aggregate byte/deadline/cancellation budgets. Native bounded reads
reject oversized external values before materializing their contents.

Continuation delivery resolves selection and field locators within one request.
It reuses that request's Snapshot admission, schema and already scanned membership;
Split visibility checks reuse admission for the same Snapshot. No admission handle
is returned to callers. A separate request checks deletion again, including empty
results. Explicit batch reads remain independent admitted requests. HTTP delivery
moves inline payload owners from resolved locators into output buffers without
cloning the payload.

Delivery resolves membership and field bindings in windows of at most sixteen
cells. Independent point reads overlap within one ancestry level; unresolved
fields proceed to the parent only after its read-only admission. Ancestor summaries
remain local to the request. Each read reserves its response cap before dispatch,
with at most sixteen requests and 2 MiB of response capacity in flight. Completed
rows consume the aggregate metadata budget, including responses waiting behind
an earlier read. Unused reservations are released before admitting a smaller final
read. Request order, missing fields, tombstones and locator integrity checks remain
independent of completion order. All windows share the request deadline and
cancellation; no reader state is persisted.

The bounded compatibility part reader checks the declared logical length before
allocation. A single buffer covering that complete object is transferred with its
allocation owner after successful EOF. Fragmented objects allocate their final
buffer once; each source buffer is copied and released before the next storage
pull, so a one-slot reader can make progress. Short, oversized and failed terminal
reads remain errors. Whole-part digest verification still runs on the returned
bytes before acceptance.

Old snapshots without projections require an explicit complete inspection rebuild.
One request bounds the source record at 9 MiB, its embedded Manifest at 8 MiB and
the entire rebuild at thirty seconds. Final rows use at most sixteen pending
writes and 2 MiB of encoded keys and values in flight. The builder writes complete
rows, inventory and parent-child discovery before installing the immutable summary
root. Partial rows are invisible without that root. A failed request returns its
error and a retry recomputes from the source; it saves no preparation cursor or
request receipt. An existing summary returns without writes. Cleanup and the
finalizer conditionally replace the same root, so a retired root rejects a delayed
finalizer.

Status can decode historical preparation progress when no summary exists, but a
complete rebuild never resumes or rewrites that progress. The summary takes
precedence over historical progress, including corrupt progress. New successful
responses retain zero revision and row-count fields for wire compatibility.
Unsupported or oversized sources return errors instead of empty Snapshots. Field
preview is explicit and capped at 64 KiB after read-only admission; it does not
create a read plan or acknowledge application consumption.

V2 read planning pins Snapshot and optional Split revision once, intersects Selection
with Split membership, then orders/shuffles and partitions workers before fetching
projected payloads. Planning advances twenty-Sample windows with fixed total ID/byte
limits. Fetch/preview/status never save consumption progress. The client saves the next
continuation after processing; acknowledgement is local. Cancel stops the current
request without a reader registry or remote close.

## 5. Ownership and cleanup

Incomplete-candidate cleanup and finalization conditionally replace the same
resource root. A cleanup seal prevents a delayed finalizer from exposing retired
rows. If the complete root wins, abandoning or superseding the draft preserves its
Manifest, initial Split members and payload owners; ordinary Snapshot reclamation
later retires them under deletion admission and grace rules. Completion transport
errors propagate without confirmation reads. Definite cleanup conflicts use the
returned value without reloading the root.

The parent/child index is installed before the child's complete root. Finalization
rechecks parent availability after that index write. Parent deletion conservatively
protects any indexed child without terminal retirement, including an incomplete
child awaiting cleanup. Thus deletion either observes the child or the child's
last parent check rejects completion. Readers never register in this protocol.
Reclaim admission may remove an exact child-index entry after validating its
binding and the child's permanent retirement authority. Completed removals shrink
later bounded scans; the retirement authority remains even if a delayed writer
recreates the derived index. A failed or timed-out index deletion returns its
unknown outcome without compensation writes. A fresh request rechecks authority
and can resume this resource cleanup; ordinary inspection remains read-only.
Discovery and per-resource positions stay in memory. Restart may repeat already
sealed prefixes, so progress requires enough consecutive bounded windows to pass
those prefixes. Grace deadlines specify earliest reclamation, not a completion
time or a guarantee under arbitrary restart frequency.

Uploads are Dataset-scoped and content-verified. Resource preparation claims only
the final Snapshot owner for each unique managed upload in its final Manifest,
including unchanged parent uploads. Completion requires all those owners. The
Manifest supplies cleanup's owner references without a second process list.
Retiring one owner preserves other owners; permanent revocation rejects delayed
claims. Legacy staging/candidate lists remain cleanup inputs for old records.

Legacy draft abandonment and cleanup use their existing bounded cursors and seals
for old work, including keys that could still receive delayed writes. Stored edit
receipts and progress can be decoded and reclaimed, but resource preparation never
creates or resumes them. Their independent discovery and replacement remain part
of migration work; removing the last discovery path would lose owned garbage.

Legacy draft cleanup competes with preparation on the final revision key. If the
complete root already exists, including a root returned by a failed cleanup CAS, cleanup
preserves its member pages and summary while retiring evaluation intents. If
cleanup seals an absent root first, a delayed preparation cannot publish it.
Abandoning the draft does not delete an already complete revision. Retiring the
owning Snapshot still permits revision cleanup and closes new read admission.
Sources use read-only availability checks. Legacy `source_lease_deadline` is only
a compatibility field; resource preparation has no reader lease or draft.

Complete Split revisions normally remain owned by their Snapshot and are reclaimed
with it; there is no independent revision TTL. Explicit
`DELETE snapshots/{snapshot}/split-revisions/{revision}` takes only those two IDs
and an empty body. It rejects the current head and the Snapshot's immutable
initial revision with 409, including an initial revision that is no longer current.
A missing revision returns 404. A successful mark immediately closes new revision
reads and frozen-source admission; ordinary requests return 404. Existing bounded
read limits still apply and may return a limit error before the closed record is
decoded. Previously admitted
work may continue until cleanup, and may fail if it outlives the grace period.

Deletion state and the fixed twenty-minute deadline replace the complete root on
the same revision key, retaining its page bindings for cleanup. The head generation
is fenced against a publisher that observed the revision before deletion. There
is no separate deletion task, reader session or request ledger. Repeating DELETE
preserves the stored deadline; an unknown mutation returns without a confirmation
read, and the client retries the same two IDs. The bounded background worker scans
revision resources using discardable local cursors, seals membership metadata and
rescans terminal resources for delayed member writes. It does not delete Samples,
upload owners or chunk payloads. If Snapshot deletion has already closed every
owner read and its own twenty-minute grace has elapsed, revision cleanup may use
that earlier owner fence without waiting for the revision's later deadline.

DELETE returns 200 with `snapshot`, `revision`, `complete` and `reclaim_after`.
Pending deletion reports `complete=false` and the fixed absolute Unix-seconds
deadline as a canonical decimal string. A terminal resource reports
`complete=true` and an explicit null deadline; it retains no historical response
from which to reconstruct the old deadline. The `split-revision-delete` capability
is independent of revision reads and preparation. Rust and synchronous/async Python
expose `delete_split_revision(snapshot, revision)`; missing capability never invokes
legacy draft abandonment. The native adapter calls the same authority under local
admission with no additional metadata GET and no automatic unknown-outcome retry.

Snapshot reclaim checks latest/stable, retained Snapshots, live children, candidate
ownership under a terminal owner fence. New deletion marks have a durable twenty-minute grace deadline;
existing marks keep their persisted absolute deadline. The grace period
separates deletion admission from cleanup. After that deadline it retires upload owners,
Snapshot-scoped Split drafts, possible index writes and ordinary metadata in phases.
Completion reports logical retirement; physical upload deletion remains deferred
to the bounded upload-authority scan. Complete Split metadata also supports the
explicit revision deletion described above.

`GET snapshots/{snapshot}/reclaim` is bounded, read-only observation. It returns
publication operation identity where available, latest/stable/retained flags,
`reclaim_after` and the compatibility `active_read_lease=false` field, and not_started/pending/completed retirement state. Parent reachability is
explicitly not evaluated in this small read; observed flags are never permission
to reclaim. Rust/Python `control().lifecycle(snapshot)` share this contract.
Metadata HTTP reads remain capped at five seconds, while bounded mutation steps
use their configured mutation deadline (normally 60 seconds).

## 6. Compatibility and invariants

Control v2 exposes strict DTOs and capability discovery. Required null preconditions
cannot be omitted; u64s use decimal strings; binary IDs use canonical base64. Clients
check capabilities before mutation and never downgrade a Split-aware operation to v1.
Existing v1 routes, persisted read interpretation and binary framing remain available.
Rust and Python control clients share transport/admission/retry/cancellation semantics.
SDK clones share a fixed atomic bitset of capabilities previously advertised by
the versioned endpoint. Positive hints remove discovery GETs from warm control
calls; absent capabilities are rediscovered, never downgraded. Explicit
`capabilities()` queries replace hints after a valid response and clear them for
an unsupported API version. Unknown transport outcomes do not imply capability
revocation. Concurrent cold calls may duplicate discovery under the existing
HTTP budget; there is no discovery lock or server session. Every actual operation
still passes server authentication, capacity, deletion admission and mutation
preconditions. Server redeployment can invalidate a hint and make the operation
fail; explicit discovery refreshes it. A preflight GET could not eliminate that
deployment window either.

- **I1 — Immutable data:** committed Snapshot content never changes.
- **I2 — Independent revision:** Split publication cannot move Dataset latest.
- **I3 — Frozen source:** source revision identity survives later pointer changes.
- **I4 — Complete publication:** data, initial membership and upload ownership are
  ready before the visible head transition.
- **I5 — Exact retry:** a successful attempt cannot create another Snapshot or
  rewind a later head.
- **I6 — Bounded work:** page, byte and time budgets constrain underlying storage
  work as well as response length.
- **I7 — Terminal retirement:** delayed writers cannot restore retired work or
  acquire a revoked owner after cleanup.
