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
version 2 of the existing candidate binding; version 1 remains an incomplete legacy
preparation unless old successful-publication evidence exists. Complete results are
readable, enumerable, retainable and usable as parents/stable targets without winning
latest. ID reads consult this root rather than publication receipts or mutable heads.

Publication has a candidate identity and a separate caller-owned attempt identity.
Each attempt binds its target and exact expected head generation. Successful commit
receipts remain durable and help incomplete publication finalization after restart.
Retrying a successful historical attempt returns its receipt without rewinding a
later head. On a head conflict the candidate remains available; an explicit new
attempt can use a newly observed head without changing its parent or contents.

Each Snapshot owns immutable Split-set revisions and an independently fenced
currentSplit pointer. Names are scoped to Dataset/Snapshot/revision. Publishing a
new revision replaces the complete set, including rename/removal; old revisions
remain readable. An empty set is a revision, distinct from an absent pointer.

## 2. Composition and inheritance

`CompositionRecord` persists the operation, input revision, source, pinned parent
Split revision, edits, evaluation progress, candidate and ownership state. Edits
use expected-revision CAS and immutable edit receipts. Historical retries return
their original draft result; current status is a separate read.

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

Evaluation advances revision-bound windows of at most twenty Samples. Durable
redo windows precede row writes; CAS progress and content bindings prevent stale
work from becoming the accepted result. Preparation validates the server-assembled
Manifest, initial Split set and all candidate upload claims before publication.
The complete resource root makes data and initial Splits available together; the
independent data head transition selects that resource as latest.

## 3. Split evaluation and publication

Standalone Split drafts pin owner Snapshot and expected current revision at begin.
Preparation checks source availability; deletion may interrupt unfinished work. Whole-set edits invalidate
evaluation; prepare requires complete membership and valid overlap/coverage policy.
Defaults are disjoint and allow-unassigned. Frozen inherited membership is explicit;
reevaluation is a deliberate definition change.

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
The draft publication route is a compatibility adapter for its prepared target and
expected revision; `publish_split_revision` accepts these caller-owned inputs
directly. New draft status remains Prepared after publication.
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

Old snapshots without projections require explicit resumable inspection preparation.
Unsupported or oversized legacy state is reported, never represented as an empty
Snapshot. Field preview is explicit and capped at 64 KiB after read-only admission; it
does not create a read plan or acknowledge application consumption.

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

Uploads are Dataset-scoped and content-verified. Composition registers staging and
candidate upload IDs durably before claims. Publication is gated on complete ownership
preparation, including unchanged shared parent uploads. Retiring one owner never
releases a different owner; permanent owner revocation rejects delayed claims.

Abandonment first fences the draft, then cleanup proceeds in bounded resumable steps.
Obsolete evaluation pages and candidates use durable cursors. Cleanup seals possible
late write keys, including currently absent keys, rather than deleting a value and
allowing a stale CAS(None) writer to recreate it. Edit IDs are registered before
payload writes so failed admissions can also be retired. Small receipt/fence records
remain after bulk cleanup to preserve replay and non-resurrection guarantees.
Split publication does not create those replay records; existing records and
editing/evaluation cleanup remain compatibility inputs.
Helping an already stored edit receipt only reads and validates it; retries do not
submit a doomed CAS for the same bytes. Evaluation journals and cleanup cursors
are scoped to one draft, not to unrelated readers. They retain exact partial-work
recovery and prevent a delayed writer from reviving cleaned candidate data.
Split cleanup competes with preparation on the final revision key. If the complete
root already exists, including a root returned by a failed cleanup CAS, cleanup
preserves its member pages and summary while retiring evaluation intents. If
cleanup seals an absent root first, a delayed preparation cannot publish it.
Abandoning the draft does not delete an already complete revision. Retiring the
owning Snapshot still permits revision cleanup and closes new read admission.
Sources use read-only availability checks. The compatibility
`source_lease_deadline` field is zero for new drafts and edits.

Snapshot reclaim checks latest/stable, retained Snapshots, live children, candidate
ownership under a terminal owner fence. A durable five-minute grace deadline
separates deletion admission from cleanup. After that deadline it retires upload owners,
Snapshot-scoped Split drafts, possible index writes and ordinary metadata in phases.
Completion reports logical retirement; physical upload deletion remains deferred
to the bounded upload-authority scan. There is no independent complete Split revision pruning.

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
