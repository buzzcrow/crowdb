<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Chunk KV Server

`crowdb-chunk-kv-server` is the standalone ownership, routing, and network
boundary for chunk-backed metadata partitions. It hosts zero or many R142
partition handles but never owns or bypasses a raw tree or stream. Group 0 is
the durable catalog and monitor authority; monitor-issued serving grants are
the only authority to admit data requests.

## Contents

- [1. Catalog Publication](#1-catalog-publication)
- [2. Domain Monitors](#2-domain-monitors)
- [3. Serving Authority](#3-serving-authority)
- [4. Request Contract](#4-request-contract)
- [5. Ordered Reads](#5-ordered-reads)
- [6. Split Publication](#6-split-publication)
- [7. Child Balance State Machine](#7-child-balance-state-machine)
- [8. Placement Policy](#8-placement-policy)
- [9. Lifecycle and Observability](#9-lifecycle-and-observability)
- [Open Issues](#open-issues)

## 1. Catalog Publication

The catalog is one checksummed generation head over ordered immutable pages.
Each entry contains an exact half-open binary-key range, stable partition ID,
owner endpoint, monotonic owner epoch, lifecycle state, stable tree ID, stable
stream name, optional tail-overlay artifact, and optional transition ID. The
overlay separately binds a chunk root-catalog generation, a tree snapshot
sequence and applied sequence, source partition/epoch/stream and stream
manifest generation, retained replay and cutover offsets, cutover sequence,
and target-WAL start sequence. A valid generation starts at the empty byte string, has
exact adjacent bounds, ends unbounded, and covers each binary key once.

Publishers validate the complete successor, including retained-partition epoch
non-regression, before I/O. They write only new pages, reread and validate every
referenced page, and write the head last. An ambiguous head write succeeds only
when rereading proves the exact intended head. Readers retain their last valid
immutable snapshot when a new head or page is absent, corrupt, incomplete, or
regressing. Unchanged pages may be referenced by later heads through their
original page generation.

## 2. Domain Monitors

`EnsureDomainMonitor` carries a persisted descriptor with domain and registry
names, compiled driver and capability versions, failure policy, balance policy,
and all health/lease timing. Identical registration is idempotent. Any field
conflict fails closed, and an unsupported compiled driver returns
`UnsupportedMonitorDomain` without changing the prior descriptor.

Every group-0 replica prepares tasks from persisted descriptors. A staged tick
may observe, plan, and publish only under the current group-0 leader fence.
Registry, catalog, binding, or transition observation failure ends the tick
before planning; it is never represented as an empty cluster. A supervisor
restarts failed tasks from durable state and a new leader resumes outstanding
transitions.

Normal service discovery filters expired registrations. Monitor observation
uses the raw scan, preserving expired entries and heartbeat timestamps so
absence cannot be confused with owner death. The default heartbeat is two
seconds, `Suspect` begins at six seconds, and `Dead` recovery begins at ten.

## 3. Serving Authority

The monitor issues one aggregate grant per instance. It contains a monotonic
lease sequence, catalog generation, wall-clock expiry, sorted exact
`(partition_id, owner_epoch)` assignments, and their canonical digest. An
instance heartbeat advertises health but cannot authorize a request.

On receipt, an owner subtracts maximum clock skew and its self-fence margin
from the wall-clock remaining duration, then projects that duration onto its
local monotonic clock. Request admission reads an immutable grant snapshot and
checks its local deadline, catalog generation, partition identity, and epoch
without taking a lock. The default 12-second lease, one-second skew, and
one-second self-fence margin fence the old owner by ten seconds. A replacement
waits until the old wall-clock expiry plus skew unless an explicit source fence
is durably proven.

Client catalog revisions and owner epochs are routing references, not serving
authority. A server may resolve an older reference through a process-local
lineage, but it always authorizes the resolved current assignment against one
internally consistent catalog and aggregate grant. The grant's catalog
generation and exact `(partition_id, owner_epoch)` assignment remain mandatory;
the selected partition repeats the epoch check at its WAL and tree boundary.
This separates stale-client tolerance from owner fencing without weakening
either fence.

Catalog reconciliation matches the tree identity as well as partition identity,
epoch, range, and stream. When a Serving descriptor has materialized its tail
overlay, an unactivated Prepared overlay handle must be replaced by recovery
from the independent checkpoint. It cannot satisfy the materialized assignment
or bypass the split commit proof. Ordinary grant activation applies only after
that independent recovery has completed.

## 4. Request Contract

Every request carries a random 128-bit client instance ID, nonzero monotonic
client sequence, catalog revision, partition ID, owner epoch, optional minimum
journal position, and optional deadline. The logical request identity survives
transport retry and rerouting. Endpoint, socket, and connection identities do
not participate in deduplication.

The contacted server validates the deadline and request envelope, resolves the
key or logical range against its current catalog and recorded local lineage,
then authorizes every partition it will access against the current aggregate
grant before R142 admission. A client topology mismatch is observed for
refresh and metrics but is not itself an admission failure when the current
owner can resolve and authorize the request locally. A same-owner split keeps
an old parent dispatcher for point, multi-get, batch, seek, and scan requests;
the dispatcher selects the current writer by key and never proxies to another
node. If the current assignment is remote, unprepared, catching up, or absent
from the local grant, the server returns the corresponding typed failure and
performs no WAL I/O.

Lineage dispatch preserves the original request identity and minimum journal
position. A selected writer accepts either its own stream position or an
inherited source-stream position covered by its durable overlay; it rejects an
unrelated stream instead of silently discarding the ordering requirement.
Point operations preserve get, put, delete, put-if-absent, compare-exchange,
and conditional-delete conditions and results. Successful and failed
conditions return the R142 journal position. `Overloaded`, `WriteStalled`,
`Recovering`, `TargetNotReady`, `LeaseExpired`, `RequestExpired`,
`RequestConflict`, `NotMyRange`, and `RefreshRequired` remain distinct wire
outcomes.

A deadline observed before sequencer admission creates no WAL record. Once the
sequencer accepts a mutation, dropping the transport response does not cancel
the partition worker; retrying the same identity retrieves the recorded result.
Reads may carry a mutation's returned journal position for explicit
read-after-write ordering.

## 5. Ordered Reads

Seek provides ceiling, higher, floor, and lower operations within one logical
partition view. A same-owner split seek may cross the local writer boundary.
Scan intervals are validated and clipped once against the logical routed range;
the dispatcher visits retained and child writers in key order, or reverse key
order for a reverse scan. A forward scan includes its lower bound and excludes
its upper bound. Its continuation resumes strictly after the last emitted key
so the split key and page boundaries neither duplicate nor skip a key. Reverse
scans use the corresponding exclusive upper cursor.

A continuation binds direction, last key, and the request's logical partition,
epoch, and catalog revision. It remains valid across a same-owner split while
that exact local lineage is retained, because the last key determines the next
writer. A mismatched continuation envelope or an owner transfer that removes
the local lineage returns `RefreshRequired` or `NotMyRange`; the server never
guesses a remote resume position. Multi-partition composition belongs to the
routed client.

## 6. Split Publication

### 6.1. Publication boundary and durable steps

The group-0 catalog head publication is the external range and assignment
cutover. A local writer handoff is a separate operation: the same process can
dispatch the old parent route to the retained parent and child before the new
catalog is visible. This compatibility must preserve every acknowledged WAL
record; it does not authorize a different owner to serve an unpublished range.

The parent retains its existing tree and journal. Only the child receives new
storage. Handoff is parent-local range dispatch, not replacement of the parent
writer with another tree/WAL and not remote ownership transfer. The successful
durable handoff status update commits the obligation to resume this split;
in-memory dispatch changes afterward. Handoff status is distinct from final
`ChildPrepared` readiness and from external catalog publication. A candidate
epoch or a process-local dispatcher cannot substitute for this status.

- **S1 — Plan:** persist the transition identity, expected parent assignment,
  split key, both successor epochs and complete storage identities. Reserve the
  affected partition and owner against another topology transition.
- **S2 — Prepare:** persist `ParentPreparing` before creating artifacts. Pin the
  exact base, construct the bounded child view and establish the common WAL
  frontier `C`. Persist the exact handoff status before changing local dispatch.
  Parent journal appends and reads continue while the status is persisted;
  no handoff waiting queue or mutation-worker wait for that update is allowed.
  Bind the original parent storage, child storage, pinned base and replay
  boundaries. Cover the additional parent suffix before actual dispatch in
  child recovery; fixing `C` does not by itself end parent right-range writes.
- **S3 — Ready:** persist the exact retained-parent and child artifacts,
  overlays, common cursor and readiness proof as `ChildPrepared`. A process-local
  handle or heartbeat cannot substitute for this durable proof.
- **S4 — Publish:** validate that the current catalog still contains the planned
  parent. Publish one successor head replacing it with two adjacent Serving
  entries. This head is the external split commit point. On an ambiguous result,
  reread the authoritative head and accept only the exact intended successors;
  an unrelated newer generation is not proof that this split committed.
- **S5 — Reconcile:** persist `CatalogCommitted` after proving publication.
  Install the matching catalog and grants; preserve old-route dispatch without
  allowing a retired writer to append. Restart must use the published assignment
  together with its exact durable split proof.
- **S6 — Materialize and retire:** checkpoint independently recoverable halves,
  publish removal of their overlays and transition markers, then release pins.
  Reclaim only artifacts with no catalog, replay, retry or pin references.

Crash recovery must distinguish these boundaries:

- Before handoff status commit, recover the published parent and retry or abort
  preparation; no child mutation may have been acknowledged.
- After status commit but before in-memory dispatch, resume the same split.
  Recover the complete original parent journal, including writes made while the
  status update was in flight; an empty child WAL is valid.
- After dispatch but before catalog publication, resume the same split from the
  original parent journal and the child journal. The old parent route remains
  published; do not discard the child or choose authority by the maximum local
  epoch. Neither final readiness nor catalog publication is needed to establish
  that the committed handoff must be recovered.
- An uncertain handoff update requires exact durable transition reread. Stale
  preparation or abort work must not overwrite a committed handoff.
  While the process remains live, retain and retry the exact prepared child
  base, WAL handle and pins. Do not reopen its WAL, release its pins or rebuild
  a different candidate merely because publication returned an error. Writes
  arriving on the parent meanwhile are covered by the later dispatch frontier.
  Once dispatch is installed, its live writers own finalization; only a proven
  pre-handoff abort may discard the pending candidate.
- After publication but before `CatalogCommitted` persistence, prove the exact
  successor entries and finish the transition marker; do not roll back the split.
- After publication, recover each assigned half's complete own WAL. The child
  inherits only the parent prefix through the final readiness frontier `C2`
  recorded in its catalog artifact, then replays its own WAL. This bound applies
  even when the child WAL is empty: later retained-parent records belong to the
  parent's next epoch and must not extend child inheritance. Require matching
  grants before admitting requests.

The initial handoff status fixes `C`, before the eventual in-memory dispatch
frontier `C2`. While only that initial status exists, recovery derives `C2` from
the first child record's sequence minus one, or inherits the parent tail when
the child WAL is empty. Final readiness records the exact boundary explicitly;
published catalog recovery uses that boundary instead of rediscovering it from
the current parent tail.

The durable handoff proof is stored in the split transition while its phase is
`ParentPreparing`. Revision CAS preserves an existing proof and reconciles an
unknown write result by exact reread. Initial and final replay frontiers remain
distinct; additional real-process crash windows are recorded under Open Issues.

### 6.2. Local execution and storage retention

The split transition phase symbols are `Planned`, `ParentPreparing`,
`ChildPrepared`, `CatalogCommitted`, and `Aborted`. A separately persisted
`handoff_proof` distinguishes preparation from committed local handoff before
final readiness. It binds the pinned tree base, original parent stream and
initial inherited frontier while the transition remains `ParentPreparing`.
Final readiness extends that frontier without replacing its base. The server and
group-0 monitor advance them in this order:

1. Group 0 persists the complete plan before local work begins. The existing
   parent keeps its identity, tree, stream, owner, and lower boundary; its next
   epoch and the one new child's identity, range, epoch, tree, and stream are
   fixed by the plan.
2. The parent process persists `ParentPreparing`, renews its serving grant as
   an active owner, checkpoints the exact parent base, builds the child's
   range-bounded base, fixes `C`, and persists the handoff status without stopping
   parent writes or reads. It then installs parent-to-child dispatch. Left-range
   writes retain the original parent journal; right-range writes move to the
   child journal only when dispatch changes. Replay coverage includes parent
   writes between the fixed frontier and that dispatch change.
3. Readiness binds the exact common cutover, the retained-parent next epoch,
   and the child's complete tail overlay.
   It is persisted before catalog publication.
4. One catalog generation atomically replaces the old parent entry with a
   smaller entry having the same partition/tree/stream identity and the next
   epoch, then inserts the one child entry. A retry accepts an already-published
   result only if transition IDs, ranges, owners, epochs, trees, streams, and
   the child overlay all match.
5. The parent owner has already activated both local writers before publishing
   the catalog. Catalog reconciliation adopts those handles for external
   routing; it neither resumes the retained parent nor activates either writer.
   An old parent route dispatches point, grouped, seek, and scan operations
   through the retained local lineage. The current catalog and grant authorize
   the actual writer or writers used by the operation.

Before child materialization, heartbeat load reports the child as dependent.
After process restart, the retained parent reopens its original tree and WAL
through ordinary range-filtered recovery; it has no split tail overlay. The
child reopens its exact pinned base, replays the range-filtered parent suffix,
and then replays its own WAL. A matching serving grant alone cannot activate
the child overlay. Startup loads the persisted split transition and requires its committed
phase, transition identity, range, owner, epoch and complete storage artifact to
match the current Serving catalog entry. Transfer overlays retain their separate
committed-transfer validation. Missing or conflicting evidence leaves the writer
Prepared; repeated matching grant refresh is idempotent.

The local maintenance loop performs bounded ownership materialization and a
serving checkpoint. A heartbeat then proves independent recovery, group 0
publishes a generation that clears both the overlay and the completed split
`transition_id`, and only that catalog state is eligible for another split or
owner balance. Releasing both markers atomically prevents a later transfer from
mistaking the independently recoverable child for an active split participant.

The child root-catalog generation is pinned under the split transition identity
before the server persists child readiness. The pin key is scoped by child tree
ID and its value is the exact root generation; it survives both tree and server
restart. Root reclamation treats the oldest durable pin as an upper bound. The
owner releases the pin only after installing the newer catalog generation whose
child has no overlay and whose parent and child have no split marker. Pin delete
is idempotent and remains reconciliation work after an ambiguous response or a
crash. An abort before handoff commit may remove unpublished child state and its
pin. After handoff commit, absence of readiness or a child catalog entry is not
permission to abort or delete child storage; recover and complete the same split.

The parent also retains its existing checkpoint generation while the child
depends on the inherited parent WAL prefix. Publishing that prefix into the
child's live tree does not release this dependency: the pinned recovery base
still needs those journal records after a crash. Parent checkpoint publication
remains suppressed by the existing transition pin until the catalog records an
independent child. Parent reads and journal appends continue throughout this
interval. The same completed catalog releases both generation pins.

## 7. Child Balance State Machine

### 7.1. Owner cutover and durable steps

The first successful catalog head publication naming the target in
`TargetCatchingUp` is the owner cutover. The later `Serving` publication changes
readiness, not ownership. A candidate target epoch in a plan is not effective
tree or serving authority. Source release precedes the owner cutover and creates
a recovery obligation; it is not itself a catalog owner change.

- **T1 — Plan:** persist the exact source assignment, target identity and higher
  epoch, artifacts, lease bounds and transition identity before preparation.
- **T2 — Prepare source:** persist `SourcePreparing`, pin an immutable root,
  initialize the independent target WAL and record replay start cursor `P`.
  Source remains the published owner and accepts writes under its existing grant.
- **T3 — Prepare target:** persist `TargetPreparing`; open the pinned base and
  replay the source suffix into a Prepared view. Persist readiness before
  `TargetPrepared`. Preparation must not advance the shared tree's effective
  owner epoch or disable source recovery. Target cannot admit data mutations.
- **T4 — Seal source:** after exact target readiness, close source admission,
  drain admitted writes and persist release proof with final durable cursor `C`.
  With an unreachable source, wait for lease exclusion and recover the complete
  durable tail instead of treating `P` as final. No target data admission is
  allowed while the catalog still names source. A durable release cannot be
  undone merely because the catalog still has the old assignment.
- **T5 — Publish owner:** validate the source assignment and durable release,
  then publish the target epoch, `TargetCatchingUp` and complete replay artifacts
  in one catalog successor. Resolve ambiguous publication by exact head reread.
  Effective target storage authority must follow this published assignment;
  a crash before that authority update is completed must allow idempotent repair.
- **T6 — Catch up:** recover the complete source prefix through `C`, followed by
  the target WAL. The published release proof permits durable unconditional
  target appends; reads and conditional operations wait for the complete view.
  Source cannot append, including through a stale client route or old grant.
- **T7 — Serve:** persist final readiness, publish the same target assignment
  as Serving, persist `CatalogCommitted` and activate only with its exact grant.
  A crash between head publication and phase persistence must finish the marker
  from exact catalog evidence rather than strand or reverse the assignment.
- **T8 — Retire:** clear the completed overlay only after independent recovery
  is proven. Release pins and reclaim old resources only after all recovery,
  retry and forwarding references have cleared.

Restart first reads the authoritative catalog and then the matching durable
transition. Before release it restores source; after release but before owner
publication it keeps source fenced and completes publication; after owner
publication it restores target. A local tree epoch, heartbeat or cached page
cannot independently choose the owner. Missing or conflicting proof fails closed.

An abort is permitted only before durable release and owner publication. Persist
the exact abort before discarding the prepared target; stale workers must not
publish or activate that aborted transition. Candidate epochs are not reused,
and cleanup must retain any source-referenced base, WAL or pin. Restoring source
does not require synchronous deletion of unrelated target temporary objects.

The effective-tree-authority ordering and publication/phase crash windows still
require implementation verification; known gaps are recorded under Open Issues.

### 7.2. Execution, proofs and recovery

A balance transition persists source and target owners, increasing target
epoch, source and target artifacts, readiness limits, old-grant deadline,
phase, release proof, initial readiness proof, final catch-up proof, and
failure. The live-source phases and actions are:

1. `Planned` → `SourcePreparing`: the source remains `Serving`, checkpoints a
   pinned base, creates the target-owned empty WAL, and records the initial
   source cursor in the target overlay.
2. `TargetPreparing`: the remote target validates the range, page-root and
   stream identities, opens the exact immutable tree-manifest generation named
   by the source-base proof rather than the latest root, and starts one shared
   async initialization coroutine that replays the source suffix into a
   `Prepared` overlay. The source remains writable. The live target handle and
   coroutine survive into final catch-up; reopening the same pinned generation
   is required only after target process or handle loss. Failure here may abort;
   source authority is unchanged.
   Prepared transfer handles are staged separately from catalog-owned handles.
   Refreshing unrelated catalog entries cannot evict their heartbeat readiness.
   Publishing the matching target assignment promotes the same handle before
   retiring staging; authoritative abort removes only the exact prepared epoch.
   After restart, persisted `TargetPrepared` or `AwaitingFence` evidence restores
   that readiness without rewinding the transition or granting serving authority.
3. `TargetPrepared` or `AwaitingFence`: the monitor requests release only when
   record, byte, estimated catch-up, deadline, capacity, request-rate,
   cooldown, and one-transition-per-owner bounds pass. The source enters one
   explicit fencing lifecycle that closes new admission while the existing
   mutation worker drains only already-admitted requests, then persists
   sequence and byte cursor `C`. If the source is unreachable, the transition waits until old grant
   expiry plus skew. A live handoff enters `AwaitingFence` only after group 0
   observes a healthy target heartbeat hosting the exact partition and target
   epoch. If the source dies before an explicit fence, the unpublished target
   overlay is discarded and the lease-excluded target reopens the source
   stream directly, so recovery observes its complete durable tail rather than
   treating preparation cursor `P` as the final cursor.
4. `TargetCatchingUp`: group 0 publishes the target owner and epoch with catalog
   state `TargetCatchingUp`. The source is no longer catalog authority and
   returns the target hint without appending. The persisted release proof is
   narrow target journal authority: ordinary unconditional mutations append
   immediately to the target WAL from `C+1`, while reads and conditional
   mutations coroutine-await target initialization within their request
   deadlines. Each wait is capped by the smaller of the remaining request
   deadline and a server initialization-wait budget. A process-wide lock-free
   admission counter bounds total initialization waiters; a full counter or
   server-cap timeout returns `TargetNotReady` with a retry delay. Awaiting does
   not block an executor thread and uses no separate transfer request queue.
   Successful unconditional writes mean the target WAL append is durable; they
   do not consume initialization-wait capacity. Every state other than
   `TargetCatchingUp` retains its simpler existing request path.
5. `CatchupPublished`: the existing target coroutine incrementally replays only
   the sealed source suffix after its preparation cursor through `C`, then
   persists the final catch-up proof. Target-WAL records already admitted after
   `C` remain durable and are applied after that complete source prefix. The
   target cannot answer a read or evaluate a condition from a shorter prefix.
6. `TargetReady`: group 0 replaces the catching-up entry with `Serving` in a
   second generation. Only a heartbeat advertising the exact prepared target
   allows a matching serving grant. The transition then becomes
   `CatalogCommitted`.

An owner-loss transfer created after lease exclusion may adopt the original
tree and stream under the higher epoch instead of constructing a live overlay.
This path is valid only after old lease expiry plus skew; writer-epoch adoption
then fences any lower-epoch stream handle.

Recovery uses persisted evidence only. Before source release, a target failure
leaves the source serving and target preparation is repeatable. After release,
abort is forbidden: catalog reread decides whether to publish or resume
catch-up. An ambiguous catalog write succeeds only if the exact intended entry
is observed. A restarted target reconstructs from base plus source and target
tails. A restarted source cannot resume writes after an explicit release or
lease-exclusion proof. Heartbeats and loaded pages are readiness observations,
never authority proofs.

The recovery decision matrix is:

| Durable evidence                                    | Authoritative action                                                                        |
|-----------------------------------------------------|---------------------------------------------------------------------------------------------|
| Plan or source base only; no target readiness       | Keep source serving; retry preparation or abort unpublished target state                    |
| Initial target readiness; no source release         | Keep source serving; recheck budgets before requesting the fence                            |
| Explicit release; catalog still names source        | Keep source fenced; publish `TargetCatchingUp` or resolve head ambiguity                     |
| Lease exclusion; catalog still names source         | Recover target under the higher epoch; source cannot reactivate                             |
| Catalog names `TargetCatchingUp`; no final proof     | Coroutine-await reads/conditions; append unconditional target WAL; replay through `C`        |
| Final catch-up proof; catching-up catalog entry      | Publish the exact `Serving` successor and then issue the target grant                        |
| Serving catalog entry; grant absent or expired       | Keep target prepared and reject admission until the exact grant arrives                     |
| Ambiguous catalog head write                         | Reread head and pages; accept only the byte-exact intended generation                       |
| Conflicting artifact, cursor, range, epoch, or proof | Fail closed; never infer authority from local state                                          |

The root-catalog generation in a target overlay is a recovery pin, not an
observation. It is distinct from the tree snapshot sequence stored inside that
root. `TargetPreparing`, `CatchupPublished`, catalog refresh, and restart must
all open the exact root generation and then compare the recovered tree snapshot
sequence and applied sequence with the proof. Opening the latest root and
merely checking afterward is invalid: a concurrent checkpoint may move latest
forward, while stale root-cache state may leave it behind.

For both split and balance, exact-open initializes the page store from the named
immutable root and bypasses latest-root cache refresh until that store publishes
its own successor. Successor publication compares the exact bootstrap generation
and checksum with the current root first; if another writer has advanced the
lineage, publication fails instead of branching. The transition pin is persisted
before readiness publication and is removed only after authoritative catalog
state no longer contains the corresponding overlay and transition dependency.

The balance invariants are:

- **SERVER-ONE-TRANSITION:** one partition and each participating owner have at
  most one active topology transition;
- **SERVER-NO-DUAL-GRANT:** `TargetCatchingUp` is never included in a serving
  grant;
- **SERVER-PROOF-BEFORE-PUBLISH:** every catalog phase is justified by the
  corresponding durable readiness or release proof;
- **SERVER-LIVE-TARGET-CONTINUITY:** normal catch-up retains the prepared target
  handle and replays only the remaining suffix; exact-root reopen is recovery;
- **SERVER-ASYNC-INITIALIZATION:** readiness waits suspend Rust futures and
  never block executor threads or create a transfer-owned request queue; only
  `TargetCatchingUp` requests enter this bounded path;
- **SERVER-AMBIGUITY-FENCES:** an unknown publication outcome never reopens the
  source writer; and
- **SERVER-CLEANUP-AFTER-PINS:** source tree, stream, retry history, and shared
  packs remain retained until catalog references, recovery pins, retry floors,
  and forwarding grace have all cleared.

## 8. Placement Policy

Balancing targets at least `live_owner_count * target_partitions_per_owner`,
defaulting to four partitions per owner. Split chooses the largest eligible
partition using approximate retained pack estimates. A split boundary comes from
a resident index separator or a bounded small key window; exact byte or key
medians are unnecessary. Live-key witnesses establish nonempty children when
the observation is made. Concurrent mutations can change that observation.
The structural query never loads cold pages or flushes the tree. A service
allows only one background sampling job, and heartbeats use epoch-fenced cached
samples without waiting for that job. Every serving partition is considered;
an unsplittable largest partition does not exclude the remaining candidates.
Retained pack estimates use the opened manifest, including shared packs, rather
than cumulative write counters. The estimate is deliberately coarse and can
lag unsnapshotted mutations; observation does not scan remote metadata or data.
The retained parent and new split child stay local. Range splitting deliberately
uses an approximate boundary and does not guarantee equal child sizes. Split
sizing and minimum partition supply do not require exact placement equality.
A move uses assigned partition counts; byte estimates remain capacity filters.
An indivisible range may prevent further useful count balancing, in which case
placement remains unchanged.

### 8.1 Count-Based Placement

Placement currently uses assigned split count only: `w_i = p_i / P`, including
healthy owners with zero assignments. `byte_weight_percent` is reserved and must
be zero; nonzero settings are rejected until root/range statistics are available.
The data contribution is disabled. Physical shared-pack bytes, memtable bytes,
write counters and accumulated split ratios are not placement data weights.
Tolerance defaults to 20% relative to equal owner share; minimum global-loss
improvement defaults to 25%. Missing or epoch-mismatched assignment observations
still defer planning. Capacity/headroom remains a separate safety filter.

Tree statistics do not yet expose current root/range page, logical-byte or live-KV
aggregates. Existing retained-pack estimates remain coarse inputs to automatic
split sizing/candidate ranking and capacity checks; they can include shared packs
and lag ordinary flushes. Count balancing does not make those estimates exact.
The split execution, boundary sampling, recovery and publication contracts remain
unchanged. Console does not display Weight or weighted balance explanations.

### 8.2 Tolerance and Move Selection

Equal owner weight is `1 / N`. The observation's relative deviation is
`D = max_i(abs(N * w_i - 1))`; `imbalance_tolerance_percent` bounds `100 * D`.
Within tolerance, no load-balancing move is planned. Outside tolerance, evaluate
candidate moves against the same global loss
`L = sum_i((N * w_i - 1)^2)` before and after the move. The partition count
contribution moves; the byte contribution is zero. The normalized denominators
stay fixed for each candidate evaluation. Only the two affected owner terms
change, so each candidate's loss delta requires constant arithmetic.

A candidate must strictly reduce loss and meet
`minimum_weighted_improvement_percent` of the current loss. A count difference
does not authorize a move that worsens the global count score. Choose the greatest loss
reduction among safe eligible candidates; stable partition/target identities
break ties. Use validated bounded integer/fixed-point arithmetic, sufficient
intermediate width, and conservative threshold comparisons; UI rounding does
not decide eligibility. With unchanged observations, a reverse move increases
the same loss and cannot qualify. Observation changes remain subject to
thresholds and cooldown, rather than triggering actions for every small change.

For example, two owners with weights 20% and 80% can move a partition whose
contribution is 40 percentage points to obtain 60% and 40%. A relative tolerance
of 20% includes that result because equal owner weight is 50%. This is an
illustration of configured thresholds, not a promise that split yields two
40-point children. When no safe indivisible candidate offers enough improvement,
report no useful move and keep the placement.

Request rate, target headroom, fresh health, exact assignment and independent
recoverability are safety filters. A parent-tail overlay is ineligible. One
partition and each participating owner have at most one active transition.
Default per-partition cooldown is one minute; cooldown supplements scoring and
tolerance. Split pacing includes recent splits and transfers; repeat-placement
pacing includes only transfers, so split does not postpone first placement.
Transfer preparation, estimated catch-up and forwarding retain independent
ten-minute safety windows. A safe candidate outside tolerance that meets the
improvement threshold must make placement progress within forty seconds when
its cooldown has elapsed; splitting alone is not placement progress.

### 8.3 Decision Observation

The monitor's bounded decision observation identifies policy coefficients and
thresholds, catalog generation, assignment/owner identities, observation time,
source freshness, counts, estimated bytes, count/byte weight contributions,
owner weights, deviation and loss. It records the selected or best rejected
candidate's predicted weights and improvement, plus move/no-move reason:
within tolerance, insufficient benefit, unavailable observation, cooldown,
active transition, inherited overlay, unhealthy owner or capacity/rate limit.
Policy version identifies the scoring formula. Diagnostics retain at most 256
owners, 4096 partition contributions and 2 MiB, independently of the planner's
complete metadata input. Omitted details display unavailable; they do not alter
planning. Unchanged diagnostics are renewed within half their freshness window
rather than written on every tick. Do not emit or persist all candidate pairs
on every tick. This diagnostic
observation is not catalog authority and does not advance catalog generation.
The observation remains backend diagnostic metadata; Console currently hides
Weight and balance explanations. Catalog generation/identity mismatch remains
explicit; UI refresh does not bypass stale cursor rejection.

## 9. Lifecycle and Observability

Startup ensures the monitor, registers the instance, loads a complete catalog,
opens each assignment's latest tree root and stream manifest as `Prepared`,
reads the applied sequence and WAL replay offset from that tree root, replays
through the stream's current durable tail, reports readiness, and serves only
after installing a matching grant. Shutdown
atomically stops new admission and clears authority; already admitted R142
operations retain handles and finish before bounded checkpoint/drain work.
Catalog refresh recovers every new or changed local assignment first, then
replaces the catalog and hosted-partition snapshots; unchanged exact-epoch
handles remain live and departed assignments are dropped.
Catalog-head watch notifications trigger reconciliation immediately and check
the matching serving grant afterward. Instance-specific grant notifications
install authority without reopening or rescanning catalog assignments.
The configured periodic refresh remains
the fallback when notifications are unavailable; grant renewal also continues
on the heartbeat path. Publication therefore does not wait for a full polling
interval before a healthy owner observes a new routing generation.

Configuration exposes identity, group-0 seeds, separate RPC listen and routable
advertise addresses, dedicated 15xxx HTTP/RPC ports, hosted-partition capacity,
refresh/drain intervals, timing policy, and balance policy. Validation rejects
unsafe timing, zero bounds, bad addresses, unspecified advertise addresses, and
empty discovery seeds. Automatic child-owner balance is enabled by default and
may be disabled explicitly with the balance policy's `enabled` setting without
disabling local partition splitting. Lock-free counters distinguish successes, redirects,
local stale-route dispatch, lease and deadline rejections, overload, split
preparation/base/tail/fence/overlay/materialization work, and internal errors.
Balance observation records selection inputs, base and tail cursors, readiness
failure, catalog and lease phase, catch-up, and background work. Health and
heartbeat views derive from the same sorted partition snapshots and catalog
generation.

## Open Issues

- **Prepared transfer crash coverage:** preparation preserves source tree
  authority; mutable target authority requires the exact published owner
  assignment. Fault injection must cover source and target restart around
  that publication, including overlapping source checkpoint work.
- **Split handoff crash coverage:** the durable handoff status precedes local
  dispatch and preserves the original parent tree and WAL. Recovery composes
  the exact child base, range-filtered parent suffix and child WAL. Fault
  injection must cover status persistence with continuing parent writes,
  dispatch before final readiness and an unconfirmed status response. Status
  persistence remains outside the mutation worker.
- **Head publication before phase persistence:** transfer and split publication
  and their committed transition markers are separate writes. Recovery must
  prove exact catalog results and finish markers after a crash in between;
  fault injection must verify admission and eventual recovery at both transfer
  publications and the split publication.
- **Abort racing publication:** an abort decision must be serialized with
  publication under the monitor fence and transition revision. A previously
  loaded readiness proof must not publish after an authoritative abort. Verify
  this race before treating cleanup as safe.

- Real-process fault injection should continue expanding coverage of ambiguous
  catalog writes and process death at every balance phase.
- Overlapping remote transfer preparation, local split materialization and
  source restart need fault coverage that proves exact catalog and transition
  reconciliation without weakening single-writer or replay epoch checks.
- Completed transfers remove their catalog overlay, but physical reclamation of
  the departed source tree and stream, plus expiry of the stale-route
  forwarding grace period, still need a manifest-fenced background policy.
