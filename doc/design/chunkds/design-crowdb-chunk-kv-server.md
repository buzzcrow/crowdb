<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk KV Server

`crowdb-chunk-kv-server` is the standalone ownership, routing, and network
boundary for chunk-backed metadata partitions. It hosts zero or many R142
partition handles but never owns or bypasses a raw tree or stream. Group 0 is
the durable catalog and monitor authority; monitor-issued serving grants are
the only authority to admit data requests.

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

## 4. Request Contract

Every request carries a random 128-bit client instance ID, nonzero monotonic
client sequence, catalog revision, partition ID, owner epoch, optional minimum
journal position, and optional deadline. The logical request identity survives
transport retry and rerouting. Endpoint, socket, and connection identities do
not participate in deduplication.

The contacted server validates deadline, catalog route, owner, grant, and local
partition before R142 admission. After a same-owner split, an old parent point
route may be remapped directly to the exact hosted child by key and transition
identity. This is process-local dispatch, not a network proxy, and it preserves
the original request identity and minimum position. All other stale routes
return `NotMyRange` with the current revision and owner hint without WAL I/O. Point
operations preserve get, put, delete, put-if-absent, compare-exchange, and
conditional-delete conditions and results. Successful and failed conditions
return the R142 journal position. `Overloaded`, `WriteStalled`, `Recovering`,
`TargetNotReady`, `LeaseExpired`, `RequestExpired`, `RequestConflict`,
`NotMyRange`, and `RefreshRequired` remain distinct wire outcomes.

A deadline observed before sequencer admission creates no WAL record. Once the
sequencer accepts a mutation, dropping the transport response does not cancel
the partition worker; retrying the same identity retrieves the recorded result.
Reads may carry a mutation's returned journal position for explicit
read-after-write ordering.

## 5. Ordered Reads

Seek provides ceiling, higher, floor, and lower operations within one partition
view. Scan intervals are validated and clipped to the routed half-open range
before execution. A forward scan includes its lower bound and excludes its
upper bound. Its continuation resumes strictly after the last emitted key so a
page boundary neither duplicates nor skips a key. Reverse scans use the
corresponding exclusive upper cursor. A continuation binds direction, last key,
partition ID, owner epoch, and catalog revision. Any split, transfer, revision,
epoch, or direction mismatch returns `RefreshRequired`; the server never
guesses a resume position. Multi-partition composition belongs to the routed
client.

## 6. Split Publication

The split transition phases are `Planned`, `ParentPreparing`,
`ChildPrepared`, `CatalogCommitted`, and `Aborted`. The server and group-0
monitor advance them in this order:

1. Group 0 persists the complete plan before local work begins. The existing
   parent keeps its identity, tree, stream, owner, and lower boundary; its next
   epoch and the one new child's identity, range, epoch, tree, and stream are
   fixed by the plan.
2. The parent process persists `ParentPreparing`, renews its serving grant as
   an active owner, checkpoints the exact parent base, builds the child's
   range-bounded base, performs the bounded writer handoff, and installs the
   local child handle.
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
   A stale parent point route dispatches to the retained parent or child by
   key; old parent scan and seek topology must refresh.

Before child materialization, heartbeat load reports the child as dependent.
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
crash. An abort before readiness removes unpublished child state and its pin;
after readiness, durable transition and catalog evidence decide cleanup.

## 7. Child Balance State Machine

A balance transition persists source and target owners, increasing target
epoch, source and target artifacts, readiness limits, old-grant deadline,
phase, release proof, initial readiness proof, final catch-up proof, and
failure. The live-source phases and actions are:

1. `Planned` → `SourcePreparing`: the source remains `Serving`, checkpoints a
   pinned base, creates the target-owned empty WAL, and records the initial
   source cursor in the target overlay.
2. `TargetPreparing`: the remote target validates the range, page-root and
   stream identities, opens the exact immutable tree-manifest generation named
   by the source-base proof rather than the latest root, and replays the source
   suffix into a `Prepared` overlay. A reopen during `CatchupPublished` must
   resolve the same pinned generation even if a newer root was published in
   the meantime. Failure here may abort; source authority is unchanged.
3. `TargetPrepared` or `AwaitingFence`: the monitor requests release only when
   record, byte, estimated catch-up, deadline, capacity, request-rate,
   cooldown, and one-transition-per-owner bounds pass. The source closes new
   admission, drains selected requests, and persists sequence and byte cursor
   `C`. If the source is unreachable, the transition waits until old grant
   expiry plus skew.
4. `TargetCatchingUp`: group 0 publishes the target owner and epoch with catalog
   state `TargetCatchingUp`. The source is no longer catalog authority and
   returns the target hint without appending. The target receives no serving
   grant and returns `TargetNotReady` with a bounded retry delay.
5. `CatchupPublished`: the target reopens the exact base and sealed source
   suffix through `C`, replays any target WAL, and persists the final catch-up
   proof. It cannot answer a read or evaluate a condition from a shorter
   prefix.
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

| Durable evidence                                      | Authoritative action                                                     |
|-------------------------------------------------------|--------------------------------------------------------------------------|
| Plan or source base only; no target readiness         | Keep source serving; retry preparation or abort unpublished target state |
| Initial target readiness; no source release           | Keep source serving; recheck budgets before requesting the fence         |
| Explicit release; catalog still names source          | Keep source fenced; publish `TargetCatchingUp` or resolve head ambiguity  |
| Lease exclusion; catalog still names source           | Recover target under the higher epoch; source cannot reactivate          |
| Catalog names `TargetCatchingUp`; no final proof       | Return `TargetNotReady`; replay the sealed suffix through `C`             |
| Final catch-up proof; catching-up catalog entry        | Publish the exact `Serving` successor and then issue the target grant     |
| Serving catalog entry; grant absent or expired         | Keep target prepared and reject admission until the exact grant arrives  |
| Ambiguous catalog head write                           | Reread head and pages; accept only the byte-exact intended generation     |
| Conflicting artifact, cursor, range, epoch, or proof   | Fail closed; never infer authority from local state                       |

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
- **SERVER-AMBIGUITY-FENCES:** an unknown publication outcome never reopens the
  source writer; and
- **SERVER-CLEANUP-AFTER-PINS:** source tree, stream, retry history, and shared
  packs remain retained until catalog references, recovery pins, retry floors,
  and forwarding grace have all cleared.

## 8. Placement Policy

Balancing targets at least `live_owner_count * target_partitions_per_owner`,
defaulting to four partitions per owner. Split chooses the largest eligible
partition and a key near the cumulative live-byte median, never an empty child.
The retained parent and new split child stay local. Placement later minimizes partition-count
difference first, then durable-byte spread. A move must repair count imbalance
or improve weighted spread by at least 25%. Request rate and target headroom are
safety filters. A child with a parent-tail overlay is ineligible. The default
per-partition cooldown is ten minutes and an owner participates in at most one
transfer at a time.

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

Configuration exposes identity, group-0 seeds, separate RPC listen and routable
advertise addresses, dedicated 15xxx HTTP/RPC ports, hosted-partition capacity,
refresh/drain intervals, timing policy, and balance policy. Validation rejects
unsafe timing, zero bounds, bad addresses, unspecified advertise addresses, and
empty discovery seeds. Lock-free counters distinguish successes, redirects,
local stale-route dispatch, lease and deadline rejections, overload, split
preparation/base/tail/fence/overlay/materialization work, and internal errors.
Balance observation records selection inputs, base and tail cursors, readiness
failure, catalog and lease phase, catch-up, and background work. Health and
heartbeat views derive from the same sorted partition snapshots and catalog
generation.

## Open Issues

- Real-process fault injection should continue expanding coverage of ambiguous
  catalog writes and process death at every balance phase.
