<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk-Backed Range KV

`crowdb-chunk-kv` is the embeddable range-partitioned state machine for the
shared-storage metadata index. It composes crowdb-tree's runtime-selected
chunk page store with a private chunk-stream WAL. It does not depend on the
Paxos-facing `crowdb-kv::KVEngine`, and partition count is independent of node
count.

## 1. Identity and Ownership

A partition has a stable 128-bit ID, an unsigned-bytewise half-open range
`[start, end)`, one ownership epoch, one tree, one distinct durable stream
name, and one partition-local mutation sequence. Unbounded endpoints are
allowed. A split key must be strictly inside the source range and its two
child ranges must be adjacent and exactly cover the parent.

The lifecycle is `Closed`, `Recovering`, `WriteStalled`, `Prepared`,
`Serving`, `SplitPreparing`, `SplitFinalizing`, `Retired`, or `Faulted`.
Data-path admission reads atomics and reserves bounded request and byte
capacity. Lifecycle control closes mutation admission and waits
asynchronously for the admitted count to reach zero. The manager registry and
split transition record use lifecycle-only locks; point reads and mutation
admission do not acquire them.

## 2. Tree and Journal Ownership

Every partition owns a `PartitionTree` and `PartitionJournal`. Production
adapters wrap crowdb-tree and R141 `ChunkStream`; injected implementations
support deterministic tests. The raw stream handle is private, so a server
cannot bypass partition ordering, trim, or epoch checks.

The tree is range-bounded at construction. Rust never implements the tree page
path through `crowdb-chunk-client`; the production tree selects R140's native
page-store backend at runtime. A balance target opens the source's pinned tree
manifest directly from shared page chunks and owns a distinct target WAL. It
does not copy keys through Rust or copy page packs before handoff.

Point reads, ceiling/higher/floor/lower, and bounded forward/reverse scans read
only the applied tree prefix. A forward scan uses an inclusive lower bound and
exclusive upper bound; its distinct continuation cursor resumes strictly after
the last emitted key. Forward operations use the native merged lower bound.
Reverse operations use a native predecessor descent that fixes the L0
memtable set, root page, and GC floor for the page, merges the highest revision
for each L0/L1 collision, and skips tombstones before returning descending
keys. Both directions enforce count and byte budgets inside C++; Rust does not
compose point reads, materialize the range, or sort results.

## 3. Mutation Ordering

`RequestId` contains a 128-bit client instance ID and a 64-bit client sequence.
A canonical SHA-256 digest covers operation kind, key, value, and condition,
but excludes routing and ownership fields. Reusing an ID with another digest
returns `RequestConflict` before I/O.

One bounded MPSC sequencer assigns monotonically increasing mutation
sequences. It evaluates put, delete, put-if-absent, compare-exchange, and
conditional delete in queue order against the applied tree plus successful
earlier entries staged in the same batch. Both applied and condition-failed
outcomes become checksummed WAL records. Therefore a lost response can be
retried without reevaluating its condition.

The sequencer appends frames to the chunk stream in mutation order. It changes
the tree only after the append is durable, applying one successful record per
tree call and advancing a failed condition as a no-op. A response is released
only after the applied frontier advances and carries the frame's stream name
and starting logical offset. Ordinary concurrent reads see the prior applied
prefix; a minimum journal position waits asynchronously for explicit
read-after-write ordering.

## 4. WAL and Recovery

Each physical frame has magic, version, flags, a bounded body length, a
serialized logical record, CRC32C, and a canonical 128-bit physical chunk ID
trailer. The CRC covers the header and body, not the trailer. The journal
submits header+body+CRC through chunk-bound stream append; the stream selects
the chunk and adds its identity. Recovery validates frame completeness,
checksum, source-chunk identity, partition identity, epoch, operation digest,
result revision, sequence continuity, and request-ID uniqueness. The durable
acknowledged cursor is the recovery upper bound, so identity validation never
promotes residual bytes. Recovery never reevaluates a conditional result. A
timestamp is omitted; any future timestamp is diagnostic-only.

A checkpoint contains the explicit 64-bit R140 tree identity, tree manifest
identifier, applied mutation sequence, stream name, and replay offset. The
tree identity is allocated and persisted; it is never derived by truncating
the partition's 128-bit identity. Recovery rejects a checkpoint or prepared
child whose tree identity differs from the injected tree. The replay offset is
the oldest WAL record needed by retained retry results, rather than simply the
current tail.
Recovery reads those pre-checkpoint frames to rebuild the retry cache without
reapplying them, then applies only the suffix above the checkpoint sequence.
WAL trim cannot cross the checkpoint's replay offset. Requests below the
declared retained floor return `RequestExpired` and are not executed anew.

The frontiers satisfy
`checkpoint_seq <= applied_seq <= journal_durable_seq`. A journal uncertainty
enters `WriteStalled` and preserves reads from the healthy applied prefix. A
non-OK tree result after durable append is `ApplyStateUnknown`, moves only that
partition to `Recovering`, and prevents later records from applying. The C ABI
catches C++ exceptions before they can cross into Rust.

## 5. Overlay Split and Writer Handoff

A `SplitPlan` names one transition, the exact parent ID/range/epoch, an
interior split key, the parent's next epoch, and one new child identity. The
existing parent retains its partition, tree, stream, and owner identities and
shrinks to `[old_start, split_key)`; the new child receives
`[split_key, old_end)`. Repeating the same active plan is idempotent; changing
any identity, range, epoch, or stream fails closed. The new child initially
remains on the parent owner. Placement is a later balance operation.

Split preparation follows these ordered steps:

1. Enter `SplitPreparing`, capture one split-owned shared memtable view, and
   range-rebuild both physical writers from one exact old-parent tree view.
2. While the old parent continues serving, replay its WAL into both ranges.
   Each writer filters the other range as no-ops, so both retain the same
   logical frontier.
3. Atomically switch the old parent handle to bounded split-session ingress.
   Requests already admitted to the old WAL drain; later requests wait in the
   ingress buffer rather than being rejected or appended to the old parent.
4. Replay the last old-parent suffix, bulk-publish the shared memtable view to
   both range trees, and durable-checkpoint both writers at the common
   frontier `C`. Release the shared view only after both durable frontiers are
   recorded in one immutable `SplitArtifact`.
5. Activate both local writers behind the old parent handle, then release the
   buffered requests directly to their range writer WAL and memtable. Reads
   use the old tree until this route is installed.
6. Publish one catalog generation that shrinks the retained parent (same ID,
   new epoch) and inserts exactly one new child. The parent owner already has
   both writers active, so refresh only publishes this catalog and serving
   grant to external routing; it does not gate local handoff, recover the
   retained parent, or replay a cutover.

The child base checkpoint records two independent counters: its logical tree
snapshot sequence and its chunk root-catalog generation. Immediately after the
child base is durable, preparation persists
`root/<child_tree_id>/pin/<transition_id> = child_root_generation`. Readiness is
not recorded until that operation succeeds. A retry with the same generation is
idempotent; another generation under the same transition identity is corruption.
If local child construction fails before readiness, preparation removes the pin
while the parent is still the sole authority.

A prepared or restarted child recovers in three layers: open the exact base
manifest, replay the retained parent suffix through `C` with range filtering,
then replay its own WAL from `C + 1`. Records at or below the base sequence
restore retained request outcomes without reapplying tree mutations. Failed
conditions remain no-ops with their original result. Sequence gaps, a wrong
source stream or epoch, a mismatched range, or a child WAL beginning anywhere
other than `C + 1` reject recovery.

The child recognizes two minimum-position namespaces. A retained parent-stream
position at or below `C` is satisfied by the inherited overlay frontier. A
child-stream position waits for the child's applied frontier. New mutations
return only child-stream positions. This preserves read-after-write across a
catalog split without treating two streams as one offset space.

After activation, bounded background passes independently prune the retained
parent tree to its smaller range and materialize the child's shared page
ownership. A checkpoint of the complete child view into its own tree root and
WAL permits the catalog to clear the parent-tail overlay and mark the child
independently recoverable. Parent manifests, stream bytes, retry results, and
page packs remain pinned while the child catalog artifact, a retained
checkpoint, forwarding grace interval, or retry floor references them.
The catalog generation that clears the child overlay also clears the retained
parent and child transition markers atomically. Only after the child owner has
installed that authoritative generation may it idempotently release the
transition's child-root pin. A crash between catalog installation and unpin is
a retention leak resolved by reconciliation, never permission to reclaim early.

The split invariants are:

- **SPLIT-PARENT-STABLE:** split preserves the parent partition, tree, stream,
  owner, and lower range boundary and creates exactly one new child;
- **SPLIT-ONE-WRITER:** before `C` the old parent sequences the full range;
  after `C` the retained parent and child sequence disjoint ranges;
- **SPLIT-COMMON-CUTOVER:** the retained parent and new child share the same
  exact `C`;
- **SPLIT-OVERLAY-DURABLE:** base plus parent suffix plus child WAL reconstructs
  values and request outcomes before activation;
- **SPLIT-NO-FOREGROUND-REJECT:** finalization buffers ingress rather than
  rejecting mutations, and it performs no ownership materialization; and
- **SPLIT-PIN-BEFORE-RECLAIM:** physical deletion never precedes the last
  catalog, snapshot, forwarding, or retry reference.

## 6. Remote Balance Storage Contract

Balancing one independently recoverable child reuses the same base-plus-tail
representation. A live source checkpoint supplies the pinned base manifest,
base sequence, stream manifest generation, and replay offset. The target owns
a distinct WAL, opens the shared tree root as `Prepared`, and replays the
source stream only through the persisted preparation cursor while the source
continues serving.

After target readiness, the source checks record, byte, estimated catch-up,
and preparation-deadline budgets before closing admission. Its release proof
records the final durable sequence and byte offset `C`. The target artifact is
then extended to `C`; final recovery reopens the base, source suffix, and
target WAL exactly as split recovery does. A target cannot serve reads or
conditional mutations until its caught-up proof covers the release proof.
After catalog and grant activation, only the target WAL can advance. A
dead-source recovery may adopt the original stream directly, but only after
the old lease expiry plus clock skew proves exclusion.

The balance invariants are:

- **BALANCE-PREPARE-WHILE-SERVING:** live target preparation never fences the
  source writer;
- **BALANCE-RELEASE-BEFORE-ACTIVATE:** target mutation authority requires a
  durable source release or lease-exclusion proof;
- **BALANCE-COMPLETE-PREFIX:** a target serves only after replay through `C`;
  and
- **BALANCE-INDEPENDENT-SOURCE:** a child retaining a split-parent suffix is
  ineligible for another owner handoff.

## 7. Failure Containment and Observability

Range, epoch, size, condition, request-conflict, expiry, and admission errors
do not damage partition health. Availability failures on a tree read remain
localized to that read. Corruption, conflicting recovery records, and unknown
post-journal apply state require recovery of the affected partition only.
Checkpoint and GC failures retain the prior manifest and WAL authority.

Per-partition lock-free counters cover mutation requests and outcomes, ordered
seeks and scans, range and stale-epoch rejection, admission backpressure, write
stalls, unknown apply outcomes, recoveries, checkpoints, and split lifecycle
events. Split counters distinguish preparation and base-checkpoint time,
tail records and bytes, catch-up lag and finalization duration, overlay replay records and
bytes, and materialization duration. Snapshot frontiers expose lifecycle,
stream identity, durable sequence and byte offset, and applied sequence without
combining independent partitions.
