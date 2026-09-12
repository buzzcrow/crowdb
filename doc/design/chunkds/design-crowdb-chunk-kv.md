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
`Serving`, `SplitPreparing`, `SplitFenced`, `Retired`, or `Faulted`.
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
page-store backend at runtime. A partition transfer reopens the same tree and
stream identities under a higher epoch instead of copying their bytes.

Point reads, ceiling/higher/floor/lower, and bounded forward/reverse scans read
only the applied tree prefix. Forward operations use the native merged lower
bound. Reverse operations use a native predecessor descent that fixes the L0
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

A checkpoint contains the tree manifest identifier, applied mutation
sequence, stream name, and replay offset. The replay offset is the oldest WAL
record needed by retained retry results, rather than simply the current tail.
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

## 5. Split and Transfer Control

A `SplitPlan` names a transition, exact parent ID/range/epoch and interior split
key, and exact child IDs/ranges/epochs. Repeating the same active plan is
idempotent; changing it or starting another lifecycle operation fails closed.
`SplitPreparing` continues to admit mutations. Once external catch-up proves
its configured lag bound, `fence_split` atomically stops new mutations and
drains all previously admitted work.

At the drained frontier, the child builder records one immutable
`SplitArtifact`. Both children must match the plan, use distinct streams, and
name manifests at the exact common cutover sequence. `commit_split` retires
the parent only when the catalog proof contains that exact artifact.
`abort_split` resumes the same parent epoch only with authoritative
non-publication proof. Missing or mismatched proof leaves the parent fenced.

Transfer uses the same fence and checkpoint boundary, then opens the existing
tree and stream under a higher epoch. The old handle remains fenced. R141's
writer epoch prevents its stream from advancing after authority moves.

## 6. Failure Containment and Observability

Range, epoch, size, condition, request-conflict, expiry, and admission errors
do not damage partition health. Availability failures on a tree read remain
localized to that read. Corruption, conflicting recovery records, and unknown
post-journal apply state require recovery of the affected partition only.
Checkpoint and GC failures retain the prior manifest and WAL authority.

Per-partition lock-free counters cover mutation requests and outcomes, ordered
seeks and scans, range and stale-epoch rejection, admission backpressure, write
stalls, unknown apply outcomes, recoveries, checkpoints, and split lifecycle
events. Snapshot frontiers expose lifecycle, stream identity, durable sequence,
and applied sequence without combining independent partitions.
