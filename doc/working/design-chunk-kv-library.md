<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk KV Library (R142)

This implementation design refines
[`../backlog/R142-chunk-kv-library.md`](../backlog/R142-chunk-kv-library.md)
and composes the R140 bounded tree with the R141 logical chunk stream. It does
not reuse the Paxos-facing `crowdb-kv::KVEngine` boundary.

## 1. Partition Ownership

`PartitionId` is a stable 128-bit identity. Each partition artifact contains
its half-open binary key range, ownership epoch, tree identity and manifest,
one distinct durable stream name, logical mutation sequence, and checkpoint
frontier. One `PartitionManager` may host any configured number of independent
partition handles.

Lifecycle state is an immutable atomic snapshot: `Closed`, `Recovering`,
`WriteStalled`, `Prepared`, `Serving`, `SplitPreparing`, `SplitFenced`,
`Retired`, or `Faulted`. Admission validates state and epoch, reserves bounded
request/frame bytes, and enters an atomic in-flight count. Lifecycle fencing
closes admission before asynchronously waiting for that count to drain. The
sequencer is the only mutation-order owner; point and range reads do not enter
it.

## 2. Tree and Journal Boundaries

The production tree adapter owns one `crowdb_tree_ffi::Crowdbtree` opened with
an injected R140 page store and `KeyRange::Bounded`. Rust never routes tree
pages through `crowdb-chunk-client`. An injected `PartitionTree` contract makes
sequencing, recovery, and lifecycle tests deterministic without weakening the
production range check.

The partition owns its R141 handle privately. The public API exposes durable
`JournalPosition {stream_name, offset}` values but never exposes the stream
handle. A `PartitionJournal` adapter frames mutations, appends in sequence
order, reads complete frames from a logical offset, trims at checkpoint
watermarks, and reopens the same identity under a higher epoch.

## 3. Mutation Model and Framing

`RequestId` is `(client_instance_id[128], client_sequence[64])`. A canonical
digest covers the operation kind, key, value, and condition; it excludes
routing and ownership data. Supported mutations are put, delete,
put-if-absent, compare-exchange by revision or value, and conditional delete.

The sequencer drains an already-queued bounded batch. It evaluates operations
in order against the applied tree plus a private overlay containing preceding
successful mutations in that batch. Both success and condition failure obtain
a `mutation_seq` and a WAL record. The physical frame contains magic, version,
flags, bounded body length, serialized record body, CRC32C, and a canonical
128-bit physical `chunk_id` trailer. The CRC covers the header and body, not the
trailer, so body+CRC can be submitted unchanged when rollover selects another
chunk. The record body contains partition ID, ownership epoch, sequence,
request ID, digest, resolved result, and operation. R142 submits header+body+CRC
through R141's chunk-bound append; R141 selects the chunk and adds its ID.
Framing rejects incomplete or oversized records without interpreting trailing
bytes.

Frames are appended to R141 in mutation order. Only after their range is
durable does the worker apply each successful record to the tree with its
sequence; condition failures advance the tree sequence as no-ops. Completion
waits for the contiguous applied frontier and returns the frame's starting
journal position. A duplicate request and digest returns the retained original
result; a different digest returns `RequestConflict` without append.

## 4. Reads and Ordered Operations

Ordinary reads observe only the tree's applied prefix, so a journal-pending
mutation is invisible. A read with `min_journal_position` waits on a
register-before-load notification until that exact stream position has
applied. All keys and intervals are checked against `[start, end)` before tree
access.

Forward/reverse scan and ceiling/higher/floor/lower seek run through one tree
view. The C++ tree gains directional cursor primitives rather than emulating a
reverse answer by materializing and sorting in Rust. Result count and bytes are
bounded, and scan intervals are clipped to the owning partition.

## 5. Checkpoint and Recovery

A checkpoint atomically identifies the tree manifest, applied mutation
sequence, stream name, and replay offset. Its invariants are
`checkpoint_seq <= applied_seq <= journal_durable_seq`. Publishing a new tree
manifest precedes WAL trim, and GC respects live checkpoint, transfer, split,
and request-result retention pins.

Recovery opens the newest complete tree manifest or fallback, starts reading
the stream at the checkpoint offset, verifies complete frame CRC and compares
the trailer with R141 chunk provenance, and replays strictly increasing
sequences. The acknowledged cursor is the recovery upper bound; a matching
trailer cannot promote later residual bytes. An identical duplicate is
idempotent; a conflicting duplicate faults the partition. Recorded conditional
outcomes are applied without reevaluation. Service resumes only when applied
and journal-durable frontiers match.

## 6. Error Containment

Range, encoding, size, stale-epoch, request-conflict, expiry, and admission
errors occur before journaling and do not change health. R141 uncertainty moves
only the partition writer to `WriteStalled`; reads remain available from the
applied prefix. A definitely absent batch retains its sequencer position for a
same-order retry.

Tree availability errors degrade only affected cold reads. Corruption,
conflicting replay, or an indeterminate post-journal apply enters `Recovering`,
closes admission, and reopens only that partition. The FFI boundary must catch
all C++ exceptions and report `ApplyStateUnknown`; an uncertain tree handle is
never used again. Maintenance failures retain the prior checkpoint and WAL and
apply backpressure only at configured memory bounds.

## 7. Split Lifecycle

An idempotent `SplitPlan` names one transition, the exact parent range and
epoch, an interior split key, and exact child IDs/ranges/epochs. Preparation
pins checkpoint `b`, rebuilds both child trees from that one immutable R140
manifest, and lets parent traffic continue while bounded workers replay
`(b, current]`. Each mutation applies to one child; the other advances the
sequence by a no-op. Request results follow only the child owning the key.

The parent enters `SplitFenced` only below record, byte, and estimated-time
lag limits. It closes mutation admission, drains prior work, replays through
cutover `c`, validates both child checkpoints, creates distinct empty child
streams starting at `c + 1`, and returns one immutable artifact. Children may
open as `Prepared` but cannot serve.

`commit_split` requires exact catalog proof for both artifacts before retiring
the parent and serving the children. `abort_split` requires proof of absence.
An ambiguous catalog result leaves the parent fenced. Mapping materialization
and repack proceed asynchronously after commit under shared budgets.

## 8. Transfer, Bounds, and Metrics

Transfer closes mutation admission, drains the old epoch, checkpoints, and
reopens the same tree and stream objects under a higher epoch. It moves
authority, not bytes. Manager-wide semaphores bound split/checkpoint IO and
memory independently of the per-partition request/frame budgets.

Lock-free metrics cover request results, range and epoch rejects, queue and
memory admission, lifecycle states, logical/WAL/applied/checkpoint frontiers,
stalls, recovery, maintenance degradation, scans/seeks, split base/catch-up/
fence work, page reuse, pins, abandoned artifacts, and transfer. Remaining
implementation and integration work is tracked only in
[`plan-chunk-kv-library.md`](plan-chunk-kv-library.md); R142 has no unresolved
human design decision.
