<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R201: crowdb-tree — MemTable write handoff before flush

Status: Deferred by user pending review of the handoff protocol and its
performance. Implementation resumes after choosing writer registration,
participant bounds, and flush completion semantics.

#### Problem

An apply call selects active MemTable A once and inserts a batch record by
record. Concurrent flush can replace active with B, drain A and remove it
from the reader-visible set before that apply finishes. The retained pointer
keeps A alive, but later records enter an unpublished table. Acknowledged KV
bindings can consequently disappear, causing ordinary S3 uploads to fail
with missing ChunkDB instance bindings. CI run 36994977434 exposed this in
`test_slow_signed_upload_releases_native_buffers`; this is a production
correctness race, not behavior exclusive to that test.

The [tree design](../design/tree/design-crowdb-tree-engine.md) describes active
and frozen L0 tables. The implementation must distinguish closing a table to
new batches from completion of already admitted writes. Blocking rotation
until a shared writer count reaches zero risks starvation under overlapping
batches; a shared counter also introduces cache-line contention.

#### Solution

Use the lifecycle Active -> Freezing -> Frozen -> Flushed. Keep the final
synchronization mechanism open for user review.

- **I1 — Admission.** Rotation closes A to new write batches and publishes B;
  a writer racing rotation either safely owns A or retries against B.
- **I2 — Completion.** Previously admitted batches finish writing A. A cannot
  become Frozen or be drained until all these writers have exited.
- **I3 — Visibility.** A remains queryable throughout Freezing and flush;
  removal follows publication of eligible records to tree and safe handling
  of records beyond the contiguous slot frontier. Existing version selection
  and query semantics remain intact.
- **I4 — Progress.** New batches writing B cannot delay A's completion.
  Rotation does not wait while holding the pointer-selection lock.
- **I5 — Bounded registration.** Every writer, including maintenance and
  snapshot import, uses one ownership protocol. Thread exit, nesting, exception
  exit, participant reuse and table generation reuse cannot bypass protection.

Work items:

1. Audit `crowdb_tree_engine`, tree FFI and C++ tree ingestion, flush leftover
   relocation and snapshot import. Define supported concurrent writers and
   the behavior of snapshot reset against ongoing writes.
2. Evaluate cache-line-separated writer-owned announcement slots, inspired by
   the existing `EpochManager` participant registration. Announce the target
   table generation and validate admission again before writing. Prove the
   publication, validation, rotation and scan memory ordering; observing zero
   without closing admission is insufficient. Keep reader reclamation separate
   so long-lived readers do not unnecessarily block write completion.
3. Compare this with Pebble's shared write references and RocksDB's write-group
   completion plus queue barrier, described below. Preserve concurrent MemTable
   insertion; a single consumer for all data writes is not the selected design.
   Select registration bounds and slot reuse policy instead
   of assuming Tokio workers are the only writers: maintenance currently uses
   `spawn_blocking`, and public library callers can supply other threads.
4. Separate reader-visible Freezing tables from drain eligibility in the tree.
   Preserve pending tables across maintenance passes and define explicit
   flush, snapshot and durability completion behavior.
5. Retain focused concurrent-batch regressions and restore any temporarily
   skipped S3 coverage once the chosen protocol is verified. Measure small
   and large batches at controlled concurrency before claiming performance gains.

RocksDB reference protocol (an alternative for evaluation, not an adopted
CROWDB implementation):

- **Concurrent writes inside a group.** `LaunchParallelMemTableWriters`
  initializes the group's atomic `running` count. Writers insert concurrently;
  `CompleteParallelMemTableWriter` decrements that count. The last writer
  performs group completion duties. This is a write-group count, not one
  persistent counter attached to each MemTable.
- **Pipelined write rotation uses a queue barrier.** The switch operation is
  coordinated with the upstream writer queue. `WaitForMemTableWriters` creates
  an empty `Writer` node and links it into the MemTable writer queue. If earlier
  writers exist, it waits until the node becomes `STATE_MEMTABLE_WRITER_LEADER`.
  Earlier groups can hand over leadership only after their inserts finish.
  Once the barrier reaches the front, the helper clears the MemTable queue.
  Together with upstream write admission ordering, this creates a boundary
  after all old-table writes and before later writes. It is not merely a
  snapshot observation that the queue happens to be empty.
- **Ordinary writes use group ordering.** Without pipelined or unordered writes,
  the prior write group completes before the next group starts. This still
  permits concurrent insertion within the active group.
- **Unordered writes use a pending count.** `WaitForPendingWrites` waits for
  `pending_memtable_writes_` to reach zero for already admitted writes. The
  count works within the surrounding admission protocol; copying only the
  zero check would not stop a new writer from entering CROWDB's old table.
- **Waiting is not completely lock-free.** Writer-state waiting starts with
  spinning, may yield, and falls back to a mutex/condition variable for longer
  waits. In pipelined mode, `WaitForPendingWrites` releases the DB mutex while
  waiting on MemTable writers and reacquires it afterward. The unordered
  count wait uses a mutex/condition variable. Evaluate these costs explicitly
  against the requested lock-free CROWDB handoff.
- **Adaptation boundary.** CROWDB currently allows independent apply calls and
  background flush; it does not already have RocksDB's write-group admission
  protocol. Any queue-barrier candidate must define who owns the admission
  boundary and include maintenance relocation and snapshot import. A marker
  inserted only into a flush queue cannot fence direct MemTable writers.

For illustration, the pipelined MemTable queue is:

```text
concurrent write group 1 -> concurrent write group 2 -> switch barrier
```

The barrier becomes eligible only after both preceding groups finish. Later
admission is coordinated upstream; this diagram does not imply a dedicated
thread serially inserts every record.

#### Dependencies

- Existing MemTable, tree epoch registration and KV maintenance are the
  baseline; no new dedicated writer pool is assumed.
- Pebble provides a reference for concurrent apply and write references:
  [mem_table.go](https://github.com/cockroachdb/pebble/blob/master/mem_table.go)
  and [rotation](https://github.com/cockroachdb/pebble/blob/master/db.go).
- RocksDB [PR 5716](https://github.com/facebook/rocksdb/pull/5716) discusses
  waiting for ongoing memtable writes before making a table immutable in
  pipelined write mode. Its synchronization is coordinated through
  `WaitForPendingWrites`; review this existing alternative alongside Pebble's
  references and writer-owned announcement slots rather than assuming epoch
  protection alone prevents late insertions.
  The [current implementation](https://github.com/facebook/rocksdb/blob/main/db/db_impl/db_impl.h)
  waits on memtable writers for pipelined writes and on pending write counts
  for unordered writes; normal write groups already provide a completion boundary.
  Concrete queue and group-counter functions are in
  [write_thread.cc](https://github.com/facebook/rocksdb/blob/main/db/write_thread.cc);
  flush scheduling and switch call sites are in
  [db_impl_write.cc](https://github.com/facebook/rocksdb/blob/main/db/db_impl/db_impl_write.cc).
  PR 5716 is a discussion reference, not proof that that particular PR merged;
  the current implementation is the behavior reference.
- Until implementation lands, any test skip is a coverage exception only;
  it does not establish correctness or repair missing acknowledged records.

#### Acceptance

- Given a paused batch writing A, rotate to B and attempt another batch;
  assert the new batch enters B, A remains Freezing and cannot drain until
  the first batch exits (I1, I2, I4). Integration test.
- Given a writer paused around announcement and validation, interleave
  rotation and a participant scan; assert it either safely completes on A or
  retries B without writing an already drained table (I1, I2). Unit test.
- Given a queue-barrier candidate with one paused group and concurrent writers
  inside that group, request a switch and submit a later batch; assert the
  barrier cannot pass unfinished inserts, later admission cannot write the
  old table, and group members remain concurrent (I1, I2, I4). Integration test.
- Given completed and incomplete slots in A and concurrent queries, finish
  its flush; assert every acknowledged key remains visible and no record
  beyond the contiguous frontier is lost (I2, I3). Integration test.
- Given ongoing writes to B while an old batch exits A, run maintenance;
  assert A becomes eligible independent of B's activity (I4). Integration test.
- Given thread churn, nesting, exception exits and registration exhaustion,
  exercise all write entry points; assert safe bounded registration and the
  chosen explicit exhaustion outcome, with no stale or overwritten ownership
  announcements (I5). Unit test.
- Given snapshot import and flush leftover relocation, race supported write
  operations; assert the selected exclusion or handoff contract preserves all
  eligible records (I2, I3, I5). Integration test.
- Given the real S3 stack and original slow signed upload workload, repeat
  with concurrent maintenance; assert successful upload, byte integrity and
  cleanup without missing bindings, with the test enabled (I1–I5). E2E test.
- Given identical hardware and configuration, compare the baseline and chosen
  protocol for small/large batches at one and multiple writer threads;
  assert recorded throughput, latency, memory and participant counts meet the
  user-approved regression budget (I4, I5). Integration test.

#### Open Questions

- Fixed pre-registered slots or lazily registered reusable slots? Fixed slots
  bound scans but require explicit worker limits; lazy slots accommodate library
  callers but require a bounded reclamation/reuse policy.
- Writer-owned announcements, shared write references or a write-group barrier?
  A barrier requires an admission/ordering protocol absent from the current
  independent apply interface and may block later groups during rotation.
  Announcements avoid
  shared increments but add scans and admission validation; references simplify
  completion detection but may contend at high batch rates.
- Should explicit flush wait for admitted writes or return with Freezing tables
  pending? Snapshot and durability callers need an explicit completion contract.
- What concurrency bound and performance regression budget are acceptable?

Run `pixi run test-cpp`, `pixi run cargo test -p crowdb-tree-ffi --tests`,
`pixi run cargo test -p crowdb-kv`,
`pixi run -e s3-e2e test-boto3-e2e`, `pixi run rs-fmt-check`,
`pixi run cargo clippy -p crowdb-tree-ffi -p crowdb-kv --all-targets -- -D warnings`,
and `pixi run tree-lint` for the implemented scope.
