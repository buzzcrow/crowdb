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
3. Compare this with a shared write-reference count and synchronized admission,
   as used in Pebble. Select registration bounds and slot reuse policy instead
   of assuming Tokio workers are the only writers: maintenance currently uses
   `spawn_blocking`, and public library callers can supply other threads.
4. Separate reader-visible Freezing tables from drain eligibility in the tree.
   Preserve pending tables across maintenance passes and define explicit
   flush, snapshot and durability completion behavior.
5. Retain focused concurrent-batch regressions and restore any temporarily
   skipped S3 coverage once the chosen protocol is verified. Measure small
   and large batches at controlled concurrency before claiming performance gains.

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
- Until implementation lands, any test skip is a coverage exception only;
  it does not establish correctness or repair missing acknowledged records.

#### Acceptance

- Given a paused batch writing A, rotate to B and attempt another batch;
  assert the new batch enters B, A remains Freezing and cannot drain until
  the first batch exits (I1, I2, I4). Integration test.
- Given a writer paused around announcement and validation, interleave
  rotation and a participant scan; assert it either safely completes on A or
  retries B without writing an already drained table (I1, I2). Unit test.
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
- Writer-owned announcements or shared write references? Announcements avoid
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
