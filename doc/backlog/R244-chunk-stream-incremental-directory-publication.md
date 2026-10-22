<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R244: chunk-stream/chunk-kv — Incremental extent directories and WAL append latency

Status: Deferred by user decision. Schedule a dedicated Chunk-Stream and
Chunk-KV WAL performance iteration before implementation. Recording this item
does not waive existing failing performance assertions.

#### Problem

The [Chunk-Stream design](../design/chunkds/design-crowdb-chunk-stream.md)
separates mirrored payload durability, the fenced ChunkDB durable cursor, and
the logical-to-physical extent directory. The framed implementation currently
couples every append batch to publication of a new directory generation:

- `publish_state` rebuilds pages from all retained extents. The KV adapter writes
  every page under the new epoch/generation, then CAS-publishes the manifest.
  Historical page contents are copied to fresh keys; existing immutable keys
  are not overwritten. With fixed-size batches and pages, per-batch work grows
  with retained history and cumulative page writes can grow quadratically.
- A fixed-size append waits for mirror writes, durable cursor advancement and
  directory publication. Old page versions also increase metadata retention and
  reclamation work. Long-running streams and small-record journals incur more
  work even when an append changes only the active tail.
- `StreamPartitionJournal` implements Chunk-KV WAL over the same append path
  using `StreamPurpose::Wal`; the standalone performance fixture uses
  `StreamPurpose::Stream`. Both incur this work. Standalone Stream measurements
  do not establish the actual Chunk-KV WAL or client mutation latency.
- On 2026-10-22, a 4 KiB, one-producer Stream sample averaged 7.44 ms: mirror
  writes 1.02 ms, durable cursor resolution 2.51 ms and directory publication
  3.82 ms. Revision reuse and eight-way page publication improved the 32-producer
  NullDisk sample from 1017 to 1838 ops/s, but 1154 publications still wrote
  10933 pages. These changes reduce waiting, not historical write amplification.

The measurements and successful scoped cleanup receipts are recorded in the
[R243 handoff](../working/plan-unified-oci-cluster.md#stream-latency-attribution-and-first-optimizations).
Its temporary plan and local runtime artifacts are supporting evidence, not
prerequisites for implementing this requirement. Correctness tests already show
that verified frames inside a committed cursor can be recovered without another
directory publication; residual bytes beyond that cursor remain unauthorized.

#### Solution

1. **Separate append commitment from directory checkpoints.** Chunk-Stream keeps
   ordered mappings for the active tail and acknowledges each nonempty batch
   only after all required mirror writes and the fenced durable cursor update.
   Ordinary appends require no Stream-directory KV publication. Directory
   checkpoints publish at allocation, rollover, trim and close boundaries.
   Chunk growth must not force a rewrite of
   historical directory pages. Recovery resolves authoritative ChunkDB capacity
   rather than treating an older directory checkpoint as the current layout.
   Invariant I1: an acknowledgment always represents durable, recoverable data.

2. **Reuse immutable history through independent references.** Protocol page
   references identify their own persisted version independently of the current
   manifest's writer epoch and generation. Use a bounded-fanout immutable extent
   index rooted by the manifest; changed tail leaves and their ancestor paths
   are copied, while unchanged pages retain their identities and bytes. Neither
   publishing nor opening the index scans or serializes all historical extents.
   A manifest must not contain an ever-growing flat copy of every page fence.
   Invariant I2: metadata work follows changed pages and index height, not the
   total retained extent count.

3. **Preserve publication and ownership fences.** `StreamMetadataStore` and its
   KV adapter persist all newly referenced pages before CAS-publishing the head.
   A partial write leaves the previous head usable. Resolve ambiguous outcomes
   by observing durable authority; retain immutable-page conflict detection,
   purpose validation, binding selection and stale-writer rejection. Adopting a
   higher writer epoch must not copy unchanged historical pages merely to retag
   them. Invariant I3: each visible root is complete and one epoch owns writes.

4. **Recover a bounded active tail and keep finite read snapshots.** Writer open
   and read-only open combine a valid directory checkpoint with verified frames
   up to one captured authoritative cursor. A fresh reader includes previously
   acknowledged appends; an existing reader keeps its captured end. Corrupt,
   incomplete or wrong-chunk frames inside committed bounds fail validation;
   physical residual bytes never extend the logical tail. Bound uncheckpointed
   recovery to the active chunk; configured chunk capacity and frame geometry
   bound its bytes/frame count. Expose that work and use bounded physical reads.
   Reopen, growth, rollover and close preserve exact logical
   offsets. Invariant I4: addressing and snapshot bounds survive deferred
   directory publication without promoting uncommitted data.

5. **Reclaim by reachability and retention.** Trim and metadata cleanup account
   for pages shared by current and retained roots, active readers, and Chunk-KV
   checkpoint/replay/transfer retention. An older page generation may remain
   live in a newer root; a generation comparison alone cannot authorize deletion.
   Bound orphan and unreachable-page cleanup and make restart idempotent.
   Invariant I5: cleanup never removes a page or byte needed by a retained view.

6. **Verify the shared WAL consumer and report amplification.** Keep
   `StreamPartitionJournal` and `ChunkKvStorage` on the shared implementation.
   Exercise mutation acknowledgment, journal replay, checkpoint/trim and
   split/transfer cutovers against the incremental directory. Extend phase perf
   distributions with directory pages/bytes read, built, reused and written,
   head publications, recovery frames/bytes and cleanup backlog. Report both
   Stream append and real Chunk-KV WAL/client mutation latency. Invariant I6:
   optimization improves shared storage without changing journal semantics.

#### Dependencies

- Reuses the existing framed Stream append, durable cursor/checksum resolution,
  epoch fencing, immutable-page CAS and process phase histograms. Until this
  requirement is scheduled, the current correct implementation remains in use;
  revision reuse and bounded parallel page writes are mitigation only.
- R243 exposed this storage issue while migrating measurement fixtures. Its
  deployment work remains separate; unchanged failing Stream performance checks
  stay visible until their actual acceptance passes. Moving work here grants no
  exception to correctness or performance gates.
- Independent of R148 metadata-group migration, sharding and sealed-chunk EC.
  R148 must reuse the versioned references and retention rules when scheduled;
  it is not a prerequisite for fixing single-group append amplification.
- Framed synthetic NullDisk reads need a valid deterministic frame fixture.
  Use identical discard-write parameters for write comparisons and real retained
  bytes for recovery acceptance. Do not attribute synthetic frame rejection to
  a durability/performance trade-off or silently change the regression baseline.

#### Acceptance

- A01: Given valid frames, mirror failure and ambiguous cursor outcomes -> append
  and restart before/after each durability boundary -> assert success only after
  proven commitment, exact retained bytes, no duplicate logical range and no
  promotion beyond the durable cursor. Invariant I1. Integration test.
- A02: Given fixed-size append batches and retained histories of 1, 16 and 256
  full directory pages -> append identical suffixes within active capacity ->
  assert zero ordinary-append directory publications and no historical page
  reads, rebuilds or writes; checkpoint work touches only changed leaves and
  bounded ancestor paths. Invariant I2. Integration test.
- A03: Given a populated index -> grow, roll over and adopt a higher writer
  epoch -> assert unchanged historical page identities/contents, bounded changed
  paths, routable head/middle/tail offsets and a bounded-size root manifest.
  Invariants I2, I3, I4. Integration test.
- A04: Given competing publishers and injected failures between new-page writes
  and head CAS, including response loss and conflicting immutable keys -> resolve
  or restart -> assert one complete authoritative root, old-writer fencing,
  immutable purpose and no partial directory visibility. Invariant I3. Integration test.
- A05: Given acknowledged uncheckpointed frames and a reader with a captured
  end -> append more data and open fresh writer/read-only handles -> assert fresh
  recovery includes acknowledged bytes, the existing reader remains finite and
  read-only open performs no writer adoption or publication. Invariant I4. Integration test.
- A06: Given corrupt/truncated/wrong-chunk frames and valid residual frames
  beyond a committed cursor -> recover within configured tail limits -> assert
  committed corruption fails closed, residual data stays invisible and recovery
  frame/byte counters satisfy the documented bounds. Invariants I1, I4. Integration test.
- A07: Given current and retained roots sharing older pages plus replay/transfer
  retention -> trim, reclaim, release views and restart cleanup -> assert shared
  live pages survive, only unreachable pages are reclaimed and work stays within
  each pass limit. Invariant I5. Integration test.
- A08: Given both Wal and Stream purposes with matching workload parameters ->
  append, checkpoint, roll, close and cold reopen -> assert purpose, exact logical
  offsets and retained bytes are preserved, including growth beyond an older
  checkpoint's capacity. Invariants I1, I3, I4. Integration test.
- A09: Given actual Chunk-KV mutation traffic and retained retry results ->
  checkpoint, trim, split/transfer and crash/restart around journal commitment ->
  assert acknowledged mutations replay exactly, cutover bounds remain fixed,
  old owners are fenced and recovery preserves retry outcomes. Invariant I6. E2E test.
- A10: Given isolated matched release fixtures and growing retained histories ->
  run one- and 32-producer 4 KiB Stream append plus actual Chunk-KV WAL workloads,
  then inspect periodic and cumulative perf output -> assert bounded directory
  work, zero errors/watchdogs, retained cumulative distributions and unchanged
  applicable regression floors/ceilings. Record binary/configuration identities,
  phase count/avg/p50/p99, publications, pages/bytes, RSS and cleanup receipts;
  separate WAL and client latency from standalone Stream results. Invariants
  I2, I6. E2E test.

Required verification commands for implementation, not for creating this item:

```bash
pixi run clean-env && pixi run cargo test -p crowdb-protocol -p crowdb-chunk-stream -p crowdb-chunk-kv -p crowdb-chunk-kv-server --all-targets
pixi run cargo test -p crowdb-common --lib metrics::
pixi run clean-env && pixi run bash tools/benchmark/bench-chunk-stream-regression.sh
pixi run clean-env && pixi run bash tools/benchmark/bench-chunk-kv-regression.sh
pixi run rs-fmt-check
pixi run cargo clippy -p crowdb-protocol -p crowdb-common -p crowdb-chunk-stream -p crowdb-chunk-kv -p crowdb-chunk-kv-server --all-targets -- -D warnings
```
