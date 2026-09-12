<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R148: chunk-stream/chunk-kv — Partition metadata scale-out and sealed-chunk EC

**Status: Deferred.** Implement after the R141/R142/R143 single-group,
three-way-mirror production path is complete and measured. All features in
this requirement are disabled by default.

## Problem

R141 deliberately binds one stream's metadata to one nonzero KV group and
writes each active stream chunk as three mirrors. R142 stores the same
partition's R140 tree root catalog in that metadata group: tree page packs and
mapping data remain in ChunkDB/DiskIO chunks, while the Paxos KV records hold
the owner authority, current root manifest, immutable manifest generations,
and reference-segment indexes that locate those chunks. Group 0 contains only
the authoritative partition/range ownership and metadata-group binding.

That baseline keeps one partition's checkpoint metadata together and keeps
high-frequency root publication out of group 0, but it does not move a hot or
oversized partition metadata namespace between groups, shard its stream extent
index and tree reference indexes across groups, or reduce the long-term
storage cost of sealed chunks.

These changes require explicit cutover protocols. Moving keys without fencing
the authoritative binding can expose mixed metadata generations, while
converting an active chunk to erasure coding can race acknowledged-cursor
advancement and make the writer's durability contract ambiguous.

## Solution

1. Treat each partition's metadata placement as one binding-generation unit.
   The selected nonzero KV group owns both the R141 stream namespace and the
   R142 tree root namespace. The tree namespace contains
   `authority(tree_id) = (owner_epoch, current_generation)`, the current
   manifest, immutable generation manifests, and reference segments. A tree
   root publish CASes the authority record and atomically writes the new
   authority value, current manifest, and immutable generation in one Paxos KV
   batch. Old epochs and same-generation concurrent publishers fail closed.
2. Add a binding-generation migration state machine for moving the complete
   partition metadata unit from one KV group to another. Bulk-copy immutable
   stream pages, tree manifest generations, and reference segments while the
   source serves. Then fence and drain partition mutation admission, publish a
   final checkpoint, copy the remaining current stream/root records, and
   verify the destination contains the exact tree generation, applied
   mutation sequence, acknowledged stream cursor, and replay-retention floor.
   Atomically publish the destination group, higher binding generation, and
   higher ownership epoch in group 0 before the destination can serve. Retain
   the source read-only for a grace interval and delete it through bounded,
   idempotent cleanup. Append and root publication never dual-write groups.
3. Make reopen resolve group 0 first and accept stream and tree metadata only
   from the group named by the same binding generation. A prepared destination
   may verify copied metadata and prewarm chunk routes but cannot append,
   checkpoint, reclaim, or serve until the exact group-0 proof arrives. A
   timeout around group-0 publication keeps the source fenced and both sides
   non-writable until an authoritative read proves commit or abort.
4. Add optional per-partition metadata sharding only after whole-unit migration
   works. Keep one small generation-fenced directory in the partition's
   primary metadata group. Route stream extent pages by logical offset and
   tree reference segments by stable object ID; keep the tree authority and
   current root manifest together in the primary group so one CAS remains the
   publication point. Random stream seek and tree-root open remain logarithmic,
   and trim/reclaim can advance nonzero shard floors without scanning or
   rewriting earlier shards.
5. Keep new and active chunks three-way mirrored. After a chunk is sealed and
   no writer can advance its cursor, optionally submit it to chunkdb's existing
   mirror-to-EC conversion workflow. Verify the EC layout before atomically
   switching the chunk layout; retry or crash leaves the mirror layout
   readable. Chunk-stream metadata continues to name the chunk, not its strips
   or current layout.
6. Gate migration, sharding, and EC conversion behind separate configuration
   switches that default to disabled. Old bindings and mirror-only chunks keep
   their current meaning, and disabling a switch stops new background work
   without invalidating already published layouts.
7. Bound copy, verification, conversion, and cleanup work across both metadata
   namespaces. Expose binding age, primary and shard group IDs, stream/tree
   metadata bytes, migration phase/lag, copied and verified bytes, source
   retention, sealed mirror backlog, EC conversion latency/failure, retained
   mirror bytes, and cleanup retries.

## Dependencies

- Depends on R141 for the production stream metadata and mirrored chunk
  baseline, including stable logical offsets and sealed-chunk lifecycle.
- Depends on R142 for the chunk-backed tree, generation-CAS root catalog,
  checkpoint tuple, and epoch-fenced partition lifecycle.
- Depends on R143 for authoritative group-0 binding publication and operator
  configuration.
- Reuses chunkdb's durable layout-conversion state machine and R103's
  copy/verify/cutover principles, but metadata-group migration is a distinct
  namespace operation.
- The metadata publication fence selected in R141 must apply to binding and
  shard-directory generations before this requirement begins.

## Acceptance

- Given a populated stream, when its metadata binding migrates to another KV
  group with a checkpointed chunk-backed tree, and failures are injected before
  bulk copy, final fence, verification, group-0 publication, and source cleanup,
  assert every reopen selects one metadata group and recovers the exact same
  tree generation, applied sequence, stream cursor, retained retry results, and
  key/value contents. Invariant: migration never combines stream metadata and
  a tree root from different binding generations. E2E test.
- Given an old source owner and a destination owner, when group 0 publishes the
  higher epoch and destination group, assert the source cannot append, publish
  a tree root, or reclaim metadata, while the destination cannot do any of
  those operations before exact publication proof. Invariant: metadata
  placement migration never creates two writable authorities. Integration test.
- Given two same-epoch checkpoint publishers with the same expected tree
  generation, when both publish concurrently, assert exactly one authority CAS
  and its matching current and immutable manifests commit. Repeat after moving
  ownership to a higher epoch and assert the old epoch always fails. Invariant:
  a root generation and its authority fence are one atomic Paxos decision.
  Integration test.
- Given a sharded partition metadata namespace spanning several KV groups, when
  readers seek near the stream head, middle, and tail, open the current tree
  root, resolve reference segments, trim complete leading extent shards, and
  reclaim old tree generations, assert lookup remains logarithmic and no live
  metadata page is rewritten or lost. Invariant: sharding preserves logical
  addressing and one primary root publication point. Integration test.
- Given a sealed mirrored chunk, when EC conversion succeeds, fails, or crashes
  around layout publication, assert readers see either the verified mirror
  layout or the verified EC layout and the stream's logical extent is
  unchanged. Invariant: conversion cannot expose a partial layout. E2E test.
- Given default configuration, when streams append, roll, and reopen, assert no
  metadata migration, sharding, or EC background task starts. Invariant: R141's
  measured mirror-only behavior remains the default. Integration test.
- Given configured work limits and a large backlog, when maintenance runs,
  assert foreground append/tree-read latency remains within the selected
  regression threshold and stream copy, tree metadata copy, verification, EC,
  and cleanup queues each report bounded progress. Invariant: coordinated
  scale-out work cannot create unbounded foreground interference. Benchmark.

Required gates:

- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
- `pixi run -- cargo test -p crowdb-chunk-stream --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-kv --all-targets`
- `pixi run -- cargo test -p crowdb-chunkdb --all-targets`
- `pixi run clean-env && pixi run test-server`
