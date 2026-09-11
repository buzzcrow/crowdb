<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R148: chunk-stream — Metadata scale-out and sealed-chunk EC

**Status: Deferred.** Implement after the R141/R143 single-group,
three-way-mirror production path is complete and measured. All features in
this requirement are disabled by default.

## Problem

R141 deliberately binds one stream's metadata to one nonzero KV group and
writes each active stream chunk as three mirrors. That baseline keeps the
append path simple and makes failure recovery auditable, but it does not move a
hot or oversized metadata namespace between groups, shard one stream's extent
index across groups, or reduce the long-term storage cost of sealed chunks.

These changes require explicit cutover protocols. Moving keys without fencing
the authoritative binding can expose mixed metadata generations, while
converting an active chunk to erasure coding can race acknowledged-cursor
advancement and make the writer's durability contract ambiguous.

## Solution

1. Add a binding-generation migration state machine for moving a complete
   stream metadata namespace from one KV group to another: freeze the source
   generation, copy immutable records, verify content and coverage, publish the
   destination binding in group 0, retain the source for a grace interval, and
   delete it through bounded idempotent cleanup. Readers refresh on a newer
   binding generation; append never dual-writes metadata groups.
2. Add optional per-stream metadata sharding only after migration works. Keep a
   small authoritative directory in the binding and route extent pages by
   logical range. Directory publication is generation-fenced, random seek
   remains logarithmic, and trim can advance a nonzero first page without
   scanning or rewriting earlier shards.
3. Keep new and active chunks three-way mirrored. After a chunk is sealed and
   no writer can advance its cursor, optionally submit it to chunkdb's existing
   mirror-to-EC conversion workflow. Verify the EC layout before atomically
   switching the chunk layout; retry or crash leaves the mirror layout
   readable. Chunk-stream metadata continues to name the chunk, not its strips
   or current layout.
4. Gate migration, sharding, and EC conversion behind separate configuration
   switches that default to disabled. Old bindings and mirror-only chunks keep
   their current meaning, and disabling a switch stops new background work
   without invalidating already published layouts.
5. Bound copy, verification, conversion, and cleanup work. Expose binding age,
   shard count/bytes, migration phase/lag, copied and verified bytes, sealed
   mirror backlog, EC conversion latency/failure, retained mirror bytes, and
   cleanup retries.

## Dependencies

- Depends on R141 for the production stream metadata and mirrored chunk
  baseline, including stable logical offsets and sealed-chunk lifecycle.
- Depends on R143 for authoritative group-0 binding publication and operator
  configuration.
- Reuses chunkdb's durable layout-conversion state machine and R103's
  copy/verify/cutover principles, but metadata-group migration is a distinct
  namespace operation.
- The metadata publication fence selected in R141 must apply to binding and
  shard-directory generations before this requirement begins.

## Acceptance

- Given a populated stream, when its metadata binding migrates to another KV
  group with failures injected before copy, verification, binding publication,
  and source cleanup, assert every reopen selects one complete authoritative
  generation and returns identical bytes. Invariant: migration never exposes a
  mixed extent map. E2E test.
- Given a sharded extent index spanning several metadata groups, when readers
  seek near the head, middle, and tail and trim removes complete leading
  shards, assert lookup is logarithmic and no live page is rewritten or lost.
  Invariant: sharding preserves logical addressing and bounded seek work.
  Integration test.
- Given a sealed mirrored chunk, when EC conversion succeeds, fails, or crashes
  around layout publication, assert readers see either the verified mirror
  layout or the verified EC layout and the stream's logical extent is
  unchanged. Invariant: conversion cannot expose a partial layout. E2E test.
- Given default configuration, when streams append, roll, and reopen, assert no
  metadata migration, sharding, or EC background task starts. Invariant: R141's
  measured mirror-only behavior remains the default. Integration test.
- Given configured work limits and a large backlog, when maintenance runs,
  assert foreground append/read latency remains within the selected regression
  threshold and every maintenance queue reports bounded progress. Invariant:
  scale-out work cannot create unbounded foreground interference. Benchmark.

Required gates:

- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
- `pixi run -- cargo test -p crowdb-chunk-stream --all-targets`
- `pixi run -- cargo test -p crowdb-chunkdb --all-targets`
- `pixi run clean-env && pixi run test-server`
