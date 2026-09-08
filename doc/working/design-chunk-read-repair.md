<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk Read Repair (R111)

This extends the landed [object read flow](design-chunk-object-read.md) with
the generic task framework from the
[mirror-to-EC design](../design/chunkdb/design-crowdb-chunkdb-mirror-to-ec.md).
The source is [R111](../backlog/R111-chunkdb-read-io-error-handling.md).

## 1. Foreground recovery

The client reconstructs only the requested interval. A failed data-shard read
of `[offset, offset + length)` reads that interval from surviving shards and
decodes `length` bytes. A shared semaphore reserves decode inputs plus output
before I/O; large requests split to fit. The client reads only enough
surviving shards to decode and asks for another shard only after an observed
failure. A successful fallback returns after the failure marker is durable,
without waiting for scanner task admission or full-block repair.

## 2. Partial results

`read_range_partial` returns ordered successful logical ranges and exact
failed ranges. Strict `read_range` returns the first failure. The stream emits
successful windows followed by an explicit ranged error, never silent
truncation or zero filling.

## 3. Failure report

The client reuses the exact-range `replace_chunk_strip_range` transaction. It
submits the observed chunk revision and old strip, with a geometry-identical
replacement whose `unavailable_segments` contains the failed identities. This
persists the marker under the existing lifecycle guard without adding a
second, weaker metadata mutation. A rotating scan deterministically admits a
task from every durable marker, including after a crash in the mark-to-task
gap.

## 4. Persistent task

Task kind `RepairStrip` has a versioned payload containing chunk ID, strip
sequence, and sorted failed segments. Its deterministic ID hashes those
fields, so duplicate clients and unknown RPC outcomes are safe.

## 5. Full-shard background repair

The handler re-queries metadata. It reads one full surviving mirror or enough
full EC shards under a server memory semaphore, allocates a replacement while
excluding failed/surviving disks, writes and fsyncs it, then performs exact
old-strip and revision-fenced publication. Changed or deleted metadata is
re-evaluated idempotently. Repair has higher task priority than mirror-to-EC
conversion because it restores redundancy rather than reclaiming space.
An ambiguous metadata publish can leave a committed but unreferenced block;
this is a capacity-cleanup concern, never a visible-data rollback.

## 6. Crash invariants

- I1: unavailable metadata is durable before a task can be lost.
- I2: returned bytes are direct reads or successful EC output.
- I3: replacement metadata is published only after fsync.
- I4: publication is fenced by revision and exact old strip.
- I5: crashes may leak tentative or committed-unreferenced capacity, never
  expose incomplete data.
- I6: client and server recovery scratch memory is reserved before I/O.

## Scope

- Protocol: repair task kind; failure reporting reuses the fenced replacement
  RPC.
- ChunkDB: unavailable mutation, admission scan, repair handler, wiring.
- Chunk client: observations, reporting, partial results, stream errors.
- Tests: failure matrices and real-service repair/restart E2E.

## Complexity

High: request-visible fallback coordinates with durable metadata and a leased
background state machine across crash and concurrent-layout boundaries.

## Test Design

- Fail one requested EC shard -> exact partial decode; recovery reads do not
  exceed request length; peak memory stays within policy.
- Make one location unrecoverable -> other ranges remain ordered and the
  failure interval is exact; streaming ends with that ranged error.
- Report a real read failure -> metadata and deterministic task persist.
- Run repair -> replacement is fsynced and fenced; follow-up read is direct.
- Crash after metadata mark -> the restarted scanner admits the durable task.
- Crash after a repair write -> fenced publication either installs the fsynced
  segment or leaves an unreferenced block for lifecycle cleanup; readers never
  observe a partially written replacement.

## Module Structure

```text
lib/crowdb-chunk-client/src/chunk/{chunk_reader,strip_reader}.rs
app/crowdb-chunkdb/src/repair.rs
app/crowdb-chunkdb/src/task/
app/crowdb-chunkdb/src/lifecycle/handler.rs
```

## Config Extensions

The client uses `ChunkReadPolicy::recovery_memory_bytes` to bound concurrent EC
decode scratch (inputs plus decode output). Returned object/range bytes are
owned by the caller; callers requiring bounded logical-result buffering use
`read_stream`. ChunkDB uses per-kind concurrency and a separate full-shard
repair-memory budget. A too-small repair budget leaves the durable task in
retry-wait with a five-second retry delay.
