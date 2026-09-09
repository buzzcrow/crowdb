<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Small-Write Path Design and Plan

## 1. Problem Statement

1 KiB single-thread write latency was ~2200 µs p50, far too high for the
work done (one 1 KiB object, three mirror RPCs to NullDisk).

Investigation traced the cost to two root causes:

1. **Metadata advancement on the critical path** (~1379 µs per batch).
   `advance_chunk_write` (ChunkDB metadata + KV Paxos round) was
   executed synchronously before returning the write response.
2. **1 MiB payload per mirror RPC** (~874 µs per mirror write). The
   client allocated `BytesMut::zeroed(strip_bytes)` (1 MiB), copied the
   1 KiB object into it, and froze + sent the entire 1 MiB buffer to
   each of the 3 mirrors — 3 MiB of payload per 1 KiB object.

The user's intended architecture:

- Metadata advancement is asynchronous, not on the response critical
  path. (Already done in a prior phase.)
- The critical path is only "write to disk". Strip prefetch and EC
  conversion are background work.
- One 1 MiB shadow buffer per pipeline, filled sequentially. No second
  allocation, no copy. Send only the written range as a view/slice.
- DiskIO owns block alignment/padding. All disk types (NullDisk,
  MemDisk, BlockDisk) traverse the same padding code path; block_size=1
  makes padding a no-op, not a bypass.
- EC conversion becomes incremental: as the shadow buffer fills,
  compute EC outputs and release memory progressively.

## 2. Small-Write Flow (After Phase 2.5)

```
Client                          DiskIO Server
------                          ------------
SmallObjectWriter.on_data
  -> SmallWritePool queue
  -> OwnedChunk.try_write_batch
       1. Allocate 1 MiB shadow buffer (once, no zeroing)
       2. Copy object fragments into shadow[written_end..]
       3. view = shadow.freeze().slice(block_offset..written_end)
       4. write_mirrors_with_repair(view, block_offset)
            -> 3x concurrent write_at_byte_offset RPCs
               (each sends only logical_bytes, not 1 MiB)
                                               -> handle_write
                                                    -> AlignedWriter::submit
                                                         -> engine->submit_write
                                                         -> uring pwrite
                                                    <- callback
                                               <- response
       5. Reclaim shadow buffer (try_into_mut)
       6. Return locations after mirror IO completes
       7. Background chain: coalesce advance_chunk_write; on strip close,
          append a bounded batch of prefetched mirror strips at low water
  <- Location (offset, length)
```

Key points:

- **Single buffer**: `BytesMut::with_capacity(strip_bytes)` allocated
  once per pipeline, reused across batches. No `zeroed()`, no
  per-batch allocation.
- **View-based send**: `frozen.slice(block_offset..written_end)` creates
  a `Bytes` view into the same allocation. Three mirror RPCs share the
  view via `Bytes::clone()` (refcount only, no copy).
- **Buffer reclamation**: After all mirror RPCs complete, the frozen
  buffer is reclaimed via `try_into_mut()`. If the view is still
  referenced (shouldn't happen in normal flow), falls back to copying
  into a new `BytesMut`.
- **Repair path**: Sends the full shadow image (offset 0 to written_end)
  to the replacement segment at offset 0. This ensures the replacement
  has the complete acknowledged prefix, not just the current batch.
- **Metadata off critical path**: normal cursor progress is coalesced in
  `pending_advance`; a later write polls a completed task but never waits for
  the preceding cursor RPC.
- **Strip prefetch**: allocation attaches four strips initially. Closed-strip
  metadata and `append_chunk(strip_count=N)` run in the background at a
  low-water mark. Seal releases attached strips beyond the written length.

## 3. Phase 1 Changes (Complete)

### 3.1 `small_pipeline.rs` — `try_write_batch`

- Removed `BytesMut::with_capacity(logical_bytes)` (compact batch
  buffer) and the copy into `self.shadow`.
- Shadow buffer is now the single source of truth: fragments are
  written directly into it at the sequential position.
- `write_mirrors_with_repair` now takes both `data` (the view for
  mirror writes) and `full_image` (the complete shadow for repair).

### 3.2 `disk_writer.rs` — `DiskWriter` trait

- `write_at_byte_offset` changed from a default method to a required
  method. This fixes an `async_trait` dispatch bug: the default was
  always called instead of the `RoutedDiskWriter` override, causing
  all byte-offset writes to fail with "segment-relative offset and
  write length must be unit aligned".

### 3.3 `routing.rs` — `RoutedDiskWriter`

- Added `write_at_byte_offset` that computes
  `zone_offset = seg.unit_offset * unit_bytes + byte_offset` and sends
  via `DiskioClient::write_bytes`. The RPC protocol already carries
  `zone_offset` as a byte offset, so no protocol change is needed.

### 3.4 `client.rs` — `MetricsDiskWriter`

- Added `write_at_byte_offset` wrapper that delegates to the inner
  writer with metrics recording.

### 3.5 Test files

- Added `write_at_byte_offset` to all test `DiskWriter` implementations
  (8 implementations across 7 files). Test implementations delegate to
  `write_at` when aligned, otherwise return an error.
- `FailSelectedDiskWrite` and `FailWritesFromCall` in
  `small_object_writer_e2e.rs` now override `write_at_byte_offset`
  directly (not just `write`) so the fault injection fires on the
  byte-offset path too.
- `small_object_test.rs` repair test updated: replacement image is now
  12 KiB (the logical size), not 1 MiB.

### 3.6 Benchmark Results (1 KiB, 1 thread, NullDisk)

| Metric        | Before (1M payload) | After (1K payload) | Change  |
|---------------|---------------------|--------------------|---------|
| p50           | 2164 us             | 1131 us            | -48%    |
| p90           | 2820 us             | 1607 us            | -43%    |
| p95           | 3139 us             | 1881 us            | -40%    |
| p99           | 3516 us             | 2421 us            | -31%    |
| TPS           | 441.88              | 566.52             | +28%    |
| Payload/obj   | 3 MiB               | 3 KiB              | -99.9%  |

## 4. Phase 2: DiskIO Alignment and Padding (Complete)

### 4.1 Problem

Current `UringEngine::submit_write` rejects unaligned O_DIRECT writes
with `-EINVAL`:

```cpp
if (disk->is_o_direct() && disk->block_size() > 0) {
    if ((size % disk->block_size()) != 0 ||
        (phys_offset % disk->block_size()) != 0) {
        on_complete(-EINVAL);
        return;
    }
}
```

After Phase 1, the client sends only `logical_bytes` (e.g. 1 KiB),
which is not block-aligned for real O_DIRECT devices. The server must
pad and align before submitting to the kernel.

### 4.2 Design Requirements (from user)

- All disk types traverse the same padding code path.
- NullDisk/MemDisk (`block_size = 1`): padding is a no-op, not a
  bypass.
- For real O_DIRECT block devices:
  - Writes must satisfy both offset and length alignment.
  - Normal writes begin aligned; unaligned tails are managed by
    DiskIO.
- DiskIO owns padding; the client should not need to send a full
  aligned unit.
- Use a pre-prepared zero buffer for padding.
- Maintain cached partial-block state for unaligned writes.
- If the required cached block is missing: read the disk block,
  merge, write back (rare/error recovery path, emit warning).
- Warn when a write starts at an unaligned offset.
- Works with both `io_uring` and blocking engines.
- Preserve write ordering for sequential pipeline writes.
- Hot paths remain lock-free or use a justified synchronization
  strategy.

### 4.3 Resolved Decisions

1. **Cache ownership**: `DiskioServer` owns one `AlignedWriter`. Cache
   keys contain disk ID and physical block offset; weak disk references
   prevent stale state from surviving a disk reopen.
2. **Partial tails**: write immediately with zero padding and cache the
   completed physical block for the next sequential continuation.
3. **Concurrency**: 64 lock-free MPSC shards assign all writes for one disk
   to the same shard and serialize one active write per shard. No hot-path
   mutex was added.
4. **Invalidation**: failed padded writes and successful aligned full-block
   overwrites invalidate affected entries. Expired disk instances fall back
   to read-merge recovery.
5. **Recovery**: an unaligned cache miss warns, reads the physical block,
   merges the logical bytes, and writes the aligned block.

### 4.4 Implemented Approach

- `AlignedWriter` runs between the write RPC handler and every `IoEngine`.
- `block_size = 1` traverses the stage and delegates as a no-op without
  padding allocation or sequencing overhead.
- Larger block sizes use a process-lifetime zero source and an aligned,
  completion-owned request buffer.
- The cache is bounded to 64 blocks per shard; eviction is safe because the
  read-merge path reconstructs missing state.
- The RPC response reports the logical length only after the padded backend
  write completes in full.
- The same stage feeds both `UringEngine` and `BlockingEngine`.

## 4.5 Phase 2.5: Metadata and Strip Prefetch (Complete)

- Normal writes never await the preceding `advance_chunk_write`. A completed
  metadata future is applied by polling; an in-flight future coalesces later
  cursor progress.
- Chunk allocation attaches up to four mirror strips initially, bounded by the
  configured chunk capacity.
- Closed-strip advancement and `append_chunk(strip_count=N)` execute in one
  revision-ordered background chain. Refill starts at the prefetch low-water
  mark.
- Exhausting the attached runway is an error and retires the pipeline; the
  object path does not fall back to synchronous allocation.
- ChunkDB seal removes unused attached strips, persists a cleanup intent,
  releases their blocks, and clears the intent.
- The more aggressive tentative `reserve → consume → confirm` protocol remains
  tracked in `doc/backlog/R136-chunkio-reserve-confirm-strip-flow.md`.

## 5. Phase 3: Incremental EC (Pending)

### 5.1 Current Behavior

EC conversion happens at strip close: the full shadow buffer is
frozen, split into 8 data shards, 4 parity shards are computed, and
all 12 shards are written to EC segments. Memory is released only
after the full conversion completes.

### 5.2 Design Requirements (from user)

- Continue using the single 1 MiB shadow buffer.
- As the buffer fills, make it eligible for EC processing.
- Compute the 8 EC 1 MiB outputs in the same general flow as large
  writes.
- Write/update EC outputs incrementally.
- Release consumed/redundant memory as soon as safe.
- Avoid a second full-strip or compact staging buffer.
- Use views/slices rather than copies.
- Preserve correctness for strip-close metadata and location updates.

### 5.3 Resolved Direction (tracked by R137)

1. EC operates one 1 MiB disk block at a time. For shared chunks, hand the
   existing buffer view to EC only after mirror DiskIO and any repair succeed;
   speculative pre-IO EC is rejected because terminal mirror failure complicates
   rollback.
2. Release the 1 MiB image after both mirror DiskIO and its parity contribution
   finish. Do not retain all eight input images.
3. Shared chunks allocate/prefetch one special 28-block group containing eight
   three-replica MirrorStrips plus four parity blocks. One placement operation
   ensures that a choice of one survivor per mirror produces an optimal 8+4 EC
   layout. Before fenced EC publication removes redundant replicas, ChunkDB
   revalidates that optimum against current topology and may reselect survivors.
   If no optimal selection remains, it keeps the mirrors and persists a
   retryable `MirrorToEc` task to track relocation instead of leaving an
   untracked cleanup gap.
4. Early seal after only one to seven used strips is valid. Keep the used strips
   mirrored and release unused attached strips, all four parity blocks, and the
   incomplete group plan. Very small chunks are supported but not optimized.
5. `advance_chunk_write` tracks acknowledged mirror bytes only. A separate
   conversion plan tracks closed/mirror-durable inputs and publishes after the
   persisted cursor covers all eight. Conversion never blocks the response path;
   failure leaves mirrors authoritative.

Detailed design and acceptance are in
`doc/backlog/R137-chunkio-incremental-ec-conversion.md`.

## 6. Verification

### Build and Test Commands

```bash
pixi run build-cpp
pixi run -- cargo build --release -p crowdb-chunk-client
pixi run -- cargo test --release -p crowdb-chunk-client
pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server \
    -p crowdb-diskdb -p crowdb-chunkdb
CHUNKIO_SMALL_BENCH_CASES="small_1k_1t" CHUNKIO_SMALL_BENCH_DURATION=5 \
    CHUNKIO_SMALL_BENCH_SKIP_BUILD=1 \
    pixi run bash tools/bench-chunkio-small-write-regression.sh
```

### Key Tests

- `lib/crowdb-chunk-client/tests/small_object_test.rs` — 22 unit
  tests including repair, batching, scale-out.
- `lib/crowdb-chunk-client/tests/small_object_writer_e2e.rs` — 13
  e2e tests with real ChunkDB + DiskIO.
- `tools/bench-chunkio-small-write-regression.sh` — latency and
  payload regression sentinel.

### Verification Checklist

- [x] 1 KiB logical writes send only 1 KiB per mirror (not 1 MiB).
- [x] All affected chunk-client tests pass.
- [x] Re-establish the p50 < 1200 us target. After removing the next-request
  metadata wait and adding batched strip prefetch, the five-second
  `small_1k_1t` run measured p50 315 us, p99 680 us, 2953.41 objects/s, zero
  errors, and exact 1 KiB payloads per mirror.
- [x] DiskIO applies alignment/padding at the server (Phase 2).
- [x] NullDisk/MemDisk traverse the same padding path (Phase 2).
- [ ] EC conversion is incremental (Phase 3, tracked by R137).
- [x] The 1 KiB case sends exactly 1 KiB per mirror without client staging.
- [x] Metadata advancement stays off the response critical path.
- [x] Mirror strips are attached in bounded batches and replenished in the
  background; seal releases unused attached strips.

## 7. Files Modified in Phase 1

- `lib/crowdb-chunk-client/src/writer/small_pipeline.rs`
- `lib/crowdb-chunk-client/src/disk_io/disk_writer.rs`
- `lib/crowdb-chunk-client/src/disk_io/routing.rs`
- `lib/crowdb-chunk-client/src/client.rs`
- `lib/crowdb-chunk-client/tests/small_object_test.rs`
- `lib/crowdb-chunk-client/tests/small_object_writer_e2e.rs`
- `lib/crowdb-chunk-client/tests/common/mod.rs`
- `lib/crowdb-chunk-client/tests/chunk_writer_test.rs`
- `lib/crowdb-chunk-client/tests/chunk_reader_test.rs`
- `lib/crowdb-chunk-client/tests/chunk_reader_e2e.rs`
- `lib/crowdb-chunk-client/tests/large_object_writer_e2e.rs`
- `lib/crowdb-chunk-client/tests/ec_strip_writer_test.rs`


## 7.1 Files Added or Updated in Phase 2

- `app/crowdb-diskio/src/engine/aligned_writer.cpp`
- `app/crowdb-diskio/src/engine/aligned_writer.h`
- `app/crowdb-diskio/tests/aligned_writer_test.cpp`
- `app/crowdb-diskio/src/rpc/dio_server.cpp`
- `doc/design/diskio/design-crowdb-diskio.md`
## 8. Key Technical Notes

### Byte-Offset Trait Contract

`write_at_byte_offset` is required so every `DiskWriter` explicitly defines
how arbitrary byte offsets are routed. The former aligned default was not a
valid fallback for the small-write path. Production implementations validate
the byte range against segment capacity before issuing the RPC.

### Bytes View Lifetime

`Bytes::slice()` creates a view into the same allocation with a
refcount increment. The view keeps the underlying allocation alive
until all clones are dropped. This allows three mirror RPCs to share
the same view without copying. After all RPCs complete, the frozen
buffer can be reclaimed via `try_into_mut()` (if no views remain) or
copied into a new `BytesMut` (fallback).

### Shadow Buffer Reclamation

```rust
let frozen = shadow.freeze();
let view = frozen.slice(block_offset_us..written_end);
// ... send view to mirrors, await completion ...
self.shadow = Some(frozen.try_into_mut().unwrap_or_else(|shared| {
    BytesMut::from(shared.as_ref())
}));
```

`try_into_mut()` returns `Ok(BytesMut)` if the `Bytes` has a single
reference (no outstanding views), allowing zero-copy reclamation. If
views remain (shouldn't happen after all RPCs complete), it returns
`Err(SharedBytes)` and we copy into a fresh `BytesMut`.
