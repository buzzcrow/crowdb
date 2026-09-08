<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R93: chunkdb — Mirror-to-EC Conversion

**Problem**

The landed small-object writer stores shared-chunk data in mirror strips
with 3 replicas first for low write latency — the caller gets success after 3 mirror
writes, before EC encoding. Mirror strips use 3× storage (3 full
copies). As data ages and becomes colder, the 3× storage overhead is
wasteful: an 8+4 EC strip stores the same data capacity at 1.5×
overhead (12 blocks for 8 data blocks) vs 3× for 3-way mirror.

chunkdb v1 (R85) supports both mirror and EC strips but has no
mechanism to convert one to the other. The chunkdb design §2 Non-Goals
explicitly defers "Background conversion of mirror strips to EC strips
(for shared chunks)" to a future requirement. The small-object
writer depends on this conversion — without it, shared chunks
permanently carry 3× mirror overhead.

**Current behavior + impact**: A mirror strip stays a mirror strip
forever. There is no `convert_strip` or background conversion task.
Shared chunks accumulate mirror strips at 3×
storage cost with no path to the more space-efficient EC encoding.
For a cluster storing 100 TB of small-object data, this is 200 TB of
wasted space (300 TB mirror vs 150 TB EC at 8+4).

**Design pointers**: chunkdb root design §2 (Non-Goals: "No
mirror-to-EC conversion in v1"), §5.2 (Strip — mirror vs EC data
capacity), §10.6 (landed fenced `replace_chunk_strip_range` transaction), §11 (EC
Encoding/Decoding — isa-l encode from data blocks).
R93 orchestrates the conversion: read a capacity-compatible group of
mirror strips, EC-encode, allocate EC blocks, write data+parity, then
atomically replace that strip range.

**Use scenarios**:

- **Background conversion of a sealed shared chunk**: A shared chunk
  has been sealed (no more writes). A background conversion task
  picks it up, reads eight adjacent 1 MiB mirror strips, EC-encodes the
  8 MiB into 8+4 one-unit shards, allocates an EC strip via chunkdb,
  writes the EC blocks via diskio (R105), and atomically replaces the
  eight-strip range with that EC strip. The old mirror blocks
  are freed. Expected: the chunk's data capacity is preserved; storage
  overhead drops from 3× to 1.5× (8+4 EC).

- **Conversion of an active shared chunk**: A shared chunk is still
  receiving writes (Active state). Eight adjacent mirror strips that
  carry the writer's durable closed marker can be converted while the chunk
  continues to receive writes to later strips. Expected: no write
  latency impact for new writes; the converted range is read-only.

- **Conversion under read load**: A reader (R107) is reading from a
  mirror range that is being converted. The range replacement is
  atomic — the reader either sees all old mirror strips or the new EC
  strip, never a partial state. Retired mirrors remain allocated through
  R107's bounded layout-validity window. Expected: reads
  continue without error; the reader may need to switch from mirror
  read to EC read if the conversion completes mid-read.

- **Conversion failure recovery**: The EC encode fails (isa-l error)
  or an EC block write fails (disk error). The conversion aborts; the
  mirror-strip group is untouched; the partially allocated EC blocks are
  freed via chunkdb rollback. Expected: the chunk is unchanged; the
  conversion task retries later or skips the strip.

- **Batch conversion**: An operator triggers conversion of all sealed
  shared chunks older than 24 hours. The conversion task processes
  chunks in priority order (oldest first, or most-mirror-strips first)
  with configurable concurrency and bandwidth throttling. Expected:
  gradual space reclamation without foreground traffic impact.

**Solution**

A background conversion service in chunkdb that transforms compatible
mirror-strip groups into EC strips using the landed fenced
`replace_chunk_strip_range` as the atomic swap primitive. The conversion reads mirror data via
diskio (R105), EC-encodes via isa-l (crowdb-common), allocates EC strip
blocks via chunkdb, writes EC data+parity via diskio, and swaps the
strip range atomically. Conversion is triggered by a configurable policy
(seal age, mirror strip count, manual trigger) and throttled to avoid
starving foreground traffic.

**One-line summary**: group adjacent mirror strips into capacity-
compatible EC stripes, then use diskio, isa-l, and a fenced atomic range
swap to reclaim 3×→1.5× storage without changing logical offsets.

**Numbered work items**:

1. **Conversion task** (`app/crowdb-chunkdb/src/conversion.rs`) — a
   background `BgRunner` task (following the diskdb `ScannerTask`/
   `BgRunner` pattern, §10) that scans for convertible chunks. A
   chunk is convertible if: (a) it has at least `data_num` adjacent,
   equal-capacity mirror strips, (b) it is Sealed or every strip in the
   range has the writer's durable closed marker, (c)
   it meets the conversion policy (age, strip count, manual trigger).
   The task enqueues compatible strip groups into a work queue with
   configurable concurrency (default 4 parallel conversions) and
   bandwidth throttling (default 50 MB/s, configurable).

2. **Conversion logic** (`app/crowdb-chunkdb/src/conversion.rs`) —
   for each group of `data_num` adjacent mirror strips:
   - Read each mirror strip's data from one replica via `DiskIoClient::read`
     (R105). If the primary replica's disk is `Bad`, fall back to
     another replica.
   - Concatenate the `data_num` equal-capacity inputs as the EC data
     shards and encode `code_num` parity shards via `crowdb-common`.
     Each shard keeps the mirror strip's integral diskdb unit count, so
     no sub-unit allocation is assumed.
   - Allocate `data_num + code_num` shards via chunkdb's rack-aware EC
     placement (§7.2). Set the replacement strip's `chunk_offset` to
     the first mirror's offset and its logical capacity and sealed
     length to the sums across the old range.
   - Write the `data_num` + `code_num` blocks to the allocated
     segments via `DiskIoClient::write` (R105), in parallel.
   - `fsync` each disk via `DiskIoClient::fsync` (R105).
   - Call `replace_chunk_strip_range` with the chunk ID, expected
     revision, exact mirror-strip range, stable operation identity, and
     new EC strip. Commit only new EC segments, atomically splice the
     replacement together with its cleanup intent, and then clean up
     only removed mirror segments. Require equal old/new total logical
     capacity so every later strip offset remains unchanged. This
     uses the landed fenced, restart-safe strip replacement contract;
     query-and-compare alone
     cannot recover old segments after a server crash.
   - Preserve the chunk's monotonic `next_strip_sequence`. Replacing
     eight entries with one does not renumber later strips, and a later
     append consumes the persisted next value rather than
     `strips.len()`.
   - On any failure: free the partially allocated EC blocks via
     chunkdb, leave the mirror range untouched, log the error, retry
     later.

3. **Conversion policy** (`app/crowdb-chunkdb/src/conversion.rs`) —
   configurable triggers:
   - `conversion_min_seal_age_secs` (default 3600) — only convert
     strips in chunks sealed more than N seconds ago.
   - `conversion_min_mirror_strips` (default 8) — only convert chunks
     with at least one full `data_num` group (amortize conversion).
   - `conversion_max_concurrency` (default 4) — max parallel
     conversions.
   - `conversion_max_bandwidth_mbps` (default 50) — throttle read +
     write I/O to avoid starving foreground traffic.
   - Manual trigger via HTTP endpoint `POST /convert_chunk` with
     `{ "chunk_id": ... }` or `POST /convert_all` with a filter.

4. **Metrics + observability** (`app/crowdb-chunkdb/src/metrics.rs`) —
   extend `LifecycleMetrics` with conversion counters:
   `conversion_started_count`, `conversion_completed_count`,
   `conversion_failed_count`, `conversion_bytes_read`,
   `conversion_bytes_written`, `conversion_stripes_freed` (mirror
   blocks freed). HTTP endpoint `GET /conversion_metrics` returns the
   snapshot.

5. **ChunkdbClient conversion API** (`lib/crowdb-chunkdb-client/`) —
   `async fn trigger_conversion(&self, chunk_id: &ChunkId) ->
   Result<(), ConversionError>` (manual trigger for a single chunk),
   `async fn trigger_conversion_batch(&self, filter: &
   ConversionFilter) -> Result<u64, ConversionError>` (batch trigger,
   returns count of enqueued chunks). These are management APIs used
   by the console (R96) and CLI.

**Flow diagram**:

```
Conversion Task                chunkdb           diskio (R105)       isa-l
     │                            │                   │                │
     │ 1. Scan for convertible    │                   │                │
     │    chunks (mirror strips)  │                   │                │
     │ ◄──────────────────────────│                   │                │
     │                            │                   │                │
     │ 2. Read mirror group       │                   │                │
     │    from healthy replicas   │                   │                │
     │ ──────────────────────────────────────────────►│                │
     │ ◄──────────────────────────────────────────────│                │
     │  (Bytes: data_num mirror strips)               │                │
     │                            │                   │                │
     │ 3. EC encode (data + parity)                   │                │
     │ ───────────────────────────────────────────────────────────────►│
     │ ◄───────────────────────────────────────────────────────────────│
     │  (data_num + code_num blocks)                  │                │
     │                            │                   │                │
     │ 4. Allocate EC strip        │                   │                │
     │ ──────────────────────────►│                   │                │
     │ ◄──────────────────────────│                   │                │
     │  (EC strip segments)       │                   │                │
     │                            │                   │                │
     │ 5. Write EC blocks         │                   │                │
     │ ──────────────────────────────────────────────►│                │
     │ ◄──────────────────────────────────────────────│                │
     │                            │                   │                │
     │ 6. fsync all disks         │                   │                │
     │ ──────────────────────────────────────────────►│                │
     │ ◄──────────────────────────────────────────────│                │
     │                            │                   │                │
     │ 7. replace strip range     │                   │                │
     │    (atomic splice)         │                   │                │
     │ ──────────────────────────►│                   │                │
     │    (commit EC, publish +   │                   │                │
     │     cleanup intent, free   │                   │                │
     │     old-only segments)     │                   │                │
     │ ◄──────────────────────────│                   │                │
     │  Ok(())                    │                   │                │
```

**Edge cases at a glance**:

- Mirror replica read fails (disk `Bad`) → fall back to another
  replica; if all replicas fail, the strip is unrecoverable (data
  loss) — log critical error, skip, alert operator.
- EC encode fails (isa-l error) → abort conversion, free allocated EC
  blocks, leave mirror strip, retry later.
- EC block write fails (disk error on target) → abort, free allocated
  blocks, retry with a new EC strip allocation (different placement).
- `replace_chunk_strip_range` returns an ambiguous error → retry the same
  operation identity. The persisted cleanup intent distinguishes an
  installed EC strip awaiting old-segment cleanup from an uncommitted
  replacement; a different revision or strip is a conflict.
- Chunk is deleted during conversion → the conversion task detects
  the `Deleted` state and aborts; any allocated EC blocks are freed.
- Concurrent conversion + read → range replacement is atomic under
  the per-chunk lock (§10); the reader sees either the old mirror or
  the new EC strip, never a partial state, and old segments remain
  readable until the layout-validity grace expires.
- Conversion throttling under foreground load → bandwidth limiter
  (token bucket) reduces conversion I/O rate when foreground disk
  I/O is high; conversion pauses if the disk is near saturation.

**Dependencies**

- **Depends on**: **R105** (disk IO engine) — reads mirror data and
  writes EC blocks via `DiskIoClient`. **chunkdb** (landed, R85) —
  uses the landed range-replacement RPC, `AllocateStrip`, and chunk metadata.
  **crowdb-common EC** (landed with R85) — isa-l encode. The landed strip
  replacement transaction supplies revision fencing,
  capacity-preserving validation, set-difference commit/free, and
  persisted cleanup intent.
- **Integrates with**:
  - **Small-object writer** — R93 converts the mirror strips that the
    landed writer produces. Those strips are correct but space-inefficient;
    R93 makes the mirror-first strategy viable long-term.

**Acceptance**

**Conversion correctness**:
- A sealed shared chunk with 24 adjacent mirror strips (each 1 MiB, 3
  replicas) → run conversion → chunk has 3 EC strips (each 8+4, 8 MiB
  logical capacity with 1 MiB shards), every later `chunk_offset` and
  the total chunk capacity are unchanged, and all 72 old mirror
  segments are freed. Verify via `query_chunk`. Integration test.
- Data integrity after conversion: read the chunk's data via R107
  (read flow) before and after conversion → identical bytes.
  Integration test.
- EC parity correctness: after conversion, simulate one EC data block
  failure → EC decode reconstructs the data. Integration test (uses
  isa-l decode).

**Conversion atomicity**:
- A read (R107) concurrent with conversion → read succeeds with
  either the old mirror strip or the new EC strip, never errors.
  Integration test (start read mid-conversion).
- Conversion failure mid-way (inject diskio write error on 2nd EC
  block) → the eight-strip mirror range is untouched, EC blocks are
  freed, and chunk metadata shows every original mirror strip.
  Integration test.
- Chunkdb restarts after publishing an EC strip but before freeing its
  mirror segments → persisted cleanup resumes, only old mirror segments
  are freed, and the EC replacement is not replayed. Integration test.
- Convert the first eight strips of an Active chunk, then append a new
  mirror strip → the append uses `next_strip_sequence`, no sequence is
  duplicated, and all strip offsets remain ordered. Integration test.
- Start a read with the old mirror-range layout immediately before
  conversion → conversion publishes the EC strip but does not make old
  segments reusable until the advertised layout-validity grace expires;
  the read succeeds or retries under R107's deadline. Integration test.

**Conversion policy**:
- Chunk sealed < `conversion_min_seal_age_secs` ago → not enqueued
  for conversion. Unit test (mock time).
- Chunk with < `conversion_min_mirror_strips` → not enqueued. Unit
  test.
- A sealed tail with ten equal-capacity mirror strips → convert the
  first eight as one 8+4 group, leave the final two mirrored, and keep
  all logical offsets unchanged. Integration test.
- Manual trigger via `trigger_conversion(chunk_id)` → chunk is
  enqueued regardless of policy. Integration test.

**Throttling**:
- `conversion_max_bandwidth_mbps = 10` → conversion I/O rate does not
  exceed 10 MB/s (measured over a 5-second window). Integration test.
- `conversion_max_concurrency = 2` → at most 2 strips are being
  converted simultaneously. Integration test.

**Fallback + error handling**:
- Primary mirror replica disk is `Bad` → conversion reads from
  secondary replica, succeeds. Integration test.
- All mirror replicas are `Bad` → conversion logs critical error,
  skips the strip, continues with other chunks. Integration test.
- Chunk deleted during conversion → conversion aborts, allocated EC
  blocks are freed. Integration test.

**Metrics**:
- After converting 24 one-MiB mirror strips into three 8+4 groups →
  `conversion_completed_count` = 3, `conversion_bytes_read` = 24 MiB,
  `conversion_bytes_written` = 36 MiB, and
  `conversion_stripes_freed` = 72 mirror segments. Integration test.

**Test commands**: `pixi run cargo test -p crowdb-chunkdb --test
conversion`, `pixi run cargo test -p crowdb-chunkdb-client --test
conversion_api`, `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **Convert in-place vs new allocation**: Should the EC strip reuse
  the mirror strip's disk blocks (in-place, fewer allocations) or
  allocate fresh blocks (simpler, requires freeing mirror blocks)?
  In-place is complex (mirror blocks are on 3 nodes, EC blocks need
  12 nodes) and rarely possible. Fresh allocation is cleaner and
  decouples conversion from placement. The current design uses fresh
  allocation. Confirm this is the right trade-off.
