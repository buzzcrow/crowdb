<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Mirror-to-EC Conversion (R93)

This implementation design refines
[R93](../backlog/R93-chunkdb-mirror-to-ec-conversion.md) within the
[chunkdb design](../design/chunkdb/design-crowdb-chunkdb.md). It builds on the
landed shared small-object writer, DiskIO RPC, EC codec, monotonic strip
sequence, and fenced range-replacement transaction. The normal path performs
incremental EC in the chunk client without rereading mirror data. A durable
chunkdb task system owns discovery, takeover, retry, and the generic conversion
path after a client failure. The public R107 chunk-read API is still a
placeholder, so real-process tests validate the same mixed-layout mapping and
decode invariants that R107 will consume.

## 1. Client Incremental EC Fast Path

Every small-write pipeline groups eight consecutive closed one-MiB mirror
strips. When one mirror shadow becomes durable, the pipeline freezes its
existing `Bytes` allocation, installs a fresh shadow, and synchronously applies
that shard to four persistent parity buffers. The safe EC wrapper exposes an
incremental parity update backed by isa-l; completing shard eight therefore
does not rescan or concatenate the preceding seven shards.

The group owns eight immutable data shards plus four parity shards, all charged
to the writer memory budget. It is active work, so a pipeline with a partial
group is not eligible for queue-driven scale-in. User writes remain durable at
the mirror boundary; failure of the space-reclamation phase does not revoke an
acknowledgement.

After shard eight, the client begins a durable `MirrorToEc` task in chunkdb,
receives the tentative 8+4 placement, writes its retained data and computed
parity directly, fsyncs every target disk, and requests completion. Completion
uses the existing fenced range replacement. This is the normal route and does
not issue mirror reads.

## 2. Durable Chunkdb Task Framework

Chunkdb does not currently have a general task system. R93 adds one rather
than introducing another independent timer loop. A `TaskRecord` is persisted
in the same KV group as its chunk and contains:

- stable task ID, kind, payload version, chunk ID, and deterministic operation
  identity;
- state (`Pending`, `Claimed`, `Running`, `Retryable`, `Completed`, `Failed`,
  or `Cancelled`), attempt count, priority, and last error;
- owner instance, claim generation, lease deadline, and source chunk revision;
- kind-specific progress sufficient for idempotent takeover.

`TaskManager` owns persistence, admission, claim/release, status queries,
cancellation before publication, metrics, and dispatch. `TaskScanner` scans
both task records and chunk source-of-truth records. It requeues expired claims
and creates deterministic tasks for closed mirror groups that have no task.
An event wakes it after client admission; a periodic scan is only a missed-event
safety net. `TaskExecutor` dispatches to registered kind handlers. Pending
queue bytes/count control worker scale between configured bounds; elapsed idle
time is not a scaling input.

Duplicate execution is safe even across ownership changes: allocation belongs
to the task's stable identity, data writes are repeatable, and publication is
fenced by the exact old strip range plus chunk revision. At most one duplicate
can publish. A losing executor re-queries metadata before it frees anything.

The first handler is `MirrorToEcTask`. The envelope intentionally supports
later `ReplicaRepair`, `EcRebuild`, `RetiredSegmentCleanup`,
`OrphanAllocationCleanup`, and `PlacementVerification` handlers without putting
their payload fields in the common scheduler.

### 2.1 Task Keys

Every task has one canonical value and, while runnable or claimed, one
secondary index. All integers are big-endian so scans retain numeric order.

```text
ChunkTaskKey        = magic | 0x000D | partition-id:16 | kind:u16 | task-id:16
ReadyChunkTaskKey   = magic | 0x000E | priority-inverse:u8 |
                      eligible-at-ms:u64 | partition-id:16 | kind:u16 |
                      task-id:16
LeasedChunkTaskKey  = magic | 0x000F | lease-deadline-ms:u64 |
                      partition-id:16 | kind:u16 | task-id:16
```

`partition-id` is the target `ChunkId` for chunk-scoped work. `TaskStore`
routes every canonical and index mutation with that ID, so all records for a
task share the chunk's KV group and can change in one atomic `batch_write`.
Future range or topology tasks derive a synthetic partition ID whose hash is in
the owning chunkdb range; the typed payload retains the actual resource.

`task-id` is the stable 128-bit hash of kind plus kind-specific deduplication
identity. Mirror conversion uses `(chunk_id, first_strip_sequence, data_num,
code_num)`. Repeated scanner discovery therefore addresses the same canonical
key. A task generation that is intentionally rerun adds its source generation
to the identity rather than overwriting an older terminal record.

The ready index orders higher priority first and then retry eligibility. Its value
is empty; the canonical record is always fetched before claim. The lease index
lets the scanner find expired claims without scanning every running task.
Pending queue record count and `estimated_queue_bytes` from canonical values
drive executor scale-out/in. `eligible-at` and lease deadlines schedule retry
and takeover only; elapsed idle time is not an executor scaling input.

State changes update the canonical value and its old/new index keys in one KV
batch. The scanner treats indexes as rebuildable: a missing or stale index is
repaired from the canonical value, never used as task truth.

### 2.2 Task Value

The canonical value is a verified FlatBuffer with file identifier `CTSK`.
FlatBuffers allows common fields to be appended compatibly. Kind-specific data
is an opaque verified payload so adding a task kind does not change the common
manager or force unrelated workers to decode it.

```rust
struct ChunkTaskValue {
    schema_version: u16,
    task_id: TaskId,
    partition_id: TaskPartitionId,
    kind: u16,
    kind_version: u16,
    state: TaskState,
    priority: u8,
    revision: u64,
    operation_id: TaskId,
    source_revision: u64,
    created_at_ms: u64,
    updated_at_ms: u64,
    eligible_at_ms: u64,
    attempt: u32,
    max_attempts: u32,
    estimated_queue_bytes: u64,
    claim_owner: u64,
    claim_generation: u64,
    claim_deadline_ms: u64,
    last_error_code: u16,
    last_error: String,
    payload: Vec<u8>,
}
```

`TaskState` is `Pending`, `Running`, `RetryWait`, `Completed`, `Failed`, or
`Cancelled`. Claim data is meaningful only in `Running`. `revision` increases
on every durable transition; `claim_generation` increases on each takeover and
is copied into executor completion requests. Error text has a fixed encoded
limit so repeated failures cannot grow records without bound.

The manager understands only the envelope. Each registered `TaskHandler`
declares its stable `kind`, supported `kind_version`, payload verifier,
estimated-cost function, and `execute` implementation. Unknown kinds or newer
payload versions are retained and reported as unsupported; they are never
deleted or executed by an older binary.

### 2.3 Mirror-to-EC Payload

`MirrorToEcTaskV1` contains the exact old mirror strips, first vector index,
scheme, allocation identity, optional tentative EC strip, and phase. Storing
the full old range makes ambiguous-result comparison possible after the chunk
has already changed. The phase is one of `Discovered`, `Allocated`,
`Writing`, `Durable`, `Published`, or `Cleaning`; a takeover may conservatively
repeat writes regardless of the recorded writing phase.

The common task state controls scheduling; the payload phase records domain
progress. Neither controls what readers see. Terminal task records are retained
for a bounded audit window and then compacted only after the handler proves no
segments remain task-owned.

## 3. Conversion Selection

A `ConversionService` scans chunk metadata in bounded pages and selects runs of
`data_num` adjacent mirror strips. A run is eligible when all strips:

1. have the same nonzero `unit_kb`, capacity, and integral `unit_count`;
2. are contiguous by `chunk_offset`;
3. are immutable because the chunk is Sealed, or because every sequence is at
   or below `closed_strip_sequence`;
4. meet automatic policy thresholds for seal age and mirror-strip count.

Manual single-chunk conversion bypasses age and count policy, but never layout,
closure, or state safety. An incomplete tail remains mirrored. Groups are
processed from low to high offset so every successful replacement preserves
the next group's offset while reducing the vector length.

Automatic selection is scan-driven. It uses a configurable scan interval only
to discover work; eligibility never depends on elapsed worker idleness. The
work bound is `conversion_max_concurrency`, and each chunk has at most one
conversion in flight in a scan pass.

## 4. DiskIO Routing

`ConversionDiskIo` owns an atomically published `DiskId -> DiskIO endpoint`
route map discovered through group-0 hardware and service-registry records. It
uses one `DiskioClient` and RPC runtime, and exposes:

```rust
async fn read_segment(&self, segment: &Segment, byte_len: u32) -> Result<Bytes>;
async fn write_segment(&self, segment: &Segment, data: Bytes) -> Result<()>;
async fn fsync_segments(&self, segments: &[Segment]) -> Result<()>;
```

The chunkdb handler uses mirror reads only for scanner-created tasks or takeover
after a client stops before completion. Mirror reads try replicas in metadata order, skipping identities listed in
`unavailable_segments`, and continue after routing or DiskIO errors. Exhausting
all replicas is an unrecoverable group failure; it does not modify metadata and
does not stop other chunk conversions.

Writes issue all data and parity shard writes concurrently. A write or fsync
failure discards the entire tentative EC allocation. A later scan may choose a
fresh placement. No conversion state is published before all shards are
durable.

## 5. EC Replacement

For each admitted task, `LifecycleHandler::allocate_conversion_strip`
allocates one tentative EC strip using normal rack-aware placement. The new
strip has:

- the first mirror's `chunk_offset` and `strip_sequence`;
- the sum of old capacities and sealed lengths;
- the common mirror `unit_kb` and `unit_count`;
- `data_num + code_num` segments and `EcState::Parity` after durable writes.

The client supplies retained data and incrementally computed parity on the fast
path. A chunkdb takeover reads one full shard from every mirror. The mirror
bytes are the EC data shards without concatenation or reshaping;
`encode_parity_from_shards` generates parity without building a second
contiguous input buffer.

After all writes and fsyncs succeed, the service calls
`replace_chunk_strip_range` with the observed revision, exact old range, and a
stable operation ID derived once per attempt. An ambiguous transport outcome
retries the same operation. A revision/range conflict discards a replacement
only after a fresh query proves it was not installed. The lifecycle transaction
commits new segments, publishes the EC strip plus cleanup intent, and retires
only old mirror segments after the layout-validity grace. The persisted
`next_strip_sequence` remains monotonic.

## 6. Failure and Restart Behavior

- A client error explicitly releases the durable claim; a client crash is
  recovered after claim expiry. Both leave the mirror range authoritative.
- Read, encode, allocation, write, or fsync failure remains retryable until its
  configured attempt budget is exhausted.
- A conflict caused by another append or conversion causes a fresh selection;
  already installed operations are recognized by stable identity.
- Deletion makes subsequent lifecycle operations fail and the tentative
  replacement is discarded.
- Published cleanup intents are already replayed by
  `reconcile_pending_chunks`; restart cannot re-publish the replacement or free
  a referenced EC segment.
- Conversion task failures are isolated per group and counted. Automatic scans
  continue with later chunks.

### 6.1 Correctness Invariants

- **C1 — Layout is authoritative:** task state never selects readable data.
  Readers use only the committed `Chunk.strips` layout.
- **C2 — Mirror-before-publish:** every old mirror remains referenced and
  allocated until every EC shard has been written and fsynced.
- **C3 — Single atomic visibility point:** the fenced range replacement is the
  only operation that makes EC readable. No intermediate task phase changes
  chunk layout.
- **C4 — Ambiguity retains both sides:** an executor never discards tentative EC
  blocks after an ambiguous replacement result. It first queries the chunk and
  compares operation identity, exact range, and segment identities.
- **C5 — Referenced blocks are never cleanup candidates:** every discard and
  recovery path rechecks the current chunk under its existing lifecycle guard.
- **C6 — Retirement is replayable:** publication persists the old-segment
  cleanup intent with the new layout. A crash cannot publish EC without also
  retaining enough information to retire mirrors safely.
- **C7 — Duplicate workers are harmless:** task ID and replacement operation ID
  are deterministic for `(chunk_id, first_strip_sequence, scheme)`. Revision
  fencing permits at most one layout publication.
- **C8 — Later appends are independent:** conversion preserves first offset,
  total capacity, and `next_strip_sequence`; appending after any crash cannot
  duplicate or reorder strip identity.

### 6.2 Crash Matrix

1. **Client crashes while collecting fewer than eight shards:** completed
   mirrors remain readable. No task is required; later conversion may combine
   the group after enough closed mirrors exist.
2. **Client crashes after the eighth mirror but before task admission:** the
   scanner derives the same deterministic task from the closed strip range.
3. **Crash before or during EC allocation:** the task stays retryable and no
   chunk metadata changes. Allocation responses are associated with the stable
   task identity; an unknown response is reconciled before allocating again.
4. **Crash during EC writes or before fsync:** mirrors remain authoritative. A
   takeover rewrites all 12 shards and fsyncs them; it does not trust a partial
   progress bitmap for durability.
5. **Crash after fsync but before replacement:** the worker reuses the recorded
   placement and stable operation identity, then publishes or safely retries.
6. **Crash during replacement response:** the outcome is ambiguous. Recovery
   queries the chunk; an installed operation advances to cleanup, while an
   unchanged exact old range retries the same replacement.
7. **Crash after replacement but before task completion:** the scanner sees the
   installed operation and marks the task complete. It never rewrites the range
   or frees referenced EC segments.
8. **Crash during mirror cleanup:** the layout and cleanup intent were persisted
   together. Reconciliation frees only old-only segments after layout grace and
   can repeat safely.

An allocation whose RPC result is unknown must not trigger a second unrelated
allocation. The task allocation call therefore carries a stable allocation
identity and is idempotent per disk group. This closes the response-loss window
where physical blocks could otherwise become untracked. Resource leakage is
treated as a correctness failure for the task system even though mirror data
would remain readable.

## 7. Policy and Throttling

`ConversionConfig` adds:

```rust
pub struct ConversionConfig {
    pub enabled: bool,
    pub data_num: u32,
    pub code_num: u32,
    pub min_seal_age_secs: u64,
    pub min_mirror_strips: u32,
    pub max_concurrency: usize,
    pub max_bandwidth_mbps: u64,
    pub scan_interval_secs: u64,
}
```

Defaults are disabled for rollout safety, 8+4, one-hour seal age, eight mirror
strips, four workers, 50 MiB/s, and a 30-second scan interval. Validation
rejects zero scheme dimensions, a mirror threshold below `data_num`, zero
concurrency/bandwidth/interval, and overflow-prone rates.

A shared token bucket accounts actual mirror bytes read plus EC bytes written.
Tokens are acquired before each group I/O phase, with a burst no larger than
one conversion group. The limiter uses time only to enforce a bandwidth rate,
not to decide whether conversion or concurrency scales.

## 8. Management and Metrics

Two crowdb-rpc management calls route through `ChunkdbClient`:

```rust
async fn trigger_conversion(&self, chunk_id: &ChunkId) -> Result<()>;
async fn trigger_conversion_batch(&self, filter: &ConversionFilter) -> Result<u64>;
```

The first converts all currently eligible closed groups in one chunk. The
second scans this instance's chunks and enqueues matching candidates. HTTP
`POST /convert_chunk`, `POST /convert_all`, and `GET /conversion_metrics`
delegate to the same service. Trigger responses report accepted work; final
results are observed through metadata and metrics.

`ConversionMetrics` uses atomics for started, completed, failed, bytes read,
bytes written, mirror segments retired, active conversions, and peak active
conversions. The HTTP snapshot is nonblocking.

## 9. Scope

- `app/crowdb-chunkdb/src/task.rs` and `task/`: persistent records, store,
  scanner, manager, executor, and handler dispatch.
- `app/crowdb-chunkdb/src/conversion.rs`: selection, I/O orchestration,
  throttling, automatic runner, and manual triggers.
- `app/crowdb-chunkdb/src/conversion/io.rs`: lock-free DiskIO routing and block
  operations.
- `app/crowdb-chunkdb/src/chunkdb_config.rs`: conversion policy.
- `app/crowdb-chunkdb/src/lifecycle/handler.rs`: tentative EC allocation and
  safe discard helpers.
- `app/crowdb-chunkdb/src/metrics.rs`: conversion metrics.
- `app/crowdb-chunkdb/src/main.rs`: service construction, runner shutdown, and
  HTTP endpoints.
- `app/crowdb-chunkdb/src/service/chunkdb_rpc_service/`: manual RPC handlers.
- `lib/crowdb-protocol/src/types/chunkdb.rs` and FlatBuffers schemas: trigger
  requests and responses.
- `lib/crowdb-chunkdb-client/`: routed management methods.
- `lib/crowdb-chunk-client/src/writer/small_pipeline.rs` and EC modules:
  incremental parity, group memory ownership, and client task execution.
- `app/crowdb-chunkdb/tests/conversion_test.rs`: lifecycle and conversion
  integration coverage.
- `lib/crowdb-chunkdb-client/tests/conversion_api_test.rs`: real transport API
  coverage.
- `lib/crowdb-chunk-client/tests/small_object_writer_e2e.rs`: full-process
  small-write-to-EC data-path coverage.

## 10. Complexity

Medium. The codec, mirror shadow, lifecycle fencing, cleanup intent, KV scan,
and DiskIO RPC are already available. The new work is a small persistent task
envelope plus scanner/dispatcher, incremental parity, and orchestration of
those landed primitives. The main care is exhaustive crash-point testing, not
algorithmic complexity.

## 11. Test Design

- **Selection policy**: create sealed/active chunks with varied ages, closed
  markers, counts, capacities, and gaps -> select groups -> assert only complete
  immutable equal-geometry runs are returned and tails remain mirrored.
- **Fast-path 24-to-3 conversion**: write 24 one-MiB mirror strips through the
  real client -> incrementally encode and complete three tasks without mirror
  reads -> assert three 8+4 EC strips, identical total capacity
  and offsets, 72 retired mirror segments, and monotonic sequence state.
- **Data and parity**: write deterministic bytes -> convert -> read all EC data
  shards and compare with original; omit one data shard and decode with isa-l ->
  assert exact reconstruction.
- **Active prefix**: close eight strips, leave a later strip open -> convert ->
  assert only the prefix changes; append another mirror -> assert unique
  sequence and ordered offsets.
- **Write failure**: fail the second EC target write -> trigger -> assert all
  mirror metadata remains and tentative allocations are freed.
- **Replica fallback**: fail the first mirror route/read -> convert -> assert a
  secondary supplies identical bytes; fail all replicas -> assert the group is
  skipped, another chunk progresses, and failure metrics increment.
- **Concurrent read/layout grace**: retain the queried mirror layout, pause
  conversion before publish, issue a real DiskIO read, resume conversion ->
  assert old-layout read succeeds and retired segments are not reusable before
  grace expiry.
- **Restart cleanup**: publish replacement with nonzero grace, restart chunkdb,
  expire/reconcile -> assert only retired mirrors are freed and EC segments
  remain referenced.
- **Deletion race**: pause after allocation, delete the chunk, resume -> assert
  conversion aborts and all tentative EC blocks are freed.
- **Client takeover**: stop the client after task allocation and after partial
  EC writes -> let the chunkdb scanner claim the durable task -> assert it reads
  mirrors, completes the same operation, and leaks no tentative blocks.
- **Scanner discovery**: stop the client before task creation with eight closed
  mirrors -> scan chunks -> assert one deterministic task is created and a
  repeated scan creates no duplicate.
- **Task dispatch**: persist multiple task kinds and states -> scan/claim ->
  assert only eligible tasks reach their registered handlers and claim
  generations fence stale completion.
- **Crash matrix**: stop client or chunkdb at every boundary in §6.2 -> restart
  and scan -> assert bytes remain readable, exactly one layout is authoritative,
  referenced blocks are never freed, and no tentative allocation is orphaned.
- **Throttle/concurrency**: use adjustable test parameters and multiple chunks
  -> measure a five-second transfer window and active gauge -> assert bandwidth
  is within one-group burst tolerance and peak workers never exceeds the bound.
- **Management API**: call both client methods through a real RPC server ->
  assert single trigger bypasses policy, batch returns candidate count, and
  malformed filters/errors map without panic.
- **Metrics**: convert 24 one-MiB mirrors -> assert three completions, 24 MiB
  read, 36 MiB written, 72 retired segments, zero active conversions.

## 12. Module Structure

```text
app/crowdb-chunkdb/src/
├── task.rs                # task domain and public manager surface
├── task/
│   ├── executor.rs        # bounded kind dispatch
│   ├── manager.rs         # admission, claim, retry, status
│   ├── scanner.rs         # record recovery + source discovery
│   └── store.rs           # KV persistence
├── conversion.rs          # service, selection, runner, throttle
├── conversion/
│   └── io.rs              # DiskIO discovery, read, write, fsync
├── lifecycle.rs
└── lifecycle/
    └── handler.rs         # tentative EC allocation/discard
```

## 13. Server Wiring

The server constructs `ConversionDiskIo` only when automatic conversion is
enabled, or lazily for a manual trigger. The runner receives the existing watch
shutdown channel. RPC and HTTP handlers hold `Arc<ConversionService>`; shutdown
stops admission, waits for in-flight groups, then lets normal cleanup
reconciliation handle any published retirement intents.

## 14. Open Questions

None. Fresh EC allocation is required because mirror placement cannot in
general satisfy EC fault-domain constraints, and it keeps rollback independent
of authoritative mirror data.
