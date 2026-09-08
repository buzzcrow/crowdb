<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Mirror-to-EC Conversion (R93)

This implementation design refines
[R93](../backlog/R93-chunkdb-mirror-to-ec-conversion.md) within the
[chunkdb design](../design/chunkdb/design-crowdb-chunkdb.md). It builds on the
landed shared small-object writer, DiskIO RPC, EC codec, monotonic strip
sequence, and fenced range-replacement transaction. The public R107 chunk-read
API is still a placeholder, so conversion owns only the block-reading needed
to build EC shards; real-process tests validate the same mixed-layout mapping
and decode invariants that R107 will consume.

## 1. Conversion Selection

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

## 2. DiskIO Routing

`ConversionDiskIo` owns an atomically published `DiskId -> DiskIO endpoint`
route map discovered through group-0 hardware and service-registry records. It
uses one `DiskioClient` and RPC runtime, and exposes:

```rust
async fn read_segment(&self, segment: &Segment, byte_len: u32) -> Result<Bytes>;
async fn write_segment(&self, segment: &Segment, data: Bytes) -> Result<()>;
async fn fsync_segments(&self, segments: &[Segment]) -> Result<()>;
```

Mirror reads try replicas in metadata order, skipping identities listed in
`unavailable_segments`, and continue after routing or DiskIO errors. Exhausting
all replicas is an unrecoverable group failure; it does not modify metadata and
does not stop other chunk conversions.

Writes issue all data and parity shard writes concurrently. A write or fsync
failure discards the entire tentative EC allocation. A later scan may choose a
fresh placement. No conversion state is published before all shards are
durable.

## 3. EC Replacement

For each eligible group, `LifecycleHandler::allocate_conversion_strip`
allocates one tentative EC strip using normal rack-aware placement. The new
strip has:

- the first mirror's `chunk_offset` and `strip_sequence`;
- the sum of old capacities and sealed lengths;
- the common mirror `unit_kb` and `unit_count`;
- `data_num + code_num` segments and `EcState::Parity` after durable writes.

The converter reads one full shard from every mirror. The mirror bytes are the
EC data shards without concatenation or reshaping; `encode_parity_from_shards`
generates the parity shards. This avoids copying all input into a second
contiguous buffer.

After all writes and fsyncs succeed, the service calls
`replace_chunk_strip_range` with the observed revision, exact old range, and a
stable operation ID derived once per attempt. An ambiguous transport outcome
retries the same operation. A revision/range conflict discards a replacement
only after a fresh query proves it was not installed. The lifecycle transaction
commits new segments, publishes the EC strip plus cleanup intent, and retires
only old mirror segments after the layout-validity grace. The persisted
`next_strip_sequence` remains monotonic.

## 4. Failure and Restart Behavior

- Read, encode, allocation, write, or fsync failure leaves the mirror range
  authoritative and releases tentative EC segments.
- A conflict caused by another append or conversion causes a fresh selection;
  already installed operations are recognized by stable identity.
- Deletion makes subsequent lifecycle operations fail and the tentative
  replacement is discarded.
- Published cleanup intents are already replayed by
  `reconcile_pending_chunks`; restart cannot re-publish the replacement or free
  a referenced EC segment.
- Conversion task failures are isolated per group and counted. Automatic scans
  continue with later chunks.

## 5. Policy and Throttling

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

## 6. Management and Metrics

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

## 7. Scope

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
- `app/crowdb-chunkdb/tests/conversion_test.rs`: lifecycle and conversion
  integration coverage.
- `lib/crowdb-chunkdb-client/tests/conversion_api_test.rs`: real transport API
  coverage.
- `lib/crowdb-chunk-client/tests/small_object_writer_e2e.rs`: full-process
  small-write-to-EC data-path coverage.

## 8. Complexity

High. The codec is already available, but safe conversion crosses mutable
metadata, tentative allocation, real block I/O, durability, ambiguous RPC
outcomes, delayed cleanup, background scheduling, and shutdown. Correctness
depends on never freeing either side of an uncertain publication outcome.

## 9. Test Design

- **Selection policy**: create sealed/active chunks with varied ages, closed
  markers, counts, capacities, and gaps -> select groups -> assert only complete
  immutable equal-geometry runs are returned and tails remain mirrored.
- **24-to-3 conversion**: write 24 one-MiB mirror strips in a real cluster ->
  trigger conversion -> assert three 8+4 EC strips, identical total capacity
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
- **Throttle/concurrency**: use adjustable test parameters and multiple chunks
  -> measure a five-second transfer window and active gauge -> assert bandwidth
  is within one-group burst tolerance and peak workers never exceeds the bound.
- **Management API**: call both client methods through a real RPC server ->
  assert single trigger bypasses policy, batch returns candidate count, and
  malformed filters/errors map without panic.
- **Metrics**: convert 24 one-MiB mirrors -> assert three completions, 24 MiB
  read, 36 MiB written, 72 retired segments, zero active conversions.

## 10. Module Structure

```text
app/crowdb-chunkdb/src/
├── conversion.rs          # service, selection, runner, throttle
├── conversion/
│   └── io.rs              # DiskIO discovery, read, write, fsync
├── lifecycle.rs
└── lifecycle/
    └── handler.rs         # tentative EC allocation/discard
```

## 11. Server Wiring

The server constructs `ConversionDiskIo` only when automatic conversion is
enabled, or lazily for a manual trigger. The runner receives the existing watch
shutdown channel. RPC and HTTP handlers hold `Arc<ConversionService>`; shutdown
stops admission, waits for in-flight groups, then lets normal cleanup
reconciliation handle any published retirement intents.

## 12. Open Questions

None. Fresh EC allocation is required because mirror placement cannot in
general satisfy EC fault-domain constraints, and it keeps rollback independent
of authoritative mirror data.
