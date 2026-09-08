<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Large-Write Error Handling (R110)

This design implements
[R110](../backlog/R110-chunkdb-chunkio-error-handling.md) on the landed
[chunk IO write flow](../design/chunkio/design-crowdb-chunkio.md) and reuses
the placement-safe replacement, durable unavailable-segment, and task
mechanisms defined by
[chunk tasks](../design/chunkdb/design-crowdb-chunkdb-mirror-to-ec.md).
The replacement RPC and generic strip health representation already landed
with the small-write and read-repair paths.

## 1. Scope

- Make every large-write data or parity shard completion repair a failed
  `DiskWriter::write` by replacing only that segment.
- Share one lock-free failed-disk snapshot across large and small writers in a
  `ChunkIoClient`; repeated failures extend the exclusion lifetime.
- Keep successful shards and EC output intact while replacement allocation,
  write, and exact-strip publication complete.
- Retry ambiguous metadata publication with one deterministic operation ID and
  refresh on revision conflicts caused by strip prefetch or another repair.
- Abort and delete the unpublished active chunk when repair is exhausted.
- Add real-service E2E fault coverage for data and parity replacement and
  persistent failure cleanup. Existing mock seams remain auxiliary.

Large-write completion does not publish a degraded object. Unlike an
acknowledged small-write prefix or a read-discovered failure, an active
large-write chunk has no caller-visible location. If redundancy cannot be made
durable before seal, failing and deleting that chunk is both simpler and
stronger than returning a reduced-durability object.

Production `DiskWriter::write` is the durable completion boundary. R110 does
not add a second `fsync` pass; a backend that requires it implements that
durability inside `write`. A failed durable completion enters the same
replacement path regardless of whether its underlying cause was write or
flush.

## 2. Client-Wide Failed Disk State

`FailedDiskList` remains an `ArcSwap` snapshot: reads clone no map and take no
lock. Each value records an expiry and consecutive-failure generation.

```rust
pub struct FailedDiskList { /* ArcSwap<HashMap<DiskId, FailedDisk>> */ }
pub fn insert(&self, disk_id: DiskId);
pub fn live(&self) -> Vec<DiskId>;
```

The first failure uses the configured base TTL. Each live repeated failure
doubles it, capped so `Instant` arithmetic cannot overflow. An expired entry
starts again at the base TTL. `ChunkIoClient` constructs one list from
`SmallWritePolicy::failed_disk_ttl` and passes the same `Arc` to its small pool
and every prepared large write.

## 3. Per-Segment Durable Completion

`EcStripWriter` creates one `SegmentWrite` future per data shard. The parity
writer creates the same future per parity shard. Each future retains only the
payload for its own shard plus shared allocator, writer, failed-disk list, and
retry policy.

On successful initial write it returns immediately. On failure it:

1. Inserts the failed disk into the client list.
2. Queries current chunk metadata and finds the strip by stable
   `strip_sequence` and the failed segment by exact identity.
3. Requests one placement-safe tentative segment using all other current strip
   segments as survivors and the client-wide live exclusions.
4. Writes the retained shard payload to the tentative segment.
5. Builds a geometry-identical strip with only the failed segment replaced.
6. Publishes it with `replace_chunk_strip_range`, exact old strip, current
   revision, and a deterministic operation ID.

Concurrent strip prefetch or another segment replacement can advance the
revision. A definite state conflict discards the still-tentative segment,
re-queries, and retries only if the target old segment is still present. If the
same operation is already installed, the idempotent ChunkDB response completes
it. An ambiguous transport result retries the identical request and never
allocates or discards another segment until its outcome is resolved.

Replacement-write failure excludes that new disk, discards the tentative
segment, and consumes one bounded attempt. Exhaustion returns the original
write failure with replacement context. `ChunkWriter::seal` observes the
failed completion before `seal_chunk`; its caller aborts and deletes the
unsealed chunk. Already sealed earlier chunks remain in the returned internal
location list, but the public prepared-write API returns an error rather than a
partially successful object.

ChunkDB derives the failure-domain limit from the current strip. Mirror
replacement excludes every survivor node. EC replacement allows at most
`code_num` shards per node when the topology can satisfy safe placement; an
explicitly unsafe EC layout keeps its balanced per-node ceiling. All old and
surviving disk IDs are excluded in either case, so replacement never
co-locates two strip shards on one disk.

## 4. Failure and Crash Behavior

- Crash before replacement allocation: the original active chunk remains
  unsealed and is eligible for existing orphan cleanup.
- Crash after tentative allocation or replacement write: DiskDB owns an
  uncommitted tentative segment, reclaimed by its existing cleanup path.
- Crash after ChunkDB publication: the replacement segment is committed and
  the old segment is protected by a delayed cleanup intent.
- Error before seal: no object location is published; normal abort deletes the
  active chunk after outstanding submitted writes drain. Completion handles
  remain owned after the first failure so no late write can race segment reuse.
- Error after an earlier chunk rotated and sealed: the whole object call still
  fails. Sealed prefix chunks remain unreachable by the caller and require the
  existing orphan lifecycle policy; R110 does not invent partial-object
  success.

## 5. Configuration and Metrics

`ChunkClientConfig::large_write_repair_attempts` defaults to three and must be
nonzero. The failed-disk base TTL remains client-wide through
`SmallWritePolicy::failed_disk_ttl` until a separate top-level client policy is
introduced.

Lock-free chunk-client metrics add large-write repair attempts, successful
segment replacements, exhausted replacements, failed-disk exclusions, and
discarded tentative replacements. Benchmark output can expose these counters
without changing write correctness.

## 6. Test Design

- Failed-disk expiry/backoff unit test: insert one disk repeatedly before
  expiry, advance a test clock, and assert base/double/quadruple exclusion then
  reset after expiry. Invariant: transient quarantine is bounded and repeated
  failure extends it.
- Data segment integration test: fail one data write, run a 4+1 strip, and
  assert only the failed segment is allocated/written/replaced while the four
  successful identities remain. Invariant: no whole-strip retry.
- Parity segment integration test: fail one parity write and assert the same
  single-segment replacement path completes before seal. Invariant: data and
  parity durability use one policy.
- Persistent failure integration test: fail original and every replacement,
  assert bounded attempts, write error, active chunk deletion, and no seal.
  Invariant: exhausted redundancy is never acknowledged.
- Revision-race integration test: advance the chunk revision between
  replacement write and publication, assert refresh/retry installs one
  replacement and discards superseded tentative allocation. Invariant:
  metadata races neither leak nor overwrite unrelated changes.
- Real-process E2E: write a multi-strip object through KV, DiskDB, DiskIO, and
  ChunkDB with one selected data failure and one selected parity failure;
  query metadata and read all bytes back, assert replaced identities and valid
  parity. Invariant: recovery works through the production service boundary.
- Real-process exhaustion E2E: inject persistent client-side DiskIO failure,
  assert the object fails and its active chunk becomes deleted with no returned
  location. Invariant: complete service cleanup follows failure.

## 7. Module Structure

```text
app/crowdb-chunkdb/src/lifecycle/handler.rs
                                            strip-aware replacement placement
lib/crowdb-chunk-client/
├── src/client.rs                          shared failed-disk ownership and wiring
├── src/config.rs                          bounded large-write repair policy
├── src/negative_list.rs                   expiry and repeated-failure backoff
├── src/chunk/segment_writer.rs             one-shard write/replace/publication
├── src/chunk/ec_strip_writer.rs            data completion construction
├── src/chunk/parity_writer.rs              parity completion construction
├── src/chunk/chunk_writer.rs               seal/abort propagation
├── src/metrics.rs                          lock-free repair counters
├── tests/negative_list_test.rs             deterministic expiry/backoff
└── tests/large_object_writer_e2e.rs        real-process failure coverage
```

## 8. Complexity

Medium. The placement and fenced replacement protocols already exist. The
main difficulty is resolving concurrent revision changes and ambiguous
publication without losing a tentative segment or replacing the wrong strip.

## 9. Open Questions

None. Landed small-write/read work resolves replacement placement, generic
strip health, task representation, and per-client negative-list scope. The
current durable-write contract resolves the obsolete separate-fsync question.
