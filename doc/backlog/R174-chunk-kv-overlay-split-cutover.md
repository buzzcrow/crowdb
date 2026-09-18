<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R174: chunk-kv — Overlay-backed partition split cutover

## Problem

The current chunk-KV split prepares replacement trees while the parent
continues to accept mutations, but the old final split phase drained parent
admission, replays the final parent journal suffix, checkpoints the replacement
trees, records the artifact, and only then permits catalog publication. It also
models split as deleting the parent and creating two new children. The required
identity model instead retains the parent as one smaller partition and creates
only one child. Moving a complete child checkpoint into preparation reduces
the flush interval, but a high-rate or wide-keyspace workload can dirty most
child pages again before the fence. The final checkpoint can therefore
approach a complete flush.

The 2026-09-17 three-node 1 MiB-target test with 12,000 4 KiB writes at
concurrency 32 exposed the foreground consequence: the third 4,000-operation
round completed with 96 errors and 5.001 s p99 while split preparation and
serving-grant renewal overlapped. A 3.413 s recorded write fence alone does not
describe the user-visible interruption. Preparing must remain an ordinary
serving state, and a split must not make a client wait for a child tree flush.

The permanent design defines one parent journal, a retained-parent range and
one exact child range, an immutable common-cutover artifact, and mutation fencing in
[`design-crowdb-chunk-kv.md`](../design/chunkds/design-crowdb-chunk-kv.md)
section 5. It does not define durable ownership of the child suffix between a
preparation checkpoint and cutover. A memtable alone cannot fill that gap: it
is not restart authority, cannot preserve request-result deduplication by
itself, and cannot assign a unique owner to post-cutover conditional writes.

## Solution

Define split as base-manifest preparation plus a durable child-tail overlay and
one short parent-writer handoff. The only foreground-critical interval freezes
the old sequencer at one exact cutover; it never waits for a child checkpoint
or for background page materialization.

```text
parent serving: parent WAL + parent tree/memtables over [start, end)
       |
       +-- pin exact parent base and prepare one durable child-tail overlay
       |
short writer handoff at sequence C
       |
retained parent serves [start, split) + child serves [split, end)
       |
background parent pruning, child checkpoint, and dependency release
```

1. Extend the permanent chunk-KV, chunk-KV server, chunk-stream, and client
   designs with the child-tail overlay lifecycle, its recovery authority,
   request-result retention, minimum-position semantics, serving grants, stale
   routing, and reclamation pins. `SplitPreparing` is explicitly a serving
   lifecycle for both data admission and serving-grant renewal; it must not
   lower owner health or cause a valid grant to expire.
2. Define the identity transformation explicitly. The parent retains its
   partition ID, tree ID, stream name, owner, and lower range boundary; its
   range becomes `[old_start, split_key)` and its owner epoch advances. Create
   exactly one child for `[split_key, old_end)` with a new partition ID, tree,
   stream, and epoch. One catalog generation updates the parent entry and
   inserts the child entry. It never deletes the parent or creates a second
   replacement for the retained half.
3. Keep the parent as the only mutation sequencer during preparation. It
   continues its normal WAL, tree, and memtable path; preparation must neither
   stop parent flush nor attempt to destructively divide an active parent
   memtable. Build one range-bounded child base tree from one pinned parent
   snapshot, then checkpoint that base manifest while the parent remains
   writable. The retained parent continues using its existing tree and stream.
4. Pin one shared parent journal suffix instead of duplicating it during
   preparation. The child artifact names its base checkpoint `B`, the parent
   stream identity and retained retry floor, and one sealed parent cutover `C`.
   The child recovers or lazily warms its overlay by validating parent records,
   filtering them by its range, and applying records in `(B, C]`; records
   at or below `B` reconstruct retained request results without reapplying the
   tree. The child inherits logical sequence `C` and its own journal begins at
   `C + 1`. Recovery and read-after-write never infer this source relationship
   from volatile memory.
5. Replace the final flush fence with a `SplitCutover` handoff. It atomically
   closes parent sequencer assignment at cutover `C`, drains only requests
   already assigned to that sequencer, verifies the durable child overlay
   covers `C`, and persists an artifact binding the exact child base manifest,
   child tail cursor and stream, request-result floor, retained parent identity
   and next epoch, and `C`. A record cannot be acknowledged as post-cutover
   until the selected retained-parent or child journal has made the result
   recoverable.
6. Install the next-epoch retained-parent writer and the local child writer
   before the handoff. R174 is local; R175 alone may later prepare a remote
   child balance. Selecting a writer handle is one atomic ingress operation. A
   request that already holds the old parent handle drains through `C`; a
   request that has not selected a handle reselects by key and enters either
   the smaller retained parent or the child. No additional post-cutover queue
   is required, and the two new writers cover disjoint ranges.
7. Publish the exact retained-parent and child artifacts through one
   epoch-fenced catalog revision. New routes use the smaller parent entry or
   child entry. A source server receiving an old point route dispatches by key
   to the retained parent or hosted child without a network forward. Old parent
   scan tokens return refresh-required after publication; the client refreshes
   the catalog and replans across both ranges.
8. Reads on the child merge its durable base tree with its active and sealed
   memtables plus a lazily warmed filtered parent suffix. A child accepts a
   parent-stream minimum position at or below `C` by warming through that source
   offset, and accepts a child-stream position through its own applied frontier.
   Post-cutover writes return only child-stream positions. Reads accepted by
   the old parent before catalog publication retain their parent snapshot
   semantics. Conditional writes consult the same child overlay used by reads,
   so a condition is evaluated once by its unique sequencer.
9. Move retained-parent pruning/checkpoint, child checkpoint, pack
   materialization, and obsolete parent history reclamation out of the
   foreground cutover. Each retained child tree version
   explicitly carries its parent-suffix reference. Preserve parent manifests,
   journal prefixes, and request-result records until no retained child version
   references that suffix and the retry floor has expired or been locally
   materialized. A crash or ambiguous catalog outcome resumes or resolves the
   exact persisted transition; it never reconstructs a cutover from
   process-local memtables.
10. Persist an exact-generation pin keyed by child tree and split transition
    before recording child readiness. The value is the child's chunk
    root-catalog generation, not its logical tree snapshot sequence. Root
    reclamation cannot pass the oldest pin. After background materialization,
    publish one catalog generation that clears the child overlay and both split
    markers; only after the owner installs that generation may it delete the
    pin. Pin create/delete are idempotent so crash recovery may repeat them.
11. Expose per-transition preparation duration, base checkpoint duration,
   parent-to-child tail records and bytes, tail replication lag, cutover drain
   duration, post-cutover queue depth, child-overlay apply lag, forwarding
   count, grant-renewal failures, stale-route outcomes, and background
   checkpoint/materialization duration. The benchmark records these metrics
   alongside client p50/p99/p999 and errors while sustained writes overlap
   every observed split.

Edge outcomes are explicit: if the child base manifest or parent journal suffix
is unavailable, the parent remains serving and the unpublished transition
aborts or retries; if a child or journal fails after the parent writer closes,
the exact artifact and catalog head decide whether recovery completes or the
parent remains fenced; a client retry consults the local child result history
then the retained parent history until its retry floor expires; and an owner
never serves an uncommitted child as writable.

## Dependencies

- Depends on the landed chunk-KV split, artifact, epoch, journal recovery, and
  serving-grant baseline described by the chunk-KV and chunk-KV server designs.
- Depends on crowdb-tree's range rebuild, immutable manifest, memtable merge,
  and chunk-page retention behavior. Child overlays must use the existing
  reader-visible tree semantics rather than adding a Rust-side key merge.
- Depends on chunk-stream append/recovery and request-result retention. If the
  existing child stream format cannot encode the selected tail representation,
  extend it before enabling overlay cutover.
- Produces the snapshot, durable-tail replay, writer-handoff, forwarding, and
  recovery contract consumed by R175. R175 may not bypass this contract with a
  separate remote migration protocol.

## Acceptance

- Given a parent receiving sustained puts while a split prepares, when child
  base manifests and durable tails are built, assert parent puts remain
  acknowledged, serving-grant renewal succeeds, and no lease rejection is
  attributed to `SplitPreparing`. Invariant: preparation remains serving. E2E
  test.
- Given a split plan, when it is validated and published, assert the parent ID,
  tree ID, stream name, owner, and lower boundary are preserved, its range ends
  at the split key with an advanced epoch, exactly one new child starts at the
  split key, and their union equals the old range. Invariant: split is retained
  parent plus one child. Unit test.
- Given writes that touch all child pages after the base checkpoint, when
  cutover occurs, assert foreground cutover does not invoke a child checkpoint
  and completes within the configured drain and handoff limit. Invariant:
  foreground latency is independent of dirty child-page volume. Integration
  test.
- Given records before and after the child base checkpoint, including failed
  conditions and duplicate request retries, when the child restarts before
  its next checkpoint, assert recovery reconstructs the same values, source
  order, retained results, and retry outcomes from its durable base plus tail.
  Invariant: a child overlay is restart authority. E2E test.
- Given parent sequence `C` is selected while new requests arrive, when the
  old writer closes, assert requests at or below `C` occur once in the parent
  order and later requests occur once in exactly one child journal order.
  Invariant: cutover has no overlapping writer and no lost acknowledged write.
  E2E test.
- Given requests for both sides arrive during writer handoff, when the
  next-epoch retained-parent and child writers become ready, assert each is
  boundedly queued, durably appended to the range-selected writer, and
  acknowledged without requiring a child checkpoint; when the bound is
  exhausted, assert it receives `Overloaded` without WAL append. Invariant:
  cutover backpressure is bounded and durable. Integration test.
- Given a client uses an old parent route before catalog cache refresh, when a
  post-cutover point read or write arrives during grace, assert the old endpoint
  dispatches it by key to the retained parent or exact child, and no old-epoch
  parent mutation is created. Invariant: stale routing preserves unique writer
  authority. E2E test.
- Given a child position returned after cutover, when a read uses that minimum
  position, assert it waits for and reads the child overlay value; when a parent
  scan token is resumed after publication, assert refresh-required. Invariant:
  ordering and pagination do not cross authority boundaries implicitly.
  Integration test.
- Given a crash during preparation, after old-writer closure, during catalog
  publication, or during forwarding grace, when a replacement owner recovers,
  assert it reaches one exact parent-serving or child-serving outcome from
  durable transition state and never from a volatile memtable. Invariant:
  split resolution is restart-idempotent. E2E test.
- Given continuous routed load through repeated low-target splits, when every
  split overlaps sustained traffic, assert errors are zero, no client deadline
  is exceeded, client percentiles remain within the configured workload bound,
  and each split emits correlated cutover and overlay metrics. Invariant:
  client availability is measured through every split, not only convergence.
  E2E test.

Required gates:

- `pixi run tree-fmt`
- `pixi run tree-lint`
- `pixi run test-tree-ct`
- `pixi run test-tree-ffi`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
- `pixi run -- cargo test -p crowdb-chunk-stream --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-kv --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-kv-client --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-kv-server --all-targets`
- `pixi run clean-env && pixi run test-server`
