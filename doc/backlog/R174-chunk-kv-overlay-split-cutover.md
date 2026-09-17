<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R174: chunk-kv — Overlay-backed partition split cutover

## Problem

The current chunk-KV split prepares child trees while the parent continues to
accept mutations, but the final `SplitFenced` phase drains parent admission,
replays the final parent journal suffix, checkpoints both children, records the
artifact, and only then permits catalog publication. Moving the first complete
child checkpoint into preparation reduces this interval, but a high-rate or
wide-keyspace workload can dirty most child pages again before the fence. The
final checkpoint can therefore approach a complete flush.

The 2026-09-17 three-node 1 MiB-target test with 12,000 4 KiB writes at
concurrency 32 exposed the foreground consequence: the third 4,000-operation
round completed with 96 errors and 5.001 s p99 while split preparation and
serving-grant renewal overlapped. A 3.413 s recorded write fence alone does not
describe the user-visible interruption. Preparing must remain an ordinary
serving state, and a split must not make a client wait for a child tree flush.

The permanent design currently defines one parent journal, two exact child
ranges, an immutable common-cutover artifact, and mutation fencing in
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
parent serving: parent WAL + parent tree/memtables
       |
       +-- prepare base manifests and durable child-tail overlays
       |
short writer handoff at sequence C
       |
children serving: child base manifest + child WAL/memtable overlay
       |
background checkpoint and parent retirement after stale-route grace
```

1. Extend the permanent chunk-KV, chunk-KV server, chunk-stream, and client
   designs with the child-tail overlay lifecycle, its recovery authority,
   request-result retention, minimum-position semantics, serving grants, stale
   routing, and reclamation pins. `SplitPreparing` is explicitly a serving
   lifecycle for both data admission and serving-grant renewal; it must not
   lower owner health or cause a valid grant to expire.
2. Keep the parent as the only mutation sequencer during preparation. It
   continues its normal WAL, tree, and memtable path; preparation must neither
   stop parent flush nor attempt to destructively divide an active parent
   memtable. Build range-bounded child base trees from one pinned parent
   snapshot, then checkpoint those base manifests while the parent remains
   writable.
3. Add durable child-tail replication. For every parent record after each
   child base checkpoint, persist enough child-owned information to rebuild
   that child's filtered state, including successful and condition-failed
   results, request identity/digest, source sequence, and source journal
   position. A child overlay may advance a common source sequence through
   no-ops, or use an explicit source-to-child cursor mapping, but recovery and
   read-after-write must not infer ordering from volatile memory. The plan must
   select one representation and reject mixed representations for one
   transition.
4. Replace the final flush fence with a `SplitCutover` handoff. It atomically
   closes parent sequencer assignment at cutover `C`, drains only requests
   already assigned to that sequencer, verifies both durable child overlays
   cover `C`, and persists an artifact binding the exact base manifests, child
   tail cursors, child stream identities, request-result floors, parent ID and
   epoch, and `C`. A record cannot be acknowledged as post-cutover until its
   selected child journal has made the result recoverable.
5. Install child writers before or as part of the handoff. Requests arriving
   after the parent assignment closes enter bounded per-child post-cutover
   queues and are ordered only by their selected child sequencer; they append
   to that child journal and apply to its active memtable before acknowledgement.
   This queue is bounded by existing admission budgets and returns `Overloaded`
   rather than retaining unbounded foreground memory. Parent and child writers
   must never both order mutations for the same child range.
6. Publish the exact child artifact through one epoch-fenced catalog revision.
   New routes use child owners and child positions. Old endpoints retain a
   finite stale-route grace: they serve reads from the authoritative parent
   view before cutover or forward post-cutover point operations to the selected
   child without creating a second write authority. They return a current owner
   hint once forwarding expires. Parent scan tokens never silently cross into a
   child range and return refresh-required after publication.
7. Reads on a child merge its durable base tree with its active and sealed
   memtables, including the durable tail overlay. A read carrying a returned
   post-cutover child position waits only for that child applied frontier.
   Reads accepted by the old parent before catalog publication retain their
   parent snapshot semantics. Conditional writes consult the same child overlay
   used by reads, so a condition is evaluated once by its unique sequencer.
8. Move child checkpoint, pack materialization, parent-journal copying, and
   parent tree/stream reclamation out of the foreground cutover. Preserve all
   parent manifests, parent journal prefixes, child base manifests, child
   tails, and request-result records until the catalog decision, stale-route
   grace, retry floors, and recovery pins prove them unreachable. A crash or
   ambiguous catalog outcome resumes or resolves the exact persisted transition;
   it never reconstructs a cutover from process-local memtables.
9. Expose per-transition preparation duration, base checkpoint duration,
   parent-to-child tail records and bytes, tail replication lag, cutover drain
   duration, post-cutover queue depth, child-overlay apply lag, forwarding
   count, grant-renewal failures, stale-route outcomes, and background
   checkpoint/materialization duration. The benchmark records these metrics
   alongside client p50/p99/p999 and errors while sustained writes overlap
   every observed split.

Edge outcomes are explicit: if child-tail replication cannot remain within
record, byte, or estimated-time budgets, the parent remains serving and the
unpublished transition aborts or retries; if a child or journal fails after
the parent writer closes, the exact artifact and catalog head decide whether
recovery completes or the parent remains fenced; a client retry preserves its
request identity across parent forwarding and child ownership; and an owner
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
- Given writes that touch all child pages after their base checkpoints, when
  cutover occurs, assert foreground cutover does not invoke a child checkpoint
  and completes within the configured drain and handoff limit. Invariant:
  foreground latency is independent of dirty child-page volume. Integration
  test.
- Given records before and after each child base checkpoint, including failed
  conditions and duplicate request retries, when either child restarts before
  its next checkpoint, assert recovery reconstructs the same values, source
  order, retained results, and retry outcomes from its durable base plus tail.
  Invariant: a child overlay is restart authority. E2E test.
- Given parent sequence `C` is selected while new requests arrive, when the
  old writer closes, assert requests at or below `C` occur once in the parent
  order and later requests occur once in exactly one child journal order.
  Invariant: cutover has no overlapping writer and no lost acknowledged write.
  E2E test.
- Given a request arrives during writer handoff, when its selected child writer
  becomes ready, assert it is boundedly queued, durably appended to that child,
  and acknowledged without requiring a child checkpoint; when the bound is
  exhausted, assert it receives `Overloaded` without WAL append. Invariant:
  cutover backpressure is bounded and durable. Integration test.
- Given a client uses an old parent route before catalog cache refresh, when a
  post-cutover point read or write arrives during grace, assert the old endpoint
  forwards it to the exact child or returns a current owner hint, and no second
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

## Open Questions

- Should the first release persist a filtered record in each child stream, or
  retain an explicitly range-filtered parent-stream suffix until background
  copying completes? Child streams make recovery ownership local but duplicate
  tail bytes; a shared suffix reduces write amplification but couples child
  recovery and reclamation to the parent stream.
- Should stale-route grace forward only point reads/writes, or also support
  bounded scans? Point forwarding keeps cursor ownership simple; scan forwarding
  improves old-client continuity but requires a cross-range continuation rule.
- What workload-specific cutover, post-cutover queue, and client-tail-latency
  limits should become admission policy? Tight limits protect foreground traffic
  but can defer splits under sustained overload; relaxed limits improve eventual
  balance but raise tail latency.
