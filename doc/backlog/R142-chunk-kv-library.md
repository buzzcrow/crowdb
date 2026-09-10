<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R142: chunk-kv — Range-partitioned KV library

## Problem

crowdb-kv couples each tree to a Paxos group and local tree/WAL persistence.
That is appropriate for its replicated KV service but does not provide the
shared-storage model needed by a scale-out object metadata index. In the new
model, partition count must be independent of node count: one node may manage
many ranges, a new node may take ownership of existing ranges, and a large
range may split without copying its durable chunks to the new owner.

The intended workload writes object metadata under an object name and later
looks it up by that name. The values are metadata, not necessarily large
objects, so `large-kv` would describe the workload poorly. There is currently
no reusable library that combines crowdb-tree's ordered API, chunk-backed tree
pages, range ownership epochs, split/rebuild, and a chunk-stream WAL.

The existing `crowdb-kv::KVEngine` is not the right library boundary. Its
logical slot, no-op, snapshot, and replay contracts serve the Paxos learner,
and `CrowdbTreeEngine` currently opens only the ordinary tree FFI build. The
current tree FFI snapshot view materializes all entries into Rust-owned
vectors, while portable snapshot export first builds a whole byte stream.
Using either path for partition split would bypass R140's page-reference reuse
and make split memory proportional to the source tree.

Split also needs a durable cutover protocol. A child built from a pinned tree
manifest is stale if the parent continues accepting writes, while fencing the
parent for the complete rebuild can cause an unbounded write outage. Retrying
or recovering a partially completed split must not publish one child, lose a
committed parent WAL record, reclaim reused chunks, or let both parent and
children accept writes.

The existing network KV interface is also shaped by Paxos. Conditional writes
are deferred because replicas can apply slots out of order, and the current
tree/FFI scan is forward-only even though its page views already provide
`lower_bound`. A single epoch-fenced range owner with one ordered journal can
evaluate richer single-key conditions deterministically, but acknowledged
condition failures must be journaled for stable retry results. Reverse seek and
scan still require new C++ L0/L1 cursor support; removing Paxos does not create
that data-structure capability automatically.

Treating every stream or tree error as a reason to close and rebuild the whole
partition would turn transient mirror, checkpoint, or backpressure failures
into unnecessary outages. The library needs explicit durable, applied, and
checkpoint frontiers plus typed error containment so it can retry or degrade
only the failed layer without ever exposing unjournaled state.

The root design links are `doc/design/tree/design-crowdb-tree.md`,
`doc/design/tree/design-crowdb-tree-storage.md`, and
`doc/design/kv/design-crowdb-kv-state-machine.md`.

## Solution

Create `crowdb-chunk-kv`, an embeddable range-partitioned KV library backed by
R140's runtime-selected C++ chunk backend and an R141 chunk-stream WAL. Support
online split preparation with a replay-budget-gated final write fence.
Partition merge is specified separately by R144.

1. Write a permanent design under `doc/design/kv/` defining partition identity,
   bytewise half-open range bounds, ownership epochs, logical mutation
   sequence, lifecycle states, write acknowledgement, checkpoint/replay,
   split preparation and resolution, transfer, retention, and failure
   behavior.
2. Add `lib/crowdb-chunk-kv` without depending on `crowdb-kv`. Its manager can
   open, close, and recover multiple independent partitions in one process.
   Each partition owns one crowdb-tree instance created with a chunk backend
   handle and R140's immutable `Bounded(start, end)` range policy, one active
   R141 stream handle, one published tree manifest, one ownership epoch, and
   one logical mutation sequence. R142 creates one distinct `stream_name` for
   every partition tree. The durable stream identity is part of the
   partition artifact and survives process and node transfer. The partition
   lifecycle task is the only production owner allowed to create/open, append,
   replay, advance the checkpoint watermark, reclaim, or close that handle.
   R142 selects R140's private native C++ chunk backend at tree creation;
   backend choice is not a Cargo feature
   and Rust never performs tree page reads or writes through
   `crowdb-chunk-client`.
3. Model a partition as `Closed`, `Recovering`, `WriteStalled`, `Prepared`,
   `Serving`, `SplitPreparing`, `SplitFenced`, `Retired`, or `Faulted`.
   `WriteStalled` rejects or queues mutations within a bound while continuing
   reads from a healthy applied prefix. `Recovering` rejects all data
   operations because tree integrity is not established. `Prepared` opens
   and validates a transferred or child artifact but cannot serve until an
   exact catalog proof arrives. A single lifecycle task performs transitions.
   Request admission reads an immutable state/epoch snapshot and takes a
   bounded in-flight guard without adding a lifecycle lock. Existing tree
   synchronization is unchanged; get, scan, and page resolution remain
   lock-free. Close, transfer, and split first stop new mutation admission and
   then drain operations admitted under the old state before advancing it.
4. Define a chunk-KV-native interface rather than copying the Paxos server
   surface. Support point get, put, delete, `put_if_absent`, compare-exchange by
   revision or value, and conditional delete. Support bounded forward and
   reverse scans plus `ceiling`/`higher`/`floor`/`lower` seeks with explicit
   inclusive or exclusive keys. Implement ordered lookup in the C++ tree so it
   resolves L0 and L1 under one view; do not emulate it in Rust with a racy
   `get` followed by `scan`. Keep multi-key conditional transactions out of the
   first version.
5. Use arbitrary binary keys with the same unsigned bytewise ordering as
   crowdb-tree. Every operation validates the requested key or scan interval
   against the partition's `[start, end)` bounds and the caller's ownership
   epoch. A scan is clipped to one partition; cross-partition scans and
   canonical object-name encoding remain server/client concerns. Every
   mutation belongs to exactly one child during split.
6. Assign every admitted mutation a monotonically increasing partition-local
   `mutation_seq`, distinct from the chunk-stream's physical position. A
   bounded, single-writer partition sequencer establishes the semantic order
   when concurrent requests enter its journal-admission queue. It evaluates a
   bounded group-commit batch in that order against the tree's applied prefix
   plus a sequencer-owned staged overlay for earlier operations in the same
   batch, then submits
   `(partition_id, ownership_epoch, mutation_seq, request_id, resolved_result,
   operation)` as a framed record to R141 in mutation order without adding a
   request-path lock. R141 preserves frame order and assigns logical offsets.
   Define `request_id` as `(client_instance_id[128], client_sequence[64])`;
   neither field is derived from a connection or endpoint.
   Journal both success and condition-failed outcomes. Do not mutate crowdb-tree
   before R141 returns `{stream_name, begin, end}` durable. Then apply successful
   records to the tree and advance condition-failed records as no-ops in
   `mutation_seq` order. Acknowledge only after their applied frontier advances,
   returning `{stream_name, begin}`. A default read
   bypasses the sequencer and sees only the current applied prefix; it may
   therefore return the old value while a concurrent mutation is journal-pending,
   which is a valid linearization before that mutation. An optional
   `min_journal_position` read waits asynchronously for that position to apply,
   providing explicit read-after-write ordering without putting all reads
   through the write sequencer. Persist a canonical logical-operation digest
   with each `request_id`; exclude routing revision, owner endpoint, and owner
   epoch so the same operation can retry after transfer or split. A retry with
   the same ID and digest returns the journaled result and never re-evaluates an
   already durable condition; the same ID with a different digest returns
   `RequestConflict` without execution.
7. Define a checkpoint as an atomically published tuple of tree manifest,
   applied `mutation_seq`, `stream_name`, and R141 logical replay offset. Page
   checkpointing is asynchronous. Recovery opens the manifest through R140,
   replays later R141 bytes in logical-offset order, decodes complete WAL
   records, rejects epoch or
   sequence regression, treats an identical replay as idempotent, and fails on
   conflicting duplicate sequences. Maintain
   `checkpoint_seq <= applied_seq <= journal_durable_seq`. Prefix GC cannot pass
   the oldest live checkpoint, transfer or split pin, or bounded request-result
   retention window. A retry older than the declared retention floor returns
   `RequestExpired` and is never executed as a new request.
8. Use containment-first error handling, with whole-tree reopen as the final
   integrity fallback:

   - Reject invalid range, malformed operation, oversized key/value, stale
     epoch, and admission backpressure before journaling. Journal a failed CAS
     as the resolved normal result required for deterministic retry. Neither
     kind changes partition health.
   - Let R141 resolve and retry definite or ambiguous mirror/cursor failures.
     While journal order is unresolved, enter `WriteStalled`; keep reads on the
     healthy applied prefix and submit no later mutation. If the batch is
     proven absent, retry it at the same queue position; if durable, apply it.
   - Prevalidate range, encoding, and key/value limits and reserve the bounded
     R142 request and WAL-frame memory before journaling. Current crowdb-tree
     validates a batch before mutation, performs no IO while installing it in
     L0, and is highest-slot-wins. After this admission, successful apply is the
     normal path and replaying the same `mutation_seq` is idempotent. The
     current API exposes no ordinary retryable apply status: a non-OK result
     after prevalidation, an exception boundary, or otherwise indeterminate
     apply health is an integrity event and enters `Recovering` rather than
     continuing on an assumed-good tree.
   - Apply each successful single-key WAL record through a distinct tree call
     in sequence order, even when R141 group-commits their frames. Add an
     exception-safe C++/C ABI boundary so no C++ exception crosses into Rust.
     Convert an exception or unknown completion into a typed
     `ApplyStateUnknown`, latch only that tree handle unhealthy, and let R142
     recover only its partition from the already durable record. Never catch an
     unknown apply and continue serving from that handle.
   - Treat checkpoint, flush-to-chunk, compaction, and GC failures as
     maintenance degradation. The prior manifest and journal remain
     authoritative; retain WAL, retry maintenance, and apply mutation
     backpressure only if the configured L0/memory bound is reached.
   - Let R140 perform mirror fallback, layout refresh, and bounded retry for a
     page-read IO error. If it returns a definite availability error without
     checksum or structural corruption, fail that read and mark the partition
     degraded; do not discard the healthy resident tree or stop mutations that
     do not need the failed page.
   - Only `Corruption`, a structural/internal invariant violation, conflicting
     replay, or an apply outcome that cannot prove the tree state enters
     `Recovering`. Stop all partition admission, drain outstanding tree calls,
     close the tree handle, open the newest valid published root (falling back
     to its prior complete generation), and replay R141 from the checkpoint's
     logical offset through the durable tail. Replay uses each WAL frame's
     recorded conditional result and never re-evaluates CAS. Resume only after
     tree validation and `applied_seq == journal_durable_seq`; repeated failure
     enters `Faulted` for R143 to withdraw ownership.

   Use typed R140/R141 error categories rather than matching error strings.
   No path performs speculative tree mutation, in-memory rollback, or a
   compensating journal write.
9. Start a split only from a caller-supplied, idempotent plan containing
   `transition_id`, parent identity/range/epoch, split key, and the two child
   identities/ranges/epochs. Reject a stale epoch, invalid interior split key,
   changed plan under the same transition ID, or a second concurrent lifecycle
   operation. R142 validates and prepares artifacts; R143 owns the durable
   group-0 transition and atomic range-map publication.
10. During `SplitPreparing`, flush and publish a parent base checkpoint at
   mutation sequence `b`, pin its manifest and WAL prefix, and invoke R140 in
   parallel to rebuild `[start, split_key)` and `[split_key, end)` from that
   exact manifest. Parent reads and mutations continue while the immutable
   child bases are built. Then replay parent WAL deltas into both children while
   the parent remains serving: apply a matching record to one child and a no-op
   at the same sequence to the other. Copy the request-result identity and
   logical-operation digest only to the child containing the operation key; the
   sequence-only no-op in the other child does not claim that request ID. The
   workers are bounded by the manager's shared I/O and memory admission budgets.
11. Enter `SplitFenced` only after both child bases validate and their replay lag
   is below configured record, byte, and estimated-time limits. If catch-up
   cannot reach those limits, keep the parent serving and cancel or retry
   preparation without imposing a write outage. Prewarm assigned child owners'
   backend connections and base artifacts before fencing. Once fenced, reject
   new parent mutations with a retryable split error, drain all operations
   admitted before the fence, and replay the bounded remainder through a
   cutover sequence `c`. Persist and validate both child manifests at `c`,
   create distinct empty child streams whose next mutation sequence is
   `c + 1`, and return one immutable split artifact. Base rebuild and bulk catch-up
   are outside the write outage; its data-plane work is limited to drain, final
   delta replay, and child checkpointing.
12. Keep the parent authoritative until the caller resolves the split artifact.
    `commit_split` accepts proof that group 0 atomically replaced the parent
    with both exact children, rejects an unexpected child epoch or manifest,
    retires the parent, and releases pins according to the published
    references. Before publication, assigned child owners may open the exact
    artifacts as `Prepared` so backend connections, manifests, and recovery are
    already validated, but they cannot serve. Commit proof transitions a
    matching prepared child to `Serving`; a mismatch fails closed. After
    commit, schedule R140's bounded mapping materialization and shared-pack
    repack for both children. Cleanup does not delay serving and retries from
    the last published child manifests.
    `abort_split` is allowed only after proof that the transition was not
    published; it discards unpublished child artifacts and resumes the parent
    under the same epoch. An ambiguous publication result keeps the parent
    fenced until the caller reads the authoritative catalog and supplies commit
    or abort resolution.
13. Implement ownership transfer by fencing and draining the old epoch,
    publishing a final checkpoint, and reopening the same manifest and stream
    under a higher epoch in another process. Existing page and WAL chunks are
    referenced in place. A stale owner cannot append, checkpoint, resolve a
    split, or resume service after the epoch advances.
14. Expose partition-level metrics for operations, conditional outcomes,
    ordered seeks and scan direction, mutation and WAL positions,
    checkpoint/recovery, lifecycle states, split base-build, serving catch-up,
    replay lag, fenced-catch-up time, delta records, page reuse, abandoned
    artifacts, range rejects, stale epochs, chunk pins, journal/applied/checkpoint
    frontier gaps, stalled writes, degraded maintenance, localized IO errors,
    recovery cause/replay/time, faults, and admission backpressure.

Edge outcomes are explicit: keys at the exclusive end are rejected; an empty
partition may exist but a split key must be strictly inside its non-empty
parent range; writes committed before the split fence enter exactly one child;
writes arriving after the fence receive a retryable error; and a failed or
unresolved cutover publishes neither a partial child set nor an unfenced
parent. The fence affects mutations only for the source range: reads from its
authoritative snapshot and operations on other partitions continue. If catalog
resolution is unavailable after fencing, source-range mutations remain paused
rather than risk dual ownership. The normal pause also includes final child
`Prepared` confirmation and group-0 activation, so replay limits bound the
data-plane portion but cannot guarantee a hard end-to-end limit during target
or control-plane failure.

Outside a lifecycle fence, a journal-pending mutation is invisible to ordinary
reads. A read concurrent with it may return the old committed value; a read
carrying its returned journal position cannot begin until the mutation has
already been applied because that position is returned only after apply. A CAS
ordered behind pending mutations observes the sequencer's staged state, not a
stale tree-only read. Normal condition failure is durable data, not a tree
error. Localized IO and maintenance errors preserve safe service wherever the
three frontiers prove it; only loss of tree-state trust triggers full replay.

## Dependencies

- Depends on R140 for the runtime backend handle, native asynchronous chunk
  page backend, generation-addressed manifests, pinned-reference retention,
  and bounded range rebuild. R142 creates a chunk store when it opens the tree,
  calls the range rebuild API directly, and does not use the materializing Rust
  snapshot view/export path for split. Split selects one durable base manifest;
  children may initially share its immutable mapping-segment images and page
  packs, while older parent snapshots remain in the parent lineage and are not
  rebuilt.
- Depends on R141 for production byte append/read, logical offsets, writer
  fencing, stream creation, metadata-group binding, and prefix GC. R142 owns
  WAL framing, record checksum, and idempotent request identity. The WAL payload
  carries R142's logical `mutation_seq`; R141 logical byte offsets are not
  reused as tree apply sequences.
- R143 supplies plans, ownership epochs, and commit/abort proofs only through
  R142's partition lifecycle API. It does not receive a raw stream handle or
  call R141 data-plane and GC operations directly.
- R143 owns group-0 records, leases, routing, split-plan creation, atomic
  parent-to-children publication, and authoritative commit/abort proof. R142
  accepts typed plans and resolutions and remains usable with an in-memory
  catalog in tests. Existing group-0 sysdata supports blind puts, not general
  compare-and-swap, so R143 must publish one self-contained, epoch-fenced
  range-map activation record rather than ask R142 to coordinate multiple
  group-0 keys.
- Depends on the C++ engine and FFI portion of R52 for reverse cursor support.
  Ceiling and higher seek extend the existing `lower_bound`/forward cursor;
  floor, lower, and reverse scan require a real reverse L0/L1 cursor rather
  than client-side sorting.
- R144 consumes R142's lifecycle, checkpoint, sequence, and catalog-proof
  boundaries to add partition merge. R142 does not expose a merge operation.
- Existing crowdb-kv remains on its Paxos/local-WAL architecture. R142 does not
  implement `crowdb-kv::KVEngine`, import its Paxos slot types, or change
  crowdb-kv-server's tree build. It may reuse protocol-neutral operation/value
  types only after they are moved to a lower-level crate without pulling in
  crowdb-kv.

## Acceptance

- Given the normal crowdb-kv-server and chunk-kv-server binaries, when their
  tree creation options and native link graphs are inspected, assert the first
  selects a local backend and contains no chunk backend symbols, while the
  second selects R140's chunk handle from the same static tree archive; neither
  tree page path links a Rust chunk client. Invariant: storage is selected per
  tree without a backend feature or dynamic library. Integration test.
- Given two partitions hosted by one manager, when independent reads and writes
  execute, assert each tree, stream, mutation sequence, epoch, and lifecycle
  advances independently. Invariant: a node can manage multiple partitions
  without shared ordering. Integration test.
- Given two partition trees are created, when their first mutations are
  journaled, assert R142 creates two distinct stream names and each mutation
  result carries its partition's stream name and durable logical offset.
  Invariant: every partition tree has exactly one independently addressable WAL
  stream. Integration test.
- Given a partition handle is serving, when its server attempts to bypass R142
  and access the raw R141 handle, assert no production API exposes that handle;
  after transfer, assert the new R142 partition handle opens the same durable
  stream identity under the higher epoch. Invariant: WAL mechanics are
  partition-owned while server authority is replaceable. Unit test.
- Given more partitions than nodes and a deterministic assignment, when all
  partitions open, assert no library limit couples partition count to process
  count. Invariant: partition and node cardinalities are independent. Unit
  test.
- Given a key below, inside, and at the exclusive end of `[a, m)`, when get,
  put, delete, and scan execute, assert only in-range work proceeds and scans
  cannot return a key outside the partition. Invariant: every data path enforces
  bytewise half-open ownership bounds. Unit test.
- Given current revision `r` and concurrent conditional mutations for one key,
  when compare-exchange, put-if-absent, or conditional delete reaches the
  partition sequencer, assert conditions observe the applied tree plus preceding
  staged mutations in the same group-commit batch and only the matching
  mutation changes the tree. Invariant: conditional mutation is linearizable
  within one ownership epoch even before earlier batch entries reach L0.
  Integration test.
- Given a put is journal-pending but not applied, when an ordinary concurrent
  read executes, assert it returns the prior applied value; when the put becomes
  durable and applied, assert its response carries a journal position and a
  read constrained to that position returns the new value. Invariant: pending
  state is never speculative, and explicit read-after-write waits on the
  applied frontier. Integration test.
- Given an acknowledged condition-failed result whose response is lost, when
  the same request ID is retried after intervening writes or recovery, assert
  the journaled original failure and observed revision are returned. Invariant:
  retry does not re-evaluate an already resolved condition. Integration test.
- Given a durable request ID is reused with a different key, operation, value,
  or condition, when admission compares its canonical logical-operation digest,
  assert it returns `RequestConflict` without WAL append or tree mutation.
  Invariant: request identity cannot alias two logical mutations. Unit test.
- Given resident and cold L0/L1 keys around target `k`, when ceiling, higher,
  floor, lower, forward scan, and reverse scan execute, assert each returns the
  mathematically nearest in-range key in the requested direction without
  client-side sorting. Invariant: ordered lookup merges L0 and L1 under one
  consistent tree view. Integration test.
- Given concurrent mutation admission while a lifecycle fence advances, when
  accepted operations drain, assert every operation belongs wholly before or
  after the fence and ThreadSanitizer reports no race. Invariant: lifecycle
  admission adds no lock and has a precise drain point without changing the
  tree's existing synchronization. Integration test.
- Given a journal batch has a definite or ambiguous write error while the tree
  remains healthy, when R141 resolves it, assert later mutations stop, reads
  continue from the applied prefix, and the batch is either retried at the same
  order position or applied from its proven durable range. Invariant: journal
  uncertainty stalls writes without forcing tree recovery or creating a gap.
  Integration test.
- Given a request has a deterministic range, encoding, or size error, when it
  reaches admission, assert it is rejected before WAL append; given a durable,
  prevalidated frame, assert normal tree apply performs no chunk IO. Inject a
  non-OK or indeterminate post-journal apply outcome and assert the partition
  enters `Recovering` instead of retrying while serving. Invariant: routine
  request errors never require recovery, while an untrusted apply result is
  never hidden by an invented retryable status. Unit and integration tests.
- Given an allocation or other C++ exception is injected before, during, or
  after a single-record tree apply, when it reaches the C ABI boundary, assert
  no exception crosses into Rust, only that tree handle is latched unhealthy,
  the result is `ApplyStateUnknown`, and no later record applies before
  partition recovery replays the durable sequence. Invariant: an uncertain
  in-memory apply cannot crash unrelated partitions or remain visible through
  an assumed-healthy handle. Integration test.
- Given checkpoint, compaction, or page-pack GC fails before publication, when
  foreground traffic continues, assert the old manifest and WAL remain pinned,
  reads and writes continue until the declared memory bound, and maintenance
  retries without reopening the tree. Invariant: an unpublished maintenance
  failure cannot invalidate serving state. Integration test.
- Given a cold read exhausts mirror retries with a typed availability error but
  no checksum or structure failure, when another resident key is accessed,
  assert only the failed read reports unavailable and the partition remains
  serving in degraded state; repeat with corruption and assert all admission
  enters `Recovering`. Invariant: localized availability loss and loss of tree
  integrity have different blast radii. Integration test.
- Given a durable journal suffix and an injected tree corruption or
  indeterminate apply outcome, when recovery runs, assert admission closes,
  R140 opens the newest valid root or its complete fallback, and R142 replays
  recorded outcomes through the durable logical tail without re-evaluating CAS;
  assert service resumes only when applied and durable frontiers match.
  Invariant: full reopen is reserved for loss of state trust and reconstructs
  exactly one journal-defined state. E2E test.
- Given a lost response and the same request ID is retried, when append and
  apply complete, assert one mutation sequence, one WAL record, and one logical
  result exist. Invariant: client retry cannot duplicate a mutation. Unit test.
- Given concurrent writes receive consecutive mutation sequences and append at
  sequence `n` fails, when the sequencer handles later queued writes, assert
  physical WAL order matches mutation order and no record above `n` is
  published before recovery. Invariant: a partition WAL never exposes a
  mutation-sequence gap. Integration test.
- Given a checkpoint at mutation sequence `p` and later WAL records including
  an identical retry and a conflicting duplicate sequence, when the partition
  recovers, assert the retry is idempotent and the conflict faults recovery.
  Invariant: manifest and WAL form one ordered recovery boundary. Integration
  test.
- Given epoch 4 is replaced by epoch 5, when the old owner attempts a mutation,
  checkpoint, split resolution, or reopen, assert it is rejected before durable
  publication and the new owner can continue from the same chunks. Invariant:
  a stale owner cannot advance partition state. Integration test.
- Given a populated source `[a, z)` and a split plan at `m`, when the base child
  rebuilds run while parent writes continue, assert those writes remain
  acknowledged by the parent and the source manifest/WAL pins remain live.
  Invariant: base range rebuild does not impose the split write outage.
  Integration test.
- Given the source retains multiple snapshot generations and page-pinned views,
  when split selects base manifest `b`, assert it creates only two child
  directories from `b`, leaves every older parent snapshot unchanged, and lets
  new child snapshots COW only dirty or materialized mapping segments.
  Invariant: split cost is not multiplied by historical snapshot count.
  Integration test.
- Given mutations on both sides of `m` after base sequence `b`, when split
  fencing selects cutover `c`, assert all admitted parent operations drain,
  every record in `(b, c]` enters exactly one child, both child manifests cover
  sequence `c`, and post-fence writes receive a retryable error. Invariant: the
  cutover has no lost, duplicated, or unbounded-gap mutation. E2E test.
- Given a pre-split request result for a key on the right, when split commits
  and the response is retried, assert only the right child retains its request
  ID and operation digest while the left child's sequence no-op does not claim
  it. Invariant: split preserves retry identity without creating cross-range
  deduplication aliases. Integration test.
- Given parent write rate keeps replay lag above the configured fence limits,
  when split preparation reaches its catch-up deadline, assert the parent
  remains serving, no mutation fence is entered, and the attempt cancels or
  retries with its pins accounted for. Invariant: split never begins a
  predictably unbounded final outage. Integration test.
- Given a split record whose key belongs to the left child, when delta replay
  applies it, assert the left tree applies its sequence and the right tree
  advances the same sequence as a no-op; repeat symmetrically for a right key.
  Invariant: filtered replay preserves a contiguous checkpoint frontier in both
  children. Unit test.
- Given a valid split artifact, when group 0 atomically publishes both matching
  children and supplies commit proof, assert the parent retires and both child
  streams start at `c + 1`; if either identity, epoch, range, or manifest
  differs, assert resolution fails closed. Invariant: only the exact prepared
  child pair can replace the parent. Integration test.
- Given both child artifacts are assigned to other nodes, when those managers
  open them before range-map publication, assert they remain `Prepared` and
  reject data requests; after exact commit proof, assert they enter `Serving`
  without rebuilding or copying page chunks. Invariant: target readiness can
  shorten cutover without granting authority early. E2E test.
- Given both children are serving from shared immutable mapping images and page
  packs, when post-commit cleanup runs under its budget, assert foreground
  requests continue, child manifests advance independently, and cleanup
  eventually removes current-generation cross-child sharing without releasing
  objects pinned by old snapshots. Invariant: physical separation converges
  asynchronously without extending the split fence. Integration test.
- Given child rebuild, delta replay, or checkpoint failure, when group 0 proves
  the transition absent and the caller aborts it, assert neither child is
  publishable, abandoned objects become reclaimable, and the parent resumes
  under its unchanged epoch. Invariant: an aborted split preserves one
  authoritative writable parent. Integration test.
- Given a timeout after the group-0 publication request, when the outcome is
  unknown, assert the parent remains mutation-fenced until a catalog read
  proves commit or abort. Invariant: ambiguous publication cannot create a
  writable parent alongside writable children. E2E test.
- Given one range is split on a manager hosting other partitions, when the
  source enters `SplitFenced`, assert source-range mutations pause while its
  reads and all operations on unrelated partitions continue. Invariant: split
  does not pause the server or node. E2E test.
- Given the manager crashes in `SplitPreparing` or `SplitFenced`, when R143
  resubmits the same transition plan after reading group 0, assert R142 reuses
  valid artifacts or safely rebuilds them and reaches the same split result.
  Invariant: split preparation and resolution are idempotent across restart.
  Integration test.
- Given ownership moves to another process, when the new manager opens the
  partition at a higher epoch, assert it recovers all acknowledged keys from
  existing tree/WAL chunks and no bulk data-copy operation occurs. Invariant:
  owner transfer moves authority, not stored data. E2E test.
- Given concurrent checkpoints and split workers for disjoint partitions under
  configured I/O and memory budgets, when load exceeds either budget, assert
  admission applies backpressure while foreground hot paths remain lock-free
  and memory remains bounded. Invariant: parallel dump has bounded resource
  use. Integration test.
- Given an empty partition, an unbounded endpoint, and split keys equal to or
  strictly inside the parent bounds, when validation runs, assert only a key
  strictly inside a non-empty range can start split and all accepted child
  ranges are exact and adjacent. Invariant: split cannot create a gap, overlap,
  or meaningless empty child. Unit test.
- Given the R142 public API and lifecycle states, when they are inspected,
  assert no merge operation or transitional merge state exists. Invariant:
  merge cannot accidentally bypass split and ownership fencing. Unit test.
- Given point operations, replay, checkpoint, split preparation/fencing,
  commit/abort, stale epochs, faults, range rejects, and backpressure, when
  partition metrics are collected, assert each outcome is attributed to the
  correct partition and transition ID. Invariant: operational state is
  observable without combining partitions. Unit test.

Required gates:

- `pixi run tree-fmt`
- `pixi run tree-lint`
- `pixi run test-tree-ct`
- `pixi run test-tree-ffi`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
- `pixi run -- cargo test -p crowdb-chunk-stream --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-kv --all-targets`
- `pixi run clean-env && pixi run test-server`
