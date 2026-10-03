<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R201: crowdb-tree — Concurrent MemTable writes and safe flush handoff

Status: Design finalized on 2026-10-03; implementation remains deferred until
requested. Select one node per key with selective prefix-version retention,
per-table closed/count admission, immutable Frozen sources and observed-version
scans. L1 accepts only records within a proven contiguous frontier. Retain soft
capacity thresholds and measure overwrite, retention, merge and memory costs.
No human design decisions remain open; implementation must satisfy the proofs,
failure behavior and acceptance cases below before the race is considered fixed.

#### Problem

An apply call selects active MemTable A once and inserts a batch record by
record. Concurrent flush can replace active with B, drain A and remove it
from query sources before that apply finishes. A retained pointer keeps A
alive, but later records enter an unpublished table. Acknowledged KV bindings
can disappear. CI run 36994977434 exposed this in
`test_slow_signed_upload_releases_native_buffers` as missing ChunkDB instance
bindings during an ordinary S3 upload.

The existing `ConcurrentSkipList::upsert` also holds one table-wide spinlock
across search, slot comparison, allocation and publication. Independent keys
cannot mutate the table concurrently. Removing it requires atomic insertion
and version replacement, not just deleting the guard. Per-overwrite calls to
`EpochManager::retire` currently take a separate shared reclamation mutex.

The [tree engine design](../design/tree/design-crowdb-tree-engine.md), especially
its L0 section, is the architecture reference. Its historical serialized-write
and snapshot descriptions are superseded by the selected contracts below when
this requirement is implemented. The relevant current implementation facts are:

- Active-pointer selection ends before insertion; neither `shared_ptr` nor a
  reader epoch establishes writer completion.
- Flush physically drains old tables and reinserts beyond-frontier records
  into a captured active table. This races writers and can invalidate an old
  scan's traversal even when removed allocations remain epoch-alive.
- Forward scan cursors can outlive the temporary source-owner vector. Skip-list
  destruction directly frees its remaining contents; an epoch guard does not
  automatically defer a normal destructor.
- Flush rereads the global contiguous frontier after publishing the successor.
  It can therefore advertise a slot belonging only to the new table.
- Reset, snapshot import and split-view capture also replace or detach table
  generations. They need the same ownership rules as ordinary rotation.
- Ordinary scans traverse live cells/pages without a common snapshot slot.
  Cold-page retries resume after emitted keys using another traversal attempt.
  The user confirms this observed-version behavior is the intended contract.
- Flush publishes into in-memory L1. Snapshot persistence establishes the
  durable recovery point; flush alone does not authorize WAL reclamation.

##### Confirmed S3 journal-cursor regression

The [R205 investigation](R205-s3-concurrent-client-progress.md) also confirms
a visibility gap without a late writer. Temporary chunk-key logging reproduced
it in the accumulated S3 suite during default boto3 multipart upload, after the
three copy cases. The observed sequence on 2026-10-03 (UTC) was:

- 01:54:20.775: MemTable 18 accepts slot 853. At .776651, ChunkDB acknowledges
  revision 853, modify_ts=60 and journal cursor=24040.
- The next flush sees contiguous frontier 852, leaving slot 853 ineligible for
  L1 publication. It removes that entry before reinserting it into MemTable 19.
- At .793, point reads find neither table's entry and return L1 slot 843,
  modify_ts=58, cursor=23039. A subsequent CAS toward modify_ts=61/cursor=24643
  correctly rejects the stale precondition; journal validation sees a cursor
  behind the acknowledged 24040.
- At .798, relocation inserts slot 853 into table 19; at .799, a subsequent
  flush publishes it to L1. Reads recover, but the earlier journal operation
  has already failed. The client fails after 66.080 seconds.

The old drain-before-reinsert path unlinks the above-frontier nodes before
publishing their replacement. Keeping the source table in the query-source
list or keeping its allocation alive does not preserve unlinked records.
Checking only writer completion/table presence therefore misses this I3 failure.
The fix must preserve point-read visibility throughout flush, while I6 still
forbids publishing slot 853 into L1 under frontier 852. CAS acknowledgement and
tree apply agree in this reproduction; a false successful CAS is not its cause.

The private fixture and removed diagnostic patch are retained at
`.crowdb-runtime/persistent/s3-client-failures/default-suite-relocation-gap`.
R205 retains the earlier accumulated 1,000-key failures, including the 414-byte
cursor regression, and their fixtures. Those traces are consistent with this
failure class but lack the same per-version proof. Default-concurrency CLI
SlowDown/stalls also remain separately unproven. Passing R201's deterministic
regression must not be used to declare all those failures resolved.

#### Solution

Select one node per key with CAS insertion and version-set publication, one
combined closed flag and batch count per MemTable, immutable Frozen contents,
and retained read-source ownership. No write groups, counter stripes, writer
announcement registry, historical scan queries or partitioned MemTables are
introduced. Independent apply calls continue to execute on their own callers.
Retained versions serve prefix flush; ordinary reads still choose the highest
observed slot. Alternatives below explain the choice and are not implementation
options to switch between without revisiting the design.

The lifecycle is Active -> Freezing -> Frozen -> Flushed. Flushed means source
publication is complete; physical reclamation can happen later. Readers do not
participate in the writer count.

##### Invariants

- **I1 — Admission.** A writer inserts into A only after conditional registration
  on A succeeds before closure. A stale selector that loses to closure retries
  against the successor. Closed tables never reopen.
- **I2 — Completion.** Frozen requires admission closed and outstanding batch
  count zero, observed with completion synchronization. Guard exit follows all
  mutation, retirement submission and accounting; successful slot completion
  also precedes exit. Failed batches do not mark their slot applied.
- **I3 — Visibility.** Freezing and unpublished Frozen contents remain queryable.
  Removing a source requires covered L1 publication, and existing readers retain
  the old traversal. A stable key cannot disappear merely because of flush.
- **I4 — Progress.** New batches use B while admitted A batches finish. They do
  not enlarge A's wait set. An independent-key writer does not need a paused
  writer to release a table-wide mutation token.
- **I5 — Ownership.** Guards retain the exact table/generation they entered.
  Counts cannot overflow, underflow or be reset under old guards. Every node,
  cell and external buffer has one reclamation owner. Readers and writers both
  protect borrowed pointers; engine replacement cannot reuse old live state.
- **I6 — Coverage.** A flushed frontier certifies completed publication of its
  prefix under highest-slot-wins semantics. It never exceeds proven coverage.
  Persisted coverage alone determines recovery/WAL reclamation. Slot gaps,
  partial failures and deduplication cannot fabricate coverage.
  Every record published into L1 must be within the flush's proven contiguous
  frontier. Publishing a record above that frontier while keeping a lower
  recovery watermark is explicitly excluded by the user.
- **I7 — Mutation.** Per-key highest-slot-wins, equal-slot replay, tombstones
  and intra-batch last-occurrence-wins remain unchanged. Readers observe fully
  initialized nodes and coherent key/slot/value tuples.
- **I8 — Ordinary scan.** A scan resolves the highest slot it actually observes
  for each key across its sources; a winning tombstone suppresses that key.
  Results respect direction, range and limit, without duplicates. Different
  keys may reflect different times or part of a concurrent batch. Concurrent
  insertion may be seen or missed. There is no common snapshot version and no
  promise to chase updates until response completion. This does not weaken
  I3 or I5, or the separate durable-snapshot and split-cutover contracts.

##### Concurrent skip list and reclamation

The selected representation keeps one node per key and atomically publishes
an immutable version-set descriptor. The descriptor contains one current winner
and any retained prefix versions; it is not merely a pointer to the latest
payload. The descriptor layout and allocator are implementation details; they
must preserve the single-version fast path and the ownership contract below.

- Preserve one authoritative node per user key. CAS on predecessor links
  inserts an absent key; competing insertion of the same key must converge on
  one node. Initialize payload and links before release publication. Level zero
  establishes membership; upper-level linking is an optimization that cannot
  block searches or unrelated insertion while its owner is paused. Complete
  tower publication before the writer guard exits. Height selection must not
  race a shared RNG or add a table-wide lock.
- For an existing key, CAS its version-set descriptor. On failure, reload and
  recompute insertion/merge against the winning descriptor. Only a strictly
  higher slot changes the current visible winner; a lower slot can still enter
  the retained set for prefix flush. Equal key/slot replay is idempotent, and
  operations within one batch are deduplicated before publication. Capture one
  protected cell pointer for a merge candidate and use its slot, flags and value
  together; do not rank one version and materialize another after an update.
- Do not physically unlink published nodes during Active or Freezing. Deletes
  publish tombstones. Frozen tables are immutable too: flush reads them and
  eventually detaches whole query sources. There is no concurrent physical
  skip-list deletion algorithm in this requirement.
- Enter epoch protection before inspecting existing nodes/cells on the write
  path. It lasts through CAS retry, accounting and retirement submission.
  Allocation, byte/entry statistics and slot-range updates must remain correct
  under out-of-order writer completion. Failed duplicate candidates were never
  published and can be destroyed by their owner.
- Retire a superseded value only after it is no longer needed for prefix
  publication under the selected retention policy. Reader grace periods alone
  do not establish that condition. Batch eligible retirement locally, then
  submit ownership through a lock-free pending-retirement queue. Existing
  maintenance drains the queue
  into epoch collection; the apply path does not take `reclaim_mu_` per record
  or run arbitrary old-buffer destruction callbacks as a reclamation pass.
  Prepare retirement bookkeeping before irreversible publication; failure and
  exception exits also submit everything they own. A retirement epoch assigned
  when maintenance collects the batch is conservative, never earlier than
  removal from the structure. Reuse requires a subsequent safe epoch grace
  period. Collection must run even with an idle write workload, and final
  teardown must drain both pending and epoch-retired allocations.
- Long readers can delay cell reclamation, but cannot prevent writer completion
  or table freezing. Table nodes/current cells survive both source ownership
  and outstanding borrowed access. Retain table owners in cursor bundles, and
  defer remaining allocation destruction through the epoch mechanism when
  borrowed views can outlive those owners. Keep the manager alive through its
  guards; an immediate table destructor is not sufficient.
- This removes table-wide mutation serialization and per-record retirement
  locking. Existing pointer-selection, slot-bookkeeping and L1-publication
  synchronization remains in scope for measurement. Allocator internals and
  those existing locks prevent claiming the entire apply path is lock-free.

##### Selective retention: feasibility and safety boundary

The user expects overwrites to be common and gap-related conflicts relatively
rare. This is a workload expectation to measure, not a measured result. Under
that expectation, retaining every overwrite until table retirement loses a
major benefit of the current representation. Select selective retention and
make its costs observable before considering that trade-off justified.

- Let D be a proven pruning bound for a writer, such that every future flush
  target using that table will be at least D. For each key, retain the highest
  version at or below D, if one exists, plus every version above D. The retained
  prefix version is an anchor; older versions in the completed prefix can be
  merged away. It need not be kept if already-covered L1 data proves it redundant,
  but that optimization is not needed for the initial correctness argument.
- This preserves the winner for every target F >= D: the winner is either an
  explicitly retained version above D, or the prefix anchor. Include tombstones
  in that reasoning; do not resurrect an older value by discarding the anchor
  tombstone. Arrival order does not change the rule. Do not confuse numerical
  adjacency of two versions of one key with global contiguous completion.
- Example: D=100 and versions 2, 80, 102, 105 of x. Keep 80, 102 and 105;
  version 2 can be merged away. A future flush at 102 needs version 102, so
  retaining only the anchor and highest version would be wrong. Once a safe
  bound reaches 105, one retained version suffices. This is not a two-version
  hard bound: a long gap can require many future versions of one hot key.
- Sampling the current global frontier during arbitrary mutation is unsafe.
  A flush might already own F=100 when the frontier advances to 102; pruning
  away x@2 in favor of x@102 would break that flush. Bind pruning permission to
  writer admission/closure, and stop advancing a closed table's pruning bound.
- A concrete proof baseline uses the chosen combined admission state. Sample
  D from the completed frontier before the successful registration CAS. At a
  flush boundary, close A first, then capture F, then expose B, all under the
  short rotation ownership. An admitted writer's sample precedes its admission
  RMW, which precedes closure; the acquire-release chain and monotonic frontier
  ensure D <= F. Delayed writers use their admitted bound even if B later drives
  the global frontier higher. Failed/stale admission must not authorize pruning.
- A new batch's own slot usually is not complete at initial registration. Its
  overwrite may therefore retain an old value temporarily even without a true
  out-of-order gap. After publishing successful slot completion, a still-counted
  writer may refresh D only by sampling the frontier and successfully performing
  a conditional RMW that validates A is still open. This can compare-exchange
  the combined state to the same value: it is an ordering event, not another
  writer registration or a mutation lock. If closure wins, keep the original
  bound. If validation wins, closure's later frontier capture covers the sample.
  All resulting per-key merge/accounting work still precedes guard release.
  This adds shared validation work per batch and possible descriptor CAS work;
  measure it rather than assuming selective retention is free.
  Post-completion merging is best effort: failure to prepare a replacement
  descriptor leaves the already-published versions intact and skips that merge.
  It must not turn an already completed slot into a reported failed apply.
- All consumers needing a prefix from L0, including split capture, must respect
  that bound or retain an independently protected view. This rule supports future
  captured prefixes, not arbitrary historical queries below a pruning bound.
- Publish a whole immutable descriptor by CAS, including current winner and
  retained references. Never destructively edit a version list read by another
  thread. CAS retry recomputes from the latest descriptor; replacement/pruning
  cannot lose a concurrently added lower version. Shared payload ownership and
  epoch retirement must free neither a payload still in a new descriptor nor a
  descriptor still borrowed by a reader. Copying descriptor references may cost
  O(h) for h retained versions; payload bytes need not be copied. The common
  one-version case should not pay for an unbounded container or scan history.
  Record the descriptor's validated pruning bound and carry it forward
  monotonically. A delayed writer may reuse a higher bound already established
  by a winning descriptor; its older admission sample must not lower that bound
  or reintroduce versions already known redundant. All previously validated
  bounds are covered by the same close/capture ordering proof.

A finite slot-selection model checked all subsets of slots 1 through 8, each
bound D, and every target F >= D: all 11,520 comparisons preserved the winner.
It also reproduced the two counterexamples above: dropping intermediate future
versions, and pruning against a frontier newer than an outstanding flush target.
This supports the selection rule only; it is not validation of concurrent CAS,
memory ordering, reclamation, or performance.

##### Alternatives and trade-offs

- **A — One node per key, selective retained versions (selected).** One
  tower/key allocation per distinct key; completed-prefix
  overwrites collapse, while gaps and captured boundaries retain required values.
  This preserves the intended memory advantage. Cost: descriptor CAS, conditional
  pruning permission, payload ownership and possible temporary batch retention.
  A long gap increases both memory and per-key merge cost; no constant bound or
  throughput gain is claimed.
- **B — Immutable `(key, slot)` entries.** Structurally feasible: compare user
  key ascending and full-width slot descending, insert by CAS, deduplicate equal
  key/slot, and never replace published payloads. Ordinary reads choose the
  maximum slot; flush chooses the maximum at or below F across all sources.
  This simplifies mutation and retains every necessary prefix. Cost: one tower
  and repeated key per retained version, plus every overwritten value until
  safe table retirement/compaction. Frequent overwrites therefore increase
  resident memory and rotation frequency relative to A. It is a fallback if
  A's complexity or measured CAS costs outweigh its memory benefit. Such a
  change requires a new design decision; B is not included in this implementation.
- **C — One node per key with an append-only version log.** Avoids repeated
  keys/towers and avoids concurrent pruning; entries can publish in arrival order.
  Flush must search the log for the highest eligible slot. Fast current reads
  need a separately coordinated maximum or a suitable immutable descriptor;
  the most recently appended value need not have the highest slot. Retaining all
  payloads still loses overwrite compaction, so it is not a substitute for A's
  memory objective. Adding selective pruning turns it back into A's lifecycle
  problem, even if the internal container differs.
- **D — Completed-prefix map plus out-of-order staging.** Keep future writes
  separately until their prefix is ready, then merge into a key-deduplicated
  structure. This moves version retention out of the main nodes but needs
  promotion ownership, source publication and read merging across two structures.
  Staging itself must retain intermediate future versions, and promotion cannot
  overwrite values needed by an older captured flush. If writes instead wait for
  global in-order apply, a missing slot delays later apply completion/visibility;
  that changes existing behavior and can introduce a global execution bottleneck.
  This is a larger write-pipeline redesign, not a free simplification of A.
- WAL reconstruction can recover discarded prefix payloads, but introduces WAL
  retention/indexing, replay and I/O into flush and couples the generic tree to
  an upstream log source. Sharding alone does not restore a lost version.
  Neither is a preferred solution to this requirement. Delayed coverage remains
  an availability/reclamation trade-off; beyond-frontier L1 writes are excluded.

For B, additional read-path work is mandatory: user-key seek/range/continuation
bounds must skip all versions of a key; reverse traversal cannot treat the first
composite predecessor as the newest version. Select the highest eligible slot
before processing tombstones, including when a newer out-of-range tombstone is
skipped by flush. Existing `MergeSource` and cursor code assumes one entry per
key in a table, so a comparator-only change would be incomplete. Internal slot
ordering must not change the external user-key encoding or user-key length limit.

Use distinct-key count U and retained-version count V to compare costs. B pays
key/tower overhead approximately V times, versus U times for A/C. C retains all
version payloads; A additionally reduces the number of payloads to prefix anchors
and versions still needed above protected bounds. Actual savings depend on key
and value sizes, overwrite frequency, gap duration and delayed readers; measure
those quantities rather than equating node count with total memory.

##### Admission and the Freezing-to-Frozen boundary

- Use one atomic state containing the closed bit and outstanding batch count.
  Register once per batch by CAS only while open; unregister once on exit.
  For selective retention, sample the pruning bound before registration and
  keep it with the guard. Optional completion-time open validation follows the
  ordering proof above; it does not change the batch count.
  A separate open check followed by unconditional increment is invalid.
  Reject count overflow before mutation. Empty/NoOp applies also participate
  in generation ownership because they update slot bookkeeping.
- Prepare B and pending-set storage before closure. Under the existing short
  selection synchronization, revalidate that A is still active, close A without
  changing its count, retain it in the source set, then publish B. All steps
  after irreversible closure must be non-failing. Preparation failure leaves A
  open; competing rotators cannot install different successors for the same A.
  Empty contents do not imply zero admitted writers.
- A batch completes all table work and submits successful slot completion
  before releasing its own guard. Trigger threshold rotation after release so
  a caller never waits on itself. Failure may leave partial visible records,
  as today, but must release ownership and report failure without a false slot
  acknowledgement. Preserve this result through C++, C and Rust boundaries.
- Use acquire-release RMWs for admission, closure and release as the proof
  baseline, with acquire observation of closed/zero. Mutations precede each
  release; the atomic RMW chain carries completion to the drain owner.
  Weakening orders requires a written proof and focused interleaving tests.
- Closed/zero is permanent readiness, not a notification count. Register a
  waiter and recheck readiness before sleeping. The last writer may signal;
  it does not flush on the apply path. One existing maintenance owner performs
  publication. No waiting holds selection or slot-bookkeeping locks.

##### Reader sources and flush publication

- Capture source owners and their published L1 floor together under existing
  source-selection synchronization, before obtaining L1 traversal pointers.
  The floor is a coverage filter, not a snapshot timestamp. Live L0 candidates
  at or below that captured floor are excluded, including stale replay entries
  inserted into B before its rejection floor was advanced. Resolve competing
  L0 sources by slot, not table age. Active/Freezing pointers may still update.
- Keep that owner bundle for the full cursor attempt. Frozen contents are
  never drained or relocated into B. After L1 publication, atomically update
  the source catalog/floor and detach fully covered sources. Existing readers
  retain their earlier owners and filter; new readers use the new catalog.
  This protects scans that captured A before B existed and scans with an old
  L1 leaf head. Removing a source owner never mutates its retained contents.
- Borrowed get/scan results retain appropriate epoch/lifetime protection until
  last access. Forward, reverse, no-load and async scan paths share this rule.
  An async retry may acquire fresh sources after the last emitted key; it does
  not promise a common data version across attempts.
- Explicit flush captures a finite set of old generations and target F at one
  admission boundary. Close A, then read the contiguous frontier before exposing
  B, while rotation is serialized; include all unpublished old generations.
  Complete old batches, then publish only proven coverage of this captured work. Never
  widen F from a later global frontier or repeatedly annex new B generations.
  Example: A covers 100 and B later completes 101; flushing A cannot certify 101.
- Use the same completion contract for explicit and maintenance flush. A
  synchronous flush waits for its captured writers, not old readers or future
  writers. Async entry points must suspend/offload the wait instead of blocking
  a Tokio worker. Existing maintenance's blocking execution can host synchronous
  waits; no new write-group worker pool is required. Already-ready and
  completion-before-wait races cannot lose wakeups.
- With the retained prefix versions, successful flush
  must cover its captured F; an above-F version must not force coverage back
  below that target. Slot gaps already limit F when it is captured. Snapshots
  read completed coverage, never the later live frontier. NoOp-only advancement
  is permitted when it does not skip any data-bearing work. Stalled admitted writers can delay
  the call indefinitely; this is the selected wait contract, not a bounded
  latency guarantee.
- Publish source removal and the rejection/coverage floor only after complete
  L1 publication. On cancellation or failure, retain pending sources and the
  previous certified frontier; retry is idempotent. If a failure leaves partial
  L1 output, snapshot/export must wait for publication repair before capturing
  that generation. Never convert a failed flush into successful persistence.

Prefix retention avoids the rejected latest-value-only failure: with covered
frontier 1, contiguous frontier 100 and x versions 2 and 102, keep x@2 for flush
through 100 while x@102 remains in L0. The same outcome is required when 102
arrives before 2. Ordinary reads may return x@102, but filtering that current
winner out of flush cannot substitute for retaining x@2. A certified frontier
never moves backward, and range metadata alone cannot reconstruct lost payloads.

##### Reset, import and split ownership

- Normal rotation keeps the engine open. `clear`, native/portable snapshot
  replacement and destruction instead close engine-generation admission under
  the existing lifecycle/source synchronization. Fence new read/write entry,
  close every writable old generation, and wait for all admitted old writers
  before resetting slot state. Drain old read operations before replacing live
  mappings; retained detached snapshots keep their own allocation ownership.
- Calls that arrive during replacement wait outside selection locks and enter
  the newly published generation afterward. Async callers suspend. New NoOps
  and forced-frontier operations cannot update slot state during the reset.
  Already admitted operations finish in the old generation before replacement;
  clear/import then intentionally supersedes that state. This is engine-enforced
  exclusion, not an undocumented caller-quiescence assumption.
- Build imports in a private owned generation and publish contents, mappings,
  slots and admission together. Do not reopen public admission halfway through
  portable import. On preparation/import failure, preserve the old logical
  state and reopen it using a fresh active successor if closure already occurred;
  never reopen a closed MemTable or expose a half-installed replacement.
  Normal batch ownership still covers internal import mutation. Destruction
  stops admission permanently and finishes reclamation before manager teardown.
- A split view captures a named finite generation set using the same close and
  successor publication protocol. Wait for those writers before publishing an
  immutable view. Keep source/overlay owners through both child publications
  and overlay removal, and keep old reader owners through their last access.
  Release must not discard unpublished parent records; transfer any still-needed
  source ownership back to normal pending sources instead.
- Split's journal cutover frontier and L1/persisted coverage are different
  quantities. The existing partition split cutover fences parent admission and
  expects its captured journal frontier to equal cutover sequence. Preserve
  that contract. An overlay reference alone cannot certify durable coverage.
  Overlay read descriptors must retain their own coverage context: advancing
  a destination journal position cannot filter out inherited source cells
  that have not been materialized into that destination's L1.
  If a requested cutover cannot be represented by the captured retained-version
  data, fail the publication before advancing destination coverage and retain
  source/overlay state for retry. Do not silently lower a split cutover or drop
  records above it. Forced frontier advance is a caller-certified recovery or
  NoOp operation, never a way for flush to erase an unresolved gap.

##### Capacity and performance decisions

- **Selected by the user:** retain existing soft table/byte thresholds. Freezing
  and Frozen tables both count as pending. When threshold rotation is suppressed
  by pending capacity, Active can continue to grow; forced rotation can exceed
  the soft count. This requirement adds no memory hard limit or write backpressure.
- Account for live, retained and pending-retirement bytes separately. Record
  outstanding writers, oldest Freezing age, pending-table count, reader-retained
  bytes and coverage lag; emit rate-limited backlog warnings. A stalled writer,
  reader or slot gap can retain memory/WAL. Use state changes and existing
  maintenance scheduling, not an immediate retry loop while pending stays true.
- **Selected by the user:** correctness and demonstrated independent-writer
  progress are required, together with a complete before/after performance
  comparison. No predetermined throughput or regression percentage is imposed.
  Measure one/multiple writers, small/large batches, distinct/hot keys and
  mixed get/scan/flush. Report throughput, latency distributions, CPU, retained
  memory, CAS retries, shared-counter and retirement costs. Explain regressions;
  removing a lock alone is not evidence of a speedup.

The user requests metrics to test the expected balance of overwrite versus
retention. Define logical events precisely before implementing counters:

- `mt_overwrite_total`: a successful publication changes an existing key's
  visible winner to a higher slot. New keys, equal-slot replay and CAS retries
  do not increment it. Retaining the previous winner does not cancel the fact
  that a logical overwrite occurred.
- `mt_history_keep_total`, with a fixed reason label: a version first enters
  the retained non-winning set, either as a superseded winner or a late lower
  slot needed by a prefix. Reasons distinguish `slot_gap`, `batch_inflight` and
  `flush_boundary`. Use the protected bound and completed frontier at the
  decision; when causes overlap, an already closed/captured boundary takes
  precedence, followed by a gap preceding the relevant version's batch, then
  temporary retention for that batch's own completion. This records why it was
  first retained, not every later reason it remains live. Descriptor copies
  and repeated visits must not count it again.
- `mt_history_merge_total`: count retained non-winning versions logically
  removed because safe prefix merging makes them unnecessary. One merge can
  remove several versions. Distinguish this from physical epoch reclamation and
  from retiring a whole table after flush; it measures overwrite compaction.
- `mt_history_versions` and `mt_history_bytes`: current retained historical
  payloads, counting shared payload ownership once. Track epoch-retired bytes
  separately so a long reader does not look like a slot gap. Include versions
  per key, time retained and Frozen-source retained bytes in distributions or
  existing memory instrumentation; no per-key or per-slot metric labels.

Overwrite and retention counters are not mutually exclusive. A normal overwrite
can temporarily keep the old value while its batch completes and merge it
immediately afterward. A late lower-slot insertion can increase retention without
overwriting the visible winner. Report rates, reason breakdown and live/peak
retained bytes together; `history_keep / overwrite` alone is not a gap rate or
a memory ratio. Count successful state transitions once, including real mutations
from a later-failed batch, and report batch failures separately. Accumulate
deltas locally per batch and use the existing metrics aggregation mechanism;
do not add a contended shared counter update to every CAS attempt or record.

Extend the workload matrix with overwrite-heavy, mostly in-order traffic;
rare/short and sustained gaps; reversed arrival; hot keys; and paused flush/readers.
The first case must demonstrate actual prefix-version collapse, not merely a
high overwrite counter while all payloads remain resident. No runtime metrics
have been collected in this documentation-only phase.

##### Work items

1. In `memtable/skip_list` and `memtable/memtable`, implement the selected entry
   representation with concurrent CAS publication, writer lifetime protection,
   correct accounting and non-destructive Frozen iteration under I5 and I7.
2. In `memtable/memtable`, tree ingestion and `epoch`, implement batch ownership,
   close/zero readiness and deferred retirement. Cover normal, encoded, external,
   empty and failed batches without adding a per-record shared reclamation lock.
3. In tree source selection, get and every scan path, retain source bundles,
   capture coherent cell candidates and maintain coverage filtering under I3/I8.
4. In tree rotation/flush, implement the finite boundary, selected safe coverage
   rule, exception-safe publication and readiness waiting. Remove destructive
   leftover relocation. Connect async/FFI and KV maintenance completion semantics.
5. In tree reset/import/split paths, FFI and `crowdb-chunk-kv` partition split,
   enforce generation ownership and distinguish journal cutover from persisted
   coverage. Audit all callers; do not rely on only closing current Active.
6. Add deterministic interleaving, lifecycle, persistence and original S3 race
   regressions; keep the S3 test enabled. Produce the performance comparison.
   Add the requested overwrite/retention/merge counters and memory gauges;
   validate their semantics and aggregate off per-record CAS retry paths.
   Reconcile permanent tree/KV descriptions of serialized L0 writes, ordinary
   scan snapshots and flush durability with the implemented contracts.

#### Dependencies

- Existing MemTable, epoch, FFI and KV maintenance are the baseline; no other
  unlanded requirement, dedicated writer pool or RocksDB queue protocol is needed.
- The [atomic ordering rules](https://eel.is/c++draft/atomics.order) support the
  combined-state proof. The implementation must additionally prove pointer
  lifetime, guard scope and non-failing successor publication.
- RocksDB's [InlineSkipList](https://github.com/facebook/rocksdb/blob/main/memtable/inlineskiplist.h)
  is a concurrent-insertion reference with retained nodes and immutable payloads.
  Its CAS insertion does not provide CROWDB's version-replacement or reclamation
  proof. RocksDB write groups are not selected for this design.
- RocksDB's [internal key format](https://github.com/facebook/rocksdb/blob/main/db/dbformat.h)
  includes a user key, sequence number and value type. It is a representation
  reference for alternative B, not justification for adopting B's memory costs
  or RocksDB's unrelated write-queue protocol.
- [KV state-machine semantics](../design/kv/design-crowdb-kv-state-machine.md)
  govern single-version slots, recovery and exported snapshots; the ordinary
  scan policy is explicitly I8. Partition split's existing cutover/journal and
  source ownership contracts remain required integration boundaries.
- Until implementation passes acceptance, existing test skips are coverage
  exceptions only. This document does not establish that the race is fixed.
- R200/R205 retain accumulated S3 acceptance and concurrency reproducers.
  Revalidate them after this implementation, keeping unresolved resource or
  progress failures under R205 with their first divergence. The completed
  [manual language SDK recipes](../../app/crowdb-access-server/tests/common/s3_sdks/README.md)
  provide additional regression coverage; their existing fresh-fixture passes
  do not establish the correctness of accumulated storage state.

#### Acceptance

- Given distinct-key writers in one table, pause one during search, level-zero
  insertion or upper-level linking and run another; assert the second can
  complete and its value is reachable without the first releasing a mutation
  token (I4, I7). Unit test.
- Given competing absent-key inserts and out-of-order/equal-slot updates,
  interleave CAS retries, duplicate operations and tombstones; assert one
  authoritative node per key, one visible winner and exact byte/entry ownership after all writers
  finish (I5, I7). Unit test.
- Given pointer borrowers and external-buffer callbacks, overwrite values,
  enqueue retirement, end writes and run idle maintenance; assert no early free,
  no callback loss, exactly-once eventual release and no per-record entry into
  the shared reclamation lock (I4, I5). Unit test.
- Given allocation/retirement-bookkeeping failures and exceptions in every
  apply entry point, fail before and after partial publication; assert guard
  release, retained visible data, valid ownership and no false successful slot
  completion across C/FFI (I2, I5, I6). Integration test.
- Given a writer holding A before registration, race closure in both orders;
  assert it either owns a counted admission or inserts nothing into A and
  retries B, including count-limit and stale-pointer cases (I1, I2, I5). Unit test.
- Given an empty A with a writer paused before its first record, rotate and
  complete a new B batch; assert A remains Freezing until its writer finishes,
  B progresses and no acknowledged key disappears (I1–I4). Integration test.
- Given competing rotators and failed successor/pending-set allocation,
  attempt closure; assert failure leaves usable Active and successful closure
  publishes exactly one successor without resetting any count (I1, I5). Unit test.
- Given open/zero, closed/nonzero and closed/zero states, race last completion
  with waiter registration and a long reader; assert only published closed/zero
  becomes Frozen, no wakeup is lost and the reader does not block it
  (I2, I4, I5). Unit test.
- Given thread churn, nested independent guards and exception exits, perform
  rotation; assert every guard releases its original generation exactly once
  without requiring a bounded writer registry (I1, I2, I5). Unit test.
- Given a scan paused after key a, update a and b before it reaches b; assert
  mixed-time results are permitted, each tuple is coherent and there is no
  restart requirement to obtain a common snapshot (I7, I8). Integration test.
- Given forward/reverse/no-load/async scans with controlled updates and
  tombstones across several sources, resume traversal; assert observed winner,
  ordering, bounds and uniqueness, including a changed candidate between merge
  ranking and materialization (I7, I8). Integration test.
- Given an old scan source bundle and old L1 head with a stable key ahead of
  its cursor, publish/detach its Frozen source; assert the key remains visible
  and table destruction cannot precede the last borrowed access (I3, I5, I8).
  Integration test.
- Given L1 value x@843 and acknowledged L0 value x@853, a flush target F=852
  and no remaining writer or competing mutation of x, pause flush at controlled
  source/publication boundaries and issue point reads from old and newly
  acquired source bundles; assert every read returns x@853, never x@843 or
  absence, and no x@853 enters L1 under F=852. The regression must expose the
  old remove-before-publish gap deterministically, without sleeps or reliance
  on diagnostic-log timing. Advance proven coverage through 853, publish and
  release readers; assert continuous visibility and safe retirement (I3, I5,
  I6). Integration test.
- Given real tree-backed ChunkDB state acknowledged at revision 853,
  modify_ts=60/cursor=24040 with an older revision 843/cursor=23039 underneath,
  interleave maintenance with a journal CAS toward cursor=24643 and no competing
  same-key writer; assert no stale-read-induced CAS rejection, no cursor
  regression behind the acknowledged position and exact committed journal
  bytes. Close slot gaps, persist and reopen; assert the committed cursor/data
  remain consistent without premature coverage (I2, I3, I6, I7). Integration test.
- Given a new source catalog after L1 publication and a stale replay cell in B
  below that catalog's floor, query it; assert L0 does not mask the newer L1
  value and old source bundles remain valid (I3, I6–I8). Integration test.
- Given A covering slot 100 and post-boundary B completing 101, keep writing B
  while flushing A; assert finite old-writer completion and that neither the
  reported frontier nor rejection floor claims uncovered slot 101 (I2, I4, I6).
  Integration test.
- Given only completed NoOps since the previous covered frontier, flush and
  persist; assert legitimate empty progress without credit for later
  data-bearing B batches (I2, I6). Integration test.
- Given a prefix value overwritten or rejected behind a beyond-frontier value,
  capture flush before the gap closes; assert the selected coverage policy
  retains required data/WAL and never certifies a missing prefix. Fill the gap,
  flush/persist/recover and assert the highest-slot value (I3, I6, I7).
  Integration test.
- Given covered frontier 1, contiguous frontier 100 and `x@2`/`x@102` in either
  arrival order, flush before slot 101 completes; assert no `x@102` enters L1,
  no missing `x@2` is credited as covered, and the selected retention/progress
  policy is explicit. Fill 101 and assert eventual publication through 102
  without lowering any prior certified frontier (I3, I6, I7). Integration test.
- Given selective-retention bound D=100 and x versions 2, 80, 102 and 105,
  merge eligible versions and flush at 100, 102 and 105 in separate scenarios;
  assert winners 80, 102 and 105, with only 2 initially redundant. Repeat with
  tombstones and reversed arrival (I6, I7). Unit test.
- Given a batch paused before admission, after admission or after sampling a
  refreshed bound, race closure and F capture in both orders; assert every
  authorized pruning bound is <= F, failed validation cannot prune at a newer
  bound, and B's frontier advances cannot destroy A's needed prefix (I1, I2,
  I5, I6). Integration test.
- Given same-key concurrent lower-version insertion and prefix merging, force
  descriptor CAS failures and keep old descriptors borrowed; assert no needed
  version is lost, current winner never regresses and payloads are freed exactly
  once after all owning descriptors/readers release them (I5–I8). Unit test.
- Given a successfully applied batch whose optional completion-time merge
  cannot allocate its descriptor, finish the call; assert successful apply,
  intact retained versions, exact guard release and eventual later merging,
  without a false failed-slot outcome (I2, I5, I6). Unit test.
- Given retained per-key versions, run reverse scans, user-key pagination,
  prefix bounds and flush with a future tombstone followed by an eligible
  value; assert correct user-key uniqueness and the eligible flush winner
  without prematurely suppressing it (I6–I8). Integration test.
- Given a delayed writer holding a lower pruning bound and a concurrently
  published descriptor with a higher validated bound, resume its CAS retry;
  assert the bound stays monotonic, redundant lower slots are not reintroduced
  and all still-required prefix versions survive (I5–I7). Unit test.
- Given known new-key, overwrite, lower-slot retention, equal-slot replay and
  forced CAS-retry events, inspect batch-aggregated metrics; assert exactly-once
  overwrite/keep/merge counts and no duplicate accounting from descriptor copies
  or retries. Complete batches, close gaps and release readers; assert logical
  history and physical retirement gauges fall at the correct separate stages
  (I2, I5–I7). Integration test.
- Given overlapping captured table slot ranges, partially failed batches and
  a previous covered frontier, execute the selected safe-prefix rule; assert
  conservative nondecreasing coverage, no underflow, and eventual progress
  after all required slots complete (I2, I6). Integration test.
- Given a failure after partial L1 publication, request persistence and retry;
  assert old sources/floors survive, incomplete publication is not snapshotted
  as completed work, and retry produces recoverable state (I3, I5, I6).
  Integration test.
- Given admitted writers in multiple old generations and an active reader,
  start clear or native/portable import, then race another apply/NoOp/read;
  assert old operations finish before replacement and waiting new operations
  enter only the published generation, with no old slot updates afterward
  (I1, I2, I5, I6). Integration test.
- Given import failure, outstanding borrowed results and pending retirements,
  replace or destroy the tree; assert failed replacement preserves old logical
  state and teardown frees allocations only after their owners finish
  (I3, I5). Integration test.
- Given a split view with paused writers and readers, publish to both children
  and release overlays; assert named-generation readiness, stable traversal,
  correct cutover coverage, inherited visibility despite a newer destination
  journal position, and retained parent records on failed/uncovered publication
  (I1–I3, I5, I6). Integration test.
- Given a full soft pending queue plus a stalled writer/reader or slot gap,
  run writes and maintenance; assert active growth remains allowed, backlog
  accounting/warnings reflect retained memory, and no pending-only busy loop
  or implicit hard-limit rejection is introduced (I3, I4, I5). Integration test.
- Given the original slow signed S3 upload workload, repeat with concurrent
  maintenance; assert successful upload, byte integrity and cleanup without
  missing bindings, with the regression test enabled (I1–I6). E2E test.
- Given a fresh real-storage fixture, run the entire default S3 suite in its
  existing order, including the three copy cases, default boto3 MPU and
  `test_batch_delete_thousand_keys_and_unversioned_retry`, without resetting
  storage between cases; repeat the complete run at least three times with
  normal logging. Assert exact bytes/deletions, successful cleanup and no
  journal cursor regression, snapshot Corruption or missing bindings. A
  focused fresh-fixture pass cannot substitute for accumulated execution;
  remove the existing slow-upload skip before claiming full R201 acceptance
  (I1–I6). E2E test.
- Given the existing default-concurrency AWS CLI fixture and its documented
  receive budget, rerun concurrent MPU after the repair; assert no acknowledged
  metadata disappears and no journal cursor regresses. Retain any SlowDown,
  fsync deadline or progress stall with its first divergence under R205 until
  separately explained; do not increase budgets/retries or call a failed run a
  pass to close this item (I3, I4, I6). E2E test.
- Given each pinned Java, JavaScript and Go recipe on a fresh real-storage
  fixture, run the manual tasks sequentially after the storage repair; assert
  their existing exact-byte, metadata, default-checksum, presigned, listing,
  delete and injected-MPU-failure cleanup checks still pass. These remain
  manually invoked checks, outside routine CI/container/release gates
  (I3, I5, I6). E2E test.
- Given identical hardware/configuration and the selected workload matrix,
  compare baseline and implementation; assert independent-writer progress
  and record complete throughput, latency, CPU, memory and contention results,
  including regressions without an invented percentage gate (I4, I5, I7).
  Integration test.

Run the following S3 checks sequentially after implementation. Clear
`CROWDB_S3_E2E_ENDPOINT`, `CROWDB_S3_E2E_ONLY` and `CROWDB_S3_E2E_SDK` before
the full-suite commands so an external endpoint or focused mode cannot replace
the accumulated fixture. Remove the slow-upload skip in the harness first;
setting its focused selector alone currently still skips that case.

```sh
# Focused reproductions; these do not replace the accumulated gate.
pixi run clean-env && CROWDB_S3_E2E_ONLY=test_default_boto3_checksums_and_multipart pixi run -e s3-e2e test-boto3-e2e
pixi run clean-env && CROWDB_S3_E2E_ONLY=test_batch_delete_thousand_keys_and_unversioned_retry pixi run -e s3-e2e test-boto3-e2e
pixi run clean-env && CROWDB_S3_E2E_ONLY=test_slow_signed_upload_releases_native_buffers pixi run -e s3-e2e test-boto3-e2e
# Run this complete command at least three times, preserving order within each run.
pixi run clean-env && pixi run -e s3-e2e test-boto3-e2e
pixi run clean-env && pixi run -e s3-e2e test-aws-cli-concurrent
pixi run test-s3-sdks
```

Run `pixi run test-cpp`, `pixi run cargo test -p crowdb-tree-ffi --tests`,
`pixi run clean-env && pixi run cargo test -p crowdb-kv`,
`pixi run clean-env && pixi run cargo test -p crowdb-chunk-kv`,
`pixi run clean-env && pixi run test-chunkdb`,
`pixi run clean-env && pixi run test-chunk-stream`, `pixi run rs-fmt-check`,
`pixi run cargo clippy -p crowdb-tree-ffi -p crowdb-kv -p crowdb-chunk-kv --all-targets -- -D warnings`,
`pixi run tree-fmt`, and `pixi run tree-lint` for the implemented scope.
