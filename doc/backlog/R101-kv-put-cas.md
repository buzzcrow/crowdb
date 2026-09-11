<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R101: kv — Compare-and-Set Writes

## Status

Re-proposed with a reviewed design: evaluate revision preconditions on the
leader before proposal, serialize conditional mutations through a
`cas_transient_map`, and put only the resulting ordinary mutation batch in
Paxos. Different keys retain parallel-slot concurrency and replicas never
evaluate a condition.

The guarantee is deliberately scoped: conditional writes serialize with other
conditional writes that name the same precondition key. Blind Put, Delete, and
BatchWrite requests do not join the guard and receive no conflict-detection
guarantee. A caller that requires CAS protection for a key must use conditional
writes for every competing mutation of that key.

R101 is not a dependency of R132.

## Problem

`FBKvSetRequest` and `FBKvBatchWriteRequest` are blind overwrites. A caller
cannot atomically require that a mutation key still has the revision it read.
Read-before-propose alone is insufficient because the sliding proposal window
allows two requests for the same key to read revision N and enter different
slots. Apply-time conditions are also invalid: learners apply slots out of
order, so replicas could evaluate the same condition against different local
states and diverge.

The original diskdb free path demonstrates the missing primitive. Its desired
operation is:

```
check BusyBlockKey at the revision just read
  -> Delete BusyBlockKey
  -> Put incarnation-qualified FreeBlockKey
```

Without KV CAS, diskdb uses a safety-preserving compromise in
`model/alloc.rs::free_block/free_blocks`: it blindly writes an immutable free
fact, leaves the busy record present, and defers incarnation validation and
busy deletion to compaction. This makes free idempotent but retains busy and
free records until background work runs.

The real `FreeBlocks` RPC accepts multiple segments, but freeing one segment
does not require another segment to succeed. It should specialize the generic
primitive as one independently guarded conditional batch per segment, execute
those batches with bounded concurrency, and return both the number freed and
the segments that could not be freed. This preserves partial progress and
keeps the KV primitive to one precondition per batch.

Design sources that must be reconciled when R101 is implemented:

- `doc/design/kv/design-crowdb-kv.md` currently excludes CAS because it has no
  serialization boundary.
- `doc/design/kv/design-crowdb-kv-slot.md` defines slot order and
  highest-slot-wins apply.
- `doc/design/kv/design-crowdb-kv-state-machine.md` defines asynchronous,
  out-of-order apply and the applied frontier.
- `doc/design/diskdb/design-crowdb-diskdb.md` and
  `design-crowdb-diskdb-zone-management.md` describe the current immutable-free
  compromise.

## Solution

Add one optional revision precondition to Put and BatchWrite. The leader checks
the precondition while holding a non-waiting per-key guard, then proposes an
ordinary Put/Delete batch. The checked key must be mutated by that batch.
Conditions never enter the Paxos payload and are never re-evaluated by learners
or replay.

1. **Wire contract and client surface**

   - Add `FBKvRevisionPrecondition { key, expected_revision }` and one optional
     `precondition` field to `FBKvSetRequest` and `FBKvBatchWriteRequest` in
     `lib/crowdb-protocol/src/fbs/kv_client.fbs`. Absence means a blind
     mutation; a present revision zero means create-if-absent.
   - Add `CasFailed`, `CasBusy`, and `OutcomeUnknown` return/error variants.
     `CasFailed` returns the checked key's current revision. `CasBusy` is
     retryable contention. `OutcomeUnknown` means the server could not prove
     whether an Accepted value will later be chosen.
   - Add `put_cas` and conditional `batch_write` surfaces in
     `lib/crowdb-kv-client`. Existing `put` and `batch_write` signatures and
     blind behavior remain unchanged.
   - Require the precondition key to occur as a mutation in the batch. For a
     Set request it must equal the Set key. `expected_revision=0` is legal for
     Put/create, but not Delete of an absent key.

2. **Conditional admission and check order**

   - Add a per-group lock-free `cas_transient_map` owned by `PxGroup`, using
     the existing `crossbeam_skiplist::SkipMap<Bytes, CasOwnerToken>`
     dependency. It maps each unresolved precondition key to a unique
     `(tenure, request)` ownership token. Only conditional requests touch it;
     the blind write hot path remains unchanged.
   - Claim with `compare_insert(key, token, replace_if_stale_tenure)` and
     compare the returned token. Equality means this request inserted and owns
     that exact entry; a different token from the active tenure returns
     `CasBusy`. A new tenure may atomically replace an old-tenure entry after
     its recovery barrier. The owner retains the returned entry handle and
     removes that exact node on release, so a delayed old task cannot remove
     the replacement. The operation never waits or introduces a lock into the
     write path.
   - Copy the precondition key once into `Bytes` before launching the
     group-owned task because the RPC frame may be released first. Retain that
     allocation through check, proposal resolution, and exact-entry removal.
   - Acquire the key guard before reading its revision. Reading before guard
     acquisition would recreate the original time-of-check/time-of-use race.
   - Add a fallible versioned lookup to `KVEngine` for conditional admission.
     It returns live `(revision, value)`, absent/tombstoned, or an engine error.
     The existing `CrowdbTreeEngine::get` cannot be used unchanged because it
     currently folds an underlying `try_get` error into `None`; CAS must never
     interpret an I/O or corruption error as key absence.
   - Under the guard, use that lookup over the complete leader engine view,
     not the memtable directly. Crowdb-tree first checks every live L0
     memtable; an L0 miss must continue into the L1 tree, including
     asynchronous demand-load on a cold page. Only a successful lookup that
     finds neither a live L0 value nor an L1 value means revision 0. An L0
     tombstone masks an older L1 value and also contributes revision 0. Return
     `CasFailed` without proposing if the expected revision differs; return an
     engine error without allocating a slot if the lookup fails.
   - Keeping an owned skip-list entry in `cas_transient_map` across an
     asynchronous L1 read is intentional. No epoch pin, borrowed RPC frame,
     or lock may be retained across `.await`.
   - Recheck role, term, and conditional-write readiness immediately before
     slot allocation. A leadership miss returns `NotLeader` without a
     proposal.

3. **Proposal, apply visibility, and guard lifetime**

   - Encode only the ordinary mutation batch and propose it through Paxos.
     Conditional requests initially bypass proposal coalescing so one guarded
     request owns one payload and outcome; the guarded batch may mutate the
     precondition key plus related keys up to the normal batch limit.
   - After the request payload is chosen, wait until its slot is locally
     applied before releasing its key guards and returning success. The
     existing contiguous applied fence may be reused initially; a per-slot
     completion notification is an optimization.
   - Run the guarded protocol in a group-owned task. RPC cancellation drops
     only the response waiter, not the task or its guards.
   - A guard is not an ordinary return-path Drop guard. It may be released only
     after all slots allocated for the request are resolved, after the group
     has closed conditional admission for the tenure, or after step-down.
     Step-down invalidates the old tenure token; delayed old tasks can no
     longer remove entries installed by a later tenure.

4. **Close every uncertain allocated slot**

   - Track every slot allocated by a conditional proposal. If normal proposal
     retries cannot determine a slot, run Phase 1 for that slot with a higher
     ballot before releasing the guards.
   - If the Phase-1 quorum reports no Accepted value, choose and apply NoOp.
     If it reports an Accepted value, obey Paxos and choose the highest-ballot
     value; a leader must never overwrite it with NoOp.
   - If the adopted value is this request's payload, apply it and complete the
     request successfully. If it is another payload, apply it, re-read the
     guarded precondition, and retry this request in a new slot only if it
     still matches.
   - If the leader cannot obtain a resolution quorum, set conditional-write
     readiness false for the tenure and return `OutcomeUnknown`. No later CAS
     is admitted by that leader until recovery succeeds. Blind writes remain
     outside this contract.

5. **Leadership recovery barrier**

   - Set conditional-write readiness false before a multi-replica candidate is
     exposed as a CAS-serving leader. The existing role/term proposal gate is
     not sufficient because bulk Phase 1 currently runs after leader promotion.
   - Resolve every pre-tenure slot through the election ceiling and apply every
     recovered value locally. A failed slot repair keeps conditional-write
     readiness false; it must not be skipped followed by a ready transition.
   - Set conditional-write readiness true only after that resolve-and-apply
     barrier completes. A new leader then needs no predecessor's temporary key
     map: all prior conditional writes are represented by ordinary applied
     mutations.
   - A single-replica leader may become ready after local WAL replay and engine
     recovery complete.

6. **Scoped interaction with blind writes**

   - Blind mutations neither inspect nor reserve conditional keys. They may
     overwrite a conditional result or be overwritten according to ordinary
     highest-slot-wins behavior.
   - R101 does not claim linearizable CAS relative to blind mutations. It
     guarantees serialization only among conditional mutations sharing a
     precondition key.
   - Components relying on CAS must not mix blind and conditional mutations
     for the protected state. This is an API contract, not an inference made
     from request arrival order.

7. **diskdb direct free migration**

   - Change `DdbKvClient::get_busy` to propagate both `BusyBlockValue` and its
     KV revision. Add a point lookup for the incarnation-qualified free fact
     so a response-loss retry can recognize an already completed free.
   - Specialize one segment as one conditional batch:
     `Delete BusyBlockKey + Put FreeBlockKey`, with the busy key and its read
     revision as the batch's sole precondition.
   - In `model/alloc.rs::free_block`, first require the busy value's
     `allocation_ts`, `unit_count`, and `owner_chunk` to equal the request
     `Segment`; a stale or forged free must not delete a newer incarnation.
   - In `free_blocks`, run the per-segment operations with bounded concurrency.
     One segment's `NotBusy`, identity mismatch, CAS conflict, or unavailable
     outcome does not roll back successful frees for other segments.
     Deduplicate repeated physical-incarnation entries within one request so
     they produce one mutation and one response result rather than inflate
     `freed_count`.
   - Extend `FreeResponse` and `FBFreeResponse` with append-only per-segment
     failures. Keep `freed_count`; add entries containing the original segment
     and a stable reason (`NotBusy`, `IncarnationMismatch`, `Conflict`, or
     `OutcomeUnknown`). A completed partial request uses the Success top-level
     code and requires the caller to inspect the failures.
   - Preserve idempotent retries: if BusyBlockKey is absent but the matching
     FreeBlockKey and value already exist, count that segment as freed rather
     than return `NotBusy`. If both are absent, return `NotBusy`.
   - Convert `commit_blocks` updates, and future state/health updates of an
     existing `BusyBlockKey`, to the same single-key conditional write so they
     cannot cross a free of that key. Allocation creates remain separated from
     free by the conservative bitmap and compaction-before-reuse invariant;
     create-if-absent CAS can be added without changing the free primitive.
     The tentative cache must retain the Busy Put's returned KV revision for a
     later conditional commit, or `commit_blocks` must fall back to the
     fallible versioned KV lookup.
   - Adapt compaction to the new proof: a successfully written free fact no
     longer needs a live busy record because the conditional batch already
     validated and deleted the exact busy revision. Compaction clears the
     conservative bitmap range from the free fact, writes `ZoneValue`, and
     deletes the processed free fact atomically.
   - Preserve mixed-version cleanup: legacy free facts may coexist with their
     busy records. Compaction continues full incarnation matching for those
     pairs and conditionally deletes the matched busy revision. New-format
     free facts with no busy record use the direct-free proof.
   - The in-memory bitmap remains conservative and is still cleared only by
     compaction. R101 removes the delayed busy-record validation compromise;
     it does not move bitmap reuse onto the free RPC hot path.

8. **chunkdb integration**

   - Propagate `GetOutcome::Found.revision` through `ChunkStore`.
   - Write lifecycle metadata with `put_cas`. Map `CasFailed` to
     `LifecycleError::StateConflict`, re-read, re-run the state transition,
     and retry within the caller's bounded policy.
   - Do not retry an `OutcomeUnknown` as a fresh logical mutation. Reconcile by
     reading current state or reuse the same idempotency identity when the
     original leader remains available.

## Dependencies

- Depends on the existing proposal retry/adoption path, async apply fence,
  bulk Phase-1 leader recovery, and `GetOutcome::Found.revision`.
- Updates the KV root, slot, state-machine, RPC, and test designs when
  implemented; those permanent documents continue to describe current code
  until the requirement lands.
- Replaces diskdb's immutable-free validation compromise. Implementation must
  update both diskdb permanent design documents together with code.
- R99 and R100 remain the primary chunkdb ownership and in-process lifecycle
  boundaries; R101 adds KV conflict detection.

## Acceptance

- Given revision N, `put_cas(expected=N)` succeeds, applies at revision S, and
  releases its guard only after the local engine reports S. Unit test.
- Given a different revision, CAS returns `CasFailed` with the current
  revision and allocates no slot. Unit test.
- Given an absent key, Put with expected revision 0 succeeds; the same request
  against a live key fails. Unit test.
- Given a memtable miss and an existing value in a resident or cold L1 page,
  CAS observes the tree value and revision rather than treating the key as
  absent. Integration test.
- Given an L0 tombstone over an older L1 value, CAS observes absence and never
  resurrects the older tree revision during its check. Integration test.
- Given an L1 demand-load I/O or corruption error, CAS returns an engine error,
  does not reinterpret it as revision 0, and allocates no slot. Integration
  test.
- Given two same-key CAS requests that both expect N, exactly one succeeds;
  after retry, the other observes the new revision and fails. Integration test.
- Given CAS requests on different keys, both can hold guards and occupy
  different in-flight slots concurrently. Integration test.
- Given a single-precondition batch, a mismatch leaves every batch item
  unchanged; a match applies the full Delete/Put batch atomically. Integration
  test.
- Given a guard collision, the request returns `CasBusy` without reading or
  allocating a slot. Given a delayed task from an old tenure, its token cannot
  remove the new tenure's same-key guard. Unit test.
- Given chosen-but-delayed apply, a second same-key CAS remains busy until the
  first slot is locally visible. Integration test.
- Given RPC cancellation after slot allocation, the group-owned task keeps the
  guard and resolves the slot before admitting another same-key CAS.
  Integration test.
- Given a partially Accepted conditional slot, higher-ballot closure chooses
  the required adopted value or NoOp; it never overwrites an Accepted value
  illegally. Paxos integration test.
- Given an adopted foreign value that changes a guarded key, the conditional
  request rechecks and fails instead of copying its stale decision into a new
  slot. Paxos integration test.
- Given leader loss with an unresolved conditional slot, the new leader rejects
  CAS until bulk Phase 1 resolves and locally applies the complete recovery
  range. Paxos integration test.
- Given a repair failure inside that range, conditional-write readiness stays
  false. Paxos integration test.
- Given a concurrent blind write, no CAS-relative ordering assertion is made;
  all replicas still converge by ordinary highest-slot-wins apply. Integration
  test.
- Given one diskdb segment with matching busy identity and revision, free
  atomically deletes BusyBlockKey and puts FreeBlockKey. Integration test.
- Given a missing, mismatched-incarnation, or concurrently changed busy record,
  diskdb free writes neither delete nor free record. Integration test.
- Given multiple diskdb segments with mixed valid and invalid busy records,
  every valid segment is freed, `freed_count` reports those successes, and the
  response identifies every segment that was not freed with its reason.
  Integration test.
- Given a retried segment whose busy key is absent and matching free fact is
  present, diskdb reports it as already freed successfully. Integration test.
- Given a committed direct-free batch and delayed compaction, recovery remains
  conservative; compaction later clears the bitmap and removes the free fact.
  Integration test.
- Given legacy busy-plus-free records during upgrade, compaction validates the
  incarnation and migrates them without deleting a newer allocation.
  Integration test.
- Existing blind Put, Delete, and BatchWrite requests retain their wire and
  highest-slot-wins behavior. Integration test.

Run:

- `pixi run -- cargo test -p crowdb-kv --test group_test cas`
- `pixi run -- cargo test -p crowdb-kv --test paxos_test cas`
- `pixi run -- cargo test -p crowdb-kv-client --test put_cas_test`
- `pixi run -- cargo test -p crowdb-diskdb --test cas_free_test`
- `pixi run -- cargo test -p crowdb-chunkdb --test lifecycle_test cas`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run -- cargo clippy --all-targets -- -D warnings`
