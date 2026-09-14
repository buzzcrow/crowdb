<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R144: chunk-kv — Adjacent partition merge

**Status: Deferred.** Implement after R140, R142, R143, and R145 establish
stable manifest reuse, split/transfer fencing, group-0 transition recovery, and
routed retry semantics.

## Problem

R142 can split a partition and transfer ownership, but it intentionally exposes
no merge operation. Without merge, many formerly hot ranges can remain as
permanent small partitions after their load or data volume falls, increasing
routing, checkpoint, stream, and per-partition memory overhead.

Merge is not the inverse of split at the control-plane level. Two adjacent
parents can have different owners, ownership epochs, tree manifests, WAL
streams, mutation sequences, checkpoints, and in-flight operations. Publishing
a merged child after fencing only one parent can lose acknowledged writes or
leave an overlapping writer. Reusing both trees also requires a valid combined
B+tree root and a leaf sibling chain that crosses the old boundary without
escaping the merged range.

The existing crowdb-tree snapshot APIs rebuild or export one tree and R140
rebuilds one manifest into a subrange; neither composes two roots. Copying every
key through Rust would lose page-reference reuse and make time and memory
proportional to the combined data set. The merged partition also cannot reset
its logical mutation sequence below a resolved sequence already stored in a
reused page.

The root design links are R140's chunk page-store design, R142's partition
lifecycle design, and R143's group-0 range-map and transition design.

## Solution

Add an explicit two-parent merge transition that prepares one combined child
from adjacent immutable manifests, briefly fences both parents for catch-up,
and activates the child through one epoch-fenced group-0 range-map revision.

1. Extend the permanent tree and chunk-KV designs with adjacent-manifest
   composition, dual-parent lifecycle fencing, destination sequence seeding,
   publication, rollback, recovery, retention, and failure semantics. Only two
   exactly adjacent partitions may merge in one transition; repeated or
   multi-way merge is composed from successful pairwise transitions.
2. Add a native crowdb-tree operation that builds a destination manifest for
   `[left.start, right.end)` from the left and right pinned manifests. Reuse
   verified interior pages from both sources, build a combined root, and
   rewrite boundary ancestors plus the last left leaf so its sibling reference
   leads to the first right leaf. It must not materialize all keys in Rust.
3. Accept an idempotent merge plan containing `transition_id`, both parent
   identities, exact adjacent bounds, epochs and manifests, and the destination
   identity/range/epoch. Reject gaps, overlaps, reversed order, stale inputs,
   changed plans under one transition ID, or a parent already participating in
   another lifecycle transition.
4. Enter `MergePreparing` on both parents, publish and pin independent base
   checkpoints at sequences `b_left` and `b_right`, and build the destination
   base while both parents continue serving. Seed its destination-local
   sequence at `max(b_left, b_right)`, then replay both source deltas while the
   parents remain writable. Preserve order within each source, assign each
   replayed mutation a new contiguous destination sequence, and checkpoint the
   two source cursors with that sequence so restart can resume without relying
   on colliding source-local numbers. Preserve source WAL prefixes and every
   reused page-pack reference until commit or proven abort.
5. Close mutation admission on both parents only after destination replay lag
   is below configured record, byte, and estimated-time limits. If either
   source cannot catch up, keep both serving and cancel or retry preparation.
   Once fenced, drain all operations admitted on either side, reconcile their
   durable streams through `c_left` and `c_right`, and assign the bounded final
   delta contiguous destination sequences. Sequence overflow fails before the
   dual fence.
6. Persist and validate one destination manifest at its final destination
   sequence `d`, and create a distinct empty destination stream whose next
   mutation sequence is `d + 1`. Return one immutable merge artifact binding
   both exact parents, source cutover cursors, recorded conditional outcomes,
   and retained request-result windows to that destination. Coalesce an exact
   duplicate `(client_instance_id, client_sequence, operation_digest, result)`;
   a repeated request identity with a different digest or result rejects merge
   before fencing as `RequestConflict`. Reads may continue from the authoritative
   parents during preparation and fencing; new mutations receive a retryable
   merge error after the dual fence.
7. Extend R143's catalog transition so one new immutable range-map generation
   replaces both still-current parents with the exact destination and publishes
   one head. Current group-0 sysdata exposes blind puts rather than general
   compare-and-swap, so publication must not depend on coordinated updates to
   two live parent keys. Commit proof retires both parents and permits
   destination activation. Abort proof resumes both unchanged parents. An
   ambiguous head outcome keeps both parents mutation-fenced until an
   authoritative head read resolves it.
8. Make preparation, fencing, and resolution restart-idempotent. A replacement
   group-0 domain monitor reconstructs the transition, validates manifests and
   stream tails, and either reuses the exact artifact or restarts an unpublished
   build. If either parent owner dies, it waits for the old serving lease fence
   before recovering that parent or aborting; it never infers commit from local
   files, memory, or a service heartbeat.
9. Select a healthy destination owner and open the merged artifact as
   `Prepared`. Publish its serving lease only after the catalog head commits and
   both parent grants are fenced. A balance or split planner cannot start a
   second lifecycle transition on either parent or the destination until merge
   commit or abort is proven.
10. Reclaim parent manifests, streams, request-result records, and abandoned
    destination objects only after catalog references and transition pins allow
    it. Shared page packs survive as long as the destination or another manifest
    references them.
11. Add merge metrics for preparation, dual-fence drain, delta replay, reused
    and rewritten pages, publication/abort, ambiguous resolution, source pins,
    orphan bytes, owner failures, and write-fence duration.

Edge outcomes are explicit: non-adjacent partitions are rejected; either stale
parent epoch fails the whole transition; failure before publication leaves both
parents authoritative; partial parent replacement is invalid; sequence
overflow fails before fencing; and the destination never serves concurrently
with either writable parent.

Operation outcomes are explicit during merge:

- Gets, ordered seeks, and bounded scans already admitted to a parent complete
  on that authoritative parent view. A new request after catalog publication is
  routed to the destination; a stale request receives `NotMyRange`.
- Successful and condition-failed mutations admitted before the fence remain in
  source order, including their recorded CAS result and request ID. Mutations
  after the fence receive a retryable lifecycle result without WAL append.
- A lost pre-fence mutation response can be retried on the destination after
  commit and returns the retained original result rather than executing again.
- Parent scan tokens are invalid after commit and return refresh-required. The
  client cannot resume a parent token against the wider destination range.
- A journal, tree, owner, or group-0 failure follows R142/R143 containment and
  lease rules; it never makes the unfenced parent or uncommitted destination
  authoritative by inference.

## Dependencies

- Depends on R140's generation-addressed manifests, verified page fences,
  immutable reference retention, and native async chunk backend. R144 extends
  its range rebuild machinery with two-manifest adjacent composition.
- Depends on R141's independent stream replay, fencing, stable positions, and
  prefix pins for both parents and the destination stream.
- Depends on R142's partition manager, logical mutation sequences, lock-free
  admission fence, lifecycle proof boundary, checkpoint tuples, and fault
  behavior. R142 remains complete without R144.
- Depends on R143's authoritative paged range map, monotonic epochs, persisted
  transitions, domain monitor, owner serving grants, and atomic catalog-head
  publication. R143 must add a two-parent merge plan and exact commit/abort
  proof before R144 can ship.
- Depends on R145 for routed retry, request-identity persistence, scan replanning,
  and real-process merge E2E coverage.

## Acceptance

- Given adjacent parents `[a, m)` and `[m, z)` with verified manifests, when
  native composition completes, assert the destination scan equals their
  ordered union, its root covers `[a, z)`, and the rewritten sibling chain does
  not escape that range. Invariant: tree composition preserves exact key and
  navigation bounds. Integration test.
- Given fully contained interior pages and pages touching `m`, when composition
  runs, assert eligible interiors retain their page references while boundary
  ancestors and the final left leaf are rewritten. Invariant: merge reuses only
  pages whose references remain structurally valid. Unit test.
- Given parents with a gap, overlap, reversed order, stale epoch, or changed
  plan under the same transition ID, when merge preparation is requested,
  assert it fails before either mutation fence. Invariant: only one exact,
  adjacent, current parent pair can enter a merge. Unit test.
- Given writes continue on both parents during destination base construction,
  when preparation runs, assert the writes remain acknowledged and both base
  manifests and WAL prefixes stay pinned. Invariant: base construction does not
  impose the merge write outage. Integration test.
- Given both parents contain colliding source-local sequence numbers and later
  mutations, when replay and the dual fence drain through `c_left` and
  `c_right`, assert every acknowledged mutation appears once, destination
  sequences are contiguous above `max(b_left, b_right)`, and the next sequence
  is greater than every reused-page sequence. Invariant: independent parent
  histories converge without sequence collision or rollback. E2E test.
- Given retained request windows contain an exact duplicate identity/result or
  the same `(client_instance_id, client_sequence)` with conflicting digests,
  when preparation combines them, assert the exact duplicate is coalesced and
  the conflict aborts before either parent fence. Invariant: merge cannot make
  request retry identity ambiguous. Unit test.
- Given either source write rate keeps destination replay lag above the fence
  limits, when merge preparation reaches its catch-up deadline, assert both
  parents remain serving and no dual fence begins. Invariant: merge never
  begins a predictably unbounded final outage. Integration test.
- Given one parent faults or fails to drain, when finalization runs, assert no
  destination artifact becomes publishable and the other parent does not
  resume until group 0 proves abort. Invariant: dual-parent fencing is one
  transition, not two independent decisions. Integration test.
- Given one parent owner dies during preparation or after the dual fence, when
  the group-0 monitor resumes the transition, assert it waits for the old lease
  fence, recovers the exact durable parent suffix, and commits or aborts without
  granting overlapping authority. Invariant: owner death cannot shortcut a
  dual-parent fence. E2E test.
- Given a valid merge artifact, when group 0 publishes its epoch-fenced
  range-map activation record, assert the visible revision replaces both
  parents or neither and only an exact commit proof activates the destination.
  Invariant: the range map never exposes a gap, overlap, or partial merge. E2E
  test.
- Given a timeout after publication is submitted, when its result is unknown,
  assert both parents remain mutation-fenced until an authoritative read
  returns commit or abort proof. Invariant: ambiguity cannot create concurrent
  writable ownership. E2E test.
- Given the destination is prepared on a healthy server, when the catalog head
  has not committed or either parent grant remains valid, assert it rejects all
  data operations; after commit and both fences, assert one destination serving
  lease is issued. Invariant: preparation and storage readiness do not grant
  ownership. E2E test.
- Given a crash during preparation, fencing, commit, or abort, when a new
  coordinator resumes the persisted transition, assert the final range map and
  manifests match one valid all-parent or one valid destination outcome.
  Invariant: merge is restart-idempotent. E2E test.
- Given source manifests, streams, and page packs referenced by a live or
  unresolved transition, when GC runs, assert they remain; after committed or
  aborted references and pins clear, assert only unreachable objects become
  reclaimable. Invariant: reclamation never precedes merge resolution.
  Integration test.
- Given concurrent reads during preparation and fencing, when the merge commits
  or aborts, assert every read is served by an authoritative parent snapshot or
  the published destination and never observes keys outside its routed range.
  Invariant: merge preserves read range isolation. Integration test.
- Given a pre-fence successful or condition-failed mutation response is lost,
  when its request ID is retried against the committed destination, assert the
  original result is returned and no condition is re-evaluated. Invariant:
  merge preserves the deduplication and CAS-result retention boundary. E2E test.
- Given a parent scan token before merge commit, when it is resumed after the
  destination becomes current, assert it returns refresh-required rather than
  scanning the widened range. Invariant: merge cannot silently change a scan's
  range or pagination history. Integration test.
- Given merge preparation, fencing, replay, publication, abort, ambiguity, and
  GC, when metrics are collected, assert each outcome is attributed to the
  transition and both parents. Invariant: all material merge states are
  observable. Unit test.

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

- Should the destination normally stay on the left owner, the less-loaded
  parent owner, or a third server selected by the balance score? Reusing one
  parent owner reduces warm-up; choosing by load can avoid an immediate follow-
  up transfer.
- Should merge initially be operator-triggered only, or may the balance monitor
  propose and automatically execute merges after a long low-load window?
  Automatic merge controls partition overhead but adds a second autonomous
  topology-changing policy beside split.
- Is native two-manifest composition required for the first merge release, or
  may it fall back to a bounded background rebuild when page topology prevents
  safe reuse? Composition minimizes IO; a rebuild fallback is simpler but may
  make large merges impractically slow.
- What maximum dual-fence duration is acceptable before merge must abort? A
  short bound protects foreground availability; a longer bound tolerates slow
  final replay and checkpoint publication.
