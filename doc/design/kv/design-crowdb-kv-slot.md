<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Slots — Parallel Pipelining & Acceptor Retention

Depends on: [`design-crowdb-kv.md`](design-crowdb-kv.md) §5.1, §6.5, §7.3
Satisfies: [`design-crowdb-kv.md`](design-crowdb-kv.md) §5.1, §6.5, §7.3

This document covers two aspects of CROWDB's slot mechanism:

- **Part A (§1–§14): Parallel Slot Pipelining** — the consensus protocol that distinguishes CROWDB from Raft-based KV systems. How the leader pipelines proposals, how gaps are detected and repaired, how the safe-slot is maintained, and how the system stays correct under concurrent in-flight slots.
- **Part B (§15–§22): Acceptor Retention** — guarded per-slot state, concurrent trim/reclamation and snapshot-backed retention certificates.

## Table of Contents

- [1. Why Parallel Slots](#1-why-parallel-slots)
- [2. Concepts and Invariants](#2-concepts-and-invariants)
- [3. Slot Lifecycle on the Leader](#3-slot-lifecycle-on-the-leader)
- [4. Sliding Window and Backpressure](#4-sliding-window-and-backpressure)
- [5. Pipelined Fanout](#5-pipelined-fanout)
- [6. Per-Key Resolved-Slot](#6-per-key-resolved-slot)
- [7. Safe-Slot Computation and Propagation](#7-safe-slot-computation-and-propagation)
- [8. Gap Detection](#8-gap-detection)
- [9. Gap Repair via Classic Paxos](#9-gap-repair-via-classic-paxos)
- [9A. Follower-Side Apply and Catch-up](#9a-follower-side-apply-and-catch-up)
- [10. Timing Diagrams](#10-timing-diagrams)
- [11. Interaction with Snapshot and WAL GC](#11-interaction-with-snapshot-and-wal-gc)
- [12. Tunables and Defaults](#12-tunables-and-defaults)
- [13. Correctness Analysis for Parallel Slot Writes](#13-correctness-analysis-for-parallel-slot-writes)
- [14. Parallel-Slot Linearizability Analysis](#14-parallel-slot-linearizability-analysis)
- [15. Acceptor Storage Requirements](#15-acceptor-storage-requirements)
- [16. Retained Slot Entries](#16-retained-slot-entries)
- [17. Per-Slot State](#17-per-slot-state)
- [18. Lookup and Reclamation](#18-lookup-and-reclamation)
- [19. Acceptor API](#19-acceptor-api)
- [20. Retention Invariants](#20-retention-invariants)
- [21. Storage Cost](#21-storage-cost)
- [22. Retention Limits](#22-retention-limits)
- [23. Server-side Proposal Coalescing](#23-server-side-proposal-coalescing)

---

# Part A: Parallel Slot Pipelining

## 1. Why Parallel Slots

A Raft leader cannot acknowledge slot N+1 until slot N has been committed; the log is contiguous, so a single slow follower stalls the entire commit pipeline (head-of-line blocking). Multi-Paxos has no such constraint: each slot is a separate Paxos instance, and a quorum that decides slot N+1 need not include the same nodes that decide slot N.

CROWDB exploits this by running many slots in parallel on the leader. Throughput is bounded by network and disk bandwidth, not by per-slot serialized round-trips. The price is two-fold:

- **Gaps.** A slot may remain undecided long after later slots are decided. We need a mechanism to resolve gaps without stalling the hot path.
- **Conservative cross-key reads.** A `Scan` must wait for a no-gap prefix; point reads do not.

The blind-ops premise from design-crowdb-kv.md §5.2](design-crowdb-kv.md) makes the trade-off cheap: out-of-order *apply* is safe because no operation reads before writing.

---

## 2. Concepts and Invariants

- **I1 — Single slot counter.** On a leader, slot assignment is performed by exactly one logical worker. Two writes never receive the same slot, and no two slots are assigned out of arrival order.
- **I2 — Slot determines linearization.** The slot number assigned to an op is the op's position in the global linearization order (design-crowdb-kv.md §6.1](design-crowdb-kv.md)).
- **I3 — Quorum-fsync before ack.** A client write is acked only after a quorum of acceptors (including the leader) have fsynced their `Accepted(slot, ballot, value)` records. This is the durability hook that makes I2 robust to failures.
- **I4 — Apply-order independence for blind ops.** For any key *k*, the engine's final value is `value(max{ slot | slot writes k })`. Apply order between non-overlapping keys is irrelevant; for the same key, the higher slot wins regardless of arrival order ([§13](#13-correctness-analysis-for-parallel-slot-writes)).
- **I5 — Per-key resolved-slot is monotone.** A learner's per-key tracker only ever advances. This is the basis for read-your-writes from followers.
- **I6 — Safe-slot is contiguous.** The cluster-wide safe-slot is the maximum N such that *every* slot ≤ N is chosen and applied on every learner. It is by definition gap-free.

---

## 3. Slot Lifecycle on the Leader

| State | Trigger to enter | Trigger to leave | Notes |
| --- | --- | --- | --- |
| `Assigned` | client request admitted; counter incremented | leader's WAL fsync of the `PxLogEntry` completes | Visible to no one yet |
| `Proposing` | local fsync done; `Accept` fanned out to followers | quorum of `Accepted` responses received | At this point the value is *chosen* |
| `Chosen` | quorum reached | learner has applied to engine | Ack to client may be sent here (see below) |
| `Applied` | learner.apply() returned | terminal | Used by metrics; safe to evict |
| `OrphanRepair` | leader changed before reaching `Chosen`; repair task takes over | `Chosen` (via repair) | New leader's responsibility |

**When does the ack happen?** When the slot reaches `Chosen` *and* the leader's own learner has applied it. The leader's own learner application is required so that an immediately-following `Get` on the leader sees the new value (see [§6.1 of design-crowdb-kv.md](design-crowdb-kv.md#61-linearizable-leader-read)).

**Eviction.** Acceptor entries and WAL segments become eligible below the snapshot-backed reclamation watermark in §11. Local application alone does not authorize eviction.

---

## 4. Sliding Window and Backpressure

The number of in-flight slots is capped at the **window size** (`max_inflight_proposals`, default 32). The proposer uses an `InflightAdmission` gate backed by one or more `Semaphore`s. If a permit is available, the request is admitted, slot is assigned, and proposing begins. The permit is held for the entire proposal duration.

**Admission policy** (`reject_on_inflight_window_full: bool`, internal config, not exposed via CLI):

- **`false` (default)** — If no permit is available, the caller blocks on `acquire().await` until a permit is freed. This eliminates client-side reject-retry storms under contention (e.g., window=1 with 16 writers). Wait time and queue depth are tracked as metrics (`inflight_total_enqueued`, `inflight_total_wait_us`, `inflight_queue_depth`).
- **`true`** — If no permit is available (`try_acquire` fails), the leader immediately returns `Busy` — a retryable error. No queuing. Used only in tests that need to verify fail-fast behavior.

**Multi-queue routing** (`--inflight-queues`, default 1): The window is split across N semaphores, each sized `ceil(max_inflight / N)`. Proposals are routed round-robin. Multiple queues reduce semaphore contention under high concurrency without affecting correctness (each slot is an independent Paxos instance; admission ordering does not influence consensus safety).

---

## 5. Pipelined Fanout

As soon as the leader has fsynced its own copy of slot N, it sends `Accept(N, ...)` to all followers. It does **not** wait for slot N-1, N-2 to reach quorum first.

**Transport: shared per-peer `crowdb-rpc` connection.** Each Accept is a unary
request correlated by `RpcClient`, so concurrent slots may remain in flight
without a dedicated application-level stream.

**Per-follower flow control.** `crowdb-rpc` uses the configured bounded send
queue plus a bounded overflow queue drained on writable events. Terminal
queue pressure surfaces as `PxReplicaError::Backpressure`; the proposal retry
budget applies backoff without invalidating the healthy connection.

**Quorum bookkeeping.** For each in-flight slot, the leader keeps a small bitmap of which peers have `Accepted` it. As soon as a majority is reached (counting itself), the slot transitions to `Chosen`.

**Out-of-order `Accepted` and `Chosen` notifications are fine.** A follower may respond `Accepted(N+2)` before `Accepted(N)`. Each is processed independently. A follower may receive `Chosen(N+2)` before `Chosen(N)` and apply only the parts safe to apply (per-key tracking handles this).

**Zero-copy accept path.** `Acceptor::accept` and `ReplicaHandler::on_accept` take `&PxLogEntry` instead of owned `PxLogEntry`. The caller (proposer's `run_accept_phase`, crowdb-rpc `handle_accept_inner`, WAL replay) passes a reference; no clone for the acceptor call. The only clone is inside `inner_accept` for `cas_accepted`, where the slot node must own its copy. `WALRecord::from_accepted` already borrows, so the WAL encode after accept is also zero-copy. With `Bytes` payloads these clones were O(1) ref-count bumps; the signature change makes the zero-copy intent explicit and avoids redundant bumps.

---

## 6. Per-Key Resolved-Slot

Each learner maintains, per key it has applied, the highest slot that has touched that key. This enables:

- **Read-your-writes** ([§6.2 of design-crowdb-kv.md](design-crowdb-kv.md#62-read-your-writes-follower-read)): a follower can serve `Get(k, slot=N)` as soon as `resolved_slot[k] ≥ N`.
- **Linearizability** ([§14](#14-parallel-slot-linearizability-analysis)): per-key tracking makes "highest slot wins" correct under out-of-order apply.

**Update rule.** On `apply(slot, batch)`: for each `(k, op, v?)` in the batch, if `slot > resolved_slot[k]`, update `resolved_slot[k] = slot` and write/tombstone `v`. Otherwise drop. Then advance `max_applied` and the contiguous-applied frontier.

---

## 7. Safe-Slot Computation and Propagation

The safe-slot is the cluster-wide **contiguous applied frontier**: the maximum N such that every slot ≤ N has been chosen and applied on every learner.

**Aggregation.** The leader collects per-learner `contiguous_applied` reports piggy-backed on heartbeat responses. The safe-slot is recomputed at the end of each quorum heartbeat round:

```
   safe_slot = min(contiguous_applied[learner])  for learner in voting members
```

A peer that has never reported is treated as `0`, so the safe-slot only rises once *all* voting members are heard from. The safe-slot is monotonic within a tenure (`fetch_max`); a new leader resets it to `0` and re-establishes it from fresh heartbeats.

**Propagation.** The leader includes `safe_slot` in heartbeats, write responses, read responses, and the describe-cluster RPC.

`Scan(Linearizable)` uses the leader's *own* contiguous frontier rather than the safe-slot. It is strictly ≥ safe-slot at all times ([§6.4 of design-crowdb-kv.md](design-crowdb-kv.md#64-scan-modes)).

---

## 8. Gap Detection

A "gap" is a slot N < max-chosen-slot for which the leader has no `Chosen` decision yet. The leader's learner maintains `contiguous_chosen` (highest gap-free chosen slot) and `last_chosen_slot` (highest slot ever seen chosen, gaps allowed). A gap exists when `contiguous_chosen < last_chosen_slot`.

**Repair mechanism:**

1. **Opportunistic repair.** After each heartbeat round in the leader-state loop, `repair_once()` is called. It finds the lowest gap (`contiguous_chosen + 1`) and runs classic Paxos (Phase 1 + Phase 2) to close it. A no-gap leader returns immediately without any RPCs.
2. **On leader change:** the new leader runs bulk Phase 1 over `[contiguous_chosen+1, ceiling]` as a one-shot sweep (see [`design-crowdb-kv-leader-election.md`](design-crowdb-kv-leader-election.md) §4).

There is no separate repair task with its own tick or concurrency cap; repair is interleaved with heartbeats at the heartbeat cadence.

---

## 9. Gap Repair via Classic Paxos

Once a gap is selected, repair runs full classic-Paxos for that slot:

1. **Pick a fresh ballot.** `(round, leader_id)` with `round = max_seen_round + 1`.
2. **Phase 1 — Prepare.** Send `Prepare(slot, ballot)` to all acceptors.
3. **Wait for quorum of `Promise`s.** Each carries the highest `(ballot', value')` previously accepted, if any.
4. **Choose a value.** If any `Promise` returned an accepted value, re-propose the one with the highest `ballot'`. If none, propose `NoOp`.
5. **Phase 2 — Accept.** Send `Accept(slot, ballot, value)`. Wait for quorum.
6. **Chosen.** Broadcast `Chosen(slot)` and apply locally.

The repair never invents a fresh user value — if any acceptor had a half-baked accept, that value is re-chosen. Repair touches one slot at a time; the hot path on other slots is unaffected. A repair on slot N does not block new writes at slot M > N.

---

## 9A. Follower-Side Apply and Catch-up

The leader-side repair in §9 closes gaps in the leader's *chosen* frontier. Followers face a symmetric but distinct problem: they learn about chosen slots via `ChosenNotice` (a fire-and-forget notification the leader broadcasts after quorum), and they may miss or receive stale versions of those slots. This section covers the follower-side apply path and catch-up mechanism.

### 9A.1 Accept Path Stores Only

A follower's `handle_accept` stores the accepted value in its acceptor and persists it to the WAL, but does **not** advance `known_commit_slot` or wake the apply loop. Accept is a storage event, not a commit event. The leader may crash before reaching quorum, in which case the accepted value may never be chosen (or may be overwritten by a higher-ballot value during new-leader recovery).

Advancing the apply frontier on Accept would let a follower apply an un-chosen value, violating Paxos safety, the same class of bug as applying before `leaderCommit` in Raft.

The one exception is the new-leader transition: after a follower wins election and completes bulk Phase 1 over `[contiguous_chosen+1, ceiling]`, it advances `known_commit_slot` to its new `contiguous_chosen` and wakes the apply loop. This is safe because bulk Phase 1 has resolved (chosen or NoOp-filled) every slot in that range under the new leader's ballot.

### 9A.2 ChosenNotice — Ballot-Verified Out-of-Order Apply

When the leader chooses a slot, it broadcasts a `ChosenNotice` carrying `(slot, term, leader_id, ballot_round)` to all followers. The follower's ChosenNotice handler:

- If the follower's acceptor has an accepted value at `slot` with `ballot == chosen_ballot` → the value matches what was chosen. The follower calls `update_chosen_frontier` and wakes the apply loop. Apply can proceed out-of-order; the per-key slot tracking in §3 makes this safe (highest slot wins per key).
- If the follower's accepted ballot at `slot` is **lower** than the chosen ballot (stale) → the follower has an outdated value. It records a gap for `slot` (driven by FetchGap, §9A.3) and does not apply.
- If the follower has no accepted value at `slot` → it records a gap and does not apply.

The `ballot_round` field in the ChosenNotice flatbuffer enables this verification. Without it, the follower cannot distinguish a fresh chosen value from a stale re-delivery.

### 9A.3 Follower-Driven FetchGap Catch-up

When a follower detects a gap (missing or stale slot in the chosen range), it sends a unary `FetchGap(slot)` request to the leader. The leader:

- Has completed leader recovery and retains the value within its contiguous
  chosen prefix → replies with the full entry (payload + ballot + term).
- Otherwise → returns pending/not found. Normal leader recovery or independent
  gap repair must establish the chosen value before a later request can fetch it.

On receipt, a durable follower first writes the original Accepted entry to its
WAL, then installs it, advances the chosen frontier and wakes the apply loop.
Historical entries retain their original election term; a newer local term
does not reject committed history. Failed persistence leaves the gap pending.
Learning a proven chosen value does not answer a new Accept request and must
not discard a later promise. It retains the original entry alongside that
higher promise; an already higher accepted ballot must carry the same value.
Replay rebuilds highest accepted values before restoring promise fences, so
the physical order of historical fetch and later promise records cannot lose
either durable fact. Accepted history alone is never a FetchGap chosen proof.
`MAX_INFLIGHT_FETCHGAP` (default 16) bounds the entire fetch/persist/install
operation, including error and cancellation paths.

This replaces the previous leader-driven catch-up that ran inline in `run_heartbeat_round` (up to 64 synchronous `send_accept().await` calls per lagging follower). That inline catch-up could exceed `heartbeat_interval` and trigger spurious elections. The heartbeat round is now pure liveness + lease: one RPC round-trip, no catch-up work.

### 9A.4 Snapshot Fallback

If a follower's gap count exceeds `catchup_snapshot_threshold` (default 1024),
it records the large backlog and continues FetchGap under the same fixed
16-request limit. A missing automatic snapshot path must not disable catch-up.

The bounded snapshot protocol is used only while bootstrapping a fresh,
unpublished member. It does not replace the engine of a running follower and
is not invoked by this diagnostic threshold. A live replacement requires a separate
atomic engine-generation publication design.

A recovered durable engine's gap-free resume watermark remains authoritative
when WAL reclamation removed the boundary record or all preceding segments.
Restore retains that complete chosen/applied prefix without capping it to the
remaining WAL tip. If the exact boundary term is unavailable, its historical
term is reported as zero (unknown); voting freshness still uses the durable
acceptor log tip. Higher retained entries replay after the snapshot prefix.

### 9A.5 Apply Loop

The follower's apply loop targets `max(known_commit_slot, last_chosen_slot)`. For each slot in the apply range:

- If `slot ≤ known_commit_slot` → apply (committed by definition).
- Else if `learner.is_chosen(slot)` → apply (out-of-order chosen slot).
- Else (accepted but not chosen) → skip and record a gap for FetchGap.

This ensures accepted-but-not-chosen slots are never applied, while allowing out-of-order chosen slots to apply immediately without waiting for lower slots to be resolved.

---

## 10. Timing Diagrams

### 10.1 Best case — fully pipelined window

```
  time →

  slot N    : assign─ fsync─ Accept ──► quorum ──► Chosen ─ apply ─ ack
  slot N+1  :         assign─ fsync─ Accept ──► quorum ──► Chosen ─ apply ─ ack
  slot N+2  :                 assign─ fsync─ Accept ──► quorum ──► Chosen ─ apply ─ ack

  end-to-end latency for slot N      = fsync + RTT + apply
  per-slot incremental latency       ≈ batched-fsync amortization
  steady-state throughput            = window × (1 / RTT) when network-bound
                                     = disk_bw / record_size when disk-bound
```

### 10.2 Gap repair (slow follower)

```
  time →

  slot N    : assign─ fsync─ Accept ──► follower-A: Accepted
                                      │ follower-B: ...........(slow)
  slot N+1  :       assign─ fsync─ Accept ──► quorum ──► Chosen ─ apply ─ ack(N+1)
  slot N+2  :              assign─ fsync─ Accept ──► quorum ──► Chosen ─ apply ─ ack(N+2)

  ... after heartbeat round ...

  repair    : Prepare(N, ballot') ─► quorum of Promises
                                  │  Promise from leader: Accepted(ballot, v)
              Accept(N, ballot', v) ──► quorum ──► Chosen(N)
              apply(N) on leader, advance contiguous_applied to N+2
```

### 10.3 Leader change

```
  time →

  old leader: ...slot N-1: Chosen, applied
              slot N:   Accepted on 1 follower only (no quorum)
              slot N+1: Accepted on 0 followers
              <crash>

  new leader (term T+1):
              Prepare((T+1, me)) over [N, N+1, ..., max_chosen]
              Promises:
                slot N   : (older_ballot, v_N) from one follower
                slot N+1 : empty
              re-Accept((T+1,me), v_N) at slot N    → Chosen
              Accept((T+1,me), NoOp)   at slot N+1  → Chosen
              steady state resumes
```

---

## 11. Interaction with Snapshot and WAL GC

Acceptor and WAL reclamation share a certificate distinct from the commit
frontier and voting-member `group_safe_slot`:

- The leader takes the minimum contiguous-applied frontier of itself and every
  configured remote learner, including non-voting learners. Unknown progress
  is zero; replies arriving after quorum still update progress.
- It caps that frontier by its local durable snapshot and the greatest durable
  snapshot of a voting peer. A singleton voting group needs only its own durable
  snapshot, while any attached non-voting learners still constrain progress.
- Heartbeats carry `gc_before_slot` and `gc_membership_epoch`. A follower accepts
  the certificate only from its known voting leader in the current term and
  membership epoch. The retained certificate binds these identities and the
  watermark atomically; a later epoch or term cannot relabel an old certificate.
- Each replica caps its certificate by its own contiguous-applied and durable
  snapshot frontiers before reclaiming. A missing WAL/snapshot yields zero.
- Maintenance trims acceptor entries with `slot < gc_slot` and unlinks only WAL
  segments wholly below that boundary. It retains the boundary slot. Outstanding
  readers retain node/payload owners until they finish.

A new group or leader tenure clears progress evidence. Membership replacement
also clears evidence, including when it adds a non-voting learner. A joining
replica that needs a prefix already reclaimed uses snapshot catch-up.

See [WAL garbage collection](design-crowdb-kv-wal.md#7-garbage-collection).

---

## 12. Tunables and Defaults

| Parameter                        | Default | Where it lives                                                        |
| -------------------------------- | ------- | --------------------------------------------------------------------- |
| `max_inflight_proposals`         | 32      | `PaxosConfig` (total semaphore permits)                               |
| `reject_on_inflight_window_full` | `false` | `PaxosConfig` (internal, not CLI-exposed)                             |
| `coalesce_max_keys`              | 32      | `PaxosConfig` (max ops per coalesced batch; 0 = off)                  |
| `coalesce_drain_threshold`       | 1       | `PaxosConfig` (skip drain at this inflight count)                     |
| `max_paxos_retries`              | 3       | `PaxosConfig` (per-slot Phase-2 retries)                              |
| `max_slot_retries`               | 3       | `PaxosConfig` (new-slot retries before giving up)                     |
| `retry_base_backoff_ms`          | 5       | `PaxosConfig` (exponential backoff base)                              |
| `bulk_prepare_window`            | 1024    | `PxElectionConfig` (bulk Phase-1 batch size)                          |
| `catchup_snapshot_threshold`     | 1024    | `PxElectionConfig` (diagnostic threshold; bounded FetchGap continues) |

Bulk Phase 1 recovers independent slots concurrently, with at most
`bulk_prepare_window` complete Prepare/Accept operations in flight. It starts
at the new leader's own contiguous frontier, adopts quorum values and opens
reads only after the entire observed prefix has resolved and applied. Tenure
cancellation prevents prepared slots from proceeding to Accept.

Steady-state gap repair starts only after initial recovery has completed;
the two paths never compete for the same slot. Each background pass drains
its observed backlog independently of heartbeat cadence. Follower-side gaps
use at most `MAX_INFLIGHT_FETCHGAP` (default 16) concurrent requests, including
backlogs above the snapshot diagnostic threshold.

---

## 13. Correctness Analysis for Parallel Slot Writes

**Key insight:** Parallel slot writes are safe because we only support **blind operations** (`Put`, `Delete`). No operation reads current state before writing. The final value of each key is determined solely by the highest slot that touched it.

- **Consensus phase (parallel):** Each `PxSlot` is an independent Paxos instance. They can be decided in any order.
- **Apply phase (per-key slot tracking):** `PxLearner`s store `(slot, value)` per key and only accept writes where `slot > current_slot`. Regardless of apply order, the highest slot wins.
- **Gaps don't block point reads:** An undecided slot 3 does not block `Get(k)` if slot 5 for key `k` is already applied. Gaps only block cross-key scans and WAL truncation.
- **Batches:** Intra-batch order is **as written by the client**. A batch uses the batch's slot for each key.

This would NOT be safe for `CAS` or `Increment`, which read current state. Not supported.

---

## 14. Parallel-Slot Linearizability Analysis

**Premises:** (1) Only blind ops. (2) Leader serializes slot assignment. (3) Quorum durable-flush before ack. (4) Per-key `(slot, value)` tracking, highest slot wins. (5) Leader reads fenced by lease or ReadIndex.

**Claim:** The assigned slot number is a valid linearization point for every op.

**Sketch:**

- **Real-time order → slot order.** If `ack(A)` completes before `invoke(B)`, the counter has already advanced past `slot(A)`, so `slot(A) < slot(B)`.
- **Blind apply-order independence.** An undecided earlier slot writing *k* will be ordered earlier and immediately overwritten by the later slot's value.
- **Durability before visibility.** Quorum durable-flush before ack ensures a client-observed write cannot be lost by leader change. Gap repair re-chooses the same value.
- **Leader read correctness.** The leader's learner has applied slot N before acking. A subsequent `Get(k)` reflects the highest chosen slot writing *k*.
- **Follower read correctness.** Read-your-writes uses per-key resolved-slot; bounded-stale uses the gap-free `safe-slot`.

**Single remaining cost — linearizable `Scan`.** Must wait for the leader's **contiguous applied frontier** to cover the target. Latency is bounded by (window size) × (slot-resolution time). Mitigation: `Scan(mode = SafeSlot)` or `Scan(mode = AtSlot(N))`. Point `Get`s bypass gaps entirely.

**Why `CAS`/`Increment` would break this:** Read-modify-write ops must read current value, which is unknown until all earlier slots for that key are resolved. Would require sequential consensus or per-key dependency tracking. Neither is in scope.

**Implementation invariants:** Slot assignment is single-point. `Accepted` not sent before durable flush. Classic-Paxos recovery never discards accepted values. Lease/ReadIndex fencing on every linearizable read. Per-key slot comparison atomic with write.

---

# Part B: Acceptor Retention

## 15. Acceptor Storage Requirements

Per-slot promised ballots and accepted entries must support sparse, concurrent
Prepare/Accept operations while maintenance reclaims a proven-safe prefix.
Lookups must retain the node before accessing its fields. Reclamation must
support overlapping maintenance callers during group replacement.

## 16. Retained Slot Entries

`PxAcceptor` owns a `crossbeam_skiplist::SkipMap<SlotIndex, PxSlotNode>`, a
monotonic `trim_slot`, and a monotonic `highest_seen_slot`. Each operation keeps
the returned map `Entry` alive throughout field access. Removing an entry does
not invalidate an outstanding retained entry. Sparse slots allocate individual
nodes; absent slot ranges allocate nothing.

## 17. Per-Slot State

`PxSlotNode` contains atomic promised and accepted pointers. Replaced pointers
enter per-node retired lists and are freed only when the node is dropped. This
keeps a field read valid for the entire retained node lifetime. Returned log
entries clone their `Bytes` owner, so a caller may keep a payload after trim.

## 18. Lookup and Reclamation

- `open_slot` rejects a slot below the trim boundary, obtains or inserts a
  retained node, then checks the boundary again. An insertion racing trim is
  removed and rejected instead of resurrecting old history.
- Prepare/Accept keep the entry guard while reading or changing node fields.
  Queries and ordered range scans clone values while retaining their entries.
- `trim` advances the boundary with `fetch_max`. `reclaim` removes entries below
  that boundary; concurrent removers can safely encounter the same entry.
- A lookup already in progress may complete with its retained state. A newly
  admitted request below the boundary is rejected. The boundary is exclusive.

## 19. Acceptor API

- `prepare`, `accept`, `accepted_at`, `promised_at`: per-slot protocol access.
- `accepted_iter_range`: ordered retained-history view; missing/trimmed slots
  are absent. `JournalScan` reports a GC gap for a start below `trim_slot`.
- `accepted_log_tip`: highest retained accepted slot and its term.
- `highest_seen_slot`: highest slot ever opened, independent of reclamation.
- `trim`, `reclaim`, `trim_slot`, `retained_slots`: reclamation and observability.

`ReplicaStatus.acceptor` exposes the exclusive trim boundary and retained slot
count. The count describes map entries, not payload bytes or process RSS.

## 20. Retention Invariants

- **R1 — Retained access:** every node field access holds a live map entry.
- **R2 — No resurrection:** a request admitted below the current trim boundary
  cannot create visible accepted history.
- **R3 — Durable local recovery:** reclamation cannot exceed the local durable
  snapshot or contiguous-applied frontier.
- **R4 — Learner coverage:** the leader certificate cannot exceed the least
  applied frontier of any configured learner, including non-voting learners.
- **R5 — Certificate identity:** followers use only the certificate bound to
  their current membership epoch, term and known voting leader.
- **R6 — Independent payload lifetime:** reclaiming a node does not invalidate
  a log entry already cloned by a caller.

## 21. Storage Cost

The ordered map adds per-node allocation and lookup overhead. Reclamation walks
only the eligible prefix and releases historical payload owners as readers
finish. No throughput improvement follows from this representation alone;
measure request throughput, tails, retained slots and RSS under sustained load.

## 22. Retention Limits

A lagging configured learner or unavailable durable witness can hold retention
at zero. An operator must restore/catch up that learner or change membership;
maintenance cannot discard history merely to satisfy a memory target. A slot
with repeated ballot/value changes retains its old field versions until that
slot is reclaimed. Long-lived readers may delay physical release after logical
removal. The generic `PxSlotList` remains a separate utility and is not the
acceptor's reclamation mechanism.

## 23. Server-side Proposal Coalescing

### 23.1 Problem

Each client `PUT`/`DELETE` is its own Paxos proposal: one slot, one
quorum RPC round, one WAL record, one learner apply. WAL batch
aggregation already amortizes `fsync` across concurrent proposals, and
the leader's local `fsync` is off the critical path, so the
remaining per-proposal fixed cost is the **quorum RPC round**. The
write-flow sweep shows throughput plateaus at ~29K (Intel) / ~48K
(M5 Pro) once the consensus pipeline saturates, independent of the
inflight window above MI=16. The bottleneck is the per-proposal quorum
RPC rate, not `fsync`.

The `Batch` payload format already supports multiple ops per slot and
`kv_batch_write` exposes it. Without server-side coalescing, concurrent
single-key proposes each take a distinct slot and pay the full quorum
round. The coalescer (enabled by default, `coalesce_max_keys = 32`)
event-batches these into one multi-key Paxos proposal per batch.

### 23.2 Drain Threshold

The first op starts a 1-op slot-task **immediately** (no timer). Ops
arriving during the round join a pending batch. On round completion,
`coalesce_drain_after_round` checks the in-flight slot-task count
(`occupied`, permits held) before taking the pending batch. If
`occupied >= coalesce_drain_threshold`, the drain is skipped; the
`max_keys` overflow path handles high load with full batches. If
`occupied < coalesce_drain_threshold`, the pending batch is taken and
the next slot-task is started (if non-empty) or the coalescer goes
idle (if empty).

At high load, drains almost never fire (many slot-tasks in flight);
the `max_keys` overflow path produces full batches. At low load,
drains always fire (few slot-tasks, threshold not exceeded), so there
is no latency floor; a lone op flushes immediately. Default
`coalesce_drain_threshold = 1` (see §23.5).

### 23.3 Coalescer Design

Per-group state on `PxGroup`:

- `coalescer: parking_lot::Mutex<Option<PendingBatch>>` — the
  accumulating batch, `None` when idle.
- `self_weak: OnceLock<Weak<PxGroup>>` — set once the group is wrapped
  in `Arc`, so spawned slot-tasks can upgrade without holding a strong
  self-reference.
- `coalesce_last_activity_us: AtomicU64` — last coalescer activity
  timestamp (updated on every enqueue and drain). Used by the
  activity-based watchdog.

The coalescer mutex is retained because it protects one small batch mutation
and is released before slot proposal, RPC, or engine work. An MPSC owner task
would remove producer contention but add scheduling and queue latency to every
write; profiling must show this critical section is material before making that
trade. Peer applied/durable maps and follower gap tracking also retain their
short locks: replica sets are normally 3–7 members and gaps are exceptional,
so their bounded critical sections do not justify per-peer atomics or another
concurrent map.

`PendingBatch` holds:
- `op_bodies: Vec<u8>` — concatenated op bodies (each single-op
  payload's leading count bytes dropped; op bodies are self-delimited).
- `op_count: u16` — number of ops accumulated.
- `identities: Vec<RequestIdentity>` — one `(client_id, seq)` request
  identity per client operation, all mapping to the shared result slot.
- `waiters: Vec<oneshot::Sender<ProposeResult>>` — one per coalesced
  caller; each receives the shared `ProposeResult` on flush.
- `timer: Option<JoinHandle<()>>` — reserved (unused in event mode;
  the watchdog is activity-based, not per-batch).

`propose` flow (refactored into `propose` + `propose_inner`):

1. Leadership gate (as before).
2. Request-result lookup — a hit returns the cached slot
   immediately, never enters a batch.
3. If `coalesce_max_keys == 0` (coalescing disabled) or `self_weak` is
   unset: call `propose_inner(payload, &[tag])` directly; bit-identical
   to the fallback path.
4. Else (coalescing on, default `coalesce_max_keys = 32`):
   `coalesce_enqueue(payload, tag)`:
   - Coalescer is `None` (idle) → create a batch with this op, start a
     1-op slot-task **immediately**. Open a fresh empty batch for ops
     arriving during this round.
   - Coalescer is `Some` (batch exists) → append op body + tag +
     waiter. If `op_count >= max_keys` → overflow: take the batch,
     spawn a concurrent slot-task, open a new empty batch.
5. `await` the waiter's `ProposeResult`.

`coalesce_flush_batch` (called by overflow and drain):

1. Build merged payload: `[op_count as u16 LE][op_bodies]`.
2. Open a fresh empty batch in `coalescer` (for ops during the next
   round).
3. `tokio::spawn` `propose_inner(payload, tags)`. On completion, fan
   the `ProposeResult` (now `Clone`) to all waiters, then call
   `coalesce_drain_after_round`.

`coalesce_drain_after_round` (called after each slot-task finishes):

1. Touch activity timestamp.
2. If `occupied >= coalesce_drain_threshold` → return (skip drain;
   overflow handles it).
3. Take the pending batch from `coalescer`.
4. If `op_count == 0` → go idle (return).
5. If `op_count > 0` → `coalesce_flush_batch(batch)` (start next round).

**Watchdog**: A single background task per group sleeps 1000ms, then
checks if there's been no coalescer activity for 1000ms. If so, it
flushes any stuck non-empty batch. Safety net for edge cases (drain
panic, spawn failure). Zero overhead during normal operation.

### 23.4 Request Identity Threading

A coalesced batch carries K client request identities but one result slot.
The identities stay with the leader's coalescer waiters and are published to
the leader-local request-result cache only after that value is chosen. They
are not part of `PxLogEntry`, the WAL, or `FBAcceptRequest` because an
Accepted value is not necessarily chosen and followers cannot safely publish
a result from Accept alone.

`Learner::learn` accepts a slice of `RequestIdentity` values. Normal leader
proposals pass their identities; repair, election, follower notification, and
restore paths pass an empty slice. The cache is lost on leader change or
restart, at which point ordinary data-idempotent operations may be proposed
again.

### 23.5 Config

`PaxosConfig` gains:

- `coalesce_max_keys: usize` — max ops per batch (cap 65535, the
  payload count field is `u16`). Default `32`; `0` disables coalescing.
- `coalesce_drain_threshold: usize` — skip drain when in-flight
  slot-task count >= this. Default `1` (the last finisher always
  drains). `0` = always drain (pure event mode); higher values skip
  the drain at high load so the `max_keys` overflow path produces full
  batches.

CLI: `--coalesce-max-keys` on `crowdb-kv-server`, applied in `main.rs`
into `config.paxos`. Wired into the group via `set_from_config` (the
coalescer reads `self.config.paxos.*`).

### 23.6 Correctness

- **Request replay**: every coalesced identity is recorded against the shared
  slot on the leader after choice. Same-leader retries return that slot;
  retries after leader change or restart may re-propose ordinary mutations.
- **Per-key ordering**: unchanged; all ops in a batch share one slot;
  across batches, per-key highest-slot-wins applies as before.
- **`ProposeResult::Chosen { slot }` contract**: every coalesced
  waiter receives the same slot. `ProposeResult` gains `Clone`.
- **`coalesce_max_keys = 0`**: `propose` calls `propose_inner` with a
  one-identity slice and records the same leader-local result after choice.
- **Leadership**: re-checked inside `propose_inner`; a step-down
  between batch collection and flush surfaces as `NotLeader` to all
  waiters.
- **Shutdown**: slot-tasks hold a `Weak<PxGroup>`; on shutdown the
  pending batch is dropped (waiters get a closed oneshot → mapped
  to `Err`).
- **Drain threshold liveness**: the inflight permit is released (on
  permit drop in `propose_inner`) **before** `coalesce_drain_after_round`
  is called. So the last finisher always sees `occupied = 0 < threshold`
  and takes the batch. The batch is never stuck. The 1000ms watchdog
  is a backstop for edge cases (drain panic, spawn failure).

### 23.7 Benchmark Results (10s mem mode, 3-node cluster, max_keys=32)

| Threads | Baseline TPS | Timer TPS | Event TPS | Drain TPS | Timer WAL | Drain WAL |
|---|---|---|---|---|---|---|
| 32 | 27,787 | 33,029 | 48,346 | 47,485 | 31,090 | 139,404 |
| 64 | 28,062 | 64,145 | 68,201 | 68,741 | 60,498 | 106,926 |
| 128 | 28,260 | 97,554 | 86,759 | 101,537 | 92,752 | 101,350 |
| 256 | 27,804 | 113,671 | 97,865 | 118,377 | 110,034 | 111,944 |

Drain beats Timer at high load (128: 102K vs 98K, 256: 118K vs 114K) with
WAL counts close to Timer. At 64 threads it matches Event mode and beats
Timer. At 32 threads it matches Event mode (no regression). The default
threshold of 1 still allows drains at low load, preserving the zero-
latency-floor behavior.
