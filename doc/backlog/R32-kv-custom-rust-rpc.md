<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R32: kv — KV Server and Core Library Review

**Problem**

R32 originally migrated the KV replica-to-replica consensus path from
tonic/gRPC to flatbuffer messages over `crowdb-rpc`. That migration is
complete: `PxRpcService` and `PxRpcTransport` carry consensus
traffic, the server registers consensus and client-facing handlers on the
same `RpcServer`, and production `crowdb-kv` / `crowdb-kv-server` code no
longer depends on tonic or prost.

The original unfinished acceptance list is no longer a sound contract:

- A mixed gRPC/`crowdb-rpc` rollout is obsolete after removal of the gRPC
  implementation. Reintroducing the old server only to test migration would
  add dead compatibility code.
- `NotLeaderHint` is a client-facing KV routing concern covered by R117, not a
  replica-to-replica Paxos transport response.
- Comparing against an unavailable legacy binary cannot be a repeatable
  regression gate. The cited read-throughput loss also cannot be attributed
  solely to the consensus RPC path without holding CPU, connection count,
  storage, and workload constant.
- The old file describes separate ports, a tonic-era `LearnerStream`, and
  server-streaming snapshot machinery that no longer match the unified
  server and follower-driven `FetchGap` architecture.

The useful remaining work is a cross-cutting review of the current
`crowdb-kv` library and `crowdb-kv-server` application. The code has evolved
across consensus, coalescing, CAS, WAL, recovery, membership, RPC, management,
and observability changes. Reviewing each change in isolation does not prove
that their boundaries preserve the system invariants under failover and
load.

The initial review has already identified concrete examples of boundary
risk:

- The code calls its client-request replay cache `dedup`, which is easily
  confused with data or WAL deduplication. Permanent docs promise replication
  and restart recovery, while implementation is leader-local/in-memory and
  still carries unused tag fields through parts of Accept. The required retry
  boundary is unclear and the unused plumbing adds hot-path complexity.
- The RPC sender already defers a full primary send queue into a 256-entry
  lock-free overflow queue and drains it first on writable events. Only when
  both queues are full does it return `SendQueueFull`. KV currently
  invalidates the selected connection generation for that terminal pressure
  result even though the connection remains healthy, turning extreme bursts
  into reconnect churn.
- Transport failures and outer RPC deadlines collapse into
  `PxReplicaError::Internal`, preventing callers and metrics from
  distinguishing backpressure, timeout, and connection failure.
- The Chosen RPC integration test checks `accepted_at` after Accept already
  populated it, so it does not prove that Chosen advances the learner.
- Migration-era comments, broad `dead_code` allowances, duplicate server
  state, and stale port/stream descriptions obscure the runtime ownership
  model.

These issues can affect retry behavior, overload behavior, failure diagnosis,
and confidence in recovery. The root architecture is
documented in `doc/design/kv/design-crowdb-kv.md`,
`doc/design/kv/design-crowdb-kv-rpc.md`,
`doc/design/kv/design-crowdb-kv-slot.md`, and
`doc/design/kv/design-crowdb-kv-wal.md`.

Concrete scenarios include:

- A client times out and retries an ordinary write against the same leader.
  The leader should suppress the immediate replay without consuming another
  slot. If leadership changed, re-proposing Put/Delete/Batch is data-idempotent
  and may be an acceptable simpler contract.
- A peer's RPC send queue fills while the TCP connection remains healthy.
  The caller must apply bounded backpressure without discarding the healthy
  pool and reconnecting repeatedly.
- A replica times out or disconnects during Prepare or Accept. The failed
  connection generation must be invalidated, quorum progress must remain
  bounded, and a later request must recover on a replacement connection.
- A process restarts with accepted, chosen, and applied frontiers at different
  points. WAL replay, gap recovery, and election must converge without serving
  a stale linearizable read or losing an acknowledged mutation.
- A store is created, restored, reconfigured, and shut down through the server
  and management paths. RPC ownership, advertised endpoints, background
  tasks, and persisted membership must agree throughout the lifecycle.

**Solution**

Treat the gRPC replacement as completed history and perform a bounded,
evidence-driven review and remediation of `lib/crowdb-kv` and
`app/crowdb-kv-server`. Preserve the current `crowdb-rpc` architecture; do not
restore a legacy transport or add compatibility switches.

Use risk-based disposition while reviewing:

- Fix a finding directly when the change is local, its intended semantics are
  already established, it does not introduce a lock or alter a protocol /
  durability contract, and a focused regression test can prove it.
- Record a finding as a task when it spans components, requires fault or
  performance evidence, or cannot be completed and verified as one small
  coherent change.
- Present design defects and alternatives for human agreement before changing
  architecture. This includes retry ownership, metadata durability, RPC
  isolation, acknowledgement semantics, and new synchronization.
- Treat KV performance as a correctness-adjacent constraint. Flag allocation,
  copying, lock contention, task fan-out, queueing, reconnect churn, cache-line
  contention, blocking work, and extra WAL/RPC operations when found. Compare
  performance only with the same CPU allocation, host, configuration, data,
  and workload, and use stage latency/counters to explain a delta.

The review preserves these invariants:

- The selected request-replay contract is explicit. A cached result is never
  published before its value is chosen, and conditional-write ambiguity is
  returned as `OutcomeUnknown` for read reconciliation rather than hidden by
  automatic replay.
- Ballot, term, membership epoch, chosen, durable, and applied frontiers never
  regress and are not conflated.
- An acknowledged write satisfies the configured durability contract before
  success is returned.
- Backpressure is bounded and does not masquerade as connection failure.
  Actual connection failure invalidates only the generation used by the
  failed request.
- No blocking storage operation or contended lock is introduced into a hot
  async/RPC path. Any existing hot-path lock retained by the review has a
  documented owner, critical section, and measurement-based justification.
- Startup, reconfiguration, recovery, and shutdown have one clear owner for
  each RPC server and background task and remain idempotent.
- Status and metrics distinguish queue pressure, in-flight work, transport
  failure, consensus delay, WAL delay, and apply delay without inventing
  unsupported queue semantics.

Numbered work items:

1. **Consensus and request-replay correctness** — replace ambiguous `dedup`
   terminology with request identity/result-cache terminology and define the
   minimum required replay boundary. The selected low-complexity contract is
   a bounded, leader-local, in-memory cache for immediate ordinary-write
   retries. Leader change/restart may re-propose data-idempotent Put/Delete/
   Batch; ambiguous CAS remains `OutcomeUnknown` plus read reconciliation.
   Remove follower/WAL tag plumbing if it is not required by the agreed
   contract. Separately verify ballot, term, epoch, Chosen, and frontier fences.

2. **WAL, recovery, and acknowledgement contract** — trace a mutation from
   admission through local/remote Accept, WAL persistence, chosen publication,
   engine apply, snapshot, replay, and garbage collection. Fix any path where
   acknowledgement can outrun its configured durability guarantee, replay can
   reconstruct a different frontier, or a failure can strand progress.

3. **RPC transport and overload behavior** — review connection-pool
   generation handling, request deadlines, queue saturation, fire-and-forget
   completion, malformed frames, snapshot transfer, and retry ownership.
   Keep the existing primary plus overflow queues and writable-event drain.
   Preserve healthy connections on terminal `SendQueueFull`; invalidate failed
   generations only on connection errors. KV-server-to-KV-server sends own
   bounded deferred retry until success, deadline, or connection failure, so
   an accepted internal message is not silently lost. Expose distinct
   transport outcomes to callers and metrics.
   Remove avoidable payload and metadata copies on measured hot paths.

4. **Concurrency and hot-path review** — inventory locks, atomics, channels,
   semaphores, spawned tasks, and blocking calls in proposal, read, apply, WAL,
   RPC, watch, and status paths. Check ordering, cancellation, lost-wakeup,
   starvation, and shutdown behavior. Replace or narrow a lock only when the
   call path and contention evidence justify it; any newly required lock is a
   separate human decision under the repository rules.

5. **Server lifecycle, recovery, and management review** — review
   `crowdb-kv-server` startup, port advertisement, store/group creation,
   persisted topology reconciliation, dynamic membership, background
   monitors, operation tracking, and graceful shutdown together with
   `PxKvStore` ownership. Consolidate obsolete migration-era state and ensure
   runtime-created and restored stores receive identical configuration and
   RPC wiring.

   Consensus and client RPC continue sharing the same `RpcServer` and worker
   pool. Review queue fairness and handler latency within that architecture;
   do not split listeners or dedicate separate worker pools.

6. **Observability and operational contract** — verify that existing status,
   counters, and latency histograms cover admission, consensus, RPC,
   persistence, apply, recovery, and maintenance with unambiguous names and
   units. Add only counters needed to diagnose a confirmed blind spot and keep
   collection off critical sections.

7. **Focused regression coverage** — strengthen tests at the boundary where
   each confirmed defect occurred rather than duplicating implementation
   tests. Include real three-replica RPC paths for failover and recovery,
   deterministic fault injection for timeout/backpressure/connection loss,
   and lifecycle tests for create/restore/reconfigure/shutdown.

8. **Cleanup and architecture reconciliation** — remove obsolete gRPC,
   LearnerStream, port-offset, phase, and blanket-dead-code residue after the
   relevant code is covered. Update permanent KV design documents only where
   the implementation review confirms that the architecture has changed.
   Record substantial out-of-scope discoveries as separate requirements
   rather than expanding R32 indefinitely.

   Snapshot streaming is split into R151. R32 records current single-frame
   pressure evidence but does not duplicate the R151 implementation.

**Dependencies**

- R104, R114, R115, R116, and R117 provide the current `crowdb-rpc` engine,
  FFI, and service migration patterns and are treated as landed.
- The review uses the current KV architecture documents and existing
  regression scripts. It does not depend on reconstructing a gRPC baseline.
- Changes inside `crowdb-rpc`, `crowdb-protocol`, or `crowdb-tree` are allowed
  only when needed to repair a boundary used by KV; broader redesign becomes
  a separate requirement.
- Unrelated chunkdb, diskdb, diskio, and console work is out of scope except
  where an integration test must exercise the public KV contract.

**Acceptance**

- Given an ordinary write that is chosen but whose response is retried against
  the same leader with the same request identity, when the retry arrives, it
  returns the original slot without another proposal/WAL entry. This proves
  bounded request-replay suppression at the selected boundary.
  Integration test.

- Given an ordinary write retry after leader change or restart, when the new
  leader lacks the prior cache entry, re-proposal leaves the final key state
  correct. Given an ambiguous conditional write, the client returns
  `OutcomeUnknown` and does not automatically re-execute it. This proves the
  simpler retry contract remains safe without replicated request metadata.
  Integration test.

- Given an accepted entry on a follower, when a matching Chosen notification
  arrives, its chosen frontier advances and the apply path makes the value
  readable; without the notification or another convergence signal the same
  assertion does not pass. This proves the test observes Chosen rather than
  Accept. Integration test.

- Given a live peer connection whose send queue is full, when consensus RPC
  submission returns `SendQueueFull`, the same connection generation remains
  cached, retry/backoff is bounded, and overload is reported distinctly from
  a connection error. This proves backpressure does not trigger reconnect
  churn. Unit test.

- Given a connection reset and a late failure from its old generation, when a
  replacement connection is installed, the old completion cannot evict the
  replacement and a subsequent RPC succeeds. This proves generation-safe
  recovery. Integration test.

- Given a consensus RPC that exceeds its deadline, when the call completes,
  the caller and metrics observe a timeout rather than an internal invariant
  error, and no request remains indefinitely pending. This proves timeout
  classification and bounded completion. Unit test.

- Given an Accept request after the replay contract is applied, when it is
  serialized and decoded, it carries no request-cache metadata that followers
  do not consume, and it does not allocate an extra full payload-sized
  temporary before the FlatBuffer copy. This proves wire simplicity and
  removes identified hot-path overhead. Unit test.

- Given WAL-backed writes followed by restart at each supported
  acknowledgement mode, when replay and gap recovery finish, acknowledged
  values, accepted/chosen/applied frontiers, and next-slot allocation match
  the pre-crash durability contract. This proves recovery consistency.
  Integration test.

- Given a store created with an ephemeral listen port and another restored or
  created through management, when peers are wired, each advertises and uses
  the actual bound endpoint and the same effective runtime configuration.
  This proves lifecycle wiring consistency. Integration test.

- Given repeated start, stop, join, reconfiguration, and shutdown sequences,
  when the server terminates, RPC servers and group background tasks have one
  owner, stop idempotently, and leave no live task or listener. This proves
  lifecycle ownership. Integration test.

- Given focused read, write, mixed, coalesced, CAS, and recovery workloads on
  the same host and CPU allocation, when the existing KV regression scripts
  run before and after a confirmed hot-path change, throughput and latency
  deltas are reported with RPC, consensus, WAL, and apply counters. Any
  material regression is explained or fixed; no comparison to the removed
  gRPC implementation is required. E2E test.

- Given the completed review, all confirmed critical/high findings are fixed,
  medium findings are fixed or split into an explicitly accepted follow-up,
  and obsolete migration allowances/comments are removed. This proves R32 is
  a bounded review rather than an open-ended cleanup item. Integration test.

Exact verification commands:

- `pixi run cargo test -p crowdb-kv`
- `pixi run cargo test -p crowdb-kv-server`
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy -p crowdb-kv -p crowdb-kv-server --all-targets -- -D warnings`
- `pixi run bash tools/bench-kv-read-regression.sh`
- `pixi run bash tools/bench-kv-write-regression.sh`
