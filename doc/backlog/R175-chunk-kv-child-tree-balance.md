<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R175: chunk-kv — Child-tree owner balance

## Problem

Splitting a hot partition creates child trees, but selecting a remote owner and
moving a child to correct owner-count or durable-byte imbalance is a different
operation. The source and target have different serving grants, ownership
epochs, process-local tree handles, memtables, request-result caches, and
journal writers. Treating this as a delayed part of split would keep a source
fenced while remote I/O, tree open, snapshot transfer, or catch-up runs; it
would couple foreground split availability to cluster placement speed.

The desired operational behavior is a short writer handoff: the remote target
prepares from a pinned source snapshot and durable tail while the source keeps
serving, then source writer ownership moves at one exact cursor. Remote
preparation, background checkpointing, physical page materialization, and
source cleanup must not extend the foreground cutover. This extends the range
catalog and serving-grant model in
[`design-crowdb-chunk-kv-server.md`](../design/chunkds/design-crowdb-chunk-kv-server.md)
sections 3 and 6 and must be distinct from the same-owner split transition.

## Solution

Add an explicit child-tree balance transition that reuses R174's durable-tail
and short writer-handoff contract, but independently chooses target placement,
remote readiness, source retirement, and stale-route grace.

```text
source serving + target prepared from pinned snapshot
                  |
                  +-- target replays durable source tail
                  |
short source-writer -> target-writer handoff at cursor C
                  |
catalog/lease target serving + source forwards stale routes
                  |
background source cleanup and shared-page materialization
```

1. Extend permanent chunk-KV server and range-catalog designs with an
   idempotent `BalanceChildTree` transition. It names one current child
   partition, source owner and epoch, selected target owner, exact source base
   manifest and tail cursor, transition ID, target epoch, expected catalog
   revision, forwarding-grace policy, and commit/abort proofs. A child may have
   only one active split, balance, merge, or transfer transition.
2. Keep placement selection separate from data movement. The balance monitor
   chooses a healthy remote target only when it improves the existing
   owner-count and durable-byte score subject to target headroom, request-rate,
   cooldown, and one-transition-per-owner limits. A target rejection or lack of
   capacity leaves the source serving and records a retryable planning outcome.
3. Pin a source base manifest and durable journal/retry floor while the source
   continues to serve. The remote target opens the exact range-bounded tree as
   `Prepared`, validates tree, stream, and page-pack identities, and obtains
   read-only access to shared immutable packs or a verified copied snapshot.
   It applies the source's durable tail into the R174-defined target overlay;
   it does not copy foreground keys through Rust or depend on source memory.
4. Require remote readiness before any source writer fence: the target must
   prove base-manifest validity, tail replay below record, byte, and estimated
   time limits, sufficient request-result history, and a durable target tail
   representation. If it cannot catch up before its preparation deadline, drop
   the unpublished target state and leave the source writer serving.
5. At a bounded handoff cursor `C`, stop assigning new writes to the source
   sequencer, drain only previously assigned source work, verify the target
   durable overlay covers `C`, and persist an exact transfer artifact. Install
   the target writer for later requests before catalog publication or route
   them through R174's bounded post-cutover mechanism. The handoff never waits
   for a source or target checkpoint, complete page materialization, or remote
   full-tree copy.
6. Publish one epoch-fenced range-catalog revision replacing the source owner
   with the exact target owner. The target obtains its serving grant only after
   catalog commit and source-writer release proof. The source remains
   read/forward-capable for a finite stale-route grace but cannot append another
   source mutation; old routes forward point operations to the target or return
   a current owner hint. Target and source never have simultaneous mutation
   authority for the range.
7. Recover every balance phase from transition, artifact, catalog, manifest,
   tail, and serving-grant proofs. A target crash before commit leaves the
   source serving; an ambiguous publication keeps the source writer closed
   until an authoritative catalog read proves commit or abort; a source crash
   waits for its old lease fence before target activation. A replacement never
   treats a heartbeat, a loaded page, or a local memtable as transfer proof.
8. Run target checkpointing, page-pack materialization, source stream/tree
   reclamation, and forwarding removal in background after commit. Retain all
   source manifests, journal prefixes, retry windows, target tail records, and
   shared page packs until catalog references, stale-route grace, recovery
   pins, and retry floors prove them reclaimable.
9. Report per-transition target selection score, source and target base/tail
   cursors, tail records and bytes, prepare duration, handoff drain duration,
   post-cutover queue depth, catalog/lease timing, forwarding count, target
   readiness failures, materialization duration, and reclaimed bytes. E2E
   benchmarks keep sustained routed traffic active through each balance move.

Edge outcomes are explicit: balancing does not start when it cannot improve
placement safely; a source write-rate that prevents target catch-up never
forces a long source outage; source or target failure cannot create dual
writers; catalog ambiguity retains the safe fenced source state; and all
physical cleanup is subordinate to published and recovery references.

## Dependencies

- Depends on R174 for durable child-tail overlays, post-cutover child writers,
  stale-route forwarding, request-result preservation, and the exact short
  handoff contract. R175 must not introduce a second incompatible transfer
  representation.
- Depends on the existing range catalog, group-0 transition persistence,
  serving grants, epoch validation, and balance scoring baseline.
- Depends on crowdb-tree immutable manifest opening, range bounds, verified
  page references, and background materialization; remote preparation must use
  those native mechanisms.
- Depends on chunk-stream durable append, replay, prefix retention, and writer
  epochs for target-tail recovery and source writer release.

## Acceptance

- Given an imbalanced cluster with an eligible child and healthy target, when
  the planner evaluates balance, assert it creates one transition only when the
  count/byte score improves within headroom, rate, cooldown, and concurrency
  limits. Invariant: balancing is improving and bounded. Unit test.
- Given sustained source writes during remote preparation, when the target
  opens the pinned manifest and replays the source tail, assert source writes
  remain acknowledged and the target reaches the configured tail budget before
  any source writer closure. Invariant: remote preparation has no foreground
  write outage. E2E test.
- Given a target that cannot catch up within record, byte, estimated-time, or
  preparation deadline limits, when balance retries, assert the source remains
  serving, no target becomes writable, and unpublished target objects are
  reclaimable. Invariant: inability to catch up does not force a long cutover.
  Integration test.
- Given handoff cursor `C` while new requests arrive, when source assignment
  closes and the target writer installs, assert source records through `C` and
  target records after `C` are each acknowledged exactly once, recovered after
  restart, and never ordered by both writers. Invariant: one exact writer
  handoff preserves all acknowledged mutations. E2E test.
- Given dirty source and target trees with slow background checkpointing, when
  handoff executes, assert its duration excludes checkpoint and materialization
  work and remains within the configured bound. Invariant: physical persistence
  is off the foreground balance path. Integration test.
- Given catalog publication after a valid target artifact, when a new route
  reaches the target and a stale route reaches the source during grace, assert
  the target serves normally and the source forwards the point request or
  returns a current owner hint without appending locally. Invariant: catalog
  switch has no dual writer and bounded stale-route behavior. E2E test.
- Given a target failure before commit, source failure before or after writer
  closure, or an unknown catalog-publication result, when a monitor resumes the
  transition, assert it selects one proof-backed source-serving, target-serving,
  or safely fenced outcome and waits through the old source lease when needed.
  Invariant: failure recovery never grants overlapping ownership. E2E test.
- Given committed balance and expired forwarding/retry/recovery pins, when
  background materialization and GC run, assert shared packs and source journal
  bytes remain while referenced and become reclaimable only after all proofs
  clear. Invariant: cleanup never outruns recovery authority. Integration test.
- Given repeated child balances under sustained routed reads and writes, when
  metrics and client percentiles are collected for every move, assert errors
  are zero, no client deadline is exceeded, and each move has correlated
  prepare, tail, handoff, forwarding, and background-work metrics. Invariant:
  balance availability is demonstrated under live traffic. E2E test.

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

- May a remote target initially read shared page packs directly, or must each
  deployment profile materialize a local verified copy before it is declared
  ready? Direct shared access minimizes cutover work; mandatory copying improves
  fault isolation but can delay balance substantially.
- Should source forwarding persist for a fixed lease-derived time or until the
  control plane observes client catalog adoption? A fixed bound is simple and
  failure-safe; observed adoption may reduce stale retries but needs durable,
  privacy-safe client observation.
- Is one target writer installation before catalog publication sufficient, or
  must the catalog atomically carry an explicit post-cutover queue ownership
  proof? Preinstallation shortens the handoff; a catalog-bound proof simplifies
  recovery authority at the cost of another durable transition detail.
