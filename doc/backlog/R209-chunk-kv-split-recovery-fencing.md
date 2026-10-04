<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R209: Chunk-KV — Durable split abort and recovery fencing

## Problem

The split state machine correctly rejects an abort after `CatalogCommitted`, but
the recovery path for a pre-publication abort is incomplete. The local parent
abort clears its process-local transition and ingress, while the child overlay,
prepared child handle, and generation pin are owned by the split worker. The
current aborted-transition processing releases the child pin, but does not
provide one durable, idempotent operation that discards every prepared child
artifact and restores the parent route after a crash.

This leaves a failure mode in which a crash between child preparation and
catalog resolution can leave prepared child state, ingress, or a root-generation
pin without a deterministic cleanup/recovery path. The intended behavior is defined in
[`design-crowdb-chunk-kv.md`](../design/chunkds/design-crowdb-chunk-kv.md) §5 and
[`design-crowdb-chunk-kv-server.md`](../design/chunkds/design-crowdb-chunk-kv-server.md) §6.
Relevant code is `lib/crowdb-protocol/src/chunk_kv.rs`,
`lib/crowdb-chunk-kv/src/partition.rs`, and
`lib/crowdb-chunk-kv/src/partition/split.rs`.

## Solution

Implement one durable split state machine whose catalog evidence controls both
authority and cleanup.

1. Keep `CatalogCommitted` terminal for abort at both the reducer and durable
   update boundary. An abort proof must identify the transition and prove that
   the authoritative catalog still contains the original parent.
2. Persist `Planned`, `ParentPreparing`, `ChildPrepared`, `CatalogCommitted`,
   and `Aborted` transitions with revision/identity checks. Replaying the same
   transition is idempotent; a conflicting transition identity is rejected.
3. Add startup reconciliation that reads the transition, catalog head, parent,
   child artifact, ingress, overlay, and generation pin as one recovery view.
   Before publication it either resumes preparation or executes idempotent
   cleanup and returns the parent to `Serving`. After publication it completes
   the committed path and never rolls back to the old full range.
4. Make cleanup explicit and idempotent: clear parent ingress, retire the
   prepared child writer, remove the transition marker, and release the exact
   child root-generation pin only after catalog evidence permits it. Cleanup
   must be callable after restart from durable transition state.
5. Define and enforce partition-local owner epochs. An epoch must increase for
   every replacement of the same `partition_id`; a split child gets its own
   initial epoch because it has a new identity. Partition IDs are globally
   unique and must never be reused, so an old writer cannot become valid by
   reappearing under a recycled child ID.

```text
durable transition + catalog head
          |
          +--> pre-commit: resume or abort, then clean artifacts
          |
          `--> committed: retain parent/child authority, finish cleanup
```

## Dependencies

- Incoming: the chunk-KV split and serving-authority designs cited above.
- Incoming: the existing group-0 catalog publisher and its compare-and-put
  contract in `app/crowdb-chunk-kv-server/src/catalog/store.rs`.
- Outgoing: tree generation pin/unpin APIs and partition recovery startup.
- The catalog publisher must remain the authority boundary; do not infer
  publication or cleanup permission from artifact existence.

## Acceptance

1. Create a split through `ChildPrepared`, issue a valid non-publication abort,
   restart, and assert that the original parent serves the full range, the
   child is not authoritative, ingress is absent, and all temporary pins are
   eventually released. Invariant: pre-commit abort preserves the parent and
   cleans temporary state. **Integration test**
2. Try to validate or apply `CatalogCommitted -> Aborted` and assert rejection.
   Invariant: committed authority cannot be rolled back by an abort transition.
   **Unit test**
3. Crash after child preparation, after ingress installation, and after
   catalog publication; restart and assert deterministic resume-or-cleanup
   behavior for each point. Invariant: every durable split phase has a forward
   recovery path. **Integration test**
4. Repeat abort, cleanup, and catalog reconciliation with the same transition
   identity and assert idempotent results. Invariant: retries cannot leak pins
   or create a second child. **Unit test**
5. Submit a stale child or parent writer after recovery and assert rejection by
   catalog generation, transition identity, or owner epoch. Invariant:
   temporary artifacts never grant serving authority. **Integration test**
6. Split a parent into a new child whose initial epoch is numerically equal to
   the parent's epoch and assert that the child is accepted under its new ID;
   then attempt to reuse an old partition ID and assert rejection. Invariant:
   epochs are monotonic per partition and partition identities are never reused.
   **Unit test**

Run the relevant gates with:

```text
pixi run cargo fmt --all -- --check
pixi run cargo clippy -p crowdb-protocol -p crowdb-chunk-kv --all-targets -- -D warnings
pixi run cargo test -p crowdb-protocol --test chunk_kv_catalog_test
pixi run cargo test -p crowdb-chunk-kv --test partition_test split_abort
```
