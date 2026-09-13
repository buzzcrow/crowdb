<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R151: kv — Resumable Chunked Snapshot Streaming

**Problem**

KV snapshot installation currently materializes an entire engine snapshot as
one `Vec<u8>`, sends it in one `crowdb-rpc` data frame, copies the complete
response again on the receiver, and imports it from one contiguous slice. The
wire schema documents a 64 MiB frame ceiling. A snapshot larger than that
cannot install, while snapshots below the ceiling still create large memory
peaks and occupy shared RPC send capacity for one frame.

This conflicts with the state-machine architecture in
`doc/design/kv/design-crowdb-kv-state-machine.md` §6, which specifies stable,
bounded, resumable snapshot chunks and atomic activation. Crowdb-tree's C API
already exposes incremental export (`begin`/`next`/`end`) and import
(`begin`/`feed`/`finish`/`end`), but the Rust FFI and `KVEngine` interface
collapse those operations into whole-snapshot vectors.

Consensus and client RPC intentionally share one `RpcServer` and worker pool.
A large single-frame snapshot can therefore increase memory pressure and
delay ordinary client requests, Heartbeats, and Accept processing on the same
workers. Adding a member with a large log gap is the concrete production case:
slot-by-slot `FetchGap` is inefficient, but the current snapshot path is not
bounded by chunk.

**Solution**

Replace the single-frame snapshot RPC with a receiver-driven, resumable chunk
stream over the existing persistent `crowdb-rpc` connection. Keep consensus
and client traffic on the same server and workers. Use correlated unary
Begin/Read/Finish/Abort requests as a pull stream instead of adding a generic
transport streaming abstraction: the receiver controls admission, every
response is bounded, and reconnection resumes at an acknowledged offset.

The design preserves these invariants:

- Export represents one stable engine snapshot and `at_slot`; chunks from
  different snapshots are never mixed.
- Memory retained by one transfer is bounded independently of total snapshot
  size.
- The receiver never exposes partially imported state. Activation happens
  only after the final length and integrity check succeeds.
- A repeated chunk request for the same snapshot identity and offset returns
  the same bytes, allowing retry after timeout or reconnect.
- Snapshot traffic shares the configured RPC workers without monopolizing
  their queue or creating an unbounded detached task set.
- Small gaps continue using `FetchGap`; snapshot streaming is selected only
  when the missing range is large or required WAL entries are unavailable.

Numbered work items:

1. **Incremental engine interface** — expose RAII export and import sessions
   through `crowdb-tree-ffi` and `KVEngine`. Export yields stable bounded
   chunks without constructing a full `Vec<u8>`. Import accepts chunks into a
   staging session and atomically activates only on successful finish. Drop or
   cancellation closes the underlying C handle.

2. **Snapshot stream protocol** — add FlatBuffer requests/responses for begin,
   chunk read, finish acknowledgement, and abort. A begin response returns an
   opaque snapshot identity, `at_slot`, term, membership epoch, chunk limit,
   and integrity metadata. Each read includes identity and byte/chunk offset;
   each response includes offset, payload, end marker, and integrity data.

3. **Bounded exporter sessions** — maintain a bounded per-store registry of
   immutable export sessions. Apply idle expiry and explicit abort/finish
   cleanup. Reject excess sessions with typed backpressure; never evict an
   active session whose acknowledged lease is still valid.

4. **Receiver-driven transfer** — fetch chunks over a persistent peer
   connection with a bounded number of in-flight reads. Feed each verified
   chunk directly into the staged engine importer, advance the acknowledged
   offset only after successful feed, and resume from that offset after a
   retryable disconnect.

5. **Integrity and fencing** — validate snapshot identity, offset, total
   length, per-chunk integrity, final stream integrity, source term, membership
   epoch, and `at_slot` before activation. A stale membership or superseded
   install aborts without modifying the live engine.

6. **Gap policy** — add an explicit, configurable threshold for choosing
   `FetchGap` versus snapshot streaming. Below the threshold, retain the
   existing slot repair path. Above it, or when the WAL range is unavailable,
   install a snapshot and then use `FetchGap` for the bounded tail.

7. **Shared-worker fairness and pressure metrics** — keep the unified RPC
   server/worker architecture. Bound chunk size and in-flight reads, yield
   between chunks, and expose active sessions, bytes/chunks, retries, resumed
   offsets, expired/aborted sessions, integrity failures, export/import
   latency, and snapshot-induced RPC queue wait. Do not add a lock to an RPC or
   consensus hot path.

8. **Remove the single-frame path** — after compatibility is unnecessary,
   remove `FBSnapshotRequest`/`FBSnapshotResponse`, whole-buffer
   `SnapshotReply`, the 64 MiB exception, and obsolete documentation. Do not
   retain two production snapshot protocols.

**Dependencies**

- Depends on the current `crowdb-rpc` request/response correlation and
  generation-safe connection pool from R104/R114. It does not require a new
  generic streaming primitive.
- Depends on crowdb-tree's existing incremental snapshot C API. Rust RAII
  wrappers and the `KVEngine` incremental interface land as part of R151.
- R32 supplies the current KV RPC/lifecycle review and the single-frame
  performance baseline. R151 is implemented independently so R32 does not
  grow into a snapshot redesign.
- The same-engine snapshot restriction remains. Portable cross-engine
  snapshots are out of scope.

**Acceptance**

- Given a snapshot larger than 64 MiB, when a new member installs it, every
  RPC response stays within the configured chunk bound and installation
  completes without constructing a full snapshot-sized Rust buffer. This
  proves size-independent streaming. Integration test.

- Given an export session and repeated reads for the same offset, when a read
  is retried, both responses carry identical bytes and metadata. This proves
  resumable deterministic chunks. Unit test.

- Given a connection drop after several acknowledged chunks, when the
  receiver reconnects, it resumes from the last fed offset and activates a
  state identical to the exporter without restarting from byte zero. This
  proves reconnect recovery. Integration test.

- Given a corrupt, missing, duplicated, reordered, or wrong-snapshot chunk,
  when the receiver validates or feeds it, installation fails, the staging
  session is discarded, and the prior live engine remains readable. This
  proves integrity and atomic activation. Integration test.

- Given an idle, aborted, completed, or cancelled transfer, when its cleanup
  condition occurs, all export/import handles and registry capacity are
  released exactly once. This proves bounded lifecycle ownership. Unit test.

- Given concurrent client writes, Heartbeats, Accepts, and one large snapshot
  on the shared RPC workers, when the transfer runs, snapshot retained memory
  remains within the configured session/chunk/in-flight bounds and consensus
  requests continue making progress. Stage latency and queue counters expose
  any tail impact. This proves shared-worker fairness. E2E test.

- Given a follower below the configured gap threshold, when it catches up, it
  uses `FetchGap`. Given a larger gap or unavailable WAL range, it streams a
  snapshot and repairs only the bounded tail. This proves gap-policy routing.
  Integration test.

- Given a membership epoch or install generation change during transfer, when
  the receiver reaches the next fence check, it aborts the stale session and
  does not activate it. This proves topology fencing. Integration test.

Exact verification commands:

- `pixi run cargo test -p crowdb-tree-ffi`
- `pixi run cargo test -p crowdb-kv`
- `pixi run cargo test -p crowdb-kv-server`
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy -p crowdb-tree-ffi -p crowdb-kv -p crowdb-kv-server --all-targets -- -D warnings`
- `pixi run tree-lint`

