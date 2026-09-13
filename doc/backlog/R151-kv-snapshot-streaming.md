<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R151: kv — Resumable Chunked Snapshot Streaming

**Problem**

KV snapshot installation currently materializes an entire engine snapshot as
one `Vec<u8>`, sends it in one `crowdb-rpc` data frame, copies the complete
response again on the receiver, and imports it from one contiguous slice. The
wire schema documents a 64 MiB frame ceiling. A snapshot larger than that
cannot install, while snapshots below the ceiling still create large memory
peaks and occupy shared RPC send capacity for one frame. Although crowdb-tree's
C API is shaped as `begin`/`next`/`end` and `begin`/`feed`/`finish`/`end`, its
current C++ exporter owns the complete serialized stream and its importer
accumulates the complete serialized stream. The implementation is therefore
not actually memory-bounded yet.

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
- Serialized transfer memory is bounded independently of total snapshot size.
  The immutable source view and staged destination engine state necessarily
  scale with logical database size, but no additional full-stream byte buffer
  is retained.
- The receiver never exposes partially imported state. Activation happens
  only after the final length and integrity check succeeds.
- A repeated chunk request for the same snapshot identity and offset returns
  the same bytes, allowing retry after timeout or reconnect.
- Snapshot traffic shares the configured RPC workers without monopolizing
  their queue or creating an unbounded detached task set.
- Small gaps continue using `FetchGap`; snapshot streaming is selected only
  when the missing range is large or required WAL entries are unavailable.

Numbered work items:

1. **Incremental engine interface** — refactor crowdb-tree's C++ portable
   exporter and importer so they encode and parse incrementally rather than
   retaining the complete serialized stream, then expose RAII export and
   import sessions through `crowdb-tree-ffi` and `KVEngine`. Export retains an
   immutable engine view and at most one encoded chunk. Import retains its
   parser carry plus staged logical engine state and atomically activates only
   on successful finish. Drop or cancellation closes the underlying C handle.
   The existing portable stream format and whole-stream CRC32C remain the
   compatibility contract.

2. **Snapshot stream protocol** — add FlatBuffer requests/responses for begin,
   chunk read, finish acknowledgement, and abort. A begin response returns an
   opaque 128-bit snapshot identity (server boot nonce plus monotonic session
   counter), `at_slot`, term at that slot, serving leader term, membership
   epoch, chunk limit, total encoded length, and final CRC32C. Each read uses a
   byte offset; its response echoes
   the identity and offset and carries payload, end marker, and payload CRC32C.
   A session accepts its current offset or an exact retry of the immediately
   preceding chunk. Other offsets fail as typed invalid-offset errors. Finish
   succeeds only after the final offset was acknowledged; finish and abort are
   idempotent.

3. **Bounded exporter sessions** — maintain a bounded per-store registry of
   immutable export sessions. Each session is a bounded actor that owns its C++
   export handle, serializes reads without a mutex, and retains only its most
   recent response for exact retry. A lock-free immutable registry snapshot
   handles identity lookup and capacity. Apply idle expiry and explicit
   abort/finish cleanup. Reject excess sessions with typed backpressure; never
   evict an active session whose acknowledged lease is still valid. Defaults
   are four sessions per store, a 30-second idle lease, and the existing 1 MiB
   chunk size; all are statically configurable and validated.

4. **Receiver-driven transfer** — fetch chunks over a persistent peer
   connection with one in-flight read per install. Feed each verified chunk
   directly into the staged engine importer, advance the acknowledged offset
   only after successful feed, and resume from that offset after a retryable
   disconnect. Retry preserves the server identity and offset; an expired or
   unknown identity restarts with a new begin and a fresh staging importer.

5. **Integrity and fencing** — validate snapshot identity, offset, total
   length, per-chunk CRC32C, final stream CRC32C, serving leader term,
   membership epoch, and `at_slot` before activation. The exporter checks that
   it remains leader in the captured term and epoch before every read. The
   receiver assigns a monotonic install generation and rechecks that generation
   and its expected topology immediately before activation. A stale topology,
   leadership change, or superseded install aborts without modifying the live
   engine. Successful live install atomically replaces engine state, resets the
   learner to `at_slot`, and replays only accepted/chosen tail slots above it.

6. **Gap policy** — use the existing configurable
   `catchup_snapshot_threshold` to choose `FetchGap` versus snapshot streaming.
   At or below the threshold, retain slot repair. Above it, start at most one
   live install per follower. A leader reports a chosen slot as unavailable
   when the requested slot is at or below its contiguous-chosen frontier but
   its acceptor no longer has the value; that typed response also selects
   snapshot streaming. After activation, `FetchGap` repairs the bounded tail.

7. **Shared-worker fairness and pressure metrics** — keep the unified RPC
   server/worker architecture. Bound chunk size and in-flight reads, naturally
   yield at each unary response, and expose active sessions, bytes/chunks,
   retries, resumed offsets, expired/aborted sessions, integrity failures,
   export/import latency, and snapshot RPC queue wait. The snapshot actor and
   immutable registry must not add a lock to an RPC or consensus hot path.

8. **Remove the single-frame path** — after compatibility is unnecessary,
   remove `FBSnapshotRequest`/`FBSnapshotResponse`, whole-buffer
   `SnapshotReply`, the 64 MiB exception, and obsolete documentation. Do not
   retain two production snapshot protocols.

**Dependencies**

- Depends on the current `crowdb-rpc` request/response correlation and
  generation-safe connection pool from R104/R114. It does not require a new
  generic streaming primitive.
- Depends on crowdb-tree's existing incremental snapshot C API shape. Its
  current whole-stream C++ implementation, Rust RAII wrappers, and the
  `KVEngine` incremental interface are all corrected as part of R151.
- R32 supplies the current KV RPC/lifecycle review and the single-frame
  performance baseline. R151 is implemented independently so R32 does not
  grow into a snapshot redesign.
- The same-engine snapshot restriction remains. Portable cross-engine
  snapshots are out of scope.

**Acceptance**

- Given a snapshot larger than 64 MiB, when a new member installs it, every
  RPC response stays within the configured chunk bound and installation
  completes without constructing a full snapshot-sized serialized buffer in
  Rust or C++. This proves size-independent transfer buffering. Integration
  test.

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

- Given a configured chunk size, session cap, or lease outside its documented
  range, when configuration is validated, startup rejects it. Given omitted
  fields, the 1 MiB, four-session, and 30-second defaults apply. This proves
  bounded configuration. Unit test.

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
