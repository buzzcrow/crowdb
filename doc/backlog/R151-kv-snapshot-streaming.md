<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R151: kv — Resumable Chunked New-Member Snapshot Streaming

**Problem**

The new-member join path exports the complete KV engine snapshot into one
`Vec<u8>`, sends it in one `crowdb-rpc` data frame, copies that response on the
receiver, and imports one contiguous slice. `crowdb-rpc` accepts at most a 4
MiB data frame, while the KV protocol and server still describe a special 64
MiB snapshot allowance that the current socket transport does not provide.
Snapshots beyond the effective frame limit therefore cannot bootstrap a
member, and smaller snapshots retain multiple full-stream byte buffers.

The whole-buffer behavior persists through every layer. Crowdb-tree exposes
`begin`/`next`/`end` and `begin`/`feed`/`finish`/`end` C functions, but its Rust
wrapper and `KVEngine` collapse them into whole-stream methods. The C++
portable exporter and parser are now incremental, yet callers cannot retain
their sessions across RPC requests. The in-memory engine also exposes only a
whole buffer.

`POST /stores/{sid}/groups/{gid}/join` already supplies the safe publication
boundary needed here: it constructs a fresh group, imports before adding the
group to the store, and wires neither remotes nor election work until after a
successful import. R151 had additionally proposed replacement of a running
follower and automatic large-gap fallback. That broader behavior requires an
atomic crowdb-tree generation swap because root, memtables, and watermarks are
published independently. It is not required to fix new-member bootstrap and
is outside this requirement.

The root contracts are `doc/design/kv/design-crowdb-kv-state-machine.md` §6,
`doc/design/kv/design-crowdb-kv-rpc.md`, and the pre-publication join lifecycle
in `app/crowdb-kv-server/src/mgmt/group_ops.rs`.

**Solution**

Replace the single-frame new-member snapshot RPC with a receiver-pulled series
of bounded unary requests over the existing persistent `crowdb-rpc`
connection. Preserve the current portable stream format and whole-stream
CRC32C. Do not introduce a generic transport streaming abstraction or modify
the running-follower catch-up policy.

The design preserves these invariants:

- One export identity pins one immutable engine view and one `at_slot`; chunks
  from different views are never mixed.
- The source membership epoch captured by Begin must still match on every Read
  and Finish. An epoch change expires the session so a new member cannot be
  published from stale topology metadata.
- No Rust or C++ layer retains an additional full serialized snapshot. Export
  and transport buffering is bounded by one configured chunk per session plus
  the immediately previous chunk used for retry. The pinned source view and
  the importer's staged logical entries may still scale with database size.
- Import occurs only on a fresh, unpublished group. The group is registered
  and made eligible for topology wiring only after final integrity checks,
  engine finish, and learner-frontier seeding succeed.
- A repeated read at the last issued offset returns identical bytes and
  metadata. A reconnect resumes at the receiver's last successfully fed
  offset while the source session remains leased.
- Expired source sessions cause a new Begin and a fresh importer from offset
  zero. Bytes from two snapshot identities are never fed to one importer.
- Snapshot requests share the existing RPC server and runtime, but session
  count, chunk size, per-session work, and receiver in-flight requests are
  bounded.

Numbered work items:

1. **Engine sessions** — expose non-clone RAII export and import session traits
   from `KVEngine`, with metadata, sequential chunk reads, feed, finish, and
   abort/drop semantics. Implement them for `InMemKV` and
   `CrowdbTreeEngine`. Extend `crowdb-tree-ffi` so Rust owns the existing C
   handles directly instead of concatenating their output. Keep the completed
   incremental portable C++ encoder and parser; the native snapshot format is
   not used by this protocol.
2. **Bounded protocol** — replace `FBSnapshotRequest` and
   `FBSnapshotResponse` with Begin, Read, Finish, and Abort request/response
   pairs. Begin returns a server-boot nonce plus monotonic session number,
   group and engine format, `at_slot`, term at that slot, membership epoch,
   chunk limit, total encoded length, and final CRC32C. Read carries identity
   and byte offset; the response echoes both, carries payload in the data
   buffer, and reports payload CRC32C and end-of-stream. Finish and Abort are
   idempotent.
3. **Source session ownership** — add a per-store registry whose capacity is
   guarded by owned permits. Each entry owns one export session in a bounded
   actor, its immutable metadata, its last response for exact retry, and a
   30-second sliding idle lease. The default capacity is four sessions and the
   default chunk size is 1 MiB. Begin rejects excess work with typed
   backpressure; expiry, Finish, Abort, connection-independent cancellation,
   and server shutdown release the handle and permit exactly once. Registry
   synchronization is confined to snapshot RPCs and adds no lock to client,
   apply, or consensus paths.
4. **Read contract** — a source accepts the current sequential offset or an
   exact retry of the immediately preceding offset. It rejects skips, older
   offsets, wrong identities, reads after completion, and a changed source
   membership epoch with typed errors. The response is never larger than the
   session's negotiated 1 MiB maximum, which remains below crowdb-rpc's 4 MiB
   data-frame ceiling.
5. **Receiver transfer** — update `join_via_snapshot` to keep one Read in
   flight, validate identity, offset, bounds, payload CRC32C, total length, and
   final CRC32C, then feed the verified payload directly to the importer. A
   retryable disconnect reconnects and repeats the unacknowledged offset. An
   unknown or expired identity aborts the importer and restarts Begin from
   zero with a bounded retry budget.
6. **Pre-publication commit** — after the end marker, call importer finish,
   require its `at_slot` to match Begin metadata, seed the fresh learner with
   `at_slot` and its term, and only then add the group to the store. Any
   failure leaves the group unpublished and triggers best-effort Abort; the
   failed group and engine are dropped rather than reused. Membership wiring,
   election start, and WAL-tail repair remain the caller's existing follow-up
   steps.
7. **Metrics and compatibility removal** — expose active/rejected/expired
   sessions, bytes, chunks, exact retries, reconnect resumes, restarts,
   integrity failures, and export/import latency. Remove the whole-buffer
   engine methods, `SnapshotReply`, legacy protocol messages, and all claims
   of a 64 MiB snapshot exception after every caller migrates.

Running-replica snapshot replacement, `catchup_snapshot_threshold` routing,
automatic recovery from unavailable WAL history, cross-engine transfer, and a
native-frame streaming format are explicitly out of scope. A future live
replacement requirement must first define atomic engine-generation
publication; it must not add a lock to apply or read hot paths.

**Dependencies**

- Uses the current `crowdb-rpc` request correlation and generation-safe
  connection pool. It does not require transport-level streaming.
- Uses the existing pre-publication management join sequence. If a future
  caller cannot keep the target group unpublished, that caller cannot use this
  protocol without a separate atomic activation design.
- Uses the completed incremental portable encoder and parser in
  `lib/crowdb-tree/src/snapshot/snapshot_io.cpp`. Until Rust session wrappers
  land, the existing whole-buffer methods remain only as migration shims.
- R32 provides the KV RPC/lifecycle baseline. Running-follower catch-up remains
  on `FetchGap`; the configured threshold continues to stop oversized repair
  storms and log that automatic snapshot fallback is unavailable.

**Acceptance**

- Given a portable snapshot larger than 64 MiB, when a fresh member joins,
  assert every response payload is at most 1 MiB and the imported engine
  matches the exporter without any full serialized snapshot buffer in Rust or
  C++. Invariant: transfer buffering is independent of snapshot size.
  Integration test.
- Given an export session whose most recent chunk response is lost, when the
  receiver repeats that offset, assert the source returns byte-identical
  payload and metadata and then continues from the following offset.
  Invariant: the acknowledged offset is a safe reconnect point. Unit test.
- Given a disconnect after multiple chunks have been fed, when the receiver
  reconnects before lease expiry, assert it resumes at the first unacknowledged
  offset and completes without restarting from zero. Invariant: connection
  lifetime does not define snapshot lifetime. Integration test.
- Given an expired source identity during transfer, when the receiver retries,
  assert it drops the old importer, begins a new snapshot, and feeds no old
  bytes into the new session. Invariant: one importer consumes one snapshot
  identity. Integration test.
- Given the source membership epoch changes after Begin, when the receiver
  requests another chunk or Finish, assert the source expires the session and
  the receiver restarts from a new Begin without publishing the stale epoch.
  Invariant: published bootstrap metadata comes from one topology generation.
  Integration test.
- Given a corrupt payload, wrong identity, skipped offset, truncated stream,
  wrong total length, or final CRC mismatch, when join validates or finishes
  the stream, assert join fails and the group is absent from the store.
  Invariant: invalid or partial state is never published. Integration test.
- Given successful import with matching `at_slot`, when the management join
  completes, assert the learner is seeded before store registration and later
  WAL-tail repair starts above that slot. Invariant: published learner and
  engine frontiers agree. E2E test.
- Given more concurrent Begin requests than the configured capacity, when the
  server admits them, assert only the permitted number own engine sessions and
  the rest receive typed backpressure. Given Finish, Abort, expiry, or server
  shutdown, assert every permit and C handle is released once. Invariant:
  source ownership is bounded. Unit test.
- Given concurrent client and consensus traffic on the source while a large
  export runs, when chunks are pulled one at a time, assert snapshot memory
  stays within the configured session/chunk bounds and ordinary RPC counters
  continue advancing. Invariant: snapshot work is bounded and yields between
  chunks. E2E test.
- Given the migrated protocol, when a peer attempts the legacy single-frame
  request, assert it is unsupported and no 64 MiB transport override remains.
  Invariant: only one production snapshot protocol exists. Integration test.

Exact verification commands:

- `pixi run test-tree-ct`
- `pixi run -- cargo test -p crowdb-tree-ffi --all-targets`
- `pixi run -- cargo test -p crowdb-kv --all-targets`
- `pixi run -- cargo test -p crowdb-kv-server --all-targets`
- `pixi run tree-fmt`
- `pixi run tree-lint`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run -- cargo clippy -p crowdb-tree-ffi -p crowdb-kv -p crowdb-kv-server --all-targets -- -D warnings`
