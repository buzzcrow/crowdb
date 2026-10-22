<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RDMA datapath stream Plan

Upstream: [R240](../backlog/R240-rdma-datapath-stream.md),
[root RDMA plan](plan-rdma.md), [registered buffers](../design/rpc/design-crowdb-rpc-rdma.md#4-registered-arenas-and-slot-ownership),
and [RDMA RPC transport](../design/rpc/design-crowdb-rpc-rdma.md).

Goal: connect existing HTTP/ChunkIO aggregation to the RDMA RPC stream and
DiskIO receive loop without introducing another aggregation or CPU payload
copy.

## Execution checkpoint

The transport/foundation acceptance is complete at `55ffee38`; cleanup is
committed at `d6bedf94`. Preserve the verified contiguous-slot benchmark path.
The six existing DiskIO-client integration cases pass before service changes.
The full C++ gate already covers DiskIO engines and AlignedWriter.
The 31 focused ChunkIO configuration/mirror/stream cases and 11 Access Server
configuration/service cases also pass before production changes.

Observed flow and the first missing seam:

- `NativeBodyAllocator` / `NativeOwner` in `crowdb-access-s3` supplies Hyper's
  body receive regions. `NativeFramedOwner` finalizes physical frame regions
  in place and supplies immutable `Bytes` views. S3 `write_object_body` passes
  those owners through `UploadBuffer::Framed` to
  `LargeAsyncObjectWriter::on_framed_data`. Do not route this path through
  `frame_tail` or introduce another HTTP assembly copy.
- `RoutedDiskWriter` delegates views to `DiskioClient::write_views`, then
  `WireClient::write_segment_views` and `RpcClient::call_chain`. Existing
  grouping preserves order and `ordering_zone_offset`. The current RDMA
  `flush_connection` copies every view into a new contiguous wire slot.
- `DiskioServer::handle_write` clones `request->data_buf`, deletes the Frame,
  and retains the clone through `AlignedWriter::submit_ordered` completion.
  Its current `data_buf->data` access must honor `payload_data()` for views.
  AlignedWriter passes aligned full blocks directly, with explicit edge-block
  staging only when required by device alignment.
- Reads currently allocate from `conn->pool()` and may stage O_DIRECT data.
  Allocate eligible direct reads from registered producer storage. Rust
  `WireClient::await_read_response` already returns `Bytes::from_owner(Buffer)`;
  the final caller release can therefore own receive repost admission.

### Direct layout and implementation order

The existing one-SGE path copies complete frames and remains the ordinary RPC
benchmark path. A zero-copy service producer needs an immutable registered
payload independent of each destination's control header: mirror targets have
different disk addresses and may send concurrently. Add an explicitly
negotiated control/data layout for this service path, using the QP's existing
two-SGE capability. This avoids rewriting payload prefixes during fanout.
The layout is an implementation decision for the required zero-copy path;
it does not add application aggregation or a flush timer.

- [x] **Expose bounded producer allocation first**: add native transport and
  C ABI allocation from the already device-owned send arena, with explicit
  `require_registered` policy and no replacement allocation on exhaustion.
  Publish the arena pointer only after CM/device adoption using release/acquire;
  the existing operation gate pins shutdown. Add unique writable Rust storage
  with an uninitialized-capacity view and an explicit initialization contract.
  Test no-device allocation, hard bounds, pointer-preserving commit and real
  registered storage retained across server destruction. This API alone does
  not establish zero-copy SEND; direct layout and producer wiring follow.
  The new ordinary-buffer lifetime test first reproduces a SIGSEGV in
  `Buffer::release()` after the `SystemBufferPool` facade is destroyed (GDB
  confirms the retained raw recycler pointer). Move ordinary pool admission
  and recycling into a retained shared State, keeping its CAS admission and
  avoiding a new lock. Outstanding buffers must remain releasable after their
  facade is destroyed, as registered leases already do. Verify existing detach
  and concurrent outstanding-admission tests alongside the new regression.
  Use the admission reservation itself as the allocation's State lifetime
  reference, plus one facade bit. This retains the prior one-CAS acquire and
  one-counter-release hot path; final release and facade destruction atomically
  choose the sole State destructor. Add their concurrent race test.
  The first HCA producer case reached its readiness assertion before calling
  `RpcServer::start`; the listener is created during start. Add that missing
  setup action and rerun the case without changing deadlines or assertions.
  The next run confirms listen readiness but rejects allocation: PD/arenas are
  adopted on the first real CM connection, not on listen. Establish a real
  client QP before the allocation assertions; also assert required allocation
  is unavailable before that CM adoption. This matches route-before-producer
  initialization in the planned service wiring.
  The corrected real-HCA test passes in 14 ms: exact arena exhaustion,
  no optional system replacement, release/reallocation, and a cloned lease
  surviving server/CM teardown. Held receive and peer-close cases also pass.
  The final production source passes full C++/FFI verification at
  `.crowdb-runtime/artifacts/unit/cpp-9266930ffd4142d18fd3d5c5318082de/`,
  all 141 no-verbs CTest cases (one HCA case skips there), both new Rust
  producer tests, workspace clippy/rs-lint, formatting and scoped tree-lint.
  Tree-lint exits zero with style warnings. The test's corrected startup/CM
  setup is rebuilt and verified separately after that full gate.
- [~] **Add negotiated direct layout**: add `RdmaWireLayout::Contiguous` and
  `ControlData`, defaulting to `Contiguous`. For `ControlData`, the control
  region is `RDMA_CONTROL_CAPACITY` including the 14-byte RPC header; derive
  data capacity from `wire_slot_capacity - RDMA_CONTROL_CAPACITY` and validate
  it against the existing classes. The 1 MiB data default therefore has total
  `wire_slot_capacity = 1049600`, rather than claiming 1 MiB of payload fits
  in a 1 MiB total wire slot. Advertise layout in the versioned TCP capability
  and bind it in the compact CM offer. A mismatched layout fails before payload
  and follows the existing explicit fallback policy. Reject direct layout with
  transport aggregation enabled or provider SGE capacity below two. Extend
  typed C/Rust cold config, environment parsing and status with the same names.
  Preserve the existing C ABI configuration initializer/struct size. Add a
  versioned configure entry accepting the existing config plus `wire_layout`,
  rather than writing new fields past a version-one caller's storage.
  Files: `rdma_config.h`, capability/offer/preflight modules, config ABI/FFI,
  `rdma_connection.cpp`, config/status tests.
- [ ] **Allocate and send immutable registered data**: expose bounded producer
  allocation from the transport's device-owned arena. Provide a unique Rust
  `WritableBuffer` with a `MaybeUninit<u8>` capacity view and consuming commit;
  freeze into the existing owner-backed `Bytes` contract. Validate address
  ranges against the owning arena before using an MR key; retain the exact
  underlying allocation through SEND completion and propagate quarantine to
  that allocation. Build the small per-request header/control in its own
  registered region, pad that region to 1 KiB, and post control plus payload
  as two SGEs. Keep one WR per complete RPC and count direct/copied payload
  bytes in status. No range registry or new hot-path lock. Files: buffer arena,
  RDMA pool/send/work-request modules, C ABI/FFI buffer producer API.
- [ ] **Receive into aligned data storage**: prepost two registered SGEs in
  direct layout, with 1 KiB control and a separately aligned data allocation.
  Charge both against `receive_arena_bytes`; descriptor count covers both and
  arithmetic is checked. Validate the complete control/header/length before
  dispatch; retain data ownership through disk/caller completion. Release or
  quarantine both regions exactly once on post/CQE/cancel/shutdown failure.
  Files: `rdma_receive.cpp`, receive posting, WorkRequest and repost context,
  real HCA ownership tests. Keep generic receive credit implicit in RQ slots.
- [ ] **Connect the actual HTTP producer**: allow `NativeBodyAllocator` to
  obtain its owner storage from the retained DiskIO producer allocator after
  route setup. Preserve its existing byte admission, AtomicWaker pressure and
  framed-owner API. Wire it through `ChunkIoClient` / `RoutedDiskWriter` /
  S3 storage setup. A bounded native allocator failure pauses intake; it must
  not silently allocate an unregistered replacement. Retain TCP allocation
  when the service selects TCP. Cover owner views, slices and mirror fanout.
- [ ] **Close DiskIO handoff and read lifetime**: use exact view pointer/length,
  validate request bounds before disk submission, transition data to DiskOwned
  until the callback returns, and retain connection state across delayed I/O.
  Registered read storage must cover engine completion and SEND completion;
  client receive storage remains held through returned Bytes. Preserve existing
  alignment handling and short-I/O/error semantics. Test delayed I/O, queue
  pressure, held caller buffers, error, cancellation and connection shutdown.
- [ ] **Verify real service flows**: run focused baseline and changed tests,
  then real DiskIO plus HTTP/ChunkIO upload/download of objects larger than
  1 MiB on the prepared HCA. Assert direct-payload counters and same data
  addresses at producer/SEND and receive/disk handoff, exact bytes and existing
  frame integrity/order. Repeat TCP-only and two non-root OCI profiles. Keep
  per-case manifests, settings, errors and complete logs. Run required fmt,
  clippy/tree-lint and full affected gates before cleanup; then use R241's own
  plan for Chunk-KV integration.

## Phase 1: map existing data flow

- [ ] **Trace upload flow**: identify where HTTP body bytes enter ChunkIO,
  become aggregated buffers, are submitted to DiskIO, and are released after
  durable completion. Files: `app/crowdb-access-server/`,
  `lib/crowdb-chunk-client/`, `lib/crowdb-diskio-client/`, and ChunkIO designs.
- [ ] **Trace download flow**: identify how SDK/HTTP consumers receive data,
  how read-ahead aggregation chooses message boundaries, and when a caller
  releases each buffer.
- [ ] **Define transport adapter**: add the smallest Access/DiskIO adapter that
  accepts an already formed buffer, actual length, request identity, sequence,
  and logical offset. Do not add a second flush timer or aggregation queue.

## Phase 2: upload to DiskIO

- [ ] **Allocate 1 MiB datapath class**: select the configured 1 MiB data
  capacity plus 1 KiB control buffer for S3/HTTP/DiskIO streams; the configured
  total wire slot includes both regions and the header.
- [ ] **Fill registered send buffer**: make the HTTP/ChunkIO producer write
  directly into the send-owned registered buffer and submit its actual length.
  The producer must stop when the send/registered budget is exhausted.
- [ ] **Receive ownership handoff**: on RDMA receive completion, transfer the
  DiskIO buffer to the asynchronous disk writer. Keep request, sequence,
  logical offset, and expected length attached to the write.
- [ ] **Repost after disk completion**: only reset and repost the receive buffer
  after the disk write and integrity result are complete. Propagate disk errors
  to the stream and do not silently repost corrupted data.

## Phase 3: download to SDK/HTTP

- [ ] **Prepost receive buffers**: use the R238 pool and R239 receive window;
  publish a `BufferLease` only after header/class/length validation.
- [ ] **Preserve caller ownership**: keep the lease caller-owned until explicit
  release. Do not let CQ completion or a stream callback recycle it.
- [ ] **Implement multi-message assembly**: deliver messages in sequence and
  logical offset order, preserving end/error flags and cancellation behavior.
- [ ] **Keep TCP parity**: use the same access result and error mapping when the
  stream selects TCP before RDMA payload submission.

## Phase 4: pressure and failure behavior

- [ ] **Disk queue pressure**: stop reposting when the asynchronous disk queue
  reaches its byte bound; let receive-window exhaustion backpressure the
  sender.
- [ ] **SDK pressure**: stop publishing new leases when held bytes reach the
  configured limit; resume after release.
- [ ] **Partial stream failure**: fail the logical object/request after partial
  RDMA transfer and quarantine uncertain buffers; never append TCP bytes.
- [ ] **Cancellation**: stop new aggregation submissions, drain known WRs,
  preserve in-flight buffer lifetimes, and return only buffers proven safe.

## Tests

Unit tests:

- actual length below 1 MiB does not send padded bytes;
- sequence/offset/end/error parsing and multi-message assembly;
- DiskIO repost is rejected before disk completion;
- SDK lease release is the only return transition for caller-owned buffers;
- no second aggregation or flush timer is invoked by the RDMA adapter.

Integration tests:

- HTTP/ChunkIO aggregate enters a registered send buffer directly;
- DiskIO receive completion hands the same buffer to asynchronous disk I/O;
- full disk queue stops repost and resumes after completion;
- all SDK leases held causes bounded producer backpressure;
- TCP and RDMA return identical bytes, lengths, and errors;
- cancellation and partial RDMA failure leave no reusable uncertain buffer.

E2E tests:

- single-node HCA local-loop OCI upload into DiskIO and download back to a client;
- object larger than 1 MiB reconstructed from ordered messages;
- TCP-only OCI profile produces the same object and metadata;
- single-node local-loop multi-container profile reports bounded pressure without claiming physical
  endpoint isolation.

## Consolidated files

- `app/crowdb-access-server/`
- `lib/crowdb-chunk-client/`
- `lib/crowdb-diskio-client/`
- `lib/crowdb-rpc/`
- `lib/crowdb-chunkio/` or the existing ChunkIO owner module
- relevant S3/DiskIO integration tests and OCI E2E runner

## Verification gates

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-access-server --all-targets
pixi run -- cargo test -p crowdb-diskio-client --all-targets
pixi run -- cargo test -p crowdb-chunk-client --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
pixi run rs-lint
pixi run tree-lint
```

Do not hand off to R242 until both upload and download have explicit lease
ownership tests and the DiskIO receive loop proves repost-after-disk-completion.

## Provider references

- [libibverbs receive work requests](https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/man/ibv_post_recv.3)
  defines the SGE address/length/lkey array and completion-before-buffer-reuse
  requirement. CrowDB additionally waits for the disk/caller lease to end.
- [libibverbs send work requests](https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/man/ibv_post_send.3)
  defines scatter/gather submission and its completion ownership. The fixed
  1 KiB control region, total wire admission and layout negotiation are
  CrowDB's own protocol contract.
