<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: RDMA RPC and stream transport

Depends on: [`design-crowdb-rpc.md`](design-crowdb-rpc.md) for common RPC
framing, request correlation, connection, buffer, and worker concepts.

This design defines the RDMA transport used by CROWDB RPC and the stream
datapath. It uses RC SEND/RECV with registered host buffers. TCP is the
fallback transport for the same logical connection; RDMA never upgrades an
established TCP connection.

## Table of Contents

- [1. Scope and goals](#1-scope-and-goals)
- [2. Endpoint discovery and path selection](#2-endpoint-discovery-and-path-selection)
- [3. Message and wire-slot model](#3-message-and-wire-slot-model)
- [4. Registered arenas and slot ownership](#4-registered-arenas-and-slot-ownership)
- [5. Event and completion model](#5-event-and-completion-model)
- [6. Failure and fallback](#6-failure-and-fallback)
- [7. Service interactions](#7-service-interactions)
- [8. Tunables](#8-tunables)
- [9. Correctness invariants](#9-correctness-invariants)
- [10. Scope boundary](#10-scope-boundary)

## 1. Scope and goals

RDMA removes kernel socket processing and CPU payload copies between registered
buffers. It does not change RPC correlation, access authorization, ChunkIO
aggregation, or DiskIO durability. The transport provides RC connection setup
through RDMA CM, two-sided SEND/RECV, registered host-buffer pools, event
wakeups through the existing epoll engine, bounded CQ polling, and completion
delivery to the C++ RPC layer and FFI callers.

The base transfer contract does not expose RDMA READ or RDMA WRITE. A large
logical result is an ordered stream of messages with logical offsets.

## 2. Endpoint discovery and path selection

Service discovery first publishes coarse capability: whether the endpoint is
RDMA-capable and whether an RDMA address is currently available. If either
side lacks capability, or local configuration or device probing disables RDMA,
the client uses TCP without attempting RDMA CM.

When coarse capability permits RDMA, the TCP handshake exchanges the current
RDMA CM address and port, GID/device identity, capability generation, protocol
version, and supported data classes. The client verifies that the generation
and peer identity are current before creating the RDMA connection.

`SocketTransport` performs this control exchange on a temporary nonblocking
TCP socket with one deadline. The 160-byte version 3 capability advertises
receive slot capacity, the maximum number of frames accepted in one SEND and
`wire_layout`. Older capability versions can be decoded for pre-payload TCP
fallback; an RDMA connection requires the current protocol and matching layout.
A 48-byte CM offer
echoes the expected peer generation and identity digest, and declares the
sender's generation, digest, layout and receive admission. Both CM sides validate it
before publishing the logical connection. The digest detects inconsistent or
stale discovery; it does not authenticate a peer. Low-level CM diagnostics may
omit the TCP expectation but still negotiate receive admission.

The control socket closes on successful RDMA setup. Allowed TCP fallback may
transfer the fully consumed control stream to the ordinary worker; a failed or
partial control exchange closes it and uses a fresh socket. Strict RDMA policy
permits this TCP control traffic and rejects TCP workload fallback. Disabled or
unavailable local RDMA under strict policy fails before opening a socket.

Cold explicit configuration distinguishes a deliberately selected TCP transport
(`rdma_enabled_ = false`) from a failed RDMA attempt. That selection connects
directly over TCP even when `allow_tcp_fallback_ = false`. It does not count as
fallback. Explicit values override environment defaults before listen/start.

One logical RPC connection chooses RDMA as its preferred path and TCP as its
one-way fallback. The path is selected before the first payload. TCP is chosen
when RDMA setup fails before payload submission; an established TCP connection
is never upgraded to RDMA.

## 3. Message and wire-slot model

ChunkIO and Chunk-KV retain ownership of application aggregation and flush
policy. The RDMA CQ owner may also pack available complete RPC frames into
one SEND without a waiting timer. The wire representation is contiguous inside
one fixed-size wire slot:

```text
[RPC header][control][payload] ... [RPC header][control][payload][unused bytes]
```

The common header carries the control and payload lengths. The complete wire
length must fit `min(aggregation_max_bytes, wire_slot_capacity,
peer_receive_slot_capacity)`, including every frame's header and control.
`aggregation_max_frames` and the peer's advertised frame limit also bound each
SEND; disabled aggregation sends one frame. Oversized individual frames fail
admission without fragmentation or fallback. A logical result larger than the
selected capacity is an ordered sequence identified by request ID, stream ID,
sequence, logical offset, and START/DATA/END/ERROR flags.

In the default `Contiguous` layout, each SEND uses one SGE pointing at the slot and the actual wire length. Each
RECV is posted with one SGE pointing at a complete receive slot and its full
capacity. The sender and receiver do not share addresses and do not need equal
ring counts; the receive slot is a bounded window for incoming messages. One
SGE on each side keeps the wire layout independent of sender SGE boundaries.

The transport may copy caller-owned control and payload buffers into the slot
before posting SEND. This copy is the transport boundary needed to make the
wire message contiguous. RPC correlation and logical operation counts remain
per frame, independently of SEND count.

`ControlData` is the negotiated direct datapath layout. It posts a fixed
1024-byte control region, including the 14-byte RPC header, followed by the
actual payload length in a separate SGE. The control FlatBuffer is zero padded
inside that region. Receive uses two SGEs with matching region capacities;
data starts at its own registered allocation base. Data classes of 64 KiB and
above have 4096-byte alignment. Control and data consume the configured receive
arena budget; a 1 MiB data capacity requires `wire_slot_capacity = 1049600`.
Header and CQE lengths must match exactly before dispatch. This layout sends
one RPC per SEND and rejects transport aggregation or fewer than two provider
SGEs during setup. It leaves the HTTP/ChunkIO producer's aggregation intact.

Immutable contiguous data already owned by the device's producer arena or
the connection's receive arena is borrowed by exact allocation range. SEND
retains that allocation without rewriting it, allowing different mirror
control headers to share the same payload. Unregistered or noncontiguous data
uses a bounded copy into the device arena. Per-CQ `direct_payload_bytes` and
`copied_payload_bytes` distinguish these paths; `direct_sends` counts all SENDs
using this layout. A failed SEND quarantines the canonical source allocation,
including when the WR retains a nested view.

Each QP has a bounded `SendQueue` in `ConnectionPools`. Producers validate and
transfer immutable frame ownership through its MPSC admission; the CQ owner
alone drains it, carries one next-slot frame and posts SENDs within effective
SQ admission. QPs on a CQ rotate through the SEND budget. A linked frame chain
keeps every source owner alive until that SEND completes. Failure closes the
uncertain stream; no suffix moves to TCP. Queue reservations, high-water and
rejection counters include admitted frames awaiting publication.

Receive parsing validates the entire aggregate and its bounded frame count
before publishing any frame. Independent immutable `Buffer::slice` descriptors
retain one parent receive-slot lease. Releasing one frame cannot change another
frame's payload offset; the final view release alone allows repost. The parent
also remains retained across dispatch of all frames. An early aggregate that
contains application data waits for handler publication as a whole.

## 4. Registered arenas and slot ownership

Each connection owns a bounded registered arena. The arena is registered once
and split into fixed-size slots; individual slots do not call `ibv_reg_mr` or
`ibv_dereg_mr`. Send and receive rings contain descriptors for slots in their
respective arenas. A stream does not reserve a private set of slots for its
lifetime. A slot moves through these states:

```text
send_free -> send_in_flight -> send_free
receive_free -> posted_receive -> receive_in_flight -> caller_owned
caller_owned -> receive_free
any uncertain state -> quarantined
```

The SEND completion returns a send slot to the send ring. A receive completion
transfers the slot to the Frame owner after header and length validation. The
Frame owner explicitly releases the slot; only then is it posted again to the
receive ring. DiskIO reposts a receive slot only after its asynchronous disk
write completes. An uncertain completion quarantines the slot until connection
quiescence. Registered memory is not reused merely because a deadline expired.

Posted receive slots are the implicit data credits. Buffer exhaustion, a full
SDK lease budget, or a full DiskIO write queue applies backpressure to the
existing producer or aggregation layer. RDMA adds no application-level credit
message. A slot is never posted twice concurrently.

`Connection::pool()` is the application allocator used by echo and default
handlers. Initial receives and reposts use the separate retained receive arena;
fully posting the RQ therefore does not consume reply allocation admission.
Send and receive arenas enforce both byte and total descriptor limits. Tagged
entry indices with atomic links protect their class freelists from concurrent
allocation/recycle ABA; quarantined entries remain outside those freelists.

Each outstanding registered buffer retains its shared arena even after the
`RdmaBufferPool` facade or transport is destroyed. The arena retains the PD and
the RDMA-CM device context list. The last lease release destroys arena
descriptors, deregisters its MR, then drops the device owner; the last device
owner deallocates its PD before releasing the context list. Completed receive
leases additionally retain their connection and repost admission state. After
shutdown, final release retires the slot without using the destroyed transport
or posting another receive. QP retirement does not accumulate arenas in a
transport-wide retired list.

`RegisteredBufferPool` uses the same `BufferArena` descriptor admission,
ownership transitions and tagged class freelists without requiring verbs or an
HCA. `BufferRegion` owns the bounded allocation; a verbs-backed region adds
registration metadata and retains its PD owner. `committed_bytes` reports carved
arena bytes, while the region capacity bounds the reserved allocation. Detached
portable buffers keep the region alive and reserve their descriptor until final
release. Their detached ranges remain committed and are not reused by another
stream. Cached and quarantined descriptors both count toward the hard total.
Application-pool count admission also uses one atomic reservation.
When a requested class cannot obtain a fresh allocation, it may reuse an
available larger class without extending the arena. The returned buffer keeps
its actual capacity class. This preserves admission after differently sized
HTTP owners have carved the arena.

`SystemBufferPool` retains its admission counter and recycler in a shared
state owned by every outstanding allocation. Destroying the facade therefore
does not invalidate a TCP/application buffer still held by an asynchronous
consumer. Clones share one allocation charge; its final release returns that
charge. The transport owns any default application-pool facade it creates.

`allocate_producer_buffer` lends storage from the device-owned send arena after
CM context adoption publishes that arena. Its registration charge and arena
owner survive the transport. `require_registered` rejects unavailable
registration; an exhausted active arena never substitutes an ordinary heap
allocation. Without an active registered arena, optional allocation uses the
ordinary bounded application pool. `WritableBuffer` exposes only unique
uninitialized capacity until its producer publishes an initialized prefix;
immutable views then retain the lease through their final release. Allocation
alone does not bypass the contiguous transport's payload copy.

The device owner charges MR creation against
`registered_memory_budget_bytes`. Zero derives a checked bound from all
CQ-admitted QPs, their send/receive arenas and the two compatibility copy
arenas. An explicit budget must admit those compatibility arenas and at least
one QP. Invalid and overflowed limits fail configuration. Reservation and MR
creation failure reject setup before connection publication and return the
temporary reservation. Releasing CQ admission does not release registered
bytes while a closed connection or held lease still owns an MR; its final
deregistration returns the charge. `registered_bytes`,
`registered_bytes_high_water`, `registration_rejects` and
`registration_failures` expose current reservations, peak reservations,
budget rejection and provider failure separately. Reservations include pending
MR creation and remain bounded during concurrent setup.

## 5. Shared CQ and completion model

Connections share a bounded CQ pool. Each QP uses one assigned CQ for both
SEND and RECV. Each CQ has its own completion channel, generation-tagged WR
table, wake descriptor and fixed polling/event thread. QP admission reserves
`sq_depth + posted_receive_target` entries on its assigned CQ; insufficient
capacity rejects setup. Failed setup rolls the reservation back, and QP
retirement releases it. Each WR retains its connection, arena owners, buffers
and request metadata. Its token resolves the CQ partition, local entry and
generation without scanning connections or trusting an error CQE's opcode.

Normal completion processing belongs to the assigned CQ thread:

1. poll bounded verbs batches and dispatch completions;
2. while traffic continues, return directly to polling;
3. after the empty-poll budget, arm notification with `ibv_req_notify_cq`;
4. poll again to close the notification race;
5. enter the CQ's private epoll wait only when still idle;
6. acknowledge completion events after waking and resume polling.

The completion and wake descriptors stay registered with the private epoll
engine. Generic socket workers handle the separately serialized CM channel and
do not drain managed CQs. CM retirement can acquire exclusive CQ ownership to
drain acknowledged responses before closing a connection, then destroy its QP
and cancel unresolved work. Per-connection posting admission prevents QP
destruction from racing SEND, receive posting or repost. Stop wakes and joins
CQ threads before destroying their QPs, CQs and channels.

Completed receives that precede application handler installation stay in a
bounded deferred list with their WR and arena owners. Publishing the handler
wakes the CQ thread; only then are those frames dispatched. Connection
capability frames are consumed by the common connection seam before application
routing. Handlerless data is never discarded merely because CM setup and CQ
processing ran on different threads.

A completed WR entry is released before invoking application callbacks or
releasing original send-buffer owners. Immediate replies and receive reposts
can therefore reuse bounded WR admission. The registered slot itself remains
leased until all frame/C++/FFI owners release it. Work cancellation and deferred
receive retirement take the same exclusive CQ ownership gate. FFI receives a
callback or buffer lease only after frame validation and ownership resolution.

## 6. Failure and fallback

WR flushes on a closed connection are counted as `shutdown_flushes` separately
from `cq_errors`. A flush on an open connection still fails that stream and
counts as a workload error. Slots with uncertain completion are quarantined in
both cases.

RDMA setup failure, missing capability, or a transport error before the first
payload may select TCP. The request ID is shared by both transports. Retry is
allowed only when existing request semantics provide idempotency or duplicate
suppression.

After RDMA payload submission, an uncertain or partial stream fails as one
operation. TCP bytes are never appended to the partial RDMA result. Late CQ
completions cannot publish data into a reused buffer. QP, CM, and CQ errors
move the connection to draining or failed state and quarantine uncertain
buffers.

`Connection::close()` also wakes its assigned CQ through a private transport
notifier. The CQ owner requests `rdma_disconnect` once, guarded by the same
per-connection operation admission that protects posting and CM-ID lifetime.
The notifier only wakes the owner; application callbacks do not drain their
own CQ. CM retirement and transport shutdown use the same disconnect guard
before stopping admission and destroying the QP. A close removes the logical
connection from socket-transport indexes, including a close racing publication;
retained buffer leases continue to own their arena and device resources.

## 7. Service interactions

For S3/HTTP and DiskIO, HTTP or ChunkIO fills the Access Server registered send
buffer; DiskIO receives into its own registered buffer and submits that buffer
directly to asynchronous disk I/O. DiskIO returns it to the receive pool only
after disk completion.

For Access Server and Chunk-KV, the existing access and aggregation layer
selects the 64 KiB or 256 KiB class. RDMA transports each aggregate with the
same request, sequence, and response semantics as TCP.

The fat client uses the access core directly. Its caller owns each completed
host buffer lease until release. GPU support initially consists of an explicit
host-to-GPU copy; a future allocator may provide GPU-capable memory without
changing the lease contract.

## 8. Tunables

The transport configuration supplies defaults when the transport is created:

```toml
wire_slot_capacity = 1048576
send_arena_bytes = 67108864
receive_arena_bytes = 1073741824
sq_depth = 1024
rq_depth = 1024
posted_receive_target = 1024
cq_count = 1
cq_depth = 8192
poll_batch = 256
verbs_poll_batch = 32
poll_empty_budget = 256
```

CQ depth is per CQ; arena budgets and SQ/RQ depths are per QP. For example,
one 8192-entry CQ admits four QPs with the default send and posted-receive
limits. `poller_count` follows the number of initialized CQs. Small-message
benchmarks select 1024-byte slots and matching arena budgets explicitly.

The versioned C ABI and Rust `RdmaConfig` expose cold configuration.
`crowdb_rpc_server_configure_rdma_v2` accepts `wire_layout` separately from the
unchanged version-one config structure; the original entry selects contiguous
layout. `CROWDB_RDMA_WIRE_LAYOUT` uses 0 for contiguous and 1 for control/data.
Owned
status JSON contains requested settings, actual QP/CQ/provider capacities,
transport errors, registration budgets and process resource counters. Connection
status caches immutable provider values before publication and retains them
after close; it never dereferences a QP during teardown. Unsupported resource
measurements are null.

`bench rdma` requires strict RDMA workload connections and unique QPs.
`bench rpc` explicitly selects TCP. Both use the same coroutine echo workload,
nanosecond histogram and canonical result file. Each loader has one outstanding
RPC; one startup thread creates the coroutines, and completion workers resume
them. TPS counts validated response callbacks inside the measurement window;
warmup and drain are separate. Native RTT starts after request construction,
before submission, and ends before Rust validation. TCP status management
connections run outside the traffic interval and close before measurement.

The effective slot counts are bounded by registered bytes, QP send/receive
depth, DiskIO queue bytes, and SDK-held slots. A configured slot size is a
capacity limit, not a required wire length; SEND uses the actual message
length while RECV posts the full slot capacity. Services may select a smaller
slot class only when the complete wire message fits that class.

## 9. Correctness invariants

- **I1:** A message is published only after its control header and payload
  length pass validation and the complete wire length fits one slot.
- **I2:** A slot has exactly one owner and is not reused before transport,
  disk, and caller access have completed.
- **I3:** Posted receive capacity is the bounded receive window; no unbounded
  allocation or second post replaces a missing slot.
- **I4:** A logical stream publishes messages in sequence order and never mixes
  bytes from TCP and a partially completed RDMA stream.
- **I5:** The shared request ID and generation prevent an ambiguous retry from
  being mistaken for a distinct operation.
- **I6:** FFI completion never transfers a buffer whose ownership or length is
  unresolved.
- **I7:** Contiguous layout parses complete frames inside one registered slot.
  Control/data layout uses its negotiated fixed control region and independent
  data capacity. Both enforce total wire admission and retain every region
  until transport and consumer access complete.
- **I8:** Every QP is assigned to exactly one shared CQ, and every shared CQ is
  polled by exactly one worker at a time.
- **I9:** Epoll notification only wakes an idle CQ polling worker; completion
  dispatch occurs in the polling worker.
- **I10:** Unsupported RDMA leaves the TCP service usable and does not require
  RDMA libraries or devices at runtime.

## 10. Scope boundary

This design covers host-memory RC SEND/RECV RPC and data streams. RDMA READ,
RDMA WRITE, remote destination descriptors, and GPUDirect are outside the base
transport contract. A future memory adapter may add them only behind the same
request, ownership, completion, and fallback semantics.
