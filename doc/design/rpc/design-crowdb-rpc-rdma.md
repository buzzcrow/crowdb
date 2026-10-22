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

One logical RPC connection chooses RDMA as its preferred path and TCP as its
one-way fallback. The path is selected before the first payload. TCP is chosen
when RDMA setup fails before payload submission; an established TCP connection
is never upgraded to RDMA.

## 3. Message and wire-slot model

ChunkIO and Chunk-KV retain ownership of aggregation and flush policy. RDMA
receives an already aggregated buffer and transmits it without additional
waiting or coalescing. The RDMA wire representation is contiguous inside one
fixed-size wire slot:

```text
[common RPC header][control metadata][payload][unused slot bytes]
```

The common header carries the control and payload lengths. The complete wire
length must not exceed the slot capacity. A logical result larger than the
selected capacity is an ordered sequence identified by request ID, stream ID,
sequence, logical offset, and START/DATA/END/ERROR flags.

Each SEND uses one SGE pointing at the slot and the actual wire length. Each
RECV is posted with one SGE pointing at a complete receive slot and its full
capacity. The sender and receiver do not share addresses and do not need equal
ring counts; the receive slot is a bounded window for incoming messages. One
SGE on each side keeps the wire layout independent of sender SGE boundaries.

The transport may copy caller-owned control and payload buffers into the slot
before posting SEND. This copy is the transport boundary needed to make the
wire message contiguous; it is not an additional application-level message or
aggregation step.

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
- **I7:** A SEND uses one contiguous registered slot and a RECV uses one
  complete posted slot; sender SGE boundaries never define receiver parsing.
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
