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
- [3. Message and aggregation model](#3-message-and-aggregation-model)
- [4. Registered buffers and ownership](#4-registered-buffers-and-ownership)
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

## 3. Message and aggregation model

ChunkIO and Chunk-KV retain ownership of aggregation and flush policy. RDMA
receives an already aggregated buffer and transmits it without additional
waiting or coalescing. A data message has:

```text
SGE 0: 1 KiB control block
SGE 1: selected data buffer
```

The control block contains the common RPC header and control metadata. The
selected data capacity is 64 KiB for metadata, 256 KiB for aggregated
Chunk-KV results, or 1 MiB for the S3/HTTP/DiskIO datapath. The actual payload
length is carried in the header. A logical result larger than the selected
capacity is an ordered sequence identified by request ID, stream ID, sequence,
logical offset, and START/DATA/END/ERROR flags.

The two-SGE layout is intentional. It keeps control and data ownership clear
and avoids CPU concatenation. A sender may inline the small control block when
the device permits it; receive buffers retain the two-SGE layout.

## 4. Registered buffers and ownership

Each process has a bounded registered host-memory budget. Data classes and the
control pool are shared by streams; a stream does not reserve a private set of
buffers for its lifetime. A buffer moves through these states:

```text
available -> posted_receive -> receive_in_flight -> caller_owned
available -> send_in_flight -> available
receive_in_flight -> disk_owned -> available
any uncertain state -> quarantined
```

The owner that receives a completed SDK buffer must explicitly release its
lease. DiskIO reposts a receive buffer only after its asynchronous disk write
completes. An uncertain completion is quarantined until connection quiescence.
Registered memory is not reused merely because a deadline expired.

Posted receives are the implicit data credits. Buffer exhaustion, a full SDK
lease budget, or a full DiskIO write queue applies backpressure to the existing
producer or aggregation layer. RDMA adds no application-level credit message.

## 5. Event and completion model

The RDMA completion-channel fd and RDMA-CM event fd are registered with the
existing epoll engine. Epoll provides wakeups; the RPC worker then:

1. acknowledges and re-arms the channel event;
2. drains CM events and updates connection state;
3. polls bounded batches from the CQ;
4. resolves each work request through its `wr_id` record;
5. dispatches frame completion or buffer ownership transfer.

The RPC library controls polling. FFI receives only a completion callback or a
buffer lease after C++ has validated the frame and resolved ownership. There is
no independent RDMA polling thread in the default path.

## 6. Failure and fallback

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
stream_buffer_size = "1MiB"
chunk_kv_metadata_buffer_size = "64KiB"
chunk_kv_aggregate_buffer_size = "256KiB"
rpc_control_block_size = "1KiB"
rdma_registered_bytes = "64MiB"
rdma_max_inflight_bytes = "32MiB"
```

The effective pool counts are bounded by registered bytes, receive depth, send
depth, DiskIO queue bytes, and SDK-held bytes. A configured class is a capacity
limit, not a required wire length.

## 9. Correctness invariants

- **I1:** A message is published only after its control header and payload
  length pass validation for the negotiated class.
- **I2:** A buffer has exactly one owner and is not reused before transport,
  disk, and caller access have completed.
- **I3:** Posted receive capacity is the bounded receive window; no unbounded
  allocation replaces a missing buffer.
- **I4:** A logical stream publishes messages in sequence order and never mixes
  bytes from TCP and a partially completed RDMA stream.
- **I5:** The shared request ID and generation prevent an ambiguous retry from
  being mistaken for a distinct operation.
- **I6:** FFI completion never transfers a buffer whose ownership or length is
  unresolved.
- **I7:** Unsupported RDMA leaves the TCP service usable and does not require
  RDMA libraries or devices at runtime.

## 10. Scope boundary

This design covers host-memory RC SEND/RECV RPC and data streams. RDMA READ,
RDMA WRITE, remote destination descriptors, and GPUDirect are outside the base
transport contract. A future memory adapter may add them only behind the same
request, ownership, completion, and fallback semantics.
