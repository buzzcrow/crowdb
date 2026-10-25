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

Service discovery resolves the RPC endpoint. The current endpoint's TCP
preflight supplies `RdmaEndpointCapability`, including availability, RDMA CM
address and port, GID/device identity, generation, protocol version and data
classes. Group 0 does not store a second authoritative copy of this transient
transport state. A discovered endpoint alone cannot authorize RDMA CM.

RDMA is opt-in through explicit cold configuration or `CROWDB_RDMA_ENABLED=1`.
Hardware presence alone does not enable it. Defaults permit TCP fallback;
strict workloads explicitly disable fallback. Typed configuration overrides
environment settings and rejects an unavailable requested RDMA device before
startup. Environment-based probing can remain on TCP when fallback is allowed.
A deliberately disabled typed configuration selects TCP directly, independently
of the fallback flag.

An enabled, locally ready client checks the live TCP capability before CM.
An unavailable or incompatible peer skips CM and follows the fallback policy.
The CM offer binds that exact generation and identity, so a changed endpoint
between preflight and connection cannot silently replace its negotiated peer.

`SocketTransport` performs this control exchange on a temporary nonblocking
TCP socket with one deadline. The 160-byte version 4 capability advertises
receive slot capacity, the maximum number of frames accepted in one SEND,
`wire_layout` and `max_control_message_bytes`. Older capability versions can be decoded for pre-payload TCP
fallback; an RDMA connection requires the current protocol and matching layout.
A 52-byte CM offer
echoes the expected peer generation and identity digest, and declares the
sender's generation, digest, layout, receive admission and maximum logical
control length. The explicit limit is bound into the digest and validated
against TCP capability when available. Both CM sides validate it
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
SEND; disabled aggregation sends one frame. Oversized individual frames in contiguous layout fail
admission without fragmentation or fallback. Control/data layout also carries
bounded streams of encoded logical control bytes.

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
Header and CQE lengths must match exactly before dispatch. A direct envelope
sends one RPC per SEND. Setup requires at least two provider SGEs. It leaves
the HTTP/ChunkIO producer's aggregation intact.

Optional small-frame aggregation uses the same fixed prefix and separate data
region. A control/data capability and CM offer with `receive_frame_limit > 1`
explicitly advertise packed-envelope support; `1` permits direct envelopes
only. The advertised limit remains bound into the capability identity. A sender
uses packing only when enabled locally and the peer advertises at least two
frames. Direct envelopes remain valid on the same QP. Legacy peers reject the
extended advertisement before workload submission; use disabled aggregation
for their existing direct RDMA policy, or the explicit setup-only TCP fallback.

The packed envelope uses reserved transport message type `0xFFFD`, one-way
flags, and the ordinary fixed-prefix header carrying its actual packed length.
Its padded control begins with `RDPK`, little-endian version 1 and a 16-bit frame
count. Remaining prefix bytes are zero. Its data region contains complete
ordinary `[header][control][data]` frames, bounded by local/peer capacity,
aggregate byte/frame limits, and the advertised receive frame count. The count,
all lengths, every inner frame and the complete suffix validate before any
logical frame is published. Nested envelopes, capability messages and control
fragments cannot appear inside it.

Only complete frames at most 4096 bytes are eligible. DiskIO messages bypass
packing, including control-only READ requests whose receive allocation later
becomes the disk destination. Other services enabling aggregation accept
unaligned retained data views for small messages; services requiring independent
aligned allocations keep aggregation disabled. Large payloads remain direct.
The owner looks ahead only into available input, packs at least two eligible
adjacent frames and stops at a direct/control-stream barrier. A singleton goes
direct immediately. The final view release permits parent receive repost.

Immutable contiguous data already owned by the device's producer arena or
the connection's receive arena is borrowed by exact allocation range. SEND
retains that allocation without rewriting it, allowing different mirror
control headers to share the same payload. Unregistered or noncontiguous data
uses a bounded copy into the device arena. Per-CQ `direct_payload_bytes` and
`copied_payload_bytes` distinguish these paths; `direct_sends` counts all SENDs
using this layout. A failed SEND quarantines the canonical source allocation,
including when the WR retains a nested view.
The immutable source descriptor identifies its canonical allocation and arena
directly, so the send path validates its range without scanning cached arena
descriptors. Nested view offsets and detached C/FFI leases retain that same
allocation and registration.
An external descriptor whose enclosing owner retains a native allocation uses
the bounded arena range lookup when direct descriptor identification is
unavailable. That lookup retains the canonical registered owner before posting;
such views do not require a compatibility staging copy.

Producer sources and compatibility/packing staging use separate bounded
device arenas with independent descriptor admission. Both are registered at
device adoption under the existing two-arena memory reservation. A burst of
unretired staging SENDs cannot consume producer descriptors. Staging allocations
use the configured maximum data class so cached small slots cannot prevent
later larger transfers; actual copied and transmitted lengths remain unchanged.

### 3.1. Encoded logical control streams

A control larger than the 1010-byte FlatBuffer prefix uses transport message
`ERpcControlFragment`. Each fragment has the common correlation fields plus
stream ID, transport generation, sequence, exact logical offset and total,
negotiated data capacity, original message type/flags and START/END/ERROR flags.
Its data SGE contains the next encoded-control bytes. The original application
message has no separate data payload; ordinary small control plus direct data
keeps its existing path.

The sender admits one original encoding under `control_send_budget_bytes` and
the logical queue count. Its CQ owner walks one cursor, stages only the next
fragment and completes that cursor before taking the next logical frame on the
QP. Every posted WR retains the original source until its own completion.
Control staging descriptors use the configured `data_slot_capacity()` even
when the negotiated capacity or final fragment length is smaller. This keeps
the fixed SQ descriptor window reusable across mixed sizes and peer classes.
SEND carries only the actual fragment length. Rejected queue publication
restores the caller's original control ownership.

Control/data QP data capacity is `min(local_data_capacity, peer_data_capacity)`.
When TCP preflight supplies the bound peer capability, active CM setup allocates
and primes the negotiated receives before connect; the CM reply must confirm
that same capability before publication. Low-level diagnostics without preflight
prime after validating the reply. Passive setup already has the offer. Actual RX allocation/posting
and SEND validation use this minimum, which immutable connection status exposes
as `effective_data_capacity`. Contiguous slots retain their fixed local wire
capacity and are posted before active CM connect, so an immediate peer response
finds receive credits without entering an RNR retry cycle.

The receiver reserves the complete logical length under
`control_receive_budget_bytes` before assembly. One active message per QP
validates generation, identity, sequence, offset, total, negotiated capacity
and final flag. It copies each fragment once into one logical control owner
and releases the consumed RQ slot immediately. A result may exceed the entire
RQ window. Only a complete validated original Frame reaches the existing
application handler or RPC completion table.

Admission exhaustion retains only existing RQ owners in CQ FIFO order. It does
not carve replacement buffers. Completed controls retain admission through the
final native/FFI caller release; C ABI conversion moves their storage without a
second full-message copy. Storage is destroyed before admission wakes CQ
workers through the retained operation gate. Active partial messages expire
under `control_assembly_timeout_ms`; a first fragment waiting for consumer byte
admission has no arbitrary consumer hold deadline. Close, cancellation and
expiry discard incomplete assembly, while completed caller-owned results stay
valid after transport shutdown.

Logical RPC counts, fragment counts and `copied_control_send_bytes` /
`copied_control_receive_bytes` are distinct from direct application payload
copy counters. Directional admission reports current bytes, high-water and
stalls. Ordinary contiguous traffic does not scan control-stream deadlines.

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
`RpcServer` and asynchronous service callbacks share the transport owner.
Stopping the server closes admission and connections; outstanding disk
callbacks retain their connection, transport and buffer until completion.
An externally supplied application pool must outlive these retained users.

`allocate_producer_buffer` lends storage from the device-owned send arena after
CM context adoption publishes that arena. Its registration charge and arena
owner survive the transport. `require_registered` rejects unavailable
registration; an exhausted active arena never substitutes an ordinary heap
allocation. Without an active registered arena, optional allocation uses the
ordinary bounded application pool. `WritableBuffer` exposes only unique
uninitialized capacity until its producer publishes an initialized prefix;
immutable views then retain the lease through their final release. Allocation
alone does not bypass the contiguous transport's payload copy.

The DiskIO client exposes a retained `ProducerBufferAllocator` through the
ChunkIO disk-writer interface. S3 attaches it to `NativeBodyAllocator` before
sharing that allocator. Hyper fills disjoint payload regions in this native
backing; `NativeFramedOwner` finalizes frame regions in place and publishes
immutable views. The backing lease returns before HTTP credit waiters wake.
An exhausted active producer arena pauses body intake. HTTP owner release
wakes existing AtomicWakers; a 1 ms retry wake also observes credits returned
by other producers or native SEND completion. This retry does not flush or
reassemble application messages.

Service constructors can supply layout and data-capacity defaults before
environment overrides. DiskIO server/client select control/data with 1 MiB
data capacity. Ordinary RPC constructors retain contiguous defaults; this
selection alone does not enable RDMA or change the explicit fallback policy.

The device owner charges MR creation against
`registered_memory_budget_bytes`. Zero derives a checked bound from all
CQ-admitted QPs, their send/receive arenas and the device producer/staging
arenas. An explicit budget must admit those device arenas and at least
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

The CQ owner prepares bounded lists of SEND WRs for one QP and submits each
list with one `ibv_post_send`. This batches posting without packing payloads:
each WR retains its own variable-length SGEs and source owners. Input exhaustion,
the software window, or the round budget flushes the actual list immediately,
including a singleton. No message waits for a future request to fill a batch.
One preparation round admits at most 64 SEND WRs, 256 logical frames/fragments
and 4 MiB of wire preparation. QPs rotate between rounds, which continue CQ
progress when preparation or SQ admission runs out. Packed envelopes also
stop at 32 frames or 64 KiB of packed bytes. These limits bound the work done
before returning to completions independently of the CQ poll budget.

Receive dispatch independently admits at most 256 logical frames/fragments
and 4 MiB of wire data per round. A packed CQE counts every enclosed frame.
An envelope that cannot fit the remaining budget stays intact in the bounded
deferred receive list, retaining its posted slot until the next round. Later
receives preserve per-QP order while SEND CQEs can still retire. Control-stream
admission backpressure does not consume dispatch credits. Every valid envelope
fits a fresh round, so budget deferral always has a continuation with progress.

Both QP directions use `sq_sig_all = 0`. The owner signals the last WR of every
actual posted batch and retains a FIFO of posted sends. A successful tail CQE
retires that WR and its preceding unretired sends on the same SQ. Multiple
batches may remain outstanding; completion replenishes WR and wire-byte
admission without waiting for the entire window. This reduces successful SEND
CQEs only; each completed RECV still produces its own CQE. Posted receive
capacity and final consumer release remain independent peer credits.

A failed linked post uses `bad_wr` to retain the accepted prefix and release
only the unposted suffix. It closes the uncertain connection without replay.
An accepted prefix missing its planned signaled tail retains its owners until
error/flush completion or QP destruction and cancellation. Error CQEs use their
generation-tagged work records, including unsignaled WRs, rather than treating
an error as a successful FIFO watermark. Uncertain buffers are quarantined.

Optional receive batching transfers fully released slots through a bounded
per-QP MPSC repost queue. The queue holds each slot's final reference; it
does not allocate replacement receive storage. The CQ owner rotates QPs,
prepares stable linked RECV lists and flushes the actual partial list at the
round boundary and before idle. A singleton or cross-thread final release
therefore cannot wait for another message. Cold receive priming also flushes
its tail before connection publication. Partial provider failure retains only
the accepted prefix, releases unposted owners and closes the connection.

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

The semantic DiskIO client treats failure of an accepted RDMA read, write or
fsync as terminal for that operation. It does not reconnect and replay that
uncertain operation through TCP. Writes report their ambiguous outcome and
fsync preserves durability failure mapping. Known pre-submit rejection and
the existing TCP retry policy retain their separate handling.

An accepted read interruption retains its terminal type through the routed
DiskIO adapter, mirror/EC readers, frame verification and native object stream.
It cannot become a route refresh, replica switch, EC recovery or layout retry.
Ordinary disk failures and verified corruption retain their qualified recovery
rules. A native object body keeps the interruption visible on later pulls;
caller-held buffers remain valid independently of that failure.

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
after disk completion. Disk requests validate their complete control buffer,
payload length and physical zone bounds before submission. Disk callbacks use
the exact view address and retain their owners through short I/O, errors and
server shutdown.

A direct-layout READ reuses the request's reserved receive data allocation as
its disk destination. READ has no input payload and its control has already
been validated. The registered data region remains owned through disk and
response SEND completion, then becomes eligible for repost. This makes RQ
admission bound asynchronous reads as well as writes. A slow client retains
response receive leases; exhausted client receive admission retains server
SEND sources and eventually stops new request receives. Eligible aligned
O_DIRECT reads use that same allocation; alignment edges use explicit staging.
TCP uses the ordinary application pool and the same disk result mapping.

For Access Server and Chunk-KV, `ChunkKvRpcTransport` has fixed metadata and
aggregate connection groups. Point calls select 64 KiB; seek, bounded scan,
scan, multi-get and batch mutation select 256 KiB. An explicit override can
select 1 MiB. The server defaults to 256 KiB and negotiates the smaller class
for each QP. These are encoded FlatBuffer control bytes, including values and
aggregated results. Common control streaming reconstructs one existing message;
it adds no Chunk-KV aggregation or correlation layer.

Both groups share one `RpcClient`, completion reaper and global RPC ID
sequence, while retaining separate connection indexes and generation-based
invalidation. Existing logical mutation identities, duplicate suppression,
result order, typed errors and eligible retries remain in the application
layer. Transport generation validates fragments independently of the catalog
and partition owner epochs.

Access common `chunk_kv_rpc` settings provide `metadata_rdma` and
`aggregate_rdma` overrides consistently to S3, Iceberg, Iceberg GC and Dataset.
Missing overrides keep environment policy and fixed service classes. Server
`rpc_rdma` config owns listener policy. Explicit disabled RDMA selects TCP;
`allow_tcp_fallback` controls failed setup before payload. Client class groups
always disable listener creation.

An asynchronous C ABI handler's Frame retains its connection until final
request release. This owner is also required for assembled controls, which no
longer hold an RQ data lease. `ServerRequest::close_connection` closes that
retained connection when response encoding or submission fails, waking the
client instead of silently discarding its response.

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
send_batch_size = 16
receive_batch_size = 1
send_window_size = 0
send_window_bytes = 0
rq_depth = 1024
posted_receive_target = 1024
cq_count = 1
cq_depth = 8192
poll_batch = 256
verbs_poll_batch = 32
poll_empty_budget = 256
max_control_message_bytes = 33554432
control_send_budget_bytes = 67108864
control_receive_budget_bytes = 67108864
control_assembly_timeout_ms = 30000
```

CQ depth is per CQ; arena budgets and SQ/RQ depths are per QP. For example,
one 8192-entry CQ admits four QPs with the default send and posted-receive
limits. `poller_count` follows the number of initialized CQs. Small-message
benchmarks select 1024-byte slots and matching arena budgets explicitly.

The versioned C ABI and Rust `RdmaConfig` expose cold configuration.
`crowdb_rpc_server_configure_rdma_v2` accepts `wire_layout` separately from the
unchanged version-one config structure; the original entry selects contiguous
layout. `crowdb_rpc_server_configure_rdma_v3` adds a separately versioned
logical-stream config while preserving the version-one structure and previous
entry points. Rust `RdmaConfig::with_control_data_capacity` derives wire and
SQ/RQ arena capacities from an explicit service data class. Logical message
limits must be nonzero, fit FlatBuffer length bounds and fit each directional
logical byte budget; these budgets are independent of registered MR bytes.
`crowdb_rpc_server_configure_rdma_v4` adds a separately versioned posting config;
earlier entry points select its defaults without changing their structures.
`send_batch_size` bounds each linked post (1 through 256). `send_window_size = 0`
uses SQ admission; a nonzero value cannot exceed allocated SQ depth and is
also capped by provider capacity. Prepared and posted WRs share this window.
`send_window_bytes = 0` disables the additional wire-byte cap; a nonzero cap
must fit the maximum configured wire message. It charges prepared and posted
wire bytes until retirement, independently of arena allocation budgets.
The matching environment names are `CROWDB_RDMA_SEND_BATCH_SIZE`,
`CROWDB_RDMA_SEND_WINDOW_SIZE`, and `CROWDB_RDMA_SEND_WINDOW_BYTES`.
`receive_batch_size` independently bounds linked RECV posts (1 through 256),
with `CROWDB_RDMA_RECEIVE_BATCH_SIZE` as its environment setting. A value of
one keeps immediate final-release posting. Status separates actual send/receive
post calls, successful send CQEs, logical frames, direct payload bytes,
aggregate payload copy bytes, compatibility staging bytes and control-framing
copy bytes. Copy counters include actual preparation work, including retries
after temporary admission pressure.
`CROWDB_RDMA_WIRE_LAYOUT` uses 0 for contiguous and 1 for control/data.
Owned
status JSON contains requested settings, actual QP/CQ/provider capacities,
transport errors, registration budgets and process resource counters. Connection
status caches immutable provider values before publication and retains them
after close; it never dereferences a QP during teardown. Unsupported resource
measurements are null.

The standalone `crowdb-rpc-test --rdma` client requires strict RDMA workload
connections and unique QPs. Without `--rdma`, it explicitly selects TCP.
Both use the same coroutine echo workload,
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
- **I11:** An encoded logical control publishes exactly once after complete
  validation. Its admission remains charged until final caller release;
  incomplete controls never reach application dispatch.
- **I12:** Fragment staging uses the configured data class, so smaller cached
  allocations cannot consume every descriptor needed by a later larger stream.

## 10. Scope boundary

This design covers host-memory RC SEND/RECV RPC and data streams. RDMA READ,
RDMA WRITE, remote destination descriptors, and GPUDirect are outside the base
transport contract. A future memory adapter may add them only behind the same
request, ownership, completion, and fallback semantics.
