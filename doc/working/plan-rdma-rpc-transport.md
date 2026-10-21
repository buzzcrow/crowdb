<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RDMA RPC transport Plan

Upstream: [R239](../backlog/R239-rdma-rpc-transport.md),
[root RDMA plan](plan-rdma.md), and
[RDMA RPC design](../design/rpc/design-crowdb-rpc-rdma.md).

Goal: add RC SEND/RECV to the common RPC transport with epoll/CQ event
composition and one-way RDMA-to-TCP fallback.

## Progress

The transport implementation now includes validated endpoint capability data
(protocol, generation, device, GID, port, and supported capacity classes),
explicit RDMA path states, and a pre-payload-only TCP fallback predicate.
Implemented since this checkpoint: active/passive RDMA-CM setup, RC QP
creation, one-MR arena-backed wire slots, one-SGE SEND/RECV posting, bounded receive posting, and the initial completion-channel
polling with CQ acknowledgement, error quarantine, validated receive-frame
dispatch into `Connection::on_frame`, and epoll external-fd registration.
The CQ path now uses one transport-wide shared CQ for all QPs. Its completion
channel is registered with one epoll engine; the worker drains completions with
a bounded busy-poll budget and rearms notification after the queue is idle.
Epoll is therefore only the wakeup path, while completion dispatch remains in
the CQ drain. Stop now wakes the owning engine, CQ polling and completion-error
counters are exposed, and CM disconnect events close and retire their connection
pools before QP/ID teardown. The OCI runner now also has an unprivileged
shared-HCA profile with explicit verbs/CM devices, IPC_LOCK, and bounded
memlock. Capability handshake bytes
have a fixed wire representation with round-trip tests. Negotiated capability
exchange through the existing TCP handshake, common transport path
selection/fallback, receive repost after owner release, and two-peer OCI E2E
execution are now wired at the RPC seam: TCP connections exchange the fixed
capability frame, connections retain the peer capability, RDMA-enabled
connections are selected before TCP fallback, and completed RDMA frames retain
their buffers until the owning `Frame` is destroyed, then repost the receive
pair. The live host HCA local-loop CM/QP/send/receive flow now passes, including
receive repost and connection-level CQ handling. The actual `RpcServer`/
`RpcClient` worker path now passes a live request/response round trip with
multiple sequential frames on one QP, including CQ wakeup, frame dispatch,
request correlation, response completion, and clean teardown. Remaining
acceptance includes failure-injection coverage, pre-payload TCP fallback proof,
stable completion ownership mapping, and executing the single-node
multi-container OCI profiles against a built `crowdb:test` image. Physical
multi-node validation is deferred to [the separate follow-up plan](plan-rdma-multi-node.md).

The C++ unit/integration gate and Rust FFI tests pass after the teardown and
completion-error changes; `tree-lint` completes with the repository's existing
clang-tidy warning set. The OCI runner now mounts a named data volume and uses
the configured RDMA local interface by default. TCP-only and unprivileged
shared-HCA startup smoke tests both reached the container readiness message;
the unprivileged smoke correctly fell back to TCP because this container host
did not expose a usable HCA device. A privileged multi-container HCA run still
requires the prepared local-loop topology.

## R239 status checkpoint

Completed:

- active and passive RDMA-CM address, route, connect, and accept flow;
- connection-level PD and registered send/receive slot pools;
- initial receive posting, CQ completion dispatch, owner-release repost, and
  RNR retry configuration;
- RDMA-to-TCP capability negotiation and pre-payload fallback wiring;
- epoll registration for CM and CQ wakeup events;
- live HCA local-loop CM/QP/send/receive E2E with all frames received and clean CQ
  status, and clean client/server exit;
- actual `RpcServer`/`RpcClient` worker-path request/response round trip with
  sequential frames and clean teardown;
- contiguous one-SGE wire-slot send/receive flow with 128B, 1KiB, and 4KiB
  payload benchmark smoke tests reporting zero errors;
- non-root runner selection and the library consumer/deployment contract.

Remaining implementation and acceptance work:

- add mock/integration coverage for CM disconnect, QP error, CQ flush/error,
  epoll wakeup, receive exhaustion, and pre-payload TCP fallback;
- make each completion's `wr_id` resolve a stable connection/request/slot
  owner and test success, flush, protection, retry, and remote-access errors;
- run the single-node HCA local-loop multi-container OCI profile against the
  built image;
- remove temporary hardware diagnostics, normalize failure logging, and run
  the full C++ format/tree-lint gates;
- keep multi-CQ and a dedicated polling worker as follow-up performance work;
  they are not R239 blockers unless the acceptance profile shows contention;
- rerun the RDMA benchmark concurrency and payload cases after the acceptance
  changes.
  Completed on the local HCA at 128B, 1KiB, and 4KiB with 0 errors for 1, 4,
  8, and 32 connections. The 4-connection cases reached about 442k ops/s
  (128B), 414k ops/s (1KiB), and 376k ops/s (4KiB).

## Phase 1: common transport seam

- [x] **Trace TCP submission**: map `Transport::submit`, `Connection`,
  `OutFrame`, parser dispatch, completion callbacks, and connection shutdown.
  Files: `lib/crowdb-rpc/include/crowdb-rpc/transport.h`, `connection.h`,
  `out_frame.h`, TCP transport and worker sources.
- [x] **Add RDMA capability state**: define unavailable, probing, connecting,
  ready, draining, failed, and TCP-fallback states. Keep path state per logical
  connection and prevent TCP-to-RDMA upgrade. Files: transport/connection
  headers and state tests.
- [x] **Define endpoint handshake data**: represent coarse discovery result,
  CM address/port, GID/device identity, capability generation, protocol
  version, and supported data classes. Validate stale generation and peer
  identity before CM setup. Files: RPC control schema and endpoint config.

## Phase 2: CM and QP lifecycle

- [x] **Implement device selection**: select configured HCA, port, local
  address, and GID; fail closed to TCP when local probing finds no usable
  device. Files: `rdma_transport.h/.cpp` and configuration adapter.
- [x] **Implement active/passive CM flow**: create IDs, bind/listen or resolve
  address/route, create QP, connect/accept, and publish ready only after initial
  receive buffers are posted. Files: RDMA transport sources.
- [x] **Implement connection teardown**: map CM disconnect and CQ completion
  errors to one close/fail transition. Retire the connection pools before QP/ID
  destruction, quarantine uncertain buffers, and wake the owning engine on stop.

## Phase 3: event/poll loop

- [x] **Extend epoll registration**: add external event-fd registration and
  removal to the existing `EpollEngine` without changing TCP readiness rules.
  Files: `lib/crowdb-rpc/include/crowdb-rpc/transport/epoll/epoll_engine.h`,
  `lib/crowdb-rpc/src/transport/epoll/epoll_engine.cpp`.
- [ ] **Drain CM events**: acknowledge all received CM events, update the
  connection state, and re-arm the CM channel where required.
- [x] **Create shared CQ**: all QPs use the transport-wide CQ and completion
  channel, so multiple connections share one completion stream.
- [x] **Add bounded CQ polling**: drain the shared CQ with an empty-poll
  budget, then arm CQ notification for the next epoll wakeup.
- [x] **Make epoll a CQ wakeup**: acknowledge the CQ event and resume polling;
  epoll does not scan or dispatch per-connection CQs.
- [x] **Make stop wakeable**: stop wakes the owning epoll engine, then shutdown
  drains known CQ work before destroying IDs and pools.

## Phase 4: frame submission and completion

- [x] **Build one-SGE wire slots**: allocate one fixed-size registered slot
  from the send ring, copy header/control/payload contiguously, and post one
  SEND SGE with the actual wire length. Reject messages larger than the slot.
- [x] **Post one-slot receives**: prepost one full registered receive slot per
  receive ring entry, associate each with a stable `wr_id`, and replenish only
  after the owner releases it.
- [~] **Resolve WR identity**: `wr_id` must identify connection, request ID,
  stream ID, sequence, logical offset, buffers, and ownership state. Handle
  success, flush, local protection, retry, and remote access errors distinctly.
- [x] **Dispatch common callbacks**: validate lengths and class before parser
  publication, then invoke the existing C++ completion path and FFI callback.

## Phase 5: fallback and observability

- [~] **Implement pre-payload fallback**: if CM/QP setup or first submission
  fails before payload, select TCP with the same request identity.
- [x] **Reject mixed completion**: after any payload submission, fail the stream
  on uncertainty and never enqueue a TCP suffix.
- [x] **Add metrics**: posted receives, fallback count, quarantined bytes, CQ
  poll/empty-poll counts, CM disconnects, flush, local-protection, retry, and
  remote-access errors are exposed; QP state, CQ depth, and RNR counters remain.

## Tests

Unit tests:

- endpoint capability and generation validation;
- connection-state transitions and illegal TCP-to-RDMA upgrade;
- frame class/length validation and one-SGE wire-slot construction;
- `wr_id` mapping and completion state transitions;
- fallback boundary and request-ID reuse rules.

Integration tests:

- RDMA-disabled build and host selects TCP without CM calls;
- mocked CM/CQ setup, reconnect, CQ error, flush completion, and shutdown;
- multiple connections sharing one CQ pool preserve completion ownership and
  request correlation;
- epoll external event wakes worker and drains all current CQ completions;
- send/receive sequence preserves exact control/data lengths inside one slot;
- pre-payload failure retries idempotent request on TCP;
- partial payload failure never appends TCP data.

E2E tests:

- TCP-only OCI profile;
- single-node HCA local-loop profile with multiple containers sharing one HCA.
- non-root OCI profile: host prepares RDMA and network topology, the container
  receives only the required `/dev/infiniband` devices and memlock allowance,
  and the RPC flow runs as the application user without `sudo`, module loads,
  link moves, or host namespace entry.

## Consolidated files

- `lib/crowdb-rpc/include/crowdb-rpc/transport.h`
- `lib/crowdb-rpc/include/crowdb-rpc/connection.h`
- `lib/crowdb-rpc/include/crowdb-rpc/transport/epoll/epoll_engine.h`
- `lib/crowdb-rpc/src/transport/epoll/epoll_engine.cpp`
- `lib/crowdb-rpc/include/crowdb-rpc/transport/rdma/rdma_transport.h`
- `lib/crowdb-rpc/src/transport/rdma/rdma_transport.cpp`
- `lib/crowdb-rpc/tests/` transport, fallback, and event-loop tests
- OCI image and single-node multi-container E2E runner configuration

## Verification gates

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-rpc --all-targets
pixi run -- cargo test -p crowdb-rpc-ffi --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
pixi run rs-lint
pixi run tree-lint
```

Do not hand off to R240/R241 until a framed RPC stream works over RDMA and TCP
fallback has passed the partial-transfer boundary tests.

## Container launch contract

The host performs privileged RDMA setup with `setup-rdma.sh local`. Each
container receives the required `/dev/infiniband` devices, device-access group,
the selected HCA mapping, and bounded memlock limit. The RPC process runs as
the application user and does not use `sudo`, load kernel modules, move links,
or enter host namespaces.

The local hardware smoke test is:

```bash
sudo tools/rdma/setup-rdma.sh local
tools/rdma/run-local-loop.sh local
```

Multiple containers on one node use the same HCA local-loop model and open
independent QPs. Physical multi-node OCI runs are outside this plan and are
specified in the separate follow-up plan. No namespace operation is part of
the test or application path.

## Library consumer contract

An application linking `crowdb-rpc` does not run the RDMA setup script and does
not need to enter a network namespace. Deployment prepares the host or
container once; the application then uses the normal RPC server/client API.

The deployment layer must provide:

- a loaded RDMA kernel provider and an `ACTIVE` HCA port with a routable local
  address/GID;
- `libibverbs`, `librdmacm`, and the matching provider library in the runtime
  image;
- access for the application user to the required `/dev/infiniband` devices;
- a sufficient memlock limit for registered buffers;
- peer reachability on the selected RDMA address and port.

The application layer only supplies the normal RPC endpoint configuration. The
library discovers usable devices and selects RDMA before TCP when the peer
advertises compatible capability. `CROWDB_RDMA_DEVICE`, `CROWDB_RDMA_PORT`, and
`CROWDB_RDMA_SOURCE_ADDR` are deployment overrides, not required API calls.
For host-network multi-container runs, deployment also supplies one
`CROWDB_PORT_OFFSET` per container. The library and service launchers apply the
same offset to every default TCP/RPC and RDMA CM service port; each colocated
RPC service adds its stable `CROWDB_RDMA_SERVICE_SLOT` to the CM port so
services do not contend for one listener. The HCA physical port selected by
`CROWDB_RDMA_PORT` is unchanged. The default RDMA CM service port is `19101`.
When any local capability, CM, QP, or pre-payload setup step is unavailable,
the library reports the reason and uses TCP without requiring application code
changes. After payload submission, the connection fails as one stream and does
not append a TCP suffix.
