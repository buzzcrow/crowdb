<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RDMA RPC transport Plan

Upstream: [R239](../backlog/R239-rdma-rpc-transport.md),
[root RDMA plan](plan-rdma.md), and
[RDMA RPC design](../design/rpc/design-crowdb-rpc-rdma.md).

Goal: add RC SEND/RECV to the common RPC transport with epoll/CQ event
composition and one-way RDMA-to-TCP fallback.

## Progress

The transport foundation now includes validated endpoint capability data
(protocol, generation, device, GID, port, and supported capacity classes),
explicit RDMA path states, and a pre-payload-only TCP fallback predicate.
Implemented since this checkpoint: active/passive RDMA-CM setup, RC QP
creation, two-SGE SEND construction, bounded receive posting, completion-channel
polling with CQ acknowledgement, error quarantine, validated receive-frame
dispatch into `Connection::on_frame`, and epoll external-fd registration.
The CQ path is now drained by an epoll worker through `completion_fd()` and
`drain_cq()` rather than a private polling thread. Capability handshake bytes
have a fixed wire representation with round-trip tests. Negotiated capability
exchange through the existing TCP handshake, common transport path
selection/fallback, receive repost after owner release, and two-peer OCI E2E
execution are now wired at the RPC seam: TCP connections exchange the fixed
capability frame, connections retain the peer capability, RDMA-enabled
connections are selected before TCP fallback, and completed RDMA frames retain
their buffers until the owning `Frame` is destroyed, then repost the receive
pair. The live host HCA local-loop CM/QP/send/receive flow now passes, including
receive repost and connection-level CQ handling. Remaining validation includes
executing the OCI profiles against a built `crowdb:test` image and exercising
the actual RPC worker path.

## R239 status checkpoint

Completed:

- active and passive RDMA-CM address, route, connect, and accept flow;
- connection-level PD, CQ, completion channel, and registered send/receive
  pools;
- initial receive posting, CQ completion dispatch, owner-release repost, and
  RNR retry configuration;
- RDMA-to-TCP capability negotiation and pre-payload fallback wiring;
- epoll registration for CM, transport CQ, and connection CQ events;
- live HCA local-loop CM/QP/send/receive E2E with all frames received and clean CQ
  status, and clean client/server exit;
- non-root runner selection and the library consumer/deployment contract.

Remaining:

- run the same flow through the actual `RpcServer`/`RpcClient` worker path,
  including frame dispatch and request correlation rather than the transport
  harness only;
- add mock/integration coverage for CM disconnect, QP error, CQ flush/error,
  epoll wakeup, and pre-payload TCP fallback;
- exercise multiple sequential frames on one QP to verify receive repost and
  buffer reuse under the worker;
- run single-node HCA local-loop multi-container and physical multi-node OCI profiles against the built image;
- remove temporary hardware diagnostics, normalize failure logging, and run
  the full C++ format/tree-lint gates.

## Phase 1: common transport seam

- [ ] **Trace TCP submission**: map `Transport::submit`, `Connection`,
  `OutFrame`, parser dispatch, completion callbacks, and connection shutdown.
  Files: `lib/crowdb-rpc/include/crowdb-rpc/transport.h`, `connection.h`,
  `out_frame.h`, TCP transport and worker sources.
- [ ] **Add RDMA capability state**: define unavailable, probing, connecting,
  ready, draining, failed, and TCP-fallback states. Keep path state per logical
  connection and prevent TCP-to-RDMA upgrade. Files: transport/connection
  headers and state tests.
- [ ] **Define endpoint handshake data**: represent coarse discovery result,
  CM address/port, GID/device identity, capability generation, protocol
  version, and supported data classes. Validate stale generation and peer
  identity before CM setup. Files: RPC control schema and endpoint config.

## Phase 2: CM and QP lifecycle

- [ ] **Implement device selection**: select configured HCA, port, local
  address, and GID; fail closed to TCP when local probing finds no usable
  device. Files: `rdma_transport.h/.cpp` and configuration adapter.
- [ ] **Implement active/passive CM flow**: create IDs, bind/listen or resolve
  address/route, create QP, connect/accept, and publish ready only after initial
  receive buffers are posted. Files: RDMA transport sources.
- [ ] **Implement connection teardown**: map CM disconnect, QP error, and CQ
  error to one drain/fail transition. Stop new work, resolve known completions,
  quarantine uncertain buffers, and wake blocked submitters.

## Phase 3: event/poll loop

- [ ] **Extend epoll registration**: add external event-fd registration and
  removal to the existing `EpollEngine` without changing TCP readiness rules.
  Files: `lib/crowdb-rpc/include/crowdb-rpc/transport/epoll/epoll_engine.h`,
  `lib/crowdb-rpc/src/transport/epoll/epoll_engine.cpp`.
- [ ] **Drain CM events**: acknowledge all received CM events, update the
  connection state, and re-arm the CM channel where required.
- [ ] **Drain CQ events**: acknowledge and re-arm completion notifications, then
  poll bounded batches until the current CQ work is drained or the worker
  budget is reached. No independent polling thread.
- [ ] **Make stop wakeable**: stop must wake epoll, stop new submissions, drain
  known work, and join without waiting forever on an idle channel.

## Phase 4: frame submission and completion

- [ ] **Build two-SGE WRs**: map the 1 KiB control buffer and selected data
  buffer to one SEND WR. Inline control only when the configured device limit
  allows; never concatenate control and data in the CPU.
- [ ] **Post receives**: prepost control/data receive buffers, associate each
  with a stable `wr_id`, and replenish only after the owner releases it.
- [ ] **Resolve WR identity**: `wr_id` must identify connection, request ID,
  stream ID, sequence, logical offset, buffers, and ownership state. Handle
  success, flush, local protection, retry, and remote access errors distinctly.
- [ ] **Dispatch common callbacks**: validate lengths and class before parser
  publication, then invoke the existing C++ completion path and FFI callback.

## Phase 5: fallback and observability

- [ ] **Implement pre-payload fallback**: if CM/QP setup or first submission
  fails before payload, select TCP with the same request identity.
- [ ] **Reject mixed completion**: after any payload submission, fail the stream
  on uncertainty and never enqueue a TCP suffix.
- [ ] **Add metrics**: expose RDMA capability, selected device, QP state, CQ
  depth, posted receives, RNR/retry errors, fallback count, and quarantined
  bytes.

## Tests

Unit tests:

- endpoint capability and generation validation;
- connection-state transitions and illegal TCP-to-RDMA upgrade;
- frame class/length validation and two-SGE construction;
- `wr_id` mapping and completion state transitions;
- fallback boundary and request-ID reuse rules.

Integration tests:

- RDMA-disabled build and host selects TCP without CM calls;
- mocked CM/CQ setup, reconnect, CQ error, flush completion, and shutdown;
- epoll external event wakes worker and drains all current CQ completions;
- send/receive sequence preserves exact control/data lengths;
- pre-payload failure retries idempotent request on TCP;
- partial payload failure never appends TCP data.

E2E tests:

- TCP-only OCI profile;
- single-node HCA local-loop profile with multiple QPs;
- physical multi-node profile with one RDMA NIC bound per container.
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
- OCI image and E2E runner configuration

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
independent QPs. Multi-node OCI runs use the same application contract, with
each container bound to the RDMA NIC on its own physical node. No namespace
operation is part of the test or application path.

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
When any local capability, CM, QP, or pre-payload setup step is unavailable,
the library reports the reason and uses TCP without requiring application code
changes. After payload submission, the connection fails as one stream and does
not append a TCP suffix.
