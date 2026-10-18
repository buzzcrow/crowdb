<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R239: RPC — RC SEND/RECV transport with TCP fallback

## Problem

CROWDB RPC has a common TCP transport but no usable RDMA transport. RDMA must
reuse RPC correlation and framing while combining epoll wakeups with bounded CQ
polling. It must be optional: unsupported endpoints must use TCP without
repeated RDMA attempts, and failures before payload submission must fall back
without changing request semantics.

## Solution

1. Add an RC SEND/RECV transport behind the common RPC `Transport` interface.
   Data messages use one 1 KiB control SGE and one selected data SGE; no CPU
   concatenation is introduced.
2. Use service discovery for coarse RDMA capability and TCP handshake for the
   current CM endpoint, device/GID identity, capability generation, protocol
   version, and supported data classes. Local configuration and probing can
   disable RDMA before CM setup.
3. Register the RDMA completion and CM event descriptors with the existing
   epoll engine. Epoll wakes the worker; the RPC library acknowledges events,
   re-arms notifications, polls bounded CQ batches, and dispatches completions.
4. Select RDMA as the preferred path for one logical connection and TCP as its
   one-way fallback. A TCP connection is never upgraded to RDMA. The shared
   request ID is used for idempotent or duplicate-suppressed retry.
5. Define CM, QP, receive posting, CQ error, drain, and shutdown states. A
   partial or uncertain RDMA stream fails as one operation and is never joined
   with TCP bytes.
6. Provide OCI image test profiles. The image contains `libibverbs`,
   `librdmacm`, and the provider libraries; the host supplies the kernel RDMA
   driver and HCA devices. The current hardware profile runs multiple
   containers on one physical host with `--network=host`, `--privileged`, and
   `--ulimit memlock=-1`, sharing one configured HCA and opening independent
   QPs. A later restricted profile maps only the required `/dev/infiniband`
   devices and uses `IPC_LOCK` plus a bounded memlock limit.
7. Keep two current E2E topology profiles. The TCP-only profile exposes no
   RDMA devices and verifies that capability discovery avoids all RDMA
   attempts. The shared-HCA profile runs multiple logical containers on one
   physical host and verifies multiple QPs and fallback. Physical multi-node
   topology is tracked by a separate follow-up plan and is outside R239 scope.
8. Use host networking for the current container profile. Every container
   receives one `CROWDB_PORT_OFFSET`; each service applies that same offset to
   its documented default port. The RDMA HCA device and physical HCA port are
   shared and are not offset. The default RDMA CM service port is `19101`, so
   the effective port is `19101 + CROWDB_PORT_OFFSET`.

## Dependencies

- R238 registered host-buffer foundation.
- `design-crowdb-rpc.md` common frame and correlation contract.
- Existing TCP epoll worker and transport error semantics.
- R240 and R241 consume this transport.

## Acceptance

- Given both peers advertise RDMA, when the TCP handshake completes, the
  client establishes the negotiated RC path and sends a valid two-SGE frame.
  Invariant: shared RPC framing. Integration test.
- Given no RDMA capability or local RDMA configuration, when a connection is
  created, TCP is selected without an RDMA attempt. Invariant: optional RDMA.
  Integration test.
- Given RDMA setup fails before payload submission, when an idempotent request
  is retried, TCP returns one correlated response. Invariant: one-way fallback.
  Integration test.
- Given an RDMA payload partially completes, when the connection fails, no TCP
  bytes are appended and uncertain buffers remain quarantined. Invariant:
  partial transfer isolation. Integration test.
- Given CQ and CM events arrive, when epoll wakes the worker, all currently
  available completions are drained without a private polling thread.
  Invariant: event/poll composition. Integration test.
- Given an OCI image with no RDMA devices, when the TCP-only profile starts,
  service discovery selects TCP without attempting RDMA. Invariant: optional
  capability. E2E test.
- Given multiple containers sharing one HCA, when the shared-HCA profile opens
  multiple logical connections, each QP remains correlated and bounded, while
  the test reports a single physical host rather than physical node isolation.
  Invariant: topology declaration matches test claim. E2E test.
- Given a prepared OCI runtime with the required RDMA devices and memlock
  limit, when the RPC flow runs as a non-root application user, the container
  completes CM/QP/send/receive/repost without invoking `sudo`, loading modules,
  moving links, or entering host namespaces. Invariant: privileged host setup
  is separated from unprivileged application execution. E2E test.
- Given the RDMA benchmark runner uses the same echo workload as the TCP
  regression benchmark, when it runs on a prepared HCA, it reports throughput,
  latency percentiles, and errors for matching concurrency cases. Invariant:
  transport regressions and large flow bugs are visible before downstream use.
  Benchmark test.
- Given an application linked against `crowdb-rpc`, when deployment provides
  verbs/rdmacm libraries, HCA device access, memlock, and peer reachability,
  the application uses the ordinary RPC API and the library selects RDMA or
  TCP without setup-script calls or namespace operations in application code.
  Invariant: RDMA deployment concerns stay outside the library consumer.
  Integration test and documented runtime contract.

Run:

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-rpc --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
```

The initial local command shape is:

```bash
docker run --rm --network=host --privileged --ulimit memlock=-1 \
  -e CROWDB_RDMA_DEVICE=mlx5_0 \
  -e CROWDB_RDMA_PORT=1 \
  crowdb:test
```

The second container uses the same physical host HCA and its own logical
connection. The restricted container profile is a follow-up hardening step
after the shared-HCA profile passes. Physical multi-node execution is specified
in the separate follow-up plan. With host networking, each container must use
a distinct port offset; the offset applies to TCP/RPC and RDMA CM service
ports, while `CROWDB_RDMA_PORT` remains the HCA physical port number.
