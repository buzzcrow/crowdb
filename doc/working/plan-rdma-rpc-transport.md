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

The completion ownership mapping now uses a bounded, generation-tagged
`WorkRequest` table for SEND and RECV completions. It retains the connection,
request, stream, sequence, and logical offset. Error dispatch uses the retained
work kind rather than an undefined completion opcode. A receive lease is
reposted by `Buffer::last_release_cb` only after the final C++/FFI owner drops
it; held caller leases survive frame deletion and transport shutdown.

The current code also preposts receives before `rdma_connect`/`rdma_accept`,
uses a short configurable RNR timer, drains acknowledged CQ work before CM
close, and fixes CQ registration and buffer-pool teardown ordering. The earlier
64-request burst exposed RNR stalls and premature test-server teardown. The
worker test now has a client-finished barrier, configurable workload size,
worker count, QP count, bounded in-flight window, per-request payload validation,
and TPS/P50/P95/P99 output.

## Current continuation checkpoint

- [x] **Use standard queue terminology**: `sq_depth`, `rq_depth`,
  `posted_receive_target`, `cq_depth`, `wire_slot_capacity`, `send_arena_bytes`,
  and `receive_arena_bytes` are the configuration variables. SQ/RQ map to
  verbs `max_send_wr`/`max_recv_wr`; byte budgets use 64-bit values so a 4096
  entry RQ with 1 MiB slots can represent its full 4 GiB receive arena.
- [x] **Verify queue/buffer regressions**: the 32-way C++ build and 26 focused
  configuration, buffer, work ownership, CQ/CM, and epoll tests passed.
- [x] **Verify the small-message functional baseline**: 1000 requests,
  128-byte payload, one worker, one QP, one in-flight request, 1 KiB wire
  slots, `sq_depth = rq_depth = posted_receive_target = 1024`, and 1 MiB send
  and receive arenas passed at 71,682 TPS, P50 13.08 us, P95 17.99 us,
  P99 20.69 us, and zero RPC errors. Posted receives flushed at normal peer
  shutdown are reported separately by the transport counters. These short
  fixed-count results are functional evidence only, not sustained load evidence.
- [x] **Complete staged small-message load acceptance**: discuss these actual code variables
  with the user, then run each profile for one second of warmup and ten
  seconds of measured traffic. Count successful completions inside that
  interval; exclude warmup and post-window draining. Increase
  `producer_count`, `io_workers`, and `connection_count` through 1, 2, 4, 8,
  16, and 32, with a separate one-in-flight serial baseline and 32 in flight
  per producer for pipelined profiles. Validate 1024 and 4096 RQ depths;
  use small wire slots for small-message tests and larger slots/arenas for
  later realistic payloads. `RdmaHardwareTest.RpcServerClientTimedLoad` and
  `tools/rdma/run-timed-load.py` retain complete stage logs and CSV results
  below `.crowdb-runtime/artifacts/rdma/`. Completion correlation, payload
  contents, duplicate completion, accepted-versus-completed counts, and
  server request totals must agree at every stage.
- [x] **Rerun code gates**: `pixi run test-cpp`, the 26 Rust RPC FFI tests,
  `cargo fmt --all -- --check`, `rs-lint`, changed C++ formatting, and
  `tree-lint` passed on this diff. Tree-lint exits zero with style warnings.
  The first full C++ run hit the existing 128-byte temporary-path limit;
  rerunning under `CROWDB_RUNTIME_ROOT=.crowdb-runtime/rdma-check` passed.
  Full gate artifacts:
  `.crowdb-runtime/artifacts/unit/cpp-3ab318e0c5a04f27a8328be78ca1cab6/`.
- [~] **Complete remaining transport and OCI acceptance**: negotiated TCP
  capability preflight, real registered-lease PD/MR lifetime verification,
  and multi-container profiles remain open. Do not treat the historical
  gate record below as verification of these unfinished contracts.

### Timed load evidence

- `tools/rdma/run-timed-load.py` now runs one-second warmup followed by a
  ten-second completion window for every profile. It validates unique QPs,
  RDMA path selection, request correlation, 128-byte payload contents, drain
  counts, and server totals. Metrics retain bounded per-producer histograms
  and in-flight slots rather than one entry per request.
- The 16-worker run exposed competing CM consumers taking passive setup
  events. Passive setup now uses a private CM channel until ESTABLISHED,
  and shared CM event handling has one atomic consumer.
- The 32-worker run exposed a lost continuation: a competing worker could
  clear `continuation_fd_` before acquiring the CQ. Only the CQ consumer now
  clears it. The bounded epoll continuation test reproduces that contention
  deterministically; all 26 focused tests pass after the fix.
- Every profile in the corrected matrix completed its full ten seconds with
  zero RPC, correlation, or payload errors. The single-in-flight baseline
  completed 626,983 requests (62,698 TPS, P99 26.24 us).
- All rows below use `producer_count = connection_count`,
  `in_flight_per_producer = 32`, `wire_slot_capacity = 1024`,
  `sq_depth = rq_depth = posted_receive_target = 1024`. Each I/O worker
  count is **per peer**, with both peers on one physical host/HCA.

| Producers / QPs | TPS: I/O workers = producers | TPS: I/O workers = 1 |
|-----------------|------------------------------|---------------------|
| 1               | 182,431                      | 183,340             |
| 2               | 292,921                      | 322,559             |
| 4               | 327,623                      | 356,282             |
| 8               | 354,189                      | 396,683             |
| 16              | 206,622                      | 392,360             |
| 32              | 83,218                       | 292,424             |

- Complete corrected matrix:
  `.crowdb-runtime/artifacts/rdma/20261022T100944Z/results.csv`.
- Fixed-I/O-worker control:
  `.crowdb-runtime/artifacts/rdma/20261022T101119Z/results.csv`.
- The `rq_depth = posted_receive_target = 4096` control, with
  `receive_arena_bytes = 4 MiB` per QP and `io_workers = 1`, passed at
  369,719 TPS for 8 producers/QPs (P99 761.86 us) and 278,266 TPS for 32
  (P99 4653.06 us), both with zero errors. Complete logs:
  `.crowdb-runtime/artifacts/rdma/20261022T101244Z/results.csv`.
- The control supports investigating shared-CQ worker contention and CPU
  scheduling before increasing receive depth. It does not establish the HCA
  hardware throughput limit. CPU affinity and frequency are not fixed.
- Remaining contract work is separate from this sustained small-message
  acceptance: complete TCP capability preflight before CM probing and prove
  fallback, retain real PD/MR lifetimes through held leases, and run the OCI
  profiles. Existing capability serialization and state predicates alone do
  not prove that every connection follows the negotiated setup contract.

### Benchmark audit and proposed unified measurement

User decisions: use coroutine loaders for the primary throughput benchmark;
keep producer-thread stress as diagnostic coverage; include errors in every
performance result; prohibit TCP fallback in RDMA benchmarks; redesign the
latency histogram to resolve sub-65-us RTTs.

- [x] **Audit historical baseline**: the retained 128-byte run at
  `.crowdb-runtime/artifacts/bench/rdma-rpc-20261018-220617/server/metrics.log`
  records TCP socket read/writev data for all cases. Its 32-connection count
  is 3,560,660 payloads over approximately three seconds, matching the
  script comment's 1,186,876 TPS. Treat that number as TCP-path evidence,
  not a verified RDMA baseline. The old `results.tsv` files contain error
  rows despite successful shell exit; the script also expects JSON while
  the current CLI emits a human report.
- [x] **Add explicit fallback policy**: `CROWDB_RDMA_ALLOW_TCP_FALLBACK=0`
  sets `SocketTransport::allow_tcp_fallback_` false. Unavailable, disabled,
  or uncompiled RDMA then returns connection failure before opening a TCP
  socket. The default remains enabled; invalid policy values disable TCP
  fallback. Count only allowed fallback attempts. Five path-policy tests
  and five existing capability tests pass.
  The full 126-test native RPC suite passes (hardware-only cases skip there),
  and the actual hardware worker round trip also passes with fallback disabled.
- [x] **Reject false benchmark success**: both RDMA load runners set the
  strict policy. The regression script preserves complete per-case client
  output, records `client_exit`, `report_errors`, and `valid`, and returns
  nonzero for a failed client or invalid report. Missing RPC error counts
  are `NA`, not zero. A rebuilt-client invalid-device reproduction returns
  client exit 2 and script exit 1. Retained evidence:
  `.crowdb-runtime/artifacts/rdma/strict-policy-script-fixed/`.
  Changed C++ formatting/tree-lint and workspace clippy pass. The historical
  table is now annotated as unverified RDMA data instead of a regression gate.
  Four runner contract tests cover a successful report, a failed client with
  preserved error count, a missing report despite exit zero, and reported RPC
  errors despite exit zero. These use fixture processes without an HCA:
  `pixi run -- python -m unittest discover -s tools/benchmark/tests -p rdma_regression_test.py -v`.
- [ ] **Unify the workload and report contract after the test-design
  decision**: use one coroutine-driven RPC echo workload, one case manifest,
  and one JSON/CSV schema for native processes and two host-network
  containers. Require actual RDMA path and unique QP counts. Verify request
  correlation and response length/content; include submission rejection and
  native coroutine retry counters, which are not fully represented by the
  current Rust `EchoHandler` response-error counter.
  Audit the built-in echo allocator before using it as the common workload:
  it allocates replies from `conn->pool()`, which is currently the RDMA
  receive pool, while the diagnostic echo uses `server.pool()`. A completely
  preposted receive pool leaves no allocation admission for those replies.
- [ ] **Unify histogram semantics**: use nanosecond input, fixed bounded
  memory, mergeable per-polling-owner shards, explicit zero/overflow counts,
  exact count/sum/min/max, and avg/P50/P90/P95/P99/P99.9 output. Remove the
  common C++/Rust histograms' 65.5-us underflow floor; do not retain a separate
  benchmark-only precision contract. Specify a shared bucket layout and
  golden vectors. Record timing origin and exclude warmup/drain from TPS.
- [ ] **Confirm CQ ownership before expanding the performance matrix**:
  current `RdmaTransport` has one `cq_` per peer, no dedicated polling
  thread, and competing epoll workers protected by `cq_polling_`.
  `RdmaTransport::start()` only changes lifecycle state. The recommended
  target is one assigned polling/event thread per CQ, with QPs assigned
  to one CQ, bounded busy polling under traffic, and arm/poll/epoll sleep
  after the idle budget. Proposed `cq_count` and `poller_count` are not
  existing configuration knobs. Compare native and OCI runs with the same
  manifest; neither single-host topology establishes physical-node isolation.

Proposed result schema (client/server resource values are separate):

| Group       | Required columns                                                                                                             |
|-------------|------------------------------------------------------------------------------------------------------------------------------|
| Identity    | case_id, run_id, git_sha, binary hashes, topology, CPU/HCA identity, CPU affinity, actual path, fallback policy/count             |
| Workload    | client_mode, loader_count, producer_threads, in_flight_per_loader, connection_count, logical_ops_per_rpc, payload_bytes, wire_bytes |
| Scheduling  | io_engines, io_workers, cq_count, poller_count, poll_batch, verbs_poll_batch, poll_empty_budget, epoll_wakeups, CQ event/poll counts |
| Capacity    | requested/effective sq_depth, rq_depth, posted_receive_target, cq_depth, wire_slot_capacity, arena bytes, queue/pool high-water marks |
| Measurement | warmup_s, measure_s, repeat_index, latency_scope, submitted, completed_in_window, drain_completed, rpc_tps, logical_ops_s, payload_Bps |
| Latency     | latency_count, avg_ns, p50_ns, p90_ns, p95_ns, p99_ns, p999_ns, min_ns, max_ns, histogram_zero_count, histogram_overflow_count       |
| Errors      | errors_total, setup_errors, submit_rejects, timeouts, connection_errors, cq_errors, payload_mismatches, duplicate_completions, retries, report_errors, client_exit, server_exit, valid |
| Cost        | client/server CPU time, context switches, CPU migrations, registered bytes, quarantined bytes, available RNR/provider counters   |

All fields belong to one machine-readable row; unavailable values are null.
Human tables can show projections joined by case_id but always include errors
and validity. Normal shutdown WR flushes are separate from workload failures.
Each measured operation is one validated RPC round trip; current code copies
one OutFrame into one SEND slot and does not combine multiple RPCs merely
because they share a QP. Application aggregation, if benchmarked, must report
logical_ops_per_rpc and logical_ops_s separately from rpc_tps.

Recommended sweep: single-loader latency baseline; then sweep coroutine count
with one QP/CQ/poller; then QP count; then CQ/poller count; then queue depths
and payload/slot size. Use one-second warmup and ten-second measurement, repeat
candidate peaks, and retain all trials and errors. Choose the highest repeatable
zero-error TPS and show its latency rather than selecting unrelated best
throughput and latency fields from different trials. Wire header/control bytes
count toward slot capacity, including at the 1-MiB class boundary.

Provider references for the implementation choices:

- [libibverbs QP capacity terminology](https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/man/ibv_create_qp.3).
- [RDMA-CM receive posting before connection readiness](https://github.com/linux-rdma/rdma-core/blob/master/librdmacm/man/rdma_create_qp.3).
- [RDMA-CM RNR behavior and default delay](https://github.com/linux-rdma/rdma-core/blob/master/librdmacm/man/rdma_accept.3).

## Historical verification record

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
- [x] **Resolve WR identity**: `wr_id` identifies connection, request ID,
  stream ID, sequence, logical offset, buffers, and ownership state. Success,
  flush, local protection, retry, and remote access errors are accounted for;
  injected provider-error coverage remains in the acceptance checklist.
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
