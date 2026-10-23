<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RDMA mixed-size send efficiency Plan

Upstream: user-requested transport optimization,
[RDMA RPC design](../design/rpc/design-crowdb-rpc-rdma.md),
[accepted RDMA implementation](plan-rdma.md),
[RPC flow analysis](../design/rpc/rpc-flow-analysis.md), and
[physical-node validation](plan-rdma-multi-node.md).

Goal: improve completed client requests per CPU cost under mixed message sizes,
using registered payloads, bounded small-message aggregation, batched WR posting,
and continuously replenished per-QP send windows without waiting to fill batches.

Status: planned; code paths and external references inspected. No implementation
or new performance runs have been performed. Delete this temporary plan after
implementation, verification, and migration of accepted contracts/evidence.
Existing SDK/backlog work remains independent of this transport optimization.
After mixed-workload acceptance demonstrates a repeatable resource/throughput
benefit within the declared latency bounds, continue the existing
[R242 SDK plan](plan-rdma-fat-client-sdk.md), as authorized by the user.
Keep that conditional handoff distinct from transport completion; do not start
Dataset integration under R220 as part of R242.

## Existing behavior and intended changes

- Coroutine benchmark loaders are closed-loop: each has one outstanding request
  and issues its next request after its response. Multiple loaders can run on
  one completion thread. Loader count, producer thread count, CQ owner count,
  QP count, and outstanding SEND count are different quantities.
- `ConnectionPools::send_queue` is bounded MPSC admission. Its fixed CQ owner
  drains frames. `sends_in_flight` already caps posted but unreclaimed SEND WRs
  at `min(sq_depth, effective_capacity.max_send_wr)` per QP.
- Both layouts currently post one WR per `ibv_post_send`; QP setup uses
  `sq_sig_all = 1`. `poll_batch` is a loop budget, not a linked WR submission.
- `Contiguous` copies complete frames into one registered slot and can pack
  several requests. `ControlData` uses a fixed 1024-byte control prefix and
  actual-length payload; registered contiguous producer/receive ranges can be
  borrowed. It currently rejects transport aggregation.
- Receive buffers are posted before the message arrives. Sender SGE boundaries
  are not transferred to the peer. The fixed prefix and negotiated payload
  capacity preserve scatter placement for variable lengths and data alignment.
- Current echo builds payloads through ordinary buffers, copies the server echo,
  and validates response bytes. The existing fixed-size peak is a regression
  control, not evidence of efficiency under mixed service traffic.

## Execution rules

- Retain RC SEND/RECV, control/data separation, setup-only TCP fallback,
  request identities, stream sequencing, generation checks, and quarantine.
- Producers prepare immutable source buffers and publish through existing
  lock-free admission. One CQ owner prepares each QP's pending aggregate,
  ordered ready items, WR batches, and send retirement. Do not introduce a
  shared mutable aggregate written by multiple producers or a new hot-path lock.
- Prepare direct items before publishing them as ready; the posting stage must
  not discover that it needs to gather/copy an entire large payload. A proposed
  ready-item record retains prefix, data ranges, original logical frames,
  actual wire bytes, and completion owners. Symbol names are finalized in code.
- Keep one ordered stream per QP. A large/direct item seals the earlier pending
  aggregate before entering ready. Control-stream fragments remain ordered and
  cannot be interleaved with later application frames on that stream.
- Small aggregation may copy control and data. Eligibility includes complete
  encoded size, message semantics, and alignment/lease requirements; mandatory
  disk-aligned or directly retained payloads bypass aggregation. A large
  registered payload bypasses packing and retains its source allocation.
- Aggregate byte/frame limits and WR batch size are upper bounds. Seal the
  aggregate when full, when a direct/barrier item arrives, when the preparation
  budget ends, or when available input is exhausted. Submit a single ready item
  immediately when it is the only work. Do not wait for a future request.
- Bound preparation by items and copied bytes, and bound receive/callback and
  post work per round. Continue CQ progress when SQ/memory admission is full;
  a producer backlog must not starve completions or other QPs. Do not sleep
  with a sealable aggregate or sendable ready item. A byte/WR-blocked ready
  item may wait for the actual completion/release event without busy spinning.
- Treat software queue capacity, posted SEND WR count, retained bytes, and
  client requests awaiting responses as separate budgets. Release each charge
  only at its relevant ownership boundary; buffer views sharing an allocation
  must not multiply-count physical retained bytes.

## Phase 1: representative workload and baseline

- [ ] **Add deterministic per-request sizes**: extend CLI workload arguments
  and manifest cases with weighted sizes/seed and an explicit wire layout;
  record expected length per loader/request instead of one global
  `EchoWorkload::payload_bytes`. Validate ID, length, bytes, and response
  control for every request. Preserve fixed-size mode and TCP parity. Files:
  `app/crowdb-cli/src/commands/bench/rpc/{args,workload,runtime,report}.rs`,
  `tools/benchmark/rpc_bench_cases.py`, `app/crowdb-cli/tests/rpc_bench_test.rs`,
  `tools/benchmark/tests/rdma_regression_test.py`.
- [ ] **Capture service size distributions**: use existing HTTP/DiskIO and
  Chunk-KV corpora to retain actual control/data lengths, class boundaries,
  registration origin, alignment requirements, and lease-hold behavior.
  Add reproducible synthetic mixes spanning control-only, 128 B, 1/4 KiB,
  64/256 KiB and near negotiated capacity; include non-power-of-two sizes.
  Label synthetic mixes explicitly rather than claiming production traces.
  Files: `tools/rdma/run-datapath.py`, `tools/rdma/run-chunk-kv.py`,
  `app/crowdb-access-server/tests/common/rdma_datapath/`,
  `app/crowdb-chunk-kv-server/tests/common/rdma_chunk_kv/`.
- [ ] **Retain comparable baselines**: save source/binary identity, machine
  topology, CPU placement, actual paths, request mix/seed, worker/QP counts,
  budgets and error counters. Measure single-loader latency, multi-loader
  saturation, burst/idle transitions, and delayed consumer release. Keep
  offered work and application behavior identical in before/after pairs.
  Declare performance noise and acceptable per-size tail-latency bounds from
  repeated baseline runs before selecting optimized defaults. Files:
  `tools/benchmark/rpc_regression.py`, `tools/benchmark/rpc_bench_report.py`,
  `tools/benchmark/bench-rdma-rpc-regression.sh`.

## Phase 2: registered producers and copy accounting

- [ ] **Generate payloads in registered buffers**: audit the existing
  `allocate_producer_buffer` and Rust writable/frozen APIs, pass the transport
  producer allocator through the benchmark runtime, and write variable-length
  data into the final registered allocation. Retain an ordinary-buffer mode
  for compatibility and a paired registered-buffer baseline. Avoid allocating
  a Vec and then copying it merely to claim producer zero-copy. Files:
  `lib/crowdb-rpc/src/transport/rdma/rdma_producer.cpp`,
  `lib/crowdb-rpc/ffi/src/{buffer,co_bench}.rs`,
  `app/crowdb-cli/src/commands/bench/rpc/{runtime,workload}.rs`.
- [ ] **Preserve direct source ownership**: prepare eligible payload ranges
  using the existing producer/receive arena borrow checks; retain MR/PD,
  allocation identity and immutable source through send retirement. Cover
  nested views, shared mirror payloads, zero-length data and arbitrary
  supported lengths. Unregistered or unsupported scattered input uses a
  bounded, separately measured compatibility copy. Preserve usable pool
  classes when a small allocation precedes a larger one. Files:
  `lib/crowdb-rpc/src/transport/rdma/rdma_direct_send.cpp`,
  `lib/crowdb-rpc/src/transport/rdma/rdma_buffer_pool.cpp`,
  `lib/crowdb-rpc/tests/{producer_buffer,rdma_memory,rdma_lifetime}_test.cpp`.
- [ ] **Separate copy causes**: expose bytes copied for small aggregation,
  unregistered/scattered compatibility staging, control framing, and echo
  application work separately from borrowed direct payload bytes. Keep a
  registered-source echo/retained-lease mode distinct from the existing
  copy-echo workload; compare each mode against itself. Files:
  `lib/crowdb-rpc/src/server/handler.cpp`,
  `lib/crowdb-rpc/src/transport/rdma/{rdma_cq.h,rdma_status.cpp}`,
  `app/crowdb-cli/src/commands/bench/rpc/{status,report}.rs`.

## Phase 3: mixed direct and aggregate preparation

- [ ] **Define the negotiated aggregate format**: extend the formal RDMA
  contract before implementation. Retain the fixed control prefix; add an
  unambiguous aggregate envelope and bounded packed data containing complete
  logical control/data frames. Specify counts, lengths, offsets, maximum wire
  bytes, supported message types, and whole-envelope validation. Direct data
  retains its independent aligned allocation. Negotiate support explicitly;
  older peers keep the existing direct format, without new-format payload
  submission or midstream fallback. Files:
  `doc/design/rpc/design-crowdb-rpc-rdma.md`,
  `lib/crowdb-rpc/src/transport/rdma/{rdma_offer,rdma_direct_frame,rdma_aggregate}.{h,cpp}`,
  `lib/crowdb-rpc/include/crowdb-rpc/transport/rdma/rdma_capability.h`,
  `lib/crowdb-rpc/src/transport/rdma/rdma_capability.cpp`.
- [ ] **Add bounded preparation state**: extend `ConnectionPools` with an
  owner-only pending aggregate and ordered ready items, reusing existing
  input admission. Apply a small-message copy threshold and independent
  aggregate byte/frame limits. Check local and peer capacities before copying;
  preserve admission accounting through pending, ready and posted states.
  Do not implement an additional unbounded queue or independent per-producer
  aggregate that can retain a lone request indefinitely. Files:
  `lib/crowdb-rpc/include/crowdb-rpc/transport/rdma/rdma_transport.h`,
  `lib/crowdb-rpc/src/transport/rdma/{rdma_send,rdma_direct_send,rdma_control_send}.cpp`.
- [ ] **Dispatch mixed receive envelopes**: validate all packed frame bounds
  before publishing any frame; create retained views for individual payloads.
  A final caller release controls parent repost. Reject malformed envelopes
  without partial RPC publication. Keep large logical control fragmentation,
  handshake messages and alignment-sensitive service traffic on explicitly
  compatible paths. Files:
  `lib/crowdb-rpc/src/transport/rdma/{rdma_direct_receive,rdma_receive,rdma_aggregate}.cpp`,
  `lib/crowdb-rpc/tests/{rdma_aggregate,rdma_direct_frame,rdma_offer,rdma_control_stream}_test.cpp`.

## Phase 4: send windows and batched posting

- [ ] **Make the send window explicit**: retain `sq_depth` as QP allocation
  capacity and add a configurable software WR window no larger than effective
  SQ capacity, plus explicit queued/prepared/posted retained-byte bounds.
  Account control prefix, aggregation storage, and retained source allocations;
  separate per-QP limits from shared-arena limits. Reject an item that can
  never fit its configured admission rather than stalling forever. Keep
  posted receives as implicit peer credit; local SQ completion does not imply
  the peer has released/reposted its receive buffer. Files:
  `lib/crowdb-rpc/include/crowdb-rpc/transport/rdma/{rdma_config,rdma_transport}.h`,
  `lib/crowdb-rpc/src/transport/rdma/{rdma_config,rdma_send,rdma_completion}.cpp`.
- [ ] **Build linked WR batches**: post ready items from the same QP together,
  bounded by ready count, post batch cap, free WR slots and byte admission.
  Each WR retains its own variable-length SGEs and immutable owners. Keep WR
  and SGE descriptor storage stable through the post call. For partial post
  failure, use `bad_wr` to identify the accepted prefix; retire unposted items
  separately, fail/drain the uncertain connection, and never replay accepted
  operations as though they were unsubmitted. Files:
  `lib/crowdb-rpc/src/transport/rdma/{rdma_send,rdma_direct_send,rdma_control_send}.cpp`,
  `lib/crowdb-rpc/tests/{rdma_send,rdma_failure}_test.cpp`.
- [ ] **Retire sends by sequence**: change both active/passive QP setup to
  `sq_sig_all = 0`. Give posted WRs ordered per-QP sequences while retaining
  generation-tagged completion identity. Initially signal each actual batch
  tail, including a singleton. A successful tail retires its covered prefix,
  releasing WR/byte charges and owners exactly once. Keep multiple batches
  outstanding and refill freed space without waiting for an entire window.
  Handle error CQEs for unsignaled WRs, partial posts missing the planned
  signaled tail, stale CQEs, cancellation and QP destruction. Preserve
  conservative CQ/WR admission for error/flush completions. Files:
  `lib/crowdb-rpc/src/transport/rdma/{rdma_active,rdma_connection,rdma_work_request,rdma_completion}.cpp`,
  `lib/crowdb-rpc/tests/{rdma_completion,rdma_failure,rdma_lifetime}_test.cpp`.
- [ ] **Schedule bounded progress**: separate preparation, post and CQ budgets
  instead of treating `poll_batch` as all three. Interleave CQ processing and
  preparation/submission, round-robin QPs, and seal pending aggregates at a
  round boundary. Full SQs or slow peers must not stop receive/response work.
  Preserve idle arm/recheck/wake correctness, including caller-held leases
  released on another thread. Files:
  `lib/crowdb-rpc/src/transport/rdma/{rdma_poller,rdma_completion,rdma_send}.cpp`,
  `lib/crowdb-rpc/src/transport/rdma/rdma_cq.h`,
  `lib/crowdb-rpc/tests/{rdma_worker,rdma_send}_test.cpp`.
- [ ] **Batch receive replenishment**: collect only fully released receive
  owners and post bounded linked RECV lists through the assigned owner. Flush
  a partial replenish batch before idle or receive starvation. Preserve
  posted-receive credits, data alignment, and caller lease lifetime; handle
  partial post failure without losing or double-posting a slot. Files:
  `lib/crowdb-rpc/src/transport/rdma/{rdma_transport,rdma_direct_receive,rdma_receive,rdma_poller}.cpp`,
  `lib/crowdb-rpc/tests/{rdma_worker,rdma_lifetime,rdma_failure}_test.cpp`.
- [ ] **Expose consistent configuration and diagnostics**: thread new limits
  through C ABI, Rust config, CLI and runtime manifests with compatible
  defaults and validation. Report in-flight WR/bytes, ready/pending age and
  high-water, actual WRs/post, aggregation copy bytes, SQ stalls, peer receive
  pressure, and per-size latency. Prefer owner-sharded counters and sampled
  age checks over per-frame shared atomics. Files:
  `lib/crowdb-rpc/include/crowdb-rpc/c_api.h`,
  `lib/crowdb-rpc/src/c_api/transport.cpp`,
  `lib/crowdb-rpc/ffi/src/{sys,transport}.rs`,
  `lib/crowdb-rpc/src/transport/rdma/rdma_status.cpp`,
  `app/crowdb-cli/src/commands/bench/rpc/`, `tools/benchmark/`.

## Phase 5: practical acceptance and default selection

- [ ] **Verify concurrency and low-load progress**: cover one loader/one owner,
  many loaders/one owner, many producers on one QP, multiple QPs per owner and
  multiple owners. Submit one small request with no successor; it must finish
  without a batch-fill wait. Exercise continuous producers, small/large/small
  ordering, asymmetric request/response sizes, and a busy QP alongside an idle
  or slow QP. Files: `lib/crowdb-rpc/tests/rdma_worker_test.cpp`,
  `lib/crowdb-rpc/tests/rdma_direct_hardware_test.cpp`,
  `app/crowdb-cli/tests/rpc_bench_test.rs`, `tools/benchmark/`.
- [ ] **Verify bounded pressure and shutdown**: hold receive leases to exhaust
  peer RQ, saturate WR and byte limits independently, then release and verify
  recovery without hot spinning, leaks or premature reuse. Inject errors
  before/within a posted batch, at signaled-tail boundaries, during control
  fragmentation and at disconnect. Preserve typed uncertain outcomes and no
  TCP suffix after accepted RDMA. Files:
  `lib/crowdb-rpc/tests/{rdma_failure,rdma_lifetime,rdma_control_hardware}_test.cpp`,
  `lib/crowdb-diskio-client/tests/rdma_failure_test.rs`.
- [ ] **Run service parity and topology controls**: run existing HTTP/DiskIO
  and real Chunk-KV/tree/WAL corpora with mixed traffic and retained leases,
  comparing exact results and errors against TCP. Reuse native, shared-HCA OCI
  and no-device TCP profiles. Coordinate a physical two-host run through the
  linked multi-node plan before claiming production-network performance;
  local HCA traversal remains labeled as such. Record unavailable hardware
  gates as pending rather than replacing them with a synthetic success.
  Files: `tools/rdma/run-datapath.py`, `tools/rdma/run-chunk-kv.py`,
  `tools/rdma/run-oci-profile.sh`, `tools/rdma/run-timed-load.py`,
  `doc/working/plan-rdma-multi-node.md`.
- [ ] **Compare changes incrementally**: retain paired baseline, registered
  direct, hybrid aggregation, WR batching and selective-completion results.
  Use client completed requests/s as the primary throughput metric, plus CPU
  seconds/request at matched throughput and throughput at matched CPU budget,
  valid payload bytes/s, per-size P50/P99/P999, queue residence, registered and
  retained memory, errors and fallback. Record mixed workloads at low load,
  below saturation, saturation and overload/burst recovery. A bounded offered
  load case records intended-arrival-to-completion delay, not only latency
  of admitted requests. Do not infer open-load latency from closed-loop runs.
  Files: `tools/benchmark/`, `app/crowdb-cli/src/commands/bench/rpc/report.rs`.
- [ ] **Select defaults from evidence**: require zero correctness errors,
  bounded memory and eventual progress after pressure release. Demonstrate
  reduced payload copying on eligible registered large messages and a
  repeatable CPU-cost or request-throughput benefit on service-informed mixed
  workloads within the baseline-declared tail limits. Small aggregation copy
  is allowed and included in costs. Report regressions and leave unhelpful
  options disabled; a larger 128-byte peak alone cannot close this plan.
  Publish accepted contracts and results, then delete this plan. Files:
  `doc/design/rpc/design-crowdb-rpc-rdma.md`,
  `doc/design/rpc/rpc-flow-analysis.md`, changed config defaults.

## Consolidated files and checks

- Transport: `lib/crowdb-rpc/src/transport/rdma/` and
  `lib/crowdb-rpc/include/crowdb-rpc/transport/rdma/`.
- Producer/C ABI/Rust: `lib/crowdb-rpc/src/c_api/`,
  `lib/crowdb-rpc/include/crowdb-rpc/c_api.h`, `lib/crowdb-rpc/ffi/src/`.
- Workloads/reports: `app/crowdb-cli/src/commands/bench/rpc/`,
  `tools/benchmark/`, `tools/rdma/` and the existing service fixtures.
- Unit: RDMA config/offer/aggregate/direct-frame/send/completion/worker/lifetime
  and failure tests under `lib/crowdb-rpc/tests/`; Rust CLI/FFI contracts under
  their crates' `tests/`; Python manifest/report tests in `tools/benchmark/tests/`.
- Integration: real-HCA direct/control hardware tests, TCP/no-verbs compatibility,
  mixed-loader completion validation and slow-consumer lease pressure.
- E2E: HTTP/DiskIO and Chunk-KV native/OCI parity, no-device TCP, and separately
  identified physical-host performance evidence.
- Run all implementation builds/tests/lints through `pixi run`, with focused
  checks per phase before wider affected suites. Use Rust fmt/clippy, changed
  C++ clang-format/tree-lint, C++/FFI/CLI and affected access/Chunk-KV/DiskIO
  suites. Keep full errors; enable `CROWDB_E2E_PRINT_SERVICE_ERRORS=1` in CI or
  when debugging. Start long hardware runs in the background and poll.
- For this plan-only edit, check Markdown links and `git diff --check`; do not
  report implementation tests or performance acceptance as completed.

## External implementation references

These describe borrowed mechanisms, not CROWDB contracts or target QPS.
Inspected on 2026-10-23; pin the source revision when running comparisons.

- [bRPC RDMA documentation](https://github.com/apache/brpc/blob/master/docs/cn/rdma.md):
  registered IOBuf pools, RC, event/polling modes and peer receive-window ACKs.
- [bRPC RDMA endpoint](https://github.com/apache/brpc/blob/master/src/brpc/rdma/rdma_endpoint.cpp):
  local SQ and remote RQ windows, reference-based SGE gathering, selective
  signaling and solicited wakeups. Its inspected send loop posts WRs individually;
  its event aggregation must not be described as linked-WR posting or CQE removal.
- [bRPC dispatch](https://github.com/apache/brpc/blob/master/src/brpc/rdma_transport.cpp):
  polling defaults to dispatching application messages as bthreads; disabling
  bthreads allows inline handling. Poller count is not the process's CPU budget.
- [bRPC performance client](https://github.com/apache/brpc/blob/master/example/rdma_performance/client.cpp)
  and [server](https://github.com/apache/brpc/blob/master/example/rdma_performance/server.cpp):
  callback-replenished request windows; `queue_depth` is application outstanding
  requests, and `thread_num` starts bthreads. Attachments are prepared/reused;
  echo is optional and the server appends IOBuf references. This is not a
  mixed-size storage workload or a complete payload-validation benchmark.
- [bRPC general benchmark](https://brpc.apache.org/zh/docs/benchmark/):
  do not relabel its general framework results as measured RDMA performance.
  The inspected official RDMA guide does not publish a directly comparable
  single-core QPS claim. Any future comparison must retain mode, all worker
  CPU costs, attachment reuse/echo behavior, size and request window.
- [eRPC paper, sections 4 and 6](https://www.usenix.org/system/files/nsdi19-kalia.pdf):
  registered application buffers, preallocated short responses and different
  small-RPC/bandwidth experiment conditions. Its 10M headline combines outgoing
  and incoming RPC processing in a symmetric experiment.
- [verbs posting](https://man7.org/linux/man-pages/man3/ibv_post_send.3.html)
  and [InfiniBand section 10.8.6](https://www.afs.enea.it/asantoro/V1r1_2_1.Release_12062007.pdf):
  partial posting and later completion on the same SQ confirming earlier
  unsignaled sends. Keep error/drain ownership separate from normal retirement.
