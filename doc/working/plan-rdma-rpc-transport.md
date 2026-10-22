<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RDMA RPC transport Plan

Upstream: [R239](../backlog/R239-rdma-rpc-transport.md),
[root RDMA plan](plan-rdma.md), and
[RDMA RPC design](../design/rpc/design-crowdb-rpc-rdma.md).

Goal: complete RC SEND/RECV ownership and fallback acceptance, together with
CLI coroutine benchmarks, bounded RPC aggregation, and one polling/event
worker per CQ, using the existing RPC regression runners.

## Progress

The registered-buffer foundation and initial RC SEND/RECV flow are committed.
SEND/RECV tokens retain stable owners, receive leases wait for final C++/FFI
release, and strict RDMA tests reject TCP fallback. Nanosecond histogram and
coroutine handoff/joining fixes are also committed and verified below.

The current CQ implementation assigns each QP to one bounded CQ, with one
fixed polling/event thread per CQ and separate CM event handling. Early
receives wait for application handler publication; QP teardown gates concurrent
posting. Per-connection arena ownership no longer occupies application
`Connection::user_data`. CQ and provider-returned QP capacity values are
retained, and poll-call counters use per-CQ shards. The CQ diff passed focused
tests, live HCA diagnostics, the full C++/FFI gate, formatting and workspace
clippy and scoped tree-lint. Tree-lint exits zero with style warnings.

Application reply allocation now uses a separate pool, and real HCA tests
prove held registered/receive leases survive pool/transport/CM-ID destruction.
The C ABI now returns independent handles for retained buffer references.

Portable and verbs-backed pools now share the same arena/lifetime machinery.
Still pending: registration admission across retired connections, bounded
transport aggregation, `bench rdma`/shared coroutine reporting, unified regression
orchestration, negotiated TCP capability preflight, failure injection, and the
two-container acceptance profiles. Capability serialization and automatic
consumption at the connection seam do not establish negotiated preflight:
`connect` still attempts CM before the required TCP preflight. Do not treat the
historical benchmark comments or older subtask ledger as completed acceptance.
Physical multi-node validation remains outside this scope in
[the follow-up plan](plan-rdma-multi-node.md).

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
- [ ] **Complete remaining transport and OCI acceptance**: negotiated TCP
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

### Benchmark audit and unified measurement

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
- [ ] **Unify the workload and report contract**: use one
  coroutine-driven RPC echo workload, one case manifest,
  and one JSON/CSV schema for native processes and two host-network
  containers. Require actual RDMA path and unique QP counts. Verify request
  correlation and response length/content; include submission rejection and
  native coroutine retry counters, which are not fully represented by the
  current Rust `EchoHandler` response-error counter.
  Audit the built-in echo allocator before using it as the common workload:
  it allocates replies from `conn->pool()`, which is currently the RDMA
  receive pool, while the diagnostic echo uses `server.pool()`. A completely
  preposted receive pool leaves no allocation admission for those replies.
- [x] **Unify histogram semantics**: use nanosecond input, fixed bounded
  memory, mergeable per-polling-owner shards, explicit zero/overflow counts,
  exact count/sum/min/max, and avg/P50/P90/P95/P99/P99.9 output. Remove the
  common C++/Rust histograms' 65.5-us underflow floor; do not retain a separate
  benchmark-only precision contract. Specify a shared bucket layout and
  golden vectors. Record timing origin and exclude warmup/drain from TPS.
- [x] **Replace competing CQ consumers before expanding the performance matrix**:
  `RdmaTransport` now assigns QPs to a bounded CQ pool and starts one fixed
  polling/event thread per CQ. Generic epoll workers handle CM events only.
  Busy polling continues under traffic; idle workers arm, poll again, then
  sleep on their private epoll engine. `poller_count` derives from `cq_count`.
  Native and OCI comparison with the same manifest remains in phase E;
  neither single-host topology establishes physical-node isolation.

Required result schema (client/server resource values are separate):

| Group       | Required columns                                                                                                             |
|-------------|------------------------------------------------------------------------------------------------------------------------------|
| Identity    | case_id, run_id, git_sha, binary hashes, topology, CPU/HCA identity, CPU affinity, actual path, fallback policy/count             |
| Workload    | client_mode, loader_count, producer_threads, in_flight_per_loader, connection_count, qpair_count, logical_ops_per_rpc, payload_bytes, wire_bytes, aggregation_enabled, aggregation_max_bytes, aggregation_max_frames, frames_per_send |
| Scheduling  | io_engines, io_workers, cq_count, poller_count, poll_batch, verbs_poll_batch, poll_empty_budget, epoll_wakeups, CQ event/poll counts |
| Capacity    | requested/effective sq_depth, rq_depth, posted_receive_target, cq_depth, wire_slot_capacity, arena bytes, application_buffer_capacity, send_queue_capacity, queue/pool high-water marks |
| Measurement | warmup_s, measure_s, repeat_index, latency_scope, submitted, completed_in_window, drain_completed, rpc_tps, logical_ops_s, payload_Bps |
| Latency     | latency_count, avg_ns, p50_ns, p90_ns, p95_ns, p99_ns, p999_ns, min_ns, max_ns, histogram_zero_count, histogram_overflow_count       |
| Errors      | errors_total, setup_errors, submit_rejects, timeouts, connection_errors, cq_errors, payload_mismatches, duplicate_completions, retries, report_errors, client_exit, server_exit, valid |
| Cost        | client/server CPU time, context switches, CPU migrations, registered bytes, quarantined bytes, available RNR/provider counters   |

All fields belong to one machine-readable row; unavailable values are null.
Human tables can show projections joined by case_id but always include errors
and validity. Normal shutdown WR flushes are separate from workload failures.
Each measured operation is one validated RPC round trip. Transport aggregation
may put multiple complete RPC frames in one SEND slot; count every correlated
RPC once and report SEND count and frames per SEND separately. Application
aggregation, if benchmarked, must report logical_ops_per_rpc and logical_ops_s
separately from rpc_tps.

Recommended sweep: single-loader latency baseline; then sweep coroutine count
with one QP/CQ/poller; then QP count; then CQ/poller count; then queue depths
and payload/slot size. Use one-second warmup and ten-second measurement, repeat
candidate peaks, and retain all trials and errors. Choose the highest repeatable
zero-error TPS and show its latency rather than selecting unrelated best
throughput and latency fields from different trials. Wire header/control bytes
count toward slot capacity, including at the 1-MiB class boundary.

## Agreed implementation sequence

The user selected joint implementation of the benchmark and CQ architecture
on 2026-10-22, then requested this plan before code changes. The primary runner
is `tools/benchmark/bench-rdma-rpc-regression.sh`: start the RPC server and use
`crowdb-cli bench rdma` as its client, sharing the workload implementation with
`bench rpc`. Thread-driven gtests remain transport diagnostics. Transport
aggregation is permitted for max TPS; `qpair_count` and `wire_slot_capacity`
are independent, reported parameters. These decisions replace earlier notes
deferring multiple CQs or forbidding transport coalescing.

### A. Contract and reliable timing

- [x] **Record the agreed scope**: amend this plan, the root decisions, and
  R239's high-level contract; preserve historical measurements as historical.
  Files: this plan, `plan-rdma.md`, `../backlog/R239-rdma-rpc-transport.md`.
- [x] **Replace the histogram floor**: use a shared C++/Rust logarithmic
  nanosecond bucket layout with bounded memory, explicit zero/overflow, exact
  count/sum/min/max, and P50/P90/P95/P99/P99.9. Keep existing metric callers
  compatible, expose mergeable snapshots, and verify shared golden vectors,
  sub-microsecond values, bucket boundaries, empty/reset and overflow cases.
  Files: `lib/crowdb-common/{cpp,rust}/` histogram implementations and tests;
  `app/crowdb-cli/src/commands/bench/{loader,result}.rs`.
- [x] **Make coroutine completion safe before parallel CQ benchmarks**:
  `co_on_complete` currently fails a CAS against `CO_RUNNING`, then stores
  `CO_RESPONSE_READY`; `await_suspend` can change to `CO_SUSPENDED` between
  those operations and lose the resume. Replace that handoff with one atomic
  publication. Replace concurrent `coroutine_handle::done()` inspection and
  forced timeout resume/destruction with explicit final-suspend publication
  and callback quiescence. Keep timeout handling in the client's request
  completion/reaper path. Add deterministic completion-before/after-suspend
  and concurrent handoff tests, plus real coroutine echo/error coverage.
  Files: `client/co_client.cpp`, a private coroutine state header, RPC tests
  and FFI coroutine API. This is prerequisite to attributing multi-CQ errors
  or performance to the transport.

Verified coroutine checkpoint:

- `co_on_complete` publishes readiness with one atomic exchange, and final
  suspension publishes completion once. The joining thread sleeps while
  waiting and checks callback quiescence before reclaiming state or Rust
  context; it never inspects a concurrently running coroutine handle or
  force-resumes a pending await. Closed-connection submission rejection is
  counted and stops that coroutine. Configure the existing request reaper for
  stalled requests; its ordinary completion path handles timeouts.
- Rust `co_spawn` returns native `CoStats`, including submission rejection
  errors; `CoBenchHandler::on_response_data` borrows response bytes for payload
  validation. CLI report integration remains pending in phase D.
- Three C++ handoff cases pass, including 2,000 concurrent publication trials.
  Three Rust integration cases pass: 2,048 validated echoes across 64
  coroutines/four connections/four I/O workers, timeout completion, and a
  rejected closed connection. The full 29-test RPC FFI suite, 133-case native
  RPC CTest suite (four hardware cases skipped), fmt, workspace clippy and
  scoped tree-lint pass. No sanitizer or multi-CQ claim is made by these tests.
- The FFI build now tracks private source headers as well as public headers;
  changes to the coroutine state or private transport headers cannot silently
  leave the native client stale.

Verified histogram checkpoint:

- C++/Rust share 18 golden vectors, inclusive bucket bounds, nearest-rank
  quantiles, exact 0..255 ns resolution, 128 sub-buckets per magnitude below
  `2^40 ns`, explicit zero/overflow counts and a 128-bit exact sum. Owned
  shards merge without atomics; final cumulative captures are exact after
  producers join. Concurrent live/window captures remain approximate.
- Both language suites cover sub-microsecond samples, empty/reset, nearest
  rank, exact extrema, overflow/carry, shard merge and concurrent final counts.
  All 79 Rust common tests, workspace clippy, formatting and scoped tree-lint
  pass. Latest full C++/FFI gate:
  `.crowdb-runtime/artifacts/unit/cpp-0837d1ed9b1e4171a8c7afdd54f955c8/`.
- The diagnostic thread workload now uses the common histogram. One 10 s
  hardware run with 1 s warmup, one producer/QP/worker, 32 in flight and 1 KiB
  slots completed 1,767,704 measured RPCs with zero errors or histogram overflow
  (176,770 TPS; avg 174.61 us; P90 195.58 us; P99 215.04 us). Submitted count
  equals measured + drained. This verifies the migrated diagnostic histogram,
  not the future coroutine max-TPS claim. Artifact:
  `.crowdb-runtime/artifacts/rdma/20261022T111938Z/results.csv`.

### B. CQ owners and resource ownership

- [x] **Create the bounded CQ pool**: add `RdmaConfig::cq_count`,
  `verbs_poll_batch`, and strict environment validation. Each CQ owns its
  completion channel, wake fd, fixed poller and bounded WR table. Assign each
  QP to one CQ at creation; SEND/RECV and repost retain that assignment.
  Distinguish requested from provider-effective depths and reject insufficient
  CQ capacity for the configured QPs and posted receives before traffic.
  Files: `rdma_transport.h`, `rdma_config.cpp`, `rdma_connection.cpp`,
  `rdma_work_request.cpp`, new private CQ implementation under `transport/rdma/`.
- [x] **Run one polling/event worker per CQ**: continuously poll while busy,
  dispatch callbacks on the owner, arm notification after the empty budget,
  poll again before sleeping, acknowledge events, and wake promptly on submit
  and stop. Keep CM management separate from CQ completion ownership; generic
  epoll workers cannot compete for that CQ. Stop producers, wake/join pollers,
  drain/quarantine WRs, then destroy QPs/CQs/channels. Test idle/busy transitions,
  arm races, contention, multi-CQ mapping, partial setup and repeated teardown.
  Files: RDMA transport/completion sources, `socket_transport.cpp`, RPC tests.
  Retain arenas in the connection's private transport context; leave
  `Connection::user_data` to its caller. Gate per-QP posting before destruction,
  and publish migrated CM IDs together with their owner map under the existing
  lifecycle mutex. Reclaim a completed WR entry before invoking an application
  callback or releasing source-buffer owners, so exact-depth handoffs can
  admit a reply/repost. Poll-call counters count verbs invocations and use
  per-CQ atomics with aggregation on status reads.
  A CQ worker may receive data before CM returns and the caller installs its
  frame handler. Retain those completed receives in a bounded per-CQ deferred
  list, with their WR/connection/pool owners, until handler publication wakes
  the CQ owner. Connection-level capability frames are consumed at the common
  connection seam. Cancellation takes exclusive CQ ownership to retire deferred
  work; shutdown joins pollers before destroying QPs/CQs. No hot-path lock is
  introduced. Test early receive/late handler, deferred cancellation, fixed-owner
  busy/idle wakeups and late buffer release.
- [x] **Separate application and receive allocators**: built-in echo and
  default handlers allocate from the common application pool, not a fully
  preposted receive arena. Receive posting and repost explicitly use the
  connection's retained RDMA receive pool. Test `posted_receive_target ==
  rq_depth` with request and response payload validation. Files: `connection.h`,
  RDMA setup/completion, `c_api.cpp`, `server/handler.cpp`, transport tests.
- [x] **Finish real registered-lease shutdown**: retain PD/context/MR owners
  until the last held receive lease is released, including after transport
  shutdown; no deregister-before-release, double repost, premature recycle or
  silent PD leak. Files: RDMA buffer pool and lifecycle sources, lease tests.
  `RdmaBufferPool` becomes a facade over a shared arena; each outstanding
  buffer retains that arena, including buffers returned by `register_buffer`.
  The arena retains a shared `RdmaDevice` PD owner and the `rdma_get_devices`
  context list. Deregister MR before releasing PD, then release the list.
  Replace raw-pointer freelist heads with tagged entry indices and atomic
  links to prevent concurrent allocation/recycle ABA and link data races.
  Enforce a hard total buffer-entry bound as well as the arena byte bound.
  Verify the C ABI handle layer too: `crowdb_rpc_buffer_ref` currently
  increments the inner buffer reference but returns the same wrapper, while
  `crowdb_rpc_buffer_release` deletes that wrapper. Return an independent
  opaque handle per retained reference and test both release orders, an
  external owner released exactly once, and a cloned real registered lease
  surviving pool/PD facade destruction. This is part of the lease contract.
- [x] **Finish portable pool parity**: the no-verbs `RegisteredBufferPool`
  still uses raw-pointer freelist heads and a facade pointer in outstanding
  buffers. Reuse the bounded shared-arena/index freelist machinery for that
  pool too, retaining detach semantics and optional registration. Add no-device
  concurrent recycle, held lease after facade destruction, byte/descriptor
  bounds, quarantine and detach tests. Make `SystemBufferPool` outstanding
  admission one atomic reservation so parallel reply allocation cannot exceed
  its declared count. Files: `buffer.h`, `buffer.cpp`, private common arena
  sources, RDMA pool wrapper and buffer tests. This closes the foundation
  contract without leaving concurrency safety dependent on HCA availability.
  The shared private `BufferArena` owns descriptors; `BufferRegion` owns its
  allocation and optional MR/PD owner. Detached portable descriptors keep arena
  ownership until their final callback, returning index admission only then;
  the detached byte range stays committed. Add `CROWDB_RPC_ENABLE_RDMA=OFF`
  as a supported CMake build switch so the no-verbs gate is reproducible even
  on this host with rdma-core installed.
- [~] **Bound registration across connection retirement**: add a byte
  reservation to the existing `RdmaDevice` resource owner. A held lease must
  retain that reservation after its QP releases CQ admission; a new QP cannot
  bypass the registered-memory bound by replacing an arena that is still held.
  Add `registered_memory_budget_bytes` (zero derives a checked bound from the
  CQ-admitted QP count, per-QP arenas and compatibility copy pools), current and
  high-water reserved bytes, and registration rejection/failure counters. Keep
  reservations atomic and retained by the MR owner until deregistration.
  Reject setup when either send or receive MR creation fails: a non-null pool
  facade alone does not prove registration succeeded. Test retained-lease
  exhaustion/release and concurrent reservations, plus invalid/overflowed
  budgets. Files: `rdma_device.{h,cpp}`, RDMA config/setup/pool sources and
  resource lifetime tests. Expose these values in phase D's status/report API.
  Include final detached-buffer quarantine accounting in the shared arena and
  verify it with a no-device case.

Portable pool verification:

- `BufferArena`/`BufferRegion` back both registered pool facades; detach keeps
  its arena owner, and final detach release returns descriptor admission. Six
  portable tests cover held/detached leases, the hard descriptor and byte
  bounds, quarantine, 160,000 concurrent recycling iterations and concurrent
  `SystemBufferPool` count admission.
- `CROWDB_RPC_ENABLE_RDMA=OFF` builds successfully; all 123 CTest cases pass
  in that build. The enabled build passes all 42 focused buffer/completion
  cases (one compatibility-copy case skips before CM adoption). The real HCA
  held receive/registered-copy test passes ten repeated CM setup/teardown cycles
  after the shared-arena extraction.
- Full enabled C++/FFI gate and all 29 Rust RPC FFI tests pass:
  `.crowdb-runtime/artifacts/unit/cpp-26a123727c20408cbc7ef7f73be952f1/`.
  Formatting, scoped tree-lint and workspace clippy pass. Tree-lint retains its
  non-blocking style warning set.

Application allocator verification:

- `SocketTransport` passes its application pool into RDMA connections;
  `prime_receive` allocates from private `ConnectionPools::recv` explicitly.
  The C API and native test use the same built-in `handle_echo` implementation.
- A real HCA worker round trip with RQ/post target 1024 and exactly 1 MiB of
  1 KiB receive slots completed 1000 payload-validated requests with zero
  request errors. The server asserts the connection application pool equals
  `RpcServer::pool()`. This fixed-count case is functional evidence only.

Registered lifetime verification:

- A real registered lease, cloned through the C ABI, remains readable after
  its pool facade is destroyed. Final release destroys the arena/MR before
  the retained PD owner; `ibv_dealloc_pd` returns zero. Two separate C ABI cases
  reproduce the original shared-wrapper bug before the fix and pass afterward,
  covering both release orders and exactly one external-owner callback.
- Eight producer threads complete 160,000 allocate/clone/recycle iterations
  without duplicate live slot ownership or payload corruption. Oversized
  allocation failures preserve smaller-class descriptor admission.
- The live local-HCA CM case holds a receive lease and a `register_buffer`
  copy across destruction of both transports, both CM IDs, and caller-held
  connection handles. Both payloads remain readable; releasing their final
  references expires the respective device owners. Ten repeated setup/teardown
  cycles pass. This covers real MRs rather than only the emulated pool.
- Closed-connection flushes count in `shutdown_flushes`; open-connection flush
  and other CQ errors still fail the stream. The focused regression verifies
  these counters independently.
- Full C++/FFI gate before the final C ABI delta passes at
  `.crowdb-runtime/artifacts/unit/cpp-df0a8b16e65c44dfb32c065e6a50cd1e/`.
  After that delta, all 149 native RPC CTest cases pass (five opt-in cases skip
  there), all 29 Rust RPC FFI tests pass, changed formatting and scoped
  tree-lint and workspace clippy pass.
- The same 2-QP/2-CQ/2-poller, 2-producer/window-32 diagnostic profile now
  completes 7,828,933 measured RPCs in 10 s: 782,893 TPS, avg 78.96 us,
  P90 88.58 us, P99 100.35 us, zero request errors/overflow, 57 drained.
  The earlier CQ-only revision measured 827,807 TPS; this single run is about
  5.4% lower and is not sufficient to attribute a stable regression or identify
  max TPS. Keep both source/binary identities; do not sacrifice lease ownership
  to recover throughput. Latest artifact:
  `.crowdb-runtime/artifacts/rdma/20261022T131359Z/`.

CQ verification:

- 27 focused capability/configuration/completion tests pass, including CQ
  reservation rollback, partitioned tokens, independent polling gates, deferred
  receive ownership, wake-on-handler-publication, cancellation and exact-depth
  callback admission. A fixture allocation failure was diagnosed in GDB and
  fixed by separating application-buffer admission from the WR exhaustion
  being tested; it was not a transport CQ crash.
- The final diagnostic uses two native processes on one HCA, two QPs/two CQs
  per peer, one polling thread per CQ, one generic I/O worker per peer, two
  producer threads, 32 in flight each, 128-byte payload, 1 KiB slots,
  SQ/RQ/post target 1024, CQ depth 4096 per CQ, and 1 MiB send/receive arenas
  per QP. After 1 s warmup, 10 s measured traffic completed 8,278,071 RPCs:
  827,807 TPS, avg 74.92 us, P90 86.02 us, P99 97.28 us, zero RPC errors and
  zero histogram overflow; 62 requests drained afterward. Actual QP/CQ/poller
  counts are asserted. This is a transport diagnostic, not the primary CLI
  coroutine max-TPS result. Artifact and binary/source identity:
  `.crowdb-runtime/artifacts/rdma/20261022T124101Z/`.
- A prior successful diagnostic at
  `.crowdb-runtime/artifacts/rdma/20261022T122640Z/` measured 829,227 TPS with
  its retained binary hash. The subsequent source revision corrected/sharded
  poll-call statistics; do not combine measurements from these two binaries.
- `.crowdb-runtime/artifacts/rdma/20261022T122423Z/` is invalid: the runner
  started with the previous binary after a failed incremental build. Its
  `INVALID.md` explicitly excludes it from acceptance/comparisons. Later runs
  require a successful build before launch and retain binary/source hashes.
- The full C++ gate and all 29 Rust RPC FFI tests pass on the final CQ source.
  Complete logs and results:
  `.crowdb-runtime/artifacts/unit/cpp-c333ccb1d15145bfadc725c037c7be19/`.
  Workspace clippy and changed C++ formatting also pass.

### C. Bounded aggregation

- [ ] **Negotiate receive admission before coalescing**: finish TCP capability
  preflight before the CM attempt; reuse its TCP connection only for allowed
  pre-payload fallback. Publish actual CM address/port, per-instance generation,
  device/GID identity, receive slot capacity and supported aggregate frame count.
  Bind the CM private-data exchange to the preflight generation and capacities;
  reject stale/mismatched peers before publishing an RDMA connection. Direct
  low-level CM diagnostics also negotiate receive slot capacities. Strict RDMA
  permits the TCP control preflight but never TCP workload bytes. Keep no-device
  strict rejection before opening a socket. Retire a successful preflight TCP
  connection through the common worker cleanup path. This is a prerequisite
  for the peer-slot bound below; the remaining OCI/failure matrix stays in E.
  Files: RDMA capability/CM sources, `socket_transport.cpp`, server readiness,
  capability/path and hardware tests.
- [ ] **Coalesce complete frames on the CQ owner**: use bounded lock-free
  admission and per-QP queues; drain available frames without adding a waiting
  timer. Keep per-QP ordering and common frame/request IDs. Limit a SEND to
  `min(aggregation_max_bytes, local send slot capacity, peer receive slot
  capacity)` including every header/control byte and to
  `aggregation_max_frames`. Disabled mode sends one frame. Reject oversized
  single frames before admission; do not fragment or silently fall back.
  Files: RDMA submit/owner loop, `transport.h`, config/capability validation.
  Reuse `Connection::send_queue` for MPSC frame admission. Keep a bounded
  per-CQ connection directory, changed only under its existing CQ ownership
  gate during cold setup/retirement. The CQ owner drains per-QP queues with a
  fixed SEND budget and retains one overflow frame for the next slot; do not
  read a lifecycle-mutated owner map in the polling loop or introduce an
  atomic-shared-pointer load with hidden locking on every poll. Wake a sleeping
  owner with a pending-publication/idle handshake rather than an eventfd write
  for every RPC. Retire admitted/pending frames on every failure path.
- [ ] **Retain aggregate owners through completion**: one SEND WR retains all
  constituent frames until CQE; failure closes the uncertain stream and
  quarantines its slot once. Validate the entire received aggregate before
  publishing any frame. Give each frame an independent immutable buffer view
  sharing the receive-slot lease; repost only after all C++/FFI views release.
  Test exact slot fit, one byte over, multi-frame correlation/order, malformed
  suffix, held middle frame, send errors, bounded exhaustion and shutdown.
  Files: RDMA completion/WR code, buffer views and focused RPC tests.
  Link constituent `OutFrame` owners in the SEND WR without a per-frame vector
  allocation. Parse the full receive slot into bounded header/offset records
  before the first callback. Each published payload view has its own descriptor
  and view offset, sharing only the parent slot lease; `ref_clone` on the parent
  alone cannot represent different immutable offsets safely. Keep a parent
  reference until every frame has been dispatched. Defer a mixed capability/data
  aggregate if the application handler is not yet published.
- [ ] **Expose aggregation observations**: export actual frames/SEND, wire
  bytes, queue high-water and rejects. Keep RPC TPS distinct from transport
  SEND rate; compare aggregation disabled/enabled for both TCP and RDMA.
  Files: transport statistics, C API/Rust FFI and benchmark reports.

### D. CLI coroutine workload and canonical results

- [ ] **Add `bench rdma` with shared echo workload**: reuse `bench rpc`
  coroutine machinery; expose `qpair_count`, loader count, warmup/duration,
  slot/aggregation and CQ/queue knobs using the same code variable names as
  the report. RDMA command requires RDMA and disables TCP fallback. Preserve
  existing TCP command behavior; no OS thread per loader. Reject inconsistent
  settings before starting. Files: CLI `bench.rs`, `bench/verb.rs`,
  `bench/rpc.rs`, shared workload/config modules.
- [ ] **Validate and measure each completion**: expose response data to
  `CoBenchHandler`, verify request ID, length and payload, count submission
  rejects/retries/timeouts/connection/payload/duplicate errors, and reconcile
  C++ coroutine statistics with Rust results. Warm up for one second, measure
  completions for ten seconds using a monotonic deadline, then drain separately;
  exclude setup/warmup/drain from TPS. Define latency from submission to
  validated response in nanoseconds. Never destroy a coroutine while a late
  completion can resume it (the lifecycle prerequisite is in phase A).
  Files: `co_client.cpp`, FFI `co_bench.rs`, CLI echo
  handler and lifecycle tests.
- [ ] **Write the canonical report**: emit a versioned JSON result directly
  to a requested file, plus human output; serialize all schema columns above.
  Use explicit null for unavailable counters. Record measured errors and mark
  invalid on setup/client/server/report failure or RDMA path mismatch. Include
  client/server resource settings and actual QP/CQ/poller/path/fallback stats.
  Files: CLI benchmark result/transport FFI modules and report tests.

### E. One runner, staged evidence and requirement completion

- [ ] **Unify regression orchestration**: keep the named RDMA regression
  script as the entry point and share common orchestration with TCP regression.
  Launch the RPC server then the CLI client from one case manifest; wait for
  actual RDMA readiness, not just the TCP listen message. Save complete logs,
  canonical JSON and a wide CSV/TSV with errors/validity; propagate failures.
  Record git/binary hashes, topology, CPU/HCA, affinity, CPU time and supported
  provider counters. Tests cover invalid/missing reports, reported errors,
  failed/early-exit server, disabled fallback and actual argument propagation.
  Files: `tools/benchmark/bench-{rdma-,}rpc-regression.sh`, common runner and
  `tools/benchmark/tests/rdma_regression_test.py`.
- [ ] **Run gradual native hardware profiles**: first one coroutine/QP/CQ,
  then loader sweep 1/2/4/8/16/32 and higher as needed, then QP and CQ sweeps;
  compare aggregation off/on, SQ/RQ 1024 and 4096, small slot classes and larger
  payload/slot pairs. Increase depth only when counters show it limits load.
  Each case uses 1 s warmup + 10 s measured traffic. Repeat candidate peaks
  three times and retain all rows. Require zero workload errors/fallback and
  report avg/P90/tails with the highest repeatable TPS. Do not claim a hardware
  maximum when CPU/affinity/topology still limits the result.
- [ ] **Run matching OCI profiles and finish protocol tests**: use the same
  CLI workload and manifest in two host-network containers on this physical
  HCA; record separate process/container topology. Complete negotiated TCP
  preflight, fallback-before-payload, no mixed stream, no-device TCP and
  non-root device/memlock acceptance. Compare matching native/container rows
  and explain any gap. Files: RDMA OCI runner, capability/path tests.
- [ ] **Gate and document each coherent change**: build with `-j32`; run all
  executables/tests/lints through `pixi run`, use `clean-env` before server
  tests, and keep complete failure output. Run affected tests, fmt, clippy,
  changed C++ format/tree-lint, then full C++/FFI gates. Update permanent design
  to implemented behavior; remove requirements/plans only after all acceptance
  passes. Keep an explicit verified/pending checkpoint after each commit.

Provider references for the implementation choices:

- [libibverbs QP capacity terminology](https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/man/ibv_create_qp.3).
- [RDMA-CM receive posting before connection readiness](https://github.com/linux-rdma/rdma-core/blob/master/librdmacm/man/rdma_create_qp.3).
- [RDMA-CM RNR behavior and default delay](https://github.com/linux-rdma/rdma-core/blob/master/librdmacm/man/rdma_accept.3).
- [RDMA-CM device context retention](https://github.com/linux-rdma/rdma-core/blob/master/librdmacm/cma.c)
  (`rdma_get_devices` increments device references; `rdma_free_devices`
  releases them). Retaining the matching CM context list avoids substituting
  an independently opened context for a CM-owned QP.

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
- capability serialization and explicit pre-payload fallback policy; TCP preflight remains pending;
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
- implement multi-CQ and a fixed polling/event worker per CQ together with
  the CLI coroutine benchmark, as agreed in the implementation sequence above;
- rerun the RDMA benchmark concurrency and payload cases after the acceptance
  changes.
  Earlier script results claiming 442k/414k/376k ops/s did not establish the
  RDMA path; replace them with validated canonical results from the CLI runner.

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

- [ ] **Implement pre-payload fallback**: if CM/QP setup or first submission
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

- `lib/crowdb-common/cpp/include/crowdb-common/metrics/latency_histogram.h`
- `lib/crowdb-common/cpp/src/metrics/latency_histogram.cpp`
- `lib/crowdb-common/rust/src/metrics/histogram.rs` and `histogram/data.rs`
- `lib/crowdb-common/testdata/latency_histogram.csv` and both language tests
- `lib/crowdb-rpc/src/client/co_client.cpp` and private coroutine state header
- `lib/crowdb-rpc/src/buffer_arena.{h,cpp}`, `registered_buffer_pool.cpp`,
  shared buffer facade and registered/real-MR lifetime tests
- `lib/crowdb-rpc/ffi/src/co_bench.rs` and C/Rust statistics bindings
- `app/crowdb-cli/src/commands/bench/{verb,rpc,loader,result}.rs`
- `tools/benchmark/bench-rdma-rpc-regression.sh`, shared RPC runner and tests
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
