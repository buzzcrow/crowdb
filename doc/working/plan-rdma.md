<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB RDMA implementation Plan

Upstream: [R5](../backlog/R5-rdma-alloc.md),
[RDMA RPC design](../design/rpc/design-crowdb-rpc-rdma.md), and the remaining
child backlog [R242](../backlog/R242-rdma-fat-client-sdk.md).

Goal: implement the host-memory RDMA SEND/RECV path end to end, with RDMA as
the preferred connection path, TCP as the one-way fallback, shared aggregation
semantics, bounded buffer leases, and automated OCI verification using a
single-node HCA local-loop with multiple containers.

This root plan persists until the umbrella goal is complete. Completed child
plans are deleted; remaining child plans retain their implementation detail.

## Current implementation status

Registered host buffers and the common RC SEND/RECV transport are accepted.
The shared arena has bounded lock-free admission, retained MR/PD/device
ownership, explicit leases and quarantine. TCP preflight binds CM setup to the
current endpoint and receive admission. QPs have one fixed polling/event owner
per CQ; bounded aggregation preserves complete frames. Strict RDMA CLI tests
share the coroutine workload and canonical results with TCP.

The final transport source `944274af` passes 543 tree, 192 RPC, 71 DiskIO and
35 common native cases plus both FFI suites, 160 no-verbs CTest cases, fourteen
real-HCA cases, formatting and scoped C++ lint. The unchanged Rust integration
passes 1,244 affected cases, workspace all-feature clippy and `rs-lint` at
`018da2fc`. Native and two non-root containers on one shared HCA pass all 26
final 1 s warmup + 10 s measurement trials. Median RDMA throughput is 3.027M
TPS raw-server and 3.237M OCI, with zero errors/fallback. Matching TCP medians
are 1.203M/2.168M raw-server and 1.095M/2.113M OCI with Nagle off/on. Exact
settings, identities, latency tails and the old TCP mislabeling are retained
in [RPC flow analysis](../design/rpc/rpc-flow-analysis.md) and both benchmark
wrapper scripts.

The HTTP/ChunkIO/DiskIO datapath is accepted: immutable registered HTTP owners,
aligned receive-to-disk handoff, read reuse through SEND completion, caller-held
receive credits, retained asynchronous service owners and terminal accepted-RDMA
failures. A 3,158,073-byte HTTP corpus crosses two chunks and matches
TCP/native/OCI bytes and geometry. All five functional profiles also pass the
common-control regression at `018da2fc`; peer-close fails a partial download
without a TCP suffix. The fixture injects placement/catalog state and records
its actual io_uring/OCI BlockingEngine selection.

Chunk-KV/access integration is accepted with distinct 64 KiB metadata and
256 KiB aggregate connection groups sharing request correlation. Protocol v4
supports bounded logical controls beyond one RQ window, independent byte
budgets, negotiated capacity and final-caller admission ownership. The real
Chunk-KV service/tree/WAL corpus verifies eight values totalling 3,409,975 bytes,
typed errors, mutation-ID replay and exact TCP/RDMA equality in seven native/OCI
profiles. Partial-fragment peer close publishes no incomplete result or TCP
suffix. Catalog/serving authority/memory ChunkStream backing are injected;
Access construction is covered by the affected Rust suites. Formal service
contracts and complete acceptance evidence are linked from the RDMA design
and RPC flow analysis.

The foundation, transport, datapath and Chunk-KV continuation is complete.
The subsequent mixed-size send optimization also passes its local-HCA
acceptance, as recorded below. The user authorized continuation with R242
after a repeatable benefit; its implementation now follows the existing
[SDK plan](plan-rdma-fat-client-sdk.md). Physical multi-node validation remains
in its separate plan.

## Accepted mixed-size send optimization

- Registered producer storage is filled once in place. A retained native
  descriptor supplies a constant-time ownership check; external HTTP/FFI
  wrappers retain the bounded canonical range lookup. Compatibility staging
  preserves usable allocation classes across small/large transitions.
- The existing second device MR now owns producer descriptors separately from
  staging/packing, without raising the registered-memory budget. Both native
  and external-view direct reuse are covered by HCA regressions.
- Linked SEND lists retain stable WR/SGE records and signal their actual tail,
  including a singleton. FIFO success retirement replenishes independent WR
  and wire-byte windows while several batches remain in flight. Partial post
  and error completions retain uncertain owners until drain, without replay.
- Control/data separation remains. Optional negotiated packing copies only
  adjacent eligible small messages, at most 32 frames / 64 KiB, without a
  batch-fill timer. Large data and alignment-sensitive DiskIO stay direct.
  SEND preparation and receive dispatch each have separate logical-frame and
  byte quanta; SEND, receive repost and CQ polling remain independently bounded.
- TCP preflight permits active ControlData RQs to be primed before CM connect.
  Repeated idle singleton tests caught and then verified the cold-start race
  without extending their one-second completion deadline.
- Default SEND batch is 16. Receive batching remains optional with default 1;
  its additional CPU benefit was absent. Packing remains opt-in on both peers.
  The reproducible mixed profile is
  `tools/benchmark/profiles/rdma-mixed-size.json`.
- Accepted final binary hashes: CLI
  `0df668817118cfab7dfc12966690afad9cd05ab4504741284ec9ceef3c1104c9`;
  native server
  `92794c08a31250f57ecbffb3755b1eeaf7e2e83b45011d27fd4f7b03f8ee1197`.
  Binaries, tracked diff and new sources are retained under
  `.crowdb-runtime/rdma-check/binaries/mixed-send-final/`.
- Final correctness gates: 209 native RPC cases pass with three explicit
  environment/process-pair skips; 128 no-verbs cases pass with one hardware
  skip; all FFI tests, five CLI cases, seventeen runner cases, Rust fmt/clippy,
  C++ formatting and scoped tree-lint pass. Scoped tree-lint still reports
  existing style warnings and exits zero.
- Actual multi-producer progress passes with four producers sharing one QP
  and with four QPs / 128 outstanding requests. Mixed coroutine, Tokio and
  four-CQ cases all validate every payload with no errors.
- Final repeated fixed-CPU mixed evidence is in
  `.crowdb-runtime/rdma-check/artifacts/bench/mixed-steady-final/`, with the
  declared original thresholds in `mixed-pinned-baseline/acceptance-limits.json`.
  Each peer is pinned to one distinct physical core (client 2, server 4), on a
  Ryzen 5950X and one shared 100G HCA. The deterministic synthetic mix includes
  0, 128, 513, 4096, 65535, 262144 and 1048576 bytes; seed 93; warmup 5 s,
  measurement 10 s, three repeats. No production-frequency claim is made.
- Against `mixed-steady-baseline`, median completed requests/s are singleton
  16,655.9 -> 16,919.2, 64 loaders / one QP 43,437.6 -> 48,842.7, and 128
  loaders / four QPs 50,180.5 -> 51,657.7. Total client+server CPU ns/callback
  over warmup/measurement/drain are 62,097 -> 60,587, 26,743 -> 19,224, and
  30,944 -> 23,153. Every size's P99 passes the declared baseline plus 10%;
  all rows have zero allocation, validation, timeout or transport errors.
  Copy echo is unchanged in both versions and included in CPU cost. P999 is
  recorded, without inferring open-arrival latency from closed-loop loaders.
- The fixed 128-byte regression control uses the same two-core pinning,
  512 loaders / four QPs, unchanged copy echo and packing in both versions.
  Its two repeats are 702,145 / 696,953 requests/s before and 877,811 / 879,331
  after, retained in `mixed-fixed-baseline/` and `mixed-fixed-final/`.
  These CPU-constrained controls are distinct from the earlier unrestricted
  3M headline; both final rows validate all requests with zero errors.
- Failed experiments remain retained: shared-owner baseline staging exhaustion,
  scalar and double-fill registered initializers, shared producer/staging
  descriptor exhaustion, and a fast-only borrow check that missed external
  HTTP/FFI owners. No failed row is counted as accepted performance.
- Service revalidation with TX/RX 16 and packing enabled preserves HTTP bytes,
  aligned DiskIO and zero copied payload bytes in native/OCI HCA profiles
  `rdma-datapath/c562dc95cc9841d6921b2dce5334a57f` and
  `rdma-datapath/f6a02c215e8d4f39ac695d4b98f1c4a6`. Native partial-download
  disconnect `670211f0f3d345269342eebd3d045717` terminates without a TCP suffix;
  no-device OCI TCP `926068fccb004ed9981e0e82cdf3af22` preserves exact bytes.
  Native uses io_uring; the container's existing EPERM fallback uses blocking
  disk I/O and is functional evidence only.
- Real Chunk-KV/tree/WAL passes native RDMA
  `rdma-chunk-kv/36d69f7a97764016950e28753b54340a`, final OCI RDMA
  `778a5ee288b543f38433f696afd148fa`, OCI TCP
  `b6333c5e1ddd4a859ed53720052de6fc`, native setup-only fallback
  `372dd806ab434dc28b917f841cfb1613`, and OCI partial-response disconnect
  `7f4dc5c2c0e54678b9d0a523a8b0bbed`. Corpus values cross the 64/256 KiB
  classes and 1 MiB control-fragment boundary. These are service-path tests
  with injected catalog/storage seams, not physical cross-host measurements.

Further tuning remains separate from this accepted mechanism: intended-arrival
open-load latency, sampled queue-age/peer-pressure telemetry and a retained-echo
benchmark mode. Transport counters already distinguish direct payload,
aggregation, compatibility staging and framing/control copies; application echo
copy remains explicitly identified. Physical multi-node performance continues
under `plan-rdma-multi-node.md`.

## Fixed design decisions

- Use one common RPC abstraction for TCP and RDMA.
- Use RC SEND/RECV. The accepted contiguous RPC layout uses one wire slot
  and one SGE per message. The accepted datapath adds a negotiated control/data layout with
  separate SGEs for immutable producer payload and per-target control; its
  total wire admission includes both regions.
- Use service discovery for coarse RDMA capability and TCP handshake for the
  current RDMA endpoint, capability generation, and data classes.
- Select RDMA before the first payload; fallback is RDMA to TCP only.
- Preserve existing ChunkIO and Chunk-KV aggregation. Additionally permit
  bounded transport coalescing of complete RPC frames without a batching timer;
  every SEND fits both the local send slot and peer receive slot, including
  headers, and retains each RPC's identity and completion ownership.
- Assign each QP to one CQ and each CQ to one fixed polling/event worker.
  Use `crowdb-cli bench rdma` and the shared RPC regression runner to report
  QP count, slot size, aggregation and errors.
- Use 64 KiB metadata, 256 KiB aggregated Chunk-KV, and 1 MiB datapath
  capacities, configured as transport defaults.
- Use posted receives as implicit data credit; do not add an extra credit
  protocol.
- Expose host buffer leases first. GPU delivery initially means an explicit
  host-to-GPU copy. RDMA READ/WRITE is outside this implementation.
- Use `setup-rdma.sh local` as the standard host setup. A single node runs
  client and server through the same HCA port and local IP; multiple containers
  on that node use the same local-loop model and open independent QPs.
- Treat `run-local-loop.sh local` as the smoke test for every local hardware
  setup. The current acceptance scope is one physical node with multiple
  containers sharing the configured HCA and opening independent QPs.
- Use host networking only for the multi-container profile. Assign each
  container a `CROWDB_PORT_OFFSET`; apply it uniformly to all default
  TCP/RPC and RDMA CM service ports, while keeping the HCA physical port
  unchanged.
- Physical multi-node testing is deferred to [the separate follow-up plan](plan-rdma-multi-node.md)
  and is not part of the current backlog scope.

## Remaining dependency order

```text
Accepted registered host buffers and common RPC
                  |
Accepted HTTP/ChunkIO/DiskIO and Chunk-KV/access
                  |
                  v
 R242 shared runtime/topology, S3 SDK and leases
                  |
                  v
     R220 Dataset adapter and measurements
```

The remaining child uses the existing [fat-client SDK plan](plan-rdma-fat-client-sdk.md)
when the user includes that scope. Do not create a duplicate plan or repeat the
completed service integration. The shared benchmark/functional `Runtime`
already launches native, no-device TCP OCI and shared-HCA OCI profiles.

Before changing the SDK frontend, run its focused baseline corpus, retain the
accepted transport/service settings and compare exact result bytes and errors.
Transport echo acceptance alone does not prove SDK lease semantics.

## Phase 4: R242 fat client SDK

- [ ] **Execute R242 child plan**: build the shared trusted internal runtime,
  expose storage topology and FFI-safe host leases, and deliver native S3
  using existing S3 components and Chunk-KV/ChunkIO transports. Native calls
  take explicit tenant/context without SigV4 or a user-secret database;
  Dataset attaches afterward under R220 using the same runtime.
- [ ] **Host-to-GPU simulation**: hold each lease until the explicit host-to-GPU
  copy completes, then release it; no GPU pointer is exposed by the base API.
- [ ] **SDK pressure tests**: hold all returned leases, verify bounded admission,
  cancellation, shutdown, and recovery after release.

Exit condition: native S3 and HTTP return equivalent semantic results, topology
is exposed for caller scheduling with existing freshness/fencing rules, and
caller-controlled lease lifetime is testable through FFI. R220's later Dataset
adapter and measurements do not gate R242 or this RDMA umbrella acceptance.

## Phase 5: SDK OCI E2E matrix

- [ ] **SDK TCP-only profile**: extend the shared launcher with the SDK corpus,
  omit RDMA devices and require zero RDMA attempts with exact frontend parity.
- [ ] **SDK shared-HCA profile**: run the SDK corpus in two non-root host-network
  containers, requiring independent QPs, retained caller leases, bounded
  pressure and setup-only TCP fallback.
- [ ] **SDK topology selection**: reuse existing runtime/readiness/report
  contracts for the SDK fixture when each profile's prerequisites are present.

Exit condition: SDK profiles report topology, actual path/classes/QPs, fallback,
held lease pressure, typed errors and exact frontend result equality. Existing
HTTP/DiskIO and Chunk-KV native/OCI corpora remain regression controls.

## Invariants to retain in SDK work

The accepted transport/service tests already cover these properties. Reapply
them to the new SDK frontend and its caller-controlled leases:

- [ ] **No mixed stream**: never append TCP bytes to a partially completed RDMA
  stream.
- [ ] **Bounded aggregation**: application message boundaries remain intact;
  transport coalescing preserves complete frames, order and per-RPC identity
  and stays within both peers' slot limits.
- [ ] **No premature reuse**: completion, disk completion, and caller release
  all precede buffer reuse for their respective ownership path.
- [ ] **Bounded memory**: registered bytes, posted receives, send depth, disk
  queue bytes, and SDK-held bytes remain within configuration.
- [ ] **Capability optionality**: a host or OCI container without RDMA remains
  a supported TCP deployment.
- [ ] **NIC ownership**: each single-node container uses the explicitly
  configured shared HCA; no test depends on moving a link or entering a network
  namespace.
- [ ] **Shared identity**: request ID, generation, stream sequence, and logical
  offset retain their meaning across TCP and RDMA.

## Final verification

Unit and integration gates:

```bash
pixi run -- cargo fmt --all -- --check
pixi run clean-env && pixi run test-cpp
pixi run -- cargo test -p crowdb-rpc-ffi --all-targets
pixi run -- cargo test -p crowdb-chunk-kv-client --all-targets
pixi run -- cargo test -p crowdb-access-server --all-targets
pixi run -- cargo test -p crowdb-diskio-client --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
pixi run rs-lint
pixi run tree-lint
```

OCI E2E verification must run the TCP-only and single-node HCA local-loop
multi-container profiles when their prerequisites are available. Physical
multi-node verification is outside this plan and is covered by the separate
follow-up plan. Namespace profiles are not part of acceptance.

## Test and setup files

- `tools/rdma/setup-rdma.sh`
- `tools/rdma/run-local-loop.sh`
- OCI image and multi-container runner configuration

## Handoff and blocking rules

Each child plan is marked `[x]` only after its own acceptance cases and gates
pass. If a child discovers a protocol ambiguity, stop that child, record the
question here, and resolve it before changing the formal design. If the host
cannot provide RDMA, continue portable SDK/TCP checks and mark only hardware
E2E as pending.
