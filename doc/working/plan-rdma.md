<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB RDMA Acceptance Record

Upstream: [RDMA RPC design](../design/rpc/design-crowdb-rpc-rdma.md), and the
[native SDK contract](../design/access-server/design-crowdb-native-sdk.md).

Scope: accepted host-memory RDMA SEND/RECV, bounded mixed-size aggregation,
registered buffer ownership, and single-node shared-HCA verification.

Persistent-plan exception: retain this root record for accepted local-HCA
measurements. Completed child plans and implementation checklists are removed; the evidence remains reproducible.

R5 is complete. The [native Dataset guide](../../lib/crowdb-sdk-native/README.md#dataset)
and [Dataset design](../design/access-server/dataset/design-crowdb-access-dataset.md)
define its supported contract. Dataset service and workload evidence remains
in the [Access acceptance record](../../app/crowdb-access-server/tests/README.md#dataset-httpnative-comparison).
[Physical multi-node verification](plan-rdma-multi-node.md) is an independent,
plan-only follow-up and is outside the completed R5 acceptance.

## Current implementation status

Status: implementation and local-HCA acceptance complete. Endpoint discovery
and opt-in defaults are settled by the existing implementation. Final policy
checks pass 31 native capability/configuration/preflight cases, including a
real-HCA TCP-only peer with strict and allowed fallback, plus nine Rust FFI
configuration cases. No production behavior changed during this closure.

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
acceptance, as recorded below. The resulting shared native runtime and S3
frontend pass their TCP and shared-HCA ownership profiles.

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
copy remains explicitly identified.

## Fixed design decisions

- Use one common RPC abstraction for TCP and RDMA.
- Use RC SEND/RECV. The accepted contiguous RPC layout uses one wire slot
  and one SGE per message. The accepted datapath adds a negotiated control/data layout with
  separate SGEs for immutable producer payload and per-target control; its
  total wire admission includes both regions.
- Service discovery resolves the RPC endpoint. Live TCP preflight supplies
  capability, generation, CM identity and data classes; CM binds that exact
  identity. Group 0 holds no second authoritative transient capability copy.
- RDMA is opt-in through explicit cold configuration or environment enablement.
  Hardware detection alone cannot activate it. Typed settings override the
  environment, explicit TCP remains usable, and strict RDMA cannot fall back.
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
- Use host networking only for the shared-HCA multi-container profile.
  Benchmark servers use `CROWDB_PORT_OFFSET`. The real-service SDK fixture
  assigns separate loopback endpoints and nonoverlapping RPC/CM ports, while
  keeping the HCA physical port unchanged.

## Accepted native runtime and S3 delivery

- `crowdb-sdk-native` embeds trusted internal S3 over existing metadata and
  ChunkIO/DiskIO clients. `ClientRuntime` shares transport, topology and
  lock-free request/producer/held-capacity admission. The portable Dataset
  HTTP SDK remains independently packaged without storage FFI dependencies.
- HTTP and native use the same S3 execution for namespace, immutable selection,
  conditional reads, copy and multipart publication. Native execution needs
  explicit tenant/context, with no Access listener or credential lookup.
- `OwnedPayload` preserves physical allocation ownership through RPC, DiskIO,
  ChunkIO, metrics and S3. Ordered leases expose request, sequence, offset,
  length, capacity and registration identity. Registered framed producers fill
  final storage slots before freezing; ordinary input and EC processing retain
  their required copies.
- Successful EOF includes final length/checksum verification. Interrupted
  accepted RDMA reads fail terminally through mirror/EC recovery; they cannot
  append a TCP suffix or change replicas to conceal an interrupted transfer.
  Publication uncertainty never authorizes automatic mutation replay.
- Shared views charge an allocation once until final release. Pressure blocks
  further delivery at the configured limit. Cancellation and owned shutdown
  close admission, drain writers and join RPC workers without invalidating
  caller-held leases. Borrowed groups retain their original shutdown owner.
- Topology views retain separate source generations/freshness. A moved owner
  test uses the real routed metadata client with an injected catalog/transport:
  one stale-owner response refreshes execution while the old view remains
  inspectable. Selected-object, expired-layout and refresh-failure regressions
  preserve the logical generation and authority boundary.
- Native and no-device OCI TCP acceptance passes before and after full storage
  restart in `container-e2e/run-0ae636d32f1c4b228918a4bf1099708c`. The native-only
  fixture has twelve real services; the separate thirteen-service parity
  fixture compares HTTP/native bytes, ETags, metadata, errors, copy and multipart.
- Final shared-HCA acceptance passes in
  `native-s3-hca/3522f5c97f7c46e880e09ff8ed158605`, using image
  `sha256:685c3632b0b27fdb8a684fda65914f509a34972bc2a9b1b8c6959ea5f92fe28b`.
  Native and non-root OCI consumers each write a corpus, then verify after all
  storage services restart. Sizes include 0, 7, 513, 65502, 65503, 1048589,
  2097152 and 4194335 bytes, plus framed producer and multipart cases.
- Each HCA reader reports twelve data connections, 38 receives, zero
  fallback/CQ/protocol/post errors, zero SDK compatibility-copy bytes,
  8 MiB held high water and 436404224 registered bytes within a 512 MiB budget.
  A registered pressure-test lease survives owned shutdown while a simulated
  asynchronous host-to-GPU copy holds it; release returns its final charge.
- Each producer records 1114112 producer bytes high water and 5374424 held
  bytes high water. Registered framed input directly sends 2099385 bytes;
  ordinary input and EC construction still stage 21247554 bytes. The small
  framed-input transport copy delta is zero. Open-strip repair retains small
  writer inputs until drain; producer and held charges then return to zero.
- This is shared-HCA local-loop functional evidence. Container io_uring can
  select its existing blocking fallback; these checks make no cross-host or
  storage-throughput claim.
- Final full S3 acceptance passes all 45 fixtures in
  `container-e2e/run-c3ded34f0ac84d70aae9f0ff913d63d1`, including native/HTTP
  parity, recovery and Java/JavaScript/Go SDK clients. All 163 fixture tests,
  affected Rust suites, workspace all-feature clippy, `rs-lint`, formatting and
  independent HTTP SDK boundary/twenty functional tests pass. The additional
  serial protocol recheck passes all 23 fixtures in
  `container-e2e/run-6dd0488a66be4fd5bd480fdf157f06cc`. The final C++ source
  previously passed tree-lint and the native transport gates above.

The [native SDK guide](../../lib/crowdb-sdk-native/README.md) and
[architecture](../design/access-server/design-crowdb-native-sdk.md) define the
supported API. The [Dataset facade](../../lib/crowdb-sdk-native/README.md#dataset)
attaches shared Dataset authority, Snapshot/Split binding and caller-owned
checkpoints to the same runtime, without persistent read sessions or remote
acknowledgements. Its service, workload and local-HCA evidence is retained in
the [Access acceptance record](../../app/crowdb-access-server/tests/README.md#dataset-httpnative-comparison).
GPUDirect remains a separate follow-up.

## Verification commands

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

Revalidation uses the TCP-only and single-node HCA local-loop multi-container
profiles with their declared prerequisites. These commands reproduce checks;
they are not outstanding R5 tasks. Namespace profiles are outside acceptance.

## Test and setup files

- `tools/rdma/setup-rdma.sh`
- `tools/rdma/run-local-loop.sh`
- OCI image and multi-container runner configuration
