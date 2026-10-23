<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RPC Flow Analysis

The standalone RPC echo benchmark measures crowdb-rpc transport:
epoll/kqueue, framing, request correlation and request/response completion.
Echo throughput excludes KV/storage work. Separate functional corpora below
verify actual service and HTTP/DiskIO behavior.

## 1. Flow

```text
crowdb-cli bench rpc / bench rdma
  -> co_spawn native coroutine loaders, one in-flight request each
  -> Rust callback builds flatbuffer control and pooled data buffers
  -> RpcClient::send(..., callback)
     claim a preallocated completion slab slot
     submit through the selected TCP or RDMA transport, suspend coroutine
  -> standalone server receives complete request frames
     validate control, copy request data into an echo response
     submit through the same selected transport
  -> client completion owner parses response and claims the slab slot DONE
     resume coroutine and validate response through the Rust callback
     record owner-sharded counters and nanosecond histogram
     release buffers and submit the next request
  -> co_spawn returns after all coroutines and active callbacks drain
```

The native coroutine mode avoids a tokio scheduler round trip. The tokio mode
uses `RpcClient::call`, a oneshot channel, and a task wake for each response. Both
models share `RpcClient`, `Connection`, `BufferPool`, and `RpcServer`.

The TCP hot path uses connection pointers in epoll/kqueue user data, per-worker
receive buffers, caller-thread `writev`, batched server responses, and a
slab completion pool with a map fallback. The timeout reaper handles lost
responses outside the hot path. Reference-count operations are O(1); frame
construction, the echo memcpy, flatbuffer serialization, and kernel socket
copies contribute to this workload's cost.

RDMA uses the same native coroutine loaders and echo handler. Submission enters
a bounded per-QP send queue; its assigned CQ owner packs complete RPC frames
into a registered slot and posts one SGE. Each CQ has one fixed polling/event
thread. Receive frame views retain their registered slot until the final
consumer releases it; socket workers handle CM rather than competing for CQ
polling. The echo workload still copies its response payload and does not
measure a storage or zero-copy application path.

## 2. Latest Benchmark Results

### Linux TCP/RDMA — 2026-10-23

The common runners are `bench-rdma-rpc-regression.sh` and
`bench-rpc-regression.sh`. They launch fb-server and `crowdb-cli bench rdma`
or `bench rpc` from the same manifest and retain canonical JSON/CSV/TSV,
complete logs, binary hashes, source identity, settings and raw peer status.
Missing reports, process failures, nonzero errors, fallback or path mismatches
fail the runner. RDMA remote request totals must equal accepted submissions.

Hardware: Ryzen 9 5950X, 16 physical/32 logical CPUs, approximately 62 GiB RAM,
`rocep35s0f0` port 1, 100 Gb/s link, PCIe 8 GT/s x4. Both peers share this
physical host/HCA. raw-server means two native host processes; OCI means two
host-network containers using independent QPs, UID 1000, Infiniband device mappings
and unlimited memlock. CPU affinity/frequency are not pinned. This topology
measures local HCA traversal rather than physical-node isolation.

Every trial warms up for 1 s, counts validated completions in a 10 s window,
then drains separately. Coroutine loaders have one request in flight each;
there is no producer OS thread per loader. Latency is native submit-to-response
callback RTT: payload construction and Rust response validation are outside
the latency sample but remain inside throughput work. Histograms use bounded
nanosecond buckets, exact count/sum/extrema, nearest-rank upper bounds and
explicit zero/overflow counts. Resource and transport counter deltas include
warmup and drain, as declared in each row.

All six peak rows below use clean source `944274af` and matching release
binaries. They select median TPS from three independent repeats, retaining
latency from that same trial. All 26 final trials have valid reports, zero
unexpected errors and zero fallback. TCP OCI has no RDMA device mappings.
These are closed-loop coroutine workloads, not fixed-rate open-loop SLO tests.
Peak rows use different tuned concurrency; they do not compare latency at
equal offered load.

| Path / topology | Loaders | Connections / QPs | CQ / pollers | Socket workers | Aggregation / Nagle | RPC TPS     | Avg us | P90 us  | P99 us  | Max ms | Errors | Fallback |
|-----------------|---------|-------------------|--------------|----------------|---------------------|-------------|--------|---------|---------|--------|--------|----------|
| RDMA raw-server | 2048    | 16                | 16           | 1              | aggregation on      | 3,026,938.6 | 675.55 | 823.29  | 1097.73 | 5.788  | 0      | 0        |
| RDMA OCI        | 2048    | 16                | 16           | 1              | aggregation on      | 3,236,760.8 | 631.64 | 704.51  | 892.93  | 3.559  | 0      | 0        |
| TCP raw-server  | 1000    | 32                | n/a          | 16             | Nagle off           | 1,203,469.3 | 829.48 | 1589.25 | 7143.42 | 22.432 | 0      | 0        |
| TCP raw-server  | 1000    | 32                | n/a          | 16             | Nagle on            | 2,167,732.3 | 459.98 | 851.97  | 1548.29 | 5.356  | 0      | 0        |
| TCP OCI         | 1000    | 32                | n/a          | 16             | Nagle off           | 1,094,684.9 | 912.03 | 1441.79 | 9109.50 | 38.539 | 0      | 0        |
| TCP OCI         | 1000    | 32                | n/a          | 16             | Nagle on            | 2,112,923.9 | 471.92 | 888.83  | 1671.17 | 6.659  | 0      | 0        |

The worst TCP maximum across the three repeats is 24.996 ms raw-server/Nagle
off, 7.170 ms raw-server/Nagle on, 38.539 ms OCI/Nagle off and 7.063 ms
OCI/Nagle on. P90 does not describe this tail. RDMA's worst repeated maximum
is 5.788 ms raw-server and 3.559 ms OCI. All timeout, payload mismatch,
duplicate completion, CQ, protocol and report error checks remain enabled.

Single-loader latency and high-load throughput are separate controls:

| RDMA raw-server loaders | QPs | CQ / pollers | Aggregation | RPC TPS     | Avg us | P90 us | P99 us | Max ms | Errors |
|-------------------------|-----|--------------|-------------|-------------|--------|--------|--------|--------|--------|
| 1                       | 1   | 1            | off         | 60,964.4    | 15.88  | 15.94  | 21.25  | 0.361  | 0      |
| 512                     | 8   | 8            | on          | 2,290,244.8 | 222.58 | 257.02 | 299.01 | 0.656  | 0      |

At 2048 outstanding RPCs, roughly 3M TPS implies average RTT around 0.67 ms;
this queueing cost must not be presented as the single-loader 16 us latency.
The contiguous layout posts fixed local receive slots before active CM
connect. Moving that step after CM had produced zero warmup completions and a
1.46-second cold response. The corrected single-loader trial completes 57,184
warmup RPCs and has a 0.361 ms maximum, with unchanged one-second warmup,
ten-second measurement and five-second request deadline. Control/data still
allocates its actual negotiated minimum after validating the CM reply.

RDMA queue settings for both peak topology rows:

| Code variable                    | Requested value             | Observed / scope                         |
|----------------------------------|-----------------------------|------------------------------------------|
| `io_engines`, `io_workers`       | 1, 1                        | per peer                                 |
| `cq_count`, `poller_count`       | 16, 16                      | one fixed thread per CQ per peer         |
| `sq_depth`, `rq_depth`           | 1024, 1024                  | per QP; provider values in canonical row |
| `posted_receive_target`          | 1024                        | per QP                                   |
| `cq_depth`                       | 2048                        | per CQ; provider returns 4095            |
| `wire_slot_capacity`             | 1024 bytes                  | all headers/control/data included        |
| `send_arena_bytes`               | 1 MiB                       | per QP                                   |
| `receive_arena_bytes`            | 1 MiB                       | per QP                                   |
| `send_queue_capacity`            | 4096                        | per QP                                   |
| `poll_batch`, `verbs_poll_batch` | 256, 32                     | per owner                                |
| `poll_empty_budget`              | 256                         | per owner                                |
| `aggregation_max_bytes`          | 1024                        | also bounded by peer receive slot        |
| `aggregation_max_frames`         | 256                         | complete frames only, no flush timer     |
| `registered_bytes`               | derived budget              | 34 MiB per peer at 16 QPs                |
| `rnr_nak_retry_err`              | cumulative provider counter | 1789 before/after; delta 0 in raw median |
| CPU migrations/quarantined bytes | unavailable aggregate       | null, never fabricated zero              |

`connection_count` counts RC QPs; `qpair_number` in connection status is a
provider QP identifier. CQ count and polling thread count match one-for-one.
TCP uses `io_engines = 1`, `io_workers = 16`, 32 connections and
`send_queue_capacity = 4096`; RDMA is explicitly disabled there. CQ/SQ/RQ and
registered slots are not TCP settings.

CPU user/system time, context switches, affinity, SEND counts/rate, frames per
SEND, actual capacities and every error category remain in the canonical wide
CSV/TSV alongside raw before/after snapshots. Counter deltas include warmup
and drain. Native peers load the pixi runtime libraries; OCI preloads its
system verbs libraries and configured provider. CPU affinity/frequency are not
pinned. OCI RDMA is 6.93% higher than raw-server; OCI TCP is 9.04% lower with
Nagle off and 2.53% lower with Nagle on. These observations do not isolate a
container effect from library/provider and scheduling differences. The shared
workload, settings, deadlines and report validation are identical.

Canonical evidence is retained in both the repository runtime directory
`.crowdb-runtime/rdma-check/artifacts/bench/` and
`/tmp/crowdb-rdma-probe/artifacts/`. Profile directory names are:

- `accepted-944274af-rdma-native-final` and `accepted-944274af-rdma-oci-final`;
- `accepted-944274af-tcp-native-off-final` and `accepted-944274af-tcp-native-on-final`;
- `accepted-944274af-tcp-oci-off-final` and `accepted-944274af-tcp-oci-on-final`.

RDMA raw-server repeat TPS: 3,036,947.8 / 3,026,938.6 / 3,026,730.4.

RDMA OCI repeat TPS: 3,236,760.8 / 3,228,268.4 / 3,251,586.3.

TCP raw-server Nagle off repeat TPS: 1,207,836.4 / 1,203,469.3 / 1,202,207.6.

TCP raw-server Nagle on repeat TPS: 2,177,406.0 / 2,165,395.3 / 2,167,732.3.

TCP OCI Nagle off repeat TPS: 1,092,294.1 / 1,100,987.3 / 1,094,684.9.

TCP OCI Nagle on repeat TPS: 2,116,257.1 / 2,112,923.9 / 2,100,983.2.

Server SHA256 is
`227fe3de6d7b337855d99f7e76ac48a4d348e0a13c700b347e654f9c64c5c55b`;
CLI SHA256 is
`ee349771016cf06012406806040266947e83b26ec50d3f12eea5f993306a26b0`.
The pinned OCI image is
`sha256:e3c9c5c6d5fc3424ac3f22cf4cdcc9b1c872f47336484f54962b2541c4ed6cef`.
Both wrapper scripts embed the environment, parameters, repeats and results.

The earlier transport-tuning source `55ffee38` measured 2.903M TPS native and
2.954M OCI; matching TCP Nagle off measured 1.196M TPS. The pre-fix v4 source
`018da2fc` measured 2.901M/3.073M RDMA native/OCI with 1.5–1.8-second cold
outliers. Those controls exposed the contiguous receive-priming regression;
low P90 alone had hidden it. Earlier canonical directories were removed during
this session; their IDs below are historical evidence identifiers rather than
currently available downloads. Surviving acceptance/gate logs are retained
under `/tmp/r241-*.log`; current perf and gate artifacts are also backed up in
the repository runtime directory.

The staged sweep retained 24 rows and 203,195,362 measured completions at
`rdma-rpc-20261022T160711.769136Z`, then nine higher-load rows at
`rdma-rpc-20261022T161241.180591Z`. With one QP/CQ, increasing coroutine
loaders from 1 to 512 raises throughput from about 60k to 566k TPS.
Aggregation raises the 512-loader case to about 701k. With eight QPs, CQ
counts 2/4/8 produce about 880k/1.48M/2.21M TPS. At higher load, 16 CQs
reach approximately 2.9M; 32 CQs per peer fall to 2.02M and P99 4.00 ms
because both peers together have 64 polling threads on 32 logical CPUs.
RQ 4096 and larger 64/256 KiB slots do not improve matching controls.
Polling-budget and slot controls were recorded as
`rdma-rpc-20261022T162311.140989Z`; this is the best repeatable tested
configuration, not proof of the HCA's hardware maximum.

Payload controls at `rdma-rpc-20261022T163008.055655Z` cover 512 bytes through
1,048,490 bytes with matching 1 KiB/64 KiB/256 KiB/1 MiB wire slots,
64 loaders, two QPs/CQs and SQ/RQ/post target 64. Large-payload echo throughput
plateaus around 1.5 GB/s of validated payload. The full-payload validation,
echo copies and PCIe x4 link all warrant profiling; the 100 Gb/s link rate
alone does not predict this round-trip workload's TPS. The largest payload
has average/P90 latency about 44 ms, so small-message latency must not be
presented as the latency of every size/load case. The no-device TCP OCI
corpus at `tcp-rpc-20261022T163555.799432Z` also passes with zero RDMA attempts.

Historical audit: the 1,186,876 TPS value once labeled RDMA used TCP socket
reads/writev, and its TSV contained error rows despite successful script exit.
The current matching Nagle-off TCP median is 1,203,469 TPS (+1.40%). It is
not an RDMA regression. The previous fixed-request echo did not test large
encoded Chunk-KV controls, mixed fragment sizes, asynchronous handler lifetime
or interrupted logical results. Current runners require strict path identity,
zero errors and fallback, matching remote submissions, complete reports and
successful process exits. Echo throughput and the service corpora below prove
different properties and share the same native/two-container process launcher.

Final transport verification passes 543 tree, 192 RPC, 71 DiskIO and 35 common
native cases plus both FFI suites at
`.crowdb-runtime/artifacts/unit/cpp-b493f2503a414232a8c41bc2a13824d6/`.
All 160 no-verbs CTest cases pass (one hardware case skipped), and all 14
selected real-HCA cases pass, including first-response latency, asymmetric
classes, mixed-size staging, held results, bounded admission, expiry and
peer-close behavior. Changed C++ format/tree-lint, Rust format and thirteen
runner contract cases pass. Current gate logs and HCA XML are backed up at
`.crowdb-runtime/rdma-check/artifacts/verification-944274af/`.
The unchanged Rust service integration passed 1,244 affected cases (four
optional cases ignored), workspace all-feature clippy and `rs-lint` at clean
source `018da2fc`; the final native setup correction preserves those service
control/data semantics. Earlier four CLI timed/coroutine contract cases pass.

Two existing intermittent failures were retained rather than repaired here:
`DoubleBuffer.ConcurrentReadersDuringFrequentFreezeAndDrainNoCorruption`
failed `bad.load() == false`, then its thirteen-case suite and full tree gate
passed without source changes. `SqFullBackpressureTest.UringEngineTinySqManyWrites`
returned five `-ENOMEM` callbacks when the existing 1000 yield attempts could
not obtain an SQ slot; the exact case, fourteen-case suite and full gate then
passed unchanged. Initial gate IDs are `cpp-f0f7acd09fb541f59d048f81e3923cc2`
and `cpp-bdbe5707db274b8aa625247ce0eb00eb`. Complete surviving failure logs
include `/tmp/r241-class-cpp.log`; this work does not claim either fix.

The sections below retain earlier 20-second TCP references. `Nagle` means TCP
coalescing is enabled; old histogram precision and measurement scopes differ.

### HTTP/ChunkIO and DiskIO functional acceptance — 2026-10-23

`tools/rdma/run-datapath.py` runs a real Hyper HTTP socket, the production S3
body transfer, ChunkIO framing/mirror writes and verified reads, and a separate
DiskIO executable over O_DIRECT files. Only chunk placement/catalog state is
injected; this case does not exercise namespace/authentication or Group-0.
The corpus is 3,158,073 bytes, with an unaligned tail and two chunk locations.
All successful TCP/RDMA profiles match SHA256, ETag and physical/logical
location geometry. TPS and latency percentiles are not measured by this
functional fixture; the RPC throughput runner above remains the common load
measurement.

| Topology                | Path | Disk engine    | Downloaded bytes | Upload direct bytes | Download direct bytes | Transport copy bytes | TCP fallback | Expected errors | Verification errors |
|-------------------------|------|----------------|------------------|---------------------|-----------------------|----------------------|--------------|-----------------|---------------------|
| Two native processes    | RDMA | io_uring       | 3,158,073        | 6,319,478           | 3,159,739             | 0                    | 0            | 0               | 0                   |
| Two native processes    | TCP  | io_uring       | 3,158,073        | n/a                 | n/a                   | n/a                  | n/a          | 0               | 0                   |
| Two non-root containers | RDMA | BlockingEngine | 3,158,073        | 6,319,478           | 3,159,739             | 0                    | 0            | 0               | 0                   |
| Two non-root containers | TCP  | BlockingEngine | 3,158,073        | n/a                 | n/a                   | n/a                  | n/a          | 0               | 0                   |
| Containers / peer close | RDMA | BlockingEngine | 3,144,096 prefix | 6,319,478           | 3,145,728             | 0                    | 0            | 1               | 0                   |

The failure case gates HTTP delivery after one verified frame, closes DiskIO,
then resumes delivery. The body terminates with an error and never publishes a
successful complete object. Expected attempts to unavailable read replicas are
visible in the raw preflight counters. Accepted RPC retry counts remain zero.
The upload direct-byte count includes two replicas and physical frame overhead.
HTTP's existing prefetched prefix copy is reported separately (8,098 bytes in
the native RDMA case); transport payload copies remain zero.

Shared RDMA settings: two QPs per peer, one CQ and polling/event thread,
`sq_depth = rq_depth = posted_receive_target = 4`, requested `cq_depth = 16`
(provider 31), `wire_slot_capacity = 1049600`, `control_slot_capacity = 1024`,
`data_slot_capacity = 1048576`, `send_arena_bytes = 4194304` and
`receive_arena_bytes = 4198400`. Transport aggregation and TCP fallback are
disabled. One socket worker serves each peer. HTTP native admission peaks at
2 MiB. Native disk I/O uses `sq_entries = 32`; default OCI seccomp rejects
io_uring, so the service selects its asynchronous BlockingEngine. This disk
engine selection is independent of the RDMA/TCP path.

Both containers use UID 1000, host networking and the same host/HCA, with
independent QPs. The pinned runtime image is
`sha256:e3c9c5c6d5fc3424ac3f22cf4cdcc9b1c872f47336484f54962b2541c4ed6cef`.
All five profiles also pass the clean `018da2fc` common-control regression.
Manifests retain binary hashes,
source/dirty state, actual server identity,
engine, settings, complete logs and raw counters. Final regression artifact
IDs below are historical after runtime cleanup; complete role/runner logs
remain under `/tmp/r241-datapath-*.log`:

- `4228dda544c44d5c84f7f8ad4e215521`: native RDMA;
- `2d6bfc8b120b49648a7cd467cdfb8ae4`: native TCP;
- `6307140655914a9f99ffc95e7cf1a1a4`: shared-HCA OCI RDMA;
- `da8e41b8e25340058c67f057894b1732`: OCI TCP without RDMA devices;
- `e15280fc8ca649c5a9ccf77cf05f0ac3`: OCI partial-download peer close.

### Chunk-KV / Access functional acceptance — 2026-10-23

`tools/rdma/run-chunk-kv.py` uses the same native/two-container `Runtime` as
the throughput runners. Each profile runs production `ChunkKvRpcService`,
`CrowdbPartitionTree` and partition WAL logic in a separate server process.
Catalog, serving authority and memory ChunkStream backing are injected; this
corpus does not start a complete Group-0/namespace/authentication deployment.
Access configuration and construction are covered by the affected Rust suites.

Eight values total 3,409,975 bytes: 63 KiB, 65,553 B, 255 KiB, 262,175 B,
768 KiB, 1,048,583 B, 300 KiB and 600 KiB. The corpus verifies exact point
put/get, conditional mutation, delete, seek, forward/reverse/paged scan,
600 KiB bounded scan, multi-get duplicate order and a two-mutation 900 KiB
batch. Stable mutation IDs 1–8 replay without duplicate effects. Expired
requests return `RequestExpired`; stale scan continuations return
`RefreshRequired`. A point route hint keeps the existing current-catalog
behavior. TCP/RDMA results and typed errors match.

| Topology   | Profile         | Typed errors expected | Transport errors expected | Setup fallback | Unexpected errors | Verification errors |
|------------|-----------------|-----------------------|---------------------------|----------------|-------------------|---------------------|
| raw-server | strict RDMA     | 2                     | 0                         | 0              | 0                 | 0                   |
| raw-server | explicit TCP    | 2                     | 0                         | 0              | 0                 | 0                   |
| OCI        | strict RDMA     | 2                     | 0                         | 0              | 0                 | 0                   |
| OCI        | no-device TCP   | 2                     | 0                         | 0              | 0                 | 0                   |
| OCI        | setup fallback  | 2                     | 0                         | 2              | 0                 | 0                   |
| raw-server | RDMA peer close | 2                     | 1                         | 0              | 0                 | 0                   |
| OCI        | RDMA peer close | 2                     | 1                         | 0              | 0                 | 0                   |

Value SHA256:
`4a9ba75a81f4b56d08eb8a9de0ddc1feee810c932c26f40bf35a2765b324e3af`.
Mutation-result SHA256:
`4b9633bb71ba81353fb3fa5f88be2dd369b9b8a1e2a3a34278cdcdd94be7486b`.
The no-device TCP profile makes no RDMA attempt. Setup fallback occurs once
per metadata/aggregate group before any RDMA application payload, preserving
the corpus, IDs and digests.

Both peer-close profiles first finish the complete corpus, then pause the
server after one actual response fragment. The client observes exactly one
fragment before server termination, publishes zero incomplete logical results
and returns connection-closed within the existing five-second deadline.
There is no TCP replay/suffix. The fragment gate exists only in `test-util`.

Client metadata and aggregate groups have separate fixed transports and QPs
but share one `RpcClient`, reaper and global request ID sequence. Metadata
uses 64 KiB data slots; aggregate operations use 256 KiB. The server uses a
256 KiB local class and negotiates actual 64 KiB/256 KiB receives per QP.
`control_slot_capacity = 1024`, `sq_depth = rq_depth = posted_receive_target = 2`,
requested `cq_depth = 16` (provider 31). Each client group has one CQ/poller;
the server has one CQ/poller for both QPs. Control/data transport aggregation
is disabled. One socket worker serves each peer.

| Owner            | QPs | CQ / pollers | Actual data capacity | Registered peak B | Logical send peak B | Logical receive peak B |
|------------------|-----|--------------|----------------------|-------------------|---------------------|------------------------|
| Client metadata  | 1   | 1            | 64 KiB               | 526,336           | 1,048,736           | 1,048,688              |
| Client aggregate | 1   | 1            | 256 KiB              | 2,099,200         | 3,282,896           | 5,536,656              |
| Server           | 2   | 1            | 64 / 256 KiB         | 2,756,608         | 5,536,656           | 3,282,896              |

Metadata wire/send/receive arena bytes are 66,560 / 131,072 / 133,120;
aggregate values are 263,168 / 524,288 / 526,336. Logical controls have a
32 MiB maximum and independent 64 MiB send/receive admission budgets. Results
larger than the whole two-slot RQ window complete while those slots are
reposted as fragments are consumed. Completed logical controls retain their
byte admission through final native/FFI caller release and remain readable
after transport destruction.

Metadata client sends/receives 84/59 control fragments and copies
4,978,576 / 3,540,120 encoded control bytes. Aggregate client sends/receives
26/69 and copies 6,565,792 / 16,422,136 bytes. The server sends/receives
128/110 and copies 19,962,256 / 11,544,368 bytes. Encoded control assembly
copies each fragment once; these counters are distinct from direct data
payload copies in the HTTP/DiskIO corpus. CQ/protocol/post/SEND errors and
unexpected fallback remain zero in strict normal profiles.

The first mixed-size real service corpus exposed cached smaller staging
allocations occupying every SQ descriptor. A later larger fragment then
could not progress despite available arena bytes. Control staging now keeps
descriptors at the configured data class while SEND uses actual length.
The dedicated mixed-size HCA case failed before the fix and passed afterward;
admission limits and timeouts were unchanged. This traffic is absent from
the small contiguous echo, explaining why that older benchmark could pass.

All seven profiles pass clean source `018da2fc`, with the same pinned OCI
image, UID 1000 and host/HCA topology as the throughput runs. Historical
artifact IDs are `1b0ca70d7c624f91a043da5a4eb28355` (native RDMA),
`e05741caef6545489d1cf68346c09ab2` (native TCP),
`6a005f6dafde4ad6b384ba42ce457bc6` (OCI RDMA),
`168119889e2b4d308ceedb8d31aa3368` (OCI TCP),
`c2f6e04af419473786c5bc2de4964632` (OCI fallback),
`bacc9abc534a4e79bbcc4f56d9800c46` (native peer close) and
`74401eb8000c42fb91721ade5697e81a` (OCI peer close). Complete surviving logs
are `/tmp/r241-accepted-*.log`; the measured corpus is functional acceptance,
not a Chunk-KV TPS claim.
Surviving service, gate and mixed-size failure logs are also backed up under
`service-018da2fc-logs/` in both current artifact roots.


### Linux — 2026-08-27

AMD Ryzen 9 5950X, 16c/32t, x86_64, Linux 6.8. Single engine; coroutine
results use direct callbacks. The non-Nagle run is the reference mode.

| Workers | Load | Mode | Nagle | ops/s | avg us | p50 us | p99 us | p999 us | Errors |
| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1T:1C | coroutine | off | 52,759 | 17 | 17 | 25 | 31 | 0 |
| 4 | 64T:4C | coroutine | off | 514,844 | 122 | 108 | 186 | 326 | 0 |
| 8 | 512T:8C | coroutine | off | 820,424 | 621 | 612 | 842 | 1,181 | 0 |
| 16 | 1,000T:32C | coroutine | off | 1,246,622 | 798 | 401 | 6,148 | 10,416 | 0 |
| 4 | 64T:4C | coroutine | on | 983,245 | 63 | 59 | 162 | 520 | 0 |
| 8 | 512T:8C | coroutine | on | 1,744,728 | 291 | 261 | 540 | 5,248 | 0 |
| 16 | 1,000T:32C | coroutine | on | 2,023,369 | 490 | 418 | 1,550 | 2,282 | 0 |
| 1 | 1T:1C | tokio | off | 23,564 | 41 | 42 | 69 | 102 | 0 |
| 4 | 64T:4C | tokio | off | 557,362 | 113 | 105 | 265 | 425 | 6 |
| 16 | 1,000T:32C | tokio | off | 849,575 | 1,156 | 1,063 | 2,652 | 3,838 | 591 |

Nagle improves coroutine throughput by 91%, 113%, and 62% at 64T, 512T,
and 1,000T. It also reduces p99 by 13%, 36%, and 75%. Tokio is slower and
produces queue-full errors under bursty high concurrency.

### Linux — 2026-08-31

AMD Ryzen 9 5950X, 16c/32t, x86_64, Linux 6.8. Same hw as 2026-08-27.
Fix: per-call control flatbuffer — `request_id` now embedded in
`ConnectionPingRequest.id` per call (was fixed `id=0`), enabling correct
slab slot correlation. 6 of 10 configs strictly better than 2026-08-27;
4 retained (1L/1C coroutine ops/s slightly lower, 64T nagle-on p999
worse, tokio 64T ops/s lower, tokio 1000T errors worse).

| Workers | Load | Mode | Nagle | ops/s | avg us | p50 us | p99 us | p999 us | Errors |
| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1T:1C | coroutine | off | 52,134 | 18 | 17 | 17 | 31 | 0 |
| 4 | 64T:4C | coroutine | off | 517,687 | 122 | 94 | 96 | 260 | 0 |
| 8 | 512T:8C | coroutine | off | 830,174 | 614 | 486 | 505 | 1,074 | 0 |
| 16 | 1,000T:32C | coroutine | off | 1,307,488 | 762 | 302 | 324 | 9,645 | 0 |
| 4 | 64T:4C | coroutine | on | 994,296 | 63 | 44 | 46 | 578 | 0 |
| 8 | 512T:8C | coroutine | on | 1,869,083 | 271 | 209 | 219 | 3,993 | 0 |
| 16 | 1,000T:32C | coroutine | on | 2,213,182 | 448 | 157 | 171 | 1,999 | 0 |
| 1 | 1T:1C | tokio | off | 29,367 | 32 | 24 | 25 | 71 | 0 |
| 4 | 64T:4C | tokio | off | 487,362 | 127 | 63 | 71 | 519 | 4 |
| 16 | 1,000T:32C | tokio | off | 995,573 | 993 | 327 | 408 | 5,540 | 895 |

The per-call control fix dramatically improves p99 at high concurrency:
coroutine 1,000T p99 drops from 6,148us to 324us (−95%) without nagle,
and from 1,550us to 171us (−89%) with nagle. The 1,000T nagle-on peak
rises to 2.21M ops/s (+9.4%). Tokio 1T:1C improves 24.6% (29,367 vs
23,564) — the fixed correlation eliminates resp_missed overhead.

### Linux — 2026-09-02

AMD Ryzen 9 5950X, 16c/32t, x86_64, Linux 6.8. Same hw as 2026-08-31.
The crowdb-tree engine changes (page-count metrics + flush re-check
loop) do not touch the RPC transport — this run confirms no
transport-level regression. Results are within noise (−1% to −5% vs
2026-08-31). p50/p99 use coarser histogram buckets (100/500/1000/5000us)
than the 2026-08-31 exact values. Not strictly better — reference
retained per regression policy.

| Workers | Load | Mode | Nagle | ops/s | avg us | p50 us | p99 us | Errors |
| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 1T:1C | coroutine | off | 50,663 | 18 | 100 | 100 | 0 |
| 4 | 64T:4C | coroutine | off | 514,318 | 123 | 500 | 500 | 0 |
| 8 | 512T:8C | coroutine | off | 814,807 | 626 | 1,000 | 1,000 | 0 |
| 16 | 1,000T:32C | coroutine | off | 1,271,548 | 783 | 500 | 10,000 | 0 |
| 4 | 64T:4C | coroutine | on | 973,317 | 64 | 100 | 500 | 0 |
| 8 | 512T:8C | coroutine | on | 1,775,602 | 286 | 500 | 500 | 0 |
| 16 | 1,000T:32C | coroutine | on | 2,130,032 | 466 | 500 | 5,000 | 0 |
| 1 | 1T:1C | tokio | off | 28,062 | 34 | 100 | 100 | 0 |
| 4 | 64T:4C | tokio | off | 492,924 | 126 | 500 | 500 | 0 |
| 16 | 1,000T:32C | tokio | off | 988,956 | 999 | 1,000 | 5,000 | 0 |

### Linux — 2026-09-02 (group 1, mem-block WAL wipe fix)

AMD Ryzen 9 5950X, 16c/32t, x86_64, Linux 6.8. Same hw as prior runs.
RPC echo does not touch the KV/storage layer, so the mem-block WAL wipe
fix and group-1 bench change have no transport-level impact. Results are
within noise of the prior 2026-09-02 run (all within ±2.6%). Reference
retained per regression policy.

| Workers | Load | Mode | Nagle | ops/s | avg us | p50 us | p99 us | Errors |
| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 1T:1C | coroutine | off | 50,168 | 18 | 100 | 100 | 0 |
| 4 | 64T:4C | coroutine | off | 505,195 | 125 | 500 | 500 | 0 |
| 8 | 512T:8C | coroutine | off | 817,808 | 624 | 1,000 | 1,000 | 0 |
| 16 | 1,000T:32C | coroutine | off | 1,275,553 | 781 | 500 | 10,000 | 0 |
| 4 | 64T:4C | coroutine | on | 968,977 | 64 | 100 | 500 | 0 |
| 8 | 512T:8C | coroutine | on | 1,775,375 | 286 | 500 | 500 | 0 |
| 16 | 1,000T:32C | coroutine | on | 2,116,393 | 468 | 500 | 5,000 | 0 |
| 1 | 1T:1C | tokio | off | 28,790 | 33 | 100 | 100 | 0 |
| 4 | 64T:4C | tokio | off | 490,288 | 126 | 500 | 500 | 0 |
| 16 | 1,000T:32C | tokio | off | 991,402 | 996 | 1,000 | 5,000 | 0 |

### macOS — 2026-08-21

Apple M5 Pro, 18c, arm64, macOS 26/Darwin 25.5. kqueue, 128B values, and
Nagle disabled.

| Engines | Workers | Load | ops/s | avg us | p50 us | p99 us | p999 us | Errors |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 1T:1C | 49,445 | 19 | 19 | 29 | 49 | 0 |
| 1 | 4 | 64T:4C | 558,326 | 112 | 106 | 231 | 299 | 0 |
| 1 | 8 | 512T:8C | 900,017 | 564 | 503 | 1,446 | 3,938 | 0 |
| 2 | 8 | 512T:8C | 927,537 | 547 | 521 | 951 | 3,630 | 0 |
| 1 | 16 | 1,000T:32C | 722,644 | 1,372 | 1,009 | 5,484 | 14,384 | 0 |
| 2 | 16 | 1,000T:16C | 900,252 | 1,099 | 851 | 4,012 | 9,056 | 0 |

The macOS peak is 927,537 ops/s at two engines and eight workers. Linux is
faster at comparable high-concurrency loads, reaching 2.02M ops/s with Nagle
and 1.25M without it. The platforms use different kernels and socket
transports, so this is directional rather than a controlled A/B result.

## 3. Change History

### Shared connections and caller-thread writes

Removed the notify queue from submit and moved `writev` to the submitting
I/O worker thread. The `in_send_` CAS serializes concurrent senders; EAGAIN
arms write on the owning engine.

Perf: macOS throughput rose from ~132K to ~317K ops/s.

### Direct callback completion

Added the callback client model (`send()`) with a preallocated slab
completion pool, removing the oneshot channel allocation and scheduler wake
from the benchmark path.

Perf: peak throughput reached ~585K ops/s (2.1x the previous baseline).

### Slab fallback and reaper

Added CAS-based slot ownership (`FREE/DONE→PENDING` claim), map fallback for
occupied slots, timeout reclamation, and race fixes (init mutex, two-phase
PENDING, read-before-CAS in `on_response`).

Perf: peak Linux throughput reached ~2.29M ops/s (8.3x baseline) with zero
errors. Fixed silent callback loss under high worker contention.

### Standalone server and worker scaling

The same transport was measured in separate client/server processes instead
of in-process echo. Multi-engine and multi-worker configurations were swept.

Perf: Linux reached 2.40M ops/s (2e16w 1000t16c); macOS reached 928K ops/s
(2e8w 512t8c). Linux is 2.6x macOS under the same codebase and config.

### Tokio comparison

The async `call()` path (oneshot channel + scheduler wake per op) was
compared against the coroutine path (direct callback on I/O thread).

Perf: tokio runs at 45–69% of coroutine throughput and 1.5–2.2x the latency.
Per-call heap alloc (2 allocs/op) and scheduler round-trip account for most
of the gap; high-load bursts fill the send queue (30–879 errors at 64+
loaders).

| Config | Coroutine ops/s | Tokio ops/s | Tokio % of coroutine |
| --- | ---: | ---: | ---: |
| 1e1w 1T:1C | 52,759 | 23,564 | 45% |
| 1e4w 64T:4C | 514,844 | 557,362 | 108% |
| 1e16w 1000T:32C | 1,246,622 | 849,575 | 68% |

### Nagle comparison (2026-08-27)

Enabling TCP coalescing (`--enable-nagle`) allows the kernel to batch
multiple small 128B frames into a single TCP segment, reducing syscall
overhead for bursty coroutine workloads.

Perf:

| Load | Nagle off ops/s | Nagle on ops/s | Speedup | p99 off us | p99 on us |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64T:4C | 514,844 | 983,245 | +91% | 186 | 162 |
| 512T:8C | 820,424 | 1,744,728 | +113% | 842 | 540 |
| 1000T:32C | 1,246,622 | 2,023,369 | +62% | 6,148 | 1,550 |

The result shows syscall pressure in the small-frame, bursty workload;
application-level send batching is the longer-term alternative.

### Benchmark update (2026-08-27)

Replaced the Linux reference with the current single-engine sweep and
retained the macOS standalone baseline. Metrics shutdown now wakes its
condition variable, reducing shutdown delay from ~5s to 60ms; it does not
affect request throughput.

### Per-call control flatbuffer (2026-08-31)

The `bench rpc` echo client was building a fixed `ConnectionPingRequest`
control flatbuffer with `id=0` and reusing it for every request. The
echo handler extracts `request_id` from the control's `id` field during
parse and echoes it back in the response. With `id=0`, every response
arrived with `request_id=0`, which never matched the slab slot's
per-call `request_id` — causing `resp_missed` and indefinite hangs.

Fix: `build_control(request_id)` builds a fresh `ConnectionPingRequest`
with `id=request_id` per call. Both tokio and coroutine modes now
correlate responses correctly.

Perf: 6 of 10 configs strictly better than 2026-08-27. The most dramatic
improvement is p99 at 1,000T: 6,148us→324us (−95%) without nagle,
1,550us→171us (−89%) with nagle. Peak nagle-on throughput rises to
2.21M ops/s (+9.4%). Tokio 1T:1C improves 24.6% (29,367 vs 23,564).

### B-tree page-count metrics + flush re-check loop (2026-09-02)

The crowdb-tree engine changes (page-count gauges + flush re-check loop)
do not touch the RPC transport layer. This run confirms no
transport-level regression: results are within noise (−1% to −5% vs
2026-08-31). Reference retained per regression policy. See the
"Linux — 2026-09-02" subsection above for the full table.

## Mixed-size posting and CPU comparison

The mixed-size continuation uses registered producer allocation, optional bounded
small-frame packing, linked WR posting, selective tail completion and per-QP WR
and wire-byte windows. Its formal ownership and scheduling contract is in
[RDMA RPC](design-crowdb-rpc-rdma.md#5-shared-cq-and-completion-model), and the persistent
[RDMA execution record](../../working/plan-rdma.md#accepted-mixed-size-send-optimization)
retains binary identities, failed experiments, final per-size latency gates and
native/OCI service results. The checked profile is
`tools/benchmark/profiles/rdma-mixed-size.json`.

A producer thread, closed-loop loader, outstanding RPC, posted SEND WR, packed
frame, CQE and CQ owner are distinct quantities. Completed validated client
requests/s remains the primary TPS metric, including useful small-message
aggregation. Both payload initialization and the unchanged application echo copy
are included in the paired CPU measurements. Large registered messages reuse
source owners; small eligible messages may copy into one aggregate. No future
arrival is required to flush a singleton or partial WR list.

The 2026-10-23 repeated two-core mixed comparison improves median one-owner
throughput from 43,437.6 to 48,842.7 requests/s and total CPU cost from 26,743 to
19,224 ns/callback. Four QPs sharing an owner improve CPU cost from 30,944 to
23,153 ns/callback with similar throughput (50,180.5 to 51,657.7). Every size's
P99 passes the previously declared baseline plus 10%, with zero errors. This
synthetic mix and shared-HCA local-loop establish a bounded mixed-size benefit;
physical-network and intended-arrival latency need their own measurements.

Primary comparison references inspected on 2026-10-23:

- [bRPC RDMA guide](https://github.com/apache/brpc/blob/master/docs/cn/rdma.md)
  and [endpoint implementation](https://github.com/apache/brpc/blob/master/src/brpc/rdma/rdma_endpoint.cpp)
  describe registered IOBuf storage, RC, peer RQ windows, selective signaling
  and event/polling modes. The inspected send loop posts individual WRs;
  dispatch aggregation does not establish linked-WR submission or CQE removal.
- [bRPC RDMA dispatch](https://github.com/apache/brpc/blob/master/src/brpc/rdma_transport.cpp)
  defaults to bthread application dispatch in polling mode. Its
  [performance client](https://github.com/apache/brpc/blob/master/example/rdma_performance/client.cpp)
  replenishes outstanding requests through callbacks; `thread_num` starts
  bthreads and `queue_depth` is application concurrency. Attachments are reused
  and echo is optional. Poller count alone does not establish process CPU cost.
  The official guide supplies no directly comparable single-core RDMA QPS claim.
- [eRPC paper, sections 4 and 6](https://www.usenix.org/system/files/nsdi19-kalia.pdf)
  uses registered application buffers and preallocated short responses. Its
  10M symmetric experiment combines outgoing and incoming RPC processing;
  it is not a mixed-size storage benchmark or an identical client TPS measure.
- [ibv_post_send](https://man7.org/linux/man-pages/man3/ibv_post_send.3.html)
  and [InfiniBand section 10.8.6](https://www.afs.enea.it/asantoro/V1r1_2_1.Release_12062007.pdf)
  define partial posting and successful later completion of earlier unsignaled
  sends on the same SQ. Error/flush teardown retains separate ownership rules.
