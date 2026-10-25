<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Access server acceptance

Protocol authority: [native Iceberg design](../../../doc/design/access-server/iceberge/design-crowdb-iceberg.md).
Client recipes: [container development](../../../container/single-node-container/README.md).

- REST/official-SDK conformance, native durability and ecosystem compatibility
  are distinct evidence. Passing SDK tests cannot certify compute engines.
- The [manual ecosystem fixture](../../../e2e-test/clients/ecosystem/README.md)
  uses a separate locked environment and workflow dispatch. Its matrix records
  only actual pinned client operations against the container and a volume restart.
- [S3 language SDKs](../../../e2e-test/clients/S3-SDK.md) and the Rust Iceberg SDK retain
  independent manual workflows. They are not default test dependencies.
- Selected ORC and optional caches remain independent backlog ownership under
  [ORC validation](../../../doc/backlog/R186-access-iceberg-orc-validation.md)
  and [cache optimization](../../../doc/backlog/R185-access-iceberg-cache-invalidation.md).
  The functional fixtures select Parquet and establish no cache speed guarantee.
- Physical GC remains an independent opt-in runtime contract. Logical drop,
  clear, expiry and CAS loss do not authorize physical deletion by TTL alone.
  Provisioned allocation capacity bounds storage; cumulative orphan storage
  is not bounded by active request limits.

## Evidence retention after shared-directory removal

During the subsequent Dataset run, the repository's `.crowdb-runtime` and
`target` directories disappeared. The deletion source is unconfirmed. Artifact
IDs below identify their completed executions; the original runtime directories
are no longer present. Source commits and Git bundle backups remain intact.

The new image's 12-case acceptance stdout, component/lint logs, recovered GC
observations and all 18 Dataset trial JSON records are retained in
`/tmp/crowdb-access-continuation-evidence-20261025`. The Dataset trials complete
4,608 reads with zero errors, but the semantic process exits unsuccessfully and
its deleted stderr cannot be recovered. The runner then fails on missing log and
manifest paths. This execution does not pass installed-binding or restart
acceptance, and its service cgroup manifest is unavailable. Rerun only with a
stable artifact directory; do not substitute the completed trials for the missing
semantic/restart gate. The imported Docker image remains available independently
of the removed OCI archive.

## Performance work to consolidate later

- A native fault-matrix diagnostic run returned `Store(Client(Deadline))` from
  the independent verification client's first file-record load, after HTTP replay
  succeeded. No request timeout or caller retry was changed; two subsequent complete
  44-case runs passed. The cause of that one five-second client deadline remains
  unconfirmed. Capture fresh client routing/transport and backend timing if it
  recurs; do not describe it as fixed by FileIO scheduling changes.
- Native multipart diagnostics exposed unequal competing copy windows, tiny
  checkpoint-only leaves, repeated directory reads and duplicate JSON digest
  passes. Assembly now recomputes from the complete selected-part input without
  persisting byte progress. Broader batching, shared decoded caches,
  sustained throughput and recovery-page scaling remain measurement work, not
  implied guarantees from the original-bound functional fixture passing.

- SDK diagnostic: the first expanded in-memory Java lifecycle run returned 503
  at purge on 2026-09-24. One instrumented rerun and two fixed diagnostic batches
  (five and ten runs) passed without changing timeouts, adding retries or suppressing
  assertions. No server diagnostic was captured for the original failure; its root
  cause remains unconfirmed. Keep this as a follow-up observation, not a fixed bug
  or a reason to claim a stronger latency guarantee. Preserve the unchanged SDK
  command and capture request-admission/deadline diagnostics if it recurs.

- Historical namespace diagnostics measured roughly 45–75 ms per durable phase
  and intermittent failure under a 500-ms total bound. Refresh measurements before
  attributing current cost to any component; these are not current p95/p99 values.
- Full namespace CRUD uses the bounded 300,000-ms functional profile with
  delegation disabled; raw HTTP client timeouts remain five seconds. The separate
  maintenance fixture retains 500 ms. Their passing results are not evidence that
  every namespace mutation meets 500 ms. Earlier redundant immutable-payload and
  terminal-cleanup writes were fixed without altering publication CAS.
- Maintenance fixture repair errors for synthetic reserved name mappings without
  journals are expected from `verify_name_index`; retain the diagnostics rather
  than interpreting them as production corruption or suppressing them.
- Profile journal/retry-ledger round trips and durable payload/checkpoint writes
  on identical storage, concurrency and data. Prior redundant writes already
  received no-op/read-before-put fixes; do not reimplement them blindly.
- Consider batching or pipeline changes only with measured evidence and preserved
  publication/clear/replay invariants. Record before/after latency, KV round trips,
  storage I/O, CPU and memory alongside failure-injection regression results.
- Create one consolidated optimization backlog later. No new performance backlog
  or latency guarantee is introduced by the functional-test split itself.

## Current cleanup regression gates

Source `b566fdcd` adds compact upload-resource storage, one-step ordinary inline
purge completion and request-local Dataset delivery reductions. The upload/GC
batch passes 1,709 tests across five affected crates, with 13 independently
configured cases ignored. The subsequent Dataset/Access/native batch passes
669 tests, with the same 13 configured cases ignored. These suites overlap;
their counts are not additive. Workspace all-target/all-feature clippy, normal
lint, formatting and focused final regressions pass. The matching v7 image from
`a78e981f` passes six S3, five Dataset and five GC cases, plus the complete
TCP/installed-Python/restart workload. The v6 results below cover `c22bc40e`.

Source `d400ae21` also discovers expired multipart uploads by immutable bucket
identity after a bucket name is deleted and recreated. A production-actor
regression fails on the preceding scanner and passes with the fix. The affected
S3/Access/native suite passes 495 tests, with 12 separately configured cases
ignored; the final multipart target passes all 64 cases. Only expired Open
uploads are marked Aborted. Index, part and generation rows remain, and this
does not establish physical space recovery. Workspace all-feature clippy,
normal lint and formatting pass. Matching v7 service acceptance passes.

The upload codec keeps all owner identities and the public JSON contract while
writing a fixed 52-byte header plus 16 bytes per identity. Its maximum record is
69,684 bytes. Legacy JSON keeps its original validation, and CAS conditions use
the actual stored bytes. JSON-only Access servers and direct native clients
require a coordinated upgrade. No resource key or foreground RPC is added.
The fixed legacy authority-read budget is retained.

An exclusive CPU-2 release measurement uses seven samples of 128 iterations,
with the same logical 4,095 retired and two live owners. Canned retirement
includes GET cloning and exact CAS comparisons, without KV, WAL or network.
Compact input takes a median 25.644 us after sorted insertion, compared with
29.395 us using append-and-sort; validated compact round trips take 7.589 us.
Legacy JSON migration takes 654.204 us in the later run and 629.274 us in the
preceding run, so it is not reported as an insertion speedup. Raw samples,
timer-boundary measurements, source/binary hashes and scope are retained in
`/tmp/crowdb-c12-owner-cpu-20261025` and
`/tmp/crowdb-c12-owner-cpu-insert-20261025`. These are CPU observations, not TPS
or total-system efficiency claims.

Source `c22bc40e` additionally includes retained point-operation buffers across
routed retries, observed-authority multipart expiry and reused validated inline
digests. The six affected crates pass 1,109 tests, with 13 separately configured
cases ignored. Workspace clippy, normal lint and formatting pass. Its matching
`access-cleanup-v6-20261025` image passes six S3 protocol cases and all five
Dataset cases (`run-8376c20aa6ea425583e2d8d6be33fcd9`), plus the complete
[TCP/restart workload](#dataset-httpnative-comparison). The five-case GC scope also passes in
`run-d56438cb607349f3a4d819402ee8d484`, including exact full inline-backlog
convergence: 16 real-service cases in total.

Boundary regressions preserve the caller's point payload allocations through
first dispatch and routed retry. Expired multipart refresh uses one GET and one
CAS instead of two GETs and one CAS; independent public abort still reads its
own authority. Both native Dataset entry points verify inline corruption before
reusing the immutable result. These are copy/scan and logical-operation results,
not an isolated end-to-end throughput measurement of the changes. Raw v6
artifacts and immutable image identity are independently retained in
`/tmp/crowdb-access-cleanup-v6-evidence-20261025`.

The preceding source `ae4e1b8a` includes the bounded upload-retirement, Split-membership and
Iceberg receiver optimizations. All-target/all-feature Dataset/Access
Server/native coverage passes: 645 tests, 13 separately configured cases
ignored. Workspace clippy, `rs-lint` and formatting pass.
The matching `access-cleanup-v5-20261025` image passes 21 real-service cases:

- All 16 Iceberg file cases: `run-f2d2bfc503214a77bae216d1cc8942f6`.
- All five Dataset cases: `run-23ba53d4761b494781c86b799899ad17`.

The file scope includes the size matrix, 32 concurrent PUTs, 100-MiB PUT/MPU,
slow uploads, capacity failure/recovery, explicit cleanup and storage restart.
These functional checks do not measure receiver fast-path hit rate or copy CPU.
Executable cost regressions show ten shared upload references need one upload
record GET plus one CAS within a cleanup window, and twenty candidates in one
Split membership page need one bounded page read. Previously those paths used
ten and twenty reads respectively. These count logical metadata actions;
routed frame counts and end-to-end TPS savings are not inferred from them.

Logs, image identity, summaries and raw evidence are independently backed up in
`/tmp/crowdb-access-cleanup-v5-evidence-20261025`. The
[current Dataset comparison](#dataset-httpnative-comparison) records the same
image's complete TCP workload and installed-binding restart acceptance.

The preceding source `15efaaec` passes the broader affected
Dataset/Iceberg/S3/Access Server/native coverage across full runs and corrected
HTTP targets:
1,644 passed, 13 cases requiring a separate environment or measurement run
ignored. Workspace clippy, `rs-lint`, formatting, SDK dependency boundaries
and C++ tree lint pass.
The installed portable Python package passes 21 contract tests.

That matching `access-cleanup-v4-20261025` image passes 18 real-service cases.
Artifact identifiers are relative to `.crowdb-runtime/artifacts/container-e2e/`:

- Catalog creation/recovery: `run-3cbb504df0c6455e9613caf31be0ae1f`.
- Namespace CRUD/restart: `run-c1b02d1e879e49a881fbccbfec31cb8d`.
- Four GC cases: `run-5fb1fc306a394e59aefbc5393f3527b8`.
- Table metadata budget: `run-45c57f6f283a40fcbf47b6619377d189`.
- Five Dataset HTTP/SDK/restart cases: `run-05a9a16625e2492a84aab1953e2c7ec9`.
- S3 checksums/multipart: `run-3bf48fc5107b4433a48da5a8b6ab2e3c`.
- S3 fragmentation/boundaries: `run-f41c7b14cf57438fa374fd2393f3579d`.
- Independent S3 frontends: `run-4eb57d28dbc84853ac86fd1b802f7666`.
- Multipart copy/replaced parts: `run-76edf0fd31e341abb8f0cc3260254f82`.
- Selected multipart publication: `run-1cd9d713bdea4fac8b6104bbd79a7f99`.
- S3 metadata/copy/multipart: `run-51db5b3ff45340b18187286ca0a6cb65`.

Logs, manifests, image identity and trial summaries are also retained outside
the runtime/build directories in `/tmp/crowdb-access-cleanup-v4-evidence-20261025`.
Its complete TCP workload also passed in
`dataset-workload/50925d2ddaf04a5da8fb1687132c4228`; that earlier image's
measurements remain evidence for its own source.

## Earlier cleanup regression gates

At source `be5eca5c`, the affected Iceberg crate passes all 823 tests; Access
Server passes 201 tests with 11 existing ignored cases. Workspace all-target /
all-feature clippy, normal `rs-lint`, formatting, the 163 fixture unit tests and
container-client compilation pass.

The matching `access-cleanup-v3-20261025` image passes 12 affected real-service
cases. GC evidence is below. Namespace CRUD and native-storage/listener restart
pass in `container-e2e/run-79033ed427994b0ca117463063119374`.
Seven independent `s3.protocol` cases pass with the original checks and timeouts:

- AWS chunked trailers: `run-b59ea5a1911247dc802bbe593b2357df`.
- Default boto3 checksums and multipart: `run-704e090a405f47b9835b83e98e96b75d`.
- Fragmentation and storage boundaries: `run-a4f03ef2a6b642daa229a5b03af0b6f1`.
- Ordinary PUT size matrix: `run-37c54686be42429d8cee6ecf9de4afa3`.
- Slow signed upload and buffer release: `run-4b60cd2091a5470dafcc065bd1214c42`.
- Truncated signed upload and credit release: `run-b757efd59d334ed58237fd9ae846556c`.
- Signed raw HTTP wire contract: `run-5d9fbbfe7b894eb685dd6c68ac037ed3`.

These are functional and request-cost regressions. They do not measure the hit
rate or end-to-end throughput benefit of forwarding complete S3 body owners.

## GC scheduling and foreground observations

The v7 five-case scope passes in
`container-e2e/run-35fb5dec04fa4fe69dc9200665a10519`. The unchanged 128-file,
1,554-logical-byte case completes in 2,396,811 ms, with final revision 792.
All original canonical keys are absent and all candidates/task are Complete;
no deferred range, retry, stall or fence remains. First deletion occurs at
2,002,959 ms, revision 663. The observer records 2,393 task GETs and 256 final
verification GETs, preserving all 792 observed revisions.

The separate v6 run below completes at revision 920 in 2,785,403 ms. The current
inline path removes 128 checkpoint-only worker steps, consistent with the
128-revision difference. These independent same-host runs are not an isolated
timing comparison or a general GC capacity/SLO guarantee.

Four SDK workers complete 48 table lifecycles while 64 native writes succeed;
two observed deletion intervals overlap the finite load. SDK create/commit/drop
p50/p99 are 89.42/338.77, 47.15/56.36 and 41.39/46.37 ms. Native write p50/p99
are 0.72/84.88 ms. Full per-operation samples and exact image identity are in
`/tmp/crowdb-access-cleanup-v7-evidence-20261025`. This proves inline metadata
convergence and bounded overlap, without external range/tree notification or
physical capacity recovery evidence.

The preceding v6 five-case scope passes in
`container-e2e/run-d56438cb607349f3a4d819402ee8d484`. Its separate expired-inline
case starts with 128 canonical files (1,554 logical bytes), a genuinely elapsed
fixed deletion deadline, one-item pages and the production 30-minute idle
interval. Other window, concurrency and I/O budgets remain unchanged.

- First actual deletion: 2,005,848 ms, task revision 664.
- Complete verification: 2,785,403 ms, final task revision 920, deleted 128 and
  logical bytes 1,554. All 128 original file keys are absent and their exact
  candidates are Complete; no deferred ranges, retry, stalled state or fence
  remains. This is logical inline metadata retirement, not chunk notification
  or physical capacity release.
- The observer performs 2,781 task GETs and 256 separate final verification
  GETs. All 919 observed revisions and phase transitions remain in the raw log.
- After first deletion, four official SDK workers each process twelve tables
  while the native writer completes 64 writes. Both loads remain active across
  one observed interval with increasing deletion count, and a successful SDK
  operation finishes inside it. This is finite overlap, not sustained traffic
  throughout the 46-minute cleanup or an isolated GC performance baseline.
- One supplemental admin metrics request is recorded separately. Those counters
  belong to the foreground store; GC's independent store is not included.

The new case retains raw latencies and nearest-rank quantiles. SDK table actions
have 48 samples; namespace actions have four. Intentional 500-ms inter-table
and 250-ms inter-write delays are excluded from the operation timings.

| Operation          | Samples | p50 (ms) | p99 (ms) |
| ------------------ | ------- | -------- | -------- |
| Create namespace   | 4       | 75.22    | 75.38    |
| Create table       | 48      | 78.82    | 318.43   |
| Commit             | 48      | 42.87    | 50.72    |
| Load table         | 48      | 2.71     | 4.26     |
| Credentials        | 48      | 2.73     | 4.11     |
| Read metadata      | 48      | 2.52     | 9.31     |
| Drop table         | 48      | 38.53    | 51.99    |
| Drop namespace     | 4       | 245.05   | 253.12   |
| Native chunk write | 64      | 0.71     | 84.79    |

Raw output, exact candidate checks, summaries and image identity are retained
in `/tmp/crowdb-access-cleanup-v6-evidence-20261025`. The original foreground
case remains independently unchanged. Its earlier v3 observations follow.

The four GC container cases pass on `crowdb-node:access-cleanup-v3-20261025`
in `container-e2e/run-15dee04958f34b6f9ad7207fcff1d679`. The imported Docker ID is
`sha256:6636ec6d437cc3bbe32d4c0d355cd1b0a778fbc156797fb39f00ee8eed3cbe0d`,
source revision `be5eca5c52e8f64bdbc4c5c2e96fc6091af6729e`, source tree
`c4e3b70c54ba6c4b4aef2d54b0af21cf98b3f473`.

The foreground case uses the production default 30-minute idle interval,
one-item discovery pages, 128 seeded files and the original per-window budgets.
Four PyIceberg workers each create, commit, read and drop three tables while a
native chunk writer completes 64 writes of 128 bytes. All operations succeed.
The task is observed at revisions 1 and 2 at 0 and 1,018 ms; the continuation
check observes revision 3 at 4,077 ms. It remains in `Discover`: this proves
continued advancement, not complete backlog retirement or a sustained cleanup rate.

The test prints every successful operation's latency and nearest-rank quantiles:

| Operation             | Samples | p50 (ms) | p99 (ms) |
| --------------------- | ------- | -------- | -------- |
| Create namespace      | 4       | 281.07   | 282.19   |
| Create table          | 12      | 72.68    | 472.67   |
| Commit                | 12      | 33.58    | 50.15    |
| Load table            | 12      | 2.95     | 7.65     |
| Credentials           | 12      | 3.14     | 5.97     |
| Read metadata         | 12      | 2.79     | 20.89    |
| Drop table            | 12      | 146.84   | 151.18   |
| Drop namespace        | 4       | 243.95   | 244.53   |
| Native chunk write    | 64      | 0.54     | 85.17    |

Chunk timing includes writer preparation, payload delivery and finish; it excludes
the intentional 50-ms sleeps and initial client connection. It does not measure
an HTTP object PUT or object metadata publication. Its first sample is 85.17 ms
and remains included. The observer adds one task GET per 100 ms during overlap
and continuation checks. These same-host, unpinned, small-sample observations have
no isolated baseline; their p99 values are near maxima, not production latency
guarantees or evidence that GC adds no foreground cost.

```sh
pixi run clean-env && pixi run python e2e-test/run.py --layer iceberg --scope iceberg.gc --image crowdb-node:access-cleanup-v3-20261025 --concurrency 1 --cpus 4 --memory-mib 12288
```

## Dataset HTTP/native comparison

The workload runner now records finite observations around each original trial.
The child captures local cached status, announces its exact run/path/concurrency/
repeat/Snapshot identity, and waits while the parent reads owned service cgroups
and process counters. A second handshake captures the end before the independent
digest kernel or next trial. The original mixed sizes, concurrency, warmups,
three repeats, 4,608 requests, verification and 600-second child bound remain.
Direct child execution without the observation environment variable stays
standalone. The tool passes all 179 fixture unit checks, including 16
protocol/resource cases, and all 18 matching-image trial observations complete.

Each path writes its raw log and `*-observations.json`. Service CPU deltas include
background work and the explicitly timed sequential capture interval. Actual
member-process RSS, cumulative process HWM, charged cgroup memory and cumulative
cgroup peaks are retained separately; none is a per-trial peak. Local native
metadata, aggregate and data transport groups retain selected sent-frame/byte
deltas without adding repeated process RUSAGE counters. Missing, reset or changed
identities remain unavailable. Receive traffic, all-copy bytes, layout queries
and per-request header/first-byte/EOF timing remain unmeasured. The enclosing
service and inner client windows are not additive total-system CPU measurements.

The current v7 complete TCP/installed-Python/storage-restart acceptance passes in
`dataset-workload/419095e911824fdc9aefe50bbc5bacc9`: 18 trials, 4,608 requests,
zero errors. Source revision is `a78e981fdb8f3f8251aa3bc99e7beb88aa078d80`, tree
`32ac07470397bb295e58d58abf79d72f3dbb15a1`, imported Docker ID
`sha256:979f2646dc189f3c0cb6e8a936faf2e9380716b404372c680b87d40863f060a2`.
The same eight sizes, CPU placement, concurrency, warmups and budgets as the
preceding v6 run below are retained. Raw records, validated summaries and hashes
are backed up in `/tmp/crowdb-access-cleanup-v7-evidence-20261025`.

| Path          | Concurrency | MiB/s  | Inner client CPU (s) | Lifetime client RSS (MiB) |
| ------------- | ----------- | ------ | -------------------- | ------------------------- |
| HTTP          | 1           | 175.26 | 0.87                 | 11.59                     |
| Native Rust   | 1           | 207.31 | 0.91                 | 28.55                     |
| Native Python | 1           | 208.53 | 0.95                 | 55.21                     |
| HTTP          | 3           | 249.01 | 2.55                 | 17.07                     |
| Native Rust   | 3           | 280.41 | 2.68                 | 30.56                     |
| Native Python | 3           | 275.40 | 2.76                 | 60.90                     |

Service capture spans are 5.80–10.18 ms. For the three concurrency-one trials,
HTTP/native-Rust enclosing Access CPU is 0.809512/0.002916 s; metadata/control
CPU is 0.286016/0.336947 s. These are separate role windows including background
work and capture slack, not exact total-system CPU. The native metadata,
aggregate and data groups send 192/1,024/288 frames per 128-request trial.
Aggregate here names the bounded metadata transport, not business-message
aggregation. These groups omit control/layout RPCs and expose outgoing request
bytes rather than payload receive bytes. The observed compatibility fallback
copy counter stays zero; Python byte copies remain included in client work.
Differences from v6 are independent observations, not isolated patch effects.

The preceding v6 complete same-host TCP workload and installed Python/storage-restart
acceptance pass in `dataset-workload/d1fa8f646f124426b6c9e24688d92786` under
`.crowdb-runtime/artifacts/`. The manifest records image labels, CPU placement
and per-phase service counters; client logs retain all per-size p50/p99 and
digest timings. Raw trials and the aggregate summary are independently retained
in `/tmp/crowdb-access-cleanup-v6-evidence-20261025`.

- Image: `crowdb-node:access-cleanup-v6-20261025`, imported Docker ID
  `sha256:cc1f879276f039a13a0ed0b7232833d176a1659fc10bf508700a43ef980b4e2f`.
- Source revision: `c22bc40ebb6663b4b3cc4f0a89075a41f1c9f5a6`; source tree:
  `e2d0f660762290085649081fd2b437b53cd343da`.
- Client CPU 2; service CPUs 4–15; each metadata replica capped at 2 GiB.
  Metadata cgroup peaks were 204.61/204.60/204.68 MiB, without a stopped replica.
- Sizes: 0, 128, 513, 4,096, 65,535, 262,144, 1,048,576 and 4,194,321 bytes.
  Two warmup rounds precede three trials at each concurrency. Each worker
  performs 128 requests per trial: 4,608 measured requests, zero errors.
  Every path verifies exact bytes and integrity; Python byte-copy costs remain
  included. No workload budget, deadline or retry bound was increased.

Throughput is total bytes divided by total elapsed time across three trials.
Client CPU is the three-trial sum; RSS is the largest observed client value.
HTTP runs authority and payload processing in Access Server, while native
runs that work in the client. Client CPU alone does not measure total system
efficiency. HTTP v1 uses Manifest projection and native uses inspection
metadata; the product comparison includes this implementation difference.

| Path          | Concurrency | MiB/s  | Client CPU (s) | Peak RSS (MiB) |
| ------------- | ----------- | ------ | -------------- | -------------- |
| HTTP          | 1           | 160.06 | 0.92           | 11.24          |
| Native Rust   | 1           | 190.86 | 0.96           | 28.95          |
| Native Python | 1           | 187.70 | 0.98           | 54.60          |
| HTTP          | 3           | 255.21 | 2.55           | 16.73          |
| Native Rust   | 3           | 268.17 | 2.77           | 31.21          |
| Native Python | 3           | 266.02 | 2.86           | 61.30          |

Native Rust throughput is about 19% higher at concurrency one and 5% higher at
concurrency three in this execution. Client CPU and memory increase. Individual
sizes also differ: at concurrency one, the median trial p50 for 128 bytes is
557 µs over HTTP and 700 µs in native Rust; for 1 MiB it is 6,788 and 3,681 µs
respectively. At concurrency three, the 128-byte p50 is 798 versus 3,854 µs,
while its p99 is 9,548 versus 7,268 µs. These are medians of three trial
quantiles, not pooled percentiles. Only 16/48 observations per size enter each trial, so these tails
do not establish a production latency guarantee.
The separately timed resident-buffer 64-MiB MD5 kernel takes approximately
85 ms in Rust and 63 ms in Python; normal reads still perform integrity checks.

The shared fixture's service `memory.peak` is cumulative, and HTTP service CPU
also includes initial publication. Those counters are not isolated per-path
server peaks or a fair read-only server-CPU comparison. This is a pinned
same-host functional workload, not cross-host RDMA performance evidence.

The semantic phase compares HTTP/native ReadSpec and Split batch headers,
sample IDs and bytes; replays the original cursor on a second runtime and after
storage restart; and checks publication conflicts, multipart ownership, a
65-second idle, cancellation, caller-held buffers and an explicit complete-Split
deletion whose fixed deadline survives retry and storage restart. The native
runtime never uses HTTP as an implicit fallback.

Reproduce after building/importing the matching service image:

```sh
pixi run clean-env && pixi run python tools/sdk/dataset_workload.py --image crowdb-node:access-cleanup-v6-20261025
```

The explicit local-HCA profile remains a separate gate. Its current host setup
needs restoration; environment-gated unit cases are not counted as hardware
acceptance.

The preceding v5 run was
`dataset-workload/def69a711d4e468880c5cb9fae82e32e`; its native Rust throughput
was about 36%/5% above HTTP at concurrency one/three. The preceding v4 run was
`dataset-workload/50925d2ddaf04a5da8fb1687132c4228`; its image and raw evidence
remain preserved separately. Earlier accepted runs were
`dataset-workload/9dbf56c386e142e995b8eb400305b467`
and `dataset-workload/abea0a0568824233a32ba85a063e522a`; their original runtime
directories were removed as described above.
Differences between independent runs
are not an isolated before/after measurement of any one code change.

## Bounded immutable write comparison

Build/import the matching service image, then run:

```sh
pixi run clean-env && pixi run python e2e-test/write_batch_comparison.py --image crowdb-node:access-cleanup-20261025
```

The owned persistent Chunk-KV fixture compares release point writes and routed
batch writes on client CPU 2 and service CPUs 4–15. Both paths admit at most
16 rows / 2 MiB of logical key/value input, plus one prepared row. Three warmups
precede three paired, alternating trials for small schema/membership/field rows
and 64-KiB inline payloads with their actual JSON encoding. The test records typed
results, routes, owner RPC attempts, completed TCP frames and protocol bytes,
client CPU ticks and RSS. Artifacts include the image/source identity, client
binary/source hashes, original output and `comparison.json`.
Cold warmups account for one capability-handshake frame per new pooled TCP
connection. Measured trials require no new connections and exact equality
between owner RPC attempts and completed protocol frames.

This comparison does not publish a Dataset head. Logical admission bytes exclude
transport copies; TCP protocol bytes exclude IP/retransmissions. Allocation and
copied-byte counts are unmeasured. Process memory peaks are cumulative, and
short trials can report zero CPU ticks without having zero CPU cost. Report
the measured routing scope rather than assuming multiple partitions or owners.

The comparison passes in
`write-batch-comparison/acd38136ec0943aeac3aaa9d849c0a5c` with
`crowdb-node:access-cleanup-20261025` from source `7b2c61aa`, tree `aecf740c`.
Its manifest retains the exact imported image and client binary/source hashes.
All warmup and measured creates return typed Applied results. The actual route
is one partition, one owner and catalog generation 1; it is not a multi-partition
measurement. Each measured trial keeps both connections open and has no setup
frames or failed owner attempts.

| Rows                   | Path           | Min / median / max (ms)  | Owner RPCs | Sent protocol bytes |
| ---------------------- | -------------- | ------------------------ | ---------- | ------------------- |
| 256 metadata rows      | Sliding points | 371.3 / 469.4 / 470.0    | 256        | 80,680              |
| 256 metadata rows      | Routed batch   | 2913.6 / 3708.7 / 4102.8 | 16         | 167,864             |
| 16 inline 64-KiB rows  | Sliding points | 286.7 / 318.0 / 320.5    | 16         | 4,199,136           |
| 16 inline 64-KiB rows  | Routed batch   | 545.6 / 550.9 / 561.5    | 3          | 12,593,170          |

Keep the existing bounded sliding point writes for inspection preparation:
the current batch reduces RPC count but is slower in both profiles. Inline
values already encode bytes as JSON numbers in the Dataset row; the group
envelope JSON-encodes that row again, increasing protocol bytes to about three
times the point path. Both large-value paths actually admit seven rows at a
time because the logical byte bound wins before the row-count bound.

Across the three large-value trials, client CPU is 4 ticks for points and
21 ticks for batch at 100 Hz. Small-row CPU totals are only 3 and 2 ticks and
cannot establish a useful CPU advantage. The lifetime HWM reaches 60,116 KiB
during warmup and is shared by subsequent paths; it is not a per-path memory
comparison. Automatic catalog source refresh occurs twice in measured small
batches and once in measured large points; those calls remain included in
elapsed time and separately reported from owner RPCs.

The service processes each batch's items sequentially, while the point window
can overlap row completion. This and encoding/copying are concrete differences;
their individual contributions were not profiled. Alternating trials do not
reset accumulated persistent state, and three rounds give no stable latency
SLO. Reconsider grouped writes after improving the wire/processing path and
repeating this comparison, without relaxing ordering or publication barriers.

## Shared upload owner CPU

The release CPU decomposition passes on client CPU 2 in
`upload-owner-cpu/10866bc17bf14e53877a2d3be19c390e`. Its manifest retains binary
and source hashes; `measurement.log` and `measurements.json` retain every sample.
The fixture has two live owners and 1/64/256/4,095 retired owners. A retirement
removes one live owner and adds its permanent negative fence. The last case
reaches the existing 4,096-entry bound; it does not change the saturated-resource
format or late-claim rules.

Each mode has eight warmups and seven fixed 128-iteration samples. Fixture
setup is separate. Values below are medians of calling-thread CPU per operation,
with Linux scheduler accounting refreshed at both boundaries by a safe yield.
The empty boundary costs approximately 6.6 µs per sample and is reported without
subtraction. Wall-clock samples are retained separately. The initial unrefreshed
counter was tick-quantized and is not used for these results.

| Retired owners | Input record (bytes) | Serde roundtrip (µs) | Canned retirement (µs) |
| -------------- | -------------------- | -------------------- | ---------------------- |
| 1              | 343                  | 1.3                  | 1.5                    |
| 64             | 4,051                | 14.3                 | 15.1                   |
| 256            | 15,535               | 55.9                 | 57.5                   |
| 4,095          | 245,760              | 832.1                | 849.2                  |

The canned path calls production `retire_owner`, including its record checks,
one GET clone and one exact expected/result comparison. Its CAS returns a fixed
Applied outcome and does not persist. Standalone serde modes exclude session
validation. They are independently timed; subtracting or adding their medians
cannot apportion production CPU. At the largest input, decode and encode alone
measure approximately 574 and 236 µs. A separate test-store CAS/reset pair costs
about 106 µs and includes two mutations and fixture map/Vec cloning; it is never
subtracted from the authority measurement.

This establishes substantial whole-record codec cost with large shared-owner
histories. It is not routed RPC latency, WAL cost, allocation counting or an
end-to-end throughput limit. Keep the CAS and retired-owner safety contract;
review a smaller resource representation or different layout before changing
publication/reclaim ordering.

Reproduce in an exclusive window, with no service workload running:

```sh
pixi run cargo test --release --locked -p crowdb-access-dataset --test upload_owner_cpu_test --no-run
pixi run taskset -c 2 cargo test --release --locked -p crowdb-access-dataset --test upload_owner_cpu_test upload_owner_cpu_decomposition -- --ignored --exact --nocapture --test-threads=1
```

## Verification and execution notes

- Library: `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`.
- HTTP: `pixi run clean-env && pixi run -- cargo test -p crowdb-access-server --all-targets`.
  Default server tests alone skip the Iceberg suites.
- SDK: `pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_table_sdk_test -- --ignored --nocapture --test-threads=1`.
- Namespace SDK: `pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_namespace_sdk_test -- --ignored --nocapture`.
  Set `CROWDB_ICEBERG_E2E_PYTHON=$PWD/.pixi/envs/iceberg-e2e/bin/python` and the
  Java environment below. The pinned PyIceberg method requests complete lists;
  Java RESTCatalog implements token continuation. Neither client is patched.
- Native storage namespace: `pixi run test-container-e2e --scope iceberg.namespace --cpus 16 --memory-mib 16384`.
- Native SDK: `pixi run test-container-e2e --scope iceberg.sdk.java --cpus 16 --memory-mib 16384`.
- Native FileIO faults/lifecycle: `pixi run test-container-e2e --scope iceberg.files --cpus 16 --memory-mib 16384`.
- Keep component crash-cut matrices: `pixi run cargo test -p crowdb-access-server --test iceberg_commit_recovery_test --test iceberg_file_recovery_test`.
- Manual concurrent SF1 loaders use the [locked host TPC client](../../../e2e-test/clients/tpc-loader/README.md)
  and explicit `iceberg.manual.tpc` scope. They are excluded from ordinary CI.
- For Java tests, use default Pixi for Cargo; set
  `JAVA_HOME=$PWD/.pixi/envs/iceberg-e2e/lib/jvm` and
  `CROWDB_ICEBERG_E2E_MVN=$PWD/.pixi/envs/iceberg-e2e/bin/mvn`.
- Prefix native tests with clean-env using the same isolated
  `CROWDB_RUNTIME_ROOT=$PWD/.crowdb-runtime/ephemeral/iceberg-catalog-e2e`;
  preserve unrelated persistent port claims. Do not clean while another test runs.
- Gates: `pixi run -- cargo fmt --all -- --check`, `pixi run rs-lint`, and
  `pixi run -- cargo clippy -p crowdb-access-server --features iceberg-e2e --all-targets -- -D warnings`.
- Long native/full-suite commands run in the background and are polled, rather
  than being mistaken for failures at the default sixty-second shell cutoff.
- Maven SDK shutdown-thread/logging warnings are nonfatal in the passing native
  fixture. Pinned SDK dependency order must precede Hadoop's older transitives.
