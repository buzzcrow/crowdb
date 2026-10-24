<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Access server acceptance

Protocol authority: [native Iceberg design](../../../doc/design/access-server/iceberge/design-crowdb-iceberg.md).
Client recipes: [container development](../../../container/single-node-container/README.md).

- REST/official-SDK conformance, native durability and ecosystem compatibility
  are distinct evidence. Passing SDK tests cannot certify compute engines.
- The [manual ecosystem fixture](../../../container/single-node-container/tests/ecosystem/README.md)
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

## Dataset HTTP/native comparison

The complete same-host TCP workload and installed Python/storage-restart
acceptance pass in `dataset-workload/9dbf56c386e142e995b8eb400305b467` under
`.crowdb-runtime/artifacts/`. `manifest.json` retains image labels, CPU placement
and per-phase service counters; each client log retains all per-size p50/p99
observations and digest timings. `summary.json` aggregates the three trials.

- Image: `crowdb-node:access-stateless-20261025`, imported Docker ID
  `sha256:95dd137f69d4cf4e767945b02fec5e63d72ec530338e5ecdd1f342dbcadc1667`.
- Source revision: `4cd4f752d08c5dc48118965a5405a1c2d24082ad`; source tree:
  `528ce6b7bb9954663734242df1f2142d436b9c53`.
- Client CPU 2; service CPUs 4–15; each metadata replica capped at 2 GiB.
  Metadata cgroup peaks were 234/205/205 MiB, without a stopped replica.
- Sizes: 0, 128, 513, 4,096, 65,535, 262,144, 1,048,576 and 4,194,321 bytes.
  Two warmup rounds precede three trials at each concurrency. Each worker
  performs 128 requests per trial: 4,608 measured requests, zero errors.
  Every path verifies exact bytes and integrity; Python byte-copy costs remain
  included. No workload budget, deadline or retry bound was increased.

Throughput is total bytes divided by total elapsed time across three trials.
Client CPU is the three-trial sum; RSS is the largest observed client value.

| Path          | Concurrency | MiB/s  | Client CPU (s) | Peak RSS (MiB) |
| ------------- | ----------- | ------ | -------------- | -------------- |
| HTTP          | 1           | 183.81 | 0.87           | 11.38          |
| Native Rust   | 1           | 213.80 | 0.89           | 28.72          |
| Native Python | 1           | 202.29 | 0.95           | 55.08          |
| HTTP          | 3           | 254.64 | 2.46           | 17.51          |
| Native Rust   | 3           | 271.47 | 2.72           | 30.26          |
| Native Python | 3           | 268.57 | 2.84           | 60.52          |

Native throughput improves for this mixed corpus, while client memory and CPU
increase. Individual sizes do not all improve: at concurrency one, the median
trial p50 for 128 bytes is 524 µs over HTTP and 666 µs in native Rust; for 1 MiB
it is 6,361 and 3,035 µs respectively. Only 16/48 observations per size enter each
trial, so these tails do not establish a production latency guarantee.
The separately timed resident-buffer 64-MiB MD5 kernel takes approximately
85 ms in Rust and 63 ms in Python; normal reads still perform integrity checks.

The shared fixture's service `memory.peak` is cumulative, and HTTP service CPU
also includes initial publication. Those counters are not isolated per-path
server peaks or a fair read-only server-CPU comparison. This is a pinned
same-host functional workload, not cross-host RDMA performance evidence.

The semantic phase compares HTTP/native ReadSpec and Split batch headers,
sample IDs and bytes; replays the original cursor on a second runtime and after
storage restart; and checks publication conflicts, multipart ownership, a
65-second idle, cancellation and caller-held buffers. The native runtime never
uses HTTP as an implicit fallback.

Reproduce after building/importing the matching service image:

```sh
pixi run -- python tools/sdk/dataset_workload.py --image crowdb-node:access-stateless-20261025
```

The explicit local-HCA profile remains a separate gate. Its current host setup
needs restoration; environment-gated unit cases are not counted as hardware
acceptance.

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

| Rows                   | Path           | Min / median / max (ms) | Owner RPCs | Sent protocol bytes |
| ---------------------- | -------------- | ----------------------- | ---------- | ------------------- |
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
