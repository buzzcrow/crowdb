<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Native Dataset SDK Plan

Upstream: [R220](../backlog/R220-dataset-fat-client-plan.md),
[Dataset](../design/access-server/dataset/design-crowdb-access-dataset.md),
[native runtime](../design/access-server/design-crowdb-native-sdk.md).

Goal: attach trusted Dataset access to the accepted runtime with explicit ack,
bounded owner-backed delivery and independently packaged Python bindings.

## Baseline and contract

- [ ] **Measure HTTP**: add a reproducible release Rust consumer over the owned
  real-service fixture. Pin one client physical core, record the server CPU
  placement, immutable image/source, snapshot, mixed payload sizes, warmup,
  repeats, concurrency, completed requests, per-size p50/p99, throughput,
  process CPU/RSS and separately timed digest work. Preserve exact-byte checks.
  Files: `sdk/crates/crowdb-sdk-dataset/examples/`, `tools/sdk/`.
- [x] **Freeze API wiring**: use `DatasetSessions` (including Split preparation),
  not legacy `DatasetDirectClient::scan` which acknowledges fetched batches.
  Attach `ChunkKvDatasetStore`, `DatasetUploads` and `DatasetAuthority` to the
  shared runtime's Dataset writer group. Use explicit `DatasetIdentity`; no
  bearer/secret lookup or HTTP fallback. Files: `lib/crowdb-sdk-native/src/dataset/`.

## Shared payload and native facade

- [x] **Retain payload owners**: extend the existing payload/upload boundary
  with lazy `OwnedPayload` delivery and verified lengths/digests. Keep the
  byte-returning HTTP path compatible. Accept frozen producer storage without
  changing publication or durable intent recovery. Files:
  `lib/crowdb-access-dataset/src/payload/`, `src/upload/`.
- [x] **Expose authority operations**: admit control calls with the existing
  `OperationContext`, preserve typed Dataset errors and mutation uncertainty.
  Reuse publication, upload, composition, Split and cleanup services rather
  than duplicating their CAS rules. Files: native `dataset/`, `error.rs`.
- [x] **Deliver bounded batches**: separate immutable field metadata, payload
  leases, verified EOF and acknowledgement. Pull at most a bounded part ahead;
  charge retained allocations, preserve pending data if a pull future is
  dropped, and make terminal errors sticky. Renew the existing 60-second
  active-read lease while producing; close/cancel stops production and releases
  protection without acknowledging or invalidating caller-held buffers.
  Files: native `dataset/read/`, shared session/lease code as needed.
- [x] **Share topology and lifetime**: expose existing runtime observations
  with Dataset locations as hints only. S3 and Dataset use independent facades
  and writer policies over shared admission/transports; closing a facade or
  read cannot close the runtime. Files: native `dataset/`, `tests/`.

## Python and acceptance

- [~] **Add optional native binding**: main-workspace PyO3 extension
  `crowdb_dataset_native`, with owned runtime, Dataset control/read operations,
  retained immutable payload objects, explicit ack/cancel/close and exception
  mapping. Keep `sdk/` HTTP distribution free of native storage dependencies.
  Files: `lib/crowdb-sdk-native-python/`, `tools/sdk/`, workspace manifest.
- [ ] **Verify semantics**: shared authority tests and native facade tests for
  projection/missing/tombstone, Split/revision/worker selection, explicit ack
  replay, TTL, corruption/truncation, cancellation, dropped pulls, held memory,
  stale routing, partial transport failure and S3/runtime coexistence. Files:
  native and access-dataset `tests/`, access-server parity tests.
- [ ] **Run real services and compare**: use the same corpus/snapshot for HTTP
  and native Rust/Python, restart storage, validate explicit resume and held
  buffers, TCP and local shared-HCA policies. Run the same pinned workload and
  report measured trade-offs, not an assumed throughput win. Physical multi-node
  execution belongs to its independent plan and is not scheduled here. Files:
  `e2e-test/`, `tools/sdk/`, local-HCA runner.
- [ ] **Document and clean up**: update supported native API and Dataset design,
  retain reproducible benchmark artifacts; remove the completed requirement,
  backlog entry and this plan after applicable gates.

## Verification

- Unit/integration: `pixi run clean-env && pixi run -- cargo test -p
  crowdb-sdk-native -p crowdb-access-dataset -p crowdb-access-server --all-targets`.
  Native Python binding tests run against the built installed package.
- Boundary: `pixi run check-dataset-sdk-boundary`, `pixi run test-dataset-sdk`,
  `pixi run test-dataset-sdk-python`.
- E2E: `pixi run clean-env && pixi run test-e2e-dataset`; independent owned
  baseline/native workload and local-HCA profile commands are recorded below
  when implemented. Never count missing prerequisites as a passing case.
- Gates: `pixi run -- cargo fmt --all -- --check`,
  `pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings`,
  `pixi run rs-lint`, affected fixture tests and standalone SDK fmt/clippy.

## Evidence

R5 is complete in `62653eef` and `f5b16896`. Its physical multi-node validation
is an independently scheduled, plan-only follow-up; no remote tests were run.

Initial HTTP evidence: `.crowdb-runtime/artifacts/dataset-workload/6c77b1fe70df42bfbb71674b9bc5d143`.
The release client used CPU 2; all server containers used CPUs 4-15, real
file-backed storage and the unchanged accepted image. Eight sizes from 0 to
4 MiB + 17, two warmup rounds and three 128-request singleton trials completed
with exact-byte/digest checks. Singleton trials measured 18.6-29.4 requests/s,
55-58 client CPU ticks (100 Hz), and 11.3 MiB peak RSS. These short runs have only
16 observations per size/trial; tail estimates are exploratory, not a stable
performance claim. The resident-buffer MD5 kernel measured 64 MiB in 111-115 ms.

The first multi-flight attempt correctly exceeded the HTTP SDK's default
128-MiB admission (three 48-MiB stream reservations); the driver now explicitly
records a 256-MiB budget and disables HTTP retries. The subsequent run failed
with HTTP 503 because metadata node 0 was OOM-killed at the fixture's 512-MiB
limit (Docker `OOMKilled=true`, exit 137). This is a failed baseline, not accepted
multi-flight performance. The workload runner now reserves 2 GiB for each of
its three metadata nodes and records stopped-process state without masking the
original failure. Rerun the complete comparison after rebuilding changed native
server dependencies. No production memory or retry policy was relaxed.

The host-only runner is excluded from server-image identity checks; 19 image
identity tests and the standalone workload fmt/clippy gates pass.

Native implementation evidence: all native SDK target tests pass, including
Dataset explicit-ack replay, dropped capacity waits, retained owners after
cancellation/shutdown, and shared S3/Dataset publication. The production framed
writer test retains the original producer pointer through durable storage;
composition cleanup retains published upload ownership. Shared upload-stream
tests validate length/digest and terminal partial failures.

Native/Python all-target, all-feature clippy passes. The release native wheel
builds, bundles its native shared libraries and imports from a separate installed
consumer. Full Python semantics, common-snapshot performance and real-service
restart are pending; the wheel build/import alone is not binding acceptance.

The shared Dataset/Access Server all-target tests, standalone HTTP SDK boundary
and Rust tests, isolated HTTP sdist/wheel tests (10 Python cases), and 163 fixture
unit tests pass. Workspace all-feature clippy, rs-lint, version consistency and
fmt pass. Environment-gated HCA/installed-HTTP-Python cases in the default Rust
suite remain separate acceptance steps, not implicit passes.

The implementation is committed in `f5fefdfe`. Additional native tests confirm
Dataset snapshot selection survives stale-owner refresh and that projected
missing fields and tombstones remain distinct. The service image is being
rebuilt from the committed source before running the new workload and local-HCA
scripts. Both scripts own and remove only their own fixture resources.

Real-service comparison exposed a shared read-lease contention bug, independently
reproduced in runs `3258f05c74d84ecfb76bca7f6065bc27` and
`f79a47a4449b4035a7c8761f830a9020`. HTTP singleton trials passed; concurrency three
returned 409 after a lease update lost all 64 CAS attempts. All containers stayed
alive without OOM. Temporary diagnostics showed another lease update advancing
the record between each GET and CAS (final expected revision 2925, observed 2930).
These runs are failed acceptance, not comparable native performance.

The shared authority now recomputes from the CAS conflict observation, preserving
other handles and reclaim fences without reopening a point-read race each time.
HTTP heartbeats begin after 15 seconds instead of immediately duplicating lease
acquisition. A deterministic competing-renewal test fails before this fix and
passes afterward, including independent release and terminal reclaim protection.
Temporary diagnostics are removed. Full affected tests and rebuilt real-service
acceptance are running before this item can close.

Run `9b155417702648d88850778d946737df` still exhausted the same lease CAS bound
at concurrency three after that first fix. Reusing the conflict observation
alone does not prevent competing acquisitions and asynchronous releases from
repeatedly colliding. A second regression synchronizes 96 independent readers:
the previous implementation fails acquisition; bounded randomized contention
windows allow all acquisitions and releases to finish. The CAS budget remains
64; only repeated conflicts wait 1-16 ms, then refresh the observation. No HTTP
retry, lock, or lease/reclaim invariant is changed. Focused contention and reclaim
tests, formatting and affected all-feature clippy pass. Rebuilt real-service
acceptance remains necessary before claiming this resolves the load failure.

The fixed short window still failed in `ec575a0475a541e782ac269756cda8f5`.
Temporary complete conflict histories in diagnostic run
`cf24af1bf19a4c8c8307fd35d64e00e2` show other handles advancing the record at
every attempt, with durable CAS round trips around 28-35 ms. The contention
window now scales with that measured round trip (capped at 256 ms), refreshes
after waiting and preserves the 64-attempt bound. All diagnostics are removed.
Dataset all-target tests, fmt and workspace rs-lint pass.

The exact HTTP reproduction `387d9466a9814362b11494353f2d605e` now completes
all six measured trials (1,536 requests plus warmups) with exact bytes and zero
errors. This is correctness evidence, not a throughput win: singleton rates
are 30.6-36.0 requests/s, while concurrency-three repeats degrade from 17.7 to
11.5 to 6.1 requests/s and tails reach seconds. The full common-snapshot
HTTP/native/Python comparison must retain this elapsed-run/state limitation;
sequential phase results cannot establish a transport speedup by themselves.

Full comparison `8cf0653d34f74e0c955f5ac5642d1316` completes the six HTTP trials
without lease errors and three native Rust singleton trials with exact bytes.
The next native multi-flight phase fails with a Chunk-KV deadline because
metadata replicas 1 and 2 are OOM-killed at their 2-GiB limits (exit 137).
The metadata group's tree snapshots and immutable extent pages grow throughout
the run; `ChunkStream::publish_state` rewrites every historical extent page on
every append, and no production caller currently drives
`Partition::reclaim_published_checkpoint`. This is unfinished shared-storage
diagnosis, not accepted native performance. Do not increase memory, shorten
the workload, or retry the failed requests to count the comparison as passed.

Ordinary `pixi run clean-env && pixi run test-e2e-dataset` passes in
`container-e2e/run-97e635e2d2054f1b8f6ff8d44e7a475c`, including HTTP publication,
restart/recovery and installed Rust/Python Split consumers.

Repeated native wheel packaging exposed a cached extension hardlinked to
Maturin's repaired staging artifact: the second build references wheel-local
hashed SONAMEs unavailable to its linker search. Packaging now emits an
unrepaired Linux wheel, then runs external auditwheel repair on that wheel;
two consecutive build/install/import passes preserve the Cargo cache. Host
`libibverbs` and `librdmacm` remain external so the installed host providers
register devices with the same library instance. Bundling a renamed verbs
library had produced zero devices despite two available HCAs.

The Python default namespace now matches HTTP/Rust `NamespacePath::root()`;
an empty namespace previously made the HTTP-published snapshot invisible.
The new `--semantics-only` workload mode preserves real publication, Python
semantics and restart while independently diagnosing the long-run OOM; it is
not performance acceptance.

HCA run `abc5aeec5cea4ced80f79ea3392aa521` establishes native connections but
fails its first 4-MiB upload: producer allocations support at most 1 MiB.
The native facade now concatenates independently retained final-frame owners;
Python fills each bounded allocation once. The maximum-part regression
verifies every original payload address survives the durable writer, while
partial allocation failure, short interior frames and mixed budgets release
their credits correctly. Six focused producer/upload tests pass. Real HCA
and common-snapshot semantics are still pending.
