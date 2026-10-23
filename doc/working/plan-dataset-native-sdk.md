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

- [x] **Add optional native binding**: main-workspace PyO3 extension
  `crowdb_dataset_native`, with owned runtime, Dataset control/read operations,
  retained immutable payload objects, explicit ack/cancel/close and exception
  mapping. Keep `sdk/` HTTP distribution free of native storage dependencies.
  Files: `lib/crowdb-sdk-native-python/`, `tools/sdk/`, workspace manifest.
- [x] **Verify semantics**: shared authority tests and native facade tests for
  projection/missing/tombstone, Split/revision/worker selection, explicit ack
  replay, TTL, corruption/truncation, cancellation, dropped pulls, held memory,
  stale routing, partial transport failure and S3/runtime coexistence. Files:
  native and access-dataset `tests/`, access-server parity tests.
- [~] **Bound shared KV retention**: retain acceptor nodes through concurrent
  lookup/reclamation, reclaim only below an all-learner and durable-snapshot
  certificate, and propagate that certificate to followers with membership
  and term fencing. Verify RPC, maintenance, replay and concurrent access;
  prove the unchanged full workload no longer OOMs before closing this task.
  Files: `lib/crowdb-kv/src/{paxos,cluster,rpc}/`, related tests, protocol schema.
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

The packaging, namespace and producer-chain fixes are committed in `d3e285b2`.
All native SDK target tests, native/Python all-feature clippy, fmt, rs-lint and
19 image-identity tests pass. A first unprivileged suite invocation could not
open the TCP fixture listener; the authorized socket-capable rerun passes.

Strict same-host HCA acceptance passes in
`native-dataset-hca/3d1ddb03a4f148d2917300cb6f97a2c1`: 4-MiB multipart
publication, Split read, durable owner cleanup, restart, registered receive
ownership and held buffers after runtime shutdown. Fallback, protocol, CQ,
send and receive-post error counters stay zero. This is local HCA functional
evidence, not a physical multi-host throughput claim.

Long-run OOM diagnosis also finds that no production caller trims/reclaims
Paxos acceptor history. Simply enabling `PxSlotList` reclamation is unsafe:
its documented late-pin race and `get_or_prepare_slot`'s unpinned returned
reference permit a reader to outlive its node. The bounded retention fix must
first give each acceptor access a retained node, then reclaim only below all
learners' applied frontier and durable snapshots. Heartbeat progress from
peers completing after the first quorum must be retained, and followers need
an explicit membership-fenced reclamation watermark distinct from the commit
frontier. Keep the exact failed workload and 2-GiB limits for verification.

Installed Python semantics and restart acceptance passes in
`dataset-workload/988b49756a1841e0862b63b75977a222`, snapshot
`afc3c2c1c2be4960aa78b894f924d11c`. HTTP publishes all eight payload sizes;
Python verifies exact bytes, disjoint workers, explicit ack/unacked replay,
65-second idle across the 60-second lease TTL, held-budget pressure/cancel,
multipart publication, Split, owner cleanup and retained buffers after close.
This `--semantics-only` run is functional evidence, not performance acceptance.

## Handoff 2026-10-23

### Scope and pause

- User requested a handoff, commit of completed work, and deferred long-running
  acceptance. Stop after the commit; do not start another image build or workload.
- Branch: `task-rdma`. R5 is complete; retain its accepted evidence in
  [RDMA acceptance record](plan-rdma.md). Physical multi-node testing is only a
  refined [independent plan](plan-rdma-multi-node.md), not a remaining R5 gate.
- R220 is not complete. Keep its requirement/backlog entry and this plan until
  full workload acceptance and final cleanup. Do not restart R242 or R5 work.

### Completed implementation and evidence

- Native Dataset implementation: `f5fefdfe`; common workload: `982c1809`.
  Shared read-lease CAS fixes: `93aefa43`, `4127a966`, `a8e1571e` with focused
  competing-renewal regressions. HTTP contention no longer returns 409 in the
  recorded full HTTP runs; the subsequent failure is metadata process OOM.
- Packaging/default namespace/bounded multi-owner producer fixes: `d3e285b2`.
  Python fills bounded final-frame owners once; multi-part producer chains
  preserve original pointers through the durable writer. Ordinary Python input
  still needs that initial copy; do not claim a completely copy-free upload.
- Ordinary Dataset E2E passes in
  `container-e2e/run-97e635e2d2054f1b8f6ff8d44e7a475c`.
- Strict local HCA passes in
  `native-dataset-hca/3d1ddb03a4f148d2917300cb6f97a2c1`, using
  `rocep35s0f0` / `10.200.1.1`. Python TCP semantics passes in the run above.
- Artifact paths are relative to `.crowdb-runtime/artifacts/`. Prior images and
  artifacts remain evidence for their own source versions, not the new KV fix.

### KV retention change in this handoff commit

- `PxAcceptor` uses the existing lock-free `SkipMap`, retaining its `Entry`
  throughout node access. The old `PxSlotList` late-pin/unpinned-reference paths
  are no longer used by the acceptor. Late requests below trim are rejected;
  outstanding cloned payload owners survive reclamation.
- `group_retention` derives a certificate capped by every configured learner's
  applied progress, local durable snapshot and a durable voting witness.
  Singleton voting groups need only local durability but still wait for their
  non-voting learners. Replies completing after quorum now update progress.
- Heartbeats carry an exclusive GC boundary and membership epoch. Followers
  retain epoch, term, leader and watermark together in `ArcSwapOption`, then
  cap reclamation by local applied/snapshot state. Epoch adoption cannot relabel
  an old certificate. Membership replacement and new tenure reset evidence.
- Maintenance drives acceptor trim/reclaim and WAL segment GC. Status exposes
  `acceptor.trim_before_slot` and `acceptor.retained_slots`. RPC heartbeat
  implementation is split into child modules; protocol fields are appended.
- Focused verification passes: four group retention tests, thirteen acceptor
  tests including concurrent readers/removers and retained payload lifetime.
  `pixi run clean-env && pixi run cargo test -p crowdb-kv -p crowdb-protocol
  --all-targets` passes, including election, group, WAL/replay and RPC tests.
  Scoped KV/protocol all-feature clippy, workspace `rs-lint`, Rust formatting
  and `git diff --check` pass. No C++ source changed in this commit.
- No new lock, dependency or unsafe exception is introduced. Generic slot-list
  reclamation remains a separate utility; this change does not establish that
  utility's general concurrent-reclamation safety.
- Long-run memory/performance acceptance is still pending. The ordered map
  adds per-slot allocation/lookup overhead; do not describe it as a measured
  throughput improvement. A stalled configured learner can still hold GC back.

### Next work, in order

1. Resume from the committed branch and check `git status` before editing.
   Rebuild/import the server image from the current source before real-service
   acceptance; the currently accepted image predates the retention change.
2. Run the unchanged complete HTTP/native Rust/native Python workload. Keep
   client CPU 2, server CPUs 4-15, metadata memory 2 GiB, all eight payload sizes,
   warmups/repeats and exact-byte checks. The runner pins one common snapshot.
   Do not use `--semantics-only` as the performance gate; do not raise memory,
   shorten the workload, extend deadlines or add request retries to obtain a pass.
3. Inspect retained slots, trim progress, per-container RSS/CPU and exit status
   alongside throughput and per-size tails. Compare phase/state effects: an
   HTTP phase earlier in the same accumulating fixture is not an isolated
   transport-speed baseline. Report trade-offs even if native TPS does not win.
4. If memory is still unbounded, diagnose the first growing owner. Separate
   known remaining amplification: `ChunkStream::publish_state` rewrites all
   historical extent pages per append, and `Partition::reclaim_published_checkpoint`
   has no production caller. These paths are not changed by this commit.
5. After final behavior changes, run affected real-service restart/TCP/local-HCA
   acceptance and applicable gates. Only then remove the R220 detail, backlog
   entry and this plan in a cleanup commit, moving final accepted evidence to
   the retained record and fixing its links.

Commands from the repository root (run long commands asynchronously and poll):

```bash
pixi run image-build --privileged
pixi run image-import target/crowdb.oci.tar --image crowdb-node:dev
pixi run -- python tools/sdk/dataset_workload.py
pixi run -- python tools/rdma/run-native-dataset.py --address 10.200.1.1 --device rocep35s0f0
pixi run clean-env && pixi run test-e2e-dataset
```

The currently imported pre-retention image is
`sha256:6a840db3f06d2247d5827b2ed59e75fca3f547bb6427d8f6460db6831469f242`
with revision `d3e285b2`; the runner must reject it after the server-source change.
Docker, HCA and socket-based tests require the existing authorized execution
environment. Do not run heavy builds concurrently with measured trials.

The failed full comparison to reproduce is
`dataset-workload/8cf0653d34f74e0c955f5ac5642d1316`: HTTP six trials and native
Rust singleton trials completed, then native multi-flight failed with a Chunk-KV
deadline; metadata replicas 1 and 2 exited 137 with `OOMKilled=true` at 2 GiB.
Those partial rates are not accepted comparative performance. There is no
remaining user design decision and no requirement `Blocked` condition.
