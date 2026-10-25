<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Native Dataset SDK Plan

Upstream: [R220](../backlog/R220-dataset-fat-client-plan.md),
[Dataset](../design/access-server/dataset/design-crowdb-access-dataset.md),
[native runtime](../design/access-server/design-crowdb-native-sdk.md).

Goal: attach trusted Dataset access to the accepted runtime with client checkpoints,
bounded owner-backed delivery and independently packaged Python bindings.

## Baseline and contract

- [x] **Measure HTTP**: add a reproducible release Rust consumer over the owned
  real-service fixture. Pin one client physical core, record the server CPU
  placement, immutable image/source, snapshot, mixed payload sizes, warmup,
  repeats, concurrency, completed requests, per-size p50/p99, throughput,
  process CPU/RSS and separately timed digest work. Preserve exact-byte checks.
  Files: `sdk/crates/crowdb-sdk-dataset/examples/`, `tools/sdk/`.
- [x] **Replace persisted read sessions**: user decision 2026-10-24 supersedes
  the earlier ack/resume/close contract. Use `DatasetReads`, caller-owned
  continuations and local checkpoints; track cleanup and validation in
  [Access cleanup](plan-access-simplification.md). Historical test notes below
  describe their original revisions, not the current API.
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
  dropped, and make terminal errors sticky. Check deletion admission once;
  close/cancel returns local request credit without persistent reader cleanup,
  acknowledgement or invalidation of caller-held buffers.
  Files: native `dataset/read/`, shared authority admission.
- [x] **Share topology and lifetime**: expose existing runtime observations
  with Dataset locations as hints only. S3 and Dataset use independent facades
  and writer policies over shared admission/transports; closing a facade or
  read cannot close the runtime. Files: native `dataset/`, `tests/`.

## Python and acceptance

- [x] **Add optional native binding**: main-workspace PyO3 extension
  `crowdb_dataset_native`, with owned runtime, Dataset control/read operations,
  retained immutable payload objects, local checkpoints/cancel/close and exception
  mapping. Keep `sdk/` HTTP distribution free of native storage dependencies.
  Files: `lib/crowdb-sdk-native-python/`, `tools/sdk/`, workspace manifest.
- [x] **Verify semantics**: shared authority tests and native facade tests for
  projection/missing/tombstone, Split/revision/worker selection, caller-cursor
  replay, deletion admission, corruption/truncation, cancellation, dropped pulls, held memory,
  stale routing, partial transport failure and S3/runtime coexistence. Files:
  native and access-dataset `tests/`, access-server parity tests.
- [x] **Bound shared KV retention**: retain acceptor nodes through concurrent
  lookup/reclamation, reclaim only below an all-learner and durable-snapshot
  certificate, and propagate that certificate to followers with membership
  and term fencing. Verify RPC, maintenance, replay and concurrent access;
  full-workload memory acceptance
  also requires the separate immutable metadata cleanup below.
  Files: `lib/crowdb-kv/src/{paxos,cluster,rpc}/`, related tests, protocol schema.
- [x] **Reclaim serving metadata history**: run bounded immutable extent-page
  reclamation for independent serving assignments; keep WAL bytes and tree
  checkpoint retention unchanged. Serialize source checkpoint pins with metadata
  GC using existing maintenance exclusion, batch deletion, and verify old-root
  recovery, retained retry results and Split/transfer pins. Fix complete-chunk
  trim eligibility before any existing explicit trim can delete a partial chunk.
  Files: chunk-kv `partition/checkpoint.rs`, `partition/maintenance.rs`, server
  maintenance/storage, chunk-stream `kv.rs`, focused integration tests.
- [~] **Run real services and compare**: use the same corpus/snapshot for HTTP
  and native Rust/Python, restart storage, validate caller-checkpoint replay and held
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

### Historical stop point (superseded by the deletion redesign)

- Metadata memory is fixed at `b18efa9a`: unchanged full run
  `dataset-workload/c7fd408b216a451e81feb9009e2eeac1` stays below 280 MiB,
  without swap/OOM. Native multi-flight still exhausts lease CAS attempts.
- Native EOF/cancel cleanup now retains request admission through its existing
  bounded task; focused lifecycle tests pass. This is a resource-accounting fix,
  not a claim that the contention failure or R220 acceptance is complete.
- After five contention corrections, the slow durable-CAS regression still
  fails. The approved deletion redesign below replaces this model. The unaccepted
  authority RTT experiment and failing regression are preserved in stash
  `a6e19f404d9cb614a3004a09575051196d6d0727`; no experiment is discarded.
- The recorded HCA/Python semantics evidence below predates the final memory
  and cleanup changes. Full mixed-size comparison, current-source HCA/restart,
  and requirement/doc cleanup remain outstanding.

### Scope and resume

- The handoff was committed in `0f5d94a8`. The user has resumed work and
  authorized completing the remaining acceptance and cleanup.
- Branch: `task-rdma`. R5 is complete; retain its accepted evidence in
  [RDMA acceptance record](plan-rdma.md). Physical multi-node testing is only a
  refined [independent plan](plan-rdma-multi-node.md), not a remaining R5 gate.
- R220 remains open for current-source HCA acceptance and final cleanup. The
  complete current-source TCP workload has passed. Keep the requirement/backlog
  entry and this plan until the HCA gate passes. Do not restart R242 or R5 work.

### Completed implementation and evidence

- Native Dataset implementation: `f5fefdfe`; common workload: `982c1809`.
  Shared read-lease CAS fixes: `93aefa43`, `4127a966`, `a8e1571e` with focused
  competing-renewal regressions. HTTP contention no longer returns 409 in the
  recorded full HTTP runs. Subsequent metadata OOM is fixed; native contention
  remains, as recorded at the current stop point above.
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

1. Restore the existing local-HCA test address/local-loop setup, then run the
   strict native Dataset profile on `access-cleanup-v5-20261025`. The latest
   read-only host check finds an ACTIVE device but no test IP or network
   namespace. Do not count TCP or environment-gated unit cases as HCA evidence.
2. Retain the unchanged complete TCP comparison and restart results at source
   `ae4e1b8a`: 18 trials, 4,608 reads, zero errors, installed Python semantics
   and fixed Split-deletion deadlines across restart. CPU placement, eight
   sizes, warmups, repeats, budgets and integrity checks are unchanged. See the
   [accepted measurements](../../app/crowdb-access-server/tests/README.md#dataset-httpnative-comparison).
   Native Rust gains about 36% at concurrency one and 5% at three in this run,
   with higher CPU and RSS; these are measured trade-offs, not an isolated
   before/after benefit of one cleanup patch.
3. After the applicable HCA gate passes, retain its evidence and remove the
   completed R220 detail, backlog entry and this plan in a cleanup commit,
   fixing links. Physical multi-node validation and the pending Access
   Namespace DROP decision remain independently owned work.

Commands from the repository root (run long commands asynchronously and poll):

```bash
pixi run image-build --output target/crowdb-access-cleanup-v5-20261025.oci.tar
pixi run --manifest-path container/oci-image/pixi.toml import target/crowdb-access-cleanup-v5-20261025.oci.tar --image crowdb-node:access-cleanup-v5-20261025
pixi run clean-env && pixi run python tools/sdk/dataset_workload.py --image crowdb-node:access-cleanup-v5-20261025
pixi run python tools/rdma/run-native-dataset.py --image crowdb-node:access-cleanup-v5-20261025 --address 10.200.1.1 --device rocep35s0f0
CROWDB_E2E_IMAGE=crowdb-node:access-cleanup-v5-20261025 pixi run test-e2e-dataset
```

The historical pre-retention image was
`sha256:6a840db3f06d2247d5827b2ed59e75fca3f547bb6427d8f6460db6831469f242`
with revision `d3e285b2`; the runner rejects it after the server-source change.
Docker, HCA and socket-based tests require the existing authorized execution
environment. Do not run heavy builds concurrently with measured trials.

The historical failed full comparison is
`dataset-workload/8cf0653d34f74e0c955f5ac5642d1316`: HTTP six trials and native
Rust singleton trials completed, then native multi-flight failed with a Chunk-KV
deadline; metadata replicas 1 and 2 exited 137 with `OOMKilled=true` at 2 GiB.
Those partial rates are not accepted comparative performance.

### Resumed verification

- Standard rootless `pixi run image-build` and image import pass from
  `0f5d94a8`, source tree `9299ad506bccb2c0080cd60970627a9a6717014e`.
  Image config is `sha256:74c2be3126997b3899d803dd64ba224019608d0a39a5ecc345f4ef1353b62da5`.
  The old handoff's `--privileged` command was inapplicable to the current
  non-root user; no source/build policy was changed to use the standard path.
- Complete unchanged workload failed in
  `dataset-workload/6581a65b0b9243be9b048b16b2caca71`. Three metadata containers
  remain capped at 2 GiB. Extra read-only topology/cgroup observations are
  retained as `retention-observations.jsonl` in that run directory.

- Run `6581a65b0b9243be9b048b16b2caca71` completed all six HTTP and three native
  Rust singleton trials. Native multi-flight failed with `chunk KV operation
  deadline elapsed`; all three metadata replicas were OOM-killed (exit 137).
  Acceptor reclamation advanced correctly (64,877 slots: trim 64,161, retained
  716), while tree live value bytes grew from 343 MB to 663 MB before failure.
  Cgroup observations show anonymous RSS plus substantial swap, not file cache.
  These partial rates are not accepted comparative performance.
- Second OOM root-cause fix is in progress: ordinary partitions never call
  checkpoint metadata reclamation, while each append publishes fresh copies
  of historical extent pages. Do not change the workload or resource limits.

- Recovery regression exposed a pre-existing WAL trim gap: the checkpoint's
  replay offset retains recent results but does not persist expired client
  floors. Automatic maintenance therefore reclaims only superseded extent
  directories; it does not invoke WAL/tree checkpoint reclamation. Recovery
  through an older root must still replay the complete required WAL and reject
  an expired request. Persisting retry-floor state is required before enabling
  automatic physical WAL trim, outside the metadata-memory fix.

- Focused regressions pass for bounded obsolete-page batches, old-checkpoint
  replay and expired-request rejection, concurrent exact-root pins, and active/
  boundary chunk retention. Nonzero physical WAL trim now fails explicitly until
  durable retry-floor metadata exists, instead of deleting recovery evidence.

- Verification passes for this metadata fix: all targets of `crowdb-chunk-stream`,
  `crowdb-chunk-kv`, and `crowdb-chunk-kv-server`; the added physical-WAL guard
  and modified trim/serving maintenance paths were rerun after final edits.
  Production KV batch deletion preserves the protected manifest generation.
  Workspace all-target/all-feature clippy, `rs-lint`, Rust fmt and diff checks
  pass. No C++ source, new dependency, lock or unsafe exception was introduced.
- Next: rebuild/import this source, then rerun the unchanged full workload.
  Metadata history cleanup is verified at component level; full-service memory
  and comparative performance remain unaccepted until that run completes.

- Metadata reclamation is committed as `1c9cc06e`. Standard rootless image build
  and import pass. Image config:
  `sha256:2991a6e21f6cb96f544aee0e6d8a3cd295878a7295dbe5aca2e1433a9f8e6a25`;
  source tree `0f7d29d911eb74e73f07acc116de305ddd0299ec`.
- Full unchanged workload failed in
  `dataset-workload/968559deb2cf49158eefc01a19939ca1`, with service error capture
  enabled. Supplemental read-only observations include cgroup RAM, swap and OOM
  counters plus retained slots and tree live-value summaries.
- This run completed HTTP six trials and native Rust singleton three trials;
  native multi-flight returned `publication lost its head race`. Metadata RSS
  still approached the 2-GiB limit, although extent cleanup kept live tree data
  near 1–2 MB and acceptor trim/retained counts advanced. Partial rates remain
  unaccepted comparative performance.
- Low-overhead sampled heap evidence is in diagnostic run
  `dataset-workload/f5bf584463564df8971885734f38fb51`. Live allocations grew from
  255 MB to 1.76 GB and were dominated by `PxKvStore::encode_kv_payload`. This
  diagnostic was stopped after locating the leak; it is not a performance run.
  An earlier full heap-profile run (`212aa76eed8c463d88e406d7faf67a4b`) distorted
  follower progress and must not be used to attribute normal RPC queue growth.
- Root cause: crossbeam-skiplist 0.1.3 `base::RefRange::{next,next_back}` replace
  cursor entries without releasing their prior explicit references. Logical
  removal succeeds while every scanned payload remains allocated. Acceptor
  reclamation/log-tip now use the full iterator, and journal range scans walk
  owned entries from `lower_bound`; both paths release cursor references.
  The owner-destructor regression previously released 0 of 8192 payloads with
  only one visible slot; it now passes while the acceptor remains live.
- Prepare and both accept quorum drains also retained completed tasks: matching
  `Some(reply)` disabled the exhausted stream branch while cancellation stayed
  pending. Explicit `None => return` releases completed drains immediately.
  The real two-voter RPC regression reproduced two leaked group references in
  both durable and early-ack modes before the fix and passes after it.
- All KV targets pass, including 53 Paxos and 127 group tests, election,
  concurrent readers/removers, RPC, store restart and WAL recovery. Workspace
  all-target/all-feature clippy, `rs-lint`, Rust fmt and diff checks pass.
  Rebuilt full-workload acceptance remains pending. No C++ source changes, new
  locks, unsafe exceptions, dependencies, resource limits, request retries or
  deadline changes.
- The payload/drain fix is committed as `b18efa9a`; rootless build/import pass
  with source tree `f47e633e7c2bc4b3fd519f9ef35e5131372a4200` and image config
  `sha256:134008ce6622c77355288d34ef8dff7046b46b329ae5484658604d1d392d950f`.
- Full unchanged run `dataset-workload/c7fd408b216a451e81feb9009e2eeac1` again
  completes HTTP six trials and native Rust singleton three trials, then fails
  native multi-flight with `publication lost its head race`. This time all
  metadata replicas remain alive: observed memory stays below 280 MiB through
  70,000 log slots, without swap or OOM. The CAS exhaustion is independent of
  the fixed payload leak. Full performance/restart acceptance remains pending.
- Diagnostic run `dataset-workload/0df852363785426086b924ad6c744a4d` retains
  per-attempt native lease-conflict revision/latency histories. Its native
  library has temporary logging while the frozen server keeps the identical
  lease algorithm; it is diagnostic evidence, not a source-matched acceptance.
- The 64 failed native CAS operations took 69–454 ms each (median 126 ms).
  The existing 32-ms RTT clamp and 256-ms maximum window understate durable
  sequencer latency. The fourth contention attempt uses actual RTT and full-width
  random jitter, preserving 64 attempts and all caller deadlines.
- A paused-time diagnostic with three readers and unbounded detached releases
  still fails after that change: cleanup can accumulate beyond client request
  concurrency. That model omitted the SDK's five-second cleanup timeout and is
  not a bounded production lifecycle. Its failure is retained here, not counted
  as a passing regression. The authority regression now covers the proposed
  bounded acquire/read/release lifecycle; the unchanged service workload remains
  the acceptance gate.
- The fifth contention correction makes native verified EOF await the existing
  bounded cleanup task. Cancel/drop remains prompt but the task retains request
  admission until completion/timeout. Dropped EOF waits reuse the same task;
  no new lock, caller retry, TTL/deadline increase, or workload reduction.
  Native regressions cover admission during cleanup, cancellation/drop, one
  release across interrupted EOF waits, timeout fallback and retained buffers.
  All six native lifecycle/read tests pass. The slow authority regression
  initially passed alone but failed in the component suite and again alone;
  this is not accepted contention evidence.

## Approved deletion redesign

- User update 2026-10-25: new deletion marks use a twenty-minute grace period.
  Existing persisted absolute deadlines remain fixed across retries and restart.
  Earlier five-minute observations below are historical verification records.

- The user replaced active-read GC protection with durable deletion admission and
  a five-minute grace deadline. Remove per-reader activity CAS, renewal tasks and
  asynchronous release writes; reads may fail if reclamation interrupts them.
- Implementation and Access Server coordination cleanup inventory are tracked in
  [Access simplification](plan-access-simplification.md). Historical contention
  experiments below are not acceptance requirements for the removed lease model.
- Preserve existing mixed-size workload, HTTP/native parity, memory, publication,
  shared payload ownership and local buffer/admission tests. Same-source workload
  and final HCA acceptance are still required before closing the SDK requirement.

## Prior contention investigation (superseded)

- The old model updated one snapshot-wide reader record on admission, renewal
  and release. Returned CAS observations avoid extra GETs but do not prevent
  independent readers from repeatedly invalidating each other's base.
- The 96-reader test exhausted the 64-attempt bound with
  `reader 32, release=false: publication lost its head race`. The slow durable
  model also failed `slow_durable_cas_completes_with_bounded_reader_lifetimes`.
  Jitter/RTT backoff and retaining local request credit through cleanup did not
  establish distributed fairness. The slow model is diagnostic, not a faithful
  production sequencer or accepted end-to-end result.
- Preserve experiment stash `a6e19f404d9cb614a3004a09575051196d6d0727` as historical
  evidence. Do not apply it to the replacement design: its lease model and tests
  are obsolete. No queue or independent-reader lease replacement was selected.
- The replacement concurrency test runs 96 readers with 64 admission checks each
  and verifies zero persistent mutations. Full workload throughput remains an
  independent acceptance gate; do not infer TPS from this component test.

## Deletion redesign verification (2026-10-24)

- Implemented persistent deletion marks and five-minute grace, read-only
  admission, restartable background cleanup, and immediate local EOF/cancel
  credit return. New reads reject marked snapshots; already admitted bodies
  can finish, and unusually long reads may fail after reclamation.
- Shared Dataset, native SDK and Access Server all-target tests pass. Focused
  regressions cover duplicate marks/restart, legacy record decoding, cleanup
  boundaries, held buffers, corrupt-mark isolation and owner/index cleanup after
  metadata completion.
- Workspace all-target/all-feature clippy, standard Rust lint, independent SDK
  tests/lint and dependency-boundary checks pass. Environment-gated hardware or
  installed-Python cases are not current-source hardware acceptance.
- Next: rebuild/import the same source and rerun unchanged mixed-size workload,
  then final real-service restart/local-HCA acceptance. R220 stays open.

## Bounded read and upload follow-up (2026-10-25)

- Source `d48a77d9` includes three independent changes: Snapshot cleanup
  retires an identical upload reference once per window, Split query planning
  verifies a consulted membership page once per bounded candidate window, and
  Iceberg fallback uploads hand off complete target buffers without merging.
- Executable cost regressions pass: ten shared references use one upload-record
  read and one CAS instead of ten reads and one CAS; twenty candidates in one
  Split page use one bounded metadata read instead of twenty. These are logical
  store operations, not measured routed frame counts or TPS gains.
- The receiver retains complete `Bytes` owners; fragmented input and short
  tails still merge. Original frame grouping, admission, digests, bounds,
  checkpoint and deletion rules remain. No persistent process state is added.
- All 645 affected Dataset/Access Server/native tests pass, with 13 separately
  configured cases ignored. Workspace all-target/all-feature clippy, normal
  lint and formatting pass. Matching-image acceptance now passes: all 16
  Iceberg file cases, all five Dataset cases, and the unchanged full TCP/restart
  workload. The earlier v4 results remain evidence for their own source.

## Point, expiry and inline follow-up (2026-10-25)

- Committed changes retain the original point-operation buffers through routed
  retries (`671c6186`), reuse freshly read multipart authority for expiry abort
  (`5476926a`), and reuse inline integrity already checked by locator resolution
  (`5eac9ecc`). Exact conditions, stable logical identities/deadlines, returned
  observations, public abort reads, Chunk digests and owner lifetimes remain.
- Pointer regressions demonstrate the removed deep clone of a CAS with two
  245,784-byte buffers on every dispatch. Expired refresh costs one GET plus one
  CAS instead of two GETs plus one CAS. Native inline corruption tests still
  reject payload and metadata mismatches before returning a body. These are
  boundary-work and logical-operation results, not measured throughput gains.
- A separate GC case (`6b731ae8`) seeds 128 inline files with a real expired
  absolute deletion deadline. It requires exact task/candidate completion and
  absent original file keys while finite SDK and chunk-write traffic succeeds.
  The original overlap case, twenty-minute grace and production budgets stay.
  New-case timeout is 3,300 seconds, with a 3,600-second fixture allowance; this
  bounds observation without changing the original 180-second case. It measures
  logical inline metadata cleanup, not physical storage capacity recovery.
- The six affected crate gates pass: 1,109 tests, 13 separately configured cases
  ignored. Workspace all-target/all-feature clippy, normal lint and formatting
  pass. The E2E client builds; the new GC case and matching-image S3/Dataset/
  complete TCP/restart acceptance still need execution on v6. Earlier v5
  evidence below remains tied to its own source. HCA prerequisites and pending
  Namespace DROP/MPU concurrency choices remain independently open.

## Current acceptance (2026-10-25)

- Image `access-cleanup-v5-20261025` from source `ae4e1b8a`, tree
  `652168eeca88342b04068ddbf32a09ebb08f41dc`, passes 21 affected real-service
  cases and the unchanged complete TCP workload in
  `dataset-workload/def69a711d4e468880c5cb9fae82e32e`: 18 trials, 4,608 requests,
  zero errors. File cases are in `container-e2e/run-f2d2bfc503214a77bae216d1cc8942f6`;
  Dataset cases are in `container-e2e/run-23ba53d4761b494781c86b799899ad17`.
  Installed native Python
  semantics, HTTP/native Split parity, cross-runtime and storage-restart cursor
  replay, held buffers and explicit Split deletion all pass. Metadata replicas
  peak at about 205 MiB under the unchanged 2-GiB cap. Exact identity,
  measurements and trade-offs are in the
  [Access acceptance record](../../app/crowdb-access-server/tests/README.md#dataset-httpnative-comparison).
  The image, Git bundle, raw logs and summaries are backed up outside runtime
  and build directories in `/tmp/crowdb-access-cleanup-v5-evidence-20261025`
  and the separate image archive. Current-source HCA acceptance remains before
  requirement cleanup; host prerequisites are still missing.
- Complete Split revisions now have an explicit ID-based DELETE in HTTP,
  native Rust, both Python bindings and the portable SDK. Current and initial
  revisions remain protected. Deletion closes the resource, fences concurrent
  publication and preserves a fixed twenty-minute grace across retries and
  restarts; bounded background discovery retires metadata without deleting
  sample payloads. No request ledger or deletion task is added.
- Focused Split deletion evidence passes: 19 new core cases, three native
  facade cases, four real HTTP cases, five portable SDK cases and 21 installed
  portable Python cases. Workspace all-target/all-feature clippy, standard Rust
  lint, formatting, SDK dependency boundaries and C++ tree lint pass. The v2
  OpenAPI document parses with all 298 local references resolved. These are
  component gates; the matching-image TCP/restart result is recorded above.
- The affected five-crate all-target/all-feature regression coverage passes
  across the full runs and corrected HTTP targets: 1,644 tests passed and 13
  were ignored for separate environment or measurement prerequisites. The old HTTP
  receipt expectations now follow the approved stateless Namespace CREATE and
  table DELETE contracts; Namespace DROP retains its existing recreated-name
  protection pending a separate decision. No production behavior was changed
  to satisfy those historical assertions.
- The shared native-Python semantic consumer now prepares an independent
  complete revision, marks it deleted, checks identical retry deadlines, then
  repeats the same two-ID DELETE after storage restart. This adds a semantic
  check without changing the 18 performance trials, sizes, concurrency or
  resource budgets. Its current-source runtime and storage-restart checks pass.
- New Snapshot resources now store their fixed internal `gc_after` in the first
  resource write. Bounded direct Snapshot scanning covers interruption before
  the content binding; legacy resources inherit an existing deadline or receive
  one fixed background deadline. No foreground discovery RPC or task is added.
- Dataset regression gate: 317 tests passed, including the twenty-minute grace,
  old absolute-deadline compatibility, interrupted first Manifest write, late
  writes after retirement, legacy deadline migration, unknown-outcome stopping,
  versioned-record rejection and large-resource scan paging. All-target,
  all-feature Dataset clippy passed. The Access HTTP retirement target passed
  all three cases. The matching-image TCP/restart acceptance now passes;
  current-source HCA acceptance remains outstanding.

- Subsequent source `be5eca5c` passes the affected 823 Iceberg tests, 201 Access
  tests (11 ignored), workspace all-feature clippy, normal lint, formatting and
  163 fixture unit tests. Its matching `access-cleanup-v3-20261025` image passes
  all 12 affected GC/namespace/S3 container cases. GC now continues bounded
  backlog windows instead of waiting the full idle interval after every step.
- The subsequent unchanged full TCP workload outputs all 18 trials / 4,608
  successful reads, but does not complete semantic/restart acceptance. While its
  installed Python consumer runs, `.crowdb-runtime` and `target` disappear;
  the consumer exits 1 and the runner fails to read its deleted stderr or write
  the final manifest. The deletion source is unconfirmed. All original runtime
  artifact paths below are consequently historical identifiers, not files still
  available for inspection. Source commits and bundle backups are intact.
  Recovered trial JSON, GC observations and console logs are saved outside those
  directories in `/tmp/crowdb-access-continuation-evidence-20261025`.
  The imported image remains available. Keep real-service acceptance open and
  rerun after shared-directory cleanup stops; do not infer semantic success from
  the completed performance trials. The user has been asked about concurrent cleanup.
- The final seven-crate Access/Dataset/Iceberg/S3/native/ChunkClient/Chunk-KV
  all-target gate at `7b2c61aa` passes: 1,699 tests, zero failures, four ignored cases.
  Workspace all-target/all-feature clippy, standard Rust lint, formatting and
  SDK dependency boundaries pass. Official Java/PyIceberg SDK acceptance passes
  all eight cases. These results do not substitute for current-image acceptance.
- Dataset partial TCP read failure and accepted-read cancellation pass through
  the production facade and DiskIO RPC, with in-process metadata/layout fixtures.
  Delivered owners survive terminal failure and shutdown; local admission and
  held charges drain independently. TCP preserves its three bounded target
  attempts, while subsequent sticky pulls issue no RPC. The explicit RDMA case
  remains unactivated until the HCA environment is restored.
- The semantic consumer now compares HTTP/native ReadSpec and Split batches,
  replays the original cursor on another runtime, and persists an original
  Split cursor/header/digests for storage-restart verification. Mixed-size
  performance sizes, concurrency, rounds, budgets and deadlines are unchanged.
- The host HCA is active, but its test address and local-loop settings were
  reset. Restoring them requires a sudo password unavailable to the agent.
  The user has been asked to run the existing local setup command. Continue
  image, TCP workload and container acceptance independently; do not count
  environment-gated unit cases as actual HCA evidence.
- The earlier image build/import and unchanged complete TCP workload pass
  in `dataset-workload/9dbf56c386e142e995b8eb400305b467`, including installed
  Python semantics, HTTP/native Split parity, cross-runtime cursor replay,
  held buffers and storage-restart replay. All 4,608 measured requests succeed;
  its original image/artifacts remain available as earlier evidence.
- Dataset container acceptance on that earlier image passes in
  `container-e2e/run-4e53cd0385824d9b8b5d6218655a02cb`.
- The rebuilt `crowdb-node:access-cleanup-20261025` image is from `7b2c61aa`,
  source tree `aecf740ce52381d5b90843921b21608f494db30e`, imported Docker ID
  `sha256:43e6d9e595055b506c664d3dcca018a9b62c7cb64d56bfdb286da695b97d5a14`.
  All 16 affected Iceberg file cases and four GC cases pass in
  `container-e2e/run-7182e79d73e24569b9df7900ba084315` and
  `container-e2e/run-394ea52271a84a8a8ed05d2df7de1188`, including both prior
  failures. Six affected S3 cases pass; all five Dataset container cases pass
  in `container-e2e/run-b5f5b8ddadcb45a0bec8c698a783d3ba`.
- The unchanged full TCP workload passes on the rebuilt image in
  `dataset-workload/abea0a0568824233a32ba85a063e522a`: 18 trials, 4,608 completed
  requests, zero errors, installed Python semantics, HTTP/native Split parity,
  cross-runtime and storage-restart cursor replay, held buffers and cancellation.
  Metadata replicas peak at approximately 205 MiB each under the unchanged
  2-GiB cap. The [Access acceptance record](../../app/crowdb-access-server/tests/README.md#dataset-httpnative-comparison)
  retains exact identity and trade-offs: native Rust throughput is about 20%
  higher at concurrency one and 4% higher at three; memory and multi-flight CPU
  are higher, and some small-request p50 values are slower. This is not an
  isolated before/after performance claim for the cleanup patches.
- The bounded immutable write comparison passes in
  `write-batch-comparison/acd38136ec0943aeac3aaa9d849c0a5c`. Both profiles are
  slower with the current routed batch despite fewer owner RPCs and use more
  protocol bytes; retain bounded point concurrency. The cold warmup handshake
  accounting fix preserves exact frame equality and forbids new connections in
  measured trials. Applicable fmt/clippy and the real fixture pass.
- Local HCA remains outstanding because the address/local-loop prerequisites
  have not been restored. Keep this plan, the requirement and its backlog entry
  until the applicable gates actually pass.
