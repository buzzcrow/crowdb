<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Two Physical Node Test Track

Run ID: `two-physical-nodes-20261030T010205Z`.
Latest fixture: `physical-20261030-r17`; result: **five-container recovery matrix
passed across two physical hosts**, including all five replicas' acknowledged
Group-0/1/3 checkpoints. All owned physical fixtures are cleaned up and key-only
SSH works. First publication is `25059cf7d`; the follow-up CI repairs pass local
verification and are prepared for the authorized second publication. Historical
CI runs remain failed; the next published commit requires its own remote result.

The separate 512-record/default-five-second capacity probe failed; 294 confirmed
records are exact recovery witnesses, not a capacity pass. User disk cleanup
deleted the earlier local archives, including attempt 15 forensic files. Later
sections describe that loss and the surviving/reconstructed evidence explicitly;
historical archive descriptions below refer to their state before that cleanup.

## Attempt 12 placement and image (historical)

- Host A: `u24-amd`, `192.168.31.129`, two logical voters, offsets `0/1`.
- Host B: `cj7960`, `192.168.31.184`, one logical voter, offset `0`.
- Cluster ID: `475d2a73-19f4-421e-8ccf-39693ef368c2`.
- Host networking, separate retained volumes, one CPU and 2 GiB per node.
- Final OCI digest:
  `sha256:105cd3d3fccad16b1e907e3ba7a7f8cad327319a9f678a4815cdd09921a9b080`.
- Build revision: `5d03757b75a9b1867eebbcab9346e84d153bdcf2`; source snapshot:
  `b1a845605cd9f41136a67e81a91f51119912639b`.
- Both hosts imported the same archive. Execution configuration and ordered
  rootfs layers were verified, including across Docker 29/28 import formats.
  The receipt records the production sources at build time; the allocation
  retry tests and this report were added afterward.

## Attempt 12 verified observations (historical)

- [x] Host inventory, direct reachability, and distinct host-network listeners.
- [x] Host-B `cj` key-only SSH using `/home/cj/.ssh/id_ed25519` with
  `BatchMode=yes` and `IdentitiesOnly=yes`; public key installed without
  removing existing authorized keys. SSH directory/file modes verified.
- [x] All discovery candidates admitted with `ssh_password = null`.
- [x] Three-voter Group 0 active across the two physical hosts.
- [x] All seven services healthy on every node after production disk setup.
- [x] KV put/get/batch, 96 KiB S3 object exact-byte reads, Iceberg namespace,
  table and property round trip, and initial small mirrored allocation.
- [x] All three containers recreated from retained volumes with the corrected
  image, original identities retained, and no password-file environment.
- [x] Managed Chunk-KV listener fault: `SIGSTOP` on the owned child, bounded
  monitor replacement with a different PID, all services healthy, then
  KV/S3/Iceberg reads through all three endpoints.
- [x] With host B stopped, a Group-3 KV write and read succeeded through host A.
- [x] After host B returned, existing KV/S3/Iceberg data read correctly from
  every endpoint. All seven services had stable PIDs across the next health
  pass; a final native KV batch also passed.
- [x] Focused RPC tests prove a known allocation `Unavailable` retries with the
  same chunk ID, while a lost allocation reply returns `OutcomeUnknown` and
  submits exactly once. These are separate from physical fault coverage.
- [x] Full final service logs frozen after container stop, inspect/status and
  policy snapshots retained, and exact owned containers/volumes removed.
  All owned endpoints closed; the host SSH key remains installed.

## Attempt 12 failures and coverage limits (historical)

These failures are retained as first-divergence evidence. Attempt 16 below
passed the repaired storage failover, fresh allocation, physical safe-retry
and expected majority-host-loss cases.

- [ ] **Host-B storage availability:** the first S3 read while B was stopped
  returned `ServiceUnavailable` with caller retries disabled. A subsequent
  observation-only wait also failed: after the existing 180-second bound,
  catalog generation 14 still assigned B's range to owner 3. A persisted
  `TargetPreparing` transfer from owner 2 to owner 1 reserved both surviving
  owners. That explains why the current planner had no eligible target; the
  reason preparation remained unfinished required diagnosis. Returning B
  restored corpus reads. Availability during B's absence did not pass.
- [ ] **Final allocation:** a fresh native allocation after restored health
  failed with `diskdb allocate failed for disk_group 2: diskdb server does not
  own the requested resource: disk-group not owned`. Listener health and old
  data reads therefore do not establish restored allocation readiness.
- [ ] **Physical allocation-only retry:** a bounded pause of the two follower
  KV processes was followed by successful allocation and query, but the
  instrumented client observed no allocation-level safe retry. The physical
  case does not prove that specific path. Retry limits and caller deadlines
  were unchanged. Pausing only one of three voters cannot remove quorum.
- [ ] **Final-image two-voter host-loss repeat:** not run after the preceding
  recovery failure. The expected quorum-loss observation passed in attempt 11
  on its earlier image and is retained as historical evidence only.

## Repairs validated during continuation

- Admission now publishes the target's accepted, offset-resolved setup choices;
  omitted choices previously published identical KV endpoints for every node.
- Standard DiskDB policy uses independent main/HTTP/RPC blocks
  `11000/11100/11200`, each shifted once. Adjacent offset nodes previously
  collided because the RPC field was interpreted as a legacy three-port base.
  Explicit listeners preserve legacy custom-base deployment behavior.
- Access startup probes every catalog owner before another process launch;
  a healthy owner previously masked another owner replaying its WAL and
  consumed the restart budget. Tests cover redirects, multiple healthy owners
  and an unavailable second owner. The physical listener fault then passed.

## Evidence

Root: `.crowdb-runtime/artifacts/two-physical-nodes-20261030T010205Z/`.

- `attempt10-local-fixed/`: local offsets `0/1/2`, active Group 0; no storage
  readiness claim for that diskless fixture.
- `attempt11-final-physical/`: earlier mixed-placement KV/quorum/recreation
  results, expected two-voter loss, and cleanup. Its storage phase failed.
- `attempt12/result.json`: historical case classification and next unrun work.
- `attempt12/catalog-all-owners-fix-receipt.json`: final image receipt.
- `attempt12/baseline-catalog-all-owners-fix/`, `listener-fixed/`:
  corrected-image baseline and successful managed recovery evidence.
- `attempt12/remote-stop-{catalog,transfer,split}.json`,
  `matrix-failure-remote/`: failover diagnostics and durable transitions.
- `attempt12/native-final-allocation.json`, `final-allocation-failure/`:
  final allocation error and complete frozen service logs.
- `attempt12/corpus-read.json`, `final-health-stable.json`, `cleanup.json`:
  restored reads, stable process observations, and verified cleanup.
- Reproduction scripts are retained in `attempt12/`; client-only retry logging
  lived under `/tmp` and did not change production client sources.

## Local verification

- Passed: full `crowdb-protocol` and `crowdb-web` Rust tests.
- Passed: monitor policy/dependencies/readiness, 15 tests; ChunkDB allocation
  and dynamic retry, five tests.
- Passed: clippy for protocol, monitor, Web and ChunkDB with tests and
  `-D warnings`; Rust fmt and `git diff --check`.
- Earlier unchanged launcher verification: 134 OCI Python tests and shell
  syntax passed. The final production image build/import passed.
- Default `ssh -o BatchMode=yes cj@192.168.31.184` also succeeded without an
  explicit identity flag after fixture cleanup.

## Attempt 13 diagnostics (historical)

- User requested continued repair and full physical validation before inspecting
  the eight linked CI jobs.
- Corrected stale DiskDB ownership rejection routing and released-source
  Chunk-KV grant/WAL recovery; focused regressions and affected clippy pass.
- OCI digest: `sha256:3b21576d0bb18aec223bdb4cd4916b6400c76d3b1cf74a883afa90eb588d78f7`;
  source snapshot `8d0e21fb044c27b8dd0e9bcb98cf4de1f808b7fd`.
- New owned fixture `physical-20261030-r13` reached active three-voter Group 0
  and seven healthy services on every node. Baseline KV/S3/Iceberg reads pass.
  Retained-volume recreation and the remaining matrix are in progress.
- The corpus harness now waits for a newly created data group's elected leader
  before its first write; the initial immediate write encountered election
  startup. This adds a setup assertion, not a retry of a data operation.
- Evidence is retained in `attempt13/`; attempt-12 failures remain preserved.

- Attempt 13 retained-volume recreation and listener recovery passed. The
  isolated quorum case exposed a stale KV route to a removed replica: the
  missing-local-group response did not trigger discovery. Regression first
  failed, then passed for point reads/writes, conditional writes and scans;
  the 65-test KV store suite also passed. Missing-group responses now carry
  the existing typed NotLeader status, with legacy point-response fallback.
- The temporary two-voter metadata fixture retired its removed replica's local
  creation record; it is not reused for the later three-voter recovery cases.
  Its logs, membership errors and cleanup evidence remain in `attempt13/`.

## Attempt 14 diagnostics

- The isolated Group-1 outage also affected DiskDB metadata and therefore
  produced terminal disk-allocation errors before an allocation-only retry.
  These are failed observations, not retry coverage. The removed replica is
  retired and the invalid two-voter production fixture is not reused.
- Recovery publication incorrectly applied the observer's local KV management
  and RPC ports to every member. Binding and service receipts showed duplicated
  Host-A seeds and an incorrect offset on Host B. A mixed-port/same-host/IPv6
  regression reproduced this, then passed after retaining each member's ports.
- All 13 monitor member-trust tests and monitor clippy pass. Attempt-14 owned
  containers, volumes and transfer archives were removed; frozen logs remain.
- User authorized the full physical and CI repair commit/push, followed by a
  separate five-container test on these two hosts and another commit/push.

## Attempt 15 continuation

- OCI `sha256:5a9495fc919858e5c4aebc707c35115dce769aeb3646dba88d83c489660a7e43`,
  source snapshot `41c9b797a28641a2408d3fa3562805d754f25a7e`, passed baseline,
  all-node retained-volume recreation, and managed listener recovery.
- Every binding retained the same correct three mixed-port management seeds.
- Allocation-only retry passed with the original five-retry/10-second client
  limits: concurrent Group-1-only step-downs left all three voters and Group 0
  live, produced a known `Unavailable` response, and then allocation/query
  succeeded for the same chunk identity. This validates transient leadership
  unavailability; it is not a claim of physically pausing two KV processes.
- Host-B-stop catalog recovery failed within the existing 180-second bound.
  A target rejected a sealed, physically empty WAL chunk whose strips had
  legitimately been returned by `trim_sealed_strips`. The authoritative chunk
  had zero capacity, sealed length and acknowledged cursor. Recovery incorrectly
  required at least one mirror strip. B was restored in the fixture's finally
  block, and complete diagnostics remain under `attempt15/matrix-failure-remote`.
- A regression reproduces the empty sealed tail and checks active, nonempty,
  wrong-purpose and stale-snapshot rejection. The 47-test production chunk
  adapter suite passes after handling only this canonical empty sealed state.
  The existing physical fixture will be upgraded with its volumes preserved.

- Upgrading the retained fixture to OCI
  `sha256:0f9326c2f4b08b8e069983b9348efad7dea88a2e275392dde0c6c7d189828ae9`
  resolved the previously stuck empty-tail transfer; all services and corpus
  reads recovered, and listener recovery passed again.
- The next Host-B-stop run exposed missing repair admission on a failed old-WAL
  drain. The referenced active chunk remained at writer epoch 4, with no new
  epoch-5 allocation: failure occurred before successor allocation. The
  recovery path returned on an offline mirror without feeding the existing
  durable repair queue. The fix drains every mirror, conditionally marks failed
  segments unavailable only when a drained survivor exists, and keeps recovery
  unavailable until the replacement layout itself drains successfully. It does
  not mark transport failures as corruption or omit failed drains from success.
- The ChunkDB crash-gap regression confirms unavailable layout records are
  admitted by its repair scanner. Focused recovery tests cover lost metadata
  replies and the absence of any drained survivor.

- All 49 production adapter tests pass, including durable failure-report reply
  loss and all-mirror drain failure. The existing recovery IO deadline remains
  unchanged. The repaired physical matrix remains the next required gate.

## Attempt 15: reclaimed journal exposed duplicate physical allocation

- A1 Chunk-KV recovery rejected tree `4789280886995677209`: both mirrors of
  chunk `216631886962935262:3939770405816213384` contained zero bytes in the
  first 262284 bytes, including the frame at offset 65606.
- Its sealed tree metadata still referenced A2 disk unit 318 and A1 disk unit
  321. Current DiskDB busy records assigned both units to newer WAL chunks.
  Allocation timestamps and chunk records are retained in
  `attempt15/allocation-records.log` and `overlapping-chunks.log`; both raw
  mirror reads and complete service logs are retained with the fixture.
- A deterministic restart regression reproduced the upstream fault: a durable
  KV engine retained a busy record while earlier WAL segments were reclaimed;
  the restored acceptor still had trim watermark zero before maintenance.
  `JournalScan` returned success with empty history, so DiskDB recovered an
  incomplete occupancy bitmap and could reuse a live extent.
- `JournalScan` now requires every slot in the requested applied interval,
  including NoOps, before filtering by key. Missing history returns the existing
  typed GC-gap error, selecting DiskDB's full-scan fallback. The regression was
  observed failing before the fix and passing afterward. The full KV store
  suite passed 66 tests. A separate DiskDB integration regression verifies the
  old busy unit remains reserved and is excluded from subsequent allocations.
- This does not repair overwritten mirror contents. Attempt 15 remains a
  failed durability case; preserve its evidence and repeat the matrix with
  fresh owned volumes after rebuilding. No data-loss case is counted as a pass.
- The next fixture explicitly accepts ChunkDB dynamic ownership policy.
  Attempt 15's fixed ownership policy cannot establish availability while a
  ChunkDB owner is stopped; changing ownership mode requires a fresh accepted
  deployment rather than silently changing the existing cluster.

- The existing DiskDB recovery suite passed all eight tests, and KV/DiskDB
  clippy with tests and denied warnings passed. The failed fixture is stopped;
  its volumes remain intact, with frozen metadata/WAL and service-log archives
  for all three nodes under `attempt15/allocation-overlap-frozen/`.

## Attempt 16: explicit policy file and nonzero port offset

- The fresh dynamic-ownership fixture stopped at A2 startup: `deployment inputs
  differ from accepted node policy`. The preflight had sealed shifted service
  ports, but the following monitor invocation compared the unshifted file
  inputs against that accepted record. No Group-0 bootstrap or data corpus had
  started. A1/A2 volumes are retained for retry with the same accepted inputs.
- Port resolution now compares explicit file/argument listeners in the accepted
  port space and hands the complete resolved policy to preflight/runtime once.
  Repeated acceptance preserves policy bytes; a changed listener is rejected.
- The new repeated-file-acceptance regression failed before repair. All 20
  policy, CLI-input and management-input tests pass, as does monitor clippy
  with tests and denied warnings. `pixi run tree-lint` also completed with exit
  zero; its complete diagnostic output is retained with verification logs.
- First-start failure logs and the prior image receipt remain under
  `attempt16/before-policy-file-offset-fix/`; subsequent bootstrap has not yet
  been verified on the corrected image.

- Corrected attempt-16 image: OCI
  `sha256:faab2bd15b6abe37598caa9a78e8e6d75d38490559f2ae97144d05e1846aa8fc`,
  source snapshot `59bca7e66bddd0454c2a3121b57dd6219264e3c2`. Both hosts
  imported matching configuration/rootfs layers. All three execution records
  explicitly contain `CROWDB_CHUNKDB_OWNERSHIP_POLICY=dynamic`.
- All three nodes initialized and reached seven healthy services. KV batch/data,
  S3 exact-byte payload and Iceberg metadata reads passed through every node.
- All containers recreated with their retained volumes and identities; all
  services and three-endpoint data reads passed again. Native scans found no
  cross-chunk physical overlap among 134 live mirrored segments before
  recreation or 238 afterward.
- Managed Chunk-KV `SIGSTOP` recovery completed inside the existing 60-second
  bound with a changed PID, followed by all-service health and exact corpus
  reads on every endpoint. Host-B stop/recovery is the next active case.

- Host B stopped: Group-3 KV quorum write/read passed, all catalog entries
  returned to surviving owners within the original bound, and one-shot S3 and
  Iceberg corpus reads passed through both A endpoints while B stayed stopped.
  B then returned and was recreated from its volume: all services and exact
  reads passed on all three endpoints. This supersedes the earlier failed
  remote-availability observations for this corrected dynamic-policy image.

- The first host-A loss case confirmed quorum loss and rejection of a write.
  Its recovery assertion then failed with S3 connection refused because the
  harness read a retained pre-stop `monitor.json` as current readiness.
  Failure capture showed A1/A2's new snapshots had no Access entry yet, while
  the earlier readiness observations had already accepted the old seven-service
  snapshot. The same processes later became ready without another intervention.
- The physical harness now requires status timestamps to follow the current
  Docker container start and be no older than the existing three-second status
  freshness contract. Failure capture still retains raw stale evidence. The
  original 180-second recovery bound and one-shot corpus reads are unchanged;
  repeat this host-loss case with the corrected assertion before advancing.

- Repeating the host-A two-voter loss with current-status checks passed:
  Group 0 became unavailable, a write could not complete during quorum loss,
  and all seven services plus exact KV/S3/Iceberg reads recovered at all three
  endpoints. The timed-out write has an unknown eventual outcome; this case
  asserts unavailable write completion during the outage, not nonexistence of
  its key after quorum returns.
- Group-1-only rapid leadership step-downs reproduced the explicit native
  allocation `Unavailable` retry (`retries exhausted after 11 attempts, last
  error: not leader`). Releasing the fault completed same-ID allocation/query
  within the unchanged client bounds, with all three metadata voters retained.
  All service checks and three-endpoint corpus reads passed afterward.

- Final attempt-16 gate passed: every catalog partition Serving, all seven
  services per node healthy with stable PIDs across the next health pass, a
  fresh native KV batch and fresh allocation/query, plus exact corpus reads
  through all three endpoints. A final native metadata scan found no
  cross-chunk overlap among 2432 live mirrored segments.
- Attempt-16 and the quarantined attempt-15 owned containers/volumes were then
  removed after freezing final service logs. Exact run labels were checked;
  all owned endpoints closed, transfer/policy files were removed, and the host
  SSH key was retained. Cleanup receipts are stored in each attempt directory.

## Requested CI continuation

- All eight requested job logs are retained under `ci/run-41003611925/`.
  The original run used commit `39440d023d7fb162dd61c377e8eb17eefa8c4c9a`.
- AccessTests: retained Monitor startup/resume/cleanup fixtures now seal accepted
  policy before identity effects. Exact failures, full Monitor tests, component
  Access tests, official SDK tests, Dataset SDK boundary/lint/tests and Python
  installed-consumer checks pass. Python coverage includes 21 consumer cases
  and the real HTTP Split SDK round trip. Records:
  `ci/ci-dataset-sdk-results.json`, `ci/ci-access-tests.log`,
  `ci/ci-access-sdk.log`.
- Console/deployment bootstrap: diskless fixtures incorrectly disabled mandatory
  Paxos KV, causing node exit followed by `No route to host`. Shared fixtures
  now retain explicit Group-0 KV listeners and disable ordinary storage setup.
- Monitor restart decisions now use liveness. Normal ChunkKV transfer readiness
  withdrawal previously caused responsive children to be replaced. The
  regression preserves responsive PIDs beyond the failure threshold and still
  requires replacement for a genuinely unhealthy listener. Published status,
  startup dependencies and admission retain readiness checks.
- KV-only diskless setup must not reserve default Group 1. Dataset browse
  reproduced the race (`200` instead of absent-group `404`), and the setup
  regression reproduced `Running` instead of `Ready`. Preparation now excludes
  this mandatory prerequisite when storage is diskless. All 12 setup tests,
  full Monitor tests and clippy pass.
- S3: all 45 original cases pass with two slots and the original resource and
  deadline budgets, including official SDK injected cleanup, storage outages,
  process recovery and protocol parity. Artifact:
  `container-e2e/run-76afe2e0dfe24ac6a583002643fddd3d`; log: `ci/s3-final.log`.
  The original Go cleanup failure is unclassified: its artifact omitted the
  underlying error. Focused and full executions pass; safe operation/error-code
  diagnostics now preserve evidence if it recurs.
- Console browser: all 27 cases pass, including original failed replica
  navigation and logical creation. Artifact:
  `container-e2e/run-810e73b204924f97bd774df1c148dbc0`;
  log: `ci/console-browser-final.log`.
- Native data: all 14 original cases pass across the five Dataset cases,
  six-node round trips and ordered remainder. Original data assertions,
  balancing, process-preservation checks and deadlines remain. Records:
  `ci/remaining-sequence.json`, `ci/native-data-remainder-cases.json`.
- The candidate review helper now expects mandatory Paxos KV checked and the
  other five services unchecked; all fixed choices remain read-only. Six-node
  verification passes. Bridge default-policy assertions now use the accepted
  DiskDB RPC port 11200 and one 16-GiB zone. No visible UI behavior changed.
- Complete bridge admission passes SSH proof, candidate review, foreign-cluster
  rejection, restart, service lifecycle, minority authority gating and default
  setup. It verifies retained 16-GiB disk identity after Monitor restart,
  bitmap/capacity properties and no OOM/PID exhaustion. Artifact:
  `container-e2e/run-29a8a32454d945c795c825e79084525b`;
  log: `ci/bridge-admission-contract-final.log`.
- Inspection passes the complete original production balancing/browser case
  in 329 seconds. Artifact:
  `container-e2e/run-4380b4daaa054bd1a528ac73ffafbb01`;
  log: `ci/console-inspection-final.log`.
- Native operations exposed a remote-leader routing defect. Stopping a local
  follower left the remote leader healthy with a valid lease, but restart
  returned 502 after requests to `127.0.0.1:10100`. HTTP topology ingestion now
  resolves wildcard store listeners against the reporting management origin
  before caching. Explicit peer endpoints remain intact. The regression fails
  before the fix and passes afterward, covering IPv4/IPv6 and the cached
  Group-0 leader. Full Console shared, Web and Monitor tests/clippy pass.
  The exact lifecycle case passes in 8.8 seconds with original assertions:
  `container-e2e/run-4315916e556445948d33173a097cfede`;
  log: `ci/ci-diskdb-kv-origin-fixed.log`.
- Reference launch preflight exposed stale container protocol mappings. Fresh
  Docker/Pixi/CLI/Compose launches now publish S3 16000, Iceberg 17000 and
  Dataset 18000 while retaining host-side port inputs. Version-2 launch
  receipts select canonical targets; version-1 receipts keep their immutable
  ports/image through resume and retained-volume recreation. Compose provides
  explicit legacy target overrides. Image exposed ports and examples agree.
  All 136 OCI tests, 14 reference-fixture tests, six CLI lifecycle tests and
  full CLI tests/clippy pass. The rebuilt image receipt is retained as
  `ci/protocol-ports-image-receipt.json`.
- On the rebuilt image, complete native operations passes all 19 original
  cases in 530 seconds, including typed restarts, retained bytes under load,
  catalog navigation, ownership, multipart and explicit binding repair.
  Artifact: `container-e2e/run-912e419cf5774ea197ec626fce31dbee`;
  log: `ci/ci-console-native-operations-closing.log`.
- Complete transition passes production split, journal inspection and final
  balanced Serving placement across three owners in 281 seconds. Artifact:
  `container-e2e/run-a06b30aecc5e4ba8953d59df60b463a5`;
  log: `ci/ci-console-transition-closing.log`.
- All six deployment-reference cases pass on the final image: Docker, Compose
  and editable UI for one/three nodes, plus real Pixi shortcuts and confirmed
  nonvoter cold recovery on new names/IPs. Original voter IDs, retained data,
  credentials and identities are preserved. Artifact:
  `container-e2e/run-554eaff75f0b4beb83d0dfdb59e3bb45`;
  log: `ci/ci-deployment-reference-closing.log`.
- Final-image Console browser again passes all 27 original cases in 291
  seconds. Artifact: `container-e2e/run-5d6bcf681bda4e88998e34dd06bb9e47`;
  log: `ci/ci-console-browser-closing.log`.
- Final-image native data passes all 14 original cases in one complete run,
  including all five Dataset cases, six-node protocol round trips, topology,
  retry, geometry, hardware and maintenance. Artifact:
  `container-e2e/run-c831fca5df334574a14035c11c89ae54`;
  log: `ci/ci-console-native-data-closing.log`.
- Final-image inspection passes the complete original case in 317 seconds,
  including production balancing, real page layout and Iceberg inspection.
  Artifact: `container-e2e/run-eac3d6766dab43809a0eee22414435e0`;
  log: `ci/ci-console-inspection-closing.log`.
- All eight requested CI job selections now pass locally. Ordered closing
  commands, image identity and complete results are in `ci/closing-sequence.json`;
  per-job classification is in `ci/requested-job-results.json`. Remote CI for
  the new commits is observed after publication.
- Quality: affected all-target clippy with warnings denied, workspace Rust
  formatting, launcher shell syntax and C++ tree-lint pass. No C++ source
  changed. Both bridge fixtures completed resource cleanup; their six owned
  16-GiB temporary payload files were retired after matching inode/size receipts.
  Logs, metadata, disk headers and exact retirement records remain under
  `ci/retired-bridge-disks-*`. Attempt-15 forensic archives remain intact.

## Authorized publication and five-container follow-up

- The first coherent commit/push includes the passed physical matrix, all eight
  requested local CI selections and relevant quality gates.
- After that push, a fresh five-container cluster will span the same two
  physical hosts (three containers on A, two on B). Preserve immutable image
  identity, key-only admission, exact data witnesses, retained-volume recovery,
  both host-loss cases and owned-resource cleanup.
- Verify repairs and commit/push the five-node changes and evidence summary
  a second time. The execution plan remains active until then.

## Five-container physical continuation

- First publication completed: commit
  `25059cf7d9a5f08d61479f4749659aa6f3fe119f` pushed to `task-fix-build`.
  PR #55 CI run `41128190276` completed: 22 jobs passed; five follow-up
  failures are recorded below and remain part of this task.
- Selected five initial voters across two physical hosts: A offsets `0/1/2`,
  B offsets `0/1`, with a separate volume and seven services per container.
- New regressions reproduced three entry restrictions: accepted voter count,
  default preparation membership and bound-member conflict handling. Policy now
  accepts only `1/3/5`; preparation maps every admitted voter to its own initial
  storage binding; all multi-node scopes retain the existing formation fences.
  Policy, all nine preparation tests and all nine formation tests pass.
- Full affected tests/clippy and a fresh image precede physical deployment.
  The physical matrix will include 512 exact 64-KiB ChunkKV records, all five
  serving owners, cross-host KV/S3/Iceberg reads, retained-volume recreation,
  unresponsive-child recovery, remote-leader KV stop/start, both host-loss cases,
  bounded allocation retry and a final no-overlapping-allocation scan.

- Full affected Protocol, Console shared, Web and Monitor tests, all-target
  clippy and workspace formatting pass. One Monitor CLI test retained the old
  invalid-voter error text; it now asserts the `1/3/5` contract and retains all
  no-identity/no-storage side-effect checks.
- The first remote CI run also exposed `g3_leader_change::leader_change_simulation`:
  the second one-shot write returned failure. Its original artifact omitted the
  response, so the precise original reply remains unknown. The focused case,
  a complete 131-case group run and twenty logged repetitions pass before the
  helper repair. A deterministic regression then proves that the fixture's
  role-only wait can return before a current quorum lease exists. The helper
  now waits within the same three-second bound for current proposing tenure
  and quorum lease; writes remain one-shot and report the complete response on
  failure. The three affected cases, complete `test-core` gate and KV clippy
  pass. Production Paxos logic is unchanged. Evidence:
  `ci/run-41128190276/core-verification.json`, `ci-leader-ready-{red,green}.log`.


### Attempt 17: five voters on two physical hosts

- Immutable image `0ef175e40bef7a152fda9abff2929f41df3830d39bedf555f04de53776dcab51`
  is verified on both hosts. All five Group-0/default-metadata/manual-data voters,
  35 service health observations and KV/S3/Iceberg witnesses pass initially.
- The additional 512-record capacity case failed its original five-second batch
  deadline during split/cutover. A full read-only audit found records 0–293 exact
  (19,267,584 bytes), with 294–511 absent. No timed-out mutation was replayed.
  Preserve this failed capacity case separately from the 294-record recovery
  witness. No process OOM or PID replacement occurred during the capacity failure.
- All-five retained-volume recreation passes with exact identities and witness
  bytes. SIGSTOP ChunkKV is replaced within 48.3 seconds; the other 34 PIDs and
  all witnesses are retained. Cross-host local-KV stop/start passes after fixing
  the harness to accept an omitted stopped-service row as well as a null PID.
- Stopping both B containers preserves the manual-data KV quorum write, but
  catalog recovery fails its original 180-second bound. WAL recovery drains
  schedule repair, then new active WAL initialization still selects stopped
  physical IO targets. This remains a failed case until repaired and rerun.
- Whole-cluster readiness also exposes a fixed-9090 lookup and a `1/3` reducer
  restriction. Repair accepted Web-listener publication and five-voter reduction;
  verify the original volume set after upgrading the image.
- First-push CI exposed four additional groups: KV-only readiness collection,
  stale CLI Access ports, journal selection during normal owner transfer, and GC
  foreground overlap. Keep the original assertions/deadlines and verify each
  group before final publication. Browser trace and GC operation timestamps are
  retained under `ci/run-41128190276/`.

- Repaired-image round 1 preserved the original volumes but exposed finalized
  empty WAL tails: the current Stream record still referenced a zero-boundary
  chunk that the ChunkDB finalizer had legitimately sealed, reclaimed and
  tombstoned. Exact records under `upgrade-round1/` show `Deleted`, a seal time,
  zero capacity/cursors and no remaining strips. Recovery previously accepted
  only `Sealed` for that empty case. Both a cold adapter and complete writer
  reopen regression reproduce the error, then pass with exact finalization
  proof. Reopen preserves the tombstone and allocates a fresh chunk; eleven
  invalid/unproven states remain rejected. Full Stream and ChunkKV server tests
  pass; quality gates and the second image upgrade precede resumed faults.
- The same round confirms the independent cluster-observation repair: all five
  UUIDs resolve to the accepted per-host `9090/9091/9092` listeners, with no
  invalid-voter or foreign-member reasons. Remaining service reasons correctly
  report the blocked WAL recovery. See `upgrade-round1/partial-cluster-status.json`.

- Round 2 image `cde2868ade7b3437c27f01c67c7eaf407a02c15b53b874e580c04d0611d1b1ea`
  restores all original volumes, 35 services, all five cluster-readiness views
  and exact witnesses. B-host loss now passes Catalog takeover, original data,
  and one-shot S3/Iceberg writes. Both B containers recreate with retained
  identities and all five nodes verify the outage writes and 294 exact records.
- The subsequent majority-host loss correctly rejects writes, but restoration
  fails the original 180-second bound. KV role/lease health returns while new
  leaders' applied frontiers remain behind. `upgrade-round2/` preserves the
  exact failure, current per-replica health and complete logs. They show serial
  bulk Phase 1 competing with background repair, plus follower FetchGap stopping
  permanently above the 1024-gap snapshot threshold despite having no automatic
  running-replica snapshot replacement. Two regressions reproduce these paths.
  Bounded concurrent recovery, mutually exclusive initial/background repair and
  continued 16-request FetchGap pass three regressions, including cancellation
  before Accept. Full core gates and a rebuilt image precede the resumed matrix.

- The complete `test-core` gate, KV all-target clippy and workspace formatting
  pass after the backlog repair. Evidence: `ci/run-41128190276/backlog-gates.json`.

- During round 3 the user deleted `.crowdb-runtime` for disk cleanup. Historical
  artifact archives, including the earlier frozen corruption records, are no
  longer available. Surviving `/tmp` logs/scripts were copied back into the same
  runtime artifact root, as requested; current state and complete service logs
  were recaptured from the five original Docker volumes. The cleanup also
  interrupted artifact publication after the already-failed restoration wait.
  Do not treat reconstructed expectations or new captures as recovered archives.
- Round 3 image `64454951913004b00d5d8dad480e8ddeb65088ce34cb427b64c3d0269ec48a71`
  again passes original-volume upgrade and B-host loss/recreation. Majority loss
  rejects writes, but restoration still fails the original 180-second bound.
  Further regressions show FetchGap failing to persist recovered entries and
  restart discarding a valid durable snapshot prefix when its WAL boundary is
  absent. Both failures reproduce before their fixes, then focused restart and
  durable-tree regressions pass. Recovery also needs to probe retained pinned
  contacts when multicast is absent; an empty-discovery regression reproduces
  the missing probes. All route publication continues to require pinned SSH
  identity/bootstrap proof. Complete gates and another image precede resumption.

- After the durable catch-up and retained-contact fixes, complete `test-core`,
  Monitor tests, both crates' all-target clippy and workspace formatting pass.
  Confirmation/publication share their existing three-second authority budget,
  keeping offline SSH proof refresh below the fifteen-second route fence.
  Evidence: `ci/run-41128190276/durable-gates.json`.

- Round 4 image `2989a3a0dc7729126f005330ee58f1e701a87d3696164ce750a259d874755f03`
  restores all 35 services, five cluster-readiness views and exact witnesses.
  The stronger per-replica check then fails its 180-second bound: B replicas
  remain at Group-0 applied slots 77005 and 34771 while A advances beyond 140000.
  The matrix stops before new fault injection. Current receipts, checkpoints and
  complete logs are retained under attempt 17's `round4/upgrade-failure/`.
- A real-RPC regression reproduces historical chosen replay being rejected by
  a higher promise; another proves FetchGap incorrectly exposed accepted-only
  values. Both fail before the repair. Learning now preserves higher promises,
  WAL replay retains accepted history independently of promise record order,
  and the source requires a recovered contiguous chosen proof. Focused tests
  pass; complete core gates and another retained-volume upgrade remain pending.
- An actual zero-ballot probe confirms both blocked boundary slots retain a
  higher promise (`(1,4)` / `(1,5)`) than the source's historical chosen ballot
  `(0,3)`. Both probes are rejected without changing those promises. The
  retained receipt, probe source and complete response are under `round4/`.
- The repaired FetchGap cases preserve exact nonempty values and higher
  promises through WAL restart. The existing RPC migration fixture now
  publishes its actual two-voter quorum decision before fetching; response
  assertions remain intact. Complete core coverage passes through its retained
  prefix and resumed remainder; affected all-target clippy and formatting pass.
  Evidence: `ci/run-41128190276/promise-resumed-gates.json` and
  `ci-fetch-gap-promise-{red,green}.log`.
- Round 5 image `51d0a67638ee9102664710eb95711dae3d201d066c09e44bb8a3aa1ae7e9996d`
  restores all services, five readiness views and exact data. All five replicas
  reach the fixed Group-0/1/3 checkpoints within the original 180-second bound;
  the Group-0 check completes in 110.3 seconds after both B replicas had been
  tens of thousands of slots behind. Per-second checkpoint observations and
  immutable receipts accompany the retained-volume upgrade. Host faults follow.
- Review found that 64 retained address hints could consume the whole probe
  budget and exclude a discovered changed IP. A 64-member regression fails
  before prioritizing fresh hints, then both discovery regressions and Monitor
  clippy pass. The five-member physical fixture probes the same complete set
  in either order; the next CI image includes this budget-boundary repair.
- Round 5 B-host loss/recreation passes in 338.6 seconds for the whole
  sequence: surviving quorum write/read, Catalog takeover within 180 seconds,
  new unique acknowledged KV/S3/Iceberg witnesses, exact original reads,
  retained identity recreation of both B containers and all-five reads.
  Majority-host loss, allocation fault, final audit and cleanup remain pending.
- Majority-host loss/restoration passes in 175.3 seconds for the whole sequence:
  the two surviving voters reject the write, then all 35 services, all-five
  replica checkpoints and exact witnesses recover within their original bounds.
- The first allocation fault observation failed a harness race: its wait ended
  on the first follower's no-op reply before the leader's accepted reply arrived.
  It now waits for actual acceptance within the same three-second bound and
  persists every reply. The resumed case observes explicit allocation-only safe
  retry, completes same-ID allocation/query in 3.12 seconds, keeps every KV PID
  and verifies Group-0 lease availability during the fault. Earlier passes are
  retained; final fresh-outage writes, allocation audit and cleanup follow.
- The resumed final gate passes: a second unique outage witness, all-five exact
  reads, stable service PIDs, fresh native batch/allocation and all Group-0/1/3
  applied checkpoints. The final scan finds 1774 live mirrored segments with
  no cross-chunk physical overlap. All five labeled containers/volumes are
  removed, all 85 owned endpoints are closed, and key-only host SSH still works.
  Final frozen service logs were archived after stopping each container, with
  no concurrent-write tar caveat. The separate 512-record capacity deadline
  failure remains a measured limit; 294 exact records are the recovery witness.
  Evidence: `attempt17/repaired-matrix-sequence.json`, `final-closed/`,
  `final-all-replicas-caught-up.json`, `physical-five-occupancy.log`, `cleanup.json`.
- Complete storage tests and formatting pass after the physical matrix. The
  current isolated Journal browser case passes in 3268 ms; its original ordered
  native-operations group then stops at Shell's KV-stop command. Three earlier
  cases pass and are retained. Whole-workspace clippy and all 277 framework
  tests pass before the newly diagnosed stop-receipt repair.
- Shell's trace shows Stop requested at `00:45:35.108Z`, KV exiting about
  0.1 seconds later, and the identical SSH request timing out after another
  three-second authority read. The target had been Group-0 leader; its durable
  execution record already says complete. The isolated Shell case passes in
  4.3 seconds, then a real-authority/owned-child regression reproduces receipt
  failure after authority loss. Exact completed KV-stop receipts now carry
  bootstrap proof and return without re-execution; legacy, foreign, incomplete,
  retired and live-replacement states still require current authority.

### Stop-receipt and settled-Journal verification

- Full Monitor tests, affected Monitor/E2E clippy, the 277 fixture tests,
  bundled native-browser type checks, workspace fmt and required `tree-lint`
  pass. The C++ gate exited zero with existing warnings in unchanged sources.
- The release image containing the completed-stop receipt fix built in 139.7 s;
  Docker execution identity is
  `sha256:fea066994ab45918a04335fe93b00044aebb3fe73b958278806536a90aa05cba`.
- Exact Shell recovery passes: Playwright 6.5 s, initial election 3385 ms;
  the stopped leader's duplicate operation returns its immutable receipt.
- Exact Journal recovery passes: placement settles in 502 ms before the browser,
  Playwright 4.6 s, owner restart response 2290 ms. The measured pre-change
  browser baseline was 3.268 s; the new time is below twice that baseline.
  All navigation, identity, owner-loss/restart, stale-cursor and cleanup assertions
  retain their original deadlines.
- Evidence: `ci/run-41128190276/receipt-fixture-gates.json` and its named logs.
  The initial direct-source TypeScript invocation lacked the serializer snapshot
  supplied by `bundle_client`; the passing check uses that same snapshot layout.
- Continue the native-operations selection in its original relative order:
  retain unchanged binding/CRUD passes and rerun the KV-stop lifecycle case,
  failed Shell case and all fifteen previously unrun cases (17 remaining).

### Complete native operations and explicit KV-only witness setup

- The native-operations closing run actually executed all original 19 cases,
  despite the launcher-only remainder filter described above. All 19 passed;
  `native-operations-complete-coverage.json` validates the exact original case
  set against `results.json`. `native-resume-orchestration-note.json` preserves
  the planned/actual selection difference. The resume helper now filters both
  launcher and executor selections; no assertions or resources changed.
- The deployment-services closing selection stopped at
  `deployment::kv_bootstrap::monitor_creates_real_kv_groups_then_validates_after_restart`:
  `/stores/0/groups/1/ready` returned 404 (`group 1 not found in store 0`). Seven
  earlier/in-flight cases passed and remain valid.
- This KV-only fixture must explicitly request its ordinary witness Group 1:
  `/system/prepare` accepts bootstrap ownership; it does not create that group.
  Both KV-only witness tests now assert no default-preparation record and no
  existing Group 1, create the group once through the production management API,
  and await its leader under the existing management four-second setup bound.
  Group-0/1 identity, readiness, foreign-bootstrap rejection, exact data and
  retained initialization checks remain. Restart does not recreate the group.
- Repair sequence: affected E2E clippy/fmt, both exact KV-only cases, then the
  24 unrun deployment cases in original relative order. Combined with the seven
  valid passes and two focused cases, this preserves all 33 deployment cases.

### Sustained terminal-writer fault versus automatic DiskIO recovery

- Console API stopped at `console::cli_container::terminal_chunk_writer_recovery`.
  The correct health endpoints returned fresh `ready: true` after the nominal
  forty-second pause. Six independent cases passed, including the formerly
  failing plain Docker attachment; retain them for the resumed selection.
- Monitor lifecycle logs prove the fault healed before the thirty-second I/O
  deadline: DiskIO PIDs 1414/594/631 were replaced by 2344/1522/1515 around
  19–27 seconds after fault submission. The healthy responses were not stale.
- The injector now holds each verified owned Monitor while pausing DiskIO,
  verifies all six original processes remain stopped after forty seconds,
  and resumes DiskIO before Monitor. Production recovery policy, original fault
  duration, health assertions, terminal-replay proof and restart/data checks
  remain intact. No host-wide settings change.
- An `ExitStack` restores every paused process even if another resume fails;
  client reaping still runs on failure. Two cleanup regressions and all 279
  fixture tests pass. The focused terminal-writer test is running.
- Deployment's complete 33-case union and Native Operations' exact 19-case set
  are verified by the named `*-complete-coverage.json` records.

### Replace supervisor suspension with an owned network fault

- Holding Monitor also suspends its child-log pumps. That attempted injector
  produced health-listener curl timeouts while all six PIDs remained stopped;
  it is a failed observation, not a pass. Preserve `terminal-fault-held-focused.log`
  and `run-fd6b4633ab9c4f0e981d5a62da2387fa` for that first divergence.
- Use a forty-second packet drop on each accepted DiskIO TCP listener instead.
  The helper joins only an authenticated owned CLI bridge container's network
  namespace, receives only NET_ADMIN, and mounts a fingerprinted host netfilter
  tool snapshot read-only. It never joins the host namespace or edits host rules.
  Existing Monitor, log pumping and health samplers continue during the outage;
  automatic DiskIO replacement cannot heal the blocked listener early.
- A disposable private-namespace probe passed add/check/delete of the exact
  DROP rule using the current release image. ExitStack cleanup handles each
  jump, rule and chain independently, including partial setup and a failed
  removal; focused unit coverage also rejects host-network targets before any
  helper execution. The original health/currentness, terminal writer restart,
  unchanged Access PIDs, exact data and operation-fencing assertions remain.

### Bound the sustained GC load at its original cadence

- Native Operations 19/19, deployment-services 33/33 and Console API 92/92
  complete coverage is verified. GC cleanup's six cases passed; convergence
  failed at 96/128 files after otherwise monotonic revisions.
- The convergence failure is an explained process exit: the sole Chunk-KV
  container has `OOMKilled=true`, exit 137, with its unchanged 512 MiB limit.
  Four zero-delay SDK workers created 45,828 table cycles before that exit,
  versus the original minimum 48. Keep the frozen state and counters in
  `gc-zero-delay-oom-proof.json`; do not count this overload as a passing run.
- Retain the original 500 ms inter-table interval while running until cleanup
  completes. Stagger four workers by quarter-cycle offsets so 250 ms deletion
  observations do not repeatedly alias all workers' gaps. Sleeps stay outside
  latency measurements; the minimum counts, real deletion overlap, 64 small
  writes and original test deadlines remain. The separate zero-delay foreground
  probe keeps its existing zero-delay/yield behavior.
- Repeat convergence and the restart matrix with that shared pressure change.
  The five other cleanup cases retain valid passes because their load inputs
  are unchanged. Preserve the complete failed long-run evidence.

### Closing verification and publication

- Both paced GC repetitions pass: restart matrix 132.54 s; convergence 479.44 s
  including fixture lifecycle, with 471.46 s client execution. The convergence
  task reaches `Complete`, deletes all 128 source files and verifies their 1554
  logical bytes. The original 64 foreground writes, minimum SDK operation counts
  and measured overlap with actual deletion intervals all pass. The failed
  zero-delay overload remains separate evidence.
- Complete affected container coverage is 151 distinct cases: Native Operations
  19, deployment-services 33, Console API 92, GC cleanup 6 and GC convergence 1.
  Their `*-complete-coverage.json` records identify the actual result files and
  distinguish retained unchanged passes from repeated changed cases. The two
  shared GC pressure consumers were both repeated after restoring the cadence.
- Core, storage, Protocol, Web, Monitor, Console Shared, KV Client, ChunkDB and
  Chunk Stream verification pass, along with workspace fmt, whole-workspace and
  subsequent affected-crate clippy, native-browser type checks, all 280 fixture
  tests and required C++ tree lint. The final post-pressure E2E clippy/fmt and
  framework checks pass. No production changes followed the final release-image
  build; later changes are tests, fixture tooling, CI prerequisites and docs.
- Rechecked run `41003611925`: eight failed jobs, repaired and locally verified
  before the first publication. Rechecked run `41128190276`: five failed jobs,
  repaired and verified by the follow-up coverage above. The historical job
  conclusions are retained; local passes do not change those remote conclusions.
- The user renewed commit/push authorization after temporarily deferring push.
  The completed execution plan is removed for this publication. Cleanup-tool
  changes belonging to the user's concurrent work are excluded from the commit.
- Physical scope remains five containers across two hosts, with complete
  recovery/cleanup results. The separate 512-record/default-five-second capacity
  failure is unresolved as a capacity limit; the 294 acknowledged records are
  verified recovery witnesses only. Earlier archives deleted during user disk
  cleanup cannot be recovered or claimed as retained.
