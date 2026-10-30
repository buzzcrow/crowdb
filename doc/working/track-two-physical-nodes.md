<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Two Physical Node Test Track

Plan: [plan-two-physical-nodes.md](plan-two-physical-nodes.md)

Run ID: `two-physical-nodes-20261030T010205Z`.
Latest fixture: `physical-20261030-r16`; result: **physical recovery matrix passed**.
SSH access is repaired. All owned physical fixtures are cleaned up. Attempt 15
forensic metadata/WAL, raw mirror reads and complete logs remain archived;
attempt 16 passed using fresh test data and the retained preflight policy
volumes. Historical failures remain recorded below.

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
