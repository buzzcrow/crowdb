<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Physical Node and CI Handoff Plan

Upstream: [Physical Node Test Plan](../dev/physical-node-test-plan.md),
[CI Stability Review Plan](plan-ci-stability-review.md),
[container acceptance](../../e2e-test/README.md), and
[OCI image operations](../../container/oci-image/README.md).

Goal: finish CI stability verification and publish CROWDB, then resolve P13 and
complete the two-host physical recovery matrix without losing data or evidence.

## 1. Current checkpoint: S3 UI repair locally verified, publication pending

Updated 2026-11-01, 23:10 Asia/Shanghai. The user explicitly resumed this plan.
Continue the agreed CI refinement, commit/push, exact-head CI, then physical
recovery sequence. No physical fault or retirement has been dispatched on resume.

- The first resume scope check failed because all five current physical nodes
  are stopped. All seven container/image/volume identities remain unchanged.
  A stopped at 14:44 with exit 0; B stopped at 15:14 with exit 137 and no Docker
  OOM flag. Both hosts subsequently rebooted. The stop cause is not established.
  Preserve `ci/stability-final/resume-host-reboot-scope.json` and
  `resume-host-states.json`; do not count this interruption as a physical PASS.
- The original `scope-guard-before.json` remains immutable. The resume guard
  uses `scope-guard-resumed-before.json`, preserving exact identities and the
  observed stopped state throughout CI. Physical restart remains deferred.

- The previous-image container acceptance completed: all 396 canonical cases
  have a finalized latest PASS; required concurrent isolation also passed.
- The strengthened CLI five cases, real GC three cases, three fresh paired
  auxiliary/Journal browser rounds, three fresh Go SDK rounds and instrumented
  Dataset browse all passed. The resumed controller exited 0; its closing
  scope guard preserved all seven physical containers and volumes exactly.
- The resumed browser queue first failed an immediate Access-health assertion
  after ChunkKV replacement. The test now closes dependency readiness within
  its existing three-second predicate. Three fresh paired rounds passed; the
  earlier failure, traces and read-only diagnosis remain preserved.
- Session 44834, the all-domain controller, also exited 0. Its resumed command
  took 5574.963 seconds; the earlier actual CLI failure remains in its attempts.
- The CI pause marker was removed after the normal client refresh and resumed
  scope guard passed. The separate physical investigation marker remains.
- The previous full-suite controller completed. Current lifecycle repair tests
  are tracked under ci/lifecycle-dependencies-fixed/; original failures and
  baseline comparisons remain under ci/published-5af2737f/. Physical work remains deferred; the
  seven retained physical containers and their volumes remain owned and preserved.
  Do not restart old session IDs.
- Commit 76fc6508189dd31018e953674a8f1c693011498d was pushed successfully.
  [CI run 41295426521](https://github.com/buzzcrow/crowdb/actions/runs/41295426521),
  attempt 1, finished with 24 successful and three failed jobs out of 27.
  All job logs and the three failed-job artifacts are preserved under
  ci/published-76fc6508/. Artifact sizes, SHA-256 and ZIP CRCs were verified.
- All three test-boundary repairs are locally verified: a frame-aware lost-reply
  proxy, exact selected-node requests per topology cycle, and final CLI readiness
  within the remaining original startup deadline. No production source changed.
  The chunk job's 47 remaining cases passed, retaining three unaffected remote
  passes. All six CLI cases, including the interrupted terminal-recovery peer,
  and the original shell/capacity browser pair passed. Fixture 304, full clippy,
  fmt and TypeScript passed; the closing scope guard preserved all seven nodes.
  The coherent test repair commit 70ae86b1c62e095be2754ed178890736e49be5a4
  was pushed successfully. Its new run 41299662435, attempt 1, finished with
  26 successful jobs and one failure: console-api's terminal chunk writer
  recovery. All 27 job logs and its verified failure artifact are preserved
  in ci/published-70ae86b1/. The three previous failed cases passed remotely.
- After a lease-excluded transfer superseded a committed split's retained
  parent, child materialization still required two original split markers.
  The monitor repeatedly failed before serving-grant renewal; the original
  180-second recovery deadline expired. An exact local run passed, but the
  durable-transition regression reproduced the same error deterministically.
  The production fix validates remaining assignments against the durable split
  proof and preserves the transferred parent. All 86 KV Server and 112 ChunkKV
  Server tests passed, with one existing opt-in RDMA ignore. Full clippy and fmt
  passed. A new source-verified image passed the exact terminal recovery case
  (151.1 seconds; recovery check 21.2 seconds), all nine CLI cases at default
  concurrency, two ChunkKV cases, balance and 83 remaining console-api cases.
  These are 95 distinct current-image cases. The coherent fix was committed and
  fast-forward pushed as 5af2737fd5797a1afd1395eac63427e8fdb892d2; commit hooks
  passed fmt/clippy. Run 41304660599 attempt 1 ended 25 PASS / 2 FAIL. Its merge checkout
  3c87db4bcf71f2d532762bbadb3a4416732514f1 has the same complete tree as HEAD:
  4b93303bfaf8cc2e0b797b14815d8e884771e473. Previous 396-case coverage belongs
  to the previous image. All 27 current jobs must pass before physical dispatch.
- The previous terminal writer recovery now passes remotely. The two new failures
  are auxiliary external Access restart during an initial ChunkKV transfer and
  six-node Access deployment Waiting classified as terminal failure. All 27 logs
  and both verified artifacts are retained. The six-node trace exceeded the
  existing 16-MiB export limit and was not uploaded; do not claim it was inspected.
- Local exact auxiliary passed (12.4 seconds), its original concurrent pair
  reproduced the same restart 409 (12.1 seconds), and the serial auxiliary
  passed (13.7 seconds). The first repair used placement settle; subsequent
  source review found that its transfer observation cannot exclude a split
  planned in the same or next tick. Use the actual local ChunkKV readiness
  immediately before each Access restart. Preserve every original response,
  process, sibling and readiness assertion.
- The original ordered local partition stopped earlier at KV restart after
  stopping Group 0's leader. The isolated serial case also failed. A new leader
  was elected after submission; the authority guard returned 502. Observe the
  same cluster's surviving authority within the existing election budget before
  submitting one restart. The mutation response budget remains unchanged.
- Two deterministic plan-hook regressions reproduced Access Waiting and multiline
  ChunkKV Waiting becoming failed. A further regression exposed stale waiting
  detail selecting the ten-second missing-disk backoff for a registered startup.
  The repair restores the existing dependency-wait contract and observes the
  accepted operation without a duplicate deploy/restart. The new source-verified
  image passed all 48 distinct affected cases: native operations 19, browser 27
  and native deployment plans two. Exact KV, auxiliary pair and six-node cases
  passed first; lifecycle cases passed again at original default concurrency.
  The two data-plan cases used concurrency two but resource-aware batching ran
  them sequentially (12,800 + 6,656 MiB exceed the 16,384-MiB budget).
  UI 366, plan-hook 28 including two new regressions, fixture 304, TypeScript,
  fmt and full clippy passed. Commit 18015caf0cc99978eec99517a3f52b468128c0e2
  was fast-forward pushed; run 41311084753 attempt 1 completed all 27 jobs
  successfully. All logs, exact-head/merge source equality and the actual CI
  image receipt are retained in ci/published-18015caf/.
- A test-only refinement replaces the insufficient early placement predicate
  with a direct, identity-checked ChunkKV /ready probe before the single Access
  restart. It retains the three-second observation budget and all mutation
  assertions. Its first local attempt failed on an incorrect inventory URL
  field; the corrected mgmt_url exact case and original concurrent pair passed.
  The full operation group stopped at a separate S3 delete failure: 17 PASS,
  one FAIL and one unrun. Preserve every attempt in ci/access-readiness-fixed/.
  The current refinement and S3 UI repair need publication and all 27 exact-head
  CI jobs before physical dispatch.
- S3 multipart delete's original three-second DOM assertion failed both in the
  full group and a fresh serial baseline. The isolated and concurrent-pair
  baselines passed. In the full-group trace, DELETE returned 204 in 1.279 seconds;
  its subsequent list returned 200 with zero keys in 2.036 seconds. S3View clears
  selection on deletion success but retains cached rows until listing finishes.
  Correction: remove only the successfully deleted exact object's
  current-bucket row before the existing refresh; retain failed/unknown rows.
  The user approved that behavior. S3-04 and its ASCII layout were updated
  first, followed by a deterministic deferred-list regression. After correcting
  its provider/field-label setup, the old UI fails the exact stale-row assertion;
  the one-line removal fix passes all three cases, including rejected and
  unknown deletes and a failed post-delete list refresh. The original three-second
  E2E assertion remains unchanged. Diagnosis, trace timeline and baselines:
  ci/access-readiness-fixed/s3-delete/. New image and verification:
  ci/s3-ui-fixed/ and image/s3-ui-fixed/. The new image passed the exact multipart
  case (4.6-second Playwright), original auxiliary pair (12.7-second auxiliary)
  and all 19 native operation cases at default concurrency (417.230 seconds).
  Multipart passed again in the group at 4.1 seconds. UI 369, full TypeScript,
  fmt and full clippy passed. Initial unsupported Testing Library options failed
  type checking after all UI cases passed; that attempt and the corrected gate
  are preserved separately. Publication and new exact-head CI remain pending.
- Offline physical observer regressions now require an immutable 35-process
  identity fence after recovery and throughout the subsequent full data audit.
  A second fence cannot accept a later restart. Both fence regressions and all
  seven closure acceptance checks passed; physical resources remain untouched.
- Run 41299662435 checked out merge commit e9f241349146046d52c1198cf3f1b0e0dbdbccff.
  Its complete source tree equals the pushed head's tree
  9a7a6a85ba27c2722610d4185912edf67b3c86bf. Local and CI image runtime digests
  differ; source equality does not claim identical binaries.
- Two physical hosts have not completed final acceptance: P13 is still failed,
  P14–P19 are unrun, and the latest native lifecycle fix needs physical validation.

This is a temporary execution handoff. Delete it only after physical completion
and retention of the final report. Keep the separate 16-item CI checklist until
the user finishes joint review; a PASS must not check a human review box.

## 2. Repository and publication boundary

- CROWDB root: /cjdata/cpp/crowdb2.
- Branch: task-fix-build; latest published HEAD:
  18015caf0cc99978eec99517a3f52b468128c0e2.
- The four previously local commits were included in the fast-forward push:
  cdf023d6 (recovery/liveness), 81d07f03 (nearby mirrors),
  d396a220 (healthy leader quorum lease), and 7a93c4e5 (cleanup/handoff).
- The production changes, subsequent test fixes and new split materialization
  repair and lifecycle/dependency repair are committed and pushed. The Access
  test precondition and working-plan updates form the next commit. Preserve
  any new user edits; do not reset or restore.
- Remote task-fix-build was fetched at 5af2737f immediately before the last push;
  GitHub then confirmed the new exact head 18015caf.
  [PR 55](https://github.com/buzzcrow/crowdb/pull/55) is open.
  Fetch and verify remote state again immediately before the eventual push.
- After explicit resume, the agreed order remains: finish local CI stability
  gates; commit/push CROWDB; inspect the new exact-head-SHA CI and every expanded
  job; then resume P13 and the physical matrix. Physical completion is not a
  prerequisite for that CROWDB push.
- Loader and website publication are outside scope. Last recorded loader HEAD
  was local 447a1e619550bbdc0150139976e0c442675f92b0, with origin/main at
  4cafdc9227871f55942a2b215ccb5a3078ade70d; website had local c9306c51.
  These are historical identities, not freshly checked remote status.
  Do not read or stage any .aws directory in any workspace.

## 3. Evidence and frozen inputs

All abbreviated paths below are relative to:

    /cjdata/cpp/crowdb2/.crowdb-runtime/artifacts/physical-nodes/physical-20261031T194149Z

- CURRENT-STATE.json: current pause and next actions.
- ci/stability-final/: previous-image local gates, queues, complete logs,
  coverage, independent reviews and pending refinement evidence.
- ci/stability-final/final-current-image-evidence.json:
  finalized 396-case coverage and current-image mappings for all original
  32 container failure occurrences plus the newer run's three cases.
  Occurrences include repeated case names; they are not 35 distinct cases.
- ci/stability-final/container-coverage.json: latest finalized case per scope/name.
- ci/stability-final/all-queue.json: final all-domain PASS plus preserved failed
  attempt. container-all/run-72c5f00c04874d38ae2b97bb70822cd6 is the completed
  resumed run. Do not label every historical attempt successful.
- ci/stability-final/refined-e2e-queue.json: all ten queue rows PASS, with
  earlier browser attempts preserved. The per-round finalized fixture/client
  identities and Dataset timings are in resume-refinement-closure.json.
- ci/stability-final/resume-publication-gates.json: Rust fmt/full clippy,
  affected C++ format/tree-lint and diff check PASS. Full E2E TypeScript also
  passed after the final test-only dependency-recovery predicate change.
- ci/stability-final/scope-guard-before.json and scope-guard-latest.json:
  exact retained A3/B4 container, image, running-state and volume identities.
- ci/published-76fc6508/: all 27 job logs, failed artifacts, repair-diagnosis.json,
  repair-queue.json, local-repair-closure.json and exact/original-pair results.
  These three newly published failures are separate from the historical 16 jobs.
- ci/published-70ae86b1/: all 27 job logs, the checked terminal-recovery artifact,
  exact baseline, deterministic red/green and both affected component suites.
- ci/split-materialization-fixed/: new image build, exact and grouped container
  queues, 95-case source-specific local closure, quality gates and diff review.
- ci/published-5af2737f/: third publication, all 27 job logs, both checked failure
  artifacts, concurrent/serial baselines and deterministic dependency regressions.
- ci/lifecycle-dependencies-fixed/: current prepare/exact/operations/browser/data
  queues, 48-case source-specific closure, quality gates and diff review.
- ci/published-18015caf/: all 27 successful job logs, source equivalence,
  CI image receipt and offline physical audit/retirement checks.
- ci/access-readiness-fixed/: the subsequent test-only readiness refinement,
  first failed endpoint-field attempt, corrected verification and S3 diagnosis.
- ci/s3-ui-fixed/: UI red/green evidence, corrected gates, exact cases and full
  19-case operation group on the new source-verified image.
- The earlier long handoff is preserved at
  handoff-history/plan-physical-node-ci-handoff-before-rewrite-20261101T062936Z.md.
  It contains superseded historical instructions, not current authorization.

Current local server image:

- Tag: crowdb-node:s3-ui-fixed.
- Source tree: 4b061ce8ab3d74493e975659c79505e5b620b054.
- Docker image ID:
  sha256:05bbc6cb53d6aafbc18004f8a2271ddc4708c88ac69d32a6a41fd30e96468c2c.
- OCI config digest:
  sha256:381d352c595863b44b7deaa55c75cf1779ec038b40822d58b39466aeca0bf7b0.
- Runtime SHA-256:
  84cdaba42a73f087a5b278834cf768bc210b08af2774839166a74ea7e4dd778f.
- OCI archive: image/s3-ui-fixed/crowdb.oci.tar; SHA-256:
  4ab605d4b0f060cc040e5d217d94850bfa88e9f54437b980ce846867f7dac979.
- Revision label 18015caf predates the new fix; the source-tree label
  records the actual source. Physical hosts have not been upgraded. Setup and
  retirement require this image, current local verification and the eventual
  new all-27-job CI PASS.
- The preceding lifecycle-fixed image and its 48-case validation remain in
  image/lifecycle-dependencies-fixed/ and ci/lifecycle-dependencies-fixed/.
  Its Docker ID begins 359897a7, source tree is
  1516797b67ad493c9bbf9c25048e574e7d994f31 and runtime SHA-256 is
  c03d3b78cf126c42923cfd91b3e6d03160ae082e77039708afafda465d9a5e26.
  Those 48 cases are not relabeled as new S3-image coverage.
- The preceding split-fixed image and its 95-case validation remain in
  image/split-materialization-fixed/ and ci/split-materialization-fixed/.
  Its Docker ID begins cec0d2c1 and source tree is
  861fc9d45ae32f72a5199585a8bb1b231048b448; those passes are not relabeled as
  current-image coverage.

Previous local server image and retained 396-case evidence:

- Tag: crowdb-node:ci-stable.
- Production source tree: 376fdc26a0e96d7810313ea670bf043c4f0dfcae.
- Docker image ID:
  sha256:c2bc8bfd6228cf61fa5a73e8ab296766cd880f8eb4fb100efe247fa23c0a55e9.
- OCI config digest:
  sha256:c208f00b8c04b085053010227a755dfe114d1e5a756a13f30019422eb91d1bc0.
  This is a different object from the Docker image ID.
- Runtime SHA-256:
  7e17ab9c83a2d4c0411114c39377cd4cabb28d2330791dd5d09d3dce3ac98cfd.
- OCI archive: image/stability-final/crowdb.oci.tar; SHA-256:
  565ffcde6e39f3286d3a671dea8310e646c467cac8d0a9e6af4e598a662100d8.
- Image revision label is the old HEAD 7a93c4e5; the source-tree label captures
  the uncommitted production fixes. The image is built and locally verified.
  It has not replaced the older image on the physical hosts.
- Latest compiled client tree used by all three fresh paired browser rounds,
  all three Go rounds and Dataset browse:
  9f2a4a339b6b02852e39cffa16b5cae0f6f9d8a5.
  The CLI used 65819b6aa8cdacf284941faf932681219d01cd0a; resumed GC used
  d862cc4d3238afd6e9ccb39e4996ab415d10c5bc. Later changes affect only the
  auxiliary browser helper and documentation, so those prior passes remain valid.
- ci/stability-final/refined-source-verification.json confirmed the previous
  server sources at that checkpoint. The full all-domain run used an earlier
  copied client bundle.
  Its PASS cannot validate the newer GC/Journal assertions; their separate
  strengthened runs passed. The new production split fix requires the separate
  split-fixed image and its current source-verification receipt above.
- This handoff update changes the repository source identity again. Before
  another E2E run, refresh compiled clients through the normal builder.
  Do not bypass manifest validation or blindly rerun all-queue, which is complete.

## 4. Work already verified

- [x] **Core production fixes and component gates**: core 1165,
  access/monitor 2039, console 426 and full fixture 301 tests passed, with the
  existing opt-in/environment ignores retained. UI, full TypeScript,
  Rust fmt/full clippy, affected C++ format/tree-lint, full C++/FFI, storage,
  ChunkDB and CI task mapping passed. Exact queues/logs are retained.
- [x] **RPC/native lifecycle**: final shared route ownership is retired outside
  RPC callbacks, including recursive retirement and process-exit draining.
  C ABI send/chain/slab retain native owners before synchronous completion.
  Deterministic Rust FFI and four native red/green cases, full gates and
  independent review passed. See ci/route-retirement-fixed/ and
  first-failures/P12-A3-survivor-restart/crash/FINAL-READONLY-REVIEW.md.
  The physical P12 abort has no stack; do not call it the same proven stack.
- [x] **Service observation**: HTTP reads no longer perform authority/SSH/health
  network collection. Independent samplers use sample-start freshness and
  binding/endpoint/operation/mutation fences; real monitor identity survives
  Group 0 unavailability. Actual readiness remains independently checked.
  Nine focused Web tests, two real monitor tests, ten replay tests and full
  component gates passed. Final source review has no blocking finding.
- [x] **Browser batch failure provenance and scoped cleanup**: undispatched peers
  are explicitly not_executed, never PASS or an independent root failure.
  Original errors and case-specific cleanup errors remain distinct. Shortcut
  cleanup is scoped to recorded owned resources. Full fixture tests passed;
  actual all-domain and scope-guard results passed.
- [x] **Auxiliary lifecycle coverage**: original combined three-second predicate
  passed on the new image (Playwright 9.7 seconds). Strengthened actual PID,
  namespace, old-process exit, generation/operation, sibling identity and
  registration uniqueness coverage passed (12.4 seconds). All 19 native
  operations cases passed. Monitor operation change is checked; the HTTP API
  does not expose an exact accepted operation ID, so that is not claimed.
- [x] **CLI cold observation and actual process identity**: the real first
  all-domain failure was KeyError: 'pid' in plain_docker_target_attachment.
  A ready cluster need not have every optional cached PID immediately.
  The fix checks owned containers/bindings, 21 monitor services and real /proc
  liveness while preserving 18 unique full endpoint registrations.
  Four regressions and all five strengthened actual CLI cases now pass.
  This does not add waits or caller retries.
- [x] **Previous image container acceptance**: 396/396 latest canonical PASS and
  required four-case concurrent isolation PASS. The progress helper shows eight
  isolation passes because two all-domain attempts each ran four; do not count
  them as eight distinct isolation cases. All seven physical resources retained
  their exact identities.
- [x] **CI failure audit**: all 16 failed jobs from the three user-specified runs
  are in the linked stability TODO. Complete job logs, ten original artifact
  ZIPs with matching hashes, and case mappings are retained in ci/prior-ci-audit/.
  The runs are 41003611925 (8/27 failed), 41128190276 (5/27), and
  41159636766 (3/27 at head 8885d2a). All 16 joint review boxes remain unchecked.

Main source ownership in the published change:

- RPC/C ABI: lib/crowdb-rpc/ffi/src/owned_route* and logging.rs,
  lib/crowdb-rpc/src/c_api.cpp, lib/crowdb-tree/ffi/src/tree.rs, and tests.
- Repair/recovery/read path: app/crowdb-chunkdb/src/repair*,
  app/crowdb-chunk-kv-server/src/storage.rs,
  lib/crowdb-chunk-kv/src/partition* and lib/crowdb-access-iceberg/src/file/reader.rs.
- Observation: app/crowdb-web/src/services/observation* and remote/deployment/state
  integration; container/crowdb-monitor/src/node/runtime/{observation,services}*
  and corresponding crate tests.
- Test stability: e2e-test/browser_batch.py, fixture*.py, report.py,
  oci_shortcuts.py, console_cli_target.py, GC convergence/pressure/sdk_pressure,
  console/native_balance.rs, console-ui lifecycle/Journal flows and helpers,
  their regressions, and tools/test-metrics/measure.py.
- Split materialization: app/crowdb-kv-server/src/background/domain_monitor/
  chunk_kv/catalog.rs and tests/common/split_materialization.rs.
- Documentation: this handoff, plan-ci-stability-review.md and
  doc/dev/physical-node-test-plan.md. Inspect git status before staging.

## 5. Remaining CI stability and publication tasks

The user has resumed; proceed in the order below.

- [x] **Validate the GC ready barrier with the new bundle**: run the three exact
  convergence, notification matrix and foreground SDK cases in the saved queue.
  Four real SDK workers now warm up and park before executable purge markers;
  actual deletion releases work after the original faults. Preserve nine files,
  three restarts, six operation classes, 64 writes, true completion/deletion
  overlap and original 720/3300-second absolute deadlines. Six pure regressions,
  compiled clients, clippy and independent review passed. All three strengthened
  real cases passed on resume; queue duration 480.220 seconds. Inline convergence
  and matrix retain 240/81 SDK completions inside 5/7 deletion intervals, with
  64 writes each and all three matrix restarts before release. Evidence:
  `ci/stability-final/resume-gc-proof.json`; see GC-READY-BARRIER.md and
  GC-READY-FINAL-REVIEW.md.
- [x] **Validate Journal request ownership and repeat the browser pair**: exact
  request/cursor identity plus Response.request() association and body completion
  replace pathname-only response matching. All waits share the original three
  seconds; owner-loss/restart/stale-cursor assertions remain. Four event
  regressions and full TypeScript pass. Run auxiliary plus
  journal_identity_and_owner_recovery together, three fresh fixture rounds,
  all required to pass. See JOURNAL-REQUEST-STABILITY.md.
  Resume round one passed; round two failed the auxiliary Access initial `up`
  assertion after Chunk-KV replacement, while Journal passed both rounds.
  Exact isolated and paired serial baselines passed. Read-only sampling of the
  unchanged concurrent pair confirms real Access 503 during dependency faults,
  followed by recovery. The existing three-second replacement predicate now
  additionally requires same-node Access recovery before advancing. No assertion,
  timeout, or mutation retry was removed or relaxed. Three fresh paired rounds
  on this strengthened source all passed in 45.303, 44.938 and 46.144 seconds.
  Earlier attempts are preserved under
  `ci/stability-final/first-resumed-browser-failure/`; diagnosis is in
  ACCESS-DEPENDENCY-CLOSURE.md.
- [x] **Repeat real Go SDK cleanup three times**: normal and injected cleanup
  assertions stay unchanged. Old CI cleanup's operation/root cause is unknown;
  enhanced Smithy operation/API-code errors are retained. Repeats provide
  current coverage, not a claim that an unknown historical algorithm was fixed.
  Three fresh rounds passed in 11.143, 10.611 and 10.139 seconds. Each log
  confirms both ordinary and injected-failure cleanup completed.
- [x] **Run instrumented Dataset browse and retain headroom risk**: phase timing
  now separates seed, default production balance and exact 512-record readback.
  In the completed all-domain run browse was 281.284 seconds, import 321.901,
  recovery 331.468; the latter two leave only 38.099/28.532 seconds under 360.
  Late partition formation is observed; no safe behavior fix is proven.
  Keep policy, cooldown, three owners, at least 12 partitions, data and deadlines.
  DATASET-HEADROOM-AUDIT.md records causal limits and bounded follow-up evidence.
  Instrumented browse passed: seed 2.276 seconds, balance 260.002, exact readback
  0.273; 12 partitions and three owners. Client 278.32 seconds, full queue case
  291.230 seconds. This risk remains open even after a PASS.
- [x] **Close local evidence**: all refinement rows and finalized fixture
  timings checked; fresh scope guard and production source verification pass.
  Final-current-image evidence confirms 396 latest PASS, mapping all 32 older
  failure occurrences and three newer cases. Client binary hashes match the
  frozen build manifest; browser/Go/Dataset source digests agree. Stability TODO
  updated; all human review boxes remain unchecked.
- [x] **Commit and push CROWDB only**: final relevant gates and staged diff
  check passed; the pre-commit hook independently passed fmt/clippy. Created
  76fc6508 and fast-forward pushed after fetching and verifying the old remote.
  The publication receipt is ci/stability-final/publication.json. Required rules:
  inspect staged contents, and make one coherent commit with a single-line
  subject, no body/trailers/doc references/requirement numbers. Fetch and verify
  the remote before the authorized fast-forward push; do not force-push.
  No Sirius-specific pre-push rules apply to this repository.
- [~] **Inspect new exact-head CI**: run 41295426521 attempt 1 ended 24 PASS / 3 FAIL.
  The three fixes and local affected regressions passed and were pushed as
  70ae86b1. Run 41299662435 attempt 1 ended 26 PASS / 1 FAIL. Its production
  split materialization repair now has 198 affected component PASS and 95
  exact-image container PASS. The fix is pushed as 5af2737f; inspect all jobs in
  run 41304660599 attempt 1 ended 25 PASS / 2 FAIL. Repair the two new lifecycle
  failures and the local Group 0 restart boundary, then publish and inspect the
  next exact-head run. Current diagnosis: ci/published-5af2737f/repair-diagnosis.json.
  Match the actual new head SHA/run/attempt, inspect every expanded job
  (the three historical runs had 27), preserve every first failure and artifact,
  fix real errors and rerun the appropriate new-head job/run. Do not rerun an old
  failing SHA as validation of the new code.
- [ ] **Joint review**: retain all 16 job-level entries for the user. Passing
  tests cannot close the historical Go root-cause unknown, Dataset headroom,
  or unfinished physical validation.

The refinement queue is complete. Do not replay it merely because this document
changed. If a later code fix needs E2E, refresh clients normally and select the
affected group while retaining valid completed evidence. Reuse the final image
while production sources still match. Every command and original error is durable.

## 6. Physical checkpoint and remaining work

Physical work is deferred until the CI/publication stage above is complete.

- Hosts: A 192.168.31.129 (A1/A2/A3) and B 192.168.31.184 (B1/B2).
  Existing SSH uses BatchMode/IdentitiesOnly. Verify host identity/clocks on resume.
- Remote project commands use /home/cj/.pixi/bin/pixi run with explicit manifest
  /home/cj/crowdb-runs/pixi.toml. Remote artifact/harness root:
  /home/cj/.crowdb-runtime/artifacts/physical-nodes/physical-20261031T194149Z.
- Current owned nodes: a1/a2/a3/b1/b2 labelled with this run. Two older stopped
  B containers are additionally retained: seven physical containers total, A3/B4.
- Five voters, two physical racks, Groups 0/1/3, 35 managed services;
  each node has hard 1 CPU, 2 GiB, 512 PIDs, one Tokio worker,
  16 GiB media with 1 GiB reserve. Preserve port offsets A=0/1/2 and B=0/1.
  Port bases: Web9090, KV10000/10100, DiskDB11100/11200, DiskIO13000,
  ChunkDB12100/12200, ChunkKV15100/15200, S316000, Iceberg17000,
  Dataset18000, health19000, container SSH2222. Preserve host SSH22.
- P00–P08 passed on the initial image, including complete eight-table Iceberg
  SF1 file/checksum/row and bidirectional SQL checks, independent zero-retry S3
  SF1, Dataset full fields/multipart, 512/4096 native records across five safe
  owners, acknowledged-deletion audits, and the first complete 30-minute soak.
- Actual mixed-workload overlap was 1818.511 seconds: 1826 native cycles,
  1829 S3 cycles, 183 Iceberg and 183 Dataset cycles. All 35 identities unchanged;
  909 complete observations, max gap 2.617 seconds, no OOM.
  Do not repeat/resume that timed workload merely to manufacture a new pass.
  Production changes during a new soak would require restarting that new soak.
- P09–P11 passed on the older repair image with full data/history and sibling
  identity closure. P12 original repair defect was fixed: expired unpublished
  Allocated targets are replaced under the claim, copied/published targets and
  normal owner-grace reclamation remain intact. Original takeover then passed
  in 40.184 seconds. Old physical image input tree is
  76f794e03757249f37efa96cd0bfcad064be4388.
- A P12 harness witness exceeded its EC 8+4 outage budget when B removed six
  fragments; the corrected fresh mirrored outage witness passes, and complete
  history returns after B recovery. Keep this harness boundary distinct from
  production repair and retain the complete post-recovery historical audit.

- [ ] **Archive current physical data before image upgrade**: existing private
  archives are complete/tested/hashed but predate later P12/P13 data. Make fresh
  private archives of every still-existing volume before any new upgrade or
  retirement. Archive helper containers must use --log-driver none so binary
  tar data cannot fill Docker JSON logs. Keep incomplete old streams invalid.
  Transfer/verify the final production image and preserve exact mounts/limits.
  Do not blindly replay resume_repaired_image.py or old witness mutation IDs.
- [ ] **Close unexpected P12 A3 identity change**: A3 ChunkKV changed PID451/gen2
  to PID66135/gen3, stderr Resource deadlock avoided, no usable core.
  Local Rust-FFI gdb proves callback-final-owner destruction can join its own
  RPC worker; the physical stack is unknown. Validate the new lifecycle fix on
  physical hosts and close all collateral process identities through full audits.
  Preserve first-failures/P12-A3-survivor-restart/ and
  faults/P12-full-identity-closure.json.
- [ ] **Resolve P13 Iceberg read timeout**: after A quorum loss, recovery/native
  and five complete raw S3 SF1 audits passed. Three Iceberg readers returned
  curl 28/GetObject low-speed timeout; two remaining owned read-only readers
  were stopped after preserving errors and are interrupted, not PASS.
  Dataset/history checks were not dispatched. Exact endpoint/object/range cause
  is unknown; unlabeled old stdout cannot assign failures by output order.
  B1/B2 had controlled process-lease-expiry restarts with empty stderr, distinct
  from the P12 A3 abort. Read first-failures/P13-protocol-read/,
  owned-reader-stop.json, READONLY-REVIEW.md and ci/physical-after-witness-scope.log.
  Add per-endpoint/object-stage read-only diagnostics before retrying verification;
  keep original I/O semantics, limits and credential redaction.
  The minority write returned OutcomeUnknown; later value presence is not an
  acknowledgment and must not cause automatic mutation replay.
- [ ] **P14 allocation admission fault**: preserve bounded Group1 step-down,
  unchanged Group0 lease/term observations, explicit allocation Unavailable,
  same-ID safe retry under the original 10-second/five-retry client policy,
  all 35 process identities, and complete subsequent data audit.
- [ ] **P15 backlog and cold restart**: stop a real Group3 follower, acknowledge
  1100 single writes and prove gap greater than 1024; restore within the original
  independent 180-second checkpoint gates, audit all writes, then cold-restart
  all five and audit complete retained history. Do not claim reclaimed-WAL or
  other untriggered physical sub-boundaries independently exercised.
- [ ] **P16 final occupancy/live overlap**: validate 17 total KVs including the
  fresh batch, allocation, full data/history, capacity and process identity
  fences. Follow the actual contracts in harness/recovery_final.py and the
  reusable physical plan; do not replace them with a weak readiness-only pass.
- [ ] **P17–P19 close only after all prerequisites pass**: freeze complete
  evidence/results/summary, inspect identities across the entire fault and audit
  intervals, privately preserve remaining data, then exact owned-only retirement.
  Use only the reviewed harness/close_physical_final.py and
  cleanup_host_final.py stages. Never replay obsolete close_run.py or
  cleanup_host.py; no global cleanup.

The separate investigate-unexpected-P12-restart marker prevents recovery_final.py
from dispatching P14. It is not the CI pause marker. Do not remove it merely to
make the controller advance; P12/P13 evidence and gates must actually be closed.
Old controllers already submitted mutations: never run all_physical.py or an
earlier resume script blindly.

## 7. Non-negotiable evidence and data boundaries

- Run all builds/tests/lints/project executables through pixi run, including
  remote commands with the verified manifest. Serialize shared-output builders.
- Print the real failing case/error; use CROWDB_E2E_PRINT_SERVICE_ERRORS=1 while
  diagnosing and retain complete logs. Only finalized timing records containing
  total and stop count as results; provisional diagnostic status is not failure.
- Keep existing assertions, operation budgets and caller retry policies.
  Add no hot-path lock or unsafe exception. Unknown mutations are reconciled
  read-only; acknowledgment cannot be inferred from a later successful read.
- Native keys exclude reserved S/I/D prefixes. Keep the original 64-KiB values,
  batches of 16, five-second operation deadline and actual safe-owner coverage.
- Observe all 35 service identities and OOM state with at most five seconds
  between complete samples. Readiness must postdate the current process start
  and be at most three seconds old; retain at least 30% free test media.
- Recovery service/catalog/fixed-checkpoint gates remain independently bounded
  at 180 seconds; PID checks include the entire subsequent data-audit interval.
- Preserve the earlier destructive cleanup incident honestly in the final report:
  the old unscoped shortcut cleanup removed seven containers, 89 volumes and one
  network on A, including six physical A data volumes before archival. Those six
  contents cannot be recovered from retained logs. Incident evidence is under
  the older physical-20261031T145356Z run's
  ci/native-worker-fixed/unscoped-cleanup-incident.json.
- Cleanup has since been scoped and verified, but it does not undo that loss.
  All still-existing failed-run volumes were privately archived before previous
  retirement. Never use Docker-wide prune, generic --all cleanup, prefix-wide
  protocol deletion, or remove unrelated data.
- Current authoritative state is this checkpoint plus current runtime receipts.
  Historical 024347 runtime was deleted; its old paths and sessions are not
  surviving proof or valid executable inputs. The archived earlier handoff is
  historical context only.
