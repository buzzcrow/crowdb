<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Container E2E Plan

Upstream: [R230](../backlog/R230-test-container-layer-e2e.md).

## Current state

- Migration and classification are implemented; important component UTs remain.
  Formal optimized debug release: `9c0f8805`, OCI
  `sha256:4e34adf470ae00773bcf24b37c3c328e9707e5160346b5e24618b91d4558e853`,
  Docker `sha256:15a8ceb58cc9a6928ccf89075de215856943eb1308b4b878c5e0ed86796031eb`.
  Receipt: `target/crowdb.oci.tar.json`; each run pins client/image identities.
- Final-image Console 153/153, Deployment 33/33 and nine independent domains
  pass. Iceberg completes 31/32 in 1762.795 s, including its original approximately
  20-minute retirement grace. Measured domain rows are in [test.md](test.md).
- All six special profiles pass: bridge SSH admission, Linux host, containerd
  host/cluster, five-engine ecosystem and Python/DuckDB. Their identities and
  measurements are under `/tmp/crowdb-r230-profile-*-final/`.
- Unit/component repair gates pass: DiskDB 177, monitor 75, KV-server 64,
  CLI 6, fixture 106 and UI 183. Pixi/CI classification verifies 28 packages.
- Preserve concurrent worktree edits to the host development helper and README.
  Its unstaged cleanup command is excluded from image identity only after an
  audited staging/build read check and before/after fixture proof. Release
  source, Dockerfile and entrypoint changes remain guarded.

## Remaining tasks

- [ ] **Iceberg concurrent PUT**: resolve
  `access_iceberg::file_http::concurrency::thirty_two_concurrent_direct_puts`,
  preserve all 32 direct 8-MiB PUT successes, and rerun the affected domain.
  Complete-run artifact:
  `/tmp/crowdb-r230-domain-final-iceberg/run-08f1edcfd2274d2ca669f25d48ef8329`.
  Same-resource single-case reproduction:
  `/tmp/crowdb-r230-iceberg-concurrency-exact/run-ba80e3320873495781ddb21dc76058bb`.
  Full run exhausts request deadlines; reproduction has 2 successful uploads
  and 30 deadline failures in 10023 ms. Generic `Catalog is not ready` does not
  prove initialization failure. The remaining grace cluster was idle while
  this one-case reproduction ran; no other operation clients were active.
- [ ] **Resource comparison**: capture CPU/phase evidence and choose the fix.
  `/tmp/crowdb-r230-iceberg-concurrency-resource.log` samples Access memory at
  up to 126.2 MiB, no OOM/max events, and 38 additional throttled periods over
  the approximately 10-second window from 5.7 s before the barrier to 4.3 s
  afterward. Throttled usec are aggregated across threads, not wall time.
  This supports a CPU comparison, not a conclusion that quota is the only cause.
  Temporary phase instrumentation has been removed; original assertions remain.
- [ ] **CI capacity**: make the six-node gate reachable under an approved CPU
  policy/runner budget. Final-source 4-CPU preflight rejects the case before
  allocation: at least 6.5 CPU / 12800 MiB. Evidence:
  `/tmp/crowdb-r230-ci-final-admission.log`. Do not invent hardware capacity.
- [ ] **Closure**: finish the remaining case and CI admission, export final
  redacted diagnostics, update measured rows and remaining issues, then delete
  R230 detail, backlog entry and this plan in a gated cleanup commit. Keep the
  benchmark script and CLI/benchmark redesign as later review. R139 owns full
  configuration/performance profiles. AMD/macOS and remote CI remain unmeasured.

## Open Questions

- User previously requested no resource changes. Permission is pending for the
  prepared Access-only diagnostic `/tmp/crowdb-r230-iceberg-access-cpu1.py`:
  only node 12 CPU changes from 0.25 to 1; other role quotas, 512-MiB memory,
  image, request count/size and deadline remain unchanged. Do not execute until
  the user answers the asynchronous question.
- CI needs a decision between sufficient runner capacity and a separately
  verified CPU-sharing policy. Current `--cpus` is a hard quota, not cpuset or
  relative CPU shares; no resource/admission semantics have been changed.

## Verification

- Root-cause fix plus focused before/after evidence, affected component checks
  and exact acceptance; never increase deadlines, retry mutations or weaken
  assertions to make a symptom pass.
- Prepare browser-batch networks before clients and retain them until all peers
  end. Keep independent profiles from changing bridges during browser clients.
- Redacted exports: `/tmp/crowdb-r230-console-final-public`,
  `/tmp/crowdb-r230-deployment-final-public`,
  `/tmp/crowdb-r230-domain-final-public`; private originals remain for diagnosis.

## Blocked

- All independently executable acceptance/profile work is finished. Resource
  changes wait on the user's earlier instruction to leave them unchanged.
  The prepared Access-only 1-CPU comparison has not been executed; its question
  remains pending. This is a resource decision, not the five-attempt rule.
- Failed command: `pixi run test-container-e2e --layer iceberg --case
  access_iceberg::file_http::concurrency::thirty_two_concurrent_direct_puts
  --concurrency 1 --node-cpus 0.25 --cpus 16 --memory-mib 24576`.
  Complete-domain run and focused reproduction both fail with HTTP 503 request
  deadlines. CPU throttling is observed; causality still requires comparison.
- Choices: approve the isolated 1-CPU comparison and apply the measured fix;
  retain quotas and investigate CPU work in code; or define a separately
  verified CPU-sharing profile. CI separately needs enough actual capacity or
  an approved sharing/admission contract. Never declare R230 complete while
  the case and CI budget remain unresolved.
