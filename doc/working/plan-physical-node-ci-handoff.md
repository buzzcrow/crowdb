<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Physical Node and CI Handoff Plan

Upstream: [Physical Node Test Plan](../dev/physical-node-test-plan.md),
[OCI image operations](../../container/oci-image/README.md), and
[container acceptance](../../e2e-test/README.md).

Goal: validate the committed recovery fixes with a fresh five-container run
across two physical hosts, including 30 continuous minutes of active load,
the recovery matrix, and the remaining CI cases.

This is a temporary handoff, updated on 2026-10-31. Delete it after the remaining
work is complete and the final report is retained in the new run directory.

## 1. Read this first

- The user deliberately deleted `.crowdb-runtime` and instructed us to retest
  next time. The old physical run is interrupted, not an acceptance pass.
- **The full 30-minute workload has not completed.** The formal final workload
  had not started at the last recorded checkpoint. Earlier short runs do not
  count toward the requested duration.
- **A complete Iceberg SF1 load has not passed.** The latest election fix still
  needs physical load verification. Passing unit tests and retained-volume
  startup do not establish that the original load failure is fixed.
- The deleted directory contained harness scripts, wheels, logs, ledgers,
  image receipts, and controller state. Historical paths below are identifiers,
  not available evidence. Reconstruct a fresh harness from current interfaces;
  do not launch commands that depend on those deleted files.
- The loader's S3/Dataset implementation is committed and pushed. Its later
  0.1.2 filename changes and all CROWDB changes are committed locally.
  **Do not push CROWDB without new user authorization.** No further push was
  requested when committing the remaining changes.
- No fresh run was started for this handoff. Keep new artifacts under
  `.crowdb-runtime`, as requested by the user.

## 2. Repositories and publication boundaries

- **CROWDB**: `/cjdata/cpp/crowdb2`, branch `task-fix-build`.
  The recovery checkpoint ends at
  `d396a2207e5a13ef61ee1fe04e68ee40b0f8622c`.
  These three local commits contain the recovery work:
  - `cdf023d642da40f6b2db03c54b3f1a4fdc5fd049`:
    `Reduce recovery memory and harden storage liveness`.
  - `81d07f03398aaa144e8dcec1187bff0a0d18f91b`:
    `Prefer nearby mirrors during partition recovery`.
  - `d396a2207e5a13ef61ee1fe04e68ee40b0f8622c`:
    `Protect leader votes with the current quorum lease`.
  The follow-up commit containing this handoff also records the remaining
  OCI cleanup changes. Do not push this repository.
- **TPC loader**: `/cjdata/cpp/crowdb-tpc-loader`, branch `main`.
  Current HEAD is `447a1e619550bbdc0150139976e0c442675f92b0`,
  `Use readable table-scoped Parquet names for version 0.1.2`.
  This follow-up is local only. The local `origin/main` tracking ref remains at
  `4cafdc9227871f55942a2b215ccb5a3078ade70d`,
  `Add standalone S3 and synthetic Dataset loading`.
  This was pushed, including its ancestor `ead5449`, which publishes native
  Iceberg upload locations before manifest registration.
  [Loader CI run 41194754281](https://github.com/buzzcrow/crowdb-tpc-loader/actions/runs/41194754281)
  was recorded successful for Python 3.10, 3.11, and 3.12.
- **Website**: `/cjdata/cpp/crowdb-web`, branch `main`.
  `c9306c51c71dd3b71a4daf9255692da53f0f3544`,
  `Document standalone S3 and Dataset loader commands`, is local only.
  Do not publish it without authorization. Never read or stage its untracked
  `.aws` directory.

The remaining changes were committed at the user's request; no additional
repository was pushed.

## 3. Physical environment and remaining resources

- Host A: `192.168.31.129`, previously using interface `wlp37s0`.
  Nodes A1/A2/A3 use port offsets 0/1/2.
- Host B: `192.168.31.184`, previously using interface `wlp4s0`.
  Nodes B1/B2 use port offsets 0/1.
- Verify addresses, interfaces, host identity, clocks, and ownership again.
  These are two physical failure domains and five container voters.
- Existing Ed25519 SSH access works noninteractively. A fresh `BatchMode`
  connection to B succeeded during handoff preparation. Use private local
  SSH configuration; do not put login accounts or credentials in documents.
- The previous containers used host networking, `1 CPU / 2048 MiB` each,
  one Tokio worker per service, five voters, Groups 0/1/3, and 35 managed
  services. Test media was 16 GiB per node with a 1 GiB reserve.
- Old run ID and ownership label:
  `physical-20261031T024347Z` and
  `org.crowdb.e2e.run=physical-20261031T024347Z`.
- Deleting `.crowdb-runtime` did not remove Docker resources. At handoff,
  the three A containers still existed and were running, with the expected
  ownership label, CPU/memory limits, and no container-level `OOMKilled` flag.
  B containers were not freshly inventoried during this handoff; their last
  recorded state was running the same upgraded image.
- The old physical controller exited with status 1. The orphaned CI controller,
  which was waiting for workload ledgers that no longer exist, was stopped with
  `SIGTERM`; its exit was verified. Do not resume either controller or rely on
  old session IDs.
- No persistent volumes were removed for this handoff. Cleanup is unfinished.
  Inventory both hosts before stopping, replacing, or deleting any resource.
  Do not use Docker-wide prune or remove another task's containers/volumes.

Read-only starting commands, from the CROWDB root:

```sh
git status --short
git log -4 --oneline
pixi run docker ps -a --filter label=org.crowdb.e2e.run=physical-20261031T024347Z
pixi run ssh -o BatchMode=yes -o IdentitiesOnly=yes -o ConnectTimeout=5 192.168.31.184 true
```

For remote executable/test commands, use remote Pixi with an explicitly verified
project manifest or working directory. A bare remote `pixi run` from the login
directory failed because there was no Pixi manifest there; this was not an SSH
authentication failure. Do not guess the old remote staging directory, whose
local receipt was deleted.

## 4. Latest root cause: healthy leader votes away its quorum

The last failed physical Iceberg SF1 run did not report OOM. Six tables committed:
`region`, `nation`, `supplier`, `customer`, `part`, and `partsupp`.
The `orders` data upload completed, but a manifest `UploadPart` returned
`INTERNAL_FAILURE`, leaving its outcome uncertain. `lineitem` failed during
native upload with HTTP 500. These outcomes must not be counted as successful
table commits or blindly replayed.

The first divergence was in Root Group 0 election handling:

- A3 was the leader with a current quorum lease and a responsive A1/A2 majority.
  The two B voters lagged under load and repeatedly attempted elections.
- Around 11:37 UTC, a B challenger obtained a vote from the healthy leader.
  Leadership changed, recovery scanned a backlog of roughly 7,000 slots,
  process leases expired, and a native file-block intent reached its ChunkKV
  operation deadline.
- `handle_pre_vote` and `handle_request_vote` checked the old self-vote lockout.
  That lockout can expire during a long leader tenure even while heartbeat
  quorum acknowledgments keep the read lease current.
- Commit `d396a220` rejects both vote paths while the replica is Leader and
  `lease_read_until() > now`. Normal voting resumes when that lease expires.
  It uses the existing lease and vote-handler synchronization; no new hot-path
  lock, caller retry, or larger deadline was introduced.

Relevant files:

- `lib/crowdb-kv/src/cluster/local_replica_election.rs`.
- `lib/crowdb-kv/tests/election_test/vote_test.rs`.
- `doc/design/kv/design-crowdb-kv-leader-election.md`, section 6.1.

Recorded verification before runtime deletion:

- Both new leased-leader vote tests failed before the fix and passed afterward.
- The final election suite passed 79 cases, including the five-voter case where
  two lagging voters cannot obtain the healthy leader's third pre-vote.
- Affected KV/server tests passed 733 cases across 31 test binaries.
- Rust formatting and clippy with warnings denied passed.
- The OCI build passed. All five retained-volume nodes were upgraded, and
  35-service readiness passed in 47.582 seconds under the original 180-second
  gate; identity restoration took 2.355 seconds.
- The controller had started read-only reconciliation of prior Iceberg failures
  when last observed. No subsequent fresh SF1 success was recorded.

The locally inspected running A image ID was
`sha256:439f85489106fc8a7d01c3ee265e31445c0879bd95b2578eaea7e573fed838d5`.
The recorded OCI config digest was
`sha256:a1f93e79d6fe463733e7a1b789a746c5fe35811dcd53cac26286dce737054519`.
These identify different image objects; do not equate a config digest with a
Docker image ID. Rebuild and verify both hosts for the next run.

## 5. Previous fixes and what they establish

- **Mirror locality, commit `81d07f03`**: partition recovery replayed an
  approximately 88 MB WAL very slowly. In the diagnostic sample, 761 of 775
  slow reads selected a remote-rack mirror first. Optional
  `ChunkReadPolicy.preferred_node_id` orders complete mirror hints by local
  node, then local rack, preserving fallback and the original deadline.
  Unknown or incomplete placement leaves the original order intact.
  Production ChunkKV supplies its node ID; cache contents and replica membership
  are unchanged. Files are under `lib/crowdb-chunk-client/src/chunk/`, its
  `tests/`, and `app/crowdb-chunk-kv-server/src/storage.rs`.
  Recorded results: 427 affected tests passed, one opt-in RDMA test ignored,
  clippy passed, and retained-volume readiness completed in 46.796 seconds.
  The later SF1 run exposed the separate election issue above.
- **Recovery memory, commit `cdf023d6`**: preserve the replay-built WAL index,
  stream replay while retaining independent highest accepted/promise records,
  and preserve current term/vote records during WAL garbage collection.
  Other changes bound ChunkKV replay buffers, release encoded snapshot pages,
  and avoid prefaulting unused frame-cache capacity. Focused memory experiments
  and C++/FFI/tree checks passed. An isolated exact-volume Root replay improved
  readiness from 97.756 to 1.791 seconds with matching recovered state.
  This isolated diagnostic does not replace five-node acceptance.
- **Storage liveness in the same checkpoint**: honor the independently renewed
  WAL process lease during ownership transfer; preserve staged strip state
  through empty liveness refreshes; renew DiskDB ownership by CAS; refresh
  Monitor route authority before optional old-owner confirmation; and retain
  bounded recovery progress and mirror-rack consistency.
- **CI fixes in the same checkpoint**: disable initial auto-join before the
  explicit fourth-node admission case, and accept only the specified bootstrap
  readiness responses under the original limits. Earlier local reruns passed
  the three previously failing cases, but closing verification on current HEAD
  is still required.

Do not interpret a previous image's pass as a pass for current HEAD. The old
raw receipts were deleted, so retain fresh evidence for any final claim.

## 6. TPC loader implementation and acceptance limits

- The committed implementation supports native Iceberg loading, standalone
  S3 generation/upload/verification, and generated Dataset samples. Shared
  journaling records acknowledged, failed, and uncertain outcomes.
- Standalone S3 explicitly uses zero SDK mutation retries. A regression checks
  that HTTP 500 produces exactly one PUT. Failed/uncertain candidates are
  retained for read-only reconciliation instead of automatic mutation replay
  or generic deletion.
- Recorded exact-commit checks for the pushed 0.1.1 implementation: 252 tests
  passed, six opt-in cases skipped; Ruff, wheel/sdist build, and Twine checks
  passed. Its pushed CI was green.
- The subsequent 0.1.2 commit records readable table-scoped Parquet filenames,
  the version bump, and matching usage/changelog/test changes. Fresh local
  checks also passed: 252 tests, six opt-in skips, Ruff lint/format for 63 files,
  wheel/sdist build, and Twine. These are local checks, not fresh remote
  acceptance or a published release. New receipts are under
  `.crowdb-runtime/artifacts/commit-remaining-20261031T120321Z/`.
- Older physical runs passed Iceberg SF0.01 for eight tables/86,805 rows,
  standalone S3 SF1 for eight files/8,661,245 rows/340,201,375 bytes, and Dataset
  readback for eight samples. These used older backend states and do not close
  the current physical acceptance. The S3 result also predates the final
  zero-retry acceptance setup.
- For the next run, build a wheel from the exact committed loader tree, save
  its SHA-256 and source identity, and install it into a dedicated run-local
  environment. Inspect any new worktree changes first. Recheck all three paths
  against the new CROWDB image, including the 0.1.2 filename behavior.

## 7. Remaining changes now captured

The user subsequently requested committing the edits previously kept separate
from recovery work. They are now captured without overwriting their content:

- CROWDB: `container/oci-image/README.md`, `clean.py`,
  `docker_local_cleanup.py`, `host_cleanup.py`, `tests/cleanup_cli_test.py`,
  `tests/docker_local_test.py`, `tests/host_cleanup_test.py`, untracked
  `docker_garbage.py` and `tests/docker_garbage_test.py` under that directory;
  `container/single-node-container/README.md`;
  `doc/design/deploy/design-crowdb-oci-image.md`; and `pixi.toml`.
- Loader: `CHANGELOG.md`, `README.md`, `pyproject.toml`,
  `src/crowdb_tpc_loader/__init__.py`, `src/crowdb_tpc_loader/loader.py`, and
  `tests/unit/test_commits.py`.
- CROWDB's cleanup change preserves running recorded clusters by default;
  explicit `--cluster`/`--all` can stop selected clusters. Unscoped cleanup also
  collects unused Docker resources, while retaining protected receipts and data
  according to `--keep-data`. Its affected OCI tooling suite passed all 150
  tests before commit. No cleanup command was run against the physical fixtures
  as part of this commit validation.
- Both `/cjdata/cpp/crowdb-web` and `/cjdata/cpp/buzzcrow.github.io` still have
  an untracked `.aws` path. Neither was read or staged. There were no other
  pending website/journal changes when inspected.

Capture a fresh `git status` before further edits and preserve any new user work.
The original physical-run hash receipts and other evidence remain deleted.

## 8. Remaining execution, in dependency order

- [x] **Implementation checkpoint**: commit the recovery and election changes
  locally; push the completed loader implementation. Files: the commit scopes
  in sections 2, 4, 5, and 6.
- [x] **Handoff and interruption record**: preserve this state outside the
  deleted runtime directory; defer fresh testing to the next session as the
  user requested. Files: this document.
- [x] **Remaining changes checkpoint**: commit the OCI cleanup behavior and
  handoff in CROWDB, and the 0.1.2 filename changes in the loader, after local
  affected checks. Files: the paths in section 7 and this document.
- [ ] **Fresh preparation**: inventory old owned resources on both hosts,
  preserve user edits, create a new run ID, reconstruct the harness, build one
  frozen-source OCI serially, import/verify it on both hosts, and establish
  fresh baseline data and identities. Files: a new run's `run.json`, harness
  source, image receipts, and P00-P03 results.
- [ ] **First-divergence regression**: complete a fresh native Iceberg SF1 load
  with full file checksums and bidirectional SQL verification. Observe Root
  leases, elections, backlog, process epochs, and memory. On failure, print the
  exact error, capture the first divergence, and stop dependent work.
  Files: new loader reports, operation ledger, and service logs.
- [ ] **Scale and protocol acceptance**: pass isolated native 512-record and
  4096-record cases across all five owners with full readback; verify fresh S3
  SF1 with zero mutation retries and generated Dataset publication/readback.
  Files: P04/P05 ledgers and loader reports.
- [ ] **Thirty-minute workload**: run P06-P08 for 1,800 continuous active
  seconds after preparation passes. Files: durable native/protocol ledgers,
  complete metrics, and final data audit.
- [ ] **Recovery matrix**: execute P09-P16 using the new soak data, including
  retained-volume recreation, the unresponsive child, follower/leader lifecycle,
  two-node B outage, three-node A quorum loss, allocation safe retry, backlog,
  full cold restart, and final occupancy audit. Files: per-case receipts and
  fixed-checkpoint/data verification.
- [ ] **Closing CI**: rerun the original failures and affected protocol suites
  against current inputs. Files: CI artifacts and final result matrix.
- [ ] **Close the run**: capture failures or the final pass, verify owned-only
  cleanup, commit coherent follow-up fixes/results as authorized, and report
  unfinished coverage honestly. Do not push CROWDB. Files: `results.json`,
  `cleanup.json`, and `summary.md` in the new run directory.

## 9. Workload and failure contracts to retain

- Native scale: 512 then at least 4096 records, each 64 KiB, batches of 16,
  with the original five-second operation deadline. Cover all five Serving
  owners using actual catalog ranges.
- Exclude native key first-byte namespaces reserved by S3, Iceberg, and
  Dataset (`S`, `I`, and `D`). A former harness used these prefixes and
  contaminated protocol scans. Exact owned-key retirement was recorded before
  deletion; do not reuse that harness or delete whole protocol prefixes.
- Intended native soak: one batch in flight, 80/20 put/delete mix with 16 exact
  reads per cycle, durable intent/acknowledgment records, bounded live data, and
  a final audit of all 4096 keys. Declare actual pacing and concurrency before
  starting. Build/deploy/idle time does not count toward 30 minutes.
- Concurrent protocols: bounded S3 put/get/delete plus absence verification;
  Iceberg property commit/load/cleanup; Dataset sample publication and complete
  field verification. Keep mutation attempts single where required. An unknown
  outcome is audited read-only, never retried merely to obtain success.
- Observe all 35 service PIDs and OOM state; collect metrics at least every
  five seconds. Readiness observations must postdate the current process start
  and be at most three seconds old. Require at least 30% free test media.
- Preserve independent 180-second service-recovery, catalog-takeover, and
  per-group fixed-checkpoint catch-up gates. Do not replace them with one
  whole-phase duration, increase timeouts, or weaken assertions.
- Any source/harness repair during the soak requires a complete new 30-minute
  run after the repair. Earlier 70-second and 148.9-second runs stopped on
  harness freshness errors and do not count.
- Run builds, tests, lint, and executables through `pixi run`, including remote
  execution. Serialize shared-output builds and freeze production source during
  OCI build. Add no new hot-path lock or unsafe exception without following
  project rules.

## 10. Closing CI scope and useful entry points

Original user-reported runs:

- [CROWDB run 41003611925](https://github.com/buzzcrow/crowdb/actions/runs/41003611925).
- [CROWDB run 41128190276](https://github.com/buzzcrow/crowdb/actions/runs/41128190276).

The pending closing queue covered console native operations, deployment
services, deployment reference, Iceberg GC cleanup, and full S3 acceptance.
It did not dispatch on the latest election-fix image before interruption.
Reconstruct commands from current Pixi tasks and `e2e-test/README.md`; old
controller files are gone. Set `CROWDB_E2E_PRINT_SERVICE_ERRORS=1` for diagnosis
and retain complete client/service errors. Container CI does not replace the
physical run.

Useful files for selecting focused validation:

- Unit/integration: `lib/crowdb-kv/tests/election_test/vote_test.rs`,
  `lib/crowdb-kv/tests/group_test/`,
  `lib/crowdb-chunk-client/tests/chunk_reader_test/`, and
  `lib/crowdb-chunk-client/tests/common/mirror_locality.rs`.
- Service/transport: `app/crowdb-kv-server/`,
  `app/crowdb-chunk-kv-server/`, `app/crowdb-chunkdb/tests/`, and
  `container/crowdb-monitor/tests/`.
- Container/protocol: `e2e-test/tests/console/`,
  `e2e-test/tests/deployment/`, `e2e-test/tests/access_iceberg/`, and the
  S3 acceptance entries documented in the E2E README.
- Design contracts: `doc/design/kv/design-crowdb-kv-leader-election.md`,
  `doc/design/chunkio/design-crowdb-chunkio-reader.md`, and
  `doc/dev/physical-node-test-plan.md`.

Historical artifact root, now deleted:
`.crowdb-runtime/artifacts/physical-nodes/physical-20261031T024347Z/`.
Its former controller progression, `CURRENT-STATE.json`, `loader/` reports,
`mirror-locality-*` evidence, and `leader-vote-lease-*` evidence cannot be used
as executable inputs or linked as surviving proof. The commits and this handoff
are the starting point for the next test session.
