<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R188: console — Group 0 authority and deployment configuration cleanup

## Status

Deferred until the R187 single-node Docker image is locally verified. This work
must not delay the preview image or turn Group 0 into a Docker deployment
registry.

## Problem

The unreleased `ConsoleConfig` in
[`design-crowdb-console.md`](../design/console/design-crowdb-console.md)
currently mixes cluster topology with host, SSH, binary, port, PID, and local
launch information. CLI and bare-metal Web can write the local file before a
Group 0 mutation succeeds, and some reads and restart paths still accept that
file or a monitor cache as topology authority. A second console can therefore
observe a different cluster, while an outage can resurrect stale topology.

R187's Docker monitor already owns container process supervision; Group 0 owns
CROWDB system metadata, not container IDs, images, mounts, PIDs, restart
generations, or machine-local launch policy. Completing a cross-mode console
rewrite is not a prerequisite for packaging that monitor and the single-node
profile. The remaining boundary cleanup belongs in this separate requirement.

## Solution

1. Keep Group 0 as the durable authority for CROWDB hardware hierarchy,
   ownership and binding maps, KV store/group/replica metadata, and service
   registration. Do not add Docker or bare-metal process deployment records to
   Group 0. Docker process state comes from `crowdb-monitor`; bare-metal launch
   policy remains local. Deployment mode changes which lifecycle and hardware
   controls are allowed, not the meaning of Group 0 records.
2. Replace the mixed `ConsoleConfig` persistence in
   `crowdb-console-shared::config`, `crowdb-web`, and `crowdb-cli` with a
   versioned Web process configuration and a separate bare-metal launch-only
   registry. Finish wiring the existing `LaunchRegistry` parser to actual
   bare-metal deploy/restart operations; remove the unreleased mixed
   parser/writer, topology fields, restore path, fixtures, and fallback rather
   than adding a compatibility reader. Retain SSH credential references,
   binary/config paths, workspace, and auto-start policy locally; never persist
   inline secrets or runtime PID as topology. Docker Web rejects a launch
   registry and keeps its monitor-owned process path.
3. Unify CLI and bare-metal Web hardware mutations through Group 0-backed
   operations in `crowdb-console-shared::ops::hardware`. Confirm writes before
   updating a read model; preserve conflicts and uncertain results. Docker Web
   continues to reject hardware and process mutations.
4. Complete the common Group 0-backed logical store/group/replica flow in
   `crowdb-console-shared::ops::kv_logical` for CLI and both Web modes. Reconcile
   lost responses by reading confirmed authority, test multi-node fan-out and
   rollback, and remove local logical-topology commits. A failed node-side
   deletion must not erase surviving Group 0 membership.
5. Replace config-backed monitor refresh, KV endpoint fallback, and
   physical/deployment topology reads with Group 0 membership and live service
   registration. A missing or ambiguous live endpoint fails unavailable; a
   stopped service may still have local launch policy but is not reported as
   live. Neither a local launch registry nor a monitor cache is an authority
   fallback during a Group 0 outage.
6. Keep pre-Group-0 bootstrap intent separate. After creation, verify every
   committed hardware and logical record and delete the local topology copy.
   Persist enough bootstrap identity to resume an interrupted transfer, prove
   already committed content, and reject conflict. Destroy/clean must use
   confirmed Group 0 state. If nonmember KV processes were launched before
   Group 0 exists, propagate usable Group 0 seed hints after initialization
   before treating their registration as live; seed hints are not topology.
7. Audit the S3 mini-cluster's local `console.toml` and restart path under the
   same authority boundary. Retain only launch inputs and bootstrap seeds
   locally after Group 0 cutover; do not replay a local topology copy.
8. Migrate the verified bare-metal deployment and operations material from the
   old `doc/user-manual/user-guide.md` into
   `doc/user-manual/bare-metal-user-guide.md`, organized by KV cluster, chunk
   layer, and data access servers. State that bare-metal is not yet
   production-ready. Remove the old combined guide only after its supported
   material and links are migrated; Docker documentation remains independent.

## Dependencies

- R187 provides the working single-node Docker profile, monitor-owned process
  state, managed Web baseline, and Group 0-backed system metadata. R187 image
  verification does not depend on this cross-mode cleanup.
- The existing Group 0 schema and `crowdb-kv-client` service APIs remain the
  authority. If a live registration is absent, operations fail unavailable or
  wait for registration; local launch policy never substitutes for it.
- The old mixed console file is unreleased. No on-disk compatibility promise or
  migration tool is required, but bootstrap replay must not overwrite a
  confirmed initialized cluster.

## Acceptance

- Given a Docker process restart and a bare-metal process restart, when runtime
  state is queried, assert Docker PID/restart state comes from the monitor and
  bare-metal launch policy stays local, while neither appears as Group 0
  topology. Invariant: deployment state is not sysdata. Integration test.
- Given a mixed legacy config and valid/invalid launch registries, when Web and
  CLI start, assert only versioned process and launch inputs are accepted, no
  local topology is restored, Docker rejects the registry, and inline secrets
  or topology fields fail validation. Invariant: separated configuration.
  Integration test.
- Given two bare-metal consoles and one ready Group 0, when each mutates racks,
  nodes, disk groups, or disks and a write conflicts or loses its response,
  assert both read one confirmed result and neither commits a local-first
  topology change. Invariant: hardware authority. Integration test.
- Given CLI, Docker Web, and bare-metal Web with the same Group 0, when each
  performs authenticated logical store/group/replica operations, assert one
  shared result, correct fan-out/rollback, and no local logical copy.
  Invariant: common logical authority. Integration test.
- Given missing, duplicated, or expired registrations and then a Group 0
  outage, when topology, endpoint, or deployment status is read, assert no
  stale local endpoint or monitor snapshot is presented as authoritative.
  Invariant: fail-closed discovery. Integration test.
- Given a crash before and after each bootstrap commit and before local
  deletion, when startup resumes, assert it proves identity and committed
  content, writes only safely missing records, and rejects conflict without
  overwriting Group 0. Invariant: replay-safe cutover. Integration test.
- Given nonmember KV processes launched before Group 0 initialization, when
  Group 0 is created and seed hints are propagated, assert each process
  registers exactly one live node identity before logical operations use it.
  Invariant: registration readiness. E2E test.
- Given a persisted S3 mini-cluster and a Group 0 outage, when it restarts or
  tears down, assert local launch data cannot recreate or mask old cluster
  topology. Invariant: no secondary authority. Integration test.
- Given the two deployment guides and a reader following bare-metal steps,
  when the reader deploys KV, chunk services, and Iceberg or S3 access servers,
  assert each layer has a verified setup and health check, the non-production
  boundary is explicit, and no link targets the removed combined guide.
  Invariant: deployment guidance follows its implementation. E2E test.

Required gates:

- `pixi run clean-env && pixi run test-console`
- `pixi run clean-env && pixi run test-console-ui`
- `pixi run rs-fmt-check`
- `pixi run rs-lint`
