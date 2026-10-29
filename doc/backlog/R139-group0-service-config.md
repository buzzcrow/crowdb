<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R139: config — Deployment profiles and Group-0 service configuration

## Status

Deferred until the file-backed service configuration contract is established.

## Problem

Service configuration is local to each node. Changing a cluster-wide policy
requires editing files or launch arguments on every host, and operators cannot
see which revision each process has accepted. Blind live reload is unsafe
because some fields can be applied dynamically while others define listener,
thread, storage, or durability state and require a restart.

Single-container deployment, multi-node clusters and test fixtures currently
select tuning through different paths: service TOML files, command-line presets
such as KV's `e2e` election profile, and deployment request overrides such as
DiskDB's keepalive interval. A single container and a cluster require different
settings; clusters also need low- and high-performance profiles. These choices
must be explicit and reproducible without building separate images or scattering
special timing values through test clients.

The group-0 architecture in
`doc/design/kv/design-crowdb-kv-group0.md` already provides durable replicated
sysdata and service registration, but it has no versioned configuration model,
target selection, acknowledgement state, or restart contract.

## Solution

Provide named, versioned configuration profiles through the normal deployment
and service configuration contract. Resolve the distributed record and rollout
details below before implementation.

Profile invariants:

- **I1 — Explicit selection:** distinguish single-container and cluster profiles;
  cluster profiles include low- and high-performance settings. Selection is
  recorded and remains stable across restart and cluster expansion.
- **I2 — One runtime:** profiles configure the same release image and binaries.
  Tests may select a documented test tuning profile or explicit timing overrides
  using the same schema, validation and startup mechanism as operators.
- **I3 — Complete validation:** validate the resolved configuration and its
  cross-field constraints before starting or applying changes. Performance
  settings never relax placement, quorum, lease safety or durability guarantees.
- **I4 — Observable configuration:** expose the profile name/version, resolved
  values and accepted/applied revision for each service; errors identify the
  conflicting setting and target.

Numbered work items:

1. Define single-container and cluster profiles, with low- and high-performance
   cluster choices. Document the intended resource envelope and service settings
   for each rather than treating a higher number as universally faster.
2. Unify profile selection at container/cluster creation and service startup;
   support typed caller overrides and persist the resolved choice. Cover KV
   election/heartbeat/lease settings, DiskDB keepalive/synchronization intervals,
   and applicable worker, concurrency and batching limits. Test startup must be
   able to request, for example, a 3-second DiskDB cadence through this contract.
3. Define deterministic precedence between profile defaults, cluster defaults,
   service/node/instance overrides and explicitly supplied startup values.
   Existing CLI presets and deployment fields must resolve through this contract
   rather than silently overwrite one another. Distinguish heartbeat cadence
   from metadata synchronization cadence and specify their actual consumers.
4. Keep bootstrap selection available before Group 0 exists. Persist profile
   identity and effective local configuration, then reconcile with the validated
   distributed configuration when authority is available. Joining nodes inherit
   the cluster selection unless an explicit validated override applies.
5. Define the distributed configuration and rollout model:

- Define a versioned group-0 record for cluster defaults plus service-,
   instance-, and node-scoped overrides, with deterministic precedence.
- Reuse the file-backed schemas as typed payloads or define an evolvable
   envelope that preserves unknown fields during mixed-version rollout.
- Classify every field as dynamic or restart-required and make each service
   validate a complete candidate before atomically applying any dynamic subset.
- Publish per-instance observed, accepted, applied, and restart-required
   revisions through the service registry so operators can detect drift.
- Define bootstrap and outage behavior: local file remains the seed and
   fallback until group 0 is reachable; the last validated group-0 snapshot is
   cached locally for restart.
- Add an authenticated administrative write path with compare-and-set revision
   fencing, validation, staged rollout, and rollback semantics.

Edge cases requiring design include an older binary receiving unknown fields,
group-0 loss during rollout, a process restart between acceptance and apply,
conflicting scope overrides, invalid cross-field combinations, and a static
field changed while a service is live.

## Dependencies

- Depends on the unified file-backed server configuration contract.
- Depends on group-0 service registry and watch/notify support.
- A first version may poll if watch delivery is unavailable; correctness must
  not depend on notifications.

## Acceptance

- Given the same release image, starting with a single-container profile and
  with each cluster performance profile produces the documented effective
  settings and records the selected version. Invariants: I1, I2, I4. E2E test.
- Given profile defaults plus scoped and explicit overrides, resolution produces
  one deterministic validated configuration; an unknown profile, zero interval
  or unsafe election/lease combination fails before partial startup. Invariants:
  I1, I3. Unit test.
- Given a test tuning selection and a 3-second DiskDB cadence override, normal
  deployment creates the effective configuration and the actual timer consumes
  that value; no test server build or hidden fixture mutation is used.
  Invariants: I2, I4. Integration test.
- Given a selected cluster profile, service restart and admission of a new node
  preserve or inherit the profile and report effective overrides accurately.
  Invariants: I1, I4. E2E test.
- Given heartbeat and metadata synchronization settings differ, each runtime
  timer uses its documented setting; updates follow their dynamic or
  restart-required classification. Invariants: I3, I4. Integration test.

- Given cluster, service, node, and instance records, resolving a candidate is
  deterministic and produces the documented precedence. Invariant: every
  instance resolves the same revision from the same records. Unit test.
- Given a candidate containing unknown fields during a mixed-version rollout,
  the old service reports unsupported configuration without corrupting or
  partially applying it. Invariant: candidate validation is atomic. E2E test.
- Given dynamic and restart-required changes in one revision, the service
  applies only the validated dynamic subset and reports the static subset as
  pending restart. Invariant: runtime state and reported revision agree.
  Integration test.
- Given group 0 becomes unavailable, a running service retains its last applied
  config and a restarting service uses its last validated cache or local file
  according to the bootstrap contract. Invariant: control-plane loss does not
  silently reset tunables to compiled defaults. E2E test.
- Given concurrent publishers, compare-and-set revision fencing permits one
  winner and produces an auditable rejected update. Invariant: published
  revisions form one ordered history. Integration test.
- Given a staged rollout and an unhealthy acknowledgement, rollback restores a
  prior valid revision and all targeted instances converge or report a precise
  blocker. Invariant: rollout status exposes all configuration drift. E2E test.

Required gates:

- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
- `pixi run test-core`
- `pixi run test-storage`
- `pixi run clean --runtime-only && pixi run test-container-e2e --scope console.operations`
- `pixi run test-suite`
