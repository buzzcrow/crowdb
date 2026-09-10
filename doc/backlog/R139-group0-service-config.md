<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R139: config — Group-0 distributed service configuration

## Status

Deferred until the file-backed service configuration contract is established.

## Problem

Service configuration is local to each node. Changing a cluster-wide policy
requires editing files or launch arguments on every host, and operators cannot
see which revision each process has accepted. Blind live reload is unsafe
because some fields can be applied dynamically while others define listener,
thread, storage, or durability state and require a restart.

The group-0 architecture in
`doc/design/kv/design-crowdb-kv-group0.md` already provides durable replicated
sysdata and service registration, but it has no versioned configuration model,
target selection, acknowledgement state, or restart contract.

## Solution

No clear solution yet - deferred to design. Resolve these decisions before
implementation:

1. Define a versioned group-0 record for cluster defaults plus service-,
   instance-, and node-scoped overrides, with deterministic precedence.
2. Reuse the file-backed schemas as typed payloads or define an evolvable
   envelope that preserves unknown fields during mixed-version rollout.
3. Classify every field as dynamic or restart-required and make each service
   validate a complete candidate before atomically applying any dynamic subset.
4. Publish per-instance observed, accepted, applied, and restart-required
   revisions through the service registry so operators can detect drift.
5. Define bootstrap and outage behavior: local file remains the seed and
   fallback until group 0 is reachable; the last validated group-0 snapshot is
   cached locally for restart.
6. Add an authenticated administrative write path with compare-and-set revision
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
- `pixi run test-kv-client`
- `pixi run clean-env && pixi run test-server`
- `pixi run test-suite`
