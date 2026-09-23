<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R182: access server / Iceberg — Atomic table commits and recovery

## Problem

Iceberg create and update operations validate requirements against one current
metadata state, apply an ordered update list, write a new immutable metadata file,
and atomically select it. A partial implementation that ignores an unknown update,
publishes files before validation, repeats a successful mutation after response
loss, or treats compare-exchange failure as generic server error would violate the
REST and table specifications.

R181 deliberately leaves `TableHead` publication to one owner. This requirement
implements create, staged create, v1/v2/v3 updates and upgrades, deterministic
conflicts, idempotency, and crash recovery without a table-wide lock.

## Solution

- **COMMIT-I1 — One input generation:** all requirements and updates in a request
  are evaluated against one retained `TableHead` and metadata generation.
- **COMMIT-I2 — Ordered atomic update:** either the complete ordered update list is
  selected by one head CAS or none of it is visible.
- **COMMIT-I3 — Format fidelity:** every requirement, update, inheritance rule, and
  upgrade follows the selected v1, v2, or v3 specification; unknown or disabled
  variants fail before candidate publication.
- **COMMIT-I4 — Retry identity:** one request identity and digest has one durable
  final result across instances and response loss.
- **COMMIT-I5 — Orphan safety:** a CAS-losing or abandoned candidate is unreachable
  and only becomes an R183 reclamation candidate.

1. Add `commit/requirement.rs`, `update.rs`, `evaluator.rs`, `operation.rs`,
   `create.rs`, and `repository.rs`. Wire types decode into bounded domain enums;
   no unknown tagged union is ignored or passed through as JSON.
2. Implement immediate create and staged create. Reserve the table name under the
   namespace fence, validate initial schema/spec/order/properties and target format,
   persist immutable metadata, then publish one initial `TableHead`. Staged state is
   durable, expires, and can be completed only by its bound commit identity.
   Use R179's durable reservation before parent admission CAS, including final
   staged-create publication. Expiry initiates phase-fenced abort/recovery; it
   never removes a reservation with an unknown publication outcome.
3. For update, retain one head revision and canonical metadata input; validate all
   requirements; apply updates in request order to a bounded builder; revalidate
   the complete output; serialize one canonical standard metadata JSON file; then
   compare-exchange the head from the retained revision to generation plus one.
4. Cover the complete requirement and update union needed by the backed-up OpenAPI
   and table specification for v1, v2, and v3. This includes schemas and defaults,
   partition specs, sort orders, properties, locations, snapshots and references,
   statistics, sequence and row-ID inheritance, row lineage, delete semantics,
   encryption-key metadata, and version-specific fields.
5. Support any explicit higher supported target, including direct v1-to-v3.
   Expand direct upgrades into v1-to-v2 and v2-to-v3 internal transitions; validate
   the source and preserve each intermediate version's rules before validating
   the result under the target version. Reject downgrades, unsupported targets,
   and any upgrade that would lose active metadata semantics.
6. Classify a failed requirement, stale generation, name/lifecycle fence, duplicate
   create, unsupported operation, malformed metadata, and head CAS loss into their
   precise REST conflict or validation response. A CAS loser never retries against
   a new generation inside the same request.
7. Persist an `OperationRecord` before mutation with request identity, canonical
   digest, table/name context, input generation, phase, candidate FileId, and final
   response. Phase transitions use CAS. Same identity plus a different digest
   conflicts; same identity plus the same digest resumes or returns the result.
   Consume R178's standard optional HTTP key, system binding, retention, final 4xx
   replay, and non-final 5xx rules. A retired catalog result cannot be replayed as
   a resource response or rebound to the current domain.
8. Bound request bytes, update and requirement counts, metadata input/output bytes,
   projection work, serialization buffers, candidate writes, and concurrent commits
   independently. Stream large canonical JSON where possible and fail admission
   before exceeding a hard cap.

## Dependencies

- Depends on R177 through R181 for namespace fences, immutable files, metadata
  validation, TableHead, REST error types, and request identity.
- Produces selected metadata generations, candidate/orphan records, operation
  histories, and upgrade results consumed by R183 through R185.
- R183 is not required to make CAS losers safe; before it lands candidates may leak
  storage but remain unreachable.
- R185 may accelerate input loading and evaluation, but every mutation still
  validates the authoritative head revision before publication.

## Acceptance

- Given two commits based on one generation, when they publish concurrently, assert
  one head CAS selects one complete output, the loser receives the precise conflict,
  and no partial update is visible. Invariants: COMMIT-I1 and COMMIT-I2. Integration test.
- Given every declared requirement and update for v1, v2, and v3 plus unknown tagged
  variants, when evaluated against reference fixtures, assert supported results
  match the spec and unknown or disabled input fails before candidate publication.
  Invariant: COMMIT-I3. Unit test.
- Given valid and invalid v1-to-v2, v2-to-v3 and direct v1-to-v3 upgrades, when committed, assert all
  transition defaults and inheritance rules are applied, invalid or lossy upgrades
  fail, direct upgrades apply both internal transitions, and downgrade or unsupported-version
  requests do not mutate the head. Invariant:
  COMMIT-I3. Integration test.
- Given crashes at every create, staged-create, candidate-write, operation-phase,
  and head-CAS boundary, when another server resumes with the same request identity,
  assert one table/generation/result is visible and different input under that
  identity conflicts. Invariant: COMMIT-I4. E2E test.
- Given parent drop racing immediate or staged-create publication and expiration,
  when recovery resolves uncertain CAS outcomes, assert reservations protect every
  publishable child and aborted publishers cannot later expose a table beneath a
  tombstone. Invariants: COMMIT-I2 and COMMIT-I4. Integration test.
- Given a failed requirement, stale generation, duplicate name, lifecycle fence,
  malformed metadata, unsupported update, and CAS loss, when official clients commit,
  assert each receives the standard status and error type and no case is collapsed
  into a successful no-op. Invariants: COMMIT-I2 and COMMIT-I3. E2E test.
- Given a candidate whose publisher loses or crashes, when load and list execute
  before reclamation, assert the candidate is unreachable and the current head
  still resolves to complete canonical bytes. Invariant: COMMIT-I5. Integration test.
- Given requests at every byte/count limit and over each hard cap, when commit
  admission and evaluation run, assert accepted resource use stays bounded and
  rejected requests leave no operation or candidate leak. Invariant: COMMIT-I2.
  Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
