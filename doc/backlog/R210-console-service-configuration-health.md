<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R210: console — unified node service configuration and health

Problem: Add Node currently treats Paxos-KV and DiskDB as primary services and
the other four services as an automatic follow-up. Their fields, ordering,
names, listener types, and health states therefore differ in one dialog. The
operator cannot configure the six services as one node plan or choose each
service independently. Several internal services are shown with HTTP listeners
even though only Access Server is externally exposed. DiskIO is reported as
Unknown even when its process is running. The current service identifiers also
mix user-facing names with internal names (`kv`, `CrowDB Storage`, and
`ChunkDB`).

Solution: Make the Add Node dialog a single service-plan editor. It presents
the services in this order: `crowdb-access-server`, `crowdb-chunk-kv`,
`crowdb-chunk-db`, `crowdb-disk-db`, `crowdb-disk-io`, and
`crowdb-paxos-kv`. Each card has an enable switch and only the listeners owned
by that service. Access Server owns HTTP/S3; all other service health probes
use internal FlatBuffer RPC. HTTP management endpoints may remain for local
operations but are not health authority.

Internal service kinds use `paxos-kv`, `chunk-kv`, `chunkdb`, `diskdb`,
`diskio`, and `access-server`. Persisted and wire service types use the same
names; the old `kv` service type is not accepted. User-facing labels always
use the `crowdb-*` names above.

Work items:

1. Update `lib/crowdb-console-shared/src/config.rs` and all service matching
   code to use `ServiceType::PaxosKv` and serialize `paxos-kv`.
2. Update `app/crowdb-web/src/services/plans.rs`, deployment defaults, and
   observation routes to use the unified service order and names.
3. Add RPC health probes for DiskDB and DiskIO, then use the probe result for
   `ServerSummary.health`; process liveness is the fallback during probe
   startup.
4. Refactor `app/crowdb-web/ui/src/components/dialogs/AddNodeDialog.tsx` so
   all six services share one card component, one submit action, and service
   specific listener fields.
5. Keep waiting, disabled, deploying, failed, and deployed states in the
   persisted service plan. Closing the dialog must not require a second
   confirmation or lose an unfinished plan.

Dependencies: This changes the console configuration and API contract. Update
the console UI tests, service-plan tests, lifecycle fixtures, and any native
configuration fixtures that serialize `ServiceType`. Existing persisted
workspaces containing `kv` require an explicit migration or a clean workspace
before this requirement is enabled.

Acceptance:

- With an empty workspace, open Add Node and select any subset of six services;
  submit once; the node and exactly the selected service plans are persisted.
  **E2E test**
- Configure each service card and assert that only its valid listener fields
  are submitted: Access Server has HTTP/S3, while internal services have RPC
  health configuration. **E2E test**
- Disable a service before submission and assert that its plan is `disabled`
  and no deployment request is sent. **Integration test**
- Restart or stop DiskDB and DiskIO and assert their health changes through
  the RPC probe without changing Paxos-KV health. **Integration test**
- Serialize a `ServiceType::PaxosKv` entry and assert that the wire value is
  `paxos-kv`; parsing `kv` fails. **Unit test**
- Submit a plan with a dependency that is not ready and assert that the dialog
  closes once, the service remains `waiting`, and a later refresh deploys it
  without another confirmation. **E2E test**

Open Questions:

- Should local HTTP management listeners remain configurable on internal
  services, or be fixed to loopback and hidden from the node dialog? Hiding
  them reduces operator choices; exposing them helps local debugging.

Validation commands:

- `pixi run -- cargo fmt --all -- --check`
- `pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `pixi run test-console`
- `pixi run test-console-ui`
