<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R211: console — Independent Access Server health listener

### Problem

The console currently models `crowdb-access-server` with an Iceberg HTTP port
and an S3 HTTP port. Its readiness endpoint is served by the S3 listener, so a
health probe is coupled to the public S3 data plane. This makes the health
listener impossible to firewall or expose independently and makes the service
status misleading when S3 is intentionally isolated. The Add Node dialog also
does not let an operator configure a separate health port, while the server
observation path reports Access Server health as `Unknown`.

### Solution

Access Server has three distinct listeners: Iceberg HTTP, S3 HTTP, and an
internal health HTTP listener. The health listener serves liveness and
readiness responses without accepting S3 data requests. Readiness reflects the
same dependency state as the Access Server process and is the source for the
console health badge.

1. Extend the access-server configuration and launch contract in
   `app/crowdb-access-server/src/config.rs` and
   `app/crowdb-access-server/src/main.rs` with a dedicated health listen
   address and `/_crowdb/health/{live,ready}` routes (without the space).
2. Allocate and return a distinct `health_port` in
   `app/crowdb-web/src/services/defaults.rs`; reserve it with the other
   listeners and pass it from `app/crowdb-web/src/services/deployment/launch.rs`.
3. Add the health port to the Access Server card and deployment overrides in
   `app/crowdb-web/ui/src/components/dialogs/AddNodeDialog.tsx` and the shared
   service types. The card must show Iceberg, S3, and Health separately.
4. Update `app/crowdb-web/src/services/observation.rs` so Access Server health
   is derived from the independent readiness listener and is rendered as
   Healthy, Failed, or Unknown according to the probe result.
5. Preserve restart and persisted launch behavior: an existing deployment
   without a health port is reported as needing reconciliation rather than
   silently reusing the S3 port.

### Dependencies

This requirement extends the six-service configuration and health contract in
[R210](R210-console-service-configuration-health.md). If R210 is not yet
implemented, the new port and health fields must still be accepted by the
deployment API and remain visible in the single Add Node dialog.

### Acceptance

- Given an Access Server deployment, starting it binds three distinct ports;
  requesting `/_crowdb/health/ready` (without the space) on the health port
  returns readiness, while the S3 port remains dedicated to S3 requests.
  **Integration test**
- Given occupied S3 and Iceberg ports, deployment defaults allocate a distinct
  available health port and reject duplicate listener values. **Unit test**
- Given the Add Node dialog, the Access Server card displays and persists
  separate Iceberg, S3, and Health values, and disabling the service disables
  all three listeners together. **E2E test**
- Given a running, ready Access Server, the server list reports Healthy; when
  the health listener is unreachable it reports Failed or Unknown according to
  the probe result and never infers health from S3 traffic alone. **Integration test**
- Given a persisted launch record with no health listener, restart does not
  bind the S3 port as a fallback and reports the deployment as requiring
  reconciliation. **Unit test**

Run the focused gates with:

```text
pixi run cargo fmt --all -- --check
pixi run cargo test -p crowdb-access-server
pixi run cargo test -p crowdb-web
pixi run cargo clippy -p crowdb-access-server -p crowdb-web --all-targets -- -D warnings
```

### Open Questions

- Should the health listener be bound only to the node's private address, or
  should the operator be allowed to choose a separate bind address as well as
  its port? A private-only bind reduces exposure; a configurable address helps
  external monitoring.
