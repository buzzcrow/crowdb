<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R212: console — Reliable node creation and Cluster scope

### Problem

The Add Node dialog now presents the six services and their listeners, but a
real node creation can still fail after the operator submits the form. The
failure can leave an ambiguous partial state: the node may be registered while
one service deployment failed, or the operator may receive no actionable
reason. Separately, the Cluster center panel still renders Chunk ownership for
the selected rack or node before a ChunkDB exists. Chunk ownership is managed
from the Chunk tab and does not belong in the Cluster topology workflow. The
related UI consolidation item is recorded in `doc/working/ui-todo.md`.

### Solution

Treat node registration and service deployment as two explicit phases of one
operator workflow. Registration must either succeed with a durable node record
or report the backend error without starting service deployment. After a node
exists, each selected service is deployed independently; every failure is
shown with its service name, listener values, and retry action. A retry must
reuse the existing node and must never create a duplicate node. The dialog
must remain a single configuration and progress surface.

The Cluster center panel renders topology and service information only. Chunk
ownership details are rendered in the Chunk tab after a real ChunkDB or
ownership target exists, with a concise ownership hint on the corresponding
left tree item. Selecting a rack, node, or datacenter in Cluster must not
trigger ownership probes.

Every service selected in Add Node is part of the durable node plan even when
its prerequisites are not ready. The UI must distinguish `queued/waiting`
from `disabled`, `failed`, and `deployed`; a queued service is not silently
dropped and is deployed automatically once its dependency condition is met.
Only `paxos-kv` (shown compactly as `PKV`) may deploy immediately on a new
node. DiskDB and every other selected service wait for Group 0 readiness; the
planner checks the dependency again every 10 seconds and continues without a
second confirmation.

ChunkDB also waits until the selected DiskIO services have published live
disk-group ownership. Starting ChunkDB before that registry state exists can
leave its allocator with no usable owner and causes downstream Chunk-KV
bootstrap to fail; the UI must keep this dependency visible as a queued state.

1. Trace and harden the request sequencing in
   `app/crowdb-web/ui/src/components/dialogs/AddNodeDialog.tsx`,
   `app/crowdb-web/ui/src/services/useNodeServicePlans.ts`, and the node and
   service deployment routes so partial results are durable and retryable.
2. Return structured, service-scoped errors from the deployment API and render
   them in the dialog and node tree without losing the selected ports.
3. Move the `OwnershipPanel` selection boundary out of the Cluster center
   view and add the Chunk-tab tree hint described by `doc/working/ui-todo.md`.
4. Add focused browser and route tests for successful creation, registration
   failure, service failure, retry without duplication, and the Cluster/Chunk
   ownership boundary.

### Dependencies

This builds on [R210](R210-console-service-configuration-health.md) and
[R211](R211-console-access-health-listener.md). If either service-listener
change is incomplete, node creation must still preserve the selected service
configuration and report the missing listener explicitly.

### Acceptance

- Given valid rack, node, host, and service values, submitting Add Node creates
  exactly one node and records selected services with their configured ports.
  **E2E test**
- Given a node-registration error, the dialog stays open, shows the backend
  error, and sends no service-deployment requests. **Integration test**
- Given one failed service deployment after registration, the node remains
  visible, the failed service is named with its reason, and retry sends no
  second node-registration request. **E2E test**
- Given selected and disabled services, the persisted plan marks disabled
  services disabled and retries only pending or failed selected services.
  **Unit test**
- Given selected services whose Group 0, metadata group, or disk-group
  prerequisite is unavailable, the plan persists every selected service as
  queued/waiting with an actionable dependency reason; after the prerequisite
  becomes ready, the service is deployed without reopening Add Node.
  **Integration test**
- Given a new node with all six services selected, only PKV may start before
  Group 0 exists; DiskDB and the remaining services stay queued, are visible
  in the progress dialog, and are retried every 10 seconds until ready.
  **Unit and E2E test**
- Given a rack, node, or datacenter selection in Cluster before ChunkDB exists,
  the center panel contains no Chunk ownership probe or ownership cards.
  **E2E test**
- Given a real ChunkDB ownership target in the Chunk tab, the left tree item
  shows the ownership hint and selecting it renders the ownership details.
  **E2E test**

Run the focused gates with:

```text
pixi run cargo fmt --all -- --check
pixi run cargo test -p crowdb-web
pixi run cargo clippy -p crowdb-web --all-targets -- -D warnings
cd app/crowdb-web/ui && ./node_modules/.bin/vitest run src/components/dialogs/createFlows.test.tsx
```

The implementation keeps a registered node when a service fails. This
preserves retryability and avoids deleting operator state; node removal remains
an explicit separate action.
