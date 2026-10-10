<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R227: console — Multi-node discovery and light-container deployment

## Problem

CROWDB needs a multi-node deployment flow in which each node runs
`crowdb-monitor` and a Web UI. Operators should not have to enter every peer
address before discovering available nodes. The current monitor and console
surfaces do not establish the LAN discovery and shared bootstrap contract
described here; existing service registration is not node discovery.

The permanent [console design](../design/console/design-crowdb-console.md)
defines Group 0 as topology authority, and the
[Group-0 design](../design/kv/design-crowdb-kv-group0.md) defines its service
registry. Neither a local discovery cache nor a console-local launch registry
can replace that authority. Existing console node/server coupling must be
reviewed when adding nodes before any server exists.

Concrete scenarios:

- Three machines on a home LAN discover each other through their monitors.
  An operator opens any node's UI, assigns racks, and initializes Group 0.
- Several containers on one machine simulate independent nodes on one Docker
  bridge network, with independent identities, state directories and disks.
- Production Linux nodes use light containers with host networking, host
  persistent state, and explicit disk/device access. Containers package binaries
  and dependencies while host resources remain explicitly managed.
- A network partition must not allow an isolated UI or monitor to create a
  replacement cluster or execute stale deployment commands.

## Solution

The target architecture is defined in the
[multi-node deployment design](../design/deploy/design-crowdb-deploy.md).
This requirement tracks implementation and acceptance of the approved decisions
in that design. Docker is the initial runtime; other OCI runtimes are deferred.

### Architecture and authority

```text
browser on any node
    -> local crowdb-web / shared console operations
    -> Group 0: confirmed topology and management authorization
    -> target crowdb-monitor
    -> Docker node container / managed server processes

before Group 0 exists:
local UI -> local monitor discovery -> peer monitors -> bootstrap preparation
```

Every node has a monitor management endpoint and a Web UI. No fixed UI node
is required. Discovery advertises nodes, not each managed server. Group 0 is
the first managed service allowed to start; more complex server deployment is
enabled only after Group 0 is ready and the initial topology is published.

### Invariants

- **I1 — Stable node identity:** node identity is persisted outside the image
  and container writable layer. IP, container name, restart and rack reassignment
  do not change it. Cloned identity directories are detected as conflicts rather
  than silently merging two nodes.
- **I2 — Discovery is a candidate view:** discovery never grants cluster
  membership, voting rights, rack authority or deployment permission. Records
  can expire without deleting confirmed members from Group 0.
- **I3 — Shared authority:** after bootstrap, confirmed node/rack topology and
  cluster deployment intents are authoritative in Group 0. UI caches can lag,
  but mutations succeed only through committed control-plane state.
- **I4 — One bootstrap identity per node:** a node persists its accepted cluster
  identity and initial Group-0 manifest before starting a replica. Conflicting
  manifests are rejected; retries of the same operation are idempotent. No
  timeout or empty discovery result permits automatic replacement bootstrap.
- **I5 — Quorum-backed authorization:** connectivity to one Group-0 replica is
  insufficient. New management mutations require current quorum-backed
  authority. Stale commands cannot execute after authority moves or expires.
- **I6 — Explicit placement:** virtual racks are editable logical groups.
  Physical host/failure-domain information is recorded separately; multiple
  containers on one host do not become independent physical failure domains
  because their virtual rack labels differ.
- **I7 — Portable image, explicit resources:** the same OCI image for a supported
  OS/CPU architecture is used for Docker bridge and Docker host-network
  deployment. Network, disks, persistent mounts and resource limits
  are runtime inputs, not baked cluster identity.

### Numbered work items

1. Extend `crowdb-monitor` identity/profile/status surfaces and shared types in
   `crowdb-protocol` to expose stable node identity, monitor endpoint, protocol
   compatibility, optional rack hint, physical-host identity and cluster binding.
   Obtain full hardware/rack information through a direct monitor handshake.
   A node can exist before any KV or application server is deployed. Generate
   and persist a discovery UUID for candidate identity. Bootstrap fixes its
   mapping to a canonical numeric node ID; later admission allocates the numeric
   ID through conditional Group-0 state and durably publishes the mapping.
   Concurrent admissions cannot allocate conflicting IDs; retries reuse the same
   mapping. IP/rack changes preserve identity and duplicate UUID claims conflict.

2. Add mDNS + DNS-SD announcement and continuous browsing to `crowdb-monitor`.
   Use a CROWDB node service type (proposed `_crowdb-node._tcp.local.`), SRV
   address/port resolution and bounded TXT metadata. Select management interfaces
   explicitly; do not announce on storage data-plane interfaces by default.
   Support record updates, expiry, clean departure, duplicate identity detection
   and incompatible-peer reporting. The browser UI reads its backend's candidate
   view; it does not perform multicast itself.

   mDNS is link-local UDP discovery, not consensus or a health guarantee.
   DNS-SD structures service enumeration and endpoint metadata. These are borrowed
   standard discovery mechanisms; CROWDB defines its own node and authorization
   semantics. See [RFC 6762](https://www.rfc-editor.org/rfc/rfc6762.html) and
   [RFC 6763](https://www.rfc-editor.org/rfc/rfc6763.html).
   Provide explicit peer/seed addresses as a fallback when multicast is blocked
   or nodes span subnets. Neither fallback nor discovered endpoints bypass the
   same handshake and cluster-binding checks.

3. Extend `crowdb-web` and `crowdb-console-shared` with a pre-bootstrap view:
   candidate nodes, editable virtual racks, node-to-rack assignments and Group-0
   initialization. The Cluster tab has a separate Candidate Nodes list below
   its left-side cluster view. Moving a candidate into the cluster requires
   SSH access setup and rack selection; before bootstrap it only edits the
   uncommitted draft, afterward it requires committed admission.
   Node-supplied rack information is a hint, not immutable
   placement. A pre-bootstrap draft is local and visibly uncommitted; differing
   drafts on different UIs do not constitute confirmed topology. Limit this
   stage to node/rack/bootstrap operations, excluding other server deployment.

   Show unbound draft, bootstrap in progress, topology publishing, active and
   authority-unavailable states explicitly. Bootstrap shows per-node store/group
   progress and fixed submitted inputs; later draft edits cannot alter the
   operation. Failures offer resume or explicit cleanup. Once publication is
   complete, load confirmed Group-0 topology in place of the temporary view;
   unused drafts are not automatically imported. Authority loss labels the last
   confirmed view stale, preserves diagnostics and never enables replacement
   bootstrap. Distinct discovered clusters are separately selectable, not merged.
   Partial admission shows joining/recovery-required progress independently of
   confirmed membership. Cancellation preserves pre-existing or concurrently
   required SSH authorization; incomplete cleanup stays visible.

4. Extend the existing shared `bootstrap_intent` and cluster operations to submit
   one fixed initial configuration: cluster identity, operation identity,
   selected initial replica members, reachable endpoints and node/rack draft.
   Each target `crowdb-kv-server` atomically and durably accepts the first
   operation before creating store 0. Exclusion covers store-0/group-0 creation;
   conflicting operations cannot create or reuse either resource. Monitor
   preparation and KV-server acceptance use the same identity/configuration.
   Generic store/group creation cannot bypass this exclusion. Same-operation
   retries verify existing state and resume unfinished steps after restart.
   Failure, timeout or an existing store 0 without group 0 never permits another
   operation to take over.

   All selected members must accept the same operation before initial election
   or publication. Overlapping requests can each occupy part of the membership;
   expose incomplete/conflicting state and support explicit cleanup/retry rather
   than promising that one always succeeds. Disjoint selections may create
   independent clusters, displayed separately without automatic merge.

   Provide UI Group-0 member management and explicit group/store deletion.
   Recovery selects a retained cluster, cleans the other cluster's group 0,
   store 0 and binding, then admits cleaned nodes and uses existing voting
   reconfiguration/catch-up. Deleting group/store alone does not release binding
   or operation ownership. Cleanup fences delayed commands, prevents old replica
   restart and retains terminal operation state before permitting reuse.
   Unreachable nodes remain pending and cannot be reused. Committed clusters
   require explicit destructive cleanup; canceling a draft cannot erase them.

5. Wait for a usable Group-0 leader/quorum, then publish the initial topology
   idempotently. Gate general deployment on both consensus readiness and
   topology publication. Persist cluster binding and Group-0 contact information
   at each accepted node. Other UIs attach to that cluster and read Group 0;
   incompatible cluster bindings are shown separately and never auto-merged.
   Newly discovered nodes remain candidates until a committed admission operation.
   Later Group-0 voting membership changes use the existing reconfiguration path.

   Admission is explicitly approved in UI. Use username/password for initial
   SSH setup unless key access is already provisioned. Each node generates and
   persists its own `id_ed25519`; exchange only public keys into peer
   `authorized_keys`, never private keys. Verify SSH host/candidate identity
   and bidirectional key access with every existing member before confirming
   admission. Partial setup stays pending, blocks deployment and supports
   idempotent retry; cancellation removes only that operation's installed
   authorization entries. Passwords are discarded after setup. Group 0 stores
   nonsecret connection metadata and credential references, not passwords or
   private keys. All admitted nodes can SSH to each other as the configured
   login user. The multi-node Docker test profile supplies `crowdb` / `crowdb`
   and prefills the UI; production credentials are explicit. Persist node keys,
   authorized keys and host keys outside the container writable layer.

6. Route rack changes and deployment mutations from every UI through shared
   console operations and Group 0. Use consistent reads where confirmation needs
   current authority, with watch/refresh for presentation. Record operation
   identity and execution state so UI retries or UI-node loss cannot duplicate a
   deployment. Monitor execution validates the current authorization and rejects
   stale commands. Group-0 unavailability leaves discovery/diagnostics available
   but disables new cluster management mutations. Existing data groups retain
   their own Paxos authority. Group-0 keep-alive, ownership acquisition/renewal
   and balancing fail when authority is unavailable. Unavailable ownership
   content is not empty ownership and cannot trigger release/reassignment.
   Existing ownership follows its own expiry/fencing contract; cached state
   does not extend validity.

7. Implement standard Docker node containers first, with monitor-managed local
   processes. Use the system Docker installation; do not install Docker through
   Pixi or require a Docker socket inside every node. Validate image digest,
   CPU/memory boundaries, explicit networking, persistent mounts and permitted
   disk/device inputs. An external Docker lifecycle starts/recreates the node
   container; monitor provides explicit managed-service start/stop/upgrade and
   process health. Use the same image, monitor and UI for single-node and multi-node operation.
   Single-node startup creates virtual disks and initializes its topology
   automatically, then disables UI editing. Multi-node startup keeps topology
   uninitialized for candidate/draft operations. These are startup policies, not
   separate implementations or images; observation and service semantics remain
   shared. UI disabling is backed by mutation rejection, not only hidden controls.
   Defer containerd/other OCI runtime integration until Docker behavior passes;
   future runtime tools may be packaged through Pixi. Docker images already use
   OCI-compatible packaging; the deferred work is additional runtime support,
   not conversion to a new image format.

8. Extend `container/single-node-container` packaging/test infrastructure for
   multiple node containers from the same OCI image. Each gets an independent
   persistent root and simulated disk, joins one dedicated user-defined Docker
   bridge, and advertises its internally reachable monitor endpoint. Publish
   distinct host UI ports for browser access. Separate test networks bound the
   discovery scope. Each simulated node container runs monitor, UI and SSH;
   three started containers discover each other and expose candidates from any
   UI without a peer list. Docker tests can use local managed processes without
   implicitly mounting a runtime socket. Production uses host networking and real disks on supported
   Linux hosts; discovery and cluster-management semantics stay identical.
   OCI compatibility does not claim complete Kubernetes lifecycle integration
   or unverified CPU/OS support.

## Dependencies

- Existing Group-0 KV operations, consistent reads, service registry and
  membership reconfiguration are reused; node discovery does not replace them.
- Existing `crowdb-monitor`, `crowdb-console-shared`, `crowdb-web`,
  `crowdb-protocol` and single-node image infrastructure are the implementation
  base. Existing working-tree changes must be preserved.
- R139 concerns distributed service configuration. This requirement must define
  the minimal deployment-intent/authorization records it needs without silently
  assuming R139 is implemented or replacing all local process configuration.
- R193 concerns failure-budget placement; physical failure-domain records must
  remain distinguishable from editable virtual racks for that integration.
- The backlog index currently references absent R210/R211/R212 detail files.
  Reconcile those workstreams before implementation to avoid duplicating node
  lifecycle, health and cluster-scope contracts; do not assume they are available.
- `/nv/cpp/adcp/topic/deploy.md` is discussion context for light-container
  deployment, not a portable repository dependency. The applicable resource and
  lifecycle choices are captured above.
- Implementation must apply the accepted bootstrap and authority-loss rules above
  within the approved Docker-first scope.
  Permanent console, Group-0, configuration and container documentation must be
  updated to the shipped behavior during implementation cleanup.

## Acceptance

1. Setup: a persisted node root. Action: recreate the container, change its IP
   and rack. Assertion: node identity remains unchanged; a second live endpoint
   using the same identity is reported as a conflict (I1). Integration test.
2. Setup: three monitors on a multicast-capable test network. Action: start,
   update and stop nodes. Assertion: every backend discovers reachable peer
   identities/endpoints, removes expired candidate records, and does not modify
   confirmed membership on expiry (I2). Integration test.
3. Setup: multicast disabled and explicit reachable seeds supplied. Action:
   discover peers. Assertion: the same compatibility/binding checks apply;
   discovery does not itself authorize admission (I2). Integration test.
4. Setup: multiple network interfaces and an incompatible peer. Action: enable
   discovery on the management interface only. Assertion: advertisements remain
   scoped there, incompatible nodes are reported and cannot bootstrap (I2).
   Integration test.
5. Setup: uninitialized nodes with rack hints. Action: open any node UI, create
   virtual racks and move nodes. Assertion: hints can be overridden, drafts are
   visibly uncommitted and non-Group-0 deployment is blocked (I3, I6). E2E test.
6. Setup: overlapping candidate nodes and two incompatible bootstrap manifests.
   Action: submit concurrently and restart a prepared monitor. Assertion: durable
   conflict rejection prevents conflicting replica startup; the same operation
   can resume without duplicating a group (I4). Integration test.
7. Setup: bootstrap interrupted before and after Group-0 commit. Action: follow
   the specified recovery flow. Assertion: partial setup is resumable, committed
   cluster identity is preserved and no timeout creates a replacement (I4).
   Integration test.
8. Setup: three selected Group-0 replicas. Action: bootstrap and delay initial
   topology publication. Assertion: general deployment remains gated until both
   quorum readiness and publication; all UIs then read the same confirmed
   node/rack state (I3). E2E test.
9. Setup: an active cluster and a new candidate. Action: discover then admit it
   through another UI. Assertion: discovery alone changes no membership; admission
   persists in Group 0 and any voting change uses reconfiguration (I2, I3).
   Integration test.
10. Setup: two independent initialized clusters on one LAN. Action: discover
    each other. Assertion: their identities remain separate and no automatic
    merge or rebinding occurs (I4). Integration test.
11. Setup: concurrent UI mutations and retried deployment requests. Action:
    update racks/deploy and lose the initiating UI. Assertion: committed outcomes
    are shared and each operation executes at most once or resumes through its
    recorded idempotent stages (I3, I5). Integration test.
12. Setup: a three-replica Group 0 partitioned two-to-one. Action: issue management
    commands on both sides and deliver a previously authorized stale command.
    Assertion: only quorum-authorized mutations execute, stale execution is
    rejected, and minority monitors retain diagnostics without replacement
    bootstrap (I4, I5). Integration test.
13. Setup: several simulated nodes on one physical host in different virtual
    racks. Action: inspect placement inputs. Assertion: the shared physical
    failure domain remains visible and rack changes do not rewrite it (I6).
    Integration test.
14. Setup: one supported-architecture Docker image digest. Action: launch multiple
    Docker bridge nodes and Linux Docker host-network nodes with appropriate
    disks/mounts. Assertion: both discover/bootstrap/manage successfully, state
    survives replacement, internal advertised addresses are reachable and the
    simulated-disk run makes no production-performance claim (I1, I7).
    Integration test.
15. Setup: Docker deployment with explicit devices/resources. Action: start,
    stop, restart and upgrade through monitor. Assertion: requested resource
    limits and mounts are applied, process health is observable and restart
    preserves persistent state (I7). Integration test.

16. Setup: candidates and an initialized cluster. Action: move a candidate from
    the Cluster tab's separate list, provide SSH credentials and select its rack.
    Assertion: host/monitor identity and mutual Ed25519 access are verified before
    committed admission; partial failure remains pending and retry does not
    duplicate keys; cancellation preserves unrelated authorization entries (I2,
    I3). E2E test and Integration test.
17. Setup: multi-node Docker test containers with `crowdb` / `crowdb` initial
    access. Action: join nodes and recreate a container using its persistent root.
    Assertion: node private keys are never exchanged, host/public-key trust
    survives recreation and initialization passwords are not persisted (I1,
    I7). Integration test.
18. Setup: unavailable Group 0 and a data group with quorum. Action: perform
    data operations and request keep-alive/ownership renewal/balance. Assertion:
    data operations follow their group's Paxos rules; Group-0 operations fail
    explicitly, unavailable ownership is not treated as empty, and ownership
    dependent actions stop at their existing authority expiry (I5). Integration
    test.

19. Setup: concurrent conflicting bootstrap requests to one KV server. Action:
    race store-0 creation, interrupt before group-0 creation and restart.
    Assertion: only one operation owns store 0/group 0, conflicting takeover
    fails and the accepted operation resumes idempotently (I4). Integration test.
20. Setup: disjoint bootstrap member sets. Action: initialize both, select one
    retained cluster in UI, clean the other and admit its nodes. Assertion:
    clusters remain separate until explicit cleanup; delayed old commands cannot
    recreate deleted resources, unreachable nodes cannot be reused, and added
    voting replicas catch up through reconfiguration (I4). Integration test and
    E2E test.

21. Setup: independently initialized discovery UUIDs and concurrent admissions.
    Action: bootstrap, admit nodes, retry and change rack/endpoints. Assertion:
    numeric IDs and UUID mappings remain unique and stable; cloned UUIDs conflict
    rather than merge (I1, I3). Integration test.
22. Setup: three node containers on one discovery-capable Docker test bridge.
    Action: start them without peer lists, open any UI, then recreate one node.
    Assertion: each monitor discovers peers, each UI exposes candidates and
    persistent identity survives external container recovery (I1, I2, I7).
    Integration test and E2E test.

23. Setup: local UI drafts and an interrupted bootstrap. Action: submit, edit
    a draft, resume creation and delay topology publication. Assertion: submitted
    inputs remain fixed, per-node progress and recovery actions are visible,
    general deployment remains disabled until publication, then confirmed Group-0
    topology replaces the draft without importing unrelated edits (I3, I4).
    E2E test.
24. Setup: active cluster, partial admission and another discovered cluster.
    Action: lose Group-0 authority, retry/cancel admission and select a cluster.
    Assertion: stale authority is explicit, diagnostics remain usable, replacement
    bootstrap is disabled, clusters remain separate and cleanup preserves
    unrelated or concurrently required SSH authorization (I2, I3, I4, I5).
    E2E test.

Implementation verification commands (these are required future gates, not
claims that the new acceptance coverage already exists):

```bash
pixi run cargo test -p crowdb-monitor
pixi run cargo test -p crowdb-console-shared
pixi run cargo test -p crowdb-web
pixi run cargo test -p crowdb-protocol
pixi run test-console
pixi run test-console-ui
pixi run rs-fmt-check
pixi run rs-lint
```

## Open Questions

None. Discovery UUID/numeric-ID mapping and containerized monitor/UI placement
are approved along with the bootstrap, authority-loss and admission contracts.
