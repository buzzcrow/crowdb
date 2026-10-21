<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R243: deployment — Unified OCI cluster deployment and cluster status

#### Problem

CROWDB already has a shared OCI image, Docker/containerd runtime adapters,
mDNS/DNS-SD discovery, authenticated node admission, and durable monitor-owned
bootstrap. Deployment still exposes several competing workflows: the read-only
automatic single-node profile, manual node containers, the CLI-owned three-node
S3 cluster, bare-metal launch registries, and component-specific local deployment.
The CLI container workflow separately provisions storage and services. Starting
containers directly does not receive the same complete preparation as the CLI.

The implementation gaps are observable, not just command naming differences:

- `container/crowdb-monitor`'s setup storage runner and Web's planned Chunk-KV
  builder require an existing ordinary metadata group. The CLI currently creates
  Store 0 / Group 1 itself with fixed node IDs; removing that caller without a
  durable replacement leaves direct startup permanently waiting.
- Completed `SetupStepPhase::Ready` and `Warning` service steps are skipped by
  the setup runner. An operator's later stop can therefore coexist with historical
  setup completion. Neither completion nor a PID proves current readiness.
- The old single profile and CLI cluster use different storage geometry,
  placement settings and access ports. The Dataset listener is optional in
  ordinary service deployment. Moving entrypoints alone does not make all three
  access protocols work from a host client.
- Existing containerd cluster acceptance uses Docker to provision disposable
  outer hosts and verifies Group-0 recovery. It is useful adapter coverage, but
  does not alone prove complete storage/access readiness on a Docker-free host.

On a dedicated single-host bridge, three ordinary node containers already
discover one another without peer addresses. However, Auto join waits for a rack
and working SSH access, and automatic initialization creates only Store 0 / Group
0. The existing single-node policy follows a separate profile and disables
management mutations. Users cannot use one simple, transparent Docker workflow
for an editable one-node or three-node cluster and obtain equivalent CLI/Pixi
behavior. Per-component launch and deployment checks also obscure whether the
whole cluster is usable.

The [deployment architecture](../design/deploy/design-crowdb-deploy.md) and
[OCI image design](../design/deploy/design-crowdb-oci-image.md) must converge on
OCI node containers as the supported deployment unit. Operators need direct
Docker commands for evaluation, isolated local clusters, host-network deployment
on cloud Linux machines, and a runtime-independent node contract that future
Kubernetes deployment can reuse. RDMA deployment uses host networking; this
requirement adds no RDMA-specific configuration, tuning or hardware qualification.

#### Solution

The same versioned OCI node image is the software delivery unit for all supported
deployment modes. A node container owns its monitor and managed CROWDB processes.
The runtime owns the container, filesystem mounts, network and device access;
Group 0 remains authoritative for confirmed topology and service intent.
Initialization accepted by a monitor survives the initiating CLI/UI connection.
The default local workflow creates a complete usable cluster, rather than merely
a running container or a ready Group 0.

1. **One image and external durable state.** Consolidate the container packaging,
   `crowdb-monitor`, and shared deployment configuration into one node contract.
   Package binaries, user-space libraries, configuration templates and the normal
   entrypoint. Keep node UUIDs, cluster bindings, SSH identities, configuration,
   operation records and storage outside image layers. Keep image version/digest
   separate from persistent cluster identity. Docker and containerd consume the
   same artifact without rebuilding application content. Production image
   replacement retains mounted state and validates the current node contract;
   image replacement is not permission to reset or rebootstrap data. Backward
   compatibility with obsolete deployment commands, configuration or state
   formats is not required; no legacy aliases or conversion flows are needed.

2. **One cluster model; UI permissions independent of deployment.** Refine monitor startup
   and shared deployment policy so a one-node cluster is a topology/policy case
   of the normal cluster lifecycle. Default UIs remain editable for both
   topologies and every runtime. Do not derive readonly/managed UI policy or
   mutation rejection from node count, automatic startup, local/production
   deployment, or container runtime. Stop selecting the existing immutable
   single-node UI mode in the default deployment workflow. Consolidate its
   duplicate initialization/service setup after callers migrate. Future UI user
   roles and permissions are a separate concern and are not introduced here;
   existing authentication, authorization and consistency checks still apply.
   The initial voter policy distinguishes an intentional one-node
   cluster from a three-node cluster still waiting for peers. The simple local
   entry defaults to one node; the three-container example selects the local
   three-node policy without supplying peer addresses, fixed IPs or a membership
   manifest. An incomplete three-node start remains pending, never bootstraps a
   singleton on timeout, and never expands an accepted voter set from discovery.
   Persist accepted policy and reject incompatible restarts. Remove obsolete
   single/manual deployment paths without a backward-compatibility requirement.
   Removing legacy support does not authorize deleting existing user data.

3. **Reuse discovery, admission and bootstrap.** Preserve monitor mDNS/DNS-SD on
   management interfaces, seed fallback across discovery boundaries, candidate
   compatibility/identity checks, mutual SSH admission, fixed bootstrap manifests
   and KV-server exclusion. Local three-container setup supplies the default
   rack and local test authentication through a clearly identified local policy,
   completes authenticated admission, and uses the existing UUID-ordered
   three-voter selection and monitor execution. It does not add a peer-list
   protocol, copy private keys, or treat advertisements as authority. Existing
   general production admission still requires configured credentials and trust;
   local convenience credentials never become the production fallback. Foreign
   clusters are not merged or selected arbitrarily.

4. **Complete default preparation inside the node lifecycle.** Move the reusable
   storage/service preparation now orchestrated by the CLI container cluster
   into the shared, resumable node deployment flow. The local policy prepares
   independent file-backed test storage, the required data groups, DiskDB,
   DiskIO, ChunkDB, Chunk-KV and access services in dependency order. Reuse
   existing disk admission, service-intent and ownership mechanisms. Persist
   operation progress and expose the first failed or waiting dependency;
   retries resume accepted work without duplicate disks, services or bootstrap.
   Keep management available while preparation is pending. One-node test storage
   explicitly declares zero node-failure tolerance. Three containers on one
   physical host remain one physical failure domain and must not claim physical
   host redundancy; any test placement allowance is explicit in the local policy.

5. **Transparent direct-container entry points.** Publish tested direct
   `docker run` examples for a one-container cluster and a three-container
   cluster, plus equivalent Compose configuration. The single-node example
   needs only the image, persistent volume and published client/UI ports. The
   three-node example creates one dedicated bridge and uses distinct containers
   and volumes with the same local three-node policy; it needs no UI preparation,
   CLI initializer, manually entered peers, Docker socket inside nodes, or
   blanket privileged container. Publish documented stable client/UI ports,
   credentials access and startup progress. Each member UI uses the same
   confirmed cluster. Docker bridge DNS is available but does not replace the
   existing discovery contract. Direct runtime startup and helper startup must
   execute the same node lifecycle.

6. **Separate runtime, network and hardware policy.** Preserve distinct Docker
   and containerd/nerdctl adapters. A local bridge is the default for single-host
   tests; each cluster gets its own network. Support Linux host networking for
   both runtimes with explicit management/data addresses, stable physical-host
   identity, persistent mounts and configured resources. With current fixed
   service ports, host deployment places one node per host and rejects local
   port conflicts rather than applying ineffective published-port mappings.
   Management discovery and data transport selection remain independent.
   Preserve existing block-device access and resource validation. RDMA deployment
   uses host networking, with no new RDMA device, memory-lock, CPU/NUMA tuning or
   hardware qualification requirements. Reject unsupported runtime/network
   combinations rather than silently substituting bridge mode or ignoring
   requested resource limits. Host networking is not a performance guarantee.

7. **Cluster-name-scoped management and runtime portability.** Expose a common
   cluster name through `--cluster`, backed by a durable cluster UUID. Keep
   namespace as a backend term rather than a second public cluster selector.
   Docker maps the name to ownership labels, names and isolated local networks/data;
   containerd maps it to an owned runtime namespace and corresponding node
   resources. Record runtime endpoint/context so a later operation cannot
   silently select a different daemon. Namespaces are management scope, not a
   security boundary or host-network port isolation. Keep the node's image,
   configuration, persistent paths, probes and bootstrap independent of Docker
   object IDs. Future Kubernetes manifests can supply these same inputs, with
   Kubernetes owning scheduling, Pod lifecycle, volumes, network/device allocation
   and rollout. This requirement implements Docker and containerd deployment;
   it preserves the Kubernetes contract without claiming an implemented operator
   or complete Kubernetes support. See the external
   [containerd namespace semantics](https://containerd.io/docs/main/namespaces/),
   [Docker host networking](https://docs.docker.com/engine/network/drivers/host/)
   and [Kubernetes device interface](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/);
   these are runtime mechanisms, not CROWDB membership semantics.

8. **Consolidated CLI and Pixi responsibilities.** Promote container cluster
   lifecycle out of `s3 cluster` into the common `crowdb-cli cluster` domain.
   Define cluster-name-scoped `start`, `stop`, `status` and `check`; `start` defaults
   to a one-node local cluster, with the same flow for three nodes. Runtime and
   network are independent inputs. The CLI creates/selects runtime resources
   and waits for shared cluster readiness; it does not carry a second bootstrap
   or storage/service provisioning implementation. Remove public bare-metal
   launch-registry, component local-deploy, and obsolete deployment/check paths;
   migrate callers and remove dead code instead of retaining aliases. Internal
   unit-test fixtures and service process supervision are not public deployment
   products. Keep data-plane and normal topology management operations.
   Public container inspection, logs, exec and image operations use the runtime's
   standard tools. Pixi exposes `oci-build`, `oci-run`, `oci-run-1c`,
   `oci-run-3c`, `oci-stop` and `oci-clean` as local development/test shortcuts.
   Their host scripts invoke Docker directly, never `crowdb-cli`, to create,
   start, stop and remove containers and networks. They do not depend on a built
   or installed CLI or on CLI deployment records. `oci-run` defaults to one node;
   `oci-run-1c` is its explicit equivalent and `oci-run-3c` starts three containers
   on a dedicated local Docker bridge. Run reuses the built/loaded image, never
   rebuilds implicitly, and leaves cluster preparation to the normal in-image
   lifecycle. A missing image produces an actionable build/load error. The
   direct Docker examples are the reference behavior for these scripts. This
   Docker convenience layer does not restrict the OCI node contract's other
   runtimes. Build owns artifact production and optional loading into the
   selected runtime. Construction/staging/verification helpers
   remain internal CI/build tools where needed, not extra root task wrappers.
   Stop/clean accept `--cluster`; global scope requires `--all`. Clean belongs
   to the host/Pixi scripts, never to a new CLI container-clean command. It stops
   and removes owned containers and unused owned networks, preserves data unless
   deletion is explicit, and reclaims only unused CROWDB image garbage while
   respecting images referenced by other clusters. Do not use unscoped runtime
   pruning or container-name substring matching as resource ownership.

9. **Cluster status is the operator-facing check.** Upgrade `cluster status`
   and `cluster check` to report cluster UUID, selected cluster name, observed
   members, initial voter policy, image/runtime, quorum/authority, initialization
   progress, storage readiness and required service readiness. Distinguish
   waiting, initializing, ready, degraded, unavailable and failed observations
   with actionable causes and retained operation identity. Container running,
   an HTTP listener, successful submission or Group-0 readiness alone cannot
   imply whole-cluster readiness. Status is observational; check has a bounded
   wait option, machine-readable output and nonzero exit on an unmet readiness
   contract. Remote checks work from management endpoints without a local Docker
   socket and mark unavailable runtime facts unknown. Preserve useful per-node
   diagnostics beneath this aggregate. Remove redundant public deployment
   check commands, not internal bootstrap fencing, device/resource validation,
   authorization checks, component tests or release-artifact verification.

10. **Refine designs and migrate acceptance together.** Update the deployment
    root design, OCI runtime sub-design, affected CLI/container guides, Pixi/CI
    callers and deployment acceptance around this contract. Remove obsolete
    bare-metal and special read-only deployment recommendations and duplicate
    public command paths. Permanent design describes implemented behavior only;
    keep not-yet-implemented Kubernetes automation explicit. Replace duplicated
    layer-launch deployment scenarios with direct-container and cluster-status
    acceptance while preserving meaningful component correctness coverage.

11. **Audit the complete CLI tree, including implicit benchmark deployment.**
    Review help, dispatch, shared-library calls, tests and examples together;
    matching only command names such as `deploy` is insufficient. Use this
    command-level contract as the joint review baseline before implementation:
    - Remove the whole `launch` domain and its public `--registry` process
      configuration. Remove `cluster local-deploy` and all of its component
      selectors and server tunables. Remove the old process-oriented cluster
      teardown/reset paths, including `cluster clean --restart-services`;
      container/environment cleanup remains a Pixi/host responsibility. Normal
      scoped data deletion is distinct from container or process lifecycle.
    - Remove the whole `kv server` domain, including deploy/start/restart/stop/
      delete/list. Remove DiskDB, ChunkDB and DiskIO deployment and process
      controls beneath `chunk diskdb` and `chunk stub`. Move useful service
      inventory/health into the aggregate cluster status/check; do not preserve
      registry/PID lists as a parallel server-management surface. Remove exposed
      not-implemented lifecycle/maintenance placeholders instead of promising
      unsupported behavior. This does not remove actual KV data operations,
      store/group/replica semantics, or working storage maintenance APIs.
    - Remove `s3 cluster` after moving its runtime lifecycle to `cluster`.
      Keep S3 bucket/object operations and attach them to the common cluster
      selection. The current shared container target must not remain an
      S3-specific deployment dependency.
    - Keep benchmark workloads as clients of an explicitly selected existing
      cluster. `bench kv prepare` prepares workload data, not infrastructure;
      it may write its declared dataset but cannot initialize groups or spawn
      servers. KV, DiskDB, ChunkDB and ChunkIO workload validation and client
      background I/O are not server deployment. Workload-owned object cleanup
      is permitted; cluster stop, topology reset, provisioning and image changes
      are not benchmark side effects.
    - Refactor `bench s3`: its current shared `s3_bench::run` calls
      `s3::start_memory` and `s3::stop` around each workload. Remove that hidden
      in-process/bare-metal cluster lifecycle and attach to the selected OCI
      cluster, with explicit access endpoint and credentials. Memory/storage
      mode belongs to cluster configuration; benchmark work/output directories
      cannot select or allocate a private deployment. Separate fixture-only
      helpers from the public workload path and delete obsolete product code
      after migrating callers. Do not merely replace the hidden process launch
      with an equally hidden container launch.
    - Remove RPC-specific server/client deployment and lifecycle from the
      product CLI. The current RPC benchmark uses a standalone echo server,
      documents `cluster local-deploy -t rpc` for setup, and owns a transport
      client runtime. Move that standalone RPC test workflow, including the
      public `bench rpc` entry, to RPC test/benchmark tooling. Tests explicitly
      start and stop their own server and client; no replacement product RPC
      launcher or attach command is required. Preserve reusable transport
      workload/measurement code where tests need it. Ordinary client transport
      workers used by retained KV/storage workloads are not separate deployments.
    - Define one common cluster target for status/check, logical operations and
      benchmarks: a cluster name resolved to recorded cluster/runtime identity,
      or explicit remote management endpoints. Attach/read commands also work
      for clusters started with plain Docker commands and require no CLI-created
      launch registry. A namespace/endpoint mismatch fails before workload
      mutation; an unavailable cluster reports an error rather than auto-start.
      Benchmark workload targets such as KV store/group, bucket and object
      prefixes remain explicit within that selected cluster.
    - Migrate CLI and benchmark E2E fixtures, scripts, docs and dependencies with
      the command tree. Preserve measured workload semantics while moving
      environment preparation to an explicit OCI fixture. Remove obsolete
      process-launch code and dependencies only after checking non-CLI callers;
      monitor-owned process supervision and narrow internal unit-test helpers
      remain valid inside their owning scope. Removed public commands have no
      compatibility aliases or hidden fallback to bare-metal deployment.

##### Required lifecycle and failure semantics

The following contracts refine solution items 1–11. They are mandatory outcomes;
the working plan owns type names, file placement and execution order.

- **C1 — Accept inputs before effects.** Resolve explicit overrides over file
  configuration over fresh-volume defaults once. An existing accepted policy
  supplies omitted values on restart; a new image's defaults cannot rewrite it.
  Seal the policy version, local/production mode, initial voter count, cluster
  scope, storage choices and required service/listener choices before admission.
  Reject unknown versions, corrupt records, incompatible overrides and obsolete
  populated roots without formatting, generating a replacement identity or
  allocating fallback storage. The initial voter count describes bootstrap,
  not a desired-membership reconciler; subsequent topology changes use existing
  explicit topology APIs. Simultaneous use of one writable data root by two node
  instances is rejected. A copied node identity is not a new cluster member.

- **C2 — Scope discovery and trust.** Fresh local startup defaults to one voter;
  three-node startup explicitly selects three and a common scope on its dedicated
  bridge. A singleton never enrolls unrelated discoveries automatically. Check
  policy/scope compatibility before attempting local credentials; retain SSH host
  identity checks and authenticated admission. Keep the existing local test
  credential convenience explicitly confined to local policy, document its lack
  of production trust, and never use it after production credential failure.
  Cluster names are discovery/management hints, not credentials or cluster UUIDs.
  No private SSH key is shared. A rejected host key or conflicting accepted
  manifest requires operator attention; it is not retried with weaker trust.
  The minimal automatic formation contract has exactly one or three intended
  fresh nodes. A known overfull/ambiguous fresh candidate set must report conflict
  before acceptance. An extra node arriving after acceptance cannot change the
  fixed voters; adding it requires the normal explicit admission/topology flow.
  Multiple already-bound cluster UUIDs never trigger automatic merge or reset.
  UUID sorting alone is not a distributed exclusion mechanism: reuse and verify
  accepted-manifest fencing under concurrent coordinators and partial visibility.
  Retain cluster UUID, discovery UUID, numeric node ID and Ed25519 SSH host-key
  bindings through existing authenticated bootstrap/admission. Reuse those local
  bindings to verify rediscovered members at changed IPs without Group-0 quorum.
  Discovery is only a hint: require a pinned-key SSH handshake and matching peer
  binding before using its current endpoints. Recover existing replica routes,
  never votes or membership; publish addresses through normal authority after
  quorum returns. Do not require stable DNS names, fixed IPs or a peer list.
  Missing trust, changed keys, foreign bindings and duplicate authenticated live
  claimants fail closed. Multicast scope and seed fallback remain unchanged.

- **C3 — Own cluster-wide preparation once.** After Group-0 authority is available,
  create one durable default preparation intent through existing committed/CAS
  mechanisms. It seals the ordinary metadata group, replica mapping derived from
  admitted members, storage bindings and shared access credential reference.
  Any authorized runner may resume that same intent after coordinator loss;
  losing a local Web process must not lose the accepted work. A conflicting
  existing group or binding fails visibly instead of choosing the first available
  ordinary group. A create timeout is an unknown outcome: read back the accepted
  identity before retrying. Per-node journals own local disks and services;
  they do not each allocate an independent cluster-wide default group.
  After explicit deletion/retirement, automatic preparation must not recreate
  the deleted topology or overwrite a later service intent.

- **C4 — Progress dependencies without a cycle.** The readiness path is
  Group 0 -> ordinary metadata group -> DiskDB and disk registration -> DiskIO
  runtime/ownership -> ChunkDB placement -> Chunk-KV catalog -> access protocols.
  Disk preparation and service startup are interleaved: start DiskIO after its
  prerequisites are published, without first waiting for disk Online, which
  itself requires DiskIO. Persist operation IDs and accepted geometry before
  effects. Validate local file count/capacity, allocation unit, mirror/EC policy
  and failure budget together for both node counts. Local sparse files are an
  explicit test policy; production allocation never falls back to sparse files.
  Supplied media disables automatic-file fallback. ENOSPC, wrong inode, duplicate
  medium and unsupported geometry retain the failed intent and original data.
  Status distinguishes published metadata from usable capacity and live ownership.
  Colocated storage is never relabeled as independent physical hosts to satisfy
  placement. Reuse existing unsafe local placement only within local policy;
  do not change the production protection contract or introduce R193 here.

- **C5 — Expose usable endpoints and stable credentials.** Default local node
  listeners are UI 9090, S3 9091, Iceberg 9092 and Dataset 9093. Publish every
  member UI at a distinct host port in the three-container example; host-side
  port overrides must not alter internal service identity. Separate internal
  discovery/transport addresses from advertised client URIs. A host client must
  complete operations without receiving an unreachable bridge address in catalog
  or object locations. External URI overrides are explicit when the mapped host
  port/address differs; the node does not inspect a Docker socket to infer NAT.
  Shared access credentials are prepared once through existing credential
  ownership and delivered to admitted nodes; retries/recreation do not rotate
  them. Document retrieval through existing protected files/UI, never status
  JSON, labels, logs or command arguments. Dataset must actually be enabled;
  successful S3 health is not evidence for Dataset or Iceberg readiness.

- **C6 — Separate current readiness from completed setup.** Whole-cluster ready
  requires current Group-0 authority, the accepted/default metadata binding,
  available storage ownership/capacity, all policy-required service instances,
  and each required protocol's non-mutating readiness observation. Use current
  committed service intent, not a historical PID or successful submission.
  A required service intentionally stopped by an operator makes the default
  readiness contract unmet without authorizing an automatic restart. Observe
  version/freshness per dependency; timeout or stale success is unknown, never
  healthy. Missing runtime facts on a remote check remain unknown and alone do
  not invalidate application readiness.
  Report storage in dependency order: Group-0 disk/group state and metadata
  binding, current owner and lease, then current-owner capacity. A blocked
  upstream layer leaves downstream checks waiting for that named dependency.
  Distinguish missing/unknown, stale and exhausted capacity. Successful and
  unsuccessful observations both carry concrete, credential-free evidence;
  errors retain their specific code and full explanation through aggregation.
  Chunk-KV instances are stateless workers assigned tree-range ownership. An
  idle instance with a loaded catalog needs no assigned range; an assigned
  range requires matching ownership authority and a serviceable recovered tree.
  Apply one deterministic aggregate rule: a validated terminal failure of a
  current blocking operation or an identity conflict is `failed`; superseded
  historical failures do not poison a recovered cluster. Otherwise missing
  current cluster authority for a bound cluster is `unavailable`; otherwise all
  readiness predicates yield `ready`; otherwise previously completed setup or
  an explicit required-service stop
  yields `degraded`; otherwise accepted work in progress is `initializing`, and
  missing admission/bootstrap prerequisites are `waiting`. Missing voters before
  first bootstrap are waiting, not quorum loss. Return all dependency reasons
  even when a higher-priority aggregate reason wins. One missing member of an
  established three-node cluster is degraded when quorum remains; distinguish
  this strict deployment readiness from continued data-plane availability.
  Status/check perform no probe writes, application-operation retries, repair
  or initialization; bounded observation retries/polling are allowed.

- **C7 — Resolve targets before workload mutation.** Explicit management endpoints
  support direct-started clusters without a local runtime or CLI record. With
  both a name binding and endpoints, all observed identities must agree; fail
  on mixed UUIDs, ambiguous names, stale bindings or runtime endpoint/context
  changes before sending a workload write. Do not fall back to localhost or a
  different daemon. Runtime names/ownership tokens exist before bootstrap;
  application UUID is initially unknown and is bound only after authoritative
  observation. Do not mint a second cluster UUID in a host helper. Endpoint-only
  attach grants no ownership of containers for stop/clean. On a bridge, remote
  KV/storage benchmarks additionally need reachable transport endpoints; only
  publishing the UI does not make native RPC reachable. Reject unsupported
  endpoint reachability explicitly; no proxy/tunnel or new transport protocol
  is introduced by this requirement.

- **C8 — Make runtime operations recoverable and cleanup conservative.** An
  interrupted host start records/reconstructs owned resources and resumes only
  matching mounts, image, network, policy and ownership. Same-name foreign
  containers/volumes and changed configurations are conflicts, not adoption.
  A startup/check deadline does not delete resources or cancel accepted node
  work. Stop retains containers/data and does not retire membership; it is
  idempotent for an already stopped owned container. Clean removes only selected
  owned runtime resources and preserves volume/bind data plus the information
  needed to reuse it. Data deletion requires a separate explicit option scoped
  to owned storage, with no recursive deletion through foreign paths/symlinks.
  Scope `--all` to CROWDB resources on the recorded/selected runtime endpoint,
  not every configured daemon. Missing objects are idempotent; permission,
  timeout and daemon failures produce failure and retain retry information.
  Recheck references before image/network deletion, including stopped containers
  and retained cluster bindings. Skip uncertain/shared image ownership; do not
  force removal or use global prune. Unlabelled direct Docker resources remain
  manageable with Docker; endpoint attach alone cannot make Pixi clean own them.

#### Dependencies

- Existing shared OCI build/publication, Docker/containerd adapters, monitor
  discovery/identity, SSH admission, node setup, durable initialization,
  Group-0 service intent and readiness APIs are the baseline. Refactor and reuse
  them; no alternate membership or authority system is required.
- R193 may generalize node-failure budgets. Until then, preserve the existing
  supported one-node test and protected multi-node placement policies, and
  report the actual shared-host limitation in local tests.
- R139 may distribute additional configuration through Group 0. This work must
  run with the existing file-backed and committed service-intent contracts and
  must not wait for a new configuration service.
- Docker provides local bridge acceptance. Containerd/nerdctl requires its
  configured daemon/runtime tools; any bridge backend additionally requires its
  networking dependencies. The host-network adapter must not require Docker.
- RDMA deployment uses the host-network contract; RDMA-specific tuning and
  hardware certification are outside this requirement. Kubernetes orchestration
  and generalized remote scheduling remain future work; neither blocks the
  portable node contract.
- UI user roles and permission modes will be designed separately. They do not
  block editable default deployment and must not be encoded in deployment or
  node-count policy.

#### Acceptance

- **A01.** Given the built image and a fresh named volume, execute only the documented
  single-node Docker command; the normal editable one-member cluster becomes
  ready with usable storage and S3/Iceberg/Dataset services, and a normal
  management mutation succeeds. Invariants: one lifecycle, complete readiness.
  E2E test.
- **A02.** Given one-node and three-node clusters using local or host deployment, open
  their default UIs and perform authorized supported management mutations;
  node count, runtime and automatic preparation do not cause readonly rejection.
  Unauthorized or inconsistent operations still fail for their actual cause.
  Invariant: UI permissions are independent of deployment. E2E test.
- **A03.** Given a fresh dedicated Docker bridge and three independent volumes, execute
  only the documented three-container commands in different orders; nodes
  discover, authenticate, admit, prepare and form one ready three-voter cluster
  without UI/CLI preparation, peer lists or fixed IPs. Repeat with Compose;
  UUID membership and readiness semantics match. Invariant: shared bootstrap.
  E2E test.
- **A04.** Given a three-node local policy with only one or two containers running, wait
  beyond discovery/startup deadlines; status remains pending and no singleton
  bootstrap occurs. Start the remaining node; the existing operation reaches
  readiness with three fixed voters. Invariant: explicit initial policy.
  E2E test.
- **A05.** Given the local policy's preparation steps, interrupt rack/admission, bootstrap,
  disk preparation or service deployment and restart the responsible process;
  accepted work resumes with unchanged identity and without duplicate media,
  keys or services. A failed operation reports its cause and retries retain its
  accepted inputs. Invariant: durable initialization. Integration test.
- **A06.** Given retained state created by the unified lifecycle, restart under a
  conflicting topology or policy; startup rejects the conflicting inputs before
  mutation and preserves accepted identity/data. No legacy-format conversion
  is required. Invariant: no implicit rebootstrap. Integration test.
- **A07.** Given two named local clusters on separate networks with independent volumes,
  start, stop and clean one; the other retains its running members, data and
  image references. Containers sharing a physical host report that common
  failure domain and no host-failure tolerance claim. Invariants: scoped
  ownership and truthful placement. E2E test.
- **A08.** Given the same OCI artifact, deploy nodes using Docker and native
  containerd/nerdctl host networking on separate Linux hosts; discovery/admission,
  initialization, readiness, restart and persisted replacement work without a
  Docker daemon in the containerd case. Immutable artifact and retained node
  identities match. Invariant: runtime-independent node contract. E2E test.
- **A09.** Given host-network nodes, request port-conflicting placement, invalid existing
  device access or unsupported networking/resource settings; deployment fails
  visibly without substituting bridge mode or changing accepted identity. RDMA
  deployment selects host networking through the same runtime path, without
  additional RDMA tuning or hardware certification gates. Invariant: explicit
  runtime resources. E2E test.
- **A10.** Given equivalent direct Docker and CLI/Pixi launches, compare persisted node
  policy, members, service intent and readiness; results match and neither host
  helper executes a separate application bootstrap. Invoke removed bare-metal,
  S3-specific deployment and layer-check commands; they are absent from help
  and callers use the consolidated surface. Invariant: one deployment path.
  Integration test.
- **A11.** Given a built image loaded into Docker and no `crowdb-cli` executable, invoke
  Pixi one-node and three-node run shortcuts, then scoped stop/clean; the host
  scripts operate through Docker, do not compile/download/invoke the CLI or
  rebuild the image, and nodes use the same in-image preparation as direct
  Docker examples. A missing image fails with the build/load action and no
  implicit deployment fallback. Invariant: independent local test shortcuts.
  E2E test.
- **A12.** Given the complete CLI help/dispatch tree, enumerate all public subcommands
  and follow each lifecycle route; only common container cluster lifecycle is
  reachable, old server/process controls and placeholders are absent, and
  service inventory is available through cluster status/check. Existing logical
  KV and S3 operations still address the selected cluster. Invariant: one public
  deployment domain. Integration test.
- **A13.** Given ready OCI clusters and all retained benchmark families, run their workloads and
  compare container/process inventory, cluster UUID, image identity and topology
  before and afterward; only declared workload data and result artifacts change.
  In particular, S3 does not create/stop a memory cluster. Given an unavailable
  target, every workload fails without deployment, reset or resource cleanup.
  Invariant: benchmarks are attach-only clients. E2E test.
- **A14.** Given a plain-Docker-started cluster, select it through management endpoints
  and run status/check plus KV/S3 workloads; no host launch registry is required.
  Given a conflicting namespace/endpoint target, reject it before data writes.
  RPC tests start their own server/client and run the transport workload without
  `cluster local-deploy -t rpc`, `bench rpc` or another product deployment
  command; retained application clients may still own transport workers.
  Invariants: common explicit target selection and test-owned RPC lifecycle.
  E2E test.
- **A15.** Given running containers with no Group-0 quorum, pending disks, unavailable
  required services or an interrupted operation, run cluster status/check;
  structured results identify the actual waiting/degraded/failure cause and
  check exits nonzero within its bound. Healthy readiness succeeds; read-only
  remote checks need no runtime socket and never initialize or repair the
  cluster. Invariant: readiness reflects committed cluster state. E2E test.
- **A16.** Given cluster-owned and unrelated runtime resources plus an image shared by
  another cluster, invoke scoped clean and then explicit global CROWDB
  cleanup; only selected owned resources and unused CROWDB image garbage are
  removed, persistent data is retained by default, and missing/runtime-error
  responses are not confused. Invariant: cleanup ownership. Integration test.
- **A17.** Given the refined designs, guides, CI and task graph, follow each documented
  public command against the tested image; examples use the common entrypoint
  and name only implemented capabilities. Kubernetes deployment inputs can be
  expressed without Docker sockets, IDs or CLI-owned bootstrap state.
  Invariant: portable, reviewable deployment contract. Integration test.

Cases A18–A33 cover the additional negative and recovery paths of C1–C8;
the plan maps these stable acceptance IDs to tasks and test owners.

- **A18.** Given a retained accepted policy, omit overrides and restart with changed image
  defaults; accepted inputs and UUID remain unchanged. Supply a conflicting
  override, unknown version or corrupt/obsolete populated root; startup rejects
  before new identity/storage effects and preserves the root. Invariant: C1,
  accepted inputs outrank defaults. Integration test.
- **A19.** Given two processes using one writable root, or two advertised nodes with the
  same persisted node UUID, start admission; reject the duplicate use/identity
  without registering a second member or writing a second setup. Invariant: C1,
  exclusive node identity. Integration test.
- **A20.** Given incompatible scopes/policies, changed SSH host keys, foreign bound UUIDs
  or an observed overfull fresh candidate set, run automatic discovery; no
  incompatible admission/bootstrap occurs and status explains the conflict.
  A production credential failure never attempts the local default credential.
  Invariant: C2, authenticated scoped formation. Integration test.
- **A21.** Given the intended three nodes with concurrent coordinators and asymmetric
  visibility, interrupt the selected coordinator after prepare/accept and heal
  the network; only one accepted manifest may execute, or a conflicting state
  remains visibly fenced without reset. An extra late node cannot replace its
  voters. Invariant: C2, accepted-manifest exclusion. Integration test.
- **A22.** Given Group 0 and concurrent preparation runners, inject lost replies and
  coordinator restart around ordinary-group/credential creation; recover one
  intent with the same replica mapping and credentials. Preexisting conflicting
  bindings fail; later unrelated groups are never selected. Invariant: C3,
  cluster-wide idempotency. Integration test.
- **A23.** Given completed preparation, explicitly stop a required service or retire its
  setup/delete a managed group; restart monitors and replay old work. The later
  intent/tombstone wins, readiness is unmet, and no service/group is recreated.
  Invariants: C3/C6, operator intent and observational checks. Integration test.
- **A24.** Given local one/three-node storage defaults, progress file registration and
  service startup; DiskIO can start while disks await Online and all dependencies
  eventually become usable. Inject ENOSPC, changed inode, supplied-media conflict
  and invalid geometry; each retains identity and reports its precise dependency
  without fallback or duplicate media. Invariant: C4, acyclic durable setup.
  Integration test.
- **A25.** Given ready local one/three-node clusters with published/overridden ports,
  use host clients for S3 put/get, Iceberg commit/read and Dataset publish/read;
  returned endpoints work, content survives recreation, credentials stay stable,
  and all member UIs report the same UUID. Invariant: C5, complete access contract.
  E2E test.
- **A26.** Given concurrent terminal failure, stale health, quorum loss and missing
  runtime facts, reduce typed observations in different arrival orders; aggregate
  state/reasons are deterministic, stale success never means ready, superseded
  failures do not invalidate recovered health, and unknown runtime facts alone
  do not invalidate readiness. Invariant: C6, current evidence.
  Unit test.
- **A27.** Given a remote cluster and deliberately slow/unreachable dependency endpoints,
  run JSON status/check with a finite deadline; total observation/polling fits
  the bound, check returns nonzero with reasons, no credential is emitted and
  no state mutation occurs. Healthy check exits zero. Invariant: C6, bounded
  observational interface. Integration test.
- **A28.** Given named, endpoint-only, conflicting and unreachable targets, resolve each
  before all retained workload families; reject mismatched identities or
  unreachable required transports before data writes, with no localhost/runtime
  fallback. Endpoint-only attach cannot stop/clean containers. Invariant: C7,
  target and lifecycle ownership separation. Integration test.
- **A29.** Given an interrupted start, changed daemon context and same-name foreign
  resources, retry start/stop/clean; only matching owned resources are reused,
  conflicts fail before destructive work, and a readiness timeout leaves nodes
  recoverable. Invariant: C8, recoverable runtime lifecycle. Integration test.
- **A30.** Given stopped image users, retained volumes, foreign/symlink paths and a
  reference added during cleanup, run scoped and explicit all cleanup; shared
  images/data survive and daemon errors retain retry records. Explicit data
  deletion affects only selected owned storage. Invariant: C8, conservative
  reclamation. Integration test.
- **A31.** Given a Linux host with no Docker client/socket/daemon, deploy the verified
  artifact through native containerd host networking; complete storage and
  S3/Iceberg/Dataset readiness and persisted recreation succeed. Record this
  separately from Docker-provisioned disposable-host tests. Invariant: runtime
  independence demonstrated by actual environment. E2E test.

- **A32.** Given a ready three-node cluster with retained data and identity-bound
  SSH trust, stop every container and restart in a different order with changed
  IPs and no stable names or peer list; authenticated rediscovery restores the
  original replica routes, quorum, UUIDs, credentials and S3/Iceberg/Dataset data.
  Repeat on host networking within a working multicast scope. Invariant: C2,
  identity-bound recovery without current quorum. E2E test.
- **A33.** Given retained member trust, advertise a copied node-id with another
  key, a foreign bootstrap, a cancelled member or duplicate authenticated live
  identities; recovery refuses the candidate without replacing trust, expanding
  votes or resetting state. Missing/corrupt trust cannot fall back to TOFU.
  Verify successful address recovery changes neither accepted membership nor
  bootstrap inputs. Invariant: C2, recovery grants no membership authority.
  Integration test.

Run these gates for the implemented scope; runtime E2E results must state the
actual hosts, runtime and image digest. No RDMA hardware qualification gate is
required. Existing profile names below are entry points to migrate, not evidence
that the new cases already exist. Phase 6 must register A01–A33 in the appropriate
fixture/test suites and CI jobs. A31 additionally requires a recorded Docker-free
host run; Docker-hosted profile results cannot substitute for it:

```bash
pixi run clean-env && pixi run cargo test -p crowdb-protocol
pixi run clean-env && pixi run cargo test -p crowdb-monitor
pixi run clean-env && pixi run cargo test -p crowdb-console-shared
pixi run clean-env && pixi run cargo test -p crowdb-web
pixi run clean-env && pixi run cargo test -p crowdb-cli
pixi run test-oci-image
pixi run test-container-e2e-fixture
pixi run test-container-e2e --layer deployment --concurrency 1
pixi run test-container-e2e --scope console.cli --concurrency 1
pixi run test-container-e2e --profile bridge-admission --concurrency 1 --node-cpus 1 --cpus 4 --memory-mib 6144
pixi run test-container-e2e --profile host --concurrency 1 --cpus 4 --memory-mib 4096
pixi run test-container-e2e --profile containerd-host --concurrency 1 --cpus 4 --memory-mib 4096
pixi run test-container-e2e --profile containerd-cluster --concurrency 1 --node-cpus 1 --cpus 4 --memory-mib 6144
pixi run check-ci-test-tasks
pixi run rs-fmt-check
pixi run rs-lint
```
