<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R187: deployment — Single-node Docker preview

## Problem

CROWDB has separate production binaries for KV, disk, chunk, S3, Iceberg, and
console responsibilities, but an evaluator cannot currently start a usable
instance with one container command. They must build the workspace, discover
an internal process order, provision topology and storage metadata, initialize
credentials and an Iceberg catalog, and keep several processes alive. This
blocks the development, demonstration, CI, and client-interoperability use cases
defined by the source Docker release brief provided for this requirement.

The existing full-stack harness proves many components together, but its
in-process setup and temporary paths are not a distributable runtime contract.
The [user guide](../user-manual/user-guide.md) describes manual service
operation, while the [ChunkDB root design](../design/chunkdb/design-crowdb-chunkdb.md)
permits an explicit `unsafe_colocated` minimum topology without promising disk,
node, or zone fault tolerance. Packaging ad hoc test behavior would create a
second storage semantic, hide partial startup, expose internal ports, or lose
state on restart. The preview instead needs a bounded, reproducible composition
of the normal binaries with an honest non-production boundary.

`crowdb-web` also has an unresolved authority split that a container must not
preserve. Its current `--config` path loads one `ConsoleConfig`/`registry.toml`
containing racks, nodes, servers, stores, groups, process launch settings, and
management endpoints. Lifecycle handlers commit that local file first and then
attempt Group 0 sysdata updates best-effort, so a failed Group 0 write can leave
the UI and cluster divergent. Startup logs that Group 0 is authoritative when it
is ready, but `startup_topology_check` still calls the local
`restore_persisted_topology` path. The same file therefore mixes cluster
authority, bootstrap discovery, machine-local launch policy, runtime endpoint
hints, and UI state; two consoles can independently overwrite different local
truths. Group 0 already owns cluster topology and service registration, while
binary paths, SSH/local launch settings, PIDs, and monitor state are deployment
concerns. The Docker composition needs that boundary corrected rather than
backing up another `registry.toml` beside Group 0.

Concrete scenarios are a developer uploading and range-reading Parquet through
S3, a PyIceberg client using the enabled REST catalog and FileIO operations, an
operator viewing the same instance in the web console, and a CI job restarting
the container against the same volume before repeating those operations.

## Solution

The first image is a single-host preview for disposable development and
integration data. It is not a production, high-availability, upgrade-stable, or
fault-tolerant deployment.

- **DOCKER-I1 — One-command service:** one documented container invocation
  starts one usable CROWDB instance and exposes only S3 on port 16000, Iceberg
  REST/FileIO on port 8181, and the web console on port 14000.
- **DOCKER-I2 — Product-path fidelity:** the image runs the normal
  `crowdb-kv-server`, `crowdb-diskdb`, `crowdb-diskio`, `crowdb-chunkdb`,
  `crowdb-chunk-kv-server`, `crowdb-access-server`, `crowdb-iceberg`, and
  `crowdb-web` binaries. Docker-only code may compose and bootstrap them but may
  not replace their protocol or persistence semantics.
- **DOCKER-I3 — One durable boundary:** all durable database files, topology,
  bootstrap state, credentials, and bounded rotating logs live below the single
  `/opt/crowdb/data` mounted data root. Executables and packaged UI/config
  templates are immutable image content; generated runtime configs, sockets,
  status, and process IDs live below `/opt/crowdb/run` and are disposable.
- **DOCKER-I4 — Dependency-gated readiness:** container readiness becomes true
  only after durable bootstrap is complete and KV, disk, chunk, S3, Iceberg, and
  web probes all confirm the same instance is usable. A live PID is not proof of
  readiness.
- **DOCKER-I5 — Monitored recovery:** a dedicated `crowdb-monitor` daemon is PID
  1 and the sole owner of every child process. It detects unexpected exits and
  failed bounded liveness probes, drops readiness before recovery, terminates and
  reaps the old process, and restarts the failed process plus affected dependents
  in dependency order with the same durable identity and configuration. Restart
  attempts use bounded backoff and a finite crash-loop budget; exhaustion
  terminates the
  container nonzero so the container runtime's restart policy can recreate it.
  `SIGTERM` disables restart, stops external admission first, drains bounded
  work, stops dependent services in reverse order, and leaves restartable
  durable state.
- **DOCKER-I6 — Secret boundary:** the image contains no baked-in credentials or
  fixed production secrets. Secret values never appear in image layers, command
  arguments, health output, or ordinary logs.
- **DOCKER-I7 — Honest preview:** startup output, UI, examples, labels, and
  release metadata identify this as a single-node non-production preview and
  advertise only capabilities proven by its pinned acceptance matrix.
- **DOCKER-I8 — Runtime-only initialization:** the image contains no initialized
  disk, topology, group, tenant, catalog, or bootstrap manifest. On an empty data
  root, `crowdb-monitor` creates Group 0, Group 1, and the remaining topology at
  runtime. On a complete existing data root it performs validation only and
  never reissues creation. An interrupted initialization resumes with the same
  durable identities; unknown or conflicting existing state fails without
  mutation.
- **DOCKER-I9 — Convenient authenticated access:** on first boot the monitor
  generates strong S3 and Iceberg credentials, stores them only in the mounted
  secret directory with mode 0600, and exposes client credentials through an
  explicit local retrieval command. Startup logs print the retrieval command and
  public endpoints but never credential values.
- **DOCKER-I10 — Honest multi-disk simulation:** the preview provisions one node,
  one disk group, and four 16 GiB sparse file-backed disks with one zone each.
  Each registered disk has its own stable identity and backing file so DiskDB and
  DiskIO exercise a normal multi-disk topology. All files remain on one host
  filesystem and are not presented as replica or independent failure-domain
  durability.
- **DOCKER-I11 — One configuration authority:** Group 0 is the sole durable
  authority for cluster topology and service registration. `crowdb-web.toml`
  contains only web-process startup policy. `registry.toml`, when used outside
  this image, contains only machine-local launch records and cannot override or
  restore Group 0 state. Container mode has no `registry.toml` and never falls
  back to local topology after Group 0 exists.
- **DOCKER-I12 — Verifiable preview publication:** release-tag workflows publish
  the gated `linux/amd64` image to one public Docker Hub repository only after
  manual approval. Version and `git-<commit>` tags are immutable; `preview` is the
  sole moving convenience tag and `latest` is not published. Every public digest
  has a verifiable signature, SBOM, and build provenance. Pull-request workflows
  build and test but have no publication authority.
- **DOCKER-I13 — Deployment-profile boundary:** container implementation lives
  under the repository-root `container/` directory. `crowdb-monitor` provides a
  topology-neutral process graph, supervision, probe, rendering, and bootstrap
  runtime; the named **CROWDB Single-Node Preview** profile supplies this
  requirement's two groups, four file disks, services, ports, and paths. A future
  multi-node image or bare-metal launcher can reuse the monitor without adding
  single-node policy branches to its supervision core.

```text
crowdb-monitor (PID 1) -> start / probe / restart every process
  host clients -> S3 access server -----------+
               -> Iceberg REST/FileIO --------+-> Chunk-KV -> ChunkDB -> DiskDB/DiskIO
               -> web console ----------------+       |
                                                      +-> one KV server
                                                          +-> Group 0: system
                                                          +-> Group 1: data
```

The source layout for this deployment is:

```text
container/
  crowdb-monitor/              reusable deployment runtime crate and binary
  single-node-preview/         CROWDB Single-Node Preview profile
    Dockerfile                 amd64 multi-stage image
    templates/                 profile-owned service configuration inputs
    tests/                     profile and container acceptance assets
```

Only the current profile is created by R187. Later container profiles may add
sibling directories; a later bare-metal requirement may package the same monitor
without moving or duplicating its runtime code.

### Web configuration authority

- **Group 0:** owns racks, nodes, disk groups, disks, stores, groups, replicas,
  bindings, and the service registry. Web topology reads and mutations use Group
  0 directly; a successful local file write cannot substitute for a failed Group
  0 mutation. When Group 0 is unavailable after initialization, topology APIs
  fail unavailable rather than serving or restoring a local topology copy.
- **`crowdb-web.toml`:** is a versioned, non-secret process configuration. It
  contains the web bind address and port, Group 0 management seeds, packaged UI
  root, monitor status endpoint, log policy, request bounds, and a mode selecting
  monitor-managed or standalone operation. It contains no racks, nodes, stores,
  groups, replicas, service inventory, PIDs, binary paths, credentials, or SSH
  material. In this image `crowdb-monitor` renders it at
  `/opt/crowdb/run/config/crowdb-web.toml` on every start and invokes
  `crowdb-web --config` with that path.
- **`registry.toml`:** is an optional, versioned standalone deployment registry,
  selected only by a separate `crowdb-web --registry` option. It may map stable
  Group 0 node/service identities to machine-local connection and launch policy:
  host, SSH credential reference, binary and service-config path, workspace, and
  auto-start choice. It stores no topology relationships, stores, groups,
  replicas, authoritative service endpoint, health, PID, monitor state, UI
  preference, or inline secret. Container monitor-managed mode rejects a
  registry path because `crowdb-monitor` owns every process.
- **Runtime/UI state:** live endpoints come from Group 0 service discovery;
  process PID, restart generation, and crash-loop state come from
  `crowdb-monitor`; browser-only preferences remain in browser storage. None is
  copied into either TOML file.
- **Unreleased format replacement:** the current mixed `ConsoleConfig` format is
  not a compatibility surface because CROWDB has not released it. Remove its
  parser, writer, restore behavior, fixtures, and documentation in the same
  change; do not add a migration tool, dual reader, fallback, or schema alias.
  Existing development files are unsupported inputs and may be deleted.

The container filesystem contract is:

```text
/opt/crowdb/bin/              immutable executables
  crowdb-monitor
  crowdb-kv-server
  crowdb-diskdb
  crowdb-diskio
  crowdb-chunkdb
  crowdb-chunk-kv-server
  crowdb-access-server
  crowdb-iceberg
  crowdb-web
/opt/crowdb/ui/               immutable compiled web UI
/opt/crowdb/etc/templates/    immutable service config templates

/opt/crowdb/data/             one required host bind mount or named volume
  bootstrap/manifest.json        durable initialization state and identities
  secrets/server.env             internal master keys and privileged tokens, 0600
  secrets/client.env             retrievable S3/Iceberg client credentials, 0600
  kv/node-1/
    waldata/                     Group 0 and Group 1 WAL
    ctdata/                      Group 0 and Group 1 KV engine data
    conf/                        KV fixed-layout state
  disks/
    disk-0001.img               sparse 16 GiB file disk / one zone
    disk-0002.img               sparse 16 GiB file disk / one zone
    disk-0003.img               sparse 16 GiB file disk / one zone
    disk-0004.img               sparse 16 GiB file disk / one zone
  log/                           bounded rotating per-process logs
    monitor/
    kv/
    diskdb/
    diskio/
    chunkdb/
    chunk-kv/
    s3/
    iceberg/
    web/

/opt/crowdb/run/              disposable; recreated on every container start
  config/
    crowdb-web.toml               rendered web process config; no registry
    diskdb.toml                   rendered DiskDB process config
    diskio.toml                   rendered DiskIO process config
    chunkdb.toml                  rendered ChunkDB process config
    chunk-kv.toml                 rendered Chunk-KV process config
  pid/                             child PID records
  status/                          monitor liveness/readiness and restart state
  ports/                           internal port claims
```

The image prepends `/opt/crowdb/bin` to `PATH` and uses
`/opt/crowdb/bin/crowdb-monitor` as its entrypoint, so documented commands can
use short executable names without searching the filesystem.

A user supplies one host path, for example
`-v /host/crowdb:/opt/crowdb/data`. Database recovery requires `bootstrap/`,
`secrets/`, `kv/`, and `disks/`; `log/` is persisted for post-crash diagnosis but
can be excluded from backups. No web registry belongs in the backup. Group and
service metadata use the KV and Chunk-KV authorities rooted in `kv/node-1`, while
S3 object bytes and native Iceberg file bytes are distributed through
DiskDB/DiskIO across the four files in `disks/`; DiskDB, ChunkDB, and the access
services do not invent additional local durable roots. Each log file is limited
to 30 MiB with five rotated files, and warning/error output is also mirrored to
container stderr. `/opt/crowdb/run` and all image paths are never
part of a data backup. The monitor sets `CROWDB_RUNTIME_ROOT=/opt/crowdb/run` and
passes explicit data and log paths to every child.

1. Add a reproducible `linux/amd64`-only multi-stage image build. The first
   preview publishes no arm64 image or multi-architecture manifest. The build
   stage uses the repository's pinned Rust, C++, and UI dependency inputs to
   produce release binaries and installs the `crowdb-web` static UI under
   `/opt/crowdb/ui`; the web service must resolve that packaged runtime
   path rather than a build-workspace path. The build must not execute database
   initialization or copy any generated disk, topology, Group 0, Group 1, tenant,
   catalog, credential, or bootstrap-manifest state into an image layer. The
   runtime stage is based on a digest-pinned `ubuntu:24.04`, contains only
   required runtime libraries and artifacts, runs as a dedicated non-root user,
   and records the source revision and preview version in OCI labels. Build
   context excludes local runtime data, credentials, test output, VCS data, and
   unrelated build products.
2. Add a dedicated `crowdb-monitor` deployment daemon as the image entrypoint
   and PID 1. It owns configuration validation, runtime initialization, child
   creation and reaping, process and functional-liveness monitoring, restart
   backoff and budgets, readiness aggregation, signal handling, and shutdown. It
   classifies the durable root before mutation, validates every path, address,
   capacity, and secret input before starting storage, and never interprets S3
   or Iceberg requests. A replacement child may start only after the prior PID is
   reaped and its listeners are no longer serving; the daemon never permits
   overlapping owners of one durable identity.
3. Drive the minimum topology through existing management and client APIs only
   at runtime. For an empty data root, `crowdb-monitor` durably records a
   versioned `Initializing` manifest with generated stable identities, starts the
   minimum dependencies needed for management calls, and creates one rack, one
   node, one disk group, four stable disk identities backed by the 16 GiB sparse
   files `/opt/crowdb/data/disks/disk-0001.img` through `disk-0004.img`, one
   DiskDB instance, one `unsafe_colocated` ChunkDB placement domain, one Chunk-KV
   service, one S3 tenant, one active Iceberg catalog, and one KV server with
   exactly two groups.
   Group 0 is the system group and owns topology, service, and other system
   authority; Group 1 is the data group and owns user data routed by the preview.
   Each step is replay-safe and advances the manifest until it is durably
   `Ready`. If startup finds `Initializing`, the monitor resumes with the same
   identities and operation inputs. If startup finds `Ready`, it makes no create
   call: it starts services against existing state and validates both group
   identities, roles, bindings, tenant, catalog, and disk before readiness. A
   non-empty root with a missing, corrupt, unsupported, or conflicting manifest
   fails without creating, replacing, or truncating anything.
4. Start dependencies in probe order: the KV server with ready Group 0 and Group
   1, DiskDB and DiskIO, ChunkDB, Chunk-KV, S3 and Iceberg access processes, then
   `crowdb-web`. Each stage has a bounded deadline and emits a diagnostic naming
   the failed component. Partial startup never reports ready. On crash or failed
   liveness, `crowdb-monitor` first marks the instance unready, stops affected
   dependents, restarts the failed layer, revalidates its durable authority, and
   then restarts dependents in this same order. Internal management, RPC, and
   health listeners bind only to the container network namespace and are not
   declared as public image ports.
5. Configure `crowdb-access-server` with normal S3 authentication on
   `0.0.0.0:16000`, `crowdb-iceberg` with its independent authenticated catalog
   and native FileIO listener on `0.0.0.0:8181`, and `crowdb-web` on
   `0.0.0.0:14000`. The quick start maps these ports one-to-one and uses
   `http://localhost:16000`, `http://localhost:8181`, and
   `http://localhost:14000`. `CROWDB_ICEBERG_PUBLIC_URI` defaults to the local
   Iceberg URI and is the one documented override when a remote hostname,
   reverse proxy, or different host-port mapping changes the client-visible
   address. S3 buckets and credentials do not select or authorize Iceberg
   resources.
6. Make authentication automatic but explicit. On a fresh volume,
   `crowdb-monitor` generates the S3 master key, one preview S3 access-key pair,
   and four distinct Iceberg read/write/manage/clear bearer tokens required by
   the existing services. It writes all server-only material to
   `/opt/crowdb/data/secrets/server.env` and writes only client endpoints, region,
   S3 access key/secret, and the Iceberg writer token to
   `/opt/crowdb/data/secrets/client.env`; both files are owned by the container
   user with mode 0600 and are reused unchanged after restart. The local command
   `crowdb-monitor credentials show --format env` prints `client.env` only when
   explicitly invoked, so the quick start can use
   `docker exec crowdb crowdb-monitor credentials show --format env` while normal
   startup logs reveal only that command. Authentication is necessary because S3
   requires SigV4 credentials and the Iceberg server requires distinct bearer
   roles; automatic generation removes that setup burden without disabling either
   protocol boundary.
7. Refactor `crowdb-web` and `crowdb-console-shared` around the configuration
   authority contract before shipping the compiled UI. Replace the current mixed
   `ConsoleConfig` load with distinct versioned web-process and optional launch
   registry models; make `--config` and `--registry` unambiguous; and remove the
   old parser, writer, restore path, fixtures, and docs without compatibility
   handling. Topology handlers commit Group 0 first and refresh their read model
   only after success; they never persist topology
   locally or ignore a Group 0 failure. Startup uses configured seeds to load
   Group 0 and service discovery rather than calling local
   `restore_persisted_topology` once Group 0 exists. In monitor-managed mode the
   console has no registry engine, overlays `crowdb-monitor` process/restart state
   onto Group 0 service records, and rejects process-lifecycle mutations because
   the monitor is the sole process owner. The web UI displays source and stale/
   unavailable status instead of presenting a local fallback as authoritative.
8. Enforce the mounted data-root contract and subtree ownership shown above.
   Starting without `/opt/crowdb/data` requires an explicit disposable mode;
   otherwise startup fails before writing data. Empty-root detection cannot treat
   a non-empty directory as fresh merely because its manifest is absent. Reject
   missing-on-non-empty, corrupt, unsupported, or state-conflicting bootstrap
   manifests and on-disk layout versions without mutation. The four disk
   identities, backing paths, one-zone layouts, and 16 GiB per-disk capacities
   are fixed in the first bootstrap manifest; adding, removing, replacing, or
   resizing a disk is unsupported in this preview. This preview does not promise
   in-place upgrade compatibility until a later requirement defines it.
9. Add container-level liveness and readiness commands. Container liveness
   proves `crowdb-monitor` is responsive and its event loop is advancing without
   contacting external networks. The monitor separately runs bounded functional
   liveness probes for every child rather than treating a PID as healthy.
   Readiness checks the web `/healthz`, KV leadership and topology, storage
   registration, S3 health, Iceberg `/v1/config`, completion of bootstrap, and
   absence of an active restart. Probes use internal least-privilege credentials,
   bounded timeouts, per-process failure thresholds, and disclose no secrets.
10. Add a hermetic Docker acceptance harness and pixi tasks for image build and
    test. It starts from an empty named volume, waits for readiness, runs AWS CLI
    and boto3 object PUT/LIST/HEAD/range-GET/GET against a Parquet fixture, runs
    the pinned PyIceberg operations currently enabled by R184, checks the web UI
    and API, restarts the container with the same volume, and repeats reads and
    catalog loads. It also tests first-start runtime initialization, interruption
    and replay after every initialization step, restart with a `Ready` manifest
    without creation calls, invalid configuration, unavailable dependency, every
    required-process crash and liveness hang, successful monitored restart,
    crash-loop budget exhaustion, monitor failure, `SIGTERM`, wrong secrets,
    read-only/unwritable volume, and missing, corrupt, incompatible, or conflicting
    bootstrap manifest outcomes.
11. Publish a minimal quick start that pins an image tag, maps ports 16000, 8181,
    and 14000, mounts one host data path at `/opt/crowdb/data`, configures the
    container runtime restart policy for monitor-budget exhaustion, retrieves
    generated preview credentials with the explicit monitor command, and includes
    independent S3 and Iceberg examples. The compatibility list names exact
    tested client versions and operations; pure Parquet-over-S3 results are not
    presented as Iceberg conformance.
12. Add separate CI build/test and release workflows. Pull requests build the
    amd64 image and run all Docker gates without registry write credentials. A
    Git release tag reruns the complete gates for the exact commit, waits for
    manual approval, then publishes to the public Docker Hub repository under an
    immutable release-version tag, immutable `git-<commit>` tag, and moving
    `preview` tag. The workflow never emits `latest`, refuses to overwrite either
    immutable tag, and attaches a signature, SBOM, and build provenance to the
    published digest. arm64 publication is deferred until a later requirement
    supplies a Linux arm64 toolchain and the complete Docker E2E matrix.
13. Keep reusable deployment mechanics in the `container/crowdb-monitor` crate
    and every single-node decision in `container/single-node-preview`. The
    monitor consumes a validated profile to construct its dependency graph,
    render configs, bootstrap authorities, and aggregate health; it does not
    infer topology from its executable name or Docker environment. Unit tests
    exercise the runtime with synthetic profiles, while Docker acceptance uses
    only the named single-node profile. Do not create placeholder multi-node or
    bare-metal implementations in R187.

## Dependencies

- Depends on the delivered S3 baseline R152 through R166 and its restart-safe
  object path. R167 multipart upload, R168 shared-object reclamation, R169 shared
  chunk-tree GC, and R170 RDMA are not required for the initial image and must
  not be implied by its capability claims.
- Depends on R177's native Iceberg authority and the implemented R178 through
  R183 functionality. R183's opt-in GC and deferred shared-range deletion remain
  visible limitations. The image may expose only the R184 routes and client
  operations that pass the pinned container matrix; it cannot close or bypass
  R184. R185 caching and R186 ORC validation are not dependencies.
- Reuses existing process binaries, management APIs, service registration,
  health endpoints, runtime-root conventions, and the compiled
  `app/crowdb-web/ui` artifact. R187 owns the required `crowdb-web`/
  `crowdb-console-shared` configuration split and Group 0 authority cleanup;
  retaining the current mixed `ConsoleConfig` as a container fallback is not
  permitted. Missing composition or probe APIs are added to their owning modules
  rather than duplicated in shell parsing.
- Reuses `unsafe_colocated` only as the explicit minimum-topology placement
  policy. Its loss-of-resource durability limitation must remain visible in the
  image metadata, quick start, and UI.
- Requires a Docker-capable `linux/amd64` acceptance runner. If Docker or native
  amd64 execution is unavailable in ordinary CI, the pixi test task must fail
  with a clear prerequisite message or run in a separately declared amd64
  container job; it must not silently skip release acceptance.
- Public release requires one Docker Hub repository, a protected release
  environment holding write credentials and signing identity, and CI support for
  attached SBOM and provenance artifacts. Missing or unauthenticated publication
  infrastructure blocks release rather than producing an unsigned or partially
  described image; pull-request testing remains available without it.

## Acceptance

- Given a clean checkout on a `linux/amd64` runner, when the amd64 image is built
  twice from identical locked inputs through pixi, assert both builds publish no
  arm64 image or multi-architecture manifest and contain the expected release
  binaries and UI, pinned Ubuntu runtime, non-root user, and revision labels, but
  contain no source/build/secret files or
  initialized disk, topology, group, tenant, catalog, or bootstrap state; record
  and gate any permitted nondeterministic metadata. Invariants: DOCKER-I2,
  DOCKER-I6, and DOCKER-I8. Integration test.
- Given two synthetic deployment profiles and the CROWDB Single-Node Preview
  profile, when monitor graph construction, config rendering, probes, restart
  ordering, and bootstrap dispatch run, assert reusable behavior depends only on
  validated profile inputs, all two-group/four-disk/port/path choices live in the
  single-node profile, and no multi-node or bare-metal placeholder is required.
  Invariant: DOCKER-I13. Unit test.
- Given an empty mounted volume and valid explicit configuration, when the
  container starts, assert `crowdb-monitor` creates an `Initializing` manifest
  at runtime, drives exactly Group 0 as the system group and Group 1 as the data
  group on the single KV server, advances the manifest to `Ready`, and keeps
  readiness false until both groups, every required process, topology binding,
  S3 tenant, and active Iceberg catalog are usable; then assert only S3, Iceberg,
  and web endpoints are reachable from the host. Invariants: DOCKER-I1,
  DOCKER-I4, and DOCKER-I8. E2E test.
- Given one host directory mounted at `/opt/crowdb/data`, when first bootstrap and
  representative S3 and Iceberg writes complete, assert all durable state uses
  only the documented bootstrap, secrets, kv, disks, and log subtrees; no
  `registry.toml` or console topology copy exists; all generated configs, PID,
  status, and port claims use `/opt/crowdb/run`; executables, templates, and UI
  remain immutable; and process logs are bounded and rotated.
  Invariant: DOCKER-I3. Integration test.
- Given valid and invalid versioned `crowdb-web.toml` and `registry.toml` fixtures,
  when each is decoded in its permitted mode, assert web configuration accepts
  only process settings, standalone registry accepts only secret references and
  launch policy, forbidden topology/runtime/inline-secret fields fail closed,
  and monitor-managed mode rejects every registry path. Invariant: DOCKER-I11.
  Unit test.
- Given two consoles connected to one ready Group 0, when topology mutations
  succeed, conflict, lose their response, or encounter unavailable Group 0,
  assert both consoles converge on Group 0 after success, preserve conflict and
  retry semantics, commit no local topology before authority, and return an
  explicit unavailable result without serving a local fallback. Invariant:
  DOCKER-I11. Integration test.
- Given ready Group 0 and any supplied registry path, when monitor-managed
  `crowdb-web` starts, assert it rejects the registry path; with no registry it
  uses configured seeds, Group 0 topology, service discovery, and monitor runtime
  state, never invokes local topology restore, and marks unavailable/stale
  sources accurately. Invariant: DOCKER-I11. E2E test.
- Given the repository's former mixed `ConsoleConfig` files, fixtures, restore
  calls, and documentation, when the configuration split lands, assert none
  remain in production or test paths and no migration, dual-read, fallback, or
  alias accepts that unreleased format. Invariant: DOCKER-I11. Integration test.
- Given the fresh single-node topology, when storage registration, direct
  per-disk write/read, and filesystem allocation are inspected, assert exactly
  one disk group contains four stable disk identities backed one-to-one by
  `disks/disk-0001.img` through `disk-0004.img`, every disk has one 16 GiB zone,
  every initial file allocation is sparse, all four disks serve correct bytes,
  and no replica or independent-failure-domain claim is emitted. Invariant:
  DOCKER-I10. E2E test.
- Given no supplied credentials on first start, when bootstrap completes and the
  explicit `crowdb-monitor credentials show --format env` command is run, assert
  server and client files are mode 0600, only client credentials are printed,
  AWS CLI and PyIceberg authenticate with them, restart preserves the same
  values, and image layers, process arguments, probes, status, and ordinary logs
  contain none of those values. Invariants: DOCKER-I6 and DOCKER-I9. E2E test.
- Given default one-to-one mappings and then an overridden external Iceberg URI,
  when clients discover and call all public services, assert S3 is available at
  port 16000, Iceberg REST/FileIO at 8181, web at 14000, no internal listener is
  host-reachable, and Iceberg advertises the configured client-visible URI.
  Invariants: DOCKER-I1 and DOCKER-I7. E2E test.
- Given first-time initialization is interrupted after each durable step, when
  the container restarts with the `Initializing` volume, assert the monitor
  replays with the same identities and inputs, creates no duplicate authority,
  reaches `Ready`, and serves S3, Iceberg, and web successfully. Invariants:
  DOCKER-I3 and DOCKER-I8. E2E test.
- Given the P0 S3 client matrix and a Parquet object larger than 1 MiB, when AWS
  CLI and boto3 upload, list, head, range-read, and download it, assert bytes and
  metadata match and the request traverses the normal large-object path.
  Invariants: DOCKER-I1 and DOCKER-I2. E2E test.
- Given the pinned PyIceberg profile and only capabilities enabled by R184, when
  the client discovers configuration and performs the advertised namespace,
  table, metadata, and FileIO workflow, assert standard results are readable
  after reconnect and no general S3 bucket authority is used for the catalog.
  Invariants: DOCKER-I2 and DOCKER-I7. E2E test.
- Given a ready instance, when a child restart is triggered and a conflicting
  lifecycle action is attempted through the web console, assert the single
  bootstrapped topology, process health, monitor restart state, and external
  endpoints are visible, the lifecycle action returns unsupported, and no
  duplicate process is spawned. Invariants: DOCKER-I1, DOCKER-I4, and DOCKER-I5.
  E2E test.
- Given a `Ready` volume with successful S3 and Iceberg writes, when the
  container receives `SIGTERM` and is recreated with the same volume and
  configuration, assert shutdown is bounded, `crowdb-monitor` issues no topology,
  group, tenant, or catalog creation call, validates and reuses every persisted
  identity, restores readiness, and loads prior objects and tables with identical
  bytes and metadata. Invariants: DOCKER-I3, DOCKER-I5, and DOCKER-I8. E2E test.
- Given no mounted data root outside explicit disposable mode, an unwritable or
  read-only root, a non-empty root with no manifest, a corrupt or incompatible
  manifest, conflicting topology, or an invalid capacity/endpoint, when startup
  is attempted, assert it fails before mutation, creates no group or authority,
  and names the corrective input without exposing secrets. Invariants: DOCKER-I3,
  DOCKER-I6, and DOCKER-I8. Integration test.
- Given each required child process is killed and then made liveness-unresponsive
  in turn, when `crowdb-monitor` observes it, assert readiness drops within the
  bound, the prior PID is reaped, affected dependents stop, the failed layer and
  dependents restart in dependency order with unchanged durable identities, and
  readiness returns only after S3, Iceberg, and web operations succeed. Invariants:
  DOCKER-I4 and DOCKER-I5. E2E test.
- Given one child repeatedly exits or fails liveness beyond its configured
  restart budget, when bounded backoff is exhausted, assert no overlapping child
  instance was started, the container never returns ready, diagnostics identify
  the crash loop without secrets, and `crowdb-monitor` exits nonzero so the
  container restart policy can act. Invariant: DOCKER-I5. E2E test.
- Given `crowdb-monitor` itself stops or its event loop ceases advancing, when the
  container liveness contract is evaluated, assert PID 1 termination stops the
  container or the liveness probe fails without reporting the child processes as
  healthy. Invariants: DOCKER-I4 and DOCKER-I5. E2E test.
- Given image inspection, startup output, web UI, quick start, S3 examples, and
  Iceberg examples, when release metadata is checked, assert all surfaces say
  single-node non-production preview, list unreclaimed-space and durability
  limitations, and claim only client/version operations proven by the matrix.
  Invariant: DOCKER-I7. Integration test.
- Given a pull request workflow run, when image build and Docker gates complete,
  assert the amd64 artifact is test-only, the job has no Docker Hub publication
  credentials, and no public tag or digest is created. Invariant: DOCKER-I12.
  Integration test.
- Given a Git release tag for a commit, when any required gate fails, approval is
  absent, or an immutable version/commit tag already names another digest, assert
  publication stops without moving a public tag. When all gates and approval
  succeed, assert the public Docker Hub digest is amd64-only, has immutable
  release-version and `git-<commit>` tags plus the moving `preview` tag, has no
  `latest` tag, and its signature, SBOM, and build provenance verify against the
  exact source commit. Invariant: DOCKER-I12. Integration test.

Required gates:

- `pixi run build-docker-preview`
- `pixi run test-docker-preview`
- `pixi run -e s3-e2e test-boto3-e2e`
- `pixi run -e iceberg-e2e test-pyiceberg-e2e`
- `pixi run test-console`
- `pixi run test-console-ui`
- `pixi run rs-fmt-check`
- `pixi run rs-lint`
