<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# E2E tests

The root-level `e2e-test/` is the unified E2E project. Its Rust workspace package
is `e2e-test`; server domains use separate test modules within this crate.
The Python runner owns container lifecycle and scheduling. Browser and
SDK clients with independent language dependencies live under `clients/<client>/`
and consume the same fixture endpoints and ownership contract. They do not
introduce separate server launchers. Domain selection and measured remaining issues are tracked in
[the test record](../doc/working/test.md).

Test clients run on the host against inspected node-container IPs and fixed
ports, with three node containers for a three-node cluster. Fresh one- through eight-node profiles exercise management, membership,
async operations and multi-store routing. Host bridge IP reachability is
required; remote Docker and Linux-VM routing are not implicitly supported.
Containers supply server subsets or clusters from the formal release image;
they are not test execution environments. Domain profiles configure that same
server artifact rather than introducing separate test-server builds.

This component owns real-server acceptance. The KV
profile uses three packaged `crowdb-kv-server` processes with block tree storage,
block-device WAL and the existing `e2e` election preset. It consumes the shared
`crowdb-node` OCI runtime image; it never starts a host server substitute.

## Run

```sh
pixi run test-container-e2e-fixture
pixi run test-e2e-kv
pixi run test-e2e-diskdb
pixi run test-e2e-diskio
pixi run test-e2e-chunkdb
pixi run test-e2e-chunk-client
pixi run test-e2e-chunk-stream
pixi run test-e2e-chunk-kv
pixi run test-e2e-s3
pixi run test-e2e-iceberg
pixi run test-e2e-dataset
pixi run test-e2e-console
pixi run test-e2e-deployment
# The shared host dispatcher also accepts subcommands:
pixi run test unit core
pixi run test e2e s3 --case s3.sdk::java
pixi run test profile containerd-cluster
# Explicit Linux host-network admission/recovery; run on an exclusive host:
pixi run sudo "$(command -v pixi)" run --as-is test-container-e2e --profile host --concurrency 1 --cpus 4 --memory-mib 4096
# Native containerd on an exclusive Docker-free Linux host:
pixi run sudo "$(command -v pixi)" run --as-is test-container-e2e --profile containerd-host --concurrency 1 --cpus 3 --memory-mib 3072
pixi run test-container-e2e --profile containerd-cluster --concurrency 1 --node-cpus 1 --cpus 4 --memory-mib 8192
# Manual discovery/SSH/bootstrap and four host browser acceptance flows:
pixi run bash -c 'cd e2e-test/clients/console-ui && npm ci --ignore-scripts'
pixi run test-container-e2e --profile bridge-admission --concurrency 1 --node-cpus 1 --cpus 4 --memory-mib 6144
# Select one exact client case or list available profiles without Docker:
pixi run test-container-e2e --case kv::bootstrap::custom_replica
pixi run test-container-e2e --list --scope kv.group0
# Optional parent outside the default artifact directory:
pixi run test-container-e2e --layer kv --data-root /path/to/e2e-data
```

Build/import the shared OCI image first. An absent image, missing Docker daemon,
unsupported daemon, changed release server sources or inadequate budget fails the
required test. There is no successful missing-prerequisite skip.
`--layer all` selects registered KV, Diskdb, Diskio, Chunkdb, chunk-client,
chunk-stream, Chunk-KV, S3, Iceberg, Dataset, Console and deployment cases.
The host profile retains its owned bind-mounted data/log/crash root and an exact
image/source record; fixed ports require an exclusive Linux host. It is an
explicit deployment acceptance and is excluded from ordinary bridge domains.
The Docker host and native containerd host profiles qualify two explicit policies.
A one-voter production diskless node exercises the common CLI's accepted inputs,
authenticated admission, current metadata authority and retained recreation. This
metadata-only configuration does not claim whole-cluster storage readiness.
A Local one-voter node exercises all three access protocols and retained data,
identity and credentials through ordinary container recreation, with zero node
failure tolerance. Each node has a 1-CPU quota and 2048 MiB memory limit.
Protected production storage requires independent voters; the separate three-host
acceptance verifies that contract. The Docker host profile also retains file/raw
fsync, Offline, Remove/Rejoin, exclusive-descriptor and physical-tail assertions.
These profiles require an explicitly privileged disposable Linux host for owned
fixture setup; the node container receives no blanket privilege.

Containerd profiles require the installed `container/oci-image` runtime Pixi
environment and a verified OCI archive/receipt. `containerd-host` starts its own
rootful native-snapshotter daemon and rejects a Docker client, socket, daemon or
container-provisioned host. `containerd-cluster` separately labels its three
Docker-provisioned privileged Linux hosts; it proves adapter/discovery/quorum
behavior and does not substitute for the native-host gate. Both retain private
data and complete logs. Clients execute outside the formal node image.
The Local host fixture selects an 8-GiB sparse managed file with a 1-GiB
host reserve. Sparse files are confined to the explicit Local policy. `CROWDB_E2E_CLI_BUNDLE` supplies a prebuilt host CLI and
`lib/` directory when the host cannot build clients; Docker media probes can
likewise use `CROWDB_E2E_DOMAIN_BUNDLE` with `domain-test` and `lib/`.

DiskIO process-exit faults retain their network
namespace, so the original 15-second stream/reader/repair bounds still apply.

Chunkdb lifecycle, placement and recovery use five owned containers: three KV
voters, one Diskdb and one production-mode Chunkdb. Actual inspected KV replicas
are registered through the public metadata API. Allocation transport acceptance
needs only KV and Diskdb. The placement profile seeds four distinct failure
domains and checks metadata geometry. The storage profile owns eleven containers:
three KV, Diskdb, Chunkdb and six Diskio failure domains, with eighteen disks.
Only process-exit scenarios add six release-image sleep processes to retain
DiskIO network namespaces: killing a storage service closes its listener without
removing its IP. These scenarios reserve an additional 0.3 CPU / 96 MiB and
remove namespace owners after their service containers. Ordinary storage cases
use the eleven service containers directly.
The multislot profile owns three Chunkdb service instances, three KV voters and
Diskdb. Storage cases verify public conversion, partial bytes, exact parity,
expired foreground claim takeover, four missing-shard repair, degraded node-return
repair and exact relocation journal copy/publication/source-free ordering. All eleven
Chunkdb cases and their source retirement pass. Important internal task, journal,
fence and placement proofs remain in the Chunkdb component with pinned memory
metadata and their original assertions.
S3 joint-node faults use three actual KV voters and three independent
Diskdb/Chunkdb/DiskIO failure domains, plus the catalog and Access service.
Fourteen server containers consume the same image; twelve namespace owners
retain fixed endpoints while those storage processes exit. At the default
CPU sharing this profile reserves 1.5 CPU / 7872 MiB, including its host
client. The three outage cases preserve old/new exact bytes; node two also
checks persisted degraded EC placement, full restart and the 25-second repair
bound. A separate protected restart case checks stable endpoints and range
reads. Local state/secret/owned-directory checks remain component UT, while
Web health and Group-0 preview have their Console owner.

Image and client revisions are recorded independently. Client/documentation/test-registration
commits reuse an image after verifying the release server inputs still match;
production source, dependency, runtime configuration and build changes require
a rebuild. A missing/unverifiable image source label fails this verification.

`--concurrency` is a cap. Each server container defaults to one CPU of relative
weight (`--cpu-shares 1024`), with no CPU quota or cpuset. Idle CPU can be used
by any container; weights govern contention. `--node-cpus` scales this weight,
not a hard ceiling. Node services inherit `TOKIO_WORKER_THREADS=1` to avoid
automatically creating a host-sized pool for every process and exhausting the
existing PID/memory limits. RPC and blocking workers retain their normal
configuration. Named-flow admission reserves a shared 1.5-CPU scheduling
allowance per fixture (servers plus host client), rather than adding node weights.
This bounds concurrent client work, not guaranteed server throughput. Memory
limits and per-profile memory sums remain hard admission bounds. Actual Docker
host capacity further limits the declared budget. Two slots run isolated fixtures
at once; one slot does not claim concurrent isolation.
Recovery/isolation acceptance uses at most two slots; independent named flows
use the effective cap. CI sets `--require-isolation` and
fails if two slots are unavailable, so the required isolation check cannot skip. Host/hardware and containerd profiles are explicit gates.

## Lifecycle and endpoints

- Each fixture owns a UUID-labeled bridge network, independent host data bind
  mounts and its server containers. No global prune or shared
  implicit cluster is used. Cleanup verifies ownership labels and exact names.
- Network-local management is fixed at `7000`; store RPC uses `7001..7011`.
  Every fixture reuses the same names, store/replica IDs and port pool inside its
  own network. New fixtures do not call the legacy offset/probe/flock allocator.
- No service port is host-published. Management and RPC clients use inspected
  node IPs directly, so concurrent fixtures reuse all fixed internal ports.
- Host RPC clients consume the owned endpoint list. Peer topology rewrites
  wildcard listen addresses to inspected bridge IPs (the C++ RPC transport
  expects numeric addresses) while retaining actual
  returned store ports. RPC ports are never host-published.
- KV scopes separately test user-data groups and Group-0 initialization,
  discovery, membership CAS and metadata authority through public APIs. Bare
  profiles start no initial store; Group-0 ownership tests explicitly initialize
  through management. SSH, monitor and console coverage belongs to deployment and Console
  profiles.
- SIGKILL/restart retains the same mounted data, network identities and internal
  endpoints. Inspection rejects a changed peer IP rather than masking stale
  topology by choosing another endpoint.
  Monitor automatic recovery is absent in this layer profile.
- Host fault requests use a bounded fixture-owned control directory. Only
  stop/start/kill/pause/unpause of declared nodes are accepted; ownership is
  rechecked before each action. Responses and individual action timings survive
  teardown. Client phase time already includes these actions.
- Client compilation uses locked Cargo dependencies; the staged executable and
  Pixi shared-library dependencies execute on the host with per-run environment.
  Normal component Rust gates leave the container-only test ignored; this
  runner explicitly executes selected cases and missing fixture setup fails.
- Interruption or partial startup gathers diagnostics and tears down owned
  allocations. Operations have bounded deadlines; SIGINT/SIGTERM abort runner
  coordination and cleanup follows in-flight operations. SIGKILL cannot execute
  cleanup; retained resource labels identify orphaned fixture ownership.
- Node mounts and their logs/data/crash files remain on the host after teardown.
  `mounts.json` records paths. Startup, client phases, collection and teardown
  produce timing reports; a required client invocation must execute one case.
  `endpoints.json` records node/store/group/data layout. `results.json` collects
  per-case client, start/restart/collect/stop and complete wall durations, sorts
  the slowest flows, and preserves failure status after cleanup errors.

Listener implementations follow the
[TCP restart/ownership contract](../doc/design/rpc/design-crowdb-rpc-tcp.md#7-listener-ownership-and-restart):
`SO_REUSEADDR` before bind, stable four-digit endpoints and one live owner;
no default `SO_REUSEPORT` or conflict-driven port hopping.

## Assertions and artifacts

The migrated `e2e_three_node_cluster_kv_put_batch_delete` retains every original
assertion: two remotes per voter, Put success, exact Get bytes, Batch success,
Delete success and empty/not-found after deletion. It additionally checks the
surviving batch value, a per-fixture value under the same key, and a tombstone
across crash/restart. In the concurrent case both clusters first write different
values, then one crashes/restarts and is removed; the other must still read its
own exact bytes and report its three-voter topology.

Artifacts under `.crowdb-runtime/artifacts/container-e2e/run-*` retain image
config ID, labels/source revision, registry digests when available, client
results, topologies and Docker logs/resource state. Diagnostics redact credential
fields. CI consumes the once-built OCI artifact in independent KV jobs in both
regular CI, preview and release workflows. Successful KV digest validation is required by
publication; workflow dispatch/publication remain separate operator actions.

CI exports reports and redacted text diagnostics with
`pixi run python e2e-test/publish.py --source .crowdb-runtime/artifacts/container-e2e --output target/container-e2e-public`.
The output must be a fresh directory outside the original runs. Mounted node
data, generated server configuration, client build trees and private credentials
remain in the original host directories; they are excluded from upload. JSON
measurements retain their original status/count/timing values. Local raw logs
and crash data remain available for investigation.

## Component ownership

- Public server flows have one owner in the domain registry. Retired host
  launchers and overlapping acceptance files are removed.
- Component tests retain internal invariants, planning, protocol peers and
  injected hardware/storage behavior. They do not duplicate the full server flow.
- Current complete-run results and remaining coverage gaps live only in
  `doc/working/test.md`.

The cluster flows also cover exact follower hints, nontrivial topology identities,
dynamic group creation/removal with isolated same-key values, and a five-node
3→5→3 membership transition with acknowledged data and subsequent writes. The
former KV server `cluster_e2e_test.rs` is fully migrated. Selected-case resource
budgets reserve a shared 1.5 CPU allowance and the summed hard memory limits
per fixture; the default KV budget permits two three-node isolation fixtures.

Snapshot join uses two voting nodes and a third bare store from the same release
image. Direct local reads prove imported old values before remote wiring, then
verify WAL-tail catch-up on the non-voting member. Node profiles can omit initial
groups; the management API owns join/bootstrap.

## Storage and normal monitor batches

- Diskdb client allocation, ownership-routing and exhaustion/reuse now have one
  owner under `tests/diskdb/`; the old client process fixtures are removed after
  acceptance. Six deterministic client UT remain in their component. Diskdb
  application keepalive/fence/journal/scanner proofs retain component ownership
  using pinned in-memory metadata, with no host KV launcher. Public recovery
  independently SIGKILLs Diskdb or Diskdb plus KV and checks exact durable
  records/revisions, persist-only free, compaction and capacity reuse.
- `deployment.selection` consumes mounted normal profiles in the same immutable
  release image: complete seven services, KV-only and KV+Diskdb. Check exact
  selected readiness, acknowledged host-client writes, abrupt node loss and
  recreation with preserved data. Unknown/missing-dependency selections fail
  before partial startup. Single-mode browser/runtime failures now belong to
  the deployment layer; manual UI/SSH admission has its explicit bridge profile.
## Diskio profiles

- `diskio.durability` uses an owned 16 MiB regular data file with buffered file I/O;
  `diskio.direct` uses the release default O_DIRECT setting on the same geometry.
  Both fsync acknowledged aligned and unaligned bytes, SIGKILL/restart at the
  same IP/7200, and verify exact persisted contents plus an unaligned range read.
  Regular files do not claim real block-device or io_uring capability coverage.
- `diskio.io.mem` and `diskio.io.null` own KV plus Diskio, with the original four
  disks including a high-half disk ID. They retain every boundary size, overwrite,
  zone/offset and 100 concurrent cycles, with exact bytes or NullDisk patterns.
- `diskio.discovery` owns heartbeat, stable connection pools across refreshes,
  malformed-owner rejection and new-disk reconciliation through Group 0.
- `diskio.errors` owns missing disk/zone and missing write creation time;
  `diskio.faults` owns deterministic I/O errors using the normal release option.
- `diskio.semantic` owns payload/view framing, ordering, independent strip and
  priority admission, endpoint replacement and error classification. Its KV and
  three sequential Diskio generations use four owned containers. Replacement
  containers are created dormant and started by the bounded fixture control
  only after the prior generation exits. Startup waits for its RPC listener;
  tests then verify disk and registry readiness without hiding transport errors.
- Address validation, authoritative-owner topology and synthetic native-route
  lease recovery remain component tests. The shared host Diskio launcher remains
  until chunk client/stream callers migrate, and is not used by this domain.

## Access S3

- `s3.protocol` owns 23 existing boto3, AWS CLI and rclone contracts. Each case
  uses two Access frontends, one protected Chunk-KV server, six persistent
  Diskio failure domains, Chunkdb, Diskdb and three KV voters. HTTP/health
  ports are 7500/7501; every fixture owns its bridge and bind-mounted data.
- `s3.recovery` separately restarts each service class and checks exact bytes,
  overwrites, deletions, copies and unfinished multipart sessions.
- `s3.responses` tests lost committed responses. `s3.credentials` runs the
  exact release credential CLI on the host. `s3.benchmark` records functional
  direct Chunk and HTTP request-path samples, including Access CPU/RSS.
- `s3.sdk` owns the [Java, JavaScript and Go client projects](clients/S3-SDK.md),
  their original locks and injected-failure cleanup checks.
- Native buffer, payload-copy and read metrics remain asserted. Parsers,
  signatures, repository state machines, publication races and fake transport
  wire proofs remain component tests. These wire stubs do not start a CROWDB
  release server or replace production storage acceptance.

```sh
pixi run test-container-e2e --layer s3 --concurrency 2 --cpus 16 --memory-mib 16384
pixi run test-container-e2e --layer s3 --scope s3.recovery --concurrency 1 --cpus 8 --memory-mib 8192
```

## Console browser flows

`console.native.binding` uses three admitted release nodes and real DiskDB
services to validate binding selection, owner acquisition and explicit repair
of missing authoritative records. Its deployment selects a normal one-second
heartbeat and retains the three-second acquisition assertion.

The other native scopes reuse three formal nodes with all 18 typed services:

- `console.native.s3`: multipart bytes, actual Chunk acknowledgement, physical
  locations and stale continuation rejection.
- `console.native.recovery`: every typed service restart, the live authority
  leader, old/new container PIDs, exact inventory and durable object bytes.
- `console.native.browser.s3`: host-browser multipart upload, HEAD, preview,
  physical placement/navigation and deletion.
- `console.native.load`: production 32-MiB Chunk-KV load, heartbeat/lease bounds,
  retained-byte scale and exact data after owner restart.

Disk capacities are declared per fixture and recorded with endpoints; mounted
sparse files and public deployment use the same values. No image rebuild or
server test mode is needed for a capacity profile.

```sh
pixi run test-container-e2e --scope console.native.binding --cpus 4 --memory-mib 8192 --node-cpus 1
pixi run test-container-e2e --scope console.native --cpus 8 --memory-mib 16384 --node-cpus 1 --concurrency 2
```

- `console.browser.shell` runs shell navigation and embedding checks with the
  pinned host browser against an ordinary release Web container. Each case has
  its own network, data mount and browser workspace; server ports stay fixed.
  The Web/KV node reserves at least 2 CPU plus the host browser (2.5 CPU /
  2560 MiB total). A 4 CPU budget runs one case; 6 CPU allows two.
  The proxy page must be ready before the embedding navigation is checked.
- `console.browser.registry` covers the empty shell, datacenter rack menu,
  ownership-probe scope, persisted Access-only listener plan and fixed-slot
  warning, plus duplicate rack/node rejection. It uses public HTTP setup and fixed Access ports 9000/9001/9002/9003;
  dependencies remain unprovisioned and the Access service stays waiting.
- `console.browser.kv` covers real KV pagination, binary representation and
  automatic scans. `console.browser.canvas` covers fit, pan, collapsed children
  and navigation state. These owned Web nodes deploy one ordinary KV server
  at fixed 7000/7001 using durable defaults.
- `console.browser.logical` covers Store/Group/Replica creation, duplicate Group
  rejection and deletion. `console.browser.node` covers physical/logical jumps
  and remote-proxy removal without deleting the logical member. Each owns three
  manual release containers, admitted through SSH and bootstrapped through the
  public API. At least 1 CPU / 2048 MiB per node plus its host browser reserves
  3.5 CPU / 6656 MiB per case. KV uses fixed 10000/10100 node ports.
- Host shell/registry/KV/canvas clients run in batches bounded by the same case slots.
  All batch containers start before clients run concurrently; all clients finish
  before any batch network is removed. Other selected server cases finish first.
  This prevents Docker bridge churn during browser mutations without retries
  or relaxed UI deadlines. Failed clients and partial startup still clean up.
- Backend-unavailable checks use a closed container listener at port 9091 so
  Chromium does not need to authorize a private-IP page accessing loopback.
- `console.managed` runs both full-stack managed browser flows. Its mounted Web
  template sets readonly false while the public release template remains true.
  Both use the same image; physical capabilities remain restricted. The host
  control channel pauses/resumes only the current owned KV service PID.
- These accepted cases are included once in `console.operations`; managed
  browser and native UI owners are included in the complete Console domain.

```sh
pixi run test-container-e2e --scope console.browser.shell --concurrency 2
pixi run test-container-e2e --scope console.browser.registry --concurrency 2
pixi run test-container-e2e --scope console.browser --concurrency 2 --cpus 8 --memory-mib 16384 --node-cpus 1
```

## Dataset browser workspace

`console.native.browser.dataset` owns five isolated release-backed browser flows:
`browse`, `import`, `split_revisions`, `recovery`, and `capabilities`, under
`console::browser_dataset`. The `80/81/82-dataset-*` specs cover the combined
Sample/Schema matrix, Snapshot ancestry, multipart local import, independent Split
revisions, frozen inheritance, publication-response loss, and readonly/prefix modes.
Each case uses real Access/storage services. Recovery may sever a real reverse-proxy
connection after the upstream commits; it never substitutes a service response.

## Console WAL recovery

- `console.operations` includes the public Web WAL recovery flows. Single-node
  recovery uses the normal standalone Web and its packaged KV child;
  `console.web.wal` uses 3/5/6 separate KV containers and one writable Web.
- Rack/node, Group-0 and store/group creation use the public Web APIs. Clients
  inspect Accepted records in the mounted WAL and require unchanged max slots
  after stopping and restarting every voter, then verify exact values/deletions.
- Multi-node profiles select the supported file-WAL backend from the same
  release image; standalone recovery retains the normal child defaults.
  KV storage remains durable. Internal ports are fixed; batch lifecycle
  validates all voter labels before mutation and leaves Web running.

## Deployment flows

- The canonical deployment layer has 33 named owners. It covers service
  selection, hardware/KV/logical bootstrap, interruption and persistent recovery,
  fourteen supervised child crash/hang cases, invalid inputs, official host SDKs,
  protocol layouts, runtime permissions, readonly release browser behavior,
  terminal restart exhaustion, durable identity rejection, lifecycle log/secret
  checks, anonymous-volume policy, Monitor death, occupied protocol listeners
  and disabled single-node split planning.
- Complete service fixtures reserve 2 CPUs/2048 MiB for the node plus a host
  client. Occupied-listener checks reserve an additional 0.5 CPU/512 MiB release
  process sharing only the owned node's network namespace; the other protocol
  and health listener remain unoccupied. Independent scenarios keep private
  networks, credentials and mounts.
- `container/single-node-container/tests/container-e2e.sh` delegates to this
  layer. It owns no server launch, client, published port or assertion.
  Build/import the formal artifact before invoking the canonical runner.
- Browser clients use the pinned `clients/console-ui` package and the installed
  system browser. Each fixture prepares its own host workspace. Signals pass
  through label/PID/executable guards. Default anonymous-volume policy is an
  explicit exception to data bind mounts; logs/crash files are copied to host
  artifacts before its owned volume is removed.
- Domain wall times are recorded in [test.md](../doc/working/test.md); individual
  client/fixture times remain in each run's `results.json`.
  Manual admission, native host and ecosystem
  profiles and final Pixi/CI classification remain in the working plan.

`bridge-admission` runs manual release nodes on an owned bridge, using inspected
IPs and fixed Web/SSH listeners without host port publishing. Its five-node peak
reserves 3.5 CPU / 6144 MiB at 0.5 CPU/node, including the host browser. Four
browser reports must each pass exactly once with zero skips. Browser traces,
node data/log/crash files and credentials stay under the private per-run data
root; only redacted client output and identity/results are uploaded. Both
partial startup and active-browser interruption remove all owned runtime
resources and terminate the host browser children while retaining diagnostics.

## Manual external engines

- `pixi run -e iceberg-ecosystem test-iceberg-container-ecosystem` delegates to
  root `--profile ecosystem`; `CROWDB_ECOSYSTEM_PROFILE=python` selects the focused
  Python/DuckDB profile. Neither belongs to default CI or ordinary Iceberg counts.
- [Pinned matrix and prerequisites](clients/ecosystem/README.md) describe the
  host SDK/JVM clients, optional Trino engine, fixed bridge endpoints, retained
  private data and public redacted reports. Both profiles recreate the formal
  server against its original mounted data and assert unchanged credentials.

## Console browser ownership

Browser flows live under `clients/console-ui/e2e/flows/`. The root runner owns
normal release Web nodes and the actual service cluster; clients never launch
host servers or allocate host ports. `test-console` and `test-console-ui` retain
backend and UI component tests without Docker. The npm `test:e2e` entry delegates
to the same required container domain:

```sh
pixi run test-container-e2e --scope console.operations --concurrency 2 --node-cpus 1 --cpus 4 --memory-mib 16384
```

Native full-service fixtures reserve 512 PIDs/threads per node and 2048 MiB,
with a private network and fixed service ports. The six-node owner combines
three storage and three diskless nodes; it reserves 6.5 CPU / 12800 MiB.
Its normal DiskDB deployment requests a 3-second keepalive cadence. CLI replay
also requests `--diskdb-heartbeat-interval-secs 3` through normal cluster creation;
the setting persists across restart. CLI defaults retain the production 10-second
cadence. New disk
initialization and actual SDK data checks remain bounded, and all selected
services must deploy without user retry.
