<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: OCI Image Build and Runtime

Depends on: [deployment architecture](design-crowdb-deploy.md).
Satisfies: Docker-free, Pixi-managed Linux image construction and shared images
for Docker, containerd and future Kubernetes deployment.

Linux amd64 construction uses Pixi-managed standalone BuildKit. Docker and
containerd consume the shared image; explicit probes and resource verification
cover both runtimes. Operational commands and tested versions are in the
[shared image guide](../../../container/oci-image/README.md).
macOS implementation and Kubernetes deployment automation are deferred.

## Table of Contents

- [1. Scope and invariants](#1-scope-and-invariants)
- [2. Image and base system](#2-image-and-base-system)
- [3. Pixi-managed construction](#3-pixi-managed-construction)
- [4. Artifacts and publication](#4-artifacts-and-publication)
- [5. Docker deployment](#5-docker-deployment)
- [6. Lightweight Linux deployment](#6-lightweight-linux-deployment)
- [7. macOS and Kubernetes extension points](#7-macos-and-kubernetes-extension-points)
- [8. Validation and supported boundaries](#8-validation-and-supported-boundaries)
- [9. External references](#9-external-references)

## 1. Scope and invariants

- **I1 — Docker-free build**: a Linux amd64 host with Pixi and documented kernel
  prerequisites can compile, stage, construct and validate an OCI image without
  Docker CLI, Docker daemon, Docker socket or containerd daemon.
- **I2 — One image**: for a given architecture and release, Docker and containerd
  consume the same published image digest. Single-node initialization and manual
  multi-node admission are startup policies, not separate image builds.
- **I3 — External state**: UUIDs, SSH keys, cluster bindings, server data and logs
  remain outside image layers. Replacing a runtime retains persistent state.
- **I4 — Enforced resources**: production startup fails if requested CPU/memory
  limits or host networking cannot be applied and verified.
- **I5 — Verified publication**: publication copies the verified image without
  rebuilding its root filesystem or converting its manifest media types.
- **I6 — Bounded lifecycle**: build processes have isolated sockets, owned state
  and explicit cleanup. Cleanup never terminates unrelated runtime services or
  deletes node data or shared image caches.

Initial build/output platform is Linux amd64. Running these images on another
architecture requires an explicitly supported emulator or a separately verified
architecture build; an OCI index alone does not provide architecture support.

## 2. Image and base system

Ubuntu 24.04 remains the runtime base, pinned by digest. OCI specifies image
layout, configuration and layers; it does not require a particular distribution.
Ubuntu supplies the existing SSH, certificate, shell and system-library runtime.
Keep the current runtime dependency checks and UID/GID 10001 data ownership.

The image contains the same monitor, UI, Rust/C++ servers, shared libraries,
templates and entrypoint used by both deployment modes. The common
image defaults to `CROWDB_STARTUP_MODE=manual`: discovery, SSH and UI start, then
the node waits for preparation and Group-0 bootstrap. Manual mode supports one
or multiple nodes. An operator can select the local candidate, admit it into the
prepared cluster and initialize a one-member Group 0; this is an ordinary
editable cluster and can later admit more nodes. It still requires configured
storage and passes the same preparation/bootstrap validation.

Explicit `CROWDB_STARTUP_MODE=single` selects the existing convenience policy:
automatic virtual-disk creation and cluster initialization, followed by disabled
UI editing and backend mutation rejection. Node count alone never activates this
policy. Initialization always happens at startup or through management actions,
never during image construction. No additional single-node image is needed for
manual one-node bootstrap. The production policy remains
`CROWDB_DEPLOYMENT_MODE=production` with explicit credentials and resources.

BuildKit's Dockerfile frontend remains usable, including existing bind mounts in
`RUN` instructions. The recipe name does not imply a Docker daemon dependency.
The final image uses OCI manifest, configuration and layer media types and
standard image annotations. Digest verification checks the actual exported
objects, not only `org.opencontainers` labels.

Docker-specific `HEALTHCHECK` is not a portable OCI configuration field. Runtime
health checks invoke `crowdb-monitor liveness` and `crowdb-monitor readiness`
explicitly. Container restart policy and application readiness are distinct:
unready Group 0 must not trigger destructive reinitialization or restart loops.

## 3. Pixi-managed construction

Standalone BuildKit uses its OCI worker with `runc`; containerd is unnecessary
for construction. Pixi manages locked versions of `buildctl`, `buildkitd`,
`runc`, and the image inspection/copy tool, Skopeo. Rootless build
also needs RootlessKit and any required snapshotter helpers. Existing Rust,
CMake, Node and packaging tools continue to use the repository's locked Pixi
environment.

A separate Linux-only Pixi workspace isolates the tools from macOS and ordinary
application development. Conda-forge supplies runc and Skopeo. Checked-in Pixi
source-package recipes supply BuildKit, RootlessKit, containerd, nerdctl, Syft
and Cosign using pinned upstream release checksums. Packages build locally;
an external project channel is unnecessary. No implicit apt/Homebrew or
unverified download bootstraps tools.

The public build operation has three stages:

- **Compile/stage**: on Linux, compile release artifacts with the current FFI
  feature contract and assemble a source-free runtime directory. This stage
  retains Cargo/CMake/npm caches and does not inspect any container daemon.
- **Construct**: start a task-owned BuildKit instance, wait for its private Unix
  socket, run the Dockerfile frontend and explicitly export OCI output. Keep
  cache/state separate from node data. Propagate errors and clean up the owned
  daemon on success, failure and interruption.
- **Validate**: inspect layout, media types, blob hashes, platform, revision and
  version; reject incomplete output. Record the image manifest/index digest
  separately from archive checksum. Successful output is atomically published
  to the requested local artifact path.

Rootless construction is preferred, subject to user namespaces and snapshotter
support. Preflight reports required kernel/host setup. No automatic privileged
fallback is allowed: a documented administrator-enabled build mode can be
selected explicitly if rootless support is unavailable. Pixi supplies userspace
tools, not kernel capabilities or privilege delegation.

The common build component lives under `container/oci-image/`. The existing
single-node component delegates staging and image construction to it while
retaining its startup conveniences and acceptance tests. A Docker load/start
helper is separate from the Docker-free image build. The same split supports a
future Linux-VM build backend without putting host-platform branches in the
image recipe.

## 4. Artifacts and publication

The canonical local artifact is an OCI image layout/archive with a build receipt:
source revision, project version, platform, base digest, tool versions, runtime
content checksum, OCI root digest and archive checksum. Runtime content and
final image are independently identifiable. Timestamps/package repositories may
affect reproducibility; this design does not promise byte-identical rebuilds.

Reuse the existing release workflow and release-branch/version policy. It gains
separate construct, runtime verification and publish jobs:

- Construct through Pixi without Docker access or registry publication secrets.
  Upload the OCI artifact and receipt; do not rebuild from staged files later.
- Verify the same artifact on system Docker and on containerd/nerdctl. At least
  one CI environment has no Docker installation; otherwise accidental dependency
  on Docker cannot be excluded. Each job emits the verified OCI digest.
- Publish only after both runtime gates pass, with the existing protected
  publication environment and current-release-head check. Copy the exact OCI
  graph using digest-preserving transfer; failure to preserve it is an error.
  Verify the registry digest before signing or updating moving tags.

The canonical Docker Hub repository is `crowdb/crowdb-node`. The Iceberg, S3 and Dataset
repositories provide single-node usage names for the same image digest. Their
startup instructions explicitly select `CROWDB_STARTUP_MODE=single`; repository
names do not alter the default manual mode. The Dataset name reserves its usage
entry point while Dataset access remains unavailable. Each
repository receives version and `latest` tags; there is no runtime-specific or
service-specific image build.
Changing the published default from automatic single to manual is a usage
change: existing automatic examples/helpers must explicitly select `single`.
Do not silently change the startup policy of an existing populated data root.

Keep signing, SBOM and provenance. Generate provenance from the source/tool/base
receipt and attach attestations to the verified digest without rebuilding the
image. Distinguish the runnable image digest from any attestation-bearing index
digest, and verify both where an index is published. Failures leave the verified
artifact available for retry; credentials never enter layers, logs or receipts.

## 5. Docker deployment

Use the host's Docker installation. Pull the OCI image from the registry by
digest, then start it with the existing entrypoint/environment/mount contract.
Production uses `--network host`, explicit `--cpus` and `--memory`, a persistent
data directory, physical host identity, management interface and credential
mounts. Bridge-mode single-host simulation remains available for tests.
Ordinary one-node usage needs only the usual ports/persistent data mount and UI
bootstrap. Automatic development usage adds `-e CROWDB_STARTUP_MODE=single`;
production resource/device arguments are not prerequisites for the simple
development example. Both use the same image.

Local OCI archive loading is version-dependent and must be tested separately.
The baseline production contract is registry pull by digest. If a supported
Docker version needs a Docker archive for offline loading, an explicit import
helper may convert the already built artifact for local use; it must not rebuild
or create a second published release. Converted local image IDs are not assumed
to equal the canonical OCI manifest digest.

## 6. Lightweight Linux deployment

The selected alternative is **containerd + nerdctl + runc**. Containerd supplies
image storage and container lifecycle, nerdctl supplies the operator CLI, and
runc executes the container. This excludes the Docker daemon but still requires
a running containerd service. Direct runc bundle management is outside scope.

Initial production support is rootful Linux with cgroup v2 CPU/memory
controllers. `--network host` shares the host network namespace; no bridge,
NAT, CNI setup or published-port mapping is needed for this profile. Each host
normally runs one node because fixed service ports conflict on a shared host
network. Multi-node tests on one host keep their separate bridge profile.

Pixi supplies pinned runtime binaries through a separate runtime environment;
an administrator installs/starts the host containerd service with stable binary
paths and explicit configuration. Updating a Pixi lock or cleaning a development
environment must not silently change a running production daemon. System
containerd can also be used if its version/configuration passes preflight.
Use a dedicated CROWDB namespace and never alter a Kubernetes-owned service's
configuration or containers automatically.

A shared launcher validates digest, platform, data ownership, credentials,
physical host identity, management interface and allowed devices before choosing
Docker or nerdctl. It verifies applied cgroup `cpu.max` and `memory.max`, actual
host network namespace and device access after startup. Requested limits cannot
silently degrade into advisory values. It records runtime/container identity
for explicit stop, restart and cleanup. Host limits are upper bounds, not
guaranteed CPU reservations. Block-device access stays explicit; no blanket
privileged container is needed.

Explicit monitor liveness/readiness probes report node health. A dedicated
systemd unit restarts the containerd daemon, and its restart plugin applies the
node container’s `unless-stopped` policy. Monitor supervises child services.
Readiness failure alone does not restart or reinitialize the node. Persisted UUID,
SSH keys and cluster binding survive recreation under either runtime.
Rootless production networking/device/resource support is deferred; rootless
build support does not imply rootless production deployment support.

## 7. macOS and Kubernetes extension points

macOS retains a build backend slot: the Pixi-managed client starts/connects to a
Linux VM builder (with Lima as the provisional VM provider). Compilation, Linux library collection and
image construction all execute inside that Linux environment. Native macOS
binaries cannot be staged into the Linux image. VM CPU architecture, repository
mounts, cache, artifact transfer and lifecycle require later design/acceptance;
this change does not add macOS implementation or multi-architecture promises.

Kubernetes uses the same OCI image digest through its CRI runtime. Later manifests
or an operator supply persistent mounts, host networking where appropriate,
resource limits, credentials and monitor probes. Discovery and physical-host
identity require explicit scheduling/network policy. Kubernetes deployment
support is not established merely by OCI image publication.

## 8. Validation and supported boundaries

- Verify Docker-free full build on a clean Linux host, with the builder packages
  resolved entirely through Pixi and no system container binaries required.
- Verify the same OCI digest under Docker and containerd: both startup modes,
  persistent identity/data, health checks, SIGTERM and recreated containers.
- Verify host networking discovery/admission, actual resource enforcement and
  rejection of unsupported hosts; preserve existing SSH and Group-0 contracts.
- Verify release publication preserves tested digests and includes signatures,
  SBOM/provenance; failed gates never publish moving tags.
- Rootful build and isolated Linux containerd host acceptance use the pinned tool
  matrix in the operational guide. Rootless preflight reports missing host UID/GID
  helpers; native rootless execution has not yet been accepted on this host.
- CI publication, remote signing and protected-environment approval are configured;
  live registry acceptance requires a release workflow dispatch.
- Keep Ubuntu 24.04 and Linux amd64 initially. ARM64, macOS VM execution and
  Kubernetes orchestration are separate follow-up work.

## 9. External references

These documents define external formats/tools, not CROWDB membership semantics:

- [OCI Image Specification](https://specs.opencontainers.org/image-spec/).
- [BuildKit: standalone workers, OCI output and macOS client](https://github.com/moby/buildkit).
- [BuildKit rootless prerequisites](https://github.com/moby/buildkit/blob/master/docs/rootless.md).
- [nerdctl CLI and resource/network options](https://github.com/containerd/nerdctl/blob/main/docs/command-reference.md).
- [nerdctl rootless limitations](https://github.com/containerd/nerdctl/blob/main/docs/rootless.md).
- [Skopeo image inspection and transfer](https://github.com/podman-container-tools/skopeo).
