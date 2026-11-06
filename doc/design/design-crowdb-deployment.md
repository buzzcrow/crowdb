<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Container Deployment

CROWDB packages a storage node as an Open Container Initiative (OCI) image for Linux amd64. The image carries services and runtime dependencies; the host supplies the network, storage devices, and persistent node state. Operators can replace the software image while keeping node identity and data on the host.

The recommended performance-oriented configuration uses host networking and explicitly attached host block devices. The container runtime manages CPU and memory through cgroups. Network and storage paths stay close to the host, while compute and memory consumption remain controlled.

## An OCI image, without a required orchestrator

`crowdb-node` is the deployable artifact. Its build can produce an OCI archive without a running Docker or containerd daemon; loading and launching that artifact are separate operations. An image digest identifies the exact software being deployed, independent of the command used to start it.

Docker and containerd launch paths are available. CROWDB's architecture does not require either runtime or a Kubernetes control plane. An orchestration environment can manage node containers through the same image and host-resource requirements; that portability is an image and runtime contract, rather than a claim that every orchestrator integration is supplied.

This keeps a useful operational boundary. Container tooling distributes software and establishes process resources. CROWDB manages storage topology, replica membership, partition ownership, and recovery through its own services. Moving between deployment environments does not require changing the storage model.

## Host networking keeps the data path direct

Host networking places service listeners on the host's network interfaces. Storage clients reach the node's advertised endpoints without a container bridge, container address translation, or container port forwarding in the payload path. RDMA also requires explicit device access and suitable memory-registration permissions; host networking alone does not enable it. TCP remains available according to transport policy.

One node container per physical host is the natural production layout for the fixed service ports. It also makes the relationship between advertised addresses, physical topology, and the machine serving I/O easy to inspect. The launcher validates the effective host network namespace rather than assuming that the requested setting took effect.

## Host devices keep storage outside the image

Storage devices are attached explicitly to the node container. DiskDB manages allocation on the configured media, and DiskIO reads, writes, and synchronizes the addressed blocks. The container's writable image layer is not part of this storage path.

Direct block-device access avoids adding a container filesystem layer between the storage engine and its media. Device permissions and mappings remain explicit deployment inputs. Disk files can also supply storage for suitable environments, including local testing; the performance-oriented deployment uses host block devices.

Node identity and retained state live in a persistent host directory or volume, separate from the replaceable image. Recreating a container reopens the intended node and its data. Image replacement therefore does not imply reinitializing the disks or inventing a new storage identity.

## CPU and memory are container resources

The runtime applies the node's CPU and memory policy through cgroups. A CPU ceiling caps consumption; relative CPU shares allow the node to use otherwise idle capacity while controlling its weight under contention. A memory limit bounds the container's aggregate footprint. Launch validation checks the effective cgroup settings, so the accepted deployment describes actual limits.

Inside that budget, CROWDB limits worker pools, registered memory, caches, and in-flight I/O. Operators must size those limits together with service overhead to fit the container's budget. Storage admission can delay or reject work under pressure; the cgroup limit bounds the whole container and can terminate processes if memory is exhausted.

## The runtime starts the node; the monitor runs its services

The container runtime owns creation, resource configuration, and container stop. Tini handles PID 1 duties, while `crowdb-monitor` owns the service processes and their shutdown order. Group 0 retains system metadata and the durable decisions that govern those processes.

This separates replacing an executable environment from changing storage authority. A restarted container can recover accepted identity and deployment state; the monitor can restore the appropriate services under the existing cluster model. See [cluster management](design-crowdb-monitor.md) for discovery, readiness, and management operations.

Container deployments have been exercised on physical Linux hosts. Build inputs, runtime options, persistent paths, and launch commands are documented in the [OCI image guide](../../container/oci-image/README.md); the [local node guide](../../container/single-node-container/README.md) covers a smaller setup.
