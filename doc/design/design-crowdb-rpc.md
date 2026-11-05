<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Storage RPC

`crowdb-rpc` puts RDMA at the center of CROWDB's high-performance internal data path, with TCP as the fallback for environments without a usable RDMA connection. The transport connects storage services and native consumers through registered buffers and asynchronous completion, carrying memory ownership across the boundaries where ordinary message handling often adds staging and copies.

A C++ engine supplies connections, framing, workers, and both transport backends; `crowdb-rpc-ffi` exposes them to Rust. Services use the same request and payload model on either path. Deployment policy enables RDMA and controls whether setup may fall back to TCP.

## RDMA carries the storage data path

The RDMA backend manages registered memory, posted receives, work requests, and completion queues. Payload views retain their allocations until the hardware and subsequent consumers have finished with them. Eligible buffers can keep their registration through DiskIO, Chunk reads, and native SDK delivery, reducing repeated allocation, registration, and intermediate copying.

The value appears across the complete I/O path. A DiskIO read can fill a registered buffer for transport; a native consumer can retain the received allocation while processing its bytes. A direct native client also removes the Access Server payload relay. CROWDB can therefore optimize both the network transfer and the handoffs on either side of it. The [DiskIO design](design-crowdb-diskio.md#a-short-path-from-request-to-device) and [native SDK](design-crowdb-sdk-native.md) describe those paths.

## Control and payload take different paths

A control message describes the operation. Payload views describe the bytes it transfers and retain their owners. The transport can keep those allocations alive until asynchronous send or receive work completes, without forcing every caller to serialize the payload into a new message buffer first.

Request correlation and transport errors belong to RPC. The service handler defines business success, retry behavior, and durability. A successful send cannot establish that an object was published or a KV mutation committed.

This division lets DiskIO, metadata services, and native consumers share the transport while keeping their completion rules explicit.

## Backpressure follows the resources still in use

Storage resource use continues beyond network transfer. A payload may be queued for DiskIO, retained during a write, involved in EC work, or held by the application after delivery. The RPC engine bounds registered memory and transport work; the Chunk pipeline and native runtime apply their own admission limits to the work and allocations they own.

Consider a burst of writes that arrives faster than the disks can complete it. Receiving another payload may be easy for the network while its memory remains occupied behind earlier device operations. Transport flow control sees network progress; storage admission must also see queued and in-flight storage work. Bounding both requests and bytes matters because a small control operation and a large payload consume very different amounts of memory.

When downstream capacity is occupied, those limits stop or delay further production. A slow disk or consumer therefore becomes visible as backpressure, rather than an invitation to accumulate more payloads. Separate budgets account for request concurrency, bytes, and processing resources; a count of completed RPCs alone cannot describe the system's available capacity.

The receive pool, send queue, storage pipeline, and consumer each release capacity when their part of the work actually finishes. They can share a payload allocation without pretending that these are the same event. That is especially valuable for RDMA, where registered memory is a finite resource: reusing an allocation saves staging work only if its lifetime remains visible to every stage that retains it.

Ownership also survives cancellation. If a write times out after device submission, the caller can stop waiting while the device still references the buffer. The retained owner keeps that allocation valid until the outstanding work finishes. On reads, application-held results remain charged after transport completion. This connects RDMA efficiency with controlled memory use under sustained load. The [native SDK](design-crowdb-sdk-native.md#completed-reads-still-consume-capacity) carries that accounting through to the consumer.

## TCP provides a practical fallback

TCP keeps the same storage services usable on ordinary hosts and networks. Its backend uses event-driven workers with epoll or kqueue, handling framing, send and receive progress, and connection shutdown. A deployment can select TCP explicitly; an RDMA-enabled client can use it when capability negotiation or connection setup cannot establish a supported RDMA path and fallback is allowed.

Performance-sensitive deployments can disable fallback to require RDMA. This makes the chosen transport an explicit operational policy: a workload that requires RDMA fails setup visibly instead of silently running on another path.

Fallback happens before payload submission. Once an RDMA operation has an uncertain outcome, the transport reports that failure; it does not splice TCP bytes onto a partial result or transparently replay the operation. The service's request semantics determine whether a subsequent retry is safe.

## Why own an RPC engine?

A storage RPC engine can make registered memory and asynchronous I/O part of its basic interface. Control fields remain small, payload buffers keep their owners, and completion releases resources at the right stage. RDMA receives the design attention required by the fast path, while TCP preserves deployment flexibility through the same service boundary.

Owning that boundary lets the Chunk client, DiskIO, and native SDK agree on memory lifetime. It removes avoidable staging at supported handoffs and lets resource admission span several libraries. The transport becomes part of the storage pipeline, with an interface shaped around moving bytes efficiently between devices and consumers.

Internal RPC operates within the cluster's trust boundary; externally authenticated applications use the access interfaces. Public protocol compatibility remains with those interfaces.

See the [transport interface](../../lib/crowdb-rpc/include/crowdb-rpc/transport.h), [buffer interface](../../lib/crowdb-rpc/include/crowdb-rpc/buffer.h), and [FFI](../../lib/crowdb-rpc/ffi). The [native SDK](design-crowdb-sdk-native.md) shows how retained transport allocations become application-visible results.
