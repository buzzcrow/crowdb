<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Native SDK

`crowdb-sdk-native` is CROWDB's RDMA-oriented access path for high-performance applications. When compute and storage belong to the same trusted system, an application can embed the Dataset and S3 access libraries and communicate directly with storage services. Bulk data travels between disk-serving nodes and the application without passing through an additional Access Gateway.

The objective is efficiency across the whole system: fewer network hops, less protocol work, fewer staging buffers, and less CPU spent forwarding bytes. RDMA is most valuable when the architecture also removes the intermediate process that would otherwise receive, wrap, and retransmit the payload.

## Make storage part of the application runtime

An HTTP access path sends storage-node payloads through Access Server before they reach the application. The native runtime executes the access-library work inside the application process and uses the Chunk client and DiskIO connections directly. This removes the storage-node-to-gateway-to-application relay from bulk I/O.

The same Dataset Snapshot, Split, and field-selection rules still apply. S3 operations still resolve bucket and key and use the owning library's publication rules. Metadata services retain their authority; the application gains a shorter data path rather than a second interpretation of stored data.

For training and analytical systems, this changes where resources are spent. Gateway CPU, response framing, forwarding buffers, and a second payload transfer no longer have to scale with every byte consumed by an internal application. Compute and storage can be provisioned around their actual work.

## One runtime for many operations

`ClientRuntime` owns or borrows the underlying service clients. Dataset and S3 handles share its connection groups and resource budgets. Topology observations help locate services and data; current routing and serving authority still determine whether a request may execute.

Sharing matters under concurrency. Creating independent connection pools and memory limits for every object or worker handle would multiply both resources and coordination. A common runtime gives the application one place to account for active requests, producer buffers, and held results.

Operations carry deadlines and cancellation. Shutdown closes admission and drains owned work. Buffers already handed to an application retain their own lifetime, so closing a stream or runtime does not invalidate data that the caller still holds.

## RDMA needs buffer ownership all the way to the consumer

Reads return `BufferLease` values: immutable views that retain the backing allocation. A consumer can process a view, pass it to another asynchronous operation, or retain it until a host-to-device copy finishes. Resource accounting follows the allocation until its final owner releases it.

Eligible registered receive buffers reach the caller without a second SDK payload copy. Producers can also fill native allocations and submit immutable views. Buffer ownership survives asynchronous I/O and application processing, so a fast transport does not immediately lose its benefit to another copy at the SDK boundary.

Copies still have reasons to exist: ordinary input may need framing, erasure coding creates or reconstructs fragments, and language adapters may materialize their own values. For example, the native Python binding's `to_bytes()` explicitly creates Python-owned bytes. The benefit comes from making those conversions deliberate and keeping ownership intact between them.

## Completed reads still consume capacity

A training worker can finish a read and keep its buffers while decoding samples or waiting for a host-to-device transfer. The SDK continues to charge those allocations against the caller-held capacity budget until their final owner releases them. Completing the RPC or closing the stream does not erase that memory use.

For example, a worker may prefetch the next batch while another task decodes the current one. Both batches occupy capacity until their owners finish with them. Passing a shared view to another task preserves the same backing allocation and charge; it does not create another copy merely to transfer ownership. Once the last owner releases that allocation, the capacity becomes available to a later read.

When held capacity is exhausted, later reads wait for release or return a deadline or cancellation result. Producer allocations and active requests have their own limits, alongside the transport's registered-memory and queue budgets. Sharing a runtime makes Dataset and S3 handles share the appropriate SDK accounts, so adding handles does not silently multiply the application's allowance.

Request concurrency controls how much work is active; held capacity controls how much delivered data remains in the application. A workload can have few active requests and still hold substantial memory from earlier reads, so those limits serve different purposes. Producer memory has a similar lifetime concern: shared small-write machinery can retain submitted owners for open-strip repair after an individual PUT completes.

This lets the consumer set an SDK-managed working-set limit that remains meaningful after bytes leave the network. The application can retain a view through asynchronous processing while backpressure controls how much more data arrives. Copies into application-owned arrays, decoded objects, or Python `bytes` need the application's own memory policy; the SDK accounts for the allocations whose lifetime it manages.

## Transport policy follows the deployment

The runtime selects transport policy for metadata and data connections. RDMA uses configured devices, registered memory, and bounded transport resources. TCP supplies the portable path for deployments without that hardware. Applications keep the same access semantics and retained-buffer model in either case.

An operation that has already transferred data cannot be treated as a fresh attempt merely because another transport is available. The SDK preserves errors and uncertain mutation outcomes so the application can recover with the original identity and publication conditions. It does not turn an ambiguous write into an automatic new write.

## Extend the direct path toward GPU memory

GPU workloads make the final destination of each byte especially important. The next integration step is direct delivery into GPU memory through suitable RDMA and GDS / GPUDirect support. The system-level goal is to remove gateway forwarding and host staging from the storage-to-GPU path, rather than accelerating one link while leaving the same copies around it.

CROWDB has taken the first step with direct RDMA access and retained host buffers. The current SDK exposes host memory, including buffers that an application can retain until an asynchronous host-to-device copy completes. GPU pointers and direct GPU delivery require the subsequent GPU integration. The existing ownership and completion model provides the place to carry that lifetime through the full path.

## The deployment boundary is explicit

The embedding application needs access to internal management and storage services. It has the same internal trust as an access service; this API does not perform S3 SigV4 authentication. HTTP remains the appropriate boundary for portable clients and externally authenticated access.

Native integration brings native libraries and, for RDMA, host device and registration requirements. That is a useful fit for an internal compute/storage cluster whose operators control both ends of the connection. HTTP remains available for portable clients and external access.

The [native SDK README](../../lib/crowdb-sdk-native/README.md) contains Rust examples and runtime options. The [native Python README](../../lib/crowdb-sdk-native-python/README.md) describes packaging and its explicit copy boundary. The separate [Dataset HTTP SDK](../../sdk/README.md) provides the portable path.
