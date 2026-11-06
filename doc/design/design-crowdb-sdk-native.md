<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Native SDK

`crowdb-sdk-native` lets trusted applications embed the Dataset and S3 access libraries and communicate directly with storage services. Bulk data travels between DiskIO nodes and the application over RDMA or TCP, without an Access Server relay. The SDK exposes retained host buffers; direct GPU-memory delivery is future work.

The objective is efficiency across the whole system: fewer network hops, less protocol work, fewer staging buffers, and less CPU spent forwarding bytes. RDMA is most valuable when the architecture also removes the intermediate process that would otherwise receive, wrap, and retransmit the payload.

## Make storage part of the application runtime

An HTTP access path sends storage-node payloads through Access Server before they reach the application. The native runtime executes the access-library work inside the application process and uses the Chunk client and DiskIO connections directly. This removes the storage-node-to-gateway-to-application relay from bulk I/O.

The same Dataset Snapshot, Split, and field-selection rules still apply. S3 operations still resolve bucket and key and use the owning library's publication rules. Metadata services retain their authority; the application gains a shorter data path rather than a second interpretation of stored data.

The native SDK currently covers Dataset and S3; Iceberg's native upload interface is a separate integration described in the [Iceberg design](design-crowdb-access-iceberg.md).

## One runtime for many operations

`ClientRuntime` owns or borrows the underlying service clients. Dataset and S3 handles share its connection groups and resource budgets. Topology observations help locate services and data; current routing and serving authority still determine whether a request may execute.

Sharing matters under concurrency. Creating independent connection pools and memory limits for every object or worker handle would multiply both resources and coordination. A common runtime gives the application one place to account for active requests, producer buffers, and held results.

Operations carry deadlines and cancellation. Shutdown closes admission and drains owned work. Buffers already handed to an application retain their own lifetime, so closing a stream or runtime does not invalidate data that the caller still holds.

## RDMA needs buffer ownership all the way to the consumer

Read streams return `BufferLease` values: immutable views that retain the backing allocation. A consumer can process a view, pass it to another asynchronous operation, or retain it until a host-to-device copy finishes. Resource accounting follows the allocation until its final owner releases it. A lease contains a piece of the result; only the stream's verified end establishes complete-read success.

Eligible registered receive buffers reach the caller without a second SDK payload copy. Producers can also fill native allocations and submit immutable views. Buffer ownership survives asynchronous I/O and application processing, so a fast transport does not immediately lose its benefit to another copy at the SDK boundary.

Copies still have reasons to exist: ordinary input may need framing, erasure coding creates or reconstructs fragments, and language adapters may materialize their own values. For example, the native Python binding's `to_bytes()` explicitly creates Python-owned bytes. The benefit comes from making those conversions deliberate and keeping ownership intact between them.

## Completed reads still consume capacity

A training worker can finish a read and keep its buffers while decoding samples or waiting for a host-to-device transfer. The SDK continues to charge those allocations against the caller-held capacity budget until their final owner releases them. Completing the RPC or closing the stream does not erase that memory use.

For example, prefetching a batch while decoding the previous one keeps both batches charged. Sharing or slicing a view retains the same allocation and charges its capacity once, even if a small slice keeps a much larger buffer alive. Releasing the final owner makes that capacity available to another read.

When held capacity is exhausted, later reads wait for release or return a deadline or cancellation result. Producer allocations and active requests have their own limits, alongside the transport's registered-memory and queue budgets. Sharing a runtime makes Dataset and S3 handles share the appropriate SDK accounts, so adding handles does not silently multiply the application's allowance.

Request concurrency controls how much work is active; held capacity controls how much delivered data remains in the application. A workload can have few active requests and still hold substantial memory from earlier reads, so those limits serve different purposes. Producer memory has a similar lifetime concern: shared small-write machinery can retain submitted owners for open-strip repair after an individual PUT completes.

The held-buffer limit covers SDK-managed allocations after delivery. Copies into application-owned arrays, decoded objects, or Python `bytes` need the application's own memory policy. Retained-buffer accounting is not a limit on the process's total memory.

## Transport policy follows the deployment

The runtime selects transport policy for metadata and data connections. RDMA uses configured devices, registered memory, and bounded transport resources. TCP supplies the portable path for deployments without that hardware. Applications keep the same access semantics and retained-buffer model in either case.

An operation that has already transferred data cannot be treated as a fresh attempt merely because another transport is available. The SDK preserves errors and uncertain mutation outcomes so the application can recover with the original identity and publication conditions. It does not turn an ambiguous write into an automatic new write.

## Extend the direct path toward GPU memory

A future GPU integration could extend the direct storage path into device memory and remove host staging. RDMA transport and retained host buffers provide useful foundations, but direct GPU delivery needs its own registration, ownership, and completion support.

Today, applications must retain the host lease until an asynchronous host-to-device copy finishes. Neither GPU pointers nor GPUDirect Storage are part of the current SDK interface.

## The deployment boundary is explicit

The embedding application needs access to internal management and storage services. It has the same internal trust as an access service; this API does not perform S3 SigV4 authentication. HTTP remains the appropriate boundary for portable clients and externally authenticated access.

Native integration brings native libraries and, for RDMA, host device and registration requirements. It fits an internal compute/storage cluster whose operators control both ends of the connection.

The [native SDK README](../../lib/crowdb-sdk-native/README.md) contains Rust examples and runtime options. The [native Python README](../../lib/crowdb-sdk-native-python/README.md) describes packaging and its explicit copy boundary. The separate [Dataset HTTP SDK](../../sdk/README.md) provides the portable path.
