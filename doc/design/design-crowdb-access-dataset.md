<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Dataset Storage

CROWDB's Dataset storage is built around datasets, samples, and fields. A training application asks for selected samples from a fixed dataset version, with the fields and worker assignment it needs. The storage system carries that selection down to the payload reads, so a batch does not have to begin as a walk through a directory of small files.

Iceberg integration implements an established table format and catalog interface. CROWDB Dataset defines its own sample-oriented storage contract: immutable input, field projection, Split selection, worker partitioning, and resumable reads. It is independent of Iceberg tables and S3 buckets, and does not require a training framework's data types to become storage types.

`crowdb-access-dataset` owns that model and its publication rules. Chunk-KV stores the metadata, while Chunk storage holds larger field payloads. The native RDMA path connects trusted training processes directly to storage nodes, with GPU-memory delivery as the next integration step.

## Small samples do not need small physical files

A file-per-sample layout makes a batch perform repeated path resolution, file metadata access, open/close work, and small payload reads. Packing samples into archives can reduce that overhead, but moves indexing, selection, and update organization into another application layer.

CROWDB gives samples and fields identities in its distributed metadata and aggregates their bytes in shared Chunks. Small fields can remain inline. Eligible uploaded payloads use the shared small-object writer, which batches many objects across a bounded set of pipelines and active chunks. Each field keeps its own location and integrity information while allocation and physical I/O are shared.

That separates the unit an application understands from the unit the disks handle. A sample can stay small and independently selectable without demanding its own file, filesystem inode, or exclusive physical chunk. Dataset reads operate on sample metadata and payload ranges; they avoid routing every sample through a general filesystem namespace.

The benefit extends beyond file counts. Field projection can skip an image when only a label is needed, Split selection can narrow the sample set before payload I/O, and read batches can aggregate the selected work. Small-object efficiency becomes part of the storage design rather than a packaging convention every training job must maintain.

## Immutable input for repeatable work

A Dataset groups immutable Snapshots of samples. Splits define selected sample sets within a Snapshot, with explicit revisions so a running job can retain the selection it chose. Each sample contains named fields; the Snapshot's Manifest records those samples and fields, whose values can be bounded inline data or references to stored payload.

For example, a sample may have an image payload, a label, and preparation metadata. A read can select the label alone without transferring the image. A subsequent training job can use the same Snapshot with a different field projection or Split. The storage model captures identity and selection; decoding an image or constructing a tensor remains application work.

Resource preparation and head publication are separate operations. A client can prepare a complete immutable Snapshot, inspect it, and then conditionally select it as the Dataset head. Another publisher changing the head produces a conflict that the application can resolve deliberately. Preparing a new version does not silently replace the input of a running job.

## From a read plan to selected payloads

![Dataset read binds a Snapshot and Split, selects and projects metadata, reads inline or uploaded fields, and returns a verified batch with a continuation.](../assets/dataset-read.svg)

A read first resolves `latest` or `stable`, if used, to an immutable Snapshot ID and binds any selected Split revision. The read plan fixes sample selection, projected fields, ordering or deterministic shuffle, worker partitioning, and batch policy. Its continuation also records the current position, so another server can replay the same batch.

This avoids tying a long training run to a server-side read session. Workers can restart with a saved continuation, and a slow worker need not keep an idle session alive. Reads remain subject to data retention: an old continuation is a position, not a promise to preserve deleted data forever.

Selection inspects bounded metadata windows in Chunk-KV. Split filtering precedes deterministic shuffle and worker partitioning; field projection identifies the values the application actually requested. The reader does not load a complete Manifest or fetch every field merely to build one batch.

Field locators connect the read path to the data prepared by the write path. An inline field is already in metadata. An uploaded field resolves its immutable completed-upload description and ordered part receipts, then reads the referenced Chunk ranges or streams through the Chunk client and DiskIO. Readers check part lengths and digests, and verify the complete field before reporting success.

Delivery remains bounded through metadata selection, payload windows, and held buffers. HTTP streams a header, payload frames, field endings, and a batch ending; successful completion also requires EOF. The native path uses the same Dataset selection and storage semantics with retained host buffers. After processing the verified batch, the application saves the next continuation. Fetching, EOF, and buffer release do not acknowledge application progress.

## Shared payloads need explicit retention

Dataset versions and compositions can refer to existing payloads. Reuse avoids copying the same content merely to produce another logical view, but it makes deletion a storage concern. CROWDB tracks the owners and retained resources needed to reclaim data safely.

The same principle applies to Split revisions. Changing the current Split does not rewrite field bytes. Removing historical membership metadata and reclaiming payload storage are different operations, with different owners. This lets dataset organization evolve without turning each edit into a bulk data movement job.

## RDMA keeps the training data path direct

In an internal training cluster, the native SDK embeds the Dataset access library in the application. It resolves the selected metadata and reads Chunk payloads directly from DiskIO nodes over the configured RDMA connections. Bytes do not need to travel from a disk node to Access Server and then cross the network again to the training process.

This removes a gateway relay, its HTTP handling, and its forwarding buffers from the bulk-data path. Registered buffers and retained ownership carry the result into the application without an extra SDK copy on eligible paths. The useful optimization is the whole chain: select fewer bytes, aggregate small payload work, transfer directly, and keep the received allocation alive until its consumer finishes.

RDMA is the completed first step toward the GPU path. The current native interface delivers host buffers that can stay valid through an asynchronous host-to-device copy. Direct delivery into GPU memory through RDMA and GDS / GPUDirect integration is the next direction, with the aim of removing host staging as well as gateway forwarding. See the [native SDK design](design-crowdb-sdk-native.md) for that boundary.

## Portable access uses the same Dataset model

The [HTTP SDK](../../sdk/README.md) packages portable Rust and Python clients. Rust owns transfers, integrity checks, retries where appropriate, and read progress; Python exposes synchronous and asynchronous interfaces.

The native Python binding is a separate distribution for trusted deployments with internal storage access. HTTP and native clients share Dataset identities, selection, and publication semantics, so choosing a transport does not require rebuilding the dataset organization.

Training frameworks retain responsibility for tensor decoding, augmentation, and accelerator scheduling. CROWDB supplies the part that benefits from storage-level coordination: repeatable sample selection, efficient small-payload organization, and direct bounded I/O. See the [Dataset source](../../lib/crowdb-access-dataset/src) and [SDK examples](../../sdk/README.md#python).
