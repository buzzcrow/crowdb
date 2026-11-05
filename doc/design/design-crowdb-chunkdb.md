<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Chunk Storage

CROWDB Chunk storage combines placement, data protection, and payload I/O. ChunkDB owns metadata, allocation coordination, and layout lifecycle. The Chunk client uses those layouts to move bytes directly through DiskIO. Together they provide dedicated large-write pipelines, shared small-write pipelines, mirror/EC protection, and bounded reads.

This puts the protection policy below Iceberg, Dataset, S3, and internal storage. Each can retain its own logical data model while reusing the same placement, repair, and capacity machinery.

## From a Chunk to physical blocks

A Chunk is a logical storage unit. Its layout describes strips and the physical blocks that hold their replicas or erasure-coded fragments. Placement uses the available topology and failure domains to choose those locations; DiskDB supplies the underlying allocations.

`crowdb-chunkdb` manages those records and decisions. `crowdb-chunkdb-client` routes metadata operations to their owning service. `crowdb-chunk-client` performs payload I/O against the selected layout. The metadata service therefore does not need to proxy every byte of a large transfer.

![Chunk client obtains a layout from ChunkDB, whose metadata is in Paxos-KV, then writes through DiskIO to disks on multiple nodes.](../assets/chunk-placement.svg)

Chunk allocation follows a concrete sequence:

1. The Chunk client requests a new layout from ChunkDB.
2. ChunkDB chooses node and disk-group targets, then asks DiskDB to allocate physical blocks.
3. ChunkDB persists the Chunk ID, strips, segment locations, and lifecycle state in the selected nonzero Paxos-KV store group. Group 0 supplies system metadata such as topology and service registrations.
4. The client receives the layout and sends payload through the target nodes' DiskIO services to their local disks. Later reads query the current layout.

Each node can contain multiple disks. Placement combines segments from those disks according to the required mirror or EC protection and failure-domain policy. Metadata operations establish where the chunk lives; the bulk data path runs directly between the client and DiskIO.

## Two write paths

![Large writes use dedicated pipelines and chunks; small writes multiplex N objects over M pipelines and shared chunks, with distinct EC paths.](../assets/chunk-io.svg)

A large write has a dedicated pipeline and an exclusive active chunk: a 1:1 mapping while that chunk is being filled. Input is framed and buffered within bounded budgets. The pipeline writes the selected mirror or EC layout, seals the chunk, and returns its location. An object larger than one chunk rotates through additional dedicated chunks and returns their ordered locations. Another object's bytes never enter those chunks.

This gives a sustained transfer its own progress and backpressure. Preparing the next chunk and submitting independent I/O can overlap with active work, without allocating storage or memory for the entire object in advance.

Small writes use an N:M mapping: N objects enter a bounded pool of M pipelines, and each pipeline exclusively owns one active shared chunk. The pool routes admitted objects to an available pipeline. Each pipeline batches queued whole objects, frames their data, and writes their ranges to every mirror. Objects may cross strip boundaries within that chunk, but they do not cross a chunk boundary.

Each small object keeps its own location and completion result. A shared chunk may contain many objects, while allocation, physical writes, and chunk rotation are shared across them. The pool can adjust the number of active pipelines within configured limits. This reduces the cost of creating a separate physical container for every small payload while keeping concurrency and retained memory bounded.

## EC follows the write path

For a large write using EC, the strip layout supplies the data and parity widths. Data shards feed incremental parity computation and are submitted to DiskIO as they become available. At the end of a strip, the writer submits its parity shards. The next strip's data writes can overlap the previous strip's parity I/O. Chunk sealing joins the required data and parity completions before exposing the completed locations. Partial tails retain their exact valid byte length.

The small-write path starts with mirrored strips. When automatic conversion is enabled, closed strips incrementally contribute to an EC group; the current shared path uses eight data strips and four parity shards. The client writes and synchronizes the parity, then ChunkDB selects a healthy replica from each mirror set and publishes the 8+4 layout. Those existing replicas become the data shards, so conversion reuses their stored bytes. Mirrors remain authoritative until publication; incomplete groups or failed foreground conversion remain mirrored and can be handled by background conversion.

The two paths put EC at different points in the lifecycle. Large writes can establish their EC representation directly. Small writes first aggregate independently completing objects in mirrored storage, then reduce redundancy overhead across completed strips.

## Completion stays per object

Large-write completion includes sealing every chunk used by the object. Shared chunks stay open across many object completions. Callers that immediately publish readable data use the shared writer's durable completion barrier, which confirms that the chunk's acknowledged cursor covers the returned locations. The lower-level asynchronous completion mode can return before that cursor catches up.

Iceberg, Dataset, and S3 then perform their own metadata publication. Sharing physical storage does not merge their visibility or deletion rules. An open shared writer can retain buffers for repair, and reclamation must account for other objects still using the chunk.

## Read flow

![Locations resolve to current chunk layouts; mirror reads and direct or recovered EC reads feed verified bounded delivery.](../assets/chunk-read.svg)

A reader maps the requested logical range through the object's locations and current ChunkDB metadata. It follows the stored strip intervals, exact valid lengths, and current physical segment identities. This allows a location to keep its meaning after a mirror-to-EC layout change.

A mirror read fetches the needed interval from a usable replica and can try another replica when the failure permits a safe retry. A healthy EC read fetches only the data-shard intervals the request needs. If a required data shard is unavailable, the reader obtains enough surviving data and parity intervals to reconstruct that slice within its recovery budget. It does not decode the entire object merely to serve a small range.

Frame checks, exact lengths, and layout validity govern delivery. A streaming reader returns bounded buffers as the consumer pulls; cancellation stops further production, while held buffers retain their owners. A readable prefix is useful for streaming, but complete success requires the validated end of the requested range. Repair remains a separate operation whose allocation and layout publication are owned by ChunkDB.

## Repair is a metadata transition as well as a copy

Rebuilding bytes is only part of repair. The system also needs to identify the work, select the current layout, publish the valid replacement, and eventually release obsolete allocations. ChunkDB owns that lifecycle.

Tasks carry the authority needed to act on their Chunk. Routing and ownership changes must fence old workers, including workers that resume after a long pause. A completed physical copy cannot override a newer layout merely because its original task eventually finished.

Keeping this coordination with Chunk metadata gives recovery a durable place to resume. It also lets foreground reads and writes use published layouts while background conversion or repair prepares their successors.

## Placement and resource control

Explicit layouts let CROWDB change physical protection and placement while retaining a file or object's logical data reference. A computed placement rule is another possible design; stored layouts give ChunkDB a durable record of the exact allocations, conversion, and repair work it owns. Those decisions stay out of the bulk-data relay once the client has its layout.

The shared small-write pool amortizes allocation and I/O across objects; dedicated large writers preserve independent progress and backpressure. Readers fetch requested intervals and reconstruct only missing EC slices. These mechanisms let physical storage work follow the size and shape of the request while the access libraries retain their own publication semantics.

Memory depends on active pipelines, queued objects, outstanding I/O, parity state, recovery scratch, and caller-held allocations. Each has an owner and a budget. Retained buffers can avoid staging copies across eligible handoffs, while framing, alignment, and EC still perform the transformations the storage format requires. Repair and conversion share real disk and network capacity with foreground traffic, so their work is budgeted as part of the same system.

See [placement selection](../../app/crowdb-chunkdb/src/selector), [conversion](../../app/crowdb-chunkdb/src/conversion.rs), [repair](../../app/crowdb-chunkdb/src/repair.rs), the [large writer](../../lib/crowdb-chunk-client/src/writer/large_object.rs), [small writer pool](../../lib/crowdb-chunk-client/src/writer/small_pool.rs), and [reader](../../lib/crowdb-chunk-client/src/chunk/chunk_reader.rs).
