<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: ChunkStream

ChunkStream is CROWDB's reusable durable byte-stream layer. It turns finite storage chunks into one ordered logical space, with stable stream identity, append and offset reads, writer fencing, rollover, recovery, and prefix reclamation. Journals can use that interface without implementing their own distributed physical log storage.

`crowdb-chunk-stream` is a library over CROWDB's metadata and Chunk storage. Chunk-KV uses it for partition journals. The stream understands append ordering and byte validity; its caller understands the records stored in those bytes.

## Logical order across physical chunks

![Adjacent chunks cover continuous logical ranges [0, L1), [L1, L2), and [L2, tail).](../assets/chunk-stream.svg)

A writer appends to the active tail. Adjacent chunks cover contiguous half-open logical ranges: `[0, L1)`, `[L1, L2)`, and `[L2, tail)`. One chunk's logical end is the next chunk's logical start. Rollover introduces no logical gap and does not reset the offset. Readers can request a logical range spanning several chunks without reconstructing the physical transitions themselves.

Stream metadata records the structure needed to find and recover the tail. Writer ownership prevents an obsolete writer from extending the stream after a handoff. Recovery establishes the usable appended content and the writer's current authority before continuing.

This is especially useful after an interrupted append. Physical bytes can exist without proving that the complete logical append finished. The stream provides the boundary that lets a journal distinguish usable content from an incomplete tail.

## Journals retain their own meaning

A Chunk-KV journal can encode mutations into the stream and replay them into Tree. ChunkStream does not interpret those mutations or decide when the partition may report a successful update. Its append result is one input to the caller's completion rules.

The journal supplies the record format, mutation order, and the relationship between an applied mutation and a stream position. ChunkStream supplies the ordered byte range and its durable append boundary. Rollover can move later bytes into another physical Chunk while the journal continues using the same logical offset space. Record parsing and physical chunk-chain management therefore remain separate jobs.

Likewise, old stream bytes can be reclaimed only after the owner has established that they are no longer needed. A partition may base that decision on durable Tree state and its recovery requirements. ChunkStream then manages the physical prefix cleanup.

After a restart or owner transfer, the partition reopens its durable Tree state and reads the required journal suffix through logical stream ranges. Writer fencing prevents an obsolete owner from extending the stream under its old authority. As the partition advances its durable recovery point and releases other retained dependencies, it can authorize trimming an older prefix. The same stream lifecycle supports writing, reopening, and eventual reclamation without making the journal own physical disk allocation.

## Why use shared chunks for a journal?

A local append file is a natural choice when the journal belongs to one machine. A shared journal supports a serving process that may move: a new owner can recover the same stream through the storage services.

Using CROWDB's Chunk layer also reuses its data protection and physical allocation. The stream needs to manage ordering and boundaries, while the underlying services manage where the bytes live. The cost is remote I/O and dependence on the metadata and storage services needed to open the stream.

## A foundation for another access model

WAL is one application of a durable ordered stream. A future native Stream API could reuse the existing append, offset-read, rollover, recovery, and trim machinery, concentrating new work on public access, quotas, retry behavior, and the delivery contract. It would inherit physical allocation and data protection from the same Chunk services that already support CROWDB's other models.

A record-oriented log adapter could build on that foundation with its own record offsets, producer identity, visibility, and retention rules. A record number would need a mapping to its byte range; a retried producer request would need an identity that distinguishes a repeat from a new append. Those meanings belong above the stream, just as mutation records belong to Chunk-KV's journal.

A public service would also choose how readers reach the tail. The existing sequential reader captures a finite readable end; a subscription or tail-following interface would add waiting, cancellation, and retention behavior. These are future interface choices. The architectural advantage is that their implementation can start from durable ordered storage already used by Chunk-KV, concentrating new work on the model callers actually need.

The [stream implementation](../../lib/crowdb-chunk-stream/src) contains the authority, metadata, and append logic. Its main architectural consumer is [Chunk-KV](design-crowdb-chunk-kv.md).
