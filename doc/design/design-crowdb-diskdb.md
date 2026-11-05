<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Disk Allocation

DiskDB turns disk capacity into managed allocations. It records which space is owned, which space can be reused, and how disks and zones recover their allocation state. Byte reads and writes are handled separately by DiskIO.

This separation lets CROWDB make allocation a durable metadata operation without putting that operation in the path of every subsequent I/O. ChunkDB requests space for its layouts; payload clients then use the resulting locations to access DiskIO.

## Durable records and working allocation state

`crowdb-diskdb` manages disk groups across multiple nodes. A disk group belongs to one node and contains disks; a node can have several disk groups. Each disk is divided into zones and allocation blocks. Group 0 stores topology and owner bindings, while each disk group has an explicit Paxos-KV data-group binding for its allocation records. The client routes requests to the current DiskDB owner. Durable allocation records establish ownership. Working structures such as bitmaps make allocation and free-space queries efficient, but they can be reconstructed from the persisted records.

That distinction is valuable after a restart. The server can rebuild the working representation from the recorded facts rather than treating a partially updated in-memory bitmap as the only account of disk ownership. Scanner and recovery work reconcile the durable and operational views.

![DiskDB routes allocations across disk groups on multiple nodes, with durable records in Paxos-KV.](../assets/disk-allocation.svg)

Freeing space also changes ownership. It must be reflected in the durable allocation model before that space is safely reused. Zone maintenance and compaction manage the representation and accumulated work behind those decisions.

## Capacity is more than a free-byte counter

Operators need to understand disks, zones, allocated space, and the work that can return capacity. DiskDB maintains counts and observations at the appropriate levels; `crowdb-diskdb-client` routes calls and aggregates information for its consumers.

These observations help placement and management, while the allocator remains responsible for the actual decision to grant space. A cached capacity report is useful for choosing candidates, but it cannot reserve the bytes it describes.

Keeping capacity reporting near allocation makes its meaning easier to explain. Physical space, allocated space, and logical user data are different quantities, particularly with replication, erasure coding, and deferred cleanup.

## Why allocation is a separate service

An allocation changes durable ownership. A read of an existing allocation usually does not. Combining both into one broad operation would make it harder to optimize their different frequencies and failure behavior.

DiskDB gives CROWDB a focused place for disk lifecycle, space accounting, and rebuildable allocation state. DiskIO can concentrate on efficient byte operations, while ChunkDB chooses how several allocations form a protected layout.

The cost is a distributed dependency when obtaining or freeing space. Durable records, working allocation structures, and background recovery must agree. CROWDB accepts that coordination to keep ownership explicit and recoverable across process failures.

See the [allocation model](../../app/crowdb-diskdb/src/model), [persistence](../../app/crowdb-diskdb/src/persistence.rs), and [recovery](../../app/crowdb-diskdb/src/recovery) code. The adjacent [DiskIO design](design-crowdb-diskio.md) covers actual I/O.
