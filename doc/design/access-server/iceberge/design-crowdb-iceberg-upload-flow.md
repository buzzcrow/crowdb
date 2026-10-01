<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Iceberg File Upload Flow

This document traces the FileIO upload path used by Iceberg clients and records
measurements from a single-node container. It covers file publication, not table
snapshot publication.

Depends on: [Native Iceberg Storage](design-crowdb-iceberg.md) and
[Chunk I/O](../../chunkio/design-crowdb-chunkio.md).

## Table of contents

1. [Client and server flow](#1-client-and-server-flow)
2. [Large file measurement](#2-large-file-measurement)
3. [Small file measurement](#3-small-file-measurement)
4. [Interpretation and next measurements](#4-interpretation-and-next-measurements)

## 1. Client and server flow

The TPC loader writes one local Parquet part at a time through `CrowdbFileIO`.
It copies bounded input buffers into the PyArrow output stream while computing
a local SHA-256 digest. It does not read the uploaded object back. Closing the
stream waits for the FileIO transfer to finish. Only after all files for a
table are uploaded does the loader import them and commit the table once.

PyArrow currently uses multipart FileIO even for the measured 64-KiB object:

1. FileIO checks whether the exact target exists, then starts a multipart
   session. Neither step publishes a table snapshot.
2. Each `UploadPart` authenticates and validates its body, prepares a chunk
   writer, streams bytes to Chunk I/O, finishes the writer, and persists the
   part's locations and session progress. The write path verifies the supplied
   body integrity information before accepting it.
3. `CompleteMultipart` freezes the selected parts and assembles their chunk
   locations into one file record. Native stream parts are composed logically;
   completion does not copy the object payload. The file mapping is then
   published. Recovery can resume an interrupted completion.
4. A later Iceberg table commit publishes metadata that references this file.
   Uploaded files remain outside the table snapshot until that commit.

Writer selection uses the decoded length of each HTTP request, when available.
Payloads below the small-object threshold can use the small-object writer for
one frame or the shared-object writer for a longer request. Other requests use
the large writer. The total logical file size alone does not select the writer.
The large writer drives chunk strips and seals its locations on completion.

## 2. Large file measurement

On 2026-10-01, a local single-node container received a 100-MiB generated
object through the same `CrowdbFileIO` output API as the loader. The object was
not registered in an Iceberg table. Two runs took 6.23 s and 5.90 s. The second
run's stage and server-counter deltas were:

| Measurement                           |      Result |
| ------------------------------------- | ----------: |
| Create output stream, including probe |     0.322 s |
| Thirteen 8-MiB client writes          |     0.075 s |
| Close and finish multipart transfer   |     5.507 s |
| End-to-end elapsed time               |     5.904 s |
| Effective logical throughput          | 16.9 MiB/s |
| Successful FileIO HTTP requests       |          12 |
| Catalog GET operations                |         144 |
| Catalog compare-exchange operations   |          16 |
| Small-write completions               |           0 |

The 12 successful requests are consistent with creating a session, uploading
parts, and completing it. The existence probe returns not found and is not in
the success count. The summed server dispatch time was 31.61 s across requests;
multipart part requests overlap, so this sum is not wall-clock latency.

The client writes returned in 75 ms because PyArrow buffers or schedules the
transfers. The 5.5-s `close()` wait is the dominant observed client stage.
The zero small-write delta rules out the small-object queue as the path for
this 100-MiB sample. Catalog GET and CAS counts show metadata work remains
per multipart session and part, rather than a single final table update.
These counters do not yet isolate network transfer, chunk allocation, DiskIO,
or sealing within the 5.5-s wait.

## 3. Small file measurement

The same container and API were used for three generated objects. Each used
three successful FileIO requests, 45 catalog GETs, and seven catalog CAS
operations, even though the payload sizes differed.

| Object size |    Open | Client write |   Close |   Total | Small-write completions |
| ----------- | ------: | -----------: | ------: | ------: | ----------------------: |
| 64 KiB      | 0.345 s |     <0.001 s | 1.242 s | 1.587 s |                       1 |
| 1 MiB       | 0.328 s |     <0.001 s | 1.213 s | 1.541 s |                       0 |
| 5 MiB       | 0.344 s |      0.001 s | 1.498 s | 1.844 s |                       0 |

The 64-KiB transfer completed through the small-write queue. The 1-MiB and
5-MiB requests did not increment that counter; the available metrics do not
separate shared-object from large-writer completions. Fixed FileIO session,
part, and completion work is material for a small object. The table-level
commit is not included in these timings.

## 4. Interpretation and next measurements

The loader's local copy and checksum loop is not the measured bottleneck for
these generated objects. The large-file wait is inside remote multipart
completion and its concurrent part uploads. A 100-MiB transfer still takes
about six seconds on this single-node setup, which is too slow to dismiss as
file size alone. The current metrics do not identify a specific erroneous
server operation, so changing writer policy or removing metadata checks would
be premature.

The next diagnostic comparison is a direct 100-MiB PUT against multipart
UploadPart requests with equal payload and durability settings. Per-request
timing should then split transfer, chunk preparation, DiskIO completion, part
state publication, and final file publication. For small files, measure the
same stages separately from the exact-object probe and multipart session
setup. Catalog GET and CAS counts should be traced to operation names before
removing any recovery or fencing reads.
