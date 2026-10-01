<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Iceberg File Upload Flow

This document defines the FileIO upload path used by Iceberg clients and records
measurements from the single-node profile. It covers file publication, not table
snapshot publication.

Depends on: [Native Iceberg Storage](design-crowdb-iceberg.md) and
[Chunk I/O](../../chunkio/design-crowdb-chunkio.md).

## Table of contents

1. [Client and server flow](#1-client-and-server-flow)
2. [Large write ownership and scheduling](#2-large-write-ownership-and-scheduling)
3. [Integrity and durable publication](#3-integrity-and-durable-publication)
4. [Measured costs](#4-measured-costs)

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

## 2. Large write ownership and scheduling

The single-use `WriteObject` owns the request body, bounds, file identity,
writer, digest pipe, measurements, and final publication action. Its transfer
coroutine polls the body receiver and write consumer with one task waker. Only
the receiver fetches the socket body. When it offers a prepared buffer, the
same task immediately polls the consumer. If both sides are pending, the task
yields; a Hyper body-read event or a write completion wakes it again. An idle
writer therefore resumes when a slow socket supplies the next buffer.

The receiver assembles up to 1 MiB of payload from body frames. The native
receive path prepares frame headers and CRC32C on the received owner before
handoff. CRC32C excludes the chunk ID; after placement the writer fills that
field and writes the frame without an object-sized copy. The receiver offers
the owner to a bounded channel with four held-buffer slots. A full channel
pauses further body reads until the consumer removes an owner. The digest
worker receives borrowed payload views after the write offer, so checksum work
can overlap receive and DiskIO without controlling write backpressure.

The large chunk writer prepares strips ahead of demand. For a known object
size, it batches up to the configured strip-prefetch limit and requests the
next batch when half of the current one has been consumed. Mirror strips are
submitted to independent tasks, with at most four strip writes in flight by
default. Later writes may finish first, but completion is consumed in strip
order. At a full write window the coroutine awaits the oldest completion;
the completed owner queue can still retain four prepared buffers. The
`large_parallel_strip_writes` and `large_held_buffers` settings are separate.

The following invariants apply:

- **I1 — One body reader.** No second task reads an object's HTTP body.
- **I2 — Bounded ownership.** Receive buffers remain owned until the writer and
  digest have consumed their views; the write queue controls backpressure.
- **I3 — Ordered durability.** A later strip result cannot make an earlier
  failed strip successful. Chunk sealing waits for every submitted strip and
  its required fsyncs.
- **I4 — Event-driven progress.** Socket readiness and write completion wake
  the suspended coroutine. The write path does not spin or poll a timer for
  capacity.

## 3. Integrity and durable publication

The object-scoped OpenSSL worker computes MD5 over ordered logical payload
views. It also computes SHA-256 when a signed payload requires it. It never
hashes frame headers or footers. The writer fills placement-dependent chunk
IDs after the receiver has calculated placement-independent CRC32C. Digest
failure, declared-length mismatch, failed DiskIO, or seal failure prevents
file and part publication. An upload is published only after the body has
ended, the digest has been verified, all writes and fsyncs have completed,
and chunk locations have been sealed. Multipart completion and table commit
are later, distinct authority operations.

## 4. Measured costs

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

A second container run with the checksum worker used one prepared 1-MiB block
100 times through the same FileIO output stream. The 100-MiB upload took
5.014 s: 0.034 s to open, 0.104 s in client writes, and 4.876 s in `close()`.
The FileIO path still uses multipart. This result shows that checksum work is
not the only cost in its close stage; it does not isolate the remaining
multipart, transport, or storage costs.

### Small files

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

### Focused direct PUT

The single-node small-cluster fixture uses one mirror copy and null DiskIO.
Its producer yields 100 references to one prepared 1-MiB buffer, with
Content-MD5 computed before timing and `UNSIGNED-PAYLOAD` in the request. The
direct 100-MiB PUT completed in 335.9 ms. The upload observation was 315.4 ms,
including 224.0 ms in body-frame polling and waiting, 47.4 ms across 30
writer-capacity waits, 23.1 ms across two strip-preparation waits, 23.0 ms in
writer finish, and 47.9 ms in metadata publication. The sum of 101 strip-write
durations was 558.0 ms and digest CPU time was 234.3 ms. These stages overlap
and must not be added to estimate wall-clock time. Body-frame time includes
Hyper delivery and coroutine scheduling, not just socket reads.

The container's multipart FileIO timings above have different transport,
storage, and publication work from this focused direct PUT. A comparable
FileIO profile must record part size, concurrency, topology, durability,
software revision, and raw stage samples before attributing its close time
to a specific server stage.
