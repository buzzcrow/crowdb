<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R146: chunkdb — Self-validating chunk frames and abandoned-chunk finalization

## Problem

`Repo`, chunk `Stream`, and the B+tree (`BtreePage` and `PageIndex`) append
user bytes to active chunks, but today their durability, integrity, and
abandoned-chunk recovery contracts are separate. A process crash can leave an
Active tail whose safe end is not derivable from disk; the existing
writer-lease sweep scans chunk metadata rather than an expiry index; and a
successful raw read has no common end-to-end checksum that can turn silent
corruption into a mirror or EC protection read.

Persisting a sidecar checksum or cursor for every small write would add a
second write path, cause poor HDD seek behavior and SSD write amplification,
and make location metadata disproportionately large. Padding every small
object to I/O alignment would similarly waste capacity. A large object also
needs range reads with bounded verification amplification.

The root lifecycle contract is
`doc/design/chunkdb/design-crowdb-chunkdb.md`. This requirement replaces
R146's old generic `owner_key`, writer-lease, acknowledged-cursor, and full
Active-chunk-scan design. There is no released-data compatibility requirement:
existing test/development data and callers may be rewritten.

## Solution

All persistent chunk users use a public, self-validating frame protocol and
one durable liveness task per Active chunk. The frame is both the integrity
unit for reads and the recovery unit for abandoned writes. It is not a DiskIO
alignment unit; DiskIO continues to handle device alignment internally without
callers adding padding to a frame.

1. Define the public frame wire contract and shared utilities in
   `lib/crowdb-protocol`. The canonical header prefix is encoded explicitly,
   with no language-struct padding:

   ```text
   FrameHeaderPrefix {
     magic:          u16,  // public FrameMagic enum value (kind + version)
     payload_offset: u16,  // byte offset from the frame start
     payload_size:   u16,
     write_time_ms:  u64,  // diagnostic wall-clock time only
   }

   FrameFooter {
     checksum_crc32c: u32,
     chunk_id:        [u8; 16],
   }
   ```

   `payload_offset` is at least the prefix length and permits a future header
   extension before the payload. `FrameKind` is a public enum constant,
   including distinct values for `RepoSmall`, `RepoLarge`, `Stream`,
   `BtreePage`, and `PageIndex`; it is not repeated in a location. A change
   with different interpretation uses a new magic/version. The CRC32C covers
   the encoded header, payload, and chunk ID, but not its own checksum field.
   The parser rejects an unknown magic, an offset or size outside the frame, a
   footer whose chunk ID differs from the expected chunk, and a checksum
   mismatch. `write_time_ms` is for diagnosis only and never grants write
   authority or determines liveness.

2. A frame's physical length is
   `payload_offset + payload_size + sizeof(FrameFooter)` and must be at most
   64 KiB. Frames are variable length and are written without synthetic
   padding. A `RepoSmall` object is one frame. A `RepoLarge` object is a
   sequence of frames: every interior frame fills the 64 KiB maximum and
   its final frame may be shorter. The common location utility represents a
   single location and an ordered combined location, validates contiguity,
   merges adjacent locations, and maps a logical subrange to the containing
   physical frame range. A large-object location records only the chunk,
   first-frame position, and logical length needed for that computation; it
   does not store a per-frame checksum, magic, layout, or location. B+tree and
   PageIndex pages use the same location form: the default persistent page
   limit is 65,502 bytes, so the usual page is one frame and one location. A
   larger page is split into frames within one chunk and remains one location.
   A tree page never crosses chunks: if the Active chunk's remaining capacity
   cannot hold the complete framed page, the writer rotates before that page.
   The configured maximum tree-page size must fit one allocatable chunk. The
   tree manifest replaces its scalar `ChunkPageRef` checksum/location
   representation with the shared single-location representation; it never
   adds a per-frame checksum. A Stream frame's payload is the stream's journal
   record; the stream journal removes checksum/provenance fields now supplied
   by the outer frame.

3. Make the protocol implementation usable by every caller rather than
   duplicating parsers. `crowdb-protocol` supplies encode, parse, verify, and
   location/subrange helpers plus shared cross-language test vectors. The Rust
   chunk clients and the C++ tree and DiskIO paths consume that same wire
   definition and vectors. A fat client verifies frames itself before data is
   exposed or RDMA is completed; DiskIO recognizes and bounds-checks the
   public format at its boundary without forcing an extra disk read, parse, or
   payload copy on the normal write path.

4. On creating an Active chunk, chunkdb atomically creates its sole durable
   `FinalizeChunk` liveness task, keyed by the chunk ID and containing the
   owner generation and expiry. The initial expiry is 15 minutes. A live owner
   renews on a 12-minute cadence, independent of writes. Renewal is one
   conditional transaction batch in the chunk's task partition: compare the
   canonical task's expected revision, state, and owner generation; delete the
   old deadline-ordered ready index; overwrite the same task record with its
   new deadline and revision; and insert the new ready index. The task is the
   only source of the liveness deadline; chunk metadata must not retain a
   second deadline. The owner generation identifies the allocation owner and
   does not change for ordinary renewal. On creation and every successful
   renewal, the owner converts that wall-clock expiry to a local monotonic
   self-fence deadline by subtracting the shared maximum clock skew and
   self-fence margin. Write admission checks that local deadline.

5. The task scanner scans only due ready-index entries, whose binary key has a
   fixed-width ordered deadline prefix, never the chunk table or unrelated
   task kinds. Claiming uses the same compare-and-transition primitive. Thus a
   scanner that observed an old index loses to an already-committed renewal; if
   it has already claimed the task, a renewal fails and the owner immediately
   stops using that chunk and allocates a new one. A restart never resumes a
   prior process's Active chunk. The owner metadata/publication for repository
   chunks belongs behind the chunk-kv interface, not a Paxos KV group; this
   requirement must not couple it to the current backing store.

6. A liveness deadline uses the existing serving-authority self-fence model.
   An owner that cannot renew stops admitting writes at its conservative local
   monotonic deadline, including when it remains connected to DiskIO during a
   network partition. A claimed liveness task waits through the shared maximum
   write-request age, peer wall-clock skew bound, and scanner safety margin. It
   then blocks new writes for that chunk, reads and validates consecutive frames
   from offset zero, and seals at the largest complete valid frame boundary. It
   releases an empty chunk. Chunkdb crash recovery resumes the durable task.
   Owner-server crash recovery always writes a new chunk; a surviving old
   request is rejected by DiskIO when its explicit wall-clock creation time
   exceeds the configured request-age bound after allowing the peer-skew bound.
   `rpc_create_nano` remains a local RPC correlation value and must not be used
   for this cross-node check.

7. Add an explicit wall-clock write creation timestamp and `OldRequest` result
   to `lib/crowdb-protocol/src/fbs/diskio.fbs`, enforce it in
   `app/crowdb-diskio/src/rpc/dio_server.cpp`, and propagate it through the
   DiskIO client. `max_write_request_age` is one shared bound for DiskIO queue
   residence, network delay, and permitted write retries. The existing
   serving-authority timing policy supplies maximum peer skew and self-fence
   margin; DiskIO rejection and task finalization consume those shared bounds
   rather than adding caller-local time configuration.

8. Integrate frame integrity failure into
   `lib/crowdb-chunk-client/src/chunk/strip_reader.rs` and the repair path.
   For one connected target, a read gets at most three total attempts. A
   connection-establishment failure skips further attempts on that target and
   moves immediately to another mirror or the EC path. A returned frame with
   invalid structure, mismatched chunk ID, or bad CRC32C is a corrupt strip,
   never successful data. The validating read interface retains the source
   segment provenance until frame verification has passed, so it can exclude
   that exact mirror/shard, obtain valid mirror data or reconstruct from EC
   shards, verify reconstructed frames before return, and submit the corrupt
   strip to the existing repair workflow. Exhausting viable sources returns an
   integrity/read error and never returns unchecked bytes.

9. Move Repo, Stream, and B+tree append/read paths onto these frame and
   liveness contracts. Location publication happens only after the relevant
   user data is durable; recovery finds the physical committed boundary from
   frames, not from a per-write chunkdb cursor. Expose frame validation
   failures, protection-read/reconstruction counts, liveness renewals,
   due-task lag, chunks sealed/deleted, retained bytes, and rejected old
   writes.

## Dependencies

- Reuses the existing serving-authority timing policy, including maximum clock
  skew and self-fence margin. R146 adds its shared `max_write_request_age` to
  that policy and uses it for both DiskIO and finalization.
- Reuses `TaskStore`/`TaskManager` in `app/crowdb-chunkdb/src/task` and its
  atomic same-partition batch writes; R146 adds the liveness task kind and its
  conditional renewal/claim behavior.
- Reuses mirror fallback, EC reconstruction, and repair submission in
  `lib/crowdb-chunk-client`; R146 extends their failure input to verified frame
  corruption.
- R147 may consume the sealed chunks for physical strip reclamation.

## Acceptance

- Given each public frame kind and a payload at the maximum allowed size, when
  Rust and C++ encode, parse, and verify the shared vectors, assert both
  implementations produce and accept identical bytes; assert bad magic,
  bounds, footer chunk ID, and CRC are rejected. Invariant: every reader has
  one wire interpretation. Unit test.
- Given a `RepoSmall` object and a `RepoLarge` object whose final payload does
  not fill a frame, when they are written and their combined locations are used
  for a range read, assert no frame exceeds 64 KiB, no write supplies alignment
  padding, all requested bytes round-trip, and only containing frames are read
  and verified. Invariant: bounded range verification needs no per-frame
  location metadata. Integration test.
- Given a default-size B+tree or PageIndex durable page, when it is persisted
  and read, assert it is one verified frame and one location. Given a larger
  page or insufficient tail capacity in the Active chunk, assert the writer
  frames the complete page in one chunk, rotates before the page when needed,
  and round-trips only after every frame verifies. Given a configured page too
  large for one chunk, assert allocation is rejected before I/O. Invariant: a
  tree page never crosses chunks and always has one location. Integration test.
- Given a Stream journal record, when it is framed and replayed, assert the
  stream payload format round-trips and no duplicate outer checksum or
  provenance trailer is persisted. Invariant: the frame owns physical integrity
  metadata. Integration test.
- Given an Active chunk, when it is created and renewed, assert its one
  liveness task has exactly one canonical record and the old ready index is
  removed while the new deadline index appears in the same committed batch.
  Invariant: task expiry has one durable source of truth. Integration test.
- Given concurrent renewal and task claim with the same observed task revision,
  when both submit their conditional batch, assert exactly one commits and the
  losing operation observes a conflict rather than overwriting the winner.
  Invariant: transaction atomicity does not permit lost task transitions.
  Integration test.
- Given a scanner has read an old due index, when a valid liveness renewal
  commits before the scanner claims the task, assert claim is rejected and the
  chunk remains Active. Given claim commits first, assert renewal fails and the
  owner allocates a new chunk. Invariant: renewal and finalization have one
  ordered result. Integration test.
- Given an owner-server crash and later a chunkdb restart, when its liveness
  expiry plus request-age, skew, and safety bounds pass, assert the resumed
  task seals at the final complete CRC-valid frame boundary or deletes an empty
  chunk; assert a delayed pre-crash write receives `OldRequest`. Invariant: no
  ambiguous tail becomes visible. E2E test.
- Given a chunk owner loses renewal while its DiskIO connection remains live,
  when its conservative local monotonic self-fence deadline arrives, assert it
  admits no further writes; assert the finalizer runs only after the shared
  request-age, skew, and scanner-safety window. Invariant: a network partition
  cannot prolong a chunk writer beyond its liveness authority. Integration test.
- Given a connected replica returns an I/O error twice then data, when the
  frame is read, assert at most three total attempts are issued to that target
  and the verified data is returned. Given connection establishment fails,
  assert that target receives no retry and protection reading starts. Given a
  returned frame has a bad CRC, assert it is excluded, mirror fallback or EC
  reconstruction is verified before return, and repair is submitted for the
  corrupt strip. Invariant: a checksum failure is a protection-read failure,
  not successful data. Integration test.
- Given no mirror or reconstructable EC set yields a verified frame, when the
  caller reads it, assert it receives an integrity/read error and no payload.
  Invariant: unchecked bytes never cross the client boundary. Integration test.

Required gates:

- `pixi run tree-fmt`
- `pixi run tree-lint`
- `pixi run test-tree-ct`
- `pixi run test-tree-ffi`
- `pixi run -- cargo test -p crowdb-protocol --all-targets`
- `pixi run -- cargo test -p crowdb-chunkdb --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-stream --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
